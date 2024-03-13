use std::io::Read;

use bitflags::bitflags;
use bson::{doc, Array, Document};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    header::{Header, OpCode},
    next_request_id,
};
use crate::{
    bson::RawDocumentBuf,
    bson_util,
    checked::Checked,
    cmap::{conn::wire::util::SyncCountReader, Command},
    compression::{Compressor, Decoder},
    error::{Error, ErrorKind, Result},
    runtime::SyncLittleEndianRead,
};

/// Represents an OP_MSG wire protocol operation.
#[derive(Debug)]
pub(crate) struct Message {
    // OP_MSG payload type 0
    pub(crate) document_payload: RawDocumentBuf,
    // OP_MSG payload type 1
    pub(crate) document_sequences: Vec<DocumentSequence>,
    pub(crate) response_to: i32,
    pub(crate) flags: MessageFlags,
    pub(crate) checksum: Option<u32>,
    pub(crate) request_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentSequence {
    pub(crate) identifier: String,
    pub(crate) documents: Vec<RawDocumentBuf>,
}

impl Message {
    /// Creates a `Message` from a given `Command`. Note that the `response_to` field must be set
    /// manually.
    pub(crate) fn from_command<T: Serialize>(
        command: Command<T>,
        request_id: Option<i32>,
    ) -> Result<Self> {
        let document_payload = bson::to_raw_document_buf(&command)?;

        let mut flags = MessageFlags::empty();
        if command.exhaust_allowed {
            flags |= MessageFlags::EXHAUST_ALLOWED;
        }

        Ok(Self {
            document_payload,
            document_sequences: command.document_sequences,
            response_to: 0,
            flags,
            checksum: None,
            request_id,
        })
    }

    /// Gets this message's command as a Document. If serialization fails, returns a document
    /// containing the error.
    pub(crate) fn get_command_document(&self) -> Document {
        let mut command = match self.document_payload.to_document() {
            Ok(document) => document,
            Err(error) => return doc! { "serialization error": error.to_string() },
        };

        for document_sequence in &self.document_sequences {
            let mut documents = Array::new();
            for document in &document_sequence.documents {
                match document.to_document() {
                    Ok(document) => documents.push(document.into()),
                    Err(error) => return doc! { "serialization error": error.to_string() },
                }
            }
            command.insert(document_sequence.identifier.clone(), documents);
        }

        command
    }

    /// Reads bytes from `reader` and deserializes them into a Message.
    pub(crate) async fn read_from<T: AsyncRead + Unpin + Send>(
        mut reader: T,
        max_message_size_bytes: Option<i32>,
    ) -> Result<Self> {
        let header = Header::read_from(&mut reader).await?;
        let max_len = max_message_size_bytes.unwrap_or(DEFAULT_MAX_MESSAGE_SIZE_BYTES);
        if header.length > max_len {
            return Err(ErrorKind::InvalidResponse {
                message: format!("Message length {} over maximum {}", header.length, max_len),
            }
            .into());
        }

        if header.op_code == OpCode::Message {
            return Self::read_from_op_msg(reader, &header).await;
        }
        if header.op_code == OpCode::Compressed {
            return Self::read_from_op_compressed(reader, &header).await;
        }

        Err(Error::new(
            ErrorKind::InvalidResponse {
                message: format!(
                    "Invalid op code, expected {} or {} and got {}",
                    OpCode::Message as u32,
                    OpCode::Compressed as u32,
                    header.op_code as u32
                ),
            },
            Option::<Vec<String>>::None,
        ))
    }

    async fn read_from_op_msg<T: AsyncRead + Unpin + Send>(
        mut reader: T,
        header: &Header,
    ) -> Result<Self> {
        let length = Checked::<usize>::try_from(header.length)?;
        let length_remaining = length - Header::LENGTH;
        let mut buf = vec![0u8; length_remaining.get()?];
        reader.read_exact(&mut buf).await?;
        let reader = buf.as_slice();

        Self::read_op_common(reader, length_remaining.get()?, header)
    }

    async fn read_from_op_compressed<T: AsyncRead + Unpin + Send>(
        mut reader: T,
        header: &Header,
    ) -> Result<Self> {
        let length = Checked::<usize>::try_from(header.length)?;
        let length_remaining = length - Header::LENGTH;
        let mut buf = vec![0u8; length_remaining.get()?];
        reader.read_exact(&mut buf).await?;
        let mut reader = buf.as_slice();

        // Read original opcode (should be OP_MSG)
        let original_opcode = reader.read_i32_sync()?;
        if original_opcode != OpCode::Message as i32 {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The original opcode of the compressed message must be {}, but was {}.",
                    OpCode::Message as i32,
                    original_opcode,
                ),
            }
            .into());
        }

        // Read uncompressed size
        let uncompressed_size = Checked::<usize>::try_from(reader.read_i32_sync()?)?;

        // Read compressor id
        let compressor_id: u8 = reader.read_u8_sync()?;

        // Get decoder
        let decoder = Decoder::from_u8(compressor_id)?;

        // Decode message
        let decoded_message = decoder.decode(reader)?;

        // Check that claimed length matches original length
        if decoded_message.len() != uncompressed_size.get()? {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server's message claims that the uncompressed length is {}, but was \
                     computed to be {}.",
                    uncompressed_size,
                    decoded_message.len(),
                ),
            }
            .into());
        }

        // Read decompressed message as a standard OP_MSG
        let reader = decoded_message.as_slice();
        let length_remaining = decoded_message.len();

        Self::read_op_common(reader, length_remaining, header)
    }

    fn read_op_common(mut reader: &[u8], length_remaining: usize, header: &Header) -> Result<Self> {
        let mut length_remaining = Checked::new(length_remaining);
        let flags = MessageFlags::from_bits_truncate(reader.read_u32_sync()?);
        length_remaining -= std::mem::size_of::<u32>();

        let mut count_reader = SyncCountReader::new(&mut reader);
        let mut document_payload = None;
        let mut document_sequences = Vec::new();
        while (length_remaining - count_reader.bytes_read()).get()? > 4 {
            let next_section = MessageSection::read(&mut count_reader)?;
            match next_section {
                MessageSection::Document(document) => {
                    if document_payload.is_some() {
                        return Err(ErrorKind::InvalidResponse {
                            message: "an OP_MSG response must contain exactly one payload type 0 \
                                      section"
                                .into(),
                        }
                        .into());
                    } else {
                        document_payload = Some(document);
                    }
                }
                MessageSection::Sequence(document_sequence) => {
                    document_sequences.push(document_sequence)
                }
            }
        }

        length_remaining -= count_reader.bytes_read();

        let mut checksum = None;

        if length_remaining.get()? == 4 && flags.contains(MessageFlags::CHECKSUM_PRESENT) {
            checksum = Some(reader.read_u32_sync()?);
        } else if length_remaining.get()? != 0 {
            let header_len = Checked::<usize>::try_from(header.length)?;
            return Err(Error::invalid_response(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                header.length,
                header_len - length_remaining + count_reader.bytes_read(),
            )));
        }

        Ok(Self {
            response_to: header.response_to,
            flags,
            document_payload: document_payload.ok_or_else(|| ErrorKind::InvalidResponse {
                message: "an OP_MSG response must contain exactly one payload type 0 section"
                    .into(),
            })?,
            document_sequences,
            checksum,
            request_id: None,
        })
    }

    /// Serializes the Message to bytes and writes them to `writer`.
    pub(crate) async fn write_to<T: AsyncWrite + Send + Unpin>(&self, mut writer: T) -> Result<()> {
        let sections = self.get_sections_bytes()?;

        let total_length = Checked::new(Header::LENGTH)
            + std::mem::size_of::<u32>()
            + sections.len()
            + self
                .checksum
                .as_ref()
                .map(std::mem::size_of_val)
                .unwrap_or(0);

        let header = Header {
            length: total_length.try_into()?,
            request_id: self.request_id.unwrap_or_else(next_request_id),
            response_to: self.response_to,
            op_code: OpCode::Message,
        };

        header.write_to(&mut writer).await?;
        writer.write_u32_le(self.flags.bits()).await?;
        writer.write_all(&sections).await?;

        if let Some(checksum) = self.checksum {
            writer.write_u32_le(checksum).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    /// Serializes message to bytes, compresses those bytes, and writes the bytes.
    pub(crate) async fn write_compressed_to<T: AsyncWrite + Unpin + Send>(
        &self,
        mut writer: T,
        compressor: &Compressor,
    ) -> Result<()> {
        let mut encoder = compressor.to_encoder()?;
        let compressor_id = compressor.id() as u8;

        let sections = self.get_sections_bytes()?;

        let flag_bytes = &self.flags.bits().to_le_bytes();
        let uncompressed_len = Checked::new(sections.len()) + flag_bytes.len();
        // Compress the flags and sections.  Depending on the handshake
        // this could use zlib, zstd or snappy
        encoder.write_all(flag_bytes)?;
        encoder.write_all(sections.as_slice())?;
        let compressed_bytes = encoder.finish()?;

        let total_length = Checked::new(Header::LENGTH)
            + std::mem::size_of::<i32>()
            + std::mem::size_of::<i32>()
            + std::mem::size_of::<u8>()
            + compressed_bytes.len();

        let header = Header {
            length: total_length.try_into()?,
            request_id: self.request_id.unwrap_or_else(next_request_id),
            response_to: self.response_to,
            op_code: OpCode::Compressed,
        };

        // Write header
        header.write_to(&mut writer).await?;
        // Write original (pre-compressed) opcode (always OP_MSG)
        writer.write_i32_le(OpCode::Message as i32).await?;
        // Write uncompressed size
        writer.write_i32_le(uncompressed_len.try_into()?).await?;
        // Write compressor id
        writer.write_u8(compressor_id).await?;
        // Write compressed message
        writer.write_all(compressed_bytes.as_slice()).await?;

        writer.flush().await?;

        Ok(())
    }

    fn get_sections_bytes(&self) -> Result<Vec<u8>> {
        let mut sections = Vec::new();

        // Payload type 0
        sections.push(0);
        sections.extend(self.document_payload.as_bytes());

        for document_sequence in &self.document_sequences {
            // Payload type 1
            sections.push(1);

            let identifier_bytes = document_sequence.identifier.as_bytes();

            let documents_size = document_sequence
                .documents
                .iter()
                .fold(0, |running_size, document| {
                    running_size + document.as_bytes().len()
                });

            // Size bytes + identifier bytes + null-terminator byte + document bytes
            let size = Checked::new(4) + identifier_bytes.len() + 1 + documents_size;
            sections.extend(size.try_into::<i32>()?.to_le_bytes());

            sections.extend(identifier_bytes);
            sections.push(0);

            for document in &document_sequence.documents {
                sections.extend(document.as_bytes());
            }
        }

        Ok(sections)
    }
}

const DEFAULT_MAX_MESSAGE_SIZE_BYTES: i32 = 48 * 1024 * 1024;

bitflags! {
    /// Represents the bitwise flags for an OP_MSG as defined in the spec.
    pub(crate) struct MessageFlags: u32 {
        const CHECKSUM_PRESENT = 0b_0000_0000_0000_0000_0000_0000_0000_0001;
        const MORE_TO_COME     = 0b_0000_0000_0000_0000_0000_0000_0000_0010;
        const EXHAUST_ALLOWED  = 0b_0000_0000_0000_0001_0000_0000_0000_0000;
    }
}

/// Represents a section as defined by the OP_MSG spec.
#[derive(Debug)]
enum MessageSection {
    Document(RawDocumentBuf),
    Sequence(DocumentSequence),
}

impl MessageSection {
    /// Reads bytes from `reader` and deserializes them into a MessageSection.
    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let payload_type = reader.read_u8_sync()?;

        if payload_type == 0 {
            let bytes = bson_util::read_document_bytes(reader)?;
            let document = RawDocumentBuf::from_bytes(bytes)?;
            return Ok(MessageSection::Document(document));
        }

        let size = Checked::<usize>::try_from(reader.read_i32_sync()?)?;
        let mut length_remaining = size - std::mem::size_of::<i32>();

        let mut identifier = String::new();
        length_remaining -= reader.read_to_string(&mut identifier)?;

        let mut documents = Vec::new();
        let mut count_reader = SyncCountReader::new(reader);

        while length_remaining.get()? > count_reader.bytes_read() {
            let bytes = bson_util::read_document_bytes(&mut count_reader)?;
            let document = RawDocumentBuf::from_bytes(bytes)?;
            documents.push(document);
        }

        if length_remaining.get()? != count_reader.bytes_read() {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server indicated that the reply would be {} bytes long, but it instead \
                     was {}",
                    size,
                    length_remaining + count_reader.bytes_read(),
                ),
            }
            .into());
        }

        Ok(MessageSection::Sequence(DocumentSequence {
            identifier,
            documents,
        }))
    }
}

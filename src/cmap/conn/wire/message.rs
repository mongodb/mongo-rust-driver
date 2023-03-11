use super::header::{Header, OpCode};
use crate::compression::{Compressor, Decoder};
use crate::{
    bson_util,
    cmap::{
        conn::{command::RawCommand, wire::util::SyncCountReader},
        Command,
    },
    error::{Error, ErrorKind, Result},
    runtime::{AsyncLittleEndianWrite, AsyncStream, SyncLittleEndianRead},
};
use bitflags::bitflags;
use futures_io::AsyncWrite;
use futures_util::{
    io::{BufReader, BufWriter},
    AsyncReadExt, AsyncWriteExt,
};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Read};

/// Represents an OP_MSG wire protocol operation.
#[allow(missing_docs)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Message {
    pub response_to: i32,
    pub flags: MessageFlags,
    pub sections: Vec<MessageSection>,
    pub checksum: Option<u32>,
    pub request_id: Option<i32>,
}

impl Message {
    /// Creates a `Message` from a given `Command`.
    ///
    /// Note that `response_to` will need to be set manually.
    pub(crate) fn with_command(command: Command, request_id: Option<i32>) -> Result<Self> {
        let bytes = bson::to_vec(&command)?;
        Ok(Self::with_raw_command(
            RawCommand {
                bytes,
                target_db: command.target_db,
                name: command.name,
            },
            request_id,
        ))
    }

    /// Creates a `Message` from a given `Command`.
    ///
    /// Note that `response_to` will need to be set manually.
    pub(crate) fn with_raw_command(command: RawCommand, request_id: Option<i32>) -> Self {
        Self {
            response_to: 0,
            flags: MessageFlags::empty(),
            sections: vec![MessageSection::Document(command.bytes)],
            checksum: None,
            request_id,
        }
    }

    /// Gets the first document contained in this Message.
    pub(crate) fn single_document_response(self) -> Result<Vec<u8>> {
        let section = self.sections.into_iter().next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidResponse {
                    message: "no response received from server".into(),
                },
                Option::<Vec<String>>::None,
            )
        })?;
        match section {
            MessageSection::Document(doc) => Some(doc),
            MessageSection::Sequence { documents, .. } => documents.into_iter().next(),
        }
        .ok_or_else(|| {
            Error::from(ErrorKind::InvalidResponse {
                message: "no message received from the server".to_string(),
            })
        })
    }

    /// Reads bytes from `reader` and deserializes them into a Message.
    pub(crate) async fn read_from(reader: &mut AsyncStream) -> Result<Self> {
        let mut reader = BufReader::new(reader);
        let header = Header::read_from(&mut reader).await?;

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

    async fn read_from_op_msg(
        mut reader: BufReader<&mut AsyncStream>,
        header: &Header,
    ) -> Result<Self> {
        // TODO: RUST-616 ensure length is < maxMessageSizeBytes
        let length_remaining = header.length - Header::LENGTH as i32;
        let mut buf = vec![0u8; length_remaining as usize];
        reader.read_exact(&mut buf).await?;
        let reader = buf.as_slice();

        Self::read_op_common(reader, length_remaining, header)
    }

    async fn read_from_op_compressed(
        mut reader: BufReader<&mut AsyncStream>,
        header: &Header,
    ) -> Result<Self> {
        let length_remaining = header.length - Header::LENGTH as i32;
        let mut buf = vec![0u8; length_remaining as usize];
        reader.read_exact(&mut buf).await?;
        let mut reader = buf.as_slice();

        // Read original opcode (should be OP_MSG)
        let original_opcode = reader.read_i32()?;
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
        let uncompressed_size = reader.read_i32()?;

        // Read compressor id
        let compressor_id: u8 = reader.read_u8()?;

        // Get decoder
        let decoder = Decoder::from_u8(compressor_id)?;

        // Decode message
        let decoded_message = decoder.decode(reader)?;

        // Check that claimed length matches original length
        if decoded_message.len() as i32 != uncompressed_size {
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

        Self::read_op_common(reader, length_remaining as i32, header)
    }

    pub(crate) fn read_op_common(
        mut reader: &[u8],
        mut length_remaining: i32,
        header: &Header,
    ) -> Result<Self> {
        let flags = MessageFlags::from_bits_truncate(reader.read_u32()?);
        length_remaining -= std::mem::size_of::<u32>() as i32;

        let mut count_reader = SyncCountReader::new(&mut reader);
        let mut sections = Vec::new();

        while length_remaining - count_reader.bytes_read() as i32 > 4 {
            sections.push(MessageSection::read(&mut count_reader)?);
        }

        length_remaining -= count_reader.bytes_read() as i32;

        let mut checksum = None;

        if length_remaining == 4 && flags.contains(MessageFlags::CHECKSUM_PRESENT) {
            checksum = Some(reader.read_u32()?);
        } else if length_remaining != 0 {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server indicated that the reply would be {} bytes long, but it instead \
                     was {}",
                    header.length,
                    header.length - length_remaining + count_reader.bytes_read() as i32,
                ),
            }
            .into());
        }

        Ok(Self {
            response_to: header.response_to,
            flags,
            sections,
            checksum,
            request_id: None,
        })
    }

    /// Serializes the Message to bytes and writes them to `writer`.
    pub async fn write_to<T: AsyncWrite + Unpin + Send>(&self, stream: &mut T) -> Result<()> {
        let mut writer = BufWriter::new(stream);
        let mut sections_bytes = Vec::new();

        for section in &self.sections {
            section.write(&mut sections_bytes).await?;
        }

        let total_length = Header::LENGTH
            + std::mem::size_of::<u32>()
            + sections_bytes.len()
            + self
                .checksum
                .as_ref()
                .map(std::mem::size_of_val)
                .unwrap_or(0);

        let header = Header {
            length: total_length as i32,
            request_id: self.request_id.unwrap_or_else(super::util::next_request_id),
            response_to: self.response_to,
            op_code: OpCode::Message,
        };

        header.write_to(&mut writer).await?;
        writer.write_u32(self.flags.bits()).await?;
        writer.write_all(&sections_bytes).await?;

        if let Some(checksum) = self.checksum {
            writer.write_u32(checksum).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    /// Serializes message to bytes, compresses those bytes, and writes the bytes.
    pub async fn write_compressed_to(
        &self,
        stream: &mut AsyncStream,
        compressor: &Compressor,
    ) -> Result<()> {
        let mut encoder = compressor.to_encoder()?;
        let compressor_id = compressor.id() as u8;

        let mut writer = BufWriter::new(stream);
        let mut sections_bytes = Vec::new();

        for section in &self.sections {
            section.write(&mut sections_bytes).await?;
        }
        let flag_bytes = &self.flags.bits().to_le_bytes();
        let uncompressed_len = sections_bytes.len() + flag_bytes.len();
        // Compress the flags and sections.  Depending on the handshake
        // this could use zlib, zstd or snappy
        encoder.write_all(flag_bytes)?;
        encoder.write_all(sections_bytes.as_slice())?;
        let compressed_bytes = encoder.finish()?;

        let total_length = Header::LENGTH
            + std::mem::size_of::<i32>()
            + std::mem::size_of::<i32>()
            + std::mem::size_of::<u8>()
            + compressed_bytes.len();

        let header = Header {
            length: total_length as i32,
            request_id: self.request_id.unwrap_or_else(super::util::next_request_id),
            response_to: self.response_to,
            op_code: OpCode::Compressed,
        };

        // Write header
        header.write_to(&mut writer).await?;
        // Write original (pre-compressed) opcode (always OP_MSG)
        writer.write_i32(OpCode::Message as i32).await?;
        // Write uncompressed size
        writer.write_i32(uncompressed_len as i32).await?;
        // Write compressor id
        writer.write_u8(compressor_id).await?;
        // Write compressed message
        writer.write_all(compressed_bytes.as_slice()).await?;

        writer.flush().await?;

        Ok(())
    }
}

bitflags! {
    /// Represents the bitwise flags for an OP_MSG as defined in the spec.
    #[derive(Serialize,Deserialize)]
    pub struct MessageFlags: u32 {
        const CHECKSUM_PRESENT = 0b_0000_0000_0000_0000_0000_0000_0000_0001;
        const MORE_TO_COME     = 0b_0000_0000_0000_0000_0000_0000_0000_0010;
        const EXHAUST_ALLOWED  = 0b_0000_0000_0000_0001_0000_0000_0000_0000;
    }
}

/// Represents a section as defined by the OP_MSG spec.
#[allow(missing_docs)]
#[derive(Debug, Deserialize, Serialize)]
pub enum MessageSection {
    Document(Vec<u8>),
    Sequence {
        size: i32,
        identifier: String,
        documents: Vec<Vec<u8>>,
    },
}

impl MessageSection {
    /// Reads bytes from `reader` and deserializes them into a MessageSection.
    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let payload_type = reader.read_u8()?;

        if payload_type == 0 {
            return Ok(MessageSection::Document(bson_util::read_document_bytes(
                reader,
            )?));
        }

        let size = reader.read_i32()?;
        let mut length_remaining = size - std::mem::size_of::<i32>() as i32;

        //let mut identifier = String::new();
        let mut identifier_vec = Vec::new();
        let mut buf_read = std::io::BufReader::new(reader);
        let n = buf_read.read_until(0x00, &mut identifier_vec)?;
        identifier_vec.pop();
        let identifier = String::from_utf8(identifier_vec).unwrap_or_default();
        //length_remaining -= reader.read_to_string(&mut identifier).unwrap_or_default() as i32;
        length_remaining -= n as i32;

        let mut documents = Vec::new();
        let mut count_reader = SyncCountReader::new(buf_read.by_ref());

        while length_remaining > count_reader.bytes_read() as i32 {
            documents.push(bson_util::read_document_bytes(&mut count_reader)?);
        }

        if length_remaining != count_reader.bytes_read() as i32 {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server indicated that the reply would be {} bytes long, but it instead \
                     was {}",
                    size,
                    length_remaining + count_reader.bytes_read() as i32,
                ),
            }
            .into());
        }

        Ok(MessageSection::Sequence {
            size,
            identifier,
            documents,
        })
    }

    /// Serializes the MessageSection to bytes and writes them to `writer`.
    async fn write<W: AsyncWrite + Unpin + Send>(&self, writer: &mut W) -> Result<()> {
        match self {
            Self::Document(doc) => {
                // Write payload type.
                writer.write_u8(0).await?;
                writer.write_all(doc.as_slice()).await?;
            }
            Self::Sequence {
                size,
                identifier,
                documents,
            } => {
                // Write payload type.
                writer.write_u8(1).await?;

                writer.write_i32(*size).await?;
                super::util::write_cstring(writer, identifier).await?;

                for doc in documents {
                    writer.write_all(doc.as_slice()).await?;
                }
            }
        }

        Ok(())
    }
}

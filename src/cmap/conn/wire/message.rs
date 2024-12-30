use std::io::Read;

use bson::{doc, Array, Document, RawDocumentBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use crate::options::Compressor;

#[cfg(feature = "fuzzing")]
use arbitrary::Arbitrary;

#[cfg(feature = "fuzzing")]
use crate::fuzz::{
    message_flags::MessageFlags,
    raw_document::{FuzzDocumentSequenceImpl, FuzzRawDocumentImpl},
};

#[cfg(not(feature = "fuzzing"))]
bitflags::bitflags! {
    pub(crate) struct MessageFlags: u32 {
        const CHECKSUM_PRESENT = 0b_0000_0000_0000_0000_0000_0000_0000_0001;
        const MORE_TO_COME =  0b_0000_0000_0000_0000_0000_0000_0000_0010;
        const EXHAUST_ALLOWED = 0b_0000_0000_0000_0001_0000_0000_0000_0000;
    }
}

use crate::{
    bson_util,
    checked::Checked,
    cmap::conn::{
        wire::{
            header::{Header, OpCode},
            next_request_id,
            util::SyncCountReader,
        },
        Command,
    },
    compression::decompress::decompress_message,
    error::{Error, ErrorKind, Result},
};

#[cfg(feature = "fuzzing")]
fn generate_raw_document(u: &mut arbitrary::Unstructured) -> arbitrary::Result<RawDocumentBuf> {
    let doc = FuzzRawDocumentImpl::arbitrary(u)?;
    Ok(doc.into())
}

#[cfg(feature = "fuzzing")]
fn generate_document_sequences(
    u: &mut arbitrary::Unstructured,
) -> arbitrary::Result<Vec<DocumentSequence>> {
    let seq = FuzzDocumentSequenceImpl::arbitrary(u)?;
    Ok(vec![DocumentSequence {
        identifier: String::arbitrary(u)?,
        documents: seq.0.into_iter().map(Into::into).collect(),
    }])
}

const DEFAULT_MAX_MESSAGE_SIZE_BYTES: i32 = 48 * 1024 * 1024;

/// Represents an OP_MSG wire protocol operation.
#[derive(Debug, Clone)]
#[cfg(feature = "fuzzing")]
#[cfg_attr(feature = "fuzzing", derive(Arbitrary))]
pub struct Message {
    /// OP_MSG flags
    pub flags: MessageFlags,

    /// OP_MSG payload type 0 (the main document)
    #[cfg_attr(feature = "fuzzing", arbitrary(with = generate_raw_document))]
    pub(crate) document_payload: RawDocumentBuf,

    /// OP_MSG payload type 1 (document sequences)
    #[cfg_attr(feature = "fuzzing", arbitrary(with = generate_document_sequences))]
    pub(crate) document_sequences: Vec<DocumentSequence>,

    /// Optional CRC32C checksum
    pub(crate) checksum: Option<u32>,

    /// Request ID for the message
    pub(crate) request_id: Option<i32>,

    /// Response to request ID
    pub response_to: i32,

    /// Whether the message should be compressed
    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    pub(crate) should_compress: bool,
}

/// Represents an OP_MSG wire protocol operation.
#[derive(Debug, Clone)]
#[cfg(not(feature = "fuzzing"))]
pub(crate) struct Message {
    /// OP_MSG flags
    pub flags: MessageFlags,

    /// OP_MSG payload type 0 (the main document)
    #[cfg_attr(feature = "fuzzing", arbitrary(with = generate_raw_document))]
    pub(crate) document_payload: RawDocumentBuf,

    /// OP_MSG payload type 1 (document sequences)
    #[cfg_attr(feature = "fuzzing", arbitrary(with = generate_document_sequences))]
    pub(crate) document_sequences: Vec<DocumentSequence>,

    /// Optional CRC32C checksum
    pub(crate) checksum: Option<u32>,

    /// Request ID for the message
    pub(crate) request_id: Option<i32>,

    /// Response to request ID
    pub response_to: i32,

    /// Whether the message should be compressed
    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    pub(crate) should_compress: bool,
}

#[derive(Clone, Debug)]
#[cfg(feature = "fuzzing")]
pub struct DocumentSequence {
    pub identifier: String,
    pub documents: Vec<RawDocumentBuf>,
}

#[derive(Clone, Debug)]
#[cfg(not(feature = "fuzzing"))]
pub(crate) struct DocumentSequence {
    pub(crate) identifier: String,
    pub(crate) documents: Vec<RawDocumentBuf>,
}

/// Creates a Message from a Command. The response_to and request_id fields must be set manually.
impl TryFrom<Command> for Message {
    type Error = Error;

    fn try_from(command: Command) -> Result<Self> {
        let document_payload = bson::to_raw_document_buf(&command)?;
        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        let should_compress = command.should_compress();

        let mut flags = MessageFlags::empty();
        if command.exhaust_allowed {
            flags |= MessageFlags::EXHAUST_ALLOWED;
        }

        Ok(Self {
            document_payload,
            document_sequences: command
                .document_sequences
                .into_iter()
                .map(Into::into)
                .collect(),
            response_to: 0,
            flags,
            checksum: None,
            request_id: None,
            #[cfg(any(
                feature = "zstd-compression",
                feature = "zlib-compression",
                feature = "snappy-compression"
            ))]
            should_compress,
        })
    }
}

#[cfg(feature = "fuzzing")]
impl From<FuzzDocumentSequenceImpl> for Vec<DocumentSequence> {
    fn from(seq: FuzzDocumentSequenceImpl) -> Self {
        vec![DocumentSequence {
            identifier: "documents".to_string(),
            documents: seq.0.into_iter().map(Into::into).collect(),
        }]
    }
}

impl Message {
    #[cfg(feature = "fuzzing")]
    pub fn read_from_slice(data: &[u8], header: Header) -> Result<Self> {
        // this case should not actually be hit during fuzzing since we create the Header
        // from the data first, but this is a good sanity check, ensuring that we will
        // not panic if we do hit this case.
        if data.len() < Header::LENGTH {
            return Err(ErrorKind::InvalidResponse {
                message: format!("Message data too short: {} bytes", data.len()),
            }
            .into());
        }
        let data = &data[Header::LENGTH..];
        if header.op_code == OpCode::Message {
            return Self::read_op_common(data, data.len(), &header);
        }
        Err(Error::new(
            ErrorKind::InvalidResponse {
                message: format!(
                    "Invalid op code for fuzzing, expected {} but got {}",
                    OpCode::Message as u32,
                    header.op_code as u32
                ),
            },
            Option::<Vec<String>>::None,
        ))
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
            return Self::read_op_compressed_from(reader, &header).await;
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

    async fn read_op_compressed_from<T: AsyncRead + Unpin + Send>(
        mut reader: T,
        header: &Header,
    ) -> Result<Self> {
        use crate::runtime::SyncLittleEndianRead;

        let length = Checked::<usize>::try_from(header.length)?;
        let length_remaining = length - Header::LENGTH;
        let mut buffer = vec![0u8; length_remaining.get()?];
        reader.read_exact(&mut buffer).await?;
        let mut compressed = buffer.as_slice();

        let original_opcode = compressed.read_i32_sync()?;
        if original_opcode != OpCode::Message as i32 {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The original opcode of the compressed message must be {}, but was {}",
                    OpCode::Message as i32,
                    original_opcode,
                ),
            }
            .into());
        }

        let uncompressed_size = Checked::<usize>::try_from(compressed.read_i32_sync()?)?;
        let compressor_id: u8 = compressed.read_u8_sync()?;
        let decompressed = decompress_message(compressed, compressor_id)?;

        if decompressed.len() != uncompressed_size.get()? {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server's message claims that the uncompressed length is {}, but was \
                     computed to be {}.",
                    uncompressed_size,
                    decompressed.len(),
                ),
            }
            .into());
        }

        // Read decompressed message as a standard OP_MSG.
        let reader = decompressed.as_slice();
        let length_remaining = decompressed.len();

        Self::read_op_common(reader, length_remaining, header)
    }

    fn read_op_common(mut reader: &[u8], length_remaining: usize, header: &Header) -> Result<Self> {
        use crate::runtime::SyncLittleEndianRead;
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
            #[cfg(any(
                feature = "zstd-compression",
                feature = "zlib-compression",
                feature = "snappy-compression"
            ))]
            should_compress: false,
        })
    }

    /// Serializes this message into an OP_MSG and writes it to the provided writer.
    pub(crate) async fn write_op_msg_to<T: AsyncWrite + Send + Unpin>(
        &self,
        mut writer: T,
    ) -> Result<()> {
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

    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    /// Serializes this message into an OP_COMPRESSED message and writes it to the provided writer.
    pub(crate) async fn write_op_compressed_to<T: AsyncWrite + Unpin + Send>(
        &self,
        mut writer: T,
        compressor: &Compressor,
    ) -> Result<()> {
        let flag_bytes = &self.flags.bits().to_le_bytes();
        let section_bytes = self.get_sections_bytes()?;
        let uncompressed_len = Checked::new(section_bytes.len()) + flag_bytes.len();

        let compressed_bytes = compressor.compress(flag_bytes, &section_bytes)?;

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

        header.write_to(&mut writer).await?;
        writer.write_i32_le(OpCode::Message as i32).await?;
        writer.write_i32_le(uncompressed_len.try_into()?).await?;
        writer.write_u8(compressor.id()).await?;
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

/// Represents a section as defined by the OP_MSG spec.
#[derive(Debug)]
enum MessageSection {
    Document(RawDocumentBuf),
    Sequence(DocumentSequence),
}

impl MessageSection {
    /// Reads bytes from `reader` and deserializes them into a MessageSection.
    fn read<R: Read>(reader: &mut SyncCountReader<R>) -> Result<Self> {
        use crate::runtime::SyncLittleEndianRead;
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

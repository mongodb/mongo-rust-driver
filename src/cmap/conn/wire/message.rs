use std::io::Read;

use bitflags::bitflags;
use futures_io::AsyncWrite;
use futures_util::{
    io::{BufReader, BufWriter},
    AsyncReadExt,
    AsyncWriteExt,
};

use super::header::{Header, OpCode};
use crate::{
    bson_util,
    cmap::{
        conn::{command::RawCommand, wire::util::SyncCountReader},
        Command,
    },
    error::{Error, ErrorKind, Result},
    runtime::{AsyncLittleEndianWrite, AsyncStream, SyncLittleEndianRead},
};

/// Represents an OP_MSG wire protocol operation.
#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) response_to: i32,
    pub(crate) flags: MessageFlags,
    pub(crate) sections: Vec<MessageSection>,
    pub(crate) checksum: Option<u32>,
    pub(crate) request_id: Option<i32>,
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

        // TODO: RUST-616 ensure length is < maxMessageSizeBytes
        let mut length_remaining = header.length - Header::LENGTH as i32;
        let mut buf = vec![0u8; length_remaining as usize];
        reader.read_exact(&mut buf).await?;
        let mut reader = buf.as_slice();

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
    pub(crate) async fn write_to(&self, stream: &mut AsyncStream) -> Result<()> {
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
}

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
pub(crate) enum MessageSection {
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

        let mut identifier = String::new();
        length_remaining -= reader.read_to_string(&mut identifier)? as i32;

        let mut documents = Vec::new();
        let mut count_reader = SyncCountReader::new(reader);

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

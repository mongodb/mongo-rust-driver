use std::io::{Read, Write};

use bitflags::bitflags;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    header::{Header, OpCode},
    util::CountReader,
};
use crate::{
    cmap::conn::command::Command,
    error::{Error, ErrorKind, Result},
};

/// Represents an OP_MSG wire protocol operation.
#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) response_to: i32,
    pub(crate) flags: MessageFlags,
    pub(crate) sections: Vec<MessageSection>,
    pub(crate) checksum: Option<u32>,
}

impl Message {
    /// Creates a `Message` from a given `Command`.
    ///
    /// Note that `response_to` will need to be set manually.
    pub(crate) fn from_command(mut command: Command) -> Self {
        command.body.insert("$db", command.target_db);

        if let Some(read_pref) = command.read_pref {
            command
                .body
                .insert("$readPreference", read_pref.into_document());
        };

        Self {
            response_to: 0,
            flags: MessageFlags::empty(),
            sections: vec![MessageSection::Document(command.body)],
            checksum: None,
        }
    }

    /// Creates a Message with a single section containing `document`.
    ///
    /// Note that `response_to` will need to be set manually, as well as any non-default flags.
    pub(crate) fn from_document(document: Document) -> Self {
        Self {
            response_to: 0,
            flags: MessageFlags::empty(),
            sections: vec![MessageSection::Document(document)],
            checksum: None,
        }
    }

    /// Gets the first document contained in this Message.
    pub(crate) fn single_document_response(self) -> Result<Document> {
        self.sections
            .into_iter()
            .next()
            .and_then(|section| match section {
                MessageSection::Document(doc) => Some(doc),
                MessageSection::Sequence { documents, .. } => documents.into_iter().next(),
            })
            .ok_or_else(|| {
                Error::from_kind(ErrorKind::ResponseError(
                    "no response received from server".into(),
                ))
            })
    }

    /// Gets all documents contained in this Message flattened to a single Vec.
    pub(crate) fn documents(self) -> Vec<Document> {
        self.sections
            .into_iter()
            .flat_map(|section| match section {
                MessageSection::Document(doc) => vec![doc],
                MessageSection::Sequence { documents, .. } => documents,
            })
            .collect()
    }

    /// Reads bytes from `reader` and deserializes them into a Message.
    pub(crate) fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let header = Header::read_from(reader)?;
        let mut length_remaining = header.length - Header::LENGTH as i32;

        let flags = MessageFlags::from_bits_truncate(reader.read_u32::<LittleEndian>()?);
        length_remaining -= std::mem::size_of::<u32>() as i32;

        let mut count_reader = CountReader::new(reader);
        let mut sections = Vec::new();

        while length_remaining - count_reader.bytes_read() as i32 > 4 {
            sections.push(MessageSection::read(&mut count_reader)?);
        }

        length_remaining -= count_reader.bytes_read() as i32;

        let mut checksum = None;

        if length_remaining == 4 && flags.contains(MessageFlags::CHECKSUM_PRESENT) {
            checksum = Some(reader.read_u32::<LittleEndian>()?);
        } else if length_remaining != 0 {
            bail!(ErrorKind::OperationError(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                header.length,
                header.length - length_remaining + count_reader.bytes_read() as i32,
            )));
        }

        Ok(Self {
            response_to: header.response_to,
            flags,
            sections,
            checksum,
        })
    }

    /// Serializes the Message to bytes and writes them to `writer`.
    pub(crate) fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut sections_bytes = Vec::new();

        for section in &self.sections {
            section.write(&mut sections_bytes)?;
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
            request_id: super::util::next_request_id(),
            response_to: self.response_to,
            op_code: OpCode::Message,
        };

        header.write_to(writer)?;
        writer.write_u32::<LittleEndian>(self.flags.bits())?;
        writer.write_all(&sections_bytes)?;

        if let Some(checksum) = self.checksum {
            writer.write_u32::<LittleEndian>(checksum)?;
        }

        writer.flush()?;

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
    Document(Document),
    Sequence {
        size: i32,
        identifier: String,
        documents: Vec<Document>,
    },
}

impl MessageSection {
    /// Reads bytes from `reader` and deserializes them into a MessageSection.
    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let payload_type = reader.read_u8()?;

        if payload_type == 0 {
            return Ok(MessageSection::Document(bson::decode_document(reader)?));
        }

        let size = reader.read_i32::<LittleEndian>()?;
        let mut length_remaining = size - std::mem::size_of::<i32>() as i32;

        let mut identifier = String::new();
        length_remaining -= reader.read_to_string(&mut identifier)? as i32;

        let mut documents = Vec::new();
        let mut count_reader = CountReader::new(reader);

        while length_remaining - count_reader.bytes_read() as i32 > 0 {
            documents.push(bson::decode_document(&mut count_reader)?);
        }

        if length_remaining - count_reader.bytes_read() as i32 != 0 {
            bail!(ErrorKind::OperationError(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                size,
                size - length_remaining + count_reader.bytes_read() as i32,
            )));
        }

        Ok(MessageSection::Sequence {
            size,
            identifier,
            documents,
        })
    }

    /// Serializes the MessageSection to bytes and writes them to `writer`.
    fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Self::Document(doc) => {
                // Write payload type.
                writer.write_u8(0)?;

                bson::encode_document(writer, doc)?;
            }
            Self::Sequence {
                size,
                identifier,
                documents,
            } => {
                // Write payload type.
                writer.write_u8(1)?;

                writer.write_i32::<LittleEndian>(*size)?;
                super::util::write_cstring(writer, identifier)?;

                for doc in documents {
                    bson::encode_document(writer, doc)?;
                }
            }
        }

        Ok(())
    }
}

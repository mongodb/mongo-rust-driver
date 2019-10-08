use std::io::Read;

use bitflags::bitflags;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt};

use super::{header::Header, util::CountReader};
use crate::error::{ErrorKind, Result};

/// Represents a wire protocol OP_REPLY operation.
#[derive(Debug)]
pub(crate) struct Reply {
    header: Header,
    response_flags: ResponseFlags,
    pub(crate) cursor_id: i64,
    starting_from: i32,
    num_returned: i32,
    pub(crate) docs: Vec<Document>,
}

bitflags! {
    /// Represents the bitwise flags for an OP_REPLY.
    pub(crate) struct ResponseFlags: u32 {
        const CURSOR_NOT_FOUND = 0b_0000_0000_0000_0000_0000_0000_0000_0001;
        const QUERY_FAILURE    = 0b_0000_0000_0000_0000_0000_0000_0000_0010;
        const AWAIT_CAPABLE    = 0b_0000_0000_0000_0000_0000_0000_0000_0100;
    }
}

impl Reply {
    /// Reads bytes from `reader` and deserializes them into a Reply.
    pub(crate) fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let header = Header::read_from(reader)?;

        let response_flags = ResponseFlags::from_bits_truncate(reader.read_u32::<LittleEndian>()?);
        let cursor_id = reader.read_i64::<LittleEndian>()?;
        let starting_from = reader.read_i32::<LittleEndian>()?;
        let num_returned = reader.read_i32::<LittleEndian>()?;

        let length_remaining = header.length
            - Header::LENGTH as i32
            - std::mem::size_of::<u32>() as i32  // flags
            - std::mem::size_of::<i64>() as i32  // cursor id
            - std::mem::size_of::<i32>() as i32  // starting_from
            - std::mem::size_of::<i32>() as i32; // num_returned

        let mut r = CountReader::new(reader);
        let mut docs = Vec::new();

        while length_remaining as usize > r.bytes_read() {
            docs.push(bson::decode_document(&mut r)?);
        }

        if length_remaining as usize != r.bytes_read() {
            return Err(ErrorKind::OperationError {
                message: format!(
                    "The server indicated that the reply would be {} bytes long, but it instead \
                     was {}",
                    header.length,
                    header.length - length_remaining + r.bytes_read() as i32,
                ),
            }
            .into());
        }

        Ok(Self {
            header,
            response_flags,
            cursor_id,
            starting_from,
            num_returned,
            docs,
        })
    }
}

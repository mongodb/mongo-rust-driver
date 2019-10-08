use std::io::{Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::error::{ErrorKind, Result};

/// The wire protocol op codes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum OpCode {
    Reply = 1,
    Query = 2004,
    Message = 2013,
}

impl OpCode {
    /// Attempt to infer the op code based on the numeric value.
    fn from_i32(i: i32) -> Result<Self> {
        match i {
            1 => Ok(OpCode::Reply),
            2004 => Ok(OpCode::Query),
            2013 => Ok(OpCode::Message),
            other => Err(ErrorKind::OperationError {
                message: format!("Invalid wire protocol opcode: {}", other),
            }
            .into()),
        }
    }
}

/// The header for any wire protocol message.
#[derive(Debug)]
pub(crate) struct Header {
    pub length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: OpCode,
}

impl Header {
    pub(crate) const LENGTH: usize = 4 * std::mem::size_of::<i32>();

    /// Serializes the Header and writes the bytes to `w`.
    pub(crate) fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_i32::<LittleEndian>(self.length)?;
        writer.write_i32::<LittleEndian>(self.request_id)?;
        writer.write_i32::<LittleEndian>(self.response_to)?;
        writer.write_i32::<LittleEndian>(self.op_code as i32)?;

        Ok(())
    }

    /// Reads bytes from `r` and deserializes them into a header.
    pub(crate) fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        Ok(Self {
            length: reader.read_i32::<LittleEndian>()?,
            request_id: reader.read_i32::<LittleEndian>()?,
            response_to: reader.read_i32::<LittleEndian>()?,
            op_code: OpCode::from_i32(reader.read_i32::<LittleEndian>()?)?,
        })
    }
}

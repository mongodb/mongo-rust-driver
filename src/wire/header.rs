use std::{
    io::{Read, Write},
    sync::atomic::{AtomicIsize, Ordering},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::error::{ErrorKind, Result};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OpCode {
    Reply = 1,
    Query = 2004,
}

impl OpCode {
    fn from_i32(i: i32) -> Result<Self> {
        match i {
            1 => Ok(OpCode::Reply),
            2004 => Ok(OpCode::Query),
            other => bail!(ErrorKind::OperationError(format!(
                "Invalid wire protocol opcode: {}",
                other
            ))),
        }
    }
}

#[derive(Debug)]
pub struct Header {
    pub length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub opcode: OpCode,
}

impl Header {
    pub const LENGTH: i32 = (32 / 8) * 4;

    pub fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_i32::<LittleEndian>(self.length)?;
        w.write_i32::<LittleEndian>(self.request_id)?;
        w.write_i32::<LittleEndian>(self.response_to)?;
        w.write_i32::<LittleEndian>(self.opcode as i32)?;

        Ok(())
    }

    pub fn read<R: Read>(r: &mut R) -> Result<Self> {
        Ok(Self {
            length: r.read_i32::<LittleEndian>()?,
            request_id: r.read_i32::<LittleEndian>()?,
            response_to: r.read_i32::<LittleEndian>()?,
            opcode: OpCode::from_i32(r.read_i32::<LittleEndian>()?)?,
        })
    }
}

pub fn new_request_id() -> i32 {
    lazy_static! {
        static ref CURRENT_REQUEST_ID: AtomicIsize = AtomicIsize::new(0);
    }

    CURRENT_REQUEST_ID.fetch_add(1, Ordering::SeqCst) as i32
}

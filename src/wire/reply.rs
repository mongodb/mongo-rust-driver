use std::io::{Read, Write};

use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    doc_reader::DocReader,
    flags::{ResponseFlags, RESPONSE_FLAGS_LENGTH},
    header::Header,
};
use crate::error::{ErrorKind, Result};

#[derive(Debug)]
pub struct Reply {
    header: Header,
    response_flags: ResponseFlags,
    pub cursor_id: i64,
    starting_from: i32,
    num_returned: i32,
    pub docs: Vec<Document>,
}

impl Reply {
    pub fn read<R: Read>(r: &mut R) -> Result<Self> {
        let header = Header::read(r)?;

        let response_flags = ResponseFlags::from_bits_truncate(r.read_i32::<LittleEndian>()?);
        let cursor_id = r.read_i64::<LittleEndian>()?;
        let starting_from = r.read_i32::<LittleEndian>()?;
        let num_returned = r.read_i32::<LittleEndian>()?;

        let length_remaining =
            header.length - Header::LENGTH - RESPONSE_FLAGS_LENGTH - (64 / 8) - (32 / 8) - (32 / 8);

        let mut r = DocReader::new(r);
        let mut docs = Vec::new();

        while length_remaining as usize > r.bytes_read() {
            docs.push(bson::decode_document(&mut r)?);
        }

        if length_remaining as usize != r.bytes_read() {
            bail!(ErrorKind::OperationError(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                header.length,
                header.length - length_remaining + r.bytes_read() as i32,
            )));
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

    #[allow(dead_code)]
    pub fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        self.header.write(w)?;
        w.write_i32::<LittleEndian>(self.response_flags.bits())?;
        w.write_i64::<LittleEndian>(self.cursor_id)?;
        w.write_i32::<LittleEndian>(self.starting_from)?;
        w.write_i32::<LittleEndian>(self.num_returned)?;

        for doc in &self.docs {
            bson::encode_document(w, doc)?;
        }

        Ok(())
    }
}

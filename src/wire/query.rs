use std::io::{Read, Write};

use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    doc_reader::DocReader,
    flags::{QueryFlags, QUERY_FLAGS_LENGTH},
    header::Header,
};
use crate::error::{ErrorKind, Result};

#[derive(Debug)]
pub struct Query {
    pub header: Header,
    pub flags: QueryFlags,
    pub full_collection_name: String,
    pub num_to_skip: i32,
    pub num_to_return: i32,
    pub query: Document,
    pub return_field_selector: Option<Document>,
}

impl Query {
    #[allow(dead_code)]
    pub fn read<R: Read>(r: &mut R) -> Result<Self> {
        let header = Header::read(r)?;
        let flags = QueryFlags::from_bits_truncate(r.read_i32::<LittleEndian>()?);
        let mut full_collection_name = String::new();
        let _ = r.read_to_string(&mut full_collection_name)?;
        let num_to_skip = r.read_i32::<LittleEndian>()?;
        let num_to_return = r.read_i32::<LittleEndian>()?;

        let length_remaining =
            header.length - Header::LENGTH - QUERY_FLAGS_LENGTH - (32 / 8) - (32 / 8);

        let mut r = DocReader::new(r);
        let query = bson::decode_document(&mut r)?;

        let return_field_selector = if (length_remaining as usize) > r.bytes_read() {
            Some(bson::decode_document(&mut r)?)
        } else {
            None
        };

        if length_remaining as usize > r.bytes_read() {
            bail!(ErrorKind::OperationError(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                header.length,
                header.length - length_remaining + r.bytes_read() as i32,
            )));
        }

        if (length_remaining as usize) < r.bytes_read() {
            bail!(ErrorKind::OperationError(format!(
                "The server indicated that the reply would be {} bytes long, but it instead was {}",
                header.length,
                header.length - length_remaining + r.bytes_read() as i32,
            )));
        }

        Ok(Self {
            header,
            flags,
            full_collection_name,
            num_to_skip,
            num_to_return,
            query,
            return_field_selector,
        })
    }

    pub fn write<W: Write>(&self, w: &mut W) -> Result<()> {
        self.header.write(w)?;
        w.write_i32::<LittleEndian>(self.flags.bits())?;
        w.write_all(self.full_collection_name.as_bytes())?;
        let _ = w.write(&[0])?; // null-terminator for full_collection_name
        w.write_i32::<LittleEndian>(self.num_to_skip)?;
        w.write_i32::<LittleEndian>(self.num_to_return)?;

        bson::encode_document(w, &self.query)?;

        if let Some(ref return_field_selector) = self.return_field_selector {
            bson::encode_document(w, return_field_selector)?;
        }

        Ok(())
    }
}

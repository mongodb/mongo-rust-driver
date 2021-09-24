use flate2::Compression;
use flate2::write::{ZlibEncoder, ZlibDecoder};
use std::convert::TryInto;
use std::io::prelude::*;
use crate::error::Result;

#[derive(Clone, Debug)]
pub(crate) enum Compressor {
    Zstd(u32),
    Zlib(u32),
    Snappy,
}

impl Compressor {
    pub fn encode(&self, buf: &[u8]) -> Result<Vec<u8>> {
        match *self {
            Compressor::Zstd(level) => {
                let mut dest = Vec::new();
                zstd::stream::copy_encode(buf, &mut dest, level.try_into().unwrap_or(6))?;
                Ok(dest)
            }
            Compressor::Zlib(level) => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level));
                encoder.write_all(buf);
                let compressed_bytes = encoder.finish()?;
                Ok(compressed_bytes)
            }
            Compressor::Snappy => {
                panic!("Support for Snappy is unimplemented");
            }
        }
    }

    pub fn decode(&self, compressed_bytes: &[u8]) -> Result<Vec<u8>> {
        match *self {
            Compressor::Zstd(_) => {
                let mut dest = Vec::new();
                zstd::stream::copy_decode(compressed_bytes, &mut dest)?;
                Ok(dest)
            }
            Compressor::Zlib(_) => {
                let mut writer = Vec::new();
                let mut decoder = ZlibDecoder::new(writer);
                decoder.write_all(compressed_bytes);
                let x = decoder.finish()?;
                Ok(x)
            }
            Compressor::Snappy => {
                panic!("Support for Snappy is unimplemented");
            }
        }

    }
}

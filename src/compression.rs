use flate2::Compression;
use flate2::write::{ZlibEncoder, ZlibDecoder};
use std::convert::TryInto;
use std::io::prelude::*;
use crate::error::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub(crate) enum Compressor {
    Zstd(u32),
    Zlib(u32),
    Snappy,
}
pub(crate) trait FinishCompression: Write {
    fn finish_compression(self: Box<Self>) -> Result<Vec<u8>>;
}

impl FinishCompression for zstd::Encoder<'_, Vec<u8>> {
    fn finish_compression(self: Box<Self>) -> Result<Vec<u8>> {
        self.finish().map_err(|e| { Error::new(
            ErrorKind::Compression {
                message: format!("an error occured finishing Zstd compression {}", e),
            },
            Option::<Vec<String>>::None,
        ) } )
    }
}

impl FinishCompression for ZlibEncoder<Vec<u8>> {
    fn finish_compression(self: Box<Self>) -> Result<Vec<u8>> {
        self.finish().map_err(|e| { Error::new(
            ErrorKind::Compression {
                message: format!("an error occured finishing Zlib compression {}", e),
            },
            Option::<Vec<String>>::None,
        ) } )
    }
}

impl FinishCompression for snap::write::FrameEncoder<Vec<u8>> {
    fn finish_compression(self: Box<Self>) -> Result<Vec<u8>> {
        self.into_inner().map_err(|e| { Error::new(
            ErrorKind::Compression {
                message: format!("an error occured finishing Snappy compression {}", e),
            },
            Option::<Vec<String>>::None,
        ) } )
    }
}

impl Compressor {
    pub fn to_writer(&self) -> Result<Box<dyn FinishCompression>> {
        match *self {
            Compressor::Zstd(level) => {
                let mut x = zstd::Encoder::new(vec![], level.try_into().unwrap()).map_err(|e| { Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured getting a new zstd encoder {}", e),
                    },
                    Option::<Vec<String>>::None,
                ) } )?;
                Ok(Box::new(x))
            }
            Compressor::Zlib(level) => {
                let mut x = ZlibEncoder::new(vec![], Compression::new(level));
                Ok(Box::new(x))
            },
            Compressor::Snappy => {
                let mut x = snap::write::FrameEncoder::new(vec![]);
                Ok(Box::new(x))
            },
        }
    }
}

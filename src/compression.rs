use crate::error::{Error, ErrorKind, Result};
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use std::{convert::TryInto, io::prelude::*};

const NOOP_COMPRESSOR_ID: u8 = 0;
const SNAPPY_ID: u8 = 1;
const ZLIB_ID: u8 = 2;
const ZSTD_ID: u8 = 3;

#[derive(Clone, Debug)]
pub(crate) enum Compressor {
    Zstd(u32),
    Zlib(u32),
    Snappy,
}

impl Compressor {
    pub(crate) fn to_compressor_id(&self) -> u8 {
        match *self {
            Compressor::Zstd(_) => ZSTD_ID,
            Compressor::Zlib(_) => ZLIB_ID,
            Compressor::Snappy => SNAPPY_ID,
        }
    }

    pub(crate) fn to_encoder(&self) -> Result<Encoder> {
        match *self {
            Compressor::Zstd(level) => {
                let encoder =
                    zstd::Encoder::new(vec![], level.try_into().unwrap()).map_err(|e| {
                        Error::new(
                            ErrorKind::Compression {
                                message: format!(
                                    "an error occured getting a new zstd encoder: {}",
                                    e
                                ),
                            },
                            Option::<Vec<String>>::None,
                        )
                    })?;

                Ok(Encoder::Zstd { encoder })
            }
            Compressor::Zlib(level) => {
                let encoder = ZlibEncoder::new(vec![], Compression::new(level));
                Ok(Encoder::Zlib { encoder })
            }
            Compressor::Snappy => {
                let encoder = snap::write::FrameEncoder::new(vec![]);
                Ok(Encoder::Snappy { encoder })
            }
        }
    }
}

pub(crate) enum Encoder {
    Zstd {
        encoder: zstd::Encoder<'static, Vec<u8>>,
    },
    Zlib {
        encoder: ZlibEncoder<Vec<u8>>,
    },
    Snappy {
        encoder: snap::write::FrameEncoder<Vec<u8>>,
    },
}

impl Encoder {
    pub(crate) fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match *self {
            Encoder::Zstd { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured writing to the zstd encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
            Encoder::Zlib { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured writing to the zlib encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
            Encoder::Snappy { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured writing to the snappy encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
        }
    }

    pub(crate) fn finish(self) -> Result<Vec<u8>> {
        match self {
            Encoder::Zstd { encoder } => encoder.finish().map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured finishing zstd encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
            Encoder::Zlib { encoder } => encoder.finish().map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured finishing zlib encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
            Encoder::Snappy { encoder } => encoder.into_inner().map_err(|e| {
                Error::new(
                    ErrorKind::Compression {
                        message: format!("an error occured finishing snappy encoder: {}", e),
                    },
                    Option::<Vec<String>>::None,
                )
            }),
        }
    }
}

pub(crate) enum Decoder {
    Zstd,
    Zlib,
    Snappy,
    Noop,
}

impl Decoder {
    pub(crate) fn decode(self, source: &[u8]) -> Result<Vec<u8>> {
        match self {
            Decoder::Zstd => {
                let mut ret = Vec::new();
                zstd::stream::copy_decode(source, &mut ret).map_err(|e| {
                    Error::new(
                        ErrorKind::Compression {
                            message: format!("Could not decode using zstd decoder: {}", e),
                        },
                        Option::<Vec<String>>::None,
                    )
                })?;
                Ok(ret)
            }
            Decoder::Zlib => {
                let mut decoder = ZlibDecoder::new(vec![]);
                decoder.write_all(source)?;
                let output = decoder.finish()?;
                Ok(output)
            }
            Decoder::Snappy => {
                let mut bytes = Vec::new();
                snap::read::FrameDecoder::new(source).read_to_end(&mut bytes)?;
                Ok(bytes)
            }
            Decoder::Noop => Ok(source.to_vec()),
        }
    }

    pub(crate) fn from_compressor_id(id: u8) -> Result<Self> {
        match id {
            NOOP_COMPRESSOR_ID => Ok(Decoder::Noop),
            SNAPPY_ID => Ok(Decoder::Snappy),
            ZLIB_ID => Ok(Decoder::Zlib),
            ZSTD_ID => Ok(Decoder::Zstd),
            _ => Err(Error::new(
                ErrorKind::Compression {
                    message: format!(
                        "Received invalid compressor id.  Expected {}, {}, or {}, got {}",
                        SNAPPY_ID, ZLIB_ID, ZSTD_ID, id
                    ),
                },
                Option::<Vec<String>>::None,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::compression::{Compressor, Decoder, SNAPPY_ID, ZLIB_ID, ZSTD_ID};

    #[test]
    fn test_zlib_compressor() {
        let zlib_compressor = Compressor::Zlib(4);
        assert_eq!(ZLIB_ID, zlib_compressor.to_compressor_id());
        let mut encoder = zlib_compressor.to_encoder().unwrap();
        encoder.write_all(b"foo");
        encoder.write_all(b"bar");
        encoder.write_all(b"ZLIB");

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_compressor_id(ZLIB_ID).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarZLIB", original_bytes.as_slice());
    }

    #[test]
    fn test_zstd_compressor() {
        let zstd_compressor = Compressor::Zstd(0);
        assert_eq!(ZSTD_ID, zstd_compressor.to_compressor_id());
        let mut encoder = zstd_compressor.to_encoder().unwrap();
        encoder.write_all(b"foo");
        encoder.write_all(b"bar");
        encoder.write_all(b"ZSTD");

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_compressor_id(ZSTD_ID).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarZSTD", original_bytes.as_slice());
    }

    #[test]
    fn test_snappy_compressor() {
        let snappy_compressor = Compressor::Snappy;
        assert_eq!(SNAPPY_ID, snappy_compressor.to_compressor_id());
        let mut encoder = snappy_compressor.to_encoder().unwrap();
        encoder.write_all(b"foo");
        encoder.write_all(b"bar");
        encoder.write_all(b"SNAPPY");

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_compressor_id(SNAPPY_ID).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarSNAPPY", original_bytes.as_slice());
    }
}

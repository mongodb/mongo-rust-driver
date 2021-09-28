use crate::error::{Error, ErrorKind, Result};
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use std::{convert::TryInto, io::prelude::*};

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub(crate) enum CompressorID {
    NoopID = 0,
    SnappyID = 1,
    ZlibID = 2,
    ZstdID = 3,
}

impl CompressorID {
    pub(crate) fn from_u8(id: u8) -> Result<Self> {
        match id {
            0 => Ok(CompressorID::NoopID),
            1 => Ok(CompressorID::SnappyID),
            2 => Ok(CompressorID::ZlibID),
            3 => Ok(CompressorID::ZstdID),
            other => Err(ErrorKind::InvalidResponse {
                message: format!("Invalid wire protocol compressor id: {}", other),
            }
            .into()),
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub(crate) enum Compressor {
    Zstd(u32),
    Zlib(u32),
    Snappy,
}

impl Compressor {
    pub(crate) fn to_compressor_id(&self) -> CompressorID {
        match *self {
            Compressor::Zstd(_) => CompressorID::ZstdID,
            Compressor::Zlib(_) => CompressorID::ZlibID,
            Compressor::Snappy => CompressorID::SnappyID,
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
            Compressor::Snappy => Ok(Encoder::Snappy { bytes: vec![] }),
        }
    }
}

#[non_exhaustive]
pub(crate) enum Encoder {
    Zstd {
        encoder: zstd::Encoder<'static, Vec<u8>>,
    },
    Zlib {
        encoder: ZlibEncoder<Vec<u8>>,
    },
    Snappy {
        bytes: Vec<u8>,
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
            Encoder::Snappy { ref mut bytes } => bytes.write_all(buf).map_err(|e| {
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
            Encoder::Snappy { bytes } => {
                // The server doesn't use snappy frame format, so we need to use snap::raw::Encoder
                // rather than snap::write::FrameEncoder.  Likewise for decoding.
                let mut compressor = snap::raw::Encoder::new();
                compressor.compress_vec(bytes.as_slice()).map_err(|e| {
                    Error::new(
                        ErrorKind::Compression {
                            message: format!("an error occured finishing snappy encoder: {}", e),
                        },
                        Option::<Vec<String>>::None,
                    )
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
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
                decoder.finish().map_err(|e| {
                    Error::new(
                        ErrorKind::Compression {
                            message: format!("Could not decode using zlib decoder: {}", e),
                        },
                        Option::<Vec<String>>::None,
                    )
                })
            }
            Decoder::Snappy => {
                let mut decompressor = snap::raw::Decoder::new();
                decompressor.decompress_vec(source).map_err(|e| {
                    Error::new(
                        ErrorKind::Compression {
                            message: format!("Could not decode using snappy decoder: {}", e),
                        },
                        Option::<Vec<String>>::None,
                    )
                })
            }
            Decoder::Noop => Ok(source.to_vec()),
        }
    }

    pub(crate) fn from_u8(id: u8) -> Result<Self> {
        let compressor_id = CompressorID::from_u8(id)?;
        match compressor_id {
            CompressorID::NoopID => Ok(Decoder::Noop),
            CompressorID::SnappyID => Ok(Decoder::Snappy),
            CompressorID::ZlibID => Ok(Decoder::Zlib),
            CompressorID::ZstdID => Ok(Decoder::Zstd),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::compression::{Compressor, CompressorID, Decoder};

    #[test]
    fn test_zlib_compressor() {
        let zlib_compressor = Compressor::Zlib(4);
        assert_eq!(CompressorID::ZlibID, zlib_compressor.to_compressor_id());
        let mut encoder = zlib_compressor.to_encoder().unwrap();
        assert!(encoder.write_all(b"foo").is_ok());
        assert!(encoder.write_all(b"bar").is_ok());
        assert!(encoder.write_all(b"ZLIB").is_ok());

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_u8(CompressorID::ZlibID as u8).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarZLIB", original_bytes.as_slice());
    }

    #[test]
    fn test_zstd_compressor() {
        let zstd_compressor = Compressor::Zstd(0);
        assert_eq!(CompressorID::ZstdID, zstd_compressor.to_compressor_id());
        let mut encoder = zstd_compressor.to_encoder().unwrap();
        assert!(encoder.write_all(b"foo").is_ok());
        assert!(encoder.write_all(b"bar").is_ok());
        assert!(encoder.write_all(b"ZSTD").is_ok());

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_u8(CompressorID::ZstdID as u8).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarZSTD", original_bytes.as_slice());
    }

    #[test]
    fn test_snappy_compressor() {
        let snappy_compressor = Compressor::Snappy;
        assert_eq!(CompressorID::SnappyID, snappy_compressor.to_compressor_id());
        let mut encoder = snappy_compressor.to_encoder().unwrap();
        assert!(encoder.write_all(b"foo").is_ok());
        assert!(encoder.write_all(b"bar").is_ok());
        assert!(encoder.write_all(b"SNAPPY").is_ok());

        let compressed_bytes = encoder.finish().unwrap();

        let decoder = Decoder::from_u8(CompressorID::SnappyID as u8).unwrap();
        let original_bytes = decoder.decode(compressed_bytes.as_slice()).unwrap();
        assert_eq!(b"foobarSNAPPY", original_bytes.as_slice());
    }
}

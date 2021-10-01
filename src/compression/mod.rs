#[cfg(test)]
mod test;

use crate::error::{Error, ErrorKind, Result};
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, io::prelude::*, str::FromStr};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CompressorId {
    Noop = 0,
    Snappy = 1,
    Zlib = 2,
    Zstd = 3,
}

impl CompressorId {
    pub(crate) fn from_u8(id: u8) -> Result<Self> {
        match id {
            0 => Ok(CompressorId::Noop),
            1 => Ok(CompressorId::Snappy),
            2 => Ok(CompressorId::Zlib),
            3 => Ok(CompressorId::Zstd),
            other => Err(ErrorKind::InvalidResponse {
                message: format!("Invalid compressor id: {}", other),
            }
            .into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Compressor {
    Zstd { level: Option<i32> },
    Zlib { level: Option<i32> },
    Snappy,
}

impl FromStr for Compressor {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "zlib" => Ok(Compressor::Zlib { level: None }),
            "zstd" => Ok(Compressor::Zstd { level: None }),
            "snappy" => Ok(Compressor::Snappy),
            other => Err(ErrorKind::InvalidArgument {
                message: format!("Received invalid compressor: {}", other),
            }
            .into()),
        }
    }
}

impl fmt::Display for Compressor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Compressor::Zstd { .. } => write!(f, "zstd"),
            Compressor::Zlib { .. } => write!(f, "zlib"),
            Compressor::Snappy => write!(f, "snappy"),
        }
    }
}

impl<'de> Deserialize<'de> for Compressor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(|e| D::Error::custom(format!("{}", e)))
    }
}

impl Serialize for Compressor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl Compressor {
    pub(crate) fn id(&self) -> CompressorId {
        match *self {
            Compressor::Zstd { level: _ } => CompressorId::Zstd,
            Compressor::Zlib { level: _ } => CompressorId::Zlib,
            Compressor::Snappy => CompressorId::Snappy,
        }
    }

    pub(crate) fn to_encoder(&self) -> Result<Encoder> {
        match *self {
            Compressor::Zstd { level } => {
                let encoder =
                    zstd::Encoder::new(vec![], level.unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL))
                        .map_err(|e| {
                            Error::from(ErrorKind::Internal {
                                message: format!(
                                    "an error occurred getting a new zstd encoder: {}",
                                    e
                                ),
                            })
                        })?;

                Ok(Encoder::Zstd { encoder })
            }
            Compressor::Zlib { level } => {
                let level = match level {
                    Some(level) if level != -1 => Compression::new(level as u32),
                    _ => Compression::default(),
                };
                let encoder = ZlibEncoder::new(vec![], level);
                Ok(Encoder::Zlib { encoder })
            }
            Compressor::Snappy => Ok(Encoder::Snappy { bytes: vec![] }),
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
        bytes: Vec<u8>,
    },
}

impl Encoder {
    pub(crate) fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match *self {
            Encoder::Zstd { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred writing to the zstd encoder: {}", e),
                }
                .into()
            }),
            Encoder::Zlib { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred writing to the zlib encoder: {}", e),
                }
                .into()
            }),
            Encoder::Snappy { ref mut bytes } => bytes.write_all(buf).map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred writing to the snappy encoder: {}", e),
                }
                .into()
            }),
        }
    }

    pub(crate) fn finish(self) -> Result<Vec<u8>> {
        match self {
            Encoder::Zstd { encoder } => encoder.finish().map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred finishing zstd encoder: {}", e),
                }
                .into()
            }),
            Encoder::Zlib { encoder } => encoder.finish().map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred finishing zlib encoder: {}", e),
                }
                .into()
            }),
            Encoder::Snappy { bytes } => {
                // The server doesn't use snappy frame format, so we need to use snap::raw::Encoder
                // rather than snap::write::FrameEncoder.  Likewise for decoding.
                let mut compressor = snap::raw::Encoder::new();
                compressor.compress_vec(bytes.as_slice()).map_err(|e| {
                    ErrorKind::Internal {
                        message: format!("an error occurred finishing snappy encoder: {}", e),
                    }
                    .into()
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
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
                    Error::from(ErrorKind::Internal {
                        message: format!("Could not decode using zstd decoder: {}", e),
                    })
                })?;
                Ok(ret)
            }
            Decoder::Zlib => {
                let mut decoder = ZlibDecoder::new(vec![]);
                decoder.write_all(source)?;
                decoder.finish().map_err(|e| {
                    ErrorKind::Internal {
                        message: format!("Could not decode using zlib decoder: {}", e),
                    }
                    .into()
                })
            }
            Decoder::Snappy => {
                let mut decompressor = snap::raw::Decoder::new();
                decompressor.decompress_vec(source).map_err(|e| {
                    ErrorKind::Internal {
                        message: format!("Could not decode using snappy decoder: {}", e),
                    }
                    .into()
                })
            }
            Decoder::Noop => Ok(source.to_vec()),
        }
    }

    pub(crate) fn from_u8(id: u8) -> Result<Self> {
        let compressor_id = CompressorId::from_u8(id)?;
        match compressor_id {
            CompressorId::Noop => Ok(Decoder::Noop),
            CompressorId::Snappy => Ok(Decoder::Snappy),
            CompressorId::Zlib => Ok(Decoder::Zlib),
            CompressorId::Zstd => Ok(Decoder::Zstd),
        }
    }
}

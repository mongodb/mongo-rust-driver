#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
#[cfg(test)]
mod test;

#[cfg(feature = "zlib-compression")]
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};

#[cfg(feature = "zlib-compression")]
use std::convert::TryInto;
#[cfg(feature = "zstd-compression")]
use std::io::prelude::*;

use crate::error::{Error, ErrorKind, Result};
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CompressorId {
    Noop = 0,
    #[cfg(feature = "snappy-compression")]
    Snappy = 1,
    #[cfg(feature = "zlib-compression")]
    Zlib = 2,
    #[cfg(feature = "zstd-compression")]
    Zstd = 3,
}

impl CompressorId {
    pub(crate) fn from_u8(id: u8) -> Result<Self> {
        match id {
            0 => Ok(CompressorId::Noop),
            #[cfg(feature = "snappy-compression")]
            1 => Ok(CompressorId::Snappy),
            #[cfg(feature = "zlib-compression")]
            2 => Ok(CompressorId::Zlib),
            #[cfg(feature = "zstd-compression")]
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
/// Enum representing supported compressor algorithms.
/// Used for compressing and decompressing messages sent to and read from the server.
/// For compressors that take a `level`, use `None` to indicate the default level.
/// Higher `level` indicates more compression (and slower).
/// Requires one of `zstd-compression`, `zlib-compression`, or `snappy-compression`
/// feature flags.
pub enum Compressor {
    /// Zstd compressor.  Requires Rust version 1.54.
    /// See [`Zstd`](http://facebook.github.io/zstd/zstd_manual.html) for more information
    #[cfg(feature = "zstd-compression")]
    Zstd {
        /// Zstd compression level
        level: Option<i32>,
    },
    /// Zlib compressor.
    /// See [`Zlib`](https://zlib.net/) for more information.
    #[cfg(feature = "zlib-compression")]
    Zlib {
        /// Zlib compression level
        level: Option<i32>,
    },
    /// Snappy compressor.
    /// See [`Snappy`](http://google.github.io/snappy/) for more information.
    #[cfg(feature = "snappy-compression")]
    Snappy,
}

impl<'de> Deserialize<'de> for Compressor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Compressor::to_compressor(&s).map_err(|e| D::Error::custom(format!("{}", e)))
    }
}

impl Serialize for Compressor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_variant_string().serialize(serializer)
    }
}

impl Compressor {
    #[allow(unused_variables)]
    pub(crate) fn write_zlib_level(&mut self, level: i32) {
        #[cfg(feature = "zlib-compression")]
        if let Compressor::Zlib {
            level: ref mut zlib_level,
        } = *self
        {
            *zlib_level = if level == -1 { None } else { Some(level) }
        }
    }

    pub(crate) fn to_compressor(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            #[cfg(feature = "zlib-compression")]
            "zlib" => Ok(Compressor::Zlib { level: None }),
            #[cfg(feature = "zstd-compression")]
            "zstd" => Ok(Compressor::Zstd { level: None }),
            #[cfg(feature = "snappy-compression")]
            "snappy" => Ok(Compressor::Snappy),
            other => Err(Error::from(ErrorKind::InvalidArgument {
                message: format!("Invalid compressor: {} was supplied but is invalid", other),
            })),
        }
    }

    pub(crate) fn to_variant_string(&self) -> String {
        match *self {
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { .. } => "zstd".to_string(),
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { .. } => "zlib".to_string(),
            #[cfg(feature = "snappy-compression")]
            Compressor::Snappy => "snappy".to_string(),
        }
    }

    pub(crate) fn id(&self) -> CompressorId {
        match *self {
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { level: _ } => CompressorId::Zstd,
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level: _ } => CompressorId::Zlib,
            #[cfg(feature = "snappy-compression")]
            Compressor::Snappy => CompressorId::Snappy,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        #[allow(unreachable_patterns)]
        match *self {
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { level: Some(level) }
                if !zstd::compression_level_range().contains(&level) =>
            {
                Err(Error::from(ErrorKind::InvalidArgument {
                    message: format!("invalid zstd level: {}", level),
                }))
            }
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level: Some(level) } if !(-1..10).contains(&level) => {
                Err(Error::from(ErrorKind::InvalidArgument {
                    message: format!("invalid zlib level: {}", level),
                }))
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn to_encoder(&self) -> Result<Encoder> {
        match *self {
            #[cfg(feature = "zstd-compression")]
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
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level } => {
                let level = match level {
                    Some(level) if level != -1 => {
                        Compression::new(level.try_into().map_err(|e| {
                            Error::from(ErrorKind::Internal {
                                message: format!(
                                    "an invalid zlib compression level was given: {}",
                                    e
                                ),
                            })
                        })?)
                    }
                    _ => Compression::default(),
                };
                let encoder = ZlibEncoder::new(vec![], level);
                Ok(Encoder::Zlib { encoder })
            }
            #[cfg(feature = "snappy-compression")]
            Compressor::Snappy => Ok(Encoder::Snappy { bytes: vec![] }),
        }
    }
}

pub(crate) enum Encoder {
    #[cfg(feature = "zstd-compression")]
    Zstd {
        encoder: zstd::Encoder<'static, Vec<u8>>,
    },
    #[cfg(feature = "zlib-compression")]
    Zlib { encoder: ZlibEncoder<Vec<u8>> },
    #[cfg(feature = "snappy-compression")]
    Snappy { bytes: Vec<u8> },
}

#[allow(unused_variables)]
impl Encoder {
    pub(crate) fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match *self {
            #[cfg(feature = "zstd-compression")]
            Encoder::Zstd { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred writing to the zstd encoder: {}", e),
                }
                .into()
            }),
            #[cfg(feature = "zlib-compression")]
            Encoder::Zlib { ref mut encoder } => encoder.write_all(buf).map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred writing to the zlib encoder: {}", e),
                }
                .into()
            }),
            #[cfg(feature = "snappy-compression")]
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
            #[cfg(feature = "zstd-compression")]
            Encoder::Zstd { encoder } => encoder.finish().map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred finishing zstd encoder: {}", e),
                }
                .into()
            }),
            #[cfg(feature = "zlib-compression")]
            Encoder::Zlib { encoder } => encoder.finish().map_err(|e| {
                ErrorKind::Internal {
                    message: format!("an error occurred finishing zlib encoder: {}", e),
                }
                .into()
            }),
            #[cfg(feature = "snappy-compression")]
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
    #[cfg(feature = "zstd-compression")]
    Zstd,
    #[cfg(feature = "zlib-compression")]
    Zlib,
    #[cfg(feature = "snappy-compression")]
    Snappy,
    Noop,
}

impl Decoder {
    pub(crate) fn decode(self, source: &[u8]) -> Result<Vec<u8>> {
        match self {
            #[cfg(feature = "zstd-compression")]
            Decoder::Zstd => {
                let mut ret = Vec::new();
                zstd::stream::copy_decode(source, &mut ret).map_err(|e| {
                    Error::from(ErrorKind::Internal {
                        message: format!("Could not decode using zstd decoder: {}", e),
                    })
                })?;
                Ok(ret)
            }
            #[cfg(feature = "zlib-compression")]
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
            #[cfg(feature = "snappy-compression")]
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
            #[cfg(feature = "snappy-compression")]
            CompressorId::Snappy => Ok(Decoder::Snappy),
            #[cfg(feature = "zlib-compression")]
            CompressorId::Zlib => Ok(Decoder::Zlib),
            #[cfg(feature = "zstd-compression")]
            CompressorId::Zstd => Ok(Decoder::Zstd),
        }
    }
}

#[cfg(test)]
mod test;

use crate::error::{Error, ErrorKind};
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

#[cfg(feature = "zlib-compression")]
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use std::io::prelude::*;

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use crate::error::Result;

#[derive(Clone, Debug, PartialEq)]
#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
pub(crate) enum CompressorId {
    Noop = 0,
    #[cfg(feature = "snappy-compression")]
    Snappy = 1,
    #[cfg(feature = "zlib-compression")]
    Zlib = 2,
    #[cfg(feature = "zstd-compression")]
    Zstd = 3,
}

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
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
#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
/// Enum representing supported compressor algorithms.
/// Used for compressing messages sent to the server.
/// For compressors that take a level, use None to indicate the default level.
/// Requires one of zstd-compression, zlib-compression, or snappy-compression
/// feature flags.
pub enum Compressor {
    /// Zstd compressor.  Level 0 is the default.  Requires Rust version 1.54.
    /// See Zstd [manual](http://facebook.github.io/zstd/zstd_manual.html) for more information
    #[cfg(feature = "zstd-compression")]
    Zstd { level: Option<i32> },
    /// Zlib compressor.  Levels -1 through 9 are valid.
    /// Level -1 indicates default level.
    /// Level 0 indicates no compression.
    /// Higher level indicates more compression (and slower).
    #[cfg(feature = "zlib-compression")]
    Zlib { level: Option<i32> },
    /// Snappy compressor.
    #[cfg(feature = "snappy-compression")]
    Snappy,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
#[cfg(not(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
)))]
pub(crate) enum Compressor {}

impl FromStr for Compressor {
    type Err = Error;

    #[allow(unused_variables)]
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            #[cfg(feature = "zlib-compression")]
            "zlib" => Ok(Compressor::Zlib { level: None }),
            #[cfg(feature = "zstd-compression")]
            "zstd" => Ok(Compressor::Zstd { level: None }),
            #[cfg(feature = "snappy-compression")]
            "snappy" => Ok(Compressor::Snappy),
            other => {
                #[cfg(not(any(
                    feature = "zstd-compression",
                    feature = "zlib-compression",
                    feature = "snappy-compression"
                )))]
                {
                    Err(ErrorKind::InvalidArgument {
                        message: "No compressor flag enabled".to_string(),
                    }
                    .into())
                }
                #[cfg(any(
                    feature = "zstd-compression",
                    feature = "zlib-compression",
                    feature = "snappy-compression"
                ))]
                {
                    Err(ErrorKind::InvalidArgument {
                        message: format!("Received invalid compressor: {}", other),
                    }
                    .into())
                }
            }
        }
    }
}

impl fmt::Display for Compressor {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { .. } => write!(f, "zstd"),
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { .. } => write!(f, "zlib"),
            #[cfg(feature = "snappy-compression")]
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

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
impl Compressor {
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
                    Some(level) if level != -1 => Compression::new(level as u32),
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

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
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

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
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
#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
pub(crate) enum Decoder {
    #[cfg(feature = "zstd-compression")]
    Zstd,
    #[cfg(feature = "zlib-compression")]
    Zlib,
    #[cfg(feature = "snappy-compression")]
    Snappy,
    Noop,
}

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
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

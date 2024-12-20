use crate::error::{ErrorKind, Result};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "fuzzing")]
use arbitrary::Arbitrary;
#[cfg(feature = "fuzzing")]
use byteorder::{LittleEndian, ReadBytesExt};
#[cfg(feature = "fuzzing")]
use std::io::Cursor;

/// The wire protocol op codes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "fuzzing", derive(Arbitrary))]
pub enum OpCode {
    Reply = 1,
    Query = 2004,
    Message = 2013,
    Compressed = 2012,
}

impl OpCode {
    /// Attempt to infer the op code based on the numeric value.
    pub fn from_i32(i: i32) -> Result<Self> {
        match i {
            1 => Ok(OpCode::Reply),
            2004 => Ok(OpCode::Query),
            2013 => Ok(OpCode::Message),
            2012 => Ok(OpCode::Compressed),
            other => Err(ErrorKind::InvalidResponse {
                message: format!("Invalid wire protocol opcode: {}", other),
            }
            .into()),
        }
    }
}

/// The header for any wire protocol message.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "fuzzing", derive(Arbitrary))]
pub struct Header {
    pub length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: OpCode,
}

impl Header {
    #[cfg(feature = "fuzzing")]
    pub const LENGTH: usize = 4 * std::mem::size_of::<i32>();

    #[cfg(not(feature = "fuzzing"))]
    pub(crate) const LENGTH: usize = 4 * std::mem::size_of::<i32>();

    #[cfg(feature = "fuzzing")]
    pub fn from_slice(data: &[u8]) -> Result<Self> {
        if data.len() < Self::LENGTH {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "Header requires {} bytes but only got {}",
                    Self::LENGTH,
                    data.len()
                ),
            }
            .into());
        }
        let mut cursor = Cursor::new(data);

        let length = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor).map_err(|e| {
            ErrorKind::InvalidResponse {
                message: format!("Failed to read length: {}", e),
            }
        })?;

        let request_id = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor).map_err(|e| {
            ErrorKind::InvalidResponse {
                message: format!("Failed to read request_id: {}", e),
            }
        })?;

        let response_to = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor).map_err(|e| {
            ErrorKind::InvalidResponse {
                message: format!("Failed to read response_to: {}", e),
            }
        })?;

        let op_code =
            OpCode::from_i32(ReadBytesExt::read_i32::<LittleEndian>(&mut cursor).map_err(
                |e| ErrorKind::InvalidResponse {
                    message: format!("Failed to read op_code: {}", e),
                },
            )?)?;

        Ok(Self {
            length,
            request_id,
            response_to,
            op_code,
        })
    }

    pub(crate) async fn write_to<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        stream.write_all(&self.length.to_le_bytes()).await?;
        stream.write_all(&self.request_id.to_le_bytes()).await?;
        stream.write_all(&self.response_to.to_le_bytes()).await?;
        stream
            .write_all(&(self.op_code as i32).to_le_bytes())
            .await?;

        Ok(())
    }

    pub(crate) async fn read_from<R: tokio::io::AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> Result<Self> {
        let length = reader.read_i32_le().await?;
        let request_id = reader.read_i32_le().await?;
        let response_to = reader.read_i32_le().await?;
        let op_code = OpCode::from_i32(reader.read_i32_le().await?)?;
        Ok(Self {
            length,
            request_id,
            response_to,
            op_code,
        })
    }
}

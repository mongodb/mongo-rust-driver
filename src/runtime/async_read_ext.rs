use async_trait::async_trait;
use futures::io::AsyncRead;

use crate::error::Result;

/// Trait providing helpers that read various integer types in little-endian order.
#[async_trait]
pub(crate) trait AsyncLittleEndianRead: Unpin + futures::io::AsyncReadExt {
    /// Read an `i32` in little-endian order.
    async fn read_i32(&mut self) -> Result<i32> {
        let mut buf: [u8; 4] = [0; 4];
        self.read_exact(&mut buf).await?;
        Ok(i32::from_le_bytes(buf))
    }

    /// Read a `u32` in little-endian orer.
    async fn read_u32(&mut self) -> Result<u32> {
        let mut buf: [u8; 4] = [0; 4];
        self.read_exact(&mut buf).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn read_u8(&mut self) -> Result<u8> {
        let mut buf: [u8; 1] = [0; 1];
        self.read_exact(&mut buf).await?;
        Ok(buf[0])
    }
}

impl<R: AsyncRead + ?Sized + Unpin> AsyncLittleEndianRead for R {}

use std::io::Write;

use async_trait::async_trait;
use futures_io::AsyncWrite;

use crate::error::Result;

/// Trait providing helpers that write various integer types in little-endian order.
#[async_trait]
pub(crate) trait AsyncLittleEndianWrite: Unpin + futures_util::AsyncWriteExt {
    /// Write an `i32` in little-endian order.
    async fn write_i32(&mut self, n: i32) -> Result<()> {
        self.write(&n.to_le_bytes()).await?;
        Ok(())
    }

    /// Write a `u32` in little-endian order.
    async fn write_u32(&mut self, n: u32) -> Result<()> {
        self.write(&n.to_le_bytes()).await?;
        Ok(())
    }

    async fn write_u8(&mut self, n: u8) -> Result<()> {
        self.write(&[n]).await?;
        Ok(())
    }
}

impl<W: AsyncWrite + ?Sized + Unpin> AsyncLittleEndianWrite for W {}

/// Trait providing helpers that write various integer types in little-endian order.
pub(crate) trait SyncLittleEndianWrite: Write {
    /// Write an `i32` in little-endian order.
    fn write_i32(&mut self, n: i32) -> Result<()> {
        self.write_all(&n.to_le_bytes())?;
        Ok(())
    }

    /// Write a `u32` in little-endian order.
    fn write_u32(&mut self, n: u32) -> Result<()> {
        self.write_all(&n.to_le_bytes())?;
        Ok(())
    }

    fn write_u8(&mut self, n: u8) -> Result<()> {
        self.write_all(&[n])?;
        Ok(())
    }
}

impl<W: Write> SyncLittleEndianWrite for W {}

use std::path::Path;

use anyhow::Result;
#[cfg(feature = "async-std-runtime")]
use async_std::{
    fs::{self, OpenOptions},
    io::{
        self,
        prelude::{BufReadExt, WriteExt},
    },
};
use futures::stream::Stream;
#[cfg(feature = "tokio-runtime")]
use tokio::{
    fs::{self, OpenOptions},
    io::{self, AsyncBufReadExt, AsyncWriteExt},
};

pub(crate) async fn read_to_string(path: &Path) -> Result<String> {
    let s = fs::read_to_string(path).await?;
    Ok(s)
}

pub(crate) struct File {
    inner: fs::File,
}

impl File {
    pub(crate) async fn open_write(name: &Path) -> Result<Self> {
        let inner = OpenOptions::new()
            .create(true)
            .write(true)
            .open(name)
            .await?;

        Ok(Self { inner })
    }

    pub(crate) async fn open_read(name: &Path) -> Result<Self> {
        let inner = fs::File::open(name).await?;

        Ok(Self { inner })
    }

    pub(crate) async fn write_line(&mut self, s: &str) -> Result<()> {
        Ok(self.inner.write_all(format!("{}\n", s).as_bytes()).await?)
    }
}

pub(crate) struct BufReader {
    inner: io::BufReader<fs::File>,
}

impl BufReader {
    pub(crate) fn new(file: File) -> Self {
        Self {
            inner: io::BufReader::new(file.inner),
        }
    }

    pub(crate) fn lines(self) -> impl Stream<Item = std::io::Result<String>> {
        self.inner.lines()
    }
}

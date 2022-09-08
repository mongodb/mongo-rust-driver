use std::{
    ffi::OsStr,
    process::{ExitStatus, Stdio},
};

#[cfg(feature = "async-std-runtime")]
use async_std::process::{Child, Command};
#[cfg(feature = "tokio-runtime")]
use tokio::process::{Child, Command};

use crate::error::Result;

#[derive(Debug)]
pub(crate) struct Process {
    child: Child,
}

impl Process {
    pub(crate) fn spawn<P, I, A>(path: P, args: I) -> Result<Self>
    where
        P: AsRef<OsStr>,
        I: IntoIterator<Item = A>,
        A: AsRef<OsStr>,
    {
        let mut command = Command::new(path);
        let child = command
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus> {
        #[cfg(feature = "tokio-runtime")]
        return Ok(self.child.wait().await?);
        #[cfg(feature = "async-std-runtime")]
        return Ok(self.child.status().await?);
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Attempt to reap the process.
        #[cfg(feature = "tokio-runtime")]
        let _ = self.child.try_wait();
        #[cfg(feature = "async-std-runtime")]
        let _ = self.child.try_status();
    }
}

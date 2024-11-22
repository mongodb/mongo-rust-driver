use std::{sync::Arc, time::Duration};

use serde::Deserialize;

use super::State;
use crate::{
    cmap::options::ConnectionPoolOptions,
    error::Result,
    event::cmap::CmapEvent,
    serde_util,
    test::{util::fail_point::FailPoint, RunOn},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestFile {
    #[serde(rename = "version")]
    _version: u8, // can ignore this field as there's only one version
    #[serde(rename = "style")]
    _style: TestStyle, // we use the presence of fail_point / run_on to determine this
    pub description: String,
    pub(crate) pool_options: Option<ConnectionPoolOptions>,
    pub operations: Vec<ThreadedOperation>,
    pub error: Option<Error>,
    pub(crate) events: Vec<CmapEvent>,
    #[serde(default)]
    pub ignore: Vec<String>,
    pub fail_point: Option<FailPoint>,
    pub(crate) run_on: Option<Vec<RunOn>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum TestStyle {
    Unit,
    Integration,
}

#[derive(Debug, Deserialize)]
pub struct ThreadedOperation {
    #[serde(flatten)]
    type_: Operation,

    thread: Option<String>,
}

impl ThreadedOperation {
    pub(super) async fn execute(self, state: Arc<State>) -> Result<()> {
        match self.thread {
            Some(thread_name) => {
                let threads = state.threads.read().await;
                let thread = threads.get(&thread_name).unwrap();
                thread.dispatcher.send(self.type_).unwrap();
                Ok(())
            }
            None => self.type_.execute(state).await,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    Start {
        target: String,
    },
    Wait {
        ms: u64,
    },
    WaitForThread {
        target: String,
    },
    WaitForEvent {
        event: String,
        count: usize,
        #[serde(deserialize_with = "serde_util::deserialize_duration_option_from_u64_millis")]
        #[serde(default)]
        timeout: Option<Duration>,
    },
    CheckOut {
        label: Option<String>,
    },
    CheckIn {
        connection: String,
    },
    #[serde(rename_all = "camelCase")]
    Clear {
        #[serde(default)]
        interrupt_in_use_connections: Option<bool>,
    },
    Close,
    Ready,
}

#[derive(Debug, Deserialize)]
// TODO RUST-1077: remove the #[allow(dead_code)] tag and add #[serde(deny_unknown_fields)] to
// ensure these tests are being fully run
#[allow(dead_code)]
pub struct Error {
    #[serde(rename = "type")]
    pub type_: String,
    message: String,
    address: Option<String>,
}

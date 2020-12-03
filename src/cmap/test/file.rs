use std::sync::Arc;

use serde::Deserialize;

use super::{event::Event, State};
use crate::{
    cmap::options::ConnectionPoolOptions,
    error::{ErrorKind, Result},
    test::RunOn,
};
use bson::Document;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFile {
    version: u8,
    style: TestStyle,
    pub description: String,
    pub pool_options: Option<ConnectionPoolOptions>,
    pub operations: Vec<ThreadedOperation>,
    pub error: Option<Error>,
    pub events: Vec<Event>,
    #[serde(default)]
    pub ignore: Vec<String>,
    pub fail_point: Option<Document>,
    pub run_on: Option<Vec<RunOn>>,
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
    Start { target: String },
    Wait { ms: u64 },
    WaitForThread { target: String },
    WaitForEvent { event: String, count: usize },
    CheckOut { label: Option<String> },
    CheckIn { connection: String },
    Clear,
    Close,
}

#[derive(Debug, Deserialize)]
pub struct Error {
    #[serde(rename = "type")]
    pub type_: String,
    message: String,
    address: Option<String>,
}

impl Error {
    pub fn assert_matches(&self, error: &crate::error::Error, description: &str) {
        match error.kind.as_ref() {
            ErrorKind::WaitQueueTimeoutError { .. } => {
                assert_eq!(self.type_, "WaitQueueTimeoutError", "{}", description);
            }
            _ => {
                panic!("Expected {}, but got {:?}", self.type_, error);
            }
        }
    }
}

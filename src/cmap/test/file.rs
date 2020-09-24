use std::collections::VecDeque;

use serde::Deserialize;

use super::event::Event;
use crate::{cmap::options::ConnectionPoolOptions, error::ErrorKind, test::RunOn};
use bson::Document;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFile {
    version: u8,
    style: TestStyle,
    pub description: String,
    pub pool_options: Option<ConnectionPoolOptions>,
    operations: VecDeque<ThreadedOperation>,
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
struct ThreadedOperation {
    #[serde(flatten)]
    type_: Operation,

    thread: Option<String>,
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
    },
    CheckOut {
        label: Option<String>,
    },
    CheckIn {
        connection: String,
    },
    Clear,
    Close,

    // In order to execute a `Start` operation, we need to know all of the operations that should
    // execute in the context of that thread. To achieve this, we preprocess the operations
    // specified by the test file to replace each instance of `Start` operation with a
    // `StartHelper` with the corresponding operations. `StartHelper` won't ever actually occur in
    // the original set of operations specified.
    StartHelper {
        target: String,
        operations: Vec<Operation>,
    },
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

impl TestFile {
    pub fn process_operations(&mut self) -> Vec<Operation> {
        let mut processed_ops = Vec::new();

        while let Some(operation) = self.operations.pop_front() {
            match operation.type_ {
                // When a `Start` operation is encountered, search the rest of the operations for
                // any that occur in the context of the corresponding thread, remove them from the
                // original set of operations, and add them to the newly created `StartHelper`
                // operation.
                Operation::Start { target } => {
                    let start_helper = Operation::StartHelper {
                        operations: remove_by(&mut self.operations, |op| {
                            op.thread.as_ref() == Some(&target)
                        })
                        .into_iter()
                        .map(|op| op.type_)
                        .collect(),
                        target,
                    };

                    processed_ops.push(start_helper);
                }
                other => processed_ops.push(other),
            }
        }

        processed_ops
    }
}

// Removes all items in the `VecDeque` that fulfill the predicate and return them in order as a new
// `Vec`.
fn remove_by<T, F>(vec: &mut VecDeque<T>, pred: F) -> Vec<T>
where
    F: Fn(&T) -> bool,
{
    let mut i = 0;
    let mut removed = Vec::new();

    while i < vec.len() {
        if pred(&vec[i]) {
            removed.push(vec.remove(i).unwrap());
        } else {
            i += 1;
        }
    }

    removed
}

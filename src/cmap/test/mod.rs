mod event;
mod file;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    thread::JoinHandle,
};

use self::{
    event::EventHandler,
    file::{Operation, TestFile},
};

use crate::{
    cmap::{Connection, ConnectionPool},
    error::{Error, Result},
};
use std::time::Duration;

#[derive(Debug)]
struct Executor {
    handler: EventHandler,
    operations: Vec<Operation>,
    error: Option<self::file::Error>,
    state: Arc<RwLock<State>>,
}

#[derive(Debug)]
struct State {
    pool: ConnectionPool,
    connections: HashMap<String, Connection>,
    threads: HashMap<String, JoinHandle<()>>,
}

impl Executor {
    fn new(mut test_file: TestFile) -> Self {
        let handler = EventHandler::default();
        let operations = test_file.process_operations();
        let error = test_file.error;

        let pool = ConnectionPool::new(
            "foobar",
            test_file.pool_options,
            Some(Box::new(handler.clone())),
        );

        let state = State {
            pool,
            connections: Default::default(),
            threads: Default::default(),
        };

        Self {
            error,
            operations,
            handler,
            state: Arc::new(RwLock::new(state)),
        }
    }

    fn execute_test(self) {
        let mut error: Option<Error> = None;
        let operations = self.operations;

        for operation in operations {
            let err = operation.execute(self.state.clone()).err();

            if error.is_none() {
                error = err;
            }
        }

        match (self.error, error) {
            (Some(ref expected), Some(ref actual)) => expected.assert_matches(actual),
            (Some(ref expected), None) => {
                panic!("Expected {}, but no error occurred", expected.type_)
            }
            (None, Some(ref actual)) => panic!(
                "Expected no error to occur, but the following error was returned: {:?}",
                actual
            ),
            (None, None) => {}
        }
    }
}

impl Operation {
    fn execute(self, state: Arc<RwLock<State>>) -> Result<()> {
        match self {
            Operation::StartHelper { target, operations } => {
                let state_ref = state.clone();

                let thread = std::thread::spawn(move || {
                    for operation in operations {
                        operation.execute(state_ref.clone()).unwrap();
                    }
                });

                state.write().unwrap().threads.insert(target, thread);
            }
            Operation::Wait { ms } => std::thread::sleep(Duration::from_millis(ms)),
            Operation::WaitForThread { target } => {
                state
                    .write()
                    .unwrap()
                    .threads
                    .remove(&target)
                    .unwrap()
                    .join()
                    .unwrap();
            }
            Operation::WaitForEvent { .. } => unimplemented!(),
            Operation::CheckOut { label } => {
                let conn = state.write().unwrap().pool.check_out()?;

                if let Some(label) = label {
                    state.write().unwrap().connections.insert(label, conn);
                }
            }
            Operation::CheckIn { connection } => {
                let conn = state
                    .write()
                    .unwrap()
                    .connections
                    .remove(&connection)
                    .unwrap();
                state.write().unwrap().pool.check_in(conn);
            }
            Operation::Clear => {
                state.write().unwrap().pool.clear()?;
            }
            Operation::Close => unimplemented!(),
            Operation::Start { .. } => unreachable!(),
        }

        Ok(())
    }
}

#[test]
fn cmap_spec_tests() {
    crate::test::run(&["connection-monitoring-and-pooling"], |test_file| {
        let executor = Executor::new(test_file);
        executor.execute_test();
    });
}

mod event;
mod file;

use std::{
    borrow::BorrowMut,
    collections::HashMap,
    sync::{Arc, RwLock},
    thread::JoinHandle,
    time::Duration,
};

use self::{
    event::{Event, EventHandler},
    file::{Operation, TestFile},
};

use crate::{
    cmap::{Connection, ConnectionPool},
    error::{Error, Result},
    event::cmap::*,
};

#[derive(Debug)]
struct Executor {
    operations: Vec<Operation>,
    error: Option<self::file::Error>,
    events: Vec<Event>,
    state: Arc<RwLock<State>>,
}

#[derive(Debug)]
struct State {
    handler: EventHandler,
    pool: Option<ConnectionPool>,
    connections: HashMap<String, Connection>,
    threads: HashMap<String, JoinHandle<()>>,
}

impl State {
    fn count_events(&self, event_type: &str) -> usize {
        self.handler
            .events
            .read()
            .unwrap()
            .iter()
            .filter(|cmap_event| cmap_event.name() == event_type)
            .count()
    }
}

impl Executor {
    fn new(mut test_file: TestFile) -> Self {
        let operations = test_file.process_operations();
        let handler = EventHandler::new(test_file.ignore);
        let events = test_file.events;
        let error = test_file.error;

        let pool = ConnectionPool::new(
            "42",
            test_file.pool_options,
            Some(Box::new(handler.clone())),
        );

        let state = State {
            handler,
            pool: Some(pool),
            connections: Default::default(),
            threads: Default::default(),
        };

        Self {
            error,
            events,
            operations,
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

        let state_handle = self.state.read().unwrap();
        let actual_events = state_handle.handler.events.read().unwrap();

        assert_eq!(actual_events.len(), self.events.len());

        for i in 0..actual_events.len() {
            assert_eq!(actual_events[i], self.events[i]);
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
            Operation::WaitForEvent { event, count } => {
                while state.read().unwrap().count_events(&event) < count {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
            Operation::CheckOut { label } => {
                if let Some(pool) = state.write().unwrap().pool.borrow_mut() {
                    let conn = pool.check_out()?;

                    if let Some(label) = label {
                        state.write().unwrap().connections.insert(label, conn);
                    }
                }
            }
            Operation::CheckIn { connection } => {
                let conn = state
                    .write()
                    .unwrap()
                    .connections
                    .remove(&connection)
                    .unwrap();

                if let Some(pool) = state.write().unwrap().pool.borrow_mut() {
                    pool.check_in(conn);
                }
            }
            Operation::Clear => {
                if let Some(pool) = state.write().unwrap().pool.borrow_mut() {
                    pool.clear()?;
                }
            }
            Operation::Close => unimplemented!(),
            Operation::Start { .. } => unreachable!(),
        }

        Ok(())
    }
}

fn assert_events_match(actual: &Event, expected: &Event) {
    match (actual, expected) {
        (Event::ConnectionPoolCreated(actual), Event::ConnectionPoolCreated(expected)) => {
            assert_eq!(actual.address, expected.address);

            if expected
                .options
                .as_ref()
                .map(ConnectionPoolOptions::is_empty)
                .unwrap_or(false)
            {
                assert!(actual.options.is_some())
            } else {
                assert_eq!(actual.options, expected.options);
            }
        }
        (Event::ConnectionCreated(actual), Event::ConnectionCreated(expected)) => {
            assert_eq!(actual.address, expected.address);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionReady(actual), Event::ConnectionReady(expected)) => {
            assert_eq!(actual.address, expected.address);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionClosed(actual), Event::ConnectionClosed(expected)) => {
            assert_eq!(actual.address, expected.address);
            assert_eq!(actual.reason, expected.reason);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionCheckedOut(actual), Event::ConnectionCheckedOut(expected)) => {
            assert_eq!(actual.address, expected.address);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionCheckedIn(actual), Event::ConnectionCheckedIn(expected)) => {
            assert_eq!(actual.address, expected.address);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}

#[test]
fn cmap_spec_tests() {
    crate::test::run(&["connection-monitoring-and-pooling"], |test_file| {
        let executor = Executor::new(test_file);
        executor.execute_test();
    });
}

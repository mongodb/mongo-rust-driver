mod event;
mod file;

use std::{
    collections::HashMap,
    ops::Deref,
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
};

const TEST_DESCRIPTIONS_TO_SKIP: &[&str] = &[
    "must destroy checked in connection if pool has been closed",
    "must throw error if checkOut is called on a closed pool",
    "When a pool is closed, it MUST first destroy all available connections in that pool",
];

#[derive(Debug)]
struct Executor {
    operations: Vec<Operation>,
    error: Option<self::file::Error>,
    events: Vec<Event>,
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    handler: EventHandler,
    connections: RwLock<HashMap<String, Connection>>,
    threads: RwLock<HashMap<String, JoinHandle<Result<()>>>>,

    // In order to drop the pool when performing a `close` operation, we use an `Option` so that we
    // can replace it with `None`. Since none of the tests should use the pool after its closed
    // (besides the ones we manually skip over), it's fine for us to `unwrap` the pool during these
    // tests, as panicking is sufficient to exit any aberrant test with a failure.
    pool: RwLock<Option<ConnectionPool>>,
}

impl State {
    // Counts the number of events of the given type that have occurred so far.
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

        // The CMAP spec tests use the placeholder value `42` to indicate that the presence of the
        // value should be asserted without any constraints on the value itself. To facilitate
        // deserializing the events from the spec test JSON files the event types we've defined, we
        // substitute `"address": 42` with `"address": "42"` in the JSON files (since the address
        // needs to be a string to deserialize correctly). Because of this, we use the address
        // `"42"` for the connection pool we create for the tests as well, which allows us to
        // naively assert equality between the expected events and the events that occur.
        let pool = ConnectionPool::new(
            "42",
            test_file.pool_options,
            Some(Arc::new(handler.clone())),
        );

        let state = State {
            handler,
            pool: RwLock::new(Some(pool)),
            connections: Default::default(),
            threads: Default::default(),
        };

        Self {
            error,
            events,
            operations,
            state: Arc::new(state),
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

            // Some of the CMAP spec tests have some subtle race conditions due assertions about the
            // order that concurrent threads check out connections from the pool. The solution to
            // this is to sleep for a small amount of time between each operation, which in practice
            // allows threads to enter the wait queue in the order that they were started.
            std::thread::sleep(Duration::from_millis(1));
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

        let actual_events = self.state.handler.events.read().unwrap();

        assert_eq!(actual_events.len(), self.events.len());

        for i in 0..actual_events.len() {
            assert_events_match(&actual_events[i], &self.events[i]);
        }
    }
}

impl Operation {
    fn execute(self, state: Arc<State>) -> Result<()> {
        match self {
            Operation::StartHelper { target, operations } => {
                let state_ref = state.clone();

                let thread = std::thread::spawn(move || {
                    for operation in operations {
                        // If any error occurs during an operation, we halt the thread and yield
                        // that value when `join` is called on the thread.
                        operation.execute(state_ref.clone())?;
                    }

                    Ok(())
                });

                state.threads.write().unwrap().insert(target, thread);
            }
            Operation::Wait { ms } => std::thread::sleep(Duration::from_millis(ms)),
            Operation::WaitForThread { target } => state
                .threads
                .write()
                .unwrap()
                .remove(&target)
                .unwrap()
                .join()
                .unwrap()?,
            Operation::WaitForEvent { event, count } => {
                while state.count_events(&event) < count {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            Operation::CheckOut { label } => {
                if let Some(pool) = state.pool.read().unwrap().deref() {
                    let mut conn = pool.check_out()?;

                    if let Some(label) = label {
                        state.connections.write().unwrap().insert(label, conn);
                    } else {
                        conn.pool.take();
                    }
                }
            }
            Operation::CheckIn { connection } => {
                let conn = state
                    .connections
                    .write()
                    .unwrap()
                    .remove(&connection)
                    .unwrap();

                if let Some(pool) = state.pool.read().unwrap().deref() {
                    pool.check_in(conn);
                }
            }
            Operation::Clear => {
                if let Some(pool) = state.pool.write().unwrap().deref() {
                    pool.clear();
                }
            }
            Operation::Close => {
                state.connections.write().unwrap().clear();
                state.pool.write().unwrap().take();
            }

            // We replace all instances of `Start` with `StartHelper` when we preprocess the events,
            // so this should never actually be found.
            Operation::Start { .. } => unreachable!(),
        }

        Ok(())
    }
}

fn assert_events_match(actual: &Event, expected: &Event) {
    match (actual, expected) {
        (Event::ConnectionPoolCreated(actual), Event::ConnectionPoolCreated(expected)) => {
            if let Some(ref expected_options) = expected.options {
                assert_eq!(actual.options.as_ref(), Some(expected_options));
            }
        }
        (Event::ConnectionCreated(actual), Event::ConnectionCreated(expected)) => {
            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionReady(actual), Event::ConnectionReady(expected)) => {
            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionClosed(actual), Event::ConnectionClosed(expected)) => {
            assert_eq!(actual.reason, expected.reason);

            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionCheckedOut(actual), Event::ConnectionCheckedOut(expected)) => {
            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionCheckedIn(actual), Event::ConnectionCheckedIn(expected)) => {
            if expected.connection_id != 42 {
                assert_eq!(actual.connection_id, expected.connection_id);
            }
        }
        (Event::ConnectionCheckOutFailed(actual), Event::ConnectionCheckOutFailed(expected)) => {
            assert_eq!(actual.reason, expected.reason);
        }
        (Event::ConnectionCheckOutStarted(_), Event::ConnectionCheckOutStarted(_)) => {}
        (Event::ConnectionPoolCleared(_), Event::ConnectionPoolCleared(_)) => {}
        (Event::ConnectionPoolClosed(_), Event::ConnectionPoolClosed(_)) => {}
        (actual, expected) => assert_eq!(actual, expected),
    }
}

#[test]
fn cmap_spec_tests() {
    crate::test::run(
        &["connection-monitoring-and-pooling"],
        |test_file: TestFile| {
            if TEST_DESCRIPTIONS_TO_SKIP.contains(&test_file.description.as_str()) {
                return;
            }

            let executor = Executor::new(test_file);
            executor.execute_test();
        },
    );
}

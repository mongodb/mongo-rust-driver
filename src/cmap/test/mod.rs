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
    pool: RwLock<Option<ConnectionPool>>,
    connections: RwLock<HashMap<String, Connection>>,
    threads: RwLock<HashMap<String, JoinHandle<Result<()>>>>,
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
        println!();

        let mut error: Option<Error> = None;
        let operations = self.operations;

        for operation in operations {
            let err = operation.execute(self.state.clone()).err();

            if error.is_none() {
                error = err;
            }

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
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
            Operation::CheckOut { label } => {
                if let Some(pool) = state.pool.read().unwrap().deref() {
                    let conn = pool.check_out()?;

                    if let Some(label) = label {
                        state.connections.write().unwrap().insert(label, conn);
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
                state.pool.write().unwrap().take();
            }
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
        (Event::ConnectionCheckOutStarted(actual), Event::ConnectionCheckOutStarted(expected)) => {}
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

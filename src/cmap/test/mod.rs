mod event;
mod file;
mod integration;

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use futures::future::{BoxFuture, FutureExt};

use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use self::{
    event::{Event, EventHandler},
    file::{Operation, TestFile},
};

use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::{Error, Result},
    options::TlsOptions,
    runtime::AsyncJoinHandle,
    test::{assert_matches, run_spec_test, EventClient, Matchable, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};
use bson::doc;

const TEST_DESCRIPTIONS_TO_SKIP: &[&str] = &[
    "must destroy checked in connection if pool has been closed",
    "must throw error if checkOut is called on a closed pool",
];

/// Many different types of CMAP events are emitted from tasks spawned in the drop
/// implementations of various types (Connections, pools, etc.). Sometimes it takes
/// a longer amount of time for these tasks to get scheduled and thus their associated
/// events to get emitted, requiring the runner to wait for a little bit before asserting
/// the events were actually fired.
///
/// This value was purposefully chosen to be large to prevent test failures, though it is not
/// expected that the 3s timeout will regularly or ever be hit.
const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct Executor {
    description: String,
    operations: Vec<Operation>,
    error: Option<self::file::Error>,
    events: Vec<Event>,
    state: Arc<State>,
    ignored_event_names: Vec<String>,
    pool_options: ConnectionPoolOptions,
}

#[derive(Debug)]
struct State {
    handler: Arc<EventHandler>,
    connections: RwLock<HashMap<String, Connection>>,
    unlabeled_connections: Mutex<Vec<Connection>>,
    threads: RwLock<HashMap<String, AsyncJoinHandle<Result<()>>>>,

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
        let handler = Arc::new(EventHandler::new());
        let error = test_file.error;

        let mut pool_options = test_file.pool_options.unwrap_or_else(Default::default);
        pool_options.tls_options = CLIENT_OPTIONS.tls_options();
        pool_options.event_handler = Some(handler.clone());

        let state = State {
            handler,
            pool: RwLock::new(None),
            connections: Default::default(),
            threads: Default::default(),
            unlabeled_connections: Mutex::new(Default::default()),
        };

        Self {
            description: test_file.description,
            error,
            events: test_file.events,
            operations,
            state: Arc::new(state),
            ignored_event_names: test_file.ignore,
            pool_options,
        }
    }

    async fn execute_test(self) {
        let mut subscriber = self.state.handler.subscribe();

        let pool = ConnectionPool::new(
            CLIENT_OPTIONS.hosts[0].clone(),
            Default::default(),
            Some(self.pool_options),
        );
        *self.state.pool.write().await = Some(pool);

        let mut error: Option<Error> = None;
        let operations = self.operations;

        println!("Executing {}", self.description);

        for operation in operations {
            let err = operation.execute(self.state.clone()).await.err();
            if error.is_none() {
                error = err;
            }

            // Some of the CMAP spec tests have some subtle race conditions due assertions about the
            // order that concurrent threads check out connections from the pool. The solution to
            // this is to sleep for a small amount of time between each operation, which in practice
            // allows threads to enter the wait queue in the order that they were started.
            RUNTIME.delay_for(Duration::from_millis(1)).await;
        }

        match (self.error, error) {
            (Some(ref expected), Some(ref actual)) => {
                expected.assert_matches(actual, self.description.as_str())
            }
            (Some(ref expected), None) => {
                panic!("Expected {}, but no error occurred", expected.type_)
            }
            (None, Some(ref actual)) => panic!(
                "Expected no error to occur, but the following error was returned: {:?}",
                actual
            ),
            (None, None) => {}
        }

        let ignored_event_names = self.ignored_event_names;
        let description = self.description;
        for expected_event in self.events {
            let actual_event = subscriber
                .wait_for_event(EVENT_TIMEOUT, |e| {
                    !ignored_event_names.iter().any(|name| e.name() == name)
                })
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "{}: did not receive expected event: {:?}",
                        description, expected_event
                    )
                });
            assert_matches(&actual_event, &expected_event, Some(description.as_str()));
        }
    }
}

impl Operation {
    /// Execute this operation.
    /// async fns currently cannot be recursive, so instead we manually return a BoxFuture here.
    /// See: https://rust-lang.github.io/async-book/07_workarounds/05_recursion.html
    fn execute(self, state: Arc<State>) -> BoxFuture<'static, Result<()>> {
        async move {
            match self {
                Operation::StartHelper { target, operations } => {
                    let state_ref = state.clone();
                    let task = RUNTIME.spawn(async move {
                        for operation in operations {
                            // If any error occurs during an operation, we halt the thread and yield
                            // that value when `join` is called on the thread.
                            operation.execute(state_ref.clone()).await?;
                        }
                        Ok(())
                    });

                    state.threads.write().await.insert(target, task.unwrap());
                }
                Operation::Wait { ms } => RUNTIME.delay_for(Duration::from_millis(ms)).await,
                Operation::WaitForThread { target } => {
                    state.threads.write().await.remove(&target).unwrap().await?
                }
                Operation::WaitForEvent { event, count } => {
                    while state.count_events(&event) < count {
                        RUNTIME.delay_for(Duration::from_millis(100)).await;
                    }
                }
                Operation::CheckOut { label } => {
                    if let Some(pool) = state.pool.read().await.deref() {
                        let conn = pool.check_out().await?;

                        if let Some(label) = label {
                            state.connections.write().await.insert(label, conn);
                        } else {
                            state.unlabeled_connections.lock().await.push(conn);
                        }
                    }
                }
                Operation::CheckIn { connection } => {
                    let mut subscriber = state.handler.subscribe();
                    let conn = state.connections.write().await.remove(&connection).unwrap();
                    let id = conn.id;
                    // connections are checked in via tasks spawned in their drop implementation,
                    // they are not checked in explicitly.
                    drop(conn);

                    // wait for event to be emitted to ensure check in has completed.
                    subscriber
                        .wait_for_event(EVENT_TIMEOUT, |e| {
                            matches!(e, Event::ConnectionCheckedIn(event) if event.connection_id == id)
                        })
                        .await
                        .unwrap_or_else(|| {
                            panic!(
                                "did not receive checkin event after dropping connection (id={})",
                                connection
                            )
                        });
                }
                Operation::Clear => {
                    if let Some(pool) = state.pool.write().await.deref() {
                        pool.clear();
                        // give some time for clear to happen.
                        RUNTIME.delay_for(EVENT_DELAY).await;
                    }
                }
                Operation::Close => {
                    let mut subscriber = state.handler.subscribe();

                    // pools are closed via their drop implementation
                    state.pool.write().await.take();

                    // wait for event to be emitted to ensure drop has completed.
                    subscriber
                        .wait_for_event(EVENT_TIMEOUT, |e| {
                            matches!(e, Event::ConnectionPoolClosed(_))
                        })
                        .await
                        .expect("did not receive ConnectionPoolClosed event after closing pool");
                }

                // We replace all instances of `Start` with `StartHelper` when we preprocess the
                // events, so this should never actually be found.
                Operation::Start { .. } => unreachable!(),
            }
            Ok(())
        }
        .boxed()
    }
}

impl Matchable for TlsOptions {
    fn content_matches(&self, expected: &TlsOptions) -> bool {
        self.allow_invalid_certificates
            .matches(&expected.allow_invalid_certificates)
            && self.ca_file_path.matches(&expected.ca_file_path)
            && self
                .cert_key_file_path
                .matches(&expected.cert_key_file_path)
    }
}

impl Matchable for ConnectionPoolOptions {
    fn content_matches(&self, expected: &ConnectionPoolOptions) -> bool {
        self.app_name.matches(&expected.app_name)
            && self.connect_timeout.matches(&expected.connect_timeout)
            && self.credential.matches(&expected.credential)
            && self.max_idle_time.matches(&expected.max_idle_time)
            && self.max_pool_size.matches(&expected.max_pool_size)
            && self.min_pool_size.matches(&expected.min_pool_size)
            && self.tls_options.matches(&expected.tls_options)
            && self
                .wait_queue_timeout
                .matches(&expected.wait_queue_timeout)
    }
}

impl Matchable for Event {
    fn content_matches(&self, expected: &Event) -> bool {
        match (self, expected) {
            (Event::ConnectionPoolCreated(actual), Event::ConnectionPoolCreated(ref expected)) => {
                actual.options.matches(&expected.options)
            }
            (Event::ConnectionCreated(actual), Event::ConnectionCreated(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionReady(actual), Event::ConnectionReady(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionClosed(actual), Event::ConnectionClosed(ref expected)) => {
                actual.reason == expected.reason
                    && actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionCheckedOut(actual), Event::ConnectionCheckedOut(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionCheckedIn(actual), Event::ConnectionCheckedIn(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (
                Event::ConnectionCheckOutFailed(actual),
                Event::ConnectionCheckOutFailed(ref expected),
            ) => actual.reason == expected.reason,
            (Event::ConnectionCheckOutStarted(_), Event::ConnectionCheckOutStarted(_)) => true,
            (Event::ConnectionPoolCleared(_), Event::ConnectionPoolCleared(_)) => true,
            (Event::ConnectionPoolClosed(_), Event::ConnectionPoolClosed(_)) => true,
            _ => false,
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn cmap_spec_tests() {
    async fn run_cmap_spec_tests(test_file: TestFile) {
        if TEST_DESCRIPTIONS_TO_SKIP.contains(&test_file.description.as_str()) {
            return;
        }

        let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

        let mut options = CLIENT_OPTIONS.clone();
        options.hosts.drain(1..);
        options.direct_connection = Some(true);
        let client = EventClient::with_options(options).await;
        if let Some(ref run_on) = test_file.run_on {
            let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
            if !can_run_on {
                return;
            }
        }

        let should_disable_fp = test_file.fail_point.is_some();
        if let Some(ref fail_point) = test_file.fail_point {
            client
                .database("admin")
                .run_command(fail_point.clone(), None)
                .await
                .unwrap();
        }

        let executor = Executor::new(test_file);
        executor.execute_test().await;

        if should_disable_fp {
            client
                .database("admin")
                .run_command(
                    doc! {
                        "configureFailPoint": "failCommand",
                        "mode": "off"
                    },
                    None,
                )
                .await
                .unwrap();
        }
    }

    run_spec_test(&["connection-monitoring-and-pooling"], run_cmap_spec_tests).await;
}

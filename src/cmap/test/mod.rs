pub(crate) mod event;
mod file;
mod integration;

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use self::{
    event::{Event, EventHandler},
    file::{Operation, TestFile, ThreadedOperation},
};

use crate::{
    cmap::{Connection, ConnectionPool, ConnectionPoolOptions},
    error::{Error, Result},
    event::cmap::ConnectionPoolOptions as EventOptions,
    options::TlsOptions,
    runtime::AsyncJoinHandle,
    sdam::{ServerUpdate, ServerUpdateSender},
    test::{
        assert_matches,
        run_spec_test,
        EventClient,
        Matchable,
        CLIENT_OPTIONS,
        LOCK,
        SERVER_API,
    },
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
    operations: Vec<ThreadedOperation>,
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
    threads: RwLock<HashMap<String, CmapThread>>,

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

#[derive(Debug)]
struct CmapThread {
    handle: AsyncJoinHandle<Result<()>>,
    dispatcher: tokio::sync::mpsc::UnboundedSender<Operation>,
}

impl CmapThread {
    fn start(state: Arc<State>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Operation>();
        let handle = RUNTIME
            .spawn(async move {
                while let Some(operation) = receiver.recv().await {
                    operation.execute(state.clone()).await?;
                }
                Ok(())
            })
            .unwrap();

        Self {
            dispatcher: sender,
            handle,
        }
    }

    async fn wait_until_complete(self) -> Result<()> {
        // hang up dispatcher so task will complete.
        drop(self.dispatcher);
        self.handle.await
    }
}

impl Executor {
    fn new(test_file: TestFile) -> Self {
        let handler = Arc::new(EventHandler::new());
        let error = test_file.error;

        let mut pool_options = test_file.pool_options.unwrap_or_else(Default::default);
        pool_options.tls_options = CLIENT_OPTIONS.tls_options();
        pool_options.event_handler = Some(handler.clone());
        pool_options.server_api = SERVER_API.clone();

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
            operations: test_file.operations,
            state: Arc::new(state),
            ignored_event_names: test_file.ignore,
            pool_options,
        }
    }

    async fn execute_test(self) {
        let mut subscriber = self.state.handler.subscribe();

        // CMAP spec requires setting this to 50ms.
        let mut options = self.pool_options;
        options.maintenance_frequency = Some(Duration::from_millis(50));

        let (update_sender, mut update_receiver) = ServerUpdateSender::channel();

        let pool = ConnectionPool::new(
            CLIENT_OPTIONS.hosts[0].clone(),
            Default::default(),
            update_sender,
            Some(options),
        );

        // Mock a monitoring task responding to errors reported by the pool.
        let manager = pool.manager.clone();
        RUNTIME.execute(async move {
            while let Some(update) = update_receiver.recv().await {
                match update.into_message() {
                    ServerUpdate::Error { .. } => manager.clear().await,
                }
            }
        });

        *self.state.pool.write().await = Some(pool);

        let mut error: Option<Error> = None;
        let operations = self.operations;

        println!("Executing {}", self.description);

        for operation in operations {
            let err = operation.execute(self.state.clone()).await.err();
            if error.is_none() {
                error = err;
            }
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
        let filter = |e: &Event| !ignored_event_names.iter().any(|name| e.name() == name);
        for expected_event in self.events {
            let actual_event = subscriber
                .wait_for_event(EVENT_TIMEOUT, filter)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "{}: did not receive expected event: {:?}",
                        description, expected_event
                    )
                });
            assert_matches(&actual_event, &expected_event, Some(description.as_str()));
        }

        assert_eq!(subscriber.all(filter), Vec::new(), "{}", description);
    }
}

impl Operation {
    /// Execute this operation.
    async fn execute(self, state: Arc<State>) -> Result<()> {
        match self {
            Operation::Wait { ms } => RUNTIME.delay_for(Duration::from_millis(ms)).await,
            Operation::WaitForThread { target } => {
                state
                    .threads
                    .write()
                    .await
                    .remove(&target)
                    .unwrap()
                    .wait_until_complete()
                    .await?
            }
            Operation::WaitForEvent {
                event,
                count,
                timeout,
            } => {
                let event_name = event.clone();
                let task = async move {
                    while state.count_events(&event) < count {
                        RUNTIME.delay_for(Duration::from_millis(100)).await;
                    }
                };
                RUNTIME
                    .timeout(timeout.unwrap_or(EVENT_TIMEOUT), task)
                    .await
                    .unwrap_or_else(|_| {
                        panic!("waiting for {} {} event(s) timed out", count, event_name)
                    });
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
                if let Some(pool) = state.pool.read().await.as_ref() {
                    pool.clear().await;
                }
            }
            Operation::Ready => {
                if let Some(pool) = state.pool.read().await.deref() {
                    pool.mark_as_ready().await;
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
            Operation::Start { target } => {
                state
                    .threads
                    .write()
                    .await
                    .insert(target, CmapThread::start(state.clone()));
            }
        }
        Ok(())
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

impl Matchable for EventOptions {
    fn content_matches(&self, expected: &EventOptions) -> bool {
        self.app_name.matches(&expected.app_name)
            && self.connect_timeout.matches(&expected.connect_timeout)
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
            (Event::ConnectionPoolReady(_), Event::ConnectionPoolReady(_)) => true,
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

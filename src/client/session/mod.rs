mod cluster_time;
mod pool;
#[cfg(test)]
mod test;

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    options::SessionOptions,
    Client,
    RUNTIME,
};
pub(crate) use cluster_time::ClusterTime;
pub(super) use pool::ServerSessionPool;

lazy_static! {
    pub(crate) static ref SESSIONS_UNSUPPORTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("killcursors");
        hash_set.insert("parallelcollectionscan");
        hash_set
    };
}

/// A MongoDB client session. This struct represents a logical session used for ordering sequential
/// operations. To create a `ClientSession`, call `start_session` on a `Client`.
///
/// `ClientSession` instances are not thread safe or fork safe. They can only be used by one thread
/// or process at a time.
#[derive(Debug)]
pub struct ClientSession {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
    client: Client,
    is_implicit: bool,
    options: Option<SessionOptions>,
}

impl ClientSession {
    /// Creates a new `ClientSession` wrapping the provided server session.
    pub(crate) fn new(
        server_session: ServerSession,
        client: Client,
        options: Option<SessionOptions>,
        is_implicit: bool,
    ) -> Self {
        Self {
            client,
            server_session,
            cluster_time: None,
            is_implicit,
            options,
        }
    }

    /// The client used to create this session.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// The id of this session.
    pub fn id(&self) -> &Document {
        &self.server_session.id
    }

    /// Whether this session was created implicitly by the driver or explcitly by the user.
    pub(crate) fn is_implicit(&self) -> bool {
        self.is_implicit
    }

    /// The highest seen cluster time this session has seen so far.
    /// This will be `None` if this session has not been used in an operation yet.
    pub fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    /// The options used to create this session.
    pub fn options(&self) -> Option<&SessionOptions> {
        self.options.as_ref()
    }

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub fn advance_cluster_time(&mut self, to: &ClusterTime) {
        if self.cluster_time().map(|ct| ct < to).unwrap_or(true) {
            self.cluster_time = Some(to.clone());
        }
    }

    /// Mark this session (and the underlying server session) as dirty.
    pub(crate) fn mark_dirty(&mut self) {
        self.server_session.dirty = true;
    }

    /// Updates the date that the underlying server session was last used as part of an operation
    /// sent to the server.
    pub(crate) fn update_last_use(&mut self) {
        self.server_session.last_use = Instant::now();
    }

    /// Increments the txn_number and returns the new value.
    pub(crate) fn get_and_increment_txn_number(&mut self) -> u64 {
        self.server_session.txn_number += 1;
        self.server_session.txn_number
    }

    /// Whether this session is dirty.
    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.server_session.dirty
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        let client = self.client.clone();
        let server_session = ServerSession {
            id: self.server_session.id.clone(),
            last_use: self.server_session.last_use,
            dirty: self.server_session.dirty,
            txn_number: self.server_session.txn_number,
        };

        RUNTIME.execute(async move {
            client.check_in_server_session(server_session).await;
        })
    }
}

/// Client side abstraction of a server session. These are pooled and may be associated with
/// multiple `ClientSession`s over the course of their lifetime.
#[derive(Clone, Debug)]
pub(crate) struct ServerSession {
    /// The id of the server session to which this corresponds.
    id: Document,

    /// The last time an operation was executed with this session.
    last_use: std::time::Instant,

    /// Whether a network error was encountered while using this session.
    dirty: bool,

    /// A monotonically increasing transaction number for this session.
    txn_number: u64,
}

impl ServerSession {
    /// Creates a new session, generating the id client side.
    fn new() -> Self {
        let binary = Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: Uuid::new_v4().as_bytes().to_vec(),
        });

        Self {
            id: doc! { "id": binary },
            last_use: Instant::now(),
            dirty: false,
            txn_number: 0,
        }
    }

    /// Determines if this server session is about to expire in a short amount of time (1 minute).
    fn is_about_to_expire(&self, logical_session_timeout: Duration) -> bool {
        let expiration_date = self.last_use + logical_session_timeout;
        expiration_date < Instant::now() + Duration::from_secs(60)
    }
}

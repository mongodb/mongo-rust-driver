//! Contains the types for read concerns and write concerns.

use std::time::Duration;

use bson::Document;

use crate::error::Result;

/// Specifies the consistency and isolation properties of read operations from replica sets and
/// replica set shards.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/read-concern/) for more
/// information about read concerns.
#[derive(Clone, Debug)]
pub enum ReadConcern {
    /// See the specific documentation for this read concern level [here](https://docs.mongodb.com/manual/reference/read-concern-local/).
    Local,

    /// See the specific documentation for this read concern level [here](https://docs.mongodb.com/manual/reference/read-concern-majority/).
    Majority,

    /// See the specific documentation for this read concern level [here](https://docs.mongodb.com/manual/reference/read-concern-linearizable/).
    Linearizable,

    /// See the specific documentation for this read concern level [here](https://docs.mongodb.com/manual/reference/read-concern-available/).
    Available,

    /// Specify a custom read concern level. This is present to provide forwards compatibility with
    /// any future read concerns which may be added to new versions of MongoDB.
    Custom(String),
}

impl PartialEq for ReadConcern {
    fn eq(&self, other: &Self) -> bool {
        unimplemented!()
    }
}

impl ReadConcern {
    /// Gets the string representation of the `ReadConcern`.
    pub fn as_str(&self) -> &str {
        unimplemented!()
    }
}

/// Specifies the level of acknowledgement requested from the server for write operations.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/write-concern/) for more
/// information about write concerns.
#[derive(Clone, Debug, Default, PartialEq, TypedBuilder)]
pub struct WriteConcern {
    /// Requests acknowledgement that the operation has propagated to a specific number or variet
    /// of servers.
    #[builder(default)]
    pub w: Option<Acknowledgement>,

    /// Requests acknowledgement that the operation has propagated to the on-disk journal.
    #[builder(default)]
    pub journal: Option<bool>,

    /// Specifies a time limit for the write concern. If an operation has not propagated to the
    /// requested level within the time limit, an error will return.
    ///
    /// Note that an error being returned due to a write concern error does not imply that the
    /// write would not have finished propagating if allowed more time to finish, and the
    /// server will not roll back the writes that occurred before the timeout was reached.
    #[builder(default)]
    pub w_timeout: Option<Duration>,
}

/// The type of the `w` field in a write concern.
#[derive(Clone, Debug, PartialEq)]
pub enum Acknowledgement {
    /// Requires acknowledgement that the write has reached the specified number of nodes.
    Nodes(i32),
    /// Requires acknowledgement that the write has reached the majority of nodes.
    Majority,
    /// Requires acknowledgement that the write has reached a node with the specified replica set
    /// tags.
    Tags(String),
}

impl From<i32> for Acknowledgement {
    fn from(i: i32) -> Self {
        Acknowledgement::Nodes(i)
    }
}

impl From<String> for Acknowledgement {
    fn from(s: String) -> Self {
        if s == "majority" {
            Acknowledgement::Majority
        } else {
            Acknowledgement::Tags(s)
        }
    }
}

impl WriteConcern {
    /// Creates a new write concern with the default configuration.
    pub fn new() -> Self {
        unimplemented!()
    }

    /// Validates that the write concern. A write concern is invalid if the `w` field is 0
    /// and the `j` field is `true`.
    pub fn validate(&self) -> Result<()> {
        unimplemented!()
    }

    /// Converts the write concern into a `Document`. This method will return an error if the write
    /// concern is invalid.
    pub fn into_document(self) -> Result<Document> {
        unimplemented!()
    }
}

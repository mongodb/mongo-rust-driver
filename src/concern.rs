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
    Local,
    Majority,
    Linearizable,
    Available,
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
    #[builder(default)]
    pub w: Option<Acknowledgement>,

    #[builder(default)]
    pub journal: Option<bool>,

    #[builder(default)]
    pub w_timeout: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Acknowledgement {
    Nodes(i32),
    Majority,
    TagSet(String),
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
            Acknowledgement::TagSet(s)
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

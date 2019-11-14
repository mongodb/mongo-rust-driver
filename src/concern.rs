//! Contains the types for read concerns and write concerns.

use std::time::Duration;

use bson::{Bson, Document};
use serde::{Serialize, Serializer};
use serde_with::skip_serializing_none;

use crate::{
    bson_util,
    error::{ErrorKind, Result},
};

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
        self.as_str() == other.as_str()
    }
}

impl ReadConcern {
    /// Gets the string representation of the `ReadConcern`.
    pub fn as_str(&self) -> &str {
        match *self {
            ReadConcern::Local => "local",
            ReadConcern::Majority => "majority",
            ReadConcern::Linearizable => "linearizable",
            ReadConcern::Available => "available",
            ReadConcern::Custom(ref s) => s,
        }
    }
}

/// Specifies the level of acknowledgement requested from the server for write operations.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/write-concern/) for more
/// information about write concerns.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, PartialEq, TypedBuilder, Serialize)]
pub struct WriteConcern {
    /// Requests acknowledgement that the operation has propagated to a specific number or variety
    /// of servers.
    #[builder(default)]
    pub w: Option<Acknowledgment>,

    /// Requests acknowledgement that the operation has propagated to the on-disk journal.
    #[builder(default)]
    #[serde(rename = "j")]
    pub journal: Option<bool>,

    /// Specifies a time limit for the write concern. If an operation has not propagated to the
    /// requested level within the time limit, an error will return.
    ///
    /// Note that an error being returned due to a write concern error does not imply that the
    /// write would not have finished propagating if allowed more time to finish, and the
    /// server will not roll back the writes that occurred before the timeout was reached.
    #[builder(default)]
    #[serde(rename = "wtimeout")]
    #[serde(serialize_with = "bson_util::serialize_duration_as_i64_millis")]
    pub w_timeout: Option<Duration>,
}

/// The type of the `w` field in a write concern.
#[derive(Clone, Debug, PartialEq)]
pub enum Acknowledgment {
    /// Requires acknowledgement that the write has reached the specified number of nodes.
    Nodes(i32),
    /// Requires acknowledgement that the write has reached the majority of nodes.
    Majority,
    /// Requires acknowledgement according to the given write tag. See [here](https://docs.mongodb.com/manual/tutorial/configure-replica-set-tag-sets/#tag-sets-and-custom-write-concern-behavior)
    /// for more information.
    Tag(String),
}

impl Serialize for Acknowledgment {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Acknowledgment::Majority => serializer.serialize_str("majority"),
            Acknowledgment::Nodes(n) => serializer.serialize_i32(n.clone()),
            Acknowledgment::Tag(tag) => serializer.serialize_str(tag),
        }
    }
}

impl From<i32> for Acknowledgment {
    fn from(i: i32) -> Self {
        Acknowledgment::Nodes(i)
    }
}

impl From<String> for Acknowledgment {
    fn from(s: String) -> Self {
        if s == "majority" {
            Acknowledgment::Majority
        } else {
            Acknowledgment::Tag(s)
        }
    }
}

impl Acknowledgment {
    #[allow(dead_code)]
    pub(crate) fn to_bson(&self) -> Bson {
        match self {
            Acknowledgment::Nodes(i) => Bson::I64(i64::from(*i)),
            Acknowledgment::Majority => Bson::String("majority".to_string()),
            Acknowledgment::Tag(s) => Bson::String(s.to_string()),
        }
    }
}

impl WriteConcern {
    /// Validates that the write concern. A write concern is invalid if the `w` field is 0
    /// and the `j` field is `true`.
    pub fn validate(&self) -> Result<()> {
        if let Some(Acknowledgment::Nodes(i)) = self.w {
            if i < 0 {
                return Err(ErrorKind::ArgumentError {
                    message: "write concern `w` field cannot be negative integer".to_string(),
                }
                .into());
            }
        }

        if self.w == Some(Acknowledgment::Nodes(0)) && self.journal == Some(true) {
            return Err(ErrorKind::ArgumentError {
                message: "write concern cannot have w=0 and j=true".to_string(),
            }
            .into());
        }

        if let Some(w_timeout) = self.w_timeout {
            if w_timeout < Duration::from_millis(0) {
                return Err(ErrorKind::ArgumentError {
                    message: "write concern `w_timeout` field cannot be negative".to_string(),
                }
                .into());
            }
        }

        Ok(())
    }

    pub(crate) fn to_bson(&self) -> Bson {
        let mut doc = Document::new();

        if let Some(ref w) = self.w {
            doc.insert("w", w.to_bson());
        }

        if let Some(j) = self.journal {
            doc.insert("j", j);
        }

        if let Some(wtimeout) = self.w_timeout {
            doc.insert("wtimeout", wtimeout.as_millis() as i64);
        }

        Bson::Document(doc)
    }
}

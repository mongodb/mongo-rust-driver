//! Contains the types for read concerns and write concerns.

use std::time::Duration as StdDuration;

use bson::Document;
use time::Duration;

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

// We need `WType` to be a public type in order to use it as part of the `WriteConcern::w` method
// signature, but using it directly is verbose and unnecessary when we can allow users to just pass
// anything that coerces to it in that method. The workaround for this is to make `WType` a public
// type in a private module, so the compiler views the type as public even though its inaccessible
// to users.
mod detail {
    #[derive(Clone, Debug, PartialEq)]
    pub enum WType {
        Int(i32),
        Str(String),
    }
}

use self::detail::WType;

/// Specifies the level of acknowledgement requested from the server for write operations.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/write-concern/) for more
/// information about write concerns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteConcern {
    journal: Option<bool>,
    w: Option<WType>,
    w_timeout: Option<Duration>,
}

impl From<i32> for WType {
    fn from(i: i32) -> Self {
        WType::Int(i)
    }
}

impl From<String> for WType {
    fn from(s: String) -> Self {
        WType::Str(s)
    }
}

impl<'a> From<&'a str> for WType {
    fn from(s: &'a str) -> Self {
        WType::Str(s.to_string())
    }
}

impl WriteConcern {
    /// Creates a new write concern with the default configuration.
    pub fn new() -> Self {
        unimplemented!()
    }

    /// Sets the `j` field of the write concern. If `j` is `None`, this will clear any previously
    /// set value.
    pub fn journal(self, j: impl Into<Option<bool>>) -> Self {
        unimplemented!()
    }

    /// Sets the `w` field of the write concern. If `w` is `None`, this will clear any previously
    /// set value.
    ///
    /// `w` must be one the following types:
    ///   * `i32`
    ///   * `&str`
    ///   * `String`
    pub fn w(self, w: impl Into<Option<WType>>) -> Self {
        unimplemented!()
    }

    /// Sets the `w` field of the write concern to "majority".
    pub fn w_majority(self) -> Self {
        unimplemented!()
    }

    /// Sets the `wtimeoutMS` field of the write concern. If `timeout` is `None`, this will clear
    /// any previously set value.
    pub fn w_timeout(self, timeout: impl Into<Option<StdDuration>>) -> Self {
        unimplemented!()
    }

    /// Validates that the write concern is valid. A write concern is invalid if the `w` field is 0
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

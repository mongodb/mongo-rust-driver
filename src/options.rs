//! Contains all of the types needed to specify options to MongoDB operations.
//!
//! Most of the options structs in this module use the
//! [`typed-builder`](https://crates.io/crates/typed-builder) crate to derive a type-safe builder
//! API on them. For example, to create an instance of
//! [`FindOptions`](struct.FindOptions.html) with only `limit` and `batch_size` set, the builder
//! API can be used as follows:
//!
//! ```rust
//! use mongodb::options::FindOptions;
//!
//! let options = FindOptions::builder()
//!                   .limit(20)
//!                   .batch_size(5)
//!                   .build();
//! ```

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
pub use crate::compression::compressors::Compressor;
pub use crate::{
    change_stream::options::*,
    client::{
        auth::{oidc, *},
        options::*,
    },
    coll::options::*,
    collation::*,
    concern::*,
    db::options::*,
    gridfs::options::*,
    index::options::*,
    search_index::options::*,
    selection_criteria::*,
};

/// Updates an options struct with the read preference/read concern/write concern of a
/// client/database/collection.
macro_rules! resolve_options {
    ($obj:expr, $opts:expr, [$( $field:ident ),+] ) => {
        $(
            if let Some(option) = $obj.$field() {
                let options = $opts.get_or_insert_with(Default::default);
                if !options.$field.is_some() {
                    options.$field = Some(option.clone());
                }
            }
        )+
    };
}

/// Merges the options from src into dst.
macro_rules! merge_options {
    ($src:expr, $dst:expr, [$( $field:ident ),+] ) => {
        $(
            if let Some(ref option) = $src.$field {
                if !$dst.$field.is_some() {
                    $dst.$field = Some(option.clone());
                }
            }
        )+
    };
}

/// Updates the read concern of an options struct. If a transaction is starting or in progress,
/// return an error if a read concern was specified for the operation. Otherwise, inherit the read
/// concern from the collection/database.
macro_rules! resolve_read_concern_with_session {
    ($obj:expr, $opts:expr, $session:expr) => {{
        resolve_rw_concern_with_session!($obj, $opts, $session, read_concern, "read")
    }};
}

/// Updates the read concern of an options struct. If a transaction is starting or in progress,
/// return an error if a write concern was specified for the operation. Otherwise, inherit the write
/// concern from the collection/database.
macro_rules! resolve_write_concern_with_session {
    ($obj:expr, $opts:expr, $session:expr) => {{
        resolve_rw_concern_with_session!($obj, $opts, $session, write_concern, "write")
    }};
}

macro_rules! resolve_rw_concern_with_session {
    ($obj:expr, $opts:expr, $session:expr, $concern:ident, $name:expr) => {{
        use crate::client::session::TransactionState;
        if let Some(session) = $session {
            match session.transaction.state {
                TransactionState::Starting | TransactionState::InProgress => {
                    if $opts
                        .as_ref()
                        .map(|opts| opts.$concern.is_some())
                        .unwrap_or(false)
                    {
                        return Err(crate::error::ErrorKind::InvalidArgument {
                            message: format!(
                                "Cannot set {} concern after starting a transaction",
                                $name
                            ),
                        }
                        .into());
                    }
                }
                _ => {
                    resolve_options!($obj, $opts, [$concern]);
                }
            }
        } else {
            resolve_options!($obj, $opts, [$concern]);
        }
        let result: Result<()> = Ok(());
        result
    }};
}

/// Updates the selection criteria of an options struct. If a transaction is starting or in progress
/// and the selection criteria is not configured directly on the operation, inherit the selection
/// criteria from the transaction options. Otherwise, use the selection criteria defined on the
/// operation or inherit it from the collection/database.
macro_rules! resolve_selection_criteria_with_session {
    ($obj:expr, $opts:expr, $session:expr) => {{
        use crate::client::session::TransactionState;
        if let Some(session) = $session {
            match session.transaction.state {
                TransactionState::Starting | TransactionState::InProgress => {
                    if let Some(ref options) = session.transaction.options {
                        if let Some(ref selection_criteria) = options.selection_criteria {
                            if $opts
                                .as_ref()
                                .map(|opts| opts.selection_criteria.is_none())
                                .unwrap_or(true)
                            {
                                $opts
                                    .get_or_insert_with(Default::default)
                                    .selection_criteria = Some(selection_criteria.clone());
                            }
                        }
                    }
                }
                _ => {
                    resolve_options!($obj, $opts, [selection_criteria]);
                }
            }
        } else {
            resolve_options!($obj, $opts, [selection_criteria]);
        }
        let result: Result<()> = Ok(());
        result
    }};
}

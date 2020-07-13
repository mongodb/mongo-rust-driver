//! Contains all of the types needed to specify options to MongoDB operations.
//!
//! Most of the options structs in this module use the
//! [`typed-builder`](https://crates.io/crates/typed-builder) crate to derive a type-safe builder
//! API on them. For example, to create an instance of
//! [`FindOptions`](struct.FindOptions.html) with only `limit` and `batch_size` set, the builder
//! API can be used as follows:
//!
//! ```rust
//! # use mongodb::options::FindOptions;
//! #
//! # let options = FindOptions::builder()
//! #                   .limit(20)
//! #                   .batch_size(5)
//! #                   .build();
//! ```

pub use crate::{
    client::{auth::*, options::*},
    coll::options::*,
    collation::*,
    concern::*,
    db::options::*,
    selection_criteria::*,
};

/// Updates an options struct with the read preference/read concern/write concern of a
/// client/database/collection.
macro_rules! resolve_options {
    ($obj:expr, $opts:expr, [$( $field:ident ),+] ) => {
        $(
            if let Some(option) = $obj.$field() {
                if !$opts
                    .as_ref()
                    .map(|opts| opts.$field.is_some())
                    .unwrap_or(false)
                {
                    $opts.get_or_insert_with(Default::default).$field = Some(option.clone());
                }
            }
        )+
    };
}

/// Merges the options from src into dst.
#[cfg(test)]
macro_rules! merge_options {
    ($src:expr, $dst:expr, [$( $field:ident ),+] ) => {
        $(
            if let Some(option) = $src.$field {
                if !$dst.$field.is_some() {
                    $dst.$field = Some(option.clone());
                }
            }
        )+
    };
}

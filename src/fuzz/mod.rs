//! Fuzzing support module for MongoDB wire protocol messages

#[cfg(feature = "fuzzing")]
pub mod message_flags;
#[cfg(feature = "fuzzing")]
pub mod raw_document;

#[cfg(feature = "fuzzing")]
pub use message_flags::MessageFlags;
#[cfg(feature = "fuzzing")]
pub use raw_document::{FuzzDocumentSequenceImpl, FuzzRawDocumentImpl};

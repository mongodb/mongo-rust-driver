pub mod header;
pub mod message;
pub mod util;

pub(crate) use self::{
    util::next_request_id,
};

#[cfg(feature = "fuzzing")]
pub use self::{
    message::Message,
};

#[cfg(feature = "fuzzing")]
pub use crate::fuzz::message_flags::MessageFlags;

#[cfg(not(feature = "fuzzing"))]
pub use self::message::MessageFlags;

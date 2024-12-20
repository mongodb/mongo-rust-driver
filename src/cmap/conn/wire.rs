#[cfg(feature = "fuzzing")]
#[allow(missing_docs)]
pub mod header;
#[cfg(not(feature = "fuzzing"))]
mod header;
#[cfg(feature = "fuzzing")]
#[allow(missing_docs)]
pub mod message;
#[cfg(not(feature = "fuzzing"))]
pub(crate) mod message;
#[cfg(feature = "fuzzing")]
#[allow(missing_docs)]
pub mod util;
#[cfg(not(feature = "fuzzing"))]
mod util;

pub(crate) use self::util::next_request_id;

#[cfg(feature = "fuzzing")]
pub use self::message::Message;

#[cfg(feature = "fuzzing")]
pub use crate::fuzz::message_flags::MessageFlags;

#[cfg(not(feature = "fuzzing"))]
pub use message::{Message, MessageFlags};

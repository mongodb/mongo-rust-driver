use crate::{
    concern::{ReadConcern, WriteConcern},
    read_preference::ReadPreference,
};

/// These are the valid options for creating a `Database` with `Client::database_with_options`.
#[derive(Debug, Default, TypedBuilder)]
pub struct Options {
    /// The default read preference for operations.
    #[builder(default)]
    pub read_preference: Option<ReadPreference>,

    /// The default read concern for operations.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The default write concern for operations.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

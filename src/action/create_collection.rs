use crate::{Client, Database};

impl Database {
    /// Creates a new collection in the database with the given `name`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    /// 
    /// `await` will return `Result<()>`.
    pub fn create_collection_2(
        &self,
        name: impl AsRef<str>,
    ) -> CreateCollection {
        todo!()
    }
}

/// Creates a new collection.  Create by calling [`Database::create_collection`] and execute with `await` (or [`run`](CreateCollection::run) if using the sync client).
pub struct CreateCollection<'a> {
    db: &'a Database,
}
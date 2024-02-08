use crate::{
    coll::options::DropCollectionOptions,
    db::options::DropDatabaseOptions,
    error::Result,
    operation::drop_database,
    options::WriteConcern,
    ClientSession,
    Collection,
    Database,
};

use super::{action_impl, option_setters};

impl Database {
    /// Drops the database, deleting all data, collections, and indexes stored in it.
    ///
    /// `await` will return `Result<()>`.
    pub fn drop(&self) -> DropDatabase {
        DropDatabase {
            db: self,
            options: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Database {
    /// Drops the database, deleting all data, collections, and indexes stored in it.
    ///
    /// [`run`](DropDatabase::run) will return `Result<()>`.
    pub fn drop(&self) -> DropDatabase {
        self.async_database.drop()
    }
}

/// Drops the database, deleting all data, collections, and indexes stored in it.  Create by calling
/// [`Database::drop`].
#[must_use]
pub struct DropDatabase<'a> {
    db: &'a Database,
    options: Option<DropDatabaseOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> DropDatabase<'a> {
    option_setters!(options: DropDatabaseOptions;
        write_concern: WriteConcern,
    );

    /// Runs the drop using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl<'a> Action for DropDatabase<'a> {
        type Future = DropDatabaseFuture;

        async fn execute(mut self) -> Result<()> {
            resolve_options!(self.db, self.options, [write_concern]);
            let op = drop_database::DropDatabase::new(self.db.name().to_string(), self.options);
            self.db.client()
                .execute_operation(op, self.session)
                .await
        }
    }
}

impl<T> Collection<T> {
    /// Drops the collection, deleting all data and indexes stored in it.
    ///
    /// `await` will return `Result<()>`.
    pub fn drop(&self) -> DropCollection {
        DropCollection {
            coll: self.ref_untyped(),
            options: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<T> crate::sync::Collection<T> {
    /// Drops the collection, deleting all data and indexes stored in it.
    ///
    /// [`run`](DropCollection::run) will return `Result<()>`.
    pub fn drop(&self) -> DropCollection {
        self.async_collection.drop()
    }
}

/// Drops the collection, deleting all data and indexes stored in it.  Create by calling
/// [`Collection::drop`].
#[must_use]
pub struct DropCollection<'a> {
    pub(crate) coll: &'a Collection<bson::Document>,
    pub(crate) options: Option<DropCollectionOptions>,
    pub(crate) session: Option<&'a mut ClientSession>,
}

impl<'a> DropCollection<'a> {
    option_setters!(options: DropCollectionOptions;
        write_concern: WriteConcern,
        #[cfg(feature = "in-use-encryption-unstable")]
        encrypted_fields: bson::Document,
    );

    /// Runs the drop using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

// Action impl in src/coll/action/drop.rs

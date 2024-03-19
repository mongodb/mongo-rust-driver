use std::time::Duration;

use bson::Document;

use crate::coll::options::{FindOneOptions, FindOptions};
use crate::gridfs::{FilesCollectionDocument, GridFsFindOneOptions, GridFsFindOptions};
use crate::{Cursor, GridFsBucket};
use crate::action::{action_impl, option_setters};
use crate::error::Result;

impl GridFsBucket {
    /// Finds and returns the [`FilesCollectionDocument`]s within this bucket that match the given
    /// filter.
    /// 
    /// `await` will return `Result<Cursor<FilesCollectionDocument>>`.
    pub fn find_2(&self, filter: Document) -> Find {
        Find { bucket: self, filter, options: None }
    }

    /// Finds and returns a single [`FilesCollectionDocument`] within this bucket that matches the
    /// given filter.
    /// 
    /// `await` will return `Result<Option<FilesCollectionDocument>>`.
    pub fn find_one_2(&self, filter: Document) -> FindOne {
        FindOne { bucket: self, filter, options: None }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::gridfs::GridFsBucket {
    /// Finds and returns the [`FilesCollectionDocument`]s within this bucket that match the given
    /// filter.
    /// 
    /// [`run`](Find::run) will return `Result<Cursor<FilesCollectionDocument>>`.
    pub fn find_2(&self, filter: Document) -> Find {
        self.async_bucket.find_2(filter)
    }

    /// Finds and returns a single [`FilesCollectionDocument`] within this bucket that matches the
    /// given filter.
    /// 
    /// [`run`](FindOne::run) will return `Result<Option<FilesCollectionDocument>>`.
    pub fn find_one_2(&self, filter: Document) -> FindOne {
        self.async_bucket.find_one_2(filter)
    }
}

/// Finds and returns the [`FilesCollectionDocument`]s within a bucket that match a given
/// filter.  Construct with [`GridFsBucket::find`].
#[must_use]
pub struct Find<'a> {
    bucket: &'a GridFsBucket,
    filter: Document,
    options: Option<GridFsFindOptions>,
}

impl<'a> Find<'a> {
    option_setters! { options: GridFsFindOptions;
        allow_disk_use: bool,
        batch_size: u32,
        limit: i64,
        max_time: Duration,
        skip: u64,
        sort: Document,
    }
}

action_impl! {
    impl<'a> Action for Find<'a> {
        type Future = FindFuture;

        async fn execute(self) -> Result<Cursor<FilesCollectionDocument>> {
            let find_options = self.options.map(FindOptions::from);
            self.bucket.files().find(self.filter).with_options(find_options).await
        }

        fn sync_wrap(out) -> Result<crate::sync::Cursor<FilesCollectionDocument>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

/// Finds and returns a single [`FilesCollectionDocument`] within a bucket that matches a
/// given filter.  Construct with [`GridFsBucket::find_one`].
#[must_use]
pub struct FindOne<'a> {
    bucket: &'a GridFsBucket,
    filter: Document,
    options: Option<GridFsFindOneOptions>,
}

impl<'a> FindOne<'a> {
    option_setters! { options: GridFsFindOneOptions;
        max_time: Duration,
        skip: u64,
        sort: Document,
    }
}

action_impl! {
    impl<'a> Action for FindOne<'a> {
        type Future = FindOneFuture;

        async fn execute(self) -> Result<Option<FilesCollectionDocument>> {
            let find_options = self.options.map(FindOneOptions::from);
            self.bucket
                .files()
                .find_one(self.filter)
                .with_options(find_options)
                .await
        }
    }
}
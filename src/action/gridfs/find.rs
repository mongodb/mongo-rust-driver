use std::time::Duration;

use bson::Document;

use crate::coll::options::FindOptions;
use crate::gridfs::{FilesCollectionDocument, GridFsFindOptions};
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
}

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
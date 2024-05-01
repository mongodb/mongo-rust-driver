use bson::{doc, Bson};

use crate::{action::action_impl, error::Result, gridfs::GridFsBucket};

impl GridFsBucket {
    /// Renames the file with the given 'id' to the provided `new_filename`. This method returns an
    /// error if the `id` does not match any files in the bucket.
    ///
    /// `await` will return [`Result<()>`].
    pub fn rename(&self, id: Bson, new_filename: impl Into<String>) -> Rename {
        Rename {
            bucket: self,
            id,
            new_filename: new_filename.into(),
        }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::gridfs::GridFsBucket {
    /// Renames the file with the given `id` to the provided `new_filename`. This method returns an
    /// error if the `id` does not match any files in the bucket.
    ///
    /// [`run`](Rename::run) will return [`Result<()>`].
    pub fn rename(&self, id: Bson, new_filename: impl Into<String>) -> Rename {
        self.async_bucket.rename(id, new_filename)
    }
}

/// Renames a file.  Construct with [`GridFsBucket::rename`].
#[must_use]
pub struct Rename<'a> {
    bucket: &'a GridFsBucket,
    id: Bson,
    new_filename: String,
}

#[action_impl]
impl<'a> Action for Rename<'a> {
    type Future = RenameFuture;

    async fn execute(self) -> Result<()> {
        self.bucket
            .files()
            .update_one(
                doc! { "_id": self.id },
                doc! { "$set": { "filename": self.new_filename } },
            )
            .await?;

        Ok(())
    }
}

use bson::{doc, Bson};

use crate::{
    action::action_impl,
    error::{ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    GridFsBucket,
};

impl GridFsBucket {
    /// Deletes the [`FilesCollectionDocument`] with the given `id` and its associated chunks from
    /// this bucket. This method returns an error if the `id` does not match any files in the
    /// bucket.
    ///
    /// `await` will return `Result<()>`.
    pub fn delete_2(&self, id: Bson) -> Delete {
        Delete { bucket: self, id }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::gridfs::GridFsBucket {
    /// Deletes the [`FilesCollectionDocument`] with the given `id` and its associated chunks from
    /// this bucket. This method returns an error if the `id` does not match any files in the
    /// bucket.
    ///
    /// [`run`](Delete::run) will return `Result<()>`.
    pub fn delete_2(&self, id: Bson) -> Delete {
        self.async_bucket.delete_2(id)
    }
}

/// Deletes a specific [`FilesCollectionDocument`] and its associated chunks.  Construct with
/// [`GridFsBucket::delete`].
#[must_use]
pub struct Delete<'a> {
    bucket: &'a GridFsBucket,
    id: Bson,
}

action_impl! {
    impl<'a> Action for Delete<'a> {
        type Future = DeleteFuture;

        async fn execute(self) -> Result<()> {
            let delete_result = self.bucket.files().delete_one(doc! { "_id": self.id.clone() }).await?;
            // Delete chunks regardless of whether a file was found. This will remove any possibly
            // orphaned chunks.
            self.bucket
                .chunks()
                .delete_many(doc! { "files_id": self.id.clone() })
                .await?;

            if delete_result.deleted_count == 0 {
                return Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                    identifier: GridFsFileIdentifier::Id(self.id),
                })
                .into());
            }

            Ok(())
        }
    }
}

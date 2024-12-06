use bson::{doc, Bson, Document};

use super::super::{action_impl, option_setters};
use crate::{
    client_encryption::{ClientEncryption, MasterKey},
    db::options::CreateCollectionOptions,
    error::{Error, Result},
    Database,
};

impl ClientEncryption {
    /// Creates a new collection with encrypted fields, automatically creating new data encryption
    /// keys when needed based on the configured [`CreateCollectionOptions::encrypted_fields`].
    ///
    /// `await` will return `(Document, Result<()>)` containing the potentially updated
    /// `encrypted_fields` along with the collection creation status, as keys may have been created
    /// even when a failure occurs.
    ///
    /// Does not affect any auto encryption settings on existing MongoClients that are already
    /// configured with auto encryption.
    pub fn create_encrypted_collection<'a>(
        &'a self,
        db: &'a Database,
        name: &'a str,
        master_key: MasterKey,
    ) -> CreateEncryptedCollection<'a> {
        CreateEncryptedCollection {
            client_enc: self,
            db,
            name,
            master_key,
            options: None,
        }
    }
}

/// Create a new collection with encrypted fields.  Construct with
/// [`ClientEncryption::create_encrypted_collection`].
#[must_use]
pub struct CreateEncryptedCollection<'a> {
    client_enc: &'a ClientEncryption,
    db: &'a Database,
    name: &'a str,
    master_key: MasterKey,
    options: Option<CreateCollectionOptions>,
}

impl<'a> CreateEncryptedCollection<'a> {
    option_setters!(options: CreateCollectionOptions;
        capped: bool,
        size: u64,
        max: u64,
        storage_engine: Document,
        validator: Document,
        validation_level: crate::db::options::ValidationLevel,
        validation_action: crate::db::options::ValidationAction,
        view_on: String,
        pipeline: Vec<Document>,
        collation: crate::collation::Collation,
        write_concern: crate::options::WriteConcern,
        index_option_defaults: crate::db::options::IndexOptionDefaults,
        timeseries: crate::db::options::TimeseriesOptions,
        expire_after_seconds: std::time::Duration,
        change_stream_pre_and_post_images: crate::db::options::ChangeStreamPreAndPostImages,
        clustered_index: crate::db::options::ClusteredIndex,
        comment: bson::Bson,
        encrypted_fields: Document,
    );
}

#[action_impl]
impl<'a> Action for CreateEncryptedCollection<'a> {
    type Future = CreateEncryptedCollectionFuture;

    async fn execute(self) -> (Document, Result<()>) {
        let ef = match self
            .options
            .as_ref()
            .and_then(|o| o.encrypted_fields.as_ref())
        {
            Some(ef) => ef,
            None => {
                return (
                    doc! {},
                    Err(Error::invalid_argument(
                        "no encrypted_fields defined for collection",
                    )),
                );
            }
        };
        let mut ef_prime = ef.clone();
        if let Ok(fields) = ef_prime.get_array_mut("fields") {
            for f in fields {
                let f_doc = if let Some(d) = f.as_document_mut() {
                    d
                } else {
                    continue;
                };
                if f_doc.get("keyId") == Some(&Bson::Null) {
                    let d = match self
                        .client_enc
                        .create_data_key(self.master_key.clone())
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return (ef_prime, Err(e)),
                    };
                    f_doc.insert("keyId", d);
                }
            }
        }
        // Unwrap safety: the check for `encrypted_fields` at the top won't succeed if
        // `self.options` is `None`.
        let mut opts_prime = self.options.unwrap();
        opts_prime.encrypted_fields = Some(ef_prime.clone());
        (
            ef_prime,
            self.db
                .create_collection(self.name)
                .with_options(opts_prime)
                .await,
        )
    }
}

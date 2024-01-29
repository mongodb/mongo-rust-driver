use bson::{doc, Document};

use crate::{db::options::CreateCollectionOptions, error::Result, operation::Create, ClientSession, Database, Namespace};

impl Database {
    #[allow(clippy::needless_option_as_deref)]
    pub(crate) async fn create_collection_common(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<CreateCollectionOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<()> {
        let mut options: Option<CreateCollectionOptions> = options.into();
        resolve_options!(self, options, [write_concern]);
        let mut session = session.into();

        let ns = Namespace {
            db: self.name().to_string(),
            coll: name.as_ref().to_string(),
        };

        #[cfg(feature = "in-use-encryption-unstable")]
        let has_encrypted_fields = {
            self.resolve_encrypted_fields(&ns, &mut options).await;
            self.create_aux_collections(&ns, &options, session.as_deref_mut())
                .await?;
            options
                .as_ref()
                .and_then(|o| o.encrypted_fields.as_ref())
                .is_some()
        };

        let create = Create::new(ns.clone(), options);
        self.client()
            .execute_operation(create, session.as_deref_mut())
            .await?;

        #[cfg(feature = "in-use-encryption-unstable")]
        if has_encrypted_fields {
            let coll = self.collection::<Document>(&ns.coll);
            coll.create_index_common(
                crate::IndexModel {
                    keys: doc! {"__safeContent__": 1},
                    options: None,
                },
                None,
                session.as_deref_mut(),
            )
            .await?;
        }

        Ok(())
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    async fn resolve_encrypted_fields(
        &self,
        base_ns: &Namespace,
        options: &mut Option<CreateCollectionOptions>,
    ) {
        let has_encrypted_fields = options
            .as_ref()
            .and_then(|o| o.encrypted_fields.as_ref())
            .is_some();
        // If options does not have `associated_fields`, populate it from client-wide
        // `encrypted_fields_map`:
        if !has_encrypted_fields {
            let enc_opts = self.client().auto_encryption_opts().await;
            if let Some(enc_opts_fields) = enc_opts
                .as_ref()
                .and_then(|eo| eo.encrypted_fields_map.as_ref())
                .and_then(|efm| efm.get(&format!("{}", &base_ns)))
            {
                options
                    .get_or_insert_with(Default::default)
                    .encrypted_fields = Some(enc_opts_fields.clone());
            }
        }
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    #[allow(clippy::needless_option_as_deref)]
    async fn create_aux_collections(
        &self,
        base_ns: &Namespace,
        options: &Option<CreateCollectionOptions>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<()> {
        use crate::error::ErrorKind;

        let opts = match options {
            Some(o) => o,
            None => return Ok(()),
        };
        let enc_fields = match &opts.encrypted_fields {
            Some(f) => f,
            None => return Ok(()),
        };
        let max_wire = match self.client().primary_description().await {
            Some(p) => p.max_wire_version()?,
            None => None,
        };
        const SERVER_7_0_0_WIRE_VERSION: i32 = 21;
        match max_wire {
            None => (),
            Some(v) if v >= SERVER_7_0_0_WIRE_VERSION => (),
            _ => {
                return Err(ErrorKind::IncompatibleServer {
                    message: "Driver support of Queryable Encryption is incompatible with server. \
                              Upgrade server to use Queryable Encryption."
                        .to_string(),
                }
                .into())
            }
        }
        for ns in crate::client::csfle::aux_collections(base_ns, enc_fields)? {
            let mut sub_opts = opts.clone();
            sub_opts.clustered_index = Some(crate::db::options::ClusteredIndex {
                key: doc! { "_id": 1 },
                unique: true,
                name: None,
                v: None,
            });
            let create = Create::new(ns, Some(sub_opts));
            self.client()
                .execute_operation(create, session.as_deref_mut())
                .await?;
        }
        Ok(())
    }
}
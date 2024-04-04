use crate::{
    action::{action_impl, CreateCollection},
    error::Result,
    operation::create as op,
    Database,
    Namespace,
};

#[action_impl]
impl<'a> Action for CreateCollection<'a> {
    type Future = CreateCollectionFuture;

    async fn execute(mut self) -> Result<()> {
        resolve_options!(self.db, self.options, [write_concern]);

        let ns = Namespace {
            db: self.db.name().to_string(),
            coll: self.name,
        };

        #[cfg(feature = "in-use-encryption-unstable")]
        let has_encrypted_fields = {
            self.db
                .resolve_encrypted_fields(&ns, &mut self.options)
                .await;
            self.db
                .create_aux_collections(&ns, &self.options, self.session.as_deref_mut())
                .await?;
            self.options
                .as_ref()
                .and_then(|o| o.encrypted_fields.as_ref())
                .is_some()
        };

        let create = op::Create::new(ns.clone(), self.options);
        self.db
            .client()
            .execute_operation(create, self.session.as_deref_mut())
            .await?;

        #[cfg(feature = "in-use-encryption-unstable")]
        if has_encrypted_fields {
            use crate::action::Action;
            use bson::{doc, Document};
            let coll = self.db.collection::<Document>(&ns.coll);
            coll.create_index(crate::IndexModel {
                keys: doc! {"__safeContent__": 1},
                options: None,
            })
            .optional(self.session.as_deref_mut(), |a, s| a.session(s))
            .await?;
        }

        Ok(())
    }
}

impl Database {
    #[cfg(feature = "in-use-encryption-unstable")]
    async fn resolve_encrypted_fields(
        &self,
        base_ns: &Namespace,
        options: &mut Option<crate::db::options::CreateCollectionOptions>,
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
        options: &Option<crate::db::options::CreateCollectionOptions>,
        mut session: Option<&mut crate::ClientSession>,
    ) -> Result<()> {
        use crate::error::ErrorKind;
        use bson::doc;

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
            let create = op::Create::new(ns, Some(sub_opts));
            self.client()
                .execute_operation(create, session.as_deref_mut())
                .await?;
        }
        Ok(())
    }
}

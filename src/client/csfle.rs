pub(crate) mod client_builder;
pub mod client_encryption;
pub mod options;
pub(crate) mod state_machine;

use std::{path::Path, time::Duration};

use derive_where::derive_where;
use mongocrypt::Crypt;

use crate::{
    error::{Error, Result},
    Client,
    Namespace,
};

use options::{
    AutoEncryptionOptions,
    EO_CRYPT_SHARED_LIB_PATH,
    EO_CRYPT_SHARED_REQUIRED,
    EO_MONGOCRYPTD_BYPASS_SPAWN,
    EO_MONGOCRYPTD_SPAWN_ARGS,
    EO_MONGOCRYPTD_SPAWN_PATH,
    EO_MONGOCRYPTD_URI,
};

use self::state_machine::{CryptExecutor, MongocryptdOptions};

use super::WeakClient;

#[derive_where(Debug)]
pub(super) struct ClientState {
    #[derive_where(skip)]
    crypt: Crypt,
    exec: CryptExecutor,
    #[allow(dead_code)]
    internal_client: Option<Client>,
    opts: AutoEncryptionOptions,
}

struct AuxClients {
    key_vault_client: WeakClient,
    metadata_client: Option<WeakClient>,
    internal_client: Option<Client>,
}

impl ClientState {
    const MONGOCRYPTD_DEFAULT_URI: &'static str = "mongodb://localhost:27020";
    const MONGOCRYPTD_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_millis(10_000);

    pub(super) async fn new(client: &Client, opts: AutoEncryptionOptions) -> Result<Self> {
        let crypt = Self::make_crypt(&opts)?;
        let mongocryptd_opts = Self::make_mongocryptd_opts(&opts, &crypt)?;
        let aux_clients = Self::make_aux_clients(client, &opts)?;
        let mongocryptd_connect = opts.bypass_auto_encryption != Some(true)
            && opts.bypass_query_analysis != Some(true)
            && crypt.shared_lib_version().is_none()
            && opts.extra_option(&EO_CRYPT_SHARED_REQUIRED)? != Some(true);
        let mongocryptd_client = if mongocryptd_connect {
            let uri = opts
                .extra_option(&EO_MONGOCRYPTD_URI)?
                .unwrap_or(Self::MONGOCRYPTD_DEFAULT_URI);
            let mut options = crate::options::ClientOptions::parse(uri).await?;
            options.server_selection_timeout = Some(Self::MONGOCRYPTD_SERVER_SELECTION_TIMEOUT);
            Some(Client::with_options(options)?)
        } else {
            None
        };
        let exec = CryptExecutor::new_implicit(
            aux_clients.key_vault_client,
            opts.key_vault_namespace.clone(),
            opts.kms_providers.clone(),
            mongocryptd_opts,
            mongocryptd_client,
            aux_clients.metadata_client,
        )
        .await?;

        Ok(Self {
            crypt,
            exec,
            internal_client: aux_clients.internal_client,
            opts,
        })
    }

    pub(super) fn crypt(&self) -> &Crypt {
        &self.crypt
    }

    pub(super) fn exec(&self) -> &CryptExecutor {
        &self.exec
    }

    pub(super) fn opts(&self) -> &AutoEncryptionOptions {
        &self.opts
    }

    fn make_crypt(opts: &AutoEncryptionOptions) -> Result<Crypt> {
        let mut builder = Crypt::builder()
            .kms_providers(&opts.kms_providers.credentials_doc()?)?
            .use_need_kms_credentials_state()
            .use_range_v2()?;
        if let Some(m) = &opts.schema_map {
            builder = builder.schema_map(&bson::to_document(m)?)?;
        }
        if let Some(m) = &opts.encrypted_fields_map {
            builder = builder.encrypted_field_config_map(&bson::to_document(m)?)?;
        }
        #[cfg(not(test))]
        let disable_crypt_shared = false;
        #[cfg(test)]
        let disable_crypt_shared = opts.disable_crypt_shared.unwrap_or(false);
        if !disable_crypt_shared {
            if Some(true) != opts.bypass_auto_encryption {
                builder = builder.append_crypt_shared_lib_search_path(Path::new("$SYSTEM"))?;
            }
            if let Some(p) = opts.extra_option(&EO_CRYPT_SHARED_LIB_PATH)? {
                builder = builder.set_crypt_shared_lib_path_override(Path::new(p))?;
            }
        }
        if opts.bypass_query_analysis == Some(true) {
            builder = builder.bypass_query_analysis();
        }
        let crypt = builder.build()?;
        if opts.extra_option(&EO_CRYPT_SHARED_REQUIRED)? == Some(true)
            && crypt.shared_lib_version().is_none()
        {
            return Err(crate::error::Error::invalid_argument(
                "cryptSharedRequired is set but crypt_shared is not available",
            ));
        }
        Ok(crypt)
    }

    fn make_mongocryptd_opts(
        opts: &AutoEncryptionOptions,
        crypt: &Crypt,
    ) -> Result<Option<MongocryptdOptions>> {
        if opts.bypass_auto_encryption == Some(true)
            || opts.bypass_query_analysis == Some(true)
            || opts.extra_option(&EO_MONGOCRYPTD_BYPASS_SPAWN)? == Some(true)
            || crypt.shared_lib_version().is_some()
            || opts.extra_option(&EO_CRYPT_SHARED_REQUIRED)? == Some(true)
        {
            return Ok(None);
        }
        let spawn_path = opts
            .extra_option(&EO_MONGOCRYPTD_SPAWN_PATH)?
            .map(std::path::PathBuf::from);
        let mut spawn_args = vec![];
        if let Some(args) = opts.extra_option(&EO_MONGOCRYPTD_SPAWN_ARGS)? {
            for arg in args {
                let str_arg = arg.as_str().ok_or_else(|| {
                    Error::invalid_argument("non-string entry in mongocryptdSpawnArgs")
                })?;
                spawn_args.push(str_arg.to_string());
            }
        }
        Ok(Some(MongocryptdOptions {
            spawn_path,
            spawn_args,
        }))
    }

    fn make_aux_clients(
        client: &Client,
        auto_enc_opts: &AutoEncryptionOptions,
    ) -> Result<AuxClients> {
        let mut internal_client: Option<Client> = None;
        let mut get_internal_client = || -> Result<WeakClient> {
            if let Some(c) = &internal_client {
                return Ok(c.weak());
            }
            let mut internal_opts = client.inner.options.clone();
            internal_opts.min_pool_size = Some(0);
            let c = Client::with_options(internal_opts)?;
            internal_client = Some(c.clone());
            Ok(c.weak())
        };

        let key_vault_client = if let Some(c) = &auto_enc_opts.key_vault_client {
            c.weak()
        } else if Some(0) == client.inner.options.max_pool_size {
            client.weak()
        } else {
            get_internal_client()?
        };
        let metadata_client = if Some(true) == auto_enc_opts.bypass_auto_encryption {
            None
        } else if Some(0) == client.inner.options.max_pool_size {
            Some(client.weak())
        } else {
            Some(get_internal_client()?)
        };

        Ok(AuxClients {
            key_vault_client,
            metadata_client,
            internal_client,
        })
    }
}

pub(crate) fn aux_collections(
    base_ns: &Namespace,
    enc_fields: &bson::Document,
) -> Result<Vec<Namespace>> {
    let mut out = vec![];
    for &key in &["esc", "ecoc"] {
        let coll = match enc_fields.get_str(format!("{}Collection", key)) {
            Ok(s) => s.to_string(),
            Err(_) => format!("enxcol_.{}.{}", base_ns.coll, key),
        };
        out.push(Namespace {
            coll,
            ..base_ns.clone()
        });
    }
    Ok(out)
}

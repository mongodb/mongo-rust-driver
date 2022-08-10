pub mod options;
mod state_machine;

use std::{
    path::Path,
    process::{Command, Stdio},
};

use derivative::Derivative;
use mongocrypt::Crypt;
use rayon::ThreadPool;

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

use super::WeakClient;

#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct ClientState {
    #[derivative(Debug = "ignore")]
    pub(crate) crypt: Crypt,
    mongocryptd_client: Option<Client>,
    aux_clients: AuxClients,
    opts: AutoEncryptionOptions,
    crypto_threads: ThreadPool,
}

#[derive(Debug)]
struct AuxClients {
    key_vault_client: WeakClient,
    metadata_client: Option<WeakClient>,
    _internal_client: Option<Client>,
}

impl ClientState {
    pub(super) async fn new(client: &Client, opts: AutoEncryptionOptions) -> Result<Self> {
        let crypt = Self::make_crypt(&opts)?;
        let mongocryptd_client = Self::spawn_mongocryptd_if_needed(&opts, &crypt).await?;
        let aux_clients = Self::make_aux_clients(client, &opts)?;
        let num_cpus = std::thread::available_parallelism()?.get();
        let crypto_threads = rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus)
            .build()
            .map_err(|e| Error::internal(format!("could not initialize thread pool: {}", e)))?;

        Ok(Self {
            crypt,
            mongocryptd_client,
            aux_clients,
            opts,
            crypto_threads,
        })
    }

    pub(super) fn opts(&self) -> &AutoEncryptionOptions {
        &self.opts
    }

    fn make_crypt(opts: &AutoEncryptionOptions) -> Result<Crypt> {
        let mut builder = Crypt::builder();
        if Some(true) != opts.bypass_auto_encryption {
            builder = builder.append_crypt_shared_lib_search_path(Path::new("$SYSTEM"))?;
        }
        if let Some(p) = opts.extra_option(&EO_CRYPT_SHARED_LIB_PATH)? {
            builder = builder.set_crypt_shared_lib_path_override(Path::new(p))?;
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

    /// If crypt_shared is unavailable and options have not disabled it, spawn mongocryptd.  Returns
    /// a `Client` connected to the mongocryptd if one was spawned.
    async fn spawn_mongocryptd_if_needed(
        opts: &AutoEncryptionOptions,
        crypt: &Crypt,
    ) -> Result<Option<Client>> {
        if opts.bypass_auto_encryption == Some(true)
            || opts.extra_option(&EO_MONGOCRYPTD_BYPASS_SPAWN)? == Some(true)
            || crypt.shared_lib_version().is_some()
            || opts.extra_option(&EO_CRYPT_SHARED_REQUIRED)? == Some(true)
        {
            return Ok(None);
        }
        let which_path;
        let bin_path = match opts.extra_option(&EO_MONGOCRYPTD_SPAWN_PATH)? {
            Some(s) => Path::new(s),
            None => {
                which_path = which::which("mongocryptd")
                    .map_err(|e| Error::invalid_argument(format!("{}", e)))?;
                &which_path
            }
        };
        let mut args: Vec<&str> = vec![];
        let has_idle = if let Some(spawn_args) = opts.extra_option(&EO_MONGOCRYPTD_SPAWN_ARGS)? {
            let mut has_idle = false;
            for arg in spawn_args {
                let str_arg = arg.as_str().ok_or_else(|| {
                    Error::invalid_argument("non-string entry in mongocryptdSpawnArgs")
                })?;
                has_idle |= str_arg.starts_with("--idleShutdownTimeoutSecs");
                args.push(str_arg);
            }
            has_idle
        } else {
            false
        };
        if !has_idle {
            args.push("--idleShutdownTimeoutSecs=60");
        }
        Command::new(bin_path)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let uri = opts
            .extra_option(&EO_MONGOCRYPTD_URI)?
            .unwrap_or("mongodb://localhost:27020");
        let mut options = super::options::ClientOptions::parse_uri(uri, None).await?;
        options.server_selection_timeout = Some(std::time::Duration::from_millis(10_000));
        Ok(Some(Client::with_options(options)?))
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
            _internal_client: internal_client,
        })
    }
}

pub(crate) fn aux_collections(
    base_ns: &Namespace,
    enc_fields: &bson::Document,
) -> Result<Vec<Namespace>> {
    let mut out = vec![];
    for &key in &["esc", "ecc", "ecoc"] {
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

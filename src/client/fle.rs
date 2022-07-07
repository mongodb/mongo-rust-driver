use std::{
    path::Path,
    process::{Command, Stdio},
};

use derivative::Derivative;
use mongocrypt::Crypt;

use crate::error::{Error, Result};

use super::{options::{AutoEncryptionOpts, EO_CRYPT_SHARED_LIB_PATH, EO_CRYPT_SHARED_REQUIRED, EO_MONGOCRYPTD_BYPASS_SPAWN, EO_MONGOCRYPTD_SPAWN_PATH, EO_MONGOCRYPTD_URI, EO_MONGOCRYPTD_SPAWN_ARGS}, Client};

#[cfg(feature = "fle")]
#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct ClientState {
    #[derivative(Debug = "ignore")]
    #[allow(dead_code)]
    crypt: Crypt,
    mongocryptd_client: Option<Client>,
    aux_clients: AuxClients,
}

#[cfg(feature = "fle")]
#[derive(Debug)]
struct AuxClients {
    #[allow(dead_code)]
    key_vault_client: Client,
    #[allow(dead_code)]
    metadata_client: Option<Client>,
    #[allow(dead_code)]
    internal_client: Option<Client>,
}

#[cfg(feature = "fle")]
impl ClientState {
    pub(super) async fn new(client: &Client, opts: &AutoEncryptionOpts) -> Result<Option<Self>> {
        let crypt = Self::make_crypt(opts)?;
        let mongocryptd_client = Self::spawn_mongocryptd(opts, &crypt).await?;
        let aux_clients = Self::make_aux_clients(client, opts)?;

        Ok(Some(Self {
            crypt,
            mongocryptd_client,
            aux_clients,
        }))
    }

    fn make_crypt(opts: &AutoEncryptionOpts) -> Result<Crypt> {
        let mut builder = Crypt::builder();
        if Some(true) != opts.bypass_auto_encryption {
            builder = builder.append_crypt_shared_lib_search_path(Path::new("$SYSTEM"))?;
        }
        if let Some(p) = opts.extra_option(&EO_CRYPT_SHARED_LIB_PATH)? {
            builder = builder.set_crypt_shared_lib_path_override(Path::new(p))?;
        }
        let crypt = builder.build()?;
        if opts.extra_option(&EO_CRYPT_SHARED_REQUIRED)? == Some(true) {
            if crypt.shared_lib_version().is_none() {
                return Err(crate::error::Error::invalid_argument(
                    "cryptSharedRequired is set but crypt_shared is not available",
                ));
            }
        }
        Ok(crypt)
    }

    async fn spawn_mongocryptd(opts: &AutoEncryptionOpts, crypt: &Crypt) -> Result<Option<Client>> {
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
        let has_idle = if let Some(spawn_args) =
            opts.extra_option(&EO_MONGOCRYPTD_SPAWN_ARGS)?
        {
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

        let uri = match opts.extra_option(&EO_MONGOCRYPTD_URI)? {
            Some(s) => s,
            None => "mongodb://localhost:27020",
        };
        Ok(Some(Client::with_uri_str(uri).await?))
    }

    fn make_aux_clients(client: &Client, auto_enc_opts: &AutoEncryptionOpts) -> Result<AuxClients> {
        let mut internal_client: Option<Client> = None;
        let mut get_internal_client = || -> Result<Client> {
            if let Some(c) = &internal_client {
                return Ok(c.clone());
            }
            let mut internal_opts = client.inner.options.clone();
            internal_opts.min_pool_size = Some(0);
            let c = Client::with_options(internal_opts)?;
            internal_client = Some(c.clone());
            Ok(c)
        };

        let key_vault_client = if let Some(c) = &auto_enc_opts.key_vault_client {
            c.clone()
        } else if Some(0) == client.inner.options.max_pool_size {
            client.clone()
        } else {
            get_internal_client()?
        };
        let metadata_client = if Some(true) == auto_enc_opts.bypass_auto_encryption {
            None
        } else if Some(0) == client.inner.options.max_pool_size {
            Some(client.clone())
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

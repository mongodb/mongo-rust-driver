use derivative::Derivative;
use mongocrypt::Crypt;

use crate::error::Result;

use super::options::AutoEncryptionOpts;
use super::Client;

#[cfg(feature = "fle")]
#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct ClientState {
    #[derivative(Debug = "ignore")]
    crypt: Crypt,
    mongocryptd_client: Option<Client>,
    aux_clients: AuxClients,
}

#[cfg(feature = "fle")]
#[derive(Debug)]
struct AuxClients {
    key_vault_client: Client,
    metadata_client: Option<Client>,
    internal_client: Option<Client>,
}

#[cfg(feature = "fle")]
impl ClientState {
    pub(super) fn new(client: &Client) -> Result<Option<Self>> {
        let auto_enc_opts = match client.inner.options.auto_encryption_opts.as_ref() {
            Some(o) => o,
            None => return Ok(None),
        };

        let crypt = Self::make_crypt(auto_enc_opts)?;
        let mongocryptd_client = Self::spawn_mongocryptd(&crypt)?;
        let aux_clients = Self::make_aux_clients(client, auto_enc_opts)?;

        Ok(Some(Self {
            crypt,
            mongocryptd_client,
            aux_clients,
        }))
    }

    fn spawn_mongocryptd(crypt: &Crypt) -> Result<Option<Client>> {

        todo!()
    }

    fn make_crypt(auto_enc_opts: &AutoEncryptionOpts) -> Result<Crypt> {
        use std::path::Path;
        let mut builder = Crypt::builder();
        if Some(true) != auto_enc_opts.bypass_auto_encryption {
            builder = builder.append_crypt_shared_lib_search_path(Path::new("$SYSTEM"))?;
        }
        if let Some(p) = auto_enc_opts
            .extra_options
            .as_ref()
            .and_then(|o| o.get_str("cryptSharedLibPath").ok())
        {
            builder = builder.set_crypt_shared_lib_path_override(Path::new(p))?;
        }
        let crypt = builder.build()?;
        if Some(true)
            == auto_enc_opts
                .extra_options
                .as_ref()
                .and_then(|o| o.get_bool("cryptSharedRequired").ok())
        {
            if crypt.shared_lib_version().is_none() {
                return Err(crate::error::Error::invalid_argument(
                    "cryptSharedRequired is set but crypt_shared is not available",
                ));
            }
        }
        Ok(crypt)
    }

    fn make_aux_clients(client: &Client, auto_enc_opts: &AutoEncryptionOpts) -> Result<AuxClients> {
        let mut internal_client: Option<Client> = None;
        let mut get_internal_client = || -> Result<Client> {
            if let Some(c) = internal_client.clone() {
                return Ok(c);
            }
            let mut internal_opts = client.inner.options.clone();
            internal_opts.auto_encryption_opts = None;
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

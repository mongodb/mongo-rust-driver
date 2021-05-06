use trust_dns_resolver::{
    config::ResolverConfig,
    error::ResolveErrorKind,
    lookup::{SrvLookup, TxtLookup},
    IntoName,
};

use crate::error::{Error, Result};

/// An async runtime agnostic DNS resolver.
pub(crate) struct AsyncResolver {
    #[cfg(feature = "tokio-runtime")]
    resolver: trust_dns_resolver::TokioAsyncResolver,

    #[cfg(feature = "async-std-runtime")]
    resolver: async_std_resolver::AsyncStdResolver,
}

impl AsyncResolver {
    pub(crate) async fn new(config: Option<ResolverConfig>) -> Result<Self> {
        #[cfg(feature = "tokio-runtime")]
        let resolver = match config {
            Some(config) => {
                trust_dns_resolver::TokioAsyncResolver::tokio(config, Default::default())
                    .map_err(Error::from_resolve_error)?
            }
            None => trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf()
                .map_err(Error::from_resolve_error)?,
        };

        #[cfg(feature = "async-std-runtime")]
        let resolver = match config {
            Some(config) => async_std_resolver::resolver(config, Default::default())
                .await
                .map_err(Error::from_resolve_error)?,
            None => async_std_resolver::resolver_from_system_conf()
                .await
                .map_err(Error::from_resolve_error)?,
        };

        Ok(Self { resolver })
    }
}

impl AsyncResolver {
    pub async fn srv_lookup<N: IntoName>(&self, query: N) -> Result<SrvLookup> {
        let lookup = self
            .resolver
            .srv_lookup(query)
            .await
            .map_err(Error::from_resolve_error)?;
        Ok(lookup)
    }

    pub async fn txt_lookup<N: IntoName>(&self, query: N) -> Result<Option<TxtLookup>> {
        let lookup_result = self.resolver.txt_lookup(query).await;
        match lookup_result {
            Ok(lookup) => Ok(Some(lookup)),
            Err(e) => match e.kind() {
                ResolveErrorKind::NoRecordsFound { .. } => Ok(None),
                _ => Err(Error::from_resolve_error(e)),
            },
        }
    }
}

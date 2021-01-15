use trust_dns_resolver::{
    config::ResolverConfig,
    lookup::{SrvLookup, TxtLookup},
    IntoName,
};

use crate::error::Result;

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
                trust_dns_resolver::TokioAsyncResolver::tokio(config, Default::default())?
            }
            None => trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf()?,
        };

        #[cfg(feature = "async-std-runtime")]
        let resolver = match config {
            Some(config) => async_std_resolver::resolver(config, Default::default()).await?,
            None => async_std_resolver::resolver_from_system_conf().await?,
        };

        Ok(Self { resolver })
    }
}

impl AsyncResolver {
    pub async fn srv_lookup<N: IntoName>(&self, query: N) -> Result<SrvLookup> {
        let lookup = self.resolver.srv_lookup(query).await?;
        Ok(lookup)
    }

    pub async fn txt_lookup<N: IntoName>(&self, query: N) -> Result<TxtLookup> {
        let lookup = self.resolver.txt_lookup(query).await?;
        Ok(lookup)
    }
}

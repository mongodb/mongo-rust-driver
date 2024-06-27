use hickory_resolver::{
    config::ResolverConfig,
    error::ResolveErrorKind,
    lookup::{SrvLookup, TxtLookup},
    IntoName,
};

use crate::error::{Error, Result};

/// An async runtime agnostic DNS resolver.
pub(crate) struct AsyncResolver {
    resolver: hickory_resolver::TokioAsyncResolver,
}

impl AsyncResolver {
    pub(crate) async fn new(config: Option<ResolverConfig>) -> Result<Self> {
        let resolver = match config {
            Some(config) => hickory_resolver::TokioAsyncResolver::tokio(config, Default::default()),
            None => hickory_resolver::TokioAsyncResolver::tokio_from_system_conf()
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

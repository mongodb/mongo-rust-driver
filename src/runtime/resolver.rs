use crate::error::{Error, Result};
use hickory_resolver::{
    config::ResolverConfig,
    error::ResolveErrorKind,
    lookup::{Lookup, ReverseLookup, SrvLookup, TxtLookup},
    lookup_ip::LookupIp,
    proto::rr::RecordType,
    Name,
};
use std::net::IpAddr;

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
    pub async fn cname_lookup(&self, query: &str) -> Result<Lookup> {
        let name = Name::from_str_relaxed(query).map_err(Error::from_resolve_proto_error)?;
        let lookup = self
            .resolver
            .lookup(name, RecordType::CNAME)
            .await
            .map_err(Error::from_resolve_error)?;
        Ok(lookup)
    }

    pub async fn ip_lookup(&self, query: &str) -> Result<LookupIp> {
        let name = Name::from_str_relaxed(query).map_err(Error::from_resolve_proto_error)?;
        let lookup = self
            .resolver
            .lookup_ip(name)
            .await
            .map_err(Error::from_resolve_error)?;
        Ok(lookup)
    }

    pub async fn reverse_lookup(&self, ip_addr: IpAddr) -> Result<ReverseLookup> {
        let lookup = self
            .resolver
            .reverse_lookup(ip_addr)
            .await
            .map_err(Error::from_resolve_error)?;
        Ok(lookup)
    }

    pub async fn srv_lookup(&self, query: &str) -> Result<SrvLookup> {
        let name = Name::from_str_relaxed(query).map_err(Error::from_resolve_proto_error)?;
        let lookup = self
            .resolver
            .srv_lookup(name)
            .await
            .map_err(Error::from_resolve_error)?;
        Ok(lookup)
    }

    pub async fn txt_lookup(&self, query: &str) -> Result<Option<TxtLookup>> {
        let name = Name::from_str_relaxed(query).map_err(Error::from_resolve_proto_error)?;
        let lookup_result = self.resolver.txt_lookup(name).await;
        match lookup_result {
            Ok(lookup) => Ok(Some(lookup)),
            Err(e) => match e.kind() {
                ResolveErrorKind::NoRecordsFound { .. } => Ok(None),
                _ => Err(Error::from_resolve_error(e)),
            },
        }
    }
}

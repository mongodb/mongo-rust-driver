use std::time::Duration;

#[cfg(feature = "dns-resolver")]
use crate::error::ErrorKind;
use crate::{client::options::ResolverConfig, error::Result, options::ServerAddress};

#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) hosts: Vec<ServerAddress>,
    pub(crate) min_ttl: Duration,
    pub(crate) auth_source: Option<String>,
    pub(crate) replica_set: Option<String>,
    pub(crate) load_balanced: Option<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct LookupHosts {
    pub(crate) hosts: Vec<ServerAddress>,
    pub(crate) min_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OriginalSrvInfo {
    pub(crate) hostname: String,
    pub(crate) min_ttl: Duration,
}

pub(crate) enum DomainMismatch {
    #[allow(dead_code)]
    Error,
    Skip,
}

#[cfg(feature = "dns-resolver")]
pub(crate) struct SrvResolver {
    resolver: crate::runtime::AsyncResolver,
}

#[cfg(feature = "dns-resolver")]
impl SrvResolver {
    pub(crate) async fn new(config: Option<ResolverConfig>) -> Result<Self> {
        let resolver = crate::runtime::AsyncResolver::new(config.map(|c| c.inner)).await?;

        Ok(Self { resolver })
    }

    pub(crate) async fn resolve_client_options(
        &mut self,
        hostname: &str,
    ) -> Result<ResolvedConfig> {
        let lookup_result = self.get_srv_hosts(hostname, DomainMismatch::Error).await?;
        let mut config = ResolvedConfig {
            hosts: lookup_result.hosts,
            min_ttl: lookup_result.min_ttl,
            auth_source: None,
            replica_set: None,
            load_balanced: None,
        };

        self.get_txt_options(hostname, &mut config).await?;

        Ok(config)
    }

    pub(crate) async fn get_srv_hosts(
        &self,
        original_hostname: &str,
        dm: DomainMismatch,
    ) -> Result<LookupHosts> {
        use hickory_proto::rr::RData;

        let hostname_parts: Vec<_> = original_hostname.split('.').collect();

        if hostname_parts.len() < 3 {
            return Err(ErrorKind::InvalidArgument {
                message: "a 'mongodb+srv' hostname must have at least three '.'-delimited parts"
                    .into(),
            }
            .into());
        }

        let lookup_hostname = format!("_mongodb._tcp.{}", original_hostname);

        let srv_lookup = self.resolver.srv_lookup(lookup_hostname.as_str()).await?;
        let mut srv_addresses: Vec<ServerAddress> = Vec::new();
        let mut min_ttl = u32::MAX;

        for record in srv_lookup.as_lookup().record_iter() {
            let srv = match record.data() {
                Some(RData::SRV(s)) => s,
                _ => continue,
            };

            let hostname = srv.target().to_utf8();
            let port = Some(srv.port());
            let mut address = ServerAddress::Tcp {
                host: hostname,
                port,
            };

            let domain_name = &hostname_parts[1..];

            let host = address.host();
            let mut hostname_parts: Vec<_> = host.split('.').collect();

            // Remove empty final section, which indicates a trailing dot.
            if hostname_parts.last().map(|s| s.is_empty()).unwrap_or(false) {
                hostname_parts.pop();
            }

            if !&hostname_parts[1..].ends_with(domain_name) {
                let message = format!(
                    "SRV lookup for {} returned result {}, which does not match domain name {}",
                    original_hostname,
                    address,
                    domain_name.join(".")
                );
                if matches!(dm, DomainMismatch::Error) {
                    return Err(ErrorKind::DnsResolve { message }.into());
                } else {
                    #[cfg(feature = "tracing-unstable")]
                    {
                        use crate::trace::SERVER_SELECTION_TRACING_EVENT_TARGET;
                        if crate::trace::trace_or_log_enabled!(
                            target: SERVER_SELECTION_TRACING_EVENT_TARGET,
                            crate::trace::TracingOrLogLevel::Warn
                        ) {
                            tracing::warn!(
                                target: SERVER_SELECTION_TRACING_EVENT_TARGET,
                                message,
                            );
                        }
                    }
                }
                continue;
            }

            // The spec tests list the seeds without the trailing '.', so we remove it by
            // joining the parts we split rather than manipulating the string.
            address = ServerAddress::Tcp {
                host: hostname_parts.join("."),
                port: address.port(),
            };

            min_ttl = std::cmp::min(min_ttl, record.ttl());
            srv_addresses.push(address);
        }

        if srv_addresses.is_empty() {
            return Err(ErrorKind::DnsResolve {
                message: format!("SRV lookup for {} returned no records", original_hostname),
            }
            .into());
        }

        Ok(LookupHosts {
            hosts: srv_addresses,
            min_ttl: Duration::from_secs(min_ttl.into()),
        })
    }

    async fn get_txt_options(
        &self,
        original_hostname: &str,
        config: &mut ResolvedConfig,
    ) -> Result<()> {
        let txt_records_response = match self.resolver.txt_lookup(original_hostname).await? {
            Some(response) => response,
            None => return Ok(()),
        };
        let mut txt_records = txt_records_response.iter();

        let txt_record = match txt_records.next() {
            Some(record) => record,
            None => return Ok(()),
        };

        if txt_records.next().is_some() {
            return Err(ErrorKind::DnsResolve {
                message: format!(
                    "TXT lookup for {} returned more than one record, but more than one are not \
                     allowed with 'mongodb+srv'",
                    original_hostname,
                ),
            }
            .into());
        }

        let txt_data: Vec<_> = txt_record
            .txt_data()
            .iter()
            .map(|bytes| String::from_utf8_lossy(bytes.as_ref()).into_owned())
            .collect();

        let txt_string = txt_data.join("");

        for option_pair in txt_string.split('&') {
            let parts: Vec<_> = option_pair.split('=').collect();

            if parts.len() != 2 {
                return Err(ErrorKind::DnsResolve {
                    message: format!(
                        "TXT record string '{}' is not a value `key=value` option pair",
                        option_pair
                    ),
                }
                .into());
            }

            match &parts[0].to_lowercase()[..] {
                "authsource" => {
                    config.auth_source = Some(parts[1].to_string());
                }
                "replicaset" => {
                    config.replica_set = Some(parts[1].into());
                }
                "loadbalanced" => {
                    let val = match parts[1] {
                        "true" => true,
                        "false" => false,
                        _ => {
                            return Err(ErrorKind::DnsResolve {
                                message: format!(
                                    "TXT record option 'loadbalanced={}' was returned, only \
                                     'true' and 'false' are allowed values.",
                                    parts[1]
                                ),
                            }
                            .into())
                        }
                    };
                    config.load_balanced = Some(val);
                }
                other => {
                    return Err(ErrorKind::DnsResolve {
                        message: format!(
                            "TXT record option '{}' was returned, but only 'authSource', \
                             'replicaSet', and 'loadBalanced' are allowed",
                            other
                        ),
                    }
                    .into())
                }
            };
        }

        Ok(())
    }
}

/// Stub implementation when dns resolution isn't enabled.
#[cfg(not(feature = "dns-resolver"))]
pub(crate) struct SrvResolver {}

#[cfg(not(feature = "dns-resolver"))]
impl SrvResolver {
    pub(crate) async fn new(_config: Option<ResolverConfig>) -> Result<Self> {
        Ok(Self {})
    }

    pub(crate) async fn resolve_client_options(
        &mut self,
        _hostname: &str,
    ) -> Result<ResolvedConfig> {
        Err(crate::error::Error::invalid_argument(
            "mongodb+srv connection strings cannot be used when the 'dns-resolver' feature is \
             disabled",
        ))
    }

    pub(crate) async fn get_srv_hosts(
        &self,
        _original_hostname: &str,
        _dm: DomainMismatch,
    ) -> Result<LookupHosts> {
        return Err(crate::error::Error::invalid_argument(
            "mongodb+srv connection strings cannot be used when the 'dns-resolver' feature is \
             disabled",
        ));
    }
}

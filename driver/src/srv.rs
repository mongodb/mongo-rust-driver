use std::{sync::Arc, time::Duration};

#[cfg(feature = "dns-resolver")]
use crate::error::{Error, ErrorKind, Redact};
use crate::{
    client::options::ResolverConfig,
    error::Result,
    options::{ClientOptions, ServerAddress},
};
#[cfg(feature = "dns-resolver")]
use hickory_proto::rr::RData;

#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) hosts: Vec<ServerAddress>,
    pub(crate) min_ttl: Duration,
    pub(crate) auth_source: Option<String>,
    pub(crate) replica_set: Option<String>,
    pub(crate) load_balanced: Option<bool>,
}

#[cfg(feature = "dns-resolver")]
#[derive(Debug, Clone)]
pub(crate) struct RawLookupHosts {
    pub(crate) hosts: Vec<(hickory_proto::rr::Name, u16)>,
    pub(crate) min_ttl: Duration,
}

#[cfg(feature = "dns-resolver")]
#[derive(Debug, Clone)]
pub(crate) struct NormalLookupHosts {
    hosts: Vec<(String, u16)>,
    min_ttl: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct LookupHosts {
    pub(crate) hosts: Vec<ServerAddress>,
    pub(crate) min_ttl: Duration,
}

#[cfg(feature = "dns-resolver")]
impl RawLookupHosts {
    pub(crate) fn normalize(self) -> Result<NormalLookupHosts> {
        let mut ok_hosts = vec![];
        for (host, port) in self.hosts {
            let mut host = dbg!(host.to_utf8());
            // spec normalization steps:
            // 1. Any trailing `.` MUST be stripped
            if host.ends_with('.') {
                host.pop();
            }
            // 2. The hostname MUST be converted to its A-label (Punycode) form
            // 3. The hostname MUST be normalized to lowercase using ASCII case folding
            // (both of these are done by `idna::domain_to_ascii_cow`)
            let host = idna::domain_to_ascii_cow(host.as_bytes(), idna::AsciiDenyList::URL)
                .map_err(|e| Error::invalid_response(e.to_string()))?
                .into_owned();

            ok_hosts.push((host, port));
        }
        Ok(NormalLookupHosts {
            hosts: ok_hosts,
            min_ttl: self.min_ttl,
        })
    }
}

#[cfg(feature = "dns-resolver")]
impl NormalLookupHosts {
    pub(crate) fn validate(
        self,
        original_hostname: &str,
        validator: Option<&dyn Fn(&str) -> bool>,
        dm: DomainMismatch,
    ) -> Result<LookupHosts> {
        let original_hostname_parts: Vec<_> = original_hostname.split('.').collect();
        let original_domain_name = if original_hostname_parts.len() >= 3 {
            &original_hostname_parts[1..]
        } else {
            &original_hostname_parts[..]
        };

        let mut ok_hosts = vec![];
        for (host, port) in self.hosts {
            let valid = if let Some(f) = validator {
                f(&host)
            } else {
                let hostname_parts: Vec<_> = host.split('.').collect();
                hostname_parts[1..].ends_with(original_domain_name)
            };
            if valid {
                ok_hosts.push(ServerAddress::Tcp {
                    host,
                    port: Some(port),
                });
            } else {
                let message = format!(
                    "SRV lookup for {} returned result {}, which does not match domain name {}",
                    original_hostname,
                    host,
                    original_domain_name.join(".")
                );
                match dm {
                    DomainMismatch::Error => return Err(ErrorKind::DnsResolve { message }.into()),
                    DomainMismatch::Skip => {
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
                        continue;
                    }
                }
            }
        }

        if ok_hosts.is_empty() {
            return Err(ErrorKind::DnsResolve {
                message: format!(
                    "SRV lookup for {} returned no records",
                    Redact(original_hostname)
                ),
            }
            .into());
        }

        Ok(LookupHosts {
            hosts: ok_hosts,
            min_ttl: self.min_ttl,
        })
    }
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
    options: SrvResolverOptions,
}

#[derive(Default)]
pub(crate) struct SrvResolverOptions {
    pub(crate) srv_service_name: Option<String>,
    #[cfg_attr(not(feature = "dns-resolver"), expect(unused))]
    pub(crate) srv_host_validator: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
}

impl std::fmt::Debug for SrvResolverOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            srv_service_name,
            srv_host_validator: _,
        } = self;
        f.debug_struct("SrvResolverOptions")
            .field("srv_service_name", srv_service_name)
            .finish()
    }
}

impl From<&ClientOptions> for SrvResolverOptions {
    fn from(value: &ClientOptions) -> Self {
        Self {
            srv_service_name: value.srv_service_name.clone(),
            srv_host_validator: value.srv_host_validator.clone(),
        }
    }
}

#[cfg(feature = "dns-resolver")]
impl SrvResolver {
    pub(crate) async fn new(
        config: Option<ResolverConfig>,
        options: SrvResolverOptions,
    ) -> Result<Self> {
        let resolver = crate::runtime::AsyncResolver::new(config.map(|c| c.inner)).await?;

        Ok(Self { resolver, options })
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

    async fn get_srv_hosts_raw(&self, lookup_hostname: &str) -> Result<RawLookupHosts> {
        let srv_lookup = self.resolver.srv_lookup(lookup_hostname).await?;
        let mut hosts = vec![];
        let mut min_ttl = u32::MAX;
        for record in srv_lookup.answers() {
            let RData::SRV(srv) = &record.data else {
                continue;
            };
            hosts.push((srv.target.clone(), srv.port));
            min_ttl = std::cmp::min(min_ttl, record.ttl);
        }
        Ok(RawLookupHosts {
            hosts,
            min_ttl: Duration::from_secs(min_ttl.into()),
        })
    }

    pub(crate) async fn get_srv_hosts(
        &self,
        original_hostname: &str,
        dm: DomainMismatch,
    ) -> Result<LookupHosts> {
        let lookup_hostname = format!(
            "_{}._tcp.{}",
            self.options
                .srv_service_name
                .as_deref()
                .unwrap_or("mongodb"),
            original_hostname
        );
        self.get_srv_hosts_raw(&lookup_hostname)
            .await?
            .normalize()?
            .validate(
                original_hostname,
                self.options
                    .srv_host_validator
                    .as_deref()
                    .map(|f| f as &dyn Fn(&str) -> bool),
                dm,
            )
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
        let mut txt_records =
            txt_records_response
                .answers()
                .iter()
                .filter_map(|record| match &record.data {
                    RData::TXT(txt) => Some(txt),
                    _ => None,
                });

        let txt_record = match txt_records.next() {
            Some(record) => record,
            None => return Ok(()),
        };

        if txt_records.next().is_some() {
            return Err(ErrorKind::DnsResolve {
                message: format!(
                    "TXT lookup for {} returned more than one record, but more than one are not \
                     allowed with 'mongodb+srv'",
                    Redact(original_hostname),
                ),
            }
            .into());
        }

        let txt_data: Vec<_> = txt_record
            .txt_data
            .iter()
            .map(|bytes| String::from_utf8_lossy(bytes.as_ref()).into_owned())
            .collect();

        let txt_string = txt_data.join("");

        for option_pair in txt_string.split('&') {
            let parts: Vec<_> = option_pair.split('=').collect();

            if parts.len() != 2 {
                return Err(ErrorKind::DnsResolve {
                    message: format!(
                        "TXT record string '{option_pair}' is not a value `key=value` option pair"
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
                            "TXT record option '{other}' was returned, but only 'authSource', \
                             'replicaSet', and 'loadBalanced' are allowed"
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
    pub(crate) async fn new(
        _config: Option<ResolverConfig>,
        _options: SrvResolverOptions,
    ) -> Result<Self> {
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

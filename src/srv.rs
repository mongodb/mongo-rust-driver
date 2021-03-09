use trust_dns_resolver::{config::ResolverConfig, error::ResolveErrorKind};

use crate::{
    error::{Error, ErrorKind, Result},
    options::StreamAddress,
    runtime::AsyncResolver,
};

pub(crate) struct SrvResolver {
    resolver: AsyncResolver,
    min_ttl: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) hosts: Vec<StreamAddress>,
    pub(crate) auth_source: Option<String>,
    pub(crate) replica_set: Option<String>,
}

impl SrvResolver {
    pub(crate) async fn new(config: Option<ResolverConfig>) -> Result<Self> {
        let resolver = AsyncResolver::new(config).await?;

        Ok(Self {
            resolver,
            min_ttl: None,
        })
    }

    pub(crate) async fn resolve_client_options(
        &mut self,
        hostname: &str,
    ) -> Result<ResolvedConfig> {
        let hosts = self.get_srv_hosts(hostname).await?.collect::<Result<_>>()?;
        let mut config = ResolvedConfig {
            hosts,
            auth_source: None,
            replica_set: None,
        };

        self.get_txt_options(&hostname, &mut config).await?;

        Ok(config)
    }

    pub(crate) fn min_ttl(&self) -> Option<u32> {
        self.min_ttl
    }

    pub(crate) async fn get_srv_hosts<'a>(
        &'a mut self,
        original_hostname: &'a str,
    ) -> Result<impl Iterator<Item = Result<StreamAddress>> + 'a> {
        let hostname_parts: Vec<_> = original_hostname.split('.').collect();

        if hostname_parts.len() < 3 {
            return Err(ErrorKind::ArgumentError {
                message: "a 'mongodb+srv' hostname must have at least three '.'-delimited parts"
                    .into(),
            }
            .into());
        }

        let lookup_hostname = format!("_mongodb._tcp.{}", original_hostname);

        let srv_lookup = self.resolver.srv_lookup(lookup_hostname.as_str()).await?;

        self.min_ttl = srv_lookup
            .as_lookup()
            .record_iter()
            .map(|record| record.ttl())
            .min();

        let srv_addresses: Vec<_> = srv_lookup
            .iter()
            .map(|record| {
                let hostname = record.target().to_utf8();
                let port = Some(record.port());
                StreamAddress { hostname, port }
            })
            .collect();

        if srv_addresses.is_empty() {
            return Err(ErrorKind::SrvLookupError {
                message: format!("SRV lookup for {} returned no records", original_hostname),
            }
            .into());
        }

        let results = srv_addresses.into_iter().map(move |mut address| {
            let domain_name = &hostname_parts[1..];

            let mut hostname_parts: Vec<_> = address.hostname.split('.').collect();

            // Remove empty final section, which indicates a trailing dot.
            if hostname_parts.last().map(|s| s.is_empty()).unwrap_or(false) {
                hostname_parts.pop();
            }

            if !&hostname_parts[1..].ends_with(domain_name) {
                return Err(ErrorKind::SrvLookupError {
                    message: format!(
                        "SRV lookup for {} returned result {}, which does not match domain name {}",
                        original_hostname,
                        address,
                        domain_name.join(".")
                    ),
                }
                .into());
            }

            // The spec tests list the seeds without the trailing '.', so we remove it by
            // joining the parts we split rather than manipulating the string.
            address.hostname = hostname_parts.join(".");

            Ok(address)
        });

        Ok(results)
    }

    async fn get_txt_options(
        &self,
        original_hostname: &str,
        config: &mut ResolvedConfig,
    ) -> Result<()> {
        let txt_records_response = match self.resolver.txt_lookup(original_hostname).await {
            Ok(response) => response,
            Err(e) => return ignore_no_records(e),
        };
        let mut txt_records = txt_records_response.iter();

        let txt_record = match txt_records.next() {
            Some(record) => record,
            None => return Ok(()),
        };

        if txt_records.next().is_some() {
            return Err(ErrorKind::TxtLookupError {
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
                return Err(ErrorKind::TxtLookupError {
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
                other => {
                    return Err(ErrorKind::TxtLookupError {
                        message: format!(
                            "TXT record option '{}' was returned, but only 'authSource' and \
                             'replicaSet' are allowed",
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

fn ignore_no_records(error: Error) -> Result<()> {
    match error.kind {
        ErrorKind::DnsResolve(resolve_error)
            if matches!(
                resolve_error.kind(),
                ResolveErrorKind::NoRecordsFound { .. }
            ) =>
        {
            Ok(())
        }
        _ => Err(error),
    }
}

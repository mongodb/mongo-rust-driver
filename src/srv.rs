use trust_dns_resolver::{
    error::{ResolveError, ResolveErrorKind},
    Resolver,
};

use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
};

pub(crate) struct SrvResolver {
    resolver: Resolver,
}

#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    pub(crate) hosts: Vec<StreamAddress>,
    pub(crate) auth_source: Option<String>,
    pub(crate) replica_set: Option<String>,
}

impl SrvResolver {
    pub(crate) fn new() -> Result<Self> {
        let resolver = Resolver::new(Default::default(), Default::default())?;

        Ok(Self { resolver })
    }

    pub(crate) fn resolve_client_options(&self, hostname: &str) -> Result<ResolvedConfig> {
        let hosts = self.get_srv_hosts(hostname)?;
        let mut config = ResolvedConfig {
            hosts,
            auth_source: None,
            replica_set: None,
        };

        self.get_txt_options(&hostname, &mut config)?;

        Ok(config)
    }

    fn get_srv_hosts(&self, original_hostname: &str) -> Result<Vec<StreamAddress>> {
        let hostname_parts: Vec<_> = original_hostname.split('.').collect();

        if hostname_parts.len() < 3 {
            return Err(ErrorKind::ArgumentError {
                message: "a 'mongodb+srv' hostname must have at least three '.'-delimited parts"
                    .into(),
            }
            .into());
        }

        let domain_name = &hostname_parts[1..];

        let lookup_hostname = format!("_mongodb._tcp.{}", original_hostname);

        let mut srv_addresses: Vec<_> = self
            .resolver
            .srv_lookup(&lookup_hostname)?
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

        for address in srv_addresses.iter_mut() {
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

            // The spec tests list the seeds without the trailing '.', so we remove it by joining
            // the parts we split rather than manipulating the string.
            address.hostname = hostname_parts.join(".");
        }

        Ok(srv_addresses)
    }

    fn get_txt_options(&self, original_hostname: &str, config: &mut ResolvedConfig) -> Result<()> {
        let txt_records_response = match self.resolver.txt_lookup(original_hostname) {
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

fn ignore_no_records(error: ResolveError) -> Result<()> {
    match error.kind() {
        ResolveErrorKind::NoRecordsFound { .. } => Ok(()),
        _ => Err(error.into()),
    }
}

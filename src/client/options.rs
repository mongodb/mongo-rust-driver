use std::{
    fmt,
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use percent_encoding::percent_decode;
use rustls::{
    internal::pemfile, Certificate, RootCertStore, ServerCertVerified, ServerCertVerifier, TLSError,
};

use crate::{
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    error::{Error, ErrorKind, Result},
    read_preference::{ReadPreference, TagSet},
};

const DEFAULT_PORT: u16 = 27017;

#[derive(Clone, Debug, PartialEq)]
pub struct Host {
    hostname: String,
    port: Option<u16>,
}

impl Host {
    pub fn parse(address: &str) -> Result<Self> {
        let mut parts = address.split(':');

        let hostname = match parts.next() {
            Some(part) => part,
            None => bail!(ErrorKind::InvalidHostname(address.to_string())),
        };

        let port = match parts.next() {
            Some(part) => {
                let port = u16::from_str(part).map_err(|_| {
                    Error::from_kind(ErrorKind::InvalidHostname(address.to_string()))
                })?;

                if parts.next().is_some() {
                    bail!(ErrorKind::InvalidHostname(address.to_string()));
                }

                Some(port)
            }
            None => None,
        };

        Ok(Self {
            hostname: hostname.to_string(),
            port,
        })
    }

    pub fn display(&self) -> String {
        format!("{}", self)
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }
}

impl fmt::Display for Host {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}:{}",
            self.hostname,
            self.port.unwrap_or(DEFAULT_PORT)
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, TypedBuilder)]
pub struct ClientOptions {
    #[builder(default_code = "vec![ Host {
        hostname: \"localhost\".to_string(),
        port: Some(27017),
    }]")]
    pub hosts: Vec<Host>,

    #[builder(default)]
    pub tls_options: Option<TlsOptions>,

    #[builder(default)]
    pub heartbeat_freq: Option<Duration>,

    #[builder(default)]
    pub local_threshold: Option<i64>,

    #[builder(default)]
    pub max_pool_size: Option<u32>,

    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    #[builder(default)]
    pub read_preference: Option<ReadPreference>,

    #[builder(default)]
    pub repl_set_name: Option<String>,

    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    #[builder(default)]
    pub server_selection_timeout: Option<Duration>,
}

#[derive(Debug, Default, PartialEq)]
struct ClientOptionsParser {
    pub hosts: Vec<Host>,
    pub tls_options: Option<TlsOptions>,
    pub heartbeat_freq: Option<Duration>,
    pub local_threshold: Option<i64>,
    pub max_pool_size: Option<u32>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub repl_set_name: Option<String>,
    pub write_concern: Option<WriteConcern>,
    pub server_selection_timeout: Option<Duration>,
    read_preference_tags: Option<Vec<TagSet>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TlsOptions {
    pub allow_invalid_certificates: Option<bool>,
    pub ca_file_path: Option<String>,
    pub cert_key_file_path: Option<String>,
}

struct NoCertVerifier {}

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _: &RootCertStore,
        _: &[Certificate],
        _: webpki::DNSNameRef,
        _: &[u8],
    ) -> std::result::Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}

impl TlsOptions {
    pub fn into_rustls_config(self) -> Result<rustls::ClientConfig> {
        let mut config = rustls::ClientConfig::new();

        if let Some(true) = self.allow_invalid_certificates {
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertVerifier {}));
        }

        if let Some(path) = self.ca_file_path {
            let mut store = RootCertStore::empty();
            store
                .add_pem_file(&mut BufReader::new(File::open(&path)?))
                .map_err(|_| {
                    Error::from_kind(ErrorKind::ParseError(
                        "PEM-encoded root certificate".to_string(),
                        path,
                    ))
                })?;
            config.root_store = store;
        }

        if let Some(path) = self.cert_key_file_path {
            let mut file = BufReader::new(File::open(&path)?);
            let certs = match pemfile::certs(&mut file) {
                Ok(certs) => certs,
                Err(()) => bail!(ErrorKind::ParseError(
                    "PEM-encoded client certificate".to_string(),
                    path,
                )),
            };

            file.seek(SeekFrom::Start(0))?;
            let key = match pemfile::rsa_private_keys(&mut file) {
                Ok(key) => key,
                Err(()) => bail!(ErrorKind::ParseError(
                    "PEM-encoded RSA key".to_string(),
                    path,
                )),
            };

            // TODO: Get rid of unwrap
            config.set_single_client_cert(certs, key.into_iter().next().unwrap());
        }

        Ok(config)
    }
}

impl From<ClientOptionsParser> for ClientOptions {
    fn from(parser: ClientOptionsParser) -> Self {
        Self {
            hosts: parser.hosts,
            tls_options: parser.tls_options,
            heartbeat_freq: parser.heartbeat_freq,
            local_threshold: parser.local_threshold,
            max_pool_size: parser.max_pool_size,
            read_concern: parser.read_concern,
            read_preference: parser.read_preference,
            repl_set_name: parser.repl_set_name,
            write_concern: parser.write_concern,
            server_selection_timeout: parser.server_selection_timeout,
        }
    }
}

impl ClientOptions {
    pub fn parse(s: &str) -> Result<Self> {
        ClientOptionsParser::parse(s).map(Into::into)
    }
}

impl ClientOptionsParser {
    fn parse(s: &str) -> Result<Self> {
        let end_of_scheme = match s.find("://") {
            Some(index) => index,
            None => bail!(ErrorKind::ArgumentError(
                "connection string contains no scheme".to_string()
            )),
        };

        if &s[..end_of_scheme] != "mongodb" {
            bail!(ErrorKind::ArgumentError(format!(
                "invalid connection string scheme: {}",
                &s[..end_of_scheme]
            )));
        }

        let after_scheme = &s[end_of_scheme + 3..];

        let (host_section, options_section) = match after_scheme.find('/') {
            Some(index) => after_scheme.split_at(index),
            None => (after_scheme, ""),
        };

        let hosts: Result<Vec<_>> = host_section
            .split(',')
            .map(|host| {
                let (hostname, port) = match host.find(':') {
                    Some(index) => host.split_at(index),
                    None => (host, ""),
                };

                if hostname.is_empty() {
                    bail!(ErrorKind::ArgumentError(
                        "connection string contains no host".to_string(),
                    ));
                }

                let port = if port.is_empty() {
                    None
                } else {
                    let port_string_without_colon = &port[1..];
                    let p = u16::from_str_radix(port_string_without_colon, 10).map_err(|_| {
                        ErrorKind::ArgumentError(format!(
                            "invalid port specified in connection string: {}",
                            port
                        ))
                    })?;

                    Some(p)
                };

                Ok(Host {
                    hostname: hostname.to_lowercase(),
                    port,
                })
            })
            .collect();

        let hosts = hosts?;

        let mut options = ClientOptionsParser {
            hosts,
            ..Default::default()
        };

        options.parse_options(options_section)?;

        if let Some(ref write_concern) = options.write_concern {
            write_concern.validate()?;
        }

        Ok(options)
    }

    fn parse_options(&mut self, options: &str) -> Result<()> {
        if options.is_empty() {
            return Ok(());
        }

        let options_section = match options.find('?') {
            Some(index) if index < options.len() - 1 => &options[index + 1..],
            _ => return Ok(()),
        };

        for option_pair in options_section.split('&') {
            let (key, value) = match option_pair.find('=') {
                Some(index) => option_pair.split_at(index),
                None => bail!(ErrorKind::ArgumentError(format!(
                    "connection string options is not a `key=value` pair: {}",
                    option_pair,
                ))),
            };

            // Skip leading '=' in value.
            self.parse_option_pair(
                &key.to_lowercase(),
                percent_decode(&value.as_bytes()[1..])
                    .decode_utf8_lossy()
                    .as_ref(),
            )?;
        }

        if let Some(tags) = self.read_preference_tags.take() {
            self.read_preference = match self.read_preference.take() {
                Some(read_pref) => Some(read_pref.with_tags(tags)?),
                None => bail!(ErrorKind::ArgumentError(
                    "cannot set read preference tags without also setting read preference mode"
                        .to_string()
                )),
            };
        }

        Ok(())
    }

    fn parse_option_pair(&mut self, key: &str, value: &str) -> Result<()> {
        macro_rules! get_bool {
            ($value:expr, $option:expr) => {
                match $value {
                    "true" => true,
                    "false" => false,
                    _ => bail!(ErrorKind::ArgumentError(format!(
                        "connection string `{}` option must be a boolean",
                        $option,
                    ))),
                }
            };
        }

        macro_rules! get_ms {
            ($value:expr, $option:expr) => {
                match u64::from_str_radix($value, 10) {
                    Ok(i) => i,
                    _ => bail!(ErrorKind::ArgumentError(format!(
                        "connection string `{}` option must be a non-negative integer",
                        $option
                    ))),
                }
            };
        }

        match key {
            k @ "heartbeatfrequencyms" => {
                self.heartbeat_freq = Some(Duration::from_millis(get_ms!(value, k)));
            }
            k @ "journal" => {
                let mut write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.journal = Some(get_bool!(value, k));
            }
            k @ "localthresholdms" => self.local_threshold = Some(get_ms!(value, k) as i64),
            "maxpoolsize" => {
                self.max_pool_size = match u32::from_str_radix(value, 10) {
                    Ok(u) if u > 0 => Some(u),
                    _ => bail!(ErrorKind::ArgumentError(
                        "connection string `maxPoolSize` option must be a positive integer"
                            .to_string(),
                    )),
                }
            }
            "readconcernlevel" => {
                self.read_concern = Some(ReadConcern::Custom(value.to_string()));
            }
            "readpreference" => {
                self.read_preference = Some(match &value.to_lowercase()[..] {
                    "primary" => ReadPreference::Primary,
                    "secondary" => ReadPreference::Secondary {
                        tag_sets: None,
                        max_staleness: None,
                    },
                    "primarypreferred" => ReadPreference::PrimaryPreferred {
                        tag_sets: None,
                        max_staleness: None,
                    },
                    "secondarypreferred" => ReadPreference::SecondaryPreferred {
                        tag_sets: None,
                        max_staleness: None,
                    },
                    "nearest" => ReadPreference::Nearest {
                        tag_sets: None,
                        max_staleness: None,
                    },
                    other => bail!(ErrorKind::ArgumentError(format!(
                        "'{}' is not a valid read preference",
                        other
                    ))),
                });
            }
            "readpreferencetags" => {
                let tags: Result<TagSet> = if value.is_empty() {
                    Ok(TagSet::new())
                } else {
                    value
                        .split(',')
                        .map(|tag| {
                            let mut values = tag.split(':');

                            match (values.next(), values.next()) {
                                (Some(key), Some(value)) => {
                                    Ok((key.to_string(), value.to_string()))
                                }
                                _ => bail!(ErrorKind::ArgumentError(format!(
                                    "'{}' is not a valid read preference tag (which must be of \
                                     the form 'key:value'",
                                    value,
                                ))),
                            }
                        })
                        .collect()
                };

                self.read_preference_tags
                    .get_or_insert_with(Vec::new)
                    .push(tags?);
            }
            "replicaset" => {
                self.repl_set_name = Some(value.to_string());
            }
            k @ "serverselectiontimeoutms" => {
                self.server_selection_timeout = Some(Duration::from_millis(get_ms!(value, k)))
            }
            k @ "tls" | k @ "ssl" => {
                if get_bool!(value, k) {
                    self.tls_options = Some(Default::default());
                }
            }
            k @ "tlsallowinvalidcertificates" => {
                self.tls_options
                    .get_or_insert_with(Default::default)
                    .allow_invalid_certificates = Some(get_bool!(value, k));
            }
            "tlscafile" => {
                self.tls_options
                    .get_or_insert_with(Default::default)
                    .ca_file_path = Some(value.to_string());
            }
            "tlscertificatekeyfile" => {
                self.tls_options
                    .get_or_insert_with(Default::default)
                    .cert_key_file_path = Some(value.to_string());
            }
            "w" => {
                let mut write_concern = self.write_concern.get_or_insert_with(Default::default);

                match i32::from_str_radix(value, 10) {
                    Ok(w) => {
                        if w < 0 {
                            bail!(ErrorKind::ArgumentError(
                                "connection string `w` option cannot be a negative integer"
                                    .to_string()
                            ));
                        }

                        write_concern.w = Some(Acknowledgment::from(w));
                    }
                    Err(_) => {
                        write_concern.w = Some(Acknowledgment::from(value.to_string()));
                    }
                };
            }
            k @ "wtimeoutms" => {
                let write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.w_timeout = Some(Duration::from_millis(get_ms!(value, k)));
            }
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ClientOptions, Host};
    use crate::{
        concern::{Acknowledgment, ReadConcern, WriteConcern},
        read_preference::ReadPreference,
    };

    macro_rules! tag_set {
        ( $($k:expr => $v:expr),* ) => {
            #[allow(clippy::let_and_return)]
            {
                use std::collections::HashMap;

                #[allow(unused_mut)]
                let mut ts = HashMap::new();
                $(
                    ts.insert($k.to_string(), $v.to_string());
                )*

                ts
            }
        }
    }

    fn host_without_port(hostname: &str) -> Host {
        Host {
            hostname: hostname.to_string(),
            port: None,
        }
    }

    #[test]
    fn fails_without_scheme() {
        assert!(ClientOptions::parse("localhost:27017").is_err());
    }

    #[test]
    fn fails_with_invalid_scheme() {
        assert!(ClientOptions::parse("mangodb://localhost:27017").is_err());
    }

    #[test]
    fn fails_with_nothing_after_scheme() {
        assert!(ClientOptions::parse("mongodb://").is_err());
    }

    #[test]
    fn fails_with_only_slash_after_scheme() {
        assert!(ClientOptions::parse("mongodb:///").is_err());
    }

    #[test]
    fn fails_with_no_host() {
        assert!(ClientOptions::parse("mongodb://:27017").is_err());
    }

    #[test]
    fn no_port() {
        assert_eq!(
            ClientOptions::parse("mongodb://localhost").unwrap(),
            ClientOptions {
                hosts: vec![host_without_port("localhost")],
                ..Default::default()
            }
        );
    }

    #[test]
    fn no_port_trailing_slash() {
        assert_eq!(
            ClientOptions::parse("mongodb://localhost/").unwrap(),
            ClientOptions {
                hosts: vec![host_without_port("localhost")],
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_port() {
        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_port_and_trailing_slash() {
        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_read_concern() {
        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/?readConcernLevel=foo").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                read_concern: Some(ReadConcern::Custom("foo".to_string())),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_w_negative_int() {
        assert!(ClientOptions::parse("mongodb://localhost:27017/?w=-1").is_err());
    }

    #[test]
    fn with_w_non_negative_int() {
        let write_concern = WriteConcern::builder().w(Acknowledgment::from(1)).build();

        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/?w=1").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_w_string() {
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::from("foo".to_string()))
            .build();

        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/?w=foo").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_invalid_j() {
        assert!(ClientOptions::parse("mongodb://localhost:27017/?journal=foo").is_err());
    }

    #[test]
    fn with_j() {
        let write_concern = WriteConcern::builder().journal(true).build();

        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/?journal=true").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_wtimeout_non_int() {
        assert!(ClientOptions::parse("mongodb://localhost:27017/?wtimeoutMS=foo").is_err());
    }

    #[test]
    fn with_wtimeout_negative_int() {
        assert!(ClientOptions::parse("mongodb://localhost:27017/?wtimeoutMS=-1").is_err());
    }

    #[test]
    fn with_wtimeout() {
        let write_concern = WriteConcern::builder()
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse("mongodb://localhost:27017/?wtimeoutMS=27").unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_all_write_concern_options() {
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::Majority)
            .journal(false)
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse(
                "mongodb://localhost:27017/?w=majority&journal=false&wtimeoutMS=27"
            )
            .unwrap(),
            ClientOptions {
                hosts: vec![Host {
                    hostname: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                ..Default::default()
            }
        );
    }

    #[test]
    fn with_invalid_read_preference_mode() {}

    #[test]
    fn with_mixed_options() {
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::Majority)
            .journal(false)
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse(
                "mongodb://localhost,localhost:27018/?w=majority&readConcernLevel=majority&\
                 journal=false&wtimeoutMS=27&replicaSet=foo&heartbeatFrequencyMS=1000&\
                 localThresholdMS=4000&readPreference=secondaryPreferred&readpreferencetags=dc:ny,\
                 rack:1&serverselectiontimeoutms=2000&readpreferencetags=dc:ny&readpreferencetags="
            )
            .unwrap(),
            ClientOptions {
                hosts: vec![
                    Host {
                        hostname: "localhost".to_string(),
                        port: None,
                    },
                    Host {
                        hostname: "localhost".to_string(),
                        port: Some(27018),
                    },
                ],
                read_preference: Some(ReadPreference::SecondaryPreferred {
                    tag_sets: Some(vec![
                        tag_set! {
                            "dc" => "ny",
                            "rack" => "1"
                        },
                        tag_set! {
                            "dc" => "ny"
                        },
                        tag_set! {},
                    ]),
                    max_staleness: None,
                }),
                read_concern: Some(ReadConcern::Majority),
                write_concern: Some(write_concern),
                repl_set_name: Some("foo".to_string()),
                heartbeat_freq: Some(Duration::from_millis(1000)),
                local_threshold: Some(4000),
                server_selection_timeout: Some(Duration::from_millis(2000)),
                ..Default::default()
            }
        );
    }
}

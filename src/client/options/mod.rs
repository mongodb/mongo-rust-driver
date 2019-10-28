#[cfg(test)]
mod test;

use std::{
    collections::HashSet,
    fmt,
    fs::File,
    hash::{Hash, Hasher},
    io::{BufReader, Seek, SeekFrom},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use bson::{Bson, Document};
use lazy_static::lazy_static;
use rustls::{
    internal::pemfile, Certificate, RootCertStore, ServerCertVerified, ServerCertVerifier, TLSError,
};

use crate::{
    client::auth::{AuthMechanism, Credential},
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    error::{Error, ErrorKind, Result},
    read_preference::{ReadPreference, TagSet},
};

const DEFAULT_PORT: u16 = 27017;

lazy_static! {
    /// Reserved characters as defined by [Section 2.2 of RFC-3986](https://tools.ietf.org/html/rfc3986#section-2.2).
    /// Usernames / passwords that contain these characters must instead include the URL encoded version of them when included
    /// as part of the connection string.
    static ref USERINFO_RESERVED_CHARACTERS: HashSet<&'static char> = {
        [':', '/', '?', '#', '[', ']', '@', '!'].iter().collect()
    };

    static ref ILLEGAL_DATABASE_CHARACTERS: HashSet<&'static char> = {
        ['/', '\\', ' ', '"', '$', '.'].iter().collect()
    };
}

#[derive(Clone, Debug, Eq)]
pub struct StreamAddress {
    pub hostname: String,
    pub port: Option<u16>,
}

impl PartialEq for StreamAddress {
    fn eq(&self, other: &Self) -> bool {
        self.hostname == other.hostname && self.port.unwrap_or(27017) == other.port.unwrap_or(27017)
    }
}

impl Hash for StreamAddress {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.hostname.hash(state);
        self.port.unwrap_or(27017).hash(state);
    }
}

impl StreamAddress {
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

        Ok(StreamAddress {
            hostname: hostname.to_string(),
            port,
        })
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }
}

impl fmt::Display for StreamAddress {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}:{}",
            self.hostname,
            self.port.unwrap_or(DEFAULT_PORT)
        )
    }
}

#[derive(Debug, PartialEq, TypedBuilder)]
pub struct ClientOptions {
    #[builder(default_code = "vec![ StreamAddress {
        hostname: \"localhost\".to_string(),
        port: Some(27017),
    }]")]
    pub hosts: Vec<StreamAddress>,

    #[builder(default)]
    pub app_name: Option<String>,

    #[builder(default)]
    pub tls_options: Option<TlsOptions>,

    #[builder(default)]
    pub heartbeat_freq: Option<Duration>,

    #[builder(default)]
    pub local_threshold: Option<i64>,

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

    #[builder(default)]
    pub max_pool_size: Option<u32>,

    #[builder(default)]
    pub min_pool_size: Option<u32>,

    #[builder(default)]
    pub max_idle_time: Option<Duration>,

    #[builder(default)]
    pub wait_queue_timeout: Option<Duration>,

    #[builder(default)]
    pub(crate) compressors: Option<Vec<String>>,

    #[builder(default)]
    pub(crate) connect_timeout: Option<Duration>,

    #[builder(default)]
    pub(crate) retry_reads: Option<bool>,

    #[builder(default)]
    pub(crate) retry_writes: Option<bool>,

    #[builder(default)]
    pub(crate) socket_timeout: Option<Duration>,

    #[builder(default)]
    pub(crate) zlib_compression: Option<i32>,

    #[builder(default)]
    pub direct_connection: Option<bool>,

    #[builder(default)]
    pub(crate) max_staleness: Option<Duration>,

    /// The credential to use for authenticating connections made by this client.
    #[builder(default)]
    pub credential: Option<Credential>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Debug, Default, PartialEq)]
struct ClientOptionsParser {
    pub hosts: Vec<StreamAddress>,
    pub app_name: Option<String>,
    pub tls_options: Option<TlsOptions>,
    pub heartbeat_freq: Option<Duration>,
    pub local_threshold: Option<i64>,
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub repl_set_name: Option<String>,
    pub write_concern: Option<WriteConcern>,
    pub server_selection_timeout: Option<Duration>,
    pub max_pool_size: Option<u32>,
    pub min_pool_size: Option<u32>,
    pub max_idle_time: Option<Duration>,
    pub wait_queue_timeout: Option<Duration>,
    pub compressors: Option<Vec<String>>,
    pub connect_timeout: Option<Duration>,
    pub retry_reads: Option<bool>,
    pub retry_writes: Option<bool>,
    pub socket_timeout: Option<Duration>,
    pub zlib_compression: Option<i32>,
    pub max_staleness: Option<Duration>,
    pub direct_connection: Option<bool>,
    pub credential: Option<Credential>,
    tls_insecure: Option<bool>,
    auth_mechanism: Option<AuthMechanism>,
    auth_source: Option<String>,
    auth_mechanism_properties: Option<Document>,
    read_preference_tags: Option<Vec<TagSet>>,
    tls_values: Vec<bool>,
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
            app_name: parser.app_name,
            tls_options: parser.tls_options,
            heartbeat_freq: parser.heartbeat_freq,
            local_threshold: parser.local_threshold,
            read_concern: parser.read_concern,
            read_preference: parser.read_preference,
            repl_set_name: parser.repl_set_name,
            write_concern: parser.write_concern,
            max_pool_size: parser.max_pool_size,
            min_pool_size: parser.min_pool_size,
            max_idle_time: parser.max_idle_time,
            wait_queue_timeout: parser.wait_queue_timeout,
            server_selection_timeout: parser.server_selection_timeout,
            compressors: parser.compressors,
            connect_timeout: parser.connect_timeout,
            retry_reads: parser.retry_reads,
            retry_writes: parser.retry_writes,
            socket_timeout: parser.socket_timeout,
            zlib_compression: parser.zlib_compression,
            direct_connection: parser.direct_connection,
            credential: parser.credential,
            max_staleness: parser.max_staleness,
        }
    }
}

impl ClientOptions {
    pub fn parse(s: &str) -> Result<Self> {
        ClientOptionsParser::parse(s).map(Into::into)
    }
}

/// Splits a string into a section before a given index and a section exclusively after the index.
/// Empty portions are returned as `None`.
fn exclusive_split_at(s: &str, i: usize) -> (Option<&str>, Option<&str>) {
    let (l, r) = s.split_at(i);

    let lout = if !l.is_empty() { Some(l) } else { None };
    let rout = if r.len() > 1 { Some(&r[1..]) } else { Some("") };

    (lout, rout)
}

fn percent_decode(s: &str, err_message: &str) -> Result<String> {
    match percent_encoding::percent_decode_str(s).decode_utf8() {
        Ok(result) => Ok(result.to_string()),
        Err(_) => Err(ErrorKind::ArgumentError(err_message.to_string()).into()),
    }
}

fn validate_userinfo(s: &str, userinfo_type: &str) -> Result<()> {
    if s.chars().any(|c| USERINFO_RESERVED_CHARACTERS.contains(&c)) {
        bail!(ErrorKind::ArgumentError(
            format!("{} must be URL encoded", userinfo_type).to_string()
        ))
    }
    Ok(())
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

        let (pre_slash, post_slash) = match after_scheme.find('/') {
            Some(slash_index) => match exclusive_split_at(after_scheme, slash_index) {
                (Some(section), o) => (section, o),
                (None, _) => bail!(ErrorKind::ArgumentError("missing hosts".to_string())),
            },
            None => {
                if after_scheme.find('?').is_some() {
                    bail!(ErrorKind::ArgumentError(
                        "Missing delimiting slash between hosts and options".to_string()
                    ));
                }
                (after_scheme, None)
            }
        };

        let (database, options_section) = match post_slash {
            Some(section) => match section.find('?') {
                Some(index) => exclusive_split_at(section, index),
                None => (post_slash, None),
            },
            None => (None, None),
        };

        let db = match database {
            Some(db) => {
                let decoded = percent_decode(db, "database name must be URL encoded")?;
                if decoded
                    .chars()
                    .any(|c| ILLEGAL_DATABASE_CHARACTERS.contains(&c))
                {
                    bail!(ErrorKind::ArgumentError(
                        "illegal character in database name".to_string()
                    ))
                }
                Some(decoded)
            }
            None => None,
        };

        let (authentication_requested, cred_section, hosts_section) = match pre_slash.rfind('@') {
            Some(index) => {
                // if '@' is in the host section, it MUST be interpreted as a request for
                // authentication, even if the credentials are empty.
                let (creds, hosts) = exclusive_split_at(pre_slash, index);
                match hosts {
                    Some(hs) => (true, creds, hs),
                    None => bail!(ErrorKind::ArgumentError("missing hosts".to_string())),
                }
            }
            None => (false, None, pre_slash),
        };

        let (username, password) = match cred_section {
            Some(creds) => match creds.find(':') {
                Some(index) => exclusive_split_at(creds, index),
                None => (Some(creds), None), // Lack of ":" implies whole string is username
            },
            None => (None, None),
        };

        let hosts: Result<Vec<_>> = hosts_section
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

                    if p == 0 {
                        bail!(ErrorKind::ArgumentError(format!(
                            "invalid port specified in connection string: {}",
                            port
                        )));
                    }

                    Some(p)
                };

                Ok(StreamAddress {
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

        if let Some(opts) = options_section {
            options.parse_options(opts)?;
        }

        if let Some(ref write_concern) = options.write_concern {
            write_concern.validate()?;
        }

        // Set username and password.
        if let Some(u) = username {
            let mut credential = options.credential.get_or_insert_with(Default::default);
            validate_userinfo(u, "username")?;
            let decoded_u = percent_decode(u, "username must be URL encoded")?;
            if decoded_u.chars().any(|c| c == '%') {
                bail!(ErrorKind::ArgumentError(
                    "username/passowrd cannot contain unescaped %".to_string()
                ))
            }

            credential.username = Some(decoded_u);

            if let Some(pass) = password {
                validate_userinfo(pass, "password")?;
                let decoded_p = percent_decode(pass, "password must be URL encoded")?;
                credential.password = Some(decoded_p)
            }
        }

        let db_str = db.as_ref().map(String::as_str);

        match options.auth_mechanism {
            Some(ref mechanism) => {
                let mut credential = options.credential.get_or_insert_with(Default::default);
                // If a source is provided, use that. Otherwise, choose a default based on the
                // mechanism.
                credential.source = options
                    .auth_source
                    .take()
                    .or_else(|| Some(mechanism.default_source(db_str)));

                if let Some(mut doc) = options.auth_mechanism_properties.take() {
                    match doc.remove("CANONICALIZE_HOST_NAME") {
                        Some(Bson::String(s)) => {
                            let val = match &s.to_lowercase()[..] {
                                "true" => Bson::Boolean(true),
                                "false" => Bson::Boolean(false),
                                _ => Bson::String(s),
                            };
                            doc.insert("CANONICALIZE_HOST_NAME", val);
                        }
                        Some(val) => {
                            doc.insert("CANONICALIZE_HOST_NAME", val);
                        }
                        None => {}
                    }

                    credential.mechanism_properties = Some(doc);
                }

                mechanism.validate_credential(&credential)?;
                credential.mechanism = options.auth_mechanism.take();
            }
            None => {
                if let Some(ref mut credential) = options.credential {
                    // If credentials exist (i.e. username is specified) but no mechanism, the
                    // default source is chosen from the following list in
                    // order (skipping null ones): authSource option, connection string db,
                    // SCRAM default (i.e. "admin").
                    credential.source = options
                        .auth_source
                        .take()
                        .or_else(|| db_str.and_then(|s| Some(s.to_string())));
                } else if authentication_requested {
                    bail!(ErrorKind::ArgumentError(
                        "username and mechanism both not provided, but authentication was \
                         requested"
                            .to_string()
                    ))
                } else if options.auth_source.is_some() {
                    bail!(ErrorKind::ArgumentError(
                        "username and mechanism both not provided, but authSource was specified"
                            .to_string()
                    ))
                }
            }
        };

        Ok(options)
    }

    fn parse_options(&mut self, options: &str) -> Result<()> {
        if options.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<&str> = Vec::new();

        for option_pair in options.split('&') {
            let (key, value) = match option_pair.find('=') {
                Some(index) => option_pair.split_at(index),
                None => bail!(ErrorKind::ArgumentError(format!(
                    "connection string options is not a `key=value` pair: {}",
                    option_pair,
                ))),
            };

            if key.to_lowercase() != "readpreferencetags" && keys.contains(&key) {
                bail!(ErrorKind::ArgumentError("repeated options".to_string()));
            } else {
                keys.push(key);
            }

            // Skip leading '=' in value.
            self.parse_option_pair(
                &key.to_lowercase(),
                percent_encoding::percent_decode(&value.as_bytes()[1..])
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

        if !self.tls_values.is_empty() {
            let tls_value = self.tls_values[0];

            if self.tls_values.drain(..).any(|val| val != tls_value) {
                bail!(ErrorKind::ArgumentError(
                    "All instances of `tls` and `ssl` must have the same value".to_string()
                ))
            }

            if tls_value {
                if self.tls_options == None {
                    self.tls_options = Some(Default::default());
                }
            } else {
                self.tls_options = None;
            }
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

        macro_rules! get_duration {
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

        macro_rules! get_u32 {
            ($value:expr, $option:expr) => {
                match u32::from_str_radix(value, 10) {
                    Ok(u) => u,
                    Err(_) => bail!(ErrorKind::ArgumentError(format!(
                        "connection string `{}` argument must be a positive integer",
                        $option,
                    ))),
                }
            };
        }

        macro_rules! get_i32 {
            ($value:expr, $option:expr) => {
                match i32::from_str_radix(value, 10) {
                    Ok(u) => u,
                    Err(_) => bail!(ErrorKind::ArgumentError(format!(
                        "connection string `{}` argument must be an integer",
                        $option,
                    ))),
                }
            };
        }

        match key {
            "appname" => {
                self.app_name = Some(value.into());
            }
            k @ "direct" => {
                self.direct_connection = Some(get_bool!(value, k));
            }
            k @ "heartbeatfrequencyms" => {
                self.heartbeat_freq = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "journal" => {
                let mut write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.journal = Some(get_bool!(value, k));
            }
            k @ "localthresholdms" => self.local_threshold = Some(get_duration!(value, k) as i64),
            "readconcernlevel" => {
                self.read_concern = Some(ReadConcern::Custom(value.to_string()));
            }
            k @ "maxidletimems" => {
                self.max_idle_time = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "maxpoolsize" => {
                self.max_pool_size = Some(get_u32!(value, k));
            }
            k @ "minpoolsize" => {
                self.max_pool_size = Some(get_u32!(value, k));
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
                self.server_selection_timeout = Some(Duration::from_millis(get_duration!(value, k)))
            }
            k @ "tls" | k @ "ssl" => {
                self.tls_values.push(get_bool!(value, k));
            }
            k @ "tlsinsecure" | k @ "tlsallowinvalidcertificates" => {
                let val = get_bool!(value, k);

                let allow_invalid_certificates = if k == "tlsinsecure" { !val } else { val };

                if let Some(existing_val) = self
                    .tls_options
                    .as_ref()
                    .and_then(|opts| opts.allow_invalid_certificates)
                {
                    if allow_invalid_certificates != existing_val {
                        bail!(ErrorKind::ArgumentError(
                            "all instances of 'tlsInsecure' and 'tlsAllowInvalidCertificates' \
                             must be consistent (e.g. 'tlsInsecure' cannot be true when \
                             'tlsAllowInvalidCertificates' is false, or vice-versa)"
                                .into()
                        ));
                    }
                }

                self.tls_options
                    .get_or_insert_with(Default::default)
                    .allow_invalid_certificates = Some(allow_invalid_certificates);
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
            k @ "waitqueuetimeoutms" => {
                self.wait_queue_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "wtimeoutms" => {
                let write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.w_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            "authmechanism" => {
                self.auth_mechanism = Some(AuthMechanism::from_str(value)?);
            }
            "authsource" => self.auth_source = Some(value.to_string()),
            "authmechanismproperties" => {
                let mut doc = Document::new();
                let err_func = || {
                    ErrorKind::ArgumentError(
                        "improperly formatted authMechanismProperties".to_string(),
                    )
                    .into()
                };

                for kvp in value.split(',') {
                    match kvp.find(':') {
                        Some(index) => {
                            let (k, v) = exclusive_split_at(kvp, index);
                            let key = k.ok_or_else(err_func)?;
                            let value = v.ok_or_else(err_func)?;
                            doc.insert(key, value);
                        }
                        None => return Err(err_func()),
                    };
                }
                self.auth_mechanism_properties = Some(doc);
            }
            "compressors" => {
                self.compressors = Some(value.split(',').map(String::from).collect());
            }
            k @ "connecttimeoutms" => {
                self.connect_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "retrywrites" => {
                self.retry_writes = Some(get_bool!(value, k));
            }
            k @ "retryreads" => {
                self.retry_reads = Some(get_bool!(value, k));
            }
            k @ "sockettimeoutms" => {
                self.socket_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "zlibcompressionlevel" => {
                let i = get_i32!(value, k);
                if i < -1 {
                    bail!(ErrorKind::ArgumentError(
                        "'zlibCompressionLevel' cannot be less than -1".to_string()
                    ));
                }

                if i > 9 {
                    bail!(ErrorKind::ArgumentError(
                        "'zlibCompressionLevel' cannot be greater than 9".to_string()
                    ));
                }

                self.zlib_compression = Some(i);
            }
            k @ "maxstalenessseconds" => {
                self.max_staleness = Some(Duration::from_millis(get_duration!(value, k)));
            }

            _ => {
                bail!(ErrorKind::ArgumentError(
                    "invalid option warning".to_string()
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ClientOptions, StreamAddress};
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

    fn host_without_port(hostname: &str) -> StreamAddress {
        StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                hosts: vec![StreamAddress {
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
                    StreamAddress {
                        hostname: "localhost".to_string(),
                        port: None,
                    },
                    StreamAddress {
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

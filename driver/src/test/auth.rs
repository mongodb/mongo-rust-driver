#[cfg(feature = "aws-auth")]
mod aws;
#[cfg(feature = "gssapi-auth")]
#[path = "auth/gssapi.rs"]
mod gssapi_skip_local;
#[path = "auth/plain.rs"]
mod plain_skip_local;

use lazy_static::lazy_static;

use crate::{cmap::StreamDescription, options::AuthMechanism};

lazy_static! {
    static ref MECHS: [String; 2] = [
        AuthMechanism::ScramSha1.as_str().to_string(),
        AuthMechanism::ScramSha256.as_str().to_string()
    ];
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn negotiate_both_scram() {
    let description_both = StreamDescription {
        sasl_supported_mechs: Some(MECHS.to_vec()),
        ..Default::default()
    };
    assert_eq!(
        AuthMechanism::from_stream_description(&description_both),
        AuthMechanism::ScramSha256
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn negotiate_sha1_only() {
    let description_sha1 = StreamDescription {
        sasl_supported_mechs: Some(MECHS[0..=0].to_vec()),
        ..Default::default()
    };
    assert_eq!(
        AuthMechanism::from_stream_description(&description_sha1),
        AuthMechanism::ScramSha1
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn negotiate_sha256_only() {
    let description_sha256 = StreamDescription {
        sasl_supported_mechs: Some(MECHS[1..=1].to_vec()),
        ..Default::default()
    };
    assert_eq!(
        AuthMechanism::from_stream_description(&description_sha256),
        AuthMechanism::ScramSha256
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn negotiate_none() {
    let description_none: StreamDescription = Default::default();
    assert_eq!(
        AuthMechanism::from_stream_description(&description_none),
        AuthMechanism::ScramSha1
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn negotiate_mangled() {
    let description_mangled = StreamDescription {
        sasl_supported_mechs: Some(["NOT A MECHANISM".to_string(), "OTHER".to_string()].to_vec()),
        ..Default::default()
    };
    assert_eq!(
        AuthMechanism::from_stream_description(&description_mangled),
        AuthMechanism::ScramSha1
    );
}

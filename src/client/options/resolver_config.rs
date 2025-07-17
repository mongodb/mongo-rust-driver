#[cfg(any(feature = "dns-resolver", feature = "gssapi-auth"))]
use hickory_resolver::config::ResolverConfig as HickoryResolverConfig;

/// Configuration for the upstream nameservers to use for resolution.
///
/// This is a thin wrapper around a `hickory_resolver::config::ResolverConfig` provided to ensure
/// API stability.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolverConfig {
    #[cfg(any(feature = "dns-resolver", feature = "gssapi-auth"))]
    pub(crate) inner: HickoryResolverConfig,
}

#[cfg(any(feature = "dns-resolver", feature = "gssapi-auth"))]
impl ResolverConfig {
    /// Creates a default configuration, using 1.1.1.1, 1.0.0.1 and 2606:4700:4700::1111,
    /// 2606:4700:4700::1001 (thank you, Cloudflare).
    ///
    /// Please see: <https://www.cloudflare.com/dns/>
    pub fn cloudflare() -> Self {
        ResolverConfig {
            inner: HickoryResolverConfig::cloudflare(),
        }
    }

    /// Creates a default configuration, using 8.8.8.8, 8.8.4.4 and 2001:4860:4860::8888,
    /// 2001:4860:4860::8844 (thank you, Google).
    ///
    /// Please see Google’s privacy statement for important information about what they track, many
    /// ISP’s track similar information in DNS.
    pub fn google() -> Self {
        ResolverConfig {
            inner: HickoryResolverConfig::google(),
        }
    }

    /// Creates a configuration, using 9.9.9.9, 149.112.112.112 and 2620:fe::fe, 2620:fe::fe:9, the
    /// “secure” variants of the quad9 settings (thank you, Quad9).
    ///
    /// Please see: <https://www.quad9.net/faq/>
    pub fn quad9() -> Self {
        ResolverConfig {
            inner: HickoryResolverConfig::quad9(),
        }
    }
}

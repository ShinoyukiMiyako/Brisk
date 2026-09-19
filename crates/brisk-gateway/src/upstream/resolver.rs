//! Address policy for upstream connections (04, 8.1; R25).
//!
//! Every upstream address is classified as public, private, loopback or
//! always denied (link-local, cloud metadata, multicast, reserved); private
//! and loopback addresses are reachable only for channels that opt in. The
//! policy is applied twice: by a DNS resolver that filters what the system
//! resolver returns, and at configuration time to IP-literal base URLs,
//! which never reach a resolver.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

use crate::BoxError;

/// Where an address sits in the upstream address policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddrClass {
    /// Reachable by every channel.
    Public,
    /// Private-use ranges; reachable only with `allow_private`.
    Private,
    /// Loopback; reachable only with `allow_private`.
    Loopback,
    /// Never reachable, whatever the channel configuration says.
    Denied(DeniedKind),
}

/// Why an address is always denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeniedKind {
    /// `0.0.0.0/8` and `::`.
    Unspecified,
    /// `169.254.0.0/16` and `fe80::/10`, except the metadata addresses.
    LinkLocal,
    /// Instance metadata and platform endpoints of cloud providers (D7).
    CloudMetadata,
    /// `224.0.0.0/4` and `ff00::/8`.
    Multicast,
    /// `255.255.255.255`.
    Broadcast,
    /// `240.0.0.0/4`, IPv4-compatible IPv6 and site-local `fec0::/10`.
    Reserved,
}

/// Cloud metadata endpoints that sit inside otherwise private ranges, so
/// `allow_private` would reach them without this list (D7).
const METADATA_V4: [Ipv4Addr; 4] = [
    // AWS, GCP, Azure IMDS and most others.
    Ipv4Addr::new(169, 254, 169, 254),
    // Alibaba Cloud, inside 100.64.0.0/10.
    Ipv4Addr::new(100, 100, 100, 200),
    // Azure WireServer.
    Ipv4Addr::new(168, 63, 129, 16),
    // Oracle Cloud Classic, inside 192.0.0.0/24.
    Ipv4Addr::new(192, 0, 0, 192),
];

/// AWS IMDS over IPv6, inside the ULA range `fc00::/7`.
const METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254);

/// Classifies `ip` by the table in section 1.4.9 of the M1 contract.
///
/// IPv6 addresses that embed an IPv4 address (mapped, NAT64, 6to4, Teredo)
/// are classified by the embedded address, because that is where a
/// translating network delivers the packets.
pub fn classify(ip: IpAddr) -> AddrClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

/// Public always; Private and Loopback only with `allow_private`; Denied never.
pub fn permits(ip: IpAddr, allow_private: bool) -> bool {
    match classify(ip) {
        AddrClass::Public => true,
        AddrClass::Private | AddrClass::Loopback => allow_private,
        AddrClass::Denied(_) => false,
    }
}

fn classify_v4(ip: Ipv4Addr) -> AddrClass {
    if METADATA_V4.contains(&ip) {
        return AddrClass::Denied(DeniedKind::CloudMetadata);
    }
    let [a, b, c, _] = ip.octets();
    match (a, b, c) {
        (0, _, _) => AddrClass::Denied(DeniedKind::Unspecified),
        _ if ip.is_broadcast() => AddrClass::Denied(DeniedKind::Broadcast),
        (169, 254, _) => AddrClass::Denied(DeniedKind::LinkLocal),
        (224..=239, _, _) => AddrClass::Denied(DeniedKind::Multicast),
        (240..=255, _, _) => AddrClass::Denied(DeniedKind::Reserved),
        (127, _, _) => AddrClass::Loopback,
        (10, _, _)
        | (172, 16..=31, _)
        | (192, 168, _)
        | (100, 64..=127, _)
        | (192, 0, 0)
        | (198, 18..=19, _) => AddrClass::Private,
        _ => AddrClass::Public,
    }
}

fn classify_v6(ip: Ipv6Addr) -> AddrClass {
    if ip == METADATA_V6 {
        return AddrClass::Denied(DeniedKind::CloudMetadata);
    }
    if ip.is_unspecified() {
        return AddrClass::Denied(DeniedKind::Unspecified);
    }
    if ip.is_loopback() {
        return AddrClass::Loopback;
    }
    let seg = ip.segments();
    let octets = ip.octets();
    let last_v4 = Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]);
    match seg {
        // IPv4-mapped `::ffff:0:0/96` and the well-known NAT64 prefix
        // `64:ff9b::/96`.
        [0, 0, 0, 0, 0, 0xffff, _, _] | [0x64, 0xff9b, 0, 0, 0, 0, _, _] => classify_v4(last_v4),
        // IPv4-compatible `::/96`; `::` and `::1` were handled above.
        [0, 0, 0, 0, 0, 0, _, _] => AddrClass::Denied(DeniedKind::Reserved),
        // Local-use NAT64 `64:ff9b:1::/48`: the translator is on the local
        // network, so even a public embedded address is reached privately.
        [0x64, 0xff9b, 1, ..] => match classify_v4(last_v4) {
            AddrClass::Public => AddrClass::Private,
            other => other,
        },
        // 6to4 `2002::/16` carries the IPv4 address in bytes 2 to 5.
        [0x2002, ..] => classify_v4(Ipv4Addr::new(octets[2], octets[3], octets[4], octets[5])),
        // Teredo `2001::/32` carries the client's IPv4 address inverted.
        [0x2001, 0, ..] => classify_v4(!last_v4),
        [first, ..] if first & 0xffc0 == 0xfe80 => AddrClass::Denied(DeniedKind::LinkLocal),
        [first, ..] if first & 0xffc0 == 0xfec0 => AddrClass::Denied(DeniedKind::Reserved),
        [first, ..] if first & 0xff00 == 0xff00 => AddrClass::Denied(DeniedKind::Multicast),
        [first, ..] if first & 0xfe00 == 0xfc00 => AddrClass::Private,
        _ => AddrClass::Public,
    }
}

/// System `getaddrinfo` (via `tokio::net::lookup_host`) filtered by `permits`.
///
/// Installed in every upstream client by
/// [`build_client`](super::client::build_client). reqwest never consults the
/// resolver for IP-literal hosts; those are checked by [`validate_base_url`].
#[derive(Debug, Clone)]
pub struct SafeResolver {
    allow_private: bool,
}

impl SafeResolver {
    /// A resolver that admits private and loopback addresses only when
    /// `allow_private` is set.
    pub fn new(allow_private: bool) -> Self {
        Self { allow_private }
    }
}

impl Resolve for SafeResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let allow_private = self.allow_private;
        Box::pin(async move {
            let host = name.as_str();
            // `lookup_host` runs getaddrinfo on tokio's blocking pool: the one
            // blocking call R1 allows, and only on a cold connection. Port 0
            // is replaced by reqwest with the URL's port.
            let resolved = tokio::net::lookup_host((host, 0)).await?;
            let mut denied = 0_usize;
            let permitted: Vec<SocketAddr> = resolved
                .filter(|addr| {
                    let ok = permits(addr.ip(), allow_private);
                    denied += usize::from(!ok);
                    ok
                })
                .collect();
            if permitted.is_empty() {
                return Err(Box::new(ResolveError {
                    host: host.into(),
                    denied,
                }) as BoxError);
            }
            Ok(Box::new(permitted.into_iter()) as Addrs)
        })
    }
}

/// Every address a name resolved to is outside the channel's address policy.
/// Carries only the host name and a count, never the addresses (R17).
#[derive(Debug, thiserror::Error)]
#[error("{host}: all {denied} resolved addresses are not allowed for this upstream")]
struct ResolveError {
    host: Box<str>,
    denied: usize,
}

/// Parses and checks a channel base URL (R25). IP-literal hosts are checked
/// with `permits` here because they never reach the resolver; the host kind
/// comes from `Url::host()`, never from parsing `host_str()`.
pub fn validate_base_url(raw: &str, allow_private: bool) -> Result<Url, BaseUrlError> {
    let url = Url::parse(raw).map_err(BaseUrlError::Parse)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(BaseUrlError::Scheme);
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(BaseUrlError::ExtraParts);
    }
    // The WHATWG parser has already turned decimal, hex, octal and
    // trailing-dot IPv4 spellings into `Host::Ipv4`, so the literal check
    // below cannot be bypassed by an unusual spelling.
    let ip = match url.host() {
        None => return Err(BaseUrlError::NoHost),
        Some(Host::Domain(_)) => None,
        Some(Host::Ipv4(v4)) => Some(IpAddr::V4(v4)),
        Some(Host::Ipv6(v6)) => Some(IpAddr::V6(v6)),
    };
    if let Some(ip) = ip
        && !permits(ip, allow_private)
    {
        return Err(BaseUrlError::Forbidden(ip));
    }
    if ends_with_chat_endpoint(&url) {
        return Err(BaseUrlError::EndpointPath);
    }
    Ok(url)
}

/// The last two non-empty path segments are `chat` and `completions`, i.e.
/// the operator pasted the endpoint instead of the base URL; appending the
/// endpoint again would produce `.../chat/completions/chat/completions`.
fn ends_with_chat_endpoint(url: &Url) -> bool {
    let mut segments = url.path().rsplit('/').filter(|s| !s.is_empty());
    let last = segments.next();
    let before = segments.next();
    matches!(
        (before, last),
        (Some(chat), Some(completions))
            if chat.eq_ignore_ascii_case("chat") && completions.eq_ignore_ascii_case("completions")
    )
}

/// Why [`validate_base_url`] rejected a URL.
#[derive(Debug, thiserror::Error)]
pub enum BaseUrlError {
    /// Not a URL at all.
    #[error("invalid URL")]
    Parse(#[source] url::ParseError),
    /// Only `http` and `https` are forwarded to.
    #[error("scheme must be http or https")]
    Scheme,
    /// A URL without a host.
    #[error("URL must have a host")]
    NoHost,
    /// User info, a query or a fragment; none of them belongs in a base URL.
    #[error("URL must not contain credentials, a query or a fragment")]
    ExtraParts,
    /// The path already ends with the endpoint that Brisk appends (D1).
    #[error("base URL must not end with /chat/completions")]
    EndpointPath,
    /// An IP-literal host outside the channel's address policy.
    #[error("address {0} is not allowed")]
    Forbidden(IpAddr),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> AddrClass {
        classify(s.parse::<Ipv4Addr>().unwrap().into())
    }

    fn v6(s: &str) -> AddrClass {
        classify(s.parse::<Ipv6Addr>().unwrap().into())
    }

    const METADATA: AddrClass = AddrClass::Denied(DeniedKind::CloudMetadata);
    const LINK_LOCAL: AddrClass = AddrClass::Denied(DeniedKind::LinkLocal);
    const UNSPECIFIED: AddrClass = AddrClass::Denied(DeniedKind::Unspecified);
    const MULTICAST: AddrClass = AddrClass::Denied(DeniedKind::Multicast);
    const BROADCAST: AddrClass = AddrClass::Denied(DeniedKind::Broadcast);
    const RESERVED: AddrClass = AddrClass::Denied(DeniedKind::Reserved);

    #[test]
    fn ipv4_table() {
        let cases = [
            ("169.254.169.254", METADATA),
            ("100.100.100.200", METADATA),
            ("168.63.129.16", METADATA),
            ("192.0.0.192", METADATA),
            ("169.254.0.1", LINK_LOCAL),
            ("169.254.255.255", LINK_LOCAL),
            ("0.0.0.0", UNSPECIFIED),
            ("0.255.1.2", UNSPECIFIED),
            ("224.0.0.1", MULTICAST),
            ("239.255.255.255", MULTICAST),
            ("255.255.255.255", BROADCAST),
            ("240.0.0.1", RESERVED),
            ("255.255.255.254", RESERVED),
            ("127.0.0.1", AddrClass::Loopback),
            ("127.255.0.9", AddrClass::Loopback),
            ("10.0.0.1", AddrClass::Private),
            ("172.16.0.1", AddrClass::Private),
            ("172.31.255.255", AddrClass::Private),
            ("192.168.10.180", AddrClass::Private),
            ("100.64.0.1", AddrClass::Private),
            ("100.127.255.255", AddrClass::Private),
            ("192.0.0.1", AddrClass::Private),
            ("192.0.0.191", AddrClass::Private),
            ("198.18.0.1", AddrClass::Private),
            ("198.19.255.255", AddrClass::Private),
            ("172.15.255.255", AddrClass::Public),
            ("172.32.0.0", AddrClass::Public),
            ("100.63.255.255", AddrClass::Public),
            ("100.128.0.0", AddrClass::Public),
            ("192.0.1.1", AddrClass::Public),
            ("198.20.0.0", AddrClass::Public),
            ("8.8.8.8", AddrClass::Public),
            ("168.63.129.17", AddrClass::Public),
            ("223.255.255.255", AddrClass::Public),
        ];
        for (addr, expected) in cases {
            assert_eq!(v4(addr), expected, "{addr}");
        }
    }

    #[test]
    fn ipv6_table() {
        let cases = [
            ("fd00:ec2::254", METADATA),
            ("::", UNSPECIFIED),
            ("::1", AddrClass::Loopback),
            ("fe80::1", LINK_LOCAL),
            ("febf::1", LINK_LOCAL),
            ("fec0::1", RESERVED),
            ("feff::1", RESERVED),
            ("ff02::1", MULTICAST),
            ("fc00::1", AddrClass::Private),
            ("fd12:3456::1", AddrClass::Private),
            ("fd00:ec2::253", AddrClass::Private),
            ("2606:4700::1111", AddrClass::Public),
            // IPv4-compatible `::/96`.
            ("::2", RESERVED),
            ("::a9fe:a9fe", RESERVED),
            ("::8.8.8.8", RESERVED),
            // IPv4-mapped.
            ("::ffff:169.254.169.254", METADATA),
            ("::ffff:127.0.0.1", AddrClass::Loopback),
            ("::ffff:10.1.2.3", AddrClass::Private),
            ("::ffff:8.8.8.8", AddrClass::Public),
            ("::ffff:0.0.0.0", UNSPECIFIED),
            // NAT64.
            ("64:ff9b::a9fe:a9fe", METADATA),
            ("64:ff9b::7f00:1", AddrClass::Loopback),
            ("64:ff9b::808:808", AddrClass::Public),
            // Local-use NAT64: public embedded addresses become private.
            ("64:ff9b:1::808:808", AddrClass::Private),
            ("64:ff9b:1::a9fe:a9fe", METADATA),
            ("64:ff9b:1::7f00:1", AddrClass::Loopback),
            // 6to4.
            ("2002:a9fe:a9fe::1", METADATA),
            ("2002:7f00:1::", AddrClass::Loopback),
            ("2002:c0a8:0101::1", AddrClass::Private),
            ("2002:808:808::1", AddrClass::Public),
            // Teredo: the last 32 bits hold the inverted client address.
            ("2001:0:4136:e378:8000:63bf:5601:5601", METADATA),
            ("2001:0:4136:e378:8000:63bf:80ff:fffe", AddrClass::Loopback),
            ("2001:0:4136:e378:8000:63bf:f7f7:f7f7", AddrClass::Public),
            // Outside Teredo: 2001:db8::/32 is not `2001::/32`.
            ("2001:db8::1", AddrClass::Public),
        ];
        for (addr, expected) in cases {
            assert_eq!(v6(addr), expected, "{addr}");
        }
    }

    #[test]
    fn permits_follows_the_class() {
        let public: IpAddr = "8.8.8.8".parse().unwrap();
        let private: IpAddr = "192.168.1.1".parse().unwrap();
        let loopback: IpAddr = "::1".parse().unwrap();
        let metadata: IpAddr = "100.100.100.200".parse().unwrap();
        for allow in [false, true] {
            assert!(permits(public, allow));
            assert_eq!(permits(private, allow), allow);
            assert_eq!(permits(loopback, allow), allow);
            assert!(!permits(metadata, allow));
        }
    }

    /// The URLs of the security review (S4), with the expected outcome for
    /// `allow_private = false` and `true`: `true` means accepted.
    #[test]
    fn ip_literal_spellings() {
        let cases = [
            ("http://[::1]/v1", false, true),
            ("http://[::ffff:169.254.169.254]/v1", false, false),
            ("http://[::ffff:7f00:1]/v1", false, true),
            ("http://2852039166/v1", false, false),
            ("http://0xa9.0xfe.0xa9.0xfe/v1", false, false),
            ("http://169.254.169.254./v1", false, false),
            ("http://0177.0.0.1/v1", false, true),
            ("http://[fd00:ec2::254]/v1", false, false),
        ];
        for (raw, strict, lenient) in cases {
            for (allow, expected) in [(false, strict), (true, lenient)] {
                let result = validate_base_url(raw, allow);
                assert_eq!(
                    result.is_ok(),
                    expected,
                    "{raw} allow_private={allow}: {result:?}"
                );
                if let Err(err) = result {
                    assert!(matches!(err, BaseUrlError::Forbidden(_)), "{raw}: {err:?}");
                }
            }
        }
    }

    #[test]
    fn forbidden_reports_the_normalized_address() {
        let err = validate_base_url("http://2852039166/v1", true).unwrap_err();
        assert!(
            matches!(err, BaseUrlError::Forbidden(ip) if ip == IpAddr::from([169, 254, 169, 254])),
            "{err:?}"
        );
    }

    #[test]
    fn domain_hosts_are_left_to_the_resolver() {
        let url = validate_base_url("https://api.openai.com/v1", false).unwrap();
        assert_eq!(url.as_str(), "https://api.openai.com/v1");
        validate_base_url("http://localhost:8317/v1", false).unwrap();
        validate_base_url("http://metadata.google.internal/v1", false).unwrap();
    }

    #[test]
    fn accepted_forms() {
        for raw in [
            "http://192.168.10.180:8317/v1",
            "http://192.168.10.180:8317/v1/",
            "https://example.com",
            "https://example.com/openai/v1",
            "HTTP://EXAMPLE.COM/v1",
        ] {
            let allow_private = raw.contains("192.168.");
            validate_base_url(raw, allow_private).unwrap();
        }
    }

    #[test]
    fn rejected_forms() {
        type Check = fn(&BaseUrlError) -> bool;
        let cases: [(&str, Check); 11] = [
            ("not a url", |e| matches!(e, BaseUrlError::Parse(_))),
            ("http://", |e| matches!(e, BaseUrlError::Parse(_))),
            ("ftp://example.com/v1", |e| {
                matches!(e, BaseUrlError::Scheme)
            }),
            ("file:///etc/passwd", |e| matches!(e, BaseUrlError::Scheme)),
            ("http://user:pw@example.com/v1", |e| {
                matches!(e, BaseUrlError::ExtraParts)
            }),
            ("http://user@example.com/v1", |e| {
                matches!(e, BaseUrlError::ExtraParts)
            }),
            ("http://example.com/v1?key=x", |e| {
                matches!(e, BaseUrlError::ExtraParts)
            }),
            ("http://example.com/v1?", |e| {
                matches!(e, BaseUrlError::ExtraParts)
            }),
            ("http://example.com/v1#frag", |e| {
                matches!(e, BaseUrlError::ExtraParts)
            }),
            ("http://example.com/v1/chat/completions", |e| {
                matches!(e, BaseUrlError::EndpointPath)
            }),
            ("http://example.com/v1/Chat/Completions/", |e| {
                matches!(e, BaseUrlError::EndpointPath)
            }),
        ];
        for (raw, expected) in cases {
            let err = validate_base_url(raw, true).unwrap_err();
            assert!(expected(&err), "{raw}: {err:?}");
        }
    }

    #[test]
    fn private_literals_need_the_opt_in() {
        let err = validate_base_url("http://192.168.10.180:8317/v1", false).unwrap_err();
        assert!(matches!(err, BaseUrlError::Forbidden(_)), "{err:?}");
        validate_base_url("http://192.168.10.180:8317/v1", true).unwrap();
        for raw in [
            "http://169.254.169.254/v1",
            "http://100.100.100.200/v1",
            "http://168.63.129.16/v1",
            "http://192.0.0.192/v1",
            "http://0.0.0.0/v1",
            "http://[fe80::1]/v1",
            "http://224.0.0.1/v1",
        ] {
            let err = validate_base_url(raw, true).unwrap_err();
            assert!(matches!(err, BaseUrlError::Forbidden(_)), "{raw}: {err:?}");
        }
    }

    #[tokio::test]
    async fn resolver_filters_loopback_without_the_opt_in() {
        let name: Name = "localhost".parse().unwrap();
        let Err(err) = SafeResolver::new(false).resolve(name).await else {
            panic!("localhost resolved although loopback is not allowed");
        };
        let message = err.to_string();
        assert!(message.starts_with("localhost: all "), "{message}");
        assert!(message.contains("not allowed"), "{message}");
        assert!(!message.contains("127.0.0.1"), "{message}");

        let name: Name = "localhost".parse().unwrap();
        let Ok(addrs) = SafeResolver::new(true).resolve(name).await else {
            panic!("localhost did not resolve with allow_private");
        };
        let addrs: Vec<SocketAddr> = addrs.collect();
        assert!(!addrs.is_empty());
        assert!(
            addrs.iter().all(|addr| addr.ip().is_loopback()),
            "{addrs:?}"
        );
    }
}

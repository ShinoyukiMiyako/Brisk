//! Virtual-key authentication (D6): the key format, the digest table the
//! configuration stores instead of keys, extraction of the credential from
//! the request headers with every ambiguous case rejected, and removal of
//! credentials from the request URI before routing (R17, R19).
//!
//! A key is `bk-` followed by 36 base64url characters without padding. They
//! decode to 27 bytes: 24 random bytes and 3 check bytes, the first 3 bytes
//! of `SHA-256(b"brisk-vk1" || random)`. The check bytes let a mistyped or
//! made-up key be refused before the table lookup; the table itself holds
//! only SHA-256 digests of whole keys, so the configuration never contains a
//! usable key.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hashbrown::{HashMap, HashSet};
use http::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use http::uri::{PathAndQuery, Uri};
use sha2::{Digest, Sha256};

use crate::secret::Redacted;
use crate::spec::{KeyId, KeySpec};

/// Prefix of every virtual key.
pub const KEY_PREFIX: &str = "bk-";
/// Random bytes in a key, supplied by the caller of [`format_key`].
pub const KEY_RANDOM_BYTES: usize = 24;
/// Length of a key string in bytes.
pub const KEY_LEN: usize = 39;

/// Check bytes appended to the random part.
const KEY_CHECK_BYTES: usize = 3;
/// Decoded length of the base64url part: random bytes plus check bytes.
const KEY_PAYLOAD_BYTES: usize = KEY_RANDOM_BYTES + KEY_CHECK_BYTES;
/// Domain separator of the check-byte hash, so the check bytes cannot be
/// confused with a digest computed for any other purpose.
const KEY_CHECK_DOMAIN: &[u8] = b"brisk-vk1";

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const X_GOOG_API_KEY: HeaderName = HeaderName::from_static("x-goog-api-key");

/// Formats a key from caller-supplied OS randomness.
pub fn format_key(random: &[u8; KEY_RANDOM_BYTES]) -> Redacted<String> {
    let mut payload = [0_u8; KEY_PAYLOAD_BYTES];
    payload[..KEY_RANDOM_BYTES].copy_from_slice(random);
    payload[KEY_RANDOM_BYTES..].copy_from_slice(&check_bytes(random));
    let mut key = String::with_capacity(KEY_LEN);
    key.push_str(KEY_PREFIX);
    URL_SAFE_NO_PAD.encode_string(payload, &mut key);
    debug_assert_eq!(key.len(), KEY_LEN);
    Redacted::new(key)
}

/// Prefix, length, alphabet and check bytes; no table lookup.
pub fn is_well_formed(key: &[u8]) -> bool {
    if key.len() != KEY_LEN {
        return false;
    }
    let Some(encoded) = key.strip_prefix(KEY_PREFIX.as_bytes()) else {
        return false;
    };
    let mut payload = [0_u8; KEY_PAYLOAD_BYTES];
    // 36 characters carry exactly 27 bytes with no leftover bits, so every
    // accepted string is the canonical encoding of its payload.
    if !matches!(
        URL_SAFE_NO_PAD.decode_slice(encoded, &mut payload),
        Ok(KEY_PAYLOAD_BYTES)
    ) {
        return false;
    }
    let (random, check) = payload.split_at(KEY_RANDOM_BYTES);
    check == check_bytes(random)
}

/// SHA-256 of the whole key string, the value stored in `KeySpec::sha256`.
pub fn key_digest(key: &[u8]) -> [u8; 32] {
    Sha256::digest(key).into()
}

fn check_bytes(random: &[u8]) -> [u8; KEY_CHECK_BYTES] {
    let digest = Sha256::new()
        .chain_update(KEY_CHECK_DOMAIN)
        .chain_update(random)
        .finalize();
    let mut check = [0_u8; KEY_CHECK_BYTES];
    check.copy_from_slice(&digest[..KEY_CHECK_BYTES]);
    check
}

/// A configured key as seen after authentication.
#[derive(Debug)]
pub struct KeyEntry {
    /// Position of the key in `GatewaySpec::keys`.
    pub id: KeyId,
    /// The configured name; safe to log, unlike the key.
    pub name: Box<str>,
}

/// Configured keys by the SHA-256 digest of the key string.
#[derive(Debug)]
pub struct KeyTable {
    by_digest: HashMap<[u8; 32], Arc<KeyEntry>>,
}

impl KeyTable {
    /// Builds the table; the [`KeyId`] of each key is its index in `keys`.
    ///
    /// # Panics
    ///
    /// Panics if `keys` has more than `u32::MAX` entries, which no
    /// configuration can reach.
    pub fn build(keys: &[KeySpec]) -> Result<Self, KeyTableError> {
        if keys.is_empty() {
            return Err(KeyTableError::Empty);
        }
        let mut by_digest: HashMap<[u8; 32], Arc<KeyEntry>> = HashMap::with_capacity(keys.len());
        let mut names: HashSet<&str> = HashSet::with_capacity(keys.len());
        for (index, spec) in keys.iter().enumerate() {
            if !names.insert(spec.name.as_str()) {
                return Err(KeyTableError::DuplicateName(spec.name.clone()));
            }
            if let Some(first) = by_digest.get(&spec.sha256) {
                return Err(KeyTableError::DuplicateDigest {
                    first: first.name.to_string(),
                    second: spec.name.clone(),
                });
            }
            let id = KeyId(u32::try_from(index).expect("fewer than 2^32 keys"));
            let entry = KeyEntry {
                id,
                name: spec.name.as_str().into(),
            };
            by_digest.insert(spec.sha256, Arc::new(entry));
        }
        Ok(Self { by_digest })
    }

    /// Credential from `Authorization: Bearer <key>`, `x-api-key` or
    /// `x-goog-api-key`; the rules below decide ambiguous cases.
    ///
    /// Front proxies and WAFs often keep the last of repeated headers, so
    /// taking the first one here would let the two disagree about who sent
    /// the request. Every ambiguity is therefore an error, never a guess:
    ///
    /// 1. None of the three headers present: [`AuthError::Missing`].
    /// 2. Any of them present more than once: [`AuthError::Malformed`].
    /// 3. `authorization` must be the scheme `bearer` (any letter case),
    ///    exactly one space, then the key, with no trimming; anything else,
    ///    including other schemes, is `Malformed` and never falls back to
    ///    the other headers.
    /// 4. Keys in more than one header must be identical, else `Malformed`.
    /// 5. A key that fails [`is_well_formed`] is `Malformed`; a well-formed
    ///    key missing from the table is [`AuthError::Unknown`].
    ///
    /// Nothing is allocated: the digest is computed on the stack.
    pub fn authenticate(&self, headers: &HeaderMap) -> Result<&Arc<KeyEntry>, AuthError> {
        let bearer = match single_value(headers, &AUTHORIZATION)? {
            Some(value) => Some(bearer_token(value)?),
            None => None,
        };
        let api_key = single_value(headers, &X_API_KEY)?.map(HeaderValue::as_bytes);
        let goog_key = single_value(headers, &X_GOOG_API_KEY)?.map(HeaderValue::as_bytes);

        let mut key: Option<&[u8]> = None;
        for candidate in [bearer, api_key, goog_key].into_iter().flatten() {
            match key {
                Some(seen) if seen != candidate => return Err(AuthError::Malformed),
                _ => key = Some(candidate),
            }
        }
        let key = key.ok_or(AuthError::Missing)?;
        if !is_well_formed(key) {
            return Err(AuthError::Malformed);
        }
        self.by_digest
            .get(&key_digest(key))
            .ok_or(AuthError::Unknown)
    }
}

/// The only value of `name`, `None` when absent, `Malformed` when repeated.
fn single_value<'h>(
    headers: &'h HeaderMap,
    name: &HeaderName,
) -> Result<Option<&'h HeaderValue>, AuthError> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(AuthError::Malformed);
    }
    Ok(first)
}

/// The key of `Bearer <key>`: scheme case-insensitive, one space, no trim.
fn bearer_token(value: &HeaderValue) -> Result<&[u8], AuthError> {
    const SCHEME: &[u8] = b"bearer";
    let bytes = value.as_bytes();
    match bytes.split_at_checked(SCHEME.len()) {
        Some((scheme, [b' ', key @ ..])) if scheme.eq_ignore_ascii_case(SCHEME) => Ok(key),
        _ => Err(AuthError::Malformed),
    }
}

/// Why a request was not authenticated; every variant becomes a 401.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// No credential header at all.
    #[error("no API key")]
    Missing,
    /// A credential is present but repeated, ambiguous or not a key.
    #[error("malformed API key")]
    Malformed,
    /// A well-formed key that is not configured.
    #[error("unknown API key")]
    Unknown,
}

/// Why the configured keys cannot form a table.
#[derive(Debug, thiserror::Error)]
pub enum KeyTableError {
    /// The configuration lists no key.
    #[error("no keys configured")]
    Empty,
    /// Two entries store the same digest, so they are the same key.
    #[error("keys {first:?} and {second:?} have the same digest")]
    DuplicateDigest {
        /// Name of the earlier entry.
        first: String,
        /// Name of the later entry.
        second: String,
    },
    /// Two entries share a name, which would make logs ambiguous.
    #[error("duplicate key name {0:?}")]
    DuplicateName(String),
}

/// Query parameters removed from the inbound URI before any processing (D10).
pub const CREDENTIAL_QUERY_PARAMS: [&str; 2] = ["key", "auth_token"];

/// The URI without credential query parameters (names compared after
/// percent-decoding); `None` when nothing had to be removed, so the common
/// case does not allocate.
///
/// The remaining parameters keep their order and spelling; empty parameters
/// (`a=1&&b=2`) are dropped, and the `?` goes away when nothing remains.
///
/// # Panics
///
/// Never in practice: the rebuilt path and query consist of bytes taken
/// from the valid input URI, and its other parts are reused unchanged.
pub fn strip_credential_query(uri: &Uri) -> Option<Uri> {
    let query = uri.query()?;
    if !query.split('&').any(is_credential_param) {
        return None;
    }
    let path = uri.path();
    let mut path_and_query = String::with_capacity(path.len() + 1 + query.len());
    path_and_query.push_str(path);
    let mut separator = '?';
    for param in query
        .split('&')
        .filter(|param| !param.is_empty() && !is_credential_param(param))
    {
        path_and_query.push(separator);
        path_and_query.push_str(param);
        separator = '&';
    }
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(
        PathAndQuery::try_from(path_and_query)
            .expect("a subset of a valid path and query is valid"),
    );
    Some(Uri::from_parts(parts).expect("only the path and query changed"))
}

fn is_credential_param(param: &str) -> bool {
    let name = param.split_once('=').map_or(param, |(name, _)| name);
    CREDENTIAL_QUERY_PARAMS
        .iter()
        .any(|credential| percent_decoded_eq(name.as_bytes(), credential.as_bytes()))
}

/// `raw` percent-decoded equals `target`, compared without allocating. An
/// escape that is not `%` plus two hex digits stands for itself, as in the
/// WHATWG URL parser.
fn percent_decoded_eq(raw: &[u8], target: &[u8]) -> bool {
    let mut expected = target.iter();
    let mut rest = raw;
    while let Some((&first, tail)) = rest.split_first() {
        let (byte, tail) = match (first, tail) {
            (b'%', [high, low, after @ ..]) => match (hex_value(*high), hex_value(*low)) {
                (Some(high), Some(low)) => ((high << 4) | low, after),
                _ => (first, tail),
            },
            _ => (first, tail),
        };
        if expected.next() != Some(&byte) {
            return false;
        }
        rest = tail;
    }
    expected.next().is_none()
}

fn hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RANDOM_A: [u8; KEY_RANDOM_BYTES] = [7; KEY_RANDOM_BYTES];
    const RANDOM_B: [u8; KEY_RANDOM_BYTES] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 250, 251, 252, 253, 254, 255, 62, 63, 190, 191, 128,
        127,
    ];

    fn key(random: &[u8; KEY_RANDOM_BYTES]) -> String {
        format_key(random).into_inner()
    }

    fn spec(name: &str, key: &str) -> KeySpec {
        KeySpec {
            name: name.to_owned(),
            sha256: key_digest(key.as_bytes()),
        }
    }

    fn table() -> KeyTable {
        KeyTable::build(&[
            spec("alpha", &key(&RANDOM_A)),
            spec("beta", &key(&RANDOM_B)),
        ])
        .expect("distinct keys")
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for &(name, value) in pairs {
            map.append(
                HeaderName::from_static(name),
                HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        map
    }

    fn authenticated_name(
        table: &KeyTable,
        pairs: &[(&'static str, &str)],
    ) -> Result<String, AuthError> {
        table
            .authenticate(&headers(pairs))
            .map(|entry| entry.name.to_string())
    }

    #[test]
    fn formatted_keys_are_well_formed() {
        for random in [
            RANDOM_A,
            RANDOM_B,
            [0; KEY_RANDOM_BYTES],
            [255; KEY_RANDOM_BYTES],
        ] {
            let key = key(&random);
            assert_eq!(key.len(), KEY_LEN);
            assert!(key.starts_with(KEY_PREFIX));
            assert!(
                key[KEY_PREFIX.len()..]
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "base64url alphabet only: {key}"
            );
            assert!(is_well_formed(key.as_bytes()), "{key}");
        }
    }

    #[test]
    fn formatted_key_decodes_to_random_and_check_bytes() {
        let key = key(&RANDOM_B);
        let payload = URL_SAFE_NO_PAD
            .decode(&key[KEY_PREFIX.len()..])
            .expect("valid base64url");
        assert_eq!(payload[..KEY_RANDOM_BYTES], RANDOM_B);
        let mut hasher = Sha256::new();
        hasher.update(b"brisk-vk1");
        hasher.update(RANDOM_B);
        assert_eq!(payload[KEY_RANDOM_BYTES..], hasher.finalize()[..3]);
    }

    #[test]
    fn format_key_redacts_its_output() {
        assert_eq!(format!("{:?}", format_key(&RANDOM_A)), "***");
        assert_eq!(format_key(&RANDOM_A).to_string(), "***");
    }

    #[test]
    fn changing_any_character_breaks_the_key() {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let original = key(&RANDOM_B).into_bytes();
        for position in 0..KEY_LEN {
            for &replacement in ALPHABET {
                if replacement == original[position] {
                    continue;
                }
                let mut changed = original.clone();
                changed[position] = replacement;
                assert!(
                    !is_well_formed(&changed),
                    "position {position} replaced by {:?} still passes",
                    char::from(replacement)
                );
            }
        }
    }

    #[test]
    fn prefix_length_and_alphabet_errors_fail() {
        let good = key(&RANDOM_A);
        let body = &good[KEY_PREFIX.len()..];
        assert!(!is_well_formed(b""));
        assert!(!is_well_formed(format!("sk-{body}").as_bytes()));
        assert!(!is_well_formed(format!("BK-{body}").as_bytes()));
        assert!(!is_well_formed(format!("bk_{body}").as_bytes()));
        assert!(!is_well_formed(&good.as_bytes()[..KEY_LEN - 1]));
        assert!(!is_well_formed(format!("{good}A").as_bytes()));
        assert!(!is_well_formed(format!(" {good}").as_bytes()));
        // Standard base64 characters are outside the base64url alphabet.
        let standard = good.replace('-', "+").replace('_', "/");
        if standard != good {
            assert!(!is_well_formed(standard.as_bytes()));
        }
        let mut padded = good[..KEY_LEN - 1].to_owned();
        padded.push('=');
        assert!(!is_well_formed(padded.as_bytes()));
        let mut non_ascii = good.clone().into_bytes();
        non_ascii[10] = 0xC3;
        assert!(!is_well_formed(&non_ascii));
    }

    #[test]
    fn key_digest_matches_known_vectors() {
        assert_eq!(
            key_digest(b""),
            *b"\xe3\xb0\xc4\x42\x98\xfc\x1c\x14\x9a\xfb\xf4\xc8\x99\x6f\xb9\x24\
               \x27\xae\x41\xe4\x64\x9b\x93\x4c\xa4\x95\x99\x1b\x78\x52\xb8\x55"
        );
        assert_eq!(
            key_digest(b"abc"),
            *b"\xba\x78\x16\xbf\x8f\x01\xcf\xea\x41\x41\x40\xde\x5d\xae\x22\x23\
               \xb0\x03\x61\xa3\x96\x17\x7a\x9c\xb4\x10\xff\x61\xf2\x00\x15\xad"
        );
    }

    #[test]
    fn build_assigns_ids_in_order() {
        let table = table();
        let alpha = table
            .authenticate(&headers(&[("x-api-key", &key(&RANDOM_A))]))
            .expect("alpha is configured");
        let beta = table
            .authenticate(&headers(&[("x-api-key", &key(&RANDOM_B))]))
            .expect("beta is configured");
        assert_eq!((alpha.id, &*alpha.name), (KeyId(0), "alpha"));
        assert_eq!((beta.id, &*beta.name), (KeyId(1), "beta"));
    }

    #[test]
    fn build_rejects_empty_and_duplicates() {
        assert!(matches!(KeyTable::build(&[]), Err(KeyTableError::Empty)));

        let same_key = KeyTable::build(&[
            spec("alpha", &key(&RANDOM_A)),
            spec("beta", &key(&RANDOM_A)),
        ]);
        match same_key {
            Err(KeyTableError::DuplicateDigest { first, second }) => {
                assert_eq!((first.as_str(), second.as_str()), ("alpha", "beta"));
            }
            other => panic!("expected DuplicateDigest, got {other:?}"),
        }

        let same_name = KeyTable::build(&[
            spec("alpha", &key(&RANDOM_A)),
            spec("alpha", &key(&RANDOM_B)),
        ]);
        match same_name {
            Err(KeyTableError::DuplicateName(name)) => assert_eq!(name, "alpha"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn each_header_authenticates() {
        let table = table();
        let alpha = key(&RANDOM_A);
        let bearer = format!("Bearer {alpha}");
        assert_eq!(
            authenticated_name(&table, &[("authorization", &bearer)]).as_deref(),
            Ok("alpha")
        );
        assert_eq!(
            authenticated_name(&table, &[("x-api-key", &alpha)]).as_deref(),
            Ok("alpha")
        );
        assert_eq!(
            authenticated_name(&table, &[("x-goog-api-key", &alpha)]).as_deref(),
            Ok("alpha")
        );
    }

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        let table = table();
        let beta = key(&RANDOM_B);
        for scheme in ["bearer", "BEARER", "Bearer", "bEaReR"] {
            let value = format!("{scheme} {beta}");
            assert_eq!(
                authenticated_name(&table, &[("authorization", &value)]).as_deref(),
                Ok("beta"),
                "{scheme}"
            );
        }
    }

    #[test]
    fn no_credential_is_missing() {
        let table = table();
        assert_eq!(authenticated_name(&table, &[]), Err(AuthError::Missing));
        assert_eq!(
            authenticated_name(&table, &[("cookie", "key=1"), ("x-request-id", "r")]),
            Err(AuthError::Missing)
        );
    }

    #[test]
    fn repeated_header_is_malformed() {
        let table = table();
        let alpha = key(&RANDOM_A);
        let bearer = format!("Bearer {alpha}");
        for name in ["authorization", "x-api-key", "x-goog-api-key"] {
            let value = if name == "authorization" {
                bearer.as_str()
            } else {
                alpha.as_str()
            };
            assert_eq!(
                authenticated_name(&table, &[(name, value), (name, value)]),
                Err(AuthError::Malformed),
                "{name}"
            );
        }
    }

    #[test]
    fn different_keys_in_two_headers_are_malformed() {
        let table = table();
        let alpha = key(&RANDOM_A);
        let beta = key(&RANDOM_B);
        let bearer_alpha = format!("Bearer {alpha}");
        assert_eq!(
            authenticated_name(
                &table,
                &[("authorization", &bearer_alpha), ("x-api-key", &beta)]
            ),
            Err(AuthError::Malformed)
        );
        assert_eq!(
            authenticated_name(&table, &[("x-api-key", &alpha), ("x-goog-api-key", &beta)]),
            Err(AuthError::Malformed)
        );
        assert_eq!(
            authenticated_name(
                &table,
                &[
                    ("authorization", &bearer_alpha),
                    ("x-api-key", &alpha),
                    ("x-goog-api-key", &alpha)
                ]
            )
            .as_deref(),
            Ok("alpha")
        );
    }

    #[test]
    fn other_authorization_schemes_do_not_fall_back() {
        let table = table();
        let alpha = key(&RANDOM_A);
        assert_eq!(
            authenticated_name(
                &table,
                &[
                    ("authorization", "Basic dXNlcjpwYXNz"),
                    ("x-api-key", &alpha)
                ]
            ),
            Err(AuthError::Malformed)
        );
        assert_eq!(
            authenticated_name(&table, &[("authorization", &alpha)]),
            Err(AuthError::Malformed)
        );
    }

    #[test]
    fn bearer_is_parsed_strictly() {
        let table = table();
        let alpha = key(&RANDOM_A);
        for value in [
            format!("Bearer  {alpha}"),
            format!("Bearer {alpha} "),
            format!("Bearer\t{alpha}"),
            format!("Bearer{alpha}"),
            "Bearer".to_owned(),
            "Bearer ".to_owned(),
        ] {
            assert_eq!(
                authenticated_name(&table, &[("authorization", &value)]),
                Err(AuthError::Malformed),
                "{value:?}"
            );
        }
    }

    #[test]
    fn ill_formed_key_is_malformed_and_unconfigured_key_is_unknown() {
        let table = table();
        let mut broken = key(&RANDOM_A).into_bytes();
        broken[KEY_LEN - 1] = if broken[KEY_LEN - 1] == b'A' {
            b'B'
        } else {
            b'A'
        };
        let broken = String::from_utf8(broken).expect("ASCII");
        assert_eq!(
            authenticated_name(&table, &[("x-api-key", &broken)]),
            Err(AuthError::Malformed)
        );
        assert_eq!(
            authenticated_name(&table, &[("x-api-key", "sk-test")]),
            Err(AuthError::Malformed)
        );

        let unconfigured = key(&[42; KEY_RANDOM_BYTES]);
        assert_eq!(
            authenticated_name(&table, &[("x-api-key", &unconfigured)]),
            Err(AuthError::Unknown)
        );
    }

    fn stripped(uri: &str) -> Option<String> {
        strip_credential_query(&uri.parse().expect("valid URI")).map(|uri| uri.to_string())
    }

    #[test]
    fn credential_params_are_removed() {
        assert_eq!(
            stripped("/v1/chat/completions?key=a&x=1&auth_token=b").as_deref(),
            Some("/v1/chat/completions?x=1")
        );
        assert_eq!(
            stripped("/v1/models?key=bk-abc").as_deref(),
            Some("/v1/models")
        );
        assert_eq!(stripped("/p?auth_token&key").as_deref(), Some("/p"));
        assert_eq!(
            stripped("/p?a=1&key=x&b=2&&c").as_deref(),
            Some("/p?a=1&b=2&c")
        );
        assert_eq!(
            stripped("/v1beta/models/grok-4.6(xhigh):streamGenerateContent?alt=sse&key=bk-x")
                .as_deref(),
            Some("/v1beta/models/grok-4.6(xhigh):streamGenerateContent?alt=sse")
        );
        assert_eq!(
            stripped("http://gateway.test:8080/v1/models?key=1&x=2").as_deref(),
            Some("http://gateway.test:8080/v1/models?x=2")
        );
    }

    #[test]
    fn percent_encoded_names_are_removed() {
        assert_eq!(stripped("/p?k%65y=a&x=1").as_deref(), Some("/p?x=1"));
        assert_eq!(stripped("/p?%6B%65%79=a").as_deref(), Some("/p"));
        assert_eq!(stripped("/p?auth%5ftoken=b&y").as_deref(), Some("/p?y"));
    }

    #[test]
    fn other_params_are_left_alone() {
        assert_eq!(stripped("/v1/chat/completions"), None);
        assert_eq!(stripped("/v1/chat/completions?"), None);
        assert_eq!(
            stripped("/p?keys=a&monkey=b&Key=c&KEY=d&x_key=e&k%zzy=f&api_key=g"),
            None
        );
        assert_eq!(stripped("/p?a=key&b=auth_token"), None);
        assert_eq!(stripped("/p?k%65=a&k%6"), None);
    }

    #[test]
    fn percent_decoding_comparison() {
        assert!(percent_decoded_eq(b"key", b"key"));
        assert!(percent_decoded_eq(b"%6b%65%79", b"key"));
        assert!(!percent_decoded_eq(b"ke", b"key"));
        assert!(!percent_decoded_eq(b"keyy", b"key"));
        assert!(!percent_decoded_eq(b"%6", b"%6b"));
        assert!(percent_decoded_eq(b"%zz", b"%zz"));
        assert!(percent_decoded_eq(b"%25", b"%"));
    }
}

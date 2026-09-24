//! The immutable configuration snapshot every request reads (keys, channels,
//! model routes), reached only through a single accessor so that swapping it
//! at runtime later touches this module alone (R10, D9).

use std::sync::Arc;

use hashbrown::HashMap;

use crate::auth::KeyTable;
use crate::spec::ChannelSpec;
use crate::upstream::registry::ChannelSet;

/// Everything a request reads from the configuration, built once by
/// `Gateway::new` and never modified afterwards.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Virtual keys by digest.
    pub(crate) keys: KeyTable,
    /// Validated channels with their clients and weights.
    pub(crate) channels: ChannelSet,
    /// Channels per client-facing model name.
    pub(crate) routes: ModelRoutes,
}

/// Holder of the current [`Snapshot`].
#[derive(Debug)]
pub(crate) struct State {
    snapshot: Arc<Snapshot>,
}

impl State {
    /// Holds `snapshot` for the lifetime of the gateway (D9).
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self {
            snapshot: Arc::new(snapshot),
        }
    }

    /// The only way to reach the snapshot (R10). Returning a borrow keeps
    /// the request path free of reference-count traffic; a later hot-reload
    /// changes this accessor alone.
    pub(crate) fn resolve(&self) -> &Snapshot {
        &self.snapshot
    }
}

/// Model name to the bitmap of channels serving it; bit `i` is
/// `GatewaySpec::channels[i]`.
#[derive(Debug)]
pub(crate) struct ModelRoutes {
    /// Channels that list the model explicitly.
    exact: HashMap<Box<str>, u64>,
    /// Channels with an empty `models` list, which serve every model.
    any: u64,
}

impl ModelRoutes {
    /// Indexes `specs` by the client-facing model names they serve.
    ///
    /// # Panics
    ///
    /// With more than 64 channels, which do not fit the bitmap;
    /// `ChannelSet::build` rejects such a spec before this runs.
    pub(crate) fn build(specs: &[ChannelSpec]) -> Self {
        assert!(
            specs.len() <= 64,
            "{} channels do not fit the 64-bit channel bitmap",
            specs.len()
        );
        let mut exact: HashMap<Box<str>, u64> = HashMap::new();
        let mut any = 0;
        for (index, spec) in specs.iter().enumerate() {
            let bit = 1_u64 << index;
            if spec.models.is_empty() {
                any |= bit;
            }
            for model in &spec.models {
                *exact.entry(Box::from(model.as_str())).or_default() |= bit;
            }
        }
        Self { exact, any }
    }

    /// Bitmap of channels serving `model` (exact match on the decoded name).
    pub(crate) fn candidates(&self, model: &str) -> u64 {
        // A model no channel lists is still served by the catch-all channels.
        let listed = self.exact.get(model).copied().unwrap_or(0);
        listed | self.any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::Redacted;
    use crate::spec::KeySpec;
    use crate::spec::{StreamUsage, Timeouts, WarmupTarget};
    use crate::upstream::UpstreamClientConfig;

    const TEST_MODEL: &str = "grok-4.6(xhigh)";

    #[test]
    fn resolve_returns_the_snapshot_it_was_built_with() {
        let mut specs = [channel("a", &[TEST_MODEL]), channel("b", &["grok-4.6"])];
        for spec in &mut specs {
            // The test base URL is a loopback literal.
            spec.client.allow_private = true;
        }
        let state = State::new(Snapshot {
            keys: KeyTable::build(&[KeySpec {
                name: String::from("k"),
                sha256: [7; 32],
            }])
            .expect("one key"),
            channels: ChannelSet::build(&specs).expect("valid channels"),
            routes: ModelRoutes::build(&specs),
        });
        let snapshot = state.resolve();
        assert_eq!(snapshot.channels.len(), 2);
        assert_eq!(snapshot.routes.candidates(TEST_MODEL), 0b01);
        assert!(std::ptr::eq(snapshot, state.resolve()));
    }

    fn channel(name: &str, models: &[&str]) -> ChannelSpec {
        ChannelSpec {
            name: name.to_owned(),
            base_url: String::from("http://127.0.0.1:8317/v1"),
            api_key: Redacted::new(String::from("upstream-key")),
            weight: 1,
            models: models.iter().map(|&model| model.to_owned()).collect(),
            model_map: Vec::new(),
            stream_usage: StreamUsage::Passthrough,
            timeouts: Timeouts::default(),
            client: UpstreamClientConfig::default(),
            warmup: WarmupTarget::default(),
            expose_ratelimit_headers: false,
        }
    }

    #[test]
    fn bracketed_model_names_match_exactly() {
        let routes = ModelRoutes::build(&[
            channel("a", &[TEST_MODEL]),
            channel("b", &["grok-4.6"]),
            channel("c", &[TEST_MODEL, "gpt-5.5"]),
        ]);
        assert_eq!(routes.candidates(TEST_MODEL), 0b101);
        assert_eq!(routes.candidates("grok-4.6"), 0b010);
        assert_eq!(routes.candidates("gpt-5.5"), 0b100);
        assert_eq!(routes.candidates("grok-4.6(high)"), 0);
        assert_eq!(routes.candidates("grok-4.6(XHIGH)"), 0);
        assert_eq!(routes.candidates("grok-4.6(xhigh) "), 0);
        assert_eq!(routes.candidates(""), 0);
    }

    #[test]
    fn channels_without_models_serve_every_model() {
        let routes = ModelRoutes::build(&[channel("a", &[TEST_MODEL]), channel("any", &[])]);
        assert_eq!(routes.candidates(TEST_MODEL), 0b11);
        assert_eq!(routes.candidates("unlisted"), 0b10);
    }

    #[test]
    fn a_model_listed_twice_by_one_channel_sets_one_bit() {
        let routes = ModelRoutes::build(&[channel("a", &[TEST_MODEL, TEST_MODEL])]);
        assert_eq!(routes.candidates(TEST_MODEL), 0b1);
    }

    #[test]
    fn the_sixty_fourth_channel_uses_the_top_bit() {
        let mut specs: Vec<_> = (0..63)
            .map(|i| channel(&format!("c{i}"), &["other"]))
            .collect();
        specs.push(channel("last", &[TEST_MODEL]));
        let routes = ModelRoutes::build(&specs);
        assert_eq!(routes.candidates(TEST_MODEL), 1 << 63);
        assert_eq!(routes.candidates("other"), u64::MAX >> 1);
    }

    #[test]
    #[should_panic(expected = "65 channels")]
    fn more_than_sixty_four_channels_panic() {
        let specs: Vec<_> = (0..65).map(|i| channel(&format!("c{i}"), &[])).collect();
        let _ = ModelRoutes::build(&specs);
    }
}

//! Stream-lifetime distributions and arrival processes for open-loop load.
//!
//! Stream durations follow a log-normal distribution truncated at a hard
//! maximum (by rejection, never by clamping). With Little's law the arrival
//! rate that sustains a target concurrency is
//! `λ = concurrency / LogNormal::mean()`.

/// z-score of the 99th percentile of the standard normal distribution.
const Z_P99: f64 = 2.3263;

/// Errors from distribution constructors.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum DistError {
    /// A parameter is not a positive finite number.
    #[error("{name} must be positive and finite, got {value}")]
    NotPositive {
        /// Parameter name.
        name: &'static str,
        /// Offending value.
        value: f64,
    },
    /// The percentiles are not strictly increasing.
    #[error("require median < p99 < max, got median={median} p99={p99} max={max}")]
    Unordered {
        /// Median in seconds.
        median: f64,
        /// 99th percentile in seconds.
        p99: f64,
        /// Truncation point in seconds.
        max: f64,
    },
}

fn positive(name: &'static str, value: f64) -> Result<f64, DistError> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(DistError::NotPositive { name, value })
    }
}

/// Draws a standard normal variate with the Box–Muller transform.
fn standard_normal(rng: &mut fastrand::Rng) -> f64 {
    // 1 - [0, 1) is (0, 1], keeping ln() finite.
    let u1 = 1.0 - rng.f64();
    let u2 = rng.f64();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// A log-normal distribution truncated to `(0, max]`, in seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LogNormal {
    mu: f64,
    sigma: f64,
    max: f64,
}

impl LogNormal {
    /// Builds the distribution from its median and 99th percentile (before
    /// truncation): `μ = ln(median)`, `σ = ln(p99 / median) / 2.3263`.
    pub fn from_median_p99(median_s: f64, p99_s: f64, max_s: f64) -> Result<Self, DistError> {
        let median = positive("median", median_s)?;
        let p99 = positive("p99", p99_s)?;
        let max = positive("max", max_s)?;
        if !(median < p99 && p99 < max) {
            return Err(DistError::Unordered { median, p99, max });
        }
        Ok(Self {
            mu: median.ln(),
            sigma: (p99 / median).ln() / Z_P99,
            max,
        })
    }

    /// Location parameter of the underlying normal.
    pub fn mu(&self) -> f64 {
        self.mu
    }

    /// Scale parameter of the underlying normal.
    pub fn sigma(&self) -> f64 {
        self.sigma
    }

    /// Truncation point in seconds.
    pub fn max(&self) -> f64 {
        self.max
    }

    fn sample_with_mu(&self, mu: f64, rng: &mut fastrand::Rng) -> f64 {
        loop {
            let x = (mu + self.sigma * standard_normal(rng)).exp();
            if x <= self.max {
                return x;
            }
        }
    }

    /// Draws a duration in seconds; values above the maximum are rejected and
    /// redrawn.
    pub fn sample(&self, rng: &mut fastrand::Rng) -> f64 {
        self.sample_with_mu(self.mu, rng)
    }

    /// Draws the remaining lifetime of a stream observed at a random instant
    /// in steady state, for pre-populating the target concurrency at start.
    ///
    /// The total lifetime of such a stream is length-biased (density
    /// proportional to `x·f(x)`), which for a log-normal is again log-normal
    /// with `μ + σ²` (truncated at the same maximum); the elapsed fraction is
    /// uniform, so the remainder is that lifetime times `U(0, 1)`.
    pub fn sample_residual(&self, rng: &mut fastrand::Rng) -> f64 {
        let total = self.sample_with_mu(self.mu + self.sigma * self.sigma, rng);
        total * rng.f64()
    }

    /// Mean of the truncated distribution in seconds.
    ///
    /// Computed by Simpson integration over `y = ln x` from `μ − 12σ` to
    /// `ln max`: `E[X] = ∫ eʸ φ(y) dy / ∫ φ(y) dy`.
    pub fn mean(&self) -> f64 {
        const STEPS: u32 = 20_000;
        let lo = self.mu - 12.0 * self.sigma;
        let hi = self.max.ln();
        let h = (hi - lo) / f64::from(STEPS);
        let density = |y: f64| {
            let z = (y - self.mu) / self.sigma;
            (-0.5 * z * z).exp()
        };
        let (mut num, mut den) = (0.0, 0.0);
        for i in 0..=STEPS {
            let weight = if i == 0 || i == STEPS {
                1.0
            } else if i % 2 == 1 {
                4.0
            } else {
                2.0
            };
            let y = lo + h * f64::from(i);
            let d = density(y) * weight;
            num += d * y.exp();
            den += d;
        }
        num / den
    }
}

/// A Poisson arrival process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Poisson {
    rate_per_s: f64,
}

impl Poisson {
    /// Creates a process with the given mean arrival rate per second.
    pub fn new(rate_per_s: f64) -> Result<Self, DistError> {
        Ok(Self {
            rate_per_s: positive("rate_per_s", rate_per_s)?,
        })
    }

    /// Mean arrivals per second.
    pub fn rate_per_s(&self) -> f64 {
        self.rate_per_s
    }

    /// Draws the exponential gap to the next arrival in nanoseconds.
    pub fn next_interarrival_ns(&self, rng: &mut fastrand::Rng) -> u64 {
        let u = 1.0 - rng.f64();
        seconds_to_ns(-u.ln() / self.rate_per_s)
    }
}

/// Converts seconds to nanoseconds, clamping negatives and NaN to 0 and
/// saturating at `u64::MAX`.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "range checked before the cast"
)]
pub fn seconds_to_ns(seconds: f64) -> u64 {
    /// 2^64, exactly representable as f64.
    const U64_LIMIT: f64 = 18_446_744_073_709_551_616.0;
    let ns = (seconds * 1e9).round();
    if ns.is_nan() || ns <= 0.0 {
        0
    } else if ns >= U64_LIMIT {
        u64::MAX
    } else {
        ns as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s1() -> LogNormal {
        LogNormal::from_median_p99(5.0, 60.0, 120.0).unwrap()
    }

    #[test]
    fn truncated_mean_matches_design_value() {
        let mean = s1().mean();
        assert!((8.5..=8.9).contains(&mean), "mean={mean}");
    }

    #[test]
    fn samples_match_mean_and_truncation() {
        let d = s1();
        let mut rng = fastrand::Rng::with_seed(7);
        let n = 400_000;
        let mut sum = 0.0;
        let mut below_median = 0u32;
        for _ in 0..n {
            let x = d.sample(&mut rng);
            assert!(x > 0.0 && x <= 120.0);
            sum += x;
            if x < 5.0 {
                below_median += 1;
            }
        }
        let empirical = sum / f64::from(n);
        assert!((empirical - d.mean()).abs() < 0.15, "empirical={empirical}");
        // Truncation removes ~0.15% of the mass above the median only.
        let frac = f64::from(below_median) / f64::from(n);
        assert!((frac - 0.5).abs() < 0.01, "frac={frac}");
    }

    #[test]
    fn residual_mean_matches_renewal_theory() {
        // Mean residual life = E[X²] / (2 E[X]).
        let d = s1();
        let mut rng = fastrand::Rng::with_seed(11);
        let n = 400_000;
        let (mut s1, mut s2) = (0.0, 0.0);
        for _ in 0..n {
            let x = d.sample(&mut rng);
            s1 += x;
            s2 += x * x;
        }
        let expected = s2 / (2.0 * s1);
        let residual: f64 = (0..n).map(|_| d.sample_residual(&mut rng)).sum::<f64>() / f64::from(n);
        assert!(
            (residual - expected).abs() / expected < 0.03,
            "residual={residual} expected={expected}"
        );
    }

    #[test]
    #[expect(clippy::cast_precision_loss, reason = "sum is far below 2^53")]
    fn poisson_mean_gap() {
        let p = Poisson::new(113.0).unwrap();
        let mut rng = fastrand::Rng::with_seed(3);
        let n = 200_000u32;
        let total: u64 = (0..n).map(|_| p.next_interarrival_ns(&mut rng)).sum();
        let mean_s = total as f64 / 1e9 / f64::from(n);
        assert!((mean_s * 113.0 - 1.0).abs() < 0.01, "mean_s={mean_s}");
    }

    #[test]
    fn rejects_bad_parameters() {
        assert!(matches!(
            LogNormal::from_median_p99(0.0, 1.0, 2.0),
            Err(DistError::NotPositive { name: "median", .. })
        ));
        assert!(matches!(
            LogNormal::from_median_p99(5.0, 4.0, 120.0),
            Err(DistError::Unordered { .. })
        ));
        assert!(Poisson::new(f64::NAN).is_err());
        assert_eq!(seconds_to_ns(-1.0), 0);
        assert_eq!(seconds_to_ns(1.5), 1_500_000_000);
        assert_eq!(seconds_to_ns(1e30), u64::MAX);
    }
}

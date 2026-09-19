//! Weighted random channel selection among the candidates for a model,
//! skipping channels already tried for the request; allocation-free (R10).

/// Weighted random choice among `candidates & !tried`; `draw(n)` returns a
/// value in `0..n` (production: `fastrand::u64(0..n)`).
///
/// Bit `i` of `candidates` and `tried` stands for `weights[i]`; bits at or
/// beyond `weights.len()` are ignored. Returns `None` when no untried
/// candidate with a non-zero weight remains. `draw` is not called when exactly
/// one channel remains, so the common single-candidate case does no random
/// number generation.
///
/// # Panics
///
/// When `draw` breaks its contract and returns a value outside `0..n`.
pub(crate) fn pick(
    weights: &[u32],
    candidates: u64,
    tried: u64,
    draw: impl FnOnce(u64) -> u64,
) -> Option<usize> {
    let available = candidates & !tried & index_mask(weights.len());

    let mut total = 0_u64;
    let mut only = None;
    let mut count = 0_u32;
    for index in bits(available) {
        let weight = u64::from(weights[index]);
        if weight > 0 {
            total += weight;
            only = Some(index);
            count += 1;
        }
    }
    if count <= 1 {
        return only;
    }

    let target = draw(total);
    assert!(target < total, "draw returned {target}, outside 0..{total}");
    let mut remaining = target;
    for index in bits(available) {
        let weight = u64::from(weights[index]);
        if remaining < weight {
            return Some(index);
        }
        remaining -= weight;
    }
    unreachable!("target {target} lies below the total weight {total}")
}

/// Bits `0..len`, all of them when `len` reaches the width of the bitmap.
fn index_mask(len: usize) -> u64 {
    if len >= 64 {
        u64::MAX
    } else {
        (1_u64 << len) - 1
    }
}

/// Indices of the set bits of `bitmap`, lowest first.
fn bits(mut bitmap: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        if bitmap == 0 {
            return None;
        }
        let index = bitmap.trailing_zeros() as usize;
        bitmap &= bitmap - 1;
        Some(index)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn never(_: u64) -> u64 {
        panic!("draw must not be called")
    }

    #[test]
    fn draw_maps_to_weight_intervals_in_index_order() {
        let weights = [1, 3, 2];
        let all = 0b111;
        let expected = [0, 1, 1, 1, 2, 2];
        for (value, channel) in expected.into_iter().enumerate() {
            let value = value as u64;
            let picked = pick(&weights, all, 0, |n| {
                assert_eq!(n, 6);
                value
            });
            assert_eq!(picked, Some(channel), "draw {value}");
        }
    }

    #[test]
    fn tried_channels_leave_the_draw() {
        let weights = [1, 3, 2];
        // Channel 1 tried: the total becomes 1 + 2 and the intervals shift.
        let picks: Vec<_> = (0..3)
            .map(|value| {
                pick(&weights, 0b111, 0b010, |n| {
                    assert_eq!(n, 3);
                    value
                })
            })
            .collect();
        assert_eq!(picks, [Some(0), Some(2), Some(2)]);
    }

    #[test]
    fn non_candidates_are_never_picked() {
        let weights = [5, 5, 5, 5];
        for value in 0..10 {
            let picked = pick(&weights, 0b1010, 0, |n| {
                assert_eq!(n, 10);
                value
            });
            assert_eq!(picked, Some(if value < 5 { 1 } else { 3 }));
        }
    }

    #[test]
    fn single_remaining_channel_skips_the_draw() {
        assert_eq!(pick(&[7, 9], 0b11, 0b01, never), Some(1));
        assert_eq!(pick(&[7], 0b1, 0, never), Some(0));
    }

    #[test]
    fn exhausted_candidates_return_none() {
        assert_eq!(pick(&[1, 1], 0b11, 0b11, never), None);
        assert_eq!(pick(&[1, 1], 0, 0, never), None);
        assert_eq!(pick(&[], u64::MAX, 0, never), None);
    }

    #[test]
    fn bits_beyond_the_weights_are_ignored() {
        assert_eq!(pick(&[4, 4], u64::MAX, 0b01, never), Some(1));
    }

    #[test]
    fn zero_weight_channels_are_never_picked() {
        assert_eq!(pick(&[0, 2], 0b11, 0, never), Some(1));
        assert_eq!(pick(&[0, 0], 0b11, 0, never), None);
    }

    #[test]
    fn sixty_four_channels_use_the_whole_bitmap() {
        let weights = [1_u32; 64];
        assert_eq!(pick(&weights, u64::MAX, 0, |n| n - 1), Some(63));
        assert_eq!(pick(&weights, u64::MAX, u64::MAX >> 1, never), Some(63));
    }

    #[test]
    fn production_draw_stays_among_untried_candidates() {
        let weights = [1, 2, 3, 4];
        let mut seen = [0_u32; 4];
        for _ in 0..2000 {
            let picked = pick(&weights, 0b1101, 0b0001, |n| fastrand::u64(0..n))
                .expect("two candidates remain");
            seen[picked] += 1;
        }
        assert_eq!(seen[0], 0);
        assert_eq!(seen[1], 0);
        assert!(seen[2] > 0 && seen[3] > 0, "{seen:?}");
    }

    #[test]
    #[should_panic(expected = "outside 0..3")]
    fn draw_out_of_range_panics() {
        let _ = pick(&[1, 2], 0b11, 0, |n| n);
    }
}

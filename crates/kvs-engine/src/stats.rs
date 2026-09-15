use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct Stats {
    total: AtomicU64,
    dead: AtomicU64,
}

impl Stats {
    pub(crate) fn new(total_bytes: u64) -> Stats {
        Stats {
            total: AtomicU64::new(total_bytes),
            dead: AtomicU64::new(0),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    #[allow(dead_code)] // merge's snapshot reads dead_bytes -- task 6
    pub(crate) fn dead_bytes(&self) -> u64 {
        self.dead.load(Ordering::Relaxed)
    }

    /// A record was appended: `len` is its framed length, header included.
    pub(crate) fn record_append(&self, len: u64) {
        self.total.fetch_add(len, Ordering::Relaxed);
    }

    /// `len` bytes became unreachable.
    pub(crate) fn record_dead(&self, len: u64) {
        self.dead.fetch_add(len, Ordering::Relaxed);
    }

    /// A merge committed: it reclaimed `dead` counted dead bytes and shrank the
    /// log by `bytes_freed`. Both subtractions saturate.
    #[allow(dead_code)] // merge commit step, task 6
    pub(crate) fn reclaim(&self, dead: u64, bytes_freed: u64) {
        // Employ update to avoid TOCTOU race
        self.dead
            .update(Ordering::Relaxed, Ordering::Relaxed, |curr| {
                curr.saturating_sub(dead)
            });
        self.total
            .update(Ordering::Relaxed, Ordering::Relaxed, |curr| {
                curr.saturating_sub(bytes_freed)
            });
    }

    #[allow(dead_code)] // trigger in maybe_merge, task 8
    pub(crate) fn should_merge(&self, min_merge_bytes: u64, dead_ratio: f64) -> bool {
        let total = self.total_bytes();

        total >= min_merge_bytes && (self.dead_bytes() as f64 / total as f64) >= dead_ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_stats_is_all_zero() {
        let stats = Stats::default();
        assert_eq!(stats.total_bytes(), 0);
        assert_eq!(stats.dead_bytes(), 0);
    }

    #[test]
    fn appends_accumulate_into_total_bytes() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_append(50);
        assert_eq!(stats.total_bytes(), 150);
        assert_eq!(stats.dead_bytes(), 0);
    }

    #[test]
    fn should_merge_is_false_below_the_minimum_size() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_dead(100);
        assert!(
            !stats.should_merge(1024, 0.5),
            "100% dead but far too
  small"
        );
    }

    #[test]
    fn should_merge_is_false_below_the_ratio() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(400);
        assert!(!stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_is_true_once_both_conditions_hold() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(500);
        assert!(stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_divides_in_floating_point() {
        // Integer division would floor 600/1000 to 0 and never fire.
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(600);
        assert!(stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_is_false_on_an_empty_log_rather_than_dividing_by_zero() {
        let stats = Stats::default();
        assert!(!stats.should_merge(0, 0.0));
        assert_eq!(stats.total_bytes(), 0);
    }

    #[test]
    fn reclaiming_subtracts_from_both_counters() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(600);

        stats.reclaim(600, 700);
        assert_eq!(stats.dead_bytes(), 0);
        assert_eq!(stats.total_bytes(), 300);
    }

    #[test]
    fn reclaiming_saturates_rather_than_wrapping() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_dead(10);

        stats.reclaim(999, 999);
        assert_eq!(stats.dead_bytes(), 0);
        assert_eq!(stats.total_bytes(), 0);
    }
}

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::consts::consts::TransactionId;

/// Tracks the snapshots held by currently-open long-lived (interactive) transactions, so
/// vacuum knows the oldest snapshot it must preserve.
///
/// Only interactive transactions register here. One-shot reads and writes are processed
/// synchronously within a single worker call, so they are never in flight during a
/// stop-the-world vacuum and never need to register — which keeps this off the hot path.
/// It is consulted (and a vacuum runs) only while the world is paused, so a registered
/// snapshot is always observed by the vacuum that follows it.
pub struct ActiveSnapshots {
    /// snapshot value -> number of open transactions holding it.
    counts: Mutex<BTreeMap<usize, usize>>,
}

impl ActiveSnapshots {
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, snapshot: TransactionId) {
        *self.counts.lock().unwrap().entry(snapshot.0).or_insert(0) += 1;
    }

    pub fn unregister(&self, snapshot: TransactionId) {
        let mut counts = self.counts.lock().unwrap();
        if let Some(count) = counts.get_mut(&snapshot.0) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&snapshot.0);
            }
        }
    }

    /// The oldest snapshot held by an open transaction, or `None` if none are open.
    pub fn oldest(&self) -> Option<TransactionId> {
        self.counts
            .lock()
            .unwrap()
            .keys()
            .next()
            .map(|&value| TransactionId(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_has_no_oldest() {
        assert_eq!(ActiveSnapshots::new().oldest(), None);
    }

    #[test]
    fn reports_minimum_and_clears_on_unregister() {
        let snapshots = ActiveSnapshots::new();
        snapshots.register(TransactionId(10));
        snapshots.register(TransactionId(4));
        snapshots.register(TransactionId(4));

        assert_eq!(snapshots.oldest(), Some(TransactionId(4)));

        // One of the two holders of snapshot 4 leaves; 4 is still held.
        snapshots.unregister(TransactionId(4));
        assert_eq!(snapshots.oldest(), Some(TransactionId(4)));

        // The last holder of 4 leaves; the oldest is now 10.
        snapshots.unregister(TransactionId(4));
        assert_eq!(snapshots.oldest(), Some(TransactionId(10)));

        snapshots.unregister(TransactionId(10));
        assert_eq!(snapshots.oldest(), None);
    }
}

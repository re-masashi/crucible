use ahash::RandomState;
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::term::InternedStr;

pub struct StringInterner {
    map: DashMap<String, u64, RandomState>,
    reverse: DashMap<u64, String, RandomState>,
    next_id: AtomicUsize,
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl StringInterner {
    pub fn new() -> Self {
        Self {
            map: DashMap::with_hasher(RandomState::new()),
            reverse: DashMap::with_hasher(RandomState::new()),
            next_id: AtomicUsize::new(1),
        }
    }

    #[inline(always)]
    pub fn intern(&self, s: &str) -> InternedStr {
        if let Some(entry) = self.map.get(s) {
            return InternedStr(*entry);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.insert(s.to_string(), id as u64);
        self.reverse.insert(id as u64, s.to_string());
        InternedStr(id as u64)
    }

    #[inline(always)]
    pub fn get(&self, s: &str) -> Option<InternedStr> {
        self.map.get(s).map(|entry| InternedStr(*entry))
    }

    #[inline]
    pub fn resolve(&self, id: InternedStr) -> String {
        self.reverse
            .get(&id.0)
            .map(|entry| entry.clone())
            .unwrap_or_else(|| "???".to_string())
    }
}

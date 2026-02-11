use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{ArenaError, StoreError};
use crate::interner::StringInterner;
use crate::shard::Shard;
use crate::term::{ArenaPtr, InternedStr, StorableValue, Term, TermWithTTL};

pub struct Store {
    pub name: Option<String>,
    pub shards: Box<[Shard]>,
    shard_mask: usize,
    interner: StringInterner,
    cleanup_enabled: AtomicBool,
}

impl Store {
    pub fn new_default(name: Option<String>) -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);

        let shard_count = (cpu_count * 4).next_power_of_two();
        Self::new(name, shard_count)
    }

    pub fn new(name: Option<String>, shard_count: usize) -> Self {
        assert!(shard_count > 0);
        assert!(shard_count.is_power_of_two());

        let shards: Box<[Shard]> = (0..shard_count).map(Shard::new).collect();

        Self {
            name,
            shards,
            shard_mask: shard_count - 1,
            interner: StringInterner::new(),
            cleanup_enabled: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub fn hash_key(&self, key: &str) -> u64 {
        xxh3_64(key.as_bytes())
    }

    #[inline(always)]
    pub fn get_shard_index(&self, hash: u64) -> usize {
        (hash as usize) & self.shard_mask
    }

    #[inline(always)]
    pub fn insert(&self, key: &str, value: Term) -> Result<(), StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::new(value);
        self.shards[shard_idx].map.insert(Arc::from(key), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn insert_owned(&self, key: String, value: Term) -> Result<(), StoreError> {
        let hash = self.hash_key(&key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::new(value);
        self.shards[shard_idx].map.insert(key.into(), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn insert_with_ttl(
        &self,
        key: &str,
        value: Term,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::with_ttl_seconds(value, ttl_seconds);
        self.shards[shard_idx].map.insert(Arc::from(key), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn insert_with_ttl_owned(
        &self,
        key: String,
        value: Term,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let hash = self.hash_key(&key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::with_ttl_seconds(value, ttl_seconds);
        self.shards[shard_idx].map.insert(key.into(), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn insert_with_ttl_millis(
        &self,
        key: &str,
        value: Term,
        ttl_millis: u64,
    ) -> Result<(), StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::with_ttl_millis(value, ttl_millis);
        self.shards[shard_idx].map.insert(Arc::from(key), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn insert_with_ttl_millis_owned(
        &self,
        key: String,
        value: Term,
        ttl_millis: u64,
    ) -> Result<(), StoreError> {
        let hash = self.hash_key(&key);
        let shard_idx = self.get_shard_index(hash);

        let entry = TermWithTTL::with_ttl_millis(value, ttl_millis);
        self.shards[shard_idx].map.insert(key.into(), entry);
        Ok(())
    }

    #[inline(always)]
    pub fn get(&self, key: &str) -> Result<Term, StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        match self.shards[shard_idx].map.get(key) {
            Some(entry) => {
                let term_with_ttl = *entry;

                if term_with_ttl.expires_at_ms == 0 {
                    return Ok(term_with_ttl.value);
                }

                if term_with_ttl.is_expired() {
                    drop(entry);
                    self.free_term_data(term_with_ttl.value);
                    self.shards[shard_idx].map.remove(key);
                    return Err(StoreError::KeyNotFound);
                }

                Ok(term_with_ttl.value)
            }
            None => Err(StoreError::KeyNotFound),
        }
    }

    #[inline]
    pub fn remove(&self, key: &str) -> Result<Term, StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        match self.shards[shard_idx].map.remove(key) {
            Some((_, term_with_ttl)) => {
                if term_with_ttl.is_expired() {
                    return Err(StoreError::KeyNotFound);
                }
                self.free_term_data(term_with_ttl.value);
                Ok(term_with_ttl.value)
            }
            None => Err(StoreError::KeyNotFound),
        }
    }

    #[inline(always)]
    pub fn update(&self, key: &str, value: Term) -> Result<Term, StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        let old_ttl_ms = self.shards[shard_idx]
            .map
            .get(key)
            .map(|entry| entry.expires_at_ms)
            .unwrap_or(0);

        match self.shards[shard_idx].map.insert(
            Arc::from(key),
            TermWithTTL {
                value,
                expires_at_ms: old_ttl_ms,
            },
        ) {
            Some(old) => {
                if old.is_expired() {
                    return Err(StoreError::KeyNotFound);
                }
                Ok(old.value)
            }
            None => Err(StoreError::KeyNotFound),
        }
    }

    #[inline(always)]
    pub fn update_owned(&self, key: String, value: Term) -> Result<Term, StoreError> {
        let hash = self.hash_key(&key);
        let shard_idx = self.get_shard_index(hash);

        let old_ttl_ms = self.shards[shard_idx]
            .map
            .get(key.as_str())
            .map(|entry| entry.expires_at_ms)
            .unwrap_or(0);

        match self.shards[shard_idx].map.insert(
            key.into(),
            TermWithTTL {
                value,
                expires_at_ms: old_ttl_ms,
            },
        ) {
            Some(old) => {
                if old.is_expired() {
                    return Err(StoreError::KeyNotFound);
                }
                Ok(old.value)
            }
            None => Err(StoreError::KeyNotFound),
        }
    }

    #[inline(always)]
    pub fn insert_typed<T: StorableValue>(&self, key: &str, value: T) -> Result<(), StoreError> {
        self.insert(key, value.to_term())
    }

    #[inline(always)]
    pub fn insert_typed_owned<T: StorableValue>(
        &self,
        key: String,
        value: T,
    ) -> Result<(), StoreError> {
        self.insert_owned(key, value.to_term())
    }

    #[inline(always)]
    pub fn get_typed<T: StorableValue>(&self, key: &str) -> Result<T, StoreError> {
        let term = self.get(key)?;
        T::from_term(&term)
    }

    #[inline(always)]
    pub fn update_typed<T: StorableValue>(&self, key: &str, value: T) -> Result<T, StoreError> {
        let old_term = self.update(key, value.to_term())?;
        T::from_term(&old_term)
    }

    #[inline(always)]
    pub fn update_typed_owned<T: StorableValue>(
        &self,
        key: String,
        value: T,
    ) -> Result<T, StoreError> {
        let old_term = self.update_owned(key, value.to_term())?;
        T::from_term(&old_term)
    }

    #[inline(always)]
    pub fn insert_typed_with_ttl<T: StorableValue>(
        &self,
        key: &str,
        value: T,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        self.insert_with_ttl(key, value.to_term(), ttl_seconds)
    }

    #[inline(always)]
    pub fn insert_typed_with_ttl_owned<T: StorableValue>(
        &self,
        key: String,
        value: T,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        self.insert_with_ttl_owned(key, value.to_term(), ttl_seconds)
    }

    #[inline(always)]
    pub fn insert_typed_with_ttl_millis<T: StorableValue>(
        &self,
        key: &str,
        value: T,
        ttl_millis: u64,
    ) -> Result<(), StoreError> {
        self.insert_with_ttl_millis(key, value.to_term(), ttl_millis)
    }

    #[inline(always)]
    pub fn insert_typed_with_ttl_millis_owned<T: StorableValue>(
        &self,
        key: String,
        value: T,
        ttl_millis: u64,
    ) -> Result<(), StoreError> {
        self.insert_with_ttl_millis_owned(key, value.to_term(), ttl_millis)
    }

    #[inline(always)]
    pub fn remove_typed<T: StorableValue>(&self, key: &str) -> Result<T, StoreError> {
        let term = self.remove(key)?;
        T::from_term(&term)
    }

    #[inline(always)]
    pub fn insert_string(&self, key: &str, value: &str) -> Result<(), StoreError> {
        let interned = self.intern(value);
        self.insert(key, Term::String(interned))
    }

    #[inline(always)]
    pub fn insert_string_owned(&self, key: String, value: &str) -> Result<(), StoreError> {
        let interned = self.intern(value);
        self.insert_owned(key, Term::String(interned))
    }

    #[inline(always)]
    pub fn get_string(&self, key: &str) -> Result<String, StoreError> {
        let term = self.get(key)?;
        match term {
            Term::String(interned) => Ok(self.resolve(interned)),
            _ => Err(StoreError::TypeError),
        }
    }

    #[inline(always)]
    pub fn insert_string_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let interned = self.intern(value);
        self.insert_with_ttl(key, Term::String(interned), ttl_seconds)
    }

    #[inline(always)]
    pub fn insert_string_with_ttl_millis(
        &self,
        key: &str,
        value: &str,
        ttl_millis: u64,
    ) -> Result<(), StoreError> {
        let interned = self.intern(value);
        self.insert_with_ttl_millis(key, Term::String(interned), ttl_millis)
    }

    #[inline(always)]
    pub fn update_string(&self, key: &str, value: &str) -> Result<String, StoreError> {
        let interned = self.intern(value);
        let old_term = self.update(key, Term::String(interned))?;
        match old_term {
            Term::String(old_interned) => Ok(self.resolve(old_interned)),
            _ => Err(StoreError::TypeError),
        }
    }

    #[inline(always)]
    pub fn update_string_owned(&self, key: String, value: &str) -> Result<String, StoreError> {
        let interned = self.intern(value);
        let old_term = self.update_owned(key, Term::String(interned))?;
        match old_term {
            Term::String(old_interned) => Ok(self.resolve(old_interned)),
            _ => Err(StoreError::TypeError),
        }
    }

    #[inline(always)]
    pub fn remove_string(&self, key: &str) -> Result<String, StoreError> {
        let term = self.remove(key)?;
        match term {
            Term::String(interned) => Ok(self.resolve(interned)),
            _ => Err(StoreError::TypeError),
        }
    }

    #[inline(always)]
    pub fn insert_bytes(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        let ptr = self.alloc_slice(key, value)?;
        self.insert(key, Term::Array(ptr))
    }

    #[inline(always)]
    pub fn insert_bytes_owned(&self, key: String, value: &[u8]) -> Result<(), StoreError> {
        let ptr = self.alloc_slice(&key, value)?;
        self.insert_owned(key, Term::Array(ptr))
    }

    #[inline(always)]
    pub fn get_bytes(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let term = self.get(key)?;
        match term {
            Term::Array(ptr) => unsafe {
                let hash = self.hash_key(key);
                let shard_idx = self.get_shard_index(hash);
                let slice = self.shards[shard_idx].arena.get_slice::<u8>(ptr)?;
                Ok(slice.to_vec())
            },
            _ => Err(StoreError::TypeError),
        }
    }

    #[inline(always)]
    pub fn insert_bytes_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let ptr = self.alloc_slice(key, value)?;
        self.insert_with_ttl(key, Term::Array(ptr), ttl_seconds)
    }

    #[inline(always)]
    pub fn insert_bytes_with_ttl_owned(
        &self,
        key: String,
        value: &[u8],
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let ptr = self.alloc_slice(&key, value)?;
        self.insert_with_ttl_owned(key, Term::Array(ptr), ttl_seconds)
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &str) -> bool {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        match self.shards[shard_idx].map.get(key) {
            Some(entry) => {
                let is_expired = entry.is_expired();
                if is_expired {
                    drop(entry);
                    self.shards[shard_idx].map.remove(key);
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    pub fn ttl_millis(&self, key: &str) -> Option<u64> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        self.shards[shard_idx].map.get(key).and_then(|entry| {
            let term_with_ttl = *entry;

            if term_with_ttl.is_expired() {
                drop(entry);
                self.shards[shard_idx].map.remove(key);
                return None;
            }

            term_with_ttl.remaining_ttl_millis()
        })
    }

    pub fn ttl(&self, key: &str) -> Option<u64> {
        self.ttl_millis(key).map(|ms| ms / 1000)
    }

    pub fn expire(&self, key: &str, ttl_seconds: u64) -> Result<(), StoreError> {
        self.expire_millis(key, ttl_seconds * 1000)
    }

    pub fn expire_millis(&self, key: &str, ttl_millis: u64) -> Result<(), StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.shards[shard_idx].map.alter(key, |_, mut entry| {
            entry.expires_at_ms = now_ms + ttl_millis;
            entry
        });

        Ok(())
    }

    pub fn persist(&self, key: &str) -> Result<(), StoreError> {
        let hash = self.hash_key(key);
        let shard_idx = self.get_shard_index(hash);

        self.shards[shard_idx].map.alter(key, |_, mut entry| {
            entry.expires_at_ms = 0;
            entry
        });

        Ok(())
    }

    pub fn check_expired_count(&self) -> (usize, usize) {
        let mut expired = 0;
        let mut not_expired = 0;

        for shard in self.shards.iter() {
            for entry in shard.map.iter() {
                if entry.value().is_expired() {
                    expired += 1;
                } else {
                    not_expired += 1;
                }
            }
        }

        (expired, not_expired)
    }

    pub fn cleanup_expired(&self) -> usize {
        let removed = AtomicUsize::new(0);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.shards.par_iter().for_each(|shard| {
            let keys_to_remove: Vec<Arc<str>> = shard
                .map
                .iter()
                .filter(|entry| {
                    let expires_at = entry.value().expires_at_ms;
                    expires_at != 0 && now_ms >= expires_at
                })
                .map(|entry| entry.key().clone())
                .collect();

            for key in keys_to_remove {
                if let Some((_, term_with_ttl)) = shard.map.remove(&*key) {
                    self.free_term_data(term_with_ttl.value);
                    removed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        removed.load(Ordering::Relaxed)
    }

    pub fn start_background_cleanup(
        self: std::sync::Arc<Self>,
        interval_secs: u64,
    ) -> thread::JoinHandle<()> {
        self.cleanup_enabled.store(true, Ordering::Release);

        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(interval_secs));

            if !self.cleanup_enabled.load(Ordering::Relaxed) {
                break;
            }

            let removed = self.cleanup_expired();

            if removed > 0 {
                eprintln!(
                    "[Crucible] Background cleanup removed {} expired keys",
                    removed
                );
            }
        })
    }

    pub fn stop_background_cleanup(&self) {
        self.cleanup_enabled.store(false, Ordering::Release);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.map.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            for entry in shard.map.iter() {
                self.free_term_data(entry.value().value);
            }
            shard.map.clear();
        }
    }

    pub fn arena_stats(&self) -> Vec<super::arena::ArenaStats> {
        let mut stats = Vec::new();
        for shard in self.shards.iter() {
            stats.extend(shard.arena.stats());
        }
        stats
    }

    pub fn total_arena_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.arena.total_allocated_bytes())
            .sum()
    }

    pub fn compact_arenas(&self) -> usize {
        self.shards.iter().map(|s| s.arena.compact()).sum()
    }

    pub fn start_background_compaction(
        self: std::sync::Arc<Self>,
        interval_secs: u64,
        utilization_threshold: f64,
    ) -> thread::JoinHandle<()> {
        for shard in self.shards.iter() {
            shard
                .arena
                .start_compaction(interval_secs, utilization_threshold);
        }
        thread::spawn(|| ())
    }

    pub fn alloc_slice<T: Copy>(
        &self,
        shard_key: &str,
        data: &[T],
    ) -> Result<ArenaPtr, ArenaError> {
        let hash = self.hash_key(shard_key);
        let shard_idx = self.get_shard_index(hash);
        self.shards[shard_idx].arena.alloc_slice(data)
    }

    #[inline(always)]
    pub fn intern(&self, s: &str) -> InternedStr {
        self.interner.intern(s)
    }

    pub fn resolve(&self, id: InternedStr) -> String {
        self.interner.resolve(id)
    }

    fn free_term_data(&self, term: Term) {
        match term {
            Term::Array(ptr) => {
                if ptr.ptr != 0 {
                    let hash = ptr.arena_id as u64;
                    let shard_idx = self.get_shard_index(hash);
                    self.shards[shard_idx].arena.dealloc(ptr.arena_id);
                }
            }
            Term::Map(ptr) => {
                if ptr.ptr != 0 {
                    let hash = ptr.arena_id as u64;
                    let shard_idx = self.get_shard_index(hash);
                    self.shards[shard_idx].arena.dealloc(ptr.arena_id);
                }
            }
            Term::Enum(_, ptr) => {
                if ptr.ptr != 0 {
                    let hash = ptr.arena_id as u64;
                    let shard_idx = self.get_shard_index(hash);
                    self.shards[shard_idx].arena.dealloc(ptr.arena_id);
                }
            }
            _ => {}
        }
    }
}

use std::alloc::{Layout, alloc};
use std::hash::Hash;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_64;

#[cfg(test)]
pub mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InternedStr(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArenaPtr {
    pub ptr: u64,
    pub len: usize,
    pub arena_id: usize,
}

impl ArenaPtr {
    pub const NULL: Self = ArenaPtr {
        ptr: 0,
        len: 0,
        arena_id: 0,
    };
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Term {
    Int(i64),
    Float(f64),
    Bool(bool),
    Unit,
    Any,
    String(InternedStr),
    Array(ArenaPtr),
    Map(ArenaPtr),
    Enum(u32, ArenaPtr),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TermWithTTL {
    pub value: Term,
    pub expires_at_ms: u64,
}

impl TermWithTTL {
    #[inline(always)]
    pub fn new(value: Term) -> Self {
        Self {
            value,
            expires_at_ms: 0,
        }
    }

    #[inline(always)]
    pub fn with_ttl_seconds(value: Term, ttl_seconds: u64) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            value,
            expires_at_ms: now_ms + (ttl_seconds * 1000),
        }
    }

    #[inline(always)]
    pub fn with_ttl_millis(value: Term, ttl_millis: u64) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            value,
            expires_at_ms: now_ms + ttl_millis,
        }
    }

    #[inline(always)]
    pub fn is_expired(&self) -> bool {
        if self.expires_at_ms == 0 {
            return false;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        now_ms >= self.expires_at_ms
    }

    #[inline(always)]
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        if self.expires_at_ms == 0 {
            return false;
        }
        now_ms >= self.expires_at_ms
    }

    #[inline(always)]
    pub fn remaining_ttl_millis(&self) -> Option<u64> {
        if self.expires_at_ms == 0 {
            return Some(0);
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if now_ms >= self.expires_at_ms {
            None
        } else {
            Some(self.expires_at_ms - now_ms)
        }
    }
}

pub trait StorableValue: Sized {
    fn to_term(&self) -> Term;

    fn from_term(term: &Term) -> Result<Self, StoreError>;
}

impl StorableValue for i64 {
    #[inline(always)]
    fn to_term(&self) -> Term {
        Term::Int(*self)
    }

    #[inline(always)]
    fn from_term(term: &Term) -> Result<Self, StoreError> {
        match term {
            Term::Int(n) => Ok(*n),
            _ => Err(StoreError::TypeError),
        }
    }
}

impl StorableValue for f64 {
    #[inline(always)]
    fn to_term(&self) -> Term {
        Term::Float(*self)
    }

    #[inline(always)]
    fn from_term(term: &Term) -> Result<Self, StoreError> {
        match term {
            Term::Float(f) => Ok(*f),
            _ => Err(StoreError::TypeError),
        }
    }
}

impl StorableValue for bool {
    #[inline(always)]
    fn to_term(&self) -> Term {
        Term::Bool(*self)
    }

    #[inline(always)]
    fn from_term(term: &Term) -> Result<Self, StoreError> {
        match term {
            Term::Bool(b) => Ok(*b),
            _ => Err(StoreError::TypeError),
        }
    }
}

impl StorableValue for () {
    #[inline(always)]
    fn to_term(&self) -> Term {
        Term::Unit
    }

    #[inline(always)]
    fn from_term(term: &Term) -> Result<Self, StoreError> {
        match term {
            Term::Unit => Ok(()),
            _ => Err(StoreError::TypeError),
        }
    }
}

#[derive(Debug)]
pub enum ArenaError {
    OutOfMemory,
    AllocationTooLarge,
    InvalidArenaId,
}

#[derive(Debug)]
pub enum StoreError {
    KeyNotFound,
    TypeError,
    ArenaError(ArenaError),
}

impl From<ArenaError> for StoreError {
    fn from(e: ArenaError) -> Self {
        StoreError::ArenaError(e)
    }
}

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

pub struct Arena {
    buffer: NonNull<u8>,
    capacity: usize,
    offset: AtomicUsize,
    id: usize,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(capacity: usize, id: usize) -> Self {
        unsafe {
            let layout = Layout::from_size_align_unchecked(capacity, 8);
            let buffer = alloc(layout);
            Self {
                buffer: NonNull::new(buffer).expect("allocation failed"),
                capacity,
                offset: AtomicUsize::new(0),
                id,
            }
        }
    }

    #[inline]
    pub fn try_alloc_slice<T: Copy>(&self, data: &[T]) -> Result<ArenaPtr, ArenaError> {
        let size = std::mem::size_of_val(data);
        let align = std::mem::align_of::<T>();

        let mut current_offset = self.offset.load(Ordering::Acquire);

        loop {
            let padding = (align - (current_offset % align)) % align;
            let start = current_offset + padding;

            if start + size > self.capacity {
                return Err(ArenaError::OutOfMemory);
            }

            match self.offset.compare_exchange_weak(
                current_offset,
                start + size,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    let ptr = self.buffer.as_ptr().add(start) as *mut T;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());

                    return Ok(ArenaPtr {
                        ptr: ptr as u64,
                        len: data.len(),
                        arena_id: self.id,
                    });
                },
                Err(actual) => {
                    current_offset = actual;
                }
            }
        }
    }

    /// # Safety
    /// uhm. i don't know for sure, but it's probably not safe.
    #[inline(always)]
    pub unsafe fn get_slice<T>(&self, ptr: ArenaPtr) -> Result<&[T], ArenaError> {
        if ptr.arena_id != self.id {
            return Err(ArenaError::InvalidArenaId);
        }

        if ptr.ptr == 0 {
            return Ok(&[]);
        }

        let buffer_start = self.buffer.as_ptr() as u64;
        let buffer_end = buffer_start + self.capacity as u64;

        if ptr.ptr < buffer_start || ptr.ptr >= buffer_end {
            return Err(ArenaError::InvalidArenaId);
        }

        Ok(unsafe { std::slice::from_raw_parts(ptr.ptr as *const T, ptr.len) })
    }
}

pub struct ShardArena {
    arenas: RwLock<Vec<Arena>>,
    default_arena_size: usize,
    next_arena_id: AtomicUsize,
}

impl ShardArena {
    pub fn new(default_arena_size: usize, start_id: usize) -> Self {
        Self {
            arenas: RwLock::new(vec![Arena::new(default_arena_size, start_id)]),
            default_arena_size,
            next_arena_id: AtomicUsize::new(start_id + 1),
        }
    }

    #[inline]
    pub fn alloc_slice<T: Copy>(&self, data: &[T]) -> Result<ArenaPtr, ArenaError> {
        let required_size = std::mem::size_of_val(data);

        const MAX_SINGLE_ALLOCATION: usize = 100 * 1024 * 1024;
        if required_size > MAX_SINGLE_ALLOCATION {
            return Err(ArenaError::AllocationTooLarge);
        }

        {
            let arenas = self.arenas.read();
            for arena in arenas.iter() {
                if let Ok(ptr) = arena.try_alloc_slice(data) {
                    return Ok(ptr);
                }
            }
        }

        let new_id = self.next_arena_id.fetch_add(1, Ordering::Relaxed);
        let arena_size = if required_size > self.default_arena_size {
            required_size.next_power_of_two()
        } else {
            self.default_arena_size
        };

        let new_arena = Arena::new(arena_size, new_id);
        let ptr = new_arena.try_alloc_slice(data)?;

        self.arenas.write().push(new_arena);
        Ok(ptr)
    }

    /// # Safety
    /// prolly safe
    #[inline(always)]
    pub unsafe fn get_slice<T>(&self, ptr: ArenaPtr) -> Result<&[T], ArenaError> {
        if ptr.ptr == 0 {
            return Ok(&[]);
        }

        let arenas = self.arenas.read();

        for arena in arenas.iter() {
            if arena.id == ptr.arena_id {
                let buffer_start = arena.buffer.as_ptr() as u64;
                let buffer_end = buffer_start + arena.capacity as u64;

                if ptr.ptr < buffer_start || ptr.ptr >= buffer_end {
                    return Err(ArenaError::InvalidArenaId);
                }

                drop(arenas);

                return Ok(unsafe { std::slice::from_raw_parts(ptr.ptr as *const T, ptr.len) });
            }
        }

        Err(ArenaError::InvalidArenaId)
    }
}

pub struct Shard {
    pub map: DashMap<String, TermWithTTL, RandomState>,
    arena: ShardArena,
}

impl Shard {
    fn new(shard_id: usize) -> Self {
        Self {
            map: DashMap::with_capacity_and_hasher(2048, RandomState::new()),
            arena: ShardArena::new(2 * 1024 * 1024, shard_id * 10000),
        }
    }
}

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
        self.shards[shard_idx].map.insert(key.to_string(), entry);
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
        self.shards[shard_idx].map.insert(key.to_string(), entry);
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
        self.shards[shard_idx].map.insert(key.to_string(), entry);
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
            key.to_string(),
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
    pub fn insert_typed_with_ttl<T: StorableValue>(
        &self,
        key: &str,
        value: T,
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        self.insert_with_ttl(key, value.to_term(), ttl_seconds)
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
            shard.map.retain(|_, v| {
                let expires_at = v.expires_at_ms;
                if expires_at != 0 && now_ms >= expires_at {
                    removed.fetch_add(1, Ordering::Relaxed);
                    false
                } else {
                    true
                }
            });
        });

        removed.load(Ordering::Relaxed)
    }

    pub fn start_background_cleanup(self: Arc<Self>, interval_secs: u64) -> thread::JoinHandle<()> {
        self.cleanup_enabled.store(true, Ordering::Release);

        thread::spawn(move || {
            loop {
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
            shard.map.clear();
        }
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
}

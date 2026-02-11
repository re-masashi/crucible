use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use parking_lot::RwLock;

use super::error::ArenaError;
use super::term::ArenaPtr;

pub struct Arena {
    buffer: NonNull<u8>,
    capacity: usize,
    offset: AtomicUsize,
    id: usize,
    pub(crate) live_allocs: AtomicUsize,
    pub(crate) used_bytes: AtomicUsize,
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
                live_allocs: AtomicUsize::new(0),
                used_bytes: AtomicUsize::new(0),
            }
        }
    }

    pub fn allocated_bytes(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn utilization(&self) -> f64 {
        let used = self.allocated_bytes() as f64;
        let cap = self.capacity as f64;
        if cap == 0.0 {
            0.0
        } else {
            used / cap
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live_allocs.load(Ordering::Relaxed) == 0
    }

    pub fn free(&self) {
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.capacity, 8);
            dealloc(self.buffer.as_ptr(), layout);
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

                    self.live_allocs.fetch_add(1, Ordering::Relaxed);
                    self.used_bytes.fetch_add(size, Ordering::Relaxed);

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

    pub(crate) fn dealloc(&self) {
        self.live_allocs.fetch_sub(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub(crate) fn copy_to(&self, arena_ptr: ArenaPtr) -> Option<(Vec<u8>, usize)> {
        unsafe {
            let buffer_start = self.buffer.as_ptr() as u64;
            let offset = (arena_ptr.ptr - buffer_start) as usize;
            let size = arena_ptr.len * std::mem::size_of::<u8>();
            let data = std::slice::from_raw_parts(self.buffer.as_ptr().add(offset), size).to_vec();
            Some((data, size))
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

#[derive(Clone)]
pub struct ArenaStats {
    pub id: usize,
    pub capacity: usize,
    pub used_bytes: usize,
    pub live_allocs: usize,
    pub utilization: f64,
}

pub struct ShardArena {
    arenas: RwLock<Vec<Arena>>,
    default_arena_size: usize,
    next_arena_id: AtomicUsize,
    compaction_enabled: AtomicBool,
    compaction_thread: RwLock<Option<thread::JoinHandle<()>>>,
}

impl Clone for ShardArena {
    fn clone(&self) -> Self {
        Self {
            arenas: RwLock::new(vec![]),
            default_arena_size: self.default_arena_size,
            next_arena_id: AtomicUsize::new(self.next_arena_id.load(Ordering::Relaxed)),
            compaction_enabled: AtomicBool::new(false),
            compaction_thread: RwLock::new(None),
        }
    }
}

impl ShardArena {
    pub fn new(default_arena_size: usize, start_id: usize) -> Self {
        Self {
            arenas: RwLock::new(vec![Arena::new(default_arena_size, start_id)]),
            default_arena_size,
            next_arena_id: AtomicUsize::new(start_id + 1),
            compaction_enabled: AtomicBool::new(false),
            compaction_thread: RwLock::new(None),
        }
    }

    pub fn default_arena_size(&self) -> usize {
        self.default_arena_size
    }

    pub fn next_arena_id(&self) -> usize {
        self.next_arena_id.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> Vec<ArenaStats> {
        let arenas = self.arenas.read();
        arenas
            .iter()
            .map(|a| ArenaStats {
                id: a.id,
                capacity: a.capacity(),
                used_bytes: a.allocated_bytes(),
                live_allocs: a.live_allocs.load(Ordering::Relaxed),
                utilization: a.utilization(),
            })
            .collect()
    }

    pub fn total_allocated_bytes(&self) -> usize {
        let arenas = self.arenas.read();
        arenas.iter().map(|a| a.allocated_bytes()).sum()
    }

    pub fn total_live_allocs(&self) -> usize {
        let arenas = self.arenas.read();
        arenas
            .iter()
            .map(|a| a.live_allocs.load(Ordering::Relaxed))
            .sum()
    }

    pub fn arena_count(&self) -> usize {
        self.arenas.read().len()
    }

    pub fn start_compaction(&self, interval_secs: u64, utilization_threshold: f64) {
        if self.compaction_enabled.load(Ordering::Relaxed) {
            return;
        }
        self.compaction_enabled.store(true, Ordering::Release);

        let this = std::sync::Arc::new(self.clone());
        let handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(interval_secs));

            if !this.compaction_enabled.load(Ordering::Relaxed) {
                break;
            }

            this.compact_arena(utilization_threshold);
        });

        *self.compaction_thread.write() = Some(handle);
    }

    pub fn stop_compaction(&self) {
        self.compaction_enabled.store(false, Ordering::Release);
        if let Some(handle) = self.compaction_thread.write().take() {
            let _ = handle.join();
        }
    }

    fn compact_arena(&self, threshold: f64) {
        let mut arenas = self.arenas.write();

        for i in 0..arenas.len() {
            let arena = &arenas[i];
            if arena.utilization() < threshold && !arena.is_empty() {
                continue;
            }
            if arena.utilization() > threshold {
                continue;
            }

            let arena_id = arena.id;
            let arena_capacity = arena.capacity();
            drop(arenas);

            if self.compact_single_arena(arena_id, arena_capacity) {
                return;
            }

            arenas = self.arenas.write();
        }
    }

    fn compact_single_arena(&self, target_id: usize, _target_capacity: usize) -> bool {
        let arena_ptrs: Vec<ArenaPtr> = {
            let arenas = self.arenas.read();
            let target = arenas.iter().find(|a| a.id == target_id);
            match target {
                Some(arena) => {
                    let mut ptrs = Vec::new();
                    for _ in 0..arena.live_allocs.load(Ordering::Relaxed) {
                        ptrs.push(ArenaPtr::NULL);
                    }
                    ptrs
                }
                None => Vec::new(),
            }
        };

        if arena_ptrs.is_empty() {
            return false;
        }

        let new_id = self.next_arena_id.fetch_add(1, Ordering::Relaxed);
        let new_arena = Arena::new(_target_capacity.max(1024 * 1024), new_id);

        let mut arenas = self.arenas.write();

        for (old_arena_id, old_arena) in arenas.iter().enumerate() {
            if old_arena.id != target_id {
                continue;
            }

            for _ in 0..100 {
                if old_arena.live_allocs.load(Ordering::Relaxed) == 0 {
                    break;
                }
            }

            if old_arena.live_allocs.load(Ordering::Relaxed) == 0 {
                // drop(new_arena);
                let mut new_arenas: Vec<Arena> =
                    arenas.drain(old_arena_id..old_arena_id + 1).collect();
                for a in &mut new_arenas {
                    a.free();
                }
                return true;
            }
            break;
        }

        arenas.push(new_arena);
        true
    }

    pub fn compact(&self) -> usize {
        let mut removed = 0;
        let mut arenas = self.arenas.write();

        let mut to_remove = Vec::new();
        for (i, arena) in arenas.iter().enumerate() {
            if arena.is_empty() && arenas.len() > 1 {
                to_remove.push(i);
            }
        }

        for idx in to_remove.into_iter().rev() {
            arenas[idx].free();
            arenas.remove(idx);
            removed += 1;
        }

        removed
    }

    #[inline]
    pub fn alloc_slice<T: Copy>(&self, data: &[T]) -> Result<ArenaPtr, ArenaError> {
        let required_size = std::mem::size_of_val(data);

        const MAX_SINGLE_ALLOCATION: usize = 64 * 1024 * 1024; // 64 MB
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

    pub fn dealloc(&self, arena_id: usize) {
        let arenas = self.arenas.read();
        for arena in arenas.iter() {
            if arena.id == arena_id {
                arena.dealloc();
                return;
            }
        }
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

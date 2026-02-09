use std::alloc::{Layout, alloc};
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;

use parking_lot::RwLock;

use super::error::ArenaError;
use super::term::ArenaPtr;

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

use std::sync::atomic::Ordering;

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

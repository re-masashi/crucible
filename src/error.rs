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

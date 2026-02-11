use std::sync::Arc;

use ahash::RandomState;
use dashmap::DashMap;

use crate::arena::ShardArena;
use crate::term::TermWithTTL;

pub struct Shard {
    pub map: DashMap<Arc<str>, TermWithTTL, RandomState>,
    pub arena: ShardArena,
}

impl Shard {
    pub fn new(shard_id: usize) -> Self {
        Self {
            map: DashMap::with_capacity_and_hasher(2048, RandomState::new()),
            arena: ShardArena::new(1024 * 1024, shard_id * 10000),
        }
    }
}

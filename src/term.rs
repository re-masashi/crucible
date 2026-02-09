use crate::error::StoreError;
use std::time::{SystemTime, UNIX_EPOCH};

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

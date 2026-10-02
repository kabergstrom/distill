//! RpcIO's bound on the fetched payload bytes it holds in memory.
//!
//! A fetch learns its size when the daemon answers; it then reserves those
//! bytes against one aggregate budget. A payload that does not fit (the
//! budget is spent, or the payload is over the spool threshold on its own)
//! streams to a mapped temporary spool file instead. Admission therefore
//! never waits: nothing the IO does is gated on the engine draining it.
//! A reservation lasts until the loader takes the payload from
//! `LoaderIO::poll`, or until the fetch is dropped.

use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Memory,
    Spool,
}

#[derive(Debug)]
pub struct FetchAdmission {
    memory_budget: usize,
    spool_threshold: usize,
    resident: Cell<usize>,
}

impl FetchAdmission {
    pub fn new(memory_budget: usize, spool_threshold: usize) -> Self {
        Self {
            memory_budget,
            spool_threshold,
            resident: Cell::new(0),
        }
    }

    /// Reserve `bytes` in memory when they fit, else choose the spool. A
    /// `Memory` answer holds the bytes until [`Self::release`].
    pub fn admit(&self, bytes: usize) -> Admission {
        if self.try_reserve(bytes) {
            Admission::Memory
        } else {
            Admission::Spool
        }
    }

    /// Grow a payload of `held` bytes, already reserved, by `bytes`; false
    /// (nothing reserved) when the grown payload no longer belongs in memory.
    pub fn grow(&self, held: usize, bytes: usize) -> bool {
        match held.checked_add(bytes) {
            Some(total) if total <= self.spool_threshold => self.try_reserve(bytes),
            _ => false,
        }
    }

    pub fn release(&self, bytes: usize) {
        let resident = self.resident.get();
        debug_assert!(bytes <= resident, "released more than reserved");
        self.resident.set(resident.saturating_sub(bytes));
    }

    /// Bytes currently reserved in memory.
    pub fn resident(&self) -> usize {
        self.resident.get()
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        if bytes > self.spool_threshold {
            return false;
        }
        match self.resident.get().checked_add(bytes) {
            Some(total) if total <= self.memory_budget => {
                self.resident.set(total);
                true
            }
            _ => false,
        }
    }
}

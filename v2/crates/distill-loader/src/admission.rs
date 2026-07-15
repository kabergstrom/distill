//! Single-flight fetch storage policy.
//!
//! RpcIO owns the actual one-permit lifetime gate. With at most one fetched
//! payload resident between the IO and engine threads, aggregate permit maps,
//! resize waits, and exclusive-oversize bookkeeping add no safety. This type
//! only decides whether that one payload belongs in memory or a spool file.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Memory,
    Spool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchAdmission {
    memory_budget: usize,
    spool_threshold: usize,
}

impl FetchAdmission {
    pub fn new(memory_budget: usize, spool_threshold: usize) -> Self {
        Self {
            memory_budget,
            spool_threshold,
        }
    }

    pub fn admit(&self, bytes: usize) -> Admission {
        if self.should_spool(bytes) {
            Admission::Spool
        } else {
            Admission::Memory
        }
    }

    pub fn should_spool(&self, bytes: usize) -> bool {
        bytes > self.memory_budget || bytes > self.spool_threshold
    }
}

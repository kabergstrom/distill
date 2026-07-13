//! Bounded fetch admission, including the oversized-record escape hatch.

use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum Admission {
    Memory(FetchPermit),
    Spool(FetchPermit),
    Wait,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FetchPermit {
    id: u64,
    bytes: usize,
    exclusive: bool,
}

impl FetchPermit {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    IdExhausted,
    UnknownPermit,
}

#[derive(Debug)]
pub struct FetchAdmission {
    budget: usize,
    spool_threshold: usize,
    used: usize,
    exclusive: bool,
    next_id: u64,
    active: BTreeMap<u64, (usize, bool)>,
}

impl FetchAdmission {
    pub fn new(budget: usize, spool_threshold: usize) -> Self {
        Self {
            budget,
            spool_threshold,
            used: 0,
            exclusive: false,
            next_id: 1,
            active: BTreeMap::new(),
        }
    }

    pub fn admit(&mut self, bytes: usize) -> Result<Admission, AdmissionError> {
        if self.exclusive {
            return Ok(Admission::Wait);
        }
        let oversize = bytes > self.budget;
        if oversize {
            if !self.active.is_empty() {
                return Ok(Admission::Wait);
            }
        } else if self
            .used
            .checked_add(bytes)
            .is_none_or(|sum| sum > self.budget)
        {
            return Ok(Admission::Wait);
        }

        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(AdmissionError::IdExhausted)?;
        self.used = self.used.saturating_add(bytes);
        self.exclusive = oversize;
        self.active.insert(id, (bytes, oversize));
        let permit = FetchPermit {
            id,
            bytes,
            exclusive: oversize,
        };
        if bytes > self.spool_threshold {
            Ok(Admission::Spool(permit))
        } else {
            Ok(Admission::Memory(permit))
        }
    }

    pub fn release(&mut self, permit: FetchPermit) -> Result<(), AdmissionError> {
        let Some((bytes, exclusive)) = self.active.remove(&permit.id) else {
            return Err(AdmissionError::UnknownPermit);
        };
        if bytes != permit.bytes || exclusive != permit.exclusive {
            return Err(AdmissionError::UnknownPermit);
        }
        self.used -= bytes;
        if exclusive {
            self.exclusive = false;
        }
        Ok(())
    }

    pub fn used(&self) -> usize {
        self.used
    }

    pub fn in_flight(&self) -> usize {
        self.active.len()
    }
}

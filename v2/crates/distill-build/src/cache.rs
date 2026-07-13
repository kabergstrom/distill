//! Static-key candidate buckets (§9): append-only candidates and
//! newest-first verifying-trace lookup.

use crate::trace::{revalidate, FailureRecord, FailureRecordError, TraceOp, TraceSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateOutcome<T> {
    Success(T),
    Failure(FailureRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate<T> {
    pub basis_version: u64,
    pub trace: Vec<TraceOp>,
    pub outcome: CandidateOutcome<T>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    InvalidFailure(FailureRecordError),
    FailureTraceMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandidateBucket<T> {
    candidates: Vec<Candidate<T>>,
}

impl<T> CandidateBucket<T> {
    pub fn new() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.candidates.len()
    }
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub fn commit(&mut self, candidate: Candidate<T>) {
        self.try_commit(candidate)
            .expect("candidate failure grammar must be valid");
    }

    pub fn try_commit(&mut self, candidate: Candidate<T>) -> Result<(), CacheError> {
        if let CandidateOutcome::Failure(record) = &candidate.outcome {
            record.validate().map_err(CacheError::InvalidFailure)?;
            if record.trace != candidate.trace {
                return Err(CacheError::FailureTraceMismatch);
            }
        }
        self.candidates.push(candidate);
        Ok(())
    }

    pub fn lookup(&self, source: &impl TraceSource) -> Option<&Candidate<T>> {
        self.candidates
            .iter()
            .rev()
            .find(|candidate| revalidate(&candidate.trace, source))
    }
}

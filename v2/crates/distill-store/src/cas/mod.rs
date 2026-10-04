//! The log-structured CAS (§13): append-only segment files of
//! content-addressed bytes, indexed by SQLite. The index is the authority
//! on what committed: an extent is its `cas_extents` row, and what holds
//! it is a result's `result_outputs` rows or an install's `cas_refs`
//! rows. The CAS holds only derived artifacts — source bytes are never
//! copied in (§13).
//!
//! Readers and writers never coordinate. Writers serialize on SQLite's
//! write lock and each appends to a segment of its own. Readers look a
//! location up in the index and read the bytes; nothing holds a segment
//! open on their behalf. What keeps that safe is one invariant:
//!
//! **Read bound.** Every read of CAS bytes completes within `T` of the
//! index lookup that produced its location, where `T` is the longest a
//! read transaction may live (the RPC snapshot's hard expiry).
//!
//! Eviction and compaction only change index rows. A segment whose bytes
//! no index row points at is marked dead, and its file is deleted no
//! sooner than `T` plus a margin after that ([`SegmentSweeper`]). A read
//! that still loses (it outlived `T`, or its row was deleted) finds the
//! extent or file missing and reports a cache miss, never wrong bytes.

pub mod gc;
pub mod record;
pub mod recovery;
pub mod store;

pub use gc::{CompactionReport, EvictionSweep, SegmentSweeper};
pub use recovery::RecoveryReport;
pub use store::{
    AuxSpec, BuildCommit, Candidate, CandidateRow, CommitOutcome, CommitReceipt, OutputSpec,
    SegmentKind,
};

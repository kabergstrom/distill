//! The log-structured CAS (§13): append-only segment files of framed
//! records, indexed by SQLite, recovered by segment scan. **Segments are
//! the durable record within daemon state; the index is rebuildable by a
//! segment scan.** The CAS holds only derived artifacts — source bytes
//! are never copied in (§13).
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
    AuxSpec, BuildCommit, Candidate, CommitOutcome, CommitReceipt, OutputSpec, PayloadKind,
    SegmentKind,
};

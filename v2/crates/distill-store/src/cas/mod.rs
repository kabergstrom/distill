//! The log-structured CAS (§13): append-only segment files of framed
//! records, indexed by SQLite, recovered by segment scan. **Segments are
//! the durable record within daemon state; the index is rebuildable by a
//! segment scan.** The CAS holds only derived artifacts — source bytes
//! are never copied in (§13).

pub mod gc;
pub mod manifest;
pub mod record;
pub mod recovery;
pub mod store;

pub use gc::{CompactionReport, EvictionSweep};
pub use recovery::RecoveryReport;
pub use store::{
    AuxSpec, BuildCommit, Candidate, CommitOutcome, CommitReceipt, OutputSpec, PayloadKind,
};

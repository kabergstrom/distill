//! What a processor sees of a hermetic tool run (§9).

use crate::failure::{StableFailureFingerprint, ToolLaunchDiagnostic};
use crate::query::IntakeError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRunError {
    /// Stable ToolEpoch miss. The matching failing trace operation is retained.
    Stable(StableFailureFingerprint),
    /// Post-lookup launch failure. The entire attempted trace is discarded.
    Transient(ToolLaunchDiagnostic),
    /// Invalid caller input never becomes canonical trace data.
    InvalidId(IntakeError),
    /// Store/runtime infrastructure failed before a closed launch outcome
    /// could be established. The attempted trace is discarded.
    Infrastructure {
        id: String,
        detail: String,
    },
    AttemptStopped,
}

//! Byte encodings of the RPC-only facts kept in the store's served tables
//! (LOCKLESS.md §2.2): the small change-log codes.
//!
//! These bytes never leave the daemon's state directory; they only need to
//! round-trip exactly and reject anything they did not write.

use crate::*;

/// Why a persisted RPC value failed to decode. Only a corrupt or foreign
/// store produces one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistError(pub(crate) String);

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "corrupt persisted RPC state: {}", self.0)
    }
}

fn bad_tag(what: &str, tag: u8) -> PersistError {
    PersistError(format!("unknown {what} tag {tag}"))
}

pub(crate) fn delta_state_code(state: AssetDeltaState) -> u8 {
    match state {
        AssetDeltaState::Changed => 0,
        AssetDeltaState::Deleted => 1,
        AssetDeltaState::Restored => 2,
    }
}

pub(crate) fn delta_state(code: u8) -> Result<AssetDeltaState, PersistError> {
    Ok(match code {
        0 => AssetDeltaState::Changed,
        1 => AssetDeltaState::Deleted,
        2 => AssetDeltaState::Restored,
        tag => return Err(bad_tag("asset delta state", tag)),
    })
}

pub(crate) fn reconnect_code(reason: ReconnectReason) -> u8 {
    match reason {
        ReconnectReason::TargetDefinitionChanged => 0,
        ReconnectReason::StoreInstanceChanged => 1,
        ReconnectReason::ProtocolEpochChanged => 2,
        ReconnectReason::PipelineEpochChanged => 3,
    }
}

pub(crate) fn reconnect_reason(code: u8) -> Result<ReconnectReason, PersistError> {
    Ok(match code {
        0 => ReconnectReason::TargetDefinitionChanged,
        1 => ReconnectReason::StoreInstanceChanged,
        2 => ReconnectReason::ProtocolEpochChanged,
        3 => ReconnectReason::PipelineEpochChanged,
        tag => return Err(bad_tag("reconnect reason", tag)),
    })
}

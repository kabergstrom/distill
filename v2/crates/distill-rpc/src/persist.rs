//! Byte encodings of the RPC-only facts kept in the store's served tables
//! (LOCKLESS.md §2.2): the published pipeline diagnostic, drift inputs, and the small change-log codes.
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

#[derive(Default)]
struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn u32(&mut self, value: usize) {
        let value = u32::try_from(value).expect("persisted RPC count fits u32");
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.extend_from_slice(value);
    }

    fn text(&mut self, value: &str) {
        self.u32(value.len());
        self.0.extend_from_slice(value.as_bytes());
    }

}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], PersistError> {
        if self.bytes.len() < len {
            return Err(PersistError("truncated".to_owned()));
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PersistError> {
        Ok(self.take(N)?.try_into().expect("exact length"))
    }

    fn u8(&mut self) -> Result<u8, PersistError> {
        Ok(self.array::<1>()?[0])
    }

    fn count(&mut self) -> Result<usize, PersistError> {
        let count = u32::from_le_bytes(self.array()?) as usize;
        if count > self.bytes.len() {
            return Err(PersistError("count exceeds remaining bytes".to_owned()));
        }
        Ok(count)
    }

    fn text(&mut self) -> Result<String, PersistError> {
        let len = self.count()?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| PersistError("text is not UTF-8".to_owned()))
    }

    fn finish(self) -> Result<(), PersistError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(PersistError("trailing bytes".to_owned()))
        }
    }
}

fn bad_tag(what: &str, tag: u8) -> PersistError {
    PersistError(format!("unknown {what} tag {tag}"))
}

pub(crate) fn encode_drifted_input(value: &DriftedInput) -> Vec<u8> {
    let mut out = Writer::default();
    match value {
        DriftedInput::File(path) => {
            out.u8(0);
            out.text(path);
        }
        DriftedInput::Asset(asset) => {
            out.u8(1);
            out.bytes(&asset.0);
        }
        DriftedInput::Query(query) => {
            out.u8(2);
            out.text(query);
        }
        DriftedInput::Dylib => out.u8(3),
        DriftedInput::Tool(tool) => {
            out.u8(4);
            out.text(tool);
        }
    }
    out.0
}

pub(crate) fn decode_drifted_input(bytes: &[u8]) -> Result<DriftedInput, PersistError> {
    let mut reader = Reader { bytes };
    let value = match reader.u8()? {
        0 => DriftedInput::File(reader.text()?),
        1 => DriftedInput::Asset(AssetUuid(reader.array()?)),
        2 => DriftedInput::Query(reader.text()?),
        3 => DriftedInput::Dylib,
        4 => DriftedInput::Tool(reader.text()?),
        tag => return Err(bad_tag("drifted input", tag)),
    };
    reader.finish()?;
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_values_round_trip() {
        for input in [
            DriftedInput::File("a/b".into()),
            DriftedInput::Asset(AssetUuid([8; 16])),
            DriftedInput::Dylib,
            DriftedInput::Tool("t".into()),
        ] {
            assert_eq!(decode_drifted_input(&encode_drifted_input(&input)).unwrap(), input);
        }
        assert!(decode_drifted_input(&[9]).is_err());
    }
}

//! Byte encodings of the RPC-only facts kept in the store's served tables
//! (LOCKLESS.md §2.2): the published pipeline diagnostic, the lineage-repair
//! inspection state, drift inputs, and the small change-log codes.
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

    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: usize) {
        let value = u32::try_from(value).expect("persisted RPC count fits u32");
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.extend_from_slice(value);
    }

    fn text(&mut self, value: &str) {
        self.u32(value.len());
        self.0.extend_from_slice(value.as_bytes());
    }

    fn option32(&mut self, value: Option<&[u8; 32]>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.bytes(value);
            }
            None => self.u8(0),
        }
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

    fn u16(&mut self) -> Result<u16, PersistError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn count(&mut self) -> Result<usize, PersistError> {
        let count = u32::from_le_bytes(self.array()?) as usize;
        if count > self.bytes.len() {
            return Err(PersistError("count exceeds remaining bytes".to_owned()));
        }
        Ok(count)
    }

    fn u64(&mut self) -> Result<u64, PersistError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn text(&mut self) -> Result<String, PersistError> {
        let len = self.count()?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| PersistError("text is not UTF-8".to_owned()))
    }

    fn option32(&mut self) -> Result<Option<[u8; 32]>, PersistError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.array()?)),
            tag => Err(PersistError(format!("bad option tag {tag}"))),
        }
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

/// The served pipeline blob: the input version whose publication installed
/// this diagnostic, then the diagnostic. A runtime failure rewrites the
/// diagnostic but keeps the installing version, so every snapshot that
/// pinned the same pipeline sees it.
pub(crate) fn encode_served_pipeline(installed_at: InputVersion, value: &PipelineDiagnostic) -> Vec<u8> {
    let mut out = Writer::default();
    out.u64(installed_at.0);
    write_pipeline(&mut out, value);
    out.0
}

pub(crate) fn decode_served_pipeline(
    bytes: &[u8],
) -> Result<(InputVersion, PipelineDiagnostic), PersistError> {
    let mut reader = Reader { bytes };
    let installed_at = InputVersion(reader.u64()?);
    let value = read_pipeline(&mut reader)?;
    reader.finish()?;
    Ok((installed_at, value))
}

fn write_pipeline(out: &mut Writer, value: &PipelineDiagnostic) {
    match value {
        PipelineDiagnostic::Ready => out.u8(0),
        PipelineDiagnostic::Failed(failure) => {
            out.u8(1);
            out.u16(failure.code as u16);
            out.u16(failure.origin as u16);
            out.u16(failure.cleanup as u16);
            out.bytes(&failure.identity);
            out.text(&failure.message);
        }
        PipelineDiagnostic::SchemaAcceptanceRequired(required) => {
            out.u8(2);
            out.bytes(&required.manifest.manifest_hash.0);
            out.u32(required.manifest.current_cursors.len());
            for (type_uuid, logical_hash) in &required.manifest.current_cursors {
                out.bytes(&type_uuid.0);
                out.bytes(&logical_hash.0);
            }
            out.bytes(&required.candidate.dylib_hash);
            out.u32(required.candidate.target_set.rows.len());
            for row in &required.candidate.target_set.rows {
                out.text(&row.name);
                out.bytes(&row.target_definition_hash);
            }
            out.u32(required.mismatches.len());
            for mismatch in &required.mismatches {
                out.bytes(&mismatch.type_uuid.0);
                out.option32(mismatch.candidate.as_ref().map(|hash| &hash.0));
                out.option32(mismatch.manifest.as_ref().map(|hash| &hash.0));
            }
        }
        PipelineDiagnostic::RetiredTypeReferenced(retired) => {
            out.u8(3);
            out.bytes(&retired.manifest_hash.0);
            out.bytes(&retired.basis.instance.0);
            out.u64(retired.basis.version.0);
            out.bytes(&retired.type_uuid.0);
            out.u32(retired.references.len());
            for reference in &retired.references {
                match reference {
                    RetiredTypeReference::Asset(asset) => {
                        out.u8(1);
                        out.bytes(&asset.0);
                    }
                    RetiredTypeReference::MigrationEndpoint(asset) => {
                        out.u8(2);
                        out.bytes(&asset.0);
                    }
                }
            }
        }
    }
}

fn read_pipeline(reader: &mut Reader<'_>) -> Result<PipelineDiagnostic, PersistError> {
    Ok(match reader.u8()? {
        0 => PipelineDiagnostic::Ready,
        1 => {
            let code = reader.u16()?;
            let origin = reader.u16()?;
            let cleanup = reader.u16()?;
            let identity = reader.array()?;
            let message = reader.text()?;
            PipelineDiagnostic::Failed(
                PipelineFailure::from_wire(code, origin, cleanup, identity, message)
                    .map_err(|error| PersistError(format!("pipeline failure: {error:?}")))?,
            )
        }
        2 => {
            let manifest_hash = ContentHash(reader.array()?);
            let mut current_cursors = std::collections::BTreeMap::new();
            for _ in 0..reader.count()? {
                current_cursors.insert(TypeUuid(reader.array()?), LogicalHash(reader.array()?));
            }
            let dylib_hash = reader.array()?;
            let mut rows = Vec::new();
            for _ in 0..reader.count()? {
                rows.push(distill_core::target_set::TargetSetRow {
                    name: reader.text()?,
                    target_definition_hash: reader.array()?,
                });
            }
            let target_set = distill_core::target_set::CanonicalTargetSet::from_canonical(rows)
                .map_err(|error| PersistError(format!("target set: {error:?}")))?;
            let mut mismatches = Vec::new();
            for _ in 0..reader.count()? {
                mismatches.push(SchemaRegistryMismatch {
                    type_uuid: TypeUuid(reader.array()?),
                    candidate: reader.option32()?.map(LogicalHash),
                    manifest: reader.option32()?.map(LogicalHash),
                });
            }
            PipelineDiagnostic::SchemaAcceptanceRequired(SchemaAcceptanceRequired {
                manifest: SchemaManifestBasis {
                    manifest_hash,
                    current_cursors,
                },
                candidate: PipelineCandidateIdentity {
                    dylib_hash,
                    target_set,
                },
                mismatches,
            })
        }
        3 => {
            let manifest_hash = BundleFileHash(reader.array()?);
            let basis = SnapshotStamp {
                instance: StoreInstanceId(reader.array()?),
                version: InputVersion(reader.u64()?),
            };
            let type_uuid = TypeUuid(reader.array()?);
            let mut references = Vec::new();
            for _ in 0..reader.count()? {
                references.push(match reader.u8()? {
                    1 => RetiredTypeReference::Asset(AssetUuid(reader.array()?)),
                    2 => RetiredTypeReference::MigrationEndpoint(AssetUuid(reader.array()?)),
                    tag => return Err(bad_tag("retired reference", tag)),
                });
            }
            PipelineDiagnostic::RetiredTypeReferenced(RetiredTypeReferenced {
                manifest_hash,
                basis,
                type_uuid,
                references,
            })
        }
        tag => return Err(bad_tag("pipeline diagnostic", tag)),
    })
}

pub(crate) fn encode_lineage_repair(value: &LineageRepairState) -> Vec<u8> {
    let mut out = Writer::default();
    match value {
        LineageRepairState::Missing {
            configured_root,
            configured_path,
            destination,
        } => {
            out.u8(0);
            out.text(configured_root);
            out.text(configured_path);
            match destination {
                LineageRepairDestination::Absent => out.u8(0),
                LineageRepairDestination::Occupied { file_hash, kind } => {
                    out.u8(1);
                    out.bytes(&file_hash.0);
                    out.u8(match kind {
                        OccupiedLineageDestinationKind::CanonicalBundle => 0,
                        OccupiedLineageDestinationKind::Opaque => 1,
                    });
                }
            }
        }
        LineageRepairState::Duplicate { claimants } => {
            out.u8(1);
            out.u32(claimants.len());
            for claimant in claimants {
                out.text(&claimant.root_name);
                out.text(&claimant.normalized_path);
                out.bytes(&claimant.bundle.0);
                out.text(&claimant.local_id);
                out.bytes(&claimant.asset.0);
                out.bytes(&claimant.file_hash.0);
            }
        }
    }
    out.0
}

pub(crate) fn decode_lineage_repair(bytes: &[u8]) -> Result<LineageRepairState, PersistError> {
    let mut reader = Reader { bytes };
    let value = match reader.u8()? {
        0 => {
            let configured_root = reader.text()?;
            let configured_path = reader.text()?;
            let destination = match reader.u8()? {
                0 => LineageRepairDestination::Absent,
                1 => LineageRepairDestination::Occupied {
                    file_hash: BundleFileHash(reader.array()?),
                    kind: match reader.u8()? {
                        0 => OccupiedLineageDestinationKind::CanonicalBundle,
                        1 => OccupiedLineageDestinationKind::Opaque,
                        tag => return Err(bad_tag("destination kind", tag)),
                    },
                },
                tag => return Err(bad_tag("lineage destination", tag)),
            };
            LineageRepairState::Missing {
                configured_root,
                configured_path,
                destination,
            }
        }
        1 => {
            let mut claimants = Vec::new();
            for _ in 0..reader.count()? {
                claimants.push(LineageManifestClaimant {
                    root_name: reader.text()?,
                    normalized_path: reader.text()?,
                    bundle: BundleUuid(reader.array()?),
                    local_id: reader.text()?,
                    asset: AssetUuid(reader.array()?),
                    file_hash: BundleFileHash(reader.array()?),
                });
            }
            LineageRepairState::Duplicate { claimants }
        }
        tag => return Err(bad_tag("lineage repair", tag)),
    };
    reader.finish()?;
    Ok(value)
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
    fn pipeline_and_lineage_values_round_trip() {
        let failure = PipelineFailure::new(
            PipelineFailureCode::CandidateRegistration,
            PipelineFailureOrigin::CandidateOpen,
            CleanupDisposition::CleanedAndClosed,
            "boom",
        )
        .unwrap();
        let retired = PipelineDiagnostic::RetiredTypeReferenced(RetiredTypeReferenced {
            manifest_hash: BundleFileHash([1; 32]),
            basis: SnapshotStamp {
                instance: StoreInstanceId([2; 16]),
                version: InputVersion(9),
            },
            type_uuid: TypeUuid([3; 16]),
            references: vec![
                RetiredTypeReference::Asset(AssetUuid([4; 16])),
                RetiredTypeReference::MigrationEndpoint(AssetUuid([5; 16])),
            ],
        });
        for value in [
            PipelineDiagnostic::Ready,
            PipelineDiagnostic::Failed(failure),
            retired,
        ] {
            let bytes = encode_served_pipeline(InputVersion(7), &value);
            assert_eq!(
                decode_served_pipeline(&bytes).unwrap(),
                (InputVersion(7), value)
            );
        }
        let missing = LineageRepairState::Missing {
            configured_root: "main".into(),
            configured_path: "lineage.bundle".into(),
            destination: LineageRepairDestination::Occupied {
                file_hash: BundleFileHash([6; 32]),
                kind: OccupiedLineageDestinationKind::Opaque,
            },
        };
        assert_eq!(
            decode_lineage_repair(&encode_lineage_repair(&missing)).unwrap(),
            missing
        );
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

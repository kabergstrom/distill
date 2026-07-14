//! Strict decoder for the persisted DSTR v1 trace body.

use unicode_normalization::is_nfc;

use super::*;
use crate::query::TagSelector;

const MAX_TRACE_OPS: usize = 1_000_000;
const MAX_FAILURE_DEPTH: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceDecodeError {
    Truncated,
    BadDomain,
    UnsupportedVersion(u8),
    TrailingBytes,
    TooManyOperations(u32),
    InvalidUtf8,
    NonCanonicalString,
    InvalidOptionTag(u8),
    InvalidBool(u8),
    InvalidObservedTag(u8),
    InvalidQuery,
    UnknownOperation(u8),
    ReservedToolLaunch,
    UnknownFailure(u8),
    UnknownCapability(u8),
    UnknownRole(u8),
    UnknownRawFileOp(u8),
    UnknownRawFileSubject(u8),
    UnknownRawFileFailure(u8),
    UnknownControlQuery(u8),
    UnknownControlSubject(u8),
    UnknownControlFailureSubject(u8),
    UnknownControlFailureCode(u16),
    UnknownLocalFailureClass(u16),
    NonCanonicalSet,
    InvalidControlFailure,
    FailureDepthExceeded,
    OperationAfterFailure,
}

impl std::fmt::Display for TraceDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid DSTR v1 trace: {self:?}")
    }
}

impl std::error::Error for TraceDecodeError {}

pub fn decode_trace_canonical_bytes(bytes: &[u8]) -> Result<Vec<TraceOp>, TraceDecodeError> {
    let Some(domain) = bytes.get(..4) else {
        return Err(TraceDecodeError::Truncated);
    };
    if domain != DSTR {
        return Err(TraceDecodeError::BadDomain);
    }
    let version = *bytes.get(4).ok_or(TraceDecodeError::Truncated)?;
    if version != 1 {
        return Err(TraceDecodeError::UnsupportedVersion(version));
    }
    decode_trace_payload_bytes(&bytes[5..])
}

pub fn decode_trace_payload_bytes(bytes: &[u8]) -> Result<Vec<TraceOp>, TraceDecodeError> {
    let mut reader = Reader { bytes, pos: 0 };
    let count = reader.u32()?;
    let capacity =
        usize::try_from(count).map_err(|_| TraceDecodeError::TooManyOperations(count))?;
    if capacity > MAX_TRACE_OPS {
        return Err(TraceDecodeError::TooManyOperations(count));
    }
    let mut trace = Vec::with_capacity(capacity.min(4096));
    let mut failed = false;
    for _ in 0..count {
        if failed {
            return Err(TraceDecodeError::OperationAfterFailure);
        }
        let op = reader.operation()?;
        failed = op.failed();
        trace.push(op);
    }
    if reader.pos != bytes.len() {
        return Err(TraceDecodeError::TrailingBytes);
    }
    Ok(trace)
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], TraceDecodeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(TraceDecodeError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(TraceDecodeError::Truncated)?;
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, TraceDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, TraceDecodeError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, TraceDecodeError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn bool(&mut self) -> Result<bool, TraceDecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(TraceDecodeError::InvalidBool(value)),
        }
    }

    fn array16(&mut self) -> Result<[u8; 16], TraceDecodeError> {
        Ok(self.take(16)?.try_into().expect("sixteen bytes"))
    }

    fn array32(&mut self) -> Result<[u8; 32], TraceDecodeError> {
        Ok(self.take(32)?.try_into().expect("thirty-two bytes"))
    }

    fn string(&mut self) -> Result<String, TraceDecodeError> {
        let len = usize::try_from(self.u32()?).map_err(|_| TraceDecodeError::Truncated)?;
        let value =
            std::str::from_utf8(self.take(len)?).map_err(|_| TraceDecodeError::InvalidUtf8)?;
        if !is_nfc(value) {
            return Err(TraceDecodeError::NonCanonicalString);
        }
        Ok(value.to_owned())
    }

    fn option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, TraceDecodeError>,
    ) -> Result<Option<T>, TraceDecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            tag => Err(TraceDecodeError::InvalidOptionTag(tag)),
        }
    }

    fn observed<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, TraceDecodeError>,
    ) -> Result<Observed<T>, TraceDecodeError> {
        match self.u8()? {
            1 => decode(self).map(Observed::Ok),
            2 => self.failure(0).map(Observed::Err),
            tag => Err(TraceDecodeError::InvalidObservedTag(tag)),
        }
    }

    fn operation(&mut self) -> Result<TraceOp, TraceDecodeError> {
        Ok(match self.u8()? {
            1 => TraceOp::Read {
                asset: AssetUuid(self.array16()?),
                observed: self.observed(|reader| Ok(ContentHash(reader.array32()?)))?,
            },
            2 => TraceOp::Resolve {
                path: self.string()?,
                observed: self
                    .observed(|reader| reader.option(|reader| Ok(AssetUuid(reader.array16()?))))?,
            },
            3 => TraceOp::Query {
                query: Box::new(self.asset_query()?),
                observed: self.observed(Self::array32)?,
            },
            4 => return Err(TraceDecodeError::ReservedToolLaunch),
            5 => TraceOp::Capability {
                key: self.capability()?,
                observed: self.observed(Self::array32)?,
            },
            6 => TraceOp::RefCheck {
                asset: AssetUuid(self.array16()?),
                expected_terminal: TypeUuid(self.array16()?),
                observed: self
                    .observed(|reader| reader.option(|reader| Ok(TypeUuid(reader.array16()?))))?,
            },
            7 => TraceOp::RoleCheck {
                asset: AssetUuid(self.array16()?),
                observed: self.observed(|reader| reader.option(Self::role))?,
            },
            8 => TraceOp::Control {
                query: self.control_query()?,
                observed: self.observed(Self::array32)?,
            },
            9 => TraceOp::ControlRead {
                subject: self.control_subject()?,
                observed: self.observed(|reader| Ok(ControlValueHash(reader.array32()?)))?,
            },
            10 => TraceOp::Tool {
                id: self.string()?,
                observed: self.observed(Self::array32)?,
            },
            11 => TraceOp::AuthoringRead {
                asset: AssetUuid(self.array16()?),
                observed: self.observed(|reader| {
                    reader.option(|reader| Ok(BundleFileHash(reader.array32()?)))
                })?,
            },
            tag => return Err(TraceDecodeError::UnknownOperation(tag)),
        })
    }

    fn role(&mut self) -> Result<EntryRole, TraceDecodeError> {
        match self.u8()? {
            0 => Ok(EntryRole::Runtime),
            1 => Ok(EntryRole::AuthoringOnly),
            tag => Err(TraceDecodeError::UnknownRole(tag)),
        }
    }

    fn capability(&mut self) -> Result<CapabilityKey, TraceDecodeError> {
        Ok(match self.u8()? {
            1 => CapabilityKey::MigrationFn(self.string()?),
            2 => CapabilityKey::DefaultTable(TypeUuid(self.array16()?)),
            3 => CapabilityKey::Importer(self.string()?),
            4 => CapabilityKey::Processor {
                input: TypeUuid(self.array16()?),
            },
            5 => CapabilityKey::Tool(self.string()?),
            tag => return Err(TraceDecodeError::UnknownCapability(tag)),
        })
    }

    fn asset_query(&mut self) -> Result<AssetQuery, TraceDecodeError> {
        AssetQuery {
            uuid: self.option(|reader| Ok(AssetUuid(reader.array16()?)))?,
            bundle_path: self.option(Self::string)?,
            local_id: self.option(Self::string)?,
            bundle_uuid: self.option(|reader| Ok(BundleUuid(reader.array16()?)))?,
            authored_type: self.option(|reader| Ok(TypeUuid(reader.array16()?)))?,
            terminal_type: self.option(|reader| Ok(TypeUuid(reader.array16()?)))?,
            tag: self.option(|reader| {
                Ok(TagSelector {
                    tag: reader.string()?,
                    value: reader.option(Self::string)?,
                })
            })?,
            path_prefix: self.option(Self::string)?,
            path_glob: self.option(Self::string)?,
            authoring_only: self.option(Self::bool)?,
        }
        .close(None)
        .map_err(|_| TraceDecodeError::InvalidQuery)
    }

    fn file_query(&mut self) -> Result<FileQuery, TraceDecodeError> {
        Ok(FileQuery {
            path_prefix: self.option(Self::string)?,
            path_glob: self.option(Self::string)?,
        })
    }

    fn control_query(&mut self) -> Result<ControlQuery, TraceDecodeError> {
        Ok(match self.u8()? {
            1 => ControlQuery::MigrationEdges {
                type_uuid: TypeUuid(self.array16()?),
                from_hash: LogicalHash(self.array32()?),
            },
            2 => ControlQuery::DirectoryImportRuleSet,
            tag => return Err(TraceDecodeError::UnknownControlQuery(tag)),
        })
    }

    fn control_subject(&mut self) -> Result<ControlSubject, TraceDecodeError> {
        Ok(match self.u8()? {
            1 => ControlSubject::Migration(AssetUuid(self.array16()?)),
            2 => ControlSubject::PackDefinition(AssetUuid(self.array16()?)),
            3 => ControlSubject::DirectoryImportRules(AssetUuid(self.array16()?)),
            4 => ControlSubject::ImportSettings {
                bundle: BundleUuid(self.array16()?),
                local_id: self.string()?,
            },
            5 => ControlSubject::SchemaLineageManifest,
            tag => return Err(TraceDecodeError::UnknownControlSubject(tag)),
        })
    }

    fn control_failure_subject(&mut self) -> Result<ControlFailureSubject, TraceDecodeError> {
        Ok(match self.u8()? {
            1 => ControlFailureSubject::Query(self.control_query()?),
            2 => ControlFailureSubject::Read(self.control_subject()?),
            tag => return Err(TraceDecodeError::UnknownControlFailureSubject(tag)),
        })
    }

    fn failure(&mut self, depth: usize) -> Result<StableFailureFingerprint, TraceDecodeError> {
        if depth >= MAX_FAILURE_DEPTH {
            return Err(TraceDecodeError::FailureDepthExceeded);
        }
        Ok(match self.u8()? {
            0 => StableFailureFingerprint::Ambiguous {
                conflicting: self.asset_set()?,
            },
            1 => StableFailureFingerprint::Poisoned {
                bundle: BundleUuid(self.array16()?),
            },
            2 => StableFailureFingerprint::MissingRef {
                query: Box::new(self.asset_query()?),
                expected_terminal: TypeUuid(self.array16()?),
            },
            3 => StableFailureFingerprint::Descendant {
                asset: AssetUuid(self.array16()?),
                fingerprint: Box::new(self.failure(depth + 1)?),
            },
            5 => {
                let op = match self.u8()? {
                    0 => RawFileOp::Read,
                    1 => RawFileOp::Probe,
                    2 => RawFileOp::Enumerate,
                    tag => return Err(TraceDecodeError::UnknownRawFileOp(tag)),
                };
                let subject = match self.u8()? {
                    0 => RawFileSubject::Path(self.string()?),
                    1 => RawFileSubject::Query(self.file_query()?),
                    tag => return Err(TraceDecodeError::UnknownRawFileSubject(tag)),
                };
                let class = match self.u8()? {
                    0 => RawFileFailureClass::NotFound,
                    1 => RawFileFailureClass::PermissionDenied,
                    2 => RawFileFailureClass::ListingFailed,
                    3 => RawFileFailureClass::OtherStable,
                    tag => return Err(TraceDecodeError::UnknownRawFileFailure(tag)),
                };
                StableFailureFingerprint::RawFile { op, subject, class }
            }
            6 => StableFailureFingerprint::MissingCapability {
                key: self.capability()?,
            },
            7 => {
                let class_value = self.u16()?;
                let class = LocalFailureClass::from_u16(class_value)
                    .ok_or(TraceDecodeError::UnknownLocalFailureClass(class_value))?;
                StableFailureFingerprint::Local {
                    class,
                    detail: self.array32()?,
                }
            }
            8 => StableFailureFingerprint::RoleIneligible {
                asset: AssetUuid(self.array16()?),
                observed_role: self.role()?,
            },
            9 => {
                let subject = self.control_failure_subject()?;
                let code_value = self.u16()?;
                let code = control_failure_code(code_value)?;
                let entries = self.asset_set()?;
                control_failure_fingerprint(subject, code, entries)
                    .map_err(|_| TraceDecodeError::InvalidControlFailure)?
            }
            tag => return Err(TraceDecodeError::UnknownFailure(tag)),
        })
    }

    fn asset_set(&mut self) -> Result<Vec<AssetUuid>, TraceDecodeError> {
        let count = self.u32()?;
        let mut values = Vec::with_capacity((count as usize).min(4096));
        for _ in 0..count {
            let value = AssetUuid(self.array16()?);
            if values.last().is_some_and(|previous| previous >= &value) {
                return Err(TraceDecodeError::NonCanonicalSet);
            }
            values.push(value);
        }
        Ok(values)
    }
}

fn control_failure_code(value: u16) -> Result<ControlFailureCode, TraceDecodeError> {
    Ok(match value {
        1 => ControlFailureCode::RoleViolation,
        2 => ControlFailureCode::Missing,
        3 => ControlFailureCode::Ambiguous,
        4 => ControlFailureCode::Poisoned,
        5 => ControlFailureCode::Malformed,
        6 => ControlFailureCode::WrongBuiltInType,
        7 => ControlFailureCode::WrongRole,
        8 => ControlFailureCode::UnsupportedFormat,
        9 => ControlFailureCode::SchemaClosure,
        _ => return Err(TraceDecodeError::UnknownControlFailureCode(value)),
    })
}

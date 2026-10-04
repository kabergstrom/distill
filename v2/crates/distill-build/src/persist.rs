//! Durable candidate lookup over the store's append-only result buckets.

use distill_core::id::{AssetUuid, ContentHash, TypeUuid};
use distill_store::cas::record::{FailureCause, FailureFingerprint, KeyKind, ResultOutcome};
use distill_store::cas::{Candidate, CandidateRow};
use distill_store::state::MemoSeq;
use distill_store::{StoreError, StoreReader};

use crate::trace::{
    decode_trace_payload_bytes, revalidate, trace_digest, TraceDecodeError, TraceOp, TraceSource,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydratedOutput {
    pub output_key: String,
    pub type_uuids: Vec<TypeUuid>,
    pub content_hash: ContentHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydratedAux {
    pub debug_key: String,
    pub content_hash: ContentHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistedOutcome {
    Success {
        outputs: Vec<HydratedOutput>,
        aux: Vec<HydratedAux>,
    },
    Failure {
        cause: FailureCause,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedHit {
    pub memo_seq: MemoSeq,
    pub trace_digest: [u8; 32],
    pub asset_uuid: AssetUuid,
    pub trace: Vec<TraceOp>,
    pub outcome: PersistedOutcome,
}

#[derive(Debug)]
pub enum PersistedCacheError {
    Store(StoreError),
    Trace(TraceDecodeError),
    KeyKindMismatch {
        expected: KeyKind,
        observed: KeyKind,
    },
    AssetMismatch {
        expected: AssetUuid,
        observed: AssetUuid,
    },
    TraceDigestMismatch {
        indexed: [u8; 32],
        observed: [u8; 32],
    },
    InvalidFailureCause,
}

impl std::fmt::Display for PersistedCacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "persisted build candidate: {self:?}")
    }
}

impl std::error::Error for PersistedCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Trace(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StoreError> for PersistedCacheError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<TraceDecodeError> for PersistedCacheError {
    fn from(error: TraceDecodeError) -> Self {
        Self::Trace(error)
    }
}

/// Look up the newest result whose complete trace still holds at `source`.
/// The bucket's rows are read once and each candidate's record only when
/// the walk reaches it. Payload extents are authenticated and hydrated
/// before the hit is returned; a corrupt candidate is an error rather than
/// a silent cache miss.
pub fn lookup_persisted_candidate(
    store: &StoreReader,
    key_kind: KeyKind,
    static_key: &[u8; 32],
    expected_asset: AssetUuid,
    source: &impl TraceSource,
) -> Result<Option<PersistedHit>, PersistedCacheError> {
    for row in store.candidate_rows(key_kind, static_key)? {
        let Some((candidate, trace)) = persisted_candidate(store, &row, key_kind, expected_asset)?
        else {
            continue;
        };
        if revalidate(&trace, source) {
            return hydrate_persisted_candidate(store, candidate, trace).map(Some);
        }
    }
    Ok(None)
}

/// The candidate `row` locates with its decoded trace, authenticated
/// against the bucket it was found in; `None` when its record is gone (a
/// cache miss). Hosts that materialize a trace's content dependencies
/// before revalidating walk a bucket's rows with this, then hydrate the
/// first candidate that holds.
pub fn persisted_candidate(
    store: &StoreReader,
    row: &CandidateRow,
    key_kind: KeyKind,
    expected_asset: AssetUuid,
) -> Result<Option<(Candidate, Vec<TraceOp>)>, PersistedCacheError> {
    let Some(candidate) = store.read_candidate(row)? else {
        return Ok(None);
    };
    if candidate.payload.key_kind != key_kind {
        return Err(PersistedCacheError::KeyKindMismatch {
            expected: key_kind,
            observed: candidate.payload.key_kind,
        });
    }
    if candidate.asset_uuid != expected_asset {
        return Err(PersistedCacheError::AssetMismatch {
            expected: expected_asset,
            observed: candidate.asset_uuid,
        });
    }
    let trace = decode_trace_payload_bytes(&candidate.payload.trace)?;
    let observed_digest = trace_digest(&trace);
    if observed_digest != candidate.trace_digest {
        return Err(PersistedCacheError::TraceDigestMismatch {
            indexed: candidate.trace_digest,
            observed: observed_digest,
        });
    }
    Ok(Some((candidate, trace)))
}

/// The hit a candidate whose trace holds makes: its payload extents read
/// and authenticated, a failure validated against its trace.
pub fn hydrate_persisted_candidate(
    store: &StoreReader,
    candidate: Candidate,
    trace: Vec<TraceOp>,
) -> Result<PersistedHit, PersistedCacheError> {
    let outcome = match candidate.payload.outcome {
        ResultOutcome::Success { outputs, aux } => {
            let outputs = outputs
                .into_iter()
                .map(|row| {
                    let bytes = store.cas_read(&row.content_hash.0)?;
                    Ok(HydratedOutput {
                        output_key: row.output_key,
                        type_uuids: row.type_uuids,
                        content_hash: row.content_hash,
                        bytes,
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            let aux = aux
                .into_iter()
                .map(|row| {
                    let bytes = store.cas_read(&row.content_hash.0)?;
                    Ok(HydratedAux {
                        debug_key: row.debug_key,
                        content_hash: row.content_hash,
                        bytes,
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            PersistedOutcome::Success { outputs, aux }
        }
        ResultOutcome::Failure { cause } => {
            validate_failure(&trace, &cause)?;
            PersistedOutcome::Failure { cause }
        }
    };
    Ok(PersistedHit {
        memo_seq: candidate.memo_seq,
        trace_digest: candidate.trace_digest,
        asset_uuid: candidate.asset_uuid,
        trace,
        outcome,
    })
}

fn validate_failure(trace: &[TraceOp], cause: &FailureCause) -> Result<(), PersistedCacheError> {
    match cause {
        FailureCause::Op if trace.last().is_some_and(TraceOp::failed) => Ok(()),
        FailureCause::Local(FailureFingerprint::Local { .. }) => Ok(()),
        _ => Err(PersistedCacheError::InvalidFailureCause),
    }
}

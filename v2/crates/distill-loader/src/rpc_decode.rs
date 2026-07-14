//! Shared authentication helpers for the concrete RPC loader transport.

use std::ops::Range;
use std::sync::Arc;

use distill_core::id::{ContentHash, LayoutHash};
use distill_rpc::RpcBasis;
use distill_wire::artifact::parse_artifact_parts;
use distill_wire::exec::Blob;

use crate::basis::{IoBasis, LoadPolicyAttestation, LoadPolicyError, LoadPolicyRow};
use crate::io::FetchedArtifact;

pub(crate) fn io_basis(basis: &RpcBasis) -> Result<IoBasis, LoadPolicyError> {
    let rows = basis
        .load_policy
        .rows
        .iter()
        .map(|row| LoadPolicyRow {
            type_uuid: row.type_uuid,
            build_only: row.build_only,
        })
        .collect();
    let load_policy = Arc::new(LoadPolicyAttestation::try_from_parts(
        rows,
        basis.load_policy.digest,
    )?);
    Ok(IoBasis::Rpc {
        snapshot: basis.snapshot,
        load_policy,
        daemon_compiled_projection: basis.daemon_compiled_projection,
        policy_generation: basis.policy_generation,
        target_generation: basis.target_generation,
        attestation_generation: basis.attestation_generation,
    })
}

pub(crate) fn artifact_layout_hash(
    content_hash: ContentHash,
    structural: &[u8],
    raw_blobs: &[Vec<u8>],
) -> Result<LayoutHash, String> {
    let blob_parts = raw_blobs.iter().map(Vec::as_slice).collect::<Vec<_>>();
    artifact_layout_hash_parts(content_hash, structural, &blob_parts)
}

pub(crate) fn artifact_layout_hash_parts(
    content_hash: ContentHash,
    structural: &[u8],
    raw_blobs: &[&[u8]],
) -> Result<LayoutHash, String> {
    let parsed = parse_artifact_parts(structural, raw_blobs)
        .map_err(|error| format!("invalid fetched artifact: {error}"))?;
    if parsed.content_hash != content_hash {
        return Err("fetched artifact ContentHash mismatch".into());
    }
    Ok(parsed.layout_hash)
}

pub(crate) fn artifact_layout_hash_backed(
    content_hash: ContentHash,
    backing: &(dyn AsRef<[u8]> + Send + Sync),
    structural: &Range<usize>,
    blobs: &[Range<usize>],
) -> Result<LayoutHash, String> {
    let bytes = backing.as_ref();
    let structural = bytes
        .get(structural.clone())
        .ok_or_else(|| "spooled structural range is out of bounds".to_owned())?;
    let blob_parts = blobs
        .iter()
        .map(|range| {
            bytes
                .get(range.clone())
                .ok_or_else(|| "spooled blob range is out of bounds".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    artifact_layout_hash_parts(content_hash, structural, &blob_parts)
}

pub(crate) fn fetched_artifact(
    layout_hash: LayoutHash,
    structural: Vec<u8>,
    raw_blobs: Vec<Vec<u8>>,
    wire_layout: Arc<[u8]>,
) -> Result<FetchedArtifact, String> {
    verify_wire_layout(layout_hash, &wire_layout)?;
    let blobs = raw_blobs
        .into_iter()
        .map(|bytes| {
            let len = bytes.len();
            let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(bytes);
            Blob::new(backing, 0, len)
        })
        .collect();
    Ok(FetchedArtifact {
        structural: Arc::from(structural),
        blobs,
        wire_layout,
    })
}

pub(crate) fn fetched_artifact_backed(
    layout_hash: LayoutHash,
    backing: Arc<dyn AsRef<[u8]> + Send + Sync>,
    structural: Range<usize>,
    blob_ranges: Vec<Range<usize>>,
    wire_layout: Arc<[u8]>,
) -> Result<FetchedArtifact, String> {
    verify_wire_layout(layout_hash, &wire_layout)?;
    let structural_bytes = backing
        .as_ref()
        .as_ref()
        .get(structural)
        .ok_or_else(|| "spooled structural range is out of bounds".to_owned())?;
    let blobs = blob_ranges
        .into_iter()
        .map(|range| {
            let len = range.end.saturating_sub(range.start);
            Blob::new(Arc::clone(&backing), range.start, len)
        })
        .collect();
    Ok(FetchedArtifact {
        structural: Arc::from(structural_bytes),
        blobs,
        wire_layout,
    })
}

fn verify_wire_layout(layout_hash: LayoutHash, wire_layout: &[u8]) -> Result<(), String> {
    let wire = distill_wire::dswl::decode_dswl(wire_layout)
        .map_err(|error| format!("invalid DSWL wire tree: {error:?}"))?;
    if distill_wire::dswl::dswl_hash(&wire).ok() != Some(layout_hash) {
        return Err("DSWL wire-tree hash mismatch".into());
    }
    Ok(())
}

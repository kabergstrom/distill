//! Shared authentication helpers for the concrete RPC loader transport.

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

pub(crate) fn collect_artifact_chunks(
    chunks: impl IntoIterator<Item = distill_rpc::ArtifactChunk>,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), String> {
    let mut structural = Vec::new();
    let mut blobs = std::collections::BTreeMap::<u32, Vec<u8>>::new();
    for chunk in chunks {
        let output = match chunk.kind {
            distill_rpc::ArtifactChunkKind::Structural => &mut structural,
            distill_rpc::ArtifactChunkKind::Blob { index } => blobs.entry(index).or_default(),
        };
        if chunk.offset != output.len() as u64 {
            return Err("artifact chunks are not contiguous".into());
        }
        output.extend_from_slice(&chunk.bytes);
    }
    if blobs.keys().copied().ne(0..blobs.len() as u32) {
        return Err("artifact blob chunk indices are not contiguous".into());
    }
    Ok((structural, blobs.into_values().collect()))
}

pub(crate) fn artifact_layout_hash(
    content_hash: ContentHash,
    structural: &[u8],
    raw_blobs: &[Vec<u8>],
) -> Result<LayoutHash, String> {
    let blob_parts = raw_blobs.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let parsed = parse_artifact_parts(structural, &blob_parts)
        .map_err(|error| format!("invalid fetched artifact: {error}"))?;
    if parsed.content_hash != content_hash {
        return Err("fetched artifact ContentHash mismatch".into());
    }
    Ok(parsed.layout_hash)
}

pub(crate) fn fetched_artifact(
    layout_hash: LayoutHash,
    structural: Vec<u8>,
    raw_blobs: Vec<Vec<u8>>,
    wire_layout: Arc<[u8]>,
) -> Result<FetchedArtifact, String> {
    let wire = distill_wire::dswl::decode_dswl(&wire_layout)
        .map_err(|error| format!("invalid DSWL wire tree: {error:?}"))?;
    if distill_wire::dswl::dswl_hash(&wire).ok() != Some(layout_hash) {
        return Err("DSWL wire-tree hash mismatch".into());
    }
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

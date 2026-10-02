//! Shared authentication helpers for the concrete RPC loader transport.

use std::ops::Range;
use std::sync::Arc;

use distill_core::id::LayoutHash;
use distill_rpc::RpcBasis;
use distill_wire::exec::Blob;

use crate::basis::IoBasis;
use crate::io::FetchedArtifact;

pub(crate) fn io_basis(basis: &RpcBasis) -> IoBasis {
    IoBasis::Rpc {
        snapshot: basis.snapshot,
    }
}

pub(crate) fn fetched_artifact(
    layout_hash: LayoutHash,
    structural: Vec<u8>,
    raw_blobs: Vec<Vec<u8>>,
    load_edges: Vec<distill_rpc::ServedLoadEdge>,
    wire_layout: Blob,
) -> Result<FetchedArtifact, String> {
    verify_wire_layout(layout_hash, wire_layout.as_bytes())?;
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
        load_edges,
        wire_layout,
    })
}

pub(crate) fn fetched_artifact_backed(
    layout_hash: LayoutHash,
    backing: Arc<dyn AsRef<[u8]> + Send + Sync>,
    structural: Range<usize>,
    blob_ranges: Vec<Range<usize>>,
    load_edges: Vec<distill_rpc::ServedLoadEdge>,
    wire_layout: Blob,
) -> Result<FetchedArtifact, String> {
    verify_wire_layout(layout_hash, wire_layout.as_bytes())?;
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
        load_edges,
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

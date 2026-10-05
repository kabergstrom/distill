//! Shared authentication helpers for the concrete RPC loader transport.

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
    payload: crate::capnp_io::RemotePayload,
    load_edges: Vec<distill_rpc::ServedLoadEdge>,
    wire_layout: Blob,
    timing: crate::stats::FetchTiming,
) -> Result<FetchedArtifact, String> {
    verify_wire_layout(layout_hash, wire_layout.as_bytes())?;
    // The structural section is small and read on the loader thread; the
    // blobs stay ranges of the one received buffer.
    let structural = Arc::from(&payload.bytes[payload.structural.clone()]);
    let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(payload.bytes);
    let blobs = payload
        .blobs
        .into_iter()
        .map(|range| Blob::new(backing.clone(), range.start, range.len()))
        .collect();
    Ok(FetchedArtifact {
        structural,
        blobs,
        load_edges,
        wire_layout,
        timing,
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

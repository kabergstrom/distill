use distill_loader::{IoBasis, ManifestHash};
use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

#[test]
fn rpc_basis_is_exactly_the_snapshot_stamp() {
    let stamp = SnapshotStamp {
        instance: StoreInstanceId([1; 16]),
        version: InputVersion(2),
    };
    assert_eq!(
        IoBasis::Rpc { snapshot: stamp },
        IoBasis::Rpc { snapshot: stamp }
    );
}

#[test]
fn pack_basis_is_exactly_the_manifest_hash() {
    let hash = ManifestHash([3; 32]);
    assert_eq!(
        IoBasis::Pack { manifest: hash },
        IoBasis::Pack { manifest: hash }
    );
}

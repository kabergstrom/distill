use std::collections::BTreeMap;

use distill_build::persist::{lookup_persisted_candidate, PersistedOutcome};
use distill_build::query::AssetQuery;
use distill_build::trace::{
    trace_payload_bytes, CapabilityKey, ControlQuery, ControlSubject, ControlValueHash, EntryRole,
    Observed, TraceOp, TraceSource,
};
use distill_core::id::{AssetUuid, BundleFileHash, ContentHash, TypeUuid};
use distill_store::cas::record::KeyKind;
use distill_store::cas::{BuildCommit, CommitOutcome, OutputSpec, PayloadKind};
use distill_store::{Store, StoreConfig};

#[derive(Default)]
struct Snapshot {
    paths: BTreeMap<String, Observed<Option<AssetUuid>>>,
}

impl TraceSource for Snapshot {
    fn authoring_read(&self, _: AssetUuid) -> Observed<Option<BundleFileHash>> {
        Observed::Ok(None)
    }
    fn read(&self, _: AssetUuid) -> Observed<ContentHash> {
        Observed::Ok(ContentHash([0; 32]))
    }

    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        self.paths.get(path).cloned().unwrap_or(Observed::Ok(None))
    }

    fn query(&self, _: &AssetQuery) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }

    fn tool(&self, _: &str) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }

    fn capability(&self, _: &CapabilityKey) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }

    fn ref_check(&self, _: AssetUuid, _: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(None)
    }

    fn role_check(&self, _: AssetUuid) -> Observed<Option<EntryRole>> {
        Observed::Ok(None)
    }

    fn control(&self, _: &ControlQuery) -> Observed<[u8; 32]> {
        Observed::Ok([0; 32])
    }

    fn control_read(&self, _: &ControlSubject) -> Observed<ControlValueHash> {
        Observed::Ok(ControlValueHash([0; 32]))
    }
}

fn commit(store: &mut Store, key: [u8; 32], asset: AssetUuid, trace: Vec<TraceOp>, bytes: &[u8]) {
    store
        .commit_build(BuildCommit {
            key_kind: KeyKind::Processor,
            static_input_key: key,
            asset_uuid: asset,
            static_inputs_canonical: b"static".to_vec(),
            trace: trace_payload_bytes(&trace),
            outcome: CommitOutcome::Success {
                payload_kind: PayloadKind::ProcessorOutput,
                outputs: vec![OutputSpec {
                    output_key: String::new(),
                    type_uuids: vec![TypeUuid([3; 16])],
                    bytes: bytes.to_vec(),
                }],
                aux: Vec::new(),
            },
        })
        .unwrap();
}

#[test]
fn durable_bucket_revalidates_newest_first_and_hydrates_the_selected_extent() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let key = [7; 32];
    let asset = AssetUuid([1; 16]);
    let resolved = AssetUuid([2; 16]);
    commit(
        &mut store,
        key,
        asset,
        vec![TraceOp::Resolve {
            path: "asset.bundle".into(),
            observed: Observed::Ok(None),
        }],
        b"old-basis artifact",
    );
    commit(
        &mut store,
        key,
        asset,
        vec![TraceOp::Resolve {
            path: "asset.bundle".into(),
            observed: Observed::Ok(Some(resolved)),
        }],
        b"new-basis artifact",
    );

    let missing = Snapshot::default();
    let hit = lookup_persisted_candidate(&mut store, KeyKind::Processor, &key, asset, &missing)
        .unwrap()
        .unwrap();
    let PersistedOutcome::Success { outputs, .. } = hit.outcome else {
        panic!("expected success")
    };
    assert_eq!(outputs[0].bytes, b"old-basis artifact");

    let present = Snapshot {
        paths: BTreeMap::from([("asset.bundle".into(), Observed::Ok(Some(resolved)))]),
    };
    let hit = lookup_persisted_candidate(&mut store, KeyKind::Processor, &key, asset, &present)
        .unwrap()
        .unwrap();
    let PersistedOutcome::Success { outputs, .. } = hit.outcome else {
        panic!("expected success")
    };
    assert_eq!(outputs[0].bytes, b"new-basis artifact");
}

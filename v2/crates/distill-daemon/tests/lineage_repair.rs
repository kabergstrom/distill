use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use distill_asset::{AssetReflect, AssetType};
use distill_bundle::{AssetEntry, Bundle, EntryLineageV1};
use distill_core::attestation::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID;
use distill_core::id::{AssetUuid, BundleFileHash, BundleUuid, TypeUuid};
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch, LineageStamp};
use distill_daemon::lineage_repair::LineageRepairBackend;
use distill_daemon::scanner::{AssetRoot, RootedScanner};
use distill_json::AuthoredValue;
use distill_rpc::{
    AuthoringBackend, ConfigurationStatus, LineageRepairBackendError, LineageRepairDestination,
    LineageRepairInspection, LineageRepairInvalidCode, LineageRepairStaleCode, LineageRepairState,
    OccupiedLineageDestinationKind,
};
use distill_schema::bootstrap_builtins_v1::{
    AcceptedSchemaEpochV1, AcceptedTypeLineageV1, SchemaLineageManifestV1, TypeAuthorityStateV1,
};
use distill_schema::ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode};
use distill_store::{Store, StoreConfig};

fn manifest_schema() -> LogicalSchema {
    let bytes = distill_asset::build::logical_schema_bytes::<SchemaLineageManifestV1>();
    let mut decoder = NodeDecoder {
        bytes: &bytes,
        at: 0,
    };
    let root = decoder.node();
    assert_eq!(decoder.at, bytes.len());
    LogicalSchema { root }
}

fn manifest_data() -> AuthoredValue {
    SchemaLineageManifestV1 {
        types: BTreeMap::new(),
    }
    .to_authored()
}

fn manifest_entry(asset: u8, data: AuthoredValue) -> AssetEntry {
    let descriptor = SchemaLineageManifestV1::descriptor();
    AssetEntry {
        uuid: AssetUuid([asset; 16]),
        type_uuid: SchemaLineageManifestV1::TYPE_UUID,
        schema_hash: descriptor.logical_hash,
        lineage: EntryLineageV1::Bootstrap {
            bundle_format_version: 1,
        },
        authoring_only: true,
        data,
    }
}

fn manifest_bundle(bundle: u8, asset: u8, local_id: &str) -> Vec<u8> {
    let schema = manifest_schema();
    let hash = node_hash(&schema.root).unwrap();
    assert_eq!(hash, SchemaLineageManifestV1::descriptor().logical_hash);
    distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([bundle; 16]),
        primary: None,
        schemas: BTreeMap::from([(hash, schema)]),
        assets: BTreeMap::from([(local_id.into(), manifest_entry(asset, manifest_data()))]),
    })
    .unwrap()
}

fn ordinary_bundle(bundle: u8) -> Bundle {
    let type_uuid = TypeUuid([91; 16]);
    let schema = LogicalSchema {
        root: SchemaNode::Primitive(PrimitiveKind::U8),
    };
    let schema_hash = node_hash(&schema.root).unwrap();
    let epochs = vec![AcceptedSchemaEpoch {
        digest: schema_hash,
        forward_parent: None,
    }];
    Bundle {
        format_version: 1,
        uuid: BundleUuid([bundle; 16]),
        primary: Some("ordinary".into()),
        schemas: BTreeMap::from([(schema_hash, schema)]),
        assets: BTreeMap::from([(
            "ordinary".into(),
            AssetEntry {
                uuid: AssetUuid([92; 16]),
                type_uuid,
                schema_hash,
                lineage: EntryLineageV1::Manifest(LineageStamp {
                    chain: lineage_chain_digest(type_uuid, &epochs, 0),
                    epochs,
                    cursor: 0,
                }),
                authoring_only: false,
                data: AuthoredValue::UInt(3),
            },
        )]),
    }
}

struct Harness {
    _temp: tempfile::TempDir,
    root: std::path::PathBuf,
    store: Arc<Mutex<Store>>,
    backend: LineageRepairBackend,
}

impl Harness {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("assets");
        std::fs::create_dir(&root).unwrap();
        let store = Arc::new(Mutex::new(
            Store::open(StoreConfig::new(temp.path().join("state"))).unwrap(),
        ));
        let backend = LineageRepairBackend::new(
            store.clone(),
            [AssetRoot::new(
                "main",
                &root,
                root.join(".distill-displaced"),
            )],
        )
        .unwrap();
        Self {
            _temp: temp,
            root,
            store,
            backend,
        }
    }

    fn inspection(&self, state: LineageRepairState) -> LineageRepairInspection {
        let store = self.store.lock().unwrap();
        LineageRepairInspection {
            instance: store.instance_id(),
            stamp: store.stamp(),
            state,
        }
    }

    fn scanner(&self) -> RootedScanner {
        RootedScanner::new([AssetRoot::new(
            "main",
            &self.root,
            self.root.join(".distill-displaced"),
        )])
        .unwrap()
    }
}

#[test]
fn missing_manifest_creation_is_no_replace_rescan_proven_and_store_durable() {
    let harness = Harness::new();
    let bytes = manifest_bundle(1, 2, "lineage");
    let basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "control/lineage.bundle".into(),
        destination: LineageRepairDestination::Absent,
    });

    let commit = harness
        .backend
        .prepare_create_missing_lineage(&basis, &bytes)
        .unwrap();

    assert_eq!(commit.configuration, Some(ConfigurationStatus::Ready));
    assert_eq!(commit.lineage_repair, Some(None));
    assert_eq!(
        std::fs::read(harness.root.join("control/lineage.bundle")).unwrap(),
        bytes
    );
    assert_eq!(harness.store.lock().unwrap().input_version().0, 1);
    assert_eq!(harness.scanner().lineage_claimants().unwrap().len(), 1);
}

#[test]
fn occupied_canonical_creation_preserves_every_existing_entry() {
    let harness = Harness::new();
    let target = harness.root.join("lineage.bundle");
    let old = ordinary_bundle(3);
    let old_bytes = distill_bundle::write_bundle(&old).unwrap();
    std::fs::write(&target, &old_bytes).unwrap();
    let mut proposed = old.clone();
    let manifest = manifest_entry(4, manifest_data());
    proposed
        .schemas
        .insert(manifest.schema_hash, manifest_schema());
    proposed.assets.insert("lineage".into(), manifest);
    let proposed_bytes = distill_bundle::write_bundle(&proposed).unwrap();
    let basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "lineage.bundle".into(),
        destination: LineageRepairDestination::Occupied {
            file_hash: BundleFileHash::of_observed_bytes(&old_bytes),
            kind: OccupiedLineageDestinationKind::CanonicalBundle,
        },
    });

    harness
        .backend
        .prepare_create_missing_lineage(&basis, &proposed_bytes)
        .unwrap();

    let installed = distill_bundle::parse_bundle(&std::fs::read(target).unwrap()).unwrap();
    assert_eq!(installed.assets["ordinary"], old.assets["ordinary"]);
    assert!(installed.assets.contains_key("lineage"));
}

#[test]
fn occupied_opaque_creation_retains_exact_raw_preimage_in_quarantine() {
    let harness = Harness::new();
    let target = harness.root.join("lineage.bundle");
    std::fs::write(&target, b"opaque user bytes").unwrap();
    let proposed = manifest_bundle(7, 8, "lineage");
    let basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "lineage.bundle".into(),
        destination: LineageRepairDestination::Occupied {
            file_hash: BundleFileHash::of_observed_bytes(b"opaque user bytes"),
            kind: OccupiedLineageDestinationKind::Opaque,
        },
    });

    harness
        .backend
        .prepare_create_missing_lineage(&basis, &proposed)
        .unwrap();

    assert_eq!(std::fs::read(target).unwrap(), proposed);
    let quarantined = harness.store.lock().unwrap().quarantined_entries().unwrap();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(
        std::fs::read(&quarantined[0].path).unwrap(),
        b"opaque user bytes"
    );
}

#[test]
fn duplicate_repair_keeps_the_explicit_survivor_and_quarantines_other_file() {
    let harness = Harness::new();
    std::fs::write(harness.root.join("a.bundle"), manifest_bundle(10, 11, "a")).unwrap();
    std::fs::write(harness.root.join("b.bundle"), manifest_bundle(12, 13, "b")).unwrap();
    let scanner = harness.scanner();
    let claimants = scanner.lineage_claimants().unwrap();
    let survivor = claimants[0].clone();
    let removed_path = claimants[1].normalized_path.clone();
    let basis = harness.inspection(LineageRepairState::Duplicate {
        claimants: claimants.clone(),
    });

    let commit = harness
        .backend
        .prepare_resolve_duplicate_lineage(&basis, &survivor)
        .unwrap();

    assert_eq!(commit.configuration, Some(ConfigurationStatus::Ready));
    assert_eq!(scanner.lineage_claimants().unwrap(), [survivor]);
    assert!(!harness.root.join(removed_path).exists());
    assert_eq!(harness.store.lock().unwrap().input_version().0, 1);
}

#[test]
fn colocated_duplicate_entries_are_rewritten_injectively() {
    let harness = Harness::new();
    let schema = manifest_schema();
    let hash = node_hash(&schema.root).unwrap();
    let bundle = Bundle {
        format_version: 1,
        uuid: BundleUuid([21; 16]),
        primary: None,
        schemas: BTreeMap::from([(hash, schema)]),
        assets: BTreeMap::from([
            ("a".into(), manifest_entry(22, manifest_data())),
            ("b".into(), manifest_entry(23, manifest_data())),
        ]),
    };
    let target = harness.root.join("both.bundle");
    std::fs::write(&target, distill_bundle::write_bundle(&bundle).unwrap()).unwrap();
    let scanner = harness.scanner();
    let claimants = scanner.lineage_claimants().unwrap();
    let survivor = claimants[1].clone();
    let basis = harness.inspection(LineageRepairState::Duplicate {
        claimants: claimants.clone(),
    });

    harness
        .backend
        .prepare_resolve_duplicate_lineage(&basis, &survivor)
        .unwrap();

    let remaining = scanner.lineage_claimants().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].asset, survivor.asset);
    assert_ne!(remaining[0].file_hash, survivor.file_hash);
    let parsed = distill_bundle::parse_bundle(&std::fs::read(target).unwrap()).unwrap();
    assert_eq!(parsed.assets.keys().collect::<Vec<_>>(), ["b"]);
}

#[test]
fn post_inspection_preimage_drift_is_typed_stale_and_never_overwritten() {
    let harness = Harness::new();
    let target = harness.root.join("lineage.bundle");
    std::fs::write(&target, b"opaque old").unwrap();
    let basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "lineage.bundle".into(),
        destination: LineageRepairDestination::Occupied {
            file_hash: BundleFileHash::of_observed_bytes(b"opaque old"),
            kind: OccupiedLineageDestinationKind::Opaque,
        },
    });
    std::fs::write(&target, b"external edit").unwrap();

    let error = harness
        .backend
        .prepare_create_missing_lineage(&basis, &manifest_bundle(31, 32, "lineage"))
        .unwrap_err();

    assert!(matches!(
        error,
        LineageRepairBackendError::Stale(stale)
            if stale.code == LineageRepairStaleCode::PreimageChanged
    ));
    assert_eq!(std::fs::read(target).unwrap(), b"external edit");
}

#[test]
fn durable_store_stamp_mismatch_is_stale_before_any_temp_or_target_write() {
    let harness = Harness::new();
    let mut basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "lineage.bundle".into(),
        destination: LineageRepairDestination::Absent,
    });
    basis.stamp.version.0 += 1;

    assert!(matches!(
        harness
            .backend
            .prepare_create_missing_lineage(&basis, &manifest_bundle(35, 36, "lineage")),
        Err(LineageRepairBackendError::Stale(stale))
            if stale.code == LineageRepairStaleCode::StampChanged
    ));
    assert_eq!(std::fs::read_dir(&harness.root).unwrap().count(), 0);
}

#[test]
fn proposed_authority_rejects_bootstrap_types_inside_the_manifest_map() {
    let harness = Harness::new();
    let data = SchemaLineageManifestV1 {
        types: BTreeMap::from([(
            SCHEMA_LINEAGE_MANIFEST_TYPE_UUID.0,
            AcceptedTypeLineageV1 {
                epochs: vec![AcceptedSchemaEpochV1 {
                    digest: [1; 32],
                    forward_parent: None,
                }],
                current: 0,
                authority: TypeAuthorityStateV1::Active {},
            },
        )]),
    }
    .to_authored();
    let schema = manifest_schema();
    let hash = node_hash(&schema.root).unwrap();
    let bytes = distill_bundle::write_bundle(&Bundle {
        format_version: 1,
        uuid: BundleUuid([41; 16]),
        primary: None,
        schemas: BTreeMap::from([(hash, schema)]),
        assets: BTreeMap::from([("lineage".into(), manifest_entry(42, data))]),
    })
    .unwrap();
    let basis = harness.inspection(LineageRepairState::Missing {
        configured_root: "main".into(),
        configured_path: "lineage.bundle".into(),
        destination: LineageRepairDestination::Absent,
    });

    assert!(matches!(
        harness
            .backend
            .prepare_create_missing_lineage(&basis, &bytes),
        Err(LineageRepairBackendError::Invalid(invalid))
            if invalid.code == LineageRepairInvalidCode::BootstrapTypePresent
    ));
}

struct NodeDecoder<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl NodeDecoder<'_> {
    fn node(&mut self) -> SchemaNode {
        match self.u8() {
            0x01 => SchemaNode::Primitive(PrimitiveKind::from_name(&self.string()).unwrap()),
            0x02 => SchemaNode::Struct {
                rev: self.u32(),
                fields: self.rows(),
            },
            0x03 => SchemaNode::Enum {
                rev: self.u32(),
                variants: self.rows(),
            },
            0x04 => SchemaNode::Vec(Box::new(self.node())),
            0x05 => {
                let len = self.u64();
                SchemaNode::Array {
                    len,
                    elem: Box::new(self.node()),
                }
            }
            0x06 => SchemaNode::Option(Box::new(self.node())),
            0x07 => {
                let key = Box::new(self.node());
                SchemaNode::Map {
                    key,
                    value: Box::new(self.node()),
                }
            }
            0x08 => SchemaNode::String,
            0x09 => SchemaNode::AssetRef(TypeUuid(self.fixed())),
            0x0a => SchemaNode::WeakRef(TypeUuid(self.fixed())),
            0x0b => SchemaNode::Blob,
            0x0c => SchemaNode::BackRef(self.u32()),
            0x0d => SchemaNode::Unit,
            0x0e => SchemaNode::Set(Box::new(self.node())),
            tag => panic!("unknown logical schema tag {tag}"),
        }
    }

    fn rows(&mut self) -> Vec<(String, u32, SchemaNode)> {
        (0..self.u32())
            .map(|_| {
                let name = self.string();
                let rev = self.u32();
                (name, rev, self.node())
            })
            .collect()
    }

    fn u8(&mut self) -> u8 {
        let value = self.bytes[self.at];
        self.at += 1;
        value
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn string(&mut self) -> String {
        let len = self.u32() as usize;
        let value = std::str::from_utf8(&self.bytes[self.at..self.at + len])
            .unwrap()
            .to_owned();
        self.at += len;
        value
    }

    fn fixed<const N: usize>(&mut self) -> [u8; N] {
        self.take()
    }

    fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut value = [0; N];
        value.copy_from_slice(&self.bytes[self.at..self.at + N]);
        self.at += N;
        value
    }
}

use std::collections::{BTreeMap, VecDeque};

use distill_build::import::*;
use distill_build::query::{FileQuery, RootName, RootedPath};
use distill_build::trace::{CapabilityKey, Observed, RawFileFailureClass};
use distill_core::id::{AssetUuid, BundleUuid, TypeUuid};
use ngp_schema::Value;

#[derive(Default)]
struct Fs {
    reads: VecDeque<Result<(RootedPath, Vec<u8>), RawFileFailureClass>>,
    listings: VecDeque<Result<Vec<RootedPath>, RawFileFailureClass>>,
    importer: Option<[u8; 32]>,
}

impl ImportBackend for Fs {
    fn read(&mut self, _: &str) -> Result<(RootedPath, Vec<u8>), RawFileFailureClass> {
        self.reads.pop_front().unwrap()
    }
    fn probe(&mut self, _: &str) -> Result<Option<RootName>, RawFileFailureClass> {
        Ok(None)
    }
    fn enumerate(&mut self, _: &FileQuery) -> Result<Vec<RootedPath>, RawFileFailureClass> {
        self.listings.pop_front().unwrap()
    }
    fn capability(&mut self, _: &CapabilityKey) -> Option<[u8; 32]> {
        self.importer
    }
}

#[test]
fn failed_raw_operations_and_capability_misses_are_recorded() {
    let mut fs = Fs::default();
    fs.reads.push_back(Err(RawFileFailureClass::NotFound));
    fs.listings
        .push_back(Err(RawFileFailureClass::ListingFailed));
    let mut ctx = ImportContext::new("test-importer", vec![], &mut fs).unwrap();
    assert!(ctx.read("missing.png").is_err());
    let query = FileQuery::new(Some("textures".into()), None).unwrap();
    assert!(ctx.enumerate(&query).is_err());
    assert!(ctx.importer_capability("png").is_err());
    let deps = ctx.into_read_set();
    assert_eq!(deps.len(), 3);
    assert!(matches!(
        &deps[0],
        FileDep::Read {
            observed: Observed::Err(_),
            ..
        }
    ));
    assert!(matches!(
        &deps[1],
        FileDep::Listing {
            observed: Observed::Err(_),
            ..
        }
    ));
    assert!(matches!(
        &deps[2],
        FileDep::Capability {
            observed: Observed::Err(_),
            ..
        }
    ));
}

#[test]
fn successful_read_records_root_and_raw_byte_hash() {
    let rooted = RootedPath::new("assets", "a.bin").unwrap();
    let mut fs = Fs::default();
    fs.reads.push_back(Ok((rooted.clone(), b"abc".to_vec())));
    let mut ctx = ImportContext::new("test-importer", vec![rooted.clone()], &mut fs).unwrap();
    assert_eq!(ctx.sources(), &[rooted]);
    assert_eq!(ctx.read("a.bin").unwrap(), b"abc");
    match &ctx.read_set()[0] {
        FileDep::Read {
            observed: Observed::Ok(o),
            ..
        } => {
            assert_eq!(o.path, RootedPath::new("assets", "a.bin").unwrap());
            assert_eq!(o.hash, *blake3::hash(b"abc").as_bytes());
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn importer_output_rejects_reserved_and_normalized_duplicates() {
    let mut out = ImportOutput::new();
    assert!(out
        .entry("$settings", TypeUuid([1; 16]), Value::Unit)
        .is_err());
    out.entry("e\u{301}", TypeUuid([1; 16]), Value::U32(1))
        .unwrap();
    assert!(out
        .entry("\u{e9}", TypeUuid([1; 16]), Value::U32(2))
        .is_err());
    assert_eq!(out.entries()["\u{e9}"].value, Value::U32(1));
    out.primary("\u{e9}").unwrap();
    assert!(out.primary("\u{e9}").is_err());
}

struct Ids {
    assets: u8,
    bundles: u8,
}
impl IdentitySource for Ids {
    fn next_asset(&mut self) -> AssetUuid {
        self.assets += 1;
        AssetUuid([self.assets; 16])
    }
    fn next_bundle(&mut self) -> BundleUuid {
        self.bundles += 1;
        BundleUuid([self.bundles; 16])
    }
}

fn request(output: ImportOutput) -> FoldRequest {
    FoldRequest {
        output,
        explicit_settings: None,
        default_settings: Value::U32(1),
        importer: "test".into(),
        sources: vec![RootedPath::new("assets", "x.src").unwrap()],
        watch: true,
        read_set: vec![],
        origin: None,
    }
}

#[test]
fn fold_reuses_matched_ids_retires_absent_entries_and_preserves_settings() {
    let mut first = ImportOutput::new();
    first
        .entry("a", TypeUuid([1; 16]), Value::U32(1))
        .unwrap();
    first
        .entry("b", TypeUuid([1; 16]), Value::U32(2))
        .unwrap();
    first.primary("a").unwrap();
    let mut ids = Ids {
        assets: 0,
        bundles: 0,
    };
    let mut bundle = fold_import(None, request(first), &mut ids).unwrap();
    let a_id = bundle.entries["a"].uuid;
    bundle.settings = Value::U32(99);

    let mut second = ImportOutput::new();
    second
        .entry("a", TypeUuid([1; 16]), Value::U32(3))
        .unwrap();
    second
        .entry("c", TypeUuid([1; 16]), Value::U32(4))
        .unwrap();
    second.primary("c").unwrap();
    let next = fold_import(Some(&bundle), request(second), &mut ids).unwrap();
    assert_eq!(next.bundle_uuid, bundle.bundle_uuid);
    assert_eq!(next.entries["a"].uuid, a_id);
    assert!(!next.entries.contains_key("b"));
    assert_eq!(next.settings, Value::U32(99));
}

#[test]
fn vanished_prior_primary_requires_explicit_replacement() {
    let prior = ImportedBundle {
        bundle_uuid: BundleUuid([1; 16]),
        primary: Some("gone".into()),
        entries: BTreeMap::from([(
            "gone".into(),
            ImportedEntry {
                uuid: AssetUuid([2; 16]),
                type_uuid: TypeUuid([3; 16]),
                value: Value::Unit,
            },
        )]),
        settings: Value::Unit,
        record: ImportRecord::default(),
    };
    let mut out = ImportOutput::new();
    out.entry("new", TypeUuid([3; 16]), Value::Unit)
        .unwrap();
    let mut ids = Ids {
        assets: 0,
        bundles: 0,
    };
    assert!(matches!(
        fold_import(Some(&prior), request(out), &mut ids),
        Err(FoldError::PriorPrimaryVanished { .. })
    ));
}

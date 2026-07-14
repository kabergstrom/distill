use std::collections::BTreeMap;

use distill_build::dslf::{DslfError, DslfV1};
use distill_build::query::AssetQuery;
use distill_build::trace::*;
use distill_core::id::{AssetUuid, BundleFileHash, ContentHash, LogicalHash, TypeUuid};
use distill_migrate::FieldPath;
use distill_schema::ngp_schema::{LogicalSchema, SchemaNode};
use distill_store::pipeline::{AcceptedSchemaEpoch, LineageStamp};

#[derive(Default)]
struct Snapshot {
    authoring_reads: BTreeMap<AssetUuid, Observed<Option<BundleFileHash>>>,
    reads: BTreeMap<AssetUuid, Observed<ContentHash>>,
    resolves: BTreeMap<String, Observed<Option<AssetUuid>>>,
    roles: BTreeMap<AssetUuid, Observed<Option<EntryRole>>>,
    controls: BTreeMap<ControlQuery, Observed<[u8; 32]>>,
    control_reads: BTreeMap<ControlSubject, Observed<ControlValueHash>>,
}

impl TraceSource for Snapshot {
    fn authoring_read(&self, asset: AssetUuid) -> Observed<Option<BundleFileHash>> {
        self.authoring_reads
            .get(&asset)
            .cloned()
            .unwrap_or(Observed::Ok(None))
    }
    fn read(&self, asset: AssetUuid) -> Observed<ContentHash> {
        self.reads[&asset].clone()
    }
    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        self.resolves[path].clone()
    }
    fn query(&self, _: &AssetQuery) -> Observed<[u8; 32]> {
        Observed::Ok([3; 32])
    }
    fn tool(&self, _: &str) -> Observed<[u8; 32]> {
        Observed::Ok([4; 32])
    }
    fn capability(&self, _: &CapabilityKey) -> Observed<[u8; 32]> {
        Observed::Ok([5; 32])
    }
    fn ref_check(&self, _: AssetUuid, _: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(None)
    }
    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        self.roles
            .get(&asset)
            .cloned()
            .unwrap_or(Observed::Ok(None))
    }
    fn control(&self, query: &ControlQuery) -> Observed<[u8; 32]> {
        self.controls[query].clone()
    }
    fn control_read(&self, subject: &ControlSubject) -> Observed<ControlValueHash> {
        self.control_reads[subject].clone()
    }
}

#[test]
fn dstr_authoring_read_pins_tag_eleven_and_optional_bundle_identity() {
    let asset = AssetUuid([0x33; 16]);
    let hash = BundleFileHash([0x44; 32]);
    let op = TraceOp::AuthoringRead {
        asset,
        observed: Observed::Ok(Some(hash)),
    };
    let actual = trace_canonical_bytes(std::slice::from_ref(&op));

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(11); // TraceOp::AuthoringRead
    expected.extend_from_slice(&asset.0);
    expected.push(1); // Observed::Ok
    expected.push(1); // Some
    expected.extend_from_slice(&hash.0);
    assert_eq!(actual, expected);
    assert_eq!(decode_trace_canonical_bytes(&actual).unwrap(), vec![op]);
}

#[test]
fn dstr_read_success_bytes_pin_one_based_operation_and_outcome_tags() {
    let asset = AssetUuid([0x11; 16]);
    let hash = ContentHash([0x22; 32]);
    let actual = trace_canonical_bytes(&[TraceOp::Read {
        asset,
        observed: Observed::Ok(hash),
    }]);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(1); // TraceOp::Read
    expected.extend_from_slice(&asset.0);
    expected.push(1); // Observed::Ok
    expected.extend_from_slice(&hash.0);
    assert_eq!(actual, expected);
    assert_eq!(
        decode_trace_canonical_bytes(&actual).unwrap(),
        vec![TraceOp::Read {
            asset,
            observed: Observed::Ok(hash),
        }]
    );
    assert_eq!(
        decode_trace_payload_bytes(&trace_payload_bytes(&[TraceOp::Read {
            asset,
            observed: Observed::Ok(hash),
        }]))
        .unwrap(),
        vec![TraceOp::Read {
            asset,
            observed: Observed::Ok(hash),
        }]
    );
}

#[test]
fn trace_digest_applies_the_dstr_domain_exactly_once() {
    let trace = [TraceOp::Resolve {
        path: "asset.bundle".into(),
        observed: Observed::Ok(None),
    }];
    assert_eq!(
        trace_digest(&trace),
        *blake3::hash(&trace_canonical_bytes(&trace)).as_bytes()
    );
    assert_eq!(
        trace_digest(&trace),
        distill_core::canonical::domain_digest(distill_core::canonical::DSTR, 1, |encoder| encoder
            .raw(&trace_payload_bytes(&trace)),)
    );
}

#[test]
fn dstr_distinguishes_an_explicit_runtime_role_and_rejects_authoring_queries() {
    let implicit = TraceOp::Query {
        query: Box::new(AssetQuery {
            uuid: Some(AssetUuid([9; 16])),
            ..AssetQuery::default()
        }),
        observed: Observed::Ok([3; 32]),
    };
    let explicit = TraceOp::Query {
        query: Box::new(AssetQuery {
            uuid: Some(AssetUuid([9; 16])),
            authoring_only: Some(false),
            ..AssetQuery::default()
        }),
        observed: Observed::Ok([3; 32]),
    };
    let implicit_bytes = trace_payload_bytes(std::slice::from_ref(&implicit));
    let explicit_bytes = trace_payload_bytes(std::slice::from_ref(&explicit));
    assert_ne!(implicit_bytes, explicit_bytes);
    assert_eq!(
        decode_trace_payload_bytes(&explicit_bytes),
        Ok(vec![explicit])
    );

    let forbidden = TraceOp::Query {
        query: Box::new(AssetQuery {
            uuid: Some(AssetUuid([9; 16])),
            authoring_only: Some(true),
            ..AssetQuery::default()
        }),
        observed: Observed::Ok([3; 32]),
    };
    assert_eq!(
        decode_trace_payload_bytes(&trace_payload_bytes(&[forbidden])),
        Err(TraceDecodeError::InvalidQuery)
    );
}

#[test]
fn persisted_trace_decoder_rejects_reserved_noncanonical_and_trailing_forms() {
    assert_eq!(
        decode_trace_payload_bytes(&[1, 0, 0, 0, 4]),
        Err(TraceDecodeError::ReservedToolLaunch)
    );

    let failure = TraceOp::Read {
        asset: AssetUuid([1; 16]),
        observed: Observed::Err(StableFailureFingerprint::Ambiguous {
            conflicting: vec![AssetUuid([2; 16]), AssetUuid([1; 16])],
        }),
    };
    let mut bytes = trace_payload_bytes(&[failure]);
    // Encoding canonicalizes the set. Make its second member equal to its
    // first so the persisted form is no longer strict sorted/deduplicated.
    let second = bytes.len() - 16;
    let first = second - 16;
    let copy = bytes[first..second].to_vec();
    bytes[second..].copy_from_slice(&copy);
    assert_eq!(
        decode_trace_payload_bytes(&bytes),
        Err(TraceDecodeError::NonCanonicalSet)
    );

    let mut trailing = trace_payload_bytes(&[]);
    trailing.push(0);
    assert_eq!(
        decode_trace_payload_bytes(&trailing),
        Err(TraceDecodeError::TrailingBytes)
    );
}

#[test]
fn dstr_tool_capability_miss_pins_typed_tool_key() {
    let id = "shaderc".to_owned();
    let actual = trace_canonical_bytes(&[TraceOp::Tool {
        id: id.clone(),
        observed: Observed::Err(StableFailureFingerprint::MissingCapability {
            key: CapabilityKey::Tool(id.clone()),
        }),
    }]);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1); // domain version
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(10); // TraceOp::Tool; historical ToolLaunch tag 4 is reserved
    expected.extend_from_slice(&(id.len() as u32).to_le_bytes());
    expected.extend_from_slice(id.as_bytes());
    expected.push(2); // Observed::Err
    expected.push(6); // StableFailureFingerprint::MissingCapability
    expected.push(5); // CapabilityKey::Tool
    expected.extend_from_slice(&(id.len() as u32).to_le_bytes());
    expected.extend_from_slice(id.as_bytes());
    assert_eq!(actual, expected);
}

#[test]
fn dstr_tool_lookup_hit_records_the_exact_staged_hash() {
    let id = "shaderc".to_owned();
    let staged_hash = [0x5a; 32];
    let actual = trace_canonical_bytes(&[TraceOp::Tool {
        id: id.clone(),
        observed: Observed::Ok(staged_hash),
    }]);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(10);
    expected.extend_from_slice(&(id.len() as u32).to_le_bytes());
    expected.extend_from_slice(id.as_bytes());
    expected.push(1);
    expected.extend_from_slice(&staged_hash);
    assert_eq!(actual, expected);
}

#[test]
fn revalidation_compares_labeled_outcomes_in_order() {
    let asset = AssetUuid([1; 16]);
    let hash = ContentHash([2; 32]);
    let trace = vec![
        TraceOp::Resolve {
            path: "a".into(),
            observed: Observed::Ok(None),
        },
        TraceOp::Read {
            asset,
            observed: Observed::Ok(hash),
        },
    ];
    let mut snapshot = Snapshot::default();
    snapshot.resolves.insert("a".into(), Observed::Ok(None));
    snapshot.reads.insert(asset, Observed::Ok(hash));
    assert!(revalidate(&trace, &snapshot));
    snapshot
        .resolves
        .insert("a".into(), Observed::Ok(Some(asset)));
    assert!(!revalidate(&trace, &snapshot));
    assert_ne!(
        trace_digest(&trace),
        trace_digest(&[trace[1].clone(), trace[0].clone()])
    );
}

#[test]
fn stable_failure_heals_when_observed_outcome_changes() {
    let asset = AssetUuid([8; 16]);
    let missing = StableFailureFingerprint::MissingCapability {
        key: CapabilityKey::Processor {
            input: TypeUuid([9; 16]),
        },
    };
    let trace = vec![TraceOp::Read {
        asset,
        observed: Observed::Err(missing.clone()),
    }];
    let mut snapshot = Snapshot::default();
    snapshot.reads.insert(asset, Observed::Err(missing));
    assert!(revalidate(&trace, &snapshot));
    snapshot
        .reads
        .insert(asset, Observed::Ok(ContentHash([1; 32])));
    assert!(!revalidate(&trace, &snapshot));
}

#[test]
fn failure_cause_grammar_is_checked() {
    let local = StableFailureFingerprint::Local {
        class: LocalFailureClass::Validator,
        detail: [7; 32],
    };
    assert!(FailureRecord {
        trace: vec![],
        cause: FailureCause::Local(local)
    }
    .validate()
    .is_ok());
    assert!(FailureRecord {
        trace: vec![],
        cause: FailureCause::Op
    }
    .validate()
    .is_err());
    let op = TraceOp::Tool {
        id: "shaderc".into(),
        observed: Observed::Err(StableFailureFingerprint::MissingCapability {
            key: CapabilityKey::Tool("shaderc".into()),
        }),
    };
    assert!(FailureRecord {
        trace: vec![op],
        cause: FailureCause::Op
    }
    .validate()
    .is_ok());
}

#[test]
fn local_failure_details_are_dslf_domain_separated_typed_facts() {
    let facts = DslfV1::Validator {
        asset: AssetUuid([1; 16]),
        type_uuid: TypeUuid([2; 16]),
        error_paths: vec![FieldPath::of(&["field"])],
    };
    let a = local_failure_fingerprint(&facts).unwrap();
    let b = local_failure_fingerprint(&facts).unwrap();
    let changed = local_failure_fingerprint(&DslfV1::Validator {
        asset: AssetUuid([1; 16]),
        type_uuid: TypeUuid([2; 16]),
        error_paths: vec![FieldPath::of(&["other"])],
    })
    .unwrap();
    assert_eq!(a, b);
    assert_ne!(a, changed);
    let StableFailureFingerprint::Local { detail, .. } = a else {
        unreachable!()
    };
    assert_ne!(detail, *blake3::hash(b"field").as_bytes());
}

#[test]
fn dslf_processor_bytes_pin_class_width_field_order_and_framing() {
    let facts = DslfV1::Processor {
        asset: AssetUuid([0x11; 16]),
        processor_id: "p".into(),
        processor_version: 0x0403_0201,
        stage: 0x0605,
        build_error_code: 0x0a09_0807,
    };
    let StableFailureFingerprint::Local { class, detail } =
        local_failure_fingerprint(&facts).unwrap()
    else {
        unreachable!()
    };
    assert_eq!(class, LocalFailureClass::Processor);

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"DSLF");
    bytes.push(1);
    bytes.extend_from_slice(&3u16.to_le_bytes());
    bytes.extend_from_slice(&[0x11; 16]);
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.push(b'p');
    bytes.extend_from_slice(&0x0403_0201u32.to_le_bytes());
    bytes.extend_from_slice(&0x0605u16.to_le_bytes());
    bytes.extend_from_slice(&0x0a09_0807u32.to_le_bytes());
    assert_eq!(detail, *blake3::hash(&bytes).as_bytes());
}

#[test]
fn dslf_canonicalizes_declared_sets_but_retains_validator_multiplicity() {
    let a = AssetUuid([1; 16]);
    let b = AssetUuid([2; 16]);
    let migration = |edges| DslfV1::MigrationPlan {
        type_uuid: TypeUuid([3; 16]),
        from: LogicalHash([4; 32]),
        to: LogicalHash([5; 32]),
        failure: distill_build::dslf::MigrationPlanFailureV1::AmbiguousEdge {
            conflicting_edges: edges,
        },
    };
    assert_eq!(
        migration(vec![b, a, b]).digest().unwrap(),
        migration(vec![a, b]).digest().unwrap()
    );

    let validator = |paths| DslfV1::Validator {
        asset: a,
        type_uuid: TypeUuid([6; 16]),
        error_paths: paths,
    };
    let one = FieldPath::of(&["a"]);
    let two = FieldPath::of(&["b"]);
    assert_eq!(
        validator(vec![two.clone(), one.clone()]).digest().unwrap(),
        validator(vec![one.clone(), two]).digest().unwrap()
    );
    assert_ne!(
        validator(vec![one.clone(), one.clone()]).digest().unwrap(),
        validator(vec![one]).digest().unwrap()
    );
}

#[test]
fn dslf_rejects_duplicate_importer_sources() {
    let source = distill_build::query::RootedPath::new("assets", "same.src").unwrap();
    let facts = DslfV1::Importer {
        importer_id: "gltf".into(),
        importer_error_code: 7,
        sources: vec![source.clone(), source.clone()],
    };
    assert_eq!(
        facts.digest(),
        Err(DslfError::DuplicateImporterSource(source))
    );
}

#[test]
fn authoring_only_role_failures_are_distinct_from_missing_and_heal_on_role_change() {
    let asset = AssetUuid([12; 16]);
    let failure = StableFailureFingerprint::RoleIneligible {
        asset,
        observed_role: EntryRole::AuthoringOnly,
    };
    let trace = vec![TraceOp::RoleCheck {
        asset,
        observed: Observed::Err(failure.clone()),
    }];
    let mut snapshot = Snapshot::default();
    snapshot.roles.insert(asset, Observed::Err(failure.clone()));
    assert!(revalidate(&trace, &snapshot));

    snapshot
        .roles
        .insert(asset, Observed::Ok(Some(EntryRole::Runtime)));
    assert!(!revalidate(&trace, &snapshot));
    assert_ne!(
        trace_digest(&trace),
        trace_digest(&[TraceOp::RoleCheck {
            asset,
            observed: Observed::Ok(None),
        }]),
        "role-ineligible is never encoded as an ordinary miss"
    );
}

#[test]
fn every_dslf_local_failure_class_has_a_stable_nonzero_code() {
    let classes = [
        LocalFailureClass::Validator,
        LocalFailureClass::MigrationPlan,
        LocalFailureClass::Processor,
        LocalFailureClass::MigrationFunction,
        LocalFailureClass::OutputBinding,
        LocalFailureClass::Importer,
        LocalFailureClass::ImportIntake,
        LocalFailureClass::ArtifactEncoding,
    ];
    assert_eq!(classes.map(|class| class as u16), [1, 2, 3, 4, 5, 6, 7, 8]);
}

fn migration_query() -> ControlQuery {
    ControlQuery::MigrationEdges {
        type_uuid: TypeUuid([10; 16]),
        from_hash: LogicalHash([11; 32]),
    }
}

#[test]
fn dstr_control_failure_bytes_pin_tags_u16_codes_and_framing() {
    let query = migration_query();
    let a = AssetUuid([1; 16]);
    let b = AssetUuid([2; 16]);
    let failure = control_failure_fingerprint(
        ControlFailureSubject::Query(query.clone()),
        ControlFailureCode::Ambiguous,
        vec![b, a, b],
    )
    .unwrap();
    let actual = trace_canonical_bytes(&[TraceOp::Control {
        query,
        observed: Observed::Err(failure),
    }]);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1); // domain version
    expected.extend_from_slice(&1_u32.to_le_bytes()); // trace entry count
    expected.push(8); // TraceOp::Control
    expected.push(1); // ControlQuery::MigrationEdges
    expected.extend_from_slice(&[10; 16]);
    expected.extend_from_slice(&[11; 32]);
    expected.push(2); // Observed::Err
    expected.push(9); // StableFailureFingerprint::Control
    expected.push(1); // ControlFailureSubject::Query
    expected.push(1); // ControlQuery::MigrationEdges
    expected.extend_from_slice(&[10; 16]);
    expected.extend_from_slice(&[11; 32]);
    expected.extend_from_slice(&3_u16.to_le_bytes()); // Ambiguous
    expected.extend_from_slice(&2_u32.to_le_bytes()); // sorted/dedup entries
    expected.extend_from_slice(&a.0);
    expected.extend_from_slice(&b.0);
    assert_eq!(actual, expected);
}

#[test]
fn dstr_control_read_failure_pins_read_subject_tag() {
    let asset = AssetUuid([21; 16]);
    let subject = ControlSubject::Migration(asset);
    let failure = control_failure_fingerprint(
        ControlFailureSubject::Read(subject.clone()),
        ControlFailureCode::Missing,
        Vec::new(),
    )
    .unwrap();
    let actual = trace_canonical_bytes(&[TraceOp::ControlRead {
        subject,
        observed: Observed::Err(failure),
    }]);

    let mut expected = Vec::new();
    expected.extend_from_slice(b"DSTR");
    expected.push(1);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.push(9); // TraceOp::ControlRead
    expected.push(1); // ControlSubject::Migration
    expected.extend_from_slice(&asset.0);
    expected.push(2); // Observed::Err
    expected.push(9); // StableFailureFingerprint::Control
    expected.push(2); // ControlFailureSubject::Read
    expected.push(1); // ControlSubject::Migration
    expected.extend_from_slice(&asset.0);
    expected.extend_from_slice(&2_u16.to_le_bytes()); // Missing
    expected.extend_from_slice(&0_u32.to_le_bytes()); // no conflicting entries
    assert_eq!(actual, expected);
}

#[test]
fn every_control_failure_code_has_its_pinned_nonzero_u16_value() {
    let codes = [
        ControlFailureCode::RoleViolation,
        ControlFailureCode::Missing,
        ControlFailureCode::Ambiguous,
        ControlFailureCode::Poisoned,
        ControlFailureCode::Malformed,
        ControlFailureCode::WrongBuiltInType,
        ControlFailureCode::WrongRole,
        ControlFailureCode::UnsupportedFormat,
        ControlFailureCode::SchemaClosure,
    ];
    assert_eq!(codes.map(|code| code as u16), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn control_failure_entries_are_sorted_and_deduplicated_before_revalidation() {
    let a = AssetUuid([1; 16]);
    let b = AssetUuid([2; 16]);
    let entries = ControlFailureEntries::new(vec![b, a, b]);
    assert_eq!(entries.as_slice(), &[a, b]);

    let left = control_failure_fingerprint(
        ControlFailureSubject::Query(ControlQuery::DirectoryImportRuleSet),
        ControlFailureCode::Ambiguous,
        vec![b, a, b],
    )
    .unwrap();
    let right = control_failure_fingerprint(
        ControlFailureSubject::Query(ControlQuery::DirectoryImportRuleSet),
        ControlFailureCode::Ambiguous,
        vec![a, b],
    )
    .unwrap();
    assert_eq!(left, right);
}

#[test]
fn successful_and_failed_control_reads_revalidate_and_heal() {
    let asset = AssetUuid([12; 16]);
    let subject = ControlSubject::Migration(asset);
    let identity = ControlValueHash([13; 32]);
    let success = TraceOp::ControlRead {
        subject: subject.clone(),
        observed: Observed::Ok(identity),
    };
    let mut snapshot = Snapshot::default();
    snapshot
        .control_reads
        .insert(subject.clone(), Observed::Ok(identity));
    assert!(revalidate(std::slice::from_ref(&success), &snapshot));
    snapshot
        .control_reads
        .insert(subject.clone(), Observed::Ok(ControlValueHash([14; 32])));
    assert!(!revalidate(std::slice::from_ref(&success), &snapshot));

    let failure = control_failure_fingerprint(
        ControlFailureSubject::Read(subject.clone()),
        ControlFailureCode::Malformed,
        Vec::new(),
    )
    .unwrap();
    let failed = TraceOp::ControlRead {
        subject: subject.clone(),
        observed: Observed::Err(failure.clone()),
    };
    snapshot
        .control_reads
        .insert(subject.clone(), Observed::Err(failure));
    assert!(revalidate(std::slice::from_ref(&failed), &snapshot));
    snapshot
        .control_reads
        .insert(subject, Observed::Ok(identity));
    assert!(!revalidate(std::slice::from_ref(&failed), &snapshot));
}

#[test]
fn unreadable_migration_edge_is_terminal_and_never_an_empty_result() {
    let asset = AssetUuid([15; 16]);
    let subject = ControlSubject::Migration(asset);
    let mut basis = AttemptedControlBasis::new();
    assert_eq!(
        basis.query(migration_query(), Observed::Ok([16; 32])),
        Ok([16; 32])
    );

    let unreadable = control_failure_fingerprint(
        ControlFailureSubject::Read(subject.clone()),
        ControlFailureCode::Malformed,
        Vec::new(),
    )
    .unwrap();
    assert!(matches!(
        basis.read(subject.clone(), Observed::Err(unreadable)),
        Err(AttemptedControlBasisError::ObservedFailure(_))
    ));
    assert!(basis.is_stopped());
    assert!(basis.trace().last().is_some_and(TraceOp::failed));
    assert_eq!(
        basis.read(
            subject,
            Observed::Ok(DecodedControlValue {
                identity: ControlValueHash([17; 32]),
                value: ControlValue::PackDefinition(PackDefinitionControlValue {
                    roots: vec![],
                    target: "unused-after-hard-stop".into(),
                    zstd_level: 0,
                    include_path_table: false,
                }),
            }),
        ),
        Err(AttemptedControlBasisError::HardStopped)
    );
    let trace = basis.into_trace().unwrap();
    assert_eq!(
        trace.len(),
        2,
        "the failed read remains in the attempted basis"
    );
}

#[test]
fn migration_control_reads_return_the_closed_fully_decoded_value() {
    let asset = AssetUuid([31; 16]);
    let from = LogicalHash([32; 32]);
    let to = LogicalHash([33; 32]);
    let lineage = |digest| LineageStamp {
        epochs: vec![AcceptedSchemaEpoch {
            digest,
            forward_parent: None,
        }],
        cursor: 0,
        chain: [34; 32],
    };
    let value = MigrationControlValue {
        asset,
        target_type_uuid: TypeUuid([35; 16]),
        from_hash: from,
        to_hash: to,
        from_schema: LogicalSchema {
            root: SchemaNode::Unit,
        },
        to_schema: LogicalSchema {
            root: SchemaNode::String,
        },
        from_lineage: lineage(from),
        to_lineage: lineage(to),
        kind: MigrationControlKind::Function {
            key: "upgrade".into(),
        },
    };
    let mut basis = AttemptedControlBasis::new();
    basis
        .query(migration_query(), Observed::Ok([36; 32]))
        .unwrap();
    assert_eq!(
        basis
            .read(
                ControlSubject::Migration(asset),
                Observed::Ok(DecodedControlValue {
                    identity: ControlValueHash([37; 32]),
                    value: ControlValue::Migration(Box::new(value.clone())),
                }),
            )
            .unwrap(),
        ControlValue::Migration(Box::new(value))
    );
    assert_eq!(basis.trace().len(), 2);
}

#[test]
fn a_control_value_with_the_wrong_brand_is_a_terminal_typed_failure() {
    let asset = AssetUuid([41; 16]);
    let mut basis = AttemptedControlBasis::new();
    basis
        .query(migration_query(), Observed::Ok([42; 32]))
        .unwrap();
    let error = basis
        .read(
            ControlSubject::Migration(asset),
            Observed::Ok(DecodedControlValue {
                identity: ControlValueHash([43; 32]),
                value: ControlValue::PackDefinition(PackDefinitionControlValue {
                    roots: vec![],
                    target: "wrong-brand".into(),
                    zstd_level: 0,
                    include_path_table: false,
                }),
            }),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        AttemptedControlBasisError::ObservedFailure(StableFailureFingerprint::Control(_))
    ));
    assert!(basis.is_stopped());
}

#[test]
fn directory_rule_enumeration_is_distinct_control_trace_data() {
    let empty = [0; 32];
    let migrations = TraceOp::Control {
        query: migration_query(),
        observed: Observed::Ok(empty),
    };
    let directory_rules = TraceOp::Control {
        query: ControlQuery::DirectoryImportRuleSet,
        observed: Observed::Ok(empty),
    };
    assert_ne!(
        trace_canonical_bytes(&[migrations]),
        trace_canonical_bytes(&[directory_rules])
    );
}

#[test]
fn control_failure_entry_cardinality_is_closed_by_code() {
    let subject = ControlFailureSubject::Query(ControlQuery::DirectoryImportRuleSet);
    let a = AssetUuid([1; 16]);
    let b = AssetUuid([2; 16]);
    assert_eq!(
        control_failure_fingerprint(
            subject.clone(),
            ControlFailureCode::RoleViolation,
            Vec::new(),
        ),
        Err(ControlFailureError::EntriesRequired {
            code: ControlFailureCode::RoleViolation,
        })
    );
    assert_eq!(
        control_failure_fingerprint(subject.clone(), ControlFailureCode::Ambiguous, vec![a]),
        Err(ControlFailureError::AmbiguousNeedsTwoEntries { observed: 1 })
    );
    assert_eq!(
        control_failure_fingerprint(subject, ControlFailureCode::Missing, vec![a, b]),
        Err(ControlFailureError::EntriesForbidden {
            code: ControlFailureCode::Missing,
        })
    );
}

#[test]
fn load_inputs_preserves_one_interleaved_trace_and_terminal_failure_prefix() {
    let asset = AssetUuid([51; 16]);
    let subject = ControlSubject::Migration(asset);
    let mut inputs = LoadInputs::new(7);
    inputs
        .record(TraceOp::Control {
            query: migration_query(),
            observed: Observed::Ok([52; 32]),
        })
        .unwrap();
    inputs
        .record(TraceOp::ControlRead {
            subject: subject.clone(),
            observed: Observed::Ok(ControlValueHash([53; 32])),
        })
        .unwrap();
    inputs
        .record(TraceOp::Capability {
            key: CapabilityKey::MigrationFn("edge".into()),
            observed: Observed::Ok([54; 32]),
        })
        .unwrap();
    inputs
        .record(TraceOp::Control {
            query: ControlQuery::DirectoryImportRuleSet,
            observed: Observed::Ok([55; 32]),
        })
        .unwrap();
    assert!(matches!(inputs.trace()[0], TraceOp::Control { .. }));
    assert!(matches!(inputs.trace()[1], TraceOp::ControlRead { .. }));
    assert!(matches!(inputs.trace()[2], TraceOp::Capability { .. }));
    assert!(matches!(inputs.trace()[3], TraceOp::Control { .. }));
    assert_eq!(inputs.control_queries().count(), 2);
    assert_eq!(inputs.control_reads().count(), 1);
    assert_eq!(inputs.capabilities().count(), 1);
    inputs.validate().unwrap();

    let failure = control_failure_fingerprint(
        ControlFailureSubject::Read(subject.clone()),
        ControlFailureCode::Malformed,
        Vec::new(),
    )
    .unwrap();
    inputs
        .record(TraceOp::ControlRead {
            subject,
            observed: Observed::Err(failure),
        })
        .unwrap();
    assert_eq!(
        inputs.record(TraceOp::Capability {
            key: CapabilityKey::DefaultTable(TypeUuid([56; 16])),
            observed: Observed::Ok([57; 32]),
        }),
        Err(LoadInputsError::TerminalFailureAlreadyRecorded)
    );
    inputs.validate().unwrap();
}

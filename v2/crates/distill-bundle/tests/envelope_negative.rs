//! §6 envelope negatives: schema closure, snapshot verification, primary
//! resolution, the reserved `$` namespace, id/hash parsing, key strictness,
//! and encoding choice — every defect named precisely.

mod common;

use common::*;
use distill_bundle::{
    extract_namespace_skeleton, parse_bundle, repair_missing_schemas, write_bundle, Bundle,
    BundleError as E, EntryLineageV1,
};
use distill_core::bootstrap::{
    BootstrapControlSpecV1, BootstrapControlSymbol, BOOTSTRAP_CONTROL_TYPE_UUIDS,
};
use distill_core::id::TypeUuid;
use distill_core::lineage::{lineage_chain_digest, AcceptedSchemaEpoch};
use distill_json::AuthoredValue as V;
use ngp_schema::{SchemaNode as N, SnapshotError};
use std::collections::BTreeMap;

fn valid_plain() -> (Vec<u8>, Bundle) {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        Some("a"),
    );
    (write_bundle(&b).unwrap(), b)
}

fn byte_array(bytes: &[u8]) -> V {
    V::Array(
        bytes
            .iter()
            .map(|byte| V::UInt(u128::from(*byte)))
            .collect(),
    )
}

fn migration_endpoint_stamp(type_uuid: TypeUuid, hash: distill_core::id::LogicalHash) -> V {
    let epochs = vec![AcceptedSchemaEpoch {
        digest: hash,
        forward_parent: None,
    }];
    obj(&[
        (
            "chain",
            byte_array(&lineage_chain_digest(type_uuid, &epochs, 0)),
        ),
        ("cursor", u(0)),
        (
            "epochs",
            arr(vec![obj(&[
                ("digest", byte_array(&hash.0)),
                ("forward_parent", V::Null),
            ])]),
        ),
    ])
}

fn migration_bundle() -> (Bundle, distill_core::id::LogicalHash) {
    let from_schema = simple_schema();
    let to_schema = schema(st(&[(
        "value",
        N::Primitive(ngp_schema::PrimitiveKind::U8),
    )]));
    let from_hash = lh(&from_schema);
    let to_hash = lh(&to_schema);
    let target_type = TypeUuid([0x31; 16]);
    let row = BootstrapControlSpecV1::embedded()
        .unwrap()
        .0
        .into_iter()
        .find(|row| row.symbol == BootstrapControlSymbol::Migration)
        .unwrap();
    let migration_schema = ngp_schema::node_from_bytes(&row.logical_schema).unwrap();
    let data = obj(&[
        ("from_hash", byte_array(&from_hash.0)),
        (
            "from_lineage",
            migration_endpoint_stamp(target_type, from_hash),
        ),
        ("kind", obj(&[("Ops", obj(&[("ops", arr(Vec::new()))]))])),
        ("target_type_uuid", byte_array(&target_type.0)),
        ("to_hash", byte_array(&to_hash.0)),
        ("to_lineage", migration_endpoint_stamp(target_type, to_hash)),
    ]);
    (
        Bundle {
            format_version: 1,
            uuid: BUNDLE_UUID.parse().unwrap(),
            primary: None,
            schemas: BTreeMap::from([
                (from_hash, from_schema),
                (to_hash, to_schema),
                (row.logical_hash, migration_schema),
            ]),
            assets: BTreeMap::from([(
                "migration".to_owned(),
                distill_bundle::AssetEntry {
                    uuid: UUID_A.parse().unwrap(),
                    type_uuid: row.type_uuid,
                    schema_hash: row.logical_hash,
                    lineage: EntryLineageV1::Bootstrap {
                        bundle_format_version: 1,
                    },
                    authoring_only: true,
                    data,
                },
            )]),
        },
        from_hash,
    )
}

// ---- schema closure & snapshot verification ----

#[test]
fn schema_closure_missing_hash() {
    let (plain, b) = valid_plain();
    let want = b.assets["a"].schema_hash;
    let bytes = mutate_envelope(&plain, |env| {
        *as_obj(env).get_mut("schemas").unwrap() = obj(&[]);
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::MissingSchema { local_id, schema_hash }
            if local_id == "a" && *schema_hash == want),
        "got {err:?}"
    );
}

#[test]
fn exact_held_schema_repairs_plain_bundle_canonically() {
    let (plain, bundle) = valid_plain();
    let schema_hash = bundle.assets["a"].schema_hash;
    let missing = mutate_envelope(&plain, |env| {
        *as_obj(env).get_mut("schemas").unwrap() = obj(&[]);
    });
    let holders = BTreeMap::from([(schema_hash, bundle.schemas[&schema_hash].clone())]);

    let repaired = repair_missing_schemas(&missing, &holders)
        .unwrap()
        .expect("missing schema is repairable");
    assert_eq!(repaired, plain);
    assert_eq!(parse_bundle(&repaired).unwrap(), bundle);
}

#[test]
fn migration_endpoint_hashes_are_required_and_exactly_repairable() {
    let (bundle, missing_hash) = migration_bundle();
    let canonical = write_bundle(&bundle).unwrap();
    let missing = mutate_envelope(&canonical, |env| {
        as_obj(as_obj(env).get_mut("schemas").unwrap()).remove(&missing_hash.to_string());
    });

    assert!(matches!(
        parse_bundle(&missing),
        Err(E::MissingSchema {
            local_id,
            schema_hash,
        }) if local_id == "migration" && schema_hash == missing_hash
    ));
    let holders = BTreeMap::from([(missing_hash, bundle.schemas[&missing_hash].clone())]);
    assert_eq!(
        repair_missing_schemas(&missing, &holders).unwrap(),
        Some(canonical)
    );

    let mut writer_missing = bundle;
    writer_missing.schemas.remove(&missing_hash);
    assert!(matches!(
        write_bundle(&writer_missing),
        Err(E::MissingSchema {
            local_id,
            schema_hash,
        }) if local_id == "migration" && schema_hash == missing_hash
    ));
}

#[test]
fn exact_held_schema_repairs_an_omitted_schema_table() {
    let (plain, bundle) = valid_plain();
    let schema_hash = bundle.assets["a"].schema_hash;
    let missing = mutate_envelope(&plain, |env| {
        as_obj(env).remove("schemas");
    });
    let holders = BTreeMap::from([(schema_hash, bundle.schemas[&schema_hash].clone())]);

    assert_eq!(
        repair_missing_schemas(&missing, &holders).unwrap(),
        Some(plain)
    );
}

#[test]
fn absent_exact_holder_never_guesses_schema() {
    let (plain, bundle) = valid_plain();
    let schema_hash = bundle.assets["a"].schema_hash;
    let missing = mutate_envelope(&plain, |env| {
        *as_obj(env).get_mut("schemas").unwrap() = obj(&[]);
    });

    let error = repair_missing_schemas(&missing, &BTreeMap::new()).unwrap_err();
    assert!(matches!(
        error,
        E::MissingSchema {
            local_id,
            schema_hash: missing_hash
        } if local_id == "a" && missing_hash == schema_hash
    ));
}

#[test]
fn exact_held_schema_repairs_container_without_changing_blob_data() {
    let schema = schema(st(&[("payload", N::Blob)]));
    let bundle = bundle(
        &[&schema],
        vec![(
            "a",
            entry(UUID_A, &schema, obj(&[("payload", blob(b"payload"))])),
        )],
        None,
    );
    let canonical = write_bundle(&bundle).unwrap();
    let (json, _, chunk) = container_parts(&canonical);
    let missing_json = mutate_envelope(&json, |env| {
        *as_obj(env).get_mut("schemas").unwrap() = obj(&[]);
    });
    let missing = build_container(&missing_json, &chunk);
    let schema_hash = bundle.assets["a"].schema_hash;
    let holders = BTreeMap::from([(schema_hash, bundle.schemas[&schema_hash].clone())]);

    let repaired = repair_missing_schemas(&missing, &holders)
        .unwrap()
        .expect("container schema is repairable");
    assert_eq!(repaired, canonical);
    assert_eq!(parse_bundle(&repaired).unwrap(), bundle);
}

#[test]
fn schemas_key_must_equal_snapshot_hash() {
    let (plain, b) = valid_plain();
    let real = b.assets["a"].schema_hash;
    let fake = "00000000000000000000000000000000000000000000000000000000000000aa";
    let bytes = mutate_envelope(&plain, |env| {
        let schemas = as_obj(as_obj(env).get_mut("schemas").unwrap());
        let snapshot = schemas.remove(&real.to_string()).unwrap();
        schemas.insert(fake.to_string(), snapshot);
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::Schema { hash, error: SnapshotError::HashMismatch { expected, actual } }
            if hash.to_string() == fake && expected.to_string() == fake && *actual == real),
        "error must name expected and actual hashes, got {err:?}"
    );
}

#[test]
fn primary_must_name_an_existing_entry() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("primary".to_string(), s("nope"));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::PrimaryNotFound { primary } if primary == "nope"),
        "got {err:?}"
    );
}

#[test]
fn invalid_primary_still_yields_the_current_namespace_skeleton() {
    let (plain, bundle) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("primary".to_string(), s("nope"));
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::PrimaryNotFound { .. })
    ));

    let skeleton = extract_namespace_skeleton(&bytes).unwrap();
    assert_eq!(skeleton.uuid, bundle.uuid);
    assert_eq!(skeleton.assets, bundle.assets);
}

#[test]
fn authoring_only_is_mandatory_typed_and_reserved_entries_require_it() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("x"))])),
        )],
        None,
    );
    let bytes = write_bundle(&b).unwrap();

    let missing = mutate_envelope(&bytes, |env| {
        env_entry(env, "a").remove("authoring_only");
    });
    assert!(matches!(
        parse_bundle(&missing),
        Err(E::MissingEntryKey {
            key: "authoring_only",
            ..
        })
    ));

    let wrong = mutate_envelope(&bytes, |env| {
        env_entry(env, "a").insert("authoring_only".into(), s("false"));
    });
    assert!(matches!(
        parse_bundle(&wrong),
        Err(E::AuthoringOnlyNotBool { .. })
    ));

    let reserved = mutate_envelope(&bytes, |env| {
        let assets = as_obj(as_obj(env).get_mut("assets").unwrap());
        let entry = assets.remove("a").unwrap();
        assets.insert("$settings".into(), entry);
    });
    assert!(matches!(
        parse_bundle(&reserved),
        Err(E::ReservedEntryMustBeAuthoringOnly { .. })
    ));
}

// ---- entry lineage ----

#[test]
fn lineage_is_mandatory() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        env_entry(env, "a").remove("lineage");
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::MissingEntryKey {
            ref local_id,
            key: "lineage"
        }) if local_id == "a"
    ));
}

#[test]
fn manifest_lineage_chain_is_verified() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        let lineage = env_entry(env, "a").get_mut("lineage").unwrap();
        let manifest = as_obj(as_obj(lineage).get_mut("manifest").unwrap());
        manifest.insert(
            "chain".into(),
            s("0000000000000000000000000000000000000000000000000000000000000000"),
        );
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "a"
    ));
}

#[test]
fn non_bootstrap_type_cannot_use_bootstrap_lineage() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        env_entry(env, "a").insert(
            "lineage".into(),
            obj(&[("bootstrap", obj(&[("bundle_format_version", u(1))]))]),
        );
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "a"
    ));
}

#[test]
fn bootstrap_type_cannot_use_manifest_lineage() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        env_entry(env, "a").insert(
            "type_uuid".into(),
            s(&BOOTSTRAP_CONTROL_TYPE_UUIDS[0].to_string()),
        );
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "a"
    ));
}

#[test]
fn writer_rejects_invalid_in_memory_lineage() {
    let (_, mut bundle) = valid_plain();
    bundle.assets.get_mut("a").unwrap().lineage = EntryLineageV1::Bootstrap {
        bundle_format_version: 1,
    };
    assert!(matches!(
        write_bundle(&bundle),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "a"
    ));
}

#[test]
fn manifest_lineage_rejects_duplicate_epoch_digests() {
    let (_, mut bundle) = valid_plain();
    let entry = bundle.assets.get_mut("a").unwrap();
    let EntryLineageV1::Manifest(stamp) = &mut entry.lineage else {
        panic!("fixture must use manifest lineage");
    };
    stamp.epochs.push(AcceptedSchemaEpoch {
        digest: entry.schema_hash,
        forward_parent: Some(0),
    });
    stamp.cursor = 1;
    stamp.chain = lineage_chain_digest(entry.type_uuid, &stamp.epochs, stamp.cursor);
    assert!(matches!(
        write_bundle(&bundle),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "a"
    ));
}

// ---- reserved local_ids ----

#[test]
fn bogus_dollar_local_id_rejected_on_parse() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        let assets = as_obj(as_obj(env).get_mut("assets").unwrap());
        let e = assets.remove("a").unwrap();
        assets.insert("$bogus".to_string(), e);
        as_obj(env).remove("primary");
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::ReservedLocalId { local_id } if local_id == "$bogus"),
        "got {err:?}"
    );
}

#[test]
fn bogus_dollar_local_id_rejected_on_write() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "$bogus",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::ReservedLocalId { local_id } if local_id == "$bogus"),
        "got {err:?}"
    );
}

#[test]
fn record_local_id_rejects_a_non_import_record_type() {
    let sc = simple_schema();
    let mut metadata = entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))]));
    metadata.authoring_only = true;
    let b = bundle(&[&sc], vec![("$record", metadata)], None);
    assert!(matches!(
        write_bundle(&b),
        Err(E::EntryLineage { ref local_id, .. }) if local_id == "$record"
    ));
}

// ---- malformed ids and hashes ----

#[test]
fn malformed_bundle_uuid() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("uuid".to_string(), s("not-a-uuid"));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::BadBundleUuid { found } if found == "not-a-uuid"),
        "got {err:?}"
    );
}

#[test]
fn malformed_entry_ids() {
    for (field, value) in [
        ("uuid", "xyz"),
        ("type_uuid", "1234"),
        ("schema_hash", "abc"),
    ] {
        let (plain, _) = valid_plain();
        let bytes = mutate_envelope(&plain, |env| {
            env_entry(env, "a").insert(field.to_string(), s(value));
        });
        let err = parse_bundle(&bytes).unwrap_err();
        assert!(
            matches!(&err, E::BadEntryId { local_id, field: f, found }
                if local_id == "a" && *f == field && found == value),
            "field {field}: got {err:?}"
        );
    }
}

#[test]
fn malformed_schemas_key() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        let schemas = as_obj(as_obj(env).get_mut("schemas").unwrap());
        let snap = schemas.values().next().unwrap().clone();
        schemas.insert("zz-not-a-hash".to_string(), snap);
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::BadSchemaKey { key } if key == "zz-not-a-hash"),
        "got {err:?}"
    );
}

// ---- key strictness ----

#[test]
fn unknown_envelope_key_rejected() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("extra".to_string(), u(1));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::UnknownEnvelopeKey { key } if key == "extra"),
        "got {err:?}"
    );
}

#[test]
fn unknown_extension_key_does_not_hide_the_validated_namespace() {
    let (plain, bundle) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("future".to_string(), u(1));
    });
    assert!(matches!(
        parse_bundle(&bytes),
        Err(E::UnknownEnvelopeKey { .. })
    ));

    let skeleton = extract_namespace_skeleton(&bytes).unwrap();
    assert_eq!(skeleton.uuid, bundle.uuid);
    assert_eq!(skeleton.assets, bundle.assets);
}

#[test]
fn missing_envelope_key_rejected() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).remove("uuid");
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::MissingEnvelopeKey { key: "uuid" }),
        "got {err:?}"
    );
}

#[test]
fn unknown_entry_key_rejected() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        env_entry(env, "a").insert("notes".to_string(), s("hi"));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::UnknownEntryKey { local_id, key } if local_id == "a" && key == "notes"),
        "got {err:?}"
    );
}

#[test]
fn missing_entry_key_rejected() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        env_entry(env, "a").remove("type_uuid");
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(&err, E::MissingEntryKey { ref local_id, key: "type_uuid" } if local_id == "a"),
        "got {err:?}"
    );
}

#[test]
fn duplicate_json_keys_rejected() {
    let text = br#"{"assets":{},"assets":{}}"#;
    let err = parse_bundle(text).unwrap_err();
    assert!(
        matches!(err, E::Json(e) if e.kind == distill_json::ParseErrorKind::DuplicateKey),
        "got {err:?}"
    );
}

// ---- format_version ----

#[test]
fn unsupported_format_version() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("format_version".to_string(), u(2));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(
        matches!(err, E::UnsupportedFormatVersion { found: 2 }),
        "got {err:?}"
    );
}

#[test]
fn format_version_must_be_uint() {
    let (plain, _) = valid_plain();
    let bytes = mutate_envelope(&plain, |env| {
        as_obj(env).insert("format_version".to_string(), V::Float(1.5));
    });
    let err = parse_bundle(&bytes).unwrap_err();
    assert!(matches!(err, E::FormatVersionNotUInt { .. }), "got {err:?}");
}

#[test]
fn unsupported_format_version_rejected_on_write() {
    let sc = simple_schema();
    let mut b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        None,
    );
    b.format_version = 2;
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(err, E::UnsupportedFormatVersion { found: 2 }),
        "got {err:?}"
    );
}

// ---- top-level shapes ----

#[test]
fn envelope_must_be_object() {
    for text in ["[]", "3", "\"hi\"", "null"] {
        let err = parse_bundle(text.as_bytes()).unwrap_err();
        assert!(
            matches!(err, E::EnvelopeNotObject { .. }),
            "{text}: got {err:?}"
        );
    }
}

#[test]
fn plain_file_must_be_utf8() {
    let err = parse_bundle(&[0xFF, b'h', b'i']).unwrap_err();
    assert!(matches!(err, E::NotUtf8 { offset: 0 }), "got {err:?}");
}

type Mutation = Box<dyn Fn(&mut V)>;
type ErrCheck = fn(&E) -> bool;

#[test]
fn field_shape_errors() {
    let (plain, _) = valid_plain();
    let cases: Vec<(Mutation, ErrCheck)> = vec![
        (
            Box::new(|env: &mut V| {
                as_obj(env).insert("schemas".to_string(), arr(vec![]));
            }),
            |e| matches!(e, E::SchemasNotObject { .. }),
        ),
        (
            Box::new(|env: &mut V| {
                as_obj(env).insert("assets".to_string(), s("x"));
            }),
            |e| matches!(e, E::AssetsNotObject { .. }),
        ),
        (
            Box::new(|env: &mut V| {
                as_obj(env).insert("primary".to_string(), u(1));
            }),
            |e| matches!(e, E::PrimaryNotString { .. }),
        ),
        (
            Box::new(|env: &mut V| {
                let assets = as_obj(as_obj(env).get_mut("assets").unwrap());
                assets.insert("a".to_string(), u(1));
            }),
            |e| matches!(e, E::EntryNotObject { .. }),
        ),
    ];
    for (i, (mutate, check)) in cases.into_iter().enumerate() {
        let bytes = mutate_envelope(&plain, |env| mutate(env));
        let err = parse_bundle(&bytes).unwrap_err();
        assert!(check(&err), "case {i}: got {err:?}");
    }
}

// ---- encoding choice ----

#[test]
fn blob_node_in_plain_bundle_rejected() {
    // Take a real container's JSON chunk — structurally a valid envelope
    // whose blob leaves are {"len","offset"} — and present it as a plain
    // file: the schema walk finds a Blob node, which demands the container.
    let sc = schema(st(&[("payload", N::Blob)]));
    let b = bundle(
        &[&sc],
        vec![("x", entry(UUID_A, &sc, obj(&[("payload", blob(b"bytes"))])))],
        None,
    );
    let container = write_bundle(&b).unwrap();
    let (json, _, _) = container_parts(&container);
    let err = parse_bundle(&json).unwrap_err();
    assert!(
        matches!(&err, E::BlobInPlainBundle { local_id, path }
            if local_id == "x" && path.contains("payload")),
        "got {err:?}"
    );
}

// ---- writer-side envelope validation ----

#[test]
fn write_rejects_schema_under_wrong_key() {
    let simple = simple_schema();
    let tree = tree_schema();
    let mut b = bundle(&[&simple], vec![], None);
    // Re-key the tree schema under simple's hash.
    b.schemas.insert(lh(&simple), tree);
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::Schema { hash, error: SnapshotError::HashMismatch { .. } }
            if *hash == lh(&simple)),
        "got {err:?}"
    );
}

#[test]
fn write_rejects_missing_primary() {
    let sc = simple_schema();
    let b = bundle(
        &[&sc],
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        Some("ghost"),
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::PrimaryNotFound { primary } if primary == "ghost"),
        "got {err:?}"
    );
}

#[test]
fn write_rejects_unclosed_schema_reference() {
    let sc = simple_schema();
    let b = bundle(
        &[], // schemas empty; entry references simple_schema's hash
        vec![(
            "a",
            entry(UUID_A, &sc, obj(&[("count", u(1)), ("name", s("n"))])),
        )],
        None,
    );
    let err = write_bundle(&b).unwrap_err();
    assert!(
        matches!(&err, E::MissingSchema { local_id, .. } if local_id == "a"),
        "got {err:?}"
    );
}

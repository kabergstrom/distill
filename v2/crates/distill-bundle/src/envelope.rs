//! The envelope (§6): the JSON structure both physical encodings share.
//!
//! Top-level keys: `format_version` (pinned = 1), `uuid`, `primary`
//! (optional, must name an existing entry), `schemas` (64-hex LogicalHash →
//! schema snapshot JSON, key verified to equal the snapshot's own hash),
//! `assets` (local_id → {uuid, type_uuid, schema_hash, data}). Unknown keys
//! at either level are errors — the canonical writer never emits them.
//! Bundles are schema-closed: every entry's `schema_hash` resolves in
//! `schemas`. The `$` local_id namespace is reserved for exactly
//! `$settings` and `$record`.

use std::collections::BTreeMap;

use distill_core::bootstrap::{
    BootstrapControlSpecV1, BOOTSTRAP_CONTROL_TYPE_UUIDS,
    IMPORT_RECORD_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use ngp_schema::{node_bytes, snapshot_to_json, verify_snapshot, LogicalSchema};

use crate::error::BundleError;
use crate::walk::{kind_name, walk_entry, WalkMode};
use crate::{AssetEntry, Bundle, BUNDLE_FORMAT_VERSION};

pub(crate) fn utf8(bytes: &[u8]) -> Result<&str, BundleError> {
    std::str::from_utf8(bytes).map_err(|e| BundleError::NotUtf8 {
        offset: e.valid_up_to(),
    })
}

/// Parse a plain-JSON bundle: the whole file is envelope text. The schema
/// walk runs in `PlainParse` mode — reaching a Blob node is an error
/// (blobs require the container).
pub(crate) fn parse_plain(bytes: &[u8]) -> Result<Bundle, BundleError> {
    let text = utf8(bytes)?;
    let value = distill_json::parse(text).map_err(BundleError::Json)?;
    let mut bundle = decode(value)?;
    let Bundle {
        schemas, assets, ..
    } = &mut bundle;
    for (local_id, entry) in assets.iter_mut() {
        let schema = schemas
            .get(&entry.schema_hash)
            .ok_or_else(|| BundleError::Internal {
                detail: format!("schema closure not upheld for {local_id:?}"),
            })?;
        walk_entry(
            local_id,
            &schema.root,
            &mut entry.data,
            WalkMode::PlainParse,
        )?;
    }
    Ok(bundle)
}

pub(crate) fn parse_plain_namespace(bytes: &[u8]) -> Result<Bundle, BundleError> {
    let text = utf8(bytes)?;
    let value = distill_json::parse(text).map_err(BundleError::Json)?;
    let mut bundle = decode_namespace(value)?;
    let Bundle {
        schemas, assets, ..
    } = &mut bundle;
    for (local_id, entry) in assets.iter_mut() {
        let schema = schemas
            .get(&entry.schema_hash)
            .ok_or_else(|| BundleError::Internal {
                detail: format!("schema closure not upheld for {local_id:?}"),
            })?;
        walk_entry(
            local_id,
            &schema.root,
            &mut entry.data,
            WalkMode::PlainParse,
        )?;
    }
    Ok(bundle)
}

/// Decode the complete namespace while discarding only fields that the v1
/// grammar proves cannot affect it. The ordinary decoder still reports these
/// defects; this path exists solely to decide whether their poison can be
/// bundle-scoped.
pub(crate) fn decode_namespace(value: AuthoredValue) -> Result<Bundle, BundleError> {
    let AuthoredValue::Object(mut top) = value else {
        return decode(value);
    };
    top.retain(|key, _| {
        matches!(
            key.as_str(),
            "assets" | "format_version" | "primary" | "schemas" | "uuid"
        )
    });
    // A broken primary affects path selection, but not the complete set of
    // bundle/asset/type claims. Physical-path lookup is poisoned by the bundle
    // row itself.
    top.remove("primary");
    if let Some(AuthoredValue::Object(assets)) = top.get_mut("assets") {
        for entry in assets.values_mut() {
            if let AuthoredValue::Object(fields) = entry {
                fields.retain(|key, _| {
                    matches!(
                        key.as_str(),
                        "authoring_only"
                            | "data"
                            | "lineage"
                            | "schema_hash"
                            | "type_uuid"
                            | "uuid"
                    )
                });
            }
        }
    }
    decode(AuthoredValue::Object(top))
}

/// Decode the envelope JSON value into a `Bundle` (data left as-is; the
/// caller runs the schema walk in its encoding's mode). Enforces key
/// strictness, id/hash parsing, snapshot verification against map keys,
/// reserved local_ids, schema closure, and primary resolution.
pub(crate) fn decode(value: AuthoredValue) -> Result<Bundle, BundleError> {
    let mut top = match value {
        AuthoredValue::Object(m) => m,
        other => {
            return Err(BundleError::EnvelopeNotObject {
                found: kind_name(&other),
            })
        }
    };
    for key in top.keys() {
        if !matches!(
            key.as_str(),
            "assets" | "format_version" | "primary" | "schemas" | "uuid"
        ) {
            return Err(BundleError::UnknownEnvelopeKey { key: key.clone() });
        }
    }

    let format_version = match top.remove("format_version") {
        None => {
            return Err(BundleError::MissingEnvelopeKey {
                key: "format_version",
            })
        }
        Some(AuthoredValue::UInt(n)) => {
            if n != BUNDLE_FORMAT_VERSION as u128 {
                return Err(BundleError::UnsupportedFormatVersion { found: n });
            }
            n as u32
        }
        Some(other) => {
            return Err(BundleError::FormatVersionNotUInt {
                found: kind_name(&other),
            })
        }
    };

    let uuid: BundleUuid = match top.remove("uuid") {
        None => return Err(BundleError::MissingEnvelopeKey { key: "uuid" }),
        Some(AuthoredValue::Str(s)) => s
            .parse()
            .map_err(|_| BundleError::BadBundleUuid { found: s.clone() })?,
        Some(other) => {
            return Err(BundleError::BadBundleUuid {
                found: kind_name(&other).to_string(),
            })
        }
    };

    let primary = match top.remove("primary") {
        None => None,
        Some(AuthoredValue::Str(s)) => Some(s),
        Some(other) => {
            return Err(BundleError::PrimaryNotString {
                found: kind_name(&other),
            })
        }
    };

    let schemas_map = match top.remove("schemas") {
        None => return Err(BundleError::MissingEnvelopeKey { key: "schemas" }),
        Some(AuthoredValue::Object(m)) => m,
        Some(other) => {
            return Err(BundleError::SchemasNotObject {
                found: kind_name(&other),
            })
        }
    };
    let mut schemas = BTreeMap::new();
    for (key, snapshot) in schemas_map {
        let hash: LogicalHash = key
            .parse()
            .map_err(|_| BundleError::BadSchemaKey { key: key.clone() })?;
        // The snapshot codec verifies canonical text; re-rendering the
        // parsed subtree through the canonical writer yields exactly that
        // text form, so a hand-reformatted envelope still verifies (and
        // rewrites canonically).
        let text = distill_json::write(&snapshot).map_err(BundleError::JsonWrite)?;
        let schema =
            verify_snapshot(&text, hash).map_err(|error| BundleError::Schema { hash, error })?;
        if schemas.insert(hash, schema).is_some() {
            return Err(BundleError::DuplicateSchema { hash });
        }
    }

    let assets_map = match top.remove("assets") {
        None => return Err(BundleError::MissingEnvelopeKey { key: "assets" }),
        Some(AuthoredValue::Object(m)) => m,
        Some(other) => {
            return Err(BundleError::AssetsNotObject {
                found: kind_name(&other),
            })
        }
    };
    let mut assets = BTreeMap::new();
    for (local_id, entry_value) in assets_map {
        check_local_id(&local_id)?;
        let mut entry = match entry_value {
            AuthoredValue::Object(m) => m,
            other => {
                return Err(BundleError::EntryNotObject {
                    local_id,
                    found: kind_name(&other),
                })
            }
        };
        for key in entry.keys() {
            if !matches!(
                key.as_str(),
                "authoring_only" | "data" | "lineage" | "schema_hash" | "type_uuid" | "uuid"
            ) {
                return Err(BundleError::UnknownEntryKey {
                    local_id,
                    key: key.clone(),
                });
            }
        }
        let uuid: AssetUuid = take_id(&mut entry, &local_id, "uuid")?;
        let type_uuid: TypeUuid = take_id(&mut entry, &local_id, "type_uuid")?;
        let schema_hash: LogicalHash = take_id(&mut entry, &local_id, "schema_hash")?;
        // Bundles written before lineage was retired carry a `lineage` key;
        // it is accepted and ignored.
        entry.remove("lineage");
        let authoring_only = match entry.remove("authoring_only") {
            None => {
                return Err(BundleError::MissingEntryKey {
                    local_id,
                    key: "authoring_only",
                })
            }
            Some(AuthoredValue::Bool(value)) => value,
            Some(other) => {
                return Err(BundleError::AuthoringOnlyNotBool {
                    local_id,
                    found: kind_name(&other),
                })
            }
        };
        if local_id.starts_with('$') && !authoring_only {
            return Err(BundleError::ReservedEntryMustBeAuthoringOnly { local_id });
        }
        let data = entry
            .remove("data")
            .ok_or_else(|| BundleError::MissingEntryKey {
                local_id: local_id.clone(),
                key: "data",
            })?;
        assets.insert(
            local_id,
            AssetEntry {
                uuid,
                type_uuid,
                schema_hash,
                authoring_only,
                data,
            },
        );
    }

    // Schema-closure (§6, §11): every entry schema resolves within the file.
    for (local_id, entry) in &assets {
        for schema_hash in schema_references(entry) {
            if !schemas.contains_key(&schema_hash) {
                return Err(BundleError::MissingSchema {
                    local_id: local_id.clone(),
                    schema_hash,
                });
            }
        }
        let schema = &schemas[&entry.schema_hash];
        validate_entry_role(local_id, format_version, entry, schema)?;
    }
    if let Some(p) = &primary {
        match assets.get(p) {
            None => return Err(BundleError::PrimaryNotFound { primary: p.clone() }),
            Some(entry) if entry.authoring_only => {
                return Err(BundleError::PrimaryIsAuthoringOnly { primary: p.clone() })
            }
            Some(_) => {}
        }
    }

    Ok(Bundle {
        format_version,
        uuid,
        primary,
        schemas,
        assets,
    })
}

/// Exact schema hashes named by a bundle entry.
pub(crate) fn schema_references(entry: &AssetEntry) -> Vec<LogicalHash> {
    vec![entry.schema_hash]
}

/// The reserved `$` namespace (§6): only `$settings` and `$record`, at
/// every boundary that can put an entry in a bundle.
pub(crate) fn check_local_id(local_id: &str) -> Result<(), BundleError> {
    if local_id.starts_with('$') && local_id != "$settings" && local_id != "$record" {
        return Err(BundleError::ReservedLocalId {
            local_id: local_id.to_string(),
        });
    }
    Ok(())
}

fn take_id<T: std::str::FromStr>(
    entry: &mut BTreeMap<String, AuthoredValue>,
    local_id: &str,
    field: &'static str,
) -> Result<T, BundleError> {
    match entry.remove(field) {
        None => Err(BundleError::MissingEntryKey {
            local_id: local_id.to_string(),
            key: field,
        }),
        Some(AuthoredValue::Str(s)) => s.parse().map_err(|_| BundleError::BadEntryId {
            local_id: local_id.to_string(),
            field,
            found: s.clone(),
        }),
        Some(other) => Err(BundleError::BadEntryId {
            local_id: local_id.to_string(),
            field,
            found: kind_name(&other).to_string(),
        }),
    }
}

/// Entry roles: `$record` is exactly the built-in ImportRecord, and a
/// bootstrap control entry is authoring-only with the embedded schema.
pub(crate) fn validate_entry_role(
    local_id: &str,
    format_version: u32,
    entry: &AssetEntry,
    schema: &LogicalSchema,
) -> Result<(), BundleError> {
    if (local_id == "$record") != (entry.type_uuid == IMPORT_RECORD_TYPE_UUID) {
        return role_error(
            local_id,
            "$record is reserved exclusively for the built-in ImportRecord type",
        );
    }
    let Some(index) = BOOTSTRAP_CONTROL_TYPE_UUIDS
        .iter()
        .position(|type_uuid| *type_uuid == entry.type_uuid)
    else {
        return Ok(());
    };
    if format_version != BUNDLE_FORMAT_VERSION || !entry.authoring_only {
        return role_error(local_id, "bootstrap control has invalid version or role");
    }
    let expected = &BootstrapControlSpecV1::embedded()
        .map_err(|_| BundleError::EntryRole {
            local_id: local_id.to_owned(),
            detail: "embedded bootstrap spec is invalid",
        })?
        .0[index];
    let actual_schema = node_bytes(&schema.root).map_err(|_| BundleError::EntryRole {
        local_id: local_id.to_owned(),
        detail: "bootstrap logical schema is not canonical",
    })?;
    if entry.schema_hash != expected.logical_hash || actual_schema != expected.logical_schema {
        return role_error(local_id, "bootstrap schema hash does not match DSB");
    }
    Ok(())
}

fn role_error<T>(local_id: &str, detail: &'static str) -> Result<T, BundleError> {
    Err(BundleError::EntryRole {
        local_id: local_id.to_owned(),
        detail,
    })
}

/// Build the envelope JSON value for the writer. `data` carries each
/// entry's data with blob leaves already rewritten for the target encoding
/// (verbatim for plain — there are none — or `{"len","offset"}` for the
/// container). Schema snapshots are spliced through the snapshot codec.
pub(crate) fn build(
    bundle: &Bundle,
    mut data: BTreeMap<String, AuthoredValue>,
) -> Result<AuthoredValue, BundleError> {
    let mut top = BTreeMap::new();
    top.insert(
        "format_version".to_string(),
        AuthoredValue::UInt(bundle.format_version as u128),
    );
    top.insert(
        "uuid".to_string(),
        AuthoredValue::Str(bundle.uuid.to_string()),
    );
    if let Some(p) = &bundle.primary {
        top.insert("primary".to_string(), AuthoredValue::Str(p.clone()));
    }

    let mut schemas = BTreeMap::new();
    for (hash, schema) in &bundle.schemas {
        let text =
            snapshot_to_json(schema).map_err(|error| BundleError::Schema { hash: *hash, error })?;
        let value = distill_json::parse(&text).map_err(BundleError::Json)?;
        schemas.insert(hash.to_string(), value);
    }
    top.insert("schemas".to_string(), AuthoredValue::Object(schemas));

    let mut assets = BTreeMap::new();
    for (local_id, entry) in &bundle.assets {
        let data_value = data.remove(local_id).ok_or_else(|| BundleError::Internal {
            detail: format!("writer lost data for entry {local_id:?}"),
        })?;
        let mut m = BTreeMap::new();
        m.insert(
            "uuid".to_string(),
            AuthoredValue::Str(entry.uuid.to_string()),
        );
        m.insert(
            "type_uuid".to_string(),
            AuthoredValue::Str(entry.type_uuid.to_string()),
        );
        m.insert(
            "schema_hash".to_string(),
            AuthoredValue::Str(entry.schema_hash.to_string()),
        );
        m.insert(
            "authoring_only".to_string(),
            AuthoredValue::Bool(entry.authoring_only),
        );
        m.insert("data".to_string(), data_value);
        assets.insert(local_id.clone(), AuthoredValue::Object(m));
    }
    top.insert("assets".to_string(), AuthoredValue::Object(assets));

    Ok(AuthoredValue::Object(top))
}

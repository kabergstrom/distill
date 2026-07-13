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

use std::collections::{BTreeMap, BTreeSet};

use distill_core::attestation::{
    is_bootstrap_control_type, BootstrapControlSpecV1, BOOTSTRAP_CONTROL_TYPE_UUIDS,
    IMPORT_RECORD_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_core::lineage::{
    lineage_chain_digest, AcceptedSchemaEpoch, EntryLineageV1, LineageStamp,
};
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
        let lineage = decode_entry_lineage(
            entry
                .remove("lineage")
                .ok_or_else(|| BundleError::MissingEntryKey {
                    local_id: local_id.clone(),
                    key: "lineage",
                })?,
            &local_id,
        )?;
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
                lineage,
                authoring_only,
                data,
            },
        );
    }

    // Schema-closure (§6): every referenced hash resolves within the file.
    for (local_id, entry) in &assets {
        if !schemas.contains_key(&entry.schema_hash) {
            return Err(BundleError::MissingSchema {
                local_id: local_id.clone(),
                schema_hash: entry.schema_hash,
            });
        }
        let schema = &schemas[&entry.schema_hash];
        validate_entry_lineage(local_id, format_version, entry, schema)?;
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

fn decode_entry_lineage(
    value: AuthoredValue,
    local_id: &str,
) -> Result<EntryLineageV1, BundleError> {
    let mut outer = match value {
        AuthoredValue::Object(value) if value.len() == 1 => value,
        _ => return lineage_error(local_id, "lineage must be an exact one-member object"),
    };
    if let Some(value) = outer.remove("manifest") {
        let mut manifest = match value {
            AuthoredValue::Object(value) => value,
            _ => return lineage_error(local_id, "manifest lineage must be an object"),
        };
        if manifest.len() != 3
            || !manifest.contains_key("epochs")
            || !manifest.contains_key("cursor")
            || !manifest.contains_key("chain")
        {
            return lineage_error(local_id, "manifest lineage fields are not exact");
        }
        let epochs = match manifest.remove("epochs").unwrap() {
            AuthoredValue::Array(values) => values
                .into_iter()
                .map(|value| decode_lineage_epoch(value, local_id))
                .collect::<Result<Vec<_>, _>>()?,
            _ => return lineage_error(local_id, "lineage epochs must be an array"),
        };
        let cursor = match manifest.remove("cursor").unwrap() {
            AuthoredValue::UInt(value) => {
                u32::try_from(value).map_err(|_| BundleError::EntryLineage {
                    local_id: local_id.to_owned(),
                    detail: "lineage cursor exceeds u32",
                })?
            }
            _ => return lineage_error(local_id, "lineage cursor must be unsigned"),
        };
        let chain =
            match manifest.remove("chain").unwrap() {
                AuthoredValue::Str(value) => value
                    .parse::<LogicalHash>()
                    .map(|hash| hash.0)
                    .map_err(|_| BundleError::EntryLineage {
                        local_id: local_id.to_owned(),
                        detail: "lineage chain must be 32-byte lowercase hex",
                    })?,
                _ => return lineage_error(local_id, "lineage chain must be text"),
            };
        return Ok(EntryLineageV1::Manifest(LineageStamp {
            epochs,
            cursor,
            chain,
        }));
    }
    if let Some(value) = outer.remove("bootstrap") {
        let mut bootstrap = match value {
            AuthoredValue::Object(value) if value.len() == 1 => value,
            _ => return lineage_error(local_id, "bootstrap lineage must be an exact object"),
        };
        let version = match bootstrap.remove("bundle_format_version") {
            Some(AuthoredValue::UInt(value)) => {
                u32::try_from(value).map_err(|_| BundleError::EntryLineage {
                    local_id: local_id.to_owned(),
                    detail: "bootstrap format version exceeds u32",
                })?
            }
            _ => return lineage_error(local_id, "bootstrap format version must be unsigned"),
        };
        return Ok(EntryLineageV1::Bootstrap {
            bundle_format_version: version,
        });
    }
    lineage_error(local_id, "unknown entry-lineage arm")
}

fn decode_lineage_epoch(
    value: AuthoredValue,
    local_id: &str,
) -> Result<AcceptedSchemaEpoch, BundleError> {
    let mut epoch = match value {
        AuthoredValue::Object(value) if value.len() == 2 => value,
        _ => return lineage_error(local_id, "lineage epoch must be an exact object"),
    };
    let digest = match epoch.remove("digest") {
        Some(AuthoredValue::Str(value)) => {
            value.parse().map_err(|_| BundleError::EntryLineage {
                local_id: local_id.to_owned(),
                detail: "lineage epoch digest is malformed",
            })?
        }
        _ => return lineage_error(local_id, "lineage epoch digest must be text"),
    };
    let forward_parent = match epoch.remove("forward_parent") {
        Some(AuthoredValue::Null) => None,
        Some(AuthoredValue::UInt(value)) => {
            Some(u32::try_from(value).map_err(|_| BundleError::EntryLineage {
                local_id: local_id.to_owned(),
                detail: "lineage parent exceeds u32",
            })?)
        }
        _ => return lineage_error(local_id, "lineage parent must be null or unsigned"),
    };
    Ok(AcceptedSchemaEpoch {
        digest,
        forward_parent,
    })
}

pub(crate) fn validate_entry_lineage(
    local_id: &str,
    format_version: u32,
    entry: &AssetEntry,
    schema: &LogicalSchema,
) -> Result<(), BundleError> {
    if (local_id == "$record") != (entry.type_uuid == IMPORT_RECORD_TYPE_UUID) {
        return lineage_error(
            local_id,
            "$record is reserved exclusively for the built-in ImportRecord type",
        );
    }
    match (&entry.lineage, is_bootstrap_control_type(entry.type_uuid)) {
        (
            EntryLineageV1::Bootstrap {
                bundle_format_version,
            },
            true,
        ) => {
            if *bundle_format_version != BUNDLE_FORMAT_VERSION
                || format_version != BUNDLE_FORMAT_VERSION
                || !entry.authoring_only
            {
                return lineage_error(local_id, "bootstrap lineage has invalid version or role");
            }
            let index = BOOTSTRAP_CONTROL_TYPE_UUIDS
                .iter()
                .position(|type_uuid| *type_uuid == entry.type_uuid)
                .expect("bootstrap predicate and closed UUID table agree");
            let expected = &BootstrapControlSpecV1::embedded()
                .map_err(|_| BundleError::EntryLineage {
                    local_id: local_id.to_owned(),
                    detail: "embedded bootstrap spec is invalid",
                })?
                .0[index];
            let actual_schema =
                node_bytes(&schema.root).map_err(|_| BundleError::EntryLineage {
                    local_id: local_id.to_owned(),
                    detail: "bootstrap logical schema is not canonical",
                })?;
            if entry.schema_hash != expected.logical_hash
                || actual_schema != expected.logical_schema
            {
                return lineage_error(local_id, "bootstrap schema hash does not match DSB");
            }
            Ok(())
        }
        (EntryLineageV1::Manifest(stamp), false) => {
            if stamp.epochs.is_empty()
                || stamp
                    .epochs
                    .iter()
                    .map(|epoch| epoch.digest)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != stamp.epochs.len()
                || usize::try_from(stamp.cursor)
                    .ok()
                    .is_none_or(|cursor| cursor >= stamp.epochs.len())
                || stamp.epochs[0].forward_parent.is_some()
                || stamp
                    .epochs
                    .iter()
                    .enumerate()
                    .skip(1)
                    .any(|(index, epoch)| {
                        epoch.forward_parent.is_none_or(|parent| {
                            usize::try_from(parent).unwrap_or(usize::MAX) >= index
                        })
                    })
                || stamp.selected_digest() != Some(entry.schema_hash)
                || stamp.chain != lineage_chain_digest(entry.type_uuid, &stamp.epochs, stamp.cursor)
            {
                return lineage_error(local_id, "manifest lineage stamp is not canonical");
            }
            Ok(())
        }
        (EntryLineageV1::Bootstrap { .. }, false) => {
            lineage_error(local_id, "non-bootstrap type used bootstrap lineage")
        }
        (EntryLineageV1::Manifest(_), true) => {
            lineage_error(local_id, "bootstrap type used manifest lineage")
        }
    }
}

fn lineage_error<T>(local_id: &str, detail: &'static str) -> Result<T, BundleError> {
    Err(BundleError::EntryLineage {
        local_id: local_id.to_owned(),
        detail,
    })
}

fn encode_entry_lineage(lineage: &EntryLineageV1) -> AuthoredValue {
    let mut outer = BTreeMap::new();
    match lineage {
        EntryLineageV1::Manifest(stamp) => {
            let epochs = stamp
                .epochs
                .iter()
                .map(|epoch| {
                    let mut value = BTreeMap::new();
                    value.insert(
                        "digest".into(),
                        AuthoredValue::Str(epoch.digest.to_string()),
                    );
                    value.insert(
                        "forward_parent".into(),
                        epoch.forward_parent.map_or(AuthoredValue::Null, |parent| {
                            AuthoredValue::UInt(u128::from(parent))
                        }),
                    );
                    AuthoredValue::Object(value)
                })
                .collect();
            let mut value = BTreeMap::new();
            value.insert("epochs".into(), AuthoredValue::Array(epochs));
            value.insert(
                "cursor".into(),
                AuthoredValue::UInt(u128::from(stamp.cursor)),
            );
            value.insert(
                "chain".into(),
                AuthoredValue::Str(LogicalHash(stamp.chain).to_string()),
            );
            outer.insert("manifest".into(), AuthoredValue::Object(value));
        }
        EntryLineageV1::Bootstrap {
            bundle_format_version,
        } => {
            let mut value = BTreeMap::new();
            value.insert(
                "bundle_format_version".into(),
                AuthoredValue::UInt(u128::from(*bundle_format_version)),
            );
            outer.insert("bootstrap".into(), AuthoredValue::Object(value));
        }
    }
    AuthoredValue::Object(outer)
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
        m.insert("lineage".to_string(), encode_entry_lineage(&entry.lineage));
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

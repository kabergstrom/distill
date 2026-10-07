//! Commit, diagnostic, and authored-value validation shared by the RPC
//! server and the publication paths that apply commits to the store.

use std::collections::BTreeSet;
use std::sync::Arc;

use unicode_normalization::UnicodeNormalization;

use distill_core::frames::reenter;
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{verify_snapshot, PrimitiveKind, SchemaNode};

use crate::*;

pub(crate) fn validate_commit(commit: &Commit) -> Result<(), AdminError> {
    let mut tag_assets = BTreeSet::new();
    for mutation in &commit.tag_projection_mutations {
        let (asset, tags) = match mutation {
            TagProjectionMutation::Set { asset, tags } => (*asset, Some(tags)),
            TagProjectionMutation::Remove { asset } => (*asset, None),
        };
        if !tag_assets.insert(asset) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "duplicate tag-projection mutation".to_owned(),
            });
        }
        if tags.is_some_and(|tags| {
            tags.iter().any(|(tag, value)| {
                !valid_identifier(tag)
                    || value
                        .as_deref()
                        .is_some_and(|value| !valid_identifier(value))
            })
        }) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "tag projection contains a noncanonical name or value".to_owned(),
            });
        }
    }
    let mut poison_assets = BTreeSet::new();
    for mutation in &commit.tag_poison_mutations {
        let asset = match mutation {
            TagPoisonMutation::Set { asset, .. } | TagPoisonMutation::Remove { asset } => *asset,
        };
        if !poison_assets.insert(asset) {
            return Err(AdminError::InvalidAuthoringIdentity {
                uuid: asset,
                detail: "duplicate tag-poison mutation".to_owned(),
            });
        }
    }
    let mut assets = BTreeSet::new();
    for mutation in &commit.assets {
        let uuid = mutation.uuid;
        if !assets.insert(uuid) {
            return Err(AdminError::DuplicateAssetMutation { uuid });
        }
    }
    let mut authoring = BTreeSet::new();
    for mutation in &commit.authoring {
        let uuid = match mutation {
            AuthoringMutation::Set(entry) => {
                if !valid_bundle_local_id(&entry.local_id) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "local ID is noncanonical or uses the reserved '$' namespace"
                            .to_owned(),
                    });
                }
                if !valid_logical_path(&entry.normalized_path) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "normalized path is not canonical".to_owned(),
                    });
                }
                if entry.tags.iter().any(|(tag, value)| {
                    !valid_identifier(tag)
                        || value
                            .as_deref()
                            .is_some_and(|value| !valid_identifier(value))
                }) {
                    return Err(AdminError::InvalidAuthoringIdentity {
                        uuid: entry.uuid,
                        detail: "tag name or value is not canonical".to_owned(),
                    });
                }
                validate_authoring_entry(entry).map_err(|error| {
                    AdminError::InvalidAuthoringValue {
                        uuid: entry.uuid,
                        error,
                    }
                })?;
                entry.uuid
            }
            AuthoringMutation::Remove { uuid } => *uuid,
        };
        if !authoring.insert(uuid) {
            return Err(AdminError::DuplicateAuthoringMutation { uuid });
        }
    }
    let mut paths = BTreeSet::new();
    for mutation in &commit.paths {
        let path = match mutation {
            PathMutation::Set { path, .. } | PathMutation::Remove { path } => path,
        };
        if !valid_logical_path(path) {
            return Err(AdminError::InvalidPath { path: path.clone() });
        }
        if !paths.insert(path.clone()) {
            return Err(AdminError::DuplicatePathMutation { path: path.clone() });
        }
        if let PathMutation::Set { candidates, .. } = mutation {
            if candidates.is_empty() {
                return Err(AdminError::EmptyPathCandidates { path: path.clone() });
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_authoring_entry(entry: &AuthoringEntry) -> Result<(), AuthoringValueError> {
    decode_authoring_payload(entry.schema_hash, &entry.logical_schema, &entry.value).map(drop)
}

/// Authenticate and decode an RPC authored-value carrier into the exact
/// schema-shaped value stored in a bundle. Blob tokens are interpreted only
/// while walking a `SchemaNode::Blob`, so an ordinary field named
/// `$distill_blob` can never collide with the transport escape.
pub fn decode_authoring_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
) -> Result<AuthoredValue, AuthoringValueError> {
    decode_payload(schema_hash, logical_schema, authored_value, None)
}

/// `decode_authoring_payload` for an importer's settings: a struct field the
/// authored value leaves out takes the importer's `defaults` (see
/// [`complete_settings`]) before the value is checked against the schema.
pub fn decode_settings_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
    defaults: &AuthoredValue,
) -> Result<AuthoredValue, AuthoringValueError> {
    decode_payload(schema_hash, logical_schema, authored_value, Some(defaults))
}

fn decode_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
    defaults: Option<&AuthoredValue>,
) -> Result<AuthoredValue, AuthoringValueError> {
    let schema_text =
        std::str::from_utf8(logical_schema).map_err(|_| AuthoringValueError::LogicalSchemaUtf8)?;
    let schema = verify_snapshot(schema_text, schema_hash)
        .map_err(|error| AuthoringValueError::LogicalSchemaInvalid(error.to_string()))?;
    let value_text = std::str::from_utf8(&authored_value.canonical_value)
        .map_err(|_| AuthoringValueError::ValueInvalid("value is not UTF-8".to_owned()))?;
    let mut value = distill_json::parse(value_text)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    let rewritten = distill_json::write(&value)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    if rewritten != value_text {
        return Err(AuthoringValueError::ValueNotCanonical);
    }
    if let Some(defaults) = defaults {
        complete_settings(&schema.root, &mut value, defaults);
    }
    let blob_count = u32::try_from(authored_value.blobs.len()).map_err(|_| {
        AuthoringValueError::SchemaValueShape {
            detail: "blob table exceeds UInt32".to_owned(),
        }
    })?;
    let mut used = BTreeSet::new();
    walk_schema_value(&schema.root, &value, &mut Vec::new(), &mut used, blob_count)?;
    for index in 0..blob_count {
        if !used.contains(&index) {
            return Err(AuthoringValueError::UnusedBlobIndex { index });
        }
    }
    materialize_blob_tokens(
        &schema.root,
        &mut value,
        &mut Vec::new(),
        &authored_value.blobs,
    );
    Ok(value)
}

/// Fill the fields an authored importer-settings value leaves out of a struct
/// from `defaults`, the importer's default settings: a missing field takes
/// the default's, a present struct field is completed the same way, anything
/// else stays as authored (an enum keeps its authored variant whole). A
/// present `Option` is completed as its element. Where there is no default
/// (an object authored over a `null` default, as for an `Option` struct
/// whose default is `None`), a missing `Option` field is `None` and a
/// missing required field stays missing: the validation's error names it.
/// A default holding a blob is never filled in: a blob is authored, so
/// leaving it out stays the missing-field error. Completing a completed
/// value changes nothing.
pub fn complete_settings(schema: &SchemaNode, value: &mut AuthoredValue, defaults: &AuthoredValue) {
    match (schema, value) {
        (SchemaNode::Option(_), AuthoredValue::Null) => {}
        (SchemaNode::Option(element), value) => complete_settings(element, value, defaults),
        (SchemaNode::Struct { fields, .. }, AuthoredValue::Object(object)) => {
            let defaults = match defaults {
                AuthoredValue::Object(defaults) => Some(defaults),
                _ => None,
            };
            for (name, _, field_schema) in fields {
                let default = defaults.and_then(|defaults| defaults.get(name));
                match (object.get_mut(name), default) {
                    (Some(field), Some(default)) => complete_settings(field_schema, field, default),
                    (Some(field), None) => {
                        complete_settings(field_schema, field, &AuthoredValue::Null)
                    }
                    (None, Some(default)) if !holds_blob(default) => {
                        object.insert(name.clone(), default.clone());
                    }
                    (None, None) if matches!(field_schema, SchemaNode::Option(_)) => {
                        object.insert(name.clone(), AuthoredValue::Null);
                    }
                    (None, _) => {}
                }
            }
        }
        _ => {}
    }
}

fn holds_blob(value: &AuthoredValue) -> bool {
    match value {
        AuthoredValue::Blob(_) => true,
        AuthoredValue::Array(items) => items.iter().any(holds_blob),
        AuthoredValue::Object(fields) => fields.values().any(holds_blob),
        _ => false,
    }
}

pub(crate) fn materialize_blob_tokens<'s>(
    schema: &'s SchemaNode,
    value: &mut AuthoredValue,
    frames: &mut Vec<&'s SchemaNode>,
    blobs: &[Arc<[u8]>],
) {
    match schema {
        SchemaNode::Blob => {
            let AuthoredValue::Object(object) = value else {
                unreachable!("validated Blob shape")
            };
            let Some(AuthoredValue::UInt(index)) = object.get("$distill_blob") else {
                unreachable!("validated Blob token")
            };
            *value = AuthoredValue::Blob(blobs[*index as usize].to_vec());
        }
        SchemaNode::Struct { fields, .. } => {
            frames.push(schema);
            materialize_fields(fields, value, frames, blobs);
            frames.pop();
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(object) = value else {
                unreachable!("validated enum shape")
            };
            let name = object
                .first_key_value()
                .expect("validated enum variant")
                .0
                .clone();
            let variant_schema = &variants
                .iter()
                .find(|(candidate, _, _)| candidate == &name)
                .expect("validated enum schema")
                .2;
            frames.push(schema);
            let payload = object.get_mut(&name).expect("validated enum value");
            // A variant payload opens no frame of its own (§5).
            match variant_schema {
                SchemaNode::Struct { fields, .. } => {
                    materialize_fields(fields, payload, frames, blobs)
                }
                variant_schema => materialize_blob_tokens(variant_schema, payload, frames, blobs),
            }
            frames.pop();
        }
        SchemaNode::Vec(element) | SchemaNode::Set(element) => {
            let AuthoredValue::Array(values) = value else {
                unreachable!("validated sequence shape")
            };
            for value in values {
                materialize_blob_tokens(element, value, frames, blobs);
            }
        }
        SchemaNode::Array { elem, .. } => {
            let AuthoredValue::Array(values) = value else {
                unreachable!("validated array shape")
            };
            for value in values {
                materialize_blob_tokens(elem, value, frames, blobs);
            }
        }
        SchemaNode::Option(element) => {
            if !matches!(value, AuthoredValue::Null) {
                materialize_blob_tokens(element, value, frames, blobs);
            }
        }
        SchemaNode::Map { key, value: item } => match value {
            AuthoredValue::Array(entries) => {
                for entry in entries {
                    let AuthoredValue::Array(pair) = entry else {
                        unreachable!("validated map pair")
                    };
                    let (key_value, item_value) = pair.split_at_mut(1);
                    materialize_blob_tokens(key, &mut key_value[0], frames, blobs);
                    materialize_blob_tokens(item, &mut item_value[0], frames, blobs);
                }
            }
            AuthoredValue::Object(entries) => {
                for item_value in entries.values_mut() {
                    materialize_blob_tokens(item, item_value, frames, blobs);
                }
            }
            _ => unreachable!("validated map shape"),
        },
        SchemaNode::BackRef(distance) => {
            let (target, reentry) = reenter(frames, *distance).expect("validated back-reference");
            materialize_blob_tokens(target, value, frames, blobs);
            reentry.restore(frames);
        }
        SchemaNode::Primitive(_)
        | SchemaNode::AssetRef(_)
        | SchemaNode::WeakRef(_)
        | SchemaNode::String
        | SchemaNode::Unit => {}
    }
}

/// A struct body's fields, materialized in whatever frame the caller
/// opened: a struct's own, or its enum's for a variant payload.
fn materialize_fields<'s>(
    fields: &'s [(String, u32, SchemaNode)],
    value: &mut AuthoredValue,
    frames: &mut Vec<&'s SchemaNode>,
    blobs: &[Arc<[u8]>],
) {
    let AuthoredValue::Object(object) = value else {
        unreachable!("validated struct shape")
    };
    for (name, _, field_schema) in fields {
        materialize_blob_tokens(
            field_schema,
            object.get_mut(name).expect("validated struct field"),
            frames,
            blobs,
        );
    }
}

pub(crate) fn walk_schema_value<'s>(
    schema: &'s SchemaNode,
    value: &AuthoredValue,
    frames: &mut Vec<&'s SchemaNode>,
    used: &mut BTreeSet<u32>,
    blob_count: u32,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: detail.to_owned(),
    };
    match schema {
        SchemaNode::Blob => {
            let AuthoredValue::Object(object) = value else {
                return Err(shape("Blob value must be {$distill_blob:u32}"));
            };
            if object.len() != 1 {
                return Err(shape("Blob token must contain exactly one member"));
            }
            let Some(AuthoredValue::UInt(index)) = object.get("$distill_blob") else {
                return Err(shape("Blob token key/value is malformed"));
            };
            let index =
                u32::try_from(*index).map_err(|_| shape("Blob token index does not fit UInt32"))?;
            if index >= blob_count {
                return Err(AuthoringValueError::BlobIndexOutOfRange { index, blob_count });
            }
            if !used.insert(index) {
                return Err(AuthoringValueError::DuplicateBlobIndex { index });
            }
        }
        SchemaNode::Struct { fields, .. } => {
            frames.push(schema);
            walk_fields(fields, value, frames, used, blob_count)?;
            frames.pop();
        }
        SchemaNode::Enum { variants, .. } => {
            let AuthoredValue::Object(object) = value else {
                return Err(shape("enum value must be a one-member object"));
            };
            if object.len() != 1 {
                return Err(shape("enum value must select exactly one variant"));
            }
            let (name, variant_value) = object.first_key_value().expect("one variant");
            let (_, _, variant_schema) = variants
                .iter()
                .find(|(candidate, _, _)| candidate == name)
                .ok_or_else(|| shape("enum value names an unknown variant"))?;
            frames.push(schema);
            // A variant payload opens no frame of its own (§5).
            match variant_schema {
                SchemaNode::Struct { fields, .. } => {
                    walk_fields(fields, variant_value, frames, used, blob_count)?
                }
                variant_schema => {
                    walk_schema_value(variant_schema, variant_value, frames, used, blob_count)?
                }
            }
            frames.pop();
        }
        SchemaNode::Vec(element) | SchemaNode::Set(element) => {
            let AuthoredValue::Array(values) = value else {
                return Err(shape("sequence value must be an array"));
            };
            for (index, value) in values.iter().enumerate() {
                walk_schema_value(element, value, frames, used, blob_count)
                    .map_err(|error| located(&format!("[{index}]"), error))?;
            }
        }
        SchemaNode::Array { len, elem } => {
            let AuthoredValue::Array(values) = value else {
                return Err(shape("array value must be an array"));
            };
            if usize::try_from(*len).ok() != Some(values.len()) {
                return Err(shape("array value length does not match schema"));
            }
            for (index, value) in values.iter().enumerate() {
                walk_schema_value(elem, value, frames, used, blob_count)
                    .map_err(|error| located(&format!("[{index}]"), error))?;
            }
        }
        SchemaNode::Option(element) => {
            if !matches!(value, AuthoredValue::Null) {
                walk_schema_value(element, value, frames, used, blob_count)?;
            }
        }
        SchemaNode::Map { key, value: item } => match value {
            AuthoredValue::Array(entries) => {
                for entry in entries {
                    let AuthoredValue::Array(pair) = entry else {
                        return Err(shape("map entry must be a key/value pair"));
                    };
                    if pair.len() != 2 {
                        return Err(shape("map entry must contain exactly two values"));
                    }
                    walk_schema_value(key, &pair[0], frames, used, blob_count)?;
                    walk_schema_value(item, &pair[1], frames, used, blob_count)?;
                }
            }
            AuthoredValue::Object(entries) if matches!(key.as_ref(), SchemaNode::String) => {
                for item_value in entries.values() {
                    walk_schema_value(item, item_value, frames, used, blob_count)?;
                }
            }
            _ => return Err(shape("map value does not match its key schema")),
        },
        SchemaNode::BackRef(distance) => {
            let Some((target, reentry)) = reenter(frames, *distance) else {
                return Err(shape("logical schema contains an invalid back-reference"));
            };
            let walked = walk_schema_value(target, value, frames, used, blob_count);
            reentry.restore(frames);
            walked?;
        }
        SchemaNode::Unit if !matches!(value, AuthoredValue::Null) => {
            return Err(shape("unit value must be null"));
        }
        SchemaNode::String if !matches!(value, AuthoredValue::Str(_)) => {
            return Err(shape("string value must be a string"));
        }
        SchemaNode::Primitive(kind) => validate_primitive(*kind, value)?,
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => validate_reference(value)?,
        SchemaNode::String | SchemaNode::Unit => {}
    }
    Ok(())
}

/// A struct body's fields, validated in whatever frame the caller opened:
/// a struct's own, or its enum's for a variant payload. A missing or
/// extra field is named, and an error inside a field is located there.
fn walk_fields<'s>(
    fields: &'s [(String, u32, SchemaNode)],
    value: &AuthoredValue,
    frames: &mut Vec<&'s SchemaNode>,
    used: &mut BTreeSet<u32>,
    blob_count: u32,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: String| AuthoringValueError::SchemaValueShape { detail };
    let AuthoredValue::Object(object) = value else {
        return Err(shape("struct value must be an object".to_owned()));
    };
    if let Some(extra) = object
        .keys()
        .find(|key| !fields.iter().any(|(name, _, _)| name == *key))
    {
        return Err(shape(format!("struct field {extra:?} is not in the schema")));
    }
    for (name, _, field_schema) in fields {
        let field_value = object
            .get(name)
            .ok_or_else(|| shape(format!("missing struct field {name:?}")))?;
        walk_schema_value(field_schema, field_value, frames, used, blob_count)
            .map_err(|error| located(&format!(".{name}"), error))?;
    }
    Ok(())
}

/// `error`, raised inside `part` (`.field` or `[index]`) of the value,
/// located at its path from the value's root, spelled as a bundle error's
/// (`at data.animation.skeleton: missing struct field "name"`).
fn located(part: &str, error: AuthoringValueError) -> AuthoringValueError {
    let AuthoringValueError::SchemaValueShape { detail } = error else {
        return error;
    };
    let detail = match detail.strip_prefix("at data") {
        Some(inner) => format!("at data{part}{inner}"),
        None => format!("at data{part}: {detail}"),
    };
    AuthoringValueError::SchemaValueShape { detail }
}

pub(crate) fn validate_primitive(
    kind: PrimitiveKind,
    value: &AuthoredValue,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: detail.to_owned(),
    };
    let unsigned = |max: u128| match value {
        AuthoredValue::UInt(value) if *value <= max => Ok(()),
        _ => Err(shape(
            "unsigned integer value is out of range or has the wrong JSON kind",
        )),
    };
    let signed = |min: i128, max: i128| match value {
        AuthoredValue::Int(value) if (min..=max).contains(value) => Ok(()),
        AuthoredValue::UInt(value) if *value <= max as u128 => Ok(()),
        _ => Err(shape(
            "signed integer value is out of range or has the wrong JSON kind",
        )),
    };
    match kind {
        PrimitiveKind::Bool if matches!(value, AuthoredValue::Bool(_)) => Ok(()),
        PrimitiveKind::Bool => Err(shape("bool value must be a JSON boolean")),
        PrimitiveKind::U8 => unsigned(u8::MAX.into()),
        PrimitiveKind::U16 => unsigned(u16::MAX.into()),
        PrimitiveKind::U32 => unsigned(u32::MAX.into()),
        PrimitiveKind::U64 => unsigned(u64::MAX.into()),
        PrimitiveKind::U128 => unsigned(u128::MAX),
        PrimitiveKind::I8 => signed(i8::MIN.into(), i8::MAX.into()),
        PrimitiveKind::I16 => signed(i16::MIN.into(), i16::MAX.into()),
        PrimitiveKind::I32 => signed(i32::MIN.into(), i32::MAX.into()),
        PrimitiveKind::I64 => signed(i64::MIN.into(), i64::MAX.into()),
        PrimitiveKind::I128 => signed(i128::MIN, i128::MAX),
        PrimitiveKind::F32 => match value {
            AuthoredValue::Float(value) if (*value as f32).is_finite() => Ok(()),
            AuthoredValue::Int(value) if (*value as f32).is_finite() => Ok(()),
            AuthoredValue::UInt(value) if (*value as f32).is_finite() => Ok(()),
            _ => Err(shape(
                "f32 value must be a finite representable JSON number",
            )),
        },
        PrimitiveKind::F64
            if matches!(
                value,
                AuthoredValue::Float(_) | AuthoredValue::Int(_) | AuthoredValue::UInt(_)
            ) =>
        {
            Ok(())
        }
        PrimitiveKind::F64 => Err(shape("f64 value must be a JSON number")),
        PrimitiveKind::Char => match value {
            AuthoredValue::Str(value) if value.chars().count() == 1 => Ok(()),
            _ => Err(shape("char value must be a one-scalar string")),
        },
    }
}

pub(crate) fn validate_reference(value: &AuthoredValue) -> Result<(), AuthoringValueError> {
    decode_asset_reference_query(value).map(|_| ())
}

pub fn decode_asset_reference_query(
    value: &AuthoredValue,
) -> Result<AssetReferenceQuery, AuthoringValueError> {
    let invalid = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: format!("invalid asset reference query: {detail}"),
    };
    match value {
        AuthoredValue::Str(value) => match value.parse::<AssetUuid>() {
            Ok(uuid) if uuid.to_string() == *value => Ok(AssetReferenceQuery::Uuid(uuid)),
            Ok(_) => Err(invalid(
                "UUID spelling must be canonical lowercase RFC 4122",
            )),
            Err(_) if valid_logical_path(value) => Ok(AssetReferenceQuery::Path {
                normalized_path: value.clone(),
                local_id: None,
            }),
            Err(_) => Err(invalid("bare string is neither a canonical UUID nor path")),
        },
        AuthoredValue::Object(fields) => {
            if fields.is_empty()
                || fields.len() > 2
                || fields.keys().any(|key| key != "path" && key != "asset")
            {
                return Err(invalid("object keys must be path and/or asset exactly"));
            }
            let path = match fields.get("path") {
                Some(AuthoredValue::Str(path)) if valid_logical_path(path) => Some(path.clone()),
                Some(_) => return Err(invalid("path must use the canonical logical-path grammar")),
                None => None,
            };
            let local_id = match fields.get("asset") {
                Some(AuthoredValue::Str(asset)) if valid_reference_local_id(asset) => {
                    Some(asset.clone())
                }
                Some(_) => {
                    return Err(invalid(
                        "asset must be a canonical non-reserved bundle-local id",
                    ))
                }
                None => None,
            };
            match (path, local_id) {
                (Some(normalized_path), local_id) => Ok(AssetReferenceQuery::Path {
                    normalized_path,
                    local_id,
                }),
                (None, Some(local_id)) => Ok(AssetReferenceQuery::SameBundleLocalId(local_id)),
                (None, None) => Err(invalid("object must select a path or local id")),
            }
        }
        _ => Err(invalid("reference must be a string or selector object")),
    }
}

pub(crate) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value.contains('\0')
        && value.nfc().eq(value.chars())
}

pub(crate) fn valid_reference_local_id(value: &str) -> bool {
    valid_identifier(value) && !value.starts_with('$')
}

pub(crate) fn valid_bundle_local_id(value: &str) -> bool {
    valid_reference_local_id(value) || matches!(value, "$settings" | "$record")
}

pub(crate) fn valid_logical_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component.nfc().eq(component.chars())
        })
}

pub(crate) fn valid_logical_path_prefix(path: &str) -> bool {
    path.is_empty()
        || (!path.starts_with('/')
            && !path.contains('\\')
            && !path.contains('\0')
            && path.split('/').all(|component| {
                !component.is_empty()
                    && component != "."
                    && component != ".."
                    && component.nfc().eq(component.chars())
            }))
}

pub(crate) fn path_glob_matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.as_bytes();
    let path = path.as_bytes();
    let mut previous = vec![false; path.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; path.len() + 1];
        match token {
            b'*' => {
                current[0] = previous[0];
                for index in 1..=path.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            b'?' => {
                current[1..].copy_from_slice(&previous[..path.len()]);
            }
            literal => {
                for index in 1..=path.len() {
                    current[index] = previous[index - 1] && path[index - 1] == *literal;
                }
            }
        }
        previous = current;
    }
    previous[path.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materializing_reenters_a_backref_under_its_own_ancestors() {
        // A { b: Option<B> }, B { a: Option<A>, b: Option<B> }: inside the B
        // that B.b re-enters, `a` is BackRef(1) and names A.
        let option = |inner| SchemaNode::Option(Box::new(inner));
        let record = |fields: Vec<(&str, SchemaNode)>| SchemaNode::Struct {
            rev: 0,
            fields: fields
                .into_iter()
                .map(|(name, node)| (name.to_owned(), 0, node))
                .collect(),
        };
        let schema = record(vec![(
            "b",
            option(record(vec![
                ("a", option(SchemaNode::BackRef(1))),
                ("b", option(SchemaNode::BackRef(0))),
            ])),
        )]);
        let text = r#"{"b":{"a":null,"b":{"a":{"b":null},"b":null}}}"#;
        let mut value = distill_json::parse(text).unwrap();
        materialize_blob_tokens(&schema, &mut value, &mut Vec::new(), &[]);
        assert_eq!(value, distill_json::parse(text).unwrap());
    }

    /// `{ skeleton: Option<{ path: String, name: Option<String> }>, rate: u8 }`,
    /// the shape of `GltfImportSettings.animation`'s `skeleton` beside a
    /// plain field.
    fn optional_struct_settings() -> SchemaNode {
        let record = |fields: Vec<(&str, SchemaNode)>| SchemaNode::Struct {
            rev: 0,
            fields: fields
                .into_iter()
                .map(|(name, node)| (name.to_owned(), 0, node))
                .collect(),
        };
        record(vec![
            (
                "skeleton",
                SchemaNode::Option(Box::new(record(vec![
                    ("path", SchemaNode::String),
                    ("name", SchemaNode::Option(Box::new(SchemaNode::String))),
                ]))),
            ),
            ("rate", SchemaNode::Primitive(PrimitiveKind::U8)),
        ])
    }

    /// `text` completed from `defaults` under [`optional_struct_settings`],
    /// and its validation.
    fn complete(
        text: &str,
        defaults: &str,
    ) -> (AuthoredValue, Result<(), AuthoringValueError>) {
        let schema = optional_struct_settings();
        let mut value = distill_json::parse(text).unwrap();
        complete_settings(&schema, &mut value, &distill_json::parse(defaults).unwrap());
        let walked = walk_schema_value(&schema, &value, &mut Vec::new(), &mut BTreeSet::new(), 0);
        (value, walked)
    }

    #[test]
    fn an_object_over_a_null_default_leaves_absent_option_fields_none() {
        let (value, walked) = complete(
            r#"{"skeleton":{"path":"characters/fox_rig.gltf"}}"#,
            r#"{"rate":30,"skeleton":null}"#,
        );
        walked.unwrap();
        assert_eq!(
            value,
            distill_json::parse(
                r#"{"rate":30,"skeleton":{"name":null,"path":"characters/fox_rig.gltf"}}"#
            )
            .unwrap()
        );
    }

    #[test]
    fn an_object_over_a_null_default_missing_a_required_field_names_it() {
        let (_, walked) = complete(
            r#"{"skeleton":{"name":"Armature"}}"#,
            r#"{"rate":30,"skeleton":null}"#,
        );
        let Err(AuthoringValueError::SchemaValueShape { detail }) = walked else {
            panic!("expected a shape error, got {walked:?}");
        };
        assert_eq!(detail, r#"at data.skeleton: missing struct field "path""#);
    }

    #[test]
    fn a_full_object_stays_as_authored() {
        let full = r#"{"rate":2,"skeleton":{"name":"Armature","path":"rig.gltf"}}"#;
        for defaults in [
            r#"{"rate":30,"skeleton":null}"#,
            r#"{"rate":30,"skeleton":{"name":null,"path":"other.gltf"}}"#,
        ] {
            let (value, walked) = complete(full, defaults);
            walked.unwrap();
            assert_eq!(value, distill_json::parse(full).unwrap());
        }
        // An authored `null` over a present default stays `None`.
        let (value, walked) = complete(
            r#"{"skeleton":null}"#,
            r#"{"rate":30,"skeleton":{"name":null,"path":"other.gltf"}}"#,
        );
        walked.unwrap();
        assert_eq!(
            value,
            distill_json::parse(r#"{"rate":30,"skeleton":null}"#).unwrap()
        );
    }

    #[test]
    fn an_option_struct_over_a_present_default_completes_from_it() {
        let (value, walked) = complete(
            r#"{"skeleton":{"path":"rig.gltf"}}"#,
            r#"{"rate":30,"skeleton":{"name":"Armature","path":"other.gltf"}}"#,
        );
        walked.unwrap();
        assert_eq!(
            value,
            distill_json::parse(r#"{"rate":30,"skeleton":{"name":"Armature","path":"rig.gltf"}}"#)
                .unwrap()
        );
    }

    /// Completed settings are what an output's `$settings` records and
    /// reads back: canonical JSON round trips them, and completing the
    /// read-back record (or the completed value) again changes nothing, so
    /// an unchanged rule matches its output's record on every pass.
    #[test]
    fn completed_settings_are_canonical_and_complete_to_themselves() {
        let defaults = r#"{"rate":30,"skeleton":null}"#;
        let (value, walked) = complete(r#"{"skeleton":{"path":"rig.gltf"}}"#, defaults);
        walked.unwrap();
        let written = distill_json::write(&value).unwrap();
        assert_eq!(written, r#"{"rate":30,"skeleton":{"name":null,"path":"rig.gltf"}}"#);
        let read_back = distill_json::parse(&written).unwrap();
        assert_eq!(read_back, value);
        let (again, walked) = complete(&written, defaults);
        walked.unwrap();
        assert_eq!(again, value);
    }
}

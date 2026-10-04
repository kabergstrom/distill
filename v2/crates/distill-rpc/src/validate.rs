//! Commit, diagnostic, and authored-value validation shared by the RPC
//! server and the publication paths that apply commits to the store.

use std::collections::BTreeSet;
use std::sync::Arc;

use unicode_normalization::UnicodeNormalization;

use distill_json::AuthoredValue;
use distill_schema::ngp_schema::{verify_snapshot, PrimitiveKind, SchemaNode};

use crate::*;

pub(crate) fn validate_commit(commit: &Commit) -> Result<(), AdminError> {
    if let Some(ConfigurationStatus::Failed(error)) = &commit.configuration {
        error
            .validate()
            .map_err(|error| AdminError::InvalidConfigurationError { error })?;
    }
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

pub(crate) fn materialize_blob_tokens(
    schema: &SchemaNode,
    value: &mut AuthoredValue,
    frames: &mut Vec<SchemaNode>,
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
            frames.push(schema.clone());
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
            frames.push(schema.clone());
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
            let target = frames[frames.len() - (*distance as usize + 1)].clone();
            materialize_blob_tokens(&target, value, frames, blobs);
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
fn materialize_fields(
    fields: &[(String, u32, SchemaNode)],
    value: &mut AuthoredValue,
    frames: &mut Vec<SchemaNode>,
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

pub(crate) fn walk_schema_value(
    schema: &SchemaNode,
    value: &AuthoredValue,
    frames: &mut Vec<SchemaNode>,
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
            frames.push(schema.clone());
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
            frames.push(schema.clone());
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
            for value in values {
                walk_schema_value(element, value, frames, used, blob_count)?;
            }
        }
        SchemaNode::Array { len, elem } => {
            let AuthoredValue::Array(values) = value else {
                return Err(shape("array value must be an array"));
            };
            if usize::try_from(*len).ok() != Some(values.len()) {
                return Err(shape("array value length does not match schema"));
            }
            for value in values {
                walk_schema_value(elem, value, frames, used, blob_count)?;
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
            let distance = usize::try_from(*distance).expect("u32 fits usize");
            let Some(target) = frames
                .len()
                .checked_sub(distance + 1)
                .and_then(|index| frames.get(index))
                .cloned()
            else {
                return Err(shape("logical schema contains an invalid back-reference"));
            };
            walk_schema_value(&target, value, frames, used, blob_count)?;
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
/// a struct's own, or its enum's for a variant payload.
fn walk_fields(
    fields: &[(String, u32, SchemaNode)],
    value: &AuthoredValue,
    frames: &mut Vec<SchemaNode>,
    used: &mut BTreeSet<u32>,
    blob_count: u32,
) -> Result<(), AuthoringValueError> {
    let shape = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: detail.to_owned(),
    };
    let AuthoredValue::Object(object) = value else {
        return Err(shape("struct value must be an object"));
    };
    if object.len() != fields.len() {
        return Err(shape("struct value field set does not match schema"));
    }
    for (name, _, field_schema) in fields {
        let field_value = object
            .get(name)
            .ok_or_else(|| shape("struct value is missing a schema field"))?;
        walk_schema_value(field_schema, field_value, frames, used, blob_count)?;
    }
    Ok(())
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

//! Commit, diagnostic, and authored-value validation shared by the RPC
//! server and the publication paths that apply commits to the store.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use unicode_normalization::UnicodeNormalization;

use distill_schema::ngp_schema::{
    display_path, node_hash, AssetRefValue, BlobData, Frames, LogicalSchema, Patch, PathComponent, SchemaNode, TypeTable,
    Value,
};

use crate::*;

/// Check a carried value against its schema (`ngp_value::check`): it fits,
/// each carried type is named by one of its dynamic values, and every asset
/// reference is a canonical query (§4). Blobs may be by reference (an
/// inspection's value checks without their bytes). Returns the carried
/// types as a table.
pub fn check_authoring_value(schema: &LogicalSchema, value: &AuthoringValue) -> Result<TypeTable, AuthoringValueError> {
    let types = value.type_table()?;
    ngp_value::check(ngp_value::Cx::new(schema, &types), &value.value)
        .map_err(|error| AuthoringValueError::SchemaValueShape { detail: error.to_string() })?;
    let mut named = BTreeSet::new();
    distill_schema::ngp_schema::dynamic_type_uuids(&value.value, &mut named);
    if let Some(unused) = types.types().iter().find(|ty| !named.contains(&ty.uuid)) {
        return Err(AuthoringValueError::UnusedType { path: unused.path.clone() });
    }
    validate_references(&value.value)?;
    Ok(types)
}

/// [`check_authoring_value`] for a value to store: every blob must carry its
/// bytes (a writer resolves references first, [`resolve_blob_refs`]).
/// Returns the value and its types.
pub fn stored_value(schema: &LogicalSchema, value: &AuthoringValue) -> Result<(Value, TypeTable), AuthoringValueError> {
    let types = check_authoring_value(schema, value)?;
    let mut by_ref = None;
    ngp_value::value_blobs(&value.value, &mut |blob| {
        if let BlobData::Ref(blob) = blob {
            by_ref.get_or_insert(*blob);
        }
    });
    if let Some(blob) = by_ref {
        return Err(AuthoringValueError::UnknownBlobRef { blob });
    }
    Ok(((*value.value).clone(), types))
}

/// An importer's settings as an import request carries them: a sparse
/// patch under the importer's settings `schema`, completed from the
/// importer's `defaults` by [`complete_settings`].
pub fn settings_value(
    schema: &LogicalSchema,
    settings: &AuthoringPatch,
    defaults: &Value,
) -> Result<Value, AuthoringValueError> {
    let types = crate::codec::type_table(&settings.types)?;
    ngp_value::check_patch(ngp_value::Cx::new(schema, &types), &settings.patch)
        .map_err(|error| AuthoringValueError::SchemaValueShape { detail: error.to_string() })?;
    let value = complete_settings(&schema.root, (*settings.patch).clone(), defaults)?;
    validate_references(&value)?;
    Ok(value)
}

/// Whether `value` names a blob by reference.
pub fn names_blob_ref(value: &Value) -> bool {
    let mut named = false;
    ngp_value::value_blobs(value, &mut |blob| named |= matches!(blob, BlobData::Ref(_)));
    named
}

/// `value` with every blob it names by reference replaced by the bytes
/// `held` has for it (a bundle's blobs by [`BlobRef`]): what a write stores.
/// A reference `held` lacks fails ([`AuthoringValueError::UnknownBlobRef`]).
pub fn resolve_blob_refs(
    value: &AuthoringValue,
    held: &BTreeMap<BlobRef, Arc<[u8]>>,
) -> Result<AuthoringValue, AuthoringValueError> {
    let mut resolved = (*value.value).clone();
    let mut missing = None;
    ngp_value::value_blobs_mut(&mut resolved, &mut |blob| {
        if let BlobData::Ref(r) = blob {
            match held.get(r) {
                Some(bytes) => *blob = BlobData::Bytes(bytes.clone()),
                None => {
                    missing.get_or_insert(*r);
                }
            }
        }
    });
    if let Some(blob) = missing {
        return Err(AuthoringValueError::UnknownBlobRef { blob });
    }
    Ok(AuthoringValue { value: Arc::new(resolved), types: value.types.clone() })
}

/// Every blob `value` holds with its bytes, by reference.
pub fn value_blobs(value: &Value, out: &mut BTreeMap<BlobRef, Arc<[u8]>>) {
    ngp_value::value_blobs(value, &mut |blob| {
        if let BlobData::Bytes(bytes) = blob {
            out.entry(BlobRef::of(bytes)).or_insert_with(|| bytes.clone());
        }
    });
}

/// The bytes of the blob `value` holds as `blob`, if it holds them.
pub fn value_blob(value: &Value, blob: BlobRef) -> Option<Arc<[u8]>> {
    let mut found = None;
    ngp_value::value_blobs(value, &mut |held| {
        if let BlobData::Bytes(bytes) = held {
            if found.is_none() && bytes.len() as u64 == blob.len && BlobRef::of(bytes) == blob {
                found = Some(bytes.clone());
            }
        }
    });
    found
}

/// An RPC authored value carrying `value`, its blobs' bytes and the types
/// of `types` its dynamic values name: what a client may write into a new
/// entry (a default, importer settings).
pub fn authoring_value(value: &Value, types: &TypeTable) -> Result<AuthoringValue, String> {
    Ok(AuthoringValue { value: Arc::new(value.clone()), types: carried_types(value, types)? })
}

/// [`authoring_value`] with each blob by [`BlobRef`]: what an inspection
/// carries (§17 "Authoring snapshots"; the bytes through
/// `AuthoringSnapshot.blob`).
pub fn authoring_value_by_ref(value: &Value, types: &TypeTable) -> Result<AuthoringValue, String> {
    let mut value = value.clone();
    ngp_value::value_blobs_mut(&mut value, &mut |blob| *blob = BlobData::Ref(blob.blob_ref()));
    let types = carried_types(&value, types)?;
    Ok(AuthoringValue { value: Arc::new(value), types })
}

/// The types of `types` `value`'s dynamic values name, in path order.
fn carried_types(value: &Value, types: &TypeTable) -> Result<Vec<AuthoringType>, String> {
    let mut named = BTreeSet::new();
    distill_schema::ngp_schema::dynamic_type_uuids(value, &mut named);
    let mut carried = Vec::with_capacity(named.len());
    for ty in types.subset(&named)?.types() {
        carried.push(AuthoringType {
            path: ty.path.clone(),
            type_uuid: ty.uuid,
            schema_hash: node_hash(&ty.schema.root)
                .map_err(|error| format!("cannot hash the schema of {}: {error:?}", ty.path))?,
            schema: ty.schema.clone(),
        });
    }
    carried.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(carried)
}

/// The content key of an authored entry (§17 "Content keys"): its value
/// digest (newgameplus GAP 23 §17.3) over its schema, the types its
/// dynamic values name and its encoding, blobs by reference. Equal keys
/// mean equal values, whatever text stores them.
pub fn content_key(schema: &LogicalSchema, value: &Value, types: &TypeTable) -> Result<[u8; 16], String> {
    ngp_value::digest(ngp_value::Cx::new(schema, types), value).map_err(|error| format!("cannot digest the value: {error}"))
}

/// [`content_key`] of a carried value.
pub fn content_key_of(schema: &LogicalSchema, value: &AuthoringValue) -> Result<[u8; 16], String> {
    let types = value.type_table().map_err(|error| format!("{error:?}"))?;
    content_key(schema, &value.value, &types)
}

/// Complete an importer-settings patch from `defaults`, the importer's
/// default settings, into a whole value: a field the patch leaves out takes
/// the default's, a present struct field is completed the same way, and a
/// present `Option` is completed as its element. Anything else stays as
/// authored (an enum keeps its authored variant whole, a map or sequence
/// its authored entries). Where there is no default (an object authored
/// over a `None` default, an enum payload, a sequence element), a missing
/// `Option` field is `None` and a missing required field is an error
/// naming it. A default holding a blob is never filled in: a blob is
/// authored. Completing a completed value changes nothing.
pub fn complete_settings(
    schema: &SchemaNode,
    patch: Patch,
    defaults: &Value,
) -> Result<Value, AuthoringValueError> {
    let mut path = Vec::new();
    complete(schema, patch, Some(defaults), &mut Frames::new(), &mut path).map_err(|detail| {
        AuthoringValueError::SchemaValueShape {
            detail: format!("{}: {detail}", display_path("data", &path)),
        }
    })
}

fn complete<'s>(
    node: &'s SchemaNode,
    patch: Patch,
    default: Option<&Value>,
    frames: &mut Frames<'s>,
    path: &mut Vec<PathComponent>,
) -> Result<Value, String> {
    frames
        .enter(node, |node, frames| -> Result<Value, String> {
            match (node, patch) {
                (_, Patch::Replace(value)) => Ok(value),
                (SchemaNode::Struct { fields, .. }, Patch::Struct(slots)) => {
                    let defaults = match default {
                        Some(Value::Struct(values)) if values.len() == fields.len() => {
                            Some(values)
                        }
                        _ => None,
                    };
                    complete_fields(fields, slots, defaults.map(|d| &d[..]), frames, path)
                }
                (SchemaNode::Enum { variants, .. }, Patch::Enum { variant, payload }) => {
                    let Some((name, _, SchemaNode::Struct { fields, .. })) =
                        variants.get(variant as usize)
                    else {
                        return Err(format!("variant index {variant} is not declared"));
                    };
                    path.push(PathComponent::Variant(name.clone()));
                    let payload = complete_fields(fields, payload, None, frames, path)?;
                    path.pop();
                    let Value::Struct(payload) = payload else {
                        unreachable!("fields complete to a struct")
                    };
                    Ok(Value::Enum { variant, payload })
                }
                (SchemaNode::Option(inner), Patch::Some(patch)) => {
                    let default = match default {
                        Some(Value::Option(Some(value))) => Some(&**value),
                        _ => None,
                    };
                    let value = complete(inner, *patch, default, frames, path)?;
                    Ok(Value::Option(Some(Box::new(value))))
                }
                (
                    SchemaNode::Vec(elem) | SchemaNode::Array { elem, .. },
                    Patch::Seq(patches),
                ) => {
                    let mut items = Vec::with_capacity(patches.len());
                    for (index, patch) in patches.into_iter().enumerate() {
                        path.push(PathComponent::Index(index as u64));
                        items.push(complete(elem, patch, None, frames, path)?);
                        path.pop();
                    }
                    Ok(Value::Seq(items))
                }
                (SchemaNode::Map { value, .. }, Patch::Map { set, remove }) => {
                    if !remove.is_empty() {
                        return Err("settings maps take no removals".to_owned());
                    }
                    let mut entries = Vec::with_capacity(set.len());
                    for (index, (key, patch)) in set.into_iter().enumerate() {
                        path.push(PathComponent::Index(index as u64));
                        entries.push((key, complete(value, patch, None, frames, path)?));
                        path.pop();
                    }
                    Ok(Value::Map(entries))
                }
                (node, _) => Err(format!("a patch that is not one of {}", node_kind(node))),
            }
        })
        .unwrap_or_else(|distance| Err(format!("back-reference {distance} escapes its frames")))
}

fn complete_fields<'s>(
    fields: &'s [(String, u32, SchemaNode)],
    slots: Box<[Option<Patch>]>,
    defaults: Option<&[Value]>,
    frames: &mut Frames<'s>,
    path: &mut Vec<PathComponent>,
) -> Result<Value, String> {
    if slots.len() != fields.len() {
        return Err(format!(
            "a patch of {} fields for a struct of {}",
            slots.len(),
            fields.len()
        ));
    }
    let mut values = Vec::with_capacity(fields.len());
    for (index, ((name, _, field), slot)) in fields.iter().zip(slots.into_vec()).enumerate() {
        let default = defaults.map(|defaults| &defaults[index]);
        let value = match (slot, default) {
            (Some(patch), default) => {
                path.push(PathComponent::Field(name.clone()));
                let value = complete(field, patch, default, frames, path)?;
                path.pop();
                value
            }
            (None, Some(default)) if !holds_blob(default) => default.clone(),
            (None, _) if matches!(field, SchemaNode::Option(_)) => Value::Option(None),
            (None, _) => return Err(format!("missing struct field {name:?}")),
        };
        values.push(value);
    }
    Ok(Value::Struct(values.into_boxed_slice()))
}

fn node_kind(node: &SchemaNode) -> &'static str {
    match node {
        SchemaNode::Struct { .. } => "struct",
        SchemaNode::Enum { .. } => "enum",
        SchemaNode::Option(_) => "option",
        SchemaNode::Vec(_) | SchemaNode::Array { .. } | SchemaNode::Set(_) => "sequence",
        SchemaNode::Map { .. } => "map",
        _ => "leaf",
    }
}

fn holds_blob(value: &Value) -> bool {
    match value {
        Value::Blob(_) => true,
        Value::Struct(items) => items.iter().any(holds_blob),
        Value::Seq(items) => items.iter().any(holds_blob),
        Value::Enum { payload, .. } => payload.iter().any(holds_blob),
        Value::Option(Some(inner)) => holds_blob(inner),
        Value::Map(entries) => entries.iter().any(|(k, v)| holds_blob(k) || holds_blob(v)),
        _ => false,
    }
}

/// Every asset reference in `value` is a canonical query (§4).
fn validate_references(value: &Value) -> Result<(), AuthoringValueError> {
    match value {
        Value::AssetRef(query) | Value::WeakRef(query) => {
            decode_asset_reference_query(query).map(drop)
        }
        Value::Struct(items) => items.iter().try_for_each(validate_references),
        Value::Seq(items) => items.iter().try_for_each(validate_references),
        Value::Enum { payload, .. } => payload.iter().try_for_each(validate_references),
        Value::Option(Some(inner)) => validate_references(inner),
        Value::Map(entries) => entries
            .iter()
            .try_for_each(|(k, v)| validate_references(k).and_then(|()| validate_references(v))),
        _ => Ok(()),
    }
}

pub fn decode_asset_reference_query(
    value: &AssetRefValue,
) -> Result<AssetReferenceQuery, AuthoringValueError> {
    let invalid = |detail: &str| AuthoringValueError::SchemaValueShape {
        detail: format!("invalid asset reference query: {detail}"),
    };
    match value {
        AssetRefValue::Path(value) => match value.parse::<AssetUuid>() {
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
        AssetRefValue::Query { path, asset } => {
            let path = match path {
                Some(path) if valid_logical_path(path) => Some(path.clone()),
                Some(_) => return Err(invalid("path must use the canonical logical-path grammar")),
                None => None,
            };
            let local_id = match asset {
                Some(asset) if valid_reference_local_id(asset) => Some(asset.clone()),
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
    use distill_schema::ngp_schema::{parse_patch, parse_value, write_value, Options, PrimitiveKind, WriteOptions};

    /// `{ skeleton: Option<{ path: String, name: Option<String> }>, rate: u8 }`,
    /// the shape of `GltfImportSettings.animation`'s `skeleton` beside a
    /// plain field (fields in node order: name-sorted).
    fn optional_struct_settings() -> SchemaNode {
        let record = |fields: Vec<(&str, SchemaNode)>| SchemaNode::Struct {
            rev: 0,
            fields: fields
                .into_iter()
                .map(|(name, node)| (name.to_owned(), 0, node))
                .collect(),
        };
        record(vec![
            ("rate", SchemaNode::Primitive(PrimitiveKind::U8)),
            (
                "skeleton",
                SchemaNode::Option(Box::new(record(vec![
                    ("name", SchemaNode::Option(Box::new(SchemaNode::String))),
                    ("path", SchemaNode::String),
                ]))),
            ),
        ])
    }

    fn value(text: &str) -> Value {
        parse_value(text, &optional_struct_settings(), Options::canonical()).unwrap()
    }

    /// `text` (a patch) completed from `defaults` under
    /// [`optional_struct_settings`].
    fn complete(text: &str, defaults: &str) -> Result<Value, AuthoringValueError> {
        let schema = optional_struct_settings();
        let patch = parse_patch(text, &schema, Options::canonical()).unwrap();
        complete_settings(&schema, patch, &value(defaults))
    }

    #[test]
    fn an_object_over_a_null_default_leaves_absent_option_fields_none() {
        let completed = complete(
            r#"{"skeleton":{"path":"characters/fox_rig.gltf"}}"#,
            r#"{"rate":30,"skeleton":null}"#,
        )
        .unwrap();
        assert_eq!(
            completed,
            value(r#"{"rate":30,"skeleton":{"name":null,"path":"characters/fox_rig.gltf"}}"#)
        );
    }

    #[test]
    fn an_object_over_a_null_default_missing_a_required_field_names_it() {
        let completed = complete(
            r#"{"skeleton":{"name":"Armature"}}"#,
            r#"{"rate":30,"skeleton":null}"#,
        );
        let Err(AuthoringValueError::SchemaValueShape { detail }) = completed else {
            panic!("expected a shape error, got {completed:?}");
        };
        assert_eq!(detail, r#"data.skeleton: missing struct field "path""#);
    }

    #[test]
    fn a_full_object_stays_as_authored() {
        let full = r#"{"rate":2,"skeleton":{"name":"Armature","path":"rig.gltf"}}"#;
        for defaults in [
            r#"{"rate":30,"skeleton":null}"#,
            r#"{"rate":30,"skeleton":{"name":null,"path":"other.gltf"}}"#,
        ] {
            assert_eq!(complete(full, defaults).unwrap(), value(full));
        }
        // An authored `null` over a present default stays `None`.
        assert_eq!(
            complete(
                r#"{"skeleton":null}"#,
                r#"{"rate":30,"skeleton":{"name":null,"path":"other.gltf"}}"#,
            )
            .unwrap(),
            value(r#"{"rate":30,"skeleton":null}"#)
        );
    }

    #[test]
    fn an_option_struct_over_a_present_default_completes_from_it() {
        assert_eq!(
            complete(
                r#"{"skeleton":{"path":"rig.gltf"}}"#,
                r#"{"rate":30,"skeleton":{"name":"Armature","path":"other.gltf"}}"#,
            )
            .unwrap(),
            value(r#"{"rate":30,"skeleton":{"name":"Armature","path":"rig.gltf"}}"#)
        );
    }

    /// Completed settings are what an output's `$settings` records and
    /// reads back: canonical text round trips them, and completing the
    /// read-back record again changes nothing, so an unchanged rule matches
    /// its output's record on every pass.
    #[test]
    fn completed_settings_are_canonical_and_complete_to_themselves() {
        let schema = optional_struct_settings();
        let defaults = r#"{"rate":30,"skeleton":null}"#;
        let completed = complete(r#"{"skeleton":{"path":"rig.gltf"}}"#, defaults).unwrap();
        let written = write_value(&schema, &completed, WriteOptions::default()).unwrap();
        assert_eq!(written, r#"{"rate":30,"skeleton":{"name":null,"path":"rig.gltf"}}"#);
        assert_eq!(value(&written), completed);
        assert_eq!(complete(&written, defaults).unwrap(), completed);
    }

    #[test]
    fn blobs_travel_inside_the_value_by_reference_and_resolve_from_the_holder() {
        let schema = LogicalSchema {
            root: SchemaNode::Struct {
                rev: 0,
                fields: vec![("a".to_owned(), 0, SchemaNode::Blob), ("b".to_owned(), 0, SchemaNode::Blob)],
            },
        };
        let value = Value::Struct(Box::new([Value::Blob(vec![1, 2].into()), Value::Blob(vec![3].into())]));
        let carried = authoring_value_by_ref(&value, &Default::default()).unwrap();
        // By reference only; the bytes come back from what holds them.
        assert_eq!(
            *carried.value,
            Value::Struct(Box::new([
                Value::Blob(BlobData::Ref(BlobRef::of(&[1, 2]))),
                Value::Blob(BlobData::Ref(BlobRef::of(&[3]))),
            ]))
        );
        // Bytes and references digest alike: the content key does not
        // depend on whether the carrier holds the bytes.
        assert_eq!(
            content_key_of(&schema, &carried).unwrap(),
            content_key(&schema, &value, &Default::default()).unwrap()
        );
        let mut held = BTreeMap::new();
        value_blobs(&value, &mut held);
        let resolved = resolve_blob_refs(&carried, &held).unwrap();
        assert_eq!(stored_value(&schema, &resolved).unwrap().0, value);
        assert_eq!(value_blob(&value, BlobRef::of(&[3])).as_deref(), Some(&[3u8][..]));
        assert_eq!(
            resolve_blob_refs(&carried, &BTreeMap::new()),
            Err(AuthoringValueError::UnknownBlobRef { blob: BlobRef::of(&[1, 2]) })
        );
    }

    /// A value naming its blobs by reference checks against its schema
    /// without their bytes; storing it needs them.
    #[test]
    fn a_value_with_blob_refs_checks_without_bytes() {
        let schema = LogicalSchema { root: SchemaNode::Struct { rev: 0, fields: vec![("a".to_owned(), 0, SchemaNode::Blob)] } };
        let value = Value::Struct(Box::new([Value::Blob(vec![7; 40].into())]));
        let carried = authoring_value_by_ref(&value, &Default::default()).unwrap();
        check_authoring_value(&schema, &carried).unwrap();
        assert_eq!(
            stored_value(&schema, &carried).map(drop),
            Err(AuthoringValueError::UnknownBlobRef { blob: BlobRef::of(&[7; 40]) })
        );
        let wrong = AuthoringValue { value: Arc::new(Value::Struct(Box::new([Value::U8(1)]))), types: Vec::new() };
        assert!(check_authoring_value(&schema, &wrong).is_err());
    }
}

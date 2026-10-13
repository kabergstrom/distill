//! Commit, diagnostic, and authored-value validation shared by the RPC
//! server and the publication paths that apply commits to the store.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use unicode_normalization::UnicodeNormalization;

use distill_schema::ngp_schema::{
    display_path, parse_patch, parse_value, verify_snapshot, write_value, AssetRefValue,
    Blob, BlobSink, BlobSource, Frames, LogicalSchema, Options, Patch, PathComponent, SchemaNode,
    TextError, Value, WriteOptions,
};

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
/// schema-shaped value stored in a bundle: canonical text parsed against
/// the schema (`ngp_schema::parse_value`), blobs as `{"len","offset"}`
/// placements whose `offset` indexes the carrier's blob table.
pub fn decode_authoring_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
) -> Result<Value, AuthoringValueError> {
    let schema = verified_schema(schema_hash, logical_schema)?;
    let text = value_text(authored_value)?;
    let mut table = BlobTable::new(&authored_value.blobs);
    let value = parse_value(text, &schema.root, Options::canonical().blobs(&mut table))
        .map_err(|error| table.error(error))?;
    table.all_used()?;
    let written = write_value(
        &schema.root,
        &value,
        WriteOptions {
            blobs: Some(&mut table),
            types: None,
        },
    )
    .map_err(|error| AuthoringValueError::ValueInvalid(error.render("data")))?;
    if written != text {
        return Err(AuthoringValueError::ValueNotCanonical);
    }
    validate_references(&value)?;
    Ok(value)
}

/// An importer's settings as an import request carries them: authored text
/// (`Mode::Authored`: any key order and whitespace, the authoring
/// spellings), read as a sparse patch (`ngp_schema::parse_patch`) and
/// completed from the importer's `defaults` by [`complete_settings`].
pub fn decode_settings_payload(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
    authored_value: &AuthoringValue,
    defaults: &Value,
) -> Result<Value, AuthoringValueError> {
    let schema = verified_schema(schema_hash, logical_schema)?;
    let text = value_text(authored_value)?;
    let mut table = BlobTable::new(&authored_value.blobs);
    let patch = parse_patch(text, &schema.root, Options::authored().blobs(&mut table))
        .map_err(|error| table.error(error))?;
    table.all_used()?;
    let value = complete_settings(&schema.root, patch, defaults)?;
    validate_references(&value)?;
    Ok(value)
}

fn verified_schema(
    schema_hash: LogicalHash,
    logical_schema: &[u8],
) -> Result<LogicalSchema, AuthoringValueError> {
    let schema_text =
        std::str::from_utf8(logical_schema).map_err(|_| AuthoringValueError::LogicalSchemaUtf8)?;
    verify_snapshot(schema_text, schema_hash)
        .map_err(|error| AuthoringValueError::LogicalSchemaInvalid(error.to_string()))
}

fn value_text(authored_value: &AuthoringValue) -> Result<&str, AuthoringValueError> {
    std::str::from_utf8(&authored_value.canonical_value)
        .map_err(|_| AuthoringValueError::ValueInvalid("value is not UTF-8".to_owned()))
}

/// The carrier's blob table, read by placement (`offset` = index, `len`
/// its byte length) and written back the same way. Each blob is placed
/// exactly once.
struct BlobTable<'b> {
    blobs: &'b [Arc<[u8]>],
    used: Vec<bool>,
    /// Index of each placed blob by its path, for the canonical rewrite.
    placed: BTreeMap<Vec<PathComponent>, u64>,
    error: Option<AuthoringValueError>,
}

impl<'b> BlobTable<'b> {
    fn new(blobs: &'b [Arc<[u8]>]) -> Self {
        Self {
            blobs,
            used: vec![false; blobs.len()],
            placed: BTreeMap::new(),
            error: None,
        }
    }

    fn blob_count(&self) -> u32 {
        u32::try_from(self.blobs.len()).unwrap_or(u32::MAX)
    }

    /// A parse error, or the blob-table error that caused it.
    fn error(&mut self, error: TextError) -> AuthoringValueError {
        self.error
            .take()
            .unwrap_or_else(|| AuthoringValueError::SchemaValueShape {
                detail: error.render("data"),
            })
    }

    fn all_used(&self) -> Result<(), AuthoringValueError> {
        match self.used.iter().position(|used| !used) {
            Some(index) => Err(AuthoringValueError::UnusedBlobIndex {
                index: index as u32,
            }),
            None => Ok(()),
        }
    }
}

impl BlobSource for BlobTable<'_> {
    fn blob(&mut self, path: &[PathComponent], len: u64, offset: u64) -> Result<Blob, String> {
        let blob_count = self.blob_count();
        let Some(index) = u32::try_from(offset).ok().filter(|index| *index < blob_count) else {
            let index = u32::try_from(offset).unwrap_or(u32::MAX);
            self.error = Some(AuthoringValueError::BlobIndexOutOfRange { index, blob_count });
            return Err("blob index out of range".to_owned());
        };
        if std::mem::replace(&mut self.used[index as usize], true) {
            self.error = Some(AuthoringValueError::DuplicateBlobIndex { index });
            return Err("blob index placed twice".to_owned());
        }
        let bytes = &self.blobs[index as usize];
        if bytes.len() as u64 != len {
            return Err(format!(
                "blob {index} holds {} bytes, the placement says {len}",
                bytes.len()
            ));
        }
        self.placed.insert(path.to_vec(), offset);
        Ok(bytes.to_vec())
    }
}

impl BlobSink for BlobTable<'_> {
    fn place(&mut self, path: &[PathComponent], blob: &[u8]) -> Result<(u64, u64), String> {
        let offset = *self
            .placed
            .get(path)
            .ok_or_else(|| "blob was not read from the table".to_owned())?;
        Ok((blob.len() as u64, offset))
    }
}

/// An RPC authored value carrying `value` (a value of `schema`):
/// canonical text, each blob a `{"len","offset"}` placement indexing the
/// blob table in placement order.
pub fn authoring_value(schema: &SchemaNode, value: &Value) -> Result<AuthoringValue, String> {
    struct Table(Vec<Arc<[u8]>>);
    impl BlobSink for Table {
        fn place(&mut self, _: &[PathComponent], blob: &[u8]) -> Result<(u64, u64), String> {
            self.0.push(Arc::from(blob));
            Ok((blob.len() as u64, self.0.len() as u64 - 1))
        }
    }
    let mut table = Table(Vec::new());
    let text = write_value(
        schema,
        value,
        WriteOptions {
            blobs: Some(&mut table),
            types: None,
        },
    )
    .map_err(|error| format!("cannot write authored value: {}", error.render("data")))?;
    Ok(AuthoringValue {
        canonical_value: Arc::from(text.into_bytes()),
        blobs: table.0,
    })
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
    use distill_schema::ngp_schema::PrimitiveKind;

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
    fn blobs_travel_as_placements_into_the_blob_table() {
        let schema = SchemaNode::Struct {
            rev: 0,
            fields: vec![
                ("a".to_owned(), 0, SchemaNode::Blob),
                ("b".to_owned(), 0, SchemaNode::Blob),
            ],
        };
        let value = Value::Struct(Box::new([Value::Blob(vec![1, 2]), Value::Blob(vec![3])]));
        let carried = authoring_value(&schema, &value).unwrap();
        assert_eq!(
            &*carried.canonical_value,
            br#"{"a":{"len":2,"offset":0},"b":{"len":1,"offset":1}}"#
        );
        let mut table = BlobTable::new(&carried.blobs);
        let text = std::str::from_utf8(&carried.canonical_value).unwrap();
        let read = parse_value(text, &schema, Options::canonical().blobs(&mut table)).unwrap();
        table.all_used().unwrap();
        assert_eq!(read, value);
    }
}

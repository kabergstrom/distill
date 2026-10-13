//! The conformance checker (§11): does a [`Value`] conform to a schema
//! AST? Run after EVERY migration edge, function edges included, before
//! tag extraction, validators, or artifact encoding can consume the value.
//!
//! A value carries no names, so conformance is positional: each leaf is
//! the node's primitive kind, a struct holds exactly one value per field
//! (node order), an enum's variant index names a declared variant and its
//! payload holds one value per payload field, a fixed array has its exact
//! length, and sets and maps hold no repeated element or key (by
//! `ngp_schema::key_bytes`; their order is free: writers canonicalize).
//! Floats are finite. Asset references hold a non-empty path or asset
//! UUID.

use ngp_schema::{key_bytes, AssetRefValue, Frames, SchemaNode, Value};
use std::fmt;

/// Names the structural path and the expectation that failed there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConformError {
    /// Structural path: `$` root, `.field`, `[index]`, `{Variant}`.
    pub path: String,
    pub expected: String,
    pub found: String,
}

impl fmt::Display for ConformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "at {}: expected {}, found {}",
            self.path, self.expected, self.found
        )
    }
}

impl std::error::Error for ConformError {}

fn found_of(v: &Value) -> String {
    match v {
        Value::Struct(fields) => format!("struct of {} fields", fields.len()),
        Value::Seq(items) => format!("sequence of {}", items.len()),
        Value::Map(entries) => format!("map of {}", entries.len()),
        Value::Enum { variant, payload } => {
            format!("variant {variant} with {} payload fields", payload.len())
        }
        Value::Blob(b) => format!("blob of {} bytes", b.len()),
        Value::F32(x) => format!("f32 {x}"),
        Value::F64(x) => format!("f64 {x}"),
        Value::String(s) => format!("string {s:?}"),
        other => other.kind_name().to_string(),
    }
}

fn err(path: &str, expected: impl Into<String>, v: &Value) -> ConformError {
    ConformError {
        path: path.to_string(),
        expected: expected.into(),
        found: found_of(v),
    }
}

/// Check `value` against `schema`.
pub fn conforms(value: &Value, schema: &SchemaNode) -> Result<(), ConformError> {
    check(value, schema, &mut Frames::new(), "$")
}

fn check<'s>(
    value: &Value,
    schema: &'s SchemaNode,
    frames: &mut Frames<'s>,
    path: &str,
) -> Result<(), ConformError> {
    let open = frames.len();
    frames
        .enter(schema, |node, frames| check_node(value, node, frames, path))
        .unwrap_or_else(|distance| {
            Err(err(
                path,
                format!("resolvable back-reference (distance {distance}, {open} frames open)"),
                value,
            ))
        })
}

fn check_node<'s>(
    value: &Value,
    node: &'s SchemaNode,
    frames: &mut Frames<'s>,
    path: &str,
) -> Result<(), ConformError> {
    match (node, value) {
        (SchemaNode::Primitive(kind), v) => {
            if !v.is_primitive(*kind) {
                return Err(err(path, kind.canonical_name(), v));
            }
            match v {
                Value::F32(x) if !x.is_finite() => Err(err(path, "finite f32", v)),
                Value::F64(x) if !x.is_finite() => Err(err(path, "finite f64", v)),
                _ => Ok(()),
            }
        }
        (SchemaNode::String, Value::String(_))
        | (SchemaNode::Unit, Value::Unit)
        | (SchemaNode::Blob, Value::Blob(_))
        | (SchemaNode::NodeRef { .. }, Value::NodeRef(_))
        | (SchemaNode::Handle { .. }, Value::Handle(_))
        | (SchemaNode::Dynamic { .. }, Value::Dynamic(_)) => Ok(()),
        (SchemaNode::AssetRef(_), Value::AssetRef(r))
        | (SchemaNode::WeakRef(_), Value::WeakRef(r)) => check_asset_ref(r, value, path),
        (SchemaNode::Option(_), Value::Option(None)) => Ok(()),
        (SchemaNode::Option(inner), Value::Option(Some(v))) => check(v, inner, frames, path),
        (SchemaNode::Vec(elem), Value::Seq(items)) => {
            for (i, item) in items.iter().enumerate() {
                check(item, elem, frames, &format!("{path}[{i}]"))?;
            }
            Ok(())
        }
        (SchemaNode::Array { len, elem }, Value::Seq(items)) => {
            if items.len() as u64 != *len {
                return Err(err(path, format!("array of exactly {len} elements"), value));
            }
            for (i, item) in items.iter().enumerate() {
                check(item, elem, frames, &format!("{path}[{i}]"))?;
            }
            Ok(())
        }
        (SchemaNode::Set(elem), Value::Seq(items)) => {
            let mut seen = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let ipath = format!("{path}[{i}]");
                check(item, elem, frames, &ipath)?;
                seen.push((order_key(elem, item, frames, &ipath)?, i));
            }
            unique(&mut seen, path, "no duplicate set elements", |i| &items[i])
        }
        (SchemaNode::Map { key, value: vnode }, Value::Map(entries)) => {
            let mut seen = Vec::with_capacity(entries.len());
            for (i, (k, v)) in entries.iter().enumerate() {
                let ppath = format!("{path}[{i}]");
                check(k, key, frames, &format!("{ppath}[0]"))?;
                check(v, vnode, frames, &format!("{ppath}[1]"))?;
                seen.push((order_key(key, k, frames, &ppath)?, i));
            }
            unique(&mut seen, path, "no duplicate map keys", |i| &entries[i].0)
        }
        (SchemaNode::Struct { fields, .. }, Value::Struct(values)) => {
            check_fields(values, fields, frames, path)
        }
        (SchemaNode::Enum { variants, .. }, Value::Enum { variant, payload }) => {
            let Some((vname, _, pnode)) = variants.get(*variant as usize) else {
                return Err(err(
                    path,
                    format!("a declared variant (of {})", variants.len()),
                    value,
                ));
            };
            // The variant payload struct is NOT a separate back-ref frame
            // (§5): the enum's own frame covers it.
            let vpath = format!("{path}{{{vname}}}");
            match pnode {
                SchemaNode::Struct { fields, .. } => check_fields(payload, fields, frames, &vpath),
                _ => Err(err(&vpath, "struct payload node (schema malformed)", value)),
            }
        }
        (node, v) => Err(err(path, node_name(node), v)),
    }
}

pub(crate) fn node_name(node: &SchemaNode) -> &'static str {
    match node {
        SchemaNode::Primitive(_) => "primitive",
        SchemaNode::String => "string",
        SchemaNode::Unit => "unit",
        SchemaNode::Blob => "blob",
        SchemaNode::Option(_) => "option",
        SchemaNode::Vec(_) => "sequence (vec)",
        SchemaNode::Array { .. } => "sequence (fixed-size)",
        SchemaNode::Set(_) => "sequence (set)",
        SchemaNode::Map { .. } => "map",
        SchemaNode::Struct { .. } => "struct",
        SchemaNode::Enum { .. } => "enum",
        SchemaNode::AssetRef(_) => "asset reference",
        SchemaNode::WeakRef(_) => "weak reference",
        SchemaNode::BackRef(_) => "back-reference",
        SchemaNode::Dynamic { .. } => "dynamic value",
        SchemaNode::NodeRef { .. } => "node reference",
        SchemaNode::Handle { .. } => "asset handle",
    }
}

/// Struct body totality: EXACTLY the schema's field set (§11).
fn check_fields<'s>(
    values: &[Value],
    fields: &'s [(String, u32, SchemaNode)],
    frames: &mut Frames<'s>,
    path: &str,
) -> Result<(), ConformError> {
    if values.len() != fields.len() {
        return Err(ConformError {
            path: path.to_string(),
            expected: format!("{} fields", fields.len()),
            found: format!("{} fields", values.len()),
        });
    }
    for ((name, _, fnode), v) in fields.iter().zip(values) {
        check(v, fnode, frames, &format!("{path}.{name}"))?;
    }
    Ok(())
}

/// §4: a reference names a non-empty path or asset UUID.
fn check_asset_ref(r: &AssetRefValue, value: &Value, path: &str) -> Result<(), ConformError> {
    const EXPECTED: &str = "asset reference: uuid/path string, {path, asset}, or {asset} (§4)";
    let ok = match r {
        AssetRefValue::Path(p) => !p.is_empty(),
        AssetRefValue::Query { path: p, asset } => {
            asset.as_deref().is_some_and(|a| !a.is_empty())
                && p.as_deref().is_none_or(|p| !p.is_empty())
        }
    };
    if ok {
        Ok(())
    } else {
        Err(err(path, EXPECTED, value))
    }
}

/// Canonical order bytes for set/map-key ordering (§6). Keys and elements
/// are blob-free by §5's serializability rule, so failure here means the
/// schema itself is malformed.
fn order_key<'s>(
    node: &'s SchemaNode,
    v: &Value,
    frames: &mut Frames<'s>,
    path: &str,
) -> Result<Vec<u8>, ConformError> {
    key_bytes(node, v, frames).ok_or_else(|| err(path, "canonically encodable (blob-free)", v))
}

/// No two of `seen` (order bytes, index) share bytes; the error names the
/// later of a repeated pair.
fn unique<'v>(
    seen: &mut [(Vec<u8>, usize)],
    path: &str,
    expected: &str,
    at: impl Fn(usize) -> &'v Value,
) -> Result<(), ConformError> {
    seen.sort();
    match seen.windows(2).find(|w| w[0].0 == w[1].0) {
        Some(w) => Err(err(&format!("{path}[{}]", w[1].1), expected, at(w[1].1))),
        None => Ok(()),
    }
}

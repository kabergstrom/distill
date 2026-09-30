//! Which data a plan throws away.
//!
//! A write never silently drops data: before a value stored under one
//! schema is replaced by a value under another, the automatic plan between
//! the two is checked for `DropField` ops whose field holds something other
//! than its type's default.

use crate::identical::resolve_path;
use crate::{FieldPath, MigrationOp};
use distill_json::AuthoredValue;
use ngp_schema::{PrimitiveKind, SchemaNode};
use std::collections::BTreeMap;

/// The display paths of every field `ops` drops while it holds a value
/// other than its type's default. `schema` and `value` are the input
/// schema and value.
pub fn lossy_drops(ops: &[MigrationOp], schema: &SchemaNode, value: &AuthoredValue) -> Vec<String> {
    let mut drops = Vec::new();
    walk(ops, schema, value, "$", &mut drops);
    drops
}

/// The default of a field type: zero, false, empty, `None`, and the first
/// variant of an enum. `Err` names a type that has none.
pub fn zero_value(node: &SchemaNode) -> Result<AuthoredValue, String> {
    Ok(match node {
        SchemaNode::Primitive(kind) => match kind {
            PrimitiveKind::Bool => AuthoredValue::Bool(false),
            PrimitiveKind::F32 | PrimitiveKind::F64 => AuthoredValue::Float(0.0),
            PrimitiveKind::Char => AuthoredValue::Str("\0".into()),
            _ => AuthoredValue::UInt(0),
        },
        SchemaNode::Struct { fields, .. } => AuthoredValue::Object(
            fields
                .iter()
                .map(|(name, _, field)| {
                    zero_value(field)
                        .map(|value| (name.clone(), value))
                        .map_err(|detail| format!("{name}: {detail}"))
                })
                .collect::<Result<_, _>>()?,
        ),
        SchemaNode::Enum { variants, .. } => {
            let (name, _, payload) = variants.first().ok_or("an enum without variants")?;
            let payload = zero_value(payload).map_err(|detail| format!("{name}: {detail}"))?;
            AuthoredValue::Object(BTreeMap::from([(name.clone(), payload)]))
        }
        SchemaNode::Vec(_) | SchemaNode::Set(_) => AuthoredValue::Array(Vec::new()),
        SchemaNode::Map { key, .. } if matches!(**key, SchemaNode::String) => {
            AuthoredValue::Object(BTreeMap::new())
        }
        SchemaNode::Map { .. } => AuthoredValue::Array(Vec::new()),
        SchemaNode::Array { len, elem } => {
            let elem = zero_value(elem)?;
            AuthoredValue::Array(vec![elem; *len as usize])
        }
        SchemaNode::Option(_) | SchemaNode::Unit => AuthoredValue::Null,
        SchemaNode::String => AuthoredValue::Str(String::new()),
        SchemaNode::Blob => AuthoredValue::Blob(Vec::new()),
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => {
            return Err("a required asset reference".into())
        }
        SchemaNode::BackRef(_) => return Err("a required recursive field".into()),
    })
}

fn walk(
    ops: &[MigrationOp],
    schema: &SchemaNode,
    value: &AuthoredValue,
    display: &str,
    drops: &mut Vec<String>,
) {
    for op in ops {
        match op {
            MigrationOp::DropField { at } => {
                let (Some(node), Some(field)) = (resolve_path(schema, at), lookup(value, at))
                else {
                    continue;
                };
                if zero_value(node).as_ref() != Ok(field) {
                    drops.push(join(display, at));
                }
            }
            MigrationOp::MigrateInline { at, ops } => {
                if let (Some(node), Some(field)) = (resolve_path(schema, at), lookup(value, at)) {
                    walk(ops, node, field, &join(display, at), drops);
                }
            }
            MigrationOp::MigrateElements { at, element } => {
                let (Some(node), Some(field)) = (resolve_path(schema, at), lookup(value, at))
                else {
                    continue;
                };
                let display = format!("{}[]", join(display, at));
                for (elem_schema, elem) in elements(node, field) {
                    walk(element, elem_schema, elem, &display, drops);
                }
            }
            MigrationOp::MapVariant {
                at, from, payload, ..
            } => {
                let (Some(SchemaNode::Enum { variants, .. }), Some(AuthoredValue::Object(held))) =
                    (resolve_path(schema, at), lookup(value, at))
                else {
                    continue;
                };
                let Some(held) = held.get(from) else {
                    continue;
                };
                if let Some((_, _, node)) = variants.iter().find(|(name, _, _)| name == from) {
                    let display = format!("{}{{{from}}}", join(display, at));
                    walk(payload, node, held, &display, drops);
                }
            }
            MigrationOp::CopyField { .. }
            | MigrationOp::Widen { .. }
            | MigrationOp::WriteValue { .. }
            | MigrationOp::WriteFieldDefault { .. }
            | MigrationOp::WriteParentDefault { .. }
            | MigrationOp::WriteNone { .. }
            | MigrationOp::MigrateMapKeys { .. } => {}
        }
    }
}

fn lookup<'a>(value: &'a AuthoredValue, path: &FieldPath) -> Option<&'a AuthoredValue> {
    path.0.iter().try_fold(value, |value, segment| match value {
        AuthoredValue::Object(fields) => fields.get(segment),
        _ => None,
    })
}

/// The element schema and every element value of a container.
fn elements<'a>(
    node: &'a SchemaNode,
    value: &'a AuthoredValue,
) -> Vec<(&'a SchemaNode, &'a AuthoredValue)> {
    match (node, value) {
        (SchemaNode::Option(_), AuthoredValue::Null) => Vec::new(),
        (SchemaNode::Option(inner), value) => vec![(inner, value)],
        (
            SchemaNode::Vec(inner) | SchemaNode::Set(inner) | SchemaNode::Array { elem: inner, .. },
            AuthoredValue::Array(items),
        ) => items.iter().map(|item| (inner.as_ref(), item)).collect(),
        (SchemaNode::Map { value: inner, .. }, AuthoredValue::Object(items)) => {
            items.values().map(|item| (inner.as_ref(), item)).collect()
        }
        (SchemaNode::Map { value: inner, .. }, AuthoredValue::Array(pairs)) => pairs
            .iter()
            .filter_map(|pair| match pair {
                AuthoredValue::Array(pair) if pair.len() == 2 => Some((inner.as_ref(), &pair[1])),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn join(display: &str, path: &FieldPath) -> String {
    let mut joined = display.to_owned();
    for segment in &path.0 {
        joined.push('.');
        joined.push_str(segment);
    }
    joined
}

//! Which data a plan throws away.
//!
//! A write never silently drops data: before a value stored under one
//! schema is replaced by a value under another, the automatic plan between
//! the two is checked for `DropField` ops whose field holds something other
//! than its type's zero value (`ngp_schema::zero_value`).

use crate::execute::lookup;
use crate::identical::resolve_path;
use crate::{FieldPath, MigrationOp};
use ngp_schema::{zero_value, SchemaNode, Value};

/// The display paths of every field `ops` drops while it holds a value
/// other than its type's zero value. `schema` and `value` are the input
/// schema and value.
pub fn lossy_drops(ops: &[MigrationOp], schema: &SchemaNode, value: &Value) -> Vec<String> {
    let mut drops = Vec::new();
    walk(ops, schema, value, "$", &mut drops);
    drops
}

fn walk(
    ops: &[MigrationOp],
    schema: &SchemaNode,
    value: &Value,
    display: &str,
    drops: &mut Vec<String>,
) {
    for op in ops {
        match op {
            MigrationOp::DropField { at } => {
                let (Some(node), Some(field)) = (resolve_path(schema, at), lookup(schema, value, at))
                else {
                    continue;
                };
                if zero_value(node).as_ref() != Some(field) {
                    drops.push(join(display, at));
                }
            }
            MigrationOp::MigrateInline { at, ops } => {
                if let (Some(node), Some(field)) =
                    (resolve_path(schema, at), lookup(schema, value, at))
                {
                    walk(ops, node, field, &join(display, at), drops);
                }
            }
            MigrationOp::MigrateElements { at, element } | MigrationOp::GrowArray { at, element } => {
                let (Some(node), Some(field)) = (resolve_path(schema, at), lookup(schema, value, at))
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
                let (
                    Some(SchemaNode::Enum { variants, .. }),
                    Some(Value::Enum {
                        variant,
                        payload: held,
                    }),
                ) = (resolve_path(schema, at), lookup(schema, value, at))
                else {
                    continue;
                };
                let Some((name, _, node)) = variants.get(*variant as usize) else {
                    continue;
                };
                if name == from {
                    let display = format!("{}{{{from}}}", join(display, at));
                    walk(payload, node, &Value::Struct(held.clone()), &display, drops);
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

/// The element schema and every element value of a container.
fn elements<'a>(node: &'a SchemaNode, value: &'a Value) -> Vec<(&'a SchemaNode, &'a Value)> {
    match (node, value) {
        (SchemaNode::Option(inner), Value::Option(Some(v))) => vec![(inner, v)],
        (
            SchemaNode::Vec(inner) | SchemaNode::Set(inner) | SchemaNode::Array { elem: inner, .. },
            Value::Seq(items),
        ) => items.iter().map(|item| (inner.as_ref(), item)).collect(),
        (SchemaNode::Map { value: inner, .. }, Value::Map(entries)) => {
            entries.iter().map(|(_, v)| (inner.as_ref(), v)).collect()
        }
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

//! TRANSITION (newgameplus GAP 23 §18.6 step 1): the interpreter's
//! descriptor and reflection against the `#[asset]`-generated ones, for the
//! §18.5 comparison tests. Goes with the macro (step 3).

use ngp_schema::node::{node_from_bytes, SchemaNode};
use ngp_schema::Value;

use super::interp::TypeInfo;
use super::AssetReflect;
use crate::defaults::{AssetDefaults, PathStep};
use crate::AssetType;

/// Every difference between `T`'s generated descriptor, logical schema and
/// default table and `info`'s, one line each.
pub fn compare_type<T: AssetType + AssetReflect + AssetDefaults>(info: &TypeInfo) -> Vec<String> {
    let name = std::any::type_name::<T>();
    let mut out = Vec::new();
    let mut diff = |what: &str, a: String, b: String| {
        if a != b {
            out.push(format!("{name}: {what}: macro {a} interp {b}"));
        }
    };
    let m = T::descriptor();
    let i = &info.descriptor;
    diff("type uuid", format!("{:?}", m.type_uuid), format!("{:?}", i.type_uuid));
    diff("logical hash", format!("{:?}", m.logical_hash), format!("{:?}", i.logical_hash));
    diff("build_only", m.build_only.to_string(), i.build_only.to_string());
    diff("size", m.size.to_string(), i.size.to_string());
    diff("align", m.align.to_string(), i.align.to_string());
    diff("native layout", format!("{:#?}", m.native_layout), format!("{:#?}", i.native_layout));
    let ctors = |t: &distill_wire::native::CtorTable| {
        t.entries
            .iter()
            .map(|e| (e.elem_size, e.elem_align, e.key_offset, e.value_offset))
            .collect::<Vec<_>>()
    };
    diff("ctor table", format!("{:?}", ctors(m.ctors)), format!("{:?}", ctors(i.ctors)));
    diff("drop table", m.drops.entries.len().to_string(), i.drops.entries.len().to_string());
    diff(
        "skip writer table",
        m.skip_writers.entries.len().to_string(),
        i.skip_writers.entries.len().to_string(),
    );
    let logical = node_from_bytes(&crate::build::logical_bytes::<T>()).map(|l| format!("{:?}", l.root));
    diff(
        "logical schema",
        logical.unwrap_or_else(|e| format!("undecodable {e:?}")),
        format!("{:?}", info.reflect.logical.root),
    );

    // Defaults, queried as the migration adapter queries the generated
    // table: per expanded container node, per field.
    // Over both sides' containers: the macro also expands transparent
    // wrappers (`Box<T>` repeats `T`'s node), which answer nothing new.
    let mut all: Vec<(Option<String>, SchemaNode)> = containers::<T>().into_iter().map(|(_, v, s)| (v, s)).collect();
    all.extend(info.reflect.default_containers().map(|s| (None, s.clone())));
    for (variant, schema) in &all {
        let SchemaNode::Struct { fields, .. } = schema else { continue };
        for (field, _, _) in fields {
            let at = [field.clone()];
            let interp_field = info.reflect.field_default(schema, &at).map(|r| r.ok());
            diff(
                &format!("field default {variant:?}.{field}"),
                format!("{:?}", first_answer::<T>(schema, field)),
                format!("{interp_field:?}"),
            );
            let interp_parent = info.reflect.parent_default(schema, &at).and_then(|r| r.ok());
            diff(
                &format!("parent default {variant:?}.{field}"),
                format!("{:?}", first_parent::<T>(schema, field)),
                format!("{interp_parent:?}"),
            );
        }
    }
    out
}

/// The generated table's answer for `field` of `schema`, as the adapter
/// gives it: the first expanded container structurally equal to `schema`.
fn first_answer<T: AssetDefaults>(schema: &SchemaNode, field: &str) -> Option<Option<Value>> {
    let table = T::default_table();
    for (node, variant, payload) in containers::<T>() {
        if payload != *schema {
            continue;
        }
        if let Some((_, _, w)) = table.nodes.iter().find(|(n, path, _)| {
            *n == node
                && match (variant.as_deref(), path) {
                    (None, [PathStep::Field(f)]) => *f == field,
                    (Some(v), [PathStep::Variant(pv), PathStep::Field(f)]) => *pv == v && *f == field,
                    _ => false,
                }
        }) {
            return Some(w().ok());
        }
    }
    None
}

fn first_parent<T: AssetDefaults>(schema: &SchemaNode, field: &str) -> Option<Value> {
    let table = T::default_table();
    for ty in table.types {
        let Ok(logical) = node_from_bytes(ty.logical) else { continue };
        if !matches!(logical.root, SchemaNode::Struct { .. }) || logical.root != *schema {
            continue;
        }
        if let Some(w) = ty.writer {
            return w().ok()?.field(schema, field).cloned();
        }
    }
    None
}

fn containers<T: AssetDefaults>() -> Vec<(crate::defaults::SchemaNodeId, Option<String>, SchemaNode)> {
    let mut out = Vec::new();
    for ty in T::default_table().types {
        let Ok(logical) = node_from_bytes(ty.logical) else { continue };
        match logical.root {
            node @ SchemaNode::Struct { .. } => out.push((ty.node, None, node)),
            SchemaNode::Enum { variants, .. } => {
                out.extend(variants.into_iter().map(|(n, _, p)| (ty.node, Some(n), p)));
            }
            _ => {}
        }
    }
    out
}

/// `value` through the generated `to_value` and through the interpreter.
pub fn compare_value<T: AssetReflect>(info: &TypeInfo, value: &T) -> Option<String> {
    let macro_value = value.to_value();
    let interp_value = unsafe { info.reflect.to_value((value as *const T).cast()) };
    (macro_value != interp_value).then(|| {
        format!(
            "{}: to_value differs:\nmacro  {macro_value:?}\ninterp {interp_value:?}",
            std::any::type_name::<T>()
        )
    })
}

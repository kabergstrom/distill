//! Shared test scaffolding: schema builders, value builders, providers.
#![allow(dead_code)]

use distill_core::id::{BundleUuid, LogicalHash, TypeUuid};
use distill_migrate::{DefaultProvider, FieldPath, FnProvider};
pub use ngp_schema::{Patch, Value};
use ngp_schema::{parse_patch, parse_value, write_patch, write_value, Options, PrimitiveKind, SchemaNode, WriteOptions};
use std::collections::BTreeMap;

#[allow(unused_imports)] // not every test binary uses primitives
pub use ngp_schema::PrimitiveKind as PK;

/// A registered migration function.
pub type MigrationFn = fn(Value) -> Result<Value, String>;

pub fn p(k: PrimitiveKind) -> SchemaNode {
    SchemaNode::Primitive(k)
}

pub fn strct(rev: u32, fields: &[(&str, u32, SchemaNode)]) -> SchemaNode {
    SchemaNode::Struct {
        rev,
        fields: fields
            .iter()
            .map(|(n, r, s)| (n.to_string(), *r, s.clone()))
            .collect(),
    }
}

pub fn enm(rev: u32, variants: &[(&str, u32, SchemaNode)]) -> SchemaNode {
    SchemaNode::Enum {
        rev,
        variants: variants
            .iter()
            .map(|(n, r, s)| (n.to_string(), *r, s.clone()))
            .collect(),
    }
}

/// A unit variant's payload: the zero-field struct.
pub fn unit_payload() -> SchemaNode {
    strct(0, &[])
}

pub fn vec_of(elem: SchemaNode) -> SchemaNode {
    SchemaNode::Vec(Box::new(elem))
}

pub fn set_of(elem: SchemaNode) -> SchemaNode {
    SchemaNode::Set(Box::new(elem))
}

pub fn opt(elem: SchemaNode) -> SchemaNode {
    SchemaNode::Option(Box::new(elem))
}

pub fn arr(len: u64, elem: SchemaNode) -> SchemaNode {
    SchemaNode::Array {
        len,
        elem: Box::new(elem),
    }
}

pub fn map_of(key: SchemaNode, value: SchemaNode) -> SchemaNode {
    SchemaNode::Map {
        key: Box::new(key),
        value: Box::new(value),
    }
}

pub fn path(segs: &[&str]) -> FieldPath {
    FieldPath::of(segs)
}

pub fn root() -> FieldPath {
    FieldPath::root()
}

// --- value builders -------------------------------------------------------

/// A value of `node` from its canonical text.
pub fn val(node: &SchemaNode, text: &str) -> Value {
    parse_value(text, node, Options::canonical()).unwrap_or_else(|e| panic!("{text}: {e}"))
}

/// The canonical text of a value of `node`.
pub fn txt(node: &SchemaNode, value: &Value) -> String {
    write_value(node, value, WriteOptions::default()).unwrap_or_else(|e| panic!("{value:?}: {e}"))
}

/// A patch of `node` from authored text.
pub fn patch(node: &SchemaNode, text: &str) -> Patch {
    parse_patch(text, node, Options::authored()).unwrap_or_else(|e| panic!("{text}: {e}"))
}

/// The canonical text of a patch of `node`.
pub fn ptxt(node: &SchemaNode, value: &Patch) -> String {
    write_patch(node, value, WriteOptions::default()).unwrap_or_else(|e| panic!("{value:?}: {e}"))
}

// --- identity builders ----------------------------------------------------

pub fn hash(n: u8) -> LogicalHash {
    LogicalHash([n; 32])
}

pub fn bundle(n: u8) -> BundleUuid {
    BundleUuid([n; 16])
}

pub fn tuuid(n: u8) -> TypeUuid {
    TypeUuid([n; 16])
}

// --- providers -------------------------------------------------------------

/// A provider with no defaults at all: every default op must fail hard.
pub struct NoDefaults;

impl DefaultProvider for NoDefaults {
    fn field_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<Value> {
        None
    }
    fn parent_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<Value> {
        None
    }
}

/// Table-backed defaults, keyed on the frame-relative path.
#[derive(Default)]
pub struct TableDefaults {
    pub field: BTreeMap<FieldPath, Value>,
    pub parent: BTreeMap<FieldPath, Value>,
}

impl DefaultProvider for TableDefaults {
    fn field_default(&self, _: &SchemaNode, at: &FieldPath) -> Option<Value> {
        self.field.get(at).cloned()
    }
    fn parent_default(&self, _: &SchemaNode, at: &FieldPath) -> Option<Value> {
        self.parent.get(at).cloned()
    }
}

/// Function registry; unknown keys are errors (no fallback).
#[derive(Default)]
pub struct Fns {
    pub table: BTreeMap<String, MigrationFn>,
}

impl FnProvider for Fns {
    fn run(&self, key: &str, v: Value) -> Result<Value, String> {
        match self.table.get(key) {
            Some(f) => f(v),
            None => Err(format!("unknown migration fn {key:?}")),
        }
    }
}

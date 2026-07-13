//! Shared test scaffolding: schema builders, value builders, providers.
#![allow(dead_code)]

use distill_core::id::{BundleUuid, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_migrate::{DefaultProvider, FieldPath, FnProvider};
use ngp_schema::{PrimitiveKind, SchemaNode};
use std::collections::BTreeMap;

#[allow(unused_imports)] // not every test binary uses primitives
pub use ngp_schema::PrimitiveKind as PK;

/// A registered migration function.
pub type MigrationFn = fn(AuthoredValue) -> Result<AuthoredValue, String>;

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

pub fn obj(entries: &[(&str, AuthoredValue)]) -> AuthoredValue {
    AuthoredValue::Object(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

pub fn ui(v: u128) -> AuthoredValue {
    AuthoredValue::UInt(v)
}

pub fn int(v: i128) -> AuthoredValue {
    AuthoredValue::Int(v)
}

pub fn fl(v: f64) -> AuthoredValue {
    AuthoredValue::Float(v)
}

pub fn st(v: &str) -> AuthoredValue {
    AuthoredValue::Str(v.to_string())
}

pub fn arr_v(items: &[AuthoredValue]) -> AuthoredValue {
    AuthoredValue::Array(items.to_vec())
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
    fn field_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<AuthoredValue> {
        None
    }
    fn parent_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<AuthoredValue> {
        None
    }
}

/// Table-backed defaults, keyed on the frame-relative path.
#[derive(Default)]
pub struct TableDefaults {
    pub field: BTreeMap<FieldPath, AuthoredValue>,
    pub parent: BTreeMap<FieldPath, AuthoredValue>,
}

impl DefaultProvider for TableDefaults {
    fn field_default(&self, _: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        self.field.get(at).cloned()
    }
    fn parent_default(&self, _: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        self.parent.get(at).cloned()
    }
}

/// Function registry; unknown keys are errors (no fallback).
#[derive(Default)]
pub struct Fns {
    pub table: BTreeMap<String, MigrationFn>,
}

impl FnProvider for Fns {
    fn run(&self, key: &str, v: AuthoredValue) -> Result<AuthoredValue, String> {
        match self.table.get(key) {
            Some(f) => f(v),
            None => Err(format!("unknown migration fn {key:?}")),
        }
    }
}

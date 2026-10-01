//! `PipelineDefaults` backed by an `#[asset]` type's generated
//! `DefaultTable` (§3, §11), so automatic migrations that add a field can
//! fill it from the field type's (or its container's) `Default`.
//!
//! The migration executor hands the provider a frame's to-schema root and a
//! frame-relative path through struct fields. The adapter resolves the
//! path's container node, matches it structurally against the logical node
//! of every Rust type the table expanded (enum variant payloads included),
//! and answers from that type's entries.

use std::sync::Arc;

use distill_asset::{AssetDefaults, DefaultTable, DefaultWriter, PathStep, SchemaNodeId};
use distill_json::AuthoredValue;
use distill_migrate::FieldPath;
use ngp_schema::{node_from_bytes, SchemaNode};

use crate::callbacks::PipelineDefaults;

struct Container {
    schema: SchemaNode,
    node: SchemaNodeId,
    /// Set when `schema` is one variant's payload of an enum node.
    variant: Option<String>,
    /// The container type's own default; struct containers only.
    writer: Option<DefaultWriter>,
}

pub struct AssetTableDefaults<T: AssetDefaults> {
    table: &'static DefaultTable<T>,
    containers: Arc<[Container]>,
}

impl<T: AssetDefaults> AssetTableDefaults<T> {
    pub fn new() -> Self {
        let table = T::default_table();
        let mut containers = Vec::new();
        for ty in table.types {
            // The collector produced these bytes from the same reflection
            // the descriptor hashes; a decode failure only loses lookups.
            let Ok(logical) = node_from_bytes(ty.logical) else {
                continue;
            };
            match logical.root {
                SchemaNode::Struct { .. } => containers.push(Container {
                    schema: logical.root,
                    node: ty.node,
                    variant: None,
                    writer: ty.writer,
                }),
                SchemaNode::Enum { variants, .. } => {
                    for (name, _, payload) in variants {
                        containers.push(Container {
                            schema: payload,
                            node: ty.node,
                            variant: Some(name),
                            writer: None,
                        });
                    }
                }
                _ => {}
            }
        }
        Self {
            table,
            containers: containers.into(),
        }
    }

    fn containers<'a>(
        &'a self,
        to_schema: &'a SchemaNode,
        at: &'a FieldPath,
    ) -> Option<(&'a str, impl Iterator<Item = &'a Container> + 'a)> {
        let (field, parent) = at.0.split_last()?;
        let mut node = to_schema;
        for segment in parent {
            let SchemaNode::Struct { fields, .. } = node else {
                return None;
            };
            node = fields
                .iter()
                .find(|(name, _, _)| name == segment)
                .map(|(_, _, child)| child)?;
        }
        Some((
            field.as_str(),
            self.containers
                .iter()
                .filter(move |container| container.schema == *node),
        ))
    }
}

impl<T: AssetDefaults> Default for AssetTableDefaults<T> {
    fn default() -> Self {
        Self::new()
    }
}

fn call(writer: DefaultWriter) -> AuthoredValue {
    // Re-raise inside the callback thunk so the host records a callback
    // panic instead of a missing default.
    writer().unwrap_or_else(|_| panic!("asset default writer panicked"))
}

impl<T: AssetDefaults> PipelineDefaults for AssetTableDefaults<T> {
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        let (field, mut containers) = self.containers(to_schema, at)?;
        containers.find_map(|container| {
            self.table
                .nodes
                .iter()
                .find(|(node, path, _)| {
                    *node == container.node
                        && match (container.variant.as_deref(), path) {
                            (None, [PathStep::Field(name)]) => *name == field,
                            (Some(variant), [PathStep::Variant(v), PathStep::Field(name)]) => {
                                *v == variant && *name == field
                            }
                            _ => false,
                        }
                })
                .map(|(_, _, writer)| call(*writer))
        })
    }

    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        let (field, mut containers) = self.containers(to_schema, at)?;
        containers.find_map(|container| {
            let AuthoredValue::Object(mut fields) = call(container.writer?) else {
                return None;
            };
            fields.remove(field)
        })
    }
}

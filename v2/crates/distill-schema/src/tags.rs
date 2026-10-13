//! Current-schema `#[asset(tag)]` extraction.
//!
//! Tag annotations deliberately do not participate in logical schema hashes,
//! so extraction walks the pinned shared source schema while reading a value
//! already produced by the common `load_current` path.

use std::collections::BTreeMap;

use ngp_schema::Value;
use ngp_schema::classify::{classify, Class};
use ngp_schema::{Field, FieldIdentifier, Schema, SchemaTypeId};
use unicode_normalization::UnicodeNormalization;

const MAX_TAG_WALK_DEPTH: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagExtractionError {
    MissingType(SchemaTypeId),
    WrongShape {
        path: String,
        expected: &'static str,
    },
    UnsupportedType {
        path: String,
        detail: String,
    },
    InvalidIdentifier {
        path: String,
        value: String,
    },
    DuplicateName {
        name: String,
        first: String,
        second: String,
    },
    DepthExceeded,
}

impl std::fmt::Display for TagExtractionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "tag extraction: {self:?}")
    }
}

impl std::error::Error for TagExtractionError {}

/// Extract the complete normalized search-tag map for one current-schema
/// value. Duplicate annotations with the same name and value coalesce;
/// conflicting values poison indexing because the public query grammar has a
/// single value per tag name.
pub fn extract_search_tags(
    schema: &Schema,
    root: SchemaTypeId,
    value: &Value,
) -> Result<BTreeMap<String, String>, TagExtractionError> {
    let mut tags = BTreeMap::new();
    walk(schema, root, value, "$", 0, &mut tags)?;
    Ok(tags)
}

fn walk(
    schema: &Schema,
    id: SchemaTypeId,
    value: &Value,
    path: &str,
    depth: usize,
    tags: &mut BTreeMap<String, String>,
) -> Result<(), TagExtractionError> {
    if depth > MAX_TAG_WALK_DEPTH {
        return Err(TagExtractionError::DepthExceeded);
    }
    let ty = schema
        .types
        .get(id.0)
        .ok_or(TagExtractionError::MissingType(id))?;
    match classify(schema, id) {
        Class::Primitive(_) | Class::Unit | Class::StringNode => Ok(()),
        Class::Boxed(child) | Class::ArcOf(child) => {
            walk(schema, child, value, path, depth + 1, tags)
        }
        Class::Option(child) => {
            match value {
                Value::Option(None) => Ok(()),
                Value::Option(Some(value)) => walk(schema, child, value, path, depth + 1, tags),
                _ => wrong_shape(path, "option"),
            }
        }
        Class::Vec(child) | Class::Array { elem: child, .. } | Class::Set { elem: child, .. } => {
            let Value::Seq(values) = value else {
                return wrong_shape(path, "sequence");
            };
            for (index, value) in values.iter().enumerate() {
                walk(
                    schema,
                    child,
                    value,
                    &format!("{path}[{index}]"),
                    depth + 1,
                    tags,
                )?;
            }
            Ok(())
        }
        Class::Map {
            key, value: child, ..
        } => {
            let Value::Map(entries) = value else {
                return wrong_shape(path, "map");
            };
            for (index, (k, v)) in entries.iter().enumerate() {
                walk(schema, key, k, &format!("{path}[{index}].key"), depth + 1, tags)?;
                walk(schema, child, v, &format!("{path}[{index}].value"), depth + 1, tags)?;
            }
            Ok(())
        }
        Class::Struct | Class::Tuple => {
            let Value::Struct(values) = value else {
                return wrong_shape(path, "struct");
            };
            walk_fields(schema, &ty.fields, values, path, depth, tags)
        }
        Class::Enum => {
            let Value::Enum { variant, payload } = value else {
                return wrong_shape(path, "enum");
            };
            // Variants in logical (NFC name) order, as the value indexes them.
            let mut variants: Vec<(String, &Field)> = ty
                .fields
                .iter()
                .filter_map(|field| match &field.id {
                    FieldIdentifier::Variant(name) => Some((name.nfc().collect(), field)),
                    _ => None,
                })
                .collect();
            variants.sort_by(|a, b| a.0.cmp(&b.0));
            let (variant_name, variant) = variants
                .get(*variant as usize)
                .map(|(name, field)| (name.clone(), *field))
                .ok_or_else(|| TagExtractionError::WrongShape {
                    path: path.to_owned(),
                    expected: "declared enum variant",
                })?;
            let variant_ty = schema
                .types
                .get(variant.type_id.0)
                .ok_or(TagExtractionError::MissingType(variant.type_id))?;
            walk_fields(
                schema,
                &variant_ty.fields,
                payload,
                &format!("{path}{{{variant_name}}}"),
                depth,
                tags,
            )
        }
        Class::AssetRef(_) | Class::WeakRef(_) => Ok(()),
        Class::EnumVariant
        | Class::FixedState
        | Class::SlotMap { .. }
        | Class::Opaque(_)
        | Class::Malformed(_) => Err(TagExtractionError::UnsupportedType {
            path: path.to_owned(),
            detail: format!("{:?}", classify(schema, id)),
        }),
    }
}

fn walk_fields(
    schema: &Schema,
    fields: &[Field],
    values: &[Value],
    path: &str,
    depth: usize,
    tags: &mut BTreeMap<String, String>,
) -> Result<(), TagExtractionError> {
    // A struct value is positional over the logical node's fields: the
    // non-skip fields in NFC name byte order (tuples by decimal index name).
    let mut named = Vec::with_capacity(fields.len());
    for field in fields.iter().filter(|field| !field.attrs.skip) {
        let Some(name) = field_name(&field.id) else {
            return Err(TagExtractionError::UnsupportedType {
                path: path.to_owned(),
                detail: "variant identifier in record payload".to_owned(),
            });
        };
        named.push((name.nfc().collect::<String>(), field));
    }
    named.sort_by(|a, b| a.0.cmp(&b.0));
    if named.len() != values.len() {
        return wrong_shape(path, "one value per field");
    }
    for ((name, field), value) in named.into_iter().zip(values) {
        let field_path = format!("{path}.{name}");
        if field.attrs.tag {
            let Value::String(value) = value else {
                return wrong_shape(&field_path, "string tag value");
            };
            let name = canonical_identifier(&field_path, &name)?;
            let value = canonical_identifier(&field_path, value)?;
            if let Some(first) = tags.insert(name.clone(), value.clone()) {
                if first != value {
                    return Err(TagExtractionError::DuplicateName {
                        name,
                        first,
                        second: value,
                    });
                }
            }
        } else {
            walk(schema, field.type_id, value, &field_path, depth + 1, tags)?;
        }
    }
    Ok(())
}

fn field_name(id: &FieldIdentifier) -> Option<String> {
    match id {
        FieldIdentifier::Name(name) => Some(name.clone()),
        FieldIdentifier::Number(index) => Some(index.to_string()),
        FieldIdentifier::Variant(_) => None,
    }
}

fn canonical_identifier(path: &str, value: &str) -> Result<String, TagExtractionError> {
    let normalized = value.nfc().collect::<String>();
    if normalized.is_empty() || normalized.len() > 255 || normalized.contains('\0') {
        return Err(TagExtractionError::InvalidIdentifier {
            path: path.to_owned(),
            value: value.to_owned(),
        });
    }
    Ok(normalized)
}

fn wrong_shape<T>(path: &str, expected: &'static str) -> Result<T, TagExtractionError> {
    Err(TagExtractionError::WrongShape {
        path: path.to_owned(),
        expected,
    })
}

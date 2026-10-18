//! The authoring payloads as bytes (newgameplus GAP 23 §17.6): values and
//! patches in the value encoding (`ngp_value::encoding`), schemas in the
//! schema encoding (`ngp_value::node`). Both transports go through here:
//! a schema is read only against the hash it is named by, a value only
//! against its schema and the types it carries.

use std::sync::Arc;

use distill_core::id::LogicalHash;
use ngp_value::{decode_patch, decode_value, encode_patch, encode_value, node_bytes, node_from_bytes, node_hash, Cx};

use crate::{AuthoringPatch, AuthoringType, AuthoringValue, AuthoringValueError, LogicalSchema, Patch, TypeTable, Value};

/// The schema encoding of `schema`.
pub fn schema_bytes(schema: &LogicalSchema) -> Vec<u8> {
    // The schemas the authoring calls carry were read or hashed before;
    // one that cannot encode has no hash either.
    node_bytes(&schema.root).expect("an authoring schema encodes")
}

/// `bytes` as a schema, refused unless it hashes to `hash`.
pub fn read_schema(hash: LogicalHash, bytes: &[u8]) -> Result<Arc<LogicalSchema>, AuthoringValueError> {
    let schema = node_from_bytes(bytes).map_err(|error| AuthoringValueError::LogicalSchemaInvalid(format!("{error:?}")))?;
    let actual = node_hash(&schema.root).map_err(|error| AuthoringValueError::LogicalSchemaInvalid(format!("{error:?}")))?;
    if actual != hash {
        return Err(AuthoringValueError::LogicalSchemaInvalid(format!("schema hashes to {actual}, named {hash}")));
    }
    Ok(Arc::new(schema))
}

/// `types` as the value encoding resolves them: each uuid once.
pub fn type_table(types: &[AuthoringType]) -> Result<TypeTable, AuthoringValueError> {
    TypeTable::new(
        types
            .iter()
            .map(|ty| ngp_value::DynamicType {
                uuid: ty.type_uuid,
                path: ty.path.clone(),
                schema: ty.schema.clone(),
                renames: Arc::default(),
            })
            .collect(),
    )
    .map_err(AuthoringValueError::TypeInvalid)
}

/// The value encoding of `value` under `schema`.
pub fn value_bytes(schema: &LogicalSchema, value: &AuthoringValue) -> Result<Vec<u8>, AuthoringValueError> {
    let types = type_table(&value.types)?;
    encode_value(Cx::new(schema, &types), &value.value)
        .map_err(|error| AuthoringValueError::SchemaValueShape { detail: error.to_string() })
}

/// `bytes`, the value encoding of a value of `schema` naming `types`.
pub fn read_value(
    schema: &LogicalSchema,
    types: Vec<AuthoringType>,
    bytes: &[u8],
) -> Result<AuthoringValue, AuthoringValueError> {
    let table = type_table(&types)?;
    let value: Value = decode_value(Cx::new(schema, &table), bytes)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    Ok(AuthoringValue { value: Arc::new(value), types })
}

/// The value encoding of `patch` under `schema`.
pub fn patch_bytes(schema: &LogicalSchema, patch: &AuthoringPatch) -> Result<Vec<u8>, AuthoringValueError> {
    let types = type_table(&patch.types)?;
    encode_patch(Cx::new(schema, &types), &patch.patch)
        .map_err(|error| AuthoringValueError::SchemaValueShape { detail: error.to_string() })
}

/// `bytes`, the value encoding of a patch of `schema` naming `types`.
pub fn read_patch(
    schema: &LogicalSchema,
    types: Vec<AuthoringType>,
    bytes: &[u8],
) -> Result<AuthoringPatch, AuthoringValueError> {
    let table = type_table(&types)?;
    let patch: Patch = decode_patch(Cx::new(schema, &table), bytes)
        .map_err(|error| AuthoringValueError::ValueInvalid(error.to_string()))?;
    Ok(AuthoringPatch { patch: Arc::new(patch), types })
}

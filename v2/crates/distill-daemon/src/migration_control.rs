//! Closed decoder for format-owned `MigrationV1` authoring controls.

use std::collections::BTreeMap;

use distill_build::trace::{MigrationControlKind, MigrationControlValue};
use distill_bundle::{AssetEntry, Bundle, LineageStamp};
use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
use distill_core::lineage::AcceptedSchemaEpoch;
use distill_json::AuthoredValue;
use distill_migrate::{FieldPath, MigrationOp};

#[derive(Clone, Copy, Debug)]
pub(crate) struct MigrationHeader {
    pub target_type_uuid: TypeUuid,
    pub from_hash: LogicalHash,
    pub to_hash: LogicalHash,
}

#[derive(Clone, Debug)]
pub(crate) enum MigrationDecodeError {
    Malformed(String),
    SchemaClosure(String),
}

impl std::fmt::Display for MigrationDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(detail) => write!(formatter, "malformed Migration control: {detail}"),
            Self::SchemaClosure(detail) => {
                write!(formatter, "Migration schema closure is invalid: {detail}")
            }
        }
    }
}

pub(crate) fn decode_header(
    value: &AuthoredValue,
) -> Result<MigrationHeader, MigrationDecodeError> {
    let fields = exact_object(
        value,
        &[
            "from_hash",
            "from_lineage",
            "kind",
            "target_type_uuid",
            "to_hash",
            "to_lineage",
        ],
    )?;
    Ok(MigrationHeader {
        target_type_uuid: TypeUuid(fixed_bytes(
            &fields["target_type_uuid"],
            "target_type_uuid",
        )?),
        from_hash: LogicalHash(fixed_bytes(&fields["from_hash"], "from_hash")?),
        to_hash: LogicalHash(fixed_bytes(&fields["to_hash"], "to_hash")?),
    })
}

pub(crate) fn decode(
    asset: AssetUuid,
    bundle: &Bundle,
    entry: &AssetEntry,
    header: MigrationHeader,
) -> Result<MigrationControlValue, MigrationDecodeError> {
    let fields = exact_object(
        &entry.data,
        &[
            "from_hash",
            "from_lineage",
            "kind",
            "target_type_uuid",
            "to_hash",
            "to_lineage",
        ],
    )?;
    let from_schema = bundle
        .schemas
        .get(&header.from_hash)
        .cloned()
        .ok_or_else(|| {
            MigrationDecodeError::SchemaClosure("from_hash snapshot is absent".to_owned())
        })?;
    let to_schema = bundle
        .schemas
        .get(&header.to_hash)
        .cloned()
        .ok_or_else(|| {
            MigrationDecodeError::SchemaClosure("to_hash snapshot is absent".to_owned())
        })?;
    Ok(MigrationControlValue {
        asset,
        target_type_uuid: header.target_type_uuid,
        from_hash: header.from_hash,
        to_hash: header.to_hash,
        from_schema,
        to_schema,
        from_lineage: decode_lineage(&fields["from_lineage"], "from_lineage")?,
        to_lineage: decode_lineage(&fields["to_lineage"], "to_lineage")?,
        kind: decode_kind(&fields["kind"])?,
    })
}

fn decode_lineage(
    value: &AuthoredValue,
    subject: &str,
) -> Result<LineageStamp, MigrationDecodeError> {
    let fields = exact_object(value, &["chain", "cursor", "epochs"])?;
    let AuthoredValue::Array(rows) = &fields["epochs"] else {
        return malformed(format!("{subject}.epochs must be an array"));
    };
    let mut epochs = Vec::with_capacity(rows.len());
    for row in rows {
        let epoch = exact_object(row, &["digest", "forward_parent"])?;
        let forward_parent = match &epoch["forward_parent"] {
            AuthoredValue::Null => None,
            AuthoredValue::UInt(value) => Some(
                u32::try_from(*value)
                    .map_err(|_| malformed_error(format!("{subject} parent exceeds u32")))?,
            ),
            _ => return malformed(format!("{subject} parent must be null or u32")),
        };
        epochs.push(AcceptedSchemaEpoch {
            digest: LogicalHash(fixed_bytes(&epoch["digest"], "lineage digest")?),
            forward_parent,
        });
    }
    let AuthoredValue::UInt(cursor) = &fields["cursor"] else {
        return malformed(format!("{subject}.cursor must be unsigned"));
    };
    Ok(LineageStamp {
        epochs,
        cursor: u32::try_from(*cursor)
            .map_err(|_| malformed_error(format!("{subject}.cursor exceeds u32")))?,
        chain: fixed_bytes(&fields["chain"], "lineage chain")?,
    })
}

fn decode_kind(value: &AuthoredValue) -> Result<MigrationControlKind, MigrationDecodeError> {
    let (variant, payload) = exact_variant(value)?;
    match variant {
        "Ops" => {
            let fields = exact_object(payload, &["ops"])?;
            let AuthoredValue::Array(ops) = &fields["ops"] else {
                return malformed("Migration Ops.ops must be an array");
            };
            Ok(MigrationControlKind::Ops(decode_ops(ops)?))
        }
        "Function" => {
            let fields = exact_object(payload, &["key"])?;
            let AuthoredValue::Str(key) = &fields["key"] else {
                return malformed("Migration Function.key must be a string");
            };
            Ok(MigrationControlKind::Function { key: key.clone() })
        }
        _ => malformed("unknown Migration kind"),
    }
}

fn decode_ops(values: &[AuthoredValue]) -> Result<Vec<MigrationOp>, MigrationDecodeError> {
    values.iter().map(decode_op).collect()
}

fn decode_op(value: &AuthoredValue) -> Result<MigrationOp, MigrationDecodeError> {
    let (variant, payload) = exact_variant(value)?;
    let op = match variant {
        "CopyField" => {
            let fields = exact_object(payload, &["from", "to"])?;
            MigrationOp::CopyField {
                from: decode_path(&fields["from"])?,
                to: decode_path(&fields["to"])?,
            }
        }
        "Widen" => {
            let fields = exact_object(payload, &["from", "to"])?;
            MigrationOp::Widen {
                from: decode_path(&fields["from"])?,
                to: decode_path(&fields["to"])?,
            }
        }
        "WriteValue" => {
            let fields = exact_object(payload, &["to", "value"])?;
            MigrationOp::WriteValue {
                to: decode_path(&fields["to"])?,
                value: decode_authored_value(&fields["value"])?,
            }
        }
        "WriteFieldDefault" => {
            let fields = exact_object(payload, &["to"])?;
            MigrationOp::WriteFieldDefault {
                to: decode_path(&fields["to"])?,
            }
        }
        "WriteParentDefault" => {
            let fields = exact_object(payload, &["to"])?;
            MigrationOp::WriteParentDefault {
                to: decode_path(&fields["to"])?,
            }
        }
        "WriteNone" => {
            let fields = exact_object(payload, &["to"])?;
            MigrationOp::WriteNone {
                to: decode_path(&fields["to"])?,
            }
        }
        "DropField" => {
            let fields = exact_object(payload, &["at"])?;
            MigrationOp::DropField {
                at: decode_path(&fields["at"])?,
            }
        }
        "MapVariant" => {
            let fields = exact_object(payload, &["at", "from", "payload", "to"])?;
            let AuthoredValue::Str(from) = &fields["from"] else {
                return malformed("MapVariant.from must be a string");
            };
            let AuthoredValue::Str(to) = &fields["to"] else {
                return malformed("MapVariant.to must be a string");
            };
            let AuthoredValue::Array(ops) = &fields["payload"] else {
                return malformed("MapVariant.payload must be an array");
            };
            MigrationOp::MapVariant {
                at: decode_path(&fields["at"])?,
                from: from.clone(),
                to: to.clone(),
                payload: decode_ops(ops)?,
            }
        }
        "MigrateElements" => {
            let fields = exact_object(payload, &["at", "element"])?;
            let AuthoredValue::Array(ops) = &fields["element"] else {
                return malformed("MigrateElements.element must be an array");
            };
            MigrationOp::MigrateElements {
                at: decode_path(&fields["at"])?,
                element: decode_ops(ops)?,
            }
        }
        "MigrateMapKeys" => {
            let fields = exact_object(payload, &["at", "key"])?;
            let AuthoredValue::Array(ops) = &fields["key"] else {
                return malformed("MigrateMapKeys.key must be an array");
            };
            MigrationOp::MigrateMapKeys {
                at: decode_path(&fields["at"])?,
                key: decode_ops(ops)?,
            }
        }
        "MigrateInline" => {
            let fields = exact_object(payload, &["at", "ops"])?;
            let AuthoredValue::Array(ops) = &fields["ops"] else {
                return malformed("MigrateInline.ops must be an array");
            };
            MigrationOp::MigrateInline {
                at: decode_path(&fields["at"])?,
                ops: decode_ops(ops)?,
            }
        }
        _ => return malformed("unknown Migration op"),
    };
    Ok(op)
}

fn decode_path(value: &AuthoredValue) -> Result<FieldPath, MigrationDecodeError> {
    let fields = exact_object(value, &["segments"])?;
    let AuthoredValue::Array(values) = &fields["segments"] else {
        return malformed("FieldPath.segments must be an array");
    };
    let segments = values
        .iter()
        .map(|value| match value {
            AuthoredValue::Str(value) => Ok(value.clone()),
            _ => malformed("FieldPath segment must be a string"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FieldPath(segments))
}

fn decode_authored_value(value: &AuthoredValue) -> Result<AuthoredValue, MigrationDecodeError> {
    let (variant, payload) = exact_variant(value)?;
    let decoded = match variant {
        "Null" => {
            exact_object(payload, &[])?;
            AuthoredValue::Null
        }
        "Bool" => match scalar_value(payload)? {
            AuthoredValue::Bool(value) => AuthoredValue::Bool(*value),
            _ => return malformed("AuthoredValue Bool.value must be bool"),
        },
        "Int" => match scalar_value(payload)? {
            AuthoredValue::Int(value) => AuthoredValue::Int(*value),
            _ => return malformed("AuthoredValue Int.value must be signed"),
        },
        "UInt" => match scalar_value(payload)? {
            AuthoredValue::UInt(value) => AuthoredValue::UInt(*value),
            _ => return malformed("AuthoredValue UInt.value must be unsigned"),
        },
        "Float" => match scalar_value(payload)? {
            AuthoredValue::Float(value) => AuthoredValue::Float(*value),
            _ => return malformed("AuthoredValue Float.value must be float"),
        },
        "Str" => match scalar_value(payload)? {
            AuthoredValue::Str(value) => AuthoredValue::Str(value.clone()),
            _ => return malformed("AuthoredValue Str.value must be string"),
        },
        "Blob" => match scalar_value(payload)? {
            AuthoredValue::Blob(value) => AuthoredValue::Blob(value.clone()),
            _ => return malformed("AuthoredValue Blob.value must be blob"),
        },
        "Array" => match scalar_value(payload)? {
            AuthoredValue::Array(values) => AuthoredValue::Array(
                values
                    .iter()
                    .map(decode_authored_value)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            _ => return malformed("AuthoredValue Array.value must be an array"),
        },
        "Object" => match scalar_value(payload)? {
            AuthoredValue::Object(values) => AuthoredValue::Object(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), decode_authored_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>, MigrationDecodeError>>()?,
            ),
            _ => return malformed("AuthoredValue Object.value must be an object"),
        },
        _ => return malformed("unknown AuthoredValue variant"),
    };
    Ok(decoded)
}

fn scalar_value(value: &AuthoredValue) -> Result<&AuthoredValue, MigrationDecodeError> {
    Ok(&exact_object(value, &["value"])?["value"])
}

fn exact_object<'a>(
    value: &'a AuthoredValue,
    keys: &[&str],
) -> Result<&'a BTreeMap<String, AuthoredValue>, MigrationDecodeError> {
    let AuthoredValue::Object(fields) = value else {
        return malformed("expected an object");
    };
    if fields.len() != keys.len() || keys.iter().any(|key| !fields.contains_key(*key)) {
        return malformed("object field set is not exact");
    }
    Ok(fields)
}

fn exact_variant(value: &AuthoredValue) -> Result<(&str, &AuthoredValue), MigrationDecodeError> {
    let AuthoredValue::Object(variants) = value else {
        return malformed("enum value must be an object");
    };
    if variants.len() != 1 {
        return malformed("enum value must select exactly one variant");
    }
    let (variant, payload) = variants.first_key_value().expect("one variant");
    Ok((variant, payload))
}

fn fixed_bytes<const N: usize>(
    value: &AuthoredValue,
    subject: &str,
) -> Result<[u8; N], MigrationDecodeError> {
    let AuthoredValue::Array(values) = value else {
        return malformed(format!("{subject} must be a byte array"));
    };
    if values.len() != N {
        return malformed(format!("{subject} must contain {N} bytes"));
    }
    let mut bytes = [0; N];
    for (output, value) in bytes.iter_mut().zip(values) {
        let AuthoredValue::UInt(value) = value else {
            return malformed(format!("{subject} byte must be unsigned"));
        };
        *output = u8::try_from(*value)
            .map_err(|_| malformed_error(format!("{subject} byte exceeds u8")))?;
    }
    Ok(bytes)
}

fn malformed<T>(detail: impl Into<String>) -> Result<T, MigrationDecodeError> {
    Err(malformed_error(detail))
}

fn malformed_error(detail: impl Into<String>) -> MigrationDecodeError {
    MigrationDecodeError::Malformed(detail.into())
}

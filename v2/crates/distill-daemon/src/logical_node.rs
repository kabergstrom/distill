//! Decoder for canonical logical-node bytes emitted by the shared
//! `ngp-schema` model. Bootstrap controls store this form directly; the daemon
//! reconstructs the shared AST without defining a second schema model.

use distill_schema::ngp_schema::{node_bytes, LogicalSchema, PrimitiveKind, SchemaNode, TypeUuid};
use unicode_normalization::is_nfc;

const MAX_DEPTH: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalNodeDecodeError {
    Truncated,
    TrailingBytes,
    UnknownTag(u8),
    UnknownPrimitive(String),
    InvalidUtf8,
    NonCanonicalText,
    EntriesNotStrictlySorted,
    CountOverflow,
    DepthLimit,
    NonCanonicalEncoding,
}

impl std::fmt::Display for LogicalNodeDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "logical-node decode: {self:?}")
    }
}

impl std::error::Error for LogicalNodeDecodeError {}

pub fn decode_logical_schema_bytes(bytes: &[u8]) -> Result<LogicalSchema, LogicalNodeDecodeError> {
    let mut reader = Reader { bytes, position: 0 };
    let root = decode_node(&mut reader, 0)?;
    if reader.position != bytes.len() {
        return Err(LogicalNodeDecodeError::TrailingBytes);
    }
    let canonical = node_bytes(&root).map_err(|_| LogicalNodeDecodeError::NonCanonicalEncoding)?;
    if canonical != bytes {
        return Err(LogicalNodeDecodeError::NonCanonicalEncoding);
    }
    Ok(LogicalSchema { root })
}

fn decode_node(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<SchemaNode, LogicalNodeDecodeError> {
    if depth >= MAX_DEPTH {
        return Err(LogicalNodeDecodeError::DepthLimit);
    }
    Ok(match reader.u8()? {
        0x01 => {
            let name = reader.string()?;
            SchemaNode::Primitive(
                PrimitiveKind::from_name(&name)
                    .ok_or(LogicalNodeDecodeError::UnknownPrimitive(name))?,
            )
        }
        0x02 => SchemaNode::Struct {
            rev: reader.u32()?,
            fields: decode_entries(reader, depth + 1)?,
        },
        0x03 => SchemaNode::Enum {
            rev: reader.u32()?,
            variants: decode_entries(reader, depth + 1)?,
        },
        0x04 => SchemaNode::Vec(Box::new(decode_node(reader, depth + 1)?)),
        0x05 => SchemaNode::Array {
            len: reader.u64()?,
            elem: Box::new(decode_node(reader, depth + 1)?),
        },
        0x06 => SchemaNode::Option(Box::new(decode_node(reader, depth + 1)?)),
        0x07 => SchemaNode::Map {
            key: Box::new(decode_node(reader, depth + 1)?),
            value: Box::new(decode_node(reader, depth + 1)?),
        },
        0x08 => SchemaNode::String,
        0x09 => SchemaNode::AssetRef(TypeUuid(reader.array::<16>()?)),
        0x0a => SchemaNode::WeakRef(TypeUuid(reader.array::<16>()?)),
        0x0b => SchemaNode::Blob,
        0x0c => SchemaNode::BackRef(reader.u32()?),
        0x0d => SchemaNode::Unit,
        0x0e => SchemaNode::Set(Box::new(decode_node(reader, depth + 1)?)),
        tag => return Err(LogicalNodeDecodeError::UnknownTag(tag)),
    })
}

fn decode_entries(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<Vec<(String, u32, SchemaNode)>, LogicalNodeDecodeError> {
    let count =
        usize::try_from(reader.u32()?).map_err(|_| LogicalNodeDecodeError::CountOverflow)?;
    if count > reader.bytes.len().saturating_sub(reader.position) {
        return Err(LogicalNodeDecodeError::CountOverflow);
    }
    let mut entries = Vec::with_capacity(count);
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let name = reader.string()?;
        if previous.as_ref().is_some_and(|prior| prior >= &name) {
            return Err(LogicalNodeDecodeError::EntriesNotStrictlySorted);
        }
        previous = Some(name.clone());
        entries.push((name, reader.u32()?, decode_node(reader, depth)?));
    }
    Ok(entries)
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl Reader<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], LogicalNodeDecodeError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(LogicalNodeDecodeError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(LogicalNodeDecodeError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], LogicalNodeDecodeError> {
        self.take(N)?
            .try_into()
            .map_err(|_| LogicalNodeDecodeError::Truncated)
    }

    fn u8(&mut self) -> Result<u8, LogicalNodeDecodeError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, LogicalNodeDecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, LogicalNodeDecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn string(&mut self) -> Result<String, LogicalNodeDecodeError> {
        let len =
            usize::try_from(self.u32()?).map_err(|_| LogicalNodeDecodeError::CountOverflow)?;
        let value = std::str::from_utf8(self.take(len)?)
            .map_err(|_| LogicalNodeDecodeError::InvalidUtf8)?;
        if !is_nfc(value) {
            return Err(LogicalNodeDecodeError::NonCanonicalText);
        }
        Ok(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use distill_core::attestation::BootstrapControlSpecV1;

    use super::*;

    #[test]
    fn embedded_bootstrap_graphs_decode_to_the_shared_ast_and_roundtrip() {
        let spec = BootstrapControlSpecV1::embedded().unwrap();
        for row in &spec.0 {
            let decoded = decode_logical_schema_bytes(&row.logical_schema).unwrap();
            assert_eq!(node_bytes(&decoded.root).unwrap(), row.logical_schema);
        }
    }

    #[test]
    fn rejects_trailing_unknown_and_noncanonical_entry_order() {
        assert_eq!(
            decode_logical_schema_bytes(&[0xff]),
            Err(LogicalNodeDecodeError::UnknownTag(0xff))
        );
        assert_eq!(
            decode_logical_schema_bytes(&[0x08, 0]),
            Err(LogicalNodeDecodeError::TrailingBytes)
        );
        let bytes = [
            0x02, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, b'b', 0, 0, 0, 0, 0x08, 1, 0, 0, 0, b'a', 0,
            0, 0, 0, 0x08,
        ];
        assert_eq!(
            decode_logical_schema_bytes(&bytes),
            Err(LogicalNodeDecodeError::EntriesNotStrictlySorted)
        );
    }
}

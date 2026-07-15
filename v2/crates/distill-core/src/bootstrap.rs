//! The sealed logical bootstrap for bundle format v1.
//!
//! These five rows are the only bootstrap authority. They deliberately carry
//! no target-native layout, compiled registry projection, or consumer brand.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::id::{LogicalHash, TypeUuid};

mod checked_in {
    include!("bootstrap_spec_v1.rs");
}

pub use checked_in::BOOTSTRAP_CONTROL_SPEC_V1_BYTES;

const VERSION: u8 = 1;
pub const BOOTSTRAP_CONTROL_COUNT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BootstrapControlSymbol {
    SchemaLineageManifest = 1,
    Migration = 2,
    ImportRecord = 3,
    DirectoryImportRules = 4,
    PackDefinition = 5,
}

impl BootstrapControlSymbol {
    fn decode(value: u8) -> Result<Self, BootstrapSpecError> {
        match value {
            1 => Ok(Self::SchemaLineageManifest),
            2 => Ok(Self::Migration),
            3 => Ok(Self::ImportRecord),
            4 => Ok(Self::DirectoryImportRules),
            5 => Ok(Self::PackDefinition),
            value => Err(BootstrapSpecError::UnknownSymbol(value)),
        }
    }
}

/// UUIDv5(namespace URL, `https://distill.dev/bundle/v1/control/<symbol>`).
pub const SCHEMA_LINEAGE_MANIFEST_TYPE_UUID: TypeUuid = TypeUuid([
    0x46, 0x80, 0x0e, 0xc6, 0x07, 0x26, 0x5d, 0x36, 0x9a, 0x6c, 0xc5, 0xc6, 0xd3, 0xc2, 0x53, 0x37,
]);
pub const MIGRATION_TYPE_UUID: TypeUuid = TypeUuid([
    0x7c, 0x9c, 0xfd, 0xbf, 0xd0, 0xca, 0x59, 0x33, 0xb8, 0xde, 0x39, 0xa8, 0x58, 0xeb, 0x06, 0xb3,
]);
pub const IMPORT_RECORD_TYPE_UUID: TypeUuid = TypeUuid([
    0x57, 0x05, 0x38, 0x2c, 0xee, 0x65, 0x5a, 0x35, 0xa8, 0xea, 0xd5, 0x30, 0xf9, 0x8a, 0x77, 0x12,
]);
pub const DIRECTORY_IMPORT_RULES_TYPE_UUID: TypeUuid = TypeUuid([
    0xf8, 0x2a, 0xec, 0xaf, 0x0b, 0xdf, 0x52, 0x6f, 0xaf, 0x12, 0x5c, 0xb6, 0x95, 0x81, 0xc8, 0x3b,
]);
pub const PACK_DEFINITION_TYPE_UUID: TypeUuid = TypeUuid([
    0x36, 0x7c, 0xe2, 0x4c, 0xce, 0xc3, 0x5b, 0x17, 0xb4, 0x0f, 0x36, 0x2f, 0x90, 0x0c, 0xb5, 0xb0,
]);

/// Sorted by UUID so bootstrap membership is a binary search.
pub const BOOTSTRAP_CONTROL_TYPE_UUIDS: [TypeUuid; BOOTSTRAP_CONTROL_COUNT] = [
    PACK_DEFINITION_TYPE_UUID,
    SCHEMA_LINEAGE_MANIFEST_TYPE_UUID,
    IMPORT_RECORD_TYPE_UUID,
    MIGRATION_TYPE_UUID,
    DIRECTORY_IMPORT_RULES_TYPE_UUID,
];

fn symbol_uuid(symbol: BootstrapControlSymbol) -> TypeUuid {
    match symbol {
        BootstrapControlSymbol::SchemaLineageManifest => SCHEMA_LINEAGE_MANIFEST_TYPE_UUID,
        BootstrapControlSymbol::Migration => MIGRATION_TYPE_UUID,
        BootstrapControlSymbol::ImportRecord => IMPORT_RECORD_TYPE_UUID,
        BootstrapControlSymbol::DirectoryImportRules => DIRECTORY_IMPORT_RULES_TYPE_UUID,
        BootstrapControlSymbol::PackDefinition => PACK_DEFINITION_TYPE_UUID,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapControlSpecRowV1 {
    pub symbol: BootstrapControlSymbol,
    pub type_uuid: TypeUuid,
    /// Exact canonical logical-schema bytes rooted at this type.
    pub logical_schema: Vec<u8>,
    pub logical_hash: LogicalHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapControlSpecV1(pub [BootstrapControlSpecRowV1; BOOTSTRAP_CONTROL_COUNT]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapSpecError {
    UnsupportedVersion(u8),
    WrongCount(u32),
    UnknownSymbol(u8),
    DuplicateSymbol(BootstrapControlSymbol),
    DuplicateType(TypeUuid),
    SymbolUuidMismatch {
        symbol: BootstrapControlSymbol,
        expected: TypeUuid,
        observed: TypeUuid,
    },
    EmptyLogicalSchema(BootstrapControlSymbol),
    RowsNotStrictlySorted,
    LengthOverflow,
    Truncated,
    TrailingBytes,
}

impl fmt::Display for BootstrapSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for BootstrapSpecError {}

impl BootstrapControlSpecV1 {
    pub fn parse(bytes: &[u8]) -> Result<Self, BootstrapSpecError> {
        let mut reader = Reader { bytes, position: 0 };
        let version = reader.u8()?;
        if version != VERSION {
            return Err(BootstrapSpecError::UnsupportedVersion(version));
        }
        let count = reader.u32()?;
        if count != BOOTSTRAP_CONTROL_COUNT as u32 {
            return Err(BootstrapSpecError::WrongCount(count));
        }

        let mut rows = Vec::with_capacity(BOOTSTRAP_CONTROL_COUNT);
        let mut symbols = BTreeSet::new();
        for _ in 0..BOOTSTRAP_CONTROL_COUNT {
            let symbol = BootstrapControlSymbol::decode(reader.u8()?)?;
            if !symbols.insert(symbol) {
                return Err(BootstrapSpecError::DuplicateSymbol(symbol));
            }
            let type_uuid = TypeUuid(reader.array()?);
            let expected = symbol_uuid(symbol);
            if type_uuid != expected {
                return Err(BootstrapSpecError::SymbolUuidMismatch {
                    symbol,
                    expected,
                    observed: type_uuid,
                });
            }
            let length =
                usize::try_from(reader.u32()?).map_err(|_| BootstrapSpecError::LengthOverflow)?;
            let logical_schema = reader.take(length)?.to_vec();
            if logical_schema.is_empty() {
                return Err(BootstrapSpecError::EmptyLogicalSchema(symbol));
            }
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"DSLH");
            hasher.update(&[VERSION]);
            hasher.update(&logical_schema);
            rows.push(BootstrapControlSpecRowV1 {
                symbol,
                type_uuid,
                logical_schema,
                logical_hash: LogicalHash(*hasher.finalize().as_bytes()),
            });
        }
        reader.finish()?;
        for pair in rows.windows(2) {
            if pair[0].type_uuid == pair[1].type_uuid {
                return Err(BootstrapSpecError::DuplicateType(pair[0].type_uuid));
            }
            if pair[0].type_uuid > pair[1].type_uuid {
                return Err(BootstrapSpecError::RowsNotStrictlySorted);
            }
        }
        Ok(Self(rows.try_into().expect("fixed bootstrap row count")))
    }

    pub fn embedded() -> Result<Self, BootstrapSpecError> {
        Self::parse(BOOTSTRAP_CONTROL_SPEC_V1_BYTES)
    }

    pub fn encode(&self) -> Result<Vec<u8>, BootstrapSpecError> {
        let mut bytes = vec![VERSION];
        bytes.extend_from_slice(&(BOOTSTRAP_CONTROL_COUNT as u32).to_le_bytes());
        for row in &self.0 {
            bytes.push(row.symbol as u8);
            bytes.extend_from_slice(&row.type_uuid.0);
            let length = u32::try_from(row.logical_schema.len())
                .map_err(|_| BootstrapSpecError::LengthOverflow)?;
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(&row.logical_schema);
        }
        // Encoding is used only by diagnostics/tests; the parser remains the
        // single structural authority.
        Self::parse(&bytes)?;
        Ok(bytes)
    }

    pub fn type_uuids(&self) -> [TypeUuid; BOOTSTRAP_CONTROL_COUNT] {
        self.0.clone().map(|row| row.type_uuid)
    }
}

pub fn is_bootstrap_control_type(type_uuid: TypeUuid) -> bool {
    BOOTSTRAP_CONTROL_TYPE_UUIDS
        .binary_search(&type_uuid)
        .is_ok()
}

pub fn bootstrap_control_logical_registry_v1(
) -> Result<BTreeMap<TypeUuid, LogicalHash>, BootstrapSpecError> {
    Ok(BootstrapControlSpecV1::embedded()?
        .0
        .into_iter()
        .map(|row| (row.type_uuid, row.logical_hash))
        .collect())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], BootstrapSpecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(BootstrapSpecError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(BootstrapSpecError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], BootstrapSpecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| BootstrapSpecError::Truncated)
    }

    fn u8(&mut self) -> Result<u8, BootstrapSpecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, BootstrapSpecError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn finish(self) -> Result<(), BootstrapSpecError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(BootstrapSpecError::TrailingBytes)
        }
    }
}

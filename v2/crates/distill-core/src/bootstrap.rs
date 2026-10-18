//! The sealed logical bootstrap of the bundle formats: a file under the
//! shared header ([`Domain::BootstrapSpec`], version [`VERSION`]).
//! `ImportRecord` and `PackDefinition` are as format 1 sealed them;
//! `DirectoryImportRules` is format 2's, its rule settings a sparse dynamic
//! value (`Dynamic { sparse: true }`) where format 1 spelled a JSON tree
//! (`distill_bundle::format_version_for`).
//!
//! These three rows are the only bootstrap authority. They deliberately carry
//! no target-native layout, compiled registry projection, or consumer brand.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::encoding::{Decoder, Encoder};
use crate::hash::{digest_bytes, Domain, SCHEMA_DIGEST_VERSION};
use crate::header::{read_header, write_header, HeaderError};
use crate::id::{LogicalHash, TypeUuid};

mod checked_in {
    include!("bootstrap_spec_v1.rs");
}

pub use checked_in::BOOTSTRAP_CONTROL_SPEC_V1_BYTES;

/// The spec file's version (its header's): `u32` symbols and schema
/// encodings with `u32` node kinds (§5).
pub const VERSION: u32 = 2;
pub const BOOTSTRAP_CONTROL_COUNT: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum BootstrapControlSymbol {
    ImportRecord = 3,
    DirectoryImportRules = 4,
    PackDefinition = 5,
}

impl BootstrapControlSymbol {
    fn decode(value: u32) -> Result<Self, BootstrapSpecError> {
        match value {
            3 => Ok(Self::ImportRecord),
            4 => Ok(Self::DirectoryImportRules),
            5 => Ok(Self::PackDefinition),
            value => Err(BootstrapSpecError::UnknownSymbol(value)),
        }
    }
}

/// UUIDv5(namespace URL, `https://distill.dev/bundle/v1/control/<symbol>`).
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
    IMPORT_RECORD_TYPE_UUID,
    DIRECTORY_IMPORT_RULES_TYPE_UUID,
];

fn symbol_uuid(symbol: BootstrapControlSymbol) -> TypeUuid {
    match symbol {
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
    Header(HeaderError),
    WrongCount(u32),
    UnknownSymbol(u32),
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
    Decode(crate::encoding::DecodeError),
}

impl fmt::Display for BootstrapSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for BootstrapSpecError {}

impl BootstrapControlSpecV1 {
    pub fn parse(bytes: &[u8]) -> Result<Self, BootstrapSpecError> {
        let body = read_header(bytes, Domain::BootstrapSpec, VERSION).map_err(BootstrapSpecError::Header)?;
        let mut reader = Decoder::new(body);
        let count = reader.u32().map_err(BootstrapSpecError::Decode)?;
        if count != BOOTSTRAP_CONTROL_COUNT as u32 {
            return Err(BootstrapSpecError::WrongCount(count));
        }

        let mut rows = Vec::with_capacity(BOOTSTRAP_CONTROL_COUNT);
        let mut symbols = BTreeSet::new();
        for _ in 0..BOOTSTRAP_CONTROL_COUNT {
            let symbol =
                BootstrapControlSymbol::decode(reader.u32().map_err(BootstrapSpecError::Decode)?)?;
            if !symbols.insert(symbol) {
                return Err(BootstrapSpecError::DuplicateSymbol(symbol));
            }
            let type_uuid = TypeUuid(reader.array().map_err(BootstrapSpecError::Decode)?);
            let expected = symbol_uuid(symbol);
            if type_uuid != expected {
                return Err(BootstrapSpecError::SymbolUuidMismatch {
                    symbol,
                    expected,
                    observed: type_uuid,
                });
            }
            let logical_schema = reader
                .bytes()
                .map_err(BootstrapSpecError::Decode)?
                .to_vec();
            if logical_schema.is_empty() {
                return Err(BootstrapSpecError::EmptyLogicalSchema(symbol));
            }
            let logical_hash = LogicalHash(digest_bytes(
                Domain::Schema,
                SCHEMA_DIGEST_VERSION,
                &logical_schema,
            ));
            rows.push(BootstrapControlSpecRowV1 {
                symbol,
                type_uuid,
                logical_schema,
                logical_hash,
            });
        }
        reader.finish().map_err(BootstrapSpecError::Decode)?;
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
        let mut encoder = Encoder::new();
        encoder.u32(BOOTSTRAP_CONTROL_COUNT as u32);
        for row in &self.0 {
            encoder.enum_variant(row.symbol as u32);
            encoder.raw(&row.type_uuid.0);
            if u32::try_from(row.logical_schema.len()).is_err() {
                return Err(BootstrapSpecError::LengthOverflow);
            }
            encoder.bytes(&row.logical_schema);
        }
        let mut bytes = Vec::new();
        write_header(&mut bytes, Domain::BootstrapSpec, VERSION);
        bytes.extend_from_slice(encoder.as_bytes());
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


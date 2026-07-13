//! Canonical compiled-consumer attestation (§§3, 5, 15–17).
//!
//! This module is deliberately dependency-free apart from `distill-core`'s
//! identity and hashing primitives. Every producer and consumer uses these
//! exact row types and codecs; no surface accepts caller-described opaque
//! "registry extras" bytes.

use std::fmt;

use unicode_normalization::UnicodeNormalization;

use crate::canonical::{DSCA, DSRE};
use crate::id::{LogicalHash, TypeUuid};

const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaNodeId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegistryPathStep {
    Field(String),
    Variant(String),
    Elem,
    MapKey,
    MapValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReferenceStrength {
    Weak,
    Strong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlRole {
    Unrestricted,
    AuthoringOnlyRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegistryExtraFact {
    Reference {
        strength: ReferenceStrength,
        target: TypeUuid,
    },
    Blob,
    Tag,
    Skip,
    BuildOnly(bool),
    ControlRole(ControlRole),
    /// A recursive expansion edge. The target is the node assigned at the
    /// first expansion of the repeated Rust/schema type.
    BackReference {
        target: SchemaNodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegistryExtraRow {
    pub node: SchemaNodeId,
    pub path: Vec<RegistryPathStep>,
    pub fact: RegistryExtraFact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegistryExtrasDigest(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RegistryExtrasV1 {
    pub rows: Vec<RegistryExtraRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTypeRow {
    pub type_uuid: TypeUuid,
    pub logical_hash: LogicalHash,
    pub native_layout_digest: [u8; 32],
    pub build_only: bool,
    pub registry_extras_digest: RegistryExtrasDigest,
    pub registry_extras: RegistryExtrasV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompiledAttestationDigest(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTypeTable {
    pub rows: Vec<CompiledTypeRow>,
    pub digest: CompiledAttestationDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationError {
    UnsupportedVersion(u8),
    Truncated,
    TrailingBytes,
    InvalidUtf8,
    NonCanonicalText,
    UnknownPathStep(u8),
    UnknownFact(u8),
    InvalidBool(u8),
    UnknownReferenceStrength(u8),
    UnknownControlRole(u8),
    ExtrasNotStrictlySorted,
    DuplicateExtraRow,
    ExtrasDigestMismatch,
    TypeRowsNotStrictlySorted,
    DuplicateType(TypeUuid),
    CompiledDigestMismatch,
    CountOverflow,
}

impl fmt::Display for AttestationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AttestationError {}

impl RegistryExtrasV1 {
    /// Normalizes path labels and sorts producer rows into the sole canonical
    /// order. Normalization collisions are rejected as duplicate facts.
    pub fn canonical(mut rows: Vec<RegistryExtraRow>) -> Result<Self, AttestationError> {
        for row in &mut rows {
            for step in &mut row.path {
                match step {
                    RegistryPathStep::Field(value) | RegistryPathStep::Variant(value) => {
                        *value = value.nfc().collect();
                    }
                    RegistryPathStep::Elem
                    | RegistryPathStep::MapKey
                    | RegistryPathStep::MapValue => {}
                }
            }
        }
        let mut keyed = rows
            .into_iter()
            .map(|row| Ok((registry_row_order_key(&row)?, row)))
            .collect::<Result<Vec<_>, AttestationError>>()?;
        keyed.sort_by(|left, right| left.0.cmp(&right.0));
        if keyed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(AttestationError::DuplicateExtraRow);
        }
        let rows = keyed.into_iter().map(|(_, row)| row).collect();
        Ok(Self { rows })
    }

    /// Validates bytes/rows received across a trust boundary without sorting
    /// them on the receiver's behalf.
    pub fn validate(&self) -> Result<(), AttestationError> {
        for row in &self.rows {
            for step in &row.path {
                if let RegistryPathStep::Field(value) | RegistryPathStep::Variant(value) = step {
                    if value.nfc().collect::<String>() != *value {
                        return Err(AttestationError::NonCanonicalText);
                    }
                }
            }
        }
        let keys = self
            .rows
            .iter()
            .map(registry_row_order_key)
            .collect::<Result<Vec<_>, _>>()?;
        for pair in keys.windows(2) {
            if pair[0] == pair[1] {
                return Err(AttestationError::DuplicateExtraRow);
            }
            if pair[0] > pair[1] {
                return Err(AttestationError::ExtrasNotStrictlySorted);
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, AttestationError> {
        self.validate()?;
        let mut out = vec![VERSION];
        put_count(&mut out, self.rows.len())?;
        for row in &self.rows {
            out.extend_from_slice(&row.node.0.to_le_bytes());
            encode_registry_path(&mut out, &row.path)?;
            encode_registry_fact(&mut out, &row.fact);
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AttestationError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != VERSION {
            return Err(AttestationError::UnsupportedVersion(version));
        }
        let count = reader.u32()? as usize;
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let node = SchemaNodeId(reader.u32()?);
            let path_count = reader.u32()? as usize;
            let mut path = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                path.push(match reader.u8()? {
                    1 => RegistryPathStep::Field(reader.text()?),
                    2 => RegistryPathStep::Variant(reader.text()?),
                    3 => RegistryPathStep::Elem,
                    4 => RegistryPathStep::MapKey,
                    5 => RegistryPathStep::MapValue,
                    value => return Err(AttestationError::UnknownPathStep(value)),
                });
            }
            let fact = match reader.u8()? {
                1 => {
                    let strength = match reader.u8()? {
                        0 => ReferenceStrength::Weak,
                        1 => ReferenceStrength::Strong,
                        value => return Err(AttestationError::UnknownReferenceStrength(value)),
                    };
                    RegistryExtraFact::Reference {
                        strength,
                        target: TypeUuid(reader.a16()?),
                    }
                }
                2 => RegistryExtraFact::Blob,
                3 => RegistryExtraFact::Tag,
                4 => RegistryExtraFact::Skip,
                5 => RegistryExtraFact::BuildOnly(reader.boolean()?),
                6 => RegistryExtraFact::ControlRole(match reader.u8()? {
                    0 => ControlRole::Unrestricted,
                    1 => ControlRole::AuthoringOnlyRequired,
                    value => return Err(AttestationError::UnknownControlRole(value)),
                }),
                7 => RegistryExtraFact::BackReference {
                    target: SchemaNodeId(reader.u32()?),
                },
                value => return Err(AttestationError::UnknownFact(value)),
            };
            rows.push(RegistryExtraRow { node, path, fact });
        }
        reader.finish()?;
        let result = Self { rows };
        result.validate()?;
        Ok(result)
    }

    pub fn digest(&self) -> Result<RegistryExtrasDigest, AttestationError> {
        let encoded = self.encode()?;
        let mut hash = blake3::Hasher::new();
        hash.update(&DSRE);
        // The encoded representation starts with the normative version.
        hash.update(&encoded);
        Ok(RegistryExtrasDigest(*hash.finalize().as_bytes()))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct RegistryRowOrderKey {
    node: u32,
    encoded_path: Vec<u8>,
    encoded_fact: Vec<u8>,
}

fn registry_row_order_key(row: &RegistryExtraRow) -> Result<RegistryRowOrderKey, AttestationError> {
    let mut encoded_path = Vec::new();
    encode_registry_path(&mut encoded_path, &row.path)?;
    let mut encoded_fact = Vec::new();
    encode_registry_fact(&mut encoded_fact, &row.fact);
    Ok(RegistryRowOrderKey {
        node: row.node.0,
        encoded_path,
        encoded_fact,
    })
}

fn encode_registry_path(
    out: &mut Vec<u8>,
    path: &[RegistryPathStep],
) -> Result<(), AttestationError> {
    put_count(out, path.len())?;
    for step in path {
        match step {
            RegistryPathStep::Field(value) => {
                out.push(1);
                put_bytes(out, value.as_bytes())?;
            }
            RegistryPathStep::Variant(value) => {
                out.push(2);
                put_bytes(out, value.as_bytes())?;
            }
            RegistryPathStep::Elem => out.push(3),
            RegistryPathStep::MapKey => out.push(4),
            RegistryPathStep::MapValue => out.push(5),
        }
    }
    Ok(())
}

fn encode_registry_fact(out: &mut Vec<u8>, fact: &RegistryExtraFact) {
    match fact {
        RegistryExtraFact::Reference { strength, target } => {
            out.push(1);
            out.push(match strength {
                ReferenceStrength::Weak => 0,
                ReferenceStrength::Strong => 1,
            });
            out.extend_from_slice(&target.0);
        }
        RegistryExtraFact::Blob => out.push(2),
        RegistryExtraFact::Tag => out.push(3),
        RegistryExtraFact::Skip => out.push(4),
        RegistryExtraFact::BuildOnly(value) => {
            out.push(5);
            out.push(u8::from(*value));
        }
        RegistryExtraFact::ControlRole(role) => {
            out.push(6);
            out.push(match role {
                ControlRole::Unrestricted => 0,
                ControlRole::AuthoringOnlyRequired => 1,
            });
        }
        RegistryExtraFact::BackReference { target } => {
            out.push(7);
            out.extend_from_slice(&target.0.to_le_bytes());
        }
    }
}

impl CompiledTypeRow {
    pub fn new(
        type_uuid: TypeUuid,
        logical_hash: LogicalHash,
        native_layout_digest: [u8; 32],
        build_only: bool,
        registry_extras: RegistryExtrasV1,
    ) -> Result<Self, AttestationError> {
        let registry_extras_digest = registry_extras.digest()?;
        Ok(Self {
            type_uuid,
            logical_hash,
            native_layout_digest,
            build_only,
            registry_extras_digest,
            registry_extras,
        })
    }

    pub fn validate(&self) -> Result<(), AttestationError> {
        if self.registry_extras.digest()? != self.registry_extras_digest {
            return Err(AttestationError::ExtrasDigestMismatch);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, AttestationError> {
        self.validate()?;
        let extras = self.registry_extras.encode()?;
        let mut out = Vec::new();
        out.extend_from_slice(&self.type_uuid.0);
        out.extend_from_slice(&self.logical_hash.0);
        out.extend_from_slice(&self.native_layout_digest);
        out.push(u8::from(self.build_only));
        out.extend_from_slice(&self.registry_extras_digest.0);
        put_bytes(&mut out, &extras)?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AttestationError> {
        let mut reader = Reader::new(bytes);
        let type_uuid = TypeUuid(reader.a16()?);
        let logical_hash = LogicalHash(reader.a32()?);
        let native_layout_digest = reader.a32()?;
        let build_only = reader.boolean()?;
        let registry_extras_digest = RegistryExtrasDigest(reader.a32()?);
        let registry_extras = RegistryExtrasV1::decode(reader.bytes()?)?;
        reader.finish()?;
        let result = Self {
            type_uuid,
            logical_hash,
            native_layout_digest,
            build_only,
            registry_extras_digest,
            registry_extras,
        };
        result.validate()?;
        Ok(result)
    }
}

impl CompiledTypeTable {
    pub fn canonical(mut rows: Vec<CompiledTypeRow>) -> Result<Self, AttestationError> {
        rows.sort_by_key(|row| row.type_uuid);
        validate_type_rows(&rows)?;
        let digest = compute_compiled_attestation_digest(&rows)?;
        Ok(Self { rows, digest })
    }

    pub fn from_canonical(
        rows: Vec<CompiledTypeRow>,
        digest: CompiledAttestationDigest,
    ) -> Result<Self, AttestationError> {
        validate_type_rows(&rows)?;
        let expected = compute_compiled_attestation_digest(&rows)?;
        if expected != digest {
            return Err(AttestationError::CompiledDigestMismatch);
        }
        Ok(Self { rows, digest })
    }

    pub fn validate(&self) -> Result<(), AttestationError> {
        Self::from_canonical(self.rows.clone(), self.digest).map(|_| ())
    }
}

pub fn compute_compiled_attestation_digest(
    rows: &[CompiledTypeRow],
) -> Result<CompiledAttestationDigest, AttestationError> {
    validate_type_rows(rows)?;
    let mut hash = blake3::Hasher::new();
    hash.update(&DSCA);
    hash.update(&[VERSION]);
    hash.update(
        &u32::try_from(rows.len())
            .map_err(|_| AttestationError::CountOverflow)?
            .to_le_bytes(),
    );
    for row in rows {
        let encoded = row.encode()?;
        hash.update(&encoded);
    }
    Ok(CompiledAttestationDigest(*hash.finalize().as_bytes()))
}

fn validate_type_rows(rows: &[CompiledTypeRow]) -> Result<(), AttestationError> {
    for row in rows {
        row.validate()?;
    }
    for pair in rows.windows(2) {
        if pair[0].type_uuid == pair[1].type_uuid {
            return Err(AttestationError::DuplicateType(pair[0].type_uuid));
        }
        if pair[0].type_uuid > pair[1].type_uuid {
            return Err(AttestationError::TypeRowsNotStrictlySorted);
        }
    }
    Ok(())
}

fn put_count(out: &mut Vec<u8>, count: usize) -> Result<(), AttestationError> {
    out.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| AttestationError::CountOverflow)?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), AttestationError> {
    put_count(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], AttestationError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(AttestationError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(AttestationError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, AttestationError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, AttestationError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("checked width"),
        ))
    }

    fn a16(&mut self) -> Result<[u8; 16], AttestationError> {
        Ok(self.take(16)?.try_into().expect("checked width"))
    }

    fn a32(&mut self) -> Result<[u8; 32], AttestationError> {
        Ok(self.take(32)?.try_into().expect("checked width"))
    }

    fn boolean(&mut self) -> Result<bool, AttestationError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(AttestationError::InvalidBool(value)),
        }
    }

    fn bytes(&mut self) -> Result<&'a [u8], AttestationError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn text(&mut self) -> Result<String, AttestationError> {
        let value =
            std::str::from_utf8(self.bytes()?).map_err(|_| AttestationError::InvalidUtf8)?;
        if value.nfc().collect::<String>() != value {
            return Err(AttestationError::NonCanonicalText);
        }
        Ok(value.to_owned())
    }

    fn finish(self) -> Result<(), AttestationError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(AttestationError::TrailingBytes)
        }
    }
}

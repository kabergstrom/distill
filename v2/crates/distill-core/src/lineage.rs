//! Shared bundle/store lineage carriers.

use std::fmt;

use crate::canonical::{domain_digest, DSSL};
use crate::id::{LogicalHash, TypeUuid};

/// One accepted schema epoch and its explicit forward parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedSchemaEpoch {
    pub digest: LogicalHash,
    pub forward_parent: Option<u32>,
}

/// Verifiable accepted-manifest prefix embedded beside authored data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageStamp {
    pub epochs: Vec<AcceptedSchemaEpoch>,
    pub cursor: u32,
    pub chain: [u8; 32],
}

impl LineageStamp {
    pub fn generation(&self) -> u64 {
        self.epochs.len() as u64
    }

    pub fn selected_digest(&self) -> Option<LogicalHash> {
        self.epochs
            .get(usize::try_from(self.cursor).ok()?)
            .map(|epoch| epoch.digest)
    }
}

/// Closed bundle-entry lineage carrier. Format-owned bootstrap controls do
/// not participate in user schema lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryLineageV1 {
    Manifest(LineageStamp),
    Bootstrap { bundle_format_version: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryLineageCodecError {
    UnsupportedVersion(u8),
    UnknownTag(u8),
    InvalidOptionTag(u8),
    CountOverflow,
    Truncated,
    TrailingBytes,
}

impl fmt::Display for EntryLineageCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for EntryLineageCodecError {}

impl EntryLineageV1 {
    const RECORD_VERSION: u8 = 1;
    const MANIFEST_TAG: u8 = 1;
    const BOOTSTRAP_TAG: u8 = 2;

    /// Canonical record grammar used outside the human-readable bundle JSON.
    pub fn encode_record(&self) -> Result<Vec<u8>, EntryLineageCodecError> {
        let mut bytes = vec![Self::RECORD_VERSION];
        match self {
            Self::Manifest(stamp) => {
                bytes.push(Self::MANIFEST_TAG);
                let count = u32::try_from(stamp.epochs.len())
                    .map_err(|_| EntryLineageCodecError::CountOverflow)?;
                bytes.extend_from_slice(&count.to_le_bytes());
                for epoch in &stamp.epochs {
                    bytes.extend_from_slice(&epoch.digest.0);
                    match epoch.forward_parent {
                        None => bytes.push(0),
                        Some(parent) => {
                            bytes.push(1);
                            bytes.extend_from_slice(&parent.to_le_bytes());
                        }
                    }
                }
                bytes.extend_from_slice(&stamp.cursor.to_le_bytes());
                bytes.extend_from_slice(&stamp.chain);
            }
            Self::Bootstrap {
                bundle_format_version,
            } => {
                bytes.push(Self::BOOTSTRAP_TAG);
                bytes.extend_from_slice(&bundle_format_version.to_le_bytes());
            }
        }
        Ok(bytes)
    }

    pub fn decode_record(bytes: &[u8]) -> Result<Self, EntryLineageCodecError> {
        let mut reader = RecordReader::new(bytes);
        let version = reader.u8()?;
        if version != Self::RECORD_VERSION {
            return Err(EntryLineageCodecError::UnsupportedVersion(version));
        }
        let value = match reader.u8()? {
            Self::MANIFEST_TAG => {
                let count = reader.u32()?;
                let minimum = usize::try_from(count)
                    .ok()
                    .and_then(|count| count.checked_mul(33))
                    .and_then(|count| count.checked_add(36))
                    .ok_or(EntryLineageCodecError::CountOverflow)?;
                if reader.remaining() < minimum {
                    return Err(EntryLineageCodecError::Truncated);
                }
                let mut epochs = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let digest = LogicalHash(reader.array()?);
                    let forward_parent = match reader.u8()? {
                        0 => None,
                        1 => Some(reader.u32()?),
                        tag => return Err(EntryLineageCodecError::InvalidOptionTag(tag)),
                    };
                    epochs.push(AcceptedSchemaEpoch {
                        digest,
                        forward_parent,
                    });
                }
                Self::Manifest(LineageStamp {
                    epochs,
                    cursor: reader.u32()?,
                    chain: reader.array()?,
                })
            }
            Self::BOOTSTRAP_TAG => Self::Bootstrap {
                bundle_format_version: reader.u32()?,
            },
            tag => return Err(EntryLineageCodecError::UnknownTag(tag)),
        };
        if reader.remaining() != 0 {
            return Err(EntryLineageCodecError::TrailingBytes);
        }
        Ok(value)
    }
}

struct RecordReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> RecordReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], EntryLineageCodecError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(EntryLineageCodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(EntryLineageCodecError::Truncated)?
            .try_into()
            .expect("slice has exact requested length");
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, EntryLineageCodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, EntryLineageCodecError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
}

/// DSSL v1 commitment to one accepted prefix and cursor.
pub fn lineage_chain_digest(
    type_uuid: TypeUuid,
    epochs: &[AcceptedSchemaEpoch],
    cursor: u32,
) -> [u8; 32] {
    domain_digest(DSSL, 1, |encoder| {
        encoder.raw(&type_uuid.0);
        encoder.seq(epochs, |encoder, epoch| {
            encoder.raw(&epoch.digest.0);
            encoder.option(epoch.forward_parent, |encoder, parent| encoder.u32(*parent));
        });
        encoder.u32(cursor);
    })
}

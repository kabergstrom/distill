//! Closed processor-output declarations (§9). Runtime binding uses neutral
//! authored values and the live pipeline registration arena.

use std::collections::BTreeMap;

use distill_core::id::TypeUuid;

use crate::query::{normalize_identifier, IntakeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputDecls {
    pub primary: TypeUuid,
    pub extras: BTreeMap<String, TypeUuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputError {
    InvalidKey(IntakeError),
    TooManyOutputs,
    DuplicateKey(String),
}

impl OutputDecls {
    pub fn new(primary: TypeUuid, extras: Vec<(String, TypeUuid)>) -> Result<Self, OutputError> {
        if extras.len() + 1 > 256 {
            return Err(OutputError::TooManyOutputs);
        }
        let mut normalized = BTreeMap::new();
        for (key, ty) in extras {
            let key = normalize_identifier(&key).map_err(OutputError::InvalidKey)?;
            if normalized.insert(key.clone(), ty).is_some() {
                return Err(OutputError::DuplicateKey(key));
            }
        }
        Ok(Self {
            primary,
            extras: normalized,
        })
    }
}

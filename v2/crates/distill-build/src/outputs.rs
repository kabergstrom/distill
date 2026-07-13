//! Closed processor-output binding (§9). Typed module values are lowered
//! before entering this API; the collector enforces the static declaration.

use std::collections::{BTreeMap, BTreeSet};

use distill_core::id::{AssetUuid, TypeUuid};

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
    PrimaryAlreadyBound,
    PrimaryMissing,
    UnknownExtra(String),
    ExtraAlreadyBound(String),
    WrongType {
        key: String,
        expected: TypeUuid,
        actual: TypeUuid,
    },
    MissingExtras(Vec<String>),
    DuplicateDebugKey(String),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedReference {
    pub strong: bool,
    pub asset: AssetUuid,
    pub expected_terminal: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedValue {
    pub type_uuid: TypeUuid,
    pub bytes: Vec<u8>,
    pub references: Vec<EncodedReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOutput {
    pub uuid: AssetUuid,
    /// Empty is the primary's reserved output-table key.
    pub output_key: String,
    pub encoded_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub value: EncodedValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOutputs {
    pub outputs: Vec<BoundOutput>,
    pub debug: BTreeMap<String, Vec<u8>>,
}

pub struct OutputCollector {
    parent: AssetUuid,
    decls: OutputDecls,
    primary: Option<EncodedValue>,
    extras: BTreeMap<String, EncodedValue>,
    debug: BTreeMap<String, Vec<u8>>,
    bound: BTreeSet<String>,
}

impl OutputCollector {
    pub fn new(parent: AssetUuid, decls: OutputDecls) -> Self {
        Self {
            parent,
            decls,
            primary: None,
            extras: BTreeMap::new(),
            debug: BTreeMap::new(),
            bound: BTreeSet::new(),
        }
    }

    pub fn primary(&mut self, value: EncodedValue) -> Result<(), OutputError> {
        if self.primary.is_some() {
            return Err(OutputError::PrimaryAlreadyBound);
        }
        if value.type_uuid != self.decls.primary {
            return Err(OutputError::WrongType {
                key: String::new(),
                expected: self.decls.primary,
                actual: value.type_uuid,
            });
        }
        self.primary = Some(value);
        Ok(())
    }

    pub fn extra(&mut self, key: &str, value: EncodedValue) -> Result<AssetUuid, OutputError> {
        let key = normalize_identifier(key).map_err(OutputError::InvalidKey)?;
        let expected = self
            .decls
            .extras
            .get(&key)
            .copied()
            .ok_or_else(|| OutputError::UnknownExtra(key.clone()))?;
        if value.type_uuid != expected {
            return Err(OutputError::WrongType {
                key,
                expected,
                actual: value.type_uuid,
            });
        }
        if !self.bound.insert(key.clone()) {
            return Err(OutputError::ExtraAlreadyBound(key));
        }
        let child = AssetUuid::v5(self.parent, &key);
        self.extras.insert(key, value);
        Ok(child)
    }

    pub fn debug(&mut self, key: &str, bytes: Vec<u8>) -> Result<(), OutputError> {
        let key = normalize_identifier(key).map_err(OutputError::InvalidKey)?;
        if self.debug.insert(key.clone(), bytes).is_some() {
            return Err(OutputError::DuplicateDebugKey(key));
        }
        Ok(())
    }

    pub fn finish(self) -> Result<BoundOutputs, OutputError> {
        let primary = self.primary.ok_or(OutputError::PrimaryMissing)?;
        let missing: Vec<_> = self
            .decls
            .extras
            .keys()
            .filter(|key| !self.extras.contains_key(*key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(OutputError::MissingExtras(missing));
        }
        let mut outputs = Vec::with_capacity(self.extras.len() + 1);
        outputs.push(BoundOutput {
            uuid: self.parent,
            output_key: String::new(),
            encoded_type: primary.type_uuid,
            terminal_type: primary.type_uuid,
            value: primary,
        });
        outputs.extend(self.extras.into_iter().map(|(key, value)| BoundOutput {
            uuid: AssetUuid::v5(self.parent, &key),
            output_key: key,
            // Extras are terminal directly, regardless of whether a
            // processor is registered for their declared type.
            encoded_type: value.type_uuid,
            terminal_type: value.type_uuid,
            value,
        }));
        Ok(BoundOutputs {
            outputs,
            debug: self.debug,
        })
    }
}

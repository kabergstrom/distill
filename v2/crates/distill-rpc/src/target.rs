use crate::TargetDefinitionHash;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetSetError {
    DuplicateTarget { target: String },
}

impl std::fmt::Display for TargetSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for TargetSetError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDefinition {
    name: String,
    definition_hash: TargetDefinitionHash,
}

impl TargetDefinition {
    pub fn new(name: impl Into<String>, definition_hash: TargetDefinitionHash) -> Self {
        Self {
            name: name.into().nfc().collect(),
            definition_hash,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn definition_hash(&self) -> TargetDefinitionHash {
        self.definition_hash
    }
}

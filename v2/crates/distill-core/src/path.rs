//! §8 intake normalization of root-relative paths and identifiers, shared by
//! the query grammar (`distill-pipeline-api`) and the pack reader.

use std::fmt;

use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizeError {
    EmptyIdentifier,
    IdentifierTooLong,
    Nul,
    InvalidPath,
}

impl fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentifier => f.write_str("identifier must not be empty"),
            Self::IdentifierTooLong => f.write_str("identifier exceeds 255 UTF-8 bytes"),
            Self::Nul => f.write_str("NUL is not allowed"),
            Self::InvalidPath => {
                f.write_str("path is not a normalized, lexical root-relative path")
            }
        }
    }
}

impl std::error::Error for NormalizeError {}

pub fn normalize_identifier(value: &str) -> Result<String, NormalizeError> {
    if value.contains('\0') {
        return Err(NormalizeError::Nul);
    }
    let value: String = value.nfc().collect();
    if value.is_empty() {
        return Err(NormalizeError::EmptyIdentifier);
    }
    if value.len() > 255 {
        return Err(NormalizeError::IdentifierTooLong);
    }
    Ok(value)
}

pub fn normalize_path(value: &str) -> Result<String, NormalizeError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains(['\\', '\0'])
    {
        return Err(NormalizeError::InvalidPath);
    }
    let mut normalized = Vec::new();
    for component in value.split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(NormalizeError::InvalidPath);
        }
        let component: String = component.nfc().collect();
        if component.is_empty() || matches!(component.as_str(), "." | "..") {
            return Err(NormalizeError::InvalidPath);
        }
        normalized.push(component);
    }
    Ok(normalized.join("/"))
}


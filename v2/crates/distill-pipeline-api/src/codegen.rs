//! What an authoring-side generator returns (§20).

use distill_core::id::{AssetUuid, ContentHash};

/// The one authoritative generated Rust module name for an asset.
pub fn generated_module_name(asset: AssetUuid) -> String {
    let mut name = String::with_capacity(35);
    name.push_str("sp_");
    for byte in asset.0 {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing to String is infallible");
    }
    name
}

/// The longest alias a generated file may carry, in bytes.
pub const MAX_ALIAS_LEN: usize = 64;

/// Rust's strict, reserved and edition keywords: none can name a module.
const RUST_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// Why `alias` cannot name a generated unit's re-export, `None` when it
/// can: `[a-z_][a-z0-9_]*`, at most [`MAX_ALIAS_LEN`] bytes, not `_` and
/// not a Rust keyword.
pub fn alias_defect(alias: &str) -> Option<&'static str> {
    let mut bytes = alias.bytes();
    let Some(first) = bytes.next() else {
        return Some("is empty");
    };
    if alias.len() > MAX_ALIAS_LEN {
        return Some("is longer than 64 bytes");
    }
    if !(first.is_ascii_lowercase() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Some("is not [a-z_][a-z0-9_]*");
    }
    if alias == "_" {
        return Some("is `_`");
    }
    if RUST_KEYWORDS.contains(&alias) {
        return Some("is a Rust keyword");
    }
    None
}

/// One generated unit, `sp_<hex>.rs`, named by its asset's UUID. An alias
/// adds `pub use self::sp_<hex> as <alias>;` to the namespace's `mod.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    asset: AssetUuid,
    relative_path: String,
    bytes: Vec<u8>,
    content_hash: ContentHash,
    alias: Option<String>,
}

impl GeneratedFile {
    pub fn new(asset: AssetUuid, bytes: Vec<u8>) -> Self {
        let relative_path = format!("{}.rs", generated_module_name(asset));
        let content_hash = ContentHash(*blake3::hash(&bytes).as_bytes());
        Self {
            asset,
            relative_path,
            bytes,
            content_hash,
            alias: None,
        }
    }

    /// This file, re-exported as `alias` too. The publication checks the
    /// alias (see [`alias_defect`]) and its uniqueness in the namespace.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn asset(&self) -> AssetUuid {
        self.asset
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// The physical module name, `sp_<hex>`.
    pub fn module_name(&self) -> &str {
        self.relative_path.trim_end_matches(".rs")
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
}

/// What one codegen run returns for the namespace it serves: the units, and
/// the text the namespace's `mod.rs` carries before its module lines
/// (`mod_preamble`, e.g. `#![allow(dead_code)]`) and after them
/// (`mod_items`, e.g. a table of every unit). All of it lies under the
/// generated mark.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodegenOutput {
    pub files: Vec<GeneratedFile>,
    pub mod_preamble: String,
    pub mod_items: String,
}

impl CodegenOutput {
    pub fn new(files: Vec<GeneratedFile>) -> Self {
        Self {
            files,
            ..Self::default()
        }
    }

    pub fn with_mod_preamble(mut self, preamble: impl Into<String>) -> Self {
        self.mod_preamble = preamble.into();
        self
    }

    pub fn with_mod_items(mut self, items: impl Into<String>) -> Self {
        self.mod_items = items.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenFailure {
    Generation(String),
    /// More than one file claims `name` in one namespace, as its physical
    /// module name or as its alias.
    NamespaceCollision {
        name: String,
        claimants: Vec<AssetUuid>,
    },
    /// A file's alias cannot name a re-export (see [`alias_defect`]).
    InvalidAlias {
        asset: AssetUuid,
        alias: String,
        reason: String,
    },
}

impl std::fmt::Display for CodegenFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CodegenFailure {}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    asset: AssetUuid,
    relative_path: String,
    bytes: Vec<u8>,
    content_hash: ContentHash,
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
        }
    }

    pub fn asset(&self) -> AssetUuid {
        self.asset
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenFailure {
    Generation(String),
    NamespaceCollision {
        name: String,
        claimants: Vec<AssetUuid>,
    },
}

impl std::fmt::Display for CodegenFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CodegenFailure {}

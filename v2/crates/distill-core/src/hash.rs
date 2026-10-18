//! §5 hash: every content hash, digest and identity is XXH3-128
//! (`xxhash-rust`, unseeded), its 16 output bytes little-endian. This module
//! is the only one that names `xxhash-rust`; every site calls it.
//!
//! - [`hash`]: raw bytes → [`Hash128`] (content hashes: artifacts, files,
//!   bundles, blobs, CAS extents, pack archives).
//! - [`Hasher`]: the same, streamed.
//! - [`digest`]: `XXH3-128(u32 domain ‖ u32 version ‖ body)`, the body
//!   written by the closure with the one encoding (§5,
//!   [`crate::encoding::Encoder`]). A digest's role is its [`Domain`]; the
//!   version changes when its body's fields change.

use xxhash_rust::xxh3::Xxh3;

use crate::encoding::Encoder;

/// A 128-bit hash: XXH3-128's value as 16 little-endian bytes.
pub type Hash128 = [u8; 16];

/// The XXH3-128 of `bytes`.
pub fn hash(bytes: &[u8]) -> Hash128 {
    xxhash_rust::xxh3::xxh3_128(bytes).to_le_bytes()
}

/// Streaming XXH3-128: `update` any number of times, then `finish`. Equal
/// to [`hash`] over the concatenation.
#[derive(Clone)]
pub struct Hasher(Xxh3);

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Self {
        Self(Xxh3::new())
    }

    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update(bytes);
        self
    }

    pub fn finish(&self) -> Hash128 {
        self.0.digest128().to_le_bytes()
    }
}

impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hasher")
    }
}

/// What a digest identifies, and what a standalone file holds (the shared
/// file header, [`crate::header`]). Discriminants are pinned and never
/// reused; a retired role keeps its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum Domain {
    /// A logical schema's hash: the schema encoding of its root node (§5).
    Schema = 1,
    /// A wire layout's hash: the layout encoding of its tree (§12).
    Layout = 2,
    /// A pipeline module's host-interface ABI identity (§3).
    ModuleAbi = 3,
    /// A target definition (§18).
    TargetDefinition = 4,
    /// A processor stage's static inputs: the build-cache lookup key (§9).
    ProcessorInputs = 5,
    /// A recorded trace: the operation sequence of one build (§9).
    Trace = 6,
    /// A typed local deterministic failure (§5, §9).
    LocalFailure = 7,
    /// Typed configuration-error reason facts (§5, §13, §17).
    ConfigError = 8,
    /// A typed namespace error (§7, §13, §17).
    NamespaceError = 9,
    /// A typed pipeline failure (§3, §13, §17).
    PipelineFailure = 10,
    /// A hermetic tool-execution capsule (§9, §13).
    ToolCapsule = 11,
    /// A build import's key (§8).
    ImportKey = 12,
    /// A processor stage's full input hash: static inputs and trace (§9).
    InputHash = 13,
    /// An asset node's key (§9).
    NodeKey = 14,
    /// An importer's capability: what its outputs are a function of (§8).
    ImporterCapability = 15,
    /// A directory import task: importer, sources, settings, output (§8).
    DirectoryTask = 16,
    /// An asset UUID an importer mints from a seed (§8).
    MintedAsset = 17,
    /// A bundle UUID an importer mints from a seed (§8).
    MintedBundle = 18,
    /// A type epoch: a type's schema hash and physical types (§9).
    TypeEpoch = 19,
    /// The reserved rules-bundle UUID of a root's default import layer (§8).
    DefaultRulesBundle = 20,
    /// A pack's encoded object key (§16).
    PackEntry = 21,
    /// A pack manifest: its digest and its file format (§16).
    PackManifest = 22,
    /// A query result over assets (§10).
    AssetQuery = 23,
    /// A query result over files (§10).
    FileQuery = 24,
    /// An authoring entry's content key (§17).
    ContentKey = 25,
    /// A value's identity under its schema (newgameplus GAP 23 §17.3).
    Value = 26,
    /// The artifact image file format (§12).
    ArtifactImage = 27,
    /// The pack archive file format (§16).
    PackArchive = 28,
    /// The bundle container file format (§6).
    Bundle = 29,
    /// The physical schema file format (newgameplus GAP 23 §17.7).
    SchemaFile = 30,
}

impl Domain {
    /// Every domain, in discriminant order.
    pub const ALL: [Domain; 30] = [
        Domain::Schema,
        Domain::Layout,
        Domain::ModuleAbi,
        Domain::TargetDefinition,
        Domain::ProcessorInputs,
        Domain::Trace,
        Domain::LocalFailure,
        Domain::ConfigError,
        Domain::NamespaceError,
        Domain::PipelineFailure,
        Domain::ToolCapsule,
        Domain::ImportKey,
        Domain::InputHash,
        Domain::NodeKey,
        Domain::ImporterCapability,
        Domain::DirectoryTask,
        Domain::MintedAsset,
        Domain::MintedBundle,
        Domain::TypeEpoch,
        Domain::DefaultRulesBundle,
        Domain::PackEntry,
        Domain::PackManifest,
        Domain::AssetQuery,
        Domain::FileQuery,
        Domain::ContentKey,
        Domain::Value,
        Domain::ArtifactImage,
        Domain::PackArchive,
        Domain::Bundle,
        Domain::SchemaFile,
    ];

    pub fn from_u32(value: u32) -> Option<Domain> {
        Domain::ALL.iter().copied().find(|domain| *domain as u32 == value)
    }
}

/// The digest prefix: `u32 domain ‖ u32 version`, little-endian.
pub fn digest_prefix(domain: Domain, version: u32) -> [u8; 8] {
    let mut prefix = [0; 8];
    prefix[..4].copy_from_slice(&(domain as u32).to_le_bytes());
    prefix[4..].copy_from_slice(&version.to_le_bytes());
    prefix
}

/// `XXH3-128(u32 domain ‖ u32 version ‖ body)`, `f` writing the body.
pub fn digest(domain: Domain, version: u32, f: impl FnOnce(&mut Encoder)) -> Hash128 {
    let mut encoder = Encoder::new();
    f(&mut encoder);
    digest_bytes(domain, version, encoder.as_bytes())
}

/// [`digest`] over an already encoded body.
pub fn digest_bytes(domain: Domain, version: u32, body: &[u8]) -> Hash128 {
    let mut hasher = Hasher::new();
    hasher.update(&digest_prefix(domain, version));
    hasher.update(body);
    hasher.finish()
}

/// The version of the schema digest ([`Domain::Schema`]): every component
/// that hashes a schema encoding (ngp-schema, distill-asset, the bootstrap
/// spec) digests at this version.
pub const SCHEMA_DIGEST_VERSION: u32 = 1;

/// `hash` as 32 lowercase hex chars, byte order as stored.
pub fn to_hex(hash: &Hash128) -> String {
    let mut out = String::with_capacity(32);
    for byte in hash {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

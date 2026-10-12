//! distill-asset: the asset-types support crate — the §4 runtime
//! declarations (`AssetType`, `AssetRuntimeDescriptor`, `ErasedValue`,
//! `EncodeSink`, `AssetRef`/`WeakAssetRef` native forms), the §3 `DefaultTable`
//! vocabulary, the §5 deterministic `BuildHasher`, and the reflection/
//! builder machinery `#[asset]`-generated code (distill-asset-macro)
//! runs against.
//!
//! §4 pins these declarations but names no crate for them ("Native
//! forms, defined by the asset-types crate"; §5: "the asset-types
//! support crate's fixed-seed state") — this crate is that support
//! crate. The §12 generated-table types themselves live in distill-wire
//! and are re-exported, never redeclared.

pub mod build;
pub mod defaults;
pub mod hasher;
pub mod reflect;
pub mod thunks;
pub mod types;

pub use defaults::{
    default_table, AssetDefaults, DefaultCollector, DefaultNode, DefaultNodeType, DefaultTable,
    DefaultWriter, PathStep, SchemaNodeId,
};
pub use distill_asset_macro::asset;
pub use hasher::{AssetHashMap, AssetHashSet, DeterministicState};
pub use reflect::AssetReflect;
pub use types::{
    AssetRef, AssetRuntimeDescriptor, AssetType, EncodeContainer, EncodeSink, EpochToken,
    ErasedValue, ModuleEpochPoisonCause, ModuleEpochToken, WeakAssetRef,
};

// The §12 vocabulary, re-exported for generated code and consumers —
// REUSED from distill-wire, never redeclared (one declaration rule, §5).
pub use distill_wire::exec::Blob;
pub use distill_wire::native::{
    CallbackPanic, CtorCursor, CtorEntry, CtorId, CtorTable, DropId, DropTable, NativeField,
    NativeLayoutNode, NativeTagEncoding, NativeVariant, NativeVariantTag, PushError, ScalarKind,
    SkipDefaultId, SkipEntry, SkipWriterTable,
};

pub use distill_core::id::{AssetUuid, LogicalHash, TypeUuid};
pub use distill_json::AuthoredValue;

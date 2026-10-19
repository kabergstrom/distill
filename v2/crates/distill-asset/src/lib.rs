//! distill-asset: the asset-types support crate — the §4 runtime
//! declarations (`AssetType`, `AssetRuntimeDescriptor`, `ErasedValue`,
//! `AssetRef`/`WeakAssetRef` native forms), the §5 deterministic
//! `BuildHasher`, and asset reflection: the interpreter over each asset
//! crate's walk-generated table (newgameplus GAP 23 §18).
//!
//! §4 pins these declarations but names no crate for them ("Native
//! forms, defined by the asset-types crate"; §5: "the asset-types
//! support crate's fixed-seed state") — this crate is that support
//! crate. The §12 generated-table types themselves live in distill-wire
//! and are re-exported, never redeclared.

pub mod hasher;
pub mod reflect;
pub mod thunks;
pub mod types;

pub use hasher::{AssetHashMap, AssetHashSet, DeterministicState};
/// `#[derive(Asset)]`: marks an asset type for source-walk; inert.
pub use newgameplus_api_macros::Asset;
pub use reflect::{default_value, logical, to_value};
pub use types::{
    AssetRef, AssetRuntimeDescriptor, AssetType, DynamicPatch, EpochToken, ErasedValue,
    ModuleEpochPoisonCause, ModuleEpochToken, WeakAssetRef,
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
pub use ngp_schema::{AssetRefValue, Value};

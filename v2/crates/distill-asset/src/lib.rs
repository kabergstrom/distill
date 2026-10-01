//! distill-asset: the asset-types support crate — the §4 runtime
//! declarations (`AssetType`, `AssetRuntimeDescriptor`, `ErasedValue`,
//! `EncodeSink`, `AssetRef`/`WeakAssetRef` native forms), the §15
//! `PlaceholderThunk` + `placeholder!` macro, the §3 `DefaultTable`
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
    ErasedValue, ModuleEpochPoisonCause, ModuleEpochToken, PlaceholderThunk, WeakAssetRef,
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

/// Constructs a `&'static PlaceholderThunk` from a type and a value
/// expression (§15): the body runs under `catch_unwind` inside the
/// module and the crossing value is an owned `ErasedValue` — a bare
/// `fn() -> T` could neither catch an inside-module panic nor suppress
/// automatic drop on the crossing value (§3's thunk rule, §4's
/// `ErasedValue` contract). The `migration_fn!` pattern (§11): a bare fn
/// is not registrable, so containment cannot be forgotten.
#[macro_export]
macro_rules! placeholder {
    ($ty:ty, $body:expr) => {{
        fn __make(
            owner: $crate::EpochToken,
        ) -> ::core::result::Result<$crate::ErasedValue, $crate::CallbackPanic> {
            match ::std::panic::catch_unwind(|| -> $ty { $body }) {
                ::core::result::Result::Ok(v) => {
                    ::core::result::Result::Ok($crate::ErasedValue::new_in::<$ty>(v, owner))
                }
                ::core::result::Result::Err(_) => {
                    ::core::result::Result::Err($crate::CallbackPanic)
                }
            }
        }
        static __THUNK: $crate::PlaceholderThunk = $crate::PlaceholderThunk {
            type_uuid: <$ty as $crate::AssetType>::TYPE_UUID,
            make: __make,
        };
        &__THUNK
    }};
}

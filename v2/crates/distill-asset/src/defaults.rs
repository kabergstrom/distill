//! `#[asset]`-generated default tables (§3, §6, §11). Paths are typed
//! structural steps rather than dotted strings, and every writer is a
//! panic-containing status thunk.

use core::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};

use distill_json::AuthoredValue;
use distill_wire::native::CallbackPanic;

use crate::{AssetReflect, AssetType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathStep {
    Field(&'static str),
    Variant(&'static str),
    Elem,
    MapKey,
    MapValue,
}

pub type DefaultWriter = fn() -> Result<AuthoredValue, CallbackPanic>;

pub struct DefaultTable<T: AssetType> {
    pub parent: Option<DefaultWriter>,
    pub nodes: &'static [(&'static [PathStep], DefaultWriter)],
    pub _marker: PhantomData<fn() -> T>,
}

pub trait AssetDefaults: AssetType + Sized {
    fn default_table() -> &'static DefaultTable<Self>;
}

pub fn default_table<T: AssetDefaults>() -> &'static DefaultTable<T> {
    T::default_table()
}

/// The generated status thunk used for parent and node defaults.
pub fn write_default<T>() -> Result<AuthoredValue, CallbackPanic>
where
    T: Default + AssetReflect,
{
    catch_unwind(AssertUnwindSafe(|| T::default().to_authored())).map_err(|_| CallbackPanic)
}

/// Convert owned path vectors collected recursively by reflection into
/// one immutable table with module-lifetime storage.
pub fn make_table<T: AssetType>(
    parent: Option<DefaultWriter>,
    nodes: Vec<(Vec<PathStep>, DefaultWriter)>,
) -> DefaultTable<T> {
    let nodes = nodes
        .into_iter()
        .map(|(path, writer)| {
            let path: &'static [PathStep] = Box::leak(path.into_boxed_slice());
            (path, writer)
        })
        .collect::<Vec<_>>();
    DefaultTable {
        parent,
        nodes: Box::leak(nodes.into_boxed_slice()),
        _marker: PhantomData,
    }
}

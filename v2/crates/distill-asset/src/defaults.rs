//! `#[asset]`-generated default tables (§3, §6, §11). Paths are typed
//! structural steps rather than dotted strings, and every writer is a
//! panic-containing status thunk.

use core::marker::PhantomData;
use std::any::TypeId;
use std::collections::HashMap;
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

/// Deterministic identity assigned on the first canonical expansion of a
/// Rust schema node. Recursive back-references reuse the already assigned id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaNodeId(pub u32);

pub type DefaultNode = (SchemaNodeId, Vec<PathStep>, DefaultWriter);

/// The Rust type behind one expanded schema node: its canonical logical
/// node bytes (`build::logical_bytes`) and its own default writer. Lets a
/// migration default provider map a to-schema container node back to the
/// node id its `nodes` entries are keyed by.
#[derive(Debug, Clone)]
pub struct DefaultNodeType {
    pub node: SchemaNodeId,
    pub logical: &'static [u8],
    pub writer: Option<DefaultWriter>,
}

/// First-expansion collector used by generated code. `TypeId` is only an
/// in-process recursion key; externally visible ids are consecutive canonical
/// walk ordinals and therefore do not depend on `TypeId` values.
#[derive(Default)]
pub struct DefaultCollector {
    ids: HashMap<TypeId, SchemaNodeId>,
    nodes: Vec<DefaultNode>,
    types: Vec<DefaultNodeType>,
}

impl DefaultCollector {
    /// Return a newly assigned node id, or `None` when this type was already
    /// expanded and the current edge is a back-reference.
    pub fn begin<T: AssetReflect>(&mut self) -> Option<SchemaNodeId> {
        let key = TypeId::of::<T>();
        if self.ids.contains_key(&key) {
            return None;
        }
        let id = SchemaNodeId(
            u32::try_from(self.ids.len()).expect("asset schema contains more than u32 nodes"),
        );
        self.ids.insert(key, id);
        self.types.push(DefaultNodeType {
            node: id,
            logical: Box::leak(crate::build::logical_bytes::<T>().into_boxed_slice()),
            writer: T::default_writer(),
        });
        Some(id)
    }

    pub fn add(&mut self, node: SchemaNodeId, path: Vec<PathStep>, writer: DefaultWriter) {
        self.nodes.push((node, path, writer));
    }

}

pub struct DefaultTable<T: AssetType> {
    pub parent: Option<DefaultWriter>,
    pub nodes: &'static [(SchemaNodeId, &'static [PathStep], DefaultWriter)],
    pub types: &'static [DefaultNodeType],
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
    collector: DefaultCollector,
) -> DefaultTable<T> {
    let nodes = collector
        .nodes
        .into_iter()
        .map(|(node, path, writer)| {
            let path: &'static [PathStep] = Box::leak(path.into_boxed_slice());
            (node, path, writer)
        })
        .collect::<Vec<_>>();
    DefaultTable {
        parent,
        nodes: Box::leak(nodes.into_boxed_slice()),
        types: Box::leak(collector.types.into_boxed_slice()),
        _marker: PhantomData,
    }
}

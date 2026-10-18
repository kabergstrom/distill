//! Runtime reflection used by `#[asset]` expansion. It is intentionally
//! a closed, typed vocabulary: serializable Rust kinds implement this
//! trait; unsupported and nondeterministically seeded containers do not.

use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use ngp_schema::{AssetRefValue, Value};
use distill_wire::exec::Blob;
use distill_wire::native::{CallbackPanic, NativeLayoutNode, ScalarKind};

use crate::build::{checked_align, checked_len, checked_size, LayoutBuilder, LogicalBuilder};
use crate::defaults::{DefaultCollector, DefaultWriter, PathStep};
use crate::hasher::DeterministicState;
use crate::types::{AssetRef, EncodeContainer, EncodeSink, WeakAssetRef};
use crate::AssetType;

pub use crate::defaults::DefaultNode;

/// Implemented by generated asset records and the framework's closed set of
/// serializable leaves/containers. The trait is public because proc-macro
/// output in downstream crates calls it; implementations should normally come
/// from `#[asset]`.
///
/// # Safety
///
/// `layout` must describe the exact Rust layout of `Self`, including every
/// child type and callback table entry, and all emitted callbacks must operate
/// on precisely the types and geometry named by that layout. An incorrect
/// implementation can make the loader perform invalid raw-pointer operations.
pub unsafe trait AssetReflect: 'static {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode;
    fn logical(builder: &mut LogicalBuilder);
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic>;
    /// The value in the typed model (`ngp_schema::Value`): struct fields
    /// and enum variants in schema (name-sorted) order. Map and set
    /// entries come in no particular order; writers canonicalize.
    fn to_value(&self) -> Value;

    fn default_writer() -> Option<DefaultWriter> {
        None
    }

    fn collect_default_nodes(_collector: &mut DefaultCollector) {}
}

fn collect_child_defaults<P: AssetReflect, T: AssetReflect>(
    collector: &mut DefaultCollector,
    path: Vec<PathStep>,
) {
    let Some(node) = collector.begin::<P>() else {
        return;
    };
    if let Some(writer) = T::default_writer() {
        collector.add(node, path, writer);
    }
    T::collect_default_nodes(collector);
}

fn collect_map_defaults<P: AssetReflect, K: AssetReflect, V: AssetReflect>(
    collector: &mut DefaultCollector,
) {
    let Some(node) = collector.begin::<P>() else {
        return;
    };
    if let Some(writer) = K::default_writer() {
        collector.add(node, vec![PathStep::MapKey], writer);
    }
    K::collect_default_nodes(collector);
    if let Some(writer) = V::default_writer() {
        collector.add(node, vec![PathStep::MapValue], writer);
    }
    V::collect_default_nodes(collector);
}

fn scalar<T>(offset: u32, kind: ScalarKind) -> NativeLayoutNode {
    NativeLayoutNode::Scalar {
        offset,
        size: checked_size::<T>(),
        align: checked_align::<T>(),
        kind,
    }
}

fn logical_primitive(builder: &mut LogicalBuilder, name: &str) {
    builder.kind(0x01);
    builder.string(name);
}

macro_rules! integer {
    ($ty:ty, $kind:ident, $name:literal) => {
        unsafe impl AssetReflect for $ty {
            fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
                scalar::<Self>(offset, ScalarKind::$kind)
            }
            fn logical(builder: &mut LogicalBuilder) {
                logical_primitive(builder, $name)
            }
            fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
                sink.flat(&self.to_le_bytes())
            }
            fn to_value(&self) -> Value {
                Value::$kind(*self)
            }
            fn default_writer() -> Option<DefaultWriter> {
                Some(crate::defaults::write_default::<Self>)
            }
        }
    };
}

integer!(u8, U8, "u8");
integer!(u16, U16, "u16");
integer!(u32, U32, "u32");
integer!(u64, U64, "u64");
integer!(u128, U128, "u128");
integer!(i8, I8, "i8");
integer!(i16, I16, "i16");
integer!(i32, I32, "i32");
integer!(i64, I64, "i64");
integer!(i128, I128, "i128");

unsafe impl AssetReflect for bool {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::Bool)
    }
    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "bool")
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.flat(&[u8::from(*self)])
    }
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

unsafe impl AssetReflect for char {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::Char)
    }
    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "char")
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.flat(&(*self as u32).to_le_bytes())
    }
    fn to_value(&self) -> Value {
        Value::Char(*self)
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

macro_rules! float {
    ($ty:ty, $kind:ident, $name:literal) => {
        unsafe impl AssetReflect for $ty {
            fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
                scalar::<Self>(offset, ScalarKind::$kind)
            }
            fn logical(builder: &mut LogicalBuilder) {
                logical_primitive(builder, $name)
            }
            fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
                sink.flat(&self.to_bits().to_le_bytes())
            }
            fn to_value(&self) -> Value {
                Value::$kind(*self)
            }
            fn default_writer() -> Option<DefaultWriter> {
                Some(crate::defaults::write_default::<Self>)
            }
        }
    };
}

float!(f64, F64, "f64");

unsafe impl AssetReflect for f32 {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::F32)
    }

    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "f32")
    }

    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.flat(&self.to_bits().to_le_bytes())
    }

    fn to_value(&self) -> Value {
        Value::F32(*self)
    }

    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

unsafe impl AssetReflect for () {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Unit { offset }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0D)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.flat(&[])
    }
    fn to_value(&self) -> Value {
        Value::Unit
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

unsafe impl AssetReflect for String {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Str {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x08)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Str, checked_len(self.len()))?;
        sink.flat(self.as_bytes())?;
        sink.finish()
    }
    fn to_value(&self) -> Value {
        Value::String(self.clone())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }}

unsafe impl AssetReflect for Blob {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Blob {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0B)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.blob(self.as_bytes())
    }
    fn to_value(&self) -> Value {
        Value::Blob(self.as_bytes().to_vec().into())
    }
}

unsafe impl<T: AssetReflect> AssetReflect for Vec<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let elem_node = T::layout(builder, 0);
        let elem = builder.leak_node(elem_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::vec_ctor::<T>());
        NativeLayoutNode::Vec {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            elem,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x04);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Vec, checked_len(self.len()))?;
        for value in self {
            sink.push()?;
            value.encode(sink)?;
        }
        sink.finish()
    }
    fn to_value(&self) -> Value {
        Value::Seq(self.iter().map(AssetReflect::to_value).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
}

unsafe impl<T: AssetReflect, const N: usize> AssetReflect for [T; N] {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let elem_node = T::layout(builder, 0);
        let elem = builder.leak_node(elem_node);
        NativeLayoutNode::Array {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            len: checked_len(N),
            stride: checked_size::<T>(),
            elem,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x05);
        builder.u64(N as u64);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Array, checked_len(N))?;
        for value in self {
            sink.push()?;
            value.encode(sink)?;
        }
        sink.finish()
    }
    fn to_value(&self) -> Value {
        Value::Seq(self.iter().map(AssetReflect::to_value).collect())
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
}

unsafe impl<T: AssetReflect> AssetReflect for Option<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        if let Some(backref) = builder.backref::<Self>(offset) {
            return backref;
        }
        builder.push_frame::<Self>();
        let inner_node = T::layout(builder, 0);
        let inner = builder.leak_node(inner_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::option_ctor::<T>());
        builder.pop_frame::<Self>();
        NativeLayoutNode::Option {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            inner,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x06);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Option, u32::from(self.is_some()))?;
        if let Some(value) = self {
            sink.push()?;
            value.encode(sink)?;
        }
        sink.finish()
    }
    fn to_value(&self) -> Value {
        Value::Option(self.as_ref().map(|v| Box::new(v.to_value())))
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
}

unsafe impl<T: AssetReflect> AssetReflect for Box<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let inner_node = T::layout(builder, 0);
        let inner = builder.leak_node(inner_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::box_ctor::<T>());
        NativeLayoutNode::BoxPtr {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            inner,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        T::logical(builder)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Box, 1)?;
        sink.push()?;
        (**self).encode(sink)?;
        sink.finish()
    }
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, Vec::new());
    }
}

unsafe impl<T: AssetReflect> AssetReflect for Arc<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let inner_node = T::layout(builder, 0);
        let inner = builder.leak_node(inner_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::arc_ctor::<T>());
        NativeLayoutNode::ArcPtr {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            inner,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        T::logical(builder)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.begin(EncodeContainer::Arc, 1)?;
        sink.push()?;
        (**self).encode(sink)?;
        sink.finish()
    }
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, Vec::new());
    }
}

unsafe impl<T> AssetReflect for HashSet<T, DeterministicState>
where
    T: AssetReflect + Eq + Hash,
{
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let elem_node = T::layout(builder, 0);
        let elem = builder.leak_node(elem_node);
        let ctor =
            builder.register_ctor::<Self>(crate::thunks::hash_set_ctor::<T, DeterministicState>());
        NativeLayoutNode::Set {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            elem,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0E);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        encode_set(self.iter(), self.len(), sink)
    }
    fn to_value(&self) -> Value {
        Value::Seq(self.iter().map(AssetReflect::to_value).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
}

unsafe impl<T> AssetReflect for BTreeSet<T>
where
    T: AssetReflect + Ord,
{
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let elem_node = T::layout(builder, 0);
        let elem = builder.leak_node(elem_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::btree_set_ctor::<T>());
        NativeLayoutNode::Set {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            elem,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0E);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        encode_set(self.iter(), self.len(), sink)
    }
    fn to_value(&self) -> Value {
        Value::Seq(self.iter().map(AssetReflect::to_value).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
}

unsafe impl<K, V> AssetReflect for HashMap<K, V, DeterministicState>
where
    K: AssetReflect + Eq + Hash,
    V: AssetReflect,
{
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let key_node = K::layout(builder, 0);
        let key = builder.leak_node(key_node);
        let value_node = V::layout(builder, 0);
        let value = builder.leak_node(value_node);
        let ctor =
            builder
                .register_ctor::<Self>(crate::thunks::hash_map_ctor::<K, V, DeterministicState>());
        NativeLayoutNode::Map {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            key,
            value,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x07);
        K::logical(builder);
        V::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        encode_map(self.iter(), self.len(), sink)
    }
    fn to_value(&self) -> Value {
        Value::Map(self.iter().map(|(k, v)| (k.to_value(), v.to_value())).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_map_defaults::<Self, K, V>(collector);
    }
}

unsafe impl<K, V> AssetReflect for BTreeMap<K, V>
where
    K: AssetReflect + Ord,
    V: AssetReflect,
{
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        let key_node = K::layout(builder, 0);
        let key = builder.leak_node(key_node);
        let value_node = V::layout(builder, 0);
        let value = builder.leak_node(value_node);
        let ctor = builder.register_ctor::<Self>(crate::thunks::btree_map_ctor::<K, V>());
        NativeLayoutNode::Map {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            key,
            value,
            ctor,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x07);
        K::logical(builder);
        V::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        encode_map(self.iter(), self.len(), sink)
    }
    fn to_value(&self) -> Value {
        Value::Map(self.iter().map(|(k, v)| (k.to_value(), v.to_value())).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_map_defaults::<Self, K, V>(collector);
    }
}

unsafe impl<T: AssetType> AssetReflect for AssetRef<T> {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        // UUIDs are 16 unrestricted bytes; a fixed array node preserves
        // their exact native/wire geometry.
        static BYTE: NativeLayoutNode = NativeLayoutNode::Scalar {
            offset: 0,
            size: 1,
            align: 1,
            kind: ScalarKind::U8,
        };
        NativeLayoutNode::Array {
            offset,
            size: 16,
            align: 1,
            len: 16,
            stride: 1,
            elem: &BYTE,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x09);
        builder.bytes(&T::TYPE_UUID.0);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.reference(true, self.uuid(), T::TYPE_UUID)
    }
    fn to_value(&self) -> Value {
        Value::AssetRef(AssetRefValue::Path(self.uuid().to_string()))
    }
}

unsafe impl<T: AssetType> AssetReflect for WeakAssetRef<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        <AssetRef<T> as AssetReflect>::layout(builder, offset)
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0A);
        builder.bytes(&T::TYPE_UUID.0);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        sink.reference(false, self.uuid(), T::TYPE_UUID)
    }
    fn to_value(&self) -> Value {
        Value::WeakRef(AssetRefValue::Path(self.uuid().to_string()))
    }
}

fn encode_set<'a, T: AssetReflect + 'a>(
    values: impl Iterator<Item = &'a T>,
    len: usize,
    sink: &mut dyn EncodeSink,
) -> Result<(), CallbackPanic> {
    let mut values: Vec<_> = values.map(|v| (canonical_bytes(v), v)).collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    sink.begin(EncodeContainer::Set, checked_len(len))?;
    for (_, value) in values {
        sink.push()?;
        value.encode(sink)?;
    }
    sink.finish()
}

fn encode_map<'a, K: AssetReflect + 'a, V: AssetReflect + 'a>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
    len: usize,
    sink: &mut dyn EncodeSink,
) -> Result<(), CallbackPanic> {
    let mut entries: Vec<_> = entries.map(|(k, v)| (canonical_bytes(k), k, v)).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    sink.begin(EncodeContainer::Map, checked_len(len))?;
    for (_, key, value) in entries {
        sink.push()?;
        key.encode(sink)?;
        value.encode(sink)?;
    }
    sink.finish()
}

/// A stable byte spelling of the neutral event stream. It is used only
/// for §5 map/set ordering, never as an artifact encoding.
pub fn canonical_bytes<T: AssetReflect>(value: &T) -> Vec<u8> {
    let mut sink = CanonicalSink(Vec::new());
    value
        .encode(&mut sink)
        .expect("the canonical ordering sink cannot reject an event");
    sink.0
}

struct CanonicalSink(Vec<u8>);

impl CanonicalSink {
    fn frame(&mut self, tag: u8, len: u32) {
        self.0.push(tag);
        self.0.extend_from_slice(&len.to_be_bytes());
    }
}

impl EncodeSink for CanonicalSink {
    fn flat(&mut self, bytes: &[u8]) -> Result<(), CallbackPanic> {
        self.frame(0x01, checked_len(bytes.len()));
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn begin(&mut self, kind: EncodeContainer, len: u32) -> Result<(), CallbackPanic> {
        let (tag, extra) = match kind {
            EncodeContainer::Vec => (0x10, None),
            EncodeContainer::Array => (0x11, None),
            EncodeContainer::Set => (0x12, None),
            EncodeContainer::Map => (0x13, None),
            EncodeContainer::Option => (0x14, None),
            EncodeContainer::Box => (0x15, None),
            EncodeContainer::Arc => (0x16, None),
            EncodeContainer::Str => (0x17, None),
            EncodeContainer::Struct => (0x18, None),
            EncodeContainer::Variant(index) => (0x19, Some(index)),
        };
        self.frame(tag, len);
        if let Some(index) = extra {
            self.0.extend_from_slice(&index.to_be_bytes());
        }
        Ok(())
    }
    fn push(&mut self) -> Result<(), CallbackPanic> {
        self.0.push(0x20);
        Ok(())
    }
    fn finish(&mut self) -> Result<(), CallbackPanic> {
        self.0.push(0x21);
        Ok(())
    }
    fn blob(&mut self, bytes: &[u8]) -> Result<(), CallbackPanic> {
        self.frame(0x30, checked_len(bytes.len()));
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn reference(
        &mut self,
        strong: bool,
        target: distill_core::id::AssetUuid,
        expected_terminal: distill_core::id::TypeUuid,
    ) -> Result<(), CallbackPanic> {
        self.0.push(if strong { 0x40 } else { 0x41 });
        self.0.extend_from_slice(&target.0);
        self.0.extend_from_slice(&expected_terminal.0);
        Ok(())
    }
}

/// Helper for generated recursive implementations.
pub fn type_id<T: 'static>() -> TypeId {
    TypeId::of::<T>()
}

unsafe impl AssetReflect for crate::types::DynamicPatch {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        <String as AssetReflect>::layout(builder, offset)
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.kind(0x0F);
        builder.byte(1);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) -> Result<(), CallbackPanic> {
        self.0.encode(sink)
    }
    /// Unsupported: the text reads only through its bundle's type closure
    /// (see [`crate::types::DynamicPatch`]).
    fn to_value(&self) -> Value {
        panic!("a DynamicPatch has no value without its bundle's type closure; build the value as ngp_schema::Value")
    }
}

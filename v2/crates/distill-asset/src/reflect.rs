//! Runtime reflection used by `#[asset]` expansion. It is intentionally
//! a closed, typed vocabulary: serializable Rust kinds implement this
//! trait; unsupported and nondeterministically seeded containers do not.

use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use distill_json::AuthoredValue;
use distill_wire::exec::Blob;
use distill_wire::native::{NativeLayoutNode, ScalarKind};

use crate::build::{
    checked_align, checked_len, checked_size, LayoutBuilder, LogicalBuilder, RegistryExtrasBuilder,
};
use crate::defaults::{DefaultCollector, DefaultWriter, PathStep};
use crate::hasher::DeterministicState;
use crate::types::{AssetRef, EncodeContainer, EncodeSink, WeakAssetRef};
use crate::AssetType;

pub use crate::defaults::DefaultNode;

/// Implemented by generated asset records and the framework's closed
/// set of serializable leaves/containers. The trait is public because
/// proc-macro output in downstream crates calls it; implementations are
/// an implementation detail and should normally come from `#[asset]`.
pub trait AssetReflect: 'static {
    /// Whether this key type uses JSON's object form for maps. This is a
    /// property of the type, not of the observed entries: an empty non-string
    /// map must still use the pair-array form.
    const STRING_KEY: bool = false;

    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode;
    fn logical(builder: &mut LogicalBuilder);
    fn encode(&self, sink: &mut dyn EncodeSink);
    fn to_authored(&self) -> AuthoredValue;

    /// Add facts not established by DSNL to the typed DSRE projection.
    fn collect_registry_extras(
        _builder: &mut RegistryExtrasBuilder,
        _owner: distill_core::attestation::SchemaNodeId,
        _path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
    }

    fn default_writer() -> Option<DefaultWriter> {
        None
    }

    fn collect_default_nodes(_collector: &mut DefaultCollector) {}

    fn string_key(&self) -> Option<&str> {
        None
    }
}

fn collect_child_defaults<P: 'static, T: AssetReflect>(
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

fn collect_map_defaults<P: 'static, K: AssetReflect, V: AssetReflect>(
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

fn collect_registry_child<T: AssetReflect>(
    builder: &mut RegistryExtrasBuilder,
    owner: distill_core::attestation::SchemaNodeId,
    mut path: Vec<distill_core::attestation::RegistryPathStep>,
    step: Option<distill_core::attestation::RegistryPathStep>,
) {
    if let Some(step) = step {
        path.push(step);
    }
    T::collect_registry_extras(builder, owner, path);
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
    builder.byte(0x01);
    builder.string(name);
}

macro_rules! unsigned {
    ($ty:ty, $kind:ident, $name:literal) => {
        impl AssetReflect for $ty {
            fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
                scalar::<Self>(offset, ScalarKind::$kind)
            }
            fn logical(builder: &mut LogicalBuilder) {
                logical_primitive(builder, $name)
            }
            fn encode(&self, sink: &mut dyn EncodeSink) {
                sink.flat(&self.to_le_bytes())
            }
            fn to_authored(&self) -> AuthoredValue {
                AuthoredValue::UInt(*self as u128)
            }
            fn default_writer() -> Option<DefaultWriter> {
                Some(crate::defaults::write_default::<Self>)
            }
        }
    };
}

macro_rules! signed {
    ($ty:ty, $kind:ident, $name:literal) => {
        impl AssetReflect for $ty {
            fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
                scalar::<Self>(offset, ScalarKind::$kind)
            }
            fn logical(builder: &mut LogicalBuilder) {
                logical_primitive(builder, $name)
            }
            fn encode(&self, sink: &mut dyn EncodeSink) {
                sink.flat(&self.to_le_bytes())
            }
            fn to_authored(&self) -> AuthoredValue {
                if *self < 0 {
                    AuthoredValue::Int(*self as i128)
                } else {
                    AuthoredValue::UInt(*self as u128)
                }
            }
            fn default_writer() -> Option<DefaultWriter> {
                Some(crate::defaults::write_default::<Self>)
            }
        }
    };
}

unsigned!(u8, U8, "u8");
unsigned!(u16, U16, "u16");
unsigned!(u32, U32, "u32");
unsigned!(u64, U64, "u64");
unsigned!(u128, U128, "u128");
signed!(i8, I8, "i8");
signed!(i16, I16, "i16");
signed!(i32, I32, "i32");
signed!(i64, I64, "i64");
signed!(i128, I128, "i128");

impl AssetReflect for bool {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::Bool)
    }
    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "bool")
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.flat(&[u8::from(*self)])
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Bool(*self)
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

impl AssetReflect for char {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::Char)
    }
    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "char")
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.flat(&(*self as u32).to_le_bytes())
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Str(self.to_string())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

macro_rules! float {
    ($ty:ty, $kind:ident, $name:literal) => {
        impl AssetReflect for $ty {
            fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
                scalar::<Self>(offset, ScalarKind::$kind)
            }
            fn logical(builder: &mut LogicalBuilder) {
                logical_primitive(builder, $name)
            }
            fn encode(&self, sink: &mut dyn EncodeSink) {
                sink.flat(&self.to_bits().to_le_bytes())
            }
            fn to_authored(&self) -> AuthoredValue {
                AuthoredValue::Float(*self as f64)
            }
            fn default_writer() -> Option<DefaultWriter> {
                Some(crate::defaults::write_default::<Self>)
            }
        }
    };
}

float!(f64, F64, "f64");

impl AssetReflect for f32 {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        scalar::<Self>(offset, ScalarKind::F32)
    }

    fn logical(builder: &mut LogicalBuilder) {
        logical_primitive(builder, "f32")
    }

    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.flat(&self.to_bits().to_le_bytes())
    }

    fn to_authored(&self) -> AuthoredValue {
        if !self.is_finite() {
            return AuthoredValue::Float(f64::from(*self));
        }
        let canonical = distill_json::write_f32(*self)
            .expect("finite binary32 values always have a canonical decimal");
        let semantic = canonical
            .parse::<f64>()
            .expect("the canonical binary32 decimal is also a binary64 token");
        AuthoredValue::Float(semantic)
    }

    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

impl AssetReflect for () {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Unit { offset }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.byte(0x0D)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.flat(&[])
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Null
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
}

impl AssetReflect for String {
    const STRING_KEY: bool = true;

    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Str {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.byte(0x08)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Str, checked_len(self.len()));
        sink.flat(self.as_bytes());
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Str(self.clone())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn string_key(&self) -> Option<&str> {
        Some(self)
    }
}

impl AssetReflect for Blob {
    fn layout(_: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        NativeLayoutNode::Blob {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.byte(0x0B)
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.blob(self.as_bytes())
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Blob(self.as_bytes().to_vec())
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        builder.fact(
            owner,
            path,
            distill_core::attestation::RegistryExtraFact::Blob,
        );
    }
}

impl<T: AssetReflect> AssetReflect for Vec<T> {
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
        builder.byte(0x04);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Vec, checked_len(self.len()));
        for value in self {
            sink.push();
            value.encode(sink);
        }
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Array(self.iter().map(AssetReflect::to_authored).collect())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::Elem),
        );
    }
}

impl<T: AssetReflect, const N: usize> AssetReflect for [T; N] {
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
        builder.byte(0x05);
        builder.u64(N as u64);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Array, checked_len(N));
        for value in self {
            sink.push();
            value.encode(sink);
        }
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Array(self.iter().map(AssetReflect::to_authored).collect())
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::Elem),
        );
    }
}

impl<T: AssetReflect> AssetReflect for Option<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        // Option has no general stable constructor/layout rule independent
        // of its niche. Asset records should use it normally; source-walk's
        // measured enum layout is authoritative. Here we conservatively
        // describe the actual slot as an enum-shaped scalar-free record.
        let none = builder.leak_node(NativeLayoutNode::Unit { offset: 0 });
        let some_node = T::layout(builder, 0);
        let some = builder.leak_node(some_node);
        let variants = builder.leak_variants(vec![
            distill_wire::native::NativeVariant {
                name: "None",
                declaration_index: 0,
                node: none,
                tag: distill_wire::native::NativeVariantTag::Niche { index: 0 },
            },
            distill_wire::native::NativeVariant {
                name: "Some",
                declaration_index: 1,
                node: some,
                tag: distill_wire::native::NativeVariantTag::Untagged,
            },
        ]);
        NativeLayoutNode::Enum {
            offset,
            size: checked_size::<Self>(),
            align: checked_align::<Self>(),
            tag: distill_wire::native::NativeTagEncoding::Niche {
                offset: 0,
                size: checked_size::<Self>().min(16) as u8,
                niche_start: 0,
            },
            whole_drop: builder.register_drop_if_needed::<Self>(),
            variants,
        }
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.byte(0x06);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Option, u32::from(self.is_some()));
        if let Some(value) = self {
            sink.push();
            value.encode(sink);
        }
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        self.as_ref()
            .map_or(AuthoredValue::Null, AssetReflect::to_authored)
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::Elem),
        );
    }
}

impl<T: AssetReflect> AssetReflect for Box<T> {
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
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Box, 1);
        sink.push();
        (**self).encode(sink);
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        (**self).to_authored()
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, Vec::new());
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(builder, owner, path, None);
    }
}

impl<T: AssetReflect> AssetReflect for Arc<T> {
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
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.begin(EncodeContainer::Arc, 1);
        sink.push();
        (**self).encode(sink);
        sink.finish();
    }
    fn to_authored(&self) -> AuthoredValue {
        (**self).to_authored()
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, Vec::new());
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(builder, owner, path, None);
    }
}

impl<T> AssetReflect for HashSet<T, DeterministicState>
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
        builder.byte(0x0E);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        encode_set(self.iter(), self.len(), sink)
    }
    fn to_authored(&self) -> AuthoredValue {
        authored_set(self.iter())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::Elem),
        );
    }
}

impl<T> AssetReflect for BTreeSet<T>
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
        builder.byte(0x0E);
        T::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        encode_set(self.iter(), self.len(), sink)
    }
    fn to_authored(&self) -> AuthoredValue {
        authored_set(self.iter())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_child_defaults::<Self, T>(collector, vec![PathStep::Elem]);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<T>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::Elem),
        );
    }
}

impl<K, V> AssetReflect for HashMap<K, V, DeterministicState>
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
        builder.byte(0x07);
        K::logical(builder);
        V::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        encode_map(self.iter(), self.len(), sink)
    }
    fn to_authored(&self) -> AuthoredValue {
        authored_map(self.iter())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_map_defaults::<Self, K, V>(collector);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<K>(
            builder,
            owner,
            path.clone(),
            Some(distill_core::attestation::RegistryPathStep::MapKey),
        );
        collect_registry_child::<V>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::MapValue),
        );
    }
}

impl<K, V> AssetReflect for BTreeMap<K, V>
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
        builder.byte(0x07);
        K::logical(builder);
        V::logical(builder);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        encode_map(self.iter(), self.len(), sink)
    }
    fn to_authored(&self) -> AuthoredValue {
        authored_map(self.iter())
    }
    fn default_writer() -> Option<DefaultWriter> {
        Some(crate::defaults::write_default::<Self>)
    }
    fn collect_default_nodes(collector: &mut DefaultCollector) {
        collect_map_defaults::<Self, K, V>(collector);
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        collect_registry_child::<K>(
            builder,
            owner,
            path.clone(),
            Some(distill_core::attestation::RegistryPathStep::MapKey),
        );
        collect_registry_child::<V>(
            builder,
            owner,
            path,
            Some(distill_core::attestation::RegistryPathStep::MapValue),
        );
    }
}

impl<T: AssetType> AssetReflect for AssetRef<T> {
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
        builder.byte(0x09);
        builder.bytes(&T::TYPE_UUID.0);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.reference(true, self.uuid(), T::TYPE_UUID)
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Str(self.uuid().to_string())
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        builder.fact(
            owner,
            path,
            distill_core::attestation::RegistryExtraFact::Reference {
                strength: distill_core::attestation::ReferenceStrength::Strong,
                target: T::TYPE_UUID,
            },
        );
    }
}

impl<T: AssetType> AssetReflect for WeakAssetRef<T> {
    fn layout(builder: &mut LayoutBuilder, offset: u32) -> NativeLayoutNode {
        <AssetRef<T> as AssetReflect>::layout(builder, offset)
    }
    fn logical(builder: &mut LogicalBuilder) {
        builder.byte(0x0A);
        builder.bytes(&T::TYPE_UUID.0);
    }
    fn encode(&self, sink: &mut dyn EncodeSink) {
        sink.reference(false, self.uuid(), T::TYPE_UUID)
    }
    fn to_authored(&self) -> AuthoredValue {
        AuthoredValue::Str(self.uuid().to_string())
    }
    fn collect_registry_extras(
        builder: &mut RegistryExtrasBuilder,
        owner: distill_core::attestation::SchemaNodeId,
        path: Vec<distill_core::attestation::RegistryPathStep>,
    ) {
        builder.fact(
            owner,
            path,
            distill_core::attestation::RegistryExtraFact::Reference {
                strength: distill_core::attestation::ReferenceStrength::Weak,
                target: T::TYPE_UUID,
            },
        );
    }
}

fn encode_set<'a, T: AssetReflect + 'a>(
    values: impl Iterator<Item = &'a T>,
    len: usize,
    sink: &mut dyn EncodeSink,
) {
    let mut values: Vec<_> = values.map(|v| (canonical_bytes(v), v)).collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    sink.begin(EncodeContainer::Set, checked_len(len));
    for (_, value) in values {
        sink.push();
        value.encode(sink);
    }
    sink.finish();
}

fn authored_set<'a, T: AssetReflect + 'a>(values: impl Iterator<Item = &'a T>) -> AuthoredValue {
    let mut values: Vec<_> = values.map(|v| (canonical_bytes(v), v)).collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    AuthoredValue::Array(values.into_iter().map(|(_, v)| v.to_authored()).collect())
}

fn encode_map<'a, K: AssetReflect + 'a, V: AssetReflect + 'a>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
    len: usize,
    sink: &mut dyn EncodeSink,
) {
    let mut entries: Vec<_> = entries.map(|(k, v)| (canonical_bytes(k), k, v)).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    sink.begin(EncodeContainer::Map, checked_len(len));
    for (_, key, value) in entries {
        sink.push();
        key.encode(sink);
        value.encode(sink);
    }
    sink.finish();
}

fn authored_map<'a, K: AssetReflect + 'a, V: AssetReflect + 'a>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
) -> AuthoredValue {
    let mut entries: Vec<_> = entries.map(|(k, v)| (canonical_bytes(k), k, v)).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    if K::STRING_KEY {
        let mut object = BTreeMap::new();
        for (_, key, value) in entries {
            object.insert(
                key.string_key().expect("checked").to_owned(),
                value.to_authored(),
            );
        }
        AuthoredValue::Object(object)
    } else {
        AuthoredValue::Array(
            entries
                .into_iter()
                .map(|(_, key, value)| {
                    AuthoredValue::Array(vec![key.to_authored(), value.to_authored()])
                })
                .collect(),
        )
    }
}

/// A stable byte spelling of the neutral event stream. It is used only
/// for §5 map/set ordering, never as an artifact encoding.
pub fn canonical_bytes<T: AssetReflect>(value: &T) -> Vec<u8> {
    let mut sink = CanonicalSink(Vec::new());
    value.encode(&mut sink);
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
    fn flat(&mut self, bytes: &[u8]) {
        self.frame(0x01, checked_len(bytes.len()));
        self.0.extend_from_slice(bytes);
    }
    fn begin(&mut self, kind: EncodeContainer, len: u32) {
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
    }
    fn push(&mut self) {
        self.0.push(0x20)
    }
    fn finish(&mut self) {
        self.0.push(0x21)
    }
    fn blob(&mut self, bytes: &[u8]) {
        self.frame(0x30, checked_len(bytes.len()));
        self.0.extend_from_slice(bytes);
    }
    fn reference(
        &mut self,
        strong: bool,
        target: distill_core::id::AssetUuid,
        expected_terminal: distill_core::id::TypeUuid,
    ) {
        self.0.push(if strong { 0x40 } else { 0x41 });
        self.0.extend_from_slice(&target.0);
        self.0.extend_from_slice(&expected_terminal.0);
    }
}

/// Helper for generated recursive implementations.
pub fn type_id<T: 'static>() -> TypeId {
    TypeId::of::<T>()
}

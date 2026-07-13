//! Deterministic descriptor construction shared by all `#[asset]`
//! expansions. One walk creates the native tree and every table ID;
//! `DSNL` hashes the measured tree while `DSFT` additionally commits to
//! the binary-local table assignment.

use std::any::TypeId;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use distill_core::id::{LogicalHash, TypeUuid};
use distill_wire::dsnl::dsnl_hash;
use distill_wire::native::{
    CallbackPanic, CtorEntry, CtorId, CtorTable, DropId, DropTable, NativeField, NativeLayoutNode,
    NativeVariant, SkipDefaultId, SkipEntry, SkipWriterTable,
};
use unicode_normalization::UnicodeNormalization;

use crate::{AssetReflect, AssetRuntimeDescriptor, AssetType, EncodeSink, EpochToken, ErasedValue};

#[derive(Debug, Clone, Copy)]
pub struct AssetMetadata {
    pub type_uuid: TypeUuid,
    pub build_only: bool,
}

enum AssignmentKind {
    Ctor,
    Drop,
    Skip,
}

struct Assignment {
    kind: AssignmentKind,
    id: u32,
    nominal: &'static str,
}

type DropThunk = unsafe fn(*mut u8) -> Result<(), CallbackPanic>;

#[derive(Default)]
pub struct LayoutBuilder {
    ctors: Vec<CtorEntry>,
    drops: Vec<DropThunk>,
    skips: Vec<SkipEntry>,
    ctor_ids: HashMap<TypeId, CtorId>,
    drop_ids: HashMap<TypeId, DropId>,
    assignments: Vec<Assignment>,
    frames: Vec<TypeId>,
}

impl LayoutBuilder {
    pub fn backref<T: 'static>(&self, offset: u32) -> Option<NativeLayoutNode> {
        let id = TypeId::of::<T>();
        self.frames
            .iter()
            .rposition(|candidate| *candidate == id)
            .map(|index| NativeLayoutNode::BackRef {
                distance: (self.frames.len() - 1 - index) as u32,
                offset,
            })
    }

    pub fn push_frame<T: 'static>(&mut self) {
        self.frames.push(TypeId::of::<T>());
    }

    pub fn pop_frame<T: 'static>(&mut self) {
        let popped = self.frames.pop();
        debug_assert_eq!(popped, Some(TypeId::of::<T>()));
    }

    pub fn register_ctor<T: 'static>(&mut self, entry: CtorEntry) -> CtorId {
        let type_id = TypeId::of::<T>();
        if let Some(id) = self.ctor_ids.get(&type_id) {
            return *id;
        }
        let id = CtorId(checked_len(self.ctors.len()));
        self.ctors.push(entry);
        self.ctor_ids.insert(type_id, id);
        self.assignments.push(Assignment {
            kind: AssignmentKind::Ctor,
            id: id.0,
            nominal: std::any::type_name::<T>(),
        });
        id
    }

    pub fn register_drop_if_needed<T: 'static>(&mut self) -> Option<DropId> {
        if !std::mem::needs_drop::<T>() {
            return None;
        }
        let type_id = TypeId::of::<T>();
        if let Some(id) = self.drop_ids.get(&type_id) {
            return Some(*id);
        }
        let id = DropId(checked_len(self.drops.len()));
        self.drops.push(crate::thunks::drop_in_place_thunk::<T>);
        self.drop_ids.insert(type_id, id);
        self.assignments.push(Assignment {
            kind: AssignmentKind::Drop,
            id: id.0,
            nominal: std::any::type_name::<T>(),
        });
        Some(id)
    }

    pub fn register_skip<T: Default + 'static>(&mut self) -> SkipDefaultId {
        let id = SkipDefaultId(checked_len(self.skips.len()));
        self.skips.push(crate::thunks::skip_entry::<T>());
        self.assignments.push(Assignment {
            kind: AssignmentKind::Skip,
            id: id.0,
            nominal: std::any::type_name::<T>(),
        });
        id
    }

    pub fn leak_node(&mut self, node: NativeLayoutNode) -> &'static NativeLayoutNode {
        Box::leak(Box::new(node))
    }

    pub fn leak_fields(&mut self, mut fields: Vec<NativeField>) -> &'static [NativeField] {
        fields.sort_by_key(|field| (field.node.offset(), field.declaration_index));
        Box::leak(fields.into_boxed_slice())
    }

    pub fn leak_variants(&mut self, variants: Vec<NativeVariant>) -> &'static [NativeVariant] {
        Box::leak(variants.into_boxed_slice())
    }

    fn finish(self, root: NativeLayoutNode) -> BuiltLayout {
        let root = Box::leak(Box::new(root));
        let layout_digest = dsnl_hash(root).expect("#[asset] generated a valid DSNL tree");
        let fixup_identity = fixup_identity(layout_digest, &self.assignments);
        BuiltLayout {
            root,
            layout_digest,
            fixup_identity,
            ctors: Box::leak(Box::new(CtorTable {
                entries: Box::leak(self.ctors.into_boxed_slice()),
            })),
            drops: Box::leak(Box::new(DropTable {
                entries: Box::leak(self.drops.into_boxed_slice()),
            })),
            skips: Box::leak(Box::new(SkipWriterTable {
                entries: Box::leak(self.skips.into_boxed_slice()),
            })),
        }
    }
}

struct BuiltLayout {
    root: &'static NativeLayoutNode,
    layout_digest: [u8; 32],
    fixup_identity: [u8; 32],
    ctors: &'static CtorTable,
    drops: &'static DropTable,
    skips: &'static SkipWriterTable,
}

fn fixup_identity(layout: [u8; 32], assignments: &[Assignment]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"DSFT");
    hash.update(&[1]);
    hash.update(&layout);
    hash.update(&(assignments.len() as u32).to_le_bytes());
    for assignment in assignments {
        hash.update(&[match assignment.kind {
            AssignmentKind::Ctor => 0,
            AssignmentKind::Drop => 1,
            AssignmentKind::Skip => 2,
        }]);
        hash.update(&assignment.id.to_le_bytes());
        let name = assignment.nominal.nfc().collect::<String>();
        hash.update(&(name.len() as u32).to_le_bytes());
        hash.update(name.as_bytes());
    }
    *hash.finalize().as_bytes()
}

#[derive(Default)]
pub struct LogicalBuilder {
    bytes: Vec<u8>,
    frames: Vec<TypeId>,
}

impl LogicalBuilder {
    pub fn byte(&mut self, value: u8) {
        self.bytes.push(value)
    }
    pub fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value)
    }
    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes())
    }
    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes())
    }
    pub fn string(&mut self, value: &str) {
        let value = value.nfc().collect::<String>();
        self.u32(checked_len(value.len()));
        self.bytes(value.as_bytes());
    }
    pub fn enter<T: 'static>(&mut self) -> Option<u32> {
        let id = TypeId::of::<T>();
        if let Some(index) = self.frames.iter().rposition(|candidate| *candidate == id) {
            Some((self.frames.len() - 1 - index) as u32)
        } else {
            self.frames.push(id);
            None
        }
    }
    pub fn exit<T: 'static>(&mut self) {
        let popped = self.frames.pop();
        debug_assert_eq!(popped, Some(TypeId::of::<T>()));
    }
    pub fn backref(&mut self, distance: u32) {
        self.byte(0x0C);
        self.u32(distance);
    }
}

pub fn logical_hash<T: AssetReflect>() -> LogicalHash {
    let mut builder = LogicalBuilder::default();
    T::logical(&mut builder);
    let mut hash = blake3::Hasher::new();
    hash.update(b"DSLH");
    hash.update(&[1]);
    hash.update(&builder.bytes);
    LogicalHash(*hash.finalize().as_bytes())
}

pub fn build_descriptor<T>(metadata: AssetMetadata) -> AssetRuntimeDescriptor
where
    T: AssetType + AssetReflect,
{
    assert_eq!(
        metadata.type_uuid,
        T::TYPE_UUID,
        "macro metadata UUID drift"
    );
    let mut builder = LayoutBuilder::default();
    let root = T::layout(&mut builder, 0);
    let built = builder.finish(root);
    AssetRuntimeDescriptor {
        type_uuid: metadata.type_uuid,
        layout_digest: built.layout_digest,
        fixup_identity: built.fixup_identity,
        logical_hash: logical_hash::<T>(),
        build_only: metadata.build_only,
        native_layout: built.root,
        size: std::mem::size_of::<T>(),
        align: std::mem::align_of::<T>(),
        ctors: built.ctors,
        drops: built.drops,
        skip_writers: built.skips,
        finalize: finalize::<T>,
        encode: encode::<T>,
    }
}

unsafe fn finalize<T: AssetType>(
    src: *mut u8,
    owner: EpochToken,
) -> Result<ErasedValue, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| {
        let value = std::ptr::read(src.cast::<T>());
        ErasedValue::new_in(value, owner)
    }))
    .map_err(|_| CallbackPanic)
}

unsafe fn encode<T: AssetReflect>(
    ptr: *const u8,
    sink: &mut dyn EncodeSink,
) -> Result<(), CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| (&*ptr.cast::<T>()).encode(sink))).map_err(|_| CallbackPanic)
}

pub fn checked_size<T>() -> u32 {
    u32::try_from(std::mem::size_of::<T>()).expect("asset native size exceeds u32")
}

pub fn checked_align<T>() -> u32 {
    u32::try_from(std::mem::align_of::<T>()).expect("asset native alignment exceeds u32")
}

pub fn checked_len(value: usize) -> u32 {
    u32::try_from(value).expect("asset count exceeds u32")
}

pub fn align_up(value: u32, align: u32) -> u32 {
    let align = align.max(1);
    let remainder = value % align;
    if remainder == 0 {
        value
    } else {
        value
            .checked_add(align - remainder)
            .expect("asset native layout exceeds u32")
    }
}

pub fn checked_add(left: u32, right: u32) -> u32 {
    left.checked_add(right)
        .expect("asset native layout exceeds u32")
}

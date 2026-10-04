//! Schema-directed `AuthoredValue` to canonical wire sections (§12).

use distill_bundle::{encode_path, PathComponent};
use distill_core::frames::{reenter, resolve_backref};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_json::AuthoredValue;
use ngp_schema::node::{PrimitiveKind, SchemaNode};
use unicode_normalization::{is_nfc, UnicodeNormalization};

use crate::wire::{SlotKind, WireEnumForm, WireNode, WireVariant};

const MAX_ENCODE_DEPTH: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedReference {
    pub strong: bool,
    pub asset: AssetUuid,
    pub expected_terminal: TypeUuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedBlob {
    pub path: Vec<PathComponent>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWireValue {
    pub fixed: Vec<u8>,
    pub variable: Vec<u8>,
    /// Canonical structural-path order. BlobRef indices in `fixed` and
    /// `variable` already name positions in this vector.
    pub blobs: Vec<EncodedBlob>,
    pub references: Vec<EncodedReference>,
}

pub trait AuthoredReferenceResolver {
    fn resolve(
        &mut self,
        query: &AuthoredValue,
        expected_terminal: TypeUuid,
        strong: bool,
        path: &[PathComponent],
    ) -> Result<AssetUuid, String>;
}

impl<F> AuthoredReferenceResolver for F
where
    F: FnMut(&AuthoredValue, TypeUuid, bool, &[PathComponent]) -> Result<AssetUuid, String>,
{
    fn resolve(
        &mut self,
        query: &AuthoredValue,
        expected_terminal: TypeUuid,
        strong: bool,
        path: &[PathComponent],
    ) -> Result<AssetUuid, String> {
        self(query, expected_terminal, strong, path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    Shape {
        path: String,
        expected: &'static str,
    },
    MissingWireField {
        path: String,
        field: String,
    },
    MissingWireVariant {
        path: String,
        variant: String,
    },
    InvalidWireShape {
        path: String,
        detail: &'static str,
    },
    InvalidScalar {
        path: String,
        kind: PrimitiveKind,
    },
    InvalidReference {
        path: String,
        detail: String,
    },
    KeyNotEncodable {
        path: String,
    },
    DuplicateOrderedValue {
        path: String,
    },
    DuplicateBlobPath {
        path: String,
    },
    Bounds {
        path: String,
        what: &'static str,
    },
    BackRefMismatch {
        path: String,
    },
    DepthExceeded,
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "wire encoding failed: {self:?}")
    }
}

impl std::error::Error for EncodeError {}

/// Encode one schema-conforming value. The caller supplies the exact DSWL
/// tree selected for the target and a snapshot-bound reference resolver.
pub fn encode_authored_value(
    schema: &SchemaNode,
    wire: &WireNode,
    value: &AuthoredValue,
    references: &mut impl AuthoredReferenceResolver,
) -> Result<EncodedWireValue, EncodeError> {
    if wire.offset() != 0 {
        return Err(EncodeError::InvalidWireShape {
            path: "data".to_owned(),
            detail: "root wire offset is not zero",
        });
    }
    let (size, _) = geometry(wire).ok_or_else(|| EncodeError::InvalidWireShape {
        path: "data".to_owned(),
        detail: "root wire tree is an unresolved back-reference",
    })?;
    let fixed_len = usize::try_from(size).map_err(|_| EncodeError::Bounds {
        path: "data".to_owned(),
        what: "fixed section length",
    })?;
    let mut encoder = Encoder {
        fixed: vec![0; fixed_len],
        variable: Vec::new(),
        blobs: Vec::new(),
        references: Vec::new(),
        resolver: references,
        path: Vec::new(),
        logical_frames: Vec::new(),
        wire_frames: Vec::new(),
        depth: 0,
    };
    encoder.node(
        schema,
        wire,
        value,
        Location::fixed(0),
        None,
        FrameMode::Both,
    )?;
    encoder.finish()
}

#[derive(Debug, Clone, Copy)]
enum Section {
    Fixed,
    Variable,
}

#[derive(Debug, Clone, Copy)]
enum FrameMode {
    /// An ordinary logical record/enum and its matching wire frame.
    Both,
    /// A physical variant-payload record. Logical projection folds this
    /// record into its enum frame, while DSWL counts the record separately.
    WireOnly,
}

#[derive(Debug, Clone, Copy)]
struct Location {
    section: Section,
    base: u32,
}

impl Location {
    fn fixed(base: u32) -> Self {
        Self {
            section: Section::Fixed,
            base,
        }
    }

    fn variable(base: u32) -> Self {
        Self {
            section: Section::Variable,
            base,
        }
    }

    fn shifted(self, amount: u32) -> Result<Self, EncodeError> {
        Ok(Self {
            section: self.section,
            base: self.base.checked_add(amount).ok_or(EncodeError::Bounds {
                path: "data".to_owned(),
                what: "wire location",
            })?,
        })
    }
}

struct PendingBlob {
    path: Vec<PathComponent>,
    bytes: Vec<u8>,
    slot: Location,
}

struct Encoder<'a, R: AuthoredReferenceResolver + ?Sized> {
    fixed: Vec<u8>,
    variable: Vec<u8>,
    blobs: Vec<PendingBlob>,
    references: Vec<EncodedReference>,
    resolver: &'a mut R,
    path: Vec<PathComponent>,
    logical_frames: Vec<&'a SchemaNode>,
    wire_frames: Vec<&'a WireNode>,
    depth: usize,
}

impl<'a, R: AuthoredReferenceResolver + ?Sized> Encoder<'a, R> {
    fn finish(mut self) -> Result<EncodedWireValue, EncodeError> {
        self.blobs
            .sort_by(|left, right| encode_path(&left.path).cmp(&encode_path(&right.path)));
        for pair in self.blobs.windows(2) {
            if pair[0].path == pair[1].path {
                return Err(EncodeError::DuplicateBlobPath {
                    path: display_path(&pair[0].path),
                });
            }
        }
        for index in 0..self.blobs.len() {
            let index = u32::try_from(index).map_err(|_| EncodeError::Bounds {
                path: "data".to_owned(),
                what: "blob count",
            })?;
            let slot = self.blobs[index as usize].slot;
            self.write(slot, &index.to_le_bytes())?;
        }
        let blobs = self
            .blobs
            .into_iter()
            .map(|blob| EncodedBlob {
                path: blob.path,
                bytes: blob.bytes,
            })
            .collect();
        Ok(EncodedWireValue {
            fixed: self.fixed,
            variable: self.variable,
            blobs,
            references: self.references,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn node(
        &mut self,
        schema: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        placement: Option<u32>,
        frame_mode: FrameMode,
    ) -> Result<(), EncodeError> {
        if self.depth >= MAX_ENCODE_DEPTH {
            return Err(EncodeError::DepthExceeded);
        }
        self.depth += 1;
        let result = self.node_inner(schema, wire, value, location, placement, frame_mode);
        self.depth -= 1;
        result
    }

    fn node_inner(
        &mut self,
        schema: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        placement: Option<u32>,
        frame_mode: FrameMode,
    ) -> Result<(), EncodeError> {
        // Box and Arc are transparent in the logical schema, but remain
        // explicit indirection slots in DSWL. Peel exactly one physical
        // wrapper while preserving the logical node and expansion frames.
        if let WireNode::Slot {
            size,
            kind: kind @ (SlotKind::Box | SlotKind::Arc),
            pointee,
            ..
        } = wire
        {
            if *size < 8 || pointee.len() != 1 {
                return self.invalid_wire("Box/Arc slot has invalid geometry or pointee count");
            }
            let offset = placement.unwrap_or_else(|| wire.offset());
            let (pointee_size, pointee_align) = self.geometry(&pointee[0])?;
            let variable = self.reserve_variable(pointee_size as usize, pointee_align)?;
            self.node(
                schema,
                &pointee[0],
                value,
                Location::variable(variable),
                Some(0),
                frame_mode,
            )?;
            // Box/Arc lengths are byte sizes, including zero for a ZST.
            self.write_var_ref(location.shifted(offset)?, variable, pointee_size as usize)?;
            debug_assert!(matches!(kind, SlotKind::Box | SlotKind::Arc));
            return Ok(());
        }

        if let (SchemaNode::BackRef(schema_distance), WireNode::BackRef { distance, offset }) =
            (schema, wire)
        {
            // Both trees re-enter their target under its own ancestors.
            let Some((target_schema, logical)) =
                reenter(&mut self.logical_frames, *schema_distance)
            else {
                return Err(EncodeError::BackRefMismatch {
                    path: self.path_string(),
                });
            };
            let Some((target_wire, physical)) = reenter(&mut self.wire_frames, *distance) else {
                logical.restore(&mut self.logical_frames);
                return Err(EncodeError::BackRefMismatch {
                    path: self.path_string(),
                });
            };
            let encoded = self.node(
                target_schema,
                target_wire,
                value,
                location,
                Some(placement.unwrap_or(*offset)),
                FrameMode::Both,
            );
            physical.restore(&mut self.wire_frames);
            logical.restore(&mut self.logical_frames);
            return encoded;
        }
        if matches!(schema, SchemaNode::BackRef(_)) || matches!(wire, WireNode::BackRef { .. }) {
            return Err(EncodeError::BackRefMismatch {
                path: self.path_string(),
            });
        }

        let offset = placement.unwrap_or_else(|| wire.offset());
        match schema {
            SchemaNode::Primitive(kind) => self.primitive(*kind, wire, value, location, offset),
            SchemaNode::String => self.slot_string(wire, value, location, offset),
            SchemaNode::Unit => {
                if !matches!(value, AuthoredValue::Null) || !matches!(wire, WireNode::Unit { .. }) {
                    return self.shape("unit/null");
                }
                Ok(())
            }
            SchemaNode::AssetRef(expected) => {
                self.reference(wire, value, location, offset, *expected, true)
            }
            SchemaNode::WeakRef(expected) => {
                self.reference(wire, value, location, offset, *expected, false)
            }
            SchemaNode::Blob => self.blob(wire, value, location, offset),
            SchemaNode::Option(inner) => self.option(inner, wire, value, location, offset),
            SchemaNode::Vec(element) => {
                self.sequence(element, wire, value, location, offset, SlotKind::Vec)
            }
            SchemaNode::Set(element) => {
                self.sequence(element, wire, value, location, offset, SlotKind::Set)
            }
            SchemaNode::Array { len, elem } => {
                self.array(*len, elem, wire, value, location, offset)
            }
            SchemaNode::Map { key, value: item } => {
                self.map(key, item, wire, value, location, offset)
            }
            SchemaNode::Struct { fields, .. } => {
                self.record(schema, fields, wire, value, location, offset, frame_mode)
            }
            SchemaNode::Enum { variants, .. } => {
                self.enumeration(schema, variants, wire, value, location, offset, frame_mode)
            }
            SchemaNode::BackRef(_) => unreachable!("handled above"),
        }
    }

    fn primitive(
        &mut self,
        kind: PrimitiveKind,
        wire: &WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let WireNode::Primitive {
            size,
            kind: wire_kind,
            ..
        } = wire
        else {
            return self.invalid_wire("logical primitive is not a wire primitive");
        };
        if wire_kind.grammar_id() != primitive_grammar_id(kind) {
            return self.invalid_wire("logical/wire primitive kinds differ");
        }
        let bytes = scalar_bytes(kind, value).ok_or_else(|| EncodeError::InvalidScalar {
            path: self.path_string(),
            kind,
        })?;
        if bytes.len() != *size as usize {
            return self.invalid_wire("primitive size differs from its scalar width");
        }
        self.write(location.shifted(offset)?, &bytes)
    }

    fn reference(
        &mut self,
        wire: &WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
        expected_terminal: TypeUuid,
        strong: bool,
    ) -> Result<(), EncodeError> {
        let (size, _) = geometry(wire).ok_or_else(|| EncodeError::InvalidWireShape {
            path: self.path_string(),
            detail: "reference wire shape is unresolved",
        })?;
        if size != 16 {
            return self.invalid_wire("reference wire size is not 16 bytes");
        }
        let asset = self
            .resolver
            .resolve(value, expected_terminal, strong, &self.path)
            .map_err(|detail| EncodeError::InvalidReference {
                path: self.path_string(),
                detail,
            })?;
        self.write(location.shifted(offset)?, &asset.0)?;
        self.references.push(EncodedReference {
            strong,
            asset,
            expected_terminal,
        });
        Ok(())
    }

    fn slot_string(
        &mut self,
        wire: &WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Str(value) = value else {
            return self.shape("string");
        };
        let (size, _, kind, pointee) =
            slot_parts(wire).ok_or_else(|| EncodeError::InvalidWireShape {
                path: self.path_string(),
                detail: "string is not a wire slot",
            })?;
        if size < 8 || kind != SlotKind::String || !pointee.is_empty() {
            return self.invalid_wire("string slot has the wrong kind/pointee shape");
        }
        let variable = self.reserve_variable(value.len(), 1)?;
        self.write(Location::variable(variable), value.as_bytes())?;
        self.write_var_ref(location.shifted(offset)?, variable, value.len())
    }

    fn blob(
        &mut self,
        wire: &WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Blob(bytes) = value else {
            return self.shape("blob bytes");
        };
        let (size, _, kind, pointee) =
            slot_parts(wire).ok_or_else(|| EncodeError::InvalidWireShape {
                path: self.path_string(),
                detail: "blob is not a wire slot",
            })?;
        if kind != SlotKind::Blob || size < 8 || !pointee.is_empty() {
            return self.invalid_wire("blob slot has the wrong kind/geometry");
        }
        let slot = location.shifted(offset)?;
        self.write(slot, &[0; 8])?;
        self.blobs.push(PendingBlob {
            path: self.path.clone(),
            bytes: bytes.clone(),
            slot,
        });
        Ok(())
    }

    fn sequence(
        &mut self,
        element_schema: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
        expected_kind: SlotKind,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Array(items) = value else {
            return self.shape("array");
        };
        let (size, _, kind, pointee) =
            slot_parts(wire).ok_or_else(|| EncodeError::InvalidWireShape {
                path: self.path_string(),
                detail: "sequence is not a wire slot",
            })?;
        if size < 8 || kind != expected_kind || pointee.len() != 1 {
            return self.invalid_wire("sequence slot has the wrong kind/pointee count");
        }
        let mut ordered = items.iter().collect::<Vec<_>>();
        if expected_kind == SlotKind::Set {
            let mut keyed = ordered
                .into_iter()
                .map(|item| {
                    canonical_value_bytes(item)
                        .map(|bytes| (bytes, item))
                        .ok_or_else(|| EncodeError::KeyNotEncodable {
                            path: self.path_string(),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            for pair in keyed.windows(2) {
                if pair[0].0 == pair[1].0 {
                    return Err(EncodeError::DuplicateOrderedValue {
                        path: self.path_string(),
                    });
                }
            }
            ordered = keyed.into_iter().map(|(_, item)| item).collect();
        }
        let (element_size, element_align) = self.geometry(&pointee[0])?;
        let stride = align_up(element_size, element_align).ok_or_else(|| EncodeError::Bounds {
            path: self.path_string(),
            what: "sequence stride",
        })?;
        let total = (stride as usize)
            .checked_mul(ordered.len())
            .ok_or_else(|| EncodeError::Bounds {
                path: self.path_string(),
                what: "sequence byte length",
            })?;
        let variable = self.reserve_variable(total, element_align)?;
        for (index, item) in ordered.into_iter().enumerate() {
            self.path.push(PathComponent::Index(index as u64));
            let base = variable
                .checked_add((index as u32).checked_mul(stride).ok_or_else(|| {
                    EncodeError::Bounds {
                        path: self.path_string(),
                        what: "sequence element offset",
                    }
                })?)
                .ok_or_else(|| EncodeError::Bounds {
                    path: self.path_string(),
                    what: "sequence element offset",
                })?;
            let result = self.node(
                element_schema,
                &pointee[0],
                item,
                Location::variable(base),
                Some(0),
                FrameMode::Both,
            );
            self.path.pop();
            result?;
        }
        self.write_var_ref(location.shifted(offset)?, variable, items.len())
    }

    fn array(
        &mut self,
        expected_len: u64,
        element_schema: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Array(items) = value else {
            return self.shape("fixed-size array");
        };
        let WireNode::Array {
            size,
            align,
            len,
            stride,
            elem,
            ..
        } = wire
        else {
            return self.invalid_wire("logical array is not a wire array");
        };
        if u64::from(*len) != expected_len || items.len() != *len as usize {
            return self.shape("fixed-size array with the declared length");
        }
        let (element_size, element_align) = self.geometry(elem)?;
        let expected_stride =
            align_up(element_size, element_align).ok_or_else(|| EncodeError::Bounds {
                path: self.path_string(),
                what: "array stride",
            })?;
        let expected_size =
            expected_stride
                .checked_mul(*len)
                .ok_or_else(|| EncodeError::Bounds {
                    path: self.path_string(),
                    what: "array size",
                })?;
        if *stride != expected_stride || *size != expected_size || *align != element_align {
            return self.invalid_wire("array geometry is inconsistent with its element");
        }
        let base = location.shifted(offset)?;
        for (index, item) in items.iter().enumerate() {
            self.path.push(PathComponent::Index(index as u64));
            let item_location =
                base.shifted((index as u32).checked_mul(*stride).ok_or_else(|| {
                    EncodeError::Bounds {
                        path: self.path_string(),
                        what: "array element offset",
                    }
                })?)?;
            let result = self.node(
                element_schema,
                elem,
                item,
                item_location,
                Some(0),
                FrameMode::Both,
            );
            self.path.pop();
            result?;
        }
        Ok(())
    }

    fn map(
        &mut self,
        key_schema: &'a SchemaNode,
        value_schema: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let (size, _, kind, pointee) =
            slot_parts(wire).ok_or_else(|| EncodeError::InvalidWireShape {
                path: self.path_string(),
                detail: "map is not a wire slot",
            })?;
        if size < 8 || kind != SlotKind::Map || pointee.len() != 2 {
            return self.invalid_wire("map slot has the wrong kind/pointee count");
        }
        let mut entries: Vec<(AuthoredValue, &AuthoredValue, Vec<u8>)> = match value {
            AuthoredValue::Object(map) if matches!(key_schema, SchemaNode::String) => map
                .iter()
                .map(|(key, value)| {
                    let key = AuthoredValue::Str(key.clone());
                    let encoded = canonical_value_bytes(&key).ok_or_else(|| {
                        EncodeError::KeyNotEncodable {
                            path: self.path_string(),
                        }
                    })?;
                    Ok((key, value, encoded))
                })
                .collect::<Result<_, EncodeError>>()?,
            AuthoredValue::Array(pairs) if !matches!(key_schema, SchemaNode::String) => pairs
                .iter()
                .map(|pair| {
                    let AuthoredValue::Array(pair) = pair else {
                        return self.shape("[key, value] map pair");
                    };
                    let [key, value] = pair.as_slice() else {
                        return self.shape("[key, value] map pair");
                    };
                    let encoded =
                        canonical_value_bytes(key).ok_or_else(|| EncodeError::KeyNotEncodable {
                            path: self.path_string(),
                        })?;
                    Ok((key.clone(), value, encoded))
                })
                .collect::<Result<_, EncodeError>>()?,
            _ => return self.shape("canonical map"),
        };
        entries.sort_by(|left, right| left.2.cmp(&right.2));
        for pair in entries.windows(2) {
            if pair[0].2 == pair[1].2 {
                return Err(EncodeError::DuplicateOrderedValue {
                    path: self.path_string(),
                });
            }
        }
        let (key_size, key_align) = self.geometry(&pointee[0])?;
        let (value_size, value_align) = self.geometry(&pointee[1])?;
        let value_offset = align_up(key_size, value_align).ok_or_else(|| EncodeError::Bounds {
            path: self.path_string(),
            what: "map value offset",
        })?;
        let pair_align = key_align.max(value_align);
        let pair_size =
            value_offset
                .checked_add(value_size)
                .ok_or_else(|| EncodeError::Bounds {
                    path: self.path_string(),
                    what: "map pair size",
                })?;
        let stride = align_up(pair_size, pair_align).ok_or_else(|| EncodeError::Bounds {
            path: self.path_string(),
            what: "map pair stride",
        })?;
        let total = (stride as usize)
            .checked_mul(entries.len())
            .ok_or_else(|| EncodeError::Bounds {
                path: self.path_string(),
                what: "map byte length",
            })?;
        let variable = self.reserve_variable(total, pair_align)?;
        for (index, (key, item, encoded_key)) in entries.into_iter().enumerate() {
            self.path.push(PathComponent::MapKey(encoded_key));
            let pair_base = variable
                .checked_add((index as u32).checked_mul(stride).ok_or_else(|| {
                    EncodeError::Bounds {
                        path: self.path_string(),
                        what: "map pair offset",
                    }
                })?)
                .ok_or_else(|| EncodeError::Bounds {
                    path: self.path_string(),
                    what: "map pair offset",
                })?;
            self.node(
                key_schema,
                &pointee[0],
                &key,
                Location::variable(pair_base),
                Some(0),
                FrameMode::Both,
            )?;
            let result = self.node(
                value_schema,
                &pointee[1],
                item,
                Location::variable(pair_base.checked_add(value_offset).ok_or_else(|| {
                    EncodeError::Bounds {
                        path: self.path_string(),
                        what: "map value offset",
                    }
                })?),
                Some(0),
                FrameMode::Both,
            );
            self.path.pop();
            result?;
        }
        self.write_var_ref(location.shifted(offset)?, variable, entries_len(value))
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        schema_root: &'a SchemaNode,
        fields: &'a [(String, u32, SchemaNode)],
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
        frame_mode: FrameMode,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Object(values) = value else {
            return self.shape("struct object");
        };
        let WireNode::Struct {
            fields: wire_fields,
            ..
        } = wire
        else {
            return self.invalid_wire("logical struct is not a wire struct");
        };
        if values.len() != fields.len() {
            return self.shape("struct with exactly the declared fields");
        }
        if wire_fields.len() != fields.len()
            || wire_fields
                .iter()
                .any(|wire_field| !fields.iter().any(|field| field.0 == wire_field.name))
        {
            return self.invalid_wire("wire struct fields differ from the logical schema");
        }
        if matches!(frame_mode, FrameMode::Both) {
            self.logical_frames.push(schema_root);
        }
        self.wire_frames.push(wire);
        let base = location.shifted(offset)?;
        let mut result = Ok(());
        for (name, _, field_schema) in fields {
            let Some(field_value) = values.get(name) else {
                result = self.shape("struct with every declared field");
                break;
            };
            let Some(field_wire) = wire_fields.iter().find(|field| field.name == *name) else {
                result = Err(EncodeError::MissingWireField {
                    path: self.path_string(),
                    field: name.clone(),
                });
                break;
            };
            self.path.push(PathComponent::Field(name.clone()));
            let encoded = self.node(
                field_schema,
                &field_wire.node,
                field_value,
                base,
                None,
                FrameMode::Both,
            );
            self.path.pop();
            if encoded.is_err() {
                result = encoded;
                break;
            }
        }
        self.wire_frames.pop();
        if matches!(frame_mode, FrameMode::Both) {
            self.logical_frames.pop();
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn enumeration(
        &mut self,
        schema_root: &'a SchemaNode,
        variants: &'a [(String, u32, SchemaNode)],
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
        frame_mode: FrameMode,
    ) -> Result<(), EncodeError> {
        let AuthoredValue::Object(values) = value else {
            return self.shape("single-variant enum object");
        };
        let Some((name, payload)) = values.iter().next().filter(|_| values.len() == 1) else {
            return self.shape("single-variant enum object");
        };
        let Some((_, _, payload_schema)) = variants.iter().find(|variant| variant.0 == *name)
        else {
            return self.shape("known enum variant");
        };
        let WireNode::Enum {
            form,
            variants: wire_variants,
            ..
        } = wire
        else {
            return self.invalid_wire("logical enum is not a wire enum");
        };
        let Some((_, variant)) = wire_variants
            .iter()
            .enumerate()
            .find(|(_, variant)| variant.name == *name)
        else {
            return Err(EncodeError::MissingWireVariant {
                path: self.path_string(),
                variant: name.clone(),
            });
        };
        let base = location.shifted(offset)?;
        let index = self.canonical_variant_index(wire_variants, name)?;
        self.write_enum_tag(form, index, variant, base)?;
        if matches!(frame_mode, FrameMode::Both) {
            self.logical_frames.push(schema_root);
        }
        self.wire_frames.push(wire);
        self.path.push(PathComponent::Variant(name.clone()));
        let result = self.node(
            payload_schema,
            &variant.node,
            payload,
            base,
            None,
            FrameMode::WireOnly,
        );
        self.path.pop();
        self.wire_frames.pop();
        if matches!(frame_mode, FrameMode::Both) {
            self.logical_frames.pop();
        }
        result
    }

    fn option(
        &mut self,
        inner: &'a SchemaNode,
        wire: &'a WireNode,
        value: &AuthoredValue,
        location: Location,
        offset: u32,
    ) -> Result<(), EncodeError> {
        let WireNode::Enum { form, variants, .. } = wire else {
            return self.invalid_wire("logical option is not a wire enum");
        };
        let name = if matches!(value, AuthoredValue::Null) {
            "None"
        } else {
            "Some"
        };
        let Some((_, variant)) = variants
            .iter()
            .enumerate()
            .find(|(_, variant)| variant.name == name)
        else {
            return Err(EncodeError::MissingWireVariant {
                path: self.path_string(),
                variant: name.to_owned(),
            });
        };
        let base = location.shifted(offset)?;
        let index = self.canonical_variant_index(variants, name)?;
        self.write_enum_tag(form, index, variant, base)?;
        if name == "None" {
            return Ok(());
        }
        let WireNode::Struct { fields, .. } = &variant.node else {
            return self.invalid_wire("Some payload is not a wire record");
        };
        let Some(field) = fields.iter().find(|field| field.name == "0") else {
            return Err(EncodeError::MissingWireField {
                path: self.path_string(),
                field: "0".to_owned(),
            });
        };
        self.wire_frames.push(wire);
        self.wire_frames.push(&variant.node);
        let result = self.node(
            inner,
            &field.node,
            value,
            base.shifted(variant.node.offset())?,
            None,
            FrameMode::Both,
        );
        self.wire_frames.pop();
        self.wire_frames.pop();
        result
    }

    fn write_enum_tag(
        &mut self,
        form: &WireEnumForm,
        index: usize,
        variant: &WireVariant,
        base: Location,
    ) -> Result<(), EncodeError> {
        match form {
            WireEnumForm::Single => Ok(()),
            WireEnumForm::FullyFlat {
                tag_offset,
                tag_size,
            } => {
                let raw = variant.discriminant.to_le_bytes();
                let Some(raw) = raw
                    .get(..*tag_size as usize)
                    .filter(|bytes| !bytes.is_empty())
                else {
                    return self.invalid_wire("fully-flat enum tag width is outside 1..=16");
                };
                self.write(base.shifted(*tag_offset)?, raw)
            }
            WireEnumForm::Canonical => {
                let index = u32::try_from(index).map_err(|_| EncodeError::Bounds {
                    path: self.path_string(),
                    what: "enum variant index",
                })?;
                self.write(base, &index.to_le_bytes())
            }
        }
    }

    fn canonical_variant_index(
        &self,
        variants: &[WireVariant],
        selected: &str,
    ) -> Result<usize, EncodeError> {
        let mut names = variants
            .iter()
            .map(|variant| nfc(&variant.name))
            .collect::<Vec<_>>();
        names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return self.invalid_wire("wire enum has duplicate NFC variant names");
        }
        let selected = nfc(selected);
        names
            .iter()
            .position(|name| *name == selected)
            .ok_or_else(|| EncodeError::MissingWireVariant {
                path: self.path_string(),
                variant: selected,
            })
    }

    fn geometry(&self, node: &WireNode) -> Result<(u32, u32), EncodeError> {
        match node {
            WireNode::BackRef { distance, .. } => {
                resolve_backref(&self.wire_frames, *distance)
                    .and_then(|(target, _)| geometry(target))
                    .ok_or_else(|| EncodeError::BackRefMismatch {
                        path: self.path_string(),
                    })
            }
            node => geometry(node).ok_or_else(|| EncodeError::InvalidWireShape {
                path: self.path_string(),
                detail: "wire node has no geometry",
            }),
        }
    }

    fn reserve_variable(&mut self, len: usize, align: u32) -> Result<u32, EncodeError> {
        if align == 0 || !align.is_power_of_two() {
            return self.invalid_wire("variable allocation has invalid alignment");
        }
        let start = align_up_usize(self.variable.len(), align as usize).ok_or_else(|| {
            EncodeError::Bounds {
                path: self.path_string(),
                what: "variable alignment",
            }
        })?;
        let end = start.checked_add(len).ok_or_else(|| EncodeError::Bounds {
            path: self.path_string(),
            what: "variable section length",
        })?;
        if end >= 1usize << 32 {
            return Err(EncodeError::Bounds {
                path: self.path_string(),
                what: "variable section length",
            });
        }
        self.variable.resize(end, 0);
        Ok(start as u32)
    }

    fn write_var_ref(
        &mut self,
        slot: Location,
        offset: u32,
        len: usize,
    ) -> Result<(), EncodeError> {
        let len = u32::try_from(len).map_err(|_| EncodeError::Bounds {
            path: self.path_string(),
            what: "VarRef length",
        })?;
        let mut bytes = [0; 8];
        bytes[..4].copy_from_slice(&offset.to_le_bytes());
        bytes[4..].copy_from_slice(&len.to_le_bytes());
        self.write(slot, &bytes)
    }

    fn write(&mut self, location: Location, bytes: &[u8]) -> Result<(), EncodeError> {
        let section = match location.section {
            Section::Fixed => &mut self.fixed,
            Section::Variable => &mut self.variable,
        };
        let start = location.base as usize;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| EncodeError::Bounds {
                path: display_path(&self.path),
                what: "wire write",
            })?;
        let destination = section
            .get_mut(start..end)
            .ok_or_else(|| EncodeError::Bounds {
                path: display_path(&self.path),
                what: "wire write",
            })?;
        destination.copy_from_slice(bytes);
        Ok(())
    }

    fn shape<T>(&self, expected: &'static str) -> Result<T, EncodeError> {
        Err(EncodeError::Shape {
            path: self.path_string(),
            expected,
        })
    }

    fn invalid_wire<T>(&self, detail: &'static str) -> Result<T, EncodeError> {
        Err(EncodeError::InvalidWireShape {
            path: self.path_string(),
            detail,
        })
    }

    fn path_string(&self) -> String {
        display_path(&self.path)
    }
}

fn geometry(node: &WireNode) -> Option<(u32, u32)> {
    Some(match node {
        WireNode::Primitive { size, align, .. }
        | WireNode::Struct { size, align, .. }
        | WireNode::Enum { size, align, .. }
        | WireNode::Array { size, align, .. }
        | WireNode::Slot { size, align, .. } => (*size, *align),
        WireNode::Unit { .. } => (0, 1),
        WireNode::BackRef { .. } => return None,
    })
}

fn slot_parts(node: &WireNode) -> Option<(u32, u32, SlotKind, &[WireNode])> {
    match node {
        WireNode::Slot {
            size,
            align,
            kind,
            pointee,
            ..
        } => Some((*size, *align, *kind, pointee)),
        _ => None,
    }
}

fn align_up(value: u32, align: u32) -> Option<u32> {
    if align == 0 || !align.is_power_of_two() {
        return None;
    }
    let remainder = value % align;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(align - remainder)
    }
}

fn align_up_usize(value: usize, align: usize) -> Option<usize> {
    if align == 0 || !align.is_power_of_two() {
        return None;
    }
    let remainder = value % align;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(align - remainder)
    }
}

fn nfc(value: &str) -> String {
    if is_nfc(value) {
        value.to_owned()
    } else {
        value.nfc().collect()
    }
}

fn canonical_value_bytes(value: &AuthoredValue) -> Option<Vec<u8>> {
    distill_json::write(value).ok().map(String::into_bytes)
}

fn entries_len(value: &AuthoredValue) -> usize {
    match value {
        AuthoredValue::Object(values) => values.len(),
        AuthoredValue::Array(values) => values.len(),
        _ => 0,
    }
}

fn display_path(path: &[PathComponent]) -> String {
    let mut result = String::from("data");
    for component in path {
        use std::fmt::Write as _;
        match component {
            PathComponent::Field(field) => {
                result.push('.');
                result.push_str(field);
            }
            PathComponent::Variant(variant) => {
                let _ = write!(result, ".<{variant}>");
            }
            PathComponent::Index(index) => {
                let _ = write!(result, "[{index}]");
            }
            PathComponent::MapKey(key) => {
                let _ = write!(result, "[{}]", String::from_utf8_lossy(key));
            }
        }
    }
    result
}

fn primitive_grammar_id(kind: PrimitiveKind) -> u8 {
    use crate::native::ScalarKind;
    match kind {
        PrimitiveKind::Bool => ScalarKind::Bool,
        PrimitiveKind::Char => ScalarKind::Char,
        PrimitiveKind::U8 => ScalarKind::U8,
        PrimitiveKind::U16 => ScalarKind::U16,
        PrimitiveKind::U32 => ScalarKind::U32,
        PrimitiveKind::U64 => ScalarKind::U64,
        PrimitiveKind::U128 => ScalarKind::U128,
        PrimitiveKind::I8 => ScalarKind::I8,
        PrimitiveKind::I16 => ScalarKind::I16,
        PrimitiveKind::I32 => ScalarKind::I32,
        PrimitiveKind::I64 => ScalarKind::I64,
        PrimitiveKind::I128 => ScalarKind::I128,
        PrimitiveKind::F32 => ScalarKind::F32,
        PrimitiveKind::F64 => ScalarKind::F64,
    }
    .grammar_id()
}

fn scalar_bytes(kind: PrimitiveKind, value: &AuthoredValue) -> Option<Vec<u8>> {
    macro_rules! unsigned {
        ($ty:ty) => {{
            let value = match value {
                AuthoredValue::UInt(value) => <$ty>::try_from(*value).ok()?,
                AuthoredValue::Int(value) => <$ty>::try_from(*value).ok()?,
                _ => return None,
            };
            value.to_le_bytes().to_vec()
        }};
    }
    macro_rules! signed {
        ($ty:ty) => {{
            let value = match value {
                AuthoredValue::UInt(value) => <$ty>::try_from(*value).ok()?,
                AuthoredValue::Int(value) => <$ty>::try_from(*value).ok()?,
                _ => return None,
            };
            value.to_le_bytes().to_vec()
        }};
    }
    Some(match kind {
        PrimitiveKind::Bool => vec![match value {
            AuthoredValue::Bool(false) => 0,
            AuthoredValue::Bool(true) => 1,
            _ => return None,
        }],
        PrimitiveKind::Char => {
            let AuthoredValue::Str(value) = value else {
                return None;
            };
            let mut chars = value.chars();
            let value = chars.next()?;
            if chars.next().is_some() {
                return None;
            }
            (value as u32).to_le_bytes().to_vec()
        }
        PrimitiveKind::U8 => unsigned!(u8),
        PrimitiveKind::U16 => unsigned!(u16),
        PrimitiveKind::U32 => unsigned!(u32),
        PrimitiveKind::U64 => unsigned!(u64),
        PrimitiveKind::U128 => unsigned!(u128),
        PrimitiveKind::I8 => signed!(i8),
        PrimitiveKind::I16 => signed!(i16),
        PrimitiveKind::I32 => signed!(i32),
        PrimitiveKind::I64 => signed!(i64),
        PrimitiveKind::I128 => signed!(i128),
        PrimitiveKind::F32 => {
            let mut adopted = match value {
                AuthoredValue::Float(value) => *value as f32,
                AuthoredValue::UInt(value) => *value as f32,
                AuthoredValue::Int(value) => *value as f32,
                _ => return None,
            };
            if !adopted.is_finite() {
                return None;
            }
            if adopted == 0.0 {
                adopted = 0.0;
            }
            adopted.to_bits().to_le_bytes().to_vec()
        }
        PrimitiveKind::F64 => {
            let mut value = match value {
                AuthoredValue::Float(value) => *value,
                AuthoredValue::UInt(value) => *value as f64,
                AuthoredValue::Int(value) => *value as f64,
                _ => return None,
            };
            if !value.is_finite() {
                return None;
            }
            if value == 0.0 {
                value = 0.0;
            }
            value.to_bits().to_le_bytes().to_vec()
        }
    })
}

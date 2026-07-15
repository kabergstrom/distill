//! The wire-layout grammar DSWL (§12): the layout hash is
//! `blake3("DSWL" ‖ version:u8 ‖ root)` over the wire tree, nodes
//! depth-first in physical field order — (wire offset ascending, then the
//! field's `declaration_index`) — integers LE, `str` as in §5. Node
//! payloads carry semantic identity, not just geometry.
//!
//! Node kind bytes, pinned:
//!
//! | kind | byte | payload |
//! |------|------|---------|
//! | primitive | 0x01 | `ScalarKind` id u8 |
//! | struct    | 0x02 | field count u32 + per-field (name:str, node) in physical order |
//! | enum      | 0x03 | tag-form u8 (single 0x00 / fully-flat 0x01 / canonical 0x02); fully-flat adds native tag offset u32 + tag size u8; then variant count u32 + per-variant (name:str, [fully-flat: discriminant u128 LE,] node) in name-sorted order |
//! | array     | 0x04 | len u32, stride u32, element node |
//! | slot      | 0x05 | slot kind u8 + pointee node(s): vec/set/box/arc one element node, map key then value, string/blob none; pointee offsets frame-relative to the element |
//! | unit      | 0x06 | — (header size 0, align 1 by rule) |
//! | backref   | 0x07 | distance u32 (header offset = the slot's own origin; size and align = the referenced frame's, by rule) |
//!
//! The grammar text (§12) pins struct's field count but leaves the enum
//! variant count implicit; without a count the encoding would not be
//! injective (a variant list could alias a following sibling), so the
//! count is included explicitly.

use crate::native::{LayoutHashError, ScalarKind};
use crate::wire::{SlotKind, WireEnumForm, WireField, WireNode, WireVariant};
use distill_core::id::LayoutHash;
use std::borrow::Cow;
use std::collections::BTreeSet;
use unicode_normalization::{is_nfc, UnicodeNormalization};

/// DSWL grammar version.
pub const DSWL_VERSION: u8 = 1;

pub(crate) fn nfc_of(value: &str) -> Cow<'_, str> {
    if is_nfc(value) {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(value.nfc().collect())
    }
}

fn put_str(out: &mut Vec<u8>, value: &str) -> Result<(), LayoutHashError> {
    let nfc = nfc_of(value);
    let length = u32::try_from(nfc.len()).map_err(|_| LayoutHashError::NameTooLong)?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(nfc.as_bytes());
    Ok(())
}

/// Serialize a wire tree to its pinned DSWL byte form.
pub fn dswl_bytes(root: &WireNode) -> Result<Vec<u8>, LayoutHashError> {
    let mut out = Vec::new();
    let mut frames = Vec::new();
    encode(root, &mut out, &mut frames)?;
    Ok(out)
}

/// `blake3("DSWL" ‖ version:u8 ‖ root)` — the layout hash (§12).
pub fn dswl_hash(root: &WireNode) -> Result<LayoutHash, LayoutHashError> {
    let body = dswl_bytes(root)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSWL");
    hasher.update(&[DSWL_VERSION]);
    hasher.update(&body);
    Ok(LayoutHash(*hasher.finalize().as_bytes()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DswlDecodeError {
    Truncated,
    TrailingBytes,
    Utf8,
    NonNfc,
    DuplicateName,
    NamesNotSorted,
    UnknownNode(u8),
    UnknownScalar(u8),
    UnknownSlot(u8),
    UnknownEnumForm(u8),
    InvalidGeometry,
    InvalidEnum,
    DuplicateDiscriminant,
    BackRefOutOfRange,
    BackRefGeometry,
    DepthExceeded,
    NodeLimitExceeded,
}

/// Decode the authenticated DSWL body carried by the daemon or pack. The
/// decoder is deliberately strict: it accepts only the one form emitted by
/// [`dswl_bytes`], so a layout hash can never name two in-memory trees.
pub fn decode_dswl(bytes: &[u8]) -> Result<WireNode, DswlDecodeError> {
    let mut decoder = Decoder {
        bytes,
        pos: 0,
        frames: Vec::new(),
        nodes: 0,
    };
    let root = decoder.node(0)?;
    if decoder.pos != bytes.len() {
        return Err(DswlDecodeError::TrailingBytes);
    }
    Ok(root)
}

struct Decoder<'a> {
    bytes: &'a [u8],
    pos: usize,
    frames: Vec<(u32, u32)>,
    nodes: usize,
}

impl Decoder<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], DswlDecodeError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(DswlDecodeError::Truncated)?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or(DswlDecodeError::Truncated)?;
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, DswlDecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, DswlDecodeError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }
    fn u128(&mut self) -> Result<u128, DswlDecodeError> {
        Ok(u128::from_le_bytes(
            self.take(16)?.try_into().expect("sixteen bytes"),
        ))
    }
    fn string(&mut self) -> Result<String, DswlDecodeError> {
        let len = usize::try_from(self.u32()?).map_err(|_| DswlDecodeError::Truncated)?;
        let value = std::str::from_utf8(self.take(len)?).map_err(|_| DswlDecodeError::Utf8)?;
        if !is_nfc(value) {
            return Err(DswlDecodeError::NonNfc);
        }
        Ok(value.to_owned())
    }

    fn node(&mut self, depth: usize) -> Result<WireNode, DswlDecodeError> {
        if depth > 256 {
            return Err(DswlDecodeError::DepthExceeded);
        }
        self.nodes += 1;
        if self.nodes > 1_000_000 {
            return Err(DswlDecodeError::NodeLimitExceeded);
        }
        let kind = self.u8()?;
        let offset = self.u32()?;
        let size = self.u32()?;
        let align = self.u32()?;
        if align == 0 || !align.is_power_of_two() || (size != 0 && offset % align != 0) {
            return Err(DswlDecodeError::InvalidGeometry);
        }
        match kind {
            0x01 => Ok(WireNode::Primitive {
                offset,
                size,
                align,
                kind: scalar(self.u8()?)?,
            }),
            0x02 => {
                let count = self.u32()?;
                self.frames.push((size, align));
                let mut fields = Vec::new();
                let mut names = BTreeSet::new();
                let mut previous_offset = None;
                for declaration_index in 0..count {
                    let name = self.string()?;
                    if !names.insert(name.clone()) {
                        return Err(DswlDecodeError::DuplicateName);
                    }
                    let node = self.node(depth + 1)?;
                    if previous_offset.is_some_and(|previous| previous > node.offset()) {
                        return Err(DswlDecodeError::InvalidGeometry);
                    }
                    previous_offset = Some(node.offset());
                    fields.push(WireField {
                        name,
                        declaration_index,
                        node,
                    });
                }
                self.frames.pop();
                Ok(WireNode::Struct {
                    offset,
                    size,
                    align,
                    fields,
                })
            }
            0x03 => {
                let form = match self.u8()? {
                    0x00 => WireEnumForm::Single,
                    0x01 => WireEnumForm::FullyFlat {
                        tag_offset: self.u32()?,
                        tag_size: self.u8()?,
                    },
                    0x02 => WireEnumForm::Canonical,
                    other => return Err(DswlDecodeError::UnknownEnumForm(other)),
                };
                let count = self.u32()?;
                self.frames.push((size, align));
                let mut variants = Vec::new();
                let mut previous_name: Option<String> = None;
                for _ in 0..count {
                    let name = self.string()?;
                    if previous_name
                        .as_ref()
                        .is_some_and(|previous| previous.as_bytes() >= name.as_bytes())
                    {
                        return Err(if previous_name.as_ref() == Some(&name) {
                            DswlDecodeError::DuplicateName
                        } else {
                            DswlDecodeError::NamesNotSorted
                        });
                    }
                    previous_name = Some(name.clone());
                    let discriminant = if matches!(form, WireEnumForm::FullyFlat { .. }) {
                        self.u128()?
                    } else {
                        0
                    };
                    let node = self.node(depth + 1)?;
                    variants.push(WireVariant {
                        name,
                        discriminant,
                        node,
                    });
                }
                self.frames.pop();
                validate_enum(form, size, &variants)?;
                Ok(WireNode::Enum {
                    offset,
                    size,
                    align,
                    form,
                    variants,
                })
            }
            0x04 => {
                let len = self.u32()?;
                let stride = self.u32()?;
                let elem = Box::new(self.node(depth + 1)?);
                Ok(WireNode::Array {
                    offset,
                    size,
                    align,
                    len,
                    stride,
                    elem,
                })
            }
            0x05 => {
                let slot = slot(self.u8()?)?;
                let pointee_count = match slot {
                    SlotKind::String | SlotKind::Blob => 0,
                    SlotKind::Map => 2,
                    SlotKind::Vec | SlotKind::Set | SlotKind::Box | SlotKind::Arc => 1,
                };
                let mut pointee = Vec::with_capacity(pointee_count);
                for _ in 0..pointee_count {
                    pointee.push(self.node(depth + 1)?);
                }
                Ok(WireNode::Slot {
                    offset,
                    size,
                    align,
                    kind: slot,
                    pointee,
                })
            }
            0x06 => {
                if size != 0 || align != 1 {
                    return Err(DswlDecodeError::InvalidGeometry);
                }
                Ok(WireNode::Unit { offset })
            }
            0x07 => {
                let distance = self.u32()?;
                let index = self
                    .frames
                    .len()
                    .checked_sub(1 + distance as usize)
                    .ok_or(DswlDecodeError::BackRefOutOfRange)?;
                if self.frames[index] != (size, align) {
                    return Err(DswlDecodeError::BackRefGeometry);
                }
                Ok(WireNode::BackRef { distance, offset })
            }
            other => Err(DswlDecodeError::UnknownNode(other)),
        }
    }
}

fn validate_enum(
    form: WireEnumForm,
    size: u32,
    variants: &[WireVariant],
) -> Result<(), DswlDecodeError> {
    if variants.is_empty() {
        return Err(DswlDecodeError::InvalidEnum);
    }
    match form {
        WireEnumForm::Single if variants.len() != 1 => Err(DswlDecodeError::InvalidEnum),
        WireEnumForm::FullyFlat {
            tag_offset,
            tag_size,
        } => {
            if tag_size == 0
                || tag_size > 16
                || tag_offset
                    .checked_add(u32::from(tag_size))
                    .is_none_or(|end| end > size)
            {
                return Err(DswlDecodeError::InvalidEnum);
            }
            let mask = if tag_size == 16 {
                u128::MAX
            } else {
                (1u128 << (u32::from(tag_size) * 8)) - 1
            };
            let mut discriminants = BTreeSet::new();
            for variant in variants {
                if variant.discriminant & !mask != 0 {
                    return Err(DswlDecodeError::InvalidEnum);
                }
                if !discriminants.insert(variant.discriminant) {
                    return Err(DswlDecodeError::DuplicateDiscriminant);
                }
            }
            Ok(())
        }
        WireEnumForm::Single | WireEnumForm::Canonical => Ok(()),
    }
}

fn scalar(id: u8) -> Result<ScalarKind, DswlDecodeError> {
    Ok(match id {
        0x00 => ScalarKind::Bool,
        0x01 => ScalarKind::Char,
        0x02 => ScalarKind::U8,
        0x03 => ScalarKind::U16,
        0x04 => ScalarKind::U32,
        0x05 => ScalarKind::U64,
        0x06 => ScalarKind::U128,
        0x07 => ScalarKind::I8,
        0x08 => ScalarKind::I16,
        0x09 => ScalarKind::I32,
        0x0A => ScalarKind::I64,
        0x0B => ScalarKind::I128,
        0x0C => ScalarKind::F32,
        0x0D => ScalarKind::F64,
        other => return Err(DswlDecodeError::UnknownScalar(other)),
    })
}

fn slot(id: u8) -> Result<SlotKind, DswlDecodeError> {
    Ok(match id {
        0x00 => SlotKind::Vec,
        0x01 => SlotKind::String,
        0x02 => SlotKind::Map,
        0x03 => SlotKind::Set,
        0x04 => SlotKind::Box,
        0x05 => SlotKind::Arc,
        0x06 => SlotKind::Blob,
        other => return Err(DswlDecodeError::UnknownSlot(other)),
    })
}

fn header(out: &mut Vec<u8>, kind: u8, offset: u32, size: u32, align: u32) {
    out.push(kind);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&align.to_le_bytes());
}

fn encode(
    node: &WireNode,
    out: &mut Vec<u8>,
    frames: &mut Vec<(u32, u32)>,
) -> Result<(), LayoutHashError> {
    match node {
        WireNode::Primitive {
            offset,
            size,
            align,
            kind,
        } => {
            header(out, 0x01, *offset, *size, *align);
            out.push(kind.grammar_id());
        }
        WireNode::Struct {
            offset,
            size,
            align,
            fields,
        } => {
            header(out, 0x02, *offset, *size, *align);
            let mut sorted: Vec<_> = fields.iter().collect();
            sorted.sort_by_key(|f| (f.node.offset(), f.declaration_index));
            out.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
            frames.push((*size, *align));
            for f in sorted {
                put_str(out, &f.name)?;
                encode(&f.node, out, frames)?;
            }
            frames.pop();
        }
        WireNode::Enum {
            offset,
            size,
            align,
            form,
            variants,
        } => {
            header(out, 0x03, *offset, *size, *align);
            match form {
                WireEnumForm::Single => out.push(0x00),
                WireEnumForm::FullyFlat {
                    tag_offset,
                    tag_size,
                } => {
                    out.push(0x01);
                    out.extend_from_slice(&tag_offset.to_le_bytes());
                    out.push(*tag_size);
                }
                WireEnumForm::Canonical => out.push(0x02),
            }
            // Name-sorted by NFC bytes (§5's order); a duplicate name has
            // no canonical order and is an error.
            let mut sorted: Vec<_> = variants.iter().map(|v| (nfc_of(&v.name), v)).collect();
            sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            for w in sorted.windows(2) {
                if w[0].0 == w[1].0 {
                    return Err(LayoutHashError::DuplicateName(w[0].0.clone().into_owned()));
                }
            }
            out.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
            frames.push((*size, *align));
            for (name, v) in sorted {
                let len = u32::try_from(name.len()).map_err(|_| LayoutHashError::NameTooLong)?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(name.as_bytes());
                if matches!(form, WireEnumForm::FullyFlat { .. }) {
                    out.extend_from_slice(&v.discriminant.to_le_bytes());
                }
                encode(&v.node, out, frames)?;
            }
            frames.pop();
        }
        WireNode::Array {
            offset,
            size,
            align,
            len,
            stride,
            elem,
        } => {
            header(out, 0x04, *offset, *size, *align);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&stride.to_le_bytes());
            encode(elem, out, frames)?;
        }
        WireNode::Slot {
            offset,
            size,
            align,
            kind,
            pointee,
        } => {
            header(out, 0x05, *offset, *size, *align);
            out.push(kind.grammar_id());
            for p in pointee {
                encode(p, out, frames)?;
            }
        }
        WireNode::BackRef { distance, offset } => {
            let frame_count = frames.len() as u32;
            let index = frames.len().checked_sub(1 + *distance as usize).ok_or(
                LayoutHashError::BackRefOutOfRange {
                    distance: *distance,
                    frames: frame_count,
                },
            )?;
            let (size, align) = frames[index];
            header(out, 0x07, *offset, size, align);
            out.extend_from_slice(&distance.to_le_bytes());
        }
        WireNode::Unit { offset } => {
            header(out, 0x06, *offset, 0, 1);
        }
    }
    Ok(())
}

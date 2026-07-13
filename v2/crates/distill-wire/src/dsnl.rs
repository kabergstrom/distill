//! The native-layout grammar DSNL (§12): the measured layout digest is
//! `blake3("DSNL" ‖ version:u8 ‖ root)` over the native tree — table ids
//! (`CtorId`, `DropId`, `SkipDefaultId`, every `whole_drop`) excluded by
//! rule, so two independently built binaries measuring the same layouts
//! produce equal digests.
//!
//! Nodes are serialized depth-first in physical field order — (offset
//! ascending, declaration index ascending) — integers LE, `str` as in §5
//! (u32 length + NFC UTF-8 bytes); each node is `kind:u8, offset:u32,
//! size:u32, align:u32` plus kind payload.
//!
//! Node kind bytes, pinned (declaration order of `NativeLayoutNode`):
//!
//! | kind | byte | payload |
//! |------|------|---------|
//! | scalar  | 0x01 | `ScalarKind` id u8 |
//! | struct  | 0x02 | field count u32 + per-field (name:str, declaration_index:u32, node), skip slots included |
//! | enum    | 0x03 | tag form u8 (direct 0x00: offset u32, size u8; niche 0x01: offset u32, size u8, niche_start u128; single 0x02), variant count u32, per-variant (name:str, declaration_index:u32, tag-info u8 [direct 0x00 + u128 / niche 0x01 + u32 / untagged 0x02 / single 0x03], payload node) in declaration-index order |
//! | array   | 0x04 | len u32, stride u32, element node |
//! | vec     | 0x05 | element node (no ctor id) |
//! | set     | 0x06 | element node |
//! | map     | 0x07 | key node, value node |
//! | box     | 0x08 | inner node |
//! | arc     | 0x09 | inner node |
//! | str     | 0x0A | — |
//! | blob    | 0x0B | — |
//! | skip    | 0x0C | — (measured align rides in the header; no writer id) |
//! | backref | 0x0D | distance u32 (header offset = the slot's own origin; size and align = the referenced frame's, by rule) |
//! | unit    | 0x0E | — (header size 0, align 1 by rule) |

use crate::native::{LayoutHashError, NativeLayoutNode, NativeTagEncoding, NativeVariantTag};
use unicode_normalization::{is_nfc, UnicodeNormalization};

/// DSNL grammar version.
pub const DSNL_VERSION: u8 = 1;

/// Serialize a native tree to its pinned DSNL byte form.
pub fn dsnl_bytes(root: &NativeLayoutNode) -> Result<Vec<u8>, LayoutHashError> {
    let mut out = Vec::new();
    let mut frames = Vec::new();
    encode(root, &mut out, &mut frames)?;
    Ok(out)
}

/// `blake3("DSNL" ‖ version:u8 ‖ root)` — the measured layout digest (§5).
pub fn dsnl_hash(root: &NativeLayoutNode) -> Result<[u8; 32], LayoutHashError> {
    let body = dsnl_bytes(root)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSNL");
    hasher.update(&[DSNL_VERSION]);
    hasher.update(&body);
    Ok(*hasher.finalize().as_bytes())
}

pub(crate) fn nfc_of(s: &str) -> std::borrow::Cow<'_, str> {
    if is_nfc(s) {
        std::borrow::Cow::Borrowed(s)
    } else {
        std::borrow::Cow::Owned(s.nfc().collect())
    }
}

pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) -> Result<(), LayoutHashError> {
    let nfc = nfc_of(s);
    let len = u32::try_from(nfc.len()).map_err(|_| LayoutHashError::NameTooLong)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(nfc.as_bytes());
    Ok(())
}

fn header(out: &mut Vec<u8>, kind: u8, offset: u32, size: u32, align: u32) {
    out.push(kind);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&align.to_le_bytes());
}

fn encode(
    node: &NativeLayoutNode,
    out: &mut Vec<u8>,
    frames: &mut Vec<(u32, u32)>,
) -> Result<(), LayoutHashError> {
    match *node {
        NativeLayoutNode::Scalar {
            offset,
            size,
            align,
            kind,
        } => {
            header(out, 0x01, offset, size, align);
            out.push(kind.grammar_id());
        }
        NativeLayoutNode::Struct {
            offset,
            size,
            align,
            whole_drop: _,
            fields,
        } => {
            header(out, 0x02, offset, size, align);
            let mut sorted: Vec<_> = fields.iter().collect();
            sorted.sort_by_key(|f| (f.node.offset(), f.declaration_index));
            out.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
            frames.push((size, align));
            for f in sorted {
                put_str(out, f.name)?;
                out.extend_from_slice(&f.declaration_index.to_le_bytes());
                encode(&f.node, out, frames)?;
            }
            frames.pop();
        }
        NativeLayoutNode::Enum {
            offset,
            size,
            align,
            tag,
            whole_drop: _,
            variants,
        } => {
            header(out, 0x03, offset, size, align);
            match tag {
                NativeTagEncoding::Direct { offset, size } => {
                    out.push(0x00);
                    out.extend_from_slice(&offset.to_le_bytes());
                    out.push(size);
                }
                NativeTagEncoding::Niche {
                    offset,
                    size,
                    niche_start,
                } => {
                    out.push(0x01);
                    out.extend_from_slice(&offset.to_le_bytes());
                    out.push(size);
                    out.extend_from_slice(&niche_start.to_le_bytes());
                }
                NativeTagEncoding::Single => out.push(0x02),
            }
            let mut sorted: Vec<_> = variants.iter().collect();
            sorted.sort_by_key(|v| v.declaration_index);
            out.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
            frames.push((size, align));
            for v in sorted {
                put_str(out, v.name)?;
                out.extend_from_slice(&v.declaration_index.to_le_bytes());
                match v.tag {
                    NativeVariantTag::Direct { value } => {
                        out.push(0x00);
                        out.extend_from_slice(&value.to_le_bytes());
                    }
                    NativeVariantTag::Niche { index } => {
                        out.push(0x01);
                        out.extend_from_slice(&index.to_le_bytes());
                    }
                    NativeVariantTag::Untagged => out.push(0x02),
                    NativeVariantTag::Single => out.push(0x03),
                }
                encode(v.node, out, frames)?;
            }
            frames.pop();
        }
        NativeLayoutNode::Array {
            offset,
            size,
            align,
            len,
            stride,
            elem,
        } => {
            header(out, 0x04, offset, size, align);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&stride.to_le_bytes());
            encode(elem, out, frames)?;
        }
        NativeLayoutNode::Vec {
            offset,
            size,
            align,
            elem,
            ctor: _,
        } => {
            header(out, 0x05, offset, size, align);
            encode(elem, out, frames)?;
        }
        NativeLayoutNode::Set {
            offset,
            size,
            align,
            elem,
            ctor: _,
        } => {
            header(out, 0x06, offset, size, align);
            encode(elem, out, frames)?;
        }
        NativeLayoutNode::Map {
            offset,
            size,
            align,
            key,
            value,
            ctor: _,
        } => {
            header(out, 0x07, offset, size, align);
            encode(key, out, frames)?;
            encode(value, out, frames)?;
        }
        NativeLayoutNode::BoxPtr {
            offset,
            size,
            align,
            inner,
            ctor: _,
        } => {
            header(out, 0x08, offset, size, align);
            encode(inner, out, frames)?;
        }
        NativeLayoutNode::ArcPtr {
            offset,
            size,
            align,
            inner,
            ctor: _,
        } => {
            header(out, 0x09, offset, size, align);
            encode(inner, out, frames)?;
        }
        NativeLayoutNode::Str {
            offset,
            size,
            align,
        } => {
            header(out, 0x0A, offset, size, align);
        }
        NativeLayoutNode::Blob {
            offset,
            size,
            align,
        } => {
            header(out, 0x0B, offset, size, align);
        }
        NativeLayoutNode::Skip {
            offset,
            size,
            align,
            writer: _,
        } => {
            header(out, 0x0C, offset, size, align);
        }
        NativeLayoutNode::BackRef { distance, offset } => {
            let frame_count = frames.len() as u32;
            let index = frames.len().checked_sub(1 + distance as usize).ok_or(
                LayoutHashError::BackRefOutOfRange {
                    distance,
                    frames: frame_count,
                },
            )?;
            let (size, align) = frames[index];
            header(out, 0x0D, offset, size, align);
            out.extend_from_slice(&distance.to_le_bytes());
        }
        NativeLayoutNode::Unit { offset } => {
            header(out, 0x0E, offset, 0, 1);
        }
    }
    Ok(())
}

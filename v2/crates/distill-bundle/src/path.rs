//! Canonical structural blob paths (§6): the injective key naming each blob
//! — the component sequence from the entry's `data` root to the blob leaf.
//! Blobs are ordered by `(local_id, encoded path bytes)`; the same key
//! orders the §12 artifact blob table, so the encoding is public and pinned
//! here:
//!
//! Per component: one tag byte, then the payload length as a **big-endian
//! u64**, then the payload bytes.
//!
//! | component  | tag  | payload                                          |
//! |------------|------|--------------------------------------------------|
//! | `Field`    | 0x01 | the field name's UTF-8 bytes                     |
//! | `Variant`  | 0x02 | the variant name's UTF-8 bytes                   |
//! | `Index`    | 0x03 | the index as 8-byte big-endian u64 (length = 8)  |
//! | `MapKey`   | 0x04 | the key value's canonical JSON encoding bytes    |
//!
//! Big-endian numerics make byte-order comparison agree with numeric order
//! for `Index`. `MapKey` payloads are the canonical JSON text of the key
//! value (`distill_json::write`), which is blob-free by §5's
//! serializability rule — well-defined before any blob is placed.

/// One step from an entry's `data` root toward a blob leaf.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PathComponent {
    /// Struct field, by name.
    Field(String),
    /// Enum variant, by name.
    Variant(String),
    /// Vec/array element, by position.
    Index(u64),
    /// Map value, by the key's canonical JSON encoding bytes.
    MapKey(Vec<u8>),
}

/// Encode a structural path to its pinned byte form (see module docs).
/// Paths compare by these bytes; the encoding is length-framed per
/// component, so no path is a prefix-confusable alias of another.
pub fn encode_path(components: &[PathComponent]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in components {
        match c {
            PathComponent::Field(name) => frame(&mut out, 0x01, name.as_bytes()),
            PathComponent::Variant(name) => frame(&mut out, 0x02, name.as_bytes()),
            PathComponent::Index(i) => frame(&mut out, 0x03, &i.to_be_bytes()),
            PathComponent::MapKey(bytes) => frame(&mut out, 0x04, bytes),
        }
    }
    out
}

fn frame(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    out.extend_from_slice(payload);
}

/// Human-readable rendering for error messages, rooted at `data`.
pub(crate) fn display_path(components: &[PathComponent]) -> String {
    use std::fmt::Write;
    let mut s = String::from("data");
    for c in components {
        match c {
            PathComponent::Field(name) => {
                s.push('.');
                s.push_str(name);
            }
            PathComponent::Variant(name) => {
                let _ = write!(s, ".<{name}>");
            }
            PathComponent::Index(i) => {
                let _ = write!(s, "[{i}]");
            }
            PathComponent::MapKey(bytes) => {
                let key = String::from_utf8_lossy(bytes).replace(['\n', ' '], "");
                let _ = write!(s, "[{key}]");
            }
        }
    }
    s
}

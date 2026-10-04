//! The schema-directed walk (§6): an entry's `LogicalSchema` walked
//! alongside its `data`, locating every blob leaf by canonical structural
//! path. One traversal serves four jobs (the `WalkMode`s): plain-parse
//! rejection of blob nodes, container-parse validation of
//! `{"len","offset"}` leaves, writer collection of blob bytes, and the
//! splice passes that swap between `Blob` values and placement objects.
//!
//! Structure mismatches are errors naming the structural path — canonical
//! bundles are total (§6 Adoption), so nothing is inferred:
//!
//! - `Option`: `Null` = None; anything else walks the inner node.
//! - Enums: single-key `{ "Variant": payload }` objects; the payload is
//!   the variant's node (a struct; unit variants are zero-field structs),
//!   walked in the enum's frame: a payload opens no frame of its own.
//! - `Map` with a `String` key node: a JSON object. Any other key node: an
//!   array of `[k, v]` pairs, strictly ascending by the key's canonical
//!   encoded bytes.
//! - `Set`/`Vec`/`Array`: JSON arrays; sets strictly ascending by encoded
//!   element bytes; fixed arrays length-checked.
//! - `BackRef(n)` resolves to the struct/enum expansion frame `n` levels up
//!   the walk path (0 = innermost) — an explicit frame stack, pushed on
//!   every struct/enum entry, so recursion expands naturally.
//! - Blobs are barred beneath map keys and set elements (§5): their
//!   canonical order is by encoded bytes, which a blob's offset-bearing
//!   encoding would circularly depend on.

use std::collections::BTreeMap;

use distill_json::AuthoredValue;
use ngp_schema::{PrimitiveKind, SchemaNode};

use crate::error::BundleError;
use crate::path::{display_path, encode_path, PathComponent};
use crate::MAX_WALK_DEPTH;

/// One blob located by the container-parse walk.
pub(crate) struct BlobSite {
    pub local_id: String,
    pub path_bytes: Vec<u8>,
    pub path_display: String,
    pub offset: u64,
    pub len: u64,
}

pub(crate) enum WalkMode<'m> {
    /// Plain encoding: reaching a Blob node is an error (§6 — blobs
    /// require the container).
    PlainParse,
    /// Container parse pass A: validate `{"len","offset"}` leaves against
    /// the chunk length and record placement for the global layout checks.
    ContainerValidate {
        blob_len: u64,
        sites: &'m mut Vec<BlobSite>,
    },
    /// Container parse pass B: replace validated `{"len","offset"}` leaves
    /// with the chunk bytes.
    ContainerFill { chunk: &'m [u8] },
    /// Writer pass A: expect `Blob` values, record (encoded path, len).
    WriterCollect { sites: &'m mut Vec<(Vec<u8>, u64)> },
    /// Writer pass B: move `Blob` bytes out into `sink` and splice the
    /// `{"len","offset"}` leaf from the computed `layout`.
    WriterFill {
        layout: &'m BTreeMap<Vec<u8>, u64>,
        sink: &'m mut BTreeMap<Vec<u8>, Vec<u8>>,
    },
}

/// Writer-side cap on `data` value nesting, checked iteratively (explicit
/// stack — no recursion) before anything recursive (clone, walk, drop of
/// clones) touches the value. Pinned below `distill_json::MAX_DEPTH` (512)
/// with room for the envelope's own nesting (top → assets → entry → data)
/// and the `{"len","offset"}` blob rewrite — the writer must never emit a
/// file the parser's depth cap would reject. Parse-side data is already
/// bounded by the JSON parser's cap.
pub(crate) const MAX_DATA_DEPTH: usize = 500;

/// Iterative depth check for writer input — a definite error, never a
/// stack overflow (§12's cap discipline).
pub(crate) fn check_data_depth(local_id: &str, data: &AuthoredValue) -> Result<(), BundleError> {
    let mut stack: Vec<(&AuthoredValue, usize)> = vec![(data, 1)];
    while let Some((value, depth)) = stack.pop() {
        if depth > MAX_DATA_DEPTH {
            return Err(BundleError::WalkDepth {
                local_id: local_id.to_string(),
                path: "data".to_string(),
            });
        }
        match value {
            AuthoredValue::Array(items) => {
                stack.extend(items.iter().map(|v| (v, depth + 1)));
            }
            AuthoredValue::Object(map) => {
                stack.extend(map.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Walk one entry's data against its schema root.
pub(crate) fn walk_entry(
    local_id: &str,
    root: &SchemaNode,
    data: &mut AuthoredValue,
    mode: WalkMode<'_>,
) -> Result<(), BundleError> {
    let mut walker = Walker {
        local_id,
        mode,
        frames: Vec::new(),
        path: Vec::new(),
        barred: false,
        depth: 0,
    };
    walker.walk(root, data)
}

pub(crate) fn kind_name(v: &AuthoredValue) -> &'static str {
    match v {
        AuthoredValue::Null => "null",
        AuthoredValue::Bool(_) => "bool",
        AuthoredValue::Int(_) | AuthoredValue::UInt(_) => "integer",
        AuthoredValue::Float(_) => "float",
        AuthoredValue::Str(_) => "string",
        AuthoredValue::Array(_) => "array",
        AuthoredValue::Object(_) => "object",
        AuthoredValue::Blob(_) => "blob bytes",
    }
}

struct Walker<'w, 's> {
    local_id: &'w str,
    mode: WalkMode<'w>,
    /// Expanded struct/enum frames on the walk path — the namespace
    /// `BackRef` distances index into (0 = innermost).
    frames: Vec<&'s SchemaNode>,
    path: Vec<PathComponent>,
    /// True beneath a map key or set element — blob-barred (§5).
    barred: bool,
    depth: usize,
}

impl<'w, 's> Walker<'w, 's> {
    fn path_str(&self) -> String {
        display_path(&self.path)
    }

    fn err_shape(&self, expected: impl Into<String>, value: &AuthoredValue) -> BundleError {
        BundleError::Shape {
            local_id: self.local_id.to_string(),
            path: self.path_str(),
            expected: expected.into(),
            found: kind_name(value).to_string(),
        }
    }

    fn walk(&mut self, node: &'s SchemaNode, value: &mut AuthoredValue) -> Result<(), BundleError> {
        if self.depth >= MAX_WALK_DEPTH {
            return Err(BundleError::WalkDepth {
                local_id: self.local_id.to_string(),
                path: self.path_str(),
            });
        }
        self.depth += 1;
        let out = self.walk_inner(node, value);
        self.depth -= 1;
        out
    }

    fn walk_inner(
        &mut self,
        node: &'s SchemaNode,
        value: &mut AuthoredValue,
    ) -> Result<(), BundleError> {
        match node {
            SchemaNode::Primitive(kind) => self.check_primitive(*kind, value),
            SchemaNode::String => match value {
                AuthoredValue::Str(_) => Ok(()),
                v => Err(self.err_shape("string", v)),
            },
            SchemaNode::Unit => match value {
                AuthoredValue::Null => Ok(()),
                v => Err(self.err_shape("unit (null)", v)),
            },
            // §4 reference queries use either the bare string shorthand or a
            // closed {path?, asset?} object. Resolution happens later, but the
            // bundle boundary still rejects malformed/unknown query fields.
            SchemaNode::AssetRef(_) => self.check_reference(value, "asset reference"),
            SchemaNode::WeakRef(_) => self.check_reference(value, "weak reference"),
            SchemaNode::Blob => self.handle_blob(value),
            SchemaNode::Option(inner) => match value {
                AuthoredValue::Null => Ok(()),
                some => self.walk(inner, some),
            },
            SchemaNode::Vec(elem) => {
                let AuthoredValue::Array(items) = value else {
                    return Err(self.err_shape("array (vec)", value));
                };
                self.walk_elements(elem, items, false)
            }
            SchemaNode::Array { len, elem } => {
                let AuthoredValue::Array(items) = value else {
                    return Err(self.err_shape("array (fixed-size)", value));
                };
                if items.len() as u64 != *len {
                    return Err(BundleError::ArrayLen {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        expected: *len,
                        actual: items.len() as u64,
                    });
                }
                self.walk_elements(elem, items, false)
            }
            SchemaNode::Set(elem) => {
                let AuthoredValue::Array(items) = value else {
                    return Err(self.err_shape("array (set)", value));
                };
                self.walk_elements(elem, items, true)
            }
            SchemaNode::Map { key, value: vnode } => self.walk_map(key, vnode, value),
            SchemaNode::Struct { fields, .. } => {
                self.frames.push(node);
                let out = self.walk_fields(fields, value);
                self.frames.pop();
                out
            }
            SchemaNode::Enum { variants, .. } => {
                let AuthoredValue::Object(map) = value else {
                    return Err(self.err_shape("enum object", value));
                };
                if map.len() != 1 {
                    return Err(BundleError::EnumShape {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        keys: map.len(),
                    });
                }
                let Some((vname, payload)) = map.iter_mut().next() else {
                    // len == 1 was just checked; keep this a definite error.
                    return Err(BundleError::EnumShape {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        keys: 0,
                    });
                };
                let Some((_, _, vnode)) = variants.iter().find(|(n, _, _)| n == vname) else {
                    return Err(BundleError::UnknownVariant {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        variant: vname.clone(),
                    });
                };
                self.frames.push(node);
                self.path.push(PathComponent::Variant(vname.clone()));
                // The payload is a struct node, but not a frame of its own
                // (§5): a back-reference inside it counts the enum's frame.
                let r = match vnode {
                    SchemaNode::Struct { fields, .. } => self.walk_fields(fields, payload),
                    vnode => self.walk(vnode, payload),
                };
                self.path.pop();
                self.frames.pop();
                r
            }
            SchemaNode::BackRef(distance) => {
                let frames = self.frames.len();
                let Some(index) = frames.checked_sub(1 + *distance as usize) else {
                    return Err(BundleError::BadBackRef {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        distance: *distance,
                        frames,
                    });
                };
                let target = self.frames[index];
                self.walk(target, value)
            }
        }
    }

    /// A struct body's fields, walked in whatever frame the caller opened:
    /// a struct's own, or its enum's for a variant payload.
    fn walk_fields(
        &mut self,
        fields: &'s [(String, u32, SchemaNode)],
        value: &mut AuthoredValue,
    ) -> Result<(), BundleError> {
        let AuthoredValue::Object(map) = value else {
            return Err(self.err_shape("struct object", value));
        };
        for k in map.keys() {
            if !fields.iter().any(|(name, _, _)| name == k) {
                return Err(BundleError::ExtraField {
                    local_id: self.local_id.to_string(),
                    path: self.path_str(),
                    field: k.clone(),
                });
            }
        }
        for (name, _, fnode) in fields {
            let Some(v) = map.get_mut(name) else {
                return Err(BundleError::MissingField {
                    local_id: self.local_id.to_string(),
                    path: self.path_str(),
                    field: name.clone(),
                });
            };
            self.path.push(PathComponent::Field(name.clone()));
            let r = self.walk(fnode, v);
            self.path.pop();
            r?;
        }
        Ok(())
    }

    fn check_reference(&self, value: &AuthoredValue, expected: &str) -> Result<(), BundleError> {
        match value {
            AuthoredValue::Str(_) => Ok(()),
            AuthoredValue::Object(fields)
                if !fields.is_empty()
                    && fields.keys().all(|name| name == "path" || name == "asset")
                    && fields
                        .values()
                        .all(|value| matches!(value, AuthoredValue::Str(_))) =>
            {
                Ok(())
            }
            value => Err(self.err_shape(format!("{expected} query"), value)),
        }
    }

    /// Vec/Array/Set elements. Set elements (`sorted_barred`) must be
    /// strictly ascending by canonical encoded bytes and are blob-barred.
    fn walk_elements(
        &mut self,
        elem: &'s SchemaNode,
        items: &mut [AuthoredValue],
        sorted_barred: bool,
    ) -> Result<(), BundleError> {
        let mut prev: Option<Vec<u8>> = None;
        for (i, item) in items.iter_mut().enumerate() {
            if sorted_barred {
                let enc = self.encode_key(item, "set element")?;
                if prev.as_deref().is_some_and(|p| p >= enc.as_slice()) {
                    return Err(BundleError::NotSorted {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                        what: "set elements",
                        index: i,
                    });
                }
                prev = Some(enc);
            }
            self.path.push(PathComponent::Index(i as u64));
            let saved = self.barred;
            self.barred = self.barred || sorted_barred;
            let r = self.walk(elem, item);
            self.barred = saved;
            self.path.pop();
            r?;
        }
        Ok(())
    }

    fn walk_map(
        &mut self,
        key: &'s SchemaNode,
        vnode: &'s SchemaNode,
        value: &mut AuthoredValue,
    ) -> Result<(), BundleError> {
        if matches!(key, SchemaNode::String) {
            // String-key maps are JSON objects; the parser already rejects
            // duplicate keys and BTreeMap order is the canonical order.
            let AuthoredValue::Object(map) = value else {
                return Err(self.err_shape("object (string-key map)", value));
            };
            for (k, v) in map.iter_mut() {
                let enc = self.encode_key(&AuthoredValue::Str(k.clone()), "map key")?;
                self.path.push(PathComponent::MapKey(enc));
                let r = self.walk(vnode, v);
                self.path.pop();
                r?;
            }
            return Ok(());
        }
        // Non-string-key maps: [k, v] pairs strictly ascending by the
        // key's canonical encoded bytes (§6).
        let AuthoredValue::Array(pairs) = value else {
            return Err(self.err_shape("array of [key, value] pairs (map)", value));
        };
        let mut prev: Option<Vec<u8>> = None;
        for (i, pair_value) in pairs.iter_mut().enumerate() {
            if !matches!(&*pair_value, AuthoredValue::Array(p) if p.len() == 2) {
                self.path.push(PathComponent::Index(i as u64));
                let err = self.err_shape("[key, value] pair", pair_value);
                self.path.pop();
                return Err(err);
            }
            let AuthoredValue::Array(pair) = pair_value else {
                unreachable!("shape checked above");
            };
            let enc = self.encode_key(&pair[0], "map key")?;
            if prev.as_deref().is_some_and(|p| p >= enc.as_slice()) {
                return Err(BundleError::NotSorted {
                    local_id: self.local_id.to_string(),
                    path: self.path_str(),
                    what: "map keys",
                    index: i,
                });
            }
            prev = Some(enc.clone());
            let [k, v] = pair.as_mut_slice() else {
                unreachable!("pair length checked above");
            };
            self.path.push(PathComponent::MapKey(enc));
            let saved = self.barred;
            self.barred = true;
            let rk = self.walk(key, k);
            self.barred = saved;
            let r = rk.and_then(|()| self.walk(vnode, v));
            self.path.pop();
            r?;
        }
        Ok(())
    }

    /// Canonical JSON encoding bytes of a key/element value — the ordering
    /// key and the `MapKey` path payload. Blob-free by §5, so failure
    /// (blob bytes or non-finite float inside) is a definite error.
    fn encode_key(&self, v: &AuthoredValue, what: &'static str) -> Result<Vec<u8>, BundleError> {
        distill_json::write(v)
            .map(String::into_bytes)
            .map_err(|_| BundleError::KeyNotEncodable {
                local_id: self.local_id.to_string(),
                path: self.path_str(),
                what,
            })
    }

    fn check_primitive(
        &self,
        kind: PrimitiveKind,
        value: &AuthoredValue,
    ) -> Result<(), BundleError> {
        use PrimitiveKind as K;
        if kind == K::F32
            && matches!(
                value,
                AuthoredValue::Float(_) | AuthoredValue::Int(_) | AuthoredValue::UInt(_)
            )
        {
            return self.check_f32(value);
        }
        let ok = match kind {
            K::Bool => matches!(value, AuthoredValue::Bool(_)),
            K::U8 => uint_in(value, u8::MAX as u128),
            K::U16 => uint_in(value, u16::MAX as u128),
            K::U32 => uint_in(value, u32::MAX as u128),
            K::U64 => uint_in(value, u64::MAX as u128),
            K::U128 => uint_in(value, u128::MAX),
            K::I8 => int_in(value, i8::MIN as i128, i8::MAX as i128),
            K::I16 => int_in(value, i16::MIN as i128, i16::MAX as i128),
            K::I32 => int_in(value, i32::MIN as i128, i32::MAX as i128),
            K::I64 => int_in(value, i64::MIN as i128, i64::MAX as i128),
            K::I128 => int_in(value, i128::MIN, i128::MAX),
            // Any JSON number is a float value ("2" and "2.0" are one
            // number to JSON); representability into f32 is decode's
            // concern (§12), not the walk's.
            K::F32 | K::F64 => match value {
                AuthoredValue::Float(f) => {
                    if !f.is_finite() {
                        return Err(BundleError::NonFiniteFloat {
                            local_id: self.local_id.to_string(),
                            path: self.path_str(),
                        });
                    }
                    true
                }
                AuthoredValue::Int(_) | AuthoredValue::UInt(_) => true,
                _ => false,
            },
            K::Char => matches!(value, AuthoredValue::Str(s) if {
                let mut chars = s.chars();
                chars.next().is_some() && chars.next().is_none()
            }),
        };
        if ok {
            Ok(())
        } else {
            Err(self.err_shape(format!("{} value", kind.canonical_name()), value))
        }
    }

    /// Parse through Rust's IEEE-754 binary32 conversion (specified as
    /// round-to-nearest-ties-even), then require value-level canonical
    /// JSON bytes to equal the shortest decimal re-emitted for those
    /// exact bits. Input whitespace/exponent spelling remains freely
    /// rewritable by the outer JSON parser/writer; numeric aliases do not.
    fn check_f32(&self, value: &AuthoredValue) -> Result<(), BundleError> {
        let found = distill_json::write(value).map_err(|_| BundleError::NonFiniteFloat {
            local_id: self.local_id.to_string(),
            path: self.path_str(),
        })?;
        let rounded = match value {
            // AuthoredValue's parsed semantic number is binary64; the
            // IEEE narrowing conversion itself is ties-even. Integers use
            // direct decimal parsing so values beyond binary64's exact
            // integer range are not rounded twice.
            AuthoredValue::Float(value) => *value as f32,
            AuthoredValue::Int(_) | AuthoredValue::UInt(_) => {
                found
                    .parse::<f32>()
                    .map_err(|_| BundleError::F32OutOfRange {
                        local_id: self.local_id.to_string(),
                        path: self.path_str(),
                    })?
            }
            _ => unreachable!("check_f32 is called only for numeric authored values"),
        };
        if !rounded.is_finite() {
            return Err(BundleError::F32OutOfRange {
                local_id: self.local_id.to_string(),
                path: self.path_str(),
            });
        }
        let canonical =
            distill_json::write_f32(rounded).map_err(|_| BundleError::F32OutOfRange {
                local_id: self.local_id.to_string(),
                path: self.path_str(),
            })?;
        if found != canonical {
            return Err(BundleError::NonCanonicalF32 {
                local_id: self.local_id.to_string(),
                path: self.path_str(),
                found,
                canonical,
            });
        }
        Ok(())
    }

    fn handle_blob(&mut self, value: &mut AuthoredValue) -> Result<(), BundleError> {
        let local_id = self.local_id.to_string();
        let path = self.path_str();
        if self.barred {
            return Err(BundleError::BlobBarred { local_id, path });
        }
        let path_bytes = encode_path(&self.path);
        match &mut self.mode {
            WalkMode::PlainParse => Err(BundleError::BlobInPlainBundle { local_id, path }),
            WalkMode::ContainerValidate { blob_len, sites } => {
                let limit = *blob_len;
                let (len, offset) =
                    read_blob_object(value).map_err(|detail| BundleError::BadBlobObject {
                        local_id: local_id.clone(),
                        path: path.clone(),
                        detail,
                    })?;
                let oob = |offset, len| BundleError::BlobOutOfBounds {
                    local_id: local_id.clone(),
                    path: path.clone(),
                    offset,
                    len,
                    blob_len: limit,
                };
                let end = offset.checked_add(len).ok_or_else(|| oob(offset, len))?;
                if end > limit {
                    return Err(oob(offset, len));
                }
                sites.push(BlobSite {
                    local_id,
                    path_bytes,
                    path_display: path,
                    offset,
                    len,
                });
                Ok(())
            }
            WalkMode::ContainerFill { chunk } => {
                let (len, offset) =
                    read_blob_object(value).map_err(|detail| BundleError::BadBlobObject {
                        local_id: local_id.clone(),
                        path: path.clone(),
                        detail,
                    })?;
                // Validated in pass A; slice defensively all the same.
                let bytes = usize::try_from(offset)
                    .ok()
                    .zip(usize::try_from(len).ok())
                    .and_then(|(o, l)| o.checked_add(l).map(|end| (o, end)))
                    .and_then(|(o, end)| chunk.get(o..end))
                    .ok_or(BundleError::BlobOutOfBounds {
                        local_id,
                        path,
                        offset,
                        len,
                        blob_len: chunk.len() as u64,
                    })?;
                *value = AuthoredValue::Blob(bytes.to_vec());
                Ok(())
            }
            WalkMode::WriterCollect { sites } => {
                let AuthoredValue::Blob(bytes) = value else {
                    return Err(BundleError::Shape {
                        local_id,
                        path,
                        expected: "blob bytes".to_string(),
                        found: kind_name(value).to_string(),
                    });
                };
                sites.push((path_bytes, bytes.len() as u64));
                Ok(())
            }
            WalkMode::WriterFill { layout, sink } => {
                let AuthoredValue::Blob(bytes) = value else {
                    return Err(BundleError::Shape {
                        local_id,
                        path,
                        expected: "blob bytes".to_string(),
                        found: kind_name(value).to_string(),
                    });
                };
                let Some(&offset) = layout.get(&path_bytes) else {
                    return Err(BundleError::Internal {
                        detail: format!("no layout offset for blob at {local_id:?} {path}"),
                    });
                };
                let bytes = std::mem::take(bytes);
                let mut placed = BTreeMap::new();
                placed.insert("len".to_string(), AuthoredValue::UInt(bytes.len() as u128));
                placed.insert("offset".to_string(), AuthoredValue::UInt(offset as u128));
                *value = AuthoredValue::Object(placed);
                sink.insert(path_bytes, bytes);
                Ok(())
            }
        }
    }
}

fn uint_in(v: &AuthoredValue, max: u128) -> bool {
    match v {
        AuthoredValue::UInt(n) => *n <= max,
        AuthoredValue::Int(i) => *i >= 0 && (*i as u128) <= max,
        _ => false,
    }
}

fn int_in(v: &AuthoredValue, min: i128, max: i128) -> bool {
    match v {
        AuthoredValue::Int(i) => *i >= min && *i <= max,
        AuthoredValue::UInt(n) => *n <= max as u128,
        _ => false,
    }
}

/// Decode a `{"len": n, "offset": m}` blob placement object, strictly.
fn read_blob_object(value: &AuthoredValue) -> Result<(u64, u64), String> {
    let AuthoredValue::Object(map) = value else {
        return Err(format!("found {}", kind_name(value)));
    };
    if map.len() != 2 || !map.contains_key("len") || !map.contains_key("offset") {
        return Err(format!(
            "object must have exactly {{\"len\", \"offset\"}}, found keys {:?}",
            map.keys().collect::<Vec<_>>()
        ));
    }
    let field = |name: &str| -> Result<u64, String> {
        match &map[name] {
            AuthoredValue::UInt(n) => u64::try_from(*n).map_err(|_| format!("{name} exceeds u64")),
            other => Err(format!(
                "{name} must be an unsigned integer, found {}",
                kind_name(other)
            )),
        }
    };
    Ok((field("len")?, field("offset")?))
}

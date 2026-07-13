//! The conformance checker (§11): does an AuthoredValue conform to a
//! schema AST? Run after EVERY migration edge, function edges included,
//! before tag extraction, validators, or artifact encoding can consume
//! the value.
//!
//! Pinned decisions beyond the §11 text (each marked PINNED below):
//! integer leaves accept either `Int`/`UInt` variant when in range; an
//! `F32` leaf requires exact f32 representability; `char` is a
//! single-scalar-value JSON string; asset references accept all §4 query
//! forms (uuid string, path string, `{path, asset}`, `{asset}`); sets and
//! non-string-key maps must be in canonical order (strictly increasing
//! encoded bytes — which subsumes the duplicate check).

use distill_json::AuthoredValue;
use ngp_schema::{PrimitiveKind, SchemaNode};
use std::fmt;

/// Names the structural path and the expectation that failed there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConformError {
    /// Structural path: `$` root, `.field`, `[index]`, `{Variant}`.
    pub path: String,
    pub expected: String,
    pub found: String,
}

impl fmt::Display for ConformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "at {}: expected {}, found {}",
            self.path, self.expected, self.found
        )
    }
}

impl std::error::Error for ConformError {}

fn found_of(v: &AuthoredValue) -> String {
    match v {
        AuthoredValue::Null => "null".into(),
        AuthoredValue::Bool(b) => format!("bool {b}"),
        AuthoredValue::Int(i) => format!("integer {i}"),
        AuthoredValue::UInt(u) => format!("integer {u}"),
        AuthoredValue::Float(x) => format!("float {x}"),
        AuthoredValue::Str(s) => format!("string {s:?}"),
        AuthoredValue::Array(a) => format!("array of {}", a.len()),
        AuthoredValue::Object(m) => format!("object of {}", m.len()),
        AuthoredValue::Blob(b) => format!("blob of {} bytes", b.len()),
    }
}

fn err(path: &str, expected: impl Into<String>, v: &AuthoredValue) -> ConformError {
    ConformError {
        path: path.to_string(),
        expected: expected.into(),
        found: found_of(v),
    }
}

/// Check `value` against `schema`.
pub fn conforms(value: &AuthoredValue, schema: &SchemaNode) -> Result<(), ConformError> {
    check(value, schema, &[], "$")
}

/// Integer range check. PINNED: either variant (`Int`/`UInt`) is accepted
/// when the value is in the kind's range — the canonical writer prints
/// both identically, so the variant is a representation detail, not data.
pub(crate) fn int_in_range(kind: PrimitiveKind, v: &AuthoredValue) -> Option<bool> {
    use PrimitiveKind::*;
    let (min, max): (i128, u128) = match kind {
        U8 => (0, u8::MAX as u128),
        U16 => (0, u16::MAX as u128),
        U32 => (0, u32::MAX as u128),
        U64 => (0, u64::MAX as u128),
        U128 => (0, u128::MAX),
        I8 => (i8::MIN as i128, i8::MAX as u128),
        I16 => (i16::MIN as i128, i16::MAX as u128),
        I32 => (i32::MIN as i128, i32::MAX as u128),
        I64 => (i64::MIN as i128, i64::MAX as u128),
        I128 => (i128::MIN, i128::MAX as u128),
        _ => return None, // not an integer kind
    };
    Some(match v {
        AuthoredValue::Int(i) => {
            if *i >= 0 {
                (*i as u128) <= max
            } else {
                *i >= min
            }
        }
        AuthoredValue::UInt(u) => *u <= max,
        _ => false,
    })
}

fn check(
    value: &AuthoredValue,
    schema: &SchemaNode,
    frames: &[&SchemaNode],
    path: &str,
) -> Result<(), ConformError> {
    match schema {
        SchemaNode::Primitive(kind) => check_primitive(value, *kind, path),
        SchemaNode::String => match value {
            AuthoredValue::Str(_) => Ok(()),
            _ => Err(err(path, "string", value)),
        },
        SchemaNode::Unit => match value {
            AuthoredValue::Null => Ok(()),
            _ => Err(err(path, "null (unit)", value)),
        },
        SchemaNode::Blob => match value {
            AuthoredValue::Blob(_) => Ok(()),
            _ => Err(err(path, "blob", value)),
        },
        SchemaNode::Option(inner) => match value {
            AuthoredValue::Null => Ok(()),
            v => check(v, inner, frames, path),
        },
        SchemaNode::Vec(elem) => match value {
            AuthoredValue::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    check(item, elem, frames, &format!("{path}[{i}]"))?;
                }
                Ok(())
            }
            _ => Err(err(path, "array (vec)", value)),
        },
        SchemaNode::Array { len, elem } => match value {
            AuthoredValue::Array(items) => {
                if items.len() as u64 != *len {
                    return Err(err(path, format!("array of exactly {len} elements"), value));
                }
                for (i, item) in items.iter().enumerate() {
                    check(item, elem, frames, &format!("{path}[{i}]"))?;
                }
                Ok(())
            }
            _ => Err(err(path, "array (fixed-size)", value)),
        },
        SchemaNode::Set(elem) => match value {
            AuthoredValue::Array(items) => {
                let mut prev: Option<String> = None;
                for (i, item) in items.iter().enumerate() {
                    let ipath = format!("{path}[{i}]");
                    check(item, elem, frames, &ipath)?;
                    // PINNED: canonical set order — strictly increasing
                    // encoded element bytes (§6); subsumes duplicates.
                    let enc = encode_for_order(item, &ipath)?;
                    if let Some(p) = &prev {
                        if *p == enc {
                            return Err(err(&ipath, "no duplicate set elements", item));
                        }
                        if *p > enc {
                            return Err(err(&ipath, "set elements sorted by encoded bytes", item));
                        }
                    }
                    prev = Some(enc);
                }
                Ok(())
            }
            _ => Err(err(path, "array (set)", value)),
        },
        SchemaNode::Map { key, value: val } => check_map(value, key, val, frames, path),
        SchemaNode::Struct { fields, .. } => match value {
            AuthoredValue::Object(_) => {
                let mut sub = frames.to_vec();
                sub.push(schema);
                check_struct_body(value, fields, &sub, path)
            }
            _ => Err(err(path, "object (struct)", value)),
        },
        SchemaNode::Enum { variants, .. } => match value {
            AuthoredValue::Object(m) => {
                if m.len() != 1 {
                    return Err(err(path, "single-key object naming an enum variant", value));
                }
                let (vname, payload) = m.iter().next().expect("len checked");
                let Some((_, _, pnode)) = variants.iter().find(|(n, _, _)| n == vname) else {
                    return Err(err(
                        path,
                        format!("a declared variant (got {vname:?})"),
                        value,
                    ));
                };
                // The variant payload struct is NOT a separate back-ref
                // frame (§5): the enum's own frame covers it.
                let mut sub = frames.to_vec();
                sub.push(schema);
                let vpath = format!("{path}{{{vname}}}");
                match pnode {
                    SchemaNode::Struct { fields, .. } => {
                        if !matches!(payload, AuthoredValue::Object(_)) {
                            return Err(err(&vpath, "object (variant payload)", payload));
                        }
                        check_struct_body(payload, fields, &sub, &vpath)
                    }
                    // Malformed grammar: payloads are struct nodes.
                    _ => Err(err(
                        &vpath,
                        "struct payload node (schema malformed)",
                        payload,
                    )),
                }
            }
            _ => Err(err(path, "object (enum)", value)),
        },
        // §4 pinned: references are query encodings — uuid string, path
        // string, { path, asset }, { asset }. A bare { path } is spelled
        // as a plain string; anything else is refused.
        SchemaNode::AssetRef(_) | SchemaNode::WeakRef(_) => check_asset_ref(value, path),
        SchemaNode::BackRef(d) => {
            let d = *d as usize;
            if d >= frames.len() {
                return Err(err(
                    path,
                    format!(
                        "resolvable back-reference (distance {d}, {} frames open)",
                        frames.len()
                    ),
                    value,
                ));
            }
            let idx = frames.len() - 1 - d;
            // Re-enter the referenced frame: inside it, distance 0 is that
            // frame itself, so the open stack is the prefix above it.
            check(value, frames[idx], &frames[..idx], path)
        }
    }
}

fn check_primitive(
    value: &AuthoredValue,
    kind: PrimitiveKind,
    path: &str,
) -> Result<(), ConformError> {
    use PrimitiveKind::*;
    match kind {
        Bool => match value {
            AuthoredValue::Bool(_) => Ok(()),
            _ => Err(err(path, "bool", value)),
        },
        F64 => match value {
            AuthoredValue::Float(f) if f.is_finite() => Ok(()),
            _ => Err(err(path, "finite float (f64)", value)),
        },
        // PINNED: an f32 leaf holds an exactly-f32-representable value —
        // authored f32 data round-trips losslessly through the f64 value
        // model, so anything else claims precision the leaf cannot hold.
        F32 => match value {
            AuthoredValue::Float(f)
                if f.is_finite() && (*f as f32).is_finite() && (*f as f32) as f64 == *f =>
            {
                Ok(())
            }
            _ => Err(err(path, "finite f32-representable float", value)),
        },
        // PINNED: char encodes as a JSON string holding exactly one
        // Unicode scalar value.
        Char => match value {
            AuthoredValue::Str(s) if s.chars().count() == 1 => Ok(()),
            _ => Err(err(path, "single-scalar-value string (char)", value)),
        },
        k => match int_in_range(k, value) {
            Some(true) => Ok(()),
            _ => Err(err(
                path,
                format!("integer in {} range", k.canonical_name()),
                value,
            )),
        },
    }
}

/// Struct body totality: EXACTLY the schema's field set (§11) — every
/// declared field present, no extras.
fn check_struct_body(
    value: &AuthoredValue,
    fields: &[(String, u32, SchemaNode)],
    frames: &[&SchemaNode],
    path: &str,
) -> Result<(), ConformError> {
    let AuthoredValue::Object(m) = value else {
        return Err(err(path, "object (struct)", value));
    };
    for (name, _, fnode) in fields {
        let Some(fv) = m.get(name) else {
            return Err(err(path, format!("field {name:?} present"), value));
        };
        check(fv, fnode, frames, &format!("{path}.{name}"))?;
    }
    if m.len() != fields.len() {
        let extra = m
            .keys()
            .find(|k| !fields.iter().any(|(n, _, _)| n == *k))
            .cloned()
            .unwrap_or_default();
        return Err(err(path, format!("no unknown field {extra:?}"), value));
    }
    Ok(())
}

fn check_map(
    value: &AuthoredValue,
    key: &SchemaNode,
    val: &SchemaNode,
    frames: &[&SchemaNode],
    path: &str,
) -> Result<(), ConformError> {
    if matches!(key, SchemaNode::String) {
        // String-key maps are objects (§6).
        let AuthoredValue::Object(m) = value else {
            return Err(err(path, "object (string-key map)", value));
        };
        for (k, v) in m {
            check(v, val, frames, &format!("{path}[{k:?}]"))?;
        }
        Ok(())
    } else {
        // Non-string-key maps are [k, v] pair arrays sorted by the key's
        // encoded bytes; duplicate encoded keys are an error (§6).
        let AuthoredValue::Array(pairs) = value else {
            return Err(err(path, "array of [key, value] pairs", value));
        };
        let mut prev: Option<String> = None;
        for (i, pair) in pairs.iter().enumerate() {
            let ppath = format!("{path}[{i}]");
            let AuthoredValue::Array(kv) = pair else {
                return Err(err(&ppath, "[key, value] pair", pair));
            };
            if kv.len() != 2 {
                return Err(err(&ppath, "[key, value] pair of exactly 2", pair));
            }
            check(&kv[0], key, frames, &format!("{ppath}[0]"))?;
            check(&kv[1], val, frames, &format!("{ppath}[1]"))?;
            let enc = encode_for_order(&kv[0], &ppath)?;
            if let Some(p) = &prev {
                if *p == enc {
                    return Err(err(&ppath, "no duplicate map keys", pair));
                }
                if *p > enc {
                    return Err(err(&ppath, "map keys sorted by encoded bytes", pair));
                }
            }
            prev = Some(enc);
        }
        Ok(())
    }
}

fn check_asset_ref(value: &AuthoredValue, path: &str) -> Result<(), ConformError> {
    const EXPECTED: &str = "asset reference: uuid/path string, {path, asset}, or {asset} (§4)";
    match value {
        AuthoredValue::Str(s) if !s.is_empty() => Ok(()),
        AuthoredValue::Object(m) => {
            let nonempty_str =
                |k: &str| matches!(m.get(k), Some(AuthoredValue::Str(s)) if !s.is_empty());
            let known = m.keys().all(|k| k == "path" || k == "asset");
            let ok = known
                && nonempty_str("asset")
                && (m.len() == 1 || (m.len() == 2 && nonempty_str("path")));
            if ok {
                Ok(())
            } else {
                Err(err(path, EXPECTED, value))
            }
        }
        _ => Err(err(path, EXPECTED, value)),
    }
}

/// Canonical encoded bytes for set/map-key ordering (§6). Key and element
/// encodings are blob-free by §5's serializability rule, so failure here
/// means the schema itself is malformed.
fn encode_for_order(v: &AuthoredValue, path: &str) -> Result<String, ConformError> {
    distill_json::write(v).map_err(|_| err(path, "canonically encodable (blob-free)", v))
}

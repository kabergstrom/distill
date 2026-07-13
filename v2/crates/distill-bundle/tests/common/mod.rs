//! Shared fixtures: schemas built directly from `SchemaNode` values, value
//! builders, and a raw container builder for crafting corrupt files.
#![allow(dead_code)]

use std::collections::BTreeMap;

use distill_bundle::{crc32c, AssetEntry, Bundle, CONTAINER_MAGIC, CONTAINER_VERSION};
use distill_core::id::LogicalHash;
use distill_json::AuthoredValue as V;
use ngp_schema::{node_hash, LogicalSchema, PrimitiveKind, SchemaNode as N};

// ---- value builders ----

pub fn obj(entries: &[(&str, V)]) -> V {
    V::Object(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

pub fn arr(items: Vec<V>) -> V {
    V::Array(items)
}

pub fn s(text: &str) -> V {
    V::Str(text.to_string())
}

pub fn u(n: u128) -> V {
    V::UInt(n)
}

pub fn blob(bytes: &[u8]) -> V {
    V::Blob(bytes.to_vec())
}

// ---- schema builders ----

pub fn st(fields: &[(&str, N)]) -> N {
    N::Struct {
        rev: 0,
        fields: fields
            .iter()
            .map(|(n, node)| (n.to_string(), 0, node.clone()))
            .collect(),
    }
}

pub fn en(variants: &[(&str, N)]) -> N {
    N::Enum {
        rev: 0,
        variants: variants
            .iter()
            .map(|(n, node)| (n.to_string(), 0, node.clone()))
            .collect(),
    }
}

pub fn schema(root: N) -> LogicalSchema {
    LogicalSchema { root }
}

pub fn lh(schema: &LogicalSchema) -> LogicalHash {
    node_hash(&schema.root).expect("fixture schema must hash")
}

/// Simple blob-free struct: { count: u32, name: string }.
pub fn simple_schema() -> LogicalSchema {
    schema(st(&[
        ("count", N::Primitive(PrimitiveKind::U32)),
        ("name", N::String),
    ]))
}

/// Blob leaves all named "data" but nested differently — leaf-name aliasing
/// resolved only by full structural paths: a direct field, a nested struct
/// field, an enum variant payload, a Vec element's struct, and a
/// non-string-key map's value.
pub fn blobby_schema() -> LogicalSchema {
    schema(st(&[
        (
            "choice",
            en(&[("A", st(&[("data", N::Blob)])), ("Off", st(&[]))]),
        ),
        ("data", N::Blob),
        ("inner", st(&[("data", N::Blob)])),
        ("items", N::Vec(Box::new(st(&[("data", N::Blob)])))),
        (
            "table",
            N::Map {
                key: Box::new(N::Primitive(PrimitiveKind::U32)),
                value: Box::new(st(&[("data", N::Blob)])),
            },
        ),
    ]))
}

/// Recursive tree: { children: Vec<BackRef(0)>, payload: Blob } — blobs at
/// every depth through BackRef expansion.
pub fn tree_schema() -> LogicalSchema {
    schema(st(&[
        ("children", N::Vec(Box::new(N::BackRef(0)))),
        ("payload", N::Blob),
    ]))
}

// ---- bundle builders ----

pub const UUID_A: &str = "11111111-2222-3333-4444-555555555555";
pub const UUID_B: &str = "66666666-7777-8888-9999-aaaaaaaaaaaa";
pub const TYPE_A: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
pub const BUNDLE_UUID: &str = "01234567-89ab-cdef-0123-456789abcdef";

pub fn entry(uuid: &str, schema: &LogicalSchema, data: V) -> AssetEntry {
    AssetEntry {
        uuid: uuid.parse().expect("fixture asset uuid"),
        type_uuid: TYPE_A.parse().expect("fixture type uuid"),
        schema_hash: lh(schema),
        data,
    }
}

pub fn bundle(
    schemas: &[&LogicalSchema],
    assets: Vec<(&str, AssetEntry)>,
    primary: Option<&str>,
) -> Bundle {
    Bundle {
        format_version: 1,
        uuid: BUNDLE_UUID.parse().expect("fixture bundle uuid"),
        primary: primary.map(str::to_string),
        schemas: schemas.iter().map(|s| (lh(s), (*s).clone())).collect(),
        assets: assets
            .into_iter()
            .map(|(id, e)| (id.to_string(), e))
            .collect(),
    }
}

// ---- raw container builder / dissector (for corruption tests) ----

pub fn align16(n: u64) -> u64 {
    (n + 15) & !15
}

/// Build container bytes from a JSON chunk and a blob chunk, with a correct
/// header. Corruption tests mutate the result (recomputing CRCs when the
/// corruption is meant to hit a later check).
pub fn build_container(json: &[u8], chunk: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&CONTAINER_MAGIC);
    out.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
    out.extend_from_slice(&(json.len() as u64).to_le_bytes());
    out.extend_from_slice(&(chunk.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32c(json).to_le_bytes());
    out.extend_from_slice(&crc32c(chunk).to_le_bytes());
    out.extend_from_slice(json);
    let json_end = 36 + json.len() as u64;
    out.resize(align16(json_end) as usize, 0);
    out.extend_from_slice(chunk);
    out
}

/// Split container bytes into (json, pad, blob chunk) per the header.
pub fn container_parts(bytes: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let json_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
    let blob_len = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;
    let json_end = 36 + json_len;
    let pad_end = align16(json_end as u64) as usize;
    (
        bytes[36..json_end].to_vec(),
        bytes[json_end..pad_end].to_vec(),
        bytes[pad_end..pad_end + blob_len].to_vec(),
    )
}

/// Patch the stored json_crc to match the (possibly mutated) json chunk.
pub fn fix_json_crc(bytes: &mut [u8]) {
    let (json, _, _) = container_parts(bytes);
    bytes[28..32].copy_from_slice(&crc32c(&json).to_le_bytes());
}

/// Patch the stored blob_crc to match the (possibly mutated) blob chunk.
pub fn fix_blob_crc(bytes: &mut [u8]) {
    let (_, _, chunk) = container_parts(bytes);
    bytes[32..36].copy_from_slice(&crc32c(&chunk).to_le_bytes());
}

// ---- reference envelope builder (bypasses write_bundle validation) ----

/// Build the envelope JSON value exactly as the canonical writer lays it
/// out, but with `data` spliced verbatim — no schema walk, no blob
/// handling. Lets tests construct files write_bundle would refuse.
pub fn envelope_value(b: &Bundle) -> V {
    let mut top = BTreeMap::new();
    top.insert(
        "format_version".to_string(),
        V::UInt(b.format_version as u128),
    );
    top.insert("uuid".to_string(), V::Str(b.uuid.to_string()));
    if let Some(p) = &b.primary {
        top.insert("primary".to_string(), V::Str(p.clone()));
    }
    let mut schemas = BTreeMap::new();
    for (h, sc) in &b.schemas {
        let text = ngp_schema::snapshot_to_json(sc).expect("fixture snapshot");
        schemas.insert(
            h.to_string(),
            distill_json::parse(&text).expect("snapshot is JSON"),
        );
    }
    top.insert("schemas".to_string(), V::Object(schemas));
    let mut assets = BTreeMap::new();
    for (id, e) in &b.assets {
        let mut m = BTreeMap::new();
        m.insert("uuid".to_string(), V::Str(e.uuid.to_string()));
        m.insert("type_uuid".to_string(), V::Str(e.type_uuid.to_string()));
        m.insert("schema_hash".to_string(), V::Str(e.schema_hash.to_string()));
        m.insert("data".to_string(), e.data.clone());
        assets.insert(id.clone(), V::Object(m));
    }
    top.insert("assets".to_string(), V::Object(assets));
    V::Object(top)
}

/// Plain-encoding bytes of the reference envelope (canonical text plus the
/// single trailing newline).
pub fn plain_bytes(b: &Bundle) -> Vec<u8> {
    let mut text = distill_json::write(&envelope_value(b)).expect("envelope writable");
    text.push('\n');
    text.into_bytes()
}

// ---- envelope mutation (plain text) ----

/// Parse plain-bundle bytes as JSON, apply `f` to the envelope value, and
/// re-serialize (canonical text + trailing newline).
pub fn mutate_envelope(plain: &[u8], f: impl FnOnce(&mut V)) -> Vec<u8> {
    let text = std::str::from_utf8(plain).expect("plain bundle is UTF-8");
    let mut v = distill_json::parse(text).expect("plain bundle is JSON");
    f(&mut v);
    let mut out = distill_json::write(&v).expect("mutated envelope writable");
    out.push('\n');
    out.into_bytes()
}

/// Navigate to a mutable object map inside an AuthoredValue.
pub fn as_obj(v: &mut V) -> &mut BTreeMap<String, V> {
    match v {
        V::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    }
}

/// Mutable access to an entry object inside an envelope value.
pub fn env_entry<'a>(env: &'a mut V, local_id: &str) -> &'a mut BTreeMap<String, V> {
    let assets = as_obj(env).get_mut("assets").expect("assets");
    as_obj(as_obj(assets).get_mut(local_id).expect("entry"))
}

/// Mutable access to an entry's `data` inside an envelope value.
pub fn env_data<'a>(env: &'a mut V, local_id: &str) -> &'a mut V {
    env_entry(env, local_id).get_mut("data").expect("data")
}

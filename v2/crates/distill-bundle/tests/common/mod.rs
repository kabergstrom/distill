//! Shared fixtures: schemas built directly from `SchemaNode` values, a
//! small JSON tree (`V`) for spelling data and envelopes as text, typed
//! entries converted from it through the canonical codec, and a raw
//! container builder for crafting corrupt files.
#![allow(dead_code)]

use std::collections::BTreeMap;

use distill_bundle::{crc32c, AssetEntry, Bundle, CONTAINER_MAGIC, CONTAINER_VERSION};
use distill_core::id::{LogicalHash, TypeUuid};
use distill_json::{Kind, Lexer};
use ngp_schema::{
    node_hash, parse_value, write_value, BlobSource, LogicalSchema, Options, PathComponent,
    PrimitiveKind, SchemaNode as N, TextError, Value, WriteOptions,
};

// ---- the JSON tree ----

/// A JSON value as text spells it, plus blob bytes (written as
/// `{"len","offset"}` placements when converted to a typed value).
#[derive(Debug, Clone, PartialEq)]
pub enum V {
    Null,
    Bool(bool),
    Int(i128),
    UInt(u128),
    Float(f64),
    Str(String),
    Array(Vec<V>),
    Object(BTreeMap<String, V>),
    Blob(Vec<u8>),
}

/// Canonical text of `v`; blobs are placed into `chunk` (16-aligned) or,
/// without one, panic.
fn write_json(v: &V, out: &mut String, chunk: &mut Option<&mut Vec<u8>>) {
    match v {
        V::Null => out.push_str("null"),
        V::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        V::Int(i) => out.push_str(&i.to_string()),
        V::UInt(u) => out.push_str(&u.to_string()),
        V::Float(f) => distill_json::write_f64(*f, out).expect("finite fixture float"),
        V::Str(s) => distill_json::write_str(s, out),
        V::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, out, chunk);
            }
            out.push(']');
        }
        V::Object(map) => {
            out.push('{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                distill_json::write_str(k, out);
                out.push(':');
                write_json(item, out, chunk);
            }
            out.push('}');
        }
        V::Blob(bytes) => {
            let chunk = chunk.as_mut().expect("blob outside a typed conversion");
            let offset = (chunk.len() + 15) & !15;
            chunk.resize(offset, 0);
            chunk.extend_from_slice(bytes);
            out.push_str(&format!("{{\"len\":{},\"offset\":{offset}}}", bytes.len()));
        }
    }
}

/// Canonical text of a blob-free tree.
pub fn json_text(v: &V) -> String {
    let mut out = String::new();
    write_json(v, &mut out, &mut None);
    out
}

/// Parse JSON text into the tree.
pub fn parse_json(text: &str) -> V {
    fn read(lex: &mut Lexer<'_>) -> V {
        match lex.peek().expect("JSON") {
            Kind::Null => {
                lex.null().unwrap();
                V::Null
            }
            Kind::Bool => V::Bool(lex.bool().unwrap()),
            Kind::Number => {
                let n = lex.number().unwrap();
                if !n.integer {
                    V::Float(n.f64().unwrap())
                } else if n.negative() {
                    V::Int(n.i128().unwrap())
                } else {
                    V::UInt(n.u128().unwrap())
                }
            }
            Kind::String => V::Str(lex.string().unwrap().into_owned()),
            Kind::Array => {
                let mut open = lex.array().unwrap();
                let mut items = Vec::new();
                while lex.next_item(&mut open).unwrap() {
                    items.push(read(lex));
                }
                V::Array(items)
            }
            Kind::Object => {
                let mut open = lex.object().unwrap();
                let mut map = BTreeMap::new();
                while let Some((k, _)) = lex.next_key(&mut open).unwrap() {
                    let item = read(lex);
                    assert!(map.insert(k.into_owned(), item).is_none(), "duplicate key");
                }
                V::Object(map)
            }
        }
    }
    let mut lex = Lexer::new(text);
    let v = read(&mut lex);
    lex.finish().unwrap();
    v
}

/// Typed value of `data` under `schema` through the canonical codec.
pub fn try_value(schema: &LogicalSchema, data: &V) -> Result<Value, TextError> {
    struct Chunk(Vec<u8>);
    impl BlobSource for Chunk {
        fn blob(&mut self, _: &[PathComponent], len: u64, offset: u64) -> Result<Vec<u8>, String> {
            Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
        }
    }
    let mut chunk = Vec::new();
    let mut text = String::new();
    write_json(data, &mut text, &mut Some(&mut chunk));
    let mut source = Chunk(chunk);
    parse_value(&text, &schema.root, Options::canonical().blobs(&mut source))
}

pub fn value(schema: &LogicalSchema, data: &V) -> Value {
    try_value(schema, data).unwrap_or_else(|e| panic!("fixture data: {e}"))
}

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
    typed_entry(uuid, schema, value(schema, &data))
}

pub fn typed_entry(uuid: &str, schema: &LogicalSchema, data: Value) -> AssetEntry {
    let type_uuid: TypeUuid = TYPE_A.parse().expect("fixture type uuid");
    let schema_hash = lh(schema);
    AssetEntry {
        uuid: uuid.parse().expect("fixture asset uuid"),
        type_uuid,
        schema_hash,
        authoring_only: false,
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

fn envelope_with(b: &Bundle, data: impl Fn(&str, &AssetEntry) -> V) -> V {
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
        schemas.insert(h.to_string(), parse_json(&text));
    }
    top.insert("schemas".to_string(), V::Object(schemas));
    let mut assets = BTreeMap::new();
    for (id, e) in &b.assets {
        let mut m = BTreeMap::new();
        m.insert("uuid".to_string(), V::Str(e.uuid.to_string()));
        m.insert("type_uuid".to_string(), V::Str(e.type_uuid.to_string()));
        m.insert("schema_hash".to_string(), V::Str(e.schema_hash.to_string()));
        m.insert("authoring_only".to_string(), V::Bool(e.authoring_only));
        m.insert("data".to_string(), data(id, e));
        assets.insert(id.clone(), V::Object(m));
    }
    top.insert("assets".to_string(), V::Object(assets));
    V::Object(top)
}

/// The envelope JSON value exactly as the canonical writer lays it out, a
/// blob-free bundle's data written by the codec.
pub fn envelope_value(b: &Bundle) -> V {
    envelope_with(b, |_, e| {
        let sc = &b.schemas[&e.schema_hash];
        parse_json(&write_value(&sc.root, &e.data, WriteOptions::default()).expect("data writable"))
    })
}

fn with_newline(v: &V) -> Vec<u8> {
    let mut text = json_text(v);
    text.push('\n');
    text.into_bytes()
}

/// Plain-encoding bytes of the reference envelope (canonical text plus the
/// single trailing newline).
pub fn plain_bytes(b: &Bundle) -> Vec<u8> {
    with_newline(&envelope_value(b))
}

/// Plain-encoding bytes of a one-entry bundle (`local_id` "a", `UUID_A`)
/// whose `data` is spliced verbatim — no schema check: files write_bundle
/// cannot produce.
pub fn raw_plain(schema: &LogicalSchema, local_id: &str, data: V) -> Vec<u8> {
    let b = bundle(
        &[schema],
        vec![(local_id, typed_entry(UUID_A, schema, Value::Unit))],
        None,
    );
    with_newline(&envelope_with(&b, |_, _| data.clone()))
}

// ---- envelope mutation (plain text) ----

/// Parse plain-bundle bytes as JSON, apply `f` to the envelope value, and
/// re-serialize (canonical text + trailing newline).
pub fn mutate_envelope(plain: &[u8], f: impl FnOnce(&mut V)) -> Vec<u8> {
    let text = std::str::from_utf8(plain).expect("plain bundle is UTF-8");
    let mut v = parse_json(text);
    f(&mut v);
    with_newline(&v)
}

/// Navigate to a mutable object map inside a tree.
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

//! The envelope (§6): the JSON structure both physical encodings share.
//!
//! Top-level keys: `format_version` (1, or 2 with directory import rules:
//! `format_version_for`), `uuid`, `primary`
//! (optional, must name an existing entry), `schemas` (32-hex LogicalHash →
//! schema snapshot JSON, key verified to equal the snapshot's own hash),
//! `types` (optional, the dynamic-type closure: type path →
//! {schema_hash, type_uuid}, each schema in `schemas`), `assets` (local_id →
//! {uuid, type_uuid, schema_hash, authoring_only, data}). Unknown keys at
//! either level are errors — the canonical writer never emits them. Bundles are schema-closed: every entry's
//! `schema_hash` resolves in `schemas`. The `$` local_id namespace is
//! reserved for exactly `$settings` and `$record`.
//!
//! The envelope is lexed, not parsed into a tree: each key's value span is
//! recorded (its syntax checked in full), then decoded in a fixed order so
//! errors do not depend on key order. Entry data is read last, typed,
//! against its schema (`data.rs`): `"assets"` sorts before `"schemas"`.

use std::collections::BTreeMap;
use std::ops::Range;

use distill_core::bootstrap::{
    BootstrapControlSpecV1, BOOTSTRAP_CONTROL_TYPE_UUIDS, DIRECTORY_IMPORT_RULES_TYPE_UUID,
    IMPORT_RECORD_TYPE_UUID,
};
use distill_core::id::{AssetUuid, BundleUuid, LogicalHash, TypeUuid};
use distill_json::{Kind, Lexer, ParseError, ParseErrorKind};
use ngp_schema::{check_hash, node_bytes, read_snapshot, snapshot_to_json, LogicalSchema};

use crate::data::{parse_entry, Blobs};
use crate::error::BundleError;
use crate::{
    format_version_for, AssetEntry, Bundle, BUNDLE_FORMAT_VERSION, RULES_FORMAT_VERSION,
};

pub(crate) fn utf8(bytes: &[u8]) -> Result<&str, BundleError> {
    std::str::from_utf8(bytes).map_err(|e| BundleError::NotUtf8 {
        offset: e.valid_up_to(),
    })
}

/// A value span the first pass skipped: where it is and the nesting it
/// sat at.
#[derive(Clone)]
struct Span {
    range: Range<usize>,
    depth: usize,
}

impl Span {
    fn lexer<'t>(&self, text: &'t str) -> Lexer<'t> {
        Lexer::span(text, self.range.clone(), self.depth)
    }
}

fn json(e: ParseError) -> BundleError {
    BundleError::Json(e)
}

/// What the next value is, as the envelope errors name it.
fn found(lex: &mut Lexer<'_>) -> Result<&'static str, BundleError> {
    Ok(match lex.peek().map_err(json)? {
        Kind::Number => {
            if lex.clone().number().map_err(json)?.integer {
                "integer"
            } else {
                "float"
            }
        }
        kind => kind.name(),
    })
}

/// An object's keys and value spans; a repeated key is a JSON error (§6).
/// The caller has peeked an object.
fn members(lex: &mut Lexer<'_>) -> Result<BTreeMap<String, Span>, BundleError> {
    let mut open = lex.object().map_err(json)?;
    let mut out = BTreeMap::new();
    while let Some((key, offset)) = lex.next_key(&mut open).map_err(json)? {
        let depth = lex.depth();
        let range = lex.skip().map_err(json)?;
        if out.insert(key.into_owned(), Span { range, depth }).is_some() {
            return Err(json(ParseError {
                kind: ParseErrorKind::DuplicateKey,
                offset,
            }));
        }
    }
    Ok(out)
}

/// A string value, or what was found instead.
fn string(lex: &mut Lexer<'_>) -> Result<Result<String, &'static str>, BundleError> {
    if lex.peek().map_err(json)? == Kind::String {
        Ok(Ok(lex.string().map_err(json)?.into_owned()))
    } else {
        Ok(Err(found(lex)?))
    }
}

/// How strictly the envelope is read.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Strictness {
    /// Every defect is an error.
    Full,
    /// Only what can change the namespace: unknown keys and `primary` are
    /// ignored (`extract_namespace_skeleton`).
    Namespace,
}

/// Parse an envelope and its entries' data. `blobs` is the encoding's blob
/// source.
pub(crate) fn parse(
    text: &str,
    strictness: Strictness,
    mut blobs: Blobs<'_>,
) -> Result<Bundle, BundleError> {
    let mut lex = Lexer::new(text);
    if lex.peek().map_err(json)? != Kind::Object {
        let found = found(&mut lex)?;
        lex.skip().map_err(json)?;
        lex.finish().map_err(json)?;
        return Err(BundleError::EnvelopeNotObject { found });
    }
    let mut top = members(&mut lex)?;
    lex.finish().map_err(json)?;
    for key in top.keys() {
        if !matches!(
            key.as_str(),
            "assets" | "format_version" | "primary" | "schemas" | "types" | "uuid"
        ) && strictness == Strictness::Full
        {
            return Err(BundleError::UnknownEnvelopeKey { key: key.clone() });
        }
    }

    let format_version = {
        let span = top.remove("format_version").ok_or(BundleError::MissingEnvelopeKey {
            key: "format_version",
        })?;
        let mut lex = span.lexer(text);
        let n = match lex.peek().map_err(json)? {
            Kind::Number => {
                let n = lex.clone().number().map_err(json)?;
                (n.integer && !n.negative()).then(|| n.u128()).flatten()
            }
            _ => None,
        };
        match n {
            Some(n) if n == BUNDLE_FORMAT_VERSION as u128 || n == RULES_FORMAT_VERSION as u128 => {
                n as u32
            }
            Some(n) => return Err(BundleError::UnsupportedFormatVersion { found: n }),
            None => {
                return Err(BundleError::FormatVersionNotUInt {
                    found: found(&mut lex)?,
                })
            }
        }
    };

    let uuid: BundleUuid = {
        let span = top
            .remove("uuid")
            .ok_or(BundleError::MissingEnvelopeKey { key: "uuid" })?;
        match string(&mut span.lexer(text))? {
            Ok(s) => s
                .parse()
                .map_err(|_| BundleError::BadBundleUuid { found: s.clone() })?,
            Err(found) => {
                return Err(BundleError::BadBundleUuid {
                    found: found.to_string(),
                })
            }
        }
    };

    let primary = match top.remove("primary") {
        None => None,
        Some(_) if strictness == Strictness::Namespace => None,
        Some(span) => match string(&mut span.lexer(text))? {
            Ok(s) => Some(s),
            Err(found) => return Err(BundleError::PrimaryNotString { found }),
        },
    };

    let mut schemas = BTreeMap::new();
    {
        let span = top
            .remove("schemas")
            .ok_or(BundleError::MissingEnvelopeKey { key: "schemas" })?;
        let mut lex = span.lexer(text);
        if lex.peek().map_err(json)? != Kind::Object {
            return Err(BundleError::SchemasNotObject {
                found: found(&mut lex)?,
            });
        }
        for (key, span) in members(&mut lex)? {
            let hash: LogicalHash = key
                .parse()
                .map_err(|_| BundleError::BadSchemaKey { key: key.clone() })?;
            // The snapshot is read for structure and its hash checked: a
            // hand-reformatted envelope still verifies (and rewrites
            // canonically).
            let schema = read_snapshot(&mut span.lexer(text))
                .and_then(|schema| check_hash(&schema, hash).map(|()| schema))
                .map_err(|error| BundleError::Schema { hash, error })?;
            if schemas.insert(hash, schema).is_some() {
                return Err(BundleError::DuplicateSchema { hash });
            }
        }
    }

    // The dynamic-type closure (GAP 23 §3): path → {schema_hash, type_uuid}.
    let mut types = BTreeMap::new();
    if let Some(span) = top.remove("types") {
        let mut lex = span.lexer(text);
        if lex.peek().map_err(json)? != Kind::Object {
            return Err(BundleError::BadType {
                path: String::new(),
                detail: format!("types must be a JSON object, found {}", found(&mut lex)?),
            });
        }
        for (path, span) in members(&mut lex)? {
            let mut lex = span.lexer(text);
            if lex.peek().map_err(json)? != Kind::Object {
                return Err(BundleError::BadType {
                    detail: format!("must be an object, found {}", found(&mut lex)?),
                    path,
                });
            }
            let mut entry = members(&mut lex)?;
            if let Some(key) = entry
                .keys()
                .find(|key| !matches!(key.as_str(), "schema_hash" | "type_uuid"))
            {
                return Err(BundleError::BadType {
                    detail: format!("unknown key {key:?}"),
                    path,
                });
            }
            let type_uuid: TypeUuid = take_id(text, &mut entry, &path, "type_uuid")
                .map_err(|error| BundleError::BadType { path: path.clone(), detail: error.to_string() })?;
            let schema_hash: LogicalHash = take_id(text, &mut entry, &path, "schema_hash")
                .map_err(|error| BundleError::BadType { path: path.clone(), detail: error.to_string() })?;
            if !schemas.contains_key(&schema_hash) {
                return Err(BundleError::MissingTypeSchema { path, schema_hash });
            }
            types.insert(path, crate::BundleType { type_uuid, schema_hash });
        }
    }
    let table = crate::type_table(&types, &schemas)?;

    let mut assets = BTreeMap::new();
    let mut data_spans = BTreeMap::new();
    {
        let span = top
            .remove("assets")
            .ok_or(BundleError::MissingEnvelopeKey { key: "assets" })?;
        let mut lex = span.lexer(text);
        if lex.peek().map_err(json)? != Kind::Object {
            return Err(BundleError::AssetsNotObject {
                found: found(&mut lex)?,
            });
        }
        for (local_id, span) in members(&mut lex)? {
            check_local_id(&local_id)?;
            let mut lex = span.lexer(text);
            if lex.peek().map_err(json)? != Kind::Object {
                return Err(BundleError::EntryNotObject {
                    local_id,
                    found: found(&mut lex)?,
                });
            }
            let mut entry = members(&mut lex)?;
            for key in entry.keys() {
                if !matches!(
                    key.as_str(),
                    "authoring_only" | "data" | "schema_hash" | "type_uuid" | "uuid"
                ) && strictness == Strictness::Full
                {
                    return Err(BundleError::UnknownEntryKey {
                        local_id,
                        key: key.clone(),
                    });
                }
            }
            let uuid: AssetUuid = take_id(text, &mut entry, &local_id, "uuid")?;
            let type_uuid: TypeUuid = take_id(text, &mut entry, &local_id, "type_uuid")?;
            let schema_hash: LogicalHash = take_id(text, &mut entry, &local_id, "schema_hash")?;
            let authoring_only = {
                let span = entry
                    .remove("authoring_only")
                    .ok_or_else(|| BundleError::MissingEntryKey {
                        local_id: local_id.clone(),
                        key: "authoring_only",
                    })?;
                let mut lex = span.lexer(text);
                if lex.peek().map_err(json)? != Kind::Bool {
                    return Err(BundleError::AuthoringOnlyNotBool {
                        local_id,
                        found: found(&mut lex)?,
                    });
                }
                lex.bool().map_err(json)?
            };
            if local_id.starts_with('$') && !authoring_only {
                return Err(BundleError::ReservedEntryMustBeAuthoringOnly { local_id });
            }
            let data = entry
                .remove("data")
                .ok_or_else(|| BundleError::MissingEntryKey {
                    local_id: local_id.clone(),
                    key: "data",
                })?;
            data_spans.insert(local_id.clone(), data);
            assets.insert(
                local_id,
                AssetEntry {
                    uuid,
                    type_uuid,
                    schema_hash,
                    authoring_only,
                    data: ngp_schema::Value::Unit,
                },
            );
        }
    }

    // Schema-closure (§6, §11): every entry schema resolves within the file.
    for (local_id, entry) in &assets {
        for schema_hash in schema_references(entry) {
            if !schemas.contains_key(&schema_hash) {
                return Err(BundleError::MissingSchema {
                    local_id: local_id.clone(),
                    schema_hash,
                });
            }
        }
        validate_entry_role(local_id, format_version, entry, &schemas[&entry.schema_hash])?;
    }
    check_format_version(format_version, &assets)?;
    if let Some(p) = &primary {
        match assets.get(p) {
            None => return Err(BundleError::PrimaryNotFound { primary: p.clone() }),
            Some(entry) if entry.authoring_only => {
                return Err(BundleError::PrimaryIsAuthoringOnly { primary: p.clone() })
            }
            Some(_) => {}
        }
    }

    for (local_id, entry) in assets.iter_mut() {
        let span = &data_spans[local_id];
        let root = &schemas[&entry.schema_hash].root;
        entry.data = parse_entry(local_id, root, &mut span.lexer(text), &mut blobs, &table)?;
    }

    Ok(Bundle {
        format_version,
        uuid,
        primary,
        schemas,
        types,
        assets,
    })
}

/// Exact schema hashes named by a bundle entry.
pub(crate) fn schema_references(entry: &AssetEntry) -> Vec<LogicalHash> {
    vec![entry.schema_hash]
}

/// The reserved `$` namespace (§6): only `$settings` and `$record`, at
/// every boundary that can put an entry in a bundle.
pub(crate) fn check_local_id(local_id: &str) -> Result<(), BundleError> {
    if local_id.starts_with('$') && local_id != "$settings" && local_id != "$record" {
        return Err(BundleError::ReservedLocalId {
            local_id: local_id.to_string(),
        });
    }
    Ok(())
}

fn take_id<T: std::str::FromStr>(
    text: &str,
    entry: &mut BTreeMap<String, Span>,
    local_id: &str,
    field: &'static str,
) -> Result<T, BundleError> {
    let span = entry
        .remove(field)
        .ok_or_else(|| BundleError::MissingEntryKey {
            local_id: local_id.to_string(),
            key: field,
        })?;
    let found = match string(&mut span.lexer(text))? {
        Ok(s) => match s.parse() {
            Ok(id) => return Ok(id),
            Err(_) => s,
        },
        Err(found) => found.to_string(),
    };
    Err(BundleError::BadEntryId {
        local_id: local_id.to_string(),
        field,
        found,
    })
}

/// Entry roles: `$record` is exactly the built-in ImportRecord, and a
/// bootstrap control entry is authoring-only with the embedded schema.
pub(crate) fn validate_entry_role(
    local_id: &str,
    format_version: u32,
    entry: &AssetEntry,
    schema: &LogicalSchema,
) -> Result<(), BundleError> {
    if (local_id == "$record") != (entry.type_uuid == IMPORT_RECORD_TYPE_UUID) {
        return role_error(
            local_id,
            "$record is reserved exclusively for the built-in ImportRecord type",
        );
    }
    let Some(index) = BOOTSTRAP_CONTROL_TYPE_UUIDS
        .iter()
        .position(|type_uuid| *type_uuid == entry.type_uuid)
    else {
        return Ok(());
    };
    if !entry.authoring_only {
        return role_error(local_id, "bootstrap control has invalid version or role");
    }
    if entry.type_uuid == DIRECTORY_IMPORT_RULES_TYPE_UUID && format_version != RULES_FORMAT_VERSION
    {
        return role_error(
            local_id,
            "DirectoryImportRules needs bundle format_version 2 (typed rule settings): convert this rules bundle",
        );
    }
    let expected = &BootstrapControlSpecV1::embedded()
        .map_err(|_| BundleError::EntryRole {
            local_id: local_id.to_owned(),
            detail: "embedded bootstrap spec is invalid",
        })?
        .0[index];
    let actual_schema = node_bytes(&schema.root).map_err(|_| BundleError::EntryRole {
        local_id: local_id.to_owned(),
        detail: "bootstrap logical schema is not canonical",
    })?;
    if entry.schema_hash != expected.logical_hash || actual_schema != expected.logical_schema {
        return role_error(local_id, "bootstrap schema hash does not match the bootstrap spec");
    }
    Ok(())
}

/// A bundle's `format_version` is the one its entries take
/// ([`format_version_for`]): one spelling per content.
pub(crate) fn check_format_version(
    format_version: u32,
    assets: &BTreeMap<String, AssetEntry>,
) -> Result<(), BundleError> {
    let expected = format_version_for(assets);
    if format_version != expected {
        return Err(BundleError::FormatVersionMismatch {
            found: format_version,
            expected,
        });
    }
    Ok(())
}

fn role_error<T>(local_id: &str, detail: &'static str) -> Result<T, BundleError> {
    Err(BundleError::EntryRole {
        local_id: local_id.to_owned(),
        detail,
    })
}

/// Write the envelope text. `data` holds each entry's canonical data text,
/// blob leaves already placed for the target encoding.
pub(crate) fn write(
    bundle: &Bundle,
    data: &BTreeMap<&str, String>,
) -> Result<String, BundleError> {
    use distill_json::write_str;
    let mut out = String::from("{\"assets\":{");
    for (i, (local_id, entry)) in bundle.assets.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_str(local_id, &mut out);
        out.push_str(":{\"authoring_only\":");
        out.push_str(if entry.authoring_only { "true" } else { "false" });
        out.push_str(",\"data\":");
        out.push_str(data.get(local_id.as_str()).ok_or_else(|| BundleError::Internal {
            detail: format!("writer lost data for entry {local_id:?}"),
        })?);
        out.push_str(",\"schema_hash\":");
        write_str(&entry.schema_hash.to_string(), &mut out);
        out.push_str(",\"type_uuid\":");
        write_str(&entry.type_uuid.to_string(), &mut out);
        out.push_str(",\"uuid\":");
        write_str(&entry.uuid.to_string(), &mut out);
        out.push('}');
    }
    out.push_str("},\"format_version\":");
    out.push_str(&bundle.format_version.to_string());
    if let Some(p) = &bundle.primary {
        out.push_str(",\"primary\":");
        write_str(p, &mut out);
    }
    out.push_str(",\"schemas\":{");
    for (i, (hash, schema)) in bundle.schemas.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_str(&hash.to_string(), &mut out);
        out.push(':');
        let text =
            snapshot_to_json(schema).map_err(|error| BundleError::Schema { hash: *hash, error })?;
        out.push_str(&text);
    }
    out.push('}');
    if !bundle.types.is_empty() {
        out.push_str(",\"types\":{");
        for (i, (path, ty)) in bundle.types.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_str(path, &mut out);
            out.push_str(":{\"schema_hash\":");
            write_str(&ty.schema_hash.to_string(), &mut out);
            out.push_str(",\"type_uuid\":");
            write_str(&ty.type_uuid.to_string(), &mut out);
            out.push('}');
        }
        out.push('}');
    }
    out.push_str(",\"uuid\":");
    write_str(&bundle.uuid.to_string(), &mut out);
    out.push('}');
    Ok(out)
}

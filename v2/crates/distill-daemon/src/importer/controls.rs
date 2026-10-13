//! Typed codecs for the importer's control values: a bundle's `$record`
//! (the bootstrap `ImportRecord`), directory-import rules (the bootstrap
//! `DirectoryImportRules`), and the store's watched-import failure basis.
//!
//! Values are positional (`ngp_schema::Value`): a struct's fields in node
//! order (names sorted), an enum's variant by index into its sorted
//! variant names. The name lists below are those orders.

use std::sync::OnceLock;

use distill_schema::ngp_schema::{
    parse_value, write_value, Options, PrimitiveKind, SchemaNode, Value, WriteOptions,
};

use super::*;

const FILE_DEP: &[&str] = &["Capability", "Listing", "Probe", "Read"];
const OBSERVED: &[&str] = &["Err", "Ok"];
const CAPABILITY: &[&str] = &["DefaultTable", "Importer", "MigrationFn", "Processor", "Tool"];
const GROUPING: &[&str] = &["ByStem", "PerFile"];
const FAILURE: &[&str] = &["Local", "MissingCapability", "RawFile"];
const SUBJECT: &[&str] = &["Path", "Query"];
/// The bootstrap rules' settings: a JSON tree spelled as an enum.
const SETTINGS_TREE: &[&str] = &[
    "Array", "Blob", "Bool", "Float", "Int", "Null", "Object", "Str", "UInt",
];

// ---- record ----------------------------------------------------------

pub(super) fn encode_import_record(record: &ImportRecord) -> Result<Value, RpcFailure> {
    Ok(record_value([
        ("importer", Value::String(record.importer.clone())),
        (
            "origin",
            Value::Option(record.origin.as_ref().map(|o| Box::new(encode_origin(o)))),
        ),
        (
            "read_set",
            Value::Seq(
                record
                    .read_set
                    .iter()
                    .map(|dep| encode_file_dep(dep, false))
                    .collect::<Result<_, _>>()?,
            ),
        ),
        ("settings", Value::String("$settings".into())),
        (
            "sources",
            Value::Seq(record.sources.iter().map(encode_rooted_path).collect()),
        ),
        ("watch", Value::Bool(record.watch)),
    ]))
}

pub(super) fn decode_import_record(value: &Value) -> Result<ImportRecord, RpcFailure> {
    let [importer, origin, read_set, settings, sources, watch] = fields(
        value,
        ["importer", "origin", "read_set", "settings", "sources", "watch"],
        "ImportRecord",
    )?;
    if string(settings, "ImportRecord.settings")? != "$settings" {
        return Err(invalid("ImportRecord.settings must name $settings"));
    }
    Ok(ImportRecord {
        importer: string(importer, "ImportRecord.importer")?.to_owned(),
        sources: seq(sources, "ImportRecord.sources")?
            .iter()
            .map(decode_rooted_path)
            .collect::<Result<_, _>>()?,
        watch: boolean(watch, "ImportRecord.watch")?,
        read_set: seq(read_set, "ImportRecord.read_set")?
            .iter()
            .map(|dep| decode_file_dep(dep, false))
            .collect::<Result<_, _>>()?,
        origin: option(origin, "ImportRecord.origin")?
            .map(decode_origin)
            .transpose()?,
    })
}

/// A read-set entry. `attempt`: the failure basis's spelling, where an
/// observation may be a failure; a committed record holds successes only.
fn encode_file_dep(dep: &FileDep, attempt: bool) -> Result<Value, RpcFailure> {
    let (name, payload) = match dep {
        FileDep::Read { path, observed } => (
            "Read",
            record_value([
                (
                    "observed",
                    encode_observed(observed, attempt, |value| {
                        record_value([
                            ("hash", byte_array(&value.hash)),
                            ("path", encode_rooted_path(&value.path)),
                        ])
                    })?,
                ),
                ("path", Value::String(path.clone())),
            ]),
        ),
        FileDep::Probe { path, observed } => (
            "Probe",
            record_value([
                (
                    "observed",
                    encode_observed(observed, attempt, |root| {
                        Value::Option(root.as_ref().map(|r| Box::new(Value::String(r.0.clone()))))
                    })?,
                ),
                ("path", Value::String(path.clone())),
            ]),
        ),
        FileDep::Listing { query, observed } => (
            "Listing",
            record_value([
                ("observed", encode_observed(observed, attempt, |h| byte_array(h))?),
                ("query", encode_file_query(query)),
            ]),
        ),
        FileDep::Capability { key, observed } => (
            "Capability",
            record_value([
                ("key", encode_capability_key(key)),
                ("observed", encode_observed(observed, attempt, |h| byte_array(h))?),
            ]),
        ),
    };
    Ok(variant(FILE_DEP, name, payload))
}

fn decode_file_dep(value: &Value, attempt: bool) -> Result<FileDep, RpcFailure> {
    let (name, payload) = variant_of(value, FILE_DEP, "FileDep")?;
    match name {
        "Read" => {
            let [observed, path] = payload_fields(payload, ["observed", "path"], "FileDep.Read")?;
            Ok(FileDep::Read {
                path: string(path, "FileDep.Read.path")?.to_owned(),
                observed: decode_observed(observed, attempt, "file content", |value| {
                    let [hash, path] = fields(value, ["hash", "path"], "FileContentObservation")?;
                    Ok(distill_build::import::FileContentObservation {
                        path: decode_rooted_path(path)?,
                        hash: bytes(hash, "FileContentObservation.hash")?,
                    })
                })?,
            })
        }
        "Probe" => {
            let [observed, path] = payload_fields(payload, ["observed", "path"], "FileDep.Probe")?;
            Ok(FileDep::Probe {
                path: string(path, "FileDep.Probe.path")?.to_owned(),
                observed: decode_observed(observed, attempt, "root probe", |value| {
                    option(value, "root probe value")?
                        .map(|root| {
                            RootName::new(string(root, "root probe value")?).map_err(invalid)
                        })
                        .transpose()
                })?,
            })
        }
        "Listing" => {
            let [observed, query] =
                payload_fields(payload, ["observed", "query"], "FileDep.Listing")?;
            Ok(FileDep::Listing {
                query: decode_file_query(query)?,
                observed: decode_observed(observed, attempt, "hash observation", |value| {
                    bytes(value, "hash observation value")
                })?,
            })
        }
        _ => {
            let [key, observed] =
                payload_fields(payload, ["key", "observed"], "FileDep.Capability")?;
            Ok(FileDep::Capability {
                key: decode_capability_key(key)?,
                observed: decode_observed(observed, attempt, "hash observation", |value| {
                    bytes(value, "hash observation value")
                })?,
            })
        }
    }
}

fn encode_observed<T>(
    observed: &Observed<T>,
    attempt: bool,
    ok: impl FnOnce(&T) -> Value,
) -> Result<Value, RpcFailure> {
    match observed {
        Observed::Ok(value) => Ok(variant(OBSERVED, "Ok", record_value([("value", ok(value))]))),
        Observed::Err(failure) if attempt => Ok(variant(
            OBSERVED,
            "Err",
            record_value([("failure", encode_import_failure(failure)?)]),
        )),
        Observed::Err(_) => Err(invalid("failed import observations cannot be committed")),
    }
}

fn decode_observed<T>(
    value: &Value,
    attempt: bool,
    context: &str,
    ok: impl FnOnce(&Value) -> Result<T, RpcFailure>,
) -> Result<Observed<T>, RpcFailure> {
    let (name, payload) = variant_of(value, OBSERVED, context)?;
    match name {
        "Ok" => {
            let [value] = payload_fields(payload, ["value"], context)?;
            ok(value).map(Observed::Ok)
        }
        _ if attempt => {
            let [failure] = payload_fields(payload, ["failure"], context)?;
            decode_import_failure(failure).map(Observed::Err)
        }
        _ => Err(invalid(format!(
            "{context} contains a failed observation; committed import records permit only successful observations"
        ))),
    }
}

fn encode_import_failure(failure: &StableFailureFingerprint) -> Result<Value, RpcFailure> {
    let (name, payload) = match failure {
        StableFailureFingerprint::RawFile { op, subject, class } => (
            "RawFile",
            record_value([
                ("class", Value::U8(*class as u8)),
                ("op", Value::U8(*op as u8)),
                (
                    "subject",
                    match subject {
                        RawFileSubject::Path(path) => variant(
                            SUBJECT,
                            "Path",
                            record_value([("path", Value::String(path.clone()))]),
                        ),
                        RawFileSubject::Query(query) => variant(
                            SUBJECT,
                            "Query",
                            record_value([("query", encode_file_query(query))]),
                        ),
                    },
                ),
            ]),
        ),
        StableFailureFingerprint::MissingCapability { key } => (
            "MissingCapability",
            record_value([("key", encode_capability_key(key))]),
        ),
        StableFailureFingerprint::Local { class, detail } => (
            "Local",
            record_value([
                ("class", Value::U16(*class as u16)),
                ("detail", byte_array(detail)),
            ]),
        ),
        _ => {
            return Err(invalid(
                "failure fingerprint is not valid in an authoring import basis",
            ));
        }
    };
    Ok(variant(FAILURE, name, payload))
}

fn decode_import_failure(value: &Value) -> Result<StableFailureFingerprint, RpcFailure> {
    let (name, payload) = variant_of(value, FAILURE, "import failure fingerprint")?;
    match name {
        "RawFile" => {
            let [class, op, subject] =
                payload_fields(payload, ["class", "op", "subject"], "RawFile")?;
            let op = match uint(op, "RawFile.op")? {
                0 => RawFileOp::Read,
                1 => RawFileOp::Probe,
                2 => RawFileOp::Enumerate,
                value => return Err(invalid(format!("unknown raw-file op {value}"))),
            };
            let class = match uint(class, "RawFile.class")? {
                0 => RawFileFailureClass::NotFound,
                1 => RawFileFailureClass::PermissionDenied,
                2 => RawFileFailureClass::ListingFailed,
                3 => RawFileFailureClass::OtherStable,
                value => return Err(invalid(format!("unknown raw-file failure class {value}"))),
            };
            let (kind, subject) = variant_of(subject, SUBJECT, "RawFile.subject")?;
            let subject = match kind {
                "Path" => {
                    let [path] = payload_fields(subject, ["path"], "RawFile.Path")?;
                    RawFileSubject::Path(string(path, "RawFile.Path")?.to_owned())
                }
                _ => {
                    let [query] = payload_fields(subject, ["query"], "RawFile.Query")?;
                    RawFileSubject::Query(decode_file_query(query)?)
                }
            };
            Ok(StableFailureFingerprint::RawFile { op, subject, class })
        }
        "MissingCapability" => {
            let [key] = payload_fields(payload, ["key"], "MissingCapability")?;
            Ok(StableFailureFingerprint::MissingCapability {
                key: decode_capability_key(key)?,
            })
        }
        _ => {
            let [class, detail] = payload_fields(payload, ["class", "detail"], "Local")?;
            let class = match uint(class, "Local.class")? {
                1 => LocalFailureClass::Validator,
                2 => LocalFailureClass::MigrationPlan,
                3 => LocalFailureClass::Processor,
                4 => LocalFailureClass::MigrationFunction,
                5 => LocalFailureClass::OutputBinding,
                6 => LocalFailureClass::Importer,
                7 => LocalFailureClass::ImportIntake,
                8 => LocalFailureClass::ArtifactEncoding,
                value => return Err(invalid(format!("unknown local failure class {value}"))),
            };
            Ok(StableFailureFingerprint::Local {
                class,
                detail: bytes(detail, "Local.detail")?,
            })
        }
    }
}

fn encode_capability_key(key: &CapabilityKey) -> Value {
    let (name, payload) = match key {
        CapabilityKey::MigrationFn(key) => (
            "MigrationFn",
            record_value([("key", Value::String(key.clone()))]),
        ),
        CapabilityKey::DefaultTable(type_uuid) => (
            "DefaultTable",
            record_value([("type_uuid", byte_array(&type_uuid.0))]),
        ),
        CapabilityKey::Importer(id) => ("Importer", record_value([("id", Value::String(id.clone()))])),
        CapabilityKey::Processor { input } => {
            ("Processor", record_value([("input", byte_array(&input.0))]))
        }
        CapabilityKey::Tool(id) => ("Tool", record_value([("id", Value::String(id.clone()))])),
    };
    variant(CAPABILITY, name, payload)
}

fn decode_capability_key(value: &Value) -> Result<CapabilityKey, RpcFailure> {
    let (name, payload) = variant_of(value, CAPABILITY, "CapabilityKey")?;
    let [field] = match payload {
        [field] => [field],
        _ => return Err(invalid("CapabilityKey payload must hold one field")),
    };
    Ok(match name {
        "MigrationFn" => CapabilityKey::MigrationFn(string(field, "MigrationFn.key")?.to_owned()),
        "DefaultTable" => {
            CapabilityKey::DefaultTable(TypeUuid(bytes(field, "DefaultTable.type_uuid")?))
        }
        "Importer" => CapabilityKey::Importer(string(field, "Importer.id")?.to_owned()),
        "Processor" => CapabilityKey::Processor {
            input: TypeUuid(bytes(field, "Processor.input")?),
        },
        _ => CapabilityKey::Tool(string(field, "Tool.id")?.to_owned()),
    })
}

fn encode_file_query(query: &FileQuery) -> Value {
    record_value([
        ("path_glob", optional_string(query.path_glob.as_ref())),
        ("path_prefix", optional_string(query.path_prefix.as_ref())),
    ])
}

fn decode_file_query(value: &Value) -> Result<FileQuery, RpcFailure> {
    let [glob, prefix] = fields(value, ["path_glob", "path_prefix"], "FileQuery")?;
    let text = |value: &Value, name: &str| -> Result<Option<String>, RpcFailure> {
        option(value, name)?
            .map(|value| string(value, name).map(str::to_owned))
            .transpose()
    };
    FileQuery::new(text(prefix, "path_prefix")?, text(glob, "path_glob")?).map_err(invalid)
}

fn encode_origin(origin: &DirectoryOrigin) -> Value {
    record_value([
        ("group", encode_rooted_path(&origin.group)),
        ("rule", byte_array(&origin.rule.0)),
        ("rules_bundle", byte_array(&origin.rules_bundle.0)),
    ])
}

fn decode_origin(value: &Value) -> Result<DirectoryOrigin, RpcFailure> {
    let [group, rule, rules_bundle] =
        fields(value, ["group", "rule", "rules_bundle"], "DirectoryOrigin")?;
    Ok(DirectoryOrigin {
        rules_bundle: BundleUuid(bytes(rules_bundle, "rules_bundle")?),
        rule: distill_build::import::ImportRuleId(bytes(rule, "rule")?),
        group: decode_rooted_path(group)?,
    })
}

fn encode_rooted_path(path: &RootedPath) -> Value {
    record_value([
        ("path", Value::String(path.path.clone())),
        ("root", Value::String(path.root.0.clone())),
    ])
}

fn decode_rooted_path(value: &Value) -> Result<RootedPath, RpcFailure> {
    let [path, root] = fields(value, ["path", "root"], "RootedPath")?;
    RootedPath::new(
        string(root, "RootedPath.root")?,
        string(path, "RootedPath.path")?,
    )
    .map_err(invalid)
}

// ---- failure basis ---------------------------------------------------

/// The watched-import failure basis: `{read_set, version}`, a read set
/// whose observations may be failures. Store state, written canonically
/// against [`basis_schema`].
pub(super) fn encode_attempt_basis(read_set: &[FileDep]) -> Result<Vec<u8>, RpcFailure> {
    let value = record_value([
        (
            "read_set",
            Value::Seq(
                read_set
                    .iter()
                    .map(|dep| encode_file_dep(dep, true))
                    .collect::<Result<_, _>>()?,
            ),
        ),
        ("version", Value::U32(1)),
    ]);
    write_value(basis_schema(), &value, WriteOptions::default())
        .map(String::into_bytes)
        .map_err(invalid)
}

pub(super) fn decode_attempt_basis(bytes: &[u8]) -> Result<Vec<FileDep>, RpcFailure> {
    let text = std::str::from_utf8(bytes).map_err(invalid)?;
    let value = parse_value(text, basis_schema(), Options::canonical()).map_err(invalid)?;
    if write_value(basis_schema(), &value, WriteOptions::default()).map_err(invalid)? != text {
        return Err(invalid(
            "watched import failure basis is not canonical JSON",
        ));
    }
    let [read_set, version] = fields(&value, ["read_set", "version"], "failure basis")?;
    if uint(version, "basis.version")? != 1 {
        return Err(invalid("unsupported watched import failure basis"));
    }
    seq(read_set, "basis.read_set")?
        .iter()
        .map(|dep| decode_file_dep(dep, true))
        .collect()
}

fn basis_schema() -> &'static SchemaNode {
    static SCHEMA: OnceLock<SchemaNode> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        let string = || SchemaNode::String;
        let optional = |node| SchemaNode::Option(Box::new(node));
        let bytes = |len| SchemaNode::Array {
            len,
            elem: Box::new(SchemaNode::Primitive(PrimitiveKind::U8)),
        };
        let rooted = || node_struct(vec![("path", string()), ("root", string())]);
        let query = || {
            node_struct(vec![
                ("path_glob", optional(string())),
                ("path_prefix", optional(string())),
            ])
        };
        let capability = || {
            node_enum(vec![
                ("DefaultTable", node_struct(vec![("type_uuid", bytes(16))])),
                ("Importer", node_struct(vec![("id", string())])),
                ("MigrationFn", node_struct(vec![("key", string())])),
                ("Processor", node_struct(vec![("input", bytes(16))])),
                ("Tool", node_struct(vec![("id", string())])),
            ])
        };
        let failure = || {
            node_enum(vec![
                (
                    "Local",
                    node_struct(vec![
                        ("class", SchemaNode::Primitive(PrimitiveKind::U16)),
                        ("detail", bytes(32)),
                    ]),
                ),
                ("MissingCapability", node_struct(vec![("key", capability())])),
                (
                    "RawFile",
                    node_struct(vec![
                        ("class", SchemaNode::Primitive(PrimitiveKind::U8)),
                        ("op", SchemaNode::Primitive(PrimitiveKind::U8)),
                        (
                            "subject",
                            node_enum(vec![
                                ("Path", node_struct(vec![("path", string())])),
                                ("Query", node_struct(vec![("query", query())])),
                            ]),
                        ),
                    ]),
                ),
            ])
        };
        let attempt = |ok| {
            node_enum(vec![
                ("Err", node_struct(vec![("failure", failure())])),
                ("Ok", node_struct(vec![("value", ok)])),
            ])
        };
        let dep = node_enum(vec![
            (
                "Capability",
                node_struct(vec![("key", capability()), ("observed", attempt(bytes(32)))]),
            ),
            (
                "Listing",
                node_struct(vec![("observed", attempt(bytes(32))), ("query", query())]),
            ),
            (
                "Probe",
                node_struct(vec![
                    ("observed", attempt(optional(string()))),
                    ("path", string()),
                ]),
            ),
            (
                "Read",
                node_struct(vec![
                    (
                        "observed",
                        attempt(node_struct(vec![("hash", bytes(32)), ("path", rooted())])),
                    ),
                    ("path", string()),
                ]),
            ),
        ]);
        node_struct(vec![
            ("read_set", SchemaNode::Vec(Box::new(dep))),
            ("version", SchemaNode::Primitive(PrimitiveKind::U32)),
        ])
    })
}

fn node_struct(fields: Vec<(&str, SchemaNode)>) -> SchemaNode {
    SchemaNode::Struct {
        rev: 0,
        fields: fields
            .into_iter()
            .map(|(name, node)| (name.to_owned(), 0, node))
            .collect(),
    }
}

fn node_enum(variants: Vec<(&str, SchemaNode)>) -> SchemaNode {
    SchemaNode::Enum {
        rev: 0,
        variants: variants
            .into_iter()
            .map(|(name, node)| (name.to_owned(), 0, node))
            .collect(),
    }
}

// ---- directory rules -------------------------------------------------

pub(super) fn decode_directory_rules(value: &Value) -> Result<DecodedDirectoryRules, RpcFailure> {
    let [listing, rules] = fields(value, ["listing", "rules"], "DirectoryImportRules")?;
    let listing = decode_file_query(listing)?;
    let rules = seq(rules, "DirectoryImportRules.rules")?
        .iter()
        .map(|value| {
            let [group, id, importer, matches, output, settings] = fields(
                value,
                ["group", "id", "importer", "matches", "output", "settings"],
                "ImportRule",
            )?;
            let mut text = String::new();
            settings_text(settings, 0, &mut text)?;
            Ok(DecodedDirectoryRule {
                id: distill_build::import::ImportRuleId(bytes(id, "ImportRule.id")?),
                matches: decode_file_query(matches)?,
                group: decode_directory_grouping(group)?,
                importer: normalize_identifier(string(importer, "ImportRule.importer")?)
                    .map_err(invalid)?,
                settings: text,
                output: string(output, "ImportRule.output")?.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, RpcFailure>>()?;
    if rules.is_empty() {
        return Err(invalid("DirectoryImportRules.rules cannot be empty"));
    }
    Ok(DecodedDirectoryRules { listing, rules })
}

fn decode_directory_grouping(value: &Value) -> Result<DirectoryGrouping, RpcFailure> {
    let (name, payload) = variant_of(value, GROUPING, "Grouping")?;
    if !payload.is_empty() {
        return Err(invalid("Grouping unit payload must be empty"));
    }
    Ok(match name {
        "PerFile" => DirectoryGrouping::PerFile,
        _ => DirectoryGrouping::ByStem,
    })
}

/// A rule's settings as JSON text: the bootstrap type spells them as a
/// JSON tree (`{"Object":{"value":{"k":{"Bool":{"value":true}}}}}`), which
/// the daemon parses against the rule's importer settings schema.
fn settings_text(value: &Value, depth: usize, out: &mut String) -> Result<(), RpcFailure> {
    if depth >= 512 {
        return Err(invalid("rule settings exceed the nesting limit"));
    }
    let (name, payload) = variant_of(value, SETTINGS_TREE, "rule settings")?;
    let leaf = || -> Result<&Value, RpcFailure> {
        match payload {
            [value] => Ok(value),
            _ => Err(invalid(format!("rule settings {name} must hold one value"))),
        }
    };
    match name {
        "Null" => out.push_str("null"),
        "Bool" => out.push_str(if boolean(leaf()?, "Bool")? { "true" } else { "false" }),
        "Int" | "UInt" => {
            let value = leaf()?;
            match value {
                Value::I128(v) => out.push_str(&v.to_string()),
                Value::U128(v) => out.push_str(&v.to_string()),
                _ => return Err(invalid("rule settings integer has the wrong kind")),
            }
        }
        "Float" => {
            let Value::F64(value) = leaf()? else {
                return Err(invalid("rule settings float must be f64"));
            };
            distill_json::write_f64(*value, out).map_err(invalid)?;
        }
        "Str" => distill_json::write_str(string(leaf()?, "Str")?, out),
        "Array" => {
            out.push('[');
            for (index, item) in seq(leaf()?, "Array")?.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                settings_text(item, depth + 1, out)?;
            }
            out.push(']');
        }
        "Object" => {
            let Value::Map(entries) = leaf()? else {
                return Err(invalid("rule settings object must be a map"));
            };
            out.push('{');
            for (index, (key, item)) in entries.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                distill_json::write_str(string(key, "Object key")?, out);
                out.push(':');
                settings_text(item, depth + 1, out)?;
            }
            out.push('}');
        }
        _ => return Err(invalid("rule settings cannot hold blobs")),
    }
    Ok(())
}

// ---- helpers ---------------------------------------------------------

/// A struct value from its fields in any order: node order is name order.
fn record_value<const N: usize>(mut fields: [(&str, Value); N]) -> Value {
    fields.sort_by(|a, b| a.0.cmp(b.0));
    Value::Struct(fields.into_iter().map(|(_, value)| value).collect())
}

/// Enum value `name` of the sorted `variants`, its payload a struct value.
fn variant(variants: &[&str], name: &str, payload: Value) -> Value {
    let index = variants
        .iter()
        .position(|candidate| *candidate == name)
        .expect("a declared variant");
    let Value::Struct(payload) = payload else {
        unreachable!("variant payloads are struct values")
    };
    Value::Enum {
        variant: index as u32,
        payload,
    }
}

/// The fields of a struct value, `names` in node (sorted) order.
fn fields<'a, const N: usize>(
    value: &'a Value,
    names: [&str; N],
    context: &str,
) -> Result<[&'a Value; N], RpcFailure> {
    match value {
        Value::Struct(values) => payload_fields(values, names, context),
        _ => Err(invalid(format!("{context} must be a struct"))),
    }
}

fn payload_fields<'a, const N: usize>(
    values: &'a [Value],
    names: [&str; N],
    context: &str,
) -> Result<[&'a Value; N], RpcFailure> {
    debug_assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
    if values.len() != N {
        return Err(invalid(format!("{context} must hold {N} fields")));
    }
    Ok(std::array::from_fn(|index| &values[index]))
}

/// An enum value's variant name (of the sorted `variants`) and payload.
fn variant_of<'a>(
    value: &'a Value,
    variants: &[&'static str],
    context: &str,
) -> Result<(&'static str, &'a [Value]), RpcFailure> {
    match value {
        Value::Enum { variant, payload } => variants
            .get(*variant as usize)
            .map(|name| (*name, &payload[..]))
            .ok_or_else(|| invalid(format!("{context} names an unknown variant"))),
        _ => Err(invalid(format!("{context} must be an enum"))),
    }
}

fn string<'a>(value: &'a Value, context: &str) -> Result<&'a str, RpcFailure> {
    value
        .as_str()
        .ok_or_else(|| invalid(format!("{context} must be text")))
}

fn boolean(value: &Value, context: &str) -> Result<bool, RpcFailure> {
    value
        .as_bool()
        .ok_or_else(|| invalid(format!("{context} must be a boolean")))
}

fn uint(value: &Value, context: &str) -> Result<u128, RpcFailure> {
    value
        .as_u128()
        .ok_or_else(|| invalid(format!("{context} must be an unsigned integer")))
}

fn seq<'a>(value: &'a Value, context: &str) -> Result<&'a [Value], RpcFailure> {
    match value {
        Value::Seq(items) => Ok(items),
        _ => Err(invalid(format!("{context} must be a sequence"))),
    }
}

fn option<'a>(value: &'a Value, context: &str) -> Result<Option<&'a Value>, RpcFailure> {
    match value {
        Value::Option(inner) => Ok(inner.as_deref()),
        _ => Err(invalid(format!("{context} must be an option"))),
    }
}

fn bytes<const N: usize>(value: &Value, context: &str) -> Result<[u8; N], RpcFailure> {
    let items = seq(value, context)?;
    if items.len() != N {
        return Err(invalid(format!("{context} must contain exactly {N} bytes")));
    }
    let mut out = [0; N];
    for (target, item) in out.iter_mut().zip(items) {
        let Value::U8(byte) = item else {
            return Err(invalid(format!("{context} contains a non-byte value")));
        };
        *target = *byte;
    }
    Ok(out)
}

fn byte_array(bytes: &[u8]) -> Value {
    Value::Seq(bytes.iter().map(|byte| Value::U8(*byte)).collect())
}

fn optional_string(value: Option<&String>) -> Value {
    Value::Option(value.map(|value| Box::new(Value::String(value.clone()))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_build::import::{FileContentObservation, ImportRuleId};

    fn rooted(root: &str, path: &str) -> RootedPath {
        RootedPath::new(root, path).unwrap()
    }

    fn query(prefix: Option<&str>, glob: Option<&str>) -> FileQuery {
        FileQuery::new(prefix.map(str::to_owned), glob.map(str::to_owned)).unwrap()
    }

    fn record_schema() -> SchemaNode {
        let spec = BootstrapControlSpecV1::embedded().unwrap();
        let row = spec
            .0
            .iter()
            .find(|row| row.symbol == BootstrapControlSymbol::ImportRecord)
            .unwrap();
        distill_schema::ngp_schema::node_from_bytes(&row.logical_schema)
            .unwrap()
            .root
    }

    /// One of each dependency kind, every observation a success.
    fn successful_read_set() -> Vec<FileDep> {
        vec![
            FileDep::Read {
                path: "models/chest.glb".into(),
                observed: Observed::Ok(FileContentObservation {
                    path: rooted("assets", "models/chest.glb"),
                    hash: [3; 32],
                }),
            },
            FileDep::Probe {
                path: "textures".into(),
                observed: Observed::Ok(Some(RootName::new("assets").unwrap())),
            },
            FileDep::Probe {
                path: "missing".into(),
                observed: Observed::Ok(None),
            },
            FileDep::Listing {
                query: query(Some("models"), Some("**/*.png")),
                observed: Observed::Ok([5; 32]),
            },
            FileDep::Capability {
                key: CapabilityKey::Importer("ngp.gltf".into()),
                observed: Observed::Ok([7; 32]),
            },
            FileDep::Capability {
                key: CapabilityKey::DefaultTable(TypeUuid([9; 16])),
                observed: Observed::Ok([11; 32]),
            },
            FileDep::Capability {
                key: CapabilityKey::Processor {
                    input: TypeUuid([13; 16]),
                },
                observed: Observed::Ok([15; 32]),
            },
            FileDep::Capability {
                key: CapabilityKey::MigrationFn("mesh.v2".into()),
                observed: Observed::Ok([17; 32]),
            },
            FileDep::Capability {
                key: CapabilityKey::Tool("ktx".into()),
                observed: Observed::Ok([19; 32]),
            },
        ]
    }

    #[test]
    fn an_import_record_round_trips_through_the_bootstrap_schema() {
        let schema = record_schema();
        for origin in [
            None,
            Some(DirectoryOrigin {
                rules_bundle: BundleUuid([1; 16]),
                rule: ImportRuleId([2; 16]),
                group: rooted("assets", "models/chest"),
            }),
        ] {
            let record = ImportRecord {
                importer: "ngp.gltf".into(),
                sources: vec![
                    rooted("assets", "models/chest.glb"),
                    rooted("assets", "models/chest.bin"),
                ],
                watch: true,
                read_set: successful_read_set(),
                origin,
            };
            let value = encode_import_record(&record).unwrap();
            let text = write_value(&schema, &value, WriteOptions::default()).unwrap();
            let parsed = parse_value(&text, &schema, Options::canonical()).unwrap();
            assert_eq!(decode_import_record(&parsed).unwrap(), record, "{text}");
        }
    }

    #[test]
    fn a_committed_import_record_refuses_a_failed_observation() {
        let record = ImportRecord {
            importer: "ngp.gltf".into(),
            sources: vec![rooted("assets", "a.glb")],
            watch: false,
            read_set: vec![FileDep::Probe {
                path: "a.glb".into(),
                observed: Observed::Err(StableFailureFingerprint::Local {
                    class: LocalFailureClass::Importer,
                    detail: [0; 32],
                }),
            }],
            origin: None,
        };
        assert!(encode_import_record(&record).is_err());
    }

    #[test]
    fn an_attempt_basis_round_trips_every_failure_kind() {
        let mut read_set = successful_read_set();
        read_set.extend([
            FileDep::Read {
                path: "a.glb".into(),
                observed: Observed::Err(StableFailureFingerprint::Local {
                    class: LocalFailureClass::ArtifactEncoding,
                    detail: [21; 32],
                }),
            },
            FileDep::Capability {
                key: CapabilityKey::Importer("ngp.image".into()),
                observed: Observed::Err(StableFailureFingerprint::MissingCapability {
                    key: CapabilityKey::Importer("ngp.image".into()),
                }),
            },
            FileDep::Read {
                path: "b.png".into(),
                observed: Observed::Err(StableFailureFingerprint::RawFile {
                    op: RawFileOp::Read,
                    subject: RawFileSubject::Path("b.png".into()),
                    class: RawFileFailureClass::PermissionDenied,
                }),
            },
            FileDep::Probe {
                path: "c".into(),
                observed: Observed::Err(StableFailureFingerprint::RawFile {
                    op: RawFileOp::Probe,
                    subject: RawFileSubject::Path("c".into()),
                    class: RawFileFailureClass::NotFound,
                }),
            },
            FileDep::Listing {
                query: query(None, Some("**/*.glb")),
                observed: Observed::Err(StableFailureFingerprint::RawFile {
                    op: RawFileOp::Enumerate,
                    subject: RawFileSubject::Query(query(None, Some("**/*.glb"))),
                    class: RawFileFailureClass::ListingFailed,
                }),
            },
            FileDep::Listing {
                query: query(Some("d"), None),
                observed: Observed::Err(StableFailureFingerprint::RawFile {
                    op: RawFileOp::Enumerate,
                    subject: RawFileSubject::Query(query(Some("d"), None)),
                    class: RawFileFailureClass::OtherStable,
                }),
            },
        ]);
        let bytes = encode_attempt_basis(&read_set).unwrap();
        assert_eq!(decode_attempt_basis(&bytes).unwrap(), read_set);

        // Only the canonical spelling decodes.
        let mut spaced = b" ".to_vec();
        spaced.extend_from_slice(&bytes);
        assert!(decode_attempt_basis(&spaced).is_err());
    }

    #[test]
    fn a_failure_outside_the_authoring_vocabulary_is_refused_in_a_basis() {
        let read_set = [FileDep::Read {
            path: "a.glb".into(),
            observed: Observed::Err(StableFailureFingerprint::Poisoned {
                bundle: BundleUuid([1; 16]),
            }),
        }];
        assert!(encode_attempt_basis(&read_set).is_err());
    }

    fn tree(name: &str, fields: Vec<Value>) -> Value {
        variant(SETTINGS_TREE, name, Value::Struct(fields.into()))
    }

    fn render(value: &Value) -> Result<String, RpcFailure> {
        let mut text = String::new();
        settings_text(value, 0, &mut text).map(|()| text)
    }

    #[test]
    fn rule_settings_render_their_json_tree_as_text() {
        let settings = tree(
            "Object",
            vec![Value::Map(
                vec![
                    (
                        Value::String("flag".into()),
                        tree("Bool", vec![Value::Bool(true)]),
                    ),
                    (
                        Value::String("list".into()),
                        tree(
                            "Array",
                            vec![Value::Seq(
                                vec![
                                    tree("Int", vec![Value::I128(-4)]),
                                    tree("UInt", vec![Value::U128(u128::MAX)]),
                                    tree("Float", vec![Value::F64(1.5)]),
                                    tree("Str", vec![Value::String("a\"b".into())]),
                                    tree("Null", vec![]),
                                ]
                                .into(),
                            )],
                        ),
                    ),
                    (
                        Value::String("nested".into()),
                        tree("Object", vec![Value::Map(Vec::new().into())]),
                    ),
                ]
                .into(),
            )],
        );
        assert_eq!(
            render(&settings).unwrap(),
            format!(
                r#"{{"flag":true,"list":[-4,{},1.5,"a\"b",null],"nested":{{}}}}"#,
                u128::MAX
            )
        );
    }

    #[test]
    fn rule_settings_refuse_blobs_and_mismatched_leaves() {
        assert!(render(&tree("Blob", vec![Value::Blob(vec![1, 2].into())])).is_err());
        assert!(render(&tree("Float", vec![Value::F32(1.0)])).is_err());
        assert!(render(&tree("Int", vec![Value::U8(1)])).is_err());
        assert!(render(&tree("Bool", vec![])).is_err());
    }
}

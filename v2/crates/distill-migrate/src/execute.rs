//! The typed executor (§11). Execution semantics are normative, not
//! executor-defined: every op reads the edge's IMMUTABLE input value and
//! writes into a fresh output — no op ever observes another op's writes,
//! so op order can never change the result. `CopyField` copies; the
//! source stays readable to later ops.
//!
//! Values are positional ([`Value`]): ops address struct fields by name,
//! resolved against the frame's from- and to-schemas. [`execute_sparse`]
//! runs the same ops over a [`Patch`].

use crate::conform::{conforms, ConformError};
use crate::validate::{validate_plan, EdgeKind, PlanError};
use crate::{FieldPath, MigrationKind, MigrationOp};
use ngp_schema::{sort_map, sort_set, Frames, Patch, PrimitiveKind, SchemaNode, Value};
use std::collections::BTreeMap;
use std::fmt;

/// An edge's execution result: the output value plus op provenance —
/// every path a default-table op materialized (diagnostics, §11).
#[derive(Clone, Debug, PartialEq)]
pub struct Exec {
    pub value: Value,
    pub defaulted: Vec<FieldPath>,
}

/// Materializes defaults for the automatic segment by calling into the
/// pipeline module (§11). Returning `None` FAILS the migration HARD —
/// integrity forbids fabricated values.
///
/// PINNED: `to_schema` is the executing FRAME's to-schema root and `at`
/// the frame-relative field path (resolvable via [`crate::resolve_path`]);
/// nested frames (elements, variant payloads, inline structs) pass their
/// own roots, so the provider always sees a resolvable pair.
pub trait DefaultProvider {
    /// The field type's default.
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value>;
    /// The field's value taken from the parent type's `Default`.
    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value>;
}

/// Runs registered migration functions (§3 module registry): a value of
/// the edge's from-schema to one of its to-schema. An unknown key is an
/// `Err` — there is no fallback.
pub trait FnProvider {
    fn run(&self, key: &str, v: Value) -> Result<Value, String>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum MigrationError {
    /// execute_edge validates Ops plans (Custom edge kind) before running.
    PlanInvalid(Vec<PlanError>),
    /// The input value has the wrong shape at a path (corrupt input).
    InputShape {
        path: String,
        expected: String,
    },
    /// A widened value does not fit the new leaf (corrupt input).
    WidenOutOfRange {
        path: String,
        kind: String,
    },
    /// The DefaultProvider returned None — fail hard, no fabricated values.
    MissingDefault {
        path: String,
        op: &'static str,
    },
    /// Set rebuild found equal elements post-migration (canonical order
    /// bytes) — exactly the map-key collision rule.
    DuplicateSetElement {
        path: String,
    },
    /// MigrateMapKeys produced colliding keys.
    MapKeyCollision {
        path: String,
    },
    /// Two ops wrote one output path (an unvalidated plan).
    DuplicateWrite {
        path: String,
    },
    /// No op wrote an output leaf (an unvalidated plan).
    MissingWrite {
        path: String,
    },
    /// The plan does not fit the schemas (an unvalidated plan).
    PlanShape {
        path: String,
        detail: String,
    },
    DuplicateMapVariantFrom {
        at: String,
        from: String,
    },
    /// The function edge failed (or its key is unregistered).
    FunctionFailed {
        key: String,
        message: String,
    },
    /// An edge's output failed to-schema conformance, naming the edge
    /// (§11: surfaced before tag extraction, validators, or artifact
    /// encoding can consume the value).
    NonConforming {
        edge: String,
        error: ConformError,
    },
    /// Canonical encoding failed where ordering/equality needed it.
    Unencodable {
        path: String,
    },
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MigrationError::*;
        match self {
            PlanInvalid(errs) => {
                writeln!(f, "plan validation failed:")?;
                for e in errs {
                    writeln!(f, "  {e}")?;
                }
                Ok(())
            }
            InputShape { path, expected } => {
                write!(f, "input value at {path}: expected {expected}")
            }
            WidenOutOfRange { path, kind } => write!(
                f,
                "value at {path} does not fit the new {kind} leaf (corrupt input)"
            ),
            MissingDefault { path, op } => write!(
                f,
                "{op} at {path}: no default available — integrity forbids fabricated values"
            ),
            DuplicateSetElement { path } => {
                write!(f, "set at {path}: post-migration duplicate element")
            }
            MapKeyCollision { path } => write!(f, "map at {path}: post-migration key collision"),
            DuplicateWrite { path } => write!(f, "two ops wrote output path {path}"),
            MissingWrite { path } => write!(f, "no op wrote output leaf {path}"),
            PlanShape { path, detail } => write!(f, "plan/schema mismatch at {path}: {detail}"),
            DuplicateMapVariantFrom { at, from } => write!(
                f,
                "two MapVariant ops at {at} name the same input variant {from:?}"
            ),
            FunctionFailed { key, message } => {
                write!(f, "migration function {key:?} failed: {message}")
            }
            NonConforming { edge, error } => write!(
                f,
                "edge {edge} produced a value that does not conform to its to-schema: {error}"
            ),
            Unencodable { path } => write!(f, "value at {path} has no canonical encoding"),
        }
    }
}

impl std::error::Error for MigrationError {}

/// Execute an Ops plan: input (immutable) + endpoint schemas → fresh
/// output. Does NOT validate the plan first — [`execute_edge`] does;
/// unvalidated plans surface best-effort `PlanShape`/write errors.
///
/// Normative model (§11): each op computes its output value(s) from the
/// immutable input, keyed by to-path; the output is then materialized by
/// walking the to-schema — so op order can never change the result, and
/// two ops writing one path is a definite error, never last-write-wins.
pub fn execute_ops(
    ops: &[MigrationOp],
    input: &Value,
    from: &SchemaNode,
    to: &SchemaNode,
    defaults: &dyn DefaultProvider,
) -> Result<Exec, MigrationError> {
    let mut defaulted = Vec::new();
    let value = exec_frame(ops, input, from, to, defaults, &[], &mut defaulted)?;
    Ok(Exec { value, defaulted })
}

struct NoDefaults;

impl DefaultProvider for NoDefaults {
    fn field_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<Value> {
        None
    }
    fn parent_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<Value> {
        None
    }
}

/// Execute an automatic plan over a sparse input: a [`Patch`] holding
/// only some of `from`'s struct fields, at any depth (absent = the
/// consumer's default). Ops reading an absent path are skipped, default
/// writes (`WriteFieldDefault`, `WriteParentDefault`, `WriteNone`) are not
/// materialized, and the output holds exactly the fields the remaining
/// ops wrote, so a renamed field follows its field and a dropped one
/// disappears. Whole values inside the patch (`Patch::Replace`) migrate
/// as values, with no default table. The output is not checked for
/// conformance.
pub fn execute_sparse(
    ops: &[MigrationOp],
    input: &Patch,
    from: &SchemaNode,
    to: &SchemaNode,
) -> Result<Patch, MigrationError> {
    sparse_frame(ops, input, from, to, &[])
}

/// Execute one custom edge: Ops plans are validated (EdgeKind::Custom —
/// this is the bundle-authored segment; the automatic segment validates
/// with EdgeKind::Automatic in its own driver) then executed; Function
/// edges run through the FnProvider. EVERY edge's output — function
/// edges included — is validated against the `to` schema before
/// returning (§11: surfaced before tag extraction, validators, or
/// artifact encoding can consume the value).
pub fn execute_edge(
    kind: &MigrationKind,
    input: &Value,
    from: &SchemaNode,
    to: &SchemaNode,
    defaults: &dyn DefaultProvider,
    fns: &dyn FnProvider,
) -> Result<Exec, MigrationError> {
    let (exec, edge_name) = match kind {
        MigrationKind::Ops(ops) => {
            validate_plan(ops, from, to, EdgeKind::Custom).map_err(MigrationError::PlanInvalid)?;
            (
                execute_ops(ops, input, from, to, defaults)?,
                "ops".to_string(),
            )
        }
        MigrationKind::Function(key) => {
            let out =
                fns.run(key, input.clone())
                    .map_err(|message| MigrationError::FunctionFailed {
                        key: key.clone(),
                        message,
                    })?;
            (
                Exec {
                    value: out,
                    defaulted: Vec::new(),
                },
                format!("fn {key:?}"),
            )
        }
    };
    conforms(&exec.value, to).map_err(|error| MigrationError::NonConforming {
        edge: edge_name,
        error,
    })?;
    Ok(exec)
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// Display a full diagnostic path: prefix pseudo-segments plus a
/// frame-relative field path.
fn dpath(prefix: &[String], p: &FieldPath) -> String {
    let mut s = "$".to_string();
    for seg in prefix.iter().chain(&p.0) {
        s.push('.');
        s.push_str(seg);
    }
    s
}

/// A defaulted-path record: full pseudo path (prefix + frame-relative
/// segments). PINNED: element frames contribute `[i]`, map-value frames
/// `[i]`, variant payload frames `{Variant}` — diagnostic pseudo-segments,
/// not resolvable FieldPaths.
fn full_path(prefix: &[String], p: &FieldPath) -> FieldPath {
    FieldPath(prefix.iter().cloned().chain(p.0.iter().cloned()).collect())
}

fn child_prefix(prefix: &[String], at: &FieldPath, pseudo: Option<String>) -> Vec<String> {
    let mut p: Vec<String> = prefix.to_vec();
    p.extend(at.0.iter().cloned());
    if let Some(s) = pseudo {
        p.push(s);
    }
    p
}

fn field_index<'s>(node: &'s SchemaNode, name: &str) -> Option<(usize, &'s SchemaNode)> {
    let SchemaNode::Struct { fields, .. } = node else {
        return None;
    };
    fields
        .iter()
        .position(|(n, _, _)| n == name)
        .map(|i| (i, &fields[i].2))
}

/// The value at `path` of `value` (of `schema`), if the path resolves.
pub(crate) fn lookup<'v>(schema: &SchemaNode, value: &'v Value, path: &FieldPath) -> Option<&'v Value> {
    let mut node = schema;
    let mut v = value;
    for seg in &path.0 {
        let (i, next) = field_index(node, seg)?;
        let Value::Struct(values) = v else {
            return None;
        };
        v = values.get(i)?;
        node = next;
    }
    Some(v)
}

fn read_input<'v>(
    input: &'v Value,
    from: &SchemaNode,
    path: &FieldPath,
    prefix: &[String],
) -> Result<&'v Value, MigrationError> {
    let mut node = from;
    let mut v = input;
    for (i, seg) in path.0.iter().enumerate() {
        let (index, next) = field_index(node, seg).ok_or_else(|| MigrationError::PlanShape {
            path: dpath(prefix, path),
            detail: "path does not resolve in the from-schema".to_string(),
        })?;
        let SchemaNode::Struct { fields, .. } = node else {
            unreachable!("field_index resolved a struct");
        };
        match v {
            Value::Struct(values) if values.len() == fields.len() => v = &values[index],
            _ => {
                return Err(MigrationError::InputShape {
                    path: dpath(prefix, &FieldPath(path.0[..i].to_vec())),
                    expected: format!("struct of {} fields", fields.len()),
                })
            }
        }
        node = next;
    }
    Ok(v)
}

fn resolve_or_shape<'a>(
    root: &'a SchemaNode,
    path: &FieldPath,
    prefix: &[String],
    side: &str,
) -> Result<&'a SchemaNode, MigrationError> {
    crate::identical::resolve_path(root, path).ok_or_else(|| MigrationError::PlanShape {
        path: dpath(prefix, path),
        detail: format!("path does not resolve in the {side}-schema"),
    })
}

type Writes<T> = BTreeMap<Vec<String>, T>;

fn insert_write<T>(
    writes: &mut Writes<T>,
    path: &FieldPath,
    value: T,
    prefix: &[String],
) -> Result<(), MigrationError> {
    if writes.insert(path.0.clone(), value).is_some() {
        return Err(MigrationError::DuplicateWrite {
            path: dpath(prefix, path),
        });
    }
    Ok(())
}

fn leftover<T>(writes: &mut Writes<T>, prefix: &[String]) -> Result<(), MigrationError> {
    if let Some((path, _)) = writes.pop_first() {
        // A write that did not land in the output shape: the plan was not
        // validated (a write below another write, or off-schema).
        return Err(MigrationError::PlanShape {
            path: dpath(prefix, &FieldPath(path)),
            detail: "write did not land in the to-schema output".to_string(),
        });
    }
    Ok(())
}

/// MapVariant ops sharing one `at` are one composite writer; duplicate
/// `from`s in one group are ambiguous — an error even unvalidated.
fn variant_groups<'o>(
    ops: &'o [MigrationOp],
    prefix: &[String],
) -> Result<Vec<(&'o FieldPath, Vec<&'o MigrationOp>)>, MigrationError> {
    let mut groups: Vec<(&FieldPath, Vec<&MigrationOp>)> = Vec::new();
    for op in ops {
        let MigrationOp::MapVariant { at, from, .. } = op else {
            continue;
        };
        let group = match groups.iter_mut().find(|(p, _)| *p == at) {
            Some(g) => g,
            None => {
                groups.push((at, Vec::new()));
                groups.last_mut().expect("just pushed")
            }
        };
        if group
            .1
            .iter()
            .any(|o| matches!(o, MigrationOp::MapVariant { from: g, .. } if g == from))
        {
            return Err(MigrationError::DuplicateMapVariantFrom {
                at: dpath(prefix, at),
                from: from.clone(),
            });
        }
        group.1.push(op);
    }
    Ok(groups)
}

// ---------------------------------------------------------------------------
// Full frames
// ---------------------------------------------------------------------------

fn exec_frame(
    ops: &[MigrationOp],
    input: &Value,
    from: &SchemaNode,
    to: &SchemaNode,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
) -> Result<Value, MigrationError> {
    let mut writes = Writes::new();
    for op in ops {
        match op {
            MigrationOp::CopyField { from: f, to: t } => {
                let v = read_input(input, from, f, prefix)?.clone();
                insert_write(&mut writes, t, v, prefix)?;
            }
            MigrationOp::Widen { from: f, to: t } => {
                let old_leaf = resolve_or_shape(from, f, prefix, "from")?;
                let new_leaf = resolve_or_shape(to, t, prefix, "to")?;
                let v = read_input(input, from, f, prefix)?;
                let out = widen_value(v, old_leaf, new_leaf, &dpath(prefix, f))?;
                insert_write(&mut writes, t, out, prefix)?;
            }
            MigrationOp::WriteValue { to: t, value } => {
                insert_write(&mut writes, t, value.clone(), prefix)?;
            }
            MigrationOp::WriteFieldDefault { to: t } => {
                let v = defaults.field_default(to, t).ok_or_else(|| {
                    MigrationError::MissingDefault {
                        path: dpath(prefix, t),
                        op: "WriteFieldDefault",
                    }
                })?;
                defaulted.push(full_path(prefix, t));
                insert_write(&mut writes, t, v, prefix)?;
            }
            MigrationOp::WriteParentDefault { to: t } => {
                let v = defaults.parent_default(to, t).ok_or_else(|| {
                    MigrationError::MissingDefault {
                        path: dpath(prefix, t),
                        op: "WriteParentDefault",
                    }
                })?;
                defaulted.push(full_path(prefix, t));
                insert_write(&mut writes, t, v, prefix)?;
            }
            MigrationOp::WriteNone { to: t } => {
                insert_write(&mut writes, t, Value::Option(None), prefix)?;
            }
            // Documents an unmatched input path; writes nothing.
            MigrationOp::DropField { .. } => {}
            // Grouped below.
            MigrationOp::MapVariant { .. } => {}
            MigrationOp::MigrateElements { at, element } => {
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let v = read_input(input, from, at, prefix)?;
                let out = elements_value(element, v, fnode, tnode, at, defaults, prefix, defaulted)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateMapKeys { at, key } => {
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let v = read_input(input, from, at, prefix)?;
                let out = map_keys_value(key, v, fnode, tnode, at, defaults, prefix, defaulted)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateInline { at, ops: inner } => {
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let sub_input = read_input(input, from, at, prefix)?;
                let sub_prefix = child_prefix(prefix, at, None);
                let out = exec_frame(inner, sub_input, fnode, tnode, defaults, &sub_prefix, defaulted)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
        }
    }
    for (at, group) in variant_groups(ops, prefix)? {
        let fnode = resolve_or_shape(from, at, prefix, "from")?;
        let tnode = resolve_or_shape(to, at, prefix, "to")?;
        let v = read_input(input, from, at, prefix)?;
        let out = variants_value(&group, v, fnode, tnode, at, defaults, prefix, defaulted)?;
        insert_write(&mut writes, at, out, prefix)?;
    }
    let result = materialize(to, Vec::new(), &mut writes, prefix)?;
    leftover(&mut writes, prefix)?;
    Ok(result)
}

/// Materialize the fresh output by walking the to-schema: an exact write
/// takes the slot; a struct without one is assembled from its fields; a
/// leaf without one is a missing write (unvalidated plan).
fn materialize(
    to: &SchemaNode,
    path: Vec<String>,
    writes: &mut Writes<Value>,
    prefix: &[String],
) -> Result<Value, MigrationError> {
    if let Some(v) = writes.remove(&path) {
        return Ok(v);
    }
    match to {
        SchemaNode::Struct { fields, .. } => {
            let mut out = Vec::with_capacity(fields.len());
            for (name, _, fnode) in fields {
                let mut p = path.clone();
                p.push(name.clone());
                out.push(materialize(fnode, p, writes, prefix)?);
            }
            Ok(Value::Struct(out.into()))
        }
        _ => Err(MigrationError::MissingWrite {
            path: dpath(prefix, &FieldPath(path)),
        }),
    }
}

/// Widening (§11, `ngp_schema::migrate::can_widen_*`): the old leaf's
/// value as the new leaf's kind. A value that is not of the old leaf, or
/// does not fit the new one, is corrupt input.
fn widen_value(
    v: &Value,
    old_leaf: &SchemaNode,
    new_leaf: &SchemaNode,
    path: &str,
) -> Result<Value, MigrationError> {
    let (SchemaNode::Primitive(old), SchemaNode::Primitive(new)) = (old_leaf, new_leaf) else {
        return Err(MigrationError::PlanShape {
            path: path.to_string(),
            detail: "Widen between non-primitives".to_string(),
        });
    };
    if !v.is_primitive(*old) {
        return Err(MigrationError::InputShape {
            path: path.to_string(),
            expected: old.canonical_name().to_string(),
        });
    }
    let out = match (old, new) {
        (PrimitiveKind::F32, PrimitiveKind::F64) => v.as_f64().map(Value::F64),
        (PrimitiveKind::Bool | PrimitiveKind::Char | PrimitiveKind::F32 | PrimitiveKind::F64, _) => {
            return Err(MigrationError::PlanShape {
                path: path.to_string(),
                detail: format!(
                    "Widen from non-widenable {} to {}",
                    old.canonical_name(),
                    new.canonical_name()
                ),
            })
        }
        _ => v.as_i128().and_then(|i| Value::integer(*new, i)),
    };
    out.ok_or_else(|| MigrationError::WidenOutOfRange {
        path: path.to_string(),
        kind: new.canonical_name().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Container recursion (full values)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn elements_value(
    element: &[MigrationOp],
    v: &Value,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
) -> Result<Value, MigrationError> {
    let at_disp = dpath(prefix, at);
    let shape = |expected: &str| MigrationError::InputShape {
        path: at_disp.clone(),
        expected: expected.to_string(),
    };
    match (fnode, tnode) {
        (SchemaNode::Vec(a), SchemaNode::Vec(b))
        | (SchemaNode::Array { elem: a, .. }, SchemaNode::Array { elem: b, .. }) => {
            let Value::Seq(items) = v else {
                return Err(shape("sequence"));
            };
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push(exec_frame(element, item, a, b, defaults, &sub, defaulted)?);
            }
            Ok(Value::Seq(out))
        }
        (SchemaNode::Set(a), SchemaNode::Set(b)) => {
            let Value::Seq(items) = v else {
                return Err(shape("sequence (set)"));
            };
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push(exec_frame(element, item, a, b, defaults, &sub, defaulted)?);
            }
            // Sets rebuild through real equality (canonical order, §6):
            // re-sort; a post-migration duplicate is an error — exactly
            // the map-key collision rule (§11).
            sort_set(b, &mut out, &mut Frames::new()).map_err(|i| {
                if i == usize::MAX {
                    MigrationError::Unencodable {
                        path: at_disp.clone(),
                    }
                } else {
                    MigrationError::DuplicateSetElement {
                        path: at_disp.clone(),
                    }
                }
            })?;
            Ok(Value::Seq(out))
        }
        (SchemaNode::Option(a), SchemaNode::Option(b)) => match v {
            Value::Option(None) => Ok(Value::Option(None)),
            Value::Option(Some(inner)) => {
                let sub = child_prefix(prefix, at, None);
                let out = exec_frame(element, inner, a, b, defaults, &sub, defaulted)?;
                Ok(Value::Option(Some(Box::new(out))))
            }
            _ => Err(shape("option")),
        },
        (SchemaNode::Map { value: v1, .. }, SchemaNode::Map { value: v2, .. }) => {
            // MigrateElements over a map migrates each VALUE; keys pass
            // through, so their order is unchanged.
            let Value::Map(entries) = v else {
                return Err(shape("map"));
            };
            let mut out = Vec::with_capacity(entries.len());
            for (i, (k, val)) in entries.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push((k.clone(), exec_frame(element, val, v1, v2, defaults, &sub, defaulted)?));
            }
            Ok(Value::Map(out))
        }
        _ => Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateElements requires matching container kinds on both sides".to_string(),
        }),
    }
}

/// Migrate a map's keys (values pass through) and rebuild it in the
/// to-key's canonical order; a post-migration key collision is an error
/// (§11).
#[allow(clippy::too_many_arguments)]
fn map_keys_value(
    key_ops: &[MigrationOp],
    v: &Value,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
) -> Result<Value, MigrationError> {
    let at_disp = dpath(prefix, at);
    let (SchemaNode::Map { key: k1, .. }, SchemaNode::Map { key: k2, .. }) = (fnode, tnode) else {
        return Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateMapKeys requires a map on both sides".to_string(),
        });
    };
    let Value::Map(entries) = v else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "map".to_string(),
        });
    };
    let mut migrated = Vec::with_capacity(entries.len());
    for (i, (k, val)) in entries.iter().enumerate() {
        let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
        let nk = exec_frame(key_ops, k, k1, k2, defaults, &sub, defaulted)?;
        migrated.push((nk, val.clone()));
    }
    rekey(k2, &mut migrated, &at_disp)?;
    Ok(Value::Map(migrated))
}

fn rekey<V>(key: &SchemaNode, entries: &mut Vec<(Value, V)>, at_disp: &str) -> Result<(), MigrationError> {
    sort_map(key, entries, &mut Frames::new()).map_err(|i| {
        if i == usize::MAX {
            MigrationError::Unencodable {
                path: at_disp.to_string(),
            }
        } else {
            MigrationError::MapKeyCollision {
                path: at_disp.to_string(),
            }
        }
    })
}

/// The variant lists of an enum pair and the to-index of a variant name.
fn enum_pair<'s>(
    fnode: &'s SchemaNode,
    tnode: &'s SchemaNode,
    at_disp: &str,
) -> Result<
    (
        &'s [(String, u32, SchemaNode)],
        &'s [(String, u32, SchemaNode)],
    ),
    MigrationError,
> {
    match (fnode, tnode) {
        (SchemaNode::Enum { variants: fv, .. }, SchemaNode::Enum { variants: tv, .. }) => {
            Ok((fv, tv))
        }
        _ => Err(MigrationError::PlanShape {
            path: at_disp.to_string(),
            detail: "MapVariant requires an enum on both sides".to_string(),
        }),
    }
}

/// The op naming input variant `vname`: its to-variant and payload ops.
fn variant_hit<'o>(group: &[&'o MigrationOp], vname: &str) -> Option<(&'o String, &'o [MigrationOp])> {
    group.iter().find_map(|op| match op {
        MigrationOp::MapVariant {
            from: f,
            to: t,
            payload,
            ..
        } if f == vname => Some((t, payload.as_slice())),
        _ => None,
    })
}

fn to_variant<'s>(
    tv: &'s [(String, u32, SchemaNode)],
    name: &str,
    at_disp: &str,
) -> Result<(u32, &'s SchemaNode), MigrationError> {
    tv.iter()
        .position(|(n, _, _)| n == name)
        .map(|i| (i as u32, &tv[i].2))
        .ok_or_else(|| MigrationError::PlanShape {
            path: at_disp.to_string(),
            detail: format!("variant {name:?} not in the to-schema enum"),
        })
}

#[allow(clippy::too_many_arguments)]
fn variants_value(
    group: &[&MigrationOp],
    v: &Value,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
) -> Result<Value, MigrationError> {
    let at_disp = dpath(prefix, at);
    let (fv, tv) = enum_pair(fnode, tnode, &at_disp)?;
    let Value::Enum { variant, payload } = v else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "enum".to_string(),
        });
    };
    let Some((vname, _, fpayload)) = fv.get(*variant as usize) else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: format!("a variant of {}", fv.len()),
        });
    };
    match variant_hit(group, vname) {
        // No MapVariant names the input variant: it passes through
        // unchanged (the §11 composite-writer passthrough).
        None => {
            let (index, _) = to_variant(tv, vname, &at_disp)?;
            Ok(Value::Enum {
                variant: index,
                payload: payload.clone(),
            })
        }
        Some((tname, pops)) => {
            let (index, tpayload) = to_variant(tv, tname, &at_disp)?;
            let sub = child_prefix(prefix, at, Some(format!("{{{vname}}}")));
            let out = exec_frame(
                pops,
                &Value::Struct(payload.clone()),
                fpayload,
                tpayload,
                defaults,
                &sub,
                defaulted,
            )?;
            let Value::Struct(payload) = out else {
                return Err(MigrationError::PlanShape {
                    path: at_disp,
                    detail: "variant payload did not materialize as a struct".to_string(),
                });
            };
            Ok(Value::Enum {
                variant: index,
                payload,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Sparse frames
// ---------------------------------------------------------------------------

/// The patch at `path` of a struct patch: `None` = absent.
fn read_sparse<'p>(
    input: &'p Patch,
    from: &SchemaNode,
    path: &FieldPath,
    prefix: &[String],
) -> Result<Option<&'p Patch>, MigrationError> {
    let mut node = from;
    let mut p = input;
    for (i, seg) in path.0.iter().enumerate() {
        let (index, next) = field_index(node, seg).ok_or_else(|| MigrationError::PlanShape {
            path: dpath(prefix, path),
            detail: "path does not resolve in the from-schema".to_string(),
        })?;
        match p {
            Patch::Struct(fields) => match fields.get(index) {
                Some(Some(field)) => p = field,
                Some(None) => return Ok(None),
                None => {
                    return Err(MigrationError::InputShape {
                        path: dpath(prefix, &FieldPath(path.0[..i].to_vec())),
                        expected: "struct patch of the from-schema".to_string(),
                    })
                }
            },
            _ => {
                return Err(MigrationError::InputShape {
                    path: dpath(prefix, &FieldPath(path.0[..i].to_vec())),
                    expected: "struct patch".to_string(),
                })
            }
        }
        node = next;
    }
    Ok(Some(p))
}

fn sparse_frame(
    ops: &[MigrationOp],
    input: &Patch,
    from: &SchemaNode,
    to: &SchemaNode,
    prefix: &[String],
) -> Result<Patch, MigrationError> {
    // A whole value inside a patch migrates as a value.
    if let Patch::Replace(v) = input {
        let out = exec_frame(ops, v, from, to, &NoDefaults, prefix, &mut Vec::new())?;
        return Ok(Patch::Replace(out));
    }
    let mut writes: Writes<Patch> = Writes::new();
    for op in ops {
        match op {
            MigrationOp::CopyField { from: f, to: t } => {
                if let Some(p) = read_sparse(input, from, f, prefix)? {
                    insert_write(&mut writes, t, p.clone(), prefix)?;
                }
            }
            MigrationOp::Widen { from: f, to: t } => {
                let Some(p) = read_sparse(input, from, f, prefix)? else {
                    continue;
                };
                let old_leaf = resolve_or_shape(from, f, prefix, "from")?;
                let new_leaf = resolve_or_shape(to, t, prefix, "to")?;
                let Patch::Replace(v) = p else {
                    return Err(MigrationError::InputShape {
                        path: dpath(prefix, f),
                        expected: "leaf value".to_string(),
                    });
                };
                let out = widen_value(v, old_leaf, new_leaf, &dpath(prefix, f))?;
                insert_write(&mut writes, t, Patch::Replace(out), prefix)?;
            }
            MigrationOp::WriteValue { to: t, value } => {
                insert_write(&mut writes, t, Patch::Replace(value.clone()), prefix)?;
            }
            // Left to the consumer: absent = default.
            MigrationOp::WriteFieldDefault { .. }
            | MigrationOp::WriteParentDefault { .. }
            | MigrationOp::WriteNone { .. }
            | MigrationOp::DropField { .. }
            | MigrationOp::MapVariant { .. } => {}
            MigrationOp::MigrateElements { at, element } => {
                let Some(p) = read_sparse(input, from, at, prefix)? else {
                    continue;
                };
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let out = sparse_elements(element, p, fnode, tnode, at, prefix)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateMapKeys { at, key } => {
                let Some(p) = read_sparse(input, from, at, prefix)? else {
                    continue;
                };
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let out = sparse_map_keys(key, p, fnode, tnode, at, prefix)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateInline { at, ops: inner } => {
                let Some(p) = read_sparse(input, from, at, prefix)? else {
                    continue;
                };
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let sub_prefix = child_prefix(prefix, at, None);
                let out = sparse_frame(inner, p, fnode, tnode, &sub_prefix)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
        }
    }
    for (at, group) in variant_groups(ops, prefix)? {
        let Some(p) = read_sparse(input, from, at, prefix)? else {
            continue;
        };
        let fnode = resolve_or_shape(from, at, prefix, "from")?;
        let tnode = resolve_or_shape(to, at, prefix, "to")?;
        let out = sparse_variants(&group, p, fnode, tnode, at, prefix)?;
        insert_write(&mut writes, at, out, prefix)?;
    }
    let result = materialize_sparse(to, Vec::new(), &mut writes, true).ok_or_else(|| {
        MigrationError::MissingWrite {
            path: dpath(prefix, &FieldPath::root()),
        }
    })?;
    leftover(&mut writes, prefix)?;
    Ok(result)
}

/// [`materialize`] for sparse output: a struct holds the fields written
/// beneath it and is omitted when none were (the frame root excepted);
/// an unwritten leaf is absent.
fn materialize_sparse(
    to: &SchemaNode,
    path: Vec<String>,
    writes: &mut Writes<Patch>,
    root: bool,
) -> Option<Patch> {
    if let Some(p) = writes.remove(&path) {
        return Some(p);
    }
    let SchemaNode::Struct { fields, .. } = to else {
        return None;
    };
    let mut out = Vec::with_capacity(fields.len());
    for (name, _, fnode) in fields {
        let mut p = path.clone();
        p.push(name.clone());
        out.push(materialize_sparse(fnode, p, writes, false));
    }
    (root || out.iter().any(Option::is_some)).then(|| Patch::Struct(out.into()))
}

fn sparse_elements(
    element: &[MigrationOp],
    p: &Patch,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    prefix: &[String],
) -> Result<Patch, MigrationError> {
    if let Patch::Replace(v) = p {
        let out = elements_value(element, v, fnode, tnode, at, &NoDefaults, prefix, &mut Vec::new())?;
        return Ok(Patch::Replace(out));
    }
    let at_disp = dpath(prefix, at);
    let shape = |expected: &str| MigrationError::InputShape {
        path: at_disp.clone(),
        expected: expected.to_string(),
    };
    match (fnode, tnode, p) {
        (SchemaNode::Vec(a), SchemaNode::Vec(b), Patch::Seq(items))
        | (SchemaNode::Array { elem: a, .. }, SchemaNode::Array { elem: b, .. }, Patch::Seq(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push(sparse_frame(element, item, a, b, &sub)?);
            }
            Ok(Patch::Seq(out))
        }
        (SchemaNode::Option(a), SchemaNode::Option(b), Patch::Some(inner)) => {
            let sub = child_prefix(prefix, at, None);
            Ok(Patch::Some(Box::new(sparse_frame(element, inner, a, b, &sub)?)))
        }
        (SchemaNode::Map { value: v1, .. }, SchemaNode::Map { value: v2, .. }, Patch::Map { set, remove }) => {
            let mut out = Vec::with_capacity(set.len());
            for (i, (k, val)) in set.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push((k.clone(), sparse_frame(element, val, v1, v2, &sub)?));
            }
            Ok(Patch::Map {
                set: out,
                remove: remove.clone(),
            })
        }
        (SchemaNode::Vec(_) | SchemaNode::Array { .. }, _, _) => Err(shape("sequence patch")),
        (SchemaNode::Option(_), _, _) => Err(shape("option patch")),
        (SchemaNode::Map { .. }, _, _) => Err(shape("map patch")),
        _ => Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateElements requires matching container kinds on both sides".to_string(),
        }),
    }
}

fn sparse_map_keys(
    key_ops: &[MigrationOp],
    p: &Patch,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    prefix: &[String],
) -> Result<Patch, MigrationError> {
    if let Patch::Replace(v) = p {
        let out = map_keys_value(key_ops, v, fnode, tnode, at, &NoDefaults, prefix, &mut Vec::new())?;
        return Ok(Patch::Replace(out));
    }
    let at_disp = dpath(prefix, at);
    let (SchemaNode::Map { key: k1, .. }, SchemaNode::Map { key: k2, .. }) = (fnode, tnode) else {
        return Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateMapKeys requires a map on both sides".to_string(),
        });
    };
    let Patch::Map { set, remove } = p else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "map patch".to_string(),
        });
    };
    let migrate = |i: usize, k: &Value| {
        let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
        exec_frame(key_ops, k, k1, k2, &NoDefaults, &sub, &mut Vec::new())
    };
    let mut set_out = Vec::with_capacity(set.len());
    for (i, (k, val)) in set.iter().enumerate() {
        set_out.push((migrate(i, k)?, val.clone()));
    }
    rekey(k2, &mut set_out, &at_disp)?;
    let mut remove_out = Vec::with_capacity(remove.len());
    for (i, k) in remove.iter().enumerate() {
        remove_out.push((migrate(i, k)?, ()));
    }
    rekey(k2, &mut remove_out, &at_disp)?;
    Ok(Patch::Map {
        set: set_out,
        remove: remove_out.into_iter().map(|(k, ())| k).collect(),
    })
}

fn sparse_variants(
    group: &[&MigrationOp],
    p: &Patch,
    fnode: &SchemaNode,
    tnode: &SchemaNode,
    at: &FieldPath,
    prefix: &[String],
) -> Result<Patch, MigrationError> {
    if let Patch::Replace(v) = p {
        let out = variants_value(group, v, fnode, tnode, at, &NoDefaults, prefix, &mut Vec::new())?;
        return Ok(Patch::Replace(out));
    }
    let at_disp = dpath(prefix, at);
    let (fv, tv) = enum_pair(fnode, tnode, &at_disp)?;
    let Patch::Enum { variant, payload } = p else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "enum patch".to_string(),
        });
    };
    let Some((vname, _, fpayload)) = fv.get(*variant as usize) else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: format!("a variant of {}", fv.len()),
        });
    };
    match variant_hit(group, vname) {
        None => {
            let (index, _) = to_variant(tv, vname, &at_disp)?;
            Ok(Patch::Enum {
                variant: index,
                payload: payload.clone(),
            })
        }
        Some((tname, pops)) => {
            let (index, tpayload) = to_variant(tv, tname, &at_disp)?;
            let sub = child_prefix(prefix, at, Some(format!("{{{vname}}}")));
            let out = sparse_frame(pops, &Patch::Struct(payload.clone()), fpayload, tpayload, &sub)?;
            let Patch::Struct(payload) = out else {
                return Err(MigrationError::PlanShape {
                    path: at_disp,
                    detail: "variant payload did not materialize as a struct".to_string(),
                });
            };
            Ok(Patch::Enum {
                variant: index,
                payload,
            })
        }
    }
}

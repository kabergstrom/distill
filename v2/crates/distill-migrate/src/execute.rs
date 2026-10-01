//! The AuthoredValue executor (§11). Execution semantics are normative,
//! not executor-defined: every op reads the edge's IMMUTABLE input value
//! and writes into a fresh output — no op ever observes another op's
//! writes, so op order can never change the result. `CopyField` copies;
//! the source stays readable to later ops.

use crate::conform::{conforms, ConformError};
use crate::validate::{validate_plan, EdgeKind, PlanError};
use crate::{FieldPath, MigrationKind, MigrationOp};
use distill_json::AuthoredValue;
use ngp_schema::SchemaNode;
use std::fmt;

/// An edge's execution result: the output value plus op provenance —
/// every path a default-table op materialized (diagnostics, §11).
#[derive(Clone, Debug, PartialEq)]
pub struct Exec {
    pub value: AuthoredValue,
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
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue>;
    /// The field's value taken from the parent type's `Default`.
    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue>;
}

/// Runs registered migration functions (§3 module registry). An unknown
/// key is an `Err` — there is no fallback.
pub trait FnProvider {
    fn run(&self, key: &str, v: AuthoredValue) -> Result<AuthoredValue, String>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum MigrationError {
    /// execute_edge validates Ops plans (Custom edge kind) before running.
    PlanInvalid(Vec<PlanError>),
    /// An op's input path is absent from the input value (corrupt input).
    InputPathMissing {
        path: String,
    },
    /// The input value has the wrong shape at a path (corrupt input).
    InputShape {
        path: String,
        expected: String,
    },
    /// A widened value lies outside the OLD leaf's range (corrupt input).
    WidenOutOfRange {
        path: String,
        kind: String,
    },
    /// The DefaultProvider returned None — fail hard, no fabricated values.
    MissingDefault {
        path: String,
        op: &'static str,
    },
    /// Set rebuild found equal elements post-migration (canonical encoded
    /// bytes) — exactly the map-key collision rule.
    DuplicateSetElement {
        path: String,
    },
    /// MigrateMapKeys produced colliding keys.
    MapKeyCollision {
        path: String,
    },
    /// A string-keyed to-map received a non-string migrated key.
    MapKeyNotString {
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
            InputPathMissing { path } => write!(f, "input value has no path {path}"),
            InputShape { path, expected } => {
                write!(f, "input value at {path}: expected {expected}")
            }
            WidenOutOfRange { path, kind } => write!(
                f,
                "value at {path} lies outside the old leaf's {kind} range (corrupt input)"
            ),
            MissingDefault { path, op } => write!(
                f,
                "{op} at {path}: no default available — integrity forbids fabricated values"
            ),
            DuplicateSetElement { path } => {
                write!(f, "set at {path}: post-migration duplicate element")
            }
            MapKeyCollision { path } => write!(f, "map at {path}: post-migration key collision"),
            MapKeyNotString { path } => {
                write!(f, "map at {path}: migrated key is not a string")
            }
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
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
    defaults: &dyn DefaultProvider,
) -> Result<Exec, MigrationError> {
    let mut defaulted = Vec::new();
    let value = exec_frame(ops, input, from, to, defaults, &[], &mut defaulted, false)?;
    Ok(Exec { value, defaulted })
}

/// Execute an automatic plan over a SPARSE input: an object holding only
/// some of `from`'s struct fields, at any depth (absent = the consumer's
/// default). Ops reading an absent path are skipped, default writes
/// (`WriteFieldDefault`, `WriteParentDefault`, `WriteNone`) are not
/// materialized, and the output holds exactly the fields the remaining
/// ops wrote, so a renamed field follows its key and a dropped one
/// disappears. A unit variant may be spelled as its bare name. The output
/// is sparse too and is not checked for conformance.
pub fn execute_sparse(
    ops: &[MigrationOp],
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
) -> Result<AuthoredValue, MigrationError> {
    struct NoDefaults;
    impl DefaultProvider for NoDefaults {
        fn field_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<AuthoredValue> {
            None
        }
        fn parent_default(&self, _: &SchemaNode, _: &FieldPath) -> Option<AuthoredValue> {
            None
        }
    }
    exec_frame(ops, input, from, to, &NoDefaults, &[], &mut Vec::new(), true)
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
    input: &AuthoredValue,
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
// Frame execution
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
/// `[key]`/`[i]`, variant payload frames `{Variant}` — diagnostic
/// pseudo-segments, not resolvable FieldPaths.
fn full_path(prefix: &[String], p: &FieldPath) -> FieldPath {
    FieldPath(prefix.iter().cloned().chain(p.0.iter().cloned()).collect())
}

fn read_input<'a>(
    input: &'a AuthoredValue,
    path: &FieldPath,
    prefix: &[String],
) -> Result<&'a AuthoredValue, MigrationError> {
    let mut v = input;
    for (i, seg) in path.0.iter().enumerate() {
        match v {
            AuthoredValue::Object(m) => match m.get(seg) {
                Some(next) => v = next,
                None => {
                    return Err(MigrationError::InputPathMissing {
                        path: dpath(prefix, &FieldPath(path.0[..=i].to_vec())),
                    })
                }
            },
            _ => {
                return Err(MigrationError::InputShape {
                    path: dpath(prefix, &FieldPath(path.0[..i].to_vec())),
                    expected: "object".to_string(),
                })
            }
        }
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

type Writes = std::collections::BTreeMap<Vec<String>, AuthoredValue>;

fn insert_write(
    writes: &mut Writes,
    path: &FieldPath,
    value: AuthoredValue,
    prefix: &[String],
) -> Result<(), MigrationError> {
    if writes.insert(path.0.clone(), value).is_some() {
        return Err(MigrationError::DuplicateWrite {
            path: dpath(prefix, path),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn exec_frame(
    ops: &[MigrationOp],
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
    sparse: bool,
) -> Result<AuthoredValue, MigrationError> {
    let mut writes = Writes::new();
    // MapVariant ops sharing one `at` are one composite writer.
    let mut mv_groups: Vec<(&FieldPath, Vec<&MigrationOp>)> = Vec::new();
    // Sparse input: an op reading an absent path is skipped, and default
    // writes are left to the consumer (absent = default).
    let absent = |p: &FieldPath| sparse && !has_input(input, p);

    for op in ops {
        match op {
            MigrationOp::CopyField { from: f, .. } | MigrationOp::Widen { from: f, .. }
                if absent(f) => {}
            MigrationOp::WriteFieldDefault { .. }
            | MigrationOp::WriteParentDefault { .. }
            | MigrationOp::WriteNone { .. }
                if sparse => {}
            MigrationOp::MapVariant { at, .. }
            | MigrationOp::MigrateElements { at, .. }
            | MigrationOp::MigrateMapKeys { at, .. }
            | MigrationOp::MigrateInline { at, .. }
                if absent(at) => {}
            MigrationOp::CopyField { from: f, to: t } => {
                let v = read_input(input, f, prefix)?.clone();
                insert_write(&mut writes, t, v, prefix)?;
            }
            MigrationOp::Widen { from: f, to: t } => {
                let old_leaf = resolve_or_shape(from, f, prefix, "from")?;
                let v = read_input(input, f, prefix)?;
                let out = widen_value(v, old_leaf, &dpath(prefix, f))?;
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
                insert_write(&mut writes, t, AuthoredValue::Null, prefix)?;
            }
            // Documents an unmatched input path; writes nothing.
            MigrationOp::DropField { .. } => {}
            MigrationOp::MapVariant { at, .. } => {
                if let Some(g) = mv_groups.iter_mut().find(|(p, _)| *p == at) {
                    g.1.push(op);
                } else {
                    mv_groups.push((at, vec![op]));
                }
            }
            MigrationOp::MigrateElements { at, element } => {
                let out = exec_elements(element, input, from, to, at, defaults, prefix, defaulted, sparse)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateMapKeys { at, key } => {
                let out = exec_map_keys(key, input, from, to, at, defaults, prefix, defaulted, sparse)?;
                insert_write(&mut writes, at, out, prefix)?;
            }
            MigrationOp::MigrateInline { at, ops: inner } => {
                let fnode = resolve_or_shape(from, at, prefix, "from")?;
                let tnode = resolve_or_shape(to, at, prefix, "to")?;
                let sub_input = read_input(input, at, prefix)?;
                let sub_prefix = child_prefix(prefix, at, None);
                let out = exec_frame(
                    inner,
                    sub_input,
                    fnode,
                    tnode,
                    defaults,
                    &sub_prefix,
                    defaulted,
                    sparse,
                )?;
                insert_write(&mut writes, at, out, prefix)?;
            }
        }
    }

    for (at, group) in mv_groups {
        let out = exec_map_variants(&group, input, from, to, at, defaults, prefix, defaulted, sparse)?;
        insert_write(&mut writes, at, out, prefix)?;
    }

    let result = if sparse {
        let root = materialize_sparse(to, Vec::new(), &mut writes, true);
        root.ok_or_else(|| MigrationError::MissingWrite {
            path: dpath(prefix, &FieldPath::root()),
        })?
    } else {
        materialize(to, Vec::new(), &mut writes, prefix)?
    };
    if let Some((path, _)) = writes.pop_first() {
        // A write that did not land in the output shape: the plan was not
        // validated (a write below another write, or off-schema).
        return Err(MigrationError::PlanShape {
            path: dpath(prefix, &FieldPath(path)),
            detail: "write did not land in the to-schema output".to_string(),
        });
    }
    Ok(result)
}

/// [`materialize`] for sparse output: a struct holds the fields written
/// beneath it and is omitted when none were (the frame root excepted);
/// an unwritten leaf is absent.
fn materialize_sparse(to: &SchemaNode, path: Vec<String>, writes: &mut Writes, root: bool) -> Option<AuthoredValue> {
    if let Some(v) = writes.remove(&path) {
        return Some(v);
    }
    let SchemaNode::Struct { fields, .. } = to else {
        return None;
    };
    let mut m = std::collections::BTreeMap::new();
    for (name, _, fnode) in fields {
        let mut p = path.clone();
        p.push(name.clone());
        if let Some(v) = materialize_sparse(fnode, p, writes, false) {
            m.insert(name.clone(), v);
        }
    }
    (root || !m.is_empty()).then_some(AuthoredValue::Object(m))
}

/// Whether every segment of `path` is present in `input`. A non-object
/// on the way counts as present, so the read reports its shape error.
fn has_input(input: &AuthoredValue, path: &FieldPath) -> bool {
    let mut v = input;
    for seg in &path.0 {
        match v {
            AuthoredValue::Object(m) => match m.get(seg) {
                Some(next) => v = next,
                None => return false,
            },
            _ => return true,
        }
    }
    true
}

fn child_prefix(prefix: &[String], at: &FieldPath, pseudo: Option<String>) -> Vec<String> {
    let mut p: Vec<String> = prefix.to_vec();
    p.extend(at.0.iter().cloned());
    if let Some(s) = pseudo {
        p.push(s);
    }
    p
}

/// Materialize the fresh output by walking the to-schema: an exact write
/// takes the slot; a struct without one is assembled from its fields; a
/// leaf without one is a missing write (unvalidated plan).
fn materialize(
    to: &SchemaNode,
    path: Vec<String>,
    writes: &mut Writes,
    prefix: &[String],
) -> Result<AuthoredValue, MigrationError> {
    if let Some(v) = writes.remove(&path) {
        return Ok(v);
    }
    match to {
        SchemaNode::Struct { fields, .. } => {
            let mut m = std::collections::BTreeMap::new();
            for (name, _, fnode) in fields {
                let mut p = path.clone();
                p.push(name.clone());
                m.insert(name.clone(), materialize(fnode, p, writes, prefix)?);
            }
            Ok(AuthoredValue::Object(m))
        }
        _ => Err(MigrationError::MissingWrite {
            path: dpath(prefix, &FieldPath(path)),
        }),
    }
}

// ---------------------------------------------------------------------------
// Widen
// ---------------------------------------------------------------------------

/// Value passthrough with variant normalization (PINNED: non-negative
/// integers normalize to `UInt` — the parser's pinned variant split —
/// negative stays `Int`, floats stay `Float`). A value outside the OLD
/// leaf's range is corrupt input.
fn widen_value(
    v: &AuthoredValue,
    old_leaf: &SchemaNode,
    path: &str,
) -> Result<AuthoredValue, MigrationError> {
    use ngp_schema::PrimitiveKind as PK;
    let SchemaNode::Primitive(kind) = old_leaf else {
        return Err(MigrationError::PlanShape {
            path: path.to_string(),
            detail: "Widen source is not a primitive".to_string(),
        });
    };
    match kind {
        PK::F32 => match v {
            AuthoredValue::Float(f) => {
                if f.is_finite() && (*f as f32).is_finite() && (*f as f32) as f64 == *f {
                    Ok(v.clone())
                } else {
                    Err(MigrationError::WidenOutOfRange {
                        path: path.to_string(),
                        kind: "f32".to_string(),
                    })
                }
            }
            _ => Err(MigrationError::InputShape {
                path: path.to_string(),
                expected: "float".to_string(),
            }),
        },
        PK::Bool | PK::Char | PK::F64 => Err(MigrationError::PlanShape {
            path: path.to_string(),
            detail: format!("Widen from non-widenable {}", kind.canonical_name()),
        }),
        _ => match v {
            AuthoredValue::Int(_) | AuthoredValue::UInt(_) => {
                if crate::conform::int_in_range(*kind, v) != Some(true) {
                    return Err(MigrationError::WidenOutOfRange {
                        path: path.to_string(),
                        kind: kind.canonical_name().to_string(),
                    });
                }
                Ok(match v {
                    AuthoredValue::Int(i) if *i >= 0 => AuthoredValue::UInt(*i as u128),
                    other => other.clone(),
                })
            }
            _ => Err(MigrationError::InputShape {
                path: path.to_string(),
                expected: "integer".to_string(),
            }),
        },
    }
}

// ---------------------------------------------------------------------------
// Container recursion
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn exec_elements(
    element: &[MigrationOp],
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
    sparse: bool,
) -> Result<AuthoredValue, MigrationError> {
    let fnode = resolve_or_shape(from, at, prefix, "from")?;
    let tnode = resolve_or_shape(to, at, prefix, "to")?;
    let v = read_input(input, at, prefix)?;
    let at_disp = dpath(prefix, at);
    match (fnode, tnode) {
        (SchemaNode::Vec(a), SchemaNode::Vec(b))
        | (SchemaNode::Array { elem: a, .. }, SchemaNode::Array { elem: b, .. }) => {
            let AuthoredValue::Array(items) = v else {
                return Err(MigrationError::InputShape {
                    path: at_disp,
                    expected: "array".to_string(),
                });
            };
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                out.push(exec_frame(element, item, a, b, defaults, &sub, defaulted, sparse)?);
            }
            Ok(AuthoredValue::Array(out))
        }
        (SchemaNode::Set(a), SchemaNode::Set(b)) => {
            let AuthoredValue::Array(items) = v else {
                return Err(MigrationError::InputShape {
                    path: at_disp,
                    expected: "array (set)".to_string(),
                });
            };
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                let migrated = exec_frame(element, item, a, b, defaults, &sub, defaulted, sparse)?;
                let enc =
                    distill_json::write(&migrated).map_err(|_| MigrationError::Unencodable {
                        path: at_disp.clone(),
                    })?;
                out.push((enc, migrated));
            }
            // Sets rebuild through real equality (canonical encoded bytes,
            // §6): re-sort, and a post-migration duplicate is an error —
            // exactly the map-key collision rule (§11).
            out.sort_by(|x, y| x.0.cmp(&y.0));
            if out.windows(2).any(|w| w[0].0 == w[1].0) {
                return Err(MigrationError::DuplicateSetElement { path: at_disp });
            }
            Ok(AuthoredValue::Array(
                out.into_iter().map(|(_, v)| v).collect(),
            ))
        }
        (SchemaNode::Option(a), SchemaNode::Option(b)) => match v {
            // Null stays Null; otherwise run on the inner value.
            AuthoredValue::Null => Ok(AuthoredValue::Null),
            inner => {
                let sub = child_prefix(prefix, at, None);
                exec_frame(element, inner, a, b, defaults, &sub, defaulted, sparse)
            }
        },
        (SchemaNode::Map { key: k1, value: v1 }, SchemaNode::Map { value: v2, .. }) => {
            // MigrateElements over a map migrates each VALUE; keys pass
            // through, so their order (and encoding form) is unchanged.
            if matches!(**k1, SchemaNode::String) {
                let AuthoredValue::Object(m) = v else {
                    return Err(MigrationError::InputShape {
                        path: at_disp,
                        expected: "object (string-key map)".to_string(),
                    });
                };
                let mut out = std::collections::BTreeMap::new();
                for (k, val) in m {
                    let sub = child_prefix(prefix, at, Some(format!("[{k}]")));
                    out.insert(
                        k.clone(),
                        exec_frame(element, val, v1, v2, defaults, &sub, defaulted, sparse)?,
                    );
                }
                Ok(AuthoredValue::Object(out))
            } else {
                let AuthoredValue::Array(pairs) = v else {
                    return Err(MigrationError::InputShape {
                        path: at_disp,
                        expected: "array of [key, value] pairs".to_string(),
                    });
                };
                let mut out = Vec::with_capacity(pairs.len());
                for (i, pair) in pairs.iter().enumerate() {
                    let AuthoredValue::Array(kv) = pair else {
                        return Err(MigrationError::InputShape {
                            path: format!("{at_disp}[{i}]"),
                            expected: "[key, value] pair".to_string(),
                        });
                    };
                    if kv.len() != 2 {
                        return Err(MigrationError::InputShape {
                            path: format!("{at_disp}[{i}]"),
                            expected: "[key, value] pair of exactly 2".to_string(),
                        });
                    }
                    let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
                    let nv = exec_frame(element, &kv[1], v1, v2, defaults, &sub, defaulted, sparse)?;
                    out.push(AuthoredValue::Array(vec![kv[0].clone(), nv]));
                }
                Ok(AuthoredValue::Array(out))
            }
        }
        _ => Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateElements requires matching container kinds on both sides".to_string(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn exec_map_keys(
    key_ops: &[MigrationOp],
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
    sparse: bool,
) -> Result<AuthoredValue, MigrationError> {
    let fnode = resolve_or_shape(from, at, prefix, "from")?;
    let tnode = resolve_or_shape(to, at, prefix, "to")?;
    let v = read_input(input, at, prefix)?;
    let at_disp = dpath(prefix, at);
    let (SchemaNode::Map { key: k1, .. }, SchemaNode::Map { key: k2, .. }) = (fnode, tnode) else {
        return Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MigrateMapKeys requires a map on both sides".to_string(),
        });
    };

    // Gather (old key value, value) entries from either physical form.
    let mut entries: Vec<(AuthoredValue, AuthoredValue)> = Vec::new();
    if matches!(**k1, SchemaNode::String) {
        let AuthoredValue::Object(m) = v else {
            return Err(MigrationError::InputShape {
                path: at_disp,
                expected: "object (string-key map)".to_string(),
            });
        };
        for (k, val) in m {
            entries.push((AuthoredValue::Str(k.clone()), val.clone()));
        }
    } else {
        let AuthoredValue::Array(pairs) = v else {
            return Err(MigrationError::InputShape {
                path: at_disp,
                expected: "array of [key, value] pairs".to_string(),
            });
        };
        for (i, pair) in pairs.iter().enumerate() {
            let AuthoredValue::Array(kv) = pair else {
                return Err(MigrationError::InputShape {
                    path: format!("{at_disp}[{i}]"),
                    expected: "[key, value] pair".to_string(),
                });
            };
            if kv.len() != 2 {
                return Err(MigrationError::InputShape {
                    path: format!("{at_disp}[{i}]"),
                    expected: "[key, value] pair of exactly 2".to_string(),
                });
            }
            entries.push((kv[0].clone(), kv[1].clone()));
        }
    }

    // Migrate each KEY; values pass through untouched.
    let mut migrated: Vec<(AuthoredValue, AuthoredValue)> = Vec::new();
    for (i, (k, val)) in entries.into_iter().enumerate() {
        let sub = child_prefix(prefix, at, Some(format!("[{i}]")));
        let nk = exec_frame(key_ops, &k, k1, k2, defaults, &sub, defaulted, sparse)?;
        migrated.push((nk, val));
    }

    // Rebuild in the TO key node's physical form; a post-migration key
    // collision is an error (§11).
    if matches!(**k2, SchemaNode::String) {
        let mut out = std::collections::BTreeMap::new();
        for (nk, val) in migrated {
            let AuthoredValue::Str(s) = nk else {
                return Err(MigrationError::MapKeyNotString {
                    path: at_disp.clone(),
                });
            };
            if out.insert(s, val).is_some() {
                return Err(MigrationError::MapKeyCollision { path: at_disp });
            }
        }
        Ok(AuthoredValue::Object(out))
    } else {
        let mut keyed = Vec::with_capacity(migrated.len());
        for (nk, val) in migrated {
            let enc = distill_json::write(&nk).map_err(|_| MigrationError::Unencodable {
                path: at_disp.clone(),
            })?;
            keyed.push((enc, nk, val));
        }
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        if keyed.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(MigrationError::MapKeyCollision { path: at_disp });
        }
        Ok(AuthoredValue::Array(
            keyed
                .into_iter()
                .map(|(_, k, v)| AuthoredValue::Array(vec![k, v]))
                .collect(),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn exec_map_variants(
    group: &[&MigrationOp],
    input: &AuthoredValue,
    from: &SchemaNode,
    to: &SchemaNode,
    at: &FieldPath,
    defaults: &dyn DefaultProvider,
    prefix: &[String],
    defaulted: &mut Vec<FieldPath>,
    sparse: bool,
) -> Result<AuthoredValue, MigrationError> {
    let fnode = resolve_or_shape(from, at, prefix, "from")?;
    let tnode = resolve_or_shape(to, at, prefix, "to")?;
    let at_disp = dpath(prefix, at);
    let (SchemaNode::Enum { variants: fv, .. }, SchemaNode::Enum { variants: tv, .. }) =
        (fnode, tnode)
    else {
        return Err(MigrationError::PlanShape {
            path: at_disp,
            detail: "MapVariant requires an enum on both sides".to_string(),
        });
    };
    // Duplicate `from`s in one group are ambiguous — error even here (an
    // unvalidated plan must not pick one silently).
    for (i, op) in group.iter().enumerate() {
        let MigrationOp::MapVariant { from: f, .. } = op else {
            unreachable!("group holds MapVariant ops only")
        };
        if group[..i]
            .iter()
            .any(|o| matches!(o, MigrationOp::MapVariant { from: g, .. } if g == f))
        {
            return Err(MigrationError::DuplicateMapVariantFrom {
                at: at_disp,
                from: f.clone(),
            });
        }
    }

    let v = read_input(input, at, prefix)?;
    // Sparse values may spell a unit variant as its bare name.
    if let (true, AuthoredValue::Str(vname)) = (sparse, v) {
        let renamed = group.iter().find_map(|op| match op {
            MigrationOp::MapVariant { from: f, to: t, .. } if f == vname => Some(t),
            _ => None,
        });
        return Ok(AuthoredValue::Str(renamed.unwrap_or(vname).clone()));
    }
    let AuthoredValue::Object(m) = v else {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "single-key object (enum)".to_string(),
        });
    };
    if m.len() != 1 {
        return Err(MigrationError::InputShape {
            path: at_disp,
            expected: "single-key object (enum)".to_string(),
        });
    }
    let (vname, payload) = m.iter().next().expect("len checked");

    let hit = group.iter().find_map(|op| match op {
        MigrationOp::MapVariant {
            from: f,
            to: t,
            payload: pops,
            ..
        } if f == vname => Some((t, pops)),
        _ => None,
    });
    match hit {
        // No MapVariant names the input variant: it passes through
        // unchanged (the §11 composite-writer passthrough).
        None => Ok(v.clone()),
        Some((tname, pops)) => {
            let fpayload = fv
                .iter()
                .find(|(n, _, _)| n == vname)
                .map(|(_, _, p)| p)
                .ok_or_else(|| MigrationError::PlanShape {
                    path: at_disp.clone(),
                    detail: format!("variant {vname:?} not in the from-schema enum"),
                })?;
            let tpayload = tv
                .iter()
                .find(|(n, _, _)| n == tname)
                .map(|(_, _, p)| p)
                .ok_or_else(|| MigrationError::PlanShape {
                    path: at_disp.clone(),
                    detail: format!("variant {tname:?} not in the to-schema enum"),
                })?;
            let sub = child_prefix(prefix, at, Some(format!("{{{vname}}}")));
            let out = exec_frame(pops, payload, fpayload, tpayload, defaults, &sub, defaulted, sparse)?;
            let mut result = std::collections::BTreeMap::new();
            result.insert(tname.clone(), out);
            Ok(AuthoredValue::Object(result))
        }
    }
}

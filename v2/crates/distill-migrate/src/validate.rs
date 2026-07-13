//! Plan validation (§11): paths resolve in their endpoint schemas, edge
//! legality (default ops are automatic-segment-only), and total, disjoint
//! output coverage.

use crate::identical::resolve_path;
use crate::{FieldPath, MigrationOp};
use ngp_schema::migrate::{can_widen_float, can_widen_int};
use ngp_schema::SchemaNode;
use std::fmt;

/// Which segment of a chain an Ops plan belongs to. Custom edges may not
/// carry default-table ops (§11: edge authoring materializes literals).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    Automatic,
    Custom,
}

/// Which endpoint schema a path failed to resolve in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaSide {
    From,
    To,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// A `from` path (or DropField `at` — an input path) does not resolve
    /// in the from-schema.
    MissingFromPath {
        path: String,
    },
    /// A `to` path does not resolve in the to-schema.
    MissingToPath {
        path: String,
    },
    /// An `at` path must resolve in BOTH schemas; this side failed.
    MissingAtPath {
        path: String,
        side: SchemaSide,
    },
    /// WriteFieldDefault/WriteParentDefault in a custom edge (§11).
    DefaultOpInCustomEdge {
        path: String,
        op: &'static str,
    },
    /// A coverage leaf of the to-schema no op writes.
    UncoveredLeaf {
        path: String,
    },
    /// A coverage leaf two writers cover (equal paths or prefix overlap).
    DoubleCoveredLeaf {
        path: String,
    },
    /// Two writers at one output path (detected even when the path has no
    /// coverage leaves under it, e.g. a zero-field struct — pinned).
    DuplicateWriter {
        path: String,
    },
    /// Two MapVariant ops at one `at` naming the same `from` variant.
    DuplicateMapVariantFrom {
        at: String,
        from: String,
    },
    NotAnEnum {
        path: String,
        side: SchemaSide,
    },
    UnknownVariant {
        at: String,
        variant: String,
        side: SchemaSide,
    },
    /// MigrateInline requires a Struct at `at` on both sides (pinned).
    NotAStruct {
        path: String,
        side: SchemaSide,
    },
    /// MigrateElements/MigrateMapKeys shape problems: not a container,
    /// container kind changed, array length changed, not a map.
    ContainerMismatch {
        path: String,
        detail: String,
    },
    /// Widen endpoints are not primitives related by the shared widening
    /// table (i128/u128 are never targets — the table already says so).
    InvalidWiden {
        path: String,
        detail: String,
    },
    /// WriteNone targets must be Option nodes (pinned).
    WriteNoneTargetNotOption {
        path: String,
    },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PlanError::*;
        match self {
            MissingFromPath { path } => {
                write!(f, "path {path} does not resolve in the from-schema")
            }
            MissingToPath { path } => {
                write!(f, "path {path} does not resolve in the to-schema")
            }
            MissingAtPath { path, side } => {
                write!(f, "at-path {path} does not resolve in the {side:?}-schema")
            }
            DefaultOpInCustomEdge { path, op } => write!(
                f,
                "{op} at {path}: default-table ops are automatic-segment-only; \
                 custom edges carry literals (WriteValue)"
            ),
            UncoveredLeaf { path } => write!(f, "output leaf {path} is written by no op"),
            DoubleCoveredLeaf { path } => {
                write!(f, "output leaf {path} is written by more than one op")
            }
            DuplicateWriter { path } => write!(f, "two ops write output path {path}"),
            DuplicateMapVariantFrom { at, from } => write!(
                f,
                "two MapVariant ops at {at} name the same input variant {from:?}"
            ),
            NotAnEnum { path, side } => {
                write!(
                    f,
                    "MapVariant at {path}: not an enum in the {side:?}-schema"
                )
            }
            UnknownVariant { at, variant, side } => write!(
                f,
                "MapVariant at {at}: variant {variant:?} is not declared in the \
                 {side:?}-schema enum"
            ),
            NotAStruct { path, side } => {
                write!(
                    f,
                    "MigrateInline at {path}: not a struct in the {side:?}-schema"
                )
            }
            ContainerMismatch { path, detail } => write!(f, "at {path}: {detail}"),
            InvalidWiden { path, detail } => write!(f, "Widen at {path}: {detail}"),
            WriteNoneTargetNotOption { path } => {
                write!(f, "WriteNone at {path}: target is not an Option")
            }
        }
    }
}

/// Validate a plan against its edge's endpoint schemas (§11): every
/// `from` path resolves in `from`, every `to` path in `to`, `at` paths in
/// both; custom edges reject default-table ops; the plan is total and
/// disjoint over the output — every coverage leaf of `to` (paths through
/// Struct fields only whose node is not a Struct) is written by exactly
/// one op; the MapVariant ops sharing one `at` are one composite writer.
/// Nested op lists apply the same model recursively.
pub fn validate_plan(
    ops: &[MigrationOp],
    from: &SchemaNode,
    to: &SchemaNode,
    edge: EdgeKind,
) -> Result<(), Vec<PlanError>> {
    let mut errors = Vec::new();
    validate_frame(ops, from, to, edge, "$", &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn disp(prefix: &str, path: &FieldPath) -> String {
    let mut s = prefix.to_string();
    for seg in &path.0 {
        s.push('.');
        s.push_str(seg);
    }
    s
}

/// One writer of output coverage: a plain op's `to`/`at` path, or the
/// composite MapVariant group at one `at` path.
fn validate_frame(
    ops: &[MigrationOp],
    from: &SchemaNode,
    to: &SchemaNode,
    edge: EdgeKind,
    prefix: &str,
    errors: &mut Vec<PlanError>,
) {
    // Output writer paths (frame-relative segments). MapVariant groups
    // collapse to one writer per `at`.
    type MvGroup<'a> = (Vec<String>, Vec<(&'a str, &'a str, &'a [MigrationOp])>);
    let mut writers: Vec<Vec<String>> = Vec::new();
    let mut mv_groups: Vec<MvGroup<'_>> = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CopyField { from: f, to: t } => {
                if resolve_path(from, f).is_none() {
                    errors.push(PlanError::MissingFromPath {
                        path: disp(prefix, f),
                    });
                }
                check_to(t, to, prefix, &mut writers, errors);
            }
            MigrationOp::Widen { from: f, to: t } => {
                let fnode = resolve_path(from, f);
                if fnode.is_none() {
                    errors.push(PlanError::MissingFromPath {
                        path: disp(prefix, f),
                    });
                }
                let tnode = resolve_path(to, t);
                if tnode.is_none() {
                    errors.push(PlanError::MissingToPath {
                        path: disp(prefix, t),
                    });
                } else {
                    writers.push(t.0.clone());
                }
                if let (Some(fnode), Some(tnode)) = (fnode, tnode) {
                    // Endpoints must be related by the SHARED widening
                    // table (ngp_schema::migrate) — i128/u128 are never
                    // targets, narrowing/cross-sign/int<->float never pass.
                    let ok = matches!(
                        (fnode, tnode),
                        (SchemaNode::Primitive(a), SchemaNode::Primitive(b))
                            if can_widen_int(*a, *b).is_some() || can_widen_float(*a, *b)
                    );
                    if !ok {
                        errors.push(PlanError::InvalidWiden {
                            path: disp(prefix, t),
                            detail: "endpoints are not primitives related by the widening table"
                                .to_string(),
                        });
                    }
                }
            }
            // PINNED: WriteValue is legal in BOTH edge kinds — a literal
            // is self-contained and replays byte-identically forever.
            MigrationOp::WriteValue { to: t, .. } => {
                check_to(t, to, prefix, &mut writers, errors);
            }
            MigrationOp::WriteFieldDefault { to: t } => {
                if edge == EdgeKind::Custom {
                    errors.push(PlanError::DefaultOpInCustomEdge {
                        path: disp(prefix, t),
                        op: "WriteFieldDefault",
                    });
                }
                check_to(t, to, prefix, &mut writers, errors);
            }
            MigrationOp::WriteParentDefault { to: t } => {
                if edge == EdgeKind::Custom {
                    errors.push(PlanError::DefaultOpInCustomEdge {
                        path: disp(prefix, t),
                        op: "WriteParentDefault",
                    });
                }
                check_to(t, to, prefix, &mut writers, errors);
            }
            MigrationOp::WriteNone { to: t } => {
                match resolve_path(to, t) {
                    None => errors.push(PlanError::MissingToPath {
                        path: disp(prefix, t),
                    }),
                    Some(node) => {
                        // PINNED: WriteNone targets Option nodes only
                        // (Unit's null is spelled WriteValue(Null)).
                        if !matches!(node, SchemaNode::Option(_)) {
                            errors.push(PlanError::WriteNoneTargetNotOption {
                                path: disp(prefix, t),
                            });
                        }
                        writers.push(t.0.clone());
                    }
                }
            }
            // DropField documents an unmatched INPUT path; writes nothing.
            MigrationOp::DropField { at } => {
                if resolve_path(from, at).is_none() {
                    errors.push(PlanError::MissingFromPath {
                        path: disp(prefix, at),
                    });
                }
            }
            MigrationOp::MapVariant {
                at,
                from: fv,
                to: tv,
                payload,
            } => {
                if let Some(group) = mv_groups.iter_mut().find(|(p, _)| *p == at.0) {
                    group.1.push((fv, tv, payload));
                } else {
                    mv_groups.push((at.0.clone(), vec![(fv, tv, payload)]));
                }
            }
            MigrationOp::MigrateElements { at, element } => {
                let Some((fnode, tnode)) = resolve_at(from, to, at, prefix, errors) else {
                    continue;
                };
                writers.push(at.0.clone());
                let sub = format!("{}[]", disp(prefix, at));
                // PINNED: MigrateElements requires the SAME container kind
                // on both sides; shape changes are function-edge work.
                match (fnode, tnode) {
                    (SchemaNode::Vec(a), SchemaNode::Vec(b))
                    | (SchemaNode::Option(a), SchemaNode::Option(b))
                    | (SchemaNode::Set(a), SchemaNode::Set(b)) => {
                        validate_frame(element, a, b, edge, &sub, errors);
                    }
                    (
                        SchemaNode::Array { len: l1, elem: a },
                        SchemaNode::Array { len: l2, elem: b },
                    ) => {
                        if l1 != l2 {
                            errors.push(PlanError::ContainerMismatch {
                                path: disp(prefix, at),
                                detail: format!("array length differs ({l1} vs {l2})"),
                            });
                        }
                        validate_frame(element, a, b, edge, &sub, errors);
                    }
                    // Map: MigrateElements migrates VALUES (§11).
                    (SchemaNode::Map { value: v1, .. }, SchemaNode::Map { value: v2, .. }) => {
                        validate_frame(element, v1, v2, edge, &sub, errors);
                    }
                    _ => errors.push(PlanError::ContainerMismatch {
                        path: disp(prefix, at),
                        detail: "MigrateElements requires matching container kinds \
                                 (vec/array/option/set/map) on both sides"
                            .to_string(),
                    }),
                }
            }
            MigrationOp::MigrateMapKeys { at, key } => {
                let Some((fnode, tnode)) = resolve_at(from, to, at, prefix, errors) else {
                    continue;
                };
                writers.push(at.0.clone());
                match (fnode, tnode) {
                    (SchemaNode::Map { key: k1, .. }, SchemaNode::Map { key: k2, .. }) => {
                        let sub = format!("{}[key]", disp(prefix, at));
                        validate_frame(key, k1, k2, edge, &sub, errors);
                    }
                    _ => errors.push(PlanError::ContainerMismatch {
                        path: disp(prefix, at),
                        detail: "MigrateMapKeys requires a map on both sides".to_string(),
                    }),
                }
            }
            MigrationOp::MigrateInline { at, ops: inner } => {
                let Some((fnode, tnode)) = resolve_at(from, to, at, prefix, errors) else {
                    continue;
                };
                writers.push(at.0.clone());
                // PINNED: MigrateInline requires a Struct at `at` on both
                // sides — its inner frame is struct-shaped by definition.
                let mut ok = true;
                if !matches!(fnode, SchemaNode::Struct { .. }) {
                    errors.push(PlanError::NotAStruct {
                        path: disp(prefix, at),
                        side: SchemaSide::From,
                    });
                    ok = false;
                }
                if !matches!(tnode, SchemaNode::Struct { .. }) {
                    errors.push(PlanError::NotAStruct {
                        path: disp(prefix, at),
                        side: SchemaSide::To,
                    });
                    ok = false;
                }
                if ok {
                    validate_frame(inner, fnode, tnode, edge, &disp(prefix, at), errors);
                }
            }
        }
    }

    // MapVariant groups: one composite writer per `at`; froms distinct.
    for (at_segs, group) in &mv_groups {
        let at = FieldPath(at_segs.clone());
        let Some((fnode, tnode)) = resolve_at(from, to, &at, prefix, errors) else {
            continue;
        };
        writers.push(at_segs.clone());
        let (SchemaNode::Enum { variants: fv, .. }, SchemaNode::Enum { variants: tv, .. }) =
            (fnode, tnode)
        else {
            if !matches!(fnode, SchemaNode::Enum { .. }) {
                errors.push(PlanError::NotAnEnum {
                    path: disp(prefix, &at),
                    side: SchemaSide::From,
                });
            }
            if !matches!(tnode, SchemaNode::Enum { .. }) {
                errors.push(PlanError::NotAnEnum {
                    path: disp(prefix, &at),
                    side: SchemaSide::To,
                });
            }
            continue;
        };
        let mut seen_froms: Vec<&str> = Vec::new();
        for (vfrom, vto, payload) in group {
            if seen_froms.contains(vfrom) {
                errors.push(PlanError::DuplicateMapVariantFrom {
                    at: disp(prefix, &at),
                    from: vfrom.to_string(),
                });
            }
            seen_froms.push(vfrom);
            let fpayload = fv.iter().find(|(n, _, _)| n == vfrom).map(|(_, _, p)| p);
            if fpayload.is_none() {
                errors.push(PlanError::UnknownVariant {
                    at: disp(prefix, &at),
                    variant: vfrom.to_string(),
                    side: SchemaSide::From,
                });
            }
            let tpayload = tv.iter().find(|(n, _, _)| n == vto).map(|(_, _, p)| p);
            if tpayload.is_none() {
                errors.push(PlanError::UnknownVariant {
                    at: disp(prefix, &at),
                    variant: vto.to_string(),
                    side: SchemaSide::To,
                });
            }
            if let (Some(fp), Some(tp)) = (fpayload, tpayload) {
                let sub = format!("{}{{{vfrom}}}", disp(prefix, &at));
                validate_frame(payload, fp, tp, edge, &sub, errors);
            }
        }
    }

    // Total and disjoint output coverage (§11): every coverage leaf of the
    // to-schema written exactly once. A writer covers all leaves at or
    // under its path; DropField covers nothing.
    let mut leaves = Vec::new();
    collect_leaves(to, Vec::new(), &mut leaves);
    for leaf in &leaves {
        let count = writers
            .iter()
            .filter(|w| leaf.len() >= w.len() && leaf[..w.len()] == w[..])
            .count();
        let leaf_disp = disp(prefix, &FieldPath(leaf.clone()));
        if count == 0 {
            errors.push(PlanError::UncoveredLeaf { path: leaf_disp });
        } else if count > 1 {
            errors.push(PlanError::DoubleCoveredLeaf { path: leaf_disp });
        }
    }
    // PINNED: equal writer paths that cover ZERO leaves (a zero-field
    // struct target) are still duplicates — the executor's fresh-output
    // model has exactly one slot per path.
    for (i, w) in writers.iter().enumerate() {
        if writers[..i].contains(w)
            && !leaves
                .iter()
                .any(|leaf| leaf.len() >= w.len() && leaf[..w.len()] == w[..])
        {
            errors.push(PlanError::DuplicateWriter {
                path: disp(prefix, &FieldPath(w.clone())),
            });
        }
    }
}

fn check_to(
    t: &FieldPath,
    to: &SchemaNode,
    prefix: &str,
    writers: &mut Vec<Vec<String>>,
    errors: &mut Vec<PlanError>,
) {
    if resolve_path(to, t).is_none() {
        errors.push(PlanError::MissingToPath {
            path: disp(prefix, t),
        });
    } else {
        writers.push(t.0.clone());
    }
}

/// `at` paths must resolve in BOTH schemas (§11).
fn resolve_at<'a>(
    from: &'a SchemaNode,
    to: &'a SchemaNode,
    at: &FieldPath,
    prefix: &str,
    errors: &mut Vec<PlanError>,
) -> Option<(&'a SchemaNode, &'a SchemaNode)> {
    let f = resolve_path(from, at);
    if f.is_none() {
        errors.push(PlanError::MissingAtPath {
            path: disp(prefix, at),
            side: SchemaSide::From,
        });
    }
    let t = resolve_path(to, at);
    if t.is_none() {
        errors.push(PlanError::MissingAtPath {
            path: disp(prefix, at),
            side: SchemaSide::To,
        });
    }
    Some((f?, t?))
}

/// Coverage leaves: paths from the frame root through Struct fields only
/// whose node is not a Struct (§11 pinned). A zero-field struct
/// contributes no leaves — it is trivially covered.
fn collect_leaves(node: &SchemaNode, path: Vec<String>, out: &mut Vec<Vec<String>>) {
    match node {
        SchemaNode::Struct { fields, .. } => {
            for (name, _, fnode) in fields {
                let mut p = path.clone();
                p.push(name.clone());
                collect_leaves(fnode, p, out);
            }
        }
        _ => out.push(path),
    }
}

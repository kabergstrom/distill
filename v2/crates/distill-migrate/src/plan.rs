//! The snapshot-AST automatic planner (§11): diff two schema ASTs into a
//! logical migration plan, refusing anything it cannot prove safe.
//!
//! Matching identity (PINNED, §11 "matches structurally"): struct fields
//! and enum variants match by NAME (a new field with no name match also
//! matches the old field its `renamed_from` names); a matched entry whose
//! rev differs is a HARD STOP (R20/H8) — `rev` marks same-shape/new-meaning,
//! and both silent options (copying = reinterpretation, drop+default =
//! data loss) are exactly what rev exists to prevent, so a custom
//! migration edge is required. Containers match by position/kind;
//! `BackRef` matches `BackRef` of equal distance. Any other node-kind
//! change at a matched path is a refusal — a custom edge is required.
//!
//! Further pins beyond the §11 text (marked PINNED at the sites):
//! - A TYPE-level rev change (`Struct{rev}` / `Enum{rev}`) at a matched
//!   path is likewise a refusal — the author declared "a different type"
//!   for the whole node.
//! - An enum that differs only by added variants and dropped DATA-FREE
//!   variants plans to a plain `CopyField`: every surviving variant
//!   passes through unchanged (a dropped variant present in actual data
//!   surfaces at output conformance, §11 integrity).
//! - Identity is *meaning*-aware ([`crate::identical`]): a subtree whose
//!   escaping back-references re-enter a changed frame is NOT identical;
//!   the recursion bottoms out at the `BackRef` pair as a refusal —
//!   automatic plans cannot express recursive migration (a function edge
//!   can).

use crate::identical::meaning_identical;
use crate::{FieldPath, MigrationOp};
use ngp_schema::migrate::{can_widen_float, can_widen_int};
use ngp_schema::{Renames, SchemaNode};
use std::fmt;

/// The planner's refusal: a list of (path, reason) pairs naming every
/// place automatic migration is not provably safe — a custom edge is
/// required there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanRefusal {
    pub reasons: Vec<(String, String)>,
}

impl fmt::Display for PlanRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "automatic migration refused:")?;
        for (path, reason) in &self.reasons {
            writeln!(f, "  at {path}: {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PlanRefusal {}

/// Diff `old` against `new` into a logical migration plan.
pub fn plan_automatic(old: &SchemaNode, new: &SchemaNode) -> Result<Vec<MigrationOp>, PlanRefusal> {
    plan_automatic_renamed(old, new, &Renames::new())
}

/// [`plan_automatic`] with the renamed fields of `new`
/// (`#[asset(renamed_from)]`, keyed by display path). A new field with no
/// old field of its own name matches the old field it was renamed from:
/// an identical or widened value is copied across; a rename whose value
/// also changed shape is refused, since nested ops cannot move a field.
pub fn plan_automatic_renamed(
    old: &SchemaNode,
    new: &SchemaNode,
    renames: &Renames,
) -> Result<Vec<MigrationOp>, PlanRefusal> {
    let mut ctx = Ctx {
        refusals: Vec::new(),
        renames,
    };

    let ops = ctx.diff(old, new, Vec::new(), "$", &[], &[]);
    if ctx.refusals.is_empty() {
        Ok(ops)
    } else {
        Err(PlanRefusal {
            reasons: ctx.refusals,
        })
    }
}

struct Ctx<'a> {
    refusals: Vec<(String, String)>,
    renames: &'a Renames,
}

fn kind_name(n: &SchemaNode) -> &'static str {
    match n {
        SchemaNode::Primitive(_) => "primitive",
        SchemaNode::Struct { .. } => "struct",
        SchemaNode::Enum { .. } => "enum",
        SchemaNode::Vec(_) => "vec",
        SchemaNode::Array { .. } => "array",
        SchemaNode::Option(_) => "option",
        SchemaNode::Map { .. } => "map",
        SchemaNode::String => "string",
        SchemaNode::AssetRef(_) => "asset reference",
        SchemaNode::WeakRef(_) => "weak reference",
        SchemaNode::Blob => "blob",
        SchemaNode::BackRef(_) => "back-reference",
        SchemaNode::Unit => "unit",
        SchemaNode::Set(_) => "set",
    }
}

impl Ctx<'_> {
    fn refuse(&mut self, disp: &str, reason: impl Into<String>) {
        self.refusals.push((disp.to_string(), reason.into()));
    }

    /// Diff two matched nodes. `at` is the op path within the current
    /// EXECUTION frame (always 0 or 1 segments here — nested structures
    /// get their own frames via MigrateInline/MigrateElements/MapVariant);
    /// `disp` is the full display path for refusals; the frame slices are
    /// the back-reference stacks (expansion frames, a separate concept
    /// from execution frames — they persist across containers).
    #[allow(clippy::too_many_arguments)]
    fn diff(
        &mut self,
        old: &SchemaNode,
        new: &SchemaNode,
        at: Vec<String>,
        disp: &str,
        old_frames: &[&SchemaNode],
        new_frames: &[&SchemaNode],
    ) -> Vec<MigrationOp> {
        // Identical subtree at a matched path → CopyField (§11).
        if meaning_identical(old, new, old_frames, new_frames) {
            return vec![MigrationOp::CopyField {
                from: FieldPath(at.clone()),
                to: FieldPath(at),
            }];
        }
        match (old, new) {
            (SchemaNode::Primitive(a), SchemaNode::Primitive(b)) => {
                // The SHARED widening table (ngp_schema::migrate, §11):
                // i128/u128 are never targets — the table already says so.
                if can_widen_int(*a, *b).is_some() || can_widen_float(*a, *b) {
                    vec![MigrationOp::Widen {
                        from: FieldPath(at.clone()),
                        to: FieldPath(at),
                    }]
                } else {
                    self.refuse(
                        disp,
                        format!(
                            "primitive change {} -> {} is not a widening",
                            a.canonical_name(),
                            b.canonical_name()
                        ),
                    );
                    Vec::new()
                }
            }
            (
                SchemaNode::Struct {
                    rev: r1,
                    fields: f1,
                },
                SchemaNode::Struct {
                    rev: r2,
                    fields: f2,
                },
            ) => {
                if r1 != r2 {
                    // PINNED: type-level rev bump = refusal (see module doc).
                    self.refuse(
                        disp,
                        format!("struct rev changed ({r1} -> {r2}): treated as a different type"),
                    );
                    return Vec::new();
                }
                let mut of = old_frames.to_vec();
                of.push(old);
                let mut nf = new_frames.to_vec();
                nf.push(new);
                let inner = self.diff_struct_fields(f1, f2, disp, &of, &nf);
                if at.is_empty() {
                    // This struct IS the execution frame's root: its field
                    // ops are the frame's ops.
                    inner
                } else {
                    vec![MigrationOp::MigrateInline {
                        at: FieldPath(at),
                        ops: inner,
                    }]
                }
            }
            (
                SchemaNode::Enum {
                    rev: r1,
                    variants: v1,
                },
                SchemaNode::Enum {
                    rev: r2,
                    variants: v2,
                },
            ) => {
                if r1 != r2 {
                    self.refuse(
                        disp,
                        format!("enum rev changed ({r1} -> {r2}): treated as a different type"),
                    );
                    return Vec::new();
                }
                self.diff_enum(old, new, v1, v2, at, disp, old_frames, new_frames)
            }
            (SchemaNode::Vec(a), SchemaNode::Vec(b))
            | (SchemaNode::Option(a), SchemaNode::Option(b))
            | (SchemaNode::Set(a), SchemaNode::Set(b)) => {
                let elem = self.diff(
                    a,
                    b,
                    Vec::new(),
                    &format!("{disp}[]"),
                    old_frames,
                    new_frames,
                );
                vec![MigrationOp::MigrateElements {
                    at: FieldPath(at),
                    element: elem,
                }]
            }
            (SchemaNode::Array { len: l1, elem: a }, SchemaNode::Array { len: l2, elem: b }) => {
                if l1 != l2 {
                    self.refuse(disp, format!("array length changed ({l1} -> {l2})"));
                    return Vec::new();
                }
                let elem = self.diff(
                    a,
                    b,
                    Vec::new(),
                    &format!("{disp}[]"),
                    old_frames,
                    new_frames,
                );
                vec![MigrationOp::MigrateElements {
                    at: FieldPath(at),
                    element: elem,
                }]
            }
            (SchemaNode::Map { key: k1, value: v1 }, SchemaNode::Map { key: k2, value: v2 }) => {
                let key_same = meaning_identical(k1, k2, old_frames, new_frames);
                let val_same = meaning_identical(v1, v2, old_frames, new_frames);
                match (key_same, val_same) {
                    (true, false) => {
                        let elem = self.diff(
                            v1,
                            v2,
                            Vec::new(),
                            &format!("{disp}[]"),
                            old_frames,
                            new_frames,
                        );
                        vec![MigrationOp::MigrateElements {
                            at: FieldPath(at),
                            element: elem,
                        }]
                    }
                    (false, true) => {
                        let key = self.diff(
                            k1,
                            k2,
                            Vec::new(),
                            &format!("{disp}[key]"),
                            old_frames,
                            new_frames,
                        );
                        vec![MigrationOp::MigrateMapKeys {
                            at: FieldPath(at),
                            key,
                        }]
                    }
                    (false, false) => {
                        self.refuse(disp, "both map key and value changed");
                        Vec::new()
                    }
                    // Unreachable (identity would have fired); copy is the
                    // safe answer if it somehow isn't.
                    (true, true) => vec![MigrationOp::CopyField {
                        from: FieldPath(at.clone()),
                        to: FieldPath(at),
                    }],
                }
            }
            (SchemaNode::BackRef(d1), SchemaNode::BackRef(d2)) => {
                if d1 != d2 {
                    self.refuse(
                        disp,
                        format!("back-reference distance changed ({d1} -> {d2})"),
                    );
                } else {
                    // Equal distance but not meaning-identical: the frame
                    // it re-enters changed. Automatic plans cannot express
                    // recursion (PINNED — see module doc).
                    self.refuse(
                        disp,
                        "recursive reference into a changed type: automatic plans cannot \
                         recurse; author a custom migration",
                    );
                }
                Vec::new()
            }
            (SchemaNode::AssetRef(a), SchemaNode::AssetRef(b))
            | (SchemaNode::WeakRef(a), SchemaNode::WeakRef(b)) => {
                // Same kind, different target: retargeting changes meaning
                // and must be a deliberate custom edge (§5).
                self.refuse(disp, format!("reference retargeted ({a} -> {b})"));
                Vec::new()
            }
            (o, n) => {
                self.refuse(
                    disp,
                    format!("node kind changed ({} -> {})", kind_name(o), kind_name(n)),
                );
                Vec::new()
            }
        }
    }

    /// Field matching by name, then by `renamed_from`; a matched field
    /// whose rev differs is a HARD STOP (review R20/H8): `rev` marks
    /// same-shape/new-meaning, and
    /// both silent options — copying (reinterpretation) and drop+default
    /// (data loss) — are exactly what rev exists to prevent. A custom
    /// migration edge is required.
    fn diff_struct_fields(
        &mut self,
        old_fields: &[(String, u32, SchemaNode)],
        new_fields: &[(String, u32, SchemaNode)],
        disp: &str,
        old_frames: &[&SchemaNode],
        new_frames: &[&SchemaNode],
    ) -> Vec<MigrationOp> {
        let mut ops = Vec::new();
        let mut renamed = Vec::new();
        let renames = self.renames;
        for (nname, nrev, nnode) in new_fields {
            let ndisp = format!("{disp}.{nname}");
            // Names are unique per side (the grammar rejects duplicates), so
            // by-name lookup is total matching. A rename applies only when
            // neither side still has a field of the old name.
            let by_name = old_fields.iter().find(|(oname, _, _)| oname == nname);
            let by_rename = || {
                let old = renames.get(&ndisp)?;
                if new_fields.iter().any(|(name, _, _)| name == old) {
                    return None;
                }
                old_fields.iter().find(|(oname, _, _)| oname == old)
            };
            let from_rename = by_name.is_none();
            match by_name.or_else(by_rename) {
                Some((oname, _, _)) if from_rename && renamed.contains(oname) => {
                    self.refuse(&ndisp, format!("field {oname:?} is renamed to two fields"));
                }
                Some((oname, orev, onode)) if orev == nrev => {
                    let inner = self.diff(
                        onode,
                        nnode,
                        vec![nname.clone()],
                        &ndisp,
                        old_frames,
                        new_frames,
                    );
                    if from_rename {
                        renamed.push(oname.clone());
                        ops.extend(self.rename(inner, oname, nname, &ndisp));
                    } else {
                        ops.extend(inner);
                    }
                }
                Some((_, orev, _)) => {

                    self.refuse(
                        &ndisp,
                        format!(
                            "semantic revision changed ({orev} -> {nrev}): \
                             custom migration edge required"
                        ),
                    );
                }
                // New-only field: Option → WriteNone, else the field
                // type's default (§11).
                None => ops.push(if matches!(nnode, SchemaNode::Option(_)) {
                    MigrationOp::WriteNone {
                        to: FieldPath(vec![nname.clone()]),
                    }
                } else {
                    MigrationOp::WriteFieldDefault {
                        to: FieldPath(vec![nname.clone()]),
                    }
                }),
            }
        }
        for (oname, _, _) in old_fields {
            // Name-matched fields were handled above (diffed or refused);
            // only a truly name-absent field is a documented drop.
            let name_present = new_fields.iter().any(|(nname, _, _)| nname == oname);
            if !name_present && !renamed.contains(oname) {
                ops.push(MigrationOp::DropField {
                    at: FieldPath(vec![oname.clone()]),
                });
            }
        }
        ops
    }

    /// Point a renamed field's diff at its old name: a copy or a widening
    /// moves; anything nested is refused, since nested ops read and write
    /// the same path.
    fn rename(
        &mut self,
        ops: Vec<MigrationOp>,
        old: &str,
        new: &str,
        disp: &str,
    ) -> Vec<MigrationOp> {
        let (from, to) = (FieldPath(vec![old.to_owned()]), FieldPath(vec![new.to_owned()]));
        match ops.as_slice() {
            [MigrationOp::CopyField { .. }] => vec![MigrationOp::CopyField { from, to }],
            [MigrationOp::Widen { .. }] => vec![MigrationOp::Widen { from, to }],
            // The diff already refused.
            [] => Vec::new(),
            _ => {
                self.refuse(
                    disp,
                    format!(
                        "renamed from {old:?} and changed shape: register a migration function"
                    ),
                );
                Vec::new()
            }
        }
    }

    /// Variant matching by (name, rev). Per changed matched variant emit
    /// `MapVariant{from == to == name}`; an unmatched old variant carrying
    /// data is a refusal; a data-free one is silently droppable; new-only
    /// variants need no op. If nothing changed among matched variants the
    /// enum leaf is covered by a plain copy (PINNED — module doc).
    #[allow(clippy::too_many_arguments)]
    fn diff_enum(
        &mut self,
        old: &SchemaNode,
        new: &SchemaNode,
        old_variants: &[(String, u32, SchemaNode)],
        new_variants: &[(String, u32, SchemaNode)],
        at: Vec<String>,
        disp: &str,
        old_frames: &[&SchemaNode],
        new_frames: &[&SchemaNode],
    ) -> Vec<MigrationOp> {
        let refusals_before = self.refusals.len();
        let mut of = old_frames.to_vec();
        of.push(old);
        let mut nf = new_frames.to_vec();
        nf.push(new);
        let mut ops = Vec::new();
        for (nname, nrev, npayload) in new_variants {
            let by_name = old_variants.iter().find(|(oname, _, _)| oname == nname);
            let Some((_, orev, opayload)) = by_name else {
                continue; // new-only variants need no op
            };
            let vdisp = format!("{disp}{{{nname}}}");
            // Name-matched variant with a differing rev: hard stop
            // (R20/H8), same rule as struct fields.
            if orev != nrev {
                self.refuse(
                    &vdisp,
                    format!(
                        "semantic revision changed ({orev} -> {nrev}): \
                         custom migration edge required"
                    ),
                );
                continue;
            }
            match (opayload, npayload) {
                (
                    SchemaNode::Struct {
                        rev: pr1,
                        fields: pf1,
                    },
                    SchemaNode::Struct {
                        rev: pr2,
                        fields: pf2,
                    },
                ) => {
                    if payload_identical(opayload, npayload, &of, &nf) {
                        continue; // unchanged variants pass through
                    }
                    if pr1 != pr2 {
                        self.refuse(
                            &vdisp,
                            format!(
                                "variant payload rev changed ({pr1} -> {pr2}): treated as a \
                                 different type"
                            ),
                        );
                        continue;
                    }
                    // Payload fields are diffed under the ENUM's frame
                    // (§5: the payload struct is not a separate frame).
                    let payload_ops = self.diff_struct_fields(pf1, pf2, &vdisp, &of, &nf);
                    ops.push(MigrationOp::MapVariant {
                        at: FieldPath(at.clone()),
                        from: nname.clone(),
                        to: nname.clone(),
                        payload: payload_ops,
                    });
                }
                _ => {
                    self.refuse(&vdisp, "malformed grammar: variant payload is not a struct");
                }
            }
        }
        for (oname, _, opayload) in old_variants {
            // Name-matched variants were handled above (mapped or
            // rev-refused); only a truly name-absent variant is a removal.
            let name_present = new_variants.iter().any(|(nname, _, _)| nname == oname);
            if name_present {
                continue;
            }
            let has_data = matches!(
                opayload,
                SchemaNode::Struct { fields, .. } if !fields.is_empty()
            );
            if has_data {
                // unmatched + data = error (§11).
                self.refuse(
                    &format!("{disp}{{{oname}}}"),
                    format!("variant {oname:?} was removed but carries data"),
                );
            }
            // Data-free: silently droppable — if actual data holds it, the
            // output conformance check surfaces it (§11 integrity).
        }
        if ops.is_empty() && self.refusals.len() == refusals_before {
            // Only additions / droppable removals: surviving variants pass
            // through — one plain copy covers the enum leaf (PINNED).
            return vec![MigrationOp::CopyField {
                from: FieldPath(at.clone()),
                to: FieldPath(at),
            }];
        }
        ops
    }
}

/// Meaning-identity for variant payload structs: compare bodies under the
/// enum's frame (no extra frame for the payload node, §5).
fn payload_identical(
    p1: &SchemaNode,
    p2: &SchemaNode,
    old_frames: &[&SchemaNode],
    new_frames: &[&SchemaNode],
) -> bool {
    match (p1, p2) {
        (
            SchemaNode::Struct {
                rev: r1,
                fields: f1,
            },
            SchemaNode::Struct {
                rev: r2,
                fields: f2,
            },
        ) => {
            r1 == r2
                && f1.len() == f2.len()
                && f1
                    .iter()
                    .zip(f2.iter())
                    .all(|((n1, v1, s1), (n2, v2, s2))| {
                        n1 == n2 && v1 == v2 && meaning_identical(s1, s2, old_frames, new_frames)
                    })
        }
        _ => false,
    }
}

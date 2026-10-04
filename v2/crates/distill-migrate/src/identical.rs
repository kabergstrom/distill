//! Meaning-aware schema identity and path resolution — shared by the
//! planner, the validator, and the executor.
//!
//! PINNED (matching identity, §11 "matches structurally"): struct fields
//! and enum variants match by name AND rev; containers by position/kind;
//! `BackRef` matches `BackRef` of equal distance — but a subtree is only
//! *meaning*-identical if every back-reference that escapes it resolves
//! to frames that are themselves meaning-identical. Plain `==` on two
//! `BackRef(0)` nodes would call a recursive field of a *changed* type
//! unchanged; this module compares through the frame stacks instead.

use crate::FieldPath;
use distill_core::frames::resolve_backref;
use ngp_schema::SchemaNode;

/// Resolve a struct-field-name path against a schema subtree. Navigation
/// is through `Struct` fields ONLY (§11: container/variant recursion is
/// nested ops); field lookup is by name (the rev is identity for
/// *matching*, not for addressing). `None` = the path does not resolve.
pub fn resolve_path<'a>(root: &'a SchemaNode, path: &FieldPath) -> Option<&'a SchemaNode> {
    let mut node = root;
    for seg in &path.0 {
        match node {
            SchemaNode::Struct { fields, .. } => {
                node = fields
                    .iter()
                    .find(|(name, _, _)| name == seg)
                    .map(|(_, _, n)| n)?;
            }
            _ => return None,
        }
    }
    Some(node)
}

/// Are two subtrees at matched positions identical *in meaning*?
///
/// Structural equality plus: a `BackRef` escaping the compared subtrees
/// must resolve (through the supplied enclosing frame stacks, which are
/// always the same depth on both sides — the planner only recurses
/// through matched frames) to frames that are recursively
/// meaning-identical. Cycles are closed coinductively: a frame pair
/// already under comparison is assumed identical.
pub fn meaning_identical(
    old: &SchemaNode,
    new: &SchemaNode,
    old_frames: &[&SchemaNode],
    new_frames: &[&SchemaNode],
) -> bool {
    let mut in_progress = Vec::new();
    ident(old, new, old_frames, new_frames, &mut in_progress)
}

type PtrPair = (*const SchemaNode, *const SchemaNode);

fn ident(
    old: &SchemaNode,
    new: &SchemaNode,
    old_frames: &[&SchemaNode],
    new_frames: &[&SchemaNode],
    in_progress: &mut Vec<PtrPair>,
) -> bool {
    use SchemaNode::*;
    match (old, new) {
        (Primitive(a), Primitive(b)) => a == b,
        (String, String) | (Blob, Blob) | (Unit, Unit) => true,
        (AssetRef(a), AssetRef(b)) | (WeakRef(a), WeakRef(b)) => a == b,
        (
            Struct {
                rev: r1,
                fields: f1,
            },
            Struct {
                rev: r2,
                fields: f2,
            },
        ) => {
            if r1 != r2 || f1.len() != f2.len() {
                return false;
            }
            let mut of = old_frames.to_vec();
            of.push(old);
            let mut nf = new_frames.to_vec();
            nf.push(new);
            f1.iter()
                .zip(f2.iter())
                .all(|((n1, v1, s1), (n2, v2, s2))| {
                    n1 == n2 && v1 == v2 && ident(s1, s2, &of, &nf, in_progress)
                })
        }
        (
            Enum {
                rev: r1,
                variants: v1,
            },
            Enum {
                rev: r2,
                variants: v2,
            },
        ) => {
            if r1 != r2 || v1.len() != v2.len() {
                return false;
            }
            let mut of = old_frames.to_vec();
            of.push(old);
            let mut nf = new_frames.to_vec();
            nf.push(new);
            v1.iter()
                .zip(v2.iter())
                .all(|((n1, r1, p1), (n2, r2, p2))| {
                    // Variant payloads are struct nodes but NOT separate frames
                    // (§5): compare their bodies under the enum's frame.
                    n1 == n2 && r1 == r2 && payload_ident(p1, p2, &of, &nf, in_progress)
                })
        }
        (Vec(a), Vec(b)) | (Option(a), Option(b)) | (Set(a), Set(b)) => {
            ident(a, b, old_frames, new_frames, in_progress)
        }
        (Array { len: l1, elem: a }, Array { len: l2, elem: b }) => {
            l1 == l2 && ident(a, b, old_frames, new_frames, in_progress)
        }
        (Map { key: k1, value: v1 }, Map { key: k2, value: v2 }) => {
            ident(k1, k2, old_frames, new_frames, in_progress)
                && ident(v1, v2, old_frames, new_frames, in_progress)
        }
        (BackRef(d1), BackRef(d2)) => {
            if d1 != d2 {
                return false;
            }
            let (Some((&fo, old_above)), Some((&fnew, new_above))) = (
                resolve_backref(old_frames, *d1),
                resolve_backref(new_frames, *d2),
            ) else {
                return false; // malformed: unresolvable back-reference
            };
            if std::ptr::eq(fo, fnew) {
                return true;
            }
            let key: PtrPair = (fo as *const _, fnew as *const _);
            if in_progress.contains(&key) {
                return true; // coinductive: assume identical on the cycle
            }
            in_progress.push(key);
            let r = ident(fo, fnew, old_above, new_above, in_progress);
            in_progress.pop();
            r
        }
        _ => false,
    }
}

/// Compare enum variant payload structs WITHOUT opening a frame for the
/// payload struct node itself (§5: the enum's frame covers the payload).
fn payload_ident(
    p1: &SchemaNode,
    p2: &SchemaNode,
    old_frames: &[&SchemaNode],
    new_frames: &[&SchemaNode],
    in_progress: &mut Vec<PtrPair>,
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
                        n1 == n2 && v1 == v2 && ident(s1, s2, old_frames, new_frames, in_progress)
                    })
        }
        // Malformed grammar (payload must be a struct node) — never identical.
        _ => false,
    }
}

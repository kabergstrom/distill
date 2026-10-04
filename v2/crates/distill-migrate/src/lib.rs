//! §11 Migration System — the disk-side machinery.
//!
//! This crate holds the pieces `load_current` composes (§11): the
//! snapshot-AST automatic planner ([`plan_automatic`]), the plan validator
//! ([`validate_plan`]), the `AuthoredValue` executor ([`execute_ops`],
//! [`execute_edge`]), the conformance checker ([`conforms`]), and the
//! migration-graph walk ([`select_chain`]). Orchestration (LoadContext,
//! MetadataSnapshot) belongs to a later crate.
//!
//! Pinned semantics are documented at each definition; the normative text
//! is DESIGN.md §11.

use distill_core::id::{LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use std::fmt;

mod conform;
mod execute;
mod identical;
mod lossy;
mod plan;
mod validate;
mod walk;

pub use conform::{conforms, ConformError};
pub use execute::{
    execute_edge, execute_ops, execute_sparse, DefaultProvider, Exec, FnProvider, MigrationError,
};
pub use identical::resolve_path;
pub use lossy::{lossy_drops, zero_value};
pub use plan::{plan_automatic, plan_automatic_renamed, PlanRefusal};
pub use validate::{validate_plan, EdgeKind, PlanError};
pub use walk::{select_chain, EdgeRef, SelectedChain, WalkError};

/// Struct-field-name navigation only; container/variant recursion is
/// nested ops (§11). An empty path addresses the current frame's root.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FieldPath(pub Vec<String>);

impl FieldPath {
    pub fn root() -> Self {
        FieldPath(Vec::new())
    }

    pub fn of(segments: &[&str]) -> Self {
        FieldPath(segments.iter().map(|s| s.to_string()).collect())
    }
}

/// Dot-joined for diagnostics; the empty (root) path displays as `$`.
impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("$")?;
        for seg in &self.0 {
            write!(f, ".{seg}")?;
        }
        Ok(())
    }
}

/// The path-addressed, value-level migration op vocabulary (§11, verbatim).
#[derive(Clone, Debug, PartialEq)]
pub enum MigrationOp {
    CopyField {
        from: FieldPath,
        to: FieldPath,
    },
    Widen {
        from: FieldPath,
        to: FieldPath,
    },
    /// A literal, materialized at edge-AUTHORING time — the only
    /// default-shaped op legal in custom edges (§11).
    WriteValue {
        to: FieldPath,
        value: AuthoredValue,
    },
    /// The field type's default — automatic segment only (§11).
    WriteFieldDefault {
        to: FieldPath,
    },
    /// The field's value from the parent type's `Default` — automatic
    /// segment only (§11).
    WriteParentDefault {
        to: FieldPath,
    },
    WriteNone {
        to: FieldPath,
    },
    /// Documents an intentionally unmatched INPUT path; writes nothing.
    DropField {
        at: FieldPath,
    },
    /// Unmatched + data = error (§11). The set of MapVariant ops sharing
    /// one `at` path is one composite writer of that path; input variants
    /// no op names pass through unchanged.
    MapVariant {
        at: FieldPath,
        from: String,
        to: String,
        payload: Vec<MigrationOp>,
    },
    /// Vec/array/Option/map values/set elements — sets rebuild through
    /// real equality; a post-migration duplicate element is an error,
    /// exactly the map-key collision rule (§11).
    MigrateElements {
        at: FieldPath,
        element: Vec<MigrationOp>,
    },
    /// Key collision = error (§11).
    MigrateMapKeys {
        at: FieldPath,
        key: Vec<MigrationOp>,
    },
    MigrateInline {
        at: FieldPath,
        ops: Vec<MigrationOp>,
    },
    // NOTE: there is deliberately no per-op custom escape hatch (R20/M21
    // removed `Custom { fn_key }`): it had no path, no endpoint schemas,
    // and no coverage participation. Whole-edge `MigrationKind::Function`
    // is the custom-logic carrier.
}

/// A custom migration edge, as declared by a Migration asset (§11).
#[derive(Clone, Debug, PartialEq)]
pub struct Migration {
    pub target_type_uuid: TypeUuid,
    pub from_hash: LogicalHash,
    pub to_hash: LogicalHash,
    pub kind: MigrationKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MigrationKind {
    /// Data-driven, in the logical-plan vocabulary above.
    Ops(Vec<MigrationOp>),
    /// Key of a MigrationFn registered in the pipeline module (§3).
    Function(String),
}

//! `PipelineDefaults` backed by an asset type's reflection (newgameplus
//! GAP 23 §18.2; §3, §11), so automatic migrations that add a field can
//! fill it from the field type's (or its container's) `Default`.
//!
//! The migration executor hands the provider a frame's to-schema root and a
//! frame-relative path through struct fields; `TypeReflect` resolves the
//! path's container node against the logical node of every struct and enum
//! variant payload of the type's closure and answers from its defaults.

use distill_asset::reflect::interp::TypeReflect;
use distill_asset::{AssetType, CallbackPanic};
use distill_migrate::FieldPath;
use ngp_schema::{SchemaNode, Value};

use crate::callbacks::PipelineDefaults;

pub struct AssetTableDefaults {
    reflect: &'static TypeReflect,
}

impl AssetTableDefaults {
    pub fn of<T: AssetType>() -> Self {
        Self { reflect: &T::info().reflect }
    }
}

fn answer(result: Option<Result<Value, CallbackPanic>>) -> Option<Value> {
    // Re-raise inside the callback thunk so the host records a callback
    // panic instead of a missing default.
    result.map(|r| r.unwrap_or_else(|_| panic!("asset default writer panicked")))
}

impl PipelineDefaults for AssetTableDefaults {
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value> {
        answer(self.reflect.field_default(to_schema, &at.0))
    }

    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value> {
        answer(self.reflect.parent_default(to_schema, &at.0))
    }
}

//! Asset reflection from the walk (newgameplus GAP 23 §18): one
//! interpreter ([`interp`]) over each asset crate's generated table
//! ([`table`]), and the typed entry points over it.

pub mod interp;
pub mod table;

use std::panic::{catch_unwind, AssertUnwindSafe};

use distill_wire::native::CallbackPanic;
use ngp_schema::node::LogicalSchema;
use ngp_schema::Value;

use crate::AssetType;

/// `value` in the typed model (`ngp_schema::Value`): struct fields and enum
/// variants in schema (name-sorted) order, map and set entries in the
/// container's iteration order (writers canonicalize).
pub fn to_value<T: AssetType>(value: &T) -> Value {
    // Safety: a live `T`, read through `T`'s own reflection.
    unsafe { T::info().reflect.to_value((value as *const T).cast()) }
}

/// `T`'s logical schema.
pub fn logical<T: AssetType>() -> &'static LogicalSchema {
    &T::info().reflect.logical
}

/// `T::default()` as a value; a panicking `Default` is an `Err`.
pub fn default_value<T: AssetType + Default>() -> Result<Value, CallbackPanic> {
    catch_unwind(AssertUnwindSafe(|| to_value(&T::default()))).map_err(|_| CallbackPanic)
}

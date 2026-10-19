//! Every field kind the interpreter reads: references, blobs, tags,
//! deterministic maps, skipped fields, revisions, renames.

use std::collections::BTreeMap;

use distill_asset::{AssetHashMap, AssetRef, Blob, WeakAssetRef};

#[derive(Default)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "11111111-2222-3333-8444-555555555555", rev = 3, build_only)]
pub struct Example {
    pub z: u16,
    pub name: String,
    pub values: Vec<u32>,
    pub map: AssetHashMap<u32, String>,
    #[asset(skip)]
    pub cache: BTreeMap<String, usize>,
}

#[derive(Default)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "01010101-0202-4303-8404-050505050505")]
pub struct Inner {
    pub number: u32,
}

#[derive(Default)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "11110101-0202-4303-8404-050505050505")]
pub struct Nested {
    pub values: Vec<Inner>,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "20000000-0000-4000-8000-000000000001")]
pub struct SameShapeA {
    pub value: u32,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "20000000-0000-4000-8000-000000000002")]
pub struct SameShapeB {
    pub value: u32,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "20000000-0000-4000-8000-000000000004")]
pub struct RenamedShape {
    #[asset(renamed_from = "old_value")]
    pub value: u32,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "20000000-0000-4000-8000-000000000003", rev = 1)]
pub struct RevisedShape {
    pub value: u32,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "40000000-0000-4000-8000-000000000001")]
pub struct ReferenceTarget {
    pub value: u32,
}

#[derive(distill_asset::Asset)]
#[asset(uuid = "40000000-0000-4000-8000-000000000002", build_only)]
pub struct SemanticFacts {
    pub strong: AssetRef<ReferenceTarget>,
    pub weak: WeakAssetRef<ReferenceTarget>,
    #[asset(blob)]
    pub payload: Blob,
    #[asset(tag)]
    pub label: String,
}

/// A fieldless enum with explicit discriminants: its tags are the values.
#[repr(u16)]
#[derive(Default)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "40000000-0000-4000-8000-000000000003")]
pub enum Level {
    Low = 7,
    #[default]
    High = 300,
}

//! distill-pipeline-api asset_defaults: migration defaults from the asset
//! table.

#[distill_asset::asset(uuid = "a5d00001-0000-4000-8000-000000000000")]
pub struct Outer {
    pub items: Vec<Inner>,
    pub mode: Mode,
}

#[distill_asset::asset(uuid = "a5d00002-0000-4000-8000-000000000000")]
pub struct Inner {
    pub a: u32,
    pub added: u32,
    pub name: String,
}

impl Default for Inner {
    fn default() -> Self {
        Self { a: 1, added: 5, name: "inner".into() }
    }
}

#[derive(Default)]
#[distill_asset::asset(uuid = "a5d00003-0000-4000-8000-000000000000")]
#[repr(u8)]
pub enum Mode {
    #[default]
    Off,
    On { level: u32, extra: u32 },
}

// The same types before `added` and `extra` existed. Logical nodes carry no
// type names, so these project to the old shapes of the types above.
#[distill_asset::asset(uuid = "a5d00011-0000-4000-8000-000000000000")]
pub struct OldOuter {
    pub items: Vec<OldInner>,
    pub mode: OldMode,
}

#[distill_asset::asset(uuid = "a5d00012-0000-4000-8000-000000000000")]
pub struct OldInner {
    pub a: u32,
    pub name: String,
}

#[distill_asset::asset(uuid = "a5d00013-0000-4000-8000-000000000000")]
#[repr(u8)]
pub enum OldMode {
    Off,
    On { level: u32 },
}

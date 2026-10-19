//! A recursive asset type, shaped like newgameplus's cue source tree: a
//! node re-entered inline through another record inside a `Vec`, through
//! enum variant payloads, and through an `Option<Box<_>>`.

#[derive(Debug, Default, PartialEq)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "70000000-0000-4000-8000-000000000001")]
pub struct Tree {
    pub root: Node,
    pub release: Option<Node>,
}

#[derive(Debug, Default, PartialEq)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "70000000-0000-4000-8000-000000000002")]
pub struct Node {
    pub kind: Kind,
    pub gain: u32,
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "70000000-0000-4000-8000-000000000003")]
pub enum Kind {
    Leaf { id: u32 },
    Many { children: Vec<Node> },
    Switch { children: Vec<Child> },
}

impl Default for Kind {
    fn default() -> Self {
        Kind::Leaf { id: 0 }
    }
}

/// Holds `Node` inline: re-entered through a record, not a slot.
#[derive(Debug, Default, PartialEq)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "70000000-0000-4000-8000-000000000004")]
pub struct Child {
    pub min: u32,
    pub node: Node,
    pub alt: Option<Box<Node>>,
}

/// Recursion through `Option<Box<Self>>`.
#[derive(Debug, Default, PartialEq)]
#[derive(distill_asset::Asset)]
#[asset(uuid = "12110101-0202-4303-8404-050505050505")]
pub struct Recursive {
    pub child: Option<Box<Recursive>>,
    pub number: u32,
}

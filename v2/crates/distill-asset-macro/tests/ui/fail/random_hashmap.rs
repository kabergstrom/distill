use std::collections::HashMap;
use distill_asset::{asset, AssetType};

#[asset(uuid = "f99dd1b7-d22f-4942-80c1-3d679e074962")]
struct Bad {
    map: HashMap<u32, u32>,
}

fn main() {
    let _ = Bad::descriptor();
}

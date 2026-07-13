use distill_asset::{asset, AssetType};

#[repr(u16)]
#[asset(uuid = "ca0bc0a1-9d68-4374-b88a-86c554b0454f", rev = 2)]
enum Good {
    Unit = 7,
    Pair(u16, u32) = 11,
}

fn main() {
    let _ = Good::descriptor();
}

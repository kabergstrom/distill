use distill_asset::{asset, AssetRef, AssetType, Blob, WeakAssetRef};

#[asset(uuid = "21fd2f0b-0986-424b-b21d-b5a150085675")]
struct Target {
    value: u32,
}

#[asset(uuid = "0da4a6a9-ae08-494f-a28f-cc26da4574de", build_only)]
struct Good {
    strong: AssetRef<Target>,
    weak: WeakAssetRef<Target>,
    #[asset(blob)]
    bytes: Blob,
    #[asset(tag)]
    category: String,
}

fn main() {
    let _ = Good::descriptor();
}

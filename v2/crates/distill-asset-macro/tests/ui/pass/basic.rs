use distill_asset::{asset, AssetHashMap, AssetType};

#[derive(Default)]
#[asset(uuid = "2d42e273-9b4f-4a10-b06c-f8f3a8229a55")]
struct Good {
    count: u32,
    values: Vec<String>,
    map: AssetHashMap<u32, u64>,
    #[asset(skip)]
    runtime: Option<String>,
}

fn main() {
    let _ = Good::descriptor();
}

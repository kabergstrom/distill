use distill_asset::{asset, AssetType};

struct NoDefault;

#[asset(uuid = "10e6115e-c203-48b5-8524-f6761362cba2")]
struct Bad {
    #[asset(skip)]
    field: NoDefault,
}

fn main() {
    let _ = Bad::descriptor();
}

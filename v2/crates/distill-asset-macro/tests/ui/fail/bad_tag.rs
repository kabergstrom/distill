use distill_asset::asset;

#[asset(uuid = "4f4cb8e4-05c1-47a5-8794-8f4edebc7c26")]
struct Bad {
    #[asset(tag)]
    number: u32,
}

fn main() {}

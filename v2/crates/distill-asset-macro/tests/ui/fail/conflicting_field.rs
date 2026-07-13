use distill_asset::asset;

#[asset(uuid = "cce875ef-7f5c-4a6a-86aa-959f96a3e3b9")]
struct Bad {
    #[asset(skip, rev = 0)]
    value: u32,
}

fn main() {}

use distill_asset::asset;

#[repr(C, packed)]
#[asset(uuid = "93821917-1b6b-4a39-a4e4-aa8fe98848b4")]
struct Bad {
    value: u32,
}

fn main() {}

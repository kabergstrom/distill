use distill_asset::asset;

#[asset(uuid = "547a955b-9af2-4fe1-b9d2-72eca95af610")]
enum Bad {
    A,
    B(u32),
}

fn main() {}

use distill_asset::{asset, Blob};

#[asset(uuid = "8a13f750-54d0-44b2-a01c-a92e851b8d07")]
struct MissingMarker {
    bytes: Blob,
}

#[asset(uuid = "3086d10b-765c-42fb-906a-462478cebcf3")]
struct WrongType {
    #[asset(blob)]
    bytes: Vec<u8>,
}

fn main() {}

//! §5: XXH3-128 is the one hash, behind `distill_core::hash`. Only
//! distill-core names `xxhash-rust`; blake3 and crc32c are gone from the
//! resolved workspace graph (every target platform, every dependency kind).

use std::process::Command;

#[test]
fn only_distill_core_depends_on_xxhash_and_no_crate_on_blake3_or_crc32c() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--offline", "--manifest-path"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml"))
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    let name_of = |id: &str| -> String {
        metadata["packages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|package| package["id"] == id)
            .map(|package| package["name"].as_str().unwrap().to_owned())
            .unwrap()
    };
    for package in metadata["packages"].as_array().unwrap() {
        let name = package["name"].as_str().unwrap();
        assert!(
            name != "blake3" && name != "crc32c",
            "{name} is in the dependency graph"
        );
    }
    let mut xxhash_users = Vec::new();
    for node in metadata["resolve"]["nodes"].as_array().unwrap() {
        for dep in node["deps"].as_array().unwrap() {
            if name_of(dep["pkg"].as_str().unwrap()) == "xxhash-rust" {
                xxhash_users.push(name_of(node["id"].as_str().unwrap()));
            }
        }
    }
    assert_eq!(xxhash_users, ["distill-core"]);
}

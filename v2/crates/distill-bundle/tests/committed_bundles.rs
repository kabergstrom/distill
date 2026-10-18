//! Every committed bundle of the engine's example (and the directories in
//! `NGP_SCAN_DIRS`, `:`-separated, e.g. the game repositories) parses,
//! writes back byte for byte, and survives the value encoding (GAP 23
//! §17.10 differential): text → `Value` → bytes → `Value` gives the same
//! value, maps and sets already in encoding order.
use std::path::{Path, PathBuf};

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "target" && !name.starts_with('.') {
                walk(&path, out);
            }
        } else if name.ends_with(".bundle") {
            out.push(path);
        }
    }
}

#[test]
fn committed_bundles_round_trip_through_the_writer_and_the_value_encoding() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../newgameplus");
    let mut files = Vec::new();
    walk(&root.join("example-module/assets"), &mut files);
    assert!(!files.is_empty(), "no bundles under {}", root.display());
    for dir in std::env::var("NGP_SCAN_DIRS").unwrap_or_default().split(':').filter(|d| !d.is_empty()) {
        walk(Path::new(dir), &mut files);
    }
    let mut problems = Vec::new();
    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        let bundle = match distill_bundle::parse_bundle(&bytes) {
            Ok(bundle) => bundle,
            Err(e) => {
                problems.push(format!("{}: parse: {e}", file.display()));
                continue;
            }
        };
        match distill_bundle::write_bundle(&bundle) {
            Ok(written) if written == bytes => {}
            Ok(_) => problems.push(format!("{}: writes back different bytes", file.display())),
            Err(e) => problems.push(format!("{}: write: {e}", file.display())),
        }
        let types = bundle.type_table().unwrap();
        for (id, entry) in &bundle.assets {
            let cx = ngp_value::Cx::new(&bundle.schemas[&entry.schema_hash], &types);
            let back = ngp_value::encode_value(cx, &entry.data)
                .map_err(|e| e.to_string())
                .and_then(|bytes| ngp_value::decode_value(cx, &bytes).map_err(|e| e.to_string()));
            match back {
                Ok(value) if value == entry.data => {}
                Ok(_) => problems.push(format!("{}#{id}: decodes to another value", file.display())),
                Err(e) => problems.push(format!("{}#{id}: {e}", file.display())),
            }
        }
    }
    println!("{} bundles", files.len());
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

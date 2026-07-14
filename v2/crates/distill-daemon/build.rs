use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let v2 = manifest_dir.parent().unwrap().parent().unwrap();
    let projects = v2.parent().unwrap().parent().unwrap();
    let newgameplus = projects.join("newgameplus");

    let mut files = Vec::new();
    collect_file(&v2.join("Cargo.toml"), "v2/Cargo.toml", &mut files);
    collect_file(&v2.join("Cargo.lock"), "v2/Cargo.lock", &mut files);
    for crate_name in [
        "distill-asset",
        "distill-bundle",
        "distill-build",
        "distill-core",
        "distill-daemon",
        "distill-json",
        "distill-migrate",
        "distill-schema",
        "distill-store",
        "distill-wire",
    ] {
        let root = v2.join("crates").join(crate_name);
        collect_file(
            &root.join("Cargo.toml"),
            &format!("{crate_name}/Cargo.toml"),
            &mut files,
        );
        collect_file(
            &root.join("build.rs"),
            &format!("{crate_name}/build.rs"),
            &mut files,
        );
        collect_tree(&v2.join("crates"), &root.join("src"), &mut files).unwrap();
    }
    for crate_name in ["ngp-schema", "ngp-module-host", "source-walk"] {
        let root = newgameplus.join(crate_name);
        collect_file(
            &root.join("Cargo.toml"),
            &format!("{crate_name}/Cargo.toml"),
            &mut files,
        );
        collect_tree(&newgameplus, &root.join("src"), &mut files).unwrap();
    }
    files.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    files.dedup_by(|left, right| left.0 == right.0);

    let mut configuration = env::vars()
        .filter(|(key, _)| {
            key.starts_with("CARGO_CFG_")
                || key.starts_with("CARGO_FEATURE_")
                || matches!(key.as_str(), "CARGO_PKG_VERSION" | "TARGET")
        })
        .collect::<Vec<_>>();
    configuration.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

    let mut generated = String::from("pub static HOST_INTERFACE_CLOSURE: &[(&str, &[u8])] = &[\n");
    for (label, path) in &files {
        generated.push_str(&format!(
            "    ({label:?}, include_bytes!({path:?})),\n",
            path = path.to_string_lossy()
        ));
    }
    generated.push_str("];\npub static HOST_BUILD_CONFIGURATION: &[(&str, &str)] = &[\n");
    for (key, value) in configuration {
        generated.push_str(&format!("    ({key:?}, {value:?}),\n"));
        println!("cargo:rerun-if-env-changed={key}");
    }
    generated.push_str("];\n");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("host_interface_closure.rs"),
        generated,
    )
    .unwrap();
}

fn collect_tree(
    label_root: &Path,
    directory: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_tree(label_root, &path, files)?;
        } else if metadata.is_file()
            && (matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("Cargo.toml" | "build.rs")
            ) || path.extension().and_then(|extension| extension.to_str()) == Some("rs"))
        {
            let label = path
                .strip_prefix(label_root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let label = label.strip_prefix("crates/").unwrap_or(&label).to_owned();
            collect_file(&path, &label, files);
        }
    }
    Ok(())
}

fn collect_file(path: &Path, label: &str, files: &mut Vec<(String, PathBuf)>) {
    if path.is_file() {
        println!("cargo:rerun-if-changed={}", path.display());
        files.push((label.to_owned(), path.to_path_buf()));
    }
}

//! Same-directory temp files for publications into asset roots and codegen
//! output.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn plan_same_dir_temp(target: &Path) -> Result<PathBuf, String> {
    let parent = target
        .parent()
        .ok_or_else(|| "publication target has no parent directory".to_owned())?;
    let stem = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bundle");
    for _ in 0..64 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = parent.join(format!(
            ".{stem}.distill-{}-{sequence}.tmp",
            std::process::id()
        ));
        match fs::symlink_metadata(&temp) {
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(temp),
            Err(error) => return Err(format!("inspect proposed temp path: {error}")),
        }
    }
    Err("could not allocate a unique same-directory proposal temp".into())
}

pub(crate) fn write_planned_temp(temp: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = temp
        .parent()
        .ok_or_else(|| "publication temp has no parent directory".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| format!("create target directory: {error}"))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp)
        .map_err(|error| format!("create proposed temp: {error}"))?;
    let result = file
        .write_all(bytes)
        .map_err(|error| format!("write proposed temp: {error}"))
        .and_then(|()| {
            file.sync_all()
                .map_err(|error| format!("sync proposed temp: {error}"))
        })
        .and_then(|()| FileDirSync::sync(parent));
    if let Err(error) = result {
        drop(file);
        let _ = fs::remove_file(temp);
        let _ = FileDirSync::sync(parent);
        return Err(error);
    }
    Ok(())
}

struct FileDirSync;

impl FileDirSync {
    fn sync(path: &Path) -> Result<(), String> {
        let directory = fs::File::open(path)
            .map_err(|error| format!("open proposal directory for sync: {error}"))?;
        directory
            .sync_all()
            .map_err(|error| format!("sync proposal directory: {error}"))
    }
}

pub(crate) fn unique_sibling(target: &Path, role: &str) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bundle");
    target.with_file_name(format!(
        ".{name}.distill-{role}-{}-{sequence}",
        std::process::id()
    ))
}

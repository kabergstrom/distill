//! The generation manifest: a `CURRENT` file naming the active segment
//! set, atomically replaced (§13). **`CURRENT` is the single authority**:
//! SQLite records the generation it indexed, and a mismatch at startup
//! discards the SQLite artifact index and rebuilds it from the `CURRENT`
//! generation's segments before any read — the two stores are never
//! trusted to agree on their own.
//!
//! Pinned format (text, LF-terminated lines):
//!
//! ```text
//! generation <u64>
//! regular <segment file name>
//! oversize <segment file name>
//! ...
//! ```

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::StoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationManifest {
    pub generation: u64,
    /// Active segments, in creation/log order. Oversize entries contain
    /// exactly one record and are explicitly typed in this authority.
    pub segments: Vec<ManifestSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Regular = 0,
    Oversize = 1,
}

impl SegmentKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Oversize => "oversize",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSegment {
    pub kind: SegmentKind,
    pub name: String,
}

pub fn current_path(cas_dir: &Path) -> PathBuf {
    cas_dir.join("CURRENT")
}

/// Read and validate `CURRENT`.
pub fn read_current(cas_dir: &Path) -> Result<GenerationManifest, StoreError> {
    let path = current_path(cas_dir);
    let bad = |detail: &str| StoreError::BadGenerationManifest {
        path: path.clone(),
        detail: detail.to_owned(),
    };
    let text = std::fs::read_to_string(&path).map_err(|source| StoreError::Io {
        path: path.clone(),
        source,
    })?;
    let mut lines = text.lines();
    let head = lines.next().ok_or_else(|| bad("empty manifest"))?;
    let generation = head
        .strip_prefix("generation ")
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or_else(|| bad("first line must be `generation <u64>`"))?;
    let mut segments = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (kind, name) = line
            .split_once(' ')
            .ok_or_else(|| bad("segment line must be `<regular|oversize> <file>`"))?;
        let kind = match kind {
            "regular" => SegmentKind::Regular,
            "oversize" => SegmentKind::Oversize,
            _ => return Err(bad("unknown segment kind")),
        };
        if name.contains('/') || name.contains('\\') || name.is_empty() {
            return Err(bad("segment names must be bare file names"));
        }
        segments.push(ManifestSegment {
            kind,
            name: name.to_owned(),
        });
    }
    Ok(GenerationManifest {
        generation,
        segments,
    })
}

/// Atomically replace `CURRENT`: write a temp file, fsync it, rename it
/// into place, fsync the directory (§13's write-order rule applied to
/// manifest replacement).
pub fn write_current(cas_dir: &Path, m: &GenerationManifest) -> Result<(), StoreError> {
    let io = |path: &Path| {
        let path = path.to_path_buf();
        move |source: std::io::Error| StoreError::Io { path, source }
    };
    let tmp = cas_dir.join("CURRENT.tmp");
    {
        let mut f = std::fs::File::create(&tmp).map_err(io(&tmp))?;
        let mut text = format!("generation {}\n", m.generation);
        for seg in &m.segments {
            text.push_str(seg.kind.label());
            text.push(' ');
            text.push_str(&seg.name);
            text.push('\n');
        }
        f.write_all(text.as_bytes()).map_err(io(&tmp))?;
        f.sync_all().map_err(io(&tmp))?;
    }
    let dst = current_path(cas_dir);
    std::fs::rename(&tmp, &dst).map_err(io(&dst))?;
    fsync_dir(cas_dir)?;
    Ok(())
}

/// fsync a directory — segment creation and deletion also fsync the
/// directory (§13).
pub fn fsync_dir(dir: &Path) -> Result<(), StoreError> {
    let f = std::fs::File::open(dir).map_err(|source| StoreError::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    f.sync_all().map_err(|source| StoreError::Io {
        path: dir.to_path_buf(),
        source,
    })
}

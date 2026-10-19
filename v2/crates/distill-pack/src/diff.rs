//! What changed between two pack manifests, for a game's dist build to
//! report against the one it shipped last (newgameplus GAP 32 §12): assets
//! removed, assets whose content changed, assets whose path changed. Saves
//! hold asset UUIDs; a removed one restores as a dead handle.

use std::collections::BTreeMap;

use distill_core::id::AssetUuid;

use crate::manifest::PackManifest;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestDiff {
    /// In the old manifest only, with its last path.
    pub removed: Vec<(AssetUuid, Option<String>)>,
    /// In both, another content hash; with its new path.
    pub changed: Vec<(AssetUuid, Option<String>)>,
    /// In both, under another path: old, new.
    pub moved: Vec<(AssetUuid, Option<String>, Option<String>)>,
}

/// Each asset's path as `path` or `path [name]` (a local id's asset); the
/// first row of an asset names it.
fn paths(m: &PackManifest) -> BTreeMap<AssetUuid, String> {
    let mut out = BTreeMap::new();
    for row in m.paths.iter().flatten() {
        let shown = match &row.name {
            None => row.path.clone(),
            Some(name) => format!("{} [{name}]", row.path),
        };
        out.entry(row.asset_uuid).or_insert(shown);
    }
    out
}

/// `old` against `new`, each list sorted by UUID.
pub fn diff_manifests(old: &PackManifest, new: &PackManifest) -> ManifestDiff {
    let (old_paths, new_paths) = (paths(old), paths(new));
    let new_assets: BTreeMap<AssetUuid, _> = new.assets.iter().map(|a| (a.asset_uuid, a.content_hash)).collect();
    let mut diff = ManifestDiff::default();
    for a in &old.assets {
        let (was, is) = (old_paths.get(&a.asset_uuid).cloned(), new_paths.get(&a.asset_uuid).cloned());
        match new_assets.get(&a.asset_uuid) {
            None => diff.removed.push((a.asset_uuid, was)),
            Some(hash) => {
                if *hash != a.content_hash {
                    diff.changed.push((a.asset_uuid, is.clone()));
                }
                if was != is {
                    diff.moved.push((a.asset_uuid, was, is));
                }
            }
        }
    }
    diff.removed.sort();
    diff.changed.sort();
    diff.moved.sort();
    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ManifestAssetRow, PackTarget, PathRow};
    use distill_core::id::ContentHash;

    fn manifest(assets: &[(u8, u8, &str)]) -> PackManifest {
        PackManifest {
            target: PackTarget { name: "dev".into() },
            target_def_hash: [0; 16],
            archives: Vec::new(),
            assets: assets
                .iter()
                .map(|&(id, content, _)| ManifestAssetRow { asset_uuid: AssetUuid([id; 16]), content_hash: ContentHash([content; 16]), load_deps: Vec::new() })
                .collect(),
            encodings: Vec::new(),
            index: Vec::new(),
            wire_trees: Vec::new(),
            paths: Some(assets.iter().map(|&(id, _, path)| PathRow { path: path.into(), name: None, asset_uuid: AssetUuid([id; 16]) }).collect()),
        }
    }

    #[test]
    fn removed_changed_and_moved_assets_are_listed() {
        let old = manifest(&[(1, 1, "a.prefab"), (2, 1, "b.prefab"), (3, 1, "c.png"), (4, 1, "d.cue")]);
        let new = manifest(&[(2, 2, "b.prefab"), (3, 1, "art/c.png"), (4, 1, "d.cue"), (5, 1, "e.cue")]);
        let diff = diff_manifests(&old, &new);
        assert_eq!(diff.removed, [(AssetUuid([1; 16]), Some("a.prefab".into()))]);
        assert_eq!(diff.changed, [(AssetUuid([2; 16]), Some("b.prefab".into()))]);
        assert_eq!(diff.moved, [(AssetUuid([3; 16]), Some("c.png".into()), Some("art/c.png".into()))]);
        assert_eq!(diff_manifests(&new, &new), ManifestDiff::default());
    }
}

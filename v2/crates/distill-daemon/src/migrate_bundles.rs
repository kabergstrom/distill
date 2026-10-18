//! One-time migration of committed bundles to the XXH3-128 format
//! (newgameplus GAP 23 §17.4). Reads the old bundle format and writes each
//! bundle through the current writer:
//!
//! - old container framing (8-byte magic `\x89DSB\r\n\x1a\n`, version 1,
//!   CRC-32C of json and blob chunk) or plain JSON;
//! - schema keys and every reference to them (`schema_hash`, `types`)
//!   recomputed from the schema snapshot the bundle stores;
//! - bootstrap control entries (`$record`, rules, pack definitions) take
//!   the current bootstrap schema; the 32-byte hashes their data records
//!   (import read sets, failure details) become 16 zero bytes, which no
//!   observation matches, so every imported bundle re-imports on the next
//!   daemon start;
//! - every bundle and asset UUID is kept (checked before writing).
//!
//! Old schema keys are not verified against their snapshots: that needs
//! the old hash, which the workspace no longer has. The migration runs
//! once per asset repository and is then deleted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use distill_core::bootstrap::BootstrapControlSpecV1;
use distill_core::id::TypeUuid;
use distill_schema::ngp_schema::{node_from_bytes, node_hash, snapshot_from_json, snapshot_to_json};
use serde_json::Value;

use crate::config::DaemonConfig;

const OLD_CONTAINER_MAGIC: [u8; 8] = [0x89, 0x44, 0x53, 0x42, 0x0D, 0x0A, 0x1A, 0x0A];
const OLD_HEADER_LEN: usize = 36;
const NEW_HEADER_LEN: usize = distill_core::header::HEADER_LEN + 16;

#[derive(Debug, Default)]
pub struct MigrationReport {
    pub migrated: Vec<PathBuf>,
    pub current: Vec<PathBuf>,
}

/// Migrate every `*.bundle` under `paths`: a daemon config (`*.toml`: all
/// its asset roots), a directory, or a bundle file.
pub fn migrate_paths(paths: &[PathBuf]) -> Result<MigrationReport, String> {
    let mut files = Vec::new();
    for path in paths {
        if path.extension().is_some_and(|ext| ext == "toml") {
            let config = DaemonConfig::load(path).map_err(|e| format!("{}: {e}", path.display()))?;
            for root in config.assets.roots.values() {
                collect(root, &mut files)?;
            }
        } else if path.is_dir() {
            collect(path, &mut files)?;
        } else {
            files.push(path.clone());
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut report = MigrationReport::default();
    for file in files {
        let canonical = file.canonicalize().map_err(|e| format!("{}: {e}", file.display()))?;
        if !seen.insert(canonical.clone()) {
            continue;
        }
        let bytes = std::fs::read(&canonical).map_err(|e| format!("{}: {e}", file.display()))?;
        match migrate_bytes(&bytes).map_err(|e| format!("{}: {e}", file.display()))? {
            Some(out) => {
                let temp = canonical.with_extension("bundle.migrating");
                std::fs::write(&temp, &out).map_err(|e| format!("{}: {e}", temp.display()))?;
                std::fs::rename(&temp, &canonical)
                    .map_err(|e| format!("{}: {e}", canonical.display()))?;
                report.migrated.push(file);
            }
            None => report.current.push(file),
        }
    }
    Ok(report)
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut entries: Vec<_> = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    entries.sort();
    for path in entries {
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
        if name.starts_with('.') || name == "target" {
            continue;
        }
        if path.is_dir() {
            collect(&path, out)?;
        } else if name.ends_with(".bundle") {
            out.push(path);
        }
    }
    Ok(())
}

/// The bundle's bytes in the current format, or `None` when they already
/// are.
pub fn migrate_bytes(bytes: &[u8]) -> Result<Option<Vec<u8>>, String> {
    if distill_bundle::parse_bundle(bytes).is_ok() {
        return Ok(None);
    }
    let (json, chunk) = if bytes.starts_with(&OLD_CONTAINER_MAGIC) {
        let (json, chunk) = read_old_container(bytes)?;
        (json, Some(chunk))
    } else if bytes.first() == Some(&b'{') {
        (std::str::from_utf8(bytes).map_err(|e| e.to_string())?, None)
    } else {
        return Err(format!(
            "neither the current format ({:?}) nor the old one",
            distill_bundle::parse_bundle(bytes).unwrap_err()
        ));
    };
    let mut envelope: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let identities = uuids(&envelope)?;
    rewrite(&mut envelope)?;
    let text = format!("{}\n", serde_json::to_string(&envelope).map_err(|e| e.to_string())?);
    let staged = match chunk {
        None => text.into_bytes(),
        Some(chunk) => new_container(text.as_bytes(), chunk),
    };
    let bundle = distill_bundle::parse_bundle(&staged)
        .map_err(|e| format!("the migrated bundle does not parse: {e:?}"))?;
    let out = distill_bundle::write_bundle(&bundle).map_err(|e| format!("{e:?}"))?;
    let reparsed = distill_bundle::parse_bundle(&out).map_err(|e| format!("{e:?}"))?;
    let mut kept = BTreeMap::new();
    for (local_id, entry) in &reparsed.assets {
        kept.insert(local_id.clone(), entry.uuid.to_string());
    }
    if identities.0 != reparsed.uuid.to_string() || identities.1 != kept {
        return Err(format!(
            "UUIDs changed: before {identities:?}, after {} {kept:?}",
            reparsed.uuid
        ));
    }
    Ok(Some(out))
}

type Identities = (String, BTreeMap<String, String>);

fn uuids(envelope: &Value) -> Result<Identities, String> {
    let uuid = envelope["uuid"].as_str().ok_or("no bundle uuid")?.to_owned();
    let mut assets = BTreeMap::new();
    for (local_id, entry) in envelope["assets"].as_object().ok_or("no assets")? {
        let uuid = entry["uuid"].as_str().ok_or("asset without uuid")?;
        assets.insert(local_id.clone(), uuid.to_owned());
    }
    Ok((uuid, assets))
}

fn read_old_container(bytes: &[u8]) -> Result<(&str, &[u8]), String> {
    if bytes.len() < OLD_HEADER_LEN {
        return Err("old container shorter than its header".into());
    }
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    if u32_at(8) != 1 {
        return Err(format!("old container version {}", u32_at(8)));
    }
    let json_len = usize::try_from(u64_at(12)).map_err(|e| e.to_string())?;
    let blob_len = usize::try_from(u64_at(20)).map_err(|e| e.to_string())?;
    let json_end = OLD_HEADER_LEN.checked_add(json_len).ok_or("length overflow")?;
    let chunk_start = json_end.checked_add(15).ok_or("length overflow")? & !15;
    if chunk_start.checked_add(blob_len) != Some(bytes.len()) {
        return Err("old container lengths disagree with the file size".into());
    }
    let json = &bytes[OLD_HEADER_LEN..json_end];
    let chunk = &bytes[chunk_start..];
    if crc32c(json) != u32_at(28) || crc32c(chunk) != u32_at(32) {
        return Err("old container checksum mismatch".into());
    }
    Ok((std::str::from_utf8(json).map_err(|e| e.to_string())?, chunk))
}

fn new_container(json: &[u8], chunk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(NEW_HEADER_LEN + json.len() + 15 + chunk.len());
    distill_core::header::write_header(
        &mut out,
        distill_core::hash::Domain::Bundle,
        distill_bundle::CONTAINER_VERSION,
    );
    out.extend_from_slice(&(json.len() as u64).to_le_bytes());
    out.extend_from_slice(&(chunk.len() as u64).to_le_bytes());
    out.extend_from_slice(json);
    out.resize((out.len() + 15) & !15, 0);
    out.extend_from_slice(chunk);
    out
}

fn rewrite(envelope: &mut Value) -> Result<(), String> {
    let spec = BootstrapControlSpecV1::embedded().map_err(|e| format!("{e:?}"))?;
    let mut control_schemas = BTreeMap::new();
    for row in &spec.0 {
        let schema = node_from_bytes(&row.logical_schema).map_err(|e| format!("{e:?}"))?;
        let text = snapshot_to_json(&schema).map_err(|e| format!("{e:?}"))?;
        let value: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        control_schemas.insert(row.type_uuid, value);
    }

    // Control entries: the current bootstrap schema; recorded hashes zeroed.
    let mut replaced = BTreeMap::new();
    let assets = envelope["assets"].as_object_mut().ok_or("no assets")?;
    for (local_id, entry) in assets.iter_mut() {
        let type_uuid: TypeUuid = entry["type_uuid"]
            .as_str()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| format!("{local_id}: bad type_uuid"))?;
        if let Some(schema) = control_schemas.get(&type_uuid) {
            let old = entry["schema_hash"]
                .as_str()
                .ok_or_else(|| format!("{local_id}: no schema_hash"))?
                .to_owned();
            replaced.insert(old, schema.clone());
            zero_hashes(&mut entry["data"]);
        }
    }
    let schemas = envelope["schemas"].as_object_mut().ok_or("no schemas")?;
    for (old, schema) in replaced {
        if !schemas.contains_key(&old) {
            return Err(format!("control schema {old} not in schemas"));
        }
        schemas.insert(old, schema);
    }

    // Every schema key from its stored snapshot.
    let mut renamed = BTreeMap::new();
    let old_schemas = std::mem::take(schemas);
    for (old, snapshot) in old_schemas {
        let text = serde_json::to_string(&snapshot).map_err(|e| e.to_string())?;
        let schema = snapshot_from_json(&text).map_err(|e| format!("schema {old}: {e:?}"))?;
        let new = node_hash(&schema.root).map_err(|e| format!("schema {old}: {e:?}"))?.to_string();
        schemas.insert(new.clone(), snapshot);
        renamed.insert(old, new);
    }
    rename_strings(envelope, &renamed);
    if let Some(stray) = find_old_hash(envelope) {
        return Err(format!("unmapped old hash {stray}"));
    }
    Ok(())
}

/// Recorded 32-byte hashes (arrays of 32 bytes) become 16 zero bytes.
fn zero_hashes(value: &mut Value) {
    match value {
        Value::Array(items) => {
            if items.len() == 32 && items.iter().all(|item| item.as_u64().is_some_and(|n| n < 256))
            {
                *items = vec![Value::from(0u8); 16];
            } else {
                items.iter_mut().for_each(zero_hashes);
            }
        }
        Value::Object(map) => map.values_mut().for_each(zero_hashes),
        _ => {}
    }
}

fn rename_strings(value: &mut Value, renamed: &BTreeMap<String, String>) {
    match value {
        Value::String(text) => {
            if let Some(new) = renamed.get(text.as_str()) {
                *text = new.clone();
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|item| rename_strings(item, renamed)),
        Value::Object(map) => map.values_mut().for_each(|item| rename_strings(item, renamed)),
        _ => {}
    }
}

fn is_old_hash(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn find_old_hash(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if is_old_hash(text) => Some(text.clone()),
        Value::Array(items) => items.iter().find_map(find_old_hash),
        Value::Object(map) => map
            .iter()
            .find_map(|(key, item)| {
                is_old_hash(key).then(|| key.clone()).or_else(|| find_old_hash(item))
            }),
        _ => None,
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0x82F6_3B78 } else { crc >> 1 };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURES: [(&str, &[u8]); 3] = [
        ("prefab", include_bytes!("../tests/fixtures/old-bundles/marker_red.prefab.bundle")),
        ("import", include_bytes!("../tests/fixtures/old-bundles/winch_clutch_in.wav.bundle")),
        ("rules", include_bytes!("../tests/fixtures/old-bundles/models.rules.bundle")),
    ];

    #[test]
    fn old_bundles_migrate_keeping_every_uuid_and_dropping_recorded_hashes() {
        for (name, old) in FIXTURES {
            let text = String::from_utf8_lossy(old);
            let before: Vec<&str> = text.matches("\"uuid\":\"").collect();
            let new = migrate_bytes(old).unwrap().unwrap_or_else(|| panic!("{name} was current"));
            let bundle = distill_bundle::parse_bundle(&new).unwrap();
            let json: Value = serde_json::from_str(
                std::str::from_utf8(old)
                    .map(|text| text.to_owned())
                    .unwrap_or_else(|_| read_old_container(old).unwrap().0.to_owned())
                    .as_str(),
            )
            .unwrap();
            let (uuid, assets) = uuids(&json).unwrap();
            assert_eq!(bundle.uuid.to_string(), uuid, "{name}");
            let after: BTreeMap<String, String> = bundle
                .assets
                .iter()
                .map(|(local_id, entry)| (local_id.clone(), entry.uuid.to_string()))
                .collect();
            assert_eq!(after, assets, "{name}");
            assert!(!before.is_empty());
            // Written once, current from then on.
            assert_eq!(migrate_bytes(&new).unwrap(), None, "{name}");
            if name == "import" {
                assert!(new.starts_with(&distill_core::header::MAGIC));
                let record = &bundle.assets["$record"];
                let text = format!("{:?}", record.data);
                assert!(text.contains("U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0), U8(0)"), "{text}");
            }
        }
    }

    #[test]
    fn a_damaged_old_container_is_refused() {
        let mut old = FIXTURES[1].1.to_vec();
        let last = old.len() - 1;
        old[last] ^= 1;
        assert!(migrate_bytes(&old).unwrap_err().contains("checksum"));
    }
}

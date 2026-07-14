//! Append/commit/read machinery for the log-structured CAS (§13).
//!
//! Write order (pinned): append the build's payload records first and
//! one result record last, fsync the segment, then insert the index
//! rows in one memo transaction. **The result record is the commit
//! marker** — a crash after output 2 of 3 publishes nothing. Segment
//! creation and deletion also fsync the directory.
//!
//! Ordinary commit groups roll as a unit. A record larger than the
//! regular segment cap is instead written alone in a manifest-typed
//! oversize segment; recovery resolves coverage across manifest order,
//! so a group may cross segment boundaries without weakening the result
//! marker rule.

use std::io::Write;
use std::path::PathBuf;

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, TypeUuid};

use crate::cas::manifest::{self, GenerationManifest, ManifestSegment, SegmentKind};
use crate::cas::record::{
    decode_record, encode_record, AuxRow, DecodedRecord, FailureCause, KeyKind, OutputRow, Record,
    RecordKind, ResultOutcome, ResultPayload, RECORD_HEADER_LEN,
};
use crate::db::Store;
use crate::error::StoreError;
use crate::state::MemoSeq;

/// The payload-record kind a successful build's outputs carry (§13's
/// payload kinds, minus the store-managed debug and wire-tree kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    ImportEncoding,
    ProcessorOutput,
}

impl PayloadKind {
    fn record_kind(self) -> RecordKind {
        match self {
            PayloadKind::ImportEncoding => RecordKind::ImportEncoding,
            PayloadKind::ProcessorOutput => RecordKind::ProcessorOutput,
        }
    }
}

/// One output to commit: key, declared type uuids, artifact bytes.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    /// Empty for the primary output (§13).
    pub output_key: String,
    pub type_uuids: Vec<TypeUuid>,
    pub bytes: Vec<u8>,
}

/// One auxiliary debug payload to commit (§13: pins and evicts with the
/// result, never appears in derived-output rows, manifests, or load-dep
/// closures).
#[derive(Debug, Clone)]
pub struct AuxSpec {
    pub debug_key: String,
    pub bytes: Vec<u8>,
}

/// A build's outcome at commit time. Transient infrastructure errors are
/// deliberately unrepresentable: they are typed apart and never memoized
/// (§13) — there is no API to write one.
#[derive(Debug, Clone)]
pub enum CommitOutcome {
    Success {
        payload_kind: PayloadKind,
        outputs: Vec<OutputSpec>,
        aux: Vec<AuxSpec>,
    },
    Failure {
        /// The terminal cause (§9, §13): `Op` when the trace's own
        /// terminal entry is the failing operation, `Local` for a
        /// deterministic local failure no operation produced.
        cause: FailureCause,
    },
}

/// One build result to commit (§13): payload records first, one result
/// record last, one memo transaction for the index rows.
#[derive(Debug, Clone)]
pub struct BuildCommit {
    pub key_kind: KeyKind,
    /// The `"DSSI"` StaticInputs digest or `"DSBI"` pre-key digest.
    pub static_input_key: [u8; 32],
    /// The parent asset (processor results) or entry (build imports).
    pub asset_uuid: AssetUuid,
    /// The full canonical `StaticInputs` encoding for index rebuild
    /// (DSSI); empty for DSBI.
    pub static_inputs_canonical: Vec<u8>,
    /// Canonical trace-op bytes, §9's encoding (up to and including the
    /// failing op for failures).
    pub trace: Vec<u8>,
    pub outcome: CommitOutcome,
}

/// What a commit returns.
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    pub memo_seq: MemoSeq,
    pub trace_digest: [u8; 32],
    /// `output_key → ContentHash` for every committed output.
    pub outputs: Vec<(String, ContentHash)>,
    /// `debug_key → ContentHash` for every committed aux payload.
    pub aux: Vec<(String, ContentHash)>,
    /// Derived-output assertions that did not verify against the
    /// input-versioned namespace index (§9) — reported, never silently
    /// recorded.
    pub unverified_assertions: Vec<(AssetUuid, String)>,
}

/// One bucket candidate, most-recently-committed-first (§13).
#[derive(Debug, Clone)]
pub struct Candidate {
    pub trace_digest: [u8; 32],
    pub memo_seq: MemoSeq,
    pub asset_uuid: AssetUuid,
    pub payload: ResultPayload,
    pub segment: u64,
    pub offset: u64,
}

/// In-memory CAS cursor state (rebuilt at open from `CURRENT` + the
/// segment files; ephemeral by §13's classification).
#[derive(Debug, Default)]
pub(crate) struct CasInner {
    pub(crate) dir: PathBuf,
    pub(crate) generation: u64,
    /// Active segments in id order: (segment id, file name).
    pub(crate) segments: Vec<SegmentInfo>,
    /// Append cursor into the last segment.
    pub(crate) active_len: u64,
    pub(crate) next_segment_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentInfo {
    pub(crate) id: u64,
    pub(crate) name: String,
    pub(crate) kind: SegmentKind,
}

pub(crate) fn segment_file_name(id: u64, kind: SegmentKind) -> String {
    let prefix = match kind {
        SegmentKind::Regular => "seg",
        SegmentKind::Oversize => "oversize",
    };
    format!("{prefix}-{id:016x}.dsr")
}

pub(crate) fn parse_segment_id(name: &str, kind: SegmentKind) -> Option<u64> {
    let prefix = match kind {
        SegmentKind::Regular => "seg-",
        SegmentKind::Oversize => "oversize-",
    };
    let hex = name.strip_prefix(prefix)?.strip_suffix(".dsr")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

impl Store {
    fn io_err(path: &std::path::Path) -> impl Fn(std::io::Error) -> StoreError + '_ {
        move |source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    pub(crate) fn segment_path(&self, segment_id: u64) -> PathBuf {
        let name = self
            .cas
            .segments
            .iter()
            .find(|segment| segment.id == segment_id)
            .map(|segment| segment.name.clone())
            .unwrap_or_else(|| segment_file_name(segment_id, SegmentKind::Regular));
        self.cas.dir.join(name)
    }

    /// Roll to a fresh segment when the incoming group would cross the
    /// size cap (§18's `cas.segment_size`, applied to newly rolled
    /// segments only). Protocol: create the file, fsync it and the
    /// directory, then atomically extend `CURRENT` — only a
    /// CURRENT-listed segment ever receives records, so every fsynced
    /// record lives in the durable set.
    fn ensure_active_segment(&mut self, incoming: u64) -> Result<u64, StoreError> {
        let needs_roll = match self.cas.segments.last() {
            None => true,
            Some(segment) => {
                segment.kind != SegmentKind::Regular
                    || (self.cas.active_len > 0
                        && self.cas.active_len + incoming > self.config.segment_size)
            }
        };
        if !needs_roll {
            return Ok(self.cas.segments.last().expect("active segment").id);
        }
        self.create_segment(SegmentKind::Regular)
    }

    fn create_segment(&mut self, kind: SegmentKind) -> Result<u64, StoreError> {
        let id = self.cas.next_segment_id;
        let name = segment_file_name(id, kind);
        let path = self.cas.dir.join(&name);
        let f = std::fs::File::create(&path).map_err(Self::io_err(&path))?;
        f.sync_all().map_err(Self::io_err(&path))?;
        manifest::fsync_dir(&self.cas.dir)?;
        let mut segments: Vec<ManifestSegment> = self
            .cas
            .segments
            .iter()
            .map(|s| ManifestSegment {
                kind: s.kind,
                name: s.name.clone(),
            })
            .collect();
        segments.push(ManifestSegment {
            kind,
            name: name.clone(),
        });
        manifest::write_current(
            &self.cas.dir,
            &GenerationManifest {
                generation: self.cas.generation,
                segments,
            },
        )?;
        self.conn.execute(
            "INSERT INTO cas_segments(segment_id, file_name, segment_kind, indexed_len)
             VALUES (?1, ?2, ?3, 0)
             ON CONFLICT(segment_id) DO NOTHING",
            rusqlite::params![id as i64, name, kind as i64],
        )?;
        self.cas.segments.push(SegmentInfo { id, name, kind });
        self.cas.next_segment_id = id + 1;
        self.cas.active_len = 0;
        Ok(id)
    }

    /// Append a group of encoded records to the active segment and fsync
    /// once — the group's last record is its commit marker, so one
    /// durable point suffices (§13). Returns (segment id, record start
    /// offsets).
    fn append_records(&mut self, encoded: &[Vec<u8>]) -> Result<Vec<(u64, u64)>, StoreError> {
        let mut locations = Vec::with_capacity(encoded.len());
        for bytes in encoded {
            let len = bytes.len() as u64;
            let oversize = len > self.config.segment_size;
            let segment = if oversize {
                self.create_segment(SegmentKind::Oversize)?
            } else {
                self.ensure_active_segment(len)?
            };
            let path = self.segment_path(segment);
            let offset = if oversize { 0 } else { self.cas.active_len };
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .map_err(Self::io_err(&path))?;
            f.write_all(bytes).map_err(Self::io_err(&path))?;
            f.sync_all().map_err(Self::io_err(&path))?;
            if oversize {
                // Dedicated: this segment is closed after exactly one
                // record. The next regular append rolls a new regular
                // segment, preserving manifest/log order.
                self.cas.active_len = 0;
            } else {
                self.cas.active_len = offset + len;
            }
            locations.push((segment, offset));
        }
        Ok(locations)
    }

    /// Commit a wire tree (§13: first-class CAS records, committed
    /// durably before any result record whose output headers name the
    /// LayoutHash — call this before `commit_build`). Idempotent:
    /// duplicate content is byte-identical by definition.
    pub fn put_wire_tree(&mut self, tree_bytes: &[u8]) -> Result<LayoutHash, StoreError> {
        let hash = *blake3::hash(tree_bytes).as_bytes();
        if self.extent_of(&hash)?.is_some() {
            return Ok(LayoutHash(hash));
        }
        let record = Record {
            kind: RecordKind::WireTree,
            asset_uuid: AssetUuid([0u8; 16]),
            static_input_key: Vec::new(),
            output_key: String::new(),
            payload: tree_bytes.to_vec(),
        };
        let encoded = encode_record(&record);
        let payload_offset_in_record = (RECORD_HEADER_LEN) as u64;
        let locations = self.append_records(std::slice::from_ref(&encoded))?;
        let (segment, offset) = locations[0];
        let payload_offset = offset + payload_offset_in_record;
        let indexed_len = self
            .segment_path(segment)
            .metadata()
            .map_err(Self::io_err(&self.segment_path(segment)))?
            .len();
        let txn = self.conn.transaction()?;
        upsert_extent(
            &txn,
            &hash,
            segment,
            payload_offset,
            tree_bytes.len() as u64,
        )?;
        txn.execute(
            "UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1",
            rusqlite::params![segment as i64, indexed_len as i64],
        )?;
        txn.commit()?;
        Ok(LayoutHash(hash))
    }

    /// Commit one build result (§13): payloads first, the result record
    /// last, one fsync, then one memo transaction inserting the extent
    /// rows, the candidate-bucket row, and the verified derived-output
    /// assertions. Advances only the memo sequence.
    pub fn commit_build(&mut self, commit: BuildCommit) -> Result<CommitReceipt, StoreError> {
        // Shape checks before any byte lands.
        if let CommitOutcome::Success { outputs, .. } = &commit.outcome {
            if commit.key_kind == KeyKind::BuildImport && outputs.len() != 1 {
                return Err(StoreError::BuildImportOutputArity { got: outputs.len() });
            }
        }

        // Build the record group.
        let mut records: Vec<Record> = Vec::new();
        let mut output_rows: Vec<OutputRow> = Vec::new();
        let mut aux_rows: Vec<AuxRow> = Vec::new();
        let outcome = match &commit.outcome {
            CommitOutcome::Success {
                payload_kind,
                outputs,
                aux,
            } => {
                for out in outputs {
                    output_rows.push(OutputRow {
                        output_key: out.output_key.clone(),
                        type_uuids: out.type_uuids.clone(),
                        content_hash: ContentHash(*blake3::hash(&out.bytes).as_bytes()),
                    });
                    records.push(Record {
                        kind: payload_kind.record_kind(),
                        asset_uuid: commit.asset_uuid,
                        static_input_key: Vec::new(),
                        output_key: out.output_key.clone(),
                        payload: out.bytes.clone(),
                    });
                }
                for a in aux {
                    aux_rows.push(AuxRow {
                        debug_key: a.debug_key.clone(),
                        content_hash: ContentHash(*blake3::hash(&a.bytes).as_bytes()),
                    });
                    records.push(Record {
                        kind: RecordKind::Debug,
                        asset_uuid: commit.asset_uuid,
                        static_input_key: Vec::new(),
                        output_key: a.debug_key.clone(),
                        payload: a.bytes.clone(),
                    });
                }
                ResultOutcome::Success {
                    outputs: output_rows.clone(),
                    aux: aux_rows.clone(),
                }
            }
            CommitOutcome::Failure { cause } => ResultOutcome::Failure {
                cause: cause.clone(),
            },
        };
        let result_payload = ResultPayload {
            key_kind: commit.key_kind,
            static_inputs_canonical: commit.static_inputs_canonical.clone(),
            trace: commit.trace.clone(),
            outcome,
        };
        let trace_digest = result_payload.trace_digest();
        records.push(Record {
            kind: RecordKind::Result,
            asset_uuid: commit.asset_uuid,
            static_input_key: commit.static_input_key.to_vec(),
            output_key: String::new(),
            payload: result_payload.encode(),
        });

        let encoded: Vec<Vec<u8>> = records.iter().map(encode_record).collect();
        let locations = self.append_records(&encoded)?;
        let result_index = records.len() - 1;
        let (result_segment, result_offset) = locations[result_index];
        let result_len = encoded[result_index].len() as u64;
        let mut touched = std::collections::BTreeSet::new();
        for (segment, _) in &locations {
            touched.insert(*segment);
        }
        let touched_lengths = touched
            .into_iter()
            .map(|segment| {
                let path = self.segment_path(segment);
                let len = path.metadata().map_err(Self::io_err(&path))?.len();
                Ok((segment, len))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;

        // One memo transaction for every index row (§13).
        let key_kind = commit.key_kind;
        let static_key = commit.static_input_key;
        let asset_uuid = commit.asset_uuid;
        let mut receipt_outputs = Vec::new();
        let mut receipt_aux = Vec::new();
        let mut unverified = Vec::new();
        let ((), memo_seq) = self.memo_transaction(|txn, memo_seq| {
            for (i, rec) in records.iter().enumerate().take(result_index) {
                let (segment, offset) = locations[i];
                let payload_offset = offset
                    + RECORD_HEADER_LEN as u64
                    + rec.static_input_key.len() as u64
                    + rec.output_key.len() as u64;
                let hash = *blake3::hash(&rec.payload).as_bytes();
                upsert_extent(txn, &hash, segment, payload_offset, rec.payload.len() as u64)?;
            }
            for row in &output_rows {
                receipt_outputs.push((row.output_key.clone(), row.content_hash));
            }
            for row in &aux_rows {
                receipt_aux.push((row.debug_key.clone(), row.content_hash));
            }
            upsert_candidate(
                txn,
                key_kind,
                &static_key,
                &trace_digest,
                memo_seq,
                result_segment,
                result_offset,
                result_len,
            )?;
            // Derived-output assertions: memo data verified against the
            // input-versioned namespace index, never a namespace claim
            // of its own (§9).
            for row in &output_rows {
                if row.output_key.is_empty() {
                    continue;
                }
                let child = AssetUuid::v5(asset_uuid, &row.output_key);
                if derived_row_matches(txn, child, asset_uuid, &row.output_key)? {
                    txn.execute(
                        "INSERT INTO derived_assertions(child_uuid, parent_uuid, output_key, memo_seq)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(child_uuid, memo_seq) DO NOTHING",
                        rusqlite::params![
                            child.0.as_slice(),
                            asset_uuid.0.as_slice(),
                            row.output_key,
                            memo_seq.0 as i64,
                        ],
                    )?;
                } else {
                    unverified.push((child, row.output_key.clone()));
                }
            }
            for (segment, len) in &touched_lengths {
                txn.execute(
                    "UPDATE cas_segments SET indexed_len = ?2 WHERE segment_id = ?1",
                    rusqlite::params![*segment as i64, *len as i64],
                )?;
            }
            Ok(())
        })?;

        Ok(CommitReceipt {
            memo_seq,
            trace_digest,
            outputs: receipt_outputs,
            aux: receipt_aux,
            unverified_assertions: unverified,
        })
    }

    pub(crate) fn extent_of(&self, hash: &[u8; 32]) -> Result<Option<(u64, u64, u64)>, StoreError> {
        use rusqlite::OptionalExtension;
        Ok(self
            .conn
            .query_row(
                "SELECT segment, offset, len FROM cas_extents WHERE content_hash = ?1",
                [hash.as_slice()],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)? as u64,
                        r.get::<_, i64>(1)? as u64,
                        r.get::<_, i64>(2)? as u64,
                    ))
                },
            )
            .optional()?)
    }

    /// Read an artifact (or wire tree) by hash: extent lookup, read,
    /// verify against the requested hash — corruption is caught, never
    /// returned. Valid under version and pipeline poison (§13's
    /// pure-metadata classification).
    pub fn cas_read(&self, hash: &[u8; 32]) -> Result<Vec<u8>, StoreError> {
        let (segment, offset, len) = self
            .extent_of(hash)?
            .ok_or(StoreError::NotFound { hash: *hash })?;
        let bytes = self.read_extent(segment, offset, len)?;
        if *blake3::hash(&bytes).as_bytes() != *hash {
            return Err(StoreError::CorruptExtent { segment, offset });
        }
        Ok(bytes)
    }

    /// Full doctor verification of every indexed CAS extent. Each record is
    /// read through the ordinary hash-checking path, so an index/segment drift
    /// cannot be reported healthy merely because its framing still decodes.
    pub fn verify_all_cas_extents(&self) -> Result<usize, StoreError> {
        let hashes = {
            let mut statement = self
                .conn
                .prepare("SELECT content_hash FROM cas_extents ORDER BY content_hash")?;
            let hashes = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .map(|row| {
                    let bytes = row?;
                    bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
                })
                .collect::<Result<Vec<[u8; 32]>, _>>()?;
            hashes
        };
        for hash in &hashes {
            self.cas_read(hash)?;
        }
        Ok(hashes.len())
    }

    pub(crate) fn read_extent(
        &self,
        segment: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StoreError> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.segment_path(segment);
        let mut f = std::fs::File::open(&path).map_err(Self::io_err(&path))?;
        f.seek(SeekFrom::Start(offset))
            .map_err(Self::io_err(&path))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).map_err(Self::io_err(&path))?;
        Ok(buf)
    }

    /// The candidate bucket for a static-input key, most recently
    /// committed first (§13). Touches each candidate's `last_used` for
    /// the LRU policy.
    pub fn lookup_candidates(
        &mut self,
        key_kind: KeyKind,
        static_key: &[u8; 32],
    ) -> Result<Vec<Candidate>, StoreError> {
        let rows: Vec<(Vec<u8>, i64, i64, i64, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT trace_digest, memo_seq, segment, offset, len FROM result_candidates
                 WHERE key_kind = ?1 AND static_key = ?2 ORDER BY memo_seq DESC",
            )?;
            let mapped = stmt.query_map(
                rusqlite::params![key_kind as i64, static_key.as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )?;
            mapped.collect::<Result<_, _>>()?
        };
        let now = now_millis();
        let mut out = Vec::with_capacity(rows.len());
        for (trace_digest, memo_seq, segment, offset, len) in rows {
            let bytes = self.read_extent(segment as u64, offset as u64, len as u64)?;
            let decoded: DecodedRecord = decode_record(&bytes, segment as u64, offset as u64)?;
            let payload = ResultPayload::decode(&decoded.record.payload)?;
            self.conn.execute(
                "UPDATE result_candidates SET last_used = ?4
                 WHERE key_kind = ?1 AND static_key = ?2 AND trace_digest = ?3",
                rusqlite::params![
                    key_kind as i64,
                    static_key.as_slice(),
                    trace_digest.as_slice(),
                    now,
                ],
            )?;
            let mut digest = [0u8; 32];
            digest.copy_from_slice(&trace_digest);
            out.push(Candidate {
                trace_digest: digest,
                memo_seq: MemoSeq(memo_seq as u64),
                asset_uuid: decoded.record.asset_uuid,
                payload,
                segment: segment as u64,
                offset: offset as u64,
            });
        }
        Ok(out)
    }
}

pub(crate) fn upsert_extent(
    txn: &rusqlite::Connection,
    hash: &[u8; 32],
    segment: u64,
    offset: u64,
    len: u64,
) -> Result<(), StoreError> {
    txn.execute(
        "INSERT INTO cas_extents(content_hash, segment, offset, len) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(content_hash) DO UPDATE SET
           segment = excluded.segment, offset = excluded.offset, len = excluded.len",
        rusqlite::params![hash.as_slice(), segment as i64, offset as i64, len as i64],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn upsert_candidate(
    txn: &rusqlite::Connection,
    key_kind: KeyKind,
    static_key: &[u8; 32],
    trace_digest: &[u8; 32],
    memo_seq: MemoSeq,
    segment: u64,
    offset: u64,
    len: u64,
) -> Result<(), StoreError> {
    txn.execute(
        "INSERT INTO result_candidates(key_kind, static_key, trace_digest, memo_seq,
                                       segment, offset, len, last_used)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(key_kind, static_key, trace_digest) DO UPDATE SET
           memo_seq = excluded.memo_seq, segment = excluded.segment,
           offset = excluded.offset, len = excluded.len, last_used = excluded.last_used",
        rusqlite::params![
            key_kind as i64,
            static_key.as_slice(),
            trace_digest.as_slice(),
            memo_seq.0 as i64,
            segment as i64,
            offset as i64,
            len as i64,
            now_millis(),
        ],
    )?;
    Ok(())
}

pub(crate) fn derived_row_matches(
    txn: &rusqlite::Connection,
    child: AssetUuid,
    parent: AssetUuid,
    output_key: &str,
) -> Result<bool, StoreError> {
    use rusqlite::OptionalExtension;
    let row: Option<(Vec<u8>, String)> = txn
        .query_row(
            "SELECT parent_uuid, output_key FROM derived_outputs WHERE child_uuid = ?1",
            [child.0.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(matches!(row, Some((p, k)) if p == parent.0.as_slice() && k == output_key))
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- derived-output namespace (input-versioned, §9/§13) ----

impl crate::db::InputTxn<'_> {
    /// Publish one derived-output namespace row: `child uuid → (parent
    /// uuid, output key)` — derived per published version from its
    /// assets × pinned pipeline map (§9), the only authority for child
    /// resolution.
    pub fn set_derived_output(
        &mut self,
        child: AssetUuid,
        parent: AssetUuid,
        output_key: &str,
    ) -> Result<(), StoreError> {
        self.txn.execute(
            "INSERT INTO derived_outputs(child_uuid, parent_uuid, output_key)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(child_uuid) DO UPDATE SET
               parent_uuid = excluded.parent_uuid, output_key = excluded.output_key",
            rusqlite::params![child.0.as_slice(), parent.0.as_slice(), output_key],
        )?;
        Ok(())
    }

    /// Retire a derived-output namespace row.
    pub fn remove_derived_output(&mut self, child: AssetUuid) -> Result<bool, StoreError> {
        let n = self.txn.execute(
            "DELETE FROM derived_outputs WHERE child_uuid = ?1",
            [child.0.as_slice()],
        )?;
        Ok(n > 0)
    }
}

impl Store {
    /// Resolve a derived child through the namespace index — the only
    /// authority (§9): historical result records never resurrect a
    /// retired child. Namespace-facing: fails under version poison.
    pub fn resolve_child(
        &self,
        child: AssetUuid,
    ) -> Result<Option<(AssetUuid, String)>, StoreError> {
        if let Some(poison) = self.version_poison()? {
            return Err(StoreError::Poisoned {
                error: poison.message,
            });
        }
        use rusqlite::OptionalExtension;
        Ok(self
            .conn
            .query_row(
                "SELECT parent_uuid, output_key FROM derived_outputs WHERE child_uuid = ?1",
                [child.0.as_slice()],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(p, k)| (AssetUuid(crate::bundles::blob16(p)), k)))
    }
}

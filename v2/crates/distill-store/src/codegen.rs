//! Durable expected pre-images for daemon-owned generated Rust files.

use std::collections::BTreeMap;

use distill_core::canonical::CanonicalEncoder;
use distill_core::id::ContentHash;

use crate::state::InputVersion;
use crate::{Store, StoreError, StoreReader};

const CODEGEN_BASIS_MAGIC: [u8; 4] = *b"DSCG";
const CODEGEN_BASIS_VERSION: u8 = 1;

/// The durable completion record carried by a §14 Codegen publication group.
/// Filesystem recovery installs every child first; this exact compare-and-set
/// then moves the expected-preimage authority before the group is retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodegenPublicationBasis {
    input_version: InputVersion,
    previous: BTreeMap<String, ContentHash>,
    proposed: BTreeMap<String, ContentHash>,
}

impl CodegenPublicationBasis {
    pub fn new(
        input_version: InputVersion,
        previous: BTreeMap<String, ContentHash>,
        proposed: BTreeMap<String, ContentHash>,
    ) -> Self {
        Self {
            input_version,
            previous,
            proposed,
        }
    }

    pub fn input_version(&self) -> InputVersion {
        self.input_version
    }

    pub fn previous(&self) -> &BTreeMap<String, ContentHash> {
        &self.previous
    }

    pub fn proposed(&self) -> &BTreeMap<String, ContentHash> {
        &self.proposed
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoder = CanonicalEncoder::new();
        encoder.raw(&CODEGEN_BASIS_MAGIC);
        encoder.u8(CODEGEN_BASIS_VERSION);
        encoder.u64(self.input_version.0);
        encode_outputs(&mut encoder, &self.previous);
        encode_outputs(&mut encoder, &self.proposed);
        encoder.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut decoder = BasisDecoder::new(bytes);
        if decoder.raw(4)? != CODEGEN_BASIS_MAGIC {
            return Err("codegen publication basis has the wrong magic".into());
        }
        let version = decoder.u8()?;
        if version != CODEGEN_BASIS_VERSION {
            return Err(format!(
                "unsupported codegen publication basis version {version}"
            ));
        }
        let input_version = InputVersion(decoder.u64()?);
        let previous = decoder.outputs()?;
        let proposed = decoder.outputs()?;
        if !decoder.remaining().is_empty() {
            return Err("codegen publication basis has trailing bytes".into());
        }
        let basis = Self::new(input_version, previous, proposed);
        if basis.encode() != bytes {
            return Err("codegen publication basis is not canonical".into());
        }
        Ok(basis)
    }
}

impl Store {

    /// Replace the complete generated namespace only if the caller names the
    /// exact previously published pre-images. This is memo-side state: source
    /// input versions do not advance when generated files change.
    pub fn commit_codegen_outputs(
        &mut self,
        expected: &BTreeMap<String, ContentHash>,
        proposed: &BTreeMap<String, ContentHash>,
    ) -> Result<(), StoreError> {
        self.memo_transaction(|transaction, _| {
            if &read_outputs(transaction)? != expected {
                return Err(StoreError::CodegenStateDrift);
            }
            transaction.execute("DELETE FROM codegen_outputs", [])?;
            let mut insert = transaction.prepare(
                "INSERT INTO codegen_outputs(relative_path, content_hash) VALUES (?1, ?2)",
            )?;
            for (path, hash) in proposed {
                insert.execute(rusqlite::params![path, hash.0.as_slice()])?;
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Complete or replay a recovered Codegen publication group. Applying the
    /// proposed map twice is a no-op; any third state proves that a different
    /// publication crossed the journal basis and is rejected.
    pub fn apply_codegen_publication_basis(
        &mut self,
        basis: &CodegenPublicationBasis,
    ) -> Result<(), StoreError> {
        if self.input_version() != basis.input_version {
            return Err(StoreError::CodegenStateDrift);
        }
        let current = self.codegen_outputs()?;
        if current == basis.proposed {
            return Ok(());
        }
        self.commit_codegen_outputs(&basis.previous, &basis.proposed)
    }
}

impl StoreReader {
    pub fn codegen_outputs(&self) -> Result<BTreeMap<String, ContentHash>, StoreError> {
        read_outputs(&self.conn)
    }
}

fn encode_outputs(encoder: &mut CanonicalEncoder, outputs: &BTreeMap<String, ContentHash>) {
    let rows = outputs.iter().collect::<Vec<_>>();
    encoder.seq(&rows, |encoder, (path, hash)| {
        encoder.str(path);
        encoder.raw(&hash.0);
    });
}

fn read_outputs(
    connection: &rusqlite::Connection,
) -> Result<BTreeMap<String, ContentHash>, StoreError> {
    let mut statement = connection.prepare(
        "SELECT relative_path, content_hash FROM codegen_outputs ORDER BY relative_path",
    )?;
    let rows = statement.query_map([], |row| {
        let path = row.get::<_, String>(0)?;
        let bytes = row.get::<_, Vec<u8>>(1)?;
        let hash: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            rusqlite::Error::FromSqlConversionFailure(
                bytes.len(),
                rusqlite::types::Type::Blob,
                "codegen output hash has the wrong width".into(),
            )
        })?;
        Ok((path, ContentHash(hash)))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(StoreError::from)
}

struct BasisDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> BasisDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    fn raw(&mut self, len: usize) -> Result<&'a [u8], String> {
        if self.remaining.len() < len {
            return Err("truncated codegen publication basis".into());
        }
        let (value, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.raw(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.raw(4)?
                .try_into()
                .expect("the decoder returned exactly four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.raw(8)?
                .try_into()
                .expect("the decoder returned exactly eight bytes"),
        ))
    }

    fn string(&mut self) -> Result<String, String> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| "codegen publication path length does not fit usize".to_owned())?;
        let bytes = self.raw(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| "codegen publication path is not UTF-8".into())
    }

    fn outputs(&mut self) -> Result<BTreeMap<String, ContentHash>, String> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| "codegen publication row count does not fit usize".to_owned())?;
        if count > self.remaining.len() / 36 {
            return Err("codegen publication row count exceeds the remaining record".into());
        }
        let mut outputs = BTreeMap::new();
        let mut prior = None;
        for _ in 0..count {
            let path = self.string()?;
            if prior.as_ref().is_some_and(|prior| prior >= &path) {
                return Err("codegen publication paths are not strictly sorted".into());
            }
            validate_generated_path(&path)?;
            let hash = ContentHash(
                self.raw(32)?
                    .try_into()
                    .expect("the decoder returned exactly 32 bytes"),
            );
            prior = Some(path.clone());
            outputs.insert(path, hash);
        }
        Ok(outputs)
    }
}

fn validate_generated_path(path: &str) -> Result<(), String> {
    if path == "mod.rs" {
        return Ok(());
    }
    let Some(hex) = path
        .strip_prefix("sp_")
        .and_then(|path| path.strip_suffix(".rs"))
    else {
        return Err(format!("invalid generated Rust path {path:?}"));
    };
    if hex.len() != 32
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(format!("invalid generated Rust path {path:?}"));
    }
    Ok(())
}

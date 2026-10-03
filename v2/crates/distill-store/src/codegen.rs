//! Durable expected pre-images for daemon-owned generated Rust files.

use std::collections::BTreeMap;

use distill_core::id::ContentHash;

use crate::{Store, StoreError, StoreReader};

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
            transaction
                .prepare_cached("DELETE FROM codegen_outputs")?
                .execute([])?;
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
}

impl StoreReader {
    pub fn codegen_outputs(&self) -> Result<BTreeMap<String, ContentHash>, StoreError> {
        read_outputs(&self.conn)
    }
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


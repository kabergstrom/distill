#![allow(dead_code)]
use std::path::Path;

use async_lock::{Semaphore, SemaphoreGuard};
use rusqlite::{Connection, OpenFlags};

use crate::error::Result;

/// Owned capnp reader — replaces the zero-copy `MessageReader<'a, T>` that LMDB provided via mmap.
/// SQLite returns owned `Vec<u8>`, so we deserialise into an owned reader with no lifetime parameter.
pub type OwnedMessageReader<T> =
    capnp::message::TypedReader<capnp::serialize::OwnedSegments, T>;

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

pub struct Database {
    path: std::path::PathBuf,
    write_semaphore: Semaphore,
}

impl Database {
    pub fn new(path: &Path) -> Result<Database> {
        let _ = std::fs::create_dir_all(path);
        let db_file = path.join("distill.db");

        // Open a temporary connection to initialise WAL mode + tables.
        let conn = Connection::open(&db_file)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;",
        )?;
        Self::create_tables(&conn)?;
        drop(conn);

        Ok(Database {
            path: db_file,
            write_semaphore: Semaphore::new(1),
        })
    }

    fn create_tables(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            -- Simple KV tables (capnp blob values)
            CREATE TABLE IF NOT EXISTS source_files     (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS dirty_files      (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS asset_metadata   (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS path_to_metadata (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS asset_id_to_path (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS daemon_info      (key BLOB PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS hash_to_artifact (key BLOB PRIMARY KEY, value BLOB NOT NULL);

            -- Forward-relationship tables (replace hand-rolled reverse indexes)
            CREATE TABLE IF NOT EXISTS build_deps (
                asset_id BLOB NOT NULL,
                dep_id   BLOB NOT NULL,
                PRIMARY KEY (asset_id, dep_id)
            );
            CREATE INDEX IF NOT EXISTS idx_build_deps_dep ON build_deps(dep_id);

            CREATE TABLE IF NOT EXISTS path_refs (
                source_path TEXT NOT NULL,
                ref_path    TEXT NOT NULL,
                PRIMARY KEY (source_path, ref_path)
            );
            CREATE INDEX IF NOT EXISTS idx_path_refs_ref ON path_refs(ref_path);

            -- Sequence tables (autoincrement replaces manual seq scan)
            CREATE TABLE IF NOT EXISTS rename_file_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                src TEXT NOT NULL,
                dst TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS asset_changes (
                seq   INTEGER PRIMARY KEY AUTOINCREMENT,
                value BLOB NOT NULL
            );
            ",
        )?;
        Ok(())
    }

    pub async fn rw_txn(&self) -> Result<RwTransaction> {
        let guard = self.write_semaphore.acquire().await;
        // Safety: The Database (and its Semaphore) is always held in an Arc that
        // outlives all RwTransactions. The guard is dropped when RwTransaction drops.
        let guard: SemaphoreGuard<'static> = unsafe { std::mem::transmute(guard) };
        let conn = Connection::open(&self.path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(RwTransaction {
            conn,
            _guard: guard,
            dirty: false,
        })
    }

    pub async fn ro_txn(&self) -> Result<RoTransaction> {
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("BEGIN DEFERRED")?;
        Ok(RoTransaction { conn })
    }
}

// ---------------------------------------------------------------------------
// RwTransaction
// ---------------------------------------------------------------------------

#[must_use]
pub struct RwTransaction {
    conn: Connection,
    _guard: SemaphoreGuard<'static>,
    pub dirty: bool,
}

// Safety: rusqlite::Connection is Send but not Sync. We only ever use the
// connection from one task at a time (guarded by the write semaphore), so
// sending the transaction across await points is safe.
unsafe impl Send for RwTransaction {}
unsafe impl Sync for RwTransaction {}

impl RwTransaction {
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn commit(self) -> Result<()> {
        if self.dirty {
            self.conn.execute_batch("COMMIT")?;
        }
        Ok(())
    }
}

impl Drop for RwTransaction {
    fn drop(&mut self) {
        // Rollback if not committed. Ignore errors since the connection is about to be dropped.
        let _ = self.conn.execute_batch("ROLLBACK");
    }
}

// ---------------------------------------------------------------------------
// RoTransaction
// ---------------------------------------------------------------------------

pub struct RoTransaction {
    conn: Connection,
}

unsafe impl Send for RoTransaction {}
unsafe impl Sync for RoTransaction {}

impl RoTransaction {
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

impl Drop for RoTransaction {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch("ROLLBACK");
    }
}

// ---------------------------------------------------------------------------
// queries — helpers for the KV-style tables
// ---------------------------------------------------------------------------

pub mod queries {
    use rusqlite::{params, Connection};

    use crate::error::Result;

    use super::OwnedMessageReader;

    /// Read a capnp message from a KV table.
    pub fn get_capnp<V: for<'b> capnp::traits::Owned<'b>>(
        conn: &Connection,
        table: &str,
        key: &[u8],
    ) -> Result<Option<OwnedMessageReader<V>>> {
        let sql = format!("SELECT value FROM {} WHERE key = ?1", table);
        let mut stmt = conn.prepare_cached(&sql)?;
        let result: std::result::Result<Vec<u8>, _> =
            stmt.query_row(rusqlite::params![key], |row| row.get(0));
        match result {
            Ok(bytes) => {
                let reader = capnp::serialize::read_message(
                    &mut bytes.as_slice(),
                    distill_schema::default_capnp_reader_options(),
                )?;
                Ok(Some(reader.into_typed::<V>()))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Read raw bytes from a KV table.
    pub fn get_bytes(
        conn: &Connection,
        table: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let sql = format!("SELECT value FROM {} WHERE key = ?1", table);
        let mut stmt = conn.prepare_cached(&sql)?;
        let result: std::result::Result<Vec<u8>, _> =
            stmt.query_row(rusqlite::params![key], |row| row.get(0));
        match result {
            Ok(bytes) => Ok(Some(bytes)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write a capnp message to a KV table.
    pub fn put_capnp<K: capnp::message::Allocator>(
        conn: &Connection,
        table: &str,
        key: &[u8],
        value: &capnp::message::Builder<K>,
    ) -> Result<()> {
        let mut value_bytes = Vec::new();
        capnp::serialize::write_message(&mut value_bytes, value)?;
        let sql = format!(
            "INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)",
            table
        );
        conn.execute(&sql, params![key, value_bytes])?;
        Ok(())
    }

    /// Write raw bytes to a KV table.
    pub fn put_bytes(
        conn: &Connection,
        table: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let sql = format!(
            "INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)",
            table
        );
        conn.execute(&sql, params![key, value])?;
        Ok(())
    }

    /// Delete a key from a KV table. Returns true if a row was deleted.
    pub fn delete(conn: &Connection, table: &str, key: &[u8]) -> Result<bool> {
        let sql = format!("DELETE FROM {} WHERE key = ?1", table);
        let count = conn.execute(&sql, params![key])?;
        Ok(count > 0)
    }

    /// Delete all rows from a table.
    pub fn clear_table(conn: &Connection, table: &str) -> Result<()> {
        let sql = format!("DELETE FROM {}", table);
        conn.execute(&sql, [])?;
        Ok(())
    }

    /// Iterate all rows in a KV table, returning (key, capnp reader) pairs.
    pub fn iter_all<V: for<'b> capnp::traits::Owned<'b>>(
        conn: &Connection,
        table: &str,
    ) -> Result<Vec<(Vec<u8>, OwnedMessageReader<V>)>> {
        let sql = format!("SELECT key, value FROM {} ORDER BY key", table);
        let mut stmt = conn.prepare_cached(&sql)?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            let reader = capnp::serialize::read_message(
                &mut value.as_slice(),
                distill_schema::default_capnp_reader_options(),
            )?;
            result.push((key, reader.into_typed::<V>()));
        }
        Ok(result)
    }

    /// Iterate rows in a KV table whose key starts with the given prefix (byte-wise >=).
    /// Returns (key, capnp reader) pairs.
    pub fn iter_prefix<V: for<'b> capnp::traits::Owned<'b>>(
        conn: &Connection,
        table: &str,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, OwnedMessageReader<V>)>> {
        let sql = format!("SELECT key, value FROM {} WHERE key >= ?1 ORDER BY key", table);
        let mut stmt = conn.prepare_cached(&sql)?;
        let mut rows = stmt.query(params![prefix])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            let reader = capnp::serialize::read_message(
                &mut value.as_slice(),
                distill_schema::default_capnp_reader_options(),
            )?;
            result.push((key, reader.into_typed::<V>()));
        }
        Ok(result)
    }

    /// Iterate all rows in a KV table, returning (key_bytes, value_bytes) pairs.
    pub fn iter_all_raw(
        conn: &Connection,
        table: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let sql = format!("SELECT key, value FROM {} ORDER BY key", table);
        let mut stmt = conn.prepare_cached(&sql)?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            result.push((key, value));
        }
        Ok(result)
    }
}

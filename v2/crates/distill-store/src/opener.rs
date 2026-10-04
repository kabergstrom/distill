//! One state directory opened by every thread of a process (LOCKLESS.md
//! §3). Each owner (the process loop, an RPC connection, a build job)
//! opens a writer of its own from the [`StoreOpener`] and passes it
//! explicitly to whatever writes on its behalf. SQLite's write lock orders
//! the writers, and every write transaction takes it up front
//! (`BEGIN IMMEDIATE`, [`Store::write_transaction`]).
//!
//! The opener holds no store state: only what opening a writer or reader
//! needs, and the operational configuration writers follow.

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::ConfigValidationError;
use crate::current::Current;
use crate::state::StoreInstanceId;
use crate::{Store, StoreConfig, StoreError, StoreReader};

/// What a thread needs to open a writer or a reader on one state
/// directory: `Send + Sync` and immutable apart from the operational
/// configuration.
pub struct StoreOpener {
    instance: StoreInstanceId,
    config: Arc<Current<StoreConfig>>,
    cas_dir: PathBuf,
    /// Keeps this process's lock on the state directory for as long as
    /// writers can be opened.
    state_lock: Arc<std::fs::File>,
}

impl std::fmt::Debug for StoreOpener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreOpener")
            .field("state_path", &self.config.load().state_path)
            .finish_non_exhaustive()
    }
}

impl StoreOpener {
    /// The opener of `store`'s state directory, and `store` as its first
    /// writer. Every writer opened from it follows the opener's operational
    /// configuration.
    pub fn new(mut store: Store) -> (Arc<Self>, StoreWriter) {
        let config = Arc::new(Current::from_arc(Arc::clone(&store.read.config)));
        store.config_source = Some(Arc::clone(&config));
        let opener = Arc::new(Self {
            instance: store.instance_id(),
            config,
            cas_dir: store.cas.dir.clone(),
            state_lock: Arc::clone(&store._state_lock),
        });
        (opener, StoreWriter(store))
    }

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance
    }

    pub fn config(&self) -> Arc<StoreConfig> {
        self.config.load()
    }

    /// Apply the operational-live subset of `candidate`. Every writer picks
    /// it up as its next transaction begins.
    pub fn apply_operational_config(
        &self,
        candidate: &StoreConfig,
    ) -> Result<(), ConfigValidationError> {
        candidate.validate_scheduler()?;
        self.config.update(|current| {
            let mut config = current.clone();
            config.segment_size = candidate.segment_size;
            config.cache_limit = candidate.cache_limit;
            config.parallelism = candidate.parallelism;
            config.batch_reserved_workers = candidate.batch_reserved_workers;
            config
        });
        Ok(())
    }

    /// A writer of the caller's own: its own connection and CAS segment.
    pub fn open_writer(&self) -> Result<StoreWriter, StoreError> {
        let mut store = Store::open_sibling(
            self.config.load(),
            self.instance,
            self.cas_dir.clone(),
            Arc::clone(&self.state_lock),
        )?;
        store.config_source = Some(Arc::clone(&self.config));
        Ok(StoreWriter(store))
    }

    /// A reader of the caller's own.
    pub fn open_reader(&self) -> Result<StoreReader, StoreError> {
        StoreReader::open(StoreConfig::clone(&self.config.load()))
    }
}

/// A writer one owner opened ([`StoreOpener::open_writer`]). When its owner
/// closes it, its CAS segment is sealed: no other writer ever appends to it.
pub struct StoreWriter(Store);

impl std::fmt::Debug for StoreWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::ops::Deref for StoreWriter {
    type Target = Store;

    fn deref(&self) -> &Store {
        &self.0
    }
}

impl std::ops::DerefMut for StoreWriter {
    fn deref_mut(&mut self) -> &mut Store {
        &mut self.0
    }
}

impl Drop for StoreWriter {
    fn drop(&mut self) {
        // A writer dropped mid-transaction (its owner panicked) rolls it
        // back first.
        if !self.0.read.conn.is_autocommit() {
            let _ = self.0.read.conn.execute_batch("ROLLBACK");
            self.0.read.counters.end();
        }
        if let Err(error) = self.0.seal_active() {
            tracing::warn!(%error, "sealing a closing writer's segment failed");
        }
    }
}

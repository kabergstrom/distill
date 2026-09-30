//! One state directory shared by every thread of a process (LOCKLESS.md
//! §3). Each thread writes on a connection of its own; SQLite's write lock
//! orders the writers, and every write transaction takes it up front
//! (`BEGIN IMMEDIATE`, [`Store::write_transaction`]).
//!
//! A thread borrows a writer for as long as it holds a [`WriteGuard`], and
//! keeps it while it has a transaction open or an input armed: the next
//! guard on that thread gets the same connection, and reads on that thread
//! go through it and see its uncommitted writes. Otherwise a writer goes
//! back to an idle pool and reads use this thread's own [`StoreReader`], in
//! one read transaction per guard.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::config::ConfigValidationError;
use crate::served::StoreSnapshot;
use crate::state::StoreInstanceId;
use crate::{Store, StoreConfig, StoreError, StoreReader};

/// Readers a thread keeps open, most recently used last.
const THREAD_READERS: usize = 4;
/// Idle writers kept open; one more is sealed and closed.
const IDLE_WRITERS: usize = 16;

static NEXT_SHARED_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The writers this thread holds, by [`SharedStore`].
    static HELD: RefCell<Vec<(u64, Rc<HeldWriter>)>> = const { RefCell::new(Vec::new()) };
    static READERS: RefCell<Vec<StoreReader>> = const { RefCell::new(Vec::new()) };
    /// Store guards open on this thread.
    static OPEN_GUARDS: Cell<usize> = const { Cell::new(0) };
}

/// Whether this thread holds a store guard.
pub fn guard_held() -> bool {
    OPEN_GUARDS.get() != 0
}

/// A writer held by one thread: a `RefCell` its guards share.
struct HeldWriter {
    store: UnsafeCell<Store>,
    /// 0 when free, n > 0 for n reads, -1 for the write.
    borrow: Cell<isize>,
}

pub struct SharedStore {
    id: u64,
    instance: StoreInstanceId,
    config: ArcSwap<StoreConfig>,
    /// Writers no thread holds. Held only to push or pop one.
    idle: Mutex<Vec<Store>>,
    /// Everything a new writer needs besides its connection.
    template: WriterTemplate,
}

struct WriterTemplate {
    cas_dir: std::path::PathBuf,
    state_lock: Arc<std::fs::File>,
}

impl SharedStore {
    /// Share `store`, which becomes the first idle writer.
    pub fn new(store: Store) -> Self {
        Self {
            id: NEXT_SHARED_ID.fetch_add(1, Ordering::Relaxed),
            instance: store.instance_id(),
            config: ArcSwap::new(Arc::clone(&store.read.config)),
            template: WriterTemplate {
                cas_dir: store.cas.dir.clone(),
                state_lock: Arc::clone(&store._state_lock),
            },
            idle: Mutex::new(vec![store]),
        }
    }

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance
    }

    pub fn config(&self) -> Arc<StoreConfig> {
        self.config.load_full()
    }

    /// Apply the operational-live subset of `candidate` to every writer,
    /// each from its next guard on.
    pub fn apply_operational_config(
        &self,
        candidate: &StoreConfig,
    ) -> Result<(), ConfigValidationError> {
        candidate.validate_scheduler()?;
        let mut config = StoreConfig::clone(&self.config.load());
        config.segment_size = candidate.segment_size;
        config.cache_limit = candidate.cache_limit;
        config.parallelism = candidate.parallelism;
        config.batch_reserved_workers = candidate.batch_reserved_workers;
        self.config.store(Arc::new(config));
        Ok(())
    }

    /// A reader of its own, for work that outlives one guard.
    pub fn open_reader(&self) -> Result<StoreReader, StoreError> {
        StoreReader::open(StoreConfig::clone(&self.config.load()))
    }

    fn held(&self) -> Option<Rc<HeldWriter>> {
        HELD.with_borrow(|held| {
            held.iter()
                .find(|(id, _)| *id == self.id)
                .map(|(_, writer)| Rc::clone(writer))
        })
    }

    /// Whether this thread holds a writer with a transaction open.
    pub fn in_transaction(&self) -> bool {
        self.held().is_some_and(|writer| {
            writer.borrow.get() >= 0
                // SAFETY: no write borrow is live (checked first).
                && unsafe { &*writer.store.get() }.in_transaction()
        })
    }

    /// This thread's writer. Panics if this thread already holds a guard
    /// on it.
    pub fn write(&self) -> WriteGuard<'_> {
        let writer = match self.held() {
            Some(writer) => writer,
            None => {
                let pooled = self.idle.lock().expect("idle writers").pop();
                let store = pooled.map(Ok).unwrap_or_else(|| self.open_writer());
                let store = store.unwrap_or_else(|error| {
                    panic!("cannot open a writer on {}: {error}", self.state_display())
                });
                let writer = Rc::new(HeldWriter {
                    store: UnsafeCell::new(store),
                    borrow: Cell::new(0),
                });
                HELD.with_borrow_mut(|held| held.push((self.id, Rc::clone(&writer))));
                writer
            }
        };
        assert!(
            writer.borrow.get() == 0,
            "the store is already borrowed on this thread"
        );
        writer.borrow.set(-1);
        // SAFETY: the write borrow is the only one (checked above).
        let store = unsafe { &mut *writer.store.get() };
        let config = self.config.load();
        if !Arc::ptr_eq(&store.read.config, &config) {
            store.read.config = Arc::clone(&config);
        }
        OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
        WriteGuard {
            shared: self,
            writer: Some(writer),
        }
    }

    /// Read the store. A thread in a write transaction reads through its
    /// writer; otherwise this reads this thread's reader, in one read
    /// transaction for the guard's life.
    pub fn read(&self) -> ReadGuard<'_> {
        if let Some(writer) = self.held() {
            assert!(
                writer.borrow.get() >= 0,
                "the store is read while written on this thread"
            );
            // SAFETY: no write borrow is live (checked above).
            if unsafe { &*writer.store.get() }.in_transaction() {
                writer.borrow.set(writer.borrow.get() + 1);
                OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
                return ReadGuard {
                    inner: ReadInner::Writer(writer),
                    _shared: std::marker::PhantomData,
                };
            }
        }
        let config = self.config.load();
        let reader = READERS
            .with_borrow_mut(|readers| {
                readers
                    .iter()
                    .position(|reader| {
                        reader.state_path() == config.state_path
                            && reader.instance_id() == self.instance
                    })
                    .map(|index| readers.remove(index))
            })
            .map(Ok)
            .unwrap_or_else(|| StoreReader::open(StoreConfig::clone(&config)))
            .and_then(StoreReader::begin_snapshot)
            .unwrap_or_else(|error| {
                panic!("cannot read the store at {}: {error}", self.state_display())
            });
        OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
        ReadGuard {
            inner: ReadInner::Reader(Some(reader)),
            _shared: std::marker::PhantomData,
        }
    }

    fn open_writer(&self) -> Result<Store, StoreError> {
        Store::open_sibling(
            self.config.load_full(),
            self.instance,
            self.template.cas_dir.clone(),
            Arc::clone(&self.template.state_lock),
        )
    }

    fn state_display(&self) -> String {
        self.config.load().state_path.display().to_string()
    }

    /// Give a writer this thread no longer needs back to the pool.
    fn release(&self, writer: Rc<HeldWriter>) {
        HELD.with_borrow_mut(|held| held.retain(|(id, _)| *id != self.id));
        let Ok(writer) = Rc::try_unwrap(writer) else {
            unreachable!("an unborrowed writer has no other holder");
        };
        let mut store = writer.store.into_inner();
        let mut idle = self.idle.lock().expect("idle writers");
        if idle.len() < IDLE_WRITERS {
            idle.push(store);
            return;
        }
        drop(idle);
        if let Err(error) = store.seal_active() {
            tracing::warn!(%error, "sealing a closing writer's segment failed");
        }
    }
}

pub struct WriteGuard<'a> {
    shared: &'a SharedStore,
    writer: Option<Rc<HeldWriter>>,
}

impl WriteGuard<'_> {
    fn cell(&self) -> &HeldWriter {
        self.writer.as_ref().expect("a write guard holds its writer")
    }
}

impl std::ops::Deref for WriteGuard<'_> {
    type Target = Store;

    fn deref(&self) -> &Store {
        // SAFETY: the guard holds the write borrow.
        unsafe { &*self.cell().store.get() }
    }
}

impl std::ops::DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Store {
        // SAFETY: as above; `&mut self` makes this the only reference.
        unsafe { &mut *self.cell().store.get() }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        OPEN_GUARDS.set(OPEN_GUARDS.get() - 1);
        let writer = self.writer.take().expect("a write guard holds its writer");
        writer.borrow.set(0);
        // SAFETY: no borrow is live.
        let open = unsafe { &*writer.store.get() }.in_transaction();
        // A writer with a transaction open stays with this thread.
        if !open {
            self.shared.release(writer);
        }
    }
}

pub struct ReadGuard<'a> {
    inner: ReadInner,
    _shared: std::marker::PhantomData<&'a SharedStore>,
}

enum ReadInner {
    Writer(Rc<HeldWriter>),
    Reader(Option<StoreSnapshot>),
}

impl std::ops::Deref for ReadGuard<'_> {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        match &self.inner {
            // SAFETY: the guard holds a shared borrow.
            ReadInner::Writer(writer) => unsafe { &*writer.store.get() },
            ReadInner::Reader(reader) => reader.as_ref().expect("a read guard holds its reader"),
        }
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        OPEN_GUARDS.set(OPEN_GUARDS.get() - 1);
        match &mut self.inner {
            ReadInner::Writer(writer) => writer.borrow.set(writer.borrow.get() - 1),
            ReadInner::Reader(reader) => {
                let snapshot = reader.take().expect("a read guard holds its reader");
                // A reader whose transaction will not close is dropped
                // rather than reused.
                if let Ok(reader) = snapshot.into_reader() {
                    READERS.with_borrow_mut(|readers| {
                        if readers.len() == THREAD_READERS {
                            readers.remove(0);
                        }
                        readers.push(reader);
                    });
                }
            }
        }
    }
}

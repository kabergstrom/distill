//! The daemon's store, owned by the authority (LOCKLESS.md §3).
//!
//! Only the authority writes: [`AuthorityStore::write`] is valid there (or
//! on a thread the authority lent itself to while it waits, see
//! [`crate::authority::AuthoritySender::lend`]), and
//! [`AuthorityStore::write_with`] sends the write there from anywhere else.
//! Reads on the authority borrow the writer; reads elsewhere use this
//! thread's own [`StoreReader`], inside one read transaction per guard.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::path::PathBuf;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread::ThreadId;

use distill_rpc::AuthorityStopped;
use distill_store::state::StoreInstanceId;
use distill_store::served::StoreSnapshot;
use distill_store::{Store, StoreConfig, StoreReader};

use crate::authority::AuthoritySender;

/// Readers a thread keeps open, most recently used last.
const THREAD_READERS: usize = 4;

thread_local! {
    static READERS: RefCell<Vec<StoreReader>> = const { RefCell::new(Vec::new()) };
    /// Store guards open on this thread.
    static OPEN_GUARDS: Cell<usize> = const { Cell::new(0) };
}

/// Whether this thread holds a store guard.
pub(crate) fn guard_held() -> bool {
    OPEN_GUARDS.get() != 0
}

enum Owner {
    Authority(AuthoritySender),
    /// A store without an authority (tests, tools): the thread that made it.
    Thread(ThreadId),
}

pub struct AuthorityStore {
    store: UnsafeCell<Store>,
    owner: Owner,
    /// 0 when free, n > 0 for n shared borrows, -1 for the write borrow.
    /// Only the owner touches it.
    borrow: AtomicIsize,
    config: StoreConfig,
    state_path: PathBuf,
    instance: StoreInstanceId,
}

// SAFETY: the store is only reached through `write`/`read`, which check that
// the caller is the owner. An authority and a thread it lent itself to are
// never both running (the authority is blocked meanwhile); the borrow count
// catches any overlap that would alias.
unsafe impl Sync for AuthorityStore {}

impl AuthorityStore {
    pub(crate) fn new(store: Store, authority: AuthoritySender) -> Self {
        Self::with_owner(store, Owner::Authority(authority))
    }

    /// A store owned by the calling thread, for code that runs without an
    /// authority.
    pub fn on_this_thread(store: Store) -> Self {
        Self::with_owner(store, Owner::Thread(std::thread::current().id()))
    }

    fn with_owner(store: Store, owner: Owner) -> Self {
        let config = store.config().clone();
        let state_path = store.state_path().to_path_buf();
        let instance = store.instance_id();
        Self {
            store: UnsafeCell::new(store),
            owner,
            borrow: AtomicIsize::new(0),
            config,
            state_path,
            instance,
        }
    }

    /// Whether the calling thread owns the store.
    pub fn owned(&self) -> bool {
        match &self.owner {
            Owner::Authority(authority) => authority.on_authority(),
            Owner::Thread(thread) => *thread == std::thread::current().id(),
        }
    }

    pub fn instance_id(&self) -> StoreInstanceId {
        self.instance
    }

    /// A reader of its own, for work that outlives one guard.
    pub(crate) fn open_reader(&self) -> Result<StoreReader, distill_store::StoreError> {
        StoreReader::open(self.config.clone())
    }

    /// The writer. Panics off the authority: send the write there with
    /// [`AuthorityStore::write_with`].
    pub fn write(&self) -> WriteGuard<'_> {
        assert!(self.owned(), "the store is written only on its authority");
        if self
            .borrow
            .compare_exchange(0, -1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            panic!("the store is already borrowed on its authority");
        }
        OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
        WriteGuard { cell: self }
    }

    /// Run `write` against the writer on the authority, from any thread.
    pub fn write_with<R: Send>(
        &self,
        write: impl FnOnce(&mut Store) -> R + Send,
    ) -> Result<R, AuthorityStopped> {
        match &self.owner {
            Owner::Authority(authority) => authority.run(|| write(&mut self.write())),
            Owner::Thread(_) => Ok(write(&mut self.write())),
        }
    }

    /// Read the store. On the authority this reads the writer (and sees
    /// what it has written); elsewhere it reads this thread's reader, in
    /// one read transaction for the guard's life.
    pub fn read(&self) -> ReadGuard<'_> {
        if self.owned() {
            let previous = self.borrow.fetch_add(1, Ordering::Acquire);
            if previous < 0 {
                self.borrow.fetch_sub(1, Ordering::Release);
                panic!("the store is read while written on its authority");
            }
            // SAFETY: owner thread, no write borrow (checked above).
            let store = unsafe { &*self.store.get() };
            OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
            return ReadGuard {
                inner: ReadInner::Writer {
                    store,
                    borrow: &self.borrow,
                },
            };
        }
        let reader = READERS
            .with_borrow_mut(|readers| {
                readers
                    .iter()
                    .position(|reader| {
                        reader.state_path() == self.state_path
                            && reader.instance_id() == self.instance
                    })
                    .map(|index| readers.remove(index))
            })
            .map(Ok)
            .unwrap_or_else(|| StoreReader::open(self.config.clone()))
            .and_then(StoreReader::begin_snapshot)
            .unwrap_or_else(|error| {
                panic!(
                    "cannot read the store at {}: {error}",
                    self.state_path.display()
                )
            });
        OPEN_GUARDS.set(OPEN_GUARDS.get() + 1);
        ReadGuard {
            inner: ReadInner::Reader(Some(reader)),
        }
    }
}

pub struct WriteGuard<'a> {
    cell: &'a AuthorityStore,
}

impl std::ops::Deref for WriteGuard<'_> {
    type Target = Store;

    fn deref(&self) -> &Store {
        // SAFETY: the guard holds the write borrow on the owner thread.
        unsafe { &*self.cell.store.get() }
    }
}

impl std::ops::DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Store {
        // SAFETY: as above; `&mut self` makes this the only reference.
        unsafe { &mut *self.cell.store.get() }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        OPEN_GUARDS.set(OPEN_GUARDS.get() - 1);
        self.cell.borrow.store(0, Ordering::Release);
    }
}

pub struct ReadGuard<'a> {
    inner: ReadInner<'a>,
}

enum ReadInner<'a> {
    Writer {
        store: &'a Store,
        borrow: &'a AtomicIsize,
    },
    Reader(Option<StoreSnapshot>),
}

impl std::ops::Deref for ReadGuard<'_> {
    type Target = StoreReader;

    fn deref(&self) -> &StoreReader {
        match &self.inner {
            ReadInner::Writer { store, .. } => store,
            ReadInner::Reader(reader) => reader.as_ref().expect("a read guard holds its reader"),
        }
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        OPEN_GUARDS.set(OPEN_GUARDS.get() - 1);
        match &mut self.inner {
            ReadInner::Writer { borrow, .. } => {
                borrow.fetch_sub(1, Ordering::Release);
            }
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

//! The authority thread (LOCKLESS.md §3): the one thread that publishes.
//!
//! It blocks on its inbox. Other threads hand it jobs
//! ([`distill_rpc::ServerHandle::on_authority`]); the watcher sends it
//! events, which the attached [`Driver`] (the daemon process loop) folds
//! into its queue and reconciles when its deadline fires.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Instant;

use distill_rpc::AuthorityJob;

use crate::watcher::WatcherEvent;

static NEXT_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The authority this thread runs, or 0.
    static CURRENT: Cell<u64> = const { Cell::new(0) };
}

/// The event loop state the daemon process runs on the authority.
pub(crate) trait Driver {
    fn watch(&mut self, event: WatcherEvent);
    /// When [`Driver::fire`] is next due.
    fn deadline(&self) -> Instant;
    /// Run the due work. `false` detaches the driver.
    fn fire(&mut self) -> bool;
}

/// Builds a driver on the authority thread; `None` attaches nothing.
pub(crate) type DriverFactory = Box<dyn FnOnce() -> Option<Box<dyn Driver>> + Send>;

enum Message {
    Run(AuthorityJob),
    Watch(WatcherEvent),
    Attach(DriverFactory),
    Detach,
    Shutdown,
}

/// A handle for sending to one authority.
#[derive(Clone)]
pub(crate) struct AuthoritySender {
    id: u64,
    inbox: mpsc::Sender<Message>,
}

impl AuthoritySender {
    /// Whether the calling thread is this authority.
    pub(crate) fn on_authority(&self) -> bool {
        CURRENT.get() == self.id
    }

    /// Run `step` as this authority: inline when already on it, else on
    /// the authority thread, blocking until it has run.
    pub(crate) fn run<T: Send>(
        &self,
        step: impl FnOnce() -> T + Send,
    ) -> Result<T, distill_rpc::AuthorityStopped> {
        if self.on_authority() {
            Ok(step())
        } else {
            distill_rpc::run_scoped(|job| self.execute(job), step)
        }
    }

    /// Called on the authority before it blocks on work it handed to
    /// another thread: that thread acts as the authority while it runs
    /// the work (the store and publications are the blocked authority's).
    pub(crate) fn lend(&self) -> Option<Lent> {
        self.on_authority().then_some(Lent(self.id))
    }

    /// Queue `job`; a stopped authority drops it unrun.
    pub(crate) fn execute(&self, job: AuthorityJob) {
        let _ = self.inbox.send(Message::Run(job));
    }

    pub(crate) fn watch(&self, event: WatcherEvent) {
        let _ = self.inbox.send(Message::Watch(event));
    }

    /// Install the process loop. Watcher events sent before it runs wait
    /// in the inbox; events sent while no driver is attached are dropped.
    pub(crate) fn attach(&self, factory: DriverFactory) {
        let _ = self.inbox.send(Message::Attach(factory));
    }

    /// Drop the process loop and wait until it is gone.
    pub(crate) fn detach(&self) {
        let _ = self.inbox.send(Message::Detach);
        if !self.on_authority() {
            let (done, wait) = mpsc::sync_channel::<()>(1);
            let _ = self.inbox.send(Message::Run(Box::new(move || {
                let _ = done.send(());
            })));
            let _ = wait.recv();
        }
    }
}

/// The authority thread; stops (and joins) on drop.
pub(crate) struct Authority {
    sender: AuthoritySender,
    thread: Option<JoinHandle<()>>,
}

impl Authority {
    pub(crate) fn start() -> Self {
        let id = NEXT_AUTHORITY_ID.fetch_add(1, Ordering::Relaxed);
        let (inbox, messages) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("distill-authority".to_owned())
            .spawn(move || {
                CURRENT.set(id);
                run(messages);
            })
            .expect("failed to start the distill authority thread");
        Self {
            sender: AuthoritySender { id, inbox },
            thread: Some(thread),
        }
    }

    pub(crate) fn sender(&self) -> &AuthoritySender {
        &self.sender
    }
}

impl Drop for Authority {
    fn drop(&mut self) {
        let _ = self.sender.inbox.send(Message::Shutdown);
        // The last owner may be a job on the authority itself; it exits
        // after the current message.
        if let Some(thread) = self.thread.take() {
            if !self.sender.on_authority() {
                let _ = thread.join();
            }
        }
    }
}

fn run(messages: mpsc::Receiver<Message>) {
    let mut driver: Option<Box<dyn Driver>> = None;
    loop {
        let message = match driver.as_ref().map(|driver| driver.deadline()) {
            Some(deadline) => {
                match messages.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(message) => Some(message),
                    Err(mpsc::RecvTimeoutError::Timeout) => None,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            None => match messages.recv() {
                Ok(message) => Some(message),
                Err(_) => break,
            },
        };
        match message {
            None => {
                if let Some(active) = driver.as_mut() {
                    if !active.fire() {
                        driver = None;
                    }
                }
            }
            Some(Message::Run(job)) => job(),
            Some(Message::Watch(event)) => {
                if let Some(active) = driver.as_mut() {
                    active.watch(event);
                }
            }
            Some(Message::Attach(factory)) => driver = factory(),
            Some(Message::Detach) => driver = None,
            Some(Message::Shutdown) => break,
        }
    }
}

/// An authority's identity, lent to the thread running work the authority
/// waits on ([`AuthoritySender::lend`]).
pub(crate) struct Lent(u64);

impl Lent {
    /// Act as the lending authority until the guard drops.
    pub(crate) fn enter(self) -> LentGuard {
        LentGuard {
            previous: CURRENT.replace(self.0),
        }
    }
}

pub(crate) struct LentGuard {
    previous: u64,
}

impl Drop for LentGuard {
    fn drop(&mut self) {
        CURRENT.set(self.previous);
    }
}

/// State only its authority touches: a `RefCell` whose owner is the
/// authority thread (or a thread it lent itself to while it waits).
pub(crate) struct AuthorityCell<T> {
    value: std::cell::UnsafeCell<T>,
    authority: u64,
    borrowed: std::sync::atomic::AtomicBool,
}

// SAFETY: the value is reached only through `borrow_mut`, on the authority,
// which is never running while a thread it lent itself to is; the borrow
// flag catches any overlap.
unsafe impl<T: Send> Sync for AuthorityCell<T> {}

impl<T> AuthorityCell<T> {
    pub(crate) fn new(value: T, authority: &AuthoritySender) -> Self {
        Self {
            value: std::cell::UnsafeCell::new(value),
            authority: authority.id,
            borrowed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The value. Panics off the authority or while already borrowed.
    pub(crate) fn borrow_mut(&self) -> AuthorityRef<'_, T> {
        assert!(
            CURRENT.get() == self.authority,
            "authority state is touched only on its authority"
        );
        assert!(
            !self.borrowed.swap(true, Ordering::Acquire),
            "authority state is already borrowed"
        );
        AuthorityRef { cell: self }
    }
}

pub(crate) struct AuthorityRef<'a, T> {
    cell: &'a AuthorityCell<T>,
}

impl<T> std::ops::Deref for AuthorityRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds the only borrow, on the authority.
        unsafe { &*self.cell.value.get() }
    }
}

impl<T> std::ops::DerefMut for AuthorityRef<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as above; `&mut self` makes this the only reference.
        unsafe { &mut *self.cell.value.get() }
    }
}

impl<T> Drop for AuthorityRef<'_, T> {
    fn drop(&mut self) {
        self.cell.borrowed.store(false, Ordering::Release);
    }
}

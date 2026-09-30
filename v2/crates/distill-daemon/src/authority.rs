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

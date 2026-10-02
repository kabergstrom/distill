//! RpcIO's bound on the fetched payload bytes it holds in memory.
//!
//! A fetch learns its payload size from the daemon's answer, before it
//! reads a byte of the payload, and reserves that size against one
//! aggregate budget before it reads the stream. A reservation that does not
//! fit waits, in FIFO order, without reading: the daemon's chunk stream is
//! pulled, so a waiting fetch moves nothing over the wire. A payload larger
//! than the whole budget is admitted once nothing else is resident, so it
//! cannot be starved, and nothing queued behind it overtakes it. Payloads
//! are only ever held in memory; nothing spills to disk.
//!
//! A reservation is released when the loader takes its payload from
//! `LoaderIO::poll` or when the fetch is dropped; releasing admits the
//! waiters that now fit. The engine never waits on admission: only fetch
//! tasks do, and what they wait for is released by `poll` or by other
//! fetches finishing or being dropped.
//!
//! Single-threaded: admission lives on RpcIO's `LocalSet`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// The aggregate memory budget for fetched payloads, and the FIFO queue of
/// fetches waiting for it. Clones share one budget.
#[derive(Debug, Clone)]
pub struct FetchAdmission {
    state: Rc<RefCell<State>>,
}

#[derive(Debug)]
struct State {
    budget: usize,
    resident: usize,
    waiting: VecDeque<Rc<Waiter>>,
}

#[derive(Debug)]
struct Waiter {
    bytes: usize,
    granted: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl FetchAdmission {
    pub fn new(memory_budget: usize) -> Self {
        Self {
            state: Rc::new(RefCell::new(State {
                budget: memory_budget,
                resident: 0,
                waiting: VecDeque::new(),
            })),
        }
    }

    /// Wait for `bytes` to be reserved: at once when nothing waits and they
    /// fit, else once every earlier waiter is admitted and they fit (or
    /// nothing is resident). Dropping the future leaves the queue.
    pub fn admit(&self, bytes: usize) -> Admit {
        let waiter = Rc::new(Waiter {
            bytes,
            granted: Cell::new(false),
            waker: RefCell::new(None),
        });
        self.state.borrow_mut().waiting.push_back(Rc::clone(&waiter));
        self.grant();
        Admit {
            admission: self.clone(),
            waiter: Some(waiter),
        }
    }

    /// Bytes currently reserved.
    pub fn resident(&self) -> usize {
        self.state.borrow().resident
    }

    /// Admissions waiting for memory.
    pub fn waiting(&self) -> usize {
        self.state.borrow().waiting.len()
    }

    fn release(&self, bytes: usize) {
        {
            let mut state = self.state.borrow_mut();
            debug_assert!(bytes <= state.resident, "released more than reserved");
            state.resident = state.resident.saturating_sub(bytes);
        }
        self.grant();
    }

    /// Admit waiters from the front while the front one fits.
    fn grant(&self) {
        let mut woken = Vec::new();
        {
            let mut state = self.state.borrow_mut();
            while let Some(front) = state.waiting.front() {
                let bytes = front.bytes;
                let fits = state.resident == 0
                    || state
                        .resident
                        .checked_add(bytes)
                        .is_some_and(|total| total <= state.budget);
                if !fits {
                    break;
                }
                let front = state.waiting.pop_front().expect("the front waiter exists");
                state.resident = state.resident.saturating_add(bytes);
                front.granted.set(true);
                woken.extend(front.waker.borrow_mut().take());
            }
        }
        // Wake outside the borrow.
        for waker in woken {
            waker.wake();
        }
    }
}

/// A pending admission; resolves to its reservation.
#[derive(Debug)]
#[must_use = "an admission reserves nothing unless awaited"]
pub struct Admit {
    admission: FetchAdmission,
    waiter: Option<Rc<Waiter>>,
}

impl Future for Admit {
    type Output = Reservation;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Reservation> {
        let waiter = self.waiter.as_ref().expect("Admit polled after completion");
        if !waiter.granted.get() {
            *waiter.waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let bytes = waiter.bytes;
        self.waiter = None;
        Poll::Ready(Reservation {
            admission: self.admission.clone(),
            bytes,
        })
    }
}

impl Drop for Admit {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        if waiter.granted.get() {
            // Admitted but never taken: give the bytes back.
            self.admission.release(waiter.bytes);
        } else {
            self.admission
                .state
                .borrow_mut()
                .waiting
                .retain(|queued| !Rc::ptr_eq(queued, &waiter));
            // The waiters behind it may fit now.
            self.admission.grant();
        }
    }
}

/// Payload bytes held in memory against the budget until dropped.
#[derive(Debug)]
pub struct Reservation {
    admission: FetchAdmission,
    bytes: usize,
}

impl Reservation {
    /// Reserve `bytes` more for an admitted payload, without waiting and
    /// even beyond the budget: the bytes are already in memory, and the
    /// overshoot only holds later admissions back until it is released.
    pub fn grow(&mut self, bytes: usize) {
        let mut state = self.admission.state.borrow_mut();
        state.resident = state.resident.saturating_add(bytes);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.admission.release(self.bytes);
    }
}

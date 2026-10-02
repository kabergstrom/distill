use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use distill_loader::{Admit, FetchAdmission, Reservation};

#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// An admission under test, polled by hand.
struct Pending {
    admit: Admit,
    woken: Arc<CountingWaker>,
}

impl Pending {
    fn new(admission: &FetchAdmission, bytes: usize) -> Self {
        Self {
            admit: admission.admit(bytes),
            woken: Arc::default(),
        }
    }

    fn poll(&mut self) -> Option<Reservation> {
        let waker = Waker::from(Arc::clone(&self.woken));
        match Pin::new(&mut self.admit).poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(reservation) => Some(reservation),
            Poll::Pending => None,
        }
    }

    fn woken(&self) -> usize {
        self.woken.0.load(Ordering::SeqCst)
    }
}

fn admitted(admission: &FetchAdmission, bytes: usize) -> Reservation {
    Pending::new(admission, bytes)
        .poll()
        .expect("admitted at once")
}

#[test]
fn payloads_that_fit_are_admitted_at_once_and_released_on_drop() {
    let admission = FetchAdmission::new(100);
    let first = admitted(&admission, 60);
    let second = admitted(&admission, 40);
    assert_eq!(admission.resident(), 100);
    drop(first);
    assert_eq!(admission.resident(), 40);
    drop(second);
    assert_eq!(admission.resident(), 0);
}

#[test]
fn a_spent_budget_defers_admission_until_a_release_wakes_it() {
    let admission = FetchAdmission::new(100);
    let held = admitted(&admission, 60);
    let mut waiting = Pending::new(&admission, 50);
    assert!(waiting.poll().is_none());
    assert_eq!(admission.waiting(), 1);
    assert_eq!(admission.resident(), 60);
    drop(held);
    assert_eq!(waiting.woken(), 1, "the release did not wake the waiter");
    assert_eq!(admission.waiting(), 0);
    let reservation = waiting.poll().expect("admitted after the release");
    assert_eq!(reservation.bytes(), 50);
    assert_eq!(admission.resident(), 50);
}

#[test]
fn a_payload_larger_than_the_budget_is_admitted_alone() {
    let admission = FetchAdmission::new(100);
    let held = admitted(&admission, 10);
    let mut oversized = Pending::new(&admission, 250);
    assert!(oversized.poll().is_none());
    drop(held);
    let oversized = oversized.poll().expect("admitted once nothing is resident");
    assert_eq!(admission.resident(), 250);
    let mut small = Pending::new(&admission, 1);
    assert!(small.poll().is_none(), "admitted beside an oversized payload");
    drop(oversized);
    assert!(small.poll().is_some());

    let empty = FetchAdmission::new(100);
    assert!(Pending::new(&empty, 250).poll().is_some());
}

#[test]
fn admission_is_fifo_so_a_large_waiter_is_not_overtaken_by_small_ones() {
    let admission = FetchAdmission::new(100);
    let first = admitted(&admission, 40);
    let second = admitted(&admission, 40);
    let mut large = Pending::new(&admission, 95);
    let mut small = Pending::new(&admission, 10);
    assert!(large.poll().is_none());
    assert!(small.poll().is_none());
    drop(first);
    // 40 resident: the small one would fit, but the large one is ahead of
    // it and does not fit yet.
    assert!(large.poll().is_none());
    assert!(
        small.poll().is_none(),
        "a later small payload overtook a waiting large one"
    );
    drop(second);
    let large = large.poll().expect("the large payload is admitted first");
    assert_eq!(admission.resident(), 95);
    assert!(small.poll().is_none());
    drop(large);
    assert!(small.poll().is_some());
}

#[test]
fn dropping_a_waiter_leaves_the_queue_and_admits_those_behind_it() {
    let admission = FetchAdmission::new(100);
    let held = admitted(&admission, 50);
    let blocked = Pending::new(&admission, 80);
    let mut behind = Pending::new(&admission, 30);
    assert!(behind.poll().is_none());
    assert_eq!(admission.waiting(), 2);
    drop(blocked);
    assert_eq!(admission.waiting(), 0);
    assert_eq!(behind.woken(), 1, "leaving the queue did not wake the next waiter");
    let behind = behind.poll().expect("admitted once the waiter ahead left");
    assert_eq!(admission.resident(), 80);
    drop((held, behind));
}

#[test]
fn an_admission_granted_but_never_taken_gives_its_bytes_back() {
    let admission = FetchAdmission::new(100);
    let held = admitted(&admission, 100);
    let mut waiting = Pending::new(&admission, 70);
    assert!(waiting.poll().is_none());
    drop(held);
    assert_eq!(admission.resident(), 70);
    drop(waiting);
    assert_eq!(admission.resident(), 0);
}

#[test]
fn growth_never_waits_and_holds_later_admissions_back() {
    let admission = FetchAdmission::new(100);
    let mut reservation = admitted(&admission, 80);
    reservation.grow(50);
    assert_eq!(reservation.bytes(), 130);
    assert_eq!(admission.resident(), 130);
    let mut next = Pending::new(&admission, 10);
    assert!(next.poll().is_none());
    drop(reservation);
    assert_eq!(admission.resident(), 10);
    assert!(next.poll().is_some());
}

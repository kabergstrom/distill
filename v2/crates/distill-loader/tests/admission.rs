use distill_loader::{Admission, FetchAdmission};

#[test]
fn ordinary_fetches_share_the_budget_and_apply_backpressure() {
    let mut gate = FetchAdmission::new(100, 80);
    let first = match gate.admit(60).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(gate.admit(50).unwrap(), Admission::Wait);
    let second = match gate.admit(40).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(gate.used(), 100);
    gate.release(first).unwrap();
    gate.release(second).unwrap();
    assert_eq!((gate.used(), gate.in_flight()), (0, 0));
}

#[test]
fn oversized_fetch_is_admitted_alone_instead_of_deadlocking() {
    let mut gate = FetchAdmission::new(100, 150);
    let ordinary = match gate.admit(1).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(gate.admit(101).unwrap(), Admission::Wait);
    gate.release(ordinary).unwrap();
    let oversized = match gate.admit(101).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    assert!(oversized.is_exclusive());
    assert_eq!(gate.admit(1).unwrap(), Admission::Wait);
    gate.release(oversized).unwrap();
    assert!(matches!(gate.admit(1).unwrap(), Admission::Memory(_)));
}

#[test]
fn payloads_beyond_the_pinned_threshold_are_spooled() {
    let mut gate = FetchAdmission::new(100, 32);
    let permit = match gate.admit(200).unwrap() {
        Admission::Spool(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(permit.bytes(), 200);
    assert!(permit.is_exclusive());
}

#[test]
fn dswl_bytes_resize_the_same_fetch_reservation_and_spool_decision() {
    let mut gate = FetchAdmission::new(100, 80);
    let mut permit = match gate.admit(60).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };

    assert!(gate.resize(&mut permit, 90).unwrap());
    assert_eq!((permit.bytes(), gate.used()), (90, 90));
    assert!(gate.should_spool(90));

    assert!(gate.resize(&mut permit, 140).unwrap());
    assert!(permit.is_exclusive());
    assert_eq!(gate.admit(1).unwrap(), Admission::Wait);
    gate.release(permit).unwrap();
}

#[test]
fn resize_waits_until_other_completed_fetches_release_their_permits() {
    let mut gate = FetchAdmission::new(100, 200);
    let mut first = match gate.admit(40).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };
    let second = match gate.admit(40).unwrap() {
        Admission::Memory(permit) => permit,
        other => panic!("unexpected {other:?}"),
    };

    assert!(!gate.resize(&mut first, 101).unwrap());
    assert_eq!(first.bytes(), 40);
    gate.release(second).unwrap();
    assert!(gate.resize(&mut first, 101).unwrap());
    assert!(first.is_exclusive());
}

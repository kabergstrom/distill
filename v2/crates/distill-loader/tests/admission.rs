use distill_loader::{Admission, FetchAdmission};

#[test]
fn payloads_within_threshold_and_budget_stay_in_memory() {
    let policy = FetchAdmission::new(100, 80);
    assert_eq!(policy.admit(80), Admission::Memory);
    assert_eq!(policy.resident(), 80);
}

#[test]
fn threshold_or_memory_budget_selects_spooling() {
    let threshold = FetchAdmission::new(100, 80);
    assert_eq!(threshold.admit(81), Admission::Spool);
    assert_eq!(threshold.resident(), 0);

    let budget = FetchAdmission::new(100, 150);
    assert_eq!(budget.admit(101), Admission::Spool);
}

#[test]
fn the_budget_is_aggregate_and_a_spent_budget_spools_instead_of_waiting() {
    let policy = FetchAdmission::new(100, 80);
    assert_eq!(policy.admit(60), Admission::Memory);
    assert_eq!(policy.admit(50), Admission::Spool);
    assert_eq!(policy.admit(40), Admission::Memory);
    assert_eq!(policy.resident(), 100);
    policy.release(60);
    assert_eq!(policy.admit(50), Admission::Memory);
    assert_eq!(policy.resident(), 90);
}

#[test]
fn dswl_bytes_participate_in_the_final_spool_decision() {
    let policy = FetchAdmission::new(100, 80);
    assert_eq!(policy.admit(60), Admission::Memory);
    assert!(!policy.grow(60, 30), "60 + 30 exceeds the spool threshold");
    assert!(policy.grow(60, 20));
    assert_eq!(policy.resident(), 80);
}

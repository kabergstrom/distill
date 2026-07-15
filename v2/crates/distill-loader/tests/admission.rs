use distill_loader::{Admission, FetchAdmission};

#[test]
fn single_flight_policy_keeps_bounded_payloads_in_memory() {
    let policy = FetchAdmission::new(100, 80);
    assert_eq!(policy.admit(80), Admission::Memory);
}

#[test]
fn threshold_or_memory_budget_selects_spooling() {
    let threshold = FetchAdmission::new(100, 80);
    assert_eq!(threshold.admit(81), Admission::Spool);

    let budget = FetchAdmission::new(100, 150);
    assert_eq!(budget.admit(101), Admission::Spool);
}

#[test]
fn dswl_bytes_participate_in_the_final_spool_decision() {
    let policy = FetchAdmission::new(100, 80);
    assert_eq!(policy.admit(60), Admission::Memory);
    assert!(policy.should_spool(90));
}

use distill_daemon::scheduler::{Scheduler, SchedulerConfig, SchedulerConfigError, WorkClass};

#[test]
fn scheduler_configuration_enforces_worker_and_reservation_bounds() {
    let invalid_parallelism = SchedulerConfig {
        parallelism: 0,
        batch_reserved_workers: 1,
        max_dependency_depth: 64,
    };
    assert_eq!(
        invalid_parallelism.validate(),
        Err(SchedulerConfigError::ParallelismZero)
    );
    let invalid_reservation = SchedulerConfig {
        parallelism: 4,
        batch_reserved_workers: 4,
        max_dependency_depth: 64,
    };
    assert_eq!(
        invalid_reservation.validate(),
        Err(SchedulerConfigError::BatchReservationOutOfBounds { got: 4, max: 3 })
    );
}

#[test]
fn reserves_batch_capacity_while_other_slots_keep_interactive_priority() {
    let mut scheduler = Scheduler::new(SchedulerConfig {
        parallelism: 4,
        batch_reserved_workers: 1,
        max_dependency_depth: 64,
    })
    .unwrap();
    scheduler.try_enqueue(1, WorkClass::Batch).unwrap();
    scheduler.try_enqueue(2, WorkClass::Batch).unwrap();
    scheduler.try_enqueue(10, WorkClass::Interactive).unwrap();
    scheduler.try_enqueue(11, WorkClass::Interactive).unwrap();
    scheduler.try_enqueue(12, WorkClass::Interactive).unwrap();
    assert_eq!(scheduler.admit(), vec![1, 10, 11, 12]);
}

#[test]
fn one_worker_alternates_classes_without_starvation() {
    let mut scheduler = Scheduler::new(SchedulerConfig {
        parallelism: 1,
        batch_reserved_workers: 1,
        max_dependency_depth: 64,
    })
    .unwrap();
    for id in 1..=3 {
        scheduler.try_enqueue(id, WorkClass::Interactive).unwrap();
        scheduler.try_enqueue(id + 10, WorkClass::Batch).unwrap();
    }
    let mut order = Vec::new();
    for _ in 0..6 {
        let id = scheduler.admit()[0];
        order.push(id);
        scheduler.complete(id).unwrap();
    }
    assert_eq!(order, vec![1, 11, 2, 12, 3, 13]);
}

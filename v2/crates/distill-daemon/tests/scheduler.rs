use distill_daemon::scheduler::{
    run_trampolined, ChainFrame, ChainStep, Scheduler, SchedulerConfig, SchedulerConfigError,
    WorkClass,
};

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
    scheduler.enqueue(1, WorkClass::Batch);
    scheduler.enqueue(2, WorkClass::Batch);
    scheduler.enqueue(10, WorkClass::Interactive);
    scheduler.enqueue(11, WorkClass::Interactive);
    scheduler.enqueue(12, WorkClass::Interactive);
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
        scheduler.enqueue(id, WorkClass::Interactive);
        scheduler.enqueue(id + 10, WorkClass::Batch);
    }
    let mut order = Vec::new();
    for _ in 0..6 {
        let id = scheduler.admit()[0];
        order.push(id);
        scheduler.complete(id).unwrap();
    }
    assert_eq!(order, vec![1, 11, 2, 12, 3, 13]);
}

#[test]
fn live_pool_resize_reclamps_reservation_and_drains_active_excess() {
    let mut scheduler = Scheduler::new(SchedulerConfig {
        parallelism: 4,
        batch_reserved_workers: 3,
        max_dependency_depth: 64,
    })
    .unwrap();
    for id in 1..=5 {
        scheduler.enqueue(id, WorkClass::Batch);
    }
    assert_eq!(scheduler.admit().len(), 4);
    let resize = scheduler.resize_parallelism(2).unwrap();
    assert_eq!(resize.active_slots_to_drain, 2);
    assert_eq!(resize.batch_reserved_workers, 1);
    assert!(scheduler.admit().is_empty());
}

fn deep_frame(remaining: usize) -> ChainFrame<usize> {
    ChainFrame::new(format!("node-{remaining}"), move || {
        if remaining == 0 {
            ChainStep::Complete(0)
        } else {
            ChainStep::Continue(deep_frame(remaining - 1))
        }
    })
}

#[test]
fn trampoline_is_stack_safe_and_depth_cap_is_a_named_scheduler_outcome() {
    assert_eq!(run_trampolined(deep_frame(100_000), 100_001).unwrap(), 0);
    let error = run_trampolined(deep_frame(10), 4).unwrap_err();
    assert_eq!(error.limit, 4);
    assert_eq!(
        error.chain,
        vec!["node-10", "node-9", "node-8", "node-7", "node-6"]
    );
    assert!(!error.memoizable());
}

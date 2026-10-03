use std::collections::BTreeMap;

use distill_build::codegen::{
    generated_module_name, CodegenAttempt, CodegenCoordinator, CodegenFailure, CodegenPublication,
    CodegenPublisher, CodegenSnapshot, GeneratedFile, PublicationError,
};
use distill_build::query::AssetQuery;
use distill_build::trace::{Observed, TraceOp};
use distill_core::id::{AssetUuid, ContentHash};

#[derive(Default)]
struct World {
    basis: u64,
    query: [u8; 32],
    published: Vec<(u64, Vec<GeneratedFile>)>,
}

impl CodegenSnapshot<u64> for World {
    fn current_basis(&self) -> Option<u64> {
        Some(self.basis)
    }

    fn observe(&self, op: &TraceOp) -> bool {
        matches!(op, TraceOp::Query { observed: Observed::Ok(hash), .. } if hash == &self.query)
    }
}

impl CodegenPublisher<u64> for World {
    fn publish(&mut self, basis: &u64, files: &[GeneratedFile]) -> Result<(), PublicationError> {
        self.published.push((*basis, files.to_vec()));
        Ok(())
    }
}

fn query_trace(hash: [u8; 32]) -> Vec<TraceOp> {
    vec![TraceOp::Query {
        query: Box::new(AssetQuery::default()),
        observed: Observed::Ok(hash),
    }]
}

fn file(asset: AssetUuid, bytes: &[u8]) -> GeneratedFile {
    GeneratedFile::new(asset, bytes.to_vec())
}

#[test]
fn names_are_fixed_size_uuid_identities_not_authored_ids() {
    let id = AssetUuid([0xab; 16]);
    assert_eq!(
        generated_module_name(id),
        "sp_abababababababababababababababab"
    );
    assert_eq!(
        file(id, b"x").relative_path(),
        "sp_abababababababababababababababab.rs"
    );
}

#[test]
fn stale_basis_or_newly_discovered_dependency_discards_and_requeues() {
    let mut world = World {
        basis: 7,
        query: [1; 32],
        ..World::default()
    };
    let attempt = CodegenAttempt::success(
        6,
        query_trace([1; 32]),
        vec![file(AssetUuid([1; 16]), b"a")],
    );
    let mut coordinator = CodegenCoordinator::default();
    assert_eq!(
        coordinator.finish(attempt, &mut world),
        CodegenPublication::Requeued
    );
    assert!(world.published.is_empty());

    let attempt = CodegenAttempt::success(
        7,
        query_trace([2; 32]),
        vec![file(AssetUuid([1; 16]), b"a")],
    );
    assert_eq!(
        coordinator.finish(attempt, &mut world),
        CodegenPublication::Requeued
    );
    assert!(world.published.is_empty());
    assert!(!coordinator.holds(&world), "a requeued attempt installs nothing");
}

#[test]
fn complete_batch_and_trace_publish_together_after_immediate_revalidation() {
    let mut world = World {
        basis: 9,
        query: [3; 32],
        ..World::default()
    };
    let files = vec![
        file(AssetUuid([2; 16]), b"b"),
        file(AssetUuid([1; 16]), b"a"),
    ];
    let attempt = CodegenAttempt::success(9, query_trace([3; 32]), files);
    let mut coordinator = CodegenCoordinator::default();

    let published = coordinator.finish(attempt, &mut world);
    assert!(matches!(published, CodegenPublication::Published { .. }));
    assert_eq!(world.published.len(), 1);
    assert!(world.published[0].1[0].relative_path() < world.published[0].1[1].relative_path());
    assert!(coordinator.holds(&world));
    world.query = [4; 32];
    assert!(!coordinator.holds(&world));
}

#[test]
fn duplicate_namespace_claims_fail_before_any_write() {
    let mut world = World {
        basis: 1,
        query: [4; 32],
        ..World::default()
    };
    let id = AssetUuid([5; 16]);
    let attempt = CodegenAttempt::success(
        1,
        query_trace([4; 32]),
        vec![file(id, b"first"), file(id, b"second")],
    );
    let mut coordinator = CodegenCoordinator::default();

    assert!(matches!(
        coordinator.finish(attempt, &mut world),
        CodegenPublication::Failed(CodegenFailure::NamespaceCollision { .. })
    ));
    assert!(world.published.is_empty());
    assert!(!coordinator.holds(&world));
}

#[test]
fn typed_generation_failure_is_published_only_if_its_basis_stays_current() {
    let mut world = World {
        basis: 2,
        query: [7; 32],
        ..World::default()
    };
    let mut coordinator = CodegenCoordinator::default();
    let failure = CodegenFailure::Generation("shader reflection failed".into());
    let attempt = CodegenAttempt::failure(2, query_trace([7; 32]), failure.clone());

    assert_eq!(
        coordinator.finish(attempt, &mut world),
        CodegenPublication::Failed(failure.clone())
    );
    assert!(coordinator.holds(&world));
}

#[test]
fn publication_error_does_not_install_trace_or_partial_state() {
    struct Failing(World);
    impl CodegenSnapshot<u64> for Failing {
        fn current_basis(&self) -> Option<u64> {
            Some(self.0.basis)
        }
        fn observe(&self, op: &TraceOp) -> bool {
            self.0.observe(op)
        }
    }
    impl CodegenPublisher<u64> for Failing {
        fn publish(&mut self, _: &u64, _: &[GeneratedFile]) -> Result<(), PublicationError> {
            Err(PublicationError::new("publication conflict"))
        }
    }

    let mut world = Failing(World {
        basis: 3,
        query: [8; 32],
        ..World::default()
    });
    let mut coordinator = CodegenCoordinator::default();
    let attempt = CodegenAttempt::success(
        3,
        query_trace([8; 32]),
        vec![file(AssetUuid([8; 16]), b"x")],
    );
    assert!(matches!(
        coordinator.finish(attempt, &mut world),
        CodegenPublication::PublicationFailed(_)
    ));
    assert!(!coordinator.holds(&world));
}

#[test]
fn file_content_identity_is_exposed_for_diff_and_journal_preimages() {
    let f = file(AssetUuid([9; 16]), b"same bytes");
    assert_eq!(
        f.content_hash(),
        ContentHash(*blake3::hash(b"same bytes").as_bytes())
    );
    assert_eq!(
        BTreeMap::from([(f.relative_path().to_owned(), f.content_hash())]).len(),
        1
    );
}

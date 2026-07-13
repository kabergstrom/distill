use std::collections::BTreeMap;

use distill_build::query::AssetQuery;
use distill_build::trace::*;
use distill_core::id::{AssetUuid, ContentHash, TypeUuid};

#[derive(Default)]
struct Snapshot {
    reads: BTreeMap<AssetUuid, Observed<ContentHash>>,
    resolves: BTreeMap<String, Observed<Option<AssetUuid>>>,
    roles: BTreeMap<AssetUuid, Observed<Option<EntryRole>>>,
}

impl TraceSource for Snapshot {
    fn read(&self, asset: AssetUuid) -> Observed<ContentHash> {
        self.reads[&asset].clone()
    }
    fn resolve(&self, path: &str) -> Observed<Option<AssetUuid>> {
        self.resolves[path].clone()
    }
    fn query(&self, _: &AssetQuery) -> Observed<[u8; 32]> {
        Observed::Ok([3; 32])
    }
    fn tool(&self, _: &str) -> Observed<[u8; 32]> {
        Observed::Ok([4; 32])
    }
    fn capability(&self, _: &CapabilityKey) -> Observed<[u8; 32]> {
        Observed::Ok([5; 32])
    }
    fn ref_check(&self, _: AssetUuid, _: TypeUuid) -> Observed<Option<TypeUuid>> {
        Observed::Ok(None)
    }
    fn role_check(&self, asset: AssetUuid) -> Observed<Option<EntryRole>> {
        self.roles
            .get(&asset)
            .cloned()
            .unwrap_or(Observed::Ok(None))
    }
}

#[test]
fn revalidation_compares_labeled_outcomes_in_order() {
    let asset = AssetUuid([1; 16]);
    let hash = ContentHash([2; 32]);
    let trace = vec![
        TraceOp::Resolve {
            path: "a".into(),
            observed: Observed::Ok(None),
        },
        TraceOp::Read {
            asset,
            observed: Observed::Ok(hash),
        },
    ];
    let mut snapshot = Snapshot::default();
    snapshot.resolves.insert("a".into(), Observed::Ok(None));
    snapshot.reads.insert(asset, Observed::Ok(hash));
    assert!(revalidate(&trace, &snapshot));
    snapshot
        .resolves
        .insert("a".into(), Observed::Ok(Some(asset)));
    assert!(!revalidate(&trace, &snapshot));
    assert_ne!(
        trace_digest(&trace),
        trace_digest(&[trace[1].clone(), trace[0].clone()])
    );
}

#[test]
fn stable_failure_heals_when_observed_outcome_changes() {
    let asset = AssetUuid([8; 16]);
    let missing = StableFailureFingerprint::MissingCapability {
        key: CapabilityKey::Processor {
            input: TypeUuid([9; 16]),
        },
    };
    let trace = vec![TraceOp::Read {
        asset,
        observed: Observed::Err(missing.clone()),
    }];
    let mut snapshot = Snapshot::default();
    snapshot.reads.insert(asset, Observed::Err(missing));
    assert!(revalidate(&trace, &snapshot));
    snapshot
        .reads
        .insert(asset, Observed::Ok(ContentHash([1; 32])));
    assert!(!revalidate(&trace, &snapshot));
}

#[test]
fn failure_cause_grammar_is_checked() {
    let local = StableFailureFingerprint::Local {
        class: LocalFailureClass::Validator,
        detail: [7; 32],
    };
    assert!(FailureRecord {
        trace: vec![],
        cause: FailureCause::Local(local)
    }
    .validate()
    .is_ok());
    assert!(FailureRecord {
        trace: vec![],
        cause: FailureCause::Op
    }
    .validate()
    .is_err());
    let op = TraceOp::Tool {
        id: "shaderc".into(),
        observed: Observed::Err(StableFailureFingerprint::ToolLaunch {
            id: "shaderc".into(),
            class: ToolErrorClass::NotExecutable,
        }),
    };
    assert!(FailureRecord {
        trace: vec![op],
        cause: FailureCause::Op
    }
    .validate()
    .is_ok());
}

#[test]
fn local_failure_details_are_dslf_domain_separated_typed_facts() {
    let a = local_failure_fingerprint(LocalFailureClass::Validator, |e| {
        e.u16(4);
        e.str("field");
    });
    let b = local_failure_fingerprint(LocalFailureClass::Validator, |e| {
        e.u16(4);
        e.str("field");
    });
    let changed = local_failure_fingerprint(LocalFailureClass::Validator, |e| {
        e.u16(5);
        e.str("field");
    });
    assert_eq!(a, b);
    assert_ne!(a, changed);
    let StableFailureFingerprint::Local { detail, .. } = a else {
        unreachable!()
    };
    assert_ne!(detail, *blake3::hash(b"field").as_bytes());
}

#[test]
fn authoring_only_role_failures_are_distinct_from_missing_and_heal_on_role_change() {
    let asset = AssetUuid([12; 16]);
    let failure = StableFailureFingerprint::RoleIneligible {
        asset,
        observed_role: EntryRole::AuthoringOnly,
    };
    let trace = vec![TraceOp::RoleCheck {
        asset,
        observed: Observed::Err(failure.clone()),
    }];
    let mut snapshot = Snapshot::default();
    snapshot.roles.insert(asset, Observed::Err(failure.clone()));
    assert!(revalidate(&trace, &snapshot));

    snapshot
        .roles
        .insert(asset, Observed::Ok(Some(EntryRole::Runtime)));
    assert!(!revalidate(&trace, &snapshot));
    assert_ne!(
        trace_digest(&trace),
        trace_digest(&[TraceOp::RoleCheck {
            asset,
            observed: Observed::Ok(None),
        }]),
        "role-ineligible is never encoded as an ordinary miss"
    );
}

#[test]
fn every_dslf_local_failure_class_has_a_stable_nonzero_code() {
    let classes = [
        LocalFailureClass::Validator,
        LocalFailureClass::MigrationPlan,
        LocalFailureClass::Processor,
        LocalFailureClass::MigrationFunction,
        LocalFailureClass::OutputBinding,
        LocalFailureClass::Importer,
        LocalFailureClass::ImportIntake,
        LocalFailureClass::ArtifactEncoding,
    ];
    assert_eq!(classes.map(|class| class as u16), [1, 2, 3, 4, 5, 6, 7, 8]);
}

use std::sync::Arc;

use distill_core::id::TypeUuid;
use distill_loader::basis::digest_rows;
use distill_loader::{
    IoBasis, LoadPolicyAttestation, LoadPolicyError, LoadPolicyRow, ManifestHash,
};

fn row(id: u8, build_only: bool) -> LoadPolicyRow {
    LoadPolicyRow {
        type_uuid: TypeUuid([id; 16]),
        build_only,
    }
}

#[test]
fn attestation_has_one_sorted_canonical_encoding() {
    let attestation = LoadPolicyAttestation::from_rows(vec![row(2, true), row(1, false)]).unwrap();
    assert_eq!(attestation.rows(), &[row(1, false), row(2, true)]);
    assert_eq!(attestation.digest(), digest_rows(attestation.rows()));

    assert_eq!(
        LoadPolicyAttestation::try_from_parts(vec![row(2, true), row(1, false)], [0; 32]),
        Err(LoadPolicyError::Unsorted)
    );
    assert!(matches!(
        LoadPolicyAttestation::try_from_parts(vec![row(1, false), row(1, true)], [0; 32]),
        Err(LoadPolicyError::Duplicate(_))
    ));
    assert_eq!(
        LoadPolicyAttestation::try_from_parts(vec![row(1, false)], [0; 32]),
        Err(LoadPolicyError::DigestMismatch)
    );
}

#[test]
fn runtime_closure_rejects_missing_and_build_only_types() {
    let attestation = LoadPolicyAttestation::from_rows(vec![row(1, false), row(2, true)]).unwrap();
    assert_eq!(attestation.require_runtime(TypeUuid([1; 16])), Ok(()));
    assert_eq!(
        attestation.require_runtime(TypeUuid([2; 16])),
        Err(LoadPolicyError::BuildOnly(TypeUuid([2; 16])))
    );
    assert_eq!(
        attestation.require_runtime(TypeUuid([3; 16])),
        Err(LoadPolicyError::MissingType(TypeUuid([3; 16])))
    );
}

#[test]
fn every_basis_carries_the_verified_projection() {
    let policy = Arc::new(LoadPolicyAttestation::from_rows(vec![row(1, false)]).unwrap());
    let basis = IoBasis::Pack {
        manifest: ManifestHash([9; 32]),
        load_policy: policy.clone(),
    };
    assert_eq!(basis.load_policy().digest(), policy.digest());
    assert_eq!(basis.rpc_snapshot(), None);
}

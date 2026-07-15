mod common;

use common::{leak, nfield, noption, nstruct, scalar};
use distill_wire::native::{validate_native_descriptor, CtorId, NativeLayoutNode, ScalarKind};

#[test]
fn accepts_exact_local_geometry_and_valid_table_ids() {
    let node = noption(0, 8, 8, scalar(0, ScalarKind::U32), 0);
    validate_native_descriptor(&node, 8, 8, 1, 0, 0).unwrap();
}

#[test]
fn rejects_root_allocation_drift_and_missing_callback_entries() {
    let scalar_node = scalar(0, ScalarKind::U32);
    assert!(validate_native_descriptor(&scalar_node, 8, 4, 0, 0, 0).is_err());

    let option = NativeLayoutNode::Option {
        offset: 0,
        size: 8,
        align: 8,
        inner: leak(scalar(0, ScalarKind::U32)),
        ctor: CtorId(3),
    };
    assert!(validate_native_descriptor(&option, 8, 8, 1, 0, 0).is_err());
}

#[test]
fn rejects_overlapping_non_zst_fields() {
    let node = nstruct(
        0,
        4,
        4,
        vec![
            nfield("a", 0, scalar(0, ScalarKind::U32)),
            nfield("b", 1, scalar(0, ScalarKind::U16)),
        ],
    );
    assert!(validate_native_descriptor(&node, 4, 4, 0, 0, 0).is_err());
}

#[test]
fn option_is_a_recursive_backref_frame() {
    let node = noption(
        0,
        8,
        8,
        NativeLayoutNode::BackRef {
            distance: 0,
            offset: 0,
        },
        0,
    );
    validate_native_descriptor(&node, 8, 8, 1, 0, 0).unwrap();
}

mod common;

use distill_bundle::{
    validate_entry_lineage_authority, AcceptedSchemaEpoch, EntryLineageV1, LineageStamp,
};
use distill_core::lineage::lineage_chain_digest;

#[test]
fn non_bootstrap_entry_requires_the_exact_accepted_manifest_prefix() {
    let schema = common::simple_schema();
    let mut bundle = common::bundle(
        &[&schema],
        vec![(
            "a",
            common::entry(
                common::UUID_A,
                &schema,
                common::obj(&[("count", common::u(1)), ("name", common::s("entry"))]),
            ),
        )],
        Some("a"),
    );
    let entry = bundle.assets.get_mut("a").unwrap();
    let first = entry.schema_hash;
    let accepted = vec![
        AcceptedSchemaEpoch {
            digest: first,
            forward_parent: None,
        },
        AcceptedSchemaEpoch {
            digest: distill_core::id::LogicalHash([9; 32]),
            forward_parent: Some(0),
        },
    ];
    entry.lineage = EntryLineageV1::Manifest(LineageStamp {
        epochs: accepted[..1].to_vec(),
        cursor: 0,
        chain: lineage_chain_digest(entry.type_uuid, &accepted[..1], 0),
    });
    let schema = &bundle.schemas[&entry.schema_hash];
    validate_entry_lineage_authority("a", bundle.format_version, entry, schema, Some(&accepted))
        .unwrap();
    assert!(
        validate_entry_lineage_authority("a", bundle.format_version, entry, schema, None).is_err()
    );

    let divergent = [AcceptedSchemaEpoch {
        digest: distill_core::id::LogicalHash([8; 32]),
        forward_parent: None,
    }];
    assert!(validate_entry_lineage_authority(
        "a",
        bundle.format_version,
        entry,
        schema,
        Some(&divergent),
    )
    .is_err());
}

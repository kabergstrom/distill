//! The daemon's live registry (§5, §11): current logical schema + hash per
//! stable type UUID, plus the archived-snapshot accelerator.

use distill_core::id::TypeUuid;
use distill_schema::SchemaRegistry;
use ngp_schema::build::{field, SchemaBuilder};
use ngp_schema::{
    logical_hash, node_hash, project, ExtractionError, PointerKind, PrimitiveType, ASSET_REF_UUID,
};

fn uuid(n: u8) -> TypeUuid {
    let mut b = [0u8; 16];
    b[15] = n;
    b[6] = 0x40; // version bits, cosmetic
    TypeUuid(b)
}

#[test]
fn registers_every_uuid_carrying_type_with_its_projection() {
    let mut b = SchemaBuilder::new();
    let u32_t = b.primitive(PrimitiveType::U32);
    let s_t = b.string();
    let player = b.struct_type(
        "game",
        "Player",
        vec![field("hp", u32_t), field("name", s_t)],
    );
    let enemy = b.struct_type("game", "Enemy", vec![field("hp", u32_t)]);
    let helper = b.struct_type("game", "NoUuidHelper", vec![field("x", u32_t)]);
    b.def_mut(player).uuid = Some(uuid(1));
    b.def_mut(enemy).uuid = Some(uuid(2));
    let schema = b.finish();

    let reg = SchemaRegistry::from_schema(&schema).unwrap();

    let (snap, hash) = reg.current(uuid(1)).expect("Player registered");
    assert_eq!(snap, &project(&schema, player).unwrap());
    assert_eq!(hash, logical_hash(&schema, player).unwrap());

    let (_, enemy_hash) = reg.current(uuid(2)).expect("Enemy registered");
    assert_eq!(enemy_hash, logical_hash(&schema, enemy).unwrap());
    assert_ne!(hash, enemy_hash);

    // No UUID → not addressable through the registry.
    let _ = helper;
    assert!(reg.current(uuid(3)).is_none());
}

#[test]
fn framework_uuids_never_register() {
    let mut b = SchemaBuilder::new();
    let u32_t = b.primitive(PrimitiveType::U32);
    let target = b.struct_type("game", "Tex", vec![field("w", u32_t)]);
    b.def_mut(target).uuid = Some(uuid(9));
    let r = b.asset_ref(target);
    let holder = b.struct_type("game", "Mat", vec![field("tex", r)]);
    b.def_mut(holder).uuid = Some(uuid(4));
    let schema = b.finish();

    let reg = SchemaRegistry::from_schema(&schema).unwrap();
    assert!(reg.current(uuid(4)).is_some());
    assert!(reg.current(uuid(9)).is_some());
    // The AssetRef struct itself carries the framework UUID but never
    // projects standalone (§5).
    assert!(reg.current(ASSET_REF_UUID).is_none());
}

#[test]
fn duplicate_uuid_names_both_types() {
    let mut b = SchemaBuilder::new();
    let u32_t = b.primitive(PrimitiveType::U32);
    let a = b.struct_type("game", "A", vec![field("x", u32_t)]);
    let c = b.struct_type("game", "B", vec![field("x", u32_t)]);
    b.def_mut(a).uuid = Some(uuid(7));
    b.def_mut(c).uuid = Some(uuid(7));
    let schema = b.finish();

    let err = SchemaRegistry::from_schema(&schema).unwrap_err();
    let ExtractionError::DuplicateTypeUuid {
        uuid: u,
        first,
        second,
    } = err
    else {
        panic!("{err:?}");
    };
    assert_eq!(u, uuid(7));
    assert!(first.contains("A"), "{first}");
    assert!(second.contains("B"), "{second}");
}

#[test]
fn unserializable_uuid_type_fails_construction() {
    let mut b = SchemaBuilder::new();
    let ptr = b.opaque(
        PrimitiveType::Pointer(PointerKind::MutPointer),
        "core",
        "ptr",
    );
    let bad = b.struct_type("game", "Bad", vec![field("p", ptr)]);
    b.def_mut(bad).uuid = Some(uuid(5));
    let schema = b.finish();

    assert!(matches!(
        SchemaRegistry::from_schema(&schema),
        Err(ExtractionError::Unserializable { .. })
    ));
}

#[test]
fn archive_stores_by_own_hash_and_serves_lookups() {
    let mut b = SchemaBuilder::new();
    let u32_t = b.primitive(PrimitiveType::U32);
    let t = b.struct_type("game", "T", vec![field("x", u32_t)]);
    let schema = b.finish();
    let snap = project(&schema, t).unwrap();
    let expected = node_hash(&snap.root).unwrap();

    let mut reg = SchemaRegistry::from_schema(&schema).unwrap();
    assert!(reg.archived(expected).is_none());

    let h = reg.archive(snap.clone()).unwrap();
    assert_eq!(h, expected);
    assert_eq!(reg.archived(h), Some(&snap));

    // Idempotent: archiving the same snapshot again is a no-op.
    assert_eq!(reg.archive(snap.clone()).unwrap(), h);
    assert_eq!(reg.archived(h), Some(&snap));
}

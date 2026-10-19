//! TRANSITION (newgameplus GAP 23 §18.5, §18.6 step 1): the interpreter
//! against the `#[asset]`-generated code for every Distill test asset type:
//! descriptor, logical schema, native layout, tables, defaults, default
//! values, and a recursive value encoded, fixed up through both descriptors
//! and read back through both. Goes with the macro (step 3).

use std::collections::HashMap;
use std::mem::MaybeUninit;

use distill_asset::reflect::compare::{compare_type, compare_value};
use distill_asset::reflect::interp::TypeInfo;
use distill_asset::{AssetDefaults, AssetReflect, AssetType};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_test_assets::recursion::{Child, Kind, Node, Tree};
use distill_test_assets::{defaults as d, loader as l, recursion as r, runtime as rt, semantic as s};
use distill_wire::encode::encode_value;
use distill_wire::exec::{execute_fixup, ExecEnv, ExecLimits};
use distill_wire::plan::{compile_plans, PlanId};

type Check = (TypeUuid, &'static str, fn(&TypeInfo) -> Vec<String>);

fn check<T: AssetType + AssetReflect + AssetDefaults>() -> Check {
    (T::TYPE_UUID, std::any::type_name::<T>(), compare_type::<T>)
}

fn check_default<T: AssetType + AssetReflect + AssetDefaults + Default>() -> Check {
    (T::TYPE_UUID, std::any::type_name::<T>(), |info| {
        let mut out = compare_type::<T>(info);
        out.extend(compare_value(info, &T::default()));
        out
    })
}

#[test]
fn every_test_asset_type_agrees_with_the_generated_code() {
    let checks = [
        check::<d::Outer>(),
        check::<d::Inner>(),
        check_default::<d::Mode>(),
        check::<d::OldOuter>(),
        check::<d::OldInner>(),
        check::<d::OldMode>(),
        check::<l::A>(),
        check::<l::B>(),
        check::<l::BuildOnly>(),
        check::<l::Counted>(),
        check::<l::RpcA>(),
        check::<l::PackA>(),
        check::<l::PanickingAssetDrop>(),
        check::<l::ProjectRow>(),
        check_default::<r::Tree>(),
        check_default::<r::Node>(),
        check_default::<r::Kind>(),
        check_default::<r::Child>(),
        check_default::<r::Recursive>(),
        check::<rt::Counted>(),
        check::<rt::Plain>(),
        check::<rt::PanickingDrop>(),
        check_default::<s::Example>(),
        check_default::<s::Inner>(),
        check_default::<s::Nested>(),
        check::<s::SameShapeA>(),
        check::<s::SameShapeB>(),
        check::<s::RenamedShape>(),
        check::<s::RevisedShape>(),
        check::<s::ReferenceTarget>(),
        check::<s::SemanticFacts>(),
        check_default::<s::Level>(),
    ];
    let mut by_uuid: HashMap<TypeUuid, Check> = checks.into_iter().map(|c| (c.0, c)).collect();
    let mut problems = Vec::new();
    let mut n = 0;
    for info in distill_test_assets::NGP_ASSET_TABLE.asset_types() {
        let (_, _, compare) = by_uuid
            .remove(&info.descriptor.type_uuid)
            .unwrap_or_else(|| panic!("{:?} has no check", info.descriptor.type_uuid));
        problems.extend(compare(info));
        n += 1;
    }
    assert!(by_uuid.is_empty(), "not in the table: {:?}", by_uuid.values().map(|c| c.1).collect::<Vec<_>>());
    assert!(problems.is_empty(), "{} differences:\n{}", problems.len(), problems.join("\n"));
    println!("{n} test asset types agree");
}

fn leaf(id: u32) -> Node {
    Node { kind: Kind::Leaf { id }, gain: id * 10 }
}

fn child(min: u32, node: Node, alt: Option<Node>) -> Child {
    Child { min, node, alt: alt.map(Box::new) }
}

fn sample() -> Tree {
    Tree {
        root: Node {
            kind: Kind::Switch {
                children: vec![
                    child(0, Node { kind: Kind::Many { children: vec![leaf(1), leaf(2)] }, gain: 3 }, Some(leaf(4))),
                    child(5, Node { kind: Kind::Switch { children: vec![child(6, leaf(7), None)] }, gain: 8 }, None),
                ],
            },
            gain: 9,
        },
        release: Some(Node { kind: Kind::Many { children: vec![leaf(10)] }, gain: 11 }),
    }
}

/// A type re-entered inline through a record, through variant payloads and
/// through `Option<Box<_>>`, from the table's schema: wire, encode, fixup
/// through both descriptors, read back through both.
#[test]
fn a_recursive_value_round_trips_through_both() {
    let table = &distill_test_assets::NGP_ASSET_TABLE;
    let info = table.by_uuid(Tree::TYPE_UUID).unwrap();
    let wire = distill_wire::derive::derive_wire(table.schema().single_layout().unwrap(), info.id).unwrap();
    let tree = sample();
    assert!(compare_value(info, &tree).is_none());
    let mut no_references = |_: &ngp_schema::AssetRefValue,
                             _: TypeUuid,
                             _: bool,
                             _: &[distill_bundle::PathComponent]|
     -> Result<AssetUuid, String> { Err("no references".into()) };
    let encoded = encode_value(&info.reflect.logical.root, &wire, &tree.to_value(), &mut no_references).unwrap();
    for descriptor in [Tree::descriptor(), &info.descriptor] {
        let plans = compile_plans(&wire, descriptor.native_layout).unwrap();
        let env = ExecEnv {
            ctors: descriptor.ctors,
            drops: descriptor.drops,
            skips: descriptor.skip_writers,
            blobs: &[],
            limits: ExecLimits::default(),
        };
        let mut out = MaybeUninit::<Tree>::uninit();
        let got = unsafe {
            execute_fixup(&plans, PlanId(0), &encoded.fixed, &encoded.variable, &env, out.as_mut_ptr().cast()).unwrap();
            out.assume_init()
        };
        assert_eq!(got, tree);
        assert_eq!(unsafe { info.reflect.to_value((&got as *const Tree).cast()) }, tree.to_value());
    }
}

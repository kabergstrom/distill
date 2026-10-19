use distill_asset::{to_value, AssetType};
use ngp_schema::Value;
use ngp_migrate::{
    conforms, execute_ops, plan_automatic, validate_plan, DefaultProvider, FieldPath,
};
use distill_pipeline_api::asset_defaults::AssetTableDefaults;
use distill_pipeline_api::callbacks::PipelineDefaults;
use ngp_schema::SchemaNode;

use distill_test_assets::defaults::{Inner, Mode, OldInner, OldMode, OldOuter, Outer};

fn schema<T: AssetType>() -> SchemaNode {
    distill_asset::logical::<T>().root.clone()
}

struct Provider<'a>(&'a dyn PipelineDefaults);

impl DefaultProvider for Provider<'_> {
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value> {
        self.0.field_default(to_schema, at)
    }

    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<Value> {
        self.0.parent_default(to_schema, at)
    }
}

#[test]
fn automatic_migration_fills_added_fields_from_the_generated_table() {
    let old = schema::<OldOuter>();
    let new = schema::<Outer>();
    let value = to_value(&OldOuter {
        items: vec![OldInner {
            a: 3,
            name: "x".into(),
        }],
        mode: OldMode::On { level: 2 },
    });

    let plan = plan_automatic(&old, &new).unwrap();
    validate_plan(&plan, &old, &new).unwrap();
    let defaults = AssetTableDefaults::of::<Outer>();
    let migrated = execute_ops(&plan, &value, &old, &new, &Provider(&defaults)).unwrap();

    conforms(&migrated.value, &new).unwrap();
    let expected = to_value(&Outer {
        items: vec![Inner {
            a: 3,
            added: 0,
            name: "x".into(),
        }],
        mode: Mode::On { level: 2, extra: 0 },
    });
    assert_eq!(migrated.value, expected);
}

#[test]
fn field_and_parent_defaults_resolve_nested_containers() {
    let defaults = AssetTableDefaults::of::<Outer>();
    let inner = schema::<Inner>();
    let added = FieldPath::of(&["added"]);

    assert_eq!(
        defaults.field_default(&inner, &added),
        Some(Value::U32(0))
    );
    assert_eq!(
        defaults.parent_default(&inner, &added),
        Some(Value::U32(5))
    );

    let SchemaNode::Enum { variants, .. } = schema::<Mode>() else {
        panic!("Mode is an enum");
    };
    let on = &variants.iter().find(|(name, _, _)| name == "On").unwrap().2;
    assert_eq!(
        defaults.field_default(on, &FieldPath::of(&["extra"])),
        Some(Value::U32(0))
    );

    let outer = schema::<Outer>();
    assert_eq!(
        defaults.field_default(&outer, &FieldPath::of(&["missing"])),
        None
    );
    assert_eq!(defaults.field_default(&outer, &FieldPath::root()), None);
}

use distill_asset::build::logical_bytes;
use distill_asset::AssetReflect;
use distill_json::AuthoredValue;
use distill_migrate::{
    conforms, execute_ops, plan_automatic, validate_plan, DefaultProvider, EdgeKind, FieldPath,
};
use distill_pipeline_api::asset_defaults::AssetTableDefaults;
use distill_pipeline_api::callbacks::PipelineDefaults;
use ngp_schema::{node_from_bytes, SchemaNode};

#[distill_asset::asset(uuid = "a5d00001-0000-4000-8000-000000000000")]
struct Outer {
    items: Vec<Inner>,
    mode: Mode,
}

#[distill_asset::asset(uuid = "a5d00002-0000-4000-8000-000000000000")]
struct Inner {
    a: u32,
    added: u32,
    name: String,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            a: 1,
            added: 5,
            name: "inner".into(),
        }
    }
}

#[derive(Default)]
#[distill_asset::asset(uuid = "a5d00003-0000-4000-8000-000000000000")]
#[repr(u8)]
enum Mode {
    #[default]
    Off,
    On {
        level: u32,
        extra: u32,
    },
}

// The same types before `added` and `extra` existed. Logical nodes carry no
// type names, so these project to the old shapes of the types above.
#[distill_asset::asset(uuid = "a5d00011-0000-4000-8000-000000000000")]
struct OldOuter {
    items: Vec<OldInner>,
    mode: OldMode,
}

#[distill_asset::asset(uuid = "a5d00012-0000-4000-8000-000000000000")]
struct OldInner {
    a: u32,
    name: String,
}

#[distill_asset::asset(uuid = "a5d00013-0000-4000-8000-000000000000")]
#[repr(u8)]
enum OldMode {
    #[allow(dead_code)]
    Off,
    On {
        level: u32,
    },
}

fn schema<T: AssetReflect>() -> SchemaNode {
    node_from_bytes(&logical_bytes::<T>()).unwrap().root
}

struct Provider<'a>(&'a dyn PipelineDefaults);

impl DefaultProvider for Provider<'_> {
    fn field_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        self.0.field_default(to_schema, at)
    }

    fn parent_default(&self, to_schema: &SchemaNode, at: &FieldPath) -> Option<AuthoredValue> {
        self.0.parent_default(to_schema, at)
    }
}

#[test]
fn automatic_migration_fills_added_fields_from_the_generated_table() {
    let old = schema::<OldOuter>();
    let new = schema::<Outer>();
    let value = OldOuter {
        items: vec![OldInner {
            a: 3,
            name: "x".into(),
        }],
        mode: OldMode::On { level: 2 },
    }
    .to_authored();

    let plan = plan_automatic(&old, &new).unwrap();
    validate_plan(&plan, &old, &new, EdgeKind::Automatic).unwrap();
    let defaults = AssetTableDefaults::<Outer>::new();
    let migrated = execute_ops(&plan, &value, &old, &new, &Provider(&defaults)).unwrap();

    conforms(&migrated.value, &new).unwrap();
    let expected = Outer {
        items: vec![Inner {
            a: 3,
            added: 0,
            name: "x".into(),
        }],
        mode: Mode::On { level: 2, extra: 0 },
    }
    .to_authored();
    assert_eq!(migrated.value, expected);
}

#[test]
fn field_and_parent_defaults_resolve_nested_containers() {
    let defaults = AssetTableDefaults::<Outer>::new();
    let inner = schema::<Inner>();
    let added = FieldPath::of(&["added"]);

    assert_eq!(
        defaults.field_default(&inner, &added),
        Some(AuthoredValue::UInt(0))
    );
    assert_eq!(
        defaults.parent_default(&inner, &added),
        Some(AuthoredValue::UInt(5))
    );

    let SchemaNode::Enum { variants, .. } = schema::<Mode>() else {
        panic!("Mode is an enum");
    };
    let on = &variants.iter().find(|(name, _, _)| name == "On").unwrap().2;
    assert_eq!(
        defaults.field_default(on, &FieldPath::of(&["extra"])),
        Some(AuthoredValue::UInt(0))
    );

    let outer = schema::<Outer>();
    assert_eq!(
        defaults.field_default(&outer, &FieldPath::of(&["missing"])),
        None
    );
    assert_eq!(defaults.field_default(&outer, &FieldPath::root()), None);
}

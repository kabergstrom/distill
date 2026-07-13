use std::collections::BTreeSet;

use distill_build::outputs::OutputDecls;
use distill_build::pipeline::*;
use distill_core::id::TypeUuid;

fn set<T: Ord>(v: impl IntoIterator<Item = T>) -> BTreeSet<T> {
    v.into_iter().collect()
}
fn ty(n: u8) -> TypeUuid {
    TypeUuid([n; 16])
}

fn reg(id: &str, input: u8, output: u8, selector: TargetSelector) -> ProcessorRegistration {
    ProcessorRegistration::new(
        id,
        1,
        ty(input),
        selector,
        OutputDecls::new(ty(output), vec![]).unwrap(),
        [id.as_bytes()[0]; 32],
    )
    .unwrap()
}

#[test]
fn selector_requires_coverage_and_overlap_is_rejected() {
    let vk = GraphicsApi::new("vulkan").unwrap();
    let gl = GraphicsApi::new("opengl").unwrap();
    let selector = TargetSelector::new(
        Some(set([TargetOs::Linux])),
        Some(set([vk.clone(), gl.clone()])),
    )
    .unwrap();
    let target = Target::new(TargetOs::Linux, set([vk.clone(), gl.clone()])).unwrap();
    assert!(selector.matches(&target));
    let partial = TargetSelector::new(None, Some(set([vk]))).unwrap();
    assert!(!partial.matches(&target));

    let a = reg("a", 1, 2, selector);
    let b = reg(
        "b",
        1,
        3,
        TargetSelector::new(Some(set([TargetOs::Linux])), None).unwrap(),
    );
    assert!(PipelineRegistry::new(vec![a, b]).is_err());
}

#[test]
fn chains_are_type_changing_and_detect_cycles_and_duplicate_extra_keys() {
    let all = TargetSelector::new(None, None).unwrap();
    let mut a = reg("a", 1, 2, all.clone());
    a.outputs = OutputDecls::new(ty(2), vec![("meta".into(), ty(9))]).unwrap();
    let mut b = reg("b", 2, 3, all.clone());
    b.outputs = OutputDecls::new(ty(3), vec![("other".into(), ty(8))]).unwrap();
    let registry = PipelineRegistry::new(vec![a, b]).unwrap();
    let target = Target::new(TargetOs::Linux, set([GraphicsApi::new("vulkan").unwrap()])).unwrap();
    let chain = registry.chain(ty(1), &target).unwrap();
    assert_eq!(chain.terminal, ty(3));
    assert_eq!(chain.stages.len(), 2);
    assert_eq!(chain.extras.len(), 2);

    let cycle =
        PipelineRegistry::new(vec![reg("a", 1, 2, all.clone()), reg("b", 2, 1, all)]).unwrap();
    assert!(matches!(
        cycle.chain(ty(1), &target),
        Err(PipelineError::Cycle { .. })
    ));
}

#[test]
fn terminal_and_extra_sets_must_be_target_invariant() {
    let vk = GraphicsApi::new("vulkan").unwrap();
    let gl = GraphicsApi::new("opengl").unwrap();
    let linux = Some(set([TargetOs::Linux]));
    let a = reg(
        "vk",
        1,
        2,
        TargetSelector::new(linux.clone(), Some(set([vk.clone()]))).unwrap(),
    );
    let b = reg(
        "gl",
        1,
        3,
        TargetSelector::new(linux, Some(set([gl.clone()]))).unwrap(),
    );
    let registry = PipelineRegistry::new(vec![a, b]).unwrap();
    let targets = [
        Target::new(TargetOs::Linux, set([vk])).unwrap(),
        Target::new(TargetOs::Linux, set([gl])).unwrap(),
    ];
    assert!(matches!(
        registry.validate_target_invariance(&[ty(1)], &targets),
        Err(PipelineError::TargetVariantInterface { .. })
    ));
}

#[test]
fn present_empty_selector_and_target_api_sets_are_rejected() {
    assert!(TargetSelector::new(None, Some(BTreeSet::new())).is_err());
    assert!(Target::new(TargetOs::Linux, BTreeSet::new()).is_err());
}

//! Snapshot-published projection of the target-resolved processor map.
//!
//! This is deliberately callback-free.  Filesystem and authoring
//! publications need terminal types and the complete derived namespace even
//! when no lazy build has ever executed.

use std::collections::{BTreeMap, BTreeSet};

use distill_build::pipeline::{PipelineError, PipelineRegistry, ProcessorRegistration, Target};
use distill_core::id::{AssetUuid, TypeUuid};
use distill_rpc::DerivedOutputEntry;

use crate::callbacks::ProcessorDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PipelineInterface {
    pub(crate) terminal: TypeUuid,
    pub(crate) extras: BTreeMap<String, TypeUuid>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PipelineProjection {
    interfaces: BTreeMap<TypeUuid, PipelineInterface>,
}

impl PipelineProjection {
    pub(crate) fn build(
        descriptors: Vec<ProcessorDescriptor>,
        dylib_hash: [u8; 32],
        targets: &BTreeMap<String, Target>,
        authored_types: impl IntoIterator<Item = TypeUuid>,
    ) -> Result<Self, PipelineError> {
        let registry = PipelineRegistry::new(
            descriptors
                .into_iter()
                .map(|descriptor| {
                    ProcessorRegistration::new(
                        &descriptor.id,
                        descriptor.version,
                        descriptor.input,
                        descriptor.selector,
                        descriptor.outputs,
                        dylib_hash,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        let authored_types = authored_types.into_iter().collect::<BTreeSet<_>>();
        let target_values = targets.values().cloned().collect::<Vec<_>>();
        registry.validate_target_invariance(
            &authored_types.iter().copied().collect::<Vec<_>>(),
            &target_values,
        )?;

        let mut interfaces = BTreeMap::new();
        for authored in authored_types {
            let interface = match target_values.first() {
                Some(target) => {
                    let chain = registry.chain(authored, target)?;
                    PipelineInterface {
                        terminal: chain.terminal,
                        extras: chain.extras,
                    }
                }
                None => PipelineInterface {
                    terminal: authored,
                    extras: BTreeMap::new(),
                },
            };
            interfaces.insert(authored, interface);
        }
        Ok(Self { interfaces })
    }

    pub(crate) fn interface(&self, authored: TypeUuid) -> PipelineInterface {
        self.interfaces
            .get(&authored)
            .cloned()
            .unwrap_or(PipelineInterface {
                terminal: authored,
                extras: BTreeMap::new(),
            })
    }

    pub(crate) fn derived_outputs(
        &self,
        assets: impl IntoIterator<Item = (AssetUuid, TypeUuid)>,
    ) -> BTreeMap<AssetUuid, DerivedOutputEntry> {
        assets
            .into_iter()
            .flat_map(|(parent, authored_type)| {
                self.interface(authored_type).extras.into_iter().map(
                    move |(output_key, terminal_type)| {
                        (
                            AssetUuid::v5(parent, &output_key),
                            DerivedOutputEntry {
                                parent,
                                output_key,
                                terminal_type,
                            },
                        )
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_build::outputs::OutputDecls;
    use distill_build::pipeline::{GraphicsApi, TargetArch, TargetOs, TargetSelector};
    use distill_schema::ngp_schema::LayoutIdentity;

    const SOURCE: TypeUuid = TypeUuid([1; 16]);
    const TERMINAL: TypeUuid = TypeUuid([2; 16]);
    const EXTRA: TypeUuid = TypeUuid([3; 16]);

    fn test_layout_identity() -> LayoutIdentity {
        LayoutIdentity {
            target_triple: "x86_64-unknown-linux-gnu".into(),
            rustc: "rustc test".into(),
            algorithm_version: 1,
        }
    }

    fn target(os: TargetOs) -> Target {
        Target::new(
            os,
            TargetArch::X86_64,
            BTreeSet::from([GraphicsApi::new("vulkan").unwrap()]),
            false,
            true,
            test_layout_identity(),
        )
        .unwrap()
    }

    fn descriptor(selector: TargetSelector, primary: TypeUuid) -> ProcessorDescriptor {
        ProcessorDescriptor {
            id: format!("processor_{}", primary.0[0]),
            version: 1,
            input: SOURCE,
            selector,
            outputs: OutputDecls::new(primary, vec![("reflection".to_owned(), EXTRA)]).unwrap(),
        }
    }

    #[test]
    fn projects_terminal_and_static_extra_interface_without_running_a_build() {
        let targets = BTreeMap::from([
            ("linux".to_owned(), target(TargetOs::Linux)),
            ("windows".to_owned(), target(TargetOs::Windows)),
        ]);
        let projection = PipelineProjection::build(
            vec![descriptor(
                TargetSelector::new(None, None).unwrap(),
                TERMINAL,
            )],
            [9; 32],
            &targets,
            [SOURCE],
        )
        .unwrap();
        assert_eq!(
            projection.interface(SOURCE),
            PipelineInterface {
                terminal: TERMINAL,
                extras: BTreeMap::from([("reflection".to_owned(), EXTRA)]),
            }
        );
        let parent = AssetUuid([7; 16]);
        let child = AssetUuid::v5(parent, "reflection");
        assert_eq!(
            projection.derived_outputs([(parent, SOURCE)]),
            BTreeMap::from([(
                child,
                DerivedOutputEntry {
                    parent,
                    output_key: "reflection".to_owned(),
                    terminal_type: EXTRA,
                },
            )])
        );
    }

    #[test]
    fn rejects_target_variant_terminal_or_extra_interfaces() {
        let targets = BTreeMap::from([
            ("linux".to_owned(), target(TargetOs::Linux)),
            ("windows".to_owned(), target(TargetOs::Windows)),
        ]);
        let linux = TargetSelector::new(Some(BTreeSet::from([TargetOs::Linux])), None).unwrap();
        let windows = TargetSelector::new(Some(BTreeSet::from([TargetOs::Windows])), None).unwrap();
        let error = PipelineProjection::build(
            vec![
                descriptor(linux, TERMINAL),
                descriptor(windows, TypeUuid([4; 16])),
            ],
            [9; 32],
            &targets,
            [SOURCE],
        )
        .unwrap_err();
        assert_eq!(
            error,
            PipelineError::TargetVariantInterface { authored: SOURCE }
        );
    }
}

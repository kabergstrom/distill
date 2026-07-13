use std::path::PathBuf;

use distill_daemon::policy::{
    validate_candidate_linkage, CodeLoadRequest, CodeLoadingPolicy, Linkage, NativeDependency,
    PolicyError,
};

#[test]
fn pipeline_native_dependencies_are_static_and_runtime_dlopen_is_banned() {
    assert!(validate_candidate_linkage(&[NativeDependency {
        name: "shaderc".into(),
        linkage: Linkage::Static,
    }])
    .is_ok());
    assert!(validate_candidate_linkage(&[NativeDependency {
        name: "shaderc".into(),
        linkage: Linkage::RuntimeDynamic,
    }])
    .is_err());
    assert_eq!(
        CodeLoadingPolicy::authorize(CodeLoadRequest::PipelineDlopen {
            path: PathBuf::from("libshaderc.so")
        }),
        Err(PolicyError::PipelineDlopenBanned)
    );
}

#[test]
fn only_the_host_staged_module_and_hashed_tool_subprocess_are_dynamic_boundaries() {
    assert!(
        CodeLoadingPolicy::authorize(CodeLoadRequest::HostPipelineModule {
            staged_copy: true,
            content_hash: Some([1; 32]),
        })
        .is_ok()
    );
    assert!(
        CodeLoadingPolicy::authorize(CodeLoadRequest::ToolSubprocess {
            staged_copy: true,
            content_hash: Some([2; 32]),
        })
        .is_ok()
    );
    assert!(
        CodeLoadingPolicy::authorize(CodeLoadRequest::ToolSubprocess {
            staged_copy: false,
            content_hash: Some([2; 32]),
        })
        .is_err()
    );
}

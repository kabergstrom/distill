use std::path::PathBuf;

use distill_daemon::policy::{
    validate_candidate_linkage, validate_native_library_names, validate_pipeline_image_linkage,
    CodeLoadRequest, CodeLoadingPolicy, Linkage, NativeDependency, PolicyError,
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
fn native_image_inspection_rejects_non_system_runtime_dependencies() {
    assert!(validate_native_library_names(["/usr/lib/libSystem.B.dylib"]).is_ok());
    assert!(validate_native_library_names(["libc.so.6", "libgcc_s.so.1"]).is_ok());
    assert!(validate_native_library_names(["KERNEL32.dll", "ucrtbase.dll"]).is_ok());

    assert_eq!(
        validate_native_library_names(["@rpath/libshaderc.dylib"]),
        Err(PolicyError::RuntimeDynamicDependency {
            name: "@rpath/libshaderc.dylib".to_owned(),
        })
    );
    assert!(matches!(
        validate_native_library_names(["libspirv-cross.so"]),
        Err(PolicyError::RuntimeDynamicDependency { .. })
    ));
}

#[test]
fn native_image_inspection_reads_the_actual_dependency_table() {
    let executable = std::env::current_exe().unwrap();
    let bytes = std::fs::read(executable).unwrap();
    validate_pipeline_image_linkage(&bytes).unwrap();

    assert!(matches!(
        validate_pipeline_image_linkage(b"not a native image"),
        Err(PolicyError::InvalidNativeImage { .. })
    ));
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

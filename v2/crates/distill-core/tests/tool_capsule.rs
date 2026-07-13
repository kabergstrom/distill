use distill_core::tool::{
    ToolCapsuleError, ToolCapsuleFile, ToolCapsuleFileRole, ToolCwdPolicy, ToolExecutionCapsuleV1,
    ToolLaunchMetadataV1, ToolPlatformBinding,
};

fn file(path: &str, role: ToolCapsuleFileRole, byte: u8) -> ToolCapsuleFile {
    ToolCapsuleFile {
        path: path.into(),
        role,
        executable: matches!(
            role,
            ToolCapsuleFileRole::Launcher | ToolCapsuleFileRole::Interpreter
        ),
        len: 1,
        bytes_hash: [byte; 32],
    }
}

fn valid() -> ToolExecutionCapsuleV1 {
    ToolExecutionCapsuleV1 {
        files: vec![
            file("bin/interpreter", ToolCapsuleFileRole::Interpreter, 1),
            file("bin/tool", ToolCapsuleFileRole::Launcher, 2),
            file("lib/plugin", ToolCapsuleFileRole::Plugin, 3),
            file("share/config", ToolCapsuleFileRole::DeclaredResource, 4),
        ],
        resolved_interpreter: Some("bin/interpreter".into()),
        launch: ToolLaunchMetadataV1 {
            argv0: "bin/tool".into(),
            interpreter_args: vec!["--script".into()],
        },
        environment: vec![
            ("LANG".into(), "C.UTF-8".into()),
            ("MODE".into(), "strict".into()),
        ],
        cwd_policy: ToolCwdPolicy::ReadOnlyDeclaredSubdir("share".into()),
        platform: ToolPlatformBinding::Pinned {
            platform_id: "linux-x86_64".into(),
            system_runtime_id: "glibc-2.39".into(),
        },
    }
}

#[test]
fn dsct_record_and_digest_are_pinned_and_strictly_decoded() {
    let capsule = valid();
    let record = capsule.encode_record().unwrap();
    assert_eq!(record[0], 1, "record version");
    assert_eq!(&record[1..5], &4_u32.to_le_bytes(), "file count");
    assert_eq!(
        record[5..20],
        [15, 0, 0, 0, b'b', b'i', b'n', b'/', b'i', b'n', b't', b'e', b'r', b'p', b'r']
    );
    assert_eq!(
        capsule.digest().unwrap(),
        [
            0x9f, 0xf5, 0x60, 0x53, 0x83, 0xf7, 0x41, 0xa2, 0xa7, 0xeb, 0xad, 0x96, 0x18, 0xfa,
            0x94, 0x9e, 0x51, 0x8c, 0x49, 0x48, 0xbe, 0xfb, 0x3b, 0x09, 0xe6, 0xd5, 0x21, 0x8b,
            0xa4, 0xad, 0xb9, 0x01,
        ]
    );
    assert_eq!(
        ToolExecutionCapsuleV1::decode_record(&record).unwrap(),
        capsule
    );

    let mut trailing = record.clone();
    trailing.push(0);
    assert_eq!(
        ToolExecutionCapsuleV1::decode_record(&trailing),
        Err(ToolCapsuleError::TrailingBytes)
    );
    let mut unknown_role = record;
    let first_role = 5 + 4 + "bin/interpreter".len();
    unknown_role[first_role] = 99;
    assert_eq!(
        ToolExecutionCapsuleV1::decode_record(&unknown_role),
        Err(ToolCapsuleError::UnknownFileRole(99))
    );
}

#[test]
fn capsule_rejects_nonhermetic_or_noncanonical_metadata() {
    let mut capsule = valid();
    capsule.files[0].path = "../escape".into();
    assert_eq!(
        capsule.validate(),
        Err(ToolCapsuleError::InvalidCapsulePath)
    );

    let mut capsule = valid();
    capsule.files.swap(0, 1);
    assert_eq!(capsule.validate(), Err(ToolCapsuleError::FilesNotCanonical));

    let mut capsule = valid();
    capsule.environment.swap(0, 1);
    assert_eq!(
        capsule.validate(),
        Err(ToolCapsuleError::EnvironmentNotCanonical)
    );

    let mut capsule = valid();
    capsule.files.remove(0);
    assert_eq!(
        capsule.validate(),
        Err(ToolCapsuleError::InterpreterMismatch)
    );

    let mut capsule = valid();
    capsule.platform = ToolPlatformBinding::ExplicitResidual {
        platform_id: "linux-x86_64".into(),
        system_runtime_class: "current".into(),
    };
    assert_eq!(capsule.validate(), Err(ToolCapsuleError::InvalidPlatform));
}

#[test]
fn capsule_digest_covers_every_execution_input() {
    let base = valid().digest().unwrap();
    let mut variants = Vec::new();

    let mut changed = valid();
    changed.files[3].bytes_hash[0] ^= 1;
    variants.push(changed);
    let mut changed = valid();
    changed
        .launch
        .interpreter_args
        .push("--deterministic".into());
    variants.push(changed);
    let mut changed = valid();
    changed.environment[0].1 = "C".into();
    variants.push(changed);
    let mut changed = valid();
    changed.cwd_policy = ToolCwdPolicy::EmptyScratch;
    variants.push(changed);
    let mut changed = valid();
    changed.platform = ToolPlatformBinding::ExplicitResidual {
        platform_id: "linux-x86_64".into(),
        system_runtime_class: "kernel-abi".into(),
    };
    variants.push(changed);

    for variant in variants {
        assert_ne!(variant.digest().unwrap(), base);
    }
}

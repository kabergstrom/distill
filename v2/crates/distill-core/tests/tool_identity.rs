use distill_core::tool::{
    ToolCwdPolicy, ToolExecutionIdentityV2, ToolIdentityError, ToolPackageFile,
    ToolSourceIdentityV2,
};

fn file(path: &str, executable: bool, byte: u8) -> ToolPackageFile {
    ToolPackageFile {
        path: path.to_owned(),
        executable,
        len: 1,
        bytes_hash: [byte; 32],
    }
}

fn package() -> ToolExecutionIdentityV2 {
    ToolExecutionIdentityV2 {
        source: ToolSourceIdentityV2::Package {
            launcher: "bin/tool".to_owned(),
            files: vec![file("bin/tool", true, 1), file("share/config", false, 2)],
        },
        environment: vec![("LANG".to_owned(), "C".to_owned())],
        cwd_policy: ToolCwdPolicy::ReadOnlyPackageSubdir("share".to_owned()),
    }
}

fn ambient(fingerprint: Option<[u8; 32]>) -> ToolExecutionIdentityV2 {
    ToolExecutionIdentityV2 {
        source: ToolSourceIdentityV2::Ambient {
            launcher: if cfg!(windows) {
                r"C:\toolchains\dxc.exe"
            } else {
                "/opt/toolchains/dxc"
            }
            .to_owned(),
            toolchain_id: "windows-sdk-dxc".to_owned(),
            trusted_fingerprint: fingerprint,
        },
        environment: Vec::new(),
        cwd_policy: ToolCwdPolicy::EmptyScratch,
    }
}

#[test]
fn package_identity_roundtrips_under_dsct_v2() {
    let identity = package();
    let record = identity.encode_record().unwrap();

    assert_eq!(record[0], 2);
    assert_eq!(
        ToolExecutionIdentityV2::decode_record(&record).unwrap(),
        identity
    );
    assert_eq!(identity.digest().unwrap(), identity.digest().unwrap());

    let mut trailing = record;
    trailing.push(0);
    assert_eq!(
        ToolExecutionIdentityV2::decode_record(&trailing),
        Err(ToolIdentityError::TrailingBytes)
    );
}

#[test]
fn package_paths_order_launcher_and_cwd_are_closed() {
    let mut identity = package();
    let ToolSourceIdentityV2::Package { files, .. } = &mut identity.source else {
        unreachable!();
    };
    files[1].path = "../escape".to_owned();
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::InvalidPackagePath)
    );

    let mut identity = package();
    let ToolSourceIdentityV2::Package { files, .. } = &mut identity.source else {
        unreachable!();
    };
    files.swap(0, 1);
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::FilesNotCanonical)
    );

    let mut identity = package();
    let ToolSourceIdentityV2::Package { launcher, .. } = &mut identity.source else {
        unreachable!();
    };
    *launcher = "share/config".to_owned();
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::LauncherMismatch)
    );

    let mut identity = package();
    identity.cwd_policy = ToolCwdPolicy::ReadOnlyPackageSubdir("missing".to_owned());
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::InvalidCwdPolicy)
    );
}

#[test]
fn environment_is_canonical_and_part_of_identity() {
    let mut identity = package();
    identity.environment = vec![
        ("Z".to_owned(), "1".to_owned()),
        ("A".to_owned(), "2".to_owned()),
    ];
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::EnvironmentNotCanonical)
    );

    let mut changed = package();
    changed.environment[0].1 = "POSIX".to_owned();
    assert_ne!(package().digest().unwrap(), changed.digest().unwrap());
}

#[test]
fn ambient_cacheability_requires_a_trusted_fingerprint() {
    let untrusted = ambient(None);
    let trusted = ambient(Some([7; 32]));

    assert!(!untrusted.is_cacheable());
    assert!(trusted.is_cacheable());
    assert_ne!(untrusted.digest().unwrap(), trusted.digest().unwrap());
    assert_eq!(
        ToolExecutionIdentityV2::decode_record(&trusted.encode_record().unwrap()).unwrap(),
        trusted
    );
}

#[test]
fn ambient_identity_rejects_relative_paths_empty_ids_and_package_cwd() {
    let mut identity = ambient(None);
    let ToolSourceIdentityV2::Ambient { launcher, .. } = &mut identity.source else {
        unreachable!();
    };
    *launcher = "bin/tool".to_owned();
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::InvalidAmbientLauncher)
    );

    let mut identity = ambient(None);
    let ToolSourceIdentityV2::Ambient { toolchain_id, .. } = &mut identity.source else {
        unreachable!();
    };
    toolchain_id.clear();
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::InvalidToolchainId)
    );

    let mut identity = ambient(None);
    identity.cwd_policy = ToolCwdPolicy::ReadOnlyPackageRoot;
    assert_eq!(
        identity.validate(),
        Err(ToolIdentityError::InvalidCwdPolicy)
    );
}

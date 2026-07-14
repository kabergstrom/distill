//! Sole bundle-format-v1 bootstrap generator and sealed consumer authority.
//!
//! Generation can produce untrusted artifacts from explicit inputs.  Runtime
//! authority is a different, unconstructible brand: it is issued only after
//! the literal DSCI-keyed resource has matched the literal consumer identity,
//! the checked-in DSB, and the five concrete Rust descriptors in this crate.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use distill_core::attestation::{
    validate_bootstrap_authority, BootstrapAuthorityMismatch, BootstrapControlSpecV1,
    BootstrapControlTableV1, BundleFormatVersion, CompiledAttestationDigest, CompiledTypeRow,
    BOOTSTRAP_CONTROL_COUNT, BOOTSTRAP_CONTROL_SPEC_V1_BYTES,
};
use distill_core::canonical::CanonicalEncoder;
use distill_core::id::TypeUuid;
use distill_wire::dsnl::{dsnl_bytes, DSNL_VERSION};
use distill_wire::native::NativeLayoutNode;
use ngp_schema::identity::{identity_digest, CompilationIdentity};
use unicode_normalization::UnicodeNormalization;

use crate::bootstrap_builtins_v1::{
    generated_bootstrap_control_spec_v1, locally_compiled_bootstrap_rows_v1,
    locally_measured_bootstrap_layouts_v1,
};

pub const BOOTSTRAP_GENERATOR_ALGORITHM_V1: u8 = 1;
pub const BOOTSTRAP_TABLE_VERSION_V1: u8 = 1;

// A literal, target-native fixture generated on the checked-in toolchain.
// Per-target fixtures use different DSCI keys; no runtime fallback exists.
pub const EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1: &str =
    "bootstrap/control-table-v1/561e2b65badcee65aed0ab7cf72e8475e86e0baf291b3c6b4383a31cc7c13f64.dsca";
const EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_V1: &[u8] = include_bytes!(
    "../bootstrap/control-table-v1/561e2b65badcee65aed0ab7cf72e8475e86e0baf291b3c6b4383a31cc7c13f64.dsca"
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapGenError(pub String);

impl std::fmt::Display for BootstrapGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BootstrapGenError {}

/// An explicit generator input row.  This is not an authority carrier.
#[derive(Debug, Clone, Copy)]
pub struct BootstrapMeasuredLayoutV1<'a> {
    pub type_uuid: TypeUuid,
    pub native_layout: &'a NativeLayoutNode,
}

/// Decoded exact generator input.  Its DSNL bytes remain untrusted input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBootstrapGeneratorInputV1<'a> {
    identity: CompilationIdentity,
    measured_layouts: Vec<DecodedMeasuredLayoutV1<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedMeasuredLayoutV1<'a> {
    type_uuid: TypeUuid,
    dsnl: &'a [u8],
}

impl DecodedBootstrapGeneratorInputV1<'_> {
    pub fn compilation_identity(&self) -> &CompilationIdentity {
        &self.identity
    }

    pub fn measured_layouts(&self) -> impl ExactSizeIterator<Item = (TypeUuid, &[u8])> {
        self.measured_layouts
            .iter()
            .map(|row| (row.type_uuid, row.dsnl))
    }
}

/// A generated or decoded resource artifact.  Private fields keep this type
/// from being confused with [`ConsumerBootstrapAuthorityV1`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTableArtifactV1 {
    compilation_identity: CompilationIdentity,
    identity_digest: [u8; 32],
    table: BootstrapControlTableV1,
    dsca: CompiledAttestationDigest,
    bytes: Vec<u8>,
    resource_name: String,
}

impl BootstrapTableArtifactV1 {
    pub fn compilation_identity(&self) -> &CompilationIdentity {
        &self.compilation_identity
    }

    pub fn identity_digest(&self) -> [u8; 32] {
        self.identity_digest
    }

    pub fn table(&self) -> &BootstrapControlTableV1 {
        &self.table
    }

    pub fn rows(&self) -> &[CompiledTypeRow; BOOTSTRAP_CONTROL_COUNT] {
        self.table.rows()
    }

    pub fn dsca(&self) -> CompiledAttestationDigest {
        self.dsca
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }
}

/// Sealed proof that the current consumer's literal resource equals its five
/// locally compiled built-ins.  There is intentionally no public constructor,
/// `Default`, decoder, or conversion from a canonical table/artifact.
#[derive(Debug)]
pub struct ConsumerBootstrapAuthorityV1 {
    artifact: BootstrapTableArtifactV1,
}

impl ConsumerBootstrapAuthorityV1 {
    pub fn compilation_identity(&self) -> &CompilationIdentity {
        self.artifact.compilation_identity()
    }

    pub fn identity_digest(&self) -> [u8; 32] {
        self.artifact.identity_digest()
    }

    pub fn table(&self) -> &BootstrapControlTableV1 {
        self.artifact.table()
    }

    pub fn rows(&self) -> &[CompiledTypeRow; BOOTSTRAP_CONTROL_COUNT] {
        self.artifact.rows()
    }

    pub fn dsca(&self) -> CompiledAttestationDigest {
        self.artifact.dsca()
    }

    pub fn resource_name(&self) -> &str {
        self.artifact.resource_name()
    }

    pub fn validate_boundary_rows(
        &self,
        compiled: &[CompiledTypeRow],
        format: BundleFormatVersion,
    ) -> Result<(), BootstrapAuthorityMismatch> {
        validate_bootstrap_authority(compiled, self.table(), format)
    }
}

/// Sole public acquisition path for the consumer authority brand.
pub fn consumer_bootstrap_authority_v1(
) -> Result<&'static ConsumerBootstrapAuthorityV1, BootstrapGenError> {
    static AUTHORITY: OnceLock<Result<ConsumerBootstrapAuthorityV1, BootstrapGenError>> =
        OnceLock::new();
    match AUTHORITY.get_or_init(build_consumer_bootstrap_authority_v1) {
        Ok(authority) => Ok(authority),
        Err(error) => Err(error.clone()),
    }
}

/// The literal DSCI used by this checked-in target-native fixture.
pub fn consumer_compilation_identity_v1() -> &'static CompilationIdentity {
    static IDENTITY: OnceLock<CompilationIdentity> = OnceLock::new();
    IDENTITY.get_or_init(|| CompilationIdentity {
        target_triple: "aarch64-apple-darwin".to_owned(),
        rustc: concat!(
            "rustc 1.90.0 (1159e78c4 2025-09-14)\n",
            "binary: rustc\n",
            "commit-hash: 1159e78c4747b02ef996e55082b704c09b970588\n",
            "commit-date: 2025-09-14\n",
            "host: aarch64-apple-darwin\n",
            "release: 1.90.0\n",
            "LLVM version: 20.1.8"
        )
        .to_owned(),
        source_fingerprint: decode_hex_32(
            "b096d53c63c3bb239c4c39e8b86568234f7fc8b65f00cb0e37800fcd3cdf089d",
        ),
        features: BTreeSet::from([(
            "distill-schema".to_owned(),
            "bootstrap-control-v1".to_owned(),
        )]),
        cfgs: BTreeSet::from([
            "target_arch=\"aarch64\"".to_owned(),
            "target_endian=\"little\"".to_owned(),
            "target_env=\"\"".to_owned(),
            "target_os=\"macos\"".to_owned(),
            "target_pointer_width=\"64\"".to_owned(),
            "target_vendor=\"apple\"".to_owned(),
        ]),
        manifest_lock_hash: decode_hex_32(
            "d81e4399693f1b2bc61737cf10f52687f09252aa02f6d27877ff2c065748c7fe",
        ),
        algorithm_version: 1,
    })
}

pub fn canonical_compilation_identity_bytes(
    identity: &CompilationIdentity,
) -> Result<Vec<u8>, BootstrapGenError> {
    validate_compilation_identity(identity)?;
    let mut encoder = CanonicalEncoder::new();
    encoder.str(&identity.target_triple);
    encoder.str(&identity.rustc);
    encoder.raw(&identity.source_fingerprint);
    encoder.set(identity.features.iter(), |encoder, (package, feature)| {
        encoder.str(package);
        encoder.str(feature);
    });
    encoder.set(identity.cfgs.iter(), |encoder, cfg| encoder.str(cfg));
    encoder.raw(&identity.manifest_lock_hash);
    encoder.u32(identity.algorithm_version);
    Ok(encoder.into_bytes())
}

/// Encode the exact three-input generator record.  The returned bytes are not
/// an authority; only the sealed local verification path can issue that brand.
pub fn encode_generator_input_v1(
    identity: &CompilationIdentity,
    measured_layouts: &[BootstrapMeasuredLayoutV1<'_>],
) -> Result<Vec<u8>, BootstrapGenError> {
    let layouts = canonical_layouts(measured_layouts)?;
    let identity_bytes = canonical_compilation_identity_bytes(identity)?;
    let mut out = vec![BOOTSTRAP_GENERATOR_ALGORITHM_V1];
    put_bytes(&mut out, BOOTSTRAP_CONTROL_SPEC_V1_BYTES)?;
    put_bytes(&mut out, &identity_bytes)?;
    put_count(&mut out, layouts.len())?;
    for layout in layouts {
        out.extend_from_slice(&layout.type_uuid.0);
        out.extend_from_slice(
            &dsnl_bytes(layout.native_layout)
                .map_err(|error| BootstrapGenError(format!("invalid DSNL tree: {error}")))?,
        );
    }
    Ok(out)
}

pub fn local_generator_input_bytes_v1() -> Result<Vec<u8>, BootstrapGenError> {
    let local = locally_measured_bootstrap_layouts_v1();
    let layouts = local
        .iter()
        .map(|(type_uuid, native_layout)| BootstrapMeasuredLayoutV1 {
            type_uuid: *type_uuid,
            native_layout,
        })
        .collect::<Vec<_>>();
    encode_generator_input_v1(consumer_compilation_identity_v1(), &layouts)
}

/// Exact inverse of [`encode_generator_input_v1`], including a recursive
/// canonical DSNL parser and byte-for-byte DSB/DSCI re-encoding checks.
pub fn decode_generator_input_v1(
    bytes: &[u8],
) -> Result<DecodedBootstrapGeneratorInputV1<'_>, BootstrapGenError> {
    let mut reader = Reader::new(bytes, "bootstrap generator input");
    let algorithm = reader.u8()?;
    if algorithm != BOOTSTRAP_GENERATOR_ALGORITHM_V1 {
        return Err(BootstrapGenError(format!(
            "unsupported bootstrap generator algorithm {algorithm}"
        )));
    }
    if reader.bytes()? != BOOTSTRAP_CONTROL_SPEC_V1_BYTES {
        return Err(BootstrapGenError(
            "generator input does not contain the literal DSB".to_owned(),
        ));
    }
    let identity_bytes = reader.bytes()?;
    let identity = decode_compilation_identity(identity_bytes)?;
    if canonical_compilation_identity_bytes(&identity)? != identity_bytes {
        return Err(BootstrapGenError(
            "noncanonical generator CompilationIdentity".to_owned(),
        ));
    }
    let count = reader.u32()? as usize;
    if count != BOOTSTRAP_CONTROL_COUNT {
        return Err(BootstrapGenError(format!(
            "measured bootstrap row count is {count}, expected {BOOTSTRAP_CONTROL_COUNT}"
        )));
    }
    let expected = BootstrapControlSpecV1::embedded()
        .map_err(|error| BootstrapGenError(format!("invalid embedded DSB v1: {error}")))?;
    let mut measured_layouts = Vec::with_capacity(count);
    for expected_row in &expected.0 {
        let type_uuid = TypeUuid(reader.a16()?);
        if type_uuid != expected_row.type_uuid {
            return Err(BootstrapGenError(format!(
                "generator DSNL row {}, expected {}",
                type_uuid, expected_row.type_uuid
            )));
        }
        let start = reader.position;
        parse_dsnl_node(&mut reader, 0)?;
        measured_layouts.push(DecodedMeasuredLayoutV1 {
            type_uuid,
            dsnl: &bytes[start..reader.position],
        });
    }
    reader.finish()?;
    Ok(DecodedBootstrapGeneratorInputV1 {
        identity,
        measured_layouts,
    })
}

/// Deterministically generate an unbranded table artifact from the exact
/// generator record.
pub fn generate_bootstrap_table_artifact_v1(
    input: &[u8],
) -> Result<BootstrapTableArtifactV1, BootstrapGenError> {
    let decoded = decode_generator_input_v1(input)?;
    let spec = BootstrapControlSpecV1::embedded()
        .map_err(|error| BootstrapGenError(format!("invalid embedded DSB v1: {error}")))?;
    let rows = spec
        .0
        .iter()
        .zip(&decoded.measured_layouts)
        .map(|(schema, layout)| {
            CompiledTypeRow::new(
                schema.type_uuid,
                schema.logical_hash,
                dsnl_digest(layout.dsnl),
                true,
                schema.registry_extras.clone(),
            )
            .map_err(|error| BootstrapGenError(format!("invalid bootstrap row: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    encode_table_artifact(decoded.identity, rows)
}

/// Generate twice and check every source/resource/measurement binding.  This
/// is the implementation behind `distill-bootstrap-gen check`.
pub fn check_bootstrap_generation_v1() -> Result<(), BootstrapGenError> {
    if consumer_compilation_identity_v1().source_fingerprint != local_source_fingerprint_v1() {
        return Err(BootstrapGenError(
            "literal DSCI source fingerprint is stale".to_owned(),
        ));
    }
    if consumer_compilation_identity_v1().manifest_lock_hash != local_manifest_lock_hash_v1() {
        return Err(BootstrapGenError(
            "literal DSCI manifest/lock fingerprint is stale".to_owned(),
        ));
    }
    let generated_spec = generated_bootstrap_control_spec_v1()
        .map_err(BootstrapGenError)?
        .encode()
        .map_err(|error| BootstrapGenError(format!("cannot encode generated DSB: {error}")))?;
    if generated_spec != BOOTSTRAP_CONTROL_SPEC_V1_BYTES {
        return Err(BootstrapGenError(
            "checked-in DSB differs from the concrete generated walks".to_owned(),
        ));
    }

    let first_input = local_generator_input_bytes_v1()?;
    let second_input = local_generator_input_bytes_v1()?;
    if first_input != second_input {
        return Err(BootstrapGenError(
            "bootstrap generator input is nondeterministic".to_owned(),
        ));
    }
    let decoded = decode_generator_input_v1(&first_input)?;
    if decoded.compilation_identity() != consumer_compilation_identity_v1() {
        return Err(BootstrapGenError("decoded generator DSCI drift".to_owned()));
    }
    let first = generate_bootstrap_table_artifact_v1(&first_input)?;
    let second = generate_bootstrap_table_artifact_v1(&second_input)?;
    if first != second {
        return Err(BootstrapGenError(
            "bootstrap table generation is nondeterministic".to_owned(),
        ));
    }
    if first.resource_name() != EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1
        || first.bytes() != EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_V1
    {
        return Err(BootstrapGenError(
            "embedded DSCI-keyed bootstrap resource is stale".to_owned(),
        ));
    }
    verify_artifact_against_local_measurements(first).map(|_| ())
}

fn build_consumer_bootstrap_authority_v1() -> Result<ConsumerBootstrapAuthorityV1, BootstrapGenError>
{
    let artifact = decode_keyed_bootstrap_table_artifact_v1(
        EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_NAME_V1,
        EMBEDDED_CONSUMER_BOOTSTRAP_RESOURCE_V1,
        consumer_compilation_identity_v1(),
    )?;
    verify_artifact_against_local_measurements(artifact)
}

fn verify_artifact_against_local_measurements(
    artifact: BootstrapTableArtifactV1,
) -> Result<ConsumerBootstrapAuthorityV1, BootstrapGenError> {
    let generated_spec = generated_bootstrap_control_spec_v1().map_err(BootstrapGenError)?;
    if generated_spec
        .encode()
        .map_err(|error| BootstrapGenError(format!("cannot encode generated DSB: {error}")))?
        != BOOTSTRAP_CONTROL_SPEC_V1_BYTES
    {
        return Err(BootstrapGenError(
            "local generated definitions do not match the literal DSB".to_owned(),
        ));
    }
    let local_rows = locally_compiled_bootstrap_rows_v1().map_err(BootstrapGenError)?;
    if artifact.rows() != &local_rows {
        return Err(BootstrapGenError(
            "bootstrap resource rows do not match local Rust measurements".to_owned(),
        ));
    }
    if artifact.compilation_identity() != consumer_compilation_identity_v1() {
        return Err(BootstrapGenError(
            "bootstrap resource DSCI does not match local consumer".to_owned(),
        ));
    }
    Ok(ConsumerBootstrapAuthorityV1 { artifact })
}

fn encode_table_artifact(
    identity: CompilationIdentity,
    rows: Vec<CompiledTypeRow>,
) -> Result<BootstrapTableArtifactV1, BootstrapGenError> {
    let table = BootstrapControlTableV1::canonical(rows)
        .map_err(|error| BootstrapGenError(format!("invalid bootstrap table: {error}")))?;
    let compiled = table
        .compiled_table()
        .map_err(|error| BootstrapGenError(format!("invalid bootstrap DSCA: {error}")))?;
    let identity_bytes = canonical_compilation_identity_bytes(&identity)?;
    let digest = identity_digest(&identity);
    let mut bytes = vec![BOOTSTRAP_TABLE_VERSION_V1];
    put_bytes(&mut bytes, &identity_bytes)?;
    put_count(&mut bytes, BOOTSTRAP_CONTROL_COUNT)?;
    for row in table.rows() {
        bytes.extend_from_slice(
            &row.encode().map_err(|error| {
                BootstrapGenError(format!("cannot encode bootstrap row: {error}"))
            })?,
        );
    }
    bytes.extend_from_slice(&compiled.digest.0);
    Ok(BootstrapTableArtifactV1 {
        compilation_identity: identity,
        identity_digest: digest,
        table,
        dsca: compiled.digest,
        bytes,
        resource_name: resource_name(digest),
    })
}

fn decode_keyed_bootstrap_table_artifact_v1(
    supplied_resource_name: &str,
    bytes: &[u8],
    consumer_identity: &CompilationIdentity,
) -> Result<BootstrapTableArtifactV1, BootstrapGenError> {
    validate_compilation_identity(consumer_identity)?;
    let mut reader = Reader::new(bytes, "bootstrap table");
    let version = reader.u8()?;
    if version != BOOTSTRAP_TABLE_VERSION_V1 {
        return Err(BootstrapGenError(format!(
            "unsupported bootstrap table version {version}"
        )));
    }
    let identity_bytes = reader.bytes()?;
    let identity = decode_compilation_identity(identity_bytes)?;
    if canonical_compilation_identity_bytes(&identity)? != identity_bytes {
        return Err(BootstrapGenError(
            "noncanonical embedded CompilationIdentity".to_owned(),
        ));
    }
    if &identity != consumer_identity {
        return Err(BootstrapGenError(
            "bootstrap table CompilationIdentity does not match consumer".to_owned(),
        ));
    }
    let digest = identity_digest(&identity);
    let expected_name = resource_name(digest);
    if supplied_resource_name != expected_name {
        return Err(BootstrapGenError(format!(
            "bootstrap resource key mismatch: expected {expected_name}"
        )));
    }
    let count = reader.u32()? as usize;
    if count != BOOTSTRAP_CONTROL_COUNT {
        return Err(BootstrapGenError(format!(
            "bootstrap table row count is {count}, expected {BOOTSTRAP_CONTROL_COUNT}"
        )));
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let encoded = reader.compiled_row_bytes()?;
        rows.push(
            CompiledTypeRow::decode(encoded)
                .map_err(|error| BootstrapGenError(format!("invalid compiled row: {error}")))?,
        );
    }
    let observed_dsca = CompiledAttestationDigest(reader.a32()?);
    reader.finish()?;
    let mut artifact = encode_table_artifact(identity, rows)?;
    if artifact.dsca != observed_dsca {
        return Err(BootstrapGenError("bootstrap DSCA mismatch".to_owned()));
    }
    if artifact.bytes != bytes {
        return Err(BootstrapGenError(
            "noncanonical bootstrap table encoding".to_owned(),
        ));
    }
    artifact.resource_name = expected_name;
    Ok(artifact)
}

fn validate_compilation_identity(identity: &CompilationIdentity) -> Result<(), BootstrapGenError> {
    for text in [&identity.target_triple, &identity.rustc] {
        require_nfc(text)?;
    }
    for (package, feature) in &identity.features {
        require_nfc(package)?;
        require_nfc(feature)?;
    }
    for cfg in &identity.cfgs {
        require_nfc(cfg)?;
    }
    Ok(())
}

fn require_nfc(value: &str) -> Result<(), BootstrapGenError> {
    if value.nfc().eq(value.chars()) {
        Ok(())
    } else {
        Err(BootstrapGenError(
            "CompilationIdentity text must already be NFC".to_owned(),
        ))
    }
}

fn canonical_layouts<'a>(
    measured_layouts: &'a [BootstrapMeasuredLayoutV1<'a>],
) -> Result<Vec<BootstrapMeasuredLayoutV1<'a>>, BootstrapGenError> {
    let spec = BootstrapControlSpecV1::embedded()
        .map_err(|error| BootstrapGenError(format!("invalid embedded DSB v1: {error}")))?;
    let mut layouts = measured_layouts.to_vec();
    layouts.sort_by_key(|row| row.type_uuid);
    if layouts.len() != BOOTSTRAP_CONTROL_COUNT {
        return Err(BootstrapGenError(format!(
            "measured bootstrap row count is {}, expected {BOOTSTRAP_CONTROL_COUNT}",
            layouts.len()
        )));
    }
    for (expected, measured) in spec.0.iter().zip(&layouts) {
        if expected.type_uuid != measured.type_uuid {
            return Err(BootstrapGenError(format!(
                "missing or unexpected measured bootstrap type {}",
                measured.type_uuid
            )));
        }
    }
    Ok(layouts)
}

fn resource_name(digest: [u8; 32]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("String writes do not fail");
    }
    format!("bootstrap/control-table-v1/{hex}.dsca")
}

fn dsnl_digest(body: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DSNL");
    hasher.update(&[DSNL_VERSION]);
    hasher.update(body);
    *hasher.finalize().as_bytes()
}

fn local_source_fingerprint_v1() -> [u8; 32] {
    *blake3::hash(include_bytes!("bootstrap_builtins_v1.rs")).as_bytes()
}

fn local_manifest_lock_hash_v1() -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(include_bytes!("../Cargo.toml"));
    hasher.update(include_bytes!("../../../Cargo.lock"));
    *hasher.finalize().as_bytes()
}

fn decode_compilation_identity(bytes: &[u8]) -> Result<CompilationIdentity, BootstrapGenError> {
    let mut reader = Reader::new(bytes, "CompilationIdentity");
    let target_triple = reader.text()?.to_owned();
    let rustc = reader.text()?.to_owned();
    let source_fingerprint = reader.a32()?;
    let feature_count = reader.u32()? as usize;
    let mut features = BTreeSet::new();
    let mut last_feature: Option<Vec<u8>> = None;
    for _ in 0..feature_count {
        let package = reader.text()?.to_owned();
        let feature = reader.text()?.to_owned();
        let mut key = Vec::new();
        put_text(&mut key, &package)?;
        put_text(&mut key, &feature)?;
        if last_feature.as_ref().is_some_and(|last| last >= &key) {
            return Err(BootstrapGenError("noncanonical feature set".to_owned()));
        }
        last_feature = Some(key);
        features.insert((package, feature));
    }
    let cfg_count = reader.u32()? as usize;
    let mut cfgs = BTreeSet::new();
    let mut last_cfg: Option<Vec<u8>> = None;
    for _ in 0..cfg_count {
        let cfg = reader.text()?.to_owned();
        let mut key = Vec::new();
        put_text(&mut key, &cfg)?;
        if last_cfg.as_ref().is_some_and(|last| last >= &key) {
            return Err(BootstrapGenError("noncanonical cfg set".to_owned()));
        }
        last_cfg = Some(key);
        cfgs.insert(cfg);
    }
    let manifest_lock_hash = reader.a32()?;
    let algorithm_version = reader.u32()?;
    reader.finish()?;
    let identity = CompilationIdentity {
        target_triple,
        rustc,
        source_fingerprint,
        features,
        cfgs,
        manifest_lock_hash,
        algorithm_version,
    };
    validate_compilation_identity(&identity)?;
    Ok(identity)
}

#[derive(Clone, Copy)]
struct DsnlHeader {
    offset: u32,
    size: u32,
    align: u32,
}

fn parse_dsnl_node(
    reader: &mut Reader<'_>,
    frame_depth: u32,
) -> Result<DsnlHeader, BootstrapGenError> {
    let kind = reader.u8()?;
    let header = DsnlHeader {
        offset: reader.u32()?,
        size: reader.u32()?,
        align: reader.u32()?,
    };
    if header.align == 0 || !header.align.is_power_of_two() {
        return Err(BootstrapGenError("invalid DSNL alignment".to_owned()));
    }
    match kind {
        0x01 => {
            let scalar = reader.u8()?;
            let (size, align) = match scalar {
                0x00 | 0x02 | 0x07 => (1, 1),
                0x03 | 0x08 => (2, 2),
                0x01 | 0x04 | 0x09 | 0x0c => (4, 4),
                0x05 | 0x0a | 0x0d => (8, 8),
                0x06 | 0x0b => (16, 16),
                _ => return Err(BootstrapGenError("unknown DSNL scalar".to_owned())),
            };
            if header.size != size || header.align != align {
                return Err(BootstrapGenError(
                    "DSNL scalar size/alignment mismatch".to_owned(),
                ));
            }
        }
        0x02 => {
            let count = reader.u32()? as usize;
            let mut physical = None;
            let mut declarations = BTreeSet::new();
            let mut names = BTreeSet::new();
            for _ in 0..count {
                let name = reader.text()?.as_bytes().to_vec();
                let declaration = reader.u32()?;
                if !names.insert(name) || !declarations.insert(declaration) {
                    return Err(BootstrapGenError("duplicate DSNL struct field".to_owned()));
                }
                let child = parse_dsnl_node(reader, frame_depth.saturating_add(1))?;
                let key = (child.offset, declaration);
                if physical.is_some_and(|previous| previous >= key) {
                    return Err(BootstrapGenError(
                        "noncanonical DSNL physical field order".to_owned(),
                    ));
                }
                physical = Some(key);
            }
        }
        0x03 => {
            match reader.u8()? {
                0x00 => {
                    let _offset = reader.u32()?;
                    if !matches!(reader.u8()?, 1 | 2 | 4 | 8 | 16) {
                        return Err(BootstrapGenError("invalid DSNL direct tag size".to_owned()));
                    }
                }
                0x01 => {
                    let _offset = reader.u32()?;
                    if !matches!(reader.u8()?, 1 | 2 | 4 | 8 | 16) {
                        return Err(BootstrapGenError("invalid DSNL niche tag size".to_owned()));
                    }
                    let _niche_start = reader.u128()?;
                }
                0x02 => {}
                _ => return Err(BootstrapGenError("unknown DSNL tag form".to_owned())),
            }
            let count = reader.u32()? as usize;
            let mut previous = None;
            let mut names = BTreeSet::new();
            for _ in 0..count {
                let name = reader.text()?.as_bytes().to_vec();
                if !names.insert(name) {
                    return Err(BootstrapGenError("duplicate DSNL enum variant".to_owned()));
                }
                let declaration = reader.u32()?;
                if previous.is_some_and(|prior| prior >= declaration) {
                    return Err(BootstrapGenError(
                        "noncanonical DSNL variant order".to_owned(),
                    ));
                }
                previous = Some(declaration);
                match reader.u8()? {
                    0x00 => {
                        let _value = reader.u128()?;
                    }
                    0x01 => {
                        let _index = reader.u32()?;
                    }
                    0x02 | 0x03 => {}
                    _ => return Err(BootstrapGenError("unknown DSNL variant tag".to_owned())),
                }
                parse_dsnl_node(reader, frame_depth.saturating_add(1))?;
            }
        }
        0x04 => {
            let _len = reader.u32()?;
            let _stride = reader.u32()?;
            parse_dsnl_node(reader, frame_depth)?;
        }
        0x05 | 0x06 | 0x08 | 0x09 => {
            parse_dsnl_node(reader, frame_depth)?;
        }
        0x07 => {
            parse_dsnl_node(reader, frame_depth)?;
            parse_dsnl_node(reader, frame_depth)?;
        }
        0x0a..=0x0c => {}
        0x0d => {
            let distance = reader.u32()?;
            if distance >= frame_depth {
                return Err(BootstrapGenError("invalid DSNL back-reference".to_owned()));
            }
        }
        0x0e => {
            if header.size != 0 || header.align != 1 {
                return Err(BootstrapGenError("invalid DSNL unit layout".to_owned()));
            }
        }
        _ => return Err(BootstrapGenError("unknown DSNL node".to_owned())),
    }
    Ok(header)
}

fn put_count(out: &mut Vec<u8>, count: usize) -> Result<(), BootstrapGenError> {
    let count = u32::try_from(count)
        .map_err(|_| BootstrapGenError("canonical count exceeds u32".to_owned()))?;
    out.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), BootstrapGenError> {
    put_count(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_text(out: &mut Vec<u8>, value: &str) -> Result<(), BootstrapGenError> {
    require_nfc(value)?;
    put_bytes(out, value.as_bytes())
}

fn decode_hex_32(value: &str) -> [u8; 32] {
    assert_eq!(value.len(), 64, "internal 32-byte hex literal width");
    let mut out = [0_u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("internal lowercase hex literal");
    }
    out
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
    subject: &'static str,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], subject: &'static str) -> Self {
        Self {
            bytes,
            position: 0,
            subject,
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BootstrapGenError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| BootstrapGenError(format!("truncated {}", self.subject)))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| BootstrapGenError(format!("truncated {}", self.subject)))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, BootstrapGenError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, BootstrapGenError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("checked width"),
        ))
    }

    fn u128(&mut self) -> Result<u128, BootstrapGenError> {
        Ok(u128::from_le_bytes(
            self.take(16)?.try_into().expect("checked width"),
        ))
    }

    fn a16(&mut self) -> Result<[u8; 16], BootstrapGenError> {
        Ok(self.take(16)?.try_into().expect("checked width"))
    }

    fn a32(&mut self) -> Result<[u8; 32], BootstrapGenError> {
        Ok(self.take(32)?.try_into().expect("checked width"))
    }

    fn bytes(&mut self) -> Result<&'a [u8], BootstrapGenError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn text(&mut self) -> Result<&'a str, BootstrapGenError> {
        let bytes = self.bytes()?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| BootstrapGenError(format!("invalid UTF-8 in {}", self.subject)))?;
        require_nfc(value)?;
        Ok(value)
    }

    fn compiled_row_bytes(&mut self) -> Result<&'a [u8], BootstrapGenError> {
        const EXTRAS_LENGTH_OFFSET: usize = 16 + 32 + 32 + 1 + 32;
        const FIXED_WITH_LENGTH: usize = EXTRAS_LENGTH_OFFSET + 4;
        let start = self.position;
        let header = self.take(FIXED_WITH_LENGTH)?;
        let extras_len = u32::from_le_bytes(
            header[EXTRAS_LENGTH_OFFSET..FIXED_WITH_LENGTH]
                .try_into()
                .expect("fixed slice"),
        ) as usize;
        self.take(extras_len)?;
        Ok(&self.bytes[start..self.position])
    }

    fn finish(self) -> Result<(), BootstrapGenError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(BootstrapGenError(format!(
                "trailing {} bytes",
                self.subject
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use distill_wire::native::NativeLayoutNode;

    use super::*;

    static UNIT: NativeLayoutNode = NativeLayoutNode::Unit { offset: 0 };

    #[test]
    fn non_nfc_identity_is_rejected_before_hashing_or_generation() {
        let mut identity = consumer_compilation_identity_v1().clone();
        identity.target_triple = "a\u{030a}arch64-apple-darwin".to_owned();
        assert!(canonical_compilation_identity_bytes(&identity).is_err());

        let layouts = BootstrapControlSpecV1::embedded()
            .unwrap()
            .type_uuids()
            .map(|type_uuid| BootstrapMeasuredLayoutV1 {
                type_uuid,
                native_layout: &UNIT,
            });
        assert!(encode_generator_input_v1(&identity, &layouts).is_err());
    }

    #[test]
    fn arbitrary_caller_layouts_can_generate_only_an_unbranded_artifact() {
        let layouts = BootstrapControlSpecV1::embedded()
            .unwrap()
            .type_uuids()
            .map(|type_uuid| BootstrapMeasuredLayoutV1 {
                type_uuid,
                native_layout: &UNIT,
            });
        let input =
            encode_generator_input_v1(consumer_compilation_identity_v1(), &layouts).unwrap();
        let artifact = generate_bootstrap_table_artifact_v1(&input).unwrap();
        assert!(verify_artifact_against_local_measurements(artifact).is_err());
    }

    #[test]
    fn exact_input_decoder_rejects_dsb_dsnl_and_trailing_drift() {
        let canonical = local_generator_input_bytes_v1().unwrap();
        assert_eq!(
            decode_generator_input_v1(&canonical)
                .unwrap()
                .measured_layouts()
                .len(),
            BOOTSTRAP_CONTROL_COUNT
        );
        let mut dsb_drift = canonical.clone();
        dsb_drift[5] ^= 1;
        assert!(decode_generator_input_v1(&dsb_drift).is_err());
        let mut trailing = canonical;
        trailing.push(0);
        assert!(decode_generator_input_v1(&trailing).is_err());
    }

    #[test]
    fn keyed_decoder_rejects_name_identity_and_dsca_drift() {
        let generated =
            generate_bootstrap_table_artifact_v1(&local_generator_input_bytes_v1().unwrap())
                .unwrap();
        assert!(decode_keyed_bootstrap_table_artifact_v1(
            "bootstrap/control-table-v1/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.dsca",
            generated.bytes(),
            consumer_compilation_identity_v1(),
        )
        .is_err());
        let mut other_identity = consumer_compilation_identity_v1().clone();
        other_identity.algorithm_version += 1;
        assert!(decode_keyed_bootstrap_table_artifact_v1(
            generated.resource_name(),
            generated.bytes(),
            &other_identity,
        )
        .is_err());
        let mut changed = generated.bytes().to_vec();
        *changed.last_mut().unwrap() ^= 1;
        assert!(decode_keyed_bootstrap_table_artifact_v1(
            generated.resource_name(),
            &changed,
            consumer_compilation_identity_v1(),
        )
        .is_err());
    }

    #[test]
    fn literal_identity_fingerprints_match_the_embedded_inputs() {
        assert_eq!(
            consumer_compilation_identity_v1().source_fingerprint,
            local_source_fingerprint_v1()
        );
        assert_eq!(
            consumer_compilation_identity_v1().manifest_lock_hash,
            local_manifest_lock_hash_v1()
        );
    }
}

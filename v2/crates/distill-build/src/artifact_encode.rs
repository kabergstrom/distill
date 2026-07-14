//! Canonical schema-directed artifact emission shared by build imports and
//! processor outputs.

use distill_core::id::{AssetUuid, ContentHash, LayoutHash, LogicalHash, TypeUuid};
use distill_json::AuthoredValue;
use distill_schema::ngp_schema::SchemaNode;
use distill_wire::artifact::{
    content_hash, parse_artifact, write_artifact, ArtifactError, ArtifactHeader,
};
use distill_wire::encode::{
    encode_authored_value, AuthoredReferenceResolver, EncodeError, EncodedReference,
};
use distill_wire::wire::WireNode;

#[derive(Debug, Clone)]
pub struct ArtifactValueSpec<'a> {
    pub asset_uuid: AssetUuid,
    pub authored_type: TypeUuid,
    pub terminal_type: TypeUuid,
    pub encoded_type: TypeUuid,
    pub logical_hash: LogicalHash,
    pub layout_hash: LayoutHash,
    pub schema: &'a SchemaNode,
    pub wire: &'a WireNode,
    pub value: &'a AuthoredValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedArtifact {
    /// Complete canonical DSTL bytes, suitable for the durable build CAS.
    pub bytes: Vec<u8>,
    /// Header/tables/fixed/variable prefix, suitable for RPC or pack split
    /// transport. Blob extents are carried separately below.
    pub structural: Vec<u8>,
    pub blobs: Vec<Vec<u8>>,
    pub content_hash: ContentHash,
    pub load_deps: Vec<AssetUuid>,
    pub references: Vec<EncodedReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactEncodeError {
    Value(EncodeError),
    Artifact(ArtifactError),
}

impl std::fmt::Display for ArtifactEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "artifact encoding failed: {self:?}")
    }
}

impl std::error::Error for ArtifactEncodeError {}

impl From<EncodeError> for ArtifactEncodeError {
    fn from(error: EncodeError) -> Self {
        Self::Value(error)
    }
}

impl From<ArtifactError> for ArtifactEncodeError {
    fn from(error: ArtifactError) -> Self {
        Self::Artifact(error)
    }
}

pub fn encode_artifact_value(
    spec: ArtifactValueSpec<'_>,
    resolver: &mut impl AuthoredReferenceResolver,
) -> Result<EncodedArtifact, ArtifactEncodeError> {
    let encoded = encode_authored_value(spec.schema, spec.wire, spec.value, resolver)?;
    let mut load_deps = encoded
        .references
        .iter()
        .filter(|reference| reference.strong)
        .map(|reference| reference.asset)
        .collect::<Vec<_>>();
    load_deps.sort();
    load_deps.dedup();
    let blobs = encoded
        .blobs
        .iter()
        .map(|blob| (blob.path.clone(), blob.bytes.as_slice()))
        .collect::<Vec<_>>();
    let bytes = write_artifact(
        &ArtifactHeader {
            asset_uuid: spec.asset_uuid,
            authored_type: spec.authored_type,
            terminal_type: spec.terminal_type,
            encoded_type: spec.encoded_type,
            logical_hash: spec.logical_hash,
            layout_hash: spec.layout_hash,
        },
        &load_deps,
        &encoded.fixed,
        &encoded.variable,
        &blobs,
    )?;
    let view = parse_artifact(&bytes)?;
    let structural_len = bytes
        .len()
        .checked_sub(view.blob_section.len())
        .ok_or(ArtifactError::Overflow)?;
    let structural = bytes[..structural_len].to_vec();
    let blobs = encoded.blobs.into_iter().map(|blob| blob.bytes).collect();
    Ok(EncodedArtifact {
        content_hash: content_hash(&bytes),
        bytes,
        structural,
        blobs,
        load_deps,
        references: encoded.references,
    })
}

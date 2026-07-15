//! IO-neutral sweep bases.

use distill_store::state::SnapshotStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ManifestHash(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoBasis {
    Rpc { snapshot: SnapshotStamp },
    Pack { manifest: ManifestHash },
}

impl IoBasis {
    pub fn rpc_snapshot(&self) -> Option<SnapshotStamp> {
        match self {
            Self::Rpc { snapshot } => Some(*snapshot),
            Self::Pack { .. } => None,
        }
    }
}

//! IO-neutral sweep bases.

use std::sync::Arc;

use distill_protocol::state::SnapshotStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ManifestHash(pub [u8; 16]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoBasis {
    Rpc {
        snapshot: SnapshotStamp,
    },
    Pack {
        manifest: ManifestHash,
    },
    /// An `RpcIo` that has never connected: it holds no snapshot and
    /// answers as a daemon that names nothing.
    Disconnected,
    /// A `LayeredIO` sweep: one basis per layer, bottom to top.
    Layers(Arc<[IoBasis]>),
}

impl IoBasis {
    /// The snapshot this basis reads at; a layer stack's topmost RPC one.
    pub fn rpc_snapshot(&self) -> Option<SnapshotStamp> {
        match self {
            Self::Rpc { snapshot } => Some(*snapshot),
            Self::Pack { .. } | Self::Disconnected => None,
            Self::Layers(layers) => layers.iter().rev().find_map(Self::rpc_snapshot),
        }
    }

    /// The layer bases of a `LayeredIO` sweep, bottom to top.
    pub fn layers(&self) -> Option<&[IoBasis]> {
        match self {
            Self::Layers(layers) => Some(layers),
            _ => None,
        }
    }
}

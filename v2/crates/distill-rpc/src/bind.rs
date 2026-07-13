use std::fmt;
use std::net::SocketAddr;

/// Typed configuration-staging rejection for the unauthenticated RPC bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindStageError {
    InvalidSocketAddress { address: String },
    NonLoopbackAddress { address: String },
}

impl fmt::Display for BindStageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSocketAddress { address } => {
                write!(f, "invalid daemon.address `{address}`")
            }
            Self::NonLoopbackAddress { address } => {
                write!(f, "daemon.address `{address}` is not loopback")
            }
        }
    }
}

impl std::error::Error for BindStageError {}

/// Validate the bind at candidate-configuration staging, not when the socket
/// happens to be opened. Hostnames are deliberately rejected: without an
/// authenticated transport, DNS resolution cannot be the loopback invariant.
pub fn validate_bind_address(address: &str) -> Result<SocketAddr, BindStageError> {
    let parsed =
        address
            .parse::<SocketAddr>()
            .map_err(|_| BindStageError::InvalidSocketAddress {
                address: address.to_owned(),
            })?;
    if !parsed.ip().is_loopback() {
        return Err(BindStageError::NonLoopbackAddress {
            address: address.to_owned(),
        });
    }
    Ok(parsed)
}

//! The result of a contained callback panic.

/// A caught callback panic (§3's thunk rule): returned, never unwound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallbackPanic;

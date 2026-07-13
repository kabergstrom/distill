//! The `artifacts` metadata (§13): candidate buckets, the derived-output
//! namespace and its memo assertions, the extent index, and pins — the
//! eviction observability rule's ledger.

/// Who holds a pin (§13's observability rule): eviction may not remove a
/// ContentHash referenced by any of these. Manifest pins persist;
/// lease/in-flight/pack-session pins are ephemeral state and rebuild
/// from scratch — cleared at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum PinKind {
    /// A current or last-good manifest entry.
    Manifest = 0,
    /// A live snapshot lease (§15/§17). `resolve` pins its returned
    /// ContentHash to the caller's lease *before* the response is sent.
    Lease = 1,
    /// An in-flight build.
    InFlight = 2,
    /// An open pack-build session (§16).
    PackSession = 3,
}

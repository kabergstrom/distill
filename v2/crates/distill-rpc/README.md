# distill-rpc

This crate is the transport-neutral implementation of `DESIGN.md` §17. It
models Cap'n Proto capabilities (`Root`, target-bound `Hub`, leased `Snapshot`,
and the connection's one ordered `DeltaStream`) directly in Rust and tests the
protocol state transitions independently of sockets.

`schema/distill_rpc.capnp` pins the wire interface. `build.rs` generates the
official `capnp` Rust bindings, and `capnp_transport` supplies the concrete
loopback TCP listener/client plus capability adapters. Each connection drives
the official `capnp-rpc` `RpcSystem` on Tokio's single-threaded `LocalSet`, as
required by its `!Send` execution model. Generation fencing, attestation,
cursor installation, poison handling, and basis tagging remain in the shared
state model rather than being duplicated in the transport.

# distill-rpc

The server side of `DESIGN.md` §17. It models Cap'n Proto capabilities
(`Root`, target-bound `Hub`, leased `Snapshot`, and the connection's one
ordered `DeltaStream`) directly in Rust and tests the protocol state
transitions independently of sockets.

- Wire interface: `../distill-client/schema/distill_rpc.capnp`. `distill-client`
  generates the `capnp` bindings and holds the client transport; this crate
  re-exports them.
- Data types (requests, answers, errors, `state`): `distill-protocol`.
- `capnp_transport` adds the loopback TCP listener and the server capability
  adapters. Each connection drives the official `capnp-rpc` `RpcSystem` on
  Tokio's single-threaded `LocalSet`, as its `!Send` execution model requires.
- Generation fencing, attestation, cursor installation, poison handling and
  basis tagging live in the shared state model, not in the transport.

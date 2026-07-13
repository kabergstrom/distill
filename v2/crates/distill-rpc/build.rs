fn main() {
    println!("cargo:rerun-if-changed=schema/distill_rpc.capnp");
    capnpc::CompilerCommand::new()
        .file("schema/distill_rpc.capnp")
        .run()
        .expect("failed to generate distill RPC Cap'n Proto bindings");
}

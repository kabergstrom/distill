//! A manual authoring edit through a running daemon's `Hub.write`
//! (`#[ignore]`d; for end-to-end checks of hot reload from authoring
//! writes, newgameplus GAP 23 §4): inspect one asset, replace text in its
//! canonical value, write the entry back at the inspected version.
//!
//! Environment (the first three as `distilld engine-args` prints them):
//! `DISTILL_EDIT_ADDRESS`, `DISTILL_EDIT_TARGET`, `DISTILL_EDIT_TARGET_HASH`
//! (hex), `DISTILL_EDIT_ASSET` (asset UUID), `DISTILL_EDIT_FROM` and
//! `DISTILL_EDIT_TO` (the replaced text, which must occur exactly once).

use std::sync::Arc;

use distill_rpc::capnp_transport::{decode_authoring_inspection, schema, CapnpClient, RemoteConnectOutcome};
use distill_rpc::*;
use tokio::task::LocalSet;

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

fn target_hash(hex: &str) -> TargetDefinitionHash {
    assert_eq!(hex.len(), 64, "DISTILL_EDIT_TARGET_HASH: 32 bytes of hex");
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
    }
    TargetDefinitionHash(out)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "edits a running daemon's asset; see the module docs"]
async fn edit_asset_through_hub_write() {
    LocalSet::new()
        .run_until(async {
            let address = var("DISTILL_EDIT_ADDRESS").parse().expect("DISTILL_EDIT_ADDRESS: host:port");
            let request = ConnectRequest::new(var("DISTILL_EDIT_TARGET"), target_hash(&var("DISTILL_EDIT_TARGET_HASH")));
            let asset: AssetUuid = var("DISTILL_EDIT_ASSET").parse().expect("DISTILL_EDIT_ASSET: a UUID");
            let (from, to) = (var("DISTILL_EDIT_FROM"), var("DISTILL_EDIT_TO"));

            let client = CapnpClient::connect_local(address).await.unwrap();
            let hub = match client.connect(&request).await.unwrap() {
                RemoteConnectOutcome::Connected { hub, .. } => hub,
                other => panic!("connect: {other:?}"),
            };
            let snapshot = hub.authoring_snapshot_request().send().promise.await.unwrap();
            let snapshot = match snapshot.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::authoring_snapshot_call::Which::Success(snapshot) => snapshot.unwrap(),
                _ => panic!("authoringSnapshot failed"),
            };
            let mut inspect = snapshot.inspect_request();
            inspect.get().set_uuid(&asset.0);
            let inspected = inspect.send().promise.await.unwrap();
            let inspection = match inspected.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::authoring_inspect_call::Which::Success(found) => decode_authoring_inspection(found.unwrap()).unwrap(),
                _ => panic!("inspect {asset}: no authored entry"),
            };

            let text = std::str::from_utf8(&inspection.value.canonical_value).expect("canonical text");
            assert_eq!(text.matches(from.as_str()).count(), 1, "DISTILL_EDIT_FROM must occur once in {text}");
            let edited = text.replacen(from.as_str(), &to, 1);
            let entry = AuthoringEntry {
                uuid: inspection.uuid,
                bundle: inspection.bundle,
                local_id: inspection.local_id,
                normalized_path: inspection.normalized_path,
                type_uuid: inspection.type_uuid,
                terminal_type: inspection.type_uuid,
                schema_hash: inspection.schema_hash,
                logical_schema: inspection.logical_schema,
                role: inspection.role,
                tags: Default::default(),
                value: AuthoringValue { canonical_value: Arc::from(edited.as_bytes()), ..inspection.value },
            };

            let mut write = hub.write_request();
            {
                let mut params = write.get();
                params.set_base(inspection.stamp.version.0);
                distill_rpc::capnp_loader::write_authoring_entry(params.reborrow().init_ops(1).get(0).init_set(), &entry);
            }
            let written = write.send().promise.await.unwrap();
            match written.get().unwrap().get_result().unwrap().which().unwrap() {
                schema::data_call::Which::Success(receipt) => {
                    println!("written: {:?}", WriteReceipt::decode(receipt.unwrap()).unwrap())
                }
                schema::data_call::Which::Error(error) => panic!("write: {}", error.unwrap().get_message().unwrap().to_string().unwrap()),
                _ => panic!("write failed"),
            }
        })
        .await;
}

use std::sync::Arc;

use distill_importer::SerializedAsset;
use distill_schema::{build_artifact_metadata, data::artifact};
use rusqlite::Connection;

use crate::{
    db::{queries, Database, OwnedMessageReader, RoTransaction, RwTransaction},
    error::Result,
};

const TABLE_HASH_TO_ARTIFACT: &str = "hash_to_artifact";

pub struct ArtifactCache {
    db: Arc<Database>,
}

impl ArtifactCache {
    pub fn new(db: &Arc<Database>) -> Result<ArtifactCache> {
        Ok(ArtifactCache { db: db.clone() })
    }

    // TODO: invalidate cache
    #[allow(dead_code)]
    pub async fn delete(&self, hash: u64) -> Result<bool> {
        let txn = self.db.rw_txn().await?;
        let result = queries::delete(txn.conn(), TABLE_HASH_TO_ARTIFACT, &hash.to_le_bytes())?;
        Ok(result)
    }

    pub fn insert<T: AsRef<[u8]>>(
        &self,
        txn: &mut RwTransaction,
        artifact: &SerializedAsset<T>,
    ) {
        queries::put_capnp(
            txn.conn(),
            TABLE_HASH_TO_ARTIFACT,
            &artifact.metadata.id.0.to_le_bytes(),
            &build_artifact_message(artifact),
        )
        .expect("sqlite: failed to put artifact");
        txn.dirty = true;
    }

    pub async fn ro_txn(&self) -> Result<RoTransaction> {
        self.db.ro_txn().await
    }

    pub async fn rw_txn(&self) -> Result<RwTransaction> {
        self.db.rw_txn().await
    }

    pub fn get(
        &self,
        conn: &Connection,
        hash: u64,
    ) -> Option<OwnedMessageReader<artifact::Owned>> {
        queries::get_capnp::<artifact::Owned>(conn, TABLE_HASH_TO_ARTIFACT, &hash.to_le_bytes())
            .expect("db: Failed to get entry from hash_to_artifact table")
    }
}

pub(crate) fn build_artifact_message<T: AsRef<[u8]>>(
    artifact: &SerializedAsset<T>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<artifact::Builder<'_>>();
        let mut metadata = m.reborrow().init_metadata();
        build_artifact_metadata(&artifact.metadata, &mut metadata);
        let slice: &[u8] = artifact.data.as_ref();
        m.reborrow().set_data(slice);
    }
    value_builder
}

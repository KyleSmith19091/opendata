use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{Db, DbIterator, DbSnapshot, WriteBatch};

use crate::{
    BytesRange, Record, StorageError, StorageIterator, StorageRead, StorageResult,
    storage::{Storage, StorageSnapshot},
};

/// SlateDB-backed implementation of the Storage trait.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability.
pub struct SlateDbStorage {
    pub(super) db: Arc<Db>,
}

impl SlateDbStorage {
    /// Creates a new SlateDbStorage instance wrapping the given SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl StorageRead for SlateDbStorage {
    /// Retrieves a single record by key from SlateDB.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + '_>> {
        let iter = self
            .db
            .scan(range)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

struct SlateDbIterator {
    iter: DbIterator,
}

#[async_trait]
impl StorageIterator for SlateDbIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.iter.next().await.map_err(StorageError::from_storage)? {
            Some(entry) => Ok(Some(Record::new(entry.key, entry.value))),
            None => Ok(None),
        }
    }
}

/// SlateDB snapshot wrapper that implements StorageSnapshot.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
pub struct SlateDbStorageSnapshot {
    snapshot: Arc<DbSnapshot>,
}

#[async_trait]
impl StorageRead for SlateDbStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + '_>> {
        let iter = self
            .snapshot
            .scan(range)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

#[async_trait]
impl StorageSnapshot for SlateDbStorageSnapshot {}

#[async_trait]
impl Storage for SlateDbStorage {
    /// Writes a batch of records to SlateDB.
    ///
    /// This method uses SlateDB's batch write API to write all records atomically
    /// in a single operation.
    async fn put(&self, records: Vec<Record>) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        for record in records {
            batch.put(record.key, record.value);
        }
        self.db
            .write(batch)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }

    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Arc::new(SlateDbStorageSnapshot { snapshot }))
    }
}

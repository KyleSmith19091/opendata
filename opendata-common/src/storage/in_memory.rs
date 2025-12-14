use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;

use super::{Storage, StorageSnapshot};
use crate::{BytesRange, Record, StorageError, StorageIterator, StorageRead, StorageResult};

/// In-memory implementation of the Storage trait using a BTreeMap.
///
/// This implementation stores all data in memory and is useful for testing
/// or scenarios where durability is not required.
pub struct InMemoryStorage {
    data: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
}

impl InMemoryStorage {
    /// Creates a new InMemoryStorage instance with an empty store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageRead for InMemoryStorage {
    /// Retrieves a single record by key from the in-memory store.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        match data.get(&key) {
            Some(value) => Ok(Some(Record::new(key, value.clone()))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + '_>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        // Collect all matching records into a Vec for the iterator
        let records: Vec<Record> = data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .map(|(k, v)| Record::new(k.clone(), v.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

struct InMemoryIterator {
    records: Vec<Record>,
    index: usize,
}

#[async_trait]
impl StorageIterator for InMemoryIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        if self.index >= self.records.len() {
            Ok(None)
        } else {
            let record = self.records[self.index].clone();
            self.index += 1;
            Ok(Some(record))
        }
    }
}

/// In-memory snapshot that holds a copy of the data at the time of snapshot creation.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
pub struct InMemoryStorageSnapshot {
    data: Arc<BTreeMap<Bytes, Bytes>>,
}

#[async_trait]
impl StorageRead for InMemoryStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        match self.data.get(&key) {
            Some(value) => Ok(Some(Record::new(key, value.clone()))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + '_>> {
        // Collect all matching records into a Vec for the iterator
        let records: Vec<Record> = self
            .data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .map(|(k, v)| Record::new(k.clone(), v.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

#[async_trait]
impl StorageSnapshot for InMemoryStorageSnapshot {}

#[async_trait]
impl Storage for InMemoryStorage {
    /// Writes a batch of records to the in-memory store.
    ///
    /// All records are written atomically within a single write lock acquisition.
    async fn put(&self, records: Vec<Record>) -> StorageResult<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        for record in records {
            data.insert(record.key, record.value);
        }

        Ok(())
    }

    /// Creates a point-in-time snapshot of the in-memory storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        // Clone the entire BTreeMap for the snapshot
        let snapshot_data = Arc::new(data.clone());

        Ok(Arc::new(InMemoryStorageSnapshot {
            data: snapshot_data,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[tokio::test]
    async fn should_return_none_when_key_not_found() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let result = storage.get(Bytes::from("missing_key")).await;

        // then
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_store_and_retrieve_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), value.clone())])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.key, Bytes::from("test_key"));
        assert_eq!(record.value, value);
    }

    #[tokio::test]
    async fn should_overwrite_existing_key() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let initial_value = Bytes::from("initial_value");
        let updated_value = Bytes::from("updated_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), initial_value)])
            .await
            .unwrap();
        storage
            .put(vec![Record::new(key.clone(), updated_value.clone())])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, updated_value);
    }

    #[tokio::test]
    async fn should_store_multiple_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")),
            Record::new(Bytes::from("key2"), Bytes::from("value2")),
            Record::new(Bytes::from("key3"), Bytes::from("value3")),
        ];

        // when
        storage.put(records.clone()).await.unwrap();

        // then
        for record in records {
            let retrieved = storage.get(record.key.clone()).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value, record.value);
        }
    }

    #[tokio::test]
    async fn should_scan_all_records_when_unbounded() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
        ];
        storage.put(records.clone()).await.unwrap();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0].key, Bytes::from("a"));
        assert_eq!(scanned[1].key, Bytes::from("b"));
        assert_eq!(scanned[2].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_scan_records_with_prefix() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("prefix_a"), Bytes::from("value1")),
            Record::new(Bytes::from("prefix_b"), Bytes::from("value2")),
            Record::new(Bytes::from("other_c"), Bytes::from("value3")),
        ];
        storage.put(records).await.unwrap();

        // when
        let scanned = storage
            .scan(BytesRange::prefix(Bytes::from("prefix_")))
            .await
            .unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("prefix_a"));
        assert_eq!(scanned[1].key, Bytes::from("prefix_b"));
    }

    #[tokio::test]
    async fn should_scan_records_in_bounded_range() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
            Record::new(Bytes::from("d"), Bytes::from("value_d")),
        ];
        storage.put(records).await.unwrap();

        // when
        let range = BytesRange::new(
            Bound::Included(Bytes::from("b")),
            Bound::Excluded(Bytes::from("d")),
        );
        let scanned = storage.scan(range).await.unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("b"));
        assert_eq!(scanned[1].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_return_empty_vec_when_scanning_empty_storage() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert!(scanned.is_empty());
    }

    #[tokio::test]
    async fn should_iterate_over_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")),
            Record::new(Bytes::from("key2"), Bytes::from("value2")),
        ];
        storage.put(records).await.unwrap();

        // when
        let mut iter = storage.scan_iter(BytesRange::unbounded()).await.unwrap();
        let first = iter.next().await.unwrap();
        let second = iter.next().await.unwrap();
        let third = iter.next().await.unwrap();

        // then
        assert!(first.is_some());
        assert_eq!(first.unwrap().key, Bytes::from("key1"));
        assert!(second.is_some());
        assert_eq!(second.unwrap().key, Bytes::from("key2"));
        assert!(third.is_none());
    }

    #[tokio::test]
    async fn should_create_snapshot_with_current_data() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![Record::new(
                Bytes::from("key1"),
                Bytes::from("value1"),
            )])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();

        // then
        let result = snapshot.get(Bytes::from("key1")).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("value1"));
    }

    #[tokio::test]
    async fn should_not_see_writes_after_snapshot() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![Record::new(
                Bytes::from("key1"),
                Bytes::from("value1"),
            )])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![Record::new(
                Bytes::from("key2"),
                Bytes::from("value2"),
            )])
            .await
            .unwrap();

        // then
        let snapshot_result = snapshot.get(Bytes::from("key2")).await.unwrap();
        assert!(snapshot_result.is_none());

        let storage_result = storage.get(Bytes::from("key2")).await.unwrap();
        assert!(storage_result.is_some());
    }

    #[tokio::test]
    async fn should_scan_snapshot_independently() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![Record::new(Bytes::from("a"), Bytes::from("value_a"))])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![Record::new(Bytes::from("b"), Bytes::from("value_b"))])
            .await
            .unwrap();

        // then
        let snapshot_records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(snapshot_records.len(), 1);
        assert_eq!(snapshot_records[0].key, Bytes::from("a"));

        let storage_records = storage.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(storage_records.len(), 2);
    }

    #[tokio::test]
    async fn should_handle_empty_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("empty_key");

        // when
        storage.put(vec![Record::empty(key.clone())]).await.unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::new());
    }
}

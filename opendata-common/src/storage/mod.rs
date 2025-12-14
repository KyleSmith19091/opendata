mod in_memory;
mod slate;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::BytesRange;

#[derive(Clone, Debug)]
pub struct Record {
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn empty(key: Bytes) -> Self {
        Self::new(key, Bytes::new())
    }
}

/// Error type for storage operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Storage-related errors
    Storage(String),
    /// Internal errors
    Internal(String),
}

impl std::error::Error for StorageError {}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StorageError::Storage(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl StorageError {
    /// Converts a storage error to StorageError::Storage.
    pub fn from_storage(e: impl std::fmt::Display) -> Self {
        StorageError::Storage(e.to_string())
    }
}

/// Result type alias for storage operations
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Iterator over storage records.
#[async_trait]
pub trait StorageIterator {
    async fn next(&mut self) -> StorageResult<Option<Record>>;
}

/// Common read operations supported by both Storage and StorageSnapshot.
///
/// This trait provides the core read methods that are shared between full storage
/// access and point-in-time snapshots. By extracting these common operations,
/// we can write code that works with both storage types.
#[async_trait]
pub trait StorageRead: Send + Sync {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>>;

    /// Returns an iterator over records in the given range.
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + '_>>;

    /// Collects all records in the range into a Vec.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan(&self, range: BytesRange) -> StorageResult<Vec<Record>> {
        let mut iter = self.scan_iter(range).await?;
        let mut records = Vec::new();
        while let Some(record) = iter.next().await? {
            records.push(record);
        }
        Ok(records)
    }
}

/// A point-in-time snapshot of the storage layer.
///
/// Snapshots provide a consistent read-only view of the database at the time
/// the snapshot was created. Reads from a snapshot will not see any subsequent
/// writes to the underlying storage.
#[async_trait]
pub trait StorageSnapshot: StorageRead {}

/// The storage type encapsulates access to the underlying storage (e.g. SlateDB).
/// TODO(agavra): figure out how to model merge operations
#[async_trait]
pub trait Storage: StorageRead {
    async fn put(&self, records: Vec<Record>) -> StorageResult<()>;

    /// Creates a point-in-time snapshot of the storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>>;
}

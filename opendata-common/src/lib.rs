#![allow(dead_code)]
pub mod storage;
pub mod util;

pub use storage::{Record, StorageError, StorageIterator, StorageRead, StorageResult};
pub use util::BytesRange;

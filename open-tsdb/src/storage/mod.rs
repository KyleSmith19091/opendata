use std::{collections::HashMap, sync::Arc};

use opendata_common::storage::Storage;

use crate::{
    model::{SeriesFingerprint, SeriesId, TimeBucket},
    serde::{bucket_list::BucketListValue, key::{BucketListKey, SeriesDictionaryKey}},
    util::Result,
};

pub(crate) struct OpenTsdbStorage {
    inner: Arc<dyn Storage>,
}

impl OpenTsdbStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self { inner }
    }

    /// Given a time range, return all the time buckets that contain data for
    /// that range sorted by start time.
    /// 
    /// This method examines the actual list of buckets in storage to determine the 
    /// candidate buckets (as opposed to computing theoretical buckets from the
    /// start and end times).
    pub async fn get_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> Result<Vec<TimeBucket>> {
        if let (Some(start), Some(end)) = (start_secs, end_secs) {
            if end < start {
                return Err("end must be greater than or equal to start".into());
            }
        }

        // Convert to minutes once before filtering
        let start_min = start_secs.map(|s| (s / 60) as u32);
        let end_min = end_secs.map(|e| (e / 60) as u32);

        let key = BucketListKey.encode();
        let record = self.inner.get(key).await?;
        let bucket_list = match record {
            Some(record) => BucketListValue::decode(record.value.as_ref())?,
            None => BucketListValue {
                buckets: Vec::new(),
            },
        };

        let mut filtered_buckets : Vec<TimeBucket> = bucket_list.buckets
            .into_iter()
            .map(|(size, start)| TimeBucket { size, start })
            .filter(|bucket| match (start_min, end_min) {
                (None, None) => true,
                (Some(start), None) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    bucket.start >= start_bucket_min
                }
                (None, Some(end)) => {
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start <= end_bucket_min
                }
                (Some(start), Some(end)) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start >= start_bucket_min
                        && bucket.start <= end_bucket_min
                }
            })
            .collect();

        filtered_buckets.sort_by_key(|bucket| bucket.start);
        Ok(filtered_buckets)
    }

}

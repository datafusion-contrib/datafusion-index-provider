use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion_index_provider::physical_plan::fetcher::RecordFetcher;

/// Mapper that filters batches using index results
pub struct BatchMapper {
    batches: Vec<RecordBatch>,
    rows_fetched: AtomicUsize,
}

impl BatchMapper {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            rows_fetched: AtomicUsize::new(0),
        }
    }

    /// Total number of row fetched.
    pub fn rows_fetched(&self) -> usize {
        self.rows_fetched.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for BatchMapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BatchMapper")
    }
}

#[async_trait]
impl RecordFetcher for BatchMapper {
    fn schema(&self) -> SchemaRef {
        self.batches
            .first()
            .expect("BatchMapper requires at least one batch")
            .schema()
    }

    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        log::debug!("Index batch: {index_batch:?}");
        self.rows_fetched
            .fetch_add(index_batch.num_rows(), Ordering::SeqCst);
        let indices = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_ids: Vec<u64> = indices.iter().flatten().collect();

        log::debug!("Row ids: {row_ids:?}");

        apply_row_filter(&self.batches[0], &row_ids)
    }
}

fn apply_row_filter(batch: &RecordBatch, row_ids: &[u64]) -> Result<RecordBatch> {
    log::debug!("Row ids: {row_ids:?}");
    // Convert 1-based primary keys to 0-based row positions for arrow `take`.
    let indices = UInt64Array::from_iter_values(row_ids.iter().map(|&i| i - 1));
    let new_columns: Result<Vec<Arc<dyn Array>>> = batch
        .columns()
        .iter()
        .map(|col| {
            Ok(Arc::new(datafusion::arrow::compute::take(
                col.as_ref(),
                &indices,
                None,
            )?) as Arc<dyn Array>)
        })
        .collect();

    log::debug!("New columns: {new_columns:?}");

    Ok(RecordBatch::try_new(batch.schema(), new_columns?)?)
}

use std::fmt;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion_index_provider::physical_plan::fetcher::RecordFetcher;

pub struct CompositePkFetcher {
    data: RecordBatch,
}

impl CompositePkFetcher {
    pub fn new(data: RecordBatch) -> Self {
        Self { data }
    }
}

impl fmt::Debug for CompositePkFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompositePkFetcher")
    }
}

#[async_trait]
impl RecordFetcher for CompositePkFetcher {
    fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        let req_tenants = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let req_eids = index_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let data_tenants = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_eids = self
            .data
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut matched: Vec<RecordBatch> = Vec::new();
        for i in 0..req_tenants.len() {
            let req_t = req_tenants.value(i);
            let req_e = req_eids.value(i);

            for j in 0..data_tenants.len() {
                if data_tenants.value(j) == req_t && data_eids.value(j) == req_e {
                    matched.push(self.data.slice(j, 1));
                    break;
                }
            }
        }

        Ok(concat_batches(&self.data.schema(), &matched)?)
    }
}

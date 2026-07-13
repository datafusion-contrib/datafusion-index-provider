use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::Statistics;
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_index_provider::physical_plan::create_index_schema;
use datafusion_index_provider::physical_plan::Index;

#[derive(Debug)]
pub struct DepartmentIndex {
    index: BTreeMap<String, Vec<u64>>,
}

impl DepartmentIndex {
    pub fn new(departments: &StringArray, ids: &UInt64Array) -> Self {
        let mut index: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for i in 0..departments.len() {
            let department = departments.value(i).to_string();
            let row_id = ids.value(i);
            index.entry(department).or_default().push(row_id);
        }
        DepartmentIndex { index }
    }

    pub fn create_data_from_filters(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Vec<RecordBatch> {
        let mut row_ids = BTreeSet::new();

        for filter in filters {
            if let Expr::BinaryExpr(be) = filter {
                if let Expr::Column(c) = be.left.as_ref() {
                    if c.name == "department" {
                        if let Expr::Literal(ScalarValue::Utf8(Some(v)), _) = be.right.as_ref() {
                            if be.op == Operator::Eq {
                                if let Some(ids) = self.index.get(v) {
                                    for id in ids {
                                        row_ids.insert(*id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut final_row_ids: Vec<u64> = row_ids.into_iter().collect();

        if let Some(l) = limit {
            final_row_ids.truncate(l);
        }

        if final_row_ids.is_empty() {
            return vec![];
        }

        let schema = self.index_schema();
        let column = Arc::new(UInt64Array::from(final_row_ids)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();

        vec![batch]
    }
}

impl Index for DepartmentIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "department_index"
    }

    fn index_schema(&self) -> SchemaRef {
        create_index_schema([Field::new("id", DataType::UInt64, false)])
    }

    fn table_name(&self) -> &'static str {
        "employees"
    }

    fn column_name(&self) -> &'static str {
        "department"
    }

    fn scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let data = self.create_data_from_filters(filters, limit);
        log::debug!("Department index data: {data:?}");
        Ok(Box::pin(MemoryStream::try_new(
            data,
            self.index_schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref())
    }
}

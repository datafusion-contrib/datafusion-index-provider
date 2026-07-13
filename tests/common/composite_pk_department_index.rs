use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::Statistics;
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_index_provider::physical_plan::Index;

use super::composite_pk_provider::composite_pk_schema;

#[derive(Debug)]
pub struct CompositePkDeptIndex {
    // department -> Vec<(tenant_id, employee_id)>
    index: BTreeMap<String, Vec<(String, u64)>>,
}

impl CompositePkDeptIndex {
    pub fn new(
        departments: &StringArray,
        tenant_ids: &StringArray,
        employee_ids: &UInt64Array,
    ) -> Self {
        let mut index: BTreeMap<String, Vec<(String, u64)>> = BTreeMap::new();
        for i in 0..departments.len() {
            let dept = departments.value(i).to_string();
            let tenant = tenant_ids.value(i).to_string();
            let eid = employee_ids.value(i);
            index.entry(dept).or_default().push((tenant, eid));
        }
        Self { index }
    }

    pub fn create_data_from_filters(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Vec<RecordBatch> {
        let mut keys = BTreeSet::new();

        for filter in filters {
            if let Expr::BinaryExpr(be) = filter {
                if let Expr::Column(c) = be.left.as_ref() {
                    if c.name == "department" {
                        if let Expr::Literal(ScalarValue::Utf8(Some(v)), _) = be.right.as_ref() {
                            if be.op == Operator::Eq {
                                if let Some(ids) = self.index.get(v) {
                                    for id in ids {
                                        keys.insert(id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut key_vec: Vec<(String, u64)> = keys.into_iter().collect();
        if let Some(l) = limit {
            key_vec.truncate(l);
        }

        if key_vec.is_empty() {
            return vec![];
        }

        let tenants: Vec<&str> = key_vec.iter().map(|(t, _)| t.as_str()).collect();
        let eids: Vec<u64> = key_vec.iter().map(|(_, e)| *e).collect();

        let batch = RecordBatch::try_new(
            composite_pk_schema(),
            vec![
                Arc::new(StringArray::from(tenants)) as ArrayRef,
                Arc::new(UInt64Array::from(eids)) as ArrayRef,
            ],
        )
        .unwrap();

        vec![batch]
    }
}

impl Index for CompositePkDeptIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "composite_dept_index"
    }

    fn index_schema(&self) -> SchemaRef {
        composite_pk_schema()
    }

    fn table_name(&self) -> &'static str {
        "multi_tenant_employees"
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

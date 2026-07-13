use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
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
pub struct CompositePkAgeIndex {
    // age -> Vec<(tenant_id, employee_id)>
    index: BTreeMap<i32, Vec<(String, u64)>>,
}

impl CompositePkAgeIndex {
    pub fn new(
        ages: &datafusion::arrow::array::Int32Array,
        tenant_ids: &StringArray,
        employee_ids: &UInt64Array,
    ) -> Self {
        let mut index: BTreeMap<i32, Vec<(String, u64)>> = BTreeMap::new();
        for i in 0..ages.len() {
            let age = ages.value(i);
            let tenant = tenant_ids.value(i).to_string();
            let eid = employee_ids.value(i);
            index.entry(age).or_default().push((tenant, eid));
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
                    if c.name == "age" {
                        if let Expr::Literal(ScalarValue::Int32(Some(v)), _) = be.right.as_ref() {
                            match be.op {
                                Operator::Eq => {
                                    if let Some(ids) = self.index.get(v) {
                                        for id in ids {
                                            keys.insert(id.clone());
                                        }
                                    }
                                }
                                Operator::Gt => {
                                    for (_, ids) in self.index.range((v + 1)..) {
                                        for id in ids {
                                            keys.insert(id.clone());
                                        }
                                    }
                                }
                                Operator::LtEq => {
                                    for (_, ids) in self.index.range(..=v) {
                                        for id in ids {
                                            keys.insert(id.clone());
                                        }
                                    }
                                }
                                _ => {}
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

impl Index for CompositePkAgeIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "composite_age_index"
    }

    fn index_schema(&self) -> SchemaRef {
        composite_pk_schema()
    }

    fn table_name(&self) -> &'static str {
        "multi_tenant_employees"
    }

    fn column_name(&self) -> &'static str {
        "age"
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

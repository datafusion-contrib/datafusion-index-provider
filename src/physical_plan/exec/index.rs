// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::physical_plan::Index;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::{
    execution_plan::EmissionType, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use std::sync::Arc;

/// Physical execution plan for scanning an index to produce row IDs.
///
/// This operator represents the index phase of the two-phase execution model. It scans
/// a single index with provided filter expressions and produces a stream of row IDs
/// that satisfy the predicates. The output is then used by downstream operators like
/// `RecordFetchExec` to retrieve complete records.
///
/// ## Execution Characteristics
/// - **Single partition**: Always produces exactly one partition for correctness
/// - **Streaming**: Results are emitted incrementally as they are discovered
/// - **Bounded**: Execution completes when all matching row IDs are produced
/// - **Ordering**: Preserves index ordering if the underlying index reports `is_ordered()`
///
/// ## Performance Considerations
/// - Filter selectivity directly impacts downstream performance
/// - Limit pushdown reduces unnecessary index scanning and memory usage
/// - Ordered indexes enable optimized downstream joins via SortMergeJoin
#[derive(Debug)]
pub struct IndexScanExec {
    /// The index to scan.
    index: Arc<dyn Index>,
    /// The filters to apply to the index.
    filters: Vec<Expr>,
    /// The limit to apply to the index.
    limit: Option<usize>,
    /// Properties of the plan.
    plan_properties: Arc<PlanProperties>,
}

impl DisplayAs for IndexScanExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IndexScanExec: index={}, filters=[", self.index.name())?;
                for (i, filter) in self.filters.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{filter}")?;
                }
                write!(f, "], limit={:?}", self.limit)
            }
        }
    }
}

impl ExecutionPlan for IndexScanExec {
    /// Return a reference to the name of this execution plan.
    fn name(&self) -> &str {
        "IndexScanExec"
    }

    /// Get the properties for this execution plan
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    /// Returns the children of this [`ExecutionPlan`].
    /// This plan has no children.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Create a new [`ExecutionPlan`] with new children.
    ///
    /// This method is used to reconstruct the plan with new inputs.
    /// This plan has no children, so it returns a clone of itself.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    /// Executes this plan and returns a stream of `RecordBatch`es.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "IndexScanExec only supports a single partition".to_string(),
            ));
        }

        self.index.scan(&self.filters, self.limit)
    }
}

impl IndexScanExec {
    /// Creates a new `IndexScanExec` execution plan.
    ///
    /// # Arguments
    /// * `index` - The index to scan for row IDs
    /// * `filters` - Filter expressions to apply during the scan
    /// * `limit` - Optional limit on number of row IDs to return
    /// * `schema` - Schema of the index output. All columns form the composite primary key.
    ///
    /// # Returns
    /// A configured `IndexScanExec` that will scan the index with the specified parameters.
    /// The execution plan will automatically detect if the index produces ordered results
    /// and configure appropriate output ordering properties for downstream optimization.
    pub fn try_new(
        index: Arc<dyn Index>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let ordering = if index.is_ordered() {
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| PhysicalSortExpr {
                    expr: Arc::new(Column::new(field.name(), i)),
                    options: Default::default(),
                })
                .collect()
        } else {
            vec![]
        };
        let eq = EquivalenceProperties::new_with_orderings(schema.clone(), [ordering]);

        let plan_properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            index,
            filters,
            limit,
            plan_properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Statistics;
    use datafusion::physical_plan::memory::MemoryStream;
    use std::any::Any;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockIndex {
        schema: SchemaRef,
        scan_called: Mutex<bool>,
    }

    impl MockIndex {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                scan_called: Mutex::new(false),
            }
        }
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_index"
        }

        fn index_schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_name(&self) -> &str {
            "mock_table"
        }

        fn column_name(&self) -> &str {
            "mock_column"
        }

        fn scan(
            &self,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            *self.scan_called.lock().unwrap() = true;
            let batch = RecordBatch::new_empty(self.schema.clone());
            let stream = MemoryStream::try_new(vec![batch], self.schema.clone(), None)?;
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.schema)
        }
    }

    #[tokio::test]
    async fn test_index_scan_exec() -> datafusion::common::Result<()> {
        let index = Arc::new(MockIndex::new());
        let schema = index.index_schema();
        let exec = IndexScanExec::try_new(index.clone(), vec![], None, schema)?;

        // execute
        let task_ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, task_ctx)?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;

        assert!(*index.scan_called.lock().unwrap());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_index_scan_exec_invalid_partition() {
        let index = Arc::new(MockIndex::new());
        let schema = index.index_schema();
        let exec = IndexScanExec::try_new(index.clone(), vec![], None, schema).unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        let res = exec.execute(1, task_ctx);
        match res {
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("IndexScanExec only supports a single partition"),
                    "unexpected error message: {e}"
                );
            }
            Ok(_) => panic!("expected an error"),
        }
    }
}

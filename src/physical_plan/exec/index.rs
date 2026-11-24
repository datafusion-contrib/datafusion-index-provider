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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    execution_plan::EmissionType, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
/// - Ordered indexes enable optimized downstream joins via `SortMergeJoin`
///
/// ## Tracing
/// `execute` opens a `debug` level `index_scan` span, and `RecordFetchExec` opens a
/// `record_fetch` span per fetch call. They are children of whichever span is current
/// when `execute` runs. `DataFusion` may run partitions in spawned tasks (for example a
/// parallel OR through `CoalescePartitionsExec`), where the query span is not current,
/// so these spans can end up detached from it. To keep them parented, register a
/// `JoinSetTracer` in the application with
/// `datafusion::common::runtime::set_join_set_tracer`, or use
/// [datafusion-tracing](https://github.com/datafusion-contrib/datafusion-tracing).
/// This crate does not register a tracer itself.
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
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
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
    fn name(&self) -> &'static str {
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

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let span = tracing::debug_span!(
            "index_scan",
            index = self.index.name(),
            table = self.index.table_name(),
            partition,
            filters = self.filters.len(),
        );
        let stream = {
            let _timer = baseline_metrics.elapsed_compute().timer();
            span.in_scope(|| self.index.scan(&self.filters, self.limit))?
        };
        let stream_schema = stream.schema();
        let index_schema = self.index.index_schema();
        if stream_schema.fields() != index_schema.fields() {
            return Err(DataFusionError::Internal(format!(
                "Index '{}' scan stream schema does not match Index::index_schema(): stream returned {stream_schema:?}, expected {index_schema:?}",
                self.index.name()
            )));
        }
        Ok(Box::pin(IndexScanStream {
            inner: stream,
            baseline_metrics,
            span,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
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
    ///
    /// # Errors
    /// Returns an error if the plan properties cannot be computed for the given schema.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(index = index.name(), filters = filters.len(), limit = ?limit)
    )]
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
                .map(|(i, field)| {
                    PhysicalSortExpr::new_default(Arc::new(Column::new(field.name(), i))).asc()
                })
                .collect()
        } else {
            vec![]
        };
        let eq = EquivalenceProperties::new_with_orderings(schema, [ordering]);

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
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// Wraps the stream returned by [`Index::scan`] to record [`BaselineMetrics`] and
/// poll it inside the `index_scan` span.
struct IndexScanStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    span: tracing::Span,
}

impl RecordBatchStream for IndexScanStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for IndexScanStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let poll = {
            let _timer = this.baseline_metrics.elapsed_compute().timer();
            let _entered = this.span.enter();
            this.inner.poll_next_unpin(cx)
        };
        this.baseline_metrics.record_poll(poll)
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
        scan_schema: Option<SchemaRef>,
    }

    impl MockIndex {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                scan_called: Mutex::new(false),
                scan_schema: None,
            }
        }

        /// Makes `scan` return a stream whose schema differs from `index_schema()`.
        fn with_scan_schema(mut self, schema: SchemaRef) -> Self {
            self.scan_schema = Some(schema);
            self
        }
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &'static str {
            "mock_index"
        }

        fn index_schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_name(&self) -> &'static str {
            "mock_table"
        }

        fn column_name(&self) -> &'static str {
            "mock_column"
        }

        fn scan(
            &self,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            *self.scan_called.lock().unwrap() = true;
            let schema = self.scan_schema.clone().unwrap_or(self.schema.clone());
            let batch = RecordBatch::new_empty(schema.clone());
            let stream = MemoryStream::try_new(vec![batch], schema, None)?;
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

    #[tokio::test]
    async fn test_index_scan_exec_stream_schema_mismatch() {
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Utf8,
            false,
        )]));
        let index = Arc::new(MockIndex::new().with_scan_schema(wrong_schema));
        let schema = index.index_schema();
        let exec = IndexScanExec::try_new(index, vec![], None, schema).unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        match exec.execute(0, task_ctx) {
            Err(DataFusionError::Internal(msg)) => {
                assert!(
                    msg.contains("mock_index"),
                    "unexpected error message: {msg}"
                );
            }
            Err(e) => panic!("unexpected error variant: {e:?}"),
            Ok(_) => panic!("expected an error"),
        }
    }
}

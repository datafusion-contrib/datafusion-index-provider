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

//! Sequential union execution plan that processes inputs without spawning tasks.
//!
//! This module provides [`SequentialUnionExec`], an alternative to `DataFusion`'s
//! [`UnionExec`](datafusion::physical_plan::union::UnionExec) that reports a single partition and processes all input partitions
//! sequentially. This avoids the task spawning that occurs when `CoalescePartitionsExec`
//! is inserted for multi-partition plans.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::calculate_union;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

/// A union execution plan that processes all inputs sequentially in a single partition.
///
/// Unlike `DataFusion`'s [`UnionExec`](datafusion::physical_plan::union::UnionExec) which reports N partitions for N inputs (causing
/// `CoalescePartitionsExec` to be inserted and spawn Tokio tasks), this operator
/// always reports exactly 1 partition and chains all input partition streams sequentially.
///
/// ## Use Case
/// This operator is required for non-Tokio async runtimes (e.g., custom async executors)
/// where task spawning via `JoinSet::spawn()` would panic.
///
/// ## Behavior
/// - Reports 1 output partition regardless of input partition counts
/// - Processes all partitions from all inputs sequentially in order
/// - Validates that all inputs have compatible schemas
/// - Computes common ordering properties across all inputs
#[derive(Debug)]
pub struct SequentialUnionExec {
    /// Input execution plans to union
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Schema of the output (validated to match all inputs)
    schema: SchemaRef,
    /// Cached plan properties
    properties: Arc<PlanProperties>,
}

impl SequentialUnionExec {
    /// Creates a new `SequentialUnionExec` with the given inputs.
    ///
    /// # Arguments
    /// * `inputs` - Execution plans to union. Must be non-empty and have compatible schemas.
    ///
    /// # Returns
    /// A new `SequentialUnionExec`.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `inputs` is empty
    /// - Input schemas are incompatible
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        if inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "SequentialUnionExec requires at least one input".to_string(),
            ));
        }

        let schema = inputs[0].schema();

        // Validate all schemas match
        for (i, input) in inputs.iter().enumerate().skip(1) {
            let input_schema = input.schema();
            if input_schema != schema {
                return Err(DataFusionError::Plan(format!(
                    "SequentialUnionExec schema mismatch: input 0 has schema {schema:?}, \
                     but input {i} has schema {input_schema:?}"
                )));
            }
        }

        // Compute common equivalence properties across all inputs
        let children_eqps: Vec<_> = inputs
            .iter()
            .map(|p| p.properties().equivalence_properties().clone())
            .collect();
        let eq_properties = calculate_union(children_eqps, Arc::clone(&schema))?;

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1), // KEY: Always 1 partition
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            inputs,
            schema,
            properties,
        })
    }
}

impl DisplayAs for SequentialUnionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "SequentialUnionExec")
            }
        }
    }
}

impl ExecutionPlan for SequentialUnionExec {
    fn name(&self) -> &'static str {
        "SequentialUnionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(children)?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Require single partition inputs to prevent the optimizer from inserting
        // RepartitionExec which would create multi-partition streams that deadlock
        // when polled sequentially.
        vec![Distribution::SinglePartition; self.inputs.len()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SequentialUnionExec only supports partition 0, got {partition}"
            )));
        }

        // Create all streams upfront (follows DataFusion's InterleaveExec pattern)
        let mut streams = Vec::new();
        for input in &self.inputs {
            let partition_count = input.output_partitioning().partition_count();
            for p in 0..partition_count {
                streams.push(input.execute(p, Arc::clone(&context))?);
            }
        }

        Ok(Box::pin(SequentialUnionStream {
            streams,
            current_index: 0,
            schema: Arc::clone(&self.schema),
        }))
    }
}

/// Stream that sequentially processes partitions from multiple inputs.
struct SequentialUnionStream {
    /// All input streams to process in order (created upfront)
    streams: Vec<SendableRecordBatchStream>,
    /// Index of the current stream being polled
    current_index: usize,
    /// Output schema
    schema: SchemaRef,
}

impl RecordBatchStream for SequentialUnionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for SequentialUnionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let idx = self.current_index;
            if idx >= self.streams.len() {
                return Poll::Ready(None);
            }

            match Pin::new(&mut self.streams[idx]).poll_next(cx) {
                Poll::Ready(Some(batch)) => return Poll::Ready(Some(batch)),
                Poll::Ready(None) => self.current_index += 1,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use futures::StreamExt;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn create_memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0].schema();
        MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_sequential_union_single_input() -> Result<()> {
        let batch = create_test_batch(vec![1, 2, 3]);
        let input = create_memory_exec(vec![batch.clone()]);

        let union = SequentialUnionExec::try_new(vec![input])?;

        assert_eq!(
            union.properties().output_partitioning().partition_count(),
            1
        );

        let ctx = Arc::new(TaskContext::default());
        let mut stream = union.execute(0, ctx)?;

        let result = stream.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 3);

        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_sequential_union_multiple_inputs() -> Result<()> {
        let batch1 = create_test_batch(vec![1, 2]);
        let batch2 = create_test_batch(vec![3, 4]);
        let batch3 = create_test_batch(vec![5, 6]);

        let input1 = create_memory_exec(vec![batch1]);
        let input2 = create_memory_exec(vec![batch2]);
        let input3 = create_memory_exec(vec![batch3]);

        let union = SequentialUnionExec::try_new(vec![input1, input2, input3])?;

        assert_eq!(
            union.properties().output_partitioning().partition_count(),
            1
        );

        let ctx = Arc::new(TaskContext::default());
        let mut stream = union.execute(0, ctx)?;

        // Should get all batches in order
        let r1 = stream.next().await.unwrap()?;
        assert_eq!(
            r1.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );

        let r2 = stream.next().await.unwrap()?;
        assert_eq!(
            r2.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );

        let r3 = stream.next().await.unwrap()?;
        assert_eq!(
            r3.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[5, 6]
        );

        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[test]
    fn test_sequential_union_empty_inputs_error() {
        let result = SequentialUnionExec::try_new(vec![]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one input"));
    }

    #[test]
    fn test_sequential_union_schema_mismatch_error() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema1),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema2),
            vec![Arc::new(Int64Array::from(vec![2]))],
        )
        .unwrap();

        let input1 = MemorySourceConfig::try_new_exec(&[vec![batch1]], schema1, None).unwrap();
        let input2 = MemorySourceConfig::try_new_exec(&[vec![batch2]], schema2, None).unwrap();

        let result = SequentialUnionExec::try_new(vec![input1, input2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema mismatch"));
    }

    #[tokio::test]
    async fn test_sequential_union_invalid_partition() {
        let batch = create_test_batch(vec![1]);
        let input = create_memory_exec(vec![batch]);
        let union = SequentialUnionExec::try_new(vec![input]).unwrap();

        let ctx = Arc::new(TaskContext::default());
        let result = union.execute(1, ctx);
        match result {
            Ok(_) => panic!("Expected error for invalid partition"),
            Err(e) => assert!(
                e.to_string().contains("only supports partition 0"),
                "Unexpected error: {e}"
            ),
        }
    }
}

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

//! Basic example demonstrating how to use `datafusion-index-provider`.
//!
//! This example creates an in-memory table with an age index and a department index,
//! registers it as a DataFusion table, and runs queries that leverage index-based scans.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::scalar::ScalarValue;
use datafusion_common::DataFusionError;
use datafusion_index_provider::physical_plan::exec::fetch::RecordFetchExec;
use datafusion_index_provider::physical_plan::{create_index_schema, Index};
use datafusion_index_provider::{IndexedTableProvider, RecordFetcher, UnionMode};

// ---------------------------------------------------------------------------
// 1. Implement the `Index` trait for each index type
// ---------------------------------------------------------------------------

/// A BTreeMap-based index on the `age` column. Given age predicates, it returns
/// matching primary key (id) values.
#[derive(Debug)]
struct AgeIndex {
    /// age -> list of row ids
    index: BTreeMap<i32, Vec<i32>>,
}

impl AgeIndex {
    fn new(ages: &Int32Array, ids: &Int32Array) -> Self {
        let mut index: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
        for i in 0..ages.len() {
            index.entry(ages.value(i)).or_default().push(ids.value(i));
        }
        Self { index }
    }

    fn matching_ids(&self, filters: &[Expr], limit: Option<usize>) -> Vec<u64> {
        let mut ids: BTreeSet<i32> = BTreeSet::new();
        for filter in filters {
            if let Expr::BinaryExpr(be) = filter {
                if let (Expr::Column(c), Expr::Literal(ScalarValue::Int32(Some(v)), _)) =
                    (be.left.as_ref(), be.right.as_ref())
                {
                    if c.name != "age" {
                        continue;
                    }
                    match be.op {
                        Operator::Eq => {
                            if let Some(list) = self.index.get(v) {
                                ids.extend(list);
                            }
                        }
                        Operator::Gt => {
                            ids.extend(self.index.range((v + 1)..).flat_map(|(_, l)| l))
                        }
                        Operator::GtEq => ids.extend(self.index.range(v..).flat_map(|(_, l)| l)),
                        Operator::Lt => ids.extend(self.index.range(..v).flat_map(|(_, l)| l)),
                        Operator::LtEq => ids.extend(self.index.range(..=v).flat_map(|(_, l)| l)),
                        _ => {}
                    }
                }
            }
        }
        let mut result: Vec<u64> = ids.into_iter().map(|id| id as u64).collect();
        if let Some(l) = limit {
            result.truncate(l);
        }
        result
    }
}

impl Index for AgeIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "age_index"
    }
    fn index_schema(&self) -> SchemaRef {
        create_index_schema([Field::new("id", DataType::UInt64, false)])
    }
    fn table_name(&self) -> &str {
        "employees"
    }
    fn column_name(&self) -> &str {
        "age"
    }
    fn scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let ids = self.matching_ids(filters, limit);
        let batches = if ids.is_empty() {
            vec![]
        } else {
            let col = Arc::new(UInt64Array::from(ids)) as ArrayRef;
            vec![RecordBatch::try_new(self.index_schema(), vec![col])?]
        };
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.index_schema(),
            None,
        )?))
    }
    fn statistics(&self) -> Statistics {
        Statistics::new_unknown(self.index_schema().as_ref())
    }
}

// ---------------------------------------------------------------------------
// 2. Implement the `RecordFetcher` trait
// ---------------------------------------------------------------------------

/// Fetches full rows from in-memory batches given primary key values.
struct InMemoryFetcher {
    batch: RecordBatch,
}

impl fmt::Debug for InMemoryFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemoryFetcher")
    }
}

#[async_trait]
impl RecordFetcher for InMemoryFetcher {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        let ids = index_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("expected UInt64Array for primary key column");

        // Convert 1-based ids to 0-based indices for arrow take
        let indices = Int32Array::from_iter_values(ids.iter().flatten().map(|id| (id - 1) as i32));

        let columns: Result<Vec<ArrayRef>> = self
            .batch
            .columns()
            .iter()
            .map(|col| {
                Ok(Arc::new(datafusion::arrow::compute::take(
                    col.as_ref(),
                    &indices,
                    None,
                )?) as ArrayRef)
            })
            .collect();

        Ok(RecordBatch::try_new(self.batch.schema(), columns?)?)
    }
}

// ---------------------------------------------------------------------------
// 3. Implement `IndexedTableProvider` + `TableProvider`
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct EmployeeTable {
    schema: SchemaRef,
    age_index: Arc<AgeIndex>,
    fetcher: Arc<InMemoryFetcher>,
}

#[async_trait]
impl TableProvider for EmployeeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Separate filters into index-pushable vs remaining
        let (indexed, _remaining) = self.analyze_and_optimize_filters(filters)?;

        if indexed.is_empty() {
            unimplemented!("full-table scan fallback not shown in this example");
        }

        Ok(Arc::new(RecordFetchExec::try_new(
            indexed,
            limit,
            self.fetcher.clone(),
            self.schema.clone(),
            UnionMode::Parallel,
        )?))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.supports_filters_index_pushdown(filters)
    }
}

#[async_trait]
impl IndexedTableProvider for EmployeeTable {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index + 'static>>, DataFusionError> {
        Ok(vec![self.age_index.clone()])
    }
}

// ---------------------------------------------------------------------------
// 4. Run queries
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )?;

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let ages = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let provider = EmployeeTable {
        schema: schema.clone(),
        age_index: Arc::new(AgeIndex::new(ages, ids)),
        fetcher: Arc::new(InMemoryFetcher {
            batch: batch.clone(),
        }),
    };

    let ctx = SessionContext::new();
    ctx.register_table("employees", Arc::new(provider))?;

    println!("=== Employees older than 29 (index-accelerated) ===");
    let df = ctx.sql("SELECT * FROM employees WHERE age > 29").await?;
    df.show().await?;

    println!("\n=== Employee with age = 25 ===");
    let df = ctx.sql("SELECT * FROM employees WHERE age = 25").await?;
    df.show().await?;

    Ok(())
}

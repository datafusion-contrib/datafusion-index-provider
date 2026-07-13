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

use crate::physical_plan::exec::fetch::RecordFetchExec;
use crate::physical_plan::fetcher::RecordFetcher;
use crate::physical_plan::Index;
use crate::types::{IndexFilter, IndexFilters, UnionMode};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A table provider that can be scanned using indexes.
///
/// This trait extends the [`TableProvider`] trait with additional methods
/// for scanning the table using indexes.
/// The [`IndexedTableProvider::analyze_and_optimize_filters`] and
/// [`IndexedTableProvider::create_execution_plan_with_indexes`] methods provide the main functionality
/// that can be used in your `TableProvider::scan` implementation.
#[async_trait]
pub trait IndexedTableProvider: TableProvider + Sync + Send {
    /// Returns a list of all indexes available for this table.
    /// This is the only method that must be implemented by the user.
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>>;

    /// Analyzes filters, optimizes them, and groups them by the index that can handle them.
    ///
    /// # Default implementation
    /// The default implementation groups filters by the index that supports them, based on the
    /// column they refer to. It does not perform any optimization.
    ///
    /// # Returns
    /// A tuple containing:
    /// - A list of `IndexFilter`s, where each element is a struct containing an index and a list of
    ///   expressions that can be handled by that index.
    /// - A list of expressions that cannot be handled by any index.
    fn analyze_and_optimize_filters(&self, filters: &[Expr]) -> Result<(IndexFilters, Vec<Expr>)> {
        let (indexed_filters, remaining_filters): (Vec<_>, Vec<_>) = filters
            .iter()
            .map(|expr| match self.build_index_filter(expr) {
                Ok(Some(filter)) => Ok((Some(filter), None)),
                Ok(None) => Ok((None, Some(expr.clone()))),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let indexed_filters: Vec<_> = indexed_filters.into_iter().flatten().collect();
        let remaining_filters: Vec<_> = remaining_filters.into_iter().flatten().collect();

        let final_filters = match indexed_filters.len() {
            0 => Vec::new(),
            1 => indexed_filters,
            _ => vec![IndexFilter::And(indexed_filters)],
        };

        Ok((final_filters, remaining_filters))
    }

    /// Recursively builds an `IndexFilter` tree from a given expression.
    ///
    /// This method traverses the expression tree and attempts to create a corresponding
    /// tree of `IndexFilter`s. If any part of the expression cannot be handled by an
    /// index, this method returns `Ok(None)`.
    fn build_index_filter(&self, expr: &Expr) -> Result<Option<IndexFilter>> {
        // Recursive step for AND/OR operators.
        if let Expr::BinaryExpr(be) = expr {
            let op = match be.op {
                Operator::And => |l, r| Ok(Some(IndexFilter::And(vec![l, r]))),
                Operator::Or => |l, r| Ok(Some(IndexFilter::Or(vec![l, r]))),
                _ => {
                    // Not an AND/OR, so treat as a base case.
                    return self.find_index_for_expr(expr);
                }
            };

            // Recursively build filters for the left and right sides.
            let l_filter = self.build_index_filter(be.left.as_ref())?;
            let r_filter = self.build_index_filter(be.right.as_ref())?;

            // If both sides are indexable, combine them.
            if let (Some(l), Some(r)) = (l_filter, r_filter) {
                return op(l, r);
            } else {
                // One or both sides are not indexable, so the whole expression is not.
                return Ok(None);
            }
        }

        // Base case for simple expressions (not AND/OR).
        self.find_index_for_expr(expr)
    }

    /// Finds a suitable index for a simple expression.
    fn find_index_for_expr(&self, expr: &Expr) -> Result<Option<IndexFilter>> {
        for index in self.indexes()? {
            if index.supports_predicate(expr)? {
                return Ok(Some(IndexFilter::Single {
                    index,
                    filter: expr.clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Returns the union mode to use for OR conditions in index scans.
    ///
    /// # Default implementation
    /// Returns [`UnionMode::Parallel`], which uses DataFusion's standard `UnionExec`
    /// and may spawn Tokio tasks for parallel execution.
    ///
    /// # When to override
    /// Override this method to return [`UnionMode::Sequential`] if your runtime
    /// does not support Tokio task spawning (e.g., custom async executors or
    /// single-threaded runtimes).
    fn union_mode(&self) -> UnionMode {
        UnionMode::default()
    }

    /// Returns whether the filters can be pushed down to the index.
    /// This method can be used in `TableProvider::supports_filters_pushdown`.
    ///
    /// # Default implementation
    /// The default implementation returns `TableProviderFilterPushDown::Exact` for any filter
    /// that is supported by at least one index, and `TableProviderFilterPushDown::Unsupported`
    /// otherwise.
    fn supports_filters_index_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let indexes = self.indexes()?;
        filters
            .iter()
            .map(|filter| {
                for index in &indexes {
                    if index.supports_predicate(filter)? {
                        return Ok(TableProviderFilterPushDown::Exact);
                    }
                }
                Ok(TableProviderFilterPushDown::Unsupported)
            })
            .collect()
    }

    /// Creates an execution plan that scans the table using the provided indexes.
    ///
    /// # Arguments
    /// * `indexes` - A slice of `IndexFilter` structs, each containing an index and a list of
    ///   expressions that can be handled by that index.
    /// * `_projection` - A slice of column indices to be projected. This is currently ignored.
    /// * `_remaining_filters` - A slice of expressions that cannot be handled by any index.
    /// * `limit` - An optional limit on the number of rows to return.
    /// * `schema` - The schema of the table.
    /// * `mapper` - A reference to a `RecordFetcher` that will be used to fetch records.
    ///
    /// # Returns
    /// A `Result` containing an `Arc<dyn ExecutionPlan>` that can be used to scan the table.
    async fn create_execution_plan_with_indexes(
        &self,
        indexes: &[IndexFilter],
        // TODO: Implement projection support for index-based scans
        _projection: Option<&Vec<usize>>,
        _remaining_filters: &[Expr],
        limit: Option<usize>,
        schema: SchemaRef,
        mapper: Arc<dyn RecordFetcher>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecordFetchExec::try_new(
            indexes.to_vec(),
            limit,
            Arc::clone(&mapper),
            schema.clone(),
            self.union_mode(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::Index;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::common::Statistics;
    use datafusion::datasource::TableType;
    use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
    use datafusion::prelude::{col, lit};
    use datafusion_common::{DataFusionError, Result};
    use std::any::Any;

    // Mock Index
    #[derive(Debug)]
    struct MockIndex {
        column_name: String,
        table_name: String,
    }

    impl Index for MockIndex {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "mock_index"
        }

        fn index_schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]))
        }

        fn table_name(&self) -> &str {
            &self.table_name
        }

        fn column_name(&self) -> &str {
            &self.column_name
        }

        fn scan(
            &self,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            unimplemented!("MockIndex::scan")
        }

        fn statistics(&self) -> Statistics {
            Statistics::new_unknown(&self.index_schema())
        }
    }

    // Mock IndexedTableProvider
    #[derive(Debug)]
    struct MockTableProvider {
        indexes: Vec<Arc<dyn Index>>,
    }

    impl MockTableProvider {
        fn new(indexes: Vec<Arc<dyn Index>>) -> Self {
            Self { indexes }
        }
    }

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]))
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl IndexedTableProvider for MockTableProvider {
        fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
            Ok(self.indexes.clone())
        }
    }

    #[test]
    fn test_analyze_simple_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 1);
        assert!(matches!(indexed[0], IndexFilter::Single { .. }));
        assert_eq!(remaining.len(), 0);

        Ok(())
    }

    #[test]
    fn test_analyze_no_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("b").eq(lit(1))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 0);
        assert_eq!(remaining.len(), 1);

        Ok(())
    }

    #[test]
    fn test_analyze_mixed_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 1);
        assert!(matches!(indexed[0], IndexFilter::Single { .. }));
        assert_eq!(remaining.len(), 1);

        Ok(())
    }

    #[test]
    fn test_analyze_and_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![
            Arc::new(MockIndex {
                column_name: "a".into(),
                table_name: "t".into(),
            }),
            Arc::new(MockIndex {
                column_name: "b".into(),
                table_name: "t".into(),
            }),
        ]);
        let filters = vec![col("a").eq(lit(1)).and(col("b").eq(lit(2)))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 1);
        assert!(matches!(indexed[0], IndexFilter::And { .. }));
        assert_eq!(remaining.len(), 0);

        Ok(())
    }

    #[test]
    fn test_analyze_or_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![
            Arc::new(MockIndex {
                column_name: "a".into(),
                table_name: "t".into(),
            }),
            Arc::new(MockIndex {
                column_name: "b".into(),
                table_name: "t".into(),
            }),
        ]);
        let filters = vec![col("a").eq(lit(1)).or(col("b").eq(lit(2)))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 1);
        assert!(matches!(indexed[0], IndexFilter::Or { .. }));
        assert_eq!(remaining.len(), 0);

        Ok(())
    }

    #[test]
    fn test_analyze_complex_no_pushdown() -> Result<()> {
        let provider = MockTableProvider::new(vec![Arc::new(MockIndex {
            column_name: "a".into(),
            table_name: "t".into(),
        })]);
        let filters = vec![col("a").eq(lit(1)).and(col("b").eq(lit(2)))];
        let (indexed, remaining) = provider.analyze_and_optimize_filters(&filters)?;

        assert_eq!(indexed.len(), 0);
        assert_eq!(remaining.len(), 1);

        Ok(())
    }
}

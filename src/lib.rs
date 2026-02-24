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

#![warn(missing_docs)]

//! # DataFusion Index Provider
//!
//! This crate provides a comprehensive framework for adding index-based scanning capabilities
//! to DataFusion [`datafusion::datasource::TableProvider`]s. It enables efficient query execution
//! by leveraging secondary indexes to reduce I/O and improve query performance through a
//! sophisticated two-phase execution model.
//!
//! ## Architecture Overview
//!
//! The crate implements a two-phase execution model:
//!
//! 1. **Index Phase**: Scan one or more indexes to identify primary key values matching the query filters
//! 2. **Fetch Phase**: Use the primary key values to fetch complete records from the underlying storage
//!
//! This approach is particularly effective for selective queries where indexes can significantly
//! reduce the number of rows that need to be fetched from primary storage.
//!
//! ## Core Components
//!
//! ### Index Management
//! - [`physical_plan::Index`]: Trait representing a physical index that can be scanned to retrieve primary key values
//! - [`provider::IndexedTableProvider`]: Extension of DataFusion's `TableProvider` with index discovery
//! - [`types::IndexFilter`]: Enum representing filter operations that can be pushed down to indexes
//!
//! ### Execution Engine  
//! - [`physical_plan::exec::fetch::RecordFetchExec`]: Top-level execution plan orchestrating the two phases
//! - [`physical_plan::exec::index::IndexScanExec`]: Execution plan for scanning a single index
//! - [`physical_plan::fetcher::RecordFetcher`]: Trait for fetching complete records using primary key values
//!
//! ## Query Capabilities
//!
//! The system's query capabilities are defined by the trait implementations:
//!
//! ### Index Trait Capabilities
//! Each [`physical_plan::Index`] implementation defines its query capabilities through:
//!
//! - **`supports_predicate(&self, predicate: &Expr) -> Result<bool>`**: Determines if the index can handle a specific predicate
//! - **Default implementation**: Supports any predicate that references the index's column name
//! - **Custom implementations**: Can implement sophisticated predicate analysis for complex index types
//!
//! ### IndexedTableProvider Filter Analysis
//! The [`provider::IndexedTableProvider`] trait provides comprehensive filter analysis through
//! `build_index_filter()`:
//!
//! #### Supported Expression Types
//! - **Simple predicates**: Any expression that an index's `supports_predicate()` method accepts
//! - **AND operations**: `Expr::BinaryExpr` with `Operator::And` - creates `IndexFilter::And`
//! - **OR operations**: `Expr::BinaryExpr` with `Operator::Or` - creates `IndexFilter::Or`
//! - **Nested expressions**: Recursive traversal supports arbitrarily nested AND/OR combinations
//!
//! #### Filter Processing Rules
//! 1. **Recursive traversal**: Expressions are recursively analyzed to build [`types::IndexFilter`] trees
//! 2. **All-or-nothing**: If any part of an AND/OR expression cannot be indexed, the entire expression is rejected
//! 3. **Index matching**: Each leaf predicate must match exactly one index via `supports_predicate()`
//! 4. **Automatic grouping**: Multiple root-level indexable filters are automatically wrapped in `IndexFilter::And`
//!
//! ### Supported Query Patterns
//!
//! Based on the trait design, the system supports:
//!
//! #### Basic Index Predicates
//! - Any predicate supported by the index implementation
//! - Equality conditions: `indexed_column = 'value'`
//! - Inequality conditions: `indexed_column > 100`
//! - Range queries: `indexed_column BETWEEN 10 AND 50`
//!
//! #### Logical Combinations
//! - AND across different indexes: `col_a = 1 AND col_b = 2`
//! - OR across different indexes: `col_a = 1 OR col_b = 2`
//! - Complex nested combinations: `(col_a = 1 AND col_b = 2) OR (col_a = 3 AND col_b = 4)`
//!
//! #### Multi-level Nesting
//! - Arbitrarily deep nesting supported: `((col_a = 1 OR col_a = 2) AND col_b = 3) OR (col_c = 4 AND col_d = 5)`
//!
//! ## Execution Plan Generation
//!
//! The [`physical_plan::exec::fetch::RecordFetchExec`] generates
//! optimized execution plans based on the [`types::IndexFilter`] structure:
//!
//! ### IndexFilter::Single - Direct Index Scan
//!
//! For simple conditions on a single indexed column:
//! ```text
//! RecordFetchExec
//! └── IndexScanExec (target_index)
//! ```
//!
//! ### IndexFilter::And - Index Intersection
//!
//! For conjunctive conditions across multiple indexes, the system builds a left-deep tree
//! of joins to intersect primary key values:
//! ```text
//! RecordFetchExec
//! └── Projection(PK columns)
//!     └── HashJoin/SortMergeJoin (INNER on PK columns)
//!         ├── Projection(PK columns)
//!         │   └── HashJoin/SortMergeJoin (INNER on PK columns)
//!         │       ├── IndexScanExec (col_a_index)
//!         │       └── IndexScanExec (col_b_index)
//!         └── IndexScanExec (col_c_index)
//! ```
//!
//! ### IndexFilter::Or - Union with Deduplication
//!
//! For disjunctive conditions, the system uses `UnionExec` followed by `AggregateExec`
//! for automatic primary key deduplication:
//! ```text
//! RecordFetchExec
//! └── AggregateExec (GROUP BY PK columns)
//!     └── UnionExec
//!         ├── IndexScanExec (col_a)
//!         ├── IndexScanExec (col_b)
//!         └── IndexScanExec (col_c)
//! ```
//!
//! This approach ensures that overlapping results from different indexes are automatically
//! deduplicated before record fetching.
//!
//! ## Implementation Guide
//!
//! - **Implement the Index Trait**: Create indexes that can scan and return primary key values
//! - **Implement the RecordFetcher Trait**: Define how to fetch complete records using primary key values
//! - **Implement IndexedTableProvider**: Expose available indexes and filter analysis capabilities
//! - **Update TableProvider Implementation**: Integrate index-based execution into your scan method
//!
//! ## Performance Characteristics
//!
//! ### Execution Plan Efficiency
//! - **Single index**: Direct scan with minimal overhead
//! - **AND operations**: Intersection cost scales with number of indexes and intermediate result sizes
//! - **OR operations**: Union cost includes deduplication overhead but prevents duplicate fetches
//!
//! ### Memory Usage
//! - **Index results**: Streamed through execution pipeline to minimize memory footprint
//! - **Join operations**: Hash joins require memory proportional to smaller index result set
//! - **Deduplication**: OR operations require memory to store unique primary key values during aggregation
//!
//! ### Bounded Execution
//! - **Single partition requirement**: [`physical_plan::exec::fetch::RecordFetchExec`] requires single partition input for correct result merging
//! - **Incremental emission**: Results are emitted incrementally as they become available
//! - **Bounded memory**: Execution is bounded with predictable memory usage patterns

pub mod physical_plan;
/// Index-aware [`datafusion::catalog::TableProvider`] extension trait.
pub mod provider;
/// Core types for representing index filter structures.
pub mod types;

pub use physical_plan::fetcher::RecordFetcher;
pub use physical_plan::Index;
pub use provider::IndexedTableProvider;
pub use types::{IndexFilter, IndexFilters, UnionMode};

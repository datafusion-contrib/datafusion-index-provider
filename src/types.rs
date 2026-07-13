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

//! Core type definitions for representing index-based filter operations.
//!
//! This module defines the [`IndexFilter`] enum which represents the structure of
//! filter operations that can be pushed down to indexes. These types form the
//! foundation for the query optimization and execution plan generation process.

use std::sync::Arc;

use datafusion::prelude::Expr;

use crate::physical_plan::Index;
use std::fmt;

/// Represents the structure of filter expressions that can be executed using indexes.
///
/// This enum captures the hierarchical structure of filter conditions and their
/// mapping to physical indexes. It serves as an intermediate representation between
/// SQL filter expressions and physical execution plans.
///
/// ## Execution Plan Mapping
/// Each variant maps to a specific execution strategy:
/// - `Single`: Direct index scan via `IndexScanExec`
/// - `And`: Index intersection via cascaded joins (Hash or `SortMerge` joins)
/// - `Or`: Index union via `UnionExec` + `AggregateExec` for deduplication
#[derive(Debug, Clone)]
pub enum IndexFilter {
    /// A filter condition that can be handled by a single index.
    ///
    /// This represents the base case where one index can directly answer a filter predicate.
    /// The filter expression should be compatible with the index's `supports_predicate()` method.
    Single {
        /// The index that will handle this filter
        index: Arc<dyn Index>,
        /// The filter expression to be evaluated by the index
        filter: Expr,
    },

    /// A conjunction (AND) of multiple filter conditions across different indexes.
    ///
    /// This represents scenarios where multiple indexes must be consulted and their
    /// results intersected to find rows that satisfy ALL conditions. The execution
    /// strategy builds a left-deep tree of joins to progressively narrow the result set.
    And(Vec<IndexFilter>),

    /// A disjunction (OR) of multiple filter conditions across different indexes.
    ///
    /// This represents scenarios where any of several conditions can be satisfied.
    /// The execution strategy unions results from all indexes and deduplicates on
    /// all primary key columns to ensure each row appears only once in the final result.
    Or(Vec<IndexFilter>),
}

/// A collection of [`IndexFilter`]s representing a complete index-based query plan.
///
/// This type represents the complete set of index operations needed to satisfy a query's
/// filter conditions. It is typically the output of `analyze_and_optimize_filters()` and
/// serves as input to `create_execution_plan_with_indexes()` for physical plan generation.
pub type IndexFilters = Vec<IndexFilter>;

/// Controls how union operations combine multiple index scans for OR conditions.
///
/// When executing disjunctive (OR) queries across multiple indexes, results must be
/// combined. This enum allows choosing between parallel and sequential execution
/// strategies based on runtime requirements.
#[derive(Debug, Clone, Copy, Default)]
pub enum UnionMode {
    /// Use standard `UnionExec` with parallel execution.
    ///
    /// This mode spawns Tokio tasks via `JoinSet::spawn()` to process partitions
    /// concurrently. Best for Tokio-based runtimes with good parallelism support.
    ///
    /// **Warning**: This mode will panic in non-Tokio async runtimes.
    #[default]
    Parallel,

    /// Use `SequentialUnionExec` with single-threaded sequential execution.
    ///
    /// This mode processes all input partitions sequentially in a single stream
    /// without spawning any tasks. Required for non-Tokio runtimes such as
    /// custom async executors that don't support Tokio task spawning.
    Sequential,
}

impl fmt::Display for IndexFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexFilter::Single { index, .. } => write!(f, "{}", index.name()),
            IndexFilter::And(filters) => {
                write!(f, "(")?;
                for (i, filter) in filters.iter().enumerate() {
                    if i > 0 {
                        write!(f, " AND ")?;
                    }
                    write!(f, "{filter}")?;
                }
                write!(f, ")")
            }
            IndexFilter::Or(filters) => {
                write!(f, "(")?;
                for (i, filter) in filters.iter().enumerate() {
                    if i > 0 {
                        write!(f, " OR ")?;
                    }
                    write!(f, "{filter}")?;
                }
                write!(f, ")")
            }
        }
    }
}

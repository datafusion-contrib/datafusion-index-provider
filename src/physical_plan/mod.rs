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

//! Physical plan traits and operators for index-based scanning.
//!
//! This module provides the core [`Index`] trait, which defines the interface
//! for a physical index, as well as various `ExecutionPlan` implementations
//! that use indexes to scan and fetch data.
pub mod exec;
pub mod fetcher;
pub mod joins;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{Result, Statistics};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{utils::expr_to_columns, Expr};
use datafusion_common::DataFusionError;
use std::any::Any;
use std::collections::HashSet;
use std::fmt;

use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::{Partitioning, PlanProperties};
use std::sync::Arc;

/// Creates a `PlanProperties` for a scan that is potentially ordered by the primary key columns.
///
/// # Arguments
/// * `schema` - The schema of the output plan. All columns are treated as primary key columns.
/// * `ordered` - Whether the output is ordered by the primary key columns.
pub fn create_plan_properties_for_pk_scan(schema: SchemaRef, ordered: bool) -> Arc<PlanProperties> {
    let mut eq_properties = EquivalenceProperties::new(schema.clone());
    if ordered {
        let sort_exprs: Vec<PhysicalSortExpr> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                PhysicalSortExpr::new_default(Arc::new(PhysicalColumn::new(field.name(), i))).asc()
            })
            .collect();
        eq_properties.add_ordering(sort_exprs);
    }
    Arc::new(PlanProperties::new(
        eq_properties,
        Partitioning::RoundRobinBatch(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

/// Creates a schema for an index with the specified fields.
///
/// All columns in the schema are treated as forming the composite row identifier
/// (primary key). For single-column primary keys, pass a single field.
///
/// # Arguments
/// * `fields` - The fields that form the composite primary key.
///
/// # Examples
/// ```
/// use datafusion::arrow::datatypes::{DataType, Field};
/// use datafusion_index_provider::physical_plan::create_index_schema;
///
/// // Single-column primary key
/// let schema = create_index_schema([Field::new("id", DataType::UInt64, false)]);
///
/// // Composite primary key
/// let schema = create_index_schema([
///     Field::new("tenant_id", DataType::Utf8, false),
///     Field::new("row_id", DataType::UInt64, false),
/// ]);
/// ```
pub fn create_index_schema(fields: impl IntoIterator<Item = Field>) -> SchemaRef {
    Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()))
}

/// Represents a physical index that can be scanned to find row IDs.
///
/// An `Index` is a physical operator that, given a set of predicates, can
/// efficiently produce a stream of row identifiers (e.g., row numbers, or
/// primary keys) that satisfy those predicates.
pub trait Index: fmt::Debug + Send + Sync + 'static {
    /// Returns the index as [`Any`] for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns the name of this index.
    fn name(&self) -> &str;

    /// Returns the schema of the index output.
    ///
    /// All columns in this schema form the composite row identifier (primary key).
    /// The downstream pipeline uses these columns for joining (AND operations),
    /// deduplication (OR operations), and fetching records.
    fn index_schema(&self) -> SchemaRef;

    /// Returns the name of the table this index belongs to.
    fn table_name(&self) -> &str;

    /// Returns the name of the column this index primarily covers.
    ///
    /// This is used by the default implementation of [`Self::supports_predicate`] to
    /// perform a simple check for predicate support.
    fn column_name(&self) -> &str;

    /// A fast check to see if this index can potentially satisfy the predicate.
    ///
    /// # Default implementation
    /// The default implementation checks if any column referenced in the predicate
    /// matches the value of [`Self::column_name`].
    fn supports_predicate(&self, predicate: &Expr) -> Result<bool> {
        let mut columns = HashSet::new();
        expr_to_columns(predicate, &mut columns)?;
        Ok(columns.iter().any(|col| col.name == self.column_name()))
    }

    /// Returns indication if the index is ordered by the primary key columns.
    ///
    /// # Default implementation
    /// The default implementation returns `false`.
    fn is_ordered(&self) -> bool {
        false
    }

    /// Creates a stream of primary key values that satisfy the given filters.
    ///
    /// The output of this stream MUST have a schema matching [`Self::index_schema()`].
    fn scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>;

    /// Provides statistics for the index data.
    fn statistics(&self) -> Statistics;
}

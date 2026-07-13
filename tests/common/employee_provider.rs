use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_index_provider::physical_plan::exec::fetch::RecordFetchExec;
use datafusion_index_provider::physical_plan::Index;
use datafusion_index_provider::provider::IndexedTableProvider;
use datafusion_index_provider::types::{IndexFilter, UnionMode};

use crate::common::age_index::AgeIndex;
use crate::common::department_index::DepartmentIndex;
use crate::common::record_fetcher::BatchMapper;

/// A simple in-memory table provider that stores employee data with an age index
#[derive(Debug)]
pub struct EmployeeTableProvider {
    schema: SchemaRef,
    age_index: Arc<AgeIndex>,
    department_index: Arc<DepartmentIndex>,
    mapper: Arc<BatchMapper>,
    union_mode: UnionMode,
}

impl Default for EmployeeTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmployeeTableProvider {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        // Row identifiers matching the index schema's UInt64 primary key column.
        let pk_ids = UInt64Array::from(vec![1u64, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let age_array = Int32Array::from(vec![25, 30, 35, 28, 32]);
        let department_array = StringArray::from(vec![
            "Engineering",
            "Sales",
            "Marketing",
            "Engineering",
            "Sales",
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array.clone()),
                Arc::new(name_array.clone()),
                Arc::new(age_array.clone()),
                Arc::new(department_array.clone()),
            ],
        )
        .unwrap();

        EmployeeTableProvider {
            schema,
            age_index: Arc::new(AgeIndex::new(&age_array, &pk_ids)),
            department_index: Arc::new(DepartmentIndex::new(&department_array, &pk_ids)),
            mapper: Arc::new(BatchMapper::new(vec![batch])),
            union_mode: UnionMode::Parallel,
        }
    }

    /// Set the union mode for OR condition handling.
    #[allow(dead_code)]
    pub fn with_union_mode(mut self, mode: UnionMode) -> Self {
        self.union_mode = mode;
        self
    }
}

#[async_trait]
impl TableProvider for EmployeeTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (indexed_filters, remaining_filters) = self.analyze_and_optimize_filters(filters)?;

        if indexed_filters.is_empty() {
            return self.scan_with_table(state, projection, &remaining_filters, limit);
        }

        self.scan_with_indexes(
            state,
            projection,
            &remaining_filters,
            limit,
            &indexed_filters,
        )
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.supports_filters_index_pushdown(filters)
    }
}

#[async_trait]
impl IndexedTableProvider for EmployeeTableProvider {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index + 'static>>, DataFusionError> {
        Ok(vec![self.age_index.clone(), self.department_index.clone()])
    }

    fn union_mode(&self) -> UnionMode {
        self.union_mode
    }
}

impl EmployeeTableProvider {
    fn scan_with_indexes(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _remaining_filters: &[Expr],
        limit: Option<usize>,
        indexes: &[IndexFilter],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecordFetchExec::try_new(
            indexes.to_vec(),
            limit,
            self.mapper.clone(),
            self.schema.clone(),
            self.union_mode,
        )?))
    }

    /// Builds an `ExecutionPlan` to scan the table.
    /// This is the main entry point for scanning the table without indexes.
    /// It is designed to be called by `scan_with_indexes_or_fallback`.
    fn scan_with_table(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _remaining_filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
}

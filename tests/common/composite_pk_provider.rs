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
use datafusion_index_provider::physical_plan::create_index_schema;
use datafusion_index_provider::physical_plan::exec::fetch::RecordFetchExec;
use datafusion_index_provider::physical_plan::Index;
use datafusion_index_provider::provider::IndexedTableProvider;
use datafusion_index_provider::types::UnionMode;

use super::composite_pk_age_index::CompositePkAgeIndex;
use super::composite_pk_department_index::CompositePkDeptIndex;
use super::composite_pk_fetcher::CompositePkFetcher;

pub fn composite_pk_schema() -> SchemaRef {
    create_index_schema([
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("employee_id", DataType::UInt64, false),
    ])
}

/// A multi-tenant employee table with composite PK (tenant_id, employee_id)
///
/// | tenant_id | employee_id | name    | age | department  |
/// |-----------|-------------|---------|-----|-------------|
/// | acme      | 1           | Alice   | 25  | Engineering |
/// | acme      | 2           | Bob     | 30  | Sales       |
/// | acme      | 3           | Charlie | 35  | Marketing   |
/// | globex    | 1           | David   | 28  | Engineering |
/// | globex    | 2           | Eve     | 32  | Sales       |
#[derive(Debug)]
pub struct MultiTenantEmployeeProvider {
    schema: SchemaRef,
    age_index: Arc<CompositePkAgeIndex>,
    dept_index: Arc<CompositePkDeptIndex>,
    fetcher: Arc<CompositePkFetcher>,
    union_mode: UnionMode,
}

impl Default for MultiTenantEmployeeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiTenantEmployeeProvider {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("employee_id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        let tenant_ids = StringArray::from(vec!["acme", "acme", "acme", "globex", "globex"]);
        let employee_ids = UInt64Array::from(vec![1, 2, 3, 1, 2]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let ages = Int32Array::from(vec![25, 30, 35, 28, 32]);
        let departments = StringArray::from(vec![
            "Engineering",
            "Sales",
            "Marketing",
            "Engineering",
            "Sales",
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(tenant_ids.clone()),
                Arc::new(employee_ids.clone()),
                Arc::new(names),
                Arc::new(ages.clone()),
                Arc::new(departments.clone()),
            ],
        )
        .unwrap();

        Self {
            schema,
            age_index: Arc::new(CompositePkAgeIndex::new(&ages, &tenant_ids, &employee_ids)),
            dept_index: Arc::new(CompositePkDeptIndex::new(
                &departments,
                &tenant_ids,
                &employee_ids,
            )),
            fetcher: Arc::new(CompositePkFetcher::new(batch)),
            union_mode: UnionMode::Parallel,
        }
    }

    #[allow(dead_code)]
    pub fn with_union_mode(mut self, mode: UnionMode) -> Self {
        self.union_mode = mode;
        self
    }
}

#[async_trait]
impl TableProvider for MultiTenantEmployeeProvider {
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
        let (indexed_filters, _remaining) = self.analyze_and_optimize_filters(filters)?;

        Ok(Arc::new(RecordFetchExec::try_new(
            indexed_filters,
            limit,
            self.fetcher.clone(),
            self.schema.clone(),
            self.union_mode,
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
impl IndexedTableProvider for MultiTenantEmployeeProvider {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index + 'static>>, DataFusionError> {
        Ok(vec![self.age_index.clone(), self.dept_index.clone()])
    }

    fn union_mode(&self) -> UnionMode {
        self.union_mode
    }
}

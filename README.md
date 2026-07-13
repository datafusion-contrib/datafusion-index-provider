# DataFusion Index Provider

A library that extends [Apache DataFusion](https://github.com/apache/datafusion) with index-based query acceleration for `TableProvider` implementations.

## Overview

This crate implements a two-phase execution model for index-accelerated queries:

1. **Index Phase**: Scan one or more secondary indexes to identify primary key values matching filter predicates
2. **Fetch Phase**: Use primary key values to fetch complete records from underlying storage

This approach reduces I/O by limiting data retrieval to rows that satisfy query predicates, particularly effective for selective queries (typically < 10% of rows).

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
datafusion-index-provider = "0.1.0"
```

## Core Concepts

### Index Trait

Implement the [`Index`](src/physical_plan/mod.rs) trait to define how your index scans for matching primary key values:

```rust
use datafusion_index_provider::physical_plan::Index;

impl Index for MyIndex {
    fn name(&self) -> &str { "my_index" }
    fn column_name(&self) -> &str { "indexed_column" }
    fn index_schema(&self) -> SchemaRef {
        // Schema whose columns form the composite primary key
        // e.g. create_index_schema([Field::new("id", DataType::UInt64, false)])
    }
    fn table_name(&self) -> &str { "my_table" }

    fn scan(&self, filters: &[Expr], limit: Option<usize>)
        -> Result<SendableRecordBatchStream> {
        // Return RecordBatch stream with primary key columns matching the filters
    }

    fn statistics(&self) -> Statistics { /* index statistics */ }
}
```

### RecordFetcher Trait

Implement [`RecordFetcher`](src/physical_plan/fetcher.rs) to define how complete records are retrieved using primary key values:

```rust
use datafusion_index_provider::physical_plan::fetcher::RecordFetcher;

#[async_trait]
impl RecordFetcher for MyTable {
    async fn fetch(&self, index_batch: RecordBatch) -> Result<RecordBatch> {
        // index_batch contains primary key columns as defined by index_schema()
        // Return complete records for those primary keys
    }
}
```

### IndexedTableProvider Trait

Implement [`IndexedTableProvider`](src/provider.rs) on your `TableProvider` to expose available indexes:

```rust
use datafusion_index_provider::provider::IndexedTableProvider;

impl IndexedTableProvider for MyTable {
    fn indexes(&self) -> Result<Vec<Arc<dyn Index>>> {
        Ok(vec![
            Arc::new(MyAgeIndex::new()),
            Arc::new(MyDepartmentIndex::new()),
        ])
    }
}
```

### Integration with TableProvider

Use the provided analysis and execution plan methods in your `TableProvider::scan`:

```rust
#[async_trait]
impl TableProvider for MyTable {
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Analyze which filters can use indexes
        let (indexable, remaining) = self.analyze_and_optimize_filters(filters)?;

        if !indexable.is_empty() {
            // Create index-accelerated execution plan
            self.create_execution_plan_with_indexes(
                &indexable,
                projection,
                &remaining,
                limit,
                self.schema(),
                Arc::new(self.clone()) as Arc<dyn RecordFetcher>,
            ).await
        } else {
            // Fall back to regular table scan
            self.create_full_scan_plan(projection, filters, limit).await
        }
    }
}
```

## Query Optimization

### Filter Analysis

The `analyze_and_optimize_filters` method recursively analyzes filter expressions to build an [`IndexFilter`](src/types.rs) tree:

- **Single predicates**: Matched to indexes via `Index::supports_predicate()`
- **AND expressions**: All sub-expressions must be indexable; creates `IndexFilter::And`
- **OR expressions**: All sub-expressions must be indexable; creates `IndexFilter::Or`
- **Nested combinations**: Supports arbitrary nesting depth

**All-or-nothing policy**: If any part of an AND/OR expression cannot be indexed, the entire expression is treated as non-indexable.

### Supported Query Patterns

```sql
-- Single index scan
SELECT * FROM employees WHERE age = 30;

-- Multi-index intersection (AND)
SELECT * FROM employees WHERE age > 25 AND department = 'Engineering';

-- Multi-index union (OR) with automatic deduplication
SELECT * FROM employees WHERE age < 25 OR department = 'Sales';

-- Complex nested expressions
SELECT * FROM employees
WHERE (age > 30 AND department = 'Engineering')
   OR (age < 25 AND department = 'Sales');
```

## Execution Plans

The library generates optimized physical plans based on filter structure:

### Single Index

```
RecordFetchExec
└── IndexScanExec(age_index, [age = 30])
```

### AND - Index Intersection

Uses Hash or SortMerge joins to intersect primary key values:

```
RecordFetchExec
└── Projection(PK columns)
    └── HashJoin(on: PK columns)
        ├── IndexScanExec(age_index, [age > 25])
        └── IndexScanExec(dept_index, [department = 'Engineering'])
```

**Join Selection**:
- **SortMergeJoin**: When both indexes return ordered primary keys (`Index::is_ordered()`)
- **HashJoin**: When inputs are unordered (builds hash table from left side)

### OR - Union with Deduplication

Uses UnionExec + AggregateExec to deduplicate overlapping primary key values:

```
RecordFetchExec
└── AggregateExec(GROUP BY PK columns)
    └── UnionExec
        ├── IndexScanExec(age_index, [age < 25])
        └── IndexScanExec(dept_index, [department = 'Sales'])
```

This prevents duplicate record fetches when primary key values appear in multiple index results.

## Reference Implementation

See [`tests/common/`](tests/common/) for complete working examples:

**Single-column primary key:**
- [`age_index.rs`](tests/common/age_index.rs): BTreeMap-based index supporting range queries (Eq, Lt, Gt, LtEq, GtEq)
- [`department_index.rs`](tests/common/department_index.rs): HashMap-based index for equality queries
- [`employee_provider.rs`](tests/common/employee_provider.rs): Complete TableProvider with IndexedTableProvider integration
- [`record_fetcher.rs`](tests/common/record_fetcher.rs): RecordFetcher implementation using in-memory data

**Composite primary key:**
- [`composite_pk_age_index.rs`](tests/common/composite_pk_age_index.rs): Index with composite PK (tenant_id, employee_id)
- [`composite_pk_department_index.rs`](tests/common/composite_pk_department_index.rs): Department index with composite PK
- [`composite_pk_provider.rs`](tests/common/composite_pk_provider.rs): Multi-tenant TableProvider with composite PK
- [`composite_pk_fetcher.rs`](tests/common/composite_pk_fetcher.rs): RecordFetcher for composite PK tables

Integration tests in [`tests/integration_tests.rs`](tests/integration_tests.rs) and [`tests/composite_pk_tests.rs`](tests/composite_pk_tests.rs) demonstrate query scenarios including deeply nested AND/OR combinations with both single and composite primary keys.

## Limitations

1. **Projection pushdown**: Not currently supported for indexed scans (fetches all columns)
2. **Remaining filters**: Non-indexed filters applied after record fetch, not during index scan
3. **Partitioning**: Index scans produce single partition output

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run with logging
RUST_LOG=debug cargo test

# Run integration tests only
cargo test --test integration_tests
```

The test suite includes:
- 27 unit tests covering execution plan generation and streaming
- 36 integration tests covering query scenarios from simple to deeply nested, with both single and composite primary keys

## Compatibility

- **DataFusion**: 54.x
- **Rust**: 1.88+ (MSRV)
- **Arrow**: Compatible with DataFusion's Arrow version

## Architecture Details

For in-depth technical documentation, see:
- [Crate documentation](https://docs.rs/datafusion-index-provider) - Generated rustdoc
- [`src/lib.rs`](src/lib.rs) - Architecture overview and query capabilities
- [`src/provider.rs`](src/provider.rs) - Filter analysis implementation
- [`src/physical_plan/exec/fetch.rs`](src/physical_plan/exec/fetch.rs) - Execution plan generation

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

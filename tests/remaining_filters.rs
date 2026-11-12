//! End-to-end assertions that filters not handled by any index are still applied
//! after the index scan, even when they reach the provider directly (bypassing pushdown).
//!
//! Based on [`common::employee_provider`]

mod common;

use datafusion::arrow::array::Int32Array;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::prelude::{col, lit};
use datafusion_index_provider::types::UnionMode;

/// Scans `employees` with `filters` attached to the `TableScan` and returns the ids.
async fn scan_ids(filters: Vec<Expr>, fetch: Option<usize>, mode: UnionMode) -> Result<Vec<i32>> {
    scan_ids_projected(filters, fetch, mode, None).await
}

/// Like [`scan_ids`], with `projection` (indices into the table schema) on the `TableScan`.
async fn scan_ids_projected(
    filters: Vec<Expr>,
    fetch: Option<usize>,
    mode: UnionMode,
    projection: Option<Vec<usize>>,
) -> Result<Vec<i32>> {
    let expected_columns = projection.as_ref().map(Vec::len);
    let (ctx, provider) = common::setup_test_env_with_provider(mode);
    let plan = LogicalPlanBuilder::scan_with_filters_fetch(
        "employees",
        provider_as_source(provider),
        projection,
        filters,
        fetch,
    )?
    .build()?;
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;

    let mut ids = Vec::new();
    for batch in &batches {
        if let Some(expected) = expected_columns {
            assert_eq!(batch.num_columns(), expected, "projection not applied");
        }
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        ids.extend(column.values().iter().copied());
    }
    Ok(ids)
}

#[tokio::test]
async fn remaining_filter_is_applied_after_index_scan() {
    // `age < 31` is indexed: {Alice, Bob, David}. The OR mixes an indexed and a
    // non-indexed column, so it is left as a remaining filter: only Bob matches both.
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        let ids = scan_ids(
            vec![
                col("age").lt(lit(31)),
                col("age").eq(lit(30)).or(col("name").eq(lit("Eve"))),
            ],
            None,
            mode,
        )
        .await
        .expect("query should succeed");

        assert_eq!(ids, vec![2], "{mode:?}");
    }
}

#[tokio::test]
async fn limit_is_applied_after_remaining_filter() {
    // `age > 20` is indexed and yields every row, Alice first. `name = 'Eve'` is a
    // remaining filter: applying the limit before it would return Alice (no filter)
    // or nothing (limit then filter).
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        let ids = scan_ids(
            vec![col("age").gt(lit(20)), col("name").eq(lit("Eve"))],
            Some(1),
            mode,
        )
        .await
        .expect("query should succeed");

        assert_eq!(ids, vec![5], "{mode:?}");
    }
}

#[tokio::test]
async fn remaining_filter_may_reference_unprojected_column() {
    // Only `id` (index 0) is projected, but the remaining filter `name = 'Eve'` needs `name`,
    // so the projection must be applied above the filter.
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        let ids = scan_ids_projected(
            vec![col("age").gt(lit(20)), col("name").eq(lit("Eve"))],
            None,
            mode,
            Some(vec![0]),
        )
        .await
        .expect("query should succeed");

        assert_eq!(ids, vec![5], "{mode:?}");
    }
}

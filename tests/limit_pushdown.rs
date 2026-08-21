//! End-to-end assertions for `LIMIT` that check row fetched (`LocalLimitExec` optimization)
//!
//! Based on [`common::employee_provider`]

mod common;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion_index_provider::types::UnionMode;

/// Runs `sql` and returns number of row & number of rows fetched
async fn run(sql: &str, mode: UnionMode) -> Result<(usize, usize)> {
    let (ctx, provider) = common::setup_test_env_with_provider(mode);
    let batches = ctx.sql(sql).await?.collect().await?;
    let returned = batches.iter().map(RecordBatch::num_rows).sum();
    Ok((returned, provider.rows_fetched()))
}

/// Assert that sql return limit rows and that limit ro<s where fetched
async fn assert_fetch_bounded_by(sql: &str, limit: usize, mode: UnionMode) {
    let (returned, fetched) = run(&format!("{sql} LIMIT {limit}"), mode)
        .await
        .expect("query should succeed");

    assert_eq!(
        returned, limit,
        "{sql} ({mode:?}) should return {limit} rows"
    );
    assert!(
        fetched <= limit,
        "{sql} ({mode:?}) should have fetched less or equal {limit} but fetched {fetched} rows"
    );
}

#[tokio::test]
async fn single_index_limit_bounds_fetch() {
    // {3, 5}.
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        assert_fetch_bounded_by("SELECT * FROM employees WHERE age > 30", 1, mode).await;
    }
}

#[tokio::test]
async fn and_limit_bounds_fetch() {
    // {2,5}
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        assert_fetch_bounded_by(
            "SELECT * FROM employees WHERE age < 35 AND age >= 30",
            1,
            mode,
        )
        .await;
    }
}

#[tokio::test]
async fn or_limit_bounds_fetch() {
    // {1,3,4,5}
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        assert_fetch_bounded_by(
            "SELECT * FROM employees WHERE age < 30 OR age > 30",
            3,
            mode,
        )
        .await;
    }
}

#[tokio::test]
async fn or_of_ands_limit_bounds_fetch() {
    // {1,4,5}
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        assert_fetch_bounded_by(
            "SELECT * FROM employees \
             WHERE (age < 30 AND department = 'Engineering') \
                OR (age > 30 AND department = 'Sales')",
            2,
            mode,
        )
        .await;
    }
}

#[tokio::test]
async fn no_limit_fetches_every_matching_row() {
    // no limits {1,3,4,5}
    for mode in [UnionMode::Parallel, UnionMode::Sequential] {
        let (returned, fetched) = run("SELECT * FROM employees WHERE age < 30 OR age > 30", mode)
            .await
            .expect("query should succeed");

        assert_eq!(returned, 4);
        assert_eq!(fetched, 4);
    }
}

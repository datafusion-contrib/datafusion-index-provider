//! End-to-end check that `EXPLAIN ANALYZE` reports the metrics of the index path operators.
//!
//! Based on [`common::employee_provider`]

mod common;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::error::Result;

/// Runs `EXPLAIN ANALYZE` on `sql` and returns the plan annotated with metrics.
async fn explain_analyze(sql: &str) -> Result<String> {
    explain_analyze_at_level(sql, "dev").await
}

/// Same as [`explain_analyze`] with `datafusion.explain.analyze_level` set to `level`.
async fn explain_analyze_at_level(sql: &str, level: &str) -> Result<String> {
    let ctx = common::setup_test_env();
    ctx.sql(&format!("SET datafusion.explain.analyze_level = '{level}'"))
        .await?
        .collect()
        .await?;
    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await?
        .collect()
        .await?;

    let mut plan = String::new();
    for batch in &batches {
        let column = batch.column_by_name("plan").expect("plan column");
        for row in 0..batch.num_rows() {
            plan.push_str(&array_value_to_string(column, row)?);
            plan.push('\n');
        }
    }
    Ok(plan)
}

/// Returns the line of `plan` describing `operator`.
fn operator_line<'a>(plan: &'a str, operator: &str) -> &'a str {
    plan.lines()
        .find(|line| line.trim_start().starts_with(operator))
        .unwrap_or_else(|| panic!("{operator} not found in plan:\n{plan}"))
}

#[tokio::test]
async fn explain_analyze_reports_index_path_metrics() -> Result<()> {
    // `age > 30` matches {3, 5} through the age index, returned as one batch.
    let plan = explain_analyze("SELECT * FROM employees WHERE age > 30").await?;

    let scan = operator_line(&plan, "IndexScanExec");
    for metric in ["output_rows=2", "elapsed_compute="] {
        assert!(scan.contains(metric), "missing {metric}: {scan}");
    }

    let fetch = operator_line(&plan, "RecordFetchExec");
    for metric in [
        "output_rows=2",
        "fetch_calls=1",
        "input_rows=2",
        "fetch_time=",
    ] {
        assert!(fetch.contains(metric), "missing {metric}: {fetch}");
    }
    Ok(())
}

#[tokio::test]
async fn explain_analyze_summary_level_keeps_fetch_metrics() -> Result<()> {
    let plan =
        explain_analyze_at_level("SELECT * FROM employees WHERE age > 30", "summary").await?;

    let fetch = operator_line(&plan, "RecordFetchExec");
    for metric in ["fetch_calls=1", "fetch_time="] {
        assert!(fetch.contains(metric), "missing {metric}: {fetch}");
    }
    assert!(!fetch.contains("input_rows"), "dev metric leaked: {fetch}");
    Ok(())
}

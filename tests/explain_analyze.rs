//! End-to-end check that `EXPLAIN ANALYZE` reports the metrics of the index path operators.
//!
//! Based on [`common::employee_provider`]

mod common;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::error::Result;

/// Runs `EXPLAIN ANALYZE` on `sql` and returns the plan annotated with metrics.
async fn explain_analyze(sql: &str) -> Result<String> {
    let ctx = common::setup_test_env();
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
        "fetched_rows=2",
        "fetch_time=",
    ] {
        assert!(fetch.contains(metric), "missing {metric}: {fetch}");
    }
    Ok(())
}

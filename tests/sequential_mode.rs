//! `UnionMode::Sequential` must stay single-threaded and deterministic so that the index
//! plan can run inside a deterministic simulator (no `tokio::spawn`, no extra threads).
//!
//! Queries run on `futures::executor::block_on` outside any tokio runtime, so any spawn
//! panics. The physical plan must not contain operators that spawn or merge partitions,
//! and repeated runs must return identical batches in the same order.
//!
//! Based on [`common::employee_provider`]

mod common;

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_index_provider::types::UnionMode;

use common::employee_provider::EmployeeTableProvider;

/// Operators that spawn tasks or merge several partitions. Names are matched exactly, so
/// `SequentialUnionExec` is allowed while `UnionExec` is not.
const FORBIDDEN_OPERATORS: [&str; 4] = [
    "RepartitionExec",
    "CoalescePartitionsExec",
    "SortPreservingMergeExec",
    "UnionExec",
];

const RUNS: usize = 5;

const QUERIES: [(&str, &str); 7] = [
    ("single", "SELECT * FROM employees WHERE age > 28"),
    (
        "or",
        "SELECT * FROM employees WHERE age < 28 OR department = 'Sales'",
    ),
    (
        "and",
        "SELECT * FROM employees WHERE age >= 28 AND department = 'Engineering'",
    ),
    (
        "mixed_and_or",
        "SELECT * FROM employees \
         WHERE (age < 30 AND department = 'Engineering') \
            OR (age > 30 AND department = 'Sales')",
    ),
    (
        "limit",
        "SELECT * FROM employees WHERE age < 30 OR age > 30 LIMIT 2",
    ),
    (
        "projection",
        "SELECT name, age FROM employees WHERE age < 28 OR department = 'Sales'",
    ),
    (
        "remaining_filter",
        "SELECT * FROM employees \
         WHERE (age < 33 OR department = 'Marketing') AND name <> 'Bob'",
    ),
];

/// Plans `sql` with `target_partitions = 1` in `Sequential` mode and executes it, without
/// any tokio runtime. Returns the displayed physical plan and the output batches.
fn run(sql: &str) -> Result<(String, Vec<RecordBatch>)> {
    futures::executor::block_on(async {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let provider = EmployeeTableProvider::new().with_union_mode(UnionMode::Sequential);
        ctx.register_table("employees", Arc::new(provider))?;

        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let displayed = displayable(plan.as_ref()).indent(true).to_string();
        let batches = collect(plan, ctx.task_ctx()).await?;
        Ok((displayed, batches))
    })
}

/// Operator names of a displayed plan, one per line (the text before the first `:` or space).
fn operator_names(plan: &str) -> Vec<&str> {
    plan.lines()
        .filter_map(|line| line.trim().split([':', ' ']).next())
        .filter(|name| !name.is_empty())
        .collect()
}

#[test]
fn sequential_plans_are_single_threaded_and_deterministic() {
    for (name, sql) in QUERIES {
        let (plan, expected) = run(sql).unwrap_or_else(|e| panic!("{name}: query failed: {e}"));

        let operators = operator_names(&plan);
        for forbidden in FORBIDDEN_OPERATORS {
            assert!(
                !operators.contains(&forbidden),
                "{name}: plan must not contain {forbidden}:\n{plan}"
            );
        }
        assert!(
            operators.contains(&"RecordFetchExec"),
            "{name}: the index plan should be used:\n{plan}"
        );
        assert!(
            expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
            "{name}: query should return rows"
        );

        for attempt in 1..RUNS {
            let (again_plan, again) = run(sql).unwrap();
            assert_eq!(again_plan, plan, "{name}: plan differs on run {attempt}");
            assert_eq!(again, expected, "{name}: output differs on run {attempt}");
        }
    }
}

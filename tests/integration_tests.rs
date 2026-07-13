mod common;

use common::{setup_test_env, setup_test_env_sequential};
use datafusion_common::assert_batches_sorted_eq;

// +----+---------+-----+-------------+
// | id | name    | age | department  |
// +----+---------+-----+-------------+
// | 1  | Alice   | 25  | Engineering |
// | 2  | Bob     | 30  | Sales       |
// | 3  | Charlie | 35  | Marketing   |
// | 4  | David   | 28  | Engineering |
// | 5  | Eve     | 32  | Sales       |
// +----+---------+-----+-------------+

#[tokio::test]
async fn test_employee_table_filter_age_equal() {
    let test_cases: Vec<(i32, &[&str])> = vec![
        (
            25,
            &[
                "+----+-------+-----+-------------+",
                "| id | name  | age | department  |",
                "+----+-------+-----+-------------+",
                "| 1  | Alice | 25  | Engineering |",
                "+----+-------+-----+-------------+",
            ],
        ),
        (
            30,
            &[
                "+----+------+-----+------------+",
                "| id | name | age | department |",
                "+----+------+-----+------------+",
                "| 2  | Bob  | 30  | Sales      |",
                "+----+------+-----+------------+",
            ],
        ),
        (
            35,
            &[
                "+----+---------+-----+------------+",
                "| id | name    | age | department |",
                "+----+---------+-----+------------+",
                "| 3  | Charlie | 35  | Marketing  |",
                "+----+---------+-----+------------+",
            ],
        ),
        (
            28,
            &[
                "+----+-------+-----+-------------+",
                "| id | name  | age | department  |",
                "+----+-------+-----+-------------+",
                "| 4  | David | 28  | Engineering |",
                "+----+-------+-----+-------------+",
            ],
        ),
        (
            32,
            &[
                "+----+------+-----+------------+",
                "| id | name | age | department |",
                "+----+------+-----+------------+",
                "| 5  | Eve  | 32  | Sales      |",
                "+----+------+-----+------------+",
            ],
        ),
    ];

    for (age, expected) in test_cases {
        let ctx = setup_test_env().await;

        let results = ctx
            .sql(&format!("SELECT * FROM employees WHERE age = {age}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);
    }
}

#[tokio::test]
async fn test_employee_table_filter_age_gt() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age > 30")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+------------+",
            "| id | name    | age | department |",
            "+----+---------+-----+------------+",
            "| 3  | Charlie | 35  | Marketing  |",
            "| 5  | Eve     | 32  | Sales      |",
            "+----+---------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_age_lt() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age < 30")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_department_equal() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE department = 'Sales'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+------+-----+------------+",
            "| id | name | age | department |",
            "+----+------+-----+------------+",
            "| 2  | Bob  | 30  | Sales      |",
            "| 5  | Eve  | 32  | Sales      |",
            "+----+------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_age_and_department_no_result() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age > 30 AND department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = results
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_employee_table_filter_age_and_department() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age <= 30 AND department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_age_or_department() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age <= 30 OR department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_multiple_or_on_age() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age = 35 OR age = 32")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_multiple_or_on_age_unique() {
    let ctx = setup_test_env().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age < 30 OR age > 30")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 4  | David   | 28  | Engineering |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_complex_query() {
    let ctx = setup_test_env().await;

    // (age < 30 AND department = 'Engineering') OR (age > 30 AND department = 'Sales')
    // Alice(1,25,Eng), David(4,28,Eng) from first AND
    // Eve(5,32,Sales) from second AND
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE (age < 30 AND department = 'Engineering') OR (age > 30 AND department = 'Sales')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_or_with_overlapping_conditions() {
    let ctx = setup_test_env().await;

    // age = 25 OR age < 29 → Alice(1,25,Eng), David(4,28,Eng) (deduped)
    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age < 29")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_extremely_complex_nested_query() {
    let ctx = setup_test_env().await;

    // (age = 25 OR age = 28) AND department = 'Engineering' AND age < 40 AND age > 20
    // → Alice(1,25,Eng), David(4,28,Eng)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age = 25 OR age = 28)
            AND
            department = 'Engineering'
            AND
            age < 40
            AND
            age > 20",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_deeply_nested_and_or_combinations() {
    let ctx = setup_test_env().await;

    // ((age >= 25 AND age <= 30) AND (department = 'Engineering' OR department = 'Sales'))
    // AND ((age != 27 AND age != 29) OR (department = 'Engineering' AND age < 29))
    // → Alice(1,25,Eng), David(4,28,Eng)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (
                (age >= 25 AND age <= 30)
                AND
                (department = 'Engineering' OR department = 'Sales')
            )
            AND
            (
                (age != 27 AND age != 29)
                OR
                (department = 'Engineering' AND age < 29)
            )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_complex_and_or_mixed() {
    let ctx = setup_test_env().await;

    // (age > 30 AND department = 'Engineering') OR department = 'Sales'
    // No Engineering employees > 30
    // → Bob(2,30,Sales), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age > 30 AND department = 'Engineering') OR department = 'Sales'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+------+-----+------------+",
            "| id | name | age | department |",
            "+----+------+-----+------------+",
            "| 2  | Bob  | 30  | Sales      |",
            "| 5  | Eve  | 32  | Sales      |",
            "+----+------+-----+------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_multiple_and_conditions_in_or() {
    let ctx = setup_test_env().await;

    // (age >= 25 AND department = 'Engineering') OR (age >= 30 AND department = 'Sales')
    // → Alice(1,25,Eng), David(4,28,Eng), Bob(2,30,Sales), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age >= 25 AND department = 'Engineering') OR
            (age >= 30 AND department = 'Sales')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_and_or_with_simple_conditions() {
    let ctx = setup_test_env().await;

    // (age < 28 AND department = 'Engineering') OR department = 'Sales' OR age = 28
    // → Alice(1,25,Eng), Bob(2,30,Sales), David(4,28,Eng), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age < 28 AND department = 'Engineering') OR
            department = 'Sales' OR
            age = 28",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_nested_and_or_schema_normalization() {
    let ctx = setup_test_env().await;

    // ((age > 25 AND age < 35) AND department = 'Engineering') OR (department = 'Sales' OR department = 'Marketing')
    // → David(4,28,Eng), Bob(2,30,Sales), Charlie(3,35,Marketing), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            ((age > 25 AND age < 35) AND department = 'Engineering') OR
            (department = 'Sales' OR department = 'Marketing')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 2  | Bob     | 30  | Sales       |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 4  | David   | 28  | Engineering |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_triple_or_with_and_conditions() {
    let ctx = setup_test_env().await;

    // (age = 25 AND department = 'Engineering') OR (age > 30 AND department = 'Sales') OR department = 'Marketing'
    // → Alice(1,25,Eng), Eve(5,32,Sales), Charlie(3,35,Marketing)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age = 25 AND department = 'Engineering') OR
            (age > 30 AND department = 'Sales') OR
            department = 'Marketing'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_all_and_conditions_in_or() {
    let ctx = setup_test_env().await;

    // All OR branches are AND conditions
    // → All 5 employees
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age >= 25 AND department = 'Engineering') OR
            (age >= 30 AND department = 'Sales') OR
            (age >= 28 AND department = 'Marketing')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 2  | Bob     | 30  | Sales       |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 4  | David   | 28  | Engineering |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_complex_and_or_deduplication() {
    let ctx = setup_test_env().await;

    // (age >= 30 AND department = 'Engineering') OR (department = 'Engineering' AND age > 25)
    // → David(4,28,Eng) only
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age >= 30 AND department = 'Engineering') OR
            (department = 'Engineering' AND age > 25)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_four_way_or_schema_stress_test() {
    let ctx = setup_test_env().await;

    // Four-way OR → all 5 employees
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age = 25 AND department = 'Engineering') OR
            (age >= 30 AND department = 'Sales') OR
            department = 'Marketing' OR
            age = 28",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 2  | Bob     | 30  | Sales       |",
            "| 3  | Charlie | 35  | Marketing   |",
            "| 4  | David   | 28  | Engineering |",
            "| 5  | Eve     | 32  | Sales       |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_deeply_nested_and_or_schema_normalization() {
    let ctx = setup_test_env().await;

    // ((age > 25 AND age < 30) OR (age > 30 AND age < 35)) AND (department = 'Engineering' OR department = 'Sales')
    // → David(4,28,Eng), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            ((age > 25 AND age < 30) OR (age > 30 AND age < 35)) AND
            (department = 'Engineering' OR department = 'Sales')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_employee_table_filter_edge_case_single_and_in_or() {
    let ctx = setup_test_env().await;

    // age > 30 AND department = 'Engineering' → no results
    let results = ctx
        .sql("SELECT * FROM employees WHERE age > 30 AND department = 'Engineering'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = results
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_employee_table_filter_all_simple_or_conditions() {
    let ctx = setup_test_env().await;

    // department = 'Engineering' OR department = 'Sales' OR age = 25 OR age = 32
    // → Alice(1,25,Eng), Bob(2,30,Sales), David(4,28,Eng), Eve(5,32,Sales) (deduped)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            department = 'Engineering' OR
            department = 'Sales' OR
            age = 25 OR
            age = 32",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

// =============================================================================
// Sequential Union Mode Tests
// =============================================================================
// These tests verify that UnionMode::Sequential works correctly by using
// SequentialUnionExec instead of the parallel UnionExec for OR conditions.

#[tokio::test]
async fn test_sequential_union_mode_simple_or() {
    let ctx = setup_test_env_sequential().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age = 35")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 3  | Charlie | 35  | Marketing   |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_sequential_union_mode_multiple_or() {
    let ctx = setup_test_env_sequential().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age = 30 OR age = 35")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+---------+-----+-------------+",
            "| id | name    | age | department  |",
            "+----+---------+-----+-------------+",
            "| 1  | Alice   | 25  | Engineering |",
            "| 2  | Bob     | 30  | Sales       |",
            "| 3  | Charlie | 35  | Marketing   |",
            "+----+---------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_sequential_union_mode_mixed_indexes() {
    let ctx = setup_test_env_sequential().await;

    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR department = 'Sales'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_sequential_union_mode_complex_and_or() {
    let ctx = setup_test_env_sequential().await;

    // (age >= 25 AND department = 'Engineering') OR (age >= 30 AND department = 'Sales')
    // → Alice(1,25,Eng), David(4,28,Eng), Bob(2,30,Sales), Eve(5,32,Sales)
    let results = ctx
        .sql(
            "SELECT * FROM employees WHERE
            (age >= 25 AND department = 'Engineering') OR
            (age >= 30 AND department = 'Sales')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 2  | Bob   | 30  | Sales       |",
            "| 4  | David | 28  | Engineering |",
            "| 5  | Eve   | 32  | Sales       |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#[tokio::test]
async fn test_sequential_union_mode_deduplication() {
    let ctx = setup_test_env_sequential().await;

    // age = 25 OR age < 29 → Alice(1,25,Eng), David(4,28,Eng) (deduped)
    let results = ctx
        .sql("SELECT * FROM employees WHERE age = 25 OR age < 29")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_sorted_eq!(
        [
            "+----+-------+-----+-------------+",
            "| id | name  | age | department  |",
            "+----+-------+-----+-------------+",
            "| 1  | Alice | 25  | Engineering |",
            "| 4  | David | 28  | Engineering |",
            "+----+-------+-----+-------------+",
        ],
        &results
    );
}

#![allow(dead_code)]

pub mod age_index;
pub mod composite_pk_age_index;
pub mod composite_pk_department_index;
pub mod composite_pk_fetcher;
pub mod composite_pk_provider;
pub mod department_index;
pub mod employee_provider;
pub mod record_fetcher;

use composite_pk_provider::MultiTenantEmployeeProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_index_provider::physical_plan::Index;
use datafusion_index_provider::types::UnionMode;
use employee_provider::EmployeeTableProvider;
use std::sync::Arc;

/// Returns whether `expr` has the shape `column op literal`, with `op` in `ops` and a literal
/// accepted by `literal`. This is the only shape the fixture indexes' `scan` implementations
/// evaluate, so their `supports_predicate` must reject everything else.
pub fn is_column_op_literal(
    expr: &Expr,
    column: &str,
    ops: &[Operator],
    literal: impl Fn(&ScalarValue) -> bool,
) -> bool {
    let Expr::BinaryExpr(be) = expr else {
        return false;
    };
    matches!(be.left.as_ref(), Expr::Column(c) if c.name == column)
        && ops.contains(&be.op)
        && matches!(be.right.as_ref(), Expr::Literal(v, _) if literal(v))
}

/// Fails if `filters` contains a predicate that `index` does not support, instead of letting
/// `scan` silently ignore it and return wrong rows. `supports_predicate` should keep such
/// predicates from ever reaching `scan`.
pub fn ensure_supported(index: &dyn Index, filters: &[Expr]) -> Result<()> {
    for filter in filters {
        if !index.supports_predicate(filter)? {
            return Err(DataFusionError::Plan(format!(
                "{} cannot evaluate filter: {filter}",
                index.name()
            )));
        }
    }
    Ok(())
}

/// Helper function to setup test environment with parallel union mode (default).
pub fn setup_test_env() -> SessionContext {
    setup_test_env_with_mode(UnionMode::Parallel)
}

/// Helper function to setup test environment with sequential union mode.
pub fn setup_test_env_sequential() -> SessionContext {
    setup_test_env_with_mode(UnionMode::Sequential)
}

/// Helper function to setup test environment with a specific union mode.
fn setup_test_env_with_mode(mode: UnionMode) -> SessionContext {
    setup_test_env_with_provider(mode).0
}

/// Helper function to setup test environment with a specific union mode and returns the provider
/// along the context.
pub fn setup_test_env_with_provider(
    mode: UnionMode,
) -> (SessionContext, Arc<EmployeeTableProvider>) {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let ctx = SessionContext::new();

    let provider = Arc::new(EmployeeTableProvider::new().with_union_mode(mode));
    ctx.register_table("employees", provider.clone()).unwrap();

    (ctx, provider)
}

/// Helper function to setup composite PK test environment with parallel union mode (default).
pub fn setup_composite_pk_test_env() -> SessionContext {
    setup_composite_pk_test_env_with_mode(UnionMode::Parallel)
}

/// Helper function to setup composite PK test environment with a specific union mode.
fn setup_composite_pk_test_env_with_mode(mode: UnionMode) -> SessionContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let ctx = SessionContext::new();

    let provider = MultiTenantEmployeeProvider::new().with_union_mode(mode);
    ctx.register_table("employees", Arc::new(provider)).unwrap();

    ctx
}

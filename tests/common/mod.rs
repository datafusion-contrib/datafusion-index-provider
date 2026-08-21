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
use datafusion_index_provider::types::UnionMode;
use employee_provider::EmployeeTableProvider;
use std::sync::Arc;

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

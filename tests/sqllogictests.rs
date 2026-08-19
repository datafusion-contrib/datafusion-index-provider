// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! sqllogictest driver for the index provider.
//!
//! Each `.slt` file under `tests/slt/` is executed against a `SessionContext`
//! built by [`context_for`], selected from the file name. The custom
//! [`IndexedTableProvider`](datafusion_index_provider::provider::IndexedTableProvider)
//! implementations are registered programmatically, then the SQL logic tests
//! drive them exactly like the previous Rust integration tests did.

mod common;

use std::path::Path;

use datafusion::execution::context::SessionContext;
use datafusion_sqllogictest::DataFusion;
use indicatif::ProgressBar;
use sqllogictest::Runner;

/// Builds the `SessionContext` a given `.slt` file expects, keyed by file stem.
fn context_for(stem: &str) -> SessionContext {
    match stem {
        "employees" | "employees_limit" => common::setup_test_env(),
        "employees_sequential" | "employees_limit_sequential" => {
            common::setup_test_env_sequential()
        }
        "composite_pk" => common::setup_composite_pk_test_env(),
        other => panic!("no context factory registered for slt file `{other}`"),
    }
}

#[tokio::test]
async fn sqllogictests() {
    let slt_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");

    let mut paths: Vec<_> = std::fs::read_dir(&slt_dir)
        .expect("tests/slt directory should exist")
        .map(|entry| entry.expect("readable dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "slt"))
        .collect();
    paths.sort();

    assert!(!paths.is_empty(), "no .slt files found in {slt_dir:?}");

    for path in paths {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("slt file has a valid stem")
            .to_owned();
        let ctx = context_for(&stem);

        let relative_path = path.clone();
        let mut runner = Runner::new(|| {
            let ctx = ctx.clone();
            let relative_path = relative_path.clone();
            async move { Ok(DataFusion::new(ctx, relative_path, ProgressBar::hidden())) }
        });

        // `SLT_COMPLETE=1 cargo test` fills in the expected result of every
        // query from the live output, instead of asserting against it.
        if std::env::var_os("SLT_COMPLETE").is_some() {
            runner
                .update_test_file(
                    &path,
                    " ",
                    sqllogictest::default_validator,
                    sqllogictest::default_normalizer,
                    sqllogictest::default_column_validator,
                )
                .await
                .unwrap_or_else(|e| panic!("failed to complete {}:\n{e}", path.display()));
        } else {
            runner
                .run_file_async(&path)
                .await
                .unwrap_or_else(|e| panic!("sqllogictest failures in {}:\n{e}", path.display()));
        }
    }
}

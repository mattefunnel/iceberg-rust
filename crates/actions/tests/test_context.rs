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

//! Smoke test for the shared test context.

mod common;

use common::TestContext;

#[tokio::test]
async fn test_context_creates_table_and_appends() {
    let ctx = TestContext::new("smoke").await;

    // Table should exist and have no snapshots initially
    let table = ctx.load_table().await;
    assert!(table.metadata().current_snapshot().is_none());

    // Append one data file; should create a snapshot
    ctx.append_data_file().await;
    let table = ctx.load_table().await;
    assert!(table.metadata().current_snapshot().is_some());

    // Append more data files in a second snapshot
    ctx.append_data_files(3).await;
    let table = ctx.load_table().await;
    assert_eq!(table.metadata().snapshots().count(), 2);
}

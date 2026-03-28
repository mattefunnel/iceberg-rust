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

//! Smoke test against a local Polaris catalog at `http://localhost:8181/api/catalog`.
//!
//! **Prerequisites:**
//!   CATALOG_NAME=funnel-transform-local-iceberg \
//!     podman compose -f site/content/guides/quickstart/docker-compose.yml up -d
//!
//! **Run with:**
//!   cargo test -p iceberg-actions --test polaris_smoke_test -- --ignored

#[tokio::test]
#[ignore = "requires local Polaris running on port 8181"]
async fn test_maintenance_against_polaris() {
    // 1. Build REST catalog pointing at local Polaris
    //    URI: http://localhost:8181/api/catalog
    //    Warehouse: funnel-transform-local-iceberg
    //    Credential: root:s3cr3t
    //
    // 2. Load an existing table (or create a test one)
    // 3. Run ExpireSnapshots, RewriteManifests, RemoveOrphanFiles (dry_run)
    // 4. Assert no errors

    todo!("Implement after verifying local Polaris setup — validates REST catalog code path")
}

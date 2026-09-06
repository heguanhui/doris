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

suite("test_info_schema_table_reader") {
    // Test the unified information_schema table reader path
    // (ExternalInfoSchemaTable -> LogicalFileScan -> InfoSchemaScanNode)

    // ---- Test 1: BE_LOCAL path (backend_tablets) ----
    qt_bt_count """select count(*) from information_schema.backend_tablets;"""

    qt_bt_columns """select BE_ID, TABLET_ID, REPLICA_ID, PARTITION_ID,
        TABLET_LOCAL_SIZE, TABLET_REMOTE_SIZE, VERSION_COUNT, SEGMENT_COUNT
        from information_schema.backend_tablets order by BE_ID, TABLET_ID limit 10;"""

    // ---- Test 2: FE_METADATA path (tables) ----
    qt_tbl_count """select count(*) from information_schema.tables;"""

    qt_tbl_info """select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        from information_schema.tables
        where TABLE_SCHEMA = 'information_schema'
        order by TABLE_NAME limit 10;"""

    // ---- Test 3: Predicate pushdown on tables ----
    qt_tbl_filter """select TABLE_NAME, TABLE_TYPE
        from information_schema.tables
        where TABLE_SCHEMA = 'information_schema' and TABLE_NAME = 'tables';"""

    // ---- Test 4: EXPLAIN shows VInfoSchemaScanNode ----
    qt_explain_bt """EXPLAIN SELECT * FROM information_schema.backend_tablets LIMIT 5;"""
    qt_explain_tbl """EXPLAIN SELECT * FROM information_schema.tables LIMIT 5;"""
}

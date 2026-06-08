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

suite("test_iceberg_truncate_table", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_truncate_table"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}",
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """drop database if exists test_truncate_db force"""
    sql """create database test_truncate_db"""
    sql """use test_truncate_db;"""

    sql """set enable_fallback_to_original_planner=false;"""

    // Test 1: Truncate unpartitioned table
    String unpartitioned_table = "truncate_unpartitioned_tbl"
    sql """drop table if exists ${unpartitioned_table}"""
    sql """
    CREATE TABLE ${unpartitioned_table} (
        id INT,
        name STRING
    );
    """
    sql """INSERT INTO ${unpartitioned_table} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"""
    order_qt_unpartitioned_before """SELECT * FROM ${unpartitioned_table} ORDER BY id"""

    sql """TRUNCATE TABLE ${unpartitioned_table}"""
    order_qt_unpartitioned_after """SELECT * FROM ${unpartitioned_table} ORDER BY id"""

    // Insert again to verify table is still usable after truncate
    sql """INSERT INTO ${unpartitioned_table} VALUES (4, 'David')"""
    order_qt_unpartitioned_reuse """SELECT * FROM ${unpartitioned_table} ORDER BY id"""

    // Test 2: Truncate partitioned table (full truncate)
    String partitioned_table = "truncate_partitioned_tbl"
    sql """drop table if exists ${partitioned_table}"""
    sql """
    CREATE TABLE ${partitioned_table} (
        id INT,
        name STRING,
        category STRING
    ) PARTITION BY LIST(category) ();
    """
    sql """INSERT INTO ${partitioned_table} VALUES (1, 'Alice', 'A'), (2, 'Bob', 'B'), (3, 'Charlie', 'C')"""
    order_qt_partitioned_before """SELECT * FROM ${partitioned_table} ORDER BY id"""

    sql """TRUNCATE TABLE ${partitioned_table}"""
    order_qt_partitioned_after """SELECT * FROM ${partitioned_table} ORDER BY id"""

    // Insert again to verify table is still usable after truncate
    sql """INSERT INTO ${partitioned_table} VALUES (4, 'David', 'D')"""
    order_qt_partitioned_reuse """SELECT * FROM ${partitioned_table} ORDER BY id"""

    // Test 3: Truncate table with multiple inserts
    String multi_insert_table = "truncate_multi_insert_tbl"
    sql """drop table if exists ${multi_insert_table}"""
    sql """
    CREATE TABLE ${multi_insert_table} (
        id INT,
        name STRING
    );
    """
    sql """INSERT INTO ${multi_insert_table} VALUES (1, 'Alice')"""
    sql """INSERT INTO ${multi_insert_table} VALUES (2, 'Bob')"""
    sql """INSERT INTO ${multi_insert_table} VALUES (3, 'Charlie')"""
    order_qt_multi_insert_before """SELECT * FROM ${multi_insert_table} ORDER BY id"""

    sql """TRUNCATE TABLE ${multi_insert_table}"""
    order_qt_multi_insert_after """SELECT * FROM ${multi_insert_table} ORDER BY id"""

    // Verify table is still usable
    sql """INSERT INTO ${multi_insert_table} VALUES (4, 'David')"""
    order_qt_multi_insert_reuse """SELECT * FROM ${multi_insert_table} ORDER BY id"""

    // Cleanup
    sql """drop database if exists test_truncate_db force"""
    sql """drop catalog if exists ${catalog_name}"""
}

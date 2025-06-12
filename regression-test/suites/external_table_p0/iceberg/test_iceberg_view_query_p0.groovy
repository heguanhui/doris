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

suite("test_iceberg_view_query_p0", "p0,external,iceberg,external_docker,external_docker_iceberg") {

    String enableIcebergTest = context.config.otherConfigs.get("enableIcebergTest")
    if (enableIcebergTest == null || !enableIcebergTest.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    try {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        //create iceberg hms catalog
        String iceberg_catalog_name = "test_iceberg_view_query_p0"
        sql """drop catalog if exists ${iceberg_catalog_name}"""
        sql """create catalog if not exists ${iceberg_catalog_name} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            'use_meta_cache' = 'true'
        );"""

        //using database and close planner fallback
        sql """use `${iceberg_catalog_name}`.`test_db`"""
        sql """set enable_fallback_to_original_planner=false;"""

        // run all suites
        sql """select * from view_with_partitioned_table"""
        sql """select * from view_with_unpartitioned_table"""
        sql """select * from view_with_partitioned_column"""
        sql """select count(*) from view_with_partitioned_table"""
        sql """select count(*) from view_with_unpartitioned_table"""
        sql """select count(*) from view_with_partitioned_column"""
        sql """select col1,col2,col3,col4 from view_with_partitioned_table"""
        sql """select col5 from view_with_partitioned_table"""

        sql """explain verbose select * from view_with_partitioned_table"""
        sql """explain verbose select * from view_with_unpartitioned_table"""
        sql """explain verbose select * from view_with_partitioned_column"""
        sql """explain verbose select count(*) from view_with_partitioned_table"""
        sql """explain verbose select count(*) from view_with_unpartitioned_table"""
        sql """explain verbose select count(*) from view_with_partitioned_column"""
        sql """explain verbose select col1,col2,col3,col4 from view_with_partitioned_table"""
        sql """explain verbose select col5 from view_with_partitioned_table"""

        sql """show create table view_with_partitioned_table"""
        sql """show create table view_with_unpartitioned_table"""
        sql """show create table view_with_partitioned_column"""
        sql """show create view view_with_partitioned_table"""
        sql """show create view view_with_unpartitioned_table"""
        sql """show create view view_with_partitioned_column"""

        sql """describe view_with_partitioned_table"""
        sql """describe view_with_unpartitioned_table"""
        sql """describe view_with_partitioned_column"""

        sql """select * from view_with_partitioned_table FOR TIME AS OF '2025-06-11 20:17:01' order by col1 limit 10"""
        sql """select * from view_with_partitioned_table FOR VERSION AS OF 3106988132043095748 order by col1 limit 10"""
        sql """select count(*) from view_with_partitioned_table FOR TIME AS OF '2025-06-11 20:17:01'""" // 5
        sql """select count(*) from view_with_partitioned_table FOR VERSION AS OF 3106988132043095748""" // 5

        sql """select * from view_with_unpartitioned_table FOR TIME AS OF '2025-06-11 20:17:01' order by col1 limit 10"""
        sql """select * from view_with_unpartitioned_table FOR VERSION AS OF 3106988132043095748 order by col1 limit 10"""
        sql """select count(*) from view_with_unpartitioned_table FOR TIME AS OF '2025-06-11 20:17:01'""" // 5
        sql """select count(*) from view_with_unpartitioned_table FOR VERSION AS OF 3106988132043095748""" // 5

        sql """select * from view_with_partitioned_column FOR TIME AS OF '2025-06-11 20:17:01' order by col5 limit 10"""
        sql """select * from view_with_partitioned_column FOR VERSION AS OF 3106988132043095748 order by col5 limit 10"""
        sql """select count(*) from view_with_partitioned_column FOR TIME AS OF '2025-06-11 20:17:01'""" // 5
        sql """select count(*) from view_with_partitioned_column FOR VERSION AS OF 3106988132043095748""" // 5

        sql """drop view view_with_partitioned_table"""
        sql """drop view view_with_unpartitioned_table"""
        sql """drop view view_with_partitioned_column"""

        sql """drop catalog if exists ${iceberg_catalog_name}"""
    } finally {
    }
}

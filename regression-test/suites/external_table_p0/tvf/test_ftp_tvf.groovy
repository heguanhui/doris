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

suite("test_ftp_tvf", "p0,external,external_docker") {
    if (!enableFTPTest()) {
        logger.info("skip ftp tvf test")
        return
    }

    String ftpUri = context.config.otherConfigs.get("ftpUri", "ftp://localhost:21/")
    String ftpUser = context.config.otherConfigs.get("ftpUser", "anonymous")
    String ftpPassword = context.config.otherConfigs.get("ftpPassword", "")

    // csv
    qt_sql01 """
        SELECT *
        FROM ftp(
            "uri" = "${ftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        )
        ORDER BY c1 limit 10;
    """

    qt_sql02 """
        SELECT count(*)
        FROM ftp(
            "uri" = "${ftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        );
    """

    // desc function
    qt_sql03 """
        desc function
        ftp(
            "uri" = "${ftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        );
    """

    // parquet
    qt_sql04 """
        SELECT *
        FROM ftp(
            "uri" = "${ftpUri}/data/test.parquet",
            "format" = "parquet",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        )
        ORDER BY id limit 10;
    """

    qt_sql05 """
        desc function
        ftp(
            "uri" = "${ftpUri}/data/test.parquet",
            "format" = "parquet",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        );
    """

    // json
    qt_sql06 """
        SELECT count(*)
        FROM ftp(
            "uri" = "${ftpUri}/data/test.json",
            "format" = "json",
            "strip_outer_array" = "true",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        );
    """

    // csv with csv_schema
    qt_sql07 """
        SELECT *
        FROM ftp(
            "uri" = "${ftpUri}/data/test_schema.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "k1:int;k2:string;k3:double",
            "ftp.user" = "${ftpUser}",
            "ftp.password" = "${ftpPassword}"
        )
        ORDER BY k1 limit 10;
    """

    // invalid uri should fail
    test {
        sql """
            SELECT * FROM ftp(
                "uri" = "http://example.com/data/file.csv",
                "format" = "csv"
            );
        """
        exception "Invalid ftp url"
    }
}

def enableFTPTest() {
    return context.config.otherConfigs.get("enableFTPTest", "false").toString() == "true"
}

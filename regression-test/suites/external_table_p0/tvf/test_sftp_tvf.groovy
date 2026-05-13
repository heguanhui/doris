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

suite("test_sftp_tvf", "p0,external,external_docker") {
    if (!enableSFTPTest()) {
        logger.info("skip sftp tvf test")
        return
    }

    String sftpUri = context.config.otherConfigs.get("sftpUri", "sftp://localhost:22/")
    String sftpUser = context.config.otherConfigs.get("sftpUser", "testuser")
    String sftpPassword = context.config.otherConfigs.get("sftpPassword", "testpass")
    String sftpSshKey = context.config.otherConfigs.get("sftpSshKey", "")

    // csv with password auth
    qt_sql01 """
        SELECT *
        FROM sftp(
            "uri" = "${sftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        )
        ORDER BY c1 limit 10;
    """

    qt_sql02 """
        SELECT count(*)
        FROM sftp(
            "uri" = "${sftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        );
    """

    // desc function
    qt_sql03 """
        desc function
        sftp(
            "uri" = "${sftpUri}/data/test.csv",
            "format" = "csv",
            "column_separator" = ",",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        );
    """

    // parquet
    qt_sql04 """
        SELECT *
        FROM sftp(
            "uri" = "${sftpUri}/data/test.parquet",
            "format" = "parquet",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        )
        ORDER BY id limit 10;
    """

    qt_sql05 """
        desc function
        sftp(
            "uri" = "${sftpUri}/data/test.parquet",
            "format" = "parquet",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        );
    """

    // json
    qt_sql06 """
        SELECT count(*)
        FROM sftp(
            "uri" = "${sftpUri}/data/test.json",
            "format" = "json",
            "strip_outer_array" = "true",
            "sftp.user" = "${sftpUser}",
            "sftp.password" = "${sftpPassword}"
        );
    """

    // ssh key auth (if configured)
    if (sftpSshKey != "") {
        qt_sql07 """
            SELECT *
            FROM sftp(
                "uri" = "${sftpUri}/data/test.csv",
                "format" = "csv",
                "column_separator" = ",",
                "sftp.user" = "${sftpUser}",
                "sftp.ssh_key" = "${sftpSshKey}"
            )
            ORDER BY c1 limit 10;
        """
    }

    // invalid uri should fail
    test {
        sql """
            SELECT * FROM sftp(
                "uri" = "http://example.com/data/file.csv",
                "format" = "csv"
            );
        """
        exception "Invalid sftp url"
    }
}

def enableSFTPTest() {
    return context.config.otherConfigs.get("enableSFTPTest", "false").toString() == "true"
}

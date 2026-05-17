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
suite("test_drop_row_policy_batch") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName1 = "drop_row_policy_batch_t1"
    def tableName2 = "drop_row_policy_batch_t2"
    def user1 = 'batch_drop_user1'
    def user2 = 'batch_drop_user2'
    def role1 = 'batch_drop_role1'
    def role2 = 'batch_drop_role2'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    def cloudMode = isCloudMode()

    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql """
        CREATE TABLE ${tableName1} (
            `k` INT,
            `v` INT
        ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName1} VALUES (1,1), (2,2), (3,3)"

    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
        CREATE TABLE ${tableName2} (
            `k` INT,
            `v` INT
        ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName2} VALUES (1,1), (2,2), (3,3)"

    sql "DROP USER IF EXISTS ${user1}"
    sql "DROP USER IF EXISTS ${user2}"
    sql "CREATE USER ${user1} IDENTIFIED BY '123abc!@#'"
    sql "CREATE USER ${user2} IDENTIFIED BY '123abc!@#'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName1} TO ${user1}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName1} TO ${user2}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName2} TO ${user1}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName2} TO ${user2}"

    sql "DROP ROLE IF EXISTS ${role1}"
    sql "DROP ROLE IF EXISTS ${role2}"
    sql "CREATE ROLE ${role1}"
    sql "CREATE ROLE ${role2}"
    sql "GRANT ${role1} TO ${user1}"
    sql "GRANT ${role2} TO ${user2}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName1} TO ROLE ${role1}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName2} TO ROLE ${role2}"

    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user1}""";
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user2}""";
    }

    sql 'sync'

    // test1: backward compatible - single target drop with FOR user
    sql "DROP ROW POLICY IF EXISTS test_policy_compat ON ${dbName}.${tableName1} FOR ${user1}"

    sql """
        CREATE ROW POLICY test_policy_compat ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user1} USING (k = 1)
    """
    sql 'sync'
    def result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    sql "DROP ROW POLICY test_policy_compat ON ${dbName}.${tableName1} FOR ${user1}"
    sql 'sync'
    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    // test2: drop by multiple users on same table
    sql """
        CREATE ROW POLICY policy_u1 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY policy_u2 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user2} USING (k = 2)
    """
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    def result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result2.size())

    sql "DROP ROW POLICY policy_u1 ON ${dbName}.${tableName1} FOR ${user1}, ${user2}"
    sql "DROP ROW POLICY policy_u2 ON ${dbName}.${tableName1} FOR ${user1}, ${user2}"
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result2.size())

    // test3: drop by multiple roles on same table
    sql """
        CREATE ROW POLICY policy_r1 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ROLE ${role1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY policy_r2 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ROLE ${role2} USING (k = 2)
    """
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result2.size())

    sql "DROP ROW POLICY policy_r1 ON ${dbName}.${tableName1} FOR ROLE ${role1}, ${role2}"
    sql "DROP ROW POLICY policy_r2 ON ${dbName}.${tableName1} FOR ROLE ${role1}, ${role2}"
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result2.size())

    // test4: drop all policies for a user across tables
    sql """
        CREATE ROW POLICY policy_cross_t1 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY policy_cross_t2 ON ${dbName}.${tableName2}
        AS RESTRICTIVE TO ${user1} USING (k = 2)
    """
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName2}"
    }
    assertEquals(1, result1.size())

    sql "DROP ROW POLICY FOR ${user1}"
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName2}"
    }
    assertEquals(3, result1.size())

    // test5: drop all policies for a role across tables
    sql """
        CREATE ROW POLICY policy_role_t1 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ROLE ${role1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY policy_role_t2 ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ROLE ${role2} USING (k = 2)
    """
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result2.size())

    sql "DROP ROW POLICY FOR ROLE ${role1}, ${role2}"
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result2.size())

    // test6: IF EXISTS with no matching policies should not throw
    sql "DROP ROW POLICY IF EXISTS nonexistent_policy ON ${dbName}.${tableName1} FOR ${user1}"
    sql "DROP ROW POLICY IF EXISTS FOR ROLE ${role1}"

    // test7: drop without any condition should fail
    test {
        sql "DROP ROW POLICY"
        exception "requires at least one of"
    }

    // test8: duplicate user should fail
    test {
        sql "DROP ROW POLICY IF EXISTS test_policy ON ${dbName}.${tableName1} FOR ${user1}, ${user1}"
        exception "Duplicate user"
    }

    // test9: duplicate role should fail
    test {
        sql "DROP ROW POLICY IF EXISTS test_policy ON ${dbName}.${tableName1} FOR ROLE ${role1}, ${role1}"
        exception "Duplicate role"
    }

    // test10: nonexistent user should fail
    test {
        sql "DROP ROW POLICY IF EXISTS test_policy ON ${dbName}.${tableName1} FOR nonexistent_user_xyz"
        exception "user not exist"
    }

    // test11: nonexistent role should fail
    test {
        sql "DROP ROW POLICY IF EXISTS test_policy ON ${dbName}.${tableName1} FOR ROLE nonexistent_role_xyz"
        exception "role not exist"
    }

    // test12: drop by policy name on table without FOR clause (drop all bindings of that policy on that table)
    sql """
        CREATE ROW POLICY shared_policy ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY shared_policy ON ${dbName}.${tableName1}
        AS RESTRICTIVE TO ${user2} USING (k = 2)
    """
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(1, result2.size())

    sql "DROP ROW POLICY shared_policy ON ${dbName}.${tableName1}"
    sql 'sync'

    result1 = connect(user1, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result1.size())

    result2 = connect(user2, '123abc!@#', url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(3, result2.size())
}

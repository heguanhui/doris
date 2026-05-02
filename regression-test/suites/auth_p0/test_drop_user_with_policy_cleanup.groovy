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

import org.junit.Assert;

suite("test_drop_user_with_policy_cleanup", "p0,auth,policy") {
    String testUser = 'test_drop_policy_user'
    String testPwd = 'C123_567p'
    String dbName = 'test_drop_user_with_policy_cleanup_db'
    String tableName = 'test_drop_user_with_policy_cleanup_table'

    // Setup test environment
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""

    sql """CREATE TABLE ${tableName} (  
        id INT,  
        name VARCHAR(50),  
        age INT  
    ) DISTRIBUTED BY HASH(id) BUCKETS 1  
    PROPERTIES("replication_num" = "1")"""

    try {
        // Create test user
        sql """DROP USER IF EXISTS ${testUser}"""
        sql """CREATE USER '${testUser}' IDENTIFIED BY '${testPwd}'"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}"""

        // Create row policies for the user
        sql """CREATE ROW POLICY test_policy_1 ON ${tableName}   
               AS RESTRICTIVE TO ${testUser} USING (id = 1)"""
        sql """CREATE ROW POLICY test_policy_2 ON ${tableName}   
               AS PERMISSIVE TO ${testUser} USING (age > 18)"""

        // Verify policies exist
        def policies = sql """SELECT name, condition, action FROM information_schema.workload_policy   
                             WHERE name LIKE 'test_policy_%' ORDER BY name"""
        assertEquals(2, policies.size())
        assertEquals("test_policy_1", policies[0][0])
        assertEquals("test_policy_2", policies[1][0])

        // Test 1: Drop user and verify policies are cleaned up
        sql """DROP USER ${testUser}"""

        // Verify user is deleted
        test {
            sql """SHOW GRANTS FOR ${testUser}"""
            exception "User does not exist"
        }

        // Verify policies are automatically cleaned up
        def remainingPolicies = sql """SELECT name FROM information_schema.workload_policy   
                                      WHERE name LIKE 'test_policy_%'"""
        assertEquals(0, remainingPolicies.size())

        // Test 2: Create user with role-based policies
        String testRole = 'test_policy_role'
        sql """CREATE ROLE ${testRole}"""
        sql """CREATE USER '${testUser}_2' IDENTIFIED BY '${testPwd}'"""
        sql """GRANT ${testRole} TO '${testUser}_2'"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}_2"""

        sql """CREATE ROW POLICY test_role_policy ON ${tableName}   
               AS RESTRICTIVE TO ROLE ${testRole} USING (id = 2)"""

        // Verify role policy exists
        def rolePolicies = sql """SELECT name FROM information_schema.workload_policy   
                                  WHERE name = 'test_role_policy'"""
        assertEquals(1, rolePolicies.size())

        // Drop user and verify role policy is cleaned up
        sql """DROP USER '${testUser}_2'"""

        def remainingRolePolicies = sql """SELECT name FROM information_schema.workload_policy   
                                          WHERE name = 'test_role_policy'"""
        assertEquals(0, remainingRolePolicies.size())

        // Test 3: Test with multiple tables
        String tableName2 = 'test_drop_policy_table2'
        sql """CREATE TABLE ${tableName2} (  
            id INT,  
            value VARCHAR(50)  
        ) DISTRIBUTED BY HASH(id) BUCKETS 1  
        PROPERTIES("replication_num" = "1")"""

        sql """CREATE USER '${testUser}_3' IDENTIFIED BY '${testPwd}'"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}_3"""

        sql """CREATE ROW POLICY test_multi_table_policy_1 ON ${tableName}   
               AS RESTRICTIVE TO ${testUser}_3 USING (id = 3)"""
        sql """CREATE ROW POLICY test_multi_table_policy_2 ON ${tableName2}   
               AS RESTRICTIVE TO ${testUser}_3 USING (id = 3)"""

        // Verify policies exist on both tables
        def multiTablePolicies = sql """SELECT name FROM information_schema.workload_policy   
                                        WHERE name LIKE 'test_multi_table_policy_%' ORDER BY name"""
        assertEquals(2, multiTablePolicies.size())

        // Drop user and verify all policies are cleaned up
        sql """DROP USER '${testUser}_3'"""

        def remainingMultiTablePolicies = sql """SELECT name FROM information_schema.workload_policy   
                                                WHERE name LIKE 'test_multi_table_policy_%'"""
        assertEquals(0, remainingMultiTablePolicies.size())

        // Test 4: Test IF EXISTS behavior
        sql """CREATE USER '${testUser}_4' IDENTIFIED BY '${testPwd}'"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}_4"""

        // Create policy
        sql """CREATE ROW POLICY test_if_exists_policy ON ${tableName}   
               AS RESTRICTIVE TO ${testUser}_4 USING (id = 4)"""

        // Drop user with IF EXISTS
        sql """DROP USER IF EXISTS ${testUser}_4"""

        // Verify policy is cleaned up even with IF EXISTS
        def remainingIfExistsPolicies = sql """SELECT name FROM information_schema.workload_policy   
                                              WHERE name = 'test_if_exists_policy'"""
        assertEquals(0, remainingIfExistsPolicies.size())

        // Test 5: Verify no policies are affected for other users
        sql """CREATE USER '${testUser}_5' IDENTIFIED BY '${testPwd}'"""
        sql """CREATE USER '${testUser}_6' IDENTIFIED BY '${testPwd}'"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}_5"""
        sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${testUser}_6"""

        sql """CREATE ROW POLICY test_user5_policy ON ${tableName}   
               AS RESTRICTIVE TO ${testUser}_5 USING (id = 5)"""
        sql """CREATE ROW POLICY test_user6_policy ON ${tableName}   
               AS RESTRICTIVE TO ${testUser}_6 USING (id = 6)"""

        // Drop only one user
        sql """DROP USER ${testUser}_5"""

        // Verify only user5's policy is cleaned up, user6's policy remains
        def user5Policies = sql """SELECT name FROM information_schema.workload_policy   
                                   WHERE name = 'test_user5_policy'"""
        def user6Policies = sql """SELECT name FROM information_schema.workload_policy   
                                   WHERE name = 'test_user6_policy'"""

        assertEquals(0, user5Policies.size())
        assertEquals(1, user6Policies.size())

        // Cleanup
        sql """DROP USER ${testUser}_6"""
        sql """DROP ROLE ${testRole}"""

    } finally {
        // Cleanup test environment
        sql """DROP DATABASE IF EXISTS ${dbName}"""
        sql """DROP USER IF EXISTS ${testUser}"""
        sql """DROP USER IF EXISTS ${testUser}_2"""
        sql """DROP USER IF EXISTS ${testUser}_3"""
        sql """DROP USER IF EXISTS ${testUser}_4"""
        sql """DROP USER IF EXISTS ${testUser}_5"""
        sql """DROP USER IF EXISTS ${testUser}_6"""
    }
}

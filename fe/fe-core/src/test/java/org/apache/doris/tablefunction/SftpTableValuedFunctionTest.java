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

package org.apache.doris.tablefunction;

import org.apache.doris.common.FeConstants;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SftpTableValuedFunctionTest {

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testSftpTvfBasicProperties() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.csv");
        properties.put("format", "csv");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_SFTP, tvf.getTFileType());
        Assert.assertEquals("sftp://sftp.example.com/data/file.csv", tvf.getFilePath());
        Assert.assertEquals("SftpTableValuedFunction", tvf.getTableName());
    }

    @Test
    public void testSftpTvfWithUserPassword() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.csv");
        properties.put("format", "csv");
        properties.put("sftp.user", "testuser");
        properties.put("sftp.password", "testpass");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals("sftp://sftp.example.com/data/file.csv", tvf.getFilePath());
    }

    @Test
    public void testSftpTvfWithSshKey() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.csv");
        properties.put("format", "csv");
        properties.put("sftp.user", "testuser");
        properties.put("sftp.ssh_key", "/home/user/.ssh/id_rsa");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals("sftp://sftp.example.com/data/file.csv", tvf.getFilePath());
    }

    @Test
    public void testSftpTvfWithParquetFormat() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.parquet");
        properties.put("format", "parquet");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_SFTP, tvf.getTFileType());
    }

    @Test
    public void testSftpTvfWithJsonFormat() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.json");
        properties.put("format", "json");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_SFTP, tvf.getTFileType());
    }

    @Test
    public void testSftpTvfBrokerDesc() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.csv");
        properties.put("format", "csv");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals("SftpTvfBroker", tvf.getBrokerDesc().getName());
    }

    @Test
    public void testSftpTvfInvalidUri() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "http://example.com/data/file.csv");
        properties.put("format", "csv");
        Assert.assertThrows(RuntimeException.class, () -> new SftpTableValuedFunction(properties));
    }

    @Test
    public void testSftpTvfFtpUri() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.csv");
        properties.put("format", "csv");
        Assert.assertThrows(RuntimeException.class, () -> new SftpTableValuedFunction(properties));
    }

    @Test
    public void testSftpTvfWithPort() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com:2222/data/file.csv");
        properties.put("format", "csv");
        SftpTableValuedFunction tvf = new SftpTableValuedFunction(properties);
        Assert.assertEquals("sftp://sftp.example.com:2222/data/file.csv", tvf.getFilePath());
    }
}

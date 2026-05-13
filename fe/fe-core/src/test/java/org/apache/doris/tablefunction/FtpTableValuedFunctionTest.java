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

public class FtpTableValuedFunctionTest {

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testFtpTvfBasicProperties() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.csv");
        properties.put("format", "csv");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_FTP, tvf.getTFileType());
        Assert.assertEquals("ftp://ftp.example.com/data/file.csv", tvf.getFilePath());
        Assert.assertEquals("FtpTableValuedFunction", tvf.getTableName());
    }

    @Test
    public void testFtpTvfWithUserPassword() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.csv");
        properties.put("format", "csv");
        properties.put("ftp.user", "testuser");
        properties.put("ftp.password", "testpass");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals("ftp://ftp.example.com/data/file.csv", tvf.getFilePath());
    }

    @Test
    public void testFtpTvfWithParquetFormat() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.parquet");
        properties.put("format", "parquet");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_FTP, tvf.getTFileType());
    }

    @Test
    public void testFtpTvfWithJsonFormat() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.json");
        properties.put("format", "json");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals(TFileType.FILE_FTP, tvf.getTFileType());
    }

    @Test
    public void testFtpTvfBrokerDesc() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com/data/file.csv");
        properties.put("format", "csv");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals("FtpTvfBroker", tvf.getBrokerDesc().getName());
    }

    @Test
    public void testFtpTvfInvalidUri() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "http://example.com/data/file.csv");
        properties.put("format", "csv");
        Assert.assertThrows(RuntimeException.class, () -> new FtpTableValuedFunction(properties));
    }

    @Test
    public void testFtpTvfSftpUri() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "sftp://sftp.example.com/data/file.csv");
        properties.put("format", "csv");
        Assert.assertThrows(RuntimeException.class, () -> new FtpTableValuedFunction(properties));
    }

    @Test
    public void testFtpTvfWithPort() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", "ftp://ftp.example.com:2121/data/file.csv");
        properties.put("format", "csv");
        FtpTableValuedFunction tvf = new FtpTableValuedFunction(properties);
        Assert.assertEquals("ftp://ftp.example.com:2121/data/file.csv", tvf.getFilePath());
    }
}

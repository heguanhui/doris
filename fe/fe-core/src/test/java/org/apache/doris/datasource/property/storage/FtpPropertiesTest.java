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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FtpPropertiesTest {
    private Map<String, String> origProps;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testFtpPropertiesWithExplicitSupport() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        StorageProperties sp = StorageProperties.createPrimary(origProps);
        Assertions.assertInstanceOf(FtpProperties.class, sp);
        Assertions.assertEquals("ftp", sp.getStorageName());
    }

    @Test
    public void testFtpPropertiesGuessIsMeWithUri() {
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        Assertions.assertTrue(FtpProperties.guessIsMe(origProps));
    }

    @Test
    public void testFtpPropertiesGuessIsMeWithFsSupport() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        Assertions.assertTrue(FtpProperties.guessIsMe(origProps));
    }

    @Test
    public void testFtpPropertiesGuessIsMeEmpty() {
        Assertions.assertFalse(FtpProperties.guessIsMe(new HashMap<>()));
    }

    @Test
    public void testFtpPropertiesGuessIsMeNull() {
        Assertions.assertFalse(FtpProperties.guessIsMe(null));
    }

    @Test
    public void testFtpPropertiesGuessIsMeWrongUri() {
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        Assertions.assertFalse(FtpProperties.guessIsMe(origProps));
    }

    @Test
    public void testValidateAndNormalizeUriValid() throws UserException {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        String uri = ftpProps.validateAndNormalizeUri("ftp://ftp.example.com/data/file.csv");
        Assertions.assertEquals("ftp://ftp.example.com/data/file.csv", uri);
    }

    @Test
    public void testValidateAndNormalizeUriNull() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid ftp url: null", () -> ftpProps.validateAndNormalizeUri(null));
    }

    @Test
    public void testValidateAndNormalizeUriWrongScheme() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid ftp url: http://example.com/file.csv",
                () -> ftpProps.validateAndNormalizeUri("http://example.com/file.csv"));
    }

    @Test
    public void testValidateAndNormalizeUriSftpScheme() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid ftp url: sftp://example.com/file.csv",
                () -> ftpProps.validateAndNormalizeUri("sftp://example.com/file.csv"));
    }

    @Test
    public void testValidateAndGetUri() throws UserException {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        String uri = ftpProps.validateAndGetUri(origProps);
        Assertions.assertEquals("ftp://ftp.example.com/data/file.csv", uri);
    }

    @Test
    public void testValidateAndGetUriMissing() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> propsWithoutUri = new HashMap<>(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid ftp url: null", () -> ftpProps.validateAndGetUri(propsWithoutUri));
    }

    @Test
    public void testGetUri() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("ftp://ftp.example.com/data/file.csv", ftpProps.getUri());
    }

    @Test
    public void testGetBackendConfigProperties() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        origProps.put("ftp.user", "testuser");
        origProps.put("ftp.password", "testpass");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = ftpProps.getBackendConfigProperties();
        Assertions.assertEquals("ftp://ftp.example.com/data/file.csv", backendProps.get("uri"));
        Assertions.assertEquals("testuser", backendProps.get("ftp.user"));
        Assertions.assertEquals("testpass", backendProps.get("ftp.password"));
    }

    @Test
    public void testGetStorageName() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("ftp", ftpProps.getStorageName());
    }

    @Test
    public void testGetType() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(StorageProperties.Type.FTP, ftpProps.getType());
    }

    @Test
    public void testHadoopStorageConfigIsNull() {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        FtpProperties ftpProps = (FtpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertNull(ftpProps.getHadoopStorageConfig());
    }

    @Test
    public void testCreateAllWithFtpSupport() throws UserException {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        java.util.List<StorageProperties> all = StorageProperties.createAll(origProps);
        Assertions.assertEquals(1, all.size());
        Assertions.assertInstanceOf(FtpProperties.class, all.get(0));
    }

    @Test
    public void testExplicitFtpSupportSkipsOtherGuessIsMe() throws UserException {
        origProps.put(StorageProperties.FS_FTP_SUPPORT, "true");
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        origProps.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        java.util.List<StorageProperties> all = StorageProperties.createAll(origProps);
        java.util.List<Class<?>> types = all.stream().map(Object::getClass).collect(java.util.stream.Collectors.toList());
        Assertions.assertTrue(types.contains(FtpProperties.class));
        Assertions.assertFalse(types.contains(S3Properties.class),
                "S3 should NOT be matched when fs.ftp.support is explicitly set");
    }
}

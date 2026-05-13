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

public class SftpPropertiesTest {
    private Map<String, String> origProps;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testSftpPropertiesWithExplicitSupport() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        StorageProperties sp = StorageProperties.createPrimary(origProps);
        Assertions.assertInstanceOf(SftpProperties.class, sp);
        Assertions.assertEquals("sftp", sp.getStorageName());
    }

    @Test
    public void testSftpPropertiesGuessIsMeWithUri() {
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        Assertions.assertTrue(SftpProperties.guessIsMe(origProps));
    }

    @Test
    public void testSftpPropertiesGuessIsMeWithFsSupport() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        Assertions.assertTrue(SftpProperties.guessIsMe(origProps));
    }

    @Test
    public void testSftpPropertiesGuessIsMeEmpty() {
        Assertions.assertFalse(SftpProperties.guessIsMe(new HashMap<>()));
    }

    @Test
    public void testSftpPropertiesGuessIsMeNull() {
        Assertions.assertFalse(SftpProperties.guessIsMe(null));
    }

    @Test
    public void testSftpPropertiesGuessIsMeWrongUri() {
        origProps.put("uri", "ftp://ftp.example.com/data/file.csv");
        Assertions.assertFalse(SftpProperties.guessIsMe(origProps));
    }

    @Test
    public void testValidateAndNormalizeUriValid() throws UserException {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        String uri = sftpProps.validateAndNormalizeUri("sftp://sftp.example.com/data/file.csv");
        Assertions.assertEquals("sftp://sftp.example.com/data/file.csv", uri);
    }

    @Test
    public void testValidateAndNormalizeUriNull() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid sftp url: null", () -> sftpProps.validateAndNormalizeUri(null));
    }

    @Test
    public void testValidateAndNormalizeUriWrongScheme() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid sftp url: http://example.com/file.csv",
                () -> sftpProps.validateAndNormalizeUri("http://example.com/file.csv"));
    }

    @Test
    public void testValidateAndNormalizeUriFtpScheme() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid sftp url: ftp://example.com/file.csv",
                () -> sftpProps.validateAndNormalizeUri("ftp://example.com/file.csv"));
    }

    @Test
    public void testValidateAndGetUri() throws UserException {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        String uri = sftpProps.validateAndGetUri(origProps);
        Assertions.assertEquals("sftp://sftp.example.com/data/file.csv", uri);
    }

    @Test
    public void testValidateAndGetUriMissing() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> propsWithoutUri = new HashMap<>(origProps);
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Invalid sftp url: null", () -> sftpProps.validateAndGetUri(propsWithoutUri));
    }

    @Test
    public void testGetUri() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("sftp://sftp.example.com/data/file.csv", sftpProps.getUri());
    }

    @Test
    public void testGetBackendConfigProperties() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        origProps.put("sftp.user", "testuser");
        origProps.put("sftp.password", "testpass");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = sftpProps.getBackendConfigProperties();
        Assertions.assertEquals("sftp://sftp.example.com/data/file.csv", backendProps.get("uri"));
        Assertions.assertEquals("testuser", backendProps.get("sftp.user"));
        Assertions.assertEquals("testpass", backendProps.get("sftp.password"));
    }

    @Test
    public void testGetStorageName() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("sftp", sftpProps.getStorageName());
    }

    @Test
    public void testGetType() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(StorageProperties.Type.SFTP, sftpProps.getType());
    }

    @Test
    public void testHadoopStorageConfigIsNull() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertNull(sftpProps.getHadoopStorageConfig());
    }

    @Test
    public void testCreateAllWithSftpSupport() throws UserException {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        java.util.List<StorageProperties> all = StorageProperties.createAll(origProps);
        Assertions.assertEquals(1, all.size());
        Assertions.assertInstanceOf(SftpProperties.class, all.get(0));
    }

    @Test
    public void testExplicitSftpSupportSkipsOtherGuessIsMe() throws UserException {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        origProps.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        java.util.List<StorageProperties> all = StorageProperties.createAll(origProps);
        java.util.List<Class<?>> types = all.stream().map(Object::getClass).collect(java.util.stream.Collectors.toList());
        Assertions.assertTrue(types.contains(SftpProperties.class));
        Assertions.assertFalse(types.contains(S3Properties.class),
                "S3 should NOT be matched when fs.sftp.support is explicitly set");
    }

    @Test
    public void testSftpWithSshKeyPath() {
        origProps.put(StorageProperties.FS_SFTP_SUPPORT, "true");
        origProps.put("uri", "sftp://sftp.example.com/data/file.csv");
        origProps.put("sftp.user", "testuser");
        origProps.put("sftp.ssh_key", "/home/user/.ssh/id_rsa");
        SftpProperties sftpProps = (SftpProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = sftpProps.getBackendConfigProperties();
        Assertions.assertEquals("/home/user/.ssh/id_rsa", backendProps.get("sftp.ssh_key"));
    }
}

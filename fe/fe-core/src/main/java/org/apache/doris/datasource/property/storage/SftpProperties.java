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

import org.apache.doris.common.UserException;

import com.google.common.collect.ImmutableSet;
import org.apache.hudi.common.util.MapUtils;

import java.util.Map;
import java.util.Set;

public class SftpProperties extends StorageProperties {
    private static final ImmutableSet<String> SFTP_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(StorageProperties.FS_SFTP_SUPPORT)
            .build();

    public SftpProperties(Map<String, String> origProps) {
        super(Type.SFTP, origProps);
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return origProps;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        if (url == null || !url.startsWith("sftp://")) {
            throw new UserException("Invalid sftp url: " + url);
        }
        return url;
    }

    @Override
    public String validateAndGetUri(Map<String, String> props) throws UserException {
        String url = props.get(URI_KEY);
        return validateAndNormalizeUri(url);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isNullOrEmpty(props)) {
            return false;
        }
        if (SFTP_PROPERTIES.stream().anyMatch(props::containsKey)) {
            return true;
        }
        String uri = props.get(URI_KEY);
        return uri != null && uri.startsWith("sftp://");
    }

    public String getUri() {
        return origProps.get(URI_KEY);
    }

    @Override
    public String getStorageName() {
        return "sftp";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = null;
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("sftp");
    }
}

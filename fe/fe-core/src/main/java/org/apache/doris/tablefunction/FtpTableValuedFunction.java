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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;

import java.util.Map;

public class FtpTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "ftp";

    public FtpTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        Map<String, String> props = super.parseCommonProperties(properties);
        props.put(StorageProperties.FS_FTP_SUPPORT, "true");
        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
            String uri = storageProperties.validateAndGetUri(props);
            filePath = storageProperties.validateAndNormalizeUri(uri);
            this.backendConnectProperties.put(URI_KEY, filePath);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
        if (FeConstants.runningUnitTest) {
        } else {
            parseFile();
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_FTP;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("FtpTvfBroker", processedParams);
    }

    @Override
    public String getTableName() {
        return "FtpTableValuedFunction";
    }
}

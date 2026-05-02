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

package org.apache.doris.policy;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * DESIGN NOTE: Backward-Compatible Batch Policy Deletion Log
 * This class represents a batch deletion of row policies for a specific user.
 * The design prioritizes version rollback compatibility over atomic operation semantics:
 * 1. COMPATIBILITY FEATURES:
 * - Uses JSON serialization with @SerializedName annotations for version resilience
 * - Implements static read() method following Doris EditLog patterns
 * - Contains List<DropPolicyLog> to allow individual policy replay in old versions
 * 2. ROLLBACK HANDLING:
 * - Old versions without this class will fail deserialization and skip the log entry
 * - New versions can replay all policy deletions atomically
 * - No impact on core user deletion functionality during version mismatches
 * This design enables graceful degradation during version transitions while
 * maintaining full functionality in homogeneous version environments.
 */
@AllArgsConstructor
@Getter
@Setter
public class DropPoliciesForUserLog implements Writable {

    @SerializedName(value = "userIdent")
    private UserIdentity userIdent;
    @SerializedName(value = "policyLogs")
    private List<DropPolicyLog> policyLogs;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropPoliciesForUserLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropPoliciesForUserLog.class);
    }

}

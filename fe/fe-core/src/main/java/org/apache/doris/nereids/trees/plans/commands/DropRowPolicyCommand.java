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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DropRowPolicyCommand
 **/
public class DropRowPolicyCommand extends DropCommand {
    private final boolean ifExists;
    private final String policyName;
    private final TableNameInfo tableNameInfo;
    private final List<UserIdentity> users;
    private final List<String> roles;

    /**
     * DropRowPolicyCommand
     **/
    public DropRowPolicyCommand(boolean ifExists,
                                String policyName,
                                TableNameInfo tableNameInfo,
                                List<UserIdentity> users,
                                List<String> roles) {
        super(PlanType.DROP_ROW_POLICY_COMMAND);
        this.ifExists = ifExists;
        this.policyName = policyName;
        this.tableNameInfo = tableNameInfo;
        this.users = users;
        this.roles = roles;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        if (isSingleTargetDrop()) {
            dropSingleTarget();
        } else {
            dropBatchTargets();
        }
    }

    private boolean isSingleTargetDrop() {
        return policyName != null && tableNameInfo != null
                && ((users.size() <= 1 && roles.isEmpty())
                || (users.isEmpty() && roles.size() <= 1));
    }

    private void dropSingleTarget() throws Exception {
        UserIdentity user = users.isEmpty() ? null : users.get(0);
        String roleName = roles.isEmpty() ? null : roles.get(0);
        DropPolicyLog dropPolicyLog = new DropPolicyLog(tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PolicyTypeEnum.ROW, policyName, user, roleName);
        Env.getCurrentEnv().getPolicyMgr().dropPolicy(dropPolicyLog, ifExists);
    }

    private void dropBatchTargets() throws Exception {
        String ctlName = tableNameInfo != null ? tableNameInfo.getCtl() : null;
        String dbName = tableNameInfo != null ? tableNameInfo.getDb() : null;
        String tblName = tableNameInfo != null ? tableNameInfo.getTbl() : null;

        List<RowPolicy> matchedPolicies = Env.getCurrentEnv().getPolicyMgr()
                .findMatchedRowPolicies(ctlName, dbName, tblName, policyName, users, roles);

        if (matchedPolicies.isEmpty()) {
            if (ifExists) {
                return;
            }
            throw new DdlException("the row policy not exist");
        }

        for (RowPolicy policy : matchedPolicies) {
            DropPolicyLog dropPolicyLog = new DropPolicyLog(
                    policy.getCtlName(), policy.getDbName(), policy.getTableName(),
                    PolicyTypeEnum.ROW, policy.getPolicyName(), policy.getUser(), policy.getRoleName());
            Env.getCurrentEnv().getPolicyMgr().dropPolicy(dropPolicyLog, true);
        }
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (policyName == null && tableNameInfo == null && users.isEmpty() && roles.isEmpty()) {
            throw new AnalysisException("DROP ROW POLICY requires at least one of: "
                    + "policy name ON table, FOR user, or FOR ROLE role");
        }
        if (tableNameInfo != null) {
            tableNameInfo.analyze(ctx.getNameSpaceContext());
        }
        for (UserIdentity user : users) {
            user.analyze();
            if (!Env.getCurrentEnv().getAuth().doesUserExist(user)) {
                throw new AnalysisException("user not exist: " + user.getQualifiedUser());
            }
        }
        for (String roleName : roles) {
            if (!Env.getCurrentEnv().getAuth().doesRoleExist(roleName)) {
                throw new AnalysisException("role not exist: " + roleName);
            }
        }
        checkDuplicateUsers();
        checkDuplicateRoles();
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.GRANT.getPrivs().toString());
        }
    }

    private void checkDuplicateUsers() throws AnalysisException {
        Set<String> seen = new HashSet<>();
        for (UserIdentity user : users) {
            if (!seen.add(user.getQualifiedUser())) {
                throw new AnalysisException("Duplicate user: " + user.getQualifiedUser());
            }
        }
    }

    private void checkDuplicateRoles() throws AnalysisException {
        Set<String> seen = new HashSet<>();
        for (String role : roles) {
            if (!seen.add(role)) {
                throw new AnalysisException("Duplicate role: " + role);
            }
        }
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getPolicyName() {
        return policyName;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public List<UserIdentity> getUsers() {
        return users;
    }

    public List<String> getRoles() {
        return roles;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropRowPolicyCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}

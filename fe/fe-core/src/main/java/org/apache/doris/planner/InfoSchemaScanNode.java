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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.datasource.split.FileSplit;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TInfoSchemaBeLocalParams;
import org.apache.doris.thrift.TInfoSchemaCatalogDbParams;
import org.apache.doris.thrift.TInfoSchemaFileDesc;
import org.apache.doris.thrift.TInfoSchemaSourceInfo;
import org.apache.doris.thrift.TInfoSchemaSourceType;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Scan node for information_schema tables treated as a table format (FORMAT_INFO_SCHEMA_TABLE).
 *
 * <p>This is the FE-side counterpart of the BE InfoSchemaTableReader. The data source is FE
 * metadata (not files): the BE reader fetches rows from FE through the existing
 * fetchSchemaTableData RPC, paged by offset/batch_size.
 *
 * <p>Abstraction layers (per PMC review):
 * <ul>
 *   <li>Data source abstraction: {@link TInfoSchemaSourceInfo} — FE is one implementation of
 *       a data source; other sources can be added later without changing the RPC shape.</li>
 *   <li>Series-specific filters: {@link TInfoSchemaCatalogDbParams} — the catalog/db/table
 *       series; other series (jobs/tasks/backends/...) add their own parameter structs.</li>
 *   <li>Unified identifier: {@link TInfoSchemaFileDesc} — metadata_type + output_mode +
 *       source_info + series params, carried as a single nested object through plan -> scan
 *       range -> RPC.</li>
 * </ul>
 *
 * <p>0-1 example: only information_schema.tables is wired end-to-end. getNumInstances() is
 * forced to 1 because the data source is FE metadata (a single logical source); multiple
 * instances would duplicate the full metadata scan.
 */
public class InfoSchemaScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(InfoSchemaScanNode.class);

    // Unified data source identifier + abstraction + series filters (single source of truth).
    private final TInfoSchemaFileDesc infoSchemaDesc;

    public InfoSchemaScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sv, ScanContext scanContext, TInfoSchemaFileDesc infoSchemaDesc) {
        super(id, desc, "InfoSchemaScanNode", scanContext, needCheckColumnPriv, sv);
        this.infoSchemaDesc = infoSchemaDesc;
    }

    @Override
    protected TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_INFO_SCHEMA_TABLE;
    }

    @Override
    protected List<String> getPathPartitionKeys() {
        // information_schema has no path-partitioned columns.
        return ImmutableList.of();
    }

    @Override
    protected TableIf getTargetTable() {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        return Collections.emptyMap();
    }

    /**
     * Force a single instance: the data source is FE metadata (one logical source).
     * Multiple instances would each scan the full metadata set and produce duplicate rows.
     */
    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // Single virtual split: the "file" is FE metadata, addressed by a synthetic location.
        String tableName = desc.getTable().getName();
        LocationPath path = LocationPath.of("fe://information_schema/" + tableName);
        FileSplit split = new FileSplit(path, 0, 0, 0, 0, new String[0], Collections.emptyList());
        return Lists.newArrayList(split);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        // ---- Data source abstraction ----
        TInfoSchemaSourceInfo tInfoSchemaSourceInfo = new TInfoSchemaSourceInfo();
        tInfoSchemaSourceInfo.setCurrentUserIdent(ConnectContext.get().getCurrentUserIdentity().toThrift());

        String tableName = desc.getTable().getName();
        TMetadataType metadataType = ExternalInfoSchemaTable.getMetadataType(tableName);

        if (metadataType == TMetadataType.TABLETS) {
            // BE-local metadata: each BE generates data locally.
            tInfoSchemaSourceInfo.setSourceType(TInfoSchemaSourceType.BE_LOCAL);
            infoSchemaDesc.setSourceInfo(tInfoSchemaSourceInfo);

            // Series-specific params: BE_LOCAL (backend_id from scan range location).
            TInfoSchemaBeLocalParams beLocalParams = new TInfoSchemaBeLocalParams();
            // backend_id will be set by BE at runtime via RuntimeState::backend_id().
            infoSchemaDesc.setBeLocalParams(beLocalParams);
        } else {
            // FE metadata: BE fetches data from FE via RPC.
            tInfoSchemaSourceInfo.setSourceType(TInfoSchemaSourceType.FE_METADATA);

            List<TNetworkAddress> feAddrList = new ArrayList<>();
            for (Frontend fe : Env.getCurrentEnv().getFrontends(null)) {
                TNetworkAddress addr = new TNetworkAddress();
                addr.setHostname(fe.getHost());
                addr.setPort(fe.getRpcPort());
                feAddrList.add(addr);
            }
            tInfoSchemaSourceInfo.setFeAddrList(feAddrList);

            // Serialize conjuncts as JSON.
            if (conjuncts != null && !conjuncts.isEmpty()) {
                tInfoSchemaSourceInfo.setFrontendConjuncts(GsonUtils.GSON.toJson(conjuncts));
            }
            infoSchemaDesc.setSourceInfo(tInfoSchemaSourceInfo);

            // Series-specific filters: internal catalog + information_schema database.
            TInfoSchemaCatalogDbParams catalogDbParams = new TInfoSchemaCatalogDbParams();
            catalogDbParams.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
            catalogDbParams.setDb(InfoSchemaDb.DATABASE_NAME);
            infoSchemaDesc.setCatalogDbParams(catalogDbParams);
        }

        // ---- Bridge to the existing TTableFormatFileDesc wrapper ----
        TTableFormatFileDesc tableFormatParams = new TTableFormatFileDesc();
        tableFormatParams.setTableFormatType("info_schema");
        tableFormatParams.setInfoSchemaParams(infoSchemaDesc);
        rangeDesc.setTableFormatParams(tableFormatParams);
    }
}

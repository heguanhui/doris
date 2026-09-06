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

package org.apache.doris.datasource.infoschema;

import org.apache.doris.analysis.SchemaTableType;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TInfoSchemaFileDesc;
import org.apache.doris.thrift.TMetadataOutputMode;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TSchemaTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import java.util.Optional;

public class ExternalInfoSchemaTable extends ExternalTable {

    // Data source description (the only carrier of TInfoSchemaFileDesc).
    // Lazily constructed at query time; InfoSchemaScanNode fills the data source
    // abstraction (source_info) and series-specific filters at plan time.
    private TInfoSchemaFileDesc infoSchemaDesc;

    // Generic catalog/database from SQL parsing (may be InternalCatalog, not ExternalCatalog).
    private CatalogIf<?> sqlCatalog;
    private DatabaseIf<?> sqlDb;

    public TInfoSchemaFileDesc getInfoSchemaDesc(TMetadataType metadataType,
                                                 TMetadataOutputMode outputMode) {
        if (infoSchemaDesc == null) {
            infoSchemaDesc = new TInfoSchemaFileDesc();
            infoSchemaDesc.setMetadataType(metadataType);
            infoSchemaDesc.setOutputMode(outputMode);
        }
        return infoSchemaDesc;
    }

    public static TMetadataType getMetadataType(String tableName) {
        if (!isTableReaderSupported(tableName)) {
            throw new IllegalArgumentException(
                    "unsupported information_schema table for Table Reader path yet: " + tableName);
        }
        if ("backend_tablets".equalsIgnoreCase(tableName)) {
            return TMetadataType.TABLETS;
        }
        return TMetadataType.TABLES;
    }

    public static boolean isTableReaderSupported(String tableName) {
        if (tableName == null) {
            return false;
        }
        return "tables".equalsIgnoreCase(tableName)
                || "backend_tablets".equalsIgnoreCase(tableName);
    }

    /**
     * Unified constructor: accepts any CatalogIf + DatabaseIf from SQL parsing.
     * ExternalCatalog extends CatalogIf, ExternalDatabase extends DatabaseIf,
     * so this single constructor handles both internal and external catalogs.
     *
     * Bypasses ExternalTable's constructor (which requires ExternalCatalog/ExternalDatabase)
     * and directly initializes the fields. Methods that depend on this.catalog/this.db
     * are overridden to use sqlCatalog/sqlDb instead.
     */
    public ExternalInfoSchemaTable(long id, String name, CatalogIf<?> catalog, DatabaseIf<?> db) {
        this.id = id;
        this.name = name;
        this.remoteName = name;
        this.type = TableType.SCHEMA;
        this.objectCreated = false;
        this.dbName = db.getFullName();
        this.sqlCatalog = catalog;
        this.sqlDb = db;
        this.nameMapping = new NameMapping(catalog.getId(), dbName, name, db.getFullName(), name);
    }

    // ---- Override methods that depend on this.catalog / this.db ----

    @Override
    public DatabaseIf<?> getDatabase() {
        return sqlDb;
    }

    @Override
    public long getDbId() {
        return sqlDb != null ? sqlDb.getId() : 0;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public void makeSureInitialized() {
        // For information_schema tables, no async initialization needed.
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return initSchema();
    }

    @Override
    public long getRowCount() {
        // For information_schema tables, return 0 (no row count estimation).
        return 0;
    }

    @Override
    public long getCachedRowCount() {
        return 0;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        return Optional.of(new SchemaCacheValue(SchemaTable.TABLE_MAP.get(name).getFullSchema()));
    }

    @Override
    public TTableDescriptor toThrift() {
        TSchemaTable tSchemaTable = new TSchemaTable(SchemaTableType.getThriftType(this.name));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                getFullSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }
}

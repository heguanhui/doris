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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergMetadataOpsTruncateTest {

    private IcebergMetadataOps ops;

    @Before
    public void setUp() {
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);
    }

    // --- parsePartitionName tests ---

    @Test
    public void testParsePartitionNameSingleField() {
        Map<String, String> result = ops.parsePartitionName("region=bj");
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("bj", result.get("region"));
    }

    @Test
    public void testParsePartitionNameMultipleFields() {
        Map<String, String> result = ops.parsePartitionName("dt=2025-01-01/region=bj");
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("2025-01-01", result.get("dt"));
        Assert.assertEquals("bj", result.get("region"));
    }

    @Test
    public void testParsePartitionNameNullValue() {
        Map<String, String> result = ops.parsePartitionName("region=");
        Assert.assertEquals(1, result.size());
        Assert.assertNull(result.get("region"));
    }

    @Test
    public void testParsePartitionNameEmptyValue() {
        Map<String, String> result = ops.parsePartitionName("region=");
        Assert.assertNull(result.get("region"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParsePartitionNameInvalidFormat() {
        ops.parsePartitionName("invalid_no_equals");
    }

    @Test
    public void testParsePartitionNameValueWithEquals() {
        Map<String, String> result = ops.parsePartitionName("col=a=b");
        Assert.assertEquals("a=b", result.get("col"));
    }

    // --- buildPartitionFilterFromNames tests ---

    private Table createMockTableWithIdentityPartition() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()),
                Types.NestedField.required(3, "dt", Types.DateType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }

    private Table createMockTableWithMultipleIdentityPartitions() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()),
                Types.NestedField.required(3, "dt", Types.DateType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .identity("dt")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }

    private Table createMockTableWithDayTransform() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "dt", Types.DateType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("dt")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }

    private Table createMockTableWithBucketTransform() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("region", 4)
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }

    private Table createMockUnpartitionedTable() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }

    @Test
    public void testBuildPartitionFilterSingleIdentityPartition() {
        Table table = createMockTableWithIdentityPartition();
        List<String> partitionNames = Collections.singletonList("region=bj");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterMultiplePartitions() {
        Table table = createMockTableWithIdentityPartition();
        List<String> partitionNames = Arrays.asList("region=bj", "region=sh");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterCompositePartition() {
        Table table = createMockTableWithMultipleIdentityPartitions();
        List<String> partitionNames = Collections.singletonList("region=bj/dt=2025-01-01");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterEmptyPartitionList() {
        Table table = createMockTableWithIdentityPartition();
        List<String> partitionNames = Collections.emptyList();
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertEquals(Expressions.alwaysTrue(), filter);
    }

    @Test
    public void testBuildPartitionFilterUnpartitionedTable() {
        Table table = createMockUnpartitionedTable();
        List<String> partitionNames = Collections.singletonList("region=bj");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertEquals(Expressions.alwaysTrue(), filter);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildPartitionFilterDayTransformThrows() {
        Table table = createMockTableWithDayTransform();
        List<String> partitionNames = Collections.singletonList("dt_day=20045");
        ops.buildPartitionFilterFromNames(table, partitionNames);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildPartitionFilterBucketTransformThrows() {
        Table table = createMockTableWithBucketTransform();
        List<String> partitionNames = Collections.singletonList("region_bucket=2");
        ops.buildPartitionFilterFromNames(table, partitionNames);
    }

    @Test
    public void testBuildPartitionFilterIntegerPartitionValue() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "age", Types.IntegerType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("age")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);

        List<String> partitionNames = Collections.singletonList("age=25");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterBooleanPartitionValue() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "active", Types.BooleanType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("active")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);

        List<String> partitionNames = Collections.singletonList("active=true");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterNullPartitionValue() {
        Table table = Mockito.mock(Table.class);
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);

        List<String> partitionNames = Collections.singletonList("region=");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }

    @Test
    public void testBuildPartitionFilterMultipleCompositePartitions() {
        Table table = createMockTableWithMultipleIdentityPartitions();
        List<String> partitionNames = Arrays.asList("region=bj/dt=2025-01-01", "region=sh/dt=2025-01-02");
        Expression filter = ops.buildPartitionFilterFromNames(table, partitionNames);
        Assert.assertNotNull(filter);
    }
}

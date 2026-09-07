# Doris information_schema 系统表 — 基于 Table Reader 的实现方案

---

> **源码基线**：本文档初稿整理后落后当前分支若干 commit。本版已按 **branch-4.1**（HEAD `e5ae5ab9d04`）最新代码校准源码引用；个别行号为参考值，以评审时最新代码为准。

## 整体概述

本方案将 Doris `information_schema` 系统表（`tables`/`partitions`/`columns` 等）的实现从现有的 `SchemaScanNode` + 41 个 `SchemaXxxScanner` 独立路径，统一为标准 Table Reader 路径。

**核心思路**：系统表是一种 table format，数据来源是 FE 元数据而非文件系统。只需实现一个从 FE 获取数据的 Reader，即可复用 `FileQueryScanNode` → `FileScanner` → `GenericReader` 全链路，不需要独立的 Scanner。

**数据链路**：

```
FE: ExternalInfoSchemaTable (改造现有类, extends ExternalTable)
    → InfoSchemaScanNode (extends FileQueryScanNode)  // PhysicalPlanTranslator instanceof 路由
    → toThrift() 生成 TFileScanNode (极简: tuple_id + table_name)
    → TFileScanRange 承载 TInfoSchemaFileDesc (数据源描述: 标识 + 数据源抽象 + 系列参数)

BE: FileScanOperatorX → FileScanner
    → _get_next_reader() switch-case → FORMAT_INFO_SCHEMA_TABLE
    → InfoSchemaTableReader (extends GenericReader)
    → get_next_block(): 分批调用 fetchSchemaTableData RPC
    → FE MetadataGenerator.getInfoSchemaMetadata() 生成元数据
```

**三层分离**：

| 层次 | Thrift 结构 | 职责 |
|------|-----------|------|
| Plan Node | `TFileScanNode`（复用，2 字段） | 查询上下文：`tuple_id` + `table_name` |
| Scan Range | `TFileScanRange` + `TInfoSchemaFileDesc`（新增） | 数据源描述：`metadata_type`/`output_mode` + 数据源抽象（`TInfoSchemaSourceInfo`）+ 系列专用过滤参数（按需子结构） |
| RPC | `TFetchSchemaTableDataRequest`（扩展现有） | BE→FE 请求：嵌套 `TInfoSchemaFileDesc` + `offset`/`batch_size`/`thread_id`/`time_zone`/`columns_name` |

**新增组件**：`InfoSchemaScanNode`（FE Plan Node）+ `InfoSchemaTableReader`（BE Reader）+ `TInfoSchemaFileDesc`/`TInfoSchemaSourceInfo`/系列子结构（Thrift 数据源描述）+ `FORMAT_INFO_SCHEMA_TABLE`（TFileFormatType 枚举）。**改造**现有 `ExternalInfoSchemaTable`（FE 表层，复用其 `ExternalTable` 载体，不新建第二个表类）。复用 `FileScanner`、`FileScanOperatorX`、`ScannerScheduler`、`fetchSchemaTableData` RPC 等现有基础设施。

---

## 1. 基于 Table Reader 的 information_schema 系统表实现

> **范围说明**：本方案针对 Doris 自身的 `information_schema` 系统表（`tables`/`partitions`/`columns` 等，数据来自 FE 元数据）。**不涉及** Iceberg/Paimon/Hive 等外部湖的系统表（如 `iceberg_tbl$snapshots`），那是另一条 `SysTable` 路由体系，与本次目标无关。下文引用 Paimon 系统表的 Native 路径，仅作为"系统表可作为 table format"的**可行性参考先例**。

### 1.1 设计原理：系统表作为 Table Format

核心思路：**系统表是一种 table format，区别只在于数据来源是 FE 元数据而非文件系统。只需要实现一个从 FE 获取数据的 table reader，就可以复用整个 `FileQueryScanNode` → `FileScanner` → `GenericReader` 路径，不需要独立的 Scanner。**

information_schema 系统表将自身包装为 `ExternalTable`，走标准 table reader 路径，核心链路如下：

```
FE: ExternalInfoSchemaTable (改造现有类, extends ExternalTable, type=SCHEMA)
    → LogicalFileScan → PhysicalPlanTranslator instanceof 路由（visitPhysicalFileScan 内 FileQueryScanNode 分支）
    → InfoSchemaScanNode (extends FileQueryScanNode)

BE: FileScanOperatorX → FileScanner
    → _get_next_reader() → InfoSchemaTableReader (extends GenericReader)
    → GenericReader::get_next_block()
```

**关键设计**：改造现有 `ExternalInfoSchemaTable`（`type=SCHEMA`，全仓 63 处引用，不新增表类型），在 `PhysicalPlanTranslator.visitPhysicalFileScan()` 的 if-else instanceof 链（`PhysicalPlanTranslator.java:769-822`）中新增 `ExternalInfoSchemaTable` 分支，路由到 `InfoSchemaScanNode`，完全复用普通表的扫描执行路径。系统表不是一个特殊的元数据查询，而是一个"表"，拥有自己的 schema、splits 和读取逻辑。此处不引入新表类型，避免"同一概念两个类"。

**源码依据**（FE 侧）：

| 组件 | 文件 | 行号 | 关键逻辑 |
|------|------|------|--------|
| `PhysicalPlanTranslator` | `fe/.../nereids/glue/translator/PhysicalPlanTranslator.java` | 769-822 | `visitPhysicalFileScan()` 内 if-else instanceof 链路由 → 创建对应 `FileQueryScanNode` 子类 |
| `FileScanNode.toThrift()` | `fe/.../datasource/FileScanNode.java` | 94-107 | 设置 `TPlanNodeType.FILE_SCAN_NODE`，创建 `TFileScanNode` |

**源码依据**（BE 侧）：

| 组件 | 文件 | 行号 | 关键逻辑 |
|------|------|------|--------|
| `FileScanOperatorX` 创建 | `be/src/exec/pipeline/pipeline_fragment_context.cpp` | 1338-1339 | `case FILE_SCAN_NODE` → 创建 `FileScanOperatorX` |
| `FileScanner` 创建 | `be/src/exec/operator/file_scan_operator.cpp` | 113-115 | `FileScanner::create_unique(...)` |
| `_cur_reader` 类型 | `be/src/exec/scan/file_scanner.h` | 146 | `std::unique_ptr<GenericReader> _cur_reader` |

### 1.2 GenericReader 接口契约（NVI 模式）

`GenericReader`（`generic_reader.h`）采用 **NVI（Non-Virtual Interface）模式**：对外接口为非虚 public 方法，内部调用 protected 纯虚钩子函数。子类须 override 的核心契约：
- `_do_get_next_block(Block*, size_t*, bool*)` 纯虚，实际数据读取逻辑。编译验证 V1：初稿误将 get_next_block 标记为 override 触发编译错误，根因即 NVI 模式
- `_do_init_reader(ReaderInitContext*)` 纯虚，reader 初始化，接收 ReaderInitContext 上下文
- `_get_columns_impl(name_to_type*)` 虚函数，列集报告，FileScanner 通过 public get_columns() 间接调用
- `close()` 有默认实现，按需 override

`InfoSchemaTableReader` 直接继承 `GenericReader`，按 NVI 模式实现 `_do_init_reader()`、`_do_get_next_block()`、`_get_columns_impl()` 三个 protected 钩子。`_get_columns_impl()` 逻辑简单：遍历 `_file_slot_descs` 填充 `name_to_type`，将所有列报告为存在（与 `JniReader` 实现一致），自行实现即可。

### 1.3 扩展机制

Table Reader 路径的扩展点是**单一的**——`FileScanner._get_next_reader()` 的 switch-case（`file_scanner.cpp:1027-1150`）。新增 `FORMAT_INFO_SCHEMA_TABLE` case 后，`FileScanner` 的所有调度、并行、内存管理能力自动复用。新增一种元数据类型只需在 `InfoSchemaTableReader` 中新增 case，不需要新增 Scanner 子类。

`FileScanner` 中文件相关逻辑对非文件 reader 无害（源码验证）：
- `_fill_missing_columns()`：`_missing_cols` 为空时直接返回 OK（`file_scanner.cpp:790-792`），`InfoSchemaTableReader.get_columns()` 报告所有列存在
- `_fill_columns_from_path()`：`_fill_partition_from_path` 为 false 时直接返回 OK（`file_scanner.cpp:761-763`）
- `_convert_to_output_block()`：query 模式下直接返回 OK（`file_scanner.cpp:832-833`）
- `_pre_filter_src_block()`：仅 load 模式生效（`file_scanner.cpp:607`）
- `_truncate_char_or_varchar_columns()`：`_source_file_col_name_types` 为空时使用 slot 自身 len（`file_scanner.cpp:613`）

具体设计如下：

### 1.4 层级抽象设计

> **设计核心**：当前设计只是把 `TMetaScanNode` 和 `TSchemaScanNode` 的字段机械 merge，没有理清每一层 Thrift/Java 类各自应处理什么。需要重新梳理层级抽象，将**数据源描述**、**查询上下文**、**分批控制**三层分离。

#### 现有代码中三层职责的分布（源码依据）

通过源码调研，现有三条路径中，三层职责的分布如下：

| 层次 | 职责 | TVF 路径 (TMetaScanNode + TMetaScanRange) | 系统表路径 (TSchemaScanNode) | Table Reader 路径 (TFileScanNode + TFileScanRange) |
|------|------|------|------|------|
| **Plan Node 层** | 查询上下文（跨所有 scan range 共享） | `TMetaScanNode`：仅 `tuple_id` + `metadata_type` + `current_user_ident`（3 字段，极简） | `TSchemaScanNode`：数据源描述 + 查询上下文 + FE 连接 + 谓词下推混在一起（16 字段） | `TFileScanNode`：仅 `tuple_id` + `table_name`（2 字段，极简） |
| **Scan Range 层** | 数据源描述 + 分片控制 | `TMetaScanRange`：`metadata_type` + 类型专用 params + `serialized_table`/`serialized_splits`/`hadoop_props`（17 字段） | 无独立 scan range（所有参数在 plan node 中） | `TFileScanRange`：`ranges`（`TFileRangeDesc` 列表）+ `params`（`TFileScanRangeParams`） |
| **RPC 层** | BE→FE 请求参数 | `TMetadataTableRequestParams`：tagged-union 风格 | `TSchemaTableRequestParams`：扁平结构 | 无 RPC（直接读文件） |

**设计原则**：Table Reader 路径遵循**"瘦 plan node + 胖 scan range"**的原则——plan node 层只放跨所有分片共享的查询上下文，数据源描述下沉到 scan range 层。

#### 三层分离设计

本方案复用 `TFileScanNode`（2 字段，极简）作为 plan node 层，通过 `TFileScanRange` / `TFileRangeDesc` 承载数据源描述和分批控制。三层职责明确分离：

```
┌─────────────────────────────────────────────────────────┐
│ Plan Node 层（TFileScanNode，复用现有）                  │
│ 职责：查询上下文（跨所有 scan range 共享）                │
│ 内容：tuple_id + table_name                              │
│ 对应 Java 类：InfoSchemaScanNode extends FileQueryScanNode│
├─────────────────────────────────────────────────────────┤
│ Scan Range 层（TFileScanRange + TFileRangeDesc，复用现有） │
│ 职责：数据源描述 + 分片控制                               │
│ 内容：format_type + table_format_params（数据源描述）      │
│       path/offset/size（分片控制，用于 profile 展示）      │
│ 对应 Thrift：TInfoSchemaFileDesc（新增，数据源专用参数）    │
├─────────────────────────────────────────────────────────┤
│ RPC 层（TFetchSchemaTableDataRequest，扩展现有）        │
│ 职责：BE→FE 回调请求参数                                  │
│ 内容：嵌套 TInfoSchemaFileDesc + 运行时上下文 + 分批控制    │
│ 对应 Java 类：MetadataGenerator.getInfoSchemaMetadata()    │
└─────────────────────────────────────────────────────────┘
```

#### 数据源抽象与系列抽层（回应 PMC 批注图1 + 意见①）

> **PMC 批注（图1）**："这里应该抽象成对一个数据源的描述。比如 FE 是数据源的一种。"
> **PMC 意见（①）**："catalog db 这些并不一定是通用信息，是不是可以放到 scan range 里？或者针对某个系列的元数据表，再单独抽一层？"

##### 数据源抽象：`TInfoSchemaSourceInfo`

初稿中 `current_user_ident`/`frontend_conjuncts`/`fe_addr_list` 是**顶层平铺**在 `TInfoSchemaFileDesc` 里的，隐含假设"数据源就是 FE"，没有"数据源"这个抽象概念。这正是 PMC 指出的问题——这些字段本就散落在现有两处结构（`TMetadataTableRequestParams` 的 `current_user_ident`/`fe_addr_list`、`TSchemaTableRequestParams` 的 `catalog`/`dbId`），方案不应机械 copy 两边 field，而应先抽象出"对数据源的描述"。

```thrift
// 新增结构 — 数据源抽象（"FE 是数据源的一种"）
struct TInfoSchemaSourceInfo {
    1: optional Types.TInfoSchemaSourceType source_type    // FE_METADATA / BE_LOCAL / ...
    // 编译验证 V4：实际无 source_name 字段
    3: optional list<Types.TNetworkAddress> fe_addr_list    // FE 数据源寻址（多 FE 聚合）
    4: optional Types.TUserIdentity current_user_ident     // 以谁的身份查询该数据源
    5: optional string frontend_conjuncts                  // 下推到数据源的谓词
}
```

**抽象原则**：
- **数据源是"查询对象"**：`source_type` 决定"去哪个数据源查"，`fe_addr_list` 是该数据源的寻址方式，`current_user_ident`/`frontend_conjuncts` 是"以谁的身份、带什么谓词去查"
- **FE 只是第一种数据源**：本方案 `source_type = FE_METADATA`。未来 BE 本地系列（`BackendPartitionedSchemaScanNode` 承载的 11 张表）如需统一，只需新增 `BE_LOCAL` 类型，复用同一抽象
- **消除字段散落**：`current_user_ident`/`fe_addr_list`/`frontend_conjuncts` 不再平铺在 `TInfoSchemaFileDesc`，而是 `TInfoSchemaSourceInfo` 的组成部分

##### 系列抽层：非通用过滤参数按类型抽子结构

`catalog`/`db`/`table`/`wild` 等过滤参数**不是所有元数据表都需要的通用信息**：
- `partitions`/`tables`/`columns` 等库表相关元数据需要 `catalog`/`db` 过滤
- `backends`/`frontends`/`brokers` 等全局元数据**不需要**任何 `db` 过滤

因此将这些字段抽为**系列专用子结构**（tagged-union 风格，与 `TMetaScanRange` 的类型专用 params 先例一致），由 `metadata_type` 决定携带哪个子结构：

```thrift
// 系列专用过滤参数（按 metadata_type 按需携带）
struct TInfoSchemaCatalogDbParams {
    1: optional string catalog
    2: optional i64 db_id
    3: optional string db
    4: optional string table
    5: optional string wild
}
struct TInfoSchemaShowHiddenParams {
    1: optional bool show_hidden_columns
}
```

**效果**：`TInfoSchemaFileDesc` 只保留三类语义——**数据源标识**（`metadata_type`/`output_mode`）+ **数据源抽象**（`TInfoSchemaSourceInfo`）+ **系列过滤参数**（按需子结构）。既回应 PMC"catalog/db 非通用信息"的意见，也回应"按系列再抽一层"的建议。

**系列子结构的映射规则**（按 `metadata_type` 决定携带哪个子结构）：

| 系列 | 示例 metadata_type | 携带子结构 | 说明 |
|------|-------------------|-----------|------|
| 库表相关 | `PARTITIONS`/`TABLES`/`COLUMNS`/`TABLES_CONFIG`/`SCHEMA_PRIVILEGES` 等 | `catalog_db_params` | 需要 `catalog`/`db`/`table`/`wild` 过滤 |
| 隐藏列类 | `COLUMNS`（含 `show_hidden_columns` 时）等 | `show_hidden_params` | 仅 `show_hidden_columns` |
| 全局表 | `BACKENDS`/`FRONTENDS`/`BROKERS`/`CATALOGS`/`WORKLOAD_GROUPS` 等 | **均不携带** | 无库表过滤维度 |

> 说明：`COLUMNS` 同时涉及两维（库表过滤 + 隐藏列开关），故可同时携带 `catalog_db_params` 与 `show_hidden_params`。映射在 FE 端 `getInfoSchemaMetadata()` 内按 `metadata_type` 确定，BE 端 Reader 原样透传 `TInfoSchemaFileDesc` 不感知子结构。

##### Schema（列集）抽象：列集 = f(metadata_type, output_mode)

> **抽象完备性补充**：前面补充了"数据源抽象"与"系列抽层"，但还有一个抽象维度必须显式化——**每张表的列集（schema）定义**。初稿隐含假设"所有表只有一套列集"，这不够：`TMetadataOutputMode`（`DORIS_NATIVE`/`MYSQL_COMPAT`）是方案**新增**的抽象（**现有代码无此枚举**——现有 `SchemaTable.TABLE_MAP` 只有一套 MySQL 兼容列集），需要明确列集的来源与一致性。

**列集函数**：每张 information_schema 表的列集 = `f(metadata_type, output_mode)`：
- `MYSQL_COMPAT`：MySQL 标准列集（即现有 `SchemaTable.TABLE_MAP.get(name).getFullSchema()` 的行为，**默认**）
- `DORIS_NATIVE`：Doris 原生扩展列集（未来能力，可携带更多 Doris 特有列；Phase 1 可不实现，仅保留枚举）

> **命名注意**：`DORIS_NATIVE` 与现有 `SysTableResolver.isNative()`（`systable/SysTableResolver.java:128`）中的 "native" **含义不同**——后者指"以 ExternalTable 形态存在的 native system table"（区别于 TVF 形态），与列集模式无关。若担心混淆可改名 `DORIS_EXTENDED`，本方案按 `DORIS_NATIVE` 描述。

**列集职责归属与一致性（关键设计约束）**：
- **plan 阶段定死**：列集由 `output_mode` 在 **plan 阶段**确定——`ExternalInfoSchemaTable.initSchema()` 按 `output_mode` 返回列集 → 经 `InfoSchemaScanNode` 的 tuple desc → BE 端 slot。**数据阶段不再切换列集**（FE 端 `getInfoSchemaMetadata()` 返回的 `TRow` 列序与 BE slot 严格一致，见 1.4.5 列对齐约定）
- **FE 数据生成同列序**：`getInfoSchemaMetadata()` 委托现有 `*MetadataResult` 时，按同一 `output_mode` 决定返回列序（`MYSQL_COMPAT` 即现有列序，无需裁剪/重排；`DORIS_NATIVE` 需新增对应扩展列集生成逻辑）
- **`show_hidden_columns` 与现有 session variable 的关系**：现有隐藏列由 session variable `show_hidden_columns` 控制（`Util.showHiddenColumns()`，`catalog/Table.java:407`）。方案的 `TInfoSchemaShowHiddenParams.show_hidden_columns` 是**查询级显式覆盖**（来自查询语境），优先级为"查询级 > session variable"，避免双轨不一致
- **新增 `TMetadataOutputMode` 枚举放置**：`gensrc/thrift/PlanNodes.thrift`（与 `TMetadataType` 同文件），取值 `DORIS_NATIVE`/`MYSQL_COMPAT`；默认 `MYSQL_COMPAT` 保持现有行为

#### 各层 Thrift 结构的职责梳理

**第 1 层：Plan Node 层 — 复用 `TFileScanNode`（不新增）**

```thrift
// 现有结构，不修改
struct TFileScanNode {
    1: optional TTupleId tuple_id
    2: optional string table_name
}
```

**职责**：仅传递 tuple 描述和表名，跨所有 scan range 共享。与 `TMetaScanNode`（3 字段）和 `TFileScanNode`（2 字段）的设计理念一致——plan node 层应保持极简。

**为什么不在 plan node 层放 catalog/db/thread_id 等字段**：
- `TMetaScanNode` 的设计已经证明：`catalog`/`db`/`table` 等数据源描述参数应放在 scan range 层（`TMetaScanRange`），而非 plan node 层
- `TFileScanNode` 的设计更进一步：连 `format_type` 都不在 plan node 中，而在每个 `TFileRangeDesc.format_type` 中
- `thread_id`/`time_zone` 等查询上下文通过 `RuntimeState` 在 BE 端直接获取，不需要通过 plan node 传递
- 因此，本方案完全复用 `TFileScanNode`，不在 plan node 层添加任何 information_schema 专用字段

**第 2 层：Scan Range 层 — 复用 `TFileScanRange` + `TFileRangeDesc`，新增 `TInfoSchemaFileDesc`**

```thrift
// 现有结构，不修改
struct TFileRangeDesc {
    // ... 现有字段（path, start_offset, size, format_type 等） ...
    8: optional TTableFormatFileDesc table_format_params  // 复用现有扩展点
    13: optional TFileFormatType format_type              // 设置为 FORMAT_INFO_SCHEMA_TABLE
}

// 现有结构，新增字段
struct TTableFormatFileDesc {
    1: optional string table_format_type  // 设置为 "info_schema"
    // ... 现有字段（iceberg_params, paimon_params 等） ...
    15: optional TInfoSchemaFileDesc info_schema_params  // 编译验证 V4：实际字段号 15（13/14 已被其他格式占用）
}

// 新增结构 — information_schema 数据源专用参数（数据源描述的唯一来源）
//
// 内部按职责分为三组，虽然都在 scan range 层，但语义边界清晰：
//   - 数据源标识：决定"查什么表"，FE 端 MetadataGenerator 据此分发
//   - 查询上下文：决定"以谁的身份查"，FE 端用于权限校验和谓词下推
//   - 过滤参数：决定"查哪些行"，不同元数据类型按需携带不同子集
struct TInfoSchemaFileDesc {
    // === 数据源标识（决定查什么表） ===
    1: optional Types.TMetadataType metadata_type
    2: optional TMetadataOutputMode output_mode  // DORIS_NATIVE / MYSQL_COMPAT

    // === 数据源抽象（对"查询对象"的一层描述，FE 是数据源的一种） ===
    3: optional TInfoSchemaSourceInfo source_info

    // === 系列专用过滤参数（按 metadata_type 按需携带，非通用字段） ===
    4: optional TInfoSchemaCatalogDbParams catalog_db_params    // partitions/tables/columns 等
    5: optional TInfoSchemaShowHiddenParams show_hidden_params  // show_hidden_columns 类
}
```

> **设计说明**：`thread_id`/`time_zone` 不在此结构中。它们是**查询运行时上下文**而非数据源描述——BE 端可直接从 `RuntimeState` 获取（`state->thread_id()`、`state->timezone()`），不需要通过 plan node 或 scan range 层传递。FE 端生成元数据时如需这些值（如 `partitions` 表的 `last_consistency_check_time` 依赖时区），由 BE 端在组装 RPC 请求时从 `RuntimeState` 提取并填入 RPC 层参数。这与本节上文"为什么不在 plan node 层放 catalog/db/thread_id 等字段"的论述保持一致。
>
> **字段语义（本版调整）**：`current_user_ident`/`frontend_conjuncts`/`fe_addr_list` 移入 `TInfoSchemaSourceInfo`（数据源抽象）；`catalog`/`db_id`/`db`/`table`/`wild`/`show_hidden_columns` 移入系列专用子结构。`TInfoSchemaFileDesc` 自身只保留标识 + 抽象 + 系列参数三个职责，详见上文"数据源抽象与系列抽层"。

**职责**：`TFileRangeDesc` 描述单个数据分片的物理信息（path/offset/size，用于 profile 展示），`TInfoSchemaFileDesc` 描述 information_schema 数据源的专用参数（标识 + 数据源抽象 + 系列过滤）。两者通过 `TTableFormatFileDesc` 桥接，遵循 Iceberg/Paimon 等数据湖的扩展先例。

**为什么 catalog/db 作为系列子结构放在 scan range 层而非 plan node 层**：
- `catalog`/`db`/`table`/`wild` 是**系列专用过滤参数**，不同元数据类型有不同的过滤维度（如 `backends` 表不需要 `db_id`，`partitions` 表需要 `db_id`），放在 scan range 层并**按系列抽成子结构**，可以按需携带（见上文"系列抽层"）
- 这与 `TMetaScanRange` 的设计一致：`backends_params`/`frontends_params`/`partitions_params` 等类型专用参数都在 scan range 层
- 如果放在 plan node 层或顶层平铺，所有元数据类型都要携带所有过滤字段，造成语义污染

**第 3 层：RPC 层 — 复用现有 `fetchSchemaTableData` RPC 体系，扩展而非新建**

> **关键发现**：现有代码中已有完整的 RPC 体系可复用——`fetchSchemaTableData` RPC 方法（`FrontendService.thrift:1861`）+ `TFetchSchemaTableDataRequest`/`TFetchSchemaTableDataResult` 结构 + `MetadataGenerator.getSchemaTableData()`/`getMetadataTable()` 入口。新设计**不应新建 RPC 方法和结构**，而应扩展现有体系。

> **现有实现确认（第 3 分支）**：`FrontendServiceImpl.fetchSchemaTableData()`（`fe/.../service/FrontendServiceImpl.java:2628-2644`）当前仅 **2 个分支**：
> - `schema_table_name == METADATA_TABLE` → `MetadataGenerator.getMetadataTable()`（TVF 路径）
> - 其他 → `MetadataGenerator.getSchemaTableData()`（Schema 表路径）
>
> 本方案在 RPC **服务层**新增 **第 3 个分支**：`isSetInfoSchemaParams()` → `MetadataGenerator.getInfoSchemaMetadata()`。这是"每层 thrift/Java 职责梳理"的一部分——RPC 服务层负责**按 tagged-union 分派**，不重复实现任何数据生成逻辑。伪代码：
>
> ```java
> // FrontendServiceImpl.fetchSchemaTableData() 新增分支（保持现有分支顺序）
> if (request.getSchemaTableName() == TSchemaTableName.METADATA_TABLE) {
>     return MetadataGenerator.getMetadataTable(request);      // TVF 路径（现有）
> }
> if (request.isSetInfoSchemaParams()) {
>     return MetadataGenerator.getInfoSchemaMetadata(request); // information_schema 新路径（新增）
> }
> return MetadataGenerator.getSchemaTableData(request);        // Schema 表路径（现有）
> ```

**现有 RPC 体系分析**（源码依据）：

| 现有结构 | 文件:行号 | 字段 | 新设计对应 |
|---------|----------|------|------------|
| `TFetchSchemaTableDataRequest` | `FrontendService.thrift:933-938` | `cluster_name` + `schema_table_name` + `metada_table_params`(TVF) + `schema_table_params`(Schema表) | 新增 `info_schema_params` 字段，与现有两个 params 平级 |
| `TFetchSchemaTableDataResult` | `FrontendService.thrift:940-943` | `status` + `data_batch` | 新增 `has_more` 字段（分批标记） |
| `TSchemaTableRequestParams` | `FrontendService.thrift:919-931` | `columns_name`/`current_user_ident`/`catalog`/`dbId`/`time_zone`/`frontend_conjuncts`/`thread_id` | **7 个字段与新设计完全重叠**，但新设计通过 `TInfoSchemaFileDesc` 嵌套消除重复 |
| `MetadataGenerator.getSchemaTableData()` | `MetadataGenerator.java:326` | 现有 Schema 表入口 | 新增 `getInfoSchemaMetadata()` 平级方法 |
| `MetadataGenerator.getMetadataTable()` | `MetadataGenerator.java:267` | 现有 TVF 入口 | 不受影响 |

**扩展方案**（扩展现有结构，不新建 RPC 方法）：

```thrift
// FrontendService.thrift — 扩展现有结构（不新建）

// 现有结构，新增 info_schema_params 字段（与 metada_table_params/schema_table_params 平级）
struct TFetchSchemaTableDataRequest {
    1: optional string cluster_name
    2: optional TSchemaTableName schema_table_name
    3: optional TMetadataTableRequestParams metada_table_params  // used for tvf
    4: optional TSchemaTableRequestParams schema_table_params    // used for old SchemaScanNode path
    5: optional TInfoSchemaFileDesc info_schema_params           // 新增：used for new Table Reader path
    6: optional i64 offset                                       // 新增：分批控制
    7: optional i32 batch_size                                   // 新增：分批控制
    8: optional i64 thread_id                                    // 新增：运行时上下文（BE 从 RuntimeState 获取）
    9: optional string time_zone                                 // 新增：运行时上下文
    10: optional list<string> columns_name                       // 新增：列投影
}

// 现有结构，新增 has_more 字段
struct TFetchSchemaTableDataResult {
    1: required Status.TStatus status
    2: optional list<Data.TRow> data_batch
    3: optional bool has_more  // 新增：分批标记
}

// 现有 RPC 方法，不新增（fetchSchemaTableData 已在 FrontendService.thrift:1861）
// BE 端 InfoSchemaTableReader 调用现有 fetchSchemaTableData，通过 info_schema_params 字段区分本路径
```

**职责**：RPC 层复用现有 `fetchSchemaTableData` 入口，通过 `TFetchSchemaTableDataRequest.info_schema_params` 字段（类型 `TInfoSchemaFileDesc`）携带数据源描述。BE 端 `InfoSchemaTableReader` 从 scan range 层提取 `TInfoSchemaFileDesc`，从 `RuntimeState` 提取运行时上下文（`thread_id`/`time_zone`），加上分批控制（`offset`/`batch_size`）和列投影（`columns_name`），组装成 `TFetchSchemaTableDataRequest`，通过现有 `fetchSchemaTableData` RPC 发送给 FE。FE 端 `MetadataGenerator` 新增 `getInfoSchemaMetadata()` 方法处理 `info_schema_params` 分支。

**为什么复用现有 RPC 而非新建**（核心原则："不是简单的 merge 两边的 field"）：
- 现有 `TSchemaTableRequestParams` 已有 7 个与新设计完全相同的字段（`columns_name`/`current_user_ident`/`catalog`/`dbId`/`time_zone`/`frontend_conjuncts`/`thread_id`），新建 RPC 结构会导致**第三层重复**——scan range 层与 RPC 层重复（已通过嵌套解决）之外，新 RPC 结构与现有 `TSchemaTableRequestParams` 也重复
- 现有 `TFetchSchemaTableDataResult`（`status` + `data_batch`）若新建 `TInfoSchemaMetadataResult` 只差一个 `has_more` 字段，扩展现有结构即可，新建是浪费
- 现有 `fetchSchemaTableData` RPC 已是统一入口，通过 tagged-union 区分 TVF 路径（`metada_table_params`）和 Schema 表路径（`schema_table_params`），本方案只需新增 `info_schema_params` 作为第三个分支
- 复用现有 RPC 还能复用 FE 端的 `MetadataGenerator` 分发逻辑、`filterColumns()` 列过滤逻辑、错误处理逻辑

**关于 `TInfoSchemaMetadataRequestParams`（PMC 点名类）**：方案**不新建**该类。PMC 意见中的 `TInfoSchemaMetaScanNode`/`TInfoSchemaMetadataRequestParams` 是初稿 `TMetaScanNode` 路线的产物；本方案已整体迁移到 Table Reader 路线（`TFileScanNode` + `TFileScanRange`），对应的请求参数由 `TFetchSchemaTableDataRequest.info_schema_params`（类型 `TInfoSchemaFileDesc`）承担，与现有 `metada_table_params`/`schema_table_params` 平级。若再新建 `TInfoSchemaMetadataRequestParams`，会与 `TInfoSchemaFileDesc` 形成第三层结构重复（scan range 层已定义、RPC 层又定义一遍），违反"不重复抽象"原则——这正对应 PMC"不是简单的 merge 两边的 field"的告诫。

**关于初稿 `TInfoSchemaMetaScanRange` 的通用参数（PMC 图2 红框标注）**：初稿中 `hadoop_props`/`serialized_table`/`serialized_splits`/`parquet_params` 是 `TMetaScanRange` 为**湖表 TVF**（如 `iceberg_*`/`paimon_*`）保留的字段。本方案仅针对 FE 元数据系列，**不需要**这些序列化/湖参数——FE 元数据通过 `TInfoSchemaFileDesc` 结构化描述即可，无需序列化整表/整 split。因此这些字段随 `TMetaScanRange` 一起被废弃，不进入新结构（与 1.1 节"不涉及外部湖系统表"的范围声明一致）。

**为什么 `TInfoSchemaFileDesc` 嵌套在 request 中而非重复定义字段**：

`TInfoSchemaFileDesc` 在 scan range 层定义，包含 `metadata_type`/`catalog`/`db_id` 等数据源描述字段。RPC 层的 `TFetchSchemaTableDataRequest` 通过 `info_schema_params` 字段**直接嵌套整个 `TInfoSchemaFileDesc`**，而不是在 request 中再写一遍这些字段。这样数据源描述只在一处定义（scan range 层），RPC 层引用而非复制：

- **scan range 层**（`TInfoSchemaFileDesc`）：定义数据源标识（`metadata_type`/`output_mode`）+ 数据源抽象（`TInfoSchemaSourceInfo`：`current_user_ident`/`frontend_conjuncts`/`fe_addr_list`）+ 系列专用过滤参数（`TInfoSchemaCatalogDbParams`/`TInfoSchemaShowHiddenParams`）— 这些字段在 FE plan 阶段填充，经 `TFileRangeDesc.table_format_params` 传递到 BE
- **RPC 层**（`TFetchSchemaTableDataRequest`）：仅新增 `info_schema_params`（嵌套 `TInfoSchemaFileDesc`）+ `offset`/`batch_size`/`thread_id`/`time_zone`/`columns_name` — 后者是 RPC 层独有参数（分批控制 + 运行时上下文 + 列投影），不放入 `TInfoSchemaFileDesc`
- 这与现有 `TFetchSchemaTableDataRequest` 的 tagged-union 风格一致：`metada_table_params`/`schema_table_params`/`info_schema_params` 各自携带路径专用参数

#### 各层 Java 类的职责梳理

| 层次 | Java 类 | 职责 | 继承关系 |
|------|---------|------|----------|
| **表层** | `ExternalInfoSchemaTable`（改造现有类） | 数据源描述的构造起点：构造 `TInfoSchemaFileDesc`（填充数据源标识），提供 schema | `extends ExternalTable`（现有，type=SCHEMA） |
| **Plan Node 层** | `InfoSchemaScanNode` | 持有 `TInfoSchemaFileDesc`（由表层传入），补充数据源抽象和系列过滤参数，设置到 scan range | `extends FileQueryScanNode` |
| **RPC 层（FE）** | `MetadataGenerator.getInfoSchemaMetadata()` | 数据生成：委托现有 `*MetadataResult` + output_mode 列集切换 + 分批切片，与现有 `getSchemaTableData()`/`getMetadataTable()` 平级 | 现有类新增方法 |
| **Reader 层（BE）** | `InfoSchemaTableReader` | 数据获取：分批 RPC + block 填充 | `extends GenericReader` |

**ExternalInfoSchemaTable（表层，改造现有类）**：

```java
// 改造现有类：fe/.../datasource/infoschema/ExternalInfoSchemaTable.java
// 原则：information_schema 表已是 ExternalTable 子类（TableType.SCHEMA，全仓 63 处引用），
//       不新建第二个 ExternalTable 概念；在现有类上增加 Reader 路径的数据源描述承载能力。
public class ExternalInfoSchemaTable extends ExternalTable {
    // 新增：数据源描述（TInfoSchemaFileDesc 的唯一载体），懒构造，查询时才填充
    private TInfoSchemaFileDesc infoSchemaDesc;

    // 现有构造签名保持不变（ExternalInfoSchemaDatabase.buildTableInternal():47-50 仍负责创建实例）
    public ExternalInfoSchemaTable(long id, String name, ExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, name, catalog, db, TableType.SCHEMA);
    }

    // 新增：按 metadata_type 构造数据源标识（查询时由 InfoSchemaScanNode 补充数据源抽象与系列参数）
    public TInfoSchemaFileDesc getInfoSchemaDesc(TMetadataType metadataType,
                                                 TMetadataOutputMode outputMode) {
        if (infoSchemaDesc == null) {
            infoSchemaDesc = new TInfoSchemaFileDesc();
            infoSchemaDesc.setMetadataType(metadataType);
            infoSchemaDesc.setOutputMode(outputMode);
        }
        return infoSchemaDesc;
    }

    // 现有 initSchema() 保持不变（SchemaTable.TABLE_MAP.get(name).getFullSchema()，MySQL 兼容列集）。
    // 新路径下 initSchema() 按 output_mode 返回列集：
    //   - MYSQL_COMPAT：现有列集（默认，Phase 1 行为）
    //   - DORIS_NATIVE：Doris 原生扩展列集（未来能力）
    // 列集在 plan 阶段定死，数据阶段不再切换（见 1.4.2 Schema 抽象）
}
```

**职责**：数据源描述的**构造起点**——在现有 `ExternalInfoSchemaTable` 上新增 `getInfoSchemaDesc()`，构造 `TInfoSchemaFileDesc` 并填充数据源标识（`metadata_type`/`output_mode`），传递给 `InfoSchemaScanNode`。不涉及查询执行逻辑。`PhysicalPlanTranslator.visitPhysicalFileScan()`（`PhysicalPlanTranslator.java:769-822` 的 if-else instanceof 链）新增 `ExternalInfoSchemaTable` 分支路由到 `InfoSchemaScanNode`。现有 `SCHEMA` 查询路径（`ExternalInfoSchemaDatabase.buildTableInternal()` 创建的表直接查）不受影响，两条路径共用同一个表对象。

**InfoSchemaScanNode（Plan Node 层）**：

> **命名说明**：对应 PMC 意见中的 `InfoSchemaMetaScanNode` 概念。采用 `InfoSchemaScanNode` 命名以避免与现有 `SchemaScanNode`（`SchemaScanNode.java:53`，extends `ScanNode`）混淆——本类是 `FileQueryScanNode` 子类，走 Table Reader 路径，与旧路径的 `SchemaScanNode` 是两条不同链路。

```java
// 新增类：fe/.../datasource/infoschema/source/InfoSchemaScanNode.java
public class InfoSchemaScanNode extends FileQueryScanNode {
    // 继承 FileQueryScanNode，复用：
    //   - createScanRangeLocations() 的 split 生成和分配
    //   - toThrift() 生成 TFileScanNode（极简，仅 tuple_id + table_name）
    // 注意：override getNumInstances() 返回 1（单实例，见下），
    //       与 SchemaScanNode 单实例语义一致，避免多 BE 重复拉取 FE 元数据

    // 直接持有 TInfoSchemaFileDesc，避免分散字段与 Thrift 结构重叠
    // 由 ExternalInfoSchemaTable 构造时传入（已填充数据源标识）
    // 此处补充数据源抽象（source_info）和系列过滤参数，然后设置到 scan range
    private final TInfoSchemaFileDesc infoSchemaDesc;

    @Override
    protected TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_INFO_SCHEMA_TABLE;  // 新增枚举值
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        // infoSchemaDesc 由表层构造（数据源标识），此处补充数据源抽象和系列过滤参数
        TInfoSchemaSourceInfo sourceInfo = new TInfoSchemaSourceInfo();
        sourceInfo.setSourceType(TInfoSchemaSourceType.FE_METADATA);
        sourceInfo.setFeAddrList(...);
        sourceInfo.setCurrentUserIdent(...);
        sourceInfo.setFrontendConjuncts(...);
        infoSchemaDesc.setSourceInfo(sourceInfo);

        // 系列过滤参数：库表相关元数据携带 catalog_db_params
        TInfoSchemaCatalogDbParams catalogDbParams = new TInfoSchemaCatalogDbParams();
        catalogDbParams.setCatalog(...);
        catalogDbParams.setDbId(...);
        infoSchemaDesc.setCatalogDbParams(catalogDbParams);
        // show_hidden_columns 类：设置 show_hidden_params
        // 注意：thread_id/time_zone 不在此设置，由 BE 端从 RuntimeState 获取

        // 桥接到 TTableFormatFileDesc（遵循 Iceberg/Paimon 先例）
        TTableFormatFileDesc tableFormatParams = new TTableFormatFileDesc();
        tableFormatParams.setTableFormatType("info_schema");
        tableFormatParams.setInfoSchemaParams(infoSchemaDesc);
        rangeDesc.setTableFormatParams(tableFormatParams);
    }

    @Override
    protected List<Split> getSplits(int numBackends) {
        // information_schema 无文件分片，创建一个虚拟 split
        // path 设置为有意义的标识（如 "fe://information_schema/tables"），仅用于 profile
        return Collections.singletonList(createVirtualSplit());
    }

    @Override
    public int getNumInstances() {
        // 关键：FE 元数据系列数据源是全局唯一的 FE 元数据，必须单实例执行。
        // 与现有 SchemaScanNode.getNumInstances() 返回 1（SchemaScanNode.java:184-186）保持一致；
        // 否则多个 BE 实例各自向 FE 拉取全量元数据，会导致数据重复 + N 倍 RPC 压力。
        // FileQueryScanNode 默认实现 = scanRangeLocations.size()（此处为 1 个虚拟 split），
        // 但 session variable ignoreStorageDataDistribution=true 时会退化为
        // parallelExecInstanceNum，故必须显式 override 固定为 1。
        return 1;
    }
}
```

**职责**：
- **不 override `toThrift()`**：完全复用 `FileScanNode.toThrift()` 生成极简的 `TFileScanNode`
- **override `getFileFormatType()`**：返回 `FORMAT_INFO_SCHEMA_TABLE`，设置到每个 `TFileRangeDesc.format_type`
- **override `setScanParams()`**：在 `TInfoSchemaFileDesc` 中补充数据源抽象（`TInfoSchemaSourceInfo`：`current_user_ident`/`frontend_conjuncts`/`fe_addr_list`）和系列过滤参数（`TInfoSchemaCatalogDbParams`/`TInfoSchemaShowHiddenParams`），然后通过 `TTableFormatFileDesc` 设置到 `TFileRangeDesc`
- **override `getSplits()`**：创建虚拟 split（information_schema 无文件分片）
- **override `getNumInstances()` 返回 1**：单实例执行，与 `SchemaScanNode.getNumInstances()`（`SchemaScanNode.java:184-186`）一致。FE 元数据系列数据源是全局唯一的 FE 元数据，多实例会让多个 BE 各自拉取全量，导致**数据重复 + N 倍 RPC 压力**（`FileQueryScanNode.getNumInstances()` 默认 = `scanRangeLocations.size()`，且 `ignoreStorageDataDistribution=true` 时会退化为 `parallelExecInstanceNum`，必须显式固定为 1）
- **谓词可下推性收集**：`frontend_conjuncts` 的来源复用旧路径机制——现有 `SchemaScanNode` 持可下推谓词列表并在 `toThrift()` 时 JSON 序列化下发（`SchemaScanNode.java:63,167`）。新路径 `InfoSchemaScanNode` 在 `setScanParams()` 时收集本节点 conjuncts 中可下推给 FE 的部分，填入 `TInfoSchemaSourceInfo.frontend_conjuncts`（可下推性判定沿用旧路径规则）
- **`fe_addr_list` 来源**：数据源寻址信息由 `InfoSchemaScanNode` 从 FE 侧 `Env.getCurrentEnv()` 的 FE 地址列表获取（多 FE 集群的 `frontends` 地址），填入 `TInfoSchemaSourceInfo.fe_addr_list`

> **设计说明**：`TInfoSchemaFileDesc` 是数据源描述在 FE 侧的**唯一载体**，全程以整体结构传递：`ExternalInfoSchemaTable` 构造（填充数据源标识）→ `InfoSchemaScanNode` 补充（数据源抽象 + 系列过滤参数）→ `TFileRangeDesc` 承载 → `InfoSchemaTableReader` 接收 → `TFetchSchemaTableDataRequest.info_schema_params` 嵌套。消除了 Java 类字段与 Thrift 结构的字段重叠——不再有分散的 `metadataType`/`outputMode`/`frontendConjuncts`/`feAddrList`/`catalog`/`dbId` 等字段，`setScanParams()` 只需补充查询时才能确定的参数，然后整体桥接。

**InfoSchemaTableReader（BE Reader 层）**：

```cpp
// 新增类：be/src/format/table/info_schema_table_reader.h/.cpp
// 直接继承 GenericReader（与 WalReader 相同模式），不继承 JniReader
// 原因：本方案通过 Thrift RPC 获取数据，不涉及 JNI 调用
class InfoSchemaTableReader : public GenericReader {
public:
    InfoSchemaTableReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                          RuntimeState* state,
                          const TFileRangeDesc& range);

    ~InfoSchemaTableReader() override = default;

    // GenericReader NVI 模式：override protected 钩子而非 public 接口
    // 编译验证 V1：直接 override get_columns/get_next_block 会编译失败
protected:
    Status _do_init_reader(ReaderInitContext* ctx) override;
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    void _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;
public:
    Status close() override;

private:
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    // 编译验证 V3：实际无 _profile 成员，profile 由 ReaderInitContext 传入

    // 从 TInfoSchemaFileDesc 提取的数据源描述（直接持有整个结构）
    TInfoSchemaFileDesc _info_schema_desc;

    // 从 RuntimeState 获取的查询运行时上下文（不经 scan range 层传递）
    // 编译验证 V3：实际无 _thread_id，thread_id 由 BE 从 RuntimeState 按需获取
    std::string _time_zone;

    // 分批 RPC 状态
    std::vector<TRow> _batch_data;
    size_t _batch_offset = 0;
    bool _has_more = true;
    int64_t _total_fetched_rows = 0;
    int32_t _batch_size = 4096;

    Status _fetch_next_batch();  // 组装 TFetchSchemaTableDataRequest，调用 fetchSchemaTableData
};
```

**为什么继承 `GenericReader` 而非 `JniReader`**：
- `JniReader` 的核心是 `_jni_connector`（`jni_reader.h:86`），通过 JNI 调用 Java 侧 `JniScanner` 获取数据（`jni_reader.h:58-59`）。`PaimonJniReader` 继承它是因为 Paimon 的数据读取逻辑在 Java 侧
- 本方案通过 **Thrift RPC**（`fetchSchemaTableData`）获取数据，不涉及 JNI，继承 `JniReader` 会引入无用的 `_jni_connector` 依赖
- `WalReader`（`wal_reader.h:26`）同样直接继承 `GenericReader`，因为它也是用 C++ 自行获取数据而非通过 JNI
- `_get_columns_impl()` 逻辑简单（遍历 `_file_slot_descs` 填充 `name_to_type`，与 `JniReader` 实现一致），按 NVI 模式自行实现即可

**职责**：
- 从 `TFileRangeDesc.table_format_params.info_schema_params`（`TInfoSchemaFileDesc`）提取数据源描述，直接持有整个结构
- 从 `RuntimeState` 获取查询运行时上下文（`thread_id`/`time_zone`），不经 scan range 层传递
- 组装 `TFetchSchemaTableDataRequest`：设置 `info_schema_params`（嵌套 `TInfoSchemaFileDesc`）+ `offset`/`batch_size`（分批控制）+ `thread_id`/`time_zone`（运行时上下文）+ `columns_name`（列投影）
- 通过现有 `fetchSchemaTableData` RPC 分批获取数据（复用现有 RPC，不新建）
- `get_columns()` 报告所有列存在，`_missing_cols` 为空
- **列对齐约定**：FE 端返回的 `TRow` 字段序须与 BE 端 `_file_slot_descs` 顺序一致（即与 `get_columns()` 报告的 `name_to_type` 顺序一致）。`getInfoSchemaMetadata()` 委托现有 `*MetadataResult` 时按 `output_mode` 决定的列序返回（`MYSQL_COMPAT` 即 `SchemaTable.TABLE_MAP.get(name).getFullSchema()` 列序；`DORIS_NATIVE` 用对应扩展列集，见 1.4.2 Schema 抽象），BE 端按 slot 位置填充 `Block`，无需列名匹配

**分批 RPC 的 offset 推进**（`_fetch_next_batch()` 内）：
```
offset = _total_fetched_rows;             // 本次请求从累计行数开始
resp = fetchSchemaTableData(req);          // req.offset = offset, req.batch_size = _batch_size
_batch_data = resp.data_batch;
_has_more = resp.has_more;
_total_fetched_rows += _batch_data.size(); // 下一次 offset 自动推进
```
FE 端 `getInfoSchemaMetadata()` 按 `offset`/`batch_size` 切片（配合查询级缓存时从缓存切片），`has_more` 由 FE 端按 `offset + batch_size < data.size()` 判定（委托现有 `*MetadataResult` 返回全量 `List<TRow>` 后切片，判定简单确定）。

#### FileScanner 扩展

不需要继承 `FileScanner`，只需要实现 reader。`FileScanner` 已经是通用 scanner，通过 `_get_next_reader()` 的 switch-case 分发到各 format 的 reader。本方案只需在 switch-case 中添加一个 case，`FileScanner` 的所有调度、并行、内存管理能力自动复用。

**源码依据**：
- `FileScanner` 直接继承 `Scanner`（`file_scanner.h:60`），`_get_next_reader()` 是 **protected 非虚函数**（`file_scanner.h:127`），无法 override
- `MetaScanner` 也直接继承 `Scanner`（`meta_scanner.h:47`），与 `FileScanner` 是平级关系，不是父子关系
- `SchemaScanner` 完全不在 `Scanner` 体系中（`schema_scanner.h:83`，独立基类）
- 因此，"继承 FileScanner" 在技术上不可行（`_get_next_reader()` 不是 virtual），在架构上也不必要——`FileScanner` 已经是通用 scanner，只需添加 case 即可

```cpp
// file_scanner.cpp — _get_next_reader() 新增分支（file_scanner.cpp:1027-1150 现有 switch-case）
case TFileFormatType::FORMAT_INFO_SCHEMA_TABLE: {
    auto reader = InfoSchemaTableReader::create_unique(
        _file_slot_descs, _state, range);
    // 编译验证 V3：初始化通过 ReaderInitContext，非无参 init_reader()
    ReaderInitContext ctx;
    _fill_base_init_context(&ctx);
    init_status = static_cast<GenericReader*>(reader.get())->init_reader(&ctx);
    _cur_reader = std::move(reader);
    break;
}
```

**FileScanner 中文件相关逻辑对非文件 reader 无害**（源码验证）：
- `_fill_missing_columns()`：`_missing_cols` 为空时直接返回 OK（`file_scanner.cpp:790-792`），`JniReader.get_columns()` 报告所有列存在
- `_fill_columns_from_path()`：`_fill_partition_from_path` 为 false 时直接返回 OK（`file_scanner.cpp:761-763`）
- `_convert_to_output_block()`：query 模式下直接返回 OK（`file_scanner.cpp:832-833`）
- `_pre_filter_src_block()`：仅 load 模式生效（`file_scanner.cpp:607`）
- `_truncate_char_or_varchar_columns()`：`_source_file_col_name_types` 为空时使用 slot 自身 len（`file_scanner.cpp:613`）
- **参考先例**：`PaimonJniReader`、`WalReader` 等非文件 reader 已在 `FileScanner` 中正常运行

### 1.5 方案特性

1. **不需要独立的 Scanner**：`FileScanner` 已经是通用 scanner，支持所有 reader 类型。`InfoSchemaTableReader` 只需实现 `GenericReader::get_next_block()` 接口，即可被 `FileScanner` 调度（源码依据：`file_scanner.cpp:588-589` 通过 `_cur_reader->get_next_block()` 统一分发）
2. **自动获得 ScannerScheduler 的异步调度能力**：异步预取不在 FileScanner 内部，而在 `ScannerScheduler`（线程池调度，`scanner_scheduler.h:103`）+ `ScannerContext`（block 队列 + 内存池，`scanner_context.h:108`）+ `ScanLocalState._scan_dependency`（Pipeline 阻塞/唤醒，`scan_operator.h:104`）三层架构中实现。所有通过 FileScanner 创建的 reader 自动被 ScannerScheduler 调度，无需重新实现
3. **自动获得 Adaptive Batch Size 的分批交付能力**：`AdaptiveBlockSizePredictor`（`adaptive_block_size_predictor.h`）通过 EWMA 预测每行字节数，动态调整 `reader->set_batch_size()`（`file_scanner.cpp:580-581`）。`FORMAT_JNI` 已在支持列表中（`file_scanner.cpp:235-260`），新增 `FORMAT_INFO_SCHEMA_TABLE` 只需在 `_should_enable_adaptive_batch_size()` 中加入即可
4. **扩展性强**：未来新增元数据类型只需在 `InfoSchemaTableReader` 中新增 case，不需要新增 Scanner 子类

### 1.6 注意事项

1. **`TFileScanRange` 的参数承载**：`TFileRangeDesc` 已有成熟的扩展模式——通过 `table_format_params`（类型 `TTableFormatFileDesc`）传递格式专用参数。现有先例包括 `TIcebergFileDesc`、`TPaimonFileDesc`、`TTrinoConnectorFileDesc` 等（`gensrc/thrift/PlanNodes.thrift:415-426`）。新增 `TInfoSchemaFileDesc` 遵循相同模式即可。`TFileRangeDesc.path` 字段建议设置有意义的标识（如 `fe://information_schema/tables`），仅用于错误消息和 profile 展示（源码依据：`file_scanner.cpp:1072` 设置 `_current_range_path`，仅用于 `st.append(". cur path: " + get_current_scan_range_name())`）
2. **`FileScanner` 的文件相关逻辑对非文件 reader 无害**：经源码验证，`FileScanner` 中的文件相关逻辑对非文件 reader **不会造成功能错误**。具体分析：
   - `_fill_missing_columns()`：当 `_missing_cols` 为空时直接返回 OK（`file_scanner.cpp:790-792`）。只要 `InfoSchemaTableReader.get_columns()` 正确报告所有列为存在，`_missing_cols` 即为空
   - `_fill_columns_from_path()`：当 `_fill_partition_from_path` 为 false 时直接返回 OK（`file_scanner.cpp:761-763`）。FE 元数据无分区列，`_fill_partition_from_path` 默认为 false，此逻辑为空操作
   - `_convert_to_output_block()`：query 模式下直接返回 OK（`file_scanner.cpp:832-833`）
   - `_pre_filter_src_block()`：仅 load 模式生效，query 模式下直接返回 OK（`file_scanner.cpp:607`）
   - `_truncate_char_or_varchar_columns()`：当 `_source_file_col_name_types` 为空时，使用 slot 自身 len 执行 truncate，行为正确（`file_scanner.cpp:613`）
   - **参考先例**：`PaimonJniReader`、`WalReader` 等非文件 reader 已在 `FileScanner` 中正常运行，无需任何特殊跳过逻辑
3. **`TFileFormatType` 语义扩展**：`FORMAT_INFO_SCHEMA_TABLE` 不是文件格式，而是数据源类型。现有枚举中已有类似先例——`FORMAT_JNI`（值 11）不是文件格式而是读取方式标识，`FORMAT_NATIVE`（值 18）是 Doris 内部格式。`FORMAT_INFO_SCHEMA_TABLE`（值 21）延续此模式，将枚举语义从"文件格式"自然扩展为"数据源格式"（注意：值 19/20 已被 `FORMAT_LANCE`/`FORMAT_ES_HTTP` 占用，取值 21 避开冲突）。BE 侧 `FileScanner._get_next_reader()` 已通过 switch-case 统一分发，新增 case 不影响现有文件格式路径
4. **空结果集处理**：`InfoSchemaTableReader._fetch_next_batch()` 首次 RPC 可能返回空 `data_batch` + `has_more=false`（如查询不存在的表名）。此时 `get_next_block()` 需正确设置 `eof=true` 并返回 OK，不应视为错误。伪代码逻辑：`_batch_data` 为空且 `!_has_more` 时，直接走 `*eof = true; return Status::OK()` 分支，与正常结束一致
5. **RPC 失败重试**：`fetchSchemaTableData` 是 BE→FE 的 Thrift RPC，可能因 FE 重启、网络超时等失败。重试策略：
   - **重试次数**：复用 `ThriftRpcHelper::rpc()` 的默认重试机制（`thrift_rpc_helper.cpp` 中已有重试逻辑，默认 3 次）
   - **重试失败处理**：重试耗尽后返回错误状态，`FileScanner` 将错误向上传播，查询失败并向用户报错
   - **target FE 选择**：BE 端从 `TInfoSchemaSourceInfo.fe_addr_list` 中选择 RPC 目标——对**多 FE 聚合表**（`backends`/`frontends` 等）可随机/轮询任意 FE；对**单 FE 表**（`partitions` 等，需读 master FE 元数据）必须连 master FE（FE 侧 `FrontendServiceImpl.fetchSchemaTableData` 已有 `Env.getCurrentEnv().isMaster()` 校验，非 master 会拒绝）
   - **Phase 1 简化（编译验证 V6）**：当前实现 `_fetch_next_batch()` 直接使用 `ExecEnv::GetInstance()->cluster_info()->master_fe_addr` 作为 RPC 目标，注释明确"Phase 1 simplification: always target the master FE, multi-FE routing reserved for follow-up"。上述多 FE 聚合表的随机/轮询路由在 Phase 1 未实现，留待后续
   - **FE 不可用场景**：如果目标 FE 不可用，`fe_addr_list` 中的其他 FE 可作为备选（仅适用于多 FE 聚合表；单 FE 表 master 不可用时查询失败）
6. **并发元数据变更（一致性语义）**：查询执行期间可能发生 DDL 操作（建表/删表/加减分区）。本方案采用**弱一致性**语义——分批 RPC 期间允许元数据变化，不保证快照读。理由：
   - 现有 `SchemaScanNode` 路径同样是弱一致性（`MetadataGenerator` 无锁遍历元数据树，`partitionsForInternalCatalog()` 中 `readLock()` 仅锁定单个表，不阻止 DDL）
   - information_schema 系统表本身是**诊断视图**，非事务性查询，用户不期望 ACID 语义
   - 强一致性（快照读）需要冻结元数据变更，对 DDL 操作影响过大，收益不匹配成本
   - **已知限制**：极端情况下分批 RPC 期间 DDL 可能导致行重复或遗漏（如某分区在第一批出现、第二批消失），这是可接受的行为，与现有路径一致
7. **超大结果集处理**：百万级分区的集群，FE 端 `MetadataGenerator` 遍历全量元数据可能产生内存压力。应对策略：
   - **BE 端**：`InfoSchemaTableReader` 的分批 RPC + `AdaptiveBlockSizePredictor` 动态调整 batch_size，BE 端内存始终可控（O(batch_size)）
   - **FE 端**：配合 2.2 节优化1（查询级缓存），首次 RPC 生成全量数据并缓存，后续 RPC 从缓存切片，避免重复遍历。但首次生成仍需 O(全量行数) 内存
   - **极端场景兜底**：如果 FE 端内存不足，`MetadataGenerator` 应捕获 OOM 并返回错误状态，BE 端将错误传播给用户。建议在 FE 端设置单次生成行数上限（如 1000 万行），超限时返回错误提示用户缩小查询范围
8. **V1/V2 Scanner 路由决策（编译验证 V5）**：当前实现中 `file_scan_operator.cpp::_should_use_file_scanner_v2()` 对 `FORMAT_INFO_SCHEMA_TABLE` 显式返回 false，即 info_schema 表走 **V1 旧 FileScanner** 而非 V2（`FileScannerV2`/`FileScanOperatorV2`）。原因：V2 的 reader 初始化链路与 V1 不同（V2 通过 `FileScannerV2::_init_reader()` 初始化，参数传递方式有差异），Phase 1 先在 V1 验证全链路正确性，V2 适配留待后续。这意味着本方案当前复用的是 V1 `FileScanner` 的调度/并行/内存管理能力，而非 V2。

### 1.7 兼容性与扩展性

**三层职责分离**：
- **Plan Node 层**（`TFileScanNode`，复用现有）：仅 `tuple_id` + `table_name`，极简，不混入数据源描述
- **Scan Range 层**（`TFileScanRange` + `TInfoSchemaFileDesc`，新增）：数据源描述下沉到此层——数据源标识（`metadata_type`/`output_mode`）+ 数据源抽象（`TInfoSchemaSourceInfo`）+ 系列专用过滤子结构（`TInfoSchemaCatalogDbParams`/`TInfoSchemaShowHiddenParams`），按需携带
- **RPC 层**（`TFetchSchemaTableDataRequest`，扩展现有）：三层信息汇聚点，嵌套 `TInfoSchemaFileDesc`（数据源描述唯一来源）+ 运行时上下文 + 分批控制，通过 `info_schema_params` 字段与现有 TVF/Schema 表路径平级，复用现有 `fetchSchemaTableData` RPC

**未来扩展性**：当前设计将数据源描述命名为 `TInfoSchemaFileDesc`，绑定到 `information_schema`。Doris 还有其他系统表体系（如 `mysql` 库下的系统表、未来可能的 `sys` 库）。如果未来这些系统表也需要走 table reader 路径，有两种扩展方式：
- **方式一（推荐）**：保持 `TInfoSchemaFileDesc` 不变，新增 `TSystemTableFileDesc` 等平级结构，通过 `TTableFormatFileDesc` 的不同字段区分。这与 Iceberg/Paimon 各有独立 `TIcebergFileDesc`/`TPaimonFileDesc` 的模式一致
- **方式二**：将 `TInfoSchemaFileDesc` 重命名为 `TSystemTableFileDesc`，通过 `metadata_type` 枚举区分具体表类型。优点是统一，缺点是 `metadata_type` 枚举需扩展到覆盖所有系统表类型，语义边界模糊
- **当前选择方式一的理由**：`information_schema` 是 MySQL 标准的系统表库，有明确的 schema 和语义边界；其他系统表体系（如 `mysql.user`）的参数结构可能完全不同。保持独立结构更清晰，扩展时遵循现有数据湖的先例

**覆盖边界（统一化的渐进范围）**：
- **Phase 1 仅统一 FE 元数据系列**（约 30 张表：`tables`/`partitions`/`columns`/`backends`/`frontends` 等，数据来自 FE 元数据）。这是本方案全部设计的前提
- **BE 本地系列保持现状**：11 张 BE 本地表已由 `BackendPartitionedSchemaScanNode` 多 BE 并行承载，是另一条并行路径。本方案**不合并**该路径——其数据源是 BE 而非 FE。未来如需统一，可在 `TInfoSchemaSourceInfo.source_type` 新增 `BE_LOCAL`，复用同一数据源抽象
- **legacy planner 兼容回退**：Nereids 下系统表走本方案新路径；legacy planner 下系统表仍走 `SchemaScanNode`，通过 session variable 控制。新路径验证通过前，legacy planner 不切换
- **权限语义一致性**：新路径的权限校验随 `*MetadataResult` 委托复用，与旧路径一致（各表权限模型不同：`role_mappings` 等需 ADMIN/SELECT 权限，普通表需对应库表权限），不重复实现权限抽象

**向后兼容性**：本路径与 `SchemaScanNode` 路径通过 session variable 切换，需注意以下兼容性问题：
- **查询计划缓存**：Doris 有查询计划缓存机制（`PartitionCache`/`SqlCache`）。同一个 SQL 在切换 session variable 前后会生成不同的 plan node type（`SCHEMA_SCAN_NODE` vs `FILE_SCAN_NODE`）。**应对措施**：session variable 的变更应使查询计划缓存失效（Doris 现有机制中，session variable 变更已会触发缓存失效，`ConnectContext.getSessionVariable()` 的变更会被 `CacheAnalyzer` 检测到）
- **滚动升级顺序**：升级期间新 FE 可能发送包含 `FORMAT_INFO_SCHEMA_TABLE` 的 plan 给旧 BE，旧 BE 无法识别此枚举值。**应对措施**：必须**先升级 BE，再升级 FE**。BE 升级后能识别新枚举值但不会主动使用（由 FE 的 session variable 控制路由）；FE 升级后通过 session variable 默认关闭本路径，验证后再逐步开启。这与 Doris 现有的滚动升级规范一致（新枚举值向后兼容，旧 BE 忽略未知字段，但 switch-case default 分支会报错，因此必须先升级 BE）
- **回退能力**：如果本路径出现问题，通过 session variable 切回 `SchemaScanNode` 路径即可，无需回滚代码。`SchemaScanNode` 路径在 Phase 2 仅标记 `@deprecated`，不删除代码

**实施路径**：

| 步骤 | 内容 | 层次 | 预估工作量 |
|------|------|------|-----------|
| 1 | 新增 `TFileFormatType::FORMAT_INFO_SCHEMA_TABLE`、`TInfoSchemaFileDesc`、`TMetadataOutputMode` | Thrift Scan Range 层 | 1 天 |
| 2 | 扩展现有 `TFetchSchemaTableDataRequest`（新增 `info_schema_params`/`offset`/`batch_size`/`thread_id`/`time_zone`/`columns_name`）、`TFetchSchemaTableDataResult`（新增 `has_more`），复用现有 `fetchSchemaTableData` RPC | Thrift RPC 层 | 1 天 |
| 3 | 改造现有 `ExternalInfoSchemaTable extends ExternalTable`（新增数据源描述承载 + 列定义路由） | Java 表层 | 3 天 |
| 4 | 新增 `InfoSchemaScanNode extends FileQueryScanNode`（override `getFileFormatType`/`setScanParams`/`getSplits`/`getNumInstances`=1） | Java Plan Node 层 | 5 天 |
| 5 | 新增 `InfoSchemaTableReader extends GenericReader`（含分批 RPC，自动获得 `get_columns()`） | BE Reader 层 | 5 天 |
| 6 | `FileScanner._get_next_reader()` 新增 `FORMAT_INFO_SCHEMA_TABLE` 分支 | BE Scanner 层 | 1 天 |
| 7 | `PhysicalPlanTranslator.visitPhysicalFileScan()` 新增 `ExternalInfoSchemaTable` 分支（if-else instanceof 链） | Java 路由层 | 1 天 |
| 8 | `MetadataGenerator` 新增 `getInfoSchemaMetadata()`（委托现有 `*MetadataResult` + 按 `output_mode` 决定返回列序 + 分批切片） | Java RPC 层 | 3 天 |
| 9 | 集成测试 + 回归测试 | 测试 | 5 天 |
| **合计** | | | **约 3-4 周** |

**迁移路径**（仅针对 information_schema 系统表，不涉及外部湖系统表）：

| 阶段 | 范围 | 说明 |
|------|------|------|
| Phase 1：新路径上线 | `information_schema` 系统表（FE 元数据系列） | 改造现有 `ExternalInfoSchemaTable` + 新增 `InfoSchemaTableReader`，通过 session variable 控制新旧路径切换，新路径验证通过后设为默认。**滚动升级须先 BE 后 FE**（旧 BE 无法识别 `FORMAT_INFO_SCHEMA_TABLE` 枚举值），FE 升级后 session variable 默认关闭新路径 |
| Phase 2：旧路径标记 deprecated | `SchemaScanNode` + 41 个 `SchemaXxxScanner` | 不删除旧代码，仅在代码和文档中标记 `@deprecated`，确保回退能力 |
| Phase 3：旧路径下线 | `SchemaScanNode` + `SchemaXxxScanner` | 新路径稳定运行一段时间后，移除旧路径代码 |

---

## 2. FE 端数据获取优化方案

### 2.1 现状分析

当前 information_schema 的数据获取存在"FE 每次请求都重新获取全量数据"的问题：

#### 系统表路径（SchemaPartitionsScanner）

```
SchemaPartitionsScanner.start()                    // schema_partitions_scanner.cpp:89-114
  → get_db_names()                                 // 获取 DB 列表
SchemaPartitionsScanner.get_next_block_internal()   // schema_partitions_scanner.cpp:188-221
  → get_onedb_info_from_fe()                        // schema_partitions_scanner.cpp:116-142，每个 DB 一次 RPC
  → fill_db_partitions()                            // schema_partitions_scanner.cpp:144-175，全量持有该 DB 的结果
  → 按 _block_rows_limit 切片交付                    // schema_partitions_scanner.cpp:211
```

**源码依据**：

| 组件 | 文件 | 行号 | 关键逻辑 |
|------|------|------|--------|
| `start()` | `be/src/information_schema/schema_partitions_scanner.cpp` | 89-114 | 获取 DB 列表，设置 `_block_rows_limit = state->batch_size()`（行 111） |
| `get_onedb_info_from_fe()` | 同上 | 116-142 | 每个 DB 一次 RPC，获取该 DB 全量分区数据 |
| `fill_db_partitions()` | 同上 | 144-175 | 全量持有该 DB 的结果，预分配 `_block_rows_limit` 行空间（行 159） |
| `get_next_block_internal()` | 同上 | 188-221 | 按 `_block_rows_limit` 切片交付（行 211） |
| `partitionsForInternalCatalog()` | `fe/.../MetadataGenerator.java` | 1813-1927 | 每次调用全量遍历所有表和分区，无缓存 |
| `partitionsMetadataResult()` | `fe/.../MetadataGenerator.java` | 1965-2011 | 每次调用重新获取 catalog/database/tables |

- 按 DB 分批 RPC 是应用层分批，每次 RPC 仍获取一个 DB 的**全量**分区数据
- FE 端 `partitionsMetadataResult()` 每次被调用都重新遍历所有表、所有分区（`MetadataGenerator.java:1813-1927`）
- **没有缓存**：同一个查询中，如果 BE 因 `_block_rows_limit` 切片需要多次调用 `get_onedb_info_from_fe()`，每次都会触发 FE 重新生成全量数据

#### FE 端数据生成

```java
// MetadataGenerator.java:1813-1927 — partitionsForInternalCatalog()
for (TableIf table : tables) {           // 遍历所有表
    olapTable.readLock();
    Collection<Partition> allPartitions = olapTable.getAllPartitions();  // 遍历所有分区
    for (Partition partition : allPartitions) {
        // 逐行构建 TRow，无缓存
    }
}
```

**核心瓶颈**：FE 端的 `MetadataGenerator` 方法是**无状态、无缓存**的纯函数。每次 RPC 调用都重新从元数据内存结构中遍历、构建 `TRow` 列表。对于大集群（万级表、百万级分区），这会导致：

1. **FE CPU 压力**：每次请求都重新遍历元数据树 + 构建 TRow
2. **FE 内存压力**：全量数据序列化为 `List<TRow>` 后一次性返回
3. **网络传输压力**：单次 RPC 传输大量数据
4. **重复计算**：同一查询中多次 RPC（如按 DB 分批），每次都重新计算

### 2.2 三层优化方案

以下三层优化可独立实施，不影响架构设计：

> **注意**：以下优化1和优化2为设计提案，优化3为 1.3 节 InfoSchemaTableReader 设计提案自带的能力。

#### 优化1：FE 端查询级缓存（Query-Scoped Cache）【设计提案】

**思路**：在 FE 端为每个查询的元数据请求建立短期缓存，避免同一查询内重复计算。

```java
// 新增：QueryScopedMetadataCache
public class QueryScopedMetadataCache {
    // key: query_id + metadata_type + params_hash
    // value: List<TRow> + 生成时间戳
    private final Cache<String, CachedMetadata> cache;

    // 缓存配置：
    // - 最大容量：100 个条目（防止内存溢出）
    // - 过期时间：5 分钟（查询通常在几分钟内完成）
    // - 基于 Caffeine 实现
}

// MetadataGenerator 修改：
// 原则：权限校验、谓词下推（frontend_conjuncts 解析）、列过滤（filterColumns）
//       均已内建于现有各 *MetadataResult 方法（如 roleMappingsMetadataResult 在
//       MetadataGenerator.java:780 调用 AccessManager.checkGlobalPriv 与 filterColumns）。
//       getInfoSchemaMetadata() 只做"委托 + 切片"，不重复实现。
public static TFetchSchemaTableDataResult getInfoSchemaMetadata(
        TFetchSchemaTableDataRequest request) {
    TInfoSchemaFileDesc desc = request.getInfo_schema_params();
    String cacheKey = buildCacheKey(request);
    CachedMetadata cached = queryCache.getIfPresent(cacheKey);
    if (cached != null) {
        // 缓存命中：委托现有 *MetadataResult 生成的数据已在缓存中，直接切片返回
        return sliceResult(cached.getData(), request.getOffset(), request.getBatch_size());
    }
    // 缓存未命中：
    // 1. 委托现有 *MetadataResult（按 metadata_type 分派，已内建权限/谓词/列过滤）
    //    —— 返回的 TRow 列序由 TInfoSchemaFileDesc.output_mode 决定（见 1.4.2 Schema 抽象），
    //       与 BE 端 plan 阶段按同一 output_mode 建的 slot 严格一致；
    //       数据阶段不再做列集切换（避免列错位）
    List<TRow> data = dispatchToExistingMetadataResult(desc, request);
    queryCache.put(cacheKey, new CachedMetadata(data));
    // 2. offset/batch_size 切片（分批）
    return sliceResult(data, request.getOffset(), request.getBatch_size());
}
```

> **两个实现细节**：
> - **缓存 key 的 `query_id` 来源**：`TFetchSchemaTableDataRequest` 现有字段无 `query_id`。方案在 RPC 层为该 request **新增 `query_id` 字段**（FE plan 阶段生成，随 `TFileScanNode` 下发到 BE，BE 组装 RPC 时填入），缓存 key = `query_id` + `metadata_type` + `params_hash`。若不想扩 RPC，可退化为"按 `metadata_type` + `params_hash` 缓存 + 5 分钟 TTL"，代价是跨查询偶发复用缓存。
> - **委托现有 `*MetadataResult` 的参数转换**：现有各 `*MetadataResult` 方法签名接收 `TSchemaTableRequestParams`（或 `TMetadataTableRequestParams`）。新路径 request 只有 `info_schema_params`，委托时需做一次**适配转换**：把 `TInfoSchemaFileDesc` 的 `source_info.current_user_ident`/`frontend_conjuncts` 与 `catalog_db_params` 等字段填充到一个临时 `TSchemaTableRequestParams`，再调用现有方法（权限/谓词/列过滤逻辑零改动）；或为 `getInfoSchemaMetadata()` 新增按 `TInfoSchemaFileDesc` 分派的内部重载。推荐前者（适配器），对现有方法侵入最小。

**效果**：
- 同一查询内，首次 RPC 委托现有 `*MetadataResult` 生成数据并缓存，后续 RPC（offset/batch_size 分批）直接从缓存切片
- 消除了"每次请求都重新获取全量数据"的问题
- 缓存生命周期与单次查询的分批 RPC 序列绑定，避免同一查询内重复计算
- 权限/谓词/列过滤语义与旧路径天然一致（委托复用现有 `*MetadataResult`，不重复实现）
- 元数据一致性语义见 1.7 节第 6 点：采用弱一致性，分批 RPC 期间允许 DDL 变更，缓存不保证快照读

**风险**：
- 缓存容量控制：需限制最大条目数和单条目大小
- 缓存 key 设计：需对 `TInfoSchemaFileDesc` 整体计算 hash（包含 `metadata_type`/`catalog`/`db_id`/`frontend_conjuncts` 等所有影响结果的字段），加上 `thread_id`/`time_zone` 等运行时上下文

#### 优化2：FE 端流式生成（Streaming Generation）【设计提案】

**思路**：将 `MetadataGenerator` 的全量生成改为流式生成，支持 offset + limit 参数，避免一次性构建全量 `List<TRow>`。

```java
// 优化前：全量生成
List<TRow> data = new ArrayList<>();
for (TableIf table : tables) {
    for (Partition partition : allPartitions) {
        data.add(buildRow(partition));  // 全量构建
    }
}
return sliceResult(data, offset, batchSize);

// 优化后：流式生成
public static TFetchSchemaTableDataResult getInfoSchemaMetadataStreaming(
        TFetchSchemaTableDataRequest params) {
    long offset = params.getOffset();
    int limit = params.getBatch_size();
    List<TRow> result = new ArrayList<>(limit);
    long skipped = 0;

    for (TableIf table : tables) {
        for (Partition partition : allPartitions) {
            if (skipped < offset) {
                skipped++;           // 跳过前 offset 行
                continue;
            }
            result.add(buildRow(partition));
            if (result.size() >= limit) break;
        }
        if (result.size() >= limit) break;
    }

    boolean hasMore = /* 还有未遍历的表/分区 */;
    TFetchSchemaTableDataResult ret = new TFetchSchemaTableDataResult();
    ret.setData_batch(result);
    ret.setHas_more(hasMore);
    return ret;
}
```

**效果**：
- FE 内存占用从 O(全量行数) 降为 O(batch_size)
- 每次只构建需要的行，避免无用的 TRow 构建开销

**限制**：
- 需要维护遍历游标（当前遍历到哪个表、哪个分区），增加实现复杂度
- 谓词下推（frontend_conjuncts）后行数不确定，无法精确预判 has_more
- 多次调用间元数据一致性：采用弱一致性语义（见 1.7 节第 6 点），分批 RPC 期间允许 DDL 变更，流式游标可能因元数据变化而遗漏或重复行

**推荐**：先实施优化1（查询级缓存），再视性能需求决定是否实施优化2。优化1 实现简单、风险低，已能解决大部分场景的问题。优化2 适用于极端大结果集场景（百万级分区），但实现复杂度较高。

#### 优化3：BE 端 Reader 级缓存（Reader-Scoped Cache）【InfoSchemaTableReader 自带能力】

**思路**：在 `InfoSchemaTableReader` 内部缓存已获取的数据，避免重复 RPC。

```cpp
// InfoSchemaTableReader 内部已有分批状态
Status InfoSchemaTableReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_batch_offset >= _batch_data.size()) {
        if (!_has_more) {
            *eof = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(_fetch_next_batch());  // 仅在数据耗尽时才发 RPC
    }
    // 从 _batch_data 填充 block
    // ...
}
```

**效果**：
- `InfoSchemaTableReader` 的 `_batch_data` 本身就是 Reader 级缓存
- 每次 RPC 获取 batch_size 行，消费完再请求下一批
- 与优化1 配合：首次 RPC 触发 FE 缓存填充，后续 RPC 命中 FE 缓存

### 2.3 三层优化的协同关系

```
BE: InfoSchemaTableReader
  │  get_next_block() → 从 _batch_data 取数据
  │  _batch_data 耗尽 → _fetch_next_batch() RPC
  ↓
FE: MetadataGenerator + QueryScopedMetadataCache
  │  首次请求 → 生成全量数据，写入缓存，返回切片
  │  后续请求 → 缓存命中，直接切片返回（无需重新遍历元数据）
  ↓
FE: 流式生成（可选优化2）
  │  仅生成 offset ~ offset+limit 范围的行
  │  进一步降低 FE 内存和 CPU 开销
```

### 2.4 优化优先级

| 优先级 | 优化项 | 效果 | 实现复杂度 | 风险 |
|--------|-------|------|-----------|------|
| **P0** | 查询级缓存（优化1） | 消除同一查询内重复计算 | 低 | 低 |
| **P1** | Reader 级分批（优化3） | BE 端内存可控 | 低（Table Reader 方案自带） | 低 |
| **P2** | 流式生成（优化2） | FE 内存从 O(N) 降为 O(batch_size) | 中 | 中 |

**建议**：先实施 P0 + P1，覆盖绝大多数场景。P2 仅在确认存在极端大结果集性能问题后再实施。

> **优化关系**：优化1（查询级缓存 + 切片）与优化2（流式生成）是**替代方案**——优化1 在 FE 端缓存全量再切片，优化2 只生成需要的行，两者**不叠加**（叠加会让流式游标与缓存切片语义冲突）。优化3（Reader 级分批）是 Table Reader 方案自带能力，与优化1/2 均兼容。

---

## 总结

| 方面 | 要点 |
|------|------|
| **Table Reader 架构** | information_schema 系统表作为 table format，通过**改造现有 `ExternalInfoSchemaTable`** + `InfoSchemaScanNode` + `InfoSchemaTableReader`（继承 `GenericReader`，与 `WalReader` 同模式，不继承 `JniReader`），复用 `FileScanner` + `GenericReader` 体系。扩展点单一（`FileScanner._get_next_reader()` switch-case）。**数据源抽象**（`TInfoSchemaSourceInfo`，FE 是数据源的一种）+ **系列抽层**（catalog/db 等按类型抽子结构）+ **Schema 列集抽象**（列集 = f(metadata_type, output_mode)，plan 阶段定死）。**三层职责分离**：Plan Node 层（`TFileScanNode`，复用，极简）→ Scan Range 层（`TFileScanRange` + `TInfoSchemaFileDesc`，新增，数据源描述唯一来源）→ RPC 层（`TFetchSchemaTableDataRequest`，扩展现有，新增第 3 分支 `getInfoSchemaMetadata`）。**数据正确性**：`InfoSchemaScanNode` override `getNumInstances()`=1（单实例，避免多 BE 重复拉取 FE 元数据）。**边界场景已覆盖**：空结果集、RPC 重试、并发 DDL 弱一致性、超大结果集兜底。**兼容性已分析**：查询计划缓存失效、滚动升级须先 BE 后 FE、session variable 回退 |
| **FE 端数据获取优化** | 三层优化：查询级缓存（P0）+ Reader 级分批（P1）+ 流式生成（P2）。优化1 在 FE 端 MetadataGenerator 层实施，优化3 在 Reader 层实施，均不改变 FE-BE 交互模型 |

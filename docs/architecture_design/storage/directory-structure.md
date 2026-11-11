# OceanBase AI 共享存储目录结构

> 代码基线：8a69c9305600591ddb8f25c4e68821ef13d44908，源码复核日期：2026-11-04。本文关注 OB_BUILD_SHARED_STORAGE 下的实现；目录树表示源码可生成的路径，不表示每种对象在运行环境中都已存在。

本文用于从对象类型、MacroBlockId 和运行上下文定位对象存储文件、本地缓存或内表记录。阅读时先区分三件事：

1. **存放位置**：对象存储中的 server 私有区、tenant 共享区，以及节点本地缓存，各自有不同的根目录和命名空间。
2. **元数据后端**：.T 后缀只表示 ObStorageObjectType；同一类型的 OBJECT_STORAGE、TABLE、CONTROL 形态还要结合 MacroBlockId.location_ 和访问入口判断，见第 1.4 节。
3. **路径字段含义**：相同的 op 字面量可承载 major snapshot、DDL 宏块代际标识或 compaction op_id，不能只凭文件名统一解释，见第 6 节。

当前有效对象类型为 0–41，MAX=42。SHARED_TABLET_SUB_META 的对象存储路径和表内逻辑键将 seq 补齐到至少 10 位；本地缓存文件名不补零。

## 0. 代码总览

| 模块 | 路径 | 主要职责 |
|------|------|----------|
| 目录/文件常量 | src/storage/shared_storage/ob_file_helper.h | 定义 CLUSTER_DIR_STR="cluster" 等字面量及键名 |
| 对象类型枚举 + 虚基类 | src/storage/blocksstable/ob_storage_object_type.h | ObStorageObjectType (uint8)、ObStorageObjectTypeBase 抽象接口 |
| 路径格式化主实现 | src/storage/blocksstable/ob_storage_object_type.cpp | 各类型按需实现本地、远程、逻辑相对路径及反解析接口 |
| 共享路径 helper | ObStorageObjectTypeBase::stract_remote_path_format() | 为调用它的类型拼接对象存储租户前缀；适用范围见第 2.2 节 |
| MacroBlockId ↔ 路径工具 | src/storage/blocksstable/ob_ss_obj_util.cpp | 对 ObStorageObjectTypeBase 的统一派发 |
| 目录/文件上下文 | src/storage/shared_storage/ob_file_helper.cpp | 按本地/远程上下文构造路径；本地原子写和 TMP_FILE 删除后缀见第 5 节 |
| 目录/磁盘配额管理 | src/storage/shared_storage/ob_dir_manager.cpp | create_tenant_dir / create_ls_id_dir / create_scatter_dir |
| 普通共享元数据 | src/storage/incremental/ssmeta/ob_ss_meta_table_helper.cpp | 使用逻辑相对键访问 __all_shared_meta |
| 分层子元数据 | src/storage/shared_storage/ob_table_device.cpp、src/storage/tiered_metadata_store/ob_tiered_metadata_store.cpp | table:// 设备访问及 __all_shared_tiered_meta 读写 |
| 控制行 | src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.cpp、ob_ss_meta_ctrl_table_operator.cpp | 构造 CONTROL 键并访问 __all_shared_meta_control |

---

## 1. 字面量速查表（ob_file_helper.h）

### 1.1 目录字面量

| 常量 | 值 | 作用 |
|------|---|------|
| CLUSTER_DIR_STR | "cluster" | cluster 层 |
| SERVER_DIR_STR | "server" | server 层（节点私有） |
| TENANT_DIR_STR | "tenant" | tenant 层（租户共享） |
| LS_DIR_STR | "ls" | LS/LogStream 层 |
| TABLET_DIR_STR | "tablet" | 用户 tablet 层 |
| TABLET_IDS_DIR_STR | "tablet_ids" | tablet ID 集合 |
| TABLET_DATA_DIR_STR | "tablet_data" | 私有 macro block（per-server） |
| TABLET_META_DIR_STR | "tablet_meta" | 私有 tablet 元数据 |
| META_DIR_STR | "meta" | 元数据子目录名 |
| SHARED_TABLET_SSTABLE_DIR_STR | "sstable" | 共享 SSTable 子目录名 |
| TMP_DATA_DIR_STR | "tmp_data" | TMP_FILE 数据 |
| CKPT_STR | "ckpt" | 租户/服务端 checkpoint |
| SLOG_STR | "slog" | Storage Log |
| MINI_DIR_STR / MINOR_DIR_STR / MAJOR_DIR_STR / INC_MAJOR_DIR_STR | "mini"/"minor"/"major"/"inc_major" | compaction 级别 |
| DATA_MACRO_DIR_STR / META_DIR_STR | "data"/"meta" | 宏块子类型（数据/元数据）|
| SHARED_MINI_MACRO_CACHE_DIR_STR | "shared_mini_macro_cache" | 本地 cache：共享 mini 宏块 |
| SHARED_MINOR_MACRO_CACHE_DIR_STR | "shared_minor_macro_cache" | 本地 cache：共享 minor 宏块 |
| SHARED_MAJOR_MACRO_CACHE_DIR_STR | "shared_major_macro_cache" | 本地 cache：共享 major 宏块 |
| INC_MAJOR_DATA_DIR_STR | "shared_inc_major_macro_cache" | 本地 cache：共享增量 major |
| SHARED_TABLET_SUB_META_CACHE_DIR_STR | "shared_tablet_sub_meta_cache" | 本地 cache：tablet 子元数据 |
| EXTERNAL_TABLE_FILE_DIR_STR | "external_table_file" | 外表文件 |
| MACRO_CACHE_CKPT_DIR_STR | "macro_cache_ckpt" | 远程 server 私有区的 macro cache checkpoint |
| CKM_ERROR_DIR_STR | "checksum_error_macro" | 当前仅查到常量定义，未查到创建或使用该目录的实现 |
| OP_ID_DIR_STR | "op" | op 目录（仅作目录名片段） |
| PREWARM_INFO_DIR_STR | "prewarm_info" | Major prewarm 信息 |
| COMPACTION/COMPACTOR/SCHEDULER_DIR_STR | "compaction"/"compactor"/"scheduler" | 合并调度器相关 |
| LOCK_DIR_STR | "lock" | SS meta control row 的 lock 路径片段 |
| MARK_SWEEP_GC_DIAG_DIR_STR / ROUND_DIR_STR / BKT_KEY_STR | "mark_sweep_gc_diag" / "round" / "bkt" | Mark-Sweep GC 诊断目录 |
| TX_CTX/TX_DATA/TX_LOCK/REORG_INFO/MDS_TABLET_DIR_STR | "TX_CTX"/"TX_DATA"/"TX_LOCK"/"REORG_INFO"/"MDS_DATA" | 5 个 LS 内部 tablet 的固定目录名 |

### 1.2 文件名 key / 后缀

| 常量 | 值 | 含义 |
|------|---|------|
| OBJECT_TYPE_STR | .T | 文件后缀，尾部 &lt;N&gt; 为 ObStorageObjectType 枚举值 |
| DEFAULT_TMP_STR | .tmp. | 本地原子写中间态（带 write_seq） |
| DEFAULT_DELETED_STR | .deleted | 本地 TMP_FILE 逻辑删除标记 |
| SVR_KEY_STR | svr | server_id |
| SEQ_KEY_STR | seq | 单调 seq（macro_seq / tenant_seq） |
| OP_KEY_STR | op | 依对象类型表示操作编号、major snapshot 或 DDL 宏块代际标识 |
| TABLET_KEY_STR | t | tablet_id 在扁平文件名中的前缀 |
| REORG_KEY_STR | rs | reorganization_scn |
| CG_KEY_STR | cg | column_group id |
| SEG_KEY_STR | seg | tmp file segment id |
| VER_KEY_STR | ver | 版本号（meta_version_id 等） |
| SCN_KEY_STR | scn | SCN / compaction_scn |
| LEN_KEY_STR | len | UNSEALED_REMOTE_SEG_FILE 的 valid_length |
| IDX_KEY_STR | idx | EXTERNAL_TABLE_FILE 的 offset/2MB 索引 |
| TFR_KEY_STR | tsfr | （备用） |
| BKT_KEY_STR | bkt | Mark-Sweep GC 诊断桶号 |
| IO_DETECT_FILE_NAME | io_detect_file | IO 检测文件 |
| OBJ_TIERED_METADATA_SEQ_WIDTH | 10 | SHARED_TABLET_SUB_META 的远程/逻辑路径用 %0*ld、宽度 10 格式化 seq；不足 10 位补零，更长则保留；本地路径不补零 |

### 1.3 对象类型枚举完整表（ob_storage_object_type.h）

```
PRIVATE_DATA_MACRO=0           PRIVATE_META_MACRO=1
SHARED_MINI_DATA_MACRO=2       SHARED_MINI_META_MACRO=3
SHARED_MINOR_DATA_MACRO=4      SHARED_MINOR_META_MACRO=5
SHARED_MAJOR_DATA_MACRO=6      SHARED_MAJOR_META_MACRO=7
TMP_FILE=8                     SERVER_META=9
PRIVATE_TABLET_META=10         PRIVATE_SLOG_FILE=11
PRIVATE_CKPT_FILE=12
MAJOR_PREWARM_DATA=13          MAJOR_PREWARM_DATA_INDEX=14
MAJOR_PREWARM_META=15          MAJOR_PREWARM_META_INDEX=16
TENANT_DISK_SPACE_META=17      IS_SHARED_TENANT_DELETED=18
UNSEALED_REMOTE_SEG_FILE=19    SHARED_TABLET_META=20
SHARED_TABLET_SUB_META=21      TENANT_ROOT_KEY=22
EXTERNAL_TABLE_FILE=23         MACRO_CACHE_CKPT_DATA=24
MACRO_CACHE_CKPT_META=25       SHARED_INC_MAJOR_DATA_MACRO=26
SHARED_INC_MAJOR_META_MACRO=27
SHARED_INC_MAJOR_COMPACTION_DATA_MACRO=28
SHARED_INC_MAJOR_COMPACTION_META_MACRO=29
SHARED_LS_CKPT=30              SHARED_LS_META=31
SHARED_MARK_SWEEP_GC_DIAG=32
SHARED_MINOR_SSTABLE_CTRL=33   SHARED_MAJOR_SSTABLE_CTRL=34
SHARED_INC_MAJOR_SSTABLE_CTRL=35
SHARED_LS_LOCK=36              SHARED_TABLET_LOCK=37
SHARED_LS_CKPT_LOCK=38         SHARED_MINOR_LOCK=39
SHARED_MAJOR_LOCK=40           SHARED_INC_MAJOR_LOCK=41
MAX=42
```

### 1.4 元数据的 location、逻辑键与访问入口

MacroBlockId.location_ 占 first_id_ 的 bits 24–25，枚举为 OBJECT_STORAGE=0、TABLE=1、CONTROL=2。它区分 SS 元数据的后端形态；本地缓存路径由 ObPathContext 的 is_local_cache 上下文选择。

| location | 对象类型 | 存放位置与路径 | 已核对的访问入口 |
|----------|----------|----------------|------------------|
| OBJECT_STORAGE=0 | .T20/.T21/.T30/.T31 | 对象存储；完整路径带 &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/ 前缀 | 对应类型的 to_remote_path_format |
| TABLE=1 | .T20/.T30/.T31 | __all_shared_meta；meta_key 为不带租户目录前缀的逻辑相对路径 | ObSSMetaTableHelper / ObSSMetaTableOperator 直接操作内表 |
| TABLE=1 | .T21 | __all_shared_tiered_meta；设备路径为 table:// 加逻辑相对路径，表内 meta_key 不带 table:// | ObPathContext → ObTableDevice → ObTieredMetadataObjectManager → ObTieredMetadataStore |
| CONTROL=2 | .T20/.T30/.T31，以及 .T33–.T41 | __all_shared_meta_control；meta_key 为逻辑相对路径 | ObSSMetaCtrlKey → ObSSMetaCtrlTableOperator |

TABLE 或 CONTROL 记录可以复用对象存储的相对路径形状和 .T 后缀，但后端、记录内容与访问接口不同。不能仅凭 .T20/.T30/.T31 判断它是普通元数据还是控制行，也不能仅凭 location=TABLE 推断所有调用都经过 ObTableDevice。

控制行的数据键以 20 位 control SCN 表示版本；锁行 .T36–.T41 的 meta_scn 固定为 0，格式化为 scn00000000000000000000。该值不是一次操作的实际 SCN。

源码入口：[location 定义](../../../src/storage/blocksstable/ob_macro_block_id.h#L40)、[普通元数据读取](../../../src/storage/incremental/ssmeta/ob_ss_meta_table_helper.cpp#L151)、[table 设备读取](../../../src/storage/shared_storage/ob_table_device.cpp#L198)、[控制键转换](../../../src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.cpp#L310)、[控制表写入](../../../src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.cpp#L42)。

---

## 2. 对象存储目录树

以下展示对象存储形态；TABLE/CONTROL 的逻辑键不属于这棵物理目录树。

&lt;private_macro_third_id&gt; 表示 MacroBlockId.third_id。当前 PRIVATE_DATA_MACRO、PRIVATE_META_MACRO、PRIVATE_TABLET_META 的 get_object_id 都调用 set_ss_third_id：内部 tablet 写 ls_id，用户 tablet 写 reorganization_scn。私有 tablet meta 的父目录还单独使用调用上下文传入的 ls_id、ls_epoch_id。见 [字段赋值](../../../src/storage/blocksstable/ob_macro_block_id.h#L152)、[私有宏块构造](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L571)、[私有 tablet meta 构造](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L2461)。

```
<object_storage_root_dir>/                      ← ObDirManager::set_object_storage_root_dir()
└── cluster_<cluster_id>/
    │
    ├── server_<server_id>/                     ← ★ 节点私有区 ★
    │   │
    │   ├── server_slog/                        ← 500 号租户(OBServer)的 SLOG
    │   │   └── seq<file_id>.T11
    │   │
    │   ├── server_ckpt/                        ← 500 号租户的 checkpoint
    │   │   └── <file_id>.T12
    │   │
    │   └── <tenant_id>_<tenant_epoch_id>/      ← 普通租户在该 server 上的私有目录
    │       ├── tablet_data/                    ← PRIVATE_{DATA,META}_MACRO
    │       │   └── <tablet_id>/<private_macro_third_id>/
    │       │       ├── data/svr<svr>op<op>seq<seq>.T0
    │       │       └── meta/svr<svr>op<op>seq<seq>.T1
    │       │
    │       ├── tmp_data/                       ← TMP_FILE / UNSEALED_REMOTE_SEG_FILE
    │       │   └── <tmp_file_id>/seg<seg>.T8
    │       │                    seg<seg>len<len>.T19
    │       │
    │       ├── slog/seq<file_id>.T11           ← 租户级 SLOG
    │       ├── ckpt/<file_id>.T12              ← 租户级 checkpoint
    │       │
    │       ├── ls/<ls_id>_<ls_epoch_id>/       ← 租户在该 server 的 LS 私有元数据
    │       │   └── tablet_meta/
    │       │       └── <tablet_id>/<private_macro_third_id>/
    │       │           └── ver<meta_version_id>.T10            ← PRIVATE_TABLET_META
    │       │
    │       └── macro_cache_ckpt/               ← MACRO_CACHE_CKPT_{DATA,META}
    │           ├── data/<version_id>/seq<seq_id>.T24
    │           └── meta/ver<version_id>.T25
    │
    └── tenant_<tenant_id>/                     ← ★ 租户共享区（所有 server 可访问）★
        │
        ├── TENANT_ROOT_KEY.T22                 ← ObTenantRootKeyType
        ├── IS_SHARED_TENANT_DELETED.T18        ← ObIsSharedTenantDeletedType
        │
        ├── ls/<ls_id>/                         ← LS 级内部 tablet
        │   ├── meta/
        │   │   ├── scn<20-digit>.T30                           ← SHARED_LS_CKPT
        │   │   └── scn<20-digit>.T31                           ← SHARED_LS_META
        │   └── TX_CTX / TX_DATA / TX_LOCK / REORG_INFO / MDS_DATA/
        │       ├── mini/sstable/op_<op>/data/seq<seq>.T2        ← SHARED_MINI_DATA_MACRO
        │       ├── mini/sstable/op_<op>/meta/seq<seq>.T3        ← SHARED_MINI_META_MACRO
        │       ├── minor/sstable/op_<op>/data/seq<seq>.T4       ← SHARED_MINOR_DATA_MACRO
        │       ├── minor/sstable/op_<op>/meta/seq<seq>.T5       ← SHARED_MINOR_META_MACRO
        │       ├── meta/scn<20-digit>.T20                       ← SHARED_TABLET_META
        │       └── meta/op<op>seq<10-digit>.T21                 ← SHARED_TABLET_SUB_META
        │
        ├── tablet/<tablet_id>/<reorganization_scn>/   ← 用户 tablet（共享）
        │   │
        │   ├── mini/sstable/op_<op>/
        │   │   ├── data/seq<seq>.T2                   ← SHARED_MINI_DATA_MACRO
        │   │   └── meta/seq<seq>.T3
        │   ├── minor/sstable/op_<op>/
        │   │   ├── data/seq<seq>.T4                   ← SHARED_MINOR_DATA_MACRO
        │   │   └── meta/seq<seq>.T5
        │   ├── major/sstable/cg_<cg_id>/op_<major_snapshot>/
        │   │   ├── data/seq<seq>.T6                   ← SHARED_MAJOR_DATA_MACRO
        │   │   └── meta/seq<seq>.T7
        │   ├── inc_major/sstable/cg_<cg_id>/
        │   │   └── op_<op>/
        │   │       ├── data/seq<seq>.T26              ← SHARED_INC_MAJOR_DATA_MACRO
        │   │       ├── meta/seq<seq>.T27              ← SHARED_INC_MAJOR_META_MACRO
        │   │       ├── data/seq<seq>.T28              ← SHARED_INC_MAJOR_COMPACTION_DATA_MACRO
        │   │       └── meta/seq<seq>.T29              ← SHARED_INC_MAJOR_COMPACTION_META_MACRO
        │   ├── major/prewarm_info/
        │   │   ├── scn<compaction_scn>.T13            ← MAJOR_PREWARM_DATA
        │   │   ├── scn<compaction_scn>.T14            ← MAJOR_PREWARM_DATA_INDEX
        │   │   ├── scn<compaction_scn>.T15            ← MAJOR_PREWARM_META
        │   │   └── scn<compaction_scn>.T16            ← MAJOR_PREWARM_META_INDEX
        │   └── meta/
        │       ├── scn<20-digit>.T20                   ← SHARED_TABLET_META
        │       └── op<op>seq<10-digit>.T21             ← SHARED_TABLET_SUB_META
        │
        └── mark_sweep_gc_diag/round<round>/{del|kept}/
            └── {macros|tablet_meta_scns|study_results}/bkt<bucket>.T32
```

### 2.1 关键路径模板与对应实现

| 类型 (.T) | 路径模板 | 实现 |
|----------|----------|------|
| PRIVATE_DATA_MACRO / PRIVATE_META_MACRO (.T0/.T1) | &lt;root&gt;/cluster_&lt;cid&gt;/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;tenant_epoch&gt;/tablet_data/&lt;tablet_id&gt;/&lt;private_macro_third_id&gt;/{data\|meta}/svr&lt;svr&gt;op&lt;op&gt;seq&lt;seq&gt;.T&lt;n&gt; | ObPrivateDataMacroType / ObPrivateMetaMacroType |
| SHARED_MINI_\*MACRO (.T2/.T3) | 用户tablet: …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/mini/sstable/op_&lt;op&gt;/{data\|meta}/seq&lt;seq&gt;.T&lt;n&gt;；LS内部: …/tenant_&lt;tid&gt;/ls/&lt;ls_id&gt;/&lt;TABLET_NAME&gt;/mini/sstable/op_&lt;op&gt;/{data\|meta}/seq&lt;seq&gt;.T&lt;n&gt; | ObSharedMiniDataMacroType / ObSharedMiniMetaMacroType |
| SHARED_MINOR_\*MACRO (.T4/.T5) | 同上，mini 换成 minor | ObSharedMinorDataMacroType / ObSharedMinorMetaMacroType |
| SHARED_MAJOR_\*MACRO (.T6/.T7) | …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/major/sstable/cg_&lt;cg_id&gt;/op_&lt;major_snapshot&gt;/{data\|meta}/seq&lt;seq&gt;.T&lt;n&gt; | ObSharedMajorDataMacroType / ObSharedMajorMetaMacroType |
| TMP_FILE (.T8) | &lt;root&gt;/cluster_&lt;cid&gt;/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/tmp_data/&lt;tmp_file_id&gt;/seg&lt;seg&gt;.T8 | ObTmpFileType |
| SERVER_META (.T9) | **仅本地**（pin_local）：&lt;local_cache_root&gt;/SERVER_META.T9 | ObServerMetaType（无 remote 实现） |
| PRIVATE_TABLET_META (.T10) | &lt;root&gt;/cluster_&lt;cid&gt;/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/ls/&lt;ls_id&gt;\_&lt;ls_epoch&gt;/tablet_meta/&lt;tablet_id&gt;/&lt;private_macro_third_id&gt;/ver&lt;meta_version_id&gt;.T10 | ObPrivateTabletMetaType |
| PRIVATE_SLOG_FILE (.T11) | 普通租户：…/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/slog/seq&lt;file_id&gt;.T11；500 租户：…/server_&lt;svr&gt;/server_slog/seq&lt;file_id&gt;.T11 | ObPrivateSlogFileType |
| PRIVATE_CKPT_FILE (.T12) | 普通租户：…/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/ckpt/&lt;file_id&gt;.T12；500 租户：…/server_&lt;svr&gt;/server_ckpt/&lt;file_id&gt;.T12 | ObPrivateCkptFileType |
| MAJOR_PREWARM_{DATA,DATA_INDEX,META,META_INDEX} (.T13-16) | …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/major/prewarm_info/scn&lt;compaction_scn&gt;.T&lt;n&gt; | 对应 ObMajorPrewarm\*Type |
| TENANT_DISK_SPACE_META (.T17) | **仅本地**（pin_local）：&lt;local_cache_root&gt;/&lt;tenant_id&gt;\_&lt;tenant_epoch&gt;/TENANT_DISK_SPACE_META.T17；tenant/epoch 取自 file_id.second_id/fourth_id | ObTenantDiskSpaceMetaType |
| IS_SHARED_TENANT_DELETED (.T18) | &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/IS_SHARED_TENANT_DELETED.T18 | ObIsSharedTenantDeletedType::to_remote_path_format() |
| UNSEALED_REMOTE_SEG_FILE (.T19) | …/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/tmp_data/&lt;tmp_file_id&gt;/seg&lt;seg&gt;len&lt;len&gt;.T19 | ObUnsealedRemoteSegFileType::to_remote_path_format() |
| SHARED_TABLET_META (.T20) | **对象存储**：用户 tablet …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/meta/scn&lt;20-digit&gt;.T20；LS 内部 …/tenant_&lt;tid&gt;/ls/&lt;ls_id&gt;/&lt;TABLET_NAME&gt;/meta/scn&lt;20-digit&gt;.T20。**TABLE/CONTROL**：去掉 &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/ 前缀后的逻辑键；表和访问入口见第 1.4 节 | ObSharedTabletMetaType::to_remote_path_format() / to_relative_remote_path_format() |
| SHARED_TABLET_SUB_META (.T21) | **对象存储**（location=OBJECT_STORAGE）：用户 tablet …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/meta/op&lt;op&gt;seq&lt;10-digit&gt;.T21；LS 内部 …/tenant_&lt;tid&gt;/ls/&lt;ls_id&gt;/&lt;TABLET_NAME&gt;/meta/op&lt;op&gt;seq&lt;10-digit&gt;.T21。**in-table**（location=TABLE）：数据存 __all_shared_tiered_meta；使用去掉 &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/ 前缀的同形逻辑相对路径，由 ObPathContext 添加 table:// 前缀 | ObSharedTabletSubMetaType::to_remote_path_format() / to_relative_remote_path_format() |
| TENANT_ROOT_KEY (.T22) | &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/TENANT_ROOT_KEY.T22 | ObTenantRootKeyType::to_remote_path_format() |
| EXTERNAL_TABLE_FILE (.T23) | **仅本地** | ObExternalTableFileType::to_local_path_format() |
| MACRO_CACHE_CKPT_DATA (.T24) | …/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/macro_cache_ckpt/data/&lt;version_id&gt;/seq&lt;seq_id&gt;.T24 | ObMacroCacheCkptDataType::to_remote_path_format() |
| MACRO_CACHE_CKPT_META (.T25) | …/server_&lt;svr&gt;/&lt;tid&gt;\_&lt;epoch&gt;/macro_cache_ckpt/meta/ver&lt;version_id&gt;.T25 | ObMacroCacheCkptMetaType::to_remote_path_format() |
| SHARED_INC_MAJOR_\*MACRO (.T26/.T27) | …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/inc_major/sstable/cg_&lt;cg_id&gt;/op_&lt;ddl_block_op_id&gt;/{data\|meta}/seq&lt;seq&gt;.T&lt;n&gt;；ddl_block_op_id 的选择规则见第 6 节 | ObSharedIncMajorDataMacroType / ObSharedIncMajorMetaMacroType |
| SHARED_INC_MAJOR_COMPACTION_\*MACRO (.T28/.T29) | …/tenant_&lt;tid&gt;/tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/inc_major/sstable/cg_&lt;cg_id&gt;/op_&lt;op_id&gt;/{data\|meta}/seq&lt;seq&gt;.T&lt;n&gt; | ObSharedIncMajorCompactionDataMacroType / ObSharedIncMajorCompactionMetaMacroType |
| SHARED_LS_CKPT / SHARED_LS_META (.T30/.T31) | **对象存储**：…/tenant_&lt;tid&gt;/ls/&lt;ls_id&gt;/meta/scn&lt;20-digit&gt;.T&lt;n&gt;；**TABLE/CONTROL**：去掉 &lt;root&gt;/cluster_&lt;cid&gt;/tenant_&lt;tid&gt;/ 前缀后的逻辑键，见第 1.4 节 | ObSharedLsCkptType / ObSharedLsMetaType |
| SHARED_MARK_SWEEP_GC_DIAG (.T32) | …/tenant_&lt;tid&gt;/mark_sweep_gc_diag/round&lt;round&gt;/{del\|kept}/{macros\|tablet_meta_scns\|study_results}/bkt&lt;bucket&gt;.T32 | ObSharedMarkSweepGcDiagType::to_relative_remote_path_format() |
| SHARED_MINOR/MAJOR/INC_MAJOR_SSTABLE_CTRL (.T33-.T35) | **SS meta control row**（MacroBlockId.location=CONTROL），不是 OSS 独立文件；逻辑路径复用 tablet 或 LS 内部 tablet 的 meta 路径，scn 为 20 位 control SCN | ObSharedMinorSstableCtrlType / ObSharedMajorSstableCtrlType / ObSharedIncMajorSstableCtrlType |
| SHARED_LS_LOCK / SHARED_LS_CKPT_LOCK (.T36/.T38) | **SS meta control row**：ls/&lt;ls_id&gt;/lock/scn00000000000000000000.T36 / .T38，靠对象类型区分用途 | ObSharedLsLockType / ObSharedLsCkptLockType |
| SHARED_TABLET_LOCK (.T37) | **SS meta control row**：用户 tablet 为 tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/lock/scn00000000000000000000.T37；LS 内部 tablet 为 ls/&lt;ls_id&gt;/&lt;TABLET_NAME&gt;/lock/scn00000000000000000000.T37 | ObSharedTabletLockType |
| SHARED_MINOR/MAJOR/INC_MAJOR_LOCK (.T39-.T41) | **SS meta control row**：与 .T37 同形，仅后缀类型换成 .T39 / .T40 / .T41 | ObSharedMinorLockType / ObSharedMajorLockType / ObSharedIncMajorLockType |

Major 的远程路径和本地文件名都保留 major_snapshot：[数据宏块](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L1797)、[元数据宏块](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L1955)。SHARED_TABLET_SUB_META 的不同 seq 宽度见 [本地 formatter](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L3385) 和 [逻辑相对路径 formatter](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L3443)。

### 2.2 路径 helper 的适用范围

ObStorageObjectTypeBase::stract_remote_path_format 先调用具体类型的 to_relative_remote_path_format，再拼接租户对象存储前缀：

```
<object_storage_root>/cluster_<cid>/tenant_<tid>/<relative_path>
```

| 类型 | 路径实现 |
|------|----------|
| Mini/Minor/Major/Inc-major 宏块、Major Prewarm、TENANT_ROOT_KEY、GC 诊断对象 | 调用上述 helper |
| .T20/.T21/.T30/.T31 | 对象存储完整路径由各类型直接格式化；表内形态复用逻辑相对路径 |
| IS_SHARED_TENANT_DELETED | 直接格式化租户共享区路径，tenant_id 取自 file_id.second_id |
| 私有 data/meta 宏块、tablet meta、SLOG/CKPT、TMP_FILE、UNSEALED_REMOTE_SEG_FILE、MACRO_CACHE_CKPT_\* | 直接格式化包含 server_&lt;svr&gt; 的远程路径 |
| .T33–.T41 控制行 | 使用逻辑相对键，无对象存储完整路径实现 |

因此，目录前缀应根据具体类型和 location 判断。表内逻辑键不拼接 cluster/tenant 物理目录，租户隔离由内表访问上下文及键列承载。见 [helper](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L412)、[元数据路径分支](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L3208)。

---

## 3. 本地 cache 目录结构

ObDirManager::create_tenant_dir 创建租户根、ls、tablet_data、tmp_data、slog、四类 shared macro cache、shared_tablet_sub_meta_cache 与 external_table_file。LS 目录、具体 tablet 目录、临时文件散列目录等由对应创建接口继续建立。下图覆盖本文对象类型的本地路径，不将远程 MACRO_CACHE_CKPT_\* 列入本地树。

本地根目录来自 OB_FILE_SYSTEM_ROUTER.get_sstable_dir()。SERVER_META 和 500 租户的 server_slog 直接放在根目录下；普通租户对象放在 &lt;tenant_id&gt;\_&lt;tenant_epoch_id&gt;/ 下。

```
<local_cache_root>/
├── SERVER_META.T9                              ← pin_local，服务端元数据
├── server_slog/seq<file_id>.T11                 ← 500 租户的 SLOG
└── <tenant_id>_<tenant_epoch_id>/
    ├── TENANT_DISK_SPACE_META.T17              ← pin_local，租户磁盘空间元数据
    ├── ls/                                    ← META_FILE 磁盘配额入口
    │   └── <ls_id>_<ls_epoch_id>/
    │       └── tablet_meta/<scatter[00-99]>/        ← PRIVATE_TABLET_META
    │           └── <tablet_id>/<private_macro_third_id>/ver<ver>.T10
    │
    ├── tablet_data/<scatter[00-99]>/                ← PRIVATE_DATA/META_MACRO
    │   └── <tablet_id>/<private_macro_third_id>/
    │       ├── data/svr<svr>op<op>seq<seq>.T0
    │       └── meta/svr<svr>op<op>seq<seq>.T1
    │
    ├── tmp_data/<scatter[000-999]>/                 ← TMP_FILE
    │   └── <tmp_file_id>/seg<seg>.T8
    │
    ├── slog/seq<file_id>.T11                        ← 普通租户 SLOG
    │
    ├── shared_mini_macro_cache/                     ← SHARED_MINI_*MACRO 本地缓存
    │   ├── <scatter[00-FF]>/
    │   │   └── t<tablet_id>rs<reorg_scn>op<op>seq<seq>.T{2,3}
    │   └── ls/<ls_id>/<TABLET_NAME>_op<op>seq<seq>.T{2,3}
    │
    ├── shared_minor_macro_cache/                    ← SHARED_MINOR_*MACRO
    │   ├── <scatter[00-FF]>/…
    │   └── ls/<ls_id>/…
    │
    ├── shared_major_macro_cache/<scatter[00-FF]>/   ← SHARED_MAJOR_*MACRO
    │   └── t<tablet_id>rs<reorg_scn>cg<cg>op<major_snapshot>seq<seq>.T{6,7}
    │
    ├── shared_inc_major_macro_cache/<scatter[00-FF]>/← SHARED_INC_MAJOR_*MACRO
    │   ├── t<tablet_id>rs<reorg_scn>cg<cg>op<ddl_block_op_id>seq<seq>.T{26,27}
    │   └── t<tablet_id>rs<reorg_scn>cg<cg>op<op_id>seq<seq>.T{28,29} ← SHARED_INC_MAJOR_COMPACTION_*MACRO
    │
    ├── shared_tablet_sub_meta_cache/                ← SHARED_TABLET_SUB_META
    │   ├── <scatter[00-FF]>/t<tablet_id>rs<reorg_scn>op<op>seq<seq>.T21
    │   └── ls/<ls_id>/<TABLET_NAME>_op<op>seq<seq>.T21
    │
    └── external_table_file/<scatter[00-FF]>/        ← EXTERNAL_TABLE_FILE
        └── seq<server_seq_id>idx<offset_idx>.T23
```

本地与远程的区别：

1. 本地路径没有 cluster_&lt;cid&gt;/server_&lt;svr&gt;/ 目录层；私有宏块文件名仍包含 svr&lt;svr&gt;。
2. 用户 tablet 的共享宏块/子元数据采用扁平文件名；私有宏块、私有 tablet meta、临时文件仍保留多层目录，不能把全部本地文件都视为扁平缓存。
3. 用户 tablet 的共享缓存和外表缓存有散列桶；Mini/Minor/子元数据的内部 tablet 缓存走 ls/&lt;ls_id&gt;/ 分支，不使用散列桶。
4. SHARED_TABLET_SUB_META 的远程/逻辑路径将 seq 补齐到至少 10 位，本地文件名保留原始十进制 seq。

MACRO_CACHE_CKPT_DATA/META 为 direct-read 类型，仅实现远程路径；其目录位于第 2 节的 server 私有区。CKM_ERROR_DIR_STR 当前仅有常量定义，本地树不把 checksum_error_macro 列为已实现的目录。

源码入口：[租户目录创建](../../../src/storage/shared_storage/ob_dir_manager.cpp#L374)、[SERVER_META](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L2244)、[TENANT_DISK_SPACE_META](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L2998)、[500 租户 SLOG](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L2486)、[META_FILE 配额统计](../../../src/storage/shared_storage/ob_file_manager.cpp#L1348)、[远程 cache checkpoint](../../../src/storage/shared_storage/ob_dir_manager.cpp#L2037)。

### 3.1 Scatter（散列桶）参数（ob_dir_manager.h）

| 常量 | 值 | 使用格式 | 应用场景 |
|------|---|----------|----------|
| PRIVATE_MACRO_SCATTER_DIR_NUM | 100 | %02ld（十进制 00-99） | tablet_data/ |
| PRIVATE_TABLET_META_SCATTER_DIR_NUM | 100 | %02ld | ls/&lt;ls&gt;/tablet_meta/ |
| TMP_FILE_SCATTER_DIR_NUM | 1000 | %03ld（十进制 000-999） | tmp_data/ |
| SHARED_MACRO_SCATTER_DIR_NUM | 256 | %02lX（十六进制 00-FF） | 4 个 shared_*_macro_cache/、shared_tablet_sub_meta_cache/ 中走用户 tablet 路径的对象 |
| EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM | 256 | %02lX | external_table_file/ |

| 散列对象 | 计算方式 |
|----------|----------|
| 私有宏块、私有 tablet meta | tablet_id % 100 |
| 用户 tablet 共享宏块/子元数据 | macro_id.hash() % 256 |
| 外表缓存 | macro_id.hash() % 256 |
| 临时文件 | (tmp_file_id / 1000) % 1000，先整除再取模 |

散列参数见 [ob_dir_manager.h](../../../src/storage/shared_storage/ob_dir_manager.h#L37)，临时文件公式见 [ObTmpFileType::to_local_path_format](../../../src/storage/blocksstable/ob_storage_object_type.cpp#L2114)。

---

## 4. 关键实现函数索引

### 4.1 ObDirManager（ob_dir_manager.cpp）

| 函数 | 职责 |
|------|------|
| set_object_storage_root_dir | 设置 &lt;object_storage_root_dir&gt; |
| fsync_dir | 目录 fsync |
| create_dir | 带磁盘配额预分配/回滚的 mkdir |
| create_scatter_dir | 批量创建 &lt;scatter&gt; 子目录 |
| create_tenant_dir | **租户初始化**：创建本地租户根、ls/tablet_data/tmp_data/slog、mini/minor/major/inc_major cache、tablet sub meta cache 与 external_table_file；见第 3 节源码入口 |
| create_ls_id_dir | 建出 ls/&lt;ls_id&gt;\_&lt;ls_epoch&gt;/tablet_meta/&lt;scatter&gt;/ |
| create_tablet_meta_tablet_id_dir | 建 tablet_meta/&lt;scatter&gt;/&lt;tablet_id&gt;/ |
| create_tablet_data_tablet_id_private_macro_third_id_dir | 建 tablet_data/&lt;scatter&gt;/&lt;tablet_id&gt;/&lt;private_macro_third_id&gt;/{data,meta}/ |
| create_tablet_meta_tablet_id_private_macro_third_id_dir | 建 tablet_meta/&lt;scatter&gt;/&lt;tablet_id&gt;/&lt;private_macro_third_id&gt;/ |
| create_tmp_file_dir | 建 tmp_data/&lt;scatter&gt;/&lt;tmp_file_id&gt;/ |
| delete_dir / delete_dir_rec | 删除目录并归还配额 |
| get_local_cache_root_dir / get_local_\*_dir 系列 | 各级目录 path getter |

### 4.2 类型接口与支持范围

42 个具体类型与 ObStorageObjectType 的有效枚举值一一对应，按需覆盖 ObStorageObjectTypeBase 的接口。基类的路径格式化、反解析、父目录和 effective tablet 接口默认返回 OB_NOT_SUPPORTED；不能由一个接口存在就推断所有类型都支持它。

| 接口 | 作用与边界 |
|------|------------|
| to_local_path_format | 生成本地路径；SERVER_META 仅有本地形态，MACRO_CACHE_CKPT_\* 没有本地实现 |
| to_remote_path_format | 生成对象存储路径，或在部分元数据类型中转到逻辑相对路径；.T33–.T41 未实现该接口 |
| to_relative_remote_path_format | 生成共享对象相对路径或表内逻辑键；部分类型支持内部 tablet 分支 |
| to_ss_meta_ctrl_prefix_path_format | 生成控制行查询前缀，仅适用于支持该接口的 CONTROL 类型 |
| local_path_to_macro_id / remote_path_to_macro_id / relative_remote_path_to_macro_id | 反解析各自支持的路径；CONTROL 入口还会恢复 location，不能单靠相对键区分所有后端 |
| get_parent_dir / create_parent_dir | 获取或创建支持该接口的本地父目录 |
| get_effective_tablet_id | 为支持的对象提取缓存归属标识；调用方包括缓存管理和读写路径 |
| is_pin_local / is_direct_read / write_strategy / need_fsync | 各类型的存储与读写策略标志 |

本地和远程 formatter 都需要传入运行上下文，包括 tenant_id、tenant_epoch_id、server_id、ls_id、ls_epoch_id；并非所有字段都由 MacroBlockId 自身携带。特别是 PRIVATE_TABLET_META 的父目录使用外部 ls_id/ls_epoch_id。

源码入口：[基类接口](../../../src/storage/blocksstable/ob_storage_object_type.h#L154)、[统一派发](../../../src/storage/blocksstable/ob_ss_obj_util.cpp#L377)、[CONTROL 反解析](../../../src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.cpp#L422)、[缓存归属调用](../../../src/storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.cpp#L2716)。

---

## 5. 本地原子写与临时文件删除后缀

ObPathContext::set_atomic_write_ctx 和 set_logical_delete_ctx 都将 is_local_cache_ 设置为 true；is_valid 明确拒绝远程上下文使用这两种标志。

```
<local_regular_path>.T<type>               ← 本地正式文件
<local_regular_path>.T<type>.tmp.<seq>     ← 本地原子写中间态
<local_tmp_path>/seg<seg>.T8.deleted      ← 本地 TMP_FILE 逻辑删除标记
```

| 上下文设置 | 路径行为 |
|------------|----------|
| set_file_ctx(file_id, ls_epoch_id, is_local_cache, server_id, ls_id) | 按指定上下文生成普通路径 |
| set_atomic_write_ctx(file_id, ls_epoch_id, write_seq, server_id, ls_id) | 先生成本地路径，成功后追加 .tmp.&lt;write_seq&gt; |
| set_logical_delete_ctx(file_id, ls_epoch_id, server_id, ls_id) | 先生成本地路径；仅当 SSObjUtil::is_tmp_file(file_id) 为 true 时追加 .deleted |

当前 is_tmp_file 返回 true 的对象类型是 TMP_FILE（.T8）。因此这里的 .deleted 不能推广为私有宏块、共享元数据或 OSS 对象的通用 tombstone，也不等同于租户删除对象 IS_SHARED_TENANT_DELETED.T18。

源码入口：[本地上下文约束](../../../src/storage/shared_storage/ob_file_helper.cpp#L55)、[上下文设置](../../../src/storage/shared_storage/ob_file_helper.cpp#L108)、[后缀条件](../../../src/storage/shared_storage/ob_file_helper.cpp#L195)。

---

## 6. 版本化 / 隔离点汇总

这些字段表达对象身份、版本或命名空间；它们的组合提供路径隔离，但仅凭目录形状不能证明完整的并发发布、引用保留或 GC 正确性。

| 字段 | 挂载位置 | 来源与作用范围 |
|------|----------|----------------|
| tenant_epoch_id | 本地及远程 server 私有区的 &lt;tenant_id&gt;\_&lt;tenant_epoch&gt;/ | 本机创建租户实例的 prepare 阶段分配，计数器位于 server superblock；不只用于租户 drop/重建 |
| ls_epoch_id | 私有 tablet meta 的 ls/&lt;ls_id&gt;\_&lt;ls_epoch&gt;/ | 新建 LS item 时从 tenant superblock 分配；已有 CREATING item 的重试复用原值 |
| private_macro_third_id | 私有 tablet_data/tablet_meta 下的 &lt;tablet_id&gt;/&lt;private_macro_third_id&gt;/ | 当前三个私有对象构造器均调用 set_ss_third_id：内部 tablet 为 ls_id，用户 tablet 为 reorganization_scn |
| meta_version_id | ver&lt;version&gt;.T10 | 私有 tablet meta 的版本分配器递增，存在分配重试和重启间隔规则；不等同于任意内存元数据修改次数 |
| reorganization_scn | 共享区 tablet/&lt;tablet_id&gt;/&lt;reorg_scn&gt;/ | 用户 tablet 的物理组织版本；此路径段取对象构造时传入的 reorganization_scn |
| op / major_snapshot | SSTable 路径的 op_&lt;value&gt;/，本地文件名的 op&lt;value&gt; | 依对象类型解释，见下表 |
| compaction_scn | prewarm_info/scn&lt;scn&gt;.T13–.T16 | Major prewarm 信息所属的 compaction SCN |
| macro seq / tenant_seq | 文件名中的 seq&lt;seq&gt; | 私有宏块由租户序号发生器分配 tenant_seq；共享宏块的 seq 与 tablet、reorg、cg、op 等共同标识对象 |
| location | first_id_ bits 24–25 | OBJECT_STORAGE=0、TABLE=1、CONTROL=2；路径字符串不一定包含该字段，须保留访问上下文 |

| 对象类型 | op 段的含义 | 构造规则 |
|----------|-------------|----------|
| Mini/Minor .T2–.T5 | compaction op_id | 宏块 writer 使用 get_op_id() |
| Major .T6/.T7 | major_snapshot | major/medium merge 使用 get_snapshot_version()，即使目录字面量仍是 op |
| DDL Inc-major .T26/.T27 | DDL 宏块代际标识，本文模板记作 ddl_block_op_id | 优先采用正值 op_id；未指定时回退到 snapshot_version。异步索引刷新传入每次物理执行的 ddl_build_id |
| Compaction Inc-major .T28/.T29 | atomic protocol op_id | inc-major compaction 使用 get_op_id() |

DDL 数据宏块的规则在 ObDataStoreDesc::get_inc_major_ddl_block_op_id，持久化元数据宏块在 ObTabletPersisterParam 中使用相同的“指定值优先、snapshot 回退”规则。异步索引刷新通过 ObDDLTaskParam::get_macro_block_op_id 提供 ddl_build_id；普通 DDL 未指定正值时才使用 snapshot。

源码入口：[tenant epoch 分配入口](../../../src/storage/meta_store/ob_server_storage_meta_service.cpp#L150)、[server 计数器](../../../src/storage/blocksstable/ob_object_manager.cpp#L622)、[LS epoch 分配与复用](../../../src/storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.cpp#L1697)、[meta version 分配](../../../src/storage/meta_mem/ob_tablet_pointer.cpp#L740)、[writer 选择类型及第四段](../../../src/storage/blocksstable/ob_macro_block_writer.cpp#L2624)、[DDL 代际选择](../../../src/storage/blocksstable/ob_data_store_desc.h#L358)、[持久化参数](../../../src/storage/tablet/ob_tablet_persist_common.h#L121)、[异步刷新 build id](../../../src/storage/ddl/ob_ddl_struct.cpp#L978)。

---

## 7. 路径示例

以下 ID 和版本号用于说明格式。对象存储示例需替换 &lt;root&gt; 及实际上下文字段；内表逻辑键用于查表，不能作为 OSS 对象名。引用对象时仍须使用真实 MacroBlockId 和对应的 server/tenant/LS 上下文。

### 7.1 对象存储完整路径

```
# 私有数据宏块（server_3 的租户 1002）
<root>/cluster_1/server_3/1002_1/tablet_data/500021/8/data/svr3op17seq5.T0
<root>/cluster_1/server_3/1002_1/tablet_data/500021/8/meta/svr3op17seq5.T1

# 共享 Mini 数据宏块（用户 tablet）
<root>/cluster_1/tenant_1002/tablet/500021/100/mini/sstable/op_17/data/seq5.T2

# 共享 Minor meta 宏块（LS 内部 TX_DATA）
<root>/cluster_1/tenant_1002/ls/1001/TX_DATA/minor/sstable/op_9/meta/seq3.T5

# 共享 Major 数据/元数据宏块（cg_0，major_snapshot=1711234567）
<root>/cluster_1/tenant_1002/tablet/500021/100/major/sstable/cg_0/op_1711234567/data/seq2.T6
<root>/cluster_1/tenant_1002/tablet/500021/100/major/sstable/cg_0/op_1711234567/meta/seq3.T7

# DDL Inc-major 元数据宏块（未指定正值 op_id，回退到 snapshot_version=17）
<root>/cluster_1/tenant_1002/tablet/500021/100/inc_major/sstable/cg_0/op_17/meta/seq1.T27

# 异步索引刷新 Inc-major 元数据宏块（ddl_build_id=9001；不是 snapshot_version）
<root>/cluster_1/tenant_1002/tablet/500021/100/inc_major/sstable/cg_0/op_9001/meta/seq1.T27

# compaction inc-major 元数据宏块（op_18 表示 atomic protocol op-id）
<root>/cluster_1/tenant_1002/tablet/500021/100/inc_major/sstable/cg_0/op_18/meta/seq1.T29

# Major Prewarm 索引
<root>/cluster_1/tenant_1002/tablet/500021/100/major/prewarm_info/scn1711234567.T14

# 私有 tablet meta
<root>/cluster_1/server_3/1002_1/ls/1001_2/tablet_meta/500021/0/ver42.T10

# 私有 SLOG（普通租户）
<root>/cluster_1/server_3/1002_1/slog/seq1020.T11

# 服务端 SLOG（500 租户）
<root>/cluster_1/server_3/server_slog/seq33.T11

# 租户级 checkpoint
<root>/cluster_1/server_3/1002_1/ckpt/42.T12

# 租户 Root Key
<root>/cluster_1/tenant_1002/TENANT_ROOT_KEY.T22

# Tablet 元数据（对象存储，location=OBJECT_STORAGE）
<root>/cluster_1/tenant_1002/tablet/500021/100/meta/scn00000000001711234567.T20

# Tablet 子元数据（对象存储，location=OBJECT_STORAGE）
<root>/cluster_1/tenant_1002/tablet/500021/100/meta/op1seq0000000123.T21

# 租户删除标记
<root>/cluster_1/tenant_1002/IS_SHARED_TENANT_DELETED.T18
```

### 7.2 内表逻辑键与设备路径

```
# __all_shared_meta 的 tablet 元数据键（location=TABLE，无 table:// 前缀）
tablet/500021/100/meta/scn00000000001711234567.T20

# __all_shared_tiered_meta 的子元数据：访问设备的路径
table://tablet/500021/100/meta/op1seq0000000123.T21

# 同一条子元数据在表中的 meta_key
tablet/500021/100/meta/op1seq0000000123.T21

# __all_shared_meta_control 的 tablet 控制键（location=CONTROL）
tablet/500021/100/meta/scn00000000001711234567.T20

# tablet 锁行：固定 scn=0
tablet/500021/100/lock/scn00000000000000000000.T37

# LS checkpoint 锁行：固定 scn=0
ls/1001/lock/scn00000000000000000000.T38
```

.T20 的普通元数据键和控制键在此例中字符串相同，但所属表及记录内容不同；排查时必须保留 location 和访问入口。

### 7.3 本地路径

下面的散列桶 00 仅示意路径形状；定位真实宏块时须使用实际 MacroBlockId.hash() 计算桶号。

```
<local_cache_root>/SERVER_META.T9
<local_cache_root>/server_slog/seq33.T11
<local_cache_root>/1002_1/TENANT_DISK_SPACE_META.T17
<local_cache_root>/1002_1/slog/seq1020.T11
<local_cache_root>/1002_1/shared_major_macro_cache/00/t500021rs100cg0op1711234567seq2.T6
<local_cache_root>/1002_1/shared_major_macro_cache/00/t500021rs100cg0op1711234567seq3.T7
<local_cache_root>/1002_1/shared_tablet_sub_meta_cache/00/t500021rs100op1seq123.T21
```

---

## 8. 设计小结

1. **对象存储有两类命名空间。** server 私有区承载私有宏块、TMP、SLOG、CKPT、MacroCacheCkpt，普通租户层为 &lt;tenant_id&gt;\_&lt;tenant_epoch&gt;；500 租户的 SLOG/CKPT 直接位于 server_slog/server_ckpt。tenant 共享区承载共享对象的对象存储形态，其路径不含 server_id。

2. **表内逻辑键单独路由。** TABLE/CONTROL 记录按具体类型进入相应内表；.T20/.T30/.T31 的控制行与普通元数据复用类型号。路径相同不代表后端或内容相同。

3. **内部 tablet 分支取决于具体 formatter。** Mini/Minor、tablet meta/sub-meta，以及 tablet/SSTable 控制键实现了内部 tablet 分支。Major 和两类 Inc-major 宏块的 formatter 使用 tablet/&lt;id&gt;/&lt;reorg&gt;/ 模板，不能套用 ls/&lt;ls_id&gt;/&lt;TABLET_NAME&gt;/ 形式。

4. **op 的含义由类型和构造流程决定。** Mini/Minor 使用 op_id；Major 使用 major_snapshot；DDL Inc-major 使用指定的 DDL 宏块代际标识或 snapshot 回退值；Compaction Inc-major 使用 atomic protocol op_id。Major 与 Inc-major 都有 cg_&lt;cg&gt;/op_&lt;value&gt;/ 两层。

5. **本地目录按对象类别组织。** 用户 tablet 共享缓存采用散列桶加扁平文件名，私有宏块和临时文件保留多层目录。SERVER_META、server_slog 是本地根目录下的例外；.tmp.&lt;seq&gt; 和 TMP_FILE 的 .deleted 属于本地文件操作。

6. **目录形状用于定位，生命周期另有判定。** tablet、third_id、op、seq 等可以帮助识别对象代际；实际删除还必须依据相应 GC/引用状态，不能把某个目录前缀的存在视为可删除条件。get_effective_tablet_id 的接口用途也包括缓存归属，不能只解释为数据 GC 锚点。

# OceanBase 共享存储目录结构梳理（详细版 · 4.6.2）

> 代码分支：lakebase-master；关注 `OB_BUILD_SHARED_STORAGE` 宏闭包内的实现。
>
> 本文为 4.6.2 版本目录结构说明，相对 4.6.0（`docs/architecture_design/4.6.0/ss-directory-structure.md`）的主要差异：
> `SHARED_TABLET_SUB_META`、`SHARED_TABLET_META`、`SHARED_LS_CKPT`、`SHARED_LS_META` 的对象存储形态与 in-table 形态使用同一 `ObStorageObjectType` 枚举值，由 `MacroBlockId.location_` 区分存储后端：
> - `location=OBJECT_STORAGE`（0）：使用对象存储；需要本地缓存的类型仍按各自的 local path formatter 生成缓存路径；
> - `location=TABLE`（1）：写入 inner table（如 `__all_shared_tiered_meta`），逻辑相对路径由 `ObPathContext` 加 `table://` 前缀后路由到 `ObTableDevice`。
>
> 当前 `ObStorageObjectType` 为连续的 0-41。`SHARED_TABLET_META`、
> `SHARED_TABLET_SUB_META`、`SHARED_INC_MAJOR_COMPACTION_DATA_MACRO`、
> `SHARED_INC_MAJOR_COMPACTION_META_MACRO`、`SHARED_LS_CKPT`、`SHARED_LS_META`
> 分别为 `.T20`、`.T21`、`.T28`、`.T29`、`.T30`、`.T31`。
> `SHARED_TABLET_SUB_META` 的对象存储完整路径和 in-table 逻辑相对路径均将 seq
> 补齐到 10 位；本地 cache 文件名仍使用不补零的 seq。

## 0. 代码总览

| 模块 | 路径 | 主要职责 |
|------|------|----------|
| 目录/文件常量 | `close_modules/shared_storage/storage/shared_storage/ob_file_helper.h` | 定义 `CLUSTER_DIR_STR="cluster"` 等字面量及键名 |
| 对象类型枚举 + 虚基类 | `src/storage/blocksstable/ob_storage_object_type.h` | `ObStorageObjectType` (uint8)、`ObStorageObjectTypeBase` 抽象接口 |
| 路径格式化主实现 | `src/storage/blocksstable/ob_storage_object_type.cpp`（每个 type 各自实现 `to_local/remote_path_format` / `to_relative_remote_path_format` / `remote_path_to_macro_id` / `local_path_to_macro_id`） |
| 共享基方法 | `ObStorageObjectTypeBase::stract_remote_path_format()` | 所有 tenant 共享类型走"<root>/cluster_<cid>/tenant_<tid>/<relative_path>" |
| MacroBlockId ↔ 路径工具 | `src/storage/blocksstable/ob_ss_obj_util.cpp` | 对 `ObStorageObjectTypeBase` 的统一派发 |
| 目录/文件上下文 | `ObPathContext`、`ObFileHelper`（`ob_file_helper.h`） | 构造完整路径并附加 `.tmp.<seq>` / `.deleted` 后缀 |
| 目录/磁盘配额管理 | `close_modules/shared_storage/storage/shared_storage/ob_dir_manager.cpp` | `create_tenant_dir` / `create_ls_id_dir` / `create_scatter_dir` |

---

## 1. 字面量速查表（`ob_file_helper.h`）

### 1.1 目录字面量
| 常量 | 值 | 作用 |
|------|---|------|
| `CLUSTER_DIR_STR` | `"cluster"` | cluster 层 |
| `SERVER_DIR_STR` | `"server"` | server 层（节点私有） |
| `TENANT_DIR_STR` | `"tenant"` | tenant 层（租户共享） |
| `LS_DIR_STR` | `"ls"` | LS/LogStream 层 |
| `TABLET_DIR_STR` | `"tablet"` | 用户 tablet 层 |
| `TABLET_DATA_DIR_STR` | `"tablet_data"` | 私有 macro block（per-server） |
| `TABLET_META_DIR_STR` | `"tablet_meta"` | 私有 tablet 元数据 |
| `META_DIR_STR` | `"meta"` | 元数据子目录名 |
| `SHARED_TABLET_SSTABLE_DIR_STR` | `"sstable"` | 共享 SSTable 子目录名 |
| `TMP_DATA_DIR_STR` | `"tmp_data"` | TMP_FILE 数据 |
| `CKPT_STR` | `"ckpt"` | 租户/服务端 checkpoint |
| `SLOG_STR` | `"slog"` | Storage Log |
| `MINI_DIR_STR` / `MINOR_DIR_STR` / `MAJOR_DIR_STR` / `INC_MAJOR_DIR_STR` | `"mini"/"minor"/"major"/"inc_major"` | compaction 级别 |
| `DATA_MACRO_DIR_STR` / `META_DIR_STR` | `"data"/"meta"` | 宏块子类型（数据/元数据）|
| `SHARED_MINI_MACRO_CACHE_DIR_STR` | `"shared_mini_macro_cache"` | 本地 cache：共享 mini 宏块 |
| `SHARED_MINOR_MACRO_CACHE_DIR_STR` | `"shared_minor_macro_cache"` | 本地 cache：共享 minor 宏块 |
| `SHARED_MAJOR_MACRO_CACHE_DIR_STR` | `"shared_major_macro_cache"` | 本地 cache：共享 major 宏块 |
| `INC_MAJOR_DATA_DIR_STR` | `"shared_inc_major_macro_cache"` | 本地 cache：共享增量 major |
| `SHARED_TABLET_SUB_META_CACHE_DIR_STR` | `"shared_tablet_sub_meta_cache"` | 本地 cache：tablet 子元数据 |
| `EXTERNAL_TABLE_FILE_DIR_STR` | `"external_table_file"` | 外表文件 |
| `MACRO_CACHE_CKPT_DIR_STR` | `"macro_cache_ckpt"` | macro cache checkpoint |
| `CKM_ERROR_DIR_STR` | `"checksum_error_macro"` | checksum 错误保存区 |
| `OP_ID_DIR_STR` | `"op"` | op 目录（仅作目录名片段） |
| `PREWARM_INFO_DIR_STR` | `"prewarm_info"` | Major prewarm 信息 |
| `COMPACTION/COMPACTOR/SCHEDULER_DIR_STR` | `"compaction"/"compactor"/"scheduler"` | 合并调度器相关 |
| `TX_CTX/TX_DATA/TX_LOCK/REORG_INFO_TABLET_DIR_STR` | `"TX_CTX"/"TX_DATA"/"TX_LOCK"/"REORG_INFO"` | 4 个 LS 内部 tablet 的固定目录名 |

### 1.2 文件名 key / 后缀
| 常量 | 值 | 含义 |
|------|---|------|
| `OBJECT_TYPE_STR` | `.T` | 文件后缀，尾部 `<N>` 为 `ObStorageObjectType` 枚举值 |
| `DEFAULT_TMP_STR` | `.tmp.` | 原子写中间态（带 write_seq） |
| `DEFAULT_DELETED_STR` | `.deleted` | 逻辑删除 tombstone |
| `SVR_KEY_STR` | `svr` | server_id |
| `SEQ_KEY_STR` | `seq` | 单调 seq（macro_seq / tenant_seq） |
| `OP_KEY_STR` | `op` | compaction/transfer op id |
| `TABLET_KEY_STR` | `t` | tablet_id 在扁平文件名中的前缀 |
| `REORG_KEY_STR` | `rs` | reorganization_scn |
| `CG_KEY_STR` | `cg` | column_group id |
| `SEG_KEY_STR` | `seg` | tmp file segment id |
| `VER_KEY_STR` | `ver` | 版本号（meta_version_id 等） |
| `SCN_KEY_STR` | `scn` | SCN / compaction_scn |
| `LEN_KEY_STR` | `len` | UNSEALED_REMOTE_SEG_FILE 的 valid_length |
| `IDX_KEY_STR` | `idx` | EXTERNAL_TABLE_FILE 的 offset/2MB 索引 |
| `TFR_KEY_STR` | `tsfr` | （备用） |
| `OBJ_TIERED_METADATA_SEQ_WIDTH` | `10` | `SHARED_TABLET_SUB_META` 的对象存储完整路径和逻辑相对路径中，seq 用 `%010ld` 零填充；本地 cache 路径不补零 |

### 1.3 对象类型枚举完整表（`ob_storage_object_type.h`）
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

> **存储后端区分**：上述四种元数据类型由 `MacroBlockId.location_`（`SF_BIT_LOCATION=2`，`first_id_` bits 24-25）区分：
> - `OBJECT_STORAGE=0`：`to_remote_path_format()` 生成对象存储完整路径
> - `TABLE=1`：`ObPathContext` 添加 `table://` 前缀并调用 `to_relative_remote_path_format()`，由此访问 inner table
>
> 两种后端共享同一逻辑相对路径格式和 `.T<type>` 后缀。其中 `SHARED_TABLET_SUB_META` 的远程/逻辑路径 seq 均按 `OBJ_TIERED_METADATA_SEQ_WIDTH=10` 补零；local cache formatter 不补零。

---

## 2. 远程存储完整目录树

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
    │       │   └── <tablet_id>/<macro_private_transfer_epoch>/
    │       │       ├── data/svr<svr>seq<seq>.T0
    │       │       └── meta/svr<svr>seq<seq>.T1
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
    │       │       └── <tablet_id>/<meta_private_transfer_epoch>/
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
        │   └── TX_CTX / TX_DATA / TX_LOCK / REORG_INFO/
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
        │   ├── major/sstable/cg_<cg_id>/
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
            └── {macros|tablet_meta_scns}/bkt<bucket>.T32
```

### 2.1 关键路径模板与对应实现

| 类型 (.T) | 路径模板 | 实现 |
|----------|----------|------|
| PRIVATE_DATA_MACRO / PRIVATE_META_MACRO (.T0/.T1) | `<root>/cluster_<cid>/server_<svr>/<tid>_<tenant_epoch>/tablet_data/<tablet_id>/<transfer_epoch>/{data\|meta}/svr<svr>seq<seq>.T<n>` | `ObPrivateDataMacroType` / `ObPrivateMetaMacroType` |
| SHARED_MINI_*MACRO (.T2/.T3) | 用户tablet: `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/mini/sstable/op_<op>/{data\|meta}/seq<seq>.T<n>`；LS内部: `…/tenant_<tid>/ls/<ls_id>/<TABLET_NAME>/mini/sstable/op_<op>/{data\|meta}/seq<seq>.T<n>` | `ObSharedMiniDataMacroType` / `ObSharedMiniMetaMacroType` |
| SHARED_MINOR_*MACRO (.T4/.T5) | 同上，`mini` 换成 `minor` | `ObSharedMinorDataMacroType` / `ObSharedMinorMetaMacroType` |
| SHARED_MAJOR_*MACRO (.T6/.T7) | `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/major/sstable/cg_<cg_id>/{data\|meta}/seq<seq>.T<n>` | `ObSharedMajorDataMacroType` / `ObSharedMajorMetaMacroType` |
| TMP_FILE (.T8) | `<root>/cluster_<cid>/server_<svr>/<tid>_<epoch>/tmp_data/<tmp_file_id>/seg<seg>.T8` | `ObTmpFileType` |
| SERVER_META (.T9) | **仅本地**（pin_local）：`<local_cache_root>/SERVER_META.T9` | `ObServerMetaType`（无 remote 实现） |
| PRIVATE_TABLET_META (.T10) | `<root>/cluster_<cid>/server_<svr>/<tid>_<epoch>/ls/<ls_id>_<ls_epoch>/tablet_meta/<tablet_id>/<meta_private_transfer_epoch>/ver<meta_version_id>.T10` | `ObPrivateTabletMetaType` |
| PRIVATE_SLOG_FILE (.T11) | 普通租户：`…/server_<svr>/<tid>_<epoch>/slog/seq<file_id>.T11`；500 租户：`…/server_<svr>/server_slog/seq<file_id>.T11` | `ObPrivateSlogFileType` |
| PRIVATE_CKPT_FILE (.T12) | 普通租户：`…/server_<svr>/<tid>_<epoch>/ckpt/<file_id>.T12`；500 租户：`…/server_<svr>/server_ckpt/<file_id>.T12` | `ObPrivateCkptFileType` |
| MAJOR_PREWARM_{DATA,DATA_INDEX,META,META_INDEX} (.T13-16) | `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/major/prewarm_info/scn<compaction_scn>.T<n>` | 对应 `ObMajorPrewarm*Type` |
| TENANT_DISK_SPACE_META (.T17) | **仅本地**（pin_local） | `ObTenantDiskSpaceMetaType` |
| IS_SHARED_TENANT_DELETED (.T18) | `<root>/cluster_<cid>/tenant_<tid>/IS_SHARED_TENANT_DELETED.T18` | `ObIsSharedTenantDeletedType::to_remote_path_format()` |
| UNSEALED_REMOTE_SEG_FILE (.T19) | `…/server_<svr>/<tid>_<epoch>/tmp_data/<tmp_file_id>/seg<seg>len<len>.T19` | `ObUnsealedRemoteSegFileType::to_remote_path_format()` |
| SHARED_TABLET_META (.T20) | **对象存储**（`location=OBJECT_STORAGE`）：用户 tablet `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/meta/scn<20-digit>.T20`；LS 内部 `…/tenant_<tid>/ls/<ls_id>/<TABLET_NAME>/meta/scn<20-digit>.T20`。**in-table**（`location=TABLE`）：使用去掉 `<root>/cluster_<cid>/tenant_<tid>/` 前缀的同形逻辑相对路径 | `ObSharedTabletMetaType::to_remote_path_format()` / `to_relative_remote_path_format()` |
| SHARED_TABLET_SUB_META (.T21) | **对象存储**（`location=OBJECT_STORAGE`）：用户 tablet `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/meta/op<op>seq<10-digit>.T21`；LS 内部 `…/tenant_<tid>/ls/<ls_id>/<TABLET_NAME>/meta/op<op>seq<10-digit>.T21`。**in-table**（`location=TABLE`）：数据存 `__all_shared_tiered_meta`；使用去掉 `<root>/cluster_<cid>/tenant_<tid>/` 前缀的同形逻辑相对路径，由 `ObPathContext` 添加 `table://` 前缀 | `ObSharedTabletSubMetaType::to_remote_path_format()` / `to_relative_remote_path_format()` |
| TENANT_ROOT_KEY (.T22) | `<root>/cluster_<cid>/tenant_<tid>/TENANT_ROOT_KEY.T22` | `ObTenantRootKeyType::to_remote_path_format()` |
| EXTERNAL_TABLE_FILE (.T23) | **仅本地** | `ObExternalTableFileType::to_local_path_format()` |
| MACRO_CACHE_CKPT_DATA (.T24) | `…/server_<svr>/<tid>_<epoch>/macro_cache_ckpt/data/<version_id>/seq<seq_id>.T24` | `ObMacroCacheCkptDataType::to_remote_path_format()` |
| MACRO_CACHE_CKPT_META (.T25) | `…/server_<svr>/<tid>_<epoch>/macro_cache_ckpt/meta/ver<version_id>.T25` | `ObMacroCacheCkptMetaType::to_remote_path_format()` |
| SHARED_INC_MAJOR_*MACRO (.T26/.T27) | `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/inc_major/sstable/cg_<cg_id>/op_<snapshot_version>/{data\|meta}/seq<seq>.T<n>` | `ObSharedIncMajorDataMacroType` / `ObSharedIncMajorMetaMacroType` |
| SHARED_INC_MAJOR_COMPACTION_*MACRO (.T28/.T29) | `…/tenant_<tid>/tablet/<tablet_id>/<reorg_scn>/inc_major/sstable/cg_<cg_id>/op_<op_id>/{data\|meta}/seq<seq>.T<n>` | `ObSharedIncMajorCompactionDataMacroType` / `ObSharedIncMajorCompactionMetaMacroType` |
| SHARED_LS_CKPT / SHARED_LS_META (.T30/.T31) | **对象存储**：`…/tenant_<tid>/ls/<ls_id>/meta/scn<20-digit>.T<n>`；**in-table**：去掉 tenant 前缀的同形逻辑相对路径 | `ObSharedLsCkptType` / `ObSharedLsMetaType` |
| SHARED_MARK_SWEEP_GC_DIAG (.T32) | `…/tenant_<tid>/mark_sweep_gc_diag/round<round>/{del\|kept}/{macros\|tablet_meta_scns}/bkt<bucket>.T32` | `ObSharedMarkSweepGcDiagType::to_relative_remote_path_format()` |

### 2.2 统一 helper

- `ObStorageObjectTypeBase::stract_remote_path_format()`
  ```
  "<object_storage_root>/cluster_<cid>/tenant_<tid>/<relative_path>"
  ```
  所有租户共享对象都走它；`<relative_path>` 由各类型的 `to_relative_remote_path_format()` 生成。这意味着 **所有 shared* 类型的 remote path 都只挂在 `tenant_<tid>/` 下**，server_id 不出现在路径里。

- Private* 类型 / TMP_FILE / UNSEALED_REMOTE_SEG_FILE / MACRO_CACHE_CKPT_* 则**直接写全路径**（包含 `server_<svr>`），每个类单独 snprintf。

---

## 3. 本地 cache 目录结构

`ObDirManager::create_tenant_dir()` 一次性建出下列目录（实现位于
`ob_dir_manager.cpp`）：

```
<local_cache_root>/
└── <tenant_id>_<tenant_epoch_id>/
    ├── ls/                                          ← MACRO_BLOCK 磁盘配额入口
    │   └── <ls_id>_<ls_epoch_id>/
    │       └── tablet_meta/<scatter[00-99]>/        ← PRIVATE_TABLET_META
    │           └── <tablet_id>/<transfer_epoch>/ver<ver>.T10
    │
    ├── tablet_data/<scatter[00-99]>/                ← PRIVATE_DATA/META_MACRO
    │   └── <tablet_id>/<transfer_epoch>/
    │       ├── data/svr<svr>seq<seq>.T0
    │       └── meta/svr<svr>seq<seq>.T1
    │
    ├── tmp_data/<scatter[000-999]>/                 ← TMP_FILE
    │   └── <tmp_file_id>/seg<seg>.T8
    │
    ├── slog/seq<file_id>.T11                        ← 租户 SLOG（500 租户改为 server_slog/）
    │
    ├── shared_mini_macro_cache/                     ← SHARED_MINI_*MACRO 本地缓存
    │   ├── <scatter[00-FF]>/
    │   │   └── t<tid>rs<reorg_scn>op<op>seq<seq>.T{2,3}
    │   └── ls/<ls_id>/<TABLET_NAME>_op<op>seq<seq>.T{2,3}
    │
    ├── shared_minor_macro_cache/                    ← SHARED_MINOR_*MACRO
    │   ├── <scatter[00-FF]>/…
    │   └── ls/<ls_id>/…
    │
    ├── shared_major_macro_cache/<scatter[00-FF]>/   ← SHARED_MAJOR_*MACRO
    │   └── t<tid>rs<reorg_scn>cg<cg>seq<seq>.T{6,7}
    │
    ├── shared_inc_major_macro_cache/<scatter[00-FF]>/← SHARED_INC_MAJOR_*MACRO
    │   ├── t<tid>rs<reorg_scn>cg<cg>op<op>seq<seq>.T{26,27}
    │   └── t<tid>rs<reorg_scn>cg<cg>op<op>seq<seq>.T{28,29} ← SHARED_INC_MAJOR_COMPACTION_*MACRO
    │
    ├── shared_tablet_sub_meta_cache/                ← SHARED_TABLET_SUB_META
    │   ├── <scatter[00-FF]>/t<tid>rs<reorg_scn>op<op>seq<seq>.T21
    │   └── ls/<ls_id>/<TABLET_NAME>_op<op>seq<seq>.T21
    │
    ├── external_table_file/<scatter[00-FF]>/        ← EXTERNAL_TABLE_FILE
    │   └── seq<server_seq_id>idx<offset_idx>.T23
    │
    ├── macro_cache_ckpt/                            ← MACRO_CACHE_CKPT_*
    └── checksum_error_macro/                        ← 校验出错的宏块隔离区
```

与远程不同点：
1. 本地路径里**没有 `cluster_<cid>` 和 `server_<svr>`**，只按 `<tenant>_<epoch>` 分区，因为每台机器只有自己的 cache。
2. 本地文件名是**扁平化**的（如 `t12345rs67890op100seq5.T2`），远程是**深层目录**（如 `tablet/12345/67890/mini/sstable/op_100/data/seq5.T2`）。扁平化是为了减少 mkdir 次数、加快打开。
3. 额外多了 `<scatter>` 一层，避免单目录文件数爆炸。
4. `SHARED_TABLET_SUB_META` 只有远程/逻辑相对路径将 seq 补齐到 10 位；上述本地 cache 文件名保留原始十进制 seq。

### 3.1 Scatter（散列桶）参数（`ob_dir_manager.h`）

| 常量 | 值 | 使用格式 | 应用场景 |
|------|---|----------|----------|
| `PRIVATE_MACRO_SCATTER_DIR_NUM` | 100 | `%02ld`（十进制 00-99） | `tablet_data/` |
| `PRIVATE_TABLET_META_SCATTER_DIR_NUM` | 100 | `%02ld` | `ls/<ls>/tablet_meta/` |
| `TMP_FILE_SCATTER_DIR_NUM` | 1000 | `%03ld`（十进制 000-999） | `tmp_data/` |
| `SHARED_MACRO_SCATTER_DIR_NUM` | 256 | `%02lX`（十六进制 00-FF） | 4 个 shared_*_macro_cache/、shared_tablet_sub_meta_cache/ |
| `EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM` | 256 | `%02lX` | `external_table_file/` |

私有宏块散列用 `tablet_id % 100`，共享宏块/子元数据用 `macro_id.hash() % 256`。

---

## 4. 关键实现函数索引

### 4.1 ObDirManager（`ob_dir_manager.cpp`）

| 函数 | 职责 |
|------|------|
| `set_object_storage_root_dir` | 设置 `<object_storage_root_dir>` |
| `fsync_dir` | 目录 fsync |
| `create_dir` | 带磁盘配额预分配/回滚的 mkdir |
| `create_scatter_dir` | 批量创建 `<scatter>` 子目录 |
| `create_tenant_dir` | **租户初始化**：一次性建出 ls/tablet_data/tmp_data/slog + 所有 shared_*_macro_cache + external_table_file + inc_major_macro_cache |
| `create_ls_id_dir` | 建出 `ls/<ls_id>_<ls_epoch>/tablet_meta/<scatter>/` |
| `create_tablet_meta_tablet_id_dir` | 建 `tablet_meta/<scatter>/<tablet_id>/` |
| `create_tablet_data_tablet_id_private_transfer_epoch_dir` | 建 `tablet_data/<scatter>/<tablet_id>/<transfer_epoch>/{data,meta}/` |
| `create_tmp_file_dir` | 建 `tmp_data/<scatter>/<tmp_file_id>/` |
| `delete_dir` / `delete_dir_rec` | 删除目录并归还配额 |
| `get_local_cache_root_dir` / `get_local_*_dir` 系列 | 各级目录 path getter |

### 4.2 每个类型的方法（`ob_storage_object_type.h` 虚接口）

```
class ObStorageObjectTypeBase {
  virtual int to_local_path_format(path, len, pos, file_id, tid, tenant_epoch, ls_epoch);
  virtual int to_remote_path_format(path, len, pos, file_id, root, cid, tid, tenant_epoch, svr, ls_epoch);
  virtual int to_relative_remote_path_format(path, len, pos, file_id);   // 供 stract_remote_path_format 拼接
  virtual int local_path_to_macro_id(path, macro_id);                    // 反解析
  virtual int remote_path_to_macro_id(path, macro_id);
  virtual int get_parent_dir(...);
  virtual int create_parent_dir(...);
  virtual int get_effective_tablet_id(macro_id, effective_tablet_id);    // 数据 GC 用
  virtual uint8_t write_strategy() const;                                // WRITE_THROUGH / WRITE_BACK / WRITE_THROUGH_AND_TRY_WRITE_LCACHE
  virtual bool is_pin_local() / is_direct_read() / need_fsync() / …;     // 读写策略标志
};
```

每个具体 type 类（`ObPrivateDataMacroType`、`ObSharedMajorDataMacroType` 等）都派生
该基类并覆盖上述虚函数，共 42 个具体类型，1:1 映射 `ObStorageObjectType` 枚举。

---

## 5. 文件名原子 & 删除语义

```
<regular_path>.T<type>               ← 正式文件
<regular_path>.T<type>.tmp.<seq>     ← 原子写中间态（ObPathContext::set_atomic_write_ctx）
<regular_path>.T<type>.deleted       ← 逻辑删除（ObPathContext::set_logical_delete_ctx）
```

写路径上下文见 `ob_file_helper.h` 中的 `ObPathContext`：
- `set_file_ctx`：普通文件
- `set_atomic_write_ctx(file_id, ls_epoch, write_seq)`：追加 `.tmp.<write_seq>`
- `set_logical_delete_ctx(file_id, ls_epoch)`：追加 `.deleted`

---

## 6. 版本化 / 隔离点汇总

所有"你可能以为会撞路径的地方"，代码都通过额外的 version 段位做了隔离：

| 版本字段 | 挂载位置 | 递增时机 |
|----------|----------|----------|
| `tenant_epoch_id` | server 私有区 `<tid>_<tenant_epoch>/` | 租户 drop / 重建 |
| `ls_epoch_id` | server 私有区 `ls/<ls_id>_<ls_epoch>/` | LS 副本重建 |
| `macro_private_transfer_epoch` | `tablet_data/<tablet_id>/<transfer_epoch>/` | tablet transfer 完成后递增，旧数据异步清理 |
| `meta_private_transfer_epoch` | `tablet_meta/<tablet_id>/<meta_private_transfer_epoch>/` | 同上（meta 侧） |
| `meta_version_id` | `ver<version>.T10` | 每次 tablet meta 更新 |
| `reorganization_scn` | 共享区 `tablet/<tid>/<reorg_scn>/` | tablet split/merge 等物理重组 |
| `op_<op>` | `sstable/op_<op>/` | 单次 compaction 任务号 |
| `compaction_scn` | `prewarm_info/scn<scn>.T13-16` | 每次 major compaction |
| `macro_seq`/`tenant_seq` | 文件名末尾 `seq<seq>` | 宏块级单调序号 |
| `MacroBlockId.location_` | SS 模式 `first_id_` bits 24-25（`SF_BIT_LOCATION=2`） | `OBJECT_STORAGE=0` 与 `TABLE=1`；随 `first_id_` 一并序列化 |

> inc-major 路径中，`.T26/.T27` 的 `op` 表示 `snapshot_version`，
> `.T28/.T29` 的 `op` 表示 atomic protocol `op_id`；对应 macro seq 均在各自代际内发号。

---

## 7. 远程路径完整示例（可直接去 OSS ls）

```
# 私有数据宏块（server_3 的租户 1002）
<root>/cluster_1/server_3/1002_1/tablet_data/500021/8/data/svr3seq5.T0
<root>/cluster_1/server_3/1002_1/tablet_data/500021/8/meta/svr3seq5.T1

# 共享 Mini 数据宏块（用户 tablet）
<root>/cluster_1/tenant_1002/tablet/500021/100/mini/sstable/op_17/data/seq5.T2

# 共享 Minor meta 宏块（LS 内部 TX_DATA）
<root>/cluster_1/tenant_1002/ls/1001/TX_DATA/minor/sstable/op_9/meta/seq3.T5

# 共享 Major 数据宏块（cg_0 单列组）
<root>/cluster_1/tenant_1002/tablet/500021/100/major/sstable/cg_0/data/seq2.T6

# DDL inc-major 元数据宏块（op_17 表示 snapshot_version）
<root>/cluster_1/tenant_1002/tablet/500021/100/inc_major/sstable/cg_0/op_17/meta/seq1.T27

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

# Tablet 子元数据（in-table，location=TABLE；存 __all_shared_tiered_meta，非 OSS 文件）
table://tablet/500021/100/meta/op1seq0000000123.T21

# 租户删除标记
<root>/cluster_1/tenant_1002/IS_SHARED_TENANT_DELETED.T18
```

---

## 8. 设计小结

1. **两套路径体系**：
   - **Server 私有区（`cluster_<cid>/server_<svr>/...`）** 放 Private/TMP/SLOG/CKPT/MacroCacheCkpt，路径里显式包含 server_id + `tenant_<tid>_<tenant_epoch>`。
   - **Tenant 共享区（`cluster_<cid>/tenant_<tid>/...`）** 放所有 SHARED_* / PREWARM / ROOT_KEY，路径里**不含 server_id**；其中 SSTable 宏块通过 `reorganization_scn` + 代际（snapshot/op-id）+ `seq` 隔离并发写入。

2. **LS 内部 tablet vs 用户 tablet 分叉**：所有涉及 tablet 的 shared 类型都通过 `file_id.meta_is_inner_tablet()` 判断，两条路径模板在 `to_relative_remote_path_format` 里并存。

3. **compaction 级别命名**：`mini/minor/major/inc_major` 目录名固定。Major 有 `cg_<cg>` 层以支持列存；mini/minor 使用 `op_<op>` 层，inc_major 使用 `cg_<cg>/op_<op>` 两层。DDL inc-major 的 `op` 表示 snapshot，compaction inc-major 的 `op` 表示 atomic protocol op-id，并由对象类型进一步隔离。

4. **本地 cache 为扁平文件 + 散列桶**：降低 open/mkdir 开销，而远程是深目录（可直接在 OSS 浏览并按前缀批量 list 清理）。

5. **数据回收锚点**：`get_effective_tablet_id` + `reorganization_scn` + `transfer_epoch` 给 GC 提供可识别的 tablet 粒度。

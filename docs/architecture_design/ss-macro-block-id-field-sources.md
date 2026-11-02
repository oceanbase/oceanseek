# Shared Storage 宏块 ID 字段来源梳理

> 版本：2f9de4c270afe63225a08b0eb83d1c8ad8ccb993<br>
> 范围：Shared Storage 模式下真正由 ObStorageObjectTypeBase::is_macro_data 或 is_macro_meta 判定的宏块对象。Shared Nothing 本地 ID、备份 ID，以及 tablet meta、LS meta、临时文件等非宏块对象不在本文展开。

## 1. 结论

Shared Storage 宏块物理 ID 统一落在 MacroBlockId 的 5 个 int64 字段：

| 字段 | 通用来源 | 说明 |
|---|---|---|
| first_id | 对象类型、CG ID、存储位置、ID 模式、版本压缩而成 | 不是顺序号；实际宏块创建时位置固定为 OBJECT_STORAGE |
| second_id | tablet_id | 私有与共享宏块都使用 tablet 维度 |
| third_id | LS 内部 tablet 使用 ls_id；用户 tablet 使用 reorganization_scn | 二选一，由 tablet 是否为 LS 内部 tablet 决定 |
| fourth_id | op_id 或 major_snapshot / snapshot_version | 按对象类型和写入场景重载 |
| fifth_id | 共享宏块使用 data_seq；私有宏块使用 tenant_seq | 共享与私有的发号器不同 |

普通宏块的主链路是：

```text
ObDataStoreDesc / 写任务参数
  → ObStorageObjectOpt
  → ObObjectManager::alloc_object
  → ObStorageObjectTypeBase::get_object_id
  → MacroBlockId
  → ObStorageObjectHandle
```

其中 ObStorageObjectOpt 只承载业务语义字段；first_id 的压缩位、second 到 fifth 的落位，以及类型校验，由 ObStorageObjectTypeBase 的具体类型实现完成。

## 2. 宏块类型边界

当前代码中共有 12 类宏块对象。判定依据是 src/storage/blocksstable/ob_storage_object_type.h 中具体类型覆写的 is_macro_data 或 is_macro_meta。

| 对象类型 | 枚举值 | 数据/元数据 | 主要来源 |
|---|---:|---|---|
| PRIVATE_DATA_MACRO | 0 | data | 普通/私有宏块写路径；私有宏块上传前读取本地宏块 |
| PRIVATE_META_MACRO | 1 | meta/index | 普通/私有宏块写路径；私有链式宏块信息 |
| SHARED_MINI_DATA_MACRO | 2 | data | mini 写路径；增量上传 mini 数据宏块 |
| SHARED_MINI_META_MACRO | 3 | meta/index | mini 写路径 |
| SHARED_MINOR_DATA_MACRO | 4 | data | minor 写路径；增量上传 minor 数据宏块 |
| SHARED_MINOR_META_MACRO | 5 | meta/index | minor 写路径 |
| SHARED_MAJOR_DATA_MACRO | 6 | data | normal MAJOR/MEDIUM、非 split 全量 DDL/direct-load 写路径 |
| SHARED_MAJOR_META_MACRO | 7 | meta/index | normal MAJOR/MEDIUM 写路径；链式 major 宏块信息 |
| SHARED_INC_MAJOR_DATA_MACRO | 26 | data | 增量 major DDL/direct-load 写路径 |
| SHARED_INC_MAJOR_META_MACRO | 27 | meta/index | 增量 major DDL/direct-load 写路径；链式增量 major 宏块信息 |
| SHARED_INC_MAJOR_COMPACTION_DATA_MACRO | 28 | data | 增量 major compaction 写路径 |
| SHARED_INC_MAJOR_COMPACTION_META_MACRO | 29 | meta/index | 增量 major compaction 写路径 |

这里的 META_MACRO 是写入侧 is_for_index_or_meta 分支的统称，包含索引树宏块和宏块元信息宏块；DATA_MACRO 对应数据宏块。

## 3. first_id 的字段来源

first_id 是一个 64 位 union。Shared Storage 视角的字段宽度如下：

| 逻辑片段 | 宽度 | 来源 | 取值 |
|---|---:|---|---|
| storage_object_type | 8 bit | ObStorageObjectOpt.object_type_ | 上表 12 类之一 |
| column_group_id | 16 bit | table_cg_idx；链式宏块信息与部分上传路径固定 0 | CG ID |
| location | 2 bit | ObStorageObjectTypeBase::set_ss_object_first_id_ | OBJECT_STORAGE=0 |
| reserved | 30 bit | 清零 | 0 |
| id_mode | 4 bit | 固定写入 ID_MODE_SHARE | 2 |
| version | 4 bit | MACRO_BLOCK_ID_VERSION_V1 | 1 |

关键点：

1. set_ss_object_first_id_ 先把 first_id 清零，再写入版本、模式、对象类型、CG 和位置，因此 reserved 不承载业务含义。
2. TABLE 和 CONTROL 位置用于共享元数据表或控制行，不是本文 12 类实际宏块对象的创建位置。
3. first_id 中没有租户、集群或 server 序号；这些上下文由远端路径前缀和写任务环境提供。

## 4. second_id 到 fifth_id 的字段来源

### 4.1 second_id

所有宏块统一填 tablet_id：

| 来源路径 | 字段 |
|---|---|
| 普通/倒排索引宏块写路径 | data_store_desc.get_tablet_id().id() |
| 链式宏块信息 | macro_info_param.tablet_id_.id()，最终来自 ObTabletPersisterParam.tablet_id_ |
| 增量上传 | output_table_key.tablet_id_ |
| HA 恢复复制 | 源宏块 ID 的 second_id |
| 失败 GC 探测 | 控制信息中的 tablet_id |

### 4.2 third_id

tablet 是否为 LS 内部 tablet 决定 third_id 的语义：

| 条件 | 填充字段 | 来源 |
|---|---|---|
| is_ls_inner_tablet == true | ls_id | data_store_desc.get_ls_id().id() 或持久化参数 ls_id_ |
| is_ls_inner_tablet == false | reorganization_scn | data_store_desc.get_reorganization_scn().get_val_for_tx() 或 tablet.get_reorganization_scn() |

MacroBlockId::set_ss_third_id 不是同时写两个字段，而是按开关只写其中一个；另一个 union 成员保持清零。

### 4.3 fourth_id

fourth_id 是最容易误读的字段，按对象族重载：

| 对象族 | fourth_id 语义 | 普通写路径来源 | 特殊路径 |
|---|---|---|---|
| PRIVATE_DATA_MACRO / PRIVATE_META_MACRO | op_id | data_store_desc.get_op_id() | 私有链式宏块信息固定 0 |
| SHARED_MINI_* / SHARED_MINOR_* | op_id | data_store_desc.get_op_id() | 增量上传来自上传任务 op_id |
| SHARED_MAJOR_* | major_snapshot | normal MAJOR/MEDIUM 与非 split DDL/direct-load 使用 snapshot_version | 遗留 shared-major helper 将 fourth_id 固定为 0 |
| SHARED_INC_MAJOR_* | snapshot_version | 增量 major DDL/direct-load 使用 snapshot_version | 链式信息来自持久化参数 snapshot_version_ |
| SHARED_INC_MAJOR_COMPACTION_* | op_id | data_store_desc.get_op_id() | 失败 GC 探测来自控制行 fourth_id |

normal MAJOR/MEDIUM 判断落在 compaction::is_major_merge_type 分支；增量 major compaction 优先级最高，其次才是增量 major DDL。

### 4.4 fifth_id

私有与共享宏块使用不同发号器：

| 对象族 | fifth_id 语义 | 发号来源 | 唯一性边界 |
|---|---|---|---|
| PRIVATE_DATA_MACRO / PRIVATE_META_MACRO | tenant_seq | TENANT_SEQ_GENERATOR.get_private_object_seq | 租户内预分配单调序列 |
| 10 类 SHARED_*_MACRO | data_seq / macro_seq | macro_seq_generator.get_current；链式宏块使用 start_macro_seq_ 递增 | 写任务分配的物理宏块序列窗口 |

共享宏块的 data_seq 是物理序列。写初始化先通过 get_next 取得物理序列；分配对象时使用 macro_seq_generator.get_current。链式宏块信息则从当前持久化 cur_macro_seq 开始，每写一个信息块递增一次。

## 5. 12 类宏块的完整映射

下表合并 first_id 内的类型/CG来源与业务字段来源：

| 对象类型 | first_id 类型来源 | second_id | third_id | fourth_id | fifth_id |
|---|---|---|---|---|---|
| PRIVATE_DATA_MACRO | 私有写路径选择 PRIVATE_DATA_MACRO，CG 固定 0 | tablet_id | ls_id 或 reorganization_scn | op_id | tenant_seq |
| PRIVATE_META_MACRO | index/meta 分支选择 PRIVATE_META_MACRO，CG 固定 0 | tablet_id | ls_id 或 reorganization_scn | op_id | tenant_seq |
| SHARED_MINI_DATA_MACRO | mini + data 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |
| SHARED_MINI_META_MACRO | mini + index/meta 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |
| SHARED_MINOR_DATA_MACRO | minor + data 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |
| SHARED_MINOR_META_MACRO | minor + index/meta 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |
| SHARED_MAJOR_DATA_MACRO | major/medium + data 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | major_snapshot | data_seq |
| SHARED_MAJOR_META_MACRO | major/medium + index/meta 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | major_snapshot | data_seq |
| SHARED_INC_MAJOR_DATA_MACRO | 增量 major DDL + data 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | snapshot_version | data_seq |
| SHARED_INC_MAJOR_META_MACRO | 增量 major DDL + index/meta 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | snapshot_version | data_seq |
| SHARED_INC_MAJOR_COMPACTION_DATA_MACRO | 增量 major compaction + data 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |
| SHARED_INC_MAJOR_COMPACTION_META_MACRO | 增量 major compaction + index/meta 分支，CG=table_cg_idx | tablet_id | ls_id 或 reorganization_scn | op_id | data_seq |

## 6. 主要生产链路

### 6.1 普通宏块写路径

ObMacroBlockWriter::alloc_block 负责：

1. 由 merge_type 选择 MAJOR、MINI、MINOR、INC_MAJOR 或 INC_MAJOR_COMPACTION；
2. 由 is_for_index_or_meta 选择 META_MACRO 或 DATA_MACRO；
3. 从 data_store_desc 填 tablet、LS/reorganization_scn、fourth_id、CG；
4. 从 macro_seq_generator 取当前物理序列；
5. 调 ObObjectManager::alloc_object 生成 MacroBlockId。

私有分支走 set_private_object_opt 或 set_private_meta_macro_object_opt；共享分支统一走 set_ss_share_object_opt。

### 6.2 倒排索引宏块写路径

ObInvIdxMacroBlockWriter 有一套独立 alloc_block 实现，但字段来源与普通写路径一致：tablet、LS/reorganization_scn、fourth_id、CG 来自 data_store_desc，data_seq 来自自身 macro_seq_generator。该路径只覆盖 mini/minor/major/增量 major 对象族，不生成增量 major compaction 类型。

### 6.3 增量上传路径

CopySSMacroBlockWriter 将本地私有宏块数据复制为共享 mini 或 minor DATA_MACRO：

1. tablet_id 来自 output_table_key；
2. ls_id 与 reorganization_scn 来自 tablet 上下文；
3. fourth_id 来自上传任务 op_id；
4. CG 固定 0；
5. data_seq 来自每个 IO 上下文递增的 macro_seq。

该路径只生成数据宏块；索引/元数据宏块由 index builder 后续生成。

### 6.4 链式宏块信息路径

ObLinkedMacroBlockWriter 用 ObLinkedMacroInfoWriteParam 生成链式信息块：

1. major 链式信息生成 SHARED_MAJOR_META_MACRO，fourth_id 为 major_snapshot；
2. 增量 major 链式信息生成 SHARED_INC_MAJOR_META_MACRO，fourth_id 为 snapshot_version；
3. 私有链式信息生成 PRIVATE_META_MACRO，fourth_id 固定 0；
4. fifth_id 从 cur_macro_seq 传入，并在每个信息块后递增；
5. tablet、LS/reorganization_scn 均来自 ObTabletPersisterParam。

### 6.5 恢复复制与失败 GC

HA 恢复复制不重新设计业务字段，而是把源 MacroBlockId 拆回 ObStorageObjectOpt，再生成同语义目标 ID。字段来源依次为源 ID的对象类型、tablet、fourth_id、data_seq、CG 与 reorganization_scn。

失败 GC 探测根据 SSTable 控制信息中的 LS、tablet、reorganization_scn、fourth_id、起始 seq、步长和 CG 构造候选 MacroBlockId，再批量存在性检查。这是重建候选 ID，不是新写宏块的发号路径。

## 7. 物理宏块 ID 与逻辑宏块 ID 的区别

ObLogicMacroBlockId 是另一个概念，用于宏块尾索引行和逻辑复用判断，字段包括：

1. data_seq：带块类型、合并类型、sstable logic seq 等信息的逻辑序列；
2. logic_version：logical_version；
3. tablet_id：tablet 维度；
4. info：column_group_idx 与 merge_type。

ObMacroBlockWriter::build_logic_macro_seq 会把物理 raw sequence 加上 sstable logic seq 等逻辑维度，用于 ObLogicMacroBlockId；但对象存储路径中的 MacroBlockId.fifth_id 使用的是物理 macro_seq_generator 当前值。不能把两个 data_seq 的语义混用。

## 8. 校验规则

生成后 MacroBlockId::is_valid 会检查：

1. Shared Storage 版本为 V1、ID 模式为 SHARE、reserved 为 0；
2. location 和 storage_object_type 在合法范围内；
3. 具体对象类型的 is_valid 检查 second/third/fourth/fifth 约束；
4. 私有宏块要求 tablet、third、op、tenant_seq 非负有效；
5. 共享宏块要求 tablet、third、fourth、seq 均有效。

因此，看到 fifth_id 时必须先看对象类型：私有是 tenant_seq，共享是 data_seq；看到 fourth_id 时也必须先看对象族，不能按统一 op_id 或统一快照解释。

## 9. 关键源码索引

| 主题 | 位置 |
|---|---|
| MacroBlockId 位布局与业务字段 | src/storage/blocksstable/ob_macro_block_id.h:198 |
| 对象类型枚举 | src/storage/blocksstable/ob_storage_object_type.h:32 |
| first_id 统一初始化 | src/storage/blocksstable/ob_storage_object_type.cpp:438 |
| 普通/私有对象 Opt | src/storage/blocksstable/ob_object_manager.h:23 |
| 共享对象 Opt | src/storage/blocksstable/ob_object_manager.h:72 |
| 对象分配入口 | src/storage/blocksstable/ob_object_manager.cpp:157 |
| 普通宏块字段来源 | src/storage/blocksstable/ob_macro_block_writer.cpp:2624 |
| 私有对象 ID 落位 | src/storage/blocksstable/ob_storage_object_type.cpp:571 |
| 共享 mini/minor/major ID 落位 | src/storage/blocksstable/ob_storage_object_type.cpp:979 |
| 增量 major 与 compaction ID 落位 | src/storage/blocksstable/ob_storage_object_type.cpp:4128 |
| 倒排索引宏块字段来源 | src/storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.cpp:1507 |
| 链式宏块信息来源 | src/storage/slog_ckpt/ob_linked_macro_block_writer.cpp:91 |
| 增量上传来源 | close_modules/shared_storage/storage/incremental/ob_copy_ss_macro_writer.cpp:46 |
| HA 恢复复制来源 | close_modules/shared_storage/storage/high_availability/ob_ss_storage_ha_writer.cpp:152 |
| 失败 GC 候选 ID 来源 | close_modules/shared_storage/storage/ss_garbage_collector/ob_ss_fail_gc_util.cpp:244 |
| 私有序列发号器 | src/storage/meta_store/ob_tenant_seq_generator.cpp:91 |
| 逻辑宏块 ID | src/storage/blocksstable/ob_logic_macro_id.h:130 |

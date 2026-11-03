# 共享 GC 诊断手册

> 本文档主要介绍共享数据 GC 的诊断方法，包括 Mark Sweep GC、FAIL GC、
> Lock Row GC、LS Meta GC 相关诊断虚拟表和 server event 的用法，以及如何排查
> 共享数据 GC 9120 问题。

## 1. 概述

Lakebase 的 observer 只支持 **Shared Storage（共享存储）** 模式，不支持 Shared Nothing。共享的宏块（macro block）与 tablet 元数据存放在对象存储中，主要通过 **Mark Sweep GC** 回收不再被引用的宏块与元数据；此外，对于上传失败或超时的共享数据，还会通过 **FAIL GC** 探测并删除对应宏块。

### 1.1 基本概念

| 概念 | 说明 |
|---|---|
| **租户级全局核对** | Mark Sweep GC 是租户级的，一次 Mark Sweep 即对该租户下**所有 tablet** 做一次全局标记-清除。 |
| **del / kept 集合** | 宏块迭代器把每个 tablet 的宏块分为「删除集合（del）」与「保留集合（kept）」，元数据同理分为 del / kept 的 `tablet_meta_scns`。**最终要删除的 = del 集合 − kept 集合**（在 del 中但不在 kept 中的部分）。 |
| **按轮执行（round）** | 每一轮 Mark Sweep 由一个 `round_id` 唯一标识。 |
| **执行者（executor）** | 同一租户的 Mark Sweep 通常由一个 server 执行（GC sswriter）。 |

### 1.2 诊断入口一览

| 入口 | 用途 | 详见 |
|---|---|---|
| `__all_virtual_shared_gc_task_history` | GC 任务历史：Mark Sweep 实际删除的宏块、FAIL GC 信息、FAIL GC 探测删除的宏块 | [§2](#2-诊断表一__all_virtual_shared_gc_task_history) |
| `__all_virtual_shared_gc_diagnose` | 某一轮 Mark Sweep GC 逐 tablet 的 del/kept macros 与 tablet_meta_scns 决策 | [§3](#3-诊断表二__all_virtual_shared_gc_diagnose) |
| `__all_server_event_history`（module=`SHARED_GC`） | Mark Sweep GC 的诊断信息、耗时、删除/保留宏块数；FAIL GC、Lock Row GC 与 LS Meta GC 的 DAG 生命周期和提交事件；共享 GC 任务执行 DAG 总览 | [§4](#4-server-event-诊断module--shared_gc) |
| `__all_virtual_shared_gc_control` | 当前哪个 server 在做 Mark Sweep、状态（IDLE/DOING）、GC 推进水位 | [§5](#5-控制表__all_virtual_shared_gc_control) |

---

## 2. 诊断表一：`__all_virtual_shared_gc_task_history`

**用途**：记录 GC 任务历史。`MARK_SWEEP_GC` 类型记录 Mark Sweep 实际删除的宏块；`FAIL_GC` 类型记录失败回收信息；`FAIL_GC_MACRO_ID` 类型记录 FAIL GC 探测并删除的宏块。该表数据来源于物理表 `__all_shared_gc_task_history`，为租户空间表。

定义见 [ob_inner_table_schema_def.py](../../src/share/inner_table/ob_inner_table_schema_def.py)，实现见 [ob_all_virtual_shared_gc_task.cpp](../../src/observer/virtual_table/ob_all_virtual_shared_gc_task.cpp)。

### 列说明

| 列名 | 类型 | rowkey | 说明 |
|---|---|---|---|
| `tenant_id` | int | 是 | 租户 ID |
| `type` | varchar(128) | 是 | 任务类型：`MARK_SWEEP_GC` / `FAIL_GC` / `FAIL_GC_MACRO_ID` |
| `round_id` | int | 是 | MARK_SWEEP_GC 轮次 ID；其余类型固定为 `0` |
| `task_id` | int | 是 | 任务 ID |
| `gmt_create` | timestamp | 否 | 创建时间 |
| `gmt_modified` | timestamp | 否 | 修改时间 |
| `value` | varchar(262144) | 否 | 任务载荷反序列化后的可读内容（见下方行展开规则） |
| `extra_info` | varbinary(16384) | 否 | 扩展信息；对于 `FAIL_GC_MACRO_ID` 类型，记录 `fail_gc_task_id=<源 FAIL_GC task_id>` |
| `server_id` | int | 否（可空） | 处理该任务的 server ID（未处理时设置为 -1） |
| `trace_id` | varchar(1024) | 否（可空） | 处理该任务的 trace id |

### 行展开规则

- `type = 'MARK_SWEEP_GC'`：载荷是一组待删除的宏块 ID（`ObSharedGCMacroIdList`），**一条物理任务展开为多行，每个 macro id 一行**，`value` 为该 MacroBlockId 的可读串（如 `[ver=1,mode=2,obj_type=22,obj_type_str=SHARED_MDS_MINI_DATA_MACRO,incar_id=0,cg_id=0][2nd=542][3rd=(source_type=UNKNOWN, op_id=0, macro_seq_id=0)][4th=(reorg_scn=0)`）。
- `type = 'FAIL_GC'`：载荷是失败信息（`ObSharedGCFailInfo`），**一条任务一行**，`value` 为可读的失败详情（meta_type、op_id、ls_id、tablet_id、transfer_scn 等）。
- `type = 'FAIL_GC_MACRO_ID'`：载荷与 `MARK_SWEEP_GC` 一样是一组宏块 ID（`ObSharedGCMacroIdList`），**一条物理历史记录展开为多行，每个 macro id 一行**，`value` 为该 MacroBlockId 的可读串。`extra_info = 'fail_gc_task_id=<源 FAIL_GC task_id>'` 用于关联源 `FAIL_GC` 任务。宏块列表过大时会拆成多条 `FAIL_GC_MACRO_ID` 历史行，排查时应按 `extra_info` 汇总。

### 示例 SQL

```sql
-- 查看某租户某一轮 Mark Sweep 实际删除的宏块
SELECT round_id, task_id, value, server_id, trace_id, gmt_create
FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = 1002 AND type = 'MARK_SWEEP_GC' AND round_id = 5;

-- 查看某租户的 FAIL_GC 任务
SELECT round_id, task_id, value, gmt_create
FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = 1002 AND type = 'FAIL_GC'
ORDER BY gmt_create DESC;

-- 查看某个 FAIL_GC 源任务探测并删除的宏块列表
-- 这里的 12345 是上一个查询返回的 FAIL_GC task_id
SELECT task_id AS macro_id_history_task_id, value AS macro_id,
       server_id, trace_id, gmt_create
FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = 1002 AND type = 'FAIL_GC_MACRO_ID'
  AND extra_info = 'fail_gc_task_id=12345'
ORDER BY macro_id_history_task_id, macro_id;

-- 按 macro id 反查是否被 FAIL_GC 删除
SELECT task_id AS macro_id_history_task_id, extra_info, value AS macro_id,
       gmt_create
FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = 1002 AND type = 'FAIL_GC_MACRO_ID'
  AND value LIKE '%<macro_id>%';
```

---

## 3. 诊断表二：`__all_virtual_shared_gc_diagnose`

**用途**：查看某一轮 Mark Sweep 的完整诊断审计——逐 tablet 的「删除/保留」决策。数据来源于对象存储的诊断文件，路径形如：

```
.../tenant_{id}/mark_sweep_gc_diag/round_{id}/{del|kept}/{macros|tablet_meta_scns}/...
```

定义见 [ob_inner_table_schema_def.py:18579-18600](../../src/share/inner_table/ob_inner_table_schema_def.py#L18579-L18600)，实现见 [ob_all_virtual_shared_gc_diagnose.cpp](../../src/observer/virtual_table/ob_all_virtual_shared_gc_diagnose.cpp)。

### 列说明

| 列名 | 类型 | rowkey | 说明 |
|---|---|---|---|
| `tenant_id` | int | 是 | 租户 ID |
| `round_id` | int | 是 | GC 轮次 ID（**查询必须指定，见下**） |
| `group` | varchar(128) | 是 | 分组：`del`（删除集合） / `kept`（保留集合） |
| `type` | varchar(128) | 是 | 文件类型：`macros`（宏块） / `tablet_meta_scns`（元数据 scn） |
| `ls_id` | int | 是 | 日志流 ID（可选过滤） |
| `tablet_id` | int | 是 | tablet ID（可选过滤） |
| `reorganization_scn` | int | 是 | transfer / 重组 scn（可选过滤） |
| `value` | varchar(1024) | 否 | 条目值：MacroBlockId 可读串 或 tablet_meta_scn 值 |

### 查询约束

- **`round_id` 为强制点谓词**：查询必须带 `round_id = <某值>` 的等值条件，否则无法读取对象存储诊断文件。
- **租户可见性**：sys 租户可指定 `tenant_id` 读取其他租户的诊断；普通租户只能读取自己的诊断（指定的 `tenant_id` 会被忽略）。
- `group`、`type`、`ls_id`、`tablet_id`、`reorganization_scn` 均为可选过滤条件。

### 示例 SQL

```sql
-- 查看某一轮删除集合中的宏块
SELECT ls_id, tablet_id, reorganization_scn, value
FROM __all_virtual_shared_gc_diagnose
WHERE tenant_id = 1002 AND round_id = 5
  AND `group` = 'del' AND type = 'macros';

-- 查看某一轮某 tablet 的保留 meta_scn 集合
SELECT ls_id, tablet_id, value
FROM __all_virtual_shared_gc_diagnose
WHERE tenant_id = 1002 AND round_id = 5
  AND `group` = 'kept' AND type = 'tablet_meta_scns'
  AND tablet_id = 200001;
```

---

## 4. Server Event 诊断（module = `SHARED_GC`）

共享 GC 的 Mark Sweep、FAIL GC、Lock Row GC、LS Meta GC 和 GC 任务执行 DAG
会写 server event 到 `__all_server_event_history`，module 统一为 `SHARED_GC`。

---

### 4.1 诊断信息是否写完整 —— `FINISH_MARK_SWEEP_DIAG`

代码见 [ob_mark_sweep_dag.cpp:521-527](../../close_modules/shared_storage/storage/garbage_collector/dag/ob_mark_sweep_dag.cpp#L521-L527)。

| name | 列 | 含义 |
|---|---|---|
| `tenant_id` | value1 | 租户 ID |
| `round_id` | value2 | 轮次 ID |
| `gc_meta_scn` | value3 | 本轮安全 meta scn |
| `has_diag_failed` | value4 | **0 = 诊断信息写完整；1 = 至少有一个诊断文件写失败** |
| `diag_first_fail_ret` | value5 | 第一个失败错误码（仅 `has_diag_failed=1` 时有意义） |
| `dag_ret` | value6 | DAG 执行结果码 |

```sql
-- 检查某轮诊断信息是否写完整（value4=0 表示完整）
SELECT gmt_create, svr_ip, svr_port,
       value2 AS round_id, value4 AS has_diag_failed,
       value5 AS diag_first_fail_ret, value6 AS dag_ret
FROM __all_server_event_history
WHERE module = 'SHARED_GC' AND event = 'FINISH_MARK_SWEEP_DIAG'
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

> 注意：诊断信息只有写完整（`has_diag_failed = 0`）时，上述第 3 节 `__all_virtual_shared_gc_diagnose` 的数据才是该轮的全量决策，才能够依赖该诊断表来排查问题；若为 1，则诊断表数据可能不全，不能够完全依赖该诊断表来排查问题。

---

### 4.2 各阶段耗时统计 —— `FINISH_MARK_SWEEP_GC` 的 `extra_info`

事件写入见 [ob_ss_mark_sweep_dag.cpp:797-808](../../close_modules/shared_storage/storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.cpp#L797-L808)。

| name | 列 | 含义 |
|---|---|---|
| `tenant_id` | value1 | 租户 ID |
| `round_id` | value2 | 轮次 ID |
| `gc_meta_scn` | value3 | 本轮安全 meta scn |
| `dag_ret` | value4 | DAG 结果码 |
| `dag_id` | value5 | DAG ID |
| `tablet_cnt` | value6 | 本轮处理的 tablet 总数 |
| （裸串）cost_stat | `extra_info` | 固定 10 字段摘要：阶段耗时、排序细分耗时和 kept meta scn 候选/计划数 |

`extra_info` 即 cost_stat，格式见 [ObSSMarkSweepCostStat::to_string](../../close_modules/shared_storage/storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.cpp#L143-L175)：

```text
{total_cost_us=..,sort_cost_us=..(meta_scn_scan_cost_us=..,replayed_scn_check_cost_us=..,macro_iter_and_sorter_cost_us=..,sorter_cost_us=..,kept_persist_cost_us=..,kept_persist_cnt=../..),merge_cost_us=..,diff_cost_us=..}
```

| 字段 | 含义 |
|---|---|
| `total_cost_us` | 整个 DAG 时间跨度（微秒），从 Prepare 开始到 `report_result` 报告结果 |
| `sort_cost_us` | 所有 SortTask 的最早开始时间到最晚结束时间的跨度 |
| `meta_scn_scan_cost_us` | 扫描 tablet meta scn 的调用耗时累计后归一化 |
| `replayed_scn_check_cost_us` | 读取 tablet 并检查 replayed scn 的调用耗时累计后归一化 |
| `macro_iter_and_sorter_cost_us` | 遍历 del/kept 宏块、加入 sorter 和收尾的调用耗时累计后归一化；包含 `sorter_cost_us` |
| `sorter_cost_us` | `add_item` 触发的排序/临时文件写出，加上 sorter `finish`、最终写出及文件登记的耗时累计后归一化；不包含初始化及普通 add 的分配/复制 |
| `kept_persist_cost_us` | `persist_kept_meta_scns_` 各次调用耗时累计后归一化，包含调用内过滤和重试，不包含两次调用之间的间隔 |
| `kept_persist_cnt` | `候选数/计划数`：计划数为生成的 tablet 位点计划数；候选数为与 Prepare 冻结的 Lock Row 位点比较后仍需推进的数目 |
| `merge_cost_us` | del/kept 两组所有有效 merge pass 的最早开始到最晚结束时间的跨度；无有效 pass 时为 `0` |
| `diff_cost_us` | diff 阶段耗时（生成 GC 任务、删除元数据） |

五项排序细分耗时（`meta_scn_scan_cost_us`、`replayed_scn_check_cost_us`、`macro_iter_and_sorter_cost_us`、`sorter_cost_us`、`kept_persist_cost_us`）均按 SortTask 调用累计，包含失败及重试调用的已计时部分，再除以 Prepare 冻结的有效并发额度（至少为 1，整数除法）。该额度用于统计归一化，并不表示实际同时运行的任务数；这些数值也不是各阶段独立的墙钟耗时。尤其 `macro_iter_and_sorter_cost_us` 包含 `sorter_cost_us`，不能将两者相加；前者减去后者可粗略估计宏块遍历中排序/文件写出以外的耗时。`sorter_cost_us` 同时包含排序和文件写出及相关等待，不是纯 I/O 写耗时。

`kept_persist_cnt` 的分子、分母按持久化调用累计，不按并发额度归一化。候选数不等于实际成功更新行数，候选/计划比也不能用于判断事务重试次数或重试放大。摘要不输出事务 attempt 计数或 parallelism 字段；merge 趟数与文件大小见 4.3。

```sql
-- 查看 1002 租户第 5 轮 Mark Sweep GC 的固定摘要
SELECT gmt_create, svr_ip, svr_port, value4 AS dag_ret,
       value6 AS tablet_cnt, extra_info AS cost_stat
FROM __all_server_event_history
WHERE module = 'SHARED_GC' AND event = 'FINISH_MARK_SWEEP_GC'
  AND value1 = '1002' AND value2 = '5'
ORDER BY gmt_create DESC;
```

---

### 4.3 Merge 趟数与临时文件大小 —— `MARK_SWEEP_MERGE_STAT`

merge 详情单独写入 `MARK_SWEEP_MERGE_STAT`，事件写入见 [ob_ss_mark_sweep_dag.cpp:809-818](../../close_modules/shared_storage/storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.cpp#L809-L818)。`value1` 至 `value6` 的含义与 4.2 相同，`extra_info` 由 [ObSSMarkSweepCostStat::to_merge_string](../../close_modules/shared_storage/storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.cpp#L177-L228) 输出：

```text
{del_passes=N[p0=(cost_us=..,files=..),p1=(cost_us=..,files=..),omitted_passes=M],kept_passes=N[p0=(cost_us=..,files=..),omitted_passes=M],merged_del_size=..,merged_kept_size=..}
```

| 字段 | 含义 |
|---|---|
| `del_passes=N[...]` | 删除集合有效 merge 趟数 N，及已输出的各趟详情 |
| `kept_passes=N[...]` | 保留集合有效 merge 趟数 N，及已输出的各趟详情 |
| `pX=(cost_us=..,files=..)` | 第 X 趟内 merge batch 的最早开始到最晚结束时间跨度，以及该趟开始时的输入临时文件数 |
| `omitted_passes=M` | 因 `extra_info` 容量不足而省略的有效 pass 数；仅有省略时输出，N 仍包含这些 pass |
| `merged_del_size` | 删除集合最终合并临时文件大小（字节），`-1` 表示大小未成功读取 |
| `merged_kept_size` | 保留集合最终合并临时文件大小（字节），`-1` 表示大小未成功读取 |

有效 pass 要求记录过开始时间、结束时间大于 0 且不早于开始时间。`extra_info` 缓冲区为 512 字节，输出时保留完整 pass 条目，并为两组的省略计数和末尾文件大小预留空间；出现 `omitted_passes` 表示事件没有列出全部 pass。两组 pass 可能并发，不能累加各趟耗时作为总 merge 耗时；总跨度读取 4.2 的 `merge_cost_us`。

```sql
-- 查看 1002 租户第 5 轮的 merge 详情和最终临时文件大小
SELECT gmt_create, svr_ip, svr_port, value2 AS round_id,
       value4 AS dag_ret, value5 AS dag_id, extra_info AS merge_stat
FROM __all_server_event_history
WHERE module = 'SHARED_GC' AND event = 'MARK_SWEEP_MERGE_STAT'
  AND value1 = '1002' AND value2 = '5'
ORDER BY gmt_create DESC;
```

如需排查被省略的 pass 或定位合并耗时，可在执行 server 的 observer 日志中按该 DAG 的 trace 与下列关键词查看运行过程：

- `finish to process ObSSMarkSweepMergePassCtrlTask`：查看 `group`、`pass_idx`、返回码和控制任务耗时。
- `finish to process ObSSMarkSweepMergeBatchTask`：查看 `group`、`pass_idx`、`batch_idx`、`total_batch_count`、返回码、耗时和 `retry_cnt`。
- `finish to process ObSSMarkSweepMergeCompleteTask`：查看合并收尾任务的返回码、耗时和重试数。

这些阶段日志可辅助定位批次失败、重试和调度等待；当前没有保证输出全部 pass 的 `files` 统计或最终文件大小的专用成功日志，不能用阶段日志替代完整的 merge 统计。

---

### 4.4 删除 / 保留宏块数 —— `FINISH_MARK_SWEEP_DIFF`

代码见 [ob_mark_sweep_dag.cpp:1911-1917](../../close_modules/shared_storage/storage/garbage_collector/dag/ob_mark_sweep_dag.cpp#L1911-L1917)。

| name | 列 | 含义 |
|---|---|---|
| `tenant_id` | value1 | 租户 ID |
| `round_id` | value2 | 轮次 ID |
| `gc_meta_scn` | value3 | 本轮安全 meta scn |
| `del_macro_cnt` | value4 | **删除的宏块数** |
| `kept_macro_cnt` | value5 | **保留的宏块数**（`-1` 表示读取失败） |
| `inserted_gc_task_rows` | value6 | 写入 `__all_shared_gc_task` 的任务行数 |

```sql
-- 查看 1002 租户第 5 轮 Mark Sweep GC 删除/保留宏块数
SELECT gmt_create, value4 AS del_macro_cnt, value5 AS kept_macro_cnt,
       value6 AS inserted_gc_task_rows
FROM __all_server_event_history
WHERE module = 'SHARED_GC' AND event = 'FINISH_MARK_SWEEP_DIFF'
  AND value1 = '1002' and value2 = '5'
ORDER BY gmt_create DESC;
```

---

### 4.5 FAIL GC 生成 DAG 事件

代码见 [ob_ss_gc_dag_task_base.cpp:186-200](../../src/storage/ss_garbage_collector/task/ob_ss_gc_dag_task_base.cpp#L186-L200) 和 [ob_ss_fail_gc_dag.cpp:580-635](../../src/storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.cpp#L580-L635)。

FAIL GC 生成已从 Timer 内同步逐 LS 执行改为 DAG 执行。原有的 per-LS
`START_FAILGC` / `FINISH_FAILGC` 不再产生，当前通过以下租户级 DAG 事件记录提交、开始和
结束状态：

| event | name | 列 | 含义 |
|---|---|---|---|
| `SUBMIT_FAIL_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `SUBMIT_FAIL_GC_DAG` | `schedule_ts` | value2 | 本次调度检查时间（微秒） |
| `SUBMIT_FAIL_GC_DAG` | `last_period_commit_time_us` | value3 | 已提交的调度周期锚点 |
| `SUBMIT_FAIL_GC_DAG` | `controller_state` | value4 | Timer DAG 调度状态枚举值 |
| `SUBMIT_FAIL_GC_DAG` | `trace_id` | value5 | trace id |
| `START_FAIL_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `START_FAIL_GC_DAG` | `schedule_ts` | value2 | 本次调度检查时间（微秒） |
| `START_FAIL_GC_DAG` | `dag_id` | value3 | DAG ID |
| `START_FAIL_GC_DAG` | `dag_start_time_us` | value4 | DAG 开始时间（微秒） |
| `START_FAIL_GC_DAG` | `trace_id` | value5 | trace id |
| `FINISH_FAIL_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `FINISH_FAIL_GC_DAG` | `schedule_ts` | value2 | 本次调度检查时间（微秒） |
| `FINISH_FAIL_GC_DAG` | `dag_ret` | value3 | DAG 返回码 |
| `FINISH_FAIL_GC_DAG` | `dag_status` | value4 | DAG 状态枚举值 |
| `FINISH_FAIL_GC_DAG` | `dag_id` | value5 | DAG ID |
| `FINISH_FAIL_GC_DAG` | `first_error_code` | value6 | 首个被吸收的 LS 业务错误码 |
| `FINISH_FAIL_GC_DAG` | （裸串）统计信息 | extra_info | LS 数、失败 LS 数、扫描/abandon 行数、生成任务数和 DAG 耗时 |

已经绑定 LS 的业务处理错误统一记录在 `first_error_code`，当前 LS 延后到下一轮
处理，不影响其他 LS。Task 初始化、Context 校验、DAG 建图等框架错误以及
stop/cancel 记录在 `dag_ret`。

```sql
-- 查看 1002 租户的 FAIL GC 生成 DAG 生命周期
SELECT gmt_create, event, value2 AS schedule_ts,
       value3, value4, value5, value6, extra_info
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event IN ('SUBMIT_FAIL_GC_DAG',
                'START_FAIL_GC_DAG',
                'FINISH_FAIL_GC_DAG')
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

这些事件不再提供单 LS 的 server-event 粒度。先通过
`FINISH_FAIL_GC_DAG.extra_info` 中的 `failed_ls_count` 判断是否存在 LS 级业务错误；需要
继续定位具体 LS 时，在对应 DAG 时间窗口内检索 `finish fail gc schedule task`、
`finish fail gc executor task` 日志中的 `ls_id` 和返回码。

---

### 4.6 Lock Row GC DAG 事件

当前生产 Timer 已收敛为 `ObSSLockRowGCTask` 的轻量控制面，周期到期后提交
`ObSSLockRowGCDag`，并通过租户级 SUBMIT/START/FINISH 事件诊断。旧同步执行函数已删除，
以下 per-LS 事件仅适用于切换前版本或滚动升级期间仍运行旧代码的 observer：

| event | name | 列 | 含义 |
|---|---|---|---|
| `START_LOCK_ROW_GC` | `tenant_id` | value1 | 租户 ID |
| `START_LOCK_ROW_GC` | `ls_id` | value2 | LS ID |
| `START_LOCK_ROW_GC` | `trace_id` | value3 | trace id |
| `FINISH_LOCK_ROW_GC` | `tenant_id` | value1 | 租户 ID |
| `FINISH_LOCK_ROW_GC` | `ls_id` | value2 | LS ID |
| `FINISH_LOCK_ROW_GC` | `ret` | value3 | 当前 LS 的执行结果 |
| `FINISH_LOCK_ROW_GC` | `cost_us` | value4 | 当前 LS 耗时（微秒） |
| `FINISH_LOCK_ROW_GC` | `trace_id` | value5 | trace id |

```sql
-- 查看切换前版本中 1002 租户的 per-LS Lock Row GC
SELECT gmt_create, event, value2 AS ls_id,
       value3, value4, value5
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event IN ('START_LOCK_ROW_GC', 'FINISH_LOCK_ROW_GC')
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

当前生产路径的提交事件定义见
[ob_ss_lock_row_gc_task.cpp](../../src/storage/ss_garbage_collector/task/ob_ss_lock_row_gc_task.cpp)，
DAG 生命周期事件定义见
[ob_ss_lock_row_gc_dag.cpp](../../src/storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.cpp)：

Lock Row GC DAG 使用以下租户级事件记录开始和结束状态：

| event | name | 列 | 含义 |
|---|---|---|---|
| `SUBMIT_LOCK_ROW_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `SUBMIT_LOCK_ROW_GC_DAG` | `schedule_ts` | value2 | 本次调度检查时间（微秒） |
| `SUBMIT_LOCK_ROW_GC_DAG` | `last_period_commit_time_us` | value3 | 已提交的调度周期锚点 |
| `SUBMIT_LOCK_ROW_GC_DAG` | `controller_state` | value4 | Timer DAG 调度状态枚举值 |
| `SUBMIT_LOCK_ROW_GC_DAG` | `trace_id` | value5 | trace id |
| `START_LOCK_ROW_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `START_LOCK_ROW_GC_DAG` | `schedule_ts` | value2 | 本次调度时间（微秒） |
| `START_LOCK_ROW_GC_DAG` | `dag_id` | value3 | DAG ID |
| `START_LOCK_ROW_GC_DAG` | `dag_start_time_us` | value4 | DAG 开始时间（微秒） |
| `START_LOCK_ROW_GC_DAG` | `trace_id` | value5 | trace id |
| `FINISH_LOCK_ROW_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `FINISH_LOCK_ROW_GC_DAG` | `schedule_ts` | value2 | 本次调度时间（微秒） |
| `FINISH_LOCK_ROW_GC_DAG` | `dag_ret` | value3 | DAG 返回码；框架错误或 stop/cancel 在这里体现 |
| `FINISH_LOCK_ROW_GC_DAG` | `dag_status` | value4 | DAG 状态枚举值 |
| `FINISH_LOCK_ROW_GC_DAG` | `dag_id` | value5 | DAG ID |
| `FINISH_LOCK_ROW_GC_DAG` | `first_error_code` | value6 | 首个被吸收的 LS 业务错误码 |
| `FINISH_LOCK_ROW_GC_DAG` | （裸串）统计信息 | extra_info | LS、扫描 Tablet、删除 Lock Row、失败 LS 和耗时统计 |

`FINISH_LOCK_ROW_GC_DAG.extra_info` 的格式为：

```
failed_ls_count=..,ls_count=..,scanned_tablet_count=..,deleted_tablet_lock_row_count=..,deleted_ls_lock_row_count=..,cost_us=..
```

| 字段 | 含义 |
|---|---|
| `failed_ls_count` | 出现业务错误的 LS 数；同一 LS 由首错 CAS 去重 |
| `ls_count` | 成功分配并纳入本次 DAG 的 LS Context 数 |
| `scanned_tablet_count` | Tablet iterator 成功返回的逻辑 Tablet 数；后续失败不回减 |
| `deleted_tablet_lock_row_count` | Tablet-prefix 删除事务成功提交后，实际受影响的 Tablet/SSTable Lock Row 物理行数 |
| `deleted_ls_lock_row_count` | LS Lock Row 精确删除事务成功提交且实际受影响的物理行数 |
| `cost_us` | DAG 总耗时（微秒） |

删除计数只统计事务提交已确认成功的实际 affected rows。并发删除导致的
`affected_rows=0`、回滚或 commit 失败均不计数。已经绑定 LS 的业务处理错误
统一记录在 `first_error_code`，当前 LS 延后到下一轮处理，不影响其他 LS。
Task 初始化、Context 校验、DAG 建图等框架错误以及 stop/cancel 记录在 `dag_ret`。

```sql
-- 查看 1002 租户的 Lock Row GC DAG 生命周期和最终统计
SELECT gmt_create, event, value2 AS schedule_ts,
       value3, value4, value5, value6, extra_info
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event IN ('SUBMIT_LOCK_ROW_GC_DAG',
                'START_LOCK_ROW_GC_DAG',
                'FINISH_LOCK_ROW_GC_DAG')
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

---

### 4.7 LS Meta GC DAG 事件

LS Meta GC 生产 Timer 已收敛为轻量控制面，通过以下租户级事件记录提交、
开始和结束状态：

| event | name | 列 | 含义 |
|---|---|---|---|
| `SUBMIT_LS_META_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `SUBMIT_LS_META_GC_DAG` | `schedule_ts` | value2 | 本次调度检查时间（微秒） |
| `SUBMIT_LS_META_GC_DAG` | `last_period_commit_time_us` | value3 | 已提交的调度周期锚点 |
| `SUBMIT_LS_META_GC_DAG` | `controller_state` | value4 | Timer DAG 调度状态枚举值 |
| `SUBMIT_LS_META_GC_DAG` | `trace_id` | value5 | trace id |
| `START_LS_META_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `START_LS_META_GC_DAG` | `schedule_ts` | value2 | 本次调度时间（微秒） |
| `START_LS_META_GC_DAG` | `dag_id` | value3 | DAG ID |
| `START_LS_META_GC_DAG` | `dag_start_time_us` | value4 | DAG 开始时间（微秒） |
| `START_LS_META_GC_DAG` | `trace_id` | value5 | trace id |
| `FINISH_LS_META_GC_DAG` | `tenant_id` | value1 | 租户 ID |
| `FINISH_LS_META_GC_DAG` | `schedule_ts` | value2 | 本次调度时间（微秒） |
| `FINISH_LS_META_GC_DAG` | `dag_ret` | value3 | DAG 返回码；框架错误或 stop/cancel 在这里体现 |
| `FINISH_LS_META_GC_DAG` | `dag_status` | value4 | DAG 状态枚举值 |
| `FINISH_LS_META_GC_DAG` | `dag_id` | value5 | DAG ID |
| `FINISH_LS_META_GC_DAG` | `first_error_code` | value6 | 首个被吸收的 LS 业务错误 |
| `FINISH_LS_META_GC_DAG` | （裸串）统计信息 | extra_info | LS、GC task、删除版本、完成 task、失败 LS 和耗时 |

`FINISH_LS_META_GC_DAG.extra_info` 的格式为：

```text
failed_ls_count=..,ls_count=..,gc_task_count=..,deleted_ls_meta_version_count=..,finished_gc_task_count=..,cost_us=..
```

`finished_gc_task_count` 记录成功完成 task delete/history 事务的任务数；LS 已不存在而
直接完成 task 也计入。已经进入单 LS 处理流程的错误统一记录在
`first_error_code`，当前 LS 延后到下一轮处理；Task 初始化、Context 校验、
DAG 建图等框架错误以及 stop/cancel 查看 `dag_ret`。

`SUBMIT` 只在 DAG 实际 add 成功时产生；`OB_EAGAIN` 后 probe 确认已有 DAG 不产生
该事件。`START_LS_META_GC_DAG` 在 PrepareTask 开始执行时产生，
`FINISH_LS_META_GC_DAG` 由 `report_result()` 产生。`START_LS_META_GC` /
`FINISH_LS_META_GC` 只是切换前同步路径的历史事件，当前代码不再产生。

```sql
-- 查看 1002 租户的 LS Meta GC DAG 生命周期和最终统计
SELECT gmt_create, event, value2 AS schedule_ts,
       value3, value4, value5, value6, extra_info
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event IN ('SUBMIT_LS_META_GC_DAG',
                'START_LS_META_GC_DAG',
                'FINISH_LS_META_GC_DAG')
  AND name1 = 'tenant_id'
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

---

### 4.8 执行 DAG 总览 —— `SS_GC_EXEC_DAG_FINISH`

代码见 [ob_ss_gc_exec_dag.cpp:228-234](../../close_modules/shared_storage/storage/garbage_collector/dag/ob_ss_gc_exec_dag.cpp#L228-L234)，`cost_stat` 格式见 [ob_ss_gc_exec_dag.cpp:64-69](../../close_modules/shared_storage/storage/garbage_collector/dag/ob_ss_gc_exec_dag.cpp#L64-L69)。

`SS_GC_EXEC_DAG_FINISH` 记录共享 GC 任务执行 DAG 的整体结果。`gc_type` 当前可为 `MARK_SWEEP_GC` 或 `FAIL_GC`；`FAIL_GC` 的 `allow_gc_round_id` 按代码约定为 `0`。

| name | 列 | 含义 |
|---|---|---|
| `tenant_id` | value1 | 租户 ID |
| `gc_type` | value2 | GC 任务类型：`MARK_SWEEP_GC` / `FAIL_GC` |
| `allow_gc_round_id` | value3 | Mark Sweep 可执行删除的最大轮次；`FAIL_GC` 固定为 `0` |
| `dag_ret` | value4 | DAG 结果码 |
| `dag_status` | value5 | DAG 状态枚举值 |
| `cost_stat` | value6 | 计划/执行耗时与行数统计 |

`cost_stat` 在 `value6` 中，不在 `extra_info` 中，格式为：

```
{total_cost_us=..,plan_cost_us=..,execute_cost_us=..,succ_row_count=..,failed_row_count=..}
```

| 字段 | 含义 |
|---|---|
| `total_cost_us` | 执行 DAG 总耗时（微秒） |
| `plan_cost_us` | Plan task 耗时（微秒） |
| `execute_cost_us` | Execute task 累计耗时（微秒） |
| `succ_row_count` | 成功处理并从 `__all_shared_gc_task` 转入历史表的任务行数 |
| `failed_row_count` | 执行失败或批量完成失败的任务行数 |

```sql
-- 查看某租户 Mark Sweep / FAIL GC 任务执行 DAG 的整体结果
SELECT gmt_create, value2 AS gc_type, value3 AS allow_gc_round_id,
       value4 AS dag_ret, value5 AS dag_status, value6 AS cost_stat
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event = 'SS_GC_EXEC_DAG_FINISH'
  AND value1 = '1002'
  AND value2 IN ('MARK_SWEEP_GC', 'FAIL_GC')
ORDER BY gmt_create DESC;
```

---

### 4.9 Mark Sweep 阶段失败事件速查

以下事件在对应 Mark Sweep 阶段失败时写入（module 均为 `SHARED_GC`），字段一般为 `tenant_id`(value1)、`round_id`(value2)、`gc_meta_scn`(value3)、`ret`、`dag_id`：

| 事件名 | 阶段 | 备注 |
|---|---|---|
| `START_MARK_SWEEP_GC` | 启动 | 新一轮 Mark Sweep 开始（含 tenant_id/round_id/gc_meta_scn） |
| `MARK_SWEEP_SORT_FAIL` | 排序 | 含失败 tablet 信息 |
| `MARK_SWEEP_MERGE_BATCH_FAIL` | 合并 | 临时文件合并批次失败 |
| `MARK_SWEEP_MERGE_COMPLETE_FAIL` | 合并完成 | 合并收尾失败 |
| `MARK_SWEEP_DIFF_FAIL` | diff | diff 阶段失败 |

```sql
-- 查看某租户所有 Mark Sweep 阶段失败事件
SELECT gmt_create, event, value2 AS round_id, value4, value5
FROM __all_server_event_history
WHERE module = 'SHARED_GC'
  AND event IN ('MARK_SWEEP_SORT_FAIL','MARK_SWEEP_MERGE_BATCH_FAIL',
                'MARK_SWEEP_MERGE_COMPLETE_FAIL','MARK_SWEEP_DIFF_FAIL')
  AND value1 = '1002'
ORDER BY gmt_create DESC;
```

> 注：上述失败事件里 `ret`、`dag_id` 落在 name4/name5（无 gc_meta_scn 的事件则前移），实际排查时可 `SELECT *` 后按 `name1..name6` 对照 `value1..value6`。

---

## 5. 控制表：`__all_virtual_shared_gc_control`

**用途**：查看当前由哪个 server 执行 Mark Sweep、当前状态、以及 GC 推进水位。虚拟表 TID 12612，物理表为 `__all_shared_gc_control`（TID 601，集群私有、租户空间表）。

定义见 [ob_inner_table_schema_def.py:9221-9242](../../src/share/inner_table/ob_inner_table_schema_def.py#L9221-L9242)，状态枚举见 [ob_shared_gc_control_define.h](../../close_modules/shared_storage/storage/garbage_collector/shared_gc_control/ob_shared_gc_control_define.h)。

### 列说明

| 列名 | 类型 | rowkey | 说明 |
|---|---|---|---|
| `tenant_id` | int | 是 | 租户 ID |
| `gmt_create` / `gmt_modified` | timestamp | 否 | 创建 / 修改时间 |
| `executor_server_id` | int | 否 | **当前执行 Mark Sweep 的 server ID** |
| `status` | varchar(128) | 否 | 状态：`IDLE` / `DOING` |
| `gc_round_id` | int | 否 | 当前 Mark Sweep GC 轮次 ID |
| `allow_gc_round_id` | int | 否 | 允许实际删除宏块数据的 Mark Sweep GC 的最大轮次 ID（单调递增） |
| `gc_meta_scn` | uint | 否 | 当前 Mark Sweep GC 轮次的 gc_meta_scn |
| `last_succ_gc_meta_scn` | uint | 否 | **最近一次成功完成的 gc_meta_scn（单调递增水位）** |

### 关键语义

- **当前在哪个 server 做 Mark Sweep**：`status = 'DOING'` 时，`executor_server_id` 即为当前执行的 server；`status = 'IDLE'` 时，表示当前无人在跑，任意 GC sswriter 都可抢占。
- **`last_succ_gc_meta_scn`**：只有 DAG 成功完成时才会通过 `update_last_succ_gc_meta_scn`（[ob_mark_sweep_dag.cpp:496-497](../../close_modules/shared_storage/storage/garbage_collector/dag/ob_mark_sweep_dag.cpp#L496-L497)）推进到本轮 `safe_meta_scn`。它是 GC 进度的单调水位，**严格不回退**；可用于判断 GC 是否真正推进、是否长期停滞。

### 示例 SQL

```sql
-- 查看各租户 GC 控制状态：谁在跑、跑到哪、推进水位
SELECT tenant_id, status, executor_server_id, gc_round_id,
       allow_gc_round_id, gc_meta_scn, last_succ_gc_meta_scn, gmt_modified
FROM __all_virtual_shared_gc_control;

-- 只看当前正在执行 Mark Sweep 的租户与 server
SELECT tenant_id, executor_server_id, gc_round_id, gc_meta_scn
FROM __all_virtual_shared_gc_control
WHERE status = 'DOING';
```

---

## 6. 典型排查流程

1. **看是否在跑 / 是否卡住**：查 `__all_virtual_shared_gc_control`。
   - `status = 'DOING'` 但 `gmt_modified` 长时间不变、`last_succ_gc_meta_scn` 长期不推进 → 可能卡住，结合 `executor_server_id` 定位执行 server，再查该 server 的阶段失败事件（4.9）。
   - `status = 'IDLE'` 且 `last_succ_gc_meta_scn` 不推进 → 可能没人抢占 / GC 未启用。
2. **定位任务 / 轮次**：从 control 的 `gc_round_id`、或 `__all_virtual_shared_gc_task_history` 找到相关 `MARK_SWEEP_GC` 的 `round_id`；若排查 FAIL GC，则先查 `FAIL_GC` 源任务，再用 `FAIL_GC_MACRO_ID.extra_info` 关联宏块明细。
3. **看本轮结果**：用 server event（module=`SHARED_GC`）：
   - 诊断是否完整 → `FINISH_MARK_SWEEP_DIAG`（4.1）；
   - 阶段耗时、排序细分耗时与 kept meta scn 候选/计划数 → `FINISH_MARK_SWEEP_GC` 的固定摘要（4.2）；
   - merge 趟数、每趟输入文件数与最终临时文件大小 → `MARK_SWEEP_MERGE_STAT`（4.3）。
   - 删除/保留宏块数 → `FINISH_MARK_SWEEP_DIFF`（4.4）。
   - FAIL GC 生成 DAG 是否成功 → `SUBMIT_FAIL_GC_DAG` / `START_FAIL_GC_DAG` /
     `FINISH_FAIL_GC_DAG`（4.5）；
   - Lock Row GC DAG 是否成功提交和执行 → `SUBMIT_LOCK_ROW_GC_DAG` /
     `START_LOCK_ROW_GC_DAG` / `FINISH_LOCK_ROW_GC_DAG`（4.6）；
   - LS Meta GC DAG 是否成功提交和执行 → `SUBMIT_LS_META_GC_DAG` /
     `START_LS_META_GC_DAG` / `FINISH_LS_META_GC_DAG`（4.7）；
   - Mark Sweep / FAIL GC 执行 DAG 总览 → `SS_GC_EXEC_DAG_FINISH`（4.8）。
4. **看明细决策**：Mark Sweep 用 `round_id` 查 `__all_virtual_shared_gc_diagnose`，逐 tablet 看 del/kept 的 macros 与 tablet_meta_scns；`FAIL_GC_MACRO_ID` 不适用该诊断表，应通过 `extra_info` 回查源 `FAIL_GC` 行，结合 `meta_type`、`op_id`、`ls_id`、`tablet_id`、`transfer_scn` 判断来源。

---

## 7. 案例：排查 9120（OB_OBJECT_NOT_EXIST）问题，确认是否被共享 GC 删除

当读取报 9120（对象不存在）疑似宏块丢失时，按以下步骤确认是否由 Mark Sweep GC 或 FAIL GC 删除（下文 `<macro_id>` 用 MacroBlockId 的可读串做 LIKE 模糊匹配，例如 `[ver=1,mode=2,obj_type=22,obj_type_str=SHARED_MDS_MINI_DATA_MACRO,incar_id=0,cg_id=0][2nd=542][3rd=(source_type=UNKNOWN, op_id=0, macro_seq_id=0)][4th=(reorg_scn=0)`：

**第 1 步**：在 `__all_virtual_shared_gc_task_history` 中按 macro id 反查是否被 Mark Sweep GC 或 FAIL GC 收录：

```sql
SELECT * FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = xxx
  AND type IN ('MARK_SWEEP_GC', 'FAIL_GC_MACRO_ID')
  AND value LIKE '%<macro_id>%';
```

若命中 `MARK_SWEEP_GC`，说明该宏块确为这一轮 Mark Sweep GC 删除，记录对应的 `round_id` 后继续第 2 步。若命中 `FAIL_GC_MACRO_ID`，说明该宏块由 FAIL GC 探测并删除，记录 `extra_info` 中的 `fail_gc_task_id` 后执行第 1.1 步；`FAIL_GC_MACRO_ID.round_id` 固定为 `0`，不要用它关联 Mark Sweep 轮次。

**第 1.1 步**：若第 1 步命中 `FAIL_GC_MACRO_ID`，通过 `extra_info` 回查源 `FAIL_GC` 任务：

```sql
-- 这里的 12345 来自 FAIL_GC_MACRO_ID.extra_info: fail_gc_task_id=12345
SELECT *
FROM __all_virtual_shared_gc_task_history
WHERE tenant_id = xxx AND type = 'FAIL_GC'
  AND task_id = 12345;
```

源 `FAIL_GC` 行的 `value` 中包含 `meta_type`、`op_id`、`ls_id`、`tablet_id`、`transfer_scn` 等信息，用于判断 FAIL GC 删除来源。FAIL GC 的宏块明细已经记录在 `FAIL_GC_MACRO_ID` 中，不适用 `__all_virtual_shared_gc_diagnose` 的 del/kept 进一步分析。

**第 2 步**：若第 1 步命中 `MARK_SWEEP_GC`，用该 `round_id` 在 `__all_virtual_shared_gc_diagnose` 中进一步确认该宏块是否落在该轮「删除集合」（宏块迭代器吐出的 del/macros）：

```sql
SELECT * FROM __all_virtual_shared_gc_diagnose
WHERE tenant_id = xxx AND round_id = xxx
  AND `group` = 'del' AND type = 'macros'
  AND value LIKE '%<macro_id>%';
```

若命中，则说明该宏块落在该轮「删除集合」。

**第 3 步**：若第 1 步命中 `MARK_SWEEP_GC`，用该 `round_id` 在 `__all_virtual_shared_gc_diagnose` 中进一步确认该宏块是否属于「最终要删除的宏块集合」：
```sql
select *
from (select distinct d.value
      from __all_virtual_shared_gc_diagnose d
      where d.tenant_id = xxx
        and d.round_id = xxx
        and d.`group` = 'del'
        and d.`type` = 'macros'
        and not exists (
          select 1
          from __all_virtual_shared_gc_diagnose k
          where k.tenant_id = d.tenant_id
            and k.round_id = d.round_id
            and k.`group` = 'kept'
            and k.`type` = 'macros'
            and k.value = d.value
        ) order by value) as del_macros
where value like '%<macro_id>%';
```
若命中，则说明 Mark Sweep GC 临时文件排序归并也没有问题，该宏块确实属于「最终要删除的宏块集合」。

**第 4 步**：若第 1 步命中 `MARK_SWEEP_GC`，进一步交叉验证该 tablet 的 meta_scn 决策——查该 `round_id` 下该 tablet 的 del 与 kept 的 `tablet_meta_scns` 集合，确认保留/删除的 meta_scn 是否符合预期（需要具体问题，具体分析）：

```sql
SELECT * FROM __all_virtual_shared_gc_diagnose
WHERE tenant_id = xxx AND round_id = xxx AND type = 'tablet_meta_scns'
  AND ls_id = xxx and tablet_id = xxx and reorganization_scn = xxx;
```

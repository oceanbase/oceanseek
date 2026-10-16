# OceanBase 4.6.0 Transfer 流程完整梳理

> 基于 v4.6.0.0 源码（2026-04），整合自 workmd/transfer/ 下 18 份专项分析文档。
> 代码行号基于 2026-04 版本，可能随后续提交变化。

---

## 第一章 概述与基础概念

### 1.1 Transfer 是什么

OceanBase 的 Tablet Transfer 是将一组 tablet 从源 LS（Log Stream）迁移到目标 LS 的分布式操作。整个流程通过 MDS（Multi Data Source）框架的 BufferCtx 回调机制保证事务一致性。

Transfer 是 Balance 框架的核心能力之一，用于实现 LS 间的数据均衡（TRANSFER 类型任务）、LS 分裂（SPLIT 类型任务配合）、LS 合并（MERGE 类型任务配合）等场景。

### 1.2 新旧流程对比

存在新旧两套流程，由 `enable_new_transfer()` 判断：

| 维度 | 旧流程 (new_transfer=false) | 新流程 (new_transfer=true, 当前主要路径) |
|------|---------------------------|---------------------------------------|
| 源侧转出 MDS | START_TRANSFER_OUT (ID=20) | START_TRANSFER_OUT_PREPARE (ID=25) + START_TRANSFER_OUT_V2 (ID=26) |
| 事务处理方式 | block_and_kill_tx 直接阻塞并杀死活跃事务 | 两阶段：先阻止新写入(PREPARE)，再阻塞活跃事务(V2)，最后搬迁事务上下文(MOVE_TX_CTX) |
| 目标侧准备 | 无 | TRANSFER_DEST_PREPARE (ID=28) 冻结弱读推进 |
| 事务搬迁 | 无（事务被直接杀死） | TRANSFER_MOVE_TX_CTX (ID=27) 搬迁事务上下文到目标 LS |
| 优势 | 实现简单 | 事务不中断，对业务影响小 |

**本文以新流程为主线。**

### 1.3 核心术语表

| 术语 | 说明 |
|------|------|
| **transfer_scn** | Transfer 操作的分界 SCN。由 START_TRANSFER_OUT_V2 的 redo 日志 SCN 确定。也称 `start_scn`（部分代码中使用 `transfer_start_scn_`）。是弱读保护和 Backfill 数据边界的核心标记。 |
| **data_end_scn** | 源 tablet 的数据截止 SCN。在 `wait_tablet_write_end_()` 中获取，表示源 tablet 最后一笔写入的时间点。 |
| **mds_base_scn** | MDS 基准 SCN，用于确定 MDS 快照的起始点。 |
| **Backfill（回填）** | DOING 阶段的后台任务，将 transfer_scn 之前的历史数据从源 LS 拷贝到目标 LS 的过程。 |
| **BufferCtx 回调** | MDS 框架中注册的回调机制，包括 on_register、on_replay、on_redo、on_commit、on_abort 等阶段。 |
| **Replay Barrier** | Follower 回放日志时的屏障机制，保证 MDS 操作的回放顺序正确。分 PRE_BARRIER（先 drain 再回放）和 STRICT_BARRIER（完全串行化）两级。 |
| **transfer_blocking** | 源 LS 上活跃事务的阻塞标记，由 START_TRANSFER_OUT_V2 的 on_register 设置，ON_COMMIT/ON_ABORT 时清除。 |
| **ObLSTransferStatus** | 目标 LS 上的 Transfer 状态管理类，控制弱读时间戳推进的禁用/恢复。 |
| **empty_shell** | Transfer abort 后残留的空 tablet 壳，由 GC 机制异步清理。 |

### 1.4 MDS 类型注册总表

文件: `src/storage/multi_data_source/compile_utility/mds_register.h`

| ID | 枚举名 | Helper 类 | BufferCtx 类 | 注册目标 | 阶段 | 说明 |
|----|--------|-----------|-------------|---------|------|------|
| 20 | START_TRANSFER_OUT | ObTabletStartTransferOutHelper | MdsCtx | 源 LS | START | 旧版转出 |
| 21 | START_TRANSFER_IN | ObTabletStartTransferInHelper | **ObStartTransferInMdsCtx** | 目标 LS | START | 目标侧创建 tablet |
| 22 | FINISH_TRANSFER_OUT | ObTabletFinishTransferOutHelper | MdsCtx | 源 LS | DOING | 源侧标记完成 |
| 23 | FINISH_TRANSFER_IN | ObTabletFinishTransferInHelper | **ObFinishTransferInMdsCtx** | 目标 LS | DOING | 目标侧恢复 NORMAL |
| 24 | TRANSFER_TASK | ObBalanceTaskMDSHelper | MdsCtx | SYS_LS | START | 集群级元数据同步 |
| 25 | START_TRANSFER_OUT_PREPARE | ObTabletStartTransferOutPrepareHelper | MdsCtx | 源 LS | START | 新版两阶段第一步 |
| 26 | START_TRANSFER_OUT_V2 | ObTabletStartTransferOutV2Helper | **ObTransferOutTxCtx** | 源 LS | START | 新版两阶段第二步 |
| 27 | TRANSFER_MOVE_TX_CTX | ObStartTransferMoveTxHelper | **ObTransferMoveTxCtx** | 目标 LS | START | 事务上下文搬迁 |
| 28 | TRANSFER_DEST_PREPARE | ObStartTransferDestPrepareHelper | **ObTransferDestPrepareTxCtx** | 目标 LS | START | 冻结弱读推进 |
| 32 | TRANSFER_IN_ABORTED | ObTabletAbortTransferHelper | **ObAbortTransferInMdsCtx** | 目标 LS | ABORT | 转入中止清理 |

**加粗**的 BufferCtx 类为自定义子类（非通用 MdsCtx）。

### 1.5 关键文件索引

| 文件 | 用途 |
|------|------|
| `src/storage/high_availability/ob_transfer_handler.cpp` | Transfer 主状态机（START/DOING/ABORT） |
| `src/storage/high_availability/ob_transfer_service.cpp` | Transfer 调度服务（5分钟轮询） |
| `src/storage/high_availability/ob_finish_transfer.cpp` | DOING 阶段处理 |
| `src/storage/high_availability/ob_transfer_struct.h` | Transfer 数据结构定义 |
| `src/storage/tablet/ob_tablet_transfer_tx_ctx.h/cpp` | TransferOut/MoveTx/DestPrepare 三个 Ctx |
| `src/storage/tablet/ob_tablet_start_transfer_mds_helper.cpp` | StartTransferIn/Out/Prepare Helper |
| `src/storage/tablet/ob_tablet_finish_transfer_mds_helper.cpp` | FinishTransferIn/Out Helper |
| `src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h/cpp` | StartTransferIn Ctx |
| `src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h/cpp` | FinishTransferIn Ctx |
| `src/storage/multi_data_source/ob_abort_transfer_in_mds_ctx.h/cpp` | AbortTransferIn Ctx |
| `src/storage/ls/ob_ls_transfer_status.h/cpp` | Transfer Status（弱读控制） |
| `src/storage/tx_table/ob_tx_table.h/cpp` | TxTable（enable/disable弱读推进） |
| `src/storage/tx/ob_trans_part_ctx.cpp` | 事务上下文（do_transfer_out_tx_op, move_tx_op） |
| `src/rootserver/balance/ob_balance_task_execute_service.cpp` | Balance Task 执行（LS 创建/删除） |
| `src/share/balance/ob_balance_task_table_operator.cpp` | Balance Task 内部表操作 |

---

## 第二章 Transfer 整体流程

### 2.1 入口和调度

```
ObTransferService::run1()                    # daemon thread, 5-min polling
  -> scheduler_transfer_handler_(ls_id)
    -> ObTransferHandler::process()           # handler per LS
      -> do_leader_transfer_()
        -> get_transfer_task_()               # get transfer task from inner table
        -> dispatch by task status:
          +-- STATUS::START   -> do_with_start_status_()
          +-- STATUS::DOING   -> do_with_doing_status_()
          +-- STATUS::ABORTED -> do_with_aborted_status_()
```

### 2.2 START 阶段

START 阶段是 Transfer 的核心，在源 LS 的 Leader 上执行，一个多源 MDS 事务内完成全部注册操作。

代码入口: `ob_transfer_handler.cpp:446 do_with_start_status_()`

#### 2.2.1 初始化与前置检查（步骤 1-15）

```
do_with_start_status_()
  +-- 1.  enable_new_transfer()                   # check if new transfer flow enabled
  +-- 2.  init_transfer_ls_info_()                # init src/dest LS info
  +-- 3.  build_transfer_tablet_info()            # build tablet list to transfer
  +-- 4.  get_gts()                               # get GTS sequence
  +-- 5.  start_trans_()                          # start db transaction
  +-- 6.  lock_transfer_task_()                   # lock transfer task (prevent concurrent)
  +-- 7.  lock_src_and_dest_ls_member_list()      # lock member list of both LS
  +-- 8.  get_config_version_()                   # get PALF config version
  +-- 9.  check_self_is_leader_()                 # confirm self is leader
  +-- 10. precheck_ls_replay_scn_()               # pre-check LS replay progress
  +-- 11. stop_tablets_schedule_medium_()          # stop tablet medium compaction schedule
  +-- 12. check_start_status_transfer_tablets_()   # check transfer tablet status
  +-- 13. build_storage_schema_info()              # build storage schema info
  +-- 14. update_all_tablet_to_ls_()               # update tablet-to-ls mapping (inner table)
  +-- 15. lock_tablet_on_dest_ls_for_table_lock_() # lock table on dest LS
```

#### 2.2.2 PREPARE 子阶段（步骤 16，新流程独有）

```
  +-- 16. do_trans_transfer_start_prepare_()
      +-- do_trans_transfer_dest_prepare_()
          +-- register TRANSFER_DEST_PREPARE (ID=28) on dest LS
              -> create ObTransferDestPrepareTxCtx
              -> Helper::on_register() disable dest LS weak-read ts advance
```

**关键作用**: TRANSFER_DEST_PREPARE 是 START 阶段最先注册到目标 LS 的 MDS 操作。它通过 `disable_upper_trans_calculation_()` 冻结目标 LS 的弱读时间戳推进，防止在后续事务搬迁期间读到不一致的中间状态。（详见 [3.1](#31-transfer_dest_prepare-id28--目标侧弱读冻结) 和 [4.2](#42-弱读控制机制)）

#### 2.2.3 V2 主流程（步骤 17）

```
  +-- 17. do_trans_transfer_start_v2_()            # ob_transfer_handler.cpp:1235
      +-- a. register START_TRANSFER_OUT_PREPARE (ID=25)
      |      -> block new writes on src tablets (see 3.2)
      +-- b. wait_tablet_write_end_()
      |      -> wait tablet write end, get data_end_scn
      +-- c. filter_tx_need_transfer()
      |      -> select active txns to transfer
      +-- d. register START_TRANSFER_OUT_V2 (ID=26)
      |      -> create ObTransferOutTxCtx, call record_transfer_block_op()
      |      -> block active tx writes (see 3.3)
      +-- e. get_start_transfer_out_scn_()
      |      -> get transfer_scn (see 4.3)
      +-- f. wait_src_ls_replay_to_start_scn_()
      |      -> wait src LS replay to transfer_scn
      +-- g. parallel_get_transfer_tablets_meta_()
      |      -> parallel get tablet meta
      +-- h. do_move_tx_to_dest_ls_()
      |      -> batch register TRANSFER_MOVE_TX_CTX (ID=27), 2000 txns per batch
      |      -> create tx ctx on dest LS (see 3.4)
      +-- i. do_tx_start_transfer_in_()
      |      -> register START_TRANSFER_IN (ID=21)
      |      -> create transfer tablets on dest LS (see 3.5)
      +-- j. update_transfer_status_(DOING)
```

**两阶段转出设计**: PREPARE（步骤 a）是"温和"阶段，仅阻止新写入；V2（步骤 d）是"强硬"阶段，阻塞活跃事务。两阶段之间插入等待写入结束和事务筛选步骤，实现精细化控制。

#### 2.2.4 事务提交与回调触发（步骤 18-19）

```
  +-- 18. commit_trans_()
  |      -> commit multi-source MDS transaction
  |      -> trigger all BufferCtx callbacks (on_redo -> on_commit)
  |      -> see Chapter 3 for callback details
  +-- 19. clear_prohibit_()
         -> clear blocking flags
```

### 2.3 DOING 阶段

在目标 LS Leader 上执行。代码入口: `ob_finish_transfer.cpp:92 do_tx_transfer_doing_()`

```
do_with_doing_status_()
  -> ObTxFinishTransfer::process()
    -> do_tx_transfer_doing_()
      |
      | --- pre-tx operations ---
      +-- 1.  unlock_src_and_dest_ls_member_list()       # unlock first (allow config changes)
      +-- 2.  get_transfer_tablet_info_from_inner_table_() # get tablet list
      +-- 3.  check_ls_logical_table_replaced()           # pre-check backfill done
      |
      | --- in-tx operations ---
      +-- 4.  start_trans_()                              # start transaction
      +-- 5.  select_transfer_task_for_update_()          # lock transfer task
      +-- 6.  lock_ls_member_and_learner_list_()          # re-lock member list
      +-- 7.  check_ls_logical_table_replaced()           # re-confirm backfill done
      +-- 8.  do_tx_finish_transfer_in_()                 # register FINISH_TRANSFER_IN (ID=23) (see 3.6)
      +-- 9.  wait_transfer_tablet_status_normal_()       # wait tablet status NORMAL
      +-- 10. wait_all_ls_replica_replay_scn_()           # wait all replica replay done
      +-- 11. do_tx_finish_transfer_out_()                # register FINISH_TRANSFER_OUT (ID=22) (see 3.7)
      +-- 12. update_transfer_task_result_(COMPLETED)     # update task status
      +-- 13. unlock_tablet_on_src_ls_for_table_lock()    # unlock src LS table lock
      +-- 14. unlock_ls_member_and_learner_list_()        # unlock member list
      +-- 15. commit_trans_()                             # commit transaction
```

**注意**: 步骤 1 先解锁成员列表（允许配置变更），步骤 6 再加锁。步骤 8 (FINISH_TRANSFER_IN) 在步骤 9 (wait status normal) 之前执行。

### 2.4 ABORTED 阶段

在 START 事务中止后执行。代码入口: `ob_transfer_handler.cpp:2415`

存在版本区分（>= 4.2.3 vs < 4.2.3）：

```
do_with_aborted_status_()
  -> inner_do_with_abort_status_()            # >= 4.2.3 version
    +-- 1. unlock_src_and_dest_ls_member_list()  # unlock member list
    +-- 2. start_trans_()                        # start transaction
    +-- 3. lock_transfer_task_()                 # lock transfer task
    +-- 4. do_trans_transfer_aborted_()          # register TRANSFER_IN_ABORTED (ID=32) (see 3.8)
    +-- 5. update_transfer_task_result_()        # update task status
    +-- 6. commit_trans_()                       # commit transaction
```

### 2.5 完整时序图（新流程）

```
       src LS                                                dest LS
          |                                                      |
  ========+================= START phase =======================+=======
          |                                                      |
  init: lock member_list, stop medium                            |
  build schema, update tablet-to-ls mapping                      |
          |                                                      |
  -- PREPARE sub-phase -------------------------------------------+------
          |                                                      |
  1. register TRANSFER_DEST_PREPARE (ID=28) -------------------> |
     Helper::on_register()                                       |
       -> update_status(REGISTER_SUCC) --> disable weak-read ts advance
          |                                                      |
  -- V2 main flow ------------------------------------------------+------
          |                                                      |
  2. register START_TRANSFER_OUT_PREPARE (ID=25)                 |
     -> tablet_status: NORMAL -> TRANSFER_OUT                    |
     -> block new writes on src tablets                          |
          |                                                      |
  3. wait_tablet_write_end_() -> get data_end_scn               |
          |                                                      |
  4. filter_tx_need_transfer() -> select active txns             |
          |                                                      |
  5. register START_TRANSFER_OUT_V2 (ID=26)                      |
     -> record_transfer_block_op() -> set transfer_blocking      |
          |                                                      |
  6. wait redo SCN replay done -> determine transfer_scn         |
          |                                                      |
  7. parallel get tablet meta                                    |
          |                                                      |
  8. batch register TRANSFER_MOVE_TX_CTX (ID=27) --------------> |
     Helper::on_register()                                       |
       -> move_tx_op(REGISTER_SUCC) --> create tx ctx on dest LS
          |                                                      |
  9. register START_TRANSFER_IN (ID=21) ------------------------>|
     Helper::on_register_success_()                              |
       -> create transfer tablet per tablet --> visible on dest LS
          |                                                      |
  10. update_transfer_status_(DOING)                             |
          |                                                      |
  -- commit tx ----------------------------------------------------+------
          |                                                      |
  11. commit_trans_() -> trigger all BufferCtx callbacks:         |
      |                                                          |
      | ObTransferOutTxCtx:                                      |
      |   on_redo  -> transfer_out_tx_op(ON_REDO)                |
      |   on_commit-> transfer_out_tx_op(ON_COMMIT)              |
      |                                                          |
      | ObTransferMoveTxCtx:                                     |
      |   on_redo  -> move_tx_op(ON_REDO)                        |
      |   on_commit-> move_tx_op(ON_COMMIT) -> clear blocking    |
      |                                                          |
      | ObTransferDestPrepareTxCtx:                              |
      |   on_commit-> update_status(ON_COMMIT)                   |
      |           -> restore weak-read ts advance on dest LS     |
      |                                                          |
      | ObStartTransferInMdsCtx:                                 |
      |   on_redo  -> persist MDS node                           |
      |   on_commit-> MDS node visible                           |
          |                                                      |
  ========+================= DOING phase =======================+=======
          |                                                      |
          |                                  1. unlock member_list
          |                                  2. Backfill: copy data from src LS
          |                                  3. pre-check + in-tx confirm backfill done
          |                                  4. register FINISH_TRANSFER_IN (ID=23)
          |                                     -> tablet_status: TRANSFER_IN -> NORMAL
          |                                  5. wait tablet status NORMAL
          |                                  6. wait all replica replay done
          |                                                      |
  7. register FINISH_TRANSFER_OUT (ID=22) <--------------------  |
     -> tablet_status: TRANSFER_OUT -> TRANSFER_OUT_DELETED      |
          |                                  8. update task COMPLETED
          |                                  9. commit_trans_()
          |                                                      |
  ========+================= COMPLETED =========================+=======
          |                                                      |
  ========+================= ABORT path ========================+=======
          |                                                      |
  if START tx aborted:                                           |
    ObTransferOutTxCtx::on_abort                                 |
      -> transfer_out_tx_op(ON_ABORT) -> restore tx normal state |
    ObTransferMoveTxCtx::on_abort                                |
      -> move_tx_op(ON_ABORT) ----------------------> delete tx ctx
    ObTransferDestPrepareTxCtx::on_abort                         |
      -> update_status(ON_ABORT) --> restore weak-read advance   |
    ObStartTransferInMdsCtx::on_abort                            |
      -> set empty_shell_trigger ---------------------> GC cleanup
          |                                                      |
  if STATUS=ABORTED:                                             |
    do_with_aborted_status_()                                    |
      -> register TRANSFER_IN_ABORTED (ID=32) ---------> verify GC done
      -> cleanup transfer_meta_info                              |
```


---

## 第三章 各 MDS 阶段深入分析

> 每节统一结构：定位 -> MDS 注册数据 -> Leader 调用栈 -> Follower 差异 -> BufferCtx 回调。

### 3.1 TRANSFER_DEST_PREPARE (ID=28) -- 目标侧弱读冻结

**定位**: START 阶段**最先注册**的 MDS 操作，注册到**目标 LS**。核心作用是在事务搬迁期间冻结目标 LS 的弱读时间戳推进，防止读到不一致的中间状态数据。

**注册数据**: ObTransferDestPrepareInfo（最小化信息）
```
task_id_     : int64_t    transfer task ID
src_ls_id_   : ObLSID     source LS ID
dest_ls_id_  : ObLSID     dest LS ID
```

**BufferCtx**: ObTransferDestPrepareTxCtx（直接继承 BufferCtx，不经过 MdsCtx）

**Leader 调用栈 (on_register)**:
```
do_trans_transfer_dest_prepare_()                         // ob_transfer_handler.cpp:2883
  -> conn->register_multi_data_source(TRANSFER_DEST_PREPARE)
    -> ObStartTransferDestPrepareHelper::on_register()    // ob_tablet_transfer_tx_ctx.cpp:624
       1. deserialize ObTXStartTransferInInfo
       2. ls->get_transfer_status().update_status(REGISTER_SUCC, TRANSFER_DEST_PREPARE)
          -> update_status_inner_()
            -> transfer_prepare_op_ = true
            -> disable_upper_trans_calculation_()
              -> tx_table->disable_upper_trans_calculation()  // freeze weak-read
```

**Follower 调用栈 (on_replay)**:
```
CLOG replay -> ObTxReplayExecutor::execute()
  -> ObStartTransferDestPrepareHelper::on_replay()        // ob_tablet_transfer_tx_ctx.cpp:655
     1. deserialize ObTXStartTransferInInfo
     2. ls->get_transfer_status().update_status(REGISTER_SUCC, TRANSFER_DEST_PREPARE, scn)
        -> replay_status_inner_()                         // filter duplicate replay by SCN
          -> disable_upper_trans_calculation_()            // freeze weak-read too
```

**BufferCtx 回调**:

| 回调 | 行号 | 行为 |
|------|------|------|
| on_redo | 718 | update_status(ON_REDO, redo_scn) |
| on_commit | 748 | update_status(ON_COMMIT) -> enable_upper_trans_calculation_() -> **恢复弱读推进** |
| on_abort | 776 | update_status(ON_ABORT) -> enable_upper_trans_calculation_() -> **恢复弱读推进** |

所有回调内部都是 while(true) + retry 模式，失败后每 10ms 重试。

**RPC 接口**: `get_transfer_dest_prepare_scn` -- 源 LS 调用此 RPC 查询目标 LS 是否已进入 PREPARE 状态及其 prepare_scn，用于 `wait_src_ls_advance_weak_read_ts_()` 中的跨 LS 协调。

**Replay Barrier**: NO_NEED_BARRIER（不涉及 tablet 级别数据变更）。

---

### 3.2 START_TRANSFER_OUT_PREPARE (ID=25) -- 源侧停写准备

**定位**: V2 流程"两阶段转出"的**第一阶段**，注册到**源 LS**。核心作用是将源 LS 上待迁移的 tablet 标记为 TRANSFER_OUT 状态，阻止新写入，但不阻塞活跃事务。

**注册数据**: ObTXStartTransferOutInfo（与 V2 共用结构，但 data_end_scn 为空、move_tx_ids 为空）
```
src_ls_id_, dest_ls_id_, tablet_list_, task_id_, transfer_epoch_
data_end_scn_ = empty (not yet obtained in PREPARE phase)
filter_tx_need_transfer_ = false
move_tx_ids_ = empty
```

**BufferCtx**: 通用 MdsCtx（不需要自定义回调，因为 PREPARE 不处理事务阻塞逻辑）。

**Leader 调用栈 (on_register)**:
```
do_tx_start_transfer_out_(START_TRANSFER_OUT_PREPARE)
  -> ObTabletStartTransferOutPrepareHelper::on_register()
    -> update_tablets_transfer_out_(info, ls, ctx)
      -> iterate tablet_list, for each tablet:
         1. get current tablet status (should be NORMAL, committed)
         2. modify MDS data:
            tablet_status_   = TRANSFER_OUT
            data_type_       = START_TRANSFER_OUT_PREPARE
            transfer_ls_id_  = dest_ls_id
            transfer_scn_    = SCN::min_scn()  // undetermined now, updated by V2 on_redo
         3. write to MDS table (uncommitted state)
```

**Follower 差异**:
- on_replay 路径不执行 `try_enable_dest_ls_clog_replay()` 和 `set_transfer_tablets_freeze_flag_()`（这些操作留给 V2 阶段）
- 通过 ObTabletStartTransferOutReplayExecutor 处理重放

**两阶段设计**:
```
PREPARE (soft): block new writes -> give active txns time to finish
    |
    v  wait_tablet_write_end_() -> data_end_scn
    v  filter_tx_need_transfer() -> move_tx_ids
    |
V2 (hard):      block active txns -> formal transfer out
```

**Replay Barrier**: PRE_BARRIER（redo 日志）。

---

### 3.3 START_TRANSFER_OUT_V2 (ID=26) -- 源侧正式转出与事务阻塞

**定位**: V2 流程"两阶段转出"的**第二阶段**，注册到**源 LS**。核心作用是阻塞活跃事务写入，更新 tablet 的 data_type，并通过 BufferCtx 回调管理事务的阻塞/解阻塞。

**注册数据**: ObTXStartTransferOutInfo（完整填充）
```
src_ls_id_, dest_ls_id_, tablet_list_, task_id_, transfer_epoch_
data_end_scn_              = write cutoff SCN (from step b)
filter_tx_need_transfer_   = true
move_tx_ids_               = tx ID list to transfer (from step c)
```

**BufferCtx**: ObTransferOutTxCtx（继承自 MdsCtx）

ObTransferOutTxCtx members:
```
do_transfer_block_       : bool              whether transfer block started
src_ls_id_               : ObLSID            source LS
dest_ls_id_              : ObLSID            dest LS
data_end_scn_            : SCN               data cutoff point
transfer_scn_            : SCN               transfer SCN (set on on_redo)
transfer_epoch_          : int64_t           transfer epoch
filter_tx_need_transfer_ : bool              whether tx transfer needed
move_tx_ids_             : ObSEArray<ObTransID> tx ID list to transfer
```

**Leader 调用栈 (on_register)**:
```
do_tx_start_transfer_out_(START_TRANSFER_OUT_V2)
  -> ObTabletStartTransferOutV2Helper::on_register()      // ob_tablet_start_transfer_mds_helper.cpp:788
     1. record_transfer_block_op() -> record tx blocking params
     2. ls->transfer_out_tx_op(REGISTER_SUCC)
        -> ObPartTransCtx::do_transfer_out_tx_op()
          -> sub_state_.set_transfer_blocking()            // block active txns
     3. update_tablets_transfer_out_(info, ls, ctx)
        -> pre-validation:
           tablet_status_ == TRANSFER_OUT                  (set by PREPARE)
           data_type_ == START_TRANSFER_OUT_PREPARE        (confirm PREPARE done)
           trans_stat != ON_COMMIT                         (PREPARE MDS node not committed)
        -> modify MDS data:
           data_type_ = START_TRANSFER_OUT                 // key change: PREPARE -> formal
           tablet_status_, transfer_ls_id_ unchanged
```

**Follower 差异**:
- on_replay 路径额外执行 `try_enable_dest_ls_clog_replay()` 和 `set_transfer_tablets_freeze_flag_()`
- 重放时不校验 tablet_status（因 MDS 事务回滚后重启时可能从中间开始重放）

**BufferCtx 回调**:

| 回调 | 行号 | 行为 |
|------|------|------|
| on_redo | 164 | MdsCtx::on_redo() -> transfer_scn_ = redo_scn -> transfer_out_tx_op(ON_REDO): set_transfer_blocking, transfer_op_log_cb_ |
| on_commit | 209 | MdsCtx::on_commit() -> transfer_out_tx_op(ON_COMMIT): add_intermediate_participants(dest_ls), clear_transfer_blocking, transfer_op_log_cb_ |
| on_abort | 262 | MdsCtx::on_abort() -> (仅当 do_transfer_block_==true) transfer_out_tx_op(ON_ABORT): clear_transfer_blocking |

**调用顺序**: 先基类回调（MdsCtx 推进 MDS 节点状态）-> 再业务操作（transfer_out_tx_op）。

**transfer_out_tx_op 调用链**:
```
ls->transfer_out_tx_op(param)
  -> ObLSTxService::transfer_out_tx_op()
    -> ObLSTxCtxMgr::transfer_out_tx_op()
      -> iterate all txns, TransferOutTxOpFunctor
        -> ObPartTransCtx::do_transfer_out_tx_op()
```

**Replay Barrier**: STRICT_BARRIER（redo/commit/abort 日志均为 STRICT_BARRIER）。

---

### 3.4 TRANSFER_MOVE_TX_CTX (ID=27) -- 事务上下文搬迁

**定位**: 将源 LS 上的活跃事务上下文搬迁到目标 LS。分批注册，每批最多 2000 个事务。注册到**目标 LS**。

**注册数据**: CollectTxCtxInfo
```
src_ls_id_       : ObLSID             source LS
dest_ls_id_      : ObLSID             dest LS
task_id_         : int64_t            transfer task ID
transfer_epoch_  : int64_t            transfer epoch (= task_id)
transfer_scn_    : SCN                transfer SCN
args_            : ObSArray<ObTxCtxMoveArg>  tx ctx array
```

ObTxCtxMoveArg 关键字段:
```
tx_id_, epoch_, tx_state_(ACTIVE/PREPARE/COMMIT/ABORT),
trans_version_, scheduler_, table_lock_info_
```

**BufferCtx**: ObTransferMoveTxCtx（直接继承 BufferCtx，不经过 MdsCtx。因为不涉及 tablet MDS 节点，仅搬迁事务上下文）

**Leader 调用栈 (on_register)**:
```
do_move_tx_to_dest_ls_()
  -> conn->register_multi_data_source(TRANSFER_MOVE_TX_CTX)
    -> ObStartTransferMoveTxHelper::on_register()
       1. deserialize CollectTxCtxInfo
       2. update_status(REGISTER_SUCC, TRANSFER_MOVE_TX_CTX)   // ObLSTransferStatus
       3. while(5s timeout) retry ls->move_tx_op(REGISTER_SUCC)
          -> ObLSTxCtxMgr::move_tx_op()
            -> create tx ctx on dest LS, restore tx state
       4. on failure call clean() -> move_tx_op(ON_ABORT)
```

**Follower 调用栈 (on_replay)**:
```
on_replay():
  1. deserialize CollectTxCtxInfo
  2. update_status(REGISTER_SUCC, replay_scn)
  3. ls->move_tx_op(ON_REDO, is_replay=true)
  // note: Follower replay is equivalent to redo phase, no REGISTER_SUCC needed
```

**BufferCtx 回调**:

| 回调 | 行为 |
|------|------|
| on_redo | update_status(ON_REDO) -> while(true) 重试 move_tx_op(ON_REDO): 确认持久化 |
| on_commit | while(true) 重试 move_tx_op(ON_COMMIT): 合并事务元信息、clear_transfer_blocking、恢复表锁 -> update_status(ON_COMMIT) |
| on_abort | while(true) 重试 move_tx_op(ON_ABORT): 若事务由 transfer 创建且无写入则删除上下文 -> update_status(ON_ABORT) |

**move_tx_op 各操作类型详解** (来源: ObPartTransCtx::move_tx_op):

- **REGISTER_SUCC** (Leader on_register): 在目标 LS 创建事务上下文，设置 transfer_blocking。若 next_log_entry_no_==0，需先提交 redo 保证 move 日志不是该事务的第一条日志。
- **ON_REDO**: 确认事务上下文已持久化，更新 start_log_ts/end_log_ts/max_applying_log_ts/rec_log_ts（通过 transfer_op_log_cb_）。合并 transfer_parts_ 到事务参与者列表。
- **ON_COMMIT**: 合并事务元信息（start_log_ts 取最小值、end_log_ts 取最大值）。clear_transfer_blocking 恢复可写。recover_from_table_lock_durable_info 恢复表锁。对已提交事务做额外处理（exec_info_.state_ >= COMMIT）。
- **ON_ABORT**: 检查 need_del 条件（transfer_deleted_ && 无活跃写入 && transfer_epoch 匹配），若满足则删除事务上下文。有效 op_scn 调用 transfer_op_log_cb_，无效则跳过（Leader 失败场景）。

**与 ObTransferOutTxCtx 的区别**:
- ObTransferOutTxCtx: 继承 MdsCtx，管理 MDS 节点（tablet 元数据变更），操作源 LS
- ObTransferMoveTxCtx: 继承 BufferCtx，搬迁事务上下文（不涉及 MDS 节点），操作目标 LS
- 一次 transfer 可能产生多个 ObTransferMoveTxCtx（分批 2000 个事务）

**Replay Barrier**: STRICT_BARRIER。

---

### 3.5 START_TRANSFER_IN (ID=21) -- 目标侧 Tablet 创建

**定位**: 在目标 LS 上为每个待迁移的 tablet 创建 transfer tablet。注册到**目标 LS**。

**注册数据**: ObTXStartTransferInInfo
```
src_ls_id_, dest_ls_id_, tablet_list_(with tablet meta snapshot),
start_scn_(= transfer_scn), data_end_scn_, task_id_, transfer_epoch_
```

**BufferCtx**: ObStartTransferInMdsCtx（继承自 MdsCtx）

members:
```
version_ : VERSION (current V3)
ls_id_   : ObLSID    dest LS ID (used on on_abort)
```

**Leader 调用栈 (on_register)**:
```
do_tx_start_transfer_in_()
  -> conn->register_multi_data_source(START_TRANSFER_IN)
    -> ObTabletStartTransferInHelper::on_register_success_()
       -> iterate tablet_list, for each tablet:
          1. build new tablet from src tablet meta snapshot
          2. tablet_status_ = TRANSFER_IN
             data_type_ = START_TRANSFER_IN
             transfer_ls_id_ = src_ls_id
             transfer_scn_ = start_scn
             transfer_seq = src transfer_seq + 1
          3. write to MDS table
          4. mds_ctx.set_ls_id(dest_ls_id)  // for locating LS on on_abort
```

**Follower 调用栈 (on_replay)**:
- 通过 ObTabletStartTransferInReplayExecutor 处理
- `check_can_skip_replay_()` 检查 tablet 是否已存在（幂等）
- 失败时返回 OB_EAGAIN 触发重试（如源端 tablet 未 TRANSFER_OUT、transfer table 未替换等）
- 使用 STRICT_BARRIER

**BufferCtx 回调**:

| 回调 | 行为 |
|------|------|
| on_prepare | MdsCtx::on_prepare() + 错误注入点 EN_START_TRANSFER_IN_ON_PREPARE |
| on_redo | MdsCtx::on_redo() -> MDS 节点持久化 |
| on_commit | MdsCtx::on_commit() -> MDS 节点可见（tablet 可读） |
| on_abort | MdsCtx::on_abort() -> 设置 TabletEmptyShellHandler 的 empty_shell_trigger -> 触发 GC 清理 |

**check_can_replay_commit**: 在 Follower 回放 commit 日志前调用，检查目标 LS 上的 tablet 是否已完成 transfer_in 创建，确保 commit 的前置条件满足。

**check_can_do_tx_end**: 在事务提交/中止前调用，enable_transfer_dml_ctrl (>= 4.2.3) 时执行 cleanup_transfer_meta_info 清理。

**Replay Barrier**: STRICT_BARRIER。

---

### 3.6 FINISH_TRANSFER_IN (ID=23) -- 目标侧转入完成

**定位**: DOING 阶段，将目标 LS 上的 tablet 从 TRANSFER_IN 状态恢复为 NORMAL。注册到**目标 LS**。

**注册数据**: ObTXFinishTransferInInfo
```
src_ls_id_, dest_ls_id_, tablet_list_(with transfer_seq etc.)
```

**BufferCtx**: ObFinishTransferInMdsCtx（继承自 MdsCtx）

members:
```
version_  : VERSION (current V2)
redo_scn_ : SCN    finish log SCN (as finish_transfer_scn)
```

**Leader 调用栈 (on_register)**:
```
do_tx_finish_transfer_in_()
  -> ObTabletFinishTransferInHelper::on_register_success_()
     -> iterate tablet_list, for each tablet:
        1. pre-validation:
           - check_ls_replay_scn_(): check all replica replay progress (OB_EAGAIN retry)
           - check_transfer_seq: tablet.transfer_seq == tablet_list.transfer_seq + 1
           - check_transfer_table_replaced_(): confirm backfill done (transfer_table cleared)
             triggers rebuild if not done for too long
        2. modify MDS data:
           tablet_status_ = NORMAL
           data_type_ = FINISH_TRANSFER_IN
           transfer_ls_id_ = src_ls_id (keep source info)
```

**Follower 差异**:
- 通过 ObTabletFinishTransferInReplayExecutor 处理
- check trans_stat == ON_COMMIT (confirm START_TRANSFER_IN MDS committed)

**BufferCtx 回调**:

| 回调 | 行为 |
|------|------|
| on_redo | **redo_scn_ = redo_scn**（记录 finish SCN）-> MdsCtx::on_redo() |
| on_commit | MdsCtx::on_commit() |
| on_abort | MdsCtx::on_abort() -> 设置 empty_shell_trigger -> GC 清理残留 tablet |

**check_can_do_tx_end**: 在事务提交前调用 `ls->cleanup_transfer_meta_info(redo_scn_)` 重置 transfer_meta_info（>= 4.2.3）。

**Replay Barrier**: STRICT_BARRIER。

**SS 模式特殊处理**: 见 [6.3](#63-finish_transfer_in-回填检查)。

---

### 3.7 FINISH_TRANSFER_OUT (ID=22) -- 源侧转出完成

**定位**: DOING 阶段，将源 LS 上的 tablet 标记为 TRANSFER_OUT_DELETED，后续由 GC 清理为 empty shell。注册到**源 LS**。

**注册数据**: ObTXFinishTransferOutInfo
```
src_ls_id_, dest_ls_id_, tablet_list_, finish_scn_(= FINISH_TRANSFER_IN redo_scn)
```

**BufferCtx**: 通用 MdsCtx（与 FINISH_TRANSFER_IN 使用自定义 Ctx 不同，因为 FINISH_TRANSFER_OUT 不需要额外的 cleanup 操作）。

**Leader 调用栈 (on_register)**:
```
do_tx_finish_transfer_out_()
  -> ObTabletFinishTransferOutHelper::on_register_success_()
     -> iterate tablet_list, for each tablet:
        1. pre-validation:
           - tablet_status_ == TRANSFER_OUT
           - data_type_ == START_TRANSFER_OUT (committed)
        2. modify MDS data:
           tablet_status_ = TRANSFER_OUT_DELETED
           data_type_ = FINISH_TRANSFER_OUT
        3. set persist_trigger + empty_shell_trigger -> trigger GC
```

**Follower 差异**:
- check trans_stat == ON_COMMIT (confirm START_TRANSFER_OUT MDS committed)
- tolerate OB_TABLET_NOT_EXIST (tablet may be cleaned by other path)

**Replay Barrier**: PRE_BARRIER（先 drain 再回放）。

---

### 3.8 TRANSFER_IN_ABORTED (ID=32) -- 转入中止清理

**定位**: ABORT 路径，在目标 LS 上验证 transfer in 创建的 tablet 是否已被 GC 清理。注册到**目标 LS**。

**注意**: TRANSFER_IN_ABORTED 不直接删除 tablet，而是**验证 GC 完成** -- tablet 的删除由 START_TRANSFER_IN 的 on_abort 触发的 empty_shell_trigger 异步完成。

**注册数据**: ObTXFinishTransferInInfo（复用 FINISH_TRANSFER_IN 的数据结构）

**BufferCtx**: ObAbortTransferInMdsCtx（继承自 MdsCtx）

members:
```
version_  : VERSION (current V2)
redo_scn_ : SCN    abort log SCN
```

**Leader 调用栈 (on_register)**:
```
do_trans_transfer_aborted_()
  -> ObTabletAbortTransferHelper::on_register_success_()
     -> iterate tablet_list, for each tablet:
        1. check_transfer_in_tablet_aborted_()
           -> verify tablet transfer_in cleaned (GC done)
           -> OB_EAGAIN retry until GC done
        2. no tablet_status change (no state modification)
```

**Follower 差异**:
- check_can_skip_replay_() 中 skip_replay 逻辑: 当前代码中 skip_replay 初始值为 true，条件分支后保持不变，实质上 Follower 回放总是跳过 tablet 级别的重放检查
- on_replay_success_() 仍会执行，但逻辑与 Leader 一致（验证 GC 完成）

**BufferCtx 回调**:

| 回调 | 行为 |
|------|------|
| on_redo | redo_scn_ = redo_scn -> MdsCtx::on_redo() |
| on_commit | MdsCtx::on_commit() |

**check_can_do_tx_end**: 在事务提交前调用 `cleanup_transfer_meta_info(redo_scn_)` 重置 transfer 相关元信息（>= 4.2.3）。旧版本（< 4.2.3）直接返回 true。

**Replay Barrier**: STRICT_BARRIER。

---

### 3.9 TRANSFER_TASK (ID=24) -- 集群级别元数据同步

**定位**: 注册到 **SYS_LS**（1号日志流），将 Balance Task 的 transfer 开始/结束事件写入 clog，使备库能通过回放日志感知主库的 transfer 操作。

**与其他 transfer MDS 的本质区别**: 其他 MDS 注册到数据 LS 处理 tablet 级别操作，TRANSFER_TASK 注册到 SYS_LS 处理集群级别的元数据同步。

**注册数据**: ObBalanceTaskHelperMeta
```
task_op_      : ObBalanceTaskHelperOp    TRANSFER_BEGIN or TRANSFER_END
src_ls_       : ObLSID                   source LS
dest_ls_      : ObLSID                   dest LS
ls_group_id_  : uint64_t                 LS group ID
```

**BufferCtx**: 通用 MdsCtx（无自定义回调）。

**Helper**: ObBalanceTaskMDSHelper -- **on_register 和 on_replay 均为空实现**（UNUSEDx）。真正的作用在于 clog 同步。

**注册时机**:
```
ObBalanceTaskTableOperator::update_task_status()
  -> register TRANSFER_BEGIN when status becomes TRANSFER
  -> register TRANSFER_END when status leaves TRANSFER
```

**备库消费**:
```
ObRecoveryLSService::process_ls_transfer_task_in_trans_()
  -> deserialize ObBalanceTaskHelperMeta
  -> write to __all_balance_task_helper inner table

ObRecoveryLSService::try_process_ls_balance_task_helper_()  (periodic check)
  -> TRANSFER_BEGIN: mark transfer in progress, prevent readable_scn from advancing
  -> TRANSFER_END: clear record after confirming transfer log replay done
```

**与 Clone 的冲突**: 备库回放 TRANSFER_TASK 日志时，会取消正在进行的 clone job（STANDBY_TRANSFER 冲突类型）。


---

## 第四章 跨切面机制

### 4.1 Tablet 状态机（DataType + Status 完整变迁）

#### 4.1.1 类型定义

**ObTabletMdsUserDataType** (`src/storage/tablet/ob_tablet_create_delete_mds_user_data.h:21-48`):

| 枚举值 | 数值 | 含义 |
|--------|------|------|
| START_TRANSFER_OUT_PREPARE | 7 | 源侧转出准备 |
| START_TRANSFER_OUT | 3 | 源侧开始转出 |
| START_TRANSFER_IN | 4 | 目的侧开始转入 |
| FINISH_TRANSFER_OUT | 5 | 源侧完成转出 |
| FINISH_TRANSFER_IN | 6 | 目的侧完成转入 |

**ObTabletStatus** (`src/storage/tablet/ob_tablet_status.h:19-32`):

| 枚举值 | 数值 | 含义 |
|--------|------|------|
| NORMAL | 1 | 正常状态 |
| TRANSFER_OUT | 4 | 源端 tablet 正在转移出 |
| TRANSFER_IN | 5 | 目的端 tablet 正在转移入 |
| TRANSFER_OUT_DELETED | 6 | 源端转移完成后标记为删除 |

#### 4.1.2 源端 Tablet 状态变迁

```
  NORMAL
    |
    | START_TRANSFER_OUT_PREPARE (ID=25) on_register
    | TabletStatus: NORMAL -> TRANSFER_OUT
    | DataType:     -> START_TRANSFER_OUT_PREPARE
    | transfer_scn: min (pending)
    v
  TRANSFER_OUT (DataType=PREPARE)
    |
    | START_TRANSFER_OUT_V2 (ID=26) on_register
    | TabletStatus: TRANSFER_OUT (unchanged)
    | DataType:     PREPARE -> START_TRANSFER_OUT
    v
  TRANSFER_OUT (DataType=START_TRANSFER_OUT)
    |
    | commit_trans_() -> MDS node committed
    |
    | --- DOING phase ---
    |
    | FINISH_TRANSFER_OUT (ID=22) on_register
    | TabletStatus: TRANSFER_OUT -> TRANSFER_OUT_DELETED
    | DataType:     -> FINISH_TRANSFER_OUT
    v
  TRANSFER_OUT_DELETED -> empty shell -> GC
```

**Abort 路径**: MDS 节点回滚，tablet 恢复为 NORMAL。

#### 4.1.3 目的端 Tablet 状态变迁

```
  (not exist)
    |
    | START_TRANSFER_IN (ID=21) on_register
    | TabletStatus: (create) -> TRANSFER_IN
    | DataType:     -> START_TRANSFER_IN
    | transfer_ls_id: src LS
    | transfer_scn: start_scn
    v
  TRANSFER_IN (DataType=START_TRANSFER_IN)
    |
    | commit_trans_() -> MDS node committed
    |
    | --- Backfill done ---
    |
    | FINISH_TRANSFER_IN (ID=23) on_register
    | TabletStatus: TRANSFER_IN -> NORMAL
    | DataType:     -> FINISH_TRANSFER_IN
    | transfer_ls_id: reset
    v
  NORMAL (DataType=FINISH_TRANSFER_IN)
```

**Abort 路径**: on_abort 设置 empty_shell_trigger，由 GC 异步清理。

#### 4.1.4 前置校验规则

| MDS 操作 | 要求 TabletStatus | 要求 DataType | 要求 trans_stat | 代码位置 |
|---------|------------------|--------------|----------------|---------|
| START_TRANSFER_OUT_PREPARE | NORMAL | -- | ON_COMMIT | mds_helper.cpp:433-436 |
| START_TRANSFER_OUT_V2 | TRANSFER_OUT | START_TRANSFER_OUT_PREPARE | != ON_COMMIT | mds_helper.cpp:439-443 |
| FINISH_TRANSFER_OUT | TRANSFER_OUT | -- | -- | finish_mds_helper.cpp:447-448 |
| FINISH_TRANSFER_IN | TRANSFER_IN | -- | transfer_seq 匹配 | finish_mds_helper.cpp:1141-1142 |

#### 4.1.5 注册阶段 vs 回调阶段的影响区别

1. **注册阶段**（on_register_success_ / Helper）: 直接调用 `set_tablet_status()` 设置 DataType 和 TabletStatus，数据为 **uncommitted** 状态。
2. **回调阶段**（on_redo / on_commit）: MdsCtx 的回调推进 MDS 节点状态（uncommitted -> committed），**不再修改** DataType 或 TabletStatus 的值。自定义回调（如 transfer_out_tx_op）操作的是**事务上下文**状态。
3. **on_abort**: MDS 节点回滚，tablet 恢复到修改前的状态。

---

### 4.2 弱读控制机制

#### 4.2.1 核心类: ObLSTransferStatus

文件: `src/storage/ls/ob_ls_transfer_status.h/cpp`

```
members:
  transfer_tx_id_       : ObTransID    current transfer tx ID
  transfer_task_id_     : int64_t      transfer task ID
  transfer_prepare_op_  : bool         whether DEST_PREPARE in progress
  transfer_prepare_scn_ : SCN          prepare SCN
  move_tx_op_           : bool         whether MOVE_TX in progress
  move_tx_scn_          : SCN          move_tx SCN

key methods:
  update_status(tx_id, task_id, op_scn, op_type, mds_type)
  is_finished() = !transfer_prepare_op_ && !move_tx_op_
```

Leader 调用 `update_status_inner_()`，Follower 调用 `replay_status_inner_()`（通过 SCN 过滤重复回放）。

#### 4.2.2 弱读推进控制原理

**为什么需要冻结弱读**: Transfer 期间目标 LS 接收了从源 LS 搬来的事务上下文。如果弱读时间戳继续推进，可能读到不一致的数据（搬来的事务还未完全就绪）。

**禁用**: TRANSFER_DEST_PREPARE on_register 时
```
ObLSTransferStatus::update_status(REGISTER_SUCC, TRANSFER_DEST_PREPARE)
  -> disable_upper_trans_calculation_()
    -> ObTxTable::disable_upper_trans_calculation()
      -> ATOMIC_STORE(&calc_upper_trans_is_disabled_, true)
      -> weak-read ts stops advancing
```

weak-read ts advance skipped (ob_ls_wrs_handler.cpp:98-104):
```cpp
if (ls.get_transfer_status().get_transfer_prepare_enable()) {
    timestamp.reset();
    need_skip = true;  // weak-read ts advance completely skipped
}
```

**恢复**: TRANSFER_DEST_PREPARE on_commit/on_abort 时
```
ObLSTransferStatus::update_status(ON_COMMIT/ON_ABORT, TRANSFER_DEST_PREPARE)
  -> enable_upper_trans_calculation_(op_scn)
    -> ls->clear_keep_alive_smaller_scn_info()
    -> ObTxTable::enable_upper_trans_calculation(latest_transfer_scn)
      -> ATOMIC_STORE(&calc_upper_trans_is_disabled_, false)
      -> latest_transfer_scn_ = latest_transfer_scn  // protect lower bound
```

**恢复后的保护**: `latest_transfer_scn_` 保证弱读不回退到 transfer_scn 之前（ob_tx_data_table.cpp:1004-1013）:
```cpp
if (latest_transfer_scn_.is_valid() && effective_scn < latest_transfer_scn_) {
    need_skip = true;  // skip tx version calc before transfer_scn
}
```

#### 4.2.3 状态机

```
              +----------------+
              | initial state  |
              | prepare=false  |
              | move_tx=false  |
              +-------+--------+
                      |
  DEST_PREPARE        |           MOVE_TX_CTX
  REGISTER_SUCC       |           REGISTER_SUCC
  +--------+          |           +--------+
  |prepare |<---------+---------->|move_tx |
  |=true   |          |           |=true   |
  |disable |          |           |        |
  |weak-rd |          |           |        |
  +----+---+          |           +----+---+
       | ON_COMMIT    |                | ON_COMMIT
       | ON_ABORT     |                | ON_ABORT
  +----v---+          |           +----v---+
  |prepare |          |           |move_tx |
  |=false  |          |           |=false  |
  |enable  |          |           |        |
  |weak-rd |          |           |        |
  +--------+          |           +--------+
                      |
              is_finished() = true
```

#### 4.2.4 源端等待弱读推进

禁用目标端弱读后，源端还需执行 `wait_src_ls_advance_weak_read_ts_()`（ob_transfer_handler.cpp:2918-2975）:
1. RPC 获取目标端的 `transfer_dest_prepare_scn`
2. 循环等待 `src.weak_read_ts > transfer_dest_prepare_scn`
3. 确保源端在 prepare_scn 之前的所有事务都已提交并可见

---

### 4.3 transfer_scn 的产生与作用

#### 4.3.1 产生链路

```
do_tx_start_transfer_out_()                          (ob_transfer_handler.cpp:1484)
  +-> flag.mds_base_scn_ = max(src_max_decided_scn, dest_max_decided_scn)  (line 1534)
  +-> conn->register_multi_data_source(START_TRANSFER_OUT_V2)               (line 1540)
  +-> PALF allocate log SCN:
  |   scn = max(mds_base_scn, last_scn + 1)
  +-> on_redo callback: transfer_scn_ = redo_scn
  +-> get_start_transfer_out_scn_() -> start_scn = transfer_scn_            (line 1691)
```

**transfer_scn 的语义**: 源端停止写入该 tablet 的时间点（逻辑时钟），同时服务于弱读和 Backfill 两个维度。

#### 4.3.2 弱读维度

```
time -->
  |  dest LS weak-read advancing normally (weak_read_ts = T0)
  |
  |  [TRANSFER_DEST_PREPARE on_register] -> freeze weak-read, prepare_scn = T1
  |  [wait src.weak_read_ts > T1]
  |  [START_TRANSFER_OUT_V2 redo] -> transfer_scn = T2 (>= max(src_decided, dest_decided))
  |
  |  [START tx commit, DEST_PREPARE on_commit]
  |  -> restore weak-read, latest_transfer_scn_ = T2 (lower bound, no rollback before T2)
  |
  |  dest LS weak-read continues advancing
  v
```

#### 4.3.3 Backfill 维度

Backfill 的数据范围: 源端 tablet 在 transfer_scn 时刻及之前的所有数据。

```
ObTransferWorkerMgr::process()                   (ob_transfer_backfill_tx.cpp:345)
  -> backfill_scn = tablet.transfer_info_.transfer_start_scn_     (line 195)
  -> for each memtable: compare start_scn/end_scn with backfill_scn
  -> for each sstable: compare filled_tx_scn with backfill_scn
```

Backfill 前置条件（ob_transfer_backfill_tx.cpp:299-304）: 源端 max_decided_scn 必须 >= transfer_start_scn，确保日志已回放到位。

#### 4.3.4 为什么 transfer_scn 必须 >= dest_max_decided_scn

通过 `mds_base_scn = max(src_decided, dest_decided)` 保证:
- transfer_scn 大于两端已有的所有日志 SCN
- Backfill 边界清晰（backfill_scn 在目标端时间线上也有效）
- latest_transfer_scn_ 有效保护弱读不回退

如果 transfer_scn < dest_decided_scn: latest_transfer_scn_ 的保护形同虚设（目标端弱读早已超过），backfill 范围判断可能与目标端已有操作冲突。

---

### 4.4 Replay Barrier 机制

#### 4.4.1 两级 Barrier

- **PRE_BARRIER**: drain 当前已入队的回放任务后执行，可与其他 PRE_BARRIER 并发
- **STRICT_BARRIER**: 完全串行化，drain 后独占执行，确保 tablet 状态变更的原子性

#### 4.4.2 各 MDS 类型 Barrier 对比

| MDS 类型 | ID | redo 日志 | commit 日志 | abort 日志 | 选择原因 |
|---------|-----|-----------|------------|-----------|---------|
| START_TRANSFER_OUT_PREPARE | 25 | PRE_BARRIER | - | - | 仅修改 tablet status，不涉及事务阻塞 |
| START_TRANSFER_OUT_V2 | 26 | STRICT | STRICT | STRICT | 涉及事务阻塞/解阻塞，必须串行 |
| TRANSFER_MOVE_TX_CTX | 27 | STRICT | STRICT | STRICT | 事务上下文搬迁，必须串行 |
| START_TRANSFER_IN | 21 | STRICT | STRICT | STRICT | 创建 tablet，必须串行 |
| TRANSFER_DEST_PREPARE | 28 | - | - | - | 仅控制弱读推进，无 barrier |
| FINISH_TRANSFER_IN | 23 | STRICT | - | - | 恢复 NORMAL 状态，必须串行 |
| FINISH_TRANSFER_OUT | 22 | PRE_BARRIER | - | - | 标记删除，不需要完全串行 |
| TRANSFER_IN_ABORTED | 32 | - | - | - | 仅验证清理完成，无 barrier |

---

### 4.5 Follower 重放流程

#### 4.5.1 Leader vs Follower 核心差异

| 维度 | Leader (on_register) | Follower (on_replay) |
|------|---------------------|---------------------|
| 入口 | Helper::on_register() | Helper::on_replay(buf, len, scn, ctx) |
| tablet 操作 | set_tablet_status() 直接写 MDS table | ReplayExecutor::do_replay_() -> replay_to_mds_table_() |
| SCN 参数 | invalid（由框架分配） | 从 clog 中获取的有效 scn |
| 额外校验 | 较少 | check_src/dest_transfer_tablet_ 等前置检查 |
| 最终状态 | **完全一致** | **完全一致** |

#### 4.5.2 Follower 额外校验

| MDS 类型 | Follower 额外校验 | 说明 |
|---------|-------------------|------|
| START_TRANSFER_OUT_PREPARE (25) | check_src_transfer_tablet_() | 校验源 tablet 状态 NORMAL |
| START_TRANSFER_OUT_V2 (26) | check_src_transfer_tablet_() | 校验源 tablet 已是 TRANSFER_OUT（不强制检查，因 MDS 回滚后重放可能从中间开始） |
| START_TRANSFER_IN (21) | check_can_skip_replay_() + check_transfer_dest_tablets_() | 支持 rebuild/restore 跳过 |
| FINISH_TRANSFER_IN (23) | check_transfer_table_replaced_() | 确认 Backfill 完成，否则 OB_EAGAIN 或触发 rebuild |
| FINISH_TRANSFER_OUT (22) | trans_stat == ON_COMMIT | 等待前序 MDS 节点 commit 后才能继续 |

#### 4.5.3 V2 与 PREPARE 的 Follower 差异

V2 相比 PREPARE 在 Follower 路径额外执行:
- `record_transfer_block_op()` + `transfer_out_tx_op()`: 阻塞事务
- `try_enable_dest_ls_clog_replay()`: 启用目标 LS 回放
- `set_transfer_tablets_freeze_flag_()`: 冻结源 tablet
- 使用 STRICT_BARRIER（PREPARE 使用 PRE_BARRIER）

---

### 4.6 事务阻塞与恢复（transfer_blocking 生命周期）

transfer_blocking 标记控制源 LS 上活跃事务的写入能力，完整生命周期:

```
1. PREPARE (ID=25) on_register:
   -> tablet_status = TRANSFER_OUT
   -> block new writes (new txns see TRANSFER_OUT and refuse to write)
   -> does NOT set transfer_blocking

2. V2 (ID=26) on_register:
   -> transfer_out_tx_op(REGISTER_SUCC)
     -> sub_state_.set_transfer_blocking()      // set blocking
   -> active txns blocked

3. V2 (ID=26) on_redo:
   -> transfer_out_tx_op(ON_REDO)
     -> set_transfer_blocking()
     -> exec_info_.is_transfer_blocking_ = true  // persist flag

4. MOVE_TX_CTX (ID=27) on_register (dest side):
   -> move_tx_op(REGISTER_SUCC)
     -> set_transfer_blocking()                  // blocking on dest too

5. V2 (ID=26) on_commit:
   -> transfer_out_tx_op(ON_COMMIT)
     -> clear_transfer_blocking()                // unblock on src
     -> exec_info_.is_transfer_blocking_ = false

6. MOVE_TX_CTX (ID=27) on_commit (dest side):
   -> move_tx_op(ON_COMMIT)
     -> clear_transfer_blocking()                // unblock on dest
   -> txns resume writable

7. Abort path:
   -> V2 on_abort: clear_transfer_blocking()     // rollback on src
   -> MOVE_TX_CTX on_abort: delete dest tx ctx (if no writes)
```

---

### 4.7 transfer_op_log_cb_ 状态补偿机制

文件: `ob_trans_part_ctx.cpp:10767-10791`

#### 4.7.1 作用

Transfer 操作不走正常的日志提交/回放路径，而是通过 MDS 回调直接操作事务上下文。`transfer_op_log_cb_` 负责将操作的 SCN 同步到事务上下文的时间戳字段，模拟一次日志回放的状态推进。

#### 4.7.2 更新的三组字段

| 字段 | 更新逻辑 | 用途 |
|------|---------|------|
| ctx_tx_data_.start_log_ts | 首次设置（无效时才赋值） | 事务第一条日志 SCN，TxTable 回收判断 |
| ctx_tx_data_.end_log_ts | 取最大值 | 事务最后一条日志 SCN，TxTable 回收判断 |
| exec_info_.max_applying_log_ts / max_applied_log_ts | 取最大值 | 回放去重/幂等判断 |
| rec_log_ts (via update_rec_log_ts_) | 取最小值(dec_update) | checkpoint 恢复点，最早未持久化变更 |

#### 4.7.3 调用场景

| 场景 | REGISTER_SUCC | ON_REDO | ON_COMMIT | ON_ABORT |
|------|--------------|---------|-----------|----------|
| do_transfer_out_tx_op (源端) | 不调用 | 调用 | 调用 | 调用 |
| do_transfer_in_move_tx_op (目标端) | 不调用 | 调用 | 调用 | 调用 |

REGISTER_SUCC 不调用是因为此时仅做内存操作，无需日志补偿。

#### 4.7.4 必要性

如果不调用 transfer_op_log_cb_:
- start_log_ts / end_log_ts 不正确 -> TxData 回收时机错误
- max_applying_log_ts 不推进 -> 重复回放时无法去重
- rec_log_ts 不更新 -> checkpoint 恢复时遗漏 transfer 操作

---

### 4.8 BufferCtx 回调策略对比

#### 4.8.1 继承方式

| 类 | 继承方式 | 原因 |
|----|---------|------|
| ObTransferOutTxCtx | MdsCtx -> BufferCtx | 需管理 MDS 节点（tablet 元数据变更） |
| ObStartTransferInMdsCtx | MdsCtx -> BufferCtx | 同上，目标 LS 创建 tablet 需写 MDS |
| ObFinishTransferInMdsCtx | MdsCtx -> BufferCtx | 同上 |
| ObAbortTransferInMdsCtx | MdsCtx -> BufferCtx | 同上 |
| ObTransferMoveTxCtx | BufferCtx | 不涉及 MDS 节点，仅搬迁事务上下文 |
| ObTransferDestPrepareTxCtx | BufferCtx | 不涉及 MDS 节点，仅控制弱读推进 |

#### 4.8.2 回调复杂度

| 类 | on_redo | on_commit | on_abort |
|----|---------|-----------|----------|
| ObTransferOutTxCtx | 重(MdsCtx+tx_op) | 重(MdsCtx+tx_op) | 条件执行 |
| ObTransferMoveTxCtx | 重(move_tx_op+status) | 重(move_tx_op+status) | 重(move_tx_op+status) |
| ObTransferDestPrepareTxCtx | 轻(update_status) | 轻(status+恢复弱读) | 轻(status+恢复弱读) |
| ObStartTransferInMdsCtx | MdsCtx | MdsCtx | MdsCtx+清理空壳 |
| ObFinishTransferInMdsCtx | 记录scn+MdsCtx | MdsCtx | MdsCtx |
| ObAbortTransferInMdsCtx | 记录scn+MdsCtx | MdsCtx | MdsCtx |

#### 4.8.3 重试策略

所有 transfer 相关的自定义回调都有 **while(true) 无限重试** 特点:
- sleep 间隔: 10ms (`ob_usleep(10 * 1000)`)
- 日志频率: 每 10s 输出一次 ERROR (`REACH_TIME_INTERVAL(10s)`)
- 原因: transfer 回调在事务提交路径上，不允许失败

唯一例外: ObTransferMoveTxCtx 的 on_register 有 **5s 超时限制**。


---

## 第五章 Balance 框架与 LS 生命周期

### 5.1 Balance Task 类型与状态机

Transfer 是 Balance 框架的子步骤。Balance 框架通过不同类型的 Task 管理 LS 的创建、均衡和删除。

#### 5.1.1 Task 类型

定义: `src/share/balance/ob_balance_task_table_operator.h:92-116`

| Task 类型 | 值 | 含义 | 创建 LS | 删除 LS |
|----------|----|----|---------|---------|
| SPLIT | 0 | 分裂：创建新 LS + Transfer | 是 (dest_ls) | 否 |
| ALTER | 1 | 变更 LS Group ID | 否 | 否 |
| MERGE | 2 | 合并：Transfer + 删除源 LS | 否 | 是 (src_ls) |
| TRANSFER | 3 | 纯 Transfer | 否 | 否 |
| CREATE | 4 | 创建新 LS | 是 (dest_ls) | 否 |

#### 5.1.2 Task 状态机

定义: `src/share/balance/ob_balance_task_table_operator.h:46-58`

```
INIT(0) -> CREATE_LS(1) -> ALTER_LS(2) -> SET_LS_MERGING(3) -> TRANSFER(4) -> DROP_LS(5) -> COMPLETED(6)
                                                                                             CANCELED(7)
```

each task type follows different state subset (`get_next_status()`, `ob_balance_task_table_operator.cpp:88-147`):

| 任务类型 | 状态路径 |
|---------|---------|
| SPLIT | INIT -> CREATE_LS -> TRANSFER -> COMPLETED |
| MERGE | INIT -> SET_LS_MERGING -> TRANSFER -> DROP_LS -> COMPLETED |
| TRANSFER | INIT -> TRANSFER -> COMPLETED |
| CREATE | INIT -> CREATE_LS -> COMPLETED |
| ALTER | INIT -> ALTER_LS -> COMPLETED |

### 5.2 LS 创建流程（SPLIT / CREATE 任务）

触发点: `ObBalanceTaskExecuteService::process_init_task_()` (`ob_balance_task_execute_service.cpp:616`)

```
process_init_task_(task, trans)
  +-- check task_type: is_split_task() || is_create_task()
  +-- wait_can_create_new_ls_(create_scn)
  |   ensure create_scn > last deleted LS offline_scn (prevent SCN rollback)
  +-- ObLSAttr::init(dest_ls_id, ls_group_id, flag, OB_LS_CREATING)
  +-- ObLSAttrOperator::insert_ls(ls_info, trans)
      write to __all_ls table, status = CREATING
```

Observer 收到 RPC 后本地创建:
```
ObRpcCreateLSP::process()
  -> ObLSService::create_ls(arg_)
     1. inner_create_ls_(): allocate ObLS + init
     2. add_ls_to_map_()
     3. prepare_create_ls() -> write prepare slog
     4. ls->create_ls(): create PALF instance
     5. ls->create_ls_inner_tablet(): create 4 inner tablets
     6. commit_create_ls() -> write commit slog
     7. ls->finish_create_ls()
```

LS status: (not exist) -> CREATING -> CREATED -> NORMAL

### 5.3 LS 删除流程（MERGE 任务）

#### 5.3.1 初始化: 设置 BLOCK_TABLET_IN

MERGE 任务在 INIT 阶段给源 LS 打上 `BLOCK_TABLET_IN` 标志（`ob_balance_task_execute_service.cpp:681-699`），防止 MERGE 期间新 tablet 创建到源 LS。

#### 5.3.2 Transfer 完成后设置 DROPPING

所有 partition 搬完后（`ob_balance_task_execute_service.cpp:275`）:
```
set_ls_to_dropping_(src_ls_id, trans)
  -> ls_op.update_ls_status_in_trans(src_ls_id, OB_LS_NORMAL, OB_LS_DROPPING)
```

#### 5.3.3 等待下线和删除

```
NORMAL --[set_ls_to_dropping_]--> DROPPING --[GC + offline]--> WAIT_OFFLINE --[delete_ls]--> (deleted)
```

`ObPrimaryLSService::try_delete_ls_()` (`ob_primary_ls_service.cpp:316`) 向所有副本发 RPC 确认可下线后，从 `__all_ls` 表中删除记录。

#### 5.3.4 LS 状态全景图

```
(not exist)
    |
    | insert_ls() [SPLIT/CREATE]
    v
CREATING (0) --[RPC create]--> CREATED (1) --[set member list]--> NORMAL (2)
                                                                      |
                                                  [MERGE: set_ls_to_dropping_]
                                                                      |
                                                                      v
                                                                 DROPPING (3)
                                                                      |
                                                                [GC + offline]
                                                                      |
                                                                      v
                                                              WAIT_OFFLINE (5)
                                                                      |
                                                                [delete_ls]
                                                                      |
                                                                      v
                                                                DROPPED (8)

failure path: CREATING/CREATED --[abort]--> CREATE_ABORT (6)
```

---

## 第六章 SS（Shared Storage）模式特殊处理

### 6.1 LS 创建的双重元信息管理

SS 模式下 LS 的元信息存在于**两个位置**:

| 位置 | 内容 | 管理者 |
|------|------|-------|
| 本地 | SLOG + checkpoint super block LS Item | ObTenantStorageMetaService |
| 共享存储 | 对象存储上的 LS meta + tablet meta | ObSSMetaService |

创建时依次执行两步（`ob_rpc_processor_simple.cpp:1894-1909`）:
1. `ObLSService::create_ls()`: 本地创建（SLOG + PALF + inner tablet）
2. `ObSSMetaService::create_ls()`: 将 LS meta 和 4 个 inner tablet meta 写入对象存储

SS 模式下 `ObLS::init()` 额外初始化 6 个 SS handler: ls_prewarm_handler_, primary_sswriter_ls_handler_, restore_sswriter_ls_handler_, inc_meta_ckpt_, inc_sstable_uploader_, ss_checkpoint_executor_。

### 6.2 LS 删除的多层清理

SS 模式下 LS 删除涉及三层:

**阶段 1: GC Handler -- 更新共享 LS gc_state**

GC handler 在设置本地 gc_state 前，先更新对象存储上的 LS meta（`ob_garbage_collector.cpp:828,1182`）:
```
ObGCHandler::offline_ls()
  -> [SS] update_ss_ls_meta_(ls_id, LSGCState::LS_OFFLINE)  // shared first
  -> ls_->set_gc_state(LSGCState::LS_OFFLINE)                // then local
```

**阶段 2: 本地 LS 移除**

`ObLSService::remove_ls_()` 执行 5 步（`ob_ls_service.cpp:1183-1249`）:
1. [SS] `write_tablet_id_set_to_pending_free()`: 将 tablet_id 写入 pending_free 列表
2. `TENANT_STORAGE_META_SERVICE.delete_ls()`: 写 delete slog
3. `ls->remove_ls()`: 移除 PALF
4. `remove_ls_from_map_()`: 从内存移除
5. `tx_svr->remove_tablet(ls_id)`: 清理 tablet

**阶段 3: SS GC -- 清理对象存储**

`ObSSGarbageCollector::gc_log_streams_in_ss_()` (`ob_ss_garbage_collector.cpp:476-494`):
1. 检查对象存储上 LS meta 的 gc_state == WAIT_GC 且 `__all_ls_status` 中已不存在
2. `gc_tablets_in_ss_by_detect_()`: GC 该 LS 下所有 tablet
3. `gc_ls_()`: 删除对象存储上的 LS 目录和 LS meta

### 6.3 FINISH_TRANSFER_IN 回填检查

**结论**: 当前代码只检查了计算节点的回填（`has_transfer_table()`），未单独检查共享存储回填。但由于 SS 架构特性，这在大多数场景下是充分的。

检查流程:
```
check_ls_logical_table_replaced()                (ob_finish_transfer.cpp:541)
  -> send RPC to each node in addr_list (paxos_member_list + learner_list)
  -> each node checks locally:
     iterate all transfer tablets:
       tablet->get_tablet_meta().has_transfer_table()
     any tablet still has transfer_table -> backfill_finished = false
```

**为什么计算节点检查已足够**: SS 模式的回填流程 `do_replace_logical_tables_()` 中:
1. 先替换本地 tablet 的 table_store（去掉 transfer_table）
2. 然后 `put_sstables_to_shared_tablet_()` 将 sstable 注册到共享存储

两步在同一个 `ObTransferReplaceTableTask` DAG 中串行执行。当 `has_transfer_table()` 返回 false 时，共享存储侧的操作也应已完成。

**注意**: `do_tx_transfer_doing_()` 中声明了 `is_shared_storage` 变量但未使用，说明 SS 模式的回填检查目前没有额外的特殊处理路径。

### 6.4 "先共享后本地"原则

SS 模式下 LS 状态变更遵循 **"先更新共享元信息，再更新本地元信息"** 的原则:
- GC handler 中所有 `set_gc_state()` 调用前，都先调用 `update_ss_ls_meta_()`
- 确保崩溃重启后，共享存储上的状态已是最新的
- 其他副本或 SS GC 服务可基于共享存储状态做正确判断

**例外**: 创建 LS 时是"先本地后共享"（先 `ObLSService::create_ls()`，后 `ObSSMetaService::create_ls()`），因为本地创建失败时不需要清理共享存储，而共享存储失败时 SS GC 会检测到异常 LS 并清理。

---

## 附录

### A. 源文件完整索引

| 目录 | 文件 | 用途 |
|------|------|------|
| storage/high_availability/ | ob_transfer_handler.cpp | Transfer 主状态机 |
| | ob_transfer_service.cpp | Transfer 调度服务 |
| | ob_finish_transfer.cpp | DOING 阶段 |
| | ob_transfer_struct.h | Transfer 数据结构 |
| | ob_transfer_backfill_tx.cpp | Backfill 执行 |
| storage/tablet/ | ob_tablet_transfer_tx_ctx.h/cpp | TransferOut/MoveTx/DestPrepare Ctx |
| | ob_tablet_start_transfer_mds_helper.cpp | StartTransferIn/Out/Prepare Helper |
| | ob_tablet_finish_transfer_mds_helper.cpp | FinishTransferIn/Out Helper |
| | ob_tablet_abort_transfer_mds_helper.cpp | AbortTransfer Helper |
| | ob_tablet_create_delete_mds_user_data.h | DataType/Status 枚举 |
| | ob_tablet_status.h | TabletStatus 枚举 |
| storage/multi_data_source/ | compile_utility/mds_register.h | MDS 类型注册表 |
| | ob_multi_data_source.cpp | MDS 分发逻辑 |
| | ob_start_transfer_in_mds_ctx.h/cpp | StartTransferIn Ctx |
| | ob_finish_transfer_in_mds_ctx.h/cpp | FinishTransferIn Ctx |
| | ob_abort_transfer_in_mds_ctx.h/cpp | AbortTransferIn Ctx |
| storage/ls/ | ob_ls_transfer_status.h/cpp | Transfer Status 弱读控制 |
| | ob_ls_wrs_handler.cpp | 弱读时间戳推进 |
| storage/tx_table/ | ob_tx_table.h/cpp | enable/disable 弱读推进 |
| | ob_tx_data_table.cpp | latest_transfer_scn 保护 |
| storage/tx/ | ob_trans_part_ctx.cpp | 事务上下文 (transfer_out/move_tx/log_cb) |
| | ob_tx_replay_executor.cpp | 回放执行器 |
| | ob_tx_log.cpp | Replay Barrier 定义 |
| rootserver/ | ob_balance_task_execute_service.cpp | Balance Task 执行 |
| | ob_primary_ls_service.cpp | LS 删除 |
| share/balance/ | ob_balance_task_table_operator.h/cpp | Balance Task 状态机/Helper |
| observer/ | ob_rpc_processor_simple.cpp | LS 创建 RPC |
| logservice/ | ob_garbage_collector.cpp | LS GC |
| storage/tx_storage/ | ob_ls_service.cpp | LS 本地管理 |

### B. BufferCtx 回调复杂度对比表

| 类 | on_redo | on_commit | on_abort | 重试策略 |
|----|---------|-----------|----------|---------|
| ObTransferOutTxCtx | MdsCtx + transfer_out_tx_op | MdsCtx + transfer_out_tx_op | 条件(do_transfer_block_) | while(true), 10ms |
| ObTransferMoveTxCtx | move_tx_op + update_status | move_tx_op + update_status | move_tx_op + update_status | while(true), 10ms |
| ObTransferDestPrepareTxCtx | update_status | update_status + enable弱读 | update_status + enable弱读 | while(true), 10ms |
| ObStartTransferInMdsCtx | MdsCtx | MdsCtx | MdsCtx + empty_shell_trigger | MdsCtx 默认 |
| ObFinishTransferInMdsCtx | 记录redo_scn + MdsCtx | MdsCtx | MdsCtx | MdsCtx 默认 |
| ObAbortTransferInMdsCtx | 记录redo_scn + MdsCtx | MdsCtx | MdsCtx | MdsCtx 默认 |

### C. Replay Barrier 完整对比表

| MDS 类型 | ID | redo | commit | abort | 选择原因 |
|---------|-----|------|--------|-------|---------|
| START_TRANSFER_OUT_PREPARE | 25 | PRE | - | - | 仅修改 tablet status |
| START_TRANSFER_OUT_V2 | 26 | STRICT | STRICT | STRICT | 事务阻塞 |
| TRANSFER_MOVE_TX_CTX | 27 | STRICT | STRICT | STRICT | 事务搬迁 |
| START_TRANSFER_IN | 21 | STRICT | STRICT | STRICT | 创建 tablet |
| TRANSFER_DEST_PREPARE | 28 | - | - | - | 仅弱读控制 |
| FINISH_TRANSFER_IN | 23 | STRICT | - | - | 恢复 NORMAL |
| FINISH_TRANSFER_OUT | 22 | PRE | - | - | 标记删除 |
| TRANSFER_IN_ABORTED | 32 | - | - | - | 仅验证清理 |

PRE = PRE_BARRIER（drain 后可并发），STRICT = STRICT_BARRIER（完全串行）

### D. 已知局限与待确认项

1. **代码行号时效性**: 所有行号基于 2026-04 版本源码，随版本变化可能过期。
2. **SS 回填检查原子性**: `has_transfer_table()` 返回 false 时共享存储写入是否一定完成，依赖 `ObTransferReplaceTableTask` 中两步操作的串行性保证。`is_shared_storage` 变量已声明但未使用，可能存在后续 SS 特殊检查的待开发需求。
3. **TRANSFER_IN_ABORTED skip_replay**: 当前代码中 Follower 回放总是跳过 tablet 级别的重放检查（skip_replay 初始值为 true），需确认此行为是否符合预期。
4. **故障场景覆盖不足**: 本文未深入讨论以下场景: Leader 切换期间 Transfer 的行为、网络分区对 RPC 检查的影响、并发 Transfer 和 MERGE 的交互。
5. **on_register 5s 超时**: ObTransferMoveTxCtx 的 on_register 有 5s 超时（唯一有超时限制的回调），超时后清理已创建的事务上下文并返回失败。


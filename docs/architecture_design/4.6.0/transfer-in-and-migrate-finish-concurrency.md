# 迁移 × Transfer-In 并发关键技术点

> 聚焦 "LS migration 与 transfer-in 并发场景下, dst LS 上每一条 TRANSFER_IN_START 日志如何被安全消费" 这一问题。整理当前代码中的各道防线与各时间窗口的安全性论据, 作为并发设计速查手册。


## 一、基础概念

### 1.1 两个独立 DAG / 任务

| 名称 | 主体 | 作用 | 入口代码 |
|------|------|------|----------|
| Migration (LS 迁移) | 单 LS 在副本级别移到另一个 observer | 把 LS 的成员、日志、所有 tablet 带到 dest observer, 最后切 member_list | `ObLSMigrationHandler`, `ObLSCompleteMigrationDagNet` |
| Transfer (tablet 组转移) | 一组 tablet 从 src LS 改挂到 dst LS | 跨 LS 搬 tablet, 两侧各写 MDS (TRANSFER_OUT / TRANSFER_IN) + 数据 backfill | `ObTransferHandler`, `ObTabletStartTransferIn/OutHelper`, `ObTransferWorkerMgr` |

共享资源面: `ObTabletMeta`/`ObMigrationTabletParam` 和 `ObMigrationStatus`。

### 1.2 关键状态

**`ObMigrationStatus`** (按语义分组):

- DOING 组: `ADD` / `MIGRATE` / `REBUILD` / `REPLACE` — 正在搬数据
- WAIT 组: `xxx_WAIT` — 数据搬完、等 change member list
- 收尾组: `HOLD`, `NONE` — 已完成或无迁移
- 其它: `FAIL`/`GC`/`RESTORE_STANDBY` 等

**Tablet 相关状态**:

- `ObTabletStatus`: `NORMAL` / `TRANSFER_OUT` / `TRANSFER_IN`
- `ObTabletHAStatus`: `data_status = INCOMPLETE | COMPLETE`, `has_transfer_table = true | false`


## 二、关键代码点速查

| 编号 | 职责 | 位置 |
|------|------|------|
| **迁移 DAG (dst LS)** | | |
| S1 | `ObWaitDataReadyTask::process()` 迁移主流程 | ob_ls_complete_migration.cpp:1263 |
| **CP1** | `check_all_tablet_ready_()` — 等全部 tablet `data_status=COMPLETE` | ob_ls_complete_migration.cpp:2473 |
| S3 | `update_ls_migration_status_wait_()` — 进 WAIT | ob_ls_complete_migration.cpp:2404 |
| **CP2** | `wait_transfer_table_replace_()` — 等全部 tablet `has_transfer_table=false` | ob_ls_complete_migration.cpp:1565 |
| S5 | `update_ls_migration_status_hold_()` — 进 HOLD | ob_ls_complete_migration.cpp:2441 |
| **Transfer-In (dst 侧 replay gate 链)** | | |
| I0 | `on_replay()` 入口 | ob_tablet_start_transfer_mds_helper.cpp:1625 |
| I1 | `check_can_replay_redo_log_for_restore_()` — restore + migration 复合 gate | ob_tablet_start_transfer_mds_helper.cpp:2362 |
| **I2** | `check_can_replay_redo_log_for_migration_()` — 纯 migration gate (GTS 守门) | ob_tablet_start_transfer_mds_helper.cpp:2329 |
| I3 | `on_replay_success_()` — 实际建 tablet | ob_tablet_start_transfer_mds_helper.cpp:1723 |
| I4 | `check_transfer_dest_ls_status_()` — COMMIT 路径 DOING 组 skip src 检查 | ob_tablet_start_transfer_mds_helper.cpp:1882 |
| I5 | `check_transfer_src_tablets_()` — COMMIT 路径源端强校验 | ob_tablet_start_transfer_mds_helper.cpp:1293 |
| **Backfill (dst 侧常驻)** | | |
| B1 | `ObTransferWorkerMgr::process()` | ob_transfer_backfill_tx.cpp:349 |
| B3 | `check_source_tablet_ready_()` — 源端 LS/tablet/transfer_seq 校验 | ob_transfer_backfill_tx.cpp:253 |
| B4 | `update_transfer_tablet_normal_()` — 置 NORMAL + `has_transfer_table=false` | ob_tablet_finish_transfer_mds_helper.cpp:1124 |


## 三、Tablet 生命周期 (并发视角)

```
  src LS                                 dst LS
  ------                                 ------

  NORMAL                                 (not exist)
    |
    | transfer_start (leader 2PC on src + dst)
    v                                       |
  TRANSFER_OUT (empty shell)                v
    |                                    TRANSFER_IN
    |                                    data_status      = INCOMPLETE
    |                                    has_transfer_table = true
    |                                       |
    |                                       |  backfill worker (常驻)
    |                                       |  pulls minor/major from src
    |                                       v
    |                                    has_transfer_table = false
    |                                    data_status        = COMPLETE
    |                                       |
    |      transfer_finish (leader 2PC)     |
    +------------------+--------------------+
                       v
            src: GC empty shell   dst: NORMAL
```

**并发要点**: backfill worker 常驻, 与迁移 DAG 独立推进。迁移 DAG 只"等当前已建的 tablet 达到 data_status=COMPLETE / has_transfer_table=false"; backfill 的实际进度则依赖源端 LS 稳定。


## 四、dst 侧核心流程

### 4.1 迁移 DAG (dst observer 视角)

```
+=======================================================================+
|           ObWaitDataReadyTask::process() — single thread              |
+=======================================================================+

  [ migration_status = ADD/MIGRATE/REBUILD/REPLACE ]  (DOING phase)
                         |
                         v
              wait_log_sync_()
                         |
                         v
              wait_log_replay_sync_()
                         |
                         v
          +---------------------------------+
          | check_all_tablet_ready_()       |  <==== Check Point 1 (CP1)
          |   for each tablet in LS         |
          |     wait data_status = COMPLETE |
          |   collect max_minor_end_scn     |
          +---------------------------------+
                         |
                         v
              wait_log_replay_to_max_minor_end_scn_()
                         |
                         v
              update_ls_migration_status_wait_()          ======= T1 =======
                         |
                         v
  [ migration_status = xxx_WAIT ]  (WAIT phase)
                         |
                         v
          +------------------------------------+
          | wait_transfer_table_replace_()     |  <==== Check Point 2 (CP2)
          |   for each tablet in LS            |
          |     if has_transfer_table == true  |
          |       get src_ls / src_tablet      |
          |       check transfer_seq match     |
          |       wait until replaced          |
          +------------------------------------+
                         |
                         v
              update_ls_migration_status_hold_()          ======= T2 =======
                         |
                         v
              change_member_list_with_retry_()
                         |
                         v
                        done                              ======= T3 =======
```

### 4.2 Transfer-In Redo 回放 gate 链

```
+=======================================================================+
|  on_replay(buf, scn, ctx) — dst LS replay transfer_in_start redo log  |
+=======================================================================+

  deserialize ObTXStartTransferInInfo
                         |
                         v
  get_migration_and_restore_status_()         // 一次性读取 migration+restore
                         |
                         v
  check_can_replay_redo_log_for_restore_()    // restore + migration 组合
                                              // restore_in_progress && status!=NONE
                                              // 额外校验 src LS 可达性 / max_decided_scn
                         |
                         v
  +-------------------------------------------------------------------+
  | check_can_replay_redo_log_for_migration_    (纯 migration 分支)   |
  |   if status == NONE                -> pass                        |
  |   get_gts(tenant_id, gts)                                         |
  |   if gts >= scn                    -> pass                        |
  |   if gts <  scn                    -> OB_EAGAIN, 外层重试         |
  +-------------------------------------------------------------------+
                         |
                         v
  check_can_skip_replay_()     // 若 tablet 已建好则 skip_replay=true
                         |
                         v
         skip_replay ? return  :  on_replay_success_()
                                    ├─ check_transfer_dest_tablets_
                                    └─ create_transfer_in_tablets_
                                         new tablet:
                                           tablet_status     = TRANSFER_IN
                                           data_status       = INCOMPLETE
                                           has_transfer_table = true
```

**gate 语义小结**: 两个 `_for_*_` 函数同向互补, 入口都是 `migration_status != NONE`:
- `_for_restore_`: restore 场景分支, 比较 `gts < scn_dec(scn)`, 额外做 src LS 可达性与回放进度校验。
- `_for_migration_`: 非 restore 场景分支, 比较 `gts < scn`, 由 backfill + CP1/CP2 兜底源端校验。

### 4.3 Transfer-In COMMIT 分支 (二次源端校验)

```
+=======================================================================+
|           replay commit of transfer_in_start on dst LS                |
+=======================================================================+

  check_can_skip_check_transfer_src_tablet_
                         |
                         v
  +-------------------------------------------------------+
  | dest tablets already ready          -> skip = true    |
  | dest in restore AND scn<=consistent -> skip = true    |
  | gts not pushed (gts <= scn)         -> skip = false   |
  |                                                       |
  | migration_status in DOING group                       |
  |   (ADD / MIGRATE / REBUILD / REPLACE) -> skip = true  |
  |                                                       |
  | migration_status in WAIT / HOLD / NONE / 其余         |
  |                                       -> skip = false |
  +-------------------------------------------------------+
                         |
                         v
    if !skip:  check_transfer_src_tablets_()
                         |
                         v
                      proceed
```

### 4.4 Backfill worker (与迁移 DAG 并行)

```
  ObTransferWorkerMgr::process()
        |
        v
  get_need_backfill_tx_tablets_
    filter: tablet_status == TRANSFER_IN
            AND has_transfer_table == true
            AND ha_status.data_status_complete()
        |
        v
  per tablet:
    check_source_tablet_ready_
      if src_ls.migration_status not in {NONE, GC}:  skip
      if src.max_decided_scn < transfer_start_scn:   retry
      if src_transfer_seq != dst_transfer_seq - 1:   retry
        |
        v
    schedule backfill (copy minor/major from src)
        |
        v
    update_transfer_tablet_normal_
      tablet_status        -> NORMAL
      has_transfer_table   -> false
      data_status          -> COMPLETE
```


## 五、并发交互矩阵

| src LS 迁移态 | dst LS 迁移态 | 结果 |
|---------------|---------------|------|
| NONE | NONE | 稳态 transfer, 全量路径直通 |
| NONE | DOING | I2 gate: `gts<scn` 时 OB_EAGAIN 重试; 放行后 I4 判 DOING 直建 TRANSFER_IN; 随后 CP1 等 `data_status=COMPLETE`, CP2 等 `has_transfer_table=false` |
| NONE | WAIT | I2 gate 继续按 GTS 守门; I4 判 WAIT 必须 check src; CP2 再次迭代 |
| NONE | HOLD / NONE | 走稳态路径 |
| DOING | NONE | transfer 发起不被阻止; backfill B3 跳过直到 src 迁移转 NONE/GC |
| DOING | DOING | 两侧都在迁: I2 gate 卡 dst; 放行后建 tablet, 但 B3 卡源端; dst CP1 等 data_status 一直不 COMPLETE |
| WAIT | any | O1 不阻止; backfill 按 B3 判 `src migration ∈ {NONE,GC}` 才启动 |

**三个关键观察**:

1. **Transfer 发起阶段不对 `src_ls.migration_status` 做拦截**。系统有意把"源端正在迁移"推给 backfill 阶段延迟处理 (B3 过滤)。
2. **I2 gate 只在 dst LS 处于迁移中才会触发 GTS 比较**; `migration_status = NONE` 时直接放行, 不阻塞稳态回放。
3. **backfill worker 与迁移 DAG 独立**: 迁移 DAG 只关心"当前已建的 tablet 是否达到 COMPLETE / has_transfer_table=false"; 真正搬数据由常驻 worker 完成。


## 六、三道防线

从"新到一条 TRANSFER_IN_START 日志"的视角, 正确性由三道防线组合保障:

### 6.1 第一道: I2 入口 gate

```
check_can_replay_redo_log_for_migration_
  if migration_status != NONE && gts < scn: OB_EAGAIN
```

**作用**: 拦截 "dst 正在迁移、且 transfer 的 `start_scn` 比当前 GTS 还新" 的反常回放。只锁 **未被全局时钟观察到** 的 transfer, 常态回放直接放行, 不影响正常 standby 速度。

### 6.2 第二道: I4 COMMIT 路径对称检查

```
migration_status in {ADD, MIGRATE, REBUILD, REPLACE}  -> skip src check  (DOING)
migration_status in {xxx_WAIT, HOLD, NONE, ...}       -> must check src  (WAIT+)
```

**作用**: DOING 期允许快速建 tablet, 由 CP1/CP2 统一再把关; WAIT 之后的新 transfer 必须在 commit 时确认源端 tablet 真实存在, 防止"dst tablet 已现身 + src 已消失"的断链。

### 6.3 第三道: CP1 + CP2 双迭代

- **CP1** `check_all_tablet_ready_`: 进 WAIT 前扫全部 tablet, 等 `data_status=COMPLETE`, 同时收集 `max_minor_end_scn`。
- **CP2** `wait_transfer_table_replace_`: WAIT 期间再扫一遍, 等 `has_transfer_table=false` + 源端 tablet/transfer_seq 校验。

两次迭代都调用 `build_tablet_iter` 取 LS **当前** tablet 集合, 所以迭代期间新建的 tablet 一定被覆盖。


## 七、按时间窗口的安全性论证

### 7.1 窗口划分示意图

```
time ─────────────────────────────────────────────────────────────────►

迁移 DAG 状态转换:

  DOING phase              T1     WAIT phase         T2    HOLD     T3  NONE
  ADD/MIGRATE/REBUILD/   ══════► xxx_WAIT         ══════► HOLD   ══════►
  REPLACE

关键动作锚点:
  wait_log_sync_                  wait_transfer_           change_member_
  wait_log_replay_sync_           table_replace_  (CP2)    list_with_retry_
  check_all_tablet_ready_ (CP1)
  wait_log_replay_to_max_minor_
  update_ls_migration_status_wait_

                                       CP2 done
  ├──────── 窗口 A ─────────┼─── 窗口 B ────┼─ C ─┼──── 窗口 D ────┤
  │                         │              │     │                │
  start                     T1             *     T2              T3

各窗口内, 新到 TRANSFER_IN_START 的处理策略:

  窗口 A  (DOING)
    ├─ I2 gate   : pass (常态 replay)
    ├─ I4 (COMMIT): skip src check (DOING 组)
    └─ 兜底       : CP1 统一等 data_status=COMPLETE

  窗口 B  (WAIT + CP2 迭代进行中)
    ├─ I2 gate   : gts<scn -> OB_EAGAIN 重试
    ├─ I4 (COMMIT): must check src tablet
    └─ 兜底       : CP2 再次扫到, 等 has_transfer_table=false

  窗口 C  (CP2 迭代完成 ~ T2, 极窄)
    ├─ I2 gate   : 同窗口 B
    ├─ I4 (COMMIT): 同窗口 B
    └─ 兜底       : 迁移 DAG 不再等, 常驻 backfill worker 接棒

  窗口 D  (HOLD / NONE)
    ├─ I2 gate   : NONE 放行; HOLD 同 WAIT
    ├─ I4 (COMMIT): must check src
    └─ 兜底       : 稳态 transfer 路径 + backfill worker
```

### 7.2 逐窗口论证

**窗口 A (T1 之前, DOING phase)**

1. 新到 TRANSFER_IN_START 回放: I2 gate 常态 replay 下放行; I4 判 DOING skip src 检查, 直建 tablet。
2. 迁移主流程紧接着 `wait_log_replay_sync_` 再同步一次, 保证 `log_sync_lsn_` 之前的 transfer-in 全部落地。
3. 进入 CP1, 枚举全部 tablet, 等每一个 `data_status=COMPLETE` (含刚建的 TRANSFER_IN tablet, 由 backfill worker 独立推进)。

→ **窗口 A 内建立的 transfer-in tablet 必在 CP1 完成后才能推进**。

**窗口 B (T1 ~ CP2 迭代完成, WAIT phase)**

1. 新到 TRANSFER_IN_START 回放: I2 gate 卡 `gts<scn`; `gts>=scn` 放行。
2. I4 判 WAIT 必须 check 源端 tablet, 否则 commit 失败重试。
3. CP2 重新迭代全部 tablet, 对 `has_transfer_table=true` 的 tablet 做源端 LS + transfer_seq 校验, 等到 `has_transfer_table=false`。

→ **窗口 B 内新建的 tablet 必被 CP2 捕获并等到 replace 成功**。

**窗口 C (CP2 迭代完成 ~ T2, 极窄)**

1. CP2 已退出, 状态仍是 WAIT。
2. I2 gate 继续按 `gts<scn` 守门。
3. I2 gate 放行后, I4 仍不可 skip src 检查, commit 路径确认 src tablet 存在。
4. 新建的 tablet 属性 `has_transfer_table=true, data_status=INCOMPLETE`, 源端已被校验存在。
5. 迁移 DAG 不再等这条 tablet, 但常驻 backfill worker 会继续把它搬完; 功能上等同于"加入集群后收到新 transfer 的普通 LS"。

→ **窗口 C 不会产生孤儿 tablet**: 源端必在, 回填最终完成。

**窗口 D (T2 之后, HOLD / NONE)**

1. HOLD 短暂后转 NONE。
2. HOLD/NONE 下 I4 仍不 skip src 检查; I2 gate 在 NONE 时直接放行。
3. 迁移语义已结束, 后续 transfer 走稳态路径 + 常驻 backfill。

### 7.3 对源端并发的补充

- **Src LS 正处于迁移 (DOING/WAIT)**: 发起新的 transfer 并不被阻止, 但 backfill B3 持续跳过这些 tablet, 直到 src LS 迁移转 NONE/GC 后才开始回填。在此期间 dst 侧 CP1 会一直等 `data_status=COMPLETE`, 实际表现是 dst 迁移被 **源端 LS 迁移间接阻塞** — 即"两边迁移串行化"。
- **Src LS 在 backfill 途中进入 GC**: B3 把 `GC` 视为合法源态, 因为 GC 阶段仍能读取源端 tablet 数据; 但 GC 完成后源端 tablet/LS 消失, 因此 backfill 必须在 GC 结束前拿到数据, 由 `transfer finish` 2PC 保证时序。


## 八、安全结论

1. **不漏**: 迁移 DAG 转 HOLD 的唯一路径必须依次通过 CP1 和 CP2, 两者都用 `build_tablet_iter` 迭代 LS 当前持有的全部 tablet; 任何在 T1 前、以及 T1 → CP2 迭代结束前建出的 transfer-in tablet 都被覆盖。
2. **不错**: COMMIT 路径依 DOING/WAIT 分组对称处理: DOING 期快速放行、后续两次检查补齐; WAIT 后立刻强校验源端, 不会出现"目的端 tablet 已经出现, 但源端不存在"的断链状态。
3. **不早**: I2 gate 禁止"GTS 尚未覆盖 `start_scn` 的 transfer 在迁移 dst 上建 tablet", 避免与迁移 tablet 列表的语义竞争。
4. **可回填**: 窗口 C 的极窄缝隙里即便新建了 tablet, 也有源端校验 + 常驻 backfill 兜底, 不依赖迁移 DAG 自己收尾。
5. **源端独立闭环**: 源端 LS 处于迁移态时, backfill B3 主动让路, 把新 transfer 的回填推迟到源端稳定之后; 代价是 dst 迁移可能被源端迁移间接阻塞, 但不会丢数据。

综上, 在流程层面 **迁移期间 dst LS 承认的每一条 transfer-in tablet, 终将通过 CP1/CP2 显式等待、I2/I4 gate 拒绝回退、或 backfill 最终完成三者之一收敛到一致状态**; 迁移 DAG 终止的那一刻, dst LS 不会留下"应有数据但未回填"的 tablet。


## 九、运维边界与显性假设

1. **源端 LS 生命周期假设**: 同一个 transfer 对应的源端 LS 不应在目的端迁移/backfill 结束前被整体 GC; 这是 transfer + LS 生命周期模型的整体约束, 由 RS 级 transfer 任务调度保证, 单独看本流程无法防御。
2. **GTS 推进假设**: I2 gate 是自旋重试 (OB_EAGAIN), 依赖 GTS 单调推进; 上游 tenant GTS 卡住时会表现为 "MIGRATE_WAIT 进不去 HOLD", 属 "GTS 不推高" 通用问题。
   - **性能注意**: `ObTransferUtils::get_gts` 内部是 10s 阻塞轮询循环, replay 上下文下最长可阻塞 MDS replay 线程 10s, 需关注线程吞吐。
3. **CP2 源端 LS 不存在**: 返回 `OB_TRANSFER_SRC_LS_NOT_EXIST` 允许重试; 10min 超时会触发迁移任务失败由上层重调。
4. **src + dst 同时迁移**: B3 强制 `src migration ∈ {NONE, GC}` 才启动 backfill, 形成主动串行化, 吞吐受限但数据不丢。
5. **升级兼容性**: 混合版本集群中, 未引入 I2 gate 的老版本副本在迁移中仍会过早建 tablet; 需要先升级所有副本再放开并发 transfer + migration。

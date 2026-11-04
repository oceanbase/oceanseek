# OceanBase ss 模式 · 新人入门手册

> - 代码基线：master b5909bf5399d36c06932c9189fbae3f61cc029a4（启用 `OB_BUILD_SHARED_STORAGE` 宏）
> - 目标读者：刚加入团队、要做 ss（shared storage，共享存储）方向的同学
> - 阅读时间：≈ 50 分钟
> - 初稿时间：2026-05-25；最近复核：2026-11-02

读完这本手册，你应能回答：

- **Phase 1 · 宏观地图**：OceanBase observer 进程内有哪些组件？RootService / SQL / 存储引擎 / Compaction / GC 这 5 个子系统在 ss 下分别做哪些事？
- **Phase 2 · ss 模块细节**：ss 模块远端 / 本地分别放了什么？写一个 macro 块走过哪几个函数？


| Phase | 页                                           | 主题                       | 估读   |
| ----- | ------------------------------------------- | ------------------------ | ---- |
| 宏观    | [Page 1](#page-1--oceanbase-observer-整体架构图) | OceanBase observer 整体架构图 | 5 分钟 |
| 宏观    | [Page 2](#page-2--必备词汇10-张术语卡)              | 必备词汇（10 张术语卡）            | 5 分钟 |
| 宏观    | [Page 3](#page-3--集群层rootservice--ss)       | 集群层：RootService × ss     | 5 分钟 |
| 宏观    | [Page 4](#page-4--用户访问层sql--存储引擎--ss)       | 用户访问层：SQL + 存储引擎 × ss    | 5 分钟 |
| 宏观    | [Page 5](#page-5--后台任务一compaction--ss)      | 后台任务一：Compaction × ss    | 6 分钟 |
| 宏观    | [Page 6](#page-6--后台任务二gc--ss)              | 后台任务二：GC × ss            | 5 分钟 |
| ss 细节 | [Page 7](#page-7--ss-远端--本地布局)              | ss 远端 + 本地布局             | 5 分钟 |
| ss 细节 | [Page 8](#page-8--写宏块的最短调用链)                | 写宏块的最短调用链                | 4 分钟 |


详细字典请配合同目录的 [directory-structure.md](directory-structure.md) 使用。

---

# Phase 1 · 宏观地图

> 这一阶段先看"OceanBase observer 长什么样、5 大子系统在 ss 下各司什么职"。读完后你能在脑子里画出 observer 的全貌，知道 ss 模块只是其中的一个组件——但是新人方向所在的那个组件。

---

## Page 1 · OceanBase observer 整体架构图

一台 OceanBase 服务节点上跑着一个 **observer 进程**。这个进程内部由 5 个组件组成，外部连接远端对象存储和本地盘：

```
                    OceanBase observer 进程  (一个 OB 节点)

   ───────────────────────────────────────────────────────────────
   组件 1   ▸  集群层 / RootService (RS)
                建租户目录 / 颁发 reorganization_scn / 写删除标记
   ───────────────────────────────────────────────────────────────
   组件 2   ▸  用户访问层 / SQL 引擎 → 存储引擎
                SQL 引擎对 ss 透明
                存储引擎调 ObTableScanIterator → ObTablet → ss 模块
   ───────────────────────────────────────────────────────────────
   组件 3   ▸  IO 层 / ss 模块  (本书 Phase 2 主角)
                src/storage/shared_storage
                门面      ObSSObjectAccessUtil
                抽象      ObSSBaseReader / ObSSBaseWriter
                租户管家  ObTenantFileManager
                目录路由  ObDirManager
                Cache 三件套：
                   Macro Cache       本地 SSD       ObSSMacroCacheMgr
                   Micro Cache       本地 SSD       ObSSMicroCache
                   Mem Macro Cache   observer 内存  ObSSMemMacroCache
                对象门面  ObObjectManager → ObSSObjectAccessUtil
   ───────────────────────────────────────────────────────────────
   组件 4   ▸  后台任务一 / Compaction (compaction_v2)
                合并产物上传共享区，多副本共看一份
                ObSSScheduleTabletFunc / ObLinkSSMajorSSTableDag
   ───────────────────────────────────────────────────────────────
   组件 5   ▸  后台任务二 / GC（双线）
                远端  ObSSGCService + ObSSGCTaskRunner
                本地  ObTenantGCTask + ObBlockGCHandler
   ───────────────────────────────────────────────────────────────

   以上 5 个组件都在同一个 observer 进程内。
   它们的读 / 写最终都通过 ss 模块（组件 3）落地：

                            │ ObObjectManager → ObSSObjectAccessUtil
                            ▼

   远端对象存储 (OSS / S3 / OBS / 文件系统)

       <root>/cluster_<cid>/
           ├── server_<svr>/...     ← 节点私有区  (SLOG / ckpt / 私有 macro)
           └── tenant_<id>/...      ← 租户共享区  (sstable 共享 macro)
```

记住下面 5 件事，全书剩下的内容就是把它们各自展开：

1. **observer = OB 节点服务进程**；同时服务多个租户，每台机器上一个 observer。
2. **5 个组件分工明确**：RS 调度 / SQL+存储引擎处理用户请求 / ss 模块负责 IO / Compaction 与 GC 是后台任务。
3. **ss 模块（组件 3）是新人方向**——存储对象统一门面是 `ObObjectManager`，SS 分支经 `ObSSObjectAccessUtil` 处理 Cache 与对象设备访问。
4. **远端对象存储分私有区与共享区**：`server_<svr>/` 节点私有；`tenant_<id>/` 租户共享。
5. **本书读法**：Phase 1（Pages 1-6）横向扫一遍 5 大组件；Phase 2（Pages 7-8）纵向钻进 ss 内部；

---

## Page 2 · 必备词汇（10 张术语卡）

每张卡 1-2 句话；先扫一遍建立词汇感，遇到再回查。

### 数据组织相关 5 张

1. **租户 (tenant)**：OceanBase 的多租户隔离单元，一台 observer 同时服务多个租户；每个租户有自己的内存、磁盘配额、表数据。
2. **LS (Log Stream，日志流)**：租户内的复制 / 一致性单元，一个租户由多个 LS 组成，每个 LS 在多副本之间同步 redo 日志。
3. **tablet**：表 / 分区的物理存储分片，一个 LS 内挂载很多 tablet。
4. **sstable**：tablet 的持久化数据载体；按 mini / minor / major / inc_major 等层级组织（mini/minor 增量小、合并频繁；major 是全量合并产物；inc_major 是增量 major——仅含自上次 major 以来的变化），一个 tablet 由多个 sstable 叠加而成。
5. **macro 块**：sstable 内最小的 IO 单位（默认 2MB）。**ss 模式下，"上传 / 下载到对象存储"的就是它**。

### 进程内的关键服务组件 5 张

1. **RootService (RS)**：集群"大脑"，跑在某个 observer 的 500 系统租户里；负责 schema 变更、ddl、负载均衡、主备切换、tenant / LS / tablet 调度。**新人记一句**：决定"东西放在哪台机器"的就是它。
2. **SQL 引擎**：解析、优化、执行 SQL；要读取数据时调用下面的"存储引擎"。
3. **存储引擎 (storage engine)**：管理 LS / tablet / memtable / sstable / 事务的层。**ss 模块就被它调用**——所有"读写一个 macro 块"的请求最终落到 ss。
4. **Compaction（合并）**：把多个较小的 sstable / memtable 合并为一个大 sstable 的后台流程；ss 下额外要把合并产物上传到远端共享区，对应代码在 `src/storage/compaction_v2/`。
5. **GC (Garbage Collection，垃圾回收)**：用不到的数据要在远端 / 本地清理掉，对应代码分两处：
  - 远端共享对象的 GC：`src/storage/ss_garbage_collector/`（服务壳 `ObSSGCService`，任务编排 `ObSSGCTaskRunner`）。
    - 本地 cache 的回收：`src/storage/shared_storage/ob_block_gc_handler.{h,cpp}`、`task/ob_gc_*_task.{h,cpp}`。

> **精读优先级**：上面 10 张卡里，**第 5、6、8、9、10 这五张**会在 ss 代码里反复见到，先牢记；1-4 是基础名词，遇到再回查。

---

## Page 3 · 集群层：RootService × ss

### 一句话定位

**RootService（RS）= 集群的"调度大脑"**。它跑在某个 observer 的 sys系统租户里，负责 schema 变更、ddl、负载均衡、租户的创建 / 删除、LS / tablet 的调度。**RS 决定"东西放在哪台机器、哪个租户目录"**，但不直接读写 macro 块。



备注:

- `sys` 租户：`tenant_id = 1`，正式的系统租户，保存集群元数据、系统表、租户/资源/LS 等管理信息。
- `500` 租户：`OB_SERVER_TENANT_ID = 500`,它主要承载 observer 进程级别的内部资源和节点私有元数据，比如 `server_slog/`、`server_ckpt/`、`SERVER_META`、一些内存归属等。

### 在 ss 下，RS 触发以下几件事

1. **租户服务启动** → 创建本地 cache 的 `<tenant_id>_<tenant_epoch>/` 目录并写入 io_detect_file。入口在 `ObTenantFileManager::start()`，关键调用：
  ```cpp
   // src/storage/shared_storage/ob_file_manager.cpp:977
   } else if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(tenant_id_, MTL_EPOCH_ID()))) {
  ```
   建目录的具体逻辑在 `ObDirManager::create_tenant_dir()`（声明：`src/storage/shared_storage/ob_dir_manager.h:56`，实现：`src/storage/shared_storage/ob_dir_manager.cpp:374-565`）。它基于 `get_local_cache_root_dir()` 创建本地租户目录，再创建 ls/tablet_data/tmp_data/slog、mini/minor/inc_major cache、tablet sub meta cache 与 external_table_file。远端 `tenant_<id>/` 与 `TENANT_ROOT_KEY.T22` 不是这一步创建。
2. **租户删除** → 写入 `IS_SHARED_TENANT_DELETED.T18` 标记到 `tenant_<id>/`，告诉所有 observer "这个租户已废"。这是 500 系统租户专属的写入路径：
  ```cpp
   // src/storage/shared_storage/ob_ss_object_access_util.cpp:174-180
   } else if (OB_SERVER_TENANT_ID == write_info.mtl_tenant_id_) { // case1: 500 tenant
     // 500 tenant only writes SERVER_META and IS_SHARED_TENANT_DELETED,
     // which occupies small disk space that can be ignored.
     // therefore, no need to alloc file size, directly write file.
     if (OB_FAIL(file_manager->async_write_file(write_info, object_handle))) {
  ```
3. **tablet 重组（split / transfer）** → 颁发 `reorganization_scn`，让远端共享区按 `tablet/<id>/<reorganization_scn>/` 多版本隔离新老数据。`reorganization_scn` 实际取自 tablet 的 transfer_start_scn：
  ```cpp
   // src/storage/tablet/ob_tablet.h:752
   share::SCN get_reorganization_scn() const { return tablet_meta_.transfer_info_.transfer_start_scn_; }
  ```
   这就是为什么所有用户 tablet 共享路径都嵌一层 `<reorganization_scn>`——多版本隔离避免新老数据撞路径。例如 tablet 100 经历一次 split 后，旧数据路径 `tablet/100/500/` 和新数据路径 `tablet/100/600/` 并存，旧路径由 GC 择机回收。

### 这一页要记住的核心点

> **RS 给 ss 发"号令"，ss 落地**。RS 只决策"建目录 / 写标记 / 颁发 SCN"，不下场读写 macro 块。新人短期内很少需要直接改 RS 代码；遇到"租户级 / LS 级"问题，先看 RS 状态机、再看 ss 模块如何响应。

---

## Page 4 · 用户访问层：SQL + 存储引擎 × ss

### 一句话定位

**SQL 引擎**做查询的解析 / 优化 / 执行；**存储引擎**做 LS / tablet / sstable / 事务的管理。**SQL 引擎 → 存储引擎 → ss 模块** 这条链路是用户 SELECT 抵达 ss 的唯一通道。

### SQL 引擎对 ss 是"透明"的

`src/sql/` 里**没有 ss 专属代码**——SQL 优化器 / 执行器看不到"远端对象存储"这个概念，它们只知道"通过存储引擎拿一行数据"。

**新人的实操结论**：做 ss 方向的前 0-3 个月可优先跳过 `src/sql/`，把精力放在存储引擎层及其下方的 ss 模块；遇到执行计划相关问题再回头补。

### 一条 SELECT 抵达 ss 的简化路径

```
SQL 解析 / 优化 / 执行计划                                (src/sql/ ... 不展开)
                  │
                  │   执行计划已确定目标 ls_id + tablet_id（按分区路由）
                  ▼
ObLSService::get_ls(ls_id, handle, mod)                   ── 找到承载该数据的 LS
   src/storage/tx_storage/ob_ls_service.h:42（类） / :103（方法）
                  │
                  │   返回 ObLSHandle / ObLS（Log Stream，租户内的复制 / 一致性单元）
                  ▼
ObLSTabletService::get_tablet(tablet_id, handle, ...)     ── LS 内查 tablet（后两参 timeout / mode 有默认值）
   src/storage/ls/ob_ls_tablet_service.h:122（类） / :225（方法）
                  │
                  │   返回 ObTabletHandle（包住 ObTablet：元信息 + sstable 列表）
                  ▼
ObTableScanIterator::init(scan_param, tablet_handle, ...) ── 建迭代器开始扫（第三参 need_split_dst_table 默认 true）
   src/storage/access/ob_table_scan_iterator.h:42（类） / :47（init）
                  │
                  │   通过 ObTablet 遍历 sstable，定位要读的 macro 块
                  ▼
ObObjectManager::async_read_object()
   src/storage/blocksstable/ob_object_manager.h:462
                  │
                  │   存储对象统一门面；SS 下由 ObStorageObjectHandle 分派
                  ▼
ObStorageObjectHandle::ss_async_read()
   src/storage/blocksstable/ob_storage_object_handle.cpp:283
                  │
                  ▼
ObSSObjectAccessUtil::async_pread_file()
   src/storage/shared_storage/ob_ss_object_access_util.h:31
                  │
                  ▼
   由具体 reader 处理本地 Cache / 对象设备读取，Cache 与远端策略对上层透明
   （细节见 Phase 2 · Page 8）
```

### 存储引擎里与 ss 强相关的几个目录


| 路径                          | 做什么                                                          |
| --------------------------- | ------------------------------------------------------------ |
| `src/storage/tablet/`       | tablet 对象本体（`ObTablet`、`ObTabletHandle`）                     |
| `src/storage/access/`       | 读路径迭代器（`ObTableScanIterator` 等）                              |
| `src/storage/meta_store/`   | 存储元服务（`ObTenantStorageMetaService`），管理 tablet meta 的持久化      |
| `src/storage/meta_mem/`     | tablet meta 的内存缓存与生命周期管理                                     |
| `src/storage/blocksstable/` | macro 块数据结构与 IO 接口（`MacroBlockId` / `ObStorageObjectType` 等） |


举例：`ObTenantStorageMetaService::remove_tablet()` 接口要求传一个 `last_gc_version` 参数，说明 ss 给存储引擎的元信息接口加了"可回收版本号"语义——存储引擎要告诉 ss "这个 tablet 在哪个版本之后才能真正回收"：

```cpp
// src/storage/meta_store/ob_tenant_storage_meta_service.h:61-65
int remove_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletHandle &tablet_handle,
    const int64_t last_gc_version);   // ss 下用于标识"可回收水位"
```

### 这一页要记住的核心点

> 存储引擎是与 ss 接触最深的层，**对象读写公共门面是 `ObObjectManager`，SS 分支再由 `ObStorageObjectHandle` 委托 `ObSSObjectAccessUtil`**。新人想理解一条 SQL 怎么读到数据，从 `ObTableScanIterator` 起步、跟到对象门面为止，是性价比最高的代码导览路径。

---

## Page 5 · 后台任务一：Compaction × ss

### 一句话定位

**Compaction（合并）**：把多个较小的 sstable / memtable 合并成一个大 sstable 的后台流程，目的是减少层级、加速读取。**ss 下，Compaction 多出"上传到共享区 + 多副本协调可见性"两件事**，这部分代码集中在 `src/storage/compaction_v2/`。

### "compaction_v2" 是什么

不是要替换主合并代码（主合并仍在 `src/storage/compaction/`），而是 **ss 专属的扩展**。它负责：

1. 让多副本"看到同一份合并产物"（attach），避免每个副本各自跑一次合并、生成不同 macro 块；
2. 让本地 tablet "link"到远端共享 sstable，本地不存全量；
3. 调度上述两类任务，决定"什么时候 attach、什么时候 skip merge"。

### 三个值得新人先看的关键类

#### (1) `ObSSScheduleTabletFunc` ── 调度入口

```cpp
// src/storage/compaction_v2/ob_ss_schedule_tablet_func.h:16
struct ObSSScheduleTabletFunc final : public ObScheduleTabletFunc
{
  // MediumLoop 构造方式
  ObSSScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const bool check_skip_merge = false,
    const int64_t loop_cnt = 0);
  // SSWindowLoop 构造方式：使用外层 Writer DAG quota
  ObSSScheduleTabletFunc(
    const int64_t target_scn,
    int64_t &writer_schedule_quota,
    const int64_t loop_cnt);
  int post_process_ls() override;
  ...
private:
  int schedule_link_major_dag_(...);     // ★ 关键：调度共享 major 链接 DAG
};
```

它是 ss 下“决定 tablet 怎么合并”的总入口；最重要的私有方法 `schedule_link_major_dag_()` 负责把“链接共享 sstable”这件事提交给 DAG 调度器（`src/storage/compaction_v2/ob_ss_schedule_tablet_func.h:50-55`）。

#### (2) `ObLinkSSMajorSSTableDag` ── 共享 major 链接 DAG

```cpp
// src/storage/compaction_v2/ob_ss_link_major_dag.h:14
class ObLinkSSMajorSSTableDag : public share::ObIDag
{
  ...
  int create_first_task() override;
  ...
};
```

DAG 的 `create_first_task()` 会创建下面这个 `ObTabletSSLinkMajorTask`，并打开 prewarm 与上报，让本地 tablet 链接到 tenant 共享区里的同一个 major sstable。

#### (3) `ObTabletSSLinkMajorTask` ── 把本地 tablet 链接到共享 sstable

```cpp
// src/storage/compaction_v2/ob_ss_link_major_task.h:11-29
struct ObTabletSSLinkMajorTask : public ObITask
{
  ObTabletSSLinkMajorTask(const ObLSID &ls_id,
                          const ObTabletID &tablet_id,
                          const int64_t compaction_scn,
                          const int64_t merge_base_version,
                          const compaction::ObMergeType merge_type,
                          const bool prewarm,
                          const bool need_report);
  virtual ~ObTabletSSLinkMajorTask() { destroy(); }
  virtual int process() override;
private:
  int find_target_major_(...);   // ★ 关键：找到要链接的目标 major
};
```

它在远端共享区找到目标 major sstable（通过 `find_target_major_()`），让本地 tablet "认领"这个共享 sstable 而不是自己再合并一次。

> 增量 major（`SHARED_INC_MAJOR_*`）有对偶的 `ObTabletSSLinkIncMajorTask`（同目录下 `ob_ss_link_inc_major_task.h:16`）。

### ss 下 compaction 的关键差异（一句话）


| 点                  | ss 下做了什么                                                     |
| ------------------ | ------------------------------------------------------------ |
| 合并产物去哪             | 上传到 `tenant_<id>/tablet/.../major/sstable/cg_<cg>/...` （共享区） |
| 多副本怎么共看一份          | 通过 link DAG + link task，所有副本指向同一组共享 macro 块                |
| 用什么做版本隔离           | `reorganization_scn` + `op_<op>`                             |
| compaction 期间宏块写策略 | 走 `WRITE_THROUGH` 或 `WRITE_BACK`（见 Phase 2 · Page 8）         |


### 这一页要记住的核心点

> **ss compaction 不一定真合并，更多时候是“link 到共享区已合并产物”**。读 ss compaction 代码先看 `ObSSScheduleTabletFunc` → `ObLinkSSMajorSSTableDag` → `ObTabletSSLinkMajorTask` 这条线。

---

## Page 6 · 后台任务二：GC × ss

### 一句话定位

**GC（Garbage Collection）= 后台清理线程**。在 ss 下分两条独立的线：

- **远端 GC**：清理远端共享区里"再也用不到"的 macro / sstable 对象。
- **本地 GC**：清理本地 cache 与本地私有元数据中过期的版本。

两条线代码物理隔离、运行节奏不同；新人遇到"远端文件没回收 / 本地盘没回收"的问题要分开追。

### 远端 GC：ObSSGCService + ObSSGCTaskRunner

代码主要在 `src/storage/ss_garbage_collector/`，分两层：

- `ObSSGCService`（h:20）：租户 MTL 服务壳，负责生命周期、同步 Study 信息与任务结果转发。
- `ObSSGCTaskRunner`（h:28）：启动 low/mid 两组 timer，编排 MarkSweep、FailGC、DataCheck、LSMeta、Ckpt、LockRow、执行与清理任务。

#### 服务入口

```cpp
// src/storage/ss_garbage_collector/ob_ss_gc_service.h:25
static int mtl_init(ObSSGCService *&gc_service);

// src/storage/ss_garbage_collector/ob_ss_gc_service.cpp:132-139
task_runner_.start();
```

`mtl_init()` 初始化服务并托管 `ObSSGCTaskRunner`；`start()` 启动任务 runner。runner 在 `ob_ss_gc_task_runner.cpp:94` 依次拉起生成、执行、LS meta、checkpoint、lock row 和诊断清理任务。

#### 任务地图

```cpp
// src/storage/ss_garbage_collector/ob_ss_gc_task_runner.h:54-65
ObSSMarkSweepGCGenTask     mark_sweep_gc_gen_task_;
ObSSFailGCGenTask          fail_gc_gen_task_;
ObSSDataCheckGenTask       data_check_gen_task_;
ObSSLSMetaGCTask           ls_meta_gc_task_;
ObSSCkptGCTask             ckpt_gc_task_;
ObSSLockRowGCTask          lock_row_gc_task_;
ObSSMarkSweepGCExecTask    mark_sweep_gc_exec_task_;
ObSSFailGCExecTask         fail_gc_exec_task_;
```

除上述任务外，runner 还启动 GC history、tablet study、MarkSweep 诊断文件与诊断记录清理任务。各任务职责如下：

- **MarkSweep**：按轮次研究 tablet / LS 生命期，生成删除与保留诊断，并调度实际删除。
- **FailGC / DataCheck**：处理失败 GC 的补救与数据一致性检查。
- **LSMeta / Ckpt / LockRow**：分别回收过期 LS meta、checkpoint 与 SS meta control lock row。
- **执行与清理任务**：执行 MarkSweep / FailGC 结果，并清历史、study 与诊断产物。

#### executor 选择

MarkSweep 不是旧的单一 SSWriter GC 线程模型。`ObSSMarkSweepGCGenTask::try_acquire_executor_()` 在 `task/ob_ss_mark_sweep_gc_gen_task.cpp:436` 通过 SharedGC control row 事务抢锁：IDLE 时把自己的 server_id 写成 executor 并置 DOING；发现旧 executor 失效时可接管。这样保证同一轮 MarkSweep 有唯一 executor。

LS meta 回收的生成入口在 `task/ob_ss_mark_sweep_gc_gen_task.cpp:1625`，实际删除由 `dag/ob_ss_ls_meta_gc_dag.cpp:1121` 完成。

### 本地 GC：`ObTenantGCTask` + `ObBlockGCHandler`

代码在 `src/storage/shared_storage/`。

#### 触发频率：30 秒一次

```cpp
// src/storage/shared_storage/ob_tenant_gc_task.cpp:28
const int64_t ObTenantGCTask::GC_INTERVAL = 30L * 1000L * 1000L; //30s

// 同文件 :43
int ObTenantGCTask::init(const int tg_id)
{
  ...
  TG_SCHEDULE(tg_id, *this, GC_INTERVAL, true);   // 注册定时任务
  ...
}
```

#### 真正干活的处理器

```cpp
// src/storage/shared_storage/ob_block_gc_handler.h:216-228
int gc_blocks_for_tablet_meta_versions(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t min_retain_tablet_meta_version,
    const bool is_transfer_out_deleted,
    ObPrivateGCBatcher &batcher);

int gc_tablet(
    const ObIArray<int64_t> &tablet_versions,
    const GCTabletType gc_type,
    const uint64_t min_block_id = 0,
    const uint64_t max_block_id = INT64_MAX);
```

`gc_blocks_for_tablet_meta_versions` 只为旧版本 tablet meta 提交私有块回收，不删除 tablet meta 本身；`gc_tablet` 继续按 tablet 粒度协调私有块检查、旧 meta 删除和尾部落盘。

> 同目录下还有一组 `task/ob_gc_*_task.*`（如 `ob_gc_local_major_data_task`、`ob_gc_segment_file_task`）覆盖更细的 GC 场景。

### 两条线对照


| 维度   | 远端 GC                                                  | 本地 GC                                  |
| ---- | ------------------------------------------------------ | -------------------------------------- |
| 主类   | `ObSSGCService` + `ObSSGCTaskRunner`                   | `ObTenantGCTask` + `ObBlockGCHandler`      |
| 路径   | ss_garbage_collector/                                  | shared_storage/ + shared_storage/task/     |
| 谁触发  | MarkSweep 通过 SharedGC control row 抢占 / 接管 executor；不把它简化成固定 SSWriter 线程 | 每节点本地都跑，30 秒一次                  |
| 删什么  | 过期共享宏块 / 元数据、失败 GC 与数据检查产物、LS meta、checkpoint、lock row 及历史记录 | 本地 cache、本地私有 tablet meta、tmp_data 等 |
| 触发依据 | GC round / study 水位 / control row / 任务执行结果                    | tablet meta 版本号 + min_retain             |


### 这一页要记住的核心点

> **“远端看 SharedGC executor 与 GC 任务，本地看 30 秒周期”**。问到 "ss 下数据没回收" 时，先区分是 MarkSweep、FailGC、DataCheck、LSMeta、Ckpt、LockRow、执行或清理任务，还是 `ObTenantGCTask` 本地回收。

---

# Phase 2 · ss 模块细节

> 上一阶段你已经看到 ss 模块在 observer 中的位置和它的对外门面 `ObSSObjectAccessUtil`。这一阶段下沉到 ss 内部，看它在远端 / 本地各放了什么、一次 macro 块的写入怎么走完它的内部流水线。读完 Phase 2 后再翻**进阶 Page 9**——按 7 个主题串个 todo 清单，方便后续按图索骥。

---

## Page 7 · ss 远端 + 本地布局

ss 模块的"外部表现"分两块：远端对象存储里的目录树 + 本地盘上的私有数据 + Cache。

### A. 远端对象存储里有什么

ss 把数据写到远端时遵循**两层目录约定**：先按 `cluster_<cid>` 隔离不同集群，再分私有区 / 共享区。新人需要的最小子集：

```
<object_storage_root>/
└── cluster_<cid>/
    ├── server_<svr>/                  ← 节点私有区
    │   ├── server_slog/               ← 500 系统租户的 SLOG
    │   ├── server_ckpt/               ← 500 系统租户的 checkpoint
    │   └── <tid>_<tenant_epoch>/      ← 普通租户在该 server 上的私有目录（epoch 随租户重建递增）
    │       ├── tablet_data/ ...       ← 私有 macro 块（PRIVATE_*_MACRO）
    │       ├── ls/<ls>/tablet_meta/   ← 私有 tablet 元数据
    │       ├── slog/, ckpt/           ← 普通租户的 SLOG / checkpoint
    │       └── tmp_data/              ← TMP_FILE
    │
    └── tenant_<tid>/                  ← 租户共享区
        ├── TENANT_ROOT_KEY.T22        ← 租户根密钥
        ├── ls/<ls_id>/MDS_DATA/...     ← LS 内部 tablet，MDS_DATA 是目录名
        └── tablet/<tablet_id>/<reorganization_scn>/
            ├── mini/, minor/, major/   ← 用户 tablet 各级 sstable 宏块
            ├── inc_major/              ← 增量 major
            └── meta/                   ← tablet 子元数据
```

3 条速查：

- **私有区**路径里**带 `server_<svr>`**，文件名扁平且含 op/seq（如 `svr3op17seq5.T0`）；**共享区**路径里**没有 `server_<svr>`**，目录嵌套较深。
- `**.T<n>` 后缀** 是 `ObStorageObjectType` 枚举值（当前 0-41）。新人不必背全表，看到 `.T6 / .T7` 知道是"共享 major 宏块的 data / meta"就行。
- 想查全表 / 完整路径模板 / 每个类型的代码位置 → 跳到 [directory-structure.md](directory-structure.md)。

### B. 本地盘里有什么

本地盘的角色是 **私有数据 + Cache**。整体布局：

```
本地盘  =  <local_cache_root>/<tenant>_<tenant_epoch>/

    ├── tablet_data/                  ← 私有数据宏块（持久）
    ├── ls/.../tablet_meta/           ← 私有 tablet 元数据（持久）
    ├── slog/, ckpt/                  ← 私有 SLOG / checkpoint（持久）
    ├── tmp_data/                     ← TMP_FILE
    │
    ├── shared_mini_macro_cache/      ← 共享 mini 宏块的本地缓存
    ├── shared_minor_macro_cache/     ← 共享 minor 宏块
    ├── shared_major_macro_cache/     ← 共享 major 宏块
    ├── shared_inc_major_macro_cache/ ← 共享增量 major 宏块
    ├── shared_tablet_sub_meta_cache/ ← 共享 tablet 子元数据
    │
    └── (内存中) Micro Cache、Mem Macro Cache
```

### C. Cache 三件套对照表


| Cache 类型        | 介质          | 缓存粒度                       | 解决什么问题         | 主类（含路径）                                                                                                                |
| --------------- | ----------- | -------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Macro Cache     | 本地 SSD      | 完整 2MB 宏块                  | 二次访问免去拉远端      | `ObSSMacroCacheMgr` (src/storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h:352 起)    |
| Micro Cache     | 本地 SSD      | 宏块内更小粒度（"微块"，几 KB ~ 几十 KB） | 只命中宏块中一小段时省 IO | `ObSSMicroCache` (src/storage/shared_storage/ob_ss_micro_cache.h:54 起)                        |
| Mem Macro Cache | observer 内存 | 极热的宏块                      | 命中即微秒级返回       | `ObSSMemMacroCache` (src/storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h:37 起) |


### 这一页要记住的核心点

> ss 远端分 **私有 / 共享**两区；本地盘 = **私有数据 + Cache 三件套**。新人不需要先理解三类 Cache 细节——对象门面与 SS reader/writer 会按对象类型和写策略处理 Cache。

---

## Page 8 · 写宏块的最短调用链

把 Page 1 的全景图"动起来"——以**写一个 SHARED_MAJOR data 宏块**为例（compaction 把合并产物上传时就走这条路）：

```
  Compaction / shared macro manager 决定写宏块
                       │
                       ▼
  ObSharedMacroBlockMgr::do_write_block()
      src/storage/blocksstable/ob_shared_macro_block_manager.cpp:219
                       │
                       ▼
  ObMacroBlockHandle::async_write()
      src/storage/blocksstable/ob_macro_block_handle.h:39
      src/storage/blocksstable/ob_macro_block_handle.cpp:171
                       ▼
  ObIOManager / storage object abstraction
      （对象句柄路径见下）
                       │
                       ▼
  ObObjectManager::async_write_object()
      src/storage/blocksstable/ob_object_manager.h:465
                       │
                       ▼
  ObStorageObjectHandle::ss_async_write()
      src/storage/blocksstable/ob_storage_object_handle.cpp:305
                       │
                       ▼
  ObSSObjectAccessUtil::async_write_file()
      src/storage/shared_storage/ob_ss_object_access_util.h:39
                       ▼
  ObTenantFileManager::async_write_file()
      声明  src/storage/shared_storage/ob_file_manager.h:249
      实现  src/storage/shared_storage/ob_file_manager.cpp:1503
                       │
                       │   按对象类型分发到具体的 ObSSBaseWriter 子类
                       ▼
  ObSSBaseWriter::aio_write()
      声明  src/storage/shared_storage/ob_ss_reader_writer.h:90/:95
                       │
                      │   构造 ObIOInfo 并获取写入设备/fd：
                       │
                      ├──▶  write_through：object device / fd
                      │       src/storage/shared_storage/ob_ss_reader_writer.cpp:1405-1419
                       │
                      └──▶  非 write_through 或 try-write-lcache：local cache 设备/fd
                              src/storage/shared_storage/ob_ss_reader_writer.cpp:1420-1435
```

**读路径几乎对称**：常见存储访问先进入 `ObObjectManager::async_read_object`，SS handle 再委托 `ObSSObjectAccessUtil::async_pread_file`；本地 Cache、micro/macro 粒度与远端读取由具体 reader 和 IO 设备处理。`ObRemoteIoAdapter` 当前主要提供存在性检查与删除接口，不是通用读写链路。

### 4 个写策略关键词


| 关键词                                  | 含义                                                           |
| ------------------------------------ | ------------------------------------------------------------ |
| `WRITE_THROUGH`                      | 直写远端（同步上传）                                                   |
| `WRITE_BACK`                         | 先写本地 Cache；本地空间不足时回退写对象存储                                 |
| `WRITE_THROUGH_AND_TRY_WRITE_LCACHE` | 直写远端 + 顺带写一份本地 Cache                                         |
| `pin_local`                          | 从不上传远端的本地文件（如 `SERVER_META.T9`、`TENANT_DISK_SPACE_META.T17`） |


具体每种存储对象类型走哪种策略，由 `ObStorageObjectTypeBase::write_strategy()` 这个虚函数决定（见 [directory-structure.md](directory-structure.md) 第 4 节）。

### 这一页要记住的核心点

> 宏块写先过 macro handle / IO 抽象，存储对象写再经 `ObObjectManager` 与 `ObStorageObjectHandle` 进入 SS 分支；最终由具体 writer 按 write_through、write_back 或 try-lcache 策略落盘。新人优先读 `ob_macro_block_handle` / `ob_storage_object_handle` / `ob_ss_object_access_util` / `ob_file_manager` / `ob_ss_reader_writer`。

---

# 进阶 · 关键技术点速览

> 这一阶段不展开任何技术细节，只把"做 ss 工作迟早要碰的关键技术点"按主题列成清单——每条 1-2 句话点到为止，配上代码 / 文档线索；想深入哪个，按图索骥。

---

## Page 9 · 关键技术点速览

### 1. 资源隔离（Resource Isolation）


| 技术点            | 一句话解释                                                                 | 代码起点                                                                                                   |
| -------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **IO Manager** | 多租户 / 多任务的 IO 配额与排队，限制单租户对磁盘和网络 IO 的占用                                | `ObIOManager` (src/share/io/ob_io_manager.h:322)、`ObTenantIOManager` (:441)                            |
| **对象存储 QoS**   | 远端对象存储请求的限流与优先级，避免单租户拖垮共享带宽                                           | `src/share/io/io_schedule/`、`ObStorageIOUsageRepoter` (src/share/io/ob_storage_io_usage_reporter.h:26) |
| **网络隔离**       | 内部 RPC（Paxos 日志同步、RS 调度等）与对象存储 IO（S3 / OSS 读写）走不同网络链路，避免内部通信被远端存储流量挤占 | `ObIOManager` 限流（`src/share/io/`）；RPC 连接池隔离（`deps/oblib/src/rpc/obrpc/`）                               |
| **CPU 隔离**     | 基于 cgroup 的租户 CPU 限制；ss 后台任务（GC / Compaction / Prewarm）也走它            | `ObCgroupCtrl` (src/share/resource_manager/ob_cgroup_ctrl.h:164)                                       |


### 2. 负载均衡（Load Balancing）


| 技术点              | 一句话解释                                                                    | 代码 / 文档线索                                                                                                                           |
| ---------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| **日志流（LS）迁移**    | 把整个 LS 从一台 observer 搬到另一台；ss 下需要重建本地 cache、协调远端目录可见性                     | `src/storage/high_availability/`、`src/storage/high_availability/`                                          |
| **Transfer**     | tablet 在 LS 之间转移；ss 用 `transfer_start_scn` 做 `reorganization_scn` 隔离新老路径 | `src/storage/high_availability/ob_transfer_*`、`src/rootserver/ob_tenant_transfer_service.*` |
| **Tablet Split** | 一个 tablet 拆成多个；ss 下也要颁发新的 reorganization_scn                             | `src/storage/tablet/` 中 split 相关类                                                                                                   |


### 3. 数据复制 / 高可用（HA）


| 技术点                     | 一句话解释                                           | 代码 / 文档线索                                            |
| ----------------------- | ----------------------------------------------- | ---------------------------------------------------- |
| **SSWriter / SSReader** | ss 模式下租户级“主写副本”概念；checkpoint / upload 与 SSWriter 相关，远端 MarkSweep executor 由 SharedGC control row 决定 | `src/storage/incremental/ob_sswriter_service.h`、Page 6 远端 GC |
| **多副本协调**               | 共享 major 链接 DAG（Page 5）、prewarm 协调          | `compaction_v2/ob_ss_link_major_dag.h`、`prewarm/`    |
| **主备切换 / 选举**           | palf 选举                                         | `src/logservice/palf/election/`                      |


### 4. 存储多版本与一致性


| 技术点                         | 一句话解释                                        | 代码 / 文档线索                                                                                                                                                         |
| --------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **reorganization_scn**      | tablet 重组（split / transfer）的版本水位（Page 3 已介绍） | `ObTablet::get_reorganization_scn` (src/storage/tablet/ob_tablet.h:752)                                                                                           |
| **tenant_epoch / ls_epoch** | 租户 / LS 重建后递增，避免新老数据路径冲突                     | `MTL_EPOCH_ID()`、ls_epoch 在 ObLS 元信息里                                                                                                                             |
| **macro_seq / tenant_seq**  | 共享区宏块的单调序号，多副本并发写不冲突                         | `ObIncSSMacroSeq` (src/storage/incremental/ob_inc_ss_macro_seq_define.h:15-26)；`MacroBlockId::tenant_seq()` |
| **atomic 写 / 逻辑删除**         | `.tmp.<seq>`、`.deleted` 后缀                   | `ObPathContext` (src/storage/shared_storage/ob_file_helper.h)                                                                            |


### 5. Checkpoint / 启动恢复


| 技术点                        | 一句话解释                               | 代码 / 文档线索                                                             |
| -------------------------- | ----------------------------------- | --------------------------------------------------------------------- |
| **私有 SLOG / Checkpoint**   | 节点私有的存储日志与 checkpoint 文件（Page 7 已提） | `slog/`、`ckpt/` 目录；private checkpoint 类在 `src/storage/checkpoint/`    |
| **SS Checkpoint**          | 共享区的 checkpoint，保证多副本一致             | `src/storage/incremental/ob_ss_checkpoint_`* |
| **macro cache checkpoint** | 本地 cache 的 ckpt，重启后能快速恢复 cache 视图   | `MACRO_CACHE_CKPT_DATA / META`（见 directory-structure.md）         |
| **启动重放**                   | observer 启动时回放 SLOG 重建 tablet 元信息   | `src/storage/slog_ckpt/`                                              |


### 6. Prewarm / Cache 预热


| 技术点                      | 一句话解释                           | 代码 / 文档线索                                                                   |
| ------------------------ | ------------------------------- | --------------------------------------------------------------------------- |
| **major prewarm**        | 合并完成后预热远端宏块到本地 cache，避免冷读放大     | `src/storage/shared_storage/prewarm/`              |
| **storage cache policy** | 按 hot / cold tablet 调度 cache 预热 | `src/storage/shared_storage/storage_cache_policy/` |
| **副本预热**                 | LS 副本上线 / 切主前的预热                | `prewarm/ob_ls_prewarm_handler.`*、`ha_prewarm_*`                            |


### 7. 可观测性（Observability）


| 技术点     | 一句话解释                                                     | 代码 / 文档线索                                                  |
| ------- | --------------------------------------------------------- | ---------------------------------------------------------- |
| **内部表** | `__all_virtual_ss_`* 系列虚拟表，查 cache 命中率、磁盘用量、对象状态等         | `src/observer/virtual_table/` 中 ss 前缀文件                    |
| **日志**  | `OB_LOG` / `LOG_WARN` / `LOG_INFO`；ss 子模块各自有 log_module 名 | 在 `src/storage/` 下 grep `LOG_WARN` 看子模块标签 |



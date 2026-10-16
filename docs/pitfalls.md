# 代码分析注意事项（Pitfalls）

> 本文档面向 AGENT 及开发者，汇总分析 lakebase 代码时容易踩坑的知识点。
> 每条踩坑点应包含：场景 / 根因 / 正确做法，方便快速定位与避坑。

## 条目格式

每条踩坑点按以下结构记录：

- **场景**：在什么情况下会遇到（函数名、模块、错误码等）。
- **根因**：为什么会踩坑，涉及的设计约束或历史原因。
- **正确做法**：分析或改动时应遵循的原则。

---

## 内存与生命周期

<!-- 例：
- **场景**：使用 ObMemAttr 分配内存时未设置 tenant_id。
- **根因**：默认 tenant_id 为 500（SYS），大块分配会打到 sys 租户内存池，导致统计失真或 OOM。
- **正确做法**：显式设置正确的 tenant_id，必要时设置 ctx_id 与 label。
-->


## Shared Storage 特有约束

> lakebase observer 只支持 Shared Storage，不支持 Shared Nothing。涉及存储 / meta 的分析需特别关注。

### shared meta 的 meta scn 跨 ls / tablet 不可比

- **场景**：跨 ls、tablet 比较或对齐 shared meta 的 meta scn；执行迁移、备份、fork table 解耦数据等操作。
- **根因**：shared meta 的 meta scn 在不同的 ls、tablet 之间不可比。reserved meta scn（保留位点）的含义是——之前的 shared meta scn 操作都已结束，此后所有新操作的 meta scn 都会大于该点。
- **正确做法**：上述解耦类操作必须依赖 reserved meta scn 机制定位，不能直接比较两个 ls / tablet 的 meta scn。

### fork 默认不处理 meta major

- **场景**：分析或修改 fork 的 SSTable 处理流程。
- **根因**：提交 `d803cdd0` 已禁用 meta major 的调度入口，本地和共享路径都不会产生 meta major。
- **正确做法**：fork 默认不考虑 meta major，无需为其增加复制、重写或 local-land 处理逻辑。

### 旁路导入和 DDL SSTable 的行内版本可能早于提交版本

- **场景**：分析增量旁路导入生成的 Inc Major SSTable，或 DDL 构建生成的 Major SSTable。
- **根因**：两者的行内版本和索引版本都记录任务发起或构建时的快照，可能早于实际提交版本。
- **正确做法**：普通 Major 的行锁检查不依赖实际提交版本，因此不受影响；Inc Major 需要保留增量
  事务语义，其可见性和冲突判断必须使用 `upper_trans_version` 或事务表取得的提交版本。不能把偏小的
  行内或索引版本当作 Inc Major 的提交版本；`check_exist=false` 快速路径仍需结合调用可达性判断。

## SCN 位点与副本语义

### ls meta 与 tablet meta 的 clog_checkpoint_scn_ 边界语义不一致

- **场景**：读取或比较 ls meta 与 tablet meta 的 clog_checkpoint_scn_。
- **根因**：ls meta 的 clog_checkpoint_scn_ 由各模块 rec_scn 计算，表示下一条日志的位点，不包含边界值；tablet meta 的 clog_checkpoint_scn_ 包含边界值。二者边界语义相反。
- **正确做法**：比较或使用时注意边界差异——ls meta 按开区间理解，tablet meta 按闭区间理解。

### clog 恢复依赖 64MB 起始点，piece 清理需防被依赖

- **场景**：clog 恢复、piece 清理。
- **根因**：clog 恢复必须依赖每个 64MB 的起始点，导致某个 piece 的回放可能依赖它前面的 piece。
- **正确做法**：清理 piece 时不能只看该 piece 自身，必须进一步判断是否被后面的 piece 所依赖。

### fetch_ls_replay_scn / check_ls_replay_scn 是日志应用最大值，非纯 replay_scn

- **场景**：使用 fetch_ls_replay_scn RPC 或 `ObTransferUtils::check_ls_replay_scn` 接口判断副本日志位点。
- **根因**：leader 的 replay_scn 是无效值，follower 的 applied_scn 是无效值。该 RPC 直接用 `max_decided_scn = max(replay_scn, applied_scn)` 把 leader 与 follower 的区别包掉了。
- **正确做法**：理解 `check_ls_replay_scn` 实际表示各副本日志应用的最大值，不是纯粹的 replay_scn；跨角色（leader / follower）使用时注意语义。

### 主备切换后 readable_scn 会突变约 20s

- **场景**：主库切备库后检查副本可读点、判断日志归档位点。
- **根因**：主库切备库时，租户可读点（readable_scn）会突变约 20s。切换后若与新主断联且无新日志写入，租户 sts 会比最大日志 scn 大 20s，导致日志归档的最大位点永远比 sts 小 20s。
- **正确做法**：副本可读点检查必须使用 `ObLSRecoveryStatHandler::get_ls_replica_readable_scn`（该值可能比 `ls_->get_max_decided_scn` 大）；注意此类断联备库目前不具备备份恢复能力。

## 日志与诊断

### 日志关联性

- **场景**：通过日志排查 dag、task、balance 等操作的执行链路，或跨节点、跨 RPC 追踪一个请求的完整链路；阅读此类代码时发现 traceid 被当作 id 使用。
- **根因**：traceid 是 observer 内部日志排查的利器，设计上既能保持节点内同一进程 traceid 一致，也能通过 RPC 传递保持跨节点 traceid 一致；因此在执行 dag、task、balance 等操作时，traceid 被复用为 taskid。
- **正确做法**：排查此类操作时，直接用 traceid 关联日志即可拉出同一 task 的全部执行记录（含跨节点、跨 RPC）；理解 traceid 兼作 taskid 的语义，避免困惑 traceid 为何出现在 task 相关代码 / 日志中。


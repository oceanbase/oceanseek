/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LS_META_GC_DAG_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LS_META_GC_DAG_H_

#include "lib/allocator/page_arena.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaService;

struct ObSSLSMetaGCTabletReplayedScnInfo final
{
public:
  ObSSLSMetaGCTabletReplayedScnInfo();
  ~ObSSLSMetaGCTabletReplayedScnInfo() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(max_replayed_scn), K_(is_cold));

  share::SCN max_replayed_scn_;
  bool is_cold_;
};

using ObSSLSMetaGCTabletReplayedScnInfoMap = common::hash::ObHashMap<
    ObSSGCTabletBaseInfo, ObSSLSMetaGCTabletReplayedScnInfo>;

using ObSSLSMetaGCTaskMap = common::hash::ObHashMap<
    share::ObLSID, common::ObArray<ObSharedGCTask> *>;

struct ObSSLSMetaGCDagCtx final
{
public:
  ObSSLSMetaGCDagCtx();
  ~ObSSLSMetaGCDagCtx();
  int init(const uint64_t tenant_id);
  void reset();
  bool is_valid() const;
  void add_ls_count(const int64_t count);
  void add_gc_task_count(const int64_t count);
  void add_deleted_ls_meta_version_count(const int64_t count);
  void add_finished_gc_task_count(const int64_t count);
  void record_error_code(const int error_code);
  int64_t get_ls_count() const;
  int get_first_error_code() const;
  int64_t get_failed_ls_count() const;
  int64_t get_gc_task_count() const;
  int64_t get_deleted_ls_meta_version_count() const;
  int64_t get_finished_gc_task_count() const;
  TO_STRING_KV(K_(tenant_id), K_(cur_gts), K_(map_inited),
      K_(map_iter_inited),
      "ls_count", ATOMIC_LOAD(&ls_count_),
      "first_error_code", ATOMIC_LOAD(&first_error_code_),
      "failed_ls_count", ATOMIC_LOAD(&failed_ls_count_),
      "gc_task_count", ATOMIC_LOAD(&gc_task_count_),
      "deleted_ls_meta_version_count",
      ATOMIC_LOAD(&deleted_ls_meta_version_count_),
      "finished_gc_task_count", ATOMIC_LOAD(&finished_gc_task_count_));

  uint64_t tenant_id_;
  share::SCN cur_gts_;
  common::ObArenaAllocator allocator_;
  ObSSLSMetaGCTaskMap ls_meta_gc_tasks_map_;
  ObSSLSMetaGCTaskMap::const_iterator map_iter_;
  ObSSLSMetaGCTaskMap::const_iterator map_end_;
  bool map_inited_;
  bool map_iter_inited_;
  int64_t ls_count_;
  int first_error_code_;
  int64_t failed_ls_count_;
  int64_t gc_task_count_;
  int64_t deleted_ls_meta_version_count_;
  int64_t finished_gc_task_count_;

private:
  void free_task_map_();
  DISALLOW_COPY_AND_ASSIGN(ObSSLSMetaGCDagCtx);
};

struct ObSSLSMetaGCDagParam final : public share::ObIDagInitParam
{
public:
  ObSSLSMetaGCDagParam();
  virtual ~ObSSLSMetaGCDagParam() = default;
  int init(const uint64_t tenant_id, const int64_t schedule_ts = 0);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(schedule_ts));

  uint64_t tenant_id_;
  int64_t schedule_ts_;
};

class ObSSLSMetaGCDag final : public ObSSGCDagBase
{
public:
  static constexpr share::ObDagType::ObDagTypeEnum DAG_TYPE =
      share::ObDagType::DAG_TYPE_SS_LS_META_GC;

  ObSSLSMetaGCDag();
  virtual ~ObSSLSMetaGCDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int report_result() override;
  void report_start_event();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override
  {
    return consumer_group_id_;
  }

  ObSSLSMetaGCDagCtx *get_ctx() { return ctx_; }
  const ObSSLSMetaGCDagCtx *get_ctx() const { return ctx_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_schedule_ts() const { return schedule_ts_; }
  // Only initialize a fresh identity probe before it is handed to Scheduler.
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  INHERIT_TO_STRING_KV("ObIDag", share::ObIDag, K_(is_inited),
      K_(tenant_id), K_(schedule_ts), KPC_(ctx));

private:
  int alloc_ctx_();
  void free_ctx_();

private:
  bool is_inited_;
  // Kept outside ctx_ so an identity-only probe does not need init_by_param().
  uint64_t tenant_id_;
  int64_t schedule_ts_;
  int64_t start_event_reported_;
  ObSSLSMetaGCDagCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSMetaGCDag);
};

class ObSSLSMetaGCPrepareTask final : public ObSharedGCBaseTask
{
public:
  static constexpr int64_t DEFAULT_TASK_MAP_BUCKET_NUM = 1024;

  ObSSLSMetaGCPrepareTask();
  virtual ~ObSSLSMetaGCPrepareTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLSMetaGCPrepareTask", KP(this),
      K_(is_inited));

private:
  int collect_ls_meta_gc_tasks_(
      ObSSLSMetaGCDagCtx &ctx,
      int64_t &ls_count,
      int64_t &gc_task_count);
  int append_ls_meta_gc_task_(
      ObSSLSMetaGCDagCtx &ctx,
      const ObSharedGCTask &gc_task,
      const ObSharedGCLSMetaInfo &ls_meta_info,
      bool &new_ls);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSMetaGCPrepareTask);
};

class ObSSLSMetaGCScheduleTask final : public ObSharedGCBaseTask
{
public:
  ObSSLSMetaGCScheduleTask();
  virtual ~ObSSLSMetaGCScheduleTask() = default;
  int init();
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLSMetaGCScheduleTask", KP(this),
      K_(is_inited), K_(next_task_generated));

private:
  bool is_inited_;
  bool next_task_generated_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSMetaGCScheduleTask);
};

class ObSSLSMetaGCExecutorTask : public ObSharedGCBaseTask
{
public:
  ObSSLSMetaGCExecutorTask();
  virtual ~ObSSLSMetaGCExecutorTask() = default;
  int init(
      const share::ObLSID &ls_id,
      const common::ObArray<ObSharedGCTask> *gc_tasks,
      const bool has_more);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLSMetaGCExecutorTask", KP(this),
      K_(is_inited), K_(next_task_generated), K_(ls_id), KP_(gc_tasks),
      K_(has_more),
      "gc_task_count", OB_ISNULL(gc_tasks_) ? 0 : gc_tasks_->count());

protected:
  virtual int get_ls_tablet_ids_(
      const ObSSLSMetaGCDagCtx &ctx,
      common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos);
  virtual int build_tablet_replayed_scn_info_map_(
      const ObSSLSMetaGCDagCtx &ctx,
      const common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      ObSSLSMetaGCTabletReplayedScnInfoMap &tablet_replayed_scn_info_map);
  virtual int get_ls_meta_checkpoint_scn_(
      const ObSSLSMetaGCDagCtx &ctx,
      const share::SCN &ls_meta_scn,
      share::SCN &ls_ss_checkpoint_scn,
      bool &is_mark_deleted);
  virtual int remove_ls_meta_(
      const ObSSLSMetaGCDagCtx &ctx,
      const share::SCN &ls_meta_scn);
  virtual int finish_ls_meta_gc_task_(
      const ObSSLSMetaGCDagCtx &ctx,
      const ObSharedGCTask &gc_task,
      const share::SCN &ls_meta_scn,
      bool &is_gced);

private:
  int do_ls_meta_gc_(ObSSLSMetaGCDagCtx &ctx);
  int build_tablet_info_(
      const ObSSLSMetaGCDagCtx &ctx,
      const ObSSGCTabletBaseInfo &tablet_info,
      ObSSMetaService *meta_service,
      common::ObArenaAllocator &allocator,
      ObSSLSMetaGCTabletReplayedScnInfoMap &tablet_replayed_scn_info_map);
  int do_ls_meta_version_gc_(
      ObSSLSMetaGCDagCtx &ctx,
      const ObSharedGCTask &gc_task,
      const common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      ObSSLSMetaGCTabletReplayedScnInfoMap &tablet_replayed_scn_info_map,
      bool &is_gced);

  bool is_inited_;
  bool next_task_generated_;
  share::ObLSID ls_id_;
  const common::ObArray<ObSharedGCTask> *gc_tasks_;
  bool has_more_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSMetaGCExecutorTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LS_META_GC_DAG_H_

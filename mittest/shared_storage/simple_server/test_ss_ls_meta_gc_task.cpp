// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <initializer_list>
#include <mutex>
#include <thread>
#include <unistd.h>
#define private public
#define protected public

#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "storage/ss_garbage_collector/dag/ob_ss_ls_meta_gc_dag.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/ss_garbage_collector/task/ob_ss_ls_meta_gc_task.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/utility.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

class TestSSLSMetaGCTask : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSLSMetaGCTask()
    : unittest::ObSimpleClusterTestBase("test_ss_ls_meta_gc_task"),
      task_stopped_(false)
  {
  }

  ObISQLClient &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  volatile bool task_stopped_;
};

class ScriptedSSLSMetaGCTimerTask : public ObSSLSMetaGCTask
{
public:
  explicit ScriptedSSLSMetaGCTimerTask(volatile bool &is_stopped)
    : ObSSLSMetaGCTask(is_stopped),
      check_ret_(OB_SUCCESS),
      need_schedule_(true),
      check_ts_(100),
      submit_ret_(OB_SUCCESS),
      probe_ret_(OB_SUCCESS),
      probe_exist_(true),
      check_count_(0),
      submit_count_(0),
      probe_count_(0),
      submitted_tenant_id_(OB_INVALID_TENANT_ID),
      submitted_schedule_ts_(-1)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    need_schedule = need_schedule_;
    check_ts = check_ts_;
    return check_ret_;
  }

  virtual int submit_ls_meta_gc_dag_(
      const ObSSLSMetaGCDagParam &param) override
  {
    ++submit_count_;
    submitted_tenant_id_ = param.tenant_id_;
    submitted_schedule_ts_ = param.schedule_ts_;
    return submit_ret_;
  }

  virtual int check_ls_meta_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return probe_ret_;
  }

  int check_ret_;
  bool need_schedule_;
  int64_t check_ts_;
  int submit_ret_;
  int probe_ret_;
  bool probe_exist_;
  int64_t check_count_;
  int64_t submit_count_;
  int64_t probe_count_;
  uint64_t submitted_tenant_id_;
  int64_t submitted_schedule_ts_;
};

class FakeClockSSLSMetaGCTimerTask : public ObSSLSMetaGCTask
{
public:
  explicit FakeClockSSLSMetaGCTimerTask(volatile bool &is_stopped)
    : ObSSLSMetaGCTask(is_stopped),
      eligibility_ret_(OB_SUCCESS),
      can_schedule_(true),
      check_time_us_(0),
      interval_us_(100)
  {
  }

  virtual int check_can_schedule_ls_meta_gc_dag_(
      bool &can_schedule) const override
  {
    can_schedule = can_schedule_;
    return eligibility_ret_;
  }

  virtual int64_t get_gc_dag_schedule_check_time_us_() const override
  {
    return check_time_us_;
  }

  virtual void get_gc_exec_interval_us_(
      int64_t &exec_interval_us) const override
  {
    exec_interval_us = interval_us_;
  }

  int eligibility_ret_;
  bool can_schedule_;
  int64_t check_time_us_;
  int64_t interval_us_;
};

class DynamicIntervalSSLSMetaGCTimerTask final
    : public FakeClockSSLSMetaGCTimerTask
{
public:
  explicit DynamicIntervalSSLSMetaGCTimerTask(
      volatile bool &is_stopped)
    : FakeClockSSLSMetaGCTimerTask(is_stopped),
      probe_exist_(true),
      check_count_(0),
      submit_count_(0),
      probe_count_(0),
      submitted_schedule_ts_(-1)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    return ObSSLSMetaGCTask::check_need_schedule_dag_(
        need_schedule, check_ts);
  }

  virtual int submit_ls_meta_gc_dag_(
      const ObSSLSMetaGCDagParam &param) override
  {
    ++submit_count_;
    submitted_schedule_ts_ = param.schedule_ts_;
    return OB_SUCCESS;
  }

  virtual int check_ls_meta_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return OB_SUCCESS;
  }

  bool probe_exist_;
  int64_t check_count_;
  int64_t submit_count_;
  int64_t probe_count_;
  int64_t submitted_schedule_ts_;
};

class ObservedProductionSSLSMetaGCTimerTask : public ObSSLSMetaGCTask
{
public:
  explicit ObservedProductionSSLSMetaGCTimerTask(
      volatile bool &is_stopped)
    : ObSSLSMetaGCTask(is_stopped),
      check_ts_(100),
      check_count_(0),
      submit_count_(0),
      submit_ret_(OB_ERR_UNEXPECTED),
      probe_count_(0),
      probe_ret_(OB_ERR_UNEXPECTED),
      probe_exist_(false)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    need_schedule = true;
    check_ts = check_ts_;
    return OB_SUCCESS;
  }

  virtual int submit_ls_meta_gc_dag_(
      const ObSSLSMetaGCDagParam &param) override
  {
    ++submit_count_;
    submit_ret_ = ObSSLSMetaGCTask::submit_ls_meta_gc_dag_(param);
    return submit_ret_;
  }

  virtual int check_ls_meta_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    probe_ret_ = ObSSLSMetaGCTask::check_ls_meta_gc_dag_exist_(exist);
    probe_exist_ = exist;
    return probe_ret_;
  }

  int64_t check_ts_;
  int64_t check_count_;
  int64_t submit_count_;
  int submit_ret_;
  int64_t probe_count_;
  int probe_ret_;
  bool probe_exist_;
};

static SCN make_ls_meta_gc_scn(const int64_t val_ns)
{
  SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_gts(val_ns));
  return scn;
}

static int build_ls_meta_gc_task(
    ObIAllocator &allocator,
    const int64_t task_id,
    const ObLSID &ls_id,
    const SCN &meta_scn,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  ObSharedGCLSMetaInfo ls_meta_info;
  ObString value;
  task.reset();
  ls_meta_info.ls_id_ = ls_id;
  ls_meta_info.meta_scn_ = meta_scn;
  if (OB_FAIL(ls_meta_info.to_value(allocator, value))) {
    STORAGE_LOG(WARN, "fail to serialize ls meta gc task", KR(ret),
        K(ls_meta_info));
  } else if (OB_FAIL(ob_write_string(allocator,
      ObString::make_string("prepare-extra"), task.extra_info_))) {
    STORAGE_LOG(WARN, "fail to copy ls meta gc task extra info", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator,
      ObString::make_string("prepare-trace"), task.trace_id_))) {
    STORAGE_LOG(WARN, "fail to copy ls meta gc task trace id", KR(ret));
  } else {
    task.key_ = ObSharedGCTaskKey(
        OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
        0 /* round_id */, task_id);
    task.value_ = value;
    task.server_id_ = 701;
  }
  return ret;
}

static ObSSLSMetaGCPrepareTask *find_ls_meta_gc_prepare_task(
    ObSSLSMetaGCDag &dag)
{
  ObSSLSMetaGCPrepareTask *prepare_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr == prepare_task
      && nullptr != task
      && task_list_head != task) {
    prepare_task = dynamic_cast<ObSSLSMetaGCPrepareTask *>(task);
    task = task->get_next();
  }
  return prepare_task;
}

static ObSSLSMetaGCScheduleTask *find_ls_meta_gc_initial_schedule_task(
    ObSSLSMetaGCDag &dag)
{
  ObSSLSMetaGCScheduleTask *schedule_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr == schedule_task
      && nullptr != task
      && task_list_head != task) {
    schedule_task = dynamic_cast<ObSSLSMetaGCScheduleTask *>(task);
    task = task->get_next();
  }
  return schedule_task;
}

static int create_ls_meta_gc_task_map(ObSSLSMetaGCDagCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.is_valid() || ctx.map_inited_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx.ls_meta_gc_tasks_map_.create(
      8, ObMemAttr(ctx.tenant_id_, "LSMetaGCTest")))) {
    STORAGE_LOG(WARN, "fail to create test ls meta gc task map", KR(ret));
  } else {
    ctx.map_inited_ = true;
  }
  return ret;
}

static int add_ls_meta_gc_task_group(
    ObSSLSMetaGCDagCtx &ctx,
    const ObLSID &ls_id,
    const int64_t task_id,
    ObArray<ObSharedGCTask> *&gc_tasks)
{
  int ret = OB_SUCCESS;
  ObArray<ObSharedGCTask> *new_gc_tasks = nullptr;
  void *buf = nullptr;
  gc_tasks = nullptr;
  if (OB_UNLIKELY(!ctx.map_inited_ || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(buf =
      ctx.allocator_.alloc(sizeof(ObArray<ObSharedGCTask>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new_gc_tasks = new (buf) ObArray<ObSharedGCTask>();
    new_gc_tasks->set_attr(ObMemAttr(ctx.tenant_id_, "LSMetaGCTest"));
    ObSharedGCTask gc_task;
    if (OB_FAIL(build_ls_meta_gc_task(
        ctx.allocator_, task_id, ls_id,
        make_ls_meta_gc_scn(task_id + 10000), gc_task))) {
      STORAGE_LOG(WARN, "fail to build test ls meta gc task", KR(ret),
          K(ls_id), K(task_id));
    } else if (OB_FAIL(new_gc_tasks->push_back(gc_task))) {
      STORAGE_LOG(WARN, "fail to append test ls meta gc task", KR(ret),
          K(ls_id), K(task_id));
    } else if (OB_FAIL(ctx.ls_meta_gc_tasks_map_.set_refactored(
        ls_id, new_gc_tasks))) {
      STORAGE_LOG(WARN, "fail to add test ls meta gc task group", KR(ret),
          K(ls_id), K(task_id));
    } else {
      gc_tasks = new_gc_tasks;
      new_gc_tasks = nullptr;
    }
  }
  if (OB_NOT_NULL(new_gc_tasks)) {
    new_gc_tasks->~ObArray<ObSharedGCTask>();
    ctx.allocator_.free(new_gc_tasks);
  }
  return ret;
}

static int append_ls_meta_gc_task(
    ObSSLSMetaGCDagCtx &ctx,
    ObArray<ObSharedGCTask> &gc_tasks,
    const ObLSID &ls_id,
    const int64_t task_id,
    const SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ObSharedGCTask gc_task;
  if (OB_FAIL(build_ls_meta_gc_task(
      ctx.allocator_, task_id, ls_id, meta_scn, gc_task))) {
    STORAGE_LOG(WARN, "fail to build test ls meta gc task", KR(ret),
        K(ls_id), K(task_id), K(meta_scn));
  } else if (OB_FAIL(gc_tasks.push_back(gc_task))) {
    STORAGE_LOG(WARN, "fail to append test ls meta gc task", KR(ret),
        K(ls_id), K(task_id), K(meta_scn));
  }
  return ret;
}

static void publish_ls_meta_gc_task_map(ObSSLSMetaGCDagCtx &ctx)
{
  ASSERT_TRUE(ctx.map_inited_);
  ctx.cur_gts_ = make_ls_meta_gc_scn(900000);
  ctx.map_iter_ = ctx.ls_meta_gc_tasks_map_.begin();
  ctx.map_end_ = ctx.ls_meta_gc_tasks_map_.end();
  ctx.map_iter_inited_ = true;
}

static void delete_ls_meta_gc_task_if_exist(
    const ObSharedGCTaskKey &key,
    ObISQLClient &client)
{
  const int ret = ObSharedGCTaskTableOperator::delete_task(key, client);
  EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret) << ret;
}


static void delete_ls_meta_gc_history_if_exist(
    const ObSharedGCTaskKey &key,
    ObISQLClient &client)
{
  const int ret = ObSharedGCTaskTableOperator::delete_history(key, client);
  EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret) << ret;
}

class ScopedLSMetaGCTaskRowsCleanup final
{
public:
  ScopedLSMetaGCTaskRowsCleanup(
      const ObSharedGCTaskKey &key,
      ObISQLClient &client)
    : key_(key), client_(client)
  {
  }

  ~ScopedLSMetaGCTaskRowsCleanup()
  {
    const int task_ret =
        ObSharedGCTaskTableOperator::delete_task(key_, client_);
    const int history_ret =
        ObSharedGCTaskTableOperator::delete_history(key_, client_);
    if (OB_SUCCESS != task_ret && OB_ENTRY_NOT_EXIST != task_ret) {
      LOG_WARN_RET(task_ret, "fail to clean ls meta gc task row", K_(key));
    }
    if (OB_SUCCESS != history_ret && OB_ENTRY_NOT_EXIST != history_ret) {
      LOG_WARN_RET(history_ret, "fail to clean ls meta gc history row",
          K_(key));
    }
  }

private:
  ObSharedGCTaskKey key_;
  ObISQLClient &client_;
  DISALLOW_COPY_AND_ASSIGN(ScopedLSMetaGCTaskRowsCleanup);
};

class ScriptedSSLSMetaGCExecutorTask
    : public ObSSLSMetaGCExecutorTask
{
public:
  ScriptedSSLSMetaGCExecutorTask()
    : tablet_ret_(OB_SUCCESS),
      replay_ret_(OB_SUCCESS),
      query_ret_(OB_SUCCESS),
      remove_ret_(OB_SUCCESS),
      finish_ret_(OB_SUCCESS),
      add_tablet_(false),
      is_mark_deleted_(false),
      mark_deleted_meta_scn_(),
      cold_(false),
      omit_replay_info_(false),
      use_real_finish_(false),
      stop_after_remove_(false),
      tablet_info_(),
      replayed_scn_(),
      checkpoint_scn_(),
      unsafe_meta_scn_(),
      unsafe_checkpoint_scn_(),
      tablet_call_count_(0),
      replay_call_count_(0),
      query_call_count_(0),
      remove_call_count_(0),
      finish_call_count_(0),
      queried_meta_scns_(),
      finished_task_ids_()
  {
  }

  virtual ~ScriptedSSLSMetaGCExecutorTask() = default;

protected:
  virtual int get_ls_tablet_ids_(
      const ObSSLSMetaGCDagCtx &ctx,
      ObIArray<ObSSGCTabletBaseInfo> &tablet_infos) override
  {
    UNUSED(ctx);
    ++tablet_call_count_;
    tablet_infos.reset();
    int ret = tablet_ret_;
    if (OB_SUCCESS == ret && add_tablet_) {
      ret = tablet_infos.push_back(tablet_info_);
    }
    return ret;
  }

  virtual int build_tablet_replayed_scn_info_map_(
      const ObSSLSMetaGCDagCtx &ctx,
      const ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      ObSSLSMetaGCTabletReplayedScnInfoMap
          &tablet_replayed_scn_info_map) override
  {
    UNUSED(ctx);
    ++replay_call_count_;
    int ret = replay_ret_;
    if (OB_SUCCESS == ret && add_tablet_) {
      if (tablet_infos.count() != 1
          || !(tablet_infos.at(0) == tablet_info_)
          || !replayed_scn_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!omit_replay_info_) {
        ObSSLSMetaGCTabletReplayedScnInfo replay_info;
        replay_info.max_replayed_scn_ = replayed_scn_;
        replay_info.is_cold_ = cold_;
        ret = tablet_replayed_scn_info_map.set_refactored(
            tablet_info_, replay_info);
      }
    }
    return ret;
  }

  virtual int get_ls_meta_checkpoint_scn_(
      const ObSSLSMetaGCDagCtx &ctx,
      const SCN &ls_meta_scn,
      SCN &ls_ss_checkpoint_scn,
      bool &is_mark_deleted) override
  {
    UNUSED(ctx);
    is_mark_deleted = false;
    ++query_call_count_;
    int ret = queried_meta_scns_.push_back(ls_meta_scn);
    if (OB_SUCCESS == ret) {
      ret = query_ret_;
    }
    if (OB_SUCCESS == ret) {
      is_mark_deleted = is_mark_deleted_
          || (mark_deleted_meta_scn_.is_valid() && mark_deleted_meta_scn_ == ls_meta_scn);
      ls_ss_checkpoint_scn = unsafe_meta_scn_.is_valid()
          && unsafe_meta_scn_ == ls_meta_scn
          ? unsafe_checkpoint_scn_ : checkpoint_scn_;
    }
    return ret;
  }

  virtual int remove_ls_meta_(
      const ObSSLSMetaGCDagCtx &ctx,
      const SCN &ls_meta_scn) override
  {
    UNUSED(ctx);
    UNUSED(ls_meta_scn);
    ++remove_call_count_;
    if (stop_after_remove_ && OB_NOT_NULL(get_dag())) {
      get_dag()->simply_set_stop();
    }
    return remove_ret_;
  }

  virtual int finish_ls_meta_gc_task_(
      const ObSSLSMetaGCDagCtx &ctx,
      const ObSharedGCTask &gc_task,
      const SCN &ls_meta_scn,
      bool &is_gced) override
  {
    ++finish_call_count_;
    int ret = finished_task_ids_.push_back(gc_task.key_.task_id_);
    if (OB_SUCCESS == ret) {
      if (use_real_finish_) {
        ret = ObSSLSMetaGCExecutorTask::finish_ls_meta_gc_task_(
            ctx, gc_task, ls_meta_scn, is_gced);
      } else {
        ret = finish_ret_;
        is_gced = OB_SUCCESS == ret;
      }
    }
    return ret;
  }

public:
  int tablet_ret_;
  int replay_ret_;
  int query_ret_;
  int remove_ret_;
  int finish_ret_;
  bool add_tablet_;
  bool is_mark_deleted_;
  SCN mark_deleted_meta_scn_;
  bool cold_;
  bool omit_replay_info_;
  bool use_real_finish_;
  bool stop_after_remove_;
  ObSSGCTabletBaseInfo tablet_info_;
  SCN replayed_scn_;
  SCN checkpoint_scn_;
  SCN unsafe_meta_scn_;
  SCN unsafe_checkpoint_scn_;
  int64_t tablet_call_count_;
  int64_t replay_call_count_;
  int64_t query_call_count_;
  int64_t remove_call_count_;
  int64_t finish_call_count_;
  ObSEArray<SCN, 8> queried_meta_scns_;
  ObSEArray<int64_t, 8> finished_task_ids_;
};

class LSMetaGCTestSignal final
{
public:
  LSMetaGCTestSignal() : signaled_(false) {}

  void signal()
  {
    {
      std::lock_guard<std::mutex> guard(lock_);
      signaled_ = true;
    }
    cond_.notify_all();
  }

  bool wait(const int64_t timeout_ms = 30 * 1000L)
  {
    std::unique_lock<std::mutex> guard(lock_);
    return cond_.wait_for(guard, std::chrono::milliseconds(timeout_ms),
        [this]() { return signaled_; });
  }

private:
  std::mutex lock_;
  std::condition_variable cond_;
  bool signaled_;
};

class LSMetaGCConcurrentFinishBarrier final
{
public:
  LSMetaGCConcurrentFinishBarrier()
    : arrived_count_(0), released_(false), timed_out_(false)
  {
  }

  bool arrive_and_wait()
  {
    std::unique_lock<std::mutex> guard(lock_);
    ++arrived_count_;
    if (2 <= arrived_count_) {
      released_ = true;
      cond_.notify_all();
    } else if (!cond_.wait_for(guard, std::chrono::seconds(30),
        [this]() { return released_; })) {
      timed_out_ = true;
      released_ = true;
      cond_.notify_all();
    }
    return !timed_out_;
  }

  int64_t arrived_count() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return arrived_count_;
  }

  bool timed_out() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return timed_out_;
  }

private:
  mutable std::mutex lock_;
  std::condition_variable cond_;
  int64_t arrived_count_;
  bool released_;
  bool timed_out_;
};

class ConcurrentFinishSSLSMetaGCExecutorTask final
    : public ScriptedSSLSMetaGCExecutorTask
{
public:
  ConcurrentFinishSSLSMetaGCExecutorTask()
    : concurrent_finish_call_count_(0), finish_barrier_(nullptr)
  {
  }

  void set_finish_barrier(LSMetaGCConcurrentFinishBarrier &finish_barrier)
  {
    finish_barrier_ = &finish_barrier;
  }

  int64_t concurrent_finish_call_count_;

protected:
  virtual int finish_ls_meta_gc_task_(
      const ObSSLSMetaGCDagCtx &ctx,
      const ObSharedGCTask &gc_task,
      const SCN &ls_meta_scn,
      bool &is_gced) override
  {
    int ret = OB_SUCCESS;
    is_gced = false;
    ++concurrent_finish_call_count_;
    if (OB_ISNULL(finish_barrier_)) {
      ret = OB_NOT_INIT;
    } else if (!finish_barrier_->arrive_and_wait()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = ObSSLSMetaGCExecutorTask::finish_ls_meta_gc_task_(
          ctx, gc_task, ls_meta_scn, is_gced);
    }
    return ret;
  }

private:
  LSMetaGCConcurrentFinishBarrier *finish_barrier_;
};

class ScopedLSMetaGCDagAdaptiveLimit final
{
public:
  ScopedLSMetaGCDagAdaptiveLimit(
      ObTenantDagScheduler &scheduler,
      const int64_t old_limit)
    : scheduler_(scheduler), old_limit_(old_limit), need_restore_(false)
  {
  }

  ~ScopedLSMetaGCDagAdaptiveLimit()
  {
    if (need_restore_) {
      const int ret = scheduler_.set_adaptive_limit(
          ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit_);
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to restore ls meta gc dag adaptive limit", KR(ret),
            K_(old_limit));
      }
      scheduler_.notify();
    }
  }

  int set(const int64_t new_limit)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(need_restore_)) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(scheduler_.set_adaptive_limit(
        ObDagPrio::DAG_PRIO_SS_GC_GEN, new_limit))) {
      LOG_WARN("fail to set ls meta gc dag adaptive limit", KR(ret),
          K(new_limit));
    } else {
      need_restore_ = true;
    }
    return ret;
  }

private:
  ObTenantDagScheduler &scheduler_;
  const int64_t old_limit_;
  bool need_restore_;
  DISALLOW_COPY_AND_ASSIGN(ScopedLSMetaGCDagAdaptiveLimit);
};

class ScopedQueuedLSMetaGCDagCancel final
{
public:
  ScopedQueuedLSMetaGCDagCancel(
      ObTenantDagScheduler &scheduler,
      const uint64_t tenant_id)
    : scheduler_(scheduler),
      tenant_id_(tenant_id),
      armed_(false)
  {
  }

  ~ScopedQueuedLSMetaGCDagCancel()
  {
    if (armed_) {
      ObSSLSMetaGCDag probe;
      probe.set_tenant_id(tenant_id_);
      const int ret = scheduler_.cancel_dag(
          &probe, true /* force_cancel */);
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to clean queued ls meta gc dag", KR(ret),
            K_(tenant_id));
      }
      scheduler_.notify();
    }
  }

  void arm() { armed_ = true; }
  void disarm() { armed_ = false; }

private:
  ObTenantDagScheduler &scheduler_;
  const uint64_t tenant_id_;
  bool armed_;
  DISALLOW_COPY_AND_ASSIGN(ScopedQueuedLSMetaGCDagCancel);
};

class ScopedUnsubmittedLSMetaGCDag final
{
public:
  explicit ScopedUnsubmittedLSMetaGCDag(ObTenantDagScheduler &scheduler)
    : scheduler_(scheduler), dag_(nullptr)
  {
  }

  ~ScopedUnsubmittedLSMetaGCDag()
  {
    if (OB_NOT_NULL(dag_)) {
      scheduler_.free_dag(*dag_);
    }
  }

  void hold(ObSSLSMetaGCDag *dag) { dag_ = dag; }
  void release() { dag_ = nullptr; }

private:
  ObTenantDagScheduler &scheduler_;
  ObSSLSMetaGCDag *dag_;
  DISALLOW_COPY_AND_ASSIGN(ScopedUnsubmittedLSMetaGCDag);
};

struct LSMetaGCConcurrentTestState final
{
  LSMetaGCConcurrentTestState()
    : arrived_count_(0),
      recorded_count_(0),
      completed_count_(0),
      snapshot_valid_(true),
      observed_first_error_(OB_ERR_UNEXPECTED),
      observed_failed_ls_count_(-1),
      both_entered_(),
      release_executors_(),
      all_errors_recorded_()
  {
  }

  std::atomic<int64_t> arrived_count_;
  std::atomic<int64_t> recorded_count_;
  std::atomic<int64_t> completed_count_;
  std::atomic<bool> snapshot_valid_;
  std::atomic<int> observed_first_error_;
  std::atomic<int64_t> observed_failed_ls_count_;
  LSMetaGCTestSignal both_entered_;
  LSMetaGCTestSignal release_executors_;
  LSMetaGCTestSignal all_errors_recorded_;
};

class ConcurrentSSLSMetaGCExecutorTask final
    : public ObSSLSMetaGCExecutorTask
{
public:
  ConcurrentSSLSMetaGCExecutorTask(
      const int error_code,
      const ObLSID &expected_ls_id,
      const ObArray<ObSharedGCTask> *expected_gc_tasks,
      LSMetaGCConcurrentTestState &state)
    : error_code_(error_code),
      expected_ls_id_(expected_ls_id),
      expected_gc_tasks_(expected_gc_tasks),
      expected_value_ptr_(OB_ISNULL(expected_gc_tasks)
          || expected_gc_tasks->empty()
          ? nullptr : expected_gc_tasks->at(0).value_.ptr()),
      expected_extra_info_ptr_(OB_ISNULL(expected_gc_tasks)
          || expected_gc_tasks->empty()
          ? nullptr : expected_gc_tasks->at(0).extra_info_.ptr()),
      expected_trace_id_ptr_(OB_ISNULL(expected_gc_tasks)
          || expected_gc_tasks->empty()
          ? nullptr : expected_gc_tasks->at(0).trace_id_.ptr()),
      state_(state)
  {
  }

  virtual int process() override
  {
    int ret = ObSSLSMetaGCExecutorTask::process();
    ObSSLSMetaGCDagCtx *ctx = nullptr;
    if (OB_SUCC(ret) && OB_FAIL(
        ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(*this, ctx))) {
      LOG_WARN("fail to get concurrent ls meta gc dag ctx after process",
          KR(ret), K_(expected_ls_id));
    } else if (OB_SUCC(ret)
        && 2 == state_.recorded_count_.fetch_add(
            1, std::memory_order_acq_rel) + 1) {
      state_.observed_first_error_.store(
          ctx->get_first_error_code(), std::memory_order_release);
      state_.observed_failed_ls_count_.store(
          ctx->get_failed_ls_count(), std::memory_order_release);
      state_.all_errors_recorded_.signal();
    }
    if (OB_SUCCESS != ret) {
      state_.snapshot_valid_.store(false, std::memory_order_release);
    }
    (void)state_.completed_count_.fetch_add(1, std::memory_order_release);
    return ret;
  }

protected:
  virtual int get_ls_tablet_ids_(
      const ObSSLSMetaGCDagCtx &ctx,
      ObIArray<ObSSGCTabletBaseInfo> &tablet_infos) override
  {
    UNUSED(ctx);
    int ret = validate_snapshot_(ctx);
    tablet_infos.reset();
    if (OB_FAIL(ret)) {
      LOG_WARN("invalid ls meta gc snapshot before wait", KR(ret),
          K_(expected_ls_id));
    } else {
      if (2 == state_.arrived_count_.fetch_add(
          1, std::memory_order_acq_rel) + 1) {
        state_.both_entered_.signal();
      }
      if (!state_.release_executors_.wait(60 * 1000L)) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(validate_snapshot_(ctx))) {
        LOG_WARN("invalid ls meta gc snapshot after wait", KR(ret),
            K_(expected_ls_id));
      } else {
        ret = error_code_;
      }
    }
    return ret;
  }

private:
  int validate_snapshot_(const ObSSLSMetaGCDagCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObSharedGCLSMetaInfo gc_info;
    ObArray<ObSharedGCTask> *current_gc_tasks = nullptr;
    if (OB_UNLIKELY(OB_ISNULL(expected_gc_tasks_)
        || 1 != expected_gc_tasks_->count()
        || expected_value_ptr_ != expected_gc_tasks_->at(0).value_.ptr()
        || expected_extra_info_ptr_
            != expected_gc_tasks_->at(0).extra_info_.ptr()
        || expected_trace_id_ptr_
            != expected_gc_tasks_->at(0).trace_id_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ctx.ls_meta_gc_tasks_map_.get_refactored(
        expected_ls_id_, current_gc_tasks))) {
    } else if (OB_UNLIKELY(current_gc_tasks != expected_gc_tasks_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
        gc_info.from_value(expected_gc_tasks_->at(0).value_))) {
    } else if (OB_UNLIKELY(!gc_info.is_valid()
        || gc_info.ls_id_ != expected_ls_id_)) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

private:
  const int error_code_;
  const ObLSID expected_ls_id_;
  const ObArray<ObSharedGCTask> *expected_gc_tasks_;
  const char *expected_value_ptr_;
  const char *expected_extra_info_ptr_;
  const char *expected_trace_id_ptr_;
  LSMetaGCConcurrentTestState &state_;
  DISALLOW_COPY_AND_ASSIGN(ConcurrentSSLSMetaGCExecutorTask);
};

static bool wait_ls_meta_gc_dag_absent(
    ObTenantDagScheduler &scheduler,
    const uint64_t tenant_id,
    const int64_t timeout_us,
    int &check_ret)
{
  ObSSLSMetaGCDag probe;
  bool dag_exist = true;
  bool is_emergency = false;
  const int64_t deadline_us = ObTimeUtility::current_time() + timeout_us;
  probe.set_tenant_id(tenant_id);
  check_ret = OB_SUCCESS;
  while (dag_exist && OB_SUCCESS == check_ret
      && ObTimeUtility::current_time() < deadline_us) {
    check_ret = scheduler.check_dag_exist(
        &probe, dag_exist, is_emergency);
    if (OB_SUCCESS == check_ret && dag_exist) {
      usleep(10 * 1000L);
    }
  }
  return OB_SUCCESS == check_ret && !dag_exist;
}

static int init_ls_meta_gc_executor_test_dag(
    ObArenaAllocator &dag_allocator,
    ObSSLSMetaGCDag &dag,
    ObSSLSMetaGCDagCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCDagParam param;
  ctx = nullptr;
  if (OB_FAIL(dag.basic_init(dag_allocator))) {
  } else if (OB_FAIL(param.init(OB_SYS_TENANT_ID))) {
  } else if (OB_FAIL(dag.init_by_param(&param))) {
  } else if (OB_ISNULL(ctx = dag.get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_ls_meta_gc_task_map(*ctx))) {
  }
  return ret;
}

static int alloc_scripted_ls_meta_gc_executor(
    ObSSLSMetaGCDag &dag,
    const ObLSID &ls_id,
    const ObArray<ObSharedGCTask> &gc_tasks,
    ScriptedSSLSMetaGCExecutorTask *&executor)
{
  int ret = OB_SUCCESS;
  executor = nullptr;
  if (OB_FAIL(dag.alloc_task(executor))) {
  } else if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(executor->init(ls_id, &gc_tasks, false))) {
  } else {
    // The Scheduler calls generate_next_task() before process(). This helper
    // models the terminal Executor path without publishing another Task.
    ObITask *next_task = nullptr;
    const int generate_ret = executor->generate_next_task(next_task);
    if (OB_ITER_END != generate_ret || OB_NOT_NULL(next_task)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_timer_schedule_check_is_side_effect_free_with_fake_clock)
{
  FakeClockSSLSMetaGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(0, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
  task.commit_gc_dag_schedule_time_(100);

  bool need_schedule = true;
  int64_t check_ts = -1;
  task.check_time_us_ = 150;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(150, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.check_time_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(200, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.check_time_us_ = 250;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(250, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.can_schedule_ = false;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.can_schedule_ = true;
  task.eligibility_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.commit_gc_dag_schedule_time_(250);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());
  task.destroy();
  ASSERT_FALSE(task.is_inited_);
  ASSERT_EQ(0, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_timer_reloads_dynamic_interval_after_dag_completion)
{
  DynamicIntervalSSLSMetaGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());

  task.check_time_us_ = 200;
  task.probe_exist_ = true;
  task.runTimerTask();
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.interval_us_ = 300;
  task.check_time_us_ = 310;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_time_us_ = 320;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.interval_us_ = 200;
  task.check_time_us_ = 321;
  task.runTimerTask();
  ASSERT_EQ(2, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(321, task.submitted_schedule_ts_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.check_time_us_ = 330;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(2, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.interval_us_ = 100;
  task.check_time_us_ = 400;
  task.runTimerTask();
  ASSERT_EQ(3, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);

  task.interval_us_ = 300;
  task.check_time_us_ = 421;
  task.runTimerTask();
  ASSERT_EQ(4, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);

  task.check_time_us_ = 621;
  task.runTimerTask();
  ASSERT_EQ(5, task.check_count_);
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(2, task.run_cnt_);
  ASSERT_EQ(621, task.submitted_schedule_ts_);
  ASSERT_EQ(621, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_timer_try_schedule_commits_only_accepted_submission)
{
  ScriptedSSLSMetaGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);

  task.need_schedule_ = false;
  ASSERT_EQ(OB_SUCCESS, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.need_schedule_ = true;
  task.check_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.check_ret_ = OB_SUCCESS;
  task.check_ts_ = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.check_ts_ = 30;
  task.submit_ret_ = OB_SIZE_OVERFLOW;
  ASSERT_EQ(OB_SIZE_OVERFLOW, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 40;
  task.submit_ret_ = OB_TASK_EXIST;
  ASSERT_EQ(OB_TASK_EXIST, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 50;
  task.submit_ret_ = OB_EAGAIN;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = false;
  ASSERT_EQ(OB_EAGAIN, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(3, task.submit_count_);
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 60;
  task.probe_ret_ = OB_TIMEOUT;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(4, task.submit_count_);
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 70;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(5, task.submit_count_);
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(5, task.run_cnt_);
  ASSERT_EQ(OB_SYS_TENANT_ID, task.submitted_tenant_id_);
  ASSERT_EQ(70, task.submitted_schedule_ts_);
  ASSERT_EQ(70, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  ScriptedSSLSMetaGCTimerTask success_task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, success_task.init(OB_SYS_TENANT_ID));
  success_task.check_ts_ = 80;
  success_task.submit_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, success_task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(1, success_task.submit_count_);
  ASSERT_EQ(0, success_task.probe_count_);
  ASSERT_EQ(1, success_task.run_cnt_);
  ASSERT_EQ(OB_SYS_TENANT_ID, success_task.submitted_tenant_id_);
  ASSERT_EQ(80, success_task.submitted_schedule_ts_);
  ASSERT_EQ(80, success_task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      success_task.dag_schedule_controller_.state());

  ScriptedSSLSMetaGCTimerTask stopped_task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, stopped_task.init(OB_SYS_TENANT_ID));
  stopped_task.commit_gc_dag_schedule_time_(10);
  task_stopped_ = true;
  ASSERT_EQ(OB_SERVICE_STOPPED,
      stopped_task.try_schedule_ls_meta_gc_dag_());
  ASSERT_EQ(0, stopped_task.submit_count_);
  ASSERT_EQ(0, stopped_task.run_cnt_);
  ASSERT_EQ(10, stopped_task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      stopped_task.dag_schedule_controller_.state());
  task_stopped_ = false;
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_timer_waits_for_dag_and_separates_finish_tick)
{
  ScriptedSSLSMetaGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());

  task.probe_ret_ = OB_TIMEOUT;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = true;
  task.runTimerTask();
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 320;
  task.submit_ret_ = OB_SUCCESS;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(OB_SYS_TENANT_ID, task.submitted_tenant_id_);
  ASSERT_EQ(320, task.submitted_schedule_ts_);
  ASSERT_EQ(320, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_timer_stopped_runner_blocks_entry)
{
  volatile bool stopped = true;
  ScriptedSSLSMetaGCTimerTask task(stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));

  task.runTimerTask();

  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(0, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_timer_confirms_duplicate_with_real_scheduler)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit));
  ScopedLSMetaGCDagAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.set(0));

  const uint64_t isolated_tenant_id = 1010;
  ObservedProductionSSLSMetaGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(isolated_tenant_id));
  task.check_ts_ = 700;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(isolated_tenant_id, 600));
  ScopedQueuedLSMetaGCDagCancel cleanup_guard(
      *scheduler, isolated_tenant_id);
  cleanup_guard.arm();

  ObSSLSMetaGCDag probe;
  probe.set_tenant_id(isolated_tenant_id);
  const int pre_cancel_ret = scheduler->cancel_dag(
      &probe, true /* force_cancel */);
  scheduler->notify();
  int pre_probe_ret = OB_SUCCESS;
  const bool absent_before_submit = wait_ls_meta_gc_dag_absent(
      *scheduler, isolated_tenant_id, 30L * 1000L * 1000L,
      pre_probe_ret);

  int pre_submit_ret = OB_ERR_UNEXPECTED;
  if (OB_SUCCESS != pre_cancel_ret) {
    pre_submit_ret = pre_cancel_ret;
  } else if (OB_SUCCESS != pre_probe_ret) {
    pre_submit_ret = pre_probe_ret;
  } else if (!absent_before_submit) {
    pre_submit_ret = OB_EAGAIN;
  } else {
    pre_submit_ret =
        scheduler->create_and_add_dag<ObSSLSMetaGCDag>(&param);
  }
  const int try_ret = OB_SUCCESS == pre_submit_ret
      ? task.try_schedule_ls_meta_gc_dag_() : pre_submit_ret;

  bool exist_before_cancel = false;
  bool is_emergency = false;
  const int resident_probe_ret = OB_SUCCESS == pre_submit_ret
      ? scheduler->check_dag_exist(
          &probe, exist_before_cancel, is_emergency)
      : pre_submit_ret;
  const int cancel_ret = OB_SUCCESS == pre_submit_ret
      ? scheduler->cancel_dag(&probe, true /* force_cancel */)
      : pre_submit_ret;
  scheduler->notify();
  int absent_probe_ret = cancel_ret;
  const bool absent_after_cancel = OB_SUCCESS == cancel_ret
      && wait_ls_meta_gc_dag_absent(
          *scheduler, isolated_tenant_id, 30L * 1000L * 1000L,
          absent_probe_ret);
  if (OB_SUCCESS == absent_probe_ret && absent_after_cancel) {
    cleanup_guard.disarm();
  }

  EXPECT_EQ(OB_SUCCESS, pre_cancel_ret);
  EXPECT_EQ(OB_SUCCESS, pre_probe_ret);
  EXPECT_TRUE(absent_before_submit);
  EXPECT_EQ(OB_SUCCESS, pre_submit_ret);
  EXPECT_EQ(OB_SUCCESS, try_ret);
  EXPECT_EQ(1, task.check_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(OB_EAGAIN, task.submit_ret_);
  EXPECT_EQ(1, task.probe_count_);
  EXPECT_EQ(OB_SUCCESS, task.probe_ret_);
  EXPECT_TRUE(task.probe_exist_);
  EXPECT_EQ(1, task.run_cnt_);
  EXPECT_EQ(700, task.last_period_commit_time_us_);
  EXPECT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
  EXPECT_EQ(OB_SUCCESS, resident_probe_ret);
  EXPECT_TRUE(exist_before_cancel);
  EXPECT_EQ(OB_SUCCESS, cancel_ret);
  EXPECT_EQ(OB_SUCCESS, absent_probe_ret);
  EXPECT_TRUE(absent_after_cancel);
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_scheduler_registration)
{
  ASSERT_STREQ("SS_LS_META_GC",
      ObITask::ObITaskTypeStr[ObITask::TASK_TYPE_SS_LS_META_GC]);
  ASSERT_STREQ("SS_LS_META_GC",
      ObIDag::get_dag_type_str(ObDagType::DAG_TYPE_SS_LS_META_GC));
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LS_META_GC].init_dag_prio_);
  ASSERT_EQ(3, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_GEN].score_);
  ASSERT_EQ(ObSysTaskType::SS_LS_META_GC_TASK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LS_META_GC].sys_task_type_);
  ASSERT_STREQ("SS_LS_META_GC_TASK",
      sys_task_type_to_str(ObSysTaskType::SS_LS_META_GC_TASK));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_context_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSLSMetaGCDagCtx ctx;
  ASSERT_FALSE(ctx.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, ctx.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_INIT_TWICE, ctx.init(OB_SYS_TENANT_ID));
  ASSERT_TRUE(ctx.is_valid());
  ASSERT_FALSE(ctx.cur_gts_.is_valid());
  ASSERT_FALSE(ctx.map_inited_);
  ASSERT_FALSE(ctx.map_iter_inited_);
  ASSERT_EQ(0, ctx.get_ls_count());
  ASSERT_EQ(OB_SUCCESS, ctx.get_first_error_code());
  ASSERT_EQ(0, ctx.get_failed_ls_count());
  ASSERT_EQ(0, ctx.get_gc_task_count());
  ASSERT_EQ(0, ctx.get_deleted_ls_meta_version_count());
  ASSERT_EQ(0, ctx.get_finished_gc_task_count());

  ctx.add_ls_count(2);
  ctx.add_gc_task_count(3);
  ctx.add_deleted_ls_meta_version_count(4);
  ctx.add_finished_gc_task_count(5);
  ASSERT_EQ(2, ctx.get_ls_count());
  ASSERT_EQ(3, ctx.get_gc_task_count());
  ASSERT_EQ(4, ctx.get_deleted_ls_meta_version_count());
  ASSERT_EQ(5, ctx.get_finished_gc_task_count());

  ASSERT_EQ(OB_SUCCESS, ctx.ls_meta_gc_tasks_map_.create(
      8, ObMemAttr(OB_SYS_TENANT_ID, "LSMetaGCTest")));
  ctx.map_inited_ = true;
  void *buf = ctx.allocator_.alloc(sizeof(ObArray<ObSharedGCTask>));
  ASSERT_NE(nullptr, buf);
  ObArray<ObSharedGCTask> *gc_tasks =
      new (buf) ObArray<ObSharedGCTask>();
  ASSERT_EQ(OB_SUCCESS,
      ctx.ls_meta_gc_tasks_map_.set_refactored(ObLSID(201), gc_tasks));
  ctx.map_iter_ = ctx.ls_meta_gc_tasks_map_.begin();
  ctx.map_end_ = ctx.ls_meta_gc_tasks_map_.end();
  ctx.map_iter_inited_ = true;

  ctx.reset();
  ASSERT_FALSE(ctx.is_valid());
  ASSERT_FALSE(ctx.cur_gts_.is_valid());
  ASSERT_FALSE(ctx.map_inited_);
  ASSERT_FALSE(ctx.map_iter_inited_);
  ASSERT_EQ(0, ctx.get_ls_count());
  ASSERT_EQ(OB_SUCCESS, ctx.get_first_error_code());
  ASSERT_EQ(0, ctx.get_failed_ls_count());
  ASSERT_EQ(0, ctx.get_gc_task_count());
  ASSERT_EQ(0, ctx.get_deleted_ls_meta_version_count());
  ASSERT_EQ(0, ctx.get_finished_gc_task_count());
  ctx.reset();
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_prepare_groups_and_publishes_snapshot)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID first_ls_id(2201);
  const ObLSID second_ls_id(2202);
  const int64_t first_task_id = 991001;
  const int64_t second_task_id = 991002;
  const int64_t third_task_id = 991003;
  const ObSharedGCTaskKey first_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, first_task_id);
  const ObSharedGCTaskKey second_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, second_task_id);
  const ObSharedGCTaskKey third_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, third_task_id);
  const ObSharedGCTaskKey malformed_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, 991004);
  delete_ls_meta_gc_task_if_exist(first_key, sql_proxy());
  delete_ls_meta_gc_task_if_exist(second_key, sql_proxy());
  delete_ls_meta_gc_task_if_exist(third_key, sql_proxy());
  delete_ls_meta_gc_task_if_exist(malformed_key, sql_proxy());

  ObArenaAllocator row_allocator("LSMetaPrepRows");
  ObSharedGCTask first_task;
  ObSharedGCTask second_task;
  ObSharedGCTask third_task;
  ASSERT_EQ(OB_SUCCESS, build_ls_meta_gc_task(
      row_allocator, first_task_id, first_ls_id,
      make_ls_meta_gc_scn(1001), first_task));
  ASSERT_EQ(OB_SUCCESS, build_ls_meta_gc_task(
      row_allocator, second_task_id, second_ls_id,
      make_ls_meta_gc_scn(1002), second_task));
  ASSERT_EQ(OB_SUCCESS, build_ls_meta_gc_task(
      row_allocator, third_task_id, first_ls_id,
      make_ls_meta_gc_scn(1003), third_task));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(first_task, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(second_task, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(third_task, sql_proxy()));
  row_allocator.reset();

  ObArenaAllocator dag_allocator("LSMetaPrepDag");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCPrepareTask *prepare_task =
      find_ls_meta_gc_prepare_task(dag);
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_EQ(OB_SUCCESS, prepare_task->process());

  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_TRUE(ctx->cur_gts_.is_valid());
  ASSERT_FALSE(ctx->cur_gts_.is_min());
  ASSERT_FALSE(ctx->cur_gts_.is_max());
  ASSERT_TRUE(ctx->map_inited_);
  ASSERT_TRUE(ctx->map_iter_inited_);
  ASSERT_EQ(2, ctx->get_ls_count());
  ASSERT_EQ(3, ctx->get_gc_task_count());

  ObArray<ObSharedGCTask> *first_ls_tasks = nullptr;
  ObArray<ObSharedGCTask> *second_ls_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.get_refactored(
      first_ls_id, first_ls_tasks));
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.get_refactored(
      second_ls_id, second_ls_tasks));
  ASSERT_NE(nullptr, first_ls_tasks);
  ASSERT_NE(nullptr, second_ls_tasks);
  ASSERT_EQ(2, first_ls_tasks->count());
  ASSERT_EQ(1, second_ls_tasks->count());

  bool found_first_task = false;
  bool found_second_task = false;
  bool found_third_task = false;
  for (ObSSLSMetaGCTaskMap::const_iterator iter = ctx->map_iter_;
       iter != ctx->map_end_; ++iter) {
    const ObLSID &expected_ls_id = iter->first;
    const ObArray<ObSharedGCTask> *gc_tasks = iter->second;
    ASSERT_NE(nullptr, gc_tasks);
    for (int64_t i = 0; i < gc_tasks->count(); ++i) {
      const ObSharedGCTask &gc_task = gc_tasks->at(i);
      ObSharedGCLSMetaInfo ls_meta_info;
      ASSERT_EQ(OB_SUCCESS, ls_meta_info.from_value(gc_task.value_));
      ASSERT_EQ(expected_ls_id, ls_meta_info.ls_id_);
      ASSERT_EQ(0, gc_task.extra_info_.case_compare("prepare-extra"));
      ASSERT_EQ(0, gc_task.trace_id_.case_compare("prepare-trace"));
      found_first_task = found_first_task
          || first_task_id == gc_task.key_.task_id_;
      found_second_task = found_second_task
          || second_task_id == gc_task.key_.task_id_;
      found_third_task = found_third_task
          || third_task_id == gc_task.key_.task_id_;
    }
  }
  ASSERT_TRUE(found_first_task);
  ASSERT_TRUE(found_second_task);
  ASSERT_TRUE(found_third_task);

  delete_ls_meta_gc_task_if_exist(first_key, sql_proxy());
  delete_ls_meta_gc_task_if_exist(second_key, sql_proxy());
  delete_ls_meta_gc_task_if_exist(third_key, sql_proxy());
}

class LSMetaGCSnapshotSqlProxy final : public ObMySQLProxy
{
public:
  LSMetaGCSnapshotSqlProxy(const ObLSID &ls_id, const ObSharedGCTaskKey &task_key)
    : original_(GCTX.sql_proxy_), injected_(false), enabled_(true),
      owner_(std::this_thread::get_id()), ls_id_(ls_id), task_key_(task_key)
  {
    assign(*original_);
    meta_type_.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  }
  void cleanup()
  {
    restore();
    if (!meta_key_.empty()) {
      ObSSMetaBaseOperator op;
      int64_t affected_rows = 0;
      EXPECT_EQ(OB_SUCCESS, op.init(*original_, OB_ALL_SHARED_META_TNAME));
      EXPECT_EQ(OB_SUCCESS, op.delete_row(OB_SYS_TENANT_ID, ls_id_, meta_type_,
          meta_key_.string(), false, affected_rows));
      meta_key_.reset();
    }
  }
  void restore()
  {
    enabled_ = false;
    if (GCTX.sql_proxy_ == this) { GCTX.sql_proxy_ = original_; }
  }
  using ObMySQLProxy::read;
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql,
      const int32_t group_id) override
  {
    int ret = OB_SUCCESS;
    if (owner_ == std::this_thread::get_id() && enabled_ && !injected_
        && nullptr != strstr(sql, "__all_shared_gc_task")) {
      injected_ = true;
      ret = inject_rows_();
    }
    return OB_SUCCESS == ret ? original_->read(result, tenant_id, sql, group_id) : ret;
  }

  ObMySQLProxy *original_;
  SCN before_inject_scn_;
  SCN tablet_meta_scn_;
  SCN task_meta_scn_;
  bool injected_;

private:
  int inject_rows_()
  {
    int ret = ObAtomicProtocolUtil::get_gts(OB_SYS_TENANT_ID, before_inject_scn_);
    const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
    // GTS may be cached. Require T > this barrier, which is already at least the old Prepare S.
    while (OB_SUCC(ret)
        && (!tablet_meta_scn_.is_valid() || tablet_meta_scn_ <= before_inject_scn_)) {
      if (ObTimeUtility::current_time() >= deadline) {
        ret = OB_TIMEOUT;
      } else {
        ret = ObAtomicProtocolUtil::get_gts(OB_SYS_TENANT_ID, 1000 * 1000L, tablet_meta_scn_);
      }
    }
    ObSSMetaBaseOperator op;
    ObArenaAllocator allocator("LSMetaSnapRows");
    ObSharedGCTask task;
    int64_t affected_rows = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_key_.append_fmt("tablet/2569001/0/meta/scn%020lu.T%d",
        tablet_meta_scn_.get_val_for_inner_table_field(),
        static_cast<int>(blocksstable::ObStorageObjectType::SHARED_TABLET_META)))) {
    } else if (OB_FAIL(op.init(*original_, OB_ALL_SHARED_META_TNAME))) {
    } else if (OB_FAIL(op.insert_row(OB_SYS_TENANT_ID, ls_id_, meta_type_, meta_key_.string(),
        ObString::make_string("unused by key-only iterator"), affected_rows))) {
    } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(OB_SYS_TENANT_ID, task_meta_scn_))) {
    } else if (OB_FAIL(build_ls_meta_gc_task(
        allocator, task_key_.task_id_, ls_id_, task_meta_scn_, task))) {
    } else {
      ret = ObSharedGCTaskTableOperator::insert_task(task, *original_);
    }
    return ret;
  }

  bool enabled_;
  const std::thread::id owner_;
  const ObLSID ls_id_;
  const ObSharedGCTaskKey task_key_;
  ObSSMetaType meta_type_;
  ObSqlString meta_key_;
};

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_prepare_snapshot_covers_tasks_collected_during_scan)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(2691);
  const ObSharedGCTaskKey task_key(OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC, 0, 998501);
  delete_ls_meta_gc_task_if_exist(task_key, sql_proxy());
  ScopedLSMetaGCTaskRowsCleanup task_cleanup(task_key, sql_proxy());
  ObArenaAllocator dag_allocator("LSMetaSnapDag");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCPrepareTask *prepare_task = find_ls_meta_gc_prepare_task(dag);
  ASSERT_NE(nullptr, prepare_task);
  // Background readers may retain the proxy after GCTX is restored; keep it alive until exit.
  static auto &proxy = *new LSMetaGCSnapshotSqlProxy(ls_id, task_key);
  DEFER(proxy.cleanup());
  GCTX.sql_proxy_ = &proxy;
  const int prepare_ret = prepare_task->process();
  proxy.restore();
  ASSERT_EQ(OB_SUCCESS, prepare_ret);
  ASSERT_TRUE(proxy.injected_);
  ASSERT_GT(proxy.tablet_meta_scn_, proxy.before_inject_scn_);
  ASSERT_GE(proxy.task_meta_scn_, proxy.tablet_meta_scn_);
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_TRUE(ctx->map_iter_inited_);
  ObArray<ObSharedGCTask> *tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.get_refactored(ls_id, tasks));
  ASSERT_NE(nullptr, tasks);
  ASSERT_EQ(1, tasks->count());
  ObSharedGCLSMetaInfo info;
  ASSERT_EQ(OB_SUCCESS, info.from_value(tasks->at(0).value_));
  ASSERT_EQ(proxy.task_meta_scn_, info.meta_scn_);

  // The tablet exists, but a snapshot taken before the task scan cannot see it.
  ObSSMetaTabletKey key;
  ObSSMetaTabletKeyIterator old_iter;
  ASSERT_EQ(OB_SUCCESS, old_iter.init(sql_proxy(), OB_SYS_TENANT_ID, ls_id,
      proxy.before_inject_scn_, false /* skip_mark_delete */));
  ASSERT_EQ(OB_ITER_END, old_iter.get_next(key));
  // Exercise the same real iterator and snapshot used by the executor's existence check.
  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy(), OB_SYS_TENANT_ID, ls_id,
      ctx->cur_gts_, false /* skip_mark_delete */));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ObTabletID(2569001), key.tablet_id_);
  ASSERT_EQ(proxy.tablet_meta_scn_, key.meta_scn_);
  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  ASSERT_GE(ctx->cur_gts_, proxy.task_meta_scn_);
}

// Use the real metadata service in the normal-mode SimpleServer without starting SS workers.
class ScopedLSMetaGCGeneratorRows final
{
public:
  ScopedLSMetaGCGeneratorRows(ObISQLClient &client, const ObLSID &ls_id)
    : client_(client), ls_id_(ls_id), allocator_("LSMetaGenRows"),
      original_tenant_(ObTenantEnv::get_tenant()), tenant_context_(OB_SYS_TENANT_ID)
  {
    tenant_context_ = *original_tenant_;
    tenant_context_.set(&meta_service_);
    tenant_context_.set(&diagnose_mgr_);
    ObTenantEnv::set_tenant(&tenant_context_);
  }
  ~ScopedLSMetaGCGeneratorRows()
  {
    ObArray<ObSharedGCTask> tasks;
    EXPECT_EQ(OB_SUCCESS, get_tasks(tasks));
    for (int64_t i = 0; i < tasks.count(); ++i) {
      EXPECT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(tasks.at(i).key_, client_));
    }
    ObSSMetaBaseOperator op;
    ObSSMetaType type;
    type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    ObSqlString prefix;
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, prefix.assign_fmt("ls/%ld/meta/", ls_id_.id()));
    EXPECT_EQ(OB_SUCCESS, op.init(client_, OB_ALL_SHARED_META_TNAME));
    EXPECT_EQ(OB_SUCCESS, op.delete_row(OB_SYS_TENANT_ID, ls_id_, type,
        prefix.string(), true, affected_rows));
    if (!lock_key_.empty()) {
      ObSSMetaCtrlTableOperator ctrl;
      EXPECT_EQ(OB_SUCCESS, ctrl.init(client_, 10 * 1000 * 1000L));
      EXPECT_EQ(OB_SUCCESS, ctrl.delete_row(OB_SYS_TENANT_ID, ls_id_,
          blocksstable::ObStorageObjectType::SHARED_LS_LOCK, lock_key_, false, affected_rows));
    }
    meta_service_.destroy();
    diagnose_mgr_.destroy();
    ObTenantEnv::set_tenant(original_tenant_);
  }
  void init(const int64_t terminal_version = 200)
  {
    ASSERT_FALSE(GCTX.is_shared_storage_mode());
    ASSERT_EQ(OB_SUCCESS, diagnose_mgr_.init());
    ASSERT_EQ(OB_SUCCESS, meta_service_.init());
    ObSSMetaCtrlKey key(blocksstable::ObStorageObjectType::SHARED_LS_LOCK, ls_id_.id(), 0, 0);
    char key_buf[SSMETA_PATH_MAX_LEN];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, key.serialize_meta_key(key_buf, sizeof(key_buf), pos));
    ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator_, ObString(pos, key_buf), lock_key_));
    ObSSMetaCtrlLockRowValue value;
    value.op_id_ = 1;
    value.meta_scn_ = make_ls_meta_gc_scn(terminal_version);
    value.is_mark_deleted_ = true;
    value.deleted_scn_ = make_ls_meta_gc_scn(50);
    value.last_major_snapshot_ = 0;
    ASSERT_TRUE(value.is_valid());
    char value_buf[4096];
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, value.serialize(value_buf, sizeof(value_buf), pos));
    ObSSMetaCtrlTableOperator ctrl;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, ctrl.init(client_, 10 * 1000 * 1000L));
    ASSERT_EQ(OB_SUCCESS, ctrl.insert_row(OB_SYS_TENANT_ID, ls_id_, key.type_,
        lock_key_, ObString(pos, value_buf), affected_rows));
  }
  void add_version(const int64_t version, const bool marked, const bool corrupt = false)
  {
    ObDefaultSSMetaValue<ObSSLSMeta> value;
    value.head_info_.mark_delete_ = marked;
    value.head_info_.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;
    value.meta_value_.ss_checkpoint_scn_ = make_ls_meta_gc_scn(version);
    const int64_t size = value.get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, value.serialize(buf, size, pos));
    ObSqlString key;
    ASSERT_EQ(OB_SUCCESS, key.assign_fmt("ls/%ld/meta/scn%020ld.T%d", ls_id_.id(), version,
        static_cast<int>(blocksstable::ObStorageObjectType::SHARED_LS_META)));
    ObSSMetaType type;
    type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    ObSSMetaBaseOperator op;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.init(client_, OB_ALL_SHARED_META_TNAME));
    ASSERT_EQ(OB_SUCCESS, op.insert_row(OB_SYS_TENANT_ID, ls_id_, type, key.string(),
        corrupt ? ObString::make_string("bad") : ObString(pos, buf), affected_rows));
  }
  int generate(const int64_t snapshot, const int64_t safe,
      std::initializer_list<int64_t> reservations)
  {
    volatile bool stopped = false;
    ObSSMarkSweepGCGenTask task(stopped);
    ObArray<SCN> reserved;
    int ret = task.init(OB_SYS_TENANT_ID);
    for (const int64_t scn : reservations) {
      if (OB_SUCC(ret)) { ret = reserved.push_back(make_ls_meta_gc_scn(scn)); }
    }
    return OB_SUCCESS == ret ? task.generate_ls_meta_gc_task_(
        make_ls_meta_gc_scn(snapshot), make_ls_meta_gc_scn(safe), reserved) : ret;
  }
  void assert_versions(std::initializer_list<int64_t> versions)
  {
    ObArray<ObSharedGCTask> tasks;
    ASSERT_EQ(OB_SUCCESS, get_tasks(tasks));
    ASSERT_EQ(versions.size(), tasks.count());
    int64_t i = 0;
    for (const int64_t version : versions) {
      ObSharedGCLSMetaInfo info;
      ASSERT_EQ(OB_SUCCESS, info.from_value(tasks.at(i++).value_));
      EXPECT_EQ(make_ls_meta_gc_scn(version), info.meta_scn_);
    }
  }
private:
  int get_tasks(ObIArray<ObSharedGCTask> &tasks)
  {
    ObSharedGCTaskLSMetaIterator iter;
    int ret = iter.init(OB_SYS_TENANT_ID, client_);
    tasks.reset();
    ObSharedGCTask task;
    while (OB_SUCC(ret) && OB_SUCC(ret = iter.get_next(allocator_, task))) {
      ObSharedGCLSMetaInfo info;
      if (OB_FAIL(info.from_value(task.value_))) {
      } else if (info.ls_id_ == ls_id_) {
        ret = tasks.push_back(task);
      }
    }
    return OB_ITER_END == ret ? OB_SUCCESS : ret;
  }
  ObISQLClient &client_;
  const ObLSID ls_id_;
  ObArenaAllocator allocator_;
  ObString lock_key_;
  ObTenantBase *original_tenant_;
  ObTenantBase tenant_context_;
  ObSSMetaService meta_service_;
  ObSSDiagnoseInfoMgr diagnose_mgr_;
};

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_generator_final_version_at_reservation_boundary)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ScopedLSMetaGCGeneratorRows rows(sql_proxy(), ObLSID(2791));
  ASSERT_NO_FATAL_FAILURE(rows.init());
  ASSERT_NO_FATAL_FAILURE(rows.add_version(100, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(200, true));
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {150}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({}));
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100, 200}));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_generator_preserves_live_snapshot_version)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ScopedLSMetaGCGeneratorRows rows(sql_proxy(), ObLSID(2792));
  ASSERT_NO_FATAL_FAILURE(rows.init(400));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(100, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(200, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(400, true));
  // The lock is deleted, but the last version visible at snapshot 300 is still live.
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100}));
  // With no reservation, min_meta_scn is 300; retain_latest must still preserve version 200.
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100}));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_generator_deduplicates_old_tasks_and_adds_final_version)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ScopedLSMetaGCGeneratorRows rows(sql_proxy(), ObLSID(2793));
  ASSERT_NO_FATAL_FAILURE(rows.init());
  ASSERT_NO_FATAL_FAILURE(rows.add_version(100, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(200, true));
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 150, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100}));
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100, 200}));
  ASSERT_EQ(OB_SUCCESS, rows.generate(300, 300, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({100, 200}));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_generator_propagates_exact_version_read_error)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ScopedLSMetaGCGeneratorRows rows(sql_proxy(), ObLSID(2794));
  ASSERT_NO_FATAL_FAILURE(rows.init());
  ASSERT_NO_FATAL_FAILURE(rows.add_version(100, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(200, true, true /* corrupt */));
  ObDefaultSSMetaValue<ObSSLSMeta> value;
  int64_t pos = 0;
  const int expected_ret = value.deserialize("bad", 3, pos);
  ASSERT_NE(OB_SUCCESS, expected_ret);
  ASSERT_EQ(expected_ret, rows.generate(300, 300, {300}));
  ASSERT_NO_FATAL_FAILURE(rows.assert_versions({}));
}

TEST_F(TestSSLSMetaGCTask, mark_sweep_deleted_ls_cache_respects_snapshot_and_gc_bounds)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(2795);
  ScopedLSMetaGCGeneratorRows rows(sql_proxy(), ls_id);
  ASSERT_NO_FATAL_FAILURE(rows.init(400));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(100, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(200, false));
  ASSERT_NO_FATAL_FAILURE(rows.add_version(400, true));

  ObSSMarkSweepGCGenTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  common::hash::ObHashMap<ObLSID, SCN> checkpoint_map;
  ASSERT_EQ(OB_SUCCESS, checkpoint_map.create(4, "DeletedLSCkpt"));
  ObArray<ObLSID> recyclable_deleted_ls_ids;
  struct DeletedLSCase
  {
    const char *name_;
    int64_t snapshot_;
    int64_t safe_;
    int64_t reservation_;
    bool expected_recyclable_;
    int64_t expected_checkpoint_;
  };
  const DeletedLSCase cases[] = {
    {"live_snapshot_with_deleted_lock", 300, 500, 500, false, 200},
    {"safe_before_delete", 500, 300, 500, false, 400},
    {"reservation_before_delete", 500, 500, 300, false, 200},
    {"reservation_before_first_version", 500, 500, 50, false, 100},
    {"bounds_at_delete", 500, 400, 400, true, 400},
    {"bounds_after_delete", 500, 500, 500, true, 400},
    {"no_reservation", 500, 500, 0, true, 400},
    {"old_snapshot_clears_cached_terminal_state", 300, 500, 500, false, 200},
  };
  for (const DeletedLSCase &test_case : cases) {
    SCOPED_TRACE(test_case.name_);
    ObArray<SCN> reserved;
    if (test_case.reservation_ > 0) {
      ASSERT_EQ(OB_SUCCESS, reserved.push_back(make_ls_meta_gc_scn(test_case.reservation_)));
    }
    ASSERT_EQ(OB_SUCCESS, task.get_all_ls_ss_checkpoint_scn_(
        make_ls_meta_gc_scn(test_case.snapshot_), make_ls_meta_gc_scn(test_case.safe_),
        reserved, SCN() /*no external checkpoint*/, checkpoint_map, recyclable_deleted_ls_ids));
    EXPECT_EQ(test_case.expected_recyclable_, has_exist_in_array(recyclable_deleted_ls_ids, ls_id));
    SCN checkpoint_scn;
    ASSERT_EQ(OB_SUCCESS, checkpoint_map.get_refactored(ls_id, checkpoint_scn));
    EXPECT_EQ(make_ls_meta_gc_scn(test_case.expected_checkpoint_), checkpoint_scn);
  }
  ASSERT_EQ(OB_SUCCESS, checkpoint_map.destroy());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_prepare_stops_before_side_effect)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaPrepStop");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCPrepareTask *prepare_task =
      find_ls_meta_gc_prepare_task(dag);
  ASSERT_NE(nullptr, prepare_task);

  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, prepare_task->process());
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_FALSE(ctx->cur_gts_.is_valid());
  ASSERT_FALSE(ctx->map_inited_);
  ASSERT_FALSE(ctx->map_iter_inited_);
  ASSERT_EQ(0, ctx->get_ls_count());
  ASSERT_EQ(0, ctx->get_gc_task_count());
  ASSERT_EQ(0, ATOMIC_LOAD(&dag.start_event_reported_));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_prepare_propagates_malformed_row)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObSharedGCTaskKey malformed_key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, 991004);
  delete_ls_meta_gc_task_if_exist(ObSharedGCTaskKey(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, 991001), sql_proxy());
  delete_ls_meta_gc_task_if_exist(ObSharedGCTaskKey(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, 991002), sql_proxy());
  delete_ls_meta_gc_task_if_exist(ObSharedGCTaskKey(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::LS_META_GC,
      0 /* round_id */, 991003), sql_proxy());
  delete_ls_meta_gc_task_if_exist(malformed_key, sql_proxy());
  ObSharedGCTask malformed_task;
  malformed_task.key_ = malformed_key;
  malformed_task.value_ = ObString::make_string("bad");
  ASSERT_TRUE(malformed_task.is_valid());
  ObSharedGCLSMetaInfo expected_info;
  const int expected_ret = expected_info.from_value(malformed_task.value_);
  ASSERT_NE(OB_SUCCESS, expected_ret);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(malformed_task, sql_proxy()));

  ObArenaAllocator dag_allocator("LSMetaPrepBad");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCPrepareTask *prepare_task =
      find_ls_meta_gc_prepare_task(dag);
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_EQ(expected_ret, prepare_task->process());

  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_TRUE(ctx->map_inited_);
  ASSERT_FALSE(ctx->map_iter_inited_);
  ASSERT_FALSE(ctx->cur_gts_.is_valid());
  ASSERT_EQ(0, ctx->get_ls_count());
  ASSERT_EQ(0, ctx->get_gc_task_count());
  delete_ls_meta_gc_task_if_exist(malformed_key, sql_proxy());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_dag_param_and_identity_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSLSMetaGCDagParam invalid_schedule_param;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      invalid_schedule_param.init(OB_SYS_TENANT_ID, -1));
  ObSSLSMetaGCDagParam first_param;
  ASSERT_FALSE(first_param.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      first_param.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, first_param.init(OB_SYS_TENANT_ID, 123));
  ASSERT_TRUE(first_param.is_valid());
  ASSERT_EQ(123, first_param.schedule_ts_);
  ASSERT_EQ(OB_INIT_TWICE, first_param.init(OB_SYS_TENANT_ID));

  ObSSLSMetaGCDagParam second_param;
  ASSERT_EQ(OB_SUCCESS, second_param.init(OB_SYS_TENANT_ID, 456));
  ObArenaAllocator first_allocator("LSMetaDagFirst");
  ObArenaAllocator second_allocator("LSMetaDagSecond");
  ObSSLSMetaGCDag first_dag;
  ObSSLSMetaGCDag second_dag;
  ASSERT_EQ(OB_SUCCESS, first_dag.basic_init(first_allocator));
  ASSERT_EQ(OB_SUCCESS, second_dag.basic_init(second_allocator));
  ASSERT_EQ(OB_SUCCESS, first_dag.init_by_param(&first_param));
  ASSERT_EQ(OB_SUCCESS, second_dag.init_by_param(&second_param));
  ASSERT_EQ(OB_INIT_TWICE, first_dag.init_by_param(&first_param));
  ASSERT_EQ(123, first_dag.get_schedule_ts());
  ASSERT_EQ(456, second_dag.get_schedule_ts());

  ObSSLSMetaGCDag fresh_probe;
  ObSSLSMetaGCDag other_tenant_probe;
  fresh_probe.set_tenant_id(OB_SYS_TENANT_ID);
  other_tenant_probe.set_tenant_id(1002);
  ASSERT_EQ(nullptr, fresh_probe.get_ctx());
  ASSERT_EQ(ObDagType::DAG_TYPE_SS_LS_META_GC, first_dag.get_type());
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN, first_dag.get_priority());
  ASSERT_TRUE(first_dag == second_dag);
  ASSERT_EQ(first_dag.hash(), second_dag.hash());
  ASSERT_TRUE(first_dag == fresh_probe);
  ASSERT_TRUE(fresh_probe == first_dag);
  ASSERT_EQ(first_dag.hash(), fresh_probe.hash());
  ASSERT_FALSE(first_dag == other_tenant_probe);

  char dag_key[128] = {0};
  ASSERT_EQ(OB_SUCCESS,
      fresh_probe.fill_dag_key(dag_key, sizeof(dag_key)));
  ASSERT_NE(nullptr, std::strstr(dag_key, "tenant_id=1"));

  ObSSLSMetaGCDagCtx *ctx = first_dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->add_ls_count(2);
  ctx->add_gc_task_count(3);
  ctx->add_deleted_ls_meta_version_count(1);
  ctx->add_finished_gc_task_count(1);
  compaction::ObInfoParamBuffer info_allocator;
  compaction::ObIBasicInfoParam *info_param = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      first_dag.fill_info_param(info_param, info_allocator));
  ASSERT_NE(nullptr, info_param);
  char comment[common::OB_DAG_WARNING_INFO_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, info_param->fill_comment(comment, sizeof(comment)));
  ASSERT_NE(nullptr, std::strstr(comment, "ls_count"));
  ASSERT_NE(nullptr, std::strstr(comment, "gc_tasks=3"));
  ASSERT_NE(nullptr,
      std::strstr(comment, "ls_meta_versions_deleted=1"));
  ASSERT_NE(nullptr, std::strstr(comment, "gc_tasks_finished=1"));

  first_dag.report_start_event();
  first_dag.report_start_event();
  ASSERT_EQ(1, ATOMIC_LOAD(&first_dag.start_event_reported_));
  first_dag.set_dag_ret(OB_TIMEOUT);
  ASSERT_EQ(OB_SUCCESS, first_dag.report_result());
  ASSERT_EQ(OB_TIMEOUT, first_dag.get_dag_ret());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_dag_static_prepare_schedule_topology)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaDagGraph");
  ObSSLSMetaGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_NOT_INIT, dag.create_first_task());
  ASSERT_EQ(0, dag.get_task_list_count());

  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ASSERT_EQ(2, dag.get_task_list_count());

  ObSSLSMetaGCPrepareTask *prepare_task = nullptr;
  ObSSLSMetaGCScheduleTask *schedule_task = nullptr;
  int64_t recognized_task_count = 0;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    ASSERT_EQ(&dag, task->get_dag());
    ASSERT_EQ(ObITask::TASK_STATUS_WAITING, task->get_status());
    ASSERT_EQ(ObITask::TASK_TYPE_SS_LS_META_GC, task->get_type());
    if (nullptr != dynamic_cast<ObSSLSMetaGCPrepareTask *>(task)) {
      ASSERT_EQ(nullptr, prepare_task);
      prepare_task = static_cast<ObSSLSMetaGCPrepareTask *>(task);
      ++recognized_task_count;
    } else if (nullptr != dynamic_cast<ObSSLSMetaGCScheduleTask *>(task)) {
      ASSERT_EQ(nullptr, schedule_task);
      schedule_task = static_cast<ObSSLSMetaGCScheduleTask *>(task);
      ++recognized_task_count;
    }
    task = task->get_next();
  }
  ASSERT_EQ(2, recognized_task_count);
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_TRUE(prepare_task->is_inited_);
  ASSERT_TRUE(schedule_task->is_inited_);
  ASSERT_EQ(0, prepare_task->get_indegree());
  ASSERT_EQ(1, schedule_task->get_indegree());

  ObSEArray<ObINodeWithChild *, 2> children;
  ASSERT_EQ(OB_SUCCESS, prepare_task->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_EQ(static_cast<ObINodeWithChild *>(schedule_task), children.at(0));
  children.reset();
  ASSERT_EQ(OB_SUCCESS, schedule_task->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());

  // Prepare has not published the map yet, so Schedule must fail closed.
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      schedule_task->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_FALSE(schedule_task->next_task_generated_);
  ASSERT_EQ(2, dag.get_task_list_count());
  ASSERT_EQ(OB_ERR_UNEXPECTED, schedule_task->process());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_schedule_empty_map_is_noop)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaEmpty");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ObSSLSMetaGCScheduleTask *schedule_task =
      find_ls_meta_gc_initial_schedule_task(dag);
  ASSERT_NE(nullptr, ctx);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_EQ(OB_SUCCESS, create_ls_meta_gc_task_map(*ctx));
  publish_ls_meta_gc_task_map(*ctx);
  ASSERT_TRUE(ctx->map_iter_ == ctx->map_end_);

  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(schedule_task)->generate_next_task());
  ASSERT_TRUE(schedule_task->next_task_generated_);
  ASSERT_EQ(2, dag.get_task_list_count());
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ITER_END, schedule_task->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_SUCCESS, schedule_task->process());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_dynamic_tasks_reject_invalid_input)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaInvalid");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ObSSLSMetaGCScheduleTask *schedule_task =
      find_ls_meta_gc_initial_schedule_task(dag);
  ASSERT_NE(nullptr, ctx);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_EQ(OB_SUCCESS, create_ls_meta_gc_task_map(*ctx));
  void *buf = ctx->allocator_.alloc(sizeof(ObArray<ObSharedGCTask>));
  ASSERT_NE(nullptr, buf);
  ObArray<ObSharedGCTask> *empty_gc_tasks =
      new (buf) ObArray<ObSharedGCTask>();
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.set_refactored(
      ObLSID(2251), empty_gc_tasks));
  publish_ls_meta_gc_task_map(*ctx);
  const ObSSLSMetaGCTaskMap::const_iterator initial_iter = ctx->map_iter_;
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      schedule_task->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_FALSE(schedule_task->next_task_generated_);
  ASSERT_TRUE(initial_iter == ctx->map_iter_);
  ASSERT_EQ(2, dag.get_task_list_count());

  ObSSLSMetaGCExecutorTask null_input_executor;
  ObSSLSMetaGCExecutorTask empty_input_executor;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      null_input_executor.init(ObLSID(2251), nullptr, false));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      empty_input_executor.init(ObLSID(2251), empty_gc_tasks, false));
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_schedule_executor_alternating_chain)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaDynamic");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ObSSLSMetaGCScheduleTask *initial_schedule =
      find_ls_meta_gc_initial_schedule_task(dag);
  ASSERT_NE(nullptr, ctx);
  ASSERT_NE(nullptr, initial_schedule);
  ASSERT_EQ(OB_SUCCESS, create_ls_meta_gc_task_map(*ctx));
  ObArray<ObSharedGCTask> *first_inserted_tasks = nullptr;
  ObArray<ObSharedGCTask> *second_inserted_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2301), 992001, first_inserted_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2302), 992002, second_inserted_tasks));
  ASSERT_NE(nullptr, first_inserted_tasks);
  ASSERT_NE(nullptr, second_inserted_tasks);
  publish_ls_meta_gc_task_map(*ctx);

  ObSSLSMetaGCTaskMap::const_iterator expected_iter = ctx->map_iter_;
  ASSERT_TRUE(expected_iter != ctx->map_end_);
  const ObLSID first_ls_id = expected_iter->first;
  const ObArray<ObSharedGCTask> *first_gc_tasks = expected_iter->second;
  ++expected_iter;
  ASSERT_TRUE(expected_iter != ctx->map_end_);
  const ObSSLSMetaGCTaskMap::const_iterator iter_at_second = expected_iter;
  const ObLSID second_ls_id = expected_iter->first;
  const ObArray<ObSharedGCTask> *second_gc_tasks = expected_iter->second;
  ++expected_iter;
  ASSERT_TRUE(expected_iter == ctx->map_end_);

  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(initial_schedule)->generate_next_task());
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_TRUE(initial_schedule->next_task_generated_);
  ASSERT_TRUE(ctx->map_iter_ == iter_at_second);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(initial_schedule)->generate_next_task());
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_TRUE(ctx->map_iter_ == iter_at_second);
  ASSERT_EQ(OB_SUCCESS, initial_schedule->process());

  ObSSLSMetaGCExecutorTask *first_executor = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    ObSSLSMetaGCExecutorTask *executor =
        dynamic_cast<ObSSLSMetaGCExecutorTask *>(task);
    if (nullptr != executor && executor->gc_tasks_ == first_gc_tasks) {
      first_executor = executor;
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, first_executor);
  ASSERT_EQ(first_ls_id, first_executor->ls_id_);
  ASSERT_TRUE(first_executor->has_more_);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(first_executor)->generate_next_task());
  ASSERT_EQ(4, dag.get_task_list_count());
  ASSERT_TRUE(first_executor->next_task_generated_);
  ASSERT_TRUE(ctx->map_iter_ == iter_at_second);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(first_executor)->generate_next_task());
  ASSERT_EQ(4, dag.get_task_list_count());
  ASSERT_TRUE(ctx->map_iter_ == iter_at_second);
  ASSERT_TRUE(ctx->map_iter_ == iter_at_second);

  ObSSLSMetaGCScheduleTask *continuation_schedule = nullptr;
  task = dag.task_list_.get_first();
  while (nullptr != task && task_list_head != task) {
    ObSSLSMetaGCScheduleTask *schedule =
        dynamic_cast<ObSSLSMetaGCScheduleTask *>(task);
    if (nullptr != schedule && schedule != initial_schedule) {
      continuation_schedule = schedule;
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, continuation_schedule);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(continuation_schedule)->generate_next_task());
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_TRUE(continuation_schedule->next_task_generated_);
  ASSERT_EQ(OB_SUCCESS, continuation_schedule->process());
  ASSERT_TRUE(ctx->map_iter_ == ctx->map_end_);

  ObSSLSMetaGCExecutorTask *second_executor = nullptr;
  int64_t prepare_count = 0;
  int64_t schedule_count = 0;
  int64_t executor_count = 0;
  task = dag.task_list_.get_first();
  while (nullptr != task && task_list_head != task) {
    ASSERT_EQ(&dag, task->get_dag());
    ASSERT_EQ(ObITask::TASK_STATUS_WAITING, task->get_status());
    ASSERT_EQ(ObITask::TASK_TYPE_SS_LS_META_GC, task->get_type());
    ObSEArray<ObINodeWithChild *, 2> children;
    ASSERT_EQ(OB_SUCCESS, task->copy_child_nodes(children));
    if (nullptr != dynamic_cast<ObSSLSMetaGCPrepareTask *>(task)) {
      ++prepare_count;
      ASSERT_EQ(0, task->get_indegree());
      ASSERT_EQ(1, children.count());
      ASSERT_EQ(static_cast<ObINodeWithChild *>(initial_schedule),
          children.at(0));
    } else if (ObSSLSMetaGCScheduleTask *schedule =
        dynamic_cast<ObSSLSMetaGCScheduleTask *>(task)) {
      ++schedule_count;
      ASSERT_EQ(schedule == initial_schedule ? 1 : 0,
          task->get_indegree());
      ASSERT_EQ(0, children.count());
    } else if (ObSSLSMetaGCExecutorTask *executor =
        dynamic_cast<ObSSLSMetaGCExecutorTask *>(task)) {
      ++executor_count;
      ASSERT_EQ(0, task->get_indegree());
      ASSERT_EQ(0, children.count());
      if (executor->gc_tasks_ == second_gc_tasks) {
        second_executor = executor;
      }
    }
    task = task->get_next();
  }
  ASSERT_EQ(1, prepare_count);
  ASSERT_EQ(2, schedule_count);
  ASSERT_EQ(2, executor_count);
  ASSERT_NE(nullptr, second_executor);
  ASSERT_EQ(second_ls_id, second_executor->ls_id_);
  ASSERT_FALSE(second_executor->has_more_);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(second_executor)->generate_next_task());
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_TRUE(second_executor->next_task_generated_);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(second_executor)->generate_next_task());
  ASSERT_EQ(5, dag.get_task_list_count());

  ASSERT_EQ(2, ctx->ls_meta_gc_tasks_map_.size());
  ObArray<ObSharedGCTask> *current_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.get_refactored(
      first_ls_id, current_tasks));
  ASSERT_EQ(first_gc_tasks, current_tasks);
  ASSERT_EQ(OB_SUCCESS, ctx->ls_meta_gc_tasks_map_.get_refactored(
      second_ls_id, current_tasks));
  ASSERT_EQ(second_gc_tasks, current_tasks);
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_sorts_versions_without_mutating_snapshot)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaExecSort");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2501);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ls_id, 994040, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  gc_tasks->reset();
  const SCN meta_scn_10 = make_ls_meta_gc_scn(251010);
  const SCN meta_scn_20 = make_ls_meta_gc_scn(251020);
  const SCN meta_scn_30 = make_ls_meta_gc_scn(251030);
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994040, meta_scn_30));
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994030, meta_scn_10));
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994020, meta_scn_20));
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994010, meta_scn_20));
  const int64_t original_task_ids[] = {
      994040, 994030, 994020, 994010};
  const char *original_value_ptrs[4] = {nullptr};
  for (int64_t i = 0; i < gc_tasks->count(); ++i) {
    original_value_ptrs[i] = gc_tasks->at(i).value_.ptr();
  }
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->add_tablet_ = true;
  executor->tablet_info_ = ObSSGCTabletBaseInfo(
      ObTabletID(2501001), make_ls_meta_gc_scn(251001));
  executor->replayed_scn_ = make_ls_meta_gc_scn(2);
  executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->replay_call_count_);
  ASSERT_EQ(4, executor->query_call_count_);
  ASSERT_EQ(4, executor->remove_call_count_);
  ASSERT_EQ(4, executor->finish_call_count_);
  ASSERT_EQ(4, executor->finished_task_ids_.count());
  ASSERT_EQ(994030, executor->finished_task_ids_.at(0));
  ASSERT_EQ(994010, executor->finished_task_ids_.at(1));
  ASSERT_EQ(994020, executor->finished_task_ids_.at(2));
  ASSERT_EQ(994040, executor->finished_task_ids_.at(3));
  ASSERT_EQ(4, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(4, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(4, gc_tasks->count());
  for (int64_t i = 0; i < gc_tasks->count(); ++i) {
    ASSERT_EQ(original_task_ids[i], gc_tasks->at(i).key_.task_id_);
    ASSERT_EQ(original_value_ptrs[i], gc_tasks->at(i).value_.ptr());
  }
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_breaks_after_first_unsafe_version)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaExecBreak");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2502);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ls_id, 994101, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  gc_tasks->reset();
  const SCN meta_scn_10 = make_ls_meta_gc_scn(252010);
  const SCN meta_scn_20 = make_ls_meta_gc_scn(252020);
  const SCN meta_scn_30 = make_ls_meta_gc_scn(252030);
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994101, meta_scn_10));
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994102, meta_scn_20));
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 994103, meta_scn_30));
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->add_tablet_ = true;
  executor->tablet_info_ = ObSSGCTabletBaseInfo(
      ObTabletID(2502001), make_ls_meta_gc_scn(252001));
  executor->replayed_scn_ = make_ls_meta_gc_scn(100);
  executor->checkpoint_scn_ = make_ls_meta_gc_scn(50);
  executor->unsafe_meta_scn_ = meta_scn_20;
  executor->unsafe_checkpoint_scn_ = make_ls_meta_gc_scn(200);
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->tablet_call_count_);
  ASSERT_EQ(1, executor->replay_call_count_);
  ASSERT_EQ(2, executor->query_call_count_);
  ASSERT_EQ(meta_scn_10, executor->queried_meta_scns_.at(0));
  ASSERT_EQ(meta_scn_20, executor->queried_meta_scns_.at(1));
  ASSERT_EQ(1, executor->remove_call_count_);
  ASSERT_EQ(1, executor->finish_call_count_);
  ASSERT_EQ(994101, executor->finished_task_ids_.at(0));
  ASSERT_EQ(1, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(1, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_allows_cold_or_missing_tablet_replay_info)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaReplay");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  ObArray<ObSharedGCTask> *cold_tasks = nullptr;
  ObArray<ObSharedGCTask> *missing_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2503), 994201, cold_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2504), 994202, missing_tasks));
  ASSERT_NE(nullptr, cold_tasks);
  ASSERT_NE(nullptr, missing_tasks);
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *cold_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2503), *cold_tasks, cold_executor));
  ASSERT_NE(nullptr, cold_executor);
  cold_executor->add_tablet_ = true;
  cold_executor->cold_ = true;
  cold_executor->tablet_info_ = ObSSGCTabletBaseInfo(
      ObTabletID(2503001), make_ls_meta_gc_scn(2503001));
  cold_executor->replayed_scn_ = make_ls_meta_gc_scn(100);
  cold_executor->checkpoint_scn_ = make_ls_meta_gc_scn(200);
  ASSERT_EQ(OB_SUCCESS, cold_executor->process());
  ASSERT_EQ(1, cold_executor->remove_call_count_);
  ASSERT_EQ(1, cold_executor->finish_call_count_);

  ScriptedSSLSMetaGCExecutorTask *missing_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2504), *missing_tasks, missing_executor));
  ASSERT_NE(nullptr, missing_executor);
  missing_executor->add_tablet_ = true;
  missing_executor->omit_replay_info_ = true;
  missing_executor->tablet_info_ = ObSSGCTabletBaseInfo(
      ObTabletID(2504001), make_ls_meta_gc_scn(2504001));
  missing_executor->replayed_scn_ = make_ls_meta_gc_scn(100);
  missing_executor->checkpoint_scn_ = make_ls_meta_gc_scn(200);
  ASSERT_EQ(OB_SUCCESS, missing_executor->process());
  ASSERT_EQ(1, missing_executor->tablet_call_count_);
  ASSERT_EQ(1, missing_executor->replay_call_count_);
  ASSERT_EQ(1, missing_executor->query_call_count_);
  ASSERT_EQ(1, missing_executor->remove_call_count_);
  ASSERT_EQ(1, missing_executor->finish_call_count_);

  ASSERT_EQ(2, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(2, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_terminal_version_waits_for_shared_tablets)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const char *scenarios[] = {"replayed", "cold", "missing_replay_info"};
  for (int64_t i = 0; i < ARRAYSIZEOF(scenarios); ++i) {
    SCOPED_TRACE(scenarios[i]);
    ObArenaAllocator dag_allocator("LSMetaTerminal");
    ObSSLSMetaGCDag dag;
    ObSSLSMetaGCDagCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
    ASSERT_NE(nullptr, ctx);
    const ObLSID ls_id(2551 + i);
    ObArray<ObSharedGCTask> *gc_tasks = nullptr;
    ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(*ctx, ls_id, 997001 + i, gc_tasks));
    ASSERT_NE(nullptr, gc_tasks);
    publish_ls_meta_gc_task_map(*ctx);
    ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
    ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(dag, ls_id, *gc_tasks, executor));
    ASSERT_NE(nullptr, executor);
    executor->is_mark_deleted_ = true;
    executor->replay_ret_ = OB_TRANS_TIMEOUT; // Replay progress must not be queried here.
    executor->add_tablet_ = true;
    executor->tablet_info_ = ObSSGCTabletBaseInfo(
        ObTabletID(2551001 + i), make_ls_meta_gc_scn(255001));
    executor->checkpoint_scn_ = make_ls_meta_gc_scn(100);
    executor->replayed_scn_ = make_ls_meta_gc_scn(0 == i ? 200 : 50);
    executor->cold_ = 1 == i;
    executor->omit_replay_info_ = 2 == i;
    ASSERT_EQ(OB_SUCCESS, executor->process());
    // Replay progress, cold state and missing replay info cannot bypass the
    // shared tablet existence check for the final mark-delete version.
    ASSERT_EQ(0, executor->replay_call_count_);
    ASSERT_EQ(1, executor->query_call_count_);
    ASSERT_EQ(0, executor->remove_call_count_);
    ASSERT_EQ(0, executor->finish_call_count_);
    ASSERT_EQ(0, ctx->get_deleted_ls_meta_version_count());
    ASSERT_EQ(0, ctx->get_finished_gc_task_count());
    ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
    ASSERT_EQ(0, ctx->get_failed_ls_count());

    // Retrying the blocked task must not build an unused replay map either.
    ASSERT_EQ(OB_SUCCESS, executor->process());
    ASSERT_EQ(0, executor->replay_call_count_);
    ASSERT_EQ(2, executor->query_call_count_);
    ASSERT_EQ(0, executor->remove_call_count_);
    ASSERT_EQ(0, executor->finish_call_count_);
    ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());

    // The retained task becomes executable after the shared tablets disappear.
    executor->add_tablet_ = false;
    ASSERT_EQ(OB_SUCCESS, executor->process());
    ASSERT_EQ(3, executor->tablet_call_count_);
    ASSERT_EQ(0, executor->replay_call_count_);
    ASSERT_EQ(3, executor->query_call_count_);
    ASSERT_EQ(1, executor->remove_call_count_);
    ASSERT_EQ(1, executor->finish_call_count_);
    ASSERT_EQ(1, ctx->get_deleted_ls_meta_version_count());
    ASSERT_EQ(1, ctx->get_finished_gc_task_count());
    ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
    ASSERT_EQ(0, ctx->get_failed_ls_count());
  }
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_executor_reuses_replay_map_before_blocked_final_version)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaMixed");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2561);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(*ctx, ls_id, 997101, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  gc_tasks->reset();
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
        *ctx, *gc_tasks, ls_id, 997101 + i, make_ls_meta_gc_scn((i + 1) * 100)));
  }
  publish_ls_meta_gc_task_map(*ctx);
  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->add_tablet_ = true;
  executor->tablet_info_ = ObSSGCTabletBaseInfo(ObTabletID(2561001), make_ls_meta_gc_scn(256001));
  executor->checkpoint_scn_ = make_ls_meta_gc_scn(100);
  executor->replayed_scn_ = make_ls_meta_gc_scn(200);
  executor->mark_deleted_meta_scn_ = make_ls_meta_gc_scn(300);
  ASSERT_EQ(OB_SUCCESS, executor->process());
  ASSERT_EQ(1, executor->replay_call_count_);
  ASSERT_EQ(3, executor->query_call_count_);
  ASSERT_EQ(2, executor->remove_call_count_);
  ASSERT_EQ(2, executor->finish_call_count_);
  ASSERT_EQ(997101, executor->finished_task_ids_.at(0));
  ASSERT_EQ(997102, executor->finished_task_ids_.at(1));
  ASSERT_EQ(2, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(2, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_executor_absorbs_business_errors_per_ls)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaTimeout");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  ObArray<ObSharedGCTask> *gc_tasks[7] = {nullptr};
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
        *ctx, ObLSID(2510 + i), 995000 + i, gc_tasks[i]));
    ASSERT_NE(nullptr, gc_tasks[i]);
  }
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks[2], ObLSID(2512), 996002,
      make_ls_meta_gc_scn(996002 + 10000)));
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *executors[7] = {nullptr};
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
        dag, ObLSID(2510 + i), *gc_tasks[i], executors[i]));
    ASSERT_NE(nullptr, executors[i]);
    executors[i]->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  }
  executors[0]->tablet_ret_ = OB_EAGAIN;
  executors[1]->add_tablet_ = true;
  executors[1]->tablet_info_ = ObSSGCTabletBaseInfo(
      ObTabletID(2511001), make_ls_meta_gc_scn(2511001));
  executors[1]->replayed_scn_ = make_ls_meta_gc_scn(100);
  executors[1]->replay_ret_ = OB_TRANS_TIMEOUT;
  executors[2]->query_ret_ = OB_TRANS_STMT_TIMEOUT;
  executors[3]->remove_ret_ = OB_TIMEOUT;
  executors[4]->finish_ret_ = OB_TRANS_RPC_TIMEOUT;
  executors[5]->tablet_ret_ = OB_CANCELED;

  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, executors[i]->process()) << i;
  }
  ASSERT_EQ(OB_EAGAIN, ctx->get_first_error_code());
  ASSERT_EQ(6, ctx->get_failed_ls_count());
  ASSERT_EQ(2, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(1, ctx->get_finished_gc_task_count());
  ASSERT_EQ(0, executors[0]->query_call_count_);
  ASSERT_EQ(1, executors[1]->query_call_count_);
  ASSERT_EQ(1, executors[1]->replay_call_count_);
  ASSERT_EQ(0, executors[1]->remove_call_count_);
  ASSERT_EQ(1, executors[2]->query_call_count_);
  ASSERT_EQ(0, executors[2]->remove_call_count_);
  ASSERT_EQ(0, executors[3]->finish_call_count_);
  ASSERT_EQ(1, executors[4]->remove_call_count_);
  ASSERT_EQ(1, executors[4]->finish_call_count_);
  ASSERT_EQ(0, executors[5]->query_call_count_);
  ASSERT_EQ(1, executors[6]->remove_call_count_);
  ASSERT_EQ(1, executors[6]->finish_call_count_);
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_records_all_ls_business_errors)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaExecError");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  ObArray<ObSharedGCTask> *first_tasks = nullptr;
  ObArray<ObSharedGCTask> *second_tasks = nullptr;
  ObArray<ObSharedGCTask> *third_tasks = nullptr;
  ObArray<ObSharedGCTask> *fourth_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2520), 995100, first_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2521), 995101, second_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2522), 995103, third_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2523), 995104, fourth_tasks));
  ASSERT_NE(nullptr, first_tasks);
  ASSERT_NE(nullptr, second_tasks);
  ASSERT_NE(nullptr, third_tasks);
  ASSERT_NE(nullptr, fourth_tasks);
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *malformed_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2599), *first_tasks, malformed_executor));
  ASSERT_NE(nullptr, malformed_executor);
  malformed_executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  ASSERT_EQ(OB_SUCCESS, malformed_executor->process());
  ASSERT_EQ(0, malformed_executor->tablet_call_count_);

  ScriptedSSLSMetaGCExecutorTask *allocation_error_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2521), *second_tasks, allocation_error_executor));
  ASSERT_NE(nullptr, allocation_error_executor);
  allocation_error_executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  allocation_error_executor->finish_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  ASSERT_EQ(OB_SUCCESS, allocation_error_executor->process());
  ASSERT_EQ(1, allocation_error_executor->remove_call_count_);
  ASSERT_EQ(1, allocation_error_executor->finish_call_count_);

  ScriptedSSLSMetaGCExecutorTask *invalid_argument_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2522), *third_tasks, invalid_argument_executor));
  ASSERT_NE(nullptr, invalid_argument_executor);
  invalid_argument_executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  invalid_argument_executor->tablet_ret_ = OB_INVALID_ARGUMENT;
  ASSERT_EQ(OB_SUCCESS, invalid_argument_executor->process());
  ASSERT_EQ(0, invalid_argument_executor->query_call_count_);

  ScriptedSSLSMetaGCExecutorTask *remove_error_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ObLSID(2523), *fourth_tasks, remove_error_executor));
  ASSERT_NE(nullptr, remove_error_executor);
  remove_error_executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  remove_error_executor->remove_ret_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, remove_error_executor->process());
  ASSERT_EQ(1, remove_error_executor->remove_call_count_);
  ASSERT_EQ(0, remove_error_executor->finish_call_count_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctx->get_first_error_code());
  ASSERT_EQ(4, ctx->get_failed_ls_count());
  ASSERT_EQ(1, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(0, ctx->get_finished_gc_task_count());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_finishes_absent_ls_task_with_real_history_tx)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaHistory");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2522);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ls_id, 995102, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  const ObSharedGCTask gc_task = gc_tasks->at(0);
  delete_ls_meta_gc_task_if_exist(gc_task.key_, sql_proxy());
  delete_ls_meta_gc_history_if_exist(gc_task.key_, sql_proxy());
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(gc_task, sql_proxy()));
  publish_ls_meta_gc_task_map(*ctx);
  const ObSSLSMetaGCTaskMap::const_iterator snapshot_iter = ctx->map_iter_;
  const char *snapshot_value_ptr = gc_tasks->at(0).value_.ptr();

  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->query_ret_ = OB_LS_NOT_EXIST;
  executor->use_real_finish_ = true;
  ASSERT_EQ(OB_SUCCESS, executor->process());
  ASSERT_EQ(0, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(1, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::delete_task(gc_task.key_, sql_proxy()));
  ObArenaAllocator history_allocator("LSMetaHistory");
  ObSharedGCTask history_task;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      gc_task.key_, history_allocator, sql_proxy(), history_task));
  ASSERT_EQ(gc_task.key_.tenant_id_, history_task.key_.tenant_id_);
  ASSERT_EQ(gc_task.key_.type_, history_task.key_.type_);
  ASSERT_EQ(gc_task.key_.round_id_, history_task.key_.round_id_);
  ASSERT_EQ(gc_task.key_.task_id_, history_task.key_.task_id_);
  ASSERT_EQ(gc_task.value_, history_task.value_);
  ASSERT_EQ(gc_task.extra_info_, history_task.extra_info_);
  ASSERT_EQ(gc_task.trace_id_, history_task.trace_id_);
  ObArray<ObSharedGCTask> *snapshot_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      ctx->ls_meta_gc_tasks_map_.get_refactored(ls_id, snapshot_tasks));
  ASSERT_EQ(gc_tasks, snapshot_tasks);
  ASSERT_TRUE(snapshot_iter == ctx->map_iter_);
  ASSERT_EQ(1, snapshot_tasks->count());
  ASSERT_EQ(snapshot_value_ptr, snapshot_tasks->at(0).value_.ptr());
  ObSharedGCLSMetaInfo snapshot_info;
  ASSERT_EQ(OB_SUCCESS,
      snapshot_info.from_value(snapshot_tasks->at(0).value_));
  ASSERT_EQ(ls_id, snapshot_info.ls_id_);
  delete_ls_meta_gc_history_if_exist(gc_task.key_, sql_proxy());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_two_dag_workers_concurrently_finish_same_task)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator observer_a_allocator("LSMetaObsA");
  ObArenaAllocator observer_b_allocator("LSMetaObsB");
  ObSSLSMetaGCDag observer_a_dag;
  ObSSLSMetaGCDag observer_b_dag;
  ObSSLSMetaGCDagCtx *observer_a_ctx = nullptr;
  ObSSLSMetaGCDagCtx *observer_b_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_ls_meta_gc_executor_test_dag(
      observer_a_allocator, observer_a_dag, observer_a_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ls_meta_gc_executor_test_dag(
      observer_b_allocator, observer_b_dag, observer_b_ctx));
  ASSERT_NE(nullptr, observer_a_ctx);
  ASSERT_NE(nullptr, observer_b_ctx);
  ASSERT_NE(observer_a_ctx, observer_b_ctx);
  const ObLSID ls_id(2540);
  const int64_t task_id = 995200;
  ObArray<ObSharedGCTask> *observer_a_tasks = nullptr;
  ObArray<ObSharedGCTask> *observer_b_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *observer_a_ctx, ls_id, task_id, observer_a_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *observer_b_ctx, ls_id, task_id, observer_b_tasks));
  ASSERT_NE(nullptr, observer_a_tasks);
  ASSERT_NE(nullptr, observer_b_tasks);
  ASSERT_NE(observer_a_tasks, observer_b_tasks);
  ASSERT_EQ(1, observer_a_tasks->count());
  ASSERT_EQ(1, observer_b_tasks->count());
  const ObSharedGCTask &observer_a_task = observer_a_tasks->at(0);
  const ObSharedGCTask &observer_b_task = observer_b_tasks->at(0);
  ASSERT_EQ(observer_a_task.key_.tenant_id_,
      observer_b_task.key_.tenant_id_);
  ASSERT_EQ(observer_a_task.key_.type_, observer_b_task.key_.type_);
  ASSERT_EQ(observer_a_task.key_.round_id_,
      observer_b_task.key_.round_id_);
  ASSERT_EQ(observer_a_task.key_.task_id_,
      observer_b_task.key_.task_id_);
  ASSERT_EQ(observer_a_task.value_, observer_b_task.value_);
  ASSERT_NE(observer_a_task.value_.ptr(), observer_b_task.value_.ptr());
  publish_ls_meta_gc_task_map(*observer_a_ctx);
  publish_ls_meta_gc_task_map(*observer_b_ctx);

  LSMetaGCConcurrentFinishBarrier finish_barrier;
  ConcurrentFinishSSLSMetaGCExecutorTask *observer_a_executor = nullptr;
  ConcurrentFinishSSLSMetaGCExecutorTask *observer_b_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      observer_a_dag.alloc_task(observer_a_executor));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_dag.alloc_task(observer_b_executor));
  ASSERT_NE(nullptr, observer_a_executor);
  ASSERT_NE(nullptr, observer_b_executor);
  observer_a_executor->set_finish_barrier(finish_barrier);
  observer_b_executor->set_finish_barrier(finish_barrier);
  observer_a_executor->query_ret_ = OB_LS_NOT_EXIST;
  observer_b_executor->query_ret_ = OB_LS_NOT_EXIST;
  ASSERT_EQ(OB_SUCCESS,
      observer_a_executor->init(ls_id, observer_a_tasks, false));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_executor->init(ls_id, observer_b_tasks, false));
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ITER_END,
      observer_a_executor->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_ITER_END,
      observer_b_executor->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);

  ScopedLSMetaGCTaskRowsCleanup cleanup(
      observer_a_task.key_, sql_proxy());
  delete_ls_meta_gc_task_if_exist(observer_a_task.key_, sql_proxy());
  delete_ls_meta_gc_history_if_exist(observer_a_task.key_, sql_proxy());
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(
          observer_a_task, sql_proxy()));

  std::atomic<int> observer_a_ret(OB_ERR_UNEXPECTED);
  std::atomic<int> observer_b_ret(OB_ERR_UNEXPECTED);
  std::thread observer_a_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret)) {
      ret = observer_a_executor->process();
    }
    observer_a_ret.store(ret, std::memory_order_release);
  });
  std::thread observer_b_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret)) {
      ret = observer_b_executor->process();
    }
    observer_b_ret.store(ret, std::memory_order_release);
  });
  observer_a_worker.join();
  observer_b_worker.join();

  const int active_delete_ret = ObSharedGCTaskTableOperator::delete_task(
      observer_a_task.key_, sql_proxy());
  ObArenaAllocator history_allocator("LSMetaObsHist");
  ObSharedGCTask history_task;
  const int history_ret = ObSharedGCTaskTableOperator::get_history(
      observer_a_task.key_, history_allocator, sql_proxy(), history_task);
  EXPECT_EQ(OB_SUCCESS, observer_a_ret.load(std::memory_order_acquire));
  EXPECT_EQ(OB_SUCCESS, observer_b_ret.load(std::memory_order_acquire));
  EXPECT_EQ(2, finish_barrier.arrived_count());
  EXPECT_FALSE(finish_barrier.timed_out());
  EXPECT_EQ(1, observer_a_executor->concurrent_finish_call_count_);
  EXPECT_EQ(1, observer_b_executor->concurrent_finish_call_count_);
  EXPECT_EQ(OB_SUCCESS, observer_a_ctx->get_first_error_code());
  EXPECT_EQ(OB_SUCCESS, observer_b_ctx->get_first_error_code());
  EXPECT_EQ(0, observer_a_ctx->get_failed_ls_count());
  EXPECT_EQ(0, observer_b_ctx->get_failed_ls_count());
  EXPECT_EQ(0, observer_a_ctx->get_deleted_ls_meta_version_count());
  EXPECT_EQ(0, observer_b_ctx->get_deleted_ls_meta_version_count());
  EXPECT_EQ(1, observer_a_ctx->get_finished_gc_task_count());
  EXPECT_EQ(1, observer_b_ctx->get_finished_gc_task_count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, active_delete_ret);
  EXPECT_EQ(OB_SUCCESS, history_ret);
  if (OB_SUCCESS == history_ret) {
    EXPECT_EQ(observer_a_task.key_.tenant_id_,
        history_task.key_.tenant_id_);
    EXPECT_EQ(observer_a_task.key_.type_, history_task.key_.type_);
    EXPECT_EQ(observer_a_task.key_.round_id_,
        history_task.key_.round_id_);
    EXPECT_EQ(observer_a_task.key_.task_id_,
        history_task.key_.task_id_);
    EXPECT_EQ(observer_a_task.value_, history_task.value_);
    EXPECT_EQ(observer_a_task.extra_info_, history_task.extra_info_);
    EXPECT_EQ(observer_a_task.trace_id_, history_task.trace_id_);
    EXPECT_EQ(observer_a_task.server_id_, history_task.server_id_);
  }
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_existing_history_is_preserved_on_duplicate_completion)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaDupHist");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2541);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ls_id, 995201, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  const ObSharedGCTask gc_task = gc_tasks->at(0);
  publish_ls_meta_gc_task_map(*ctx);
  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->query_ret_ = OB_LS_NOT_EXIST;
  executor->use_real_finish_ = true;

  ObArenaAllocator seed_allocator("LSHistSeed");
  ObSharedGCTask seeded_history = gc_task;
  seeded_history.server_id_ = 702;
  ASSERT_EQ(OB_SUCCESS, ob_write_string(seed_allocator,
      ObString::make_string("peer-finish-trace"),
      seeded_history.trace_id_));
  ScopedLSMetaGCTaskRowsCleanup cleanup(gc_task.key_, sql_proxy());
  delete_ls_meta_gc_task_if_exist(gc_task.key_, sql_proxy());
  delete_ls_meta_gc_history_if_exist(gc_task.key_, sql_proxy());
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_history(
          seeded_history, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::insert_task(gc_task, sql_proxy()));

  const int process_ret = executor->process();
  const int active_delete_ret = ObSharedGCTaskTableOperator::delete_task(
      gc_task.key_, sql_proxy());
  ObArenaAllocator history_allocator("LSMetaDupRead");
  ObSharedGCTask history_task;
  const int history_ret = ObSharedGCTaskTableOperator::get_history(
      gc_task.key_, history_allocator, sql_proxy(), history_task);
  EXPECT_EQ(OB_SUCCESS, process_ret);
  EXPECT_EQ(1, executor->finish_call_count_);
  EXPECT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  EXPECT_EQ(0, ctx->get_failed_ls_count());
  EXPECT_EQ(0, ctx->get_deleted_ls_meta_version_count());
  EXPECT_EQ(1, ctx->get_finished_gc_task_count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, active_delete_ret);
  EXPECT_EQ(OB_SUCCESS, history_ret);
  if (OB_SUCCESS == history_ret) {
    EXPECT_EQ(seeded_history.key_.tenant_id_,
        history_task.key_.tenant_id_);
    EXPECT_EQ(seeded_history.key_.type_, history_task.key_.type_);
    EXPECT_EQ(seeded_history.key_.round_id_,
        history_task.key_.round_id_);
    EXPECT_EQ(seeded_history.key_.task_id_, history_task.key_.task_id_);
    EXPECT_EQ(seeded_history.value_, history_task.value_);
    EXPECT_EQ(seeded_history.extra_info_, history_task.extra_info_);
    EXPECT_EQ(seeded_history.trace_id_, history_task.trace_id_);
    EXPECT_EQ(seeded_history.server_id_, history_task.server_id_);
  }
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_executor_observes_stop_at_next_version_boundary)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaExecStop");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_ls_meta_gc_executor_test_dag(dag_allocator, dag, ctx));
  ASSERT_NE(nullptr, ctx);
  const ObLSID ls_id(2530);
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ls_id, 995200, gc_tasks));
  ASSERT_NE(nullptr, gc_tasks);
  ASSERT_EQ(OB_SUCCESS, append_ls_meta_gc_task(
      *ctx, *gc_tasks, ls_id, 995201, make_ls_meta_gc_scn(253020)));
  publish_ls_meta_gc_task_map(*ctx);

  ScriptedSSLSMetaGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, alloc_scripted_ls_meta_gc_executor(
      dag, ls_id, *gc_tasks, executor));
  ASSERT_NE(nullptr, executor);
  executor->checkpoint_scn_ = make_ls_meta_gc_scn(1);
  executor->stop_after_remove_ = true;
  ASSERT_EQ(OB_CANCELED, executor->process());
  ASSERT_EQ(1, executor->remove_call_count_);
  ASSERT_EQ(1, executor->finish_call_count_);
  ASSERT_EQ(1, ctx->get_deleted_ls_meta_version_count());
  ASSERT_EQ(1, ctx->get_finished_gc_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLSMetaGCTask,
    ls_meta_gc_scheduler_runs_parallel_executors_and_naturally_drains)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit));
  ScopedLSMetaGCDagAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.set(old_limit + 2));

  ObSSLSMetaGCDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  ASSERT_NE(nullptr, dag);
  ScopedUnsubmittedLSMetaGCDag dag_guard(*scheduler);
  dag_guard.hold(dag);
  ObSSLSMetaGCDagParam param;
  const int64_t schedule_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID, schedule_ts));
  ASSERT_EQ(OB_SUCCESS, dag->init_by_param(&param));
  ObSSLSMetaGCDagCtx *ctx = dag->get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_EQ(OB_SUCCESS, create_ls_meta_gc_task_map(*ctx));
  const ObLSID first_ls_id(2601);
  const ObLSID second_ls_id(2602);
  ObArray<ObSharedGCTask> *first_gc_tasks = nullptr;
  ObArray<ObSharedGCTask> *second_gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, first_ls_id, 996101, first_gc_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, second_ls_id, 996102, second_gc_tasks));
  ASSERT_NE(nullptr, first_gc_tasks);
  ASSERT_NE(nullptr, second_gc_tasks);
  publish_ls_meta_gc_task_map(*ctx);
  ctx->add_ls_count(2);
  ctx->add_gc_task_count(2);

  LSMetaGCConcurrentTestState *test_state =
      new LSMetaGCConcurrentTestState();
  ASSERT_NE(nullptr, test_state);
  ConcurrentSSLSMetaGCExecutorTask *first_executor = nullptr;
  ConcurrentSSLSMetaGCExecutorTask *second_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(first_executor,
      OB_TIMEOUT, first_ls_id, first_gc_tasks, *test_state));
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(second_executor,
      OB_TRANS_TIMEOUT, second_ls_id, second_gc_tasks, *test_state));
  ASSERT_NE(nullptr, first_executor);
  ASSERT_NE(nullptr, second_executor);
  ASSERT_EQ(OB_SUCCESS,
      first_executor->init(first_ls_id, first_gc_tasks, false));
  ASSERT_EQ(OB_SUCCESS,
      second_executor->init(second_ls_id, second_gc_tasks, false));
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*first_executor));
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*second_executor));
  ASSERT_EQ(0, first_executor->get_indegree());
  ASSERT_EQ(0, second_executor->get_indegree());

  const int add_ret = scheduler->add_dag(dag);
  if (OB_SUCCESS == add_ret) {
    dag_guard.release();
  }
  ASSERT_EQ(OB_SUCCESS, add_ret);
  scheduler->notify();

  // The main-thread deadline is deliberately much shorter than the
  // Executor-side wait. A single worker cannot satisfy this assertion by
  // timing out one Executor and then running the other.
  const bool executors_are_parallel =
      test_state->both_entered_.wait(10 * 1000L);
  EXPECT_TRUE(executors_are_parallel);
  EXPECT_EQ(2,
      test_state->arrived_count_.load(std::memory_order_acquire));
  EXPECT_TRUE(
      test_state->snapshot_valid_.load(std::memory_order_acquire));
  ObSSLSMetaGCDag probe;
  probe.set_tenant_id(OB_SYS_TENANT_ID);
  bool dag_exist = false;
  bool is_emergency = false;
  const int resident_check_ret = scheduler->check_dag_exist(
      &probe, dag_exist, is_emergency);
  EXPECT_EQ(OB_SUCCESS, resident_check_ret);
  EXPECT_TRUE(dag_exist);

  test_state->release_executors_.signal();
  const bool errors_recorded =
      test_state->all_errors_recorded_.wait(30 * 1000L);
  EXPECT_TRUE(errors_recorded);
  int check_ret = OB_SUCCESS;
  const bool naturally_drained = wait_ls_meta_gc_dag_absent(
      *scheduler, OB_SYS_TENANT_ID, 10L * 1000L * 1000L, check_ret);
  bool cleanup_drained = naturally_drained;
  if (!cleanup_drained) {
    (void)scheduler->cancel_dag(&probe, true /* force_cancel */);
    scheduler->notify();
    cleanup_drained = wait_ls_meta_gc_dag_absent(
        *scheduler, OB_SYS_TENANT_ID, 30L * 1000L * 1000L, check_ret);
  }
  EXPECT_EQ(OB_SUCCESS, check_ret);
  EXPECT_TRUE(naturally_drained);
  EXPECT_TRUE(cleanup_drained);
  EXPECT_EQ(2,
      test_state->recorded_count_.load(std::memory_order_acquire));
  EXPECT_EQ(2,
      test_state->completed_count_.load(std::memory_order_acquire));
  EXPECT_TRUE(
      test_state->snapshot_valid_.load(std::memory_order_acquire));
  const int first_error =
      test_state->observed_first_error_.load(std::memory_order_acquire);
  EXPECT_TRUE(OB_TIMEOUT == first_error || OB_TRANS_TIMEOUT == first_error);
  EXPECT_EQ(2, test_state->observed_failed_ls_count_.load(
      std::memory_order_acquire));
  if (cleanup_drained) {
    delete test_state;
    test_state = nullptr;
  }
  // If drain fails, retain heap-backed state because Scheduler-owned Tasks
  // may still reference it. Leaking on a failing test is safer than a UAF.
}

TEST_F(TestSSLSMetaGCTask, ls_meta_gc_dynamic_generation_honors_stop)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LSMetaStop");
  ObSSLSMetaGCDag dag;
  ObSSLSMetaGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLSMetaGCDagCtx *ctx = dag.get_ctx();
  ObSSLSMetaGCScheduleTask *schedule_task =
      find_ls_meta_gc_initial_schedule_task(dag);
  ASSERT_NE(nullptr, ctx);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_EQ(OB_SUCCESS, create_ls_meta_gc_task_map(*ctx));
  ObArray<ObSharedGCTask> *first_gc_tasks = nullptr;
  ObArray<ObSharedGCTask> *second_gc_tasks = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2401), 993001, first_gc_tasks));
  ASSERT_EQ(OB_SUCCESS, add_ls_meta_gc_task_group(
      *ctx, ObLSID(2402), 993002, second_gc_tasks));
  publish_ls_meta_gc_task_map(*ctx);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(schedule_task)->generate_next_task());
  ASSERT_EQ(3, dag.get_task_list_count());
  const ObSSLSMetaGCTaskMap::const_iterator iter_after_first = ctx->map_iter_;

  ObSSLSMetaGCExecutorTask *executor_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr == executor_task
      && nullptr != task
      && task_list_head != task) {
    executor_task = dynamic_cast<ObSSLSMetaGCExecutorTask *>(task);
    task = task->get_next();
  }
  ASSERT_NE(nullptr, executor_task);
  ASSERT_TRUE(executor_task->has_more_);
  ASSERT_FALSE(executor_task->next_task_generated_);
  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED,
      static_cast<ObITask *>(schedule_task)->generate_next_task());
  ASSERT_EQ(OB_CANCELED,
      static_cast<ObITask *>(executor_task)->generate_next_task());
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_TRUE(iter_after_first == ctx->map_iter_);
  ASSERT_FALSE(executor_task->next_task_generated_);
  ASSERT_EQ(OB_CANCELED, schedule_task->process());
  ASSERT_EQ(OB_CANCELED, executor_task->process());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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
#include <functional>
#include <mutex>
#include <set>
#include <thread>
#include <unistd.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#define private public
#define protected public

#include "common/ob_tenant_data_version_mgr.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/utility.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "storage/ss_garbage_collector/ob_ss_gc_rpc.h"
#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_ls_meta_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "storage/ss_garbage_collector/ob_ss_gc_task_runner.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_schedule_controller.h"
#include "storage/ss_garbage_collector/task/ob_ss_fail_gc_gen_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_lock_row_gc_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_ls_meta_gc_task.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/lob/store/ob_outtable_lob_control_operator.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// Tests for the Fail GC Timer wrapper, DAG topology/context, scheduling,
// Executor row processing, and transaction semantics.
//
// Access to private members is granted via `#define private public` above.
// Tests use dedicated LS IDs to stay isolated while sharing the SYS tenant's
// ss_meta_ctrl storage.
class TestSSFailGCGenTask : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L; // 10s
  TestSSFailGCGenTask()
    : unittest::ObSimpleClusterTestBase("test_ss_fail_gc_gen_task")
  {
  }

  // Build a TABLET_META ctrl row (key + value with given status) and insert
  // it via ObSSMetaCtrlTableOperator. Caller receives the built key so the
  // same row can later be read back via read_ctrl_value_.
  int insert_tablet_meta_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t tablet_id_int,
      const share::SCN &meta_scn,
      const ObSSMetaCtrlStatus::STATUS status,
      ObSSMetaCtrlKey &out_key);

  int insert_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType type,
      const common::ObTabletID &tablet_id,
      const share::SCN &meta_scn,
      const ObSSMetaCtrlStatus::STATUS status,
      ObSSMetaCtrlKey &out_key);

  // Read back a ctrl value by (tenant, ls, key) via
  // ObSSMetaCtrlTableOperator::read_row.
  int read_ctrl_value_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &key,
      ObSSMetaCtrlValue &out_value);

  int delete_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &key);

  int get_matching_fail_gc_task_keys_(
      const uint64_t tenant_id,
      const blocksstable::ObStorageObjectType meta_type,
      const int64_t op_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      common::ObIArray<ObSharedGCTaskKey> &task_keys);

  int get_fail_gc_task_(
      const ObSharedGCTaskKey &key,
      common::ObIAllocator &allocator,
      ObSharedGCTask &task);

  int insert_lock_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key);

  // Insert an LS_LOCK_ROW that marks the LS as existing in the meta-ctrl
  // table. ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl enumerates LSes by
  // selecting DISTINCT ls_id WHERE meta_type = LS_LOCK_ROW, so a synthetic
  // LS with only TABLET_META rows is not visible without this row.
  int insert_ls_lock_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);

  int append_lob_ctrl_pk_condition_(
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      ObSqlString &sql);

public:
  volatile bool task_stopped_ = false;
};

int TestSSFailGCGenTask::append_lob_ctrl_pk_condition_(
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[128];
  int64_t pos = 0;
  if (OB_UNLIKELY(!macro_id.is_valid()
      || macro_id.get_serialize_size() > sizeof(macro_id_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos))) {
  } else if (OB_FAIL(sql.append("macro_id = "))) {
  } else if (OB_FAIL(sql_append_hex_escape_str(ObString(pos, macro_id_buf), sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND hash = %lu", hash))) {
  }
  return ret;
}

class AlwaysNeedScheduleSSFailGCGenTask : public ObSSFailGCGenTask
{
public:
  explicit AlwaysNeedScheduleSSFailGCGenTask(volatile bool &is_stopped)
    : ObSSFailGCGenTask(is_stopped),
      check_ret_(OB_SUCCESS),
      need_schedule_(true),
      check_ts_(100),
      check_count_(0)
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

  int check_ret_;
  bool need_schedule_;
  int64_t check_ts_;
  int64_t check_count_;
};

class ScriptedSSFailGCGenTask : public AlwaysNeedScheduleSSFailGCGenTask
{
public:
  explicit ScriptedSSFailGCGenTask(volatile bool &is_stopped)
    : AlwaysNeedScheduleSSFailGCGenTask(is_stopped),
      submit_ret_(OB_SUCCESS),
      probe_ret_(OB_SUCCESS),
      probe_exist_(true),
      submit_count_(0),
      probe_count_(0)
  {
  }

  virtual int submit_fail_gc_dag_(
      const ObSSFailGCDagParam &param) override
  {
    UNUSED(param);
    ++submit_count_;
    return submit_ret_;
  }

  virtual int check_fail_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return probe_ret_;
  }

  int submit_ret_;
  int probe_ret_;
  bool probe_exist_;
  int64_t submit_count_;
  int64_t probe_count_;
};

class FakeClockSSFailGCGenTask : public ObSSFailGCGenTask
{
public:
  explicit FakeClockSSFailGCGenTask(volatile bool &is_stopped)
    : ObSSFailGCGenTask(is_stopped),
      eligibility_ret_(OB_SUCCESS),
      can_schedule_(true),
      check_time_us_(0),
      interval_us_(100)
  {
  }

  virtual int check_can_schedule_fail_gc_dag_(
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

class DynamicIntervalSSFailGCGenTask final
    : public FakeClockSSFailGCGenTask
{
public:
  explicit DynamicIntervalSSFailGCGenTask(volatile bool &is_stopped)
    : FakeClockSSFailGCGenTask(is_stopped),
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
    return ObSSFailGCGenTask::check_need_schedule_dag_(
        need_schedule, check_ts);
  }

  virtual int submit_fail_gc_dag_(
      const ObSSFailGCDagParam &param) override
  {
    ++submit_count_;
    submitted_schedule_ts_ = param.schedule_ts_;
    return OB_SUCCESS;
  }

  virtual int check_fail_gc_dag_exist_(bool &exist) override
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

class StopBeforeSubmitSSFailGCGenTask
    : public ScriptedSSFailGCGenTask
{
public:
  explicit StopBeforeSubmitSSFailGCGenTask(volatile bool &is_stopped)
    : ScriptedSSFailGCGenTask(is_stopped),
      is_stopped_(is_stopped)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    need_schedule = true;
    check_ts = check_ts_;
    is_stopped_ = true;
    return OB_SUCCESS;
  }

private:
  volatile bool &is_stopped_;
};

class CancelErrorSSFailGCExecutorTask final
    : public ObSSFailGCExecutorTask
{
public:
  CancelErrorSSFailGCExecutorTask()
    : cancel_ret_(OB_EAGAIN),
      cancel_count_(0)
  {
  }

  int cancel_ret_;
  int64_t cancel_count_;

protected:
  virtual int cancel_atomic_protocol_(
      ObAtomicProtocolCtrl &protocol_ctrl) override
  {
    UNUSED(protocol_ctrl);
    ++cancel_count_;
    return cancel_ret_;
  }
};

class DeleteTimeoutSSFailGCExecutorTask final
    : public ObSSFailGCExecutorTask
{
public:
  DeleteTimeoutSSFailGCExecutorTask()
    : delete_count_(0)
  {
  }

  int64_t delete_count_;

protected:
  virtual int delete_abandoned_key_info_(
      const ObSSFailGCInfo &fail_gc_info,
      ObIAllocator &allocator,
      ObSSMetaCtrlTableOperator &table_operator) override
  {
    int ret = ObSSFailGCExecutorTask::delete_abandoned_key_info_(
        fail_gc_info, allocator, table_operator);
    ++delete_count_;
    if (OB_SUCC(ret)) {
      ret = OB_TRANS_RPC_TIMEOUT;
    }
    return ret;
  }
};

class FailGCTestSignal final
{
public:
  FailGCTestSignal() : signaled_(false) {}

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

  bool is_signaled() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return signaled_;
  }

private:
  mutable std::mutex lock_;
  std::condition_variable cond_;
  bool signaled_;
};

class FailGCTestBarrier final
{
public:
  explicit FailGCTestBarrier(const int64_t participant_count)
    : participant_count_(participant_count),
      arrived_count_(0),
      opened_(false),
      timed_out_(false)
  {
  }

  bool arrive_and_wait(const int64_t timeout_ms = 30 * 1000L)
  {
    bool passed = false;
    std::unique_lock<std::mutex> guard(lock_);
    if (timed_out_) {
      // A previous participant already tripped the watchdog.
    } else if (++arrived_count_ == participant_count_) {
      opened_ = true;
      passed = true;
      cond_.notify_all();
    } else {
      passed = cond_.wait_for(guard, std::chrono::milliseconds(timeout_ms),
          [this]() { return opened_; });
      if (!passed) {
        timed_out_ = true;
        opened_ = true;
        cond_.notify_all();
      } else if (timed_out_) {
        passed = false;
      }
    }
    return passed;
  }

private:
  const int64_t participant_count_;
  int64_t arrived_count_;
  bool opened_;
  bool timed_out_;
  std::mutex lock_;
  std::condition_variable cond_;
};

class ScopedDagAdaptiveLimit final
{
public:
  ScopedDagAdaptiveLimit(
      ObTenantDagScheduler &scheduler,
      const int64_t dag_prio,
      const int64_t old_limit)
    : scheduler_(scheduler),
      dag_prio_(dag_prio),
      old_limit_(old_limit),
      need_restore_(false)
  {
  }

  ~ScopedDagAdaptiveLimit()
  {
    const int ret = restore();
    if (OB_SUCCESS != ret) {
      LOG_WARN("fail to restore dag adaptive limit in test", KR(ret),
          K_(dag_prio), K_(old_limit));
    }
  }

  int set(const int64_t new_limit)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(need_restore_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("dag adaptive limit guard is already active", KR(ret),
          K_(dag_prio), K(new_limit));
    } else if (OB_FAIL(
        scheduler_.set_adaptive_limit(dag_prio_, new_limit))) {
      LOG_WARN("fail to set dag adaptive limit in test", KR(ret),
          K_(dag_prio), K(new_limit));
    } else {
      need_restore_ = true;
    }
    return ret;
  }

  int restore()
  {
    int ret = OB_SUCCESS;
    if (need_restore_) {
      if (OB_FAIL(
          scheduler_.set_adaptive_limit(dag_prio_, old_limit_))) {
        LOG_WARN("fail to restore dag adaptive limit in test", KR(ret),
            K_(dag_prio), K_(old_limit));
      } else {
        need_restore_ = false;
      }
      scheduler_.notify();
    }
    return ret;
  }

private:
  ObTenantDagScheduler &scheduler_;
  const int64_t dag_prio_;
  const int64_t old_limit_;
  bool need_restore_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDagAdaptiveLimit);
};

class ScopedUnsubmittedFailGCDag final
{
public:
  explicit ScopedUnsubmittedFailGCDag(ObTenantDagScheduler &scheduler)
    : scheduler_(scheduler),
      dag_(nullptr)
  {
  }

  ~ScopedUnsubmittedFailGCDag()
  {
    if (nullptr != dag_) {
      scheduler_.free_dag(*dag_);
    }
  }

  void hold(ObSSFailGCDag *dag) { dag_ = dag; }
  void release() { dag_ = nullptr; }

private:
  ObTenantDagScheduler &scheduler_;
  ObSSFailGCDag *dag_;
  DISALLOW_COPY_AND_ASSIGN(ScopedUnsubmittedFailGCDag);
};

class LateSubmitFailGCTestTimer final : public ObTimerTask
{
public:
  LateSubmitFailGCTestTimer(
      ObTenantDagScheduler &scheduler,
      const uint64_t tenant_id,
      FailGCTestSignal &entered,
      FailGCTestSignal &allow_submit,
      FailGCTestSignal &submitted,
      FailGCTestSignal &allow_return,
      std::atomic<int> &submit_ret)
    : scheduler_(scheduler),
      tenant_id_(tenant_id),
      entered_(entered),
      allow_submit_(allow_submit),
      submitted_(submitted),
      allow_return_(allow_return),
      submit_ret_(submit_ret)
  {
  }

  virtual void runTimerTask() override
  {
    int ret = OB_SUCCESS;
    entered_.signal();
    if (!allow_submit_.wait()) {
      ret = OB_TIMEOUT;
    } else {
      ObSSFailGCDagParam param;
      if (OB_FAIL(param.init(tenant_id_))) {
        LOG_WARN("fail to init late submit fail gc dag param", KR(ret),
            K_(tenant_id));
      } else if (OB_FAIL(
          scheduler_.create_and_add_dag<ObSSFailGCDag>(&param))) {
        LOG_WARN("fail to submit late fail gc dag", KR(ret), K_(tenant_id));
      }
    }
    submit_ret_.store(ret, std::memory_order_release);
    submitted_.signal();
    (void) allow_return_.wait();
  }

private:
  ObTenantDagScheduler &scheduler_;
  const uint64_t tenant_id_;
  FailGCTestSignal &entered_;
  FailGCTestSignal &allow_submit_;
  FailGCTestSignal &submitted_;
  FailGCTestSignal &allow_return_;
  std::atomic<int> &submit_ret_;
  DISALLOW_COPY_AND_ASSIGN(LateSubmitFailGCTestTimer);
};

class FakeLocalSSGCScheduler final
{
public:
  typedef std::function<int()> Work;

  explicit FakeLocalSSGCScheduler(Work work)
    : work_(work),
      resident_(false),
      resident_type_(ObDagType::DAG_TYPE_MAX),
      resident_tenant_id_(OB_INVALID_TENANT_ID),
      cancel_requested_(false),
      drain_wait_timed_out_(false),
      work_ret_(OB_SUCCESS)
  {
  }

  ~FakeLocalSSGCScheduler()
  {
    release_business();
    join();
  }

  template <typename DagParam>
  int submit(
      const ObDagType::ObDagTypeEnum dag_type,
      const DagParam &param)
  {
    int ret = OB_SUCCESS;
    std::lock_guard<std::mutex> guard(lock_);
    if (OB_UNLIKELY(ObDagType::DAG_TYPE_MAX == dag_type
        || !param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (resident_) {
      ret = dag_type == resident_type_
          && param.tenant_id_ == resident_tenant_id_
          ? OB_EAGAIN : OB_SIZE_OVERFLOW;
    } else if (worker_.joinable()) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      resident_ = true;
      resident_type_ = dag_type;
      resident_tenant_id_ = param.tenant_id_;
      cancel_requested_ = false;
      work_ret_ = OB_SUCCESS;
      worker_ = std::thread(&FakeLocalSSGCScheduler::run_, this);
    }
    return ret;
  }

  int probe(
      const ObDagType::ObDagTypeEnum dag_type,
      const uint64_t tenant_id,
      bool &exist) const
  {
    std::lock_guard<std::mutex> guard(lock_);
    exist = resident_
        && dag_type == resident_type_
        && tenant_id == resident_tenant_id_;
    return OB_SUCCESS;
  }

  int cancel(
      const ObDagType::ObDagTypeEnum dag_type,
      const uint64_t tenant_id)
  {
    bool need_signal = false;
    {
      std::lock_guard<std::mutex> guard(lock_);
      if (resident_
          && dag_type == resident_type_
          && tenant_id == resident_tenant_id_) {
        cancel_requested_ = true;
        need_signal = true;
      }
    }
    if (need_signal) {
      cancel_seen_.signal();
    }
    return OB_SUCCESS;
  }

  void release_business() { release_business_.signal(); }
  bool wait_entered() { return entered_.wait(); }
  bool wait_cancel_seen() { return cancel_seen_.wait(); }
  bool wait_finished() { return finished_.wait(); }

  void wait_until_absent()
  {
    std::unique_lock<std::mutex> guard(lock_);
    if (!state_cond_.wait_for(guard, std::chrono::seconds(30),
        [this]() { return !resident_; })) {
      drain_wait_timed_out_ = true;
      guard.unlock();
      release_business();
    }
  }

  bool is_resident() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return resident_;
  }

  bool is_cancel_requested() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return cancel_requested_;
  }

  bool drain_wait_timed_out() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return drain_wait_timed_out_;
  }

  int get_work_ret() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return work_ret_;
  }

  void join()
  {
    if (worker_.joinable()) {
      worker_.join();
    }
  }

private:
  void run_()
  {
    entered_.signal();
    int ret = OB_SUCCESS;
    if (!release_business_.wait()) {
      ret = OB_TIMEOUT;
    } else {
      bool canceled = false;
      {
        std::lock_guard<std::mutex> guard(lock_);
        canceled = cancel_requested_;
      }
      ret = canceled ? OB_CANCELED : work_();
    }
    {
      std::lock_guard<std::mutex> guard(lock_);
      work_ret_ = ret;
      resident_ = false;
      resident_type_ = ObDagType::DAG_TYPE_MAX;
      resident_tenant_id_ = OB_INVALID_TENANT_ID;
    }
    state_cond_.notify_all();
    finished_.signal();
  }

private:
  Work work_;
  mutable std::mutex lock_;
  std::condition_variable state_cond_;
  bool resident_;
  ObDagType::ObDagTypeEnum resident_type_;
  uint64_t resident_tenant_id_;
  bool cancel_requested_;
  bool drain_wait_timed_out_;
  int work_ret_;
  FailGCTestSignal entered_;
  FailGCTestSignal cancel_seen_;
  FailGCTestSignal release_business_;
  FailGCTestSignal finished_;
  std::thread worker_;
};

class FakeObserverFailGCTimer final
    : public AlwaysNeedScheduleSSFailGCGenTask
{
public:
  FakeObserverFailGCTimer(
      volatile bool &is_stopped,
      FakeLocalSSGCScheduler &scheduler)
    : AlwaysNeedScheduleSSFailGCGenTask(is_stopped),
      scheduler_(scheduler)
  {
  }

  int submit_duplicate()
  {
    int ret = OB_SUCCESS;
    ObSSFailGCDagParam param;
    if (OB_FAIL(param.init(tenant_id_))) {
    } else {
      ret = scheduler_.submit(ObSSFailGCDag::DAG_TYPE, param);
    }
    return ret;
  }

  int probe_identity(bool &exist)
  {
    return scheduler_.probe(
        ObSSFailGCDag::DAG_TYPE, tenant_id_, exist);
  }

  int probe_other_tenant(bool &exist)
  {
    return scheduler_.probe(
        ObSSFailGCDag::DAG_TYPE, tenant_id_ + 1, exist);
  }

protected:
  virtual int submit_fail_gc_dag_(
      const ObSSFailGCDagParam &param) override
  {
    return scheduler_.submit(ObSSFailGCDag::DAG_TYPE, param);
  }

  virtual int check_fail_gc_dag_exist_(bool &exist) override
  {
    return probe_identity(exist);
  }

private:
  FakeLocalSSGCScheduler &scheduler_;
};

int TestSSFailGCGenTask::insert_tablet_meta_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t tablet_id_int,
    const share::SCN &meta_scn,
    const ObSSMetaCtrlStatus::STATUS status,
    ObSSMetaCtrlKey &out_key)
{
  return insert_ctrl_row_(
      tenant_id,
      ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(tablet_id_int),
      meta_scn,
      status,
      out_key);
}

int TestSSFailGCGenTask::insert_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType type,
    const common::ObTabletID &tablet_id,
    const share::SCN &meta_scn,
    const ObSSMetaCtrlStatus::STATUS status,
    ObSSMetaCtrlKey &out_key)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSFailGC");

  out_key.reset();
  out_key.type_ = type;
  if (out_key.is_ls_type()) {
    out_key.ls_key_.first_id_ = ls_id.id();
    out_key.ls_key_.second_id_ = 0;
    out_key.ls_key_.control_scn_ = meta_scn;
  } else {
    out_key.tablet_key_.tablet_id_ = tablet_id;
    out_key.tablet_key_.reorg_scn_.set_min();
    out_key.tablet_key_.control_scn_ = meta_scn;
    out_key.tablet_key_.ls_id_ = ls_id;
  }

  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  // Status state machine: INVALID can only transition to START; COMPLETE /
  // ABANDON must come from START. So always enter START first, then upgrade.
  if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret));
  } else if (ObSSMetaCtrlStatus::STATUS::COMPLETE == status
      && OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_complete())) {
    LOG_WARN("failed to set status complete", K(ret));
  } else if (ObSSMetaCtrlStatus::STATUS::ABANDON == status
      && OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
    LOG_WARN("failed to set status abandon", K(ret));
  }
  // ObSSMetaCtrlObjectInfo::is_valid() requires type_ to be set; pick NONE so
  // the type-specific field checks (cg_cnt_/start_seq_/base_/...) are skipped.
  ctrl_value.object_info_.set_none();

  if (OB_SUCC(ret) && (!out_key.is_valid() || !ctrl_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared key/value", K(ret), K(out_key), K(ctrl_value));
  }

  // Serialize key / value.
  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(out_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(out_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc key_buf failed", K(ret), K(key_size));
  } else if (OB_FAIL(out_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize meta key", K(ret));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = ctrl_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc value_buf failed", K(ret), K(value_size));
    } else if (OB_FAIL(ctrl_value.serialize(value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize ctrl value", K(ret), K(ctrl_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  // Insert.
  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, out_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert ctrl row", K(ret), K(tenant_id), K(ls_id),
          K(out_key), K(ctrl_value));
    }
  }
  return ret;
}

int TestSSFailGCGenTask::read_ctrl_value_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &key,
    ObSSMetaCtrlValue &out_value)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSFailGC");
  out_value.reset();

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  if (OB_FAIL(key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize meta key", K(ret));
  } else {
    ObString meta_key_str(key_pos, key_buf);
    ObString meta_value_str;
    ObSSMetaCtrlTableOperator op;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.read_row(tenant_id, ls_id, key.type_, meta_key_str,
                                   allocator, meta_value_str))) {
      LOG_WARN("failed to read row", K(ret), K(tenant_id), K(ls_id), K(key));
    } else {
      int64_t des_pos = 0;
      if (OB_FAIL(out_value.deserialize(meta_value_str.ptr(),
                                        meta_value_str.length(), des_pos))) {
        LOG_WARN("failed to deserialize ctrl value", K(ret));
      }
    }
  }
  return ret;
}

int TestSSFailGCGenTask::delete_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &key)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSFailGC");
  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(key));
  } else if (OB_ISNULL(
      key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(
      key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize meta key", K(ret), K(key));
  } else {
    ObSSMetaCtrlTableOperator op;
    const ObString meta_key(key_pos, key_buf);
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.delete_row(
        tenant_id, ls_id, key.type_, meta_key,
        false /* prefix_delete */, affected_rows))) {
      LOG_WARN("failed to delete ctrl row", K(ret), K(tenant_id), K(ls_id),
          K(key));
    } else if (OB_UNLIKELY(0 != affected_rows && 1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key));
    }
  }
  return ret;
}

int TestSSFailGCGenTask::get_matching_fail_gc_task_keys_(
    const uint64_t tenant_id,
    const blocksstable::ObStorageObjectType meta_type,
    const int64_t op_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    common::ObIArray<ObSharedGCTaskKey> &task_keys)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSharedGCTaskTableRangeIterator iter;
  ObArenaAllocator allocator("TestSSFailGC");
  const ObSSGCTaskIdRange range(0, INT64_MAX);
  task_keys.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || op_id < 0
      || !ls_id.is_valid()
      || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(iter.init(
      tenant_id, ObSharedGCTaskType::FAIL_GC, 0 /* round_id */,
      range, proxy))) {
    LOG_WARN("failed to init fail gc task iterator", K(ret), K(tenant_id));
  } else {
    while (OB_SUCC(ret)) {
      ObSharedGCTask task;
      ObSharedGCFailInfo fail_info;
      allocator.reuse();
      if (OB_FAIL(iter.get_next(allocator, task))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("failed to iterate fail gc task", K(ret), K(tenant_id));
      } else if (OB_FAIL(fail_info.from_value(task.value_))) {
        LOG_WARN("failed to deserialize fail gc task", K(ret), K(task));
      } else if (fail_info.meta_type_ == meta_type
          && fail_info.op_id_ == op_id
          && fail_info.ls_id_ == ls_id
          && fail_info.tablet_id_ == tablet_id
          && OB_FAIL(task_keys.push_back(task.key_))) {
        LOG_WARN("failed to save matching task key", K(ret), K(task));
      }
    }
  }
  return ret;
}

int TestSSFailGCGenTask::get_fail_gc_task_(
    const ObSharedGCTaskKey &key,
    ObIAllocator &allocator,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSharedGCTaskTableRangeIterator iter;
  const ObSSGCTaskIdRange range(key.task_id_, key.task_id_);
  task.reset();
  if (OB_UNLIKELY(!key.is_valid()
      || ObSharedGCTaskType::FAIL_GC != key.type_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(iter.init(
      key.tenant_id_, key.type_, key.round_id_, range, proxy))) {
    LOG_WARN("failed to init fail gc task iterator", K(ret), K(key));
  } else if (OB_FAIL(iter.get_next(allocator, task))) {
    LOG_WARN("failed to read fail gc task", K(ret), K(key));
  } else if (OB_UNLIKELY(task.key_.tenant_id_ != key.tenant_id_
      || task.key_.type_ != key.type_
      || task.key_.round_id_ != key.round_id_
      || task.key_.task_id_ != key.task_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fail gc task key", K(ret), K(key), K(task));
  }
  return ret;
}

int TestSSFailGCGenTask::insert_lock_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSFailGC");

  ObSSMetaCtrlKey lock_key;
  if (OB_FAIL(ctrl_key.get_lock_row_key(lock_key))) {
    LOG_WARN("failed to derive lock row key", K(ret), K(ctrl_key));
  }

  // Value: valid lock-row payload; op_id/is_mark_deleted/meta_scn/deleted_scn
  // are the observable fields. Any is_valid()-passing combo is fine for our
  // cancel-path test; we don't assert on lock row state afterwards.
  ObSSMetaCtrlLockRowValue lock_value;
  lock_value.is_mark_deleted_ = false;
  lock_value.op_id_ = 1;
  lock_value.meta_scn_.set_min();
  lock_value.deleted_scn_.reset();

  if (OB_SUCC(ret) && (!lock_key.is_valid() || !lock_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared lock key/value", K(ret), K(lock_key), K(lock_value));
  }

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(lock_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(lock_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize lock key", K(ret));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = lock_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(lock_value.serialize(value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize lock value", K(ret), K(lock_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, lock_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert lock row", K(ret), K(tenant_id), K(ls_id),
          K(lock_key));
    }
  }
  return ret;
}

int TestSSFailGCGenTask::insert_ls_lock_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSFailGC");

  // Key: type=LS_LOCK_ROW, ls_key_.{first_id,second_id}=0, meta_scn=min.
  // ObSSMetaCtrlKey::is_valid() for LS_LOCK_ROW checks exactly these fields.
  ObSSMetaCtrlKey ls_key;
  ls_key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
  ls_key.ls_key_.first_id_ = ls_id.id();
  ls_key.ls_key_.second_id_ = 0;
  ls_key.ls_key_.control_scn_.set_min();

  ObSSMetaCtrlLockRowValue ls_value;
  ls_value.is_mark_deleted_ = false;
  ls_value.op_id_ = 1;
  ls_value.meta_scn_.set_min();
  ls_value.deleted_scn_.reset();

  if (!ls_key.is_valid() || !ls_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared ls lock key/value", K(ret), K(ls_key), K(ls_value));
  }

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(ls_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ls_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize ls lock key", K(ret));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = ls_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ls_value.serialize(value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize ls lock value", K(ret), K(ls_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, ls_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert ls lock row", K(ret), K(tenant_id), K(ls_id),
          K(ls_key));
    }
  }
  return ret;
}

// Small helper: build an SCN from a nanosecond counter for tests. We just
// need ordered SCNs; exact wall-clock meaning is irrelevant here.
static share::SCN make_scn(const int64_t val_ns)
{
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_gts(val_ns));
  return scn;
}

// ============================================================================
// init / destroy
// ============================================================================

TEST_F(TestSSFailGCGenTask, init_invalid_tenant_id)
{
  ObSSFailGCGenTask task(task_stopped_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(OB_INVALID_TENANT_ID));
  ASSERT_FALSE(task.is_inited_);
}

TEST_F(TestSSFailGCGenTask, init_twice)
{
  ObSSFailGCGenTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_INIT_TWICE, task.init(OB_SYS_TENANT_ID));
  task.destroy();
}

TEST_F(TestSSFailGCGenTask, init_and_destroy_reset_state)
{
  ObSSFailGCGenTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  ASSERT_TRUE(task.is_inited_);
  task.destroy();
  ASSERT_FALSE(task.is_inited_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, task.tenant_id_);
}

// ============================================================================
// input validation
// ============================================================================

TEST_F(TestSSFailGCGenTask, get_all_meta_ctrl_ls_ids_invalid_tenant)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl(
          OB_INVALID_TENANT_ID, proxy, SQL_TIMEOUT_US, ls_ids));
}

// 场景：同一个未注册到 DAG Net/DAG map 的 owner id 分别在升级前后查询。
// 触发：通过真实 self-RPC 调用 check_atomic_protocol_exist。
// 预期：4.6.2.0 保守返回存在，达到 4.6.2.1 后按真实查询返回不存在。
TEST_F(TestSSFailGCGenTask, check_atomic_protocol_exist_respects_data_version)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  cluster_version.update_data_version(0);

  ObCheckAtomicProtocolArg arg;
  arg.tenant_id_ = OB_SYS_TENANT_ID;
  arg.trace_id_.init(GCTX.self_addr());

  uint64_t actual_data_version = 0;
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_0);
  ASSERT_EQ(OB_SUCCESS,
      GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_2_0, actual_data_version);
  obrpc::Bool old_version_result(false);
  obrpc::ObSSGCProxy old_version_proxy;
  ASSERT_EQ(OB_SUCCESS,
      old_version_proxy.to(GCTX.self_addr())
          .by(OB_SYS_TENANT_ID)
          .timeout(SQL_TIMEOUT_US)
          .check_atomic_protocol_exist(arg, old_version_result));
  ASSERT_TRUE(old_version_result);

  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_1);
  ASSERT_EQ(OB_SUCCESS,
      GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_2_1, actual_data_version);
  obrpc::Bool new_version_result(true);
  obrpc::ObSSGCProxy new_version_proxy;
  ASSERT_EQ(OB_SUCCESS,
      new_version_proxy.to(GCTX.self_addr())
          .by(OB_SYS_TENANT_ID)
          .timeout(SQL_TIMEOUT_US)
          .check_atomic_protocol_exist(arg, new_version_result));
  ASSERT_FALSE(new_version_result);
}

TEST_F(TestSSFailGCGenTask, gc_dag_util_rejects_task_without_dag)
{
  ObSSFailGCPrepareTask task;
  ObSSFailGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(task, ctx));
  ASSERT_EQ(nullptr, ctx);
}

TEST_F(TestSSFailGCGenTask, gc_dag_util_rejects_wrong_dag_type)
{
  ObArenaAllocator dag_allocator("WrongDagType");
  ObSSLockRowGCDag lock_row_dag;
  ObSSLockRowGCPrepareTask *task = nullptr;
  ObSSFailGCDagCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, lock_row_dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, lock_row_dag.alloc_task(task));
  ASSERT_NE(nullptr, task);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(*task, ctx));
  ASSERT_EQ(nullptr, ctx);
}

// Exercise the exact inner-tablet fail-GC cancel chain for both key families
// that carry ls_id in fourth_id_. Before prepare_lock_row_ delegated to
// get_lock_row_key(), its hand-built lock key lost fourth_id_, so cancel was
// retried forever without moving either row to ABANDON.
TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_process_ctrl_row_cancels_inner_tablet_rows)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObTabletID tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  const share::SCN row_meta_scn = make_scn(100);
  const share::SCN gc_scn = make_scn(500);
  struct TestCase
  {
    blocksstable::ObStorageObjectType type_;
    ObSSMetaCtrlStatus::STATUS status_;
    ObLSID ls_id_;
  };
  const TestCase cases[] = {
    {blocksstable::ObStorageObjectType::SHARED_TABLET_META,
     ObSSMetaCtrlStatus::STATUS::START,
     ObLSID(80)},
    {blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL,
     ObSSMetaCtrlStatus::STATUS::COMPLETE,
     ObLSID(81)},
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(i);
    const TestCase &test_case = cases[i];
    ObSSMetaCtrlKey ctrl_key;
    ASSERT_EQ(OB_SUCCESS,
        insert_ctrl_row_(OB_SYS_TENANT_ID, test_case.ls_id_, test_case.type_,
                         tablet_id, row_meta_scn, test_case.status_, ctrl_key));
    ASSERT_EQ(test_case.ls_id_.id(), ctrl_key.fourth_id_);

    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, ctrl_key.get_lock_row_key(lock_key));
    ASSERT_EQ(test_case.ls_id_.id(), lock_key.fourth_id_);
    ASSERT_EQ(OB_SUCCESS,
        insert_lock_row_(OB_SYS_TENANT_ID, test_case.ls_id_, ctrl_key));

    ObSSMetaCtrlValue ctrl_value;
    ASSERT_EQ(OB_SUCCESS,
        read_ctrl_value_(OB_SYS_TENANT_ID, test_case.ls_id_, ctrl_key, ctrl_value));
    if (ObSSMetaCtrlStatus::STATUS::START == test_case.status_) {
      ASSERT_TRUE(ctrl_value.ctrl_info_.status_.is_start_status());
    } else {
      ASSERT_TRUE(ctrl_value.ctrl_info_.status_.is_complete_status());
    }

    ObSSFailGCDagCtx dag_ctx;
    ASSERT_EQ(OB_SUCCESS, dag_ctx.init(OB_SYS_TENANT_ID));
    dag_ctx.sql_timeout_us_ = SQL_TIMEOUT_US;
    dag_ctx.gc_scn_ = gc_scn;
    dag_ctx.need_gc_ = true;
    ObSSFailGCLSCtx *ls_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        dag_ctx.alloc_ls_ctx(test_case.ls_id_, ls_ctx));
    ASSERT_NE(nullptr, ls_ctx);

    ObSSFailGCCtrlRow ctrl_row;
    ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
    ctrl_row.ctrl_value_ = ctrl_value;
    ASSERT_TRUE(ctrl_row.is_valid());
    ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
    ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

    ObSSFailGCExecutorTask executor;
    ASSERT_EQ(OB_SUCCESS, executor.init(ls_ctx, ctrl_rows));
    ASSERT_TRUE(executor.fail_gc_tablet_map_.created());
    ObSEArray<ObSSFailGCInfo, 1> abandoned_key_infos;
    ASSERT_EQ(OB_SUCCESS,
        executor.process_ctrl_row_(
            dag_ctx, executor.ctrl_rows_.at(0), abandoned_key_infos));

    ObSSMetaCtrlValue abandoned_value;
    ASSERT_EQ(OB_SUCCESS,
        read_ctrl_value_(
            OB_SYS_TENANT_ID, test_case.ls_id_, ctrl_key, abandoned_value));
    ASSERT_TRUE(abandoned_value.ctrl_info_.status_.is_abandon_status());
    ASSERT_EQ(1, abandoned_key_infos.count());
    ASSERT_EQ(test_case.ls_id_.id(),
        abandoned_key_infos.at(0).key_info_.key_.fourth_id_);
    ASSERT_TRUE(abandoned_key_infos.at(0).need_generate_task_);
  }
}

// ============================================================================
// DAG utility helpers
// ============================================================================

// After inserting rows under a fresh LS, the DAG utility must return that LS.
// Exact count is not asserted because neighboring tests share tenant_id=1.
TEST_F(TestSSFailGCGenTask, get_all_meta_ctrl_ls_ids_returns_inserted)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const ObLSID ls = ObLSID(76);
  const share::SCN row_meta_scn = make_scn(100);
  ObSSMetaCtrlKey key;
  ASSERT_EQ(OB_SUCCESS,
            insert_tablet_meta_row_(OB_SYS_TENANT_ID, ls, 760, row_meta_scn,
                                    ObSSMetaCtrlStatus::STATUS::START, key));
  // ObSSMetaCtrlLSIterator filters on meta_type = LS_LOCK_ROW, so a TABLET_META
  // row alone is not enough to make the LS visible to the DAG utility.
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, ls));

  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl(
      OB_SYS_TENANT_ID, proxy, SQL_TIMEOUT_US, ls_ids));
  bool found = false;
  for (int64_t i = 0; !found && i < ls_ids.count(); ++i) {
    found = (ls_ids.at(i) == ls);
  }
  ASSERT_TRUE(found);
}

TEST_F(TestSSFailGCGenTask, get_fail_gc_scn_produces_valid_scn)
{
  share::SCN gc_scn;
  bool need_gc = false;
  ASSERT_EQ(OB_SUCCESS, ObSSFailGCDagUtil::get_fail_gc_scn(
      OB_SYS_TENANT_ID, need_gc, gc_scn));
  ASSERT_TRUE(need_gc);
  ASSERT_TRUE(gc_scn.is_valid());
}

// A zero shared-meta timeout disables Fail GC without reading GTS.
TEST_F(TestSSFailGCGenTask, get_fail_gc_scn_disabled_when_timeout_zero)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, proxy.write(
      "alter system set _shared_meta_operation_timeout = '0' tenant sys", affected_rows));
  // wait for the tenant config to take effect.
  sleep(3);

  share::SCN gc_scn;
  bool need_gc = true;
  ASSERT_EQ(OB_SUCCESS, ObSSFailGCDagUtil::get_fail_gc_scn(
      OB_SYS_TENANT_ID, need_gc, gc_scn));
  ASSERT_FALSE(need_gc);
  ASSERT_FALSE(gc_scn.is_valid());

  // restore default so other tests are unaffected.
  ASSERT_EQ(OB_SUCCESS, proxy.write(
      "alter system set _shared_meta_operation_timeout = '1h' tenant sys", affected_rows));
  sleep(3);
}

TEST_F(TestSSFailGCGenTask, lob_ctrl_fail_gc_disabled_when_timeout_zero)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_GE(GET_MIN_CLUSTER_VERSION(), CLUSTER_VERSION_4_6_3_1);
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, proxy.write(
      "alter system set _outtable_lob_fail_gc_timeout = '0' tenant sys", affected_rows));
  sleep(3);

  ObSSFailGCDagCtx ctx;
  ObSSLobCtrlFailGCTask task;
  ASSERT_EQ(OB_SUCCESS, ctx.init(OB_SYS_TENANT_ID));
  ctx.sql_timeout_us_ = 0;
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_SUCCESS, task.process_lob_ctrl_fail_gc_(ctx));

  share::SCN gc_scn;
  bool need_gc = false;
  ASSERT_EQ(OB_SUCCESS, ObSSFailGCDagUtil::get_fail_gc_scn(
      OB_SYS_TENANT_ID, need_gc, gc_scn));
  ASSERT_TRUE(need_gc);
  ASSERT_TRUE(gc_scn.is_valid());

  ASSERT_EQ(OB_SUCCESS, proxy.write(
      "alter system set _outtable_lob_fail_gc_timeout = '1h' tenant sys", affected_rows));
  sleep(3);
}

// ============================================================================
// new helpers introduced for major-sstable lock-row routing
// ============================================================================

// On a valid lock-row key, the common DAG utility must produce a non-empty
// buffer whose path-encoded fields round-trip via deserialize_meta_key. For a
// regular tablet, ls_id is carried by the control-table row key rather than by
// meta_key; only an LS inner-tablet path embeds it.
TEST_F(TestSSFailGCGenTask, serialize_meta_key_round_trips_lock_row)
{
  ObSSMetaCtrlKey lock_key;
  lock_key.type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_LOCK;
  lock_key.sstable_key_.tablet_id_ = ObTabletID(781);
  lock_key.sstable_key_.reorg_scn_.set_min();
  lock_key.sstable_key_.control_scn_.set_min();
  lock_key.sstable_key_.ls_id_ = ObLSID(78);

  ObArenaAllocator allocator("TestSSFailGC");
  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS,
            ObSSGCDagUtil::serialize_meta_key(
                lock_key, allocator, meta_key));
  ASSERT_GT(meta_key.length(), 0);

  ObSSMetaCtrlKey round_trip;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            round_trip.deserialize_meta_key(meta_key.ptr(), meta_key.length(), pos));
  ASSERT_EQ(meta_key.length(), pos);
  ASSERT_EQ(lock_key.type_, round_trip.type_);
  ASSERT_EQ(lock_key.sstable_key_.tablet_id_,
      round_trip.sstable_key_.tablet_id_);
  ASSERT_EQ(lock_key.sstable_key_.reorg_scn_,
      round_trip.sstable_key_.reorg_scn_);
  ASSERT_EQ(lock_key.sstable_key_.control_scn_,
      round_trip.sstable_key_.control_scn_);
  ASSERT_FALSE(round_trip.sstable_key_.ls_id_.is_valid());
}

// check_need_generate_task_ for a non-major-sstable type returns true without
// touching tablet/sslog state. Use TABLET_META which is the simplest non-major
// data type and routes through the early `need_generate_task = true` branch.
TEST_F(TestSSFailGCGenTask, check_need_generate_task_non_major_returns_true)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaCtrlKeyInfo key_info;
  key_info.tenant_id_ = OB_SYS_TENANT_ID;
  key_info.ls_id_ = ObLSID(78);
  key_info.key_.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  key_info.key_.tablet_key_.tablet_id_ = ObTabletID(780);
  key_info.key_.tablet_key_.reorg_scn_.set_min();
  key_info.key_.tablet_key_.control_scn_ = make_scn(100);
  ASSERT_TRUE(key_info.is_valid());

  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctrl_value.ctrl_info_.status_.set_status_start());
  // ObSSMetaCtrlObjectInfo::is_valid() requires type_ to be set to a non-INVALID
  // value; NONE is the simplest valid choice and skips the type-specific field
  // checks (cg_cnt_/start_seq_/etc.).
  ctrl_value.object_info_.set_none();
  ASSERT_TRUE(ctrl_value.is_valid());

  ObSSFailGCLSCtx ls_ctx;
  ls_ctx.ls_id_ = key_info.ls_id_;
  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(key_info.key_));
  ctrl_row.ctrl_value_ = ctrl_value;
  ASSERT_TRUE(ctrl_row.is_valid());
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  ObSSFailGCExecutorTask executor;
  ASSERT_EQ(OB_SUCCESS, executor.init(&ls_ctx, ctrl_rows));
  ASSERT_TRUE(executor.fail_gc_tablet_map_.created());

  bool need_generate_task = false;
  ASSERT_EQ(OB_SUCCESS,
            executor.check_need_generate_task_(
                key_info, ctrl_value, need_generate_task));
  ASSERT_TRUE(need_generate_task);
}

// Major-sstable task generation is driven by the Executor-local Tablet Map.
// A deleted tablet needs a task only when its last retained major snapshot is
// older than the abandoned sstable; a second Executor must not see this map.
TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_major_tablet_map_controls_task_generation)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(783);
  const ObTabletID tablet_id(783000);
  share::SCN abandoned_snapshot;
  share::SCN older_snapshot;
  share::SCN newer_snapshot;
  ASSERT_EQ(OB_SUCCESS, abandoned_snapshot.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, older_snapshot.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, newer_snapshot.convert_for_tx(300));

  ObSSMetaCtrlKeyInfo key_info;
  key_info.tenant_id_ = OB_SYS_TENANT_ID;
  key_info.ls_id_ = ls_id;
  const ObSSMetaCtrlKey major_key(
      blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL,
      tablet_id, SCN::min_scn(), make_scn(100), ls_id);
  ASSERT_EQ(OB_SUCCESS, key_info.key_.assign(major_key));
  ASSERT_TRUE(key_info.is_valid());

  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS,
      ctrl_value.ctrl_info_.status_.set_status_start());
  ASSERT_EQ(OB_SUCCESS,
      ctrl_value.ctrl_info_.status_.set_status_abandon());
  ASSERT_EQ(OB_SUCCESS, ctrl_value.object_info_.init_by_major(
      abandoned_snapshot, 1 /*seq_step*/, 1 /*parallel_cnt*/,
      0 /*cg_cnt*/));
  ASSERT_TRUE(ctrl_value.is_valid());

  ObSSFailGCLSCtx ls_ctx;
  ls_ctx.ls_id_ = ls_id;
  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(key_info.key_));
  ctrl_row.ctrl_value_ = ctrl_value;
  ASSERT_TRUE(ctrl_row.is_valid());
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

  ObSSFailGCExecutorTask executor;
  ASSERT_EQ(OB_SUCCESS, executor.init(&ls_ctx, ctrl_rows));
  const ObSSGCTabletBaseInfo tablet_info(
      tablet_id, key_info.key_.sstable_key_.reorg_scn_);
  bool need_generate_task = true;

  ASSERT_EQ(OB_SUCCESS, executor.fail_gc_tablet_map_.set_refactored(
      tablet_info, ObSSFailGCTabletMapValue(false, older_snapshot)));
  ASSERT_EQ(OB_SUCCESS, executor.check_need_generate_task_(
      key_info, ctrl_value, need_generate_task));
  ASSERT_FALSE(need_generate_task);

  ASSERT_EQ(OB_SUCCESS, executor.fail_gc_tablet_map_.set_refactored(
      tablet_info, ObSSFailGCTabletMapValue(true, newer_snapshot),
      true /*overwrite*/));
  ASSERT_EQ(OB_SUCCESS, executor.check_need_generate_task_(
      key_info, ctrl_value, need_generate_task));
  ASSERT_FALSE(need_generate_task);

  ASSERT_EQ(OB_SUCCESS, executor.fail_gc_tablet_map_.set_refactored(
      tablet_info, ObSSFailGCTabletMapValue(true, older_snapshot),
      true /*overwrite*/));
  ASSERT_EQ(OB_SUCCESS, executor.check_need_generate_task_(
      key_info, ctrl_value, need_generate_task));
  ASSERT_TRUE(need_generate_task);

  ObSSFailGCExecutorTask isolated_executor;
  ASSERT_EQ(OB_SUCCESS, isolated_executor.init(&ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_HASH_NOT_EXIST, isolated_executor.check_need_generate_task_(
      key_info, ctrl_value, need_generate_task));
  ASSERT_FALSE(need_generate_task);
}

// check_need_generate_task_ rejects invalid arguments before doing any I/O.
TEST_F(TestSSFailGCGenTask, check_need_generate_task_invalid_args)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaCtrlKeyInfo key_info;
  key_info.tenant_id_ = OB_SYS_TENANT_ID;
  key_info.ls_id_ = ObLSID(78);
  key_info.key_.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  key_info.key_.tablet_key_.tablet_id_ = ObTabletID(782);
  key_info.key_.tablet_key_.reorg_scn_.set_min();
  key_info.key_.tablet_key_.control_scn_ = make_scn(100);
  ASSERT_TRUE(key_info.is_valid());
  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctrl_value.ctrl_info_.status_.set_status_start());
  ctrl_value.object_info_.set_none();
  ASSERT_TRUE(ctrl_value.is_valid());

  ObSSFailGCLSCtx ls_ctx;
  ls_ctx.ls_id_ = key_info.ls_id_;
  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(key_info.key_));
  ctrl_row.ctrl_value_ = ctrl_value;
  ASSERT_TRUE(ctrl_row.is_valid());
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  ObSSFailGCExecutorTask executor;
  ASSERT_EQ(OB_SUCCESS, executor.init(&ls_ctx, ctrl_rows));

  ObSSMetaCtrlKeyInfo invalid_key_info;  // default-constructed -> invalid
  bool need_generate_task = true;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            executor.check_need_generate_task_(
                invalid_key_info, ctrl_value, need_generate_task));
  ASSERT_FALSE(need_generate_task);

  // valid key_info but invalid ctrl_value (default-constructed)
  ObSSMetaCtrlValue invalid_value;
  need_generate_task = true;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            executor.check_need_generate_task_(
                key_info, invalid_value, need_generate_task));
  ASSERT_FALSE(need_generate_task);
}

// ObSSFailGCInfo basic contracts: default-invalid, reset clears, assign copies.
TEST_F(TestSSFailGCGenTask, fail_gc_info_basic_contracts)
{
  ObSSFailGCInfo info;
  ASSERT_FALSE(info.is_valid());
  ASSERT_FALSE(info.need_generate_task_);

  info.key_info_.tenant_id_ = OB_SYS_TENANT_ID;
  info.key_info_.ls_id_ = ObLSID(79);
  info.key_info_.key_.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  info.key_info_.key_.tablet_key_.tablet_id_ = ObTabletID(790);
  info.key_info_.key_.tablet_key_.reorg_scn_.set_min();
  info.key_info_.key_.tablet_key_.control_scn_ = make_scn(100);
  info.ctrl_value_.ctrl_info_.lease_epoch_ = 1;
  info.ctrl_value_.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, info.ctrl_value_.ctrl_info_.status_.set_status_start());
  info.ctrl_value_.object_info_.set_none();
  info.need_generate_task_ = true;
  ASSERT_TRUE(info.is_valid());

  ObSSFailGCInfo copy;
  ASSERT_EQ(OB_SUCCESS, copy.assign(info));
  ASSERT_TRUE(copy.is_valid());
  ASSERT_TRUE(copy.need_generate_task_);
  ASSERT_EQ(info.key_info_.tenant_id_, copy.key_info_.tenant_id_);
  ASSERT_EQ(info.key_info_.ls_id_, copy.key_info_.ls_id_);

  copy.reset();
  ASSERT_FALSE(copy.is_valid());
  ASSERT_FALSE(copy.need_generate_task_);
}

TEST_F(TestSSFailGCGenTask, fail_gc_scheduler_registration)
{
  ASSERT_STREQ("SS_FAIL_GC",
      ObITask::ObITaskTypeStr[ObITask::TASK_TYPE_SS_FAIL_GC]);
  ASSERT_STREQ("SS_FAIL_GC",
      ObIDag::get_dag_type_str(ObDagType::DAG_TYPE_SS_FAIL_GC));
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_FAIL_GC].init_dag_prio_);
  ASSERT_EQ(3, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_GEN].score_);
  ASSERT_EQ(ObSysTaskType::SS_FAIL_GC_TASK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_FAIL_GC].sys_task_type_);
  ASSERT_STREQ("SS_FAIL_GC_TASK",
      sys_task_type_to_str(ObSysTaskType::SS_FAIL_GC_TASK));
}

TEST_F(TestSSFailGCGenTask, fail_gc_dag_param_and_identity_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSFailGCDagParam invalid_schedule_param;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      invalid_schedule_param.init(OB_SYS_TENANT_ID, -1));
  ObSSFailGCDagParam param;
  ASSERT_FALSE(param.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, param.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID, 123));
  ASSERT_TRUE(param.is_valid());
  ASSERT_EQ(123, param.schedule_ts_);
  ASSERT_EQ(OB_INIT_TWICE, param.init(OB_SYS_TENANT_ID));

  ObArenaAllocator event_dag_allocator("FailGCEventDag");
  ObSSFailGCDag event_dag;
  ASSERT_EQ(OB_SUCCESS, event_dag.basic_init(event_dag_allocator));
  ASSERT_EQ(OB_SUCCESS, event_dag.init_by_param(&param));
  ObCurTraceId::init(GCONF.self_addr_);
  ASSERT_EQ(OB_SUCCESS,
      event_dag.set_dag_id(*ObCurTraceId::get_trace_id()));
  event_dag.set_start_time();
  event_dag.report_start_event();
  event_dag.report_start_event();
  ASSERT_EQ(1, ATOMIC_LOAD(&event_dag.start_event_reported_));
  ASSERT_EQ(123, event_dag.get_schedule_ts());
  ASSERT_EQ(OB_SUCCESS, event_dag.report_result());

  ObSSFailGCDag dag1;
  ObSSFailGCDag dag2;
  ObSSFailGCDag other_tenant_dag;
  dag1.set_tenant_id(OB_SYS_TENANT_ID);
  dag2.set_tenant_id(OB_SYS_TENANT_ID);
  other_tenant_dag.set_tenant_id(1002);

  ASSERT_EQ(ObDagType::DAG_TYPE_SS_FAIL_GC, dag1.get_type());
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN, dag1.get_priority());
  ObSSMarkSweepDag mark_sweep_dag;
  ASSERT_EQ(ObDagPrio::DAG_PRIO_MARK_SWEEP, mark_sweep_dag.get_priority());
  ASSERT_TRUE(dag1 == dag2);
  ASSERT_EQ(dag1.hash(), dag2.hash());
  ASSERT_FALSE(dag1 == other_tenant_dag);

  char dag_key[128] = {0};
  ASSERT_EQ(OB_SUCCESS, dag1.fill_dag_key(dag_key, sizeof(dag_key)));
  ASSERT_NE(nullptr, std::strstr(dag_key, "tenant_id=1"));
}

TEST_F(TestSSFailGCGenTask, fail_gc_dag_warning_info_matches_diagnose_config)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator dag_allocator("FailGCWarnDag");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));

  compaction::ObInfoParamBuffer info_allocator;
  compaction::ObIBasicInfoParam *info_param = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.fill_info_param(info_param, info_allocator));
  ASSERT_NE(nullptr, info_param);

  char comment[common::OB_DAG_WARNING_INFO_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, info_param->fill_comment(comment, sizeof(comment)));
  ASSERT_NE(nullptr, std::strstr(comment, "tenant_id"));
  ASSERT_NE(nullptr, std::strstr(comment, "server_id"));
  ASSERT_NE(nullptr, std::strstr(comment, "ls_count"));
  ASSERT_NE(nullptr, std::strstr(comment, "first_error_code"));
  ASSERT_NE(nullptr, std::strstr(comment, "failed_ls_count"));
  ASSERT_NE(nullptr, std::strstr(comment, "server_addr"));
  ASSERT_NE(nullptr, std::strstr(comment, "row_stat"));
}

TEST_F(TestSSFailGCGenTask, fail_gc_stop_helper_checks_stop_or_published_error)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCPrepareTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
  ASSERT_NE(nullptr, task);
  ASSERT_EQ(OB_SUCCESS, task->init());

  ASSERT_EQ(OB_SUCCESS, ObSSGCDagUtil::check_if_stopped(*task));
  dag.set_dag_ret(OB_TIMEOUT);
  ASSERT_FALSE(dag.is_dag_failed());
  ASSERT_FALSE(dag.is_stop_requested());
  ASSERT_EQ(OB_CANCELED, ObSSGCDagUtil::check_if_stopped(*task));
  ASSERT_EQ(OB_TIMEOUT, dag.get_published_dag_ret());

  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, ObSSGCDagUtil::check_if_stopped(*task));
  ASSERT_EQ(OB_CANCELED, ObSSGCDagUtil::check_if_stopped(*task));
}

TEST_F(TestSSFailGCGenTask, fail_gc_stop_request_is_visible_across_threads)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("FailGCStopSync");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCPrepareTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
  ASSERT_NE(nullptr, task);
  ASSERT_EQ(OB_SUCCESS, task->init());

  FailGCTestSignal reader_started;
  FailGCTestSignal reader_finished;
  std::atomic<bool> fallback_release(false);
  std::atomic<int> observed_ret(OB_SUCCESS);
  std::thread reader([&]() {
    ObTenantSwitchGuard reader_tenant_guard;
    int ret = reader_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    reader_started.signal();
    while (OB_SUCCESS == ret
        && !fallback_release.load(std::memory_order_relaxed)) {
      ret = ObSSGCDagUtil::check_if_stopped(*task);
      if (OB_SUCCESS == ret) {
        ob_usleep(100);
      }
    }
    observed_ret.store(ret, std::memory_order_release);
    reader_finished.signal();
  });

  const bool reader_was_started = reader_started.wait();
  if (reader_was_started) {
    dag.simply_set_stop();
  }
  const bool stop_was_observed = reader_finished.wait();
  fallback_release.store(true, std::memory_order_relaxed);
  reader.join();

  EXPECT_TRUE(reader_was_started);
  EXPECT_TRUE(stop_was_observed);
  EXPECT_EQ(OB_CANCELED, observed_ret.load(std::memory_order_acquire));
  EXPECT_TRUE(dag.is_stop_requested());
}

TEST(SSFailGCTimerUnitTest, dag_schedule_controller_contracts)
{
  ObSSGCDagScheduleController controller;
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      controller.state());

  ASSERT_EQ(OB_SUCCESS, controller.switch_to_wait_for_dag_finish());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      controller.state());
  ASSERT_EQ(OB_STATE_NOT_MATCH, controller.switch_to_wait_for_dag_finish());

  ASSERT_EQ(OB_SUCCESS, controller.switch_to_wait_for_next_schedule());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      controller.state());
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      controller.switch_to_wait_for_next_schedule());

  ASSERT_EQ(OB_SUCCESS, controller.switch_to_wait_for_dag_finish());
  controller.reset();
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      controller.state());
}

TEST(SSFailGCTimerUnitTest,
     stopped_runner_blocks_entry_and_final_submission)
{
  volatile bool task_stopped = true;
  ScriptedSSFailGCGenTask entry_blocked_task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, entry_blocked_task.init(OB_SYS_TENANT_ID));

  entry_blocked_task.runTimerTask();
  ASSERT_EQ(0, entry_blocked_task.check_count_);
  ASSERT_EQ(0, entry_blocked_task.submit_count_);
  ASSERT_EQ(0, entry_blocked_task.run_cnt_);

  task_stopped = false;
  StopBeforeSubmitSSFailGCGenTask final_blocked_task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, final_blocked_task.init(OB_SYS_TENANT_ID));
  final_blocked_task.commit_gc_dag_schedule_time_(10);
  final_blocked_task.check_ts_ = 100;

  final_blocked_task.runTimerTask();
  ASSERT_TRUE(task_stopped);
  ASSERT_EQ(1, final_blocked_task.check_count_);
  ASSERT_EQ(0, final_blocked_task.submit_count_);
  ASSERT_EQ(0, final_blocked_task.run_cnt_);
  ASSERT_EQ(10, final_blocked_task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      final_blocked_task.dag_schedule_controller_.state());
}

TEST_F(TestSSFailGCGenTask,
       runner_supports_sequential_restart_and_resets_dag_controllers)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const uint64_t isolated_tenant_id = 1002;
  ObSSGCTaskRunner runner;
  ASSERT_EQ(OB_SUCCESS, runner.init(isolated_tenant_id));
  ASSERT_TRUE(runner.is_stopped_);

  ASSERT_EQ(OB_SUCCESS, runner.start());
  ASSERT_FALSE(runner.is_stopped_);
  const int64_t first_start_ts =
      runner.fail_gc_gen_task_.last_period_commit_time_us_;
  ASSERT_GT(first_start_ts, 0);
  ASSERT_EQ(first_start_ts,
      runner.data_check_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(first_start_ts,
      runner.ls_meta_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(first_start_ts,
      runner.lock_row_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.fail_gc_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, runner.fail_gc_gen_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, runner.data_check_gen_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, runner.ls_meta_gc_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, runner.lock_row_gc_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());

  runner.stop();
  ASSERT_TRUE(runner.is_stopped_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.fail_gc_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(first_start_ts,
      runner.fail_gc_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(first_start_ts,
      runner.data_check_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(first_start_ts,
      runner.ls_meta_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(first_start_ts,
      runner.lock_row_gc_task_.last_period_commit_time_us_);

  ASSERT_EQ(OB_SUCCESS, runner.fail_gc_gen_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(OB_SUCCESS, runner.data_check_gen_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(OB_SUCCESS, runner.ls_meta_gc_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(OB_SUCCESS, runner.lock_row_gc_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  runner.fail_gc_gen_task_.commit_gc_dag_schedule_time_(11);
  runner.ls_meta_gc_task_.commit_gc_dag_schedule_time_(22);
  runner.lock_row_gc_task_.commit_gc_dag_schedule_time_(33);
  runner.data_check_gen_task_.commit_gc_dag_schedule_time_(44);

  ASSERT_EQ(OB_SUCCESS, runner.start());
  ASSERT_FALSE(runner.is_stopped_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.fail_gc_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_GT(runner.fail_gc_gen_task_.last_period_commit_time_us_, 44);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.data_check_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.ls_meta_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.lock_row_gc_task_.last_period_commit_time_us_);

  runner.stop();
  runner.wait();
  runner.destroy();
}

TEST_F(TestSSFailGCGenTask,
       runner_partial_start_failure_rolls_back_as_stopped)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const uint64_t isolated_tenant_id = 1003;
  ObSSGCTaskRunner runner;
  ASSERT_EQ(OB_SUCCESS, runner.init(isolated_tenant_id));

  // Pre-start the middle timer so Runner::start() starts the low timer and
  // then deterministically fails on the middle timer.
  ASSERT_EQ(OB_SUCCESS, runner.ls_meta_gc_task_.dag_schedule_controller_
      .switch_to_wait_for_dag_finish());
  ASSERT_EQ(OB_SUCCESS, TG_START(runner.mid_tg_id_));
  ASSERT_NE(OB_SUCCESS, runner.start());
  ASSERT_TRUE(runner.is_stopped_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.fail_gc_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(0, runner.fail_gc_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(0, runner.data_check_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(0, runner.ls_meta_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(0, runner.lock_row_gc_task_.last_period_commit_time_us_);

  // The failed start has already stopped and joined both timer groups, so a
  // normal sequential start can reuse the initialized Runner.
  ASSERT_EQ(OB_SUCCESS, runner.start());
  ASSERT_FALSE(runner.is_stopped_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.fail_gc_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.ls_meta_gc_task_.dag_schedule_controller_.state());
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      runner.lock_row_gc_task_.dag_schedule_controller_.state());
  ASSERT_GT(runner.fail_gc_gen_task_.last_period_commit_time_us_, 0);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.data_check_gen_task_.last_period_commit_time_us_);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.ls_meta_gc_task_.last_period_commit_time_us_);
  ASSERT_EQ(runner.fail_gc_gen_task_.last_period_commit_time_us_,
      runner.lock_row_gc_task_.last_period_commit_time_us_);
  runner.stop();
  runner.destroy();
}

TEST(SSFailGCTimerUnitTest,
     schedule_check_is_side_effect_free_with_fake_clock)
{
  volatile bool task_stopped = false;
  FakeClockSSFailGCGenTask task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
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

  // Rechecking after the interval has elapsed still requires scheduling until
  // the caller explicitly commits.
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
  ASSERT_EQ(250, task.last_period_commit_time_us_);
}

TEST(SSFailGCTimerUnitTest,
     timer_reloads_dynamic_interval_after_dag_completion)
{
  volatile bool task_stopped = false;
  DynamicIntervalSSFailGCGenTask task(task_stopped);
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

TEST(SSFailGCTimerUnitTest,
     waits_for_dag_and_separates_finish_tick)
{
  volatile bool task_stopped = false;
  ScriptedSSFailGCGenTask task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);

  task.check_ts_ = 100;
  task.submit_ret_ = OB_SUCCESS;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_exist_ = true;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.probe_ret_ = OB_TIMEOUT;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  // The tick that observes DAG absence only changes state. The next tick is
  // the first one allowed to evaluate the period and submit again.
  task.need_schedule_ = false;
  task.runTimerTask();
  ASSERT_EQ(2, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.need_schedule_ = true;
  task.check_ts_ = 320;
  task.runTimerTask();
  ASSERT_EQ(3, task.check_count_);
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(2, task.run_cnt_);
  ASSERT_EQ(320, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST(SSFailGCTimerUnitTest,
     only_commits_accepted_submission)
{
  volatile bool task_stopped = false;
  ScriptedSSFailGCGenTask task(task_stopped);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);

  task.check_ret_ = OB_TIMEOUT;
  task.check_ts_ = 20;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(0, task.submit_count_);

  task.check_ret_ = OB_SUCCESS;
  task.check_ts_ = 30;
  task.submit_ret_ = OB_SIZE_OVERFLOW;
  ASSERT_EQ(OB_SIZE_OVERFLOW, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);

  task.check_ts_ = 40;
  task.submit_ret_ = OB_TASK_EXIST;
  ASSERT_EQ(OB_TASK_EXIST, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);

  task.check_ts_ = 50;
  task.submit_ret_ = OB_EAGAIN;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = false;
  ASSERT_EQ(OB_EAGAIN, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(3, task.submit_count_);
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 60;
  task.probe_ret_ = OB_TIMEOUT;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(4, task.submit_count_);
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 70;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(70, task.last_period_commit_time_us_);
  ASSERT_EQ(5, task.submit_count_);
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(5, task.run_cnt_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  ASSERT_EQ(OB_STATE_NOT_MATCH, task.try_schedule_fail_gc_dag_());
  ASSERT_EQ(5, task.submit_count_);
  ASSERT_EQ(70, task.last_period_commit_time_us_);
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_timer_confirms_duplicate_with_real_scheduler)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);

  ObSSFailGCDag initial_probe;
  initial_probe.set_tenant_id(OB_SYS_TENANT_ID);
  bool exist = false;
  bool is_emergency = false;
  ASSERT_EQ(OB_SUCCESS,
      scheduler->check_dag_exist(&initial_probe, exist, is_emergency));
  ASSERT_FALSE(exist);

  AlwaysNeedScheduleSSFailGCGenTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS,
      scheduler->get_adaptive_limit(
          ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit));
  ASSERT_EQ(OB_SUCCESS,
      scheduler->set_adaptive_limit(ObDagPrio::DAG_PRIO_SS_GC_GEN, 0));

  ObSSFailGCDagParam param;
  const int param_ret = param.init(OB_SYS_TENANT_ID);
  const int initial_submit_ret = OB_SUCCESS == param_ret
      ? scheduler->create_and_add_dag<ObSSFailGCDag>(&param)
      : param_ret;

  // The Timer submission sees OB_EAGAIN, then its production helper creates a
  // fresh identity-only probe and confirms the resident DAG.
  task.runTimerTask();
  task.runTimerTask();

  ObSSFailGCDag cancel_probe;
  cancel_probe.set_tenant_id(OB_SYS_TENANT_ID);
  const int cancel_ret = scheduler->cancel_dag(
      &cancel_probe, true/*force_cancel*/);
  bool exist_after_cancel = true;
  ObSSFailGCDag after_cancel_probe;
  after_cancel_probe.set_tenant_id(OB_SYS_TENANT_ID);
  const int cancel_check_ret = scheduler->check_dag_exist(
      &after_cancel_probe, exist_after_cancel, is_emergency);

  // DAG absence only moves WAIT -> NEXT. A later tick submits the next DAG.
  task.runTimerTask();
  task.check_ts_ = 200;
  task.runTimerTask();
  bool exist_after_resubmit = false;
  ObSSFailGCDag resubmit_probe;
  resubmit_probe.set_tenant_id(OB_SYS_TENANT_ID);
  const int resubmit_check_ret = scheduler->check_dag_exist(
      &resubmit_probe, exist_after_resubmit, is_emergency);
  const int final_cancel_ret = scheduler->cancel_dag(
      &resubmit_probe, true/*force_cancel*/);

  const int restore_ret = scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit);
  scheduler->notify();

  EXPECT_EQ(OB_SUCCESS, param_ret);
  EXPECT_EQ(OB_SUCCESS, initial_submit_ret);
  EXPECT_EQ(2, task.check_count_);
  EXPECT_EQ(2, task.run_cnt_);
  EXPECT_EQ(200, task.last_period_commit_time_us_);
  EXPECT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
  EXPECT_EQ(OB_SUCCESS, cancel_ret);
  EXPECT_EQ(OB_SUCCESS, cancel_check_ret);
  EXPECT_FALSE(exist_after_cancel);
  EXPECT_EQ(OB_SUCCESS, resubmit_check_ret);
  EXPECT_TRUE(exist_after_resubmit);
  EXPECT_EQ(OB_SUCCESS, final_cancel_ret);
  EXPECT_EQ(OB_SUCCESS, restore_ret);
}

TEST_F(TestSSFailGCGenTask,
       runner_stop_waits_for_inflight_timer_callback)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  const uint64_t isolated_tenant_id = 1006;
  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit));
  ScopedDagAdaptiveLimit limit_guard(*scheduler,
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.set(0));

  ObSSGCTaskRunner runner;
  ASSERT_EQ(OB_SUCCESS, runner.init(isolated_tenant_id));
  ASSERT_EQ(OB_SUCCESS, TG_START(runner.mid_tg_id_));
  ATOMIC_STORE(&runner.is_stopped_, false);
  FailGCTestSignal callback_entered;
  FailGCTestSignal allow_submit;
  FailGCTestSignal submitted;
  FailGCTestSignal allow_return;
  std::atomic<int> submit_ret(OB_ERR_UNEXPECTED);
  LateSubmitFailGCTestTimer late_timer(*scheduler, isolated_tenant_id,
      callback_entered, allow_submit, submitted, allow_return, submit_ret);
  const int schedule_ret = TG_SCHEDULE(
      runner.mid_tg_id_, late_timer, 1000L, false /* repeat */);
  if (OB_SUCCESS != schedule_ret) {
    runner.stop();
    runner.destroy();
    EXPECT_EQ(OB_SUCCESS, schedule_ret);
    EXPECT_EQ(OB_SUCCESS, limit_guard.restore());
    return;
  }
  const bool callback_was_entered = callback_entered.wait();
  if (!callback_was_entered) {
    allow_submit.signal();
    allow_return.signal();
    runner.stop();
    runner.destroy();
    ObSSFailGCDag cleanup_probe;
    cleanup_probe.set_tenant_id(isolated_tenant_id);
    (void) scheduler->cancel_dag(&cleanup_probe, true /* force_cancel */);
    EXPECT_TRUE(callback_was_entered);
    EXPECT_EQ(OB_SUCCESS, limit_guard.restore());
    return;
  }

  std::atomic<int> stop_thread_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> stop_returned(false);
  std::thread stop_thread([&]() {
    ObTenantSwitchGuard stop_tenant_guard;
    int ret = stop_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret)) {
      runner.stop();
    }
    stop_thread_ret.store(ret, std::memory_order_release);
    stop_returned.store(true, std::memory_order_release);
  });

  const std::chrono::steady_clock::time_point stop_flag_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!ATOMIC_LOAD(&runner.is_stopped_)
      && std::chrono::steady_clock::now() < stop_flag_deadline) {
    std::this_thread::yield();
  }
  const bool stop_flag_seen = ATOMIC_LOAD(&runner.is_stopped_);
  allow_submit.signal();
  const bool submit_finished = submitted.wait();
  ObSSFailGCDag resident_probe;
  resident_probe.set_tenant_id(isolated_tenant_id);
  bool exist_while_callback_blocked = false;
  bool is_emergency = false;
  const int resident_check_ret = scheduler->check_dag_exist(
      &resident_probe, exist_while_callback_blocked, is_emergency);
  EXPECT_TRUE(stop_flag_seen);
  EXPECT_TRUE(submit_finished);
  EXPECT_EQ(OB_SUCCESS, submit_ret.load(std::memory_order_acquire));
  EXPECT_EQ(OB_SUCCESS, resident_check_ret);
  EXPECT_TRUE(exist_while_callback_blocked);
  EXPECT_FALSE(stop_returned.load(std::memory_order_acquire));

  allow_return.signal();
  stop_thread.join();
  runner.destroy();
  ObSSFailGCDag after_stop_probe;
  after_stop_probe.set_tenant_id(isolated_tenant_id);
  bool exist_after_stop = true;
  const int final_check_ret = scheduler->check_dag_exist(
      &after_stop_probe, exist_after_stop, is_emergency);
  const int cancel_ret = scheduler->cancel_dag(
      &after_stop_probe, true /* force_cancel */);
  ObSSFailGCDag after_cancel_probe;
  after_cancel_probe.set_tenant_id(isolated_tenant_id);
  bool exist_after_cancel = true;
  const int cancel_check_ret = scheduler->check_dag_exist(
      &after_cancel_probe, exist_after_cancel, is_emergency);
  const int restore_ret = limit_guard.restore();

  EXPECT_EQ(OB_SUCCESS, stop_thread_ret.load(std::memory_order_acquire));
  EXPECT_EQ(OB_SUCCESS, final_check_ret);
  EXPECT_TRUE(exist_after_stop);
  EXPECT_EQ(OB_SUCCESS, cancel_ret);
  EXPECT_EQ(OB_SUCCESS, cancel_check_ret);
  EXPECT_FALSE(exist_after_cancel);
  EXPECT_EQ(OB_SUCCESS, restore_ret);
}

TEST_F(TestSSFailGCGenTask, fail_gc_context_atomic_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObSSFailGCDagCtx dag_ctx;
  ASSERT_FALSE(dag_ctx.gc_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, dag_ctx.init(OB_SYS_TENANT_ID));
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag_ctx.alloc_ls_ctx(ObLSID(82), ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);
  ASSERT_TRUE(ls_ctx->is_valid());
  ASSERT_FALSE(ls_ctx->checkpoint_meta_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, dag_ctx.get_first_error_code());
  ASSERT_EQ(0, dag_ctx.get_failed_ls_count());
  ASSERT_EQ(1, dag_ctx.get_ls_count());
  ASSERT_EQ(0, dag_ctx.get_scanned_ctrl_row_count());
  ASSERT_EQ(0, dag_ctx.get_abandoned_ctrl_row_count());
  ASSERT_EQ(0, dag_ctx.get_generated_task_count());

  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start_record(false);
  std::atomic<int64_t> winner_count(0);
  std::thread first([&]() {
    ready_count.fetch_add(1, std::memory_order_acq_rel);
    while (!start_record.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    if (dag_ctx.record_error_code(*ls_ctx, OB_TIMEOUT)) {
      winner_count.fetch_add(1);
    }
  });
  std::thread second([&]() {
    ready_count.fetch_add(1, std::memory_order_acq_rel);
    while (!start_record.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    if (dag_ctx.record_error_code(*ls_ctx, OB_TRANS_TIMEOUT)) {
      winner_count.fetch_add(1);
    }
  });
  while (2 != ready_count.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  start_record.store(true, std::memory_order_release);
  first.join();
  second.join();

  ASSERT_EQ(1, winner_count.load());
  ASSERT_EQ(1, dag_ctx.get_failed_ls_count());
  ASSERT_TRUE(OB_TIMEOUT == dag_ctx.get_first_error_code()
      || OB_TRANS_TIMEOUT == dag_ctx.get_first_error_code());
  ASSERT_EQ(dag_ctx.get_first_error_code(), ls_ctx->get_first_error_code());

  ObSSFailGCLSCtx *another_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag_ctx.alloc_ls_ctx(ObLSID(83), another_ls_ctx));
  ASSERT_NE(nullptr, another_ls_ctx);
  ASSERT_EQ(2, dag_ctx.get_ls_count());
  ASSERT_TRUE(dag_ctx.record_error_code(*another_ls_ctx, OB_TRANS_STMT_TIMEOUT));
  ASSERT_EQ(2, dag_ctx.get_failed_ls_count());
  ASSERT_FALSE(dag_ctx.record_error_code(*another_ls_ctx, OB_TIMEOUT));
  ASSERT_EQ(2, dag_ctx.get_failed_ls_count());

  ObSSFailGCLSCtx unowned_ls_ctx;
  unowned_ls_ctx.ls_id_ = ObLSID(84);
  unowned_ls_ctx.checkpoint_meta_scn_ = make_scn(100);
  ASSERT_TRUE(unowned_ls_ctx.record_first_error_code(OB_TIMEOUT));
  unowned_ls_ctx.reset();
  ASSERT_FALSE(unowned_ls_ctx.is_valid());
  ASSERT_FALSE(unowned_ls_ctx.checkpoint_meta_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, unowned_ls_ctx.get_first_error_code());
  unowned_ls_ctx.ls_id_ = ObLSID(84);
  ASSERT_FALSE(dag_ctx.record_error_code(unowned_ls_ctx, OB_TIMEOUT));
  ASSERT_EQ(2, dag_ctx.get_failed_ls_count());

  const int64_t stat_add_count = 1000;
  std::thread first_stat_writer([&]() {
    for (int64_t i = 0; i < stat_add_count; ++i) {
      dag_ctx.add_scanned_ctrl_row_count(1);
      dag_ctx.add_abandoned_ctrl_row_count(1);
      dag_ctx.add_generated_task_count(1);
    }
  });
  std::thread second_stat_writer([&]() {
    for (int64_t i = 0; i < stat_add_count; ++i) {
      dag_ctx.add_scanned_ctrl_row_count(1);
      dag_ctx.add_abandoned_ctrl_row_count(1);
      dag_ctx.add_generated_task_count(1);
    }
  });
  first_stat_writer.join();
  second_stat_writer.join();
  ASSERT_EQ(2 * stat_add_count, dag_ctx.get_scanned_ctrl_row_count());
  ASSERT_EQ(2 * stat_add_count, dag_ctx.get_abandoned_ctrl_row_count());
  ASSERT_EQ(2 * stat_add_count, dag_ctx.get_generated_task_count());

  dag_ctx.reset();
  ASSERT_FALSE(dag_ctx.is_valid());
  ASSERT_EQ(0, dag_ctx.get_ls_count());
  ASSERT_EQ(OB_SUCCESS, dag_ctx.get_first_error_code());
  ASSERT_EQ(0, dag_ctx.get_failed_ls_count());
  ASSERT_EQ(0, dag_ctx.get_scanned_ctrl_row_count());
  ASSERT_EQ(0, dag_ctx.get_abandoned_ctrl_row_count());
  ASSERT_EQ(0, dag_ctx.get_generated_task_count());
}

TEST_F(TestSSFailGCGenTask, fail_gc_dag_static_prepare_schedule_topology)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));

  // A base-initialized DAG must not allocate a partial graph before its
  // business init parameter has been accepted.
  ASSERT_EQ(OB_NOT_INIT, dag.create_first_task());
  ASSERT_EQ(0, dag.get_task_list_count());

  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ASSERT_EQ(3, dag.get_task_list_count());

  ObSSFailGCPrepareTask *prepare_task = nullptr;
  ObSSFailGCScheduleTask *schedule_task = nullptr;
  ObSSLobCtrlFailGCTask *lob_ctrl_task = nullptr;
  int64_t recognized_task_count = 0;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    ASSERT_EQ(&dag, task->get_dag());
    ASSERT_EQ(ObITask::TASK_STATUS_WAITING, task->get_status());
    if (nullptr != dynamic_cast<ObSSFailGCPrepareTask *>(task)) {
      ASSERT_EQ(nullptr, prepare_task);
      prepare_task = static_cast<ObSSFailGCPrepareTask *>(task);
      ++recognized_task_count;
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      ASSERT_EQ(nullptr, schedule_task);
      schedule_task = static_cast<ObSSFailGCScheduleTask *>(task);
      ++recognized_task_count;
    } else if (nullptr != dynamic_cast<ObSSLobCtrlFailGCTask *>(task)) {
      ASSERT_EQ(nullptr, lob_ctrl_task);
      lob_ctrl_task = static_cast<ObSSLobCtrlFailGCTask *>(task);
      ++recognized_task_count;
    }
    task = task->get_next();
  }
  ASSERT_EQ(3, recognized_task_count);
  ASSERT_EQ(dag.get_task_list_count(), recognized_task_count);
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_NE(nullptr, lob_ctrl_task);
  ASSERT_TRUE(prepare_task->is_inited_);
  ASSERT_TRUE(schedule_task->is_inited_);
  ASSERT_TRUE(lob_ctrl_task->is_inited_);
  ASSERT_EQ(0, schedule_task->ls_idx_);
  ASSERT_TRUE(schedule_task->is_ls_head_);
  ASSERT_EQ(nullptr, schedule_task->ls_ctx_);

  ASSERT_EQ(0, prepare_task->get_indegree());
  ASSERT_EQ(1, schedule_task->get_indegree());
  ASSERT_EQ(1, lob_ctrl_task->get_indegree());

  ObSEArray<ObINodeWithChild *, 2> children;
  ASSERT_EQ(OB_SUCCESS, prepare_task->copy_child_nodes(children));
  ASSERT_EQ(2, children.count());
  bool has_schedule_task = false;
  bool has_lob_ctrl_task = false;
  for (int64_t i = 0; i < children.count(); ++i) {
    has_schedule_task |= static_cast<ObINodeWithChild *>(schedule_task) == children.at(i);
    has_lob_ctrl_task |= static_cast<ObINodeWithChild *>(lob_ctrl_task) == children.at(i);
  }
  ASSERT_TRUE(has_schedule_task);
  ASSERT_TRUE(has_lob_ctrl_task);
  children.reset();
  ASSERT_EQ(OB_SUCCESS, schedule_task->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
}

TEST_F(TestSSFailGCGenTask, lob_ctrl_failure_does_not_block_shared_meta_fail_gc)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_GE(GET_MIN_CLUSTER_VERSION(), CLUSTER_VERSION_4_6_3_1);
  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ASSERT_EQ(OB_SUCCESS, ctx->gc_scn_.convert_from_ts(1700000000000000L));
  ctx->sql_timeout_us_ = 0;
  ObSSLobCtrlFailGCTask *lob_ctrl_task = nullptr;
  ObSSFailGCScheduleTask *schedule_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLobCtrlFailGCTask *>(task)) {
      lob_ctrl_task = static_cast<ObSSLobCtrlFailGCTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      schedule_task = static_cast<ObSSFailGCScheduleTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, lob_ctrl_task);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->process());
  ASSERT_EQ(OB_SUCCESS, dag.get_published_dag_ret());
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(899), ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);
  ASSERT_EQ(OB_SUCCESS, schedule_task->process());
  ASSERT_TRUE(ls_ctx->iterator_inited_);
  ASSERT_TRUE(ls_ctx->scan_finished_);
}

TEST_F(TestSSFailGCGenTask, fail_gc_tasks_stop_at_process_entry)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCPrepareTask *prepare_task = nullptr;
  ObSSFailGCScheduleTask *schedule_task = nullptr;
  ObSSLobCtrlFailGCTask *lob_ctrl_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCPrepareTask *>(task)) {
      prepare_task = static_cast<ObSSFailGCPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      schedule_task = static_cast<ObSSFailGCScheduleTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSLobCtrlFailGCTask *>(task)) {
      lob_ctrl_task = static_cast<ObSSLobCtrlFailGCTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_NE(nullptr, lob_ctrl_task);

  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(868), ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCCtrlRow ctrl_row;
  const ObSSMetaCtrlKey ctrl_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(868001), SCN::min_scn(), make_scn(100), ls_ctx->ls_id_);
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_.ctrl_info_.lease_epoch_ = 1;
  ctrl_row.ctrl_value_.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS,
      ctrl_row.ctrl_value_.ctrl_info_.status_.set_status_start());
  ctrl_row.ctrl_value_.object_info_.set_none();
  ASSERT_TRUE(ctrl_row.is_valid());
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  ObSSFailGCExecutorTask *executor_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor_task));
  ASSERT_NE(nullptr, executor_task);
  ASSERT_EQ(OB_SUCCESS, executor_task->init(ls_ctx, ctrl_rows));

  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, prepare_task->process());
  ASSERT_EQ(OB_CANCELED, schedule_task->process());
  ASSERT_EQ(OB_CANCELED, lob_ctrl_task->process());
  ASSERT_EQ(OB_CANCELED, executor_task->process());
  ASSERT_EQ(nullptr, schedule_task->ls_ctx_);
  ASSERT_FALSE(ls_ctx->iterator_inited_);
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
}

TEST_F(TestSSFailGCGenTask, lob_ctrl_fail_gc_processes_expired_rows)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_GE(GET_MIN_CLUSTER_VERSION(), CLUSTER_VERSION_4_6_3_1);
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const uint64_t first_hash = 91000001;
  const int64_t lob_fail_gc_timeout_us = 60L * 60L * 1000L * 1000L;
  const int64_t cutoff_ts_us = ObTimeUtility::current_time_us() - lob_fail_gc_timeout_us;
  const int64_t margin_us = 60L * 1000L * 1000L;
  const int64_t old_ts_us = cutoff_ts_us - margin_us;
  const int64_t recent_ts_us = cutoff_ts_us + margin_us;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const blocksstable::MacroBlockId macro_ids[] = {
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261001, 0, 0, 0),
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261002, 0, 0, 0),
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261003, 0, 0, 0),
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261004, 0, 0, 0),
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261005, 0, 0, 0),
      // These two locked rows sort before the unlocked candidates above.
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20260999, 0, 0, 0),
      blocksstable::MacroBlockId(blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
          20261000, 0, 0, 0),
  };
  const int64_t statuses[] = {
      OUTTABLE_LOB_CONTROL_DOING,
      OUTTABLE_LOB_CONTROL_FAIL,
      OUTTABLE_LOB_CONTROL_COMPLETE,
      OUTTABLE_LOB_CONTROL_DOING,
      OUTTABLE_LOB_CONTROL_FAIL,
      OUTTABLE_LOB_CONTROL_DOING,
      OUTTABLE_LOB_CONTROL_FAIL,
  };
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE ", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  for (int64_t i = 0; i < ARRAYSIZEOF(macro_ids); ++i) {
    if (i > 0) {
      ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(" OR "));
    }
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append("("));
    ASSERT_EQ(OB_SUCCESS,
        append_lob_ctrl_pk_condition_(macro_ids[i], first_hash + i, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(tenant_id, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS,
        sql_proxy.write(tenant_id, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  for (int64_t i = 0; i < ARRAYSIZEOF(macro_ids); ++i) {
    ObOuttableLobControlRow row;
    row.key_.macro_id_ = macro_ids[i];
    row.key_.hash_ = first_hash + i;
    row.status_ = statuses[i];
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
        sql_proxy, tenant_id, row, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  ObSqlString update_sql;
  for (int64_t i = 0; i < ARRAYSIZEOF(macro_ids); ++i) {
    const int64_t last_modify_time = (i < 3 || i >= 5) ? old_ts_us : recent_ts_us;
    ASSERT_EQ(OB_SUCCESS, update_sql.assign_fmt(
        "UPDATE %s SET last_modify_time = usec_to_time(%ld) WHERE ",
        OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, last_modify_time));
    ASSERT_EQ(OB_SUCCESS,
        append_lob_ctrl_pk_condition_(macro_ids[i], first_hash + i, update_sql));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(tenant_id, update_sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  ObArenaAllocator dag_allocator("LobCtrlFailGC");
  ObSSFailGCDag dag;
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_FALSE(ctx->need_gc_);
  ASSERT_FALSE(ctx->gc_scn_.is_valid());
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSLobCtrlFailGCTask *lob_ctrl_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLobCtrlFailGCTask *>(task)) {
      lob_ctrl_task = static_cast<ObSSLobCtrlFailGCTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, lob_ctrl_task);

  const int retryable_errors[] = {OB_EAGAIN, OB_TIMEOUT, OB_TRANS_TIMEOUT, OB_TRANS_STMT_TIMEOUT,
      OB_TRANS_RPC_TIMEOUT, OB_TRANS_UNKNOWN, OB_TRANS_COMMIT_TOO_MUCH_TIME};
  for (int64_t i = 0; i < ARRAYSIZEOF(retryable_errors); ++i) {
    ASSERT_TRUE(lob_ctrl_task->is_retryable_row_error_(retryable_errors[i]));
  }
  const int skipped_errors[] = {OB_ERR_EXCLUSIVE_LOCK_CONFLICT, OB_TRY_LOCK_ROW_CONFLICT,
      OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT, OB_ENTRY_NOT_EXIST, OB_STATE_NOT_MATCH,
      OB_CANCELED, OB_SERVICE_STOPPED};
  for (int64_t i = 0; i < ARRAYSIZEOF(skipped_errors); ++i) {
    ASSERT_FALSE(lob_ctrl_task->is_retryable_row_error_(skipped_errors[i]));
  }

  // A non-lock row operation error must also be isolated after a successful rollback.
  ObOuttableLobControlManager uninitialized_manager;
  ObOuttableLobControlRow failed_row;
  failed_row.key_.macro_id_ = macro_ids[1];
  failed_row.key_.hash_ = first_hash + 1;
  failed_row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  bool continue_scan = true;
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->delete_expired_fail_row_(
      *ctx, uninitialized_manager, failed_row, continue_scan));
  ASSERT_TRUE(continue_scan);

  // A transaction start timeout must exhaust only this row's retry budget, not stop the scan.
  {
    const int64_t saved_sql_timeout_us = ctx->sql_timeout_us_;
    DEFER(ctx->sql_timeout_us_ = saved_sql_timeout_us);
    ctx->sql_timeout_us_ = 100 * 1000;
    lib::WorkerTimeoutGuard worker_timeout_guard(ObTimeUtility::current_time() - 1);
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_TIMEOUT, trans.start(&sql_proxy, tenant_id));
    ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->delete_expired_fail_row_(
        *ctx, uninitialized_manager, failed_row, continue_scan));
    ASSERT_TRUE(continue_scan);
  }

  // Persistent fast failures must stop at the retry limit, even with a large SQL timeout.
  {
    const int64_t saved_sql_timeout_us = ctx->sql_timeout_us_;
    DEFER(ctx->sql_timeout_us_ = saved_sql_timeout_us);
    ctx->sql_timeout_us_ = 60L * 1000L * 1000L;
    lib::WorkerTimeoutGuard worker_timeout_guard(ObTimeUtility::current_time() - 1);
    const int64_t start_us = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->delete_expired_fail_row_(
        *ctx, uninitialized_manager, failed_row, continue_scan));
    const int64_t elapsed_us = ObTimeUtility::current_time() - start_us;
    ASSERT_LT(elapsed_us, 3L * 1000L * 1000L);
    ASSERT_TRUE(continue_scan);
  }

  ObMySQLTransaction lock_trans;
  ASSERT_EQ(OB_SUCCESS, lock_trans.start(&sql_proxy, tenant_id));
  DEFER({
    if (lock_trans.is_started()) {
      EXPECT_EQ(OB_SUCCESS, lock_trans.end(false /*rollback*/));
    }
  });
  ObArenaAllocator lock_allocator("LobCtrlLock");
  ObOuttableLobControlRow locked_row;
  int64_t locked_row_scn = 0;
  for (int64_t i = 5; i < ARRAYSIZEOF(macro_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        lock_trans, tenant_id, macro_ids[i], first_hash + i,
        ObOuttableLobControlOperator::LockMode::FOR_UPDATE,
        lock_allocator, locked_row_scn, locked_row));
  }
  // Check the business result directly; process() deliberately isolates business failures.
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->process_lob_ctrl_fail_gc_(*ctx));

  ObArenaAllocator allocator("LobCtrlVerify");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(macro_ids); ++i) {
    allocator.reuse();
    selected_row.reset();
    const int select_ret = ObOuttableLobControlOperator::select(
        sql_proxy, tenant_id, macro_ids[i], first_hash + i,
        ObOuttableLobControlOperator::LockMode::NO_LOCK, allocator, row_scn, selected_row);
    if (1 == i) {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, select_ret);
    } else {
      ASSERT_EQ(OB_SUCCESS, select_ret);
      const int64_t expected_status = 0 == i ? OUTTABLE_LOB_CONTROL_FAIL : statuses[i];
      ASSERT_EQ(expected_status, selected_row.status_);
      if (0 == i) {
        ASSERT_GT(selected_row.last_modify_time_, cutoff_ts_us);
      } else if (2 == i || i >= 5) {
        ASSERT_EQ(old_ts_us, selected_row.last_modify_time_);
      } else {
        ASSERT_EQ(recent_ts_us, selected_row.last_modify_time_);
      }
    }
  }
  // Rechecking a deletion that already committed must finish without retrying a missing row.
  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->delete_expired_fail_row_(
      *ctx, manager, failed_row, continue_scan));
  ASSERT_TRUE(continue_scan);

  ASSERT_EQ(OB_SUCCESS, lock_trans.end(false /*rollback*/));
  ASSERT_EQ(OB_SUCCESS, lob_ctrl_task->process_lob_ctrl_fail_gc_(*ctx));
  for (int64_t i = 5; i < ARRAYSIZEOF(macro_ids); ++i) {
    const int select_ret = ObOuttableLobControlOperator::select(
        sql_proxy, tenant_id, macro_ids[i], first_hash + i,
        ObOuttableLobControlOperator::LockMode::NO_LOCK, allocator, row_scn, selected_row);
    if (5 == i) {
      ASSERT_EQ(OB_SUCCESS, select_ret);
      ASSERT_EQ(OUTTABLE_LOB_CONTROL_FAIL, selected_row.status_);
    } else {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, select_ret);
    }
  }
}

TEST_F(TestSSFailGCGenTask, fail_gc_prepare_populates_dag_context)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID expected_ls(85);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, expected_ls));

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCPrepareTask *prepare_task = nullptr;
  ObSSFailGCScheduleTask *schedule_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCPrepareTask *>(task)) {
      prepare_task = static_cast<ObSSFailGCPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      schedule_task = static_cast<ObSSFailGCScheduleTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, schedule_task);

  ASSERT_EQ(OB_SUCCESS, prepare_task->process());
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_EQ(OB_SYS_TENANT_ID, ctx->tenant_id_);
  ASSERT_GT(ctx->sql_timeout_us_, 0);
  ASSERT_TRUE(ctx->need_gc_);
  ASSERT_TRUE(ctx->gc_scn_.is_valid());
  ASSERT_GT(ctx->ls_ctxs_.count(), 0);

  int64_t expected_ls_count = 0;
  for (int64_t i = 0; i < ctx->ls_ctxs_.count(); ++i) {
    ObSSFailGCLSCtx *ls_ctx = ctx->ls_ctxs_.at(i);
    ASSERT_NE(nullptr, ls_ctx);
    ASSERT_TRUE(ls_ctx->is_valid());
    ASSERT_FALSE(ls_ctx->checkpoint_meta_scn_.is_valid());
    ASSERT_FALSE(ls_ctx->iterator_inited_);
    ASSERT_FALSE(ls_ctx->scan_finished_);
    ASSERT_FALSE(ls_ctx->scan_failed_);
    ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
    if (expected_ls == ls_ctx->ls_id_) {
      ++expected_ls_count;
    }
  }
  ASSERT_EQ(1, expected_ls_count);
}

TEST_F(TestSSFailGCGenTask, fail_gc_head_generates_ls_siblings_without_barrier)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(100);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  for (int64_t i = 0; i < 3; ++i) {
    ObSSFailGCLSCtx *ls_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(860 + i), ls_ctx));
    ASSERT_NE(nullptr, ls_ctx);
  }

  ObSSFailGCScheduleTask *heads[3] = {nullptr, nullptr, nullptr};
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      ObSSFailGCScheduleTask *schedule =
          static_cast<ObSSFailGCScheduleTask *>(task);
      ASSERT_EQ(0, schedule->ls_idx_);
      heads[0] = schedule;
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, heads[0]);
  ASSERT_EQ(1, heads[0]->get_indegree());

  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, heads[i]);
    const int64_t old_task_count = dag.get_task_list_count();
    ASSERT_EQ(OB_SUCCESS,
        static_cast<ObITask *>(heads[i])->generate_next_task());
    ASSERT_EQ(old_task_count + (i < 2 ? 1 : 0),
        dag.get_task_list_count());
    if (i < 2) {
      // 派发失败会重试同一个 HEAD，重复生成不能再创建相同的 successor。
      const int64_t task_count_after_generate = dag.get_task_list_count();
      ObITask *duplicate_next_task = nullptr;
      ASSERT_EQ(OB_ITER_END,
          heads[i]->generate_next_task(duplicate_next_task));
      ASSERT_EQ(nullptr, duplicate_next_task);
      ASSERT_EQ(task_count_after_generate, dag.get_task_list_count());
    }

    task = dag.task_list_.get_first();
    while (nullptr != task && task_list_head != task) {
      if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
        ObSSFailGCScheduleTask *schedule =
            static_cast<ObSSFailGCScheduleTask *>(task);
        ASSERT_GE(schedule->ls_idx_, 0);
        ASSERT_LT(schedule->ls_idx_, 3);
        heads[schedule->ls_idx_] = schedule;
      }
      task = task->get_next();
    }
  }

  ASSERT_EQ(5, dag.get_task_list_count());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, heads[i]);
    ASSERT_EQ(i, heads[i]->ls_idx_);
    ASSERT_TRUE(heads[i]->is_ls_head_);
    ASSERT_EQ(0 == i ? 1 : 0, heads[i]->get_indegree());
    ObSEArray<ObINodeWithChild *, 2> children;
    ASSERT_EQ(OB_SUCCESS, heads[i]->copy_child_nodes(children));
    ASSERT_EQ(0, children.count());
  }

  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, heads[i]->process());
    ASSERT_EQ(5, dag.get_task_list_count());
    ASSERT_TRUE(ctx->ls_ctxs_.at(i)->iterator_inited_);
    ASSERT_TRUE(ctx->ls_ctxs_.at(i)->scan_finished_);
    ASSERT_FALSE(ctx->ls_ctxs_.at(i)->scan_failed_);
    ObSEArray<ObINodeWithChild *, 2> children;
    ASSERT_EQ(OB_SUCCESS, heads[i]->copy_child_nodes(children));
    ASSERT_EQ(0, children.count());
  }
}

TEST_F(TestSSFailGCGenTask, fail_gc_schedule_generates_bounded_batches)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(863);
  const int64_t first_tablet_id = 863000;
  const int64_t row_count =
      ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE + 1;
  const SCN row_meta_scn = make_scn(10);
  for (int64_t i = 0; i < row_count; ++i) {
    ObSSMetaCtrlKey ctrl_key;
    const ObSSMetaCtrlStatus::STATUS status =
        0 == i % 2 ? ObSSMetaCtrlStatus::STATUS::START
                   : ObSSMetaCtrlStatus::STATUS::COMPLETE;
    ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_row_(
        OB_SYS_TENANT_ID, ls_id, first_tablet_id + i,
        row_meta_scn, status, ctrl_key));
  }

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(100);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCScheduleTask *head = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      head = static_cast<ObSSFailGCScheduleTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, head);
  ASSERT_EQ(3, dag.get_task_list_count());
  ObSEArray<ObINodeWithChild *, 4> children;
  ASSERT_EQ(OB_SUCCESS, head->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());

  ASSERT_EQ(OB_SUCCESS, head->process());
  ASSERT_TRUE(ls_ctx->iterator_inited_);
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_FALSE(ls_ctx->checkpoint_meta_scn_.is_valid());
  ASSERT_EQ(5, dag.get_task_list_count());

  ObSSFailGCExecutorTask *first_executor = nullptr;
  ObSSFailGCScheduleTask *continuation = nullptr;
  task = dag.task_list_.get_first();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCExecutorTask *>(task)) {
      ASSERT_EQ(nullptr, first_executor);
      first_executor = static_cast<ObSSFailGCExecutorTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      ObSSFailGCScheduleTask *schedule =
          static_cast<ObSSFailGCScheduleTask *>(task);
      if (!schedule->is_ls_head_) {
        ASSERT_EQ(nullptr, continuation);
        continuation = schedule;
      }
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, first_executor);
  ASSERT_NE(nullptr, continuation);
  ASSERT_EQ(ls_ctx, first_executor->ls_ctx_);
  ASSERT_EQ(ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE,
      first_executor->ctrl_rows_.count());
  ASSERT_EQ(0, continuation->ls_idx_);
  ASSERT_EQ(1, first_executor->get_indegree());
  ASSERT_EQ(1, continuation->get_indegree());

  children.reset();
  ASSERT_EQ(OB_SUCCESS, head->copy_child_nodes(children));
  ASSERT_EQ(2, children.count());
  bool head_has_executor = false;
  bool head_has_continuation = false;
  for (int64_t i = 0; i < children.count(); ++i) {
    head_has_executor |= children.at(i) == first_executor;
    head_has_continuation |= children.at(i) == continuation;
  }
  ASSERT_TRUE(head_has_executor);
  ASSERT_TRUE(head_has_continuation);

  children.reset();
  ASSERT_EQ(OB_SUCCESS, first_executor->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
  children.reset();
  ASSERT_EQ(OB_SUCCESS, continuation->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());

  const int64_t old_task_count = dag.get_task_list_count();
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(continuation)->generate_next_task());
  ASSERT_EQ(old_task_count, dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, continuation->process());
  ASSERT_EQ(6, dag.get_task_list_count());
  ASSERT_TRUE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_TRUE(ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_ctrl_row_count());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());

  int64_t executor_count = 0;
  int64_t continuation_count = 0;
  ObSSFailGCExecutorTask *second_executor = nullptr;
  std::set<uint64_t> tablet_ids;
  task = dag.task_list_.get_first();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCExecutorTask *>(task)) {
      ObSSFailGCExecutorTask *executor =
          static_cast<ObSSFailGCExecutorTask *>(task);
      ++executor_count;
      ASSERT_EQ(ls_ctx, executor->ls_ctx_);
      ASSERT_TRUE(1 == executor->ctrl_rows_.count()
          || ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE
              == executor->ctrl_rows_.count());
      if (executor != first_executor) {
        ASSERT_EQ(nullptr, second_executor);
        second_executor = executor;
      }
      for (int64_t i = 0; i < executor->ctrl_rows_.count(); ++i) {
        const ObSSFailGCCtrlRow &row = executor->ctrl_rows_.at(i);
        ASSERT_TRUE(row.is_valid());
        ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_META,
            row.ctrl_key_.type_);
        const uint64_t tablet_id =
            row.ctrl_key_.tablet_key_.tablet_id_.id();
        ASSERT_TRUE(tablet_ids.insert(tablet_id).second);
        const ObSSMetaCtrlStatus::STATUS expected_status =
            0 == (tablet_id - first_tablet_id) % 2
                ? ObSSMetaCtrlStatus::STATUS::START
                : ObSSMetaCtrlStatus::STATUS::COMPLETE;
        ASSERT_EQ(expected_status, row.ctrl_value_.ctrl_info_.status_.status_);
      }
      children.reset();
      ASSERT_EQ(OB_SUCCESS, executor->copy_child_nodes(children));
      ASSERT_EQ(0, children.count());
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)
        && !static_cast<ObSSFailGCScheduleTask *>(task)->is_ls_head_) {
      ++continuation_count;
    }
    task = task->get_next();
  }
  ASSERT_EQ(2, executor_count);
  ASSERT_EQ(1, continuation_count);
  ASSERT_NE(nullptr, second_executor);
  ASSERT_EQ(ls_ctx, continuation->ls_ctx_);
  ASSERT_EQ(1, second_executor->get_indegree());
  children.reset();
  ASSERT_EQ(OB_SUCCESS, continuation->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_EQ(static_cast<ObINodeWithChild *>(second_executor), children.at(0));
  ASSERT_EQ(row_count, tablet_ids.size());
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(1, tablet_ids.count(first_tablet_id + i));
  }
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_schedule_exact_batch_stops_after_empty_continuation)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(872);
  const int64_t first_tablet_id = 872000;
  const int64_t row_count = ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE;
  const SCN row_meta_scn = make_scn(10);
  for (int64_t i = 0; i < row_count; ++i) {
    ObSSMetaCtrlKey ctrl_key;
    ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_row_(
        OB_SYS_TENANT_ID, ls_id, first_tablet_id + i, row_meta_scn,
        ObSSMetaCtrlStatus::STATUS::START, ctrl_key));
  }

  ObArenaAllocator dag_allocator("FailGCExact");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());

  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(100);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCScheduleTask *head = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)) {
      head = static_cast<ObSSFailGCScheduleTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, head);
  ASSERT_EQ(OB_SUCCESS, head->process());
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_ctrl_row_count());
  ASSERT_EQ(5, dag.get_task_list_count());

  int64_t executor_count = 0;
  int64_t continuation_count = 0;
  ObSSFailGCScheduleTask *continuation = nullptr;
  task = dag.task_list_.get_first();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSFailGCExecutorTask *>(task)) {
      ++executor_count;
    } else if (nullptr != dynamic_cast<ObSSFailGCScheduleTask *>(task)
        && !static_cast<ObSSFailGCScheduleTask *>(task)->is_ls_head_) {
      ++continuation_count;
      continuation = static_cast<ObSSFailGCScheduleTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_EQ(1, executor_count);
  ASSERT_EQ(1, continuation_count);
  ASSERT_NE(nullptr, continuation);

  const int64_t old_task_count = dag.get_task_list_count();
  ASSERT_EQ(OB_SUCCESS, continuation->process());
  ASSERT_EQ(old_task_count, dag.get_task_list_count());
  ASSERT_TRUE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_TRUE(ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_ctrl_row_count());
  ObSEArray<ObINodeWithChild *, 2> children;
  ASSERT_EQ(OB_SUCCESS, continuation->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_schedule_absorbs_page_iterator_timeout)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(869);

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCScheduleTask *schedule = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(schedule));
  ASSERT_NE(nullptr, schedule);
  ASSERT_EQ(OB_SUCCESS, schedule->init(0, true /* is_ls_head */));
  ASSERT_EQ(OB_SUCCESS, schedule->bind_ls_ctx_(*ctx));
  ASSERT_EQ(OB_SUCCESS, schedule->init_ls_iterator_(*ctx));

  ObSSMetaCtrlPageIterator::PageRow page_row;
  const ObSSMetaCtrlKey ctrl_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(869001), SCN::min_scn(), make_scn(100), ls_id);
  ASSERT_EQ(OB_SUCCESS, page_row.ctrl_key_.assign(ctrl_key));
  page_row.ctrl_value_.ctrl_info_.lease_epoch_ = 1;
  page_row.ctrl_value_.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS,
      page_row.ctrl_value_.ctrl_info_.status_.set_status_start());
  page_row.ctrl_value_.object_info_.set_none();
  page_row.meta_key_ = ObString::make_string("timeout-cursor");
  ASSERT_TRUE(page_row.ctrl_key_.is_valid());
  ASSERT_TRUE(page_row.ctrl_value_.is_valid());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->page_iter_.page_buffer_.push_back(page_row));

  const int64_t old_task_count = dag.get_task_list_count();
  {
    lib::WorkerTimeoutGuard worker_timeout_guard(
        ObTimeUtility::current_time() - 1);
    ASSERT_EQ(OB_SUCCESS, schedule->process());
  }

  ASSERT_EQ(old_task_count, dag.get_task_list_count());
  ASSERT_TRUE(ls_ctx->iterator_inited_);
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_TRUE(ls_ctx->scan_failed_);
  ASSERT_FALSE(ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(1, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(1, ctx->get_scanned_ctrl_row_count());
  ASSERT_EQ(OB_TIMEOUT, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(0, ctx->get_generated_task_count());
  ASSERT_EQ(OB_SUCCESS, dag.get_dag_ret());
  ASSERT_FALSE(dag.is_dag_failed());
  ObSEArray<ObINodeWithChild *, 2> children;
  ASSERT_EQ(OB_SUCCESS, schedule->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
}

TEST_F(TestSSFailGCGenTask,
    fail_gc_executor_skips_cancel_errors_and_commits_remaining_rows)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObMySQLProxy &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const ObLSID ls_id(870);
  const ObTabletID tablet_id(870001);
  const ObTabletID abandoned_tablet_id(870002);
  ObSSMetaCtrlKey ctrl_key;
  ObSSMetaCtrlKey abandoned_ctrl_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, make_scn(100), ObSSMetaCtrlStatus::STATUS::START,
      ctrl_key));
  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, ctrl_value));
  ASSERT_TRUE(ctrl_value.ctrl_info_.trace_id_.is_invalid());

  ObSSFailGCCtrlRow ctrl_row;
  ObSSFailGCCtrlRow abandoned_ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_ = ctrl_value;
  ObSEArray<ObSSFailGCCtrlRow, 2> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  CancelErrorSSFailGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);

  CancelErrorSSFailGCExecutorTask *unexpected_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(unexpected_executor));
  ASSERT_NE(nullptr, unexpected_executor);
  unexpected_executor->cancel_ret_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, unexpected_executor->init(ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_SUCCESS, unexpected_executor->process());
  ASSERT_EQ(1, unexpected_executor->cancel_count_);
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());

  // The failed START row must not block a later ABANDON row in the same batch.
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id, blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      abandoned_tablet_id, make_scn(200), ObSSMetaCtrlStatus::STATUS::ABANDON,
      abandoned_ctrl_key));
  ASSERT_EQ(OB_SUCCESS, abandoned_ctrl_row.ctrl_key_.assign(abandoned_ctrl_key));
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, abandoned_ctrl_key, abandoned_ctrl_row.ctrl_value_));
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(abandoned_ctrl_row));
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->cancel_count_);
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(1, ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(1, ctx->get_generated_task_count());
  ASSERT_EQ(OB_SUCCESS, dag.get_dag_ret());
  ASSERT_FALSE(dag.is_dag_failed());

  ObSSMetaCtrlValue remaining_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, remaining_value));
  ASSERT_TRUE(remaining_value.ctrl_info_.status_.is_start_status());
  ASSERT_EQ(OB_ITER_END, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, abandoned_ctrl_key, remaining_value));
  ObSEArray<ObSharedGCTaskKey, 1> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID, blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /*op_id*/, ls_id, abandoned_tablet_id, task_keys));
  ASSERT_EQ(1, task_keys.count());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(task_keys.at(0), proxy));
  ASSERT_EQ(OB_SUCCESS,
      delete_ctrl_row_(OB_SYS_TENANT_ID, ls_id, ctrl_key));
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_absorbs_batch_timeout_and_rolls_back)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(871);
  const ObTabletID tablet_id(871001);
  ObSSMetaCtrlKey ctrl_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, make_scn(100), ObSSMetaCtrlStatus::STATUS::ABANDON,
      ctrl_key));
  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, ctrl_value));

  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_ = ctrl_value;
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  DeleteTimeoutSSFailGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->delete_count_);
  ASSERT_EQ(OB_TRANS_RPC_TIMEOUT, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_TRANS_RPC_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_EQ(1, ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(0, ctx->get_generated_task_count());
  ASSERT_EQ(OB_SUCCESS, dag.get_dag_ret());
  ASSERT_FALSE(dag.is_dag_failed());

  ObSSMetaCtrlValue remaining_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, remaining_value));
  ASSERT_TRUE(remaining_value.ctrl_info_.status_.is_abandon_status());
  ObSEArray<ObSharedGCTaskKey, 1> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_SUCCESS,
      delete_ctrl_row_(OB_SYS_TENANT_ID, ls_id, ctrl_key));
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_process_commits_one_mixed_batch)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const ObLSID ls_id(865);
  const ObTabletID abandoned_tablet_id(865001);
  const ObTabletID future_tablet_id(865003);
  const SCN old_meta_scn = make_scn(100);
  const SCN ls_meta_scn = make_scn(110);
  const SCN future_meta_scn = make_scn(1000);

  ObSSMetaCtrlKey abandoned_tablet_key;
  ObSSMetaCtrlKey abandoned_ls_key;
  ObSSMetaCtrlKey future_tablet_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      abandoned_tablet_id, old_meta_scn,
      ObSSMetaCtrlStatus::STATUS::ABANDON, abandoned_tablet_key));
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_LS_META,
      ObTabletID(ls_id.id()), ls_meta_scn,
      ObSSMetaCtrlStatus::STATUS::ABANDON, abandoned_ls_key));
  ASSERT_TRUE(abandoned_ls_key.is_ls_type());
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      future_tablet_id, future_meta_scn,
      ObSSMetaCtrlStatus::STATUS::START, future_tablet_key));

  ObSEArray<ObSSFailGCCtrlRow, 3> ctrl_rows;
  const ObSSMetaCtrlKey *ctrl_keys[] = {
      &abandoned_tablet_key, &abandoned_ls_key, &future_tablet_key};
  for (int64_t i = 0; i < ARRAYSIZEOF(ctrl_keys); ++i) {
    ObSSFailGCCtrlRow ctrl_row;
    ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(*ctrl_keys[i]));
    ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
        OB_SYS_TENANT_ID, ls_id, *ctrl_keys[i], ctrl_row.ctrl_value_));
    ASSERT_TRUE(ctrl_row.is_valid());
    ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  }

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_SUCCESS, executor->process());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(2, ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(1, ctx->get_generated_task_count());

  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_ITER_END, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, abandoned_tablet_key, ctrl_value));
  ASSERT_EQ(OB_ITER_END, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, abandoned_ls_key, ctrl_value));
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, future_tablet_key, ctrl_value));
  ASSERT_TRUE(ctrl_value.ctrl_info_.status_.is_start_status());

  ObSEArray<ObSharedGCTaskKey, 2> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, abandoned_tablet_id, task_keys));
  ASSERT_EQ(1, task_keys.count());
  const ObSharedGCTaskKey generated_task_key = task_keys.at(0);
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_LS_META,
      1 /* op_id */, ls_id, ObTabletID(ls_id.id()), task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, future_tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());

  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_task(generated_task_key, proxy));
  ASSERT_EQ(OB_SUCCESS,
      delete_ctrl_row_(OB_SYS_TENANT_ID, ls_id, future_tablet_key));
}

TEST_F(TestSSFailGCGenTask,
       cross_observer_fail_gc_transactions_commit_atomically)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const ObLSID ls_id(868);
  const ObTabletID tablet_id(868001);
  ObSSMetaCtrlKey ctrl_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, make_scn(100), ObSSMetaCtrlStatus::STATUS::ABANDON,
      ctrl_key));

  // Both observers complete their stale read before either transaction is
  // allowed to start.
  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, ctrl_value));
  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_ = ctrl_value;
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

  ObArenaAllocator observer_a_dag_allocator("FailGCDagA");
  ObArenaAllocator observer_b_dag_allocator("FailGCDagB");
  ObSSFailGCDag observer_a_dag;
  ObSSFailGCDag observer_b_dag;
  ASSERT_EQ(OB_SUCCESS, observer_a_dag.basic_init(observer_a_dag_allocator));
  ASSERT_EQ(OB_SUCCESS, observer_b_dag.basic_init(observer_b_dag_allocator));
  ObSSFailGCDagParam observer_a_param;
  ObSSFailGCDagParam observer_b_param;
  ASSERT_EQ(OB_SUCCESS, observer_a_param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, observer_b_param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, observer_a_dag.init_by_param(&observer_a_param));
  ASSERT_EQ(OB_SUCCESS, observer_b_dag.init_by_param(&observer_b_param));

  ObSSFailGCDagCtx *observer_a_ctx = observer_a_dag.get_ctx();
  ObSSFailGCDagCtx *observer_b_ctx = observer_b_dag.get_ctx();
  ASSERT_NE(nullptr, observer_a_ctx);
  ASSERT_NE(nullptr, observer_b_ctx);
  observer_a_ctx->need_gc_ = true;
  observer_a_ctx->gc_scn_ = make_scn(500);
  observer_a_ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  observer_b_ctx->need_gc_ = true;
  observer_b_ctx->gc_scn_ = make_scn(500);
  observer_b_ctx->sql_timeout_us_ = SQL_TIMEOUT_US;

  ObSSFailGCLSCtx *observer_a_ls_ctx = nullptr;
  ObSSFailGCLSCtx *observer_b_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      observer_a_ctx->alloc_ls_ctx(ls_id, observer_a_ls_ctx));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_ctx->alloc_ls_ctx(ls_id, observer_b_ls_ctx));
  ASSERT_NE(nullptr, observer_a_ls_ctx);
  ASSERT_NE(nullptr, observer_b_ls_ctx);
  ObSSFailGCExecutorTask *observer_a_executor = nullptr;
  ObSSFailGCExecutorTask *observer_b_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, observer_a_dag.alloc_task(observer_a_executor));
  ASSERT_EQ(OB_SUCCESS, observer_b_dag.alloc_task(observer_b_executor));
  ASSERT_NE(nullptr, observer_a_executor);
  ASSERT_NE(nullptr, observer_b_executor);
  ASSERT_EQ(OB_SUCCESS,
      observer_a_executor->init(observer_a_ls_ctx, ctrl_rows));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_executor->init(observer_b_ls_ctx, ctrl_rows));

  FailGCTestBarrier executor_start_barrier(2);
  FakeLocalSSGCScheduler observer_a_scheduler([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret) && !executor_start_barrier.arrive_and_wait()) {
      ret = OB_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      ret = observer_a_executor->process();
    }
    return ret;
  });
  FakeLocalSSGCScheduler observer_b_scheduler([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret) && !executor_start_barrier.arrive_and_wait()) {
      ret = OB_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      ret = observer_b_executor->process();
    }
    return ret;
  });
  volatile bool observer_a_stopped = false;
  volatile bool observer_b_stopped = false;
  FakeObserverFailGCTimer observer_a_timer(
      observer_a_stopped, observer_a_scheduler);
  FakeObserverFailGCTimer observer_b_timer(
      observer_b_stopped, observer_b_scheduler);
  ASSERT_EQ(OB_SUCCESS, observer_a_timer.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, observer_b_timer.init(OB_SYS_TENANT_ID));
  observer_a_timer.runTimerTask();
  observer_b_timer.runTimerTask();
  ASSERT_TRUE(observer_a_scheduler.wait_entered());
  ASSERT_TRUE(observer_b_scheduler.wait_entered());
  ASSERT_TRUE(observer_a_scheduler.is_resident());
  ASSERT_TRUE(observer_b_scheduler.is_resident());

  observer_a_scheduler.release_business();
  observer_b_scheduler.release_business();
  const bool observer_a_finished = observer_a_scheduler.wait_finished();
  const bool observer_b_finished = observer_b_scheduler.wait_finished();
  observer_a_scheduler.join();
  observer_b_scheduler.join();
  EXPECT_TRUE(observer_a_finished);
  EXPECT_TRUE(observer_b_finished);
  ASSERT_EQ(OB_SUCCESS, observer_a_scheduler.get_work_ret());
  ASSERT_EQ(OB_SUCCESS, observer_b_scheduler.get_work_ret());
  ASSERT_EQ(1, observer_a_ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(1, observer_b_ctx->get_abandoned_ctrl_row_count());
  ASSERT_EQ(1, observer_a_ctx->get_generated_task_count());
  ASSERT_EQ(1, observer_b_ctx->get_generated_task_count());

  ObSSMetaCtrlValue remaining_value;
  ASSERT_EQ(OB_ITER_END, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, remaining_value));
  ObSEArray<ObSharedGCTaskKey, 2> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(2, task_keys.count());
  ASSERT_NE(task_keys.at(0).task_id_, task_keys.at(1).task_id_);

  ObArenaAllocator task_allocator("FailGCDupTasks");
  ObSharedGCTask observer_a_task;
  ObSharedGCTask observer_b_task;
  ASSERT_EQ(OB_SUCCESS,
      get_fail_gc_task_(task_keys.at(0), task_allocator, observer_a_task));
  ASSERT_EQ(OB_SUCCESS,
      get_fail_gc_task_(task_keys.at(1), task_allocator, observer_b_task));
  ASSERT_TRUE(observer_a_task.value_ == observer_b_task.value_);

  // Exercise the downstream active-to-history completion concurrently for
  // both semantic duplicates. Distinct task ids remain independently
  // traceable, while neither active row nor history state is lost.
  FailGCTestBarrier history_start_barrier(2);
  std::atomic<int> observer_a_history_ret(OB_ERR_UNEXPECTED);
  std::atomic<int> observer_b_history_ret(OB_ERR_UNEXPECTED);
  std::thread observer_a_history_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret) && !history_start_barrier.arrive_and_wait()) {
      ret = OB_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      ret = ObSharedGCTaskTableOperator::delete_task_and_insert_history(
          observer_a_task, proxy);
    }
    observer_a_history_ret.store(ret, std::memory_order_release);
  });
  std::thread observer_b_history_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret) && !history_start_barrier.arrive_and_wait()) {
      ret = OB_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      ret = ObSharedGCTaskTableOperator::delete_task_and_insert_history(
          observer_b_task, proxy);
    }
    observer_b_history_ret.store(ret, std::memory_order_release);
  });
  observer_a_history_worker.join();
  observer_b_history_worker.join();
  ASSERT_EQ(OB_SUCCESS,
      observer_a_history_ret.load(std::memory_order_acquire));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_history_ret.load(std::memory_order_acquire));

  task_keys.reset();
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ObSharedGCTask observer_a_history;
  ObSharedGCTask observer_b_history;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      observer_a_task.key_, task_allocator, proxy, observer_a_history));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      observer_b_task.key_, task_allocator, proxy, observer_b_history));
  ASSERT_TRUE(observer_a_task.value_ == observer_a_history.value_);
  ASSERT_TRUE(observer_b_task.value_ == observer_b_history.value_);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(observer_a_task.key_, proxy));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(observer_b_task.key_, proxy));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::delete_task(observer_a_task.key_, proxy));
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_batch_transaction_rolls_back_atomically)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(866);
  const ObTabletID tablet_id(866001);
  ObSSMetaCtrlKey ctrl_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, make_scn(100), ObSSMetaCtrlStatus::STATUS::ABANDON,
      ctrl_key));
  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, ctrl_value));

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_ = ctrl_value;
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  ObSSFailGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, ctrl_rows));

  ObSSFailGCInfo valid_info;
  valid_info.key_info_.tenant_id_ = OB_SYS_TENANT_ID;
  valid_info.key_info_.ls_id_ = ls_id;
  ASSERT_EQ(OB_SUCCESS, valid_info.key_info_.key_.assign(ctrl_key));
  valid_info.ctrl_value_ = ctrl_value;
  valid_info.need_generate_task_ = true;
  ASSERT_TRUE(valid_info.is_valid());
  ObSEArray<ObSSFailGCInfo, 2> abandoned_key_infos;
  ASSERT_EQ(OB_SUCCESS, abandoned_key_infos.push_back(valid_info));
  ObSSFailGCInfo mismatched_info;
  ASSERT_EQ(OB_SUCCESS, mismatched_info.assign(valid_info));
  mismatched_info.key_info_.tenant_id_ = OB_SYS_TENANT_ID + 1;
  ASSERT_TRUE(mismatched_info.is_valid());
  ASSERT_EQ(OB_SUCCESS, abandoned_key_infos.push_back(mismatched_info));

  ObSEArray<ObSharedGCTaskKey, 2> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      executor->generate_fail_gc_task_(*ctx, abandoned_key_infos));
  ASSERT_EQ(0, ctx->get_generated_task_count());

  ObSSMetaCtrlValue remaining_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, remaining_value));
  ASSERT_TRUE(remaining_value.ctrl_info_.status_.is_abandon_status());
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_SUCCESS,
      delete_ctrl_row_(OB_SYS_TENANT_ID, ls_id, ctrl_key));
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_transaction_loop_stops_and_rolls_back)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(867);
  const ObTabletID tablet_id(867001);
  ObSSMetaCtrlKey ctrl_key;
  ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_(
      OB_SYS_TENANT_ID, ls_id,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, make_scn(100), ObSSMetaCtrlStatus::STATUS::ABANDON,
      ctrl_key));
  ObSSMetaCtrlValue ctrl_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, ctrl_value));

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(500);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);

  ObSSFailGCCtrlRow ctrl_row;
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_ = ctrl_value;
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));
  ObSSFailGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, ctrl_rows));

  ObSSFailGCInfo fail_gc_info;
  fail_gc_info.key_info_.tenant_id_ = OB_SYS_TENANT_ID;
  fail_gc_info.key_info_.ls_id_ = ls_id;
  ASSERT_EQ(OB_SUCCESS, fail_gc_info.key_info_.key_.assign(ctrl_key));
  fail_gc_info.ctrl_value_ = ctrl_value;
  fail_gc_info.need_generate_task_ = true;
  ASSERT_TRUE(fail_gc_info.is_valid());
  ObSEArray<ObSSFailGCInfo, 1> abandoned_key_infos;
  ASSERT_EQ(OB_SUCCESS, abandoned_key_infos.push_back(fail_gc_info));

  ObSEArray<ObSharedGCTaskKey, 1> task_keys;
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());

  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED,
      executor->generate_fail_gc_task_(*ctx, abandoned_key_infos));

  ObSSMetaCtrlValue remaining_value;
  ASSERT_EQ(OB_SUCCESS, read_ctrl_value_(
      OB_SYS_TENANT_ID, ls_id, ctrl_key, remaining_value));
  ASSERT_TRUE(remaining_value.ctrl_info_.status_.is_abandon_status());
  ASSERT_EQ(OB_SUCCESS, get_matching_fail_gc_task_keys_(
      OB_SYS_TENANT_ID,
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      1 /* op_id */, ls_id, tablet_id, task_keys));
  ASSERT_EQ(0, task_keys.count());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS,
      delete_ctrl_row_(OB_SYS_TENANT_ID, ls_id, ctrl_key));
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_process_requires_initialization_and_dag_context)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(864);
  ObSSFailGCLSCtx ls_ctx;
  ls_ctx.ls_id_ = ls_id;
  ObSSFailGCCtrlRow ctrl_row;
  const ObSSMetaCtrlKey ctrl_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(864000), SCN::min_scn(), make_scn(10), ls_id);
  ASSERT_EQ(OB_SUCCESS, ctrl_row.ctrl_key_.assign(ctrl_key));
  ctrl_row.ctrl_value_.ctrl_info_.lease_epoch_ = 1;
  ctrl_row.ctrl_value_.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS,
      ctrl_row.ctrl_value_.ctrl_info_.status_.set_status_start());
  ctrl_row.ctrl_value_.object_info_.set_none();
  ASSERT_TRUE(ctrl_row.is_valid());
  ObSEArray<ObSSFailGCCtrlRow, 1> ctrl_rows;
  ASSERT_EQ(OB_SUCCESS, ctrl_rows.push_back(ctrl_row));

  ObSSFailGCExecutorTask executor;
  ASSERT_EQ(OB_NOT_INIT, executor.process());
  ASSERT_EQ(OB_SUCCESS, executor.init(&ls_ctx, ctrl_rows));
  ASSERT_TRUE(executor.fail_gc_tablet_map_.created());
  ASSERT_EQ(OB_ERR_UNEXPECTED, executor.process());

  ObArenaAllocator dag_allocator("FailGCDagTest");
  ObSSFailGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSFailGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->need_gc_ = true;
  ctx->gc_scn_ = make_scn(100);
  ctx->sql_timeout_us_ = SQL_TIMEOUT_US;
  ObSSFailGCExecutorTask *unowned_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(unowned_executor));
  ASSERT_NE(nullptr, unowned_executor);
  ASSERT_EQ(OB_SUCCESS, unowned_executor->init(&ls_ctx, ctrl_rows));
  ASSERT_FALSE(ctx->owns_ls_ctx_(ls_ctx));
  ASSERT_EQ(OB_ERR_UNEXPECTED, unowned_executor->process());
}

TEST_F(TestSSFailGCGenTask,
       fail_gc_executor_checkpoint_meta_scn_invalid_is_fail_closed)
{
  ObSSFailGCExecutorTask executor;

  ObSSMetaCtrlKey checkpoint_key;
  checkpoint_key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_CKPT;
  checkpoint_key.ls_key_.first_id_ = ObLSID(84).id();
  checkpoint_key.ls_key_.second_id_ = 0;
  checkpoint_key.ls_key_.control_scn_ = make_scn(100);
  ASSERT_TRUE(checkpoint_key.is_valid());
  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctrl_value.ctrl_info_.status_.set_status_start());
  ctrl_value.object_info_.set_none();
  ASSERT_TRUE(ctrl_value.is_valid());
  const share::SCN meta_scn = make_scn(100);
  const share::SCN gc_scn = make_scn(500);
  share::SCN checkpoint_meta_scn;
  bool can_do_failed_gc = true;

  checkpoint_meta_scn.reset();
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      checkpoint_key, ctrl_value, meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_FALSE(can_do_failed_gc);

  checkpoint_meta_scn = meta_scn;
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      checkpoint_key, ctrl_value, meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_FALSE(can_do_failed_gc);

  checkpoint_meta_scn = make_scn(200);
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      checkpoint_key, ctrl_value, meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_TRUE(can_do_failed_gc);

  ObSSMetaCtrlKey tablet_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(840), SCN::min_scn(), meta_scn, ObLSID(84));
  ASSERT_TRUE(tablet_key.is_valid());
  checkpoint_meta_scn.reset();
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      tablet_key, ctrl_value, meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_TRUE(can_do_failed_gc);

  const share::SCN future_meta_scn = make_scn(500);
  ObSSMetaCtrlKey future_tablet_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(841), SCN::min_scn(), future_meta_scn, ObLSID(84));
  ASSERT_TRUE(future_tablet_key.is_valid());
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      future_tablet_key, ctrl_value, future_meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_FALSE(can_do_failed_gc);

  ASSERT_EQ(OB_SUCCESS,
      ctrl_value.ctrl_info_.status_.set_status_abandon());
  ASSERT_EQ(OB_SUCCESS, executor.check_can_do_ctrl_row_failed_gc_(
      future_tablet_key, ctrl_value, future_meta_scn, gc_scn,
      checkpoint_meta_scn, can_do_failed_gc));
  ASSERT_TRUE(can_do_failed_gc);
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

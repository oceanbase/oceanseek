/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#define UNITTEST

#include <gmock/gmock.h>
#include <pthread.h>
#include <signal.h>
#include <atomic>
#include <thread>
#include <vector>
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_debug_sync.h"
#include "share/ob_upgrade_utils.h"
#include "observer/ob_srv_network_frame.h"
#include "share/io/ob_io_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "src/storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "src/storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_cleanup_task.h"
#include "src/storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_record_cleanup_task.h"
#include "src/storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "src/storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "src/storage/shared_storage/ob_remote_io_adapter.h"
#include "src/storage/shared_storage/ob_ss_object_access_util.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"

namespace oceanbase
{
// Override the weak symbol in ob_simple_server.cpp so the simple server
// starts in shared-storage mode with a MinIO-backed object store.
char *shared_storage_info = nullptr;

// Mock implementations required by the shared-storage bootstrap path.
// All shared-storage simple_server tests need these to avoid OB_NOT_INIT
// (-4006) failures during prepare_bootstrap, where ObServerZoneOpService
// and ObMasterKeyUtil call into PALF KV (not available in test environments).
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{
} // namespace sslog

using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

class ScopedNamedEvent
{
public:
  ScopedNamedEvent() : name_(nullptr), is_inited_(false) {}
  ~ScopedNamedEvent() { reset(); }

  int init(const char *name, const int error_code, const int64_t occur)
  {
    int ret = OB_SUCCESS;
    EventItem item;
    if (OB_ISNULL(name) || OB_SUCCESS == error_code || occur <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      item.error_code_ = error_code;
      item.occur_ = occur;
      item.trigger_freq_ = 0;
      item.cond_ = GETTID();
      if (OB_FAIL(EventTable::set_event(name, item))) {
      } else {
        name_ = name;
        is_inited_ = true;
      }
    }
    return ret;
  }

  void reset()
  {
    if (is_inited_) {
      EventItem item;
      (void) EventTable::set_event(name_, item);
      name_ = nullptr;
      is_inited_ = false;
    }
  }

private:
  const char *name_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ScopedNamedEvent);
};

// After the first append, pause publish, read the batch boundary, and optionally set the tenant runner stop flag.
class ObScopedStudyDiagBatch
{
public:
  ObScopedStudyDiagBatch();
  ~ObScopedStudyDiagBatch();
  int init(
      ObSSMarkSweepDiagMgr &mgr,
      volatile bool *is_stopped);
  int wait();
  int64_t get_first_record_count() const { return first_record_count_; }
  int64_t get_first_data_len() const { return first_data_len_; }

private:
  static void *inspect_first_batch_(void *arg);
  static const int64_t TIMEOUT_US = 30 * 1000 * 1000;
  ObSSMarkSweepDiagMgr *mgr_;
  volatile bool *is_stopped_;
  bool old_stopped_;
  pthread_t thread_;
  bool is_started_;
  bool is_inited_;
  int64_t old_timeout_;
  int64_t first_record_count_;
  int64_t first_data_len_;
  int wait_ret_;
  int signal_ret_;
  ObDebugSyncAction action_;
  DISALLOW_COPY_AND_ASSIGN(ObScopedStudyDiagBatch);
};

ObScopedStudyDiagBatch::ObScopedStudyDiagBatch()
  : mgr_(nullptr),
    is_stopped_(nullptr),
    old_stopped_(false),
    thread_(0),
    is_started_(false),
    is_inited_(false),
    old_timeout_(0),
    first_record_count_(0),
    first_data_len_(0),
    wait_ret_(OB_SUCCESS),
    signal_ret_(OB_SUCCESS),
    action_()
{
}

ObScopedStudyDiagBatch::~ObScopedStudyDiagBatch()
{
  int tmp_ret = wait();
  char timeout_buf[64] = {};
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "fail to wait study diagnose batch thread");
  }
  if (OB_NOT_NULL(is_stopped_)) {
    ATOMIC_STORE(is_stopped_, old_stopped_);
  }
  if (is_inited_) {
    if (OB_TMP_FAIL(GDS.set_global_action(false/*reset*/, true/*clear*/, action_))) {
      LOG_WARN_RET(tmp_ret, "fail to clear study diagnose sync action");
    }
    // Consume only this test's events; leave other sync points and the pre-test global timeout alone.
    if (!action_.signal_.is_empty()
        && OB_TMP_FAIL(GDS.event_control_.wait(action_.signal_, 1/*timeout_us*/, true/*clear*/))) {
      LOG_WARN_RET(tmp_ret, "fail to consume study diagnose batch event");
    }
    if (!action_.wait_.is_empty()
        && OB_TMP_FAIL(GDS.event_control_.wait(action_.wait_, 1/*timeout_us*/, true/*clear*/))) {
      LOG_WARN_RET(tmp_ret, "fail to consume study diagnose resume event");
    }
    if (OB_TMP_FAIL(databuff_printf(timeout_buf, sizeof(timeout_buf), "%ldus", old_timeout_))) {
      LOG_WARN_RET(tmp_ret, "fail to format previous debug sync timeout");
    } else if (!GCONF.debug_sync_timeout.set_value(timeout_buf)) {
      LOG_WARN_RET(OB_INVALID_CONFIG, "fail to restore debug sync timeout", K(timeout_buf));
    }
  }
}

int ObScopedStudyDiagBatch::init(
    ObSSMarkSweepDiagMgr &mgr,
    volatile bool *is_stopped)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    old_timeout_ = GCONF.debug_sync_timeout;
    mgr_ = &mgr;
    is_stopped_ = is_stopped;
    old_stopped_ = OB_NOT_NULL(is_stopped_) && ATOMIC_LOAD(is_stopped_);
    action_.sync_point_ = AFTER_SS_MARK_SWEEP_STUDY_DIAG_BATCH;
    action_.timeout_ = TIMEOUT_US;
    action_.execute_ = 1;
    if (!GCONF.debug_sync_timeout.set_value("30s")) {
      ret = OB_INVALID_CONFIG;
    } else if (FALSE_IT(is_inited_ = true)) {
    } else if (OB_FAIL(action_.signal_.assign("study_diag_first_batch_appended"))) {
    } else if (OB_FAIL(action_.wait_.assign("study_diag_first_batch_checked"))) {
    } else if (OB_FAIL(GDS.set_global_action(false/*reset*/, false/*clear*/, action_))) {
    } else if (0 != pthread_create(&thread_, nullptr/*attr*/, inspect_first_batch_, this)) {
      ret = OB_ERR_SYS;
    } else {
      is_started_ = true;
    }
  }
  return ret;
}

int ObScopedStudyDiagBatch::wait()
{
  int ret = OB_SUCCESS;
  if (is_started_) {
    if (0 != pthread_join(thread_, nullptr/*retval*/)) {
      ret = OB_ERR_SYS;
    } else {
      is_started_ = false;
      ret = OB_SUCCESS == wait_ret_ ? signal_ret_ : wait_ret_;
    }
  }
  return ret;
}

void *ObScopedStudyDiagBatch::inspect_first_batch_(void *arg)
{
  ObScopedStudyDiagBatch *batch = static_cast<ObScopedStudyDiagBatch *>(arg);
  if (OB_NOT_NULL(batch)) {
    // wait may return success on timeout; check the signal under lock so the case cannot pass on timeout.
    batch->wait_ret_ = GDS.event_control_.wait(batch->action_.signal_, TIMEOUT_US, false/*clear*/);
    if (OB_SUCCESS == batch->wait_ret_) {
      int tmp_ret = OB_SUCCESS;
      ObDSEventControl::Event *event = nullptr;
      ObThreadCondGuard guard(GDS.event_control_.cond_);
      if (OB_TMP_FAIL(GDS.event_control_.find(batch->action_.signal_, event))) {
        batch->wait_ret_ = OB_ENTRY_NOT_EXIST == tmp_ret ? OB_TIMEOUT : tmp_ret;
      } else if (OB_ISNULL(event) || event->signal_cnt_ <= 0) {
        batch->wait_ret_ = OB_TIMEOUT;
      }
    }
    if (OB_SUCCESS == batch->wait_ret_) {
      if (OB_ISNULL(batch->mgr_) || OB_ISNULL(batch->mgr_->diag_file_)) {
        batch->wait_ret_ = OB_ERR_UNEXPECTED;
      } else {
        const int64_t file_idx = batch->mgr_->diag_file_idx_(ObMarkSweepGroupType::KEPT,
            ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/);
        ObSpinLockGuard guard(batch->mgr_->diag_file_[file_idx].lock_);
        if (!batch->mgr_->diag_file_[file_idx].is_opened_
            || batch->mgr_->diag_file_[file_idx].is_finished_) {
          batch->wait_ret_ = OB_STATE_NOT_MATCH;
        } else {
          batch->first_record_count_ = batch->mgr_->diag_file_[file_idx].index_.count();
          batch->first_data_len_ = batch->mgr_->diag_file_[file_idx].offset_;
          if (OB_NOT_NULL(batch->is_stopped_)) {
            ATOMIC_STORE(batch->is_stopped_, true);
          }
        }
      }
    }
    // On failure still release the publish thread; dtor restores the stop flag and sync settings.
    batch->signal_ret_ = GDS.event_control_.signal(batch->action_.wait_);
  }
  return nullptr;
}

// Test fixture for MarkSweep GC end-to-end tests using a normal user tenant
// ("tt1").  Using a user tenant avoids the sys-tenant double-delete branch
// (which is fail-closed until the double-delete API is ready) and exercises
// the real GC path.
//
// SQL routing:
//   The task/history tables for user tenant rows are stored in the meta tenant
//   (gen_meta_tenant_id(tenant_id_)).  ObSharedGCTaskTableOperator handles
//   routing internally; direct SQL in helpers must pass the meta tenant id.
//
// Round-id bands isolate tests from each other and from the background timer
// (allow_gc_round_id = 0 → cannot see rows with round_id > 0):
//   empty_table_noop          :  allow_gc_round_id = 5000  (no rows inserted)
//   small_batch_e2e           :  round_id = 6000,  allow = 6001
//   dirty_value_skip          :  round_id = 7000,  allow = 7001
//   round_bound_e2e           :  round_id = 8000/9000, allow = 8000
//   claim_once                :  round_id = 10000
//   sstable_macro_e2e         :  round_id = 11000, allow = 11001
//   slice_chain_e2e           :  round_id = 12000, allow = 12001
//   tablet_meta_in_list_e2e   :  round_id = 13000, allow = 13001
//   mixed_id_e2e              :  round_id = 14000, allow = 14001
//   unsupported_type_skip     :  round_id = 15000, allow = 15001
//   lease_takeover_e2e        :  round_id = 16000, allow = 16001
#ifndef OB_USE_ASAN
// Owns only the test fake-tenant allocator; restores the limit and frees on scope exit without touching real tenants.
class ObScopedStudyDiagTenantAllocator
{
public:
  ObScopedStudyDiagTenantAllocator()
      : allocator_(nullptr),
        tenant_id_(OB_INVALID_TENANT_ID),
        old_limit_(0),
        is_inited_(false),
        old_dump_action_(),
        has_saved_dump_action_(false)
  {}
  ~ObScopedStudyDiagTenantAllocator();
  int init(const uint64_t tenant_id);
  int restore_limit();
private:
  static const int MEMORY_DUMP_SIGNAL = 49;
  lib::ObMallocAllocator *allocator_;
  uint64_t tenant_id_;
  int64_t old_limit_;
  bool is_inited_;
  struct sigaction old_dump_action_;
  bool has_saved_dump_action_;
  DISALLOW_COPY_AND_ASSIGN(ObScopedStudyDiagTenantAllocator);
};

ObScopedStudyDiagTenantAllocator::~ObScopedStudyDiagTenantAllocator()
{
  if (is_inited_) {
    EXPECT_EQ(OB_SUCCESS, restore_limit());
    EXPECT_EQ(OB_SUCCESS, allocator_->recycle_tenant_allocator(tenant_id_));
  }
  if (has_saved_dump_action_) {
    EXPECT_EQ(0, sigaction(MEMORY_DUMP_SIGNAL, &old_dump_action_, nullptr));
  }
}

int ObScopedStudyDiagTenantAllocator::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // Context limit must be positive; 1 byte is smaller than the first chunk, so a real OOM is triggered.
  const int64_t oom_limit = 1;
  lib::ObTenantCtxAllocatorGuard ctx_allocator;
  struct sigaction ignore_dump_action;
  allocator_ = lib::ObMallocAllocator::get_instance();
  if (is_inited_ || tenant_id <= OB_USER_TENANT_ID || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (allocator_->is_tenant_allocator_exist(tenant_id)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(allocator_->create_and_add_tenant_allocator(tenant_id))) {
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    ctx_allocator = allocator_->get_tenant_ctx_allocator(
        tenant_id_, ObCtxIds::DEFAULT_CTX_ID, 0/*numa_id*/);
    if (OB_ISNULL(ctx_allocator.ref_allocator())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      old_limit_ = ctx_allocator->get_limit();
      // A new fake tenant has no reusable chunk; the default context limit directly constrains the first diagnose alloc.
      if (0 != ctx_allocator->get_hold()) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        // 49 is only for the OOM memory dump; temporarily ignoring it does not change the real alloc-failure ret.
        MEMSET(&ignore_dump_action, 0, sizeof(ignore_dump_action));
        ignore_dump_action.sa_handler = SIG_IGN;
        if (0 != sigemptyset(&ignore_dump_action.sa_mask)
            || 0 != sigaction(MEMORY_DUMP_SIGNAL, &ignore_dump_action, &old_dump_action_)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          // Later init failures also restore the original handler in the dtor and do not affect other tests.
          has_saved_dump_action_ = true;
          ret = ctx_allocator->set_limit(oom_limit);
        }
      }
    }
  }
  return ret;
}

int ObScopedStudyDiagTenantAllocator::restore_limit()
{
  int ret = OB_SUCCESS;
  lib::ObTenantCtxAllocatorGuard ctx_allocator;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ctx_allocator = allocator_->get_tenant_ctx_allocator(
        tenant_id_, ObCtxIds::DEFAULT_CTX_ID, 0/*numa_id*/);
    if (OB_ISNULL(ctx_allocator.ref_allocator())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = ctx_allocator->set_limit(old_limit_);
    }
  }
  return ret;
}
#endif

class TestSSMarkSweepGC : public ObSimpleClusterTestBase
{
public:
  TestSSMarkSweepGC()
    : ObSimpleClusterTestBase("test_ss_mark_sweep_gc", "50G", "50G", "50G") {}

  // User tenant id, set once in setup_tenant and shared across tests.
  static uint64_t tenant_id_;

  // sys-level SQL proxy — routes to any tenant via the routing tenant_id arg.
  ObISQLClient &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  // SQL routing tenant for task/history tables of a user tenant.
  uint64_t meta_tid() const { return gen_meta_tenant_id(tenant_id_); }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  // Submit a MARK_SWEEP_GC DAG for the user tenant with the given allow_gc_round_id.
  //
  // DAG identity is (tenant_id, gc_type) — allow_gc_round_id is NOT part of it.
  // If a previous DAG (possibly with a lower allow_gc_round_id) is still in the
  // scheduler, a new submission returns OB_EAGAIN/OB_TASK_EXIST and would be
  // silenced, leaving rows invisible to the new round.  To avoid this, retry
  // until the scheduler slot is free and the DAG with the correct
  // allow_gc_round_id is successfully enqueued.
  void trigger_mark_sweep_dag(int64_t allow_gc_round_id)
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObSSGCExecDagInitParam param;
    param.tenant_id_         = tenant_id_;
    param.gc_type_           = ObSharedGCTaskType::MARK_SWEEP_GC;
    param.allow_gc_round_id_ = allow_gc_round_id;
    int ret = OB_SUCCESS;
    for (int i = 0; i < 120; ++i) {
      ret = ObSSGCUtil::create_and_add_dag(param);
      if (OB_SUCCESS == ret) {
        break;
      } else if (OB_EAGAIN == ret || OB_TASK_EXIST == ret) {
        usleep(500 * 1000);  // previous DAG still running; wait 500 ms
      } else {
        break;  // real error
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void wait_dag_scheduler_empty_(int64_t timeout_sec = 60)
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    ASSERT_NE(nullptr, scheduler);
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_sec * 1000000LL;
    while (!scheduler->is_empty() && ObTimeUtility::current_time() < deadline_us) {
      ob_usleep(100 * 1000);
    }
    ASSERT_TRUE(scheduler->is_empty())
        << "tenant dag scheduler is not empty after " << timeout_sec << "s";
  }

  // Spin-wait until __all_shared_gc_task has zero MARK_SWEEP_GC rows with
  // round_id <= allow_gc_round_id.  Polls every 500 ms.
  void wait_task_table_empty(int64_t allow_gc_round_id, int64_t timeout_sec = 60)
  {
    int ret = OB_SUCCESS;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_sec * 1000000LL;
    int64_t count = -1;
    ObSqlString sql;
    do {
      count = 0;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
          "SELECT COUNT(*) as cnt FROM oceanbase.__all_shared_gc_task "
          "WHERE tenant_id = %lu AND type = %ld AND round_id <= %ld",
          tenant_id_,
          static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
          allow_gc_round_id));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", count));
      }
      if (0 == count) {
        break;
      }
      usleep(500 * 1000);
    } while (ObTimeUtility::current_time() < deadline_us);
    ASSERT_EQ(0, count)
        << "task table still has " << count << " row(s) after "
        << timeout_sec << "s (allow_gc_round_id=" << allow_gc_round_id << ")";
  }

  // Build an ObSharedGCTask (MARK_SWEEP_GC) whose value serialises `ids`.
  // Memory is owned by `alloc`.
  static int build_mark_sweep_task(
      ObIAllocator &alloc,
      int64_t round_id,
      int64_t task_id,
      const ObIArray<MacroBlockId> &ids,
      ObSharedGCTask &task)
  {
    int ret = OB_SUCCESS;
    task.reset();
    task.key_ = ObSharedGCTaskKey(tenant_id_,
                                  ObSharedGCTaskType::MARK_SWEEP_GC,
                                  round_id, task_id);
    // extra_info must be non-empty: empty ObString round-trips through the
    // iterator → fill_dml_ path as a SQL NULL literal, but the column is
    // NOT NULL, so the history INSERT fails with OB_BAD_NULL_ERROR.
    task.extra_info_ = ObString::make_string("test");
    task.server_id_  = ObSharedGCTask::INVALID_SERVER_ID;
    ObSharedGCMacroIdList list;
    for (int64_t i = 0; OB_SUCC(ret) && i < ids.count(); ++i) {
      ret = list.add(ids.at(i));
    }
    if (OB_SUCC(ret)) {
      const int64_t buf_size = list.get_serialize_size();
      char *buf = static_cast<char *>(alloc.alloc(buf_size));
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", KR(ret), K(buf_size));
      } else if (OB_FAIL(list.serialize(buf, buf_size, pos))) {
        LOG_WARN("serialize macro id list failed", KR(ret));
      } else {
        task.value_.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }
    return ret;
  }

  // Build and write one real sstable macro id.  Normal MarkSweep tests should
  // prove the object-storage delete path, not just the task-table transition.
  static void write_sstable_id_(const ObTabletID &tablet_id,
                                const int64_t data_seq,
                                ObIArray<MacroBlockId> &ids)
  {
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        ObLSID(1001), tablet_id, data_seq, id));
    {
      share::ObTenantSwitchGuard guard;
      ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
      write_macro_block_(id);
      assert_block_exists_(id, true);
    }
    ASSERT_EQ(OB_SUCCESS, ids.push_back(id));
  }

  static int update_server_id_and_trace_id_with_retry_(
      const ObSharedGCTaskKey &key,
      const int64_t server_id,
      const ObString &trace_id,
      ObISQLClient &client)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < 120; ++i) {
      ret = ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
          key, server_id, trace_id, client);
      if (OB_NOT_MASTER == ret || OB_TIMEOUT == ret || OB_EAGAIN == ret) {
        ob_usleep(500 * 1000);
      } else {
        break;
      }
    }
    return ret;
  }

  int backdate_active_task_modified_(
      const ObSharedGCTaskKey &key,
      const int64_t age_us)
  {
    int ret = OB_SUCCESS;
    const int64_t old_ts_us = ObTimeUtility::current_time() - age_us;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE oceanbase.__all_shared_gc_task SET gmt_modified = usec_to_time(%ld) "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        old_ts_us,
        key.tenant_id_,
        static_cast<int64_t>(key.type_),
        key.round_id_,
        key.task_id_))) {
      LOG_WARN("fail to assign backdate sql", KR(ret), K(key));
    } else if (OB_FAIL(sql_proxy().write(meta_tid(), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to backdate active task", KR(ret), K(key), K(sql));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows when backdating active task",
          KR(ret), K(key), K(affected_rows));
    }
    return ret;
  }

  // Compute the MacroBlockId for a SHARED_MINI_* sstable macro using the
  // same opt fields the producer side would use. The id is byte-identical to
  // what batch_del_files will receive when GC processes the list.
  static int build_sstable_macro_id_(
      const blocksstable::ObStorageObjectType obj_type,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t data_seq,
      blocksstable::MacroBlockId &macro_id)
  {
    blocksstable::ObStorageObjectOpt opt;
    share::SCN reorg_min;
    reorg_min.set_min();
    opt.set_ss_share_object_opt(obj_type,
                                tablet_id.is_ls_inner_tablet(),
                                ls_id.id(),
                                tablet_id.id(),
                                0 /* op_id */,
                                data_seq,
                                0 /* cg_id */,
                                reorg_min.get_val_for_tx());
    return blocksstable::ObObjectManager::ss_get_object_id(opt, macro_id);
  }

  // Compute the MacroBlockId for a tablet sub_meta entry — byte-identical to
  // what FailGC's build_tablet_meta_block_ids probes via is_tiered_metadata_exist
  // and what batch_delete_blocks dispatches into batch_del_tiered_metadata.
  static int build_tablet_sub_meta_macro_id_(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const uint32_t op_id,
      const uint32_t data_seq,
      blocksstable::MacroBlockId &macro_id)
  {
    blocksstable::ObStorageObjectOpt opt;
    share::SCN reorg_min;
    reorg_min.set_min();
    // Build the TABLE location; OSS tests change the location after construction.
    opt.set_ss_tablet_sub_meta_opt(ls_id.id(),
                                   tablet_id.id(),
                                   op_id,
                                   data_seq,
                                   tablet_id.is_ls_inner_tablet(),
                                   reorg_min.get_val_for_tx(),
                                   false /* is_object_storage */);
    return blocksstable::ObObjectManager::ss_get_object_id(opt, macro_id);
  }

  // Synchronously write a 1MB payload for `macro_id` so is_exist returns true
  // immediately. Caller must already be inside a tenant switch to tenant_id_.
  static void write_macro_block_(const blocksstable::MacroBlockId &macro_id)
  {
    static char write_buf[1024 * 1024];
    memset(write_buf, 'a', sizeof(write_buf));
    blocksstable::ObStorageObjectWriteInfo write_info;
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = sizeof(write_buf);
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.write_strategy_ = blocksstable::ObStorageObjectWriteStrategy::WRITE_THROUGH;

    blocksstable::ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ASSERT_EQ(OB_SUCCESS,
              ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  }

  // Insert one tiered-metadata entry so is_tiered_metadata_exist returns true.
  // Caller must already be inside a tenant switch to tenant_id_.
  //
  // ObTieredMetadataObjectManager reads the tenant id from the
  // ObObjectStorageTenantGuard thread-local; without a guard it defaults to
  // OB_SERVER_TENANT_ID (500) — a virtual tenant — and the inner MTL_SWITCH
  // returns OB_OP_NOT_ALLOW (-4179).
  static void write_tablet_meta_entry_(const blocksstable::MacroBlockId &macro_id)
  {
    ObObjectStorageTenantGuard guard(
        tenant_id_,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id_) * 1000LL);
    static char meta_buf[64];
    memset(meta_buf, 'm', sizeof(meta_buf));
    int64_t write_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::write_tiered_metadata_to_table(
                  macro_id, false /*is_batch_write*/,
                  static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE),
                  meta_buf, sizeof(meta_buf), write_size));
  }

  // Assert object-storage block existence. Caller must be in tenant switch.
  static void assert_block_exists_(const blocksstable::MacroBlockId &macro_id,
                                   const bool expected_exist)
  {
    bool actual = false;
    ASSERT_EQ(OB_SUCCESS,
              ObRemoteIoAdapter::is_exist(
                  MTL_ID(), macro_id, GCONF.observer_id, actual));
    char id_buf[256] = {0};
    macro_id.to_string(id_buf, sizeof(id_buf));
    ASSERT_EQ(expected_exist, actual)
        << "macro_id=" << id_buf
        << " expected_exist=" << expected_exist;
  }

  // Assert tiered-metadata entry existence. Caller must be in tenant switch.
  static void assert_tablet_meta_exists_(const blocksstable::MacroBlockId &macro_id,
                                         const bool expected_exist)
  {
    ObObjectStorageTenantGuard guard(
        tenant_id_,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id_) * 1000LL);
    bool actual = false;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::is_tiered_metadata_exist(macro_id, actual));
    char id_buf[256] = {0};
    macro_id.to_string(id_buf, sizeof(id_buf));
    ASSERT_EQ(expected_exist, actual)
        << "tablet meta macro_id=" << id_buf
        << " expected_exist=" << expected_exist;
  }

  // Build raw watermarks for three nodes, covering IPv6 and existing watermark sentinels.
  static int build_study_nodes_(ObIArray<ObSSMarkSweepDiagStudyNode> &nodes)
  {
    int ret = OB_SUCCESS;
    ObSSMarkSweepDiagStudyNode node;
    nodes.reset();
    if (!node.server_.set_ip_addr("127.0.0.1", 2882)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      node.study_info_.set_to_min();
      if (OB_FAIL(nodes.push_back(node))) {
      } else if (!node.server_.set_ip_addr("127.0.0.1", 2883)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        node.study_info_.set_to_max();
        if (OB_FAIL(nodes.push_back(node))) {
        } else if (!node.server_.set_ip_addr("::1", 2884)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          node.study_info_.min_dep_meta_scn_.convert_for_tx(12345);
          node.study_info_.min_major_snapshot_ = 67890;
          ret = nodes.push_back(node);
        }
      }
    }
    return ret;
  }

  // Keep three raw node values for multiple LSes for fixed-file publish and cross-batch checks.
  static int build_study_raw_results_(
      const int64_t ls_count,
      const int64_t tablets_per_ls,
      ObSSGCStudyDiagInfo &diag_info)
  {
    int ret = OB_SUCCESS;
    ObArray<ObSSMarkSweepDiagStudyNode> nodes;
    ObSSGCTabletWithDelScnArray batch;
    ObSSGCTabletStudyResultArray results;
    ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
    if (ls_count <= 0 || tablets_per_ls <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(build_study_nodes_(nodes))) {
    } else {
      for (int64_t ls = 0; OB_SUCC(ret) && ls < ls_count; ++ls) {
        tablet.ls_id_ = ObLSID(1001 + ls);
        batch.reuse();
        for (int64_t i = 0; OB_SUCC(ret) && i < tablets_per_ls; ++i) {
          tablet.tablet_id_ = ObTabletID(800000 + i);
          ret = batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn()));
        }
        for (int64_t n = 0; OB_SUCC(ret) && n < nodes.count(); ++n) {
          results.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
            ret = results.push_back(
                ObSSGCTabletStudyResult(batch.at(i).gc_tablet_, nodes.at(n).study_info_));
          }
          if (OB_SUCC(ret)) {
            ret = diag_info.save_study_result(nodes.at(n).server_, batch, results);
          }
        }
      }
    }
    return ret;
  }

  // Compare each record to the pre-publish raw arrays; multi-LS cross-batch must not drop, duplicate, or merge node watermarks.
  static int check_study_raw_results_(
      const int64_t round_id,
      const ObSSGCStudyDiagInfo &diag_info,
      int64_t &batch_count)
  {
    int ret = OB_SUCCESS;
    const ObIArray<ObSSGCStudyDiagServerResult *> &servers = diag_info.get_study_results();
    ObSSMarkSweepDiagFileReader reader;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
    ObArray<ObString> records;
    ObSSMarkSweepDiagStudyResults snapshot;
    int64_t server_idx = 0;
    int64_t tablet_idx = 0;
    int64_t record_count = 0;
    batch_count = 0;
    if (round_id <= 0 || servers.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(reader.init(tenant_id_, round_id, ObMarkSweepGroupType::KEPT,
        ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/))) {
    } else {
      while (OB_SUCC(ret)) {
        records.reset();
        allocator.reuse();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
        } else {
          ++batch_count;
          for (int64_t i = 0; OB_SUCC(ret) && i < records.count(); ++i) {
            while (server_idx < servers.count()
                && tablet_idx >= servers.at(server_idx)->tablet_results_.count()) {
              const ObLSID &ls_id = servers.at(server_idx)->ls_id_;
              do {
                ++server_idx;
              } while (server_idx < servers.count() && servers.at(server_idx)->ls_id_ == ls_id);
              tablet_idx = 0;
            }
            if (server_idx >= servers.count()) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_study_record(
                records.at(i), snapshot))) {
            } else if (!(snapshot.tablet_
                == servers.at(server_idx)->tablet_results_.at(tablet_idx).gc_tablet_)) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              int64_t node_count = 0;
              for (int64_t n = server_idx; OB_SUCC(ret) && n < servers.count()
                   && servers.at(n)->ls_id_ == servers.at(server_idx)->ls_id_; ++n) {
                const ObSSGCTabletStudyInfo &expected =
                    servers.at(n)->tablet_results_.at(tablet_idx).study_info_;
                if (node_count >= snapshot.study_results_.count()) {
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  const ObSSMarkSweepDiagStudyNode &actual =
                      snapshot.study_results_.at(node_count++);
                  if (!(actual.server_ == servers.at(n)->server_)
                      || actual.study_info_.min_dep_meta_scn_ != expected.min_dep_meta_scn_
                      || actual.study_info_.min_major_snapshot_ != expected.min_major_snapshot_) {
                    ret = OB_ERR_UNEXPECTED;
                  }
                }
              }
              if (OB_SUCC(ret) && node_count != snapshot.study_results_.count()) {
                ret = OB_ERR_UNEXPECTED;
              }
            }
            ++record_count;
            ++tablet_idx;
          }
        }
      }
      if (OB_ITER_END == ret) {
        int64_t expected_count = 0;
        for (int64_t n = 0; n < servers.count(); ++n) {
          if (0 == n || servers.at(n)->ls_id_ != servers.at(n - 1)->ls_id_) {
            expected_count += servers.at(n)->tablet_results_.count();
          }
        }
        ret = expected_count == record_count && expected_count == reader.record_lens_.count()
            ? OB_SUCCESS
            : OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  // Compute the full-bucket length first, then serialize contiguously; the length index still stores one entry per tablet.
  static int build_study_bucket_(
      const int64_t bucket,
      const int64_t bucket_cnt,
      const int64_t tablet_count,
      ObIAllocator &allocator,
      ObString &data,
      ObIArray<int64_t> &record_lens)
  {
    int ret = OB_SUCCESS;
    ObArray<ObSSMarkSweepDiagStudyNode> nodes;
    ObArray<ObSSGCTablet> tablets;
    ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
    int64_t total_len = 0;
    int64_t record_len = 0;
    int64_t pos = 0;
    char *buf = nullptr;
    data.reset();
    record_lens.reset();
    if (bucket < 0 || bucket >= bucket_cnt || tablet_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(build_study_nodes_(nodes))) {
    } else {
      for (int64_t id = 800000; OB_SUCC(ret) && tablets.count() < tablet_count; ++id) {
        tablet.tablet_id_ = ObTabletID(id);
        if (static_cast<int64_t>(tablet.hash() % bucket_cnt) == bucket) {
          if (OB_FAIL(ObSSMarkSweepDiagMgr::get_study_record_size(tablet, nodes, record_len))) {
          } else if (OB_FAIL(tablets.push_back(tablet))) {
          } else if (OB_FAIL(record_lens.push_back(record_len))) {
          } else {
            total_len += record_len;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < tablets.count(); ++i) {
            ret = ObSSMarkSweepDiagMgr::encode_study_record(
                tablets.at(i), nodes, buf, total_len, pos);
          }
          if (OB_SUCC(ret) && pos != total_len) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        data.assign_ptr(buf, total_len);
      }
    }
    return ret;
  }

  // Read back all batches and verify full tablet identity, node identity, and both raw watermarks per record.
  static int check_study_bucket_(
      const int64_t round_id,
      const int64_t bucket,
      const int64_t bucket_cnt,
      const int64_t tablet_count,
      int64_t &batch_count)
  {
    int ret = OB_SUCCESS;
    ObSSMarkSweepDiagFileReader reader;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
    ObArray<ObString> records;
    ObArray<ObSSMarkSweepDiagStudyNode> expected_nodes;
    ObSSMarkSweepDiagStudyResults snapshot;
    ObSSGCTablet expected(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
    int64_t next_tablet_id = 800000;
    int64_t record_count = 0;
    batch_count = 0;
    if (round_id <= 0 || bucket < 0 || bucket >= bucket_cnt || tablet_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(build_study_nodes_(expected_nodes))) {
    } else if (OB_FAIL(reader.init(
        tenant_id_, round_id, ObMarkSweepGroupType::KEPT,
        ObSSMarkSweepDiagFileType::STUDY_RESULTS, bucket))) {
    } else if (reader.record_lens_.count() != tablet_count) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret)) {
        records.reset();
        allocator.reuse();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
        } else {
          ++batch_count;
          for (int64_t i = 0; OB_SUCC(ret) && i < records.count(); ++i) {
            do {
              expected.tablet_id_ = ObTabletID(next_tablet_id++);
            } while (static_cast<int64_t>(expected.hash() % bucket_cnt) != bucket);
            if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_study_record(records.at(i), snapshot))) {
            } else if (!(snapshot.tablet_ == expected)
                || snapshot.study_results_.count() != expected_nodes.count()) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              for (int64_t n = 0; OB_SUCC(ret) && n < expected_nodes.count(); ++n) {
                const ObSSMarkSweepDiagStudyNode &actual = snapshot.study_results_.at(n);
                const ObSSMarkSweepDiagStudyNode &expected_node = expected_nodes.at(n);
                if (!(actual.server_ == expected_node.server_)
                    || actual.study_info_.min_dep_meta_scn_
                        != expected_node.study_info_.min_dep_meta_scn_
                    || actual.study_info_.min_major_snapshot_
                        != expected_node.study_info_.min_major_snapshot_) {
                  ret = OB_ERR_UNEXPECTED;
                }
              }
              ++record_count;
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = record_count == tablet_count ? OB_SUCCESS : OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  // Query the given user tenant's diagnose objects as sys tenant, covering the virtual table's real directory listing.
  int get_study_diagnose_row_count_(
      const int64_t round_id,
      const char *predicate,
      int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    count = 0;
    if (round_id <= 0 || OB_ISNULL(predicate)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_virtual_shared_gc_diagnose "
        "WHERE tenant_id = %lu AND round_id = %ld %s", tenant_id_, round_id, predicate))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        sqlclient::ObMySQLResult *rows = nullptr;
        if (OB_FAIL(sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        } else if (OB_ISNULL(rows = result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(rows->next())) {
        } else {
          ret = rows->get_int("cnt", count);
        }
      }
    }
    return ret;
  }

  // Write and clean DEL/KEPT using the round marker.
  void check_diag_task_cleanup_(
      ObSSMarkSweepDiagMgr &dag_mgr,
      const ObSSGCTablet &tablet,
      const ObSharedGCTask &task)
  {
    volatile bool is_stopped = false;
    ObSSMarkSweepDiagRecordCleanupTask cleanup(is_stopped);
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
    ObSharedGCTask remaining_task;
    ObArray<SCN> scns;
    MacroBlockId del_id;
    MacroBlockId kept_id;
    dag_mgr.set_diag_enabled(true);
    ASSERT_EQ(OB_SUCCESS, scns.push_back(SCN::min_scn()));
    ASSERT_EQ(OB_SUCCESS, dag_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet, scns));
    ASSERT_EQ(OB_SUCCESS, dag_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT, tablet, scns));
    ASSERT_EQ(OB_SUCCESS, dag_mgr.complete());
    ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(task.key_.round_id_,
        ObMarkSweepGroupType::DEL, ObSSMarkSweepDiagFileType::TABLET_META_SCNS, 0, del_id));
    ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(task.key_.round_id_,
        ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::TABLET_META_SCNS, 0, kept_id));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(del_id, true/*expected_exist*/));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true/*expected_exist*/));
    ASSERT_EQ(OB_SUCCESS, cleanup.init(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_task_(task));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(del_id, false/*expected_exist*/));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, false/*expected_exist*/));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
        ObSharedGCTaskTableOperator::get_task(task.key_, allocator, sql_proxy(), remaining_task));
  }

  // Use a real Guard and RPC to verify the gate, thread-local collect binding, and publish path; do not set diagnose state directly.
  void check_study_diag_guard_(
      const int64_t round_id,
      const bool expect_enabled,
      const bool expect_registered,
      const int expected_prepare_ret)
  {
    const bool expect_capture = expect_enabled && OB_SUCCESS == expected_prepare_ret;
    const bool need_marker = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_6_3_0;
    const ObLSID LS_ID(900001);
    const ObAddr &server = get_curr_simple_server().get_addr();
    ObLSHandle ls_handle;
    ObLSService *ls_service = MTL(ObLSService *);
    obrpc::ObSSMarkSweepGCStudyProxy proxy;
    hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
    ObSSGCTabletWithDelScnArray batch;
    ObSSGCTabletStudyResultArray merged_results;
    ObSSGCTablet tablet(LS_ID, ObTabletID(800000), SCN::min_scn());
    ObSSMarkSweepDiagFileReader reader;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
    ObArray<ObString> records;
    ObSSMarkSweepDiagStudyResults snapshot;
    ObSSMarkSweepDiagMgr dag_mgr;
    ObSharedGCTask task;
    const ObSharedGCTaskKey key(
        tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, round_id, 0/*task_id*/);
    MacroBlockId macro_id;
    int64_t rpc_cost_us = 0;
    int64_t index = -1;
    int64_t row_count = 0;
    ASSERT_NE(nullptr, ls_service);
    ASSERT_EQ(OB_LS_NOT_EXIST,
        ls_service->get_ls(LS_ID, ls_handle, ObLSGetMod::SHARED_STORAGE_GC_MOD));
    ASSERT_NE(nullptr, GCTX.net_frame_);
    ASSERT_EQ(OB_SUCCESS, GCTX.net_frame_->get_proxy(proxy));
    ASSERT_EQ(OB_SUCCESS, tablet_index_map.create(10, ObMemAttr(tenant_id_, "SSGCStudyDiag")));
    ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
    tablet.tablet_id_ = ObTabletID(800001);
    ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
    ASSERT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
    {
      ObSSGCStudyDiagGuard diag_guard(tenant_id_, round_id);
      ObSSGCStudyDiagInfo *expected_info = expect_enabled ? &diag_guard.diag_info_ : nullptr;
      ASSERT_EQ(expect_enabled, diag_guard.is_enabled_);
      ASSERT_EQ(expected_info, ObSSGCStudyDiagInfo::get_cur_diag_info());
      if (OB_INVALID_ARGUMENT == expected_prepare_ret) {
        // Exercise a real prepare failure without changing the Study cluster-version gate.
        diag_guard.round_id_ = 0;
        DEFER(diag_guard.round_id_ = round_id);
        ASSERT_EQ(expected_prepare_ret, diag_guard.prepare());
      } else {
        ASSERT_EQ(expected_prepare_ret, diag_guard.prepare());
      }
      ASSERT_EQ(expect_registered ? OB_SUCCESS : OB_ENTRY_NOT_EXIST,
          ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
      ASSERT_EQ(OB_SUCCESS, dag_mgr.init(tenant_id_, round_id, 1));
      ASSERT_EQ(need_marker && !expect_registered ? OB_ENTRY_NOT_EXIST : OB_SUCCESS,
          dag_mgr.check_diag_task());
      ASSERT_EQ(need_marker && expect_registered, dag_mgr.is_diag_task_prepared_);
      ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::send_batch_rpc_request_(
          server, batch, proxy, tablet_index_map, merged_results, rpc_cost_us));
      ASSERT_EQ(batch.count(), merged_results.count());
      for (int64_t i = 0; i < batch.count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, tablet_index_map.get_refactored(batch.at(i).gc_tablet_, index));
        ASSERT_GE(index, 0);
        ASSERT_LT(index, merged_results.count());
        ASSERT_TRUE(batch.at(i).gc_tablet_ == merged_results.at(index).gc_tablet_);
        ASSERT_TRUE(merged_results.at(index).study_info_.min_dep_meta_scn_.is_max());
        ASSERT_EQ(INT64_MAX, merged_results.at(index).study_info_.min_major_snapshot_);
      }
      ASSERT_EQ(expected_prepare_ret, diag_guard.diag_info_.get_first_fail_ret());
      ASSERT_STREQ(OB_SUCCESS == expected_prepare_ret ? "none" : "prepare",
          diag_guard.diag_info_.get_first_fail_stage());
      ASSERT_EQ(expect_capture ? 1 : 0, diag_guard.diag_info_.get_study_results().count());
      if (expect_capture) {
        const ObSSGCStudyDiagServerResult &raw = *diag_guard.diag_info_.get_study_results().at(0);
        ASSERT_EQ(server, raw.server_);
        ASSERT_EQ(LS_ID, raw.ls_id_);
        ASSERT_EQ(batch.count(), raw.tablet_results_.count());
        for (int64_t i = 0; i < batch.count(); ++i) {
          ASSERT_TRUE(batch.at(i).gc_tablet_ == raw.tablet_results_.at(i).gc_tablet_);
          ASSERT_TRUE(raw.tablet_results_.at(i).study_info_.min_dep_meta_scn_.is_max());
          ASSERT_EQ(INT64_MAX, raw.tablet_results_.at(i).study_info_.min_major_snapshot_);
        }
      }
      ASSERT_EQ(expected_prepare_ret, diag_guard.finish(SCN::min_scn(), OB_SUCCESS));
      ASSERT_TRUE(diag_guard.is_finished_);
      ASSERT_TRUE(diag_guard.diag_info_.get_study_results().empty());
      ASSERT_EQ(OB_SUCCESS, diag_guard.diag_info_.get_first_fail_ret());
      ASSERT_STREQ("none", diag_guard.diag_info_.get_first_fail_stage());
    }
    ASSERT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
    ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(round_id,
        ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS,
        0/*bucket*/, macro_id));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(macro_id, expect_capture));
    if (expect_capture) {
      ASSERT_EQ(OB_SUCCESS, reader.init(tenant_id_, round_id, ObMarkSweepGroupType::KEPT,
          ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/));
      ASSERT_EQ(batch.count(), reader.record_lens_.count());
      ASSERT_EQ(OB_SUCCESS, reader.next_record_batch(allocator, records));
      ASSERT_EQ(batch.count(), records.count());
      for (int64_t i = 0; i < records.count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(records.at(i), snapshot));
        ASSERT_TRUE(batch.at(i).gc_tablet_ == snapshot.tablet_);
        ASSERT_EQ(1, snapshot.study_results_.count());
        ASSERT_EQ(server, snapshot.study_results_.at(0).server_);
        ASSERT_TRUE(snapshot.study_results_.at(0).study_info_.min_dep_meta_scn_.is_max());
        ASSERT_EQ(INT64_MAX, snapshot.study_results_.at(0).study_info_.min_major_snapshot_);
      }
      records.reuse();
      ASSERT_EQ(OB_ITER_END, reader.next_record_batch(allocator, records));
      ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
          round_id, "AND type = 'study_results'", row_count));
      ASSERT_EQ(batch.count(), row_count);
    }
    if (expect_registered) {
      reader.reset();
      ASSERT_NO_FATAL_FAILURE(check_diag_task_cleanup_(dag_mgr, tablet, task));
      ASSERT_NO_FATAL_FAILURE(assert_block_exists_(macro_id, false/*expected_exist*/));
    }
  }

  // Events land asynchronously; wait only for this case's round and check FAILED status plus appended-then-abandoned record count.
  int wait_study_diag_failed_event_(
      const int64_t round_id,
      const int64_t append_record_count,
      const int error_code)
  {
    int ret = OB_SUCCESS;
    int64_t count = 0;
    const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000;
    ObSqlString sql;
    if (round_id <= 0 || append_record_count <= 0 || OB_SUCCESS == error_code) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_server_event_history "
        "WHERE module = 'SHARED_GC' AND event = 'FINISH_MARK_SWEEP_STUDY_DIAG' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'round_id' AND value2 = '%ld' "
        "AND name5 = 'diag_first_fail_ret' AND value5 = '%d' "
        "AND name6 = 'publish_status' AND value6 = 'FAILED' "
        "AND extra_info LIKE '%%append_record_count=%ld,%%' "
        "AND extra_info LIKE '%%gc_meta_scn_valid=1,%%' "
        "AND extra_info LIKE '%%first_fail_stage=write,%%'",
        tenant_id_, round_id, error_code, append_record_count))) {
    } else {
      while (OB_SUCC(ret) && 0 == count && ObTimeUtility::current_time() < deadline) {
        SMART_VAR(ObMySQLProxy::MySQLResult, result) {
          sqlclient::ObMySQLResult *rows = nullptr;
          if (OB_FAIL(sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
          } else if (OB_ISNULL(rows = result.get_result())) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(rows->next())) {
          } else {
            ret = rows->get_int("cnt", count);
          }
        }
        if (OB_SUCC(ret) && 0 == count) {
          ob_usleep(100 * 1000);
        }
      }
      if (OB_SUCC(ret) && 0 == count) {
        ret = OB_TIMEOUT;
      }
    }
    return ret;
  }

  // Count rows in __all_shared_gc_task[_history] matching the predicate.
  int64_t count_rows_(const char *table_name, const char *where_clause)
  {
    int ret = OB_SUCCESS;
    int64_t count = -1;
    ObSqlString sql;
    EXPECT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT COUNT(*) as cnt FROM oceanbase.%s WHERE %s",
        table_name, where_clause));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      EXPECT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      EXPECT_NE(nullptr, result);
      EXPECT_EQ(OB_SUCCESS, result->next());
      EXPECT_EQ(OB_SUCCESS, result->get_int("cnt", count));
    }
    return count;
  }
};

uint64_t TestSSMarkSweepGC::tenant_id_ = OB_INVALID_TENANT_ID;

// ---------------------------------------------------------------------------
// observer_start: wait for sys LS leader before any tests run.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, observer_start)
{
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ObLSID(ObLSID::SYS_LS_ID), handle,
                                        ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  bool elected = false;
  for (int i = 0; i < 200; ++i) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (ObRole::LEADER == role) {
      elected = true;
      break;
    }
    ob_usleep(10 * 1000);
  }
  ASSERT_TRUE(elected) << "sys LS did not elect a leader within 2 s";
  LOG_INFO("observer ready, sys LS is leader");
}

// ---------------------------------------------------------------------------
// setup_tenant: create the user tenant "tt1" and store its id.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, setup_tenant)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    ret = create_tenant("tt1");
    if (OB_FAIL(ret)) {
      LOG_WARN("create_tenant failed, will retry", KR(ret), K(retry_cnt));
      ob_usleep(15 * 1000 * 1000);
    }
  } while (OB_FAIL(ret) && ++retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  ASSERT_TRUE(is_user_tenant(tenant_id_));
  LOG_INFO("user tenant created", K_(tenant_id));

  // Wait for the user-tenant SYS_LS to elect a leader. Without this, the first
  // DAG-driven SQL UPDATE on __all_shared_gc_task (which lives in the meta
  // tenant and routes through the user-tenant log handler) hits OB_NOT_MASTER
  // (-4016) and the WAL append times out (-4012). The fail_gc test suite
  // happens to warm up by running heavier sstable tests before its tablet_meta
  // case; mark_sweep has small_batch_e2e as its first DAG test and so needs an
  // explicit wait here.
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObLSService *ls_svr = MTL(ObLSService *);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ObLSID(ObLSID::SYS_LS_ID), handle,
                                          ObLSGetMod::STORAGE_MOD));
    ObLS *ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    bool elected = false;
    for (int i = 0; i < 1000; ++i) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS,
                ls->get_log_handler()->get_role(role, proposal_id));
      if (ObRole::LEADER == role) {
        elected = true;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_TRUE(elected) << "user tenant SYS LS did not elect a leader within 10 s";
  }
  LOG_INFO("user tenant ready", K_(tenant_id));
}

// ---------------------------------------------------------------------------
// gc_meta_scn_update_replaces_reservation: 通过真实 task 路径推进 gc_meta_scn。
// 新 reservation、旧 reservation 的删除及 gc_control 更新必须在同一事务内完成。
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, gc_meta_scn_update_replaces_reservation)
{
  const ObSharedMetaReservedType gc_type(ObSharedMetaReservedTypeEnum::GC);
  ObSharedGCControlInfo info;
  ObSharedMetaReservedMgr reserved_mgr;
  ObSharedMetaReservedOperator reserved_operator;
  ObArray<ObSharedMetaReservedInfo> reserved_infos;
  volatile bool is_stopped = false;
  ObSSMarkSweepGCGenTask task(is_stopped);
  bool is_acquired = false;
  bool need_check_old_executor = false;
  int64_t old_executor_server_id = 0;

  ASSERT_EQ(OB_SUCCESS, task.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, task.try_acquire_executor_(
      is_acquired, need_check_old_executor, old_executor_server_id));
  ASSERT_TRUE(is_acquired);
  ASSERT_FALSE(need_check_old_executor);
  ASSERT_GT(task.round_id_, 0);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, info));
  ASSERT_EQ(ObSharedGCControlStatus::DOING, info.status_);
  ASSERT_EQ(task.round_id_, info.gc_round_id_);
  ASSERT_EQ(OB_SUCCESS, reserved_mgr.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(sql_proxy(), 10L * 1000L * 1000L));
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(tenant_id_, gc_type, 0 /*limit_count*/, reserved_infos));

  // 选择尚未使用、且严格大于当前 gc_meta_scn 的 SCN，作为待替换的旧 reservation。
  SCN old_reserved_scn = SCN::scn_inc(info.gc_meta_scn_);
  for (int64_t i = 0; i < reserved_infos.count(); ++i) {
    if (old_reserved_scn <= reserved_infos.at(i).meta_scn_) {
      old_reserved_scn = SCN::scn_inc(reserved_infos.at(i).meta_scn_);
    }
  }
  const SCN new_safe_meta_scn = SCN::scn_inc(old_reserved_scn);
  ASSERT_TRUE(old_reserved_scn.is_valid());
  ASSERT_TRUE(new_safe_meta_scn.is_valid());

  ASSERT_EQ(OB_SUCCESS, reserved_mgr.create_reserved_scn(gc_type, old_reserved_scn));
  bool is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id_, task.round_id_, old_reserved_scn, is_updated));
  ASSERT_TRUE(is_updated);

  ASSERT_EQ(OB_SUCCESS, task.update_gc_meta_scn_(task.round_id_, new_safe_meta_scn));

  ObSharedGCControlInfo updated_info;
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, updated_info));
  ASSERT_EQ(new_safe_meta_scn, updated_info.gc_meta_scn_);

  reserved_infos.reset();
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(tenant_id_, gc_type, 0 /*limit_count*/, reserved_infos));
  bool has_old_reservation = false;
  bool has_new_reservation = false;
  for (int64_t i = 0; i < reserved_infos.count(); ++i) {
    has_old_reservation = has_old_reservation
        || (reserved_infos.at(i).meta_scn_ == old_reserved_scn);
    has_new_reservation = has_new_reservation
        || (reserved_infos.at(i).meta_scn_ == new_safe_meta_scn);
  }
  ASSERT_FALSE(has_old_reservation);
  ASSERT_TRUE(has_new_reservation);

  ASSERT_EQ(OB_SUCCESS, task.try_release_executor_());
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, updated_info));
  ASSERT_EQ(ObSharedGCControlStatus::IDLE, updated_info.status_);
}

// ---------------------------------------------------------------------------
// empty_table_noop: no rows → DAG scans an empty range and exits cleanly.
// Verify no spurious history rows appear.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, empty_table_noop)
{
  int ret = OB_SUCCESS;
  const int64_t allow_gc_round_id = 5000;

  ObArray<MacroBlockId> empty_ids;
  int64_t sstable_macro_cnt = 7;
  int64_t tablet_sub_meta_cnt = 9;
  ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
      tenant_id_, empty_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
  ASSERT_EQ(0, sstable_macro_cnt);
  ASSERT_EQ(0, tablet_sub_meta_cnt);

  trigger_mark_sweep_dag(allow_gc_round_id);
  // Give the scheduler 3 s to run the DAG (empty pre-scan exits almost instantly).
  usleep(3 * 1000 * 1000);

  int64_t history_count = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT COUNT(*) as cnt FROM oceanbase.__all_shared_gc_task_history "
      "WHERE tenant_id = %lu AND type = %ld AND round_id <= %ld",
      tenant_id_,
      static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
      allow_gc_round_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", history_count));
  }
  ASSERT_EQ(0, history_count) << "empty-table DAG wrote unexpected history rows";
}

// ---------------------------------------------------------------------------
// small_batch_e2e: 10 task rows with real sstable macro ids.
//   This exercises the full GC path, including object-storage deletion:
//     INSERT task rows → DAG pickup → UPDATE server/trace →
//     batch_del (written sstable id) → DELETE task + INSERT history.
//
//   Assertions: task table drains, history has NUM_TASKS rows, row 1 has
//   server_id and trace_id set.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, small_batch_e2e)
{
  int ret = OB_SUCCESS;
  const int64_t ROUND_ID = 6000;
  const int64_t NUM_TASKS = 10;
  const int64_t allow_gc_round_id = ROUND_ID + 1;

  ObArenaAllocator alloc;

  ObArray<MacroBlockId> written_ids;

  // --- Step 1: write 10 sstable macros and insert task rows for them ---
  for (int64_t t = 0; t < NUM_TASKS; ++t) {
    ObArray<MacroBlockId> macro_ids;
    write_sstable_id_(ObTabletID(300010), t, macro_ids);
    ASSERT_EQ(OB_SUCCESS, written_ids.push_back(macro_ids.at(0)));
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS,
              build_mark_sweep_task(alloc, ROUND_ID, t + 1, macro_ids, row));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));
  }

  // --- Step 2: trigger DAG and wait for task table to drain ---
  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  // --- Step 3: history table must have NUM_TASKS rows ---
  {
    int64_t history_count = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT COUNT(*) as cnt FROM oceanbase.__all_shared_gc_task_history "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld",
        tenant_id_,
        static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
        ROUND_ID));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy().read(res, meta_tid(), sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", history_count));
    }
    ASSERT_EQ(NUM_TASKS, history_count);

    // Spot-check row 1: server_id and trace_id must be set by the DAG.
    ObSharedGCTask h_row;
    ObSharedGCTaskKey k1(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                         ROUND_ID, 1);
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::get_history(k1, alloc,
                                                       sql_proxy(), h_row));
    ASSERT_NE(ObSharedGCTask::INVALID_SERVER_ID, h_row.server_id_);
    ASSERT_FALSE(h_row.trace_id_.empty());
  }

  // --- Step 4: every object listed by the tasks must be physically deleted ---
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < written_ids.count(); ++i) {
      assert_block_exists_(written_ids.at(i), false);
    }
  }

  // Cleanup: remove history rows.
  for (int64_t t = 0; t < NUM_TASKS; ++t) {
    ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                          ROUND_ID, t + 1);
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  }
}

// ---------------------------------------------------------------------------
// dirty_value_skip: a task row whose value cannot be deserialised as
// ObSharedGCMacroIdList is a per-row failure — the row stays in
// __all_shared_gc_task and will be retried by the next DAG invocation.
//
// Two rows:
//   row 1 (dirty)  — 16 bytes of 0xFF, won't parse as UNIS
//   row 2 (clean)  — valid written sstable macro id
// After the DAG:
//   row 1 must still be in __all_shared_gc_task (deserialization failure, not moved to history)
//   row 2 must be in __all_shared_gc_task_history (normal path)
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, dirty_value_skip)
{
  const int64_t ROUND_ID = 7000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;

  ObArenaAllocator alloc;

  const ObSharedGCTaskKey dirty_key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC, ROUND_ID, 1);
  const ObSharedGCTaskKey clean_key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC, ROUND_ID, 2);

  // Row 1: corrupted value.
  {
    ObSharedGCTask task;
    task.key_ = dirty_key;
    char *buf = static_cast<char *>(alloc.alloc(16));
    ASSERT_NE(nullptr, buf);
    memset(buf, 0xFF, 16);
    task.value_.assign_ptr(buf, 16);
    task.extra_info_ = ObString::make_string("test");
    task.server_id_  = ObSharedGCTask::INVALID_SERVER_ID;
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  MacroBlockId clean_macro_id;

  // Row 2: valid written sstable macro id.
  {
    ObSharedGCTask task;
    ObArray<MacroBlockId> macro_ids;
    write_sstable_id_(ObTabletID(300020), ROUND_ID, macro_ids);
    clean_macro_id = macro_ids.at(0);
    ASSERT_EQ(OB_SUCCESS,
              build_mark_sweep_task(alloc, ROUND_ID, 2, macro_ids, task));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(task, sql_proxy()));
  }

  trigger_mark_sweep_dag(allow_gc_round_id);

  // Wait for the clean row to appear in history (up to 60 s).
  {
    int ret = OB_ENTRY_NOT_EXIST;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + 60LL * 1000000LL;
    ObSharedGCTask h_row;
    do {
      ret = ObSharedGCTaskTableOperator::get_history(clean_key, alloc, sql_proxy(), h_row);
      if (OB_SUCCESS == ret) {
        break;
      }
      usleep(500 * 1000);
    } while (ObTimeUtility::current_time() < deadline_us);
    ASSERT_EQ(OB_SUCCESS, ret) << "clean row not in history after 60s";
  }

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(clean_macro_id, false);
  }

  // Dirty row must still be in the task table — deserialization failure is not moved to history.
  {
    ObSharedGCTask h_row;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
              ObSharedGCTaskTableOperator::get_history(dirty_key, alloc, sql_proxy(), h_row));
  }

  // Cleanup: dirty row stays in task table, remove it manually; clean row is in history.
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_task(dirty_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(clean_key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// claim_once: server_id/trace_id update is a claim. The first execute task owns the
// row; the second execute task must skip it instead of executing object deletion.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, claim_once)
{
  const int64_t ROUND_ID = 10000;
  ObArenaAllocator alloc;
  ObArray<MacroBlockId> macro_ids;
  MacroBlockId claim_id;
  ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
      blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
      ObLSID(1001), ObTabletID(300030), ROUND_ID, claim_id));
  ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(claim_id));
  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, 1, macro_ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  const ObString trace1 = ObString::make_string("trace-1");
  const ObString trace2 = ObString::make_string("trace-2");
  ASSERT_EQ(OB_SUCCESS, update_server_id_and_trace_id_with_retry_(
      row.key_, 1, trace1, sql_proxy()));
  // Same server re-claim with a new trace_id (next DAG retry) succeeds.
  ASSERT_EQ(OB_SUCCESS, update_server_id_and_trace_id_with_retry_(
      row.key_, 1, trace2, sql_proxy()));
  // 不同 live server 不能抢占该行；只有旧 owner 超时后才允许接管。
  ASSERT_EQ(OB_ENTRY_EXIST, update_server_id_and_trace_id_with_retry_(
      row.key_, 2, trace2, sql_proxy()));
  {
    char where[256];
    ASSERT_LT(0, snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld AND server_id = 1",
        tenant_id_, static_cast<int64_t>(row.key_.type_),
        row.key_.round_id_, row.key_.task_id_));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where));
  }

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_task(row.key_, sql_proxy()));
}

// ---------------------------------------------------------------------------
// lease_takeover_e2e: 模拟 execute DAG 在 Step B 后退出，active 行遗留旧
// owner。第一轮 DAG 看到 live owner 必须跳过；回拨 gmt_modified 让 lease
// 过期后，第二轮 DAG 应接管该行，完成对象删除并迁移到 history。
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, lease_takeover_e2e)
{
  const int64_t ROUND_ID = 16000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  ObArenaAllocator alloc;
  ObArray<MacroBlockId> macro_ids;
  MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
      blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
      ObLSID(1001), ObTabletID(300060), ROUND_ID, macro_id));
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    write_macro_block_(macro_id);
    assert_block_exists_(macro_id, true);
  }
  ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(macro_id));

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, macro_ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  const int64_t old_owner_server_id = static_cast<int64_t>(GCTX.get_server_id()) + 10000;
  const ObString old_trace = ObString::make_string("lease-old-owner");
  ASSERT_EQ(OB_SUCCESS, update_server_id_and_trace_id_with_retry_(
      row.key_, old_owner_server_id, old_trace, sql_proxy()));

  // 旧 owner 仍处于 lease 内，DAG 只能跳过，不能进入 Step C 删除对象。
  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_dag_scheduler_empty_();
  {
    char where[256];
    ASSERT_LT(0, snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        tenant_id_, static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
        ROUND_ID, TASK_ID));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where));
    ASSERT_EQ(0, count_rows_("__all_shared_gc_task_history", where));
  }
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(macro_id, true);
  }

  // 模拟旧 executor 长时间未推进。lease 过期后下一轮 DAG 可以接管并完成。
  ASSERT_EQ(OB_SUCCESS, backdate_active_task_modified_(
      row.key_, 24L * 60 * 60 * 1000 * 1000 /*age_us*/));
  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  {
    char where[256];
    ASSERT_LT(0, snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        tenant_id_, static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
        ROUND_ID, TASK_ID));
    ASSERT_EQ(0, count_rows_("__all_shared_gc_task", where));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task_history", where));
  }
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(macro_id, false);
  }

  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::delete_history(row.key_, sql_proxy()));
}

// ---------------------------------------------------------------------------
// sstable_macro_e2e: a MARK_SWEEP row whose macro-id list points to real
// sstable objects in shared storage.  Verifies the full path:
//   row.value (UNIS) -> ObSharedGCMacroIdList -> batch_delete_blocks ->
//   sstable_single -> ObRemoteIoAdapter::batch_del_files.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, sstable_macro_e2e)
{
  const int64_t ROUND_ID = 11000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  const int64_t NUM_MACROS = 4;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(300001);
  ObArenaAllocator alloc;

  // --- Write NUM_MACROS real sstable macros and collect their ids. ---
  ObArray<MacroBlockId> macro_ids;
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < NUM_MACROS; ++i) {
      MacroBlockId id;
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          LS_ID, TABLET_ID, /*data_seq=*/i, id));
      write_macro_block_(id);
      assert_block_exists_(id, true);
      ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(id));
    }
  }

  // --- Build and insert the MARK_SWEEP row carrying those ids. ---
  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, macro_ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  // --- Every macro listed in the row must now be gone. ---
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < NUM_MACROS; ++i) {
      assert_block_exists_(macro_ids.at(i), false);
    }
    // Missing objects still form a successfully processed deletion list.
    int64_t sstable_macro_cnt = -1;
    int64_t tablet_sub_meta_cnt = -1;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, macro_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(NUM_MACROS, sstable_macro_cnt);
    ASSERT_EQ(0, tablet_sub_meta_cnt);
  }

  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                        ROUND_ID, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// round_bound_e2e: allow_gc_round_id is the inclusive upper bound — rows whose
// round_id > allow_gc_round_id MUST NOT be touched.
//
// Insert one row at round_id=8000 (eligible) and one at round_id=9000 (not).
// First use allow_gc_round_id = 7999 and verify neither row is touched.
// Then trigger DAG with allow_gc_round_id = 8000:
//   * row 8000/1 → drains into history.
//   * row 9000/1 → stays in __all_shared_gc_task untouched.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, round_bound_e2e)
{
  const int64_t IN_ROUND = 8000;
  const int64_t OUT_ROUND = 9000;
  const int64_t allow_gc_round_id = IN_ROUND;
  const int64_t TASK_ID = 1;
  ObArenaAllocator alloc;
  MacroBlockId in_macro_id;
  MacroBlockId out_macro_id;

  // Eligible row (round_id <= allow).
  {
    ObArray<MacroBlockId> macro_ids;
    write_sstable_id_(ObTabletID(300040), IN_ROUND, macro_ids);
    in_macro_id = macro_ids.at(0);
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS,
              build_mark_sweep_task(alloc, IN_ROUND, TASK_ID, macro_ids, row));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));
  }
  // Out-of-window row (round_id > allow) — must not be GC'd.
  {
    ObArray<MacroBlockId> macro_ids;
    write_sstable_id_(ObTabletID(300041), OUT_ROUND, macro_ids);
    out_macro_id = macro_ids.at(0);
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS,
              build_mark_sweep_task(alloc, OUT_ROUND, TASK_ID, macro_ids, row));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));
  }

  // The minimum active round is above the watermark: do not plan any deletion.
  ASSERT_NO_FATAL_FAILURE(trigger_mark_sweep_dag(IN_ROUND - 1));
  ASSERT_NO_FATAL_FAILURE(wait_dag_scheduler_empty_(60));
  ObSharedGCTask pending_row;
  ObSharedGCTaskKey pending_key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
      IN_ROUND, TASK_ID);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(
      pending_key, alloc, sql_proxy(), pending_row));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCTaskTableOperator::get_history(
      pending_key, alloc, sql_proxy(), pending_row));
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(in_macro_id, true);
    assert_block_exists_(out_macro_id, true);
  }

  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  // In-window row reached history.
  ObSharedGCTaskKey in_key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                           IN_ROUND, TASK_ID);
  ObSharedGCTask h_row;
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::get_history(in_key, alloc,
                                                     sql_proxy(), h_row));

  // Out-of-window row still in task table, NOT in history.
  {
    char where[256] = {0};
    snprintf(where, sizeof(where),
        "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
        tenant_id_,
        static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
        OUT_ROUND, TASK_ID);
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where));
    ASSERT_EQ(0, count_rows_("__all_shared_gc_task_history", where));
  }

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    assert_block_exists_(in_macro_id, false);
    assert_block_exists_(out_macro_id, true);
  }

  // Cleanup.
  ObSharedGCTaskKey out_key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                            OUT_ROUND, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(in_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_task(out_key, sql_proxy()));
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObArray<MacroBlockId> out_ids;
    ASSERT_EQ(OB_SUCCESS, out_ids.push_back(out_macro_id));
    int64_t sstable_macro_cnt = 0;
    int64_t tablet_sub_meta_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, out_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(1, sstable_macro_cnt);
    ASSERT_EQ(0, tablet_sub_meta_cnt);
  }
}

// ---------------------------------------------------------------------------
// slice_chain_e2e: insert > _shared_gc_task_scan_batch_size rows so the
// MarkSweep execute task chain has to generate at least one follow-up slice
// via generate_next_task.  Defaults to scan_batch_size=100; 250 rows give us
// 3 chained slices.
//
// Each row points to a real object-storage block, so the chained slices must
// both drain the task table and physically delete every listed block.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, slice_chain_e2e)
{
  const int64_t ROUND_ID = 12000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t NUM_TASKS = 250;
  ObArenaAllocator alloc;
  ObArray<MacroBlockId> written_ids;

  for (int64_t t = 0; t < NUM_TASKS; ++t) {
    ObArray<MacroBlockId> macro_ids;
    write_sstable_id_(ObTabletID(300050), t, macro_ids);
    ASSERT_EQ(OB_SUCCESS, written_ids.push_back(macro_ids.at(0)));
    ObSharedGCTask row;
    ASSERT_EQ(OB_SUCCESS,
              build_mark_sweep_task(alloc, ROUND_ID, t + 1, macro_ids, row));
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));
  }

  trigger_mark_sweep_dag(allow_gc_round_id);
  // 250 rows × 2 inner-table SQLs per row across 3 chained slices is heavier
  // than small_batch_e2e — give it more headroom.
  wait_task_table_empty(allow_gc_round_id, /*timeout_sec=*/120);

  // History should carry exactly NUM_TASKS rows for this round.
  char where[256] = {0};
  snprintf(where, sizeof(where),
      "tenant_id = %lu AND type = %ld AND round_id = %ld",
      tenant_id_,
      static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
      ROUND_ID);
  ASSERT_EQ(NUM_TASKS, count_rows_("__all_shared_gc_task_history", where));

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < written_ids.count(); ++i) {
      assert_block_exists_(written_ids.at(i), false);
    }
  }

  for (int64_t t = 0; t < NUM_TASKS; ++t) {
    ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                          ROUND_ID, t + 1);
    ASSERT_EQ(OB_SUCCESS,
              ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  }
}

// ---------------------------------------------------------------------------
// sys_tablet_meta_double_route: verify the metadata manager deletes both copies
// for a sys-tenant sub-meta ID when LS checkpoint is enabled.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, sys_tablet_meta_double_route)
{
  share::ObTenantSwitchGuard tenant_guard;
  const int64_t ROUND_ID = 13500;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(300099);
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  uint64_t actual_data_version = 0;
  MacroBlockId in_table_id;
  MacroBlockId remote_id;
  ObTieredMetadataStore store;
  const char *data = "gc_sys_double_delete";
  const ObString meta_value(STRLEN(data), data);
  ObObjectStorageTenantGuard object_storage_guard(
      OB_SYS_TENANT_ID, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ObArray<MacroBlockId> macro_ids;
  ObArenaAllocator alloc;
  ObArenaAllocator dag_allocator("MSSysRetryDag");
  ObSharedGCTask row;
  ObSSGCExecDag dag;
  ObSSGCExecDagInitParam dag_param;
  ObMarkSweepExecuteTask *execute_task = nullptr;
  const ObSSGCTaskIdRange task_range(TASK_ID, TASK_ID);
  ScopedNamedEvent history_insert_error;
  const ObSharedGCTaskKey key(
      OB_SYS_TENANT_ID, ObSharedGCTaskType::MARK_SWEEP_GC, ROUND_ID, TASK_ID);
  const ObSharedGCTaskKey in_table_stat_key(
      OB_SYS_TENANT_ID,
      ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT,
      ROUND_ID,
      TASK_ID);
  const ObSharedGCTaskKey object_storage_stat_key(
      OB_SYS_TENANT_ID,
      ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT,
      ROUND_ID,
      TASK_ID);
  ObSharedGCTask original_history;
  ObSharedGCTask in_table_stat;
  ObSharedGCTask object_storage_stat;
  bool table_exist = true;
  int cleanup_ret = OB_SUCCESS;
  ASSERT_TRUE(share::ObUpgradeChecker::check_data_version_exist(DATA_VERSION_4_6_3_0));
  DEFER({
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  cleanup_ret = ObSharedGCTaskTableOperator::delete_task(key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(in_table_stat_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  cleanup_ret = ObSharedGCTaskTableOperator::delete_history(object_storage_stat_key, sql_proxy());
  ASSERT_TRUE(OB_SUCCESS == cleanup_ret || OB_ENTRY_NOT_EXIST == cleanup_ret);
  ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
      ls_id, tablet_id, 23/*op_id*/, 1/*data_seq*/, in_table_id));
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, &sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      in_table_id,
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE),
      meta_value));
  remote_id = in_table_id;
  remote_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  write_macro_block_(remote_id);
  assert_block_exists_(remote_id, true);
  ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(in_table_id));
  ASSERT_EQ(OB_SUCCESS, build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, macro_ids, row));
  // 通用构造函数默认生成用户租户任务，SYS DAG 只扫描 SYS 租户任务。
  row.key_.tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_3_0, actual_data_version);
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  dag_param.tenant_id_ = OB_SYS_TENANT_ID;
  dag_param.gc_type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
  dag_param.allow_gc_round_id_ = allow_gc_round_id;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&dag_param));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(execute_task));
  ASSERT_NE(nullptr, execute_task);
  ASSERT_EQ(OB_SUCCESS, execute_task->init(
      OB_SYS_TENANT_ID, ROUND_ID, task_range, task_range, allow_gc_round_id));
  ASSERT_EQ(OB_SUCCESS, history_insert_error.init(
      "EN_SS_MARK_SWEEP_GC_HISTORY_INSERT_ERROR", OB_TIMEOUT, 1));
  ASSERT_EQ(OB_SUCCESS, execute_task->process());
  history_insert_error.reset();
  ASSERT_EQ(1, dag.cost_stat_.failed_row_count_);
  ASSERT_EQ(1, dag.cost_stat_.succ_row_count_);
  // The failed history transaction retries the same logical tablet ID only once.
  ASSERT_EQ(0, dag.cost_stat_.succ_sstable_macro_cnt_);
  ASSERT_EQ(1, dag.cost_stat_.succ_tablet_sub_meta_cnt_);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(in_table_id, table_exist));
  ASSERT_FALSE(table_exist);
  assert_block_exists_(remote_id, false);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      key, alloc, sql_proxy(), original_history));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      in_table_stat_key, alloc, sql_proxy(), in_table_stat));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
      object_storage_stat_key, alloc, sql_proxy(), object_storage_stat));
  ASSERT_EQ(ObString::make_string("1"), in_table_stat.value_);
  ASSERT_EQ(ObString::make_string("1"), object_storage_stat.value_);
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(in_table_stat_key, sql_proxy()));
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::delete_history(object_storage_stat_key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// tablet_meta_in_list_e2e: a MARK_SWEEP row whose macro-id list contains
// tablet_meta ids.  batch_delete_blocks must dispatch them into
// tablet_meta_blocks → ObTieredMetadataObjectManager::batch_del_tiered_metadata
// instead of batch_del_files (which would reject the wrong storage type).
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, tablet_meta_in_list_e2e)
{
  const int64_t ROUND_ID = 13000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  const int64_t NUM_ENTRIES = 3;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(300002);
  const uint32_t OP_ID = 17;
  ObArenaAllocator alloc;

  // --- Pre-write tablet meta entries and collect their ids. ---
  ObArray<MacroBlockId> macro_ids;
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < NUM_ENTRIES; ++i) {
      MacroBlockId id;
      ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
          LS_ID, TABLET_ID, OP_ID, /*data_seq=*/static_cast<uint32_t>(i), id));
      write_tablet_meta_entry_(id);
      assert_tablet_meta_exists_(id, true);
      ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(id));
    }
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, macro_ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int64_t i = 0; i < NUM_ENTRIES; ++i) {
      assert_tablet_meta_exists_(macro_ids.at(i), false);
    }
    int64_t sstable_macro_cnt = -1;
    int64_t tablet_sub_meta_cnt = -1;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, macro_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(0, sstable_macro_cnt);
    ASSERT_EQ(NUM_ENTRIES, tablet_sub_meta_cnt);
  }

  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                        ROUND_ID, TASK_ID);
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// mixed_id_e2e: one row contains SSTable IDs and TABLE/OSS sub-meta IDs.
// Both sub-meta locations use the metadata manager.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, mixed_id_e2e)
{
  const int64_t ROUND_ID = 14000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  const ObLSID LS_ID(1001);
  const ObTabletID TABLET_ID(300003);
  const uint32_t OP_ID = 19;
  ObArenaAllocator alloc;

  // Interleave both metadata locations with SSTable blocks in the same task.
  ObArray<MacroBlockId> macro_ids;
  MacroBlockId sst_ids[2];
  MacroBlockId tm_ids[2];
  MacroBlockId remote_meta_id;
  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          LS_ID, TABLET_ID, /*data_seq=*/i, sst_ids[i]));
      write_macro_block_(sst_ids[i]);
      assert_block_exists_(sst_ids[i], true);

      ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
          LS_ID, TABLET_ID, OP_ID, /*data_seq=*/static_cast<uint32_t>(i), tm_ids[i]));
      write_tablet_meta_entry_(tm_ids[i]);
      assert_tablet_meta_exists_(tm_ids[i], true);
    }
    ASSERT_EQ(OB_SUCCESS, build_tablet_sub_meta_macro_id_(
        LS_ID, TABLET_ID, OP_ID, 2/*data_seq*/, remote_meta_id));
    remote_meta_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    ASSERT_NO_FATAL_FAILURE(write_macro_block_(remote_meta_id));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(remote_meta_id, true));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(sst_ids[0]));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(tm_ids[0]));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(remote_meta_id));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(sst_ids[1]));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(tm_ids[1]));
  }

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, macro_ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_mark_sweep_dag(allow_gc_round_id);
  wait_task_table_empty(allow_gc_round_id);

  {
    share::ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    for (int i = 0; i < 2; ++i) {
      assert_block_exists_(sst_ids[i], false);
      assert_tablet_meta_exists_(tm_ids[i], false);
    }
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(remote_meta_id, false));
    int64_t sstable_macro_cnt = -1;
    int64_t tablet_sub_meta_cnt = -1;
    ASSERT_EQ(OB_SUCCESS, ObSSGCUtil::batch_delete_blocks(
        tenant_id_, macro_ids, sstable_macro_cnt, tablet_sub_meta_cnt));
    ASSERT_EQ(2, sstable_macro_cnt);
    ASSERT_EQ(3, tablet_sub_meta_cnt);
  }

  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                        ROUND_ID, TASK_ID);
  ObSharedGCTask history_row;
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::get_history(key, alloc, sql_proxy(), history_row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_history(key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// unsupported_type_skip: a row containing a MacroBlockId whose
// storage_object_type is neither sstable-family nor tablet-meta-family.
// ObSSGCUtil::batch_delete_blocks returns OB_NOT_SUPPORTED → process_row_
// surfaces it as a per-row failure → the row stays in __all_shared_gc_task
// and is NOT moved to history.  This guarantees we never silently drop a row
// whose blocks we don't know how to delete.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, unsupported_type_skip)
{
  const int64_t ROUND_ID = 15000;
  const int64_t allow_gc_round_id = ROUND_ID + 1;
  const int64_t TASK_ID = 1;
  ObArenaAllocator alloc;

  // Build a MacroBlockId whose storage_object_type falls outside both the
  // sstable-family and tablet_meta-family branches in batch_delete_blocks.
  // PRIVATE_DATA_MACRO is owned by the local cache path, never by the shared
  // GC pipeline.
  MacroBlockId unknown_id;
  unknown_id.set_version_v1();
  unknown_id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::PRIVATE_DATA_MACRO));
  unknown_id.set_second_id(1);
  unknown_id.set_reorganization_scn(1);

  ObArray<MacroBlockId> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(unknown_id));
  int64_t sstable_macro_cnt = 7;
  int64_t tablet_sub_meta_cnt = 9;
  ASSERT_EQ(OB_NOT_SUPPORTED, ObSSGCUtil::batch_delete_blocks(
      tenant_id_, ids, sstable_macro_cnt, tablet_sub_meta_cnt));
  ASSERT_EQ(0, sstable_macro_cnt);
  ASSERT_EQ(0, tablet_sub_meta_cnt);

  ObSharedGCTask row;
  ASSERT_EQ(OB_SUCCESS,
            build_mark_sweep_task(alloc, ROUND_ID, TASK_ID, ids, row));
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::insert_task(row, sql_proxy()));

  trigger_mark_sweep_dag(allow_gc_round_id);
  // Give the DAG enough time to scan and fail this single row.
  usleep(5 * 1000 * 1000);

  // Row must still be in the task table, NOT in history.
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                        ROUND_ID, TASK_ID);
  ObSharedGCTask h_row;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCTaskTableOperator::get_history(key, alloc,
                                                     sql_proxy(), h_row));

  char where[256] = {0};
  snprintf(where, sizeof(where),
      "tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
      tenant_id_,
      static_cast<int64_t>(ObSharedGCTaskType::MARK_SWEEP_GC),
      ROUND_ID, TASK_ID);
  ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where));

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCTaskTableOperator::delete_task(key, sql_proxy()));
}

// ---------------------------------------------------------------------------
// Read every bucket file of one (group, type) for a round and aggregate the
// macro records into out (keyed implicitly by appearance). Only existing bucket
// files are read; tablets are spread across buckets by hash so the per-tablet
// record may live in any bucket.
// ---------------------------------------------------------------------------
static int read_all_buckets_macros(
    const uint64_t tenant_id, const int64_t round_id, const int64_t bucket_cnt,
    const ObMarkSweepGroupType group, ObIAllocator &allocator,
    ObIArray<ObSSMarkSweepDiagTabletMacros> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t b = 0; OB_SUCC(ret) && (b < bucket_cnt); ++b) {
    ObSSMarkSweepDiagFileReader reader;
    if (OB_FAIL(reader.init(tenant_id, round_id, group,
        ObSSMarkSweepDiagFileType::MACROS, b))) {
      // bucket file may not exist (tablets spread by hash); treat as empty.
      ret = OB_SUCCESS;
    } else {
      ObArray<ObString> records;
      while (OB_SUCC(ret)) {
        records.reset();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
          break;
        }
        for (int64_t i = 0; OB_SUCC(ret) && (i < records.count()); ++i) {
          ObSSMarkSweepDiagTabletMacros rec;
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_macros_record(records.at(i), rec))) {
          } else if (OB_FAIL(out.push_back(rec))) {}
        }
      }
    }
  }
  return ret;
}

static int read_all_buckets_meta_scns(
    const uint64_t tenant_id, const int64_t round_id, const int64_t bucket_cnt,
    const ObMarkSweepGroupType group, ObIAllocator &allocator,
    ObIArray<ObSSMarkSweepTabletMetaScns> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t b = 0; OB_SUCC(ret) && (b < bucket_cnt); ++b) {
    ObSSMarkSweepDiagFileReader reader;
    if (OB_FAIL(reader.init(tenant_id, round_id, group,
        ObSSMarkSweepDiagFileType::TABLET_META_SCNS, b))) {
      // bucket file may not exist (tablets spread by hash); treat as empty.
      ret = OB_SUCCESS;
    } else {
      ObArray<ObString> records;
      while (OB_SUCC(ret)) {
        records.reset();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
          break;
        }
        for (int64_t i = 0; OB_SUCC(ret) && (i < records.count()); ++i) {
          ObSSMarkSweepTabletMetaScns rec;
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_meta_scns_record(records.at(i), rec))) {
          } else if (OB_FAIL(out.push_back(rec))) {}
        }
      }
    }
  }
  return ret;
}

static int read_all_buckets_lob_meta_kept(
    const uint64_t tenant_id,
    const int64_t round_id,
    const int64_t bucket_cnt,
    ObIAllocator &allocator,
    ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  macro_ids.reset();
  for (int64_t bucket = 0; OB_SUCC(ret) && bucket < bucket_cnt; ++bucket) {
    ObSSMarkSweepDiagFileReader reader;
    if (OB_FAIL(reader.init(tenant_id, round_id, ObMarkSweepGroupType::DEL,
        ObSSMarkSweepDiagFileType::LOB_META_KEPT, bucket))) {
      ret = OB_SUCCESS;
    } else {
      ObArray<ObString> records;
      while (OB_SUCC(ret)) {
        records.reset();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < records.count(); ++i) {
          ObSSLobMetaGCDiagRows rows;
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_lob_meta_kept_record(records.at(i), rows))) {
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < rows.macro_ids_.count(); ++j) {
              if (OB_FAIL(macro_ids.push_back(rows.macro_ids_.at(j)))) {
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

static int read_all_buckets_lob_ctrl(
    const uint64_t tenant_id,
    const int64_t round_id,
    const int64_t bucket_cnt,
    const ObSSMarkSweepDiagFileType type,
    ObIAllocator &allocator,
    ObIArray<ObSSOuttableLobCtrlCandidate> &out_rows)
{
  int ret = OB_SUCCESS;
  out_rows.reset();
  for (int64_t bucket = 0; OB_SUCC(ret) && bucket < bucket_cnt; ++bucket) {
    ObSSMarkSweepDiagFileReader reader;
    if (OB_FAIL(reader.init(tenant_id, round_id, ObMarkSweepGroupType::DEL, type, bucket))) {
      ret = OB_SUCCESS;
    } else {
      ObArray<ObString> records;
      while (OB_SUCC(ret)) {
        records.reset();
        if (OB_FAIL(reader.next_record_batch(allocator, records))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < records.count(); ++i) {
          ObSSLobCtrlGCDiagRows rows;
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_lob_ctrl_record(records.at(i), rows))) {
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < rows.rows_.count(); ++j) {
              const ObSSOuttableLobCtrlCandidate &row = rows.rows_.at(j);
              const int64_t expected_bucket = static_cast<int64_t>(
                  row.macro_id_.hash() % static_cast<uint64_t>(bucket_cnt));
              if (OB_UNLIKELY(expected_bucket != bucket)) {
                ret = OB_ERR_UNEXPECTED;
              } else if (OB_FAIL(out_rows.push_back(row))) {
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

static int create_lob_ctrl_tmp_file(
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const ObIArray<ObSSOuttableLobCtrlCandidate> &rows,
    ObMarkSweepTmpFileMgr &file_mgr,
    ObSSLobCtrlGCScanFile &file)
{
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;
  ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  if (OB_UNLIKELY(rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id))) {
  } else if (OB_FAIL(writer.open(file_buf_size, 0, tenant_id, dir_id))) {
  } else if (OB_FAIL(file_mgr.acquire_file(
      tenant_id, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      ret = writer.write_item(rows.at(i));
    }
    if (OB_SUCC(ret) && OB_FAIL(writer.sync())) {
    } else if (OB_SUCC(ret)) {
      file.sample_item_ = writer.get_sample_item();
      file.item_count_ = rows.count();
    }
  }
  return ret;
}

static const ObSSOuttableLobCtrlCandidate *find_lob_ctrl_row(
    const ObIArray<ObSSOuttableLobCtrlCandidate> &rows,
    const MacroBlockId &macro_id)
{
  const ObSSOuttableLobCtrlCandidate *found = nullptr;
  for (int64_t i = 0; OB_ISNULL(found) && i < rows.count(); ++i) {
    if (rows.at(i).macro_id_ == macro_id) {
      found = &rows.at(i);
    }
  }
  return found;
}

static void check_lob_ctrl_rows(
    const ObIArray<ObSSOuttableLobCtrlCandidate> &expected_rows,
    const ObIArray<ObSSOuttableLobCtrlCandidate> &actual_rows)
{
  ASSERT_EQ(expected_rows.count(), actual_rows.count());
  for (int64_t i = 0; i < expected_rows.count(); ++i) {
    const ObSSOuttableLobCtrlCandidate &expected = expected_rows.at(i);
    const ObSSOuttableLobCtrlCandidate *actual = find_lob_ctrl_row(actual_rows, expected.macro_id_);
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(expected.macro_id_, actual->macro_id_);
    EXPECT_EQ(expected.hash_, actual->hash_);
    EXPECT_EQ(expected.mark_row_scn_, actual->mark_row_scn_);
  }
}

template <typename T>
static const T *find_tablet_rec(const ObIArray<T> &recs, const ObSSGCTablet &tablet)
{
  const T *found = nullptr;
  for (int64_t i = 0; (nullptr == found) && (i < recs.count()); ++i) {
    if (recs.at(i).tablet_ == tablet) {
      found = &recs.at(i);
    }
  }
  return found;
}

// ---------------------------------------------------------------------------
// diagnose_manager_round_trip: write DEL/KEPT macros + tablet_meta_scns for several
// tablets via the round-level ObSSMarkSweepDiagMgr (small bucket_cnt so multiple
// tablets collide into the same bucket), then read every bucket back and assert the
// decoded content matches. Covers: empty group skip, a large tablet whose macro list
// spans the data region across multiple read IO chunks, multiple tablets aggregated
// (some into one bucket), Tailer+Index parsing, and stable bucket mapping.
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, diagnose_manager_round_trip)
{
  const int64_t ROUND_ID = 20000;
  const int64_t BUCKET_CNT = 4; // small -> force several tablets to share a bucket
  const ObLSID LS_ID(1001);

  // tablet A: many del macros (big), 1 kept macro; del/kept meta scns.
  // tablet B: only kept macros (del group empty for B).
  // tablet C: del macros + del meta scns (extra tablet to exercise aggregation).
  const ObTabletID TABLET_A(400001);
  const ObTabletID TABLET_B(400002);
  const ObTabletID TABLET_C(400003);
  const int64_t A_DEL_CNT = 2000; // 2000 * 32B ~ 64KB > a single small read chunk
  const int64_t A_KEPT_CNT = 1;
  const int64_t B_KEPT_CNT = 3;
  const int64_t C_DEL_CNT = 5;

  ObSSGCTablet tablet_a(LS_ID, TABLET_A, share::SCN::min_scn());
  ObSSGCTablet tablet_b(LS_ID, TABLET_B, share::SCN::min_scn());
  ObSSGCTablet tablet_c(LS_ID, TABLET_C, share::SCN::min_scn());
  ObArray<MacroBlockId> a_del_ids;
  ObArray<MacroBlockId> a_kept_ids;
  ObArray<MacroBlockId> b_kept_ids;
  ObArray<MacroBlockId> c_del_ids;
  ObArray<SCN> a_del_scns;
  ObArray<SCN> a_kept_scns;
  ObArray<SCN> c_del_scns;

  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));

  for (int64_t i = 0; i < A_DEL_CNT; ++i) {
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        LS_ID, TABLET_A, /*data_seq=*/i, id));
    ASSERT_EQ(OB_SUCCESS, a_del_ids.push_back(id));
  }
  for (int64_t i = 0; i < A_KEPT_CNT; ++i) {
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        LS_ID, TABLET_A, /*data_seq=*/100000 + i, id));
    ASSERT_EQ(OB_SUCCESS, a_kept_ids.push_back(id));
  }
  for (int64_t i = 0; i < B_KEPT_CNT; ++i) {
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        LS_ID, TABLET_B, /*data_seq=*/i, id));
    ASSERT_EQ(OB_SUCCESS, b_kept_ids.push_back(id));
  }
  for (int64_t i = 0; i < C_DEL_CNT; ++i) {
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        LS_ID, TABLET_C, /*data_seq=*/i, id));
    ASSERT_EQ(OB_SUCCESS, c_del_ids.push_back(id));
  }
  {
    SCN s1; s1.convert_for_tx(1000);
    SCN s2; s2.convert_for_tx(2000);
    SCN s3; s3.convert_for_tx(3000);
    SCN s4; s4.convert_for_tx(4000);
    ASSERT_EQ(OB_SUCCESS, a_del_scns.push_back(s1));
    ASSERT_EQ(OB_SUCCESS, a_del_scns.push_back(s2));
    ASSERT_EQ(OB_SUCCESS, a_kept_scns.push_back(s3));
    ASSERT_EQ(OB_SUCCESS, c_del_scns.push_back(s4));
  }

  // --- write ---
  {
    ObSSMarkSweepDiagMgr mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, BUCKET_CNT));
    ASSERT_EQ(OB_SUCCESS,
        ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
    ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());
    // tablet A: del has macros, kept has one; both groups have meta scns.
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_macros(ObMarkSweepGroupType::DEL, tablet_a, a_del_ids));
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_macros(ObMarkSweepGroupType::KEPT, tablet_a, a_kept_ids));
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet_a, a_del_scns));
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT, tablet_a, a_kept_scns));
    // tablet B: del group empty (passing empty array -> no record), only kept macros.
    ObArray<MacroBlockId> empty_ids;
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_macros(ObMarkSweepGroupType::DEL, tablet_b, empty_ids));
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_macros(ObMarkSweepGroupType::KEPT, tablet_b, b_kept_ids));
    // tablet C: del macros + del meta scns.
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_macros(ObMarkSweepGroupType::DEL, tablet_c, c_del_ids));
    ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet_c, c_del_scns));
    ASSERT_EQ(OB_SUCCESS, mgr.complete());
  }

  // --- read back, aggregating across all buckets ---
  // DEL macros: tablet A (A_DEL_CNT) + tablet C (C_DEL_CNT); B's del was empty.
  {
    ObArenaAllocator rd_alloc;
    ObArray<ObSSMarkSweepDiagTabletMacros> recs;
    ASSERT_EQ(OB_SUCCESS, read_all_buckets_macros(tenant_id_, ROUND_ID, BUCKET_CNT,
        ObMarkSweepGroupType::DEL, rd_alloc, recs));
    ASSERT_EQ(2, recs.count());
    const ObSSMarkSweepDiagTabletMacros *ra = find_tablet_rec(recs, tablet_a);
    const ObSSMarkSweepDiagTabletMacros *rc = find_tablet_rec(recs, tablet_c);
    ASSERT_TRUE(nullptr != ra);
    ASSERT_TRUE(nullptr != rc);
    ASSERT_EQ(A_DEL_CNT, ra->macro_ids_.count());
    for (int64_t i = 0; i < A_DEL_CNT; ++i) {
      ASSERT_TRUE(ra->macro_ids_.at(i) == a_del_ids.at(i));
    }
    ASSERT_EQ(C_DEL_CNT, rc->macro_ids_.count());
    for (int64_t i = 0; i < C_DEL_CNT; ++i) {
      ASSERT_TRUE(rc->macro_ids_.at(i) == c_del_ids.at(i));
    }
    // tablet B must NOT appear in DEL macros (empty group skipped).
    ASSERT_TRUE(nullptr == find_tablet_rec(recs, tablet_b));
  }
  // KEPT macros: tablet A (A_KEPT_CNT) + tablet B (B_KEPT_CNT).
  {
    ObArenaAllocator rd_alloc;
    ObArray<ObSSMarkSweepDiagTabletMacros> recs;
    ASSERT_EQ(OB_SUCCESS, read_all_buckets_macros(tenant_id_, ROUND_ID, BUCKET_CNT,
        ObMarkSweepGroupType::KEPT, rd_alloc, recs));
    ASSERT_EQ(2, recs.count());
    const ObSSMarkSweepDiagTabletMacros *ra = find_tablet_rec(recs, tablet_a);
    const ObSSMarkSweepDiagTabletMacros *rb = find_tablet_rec(recs, tablet_b);
    ASSERT_TRUE(nullptr != ra);
    ASSERT_TRUE(nullptr != rb);
    ASSERT_EQ(A_KEPT_CNT, ra->macro_ids_.count());
    ASSERT_EQ(B_KEPT_CNT, rb->macro_ids_.count());
    for (int64_t i = 0; i < B_KEPT_CNT; ++i) {
      ASSERT_TRUE(rb->macro_ids_.at(i) == b_kept_ids.at(i));
    }
  }
  // DEL tablet_meta_scns: tablet A + tablet C.
  {
    ObArenaAllocator rd_alloc;
    ObArray<ObSSMarkSweepTabletMetaScns> recs;
    ASSERT_EQ(OB_SUCCESS, read_all_buckets_meta_scns(tenant_id_, ROUND_ID, BUCKET_CNT,
        ObMarkSweepGroupType::DEL, rd_alloc, recs));
    ASSERT_EQ(2, recs.count());
    const ObSSMarkSweepTabletMetaScns *ra = find_tablet_rec(recs, tablet_a);
    const ObSSMarkSweepTabletMetaScns *rc = find_tablet_rec(recs, tablet_c);
    ASSERT_TRUE(nullptr != ra);
    ASSERT_TRUE(nullptr != rc);
    ASSERT_EQ(a_del_scns.count(), ra->meta_scns_.count());
    for (int64_t i = 0; i < a_del_scns.count(); ++i) {
      ASSERT_TRUE(ra->meta_scns_.at(i) == a_del_scns.at(i));
    }
    ASSERT_EQ(c_del_scns.count(), rc->meta_scns_.count());
    ASSERT_TRUE(rc->meta_scns_.at(0) == c_del_scns.at(0));
  }
  // KEPT tablet_meta_scns: only tablet A.
  {
    ObArenaAllocator rd_alloc;
    ObArray<ObSSMarkSweepTabletMetaScns> recs;
    ASSERT_EQ(OB_SUCCESS, read_all_buckets_meta_scns(tenant_id_, ROUND_ID, BUCKET_CNT,
        ObMarkSweepGroupType::KEPT, rd_alloc, recs));
    ASSERT_EQ(1, recs.count());
    ASSERT_TRUE(recs.at(0).tablet_ == tablet_a);
    ASSERT_EQ(a_kept_scns.count(), recs.at(0).meta_scns_.count());
    ASSERT_TRUE(recs.at(0).meta_scns_.at(0) == a_kept_scns.at(0));
  }
}

TEST_F(TestSSMarkSweepGC, diagnose_lob_meta_kept_file_round_trip)
{
  const int64_t round_id = 20002;
  const int64_t bucket_cnt = 4;
  const int64_t macro_count = 1025;
  const int64_t memory_limit = 8L << 20;
  const int64_t file_buf_size = 32L << 10;
  const ObLSID ls_id(1001);
  const ObTabletID tablet_id(400004);
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ObArray<MacroBlockId> expected_macro_ids;
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, pipeline.init(
      tenant_id_, memory_limit, file_buf_size, 0 /*expire_timestamp*/, &compare));
  {
    ObMarkSweepSorter sorter;
    ObSEArray<ObSortedFileInfo, 16> files;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    for (int64_t i = 0; i < macro_count; ++i) {
      MacroBlockId macro_id;
      ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          ls_id, tablet_id, i, macro_id));
      ASSERT_EQ(OB_SUCCESS, expected_macro_ids.push_back(macro_id));
      ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(macro_id)));
    }
    ASSERT_EQ(OB_SUCCESS, sorter.finish(files));
    ASSERT_EQ(1, files.count());
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, files.at(0)));
  }
  ASSERT_EQ(OB_SUCCESS, pipeline.prepare_merge());
  ASSERT_EQ(OB_SUCCESS, pipeline.complete_group(ObMarkSweepGroupType::KEPT));
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_file(ObMarkSweepGroupType::KEPT, kept_file));
  ASSERT_EQ(macro_count, kept_file.item_count_);

  ObSSMarkSweepDiagMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, round_id, bucket_cnt));
  ASSERT_EQ(OB_SUCCESS, mgr.write_lob_meta_kept_file(kept_file, file_buf_size));
  ASSERT_EQ(OB_SUCCESS, mgr.complete());

  ObArenaAllocator allocator;
  ObArray<MacroBlockId> actual_macro_ids;
  ASSERT_EQ(OB_SUCCESS, read_all_buckets_lob_meta_kept(
      tenant_id_, round_id, bucket_cnt, allocator, actual_macro_ids));
  ASSERT_EQ(expected_macro_ids.count(), actual_macro_ids.count());
  for (int64_t i = 0; i < expected_macro_ids.count(); ++i) {
    bool found = false;
    for (int64_t j = 0; !found && j < actual_macro_ids.count(); ++j) {
      found = expected_macro_ids.at(i) == actual_macro_ids.at(j);
    }
    ASSERT_TRUE(found) << "missing macro index " << i;
  }
}

TEST_F(TestSSMarkSweepGC, diagnose_lob_ctrl_rows_and_file_round_trip)
{
  const int64_t round_id = 20003;
  const int64_t bucket_cnt = 4;
  const int64_t direct_row_count = 4;
  const int64_t file_row_count = 1025;
  const int64_t file_buf_size = 32L << 10;
  const ObLSID ls_id(1001);
  const ObTabletID tablet_id(400005);
  ObArray<ObSSOuttableLobCtrlCandidate> expected_candidates;
  ObArray<ObSSOuttableLobCtrlCandidate> expected_diffs;
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  for (int64_t i = 0; i < direct_row_count; ++i) {
    ObSSOuttableLobCtrlCandidate row;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        ls_id, tablet_id, i, row.macro_id_));
    row.hash_ = 0 == i ? 0 : (1 == i ? UINT64_MAX : i * 17);
    row.mark_row_scn_ = 2000 + i;
    ASSERT_EQ(OB_SUCCESS, expected_candidates.push_back(row));
  }
  for (int64_t i = 0; i < file_row_count; ++i) {
    ObSSOuttableLobCtrlCandidate row;
    ASSERT_EQ(OB_SUCCESS, build_sstable_macro_id_(
        blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
        ls_id, tablet_id, 10000 + i, row.macro_id_));
    row.hash_ = 0 == i % 3 ? 0 : (1 == i % 3 ? UINT64_MAX : i * 31);
    row.mark_row_scn_ = 5000 + i;
    ASSERT_EQ(OB_SUCCESS, expected_diffs.push_back(row));
  }

  ObMarkSweepTmpFileMgr file_mgr;
  ObSSLobCtrlGCScanFile diff_file;
  ASSERT_EQ(OB_SUCCESS, file_mgr.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, create_lob_ctrl_tmp_file(
      tenant_id_, file_buf_size, expected_diffs, file_mgr, diff_file));
  ASSERT_TRUE(diff_file.is_valid());
  ObSSMarkSweepDiagMgr diag_mgr;
  ASSERT_EQ(OB_SUCCESS, diag_mgr.init(tenant_id_, round_id, bucket_cnt));
  ASSERT_EQ(OB_SUCCESS, diag_mgr.write_lob_ctrl_rows(
      ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE, expected_candidates));
  ASSERT_EQ(OB_SUCCESS, diag_mgr.write_lob_ctrl_file(
      ObSSMarkSweepDiagFileType::LOB_CTRL_DIFF, diff_file, file_buf_size));
  ASSERT_EQ(OB_SUCCESS, diag_mgr.complete());

  ObArenaAllocator allocator;
  ObArray<ObSSOuttableLobCtrlCandidate> actual_candidates;
  ObArray<ObSSOuttableLobCtrlCandidate> actual_diffs;
  ASSERT_EQ(OB_SUCCESS, read_all_buckets_lob_ctrl(tenant_id_, round_id, bucket_cnt,
      ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE, allocator, actual_candidates));
  ASSERT_EQ(OB_SUCCESS, read_all_buckets_lob_ctrl(tenant_id_, round_id, bucket_cnt,
      ObSSMarkSweepDiagFileType::LOB_CTRL_DIFF, allocator, actual_diffs));
  ASSERT_NO_FATAL_FAILURE(check_lob_ctrl_rows(expected_candidates, actual_candidates));
  ASSERT_NO_FATAL_FAILURE(check_lob_ctrl_rows(expected_diffs, actual_diffs));
}

// ---------------------------------------------------------------------------
// diagnose_manager_concurrent: many threads write distinct tablets' DEL macros to
// one shared manager concurrently. After complete(), aggregate all buckets and assert
// every tablet is present exactly once with the right macro count. Exercises the
// per-bucket locking (tablets hash into a small number of shared bucket files).
// ---------------------------------------------------------------------------
TEST_F(TestSSMarkSweepGC, diagnose_manager_concurrent)
{
  const int64_t ROUND_ID = 20001;
  const int64_t BUCKET_CNT = 8;
  const int64_t THREAD_CNT = 8;
  const int64_t TABLETS_PER_THREAD = 25; // 200 tablets total across 8 buckets
  const int64_t MACROS_PER_TABLET = 4;
  const ObLSID LS_ID(1001);

  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));

  ObSSMarkSweepDiagMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, BUCKET_CNT));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());

  std::atomic<int> fail_cnt(0);
  std::vector<std::thread> threads;
  for (int64_t t = 0; t < THREAD_CNT; ++t) {
    threads.emplace_back([&, t]() {
      share::ObTenantSwitchGuard thread_guard;
      if (OB_SUCCESS != thread_guard.switch_to(tenant_id_)) {
        fail_cnt++;
        return;
      }
      for (int64_t i = 0; i < TABLETS_PER_THREAD; ++i) {
        const int64_t tablet_no = 500000 + t * TABLETS_PER_THREAD + i;
        ObSSGCTablet tablet(LS_ID, ObTabletID(tablet_no), share::SCN::min_scn());
        ObArray<MacroBlockId> ids;
        for (int64_t k = 0; k < MACROS_PER_TABLET; ++k) {
          MacroBlockId id;
          if (OB_SUCCESS != build_sstable_macro_id_(
                  blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
                  LS_ID, ObTabletID(tablet_no), /*data_seq=*/k, id)) {
            fail_cnt++;
          } else if (OB_SUCCESS != ids.push_back(id)) {
            fail_cnt++;
          }
        }
        if (OB_SUCCESS != mgr.write_tablet_macros(ObMarkSweepGroupType::DEL, tablet, ids)) {
          fail_cnt++;
        }
      }
    });
  }
  for (auto &th : threads) {
    th.join();
  }
  ASSERT_EQ(0, fail_cnt.load());
  ASSERT_EQ(OB_SUCCESS, mgr.complete());

  // aggregate all buckets and check every tablet appears once with the right count.
  ObArenaAllocator rd_alloc;
  ObArray<ObSSMarkSweepDiagTabletMacros> recs;
  ASSERT_EQ(OB_SUCCESS, read_all_buckets_macros(tenant_id_, ROUND_ID, BUCKET_CNT,
      ObMarkSweepGroupType::DEL, rd_alloc, recs));
  ASSERT_EQ(THREAD_CNT * TABLETS_PER_THREAD, recs.count());
  for (int64_t t = 0; t < THREAD_CNT; ++t) {
    for (int64_t i = 0; i < TABLETS_PER_THREAD; ++i) {
      const int64_t tablet_no = 500000 + t * TABLETS_PER_THREAD + i;
      ObSSGCTablet tablet(LS_ID, ObTabletID(tablet_no), share::SCN::min_scn());
      const ObSSMarkSweepDiagTabletMacros *r = find_tablet_rec(recs, tablet);
      ASSERT_TRUE(nullptr != r) << "tablet_no=" << tablet_no;
      ASSERT_EQ(MACROS_PER_TABLET, r->macro_ids_.count());
    }
  }
}

// ASAN bypasses the tenant allocator and calls malloc directly, so do not register failure-injection cases that rely on tenant limits.
#ifndef OB_USE_ASAN
TEST_F(TestSSMarkSweepGC, diagnose_study_raw_result_oom)
{
  const uint64_t TEST_TENANT_ID = 900001;
  ObTenantSwitchGuard guard;
  ObScopedStudyDiagTenantAllocator allocator;
  ObSSGCStudyDiagInfo diag_info(TEST_TENANT_ID);
  ObAddr server;
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray results;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSSGCTabletStudyInfo study_info;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882));
  study_info.set_to_min();
  ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
  ASSERT_EQ(OB_SUCCESS, results.push_back(ObSSGCTabletStudyResult(tablet, study_info)));
  ASSERT_EQ(OB_SUCCESS, allocator.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, diag_info.save_study_result(server, batch, results));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, diag_info.get_first_fail_ret());
  EXPECT_EQ(0, diag_info.get_study_results().count());
  EXPECT_EQ(1, results.count());
  EXPECT_TRUE(results.at(0).gc_tablet_ == tablet);
  EXPECT_TRUE(results.at(0).study_info_.min_dep_meta_scn_.is_min());

  ASSERT_EQ(OB_SUCCESS, allocator.restore_limit());
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, diag_info.get_first_fail_ret());
  EXPECT_EQ(0, diag_info.get_study_results().count());
  diag_info.reset();
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  EXPECT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  EXPECT_EQ(1, diag_info.get_study_results().count());
  // Destroy diag_info before the allocator so all diagnose arrays are freed before reclaiming the allocator.
}
#endif

// Synchronous real RPC covers diagnose-off and diagnose OOM; neither may block main-path merge.
#ifndef OB_USE_ASAN
TEST_F(TestSSMarkSweepGC, diagnose_study_rpc_error_isolation)
{
  const uint64_t TEST_TENANT_ID = 900001;
  const ObLSID LS_ID(900001);
  ObTenantSwitchGuard guard;
  ObScopedStudyDiagTenantAllocator allocator;
  ObSSGCStudyDiagInfo diag_info(TEST_TENANT_ID);
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  obrpc::ObSSMarkSweepGCStudyProxy proxy;
  hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray merged_results;
  ObSSGCTablet tablet(LS_ID, ObTabletID(800000), SCN::min_scn());
  int64_t rpc_cost_us = 0;
  int64_t index = -1;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_LS_NOT_EXIST,
      ls_service->get_ls(LS_ID, ls_handle, ObLSGetMod::SHARED_STORAGE_GC_MOD));
  ASSERT_NE(nullptr, GCTX.net_frame_);
  ASSERT_EQ(OB_SUCCESS, GCTX.net_frame_->get_proxy(proxy));
  ASSERT_EQ(OB_SUCCESS, tablet_index_map.create(10, ObMemAttr(tenant_id_, "SSGCStudyDiag")));
  ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
  tablet.tablet_id_ = ObTabletID(800001);
  ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
  for (int64_t phase = 0; phase < 2; ++phase) {
    if (1 == phase) {
      ASSERT_EQ(OB_SUCCESS, allocator.init(TEST_TENANT_ID));
    }
    // phase 0 leaves the collector unbound (diagnose off); phase 1 binds then triggers collect OOM.
    ObSSGCStudyDiagInfo::Scope diag_scope(1 == phase ? &diag_info : nullptr);
    ASSERT_EQ(OB_SUCCESS, tablet_index_map.clear());
    merged_results.reuse();
    ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::send_batch_rpc_request_(
        get_curr_simple_server().get_addr(), batch, proxy, tablet_index_map,
        merged_results, rpc_cost_us));
    ASSERT_EQ(batch.count(), merged_results.count());
    for (int64_t i = 0; i < batch.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, tablet_index_map.get_refactored(batch.at(i).gc_tablet_, index));
      ASSERT_GE(index, 0);
      ASSERT_LT(index, merged_results.count());
      ASSERT_TRUE(batch.at(i).gc_tablet_ == merged_results.at(index).gc_tablet_);
      ASSERT_TRUE(merged_results.at(index).study_info_.min_dep_meta_scn_.is_max());
      ASSERT_EQ(INT64_MAX, merged_results.at(index).study_info_.min_major_snapshot_);
    }
  }
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, diag_info.get_first_fail_ret());
  ASSERT_EQ(0, diag_info.get_study_results().count());
}
#endif

// Version gate and config switch must take effect before construction; Scope binds and RPC raw values publish only when both pass.
TEST_F(TestSSMarkSweepGC, diagnose_study_guard_gate_and_publish)
{
  ObTenantSwitchGuard tenant_guard;
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_cluster_version = GET_MIN_CLUSTER_VERSION();
  const uint64_t saved_data_version = ATOMIC_LOAD(&cluster_version.data_version_);
  const uint64_t saved_mock_data_version = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  uint64_t actual_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  ASSERT_TRUE(tenant_config.is_valid());
  const bool saved_diag_enabled = tenant_config->_enable_shared_mark_sweep_gc_diagnose;
  DEFER({
    tenant_config->_enable_shared_mark_sweep_gc_diagnose = saved_diag_enabled;
    cluster_version.update_cluster_version(saved_cluster_version);
    cluster_version.update_data_version(saved_data_version);
    ODV_MGR.set_mock_data_version(saved_mock_data_version);
  });
  ASSERT_TRUE(ObUpgradeChecker::check_data_version_exist(DATA_VERSION_4_6_3_0));
  cluster_version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_2_1);
  tenant_config->_enable_shared_mark_sweep_gc_diagnose = true;
  cluster_version.update_cluster_version(CLUSTER_VERSION_4_6_2_0);
  ASSERT_EQ(OB_NOT_SUPPORTED, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, 26010));
  ASSERT_NO_FATAL_FAILURE(check_study_diag_guard_(
      26010, false/*expect_enabled*/, false/*expect_registered*/, OB_SUCCESS));

  cluster_version.update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, actual_data_version));
  ASSERT_LT(actual_data_version, DATA_VERSION_4_6_3_0);
  ASSERT_NO_FATAL_FAILURE(check_study_diag_guard_(
      26006, true/*expect_enabled*/, true/*expect_registered*/, OB_SUCCESS));

  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  tenant_config->_enable_shared_mark_sweep_gc_diagnose = false;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_3_0, actual_data_version);
  ASSERT_NO_FATAL_FAILURE(check_study_diag_guard_(
      26007, false/*expect_enabled*/, false/*expect_registered*/, OB_SUCCESS));

  tenant_config->_enable_shared_mark_sweep_gc_diagnose = true;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, actual_data_version));
  ASSERT_EQ(DATA_VERSION_4_6_3_0, actual_data_version);
  ASSERT_NO_FATAL_FAILURE(check_study_diag_guard_(
      26008, true/*expect_enabled*/, true/*expect_registered*/, OB_SUCCESS));

  // Failed preparation disables diagnose collection, but real RPC and GC merge still succeed.
  ASSERT_NO_FATAL_FAILURE(check_study_diag_guard_(
      26009, true/*expect_enabled*/, false/*expect_registered*/, OB_INVALID_ARGUMENT));

  // A disabled Study guard is still finished; enabling config afterwards must not register a marker.
  {
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
    ObSharedGCTask task;
    const ObSharedGCTaskKey key(
        tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, 26011, 0/*task_id*/);
    tenant_config->_enable_shared_mark_sweep_gc_diagnose = false;
    ObSSGCStudyDiagGuard diag_guard(tenant_id_, key.round_id_);
    ASSERT_EQ(OB_SUCCESS, diag_guard.finish(SCN::min_scn(), OB_SUCCESS));
    ASSERT_TRUE(diag_guard.is_finished_);
    tenant_config->_enable_shared_mark_sweep_gc_diagnose = true;
    ASSERT_EQ(OB_SUCCESS, diag_guard.prepare());
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
        ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  }
}

// On failure before the safe watermark is produced, mark an invalid SCN; min/max still display as legal special values.
TEST_F(TestSSMarkSweepGC, diagnose_study_skipped_event_scn_validity)
{
  int ret = OB_SUCCESS;
  const int64_t ROUND_ID = 26003;
  const int INPUT_RET = OB_TIMEOUT;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  SCN gc_meta_scn;
  ObSqlString sql;
  char gc_meta_scn_buf[128] = {};
  int64_t count = 0;
  int64_t deadline = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_FALSE(gc_meta_scn.is_valid());
  for (int64_t i = 0; i < 3; ++i) {
    const int64_t round_id = ROUND_ID + i;
    const bool gc_meta_scn_valid = 0 != i;
    if (1 == i) {
      gc_meta_scn.set_min();
    } else if (2 == i) {
      gc_meta_scn.set_max();
    }
    ASSERT_EQ(gc_meta_scn_valid, gc_meta_scn.is_valid());
    ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, round_id));
    ASSERT_EQ(OB_SUCCESS, mgr.publish_study_diag(
        tenant_id_, round_id, gc_meta_scn, INPUT_RET, diag_info));
    ASSERT_FALSE(mgr.has_diag_failed());
    ASSERT_EQ(0, mgr.diag_file_cnt_);
    ASSERT_GT(gc_meta_scn.to_string(gc_meta_scn_buf, sizeof(gc_meta_scn_buf)), 0);
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_server_event_history "
        "WHERE module = 'SHARED_GC' AND event = 'FINISH_MARK_SWEEP_STUDY_DIAG' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'round_id' AND value2 = '%ld' "
        "AND name3 = 'gc_meta_scn' AND value3 = '%s' "
        "AND name5 = 'diag_first_fail_ret' AND value5 = '0' "
        "AND name6 = 'publish_status' AND value6 = 'SKIPPED' "
        "AND extra_info LIKE 'input_ret=%d,%%' "
        "AND extra_info LIKE '%%input_result_count=0,input_tablet_count=0,"
        "append_record_count=0,%%' "
        "AND extra_info LIKE '%%gc_meta_scn_valid=%d,%%'",
        tenant_id_, round_id, gc_meta_scn_buf, INPUT_RET, gc_meta_scn_valid));
    count = 0;
    deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000;
    while (OB_SUCC(ret) && 0 == count && ObTimeUtility::current_time() < deadline) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        sqlclient::ObMySQLResult *rows = nullptr;
        ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
        rows = result.get_result();
        ASSERT_NE(nullptr, rows);
        ASSERT_EQ(OB_SUCCESS, rows->next());
        ASSERT_EQ(OB_SUCCESS, rows->get_int("cnt", count));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      if (0 == count) {
        ob_usleep(100 * 1000);
      }
    }
    ASSERT_EQ(1, count);
  }
}

// Two LSes each have two remapped versions; three nodes keep distinct raw watermarks.
TEST_F(TestSSMarkSweepGC, diagnose_study_grouping_and_publish)
{
  const int64_t ROUND_ID = 26000;
  ObTenantSwitchGuard guard;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  ObSSMarkSweepDiagMgr::StudyDiagStat stat;
  ObArray<ObSSMarkSweepDiagStudyNode> source_nodes;
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray results;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSSGCTabletStudyInfo study_info;
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagFileReader reader;
  int64_t read_batch_count = 0;
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, build_study_nodes_(source_nodes));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(3, source_nodes.count());
  for (int64_t ls = 0; ls < 2; ++ls) {
    tablet.ls_id_ = ObLSID(1001 + ls);
    batch.reuse();
    for (int64_t t = 0; t < 2; ++t) {
      ASSERT_EQ(OB_SUCCESS, tablet.transfer_scn_.convert_for_inner_table_field(100 + t));
      ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
    }
    for (int64_t n = 0; n < source_nodes.count(); ++n) {
      results.reuse();
      for (int64_t t = 0; t < batch.count(); ++t) {
        const int64_t raw_value = 1000000 + ls * 10000 + t * 100 + n;
        ASSERT_EQ(OB_SUCCESS,
            study_info.min_dep_meta_scn_.convert_for_inner_table_field(raw_value));
        study_info.min_major_snapshot_ = raw_value + 1000000;
        ASSERT_EQ(OB_SUCCESS,
            results.push_back(ObSSGCTabletStudyResult(batch.at(t).gc_tablet_, study_info)));
      }
      ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(
          source_nodes.at(n).server_, batch, results));
      ASSERT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
    }
  }
  ASSERT_EQ(6, diag_info.get_study_results().count());
  ASSERT_EQ(OB_SUCCESS, mgr.validate_study_diag_(diag_info, stat));
  ASSERT_EQ(12, stat.input_result_count_);
  ASSERT_EQ(4, stat.input_tablet_count_);
  // Call the storage writer directly to inspect its per-stage statistics.
  ASSERT_EQ(OB_SUCCESS, mgr.write_study_diag_(tenant_id_, ROUND_ID, diag_info, stat));
  ASSERT_EQ(OB_SUCCESS, stat.first_fail_ret_);
  ASSERT_FALSE(mgr.has_diag_failed());
  ASSERT_EQ(12, stat.input_result_count_);
  ASSERT_EQ(4, stat.input_tablet_count_);
  ASSERT_EQ(4, stat.append_record_count_);
  ASSERT_EQ(OB_SUCCESS, reader.init(tenant_id_, ROUND_ID, ObMarkSweepGroupType::KEPT,
      ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/));
  ASSERT_EQ(4, reader.record_lens_.count());
  ASSERT_EQ(OB_SUCCESS, check_study_raw_results_(ROUND_ID, diag_info, read_batch_count));
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      ROUND_ID, "AND type = 'study_results'", row_count));
  ASSERT_EQ(12, row_count);
}

// Stop after the first batch is appended; the sole file cannot complete; the event keeps the append count and marks FAILED.
TEST_F(TestSSMarkSweepGC, diagnose_study_stop_during_publish)
{
  const int64_t ROUND_ID = 26001;
  const int64_t BATCH_BYTES = ObSSMarkSweepDiagMgr::STUDY_BATCH_BYTES;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  ObSSGCService *gc_service = nullptr;
  ObArray<ObSSMarkSweepDiagStudyNode> nodes;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSSMarkSweepDiagFileReader reader;
  MacroBlockId macro_id;
  int64_t record_len = 0;
  int64_t tablets_per_ls = 0;
  int64_t appended_count = 0;
  int finish_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  gc_service = MTL(ObSSGCService *);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_NE(nullptr, gc_service);
  ASSERT_FALSE(gc_service->is_stopped());
  ASSERT_EQ(OB_SUCCESS, build_study_nodes_(nodes));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_study_record_size(tablet, nodes, record_len));
  ASSERT_GT(record_len, 0);
  tablets_per_ls = BATCH_BYTES / record_len / 2 + 8;
  ASSERT_EQ(OB_SUCCESS, build_study_raw_results_(2/*ls_count*/, tablets_per_ls, diag_info));
  {
    ObScopedStudyDiagBatch batch_guard;
    ASSERT_EQ(OB_SUCCESS, batch_guard.init(mgr, &gc_service->is_stopped_));
    finish_ret = mgr.publish_study_diag(
        tenant_id_, ROUND_ID, SCN::min_scn(), OB_SUCCESS, diag_info);
    ASSERT_EQ(OB_SUCCESS, batch_guard.wait());
    ASSERT_TRUE(gc_service->is_stopped());
    appended_count = batch_guard.get_first_record_count();
    ASSERT_GT(appended_count, 0);
    ASSERT_LT(appended_count, tablets_per_ls * 2);
    ASSERT_GE(batch_guard.get_first_data_len(), BATCH_BYTES);
  }
  ASSERT_FALSE(gc_service->is_stopped());
  ASSERT_EQ(OB_SERVICE_STOPPED, finish_ret);
  ASSERT_EQ(1, mgr.bucket_cnt_);
  for (int64_t i = 0; i < mgr.diag_file_cnt_; ++i) {
    ASSERT_FALSE(mgr.diag_file_[i].is_opened_);
    ASSERT_EQ(0, mgr.diag_file_[i].index_.count());
    ASSERT_EQ(0, mgr.diag_file_[i].index_.record_lens_.get_capacity());
  }
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(ROUND_ID,
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS,
      0/*bucket*/, macro_id));
  assert_block_exists_(macro_id, false/*expected_exist*/);
  ASSERT_NE(OB_SUCCESS, reader.init(tenant_id_, ROUND_ID, ObMarkSweepGroupType::KEPT,
      ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/));
  ASSERT_EQ(OB_SUCCESS, wait_study_diag_failed_event_(ROUND_ID, appended_count, finish_ret));
  mgr.reset();
  diag_info.reset();
  ASSERT_TRUE(diag_info.get_study_results().empty());
}

// An invalid per-node watermark skips only that tablet; valid records before/after still land in the fixed file.
TEST_F(TestSSMarkSweepGC, diagnose_study_invalid_record_continues)
{
  const int64_t ROUND_ID = 26002;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagMgr::StudyDiagStat stat;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  ObSSMarkSweepDiagFileReader reader;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<ObString> records;
  ObSSMarkSweepDiagStudyResults snapshot;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, build_study_raw_results_(1/*ls_count*/, 3/*tablets_per_ls*/, diag_info));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  diag_info.get_study_results().at(1)->tablet_results_.at(1).study_info_.min_major_snapshot_ = -1;
  ASSERT_EQ(OB_SUCCESS, mgr.validate_study_diag_(diag_info, stat));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      mgr.write_study_diag_(tenant_id_, ROUND_ID, diag_info, stat));
  ASSERT_EQ(OB_INVALID_ARGUMENT, stat.first_fail_ret_);
  ASSERT_STREQ("write", stat.first_fail_stage_);
  ASSERT_EQ(9, stat.input_result_count_);
  ASSERT_EQ(3, stat.input_tablet_count_);
  ASSERT_EQ(2, stat.append_record_count_);
  ASSERT_EQ(OB_SUCCESS, reader.init(tenant_id_, ROUND_ID, ObMarkSweepGroupType::KEPT,
      ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/));
  ASSERT_EQ(2, reader.record_lens_.count());
  ASSERT_EQ(OB_SUCCESS, reader.next_record_batch(allocator, records));
  ASSERT_EQ(2, records.count());
  for (int64_t i = 0; i < records.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(records.at(i), snapshot));
    ASSERT_EQ(800000 + i * 2, snapshot.tablet_.tablet_id_.id());
    ASSERT_EQ(3, snapshot.study_results_.count());
    for (int64_t n = 0; n < snapshot.study_results_.count(); ++n) {
      const ObSSGCStudyDiagServerResult &server = *diag_info.get_study_results().at(n);
      const ObSSGCTabletStudyInfo &expected = server.tablet_results_.at(i * 2).study_info_;
      ASSERT_EQ(server.server_, snapshot.study_results_.at(n).server_);
      ASSERT_EQ(expected.min_dep_meta_scn_,
          snapshot.study_results_.at(n).study_info_.min_dep_meta_scn_);
      ASSERT_EQ(expected.min_major_snapshot_,
          snapshot.study_results_.at(n).study_info_.min_major_snapshot_);
    }
  }
}

// RPC raw values must be owned independently; LS-level rollback must not affect prior successful LSes; stop appending after collect failure.
TEST_F(TestSSMarkSweepGC, diagnose_study_raw_result_lifecycle)
{
  ObTenantSwitchGuard guard;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  ObAddr server;
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray results;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSSGCTabletStudyInfo study_info;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882));
  study_info.set_to_min();
  ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
  ASSERT_EQ(OB_SUCCESS, results.push_back(ObSSGCTabletStudyResult(tablet, study_info)));
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  ASSERT_EQ(1, diag_info.get_study_results().count());
  results.at(0).study_info_.set_to_max();
  ASSERT_TRUE(diag_info.get_study_results().at(0)->tablet_results_.at(0)
      .study_info_.min_dep_meta_scn_.is_min());
  ASSERT_EQ(0, diag_info.get_study_results().at(0)->tablet_results_.at(0)
      .study_info_.min_major_snapshot_);

  tablet.tablet_id_ = ObTabletID(800001);
  batch.reuse();
  results.reuse();
  ASSERT_EQ(OB_SUCCESS, batch.push_back(ObSSGCTabletWithDelScn(tablet, SCN::min_scn())));
  ASSERT_EQ(OB_SUCCESS, results.push_back(ObSSGCTabletStudyResult(tablet, study_info)));
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(1, diag_info.get_study_results().count());
  ASSERT_EQ(2, diag_info.get_study_results().at(0)->tablet_results_.count());

  tablet.ls_id_ = ObLSID(1002);
  batch.at(0).gc_tablet_ = tablet;
  results.at(0).gc_tablet_ = tablet;
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2883));
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(3, diag_info.get_study_results().count());
  diag_info.reset_ls_study_results(ObLSID(1002));
  ASSERT_EQ(1, diag_info.get_study_results().count());
  ASSERT_TRUE(diag_info.get_study_results().at(0)->ls_id_ == ObLSID(1001));
  ASSERT_EQ(2, diag_info.get_study_results().at(0)->tablet_results_.count());
  ASSERT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());

  results.reuse();
  ASSERT_EQ(OB_INVALID_DATA, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(OB_INVALID_DATA, diag_info.get_first_fail_ret());
  ASSERT_EQ(0, diag_info.get_study_results().count());
  ASSERT_EQ(OB_SUCCESS, results.push_back(ObSSGCTabletStudyResult(tablet, study_info)));
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(OB_INVALID_DATA, diag_info.get_first_fail_ret());
  ASSERT_EQ(0, diag_info.get_study_results().count());
  diag_info.reset();
  ASSERT_EQ(OB_SUCCESS, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  ASSERT_EQ(1, diag_info.get_study_results().count());
}

// Study always writes bucket 0 and batch records read back correctly; non-zero buckets must not open a writer.
TEST_F(TestSSMarkSweepGC, diagnose_study_bucket_round_trip)
{
  const int64_t ROUND_ID = 22000;
  const int64_t BUCKET_CNT = 4;
  const int64_t TABLET_COUNT = 10000;
  const int64_t BATCH_BYTES = ObSSMarkSweepDiagMgr::STUDY_BATCH_BYTES;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<int64_t> record_lens;
  ObString data;
  MacroBlockId macro_id;
  int64_t batch_count = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, BUCKET_CNT));
  mgr.set_diag_enabled(true);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, build_study_bucket_(
      0, BUCKET_CNT, TABLET_COUNT, allocator, data, record_lens));
  ASSERT_EQ(TABLET_COUNT, record_lens.count());
  ASSERT_LT(data.length(), BATCH_BYTES);
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  const int64_t file_idx = mgr.diag_file_idx_(
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0);
  ASSERT_EQ(TABLET_COUNT, mgr.diag_file_[file_idx].index_.count());
  ASSERT_EQ(data.length(), mgr.diag_file_[file_idx].offset_);
  ASSERT_EQ(OB_SUCCESS, mgr.complete_study_results(0));
  ASSERT_FALSE(mgr.diag_file_[file_idx].is_opened_);
  ASSERT_EQ(0, mgr.diag_file_[file_idx].index_.count());
  ASSERT_EQ(0, mgr.diag_file_[file_idx].index_.record_lens_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, check_study_bucket_(ROUND_ID, 0, BUCKET_CNT, TABLET_COUNT, batch_count));
  ASSERT_GE(batch_count, 1);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(
      ROUND_ID, ObMarkSweepGroupType::KEPT,
      ObSSMarkSweepDiagFileType::STUDY_RESULTS, 1, macro_id));
  assert_block_exists_(macro_id, false/*expected_exist*/);

  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.write_tablets_study_results(
      1, data.ptr(), data.length(), record_lens));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(macro_id, false/*expected_exist*/));
  ASSERT_EQ(OB_SUCCESS, mgr.complete());
}

// Full records batch in LS order; after the first batch hits the target it appends, the tail batch keeps writing the same file, and complete runs once.
TEST_F(TestSSMarkSweepGC, diagnose_study_batched_publish)
{
  const int64_t ROUND_ID = 22001;
  const int64_t BATCH_BYTES = ObSSMarkSweepDiagMgr::STUDY_BATCH_BYTES;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagMgr::StudyDiagStat stat;
  ObSSGCStudyDiagInfo diag_info(tenant_id_);
  ObArray<ObSSMarkSweepDiagStudyNode> nodes;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObScopedStudyDiagBatch batch_guard;
  ObSSMarkSweepDiagFileReader reader;
  int64_t record_len = 0;
  int64_t tablets_per_ls = 0;
  int64_t read_batch_count = 0;
  int64_t file_idx = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, build_study_nodes_(nodes));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_study_record_size(tablet, nodes, record_len));
  ASSERT_GT(record_len, 0);
  tablets_per_ls = BATCH_BYTES / record_len / 2 + 8;
  ASSERT_EQ(OB_SUCCESS, build_study_raw_results_(2/*ls_count*/, tablets_per_ls, diag_info));
  ASSERT_EQ(OB_SUCCESS, batch_guard.init(mgr, nullptr/*is_stopped*/));
  ASSERT_EQ(OB_SUCCESS, mgr.validate_study_diag_(diag_info, stat));
  ASSERT_EQ(OB_SUCCESS, mgr.write_study_diag_(tenant_id_, ROUND_ID, diag_info, stat));
  ASSERT_EQ(OB_SUCCESS, batch_guard.wait());
  ASSERT_EQ(OB_SUCCESS, stat.first_fail_ret_);
  ASSERT_EQ(tablets_per_ls * 6, stat.input_result_count_);
  ASSERT_EQ(tablets_per_ls * 2, stat.input_tablet_count_);
  ASSERT_EQ(tablets_per_ls * 2, stat.append_record_count_);
  ASSERT_GE(batch_guard.get_first_data_len(), BATCH_BYTES);
  ASSERT_LT(batch_guard.get_first_data_len(), BATCH_BYTES + record_len);
  ASSERT_GT(batch_guard.get_first_record_count(), tablets_per_ls);
  ASSERT_LT(batch_guard.get_first_record_count(), stat.append_record_count_);
  ASSERT_EQ(batch_guard.get_first_record_count() * record_len, batch_guard.get_first_data_len());
  ASSERT_EQ(1, mgr.bucket_cnt_);
  file_idx = mgr.diag_file_idx_(
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/);
  ASSERT_TRUE(mgr.diag_file_[file_idx].is_finished_);
  ASSERT_FALSE(mgr.diag_file_[file_idx].is_opened_);
  ASSERT_EQ(0, mgr.diag_file_[file_idx].index_.record_lens_.get_capacity());
  ASSERT_EQ(OB_SUCCESS, reader.init(tenant_id_, ROUND_ID, ObMarkSweepGroupType::KEPT,
      ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0/*bucket*/));
  ASSERT_EQ(stat.append_record_count_, reader.record_lens_.count());
  ASSERT_GT(reader.data_len_, batch_guard.get_first_data_len());
  ASSERT_LT(reader.data_len_ - batch_guard.get_first_data_len(), BATCH_BYTES);
  ASSERT_EQ(OB_SUCCESS, check_study_raw_results_(ROUND_ID, diag_info, read_batch_count));
  ASSERT_GT(read_batch_count, 1);
}

// A completed Study file must not reopen; continuing original diagnose complete must not clear the first diagnose error.
TEST_F(TestSSMarkSweepGC, diagnose_study_finished_bucket)
{
  const int64_t ROUND_ID = 22002;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<int64_t> record_lens;
  ObArray<SCN> scns;
  ObArray<ObSSMarkSweepTabletMetaScns> records;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObString data;
  int first_ret = OB_SUCCESS;
  int64_t batch_count = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, 2));
  mgr.set_diag_enabled(true);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, build_study_bucket_(0, 2, 2, allocator, data, record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.complete_study_results(0));
  first_ret = mgr.write_tablets_study_results(0, data.ptr(), data.length(), record_lens);
  ASSERT_NE(OB_SUCCESS, first_ret);
  ASSERT_TRUE(mgr.has_diag_failed());
  ASSERT_EQ(first_ret, mgr.get_first_fail_ret());
  ASSERT_EQ(OB_SUCCESS, check_study_bucket_(ROUND_ID, 0, 2, 2, batch_count));
  ASSERT_EQ(OB_SUCCESS, scns.push_back(SCN::min_scn()));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, mgr.complete());
  ASSERT_TRUE(mgr.has_diag_failed());
  ASSERT_EQ(first_ret, mgr.get_first_fail_ret());
  ASSERT_EQ(OB_SUCCESS, read_all_buckets_meta_scns(
      tenant_id_, ROUND_ID, 2, ObMarkSweepGroupType::KEPT, allocator, records));
  ASSERT_EQ(1, records.count());
  mgr.reset();
  ASSERT_FALSE(mgr.has_diag_failed());
  ASSERT_EQ(OB_SUCCESS, mgr.get_first_fail_ret());
}

// Reject illegal length indexes before upload; do not record a whole-batch length as one record's index.
TEST_F(TestSSMarkSweepGC, diagnose_study_invalid_record_lengths)
{
  const int64_t ROUND_ID = 22003;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<int64_t> record_lens;
  ObString data;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(0, nullptr, 0, record_lens));
  ASSERT_FALSE(mgr.has_diag_failed());
  ASSERT_FALSE(mgr.is_inited());
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, 1));
  ASSERT_EQ(OB_SUCCESS, build_study_bucket_(0, 1, 2, allocator, data, record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_FALSE(mgr.has_diag_failed());
  mgr.set_diag_enabled(true);
  ++record_lens.at(0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  mgr.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, 1));
  mgr.set_diag_enabled(true);
  --record_lens.at(0);
  record_lens.at(1) = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  const int64_t file_idx = mgr.diag_file_idx_(
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0);
  ASSERT_FALSE(mgr.diag_file_[file_idx].is_opened_);
  ASSERT_EQ(0, mgr.diag_file_[file_idx].index_.count());
}

// Truncation, trailing bytes, and untrusted node counts must not form a readable Study record.
TEST_F(TestSSMarkSweepGC, diagnose_study_record_decode_boundary)
{
  ObTenantSwitchGuard guard;
  ObArray<ObSSMarkSweepDiagStudyNode> nodes;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSSMarkSweepDiagStudyResults snapshot;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  int64_t size = 0;
  int64_t pos = 0;
  char *buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, build_study_nodes_(nodes));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_study_record_size(tablet, nodes, size));
  ASSERT_NE(nullptr, buf = static_cast<char *>(allocator.alloc(size + 1)));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::encode_study_record(tablet, nodes, buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(ObString(size, buf), snapshot));
  ASSERT_EQ(3, snapshot.study_results_.count());
  ASSERT_NE(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(
      ObString(size - 1, buf), snapshot));
  buf[size] = 'x';
  ASSERT_NE(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(
      ObString(size + 1, buf), snapshot));
  pos = tablet.get_serialize_size();
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, size, pos, INT64_MAX));
  ASSERT_NE(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(ObString(size, buf), snapshot));
  pos = tablet.get_serialize_size();
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, size, pos, -1));
  ASSERT_NE(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_study_record(ObString(size, buf), snapshot));
}

// Register one empty round marker; all managers reuse it regardless of type or bucket count.
TEST_F(TestSSMarkSweepGC, diagnose_task_registration)
{
  int ret = OB_SUCCESS;
  const int64_t ROUND_ID = 22020;
  const int64_t BUCKET_CNT = 2;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr study_mgr;
  ObSSMarkSweepDiagMgr dag_mgr;
  ObSSMarkSweepDiagMgr expanded_mgr;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObSharedGCTask task;
  ObSharedGCTask repeated_task;
  ObSqlString sql;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, ROUND_ID, 0);
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  ASSERT_TRUE(task.is_valid());
  ASSERT_EQ(0, task.key_.task_id_);
  ASSERT_TRUE(task.value_.empty());
  ASSERT_TRUE(task.extra_info_.empty());
  ASSERT_EQ(OB_SUCCESS, study_mgr.init(tenant_id_, ROUND_ID, 1));
  ASSERT_EQ(OB_SUCCESS, study_mgr.check_diag_task());
  ASSERT_TRUE(study_mgr.is_diag_task_prepared_);
  ASSERT_EQ(OB_SUCCESS, dag_mgr.init(tenant_id_, ROUND_ID, BUCKET_CNT));
  ASSERT_EQ(OB_SUCCESS, dag_mgr.check_diag_task());
  ASSERT_TRUE(dag_mgr.is_diag_task_prepared_);
  ASSERT_EQ(OB_SUCCESS, dag_mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, expanded_mgr.init(tenant_id_, ROUND_ID, BUCKET_CNT + 1));
  ASSERT_EQ(OB_SUCCESS, expanded_mgr.check_diag_task());
  ASSERT_TRUE(expanded_mgr.is_diag_task_prepared_);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(
      key, allocator, sql_proxy(), repeated_task));
  ASSERT_EQ(task.value_, repeated_task.value_);
  key.task_id_ = 1;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), repeated_task));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT COUNT(*) AS cnt FROM oceanbase.__all_virtual_shared_gc_task "
      "WHERE tenant_id = %lu AND type = 'MARK_SWEEP_GC_DIAG' AND round_id = %ld "
      "AND task_id = 0 AND value = ''", tenant_id_, ROUND_ID));
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", count));
    ASSERT_EQ(1, count);
  }
}


// Round markers reclaim every object below the round directory, without matching sibling prefixes.
TEST_F(TestSSMarkSweepGC, diagnose_task_cleanup_round_directory)
{
  const int64_t ROUND_ID = 1;
  const int64_t LATE_ROUND_ID = 2;
  const int64_t KEEP_ROUND_ID = 10;
  volatile bool is_stopped = false;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagMgr late_mgr;
  ObSSMarkSweepDiagMgr kept_mgr;
  ObSSMarkSweepDiagRecordCleanupTask cleanup(is_stopped);
  ObSSMarkSweepDiagCleanupTask dir_cleanup(is_stopped);
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<int64_t> record_lens;
  ObArray<SCN> scns;
  ObString data;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  ObSharedGCTask round_task;
  ObSharedGCTask task;
  ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, ROUND_ID, 0);
  MacroBlockId study_id;
  MacroBlockId late_study_id;
  MacroBlockId del_id;
  MacroBlockId kept_id;
  MacroBlockId extra_id;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  // Do not delete ledgers that may belong to other cases; use only the primary keys
  // confirmed absent below.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  key.round_id_ = LATE_ROUND_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  key.round_id_ = KEEP_ROUND_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, LATE_ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, KEEP_ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, 1));
  ASSERT_EQ(OB_SUCCESS, late_mgr.init(tenant_id_, LATE_ROUND_ID, 1));
  ASSERT_EQ(OB_SUCCESS, kept_mgr.init(tenant_id_, KEEP_ROUND_ID, 1));
  mgr.set_diag_enabled(true);
  late_mgr.set_diag_enabled(true);
  kept_mgr.set_diag_enabled(true);
  ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, late_mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, kept_mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, build_study_bucket_(0, 1, 1, allocator, data, record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.complete_study_results(0));
  ASSERT_EQ(OB_SUCCESS, late_mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_EQ(OB_SUCCESS, kept_mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_EQ(OB_SUCCESS, kept_mgr.complete_study_results(0));
  ASSERT_EQ(OB_SUCCESS, scns.push_back(SCN::min_scn()));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, mgr.complete());
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(ROUND_ID,
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0, study_id));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(LATE_ROUND_ID,
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0, late_study_id));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(ROUND_ID,
      ObMarkSweepGroupType::DEL, ObSSMarkSweepDiagFileType::TABLET_META_SCNS, 0, del_id));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(KEEP_ROUND_ID,
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::STUDY_RESULTS, 0, kept_id));
  // No object enumeration is needed in the marker, including for additional buckets.
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::get_diag_macro_id(ROUND_ID,
      ObMarkSweepGroupType::KEPT, ObSSMarkSweepDiagFileType::TABLET_META_SCNS, 99, extra_id));
  ASSERT_NO_FATAL_FAILURE(write_macro_block_(extra_id));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(study_id, true/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(late_study_id, false/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(del_id, true/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(extra_id, true/*expected_exist*/));
  key.round_id_ = ROUND_ID;
  ASSERT_EQ(OB_SUCCESS,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), round_task));
  ASSERT_EQ(OB_SUCCESS, cleanup.init(tenant_id_));
  ATOMIC_STORE(&is_stopped, true);
  ASSERT_EQ(OB_SERVICE_STOPPED, cleanup.cleanup_task_(round_task));
  ATOMIC_STORE(&is_stopped, false);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(study_id, true/*expected_exist*/));
  ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_stale_rounds_by_task_(KEEP_ROUND_ID));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(study_id, false/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(del_id, false/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(extra_id, false/*expected_exist*/));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true/*expected_exist*/));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  key.round_id_ = LATE_ROUND_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  // A writer that passed the marker check before cleanup can still complete after its
  // marker is gone.
  ASSERT_EQ(OB_SUCCESS, late_mgr.complete_study_results(0));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(late_study_id, true/*expected_exist*/));
  ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_stale_rounds_by_task_(KEEP_ROUND_ID));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(late_study_id, true/*expected_exist*/));
  // The directory-list fallback must reclaim the markerless object.
  ASSERT_EQ(OB_SUCCESS, dir_cleanup.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, dir_cleanup.cleanup_stale_rounds_by_dir_(KEEP_ROUND_ID));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(late_study_id, false/*expected_exist*/));
  ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_task_(round_task));
  // A round may have registered successfully without ever publishing an object.
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_task_(round_task));
  key.round_id_ = ROUND_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  ASSERT_EQ(OB_SUCCESS, cleanup.cleanup_stale_rounds_by_task_(KEEP_ROUND_ID));
  key.round_id_ = KEEP_ROUND_ID;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_task(key, allocator, sql_proxy(), task));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true/*expected_exist*/));
}

// Exact-type, mixed-type, and old-round queries share the existing virtual-table directory listing and multi-entry expand path.
TEST_F(TestSSMarkSweepGC, diagnose_study_virtual_table_queries)
{
  const int64_t ROUND_ID = 22004;
  const int64_t OLD_ROUND_ID = 22005;
  const int64_t TABLET_COUNT = 4;
  ObTenantSwitchGuard guard;
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagMgr old_mgr;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTest"));
  ObArray<int64_t> record_lens;
  ObArray<SCN> scns;
  ObString data;
  ObSSGCTablet tablet(ObLSID(1001), ObTabletID(800000), SCN::min_scn());
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, OLD_ROUND_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id_, ROUND_ID, 4));
  mgr.set_diag_enabled(true);
  ASSERT_EQ(OB_SUCCESS, mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, old_mgr.init(tenant_id_, OLD_ROUND_ID, 4));
  old_mgr.set_diag_enabled(true);
  ASSERT_EQ(OB_SUCCESS, old_mgr.check_diag_task());
  ASSERT_EQ(OB_SUCCESS, build_study_bucket_(0, 4, TABLET_COUNT, allocator, data, record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablets_study_results(
      0, data.ptr(), data.length(), record_lens));
  ASSERT_EQ(OB_SUCCESS, mgr.complete_study_results(0));
  ASSERT_EQ(OB_SUCCESS, scns.push_back(SCN::min_scn()));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, mgr.complete());
  ASSERT_EQ(OB_SUCCESS, old_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, old_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT, tablet, scns));
  ASSERT_EQ(OB_SUCCESS, old_mgr.complete());

  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      ROUND_ID, "AND type = 'study_results'", count));
  ASSERT_EQ(TABLET_COUNT * 3, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(ROUND_ID, "", count));
  ASSERT_EQ(TABLET_COUNT * 3 + 2, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(ROUND_ID, "AND `group` = 'kept'", count));
  ASSERT_EQ(TABLET_COUNT * 3 + 1, count);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      get_study_diagnose_row_count_(ROUND_ID, "AND `group` = 'study'", count));
  ASSERT_EQ(OB_INVALID_ARGUMENT, get_study_diagnose_row_count_(
      ROUND_ID, "AND `group` = 'study' AND type = 'study_results'", count));
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      ROUND_ID, "AND type = 'tablet_meta_scns'", count));
  ASSERT_EQ(2, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      ROUND_ID, "AND `group` = 'del' AND type = 'study_results'", count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      ROUND_ID, "AND `group` = 'kept' AND type = 'study_results'", count));
  ASSERT_EQ(TABLET_COUNT * 3, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(OLD_ROUND_ID, "", count));
  ASSERT_EQ(2, count);
  ASSERT_EQ(OB_SUCCESS, get_study_diagnose_row_count_(
      OLD_ROUND_ID, "AND type = 'study_results'", count));
  ASSERT_EQ(0, count);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  char *log_level = (char *)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  memset(buf, 0, sizeof(buf));
  oceanbase::common::databuff_printf(
      buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns,
      oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while (EOF != (c = getopt(argc, argv, "l:"))) {
    switch (c) {
    case 'l':
      log_level = optarg;
      oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
      break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

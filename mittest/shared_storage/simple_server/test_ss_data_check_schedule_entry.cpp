/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#define private public
#define protected public
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_gc_task_runner.h"
#undef protected
#undef private

#include "logservice/ob_log_service.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{

char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{

using namespace common;
using namespace share;

class ScopedDataCheckAdaptiveLimit final
{
public:
  ScopedDataCheckAdaptiveLimit(
      ObTenantDagScheduler &scheduler,
      const int64_t old_limit)
    : scheduler_(scheduler),
      old_limit_(old_limit),
      need_restore_(false)
  {}

  ~ScopedDataCheckAdaptiveLimit()
  {
    const int ret = restore();
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to restore data check adaptive limit", KR(ret),
          K_(old_limit));
    }
  }

  int freeze()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(need_restore_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("data check adaptive limit is already frozen", KR(ret));
    } else if (OB_FAIL(scheduler_.set_adaptive_limit(
        ObDagPrio::DAG_PRIO_SS_DATA_CHECK, 0))) {
      LOG_WARN("failed to freeze data check adaptive limit", KR(ret));
    } else {
      need_restore_ = true;
    }
    return ret;
  }

  int restore()
  {
    int ret = OB_SUCCESS;
    if (need_restore_) {
      if (OB_FAIL(scheduler_.set_adaptive_limit(
          ObDagPrio::DAG_PRIO_SS_DATA_CHECK, old_limit_))) {
        LOG_WARN("failed to restore data check adaptive limit", KR(ret),
            K_(old_limit));
      } else {
        need_restore_ = false;
        scheduler_.notify();
      }
    }
    return ret;
  }

private:
  ObTenantDagScheduler &scheduler_;
  const int64_t old_limit_;
  bool need_restore_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckAdaptiveLimit);
};

class ScopedDataCheckRetryState final
{
public:
  explicit ScopedDataCheckRetryState(ObSSDataCheckGenTask &task)
    : task_(task),
      old_retry_not_before_us_(0),
      old_retry_count_(0)
  {
    ObSpinLockGuard guard(task_.retry_lock_);
    old_retry_not_before_us_ = task_.retry_not_before_us_;
    old_retry_count_ = task_.retry_count_;
    task_.retry_not_before_us_ = 0;
    task_.retry_count_ = 0;
  }

  ~ScopedDataCheckRetryState()
  {
    ObSpinLockGuard guard(task_.retry_lock_);
    task_.retry_not_before_us_ = old_retry_not_before_us_;
    task_.retry_count_ = old_retry_count_;
  }

  void get(int64_t &retry_not_before_us, int64_t &retry_count) const
  {
    ObSpinLockGuard guard(task_.retry_lock_);
    retry_not_before_us = task_.retry_not_before_us_;
    retry_count = task_.retry_count_;
  }

private:
  ObSSDataCheckGenTask &task_;
  int64_t old_retry_not_before_us_;
  int64_t old_retry_count_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckRetryState);
};

class ScopedGCTaskRunnerCleanup final
{
public:
  explicit ScopedGCTaskRunnerCleanup(ObSSGCTaskRunner &runner)
    : runner_(runner),
      is_started_(false),
      need_cleanup_(true)
  {}

  ~ScopedGCTaskRunnerCleanup()
  {
    cleanup();
  }

  void mark_started()
  {
    is_started_ = true;
  }

  void cleanup()
  {
    if (need_cleanup_) {
      if (is_started_) {
        runner_.stop();
      }
      runner_.destroy();
      is_started_ = false;
      need_cleanup_ = false;
    }
  }

private:
  ObSSGCTaskRunner &runner_;
  bool is_started_;
  bool need_cleanup_;
  DISALLOW_COPY_AND_ASSIGN(ScopedGCTaskRunnerCleanup);
};

class ScopedDataCheckDagCleanup final
{
public:
  ScopedDataCheckDagCleanup(
      ObTenantDagScheduler &scheduler,
      const uint64_t tenant_id)
    : scheduler_(scheduler),
      tenant_id_(tenant_id),
      need_cleanup_(true)
  {}

  ~ScopedDataCheckDagCleanup()
  {
    const int ret = cleanup();
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to clean residual data check DAG", KR(ret),
          K_(tenant_id));
    }
  }

  void dismiss()
  {
    need_cleanup_ = false;
  }

  int cleanup()
  {
    int ret = OB_SUCCESS;
    if (need_cleanup_) {
      ObSSDataCheckDag probe;
      bool is_exist = false;
      bool is_emergency = false;
      probe.set_tenant_id(tenant_id_);
      if (OB_FAIL(scheduler_.check_dag_exist(
          &probe, is_exist, is_emergency))) {
        LOG_WARN("failed to check residual data check DAG", KR(ret),
            K_(tenant_id));
      } else if (is_exist) {
        if (OB_FAIL(scheduler_.cancel_dag(
            &probe, true /*force_cancel*/))) {
          LOG_WARN("failed to cancel residual data check DAG", KR(ret),
              K_(tenant_id));
        } else {
          scheduler_.notify();
          const int64_t deadline = ObTimeUtility::current_time()
              + 30L * 1000L * 1000L;
          while (OB_SUCC(ret) && is_exist
              && ObTimeUtility::current_time() < deadline) {
            if (OB_FAIL(scheduler_.check_dag_exist(
                &probe, is_exist, is_emergency))) {
              LOG_WARN("failed to wait residual data check DAG", KR(ret),
                  K_(tenant_id));
            } else if (is_exist) {
              ob_usleep(100L * 1000L);
            }
          }
          if (OB_SUCC(ret) && is_exist) {
            ret = OB_TIMEOUT;
            LOG_WARN("residual data check DAG cleanup timed out", KR(ret),
                K_(tenant_id));
          }
        }
      }
      if (OB_SUCC(ret) && !is_exist) {
        need_cleanup_ = false;
        scheduler_.notify();
      }
    }
    return ret;
  }

private:
  ObTenantDagScheduler &scheduler_;
  const uint64_t tenant_id_;
  bool need_cleanup_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckDagCleanup);
};

struct DataCheckFrozenParam final
{
  DataCheckFrozenParam()
    : snapshot_meta_scn_(),
      check_mode_(ObSSDataCheckMode::MAX),
      memory_limit_(0),
      file_buf_size_(0),
      io_expire_timestamp_(-1)
  {}

  SCN snapshot_meta_scn_;
  ObSSDataCheckMode check_mode_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
};

struct DataCheckResultEvent final
{
  DataCheckResultEvent()
    : snapshot_meta_scn_(0),
      check_mode_(),
      status_()
  {}

  uint64_t snapshot_meta_scn_;
  std::string check_mode_;
  std::string status_;
};

class TestSSDataCheckScheduleEntry : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t DAG_TIMEOUT_US = 300L * 1000L * 1000L;
  static constexpr int64_t EVENT_TIMEOUT_US = 30L * 1000L * 1000L;

  TestSSDataCheckScheduleEntry()
    : unittest::ObSimpleClusterTestBase(
        "test_ss_data_check_schedule_entry", "50G", "50G", "50G")
  {}

  virtual ~TestSSDataCheckScheduleEntry() = default;

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    unittest::ObSimpleClusterTestBase::TearDownTestCase();
  }

  int wait_sys_ls_leader_();
  int wait_data_check_dag_(
      ObTenantDagScheduler &scheduler,
      const uint64_t tenant_id,
      const bool expected_exist,
      const int64_t timeout_us);
  int read_queued_param_(
      ObTenantDagScheduler &scheduler,
      const uint64_t tenant_id,
      DataCheckFrozenParam &param);
  int wait_result_event_(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      DataCheckResultEvent &event);
  int get_result_event_count_(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const uint64_t snapshot_meta_scn,
      int64_t &event_count);
  int wait_submit_event_count_(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const int64_t expected_count,
      int64_t &event_count);
};

class ScopedDataCheckTestTenantCleanup final
{
public:
  ScopedDataCheckTestTenantCleanup(
      TestSSDataCheckScheduleEntry &test,
      const char *tenant_name)
    : test_(test), tenant_name_(tenant_name), need_cleanup_(true)
  {}

  ~ScopedDataCheckTestTenantCleanup()
  {
    const int ret = cleanup();
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to cleanup data check test tenant",
          KR(ret), K_(tenant_name));
    }
  }

  int cleanup()
  {
    int ret = OB_SUCCESS;
    if (need_cleanup_) {
      if (OB_FAIL(test_.delete_tenant(tenant_name_))) {
        LOG_WARN("failed to delete data check test tenant",
            KR(ret), K_(tenant_name));
      } else {
        need_cleanup_ = false;
      }
    }
    return ret;
  }

private:
  TestSSDataCheckScheduleEntry &test_;
  const char *tenant_name_;
  bool need_cleanup_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckTestTenantCleanup);
};

int TestSSDataCheckScheduleEntry::wait_sys_ls_leader_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
  const int64_t deadline = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant log service is null", KR(ret));
  }
  while (OB_SUCC(ret) && !is_leader
      && ObTimeUtility::current_time() < deadline) {
    ObRole role = INVALID_ROLE;
    int64_t proposal_id = -1;
    if (OB_FAIL(log_service->get_palf_role(SYS_LS, role, proposal_id))) {
      if (OB_LS_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get SYS_LS role", KR(ret));
      }
    } else {
      is_leader = is_strong_leader(role);
    }
    if (OB_SUCC(ret) && !is_leader) {
      ob_usleep(10L * 1000L);
    }
  }
  if (OB_SUCC(ret) && !is_leader) {
    ret = OB_TIMEOUT;
    LOG_WARN("SYS_LS strong leader election timed out", KR(ret));
  }
  return ret;
}

int TestSSDataCheckScheduleEntry::wait_data_check_dag_(
    ObTenantDagScheduler &scheduler,
    const uint64_t tenant_id,
    const bool expected_exist,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_exist = !expected_exist;
  bool is_emergency = false;
  ObSSDataCheckDag probe;
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  probe.set_tenant_id(tenant_id);
  while (OB_SUCC(ret) && is_exist != expected_exist
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(scheduler.check_dag_exist(
        &probe, is_exist, is_emergency))) {
      LOG_WARN("failed to check data check DAG existence", KR(ret),
          K(tenant_id));
    } else if (is_exist != expected_exist) {
      ob_usleep(100L * 1000L);
    }
  }
  if (OB_SUCC(ret) && is_exist != expected_exist) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check DAG state wait timed out", KR(ret),
        K(tenant_id), K(expected_exist), K(is_exist));
  }
  return ret;
}

int TestSSDataCheckScheduleEntry::read_queued_param_(
    ObTenantDagScheduler &scheduler,
    const uint64_t tenant_id,
    DataCheckFrozenParam &param)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckDag probe;
  probe.set_tenant_id(tenant_id);
  ObDagPrioScheduler &prio_scheduler =
      scheduler.prio_sche_[ObDagPrio::DAG_PRIO_SS_DATA_CHECK];
  SpinRLockGuard guard(prio_scheduler.prio_rwlock_);
  ObIDag *stored_dag = nullptr;
  if (OB_FAIL(prio_scheduler.get_stored_dag_(probe, stored_dag))) {
    LOG_WARN("failed to get queued data check DAG", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(stored_dag)
      || OB_UNLIKELY(ObDagType::DAG_TYPE_SS_DATA_CHECK
          != stored_dag->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid queued data check DAG", KR(ret),
        KP(stored_dag), K(tenant_id));
  } else {
    ObSSDataCheckDag *data_check_dag =
        static_cast<ObSSDataCheckDag *>(stored_dag);
    ObSSDataCheckCtx *ctx = data_check_dag->get_ctx();
    if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("queued data check context is invalid", KR(ret),
          KP(ctx), K(tenant_id));
    } else {
      param.snapshot_meta_scn_ = ctx->get_snapshot_meta_scn();
      param.check_mode_ = ctx->get_check_mode();
      param.memory_limit_ = ctx->get_memory_limit();
      param.file_buf_size_ = ctx->get_file_buf_size();
      param.io_expire_timestamp_ = ctx->get_io_expire_timestamp();
    }
  }
  return ret;
}

int TestSSDataCheckScheduleEntry::wait_result_event_(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    DataCheckResultEvent &event)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline = ObTimeUtility::current_time() + EVENT_TIMEOUT_US;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt(
      "SELECT CAST(value2 AS UNSIGNED) AS snapshot_meta_scn, "
      "value3 AS check_mode, value4 AS status FROM oceanbase.%s "
      "WHERE time_to_usec(gmt_create) >= %ld "
      "AND module = 'SHARED_GC' AND event = 'DATA_CHECK_RESULT' "
      "AND name1 = 'tenant_id' AND value1 = '%lu' "
      "ORDER BY gmt_create DESC LIMIT 1",
      OB_ALL_SERVER_EVENT_HISTORY_TNAME, begin_ts, tenant_id))) {
    LOG_WARN("failed to build data check result event query", KR(ret));
  }
  while (OB_SUCC(ret) && !found
      && ObTimeUtility::current_time() < deadline) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
      ObMySQLResult *result = nullptr;
      ObString check_mode;
      ObString status;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          result_holder, sql.ptr()))) {
        LOG_WARN("failed to query data check result event", KR(ret), K(sql));
      } else if (OB_ISNULL(result = result_holder.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data check result event query result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to read data check result event", KR(ret), K(sql));
        }
      } else if (OB_FAIL(result->get_uint(
          "snapshot_meta_scn", event.snapshot_meta_scn_))) {
        LOG_WARN("failed to read result snapshot meta scn", KR(ret));
      } else if (OB_FAIL(result->get_varchar("check_mode", check_mode))) {
        LOG_WARN("failed to read result check mode", KR(ret));
      } else if (OB_FAIL(result->get_varchar("status", status))) {
        LOG_WARN("failed to read data check status", KR(ret));
      } else {
        event.check_mode_.assign(check_mode.ptr(), check_mode.length());
        event.status_.assign(status.ptr(), status.length());
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ob_usleep(100L * 1000L);
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check result event wait timed out", KR(ret),
        K(begin_ts), K(tenant_id), K(sql));
  }
  return ret;
}

int TestSSDataCheckScheduleEntry::get_result_event_count_(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const uint64_t snapshot_meta_scn,
    int64_t &event_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  event_count = 0;
  if (OB_UNLIKELY(begin_ts < 0
      || OB_INVALID_ID == tenant_id
      || OB_INVALID_SCN_VAL == snapshot_meta_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check result event count argument", KR(ret),
        K(begin_ts), K(tenant_id), K(snapshot_meta_scn));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT COUNT(*) AS cnt FROM oceanbase.%s "
      "WHERE time_to_usec(gmt_create) >= %ld "
      "AND module = 'SHARED_GC' AND event = 'DATA_CHECK_RESULT' "
      "AND name1 = 'tenant_id' AND value1 = '%lu' "
      "AND name2 = 'snapshot_meta_scn' AND value2 = '%lu'",
      OB_ALL_SERVER_EVENT_HISTORY_TNAME, begin_ts,
      tenant_id, snapshot_meta_scn))) {
    LOG_WARN("failed to build data check result event count query", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          result_holder, sql.ptr()))) {
        LOG_WARN("failed to query data check result event count", KR(ret), K(sql));
      } else if (OB_ISNULL(result = result_holder.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data check result event count query result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to read data check result event count", KR(ret));
      } else if (OB_FAIL(result->get_int("cnt", event_count))) {
        LOG_WARN("failed to extract data check result event count", KR(ret));
      }
    }
  }
  return ret;
}

int TestSSDataCheckScheduleEntry::wait_submit_event_count_(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const int64_t expected_count,
    int64_t &event_count)
{
  int ret = OB_SUCCESS;
  bool matched = false;
  const int64_t deadline = ObTimeUtility::current_time() + EVENT_TIMEOUT_US;
  ObSqlString sql;
  event_count = 0;
  if (OB_UNLIKELY(expected_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected submit event count", KR(ret), K(expected_count));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT COUNT(*) AS cnt FROM oceanbase.%s "
      "WHERE time_to_usec(gmt_create) >= %ld "
      "AND module = 'SHARED_GC' AND event = 'SUBMIT_DATA_CHECK_DAG' "
      "AND name1 = 'tenant_id' AND value1 = '%lu'",
      OB_ALL_SERVER_EVENT_HISTORY_TNAME, begin_ts, tenant_id))) {
    LOG_WARN("failed to build data check submit event query", KR(ret));
  }
  while (OB_SUCC(ret) && !matched
      && ObTimeUtility::current_time() < deadline) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          result_holder, sql.ptr()))) {
        LOG_WARN("failed to query data check submit event", KR(ret), K(sql));
      } else if (OB_ISNULL(result = result_holder.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data check submit event query result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to read data check submit event count", KR(ret));
      } else if (OB_FAIL(result->get_int("cnt", event_count))) {
        LOG_WARN("failed to extract data check submit event count", KR(ret));
      } else {
        matched = event_count == expected_count;
      }
    }
    if (OB_SUCC(ret) && !matched) {
      ob_usleep(100L * 1000L);
    }
  }
  if (OB_SUCC(ret) && !matched) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check submit event count wait timed out", KR(ret),
        K(begin_ts), K(tenant_id), K(expected_count), K(event_count));
  }
  return ret;
}

TEST_F(TestSSDataCheckScheduleEntry,
    user_tenant_snapshot_reservation_is_removed_in_same_transaction)
{
  const char *tenant_name = "data_check_snapshot_barrier";
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(
      tenant_name, "4G", "4G", false /*oracle_mode*/, 2 /*tenant_cpu*/));
  ScopedDataCheckTestTenantCleanup tenant_cleanup(*this, tenant_name);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  ASSERT_TRUE(is_user_tenant(tenant_id));

  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

    volatile bool is_stopped = false;
    ObSSDataCheckGenTask task(is_stopped);
    SCN snapshot_meta_scn;
    ASSERT_EQ(OB_SUCCESS, task.init(tenant_id));
    ASSERT_EQ(OB_SUCCESS, task.get_shared_meta_snapshot_(snapshot_meta_scn));
    ASSERT_TRUE(snapshot_meta_scn.is_valid());

    ObSharedMetaReservedOperator op;
    ASSERT_EQ(OB_SUCCESS, op.init(
        get_curr_simple_server().get_observer().get_mysql_proxy(),
        10L * 1000L * 1000L));
    const ObSharedMetaReservedType data_check_type(
        ObSharedMetaReservedTypeEnum::DATA_CHECK);
    ObArray<ObSharedMetaReservedInfo> data_check_rows;
    ASSERT_EQ(OB_SUCCESS, op.scan_rows(
        tenant_id, data_check_type, 0 /*limit_count*/, data_check_rows));
    ASSERT_EQ(0, data_check_rows.count());

    const ObSharedMetaReservedType max_type(
        ObSharedMetaReservedTypeEnum::MAX_RESERVED);
    ObArray<ObSharedMetaReservedInfo> max_rows;
    ASSERT_EQ(OB_SUCCESS, op.scan_rows(
        tenant_id, max_type, 0 /*limit_count*/, max_rows));
    ASSERT_EQ(1, max_rows.count());
    ASSERT_LE(snapshot_meta_scn, max_rows.at(0).meta_scn_);
  }
  ASSERT_EQ(OB_SUCCESS, tenant_cleanup.cleanup());
}

TEST_F(TestSSDataCheckScheduleEntry,
    user_tenant_gen_task_freezes_barrier_snapshot_into_real_dag)
{
  const char *tenant_name = "data_check_user_submit";
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(
      tenant_name, "4G", "4G", false /*oracle_mode*/, 2 /*tenant_cpu*/));
  ScopedDataCheckTestTenantCleanup tenant_cleanup(*this, tenant_name);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  ASSERT_TRUE(is_user_tenant(tenant_id));

  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    ASSERT_NE(nullptr, scheduler);
    ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
        *scheduler, tenant_id, false, 10L * 1000L * 1000L));

    int64_t old_limit = 0;
    ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
        ObDagPrio::DAG_PRIO_SS_DATA_CHECK, old_limit));
    ASSERT_GT(old_limit, 0);
    ScopedDataCheckAdaptiveLimit limit_guard(*scheduler, old_limit);
    ASSERT_EQ(OB_SUCCESS, limit_guard.freeze());
    // DAG清理对象后声明，保证异常返回时先取消DAG，再恢复执行优先级。
    ScopedDataCheckDagCleanup dag_cleanup(*scheduler, tenant_id);

    volatile bool is_stopped = false;
    ObSSDataCheckGenTask task(is_stopped);
    ASSERT_EQ(OB_SUCCESS, task.init(tenant_id));
    task.commit_gc_dag_schedule_time_(0);
    const uint64_t run_count_before = task.run_cnt_;
    task.runTimerTask();
    ASSERT_EQ(run_count_before + 1, task.run_cnt_);
    ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
        task.dag_schedule_controller_.state());
    ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
        *scheduler, tenant_id, true, 10L * 1000L * 1000L));

    DataCheckFrozenParam frozen_param;
    ASSERT_EQ(OB_SUCCESS,
        read_queued_param_(*scheduler, tenant_id, frozen_param));
    ASSERT_TRUE(frozen_param.snapshot_meta_scn_.is_valid());
    EXPECT_EQ(ObSSDataCheckMode::SINGLE, frozen_param.check_mode_);
    EXPECT_EQ(16L << 20, frozen_param.memory_limit_);
    EXPECT_EQ(32L << 10, frozen_param.file_buf_size_);
    EXPECT_EQ(0, frozen_param.io_expire_timestamp_);

    ObSharedMetaReservedOperator reserved_op;
    ASSERT_EQ(OB_SUCCESS, reserved_op.init(
        get_curr_simple_server().get_observer().get_mysql_proxy(),
        10L * 1000L * 1000L));
    const ObSharedMetaReservedType data_check_type(
        ObSharedMetaReservedTypeEnum::DATA_CHECK);
    ObArray<ObSharedMetaReservedInfo> data_check_rows;
    ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
        tenant_id, data_check_type, 0 /*limit_count*/, data_check_rows));
    EXPECT_EQ(0, data_check_rows.count());

    const ObSharedMetaReservedType max_type(
        ObSharedMetaReservedTypeEnum::MAX_RESERVED);
    ObArray<ObSharedMetaReservedInfo> max_rows;
    ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
        tenant_id, max_type, 0 /*limit_count*/, max_rows));
    ASSERT_EQ(1, max_rows.count());
    EXPECT_LE(frozen_param.snapshot_meta_scn_, max_rows.at(0).meta_scn_);

    // 优先级仍为0时取消DAG，避免恢复后真正执行Expected收集和LIST。
    ASSERT_EQ(OB_SUCCESS, dag_cleanup.cleanup());
    ASSERT_EQ(OB_SUCCESS, limit_guard.restore());
  }
  ASSERT_EQ(OB_SUCCESS, tenant_cleanup.cleanup());
}

TEST_F(TestSSDataCheckScheduleEntry,
    runner_owned_gen_task_submits_and_freezes_real_dag)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_NE(nullptr, meta_service);
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, 10L * 1000L * 1000L));

  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, old_limit));
  ASSERT_GT(old_limit, 0);
  ScopedDataCheckAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.freeze());

  ObSSGCTaskRunner runner;
  ScopedDataCheckDagCleanup dag_cleanup(*scheduler, tenant_id);
  ScopedGCTaskRunnerCleanup runner_cleanup(runner);
  ASSERT_EQ(OB_SUCCESS, runner.init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, runner.start());
  runner_cleanup.mark_started();

  // Runner 启动后立即停掉测试实例的两个定时线程，避免其他 GC 定时任务
  // 干扰本用例；is_stopped_ 保持 false，随后只触发其持有的 DataCheck Task。
  runner.stop_timer_groups_();
  runner.wait_timer_groups_();
  ASSERT_FALSE(runner.is_stopped_);

  SCN lower_snapshot;
  SCN upper_snapshot;
  ASSERT_EQ(OB_SUCCESS,
      meta_service->get_max_committed_meta_scn(lower_snapshot));
  ASSERT_TRUE(lower_snapshot.is_valid());

  runner.data_check_gen_task_.reset_dag_schedule_controller_();
  runner.data_check_gen_task_.commit_gc_dag_schedule_time_(0);
  const uint64_t run_count_before = runner.data_check_gen_task_.run_cnt_;
  const int64_t begin_ts = ObTimeUtility::current_time();
  runner.data_check_gen_task_.runTimerTask();
  ASSERT_EQ(OB_SUCCESS,
      meta_service->get_max_committed_meta_scn(upper_snapshot));
  ASSERT_TRUE(upper_snapshot.is_valid());
  ASSERT_EQ(run_count_before + 1, runner.data_check_gen_task_.run_cnt_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, true, 10L * 1000L * 1000L));

  DataCheckFrozenParam frozen_param;
  ASSERT_EQ(OB_SUCCESS,
      read_queued_param_(*scheduler, tenant_id, frozen_param));
  EXPECT_TRUE(frozen_param.snapshot_meta_scn_.is_valid());
  EXPECT_LE(lower_snapshot.get_val_for_inner_table_field(),
      frozen_param.snapshot_meta_scn_.get_val_for_inner_table_field());
  EXPECT_LE(frozen_param.snapshot_meta_scn_.get_val_for_inner_table_field(),
      upper_snapshot.get_val_for_inner_table_field());
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, frozen_param.check_mode_);
  EXPECT_EQ(16L << 20, frozen_param.memory_limit_);
  EXPECT_EQ(32L << 10, frozen_param.file_buf_size_);
  EXPECT_EQ(0, frozen_param.io_expire_timestamp_);

  // 优先级被冻结时 DAG 保持驻留；第二次触发只能探测到已有 DAG，
  // 不得重新冻结快照或增加实际提交计数。
  runner.data_check_gen_task_.runTimerTask();
  EXPECT_EQ(run_count_before + 1, runner.data_check_gen_task_.run_cnt_);
  EXPECT_EQ(1, scheduler->get_dag_count(
      ObDagType::DAG_TYPE_SS_DATA_CHECK));

  // DAG 提交后停止并销毁 Runner，再放开执行，验证 DAG 执行期不依赖
  // Runner 生命周期；真实切主场景需要由多副本测试覆盖。
  runner_cleanup.cleanup();
  ASSERT_EQ(OB_SUCCESS, limit_guard.restore());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, DAG_TIMEOUT_US));
  dag_cleanup.dismiss();

  DataCheckResultEvent result_event;
  ASSERT_EQ(OB_SUCCESS,
      wait_result_event_(begin_ts, tenant_id, result_event));
  EXPECT_EQ(frozen_param.snapshot_meta_scn_.get_val_for_inner_table_field(),
      result_event.snapshot_meta_scn_);
  EXPECT_EQ("SINGLE", result_event.check_mode_);
  EXPECT_TRUE("HEALTH" == result_event.status_
      || "NOT_HEALTH" == result_event.status_);

  int64_t submit_event_count = 0;
  ASSERT_EQ(OB_SUCCESS, wait_submit_event_count_(
      begin_ts, tenant_id, 1, submit_event_count));
  EXPECT_EQ(1, submit_event_count);
}

TEST_F(TestSSDataCheckScheduleEntry,
    runner_stop_start_preserves_data_check_retry_state)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

  ObSSGCTaskRunner runner;
  ScopedGCTaskRunnerCleanup runner_cleanup(runner);
  ASSERT_EQ(OB_SUCCESS, runner.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, runner.start());
  runner_cleanup.mark_started();
  runner.stop_timer_groups_();
  runner.wait_timer_groups_();
  ASSERT_FALSE(runner.is_stopped_);

  ObSSDataCheckGenTask &task = runner.data_check_gen_task_;
  ScopedDataCheckRetryState retry_state_guard(task);
  const int64_t retry_finish_ts = ObTimeUtility::current_time_us();
  task.report_data_check_dag_result(true, retry_finish_ts);
  int64_t expected_retry_not_before_us = 0;
  int64_t expected_retry_count = 0;
  retry_state_guard.get(
      expected_retry_not_before_us, expected_retry_count);
  ASSERT_EQ(1, expected_retry_count);
  ASSERT_GT(expected_retry_not_before_us, retry_finish_ts);

  runner.stop();
  int64_t retry_not_before_us = 0;
  int64_t retry_count = 0;
  retry_state_guard.get(retry_not_before_us, retry_count);
  EXPECT_EQ(expected_retry_count, retry_count);
  EXPECT_EQ(expected_retry_not_before_us, retry_not_before_us);

  ASSERT_EQ(OB_SUCCESS, runner.start());
  runner.stop_timer_groups_();
  runner.wait_timer_groups_();
  ASSERT_FALSE(runner.is_stopped_);
  retry_state_guard.get(retry_not_before_us, retry_count);
  EXPECT_EQ(expected_retry_count, retry_count);
  EXPECT_EQ(expected_retry_not_before_us, retry_not_before_us);

  bool has_pending_retry = false;
  bool need_retry = true;
  ASSERT_EQ(OB_SUCCESS, task.check_gc_dag_retry_state_(
      expected_retry_not_before_us - 1, has_pending_retry, need_retry));
  EXPECT_TRUE(has_pending_retry);
  EXPECT_FALSE(need_retry);
  ASSERT_EQ(OB_SUCCESS, task.check_gc_dag_retry_state_(
      expected_retry_not_before_us, has_pending_retry, need_retry));
  EXPECT_TRUE(has_pending_retry);
  EXPECT_TRUE(need_retry);
}

TEST_F(TestSSDataCheckScheduleEntry,
    runtime_failure_reports_retry_through_real_scheduler)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_NE(nullptr, meta_service);
  ASSERT_NE(nullptr, gc_service);
  ASSERT_FALSE(gc_service->is_stopped_);
  ASSERT_FALSE(gc_service->task_runner_.is_stopped_);
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, 10L * 1000L * 1000L));

  ObSSDataCheckGenTask &receiver =
      gc_service->task_runner_.data_check_gen_task_;
  ScopedDataCheckRetryState retry_state_guard(receiver);

  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, old_limit));
  ASSERT_GT(old_limit, 0);
  ScopedDataCheckAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.freeze());

  SCN snapshot_meta_scn;
  ASSERT_EQ(OB_SUCCESS,
      meta_service->get_max_committed_meta_scn(snapshot_meta_scn));
  ASSERT_TRUE(snapshot_meta_scn.is_valid());

  const int64_t io_expire_timestamp =
      ObTimeUtility::current_time_us() - 1;
  ObSSDataCheckDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE,
      16L << 20, 32L << 10, io_expire_timestamp));
  ScopedDataCheckDagCleanup dag_cleanup(*scheduler, tenant_id);
  ASSERT_EQ(OB_SUCCESS,
      scheduler->create_and_add_dag<ObSSDataCheckDag>(&param));
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, true, 10L * 1000L * 1000L));

  DataCheckFrozenParam frozen_param;
  ASSERT_EQ(OB_SUCCESS,
      read_queued_param_(*scheduler, tenant_id, frozen_param));
  EXPECT_EQ(io_expire_timestamp, frozen_param.io_expire_timestamp_);

  const int64_t finish_lower_bound = ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS, limit_guard.restore());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, DAG_TIMEOUT_US));
  const int64_t finish_upper_bound = ObTimeUtility::current_time_us();
  dag_cleanup.dismiss();

  int64_t retry_not_before_us = 0;
  int64_t retry_count = 0;
  retry_state_guard.get(retry_not_before_us, retry_count);
  EXPECT_EQ(1, retry_count);

  int64_t exec_interval_us = 0;
  receiver.get_gc_exec_interval_us_(exec_interval_us);
  ASSERT_GT(exec_interval_us, 0);
  const int64_t retry_interval_us = MIN(
      60L * 60L * 1000L * 1000L, exec_interval_us);
  EXPECT_GE(retry_not_before_us,
      finish_lower_bound + retry_interval_us);
  EXPECT_LE(retry_not_before_us,
      finish_upper_bound + retry_interval_us);

  bool has_pending_retry = false;
  bool need_retry = true;
  ASSERT_EQ(OB_SUCCESS, receiver.check_gc_dag_retry_state_(
      finish_upper_bound, has_pending_retry, need_retry));
  EXPECT_TRUE(has_pending_retry);
  EXPECT_FALSE(need_retry);
}

TEST_F(TestSSDataCheckScheduleEntry,
    runtime_cancel_reports_retry_through_real_scheduler)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());

  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_NE(nullptr, meta_service);
  ASSERT_NE(nullptr, gc_service);
  ASSERT_FALSE(gc_service->is_stopped_);
  ASSERT_FALSE(gc_service->task_runner_.is_stopped_);
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, 10L * 1000L * 1000L));

  ObSSDataCheckGenTask &receiver =
      gc_service->task_runner_.data_check_gen_task_;
  ScopedDataCheckRetryState retry_state_guard(receiver);
  const int64_t retry_finish_ts = ObTimeUtility::current_time_us();
  receiver.report_data_check_dag_result(true, retry_finish_ts);
  int64_t retry_not_before_us = 0;
  int64_t retry_count = 0;
  retry_state_guard.get(retry_not_before_us, retry_count);
  ASSERT_EQ(1, retry_count);
  ASSERT_GT(retry_not_before_us, retry_finish_ts);

  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, old_limit));
  ASSERT_GT(old_limit, 0);
  ScopedDataCheckAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.freeze());

  SCN snapshot_meta_scn;
  ASSERT_EQ(OB_SUCCESS,
      meta_service->get_max_committed_meta_scn(snapshot_meta_scn));
  ASSERT_TRUE(snapshot_meta_scn.is_valid());
  const uint64_t snapshot_meta_scn_value =
      snapshot_meta_scn.get_val_for_inner_table_field();
  const int64_t begin_ts = ObTimeUtility::current_time();

  ObSSDataCheckDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE,
      16L << 20, 32L << 10, 0));
  ScopedDataCheckDagCleanup dag_cleanup(*scheduler, tenant_id);
  ASSERT_EQ(OB_SUCCESS,
      scheduler->create_and_add_dag<ObSSDataCheckDag>(&param));
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, true, 10L * 1000L * 1000L));

  ObSSDataCheckDag probe;
  bool is_exist = false;
  bool is_emergency = false;
  probe.set_tenant_id(tenant_id);
  ASSERT_EQ(OB_SUCCESS, scheduler->check_dag_exist(
      &probe, is_exist, is_emergency));
  ASSERT_TRUE(is_exist);
  const int64_t cancel_finish_lower_bound =
      ObTimeUtility::current_time_us();
  ASSERT_EQ(OB_SUCCESS,
      scheduler->cancel_dag(&probe, true /*force_cancel*/));
  scheduler->notify();
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      *scheduler, tenant_id, false, 10L * 1000L * 1000L));
  const int64_t cancel_finish_upper_bound =
      ObTimeUtility::current_time_us();
  dag_cleanup.dismiss();

  retry_state_guard.get(retry_not_before_us, retry_count);
  EXPECT_EQ(2, retry_count);
  int64_t exec_interval_us = 0;
  receiver.get_gc_exec_interval_us_(exec_interval_us);
  ASSERT_GT(exec_interval_us, 0);
  const int64_t retry_interval_us = MIN(
      2L * 60L * 60L * 1000L * 1000L, exec_interval_us);
  EXPECT_GE(retry_not_before_us,
      cancel_finish_lower_bound + retry_interval_us);
  EXPECT_LE(retry_not_before_us,
      cancel_finish_upper_bound + retry_interval_us);
  bool has_pending_retry = false;
  bool need_retry = true;
  ASSERT_EQ(OB_SUCCESS, receiver.check_gc_dag_retry_state_(
      ObTimeUtility::current_time_us(), has_pending_retry, need_retry));
  EXPECT_TRUE(has_pending_retry);
  EXPECT_FALSE(need_retry);

  // READY 状态取消不会执行 FinalizeTask，因此本轮不得发布业务结果。
  ob_usleep(1000L * 1000L);
  int64_t result_event_count = 0;
  ASSERT_EQ(OB_SUCCESS, get_result_event_count_(
      begin_ts, tenant_id, snapshot_meta_scn_value, result_event_count));
  EXPECT_EQ(0, result_event_count);
  ASSERT_EQ(OB_SUCCESS, limit_guard.restore());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  char storage_info[1000] = {};
  const int64_t current_time_ns =
      oceanbase::common::ObTimeUtility::current_time_ns();
  databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&"
      "max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET,
      current_time_ns,
      oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK,
      oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

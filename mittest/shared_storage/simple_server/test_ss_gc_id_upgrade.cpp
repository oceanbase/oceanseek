/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#define UNITTEST

#include <gmock/gmock.h>
#include <pthread.h>
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_check_stop_provider.h"
#include "share/ob_upgrade_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "storage/ss_garbage_collector/ob_ss_gc_id_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
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

class TestGCUpgradeStopProvider final : public ObCheckStopProvider
{
public:
  virtual int check_stop() const override { return OB_SUCCESS; }
};

class MockSSMarkSweepGCGenTask : public ObSSMarkSweepGCGenTask
{
public:
  explicit MockSSMarkSweepGCGenTask(volatile bool &is_stopped)
      : ObSSMarkSweepGCGenTask(is_stopped) {}
  virtual ~MockSSMarkSweepGCGenTask() = default;
  MOCK_METHOD1(check_dag_exist_, int(bool &is_exist));
  MOCK_METHOD0(cancel_dag_, int());
};

// GC ID allocation, migration and takeover tests in an isolated USER/META pair.
class TestSSGCIdUpgrade : public ObSimpleClusterTestBase
{
public:
  TestSSGCIdUpgrade()
    : ObSimpleClusterTestBase("test_ss_gc_id_upgrade", "50G", "50G", "50G") {}

  // User tenant id, set once in setup_tenant and shared across tests.
  static uint64_t tenant_id_;

  // sys-level SQL proxy — routes to any tenant via the routing tenant_id arg.
  ObMySQLProxy &sql_proxy()
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

  struct GCIdStatState
  {
    GCIdStatState() : tenant_id_(OB_INVALID_TENANT_ID), name_(nullptr), exists_(false), value_(0) {}
    uint64_t tenant_id_;
    const char *name_;
    bool exists_;
    uint64_t value_;
  };

  struct GCUpgradeState
  {
    GCUpgradeState() : gc_enabled_(false), runner_started_(false), saved_(false) {}
    GCIdStatState stats_[10];
    ObSharedGCControlInfo control_;
    ObArray<ObSharedGCTaskKey> task_keys_;
    bool gc_enabled_;
    bool runner_started_;
    bool saved_;
  };

  struct GCIdFetchContext
  {
    GCIdFetchContext()
        : tenant_id_(OB_INVALID_TENANT_ID), start_(nullptr), ret_(OB_ERR_UNEXPECTED), id_(0) {}
    uint64_t tenant_id_;
    volatile bool *start_;
    int ret_;
    int64_t id_;
  };

  static void *fetch_gc_id_thread_(void *arg)
  {
    GCIdFetchContext *ctx = static_cast<GCIdFetchContext *>(arg);
    share::ObTenantSwitchGuard guard;
    if (nullptr != ctx && nullptr != ctx->start_) {
      ctx->ret_ = guard.switch_to(ctx->tenant_id_);
      while (!ATOMIC_LOAD(ctx->start_)) {
        ob_usleep(100);
      }
      if (OB_SUCCESS == ctx->ret_) {
        ctx->ret_ = ObSSGCIdUtil::fetch_gc_id(
            ctx->tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE, ctx->id_);
      }
    }
    return nullptr;
  }

  static int get_gc_id_types_(
      const int64_t index,
      ObMaxIdType &old_type,
      ObMaxIdType &new_type)
  {
    int ret = OB_SUCCESS;
    old_type = OB_MAX_ID_TYPE;
    new_type = OB_MAX_ID_TYPE;
    switch (index) {
      case 0: {
        old_type = OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE;
        new_type = OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE;
        break;
      }
      case 1: {
        old_type = OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE;
        new_type = OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_TASK_ID_TYPE;
        break;
      }
      case 2: {
        old_type = OB_MAX_USED_SHARED_FAIL_GC_TASK_ID_TYPE;
        new_type = OB_MAX_USED_SHARED_USER_FAIL_GC_TASK_ID_TYPE;
        break;
      }
      case 3: {
        old_type = OB_MAX_USED_SHARED_FAIL_GC_MACRO_ID_TASK_ID_TYPE;
        new_type = OB_MAX_USED_SHARED_USER_FAIL_GC_MACRO_ID_TASK_ID_TYPE;
        break;
      }
      case 4: {
        old_type = OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE;
        new_type = OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }
    return ret;
  }

  int read_gc_stat_(GCIdStatState &state)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    state.exists_ = false;
    state.value_ = 0;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      ObString value;
      const uint64_t stored_tenant_id = 0;
      if (OB_ISNULL(state.name_) || !is_valid_tenant_id(state.tenant_id_)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(sql.assign_fmt(
          "SELECT value FROM oceanbase.__all_sys_stat "
          "WHERE tenant_id=%lu AND zone='' AND name='%s'", stored_tenant_id, state.name_))) {
      } else if (OB_FAIL(sql_proxy().read(res, state.tenant_id_, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(result->get_varchar("value", value))) {
      } else if (OB_FAIL(ObMaxIdFetcher::str_to_uint(value, state.value_))) {
      } else {
        state.exists_ = true;
      }
    }
    return ret;
  }

  int write_gc_stat_(const GCIdStatState &state)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t stored_tenant_id = 0;
    if (OB_ISNULL(state.name_) || !is_valid_tenant_id(state.tenant_id_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!state.exists_) {
      ret = sql.assign_fmt("DELETE FROM oceanbase.__all_sys_stat "
          "WHERE tenant_id=%lu AND zone='' AND name='%s'", stored_tenant_id, state.name_);
    } else {
      ret = sql.assign_fmt("INSERT INTO oceanbase.__all_sys_stat "
          "(tenant_id,zone,name,data_type,value,info) VALUES (%lu,'','%s',%d,'%lu','gc test') "
          "ON DUPLICATE KEY UPDATE value=VALUES(value)",
          stored_tenant_id, state.name_, static_cast<int>(ObIntType), state.value_);
    }
    if (OB_SUCC(ret)) {
      ret = sql_proxy().write(state.tenant_id_, sql.ptr(), affected_rows);
    }
    return ret;
  }

  int set_gc_counter_(
      const uint64_t tenant_id,
      const ObMaxIdType type,
      const bool exists,
      const uint64_t value)
  {
    GCIdStatState state;
    state.tenant_id_ = tenant_id;
    state.name_ = ObMaxIdFetcher::get_max_id_name(type);
    state.exists_ = exists;
    state.value_ = value;
    return write_gc_stat_(state);
  }

  int set_gc_enabled_(const bool enabled)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    bool synced = false;
    const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    if (OB_FAIL(sql.assign_fmt(
        "ALTER SYSTEM SET _enable_shared_gc_service='%s' TENANT='tt1'", enabled ? "true" : "false"))) {
    } else if (OB_FAIL(sql_proxy().write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    } else {
      while (!synced && ObTimeUtility::current_time() < deadline) {
        {
          omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
          synced = config.is_valid() && enabled == config->_enable_shared_gc_service;
        }
        if (!synced) {
          ob_usleep(100 * 1000);
        }
      }
      if (!synced) {
        ret = OB_TIMEOUT;
      }
    }
    return ret;
  }

  int write_gc_control_(const ObSharedGCControlInfo &info)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE oceanbase.__all_shared_gc_control SET executor_server_id=%ld,status='%s',"
        "gc_round_id=%ld,allow_gc_round_id=%ld,gc_meta_scn=%lu,last_succ_gc_meta_scn=%lu,"
        "gmt_modified=now(6) WHERE tenant_id=%lu", info.executor_server_id_,
        shared_gc_control_status_to_str(info.status_), info.gc_round_id_, info.allow_gc_round_id_,
        info.gc_meta_scn_.get_val_for_inner_table_field(),
        info.last_succ_gc_meta_scn_.get_val_for_inner_table_field(), tenant_id_))) {
    } else if (OB_FAIL(sql_proxy().write(meta_tid(), sql.ptr(), affected_rows))) {
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int save_gc_upgrade_state_(GCUpgradeState &state)
  {
    int ret = OB_SUCCESS;
    ObMaxIdType old_type = OB_MAX_ID_TYPE;
    ObMaxIdType new_type = OB_MAX_ID_TYPE;
    share::ObTenantSwitchGuard guard;
    ObSSGCService *gc_service = nullptr;
    ObTenantDagScheduler *scheduler = nullptr;
    const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    {
      omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
      if (!config.is_valid()) {
        ret = OB_INVALID_CONFIG;
      } else {
        state.gc_enabled_ = config->_enable_shared_gc_service;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(guard.switch_to(tenant_id_))) {
    } else if (OB_ISNULL(gc_service = MTL(ObSSGCService *))
        || OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(set_gc_enabled_(false))) {
    } else {
      state.runner_started_ = !ATOMIC_LOAD(&gc_service->task_runner_.is_stopped_);
      // Stop timers and drain DAGs before driving GC explicitly in the test.
      gc_service->task_runner_.stop();
      while (!scheduler->is_empty() && ObTimeUtility::current_time() < deadline) {
        ob_usleep(100 * 1000);
      }
      if (!scheduler->is_empty()) {
        ret = OB_TIMEOUT;
      } else {
        ret = ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, state.control_);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 5; ++i) {
      if (OB_FAIL(get_gc_id_types_(i, old_type, new_type))) {
      } else {
        state.stats_[i].tenant_id_ = tenant_id_;
        state.stats_[i].name_ = ObMaxIdFetcher::get_max_id_name(old_type);
        state.stats_[i + 5].tenant_id_ = meta_tid();
        state.stats_[i + 5].name_ = ObMaxIdFetcher::get_max_id_name(new_type);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 10; ++i) {
      ret = read_gc_stat_(state.stats_[i]);
    }
    state.saved_ = OB_SUCC(ret);
    if (OB_FAIL(ret) && nullptr != gc_service) {
      // No persistent state has changed yet; restore runtime state on failure for subsequent tests.
      int tmp_ret = set_gc_enabled_(state.gc_enabled_);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to restore gc configuration after saving state", KR(tmp_ret));
      } else if (state.runner_started_ && OB_SUCCESS != (tmp_ret = gc_service->task_runner_.start())) {
        LOG_WARN("fail to restart gc runner after saving state", KR(tmp_ret));
      }
    }
    return ret;
  }

  int cleanup_gc_upgrade_tasks_(const GCUpgradeState &state)
  {
    int ret = OB_SUCCESS;
    ObSharedGCTaskKey key;
    for (int64_t i = 0; OB_SUCC(ret) && i < state.task_keys_.count(); ++i) {
      key = state.task_keys_.at(i);
      ret = ObSharedGCTaskTableOperator::delete_task(key, sql_proxy());
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        ret = ObSharedGCTaskTableOperator::delete_history(key, sql_proxy());
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && ObSharedGCTaskType::MARK_SWEEP_GC == key.type_) {
        key.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT;
        ret = ObSharedGCTaskTableOperator::delete_history(key, sql_proxy());
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          key.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT;
          ret = ObSharedGCTaskTableOperator::delete_history(key, sql_proxy());
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    return ret;
  }

  int restore_gc_upgrade_state_(const GCUpgradeState &state)
  {
    int ret = OB_SUCCESS;
    share::ObTenantSwitchGuard guard;
    ObTenantDagScheduler *scheduler = nullptr;
    ObSSGCService *gc_service = nullptr;
    const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    if (!state.saved_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(guard.switch_to(tenant_id_))) {
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))
        || OB_ISNULL(gc_service = MTL(ObSSGCService *))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      // Wait for asynchronous DAGs to exit before restoring watermarks for subsequent tests.
      while (!scheduler->is_empty() && ObTimeUtility::current_time() < deadline) {
        ob_usleep(100 * 1000);
      }
      if (!scheduler->is_empty()) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(cleanup_gc_upgrade_tasks_(state))) {
      } else if (OB_FAIL(write_gc_control_(state.control_))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 10; ++i) {
        ret = write_gc_stat_(state.stats_[i]);
      }
      if (OB_SUCC(ret)) {
        ret = set_gc_enabled_(state.gc_enabled_);
      }
      if (OB_SUCC(ret) && state.runner_started_) {
        ret = gc_service->task_runner_.start();
      }
    }
    return ret;
  }

  int seed_gc_upgrade_(
      const int64_t floor,
      ObSharedGCControlInfo &info)
  {
    int ret = OB_SUCCESS;
    ObMaxIdType old_type = OB_MAX_ID_TYPE;
    ObMaxIdType new_type = OB_MAX_ID_TYPE;
    if (OB_FAIL(set_gc_enabled_(false))) {
    } else if (OB_FAIL(ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, info))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 5; ++i) {
      if (OB_FAIL(get_gc_id_types_(i, old_type, new_type))) {
      } else if (OB_FAIL(set_gc_counter_(tenant_id_, old_type, true, floor + i * 100))) {
      } else if (OB_FAIL(set_gc_counter_(meta_tid(), new_type, false, 0))) {
      }
    }
    if (OB_SUCC(ret)) {
      info.status_ = ObSharedGCControlStatus::IDLE;
      info.executor_server_id_ = static_cast<int64_t>(GCTX.get_server_id()) + 10000;
      info.gc_round_id_ = floor - 20;
      info.allow_gc_round_id_ = floor - 10;
      ret = write_gc_control_(info);
    }
    return ret;
  }

  void assert_gc_control_preserved_(
      const ObSharedGCControlInfo &before)
  {
    ObSharedGCControlInfo after;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, after));
    ASSERT_EQ(before.status_, after.status_);
    ASSERT_EQ(before.executor_server_id_, after.executor_server_id_);
    ASSERT_EQ(before.gc_round_id_, after.gc_round_id_);
    ASSERT_EQ(before.allow_gc_round_id_, after.allow_gc_round_id_);
    ASSERT_EQ(before.gc_meta_scn_, after.gc_meta_scn_);
    ASSERT_EQ(before.last_succ_gc_meta_scn_, after.last_succ_gc_meta_scn_);
  }

  void assert_gc_counter_(
      const uint64_t tenant_id,
      const ObMaxIdType type,
      const bool exists,
      const uint64_t expected_value)
  {
    GCIdStatState state;
    state.tenant_id_ = tenant_id;
    state.name_ = ObMaxIdFetcher::get_max_id_name(type);
    ASSERT_EQ(OB_SUCCESS, read_gc_stat_(state));
    ASSERT_EQ(exists, state.exists_);
    if (exists) {
      ASSERT_EQ(expected_value, state.value_);
    }
  }

  void assert_gc_task_exists_(const ObSharedGCTaskKey &key)
  {
    ObSqlString where;
    ASSERT_EQ(OB_SUCCESS, where.assign_fmt(
        "tenant_id=%lu AND type=%ld AND round_id=%ld AND task_id=%ld",
        key.tenant_id_, static_cast<int64_t>(key.type_), key.round_id_, key.task_id_));
    ASSERT_EQ(1, count_rows_("__all_shared_gc_task", where.ptr()));
  }

  void prepare_gc_diff_pipeline_(
      ObSSMarkSweepCtx &ctx,
      const MacroBlockId &deleted_id,
      const MacroBlockId &kept_id)
  {
    ASSERT_EQ(OB_SUCCESS, ctx.pipeline_.init(tenant_id_,
        ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT, ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE,
        0, &ctx.compare_));
    {
      ObSSMarkSweepSortTask::ObMarkSweepSorterPair pair;
      ASSERT_EQ(OB_SUCCESS, pair.init(ctx.pipeline_));
      ASSERT_EQ(OB_SUCCESS, pair.del_sorter().add_item(ObGCMacroBlockItem(deleted_id)));
      ASSERT_EQ(OB_SUCCESS, pair.del_sorter().add_item(ObGCMacroBlockItem(kept_id)));
      ASSERT_EQ(OB_SUCCESS, pair.kept_sorter().add_item(ObGCMacroBlockItem(kept_id)));
      ASSERT_EQ(OB_SUCCESS, pair.finish_and_add_file());
    }
    // Each group has one sorted run, so no intermediate merge pass is needed.
    ASSERT_EQ(OB_SUCCESS, ctx.pipeline_.prepare_merge());
    ASSERT_EQ(OB_SUCCESS, ctx.pipeline_.complete_group(ObMarkSweepGroupType::DEL));
    ASSERT_EQ(OB_SUCCESS, ctx.pipeline_.complete_group(ObMarkSweepGroupType::KEPT));
    ASSERT_EQ(OB_SUCCESS, ctx.pipeline_.complete_merge());
  }

  int run_gc_diff_(ObSSMarkSweepCtx &ctx)
  {
    int ret = OB_SUCCESS;
    ObSSMarkSweepDiffTask task;
    const ObGCMacroBlockItem *item = nullptr;
    int64_t deleted_count = 0;
    int64_t kept_count = -1;
    int64_t inserted_rows = 0;
    if (OB_FAIL(task.init())) {
      LOG_WARN("fail to initialize test diff task", KR(ret));
    } else {
      ObSSMarkSweepDiffTask::DiffFn fn(task, ctx, item, deleted_count, kept_count, inserted_rows);
      ret = fn();
      EXPECT_EQ(1, deleted_count);
      EXPECT_EQ(1, inserted_rows);
    }
    return ret;
  }

  void check_gc_diff_execution_(
      const GCUpgradeState &saved,
      const MacroBlockId &deleted_id,
      const MacroBlockId &kept_id,
      const int64_t allow_round)
  {
    ObArenaAllocator alloc;
    ObSharedGCTask row;
    ASSERT_EQ(2, saved.task_keys_.count());
    // An execution DAG drains one actual round, including the failed round's persisted task.
    ASSERT_NO_FATAL_FAILURE(trigger_mark_sweep_dag(allow_round));
    ASSERT_NO_FATAL_FAILURE(wait_dag_scheduler_empty_(60));
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
        saved.task_keys_.at(0), alloc, sql_proxy(), row));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(deleted_id, false));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true));
    ASSERT_NO_FATAL_FAILURE(assert_gc_task_exists_(saved.task_keys_.at(1)));
    ASSERT_NO_FATAL_FAILURE(trigger_mark_sweep_dag(allow_round));
    ASSERT_NO_FATAL_FAILURE(wait_task_table_empty(allow_round, 60));
    ASSERT_NO_FATAL_FAILURE(wait_dag_scheduler_empty_(60));
    ASSERT_EQ(OB_SUCCESS, ObSharedGCTaskTableOperator::get_history(
        saved.task_keys_.at(1), alloc, sql_proxy(), row));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(deleted_id, false));
    ASSERT_NO_FATAL_FAILURE(assert_block_exists_(kept_id, true));
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

  void wait_dag_scheduler_empty_(int64_t timeout_sec)
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
  void wait_task_table_empty(int64_t allow_gc_round_id, int64_t timeout_sec)
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

uint64_t TestSSGCIdUpgrade::tenant_id_ = OB_INVALID_TENANT_ID;

// ---------------------------------------------------------------------------
// observer_start: wait for sys LS leader before any tests run.
// ---------------------------------------------------------------------------
TEST_F(TestSSGCIdUpgrade, observer_start)
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
TEST_F(TestSSGCIdUpgrade, setup_tenant)
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

  // Wait for the USER SYS_LS leader before testing META-backed GC tables.
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

// Disabled GC retries failed ownership cleanup and stops querying after a successful check.
TEST_F(TestSSGCIdUpgrade, gc_disabled_release_retry)
{
  const int64_t floor = 10000;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  share::ObTenantSwitchGuard guard;
  volatile bool stopped = false;
  MockSSMarkSweepGCGenTask task(stopped);
  bool is_need = false;
  bool control_deleted = false;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  DEFER({
    if (control_deleted) {
      EXPECT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_default_row(
          sql_proxy(), tenant_id_, affected_rows));
    }
  });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, task.init(tenant_id_));
  ASSERT_FALSE(task.gc_disabled_release_checked_);
  EXPECT_CALL(task, check_dag_exist_(testing::_))
      .WillRepeatedly(testing::DoAll(testing::SetArgReferee<0>(false), testing::Return(OB_SUCCESS)));

  // A missing control row fails the SQL lookup and must not consume the retry.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), tenant_id_, affected_rows));
  control_deleted = true;
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(is_need);
  ASSERT_FALSE(task.gc_disabled_release_checked_);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_default_row(
      sql_proxy(), tenant_id_, affected_rows));
  control_deleted = false;
  info.status_ = ObSharedGCControlStatus::DOING;
  info.executor_server_id_ = static_cast<int64_t>(GCTX.get_server_id());
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(is_need);
  ASSERT_TRUE(task.gc_disabled_release_checked_);
  info.status_ = ObSharedGCControlStatus::IDLE;
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  // Releasing ownership allows the META compatibility upgrade to proceed.
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));

  // Detect repeated release attempts by restoring a self-owned DOING row.
  info.status_ = ObSharedGCControlStatus::DOING;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(is_need);
  ASSERT_TRUE(task.gc_disabled_release_checked_);
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));

  // A remaining DAG clears the completed check even when cancellation fails.
  EXPECT_CALL(task, check_dag_exist_(testing::_))
      .WillOnce(testing::DoAll(testing::SetArgReferee<0>(true), testing::Return(OB_SUCCESS)))
      .RetiresOnSaturation();
  EXPECT_CALL(task, cancel_dag_()).WillOnce(testing::Return(OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(task.gc_disabled_release_checked_);
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_TRUE(task.gc_disabled_release_checked_);
  info.status_ = ObSharedGCControlStatus::IDLE;
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));

  // Observing enabled GC resets the check even if the subsequent DAG lookup fails.
  ASSERT_EQ(OB_SUCCESS, set_gc_enabled_(true));
  EXPECT_CALL(task, check_dag_exist_(testing::_))
      .WillOnce(testing::Return(OB_TIMEOUT)).RetiresOnSaturation();
  ASSERT_EQ(OB_TIMEOUT, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(task.gc_disabled_release_checked_);
  ASSERT_EQ(OB_SUCCESS, set_gc_enabled_(false));
  info.status_ = ObSharedGCControlStatus::DOING;
  info.executor_server_id_ += 10000;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  ASSERT_EQ(OB_SUCCESS, task.check_if_need_do_gc_work(is_need));
  ASSERT_FALSE(is_need);
  ASSERT_TRUE(task.gc_disabled_release_checked_);
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));

  task.destroy();
  ASSERT_FALSE(task.gc_disabled_release_checked_);
}

// Use legacy counter watermarks and preserve larger IDs already allocated in META on reruns.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_watermarks)
{
  const int64_t floor = 20000;
  const uint64_t expected[] = {floor + 1000, floor + 1100, floor + 1200,
      floor + 1300, floor + 1900};
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  share::ObTenantSwitchGuard guard;
  int64_t id = 0;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE, true, floor + 1900));
  // The upgrade processor runs in SYS and routes the explicit proxy to USER and META.
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, expected[i]));
    ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(tenant_id_, old_type, id));
    ASSERT_EQ(expected[i] + 1, id);
  }

  // Reject DOING without changing state; an IDLE retry preserves existing allocations.
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    const int expected_ret = 0 == attempt ? OB_STATE_NOT_MATCH : OB_SUCCESS;
    info.status_ = 0 == attempt ? ObSharedGCControlStatus::DOING : ObSharedGCControlStatus::IDLE;
    ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
    ASSERT_EQ(expected_ret, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, expected[i] + 1));
    }
  }
}

// Exercise META-only admission checks and repeated calibration through the registered processor.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_processor)
{
  const int64_t floor = 30000;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObUpgradeFor4631Processor processor;
  TestGCUpgradeStopProvider stop_provider;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  const bool original_upgrade_mode = GCONF.enable_upgrade_mode;
  const obrpc::ObUpgradeStage original_stage = GCTX.get_upgrade_stage();
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  DEFER({
    GCONF.enable_upgrade_mode.set_value(original_upgrade_mode ? "true" : "false");
    GCTX.set_upgrade_stage(original_stage);
  });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_NOT_INIT, processor.post_upgrade());
  ASSERT_NE(nullptr, GCTX.oracle_sql_proxy_);
  ASSERT_NE(nullptr, GCTX.srv_rpc_proxy_);
  ASSERT_NE(nullptr, GCTX.rs_rpc_proxy_);
  ASSERT_NE(nullptr, GCTX.schema_service_);
  ASSERT_EQ(OB_SUCCESS, processor.init(
      DATA_VERSION_4_6_3_1, ObBaseUpgradeProcessor::UPGRADE_MODE_OB, sql_proxy(),
      *GCTX.oracle_sql_proxy_, *GCTX.srv_rpc_proxy_, *GCTX.rs_rpc_proxy_,
      *GCTX.schema_service_, stop_provider));
  GCONF.enable_upgrade_mode.set_value("false");
  GCTX.set_upgrade_stage(obrpc::OB_UPGRADE_STAGE_NONE);
  ASSERT_EQ(OB_SUCCESS, set_gc_enabled_(true));
  // SYS and USER skip calibration before stage and GC checks, regardless of USER role.
  processor.set_tenant_id(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  processor.set_tenant_id(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  processor.set_tenant_id(meta_tid());
  processor.mode_ = ObBaseUpgradeProcessor::UPGRADE_MODE_PHYSICAL_RESTORE;
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  processor.mode_ = ObBaseUpgradeProcessor::UPGRADE_MODE_OB;
  ASSERT_EQ(OB_OP_NOT_ALLOW, processor.post_upgrade());
  GCONF.enable_upgrade_mode.set_value("true");
  ASSERT_EQ(OB_OP_NOT_ALLOW, processor.post_upgrade());
  GCTX.set_upgrade_stage(obrpc::OB_UPGRADE_STAGE_POSTUPGRADE);
  processor.set_tenant_id(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  processor.set_tenant_id(meta_tid());
  ASSERT_EQ(OB_OP_NOT_ALLOW, processor.post_upgrade());
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  ASSERT_EQ(OB_SUCCESS, set_gc_enabled_(false));

  // A non-IDLE control blocks tenant upgrade before any META fetcher is initialized.
  info.status_ = ObSharedGCControlStatus::DOING;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  ASSERT_EQ(OB_STATE_NOT_MATCH, processor.post_upgrade());
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  info.status_ = ObSharedGCControlStatus::IDLE;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));

  // Propagate calibration failure through the actual processor without publishing partial counters.
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE, true, 0));
  ASSERT_EQ(OB_STATE_NOT_MATCH, processor.post_upgrade());
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE, true, floor + 400));
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  ASSERT_EQ(OB_SUCCESS, processor.post_upgrade());
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, floor + 1000 + i * 100));
  }
}

// Legacy fetchers are created on first use; migration initializes only their META counterparts.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_missing_legacy)
{
  const int64_t floor = 60000;
  const int64_t used_values[] = {
      floor + 1000, floor + 1100, floor + 1200, floor + 1300, floor + 1400};
  const int64_t unused_values[] = {1001, 1001, 1001, 1001, 1001};
  const int64_t missing_masks[] = {1, 2, 4, 8, 16, 10, 31};
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  share::ObTenantSwitchGuard guard;
  int64_t id = 0;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));

  // Cover each missing type, a mixture of used and unused types, and an entirely unused tenant.
  for (int64_t c = 0; c < ARRAYSIZEOF(missing_masks); ++c) {
    ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
    if (0 != (missing_masks[c] & 1)) {
      info.gc_round_id_ = 0;
      info.allow_gc_round_id_ = 0;
      ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
    }
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      if (0 != (missing_masks[c] & (1L << i))) {
        ASSERT_EQ(OB_SUCCESS, set_gc_counter_(tenant_id_, old_type, false, 0));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      const bool missing = 0 != (missing_masks[c] & (1L << i));
      const int64_t expected = missing ? unused_values[i] : used_values[i];
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, !missing, floor + i * 100));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, expected));
      if (missing) {
        ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(tenant_id_, old_type, id));
        ASSERT_EQ(expected + 1, id);
        ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, expected + 1));
        ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, false, 0));
      }
    }
  }

  // Missing legacy values must not lower META watermarks already allocated before a rerun.
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(tenant_id_, old_type, false, 0));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(meta_tid(), new_type, true, floor + 1000 + i));
  }
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, floor + 1000 + i));
    ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(tenant_id_, old_type, id));
    ASSERT_EQ(floor + 1001 + i, id);
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, false, 0));
  }
}

// Only absent rows use initial values; invalid values must leave counters and control unchanged.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_invalid_legacy)
{
  const int64_t floor = 80000;
  const char *invalid_values[] = {"-1", "", "9223372036854775808"};
  const int expected_errors[] = {OB_SIZE_OVERFLOW, OB_INVALID_ARGUMENT, OB_INVALID_ARGUMENT};
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  ObSqlString sql;
  share::ObTenantSwitchGuard guard;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_FAIL_GC_TASK_ID_TYPE, false, 0));

  // str_to_uint converts -1 to UINT64_MAX, and rejects empty strings and signed overflow.
  for (int64_t c = 0; c < ARRAYSIZEOF(invalid_values); ++c) {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "UPDATE oceanbase.__all_sys_stat SET value='%s' "
        "WHERE tenant_id=0 AND zone='' AND name='%s'", invalid_values[c],
        ObMaxIdFetcher::get_max_id_name(OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE)));
    ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id_, sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
    ASSERT_EQ(expected_errors[c], ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
    }
  }
  ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_FAIL_GC_TASK_ID_TYPE, false, 0));
}

TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_atomic_rollback)
{
  const int64_t floor = 40000;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
#ifdef ERRSIM
  ScopedNamedEvent failure;
#endif
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));

  // The SQL timeout helper must not extend an already expired caller deadline.
  {
    ObTimeoutCtx timeout_ctx;
    ASSERT_EQ(OB_SUCCESS, timeout_ctx.set_abs_timeout(ObTimeUtility::current_time() - 1));
    ASSERT_EQ(OB_TIMEOUT, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  }
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }

  // An absent round counter cannot hide a control watermark above the initial value.
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, false, 0));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, false, 0));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor));
  // Padding must not hide zero task watermarks; roll back all earlier updates.
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE, true, 0));
  ASSERT_EQ(OB_STATE_NOT_MATCH, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
  }
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE, true, floor + 400));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor - 10));
#ifdef ERRSIM
  // Named error events are registered only in ERRSIM builds.
  ASSERT_EQ(OB_SUCCESS,
      failure.init("EN_SS_GC_UPGRADE_AFTER_UPDATE_FETCHER", OB_ERR_UNEXPECTED, 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  failure.reset();
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, 0 == i, floor - 10));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
  }
#endif
  // Reject control watermarks above both counters; do not raise the round to hide inconsistency.
  info.gc_round_id_ = floor + 10;
  info.allow_gc_round_id_ = floor + 20;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, 0 == i, floor - 10));
  }
  // A sufficient META round permits reruns even if the legacy USER round was never allocated.
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, false, 0));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor + 20));
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, false, 0));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
        meta_tid(), new_type, true, 0 == i ? floor + 20 : floor + 1000 + i * 100));
  }
}

// GC ID padding must preserve room for allocation and roll back every counter on overflow.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_padding_overflow)
{
  const int64_t floor = 90000;
  const uint64_t overflow_watermark = static_cast<uint64_t>(INT64_MAX) - 1000;
  const uint64_t last_valid_watermark = static_cast<uint64_t>(INT64_MAX) - 1001;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  ObMaxIdType overflow_type = OB_MAX_ID_TYPE;
  ObMaxIdType overflow_new_type = OB_MAX_ID_TYPE;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });

  for (int64_t overflow_index = 0; overflow_index < 5; ++overflow_index) {
    ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(overflow_index, overflow_type, overflow_new_type));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(tenant_id_, overflow_type, true, overflow_watermark));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
        meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor - 10));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
        meta_tid(), overflow_new_type, true, static_cast<uint64_t>(INT64_MAX) - 1));

    // A larger META value cannot make overflowing legacy padding valid, including round IDs.
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      const uint64_t expected_meta = i == overflow_index
          ? static_cast<uint64_t>(INT64_MAX) - 1
          : floor - 10;
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true,
          i == overflow_index ? overflow_watermark : floor + i * 100));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
          meta_tid(), new_type, 0 == i || i == overflow_index, expected_meta));
    }
  }

  // All five types accept the last legacy value whose padded result remains below INT64_MAX.
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(tenant_id_, old_type, true, last_valid_watermark));
  }
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, last_valid_watermark));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
        meta_tid(), new_type, true, static_cast<uint64_t>(INT64_MAX) - 1));
  }
}

// Existing GC watermarks require explicit calibration before any META counter can be initialized.
TEST_F(TestSSGCIdUpgrade, gc_id_existing_tenant_requires_upgrade)
{
  const int64_t floor = 100000;
  const int64_t calibrated_ids[] = {
      floor + 1000, floor + 1100, floor + 1200, floor + 1300, floor + 1900};
  const int64_t rounds[][2] = {{floor - 20, 0}, {0, floor - 10}, {floor - 20, floor - 10}};
  const int64_t member_round = 123;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask task(stopped);
  share::ObTenantSwitchGuard guard;
  bool acquired = true;
  bool check_old = true;
  int64_t old_owner = -1;
  int64_t id = 99;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, task.init(tenant_id_));

  for (int64_t c = 0; c < ARRAYSIZEOF(rounds); ++c) {
    ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
    info.status_ = ObSharedGCControlStatus::IDLE;
    info.gc_round_id_ = rounds[c][0];
    info.allow_gc_round_id_ = rounds[c][1];
    ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      id = 99;
      ASSERT_EQ(OB_STATE_NOT_MATCH, ObSSGCIdUtil::fetch_gc_id(tenant_id_, old_type, id));
      ASSERT_EQ(0, id);
    }

    // Exercise the generator's transaction path as well as the standalone allocation wrapper.
    task.round_id_ = member_round;
    acquired = true;
    check_old = true;
    old_owner = -1;
    ASSERT_EQ(OB_STATE_NOT_MATCH, task.try_acquire_executor_(acquired, check_old, old_owner));
    ASSERT_FALSE(acquired);
    ASSERT_FALSE(check_old);
    ASSERT_EQ(0, old_owner);
    ASSERT_EQ(member_round, task.round_id_);
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, false, 0));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    }

    // Explicit calibration prepares all counters without changing the control row.
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
        meta_tid(), OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE, true, floor + 1900));
    ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    for (int64_t i = 0; i < 5; ++i) {
      const int64_t calibrated = calibrated_ids[i];
      ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, calibrated));
      ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(tenant_id_, old_type, id));
      ASSERT_EQ(calibrated + 1, id);
      ASSERT_GT(id, floor + i * 100);
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, calibrated + 1));
      ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(tenant_id_, old_type, true, floor + i * 100));
    }
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  }
}

TEST_F(TestSSGCIdUpgrade, gc_id_new_tenant_initialization)
{
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  int64_t id = 99;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, info));
  info.status_ = ObSharedGCControlStatus::IDLE;
  info.gc_round_id_ = 0;
  info.allow_gc_round_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(meta_tid(), new_type, false, 0));
  }
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, id));
  ASSERT_EQ(2, id);
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, 0 == i ? 2 : 1));
  }
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE, false, 0));
  ASSERT_EQ(OB_EAGAIN, ObSSGCIdUtil::fetch_gc_id(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, id));
  ASSERT_EQ(0, id);
  ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, 2));
  ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
      meta_tid(), OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE, true, 0));
  ASSERT_EQ(OB_EAGAIN, ObSSGCIdUtil::fetch_gc_id(
      tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, id));
  ASSERT_EQ(0, id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSGCIdUtil::fetch_gc_id(
      OB_INVALID_TENANT_ID, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, id));
  ASSERT_EQ(0, id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSGCIdUtil::fetch_gc_id(tenant_id_, OB_MAX_ID_TYPE, id));
  ASSERT_EQ(0, id);
}

// SYS/META allocations keep using their legacy fetchers, including SYS-only LS checkpoint GC.
TEST_F(TestSSGCIdUpgrade, gc_id_legacy_tenant_allocation)
{
  const uint64_t tenant_ids[3] = {OB_SYS_TENANT_ID, meta_tid(), OB_SYS_TENANT_ID};
  const ObMaxIdType id_types[3] = {
    OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE,
    OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE,
    OB_MAX_USED_SHARED_LS_CKPT_GC_TASK_ID_TYPE
  };
  GCIdStatState stat;
  ObMySQLTransaction trans;
  int64_t id = 0;
  uint64_t previous_max_id = 0;
  for (int64_t i = 0; i < 3; ++i) {
    stat.tenant_id_ = tenant_ids[i];
    stat.name_ = ObMaxIdFetcher::get_max_id_name(id_types[i]);
    ASSERT_EQ(OB_SUCCESS, read_gc_stat_(stat));
    previous_max_id = stat.exists_ ? stat.value_ : 1;
    ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::fetch_gc_id(trans, tenant_ids[i], id_types[i], id));
    ASSERT_GT(id, static_cast<int64_t>(previous_max_id));
    ASSERT_EQ(OB_SUCCESS, read_gc_stat_(stat));
    ASSERT_TRUE(stat.exists_);
    ASSERT_GE(stat.value_, static_cast<uint64_t>(id));
  }
  ASSERT_FALSE(trans.is_started());
}

// The control row serializes first-use requests to prevent duplicate initialization and IDs.
TEST_F(TestSSGCIdUpgrade, gc_id_concurrent_initialization)
{
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObMaxIdType old_type = OB_MAX_ID_TYPE;
  ObMaxIdType new_type = OB_MAX_ID_TYPE;
  GCIdFetchContext contexts[2];
  pthread_t threads[2];
  int create_ret = 0;
  int64_t started_count = 0;
  volatile bool start = false;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, info));
  info.status_ = ObSharedGCControlStatus::IDLE;
  info.gc_round_id_ = 0;
  info.allow_gc_round_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(meta_tid(), new_type, false, 0));
  }
  for (int64_t i = 0; 0 == create_ret && i < 2; ++i) {
    contexts[i].tenant_id_ = tenant_id_;
    contexts[i].start_ = &start;
    create_ret = pthread_create(&threads[i], nullptr, fetch_gc_id_thread_, &contexts[i]);
    if (0 == create_ret) {
      ++started_count;
    }
  }
  ATOMIC_STORE(&start, true);
  for (int64_t i = 0; i < started_count; ++i) {
    EXPECT_EQ(0, pthread_join(threads[i], nullptr));
  }
  ASSERT_EQ(0, create_ret);
  ASSERT_EQ(OB_SUCCESS, contexts[0].ret_);
  ASSERT_EQ(OB_SUCCESS, contexts[1].ret_);
  ASSERT_EQ(2, MIN(contexts[0].id_, contexts[1].id_));
  ASSERT_EQ(3, MAX(contexts[0].id_, contexts[1].id_));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, get_gc_id_types_(i, old_type, new_type));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(meta_tid(), new_type, true, 1 == i ? 3 : 1));
  }
}

TEST_F(TestSSGCIdUpgrade, gc_round_guard)
{
  const int64_t floor = 60000;
  const int64_t member_round = 123;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask task(stopped);
  bool acquired = true;
  bool check_old = true;
  bool taken_over = true;
  int64_t old_owner = -1;
  share::ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_EQ(OB_SUCCESS, task.init(tenant_id_));
  for (int64_t mode = 0; mode < 3; ++mode) {
    info.status_ = 0 == mode ? ObSharedGCControlStatus::IDLE : ObSharedGCControlStatus::DOING;
    info.executor_server_id_ = static_cast<int64_t>(GCTX.get_server_id()) + (2 == mode ? 10000 : 0);
    info.gc_round_id_ = floor + 100;
    info.allow_gc_round_id_ = floor + 200;
    task.round_id_ = member_round;
    ASSERT_EQ(OB_SUCCESS, write_gc_control_(info));
    ASSERT_EQ(OB_SUCCESS, set_gc_counter_(
        meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor + 150));
    if (2 == mode) {
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          task.try_takeover_executor_(info.executor_server_id_, taken_over));
      ASSERT_FALSE(taken_over);
    } else {
      ASSERT_EQ(OB_ERR_UNEXPECTED, task.try_acquire_executor_(acquired, check_old, old_owner));
      ASSERT_FALSE(acquired);
      ASSERT_FALSE(check_old);
      ASSERT_EQ(0, old_owner);
    }
    ASSERT_EQ(member_round, task.round_id_);
    ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
    ASSERT_NO_FATAL_FAILURE(assert_gc_counter_(
        meta_tid(), OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE, true, floor + 150));
  }
}

// Interrupt before publishing allow, then take over and verify object deletion in each round.
TEST_F(TestSSGCIdUpgrade, gc_id_upgrade_takeover_e2e)
{
  const int64_t floor = 80000;
  GCUpgradeState saved;
  ObSharedGCControlInfo info;
  ObSharedGCControlInfo after;
  ObArenaAllocator alloc;
  ObArray<MacroBlockId> ids;
  ObSharedGCTaskKey key;
  ObTabletDelMetaScnInfo invalid_meta;
  share::ObTenantSwitchGuard guard;
  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask first_task(stopped);
  ObSSMarkSweepGCGenTask takeover_task(stopped);
  bool acquired = false;
  bool check_old = false;
  bool taken_over = false;
  int64_t old_owner = 0;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, save_gc_upgrade_state_(saved));
  DEFER({ EXPECT_EQ(OB_SUCCESS, restore_gc_upgrade_state_(saved)); });
  ASSERT_EQ(OB_SUCCESS, seed_gc_upgrade_(floor, info));
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, ObSSGCIdUtil::upgrade_user_gc_ids(sql_proxy(), tenant_id_));
  ASSERT_NO_FATAL_FAILURE(assert_gc_control_preserved_(info));
  ASSERT_EQ(OB_SUCCESS, first_task.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, first_task.try_acquire_executor_(acquired, check_old, old_owner));
  ASSERT_TRUE(acquired);
  ASSERT_FALSE(check_old);
  ASSERT_EQ(floor + 1001, first_task.round_id_);
  ASSERT_NO_FATAL_FAILURE(write_sstable_id_(ObTabletID(300080), first_task.round_id_, ids));
  ASSERT_NO_FATAL_FAILURE(write_sstable_id_(ObTabletID(300081), first_task.round_id_, ids));
  void *ctx_buf = alloc.alloc(sizeof(ObSSMarkSweepCtx));
  ASSERT_NE(nullptr, ctx_buf);
  ObSSMarkSweepCtx *ctx = new (ctx_buf) ObSSMarkSweepCtx();
  DEFER({ ctx->~ObSSMarkSweepCtx(); });
  ctx->tenant_id_ = tenant_id_;
  ctx->round_id_ = first_task.round_id_;
  ctx->safe_meta_scn_ = info.gc_meta_scn_;
  ASSERT_NO_FATAL_FAILURE(prepare_gc_diff_pipeline_(*ctx, ids.at(0), ids.at(1)));
  key.tenant_id_ = tenant_id_;
  key.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
  key.round_id_ = first_task.round_id_;
  key.task_id_ = floor + 1101;
  ASSERT_EQ(OB_SUCCESS, saved.task_keys_.push_back(key));

  // Fail after the real diff persists tasks but before allow is published; no real meta is deleted.
  ASSERT_FALSE(invalid_meta.is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx->tablet_del_meta_scn_infos_.push_back(invalid_meta));
  ASSERT_EQ(OB_INVALID_ARGUMENT, run_gc_diff_(*ctx));
  ASSERT_NO_FATAL_FAILURE(assert_gc_task_exists_(key));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, after));
  ASSERT_EQ(info.allow_gc_round_id_, after.allow_gc_round_id_);
  ASSERT_NO_FATAL_FAILURE(trigger_mark_sweep_dag(after.allow_gc_round_id_));
  ASSERT_NO_FATAL_FAILURE(wait_dag_scheduler_empty_(60));
  ASSERT_NO_FATAL_FAILURE(assert_gc_task_exists_(key));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(ids.at(0), true));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(ids.at(1), true));

  after.executor_server_id_ = static_cast<int64_t>(GCTX.get_server_id()) + 10000;
  ASSERT_EQ(OB_SUCCESS, write_gc_control_(after));
  ASSERT_EQ(OB_SUCCESS, takeover_task.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, takeover_task.try_acquire_executor_(acquired, check_old, old_owner));
  ASSERT_FALSE(acquired);
  ASSERT_TRUE(check_old);
  ASSERT_EQ(after.executor_server_id_, old_owner);
  ASSERT_EQ(OB_SUCCESS, takeover_task.try_takeover_executor_(old_owner, taken_over));
  ASSERT_TRUE(taken_over);
  ASSERT_EQ(floor + 1002, takeover_task.round_id_);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id_, first_task.round_id_, affected_rows));
  ASSERT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, after));
  ASSERT_EQ(info.allow_gc_round_id_, after.allow_gc_round_id_);

  // Recompute the real difference under the new round and let DiffFn publish allow.
  ctx->round_id_ = takeover_task.round_id_;
  ctx->tablet_del_meta_scn_infos_.reuse();
  ASSERT_EQ(OB_SUCCESS, ctx->pipeline_.init_diff_iter());
  key.round_id_ = takeover_task.round_id_;
  key.task_id_ = floor + 1102;
  ASSERT_EQ(OB_SUCCESS, saved.task_keys_.push_back(key));
  ASSERT_EQ(OB_SUCCESS, run_gc_diff_(*ctx));
  ASSERT_NO_FATAL_FAILURE(assert_gc_task_exists_(key));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id_, after));
  ASSERT_EQ(takeover_task.round_id_, after.allow_gc_round_id_);
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(ids.at(0), true));
  ASSERT_NO_FATAL_FAILURE(assert_block_exists_(ids.at(1), true));
  ASSERT_NO_FATAL_FAILURE(check_gc_diff_execution_(
      saved, ids.at(0), ids.at(1), takeover_task.round_id_));
  ASSERT_EQ(OB_SUCCESS, takeover_task.try_release_executor_());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  const char *log_level = "INFO";
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
    case 'l': {
      log_level = optarg;
      oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
      break;
    }
    default: {
      break;
    }
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

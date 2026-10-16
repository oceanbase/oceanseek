/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGETEST

#include <atomic>

#include <gmock/gmock.h>

#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#undef private
#undef protected

#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_ls_meta_gc_dag.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

namespace
{

struct GCMTLProbeState
{
  GCMTLProbeState()
    : entered_(false),
      exited_(false),
      stop_ret_(OB_ERR_UNEXPECTED),
      scheduler_stopped_(false),
      destructor_scheduler_stopped_(false),
      destructor_count_(0)
  {
  }

  void reset()
  {
    entered_.store(false, std::memory_order_release);
    exited_.store(false, std::memory_order_release);
    stop_ret_.store(OB_ERR_UNEXPECTED, std::memory_order_release);
    scheduler_stopped_.store(false, std::memory_order_release);
    destructor_scheduler_stopped_.store(false, std::memory_order_release);
    destructor_count_.store(0, std::memory_order_release);
  }

  std::atomic<bool> entered_;
  std::atomic<bool> exited_;
  std::atomic<int> stop_ret_;
  std::atomic<bool> scheduler_stopped_;
  std::atomic<bool> destructor_scheduler_stopped_;
  std::atomic<int64_t> destructor_count_;
};

GCMTLProbeState GC_MTL_PROBE_STATES[3];

template <ObITask::ObITaskType TASK_TYPE>
class GCMTLProbeTask final : public ObITask
{
public:
  explicit GCMTLProbeTask(GCMTLProbeState &state)
    : ObITask(TASK_TYPE), state_(state)
  {
  }

  virtual ~GCMTLProbeTask()
  {
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    state_.destructor_scheduler_stopped_.store(
        OB_ISNULL(scheduler) || scheduler->has_set_stop(),
        std::memory_order_release);
    state_.destructor_count_.fetch_add(1, std::memory_order_acq_rel);
  }

  virtual int process() override
  {
    int ret = OB_SUCCESS;
    state_.entered_.store(true, std::memory_order_release);
    while (OB_SUCCESS == ret) {
      ret = ObSSGCDagUtil::check_if_stopped(*this);
      if (OB_SUCCESS == ret) {
        ob_usleep(1000L);
      }
    }
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    state_.stop_ret_.store(ret, std::memory_order_release);
    state_.scheduler_stopped_.store(
        OB_ISNULL(scheduler) || scheduler->has_set_stop(),
        std::memory_order_release);
    state_.exited_.store(true, std::memory_order_release);
    return ret;
  }

private:
  GCMTLProbeState &state_;
  DISALLOW_COPY_AND_ASSIGN(GCMTLProbeTask);
};

template <typename Dag, typename Param, ObITask::ObITaskType TASK_TYPE>
int submit_probe_dag(
    ObTenantDagScheduler &scheduler,
    GCMTLProbeState &state,
    const int64_t schedule_ts)
{
  int ret = OB_SUCCESS;
  Dag *dag = nullptr;
  GCMTLProbeTask<TASK_TYPE> *task = nullptr;
  Param param;
  Dag preflight_probe;
  preflight_probe.set_tenant_id(OB_SYS_TENANT_ID);
  bool preflight_exist = true;
  bool preflight_is_emergency = false;
  if (OB_FAIL(scheduler.check_dag_exist(
      &preflight_probe, preflight_exist, preflight_is_emergency))) {
    LOG_WARN("fail to check gc mtl probe dag before submit", KR(ret),
        K(schedule_ts));
  } else if (OB_UNLIKELY(preflight_exist)) {
    ret = OB_EAGAIN;
    LOG_WARN("gc mtl probe dag already exists", KR(ret), K(schedule_ts));
  } else if (OB_FAIL(param.init(OB_SYS_TENANT_ID, schedule_ts))) {
    LOG_WARN("fail to init gc mtl probe dag param", KR(ret), K(schedule_ts));
  } else if (OB_FAIL(scheduler.alloc_dag(dag))) {
    LOG_WARN("fail to allocate gc mtl probe dag", KR(ret), K(schedule_ts));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated gc mtl probe dag is null", KR(ret), K(schedule_ts));
  } else if (OB_FAIL(dag->init_by_param(&param))) {
    LOG_WARN("fail to init gc mtl probe dag", KR(ret), K(schedule_ts));
  } else if (OB_FAIL(dag->alloc_task(task, state))) {
    LOG_WARN("fail to allocate gc mtl probe task", KR(ret), K(schedule_ts));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated gc mtl probe task is null", KR(ret), K(schedule_ts));
  } else if (OB_FAIL(dag->add_task(*task))) {
    LOG_WARN("fail to add gc mtl probe task", KR(ret), K(schedule_ts));
  } else {
    ret = scheduler.add_dag(dag);
    if (OB_SUCCESS == ret) {
      dag = nullptr;
    } else if (OB_ERR_SYS == ret) {
      // add_dag() transfers ownership before signaling the Scheduler. Only a
      // signal failure can return OB_ERR_SYS after that transfer; all normal
      // insertion failures leave ownership with this helper.
      Dag probe;
      probe.set_tenant_id(OB_SYS_TENANT_ID);
      bool exist = false;
      bool is_emergency = false;
      const int probe_ret = scheduler.check_dag_exist(
          &probe, exist, is_emergency);
      if (OB_SUCCESS == probe_ret && exist) {
        dag = nullptr;
      } else if (OB_SUCCESS != probe_ret) {
        // Ownership is unknown. Avoid a possible double free; the process is
        // disposable and the lifecycle teardown below remains authoritative.
        dag = nullptr;
      }
      LOG_WARN("fail to submit gc mtl probe dag", KR(ret), K(probe_ret),
          K(exist), K(schedule_ts));
    } else {
      LOG_WARN("fail to submit gc mtl probe dag", KR(ret), K(schedule_ts));
    }
  }

  if (OB_NOT_NULL(dag)) {
    scheduler.free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}

bool wait_all_probe_tasks_entered(const int64_t timeout_us)
{
  const int64_t deadline_us = ObTimeUtility::current_time() + timeout_us;
  bool all_entered = false;
  while (!all_entered && ObTimeUtility::current_time() < deadline_us) {
    all_entered = true;
    for (int64_t i = 0; all_entered && i < 3; ++i) {
      all_entered = GC_MTL_PROBE_STATES[i].entered_.load(
          std::memory_order_acquire);
    }
    if (!all_entered) {
      ob_usleep(10L * 1000L);
    }
  }
  return all_entered;
}

} // namespace

class TestSSGCMTLLifecycle : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
};

TEST_F(TestSSGCMTLLifecycle,
    three_gc_dags_exit_after_tenant_dag_scheduler_stops)
{
  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_NE(nullptr, gc_service);

  for (int64_t i = 0; i < 3; ++i) {
    GC_MTL_PROBE_STATES[i].reset();
  }
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, 3));

  int submit_rets[3];
  const int64_t schedule_ts = ObTimeUtility::current_time();
  submit_rets[0] = submit_probe_dag<
      ObSSFailGCDag,
      ObSSFailGCDagParam,
      ObITask::TASK_TYPE_SS_FAIL_GC>(
          *scheduler, GC_MTL_PROBE_STATES[0], schedule_ts);
  submit_rets[1] = submit_probe_dag<
      ObSSLSMetaGCDag,
      ObSSLSMetaGCDagParam,
      ObITask::TASK_TYPE_SS_LS_META_GC>(
          *scheduler, GC_MTL_PROBE_STATES[1], schedule_ts + 1);
  submit_rets[2] = submit_probe_dag<
      ObSSLockRowGCDag,
      ObSSLockRowGCDagParam,
      ObITask::TASK_TYPE_SS_LOCK_ROW_GC>(
          *scheduler, GC_MTL_PROBE_STATES[2], schedule_ts + 2);
  scheduler->notify();
  const bool all_entered = wait_all_probe_tasks_entered(30L * 1000L * 1000L);

  // From this point the test deliberately destroys its only tenant through
  // the production MTL callbacks registered by MockTenantModuleEnv::init().
  // Keep all assertions until after teardown, when no MTL pointer is valid.
  MockTenantModuleEnv::get_instance().release_guard();
  MockTenantModuleEnv::get_instance().destroy();

  EXPECT_EQ(OB_SUCCESS, submit_rets[0]);
  EXPECT_EQ(OB_SUCCESS, submit_rets[1]);
  EXPECT_EQ(OB_SUCCESS, submit_rets[2]);
  EXPECT_TRUE(all_entered);
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(GC_MTL_PROBE_STATES[i].entered_.load(
        std::memory_order_acquire));
    EXPECT_TRUE(GC_MTL_PROBE_STATES[i].exited_.load(
        std::memory_order_acquire));
    EXPECT_EQ(OB_SERVICE_STOPPED, GC_MTL_PROBE_STATES[i].stop_ret_.load(
        std::memory_order_acquire));
    EXPECT_TRUE(GC_MTL_PROBE_STATES[i].scheduler_stopped_.load(
        std::memory_order_acquire));
    EXPECT_TRUE(GC_MTL_PROBE_STATES[i].destructor_scheduler_stopped_.load(
        std::memory_order_acquire));
    EXPECT_EQ(1, GC_MTL_PROBE_STATES[i].destructor_count_.load(
        std::memory_order_acquire));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ss_gc_mtl_lifecycle.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

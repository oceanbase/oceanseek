/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#define protected public
#define private public

#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/async_index/ob_async_index_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

class TestAsyncIndexRefreshWorker : public ::testing::Test
{
public:
  TestAsyncIndexRefreshWorker() {}
  virtual ~TestAsyncIndexRefreshWorker() {}

  static void SetUpTestCase()
  {
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    ASSERT_EQ(OB_SUCCESS,
              lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001));
  }

  static void TearDownTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
              lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001));
  }

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    scn.convert_for_tx(val);
    return scn;
  }

  static void make_sql(ObSqlString &sql)
  {
    ASSERT_EQ(OB_SUCCESS, sql.assign("INSERT INTO `db`.`idx` SELECT 1"));
  }

  static ObRefreshTaskStat make_finished_stat(
      const ObTabletID &tablet_id,
      const int64_t dop,
      const int64_t elapsed_us)
  {
    ObRefreshTaskStat stat;
    stat.tablet_id_ = tablet_id;
    stat.start_time_ = 100;
    stat.end_time_ = stat.start_time_ + elapsed_us;
    stat.result_ = OB_SUCCESS;
    stat.refresh_parallelism_ = dop;
    return stat;
  }

  // Build a refresh task param bundling the scheduling inputs. `sql` is
  // referenced (not copied), so it must outlive the returned param.
  static ObAsyncIndexRefreshTaskParam make_param(
      const uint64_t tenant_id, const ObLSID &ls_id,
      const ObTabletID &data_tablet_id, const ObTabletID &index_tablet_id,
      const SCN &start_scn, const SCN &target_scn, const int64_t refresh_id,
      const ObAsyncRefreshType refresh_type, const int64_t parallelism,
      const ObSqlString &sql, const int64_t schema_version,
      ObLSAsyncIndexScheduler *ls_scheduler = nullptr)
  {
    ObAsyncIndexRefreshTaskParam param;
    param.tenant_id_ = tenant_id;
    param.ls_id_ = ls_id;
    param.data_tablet_id_ = data_tablet_id;
    param.index_tablet_id_ = index_tablet_id;
    param.start_refresh_scn_ = start_scn;
    param.target_refresh_scn_ = target_scn;
    param.refresh_id_ = refresh_id;
    param.refresh_type_ = refresh_type;
    param.parallelism_ = parallelism;
    param.sql_ = &sql;
    param.schema_version_ = schema_version;
    param.ls_scheduler_ = ls_scheduler;
    return param;
  }

  // Init an LS scheduler for unit tests that only need ls_id / epoch / retry
  // state and do not have a real ObLS instance.
  static void init_scheduler_for_test(ObLSAsyncIndexScheduler &scheduler,
                                      const ObLSID &ls_id,
                                      const uint64_t tenant_id,
                                      ObAsyncIndexService *service)
  {
    UNUSED(service);
    scheduler.ls_ = nullptr;
    scheduler.ls_id_ = ls_id;
    scheduler.tenant_id_ = tenant_id;
    scheduler.refresher_ = nullptr;
    scheduler.is_leader_ = true;
    scheduler.need_do_for_switch_ = false;
    scheduler.leader_epoch_ = 0;
    scheduler.is_inited_ = true;
    scheduler.stopped_ = false;
  }
};

TEST_F(TestAsyncIndexRefreshWorker, test_init)
{
  ObAsyncIndexRefreshWorker worker;
  // invalid tenant id
  ASSERT_EQ(OB_INVALID_ARGUMENT, worker.init(OB_INVALID_TENANT_ID, 0));
  // valid init
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, /*tg_def_id*/0));
  ASSERT_TRUE(worker.is_inited_);
  ASSERT_FALSE(worker.is_stopped_);
  ASSERT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE, worker.task_pool_.free_count_);
  // host pointers all set, every slot free and not in-flight
  for (int32_t i = 0; i < OB_ASYNC_INDEX_TASK_QUEUE_SIZE; ++i) {
    ASSERT_EQ(&worker, worker.task_pool_.objs_[i].host_);
    ASSERT_FALSE(worker.task_pool_.in_use_[i]);
  }
  // double init
  ASSERT_EQ(OB_INIT_TWICE, worker.init(1001, 0));
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_refresh_dop_admission_sum)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, 0));

  // Configure a tenant-level hint DOP budget and verify that a task is admitted
  // only when its complete reservation fits.
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
  ASSERT_TRUE(tenant_config.is_valid());
  const int64_t old_dop_limit = tenant_config->_async_index_refresh_dop_limit;
  tenant_config->_async_index_refresh_dop_limit = 4;

  ASSERT_EQ(0, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_SUCCESS, worker.try_acquire_refresh_dop(3));
  ASSERT_EQ(3, worker.get_running_refresh_dop());
  // The second tablet must request its full DOP; 3 + 2 exceeds the limit.
  ASSERT_EQ(OB_EAGAIN, worker.try_acquire_refresh_dop(2));
  ASSERT_EQ(3, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_SUCCESS, worker.try_acquire_refresh_dop(1));
  ASSERT_EQ(4, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_EAGAIN, worker.try_acquire_refresh_dop(1));

  worker.release_refresh_dop(3);
  ASSERT_EQ(1, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_SUCCESS, worker.try_acquire_refresh_dop(2));
  ASSERT_EQ(3, worker.get_running_refresh_dop());

  // Restore the tenant configuration before leaving the test.
  tenant_config->_async_index_refresh_dop_limit = old_dop_limit;
  worker.release_refresh_dop(1);
  worker.release_refresh_dop(2);
  ASSERT_EQ(0, worker.get_running_refresh_dop());
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_refresh_dop_dynamic_limit_and_request_cap)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, 0));

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
  ASSERT_TRUE(tenant_config.is_valid());
  const int64_t old_dop_limit = tenant_config->_async_index_refresh_dop_limit;
  tenant_config->_async_index_refresh_dop_limit = 2;

  // A request larger than the configured limit must be rejected atomically
  // without leaving a partial reservation.
  ASSERT_EQ(OB_STATE_NOT_MATCH, worker.try_acquire_refresh_dop(3));
  ASSERT_EQ(0, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_SUCCESS, worker.try_acquire_refresh_dop(2));
  ASSERT_EQ(2, worker.get_running_refresh_dop());

  // Lowering the limit does not revoke an admitted request, but prevents new
  // requests until the old request releases its DOP.
  tenant_config->_async_index_refresh_dop_limit = 1;
  ASSERT_EQ(OB_EAGAIN, worker.try_acquire_refresh_dop(1));
  ASSERT_EQ(2, worker.get_running_refresh_dop());
  worker.release_refresh_dop(2);
  ASSERT_EQ(0, worker.get_running_refresh_dop());
  ASSERT_EQ(OB_SUCCESS, worker.try_acquire_refresh_dop(1));
  ASSERT_EQ(1, worker.get_running_refresh_dop());

  tenant_config->_async_index_refresh_dop_limit = old_dop_limit;
  worker.release_refresh_dop(1);
  ASSERT_EQ(0, worker.get_running_refresh_dop());
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_pool_pop_push)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  // pop one task
  ObAsyncIndexRefreshTask *t1 = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t1);
  ASSERT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE - 1, worker.task_pool_.free_count_);
  ASSERT_EQ(1, worker.get_in_flight_count());

  // pop second
  ObAsyncIndexRefreshTask *t2 = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t2);
  ASSERT_NE(t1, t2);
  ASSERT_EQ(2, worker.get_in_flight_count());

  // return them
  worker.task_pool_.release(t1);
  ASSERT_EQ(1, worker.get_in_flight_count());
  worker.task_pool_.release(t2);
  ASSERT_EQ(0, worker.get_in_flight_count());
  ASSERT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE, worker.task_pool_.free_count_);

  worker.destroy();
}

// Regression: with >=2 tasks in flight, returning them out of borrow order must
// release each task's OWN slot. The old counter-based push_back_ freed the wrong
// slot, leaving a still-borrowed slot reusable (and corrupting it on next pop).
TEST_F(TestAsyncIndexRefreshWorker, test_pool_out_of_order_return)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObAsyncIndexRefreshTask *t1 = worker.task_pool_.acquire();
  ObAsyncIndexRefreshTask *t2 = worker.task_pool_.acquire();
  ObAsyncIndexRefreshTask *t3 = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t1);
  ASSERT_NE(nullptr, t2);
  ASSERT_NE(nullptr, t3);
  ASSERT_EQ(3, worker.get_in_flight_count());

  // return the FIRST-borrowed task (not the last) -> FIFO completion order
  worker.task_pool_.release(t1);
  ASSERT_EQ(2, worker.get_in_flight_count());
  // t2 and t3 must still be marked in-flight; t1's slot must be free again
  ASSERT_TRUE(worker.task_pool_.in_use_[t2 - worker.task_pool_.objs_]);
  ASSERT_TRUE(worker.task_pool_.in_use_[t3 - worker.task_pool_.objs_]);
  ASSERT_FALSE(worker.task_pool_.in_use_[t1 - worker.task_pool_.objs_]);

  // the next pop must reuse t1's freed slot, never a still-borrowed one
  ObAsyncIndexRefreshTask *t4 = worker.task_pool_.acquire();
  ASSERT_EQ(t1, t4);
  ASSERT_NE(t4, t2);
  ASSERT_NE(t4, t3);
  ASSERT_EQ(3, worker.get_in_flight_count());

  worker.task_pool_.release(t2);
  worker.task_pool_.release(t3);
  worker.task_pool_.release(t4);
  ASSERT_EQ(0, worker.get_in_flight_count());
  ASSERT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE, worker.task_pool_.free_count_);

  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_pool_exhaustion)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  // drain the whole pool
  for (int32_t i = 0; i < OB_ASYNC_INDEX_TASK_QUEUE_SIZE; ++i) {
    ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
    ASSERT_NE(nullptr, t);
  }
  ASSERT_EQ(0, worker.task_pool_.free_count_);
  // next pop returns nullptr (exhausted)
  ASSERT_EQ(nullptr, worker.task_pool_.acquire());

  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_has_task_dedup)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  const SCN target_scn = make_scn(200);
  ObSqlString sql;
  make_sql(sql);
  // borrow a task and fill it as if enqueued
  ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
              make_scn(100), target_scn, 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1)));

  // has_task should find it (scans the in-flight slots)
  ASSERT_TRUE(worker.has_task(ObTabletID(200002), target_scn));
  // different tablet / scn -> not found
  ASSERT_FALSE(worker.has_task(ObTabletID(999999), target_scn));
  ASSERT_FALSE(worker.has_task(ObTabletID(200002), make_scn(300)));

  // return it; now no in-flight tasks match
  worker.task_pool_.release(t);
  ASSERT_FALSE(worker.has_task(ObTabletID(200002), target_scn));

  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_has_in_flight_task)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  // nothing in-flight yet
  ASSERT_FALSE(worker.has_in_flight_task(ObTabletID(200002)));

  // borrow a task for tablet 200002
  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
              make_scn(100), make_scn(200), 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1)));
  ASSERT_TRUE(worker.has_in_flight_task(ObTabletID(200002)));
  ASSERT_FALSE(worker.has_in_flight_task(ObTabletID(999999)));

  // return it -> no longer in-flight
  worker.task_pool_.release(t);
  ASSERT_FALSE(worker.has_in_flight_task(ObTabletID(200002)));

  worker.destroy();
}

// LS-level leader epoch fencing: a switch on the LS scheduler bumps its epoch,
// and the task's handle() checks against the LS scheduler's epoch.
TEST_F(TestAsyncIndexRefreshWorker, test_ls_epoch_fences_residual_task)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObLSAsyncIndexScheduler scheduler;
  ObAsyncIndexService service;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);
  ASSERT_EQ(OB_SUCCESS, scheduler.switch_to_leader());

  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
              make_scn(100), make_scn(200), 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1)));
  // stamp the current LS epoch
  t->ls_scheduler_ = &scheduler;
  t->stat_.leader_epoch_ = scheduler.get_leader_epoch();

  // a leader switch happens on this LS -> epoch advances
  scheduler.switch_to_follower_forcedly();

  // handle() must fence the stale task and NOT retry it
  worker.handle(t);
  ASSERT_EQ(0, worker.get_in_flight_count());
  // fenced -> dropped, not retried (retry list on scheduler should be empty
  // because switch_to_follower already cancelled it)
  ASSERT_EQ(0, scheduler.get_retry_tasks().get_size());

  worker.destroy();
  scheduler.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_handle_success_returns_to_pool)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
              make_scn(100), make_scn(200), 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1)));
  t->ls_scheduler_ = &scheduler;
  t->stat_.leader_epoch_ = scheduler.get_leader_epoch();
  ASSERT_EQ(1, worker.get_in_flight_count());

  // handle() executes the task and returns it to pool (mock always succeeds)
  worker.handle(t);
  ASSERT_EQ(0, worker.get_in_flight_count());

  worker.destroy();
  scheduler.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_add_retry_task_to_ls_scheduler)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  // build a failed task and push it to the LS scheduler's retry list
  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask *t = worker.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
              make_scn(100), make_scn(200), 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 7777)));
  t->ls_scheduler_ = &scheduler;
  t->stat_.result_ = OB_TIMEOUT;

  ASSERT_EQ(OB_SUCCESS, worker.add_retry_task_(*t));
  ASSERT_EQ(1, scheduler.get_retry_tasks().get_size());

  // verify the cloned node
  ObRetryTaskNode *node = scheduler.get_retry_tasks().get_first();
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(ObTabletID(200002), node->stat_.tablet_id_);
  ASSERT_EQ(ObLSID(1001), node->stat_.ls_id_);
  ASSERT_EQ(1, node->stat_.retry_cnt_);
  ASSERT_EQ(7777, node->stat_.schema_version_);
  ASSERT_EQ(sql.length(), node->sql_.length());

  worker.task_pool_.release(t);
  worker.destroy();
  scheduler.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_dop_quota_defer_does_not_consume_retry_budget)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask task;
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(
      1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
      make_scn(100), make_scn(200), 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 7777,
      &scheduler)));
  task.stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT;
  task.dop_quota_deferred_ = true;

  ASSERT_TRUE(task.need_retry());
  ASSERT_EQ(OB_SUCCESS, worker.add_retry_task_(task));
  ASSERT_EQ(1, scheduler.get_retry_tasks().get_size());
  ObRetryTaskNode *node = scheduler.get_retry_tasks().get_first();
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_CNT, node->stat_.retry_cnt_);
  ASSERT_EQ(OB_SUCCESS, node->stat_.result_);
  ASSERT_EQ(0, node->stat_.end_time_);

  worker.destroy();
  scheduler.destroy();
}

// A task that has exhausted its retry budget must NOT be retried again, so it
// falls into the finished ring as an observable terminal failure.
TEST_F(TestAsyncIndexRefreshWorker, test_need_retry_max_cnt_gives_up)
{
  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  ObAsyncIndexRefreshTask task;
  task.ls_scheduler_ = &scheduler;
  task.stat_.leader_epoch_ = scheduler.get_leader_epoch();
  task.stat_.result_ = OB_TIMEOUT;  // a retryable error

  // Below the cap: still retryable.
  task.stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT - 1;
  ASSERT_TRUE(task.need_retry());

  // At/above the cap: give up.
  task.stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT;
  ASSERT_FALSE(task.need_retry());
  task.stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT + 5;
  ASSERT_FALSE(task.need_retry());

  scheduler.destroy();
}

// When a per-LS retry list reaches OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN, new retry
// nodes are rejected (reject-new) with OB_SIZE_OVERFLOW and the dropped counter
// is incremented; the list never grows past the cap.
TEST_F(TestAsyncIndexRefreshWorker, test_retry_list_cap_rejects_new)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTask t;
  ASSERT_EQ(OB_SUCCESS, t.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001),
              ObTabletID(200002), make_scn(100), make_scn(200), 1,
              ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 7777)));
  t.ls_scheduler_ = &scheduler;
  t.stat_.result_ = OB_TIMEOUT;

  // Fill the retry list up to the cap; every add succeeds.
  for (int64_t i = 0; i < OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN; ++i) {
    ASSERT_EQ(OB_SUCCESS, worker.add_retry_task_(t));
  }
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN, scheduler.retry_task_count());
  ASSERT_EQ(0, worker.get_dropped_retry_cnt());

  // The next add is rejected and counted; the list does not grow.
  ASSERT_EQ(OB_SIZE_OVERFLOW, worker.add_retry_task_(t));
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN, scheduler.retry_task_count());
  ASSERT_EQ(1, worker.get_dropped_retry_cnt());

  scheduler.cancel_retry_tasks();
  worker.destroy();
  scheduler.destroy();
}

// The recent-finished ring buffer retains the most recent completed task stats
// in oldest -> newest order and overwrites the oldest once full.
TEST_F(TestAsyncIndexRefreshWorker, test_recent_finished_ring)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  // empty initially
  ObSEArray<ObRefreshTaskStat, 16> stats;
  ASSERT_EQ(OB_SUCCESS, worker.get_recent_finished_stats(stats));
  ASSERT_EQ(0, stats.count());

  // record a handful (fewer than ring size): order preserved, no overwrite
  const int64_t small = 5;
  for (int64_t i = 0; i < small; ++i) {
    ObRefreshTaskStat stat;
    stat.refresh_id_ = i;
    worker.record_finished_task_(stat);
  }
  ASSERT_EQ(OB_SUCCESS, worker.get_recent_finished_stats(stats));
  ASSERT_EQ(small, stats.count());
  for (int64_t i = 0; i < small; ++i) {
    ASSERT_EQ(i, stats.at(i).refresh_id_);  // oldest -> newest
  }

  // overflow the ring: only the most recent OB_ASYNC_INDEX_FINISHED_RING_SIZE
  // entries survive, oldest -> newest.
  const int64_t total = OB_ASYNC_INDEX_FINISHED_RING_SIZE + small + 7;
  for (int64_t i = small; i < total; ++i) {
    ObRefreshTaskStat stat;
    stat.refresh_id_ = i;
    worker.record_finished_task_(stat);
  }
  ASSERT_EQ(OB_SUCCESS, worker.get_recent_finished_stats(stats));
  ASSERT_EQ(OB_ASYNC_INDEX_FINISHED_RING_SIZE, stats.count());
  const int64_t oldest_id = total - OB_ASYNC_INDEX_FINISHED_RING_SIZE;
  for (int64_t i = 0; i < OB_ASYNC_INDEX_FINISHED_RING_SIZE; ++i) {
    ASSERT_EQ(oldest_id + i, stats.at(i).refresh_id_);
  }

  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_adaptive_parallelism_policy)
{
  ObAsyncIndexRefreshWorker worker;
  const ObTabletID tablet_id(200001);
  const ObTabletID downscale_tablet_id(200002);
  const ObTabletID projected_tablet_id(200003);
  const int64_t interval_us = 1000;
  int64_t now_us = 1000;
  int64_t dop = 0;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  // Cold starts and COMPLETE refreshes use the minimum DOP.
  worker.get_adaptive_parallelism_(
      tablet_id, 1, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);
  worker.get_adaptive_parallelism_(
      tablet_id, 1, interval_us, true, 16, now_us++, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);

  // Two slow samples at the same DOP raise at most one level.
  worker.record_adaptive_sample_(
      make_finished_stat(tablet_id, OB_ASYNC_INDEX_MIN_PARALLELISM, 900), now_us++);
  worker.record_adaptive_sample_(
      make_finished_stat(tablet_id, OB_ASYNC_INDEX_MIN_PARALLELISM, 900), now_us++);
  worker.get_adaptive_parallelism_(
      tablet_id, 2, -1 /* refresh disabled */, false, 16, now_us++, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);
  worker.get_adaptive_parallelism_(
      tablet_id, 2, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(2, dop);

  // Changing DOP resets history, so one new sample cannot raise it again.
  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 2, 900), now_us++);
  worker.get_adaptive_parallelism_(
      tablet_id, 2, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(2, dop);
  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 2, 900), now_us++);
  worker.get_adaptive_parallelism_(
      tablet_id, 2, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(3, dop);

  // No async-index hard cap is applied when the configured limit is larger.
  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 4, 900), now_us++);
  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 4, 900), now_us++);
  worker.get_adaptive_parallelism_(
      tablet_id, 2, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(5, dop);

  // Backlog prevents downscale; without backlog, two fast samples lower one level.
  worker.record_adaptive_sample_(make_finished_stat(downscale_tablet_id, 2, 300), now_us++);
  worker.record_adaptive_sample_(make_finished_stat(downscale_tablet_id, 2, 300), now_us++);
  worker.get_adaptive_parallelism_(
      downscale_tablet_id, 2, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(2, dop);
  worker.get_adaptive_parallelism_(
      downscale_tablet_id, 1, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);

  // Keep DOP 2 when the projected DOP-1 duration would exceed the slow threshold.
  worker.record_adaptive_sample_(make_finished_stat(projected_tablet_id, 2, 500), now_us++);
  worker.record_adaptive_sample_(make_finished_stat(projected_tablet_id, 2, 500), now_us++);
  worker.get_adaptive_parallelism_(
      projected_tablet_id, 1, interval_us, false, 16, now_us++, dop);
  ASSERT_EQ(2, dop);
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_adaptive_parallelism_resets_on_cap_drop)
{
  ObAsyncIndexRefreshWorker worker;
  const ObTabletID tablet_id(200001);
  int64_t now_us = 1000;
  int64_t dop = 0;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 4, 300), now_us++);
  worker.record_adaptive_sample_(make_finished_stat(tablet_id, 4, 300), now_us++);

  worker.get_adaptive_parallelism_(tablet_id, 1, 1000, false, 2, now_us++, dop);
  ASSERT_EQ(2, dop);

  // The old DOP-4 samples cannot trigger an immediate DOP-2 to DOP-1 downscale.
  worker.get_adaptive_parallelism_(tablet_id, 1, 1000, false, 2, now_us++, dop);
  ASSERT_EQ(2, dop);
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_adaptive_parallelism_filters_complete_and_ttl)
{
  ObAsyncIndexRefreshWorker worker;
  const ObTabletID tablet_id(200001);
  const ObTabletID ttl_tablet_id(200002);
  int64_t now_us = 1000;
  int64_t dop = 0;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));

  ObRefreshTaskStat stat = make_finished_stat(tablet_id, 3, 900);
  stat.is_complete_refresh_ = true;
  worker.record_adaptive_sample_(stat, now_us++);
  ASSERT_EQ(0, worker.adaptive_dop_state_count_);

  worker.record_adaptive_sample_(make_finished_stat(ttl_tablet_id, 3, 900), now_us++);
  worker.record_adaptive_sample_(make_finished_stat(ttl_tablet_id, 3, 900), now_us++);
  worker.get_adaptive_parallelism_(
      ttl_tablet_id, 1, 1000, false, 4,
      now_us + 11L * 60L * 1000L * 1000L, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_adaptive_state_cache_is_bounded)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));
  for (int64_t i = 0; i <= OB_ASYNC_INDEX_TASK_QUEUE_SIZE; ++i) {
    worker.record_adaptive_sample_(
        make_finished_stat(ObTabletID(400000 + i), 3, 700), i + 1);
  }
  ASSERT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE, worker.adaptive_dop_state_count_);

  int64_t dop = 0;
  worker.get_adaptive_parallelism_(
      ObTabletID(400000), 1, 1000, false, 4,
      OB_ASYNC_INDEX_TASK_QUEUE_SIZE + 2, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);
  worker.get_adaptive_parallelism_(
      ObTabletID(400000 + OB_ASYNC_INDEX_TASK_QUEUE_SIZE), 1, 1000,
      false, 4, OB_ASYNC_INDEX_TASK_QUEUE_SIZE + 2, dop);
  ASSERT_EQ(3, dop);
  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshWorker, test_stop_and_destroy)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, 0));
  ASSERT_FALSE(worker.is_stopped_);

  worker.stop();
  ASSERT_TRUE(worker.is_stopped_);

  worker.destroy();
  ASSERT_FALSE(worker.is_inited_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

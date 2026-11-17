/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/time/ob_time_utility.h"
#define private public
#define protected public
#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace
{
// Stub task execution outcomes. Keep the real Worker, need_retry(), retry queue,
// history ring and pool release so the rejected-retry lifecycle is exercised.
int execution_ret = OB_EAGAIN;
int execution_result = OB_SUCCESS;
bool defer_for_dop = true;
bool change_epoch = false;
bool stop_scheduler = false;
const char *const TEST_REFRESH_SQL = "INSERT INTO `db`.`idx` SELECT 1";
}

int ObAsyncIndexRefreshTask::handle()
{
  ObRefreshTaskStat stat;
  copy_stat(stat);
  stat.start_time_ = ObTimeUtility::current_time();
  dop_quota_deferred_ = defer_for_dop;
  if (!defer_for_dop) {
    stat.result_ = execution_result;
    stat.end_time_ = stat.start_time_;
  }
  publish_stat(stat);
  if (change_epoch) {
    ++ls_scheduler_->leader_epoch_;
  }
  if (stop_scheduler) {
    ls_scheduler_->stopped_ = true;
  }
  return execution_ret;
}

namespace unittest
{
class TestAsyncIndexRefreshWorkerTerminal : public ::testing::Test
{
public:
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

  void SetUp() override
  {
    execution_ret = OB_EAGAIN;
    execution_result = OB_SUCCESS;
    defer_for_dop = true;
    change_epoch = false;
    stop_scheduler = false;
    ASSERT_EQ(OB_SUCCESS, worker_.init(1001, 0));
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(1001));
    ASSERT_TRUE(tenant_config.is_valid());
    ASSERT_TRUE(tenant_config->_enable_async_index_refresh);
    scheduler_.ls_id_ = ObLSID(1001);
    scheduler_.tenant_id_ = 1001;
    scheduler_.is_leader_ = true;
    scheduler_.need_do_for_switch_ = false;
    scheduler_.leader_epoch_ = 0;
    scheduler_.is_inited_ = true;
    scheduler_.stopped_ = false;
  }

  ObAsyncIndexRefreshTask *make_task()
  {
    ObAsyncIndexRefreshTask *task = worker_.task_pool_.acquire();
    if (nullptr != task) {
      task->stat_.tenant_id_ = 1001;
      task->stat_.ls_id_ = ObLSID(1001);
      task->stat_.tablet_id_ = ObTabletID(200002);
      task->stat_.refresh_task_id_ = 36186872;
      task->stat_.leader_epoch_ = scheduler_.get_leader_epoch();
      task->ls_scheduler_ = &scheduler_;
      // Retry nodes clone the generated SQL even though handle() is stubbed.
      EXPECT_EQ(OB_SUCCESS, task->sql_.assign(TEST_REFRESH_SQL));
    }
    return task;
  }

  void fill_retry_queue()
  {
    ObAsyncIndexRefreshTask task;
    task.ls_scheduler_ = &scheduler_;
    task.stat_.leader_epoch_ = scheduler_.get_leader_epoch();
    ASSERT_EQ(OB_SUCCESS, task.sql_.assign(TEST_REFRESH_SQL));
    for (int64_t i = 0; i < OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN; ++i) {
      ASSERT_EQ(OB_SUCCESS, worker_.add_retry_task_(task));
    }
    ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN, scheduler_.retry_task_count());
  }

  void check_finished(const int result)
  {
    ObSEArray<ObRefreshTaskStat, 1> stats;
    ASSERT_EQ(OB_SUCCESS, worker_.get_recent_finished_stats(stats));
    ASSERT_EQ(1, stats.count());
    EXPECT_EQ(36186872, stats.at(0).refresh_task_id_);
    EXPECT_GT(stats.at(0).end_time_, 0);
    EXPECT_GE(stats.at(0).end_time_, stats.at(0).start_time_);
    EXPECT_EQ(result, stats.at(0).result_);
    EXPECT_EQ(0, worker_.get_in_flight_count());
    EXPECT_EQ(OB_ASYNC_INDEX_TASK_QUEUE_SIZE, worker_.task_pool_.free_count_);
  }

  ObAsyncIndexRefreshWorker worker_;
  ObLSAsyncIndexScheduler scheduler_;
};

TEST_F(TestAsyncIndexRefreshWorkerTerminal, dop_deferred_retry_queue_full)
{
  ASSERT_NO_FATAL_FAILURE(fill_retry_queue());
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  task->stat_.is_ivf_index_ = true; // DISK_BBQ also uses the IVF marker.
  worker_.handle(task);
  check_finished(OB_SIZE_OVERFLOW);
  EXPECT_EQ(OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN, scheduler_.retry_task_count());
  EXPECT_EQ(1, worker_.get_dropped_retry_cnt());
  EXPECT_EQ(0, worker_.adaptive_dop_state_count_);
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, search_deferred_retry_queue_full)
{
  ASSERT_NO_FATAL_FAILURE(fill_retry_queue());
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  task->stat_.is_search_index_ = true;
  worker_.handle(task);
  check_finished(OB_SIZE_OVERFLOW);
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, dop_deferred_retry_accepted)
{
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  task->stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT;
  worker_.handle(task);
  ObSEArray<ObRefreshTaskStat, 1> stats;
  ASSERT_EQ(OB_SUCCESS, worker_.get_recent_finished_stats(stats));
  EXPECT_EQ(0, stats.count());
  ASSERT_EQ(1, scheduler_.retry_task_count());
  EXPECT_STREQ(TEST_REFRESH_SQL, scheduler_.get_retry_tasks().get_first()->sql_.ptr());
  const ObRefreshTaskStat &stat = scheduler_.get_retry_tasks().get_first()->stat_;
  EXPECT_EQ(36186872, stat.refresh_task_id_);
  EXPECT_EQ(0, stat.end_time_);
  EXPECT_EQ(OB_SUCCESS, stat.result_);
  EXPECT_EQ(OB_ASYNC_INDEX_MAX_RETRY_CNT, stat.retry_cnt_);
  EXPECT_EQ(0, worker_.get_in_flight_count());
  EXPECT_EQ(0, worker_.get_give_up_task_cnt());
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, execution_error_retry_queue_full)
{
  ASSERT_NO_FATAL_FAILURE(fill_retry_queue());
  defer_for_dop = false;
  execution_ret = execution_result = OB_TIMEOUT;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  worker_.handle(task);
  check_finished(OB_SIZE_OVERFLOW);
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, dop_deferred_epoch_changed)
{
  change_epoch = true;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  worker_.handle(task);
  check_finished(OB_EAGAIN);
  EXPECT_EQ(0, scheduler_.retry_task_count());
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, dop_deferred_scheduler_stopped)
{
  stop_scheduler = true;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  worker_.handle(task);
  check_finished(OB_IN_STOP_STATE);
  EXPECT_EQ(0, scheduler_.retry_task_count());
  EXPECT_EQ(1, worker_.get_dropped_retry_cnt());
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, successful_completion)
{
  defer_for_dop = false;
  execution_ret = execution_result = OB_SUCCESS;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  worker_.handle(task);
  check_finished(OB_SUCCESS);
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, retry_enqueue_error)
{
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  task->ls_scheduler_ = nullptr;
  worker_.handle(task);
  check_finished(OB_ERR_UNEXPECTED);
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, execution_error_retry_exhausted)
{
  defer_for_dop = false;
  execution_ret = execution_result = OB_TIMEOUT;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  task->stat_.retry_cnt_ = OB_ASYNC_INDEX_MAX_RETRY_CNT;
  worker_.handle(task);
  check_finished(OB_TIMEOUT);
  EXPECT_EQ(0, scheduler_.retry_task_count());
  EXPECT_EQ(1, worker_.get_give_up_task_cnt());
}

TEST_F(TestAsyncIndexRefreshWorkerTerminal, preserve_no_need_update_result)
{
  defer_for_dop = false;
  execution_ret = OB_SUCCESS;
  execution_result = OB_NO_NEED_UPDATE;
  ObAsyncIndexRefreshTask *task = make_task();
  ASSERT_NE(nullptr, task);
  worker_.handle(task);
  check_finished(OB_NO_NEED_UPDATE);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

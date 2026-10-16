/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define protected public
#define private public

#include "storage/async_index/ob_async_index_refresh_task.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_async_index_deps.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

// Minimal provider returning conservative defaults for the handle() tests.
class TaskTestProvider : public ObIAsyncIndexDepProvider
{
public:
  TaskTestProvider() {}
  int get_publish_scns(const uint64_t, ObLS *,
      const ObIArray<ObTabletID> &, ObIArray<SCN> &scns) override
  { scns.reset(); return OB_SUCCESS; }
  int get_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      ObTabletAsyncIndexRefreshInfo &info) override
  { info.reset(); return OB_SUCCESS; }
  int set_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      const ObTabletAsyncIndexRefreshInfo &) override
  { return OB_SUCCESS; }
  int has_incr_dml(const uint64_t, ObLS *, const ObTabletID &, const SCN &, const SCN &,
      bool &has_data) override { has_data = false; return OB_SUCCESS; }
  int get_read_snapshot_version(const uint64_t, SCN &s) override { s = SCN::min_scn(); return OB_SUCCESS; }
  int get_index_minor_sstable_count(const uint64_t, ObLS *, const ObTabletID &, int64_t &c) override
  { c = 0; return OB_SUCCESS; }
  int get_index_delete_tablet_id(const uint64_t, const ObTabletID &, ObTabletID &id) override
  { id.reset(); return OB_SUCCESS; }
};

class TestAsyncIndexRefreshTask : public ::testing::Test
{
public:
  TestAsyncIndexRefreshTask() {}
  virtual ~TestAsyncIndexRefreshTask() {}

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    scn.convert_for_tx(val);
    return scn;
  }

  // A non-empty SQL for tests that want to observe carried SQL.
  static void make_sql(ObSqlString &sql)
  {
    ASSERT_EQ(OB_SUCCESS, sql.assign("INSERT INTO `db`.`idx` SELECT 1"));
  }

  // Build a refresh task param bundling the scheduling inputs. `sql` is
  // referenced (not copied), so it must outlive the returned param.
  static ObAsyncIndexRefreshTaskParam make_param(
      const uint64_t tenant_id, const ObLSID &ls_id,
      const ObTabletID &data_tablet_id, const ObTabletID &index_tablet_id,
      const SCN &start_scn, const SCN &target_scn, const int64_t refresh_id,
      const ObAsyncRefreshType refresh_type, const int64_t parallelism,
      const ObSqlString &sql, const int64_t schema_version,
      ObLSAsyncIndexScheduler *ls_scheduler = nullptr,
      const int64_t refresh_task_id = 0)
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
    param.refresh_task_id_ = refresh_task_id;
    return param;
  }
};

TEST_F(TestAsyncIndexRefreshTask, test_set_task)
{
  ObAsyncIndexRefreshTask task;
  const SCN start_scn = make_scn(100);
  const SCN target_scn = make_scn(200);
  ObSqlString sql;
  make_sql(sql);
  ObAsyncIndexRefreshTaskParam param = make_param(
      1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
      start_scn, target_scn, 42,
      ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777, nullptr, 987654);
  param.is_search_index_ = true;
  ASSERT_EQ(OB_SUCCESS, task.set_task(param));

  ASSERT_EQ(1001, task.stat_.tenant_id_);
  ASSERT_EQ(ObTabletID(200001), task.stat_.data_tablet_id_);
  ASSERT_EQ(ObTabletID(200002), task.stat_.tablet_id_);
  ASSERT_EQ(start_scn, task.stat_.start_refresh_scn_);
  ASSERT_EQ(target_scn, task.stat_.target_refresh_scn_);
  ASSERT_EQ(42, task.stat_.refresh_id_);
  ASSERT_EQ(987654, task.stat_.refresh_task_id_);
  ASSERT_EQ(ASYNC_REFRESH_TYPE_PERIOD, task.stat_.refresh_type_);
  ASSERT_EQ(2, task.stat_.refresh_parallelism_);
  ASSERT_EQ(7777, task.stat_.schema_version_);
  ASSERT_TRUE(task.stat_.is_search_index_);
  ASSERT_EQ(0, task.stat_.retry_cnt_);
  ASSERT_EQ(OB_SUCCESS, task.stat_.result_);
  ASSERT_GT(task.stat_.queue_time_, 0);
  // SQL deep-copied into the task's own buffer
  ASSERT_EQ(sql.length(), task.sql_.length());
  ASSERT_EQ(0, strcmp(sql.ptr(), task.sql_.ptr()));
}

TEST_F(TestAsyncIndexRefreshTask, test_reset)
{
  ObAsyncIndexRefreshTask task;
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), make_scn(200), 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));
  ASSERT_GT(task.sql_.length(), 0);

  task.reset();
  ASSERT_FALSE(task.stat_.is_valid());
  ASSERT_EQ(OB_INVALID_TENANT_ID, task.stat_.tenant_id_);
  ASSERT_EQ(0, task.stat_.queue_time_);
  ASSERT_EQ(0, task.stat_.schema_version_);
  ASSERT_EQ(0, task.sql_.length());  // sql buffer released
}

TEST_F(TestAsyncIndexRefreshTask, test_handle_success)
{
  ObAsyncIndexRefreshTask task;  // host_ is null => schema check skipped
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), make_scn(200), 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));

  ASSERT_EQ(OB_SUCCESS, task.handle());
  // start/end time stamped, result success
  ASSERT_GT(task.stat_.start_time_, 0);
  ASSERT_GE(task.stat_.end_time_, task.stat_.start_time_);
  ASSERT_EQ(OB_SUCCESS, task.stat_.result_);
  // handle() only executes, never regenerates: the carried SQL is untouched
  ASSERT_EQ(sql.length(), task.sql_.length());
  ASSERT_EQ(0, strcmp(sql.ptr(), task.sql_.ptr()));
  // a successful task should not need retry
  ASSERT_FALSE(task.need_retry());
}

TEST_F(TestAsyncIndexRefreshTask, test_handle_executes_with_provider)
{
  // Schema drift is no longer pre-validated in handle(): it is enforced by the
  // SQL's ob_ddl_schema_version hint at execution time. OB_DDL_SCHEMA_VERSION_NOT_MATCH
  // is excluded by is_async_index_retryable_error(). handle() just executes.
  ObAsyncIndexRefreshWorker worker;
  TaskTestProvider provider;
  ASSERT_EQ(OB_SUCCESS, worker.init(1001, /*tg_def_id*/0, &provider));

  ObAsyncIndexRefreshTask task;
  task.host_ = &worker;
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), make_scn(200), 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));

  // no SQL proxy in the unit-test env => executes as a no-op success
  ASSERT_EQ(OB_SUCCESS, task.handle());
  ASSERT_EQ(OB_SUCCESS, task.stat_.result_);
  ASSERT_FALSE(task.need_retry());

  worker.destroy();
}

TEST_F(TestAsyncIndexRefreshTask, test_match_for_dedup)
{
  ObAsyncIndexRefreshTask task;
  const SCN target_scn = make_scn(200);
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), target_scn, 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));

  // same tablet + same target scn -> match
  ASSERT_TRUE(task.match(ObTabletID(200002), target_scn));
  // different tablet -> no match
  ASSERT_FALSE(task.match(ObTabletID(999999), target_scn));
  // different target scn -> no match
  ASSERT_FALSE(task.match(ObTabletID(200002), make_scn(300)));
}

TEST_F(TestAsyncIndexRefreshTask, test_need_retry)
{
  ObAsyncIndexRefreshTask task;
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), make_scn(200), 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));

  // success -> no retry
  task.stat_.result_ = OB_SUCCESS;
  ASSERT_FALSE(task.need_retry());

  // retryable error -> retry
  task.stat_.result_ = OB_TIMEOUT;
  ASSERT_TRUE(task.need_retry());

  // non-retryable errors -> no retry
  task.stat_.result_ = OB_NOT_MASTER;
  ASSERT_FALSE(task.need_retry());

  task.stat_.result_ = OB_SCHEMA_ERROR;
  ASSERT_FALSE(task.need_retry());

  task.stat_.result_ = OB_TABLE_NOT_EXIST;
  ASSERT_FALSE(task.need_retry());
}

TEST_F(TestAsyncIndexRefreshTask, stale_and_unknown_errors_do_not_retry_old_sql)
{
  ObAsyncIndexRefreshTask task;
  const int errors[] = {
    OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST, OB_TABLET_NOT_EXIST,
    OB_INVALID_ARGUMENT, OB_INVALID_QUERY_TIMESTAMP, OB_SNAPSHOT_DISCARDED,
    OB_NOT_MASTER, OB_SIZE_OVERFLOW, OB_ALLOCATE_MEMORY_FAILED,
    OB_DDL_SCHEMA_VERSION_NOT_MATCH, OB_EAGAIN, OB_NEED_RETRY, -123456
  };
  for (int err : errors) {
    task.stat_.result_ = err;
    ASSERT_FALSE(task.need_retry()) << err;
  }
}

TEST_F(TestAsyncIndexRefreshTask, test_retry_delay)
{
  ObAsyncIndexRefreshTask task;
  ObSqlString sql;
  make_sql(sql);
  ASSERT_EQ(OB_SUCCESS, task.set_task(make_param(1001, ObLSID(1001), ObTabletID(200001), ObTabletID(200002),
                make_scn(100), make_scn(200), 42,
                ASYNC_REFRESH_TYPE_PERIOD, 2, sql, 7777)));

  // first 3 retries: immediate
  task.stat_.retry_cnt_ = 0;
  ASSERT_EQ(0, task.get_retry_delay_us());
  task.stat_.retry_cnt_ = 3;
  ASSERT_EQ(0, task.get_retry_delay_us());

  // 4th retry: 5s
  task.stat_.retry_cnt_ = 4;
  ASSERT_EQ(5L * 1000L * 1000L, task.get_retry_delay_us());

  // capped at 5 min for large retry counts
  task.stat_.retry_cnt_ = 100;
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, task.get_retry_delay_us());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#include "storage/async_index/ob_async_index_define.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

class TestAsyncIndexDefine : public ::testing::Test
{
public:
  TestAsyncIndexDefine() {}
  virtual ~TestAsyncIndexDefine() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

TEST_F(TestAsyncIndexDefine, test_refresh_type_enum)
{
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_FALSE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_MAX));
  ASSERT_FALSE(is_valid_async_refresh_type(static_cast<ObAsyncRefreshType>(-1)));

  ASSERT_STREQ("PERIOD", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_STREQ("CREATE_INDEX", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_STREQ("UNKNOWN", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_MAX));
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_reset)
{
  ObRefreshTaskStat stat;
  ASSERT_EQ(OB_INVALID_TENANT_ID, stat.tenant_id_);
  ASSERT_FALSE(stat.data_tablet_id_.is_valid());
  ASSERT_FALSE(stat.tablet_id_.is_valid());
  ASSERT_EQ(0, stat.refresh_id_);
  ASSERT_EQ(0, stat.retry_cnt_);
  ASSERT_EQ(ASYNC_REFRESH_TYPE_PERIOD, stat.refresh_type_);
  ASSERT_EQ(0, stat.queue_time_);
  ASSERT_EQ(0, stat.start_time_);
  ASSERT_EQ(0, stat.end_time_);
  ASSERT_EQ(0, stat.affected_rows_);
  ASSERT_FALSE(stat.start_refresh_scn_.is_valid());
  ASSERT_FALSE(stat.target_refresh_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, stat.result_);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, stat.refresh_parallelism_);
  ASSERT_EQ(0, stat.schema_version_);
  ASSERT_EQ(0, stat.leader_epoch_);
  ASSERT_FALSE(stat.is_search_index_);
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_is_valid)
{
  ObRefreshTaskStat stat;
  // after reset, stat should be invalid (missing required fields)
  ASSERT_FALSE(stat.is_valid());

  // set required fields
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  ASSERT_FALSE(stat.is_valid());

  // invalid refresh type
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_MAX;
  ASSERT_FALSE(stat.is_valid());

  // restore and invalidate tenant_id
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  stat.tenant_id_ = OB_INVALID_TENANT_ID;
  ASSERT_FALSE(stat.is_valid());
}

TEST_F(TestAsyncIndexDefine, test_retry_delay_calculation)
{
  // first 3 retries: immediate (0 delay)
  ASSERT_EQ(0, calc_async_index_retry_delay_us(0));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(1));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(2));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(3));

  // 4th retry: 5s
  ASSERT_EQ(5L * 1000L * 1000L, calc_async_index_retry_delay_us(4));
  // 5th retry: 10s
  ASSERT_EQ(10L * 1000L * 1000L, calc_async_index_retry_delay_us(5));
  // 6th retry: 20s
  ASSERT_EQ(20L * 1000L * 1000L, calc_async_index_retry_delay_us(6));
  // 7th retry: 40s
  ASSERT_EQ(40L * 1000L * 1000L, calc_async_index_retry_delay_us(7));
  // 8th retry: 80s
  ASSERT_EQ(80L * 1000L * 1000L, calc_async_index_retry_delay_us(8));
  // 9th retry: 160s
  ASSERT_EQ(160L * 1000L * 1000L, calc_async_index_retry_delay_us(9));
  // 10th retry: capped at 5 min = 300s
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(10));
  // large retry count: still capped
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(100));
}

TEST_F(TestAsyncIndexDefine, test_retryable_error_codes)
{
  // A new transaction can recover these failures while reusing the original SQL.
  ASSERT_TRUE(is_async_index_retryable_error(OB_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_RPC_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_CONNECT_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_SEND_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_POST_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_ROLLBACKED));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_NEED_ROLLBACK));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_STMT_NEED_RETRY));

  ASSERT_FALSE(is_async_index_retryable_error(OB_SUCCESS));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SCHEMA_ERROR));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TABLE_NOT_EXIST));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TENANT_NOT_EXIST));
  // stale ob_ddl_schema_version hint: SQL fails at resolve, regenerated next round
  ASSERT_FALSE(is_async_index_retryable_error(OB_DDL_SCHEMA_VERSION_NOT_MATCH));
  ASSERT_FALSE(is_async_index_retryable_error(OB_ERR_UNEXPECTED));

  // index tablet lock conflict: timer path skips retry, next round reschedules
  ASSERT_FALSE(is_async_index_retryable_error(OB_EAGAIN));

  // Errors outside the retry whitelist release the batch for the next timer round.
  ASSERT_FALSE(is_async_index_retryable_error(OB_NOT_MASTER));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SIZE_OVERFLOW));
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_to_string)
{
  ObRefreshTaskStat stat;
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.refresh_id_ = 42;
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_CREATE_INDEX;

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  pos = stat.to_string(buf, buf_len);
  ASSERT_GT(pos, 0);
  // verify key fields appear in output
  ASSERT_NE(nullptr, strstr(buf, "tenant_id"));
  ASSERT_NE(nullptr, strstr(buf, "CREATE_INDEX"));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

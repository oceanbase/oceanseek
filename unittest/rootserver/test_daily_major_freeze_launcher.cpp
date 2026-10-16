/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// 场景：sys 豁免保护期，user/meta 按用户创建时间保护，验证十分钟边界、超时、重试和缓存。
// 测试直接调用每日入口及私有判断，不启动 observer，也不等待真实十分钟。
#include <gtest/gtest.h>
#include <string>
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "rootserver/freeze/ob_freeze_reentrant_thread.h"
#include "share/config/ob_server_config.h"
#define private public
#include "rootserver/freeze/ob_daily_major_freeze_launcher.h"
#undef private

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace common::sqlclient;

struct TenantCreateRow
{
  int next_ret_ = OB_SUCCESS;
  int timestamp_ret_ = OB_SUCCESS;
  int64_t create_time_us_ = 1000000000;
};

class TenantCreateResult : public ObMySQLResultImpl
{
public:
  TenantCreateResult(ObMySQLStatement &stmt, const TenantCreateRow &row)
      : ObMySQLResultImpl(stmt), row_(row) {}
  using ObMySQLResultImpl::get_timestamp;
  int next() override { return row_.next_ret_; }
  int get_timestamp(const char *column, const ObTimeZoneInfo *, int64_t &value) const override
  {
    EXPECT_STREQ("gmt_create", column);
    if (OB_SUCCESS == row_.timestamp_ret_) {
      value = row_.create_time_us_;
    }
    return row_.timestamp_ret_;
  }
private:
  TenantCreateRow row_;
};

class TenantCreateResultHandler : public ObISQLResultHandler
{
public:
  explicit TenantCreateResultHandler(TenantCreateRow &row) : statement_(), result_(statement_, row) {}
  ObMySQLResult *mysql_result() override { return &result_; }
private:
  ObMySQLStatement statement_;
  TenantCreateResult result_;
};

class TenantCreateProxy : public ObMySQLProxy
{
public:
  using ObMySQLProxy::read;
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    int ret = read_ret_;
    ++read_count_;
    exec_tenant_id_ = tenant_id;
    last_sql_ = sql;
    remaining_timeout_us_ = ObTimeoutCtx::get_ctx().get_timeout();
    timeout_deadline_us_ = ObTimeoutCtx::get_ctx().get_abs_timeout();
    if (OB_SUCC(ret)) {
      TenantCreateResultHandler *handler = nullptr;
      ret = res.create_handler(handler, row_);
    }
    return ret;
  }

  TenantCreateRow row_;
  int read_ret_ = OB_SUCCESS;
  int64_t read_count_ = 0;
  uint64_t exec_tenant_id_ = OB_INVALID_TENANT_ID;
  int64_t remaining_timeout_us_ = 0;
  int64_t timeout_deadline_us_ = 0;
  std::string last_sql_;
};

class TestableDailyLauncher : public ObDailyMajorFreezeLauncher
{
public:
  TestableDailyLauncher(const uint64_t tenant_id, ObMySQLProxy &proxy)
      : ObDailyMajorFreezeLauncher(tenant_id)
  {
    sql_proxy_ = &proxy;
    is_inited_ = true;
  }
};

TEST(TestDailyMajorFreezeLauncher, reads_logical_user_record)
{
  // user/meta 均查询逻辑 user 的记录。
  const uint64_t tenant_ids[] = {1001, 1002};
  const uint64_t logical_ids[] = {1002, 1002};
  for (int64_t i = 0; i < 2; ++i) {
    TenantCreateProxy proxy;
    TestableDailyLauncher launcher(tenant_ids[i], proxy);
    int64_t create_time_us = -1;
    ASSERT_EQ(OB_SUCCESS, launcher.fetch_tenant_create_time_(create_time_us));
    EXPECT_EQ(proxy.row_.create_time_us_, create_time_us);
    EXPECT_EQ(OB_SYS_TENANT_ID, proxy.exec_tenant_id_);
    EXPECT_EQ("SELECT gmt_create FROM __all_tenant WHERE tenant_id = "
        + std::to_string(logical_ids[i]), proxy.last_sql_);
    EXPECT_EQ(0, launcher.tenant_create_time_us_);
    EXPECT_GT(proxy.remaining_timeout_us_, 0);
    EXPECT_LE(proxy.remaining_timeout_us_, static_cast<int64_t>(GCONF.internal_sql_execute_timeout));
  }
}

TEST(TestDailyMajorFreezeLauncher, sys_tenant_bypasses_grace_without_metadata_query)
{
  // 即使元数据读取会失败，sys 在保护期内外和时钟回拨时都应放行，且不读写创建时间缓存。
  const int64_t elapsed_us[] = {-1, 0, 599999999, 600000000, 600000001};
  for (bool cached : {false, true}) {
    for (int64_t elapsed : elapsed_us) {
      TenantCreateProxy proxy;
      proxy.read_ret_ = OB_TIMEOUT;
      TestableDailyLauncher launcher(OB_SYS_TENANT_ID, proxy);
      const int64_t cached_time_us = cached ? proxy.row_.create_time_us_ : 0;
      launcher.tenant_create_time_us_ = cached_time_us;
      bool need_skip = true;
      ASSERT_EQ(OB_SUCCESS, launcher.check_new_tenant_grace_period_(
          proxy.row_.create_time_us_ + elapsed, need_skip));
      EXPECT_FALSE(need_skip);
      EXPECT_EQ(0, proxy.read_count_);
      EXPECT_EQ(cached_time_us, launcher.tenant_create_time_us_);
      EXPECT_FALSE(launcher.already_launch_);
    }
  }
}

TEST(TestDailyMajorFreezeLauncher, exact_grace_boundary_and_clock_rollback)
{
  const int64_t grace_us = 600000000;
  const int64_t elapsed_us[] = {-1, 0, grace_us - 1, grace_us, grace_us + 1};
  for (uint64_t tenant_id : {1001, 1002}) {
    for (int64_t elapsed : elapsed_us) {
      TenantCreateProxy proxy;
      TestableDailyLauncher launcher(tenant_id, proxy);
      bool need_skip = false;
      ASSERT_EQ(OB_SUCCESS, launcher.check_new_tenant_grace_period_(
          proxy.row_.create_time_us_ + elapsed, need_skip));
      EXPECT_EQ(elapsed < grace_us, need_skip);
      EXPECT_EQ(1, proxy.read_count_);
    }
  }
}

TEST(TestDailyMajorFreezeLauncher, default_inner_sql_timeout_is_used)
{
  // 没有调用方超时时，查询预算采用 inner SQL 配置，不再截为一秒。
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  const int64_t default_timeout_us = GCONF.internal_sql_execute_timeout;
  const int64_t before_us = ObTimeUtility::current_time();
  int64_t create_time_us = 0;
  ASSERT_EQ(OB_SUCCESS, launcher.fetch_tenant_create_time_(create_time_us));
  const int64_t after_us = ObTimeUtility::current_time();
  EXPECT_GE(proxy.timeout_deadline_us_, before_us + default_timeout_us);
  EXPECT_LE(proxy.timeout_deadline_us_, after_us + default_timeout_us);
}

TEST(TestDailyMajorFreezeLauncher, query_failure_does_not_poison_cache)
{
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  proxy.read_ret_ = OB_TIMEOUT;
  bool need_skip = false;
  ASSERT_EQ(OB_TIMEOUT, launcher.check_new_tenant_grace_period_(2000000000, need_skip));
  EXPECT_TRUE(need_skip);
  EXPECT_EQ(0, launcher.tenant_create_time_us_);
  proxy.read_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, launcher.check_new_tenant_grace_period_(2000000000, need_skip));
  EXPECT_FALSE(need_skip);
  EXPECT_EQ(2, proxy.read_count_);
}

TEST(TestDailyMajorFreezeLauncher, invalid_metadata_keeps_outputs_and_cache_empty)
{
  for (int64_t scenario = 0; scenario < 3; ++scenario) {
    TenantCreateProxy proxy;
    TestableDailyLauncher launcher(1002, proxy);
    int expected_ret = OB_SUCCESS;
    if (scenario == 0) {
      proxy.row_.next_ret_ = OB_ITER_END;
      expected_ret = OB_TENANT_NOT_EXIST;
    } else if (scenario == 1) {
      proxy.row_.timestamp_ret_ = OB_ERR_NULL_VALUE;
      expected_ret = OB_ERR_NULL_VALUE;
    } else {
      proxy.row_.create_time_us_ = 0;
      expected_ret = OB_ERR_UNEXPECTED;
    }
    int64_t create_time_us = 123;
    EXPECT_EQ(expected_ret, launcher.fetch_tenant_create_time_(create_time_us));
    EXPECT_EQ(0, create_time_us);
    EXPECT_EQ(0, launcher.tenant_create_time_us_);
  }
}

TEST(TestDailyMajorFreezeLauncher, cache_hit_and_new_instance_keep_original_timestamp)
{
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  const int64_t create_time_us = proxy.row_.create_time_us_;
  bool need_skip = false;
  ASSERT_EQ(OB_SUCCESS, launcher.check_new_tenant_grace_period_(create_time_us, need_skip));
  EXPECT_TRUE(need_skip);
  proxy.read_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_SUCCESS, launcher.check_new_tenant_grace_period_(create_time_us + 600000000, need_skip));
  EXPECT_FALSE(need_skip);
  EXPECT_EQ(1, proxy.read_count_);
  proxy.read_ret_ = OB_SUCCESS;
  TestableDailyLauncher recreated(1002, proxy);
  ASSERT_EQ(OB_SUCCESS, recreated.check_new_tenant_grace_period_(create_time_us + 600000000, need_skip));
  EXPECT_FALSE(need_skip);
  EXPECT_EQ(2, proxy.read_count_);
}

TEST(TestDailyMajorFreezeLauncher, daily_skip_preserves_state_and_propagates_query_error)
{
  // 保护期正常跳过，读取失败保留错误码；两者均不获取配置 guard 或标记已发起合并。
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  proxy.row_.create_time_us_ = ObTimeUtility::current_time();
  EXPECT_EQ(OB_SUCCESS, launcher.try_launch_major_freeze());
  EXPECT_FALSE(launcher.already_launch_);
  TestableDailyLauncher failed(1002, proxy);
  proxy.read_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, failed.try_launch_major_freeze());
  EXPECT_FALSE(failed.already_launch_);
}

TEST(TestDailyMajorFreezeLauncher, shorter_timeout_is_preserved)
{
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  ObTimeoutCtx outer;
  ASSERT_EQ(OB_SUCCESS, outer.set_timeout(100000));
  const int64_t original_deadline = outer.get_abs_timeout();
  int64_t create_time_us = 0;
  ASSERT_EQ(OB_SUCCESS, launcher.fetch_tenant_create_time_(create_time_us));
  EXPECT_GT(proxy.remaining_timeout_us_, 0);
  EXPECT_LE(proxy.remaining_timeout_us_, 100000);
  EXPECT_EQ(original_deadline, outer.get_abs_timeout());
}

TEST(TestDailyMajorFreezeLauncher, expired_timeout_prevents_query)
{
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  ObTimeoutCtx outer;
  ASSERT_EQ(OB_SUCCESS, outer.set_abs_timeout(ObTimeUtility::current_time() - 1));
  int64_t create_time_us = 123;
  EXPECT_EQ(OB_TIMEOUT, launcher.fetch_tenant_create_time_(create_time_us));
  EXPECT_EQ(0, proxy.read_count_);
  EXPECT_EQ(0, create_time_us);
}

TEST(TestDailyMajorFreezeLauncher, invalid_now_and_uninitialized_launcher_do_not_query)
{
  TenantCreateProxy proxy;
  TestableDailyLauncher launcher(1002, proxy);
  bool need_skip = false;
  EXPECT_EQ(OB_INVALID_ARGUMENT, launcher.check_new_tenant_grace_period_(0, need_skip));
  EXPECT_TRUE(need_skip);
  launcher.is_inited_ = false;
  EXPECT_EQ(OB_NOT_INIT, launcher.check_new_tenant_grace_period_(2000000000, need_skip));
  EXPECT_EQ(OB_NOT_INIT, launcher.try_launch_major_freeze());
  EXPECT_EQ(0, proxy.read_count_);
}

} // rootserver 命名空间
} // oceanbase 命名空间

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

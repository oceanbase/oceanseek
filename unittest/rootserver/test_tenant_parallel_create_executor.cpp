/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#include "rootserver/ob_tenant_parallel_create_executor.h"
#undef private
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "lib/worker.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace testing;

namespace rootserver
{

class MockCreateTenantRpcProxy : public ObCommonRpcProxy
{
public:
  MOCK_METHOD2(drop_tenant, int(const ObDropTenantArg &, const ObRpcOpts &));
  MOCK_METHOD2(create_tenant_end, int(const ObCreateTenantEndArg &, const ObRpcOpts &));
};

class CreateTenantTestSchemaService : public ObMultiVersionSchemaService
{
public:
  CreateTenantTestSchemaService() = default;
  ~CreateTenantTestSchemaService() override = default;
};

class TestTenantParallelCreateExecutor : public Test
{
protected:
  TestTenantParallelCreateExecutor() : common_rpc_(&mock_rpc_) {}

  void SetUp() override
  {
    ObCreateTenantArg &arg = executor_.create_tenant_arg_;
    ASSERT_EQ(OB_SUCCESS, arg.tenant_schema_.set_tenant_name("restore_create_failure_test"));
    ASSERT_EQ(OB_SUCCESS, arg.pool_list_.push_back(ObString::make_string("test_pool")));
    arg.is_restore_ = true;
    arg.palf_base_info_.generate_by_default();
    ASSERT_EQ(OB_SUCCESS, arg.recovery_until_scn_.convert_for_logservice(100));
    arg.compatible_version_ = DATA_VERSION_4_3_0_0;
    ASSERT_TRUE(arg.is_valid());

    // Only the RPC proxy is called by finish_create_tenant_; these dependencies
    // provide a valid executor without starting an Observer or issuing SQL.
    executor_.rpc_proxy_ = &srv_rpc_;
    executor_.common_rpc_ = &common_rpc_;
    executor_.sql_proxy_ = &sql_proxy_;
    executor_.schema_service_ = &schema_service_;
    executor_.lst_operator_ = &ls_operator_;
    executor_.location_service_ = &location_service_;
    executor_.rs_mgr_ = &rs_mgr_;
    ASSERT_EQ(OB_SUCCESS, executor_.ctx_.set_timeout(10 * 1000 * 1000));
    ASSERT_EQ(OB_SUCCESS, executor_.create_tenant_schema_result_.init(USER_TENANT_ID));
    executor_.user_tenant_schema_.set_tenant_id(USER_TENANT_ID);
    executor_.meta_tenant_schema_.set_tenant_id(META_TENANT_ID);
  }

  void expect_drop(const bool restore_only, const int drop_ret = OB_SUCCESS)
  {
    const int64_t cleanup_timeout_us = GCONF._ob_ddl_timeout;
    const int64_t earliest_cleanup_deadline = ObTimeUtility::current_time() + cleanup_timeout_us;
    EXPECT_CALL(mock_rpc_, drop_tenant(_, _)).WillOnce(Invoke(
        [this, restore_only, drop_ret, cleanup_timeout_us, earliest_cleanup_deadline]
        (const ObDropTenantArg &arg, const ObRpcOpts &) {
          // Mock dispatch does not forward the proxy's timeout; check the scoped
          // cleanup context and worker deadline used by the caller instead.
          const ObTimeoutCtx &cleanup_ctx = ObTimeoutCtx::get_ctx();
          EXPECT_NE(&executor_.ctx_, &cleanup_ctx);
          // The budget starts at cleanup, not at creation, and follows DDL config.
          EXPECT_GE(cleanup_ctx.get_abs_timeout(), earliest_cleanup_deadline);
          EXPECT_GT(cleanup_ctx.get_timeout(), 0);
          EXPECT_LE(cleanup_ctx.get_timeout(), cleanup_timeout_us);
          EXPECT_EQ(cleanup_ctx.get_abs_timeout(), THIS_WORKER.get_timeout_ts());
          EXPECT_FALSE(cleanup_ctx.is_trx_timeout_set());
          // This also catches a missing force_drop_: the RPC rejects a
          // restore-only request unless that field is set.
          EXPECT_TRUE(arg.is_valid());
          EXPECT_EQ(USER_TENANT_ID, arg.tenant_id_);
          EXPECT_EQ(OB_SYS_TENANT_ID, arg.exec_tenant_id_);
          EXPECT_EQ(0, arg.tenant_name_.compare("restore_create_failure_test"));
          EXPECT_TRUE(arg.if_exist_);
          EXPECT_FALSE(arg.delay_to_drop_);
          EXPECT_EQ(restore_only, arg.drop_only_in_restore_);
          if (restore_only) {
            EXPECT_TRUE(arg.force_drop_);
          }
          return drop_ret;
        }));
  }

  void expect_create_end(const uint64_t tenant_id, const int end_ret = OB_SUCCESS)
  {
    EXPECT_CALL(mock_rpc_, create_tenant_end(_, _)).WillOnce(Invoke(
        [tenant_id, end_ret](const ObCreateTenantEndArg &arg, const ObRpcOpts &) {
          EXPECT_EQ(tenant_id, arg.tenant_id_);
          return end_ret;
        }));
  }

  static constexpr uint64_t USER_TENANT_ID = 1010;
  static constexpr uint64_t META_TENANT_ID = 1009;
  StrictMock<MockCreateTenantRpcProxy> mock_rpc_;
  ObCommonRpcProxy common_rpc_;
  ObSrvRpcProxy srv_rpc_;
  ObMySQLProxy sql_proxy_;
  CreateTenantTestSchemaService schema_service_;
  ObLSTableOperator ls_operator_;
  ObLocationService location_service_;
  ObRsMgr rs_mgr_;
  ObParallelCreateTenantExecutor executor_;
};

TEST_F(TestTenantParallelCreateExecutor, restore_upload_root_key_failure_drops_new_tenant)
{
  expect_drop(true);
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, restore_meta_finish_failure_drops_new_tenant)
{
  InSequence sequence;
  expect_create_end(META_TENANT_ID, OB_TIMEOUT);
  expect_drop(true);
  EXPECT_EQ(OB_TIMEOUT, executor_.finish_create_tenant_(OB_SUCCESS));
}

TEST_F(TestTenantParallelCreateExecutor, restore_expired_deadline_gets_fresh_cleanup_budget)
{
  const int64_t expired_deadline = ObTimeUtility::current_time() - 1;
  ASSERT_EQ(OB_SUCCESS, executor_.ctx_.set_abs_timeout(expired_deadline));
  ASSERT_EQ(OB_SUCCESS, executor_.ctx_.set_trx_timeout_us(1));
  lib::WorkerTimeoutGuard worker_guard(expired_deadline);
  ASSERT_TRUE(executor_.ctx_.is_timeouted());
  expect_drop(true);
  EXPECT_EQ(OB_TIMEOUT, executor_.finish_create_tenant_(OB_TIMEOUT));
  EXPECT_EQ(expired_deadline, executor_.ctx_.get_abs_timeout());
  EXPECT_EQ(expired_deadline, THIS_WORKER.get_timeout_ts());
  EXPECT_EQ(1, executor_.ctx_.get_trx_timeout_us());
  EXPECT_EQ(&executor_.ctx_, &ObTimeoutCtx::get_ctx());
}

TEST_F(TestTenantParallelCreateExecutor, meta_finish_exhausts_deadline_before_cleanup)
{
  const int64_t expired_deadline = ObTimeUtility::current_time() - 1;
  const int64_t worker_deadline = executor_.ctx_.get_abs_timeout();
  lib::WorkerTimeoutGuard worker_guard(worker_deadline);
  InSequence sequence;
  EXPECT_CALL(mock_rpc_, create_tenant_end(_, _)).WillOnce(Invoke(
      [this, expired_deadline](const ObCreateTenantEndArg &arg, const ObRpcOpts &) {
        EXPECT_EQ(META_TENANT_ID, arg.tenant_id_);
        EXPECT_EQ(OB_SUCCESS, executor_.ctx_.set_abs_timeout(expired_deadline));
        THIS_WORKER.set_timeout_ts(expired_deadline);
        return OB_TIMEOUT;
      }));
  expect_drop(true);
  EXPECT_EQ(OB_TIMEOUT, executor_.finish_create_tenant_(OB_SUCCESS));
  EXPECT_EQ(expired_deadline, executor_.ctx_.get_abs_timeout());
  EXPECT_EQ(expired_deadline, THIS_WORKER.get_timeout_ts());
  EXPECT_EQ(&executor_.ctx_, &ObTimeoutCtx::get_ctx());
}

TEST_F(TestTenantParallelCreateExecutor, primary_expired_deadline_gets_fresh_cleanup_budget)
{
  executor_.create_tenant_arg_.is_restore_ = false;
  const int64_t expired_deadline = ObTimeUtility::current_time() - 1;
  ASSERT_EQ(OB_SUCCESS, executor_.ctx_.set_abs_timeout(expired_deadline));
  lib::WorkerTimeoutGuard worker_guard(expired_deadline);
  expect_drop(false);
  EXPECT_EQ(OB_TIMEOUT, executor_.finish_create_tenant_(OB_TIMEOUT));
  EXPECT_EQ(expired_deadline, executor_.ctx_.get_abs_timeout());
  EXPECT_EQ(expired_deadline, THIS_WORKER.get_timeout_ts());
  EXPECT_EQ(&executor_.ctx_, &ObTimeoutCtx::get_ctx());
}

TEST_F(TestTenantParallelCreateExecutor, cleanup_timeout_restores_expired_contexts_and_original_error)
{
  const int64_t expired_deadline = ObTimeUtility::current_time() - 1;
  ASSERT_EQ(OB_SUCCESS, executor_.ctx_.set_abs_timeout(expired_deadline));
  lib::WorkerTimeoutGuard worker_guard(expired_deadline);
  expect_drop(true, OB_TIMEOUT);
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
  EXPECT_EQ(expired_deadline, executor_.ctx_.get_abs_timeout());
  EXPECT_EQ(expired_deadline, THIS_WORKER.get_timeout_ts());
  EXPECT_EQ(&executor_.ctx_, &ObTimeoutCtx::get_ctx());
}

TEST_F(TestTenantParallelCreateExecutor, restore_success_only_finishes_meta_tenant)
{
  expect_create_end(META_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, executor_.finish_create_tenant_(OB_SUCCESS));
}

TEST_F(TestTenantParallelCreateExecutor, primary_failure_keeps_existing_cleanup_behavior)
{
  executor_.create_tenant_arg_.is_restore_ = false;
  expect_drop(false);
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, primary_success_finishes_both_tenants)
{
  executor_.create_tenant_arg_.is_restore_ = false;
  InSequence sequence;
  expect_create_end(META_TENANT_ID);
  expect_create_end(USER_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, executor_.finish_create_tenant_(OB_SUCCESS));
}

TEST_F(TestTenantParallelCreateExecutor, standby_failure_does_not_drop_tenant)
{
  executor_.create_tenant_arg_.is_restore_ = false;
  executor_.create_tenant_arg_.is_creating_standby_ = true;
  executor_.create_tenant_arg_.log_restore_source_ = ObString::make_string("test_source");
  ASSERT_TRUE(executor_.create_tenant_arg_.get_tenant_role().is_standby());
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, clone_failure_leaves_cleanup_to_clone_job)
{
  executor_.create_tenant_arg_.source_tenant_id_ = 1002;
  ASSERT_TRUE(executor_.create_tenant_arg_.get_tenant_role().is_clone());
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, existing_tenant_is_not_dropped)
{
  executor_.create_tenant_arg_.if_not_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, executor_.create_tenant_schema_result_.init_with_tenant_exist());
  EXPECT_EQ(OB_SUCCESS, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, invalid_create_result_does_not_drop_tenant)
{
  executor_.create_tenant_schema_result_.user_tenant_id_ = OB_INVALID_TENANT_ID;
  EXPECT_EQ(OB_SUCCESS, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

TEST_F(TestTenantParallelCreateExecutor, cleanup_failure_preserves_original_create_error)
{
  expect_drop(true, OB_TIMEOUT);
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, executor_.finish_create_tenant_(OB_TENANT_NOT_IN_SERVER));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include <gtest/gtest.h>
#include "test_direct_load_stream_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_interface_pragma.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{
namespace
{
static const uint64_t TEST_TENANT_ID = 1002;
static const uint64_t TEST_USER_ID = 2001;
static const uint64_t TEST_OTHER_USER_ID = 2002;

class MockDirectLoadStreamDbmsMtlEnv
{
public:
  MockDirectLoadStreamDbmsMtlEnv()
    : tenant_base_(TEST_TENANT_ID),
      table_load_service_(TEST_TENANT_ID),
      mock_meta_store_(TEST_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamDbmsMtlEnv()
  {
    destroy();
  }
  int init()
  {
    int ret = common::OB_SUCCESS;
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    bool allocator_existed = false;
    int alloc_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else if (OB_ISNULL(allocator)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      // DBMS bridge 最终通过 MTL 拿到当前租户的 stream service。
      // recycle 可能因 PL/session 内存残留把 allocator 放入 unrecycled 列表，
      // 这里以 active ctx allocator 为准，必要时由 create 接口取回。
      allocator_existed = OB_NOT_NULL(allocator->get_tenant_ctx_allocator(
          TEST_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID));
      if (!allocator_existed) {
        alloc_ret = allocator->create_and_add_tenant_allocator(TEST_TENANT_ID);
        if (common::OB_SUCCESS == alloc_ret) {
          created_allocator_ = true;
        }
      }
      if (OB_UNLIKELY(common::OB_SUCCESS != alloc_ret
          && common::OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(allocator->set_tenant_limit(TEST_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_TENANT_ID));
      } else if (OB_ISNULL(allocator->get_tenant_ctx_allocator(
                           TEST_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID))) {
        ret = common::OB_ERR_UNEXPECTED;
      } else if (!self_addr_.set_ip_addr("127.0.0.1", 2882)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        old_self_addr_ = GCTX.self_addr();
        GCTX.self_addr_seq_.set_addr(self_addr_);
        old_tenant_ = share::ObTenantEnv::get_tenant();
        tenant_base_.set<observer::ObTableLoadService *>(&table_load_service_);
        share::ObTenantEnv::set_tenant(&tenant_base_);
        if (OB_FAIL(mock_meta_store_.init())) {
          LOG_WARN("fail to init DBMS test stream metadata store", KR(ret));
        } else if (OB_FAIL(table_load_service_.get_direct_load_stream_service()
                               .set_meta_store_for_test(mock_meta_store_))) {
          LOG_WARN("fail to inject DBMS test stream metadata store", KR(ret));
        } else if (OB_FAIL(table_load_service_.get_direct_load_stream_service()
                               .enable_local_stream_id_generator_for_test())) {
          LOG_WARN("fail to enable DBMS test stream id generator", KR(ret));
        } else if (OB_FAIL(table_load_service_.get_direct_load_stream_service()
                               .enable_injected_queue_topology_for_test())) {
          LOG_WARN("fail to enable DBMS test stream topology injection", KR(ret));
        } else if (OB_FAIL(table_load_service_.get_direct_load_stream_service()
                               .get_buffer_queue_manager()
                               .set_mock_queue_store_factory_for_test(
                                   observer::get_direct_load_stream_mock_store_factory()))) {
          LOG_WARN("fail to inject DBMS test stream queue store factory", KR(ret));
        } else if (OB_FAIL(table_load_service_.init())) {
          LOG_WARN("fail to init DBMS test table load service", KR(ret));
        }
        if (OB_FAIL(ret)) {
          table_load_service_.destroy();
          mock_meta_store_.reset();
          share::ObTenantEnv::set_tenant(old_tenant_);
          GCTX.self_addr_seq_.set_addr(old_self_addr_);
          old_tenant_ = nullptr;
        } else {
          is_inited_ = true;
        }
      }
      if (OB_FAIL(ret) && created_allocator_) {
        lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_TENANT_ID);
        created_allocator_ = false;
      }
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      table_load_service_.destroy();
      mock_meta_store_.reset();
      share::ObTenantEnv::set_tenant(old_tenant_);
      GCTX.self_addr_seq_.set_addr(old_self_addr_);
      old_tenant_ = nullptr;
      is_inited_ = false;
    }
    if (created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_TENANT_ID);
      created_allocator_ = false;
    }
  }

private:
  share::ObTenantBase tenant_base_;
  observer::ObTableLoadService table_load_service_;
  observer::ObMockDirectLoadStreamMetaStore mock_meta_store_;
  share::ObTenantBase *old_tenant_;
  common::ObAddr old_self_addr_;
  common::ObAddr self_addr_;
  bool created_allocator_;
  bool is_inited_;
};

int build_create_param(observer::ObDirectLoadStreamCreateParam &param)
{
  int ret = common::OB_SUCCESS;
  table::ObDirectLoadStreamQueueEntry entry;
  param = observer::ObDirectLoadStreamCreateParam();
  param.tenant_id_ = TEST_TENANT_ID;
  param.user_id_ = TEST_USER_ID;
  param.table_id_ = 4001;
  param.target_schema_version_ = 5001;
  param.expire_ts_ = 6001;
  if (OB_UNLIKELY(!GCTX.self_addr().is_valid())) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    entry.server_addr_ = GCTX.self_addr();
  }
  if (OB_SUCC(ret)) {
    entry.queue_count_ = 1;
    ret = param.queue_entries_.push_back(entry);
  }
  return ret;
}

int init_session(common::ObIAllocator &allocator,
                 const uint64_t tenant_id,
                 const uint64_t user_id,
                 sql::ObSQLSessionInfo &session)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(session.test_init(1, tenant_id, user_id, &allocator))) {
    LOG_WARN("fail to init test session", KR(ret));
  } else if (OB_FAIL(session.init_tenant(common::ObString::make_string("test_tenant"), tenant_id))) {
    LOG_WARN("fail to init test tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(session.set_user(common::ObString::make_string("test_user"),
                                      common::ObString::make_string("%"),
                                      user_id))) {
    LOG_WARN("fail to set test user", KR(ret), K(user_id));
  } else {
    session.set_priv_user_id(user_id);
  }
  return ret;
}

int create_open_stream(int64_t &stream_id)
{
  int ret = common::OB_SUCCESS;
  observer::ObDirectLoadStreamCreateParam create_param;
  stream_id = common::OB_INVALID_ID;
  if (OB_FAIL(build_create_param(create_param))) {
    LOG_WARN("fail to build create param", KR(ret));
  } else if (OB_FAIL(observer::ObDirectLoadStreamService::create_direct_load_stream(
                 create_param, stream_id))) {
    LOG_WARN("fail to create direct load stream", KR(ret), K(create_param));
  }
  return ret;
}

int pl_interface_close_stream(common::ObIAllocator &allocator,
                              sql::ObSQLSessionInfo &session,
                              const int64_t stream_id)
{
  int ret = common::OB_SUCCESS;
  sql::ObExecContext exec_ctx(allocator);
  sql::ParamStore params((common::ObWrapperAllocator(&allocator)));
  common::ObObjParam stream_id_param;
  common::ObObj result;
  int status = common::OB_SUCCESS;
  ObPLInterfaceService interface_service;
  common::ObString interface_name = common::ObString::make_string("DBMS_DIRECT_LOAD_STREAM_CLOSE");
  PL_C_INTERFACE_t entry = nullptr;
  exec_ctx.set_my_session(&session);
  stream_id_param.set_int(stream_id);
  if (OB_FAIL(params.push_back(stream_id_param))) {
    LOG_WARN("fail to push stream id param", KR(ret));
  } else if (OB_FAIL(interface_service.init())) {
    LOG_WARN("fail to init pl interface service", KR(ret));
  } else if (OB_ISNULL(entry = interface_service.get_entry(interface_name))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get direct load stream close interface", KR(ret), K(interface_name));
  } else {
    ObPLExecCtx pl_exec_ctx(&allocator, &allocator, &exec_ctx, &params, &result, &status, nullptr);
    if (OB_FAIL(entry(pl_exec_ctx, params, result))) {
      LOG_WARN("fail to close direct load stream by pl interface", KR(ret), K(stream_id));
    }
  }
  return ret;
}

int get_local_queue_state(const int64_t stream_id,
                          observer::ObDirectLoadStreamBufferQueueState &state)
{
  int ret = common::OB_SUCCESS;
  observer::ObTableLoadService *table_load_service = nullptr;
  observer::ObDirectLoadStreamBufferQueueHandle handle;
  state = observer::ObDirectLoadStreamBufferQueueState::INVALID;
  if (OB_ISNULL(table_load_service = MTL(observer::ObTableLoadService *))) {
    ret = common::OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .get_queue(observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0),
                                   handle))) {
    LOG_WARN("fail to get direct load stream local queue", KR(ret), K(stream_id));
  } else if (OB_ISNULL(handle.get_queue())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream local queue is null", KR(ret), K(stream_id));
  } else {
    state = handle.get_queue()->get_state();
  }
  return ret;
}

int cleanup_local_queue(const int64_t stream_id)
{
  int ret = common::OB_SUCCESS;
  observer::ObTableLoadService *table_load_service = nullptr;
  if (OB_ISNULL(table_load_service = MTL(observer::ObTableLoadService *))) {
    ret = common::OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .cleanup_queue(observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0)))) {
    LOG_WARN("fail to cleanup direct load stream local queue", KR(ret), K(stream_id));
  }
  return ret;
}
} // namespace

class TestDirectLoadStreamDbms : public ::testing::Test
{
public:
  TestDirectLoadStreamDbms() {}
  virtual ~TestDirectLoadStreamDbms() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// 验证 DBMS CLOSE 通过 PL interface seal 本租户本用户的 stream。
TEST_F(TestDirectLoadStreamDbms, close_seals_mock_stream)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  session.destroy(true);
}

// 验证 owner mismatch 时 DBMS CLOSE 返回权限错误，metadata 保持 OPEN。
TEST_F(TestDirectLoadStreamDbms, close_rejects_owner_mismatch)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_OTHER_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  EXPECT_EQ(common::OB_ERR_NO_PRIVILEGE, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  session.destroy(true);
}

// 完整 PL CREATE_TABLE 需要 schema guard 和真实目标表环境，归 Step 9/集成门禁补齐。
TEST_F(TestDirectLoadStreamDbms, DISABLED_create_table_returns_stream_id_with_local_queue)
{
}

// 验证 DBMS CLOSE 在 owner 校验通过后会 seal 本地 queue。
TEST_F(TestDirectLoadStreamDbms, close_seals_queue_after_owner_check)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamBufferQueueState queue_state =
    observer::ObDirectLoadStreamBufferQueueState::INVALID;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, get_local_queue_state(stream_id, queue_state));
  EXPECT_EQ(observer::ObDirectLoadStreamBufferQueueState::SEALED, queue_state);
  session.destroy(true);
}

// 验证 owner mismatch 不会触碰本地 queue。
TEST_F(TestDirectLoadStreamDbms, close_owner_mismatch_does_not_seal_queue)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamBufferQueueState queue_state =
    observer::ObDirectLoadStreamBufferQueueState::INVALID;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_OTHER_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  EXPECT_EQ(common::OB_ERR_NO_PRIVILEGE, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, get_local_queue_state(stream_id, queue_state));
  EXPECT_EQ(observer::ObDirectLoadStreamBufferQueueState::OPEN, queue_state);
  session.destroy(true);
}

// 验证重复 DBMS CLOSE 对 SEALED stream 幂等成功。
TEST_F(TestDirectLoadStreamDbms, close_is_idempotent)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamMeta meta;
  observer::ObDirectLoadStreamBufferQueueState queue_state =
    observer::ObDirectLoadStreamBufferQueueState::INVALID;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  ASSERT_EQ(common::OB_SUCCESS, get_local_queue_state(stream_id, queue_state));
  EXPECT_EQ(observer::ObDirectLoadStreamBufferQueueState::SEALED, queue_state);
  session.destroy(true);
}

// 验证 SEALED 后本地 queue missing，重复 DBMS CLOSE 仍成功且 metadata 不被改失败。
TEST_F(TestDirectLoadStreamDbms, close_ignores_missing_queue_after_sealed)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  observer::ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_USER_ID, session));
  ASSERT_EQ(common::OB_SUCCESS, create_open_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, cleanup_local_queue(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, pl_interface_close_stream(allocator, session, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
  session.destroy(true);
}

// 验证 DBMS CLOSE 缺失 stream 时返回 not found。
TEST_F(TestDirectLoadStreamDbms, close_rejects_missing_stream)
{
  MockDirectLoadStreamDbmsMtlEnv mtl_env;
  common::ObArenaAllocator allocator("DLSDbmsTest");
  sql::ObSQLSessionInfo session(TEST_TENANT_ID);
  const int64_t missing_stream_id = 12345;

  ASSERT_EQ(common::OB_SUCCESS, mtl_env.init());
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(common::OB_SUCCESS, init_session(allocator, TEST_TENANT_ID, TEST_USER_ID, session));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            pl_interface_close_stream(allocator, session, missing_stream_id));
  session.destroy(true);
}

} // namespace pl
} // namespace oceanbase

int main(int argc, char **argv)
{
  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

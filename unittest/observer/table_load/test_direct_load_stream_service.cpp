/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include "test_direct_load_stream_utils.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_executor.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_struct.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_executor.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_struct.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{

namespace
{
static const uint64_t TEST_TENANT_ID = TEST_DIRECT_LOAD_STREAM_TENANT_ID;
static const std::chrono::milliseconds SHORT_WAIT(100);
static const std::chrono::seconds LONG_WAIT(5);
int append_forward_chunk_for_test(ObDirectLoadStreamAppendSessionManager *manager,
                                  const ObDirectLoadStreamForwardChunkArg arg)
{
  return OB_ISNULL(manager) ? common::OB_INVALID_ARGUMENT
                            : manager->append_chunk(arg);
}

class MockDirectLoadStreamServiceTenantEnv
{
public:
  MockDirectLoadStreamServiceTenantEnv()
    : tenant_base_(TEST_TENANT_ID),
      table_load_service_(nullptr),
      mock_meta_store_(TEST_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      table_load_service_inited_(false),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamServiceTenantEnv()
  {
    destroy();
  }
  int init()
  {
    int ret = common::OB_SUCCESS;
    bool allocator_existed = false;
    int alloc_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else {
      // 测试 service 聚合层时准备租户 allocator，并显式设置本机地址。
      allocator_existed = OB_NOT_NULL(
          lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
              TEST_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID));
      if (!allocator_existed) {
        alloc_ret = lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
            TEST_TENANT_ID);
        if (common::OB_SUCCESS == alloc_ret) {
          created_allocator_ = true;
        }
      }
      if (OB_UNLIKELY(common::OB_SUCCESS != alloc_ret
          && common::OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(lib::ObMallocAllocator::get_instance()->set_tenant_limit(
                           TEST_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_TENANT_ID));
      } else if (OB_ISNULL(lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
                           TEST_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID))) {
        ret = common::OB_ERR_UNEXPECTED;
      } else if (!self_addr_.set_ip_addr("127.0.0.1", 2882)) {
        ret = common::OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(table_load_service_ = OB_NEW(
                               ObTableLoadService,
                               common::ObMemAttr(common::OB_SERVER_TENANT_ID, "DLSvcTest"),
                               TEST_TENANT_ID))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        old_self_addr_ = GCTX.self_addr();
        GCTX.self_addr_seq_.set_addr(self_addr_);
        old_tenant_ = share::ObTenantEnv::get_tenant();
        tenant_base_.set<ObTableLoadService *>(table_load_service_);
        share::ObTenantEnv::set_tenant(&tenant_base_);
        is_inited_ = true;
      }
      if (OB_FAIL(ret) && created_allocator_) {
        lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_TENANT_ID);
        created_allocator_ = false;
      }
    }
    return ret;
  }
  int init_table_load_service()
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_UNLIKELY(table_load_service_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else if (OB_ISNULL(table_load_service_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(mock_meta_store_.init())) {
      LOG_WARN("fail to init mock stream metadata store", KR(ret));
    } else if (OB_FAIL(table_load_service_->get_direct_load_stream_service()
                           .set_meta_store_for_test(mock_meta_store_))) {
      LOG_WARN("fail to inject mock stream metadata store", KR(ret));
    } else if (OB_FAIL(table_load_service_->get_direct_load_stream_service()
                           .enable_local_stream_id_generator_for_test())) {
      LOG_WARN("fail to enable test stream id generator", KR(ret));
    } else if (OB_FAIL(table_load_service_->get_direct_load_stream_service()
                           .enable_injected_queue_topology_for_test())) {
      LOG_WARN("fail to enable test stream topology injection", KR(ret));
    } else if (OB_FAIL(table_load_service_->get_direct_load_stream_service()
                           .get_buffer_queue_manager()
                           .set_mock_queue_store_factory_for_test(
                               get_direct_load_stream_mock_store_factory()))) {
      LOG_WARN("fail to set mock stream queue store factory", KR(ret));
    } else if (OB_FAIL(table_load_service_->init())) {
      LOG_WARN("fail to init test table load service", KR(ret));
    } else {
      table_load_service_inited_ = true;
    }
    return ret;
  }
  int stop_table_load_service()
  {
    return table_load_service_inited_ && OB_NOT_NULL(table_load_service_)
        ? table_load_service_->stop() : common::OB_NOT_INIT;
  }
  ObDirectLoadStreamService &get_mtl_stream_service()
  {
    return table_load_service_->get_direct_load_stream_service();
  }
  share::ObTenantBase *get_tenant_base()
  {
    return &tenant_base_;
  }
  void destroy()
  {
    if (table_load_service_inited_ && OB_NOT_NULL(table_load_service_)) {
      table_load_service_->destroy();
      table_load_service_inited_ = false;
    }
    if (OB_NOT_NULL(table_load_service_)) {
      OB_DELETE(ObTableLoadService,
                common::ObMemAttr(common::OB_SERVER_TENANT_ID, "DLSvcTest"),
                table_load_service_);
      table_load_service_ = nullptr;
    }
    mock_meta_store_.reset();
    if (is_inited_) {
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
  ObTableLoadService *table_load_service_;
  ObMockDirectLoadStreamMetaStore mock_meta_store_;
  share::ObTenantBase *old_tenant_;
  common::ObAddr old_self_addr_;
  common::ObAddr self_addr_;
  bool created_allocator_;
  bool table_load_service_inited_;
  bool is_inited_;
};

class MockTableLoadServiceTenantEnv
{
public:
  explicit MockTableLoadServiceTenantEnv(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      tenant_base_(tenant_id),
      old_tenant_(nullptr),
      created_allocator_(false),
      is_inited_(false)
  {
  }
  ~MockTableLoadServiceTenantEnv()
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
      // 验证 TableLoad MTL 初始化时，按目标租户显式准备 tenant allocator。
      allocator_existed = OB_NOT_NULL(allocator->get_tenant_ctx_allocator(
          tenant_id_, common::ObCtxIds::DEFAULT_CTX_ID));
      if (!allocator_existed) {
        alloc_ret = allocator->create_and_add_tenant_allocator(tenant_id_);
        if (common::OB_SUCCESS == alloc_ret) {
          created_allocator_ = true;
        }
      }
      if (OB_UNLIKELY(common::OB_SUCCESS != alloc_ret
          && common::OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(allocator->set_tenant_limit(tenant_id_, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K_(tenant_id));
      } else if (OB_ISNULL(allocator->get_tenant_ctx_allocator(
                           tenant_id_, common::ObCtxIds::DEFAULT_CTX_ID))) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        old_tenant_ = share::ObTenantEnv::get_tenant();
        share::ObTenantEnv::set_tenant(&tenant_base_);
        is_inited_ = true;
      }
      if (OB_FAIL(ret) && created_allocator_) {
        allocator->recycle_tenant_allocator(tenant_id_);
        created_allocator_ = false;
      }
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      share::ObTenantEnv::set_tenant(old_tenant_);
      old_tenant_ = nullptr;
      is_inited_ = false;
    }
    if (created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id_);
      created_allocator_ = false;
    }
  }

private:
  const uint64_t tenant_id_;
  share::ObTenantBase tenant_base_;
  share::ObTenantBase *old_tenant_;
  bool created_allocator_;
  bool is_inited_;
};

void expect_non_user_table_load_service_skips_stream(const uint64_t tenant_id)
{
  MockTableLoadServiceTenantEnv tenant_env(tenant_id);
  ObTableLoadService table_load_service(tenant_id);
  ObDirectLoadStreamMeta meta;

  ASSERT_FALSE(common::is_user_tenant(tenant_id));
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, table_load_service.init());
  EXPECT_EQ(common::OB_NOT_INIT,
            table_load_service.get_direct_load_stream_service().get_stream(1, meta));
  table_load_service.destroy();
}

int build_create_param(ObDirectLoadStreamCreateParam &param)
{
  int ret = common::OB_SUCCESS;
  table::ObDirectLoadStreamQueueEntry entry;
  param = ObDirectLoadStreamCreateParam();
  param.tenant_id_ = TEST_TENANT_ID;
  param.user_id_ = 2001;
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

ObMockDirectLoadStreamMetaStore &get_direct_load_stream_mock_meta_store()
{
  static ObMockDirectLoadStreamMetaStore meta_store(TEST_TENANT_ID);
  return meta_store;
}

int init_stream_service_with_mock_store(ObDirectLoadStreamService &stream_service)
{
  int ret = common::OB_SUCCESS;
  ObMockDirectLoadStreamMetaStore &meta_store = get_direct_load_stream_mock_meta_store();
  meta_store.reset();
  if (OB_FAIL(meta_store.init())) {
  } else if (OB_FAIL(stream_service.set_meta_store_for_test(meta_store))) {
  } else if (OB_FAIL(stream_service.enable_local_stream_id_generator_for_test())) {
  } else if (OB_FAIL(stream_service.enable_injected_queue_topology_for_test())) {
  } else if (OB_FAIL(stream_service.get_buffer_queue_manager()
                         .set_mock_queue_store_factory_for_test(
                             get_direct_load_stream_mock_store_factory()))) {
  } else {
    ret = stream_service.init();
  }
  return ret;
}

int seal_stream_for_test(ObDirectLoadStreamService *stream_service, const int64_t stream_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stream_service)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = stream_service->seal_stream(stream_id);
  }
  return ret;
}

int fail_stream_for_test(ObDirectLoadStreamService *stream_service,
                         const int64_t stream_id,
                         const int result_code)
{
  return OB_ISNULL(stream_service)
      ? common::OB_INVALID_ARGUMENT : stream_service->fail_stream(stream_id, result_code);
}

int fail_stream_if_open_for_test(ObDirectLoadStreamService *stream_service,
                                 const int64_t stream_id,
                                 const int result_code)
{
  return OB_ISNULL(stream_service)
      ? common::OB_INVALID_ARGUMENT
      : stream_service->fail_stream_if_open(stream_id, result_code);
}

int finish_direct_load_stream_for_test(share::ObTenantBase *tenant_base,
                                       const int64_t stream_id,
                                       const int result_code)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(tenant_base)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    share::ObTenantSwitchGuard tenant_guard(tenant_base);
    ret = ObDirectLoadStreamService::finish_direct_load_stream(stream_id, result_code);
  }
  return ret;
}

int fail_direct_load_stream_for_test(share::ObTenantBase *tenant_base,
                                     const int64_t stream_id,
                                     const int result_code)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(tenant_base)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    share::ObTenantSwitchGuard tenant_guard(tenant_base);
    ret = ObDirectLoadStreamService::fail_direct_load_stream(stream_id, result_code);
  }
  return ret;
}

bool wait_until_local_queue_state(ObDirectLoadStreamService &stream_service,
                                  const int64_t stream_id,
                                  const ObDirectLoadStreamBufferQueueState expected,
                                  const std::chrono::milliseconds timeout)
{
  bool matched = false;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + timeout;
  while (!matched && std::chrono::steady_clock::now() < deadline) {
    ObDirectLoadStreamBufferQueueHandle handle;
    if (common::OB_SUCCESS == stream_service.get_buffer_queue_manager().get_queue(
            ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle)
        && nullptr != handle.get_queue()) {
      matched = expected == handle.get_queue()->get_state();
    }
    if (!matched) {
      std::this_thread::yield();
    }
  }
  return matched;
}

bool wait_until_queue_state(ObDirectLoadStreamBufferQueue &queue,
                            const ObDirectLoadStreamBufferQueueState expected,
                            const std::chrono::milliseconds timeout)
{
  bool matched = false;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + timeout;
  while (!matched && std::chrono::steady_clock::now() < deadline) {
    matched = expected == queue.get_state();
    if (!matched) {
      std::this_thread::yield();
    }
  }
  return matched;
}

} // namespace

class TestDirectLoadStreamService : public ::testing::Test
{
public:
  TestDirectLoadStreamService() {}
  virtual ~TestDirectLoadStreamService() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST(TestDirectLoadStreamServiceBasic, direct_load_stream_enablement_requires_both_switches)
{
  GCONF._ob_enable_direct_load.set_value("False");
  GCONF._ob_enable_direct_load_stream.set_value("False");
  EXPECT_FALSE(ObTableLoadService::is_direct_load_stream_enabled());

  GCONF._ob_enable_direct_load.set_value("False");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  EXPECT_FALSE(ObTableLoadService::is_direct_load_stream_enabled());

  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("False");
  EXPECT_FALSE(ObTableLoadService::is_direct_load_stream_enabled());

  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  EXPECT_TRUE(ObTableLoadService::is_direct_load_stream_enabled());
}

// 验证 service 初始化 tenant store，并转发不存在 stream 的查询。
TEST(TestDirectLoadStreamServiceBasic, init_and_get_missing_stream)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamMeta meta;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  EXPECT_EQ(common::OB_INIT_TWICE, stream_service.init());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, stream_service.get_stream(10001, meta));
}

// 验证非用户租户初始化 TableLoadService 时跳过 Stream service，不阻断 MTL。
TEST(TestDirectLoadStreamServiceBasic, table_load_service_skips_stream_for_non_user_tenants)
{
  expect_non_user_table_load_service_skips_stream(common::OB_SYS_TENANT_ID);
  expect_non_user_table_load_service_skips_stream(
      common::gen_meta_tenant_id(TEST_TENANT_ID));
}

// 验证 service 的 metadata 操作入口统一拒绝非法 stream id。
TEST(TestDirectLoadStreamServiceBasic, reject_invalid_stream_id)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim_param;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            stream_service.get_stream(common::OB_INVALID_ID, meta));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            stream_service.seal_stream(common::OB_INVALID_ID));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            stream_service.finish_stream(common::OB_INVALID_ID, common::OB_TIMEOUT));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, stream_service.claim_stream(claim_param));
}

// 验证 CREATE 会拒绝租户不一致，且不返回可用 stream_id。
TEST_F(TestDirectLoadStreamService, reject_tenant_mismatch)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  param.tenant_id_ = TEST_TENANT_ID + 1;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, stream_service.create_stream(param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
}

// 未显式注入测试 ID 生成器时，缺失全局 Unique ID 服务必须直接失败。
TEST_F(TestDirectLoadStreamService, missing_global_stream_id_service_fails_create)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObMockDirectLoadStreamMetaStore meta_store(TEST_TENANT_ID);
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, stream_service.set_meta_store_for_test(meta_store));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.enable_injected_queue_topology_for_test());
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().set_mock_queue_store_factory_for_test(
                get_direct_load_stream_mock_store_factory()));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  EXPECT_EQ(common::OB_NOT_INIT, stream_service.create_stream(param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
}

// 非空 queue_entries_ 只有显式测试 seam 才能绕过生产资源拓扑。
TEST_F(TestDirectLoadStreamService, injected_topology_requires_explicit_test_seam)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObMockDirectLoadStreamMetaStore meta_store(TEST_TENANT_ID);
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, stream_service.set_meta_store_for_test(meta_store));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.enable_local_stream_id_generator_for_test());
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().set_mock_queue_store_factory_for_test(
                get_direct_load_stream_mock_store_factory()));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, stream_service.create_stream(param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
}

// 验证 queue manager 跟随 stream service 生命周期初始化和销毁。
TEST_F(TestDirectLoadStreamService, queue_manager_lifecycle_smoke)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  EXPECT_TRUE(stream_service.get_buffer_queue_manager().is_inited());
  stream_service.destroy();
  EXPECT_FALSE(stream_service.get_buffer_queue_manager().is_inited());
}

// 验证本机地址无效时拒绝 CREATE，避免 service helper 把无效 self 当作通配。
TEST_F(TestDirectLoadStreamService, reject_invalid_self_addr)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  common::ObAddr invalid_addr;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  GCTX.self_addr_seq_.set_addr(invalid_addr);
  EXPECT_EQ(common::OB_ERR_UNEXPECTED, stream_service.create_stream(create_param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
}

// 验证远端 owner 必须通过正式 server RPC proxy 分发，测试环境未装配 proxy 时明确失败。
TEST_F(TestDirectLoadStreamService, remote_queue_owner_requires_rpc_proxy)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  table::ObDirectLoadStreamQueueEntry entry;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_TRUE(entry.server_addr_.set_ip_addr("127.0.0.2", 2882));
  entry.queue_count_ = 1;
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.queue_entries_.reset();
  ASSERT_EQ(common::OB_SUCCESS, create_param.queue_entries_.push_back(entry));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED, stream_service.create_stream(create_param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
}

// 验证 create/get/seal/finish 的最小生命周期状态流转。
TEST_F(TestDirectLoadStreamService, lifecycle_roundtrip)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.finish_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
}

// 事务提交后的单次收尾先写 FINISHED(0)，再删除固定本地 queue。
TEST_F(TestDirectLoadStreamService, finish_and_cleanup_success_finishes_and_cleans_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.finish_and_cleanup_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
              ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// 重复收尾沿用首个终态做 cleanup，接口返回 cleanup 操作结果而不是业务终态。
TEST_F(TestDirectLoadStreamService, finish_and_cleanup_reuses_terminal_result_for_cleanup)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.finish_stream(stream_id, common::OB_TIMEOUT));

  EXPECT_EQ(common::OB_SUCCESS,
            stream_service.finish_and_cleanup_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// QC 不在 owner 集合时无需本地 queue，但 OPEN 和已成功 FINISHED 仍不能通过提交门禁。
TEST_F(TestDirectLoadStreamService, remote_only_drain_check_keeps_metadata_fence)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObMockDirectLoadStreamMetaStore &meta_store = get_direct_load_stream_mock_meta_store();
  ObDirectLoadStreamCreateParam create_param;
  const int64_t stream_id = 91001;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_TRUE(create_param.queue_entries_.at(0).server_addr_.set_ip_addr("127.0.0.2", 2882));
  create_param.queue_entries_.at(0).queue_count_ = 2;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(stream_id, create_param));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.check_stream_reader_drain_completed(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.seal_stream(stream_id));
  EXPECT_EQ(common::OB_SUCCESS,
            stream_service.check_stream_reader_drain_completed(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_SUCCESS));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.check_stream_reader_drain_completed(stream_id));
}

// 即使 QC 不持有 queue，也必须传播 metadata 中已保存的失败终态。
TEST_F(TestDirectLoadStreamService, remote_only_drain_check_preserves_terminal_error)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObMockDirectLoadStreamMetaStore &meta_store = get_direct_load_stream_mock_meta_store();
  ObDirectLoadStreamCreateParam create_param;
  const int64_t stream_id = 91002;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_TRUE(create_param.queue_entries_.at(0).server_addr_.set_ip_addr("127.0.0.2", 2882));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(stream_id, create_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_TIMEOUT));
  EXPECT_EQ(common::OB_TIMEOUT,
            stream_service.check_stream_reader_drain_completed(stream_id));
}

// topology 声明了本地 queue 却找不到实体时，仍报错，不能混同于合法的远端 owner。
TEST_F(TestDirectLoadStreamService, drain_check_rejects_missing_declared_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObMockDirectLoadStreamMetaStore &meta_store = get_direct_load_stream_mock_meta_store();
  ObDirectLoadStreamCreateParam create_param;
  const int64_t stream_id = 91003;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(stream_id, create_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.seal_stream(stream_id));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.check_stream_reader_drain_completed(stream_id));
}

// 每个本地 queue 都必须收到 EOF 后 direct insert 才能提交；终态 cleanup 必须删除完整
// owner batch，不能假设只有 queue 0。
TEST_F(TestDirectLoadStreamService, multi_queue_drain_and_cleanup_cover_every_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handles[3];
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObString chunk;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.queue_entries_.at(0).queue_count_ = 3;
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  for (uint64_t queue_id = 0; queue_id < 3; ++queue_id) {
    ASSERT_EQ(common::OB_SUCCESS,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_id), handles[queue_id]));
  }
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  EXPECT_EQ(common::OB_ITER_END, handles[0].get_queue()->get_next_chunk(allocator, chunk));
  EXPECT_EQ(common::OB_ITER_END, handles[1].get_queue()->get_next_chunk(allocator, chunk));
  // queue 2 尚未收到 EOF；只检查 queue 0 会错误地判定成功。
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.check_stream_reader_drain_completed(stream_id));
  EXPECT_EQ(common::OB_ITER_END, handles[2].get_queue()->get_next_chunk(allocator, chunk));
  EXPECT_EQ(common::OB_SUCCESS,
            stream_service.check_stream_reader_drain_completed(stream_id));
  for (uint64_t queue_id = 0; queue_id < 3; ++queue_id) {
    handles[queue_id].reset();
  }
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.finish_and_cleanup_stream(stream_id, common::OB_SUCCESS));
  for (uint64_t queue_id = 0; queue_id < 3; ++queue_id) {
    ObDirectLoadStreamBufferQueueHandle handle;
    EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_id), handle));
  }
}

// Direct Load 回滚后 metadata 保留调用方真实错误，
// 并清理 topology 中的全部本地 queue。
TEST_F(TestDirectLoadStreamService, finish_and_cleanup_error_finishes_and_cleans_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.queue_entries_.at(0).queue_count_ = 2;
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->fail(common::OB_TIMEOUT));
  handle.reset();
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.finish_and_cleanup_stream(stream_id, common::OB_CANCELED));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_CANCELED, meta.result_code_);
  for (uint64_t queue_id = 0; queue_id < 2; ++queue_id) {
    EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_id), handle));
  }
}

// metadata 不允许成功终结 OPEN stream 时不得先删 queue，供后续诊断/恢复处理。
TEST_F(TestDirectLoadStreamService, finish_and_cleanup_metadata_failure_keeps_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.finish_and_cleanup_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
              ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  EXPECT_NE(nullptr, handle.get_queue());
}

// 验证 CREATE 成功后本机 queue 可见且保持 OPEN。
TEST_F(TestDirectLoadStreamService, create_stream_creates_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_TRUE(handle.is_valid());
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, handle.get_queue()->get_state());
}

// 一轮 GC 串行处理未 claim 的过期记录和终态记录，执行中的 claimed 记录保持不变。
TEST_F(TestDirectLoadStreamService, gc_processes_safe_candidates_and_keeps_claimed_stream)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam expired_param;
  ObDirectLoadStreamCreateParam active_param;
  ObDirectLoadStreamClaimParam claim_param;
  ObDirectLoadStreamMeta meta;
  const int64_t now = common::ObTimeUtility::current_time();
  int64_t expired_open_id = common::OB_INVALID_ID;
  int64_t expired_claimed_id = common::OB_INVALID_ID;
  int64_t finished_id = common::OB_INVALID_ID;
  int64_t active_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(expired_param));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(active_param));
  expired_param.expire_ts_ = now - 1;
  active_param.expire_ts_ = now + 60 * 1000 * 1000;
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.create_stream(expired_param, expired_open_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.create_stream(expired_param, expired_claimed_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.create_stream(expired_param, finished_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.create_stream(active_param, active_id));

  claim_param.stream_id_ = expired_claimed_id;
  claim_param.owner_server_ = GCTX.self_addr();
  claim_param.session_id_ = 1001;
  claim_param.session_create_ts_ = now - 1;
  ASSERT_EQ(common::OB_SUCCESS, stream_service.claim_stream(claim_param));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.finish_stream(finished_id, common::OB_CANCELED));

  ASSERT_EQ(common::OB_SUCCESS, stream_service.run_gc_for_test(now + 1));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_stream(expired_open_id, meta));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_stream(expired_claimed_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  EXPECT_EQ(claim_param.session_id_, meta.session_id_);
  EXPECT_EQ(claim_param.session_create_ts_, meta.session_create_ts_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_stream(finished_id, meta));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(active_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

// 验证终态 cleanup 直接移除 queue 并唤醒在途 APPEND，释放引用后完成删除。
TEST_F(TestDirectLoadStreamService, fail_stream_removes_queue_and_wakes_append)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  common::ObArenaAllocator allocator("DLSvcTest");
  common::ObString chunk;
  ObDirectLoadStreamMeta meta;
  std::future<int> fail_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));

  fail_future = std::async(std::launch::async, fail_stream_for_test,
                           &stream_service, stream_id, common::OB_ERR_UNEXPECTED);
  ASSERT_TRUE(wait_until_queue_state(
      *handle.get_queue(), ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, handle.get_queue()->get_state());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            handle.get_queue()->append_chunk(
                append_handle, common::ObString::make_string("chunk")));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            handle.get_queue()->get_next_chunk(allocator, chunk));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_ERR_UNEXPECTED, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, handle.get_queue()->end_append(append_handle));
  handle.reset();
  ASSERT_EQ(std::future_status::ready, fail_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, fail_future.get());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

TEST_F(TestDirectLoadStreamService, fail_stream_if_open_finishes_open_stream)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  handle.reset();

  ASSERT_EQ(common::OB_SUCCESS, stream_service.fail_stream_if_open(
                                    stream_id, common::OB_SCHEMA_ERROR));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SCHEMA_ERROR, meta.result_code_);
}

// 从非 queue-0 owner 触发时，先用全局 metadata CAS 结束 Stream，再处理本地 queue。
TEST_F(TestDirectLoadStreamService, fail_stream_if_open_from_non_queue_zero_owner)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObMockDirectLoadStreamMetaStore &meta_store = get_direct_load_stream_mock_meta_store();
  ObDirectLoadStreamCreateParam create_param;
  table::ObDirectLoadStreamQueueEntry remote_entry;
  table::ObDirectLoadStreamQueueEntry local_entry;
  common::ObSEArray<uint64_t, 1> local_queue_ids;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  const int64_t stream_id = 90001;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_TRUE(remote_entry.server_addr_.set_ip_addr("127.0.0.2", 2882));
  remote_entry.queue_count_ = 1;
  local_entry.server_addr_ = GCTX.self_addr();
  local_entry.queue_count_ = 1;
  create_param.queue_entries_.reset();
  ASSERT_EQ(common::OB_SUCCESS, create_param.queue_entries_.push_back(remote_entry));
  ASSERT_EQ(common::OB_SUCCESS, create_param.queue_entries_.push_back(local_entry));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(stream_id, create_param));
  ASSERT_EQ(common::OB_SUCCESS, local_queue_ids.push_back(1));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_local_queue_batch(
                                    stream_id, create_param.target_schema_version_,
                                    GCTX.self_addr(), local_queue_ids));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 1), handle));
  handle.reset();

  const int fail_ret = stream_service.fail_stream_if_open(
      stream_id, common::OB_SCHEMA_ERROR);
  EXPECT_NE(common::OB_STATE_NOT_MATCH, fail_ret);
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SCHEMA_ERROR, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 1), handle));
}

TEST_F(TestDirectLoadStreamService, fail_stream_if_open_does_not_override_close)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));

  EXPECT_EQ(common::OB_STATE_NOT_MATCH, stream_service.fail_stream_if_open(
                                            stream_id, common::OB_SCHEMA_ERROR));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, handle.get_queue()->get_state());
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
}

// CLOSE_REQUESTED 仍属于 OPEN metadata；全局失败 CAS 获胜后必须唤醒在途 APPEND。
TEST_F(TestDirectLoadStreamService, fail_stream_if_open_wins_global_metadata_cas)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  ObDirectLoadStreamMeta meta;
  std::future<int> close_future;
  std::future<int> fail_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));

  close_future = std::async(std::launch::async, seal_stream_for_test,
                            &stream_service, stream_id);
  ASSERT_TRUE(wait_until_local_queue_state(
      stream_service, stream_id, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  fail_future = std::async(std::launch::async, fail_stream_if_open_for_test,
                           &stream_service, stream_id, common::OB_SCHEMA_ERROR);
  ASSERT_TRUE(wait_until_queue_state(
      *handle.get_queue(), ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED,
            handle.get_queue()->get_state());
  ASSERT_EQ(common::OB_ENTRY_NOT_EXIST, handle.get_queue()->end_append(append_handle));
  handle.reset();
  ASSERT_EQ(std::future_status::ready, close_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SCHEMA_ERROR, close_future.get());
  ASSERT_EQ(std::future_status::ready, fail_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, fail_future.get());

  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SCHEMA_ERROR, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

TEST_F(TestDirectLoadStreamService, registered_failure_wins_over_close_requested)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  ObDirectLoadStreamMeta meta;
  std::future<int> close_future;
  std::future<int> fail_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));

  close_future = std::async(std::launch::async, seal_stream_for_test,
                            &stream_service, stream_id);
  ASSERT_TRUE(wait_until_local_queue_state(
      stream_service, stream_id, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  fail_future = std::async(std::launch::async, fail_stream_for_test,
                           &stream_service, stream_id, common::OB_TIMEOUT);
  ASSERT_TRUE(wait_until_queue_state(
      *handle.get_queue(), ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, handle.get_queue()->get_state());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, handle.get_queue()->end_append(append_handle));
  handle.reset();
  ASSERT_EQ(std::future_status::ready, close_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_TIMEOUT, close_future.get());
  ASSERT_EQ(std::future_status::ready, fail_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, fail_future.get());

  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// metadata 使用触发 CAS 的错误；queue 历史首错不改变全局终态。
TEST_F(TestDirectLoadStreamService, fail_stream_if_open_uses_canonical_error)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->fail(common::OB_TIMEOUT));
  handle.reset();

  EXPECT_EQ(common::OB_SUCCESS, stream_service.fail_stream_if_open(
                                    stream_id, common::OB_SCHEMA_ERROR));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SCHEMA_ERROR, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// 验证公共错误 FINISH 写入终态并直接 cleanup queue。
TEST_F(TestDirectLoadStreamService, error_finish_cleans_queue_after_terminal_metadata)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  ObDirectLoadStreamMeta meta;
  std::future<int> finish_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            tenant_env.get_mtl_stream_service().get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));

  finish_future = std::async(
      std::launch::async, finish_direct_load_stream_for_test,
      tenant_env.get_tenant_base(), stream_id, common::OB_TIMEOUT);
  ASSERT_TRUE(wait_until_queue_state(
      *handle.get_queue(), ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, handle.get_queue()->get_state());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            handle.get_queue()->append_chunk(
                append_handle, common::ObString::make_string("chunk")));
  ASSERT_EQ(common::OB_SUCCESS,
            tenant_env.get_mtl_stream_service().get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, handle.get_queue()->end_append(append_handle));
  handle.reset();
  ASSERT_EQ(std::future_status::ready, finish_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, finish_future.get());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            tenant_env.get_mtl_stream_service().get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// 验证 stop 仅拒绝新请求，不阻断已有副作用的 fatal error handling。
TEST_F(TestDirectLoadStreamService, fatal_error_handling_is_allowed_after_service_stop)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  ObDirectLoadStreamMeta meta;
  std::future<int> fail_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            tenant_env.get_mtl_stream_service().get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.stop_table_load_service());
  EXPECT_EQ(common::OB_IN_STOP_STATE,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));

  fail_future = std::async(
      std::launch::async, fail_direct_load_stream_for_test,
      tenant_env.get_tenant_base(), stream_id, common::OB_CANCELED);
  ASSERT_TRUE(wait_until_queue_state(
      *handle.get_queue(), ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, handle.get_queue()->get_state());
  ASSERT_EQ(common::OB_SUCCESS,
            tenant_env.get_mtl_stream_service().get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_CANCELED, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, handle.get_queue()->end_append(append_handle));
  handle.reset();
  ASSERT_EQ(std::future_status::ready, fail_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, fail_future.get());
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            tenant_env.get_mtl_stream_service().get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// 全局 metadata 使用调用方真实错误，既有 queue 首错不覆盖事务结果。
TEST_F(TestDirectLoadStreamService, fail_stream_uses_caller_error_as_canonical_result)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int first_error = common::OB_SUCCESS;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->fail(common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->get_first_error(first_error));
  EXPECT_EQ(common::OB_TIMEOUT, first_error);
  handle.reset();

  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.fail_stream(stream_id, common::OB_CANCELED));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_CANCELED, meta.result_code_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
}

// APPEND 已提前写入 FINISHED 时，事务结束重复收尾仍需使用已持久化错误完成清理。
TEST_F(TestDirectLoadStreamService, repeated_finish_uses_persisted_result_and_cleans_queues)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle queue_zero;
  ObDirectLoadStreamBufferQueueHandle selected_queue;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.queue_entries_.at(0).queue_count_ = 3;
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_zero));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 2), selected_queue));
  ASSERT_EQ(common::OB_SUCCESS, queue_zero.get_queue()->fail(common::OB_CANCELED));
  ASSERT_EQ(common::OB_SUCCESS, selected_queue.get_queue()->fail(common::OB_CANCELED));
  queue_zero.reset();
  selected_queue.reset();

  // 模拟 APPEND 已先写入本次事务的真实错误，事务结束随后以另一错误重复收尾。
  ASSERT_EQ(common::OB_SUCCESS, stream_service.finish_stream(stream_id, common::OB_TIMEOUT));

  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamService::finish_and_cleanup_direct_load_stream(
                stream_id, common::OB_CANCELED));
  EXPECT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamService::finish_and_cleanup_direct_load_stream(
                stream_id, common::OB_ERR_UNEXPECTED));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  for (uint64_t queue_id = 0; queue_id < 3; ++queue_id) {
    EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_id), queue_zero));
  }
}

// 验证 queue create 冲突时，CREATE 将当前 attempted owner 也纳入幂等补偿。
TEST_F(TestDirectLoadStreamService, create_stream_cleans_attempted_owner_on_queue_conflict)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  const int64_t expected_stream_id = 1;
  const ObDirectLoadStreamBufferQueueKey key(expected_stream_id, 0);
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_buffer_queue_manager().create_queue(key, handle));
  handle.reset();
  EXPECT_EQ(common::OB_HASH_EXIST, stream_service.create_stream(create_param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, stream_service.get_buffer_queue_manager().get_queue(key, handle));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, stream_service.get_stream(expected_stream_id, meta));
}

// metadata INSERT 明确未提交时，CREATE 必须清理本次创建的 queue。
TEST_F(TestDirectLoadStreamService, metadata_create_not_committed_cleans_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  get_direct_load_stream_mock_meta_store().set_create_result_for_test(
      common::OB_TIMEOUT, DIRECT_LOAD_STREAM_META_NOT_CREATED);
  EXPECT_EQ(common::OB_TIMEOUT, stream_service.create_stream(create_param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(1, 0), handle));
}

// metadata INSERT 结果未知时，CREATE 保留 queue 供 orphan/reconcile，禁止盲目 cleanup。
TEST_F(TestDirectLoadStreamService, metadata_create_unknown_keeps_local_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  get_direct_load_stream_mock_meta_store().set_create_result_for_test(
      common::OB_TIMEOUT, DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN);
  EXPECT_EQ(common::OB_TIMEOUT, stream_service.create_stream(create_param, stream_id));
  EXPECT_FALSE(is_valid_stream_id(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(1, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, handle.get_queue()->get_state());
}

// 验证 batch create 创建全部本地 queue，seal 和 cleanup 复用同一组 queue id。
TEST_F(TestDirectLoadStreamService, local_queue_batch_create_seal_and_cleanup)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  common::ObArray<uint64_t> queue_ids;
  const int64_t stream_id = 1001;
  const int64_t deadline = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(3));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(4));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(5));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.create_local_queue_batch(
                stream_id, 5001, GCTX.self_addr(), queue_ids));
  for (int64_t i = 0; i < queue_ids.count(); ++i) {
    ObDirectLoadStreamBufferQueueHandle handle;
    ASSERT_EQ(common::OB_SUCCESS,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_ids.at(i)), handle));
    ASSERT_NE(nullptr, handle.get_queue());
    EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, handle.get_queue()->get_state());
  }
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.seal_local_queue_batch(
                stream_id, GCTX.self_addr(), queue_ids, deadline));
  for (int64_t i = 0; i < queue_ids.count(); ++i) {
    ObDirectLoadStreamBufferQueueHandle handle;
    ASSERT_EQ(common::OB_SUCCESS,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_ids.at(i)), handle));
    ASSERT_NE(nullptr, handle.get_queue());
    EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, handle.get_queue()->get_state());
  }
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.cleanup_local_queue_batch(
                stream_id, GCTX.self_addr(), queue_ids));
  for (int64_t i = 0; i < queue_ids.count(); ++i) {
    ObDirectLoadStreamBufferQueueHandle handle;
    EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
              stream_service.get_buffer_queue_manager().get_queue(
                  ObDirectLoadStreamBufferQueueKey(stream_id, queue_ids.at(i)), handle));
  }
}

// 验证 batch create 中途冲突时只补偿本次新建 queue，保留冲突前已存在的 queue。
TEST_F(TestDirectLoadStreamService, local_queue_batch_create_compensates_partial_result)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  common::ObArray<uint64_t> queue_ids;
  const int64_t stream_id = 1002;
  const ObDirectLoadStreamBufferQueueKey existing_key(stream_id, 4);

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(3));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(4));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(5));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().create_queue(existing_key, handle));
  handle.reset();
  EXPECT_EQ(common::OB_HASH_EXIST,
            stream_service.create_local_queue_batch(
                stream_id, 5001, GCTX.self_addr(), queue_ids));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 3), handle));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(existing_key, handle));
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, handle.get_queue()->get_state());
  handle.reset();
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 5), handle));
}

// 验证 batch 参数拒绝远端 owner、无序/重复 queue id 和已过期 seal deadline。
TEST_F(TestDirectLoadStreamService, local_queue_batch_rejects_invalid_contract)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  common::ObArray<uint64_t> queue_ids;
  common::ObAddr remote_addr;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_TRUE(remote_addr.set_ip_addr("127.0.0.2", 2882));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(4));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(3));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            stream_service.create_local_queue_batch(
                1003, 5001, GCTX.self_addr(), queue_ids));
  queue_ids.reset();
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(3));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            stream_service.create_local_queue_batch(1003, 5001, remote_addr, queue_ids));
  EXPECT_EQ(common::OB_TIMEOUT,
            stream_service.seal_local_queue_batch(
                1003, GCTX.self_addr(), queue_ids,
                common::ObTimeUtility::current_time() - 1));
}

// 验证 control executor 通过 tenant-local Service 创建 queue，并返回 canonical result_code。
TEST_F(TestDirectLoadStreamService, control_rpc_executor_create_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamControlRequest request;
  ObDirectLoadStreamControlResult result;
  ObDirectLoadStreamControlCreateQueueArg arg;
  ObDirectLoadStreamControlCreateQueueRes res;
  ObDirectLoadStreamBufferQueueHandle handle;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  arg.tenant_id_ = TEST_TENANT_ID;
  arg.stream_id_ = 1101;
  arg.target_schema_version_ = 5001;
  arg.owner_server_ = GCTX.self_addr();
  ASSERT_EQ(common::OB_SUCCESS, arg.queue_ids_.push_back(2));
  ASSERT_EQ(common::OB_SUCCESS, arg.queue_ids_.push_back(3));
  request.command_type_ = ObDirectLoadStreamControlCommandType::CREATE_QUEUE;
  result.allocator_ = &allocator;
  ASSERT_EQ(common::OB_SUCCESS, request.set_arg(arg, allocator));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamControlRpcExecutor::dispatch(request, result, allocator));
  ASSERT_EQ(ObDirectLoadStreamControlCommandType::CREATE_QUEUE, result.command_type_);
  ASSERT_EQ(common::OB_SUCCESS, result.get_res(res));
  ASSERT_EQ(common::OB_SUCCESS, res.result_code_);
  EXPECT_EQ(common::OB_SUCCESS,
            tenant_env.get_mtl_stream_service().get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(1101, 2), handle));
}

// 验证 control executor 将本地业务错误编码进结果，而不是伪装成 transport 失败。
TEST_F(TestDirectLoadStreamService, control_rpc_executor_returns_business_error)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamControlRequest request;
  ObDirectLoadStreamControlResult result;
  ObDirectLoadStreamControlCreateQueueArg arg;
  ObDirectLoadStreamControlCreateQueueRes res;
  common::ObAddr remote_addr;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ASSERT_TRUE(remote_addr.set_ip_addr("127.0.0.2", 2882));
  arg.tenant_id_ = TEST_TENANT_ID;
  arg.stream_id_ = 1102;
  arg.target_schema_version_ = 5001;
  arg.owner_server_ = remote_addr;
  ASSERT_EQ(common::OB_SUCCESS, arg.queue_ids_.push_back(0));
  request.command_type_ = ObDirectLoadStreamControlCommandType::CREATE_QUEUE;
  result.allocator_ = &allocator;
  ASSERT_EQ(common::OB_SUCCESS, request.set_arg(arg, allocator));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamControlRpcExecutor::dispatch(request, result, allocator));
  ASSERT_EQ(common::OB_SUCCESS, result.get_res(res));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, res.result_code_);
}

// 验证 SEAL 的 absolute deadline 经 executor 进入 Service，并返回 timeout 业务结果。
TEST_F(TestDirectLoadStreamService, control_rpc_executor_preserves_seal_deadline)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamControlRequest request;
  ObDirectLoadStreamControlResult result;
  ObDirectLoadStreamControlSealQueueArg arg;
  ObDirectLoadStreamControlSealQueueRes res;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  arg.tenant_id_ = TEST_TENANT_ID;
  arg.stream_id_ = 1103;
  arg.owner_server_ = GCTX.self_addr();
  arg.absolute_deadline_ = common::ObTimeUtility::current_time() - 1;
  ASSERT_EQ(common::OB_SUCCESS, arg.queue_ids_.push_back(0));
  request.command_type_ = ObDirectLoadStreamControlCommandType::SEAL_QUEUE;
  result.allocator_ = &allocator;
  ASSERT_EQ(common::OB_SUCCESS, request.set_arg(arg, allocator));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamControlRpcExecutor::dispatch(request, result, allocator));
  ASSERT_EQ(common::OB_SUCCESS, result.get_res(res));
  EXPECT_EQ(common::OB_TIMEOUT, res.result_code_);
}

// 验证 CLEANUP executor 对不存在 queue 保持幂等成功。
TEST_F(TestDirectLoadStreamService, control_rpc_executor_cleanup_is_idempotent)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamControlRequest request;
  ObDirectLoadStreamControlResult result;
  ObDirectLoadStreamControlCleanupQueueArg arg;
  ObDirectLoadStreamControlCleanupQueueRes res;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  arg.tenant_id_ = TEST_TENANT_ID;
  arg.stream_id_ = 1104;
  arg.owner_server_ = GCTX.self_addr();
  ASSERT_EQ(common::OB_SUCCESS, arg.queue_ids_.push_back(0));
  request.command_type_ = ObDirectLoadStreamControlCommandType::CLEANUP_QUEUE;
  result.allocator_ = &allocator;
  ASSERT_EQ(common::OB_SUCCESS, request.set_arg(arg, allocator));
  ASSERT_EQ(common::OB_SUCCESS,
            ObDirectLoadStreamControlRpcExecutor::dispatch(request, result, allocator));
  ASSERT_EQ(common::OB_SUCCESS, result.get_res(res));
  EXPECT_EQ(common::OB_SUCCESS, res.result_code_);
}

// 验证 OPEN stream close 时会先 seal local queue，再推进 metadata 到 SEALED。
TEST_F(TestDirectLoadStreamService, seal_stream_seals_local_queue_and_metadata)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, handle.get_queue()->get_state());
}

// 验证重复 seal 已 SEALED 的 stream 幂等成功。
TEST_F(TestDirectLoadStreamService, seal_stream_is_idempotent)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, handle.get_queue()->get_state());
}

// 验证两个并发 CLOSE 都能在 queue seal 后收敛为 SEALED 幂等成功。
TEST_F(TestDirectLoadStreamService, concurrent_seal_streams_are_idempotent)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamAppendHandle append_handle;
  ObDirectLoadStreamMeta meta;
  std::future<int> first_close_future;
  std::future<int> second_close_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_TRUE(handle.is_valid());
  ASSERT_NE(nullptr, handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->begin_append(append_handle));

  first_close_future = std::async(std::launch::async, seal_stream_for_test,
                                  &stream_service, stream_id);
  second_close_future = std::async(std::launch::async, seal_stream_for_test,
                                   &stream_service, stream_id);
  ASSERT_TRUE(wait_until_local_queue_state(
      stream_service, stream_id, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(std::future_status::timeout, first_close_future.wait_for(SHORT_WAIT));
  EXPECT_EQ(std::future_status::timeout, second_close_future.wait_for(SHORT_WAIT));
  ASSERT_EQ(common::OB_SUCCESS, handle.get_queue()->end_append(append_handle));
  ASSERT_EQ(std::future_status::ready, first_close_future.wait_for(LONG_WAIT));
  ASSERT_EQ(std::future_status::ready, second_close_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, first_close_future.get());
  EXPECT_EQ(common::OB_SUCCESS, second_close_future.get());

  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, handle.get_queue()->get_state());
}

// 验证 seal 不存在的 stream 直接返回 missing stream。
TEST_F(TestDirectLoadStreamService, seal_stream_rejects_missing_stream)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  const int64_t missing_stream_id = 12345;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, stream_service.seal_stream(missing_stream_id));
}

// 验证已 SEALED 后 queue 被清理，重复 seal 仍成功且不改写 metadata。
TEST_F(TestDirectLoadStreamService, sealed_stream_ignores_missing_queue_on_reseal)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().cleanup_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0)));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
}

// 验证 OPEN stream 本地 queue 缺失时写入 FINISHED(result_code)，并返回终态错误。
TEST_F(TestDirectLoadStreamService, seal_stream_rejects_missing_queue)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().cleanup_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0)));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, meta.result_code_);
}

// 验证 FINISHED 终态不会被 seal 覆盖，调用返回保存的终态错误码。
TEST_F(TestDirectLoadStreamService, seal_stream_keeps_finished_result)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.finish_stream(stream_id, common::OB_TIMEOUT));
  EXPECT_EQ(common::OB_TIMEOUT, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  ASSERT_NE(nullptr, handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, handle.get_queue()->get_state());
}

// 验证 CLOSE 与并发 FINISHED 竞争时返回保存的终态错误码，而不是状态竞争错误。
TEST_F(TestDirectLoadStreamService, seal_stream_rereads_finished_result_after_race)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  std::future<int> close_future;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamAppendHandle append_handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  ASSERT_TRUE(queue_handle.is_valid());
  ASSERT_NE(nullptr, queue_handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, queue_handle.get_queue()->begin_append(append_handle));

  close_future = std::async(std::launch::async, seal_stream_for_test,
                            &stream_service, stream_id);
  ASSERT_TRUE(wait_until_local_queue_state(
      stream_service, stream_id, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(std::future_status::timeout, close_future.wait_for(SHORT_WAIT));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.finish_stream(stream_id, common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS, queue_handle.get_queue()->end_append(append_handle));
  ASSERT_EQ(std::future_status::ready, close_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_TIMEOUT, close_future.get());

  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  ASSERT_NE(nullptr, queue_handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, queue_handle.get_queue()->get_state());
}

// 验证 queue seal 失败输给并发 FINISHED 时，CLOSE 返回 metadata 赢家错误码。
TEST_F(TestDirectLoadStreamService, seal_stream_returns_finished_winner_after_queue_error)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamService stream_service(TEST_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  std::future<int> close_future;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamAppendHandle append_handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, init_stream_service_with_mock_store(stream_service));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  ASSERT_TRUE(queue_handle.is_valid());
  ASSERT_NE(nullptr, queue_handle.get_queue());
  ASSERT_EQ(common::OB_SUCCESS, queue_handle.get_queue()->begin_append(append_handle));

  close_future = std::async(std::launch::async, seal_stream_for_test,
                            &stream_service, stream_id);
  ASSERT_TRUE(wait_until_local_queue_state(
      stream_service, stream_id, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT)));
  EXPECT_EQ(std::future_status::timeout, close_future.wait_for(SHORT_WAIT));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.finish_stream(stream_id, common::OB_CANCELED));
  ASSERT_EQ(common::OB_SUCCESS, queue_handle.get_queue()->fail(common::OB_TIMEOUT));
  ASSERT_EQ(std::future_status::ready, close_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_CANCELED, close_future.get());
  EXPECT_EQ(common::OB_TIMEOUT, queue_handle.get_queue()->end_append(append_handle));

  ASSERT_EQ(common::OB_SUCCESS, stream_service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_CANCELED, meta.result_code_);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED,
            queue_handle.get_queue()->get_state());
}

template <typename Arg>
int execute_forward_command(const ObDirectLoadStreamForwardCommandType command,
                            const Arg &arg,
                            const common::ObAddr &source_server,
                            int &result_code)
{
  int ret = common::OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamForwardRequest request;
  ObDirectLoadStreamForwardResult result;
  ObDirectLoadStreamForwardRes res;
  result_code = common::OB_SUCCESS;
  request.command_type_ = command;
  result.allocator_ = &allocator;
  if (OB_FAIL(request.set_arg(arg, allocator))) {
  } else if (OB_FAIL(ObDirectLoadStreamForwardRpcExecutor::dispatch(
                 request, result, allocator, source_server))) {
  } else if (OB_FAIL(result.get_res(res))) {
  } else {
    result_code = res.result_code_;
  }
  return ret;
}

// D009：BEGIN 携带完整不可变上下文，后续 RPC 只携带 statement_id。
TEST_F(TestDirectLoadStreamService, forward_rpc_begin_chunk_end_roundtrip)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardChunkArg chunk_arg;
  ObDirectLoadStreamForwardEndArg end_arg;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObString stored_chunk;
  int result_code = common::OB_SUCCESS;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.queue_entries_.at(0).queue_count_ = 3;
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));

  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 2;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20001;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;
  ASSERT_EQ(common::OB_SUCCESS,
            execute_forward_command(
                ObDirectLoadStreamForwardCommandType::BEGIN,
                begin_arg, GCTX.self_addr(), result_code));
  ASSERT_EQ(common::OB_SUCCESS, result_code);

  chunk_arg.statement_id_ = begin_arg.statement_id_;
  chunk_arg.chunk_ = common::ObString::make_string("forward-chunk");
  ASSERT_EQ(common::OB_SUCCESS,
            execute_forward_command(
                ObDirectLoadStreamForwardCommandType::CHUNK,
                chunk_arg, GCTX.self_addr(), result_code));
  ASSERT_EQ(common::OB_SUCCESS, result_code);

  end_arg.statement_id_ = begin_arg.statement_id_;
  ASSERT_EQ(common::OB_SUCCESS,
            execute_forward_command(
                ObDirectLoadStreamForwardCommandType::END,
                end_arg, GCTX.self_addr(), result_code));
  ASSERT_EQ(common::OB_SUCCESS, result_code);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 2), queue_handle));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            queue_handle.get_queue()->get_next_chunk(allocator, stored_chunk));
  EXPECT_EQ(chunk_arg.chunk_, stored_chunk);
  EXPECT_EQ(common::OB_ITER_END,
            queue_handle.get_queue()->get_next_chunk(allocator, stored_chunk));
}

// 重复 BEGIN 必须在第二次获取 queue admission 前被拒绝。
TEST_F(TestDirectLoadStreamService, append_session_duplicate_begin_is_side_effect_free)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardAbortArg abort_arg;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 0;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20002;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;

  ObDirectLoadStreamAppendSessionManager &manager =
      stream_service.get_append_session_manager();
  ASSERT_EQ(common::OB_SUCCESS, manager.begin(begin_arg, GCTX.self_addr()));
  EXPECT_EQ(common::OB_HASH_EXIST, manager.begin(begin_arg, GCTX.self_addr()));
  abort_arg.statement_id_ = begin_arg.statement_id_;
  abort_arg.result_code_ = common::OB_CANCELED;
  EXPECT_EQ(common::OB_SUCCESS, manager.abort(abort_arg));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED,
            queue_handle.get_queue()->get_state());
}

// END 必须返回最终 end_append 的真实结果。
TEST_F(TestDirectLoadStreamService, append_session_end_propagates_queue_error)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardEndArg end_arg;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 0;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20003;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;
  ObDirectLoadStreamAppendSessionManager &manager =
      stream_service.get_append_session_manager();
  ASSERT_EQ(common::OB_SUCCESS, manager.begin(begin_arg, GCTX.self_addr()));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  ASSERT_EQ(common::OB_SUCCESS, queue_handle.get_queue()->fail(common::OB_TIMEOUT));
  end_arg.statement_id_ = begin_arg.statement_id_;
  EXPECT_EQ(common::OB_TIMEOUT, manager.end(end_arg));
}

// 过期只移除 session 而不等待，并将 queue 留在失败状态。
TEST_F(TestDirectLoadStreamService, append_session_expire_erases_and_releases)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardChunkArg chunk_arg;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  int result_code = common::OB_SUCCESS;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 0;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20004;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_append_session_manager().begin(
                begin_arg, GCTX.self_addr()));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.expire_append_sessions(begin_arg.absolute_deadline_));
  chunk_arg.statement_id_ = begin_arg.statement_id_;
  chunk_arg.chunk_ = common::ObString::make_string("late-chunk");
  ASSERT_EQ(common::OB_SUCCESS,
            execute_forward_command(
                ObDirectLoadStreamForwardCommandType::CHUNK,
                chunk_arg, GCTX.self_addr(), result_code));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, result_code);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED,
            queue_handle.get_queue()->get_state());
}

// queue cleanup 先摘除所有 session，再移除 queue。
TEST_F(TestDirectLoadStreamService, queue_cleanup_detaches_append_sessions_first)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardEndArg end_arg;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  common::ObArray<uint64_t> queue_ids;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 0;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20005;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.get_append_session_manager().begin(
                begin_arg, GCTX.self_addr()));
  ASSERT_EQ(common::OB_SUCCESS, queue_ids.push_back(0));
  EXPECT_EQ(common::OB_SUCCESS,
            stream_service.cleanup_local_queue_batch(
                stream_id, GCTX.self_addr(), queue_ids));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_buffer_queue_manager().get_queue(
                ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle));
  end_arg.statement_id_ = begin_arg.statement_id_;
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST,
            stream_service.get_append_session_manager().end(end_arg));
}

// terminal RPC 不等待已经持有临时 session 引用的 CHUNK。
TEST_F(TestDirectLoadStreamService, concurrent_chunk_makes_end_return_eagain_without_wait)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamForwardBeginArg begin_arg;
  ObDirectLoadStreamForwardChunkArg chunk_arg;
  ObDirectLoadStreamForwardEndArg end_arg;
  std::future<int> chunk_future;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.create_stream(create_param, stream_id));
  ObDirectLoadStreamMockQueueStore *store =
      get_direct_load_stream_mock_store_factory().get_last_store();
  ASSERT_NE(nullptr, store);
  store->block_append();

  begin_arg.tenant_id_ = TEST_TENANT_ID;
  begin_arg.stream_id_ = stream_id;
  begin_arg.queue_id_ = 0;
  begin_arg.target_schema_version_ = create_param.target_schema_version_;
  begin_arg.statement_id_ = 20006;
  begin_arg.absolute_deadline_ = common::ObTimeUtility::current_time() + 5 * 1000 * 1000;
  ObDirectLoadStreamAppendSessionManager &manager =
      stream_service.get_append_session_manager();
  ASSERT_EQ(common::OB_SUCCESS, manager.begin(begin_arg, GCTX.self_addr()));
  chunk_arg.statement_id_ = begin_arg.statement_id_;
  chunk_arg.chunk_ = common::ObString::make_string("blocked-chunk");
  chunk_future = std::async(std::launch::async, append_forward_chunk_for_test,
                            &manager, chunk_arg);
  const bool append_entered = store->wait_until_append_entered(LONG_WAIT);
  if (!append_entered) {
    store->unblock_append();
  }
  ASSERT_TRUE(append_entered);
  end_arg.statement_id_ = begin_arg.statement_id_;
  EXPECT_EQ(common::OB_EAGAIN, manager.end(end_arg));
  store->unblock_append();
  ASSERT_EQ(std::future_status::ready, chunk_future.wait_for(LONG_WAIT));
  EXPECT_EQ(common::OB_SUCCESS, chunk_future.get());
  EXPECT_EQ(common::OB_SUCCESS, stream_service.seal_stream(stream_id));
}

// 严格消费 payload，拒绝尾部包含额外 wire bytes 的有效 D009 CHUNK。
TEST_F(TestDirectLoadStreamService, forward_rpc_rejects_trailing_payload)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDirectLoadStreamForwardChunkArg arg;
  ObDirectLoadStreamForwardChunkArg decoded_arg;
  ObDirectLoadStreamForwardRequest request;
  ObDirectLoadStreamForwardRequest corrupt_request;
  arg.statement_id_ = 20007;
  arg.chunk_ = common::ObString::make_string("chunk");
  ASSERT_EQ(common::OB_SUCCESS, request.set_arg(arg, allocator));
  char *buf = static_cast<char *>(allocator.alloc(request.arg_content_.length() + 1));
  ASSERT_NE(nullptr, buf);
  MEMCPY(buf, request.arg_content_.ptr(), request.arg_content_.length());
  buf[request.arg_content_.length()] = 0;
  corrupt_request.command_type_ = ObDirectLoadStreamForwardCommandType::CHUNK;
  corrupt_request.arg_content_.assign_ptr(buf, request.arg_content_.length() + 1);
  EXPECT_EQ(common::OB_INVALID_DATA, corrupt_request.get_arg(decoded_arg));
}

int build_route_meta(const int64_t stream_id,
                     ObDirectLoadStreamMeta &meta,
                     const int64_t local_queue_count = 2)
{
  int ret = common::OB_SUCCESS;
  common::ObAddr remote_addr;
  meta = ObDirectLoadStreamMeta();
  meta.stream_id_ = stream_id;
  meta.tenant_id_ = TEST_TENANT_ID;
  meta.status_ = DIRECT_LOAD_STREAM_STATUS_OPEN;
  if (OB_UNLIKELY(!remote_addr.set_ip_addr("127.0.0.2", 2882))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(meta.queue_entries_.push_back(
                         table::ObDirectLoadStreamQueueEntry(
                             GCTX.self_addr(), local_queue_count)))) {
  } else if (OB_FAIL(meta.queue_entries_.push_back(
                         table::ObDirectLoadStreamQueueEntry(remote_addr, 1)))) {
  }
  return ret;
}

// RouteCtx 按冻结 topology 轮询并按值返回 descriptor，
// 调用方修改不得污染缓存。
TEST_F(TestDirectLoadStreamService, route_ctx_round_robin_returns_queue_desc_by_value)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamMeta meta;
  table::ObDirectLoadStreamQueueDesc first;
  table::ObDirectLoadStreamQueueDesc selected;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_route_meta(8101, meta));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, first));
  ASSERT_TRUE(first.is_valid());
  selected = first;
  selected.queue_id_ = common::OB_INVALID_ID;
  for (int64_t i = 1; i <= 6; ++i) {
    ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, selected));
    EXPECT_EQ((first.queue_id_ + i) % 3, selected.queue_id_);
    const common::ObAddr &expected_owner = selected.queue_id_ < 2
        ? GCTX.self_addr() : meta.queue_entries_.at(1).server_addr_;
    EXPECT_EQ(expected_owner, selected.server_addr_);
  }
}

// 显式删除 RouteCtx 后，下一次选择从同一确定性 seed 重新开始。
TEST_F(TestDirectLoadStreamService, erase_route_ctx_restarts_deterministic_seed)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamMeta meta;
  table::ObDirectLoadStreamQueueDesc first;
  table::ObDirectLoadStreamQueueDesc second;
  table::ObDirectLoadStreamQueueDesc restarted;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_route_meta(8102, meta));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, first));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, second));
  EXPECT_NE(first.queue_id_, second.queue_id_);
  stream_service.erase_route_ctx(meta.stream_id_);
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, restarted));
  EXPECT_EQ(first.queue_id_, restarted.queue_id_);
  EXPECT_EQ(first.server_addr_, restarted.server_addr_);
}

// 达到数量上限时 lazy LRU 淘汰最旧 RouteCtx，重新访问会重建初始游标。
TEST_F(TestDirectLoadStreamService, route_ctx_lazy_lru_respects_count_limit)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamMeta first_meta;
  ObDirectLoadStreamMeta second_meta;
  table::ObDirectLoadStreamQueueDesc first_initial;
  table::ObDirectLoadStreamQueueDesc first_next;
  table::ObDirectLoadStreamQueueDesc ignored;
  table::ObDirectLoadStreamQueueDesc first_rebuilt;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, stream_service.set_route_ctx_limit_for_test(1));
  ASSERT_EQ(common::OB_SUCCESS, build_route_meta(8103, first_meta, 1));
  ASSERT_EQ(common::OB_SUCCESS, build_route_meta(8104, second_meta, 1));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.select_append_queue(first_meta, first_initial));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.select_append_queue(first_meta, first_next));
  EXPECT_NE(first_initial.queue_id_, first_next.queue_id_);
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.select_append_queue(second_meta, ignored));
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.select_append_queue(first_meta, first_rebuilt));
  EXPECT_EQ(first_initial.queue_id_, first_rebuilt.queue_id_);
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.set_route_ctx_limit_for_test(2));
}

// 非 OPEN metadata 拒绝选路并清除缓存，恢复 OPEN 后必须重新建立 RouteCtx。
TEST_F(TestDirectLoadStreamService, non_open_route_erases_cached_ctx)
{
  MockDirectLoadStreamServiceTenantEnv tenant_env;
  ObDirectLoadStreamMeta meta;
  table::ObDirectLoadStreamQueueDesc first;
  table::ObDirectLoadStreamQueueDesc second;
  table::ObDirectLoadStreamQueueDesc restarted;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init_table_load_service());
  ObDirectLoadStreamService &stream_service = tenant_env.get_mtl_stream_service();
  ASSERT_EQ(common::OB_SUCCESS, build_route_meta(8105, meta, 1));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, first));
  ASSERT_EQ(common::OB_SUCCESS, stream_service.select_append_queue(meta, second));
  meta.status_ = DIRECT_LOAD_STREAM_STATUS_SEALED;
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            stream_service.select_append_queue(meta, restarted));
  meta.status_ = DIRECT_LOAD_STREAM_STATUS_OPEN;
  ASSERT_EQ(common::OB_SUCCESS,
            stream_service.select_append_queue(meta, restarted));
  EXPECT_EQ(first.queue_id_, restarted.queue_id_);
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

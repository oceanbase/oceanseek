/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <future>
#include <thread>
#include "../test_sql_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_executor.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_executor.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_proxy.h"
#include "../../observer/table_load/test_direct_load_stream_utils.h"
#include "share/ob_server_struct.h"
#include "share/ob_get_compat_mode.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "sql/ob_sql_utils.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/das/ob_das_context.h"
#include "sql/engine/direct_load/ob_direct_load_stream_append_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/executor/ob_executor.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using oceanbase::table::ObDirectLoadStreamQueueEntry;

namespace
{
static const uint64_t TEST_TENANT_ID = 1002;
static const char TEST_SCHEMA_FILE[] =
    "../../../../unittest/sql/resolver/test_direct_load_stream_append_resolver.schema";
static const uint64_t TEST_STREAM_USER_ID = 2001;
static const uint64_t TEST_STREAM_ROLE_ID = 2002;
static const int64_t TEST_ROLLOVER_PAYLOAD_SIZE = 256;
static const uint64_t TEST_TIMEOUT_CHECK_ROW_COUNT = 7;

// Keep production RPC encoding, dispatch, sessions and queues; replace transport only.
class ScopedStreamForwardRpc : public obrpc::ObSrvRpcProxy
{
public:
  explicit ScopedStreamForwardRpc(const bool lose_end_response = false)
    : begin_count_(0), chunk_count_(0), end_count_(0), abort_count_(0),
      lose_end_response_(lose_end_response), proxy_(this),
      previous_proxy_(GCTX.srv_rpc_proxy_)
  {
    GCTX.srv_rpc_proxy_ = &proxy_;
  }
  ~ScopedStreamForwardRpc() { GCTX.srv_rpc_proxy_ = previous_proxy_; }
  int direct_load_stream_forward(const ObDirectLoadStreamForwardRequest &request,
                                 ObDirectLoadStreamForwardResult &result,
                                 const obrpc::ObRpcOpts &opts) override
  {
    UNUSED(opts);
    switch (request.command_type_) {
      case ObDirectLoadStreamForwardCommandType::BEGIN: ++begin_count_; break;
      case ObDirectLoadStreamForwardCommandType::CHUNK: ++chunk_count_; break;
      case ObDirectLoadStreamForwardCommandType::END: ++end_count_; break;
      case ObDirectLoadStreamForwardCommandType::ABORT: ++abort_count_; break;
      default: break;
    }
    const ObAddr source_server = GCTX.self_addr();
    GCTX.self_addr_seq_.set_addr(get_server());
    const int ret = ObDirectLoadStreamForwardRpcExecutor::dispatch(
        request, result, *result.allocator_, source_server);
    GCTX.self_addr_seq_.set_addr(source_server);
    return OB_SUCCESS == ret && lose_end_response_
        && ObDirectLoadStreamForwardCommandType::END == request.command_type_ ? OB_TIMEOUT : ret;
  }
  int direct_load_stream_control(const ObDirectLoadStreamControlRequest &request,
                                 ObDirectLoadStreamControlResult &result,
                                 const obrpc::ObRpcOpts &opts) override
  {
    UNUSED(opts);
    const ObAddr source_server = GCTX.self_addr();
    GCTX.self_addr_seq_.set_addr(get_server());
    const int ret = ObDirectLoadStreamControlRpcExecutor::dispatch(
        request, result, *result.allocator_);
    GCTX.self_addr_seq_.set_addr(source_server);
    return ret;
  }
  int64_t begin_count_;
  int64_t chunk_count_;
  int64_t end_count_;
  int64_t abort_count_;
private:
  bool lose_end_response_;
  obrpc::ObSrvRpcProxy proxy_;
  obrpc::ObSrvRpcProxy *previous_proxy_;
};

int add_column_insert_priv(MockSchemaService &schema_service,
                           const uint64_t tenant_id,
                           const uint64_t user_id,
                           const char *database_name,
                           const char *table_name,
                           const char *column_name,
                           const uint64_t priv_id)
{
  int ret = OB_SUCCESS;
  ObColumnPriv column_priv;
  column_priv.set_tenant_id(tenant_id);
  column_priv.set_user_id(user_id);
  column_priv.set_schema_version(OB_CORE_SCHEMA_VERSION);
  column_priv.set_priv_set(OB_PRIV_INSERT);
  column_priv.set_priv_id(priv_id);
  if (OB_FAIL(column_priv.set_database_name(database_name))) {
    LOG_WARN("failed to set test column privilege database", K(ret), K(database_name));
  } else if (OB_FAIL(column_priv.set_table_name(table_name))) {
    LOG_WARN("failed to set test column privilege table", K(ret), K(table_name));
  } else if (OB_FAIL(column_priv.set_column_name(column_name))) {
    LOG_WARN("failed to set test column privilege column", K(ret), K(column_name));
  } else if (OB_FAIL(schema_service.mgr_.priv_mgr_.add_column_priv(column_priv))) {
    LOG_WARN("failed to add test column insert privilege", K(ret), K(column_priv));
  }
  return ret;
}

int add_table_insert_priv(MockSchemaService &schema_service,
                          const uint64_t tenant_id,
                          const uint64_t grantee_id,
                          const char *database_name,
                          const char *table_name)
{
  int ret = OB_SUCCESS;
  ObTablePriv table_priv;
  table_priv.set_tenant_id(tenant_id);
  table_priv.set_user_id(grantee_id);
  table_priv.set_schema_version(OB_CORE_SCHEMA_VERSION);
  table_priv.set_priv_set(OB_PRIV_INSERT);
  if (OB_FAIL(table_priv.set_database_name(database_name))) {
    LOG_WARN("failed to set test table privilege database", K(ret), K(database_name));
  } else if (OB_FAIL(table_priv.set_table_name(table_name))) {
    LOG_WARN("failed to set test table privilege table", K(ret), K(table_name));
  } else if (OB_FAIL(schema_service.mgr_.priv_mgr_.add_table_priv(table_priv))) {
    LOG_WARN("failed to add test table INSERT privilege", K(ret), K(table_priv));
  }
  return ret;
}

void reset_query_ctx_schema_guard(test::TestSqlUtils &test_utils)
{
  ASSERT_NE(nullptr, test_utils.stmt_factory_.get_query_ctx());
  test_utils.stmt_factory_.get_query_ctx()->sql_schema_guard_.set_schema_guard(
      &test_utils.schema_guard_);
}

void prepare_user_tenant(test::TestSqlUtils &test_utils)
{
  ObTenantSchema tenant_schema;
  ObSysVariableSchema sys_variable;
  ObUserInfo login_user;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  int compat_init_ret = OB_SUCCESS;
  tenant_schema.set_tenant_id(TEST_TENANT_ID);
  tenant_schema.set_schema_version(test_utils.schema_version_);
  tenant_schema.set_locality("auto_locality_strategy");
  ASSERT_EQ(OB_SUCCESS, tenant_schema.add_zone("zone"));
  ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("dls_tenant"));
  ASSERT_EQ(OB_SUCCESS,
            test_utils.schema_service_->add_tenant_schema(
                tenant_schema, test_utils.schema_version_++));

  sys_variable.set_tenant_id(TEST_TENANT_ID);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_variable.set_schema_version(test_utils.schema_version_);
  ASSERT_EQ(OB_SUCCESS,
            test_utils.schema_service_->add_sys_variable_schema(
                sys_variable, test_utils.schema_version_++));

  login_user.set_tenant_id(TEST_TENANT_ID);
  login_user.set_user_id(test_utils.session_info_.get_priv_user_id());
  login_user.set_schema_version(test_utils.schema_version_);
  login_user.set_priv_set(test_utils.session_info_.get_user_priv_set());
  ASSERT_EQ(OB_SUCCESS, login_user.set_user_name(OB_SYS_USER_NAME));
  ASSERT_EQ(OB_SUCCESS, login_user.set_host(OB_SYS_HOST_NAME));
  ASSERT_EQ(OB_SUCCESS,
            test_utils.schema_service_->add_user_schema(
                login_user, test_utils.schema_version_++));
  ASSERT_EQ(OB_SUCCESS,
            test_utils.schema_service_->get_schema_guard(test_utils.schema_guard_));

  ASSERT_EQ(OB_SUCCESS,
            omt::ObTenantConfigMgr::get_instance().add_tenant_config(TEST_TENANT_ID));
  compat_init_ret = ObCompatModeGetter::instance().init(nullptr);
  ASSERT_TRUE(OB_SUCCESS == compat_init_ret || OB_INIT_TWICE == compat_init_ret);
  ASSERT_EQ(OB_SUCCESS,
            ObCompatModeGetter::instance().set_tenant_compat_mode(
                TEST_TENANT_ID, compat_mode));
  ASSERT_EQ(OB_SUCCESS,
            omt::ObTenantTimezoneMgr::get_instance().add_tenant_timezone(TEST_TENANT_ID));

  test_utils.sys_tenant_id_ = TEST_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, test_utils.session_info_.switch_tenant(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS,
            test_utils.session_info_.set_tenant(
                ObString::make_string("dls_tenant"), TEST_TENANT_ID));
  ASSERT_EQ(TEST_TENANT_ID, test_utils.session_info_.get_priv_tenant_id());
  ASSERT_EQ(TEST_TENANT_ID, test_utils.session_info_.get_effective_tenant_id());
  reset_query_ctx_schema_guard(test_utils);
}

void prepare_test_schema(test::TestSqlUtils &test_utils)
{
  ASSERT_NO_FATAL_FAILURE(test_utils.load_schema_from_file(TEST_SCHEMA_FILE));
  reset_query_ctx_schema_guard(test_utils);
}

enum class AppendExecutionMode
{
  NORMAL,
  RESCAN_BEFORE_QUEUE_TOUCH,
  PUBLISH_MOCK_CHUNK_THEN_RESCAN,
  SMALL_CHUNK_ROW_ERROR,
  FAIL_AFTER_FIRST_CHUNK_TIMEOUT
};

enum class AppendBeforeRunAction
{
  NONE,
  DROP_TARGET_TABLE,
  REVOKE_INSERT_PRIV
};

enum class AppendUnsupportedShape
{
  NONE,
  TRIGGER,
  FOREIGN_KEY
};

struct AppendShapeGateResult
{
  AppendShapeGateResult()
  {
    reset();
  }
  void reset()
  {
    captured_ = false;
    queue_handle_valid_ = false;
    append_registered_ = false;
    remote_append_started_ = false;
  }
  bool captured_;
  bool queue_handle_valid_;
  bool append_registered_;
  bool remote_append_started_;
};

struct AppendRescanResult
{
  AppendRescanResult()
    : rescan_ret_(OB_SUCCESS),
      append_registered_before_rescan_(false),
      append_registered_after_rescan_(false),
      queue_touched_after_rescan_(false)
  {
  }
  void reset()
  {
    rescan_ret_ = OB_SUCCESS;
    append_registered_before_rescan_ = false;
    append_registered_after_rescan_ = false;
    queue_touched_after_rescan_ = false;
  }
  int rescan_ret_;
  bool append_registered_before_rescan_;
  bool append_registered_after_rescan_;
  bool queue_touched_after_rescan_;
};

class WorkerTimeoutResetGuard
{
public:
  WorkerTimeoutResetGuard() : original_timeout_ts_(THIS_WORKER.get_timeout_ts()) {}
  ~WorkerTimeoutResetGuard()
  {
    THIS_WORKER.set_timeout_ts(original_timeout_ts_);
  }
private:
  int64_t original_timeout_ts_;
  WorkerTimeoutResetGuard(const WorkerTimeoutResetGuard &) = delete;
  WorkerTimeoutResetGuard &operator=(const WorkerTimeoutResetGuard &) = delete;
};

class ExplicitTransactionTestGuard
{
public:
  explicit ExplicitTransactionTestGuard(ObSQLSessionInfo &session)
    : session_(session), original_tx_desc_(session.get_tx_desc()), tx_desc_()
  {
    tx_desc_.set_explicit();
    session_.get_tx_desc() = &tx_desc_;
  }
  ~ExplicitTransactionTestGuard()
  {
    session_.get_tx_desc() = original_tx_desc_;
  }
private:
  ObSQLSessionInfo &session_;
  transaction::ObTxDesc *original_tx_desc_;
  transaction::ObTxDesc tx_desc_;
  ExplicitTransactionTestGuard(const ExplicitTransactionTestGuard &) = delete;
  ExplicitTransactionTestGuard &operator=(const ExplicitTransactionTestGuard &) = delete;
};

// 在 APPEND 已释放登记、尚未写失败 metadata 时插入竞争请求，检查失败不能被正常
// CLOSE/EOF 越过。只使用既有 metadata mock，不在生产路径加入测试 hook。
class AppendReleaseRaceMetaStore : public ObMockDirectLoadStreamMetaStore
{
public:
  explicit AppendReleaseRaceMetaStore(const uint64_t tenant_id)
    : ObMockDirectLoadStreamMetaStore(tenant_id),
      observed_stream_id_(OB_INVALID_ID), observed_(false), active_appends_(-1),
      ready_chunks_(0), queue_state_(ObDirectLoadStreamBufferQueueState::REMOVED),
      queue_error_ret_(OB_NOT_INIT), queue_error_(OB_SUCCESS), close_ret_(OB_NOT_INIT),
      read_ret_(OB_NOT_INIT), read_chunks_(0), drain_ret_(OB_NOT_INIT),
      success_finish_ret_(OB_NOT_INIT)
  {
  }
  int get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta) override
  {
    if (OB_INVALID_ID != observed_stream_id_ && stream_id == observed_stream_id_) {
      ObTableLoadService *table_load_service = MTL(ObTableLoadService *);
      ObDirectLoadStreamService &service = table_load_service->get_direct_load_stream_service();
      ObDirectLoadStreamBufferQueueHandle handle;
      const int get_ret = service.get_buffer_queue_manager().get_queue(
          ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle);
      if (OB_SUCCESS == get_ret && nullptr != handle.get_queue()) {
        ObDirectLoadStreamBufferQueue &queue = *handle.get_queue();
        if (0 == queue.active_appends_ && queue.get_ready_chunk_count_without_lock_() > 0) {
          // 关闭观察，避免嵌套 service 读取 metadata 再次进入；无并发读写测试字段。
          observed_stream_id_ = OB_INVALID_ID;
          observed_ = true;
          active_appends_ = queue.active_appends_;
          ready_chunks_ = queue.get_ready_chunk_count_without_lock_();
          queue_state_ = queue.get_state();
          queue_error_ret_ = queue.get_first_error(queue_error_);
          // CLOSE 是另一个请求，使用独立有效 deadline，结束后恢复 APPEND 上下文。
          WorkerTimeoutResetGuard close_request_guard;
          THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10 * 1000 * 1000);
          close_ret_ = service.seal_stream(stream_id);
          ObArenaAllocator allocator("DLAppendRace");
          ObString chunk;
          while (OB_SUCCESS == (read_ret_ = queue.get_next_chunk(allocator, chunk))) {
            ++read_chunks_;
          }
          drain_ret_ = service.check_stream_reader_drain_completed(stream_id);
          success_finish_ret_ = ObMockDirectLoadStreamMetaStore::finish_stream(
              stream_id, OB_SUCCESS);
        }
      }
    }
    return ObMockDirectLoadStreamMetaStore::get_stream(stream_id, meta);
  }
  void expect_failure_before_release(const int expected_error) const
  {
    EXPECT_TRUE(observed_);
    EXPECT_EQ(0, active_appends_);
    EXPECT_GT(ready_chunks_, 0);
    EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue_state_);
    EXPECT_EQ(OB_SUCCESS, queue_error_ret_);
    EXPECT_EQ(expected_error, queue_error_);
    EXPECT_EQ(expected_error, close_ret_);
    EXPECT_EQ(expected_error, read_ret_);
    EXPECT_EQ(0, read_chunks_);
    EXPECT_EQ(expected_error, drain_ret_);
    EXPECT_EQ(OB_STATE_NOT_MATCH, success_finish_ret_);
  }
  int64_t observed_stream_id_;
  bool observed_;
  int64_t active_appends_;
  int64_t ready_chunks_;
  ObDirectLoadStreamBufferQueueState queue_state_;
  int queue_error_ret_;
  int queue_error_;
  int close_ret_;
  int read_ret_;
  int64_t read_chunks_;
  int drain_ret_;
  int success_finish_ret_;
};

class MockDirectLoadStreamAppendResolverMtlEnv
{
public:
  MockDirectLoadStreamAppendResolverMtlEnv()
    : tenant_base_(TEST_TENANT_ID),
      table_load_service_(TEST_TENANT_ID),
      mock_meta_store_(TEST_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      next_stream_id_(1000000000),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamAppendResolverMtlEnv()
  {
    destroy();
  }
  int init()
  {
    int ret = OB_SUCCESS;
    bool allocator_existed = false;
    int alloc_ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
    } else {
      allocator_existed = lib::ObMallocAllocator::get_instance()->is_tenant_allocator_exist(
          TEST_TENANT_ID);
      alloc_ret = lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
          TEST_TENANT_ID);
      if (OB_UNLIKELY(OB_SUCCESS != alloc_ret && OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(lib::ObMallocAllocator::get_instance()->set_tenant_limit(
                           TEST_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_TENANT_ID));
      } else if (OB_ISNULL(lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
                           TEST_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!self_addr_.set_ip_addr("1.1.1.1", 8888)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        created_allocator_ = !allocator_existed && OB_SUCCESS == alloc_ret;
        old_self_addr_ = GCTX.self_addr();
        GCTX.self_addr_seq_.set_addr(self_addr_);
        old_tenant_ = ObTenantEnv::get_tenant();
        tenant_base_.set<ObTableLoadService *>(&table_load_service_);
        ObTenantEnv::set_tenant(&tenant_base_);
        ObDirectLoadStreamService &stream_service =
            table_load_service_.get_direct_load_stream_service();
        if (OB_FAIL(mock_meta_store_.init())) {
          LOG_WARN("fail to init append resolver mock stream meta store", K(ret));
        } else if (OB_FAIL(stream_service.set_meta_store_for_test(mock_meta_store_))) {
          LOG_WARN("fail to inject append resolver mock stream meta store", K(ret));
        } else if (OB_FAIL(stream_service.enable_local_stream_id_generator_for_test())) {
          LOG_WARN("fail to enable append resolver test stream id generator", K(ret));
        } else if (OB_FAIL(stream_service.enable_injected_queue_topology_for_test())) {
          LOG_WARN("fail to enable append resolver test stream topology injection", K(ret));
        } else if (OB_FAIL(stream_service.get_buffer_queue_manager()
                               .set_mock_queue_store_factory_for_test(
                                   get_direct_load_stream_mock_store_factory()))) {
          LOG_WARN("fail to inject append resolver mock queue store factory", K(ret));
        } else if (OB_FAIL(table_load_service_.init())) {
          LOG_WARN("fail to init append resolver table load service", K(ret));
        }
        if (OB_FAIL(ret)) {
          table_load_service_.destroy();
          mock_meta_store_.reset();
          ObTenantEnv::set_tenant(old_tenant_);
          GCTX.self_addr_seq_.set_addr(old_self_addr_);
          old_tenant_ = nullptr;
        } else {
          is_inited_ = true;
        }
      }
      if (OB_FAIL(ret) && created_allocator_) {
        lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(
            TEST_TENANT_ID);
        created_allocator_ = false;
      }
    }
    return ret;
  }
  int create_stream(const ObDirectLoadStreamCreateParam &param,
                    const bool create_local_queue,
                    int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObIDirectLoadStreamMetaStore *meta_store = nullptr;
    ObDirectLoadStreamMetaCreateOutcome create_outcome =
        DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
    ObDirectLoadStreamBufferQueueHandle queue_handle;
    ObDirectLoadStreamService &stream_service =
        table_load_service_.get_direct_load_stream_service();
    stream_id = OB_INVALID_ID;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(TEST_TENANT_ID != param.tenant_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("append test stream tenant mismatch", K(ret), K(param.tenant_id_),
               K(TEST_TENANT_ID));
    } else if (OB_FAIL(stream_service.get_meta_store(meta_store))) {
      LOG_WARN("fail to get append test stream meta store", K(ret));
    } else if (OB_ISNULL(meta_store)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("append test stream meta store is null", K(ret));
    } else {
      stream_id = ++next_stream_id_;
      const ObDirectLoadStreamBufferQueueKey queue_key(stream_id, 0);
      if (create_local_queue
          && OB_FAIL(stream_service.get_buffer_queue_manager().create_queue(
              queue_key, queue_handle))) {
        LOG_WARN("fail to create append test local queue", K(ret), K(queue_key));
      } else if (OB_FAIL(meta_store->create_stream(
                     stream_id, param, create_outcome))) {
        const int create_meta_ret = ret;
        queue_handle.reset();
        if (create_local_queue
            && OB_SUCCESS != (tmp_ret = stream_service.get_buffer_queue_manager().cleanup_queue(
                queue_key))) {
          LOG_WARN("fail to cleanup append test local queue", K(tmp_ret), K(queue_key));
        }
        ret = create_meta_ret;
        LOG_WARN("fail to create append test stream metadata", K(ret), K(stream_id), K(param));
      } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_META_CREATED != create_outcome)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected append test stream metadata create outcome",
                 K(ret), K(stream_id), K(create_outcome));
      }
    }
    if (OB_FAIL(ret)) {
      stream_id = OB_INVALID_ID;
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      table_load_service_.destroy();
      mock_meta_store_.reset();
      ObTenantEnv::set_tenant(old_tenant_);
      GCTX.self_addr_seq_.set_addr(old_self_addr_);
      old_tenant_ = nullptr;
      is_inited_ = false;
    }
    if (created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(
          TEST_TENANT_ID);
      created_allocator_ = false;
    }
  }

private:
  ObTenantBase tenant_base_;
  ObTableLoadService table_load_service_;
  AppendReleaseRaceMetaStore mock_meta_store_;
  ObTenantBase *old_tenant_;
  ObAddr old_self_addr_;
  ObAddr self_addr_;
  bool created_allocator_;
  int64_t next_stream_id_;
  bool is_inited_;
};
} // namespace

class TestDirectLoadStreamAppendResolver : public test::TestSqlUtils, public ::testing::Test
{
public:
  TestDirectLoadStreamAppendResolver()
  {
  }
  virtual ~TestDirectLoadStreamAppendResolver() {}
  virtual void SetUp()
  {
    lib::set_compat_mode(lib::Worker::CompatMode::MYSQL);
    schema_file_path_[0] = 0;
    init();
    ASSERT_NO_FATAL_FAILURE(prepare_user_tenant(*this));
    ASSERT_EQ(OB_SUCCESS, mtl_env_.init());
    ASSERT_NO_FATAL_FAILURE(prepare_test_schema(*this));
  }
  virtual void TearDown()
  {
    mtl_env_.destroy();
    EXPECT_EQ(OB_SUCCESS,
              omt::ObTenantTimezoneMgr::get_instance().del_tenant_timezone(
                  TEST_TENANT_ID));
    destroy();
  }

protected:
  int get_target_table_schema(const char *table_name, const ObTableSchema *&table_schema)
  {
    return schema_guard_.get_table_schema(session_info_.get_effective_tenant_id(),
                                          ObString::make_string("s4test"),
                                          ObString::make_string(table_name),
                                          false,
                                          table_schema);
  }
  int add_current_user_schema_(const ObPrivSet priv_set,
                               const uint64_t role_id = OB_INVALID_ID)
  {
    int ret = OB_SUCCESS;
    ObUserInfo user_info;
    user_info.set_tenant_id(session_info_.get_priv_tenant_id());
    user_info.set_user_id(session_info_.get_priv_user_id());
    user_info.set_schema_version(schema_version_);
    user_info.set_priv_set(priv_set);
    if (OB_INVALID_ID != role_id && OB_FAIL(user_info.add_role_id(role_id))) {
      LOG_WARN("failed to grant resolver test role", K(ret), K(role_id));
    } else if (OB_FAIL(user_info.set_user_name(session_info_.get_user_name()))) {
      LOG_WARN("failed to set resolver test user name", K(ret));
    } else if (OB_FAIL(user_info.set_host(session_info_.get_host_name()))) {
      LOG_WARN("failed to set resolver test user host", K(ret));
    } else if (OB_FAIL(schema_service_->add_user_schema(user_info, schema_version_++))) {
      LOG_WARN("failed to add resolver test user schema", K(ret), K(priv_set));
    }
    return ret;
  }
  int add_role_schema_(const uint64_t role_id, const char *role_name)
  {
    int ret = OB_SUCCESS;
    ObUserInfo role_info;
    role_info.set_tenant_id(session_info_.get_priv_tenant_id());
    role_info.set_user_id(role_id);
    role_info.set_schema_version(schema_version_);
    role_info.set_priv_set(0);
    role_info.set_type(OB_ROLE);
    if (OB_FAIL(role_info.set_user_name(ObString::make_string(role_name)))) {
      LOG_WARN("failed to set resolver test role name", K(ret), K(role_id));
    } else if (OB_FAIL(role_info.set_host(ObString::make_string("%")))) {
      LOG_WARN("failed to set resolver test role host", K(ret), K(role_id));
    } else if (OB_FAIL(schema_service_->add_user_schema(role_info, schema_version_++))) {
      LOG_WARN("failed to add resolver test role schema", K(ret), K(role_id));
    }
    return ret;
  }
  int create_stream_for_table(const ObTableSchema &table_schema,
                              int64_t &stream_id,
                              const uint64_t user_id = OB_INVALID_ID,
                              const int64_t schema_version_delta = 0)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamCreateParam param;
    ObDirectLoadStreamQueueEntry entry;
    stream_id = OB_INVALID_ID;
    param.tenant_id_ = session_info_.get_effective_tenant_id();
    param.user_id_ = OB_INVALID_ID == user_id ? session_info_.get_priv_user_id() : user_id;
    param.table_id_ = table_schema.get_table_id();
    param.target_schema_version_ = table_schema.get_schema_version() + schema_version_delta;
    param.expire_ts_ = 6001;
    if (OB_UNLIKELY(!GCTX.self_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      entry.server_addr_ = GCTX.self_addr();
      entry.queue_count_ = 1;
      if (OB_FAIL(param.queue_entries_.push_back(entry))) {
        LOG_WARN("fail to push stream queue entry", KR(ret));
      } else if (OB_FAIL(mtl_env_.create_stream(param, true, stream_id))) {
        LOG_WARN("fail to create append resolver test stream", KR(ret), K(param));
      }
    }
    return ret;
  }
  int resolve_sql(const char *sql, ObStmt *&stmt)
  {
    int ret = OB_SUCCESS;
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    ObParser parser(allocator_, SMO_DEFAULT);
    ParseResult parse_result;
    ObString query = ObString::make_string(sql);
    stmt = nullptr;
    if (OB_FAIL(parser.parse(query, parse_result))) {
      LOG_WARN("fail to parse sql", KR(ret), K(query));
    } else if (OB_ISNULL(parse_result.result_tree_) || OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", KR(ret), K(query));
    } else {
      ParseNode *root = parse_result.result_tree_->children_[0];
      ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
      ParamStore &param_store = pctx->get_param_store_for_update();
      ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                            ObWrapperAllocator(&allocator_));
      SqlInfo not_param_info;
      bool is_transform_outline = false;
      param_store.reset();
      if (T_SELECT == root->type_ || T_INSERT == root->type_ || T_UPDATE == root->type_ || T_DELETE == root->type_) {
        if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator_,
                                                                 session_info_,
                                                                 nullptr,
                                                                 parse_result.result_tree_,
                                                                 not_param_info,
                                                                 param_store,
                                                                 nullptr,
                                                                 fixed_param_store,
                                                                 is_transform_outline))) {
          LOG_WARN("fail to parameterize sql", KR(ret), K(query));
        } else {
          pctx->set_original_param_cnt(param_store.count());
        }
      }
      if (OB_SUCC(ret)) {
        ObSchemaChecker schema_checker;
        ObResolverParams resolver_ctx;
        if (OB_FAIL(schema_checker.init(schema_guard_))) {
          LOG_WARN("fail to init schema checker", KR(ret));
        } else {
          resolver_ctx.allocator_ = &allocator_;
          resolver_ctx.schema_checker_ = &schema_checker;
          resolver_ctx.session_info_ = &session_info_;
          resolver_ctx.param_list_ = &param_store;
          resolver_ctx.database_id_ = 1024;
          resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
          resolver_ctx.expr_factory_ = &expr_factory_;
          resolver_ctx.stmt_factory_ = &stmt_factory_;
          resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
          resolver_ctx.query_ctx_->set_questionmark_count(param_store.count());
          ObResolver resolver(resolver_ctx);
          if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *root, stmt))) {
            LOG_WARN("fail to resolve sql", KR(ret), K(query));
          }
        }
      }
    }
    parser.free_result(parse_result);
    return ret;
  }
  int resolve_append_sql(const int64_t stream_id, ObStmt *&stmt)
  {
    char sql[256];
    MEMSET(sql, 0, sizeof(sql));
    snprintf(sql, sizeof(sql), "insert into direct_load_stream %ld (c1, c2) values (1, 2)", stream_id);
    return resolve_sql(sql, stmt);
  }
  int get_local_queue(const int64_t stream_id, ObDirectLoadStreamBufferQueueHandle &queue_handle)
  {
    int ret = OB_SUCCESS;
    ObTableLoadService *table_load_service = nullptr;
    queue_handle.reset();
    if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
      ret = OB_ERR_SYS;
    } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .get_queue(ObDirectLoadStreamBufferQueueKey(stream_id, 0), queue_handle))) {
      LOG_WARN("fail to get resolver test local queue", K(ret), K(stream_id));
    }
    return ret;
  }
  void expect_open_stream_with_empty_local_queue(const int64_t stream_id)
  {
    ObDirectLoadStreamMeta meta;
    ObDirectLoadStreamBufferQueueHandle queue_handle;
    ASSERT_EQ(OB_SUCCESS,
              ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
    EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
    EXPECT_EQ(OB_SUCCESS, meta.result_code_);
    ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
    ASSERT_NE(nullptr, queue_handle.get_queue());
    EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN,
              queue_handle.get_queue()->get_state());
    EXPECT_EQ(0, queue_handle.get_queue()->active_appends_);
    EXPECT_EQ(0, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
  }

private:
  MockDirectLoadStreamAppendResolverMtlEnv mtl_env_;
};

TEST_F(TestDirectLoadStreamAppendResolver, resolve_success_marks_stream_append)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
  ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt *>(stmt);
  ASSERT_NE(nullptr, insert_stmt);
  const TableItem *table_item = insert_stmt->get_table_item(0);
  ASSERT_NE(nullptr, table_item);
  EXPECT_TRUE(insert_stmt->is_direct_load_stream_append());
  EXPECT_EQ(stream_id, insert_stmt->get_direct_load_stream_id());
  EXPECT_EQ(table_schema->get_schema_version(), insert_stmt->get_direct_load_stream_target_schema_version());
  EXPECT_EQ(table_schema->get_table_id(), insert_stmt->get_insert_table_info().ref_table_id_);
  EXPECT_EQ(table_schema->get_table_id(), table_item->ref_id_);
  ASSERT_NE(nullptr, insert_stmt->get_query_ctx());
  EXPECT_EQ(OB_USE_PLAN_CACHE_NONE,
            insert_stmt->get_query_ctx()->get_global_hint().plan_cache_policy_);
}

TEST_F(TestDirectLoadStreamAppendResolver, ordinary_insert_target_stays_non_stream)
{
  ObStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            resolve_sql("insert into s4test.t1 (c1, c2) values (1, 2)", stmt));
  ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt *>(stmt);
  ASSERT_NE(nullptr, insert_stmt);
  EXPECT_FALSE(insert_stmt->is_direct_load_stream_append());
  EXPECT_EQ(OB_INVALID_ID, insert_stmt->get_direct_load_stream_id());
  EXPECT_EQ(OB_INVALID_VERSION,
            insert_stmt->get_direct_load_stream_target_schema_version());
}

TEST_F(TestDirectLoadStreamAppendResolver, reject_owner_and_schema_mismatch)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t owner_mismatch_stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);

  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema,
                                                stream_id,
                                                session_info_.get_priv_user_id() + 1));
  owner_mismatch_stream_id = stream_id;
  EXPECT_EQ(OB_ERR_NO_PRIVILEGE, resolve_append_sql(stream_id, stmt));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);

  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id, OB_INVALID_ID, 1));
  EXPECT_EQ(OB_SCHEMA_ERROR, resolve_append_sql(stream_id, stmt));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_SCHEMA_ERROR, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(owner_mismatch_stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

TEST_F(TestDirectLoadStreamAppendResolver, terminal_status_precedes_schema_and_privilege_checks)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t sealed_stream_id = OB_INVALID_ID;
  int64_t finished_stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, sealed_stream_id));
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, finished_stream_id));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamService::seal_direct_load_stream(sealed_stream_id));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::finish_direct_load_stream(
                finished_stream_id, OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, drop_table_schema(*table_schema));

  EXPECT_EQ(OB_STATE_NOT_MATCH, resolve_append_sql(sealed_stream_id, stmt));
  EXPECT_EQ(OB_STATE_NOT_MATCH, resolve_append_sql(finished_stream_id, stmt));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(sealed_stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(finished_stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_TIMEOUT, meta.result_code_);
}

TEST_F(TestDirectLoadStreamAppendResolver, column_insert_privilege_matches_resolved_target_columns)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                               ObString::make_string("%"),
                                               TEST_STREAM_USER_ID));
  session_info_.set_priv_user_id(TEST_STREAM_USER_ID);
  session_info_.set_user_priv_set(0);
  ASSERT_EQ(OB_SUCCESS, add_current_user_schema_(0));
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS,
            add_column_insert_priv(*schema_service_, TEST_TENANT_ID, TEST_STREAM_USER_ID,
                                   "s4test", "t1", "c1", 10001));
  EXPECT_EQ(OB_ERR_NO_COLUMN_PRIVILEGE, resolve_append_sql(stream_id, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);

  ASSERT_EQ(OB_SUCCESS,
            add_column_insert_priv(*schema_service_, TEST_TENANT_ID, TEST_STREAM_USER_ID,
                                   "s4test", "t1", "c2", 10002));
  EXPECT_EQ(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
}

TEST_F(TestDirectLoadStreamAppendResolver, reject_without_target_insert_privilege)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                               ObString::make_string("%"),
                                               TEST_STREAM_USER_ID));
  session_info_.set_priv_user_id(TEST_STREAM_USER_ID);
  session_info_.set_user_priv_set(0);
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  EXPECT_NE(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);
}

// APPEND 仅在角色启用后接受角色提供的目标表 INSERT 权限。
TEST_F(TestDirectLoadStreamAppendResolver, enabled_role_supplies_target_insert_privilege)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                               ObString::make_string("%"),
                                               TEST_STREAM_USER_ID));
  session_info_.set_priv_user_id(TEST_STREAM_USER_ID);
  session_info_.set_user_priv_set(0);
  ASSERT_EQ(OB_SUCCESS, add_current_user_schema_(0, TEST_STREAM_ROLE_ID));
  ASSERT_EQ(OB_SUCCESS, add_role_schema_(TEST_STREAM_ROLE_ID, "stream_role"));
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS,
            add_table_insert_priv(*schema_service_,
                                  TEST_TENANT_ID,
                                  TEST_STREAM_ROLE_ID,
                                  "s4test",
                                  "t1"));
  EXPECT_NE(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);
  ASSERT_EQ(OB_SUCCESS, session_info_.get_enable_role_array().push_back(TEST_STREAM_ROLE_ID));
  EXPECT_EQ(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
}

TEST_F(TestDirectLoadStreamAppendResolver,
       request_syntax_and_shape_errors_keep_stream_open_for_retry)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObStmt *stmt = nullptr;
  char sql[256];
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));

  MEMSET(sql, 0, sizeof(sql));
  snprintf(sql, sizeof(sql), "insert into direct_load_stream %ld values (1,", stream_id);
  EXPECT_NE(OB_SUCCESS, resolve_sql(sql, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);

  MEMSET(sql, 0, sizeof(sql));
  snprintf(sql, sizeof(sql), "insert into direct_load_stream %ld select 1, 2", stream_id);
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve_sql(sql, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);

  MEMSET(sql, 0, sizeof(sql));
  snprintf(sql, sizeof(sql), "insert ignore into direct_load_stream %ld values (1, 2)", stream_id);
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve_sql(sql, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);

  MEMSET(sql, 0, sizeof(sql));
  snprintf(sql, sizeof(sql),
           "insert into direct_load_stream %ld (missing_column, c2) values (1, 2)",
           stream_id);
  EXPECT_NE(OB_SUCCESS, resolve_sql(sql, stmt));
  expect_open_stream_with_empty_local_queue(stream_id);
  EXPECT_EQ(OB_SUCCESS, resolve_append_sql(stream_id, stmt));
}

TEST_F(TestDirectLoadStreamAppendResolver, resolve_two_streams_keeps_target_metadata)
{
  const ObTableSchema *first_table = nullptr;
  const ObTableSchema *second_table = nullptr;
  int64_t first_stream_id = OB_INVALID_ID;
  int64_t second_stream_id = OB_INVALID_ID;
  ObStmt *first_stmt = nullptr;
  ObStmt *second_stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", first_table));
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t4", second_table));
  ASSERT_NE(nullptr, first_table);
  ASSERT_NE(nullptr, second_table);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*first_table, first_stream_id));
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*second_table, second_stream_id));

  ASSERT_EQ(OB_SUCCESS, resolve_append_sql(first_stream_id, first_stmt));
  ASSERT_EQ(OB_SUCCESS, resolve_append_sql(second_stream_id, second_stmt));
  ObInsertStmt *first_insert = dynamic_cast<ObInsertStmt *>(first_stmt);
  ObInsertStmt *second_insert = dynamic_cast<ObInsertStmt *>(second_stmt);
  ASSERT_NE(nullptr, first_insert);
  ASSERT_NE(nullptr, second_insert);
  const TableItem *first_item = first_insert->get_table_item(0);
  const TableItem *second_item = second_insert->get_table_item(0);
  ASSERT_NE(nullptr, first_item);
  ASSERT_NE(nullptr, second_item);
  EXPECT_EQ(first_stream_id, first_insert->get_direct_load_stream_id());
  EXPECT_EQ(second_stream_id, second_insert->get_direct_load_stream_id());
  EXPECT_EQ(first_table->get_table_id(), first_insert->get_insert_table_info().ref_table_id_);
  EXPECT_EQ(second_table->get_table_id(), second_insert->get_insert_table_info().ref_table_id_);
  EXPECT_EQ(first_table->get_table_id(), first_item->ref_id_);
  EXPECT_EQ(second_table->get_table_id(), second_item->ref_id_);
  EXPECT_NE(first_insert->get_insert_table_info().ref_table_id_,
            second_insert->get_insert_table_info().ref_table_id_);
  ASSERT_NE(nullptr, first_insert->get_query_ctx());
  ASSERT_NE(nullptr, second_insert->get_query_ctx());
  EXPECT_EQ(OB_USE_PLAN_CACHE_NONE,
            first_insert->get_query_ctx()->get_global_hint().plan_cache_policy_);
  EXPECT_EQ(OB_USE_PLAN_CACHE_NONE,
            second_insert->get_query_ctx()->get_global_hint().plan_cache_policy_);
}


class TestDirectLoadStreamAppendOutputFrame
{
public:
  TestDirectLoadStreamAppendOutputFrame()
    : allocator_("DLSAppendE2E"),
      exec_ctx_(allocator_),
      eval_ctx_(exec_ctx_),
      exprs_()
  {
  }
  int init(const int64_t col_count)
  {
    int ret = OB_SUCCESS;
    char **frames = nullptr;
    char *frame = nullptr;
    const int64_t res_buf_len = 128;
    const int64_t frame_size = col_count * (sizeof(ObDatum) + sizeof(ObEvalInfo) + res_buf_len);
    int64_t pos = 0;
    if (OB_FAIL(exprs_.reserve(col_count))) {
      LOG_WARN("fail to reserve append e2e output exprs", K(ret), K(col_count));
    } else if (OB_ISNULL(frames = static_cast<char **>(allocator_.alloc(sizeof(char *))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(frame = static_cast<char *>(allocator_.alloc(frame_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(frame, 0, frame_size);
      frames[0] = frame;
      eval_ctx_.frames_ = frames;
      eval_ctx_.set_max_batch_size(1);
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        ObExpr *expr = nullptr;
        if (OB_ISNULL(expr = new (allocator_.alloc(sizeof(ObExpr))) ObExpr())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          expr->frame_idx_ = 0;
          expr->datum_off_ = pos;
          pos += sizeof(ObDatum);
          expr->eval_info_off_ = pos;
          pos += sizeof(ObEvalInfo);
          expr->res_buf_off_ = pos;
          expr->res_buf_len_ = res_buf_len;
          pos += res_buf_len;
          expr->batch_result_ = false;
          expr->batch_idx_mask_ = 0;
          expr->locate_expr_datum(eval_ctx_, 0).set_null();
          expr->get_eval_info(eval_ctx_).set_evaluated(true);
          expr->get_eval_info(eval_ctx_).set_projected(true);
          if (OB_FAIL(exprs_.push_back(expr))) {
            LOG_WARN("fail to add append e2e output expr", K(ret));
          }
        }
      }
    }
    return ret;
  }
  ObIArray<ObExpr *> &exprs() { return exprs_; }
  ObEvalCtx &eval_ctx() { return eval_ctx_; }
  int64_t get_int(const int64_t col_idx) const
  {
    return exprs_.at(col_idx)->locate_expr_datum(const_cast<ObEvalCtx &>(eval_ctx_), 0).get_int();
  }
private:
  ObArenaAllocator allocator_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObSEArray<ObExpr *, 4> exprs_;
};

bool contains_phy_operator_type(const ObOpSpec &spec, const ObPhyOperatorType type)
{
  bool contains = spec.get_type() == type;
  for (uint32_t i = 0; !contains && i < spec.get_child_cnt(); ++i) {
    const ObOpSpec *child = spec.get_child(i);
    contains = nullptr != child && contains_phy_operator_type(*child, type);
  }
  return contains;
}

class TestDirectLoadStreamAppendExecutor : public test::TestOptimizerUtils
{
public:
  TestDirectLoadStreamAppendExecutor()
  {
  }
  virtual ~TestDirectLoadStreamAppendExecutor() {}
  virtual void SetUp()
  {
    lib::set_compat_mode(lib::Worker::CompatMode::MYSQL);
    schema_file_path_[0] = 0;
    test::TestOptimizerUtils::SetUp();
    ASSERT_NO_FATAL_FAILURE(prepare_user_tenant(*this));
    ASSERT_EQ(OB_SUCCESS, mtl_env_.init());
    ASSERT_NO_FATAL_FAILURE(prepare_test_schema(*this));
    ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                                 ObString::make_string("%"),
                                                 TEST_STREAM_USER_ID));
    session_info_.set_priv_user_id(TEST_STREAM_USER_ID);
    session_info_.set_user_priv_set(OB_PRIV_ALL);
    ASSERT_EQ(OB_SUCCESS, set_current_user_priv_set_(OB_PRIV_ALL));
    ASSERT_EQ(OB_SUCCESS, refresh_query_schema_guard_());
  }
  virtual void TearDown()
  {
    mtl_env_.destroy();
    EXPECT_EQ(OB_SUCCESS,
              omt::ObTenantTimezoneMgr::get_instance().del_tenant_timezone(
                  TEST_TENANT_ID));
    test::TestSqlUtils::destroy();
  }

protected:
  int set_current_user_priv_set_(const ObPrivSet priv_set)
  {
    int ret = OB_SUCCESS;
    ObUserInfo user_info;
    user_info.set_tenant_id(session_info_.get_priv_tenant_id());
    user_info.set_user_id(session_info_.get_priv_user_id());
    user_info.set_schema_version(schema_version_);
    user_info.set_priv_set(priv_set);
    if (OB_FAIL(user_info.set_user_name(session_info_.get_user_name()))) {
      LOG_WARN("fail to set append e2e user name", K(ret));
    } else if (OB_FAIL(user_info.set_host(session_info_.get_host_name()))) {
      LOG_WARN("fail to set append e2e user host", K(ret));
    } else if (OB_FAIL(schema_service_->add_user_schema(user_info, schema_version_++))) {
      LOG_WARN("fail to update append e2e user privilege", K(ret), K(priv_set));
    }
    return ret;
  }
  int apply_before_run_action_(const int64_t stream_id, const AppendBeforeRunAction action)
  {
    int ret = OB_SUCCESS;
    if (AppendBeforeRunAction::NONE == action) {
    } else if (AppendBeforeRunAction::REVOKE_INSERT_PRIV == action) {
      ret = set_current_user_priv_set_(0);
    } else if (AppendBeforeRunAction::DROP_TARGET_TABLE == action) {
      ObDirectLoadStreamMeta meta;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta))) {
        LOG_WARN("fail to get stream before append e2e schema change", K(ret), K(stream_id));
      } else if (OB_FAIL(schema_guard_.get_table_schema(meta.tenant_id_, meta.table_id_, table_schema))) {
        LOG_WARN("fail to get table before append e2e schema change", K(ret), K(meta));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
      } else if (OB_FAIL(drop_table_schema(*table_schema))) {
        LOG_WARN("fail to drop table before append e2e run", K(ret), K(meta));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }
  int set_unsupported_append_shape_for_test_(
      ObPhysicalPlan &phy_plan,
      const AppendUnsupportedShape unsupported_shape)
  {
    int ret = OB_SUCCESS;
    const ObOpSpec *root_spec = phy_plan.get_root_op_spec();
    if (AppendUnsupportedShape::NONE == unsupported_shape) {
    } else if (OB_ISNULL(root_spec)
               || OB_UNLIKELY(PHY_DIRECT_LOAD_STREAM_APPEND != root_spec->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid append root spec for unsupported shape injection", K(ret), KP(root_spec),
               "unsupported_shape", static_cast<int64_t>(unsupported_shape));
    } else {
      ObDirectLoadStreamAppendSpec &append_spec =
          static_cast<ObDirectLoadStreamAppendSpec &>(const_cast<ObOpSpec &>(*root_spec));
      const ObInsCtDef *primary_ins_ctdef = nullptr;
      if (OB_UNLIKELY(append_spec.need_trigger_fire_
                      || append_spec.need_foreign_key_check_
                      || 1 != append_spec.ins_ctdefs_.count()
                      || 1 != append_spec.ins_ctdefs_.at(0).count()
                      || OB_ISNULL(primary_ins_ctdef = append_spec.ins_ctdefs_.at(0).at(0))
                      || primary_ins_ctdef->has_instead_of_trigger_
                      || 0 != primary_ins_ctdef->fk_args_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected base append spec before unsupported shape injection", K(ret),
                 K(append_spec.need_trigger_fire_), K(append_spec.need_foreign_key_check_),
                 K(append_spec.ins_ctdefs_));
      } else if (AppendUnsupportedShape::TRIGGER == unsupported_shape) {
        append_spec.need_trigger_fire_ = true;
      } else if (AppendUnsupportedShape::FOREIGN_KEY == unsupported_shape) {
        append_spec.need_foreign_key_check_ = true;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid unsupported append shape", K(ret),
                 "unsupported_shape", static_cast<int64_t>(unsupported_shape));
      }
    }
    return ret;
  }
  int refresh_query_schema_guard_()
  {
    int ret = OB_SUCCESS;
    ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();
    if (OB_ISNULL(query_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("append e2e query ctx is null", K(ret));
    } else {
      query_ctx->sql_schema_guard_.set_schema_guard(&schema_guard_);
    }
    return ret;
  }
  int get_target_table_schema(const char *table_name, const ObTableSchema *&table_schema)
  {
    return schema_guard_.get_table_schema(session_info_.get_effective_tenant_id(),
                                          ObString::make_string("s4test"),
                                          ObString::make_string(table_name),
                                          false,
                                          table_schema);
  }
  int create_stream_for_table(const ObTableSchema &table_schema, int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamCreateParam param;
    ObDirectLoadStreamQueueEntry entry;
    stream_id = OB_INVALID_ID;
    param.tenant_id_ = session_info_.get_effective_tenant_id();
    param.user_id_ = session_info_.get_priv_user_id();
    param.table_id_ = table_schema.get_table_id();
    param.target_schema_version_ = table_schema.get_schema_version();
    param.expire_ts_ = 6001;
    if (OB_UNLIKELY(!GCTX.self_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      entry.server_addr_ = GCTX.self_addr();
      entry.queue_count_ = 1;
      if (OB_FAIL(param.queue_entries_.push_back(entry))) {
        LOG_WARN("fail to push stream queue entry", K(ret));
      } else if (OB_FAIL(mtl_env_.create_stream(param, true, stream_id))) {
        LOG_WARN("fail to create append executor test stream", K(ret), K(param));
      }
    }
    return ret;
  }
  int create_stream_metadata_for_table(
      const ObTableSchema &table_schema,
      const ObIArray<ObDirectLoadStreamQueueEntry> &queue_entries,
      int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamCreateParam param;
    stream_id = OB_INVALID_ID;
    param.tenant_id_ = session_info_.get_effective_tenant_id();
    param.user_id_ = session_info_.get_priv_user_id();
    param.table_id_ = table_schema.get_table_id();
    param.target_schema_version_ = table_schema.get_schema_version();
    param.expire_ts_ = 6001;
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_entries.count(); ++i) {
      if (OB_FAIL(param.queue_entries_.push_back(queue_entries.at(i)))) {
        LOG_WARN("fail to copy append e2e queue entry", K(ret), K(i), K(queue_entries.at(i)));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(mtl_env_.create_stream(param, false, stream_id))) {
      LOG_WARN("fail to create append executor test metadata", K(ret), K(param));
    }
    return ret;
  }
  int cleanup_local_queue(const int64_t stream_id)
  {
    int ret = OB_SUCCESS;
    ObTableLoadService *table_load_service = nullptr;
    if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("null table load service", K(ret));
    } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .cleanup_queue(ObDirectLoadStreamBufferQueueKey(stream_id, 0)))) {
      LOG_WARN("fail to cleanup append e2e local queue", K(ret), K(stream_id));
    }
    return ret;
  }
  int generate_append_logical_plan_(ObString &query,
                                    const char *query_str,
                                    ObLogPlan *&logical_plan,
                                    const bool expect_stream_append = true)
  {
    int ret = OB_SUCCESS;
    ObParser parser(allocator_, SMO_DEFAULT);
    ParseResult parse_result;
    ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
    ObStmt *basic_stmt = nullptr;
    ObDMLStmt *stmt = nullptr;
    ObPhysicalPlan *transform_phy_plan = nullptr;
    ObQueryCtx *query_ctx = nullptr;
    ObAddr addr;
    logical_plan = nullptr;
    param_store.reuse();
    THIS_WORKER.set_timeout_ts(get_usec() + 100000000);
    session_info_.set_user_session();
    if (OB_UNLIKELY(!addr.set_ip_addr("1.1.1.1", 8888))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set append e2e local addr", K(ret));
    } else if (OB_FAIL(parser.parse(query, parse_result))) {
      LOG_WARN("fail to parse append e2e sql", K(ret), K(query));
    } else if (OB_ISNULL(parse_result.result_tree_)
               || OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid append e2e parse tree", K(ret), K(query));
    } else if (OB_FAIL(test::MockCacheObjectFactory::alloc(transform_phy_plan,
                                                           session_info_.get_effective_tenant_id()))) {
      LOG_WARN("fail to alloc append e2e transform physical plan", K(ret));
    } else {
      expr_factory_.destory();
      stmt_factory_.destory();
      if (OB_ISNULL(query_ctx = stmt_factory_.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e query ctx is null after reset", K(ret));
      } else {
        query_ctx->sql_schema_guard_.set_schema_guard(&schema_guard_);
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaChecker schema_checker;
      ObResolverParams resolver_ctx;
      ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
      if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_))) {
        LOG_WARN("fail to init append e2e schema checker", K(ret));
      } else {
        resolver_ctx.allocator_ = &allocator_;
        resolver_ctx.schema_checker_ = &schema_checker;
        resolver_ctx.session_info_ = &session_info_;
        resolver_ctx.database_id_ = 1024;
        resolver_ctx.param_list_ = &param_store;
        resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
        resolver_ctx.expr_factory_ = &expr_factory_;
        resolver_ctx.stmt_factory_ = &stmt_factory_;
        resolver_ctx.query_ctx_ = query_ctx;
        resolver_ctx.query_ctx_->set_questionmark_count(param_store.count());
        pctx->set_original_param_cnt(pctx->get_param_store().count());
        if (OB_FAIL(pctx->init_datum_param_store())) {
          LOG_WARN("fail to init append e2e datum param store", K(ret));
        } else {
          ObResolver resolver(resolver_ctx);
          if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                       *parse_result.result_tree_->children_[0],
                                       basic_stmt))) {
            LOG_WARN("failed to resolve append e2e sql", K(ret), K(query));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(stmt = dynamic_cast<ObDMLStmt *>(basic_stmt))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e stmt is not dml", K(ret), KP(basic_stmt), K(query));
      } else {
        stmt->get_query_ctx()->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
        exec_ctx_.get_sql_ctx()->cur_stmt_ = stmt;
      }
    }
    if (OB_SUCC(ret)) {
      ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt *>(stmt);
      if (OB_ISNULL(insert_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e stmt is not insert", K(ret), KPC(stmt), K(query));
      } else if (OB_UNLIKELY(expect_stream_append != insert_stmt->is_direct_load_stream_append())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert e2e stream append marker is unexpected", K(ret), K(expect_stream_append),
                 "is_stream_append", insert_stmt->is_direct_load_stream_append(), K(query));
      } else if (expect_stream_append
                 && OB_UNLIKELY(OB_USE_PLAN_CACHE_NONE
                             != insert_stmt->get_query_ctx()->get_global_hint().plan_cache_policy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e plan cache policy is unexpected", K(ret),
                 K(insert_stmt->get_query_ctx()->get_global_hint().plan_cache_policy_), K(query));
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaChecker transform_schema_checker;
      if (OB_FAIL(transform_schema_checker.init(query_ctx->sql_schema_guard_))) {
        LOG_WARN("fail to init append e2e transform schema checker", K(ret));
      } else {
        ObTransformerCtx ctx;
        ctx.allocator_ = &allocator_;
        ctx.session_info_ = &session_info_;
        ctx.schema_checker_ = &transform_schema_checker;
        ctx.exec_ctx_ = &exec_ctx_;
        ctx.expr_factory_ = &expr_factory_;
        ctx.stmt_factory_ = &stmt_factory_;
        ctx.sql_schema_guard_ = &query_ctx->sql_schema_guard_;
        ctx.self_addr_ = &addr;
        ctx.merged_version_ = OB_MERGED_VERSION_INIT;
        ctx.phy_plan_ = transform_phy_plan;
        ObTransformerImpl transformer(&ctx);
        ObDMLStmt *sql_stmt = stmt;
        if (stmt->is_explain_stmt()) {
          sql_stmt = static_cast<ObExplainStmt *>(stmt)->get_explain_query_stmt();
        }
        if (OB_FAIL(transformer.transform(sql_stmt))) {
          LOG_WARN("failed to transform append e2e stmt", K(ret), K(query));
        } else {
          stmt = sql_stmt;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObOptimizerContext *ctx_ptr =
          static_cast<ObOptimizerContext *>(allocator_.alloc(sizeof(ObOptimizerContext)));
      if (OB_ISNULL(ctx_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc append e2e optimizer context", K(ret));
      } else {
        exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
        optctx_ = new (ctx_ptr) ObOptimizerContext(&session_info_,
            &exec_ctx_,
            &query_ctx->sql_schema_guard_,
            nullptr,
            static_cast<ObIAllocator &>(allocator_),
            &param_store,
            addr,
            nullptr,
            stmt->get_query_ctx()->get_global_hint(),
            expr_factory_,
            stmt,
            false,
            query_ctx);
        optctx_->set_opt_stat_manager(&opt_stat_manager_);
        optctx_->disable_batch_rpc();
        optctx_->set_local_server_addr("1.1.1.1", 8888);
        optctx_->set_use_default_stat();
        ObTableLocation table_location;
        if (OB_FAIL(optctx_->get_table_location_list().push_back(table_location))) {
          LOG_WARN("fail to add append e2e table location", K(ret));
        } else {
          ObOptimizer optimizer(*optctx_);
          if (OB_FAIL(optimizer.optimize(*stmt, logical_plan))) {
            LOG_WARN("failed to optimize append e2e sql", K(ret), K(query_str), KPC(stmt));
          }
        }
      }
    }
    return ret;
  }
  int execute_append_sql(const int64_t stream_id,
                         const char *column_list,
                         const char *values_clause,
                         int64_t &affected_rows,
                         const AppendExecutionMode execution_mode = AppendExecutionMode::NORMAL,
                         AppendRescanResult *rescan_result = nullptr,
                         const AppendBeforeRunAction before_run_action =
                             AppendBeforeRunAction::NONE,
                         const AppendUnsupportedShape unsupported_shape =
                             AppendUnsupportedShape::NONE,
                         AppendShapeGateResult *shape_gate_result = nullptr)
  {
    int ret = OB_SUCCESS;
    char sql[256];
    ObLogPlan *log_plan = nullptr;
    ObPhysicalPlan *phy_plan = nullptr;
    const bool is_rescan_mode = AppendExecutionMode::RESCAN_BEFORE_QUEUE_TOUCH == execution_mode
                                || AppendExecutionMode::PUBLISH_MOCK_CHUNK_THEN_RESCAN == execution_mode;
    affected_rows = 0;
    if (nullptr != rescan_result) {
      rescan_result->reset();
    }
    if (nullptr != shape_gate_result) {
      shape_gate_result->reset();
    }
    MEMSET(sql, 0, sizeof(sql));
    snprintf(sql, sizeof(sql),
             "insert into direct_load_stream %ld %s values %s", stream_id, column_list, values_clause);
    ObString query = ObString::make_string(sql);
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    if (OB_UNLIKELY(is_rescan_mode && nullptr == rescan_result)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("append rescan result is null", K(ret), K(stream_id),
               "execution_mode", static_cast<int64_t>(execution_mode));
    } else if (OB_FAIL(refresh_query_schema_guard_())) {
      LOG_WARN("fail to refresh append e2e schema guard", K(ret));
    } else if (OB_FAIL(generate_append_logical_plan_(query, sql, log_plan))) {
      LOG_WARN("fail to generate append e2e logical plan", K(ret), K(query));
    } else if (OB_ISNULL(log_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("append e2e logical plan is null", K(ret), K(query));
    } else if (OB_FAIL(test::MockCacheObjectFactory::alloc(phy_plan,
                                                           session_info_.get_effective_tenant_id()))) {
      LOG_WARN("fail to alloc append e2e physical plan", K(ret));
    } else {
      DatumParamStore &datum_param_store = exec_ctx_.get_physical_plan_ctx()->get_datum_param_store();
      ObCodeGenerator code_gen(false, CLUSTER_VERSION_1500, &datum_param_store);
      if (OB_FAIL(code_gen.generate(*log_plan, *phy_plan))) {
        LOG_WARN("fail to generate append e2e physical plan", K(ret));
      } else if (OB_ISNULL(phy_plan->get_root_op_spec())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e root spec is null", K(ret));
      } else if (OB_UNLIKELY(PHY_DIRECT_LOAD_STREAM_APPEND != phy_plan->get_root_op_spec()->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("append e2e root spec type is unexpected", K(ret), K(phy_plan->get_root_op_spec()->type_));
      } else if (OB_FAIL(phy_plan->set_table_locations(
                     log_plan->get_optimizer_context().get_table_partition_info(), schema_guard_))) {
        LOG_WARN("fail to set append e2e table locations", K(ret));
      } else if (OB_FAIL(set_unsupported_append_shape_for_test_(*phy_plan, unsupported_shape))) {
        LOG_WARN("fail to set unsupported append shape for test", K(ret),
                 "unsupported_shape", static_cast<int64_t>(unsupported_shape));
      } else if (OB_FAIL(apply_before_run_action_(stream_id, before_run_action))) {
        LOG_WARN("fail to apply append e2e before-run action", K(ret), K(stream_id),
                 "before_run_action", static_cast<int64_t>(before_run_action));
      } else {
        if (OB_FAIL(run_physical_plan_(*phy_plan,
                                       affected_rows,
                                       execution_mode,
                                       rescan_result,
                                       shape_gate_result))) {
          LOG_WARN("fail to execute append e2e physical plan", K(ret));
        }
      }
    }
    if (nullptr != phy_plan) {
      test::MockCacheObjectFactory::free(phy_plan);
      phy_plan = nullptr;
    }
    return ret;
  }
  int execute_append_sql(const int64_t stream_id, int64_t &affected_rows)
  {
    return execute_append_sql(stream_id, "(c1, c2)", "(7, 11), (9, 13)", affected_rows);
  }
  int generate_non_stream_insert_plan_info(const char *sql,
                                           ObPhyOperatorType &root_type,
                                           bool &has_stream_append,
                                           bool &has_table_direct_insert,
                                           bool &enable_append)
  {
    int ret = OB_SUCCESS;
    ObLogPlan *log_plan = nullptr;
    ObPhysicalPlan *phy_plan = nullptr;
    root_type = PHY_INVALID;
    has_stream_append = false;
    has_table_direct_insert = false;
    enable_append = false;
    ObString query = ObString::make_string(sql);
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    if (OB_FAIL(refresh_query_schema_guard_())) {
      LOG_WARN("fail to refresh non-stream insert schema guard", K(ret));
    } else if (OB_FAIL(generate_append_logical_plan_(query, sql, log_plan, false))) {
      LOG_WARN("fail to generate non-stream insert logical plan", K(ret), K(query));
    } else if (OB_ISNULL(log_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-stream insert logical plan is null", K(ret), K(query));
    } else if (OB_FAIL(test::MockCacheObjectFactory::alloc(
                           phy_plan, session_info_.get_effective_tenant_id()))) {
      LOG_WARN("fail to alloc non-stream insert physical plan", K(ret));
    } else {
      DatumParamStore &datum_param_store =
          exec_ctx_.get_physical_plan_ctx()->get_datum_param_store();
      ObCodeGenerator code_gen(false, CLUSTER_VERSION_1500, &datum_param_store);
      if (OB_FAIL(code_gen.generate(*log_plan, *phy_plan))) {
        LOG_WARN("fail to generate non-stream insert physical plan", K(ret), K(query));
      } else if (OB_ISNULL(phy_plan->get_root_op_spec())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-stream insert root spec is null", K(ret), K(query));
      } else {
        const ObOpSpec &root_spec = *phy_plan->get_root_op_spec();
        root_type = root_spec.get_type();
        has_stream_append = contains_phy_operator_type(
            root_spec, PHY_DIRECT_LOAD_STREAM_APPEND);
        has_table_direct_insert = contains_phy_operator_type(
            root_spec, PHY_TABLE_DIRECT_INSERT);
        enable_append = phy_plan->get_enable_append();
      }
    }
    if (nullptr != phy_plan) {
      test::MockCacheObjectFactory::free(phy_plan);
      phy_plan = nullptr;
    }
    return ret;
  }
  int run_physical_plan_(ObPhysicalPlan &phy_plan,
                         int64_t &affected_rows,
                         const AppendExecutionMode execution_mode,
                         AppendRescanResult *rescan_result,
                         AppendShapeGateResult *shape_gate_result)
  {
    int ret = OB_SUCCESS;
    ObExecutor ob_exe;
    exec_ctx_.~ObExecContext();
    new (&exec_ctx_) ObExecContext(allocator_);
    exec_ctx_.set_my_session(&session_info_);
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    sql_ctx_.schema_guard_ = &schema_guard_;
    sql_ctx_.session_info_ = &session_info_;
    exec_ctx_.get_task_exec_ctx().set_min_cluster_version(CLUSTER_VERSION_1500);
    exec_ctx_.get_task_exec_ctx().set_self_addr(optctx_->get_local_server_addr());
    GCTX.self_addr_seq_.set_addr(optctx_->get_local_server_addr());
    if (OB_FAIL(exec_ctx_.init_phy_op(phy_plan.get_phy_operator_size()))) {
      LOG_WARN("fail to init append e2e physical ops", K(ret));
    } else {
      ObTaskExecutorCtx &task_exec_ctx = exec_ctx_.get_task_exec_ctx();
      ObExecuteResult &exe_result = task_exec_ctx.get_execute_result();
      if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
        LOG_WARN("fail to create append e2e physical plan ctx", K(ret));
      } else if (OB_FAIL(exec_ctx_.init_physical_plan_ctx(phy_plan))) {
        LOG_WARN("fail to init append e2e physical plan ctx", K(ret));
      } else if (OB_FAIL(exec_ctx_.get_das_ctx().init(phy_plan, exec_ctx_))) {
        LOG_WARN("fail to init append e2e das ctx", K(ret));
      } else if (OB_FAIL(ob_exe.init(&phy_plan))) {
        LOG_WARN("fail to init append e2e executor", K(ret));
      } else if (OB_FAIL(ob_exe.execute_plan(exec_ctx_))) {
        LOG_WARN("fail to execute append e2e plan", K(ret));
      } else if (OB_FAIL(exe_result.open(exec_ctx_))) {
        if (nullptr != shape_gate_result) {
          ObOperator *root_op = const_cast<ObOperator *>(exe_result.get_static_engine_root());
          if (OB_ISNULL(root_op)) {
            LOG_WARN("append shape gate root operator is null", K(ret));
          } else if (OB_UNLIKELY(PHY_DIRECT_LOAD_STREAM_APPEND != root_op->get_spec().type_)) {
            LOG_WARN("append shape gate root operator type is unexpected", K(ret),
                     K(root_op->get_spec().type_));
          } else {
            ObDirectLoadStreamAppendOp *append_op =
                static_cast<ObDirectLoadStreamAppendOp *>(root_op);
            shape_gate_result->captured_ = true;
            shape_gate_result->queue_handle_valid_ = append_op->queue_handle_.is_valid();
            shape_gate_result->append_registered_ = append_op->append_handle_.is_valid();
            shape_gate_result->remote_append_started_ =
                append_op->remote_append_started_;
          }
        }
        LOG_WARN("fail to open append e2e execute result", K(ret));
      } else {
        ObDirectLoadStreamAppendOp *append_op = nullptr;
        if (AppendExecutionMode::NORMAL != execution_mode) {
          ObOperator *root_op = const_cast<ObOperator *>(exe_result.get_static_engine_root());
          if (OB_ISNULL(root_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("append e2e root operator is null", K(ret));
          } else if (OB_UNLIKELY(PHY_DIRECT_LOAD_STREAM_APPEND != root_op->get_spec().type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("append e2e root operator type is unexpected", K(ret), K(root_op->get_spec().type_));
          } else {
            append_op = static_cast<ObDirectLoadStreamAppendOp *>(root_op);
          }
        }
        if (OB_SUCC(ret)
            && AppendExecutionMode::SMALL_CHUNK_ROW_ERROR == execution_mode) {
          append_op->chunk_writer_.max_payload_size_ = TEST_ROLLOVER_PAYLOAD_SIZE;
        } else if (OB_SUCC(ret)
                   && AppendExecutionMode::FAIL_AFTER_FIRST_CHUNK_TIMEOUT == execution_mode) {
          append_op->chunk_writer_.max_payload_size_ = TEST_ROLLOVER_PAYLOAD_SIZE;
          append_op->try_check_tick_ =
              ObOperator::CHECK_STATUS_TRY_TIMES - TEST_TIMEOUT_CHECK_ROW_COUNT;
          THIS_WORKER.set_timeout_ts(common::ObTimeUtility::current_time() - 1);
        } else if (OB_SUCC(ret)
                   && AppendExecutionMode::PUBLISH_MOCK_CHUNK_THEN_RESCAN == execution_mode) {
          static const char MOCK_CHUNK[] = "published before rejected rescan";
          ObDirectLoadStreamBufferQueue *queue = append_op->queue_handle_.get_queue();
          if (OB_ISNULL(queue)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("append e2e queue is null before rescan", K(ret));
          } else if (OB_FAIL(queue->append_chunk(
                         append_op->append_handle_,
                         ObString(sizeof(MOCK_CHUNK) - 1, MOCK_CHUNK)))) {
            LOG_WARN("fail to publish mock chunk before append rescan", K(ret));
          } else {
            append_op->queue_touched_ = true;
            ++append_op->appended_row_count_;
          }
        }
        if (OB_SUCC(ret)
            && (AppendExecutionMode::RESCAN_BEFORE_QUEUE_TOUCH == execution_mode
                || AppendExecutionMode::PUBLISH_MOCK_CHUNK_THEN_RESCAN == execution_mode)) {
          rescan_result->append_registered_before_rescan_ = append_op->append_handle_.is_valid();
          rescan_result->rescan_ret_ = append_op->rescan();
          rescan_result->append_registered_after_rescan_ = append_op->append_handle_.is_valid();
          rescan_result->queue_touched_after_rescan_ = append_op->queue_touched_;
        } else if (OB_SUCC(ret)) {
          const ObNewRow *row = nullptr;
          int get_ret = OB_SUCCESS;
          while (OB_SUCC(ret) && OB_SUCC(get_ret = exe_result.get_next_row(exec_ctx_, row))) {
            // DML APPEND produces side effects while the result is drained.
          }
          if (OB_ITER_END == get_ret) {
            get_ret = OB_SUCCESS;
          }
          if (OB_FAIL(get_ret)) {
            ret = get_ret;
            LOG_WARN("fail to drain append e2e execute result", K(ret));
          } else {
            affected_rows = exec_ctx_.get_physical_plan_ctx()->get_affected_rows();
          }
        }
      }
      const int close_ret = ob_exe.close(exec_ctx_);
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("fail to close append e2e executor", K(close_ret), K(ret));
        if (OB_SUCC(ret)) {
          ret = close_ret;
        }
      }
      const int result_close_ret = exe_result.close(exec_ctx_);
      if (OB_SUCCESS != result_close_ret) {
        LOG_WARN("fail to close append e2e execute result", K(result_close_ret), K(ret));
        if (OB_SUCC(ret)) {
          ret = result_close_ret;
        }
      }
    }
    return ret;
  }
  int get_local_queue(const int64_t stream_id,
                      ObDirectLoadStreamBufferQueueHandle &queue_handle,
                      const uint64_t queue_id = 0)
  {
    int ret = OB_SUCCESS;
    ObTableLoadService *table_load_service = nullptr;
    queue_handle.reset();
    if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("null table load service", K(ret));
    } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .get_queue(ObDirectLoadStreamBufferQueueKey(stream_id, queue_id),
                                   queue_handle))) {
      LOG_WARN("fail to get append e2e local queue", K(ret), K(stream_id));
    }
    return ret;
  }
  void expect_unsupported_shape_rejected_before_append_registration_(
      const AppendUnsupportedShape unsupported_shape)
  {
    const ObTableSchema *table_schema = nullptr;
    int64_t stream_id = OB_INVALID_ID;
    int64_t affected_rows = 0;
    ObDirectLoadStreamMeta meta;
    ObDirectLoadStreamBufferQueueHandle queue_handle;
    ObDirectLoadStreamBufferQueue *queue = nullptr;
    AppendShapeGateResult shape_gate_result;

    ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
    ASSERT_NE(nullptr, table_schema);
    ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
    ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
    ASSERT_NE(nullptr, queue = queue_handle.get_queue());
    ASSERT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
    ASSERT_EQ(OB_SUCCESS, queue->fatal_ret_);
    ASSERT_EQ(0, queue->active_appends_);
    ASSERT_EQ(0, queue->get_ready_chunk_count_without_lock_());

    EXPECT_EQ(OB_NOT_SUPPORTED,
              execute_append_sql(stream_id,
                                 "(c1, c2)",
                                 "(7, 11)",
                                 affected_rows,
                                 AppendExecutionMode::NORMAL,
                                 nullptr,
                                 AppendBeforeRunAction::NONE,
                                 unsupported_shape,
                                 &shape_gate_result));
    EXPECT_EQ(0, affected_rows);
    EXPECT_TRUE(shape_gate_result.captured_);
    EXPECT_FALSE(shape_gate_result.queue_handle_valid_);
    EXPECT_FALSE(shape_gate_result.append_registered_);
    EXPECT_FALSE(shape_gate_result.remote_append_started_);
    ASSERT_EQ(OB_SUCCESS,
              ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
    EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
    EXPECT_EQ(OB_SUCCESS, meta.result_code_);
    EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
    EXPECT_EQ(OB_SUCCESS, queue->fatal_ret_);
    EXPECT_EQ(0, queue->active_appends_);
    EXPECT_EQ(0, queue->get_ready_chunk_count_without_lock_());
  }

private:
  MockDirectLoadStreamAppendResolverMtlEnv mtl_env_;
};

TEST_F(TestDirectLoadStreamAppendExecutor, stream_append_hash_identity)
{
  // 相同流身份的哈希稳定，且与普通 INSERT、不同流或目标 schema version 区分。
  const char *sql = "insert into s4test.t1 (c1, c2) values (1, 2)";
  ObString query = ObString::make_string(sql);
  ObLogPlan *log_plan = nullptr;
  LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
  ASSERT_EQ(OB_SUCCESS, refresh_query_schema_guard_());
  ASSERT_EQ(OB_SUCCESS, generate_append_logical_plan_(query, sql, log_plan, false));
  ASSERT_NE(nullptr, log_plan);
  ObLogInsert *insert_ptr = dynamic_cast<ObLogInsert *>(log_plan->get_plan_root());
  ASSERT_NE(nullptr, insert_ptr);
  ObLogInsert &insert_op = *insert_ptr;
  const uint64_t seeds[] = {0, 17};
  for (const uint64_t seed : seeds) {
    insert_op.set_direct_load_stream_append(false, OB_INVALID_ID, OB_INVALID_VERSION);
    const uint64_t ordinary_hash = insert_op.hash(seed);
    insert_op.set_direct_load_stream_append(true, 10086, 200);
    const uint64_t stream_hash = insert_op.hash(seed);
    EXPECT_NE(ordinary_hash, stream_hash);
    EXPECT_EQ(stream_hash, insert_op.hash(seed));

    insert_op.set_direct_load_stream_append(true, 10087, 200);
    EXPECT_NE(stream_hash, insert_op.hash(seed));
    insert_op.set_direct_load_stream_append(true, 10086, 201);
    EXPECT_NE(stream_hash, insert_op.hash(seed));
    insert_op.set_direct_load_stream_append(true, 10086, 200);
    EXPECT_EQ(stream_hash, insert_op.hash(seed));
  }
}

TEST_F(TestDirectLoadStreamAppendExecutor, ordinary_insert_codegen_keeps_existing_routes)
{
  ObPhyOperatorType root_type = PHY_INVALID;
  bool has_stream_append = false;
  bool has_table_direct_insert = false;
  bool enable_append = false;

  ASSERT_EQ(OB_SUCCESS,
            generate_non_stream_insert_plan_info(
                "insert into s4test.t1 (c1, c2) values (1, 2)",
                root_type,
                has_stream_append,
                has_table_direct_insert,
                enable_append));
  EXPECT_EQ(PHY_INSERT, root_type);
  EXPECT_FALSE(has_stream_append);
  EXPECT_FALSE(has_table_direct_insert);
  EXPECT_FALSE(enable_append);

  ASSERT_EQ(OB_SUCCESS,
            generate_non_stream_insert_plan_info(
                "insert /*+ direct(true, 0, 'inc') enable_parallel_dml parallel(2) "
                "no_gather_optimizer_statistics */ "
                "into s4test.t1 (c1, c2) select 3, 4",
                root_type,
                has_stream_append,
                has_table_direct_insert,
                enable_append));
  EXPECT_FALSE(has_stream_append);
  EXPECT_TRUE(has_table_direct_insert);
  EXPECT_TRUE(enable_append);
}

TEST_F(TestDirectLoadStreamAppendExecutor, values_append_writes_chunk_and_reader_decodes)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObString chunk;
  ObDirectLoadStreamChunkReader reader;
  TestDirectLoadStreamAppendOutputFrame output;

  int tmp_ret = get_target_table_schema("t1", table_schema);
  ASSERT_EQ(OB_SUCCESS, tmp_ret);
  ASSERT_NE(nullptr, table_schema);
  tmp_ret = create_stream_for_table(*table_schema, stream_id);
  ASSERT_EQ(OB_SUCCESS, tmp_ret);
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  tmp_ret = execute_append_sql(stream_id, affected_rows);
  ASSERT_EQ(OB_SUCCESS, tmp_ret);
  EXPECT_EQ(2, affected_rows);

  tmp_ret = get_local_queue(stream_id, queue_handle);
  ASSERT_EQ(OB_SUCCESS, tmp_ret);
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, queue->get_next_chunk(allocator_, chunk));
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), 2));
  EXPECT_EQ(2, reader.get_row_count());
  EXPECT_EQ(chunk.length() - static_cast<int64_t>(sizeof(ObDirectLoadStreamChunkHeader)), reader.get_payload_size());
  ASSERT_EQ(OB_SUCCESS, output.init(2));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
  EXPECT_EQ(7, output.get_int(0));
  EXPECT_EQ(11, output.get_int(1));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
  EXPECT_EQ(9, output.get_int(0));
  EXPECT_EQ(13, output.get_int(1));
  EXPECT_EQ(OB_ITER_END, reader.get_next_row(output.exprs(), output.eval_ctx()));
}

TEST_F(TestDirectLoadStreamAppendExecutor, autocommit_off_is_rejected_before_queue_registration)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  bool original_autocommit = false;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, session_info_.get_autocommit(original_autocommit));
  ASSERT_TRUE(original_autocommit);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  const int append_ret = execute_append_sql(stream_id, affected_rows);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(original_autocommit));

  EXPECT_EQ(OB_NOT_SUPPORTED, append_ret);
  EXPECT_EQ(0, affected_rows);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
  EXPECT_EQ(0, queue->active_appends_);
  EXPECT_EQ(0, queue->get_ready_chunk_count_without_lock_());
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

TEST_F(TestDirectLoadStreamAppendExecutor, explicit_transaction_is_rejected_before_queue_registration)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  int append_ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  ASSERT_EQ(nullptr, session_info_.get_tx_desc());
  {
    ExplicitTransactionTestGuard tx_guard(session_info_);
    append_ret = execute_append_sql(stream_id, affected_rows);
  }

  EXPECT_EQ(OB_NOT_SUPPORTED, append_ret);
  EXPECT_EQ(0, affected_rows);
  EXPECT_EQ(nullptr, session_info_.get_tx_desc());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
  EXPECT_EQ(0, queue->active_appends_);
  EXPECT_EQ(0, queue->get_ready_chunk_count_without_lock_());
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

// 验证 trigger gate 在 operator 取得 queue、登记 APPEND 前拒绝，且 stream 保持 OPEN。
TEST_F(TestDirectLoadStreamAppendExecutor, trigger_gate_rejects_before_append_registration)
{
  expect_unsupported_shape_rejected_before_append_registration_(
      AppendUnsupportedShape::TRIGGER);
}

// 验证 FK gate 在 operator 取得 queue、登记 APPEND 前拒绝，且 stream 保持 OPEN。
TEST_F(TestDirectLoadStreamAppendExecutor, foreign_key_gate_rejects_before_append_registration)
{
  expect_unsupported_shape_rejected_before_append_registration_(
      AppendUnsupportedShape::FOREIGN_KEY);
}

TEST_F(TestDirectLoadStreamAppendExecutor, runtime_schema_guard_rejects_drop_before_open)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  EXPECT_EQ(OB_TABLE_NOT_EXIST,
            execute_append_sql(stream_id,
                               "(c1, c2)",
                               "(7, 11)",
                               affected_rows,
                               AppendExecutionMode::NORMAL,
                               nullptr,
                               AppendBeforeRunAction::DROP_TARGET_TABLE));
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_TABLE_NOT_EXIST, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

TEST_F(TestDirectLoadStreamAppendExecutor, runtime_schema_guard_rejects_revoked_insert_privilege)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  const int ret = execute_append_sql(stream_id,
                                     "(c1, c2)",
                                     "(7, 11)",
                                     affected_rows,
                                     AppendExecutionMode::NORMAL,
                                     nullptr,
                                     AppendBeforeRunAction::REVOKE_INSERT_PRIV);
  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE, ret);
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue_handle.get_queue());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue_handle.get_queue()->get_state());
  EXPECT_EQ(0, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
}

TEST_F(TestDirectLoadStreamAppendExecutor, row_error_before_first_chunk_keeps_stream_open_for_retry)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue_handle.get_queue());
  EXPECT_EQ(0, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());

  const int append_ret = execute_append_sql(stream_id,
                                            "(c1, c2)",
                                            "('not_an_integer', 11)",
                                            affected_rows);
  EXPECT_NE(OB_SUCCESS, append_ret);
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN,
            queue_handle.get_queue()->get_state());
  EXPECT_EQ(0, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());

  EXPECT_EQ(OB_SUCCESS,
            execute_append_sql(stream_id, "(c1, c2)", "(7, 11)", affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(1, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
}

// No CHUNK permits retry; a published CHUNK or an unknown END result remains fatal.
TEST_F(TestDirectLoadStreamAppendExecutor, remote_error_respects_first_chunk_boundary)
{
  WorkerTimeoutResetGuard timeout_guard;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10 * 1000 * 1000);
  for (int scenario = 0; scenario < 3; ++scenario) {
    const bool publish_chunk = 1 == scenario;
    const bool lose_end_response = 2 == scenario;
    const ObTableSchema *table_schema = nullptr;
    ObAddr remote_addr;
    ObDirectLoadStreamQueueEntry entry;
    ObSEArray<ObDirectLoadStreamQueueEntry, 1> entries;
    ObSEArray<uint64_t, 1> queue_ids;
    ObDirectLoadStreamBufferQueueHandle queue_handle;
    int64_t stream_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS,
              get_target_table_schema(0 == scenario ? "t_lob" : "t1", table_schema));
    ASSERT_NE(nullptr, table_schema);
    ASSERT_TRUE(remote_addr.set_ip_addr("2.2.2.2", 9999));
    entry.server_addr_ = remote_addr;
    entry.queue_count_ = 1;
    ASSERT_EQ(OB_SUCCESS, entries.push_back(entry));
    ASSERT_EQ(OB_SUCCESS, create_stream_metadata_for_table(*table_schema, entries, stream_id));
    ASSERT_EQ(OB_SUCCESS, queue_ids.push_back(0));
    ObDirectLoadStreamService &service =
        MTL(ObTableLoadService *)->get_direct_load_stream_service();
    const ObAddr source_server = GCTX.self_addr();
    GCTX.self_addr_seq_.set_addr(remote_addr);
    const int create_ret = service.create_local_queue_batch(
        stream_id, table_schema->get_schema_version(), remote_addr, queue_ids);
    GCTX.self_addr_seq_.set_addr(source_server);
    ASSERT_EQ(OB_SUCCESS, create_ret);
    ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
    ObDirectLoadStreamBufferQueue *queue = queue_handle.get_queue();
    ASSERT_NE(nullptr, queue);
    ScopedStreamForwardRpc mock_rpc(lose_end_response);
    if (0 == scenario) {
      // Keep a claimed SOURCE alive across a genuine oversized codec input.
      ObDirectLoadStreamClaimParam claim;
      claim.stream_id_ = stream_id;
      claim.owner_server_ = source_server;
      claim.session_id_ = 91002;
      claim.session_create_ts_ = ObTimeUtility::current_time();
      ASSERT_EQ(OB_SUCCESS, service.claim_stream(claim));
    }
    ObDirectLoadStreamForwardRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    proxy.to(remote_addr).by(TEST_TENANT_ID);
    ObDirectLoadStreamForwardBeginArg begin;
    begin.tenant_id_ = TEST_TENANT_ID;
    begin.stream_id_ = stream_id;
    begin.queue_id_ = 0;
    begin.target_schema_version_ = table_schema->get_schema_version();
    begin.statement_id_ = static_cast<uint64_t>(stream_id);
    begin.absolute_deadline_ = ObTimeUtility::current_time() + 10 * 1000 * 1000;
    ASSERT_EQ(OB_SUCCESS, proxy.begin(begin));
    ObDirectLoadStreamAppendSpec spec(exec_ctx_.get_allocator(), PHY_DIRECT_LOAD_STREAM_APPEND);
    spec.stream_id_ = stream_id;
    ObDirectLoadStreamAppendOp op(exec_ctx_, spec, nullptr);
    op.queue_desc_.server_addr_ = remote_addr;
    op.queue_desc_.queue_id_ = 0;
    op.statement_id_ = begin.statement_id_;
    op.absolute_deadline_ = begin.absolute_deadline_;
    op.remote_append_started_ = true;
    TestDirectLoadStreamAppendOutputFrame input;
    if (publish_chunk) {
      // A CHUNK side effect makes a later row error fatal.
      ObDirectLoadStreamForwardChunkArg chunk;
      chunk.statement_id_ = begin.statement_id_;
      chunk.chunk_ = ObString::make_string("published data");
      ASSERT_EQ(OB_SUCCESS, proxy.append_chunk(chunk));
      op.queue_touched_ = true;
      EXPECT_EQ(OB_SUCCESS, op.finish_remote_append_(OB_INVALID_DATA));
    } else {
      // A pre-CHUNK row error is recoverable only after confirmed admission release.
      if (lose_end_response) {
        // Release the observer handle before synchronous fatal queue cleanup.
        queue_handle.reset();
        queue = nullptr;
      }
      if (lose_end_response) {
        EXPECT_EQ(OB_INVALID_DATA, op.write_rows_post_proc(OB_INVALID_DATA));
      } else {
        // Exercise the default 8 MiB codec boundary before any remote CHUNK.
        const int64_t payload_size = ObDirectLoadStreamChunkWriter::DEFAULT_MAX_PAYLOAD_SIZE;
        char *payload = static_cast<char *>(allocator_.alloc(payload_size));
        ASSERT_NE(nullptr, payload);
        MEMSET(payload, 'x', payload_size);
        ASSERT_EQ(OB_SUCCESS, input.init(2));
        // An input integer needs storage in the existing frame result buffer.
        ObExpr &id_expr = *input.exprs().at(0);
        ObDatum &id_datum = id_expr.locate_expr_datum(input.eval_ctx());
        id_datum.ptr_ = input.eval_ctx().frames_[0] + id_expr.res_buf_off_;
        id_datum.set_int(9);
        input.exprs().at(1)->locate_expr_datum(input.eval_ctx()).set_string(
            payload, static_cast<uint32_t>(payload_size));
        ASSERT_EQ(OB_SUCCESS, op.chunk_writer_.init(TEST_TENANT_ID));
        const int row_ret = op.chunk_writer_.append_row(input.exprs(), input.eval_ctx());
        ASSERT_EQ(OB_SIZE_OVERFLOW, row_ret);
        EXPECT_FALSE(op.queue_touched_);
        EXPECT_EQ(0, op.chunk_writer_.get_row_count());
        EXPECT_EQ(row_ret, op.write_rows_post_proc(row_ret));
      }
    }
    EXPECT_FALSE(op.remote_append_started_);
    EXPECT_EQ(1, mock_rpc.begin_count_);
    EXPECT_EQ(publish_chunk ? 1 : 0, mock_rpc.chunk_count_);
    EXPECT_EQ(publish_chunk ? 0 : 1, mock_rpc.end_count_);
    EXPECT_EQ(publish_chunk ? 1 : 0, mock_rpc.abort_count_);
    if (!lose_end_response) {
      EXPECT_EQ(0, queue->active_appends_);
    }
    ObDirectLoadStreamAppendHandle retry_handle;
    if (publish_chunk) {
      EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue->get_state());
      EXPECT_EQ(OB_INVALID_DATA, queue->begin_append(retry_handle));
    } else if (lose_end_response) {
      // A lost END response is fatal even after the owner released admission.
      ObDirectLoadStreamMeta meta;
      ASSERT_EQ(OB_SUCCESS, service.get_stream(stream_id, meta));
      EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
      EXPECT_EQ(OB_TIMEOUT, meta.result_code_);
      EXPECT_TRUE(op.stream_failed_reported_);
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
    } else {
      EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
      ObDirectLoadStreamMeta meta;
      ASSERT_EQ(OB_SUCCESS, service.get_stream(stream_id, meta));
      EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
      EXPECT_EQ(OB_SUCCESS, meta.result_code_);
      ++begin.statement_id_;
      ASSERT_EQ(OB_SUCCESS, proxy.begin(begin));
      op.statement_id_ = begin.statement_id_;
      op.absolute_deadline_ = begin.absolute_deadline_;
      op.remote_append_started_ = true;
      input.exprs().at(0)->locate_expr_datum(input.eval_ctx()).set_int(1);
      input.exprs().at(1)->locate_expr_datum(input.eval_ctx()).set_string("retry data", 10);
      ASSERT_EQ(OB_SUCCESS, op.chunk_writer_.append_row(input.exprs(), input.eval_ctx()));
      ASSERT_EQ(OB_SUCCESS, op.flush_chunk_());
      ASSERT_EQ(OB_SUCCESS, op.finish_remote_append_(OB_SUCCESS));
      EXPECT_EQ(2, mock_rpc.begin_count_);
      EXPECT_EQ(1, mock_rpc.chunk_count_);
      EXPECT_EQ(2, mock_rpc.end_count_);
      EXPECT_EQ(0, mock_rpc.abort_count_);
      EXPECT_EQ(0, queue->active_appends_);
      EXPECT_EQ(1, queue->get_ready_chunk_count_without_lock_());
      ASSERT_EQ(OB_SUCCESS, service.seal_stream(stream_id));
      ObDirectLoadStreamChunkReader reader;
      TestDirectLoadStreamAppendOutputFrame output;
      ObString chunk;
      ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, output.init(2));
      ASSERT_EQ(OB_SUCCESS, queue->get_next_chunk(allocator_, chunk));
      ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), 2));
      ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
      EXPECT_EQ(1, output.get_int(0));
      EXPECT_EQ(ObString::make_string("retry data"),
                output.exprs().at(1)->locate_expr_datum(output.eval_ctx()).get_string());
      EXPECT_EQ(OB_ITER_END, reader.get_next_row(output.exprs(), output.eval_ctx()));
      EXPECT_EQ(OB_ITER_END, queue->get_next_chunk(allocator_, chunk));
      EXPECT_EQ(OB_SUCCESS, service.check_stream_reader_drain_completed(stream_id));
    }
  }
}

// Reject a real LONGTEXT SQL row over 8 MiB after claim, then decode only legal rows.
TEST_F(TestDirectLoadStreamAppendExecutor, oversized_append_keeps_claimed_stream_usable)
{
  WorkerTimeoutResetGuard timeout_guard;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000);
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim;
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t_lob", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ObDirectLoadStreamService &service =
      MTL(ObTableLoadService *)->get_direct_load_stream_service();
  claim.stream_id_ = stream_id;
  claim.owner_server_ = GCTX.self_addr();
  claim.session_id_ = 91001;
  claim.session_create_ts_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, service.claim_stream(claim));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ObDirectLoadStreamBufferQueue *queue = queue_handle.get_queue();
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(OB_SUCCESS,
            execute_append_sql(stream_id, "(c1, c2)", "(1, 'before')", affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            execute_append_sql(stream_id, "(c1, c2)",
                               "(9, repeat('x', 8388608))", affected_rows));
  EXPECT_EQ(0, affected_rows);
  EXPECT_EQ(0, queue->active_appends_);
  EXPECT_EQ(1, queue->get_ready_chunk_count_without_lock_());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
  ASSERT_EQ(OB_SUCCESS, service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
  ASSERT_EQ(OB_SUCCESS,
            execute_append_sql(stream_id, "(c1, c2)", "(2, 'after')", affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(0, queue->active_appends_);
  EXPECT_EQ(2, queue->get_ready_chunk_count_without_lock_());
  // CLOSE starts a new request after the SQL harness restores its worker context.
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10 * 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, service.seal_stream(stream_id));
  ObDirectLoadStreamChunkReader reader;
  TestDirectLoadStreamAppendOutputFrame output;
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, output.init(2));
  for (int64_t expected_id = 1; expected_id <= 2; ++expected_id) {
    ObString chunk;
    ASSERT_EQ(OB_SUCCESS, queue->get_next_chunk(allocator_, chunk));
    ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), 2));
    ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
    EXPECT_EQ(expected_id, output.get_int(0));
    EXPECT_EQ(OB_ITER_END, reader.get_next_row(output.exprs(), output.eval_ctx()));
    reader.reuse();
  }
  ObString chunk;
  EXPECT_EQ(OB_ITER_END, queue->get_next_chunk(allocator_, chunk));
  EXPECT_EQ(OB_SUCCESS, service.check_stream_reader_drain_completed(stream_id));
}

TEST_F(TestDirectLoadStreamAppendExecutor, column_insert_privilege_passes_runtime_recheck)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                               ObString::make_string("%"),
                                               TEST_STREAM_USER_ID));
  session_info_.set_priv_user_id(TEST_STREAM_USER_ID);
  session_info_.set_user_priv_set(0);
  ASSERT_EQ(OB_SUCCESS, set_current_user_priv_set_(0));
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS,
            add_column_insert_priv(*schema_service_, TEST_TENANT_ID, TEST_STREAM_USER_ID,
                                   "s4test", "t1", "c1", 11001));
  ASSERT_EQ(OB_SUCCESS,
            add_column_insert_priv(*schema_service_, TEST_TENANT_ID, TEST_STREAM_USER_ID,
                                   "s4test", "t1", "c2", 11002));
  ASSERT_EQ(OB_SUCCESS, refresh_query_schema_guard_());
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));

  EXPECT_EQ(OB_SUCCESS, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(2, affected_rows);
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue_handle.get_queue());
  EXPECT_EQ(1, queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
}

// 多 queue 拓扑选中的本地 queue 缺失时，APPEND 保留原错误并将 Stream 收敛为失败终态。
TEST_F(TestDirectLoadStreamAppendExecutor,
       selected_local_queue_missing_finishes_multiple_queue_stream)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamQueueEntry entry;
  ObSEArray<ObDirectLoadStreamQueueEntry, 1> queue_entries;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  entry.server_addr_ = GCTX.self_addr();
  entry.queue_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(entry));
  ASSERT_EQ(OB_SUCCESS,
            create_stream_metadata_for_table(*table_schema, queue_entries, stream_id));

  EXPECT_EQ(OB_ENTRY_NOT_EXIST, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

// 连续 APPEND statement 按 RouteCtx 轮询不同本地 queue，并分别发布一个 chunk。
TEST_F(TestDirectLoadStreamAppendExecutor, executor_routes_statements_across_local_queues)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamQueueEntry entry;
  ObSEArray<ObDirectLoadStreamQueueEntry, 1> queue_entries;
  ObSEArray<uint64_t, 2> queue_ids;
  ObDirectLoadStreamBufferQueueHandle first_queue_handle;
  ObDirectLoadStreamBufferQueueHandle second_queue_handle;
  ObTableLoadService *table_load_service = nullptr;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  entry.server_addr_ = GCTX.self_addr();
  entry.queue_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(entry));
  ASSERT_EQ(OB_SUCCESS,
            create_stream_metadata_for_table(*table_schema, queue_entries, stream_id));
  ASSERT_EQ(OB_SUCCESS, queue_ids.push_back(0));
  ASSERT_EQ(OB_SUCCESS, queue_ids.push_back(1));
  ASSERT_NE(nullptr, table_load_service = MTL(ObTableLoadService *));
  ASSERT_EQ(OB_SUCCESS,
            table_load_service->get_direct_load_stream_service()
                .create_local_queue_batch(stream_id,
                                          table_schema->get_schema_version(),
                                          GCTX.self_addr(),
                                          queue_ids));

  ASSERT_EQ(OB_SUCCESS, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(2, affected_rows);
  affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(2, affected_rows);
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, first_queue_handle, 0));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, second_queue_handle, 1));
  ASSERT_NE(nullptr, first_queue_handle.get_queue());
  ASSERT_NE(nullptr, second_queue_handle.get_queue());
  EXPECT_EQ(1, first_queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
  EXPECT_EQ(1, second_queue_handle.get_queue()->get_ready_chunk_count_without_lock_());
}

// 选中远端 queue 时必须走正式 forward RPC，
// 测试环境缺少 proxy 应明确失败且不建本地 queue。
TEST_F(TestDirectLoadStreamAppendExecutor, executor_requires_rpc_proxy_for_remote_queue)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObAddr remote_addr;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamQueueEntry entry;
  ObSEArray<ObDirectLoadStreamQueueEntry, 1> queue_entries;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_TRUE(remote_addr.set_ip_addr("2.2.2.2", 9999));
  ASSERT_FALSE(remote_addr == GCTX.self_addr());
  entry.server_addr_ = remote_addr;
  entry.queue_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(entry));
  ASSERT_EQ(OB_SUCCESS,
            create_stream_metadata_for_table(*table_schema, queue_entries, stream_id));

  EXPECT_EQ(OB_TIMEOUT, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_TIMEOUT, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

// 单 queue 在执行前已被清理时，APPEND 保留缺失错误并将 Stream 收敛为失败终态。
TEST_F(TestDirectLoadStreamAppendExecutor, selected_local_queue_missing_finishes_stream)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, cleanup_local_queue(stream_id));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));

  EXPECT_EQ(OB_ENTRY_NOT_EXIST, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

// 真实行转换错误发生在 chunk 已发布之后：在失败 metadata 写入前，queue 就必须拒绝
// CLOSE 和读取，不能让部分 chunk 获得正常 EOF 或成功终态；收尾仍应释放全部引用。
TEST_F(TestDirectLoadStreamAppendExecutor, row_error_after_publish_blocks_close_before_metadata)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim_param;
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamService &service =
      MTL(ObTableLoadService *)->get_direct_load_stream_service();

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  claim_param.stream_id_ = stream_id;
  claim_param.owner_server_ = GCTX.self_addr();
  claim_param.session_id_ = 91001;
  claim_param.session_create_ts_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, service.claim_stream(claim_param));
  ASSERT_EQ(OB_SUCCESS, service.get_meta_store(meta_store));
  ASSERT_NE(nullptr, meta_store);
  AppendReleaseRaceMetaStore &race = *static_cast<AppendReleaseRaceMetaStore *>(meta_store);
  race.observed_stream_id_ = stream_id;
  EXPECT_EQ(OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            execute_append_sql(
                stream_id, "(c1, c2)",
                "(1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16), "
                "(7, 17), (8, 18), (9, 19), (10, 20), (11, 21), (12, 22), "
                "(13, 23), ('not_an_integer', 24)",
                affected_rows, AppendExecutionMode::SMALL_CHUNK_ROW_ERROR));
  EXPECT_EQ(0, affected_rows);
  race.expect_failure_before_release(OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD);
  ASSERT_EQ(OB_SUCCESS, service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, handle));
}

// rescan 不经过行错误后处理，也必须在释放已触碰 queue 的 APPEND 前保存失败，
// 并保证竞争 CLOSE 无法成功、登记失效且 cleanup 不等待调用方自身。
TEST_F(TestDirectLoadStreamAppendExecutor, rescan_after_publish_blocks_close_before_metadata)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle handle;
  AppendRescanResult rescan_result;
  ObDirectLoadStreamService &service =
      MTL(ObTableLoadService *)->get_direct_load_stream_service();

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, service.get_meta_store(meta_store));
  ASSERT_NE(nullptr, meta_store);
  AppendReleaseRaceMetaStore &race = *static_cast<AppendReleaseRaceMetaStore *>(meta_store);
  race.observed_stream_id_ = stream_id;
  EXPECT_EQ(OB_SUCCESS, execute_append_sql(
      stream_id, "(c1, c2)", "(7, 11), (9, 13)", affected_rows,
      AppendExecutionMode::PUBLISH_MOCK_CHUNK_THEN_RESCAN, &rescan_result));
  EXPECT_EQ(OB_NOT_SUPPORTED, rescan_result.rescan_ret_);
  EXPECT_TRUE(rescan_result.append_registered_before_rescan_);
  EXPECT_FALSE(rescan_result.append_registered_after_rescan_);
  EXPECT_TRUE(rescan_result.queue_touched_after_rescan_);
  EXPECT_EQ(0, affected_rows);
  race.expect_failure_before_release(OB_NOT_SUPPORTED);
  ASSERT_EQ(OB_SUCCESS, service.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_NOT_SUPPORTED, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, handle));
}

// cleanup 先把 queue 标记 REMOVED 并等待引用；失败 APPEND 仍必须释放登记，
// 不能因为 queue.fail() 返回缺失就跳过 end_append()，造成清理等待调用方自身。
TEST_F(TestDirectLoadStreamAppendExecutor, failed_append_releases_queue_during_cleanup)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObDirectLoadStreamService &service =
      MTL(ObTableLoadService *)->get_direct_load_stream_service();
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  exec_ctx_.set_my_session(&session_info_);
  ObDirectLoadStreamAppendSpec spec(allocator_, PHY_DIRECT_LOAD_STREAM_APPEND);
  spec.stream_id_ = stream_id;
  ObDirectLoadStreamAppendOp append_op(exec_ctx_, spec, nullptr);
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, append_op.queue_handle_));
  ASSERT_EQ(OB_SUCCESS, append_op.begin_queue_append_());
  ObDirectLoadStreamBufferQueue *queue = append_op.queue_handle_.get_queue();
  ASSERT_NE(nullptr, queue);
  ASSERT_EQ(OB_SUCCESS, queue->append_chunk(
      append_op.append_handle_, ObString::make_string("published before cleanup")));
  append_op.queue_touched_ = true;
  std::future<int> cleanup_future = std::async(
      std::launch::async, &ObDirectLoadStreamBufferQueue::cleanup, queue);
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ObDirectLoadStreamBufferQueueState::REMOVED != queue->get_state()
         && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool removed = ObDirectLoadStreamBufferQueueState::REMOVED == queue->get_state();
  const int release_ret = append_op.finish_append_(OB_CANCELED);
  const bool released = !append_op.append_handle_.is_valid();
  // 即使用例检测到释放回归，也先解开测试持有的引用，避免 future 析构阻塞测试进程。
  if (!released) {
    (void)queue->end_append(append_op.append_handle_);
  }
  append_op.queue_handle_.reset();
  const std::future_status cleanup_status = cleanup_future.wait_for(std::chrono::seconds(5));
  EXPECT_TRUE(removed);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, release_ret);
  EXPECT_TRUE(released);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  if (std::future_status::ready == cleanup_status) {
    EXPECT_EQ(OB_SUCCESS, cleanup_future.get());
  }
  EXPECT_EQ(OB_SUCCESS, service.get_buffer_queue_manager().cleanup_queue(
      ObDirectLoadStreamBufferQueueKey(stream_id, 0)));
}

TEST_F(TestDirectLoadStreamAppendExecutor, first_chunk_then_worker_timeout_fails_without_tail_flush)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  int append_ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  WorkerTimeoutResetGuard timeout_guard;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  queue_handle.reset();
  queue = nullptr;
  append_ret = execute_append_sql(
      stream_id,
      "(c1, c2)",
      "(1, 11), (2, 12), (3, 13), (4, 14), (5, 15), (6, 16), "
      "(7, 17), (8, 18), (9, 19), (10, 20), (11, 21), (12, 22)",
      affected_rows,
      AppendExecutionMode::FAIL_AFTER_FIRST_CHUNK_TIMEOUT);

  EXPECT_EQ(OB_TIMEOUT, append_ret);
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_TIMEOUT, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

TEST_F(TestDirectLoadStreamAppendExecutor, rescan_before_queue_touch_releases_append_without_finishing_stream)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObDirectLoadStreamMeta meta;
  AppendRescanResult rescan_result;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());

  ASSERT_EQ(OB_SUCCESS,
            execute_append_sql(stream_id,
                               "(c1, c2)",
                               "(7, 11), (9, 13)",
                               affected_rows,
                               AppendExecutionMode::RESCAN_BEFORE_QUEUE_TOUCH,
                               &rescan_result));
  EXPECT_EQ(OB_NOT_SUPPORTED, rescan_result.rescan_ret_);
  EXPECT_TRUE(rescan_result.append_registered_before_rescan_);
  EXPECT_FALSE(rescan_result.append_registered_after_rescan_);
  EXPECT_FALSE(rescan_result.queue_touched_after_rescan_);
  EXPECT_EQ(0, affected_rows);
  EXPECT_EQ(0, queue->active_appends_);
  EXPECT_EQ(0, queue->get_ready_chunk_count_without_lock_());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::OPEN, queue->get_state());
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

TEST_F(TestDirectLoadStreamAppendExecutor, rescan_after_queue_touch_fails_stream_without_duplicate_chunk)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObDirectLoadStreamMeta meta;
  AppendRescanResult rescan_result;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  queue_handle.reset();
  queue = nullptr;

  ASSERT_EQ(OB_SUCCESS,
            execute_append_sql(stream_id,
                               "(c1, c2)",
                               "(7, 11), (9, 13)",
                               affected_rows,
                               AppendExecutionMode::PUBLISH_MOCK_CHUNK_THEN_RESCAN,
                               &rescan_result));
  EXPECT_EQ(OB_NOT_SUPPORTED, rescan_result.rescan_ret_);
  EXPECT_TRUE(rescan_result.append_registered_before_rescan_);
  EXPECT_FALSE(rescan_result.append_registered_after_rescan_);
  EXPECT_TRUE(rescan_result.queue_touched_after_rescan_);
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_NOT_SUPPORTED, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

// 精确选中的 queue 已失败时，
// executor 返回其首错并用同一实际错误收敛 metadata。
TEST_F(TestDirectLoadStreamAppendExecutor, failed_queue_converges_first_error_to_metadata)
{
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  int queue_fatal_ret_code = OB_SUCCESS;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, queue->fail(OB_ERR_UNEXPECTED));
  ASSERT_EQ(OB_SUCCESS, queue->get_first_error(queue_fatal_ret_code));
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue_fatal_ret_code);
  queue_handle.reset();
  queue = nullptr;

  EXPECT_EQ(OB_ERR_UNEXPECTED, execute_append_sql(stream_id, affected_rows));
  EXPECT_EQ(0, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, meta.result_code_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_local_queue(stream_id, queue_handle));
}

TEST_F(TestDirectLoadStreamAppendExecutor, consecutive_two_stream_appends_keep_target_metadata)
{
  const ObTableSchema *first_table_schema = nullptr;
  const ObTableSchema *second_table_schema = nullptr;
  int64_t first_stream_id = OB_INVALID_ID;
  int64_t second_stream_id = OB_INVALID_ID;
  int64_t affected_rows = 0;
  ObDirectLoadStreamBufferQueueHandle first_queue_handle;
  ObDirectLoadStreamBufferQueueHandle second_queue_handle;
  ObDirectLoadStreamBufferQueue *first_queue = nullptr;
  ObDirectLoadStreamBufferQueue *second_queue = nullptr;
  ObString chunk;
  ObDirectLoadStreamChunkReader reader;
  TestDirectLoadStreamAppendOutputFrame output;

  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t1", first_table_schema));
  ASSERT_EQ(OB_SUCCESS, get_target_table_schema("t4", second_table_schema));
  ASSERT_NE(nullptr, first_table_schema);
  ASSERT_NE(nullptr, second_table_schema);
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*first_table_schema, first_stream_id));
  ASSERT_EQ(OB_SUCCESS, create_stream_for_table(*second_table_schema, second_stream_id));
  ASSERT_TRUE(is_valid_stream_id(first_stream_id));
  ASSERT_TRUE(is_valid_stream_id(second_stream_id));
  ASSERT_NE(first_stream_id, second_stream_id);

  ASSERT_EQ(OB_SUCCESS, execute_append_sql(first_stream_id, "(c1, c2)", "(101, 111), (102, 112)", affected_rows));
  EXPECT_EQ(2, affected_rows);
  ASSERT_EQ(OB_SUCCESS, execute_append_sql(second_stream_id, "(c1, c2)", "(201, 211), (202, 212)", affected_rows));
  EXPECT_EQ(2, affected_rows);

  ASSERT_EQ(OB_SUCCESS, get_local_queue(first_stream_id, first_queue_handle));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(second_stream_id, second_queue_handle));
  ASSERT_NE(nullptr, first_queue = first_queue_handle.get_queue());
  ASSERT_NE(nullptr, second_queue = second_queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));

  ASSERT_EQ(OB_SUCCESS, first_queue->get_next_chunk(allocator_, chunk));
  ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), 2));
  ASSERT_EQ(OB_SUCCESS, output.init(2));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
  EXPECT_EQ(101, output.get_int(0));
  EXPECT_EQ(111, output.get_int(1));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
  EXPECT_EQ(102, output.get_int(0));
  EXPECT_EQ(112, output.get_int(1));
  EXPECT_EQ(OB_ITER_END, reader.get_next_row(output.exprs(), output.eval_ctx()));

  chunk.reset();
  reader.reset();
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, second_queue->get_next_chunk(allocator_, chunk));
  ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), 4));
  TestDirectLoadStreamAppendOutputFrame second_output;
  ASSERT_EQ(OB_SUCCESS, second_output.init(4));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(second_output.exprs(), second_output.eval_ctx()));
  EXPECT_EQ(201, second_output.get_int(0));
  EXPECT_EQ(211, second_output.get_int(1));
  EXPECT_EQ(1, second_output.get_int(2));
  EXPECT_EQ(2, second_output.get_int(3));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(second_output.exprs(), second_output.eval_ctx()));
  EXPECT_EQ(202, second_output.get_int(0));
  EXPECT_EQ(212, second_output.get_int(1));
  EXPECT_EQ(1, second_output.get_int(2));
  EXPECT_EQ(2, second_output.get_int(3));
  EXPECT_EQ(OB_ITER_END, reader.get_next_row(second_output.exprs(), second_output.eval_ctx()));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_direct_load_stream_append_resolver.log", true);
  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  return RUN_ALL_TESTS();
}

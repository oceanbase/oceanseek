/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/test_sql_utils.h"
#include "sql/engine/cmd/ob_table_direct_insert_ctx.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/ob_result_set.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_instance.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/test_direct_load_stream_utils.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"
#include "observer/table_load/stream/ob_direct_load_stream_prepare_ctx.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_interface_pragma.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/table/ob_direct_load_stream_queue_list.h"
#include "share/schema/ob_table_schema.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test
{
namespace
{

const uint64_t TEST_PREPARE_TENANT_ID = 1002;
const uint64_t TEST_PREPARE_SESSION_ID = 8001;
const int64_t TEST_PREPARE_SESSION_CREATE_TS = 7001;
const uint64_t TEST_NO_INSERT_USER_ID = 8002;
const uint64_t TEST_INSERT_ROLE_ID = 8003;
const uint64_t TEST_C1_PRIV_ID = 9001;
const uint64_t TEST_C2_PRIV_ID = 9002;
const uint64_t TEST_SELECT_PRIV_ID = 9003;

class MockDirectLoadStreamPrepareMtlEnv
{
public:
  MockDirectLoadStreamPrepareMtlEnv()
    : tenant_base_(TEST_PREPARE_TENANT_ID),
      table_load_service_(TEST_PREPARE_TENANT_ID),
      mock_meta_store_(TEST_PREPARE_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamPrepareMtlEnv()
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
          TEST_PREPARE_TENANT_ID);
      alloc_ret = lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
          TEST_PREPARE_TENANT_ID);
      if (OB_UNLIKELY(OB_SUCCESS != alloc_ret && OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(lib::ObMallocAllocator::get_instance()->set_tenant_limit(
                     TEST_PREPARE_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_PREPARE_TENANT_ID));
      } else if (OB_ISNULL(lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
                           TEST_PREPARE_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant ctx allocator is null", KR(ret), K(TEST_PREPARE_TENANT_ID));
      } else if (!self_addr_.set_ip_addr("1.1.1.1", 8888)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set test self address", KR(ret));
      } else {
        created_allocator_ = !allocator_existed && OB_SUCCESS == alloc_ret;
        old_self_addr_ = GCTX.self_addr();
        GCTX.self_addr_seq_.set_addr(self_addr_);
        old_tenant_ = share::ObTenantEnv::get_tenant();
        tenant_base_.set<observer::ObTableLoadService *>(&table_load_service_);
        share::ObTenantEnv::set_tenant(&tenant_base_);
        observer::ObDirectLoadStreamService &stream_service =
            table_load_service_.get_direct_load_stream_service();
        if (OB_FAIL(mock_meta_store_.init())) {
          LOG_WARN("fail to init prepare mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.set_meta_store_for_test(mock_meta_store_))) {
          LOG_WARN("fail to inject prepare mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.enable_local_stream_id_generator_for_test())) {
          LOG_WARN("fail to enable prepare test stream id generator", KR(ret));
        } else if (OB_FAIL(stream_service.enable_injected_queue_topology_for_test())) {
          LOG_WARN("fail to enable prepare test stream topology injection", KR(ret));
        } else if (OB_FAIL(stream_service.get_buffer_queue_manager()
                               .set_mock_queue_store_factory_for_test(
                                   observer::get_direct_load_stream_mock_store_factory()))) {
          LOG_WARN("fail to inject prepare mock queue store factory", KR(ret));
        } else if (OB_FAIL(table_load_service_.init())) {
          LOG_WARN("fail to init prepare table load service", KR(ret));
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
    }
    if (OB_FAIL(ret) && created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_PREPARE_TENANT_ID);
      created_allocator_ = false;
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
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_PREPARE_TENANT_ID);
      created_allocator_ = false;
    }
  }
  observer::ObTableLoadService &get_table_load_service() { return table_load_service_; }
  observer::ObMockDirectLoadStreamMetaStore &get_mock_meta_store()
  {
    return mock_meta_store_;
  }

private:
  share::ObTenantBase tenant_base_;
  observer::ObTableLoadService table_load_service_;
  observer::ObMockDirectLoadStreamMetaStore mock_meta_store_;
  share::ObTenantBase *old_tenant_;
  ObAddr old_self_addr_;
  ObAddr self_addr_;
  bool created_allocator_;
  bool is_inited_;
};

} // namespace

class TestDirectLoadStreamPrepareCtx : public TestOptimizerUtils
{
public:
  TestDirectLoadStreamPrepareCtx() = default;
  virtual ~TestDirectLoadStreamPrepareCtx() = default;
  virtual void SetUp()
  {
    schema_file_path_[0] = 0;
    init();
    prepare_user_tenant();
    ASSERT_EQ(OB_SUCCESS, mtl_env_.init());
    prepare_database();
  }
  virtual void TearDown()
  {
    mtl_env_.destroy();
    TestSqlUtils::destroy();
  }

private:
  void prepare_user_tenant()
  {
    ObTenantSchema tenant_schema;
    ObSysVariableSchema sys_variable;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    int compat_init_ret = OB_SUCCESS;
    tenant_schema.set_tenant_id(TEST_PREPARE_TENANT_ID);
    tenant_schema.set_schema_version(schema_version_);
    tenant_schema.set_locality("auto_locality_strategy");
    ASSERT_EQ(OB_SUCCESS, tenant_schema.add_zone("zone"));
    ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("dls_tenant"));
    ASSERT_EQ(OB_SUCCESS, schema_service_->add_tenant_schema(tenant_schema, schema_version_++));

    sys_variable.set_tenant_id(TEST_PREPARE_TENANT_ID);
    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    sys_variable.set_schema_version(schema_version_);
    ASSERT_EQ(OB_SUCCESS,
              schema_service_->add_sys_variable_schema(sys_variable, schema_version_++));
    ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));

    ASSERT_EQ(OB_SUCCESS,
              omt::ObTenantConfigMgr::get_instance().add_tenant_config(
                  TEST_PREPARE_TENANT_ID));
    compat_init_ret = share::ObCompatModeGetter::instance().init(nullptr);
    ASSERT_TRUE(OB_SUCCESS == compat_init_ret || OB_INIT_TWICE == compat_init_ret);
    ASSERT_EQ(OB_SUCCESS, share::ObCompatModeGetter::instance().set_tenant_compat_mode(
                              TEST_PREPARE_TENANT_ID, compat_mode));

    // Stream service 只属于 user tenant，fixture 将 SQL schema/session 和 MTL 对齐到同一租户。
    sys_tenant_id_ = TEST_PREPARE_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, session_info_.switch_tenant(TEST_PREPARE_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS,
              session_info_.set_tenant(ObString::make_string("dls_tenant"),
                                       TEST_PREPARE_TENANT_ID));
    ASSERT_EQ(TEST_PREPARE_TENANT_ID, session_info_.get_priv_tenant_id());
    ASSERT_EQ(TEST_PREPARE_TENANT_ID, session_info_.get_effective_tenant_id());
    reset_query_ctx_schema_guard();
  }

  void prepare_database()
  {
    ObStmt *stmt = NULL;
    ASSERT_NO_FATAL_FAILURE(do_load_sql(
        ObString::make_string("create database dls_db"),
        stmt,
        JSON_FORMAT,
        OB_SUCCESS));
    stmt_factory_.destory();
    expr_factory_.destory();
    reset_query_ctx_schema_guard();

    uint64_t database_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, schema_guard_.get_database_id(
        sys_tenant_id_, ObString::make_string("dls_db"), database_id));
    ASSERT_NE(OB_INVALID_ID, database_id);
    ASSERT_EQ(OB_SUCCESS, session_info_.set_default_database(
        ObString::make_string("dls_db"), CS_TYPE_UTF8MB4_GENERAL_CI));
    session_info_.set_database_id(database_id);
  }

  void reset_query_ctx_schema_guard()
  {
    ASSERT_TRUE(NULL != stmt_factory_.get_query_ctx());
    stmt_factory_.get_query_ctx()->sql_schema_guard_.set_schema_guard(&schema_guard_);
  }

  int get_target_schema(const char *table_name, const ObTableSchema *&target_table_schema)
  {
    int ret = OB_SUCCESS;
    target_table_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schema(sys_tenant_id_,
                                               ObString::make_string("dls_db"),
                                               ObString::make_string(table_name),
                                               false,
                                               target_table_schema))) {
      LOG_WARN("fail to get target table schema for direct load stream", KR(ret), K(table_name));
    } else if (OB_ISNULL(target_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
    }
    return ret;
  }

  int add_current_user_schema(const ObPrivSet priv_set,
                              const uint64_t role_id = OB_INVALID_ID)
  {
    int ret = OB_SUCCESS;
    ObUserInfo user_info;
    user_info.set_tenant_id(session_info_.get_priv_tenant_id());
    user_info.set_user_id(session_info_.get_priv_user_id());
    user_info.set_schema_version(schema_version_);
    user_info.set_priv_set(priv_set);
    if (OB_INVALID_ID != role_id && OB_FAIL(user_info.add_role_id(role_id))) {
      LOG_WARN("fail to grant prepare test role", KR(ret), K(role_id));
    } else if (OB_FAIL(user_info.set_user_name(session_info_.get_user_name()))) {
      LOG_WARN("fail to set prepare test user name", KR(ret));
    } else if (OB_FAIL(user_info.set_host(session_info_.get_host_name()))) {
      LOG_WARN("fail to set prepare test user host", KR(ret));
    } else if (OB_FAIL(schema_service_->add_user_schema(user_info, schema_version_++))) {
      LOG_WARN("fail to add prepare test user schema", KR(ret), K(priv_set));
    }
    return ret;
  }

  int add_role_schema(const uint64_t role_id, const char *role_name)
  {
    int ret = OB_SUCCESS;
    ObUserInfo role_info;
    role_info.set_tenant_id(session_info_.get_priv_tenant_id());
    role_info.set_user_id(role_id);
    role_info.set_schema_version(schema_version_);
    role_info.set_priv_set(0);
    role_info.set_type(OB_ROLE);
    if (OB_FAIL(role_info.set_user_name(ObString::make_string(role_name)))) {
      LOG_WARN("fail to set prepare test role name", KR(ret), K(role_id));
    } else if (OB_FAIL(role_info.set_host(ObString::make_string("%")))) {
      LOG_WARN("fail to set prepare test role host", KR(ret), K(role_id));
    } else if (OB_FAIL(schema_service_->add_user_schema(role_info, schema_version_++))) {
      LOG_WARN("fail to add prepare test role schema", KR(ret), K(role_id));
    }
    return ret;
  }

  int use_restricted_user(const uint64_t role_id = OB_INVALID_ID)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(session_info_.set_user(ObString::make_string("stream_user"),
                                       ObString::make_string("%"),
                                       TEST_NO_INSERT_USER_ID))) {
      LOG_WARN("fail to set restricted prepare test user", KR(ret));
    } else {
      session_info_.set_priv_user_id(TEST_NO_INSERT_USER_ID);
      session_info_.set_user_priv_set(0);
      if (OB_FAIL(add_current_user_schema(0, role_id))) {
        LOG_WARN("fail to add restricted prepare test user schema", KR(ret), K(role_id));
      }
    }
    return ret;
  }

  int add_column_privilege(const uint64_t grantee_id,
                           const char *table_name,
                           const char *column_name,
                           const ObPrivSet priv_set,
                           const uint64_t priv_id)
  {
    int ret = OB_SUCCESS;
    ObColumnPriv column_priv;
    column_priv.set_tenant_id(session_info_.get_effective_tenant_id());
    column_priv.set_user_id(grantee_id);
    column_priv.set_schema_version(schema_version_++);
    column_priv.set_priv_set(priv_set);
    column_priv.set_priv_id(priv_id);
    if (OB_FAIL(column_priv.set_database_name("dls_db"))) {
      LOG_WARN("fail to set prepare test column privilege database", KR(ret));
    } else if (OB_FAIL(column_priv.set_table_name(table_name))) {
      LOG_WARN("fail to set prepare test column privilege table", KR(ret), K(table_name));
    } else if (OB_FAIL(column_priv.set_column_name(column_name))) {
      LOG_WARN("fail to set prepare test column privilege column", KR(ret), K(column_name));
    } else if (OB_FAIL(schema_service_->mgr_.priv_mgr_.add_column_priv(column_priv))) {
      LOG_WARN("fail to add prepare test column privilege", KR(ret), K(column_priv));
    }
    return ret;
  }

  int revoke_column_privilege(const uint64_t priv_id)
  {
    const ObColumnPrivIdKey priv_key(session_info_.get_effective_tenant_id(), priv_id);
    return schema_service_->mgr_.priv_mgr_.del_column_priv(priv_key);
  }

  int create_bound_stream(const char *table_name,
                          const int64_t target_schema_version_delta,
                          const bool seal_stream,
                          int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    observer::ObDirectLoadStreamCreateParam create_param;
    table::ObDirectLoadStreamQueueEntry queue_entry;
    stream_id = OB_INVALID_ID;
    if (OB_FAIL(get_target_schema(table_name, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_UNLIKELY(!GCTX.self_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid test self address", KR(ret), K(GCTX.self_addr()));
    } else {
      queue_entry.server_addr_ = GCTX.self_addr();
      queue_entry.queue_count_ = 1;
      create_param.tenant_id_ = session_info_.get_effective_tenant_id();
      create_param.user_id_ = session_info_.get_priv_user_id();
      create_param.table_id_ = target_table_schema->get_table_id();
      create_param.target_schema_version_ = target_table_schema->get_schema_version()
                                            + target_schema_version_delta;
      create_param.expire_ts_ = 6001;
      if (OB_FAIL(create_param.queue_entries_.push_back(queue_entry))) {
        LOG_WARN("fail to push direct load stream queue entry", KR(ret), K(queue_entry));
      } else if (OB_FAIL(observer::ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id))) {
        LOG_WARN("fail to create direct load stream", KR(ret), K(create_param));
      } else if (seal_stream
                 && OB_FAIL(observer::ObDirectLoadStreamService::seal_direct_load_stream(stream_id))) {
        LOG_WARN("fail to seal direct load stream", KR(ret), K(stream_id));
      }
    }
    return ret;
  }

  int create_multi_owner_bound_stream(const char *table_name,
                                      const bool seal_stream,
                                      int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    observer::ObDirectLoadStreamCreateParam create_param;
    table::ObDirectLoadStreamQueueEntry remote_entry;
    table::ObDirectLoadStreamQueueEntry local_entry;
    observer::ObDirectLoadStreamBufferQueueHandle local_handle;
    ObAddr remote_addr;
    stream_id = 9001;
    if (OB_FAIL(get_target_schema(table_name, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (!remote_addr.set_ip_addr("2.2.2.2", 9999)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set remote test address", KR(ret));
    } else {
      // 远端 owner 放在前面，验证本地 queue 的全局编号会正确跳过远端范围。
      remote_entry.server_addr_ = remote_addr;
      remote_entry.queue_count_ = 2;
      local_entry.server_addr_ = GCTX.self_addr();
      local_entry.queue_count_ = 1;
      create_param.tenant_id_ = session_info_.get_effective_tenant_id();
      create_param.user_id_ = session_info_.get_priv_user_id();
      create_param.table_id_ = target_table_schema->get_table_id();
      create_param.target_schema_version_ = target_table_schema->get_schema_version();
      create_param.expire_ts_ = 6001;
      if (OB_FAIL(create_param.queue_entries_.push_back(remote_entry))) {
        LOG_WARN("fail to push remote direct load stream queue entry", KR(ret), K(remote_entry));
      } else if (OB_FAIL(create_param.queue_entries_.push_back(local_entry))) {
        LOG_WARN("fail to push local direct load stream queue entry", KR(ret), K(local_entry));
      } else if (OB_FAIL(mtl_env_.get_mock_meta_store().create_stream(stream_id, create_param))) {
        LOG_WARN("fail to create multi-owner stream metadata", KR(ret), K(stream_id));
      } else if (OB_FAIL(get_queue_manager().create_queue(
                     observer::ObDirectLoadStreamBufferQueueKey(stream_id, 2), local_handle))) {
        LOG_WARN("fail to create local queue for multi-owner stream", KR(ret), K(stream_id));
      } else if (seal_stream
                 && OB_FAIL(mtl_env_.get_mock_meta_store().seal_stream(stream_id))) {
        LOG_WARN("fail to seal multi-owner stream", KR(ret), K(stream_id));
      }
    }
    return ret;
  }

  int build_prepare_param(const char *table_name,
                          const int64_t stream_id,
                          observer::ObDirectLoadStreamPrepareParam &param)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    param = observer::ObDirectLoadStreamPrepareParam();
    if (OB_FAIL(get_target_schema(table_name, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_UNLIKELY(!GCTX.self_addr().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid test self address", KR(ret), K(GCTX.self_addr()));
    } else {
      param.stream_id_ = stream_id;
      param.tenant_id_ = session_info_.get_effective_tenant_id();
      param.user_id_ = session_info_.get_priv_user_id();
      param.target_table_id_ = target_table_schema->get_table_id();
      param.owner_server_ = GCTX.self_addr();
      param.session_id_ = TEST_PREPARE_SESSION_ID;
      param.session_create_ts_ = TEST_PREPARE_SESSION_CREATE_TS;
    }
    return ret;
  }

  int get_stream_meta(const int64_t stream_id, observer::ObDirectLoadStreamMeta &meta)
  {
    return observer::ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta);
  }

  int build_direct_load_stream_scan_spec(const int64_t stream_id,
                                         ObTableScanSpec *&scan_spec)
  {
    int ret = OB_SUCCESS;
    void *buf = NULL;
    ObExternalFileFormat format;
    ObString format_str;
    scan_spec = NULL;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableScanSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate table scan spec", KR(ret));
    } else {
      scan_spec = new(buf) ObTableScanSpec(allocator_, PHY_TABLE_SCAN);
      format.format_type_ = ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
      format.direct_load_stream_format_.stream_id_ = stream_id;
      if (OB_FAIL(format.to_string_with_alloc(format_str, allocator_))) {
        LOG_WARN("fail to build direct load stream format string", KR(ret), K(stream_id));
      } else {
        scan_spec->tsc_ctdef_.scan_ctdef_.is_external_table_ = true;
        if (OB_FAIL(scan_spec->tsc_ctdef_.scan_ctdef_.external_file_format_str_.store_str(format_str))) {
          LOG_WARN("fail to store direct load stream format string", KR(ret), K(format_str));
        }
      }
    }
    return ret;
  }

  int build_direct_load_stream_source_plan(const int64_t stream_id,
                                           const ObTableSchema &target_table_schema,
                                           const char *target_column_name,
                                           ObPhysicalPlan &phy_plan,
                                           const char *need_priv_database_name = "dls_db",
                                           const char *need_priv_table_name = nullptr)
  {
    int ret = OB_SUCCESS;
    ObTableScanSpec *scan_spec = nullptr;
    ObStmtNeedPrivs stmt_need_privs(allocator_);
    const ObString table_name = nullptr == need_priv_table_name
                                    ? target_table_schema.get_table_name_str()
                                    : ObString::make_string(need_priv_table_name);
    ObNeedPriv need_priv(ObString::make_string(need_priv_database_name),
                         table_name,
                         OB_PRIV_TABLE_LEVEL,
                         OB_PRIV_INSERT,
                         target_table_schema.is_sys_table());
    if (OB_FAIL(build_direct_load_stream_scan_spec(stream_id, scan_spec))) {
      LOG_WARN("fail to build direct load stream SOURCE scan spec", KR(ret), K(stream_id));
    } else if (OB_FAIL(need_priv.columns_.push_back(
                           ObString::make_string(target_column_name)))) {
      LOG_WARN("fail to add direct load stream target privilege column", KR(ret), K(target_column_name));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
      LOG_WARN("fail to init direct load stream statement privileges", KR(ret));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
      LOG_WARN("fail to add direct load stream target INSERT privilege", KR(ret), K(need_priv));
    } else if (OB_FAIL(phy_plan.set_stmt_need_privs(stmt_need_privs))) {
      LOG_WARN("fail to set direct load stream statement privileges", KR(ret), K(stmt_need_privs));
    } else {
      phy_plan.set_root_op_spec(scan_spec);
      phy_plan.set_append_table_id(target_table_schema.get_table_id());
      phy_plan.set_px_dop(1);
      phy_plan.set_enable_inc_direct_load(true);
    }
    return ret;
  }

  int build_two_direct_load_stream_scan_plan(ObPhysicalPlan &phy_plan,
                                             const int64_t left_stream_id,
                                             const int64_t right_stream_id)
  {
    int ret = OB_SUCCESS;
    void *root_buf = NULL;
    ObOpSpec **children = NULL;
    ObOpSpec *root_spec = NULL;
    ObTableScanSpec *left_scan_spec = NULL;
    ObTableScanSpec *right_scan_spec = NULL;
    if (OB_ISNULL(root_buf = allocator_.alloc(sizeof(ObOpSpec)))
        || OB_ISNULL(children = static_cast<ObOpSpec **>(allocator_.alloc(sizeof(ObOpSpec *) * 2)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate direct load stream plan spec", KR(ret));
    } else {
      root_spec = new(root_buf) ObOpSpec(allocator_, PHY_MERGE_UNION);
      if (OB_FAIL(root_spec->set_children_pointer(children, 2))) {
        LOG_WARN("fail to set direct load stream plan children", KR(ret));
      } else if (OB_FAIL(build_direct_load_stream_scan_spec(left_stream_id, left_scan_spec))) {
        LOG_WARN("fail to build left direct load stream scan spec", KR(ret), K(left_stream_id));
      } else if (OB_FAIL(build_direct_load_stream_scan_spec(right_stream_id, right_scan_spec))) {
        LOG_WARN("fail to build right direct load stream scan spec", KR(ret), K(right_stream_id));
      } else if (OB_FAIL(root_spec->set_child(0, left_scan_spec))) {
        LOG_WARN("fail to set left direct load stream scan child", KR(ret));
      } else if (OB_FAIL(root_spec->set_child(1, right_scan_spec))) {
        LOG_WARN("fail to set right direct load stream scan child", KR(ret));
      } else {
        phy_plan.set_root_op_spec(root_spec);
      }
    }
    return ret;
  }

  bool result_set_needs_end_trans_callback(ObPhysicalPlan &phy_plan)
  {
    ObResultSet result(session_info_, allocator_);
    phy_plan.is_need_trans_ = true;
    phy_plan.set_plan_type(OB_PHY_PLAN_LOCAL);
    phy_plan.set_enable_append(true);
    result.set_stmt_type(stmt::T_INSERT);
    result.get_cache_obj_guard().cache_obj_ = &phy_plan;
    const bool need = result.need_end_trans_callback(false);
    result.get_cache_obj_guard().reset();
    return need;
  }

  void verify_stream_finished_and_queue_missing(const int64_t stream_id,
                                                 const int expected_result)
  {
    observer::ObDirectLoadStreamMeta meta;
    observer::ObDirectLoadStreamBufferQueueHandle handle;
    ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
    EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
    EXPECT_EQ(expected_result, meta.result_code_);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST,
              get_queue_manager().get_queue(
                observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  }

  void prepare_mock_direct_insert_after_px_commit_data(
      ObExecContext &exec_ctx, ObTableDirectInsertCtx &direct_insert_ctx)
  {
    ObIAllocator &allocator = exec_ctx.get_allocator();
    direct_insert_ctx.load_exec_ctx_ = OB_NEWx(observer::ObTableLoadExecCtx, &allocator);
    direct_insert_ctx.table_load_instance_ = OB_NEWx(observer::ObTableLoadInstance, &allocator);
    ASSERT_NE(nullptr, direct_insert_ctx.load_exec_ctx_);
    ASSERT_NE(nullptr, direct_insert_ctx.table_load_instance_);

    direct_insert_ctx.load_exec_ctx_->exec_ctx_ = &exec_ctx;
    direct_insert_ctx.table_load_instance_->execute_ctx_ = direct_insert_ctx.load_exec_ctx_;
    direct_insert_ctx.table_load_instance_->allocator_ = &allocator;
    direct_insert_ctx.table_load_instance_->stmt_ctx_.session_info_ = exec_ctx.get_my_session();
    direct_insert_ctx.table_load_instance_->stmt_ctx_.is_incremental_ = true;
    direct_insert_ctx.table_load_instance_->stmt_ctx_.use_insert_into_select_tx_ = true;
    direct_insert_ctx.table_load_instance_->stmt_ctx_.is_started_ = true;
    direct_insert_ctx.table_load_instance_->is_inited_ = true;
    direct_insert_ctx.is_inited_ = true;
    direct_insert_ctx.is_direct_ = true;
  }

  observer::ObDirectLoadStreamBufferQueueManager &get_queue_manager()
  {
    return mtl_env_.get_table_load_service().get_direct_load_stream_service().get_buffer_queue_manager();
  }

  int create_stream_through_pl_interface(const char *table_name,
                                         int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    ParamStore params((ObWrapperAllocator(&allocator_)));
    ObObjParam table_name_param;
    ObObjParam ttl_param;
    ObObj result;
    int status = OB_SUCCESS;
    pl::ObPLInterfaceService interface_service;
    ObString interface_name =
        ObString::make_string("DBMS_DIRECT_LOAD_STREAM_CREATE_TABLE");
    pl::PL_C_INTERFACE_t entry = nullptr;
    stream_id = OB_INVALID_ID;
    table_name_param.set_varchar(ObString::make_string(table_name));
    ttl_param.set_int(60);
    if (OB_FAIL(params.push_back(table_name_param))) {
      LOG_WARN("fail to push CREATE_TABLE name parameter", KR(ret));
    } else if (OB_FAIL(params.push_back(ttl_param))) {
      LOG_WARN("fail to push CREATE_TABLE ttl parameter", KR(ret));
    } else if (OB_FAIL(interface_service.init())) {
      LOG_WARN("fail to init PL interface service", KR(ret));
    } else if (OB_ISNULL(entry = interface_service.get_entry(interface_name))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get CREATE_TABLE interface", KR(ret), K(interface_name));
    } else {
      pl::ObPLExecCtx pl_exec_ctx(&allocator_,
                                  &allocator_,
                                  &exec_ctx_,
                                  &params,
                                  &result,
                                  &status,
                                  nullptr);
      // 本 fixture 不伪造 S8 的 alive-server/PX 生产 topology 依赖。
      auto *saved_sql_proxy = GCTX.sql_proxy_;
      GCTX.sql_proxy_ = nullptr;
      const int entry_ret = entry(pl_exec_ctx, params, result);
      GCTX.sql_proxy_ = saved_sql_proxy;
      if (OB_SUCCESS != (ret = entry_ret)) {
        LOG_WARN("fail to create direct load stream through PL interface", KR(ret), K(table_name));
      } else if (OB_UNLIKELY(!result.is_int())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected CREATE_TABLE result", KR(ret), K(result));
      } else {
        stream_id = result.get_int();
      }
    }
    return ret;
  }

  void expect_first_stream_candidate_has_no_side_effect()
  {
    const int64_t first_stream_id = 1;
    observer::ObDirectLoadStreamMeta meta;
    observer::ObDirectLoadStreamBufferQueueHandle queue_handle;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST,
              mtl_env_.get_mock_meta_store().get_stream(first_stream_id, meta));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST,
              get_queue_manager().get_queue(
                  observer::ObDirectLoadStreamBufferQueueKey(first_stream_id, 0),
                  queue_handle));
  }

  void expect_topology_failure_consumed_first_stream_id(const char *table_name)
  {
    int64_t probe_stream_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS,
              create_bound_stream(table_name, 0, false, probe_stream_id));
    EXPECT_EQ(2, probe_stream_id);
  }

  MockDirectLoadStreamPrepareMtlEnv mtl_env_;

  DISALLOW_COPY_AND_ASSIGN(TestDirectLoadStreamPrepareCtx);
};

// 正常表通过全部前置门禁后，应到达 S8 topology 依赖，并在缺少依赖时无副作用失败。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_passes_precreate_gates_before_topology)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
  expect_topology_failure_consumed_first_stream_id("dls_target");
}

// 用户缺少目标表 INSERT 权限时，CREATE 应在产生 stream 副作用前返回权限错误。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_without_insert_has_no_stream_side_effect)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, session_info_.set_user(ObString::make_string("stream_user"),
                                               ObString::make_string("%"),
                                               TEST_NO_INSERT_USER_ID));
  session_info_.set_priv_user_id(TEST_NO_INSERT_USER_ID);
  session_info_.set_user_priv_set(0);
  ASSERT_EQ(OB_SUCCESS, add_current_user_schema(0));
  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
}

// 受限用户拥有目标列 INSERT 权限时，应越过权限门禁并到达 S8 topology 依赖。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_user_column_insert_reaches_topology_gate)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c1",
                                 OB_PRIV_INSERT,
                                 TEST_C1_PRIV_ID));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
  expect_topology_failure_consumed_first_stream_id("dls_target");
}

// 角色列级 INSERT 权限仅在启用后生效，启用后应到达 S8 topology 依赖。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_role_column_insert_reaches_topology_gate)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user(TEST_INSERT_ROLE_ID));
  ASSERT_EQ(OB_SUCCESS, add_role_schema(TEST_INSERT_ROLE_ID, "stream_role"));
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_INSERT_ROLE_ID,
                                 "dls_target",
                                 "c1",
                                 OB_PRIV_INSERT,
                                 TEST_C1_PRIV_ID));
  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));

  ASSERT_EQ(OB_SUCCESS, session_info_.get_enable_role_array().push_back(TEST_INSERT_ROLE_ID));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
  expect_topology_failure_consumed_first_stream_id("dls_target");
}

// 仅有目标列 SELECT 权限不能替代 INSERT 权限，CREATE 应拒绝且不创建 stream。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_rejects_select_only_column_privilege)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c1",
                                 OB_PRIV_SELECT,
                                 TEST_SELECT_PRIV_ID));
  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE,
            create_stream_through_pl_interface("dls_target", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
}

// 视图不支持增量旁路导入，CREATE 应在创建 stream 前返回 OB_NOT_SUPPORTED。
TEST_F(TestDirectLoadStreamPrepareCtx, create_table_rejects_unsupported_view_without_stream)
{
  do_create_table("create table dls_base(c1 int)");
  ObStmt *stmt = nullptr;
  ASSERT_NO_FATAL_FAILURE(
      do_load_sql(ObString::make_string("create view dls_view as select * from dls_base"),
                  stmt,
                  JSON_FORMAT,
                  OB_SUCCESS));
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  EXPECT_EQ(OB_NOT_SUPPORTED, create_stream_through_pl_interface("dls_view", stream_id));
  EXPECT_FALSE(observer::is_valid_stream_id(stream_id));
  expect_first_stream_candidate_has_no_side_effect();
}

// 主路径：DIRECT LOAD SQL prepare 先 claim，再在 direct insert init/lock 后校验 schema 与本地 queue。
TEST_F(TestDirectLoadStreamPrepareCtx, claim_then_validate_schema_and_queue)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  ASSERT_TRUE(ctx.is_claimed());
  ASSERT_EQ(OB_SUCCESS, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_, get_queue_manager()));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
}

// 主路径：OPEN stream 也可以通过 claim 和锁后 schema/queue prepare。
TEST_F(TestDirectLoadStreamPrepareCtx, open_stream_claim_then_validate_schema_and_queue)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, false, stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  ASSERT_TRUE(ctx.is_claimed());
  ASSERT_EQ(OB_SUCCESS, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_, get_queue_manager()));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

// 多 Observer topology 中，QC 只校验本机 owner 的 queue；远端 queue 由对应 SQC 消费。
TEST_F(TestDirectLoadStreamPrepareCtx, multi_owner_prepare_validates_only_local_queues)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;
  observer::ObDirectLoadStreamBufferQueueHandle handle;

  ASSERT_EQ(OB_SUCCESS, create_multi_owner_bound_stream("dls_target", true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_queue_manager().get_queue(
      observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_queue_manager().get_queue(
      observer::ObDirectLoadStreamBufferQueueKey(stream_id, 1), handle));
  ASSERT_EQ(OB_SUCCESS, get_queue_manager().get_queue(
      observer::ObDirectLoadStreamBufferQueueKey(stream_id, 2), handle));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  ASSERT_EQ(OB_SUCCESS, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_, get_queue_manager()));
}

// claim 失败不能把 stream 改成 FINISHED，避免非 owner 的重试覆盖真实状态。
TEST_F(TestDirectLoadStreamPrepareCtx, claim_failure_does_not_finish_stream)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx owner_ctx;
  observer::ObDirectLoadStreamPrepareCtx failed_ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, owner_ctx.claim(param));
  ASSERT_EQ(OB_STATE_NOT_MATCH, failed_ctx.claim(param));
  EXPECT_FALSE(failed_ctx.is_claimed());
  EXPECT_EQ(OB_TIMEOUT, failed_ctx.finish_error_if_claimed(OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 元数据归属：prepare 参数的 tenant_id + user_id 必须与 stream metadata 一致。
TEST_F(TestDirectLoadStreamPrepareCtx, owner_mismatch_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  param.user_id_ += 100;
  ASSERT_EQ(OB_STATE_NOT_MATCH, ctx.claim(param));
  EXPECT_FALSE(ctx.is_claimed());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// schema version 必须严格等于 CREATE_TABLE 时记录的版本，漂移时在 claim 后锁后校验并收敛失败。
TEST_F(TestDirectLoadStreamPrepareCtx, schema_mismatch_is_finished_after_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 1, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  EXPECT_TRUE(ctx.is_claimed());
  ASSERT_EQ(OB_SCHEMA_EAGAIN, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_ - 1, get_queue_manager()));
  EXPECT_EQ(OB_SCHEMA_EAGAIN, ctx.finish_error_if_claimed(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_SCHEMA_EAGAIN, meta.result_code_);
}

// 锁后 fresh schema 与 metadata 一致时，loader 锁内记录的版本也必须严格一致。
TEST_F(TestDirectLoadStreamPrepareCtx, locked_loader_schema_mismatch_is_finished_after_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  EXPECT_TRUE(ctx.is_claimed());
  ASSERT_EQ(OB_SCHEMA_EAGAIN, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_ + 1, get_queue_manager()));
  EXPECT_EQ(OB_SCHEMA_EAGAIN, ctx.finish_error_if_claimed(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_SCHEMA_EAGAIN, meta.result_code_);
}

// queue_list 里的本地 queue 必须存在且可读，缺 queue 在 claim 后锁后校验并收敛失败。
TEST_F(TestDirectLoadStreamPrepareCtx, queue_missing_is_finished_after_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_queue_manager().cleanup_queue(
      observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0)));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(OB_SUCCESS, ctx.claim(param));
  EXPECT_TRUE(ctx.is_claimed());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ctx.validate_after_claim(
      schema_guard_, meta.target_schema_version_, get_queue_manager()));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ctx.finish_error_if_claimed(OB_ENTRY_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, meta.result_code_);
}

// 普通 direct insert 的 init 失败保留 direct 标记，由 ResultSet close 统一清理并决定 retry。
TEST_F(TestDirectLoadStreamPrepareCtx, ordinary_direct_insert_init_failure_defers_cleanup)
{
  ObPhysicalPlan phy_plan;
  ObTableDirectInsertCtx &direct_insert_ctx = exec_ctx_.get_table_direct_insert_ctx();
  int64_t claimed_direct_load_stream_id = OB_INVALID_ID;
  phy_plan.set_append_table_id(OB_INVALID_ID);
  phy_plan.set_px_dop(1);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  const int start_ret = ObTableDirectInsertService::start_direct_insert(
      exec_ctx_, phy_plan, OB_INVALID_ID);
  EXPECT_NE(OB_SUCCESS, start_ret);
  EXPECT_TRUE(direct_insert_ctx.get_is_direct());

  EXPECT_EQ(OB_SUCCESS,
            ObTableDirectInsertService::finish_direct_insert(
              exec_ctx_, phy_plan, false, claimed_direct_load_stream_id));
  EXPECT_FALSE(direct_insert_ctx.get_is_direct());
  EXPECT_FALSE(direct_insert_ctx.has_claimed_direct_load_stream());
  EXPECT_EQ(OB_INVALID_ID, claimed_direct_load_stream_id);
}

// SOURCE 执行时缺少目标 INSERT 权限，应在 claim 前拒绝并保持 stream 未被占用。
TEST_F(TestDirectLoadStreamPrepareCtx, missing_insert_privilege_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = nullptr;
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_NE(nullptr, target_table_schema);
  ASSERT_EQ(OB_SUCCESS,
            build_direct_load_stream_source_plan(stream_id,
                                                 *target_table_schema,
                                                 "c1",
                                                 phy_plan));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 大小写不敏感租户中，混合大小写的计划名称应匹配 schema 中的规范名称。
// 精确 c1 INSERT 权限通过 SOURCE 门禁后，应进入后续状态检查。
TEST_F(TestDirectLoadStreamPrepareCtx,
       mixed_case_exact_column_insert_privilege_passes_source_gate)
{
  do_create_table("create table dls_target(c1 int, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = nullptr;
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c1",
                                 OB_PRIV_INSERT,
                                 TEST_C1_PRIV_ID));
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_NE(nullptr, target_table_schema);
  ASSERT_EQ(OB_SUCCESS,
            build_direct_load_stream_source_plan(stream_id,
                                                 *target_table_schema,
                                                 "c1",
                                                 phy_plan,
                                                 "DLS_DB",
                                                 "DLS_TARGET"));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
  ASSERT_EQ(OB_SUCCESS, mtl_env_.get_table_load_service().stop());

  EXPECT_EQ(OB_IN_STOP_STATE,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS,
            mtl_env_.get_table_load_service()
                .get_direct_load_stream_service()
                .get_stream(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 仅持有非目标列 c2 的 INSERT 权限不能满足 c1 计划，SOURCE 应在 claim 前拒绝。
TEST_F(TestDirectLoadStreamPrepareCtx, wrong_column_insert_privilege_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = nullptr;
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c2",
                                 OB_PRIV_INSERT,
                                 TEST_C2_PRIV_ID));
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_NE(nullptr, target_table_schema);
  ASSERT_EQ(OB_SUCCESS,
            build_direct_load_stream_source_plan(stream_id,
                                                 *target_table_schema,
                                                 "c1",
                                                 phy_plan));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_ERR_NO_COLUMN_PRIVILEGE,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 计划生成后撤销目标列 INSERT 权限时，SOURCE 运行期复检应在 claim 前拒绝。
TEST_F(TestDirectLoadStreamPrepareCtx, revoked_target_column_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = nullptr;
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, use_restricted_user());
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c1",
                                 OB_PRIV_INSERT,
                                 TEST_C1_PRIV_ID));
  ASSERT_EQ(OB_SUCCESS,
            add_column_privilege(TEST_NO_INSERT_USER_ID,
                                 "dls_target",
                                 "c2",
                                 OB_PRIV_INSERT,
                                 TEST_C2_PRIV_ID));
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_NE(nullptr, target_table_schema);
  ASSERT_EQ(OB_SUCCESS,
            build_direct_load_stream_source_plan(stream_id,
                                                 *target_table_schema,
                                                 "c1",
                                                 phy_plan));
  ASSERT_EQ(OB_SUCCESS, revoke_column_privilege(TEST_C1_PRIV_ID));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_ERR_NO_COLUMN_PRIVILEGE,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// Stream SOURCE 依赖 incremental direct load 的 SQL tx/table lock；FULL direct load 必须在 claim 前拒绝。
TEST_F(TestDirectLoadStreamPrepareCtx, non_incremental_direct_load_stream_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = NULL;
  ObPhysicalPlan phy_plan;
  ObTableScanSpec *scan_spec = NULL;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_TRUE(NULL != target_table_schema);
  ASSERT_EQ(OB_SUCCESS, build_direct_load_stream_scan_spec(stream_id, scan_spec));
  phy_plan.set_root_op_spec(scan_spec);
  phy_plan.set_append_table_id(target_table_schema->get_table_id());
  phy_plan.set_px_dop(1);
  phy_plan.set_enable_inc_direct_load(false);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_NOT_SUPPORTED,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// claim id 跨 prepare/table-load 资源 destroy 保持 sticky，等待事务结束后的收尾。
TEST_F(TestDirectLoadStreamPrepareCtx, direct_insert_ctx_retains_claim_for_transaction_end)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObArenaAllocator allocator("DLSPrep");
  ObTableDirectInsertCtx direct_insert_ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, direct_insert_ctx.claim_direct_load_stream(
      param, allocator));
  ASSERT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  direct_insert_ctx.destroy();
  EXPECT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  EXPECT_EQ(stream_id, direct_insert_ctx.get_claimed_direct_load_stream_id());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 缺少 reader 正常 EOF 证明时必须在 table-load finish/transaction commit 前失败。
TEST_F(TestDirectLoadStreamPrepareCtx, direct_insert_success_requires_reader_drain_proof)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObArenaAllocator allocator("DLSPrep");
  ObTableDirectInsertCtx &direct_insert_ctx = exec_ctx_.get_table_direct_insert_ctx();
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;
  int64_t claimed_direct_load_stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, direct_insert_ctx.claim_direct_load_stream(
      param, allocator));
  ASSERT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            ObTableDirectInsertService::finish_direct_insert(
              exec_ctx_, phy_plan, true, claimed_direct_load_stream_id));
  EXPECT_EQ(stream_id, claimed_direct_load_stream_id);
  EXPECT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// 执行失败时必须导出已 claim Stream id，且不能在 transaction-end 前提前写终态。
TEST_F(TestDirectLoadStreamPrepareCtx, direct_insert_failure_exports_claimed_stream_without_early_completion)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObArenaAllocator allocator("DLSPrep");
  ObTableDirectInsertCtx &direct_insert_ctx = exec_ctx_.get_table_direct_insert_ctx();
  ObPhysicalPlan phy_plan;
  observer::ObDirectLoadStreamMeta meta;
  int64_t claimed_direct_load_stream_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, direct_insert_ctx.claim_direct_load_stream(
      param, allocator));
  ASSERT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  EXPECT_EQ(OB_SUCCESS,
            ObTableDirectInsertService::finish_direct_insert(
              exec_ctx_, phy_plan, false, claimed_direct_load_stream_id));
  EXPECT_EQ(stream_id, claimed_direct_load_stream_id);
  EXPECT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// claim 后的失败已经写入 FINISHED，retry policy 必须保留原错误并禁止再次执行 statement。
TEST_F(TestDirectLoadStreamPrepareCtx, claimed_stream_disables_retry_paths)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObResultSet result(session_info_, allocator_);
  ObTableDirectInsertCtx &direct_insert_ctx =
      result.get_exec_context().get_table_direct_insert_ctx();
  observer::ObQueryRetryCtrl retry_ctrl;
  observer::ObDirectLoadStreamMeta meta;
  int retry_init_ret = OB_SUCCESS;
  int client_ret = OB_SUCCESS;
  int quick_retry_err = OB_TRANSACTION_SET_VIOLATION;
  int64_t quick_retry_times = 0;

  result.get_exec_context().set_my_session(&session_info_);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_SUCCESS, direct_insert_ctx.claim_direct_load_stream(
      param, result.get_exec_context().get_allocator()));
  EXPECT_FALSE(result.transaction_set_violation_and_retry(
      quick_retry_err, quick_retry_times));
  EXPECT_EQ(0, quick_retry_times);
  EXPECT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());

  direct_insert_ctx.destroy();
  ASSERT_TRUE(direct_insert_ctx.has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  ASSERT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);

  retry_init_ret = observer::ObQueryRetryCtrl::init();
  ASSERT_TRUE(OB_SUCCESS == retry_init_ret || OB_INIT_TWICE == retry_init_ret);
  retry_ctrl.test_and_save_retry_state(
      GCTX, sql_ctx_, result, OB_SCHEMA_EAGAIN, client_ret);
  EXPECT_EQ(observer::RETRY_TYPE_NONE, retry_ctrl.get_retry_type());
  EXPECT_EQ(OB_SCHEMA_EAGAIN, client_ret);
  ASSERT_EQ(OB_SUCCESS,
            observer::ObDirectLoadStreamService::finish_and_cleanup_direct_load_stream(
              stream_id, OB_SCHEMA_EAGAIN));
}

// 不支持的 SQL 形态必须在 claim 前拒绝，不能把用户误用的 INSERT OVERWRITE 收敛成 stream FINISHED(error)。
TEST_F(TestDirectLoadStreamPrepareCtx, insert_overwrite_stream_is_rejected_before_claim)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  const ObTableSchema *target_table_schema = NULL;
  ObPhysicalPlan phy_plan;
  ObTableScanSpec *scan_spec = NULL;
  ObExternalFileFormat format;
  ObString format_str;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", target_table_schema));
  ASSERT_TRUE(NULL != target_table_schema);
  void *buf = allocator_.alloc(sizeof(ObTableScanSpec));
  ASSERT_TRUE(NULL != buf);
  scan_spec = new(buf) ObTableScanSpec(allocator_, PHY_TABLE_SCAN);
  format.format_type_ = ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
  format.direct_load_stream_format_.stream_id_ = stream_id;
  ASSERT_EQ(OB_SUCCESS, format.to_string_with_alloc(format_str, allocator_));
  scan_spec->tsc_ctdef_.scan_ctdef_.is_external_table_ = true;
  ASSERT_EQ(OB_SUCCESS, scan_spec->tsc_ctdef_.scan_ctdef_.external_file_format_str_.store_str(format_str));
  phy_plan.set_root_op_spec(scan_spec);
  phy_plan.set_is_insert_overwrite(true);
  phy_plan.set_enable_inc_direct_load(true);
  phy_plan.set_append_table_id(target_table_schema->get_table_id());
  phy_plan.set_px_dop(1);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_NOT_SUPPORTED,
            ObTableDirectInsertService::start_direct_insert(exec_ctx_, phy_plan, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  EXPECT_FALSE(meta.owner_server_.is_valid());
  EXPECT_EQ(OB_INVALID_ID, meta.session_id_);
  EXPECT_EQ(OB_SUCCESS, meta.result_code_);
}

// plan 边界：一个 physical plan 内不能包含多个 DIRECT_LOAD_STREAM SOURCE。
TEST_F(TestDirectLoadStreamPrepareCtx, multiple_stream_sources_are_rejected_in_plan_scan)
{
  ObPhysicalPlan phy_plan;
  int64_t stream_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, build_two_direct_load_stream_scan_plan(phy_plan, 1, 2));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));

  EXPECT_EQ(OB_NOT_SUPPORTED,
            ObTableDirectInsertService::check_direct_load_stream_source_plan(
              phy_plan, allocator_, stream_id));
  EXPECT_FALSE(exec_ctx_.get_table_direct_insert_ctx().has_claimed_direct_load_stream());
}

// ResultSet driver selector 必须让 Stream SOURCE 同步提交，同时保留普通 direct insert 的异步路径。
TEST_F(TestDirectLoadStreamPrepareCtx, result_set_driver_selector_distinguishes_stream_source)
{
  ObPhysicalPlan ordinary_plan;
  ObPhysicalPlan stream_plan;
  ObPhysicalPlan invalid_stream_plan;
  ObTableScanSpec *scan_spec = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_direct_load_stream_scan_spec(10086, scan_spec));
  ASSERT_NE(nullptr, scan_spec);
  stream_plan.set_root_op_spec(scan_spec);
  ASSERT_EQ(OB_SUCCESS, build_two_direct_load_stream_scan_plan(invalid_stream_plan, 1, 2));

  EXPECT_TRUE(result_set_needs_end_trans_callback(ordinary_plan));
  EXPECT_FALSE(result_set_needs_end_trans_callback(stream_plan));
  // 扫描失败时 driver 必须 fail-safe 选择同步 response owner，避免客户端无人回包。
  EXPECT_FALSE(result_set_needs_end_trans_callback(invalid_stream_plan));
}

// 本 fixture 的 SQL proxy 不带 inner connection pool；验证同事务成功收尾无法启动时，
// ResultSet close 保留 OB_NOT_INIT，并将 stream 写成失败终态后删除 queue。
TEST_F(TestDirectLoadStreamPrepareCtx, result_set_close_without_inner_sql_pool_finishes_failure)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObResultSet result(session_info_, allocator_);
  ObPhysicalPlan phy_plan;
  ObExecuteResult execute_result;
  ObExecContext &result_exec_ctx = result.get_exec_context();
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObTableDirectInsertCtx &direct_insert_ctx = result_exec_ctx.get_table_direct_insert_ctx();

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  result_exec_ctx.set_my_session(&session_info_);
  result_exec_ctx.set_sql_ctx(&sql_ctx_);
  ASSERT_EQ(OB_SUCCESS, result_exec_ctx.create_physical_plan_ctx());
  ASSERT_NE(nullptr, plan_ctx = result_exec_ctx.get_physical_plan_ctx());
  plan_ctx->set_is_direct_insert_plan(true);
  ASSERT_EQ(OB_SUCCESS,
            direct_insert_ctx.claim_direct_load_stream(param, result_exec_ctx.get_allocator()));
  phy_plan.set_plan_type(OB_PHY_PLAN_LOCAL);
  phy_plan.is_need_trans_ = true;
  // 这里只 mock PX 已完成 px_commit_data 后、进入 ResultSet close 的状态；真实 table-load、
  // transaction commit 和同事务 FINISHED(0) 由 Stream obtest 成功路径覆盖。
  ASSERT_NO_FATAL_FAILURE(
      prepare_mock_direct_insert_after_px_commit_data(result_exec_ctx, direct_insert_ctx));
  {
    ObArenaAllocator chunk_allocator("DLSDrain");
    ObString chunk;
    observer::ObDirectLoadStreamBufferQueueHandle handle;
    ASSERT_EQ(OB_SUCCESS, get_queue_manager().get_queue(
                            observer::ObDirectLoadStreamBufferQueueKey(stream_id, 0), handle));
    ASSERT_EQ(OB_ITER_END, handle.get_queue()->get_next_chunk(chunk_allocator, chunk));
  }

  result.set_stmt_type(stmt::T_INSERT);
  result.exec_result_ = &execute_result;
  result.errcode_ = OB_ITER_END;
  result.get_cache_obj_guard().cache_obj_ = &phy_plan;
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_EQ(nullptr, GCTX.sql_proxy_->get_pool());
  EXPECT_EQ(OB_NOT_INIT, result.close());
  result.get_cache_obj_guard().reset();

  verify_stream_finished_and_queue_missing(stream_id, OB_NOT_INIT);
}

// 验证完整 ResultSet close 在 1062 回滚后保留 SQL 首错，同时写失败终态并删除 queue。
TEST_F(TestDirectLoadStreamPrepareCtx, result_set_close_error_finishes_and_cleans_stream)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  ObResultSet result(session_info_, allocator_);
  ObPhysicalPlan phy_plan;
  ObExecuteResult execute_result;
  ObExecContext &result_exec_ctx = result.get_exec_context();
  ObPhysicalPlanCtx *plan_ctx = nullptr;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, true, stream_id));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  result_exec_ctx.set_my_session(&session_info_);
  result_exec_ctx.set_sql_ctx(&sql_ctx_);
  ASSERT_EQ(OB_SUCCESS, result_exec_ctx.create_physical_plan_ctx());
  ASSERT_NE(nullptr, plan_ctx = result_exec_ctx.get_physical_plan_ctx());
  plan_ctx->set_is_direct_insert_plan(true);
  ASSERT_EQ(OB_SUCCESS,
            result_exec_ctx.get_table_direct_insert_ctx().claim_direct_load_stream(
              param, result_exec_ctx.get_allocator()));

  phy_plan.set_plan_type(OB_PHY_PLAN_LOCAL);
  result.set_stmt_type(stmt::T_INSERT);
  result.exec_result_ = &execute_result;
  result.errcode_ = OB_ERR_PRIMARY_KEY_DUPLICATE;
  result.get_cache_obj_guard().cache_obj_ = &phy_plan;
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, result.close());
  result.get_cache_obj_guard().reset();

  verify_stream_finished_and_queue_missing(stream_id, OB_ERR_PRIMARY_KEY_DUPLICATE);
}

// already FINISHED 在 claim 前拒绝，后续错误收敛不能覆盖原始结果。
TEST_F(TestDirectLoadStreamPrepareCtx, already_finished_is_rejected_without_overwrite)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  observer::ObDirectLoadStreamPrepareParam param;
  observer::ObDirectLoadStreamPrepareCtx ctx;
  observer::ObDirectLoadStreamMeta meta;

  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", 0, false, stream_id));
  ASSERT_EQ(OB_SUCCESS, observer::ObDirectLoadStreamService::finish_direct_load_stream(
      stream_id, OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, build_prepare_param("dls_target", stream_id, param));
  ASSERT_EQ(OB_STATE_NOT_MATCH, ctx.claim(param));
  EXPECT_FALSE(ctx.is_claimed());
  EXPECT_EQ(OB_SCHEMA_EAGAIN, ctx.finish_error_if_claimed(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(OB_SUCCESS, get_stream_meta(stream_id, meta));
  EXPECT_EQ(observer::DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(OB_TIMEOUT, meta.result_code_);
}

} // namespace test

int main(int argc, char **argv)
{
  system("rm -f test_direct_load_stream_prepare_ctx.log*");
  OB_LOGGER.set_file_name("test_direct_load_stream_prepare_ctx.log", true);
  OB_LOGGER.set_log_level("INFO");
  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

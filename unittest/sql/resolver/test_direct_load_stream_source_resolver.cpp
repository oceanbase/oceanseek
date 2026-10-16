/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/test_sql_utils.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "sql/privilege_check/ob_privilege_check.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/parser/ob_parser.h"
#include "sql/optimizer/ob_log_plan.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/test_direct_load_stream_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "share/ob_get_compat_mode.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/direct_load/ob_table_direct_insert_op.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/engine/ob_physical_plan.h"
#include "share/ob_server_struct.h"
#include "share/table/ob_direct_load_stream_queue_list.h"
#include "../../observer/table_load/test_direct_load_stream_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test
{
namespace
{

const uint64_t TEST_RESOLVER_TENANT_ID = 1002;

class MockPhysicalPlanGuard
{
public:
  explicit MockPhysicalPlanGuard(ObPhysicalPlan *&phy_plan)
    : phy_plan_(phy_plan)
  {
  }
  ~MockPhysicalPlanGuard()
  {
    if (OB_NOT_NULL(phy_plan_)) {
      MockCacheObjectFactory::free(phy_plan_);
      phy_plan_ = nullptr;
    }
  }

private:
  ObPhysicalPlan *&phy_plan_;

  DISALLOW_COPY_AND_ASSIGN(MockPhysicalPlanGuard);
};

class MockDirectLoadStreamResolverMtlEnv
{
public:
  MockDirectLoadStreamResolverMtlEnv()
    : tenant_base_(TEST_RESOLVER_TENANT_ID),
      table_load_service_(TEST_RESOLVER_TENANT_ID),
      mock_meta_store_(TEST_RESOLVER_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamResolverMtlEnv()
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
          TEST_RESOLVER_TENANT_ID);
      alloc_ret = lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
          TEST_RESOLVER_TENANT_ID);
      if (OB_UNLIKELY(OB_SUCCESS != alloc_ret && OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(lib::ObMallocAllocator::get_instance()->set_tenant_limit(
                     TEST_RESOLVER_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_RESOLVER_TENANT_ID));
      } else if (OB_ISNULL(lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
                           TEST_RESOLVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant ctx allocator is null", KR(ret), K(TEST_RESOLVER_TENANT_ID));
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
          LOG_WARN("fail to init resolver mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.set_meta_store_for_test(mock_meta_store_))) {
          LOG_WARN("fail to inject resolver mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.enable_local_stream_id_generator_for_test())) {
          LOG_WARN("fail to enable resolver test stream id generator", KR(ret));
        } else if (OB_FAIL(stream_service.enable_injected_queue_topology_for_test())) {
          LOG_WARN("fail to enable resolver test stream topology injection", KR(ret));
        } else if (OB_FAIL(stream_service.get_buffer_queue_manager()
                               .set_mock_queue_store_factory_for_test(
                                   observer::get_direct_load_stream_mock_store_factory()))) {
          LOG_WARN("fail to inject resolver mock queue store factory", KR(ret));
        } else if (OB_FAIL(table_load_service_.init())) {
          LOG_WARN("fail to init resolver table load service", KR(ret));
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
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_RESOLVER_TENANT_ID);
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
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_RESOLVER_TENANT_ID);
      created_allocator_ = false;
    }
  }
  observer::ObTableLoadService &get_table_load_service() { return table_load_service_; }

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

struct DirectLoadStreamPhysicalPlanInfo
{
  DirectLoadStreamPhysicalPlanInfo()
  {
    reset();
  }
  void reset()
  {
    root_type_ = PHY_INVALID;
    has_table_direct_insert_ = false;
    has_px_repart_transmit_ = false;
    direct_insert_part_id_index_ = OB_INVALID_INDEX_INT64;
    has_pdml_partition_id_expr_ = false;
    enable_append_ = false;
    enable_inc_direct_load_ = false;
  }
  ObPhyOperatorType root_type_;
  bool has_table_direct_insert_;
  bool has_px_repart_transmit_;
  int64_t direct_insert_part_id_index_;
  bool has_pdml_partition_id_expr_;
  bool enable_append_;
  bool enable_inc_direct_load_;
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

const ObTableDirectInsertSpec *find_table_direct_insert_spec(const ObOpSpec &spec)
{
  const ObTableDirectInsertSpec *direct_insert_spec = nullptr;
  if (PHY_TABLE_DIRECT_INSERT == spec.get_type()) {
    direct_insert_spec = static_cast<const ObTableDirectInsertSpec *>(&spec);
  }
  for (uint32_t i = 0;
       nullptr == direct_insert_spec && i < spec.get_child_cnt(); ++i) {
    const ObOpSpec *child = spec.get_child(i);
    if (nullptr != child) {
      direct_insert_spec = find_table_direct_insert_spec(*child);
    }
  }
  return direct_insert_spec;
}

} // namespace

class TestDirectLoadStreamSourceResolver : public TestOptimizerUtils
{
public:
  TestDirectLoadStreamSourceResolver() = default;
  virtual ~TestDirectLoadStreamSourceResolver() = default;
  virtual void SetUp()
  {
    schema_file_path_[0] = 0;
    init();
    prepare_user_tenant();
    ASSERT_EQ(OB_SUCCESS, set_direct_load_allow_fallback(true));
    ASSERT_EQ(OB_SUCCESS, mtl_env_.init());
    prepare_database();
  }
  virtual void TearDown()
  {
    mtl_env_.destroy();
    EXPECT_EQ(OB_SUCCESS, set_direct_load_allow_fallback(true));
    EXPECT_EQ(OB_SUCCESS, omt::ObTenantTimezoneMgr::get_instance().del_tenant_timezone(
                              TEST_RESOLVER_TENANT_ID));
    TestSqlUtils::destroy();
  }
private:
  void prepare_user_tenant()
  {
    ObTenantSchema tenant_schema;
    ObSysVariableSchema sys_variable;
    ObUserInfo login_user;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    int compat_init_ret = OB_SUCCESS;
    tenant_schema.set_tenant_id(TEST_RESOLVER_TENANT_ID);
    tenant_schema.set_schema_version(schema_version_);
    tenant_schema.set_locality("auto_locality_strategy");
    ASSERT_EQ(OB_SUCCESS, tenant_schema.add_zone("zone"));
    ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("dls_tenant"));
    ASSERT_EQ(OB_SUCCESS, schema_service_->add_tenant_schema(tenant_schema, schema_version_++));

    sys_variable.set_tenant_id(TEST_RESOLVER_TENANT_ID);
    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    sys_variable.set_schema_version(schema_version_);
    ASSERT_EQ(OB_SUCCESS,
              schema_service_->add_sys_variable_schema(sys_variable, schema_version_++));

    login_user.set_tenant_id(TEST_RESOLVER_TENANT_ID);
    login_user.set_user_id(session_info_.get_priv_user_id());
    login_user.set_schema_version(schema_version_);
    login_user.set_priv_set(session_info_.get_user_priv_set());
    ASSERT_EQ(OB_SUCCESS, login_user.set_user_name(OB_SYS_USER_NAME));
    ASSERT_EQ(OB_SUCCESS, login_user.set_host(OB_SYS_HOST_NAME));
    ASSERT_EQ(OB_SUCCESS,
              schema_service_->add_user_schema(login_user, schema_version_++));

    ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));

    ASSERT_EQ(OB_SUCCESS,
              omt::ObTenantConfigMgr::get_instance().add_tenant_config(
                  TEST_RESOLVER_TENANT_ID));
    compat_init_ret = share::ObCompatModeGetter::instance().init(nullptr);
    ASSERT_TRUE(OB_SUCCESS == compat_init_ret || OB_INIT_TWICE == compat_init_ret);
    ASSERT_EQ(OB_SUCCESS, share::ObCompatModeGetter::instance().set_tenant_compat_mode(
                              TEST_RESOLVER_TENANT_ID, compat_mode));

    ASSERT_EQ(OB_SUCCESS, omt::ObTenantTimezoneMgr::get_instance().add_tenant_timezone(
                              TEST_RESOLVER_TENANT_ID));

    // Stream service 只属于 user tenant，fixture 将 SQL schema/session 和 MTL 对齐到同一租户。
    sys_tenant_id_ = TEST_RESOLVER_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, session_info_.switch_tenant(TEST_RESOLVER_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS,
              session_info_.set_tenant(ObString::make_string("dls_tenant"),
                                       TEST_RESOLVER_TENANT_ID));
    ASSERT_EQ(TEST_RESOLVER_TENANT_ID, session_info_.get_priv_tenant_id());
    ASSERT_EQ(TEST_RESOLVER_TENANT_ID, session_info_.get_effective_tenant_id());
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

  // SOURCE 会通过 ObSqlSchemaGuard 注册 mocked external table，resolver/optimizer
  // 需要从 query ctx 查到它。
  void reset_query_ctx_schema_guard()
  {
    ASSERT_TRUE(NULL != stmt_factory_.get_query_ctx());
    stmt_factory_.get_query_ctx()->sql_schema_guard_.set_schema_guard(&schema_guard_);
  }

  int publish_table_schema(ObTableSchema &table_schema)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(add_table_schema(table_schema))) {
      LOG_WARN("fail to publish updated table schema", KR(ret), K(table_schema));
    } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard_))) {
      LOG_WARN("fail to refresh schema guard", KR(ret));
    }
    return ret;
  }

  int publish_generated_column(const char *table_name)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    ObTableSchema updated_table_schema;
    ObColumnSchemaV2 generated_column_schema;
    ObObj generated_expr;
    if (OB_FAIL(schema_guard_.get_table_schema(
            sys_tenant_id_, ObString::make_string("dls_db"),
            ObString::make_string(table_name), false, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_ISNULL(target_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
    } else if (OB_FAIL(updated_table_schema.assign(*target_table_schema))) {
      LOG_WARN("fail to copy target table schema", KR(ret));
    } else {
      generated_column_schema.set_table_id(updated_table_schema.get_table_id());
      generated_column_schema.set_column_id(
          updated_table_schema.get_max_used_column_id() + 1);
      generated_column_schema.set_data_type(ObIntType);
      generated_column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      generated_column_schema.set_data_length(
          static_cast<int64_t>(sizeof(int64_t)));
      generated_column_schema.set_rowkey_position(0);
      generated_column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
      generated_expr.set_varchar(ObString::make_string("c1"));
      generated_expr.set_collation_type(ObCharset::get_system_collation());
      generated_expr.set_collation_level(CS_LEVEL_COERCIBLE);
      if (OB_FAIL(generated_column_schema.set_column_name("c2"))) {
        LOG_WARN("fail to set generated column name", KR(ret));
      } else if (OB_FAIL(generated_column_schema.set_cur_default_value(
                     generated_expr, false))) {
        LOG_WARN("fail to set generated column expression", KR(ret));
      } else if (OB_FAIL(updated_table_schema.add_column(generated_column_schema))) {
        LOG_WARN("fail to add generated column", KR(ret), K(generated_column_schema));
      } else {
        updated_table_schema.set_max_used_column_id(
            generated_column_schema.get_column_id());
      }
      if (OB_SUCC(ret) && OB_FAIL(publish_table_schema(updated_table_schema))) {
        LOG_WARN("fail to publish generated column schema", KR(ret));
      }
    }
    return ret;
  }

  int publish_force_rls(const char *table_name)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    ObTableSchema updated_table_schema;
    if (OB_FAIL(schema_guard_.get_table_schema(
            sys_tenant_id_, ObString::make_string("dls_db"),
            ObString::make_string(table_name), false, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_ISNULL(target_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
    } else if (OB_FAIL(updated_table_schema.assign(*target_table_schema))) {
      LOG_WARN("fail to copy target table schema", KR(ret));
    } else {
      updated_table_schema.set_row_level_security(true);
      updated_table_schema.set_force_row_level_security(true);
      if (OB_FAIL(publish_table_schema(updated_table_schema))) {
        LOG_WARN("fail to publish force RLS schema", KR(ret));
      }
    }
    return ret;
  }

  int republish_table_schema(const char *table_name)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    ObTableSchema updated_table_schema;
    if (OB_FAIL(schema_guard_.get_table_schema(
            sys_tenant_id_, ObString::make_string("dls_db"),
            ObString::make_string(table_name), false, target_table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_ISNULL(target_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
    } else if (OB_FAIL(updated_table_schema.assign(*target_table_schema))) {
      LOG_WARN("fail to copy target table schema", KR(ret));
    } else if (OB_FAIL(publish_table_schema(updated_table_schema))) {
      LOG_WARN("fail to republish target table schema", KR(ret));
    }
    return ret;
  }

  int set_direct_load_allow_fallback(const bool allow_fallback)
  {
    int ret = OB_SUCCESS;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(TEST_RESOLVER_TENANT_ID));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(TEST_RESOLVER_TENANT_ID));
    } else {
      tenant_config->direct_load_allow_fallback = allow_fallback;
    }
    return ret;
  }

  void verify_nested_schema_drift_is_deferred(const bool use_trigger,
                                               const bool allow_fallback)
  {
    do_create_table("create table dls_target(c1 int primary key)");
    reset_query_ctx_schema_guard();
    int64_t stream_id = OB_INVALID_ID;
    ObSqlString sql_buf;
    ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
    ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
    ASSERT_EQ(OB_SUCCESS, republish_table_schema("dls_target"));
    ASSERT_EQ(OB_SUCCESS, set_direct_load_allow_fallback(allow_fallback));
    reset_query_ctx_schema_guard();
    ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
        "insert /*+ enable_parallel_dml parallel(2) no_gather_optimizer_statistics */ "
        "into dls_target(c1) "
        "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

    ObResultSet result(session_info_, allocator_);
    ObLogPlan *logical_plan = NULL;
    bool is_select = true;
    ObString sql = sql_buf.string();
    ASSERT_EQ(OB_SUCCESS,
              generate_direct_load_stream_logical_plan(
                  result, sql, logical_plan, is_select, !use_trigger, use_trigger));
    ASSERT_TRUE(NULL != logical_plan);
    ASSERT_TRUE(NULL != logical_plan->get_stmt());
    ASSERT_TRUE(static_cast<const ObInsertStmt *>(logical_plan->get_stmt())
                  ->has_direct_load_stream_schema_drift());
    ASSERT_TRUE(NULL != optctx_);
    ASSERT_TRUE(optctx_->can_use_pdml());
    ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().can_use_direct_load());
    if (use_trigger) {
      ASSERT_TRUE(optctx_->has_trigger());
      ASSERT_FALSE(logical_plan->get_stmt()->get_query_ctx()->has_pl_udf_);
    } else {
      ASSERT_TRUE(optctx_->has_fk());
    }
    ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().use_direct_load());
    ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().is_inc_direct_load());
  }

  int create_bound_stream(const char *table_name,
                          int64_t &stream_id,
                          const int64_t target_schema_version_delta = 0)
  {
    int ret = OB_SUCCESS;
    const ObTableSchema *target_table_schema = NULL;
    observer::ObDirectLoadStreamCreateParam create_param;
    table::ObDirectLoadStreamQueueEntry queue_entry;
    stream_id = OB_INVALID_ID;
    if (OB_FAIL(schema_guard_.get_table_schema(sys_tenant_id_,
                                               ObString::make_string("dls_db"),
                                               ObString::make_string(table_name),
                                               false,
                                               target_table_schema))) {
      LOG_WARN("fail to get target table schema for direct load stream", KR(ret), K(table_name));
    } else if (OB_ISNULL(target_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
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
      }
    }
    return ret;
  }

  // 只在 S3 focused 测试内补齐 plan generation 需要的最小执行上下文，避免改公共测试工具。
  int generate_direct_load_stream_logical_plan(
      ObResultSet &result,
      ObString &sql,
      ObLogPlan *&logical_plan,
      bool &is_select,
      const bool mock_has_fk = false,
      const bool mock_has_trigger = false)
  {
    ObPhysicalPlan *phy_plan = nullptr;
    MockPhysicalPlanGuard phy_plan_guard(phy_plan);
    return generate_direct_load_stream_logical_plan(result,
                                                    sql,
                                                    logical_plan,
                                                    is_select,
                                                    phy_plan,
                                                    nullptr,
                                                    nullptr,
                                                    mock_has_fk,
                                                    mock_has_trigger);
  }

  int generate_direct_load_stream_logical_plan(
      ObResultSet &result,
      ObString &sql,
      ObLogPlan *&logical_plan,
      bool &is_select,
      ObPhysicalPlan *&phy_plan,
      share::schema::ObStmtNeedPrivs *stmt_need_privs = nullptr,
      share::schema::ObStmtOraNeedPrivs *stmt_ora_need_privs = nullptr,
      const bool mock_has_fk = false,
      const bool mock_has_trigger = false)
  {
    UNUSED(result);
    int ret = OB_SUCCESS;
    phy_plan = nullptr;
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);

    ObParser parser(allocator_, SMO_DEFAULT);
    ParseResult parse_result;
    ParamStore &param_store = exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
    param_store.reuse();

    ObStmt *basic_stmt = NULL;
    ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
    const ObStmt *old_cur_stmt = exec_ctx_.get_sql_ctx()->cur_stmt_;
    ObAddr addr;
    ObAddrWithSeq old_self_addr_seq = GCTX.self_addr_seq_;
    addr.set_ip_addr("1.1.1.1", 8888);
    GCTX.self_addr_seq_.set_addr(addr);

    session_info_.set_user_session();
    expr_factory_.destory();
    stmt_factory_.destory();
    reset_query_ctx_schema_guard();
    ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();

    if (OB_ISNULL(query_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ctx is null", K(ret));
    } else if (OB_FAIL(MockCacheObjectFactory::alloc(
                   phy_plan, session_info_.get_effective_tenant_id()))) {
      LOG_WARN("failed to alloc physical plan", K(ret));
    } else if (OB_ISNULL(phy_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan is null", K(ret));
    } else if (OB_FAIL(parser.parse(sql, parse_result))) {
      LOG_WARN("failed to parse sql", K(ret));
    } else {
      ObSchemaChecker schema_checker;
      ObResolverParams resolver_ctx;
      resolver_ctx.allocator_ = &allocator_;
      resolver_ctx.schema_checker_ = &schema_checker;
      resolver_ctx.session_info_ = &session_info_;
      resolver_ctx.database_id_ = session_info_.get_database_id();
      resolver_ctx.param_list_ = &param_store;
      resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
      resolver_ctx.expr_factory_ = &expr_factory_;
      resolver_ctx.stmt_factory_ = &stmt_factory_;
      resolver_ctx.query_ctx_ = query_ctx;

      if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_))) {
        LOG_WARN("failed to init schema checker", K(ret));
      } else {
        query_ctx->set_questionmark_count(param_store.count());
        pctx->set_original_param_cnt(param_store.count());
        if (OB_FAIL(pctx->init_datum_param_store())) {
          LOG_WARN("failed to init datum param store", K(ret));
        } else {
          ObResolver resolver(resolver_ctx);
          if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                       *parse_result.result_tree_->children_[0],
                                       basic_stmt))) {
            LOG_WARN("failed to resolve sql", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(stmt_need_privs) != OB_NOT_NULL(stmt_ora_need_privs)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("statement privilege outputs must be provided together", K(ret));
      } else if (OB_NOT_NULL(stmt_need_privs)
                 && OB_FAIL(ObPrivilegeCheck::check_privilege_new(sql_ctx_,
                                                                  basic_stmt,
                                                                  *stmt_need_privs,
                                                                  *stmt_ora_need_privs))) {
        LOG_WARN("fail to collect direct load stream statement privileges", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObDMLStmt *stmt = dynamic_cast<ObDMLStmt *>(basic_stmt);
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("statement is not DML", K(ret), KPC(basic_stmt));
      } else {
        is_select = (stmt::T_SELECT == stmt->get_stmt_type());
        stmt->get_query_ctx()->set_sql_stmt(parse_result.input_sql_, parse_result.input_sql_len_);
        exec_ctx_.get_sql_ctx()->cur_stmt_ = stmt;
      }

      ObSchemaChecker schema_checker;
      if (OB_FAIL(schema_checker.init(stmt_factory_.get_query_ctx()->sql_schema_guard_))) {
        LOG_WARN("failed to init schema checker", K(ret));
      } else {
        ObTransformerCtx transformer_ctx;
        transformer_ctx.allocator_ = &allocator_;
        transformer_ctx.session_info_ = &session_info_;
        transformer_ctx.schema_checker_ = &schema_checker;
        transformer_ctx.exec_ctx_ = &exec_ctx_;
        transformer_ctx.expr_factory_ = &expr_factory_;
        transformer_ctx.stmt_factory_ = &stmt_factory_;
        transformer_ctx.sql_schema_guard_ = &stmt_factory_.get_query_ctx()->sql_schema_guard_;
        transformer_ctx.self_addr_ = &addr;
        transformer_ctx.merged_version_ = OB_MERGED_VERSION_INIT;
        transformer_ctx.phy_plan_ = phy_plan;

        ObTransformerImpl transformer(&transformer_ctx);
        if (OB_FAIL(transformer.transform(stmt))) {
          LOG_WARN("failed to transform statement", K(ret));
        } else {
          ObOptimizerContext *ctx_ptr = static_cast<ObOptimizerContext *>(
              allocator_.alloc(sizeof(ObOptimizerContext)));
          if (OB_ISNULL(ctx_ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc optimizer context", K(ret));
          } else {
            exec_ctx_.get_sql_ctx()->session_info_ = &session_info_;
            optctx_ = new(ctx_ptr) ObOptimizerContext(
                &session_info_, &exec_ctx_, &stmt_factory_.get_query_ctx()->sql_schema_guard_,
                NULL, static_cast<ObIAllocator &>(allocator_), &param_store, addr, NULL,
                stmt->get_query_ctx()->get_global_hint(), expr_factory_, stmt, false,
                stmt_factory_.get_query_ctx());
            optctx_->set_opt_stat_manager(&opt_stat_manager_);
            optctx_->disable_batch_rpc();
            optctx_->set_local_server_addr("1.1.1.1", 8888);
            optctx_->set_use_default_stat();
            // 模拟 resolver 已识别出的 FK/Trigger nested-SQL 标志，专门验证 PDML gate。
            optctx_->set_has_fk(mock_has_fk);
            optctx_->set_has_trigger(mock_has_trigger);
            if (mock_has_trigger) {
              query_ctx->disable_udf_parallel_ = true;
            }
            ObTableLocation table_location;
            if (OB_FAIL(optctx_->get_table_location_list().push_back(table_location))) {
              LOG_WARN("failed to push table location", K(ret));
            } else {
              ObOptimizer optimizer(*optctx_);
              if (OB_FAIL(optimizer.optimize(*stmt, logical_plan))) {
                LOG_WARN("failed to optimize statement", K(ret));
              }
            }
          }
        }
      }
    }

    exec_ctx_.get_sql_ctx()->cur_stmt_ = old_cur_stmt;
    GCTX.self_addr_seq_ = old_self_addr_seq;
    parser.free_result(parse_result);
    return ret;
  }

  int generate_direct_load_stream_physical_plan_info(
      ObString &sql,
      DirectLoadStreamPhysicalPlanInfo &plan_info)
  {
    int ret = OB_SUCCESS;
    ObResultSet result(session_info_, allocator_);
    ObLogPlan *logical_plan = NULL;
    ObPhysicalPlan *phy_plan = NULL;
    bool is_select = true;
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    session_info_.set_force_rich_format(ObBasicSessionInfo::ForceRichFormatStatus::FORCE_ON);
    if (OB_NOT_NULL(exec_ctx_.get_physical_plan_ctx())) {
      exec_ctx_.get_physical_plan_ctx()->set_rich_format(session_info_.use_rich_format());
    }
    plan_info.reset();
    if (OB_FAIL(generate_direct_load_stream_logical_plan(result, sql, logical_plan, is_select))) {
      LOG_WARN("failed to generate direct load stream logical plan", K(ret), K(sql));
    } else if (OB_ISNULL(logical_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct load stream logical plan is null", K(ret), K(sql));
    } else if (OB_FAIL(MockCacheObjectFactory::alloc(
                   phy_plan, session_info_.get_effective_tenant_id()))) {
      LOG_WARN("failed to alloc direct load stream physical plan", K(ret), K(sql));
    } else {
      DatumParamStore &datum_param_store =
          exec_ctx_.get_physical_plan_ctx()->get_datum_param_store();
      ObCodeGenerator code_gen(false, CLUSTER_VERSION_1500, &datum_param_store);
      phy_plan->set_use_rich_format(session_info_.use_rich_format());
      if (OB_NOT_NULL(exec_ctx_.get_physical_plan_ctx())) {
        exec_ctx_.get_physical_plan_ctx()->set_rich_format(session_info_.use_rich_format());
      }

      if (OB_FAIL(code_gen.generate(*logical_plan, *phy_plan))) {
        LOG_WARN("failed to generate direct load stream physical plan", K(ret), K(sql));
      } else if (OB_ISNULL(phy_plan->get_root_op_spec())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("direct load stream root spec is null", K(ret), K(sql));
      } else {
        const ObOpSpec &root_spec = *phy_plan->get_root_op_spec();
        plan_info.root_type_ = root_spec.get_type();
        plan_info.has_table_direct_insert_ =
            contains_phy_operator_type(root_spec, PHY_TABLE_DIRECT_INSERT);
        plan_info.has_px_repart_transmit_ =
            contains_phy_operator_type(root_spec, PHY_PX_REPART_TRANSMIT);
        const ObTableDirectInsertSpec *direct_insert_spec =
            find_table_direct_insert_spec(root_spec);
        if (nullptr != direct_insert_spec) {
          plan_info.direct_insert_part_id_index_ =
              direct_insert_spec->row_desc_.get_part_id_index();
          const ObOpSpec *child = direct_insert_spec->get_child_cnt() > 0
              ? direct_insert_spec->get_child(0) : nullptr;
          const int64_t part_id_index = plan_info.direct_insert_part_id_index_;
          plan_info.has_pdml_partition_id_expr_ = nullptr != child
              && part_id_index >= 0 && part_id_index < child->output_.count()
              && nullptr != child->output_.at(part_id_index)
              && T_PDML_PARTITION_ID == child->output_.at(part_id_index)->type_;
        }
        plan_info.enable_append_ = phy_plan->get_enable_append();
        plan_info.enable_inc_direct_load_ = phy_plan->get_enable_inc_direct_load();
      }
    }
    if (nullptr != phy_plan) {
      MockCacheObjectFactory::free(phy_plan);
      phy_plan = nullptr;
    }
    return ret;
  }

  const share::schema::ObNeedPriv *find_target_insert_need_priv(
      const share::schema::ObStmtNeedPrivs &stmt_need_privs,
      const char *database_name,
      const char *table_name) const
  {
    const share::schema::ObNeedPriv *target_insert_need_priv = nullptr;
    for (int64_t i = 0;
         OB_ISNULL(target_insert_need_priv) && i < stmt_need_privs.need_privs_.count();
         ++i) {
      const share::schema::ObNeedPriv &need_priv = stmt_need_privs.need_privs_.at(i);
      if (share::schema::OB_PRIV_TABLE_LEVEL == need_priv.priv_level_
          && OB_PRIV_INSERT == need_priv.priv_set_
          && 0 == need_priv.db_.case_compare(database_name)
          && 0 == need_priv.table_.case_compare(table_name)) {
        target_insert_need_priv = &need_priv;
      }
    }
    return target_insert_need_priv;
  }

  MockDirectLoadStreamResolverMtlEnv mtl_env_;

  DISALLOW_COPY_AND_ASSIGN(TestDirectLoadStreamSourceResolver);
};

// 主路径：Stream SOURCE 在 INSERT SELECT 上下文中解析成 mocked external table，
// 并按 S4 Writer 的完整存储行布局生成 schema。
TEST_F(TestDirectLoadStreamSourceResolver, insert_select_source_resolve)
{
  do_create_table(
      "create table dls_target(c1 int primary key, c2 varchar(32), c3 decimal(10, 2))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into dls_target(c1, c2, c3) "
                                       "select * from source(type='DIRECT_LOAD_STREAM', "
                                       "stream_id=%ld)",
                                       stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(sql.string(), stmt, JSON_FORMAT, OB_SUCCESS));
  ASSERT_TRUE(NULL != stmt);
  ASSERT_EQ(stmt::T_INSERT, stmt->get_stmt_type());

  ObInsertStmt *insert_stmt = static_cast<ObInsertStmt *>(stmt);
  ASSERT_TRUE(insert_stmt->value_from_select());
  ASSERT_EQ(3, insert_stmt->get_values_desc().count());
}

// 行布局边界：S4 Writer 输出完整 converted row，SOURCE 不接受显式 partial 列表。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_partial_target_columns)
{
  do_create_table("create table dls_target(c1 int primary key, c2 int, c3 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1, c2) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(sql.string(), stmt, JSON_FORMAT, -OB_NOT_SUPPORTED));
}

// 行布局边界：显式完整列列表也必须与 S4 Writer 的 rowkey-first 存储顺序一致。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_reordered_target_columns)
{
  do_create_table("create table dls_target(c2 int, c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c2, c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(sql.string(), stmt, JSON_FORMAT, -OB_NOT_SUPPORTED));
}

// 省略目标列时，resolver 将 INSERT 映射重排成 S4 Writer 的 rowkey-first 存储顺序。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_reorders_implicit_target_columns)
{
  do_create_table("create table dls_target(c2 int, c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(sql.string(), stmt, JSON_FORMAT, OB_SUCCESS));
  ASSERT_NE(nullptr, stmt);
  ASSERT_EQ(stmt::T_INSERT, stmt->get_stmt_type());
  ObInsertStmt *insert_stmt = static_cast<ObInsertStmt *>(stmt);
  ASSERT_EQ(2, insert_stmt->get_values_desc().count());
  ASSERT_NE(nullptr, insert_stmt->get_values_desc().at(0));
  ASSERT_NE(nullptr, insert_stmt->get_values_desc().at(1));
  EXPECT_EQ(0, insert_stmt->get_values_desc().at(0)->get_column_name().case_compare("c1"));
  EXPECT_EQ(0, insert_stmt->get_values_desc().at(1)->get_column_name().case_compare("c2"));
}

// 计划入口：验证 S3 只要求进入 logical plan generation，
// 不覆盖 PX 调度、iterator 或 direct insert 执行。
TEST_F(TestDirectLoadStreamSourceResolver, insert_select_source_generate_logical_plan)
{
  do_create_table(
      "create table dls_target(c1 int primary key, c2 varchar(32), c3 decimal(10, 2))");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt("insert into dls_target(c1, c2, c3) "
                                           "select * from source(type='DIRECT_LOAD_STREAM', "
                                           "stream_id=%ld)",
                                           stream_id));

  ObResultSet result(session_info_, allocator_);
  ObLogPlan *logical_plan = NULL;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ObPhysicalPlan *phy_plan = nullptr;
  MockPhysicalPlanGuard phy_plan_guard(phy_plan);
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(
                result, sql, logical_plan, is_select, phy_plan));
  ASSERT_TRUE(NULL != logical_plan);
  ASSERT_TRUE(NULL != phy_plan);
  ASSERT_TRUE(NULL != logical_plan->get_plan_root());
  ASSERT_TRUE(NULL != logical_plan->get_stmt());
  ASSERT_EQ(stmt::T_INSERT, logical_plan->get_stmt()->get_stmt_type());
  ASSERT_TRUE(NULL != optctx_);
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().can_use_direct_load());
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().is_inc_direct_load());
}

// APPEND 显式表示 FULL direct load，Stream SOURCE 不能将它静默改写为 incremental。
TEST_F(TestDirectLoadStreamSourceResolver, append_hint_is_not_rewritten_to_incremental)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert /*+ append enable_parallel_dml parallel(2) "
      "no_gather_optimizer_statistics */ into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObResultSet result(session_info_, allocator_);
  ObLogPlan *logical_plan = NULL;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(result, sql, logical_plan, is_select));
  ASSERT_TRUE(NULL != logical_plan);
  ASSERT_TRUE(NULL != optctx_);
  EXPECT_TRUE(optctx_->get_direct_load_optimizer_ctx().use_direct_load());
  EXPECT_TRUE(optctx_->get_direct_load_optimizer_ctx().is_full_direct_load());
  EXPECT_FALSE(optctx_->get_direct_load_optimizer_ctx().is_inc_direct_load());
}

// schema 漂移方向：当前表版本更新时只标记 execution prepare defer，不在 resolver 提前失败。
TEST_F(TestDirectLoadStreamSourceResolver, newer_target_schema_is_deferred_to_execution_prepare)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id, -1));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(sql.string(), stmt, JSON_FORMAT, OB_SUCCESS));
  ASSERT_TRUE(NULL != stmt);
  ASSERT_EQ(stmt::T_INSERT, stmt->get_stmt_type());
  ASSERT_TRUE(static_cast<ObInsertStmt *>(stmt)
                ->has_direct_load_stream_schema_drift());
}

// generated column 导致的支持性检查在 optimizer 中延后；本用例只验证 logical plan 选路。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_generated_column_is_deferred_to_post_claim_support_check)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, publish_generated_column("dls_target"));
  reset_query_ctx_schema_guard();

  observer::ObDirectLoadStreamMeta stream_meta;
  const ObTableSchema *target_table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(
              stream_id, stream_meta));
  ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(
      sys_tenant_id_, ObString::make_string("dls_db"),
      ObString::make_string("dls_target"), false, target_table_schema));
  ASSERT_TRUE(NULL != target_table_schema);
  ASSERT_TRUE(target_table_schema->has_generated_column());
  ASSERT_GT(target_table_schema->get_schema_version(),
            stream_meta.target_schema_version_);
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObResultSet result(session_info_, allocator_);
  ObLogPlan *logical_plan = NULL;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(result, sql, logical_plan, is_select));
  ASSERT_TRUE(NULL != logical_plan);
  ASSERT_TRUE(NULL != logical_plan->get_stmt());
  ASSERT_EQ(stmt::T_INSERT, logical_plan->get_stmt()->get_stmt_type());
  ASSERT_TRUE(static_cast<const ObInsertStmt *>(logical_plan->get_stmt())
                ->has_direct_load_stream_schema_drift());
  ASSERT_TRUE(NULL != optctx_);
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().can_use_direct_load());
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().is_inc_direct_load());
}

// RLS DDL 引起 schema 漂移时，optimizer 延后 RLS gate；本用例只验证 logical plan 选路。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_force_rls_is_deferred_to_post_claim_schema_check)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, publish_force_rls("dls_target"));
  reset_query_ctx_schema_guard();

  observer::ObDirectLoadStreamMeta stream_meta;
  const ObTableSchema *target_table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS,
            observer::ObDirectLoadStreamService::get_direct_load_stream(
              stream_id, stream_meta));
  ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(
      sys_tenant_id_, ObString::make_string("dls_db"),
      ObString::make_string("dls_target"), false, target_table_schema));
  ASSERT_TRUE(NULL != target_table_schema);
  ASSERT_TRUE(target_table_schema->has_row_level_security());
  ASSERT_TRUE(target_table_schema->has_force_row_level_security());
  ASSERT_GT(target_table_schema->get_schema_version(),
            stream_meta.target_schema_version_);
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObResultSet result(session_info_, allocator_);
  ObLogPlan *logical_plan = NULL;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(result, sql, logical_plan, is_select));
  ASSERT_TRUE(NULL != logical_plan);
  ASSERT_TRUE(NULL != logical_plan->get_stmt());
  ASSERT_EQ(stmt::T_INSERT, logical_plan->get_stmt()->get_stmt_type());
  ASSERT_TRUE(static_cast<const ObInsertStmt *>(logical_plan->get_stmt())
                ->has_direct_load_stream_schema_drift());
  ASSERT_TRUE(NULL != optctx_);
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().can_use_direct_load());
  ASSERT_TRUE(optctx_->get_direct_load_optimizer_ctx().is_inc_direct_load());
}

// 真实版本漂移叠加 FK optimizer 标志时，即使允许 fallback 也必须保留 direct-load preflight。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_foreign_key_is_deferred_with_fallback_enabled)
{
  verify_nested_schema_drift_is_deferred(false, true);
}

// 真实版本漂移叠加 FK optimizer 标志时，禁用 fallback 不能在 claim 前截断执行路径。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_foreign_key_is_deferred_with_fallback_disabled)
{
  verify_nested_schema_drift_is_deferred(false, false);
}

// 真实版本漂移叠加 Trigger optimizer 标志时，允许 fallback 也必须保留 direct-load preflight。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_insert_trigger_is_deferred_with_fallback_enabled)
{
  verify_nested_schema_drift_is_deferred(true, true);
}

// 真实版本漂移叠加 Trigger optimizer 标志时，禁用 fallback 不能在 claim 前截断执行路径。
TEST_F(TestDirectLoadStreamSourceResolver,
       newer_insert_trigger_is_deferred_with_fallback_disabled)
{
  verify_nested_schema_drift_is_deferred(true, false);
}

// 无 schema 漂移时保留 RLS gate，不能把所有 Stream RLS 错误都延后。
TEST_F(TestDirectLoadStreamSourceResolver,
       force_rls_without_schema_drift_remains_rejected)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  ASSERT_EQ(OB_SUCCESS, publish_force_rls("dls_target"));
  reset_query_ctx_schema_guard();

  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObResultSet result(session_info_, allocator_);
  ObLogPlan *logical_plan = NULL;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION,
            generate_direct_load_stream_logical_plan(
                result, sql, logical_plan, is_select));
}

// schema 刷新方向：当前表版本落后 metadata 时不 claim，直接返回可重试错误。
TEST_F(TestDirectLoadStreamSourceResolver, older_target_schema_retries_before_claim)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id, 1));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)", stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(), stmt, JSON_FORMAT, -OB_SCHEMA_EAGAIN));
}

// 计划入口：S5 至少验证非分区目标表能进入 direct insert physical plan，
// 后续分区 repartition / PX 分布仍归 Step 8/P6。
TEST_F(TestDirectLoadStreamSourceResolver,
       insert_select_source_generate_non_partition_direct_insert_plan)
{
  do_create_table("create table dls_target(c1 int primary key, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert /*+ direct(true, 0, \047inc\047) enable_parallel_dml parallel(2) "
      "no_gather_optimizer_statistics */ "
      "into dls_target(c1, c2) "
      "select * from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld)",
      stream_id));

  DirectLoadStreamPhysicalPlanInfo plan_info;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_SUCCESS, generate_direct_load_stream_physical_plan_info(sql, plan_info));
  ASSERT_TRUE(plan_info.has_table_direct_insert_);
  ASSERT_TRUE(plan_info.enable_append_);
  ASSERT_TRUE(plan_info.enable_inc_direct_load_);
}

// 分区目标必须生成带 partition-id repartition 的 direct insert 物理计划。
TEST_F(TestDirectLoadStreamSourceResolver,
       partition_target_generates_pdml_partition_id_repart_direct_insert_plan)
{
  ObStmt *create_table_stmt = nullptr;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string(
          "create table dls_target(c1 int primary key, c2 int) "
          "partition by hash(c1) partitions 3"),
      create_table_stmt,
      JSON_FORMAT));
  stmt_factory_.destory();
  expr_factory_.destory();
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql_buf.assign_fmt(
      "insert /*+ direct(true, 0, \047inc\047) enable_parallel_dml parallel(2) "
      "no_gather_optimizer_statistics */ "
      "into dls_target(c1, c2) "
      "select * from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld)",
      stream_id));

  DirectLoadStreamPhysicalPlanInfo plan_info;
  ObString sql = sql_buf.string();
  ASSERT_EQ(OB_SUCCESS, generate_direct_load_stream_physical_plan_info(sql, plan_info));
  ASSERT_TRUE(plan_info.has_table_direct_insert_);
  ASSERT_TRUE(plan_info.has_px_repart_transmit_);
  ASSERT_GE(plan_info.direct_insert_part_id_index_, 0);
  ASSERT_TRUE(plan_info.has_pdml_partition_id_expr_);
  ASSERT_TRUE(plan_info.enable_append_);
  ASSERT_TRUE(plan_info.enable_inc_direct_load_);
}

// one-shot 边界：Stream SOURCE 消费 queue，不允许 join 形态引入重扫/复用风险。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_join_shape)
{
  do_create_table("create table dls_target(c1 int primary key)");
  do_create_table("create table dls_side(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1) "
      "select 1 from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld) "
      "join dls_side t on 1 = 1",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}

// one-shot 边界：多个 FROM item 同样不能让 Stream SOURCE 被普通外表路径复用。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_multiple_from_items)
{
  do_create_table("create table dls_target(c1 int primary key)");
  do_create_table("create table dls_side(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1) "
      "select 1 from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld), dls_side t",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}

// Reader 当前按 chunk 位置写 access expr；Step 8 projector 完成前拒绝非 identity 投影。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_reordered_source_projection)
{
  do_create_table("create table dls_target(c1 int primary key, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1, c2) "
      "select c2, c1 from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld)",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}

// iterator 会整块领取 queue 数据，Step 6 完成消费证明前禁止上层提前停止下探。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_limit)
{
  do_create_table("create table dls_target(c1 int primary key, c2 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into dls_target(c1, c2) "
      "select * from source(type=\047DIRECT_LOAD_STREAM\047, stream_id=%ld) limit 1",
      stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}

// 关闭本地计划缓存时，codegen 应保留混合大小写目标表对应的权限描述。
// 权限描述中的 INSERT 列集合必须保持精确。
TEST_F(TestDirectLoadStreamSourceResolver,
       no_plan_cache_keeps_exact_insert_privilege_in_physical_plan)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql_buf;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  ASSERT_EQ(OB_SUCCESS,
            session_info_.update_sys_variable(SYS_VAR_OB_ENABLE_PLAN_CACHE, 0));
  ASSERT_FALSE(session_info_.get_local_ob_enable_plan_cache());
  session_info_.force_enable_plan_tracing();
  session_info_.cached_tenant_config_info_.force_enable_plan_tracing_ = false;
  session_info_.cached_tenant_config_info_.last_check_ec_ts_ = INT64_MAX;
  session_info_.cached_tenant_config_info_.saved_tenant_info_ =
      session_info_.get_effective_tenant_id();
  ASSERT_FALSE(session_info_.force_enable_plan_tracing());
  session_info_.sys_vars_cache_.set_enable_rich_vector_format(true);
  session_info_.init_use_rich_format();
  ASSERT_TRUE(session_info_.use_rich_format());
  exec_ctx_.get_physical_plan_ctx()->set_rich_format(
      session_info_.use_rich_format());
  ASSERT_EQ(OB_SUCCESS,
            sql_buf.assign_fmt(
                "insert /*+ direct(true, 0, 'inc') enable_parallel_dml parallel(2) "
                "no_gather_optimizer_statistics */ into DLS_DB.DLS_TARGET(c1) "
                "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
                stream_id));

  ObResultSet result(session_info_, allocator_);
  result.set_exec_context(exec_ctx_);
  ObLogPlan *logical_plan = nullptr;
  bool is_select = true;
  ObString sql = sql_buf.string();
  ObPhysicalPlan *phy_plan = nullptr;
  MockPhysicalPlanGuard phy_plan_guard(phy_plan);
  share::schema::ObStmtNeedPrivs stmt_need_privs(allocator_);
  share::schema::ObStmtOraNeedPrivs stmt_ora_need_privs(allocator_);
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(result,
                                                     sql,
                                                     logical_plan,
                                                     is_select,
                                                     phy_plan,
                                                     &stmt_need_privs,
                                                     &stmt_ora_need_privs));
  ASSERT_NE(nullptr, logical_plan);
  ASSERT_NE(nullptr, logical_plan->get_stmt());
  ASSERT_NE(nullptr, phy_plan);
  phy_plan->set_use_rich_format(session_info_.use_rich_format());
  const share::schema::ObNeedPriv *resolved_insert_need_priv =
      find_target_insert_need_priv(stmt_need_privs, "dls_db", "dls_target");
  ASSERT_NE(nullptr, resolved_insert_need_priv);
  EXPECT_EQ(0, resolved_insert_need_priv->db_.compare("dls_db"));
  EXPECT_EQ(0, resolved_insert_need_priv->table_.compare("DLS_TARGET"));
  ASSERT_EQ(1, resolved_insert_need_priv->columns_.count());
  EXPECT_EQ(0, resolved_insert_need_priv->columns_.at(0).case_compare("c1"));

  ObSEArray<ObAuditUnit, 1> audit_units;
  ObDMLStmt *stmt = const_cast<ObDMLStmt *>(logical_plan->get_stmt());
  sql_ctx_.all_local_session_vars_ = &stmt->get_query_ctx()->all_local_session_vars_;
  const ObStmt *old_cur_stmt = sql_ctx_.cur_stmt_;
  sql_ctx_.cur_stmt_ = stmt;
  const int codegen_ret = ObSql::code_generate(sql_ctx_,
                                               result,
                                               stmt,
                                               stmt_need_privs,
                                               stmt_ora_need_privs,
                                               audit_units,
                                               logical_plan,
                                               phy_plan);
  sql_ctx_.cur_stmt_ = old_cur_stmt;
  EXPECT_EQ(OB_SUCCESS, codegen_ret);
  if (OB_SUCCESS == codegen_ret) {
    EXPECT_TRUE(logical_plan->get_optimizer_context()
                    .get_direct_load_optimizer_ctx()
                    .has_direct_load_stream_source());
    EXPECT_TRUE(phy_plan->get_enable_inc_direct_load());
    const share::schema::ObNeedPriv *copied_insert_need_priv =
        find_target_insert_need_priv(phy_plan->get_stmt_need_privs(), "dls_db", "dls_target");
    ASSERT_NE(nullptr, copied_insert_need_priv);
    ASSERT_EQ(1, copied_insert_need_priv->columns_.count());
    EXPECT_EQ(0, copied_insert_need_priv->columns_.at(0).case_compare("c1"));
  }
}

// 普通增量旁路导入关闭计划缓存时，不应复制无运行期消费者的权限描述。
TEST_F(TestDirectLoadStreamSourceResolver,
       no_plan_cache_ordinary_incremental_direct_load_skips_privilege_copy)
{
  do_create_table("create table dls_target(c1 int primary key)");
  reset_query_ctx_schema_guard();
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  ASSERT_EQ(OB_SUCCESS,
            session_info_.update_sys_variable(SYS_VAR_OB_ENABLE_PLAN_CACHE, 0));
  ASSERT_FALSE(session_info_.get_local_ob_enable_plan_cache());
  session_info_.force_enable_plan_tracing();
  session_info_.cached_tenant_config_info_.force_enable_plan_tracing_ = false;
  session_info_.cached_tenant_config_info_.last_check_ec_ts_ = INT64_MAX;
  session_info_.cached_tenant_config_info_.saved_tenant_info_ =
      session_info_.get_effective_tenant_id();
  ASSERT_FALSE(session_info_.force_enable_plan_tracing());
  session_info_.sys_vars_cache_.set_enable_rich_vector_format(true);
  session_info_.init_use_rich_format();
  ASSERT_TRUE(session_info_.use_rich_format());
  exec_ctx_.get_physical_plan_ctx()->set_rich_format(
      session_info_.use_rich_format());
  ObString sql = ObString::make_string(
      "insert /*+ direct(true, 0, 'inc') enable_parallel_dml parallel(2) "
      "no_gather_optimizer_statistics */ into dls_target(c1) select 1");

  ObResultSet result(session_info_, allocator_);
  result.set_exec_context(exec_ctx_);
  ObLogPlan *logical_plan = nullptr;
  bool is_select = true;
  ObPhysicalPlan *phy_plan = nullptr;
  MockPhysicalPlanGuard phy_plan_guard(phy_plan);
  share::schema::ObStmtNeedPrivs stmt_need_privs(allocator_);
  share::schema::ObStmtOraNeedPrivs stmt_ora_need_privs(allocator_);
  ASSERT_EQ(OB_SUCCESS,
            generate_direct_load_stream_logical_plan(result,
                                                     sql,
                                                     logical_plan,
                                                     is_select,
                                                     phy_plan,
                                                     &stmt_need_privs,
                                                     &stmt_ora_need_privs));
  ASSERT_NE(nullptr, logical_plan);
  ASSERT_NE(nullptr, logical_plan->get_stmt());
  ASSERT_NE(nullptr, phy_plan);
  phy_plan->set_use_rich_format(session_info_.use_rich_format());
  EXPECT_FALSE(logical_plan->get_optimizer_context()
                   .get_direct_load_optimizer_ctx()
                   .has_direct_load_stream_source());
  const share::schema::ObNeedPriv *resolved_insert_need_priv =
      find_target_insert_need_priv(stmt_need_privs, "dls_db", "dls_target");
  ASSERT_NE(nullptr, resolved_insert_need_priv);
  ASSERT_EQ(1, resolved_insert_need_priv->columns_.count());
  EXPECT_EQ(0, resolved_insert_need_priv->columns_.at(0).case_compare("c1"));

  ObSEArray<ObAuditUnit, 1> audit_units;
  ObDMLStmt *stmt = const_cast<ObDMLStmt *>(logical_plan->get_stmt());
  sql_ctx_.all_local_session_vars_ =
      &stmt->get_query_ctx()->all_local_session_vars_;
  const ObStmt *old_cur_stmt = sql_ctx_.cur_stmt_;
  sql_ctx_.cur_stmt_ = stmt;
  const int codegen_ret = ObSql::code_generate(sql_ctx_,
                                               result,
                                               stmt,
                                               stmt_need_privs,
                                               stmt_ora_need_privs,
                                               audit_units,
                                               logical_plan,
                                               phy_plan);
  sql_ctx_.cur_stmt_ = old_cur_stmt;
  EXPECT_EQ(OB_SUCCESS, codegen_ret);
  if (OB_SUCCESS == codegen_ret) {
    EXPECT_TRUE(phy_plan->get_enable_inc_direct_load());
    EXPECT_TRUE(phy_plan->get_stmt_need_privs().need_privs_.empty());
    EXPECT_TRUE(phy_plan->get_stmt_ora_need_privs().need_privs_.empty());
  }
}

// 元数据边界：stream_id 绑定的目标表必须与 INSERT 目标表一致。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_target_table_mismatch)
{
  do_create_table("create table dls_target(c1 int)");
  do_create_table("create table dls_other(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_other", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into dls_target(c1) "
                                       "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
                                       stream_id));

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 元数据归属：stream 的 tenant_id + user_id 必须匹配当前 session。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_owner_mismatch)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();
  int64_t stream_id = OB_INVALID_ID;
  ObSqlString sql;
  const uint64_t old_priv_user_id = session_info_.get_priv_user_id();
  ASSERT_EQ(OB_SUCCESS, create_bound_stream("dls_target", stream_id));
  ASSERT_TRUE(observer::is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into dls_target(c1) "
                                       "select * from source(type='DIRECT_LOAD_STREAM', stream_id=%ld)",
                                       stream_id));

  session_info_.set_priv_user_id(old_priv_user_id + 100);
  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      sql.string(),
      stmt,
      JSON_FORMAT,
      -OB_ERR_NO_PRIVILEGE));
  session_info_.set_priv_user_id(old_priv_user_id);
}

// 用户边界：standalone Stream SOURCE SELECT 不是 P0 能力，
// 避免绕开 DIRECT LOAD 主路径消费队列。
TEST_F(TestDirectLoadStreamSourceResolver, standalone_select_not_supported)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("select * from source(type='DIRECT_LOAD_STREAM', stream_id=1)"),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}

// 参数边界：stream_id=0 保留为非法值，不能进入 metadata 查询。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_zero_stream_id)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM', stream_id=0)"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 参数边界：DIRECT_LOAD_STREAM SOURCE 必须显式携带 stream_id。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_missing_stream_id)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM')"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 语法边界：stream_id 只能是整数 literal，字符串和小数不进入 resolver。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_non_integer_stream_id_in_parser)
{
  ObParser parser(allocator_, SMO_DEFAULT);

  ParseResult string_parse_result;
  ObString string_stream_id = ObString::make_string(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id='1')");
  ASSERT_NE(OB_SUCCESS, parser.parse(string_stream_id, string_parse_result));
  parser.free_result(string_parse_result);

  ParseResult decimal_parse_result;
  ObString decimal_stream_id = ObString::make_string(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', stream_id=1.2)");
  ASSERT_NE(OB_SUCCESS, parser.parse(decimal_stream_id, decimal_parse_result));
  parser.free_result(decimal_parse_result);
}

// 元数据边界：合法正数 stream_id 不存在时，resolver 直接返回 metadata lookup 错误。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_missing_stream)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM', stream_id=987654321)"),
      stmt,
      JSON_FORMAT,
      -OB_ENTRY_NOT_EXIST));
}

// 参数边界：重复 stream_id 不能以后一个覆盖前一个。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_duplicate_stream_id)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM', stream_id=1, stream_id=2)"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 参数边界：重复 type 不能让 DIRECT_LOAD_STREAM SOURCE 语义依赖后一次解析结果。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_duplicate_type)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM', type='DIRECT_LOAD_STREAM', stream_id=1)"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 参数边界：Stream type 在前、普通 type 在后时也必须按重复 type 拒绝。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_mixed_type_stream_first)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='DIRECT_LOAD_STREAM', type='ODPS')"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 参数边界：普通 type 在前、Stream type 在后时也必须按重复 type 拒绝。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_mixed_type_stream_last)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='ODPS', type='DIRECT_LOAD_STREAM', "
                            "stream_id=1)"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 语法边界：只有专用 stream_id 数值属性能进入 Stream SOURCE，
// 未知数值属性在 parser 阶段拒绝。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_unknown_numeric_property_in_parser)
{
  ObParser parser(allocator_, SMO_DEFAULT);
  ParseResult parse_result;
  ObString sql = ObString::make_string(
      "insert into dls_target(c1) "
      "select * from source(type='DIRECT_LOAD_STREAM', foo=1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(sql, parse_result));
  parser.free_result(parse_result);
}

// 参数边界：DIRECT_LOAD_STREAM SOURCE 只接受 type + stream_id，
// 不复用普通外表的 access key 等属性。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_extra_properties)
{
  do_create_table("create table dls_target(c1 int, c2 varchar(32))");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1, c2) "
                            "select * from source(type='DIRECT_LOAD_STREAM', stream_id=1, accesskey='secret')"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 参数归属：stream_id 只属于 DIRECT_LOAD_STREAM format，
// 不能挂在 CSV 等其他 format 上。
TEST_F(TestDirectLoadStreamSourceResolver, stream_id_requires_direct_load_stream_type)
{
  do_create_table("create table dls_target(c1 int)");
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("insert into dls_target(c1) "
                            "select * from source(type='CSV', stream_id=1)"),
      stmt,
      JSON_FORMAT,
      -OB_INVALID_ARGUMENT));
}

// 暴露边界：DIRECT_LOAD_STREAM 只作为 SOURCE 编译入口，
// 不允许创建持久 external table。
TEST_F(TestDirectLoadStreamSourceResolver, direct_load_stream_rejects_persistent_external_table)
{
  reset_query_ctx_schema_guard();

  ObStmt *stmt = NULL;
  ASSERT_NO_FATAL_FAILURE(do_load_sql(
      ObString::make_string("create external table dls_ext(c1 int) "
                            "location='file:///tmp/dls_ext' "
                            "properties(type='DIRECT_LOAD_STREAM', stream_id=1)"),
      stmt,
      JSON_FORMAT,
      -OB_NOT_SUPPORTED));
}
} // namespace test

int main(int argc, char **argv)
{
  system("rm -f test_direct_load_stream_source_resolver.log*");
  OB_LOGGER.set_file_name("test_direct_load_stream_source_resolver.log", true);
  OB_LOGGER.set_log_level("INFO");
  GCONF._ob_enable_direct_load.set_value("True");
  GCONF._ob_enable_direct_load_stream.set_value("True");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

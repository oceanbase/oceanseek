/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/ob_thread_mgr.h"
#include <atomic>
#include <memory>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "share/ob_ddl_sim_point.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "rootserver/ddl_task/ob_async_vector_index_build_task.h"
#include "rootserver/ddl_task/ob_drop_async_vector_index_task.h"
#include "rootserver/ddl_task/ob_drop_fts_index_task.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_tenant_ddl_service.h"
#include "rootserver/parallel_ddl/ob_update_index_status_helper.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_ddl_service_launcher.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ddl_task/ob_fts_index_build_task.h"
#include "rootserver/ddl_task/ob_drop_search_index_task.h"
#include "share/ob_ddl_sim_point.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/mock_schema_service.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/ob_debug_sync.h"
#include "share/longops_mgr/ob_longops_mgr.h"
#include "share/config/ob_server_config.h"

#define LOG_FILE_PATH "./test_ddl_task_struct.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

TEST(test_ddl_task_struct, terminal_failure_feedback_requires_persisted_non_retryable_error)
{
  ObDDLTaskRecord record;
  const int terminal_errors[] = {OB_ALLOCATE_MEMORY_FAILED, OB_ERR_UNEXPECTED,
      OB_NOT_SUPPORTED, OB_CHECKSUM_ERROR, OB_CANCELED};
  const share::ObDDLTaskStatus states[] = {share::ObDDLTaskStatus::PREPARE,
      share::ObDDLTaskStatus::REDEFINITION, share::ObDDLTaskStatus::FAIL,
      share::ObDDLTaskStatus::SUCCESS};
  for (const auto state : states) {
    record.task_status_ = state;
    for (const int error : terminal_errors) {
      record.ret_code_ = error;
      ASSERT_EQ(share::ObDDLTaskStatus::FAIL == state,
          ObDDLTask::can_report_terminal_failure_before_cleanup(record));
    }
  }
  record.task_status_ = share::ObDDLTaskStatus::FAIL;
  const int retryable_errors[] = {OB_EAGAIN, OB_TASK_EXPIRED, OB_TIMEOUT, OB_NOT_MASTER};
  for (const int error : retryable_errors) {
    record.ret_code_ = error;
    ASSERT_FALSE(ObDDLTask::can_report_terminal_failure_before_cleanup(record));
  }
  const int64_t invalid_results[] = {OB_SUCCESS, 1, INT64_MAX, INT64_MIN};
  for (const int64_t error : invalid_results) {
    record.ret_code_ = error;
    ASSERT_FALSE(ObDDLTask::can_report_terminal_failure_before_cleanup(record));
  }
}

TEST(test_ddl_task_struct, index_build_callback_preserves_first_terminal_failure)
{
  ObIndexBuildTask task;
  task.is_inited_ = true;
  task.tenant_id_ = 1001;
  task.task_id_ = 1;
  task.task_type_ = share::DDL_CREATE_INDEX;
  task.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  task.snapshot_version_ = 123;
  task.is_retryable_ddl_ = false;
  ObDDLTaskInfo info;
  ObAddr addr;
  ObTabletID tablet_id;
  ASSERT_SUCC(task.update_complete_sstable_job_status(
      tablet_id, addr, 123, 1, OB_ALLOCATE_MEMORY_FAILED, info));
  ASSERT_GT(task.sstable_complete_ts_, 0);
  task.sstable_complete_ts_ = 1;
  // Two callbacks from the same execution cannot hide its first failure.
  ASSERT_SUCC(task.update_complete_sstable_job_status(tablet_id, addr, 123, 1, OB_SUCCESS, info));
  ASSERT_SUCC(task.update_complete_sstable_job_status(tablet_id, addr, 123, 1, OB_ERR_UNEXPECTED, info));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, task.complete_sstable_job_ret_code_);
  ASSERT_GT(task.sstable_complete_ts_, 1);
  bool finished = false;
  int end_ret = OB_ERR_UNEXPECTED;
  ASSERT_SUCC(task.check_build_local_index_single_replica(finished, end_ret));
  ASSERT_TRUE(finished);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, end_ret);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, task.ret_code_);
  const int64_t completed_ts = task.sstable_complete_ts_;
  ASSERT_EQ(OB_TASK_EXPIRED, task.update_complete_sstable_job_status(
      tablet_id, addr, 123, 0, OB_SUCCESS, info));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, task.complete_sstable_job_ret_code_);
  ASSERT_EQ(completed_ts, task.sstable_complete_ts_);
}

TEST(test_ddl_task_struct, index_build_callback_keeps_retryable_execution_retry)
{
  ObIndexBuildTask task;
  task.is_inited_ = true;
  task.tenant_id_ = 1001;
  task.task_id_ = 1;
  task.task_type_ = share::DDL_CREATE_INDEX;
  task.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  task.snapshot_version_ = 123;
  task.is_retryable_ddl_ = true;
  ObDDLTaskInfo info;
  ObAddr addr;
  ObTabletID tablet_id;
  ASSERT_SUCC(task.update_complete_sstable_job_status(tablet_id, addr, 123, 1, OB_TIMEOUT, info));
  bool finished = false;
  int end_ret = OB_ERR_UNEXPECTED;
  ASSERT_SUCC(task.check_build_local_index_single_replica(finished, end_ret));
  ASSERT_FALSE(finished);
  ASSERT_EQ(OB_SUCCESS, end_ret);
  ASSERT_EQ(INT64_MAX, task.complete_sstable_job_ret_code_);
  ASSERT_EQ(OB_SUCCESS, task.ret_code_);
  ASSERT_SUCC(task.update_complete_sstable_job_status(tablet_id, addr, 123, 2, OB_SUCCESS, info));
  ASSERT_EQ(OB_SUCCESS, task.complete_sstable_job_ret_code_);
}

TEST(test_ddl_task_struct, partitioned_index_callback_preserves_failed_batch_until_retry)
{
  ObIndexBuildTask task;
  task.is_inited_ = true;
  task.tenant_id_ = 1001;
  task.task_id_ = 1;
  task.task_type_ = share::DDL_CREATE_PARTITIONED_LOCAL_INDEX;
  task.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  task.snapshot_version_ = 123;
  task.is_retryable_ddl_ = true;
  ObDDLTabletScheduler &scheduler = task.tablet_scheduler_;
  DEFER(scheduler.destroy());
  ASSERT_SUCC(scheduler.all_ls_to_tablets_map_.create(8, "TestDDLSched"));
  ASSERT_SUCC(scheduler.running_ls_to_tablets_map_.create(8, "TestDDLSched"));
  ASSERT_SUCC(scheduler.running_ls_to_execution_id_.create(8, "TestDDLSched"));
  scheduler.is_inited_ = true;
  ObDDLTaskInfo infos[2];
  for (int64_t i = 0; i < 2; ++i) {
    infos[i].ls_id_ = share::ObLSID(1001 + i);
    ASSERT_TRUE(infos[i].ls_leader_addr_.set_ip_addr("127.0.0.1", 2882 + i));
    ASSERT_SUCC(infos[i].partition_ids_.push_back(ObTabletID(2001 + i)));
    ASSERT_SUCC(scheduler.all_ls_to_tablets_map_.set_refactored(infos[i].ls_id_, infos[i].partition_ids_));
    ASSERT_SUCC(scheduler.running_ls_to_tablets_map_.set_refactored(infos[i].ls_id_, infos[i].partition_ids_));
    ASSERT_SUCC(scheduler.running_ls_to_execution_id_.set_refactored(infos[i].ls_id_, 1));
    ASSERT_SUCC(task.sql_exec_addrs_.push_back(infos[i].ls_leader_addr_));
  }

  ASSERT_SUCC(task.update_complete_sstable_job_status(infos[0].partition_ids_.at(0),
      infos[0].ls_leader_addr_, 123, 1, OB_TIMEOUT, infos[0]));
  ASSERT_EQ(OB_TIMEOUT, task.complete_sstable_job_ret_code_);
  ASSERT_EQ(1, scheduler.running_ls_to_tablets_map_.size());
  ASSERT_EQ(1, task.sql_exec_addrs_.count());
  task.sstable_complete_ts_ = 1;
  // Consume the result only after both batches reply, so success cannot hide a pending failure.
  ASSERT_SUCC(task.update_complete_sstable_job_status(infos[1].partition_ids_.at(0),
      infos[1].ls_leader_addr_, 123, 1, OB_SUCCESS, infos[1]));
  ASSERT_EQ(OB_TIMEOUT, task.complete_sstable_job_ret_code_);
  ASSERT_GT(task.sstable_complete_ts_, 1);
  ASSERT_EQ(0, scheduler.running_ls_to_tablets_map_.size());
  ASSERT_EQ(1, scheduler.all_ls_to_tablets_map_.size());
  ASSERT_EQ(0, task.sql_exec_addrs_.count());

  bool finished = true;
  int end_ret = OB_ERR_UNEXPECTED;
  ASSERT_SUCC(task.check_build_local_index_single_replica(finished, end_ret));
  ASSERT_FALSE(finished);
  ASSERT_EQ(OB_SUCCESS, end_ret);
  ASSERT_EQ(INT64_MAX, task.complete_sstable_job_ret_code_);
  ASSERT_EQ(OB_SUCCESS, task.ret_code_);

  // The failed batch can succeed in the next execution after the retry consumes its error.
  ASSERT_SUCC(scheduler.running_ls_to_tablets_map_.set_refactored(infos[0].ls_id_, infos[0].partition_ids_));
  ASSERT_SUCC(scheduler.running_ls_to_execution_id_.set_refactored(infos[0].ls_id_, 2, true));
  ASSERT_SUCC(task.sql_exec_addrs_.push_back(infos[0].ls_leader_addr_));
  ASSERT_SUCC(task.update_complete_sstable_job_status(infos[0].partition_ids_.at(0),
      infos[0].ls_leader_addr_, 123, 2, OB_SUCCESS, infos[0]));
  ASSERT_EQ(OB_SUCCESS, task.complete_sstable_job_ret_code_);
  ASSERT_EQ(2, task.execution_id_);
  ASSERT_EQ(0, scheduler.running_ls_to_tablets_map_.size());
  ASSERT_EQ(0, scheduler.all_ls_to_tablets_map_.size());
  ASSERT_EQ(0, task.sql_exec_addrs_.count());
}

namespace baseline_failure_test
{
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
static const uint64_t TEST_TENANT_ID = 1002;
static const int64_t SCHEMA_VERSION = 100;

class SchemaVersionResult : public ObMySQLResultImpl
{
public:
  explicit SchemaVersionResult(ObMySQLStatement &statement)
    : ObMySQLResultImpl(statement), pos_(0) {}
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_uint;
  using ObMySQLResultImpl::get_varchar;
  int next() override { return pos_++ == 0 ? OB_SUCCESS : OB_ITER_END; }
  int get_int(const char *column, int64_t &value) const override
  {
    if (0 == strcmp(column, "myport")) {
      value = 2882;
    } else {
      EXPECT_STREQ("version", column);
      value = SCHEMA_VERSION;
    }
    return OB_SUCCESS;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    EXPECT_STREQ("version", column);
    value = SCHEMA_VERSION;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    EXPECT_STREQ("myip", column);
    value = ObString::make_string("127.0.0.1");
    return OB_SUCCESS;
  }
private:
  int pos_;
};

class SchemaVersionResultHandler : public ObISQLResultHandler
{
public:
  SchemaVersionResultHandler() : statement_(), result_(statement_) {}
  ObMySQLResult *mysql_result() override { return &result_; }
private:
  ObMySQLStatement statement_;
  SchemaVersionResult result_;
};

class SchemaVersionProxy : public ObMySQLProxy
{
public:
  SchemaVersionProxy() : read_ret_(OB_SUCCESS), reads_(0) {}
  using ObMySQLProxy::read;
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    ++reads_;
    EXPECT_EQ(TEST_TENANT_ID, tenant_id);
    if (nullptr == strstr(sql, "MAX(schema_version)")) {
      ADD_FAILURE() << "Unexpected SQL: " << sql;
      return OB_NOT_SUPPORTED;
    }
    int ret = read_ret_;
    if (OB_SUCC(ret)) {
      SchemaVersionResultHandler *handler = nullptr;
      ret = res.create_handler(handler);
    }
    return ret;
  }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql,
           const int32_t group_id) override
  {
    return read(res, tenant_id, sql);
  }
  int read_ret_;
  int64_t reads_;
};

class TestBaselineFailureSchema : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    old_schema_service_ = GCTX.schema_service_;
    old_sql_schema_service_ = ObMultiVersionSchemaService::get_instance().schema_service_;
    GCTX.schema_service_ = &ObMultiVersionSchemaService::get_instance();
    GCTX.schema_service_->schema_service_ = &sql_schema_service_;
    old_startup_mode_ = GCTX.startup_mode_;
    old_liboblog_mode_ = ObSchemaService::g_liboblog_mode_;
    GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
    ObSchemaService::g_liboblog_mode_ = true;
    ASSERT_TRUE(GCTX.is_shared_storage_mode());
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    const uint64_t tenant_ids[] = {TEST_TENANT_ID, OB_SERVER_TENANT_ID};
    for (const uint64_t tenant_id : tenant_ids) {
      auto allocator_guard = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
          tenant_id, ObCtxIds::KVSTORE_CACHE_ID);
      if (nullptr == allocator_guard.ref_allocator()) {
        ASSERT_EQ(OB_SUCCESS,
                  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id));
      }
    }
    ASSERT_EQ(OB_SUCCESS, mem_limit_.add_tenant(TEST_TENANT_ID, 0, 256L << 20));
    ASSERT_EQ(OB_SUCCESS, mem_limit_.add_tenant(OB_SERVER_TENANT_ID, 0, INT64_MAX));
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(
        &mem_limit_, 1024, 1L << 30, lib::ACHUNK_SIZE));
    ASSERT_EQ(OB_SUCCESS, ObSysTableChecker::instance().init());
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init(nullptr));
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS,
              ObCompatModeGetter::instance().set_tenant_compat_mode(TEST_TENANT_ID, mode));
    ObMultiVersionSchemaService &service = ObMultiVersionSchemaService::get_instance();
    service.schema_mode_ = ObSimpleSchemaMode::ORIGINAL;
    ASSERT_EQ(OB_SUCCESS, service.schema_refresh_router_.init(ObSimpleSchemaMode::ORIGINAL));
    ASSERT_EQ(OB_SUCCESS, service.schema_cache_.init());
    ASSERT_EQ(OB_SUCCESS, service.schema_getter_controller_.init(&service));
    ASSERT_EQ(OB_SUCCESS, service.schema_store_map_.init(16));
    ASSERT_NO_FATAL_FAILURE(prepare_schema_mgr(sys_mgr_, OB_SYS_TENANT_ID));
    ASSERT_NO_FATAL_FAILURE(prepare_schema_mgr(tenant_mgr_, TEST_TENANT_ID));
  }
  static void TearDownTestCase()
  {
    GCTX.schema_service_->schema_service_ = old_sql_schema_service_;
    GCTX.schema_service_ = old_schema_service_;
    // Release cached schemas before the global KV cache; the singleton owns its schema stores.
    ObMultiVersionSchemaService::get_instance().schema_cache_.destroy();
    ObMultiVersionSchemaService::get_instance().schema_store_map_.destroy();
    sys_mgr_.reset();
    tenant_mgr_.reset();
    ObCompatModeGetter::instance().destroy();
    ObKVGlobalCache::get_instance().destroy();
    mem_limit_.reset();
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
    GCTX.startup_mode_ = old_startup_mode_;
    ObSchemaService::g_liboblog_mode_ = old_liboblog_mode_;
  }
  void SetUp() override
  {
    old_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy_;
  }
  void TearDown() override { GCTX.sql_proxy_ = old_proxy_; }
  static void prepare_schema_mgr(ObSchemaMgr &mgr, const uint64_t tenant_id)
  {
    ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id));
    mgr.set_schema_version(SCHEMA_VERSION);
    ObSimpleTenantSchema tenant;
    tenant.set_tenant_id(TEST_TENANT_ID);
    tenant.set_status(TENANT_STATUS_NORMAL);
    tenant.set_schema_version(SCHEMA_VERSION);
    ASSERT_EQ(OB_SUCCESS, tenant.set_tenant_name("test_tenant"));
    ASSERT_EQ(OB_SUCCESS, mgr.add_tenant(tenant));
    ObSimpleSysVariableSchema vars;
    vars.set_tenant_id(TEST_TENANT_ID);
    vars.set_schema_version(SCHEMA_VERSION);
    vars.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, mgr.sys_variable_mgr_.add_sys_variable(vars));
    ObMultiVersionSchemaService &service = ObMultiVersionSchemaService::get_instance();
    ASSERT_EQ(OB_SUCCESS, service.schema_store_map_.create(tenant_id, 8, 8));
    ObSchemaStore *store = service.schema_store_map_.get(tenant_id);
    ObSchemaMgr *evicted = nullptr;
    ASSERT_EQ(OB_SUCCESS, store->schema_mgr_cache_.put(&mgr, evicted));
    ASSERT_EQ(nullptr, evicted);
    store->update_refreshed_version(SCHEMA_VERSION);
  }
  static void add_index(const uint64_t table_id, const ObIndexStatus status,
                        const ObIndexType index_type = INDEX_TYPE_NORMAL_LOCAL)
  {
    ObTableSchema table;
    table.set_tenant_id(TEST_TENANT_ID);
    table.set_database_id(500000);
    table.set_table_id(table_id);
    table.set_schema_version(SCHEMA_VERSION);
    table.set_table_type(USER_INDEX);
    table.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    char name[64];
    snprintf(name, sizeof(name), "__idx_500007_baseline_target_%lu", table_id);
    ASSERT_SUCC(table.set_table_name(name));
    table.set_data_table_id(500007);
    table.set_index_type(index_type);
    table.set_index_status(status);
    ObSimpleTableSchemaV2 simple;
    auto &service = ObMultiVersionSchemaService::get_instance();
    ASSERT_SUCC(service.convert_to_simple_schema(table, simple));
    ASSERT_SUCC(tenant_mgr_.add_table(simple));
    ASSERT_SUCC(service.schema_cache_.put_schema(
        TABLE_SCHEMA, TEST_TENANT_ID, table_id, SCHEMA_VERSION, table));
  }
  static ObSchemaMgr sys_mgr_;
  static ObSchemaMgr tenant_mgr_;
  static ObSimpleMemLimitGetter mem_limit_;
  static ObSchemaServiceSQLImpl sql_schema_service_;
  static ObMultiVersionSchemaService *old_schema_service_;
  static ObSchemaService *old_sql_schema_service_;
  static decltype(GCTX.startup_mode_) old_startup_mode_;
  static bool old_liboblog_mode_;
  SchemaVersionProxy proxy_;
  ObMySQLProxy *old_proxy_;
};
ObSchemaMgr TestBaselineFailureSchema::sys_mgr_;
ObSchemaMgr TestBaselineFailureSchema::tenant_mgr_;
ObSimpleMemLimitGetter TestBaselineFailureSchema::mem_limit_;
ObSchemaServiceSQLImpl TestBaselineFailureSchema::sql_schema_service_;
ObMultiVersionSchemaService *TestBaselineFailureSchema::old_schema_service_ = nullptr;
ObSchemaService *TestBaselineFailureSchema::old_sql_schema_service_ = nullptr;
decltype(GCTX.startup_mode_) TestBaselineFailureSchema::old_startup_mode_;
bool TestBaselineFailureSchema::old_liboblog_mode_ = false;

TEST_F(TestBaselineFailureSchema, only_exact_error_schema_confirms_terminal_child)
{
  ASSERT_NO_FATAL_FAILURE(add_index(739751, INDEX_STATUS_AVAILABLE));
  ASSERT_NO_FATAL_FAILURE(add_index(739752, INDEX_STATUS_UNAVAILABLE));
  ASSERT_NO_FATAL_FAILURE(add_index(739753, INDEX_STATUS_INDEX_ERROR));
  ObAsyncVectorIndexBuildTask task;
  task.tenant_id_ = TEST_TENANT_ID;
  task.baseline_task_id_ = 42;
  ObDDLTaskRecord child;
  child.task_status_ = ObDDLTaskStatus::FAIL;
  child.ret_code_ = OB_ALLOCATE_MEMORY_FAILED;
  // An unrelated INDEX_ERROR must not confirm failure of an AVAILABLE target.
  const uint64_t ids[] = {739751, 739752, 739753, 739754};
  for (int64_t i = 0; i < 4; ++i) {
    task.target_object_id_ = ids[i];
    const int64_t reads = proxy_.reads_;
    EXPECT_EQ(i == 2, task.is_baseline_failure_confirmed(child));
    EXPECT_GT(proxy_.reads_, reads); // The guard must consult inner-table schema freshness.
  }
  task.target_object_id_ = 739753;
  proxy_.read_ret_ = OB_ERR_UNEXPECTED;
  EXPECT_FALSE(task.is_baseline_failure_confirmed(child));
  proxy_.read_ret_ = OB_SUCCESS;
  const int64_t reads = proxy_.reads_;
  child.ret_code_ = OB_TIMEOUT;
  EXPECT_FALSE(task.is_baseline_failure_confirmed(child));
  child.ret_code_ = OB_ALLOCATE_MEMORY_FAILED;
  child.task_status_ = ObDDLTaskStatus::SUCCESS;
  EXPECT_FALSE(task.is_baseline_failure_confirmed(child));
  EXPECT_EQ(reads, proxy_.reads_);
}

struct IndexStatusTransitionCase
{
  ObIndexStatus old_status_;
  ObIndexType index_type_;
  int64_t task_id_;
  ObIndexStatus new_status_;
  bool rejected_;
};

static const IndexStatusTransitionCase INDEX_STATUS_TRANSITIONS[] = {
    {INDEX_STATUS_AVAILABLE, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, 42, INDEX_STATUS_INDEX_ERROR, true},
    {INDEX_STATUS_UNAVAILABLE, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, 42, INDEX_STATUS_INDEX_ERROR, false},
    {INDEX_STATUS_INDEX_ERROR, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, 42, INDEX_STATUS_INDEX_ERROR, false},
    {INDEX_STATUS_AVAILABLE, INDEX_TYPE_NORMAL_LOCAL, 42, INDEX_STATUS_INDEX_ERROR, false},
    {INDEX_STATUS_AVAILABLE, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, 0, INDEX_STATUS_INDEX_ERROR, false},
    {INDEX_STATUS_AVAILABLE, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, -1, INDEX_STATUS_INDEX_ERROR, false},
    {INDEX_STATUS_INDEX_ERROR, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, 42, INDEX_STATUS_AVAILABLE, true},
};

TEST_F(TestBaselineFailureSchema, legacy_status_update_rejects_late_async_ivf_error)
{
  ObDDLService ddl;
  obrpc::ObSrvRpcProxy rpc;
  ObLSTableOperator ls_operator;
  ObTenantDDLService tenant_ddl;
  ddl.inited_ = true;
  ddl.schema_service_ = &ObMultiVersionSchemaService::get_instance();
  ddl.sql_proxy_ = &proxy_;
  ddl.rpc_proxy_ = &rpc;
  ddl.lst_operator_ = &ls_operator;
  ddl.tenant_ddl_service_ = &tenant_ddl;
  ASSERT_SUCC(ddl.check_inner_stat());
  ASSERT_EQ(nullptr, proxy_.get_pool());
  int64_t i = 0;
  for (const auto &transition : INDEX_STATUS_TRANSITIONS) {
    SCOPED_TRACE(i);
    const uint64_t index_id = 740000 + i++;
    ASSERT_NO_FATAL_FAILURE(add_index(index_id, transition.old_status_, transition.index_type_));
    obrpc::ObUpdateIndexStatusArg arg;
    arg.exec_tenant_id_ = TEST_TENANT_ID;
    arg.index_table_id_ = index_id;
    arg.status_ = transition.new_status_;
    arg.task_id_ = transition.task_id_;
    arg.convert_status_ = false;
    const int64_t reads = proxy_.reads_;
    // An allowed transition reaches transaction acquisition on the uninitialized proxy.
    // The null pool returns OB_INVALID_ARGUMENT there; no schema write or commit occurs.
    EXPECT_EQ(transition.rejected_ ? OB_EAGAIN : OB_INVALID_ARGUMENT, ddl.update_index_status(arg));
    EXPECT_GT(proxy_.reads_, reads);
  }
}

TEST_F(TestBaselineFailureSchema, parallel_status_generation_rejects_late_async_ivf_error)
{
  auto &service = ObMultiVersionSchemaService::get_instance();
  ObDDLService ddl;
  int64_t i = 0;
  for (const auto &transition : INDEX_STATUS_TRANSITIONS) {
    SCOPED_TRACE(i);
    // Cached schemas precede helper construction and outlive all its borrowed pointers.
    ObDatabaseSchema database;
    database.set_tenant_id(TEST_TENANT_ID);
    database.set_database_id(500000);
    ASSERT_SUCC(database.set_database_name("baseline_db"));
    ObTableSchema data;
    data.set_tenant_id(TEST_TENANT_ID);
    data.set_database_id(500000);
    data.set_table_id(500007);
    data.set_table_type(USER_TABLE);
    ASSERT_SUCC(data.set_table_name("baseline_data"));
    ObTableSchema index;
    index.set_tenant_id(TEST_TENANT_ID);
    index.set_database_id(500000);
    index.set_table_id(741000 + i++);
    index.set_data_table_id(data.get_table_id());
    index.set_table_type(USER_INDEX);
    index.set_index_type(transition.index_type_);
    index.set_index_status(transition.old_status_);
    ASSERT_SUCC(index.set_table_name("baseline_index"));
    obrpc::ObUpdateIndexStatusArg arg;
    arg.exec_tenant_id_ = TEST_TENANT_ID;
    arg.database_name_ = ObString::make_string("baseline_db");
    arg.data_table_id_ = data.get_table_id();
    arg.index_table_id_ = index.get_table_id();
    arg.status_ = transition.new_status_;
    arg.task_id_ = transition.task_id_;
    arg.convert_status_ = false;
    obrpc::ObParallelDDLRes result;
    ObUpdateIndexStatusHelper helper(&service, TEST_TENANT_ID, arg, result);
    helper.inited_ = true;
    helper.ddl_service_ = &ddl;
    helper.sql_proxy_ = &proxy_;
    helper.ddl_trans_controller_ = &service.get_ddl_trans_controller();
    helper.database_id_ = database.get_database_id();
    auto &guard = helper.schema_guard_wrapper_.latest_schema_guard_;
    ASSERT_FALSE(helper.schema_guard_wrapper_.is_local_guard_);
    SchemaObj objects[3];
    const ObSchemaType types[] = {DATABASE_SCHEMA, TABLE_SCHEMA, TABLE_SCHEMA};
    const uint64_t ids[] = {database.get_database_id(), data.get_table_id(), index.get_table_id()};
    ObSchema *schemas[] = {&database, &data, &index};
    for (int64_t j = 0; j < 3; ++j) {
      objects[j].schema_type_ = types[j];
      objects[j].tenant_id_ = TEST_TENANT_ID;
      objects[j].schema_id_ = ids[j];
      objects[j].schema_ = schemas[j];
      ASSERT_SUCC(guard.schema_objs_.push_back(objects[j]));
    }
    ASSERT_SUCC(helper.check_inner_stat_());
    EXPECT_EQ(transition.rejected_ ? OB_EAGAIN : OB_SUCCESS, helper.generate_schemas_());
    if (transition.rejected_) {
      EXPECT_EQ(nullptr, helper.new_data_table_schema_);
    } else {
      ASSERT_NE(nullptr, helper.new_data_table_schema_);
      EXPECT_EQ(data.get_table_id(), helper.new_data_table_schema_->get_table_id());
    }
  }
}

} // namespace baseline_failure_test

class QueueOwnershipTask : public ObDDLTask
{
public:
  QueueOwnershipTask(int &destroy_count, const uint64_t object_id, const int64_t task_id,
                     const int64_t parent_task_id = 1)
    : destroy_count_(destroy_count)
  {
    dst_tenant_id_ = 1001;
    target_object_id_ = object_id;
    dst_schema_version_ = 1;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
  }
  ~QueueOwnershipTask() override { ++destroy_count_; }
private:
  int &destroy_count_;
};

TEST(test_ddl_task_struct, queue_push_preserves_scheduling_readiness)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(task.get()));
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  EXPECT_EQ(0, next->next_schedule_ts_);
  EXPECT_TRUE(next->get_sys_task_id().is_invalid());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(next->get_task_key(), mapped));
  EXPECT_EQ(next, mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(next->get_ddl_task_id(), mapped));
  EXPECT_EQ(next, mapped);
  ASSERT_SUCC(queue.remove_task(next));
  EXPECT_EQ(0, queue.get_task_cnt());
  EXPECT_EQ(0, queue.task_map_.size());
  EXPECT_EQ(0, queue.task_id_map_.size());
  task.reset();
  EXPECT_EQ(1, destroy_count);
}

TEST(test_ddl_task_struct, queue_duplicate_keeps_caller_ownership)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> original(new QueueOwnershipTask(destroy_count, 100, 1));
  std::unique_ptr<QueueOwnershipTask> duplicate_key(new QueueOwnershipTask(destroy_count, 100, 2));
  std::unique_ptr<QueueOwnershipTask> duplicate_id(new QueueOwnershipTask(destroy_count, 101, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(original.get()));
  ASSERT_EQ(OB_ENTRY_EXIST, queue.push_task(duplicate_key.get()));
  ASSERT_EQ(OB_ENTRY_EXIST, queue.push_task(duplicate_id.get()));
  ASSERT_EQ(0, duplicate_key->next_schedule_ts_);
  ASSERT_EQ(0, duplicate_id->next_schedule_ts_);
  ASSERT_EQ(0, original->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(1, queue.task_map_.size());
  ASSERT_EQ(1, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(original->get_task_key(), mapped));
  ASSERT_EQ(original.get(), mapped);
  mapped = nullptr;
  ASSERT_SUCC(queue.task_id_map_.get_refactored(original->get_ddl_task_id(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_EQ(OB_HASH_NOT_EXIST,
            queue.task_map_.get_refactored(duplicate_id->get_task_key(), mapped));
  ASSERT_EQ(OB_HASH_NOT_EXIST,
            queue.task_id_map_.get_refactored(duplicate_key->get_ddl_task_id(), mapped));
  ASSERT_EQ(0, destroy_count);
  duplicate_key.reset();
  duplicate_id.reset();
  ASSERT_EQ(2, destroy_count);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(original.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  original.reset();
  ASSERT_EQ(3, destroy_count);
}

TEST(test_ddl_task_struct, queue_stop_rejects_before_publish)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  ASSERT_TRUE(queue.has_set_stop());
  ASSERT_EQ(OB_STATE_NOT_MATCH, queue.push_task(task.get()));
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, destroy_count);
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(task.get()));
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(0, destroy_count);
  int rejected_destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> rejected(
      new QueueOwnershipTask(rejected_destroy_count, 101, 2));
  queue.set_stop(true);
  ASSERT_EQ(OB_STATE_NOT_MATCH, queue.push_task(rejected.get()));
  ASSERT_EQ(0, rejected->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(1, queue.task_map_.size());
  ASSERT_EQ(1, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(task->get_task_key(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(task->get_ddl_task_id(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_EQ(OB_HASH_NOT_EXIST, queue.task_map_.get_refactored(rejected->get_task_key(), mapped));
  ASSERT_EQ(OB_HASH_NOT_EXIST, queue.task_id_map_.get_refactored(rejected->get_ddl_task_id(), mapped));
  ASSERT_EQ(0, rejected_destroy_count);
  rejected.reset();
  ASSERT_EQ(1, rejected_destroy_count);
  ASSERT_EQ(0, destroy_count);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  task.reset();
  ASSERT_EQ(1, destroy_count);
}

TEST(test_ddl_task_struct, queue_sys_task_registration_failure_does_not_reject_task)
{
  class SysTaskAddressGuard
  {
  public:
    SysTaskAddressGuard() : saved_addr_(SYS_TASK_STATUS_MGR.self_addr_)
    {
      SYS_TASK_STATUS_MGR.self_addr_.reset();
    }
    ~SysTaskAddressGuard() { SYS_TASK_STATUS_MGR.self_addr_ = saved_addr_; }
  private:
    ObAddr saved_addr_;
  } address_guard;
  const int64_t registered_before = SYS_TASK_STATUS_MGR.task_array_.count();
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1, 0));
  std::unique_ptr<QueueOwnershipTask> original(new QueueOwnershipTask(destroy_count, 101, 2));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(original.get()));
  ASSERT_SUCC(queue.push_task(task.get()));
  ASSERT_EQ(0, destroy_count);
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_TRUE(task->get_sys_task_id().is_invalid());
  ASSERT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
  ASSERT_EQ(2, queue.get_task_cnt());
  ASSERT_EQ(2, queue.task_map_.size());
  ASSERT_EQ(2, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(task->get_task_key(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(task->get_ddl_task_id(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(original->get_task_key(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(original->get_ddl_task_id(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(original.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  ASSERT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
  original.reset();
  task.reset();
  ASSERT_EQ(2, destroy_count);
}

class ProtectedQueueOwnershipTask
{
public:
  ProtectedQueueOwnershipTask() : mapping_(MAP_FAILED), mapping_size_(0), task_(nullptr) {}
  ~ProtectedQueueOwnershipTask()
  {
    if (nullptr != task_) {
      task_->~QueueOwnershipTask();
    }
    if (MAP_FAILED != mapping_) {
      EXPECT_EQ(0, munmap(mapping_, mapping_size_));
    }
  }
  int init(int &destroy_count, const uint64_t object_id, const int64_t task_id,
           const int64_t parent_task_id)
  {
    int ret = OB_SUCCESS;
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
      ret = OB_ERR_SYS;
    } else {
      mapping_size_ = (sizeof(QueueOwnershipTask) + page_size - 1) / page_size * page_size;
      mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (MAP_FAILED == mapping_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        task_ = new (mapping_) QueueOwnershipTask(
            destroy_count, object_id, task_id, parent_task_id);
      }
    }
    return ret;
  }
  QueueOwnershipTask *get() const { return task_; }
  int destroy_and_protect()
  {
    task_->~QueueOwnershipTask();
    task_ = nullptr;
    return 0 == mprotect(mapping_, mapping_size_, PROT_NONE) ? OB_SUCCESS : OB_ERR_SYS;
  }
private:
  void *mapping_;
  size_t mapping_size_;
  QueueOwnershipTask *task_;
};

class DDLTaskSubmitter
{
public:
  DDLTaskSubmitter(ObDDLScheduler &scheduler, ObDDLTask *task, const ObDDLTaskRecord &record,
                   int &schedule_ret, std::atomic<bool> &completed)
    : scheduler_(scheduler), task_(task), record_(record), schedule_ret_(schedule_ret),
      completed_(completed) {}
  void run()
  {
    schedule_ret_ = scheduler_.inner_schedule_ddl_task(task_, record_);
    completed_.store(true);
  }
private:
  ObDDLScheduler &scheduler_;
  ObDDLTask *task_;
  const ObDDLTaskRecord &record_;
  int &schedule_ret_;
  std::atomic<bool> &completed_;
};

class DDLSimTestConfig : public ObConfigIntListItem
{
public:
  explicit DDLSimTestConfig(const char *name)
    : ObConfigIntListItem(nullptr, Scope::TENANT, name, "", "", ObParameterAttr()) {}
protected:
  const char *value_default_ptr() const override { return ""; }
};

class DDLSimConfigUpdater
{
public:
  DDLSimConfigUpdater(const uint64_t tenant_id, const ObConfigIntListItem &random_param,
                      const ObConfigIntListItem &fixed_param, int &update_ret)
    : tenant_id_(tenant_id), random_param_(random_param), fixed_param_(fixed_param),
      update_ret_(update_ret) {}
  void run()
  {
    update_ret_ = share::ObDDLSimPointMgr::get_instance().set_tenant_param(
        tenant_id_, random_param_, fixed_param_);
  }
private:
  uint64_t tenant_id_;
  const ObConfigIntListItem &random_param_;
  const ObConfigIntListItem &fixed_param_;
  int &update_ret_;
};

// Recovery may resume only after the selected fault is visible in the sim manager.
TEST(test_ddl_task_struct, ddl_sim_ready_follows_config_application)
{
  class SyncActionGuard
  {
  public:
    explicit SyncActionGuard(const ObDebugSyncPoint point)
      : point_(point), saved_timeout_(GCONF.debug_sync_timeout.str()) {}
    ~SyncActionGuard()
    {
      ObDebugSyncAction action;
      action.sync_point_ = point_;
      EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, true, action));
      EXPECT_TRUE(GCONF.debug_sync_timeout.set_value(saved_timeout_.c_str()));
    }
  private:
    ObDebugSyncPoint point_;
    std::string saved_timeout_;
  };
  struct SimCase
  {
    const char *value_;
    ObDebugSyncPoint ready_point_;
    share::ObDDLSimPointID sim_point_;
  };
  const SimCase cases[] = {
    {"17", AFTER_DDL_SCHEDULER_STOPPED_SIM_APPLIED, share::DDL_SCHEDULER_STOPPED},
    {"8", AFTER_DDL_QUEUE_PUSH_FAILED_SIM_APPLIED, share::PUSH_TASK_INTO_QUEUE_FAILED},
    {"18", AFTER_DDL_SYS_TASK_FAILED_SIM_APPLIED, share::DDL_SCHEDULER_ADD_SYS_TASK_FAILED}
  };
  share::ObDDLSimPointMgr &manager = share::ObDDLSimPointMgr::get_instance();
  const uint64_t tenant_id = 1001;
  for (const SimCase &sim_case : cases) {
    SyncActionGuard environment(sim_case.ready_point_);
    ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
    DDLSimTestConfig random_param("ut_random");
    DDLSimTestConfig fixed_param("ut_fixed");
    ASSERT_TRUE(fixed_param.set_value(sim_case.value_));
    ASSERT_TRUE(random_param.valid());
    ASSERT_TRUE(fixed_param.valid());
    ObDebugSyncAction action;
    action.sync_point_ = sim_case.ready_point_;
    action.timeout_ = 10 * 1000 * 1000;
    action.execute_ = 1;
    ASSERT_SUCC(action.signal_.assign("ddl_sim_applied"));
    ASSERT_SUCC(action.wait_.assign("ddl_sim_release"));
    ASSERT_SUCC(GDS.set_global_action(false, false, action));
    int update_ret = OB_NOT_INIT;
    DDLSimConfigUpdater context(tenant_id, random_param, fixed_param, update_ret);
    std::thread updater(&DDLSimConfigUpdater::run, &context);
    const int ready_ret = GDS.event_control_.wait(action.signal_, action.timeout_, false);
    bool ready = false;
    {
      // WAIT_FOR has a soft timeout; inspect the signal under the event lock.
      ObThreadCondGuard guard(GDS.event_control_.cond_);
      DLIST_FOREACH_NORET(event, GDS.event_control_.used_) {
        if (event->name_ == action.signal_ && event->signal_cnt_ > 0) {
          ready = true;
        }
      }
    }
    const uint64_t task_id = 100000 + static_cast<uint64_t>(sim_case.sim_point_);
    share::ObDDLSimPointMgr::TaskSimPoint key(tenant_id, task_id, sim_case.sim_point_);
    int sim_ret = OB_SUCCESS;
    int stat_ret = OB_HASH_NOT_EXIST;
    int64_t trigger_count = 0;
    if (OB_SUCCESS == ready_ret && ready) {
      sim_ret = manager.try_sim(tenant_id, task_id, {sim_case.sim_point_});
      stat_ret = manager.task_sim_map_.get_refactored(key, trigger_count);
    }
    const int release_ret = GDS.event_control_.signal(action.wait_);
    updater.join();
    GDS.event_control_.wait(action.signal_, 1, true);
    GDS.event_control_.wait(action.wait_, 1, true);
    EXPECT_TRUE(fixed_param.set_value(""));
    EXPECT_EQ(OB_SUCCESS, manager.set_tenant_param(tenant_id, random_param, fixed_param));
    if (OB_SUCCESS == stat_ret) {
      EXPECT_EQ(OB_SUCCESS, manager.task_sim_map_.erase_refactored(key));
    }
    EXPECT_EQ(OB_SUCCESS, ready_ret);
    EXPECT_TRUE(ready);
    EXPECT_EQ(OB_SUCCESS, update_ret);
    EXPECT_NE(OB_SUCCESS, sim_ret);
    EXPECT_EQ(OB_SUCCESS, stat_ret);
    EXPECT_EQ(1, trigger_count);
    EXPECT_EQ(OB_SUCCESS, release_ret);
  }
}

// A heartbeat error after publication must leave the task alive and owned by the queue.
TEST(test_ddl_task_struct, heartbeat_failure_keeps_published_task_alive)
{
  ObAddr self;
  ASSERT_TRUE(self.set_ip_addr("127.0.0.1", 2882));
  ObCurTraceId::TraceId trace_id;
  trace_id.init(self);
  const share::ObDDLType types[] = {share::DDL_DIRECT_LOAD, share::DDL_DIRECT_LOAD_INSERT};
  for (const share::ObDDLType type : types) {
    ObTableRedefinitionTask source;
    source.tenant_id_ = source.dst_tenant_id_ = 1001;
    source.schema_version_ = source.dst_schema_version_ = 1;
    source.object_id_ = 100;
    source.target_object_id_ = 101;
    source.task_id_ = source.parent_task_id_ = 1;
    source.task_type_ = type;
    source.task_version_ = ObTableRedefinitionTask::OB_TABLE_REDEFINITION_TASK_VERSION;
    source.parallelism_ = source.data_format_version_ = 1;
    source.consumer_group_id_ = 0;
    ASSERT_SUCC(source.set_trace_id(trace_id));
    source.alter_table_arg_.alter_table_schema_.set_tenant_id(1001);
    source.alter_table_arg_.alter_table_schema_.set_schema_version(1);
    source.alter_table_arg_.alter_table_schema_.origin_database_name_ =
        ObString::make_string("test_db");
    source.alter_table_arg_.alter_table_schema_.origin_table_name_ =
        ObString::make_string("test_table");
    source.alter_table_arg_.tz_info_wrap_.set_tz_info_offset(0);
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      source.alter_table_arg_.nls_formats_[i] = ObString::make_string("NLS_TEST_FORMAT");
    }
    ObArenaAllocator allocator(lib::ObLabel("DDLTaskUT"), OB_MALLOC_NORMAL_BLOCK_SIZE,
                               OB_SERVER_TENANT_ID, 0);
    ObDDLTaskRecord record;
    ASSERT_SUCC(source.convert_to_record(record, allocator));
    ASSERT_TRUE(record.is_valid());
    ObDDLScheduler scheduler;
    ASSERT_SUCC(scheduler.allocator_.init(
        OB_MALLOC_BIG_BLOCK_SIZE, lib::ObLabel("DDLTaskUT"), OB_SYS_TENANT_ID, 64L << 20));
    ASSERT_SUCC(scheduler.task_queue_.init(16));
    scheduler.is_inited_ = true;
    scheduler.set_stop(false);
    scheduler.is_stop_ = false;
    scheduler.task_queue_.set_stop(false);
    // Leave the heartbeat manager uninitialized to exercise its actual error return.
    EXPECT_EQ(OB_NOT_INIT, scheduler.schedule_table_redefinition_task(record));
    EXPECT_EQ(1, scheduler.task_queue_.get_task_cnt());
    // Purge unused blocks; a live queued task must still keep an allocation.
    scheduler.allocator_.purge();
    ASSERT_GT(scheduler.allocator_.allocated(), 0);
    ObDDLTask *next = nullptr;
    ASSERT_SUCC(scheduler.task_queue_.get_next_task(next));
    ASSERT_NE(nullptr, next);
    EXPECT_TRUE(next->is_valid());
    EXPECT_EQ(type, next->get_task_type());
    EXPECT_EQ(record.task_id_, next->get_task_id());
    ASSERT_SUCC(scheduler.remove_ddl_task(next));
    scheduler.allocator_.purge();
    EXPECT_EQ(0, scheduler.allocator_.allocated());
    EXPECT_EQ(0, scheduler.task_queue_.task_map_.size());
    EXPECT_EQ(0, scheduler.task_queue_.task_id_map_.size());
    // A rejected publication still belongs to the caller and must be released on failure.
    scheduler.task_queue_.set_stop(true);
    EXPECT_EQ(OB_STATE_NOT_MATCH, scheduler.schedule_table_redefinition_task(record));
    scheduler.allocator_.purge();
    EXPECT_EQ(0, scheduler.allocator_.allocated());
    EXPECT_EQ(0, scheduler.task_queue_.get_task_cnt());
  }
}

// Inaccessible destroyed storage makes any resumed submitter access fail without ASan.
TEST(test_ddl_task_struct, scheduler_consumer_frees_before_submitter_resumes)
{
  class ScheduleEnvironmentGuard
  {
  public:
    ScheduleEnvironmentGuard()
      : saved_addr_(SYS_TASK_STATUS_MGR.self_addr_),
        saved_timeout_(GCONF.debug_sync_timeout.str()) {}
    ~ScheduleEnvironmentGuard()
    {
      ObDebugSyncAction action;
      action.sync_point_ = AFTER_DDL_TASK_QUEUE_PUSH;
      GDS.set_global_action(false, true, action);
      GCONF.debug_sync_timeout.set_value(saved_timeout_.c_str());
      SYS_TASK_STATUS_MGR.self_addr_ = saved_addr_;
    }
  private:
    ObAddr saved_addr_;
    std::string saved_timeout_;
  } environment;
  ObAddr self;
  ASSERT_TRUE(self.set_ip_addr("127.0.0.1", 2882));
  ASSERT_SUCC(SYS_TASK_STATUS_MGR.set_self_addr(self));
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
  ObDebugSyncAction action;
  action.sync_point_ = AFTER_DDL_TASK_QUEUE_PUSH;
  action.timeout_ = 10 * 1000 * 1000;
  action.execute_ = 1;
  ASSERT_SUCC(action.signal_.assign("ddl_schedule_arrived"));
  ASSERT_SUCC(action.wait_.assign("ddl_schedule_resume"));
  ASSERT_SUCC(GDS.set_global_action(false, false, action));
  ASSERT_TRUE(GDS.global_actions_.is_active(AFTER_DDL_TASK_QUEUE_PUSH));
  const int64_t registered_before = SYS_TASK_STATUS_MGR.task_array_.count();
  int destroy_count = 0;
  ProtectedQueueOwnershipTask task;
  ASSERT_SUCC(task.init(destroy_count, 100, 1, 0));
  ObDDLTask *const submitted = task.get();
  ObDDLTaskRecord record;
  record.gmt_create_ = 123;
  ObDDLScheduler scheduler;
  ASSERT_SUCC(scheduler.task_queue_.init(16));
  scheduler.set_stop(false);
  scheduler.is_stop_ = false;
  scheduler.task_queue_.set_stop(false);
  scheduler.rs_epoch_ = 42;
  int schedule_ret = OB_ERR_UNEXPECTED;
  std::atomic<bool> completed(false);
  DDLTaskSubmitter submitter_context(scheduler, submitted, record, schedule_ret, completed);
  std::thread submitter(&DDLTaskSubmitter::run, &submitter_context);
  const int arrived_ret = GDS.event_control_.wait(action.signal_, action.timeout_, false);
  bool arrived = false;
  {
    ObThreadCondGuard guard(GDS.event_control_.cond_);
    DLIST_FOREACH_NORET(event, GDS.event_control_.used_) {
      if (event->name_ == action.signal_ && event->signal_cnt_ > 0) {
        arrived = true;
      }
    }
  }
  if (arrived) {
    GDS.event_control_.wait(action.signal_, action.timeout_, true);
  }
  int pop_ret = OB_ERR_UNEXPECTED;
  int unregister_ret = OB_ERR_UNEXPECTED;
  int remove_ret = OB_ERR_UNEXPECTED;
  int protect_ret = OB_ERR_UNEXPECTED;
  bool same_task = false;
  bool enabled_before_resume = false;
  bool registered_before_resume = false;
  int64_t rs_epoch = 0;
  uint64_t gmt_create = 0;
  if (OB_SUCCESS == arrived_ret && arrived) {
    ObDDLTask *next = nullptr;
    pop_ret = scheduler.task_queue_.get_next_task(next);
    same_task = (next == submitted);
    if (OB_SUCCESS == pop_ret && same_task) {
      enabled_before_resume = (0 == next->next_schedule_ts_);
      registered_before_resume = !next->get_sys_task_id().is_invalid();
      rs_epoch = next->get_rs_epoch_snapshot();
      gmt_create = next->get_gmt_create();
      unregister_ret = scheduler.remove_sys_task(next);
      remove_ret = scheduler.task_queue_.remove_task(next);
      if (OB_SUCCESS == remove_ret) {
        protect_ret = task.destroy_and_protect();
      }
    }
  }
  const int destroyed_before_resume = destroy_count;
  const bool completed_before_resume = completed.load();
  const int signal_ret = GDS.event_control_.signal(action.wait_);
  submitter.join();
  GDS.event_control_.wait(action.signal_, 1, true);
  GDS.event_control_.wait(action.wait_, 1, true);
  if (nullptr != task.get()) {
    ObDDLTask *remaining = nullptr;
    if (OB_SUCCESS == scheduler.task_queue_.get_next_task(remaining)) {
      scheduler.remove_sys_task(remaining);
      scheduler.task_queue_.remove_task(remaining);
    }
  }
  EXPECT_EQ(OB_SUCCESS, arrived_ret);
  EXPECT_TRUE(arrived);
  EXPECT_FALSE(completed_before_resume);
  EXPECT_EQ(OB_SUCCESS, pop_ret);
  EXPECT_TRUE(same_task);
  EXPECT_TRUE(enabled_before_resume);
  EXPECT_TRUE(registered_before_resume);
  EXPECT_EQ(42, rs_epoch);
  EXPECT_EQ(123, gmt_create);
  EXPECT_EQ(OB_SUCCESS, unregister_ret);
  EXPECT_EQ(OB_SUCCESS, remove_ret);
  EXPECT_EQ(1, destroyed_before_resume);
  EXPECT_EQ(OB_SUCCESS, protect_ret);
  EXPECT_EQ(OB_SUCCESS, signal_ret);
  EXPECT_EQ(OB_SUCCESS, schedule_ret);
  EXPECT_EQ(1, destroy_count);
  EXPECT_EQ(0, scheduler.task_queue_.get_task_cnt());
  EXPECT_EQ(0, scheduler.task_queue_.task_map_.size());
  EXPECT_EQ(0, scheduler.task_queue_.task_id_map_.size());
  EXPECT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
}

TEST(test_ddl_task_struct, ddl_slice_info)
{
  // 1. default construct
  ObDDLSliceInfo slice_info;
  ASSERT_FALSE(slice_info.is_valid());

  // 2. push back an tablet range
  sql::ObPxTabletRange tablet_range;
  tablet_range.tablet_id_ = 0;
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_TRUE(slice_info.is_valid());

  // 3. test assign
  ObDDLSliceInfo assign_slice_info;
  tablet_range.tablet_id_ = 1;
  sql::ObPxTabletRange::DatumKey endkey;
  char test_string[32] = { 0 };
  const char *hello_str = "hello oceanbase";
  memcpy(test_string, hello_str, strlen(hello_str));
  ASSERT_SUCC(endkey.prepare_allocate(2));
  char datum_buffer[16] = { 0 };
  endkey.at(0).ptr_ = datum_buffer;
  endkey.at(0).set_int(100);
  endkey.at(1).ptr_ = datum_buffer + 8;
  endkey.at(1).set_string(test_string, sizeof(test_string));
  ASSERT_SUCC(tablet_range.range_cut_.push_back(endkey));
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_SUCC(assign_slice_info.assign(slice_info));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);

  // 4. test deep copy
  ObDDLSliceInfo deep_copied_slice_info;
  ObArenaAllocator arena;
  ASSERT_SUCC(deep_copied_slice_info.deep_copy(slice_info, arena));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);
  memset(test_string, 0, sizeof(test_string));
  LOG_INFO("after deep copy", K(slice_info), K(deep_copied_slice_info), K(assign_slice_info));
  ASSERT_EQ(0, strcmp(deep_copied_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));
  ASSERT_NE(0, strcmp(     assign_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));

  // 5. reset
  slice_info.reset();
  ASSERT_FALSE(slice_info.is_valid());
}

class TerminalResultSqlProxy : public ObMySQLProxy
{
public:
  TerminalResultSqlProxy() : write_ret_(OB_SUCCESS), writes_(0), reads_(0), persisted_ret_code_(OB_SUCCESS)
  {
    EXPECT_EQ(OB_SUCCESS, init(&pool_));
  }
  int write(const uint64_t, const char *sql, int64_t &affected_rows) override
  {
    ++writes_;
    const char *ret_code = strstr(sql, "SET ret_code=");
    EXPECT_NE(nullptr, ret_code);
    if (OB_SUCCESS == write_ret_ && nullptr != ret_code) {
      EXPECT_EQ(1, sscanf(ret_code, "SET ret_code=%d", &persisted_ret_code_));
    }
    affected_rows = OB_SUCCESS == write_ret_ ? 1 : 0;
    return write_ret_;
  }
  int read(ReadResult &, const uint64_t, const char *) override
  {
    ++reads_;
    return OB_TIMEOUT; // A parent-row lookup would fail this test.
  }
  int write_ret_;
  int64_t writes_;
  int64_t reads_;
  int persisted_ret_code_;
  sqlclient::ObMySQLConnectionPool pool_;
};

class TestAsyncVectorChildCleanup : public testing::Test
{
protected:
  void SetUp() override
  {
    old_proxy_ = GCTX.sql_proxy_;
    old_root_service_ = GCTX.root_service_;
    old_ddl_started_ = ObDDLServiceLauncher::is_ddl_service_started();
    GCTX.sql_proxy_ = &proxy_;
    GCTX.root_service_ = &root_service_;
    ObDDLServiceLauncher::is_ddl_service_started_ = true;
    init_child(child_);
  }
  void TearDown() override
  {
    GCTX.sql_proxy_ = old_proxy_;
    GCTX.root_service_ = old_root_service_;
    ObDDLServiceLauncher::is_ddl_service_started_ = old_ddl_started_;
  }
  void init_child(ObIndexBuildTask &task)
  {
    task.is_inited_ = true;
    task.tenant_id_ = 1001;
    task.dst_tenant_id_ = 1001;
    task.task_id_ = 7002;
    task.task_type_ = share::ObDDLType::DDL_CREATE_INDEX;
    task.parent_task_id_ = 7001;
    task.task_status_ = share::ObDDLTaskStatus::SUCCESS;
    task.snapshot_version_ = 9001;
    task.object_id_ = 5001;
    task.target_object_id_ = 5002;
    task.index_table_id_ = 5002;
    task.schema_version_ = 4001;
    task.dst_schema_version_ = 4001;
    task.task_version_ = 1;
    task.data_format_version_ = DATA_VERSION_4_6_3_1;
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
    ObCurTraceId::TraceId trace_id;
    trace_id.init(addr);
    ASSERT_SUCC(task.set_trace_id(trace_id));
    task.create_index_arg_.tenant_id_ = 1001;
    task.create_index_arg_.exec_tenant_id_ = 1001;
    task.create_index_arg_.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  }
  void restore_child(ObIndexBuildTask &recovered, const bool persisted_result)
  {
    ObArenaAllocator allocator("CleanupTest");
    ObDDLTaskRecord record;
    ASSERT_SUCC(child_.convert_to_record(record, allocator));
    ASSERT_TRUE(record.is_valid());
    if (persisted_result) {
      record.ret_code_ = proxy_.persisted_ret_code_;
    }
    ASSERT_SUCC(recovered.init(record));
    ASSERT_EQ(child_.task_id_, recovered.task_id_);
    ASSERT_EQ(child_.parent_task_id_, recovered.parent_task_id_);
    ASSERT_EQ(child_.get_data_format_version(), recovered.get_data_format_version());
    ASSERT_EQ(child_.task_status_, recovered.task_status_);
    ASSERT_EQ(child_.ret_code_, recovered.ret_code_);
  }

  ObMySQLProxy *old_proxy_;
  ObRootService *old_root_service_;
  bool old_ddl_started_;
  ObRootService root_service_;
  TerminalResultSqlProxy proxy_;
  ObIndexBuildTask child_;
};

TEST_F(TestAsyncVectorChildCleanup, ownership_survives_recovery_before_terminal_cleanup)
{
  child_.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  ASSERT_TRUE(child_.is_parent_managed_async_vector_child());
  ObIndexBuildTask recovered;
  restore_child(recovered, false);
  GCTX.sql_proxy_ = nullptr;
  ASSERT_TRUE(recovered.is_parent_managed_async_vector_child());
  ASSERT_EQ(0, proxy_.reads_);
  ASSERT_EQ(0, proxy_.writes_);
}

TEST_F(TestAsyncVectorChildCleanup, failed_result_write_is_retried_before_reporting)
{
  child_.task_status_ = share::ObDDLTaskStatus::FAIL;
  proxy_.write_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, child_.persist_terminal_result());
  ASSERT_EQ(OB_SUCCESS, child_.ret_code_);
  ASSERT_EQ(OB_SUCCESS, proxy_.persisted_ret_code_);
  ASSERT_TRUE(child_.is_parent_managed_async_vector_child());
  proxy_.write_ret_ = OB_SUCCESS;
  ASSERT_SUCC(child_.persist_terminal_result());
  ASSERT_EQ(OB_ERR_ADD_INDEX, child_.ret_code_);
  ASSERT_EQ(OB_ERR_ADD_INDEX, proxy_.persisted_ret_code_);
  ASSERT_EQ(0, proxy_.reads_);
  ASSERT_EQ(2, proxy_.writes_);
}

TEST_F(TestAsyncVectorChildCleanup, success_reenters_after_failed_delete_and_deleted_parent)
{
  ASSERT_SUCC(child_.persist_terminal_result());
  // Reporting succeeded but DELETE failed: recover the same remaining child.
  ObIndexBuildTask recovered;
  restore_child(recovered, true);
  GCTX.sql_proxy_ = nullptr;
  ASSERT_TRUE(recovered.is_parent_managed_async_vector_child());
  ASSERT_EQ(9001, recovered.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, recovered.ret_code_);
  GCTX.sql_proxy_ = &proxy_;
  ASSERT_SUCC(recovered.persist_terminal_result());
  ASSERT_EQ(OB_SUCCESS, proxy_.persisted_ret_code_);
  ASSERT_EQ(0, proxy_.reads_);
}

TEST_F(TestAsyncVectorChildCleanup, cancellation_reenters_after_failed_delete_and_deleted_parent)
{
  child_.task_status_ = share::ObDDLTaskStatus::FAIL;
  child_.ret_code_ = OB_CANCELED;
  ASSERT_SUCC(child_.persist_terminal_result());
  ObIndexBuildTask recovered;
  restore_child(recovered, true);
  GCTX.sql_proxy_ = nullptr;
  ASSERT_TRUE(recovered.is_parent_managed_async_vector_child());
  ASSERT_EQ(OB_CANCELED, recovered.ret_code_);
  GCTX.sql_proxy_ = &proxy_;
  ASSERT_SUCC(recovered.persist_terminal_result());
  ASSERT_EQ(OB_CANCELED, proxy_.persisted_ret_code_);
  ASSERT_EQ(0, proxy_.reads_);
}

TEST_F(TestAsyncVectorChildCleanup, failed_zero_result_recovers_before_first_result_write)
{
  child_.task_status_ = share::ObDDLTaskStatus::FAIL;
  child_.ret_code_ = OB_SUCCESS;
  ObIndexBuildTask recovered;
  restore_child(recovered, false);
  ASSERT_TRUE(recovered.is_parent_managed_async_vector_child());
  ASSERT_SUCC(recovered.persist_terminal_result());
  ASSERT_EQ(OB_ERR_ADD_INDEX, recovered.ret_code_);
  ASSERT_EQ(OB_ERR_ADD_INDEX, proxy_.persisted_ret_code_);
  ASSERT_EQ(0, proxy_.reads_);
}

TEST_F(TestAsyncVectorChildCleanup, terminal_result_preserves_original_error)
{
  child_.task_status_ = share::ObDDLTaskStatus::FAIL;
  child_.ret_code_ = OB_ERR_UNEXPECTED;
  ASSERT_SUCC(child_.persist_terminal_result());
  ObIndexBuildTask recovered;
  restore_child(recovered, true);
  ASSERT_EQ(OB_ERR_UNEXPECTED, recovered.ret_code_);
  ASSERT_EQ(0, proxy_.reads_);
}

TEST_F(TestAsyncVectorChildCleanup, offline_restore_and_other_indexes_keep_existing_ownership)
{
  child_.parent_task_id_ = 0;
  ASSERT_FALSE(child_.is_parent_managed_async_vector_child());
  child_.parent_task_id_ = 7001;
  child_.create_index_arg_.is_offline_rebuild_ = true;
  ObIndexBuildTask offline;
  restore_child(offline, false);
  ASSERT_FALSE(offline.is_parent_managed_async_vector_child());
  child_.create_index_arg_.is_offline_rebuild_ = false;
  child_.create_index_arg_.is_table_restore_ = true;
  ObIndexBuildTask restored;
  restore_child(restored, false);
  ASSERT_FALSE(restored.is_parent_managed_async_vector_child());
  child_.create_index_arg_.is_table_restore_ = false;
  const share::schema::ObIndexType types[] = {
      share::schema::INDEX_TYPE_NORMAL_LOCAL,
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL,
      share::schema::INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL};
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    child_.create_index_arg_.index_type_ = types[i];
    ASSERT_FALSE(child_.is_parent_managed_async_vector_child());
  }
  ASSERT_EQ(0, proxy_.reads_);
  ASSERT_EQ(0, proxy_.writes_);
}

#ifdef ERRSIM
TEST_F(TestAsyncVectorChildCleanup, offline_snapshot_cleanup_retries_and_preserves_ownership_after_recovery)
{
  using namespace share::schema;
  const ObIndexType owned_types[] = {INDEX_TYPE_NORMAL_LOCAL, INDEX_TYPE_UNIQUE_LOCAL,
      INDEX_TYPE_NORMAL_GLOBAL, INDEX_TYPE_UNIQUE_GLOBAL, INDEX_TYPE_FTS_DOC_WORD_LOCAL};
  const ObIndexType borrowed_types[] = {INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL,
      INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL};
  auto &mgr = share::ObDDLSimPointMgr::get_instance();
  auto &report_repeats = mgr.all_points_[share::REPORT_DDL_RET_CODE_FAILED].action_->max_repeat_times_;
  const int64_t old_report_repeats = report_repeats;
  report_repeats = 2;
  DEFER(report_repeats = old_report_repeats);
  share::ObTenantDDLSimContext context;
  context.tenant_id_ = child_.tenant_id_;
  ASSERT_SUCC(mgr.tenant_map_.set_refactored(child_.tenant_id_, context));
  DEFER(mgr.tenant_map_.erase_refactored(child_.tenant_id_));
  const share::ObDDLSimPointMgr::TaskSimPoint release_key(
      child_.tenant_id_, child_.task_id_, share::DDL_TASK_RELEASE_SNAPSHOT_FAILED);
  const share::ObDDLSimPointMgr::TaskSimPoint report_key(
      child_.tenant_id_, child_.task_id_, share::REPORT_DDL_RET_CODE_FAILED);
  for (const auto status : {share::ObDDLTaskStatus::SUCCESS, share::ObDDLTaskStatus::FAIL}) {
    for (const bool owns_snapshot : {true, false}) {
      const ObIndexType *types = owns_snapshot ? owned_types : borrowed_types;
      const int64_t count = owns_snapshot ? ARRAYSIZEOF(owned_types) : ARRAYSIZEOF(borrowed_types);
      for (int64_t i = 0; i < count; ++i) {
        SCOPED_TRACE(static_cast<int>(status));
        SCOPED_TRACE(static_cast<int>(types[i]));
        child_.task_status_ = status;
        child_.ret_code_ = share::ObDDLTaskStatus::FAIL == status ? OB_CANCELED : OB_SUCCESS;
        child_.need_retry_ = true;
        child_.create_index_arg_.index_type_ = types[i];
        child_.create_index_arg_.is_offline_rebuild_ = true;
        ASSERT_SUCC(mgr.task_sim_map_.set_refactored(release_key, 0));
        DEFER(mgr.task_sim_map_.erase_refactored(release_key));
        // Stop reporting before any schema/SQL access when the child borrows its snapshot.
        ASSERT_SUCC(mgr.task_sim_map_.set_refactored(report_key, 0));
        DEFER(mgr.task_sim_map_.erase_refactored(report_key));
        const int ret = child_.cleanup();
        ASSERT_TRUE(OB_EAGAIN == ret || OB_TIMEOUT == ret);
        ASSERT_TRUE(child_.need_retry_);
        ASSERT_EQ(9001, child_.snapshot_version_);
        ObIndexBuildTask recovered;
        restore_child(recovered, false);
        ASSERT_TRUE(recovered.create_index_arg_.is_offline_rebuild_);
        ASSERT_EQ(types[i], recovered.create_index_arg_.index_type_);
        const int recovered_ret = recovered.cleanup();
        ASSERT_TRUE(OB_EAGAIN == recovered_ret || OB_TIMEOUT == recovered_ret);
        ASSERT_TRUE(recovered.need_retry_);
        ASSERT_EQ(9001, recovered.snapshot_version_);
        ASSERT_EQ(child_.ret_code_, recovered.ret_code_);
        int64_t release_count = 0;
        int64_t report_count = 0;
        ASSERT_SUCC(mgr.task_sim_map_.get_refactored(release_key, release_count));
        ASSERT_SUCC(mgr.task_sim_map_.get_refactored(report_key, report_count));
        ASSERT_EQ(owns_snapshot ? 2 : 0, release_count);
        ASSERT_EQ(owns_snapshot ? 0 : 2, report_count);
        ASSERT_EQ(0, proxy_.reads_);
        ASSERT_EQ(0, proxy_.writes_);
      }
    }
  }
}
#endif

TEST_F(TestAsyncVectorChildCleanup, nonterminal_or_uninitialized_task_cannot_report_result)
{
  child_.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  ASSERT_EQ(OB_STATE_NOT_MATCH, child_.persist_terminal_result());
  child_.is_inited_ = false;
  ASSERT_EQ(OB_NOT_INIT, child_.persist_terminal_result());
  ASSERT_EQ(0, proxy_.writes_);
}

TEST(test_ddl_task_struct, async_metadata_defense_preserves_mixed_ddl_boundary)
{
  ObTableSchema orig;
  ObTableSchema next;
  ObAlterTableArg arg;
  orig.set_has_async_index(true);
  next.set_has_async_index(true);
  ASSERT_SUCC(arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::TABLE_NAME));
  ASSERT_TRUE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
  ASSERT_FALSE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_2_0, orig, next, arg));
  arg.is_alter_columns_ = true;
  ASSERT_FALSE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
  arg.is_alter_columns_ = false;
  arg.is_alter_partitions_ = true;
  ASSERT_FALSE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
  arg.is_alter_partitions_ = false;
  next.set_has_async_index(false);
  ASSERT_FALSE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
}

TEST(test_ddl_task_struct, async_ctas_and_rename_defense_classification)
{
  ObTableSchema orig;
  ObTableSchema next;
  ObAlterTableArg arg;
  orig.set_has_async_index(true);
  next.set_has_async_index(true);
  orig.set_session_id(42);
  ASSERT_SUCC(arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::TABLE_NAME));
  ASSERT_FALSE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
  ASSERT_SUCC(arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::SESSION_ID));
  ASSERT_TRUE(ObDDLService::use_write_only_defense_for_alter_table_(DATA_VERSION_4_6_3_0, orig, next, arg));
  using DefenseType = ObDDLService::TableDefenseType;
  ASSERT_EQ(DefenseType::WRITE_ONLY, ObDDLService::get_rename_table_defense_type_(DATA_VERSION_4_6_3_0, orig));
  ASSERT_EQ(DefenseType::READ_WRITE, ObDDLService::get_rename_table_defense_type_(DATA_VERSION_4_6_2_0, orig));
  orig.set_has_async_index(false);
  ASSERT_EQ(DefenseType::READ_WRITE, ObDDLService::get_rename_table_defense_type_(DATA_VERSION_4_6_3_0, orig));
}

class TestAsyncVectorBuildTiming : public testing::Test
{
protected:
  void SetUp() override
  {
    data_schema_.set_tenant_id(1001);
    data_schema_.set_table_id(5001);
    data_schema_.set_schema_version(4001);
    index_schema_.set_tenant_id(1001);
    index_schema_.set_table_id(5002);
    index_schema_.set_schema_version(4002);
    index_schema_.set_index_type(share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    arg_.tenant_id_ = 1001;
    arg_.exec_tenant_id_ = 1001;
    arg_.database_name_ = "test";
    arg_.table_name_ = "timing_test";
    arg_.index_name_ = "idx_timing";
    arg_.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
    arg_.index_option_.block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
    obrpc::ObColumnSortItem column;
    column.column_name_ = "vec";
    ASSERT_SUCC(arg_.index_columns_.push_back(column));
    ASSERT_TRUE(arg_.is_valid());
  }

  int init_task(ObAsyncVectorIndexBuildTask &task)
  {
    return task.init(1001, 7001, &data_schema_, &index_schema_, 4001,
                    1, 0, 0, 0, arg_, 1, 100, true);
  }

  share::schema::ObTableSchema data_schema_;
  share::schema::ObTableSchema index_schema_;
  obrpc::ObCreateIndexArg arg_;
};

namespace baseline_memory_test
{
class TaskResult : public sqlclient::ObMySQLResultImpl
{
public:
  explicit TaskResult(sqlclient::ObMySQLStatement &stmt)
    : ObMySQLResultImpl(stmt), message_(16384, 'x') {}
  int next() override
  {
    return nullptr == next_calls_ ? OB_ITER_END : (0 == (*next_calls_)++ ? OB_SUCCESS : OB_ITER_END);
  }
  int get_int(const char *column, int64_t &value) const override
  {
    const struct { const char *name; int64_t value; } fields[] = {
        {"create_time", 1}, {"tenant_id", OB_INVALID_TENANT_ID},
        {"task_id", 7002}, {"parent_task_id", 7001},
        {"object_id", 5001}, {"target_object_id", 5002},
        {"schema_version", 4002}, {"ddl_type", share::DDL_CREATE_INDEX},
        {"status", share::ObDDLTaskStatus::REDEFINITION},
        {"task_version", 1}, {"ret_code", OB_SUCCESS},
        {"execution_id", 0}, {"consensus_schema_version", 4002}};
    for (const auto &field : fields) {
      if (0 == strcmp(column, field.name)) {
        value = field.value;
        return OB_SUCCESS;
      }
    }
    return OB_ERR_UNEXPECTED;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    if (0 != strcmp(column, "snapshot_version")) {
      return OB_ERR_UNEXPECTED;
    }
    value = 100;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    if (0 == strcmp(column, "trace_id")) {
      value = ObString::make_string("Y1-0000000000000001-0-0");
    } else if (0 == strcmp(column, "message_unhex")) {
      value.assign_ptr(message_.data(), message_.size());
    } else if (0 == strcmp(column, "ddl_stmt_str_unhex")) {
      value.reset();
    } else {
      return OB_ERR_UNEXPECTED;
    }
    return OB_SUCCESS;
  }
  int64_t *next_calls_ = nullptr;
  const std::string message_;
};

class ResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  ResultHandler() : statement_(), result_(statement_) {}
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
  sqlclient::ObMySQLStatement statement_;
  TaskResult result_;
};

class WaitProxy : public ObMySQLProxy
{
public:
  WaitProxy() { EXPECT_EQ(OB_SUCCESS, init(&pool_)); }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    EXPECT_EQ(987654, tenant_id);
    const bool task_record = nullptr != strstr(sql, "FROM __all_ddl_task_status");
    EXPECT_TRUE(task_record || nullptr != strstr(sql, "from __all_ddl_error_message"));
    ++reads_;
    ResultHandler *handler = nullptr;
    const int ret = res.create_handler(handler);
    if (OB_SUCCESS == ret && task_record) {
      handler->result_.next_calls_ = &record_next_calls_;
    }
    return ret;
  }
  int64_t reads_ = 0;
  int64_t record_next_calls_ = 0;
  sqlclient::ObMySQLConnectionPool pool_;
};

TEST(test_ddl_task_struct, async_vector_baseline_wait_without_business_tenant_allocator)
{
  auto &version = ObClusterVersion::get_instance();
  const uint64_t old_cluster_version = version.get_cluster_version();
  const uint64_t old_data_version = version.data_version_;
  DEFER(version.update_cluster_version(old_cluster_version));
  DEFER(version.update_data_version(old_data_version));
  version.update_cluster_version(DATA_VERSION_4_6_3_1);
  version.update_data_version(DATA_VERSION_4_6_3_1);
  const auto wait_for_child = []() {
    auto *allocator = ObMallocAllocator::get_instance();
    ASSERT_EQ(nullptr, allocator->get_tenant_ctx_allocator(987654, ObCtxIds::DEFAULT_CTX_ID));
    WaitProxy proxy;
    ObMySQLProxy *old_proxy = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy;
    DEFER(GCTX.sql_proxy_ = old_proxy);
    ObAsyncVectorIndexBuildTask task;
    task.tenant_id_ = 987654;
    task.task_id_ = 7001;
    task.task_status_ = share::ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE;
    task.set_baseline_task(7002);
    ASSERT_SUCC(task.wait_baseline_task_finish());
    ASSERT_EQ(2, proxy.reads_);
    // Reaching ITER_END proves that the nonempty child message was allocated and parsed.
    ASSERT_EQ(2, proxy.record_next_calls_);
    ASSERT_EQ(share::ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE, task.task_status_);
    ASSERT_EQ(nullptr, allocator->get_tenant_ctx_allocator(987654, ObCtxIds::DEFAULT_CTX_ID));
  };
  ASSERT_EQ(nullptr, MTL_CTX());
  ASSERT_NO_FATAL_FAILURE(wait_for_child());
  ObTenantBase coordinator(OB_SYS_TENANT_ID);
  ObTenantSwitchGuard tenant_guard(&coordinator);
  ASSERT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ASSERT_NO_FATAL_FAILURE(wait_for_child());
}
} // namespace baseline_memory_test

namespace parent_cleanup_test
{
class CleanupTaskResult : public sqlclient::ObMySQLResultImpl
{
public:
  explicit CleanupTaskResult(sqlclient::ObMySQLStatement &stmt)
    : ObMySQLResultImpl(stmt), status_(ObDDLTaskStatus::SUCCESS), next_ret_(OB_ITER_END), snapshot_(0) {}
  int next() override
  {
    int ret = next_ret_;
    if ((is_count_query_ || !build_task_message_.empty()) && OB_SUCCESS == ret) {
      ret = row_returned_ ? OB_ITER_END : OB_SUCCESS;
      row_returned_ = true;
    }
    return ret;
  }
  int get_int(const char *column, int64_t &value) const override
  {
    int ret = OB_SUCCESS;
    if (!build_task_message_.empty()) {
      const struct { const char *name; int64_t value; } fields[] = {
          {"create_time", 1}, {"tenant_id", OB_INVALID_TENANT_ID}, {"task_id", 7002},
          {"parent_task_id", 7001}, {"object_id", 500001}, {"target_object_id", 500002},
          {"schema_version", 4001}, {"ddl_type", share::DDL_CREATE_ASYNC_SEARCH_INDEX},
          {"status", ObDDLTaskStatus::FAIL}, {"task_version", 1},
          {"ret_code", OB_CANCELED}, {"execution_id", 0}, {"consensus_schema_version", OB_INVALID_VERSION}};
      ret = OB_ERR_UNEXPECTED;
      for (const auto &field : fields) {
        if (0 == strcmp(column, field.name)) {
          value = field.value;
          ret = OB_SUCCESS;
          break;
        }
      }
    } else if (0 == strcmp(column, "have")) {
      value = task_count_;
    } else if (0 == strcmp(column, "status")) {
      value = status_;
    } else if (0 == strcmp(column, "execution_id") || 0 == strcmp(column, "affected_rows")) {
      value = 0;
    } else if (0 == strcmp(column, "ddl_type")) {
      value = share::DDL_DROP_FTS_INDEX;
    } else if (0 == strcmp(column, "consensus_schema_version")) {
      value = OB_INVALID_VERSION;
    } else if (0 == strcmp(column, "ret_code")) {
      value = ObDDLTaskStatus::FAIL == status_ ? OB_CANCELED : OB_SUCCESS;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == strcmp(column, "snapshot_version")) {
      value = snapshot_;
    } else if (0 == strcmp(column, "have")) {
      value = task_count_;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == strcmp(column, "trace_id")) {
      value = "Y1-0000000000000001-0-0";
    } else if (0 == strcmp(column, "message_unhex")) {
      value = build_task_message_;
    } else if (0 == strcmp(column, "ddl_stmt_str_unhex")) {
      value.reset();
    } else if (0 == strcmp(column, "dba_message") || 0 == strcmp(column, "user_message")) {
      value = "Successful ddl";
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int64_t status_;
  int next_ret_;
  uint64_t snapshot_;
  ObString build_task_message_;
  bool is_count_query_ = false;
  int64_t task_count_ = 0;
  bool row_returned_ = false;
};

class CleanupTaskResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  CleanupTaskResultHandler() : statement_(), result_(statement_) {}
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
  void set_next_ret(const int ret) { result_.next_ret_ = ret; }
  void set_count(const int64_t count, const int next_ret)
  {
    result_.is_count_query_ = true;
    result_.task_count_ = count;
    result_.next_ret_ = next_ret;
  }
  void set_row(const int64_t status, const int next_ret, const uint64_t snapshot)
  {
    result_.status_ = status;
    result_.next_ret_ = next_ret;
    result_.snapshot_ = snapshot;
  }
  void set_build_task_record(const ObString &message)
  {
    result_.next_ret_ = OB_SUCCESS;
    result_.snapshot_ = 100;
    result_.build_task_message_ = message;
  }
private:
  sqlclient::ObMySQLStatement statement_;
  CleanupTaskResult result_;
};

class FailureDropSqlProxy : public ObMySQLProxy
{
public:
  FailureDropSqlProxy() { EXPECT_EQ(OB_SUCCESS, init(&pool_)); }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    EXPECT_EQ(1001, tenant_id);
    int ret = OB_SUCCESS;
    CleanupTaskResultHandler *handler = nullptr;
    if (OB_FAIL(res.create_handler(handler))) {
    } else if (nullptr != strstr(sql, "message_unhex")
               && nullptr != strstr(sql, "WHERE task_id=7002")) {
      ++build_record_reads_;
      if (OB_FAIL(build_record_read_ret_)) {
      } else {
        handler->set_build_task_record(build_task_message_);
      }
    } else if (nullptr != strstr(sql, "FROM __all_ddl_task_status WHERE task_id=7003")) {
      ++record_checks_;
      handler->set_count(drop_record_exists_ ? 1 : 0, OB_SUCCESS);
    } else if (nullptr != strstr(sql, "from __all_ddl_error_message")
               && nullptr != strstr(sql, "task_id = 7003")) {
      handler->set_row(ObDDLTaskStatus::SUCCESS, drop_result_available_ ? OB_SUCCESS : OB_ITER_END, 0);
    } else {
      ADD_FAILURE() << "unexpected SQL: " << sql;
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  bool drop_record_exists_ = true;
  bool drop_result_available_ = true;
  int64_t record_checks_ = 0;
  ObString build_task_message_;
  int build_record_read_ret_ = OB_SUCCESS;
  int64_t build_record_reads_ = 0;
private:
  sqlclient::ObMySQLConnectionPool pool_;
};

class TestOfflineRebuildDropWait : public testing::Test
{
protected:
  void SetUp() override
  {
    auto &version = ObClusterVersion::get_instance();
    old_cluster_version_ = version.get_cluster_version();
    old_data_version_ = version.data_version_;
    version.update_cluster_version(DATA_VERSION_4_6_3_1);
    version.update_data_version(DATA_VERSION_4_6_3_1);
    old_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy_;
    task_.is_inited_ = true;
    task_.tenant_id_ = 1001;
    task_.dst_tenant_id_ = 1001;
    task_.task_id_ = 7002;
    task_.parent_task_id_ = 7001;
    task_.task_status_ = ObDDLTaskStatus::FAIL;
    task_.drop_index_task_id_ = 7003;
    task_.create_index_arg_.is_offline_rebuild_ = true;
  }

  void TearDown() override
  {
    GCTX.sql_proxy_ = old_proxy_;
    auto &version = ObClusterVersion::get_instance();
    version.update_data_version(old_data_version_);
    version.update_cluster_version(old_cluster_version_);
  }

  uint64_t old_cluster_version_ = 0;
  uint64_t old_data_version_ = 0;
  ObMySQLProxy *old_proxy_ = nullptr;
  FailureDropSqlProxy proxy_;
  ObFtsIndexBuildTask task_;
};

TEST_F(TestOfflineRebuildDropWait, waits_until_successful_drop_removes_task_record)
{
  const share::ObDDLType task_types[] = {share::DDL_CREATE_FTS_INDEX, share::DDL_CREATE_SEARCH_INDEX,
      share::DDL_CREATE_MULTIVALUE_INDEX, share::DDL_CREATE_VEC_SPIV_INDEX,
      share::DDL_CREATE_ASYNC_FTS_INDEX, share::DDL_CREATE_ASYNC_SEARCH_INDEX};
  for (const auto task_type : task_types) {
    SCOPED_TRACE(task_type);
    task_.task_type_ = task_type;
    const int64_t checks_before = proxy_.record_checks_;
    proxy_.drop_record_exists_ = true;
    bool is_finish = true;
    ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
    ASSERT_FALSE(is_finish);
    ASSERT_EQ(checks_before + 1, proxy_.record_checks_);

    proxy_.drop_record_exists_ = false;
    ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
    ASSERT_TRUE(is_finish);
    ASSERT_EQ(checks_before + 2, proxy_.record_checks_);
  }
}

TEST_F(TestOfflineRebuildDropWait, skips_record_check_outside_offline_rebuild_drop)
{
  task_.task_type_ = share::DDL_CREATE_FTS_INDEX;
  bool is_finish = false;
  task_.create_index_arg_.is_offline_rebuild_ = false;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_TRUE(is_finish);

  task_.create_index_arg_.is_offline_rebuild_ = true;
  task_.parent_task_id_ = 0;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_TRUE(is_finish);
  ASSERT_SUCC(task_.handle_drop_fts_index_response(OB_SUCCESS, -1));
  ASSERT_EQ(-1, task_.drop_index_task_id_);

  task_.parent_task_id_ = 7001;
  task_.create_index_arg_.is_offline_rebuild_ = false;
  ASSERT_SUCC(task_.handle_drop_fts_index_response(OB_SUCCESS, -1));
  ASSERT_EQ(-1, task_.drop_index_task_id_);
  ASSERT_EQ(0, proxy_.build_record_reads_);

  task_.create_index_arg_.is_offline_rebuild_ = true;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_TRUE(is_finish);
  ASSERT_EQ(0, proxy_.record_checks_);
}

class TestOfflineRebuildDropRecovery : public TestOfflineRebuildDropWait
{
protected:
  void SetUp() override
  {
    TestOfflineRebuildDropWait::SetUp();
    old_root_service_ = GCTX.root_service_;
    old_ddl_started_ = ObDDLServiceLauncher::is_ddl_service_started();
    GCTX.root_service_ = &root_service_;
    ObDDLServiceLauncher::is_ddl_service_started_ = true;

    task_.task_type_ = share::DDL_CREATE_ASYNC_SEARCH_INDEX;
    task_.object_id_ = 500001;
    task_.index_table_id_ = 500002;
    task_.drop_index_task_id_ = -1;
    ObFtsIndexBuildTask persisted_task;
    persisted_task.create_index_arg_.tenant_id_ = 1001;
    persisted_task.create_index_arg_.exec_tenant_id_ = 1001;
    persisted_task.create_index_arg_.index_type_ = share::schema::INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
    persisted_task.create_index_arg_.is_offline_rebuild_ = true;
    persisted_task.set_drop_index_task_id(7003);
    persisted_task.set_drop_index_task_submitted(true);
    message_.resize(persisted_task.get_serialize_param_size());
    int64_t pos = 0;
    ASSERT_SUCC(persisted_task.serialize_params_to_message(message_.data(), message_.size(), pos));
    proxy_.build_task_message_.assign_ptr(message_.data(), pos);
  }

  void inject_lost_drop_rpc_response()
  {
    // The DROP record committed, but the running build task only sees a timeout.
    ASSERT_EQ(OB_TIMEOUT, task_.handle_drop_fts_index_response(OB_TIMEOUT, 0));
    ASSERT_FALSE(task_.drop_index_task_submitted_);
    ASSERT_EQ(-1, task_.drop_index_task_id_);
  }

  void TearDown() override
  {
    GCTX.root_service_ = old_root_service_;
    ObDDLServiceLauncher::is_ddl_service_started_ = old_ddl_started_;
    TestOfflineRebuildDropWait::TearDown();
  }

  ObRootService root_service_;
  ObRootService *old_root_service_ = nullptr;
  bool old_ddl_started_ = false;
  std::string message_;
};

TEST_F(TestOfflineRebuildDropRecovery, recovers_committed_drop_after_lost_rpc_response)
{
  ASSERT_NO_FATAL_FAILURE(inject_lost_drop_rpc_response());
  // After the DROP removes the schemas, the repeated RPC cannot find an index.
  proxy_.build_record_read_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, task_.handle_drop_fts_index_response(OB_SUCCESS, -1));
  ASSERT_FALSE(task_.drop_index_task_submitted_);
  ASSERT_EQ(-1, task_.drop_index_task_id_);
  proxy_.build_record_read_ret_ = OB_SUCCESS;
  ASSERT_SUCC(task_.handle_drop_fts_index_response(OB_SUCCESS, -1));
  ASSERT_EQ(2, proxy_.build_record_reads_);
  ASSERT_TRUE(task_.drop_index_task_submitted_);
  ASSERT_EQ(7003, task_.drop_index_task_id_);

  bool is_finish = true;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_FALSE(is_finish);
  proxy_.drop_record_exists_ = false;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_TRUE(is_finish);
}

TEST_F(TestOfflineRebuildDropRecovery, reuses_committed_drop_before_resending_with_index_schema_present)
{
  using namespace share::schema;
  MockSchemaService schema_service;
  ASSERT_SUCC(schema_service.init());
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(task_.tenant_id_);
  tenant_schema.set_schema_version(4001);
  ASSERT_SUCC(tenant_schema.set_tenant_name("rebuild_drop_tenant"));
  ASSERT_SUCC(schema_service.add_tenant_schema(tenant_schema, 4001));
  ObSysVariableSchema sys_vars;
  sys_vars.set_tenant_id(task_.tenant_id_);
  sys_vars.set_schema_version(4001);
  sys_vars.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  ASSERT_SUCC(schema_service.add_sys_variable_schema(sys_vars, 4001));

  ObTableSchema index_schema;
  index_schema.set_tenant_id(task_.tenant_id_);
  index_schema.set_database_id(500000);
  index_schema.set_table_id(task_.index_table_id_);
  index_schema.set_schema_version(4001);
  index_schema.set_table_type(USER_INDEX);
  index_schema.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
  index_schema.set_data_table_id(task_.object_id_);
  index_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  ASSERT_SUCC(index_schema.set_table_name("__idx_500001_search_idx"));
  ASSERT_SUCC(schema_service.add_table_schema(index_schema, 4001));
  ObSchemaGetterGuard schema_guard;
  ASSERT_SUCC(schema_service.get_schema_guard(schema_guard));
  bool index_exists = false;
  ASSERT_SUCC(schema_guard.check_table_exist(task_.tenant_id_, task_.index_table_id_, index_exists));
  ASSERT_TRUE(index_exists);

  auto *old_schema_service = GCTX.schema_service_;
  auto *old_rs_rpc_proxy = GCTX.rs_rpc_proxy_;
  GCTX.schema_service_ = &schema_service;
  GCTX.rs_rpc_proxy_ = nullptr;
  DEFER(GCTX.schema_service_ = old_schema_service);
  DEFER(GCTX.rs_rpc_proxy_ = old_rs_rpc_proxy);

  ASSERT_NO_FATAL_FAILURE(inject_lost_drop_rpc_response());
  proxy_.build_record_read_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, task_.submit_drop_fts_index_task());
  ASSERT_FALSE(task_.drop_index_task_submitted_);
  proxy_.build_record_read_ret_ = OB_SUCCESS;
  ASSERT_SUCC(task_.submit_drop_fts_index_task());
  ASSERT_EQ(2, proxy_.build_record_reads_);
  ASSERT_TRUE(task_.drop_index_task_submitted_);
  ASSERT_EQ(7003, task_.drop_index_task_id_);

  proxy_.drop_result_available_ = false;
  bool is_finish = true;
  ASSERT_SUCC(task_.wait_drop_index_finish(is_finish));
  ASSERT_FALSE(is_finish);
  ASSERT_EQ(0, proxy_.record_checks_);
}

class DeletedTaskSqlProxy : public ObMySQLProxy
{
public:
  DeletedTaskSqlProxy()
    : delete_committed_(false), read_ret_(OB_SUCCESS), next_ret_(OB_SUCCESS), reads_(0), writes_(0)
  {
    EXPECT_EQ(OB_SUCCESS, init(&pool_));
  }
  int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override
  {
    EXPECT_EQ(1001, tenant_id);
    EXPECT_NE(nullptr, strstr(sql, "DELETE FROM __all_ddl_task_status WHERE task_id=7001"));
    ++writes_;
    delete_committed_ = true;
    affected_rows = 1;
    return OB_TIMEOUT; // The DELETE committed, but its response was lost.
  }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    EXPECT_EQ(1001, tenant_id);
    EXPECT_NE(nullptr, strstr(sql, "WHERE task_id=7001"));
    ++reads_;
    int ret = read_ret_;
    if (OB_SUCC(ret)) {
      CleanupTaskResultHandler *handler = nullptr;
      ret = res.create_handler(handler);
      if (OB_SUCC(ret)) {
        if (nullptr != strstr(sql, "SELECT count(*) as have")) {
          handler->set_count(delete_committed_ ? 0 : 1, next_ret_);
        } else {
          handler->set_next_ret(OB_ITER_END);
        }
      }
    }
    return ret;
  }
  bool delete_committed_;
  int read_ret_;
  int next_ret_;
  int64_t reads_;
  int64_t writes_;
  sqlclient::ObMySQLConnectionPool pool_;
};

class SnapshotTaskTransaction : public ObMySQLTransaction
{
public:
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    EXPECT_EQ(1001, tenant_id);
    EXPECT_NE(nullptr, strstr(sql, "WHERE task_id = 7001 FOR UPDATE"));
    ++reads_;
    int ret = read_ret_;
    if (OB_SUCC(ret)) {
      CleanupTaskResultHandler *handler = nullptr;
      if (OB_SUCC(ret = res.create_handler(handler))) {
        handler->set_row(status_, next_ret_, snapshot_);
      }
    }
    return ret;
  }
  int64_t status_ = ObDDLTaskStatus::SUCCESS;
  int read_ret_ = OB_SUCCESS;
  int next_ret_ = OB_SUCCESS;
  int64_t reads_ = 0;
  uint64_t snapshot_ = 0;
};

class TestAsyncVectorParentCleanup : public TestAsyncVectorBuildTiming
{
protected:
  void SetUp() override
  {
    TestAsyncVectorBuildTiming::SetUp();
    old_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy_;
    ASSERT_SUCC(init_task(task_));
    task_.data_format_version_ = DATA_VERSION_4_6_3_1;
    task_.task_status_ = share::ObDDLTaskStatus::FAIL;
    task_.ret_code_ = OB_CANCELED;
    task_.snapshot_version_ = 0; // Cleanup released S before it deleted the task.
    task_.need_retry_ = true;
    ASSERT_TRUE(task_.uses_parent_publish_protocol());
    ASSERT_EQ(OB_TIMEOUT, ObDDLTaskRecordOperator::delete_record(proxy_, 1001, 7001));
  }
  void TearDown() override { GCTX.sql_proxy_ = old_proxy_; }
  ObMySQLProxy *old_proxy_ = nullptr;
  DeletedTaskSqlProxy proxy_;
  ObAsyncVectorIndexBuildTask task_;
};

TEST_F(TestAsyncVectorParentCleanup, missing_record_keeps_strict_lookup_contract)
{
  ObArenaAllocator allocator("ParentCleanup");
  ObDDLTaskRecord record;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObDDLTaskRecordOperator::get_ddl_task_record(1001, 7001, proxy_, allocator, record));
  ASSERT_EQ(1, proxy_.reads_);
  ASSERT_EQ(1, proxy_.writes_);
}

TEST_F(TestAsyncVectorParentCleanup, failed_parent_stops_after_committed_delete_response_lost)
{
  // No scanner is running: cleanup reentry itself must stop this terminal task.
  ASSERT_SUCC(task_.clean_on_failed());
  ASSERT_FALSE(task_.need_retry());
  ASSERT_EQ(OB_CANCELED, task_.ret_code_);
  ASSERT_EQ(0, task_.snapshot_version_);
  ASSERT_EQ(1, proxy_.reads_);
  ASSERT_EQ(1, proxy_.writes_);
}

TEST_F(TestAsyncVectorParentCleanup, record_disappearing_after_existence_check_keeps_retry)
{
  proxy_.delete_committed_ = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task_.clean_on_failed());
  ASSERT_TRUE(task_.need_retry());
  ASSERT_EQ(OB_CANCELED, task_.ret_code_);
  ASSERT_EQ(2, proxy_.reads_);
  proxy_.delete_committed_ = true;
  ASSERT_SUCC(task_.clean_on_failed());
  ASSERT_FALSE(task_.need_retry());
  ASSERT_EQ(OB_CANCELED, task_.ret_code_);
  ASSERT_EQ(3, proxy_.reads_);
  ASSERT_EQ(1, proxy_.writes_);
}

TEST_F(TestAsyncVectorParentCleanup, query_errors_do_not_mean_cleanup_finished)
{
  const int read_errors[] = {OB_TIMEOUT, OB_ERR_UNEXPECTED};
  const bool fail_in_next[] = {false, true};
  for (const bool iteration_error : fail_in_next) {
    for (const int error : read_errors) {
      proxy_.read_ret_ = iteration_error ? OB_SUCCESS : error;
      proxy_.next_ret_ = iteration_error ? error : OB_SUCCESS;
      ASSERT_EQ(error, task_.clean_on_failed());
      ASSERT_TRUE(task_.need_retry());
      ASSERT_EQ(OB_CANCELED, task_.ret_code_);
      ASSERT_EQ(0, task_.snapshot_version_);
    }
  }
  ASSERT_EQ(4, proxy_.reads_);
  ASSERT_EQ(1, proxy_.writes_);
}

TEST_F(TestAsyncVectorParentCleanup, deferred_snapshot_cleanup_recovers_released_and_deleted_records)
{
  for (const auto status : {ObDDLTaskStatus::SUCCESS, ObDDLTaskStatus::FAIL}) {
    SCOPED_TRACE(status);
    SnapshotTaskTransaction trans;
    trans.status_ = status;
    task_.task_status_ = status;
    task_.ret_code_ = ObDDLTaskStatus::FAIL == status ? OB_CANCELED : OB_SUCCESS;
    const int original_ret = task_.ret_code_;
    task_.snapshot_version_ = 100;
    bool released = false;
    // Release COMMIT succeeded despite its lost response: storage already has S=0.
    ASSERT_SUCC(task_.try_release_snapshot(trans, released));
    ASSERT_TRUE(released);
    ASSERT_EQ(0, task_.snapshot_version_);
    // DELETE also committed despite its lost response; even a stale cache must be safe.
    trans.next_ret_ = OB_ITER_END;
    task_.snapshot_version_ = 100;
    released = false;
    ASSERT_SUCC(task_.try_release_snapshot(trans, released));
    ASSERT_TRUE(released);
    task_.finish_snapshot_release(released, false);
    ASSERT_EQ(100, task_.snapshot_version_);
    task_.finish_snapshot_release(released, true);
    ASSERT_EQ(0, task_.snapshot_version_);
    released = false;
    ASSERT_SUCC(task_.try_release_snapshot(trans, released));
    ASSERT_TRUE(released);
    ASSERT_EQ(0, task_.snapshot_version_);
    ASSERT_EQ(original_ret, task_.ret_code_);
    ASSERT_EQ(3, trans.reads_);
    // A zero cache must reload a retained snapshot; stop before external release.
    trans.next_ret_ = OB_SUCCESS;
    trans.snapshot_ = 100;
    task_.snapshot_holder_schema_version_ = 0;
    released = false;
    ASSERT_EQ(OB_ERR_UNEXPECTED, task_.try_release_snapshot(trans, released));
    ASSERT_FALSE(released);
    ASSERT_EQ(100, task_.snapshot_version_);
    ASSERT_EQ(4, trans.reads_);
    ASSERT_EQ(original_ret, task_.ret_code_);
  }
  ASSERT_EQ(0, proxy_.reads_);
}

TEST_F(TestAsyncVectorParentCleanup, deferred_snapshot_cleanup_rejects_query_errors_and_nonterminal_states)
{
  SnapshotTaskTransaction trans;
  task_.snapshot_version_ = 100;
  for (const bool fail_in_next : {false, true}) {
    for (const int error : {OB_TIMEOUT, OB_ERR_UNEXPECTED}) {
      trans.read_ret_ = fail_in_next ? OB_SUCCESS : error;
      trans.next_ret_ = fail_in_next ? error : OB_SUCCESS;
      bool released = false;
      ASSERT_EQ(error, task_.try_release_snapshot(trans, released));
      ASSERT_FALSE(released);
      ASSERT_EQ(100, task_.snapshot_version_);
    }
  }
  ASSERT_EQ(4, trans.reads_);
  trans.read_ret_ = OB_SUCCESS;
  trans.next_ret_ = OB_SUCCESS;
  trans.status_ = ObDDLTaskStatus::PREPARE;
  bool released = false;
  ASSERT_EQ(OB_STATE_NOT_MATCH, task_.try_release_snapshot(trans, released));
  ASSERT_FALSE(released);
  ASSERT_EQ(100, task_.snapshot_version_);
  ASSERT_EQ(5, trans.reads_);
  task_.task_status_ = ObDDLTaskStatus::PREPARE;
  trans.next_ret_ = OB_ITER_END;
  ASSERT_EQ(OB_STATE_NOT_MATCH, task_.try_release_snapshot(trans, released));
  ASSERT_FALSE(released);
  ASSERT_EQ(100, task_.snapshot_version_);
  ASSERT_EQ(5, trans.reads_);
}
} // namespace parent_cleanup_test

TEST_F(TestAsyncVectorBuildTiming, new_tasks_inherit_scheduling_parameters)
{
  for (const int64_t parallelism : {1L, 4L, 8L}) {
    for (const int64_t arg_parallelism : {0L, parallelism}) {
      SCOPED_TRACE(parallelism);
      SCOPED_TRACE(arg_parallelism);
      arg_.parallelism_ = arg_parallelism;
      const int64_t arg_consumer_group = 0 == arg_parallelism ? 0 : 7;
      arg_.consumer_group_id_ = arg_consumer_group;
      ObAsyncVectorIndexBuildTask task;
      ASSERT_SUCC(task.init(1001, 7001, &data_schema_, &index_schema_, 4001,
          parallelism, 0, 7, 0, arg_, DATA_VERSION_4_6_3_1, 100, true));
      ASSERT_EQ(parallelism, task.parallelism_);
      ASSERT_EQ(parallelism, task.create_index_arg_.parallelism_);
      ASSERT_EQ(7, task.consumer_group_id_);
      ASSERT_EQ(7, task.create_index_arg_.consumer_group_id_);
      ASSERT_EQ(arg_consumer_group, arg_.consumer_group_id_);
      ASSERT_EQ(arg_parallelism, arg_.parallelism_);
      ObArenaAllocator allocator("AsyncVecPar");
      obrpc::ObCreateIndexArg delete_arg;
      ASSERT_SUCC(task.construct_async_delete_arg(allocator, delete_arg));
      ASSERT_EQ(parallelism, delete_arg.parallelism_);
      ASSERT_EQ(7, delete_arg.consumer_group_id_);
    }
  }
}

TEST_F(TestAsyncVectorBuildTiming, recovered_tasks_inherit_scheduling_parameters)
{
  for (const int64_t parallelism : {1L, 4L, 8L}) {
    for (const int64_t arg_parallelism : {0L, 2L, parallelism}) {
      SCOPED_TRACE(parallelism);
      SCOPED_TRACE(arg_parallelism);
      ObAsyncVectorIndexBuildTask original;
      ASSERT_SUCC(original.init(1001, 7001, &data_schema_, &index_schema_, 4001,
          parallelism, 0, 7, 0, arg_, DATA_VERSION_4_6_3_1, 100, true));
      // Encode the inconsistent arguments that an old ALTER task could persist.
      original.create_index_arg_.parallelism_ = arg_parallelism;
      original.create_index_arg_.consumer_group_id_ = 0;
      ObAddr addr;
      ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
      ObCurTraceId::TraceId trace_id;
      trace_id.init(addr);
      ASSERT_SUCC(original.set_trace_id(trace_id));
      ObArenaAllocator allocator("AsyncVecPar");
      ObDDLTaskRecord record;
      ASSERT_SUCC(original.convert_to_record(record, allocator));
      ASSERT_TRUE(record.is_valid());
      ObAsyncVectorIndexBuildTask recovered;
      ASSERT_SUCC(recovered.init(record));
      ASSERT_EQ(parallelism, recovered.parallelism_);
      ASSERT_EQ(parallelism, recovered.create_index_arg_.parallelism_);
      ASSERT_EQ(7, recovered.consumer_group_id_);
      ASSERT_EQ(7, recovered.create_index_arg_.consumer_group_id_);
    }
  }
}

TEST_F(TestAsyncVectorBuildTiming, new_task_starts_execution_clock)
{
  ObAsyncVectorIndexBuildTask task;
  const int64_t before = ObTimeUtility::current_time();
  ASSERT_SUCC(init_task(task));
  const int64_t after = ObTimeUtility::current_time();
  ASSERT_GE(task.start_time_, before);
  ASSERT_LE(task.start_time_, after);
  ASSERT_GE(task.get_gmt_create(), before);
  ASSERT_LE(task.get_gmt_create(), after);
  const int64_t start_time = task.start_time_;
  ASSERT_EQ(OB_INIT_TWICE, init_task(task));
  ASSERT_EQ(start_time, task.start_time_);
}

TEST_F(TestAsyncVectorBuildTiming, offline_rebuild_requires_parent_task)
{
  arg_.is_offline_rebuild_ = true;
  ObAsyncVectorIndexBuildTask task;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.init(1001, 7001, &data_schema_, &index_schema_, 4001,
                1, 0, 0, 0, arg_, 1, 100, true));
  ASSERT_SUCC(task.init(1001, 7001, &data_schema_, &index_schema_, 4001,
                        1, 6001, 0, 0, arg_, 1, 100, true));
  ASSERT_TRUE(task.is_offline_redefinition_child());
}

TEST_F(TestAsyncVectorBuildTiming, async_vector_longops_reports_parent_stages)
{
  ObAsyncVectorIndexBuildTask task;
  ASSERT_SUCC(init_task(task));
  const ObDDLTask *ddl_task = &task;
  ASSERT_TRUE(ddl_task->support_longops_monitoring());
  share::ObDDLLongopsStat stat;
  ASSERT_SUCC(stat.init(&task));
  STRCPY(task.stat_info_.message_, "unchanged");
  struct Stage {
    share::ObDDLTaskStatus status;
    const char *name;
    int64_t baseline_task_id;
  };
  const Stage stages[] = {
      {share::ObDDLTaskStatus::PREPARE, "PREPARE", 0},
      {share::ObDDLTaskStatus::GENERATE_ASYNC_VECTOR_INDEX_BASELINE,
       "GENERATE_ASYNC_VECTOR_INDEX_BASELINE", 0},
      {share::ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE,
       "WAIT_ASYNC_VECTOR_INDEX_BASELINE", 7002},
      {share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, "TRIGGER_ASYNC_INDEX_REFRESH", 7002},
      {share::ObDDLTaskStatus::TAKE_EFFECT, "TAKE_EFFECT", 7002},
      {share::ObDDLTaskStatus::SUCCESS, "SUCCESS", 7002},
      {share::ObDDLTaskStatus::FAIL, "FAIL", 7002}};
  for (const auto &stage : stages) {
    SCOPED_TRACE(stage.name);
    task.task_status_ = stage.status;
    task.baseline_task_id_ = stage.baseline_task_id;
    share::ObLongopsValue value;
    ASSERT_SUCC(stat.get_longops_value(value));
    ASSERT_EQ(1001, value.tenant_id_);
    ASSERT_STREQ("create async DISK_BBQ index", value.op_name_);
    ASSERT_STREQ("5002", value.target_);
    const std::string message = std::string("TENANT_ID: 1001, TASK_ID: 7001, STATUS: ")
        + stage.name + ", BASELINE_TASK_ID: " + std::to_string(stage.baseline_task_id);
    ASSERT_STREQ(message.c_str(), value.message_);
    ASSERT_STREQ("unchanged", task.stat_info_.message_);
    ASSERT_EQ(task.get_gmt_create(), value.start_time_);
    ASSERT_GE(value.elapsed_seconds_, 0);
    ASSERT_GE(value.last_update_time_, value.start_time_);
  }
  const share::ObDDLTaskStatus unsupported_states[] = {
      static_cast<share::ObDDLTaskStatus>(39),
      share::ObDDLTaskStatus::REDEFINITION,
      share::ObDDLTaskStatus::WAIT_TRANS_END};
  for (const auto status : unsupported_states) {
    SCOPED_TRACE(static_cast<int>(status));
    task.task_status_ = status;
    share::ObLongopsValue value;
    ASSERT_EQ(OB_ERR_UNEXPECTED, stat.get_longops_value(value));
  }
}

#ifdef ERRSIM
TEST_F(TestAsyncVectorBuildTiming, async_vector_longops_recovers_after_injected_failure)
{
  ObAsyncVectorIndexBuildTask task;
  ASSERT_SUCC(init_task(task));
  share::ObDDLLongopsStat stat;
  ASSERT_SUCC(stat.init(&task));
  auto &mgr = share::ObDDLSimPointMgr::get_instance();
  share::ObTenantDDLSimContext context;
  context.tenant_id_ = task.tenant_id_;
  ASSERT_SUCC(mgr.tenant_map_.set_refactored(task.tenant_id_, context));
  DEFER(mgr.tenant_map_.erase_refactored(task.tenant_id_));
  const share::ObDDLSimPointMgr::TaskSimPoint key(
      task.tenant_id_, task.task_id_, share::DDL_TASK_COLLECT_LONGOPS_STAT_FAILED);
  ASSERT_SUCC(mgr.task_sim_map_.set_refactored(key, 0));
  DEFER(mgr.task_sim_map_.erase_refactored(key));
  share::ObLongopsValue value;
  task.task_status_ = share::ObDDLTaskStatus::REDEFINITION;
  ASSERT_EQ(OB_ERR_UNEXPECTED, stat.get_longops_value(value));
  task.task_status_ = share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH;
  task.baseline_task_id_ = 7002;
  STRCPY(task.stat_info_.message_, "unchanged");
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_EAGAIN, stat.get_longops_value(value));
    ASSERT_EQ(share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, task.task_status_);
    ASSERT_EQ(7002, task.baseline_task_id_);
    ASSERT_STREQ("unchanged", task.stat_info_.message_);
  }
  ASSERT_SUCC(stat.get_longops_value(value));
  ASSERT_STREQ("TENANT_ID: 1001, TASK_ID: 7001, STATUS: TRIGGER_ASYNC_INDEX_REFRESH, BASELINE_TASK_ID: 7002",
      value.message_);
  ASSERT_STREQ("unchanged", task.stat_info_.message_);
}
#endif

TEST_F(TestAsyncVectorBuildTiming, recovered_task_restarts_execution_clock)
{
  arg_.is_offline_rebuild_ = true;
  ObAsyncVectorIndexBuildTask original;
  ASSERT_SUCC(original.init(1001, 7001, &data_schema_, &index_schema_, 4001,
                            1, 6001, 0, 0, arg_, 1, 100, true));
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  ObCurTraceId::TraceId trace_id;
  trace_id.init(addr);
  ASSERT_SUCC(original.set_trace_id(trace_id));
  // Old creation time remains available to monitoring, but must not become
  // the execution clock when the scheduler reconstructs a task.
  const int64_t old_create_time = ObTimeUtility::current_time()
      - 2 * ObDDLTask::TASK_EXECUTE_TIME_THRESHOLD;
  original.set_gmt_create(old_create_time);
  original.task_status_ = share::ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE;
  original.baseline_task_submitted_ = true;
  original.baseline_task_id_ = 7002;
  ObArenaAllocator allocator("AsyncVecTiming");
  ObDDLTaskRecord record;
  ASSERT_SUCC(original.convert_to_record(record, allocator));
  ASSERT_TRUE(record.is_valid());
  ASSERT_EQ(6001, record.parent_task_id_);

  ObAsyncVectorIndexBuildTask recovered;
  const int64_t before = ObTimeUtility::current_time();
  ASSERT_SUCC(recovered.init(record));
  const int64_t after = ObTimeUtility::current_time();
  ASSERT_GE(recovered.start_time_, before);
  ASSERT_LE(recovered.start_time_, after);
  ASSERT_NE(old_create_time, recovered.start_time_);
  // This is the same separate monitoring update performed by the scheduler.
  recovered.set_gmt_create(record.gmt_create_);
  ASSERT_SUCC(recovered.set_trace_id(record.trace_id_));
  ASSERT_TRUE(recovered.support_longops_monitoring());
  share::ObDDLLongopsStat stat;
  ASSERT_SUCC(stat.init(&recovered));
  share::ObLongopsValue value;
  ASSERT_SUCC(stat.get_longops_value(value));
  ASSERT_EQ(old_create_time, value.start_time_);
  ASSERT_TRUE(value.trace_id_ == trace_id);
  ASSERT_STREQ("create async DISK_BBQ index", value.op_name_);
  ASSERT_STREQ("5002", value.target_);
  ASSERT_STREQ("TENANT_ID: 1001, TASK_ID: 7001, STATUS: WAIT_ASYNC_VECTOR_INDEX_BASELINE, "
               "BASELINE_TASK_ID: 7002", value.message_);
  ASSERT_EQ(old_create_time, recovered.get_gmt_create());
  ASSERT_GE(recovered.start_time_, before);
  ASSERT_EQ(original.task_status_, recovered.task_status_);
  ASSERT_EQ(original.ret_code_, recovered.ret_code_);
  ASSERT_TRUE(recovered.baseline_task_submitted_);
  ASSERT_EQ(7002, recovered.baseline_task_id_);
  ASSERT_EQ(6001, recovered.parent_task_id_);
  ASSERT_TRUE(recovered.create_index_arg_.is_offline_rebuild_);
  ASSERT_TRUE(recovered.is_offline_redefinition_child());
  const int64_t start_time = recovered.start_time_;
  ASSERT_EQ(OB_INIT_TWICE, recovered.init(record));
  ASSERT_EQ(start_time, recovered.start_time_);
}

TEST(test_ddl_task_struct, ddl_execution_warning_preserves_task_state)
{
  ObAsyncVectorIndexBuildTask task;
  task.tenant_id_ = 1001;
  task.task_id_ = 7001;
  task.start_time_ = ObTimeUtility::current_time()
      - 2 * ObDDLTask::TASK_EXECUTE_TIME_THRESHOLD;
  const auto status = task.task_status_;
  const auto ret_code = task.ret_code_;
  const bool need_retry = task.need_retry_;
  task.check_ddl_task_execute_too_long();
  ASSERT_EQ(status, task.task_status_);
  ASSERT_EQ(ret_code, task.ret_code_);
  ASSERT_EQ(need_retry, task.need_retry_);
}

TEST(test_ddl_task_struct, async_vector_build_message_round_trip)
{
  const uint64_t tenant_id = 1001;
  ObArenaAllocator allocator("AsyncVecMsg");
  ObAsyncVectorIndexBuildTask task;
  ASSERT_TRUE(ObAsyncVectorIndexBuildTask::is_supported_task_version(1));
  ASSERT_FALSE(ObAsyncVectorIndexBuildTask::is_supported_task_version(0));
  ASSERT_TRUE(ObAsyncVectorIndexBuildTask::is_supported_task_version(
      ObAsyncVectorIndexBuildTask::OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION));
  ASSERT_FALSE(ObAsyncVectorIndexBuildTask::is_supported_task_version(
      ObAsyncVectorIndexBuildTask::OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION + 1));
  task.task_version_ = 1;
  task.schema_version_ = 4001;
  task.parent_task_id_ = 6001;
  task.snapshot_version_ = 100;
  task.create_index_arg_.is_offline_rebuild_ = true;
  task.create_index_arg_.tenant_id_ = tenant_id;
  task.create_index_arg_.exec_tenant_id_ = tenant_id;
  task.create_index_arg_.index_type_ =
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  task.shared_async_delete_table_id_ = 5002;
  task.baseline_task_submitted_ = true;
  task.baseline_task_id_ = 7001;
  task.drop_task_submitted_ = true;
  task.drop_task_id_ = 7002;
  task.is_retryable_ddl_ = false;
  task.snapshot_holder_schema_version_ = 4002;
  ASSERT_SUCC(task.existing_async_index_table_ids_.push_back(5003));
  ASSERT_SUCC(task.existing_async_index_table_ids_.push_back(5004));
  ASSERT_FALSE(task.is_ddl_retryable());

  const int64_t size = task.get_serialize_param_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObAsyncVectorIndexBuildTask recovered;
  recovered.schema_version_ = 4001;
  pos = 0;
  ASSERT_SUCC(recovered.deserialize_params_from_message(
      tenant_id, buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_EQ(1, recovered.task_version_);
  // parent_task_id is a first-class DDL task-record field rather than part of
  // the task message. init(record) restores it before deserializing the arg.
  ASSERT_EQ(0, recovered.parent_task_id_);
  ASSERT_TRUE(recovered.create_index_arg_.is_offline_rebuild_);
  ASSERT_FALSE(recovered.is_offline_redefinition_child());
  ASSERT_EQ(5002, recovered.shared_async_delete_table_id_);
  ASSERT_TRUE(recovered.baseline_task_submitted_);
  ASSERT_EQ(7001, recovered.baseline_task_id_);
  ASSERT_TRUE(recovered.drop_task_submitted_);
  ASSERT_EQ(7002, recovered.drop_task_id_);
  ASSERT_FALSE(recovered.is_retryable_ddl_);
  ASSERT_EQ(4002, recovered.snapshot_holder_schema_version_);
  ASSERT_EQ(2, recovered.existing_async_index_table_ids_.count());
  ASSERT_EQ(5003, recovered.existing_async_index_table_ids_.at(0));
  ASSERT_EQ(5004, recovered.existing_async_index_table_ids_.at(1));
}

TEST_F(TestAsyncVectorBuildTiming, async_vector_record_round_trip)
{
  struct Scenario {
    uint64_t data_version;
    int64_t snapshot;
    bool parent_publish;
  };
  const Scenario scenarios[] = {
      {DATA_VERSION_4_6_2_2, 100, false},
      {DATA_VERSION_4_6_3_0, 0, true},
      {DATA_VERSION_4_6_3_0, 100, true},
      {DATA_VERSION_4_6_3_1, 0, true},
      {DATA_VERSION_4_6_3_1, 100, true}};
  for (const auto &scenario : scenarios) {
    const int64_t snapshot = scenario.snapshot;
    SCOPED_TRACE(scenario.data_version);
    SCOPED_TRACE(snapshot);
    ObAsyncVectorIndexBuildTask original;
    ASSERT_SUCC(original.init(1001, 7001, &data_schema_, &index_schema_, 4001,
        1, 0, 0, 0, arg_, scenario.data_version, snapshot, true));
    ASSERT_EQ(1, original.task_version_);
    ASSERT_EQ(scenario.parent_publish, original.uses_parent_publish_protocol());
    // Metadata DDL may advance the live schema before snapshot acquisition or recovery.
    index_schema_.set_schema_version(4999);
    ASSERT_EQ(4002, original.snapshot_holder_schema_version_);
    original.set_shared_async_delete_table_id(5003);
    ASSERT_SUCC(original.existing_async_index_table_ids_.push_back(5004));
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
    ObCurTraceId::TraceId trace_id;
    trace_id.init(addr);
    ASSERT_SUCC(original.set_trace_id(trace_id));

    ObArenaAllocator allocator("AsyncVecRecMsg");
    ObDDLTaskRecord record;
    ASSERT_SUCC(original.convert_to_record(record, allocator));
    ASSERT_TRUE(record.is_valid());
    ASSERT_EQ(snapshot, record.snapshot_version_);
    ObAsyncVectorIndexBuildTask recovered;
    ASSERT_SUCC(recovered.init(record));
    ASSERT_SUCC(recovered.set_trace_id(record.trace_id_));
    ASSERT_EQ(scenario.data_version, recovered.get_data_format_version());
    ASSERT_EQ(scenario.parent_publish, recovered.uses_parent_publish_protocol());
    ASSERT_EQ(original.task_version_, recovered.task_version_);
    ASSERT_EQ(snapshot, recovered.snapshot_version_);
    ASSERT_EQ(snapshot > 0, recovered.is_parent_snapshot_owner());
    ASSERT_EQ(4002, recovered.snapshot_holder_schema_version_);
    ASSERT_EQ(5003, recovered.shared_async_delete_table_id_);
    ASSERT_EQ(1, recovered.existing_async_index_table_ids_.count());
    ASSERT_EQ(5004, recovered.existing_async_index_table_ids_.at(0));

    recovered.task_status_ = share::ObDDLTaskStatus::FAIL;
    recovered.ret_code_ = OB_CANCELED;
    ObDDLTaskRecord cancelled_record;
    ASSERT_SUCC(recovered.convert_to_record(cancelled_record, allocator));
    ASSERT_TRUE(cancelled_record.is_valid());
    ObAsyncVectorIndexBuildTask cancelled;
    ASSERT_SUCC(cancelled.init(cancelled_record));
    ASSERT_EQ(OB_CANCELED, cancelled.ret_code_);
    ASSERT_EQ(share::ObDDLTaskStatus::FAIL, cancelled.task_status_);
    ASSERT_EQ(snapshot, cancelled.snapshot_version_);
    ASSERT_EQ(4002, cancelled.snapshot_holder_schema_version_);
    index_schema_.set_schema_version(4002);

    record.task_version_ = ObAsyncVectorIndexBuildTask::OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION + 1;
    ObAsyncVectorIndexBuildTask unsupported;
    ASSERT_EQ(OB_INVALID_ARGUMENT, unsupported.init(record));
  }
}

TEST(test_ddl_task_struct, snapshot_retention_rejects_invalid_holder_key)
{
  ObMySQLTransaction trans;
  share::schema::ObTableSchema data_schema;
  share::schema::ObTableSchema index_schema;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      share::ObDDLUtil::hold_snapshot(trans, data_schema, index_schema, 100, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      share::ObDDLUtil::hold_snapshot(trans, data_schema, index_schema, 100, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      share::ObDDLUtil::hold_snapshot(trans, data_schema, index_schema, 0, 4002));
}

// Models a publish RPC that commits one partition before returning an error.
// The two tasks in the recovery test share this durable partition state.
class AsyncVectorPublishOp : public share::ObIAsyncIndexDDLPublishOp
{
public:
  explicit AsyncVectorPublishOp(const int64_t snapshot)
    : snapshot_(snapshot), prepare_calls_(0), gts_calls_(0), publish_calls_(0),
      next_gts_(1000), publish_ret_(OB_EAGAIN), partition_versions_{snapshot, snapshot}
  {}
  int prepare(bool &need_publish) override
  {
    ++prepare_calls_;
    need_publish = true;
    for (auto &version : partition_versions_) {
      version = MAX(version, snapshot_);
    }
    return OB_SUCCESS;
  }
  int get_gts(share::SCN &scn) override
  {
    ++gts_calls_;
    return scn.convert_for_tx(++next_gts_);
  }
  int publish(const share::SCN &scn) override
  {
    ++publish_calls_;
    partition_versions_[0] = scn.get_val_for_tx();
    if (OB_SUCCESS == publish_ret_) {
      partition_versions_[1] = scn.get_val_for_tx();
    }
    return publish_ret_;
  }
  const int64_t snapshot_;
  int64_t prepare_calls_;
  int64_t gts_calls_;
  int64_t publish_calls_;
  int64_t next_gts_;
  int publish_ret_;
  int64_t partition_versions_[2];
};

TEST_F(TestAsyncVectorBuildTiming, publish_retries_keep_task_barrier_until_status_exit)
{
  ObAsyncVectorIndexBuildTask task;
  ASSERT_SUCC(task.init(1001, 7001, &data_schema_, &index_schema_, 4001,
      1, 0, 0, 0, arg_, DATA_VERSION_4_6_3_0, 100, true));
  task.task_status_ = share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH;
  AsyncVectorPublishOp op(task.snapshot_version_);
  auto &target = task.async_index_publish_refresh_target_;
  ASSERT_EQ(OB_EAGAIN, target.refresh(op, share::SCN::invalid_scn()));
  ASSERT_EQ(1001, op.partition_versions_[0]);
  ASSERT_EQ(100, op.partition_versions_[1]);
  ASSERT_SUCC(task.handle_process_error(OB_EAGAIN));
  ASSERT_EQ(share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, task.task_status_);
  op.publish_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, target.refresh(op, share::SCN::invalid_scn()));
  ASSERT_SUCC(task.handle_process_error(OB_TIMEOUT));
  op.publish_ret_ = OB_SUCCESS;
  ASSERT_SUCC(target.refresh(op, share::SCN::invalid_scn()));
  // Enabling the index or committing TAKE_EFFECT may fail after publication.
  ASSERT_SUCC(task.handle_process_error(OB_SCHEMA_EAGAIN));
  ASSERT_SUCC(target.refresh(op, share::SCN::invalid_scn()));
  ASSERT_EQ(1, op.prepare_calls_);
  ASSERT_EQ(1, op.gts_calls_);
  ASSERT_EQ(4, op.publish_calls_);
  ASSERT_EQ(1001, op.partition_versions_[0]);
  ASSERT_EQ(1001, op.partition_versions_[1]);
  task.clear_old_status_context();
  ASSERT_FALSE(target.is_prepared());
}

TEST_F(TestAsyncVectorBuildTiming, recovered_publish_realigns_durable_cohort_with_new_barrier)
{
  ObAsyncVectorIndexBuildTask original;
  ASSERT_SUCC(original.init(1001, 7001, &data_schema_, &index_schema_, 4001,
      1, 0, 0, 0, arg_, DATA_VERSION_4_6_3_0, 100, true));
  original.task_status_ = share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH;
  original.set_shared_async_delete_table_id(5003);
  original.set_baseline_task(7002);
  ASSERT_SUCC(original.existing_async_index_table_ids_.push_back(5004));
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  ObCurTraceId::TraceId trace_id;
  trace_id.init(addr);
  ASSERT_SUCC(original.set_trace_id(trace_id));
  ObArenaAllocator allocator("AsyncVecPublish");
  ObDDLTaskRecord before_publish;
  ASSERT_SUCC(original.convert_to_record(before_publish, allocator));
  AsyncVectorPublishOp op(original.snapshot_version_);
  ASSERT_EQ(OB_EAGAIN, original.async_index_publish_refresh_target_.refresh(op, share::SCN::invalid_scn()));
  ASSERT_TRUE(original.async_index_publish_refresh_target_.is_prepared());
  ObDDLTaskRecord record;
  ASSERT_SUCC(original.convert_to_record(record, allocator));
  ASSERT_EQ(before_publish.message_, record.message_);
  ObAsyncVectorIndexBuildTask recovered;
  ASSERT_SUCC(recovered.init(record));
  ASSERT_EQ(original.task_status_, recovered.task_status_);
  ASSERT_EQ(100, recovered.snapshot_version_);
  ASSERT_TRUE(recovered.uses_parent_publish_protocol());
  ASSERT_EQ(5003, recovered.shared_async_delete_table_id_);
  ASSERT_TRUE(recovered.baseline_task_submitted_);
  ASSERT_EQ(7002, recovered.baseline_task_id_);
  ASSERT_EQ(1, recovered.existing_async_index_table_ids_.count());
  ASSERT_EQ(5004, recovered.existing_async_index_table_ids_.at(0));
  auto &target = recovered.async_index_publish_refresh_target_;
  ASSERT_FALSE(target.is_prepared()); // The barrier is deliberately task-local.
  ASSERT_EQ(OB_EAGAIN, target.refresh(op, share::SCN::invalid_scn()));
  ASSERT_EQ(2, op.prepare_calls_);
  ASSERT_EQ(2, op.gts_calls_);
  ASSERT_EQ(1002, op.partition_versions_[0]);
  ASSERT_EQ(100, op.partition_versions_[1]);
  op.publish_ret_ = OB_SUCCESS;
  ASSERT_SUCC(target.refresh(op, share::SCN::invalid_scn()));
  ASSERT_EQ(2, op.prepare_calls_);
  ASSERT_EQ(2, op.gts_calls_);
  ASSERT_EQ(1002, op.partition_versions_[0]);
  ASSERT_EQ(1002, op.partition_versions_[1]);
}

TEST(test_ddl_task_struct, async_vector_parent_snapshot_ownership)
{
  ObAsyncVectorIndexBuildTask task;
  task.snapshot_version_ = 100;
  ASSERT_TRUE(task.is_parent_snapshot_owner());

  task.finish_snapshot_release(true /* snapshot_released */, false /* trans_committed */);
  ASSERT_EQ(100, task.snapshot_version_);
  ASSERT_TRUE(task.is_parent_snapshot_owner());
  task.finish_snapshot_release(false /* snapshot_released */, true /* trans_committed */);
  ASSERT_EQ(100, task.snapshot_version_);
  ASSERT_TRUE(task.is_parent_snapshot_owner());
  task.finish_snapshot_release(true /* snapshot_released */, true /* trans_committed */);
  ASSERT_EQ(0, task.snapshot_version_);
  ASSERT_FALSE(task.is_parent_snapshot_owner());

  task.snapshot_version_ = 100;
  task.set_baseline_task(7001);
  ASSERT_FALSE(task.is_parent_snapshot_owner());
  task.create_index_arg_.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  task.data_format_version_ = DATA_VERSION_4_6_3_0;
  ASSERT_TRUE(task.uses_parent_publish_protocol());
  ASSERT_TRUE(task.is_parent_snapshot_owner());
  task.baseline_task_submitted_ = false;
  task.parent_task_id_ = 6001;
  ASSERT_FALSE(task.is_offline_redefinition_child());
  ASSERT_TRUE(task.is_parent_snapshot_owner());
  task.create_index_arg_.is_offline_rebuild_ = true;
  ASSERT_FALSE(task.uses_parent_publish_protocol());
  ASSERT_TRUE(task.is_offline_redefinition_child());
  ASSERT_FALSE(task.is_parent_snapshot_owner());
  task.create_index_arg_.is_offline_rebuild_ = false;
  task.snapshot_version_ = 0;
  ASSERT_FALSE(task.is_parent_snapshot_owner());
}

TEST(test_ddl_task_struct, async_vector_refresh_and_take_effect_retry_contract)
{
  ObAsyncVectorIndexBuildTask task;
  ASSERT_FALSE(task.are_take_effect_schemas_ready(false, false));
  ASSERT_FALSE(task.are_take_effect_schemas_ready(true, false));
  ASSERT_FALSE(task.are_take_effect_schemas_ready(false, true));
  ASSERT_TRUE(task.are_take_effect_schemas_ready(true, true));

  ASSERT_TRUE(share::ObIDDLTask::in_ddl_retry_white_list(OB_SCHEMA_EAGAIN));
  task.task_status_ = share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH;
  ASSERT_SUCC(task.handle_process_error(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(share::ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH,
      task.task_status_);
  task.task_status_ = share::ObDDLTaskStatus::TAKE_EFFECT;
  ASSERT_SUCC(task.handle_process_error(OB_SCHEMA_EAGAIN));
  ASSERT_EQ(share::ObDDLTaskStatus::TAKE_EFFECT, task.task_status_);
}

TEST(test_ddl_task_struct, async_vector_failure_waits_for_baseline_contract)
{
  ObAsyncVectorIndexBuildTask task;
  bool is_finish = false;
  ASSERT_SUCC(task.wait_baseline_task_stop(is_finish));
  ASSERT_TRUE(is_finish);

  task.set_baseline_task(-1);
  is_finish = false;
  ASSERT_SUCC(task.wait_baseline_task_stop(is_finish));
  ASSERT_TRUE(is_finish);

  task.set_baseline_task(7001);
  is_finish = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.wait_baseline_task_stop(is_finish));
  ASSERT_FALSE(is_finish);
}

TEST(test_ddl_task_struct, async_ivf_drop_ddl_type_contract)
{
  ASSERT_EQ(34, static_cast<int64_t>(share::DDL_DROP_ASYNC_VEC_IVF_INDEX));
  ASSERT_STREQ("DDL_DROP_ASYNC_VEC_IVF_INDEX",
               share::get_ddl_type(share::DDL_DROP_ASYNC_VEC_IVF_INDEX));

  ObDropFTSIndexTask task;
  task.task_type_ = share::DDL_DROP_ASYNC_FTS_INDEX;
  ASSERT_TRUE(task.is_async_fts_task());
  task.task_type_ = share::DDL_DROP_ASYNC_VEC_IVF_INDEX;
  ASSERT_FALSE(task.is_async_fts_task());
  task.task_type_ = share::DDL_DROP_FTS_INDEX;
  ASSERT_FALSE(task.is_async_fts_task());

  ObDropAsyncVectorIndexTask async_vector_task;
  ObVecIndexDDLChildTaskInfo invalid_child;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      async_vector_task.init(1001, 1, 5001, share::DDL_DROP_ASYNC_FTS_INDEX,
          invalid_child, invalid_child, ObString::make_string("drop index i on t"),
          1, 0, 5002));
}

TEST(test_ddl_task_struct, async_ivf_prepare_arg_uses_round_allocator)
{
  ObAsyncVectorIndexBuildTask task;
  task.create_index_arg_.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  task.create_index_arg_.index_name_ = ObString::make_string("idx_bbq");
  task.parallelism_ = 4;
  task.create_index_arg_.index_table_id_ = 5001;
  task.create_index_arg_.index_schema_.set_table_id(5001);
  task.create_index_arg_.is_rebuild_index_ = true;
  task.create_index_arg_.is_offline_rebuild_ = true;
  ObColumnSortItem item;
  item.column_name_ = ObString::make_string("vec_col");
  ASSERT_SUCC(task.create_index_arg_.index_columns_.push_back(item));
  ASSERT_SUCC(task.create_index_arg_.store_columns_.push_back(
      ObString::make_string("vec_data")));
  ASSERT_SUCC(task.create_index_arg_.hidden_store_columns_.push_back(
      ObString::make_string("hidden_vec_data")));
  const int64_t task_allocator_used = task.allocator_.used();

  for (int64_t i = 0; i < 3; ++i) {
    ObArenaAllocator round_allocator("AsyncDelT");
    obrpc::ObCreateIndexArg arg;
    ASSERT_SUCC(task.construct_async_delete_arg(round_allocator, arg));
    ASSERT_EQ(task_allocator_used, task.allocator_.used());
    ASSERT_EQ(share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, arg.index_type_);
    ASSERT_EQ(1, arg.index_columns_.count());
    ASSERT_EQ(0, arg.store_columns_.count());
    ASSERT_EQ(0, arg.hidden_store_columns_.count());
    ASSERT_EQ(4, arg.parallelism_);
    ASSERT_EQ(OB_INVALID_ID, arg.index_table_id_);
    ASSERT_EQ(OB_INVALID_ID, arg.index_schema_.get_table_id());
    ASSERT_FALSE(arg.is_rebuild_index_);
    ASSERT_TRUE(arg.is_offline_rebuild_);
  }
}

TEST(test_ddl_task_struct, async_vector_drop_message_and_child_contract)
{
  const uint64_t tenant_id = 1001;
  ObArenaAllocator allocator("AsyncVecDrop");
  ObDropAsyncVectorIndexTask task;
  task.task_version_ = 1;
  task.user_index_.index_name_ = ObString::make_string("idx_bbq");
  task.user_index_.table_id_ = 5001;
  task.user_index_.task_id_ = 7001;
  task.async_delete_.index_name_ = ObString::make_string("__idx_async_delete");
  task.async_delete_.table_id_ = 5002;
  task.async_delete_.task_id_ = -1;
  task.ddl_stmt_str_ = ObString::make_string("drop index idx_bbq on t");
  task.drop_user_index_finish_ = true;
  task.drop_async_delete_finish_ = false;

  ASSERT_EQ(ObDropAsyncVectorIndexTask::ASYNC_DELETE, task.get_next_child_type());
  ASSERT_TRUE(ObDropAsyncVectorIndexTask::is_skipped_child_task_id(-1));
  ASSERT_FALSE(ObDropAsyncVectorIndexTask::is_skipped_child_task_id(0));
  ASSERT_FALSE(ObDropAsyncVectorIndexTask::is_skipped_child_task_id(7001));

  const int64_t size = task.get_serialize_param_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObDropAsyncVectorIndexTask recovered;
  pos = 0;
  ASSERT_SUCC(recovered.deserialize_params_from_message(tenant_id, buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_EQ(7001, recovered.user_index_.task_id_);
  ASSERT_EQ(-1, recovered.async_delete_.task_id_);
  ASSERT_TRUE(recovered.drop_user_index_finish_);
  ASSERT_FALSE(recovered.drop_async_delete_finish_);
  ASSERT_EQ(ObDropAsyncVectorIndexTask::ASYNC_DELETE, recovered.get_next_child_type());

  recovered.drop_async_delete_finish_ = true;
  ASSERT_EQ(ObDropAsyncVectorIndexTask::NO_CHILD, recovered.get_next_child_type());
  recovered.drop_user_index_finish_ = false;
  ASSERT_EQ(ObDropAsyncVectorIndexTask::USER_INDEX, recovered.get_next_child_type());

  recovered.drop_user_index_finish_ = true;
  recovered.drop_async_delete_finish_ = true;
  recovered.async_delete_.task_id_ = 7002;
  recovered.set_drop_user_index_task_id(7003);
  ASSERT_EQ(7003, recovered.user_index_.task_id_);
  ASSERT_FALSE(recovered.drop_user_index_finish_);
  recovered.set_drop_async_delete_task_id(7004);
  ASSERT_EQ(7004, recovered.async_delete_.task_id_);
  ASSERT_FALSE(recovered.drop_async_delete_finish_);
  recovered.drop_user_index_finish_ = true;
  recovered.drop_async_delete_finish_ = true;
  recovered.async_delete_.task_id_ = 7002;
  ASSERT_FALSE(recovered.can_switch_to_success(
      false /* user_index_exist */, true /* async_delete_exist */));
  ASSERT_TRUE(recovered.can_switch_to_success(
      false /* user_index_exist */, false /* async_delete_exist */));
  ASSERT_FALSE(recovered.can_switch_to_success(
      true /* user_index_exist */, false /* async_delete_exist */));

  // A skipped shared-delete child remains valid only while another user async
  // member still owns the shared schema.
  bool need_persist = false;
  recovered.async_delete_.task_id_ = -1;
  recovered.drop_async_delete_finish_ = true;
  ASSERT_TRUE(recovered.reconcile_skipped_async_delete(
      false /* user_index_exist */, true /* async_delete_exist */,
      true /* data_table_exist */, true /* has_user_async_index */,
      need_persist));
  ASSERT_FALSE(need_persist);
  ASSERT_EQ(-1, recovered.async_delete_.task_id_);
  ASSERT_TRUE(recovered.drop_async_delete_finish_);

  // If membership becomes empty while the skipped shared schema is still
  // visible, reset the durable child state so WAIT can submit a leaf DROP.
  ASSERT_FALSE(recovered.reconcile_skipped_async_delete(
      false /* user_index_exist */, true /* async_delete_exist */,
      true /* data_table_exist */, false /* has_user_async_index */,
      need_persist));
  ASSERT_TRUE(need_persist);
  ASSERT_EQ(0, recovered.async_delete_.task_id_);
  ASSERT_FALSE(recovered.drop_async_delete_finish_);

  const int64_t reset_size = recovered.get_serialize_param_size();
  char *reset_buf = static_cast<char *>(allocator.alloc(reset_size));
  ASSERT_NE(nullptr, reset_buf);
  pos = 0;
  ASSERT_SUCC(recovered.serialize_params_to_message(reset_buf, reset_size, pos));
  ASSERT_EQ(reset_size, pos);
  ObDropAsyncVectorIndexTask reset_recovered;
  pos = 0;
  ASSERT_SUCC(reset_recovered.deserialize_params_from_message(
      tenant_id, reset_buf, reset_size, pos));
  ASSERT_EQ(0, reset_recovered.async_delete_.task_id_);
  ASSERT_FALSE(reset_recovered.drop_async_delete_finish_);
  ASSERT_FALSE(reset_recovered.reconcile_skipped_async_delete(
      false /* user_index_exist */, true /* async_delete_exist */,
      true /* data_table_exist */, false /* has_user_async_index */,
      need_persist));
  ASSERT_FALSE(need_persist);

  reset_recovered.async_delete_.task_id_ = -1;
  reset_recovered.drop_async_delete_finish_ = true;
  ASSERT_TRUE(reset_recovered.reconcile_skipped_async_delete(
      false /* user_index_exist */, false /* async_delete_exist */,
      true /* data_table_exist */, false /* has_user_async_index */,
      need_persist));
  ASSERT_FALSE(need_persist);
}

TEST(test_ddl_task_struct, async_vector_drop_optional_shared_delete_contract)
{
  const uint64_t tenant_id = 1001;
  ObArenaAllocator allocator("AsyncVecDrop");
  ObDropAsyncVectorIndexTask task;
  task.task_version_ = 1;
  task.user_index_.index_name_ = ObString::make_string("idx_bbq");
  task.user_index_.table_id_ = 5001;
  task.user_index_.task_id_ = 7001;
  task.ddl_stmt_str_ = ObString::make_string("drop index idx_bbq on t");
  task.drop_user_index_finish_ = true;
  task.drop_async_delete_finish_ = false;
  ASSERT_FALSE(task.async_delete_.is_valid());
  ASSERT_EQ(ObDropAsyncVectorIndexTask::ASYNC_DELETE, task.get_next_child_type());

  const int64_t size = task.get_serialize_param_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObDropAsyncVectorIndexTask recovered;
  pos = 0;
  ASSERT_SUCC(recovered.deserialize_params_from_message(tenant_id, buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_TRUE(recovered.user_index_.is_valid());
  ASSERT_FALSE(recovered.async_delete_.is_valid());
  ASSERT_TRUE(recovered.drop_user_index_finish_);
  ASSERT_FALSE(recovered.drop_async_delete_finish_);
  ASSERT_EQ(ObDropAsyncVectorIndexTask::ASYNC_DELETE, recovered.get_next_child_type());
}

TEST(test_ddl_task_struct, user_async_index_membership_contract)
{
  using namespace share::schema;
  ObTableSchema async_search;
  ObSEArray<const ObSimpleTableSchemaV2 *, 1> index_schemas;
  bool has_user_async_index = false;
  async_search.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
  async_search.set_index_status(INDEX_STATUS_AVAILABLE);
  ASSERT_SUCC(index_schemas.push_back(&async_search));

  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_TRUE(has_user_async_index);

  async_search.set_index_status(INDEX_STATUS_INDEX_ERROR);
  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_FALSE(has_user_async_index);
}

TEST(test_ddl_task_struct, async_vector_drop_child_ddl_stmt_contract)
{
  using namespace share::schema;
  ObTableSchema user_index;
  ObTableSchema async_delete;
  user_index.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  async_delete.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  const ObString parent_stmt =
      ObString::make_string("drop index idx_bbq on t");
  ASSERT_EQ(parent_stmt,
      ObDropAsyncVectorIndexTask::get_child_ddl_stmt(
          user_index, parent_stmt));
  ASSERT_TRUE(ObDropAsyncVectorIndexTask::get_child_ddl_stmt(
      async_delete, parent_stmt).empty());
}

TEST(test_ddl_task_struct, async_vector_metadata_cleanup_membership_contract)
{
  ObSEArray<const ObSimpleTableSchemaV2 *, 4> index_schemas;
  ObSimpleTableSchemaV2 async_delete_schema;
  ObSimpleTableSchemaV2 async_fts_schema;
  ObSimpleTableSchemaV2 async_ivf_schema;
  bool has_user_async_index = true;
  async_delete_schema.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  async_fts_schema.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  async_ivf_schema.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);

  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_FALSE(has_user_async_index);
  ASSERT_SUCC(index_schemas.push_back(&async_delete_schema));
  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_FALSE(has_user_async_index);
  ASSERT_SUCC(index_schemas.push_back(&async_fts_schema));
  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_TRUE(has_user_async_index);
  index_schemas.reset();
  ASSERT_SUCC(index_schemas.push_back(&async_ivf_schema));
  ASSERT_SUCC(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
      index_schemas, has_user_async_index));
  ASSERT_TRUE(has_user_async_index);
}

TEST(test_ddl_task_struct, async_vector_failure_drop_arg_contract)
{
  using namespace share::schema;
  const uint64_t tenant_id = 1001;
  const uint64_t database_id = 4001;
  const uint64_t data_table_id = 5001;
  const uint64_t user_index_id = 5002;
  const uint64_t async_delete_id = 5003;
  ObArenaAllocator allocator("AsyncDropArg");
  ObString user_table_name;
  ObString async_delete_table_name;
  ASSERT_SUCC(ObTableSchema::build_index_table_name(
      allocator, data_table_id, ObString::make_string("idx_bbq"),
      user_table_name));
  ASSERT_SUCC(ObTableSchema::build_index_table_name(
      allocator, data_table_id, ObString::make_string("async_delete"),
      async_delete_table_name));

  ObDatabaseSchema database_schema;
  database_schema.set_tenant_id(tenant_id);
  database_schema.set_database_id(database_id);
  ASSERT_SUCC(database_schema.set_database_name(
      ObString::make_string("db1")));
  ObTableSchema data_schema;
  data_schema.set_tenant_id(tenant_id);
  data_schema.set_database_id(database_id);
  data_schema.set_table_id(data_table_id);
  ASSERT_SUCC(data_schema.set_table_name(ObString::make_string("t1")));
  ObTableSchema user_index;
  user_index.set_tenant_id(tenant_id);
  user_index.set_database_id(database_id);
  user_index.set_table_id(user_index_id);
  user_index.set_data_table_id(data_table_id);
  user_index.set_table_type(USER_INDEX);
  user_index.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  ASSERT_SUCC(user_index.set_table_name(user_table_name));
  ObTableSchema async_delete;
  async_delete.set_tenant_id(tenant_id);
  async_delete.set_database_id(database_id);
  async_delete.set_table_id(async_delete_id);
  async_delete.set_data_table_id(data_table_id);
  async_delete.set_table_type(USER_INDEX);
  async_delete.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_SUCC(async_delete.set_table_name(async_delete_table_name));

  ObAsyncVectorIndexBuildTask task;
  task.tenant_id_ = tenant_id;
  task.task_id_ = 7001;
  task.create_index_arg_.session_id_ = 8001;
  task.create_index_arg_.ddl_stmt_str_ =
      ObString::make_string("create async vector index idx_bbq on t1(vec)");

  obrpc::ObDropIndexArg arg;
  ASSERT_SUCC(task.construct_failure_drop_arg(
      data_schema, database_schema, &user_index, &async_delete, arg));
  ASSERT_TRUE(arg.is_parent_task_dropping_user_async_index_);
  ASSERT_EQ(user_index_id, arg.index_table_id_);
  ASSERT_EQ(user_index_id, arg.table_id_);
  ASSERT_EQ(2, arg.index_ids_.count());
  ASSERT_EQ(user_index_id, arg.index_ids_.at(0));
  ASSERT_EQ(async_delete_id, arg.index_ids_.at(1));

  arg.reset();
  ASSERT_SUCC(task.construct_failure_drop_arg(
      data_schema, database_schema, &user_index, nullptr, arg));
  ASSERT_FALSE(arg.is_parent_task_dropping_user_async_index_);
  ASSERT_EQ(user_index_id, arg.index_table_id_);
  ASSERT_EQ(user_index_id, arg.table_id_);
  ASSERT_EQ(1, arg.index_ids_.count());
  ASSERT_EQ(user_index_id, arg.index_ids_.at(0));

  arg.reset();
  ASSERT_SUCC(task.construct_failure_drop_arg(
      data_schema, database_schema, nullptr, &async_delete, arg));
  ASSERT_FALSE(arg.is_parent_task_dropping_user_async_index_);
  ASSERT_EQ(async_delete_id, arg.index_table_id_);
  ASSERT_EQ(async_delete_id, arg.table_id_);
  ASSERT_EQ(1, arg.index_ids_.count());
  ASSERT_EQ(async_delete_id, arg.index_ids_.at(0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.construct_failure_drop_arg(
      data_schema, database_schema, nullptr, nullptr, arg));
}

TEST(test_ddl_task_struct, async_fts_drop_message_compatibility)
{
  const uint64_t tenant_id = 1001;
  ObArenaAllocator allocator("AsyncFtsDrop");
  for (int64_t version = 1; version <= 2; ++version) {
    ObDropFTSIndexTask task;
    task.task_version_ = version;
    task.domain_index_.index_name_ = ObString::make_string("fts_user");
    task.domain_index_.table_id_ = 6001;
    task.domain_index_.task_id_ = 8001;
    task.async_delete_.index_name_ = ObString::make_string("__idx_async_delete");
    task.async_delete_.table_id_ = 6002;
    task.async_delete_.task_id_ = 8002;
    task.ddl_stmt_str_ = ObString::make_string("drop index fts_user on t");
    task.drop_domain_index_finish_ = true;
    task.drop_async_delete_index_finish_ = true;
    const int64_t size = task.get_serialize_param_size();
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_SUCC(task.serialize_params_to_message(buf, size, pos));
    ASSERT_EQ(size, pos);

    ObDropFTSIndexTask recovered;
    recovered.task_version_ = version;
    pos = 0;
    ASSERT_SUCC(recovered.deserialize_params_from_message(tenant_id, buf, size, pos));
    ASSERT_EQ(size, pos);
    ASSERT_EQ(8001, recovered.domain_index_.task_id_);
    if (1 == version) {
      ASSERT_FALSE(recovered.async_delete_.is_valid());
      ASSERT_FALSE(recovered.drop_async_delete_index_finish_);
    } else {
      ASSERT_EQ(8002, recovered.async_delete_.task_id_);
      ASSERT_TRUE(recovered.drop_async_delete_index_finish_);
    }
  }
}

TEST(test_ddl_task_struct, drop_index_arg_user_async_parent_context_wire_compatibility)
{
  ObArenaAllocator allocator("DropArgWire");
  obrpc::ObDropIndexArg legacy_async_fts_writer;
  legacy_async_fts_writer.is_parent_task_dropping_user_async_index_ = true;
  const int64_t size = legacy_async_fts_writer.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(legacy_async_fts_writer.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);

  obrpc::ObDropIndexArg recovered;
  pos = 0;
  ASSERT_SUCC(recovered.deserialize(buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_TRUE(recovered.is_parent_task_dropping_user_async_index_);

  obrpc::ObDropIndexArg assigned;
  ASSERT_SUCC(assigned.assign(recovered));
  ASSERT_TRUE(assigned.is_parent_task_dropping_user_async_index_);
  assigned.reset();
  ASSERT_FALSE(assigned.is_parent_task_dropping_user_async_index_);
}

TEST(test_ddl_task_struct, recognize_user_async_index_schemas_contract)
{
  using namespace share::schema;
  ObDDLService ddl_service;
  ObIndexBuilder index_builder(ddl_service);
  ObTableSchema user_index;
  user_index.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  ObTableSchema async_delete;
  async_delete.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ObSEArray<ObTableSchema, 3> schemas;
  int64_t index_ith = -1;
  int64_t async_delete_ith = -1;

  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_SUCC(index_builder.recognize_user_async_index_schemas(
      schemas, index_ith, async_delete_ith));
  ASSERT_EQ(0, index_ith);
  ASSERT_EQ(1, async_delete_ith);

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_SUCC(index_builder.recognize_user_async_index_schemas(
      schemas, index_ith, async_delete_ith));
  ASSERT_EQ(1, index_ith);
  ASSERT_EQ(0, async_delete_ith);

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_SUCC(index_builder.recognize_user_async_index_schemas(
      schemas, index_ith, async_delete_ith));
  ASSERT_EQ(0, index_ith);
  ASSERT_EQ(-1, async_delete_ith);

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_SUCC(index_builder.recognize_user_async_index_schemas(
      schemas, index_ith, async_delete_ith));
  ASSERT_EQ(0, index_ith);
  ASSERT_EQ(0, async_delete_ith);

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      index_builder.recognize_user_async_index_schemas(
          schemas, index_ith, async_delete_ith));

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      index_builder.recognize_user_async_index_schemas(
          schemas, index_ith, async_delete_ith));

  schemas.reuse();
  ASSERT_SUCC(schemas.push_back(user_index));
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_SUCC(schemas.push_back(async_delete));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      index_builder.recognize_user_async_index_schemas(
          schemas, index_ith, async_delete_ith));
}

TEST(test_ddl_task_struct, rowkey_doc_cleanup_ownership)
{
  const uint64_t rowkey_doc_table_id = 1001;
  const share::ObDDLType sync_task_types[] = {
    share::DDL_CREATE_FTS_INDEX,
    share::DDL_CREATE_MULTIVALUE_INDEX,
    share::DDL_CREATE_VEC_SPIV_INDEX
  };
  ObFtsIndexBuildTask task;
  task.rowkey_doc_aux_table_id_ = rowkey_doc_table_id;

  for (int64_t i = 0; i < ARRAYSIZEOF(sync_task_types); ++i) {
    obrpc::ObDropIndexArg drop_index_arg;
    task.task_type_ = sync_task_types[i];
    ASSERT_SUCC(task.append_rowkey_doc_table_id_for_cleanup(drop_index_arg));
    ASSERT_EQ(1, drop_index_arg.index_ids_.count());
    ASSERT_EQ(rowkey_doc_table_id, drop_index_arg.index_ids_.at(0));
  }

  const share::ObDDLType async_task_types[] = {
    share::DDL_CREATE_ASYNC_FTS_INDEX,
    share::DDL_CREATE_ASYNC_SEARCH_INDEX
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(async_task_types); ++i) {
    obrpc::ObDropIndexArg drop_index_arg;
    task.task_type_ = async_task_types[i];
    ASSERT_SUCC(task.append_rowkey_doc_table_id_for_cleanup(drop_index_arg));
    ASSERT_EQ(0, drop_index_arg.index_ids_.count());
  }

  obrpc::ObDropIndexArg drop_index_arg;
  task.task_type_ = share::DDL_CREATE_FTS_INDEX;
  task.rowkey_doc_aux_table_id_ = OB_INVALID_ID;
  ASSERT_SUCC(task.append_rowkey_doc_table_id_for_cleanup(drop_index_arg));
  ASSERT_EQ(0, drop_index_arg.index_ids_.count());
}

TEST(test_ddl_task_struct, drop_search_index_types_are_not_cancellable)
{
  ObDropSearchIndexTask task;
  ObDDLTask *ddl_task = &task;
  task.task_type_ = share::ObDDLType::DDL_DROP_SEARCH_INDEX;
  ASSERT_FALSE(task.ObDDLTask::is_ddl_task_can_be_cancelled());
  ASSERT_FALSE(ddl_task->is_ddl_task_can_be_cancelled());
  task.task_type_ = share::ObDDLType::DDL_DROP_ASYNC_SEARCH_INDEX;
  ASSERT_FALSE(task.ObDDLTask::is_ddl_task_can_be_cancelled());
  ASSERT_FALSE(ddl_task->is_ddl_task_can_be_cancelled());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  // Process-wide initialization must not repeat with the test bodies.
  if (oceanbase::common::OB_SUCCESS
      != oceanbase::share::ObLongopsMgr::get_instance().init()) {
    return 1;
  }
  const int ret = oceanbase::share::ObDDLSimPointMgr::get_instance().init();
  if (oceanbase::common::OB_SUCCESS != ret) {
    LOG_WARN("fail to initialize DDL simulation manager for tests", K(ret));
    return 1;
  }
  return RUN_ALL_TESTS();
}

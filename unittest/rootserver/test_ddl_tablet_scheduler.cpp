/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_ddl_common.h"
#define private public
#define protected public
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_rs_mgr.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_ddl_service_launcher.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ddl_task/ob_ddl_tablet_scheduler.h"
#include "rootserver/ob_ddl_service_launcher.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_multi_version_schema_service.h"
#undef protected
#undef private
#include "rootserver/ob_root_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"

namespace oceanbase
{
namespace share
{
class TabletMergeStatusMock
{
public:
  MOCK_METHOD6(check, int(const uint64_t, const common::ObIArray<common::ObTabletID> &,
                          const int64_t, const bool, bool &,
                          common::hash::ObHashMap<common::ObTabletID, bool> *));
};

static TabletMergeStatusMock *tablet_merge_status_mock = nullptr;

// Mock only the shared library's RPC boundary; keep real FTS schema lookup and tablet mapping.
int ObCheckTabletDataComplementOp::check_tablet_merge_status(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t snapshot_version,
    const bool only_check_one_replica,
    bool &is_all_tablets_commited,
    common::hash::ObHashMap<common::ObTabletID, bool> *tablet_finished_map)
{
  return tablet_merge_status_mock->check(
      tenant_id, tablet_ids, snapshot_version, only_check_one_replica,
      is_all_tablets_commited, tablet_finished_map);
}

// Stub external queries for init; keep real candidate filtering, schema mapping, and queue insertion.
int ObLocationService::get_leader_with_retry_until_timeout(
    const int64_t, const uint64_t, const ObLSID &, common::ObAddr &leader,
    const int64_t, const int64_t)
{
  leader = common::ObAddr(common::ObAddr::IPV4, "127.0.0.1", 2882);
  return OB_SUCCESS;
}

int ObTabletToLSTableOperator::batch_get_ls(
    common::ObISQLClient &, const uint64_t,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    common::ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    if (OB_FAIL(ls_ids.push_back(ObLSID(1001 + i)))) {
      ADD_FAILURE() << "append test LS failed: " << ret;
    }
  }
  return ret;
}

int ObDDLUtil::get_tablet_data_size(
    const uint64_t &, const common::ObTabletID &, const ObLSID &, int64_t &data_size)
{
  data_size = 1024;
  return OB_SUCCESS;
}

int ObDDLUtil::get_tablet_data_row_cnt(
    const uint64_t &, const common::ObTabletID &, const ObLSID &,
    const uint64_t, const common::ObAddr &, int64_t &data_row_cnt)
{
  data_row_cnt = 100;
  return OB_SUCCESS;
}
} // namespace share

namespace rootserver
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

static const uint64_t TEST_TENANT_ID = 1002;
static const uint64_t DATA_TABLE_ID = 500007;
static const uint64_t INDEX_TABLE_ID = 739754;
static const uint64_t DOC_WORD_TABLE_ID = 739755;
static const uint64_t INVERTED_TABLE_ID = 739756;
static const uint64_t FIRST_INVERTED_TABLET_ID = 300001;
static const int64_t SCHEMA_VERSION = 100;
static const int64_t LS_COUNT = 3;
static const uint64_t FIRST_TABLET_ID = 200001;

class DDLQueryResult : public ObMySQLResultImpl
{
public:
  DDLQueryResult(ObMySQLStatement &statement, const uint64_t tablet_id,
                const ObString &running_sql, const int64_t tablet_count)
    : ObMySQLResultImpl(statement), tablet_id_(tablet_id), running_sql_(running_sql),
      tablet_count_(tablet_count), pos_(-1) {}
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_varchar;
  int next() override
  {
    return ++pos_ < tablet_count_ && (tablet_id_ != 0 || !running_sql_.empty()) ? OB_SUCCESS : OB_ITER_END;
  }
  int get_int(const char *column, int64_t &value) const override
  {
    EXPECT_STREQ("task_id", column);
    value = tablet_id_ + pos_;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    EXPECT_STREQ("info", column);
    value = running_sql_;
    return OB_SUCCESS;
  }
private:
  uint64_t tablet_id_;
  ObString running_sql_;
  int64_t tablet_count_;
  int64_t pos_;
};

class DDLQueryResultHandler : public ObISQLResultHandler
{
public:
  DDLQueryResultHandler(const uint64_t tablet_id, const ObString &running_sql, const int64_t tablet_count)
    : statement_(), result_(statement_, tablet_id, running_sql, tablet_count) {}
  ObMySQLResult *mysql_result() override { return &result_; }
private:
  ObMySQLStatement statement_;
  DDLQueryResult result_;
};

class DDLQueryConnection : public ObMySQLConnection
{
public:
  DDLQueryConnection()
    : timeout_tablet_id_(0), missing_tablet_id_(0), callback_tablet_id_(0),
      session_ret_(OB_SUCCESS), scheduler_(nullptr) {}
  using ObMySQLConnection::execute_read;
  int execute_read(const uint64_t tenant_id, const ObString &sql,
                   ObISQLClient::ReadResult &result, bool is_user_sql = false,
                   const ObAddr *sql_exec_addr = nullptr) override
  {
    int ret = OB_SUCCESS;
    uint64_t tablet_id = 0;
    uint64_t last_tablet_id = 0;
    int64_t tablet_count = 1;
    ObString running_sql;
    const std::string sql_text(sql.ptr(), sql.length());
    if (sql_text.find("__all_virtual_session_info") != std::string::npos) {
      EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
      ret = session_ret_;
      running_sql = running_sql_;
    } else if (sql_text.find("__all_ddl_checksum") != std::string::npos) {
      EXPECT_EQ(TEST_TENANT_ID, tenant_id);
      const char *range = strstr(sql_text.c_str(), "AND task_id >= ");
      if (range == nullptr || 2 != sscanf(range, "AND task_id >= %lu and task_id <= %lu",
                                         &tablet_id, &last_tablet_id)) {
        ADD_FAILURE() << "Unexpected checksum query";
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(checksum_reads_.push_back(tablet_id))) {
      } else if (tablet_id == timeout_tablet_id_) {
        timeout_tablet_id_ = 0;
        ret = OB_TIMEOUT;
      } else if (tablet_id == callback_tablet_id_) {
        ObArray<ObTabletID> tablets;
        if (OB_FAIL(tablets.push_back(ObTabletID(tablet_id)))) {
        } else {
          ret = scheduler_->confirm_batch_tablets_status(
              1, true, ObLSID(1001 + tablet_id - FIRST_TABLET_ID), tablets);
        }
        callback_tablet_id_ = 0;
      }
      tablet_count = last_tablet_id - tablet_id + 1;
      if (tablet_id == missing_tablet_id_) {
        tablet_id = 0;
      }
    } else {
      ADD_FAILURE() << "Unexpected SQL: " << std::string(sql.ptr(), sql.length());
      ret = OB_NOT_SUPPORTED;
    }
    if (OB_SUCC(ret)) {
      DDLQueryResultHandler *handler = nullptr;
      ret = result.create_handler(handler, tablet_id, running_sql, tablet_count);
    }
    return ret;
  }
  uint64_t timeout_tablet_id_;
  uint64_t missing_tablet_id_;
  uint64_t callback_tablet_id_;
  int session_ret_;
  ObString running_sql_;
  ObArray<uint64_t> checksum_reads_;
  ObDDLTabletScheduler *scheduler_;
};

class DDLQueryPool : public ObMySQLConnectionPool
{
public:
  explicit DDLQueryPool(DDLQueryConnection &connection) : connection_(connection) {}
  using ObMySQLConnectionPool::acquire;
  using ObMySQLConnectionPool::release;
  int acquire(const uint64_t, ObISQLConnection *&connection, ObISQLClient *, const int32_t) override
  {
    connection = &connection_;
    return OB_SUCCESS;
  }
  int release(ObISQLConnection *connection, const bool) override
  {
    EXPECT_EQ(&connection_, connection);
    return OB_SUCCESS;
  }
private:
  DDLQueryConnection &connection_;
};

class TestDDLTabletScheduler : public ::testing::Test
{
public:
  TestDDLTabletScheduler() : pool_(connection_), old_proxy_(nullptr) {}
  static void init_schema_context()
  {
    // Populate local schema caches so the real session lookup never needs an observer.
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
    ASSERT_NO_FATAL_FAILURE(add_table_schema(DATA_TABLE_ID, "data_table", false));
    ASSERT_NO_FATAL_FAILURE(add_table_schema(INDEX_TABLE_ID, "__idx_500007_idx", true));
    ASSERT_NO_FATAL_FAILURE(add_table_schema(DOC_WORD_TABLE_ID, "__idx_500007_ft_fts_doc_word",
                                             true, INDEX_TYPE_FTS_DOC_WORD_LOCAL));
    ASSERT_NO_FATAL_FAILURE(add_table_schema(INVERTED_TABLE_ID, "__idx_500007_ft",
                                             true, INDEX_TYPE_FTS_INDEX_LOCAL,
                                             FIRST_INVERTED_TABLET_ID));
  }
  static void destroy_schema_context()
  {
    // Release cached schemas before the global KV cache; the singleton owns its schema stores.
    ObMultiVersionSchemaService::get_instance().schema_cache_.destroy();
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
    ASSERT_EQ(OB_SUCCESS, proxy_.init(&pool_));
    old_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy_;
    tablet_merge_status_mock = &merge_status_mock_;
    connection_.scheduler_ = &scheduler_;
    scheduler_.tenant_id_ = TEST_TENANT_ID;
    scheduler_.table_id_ = INDEX_TABLE_ID;
    scheduler_.ref_data_table_id_ = DATA_TABLE_ID;
    scheduler_.task_id_ = 12229406;
    scheduler_.snapshot_version_ = 1000;
    scheduler_.trace_id_.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 2882));
    ASSERT_EQ(OB_SUCCESS, scheduler_.all_ls_to_tablets_map_.create(8, "TestDDLSched"));
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_ls_to_tablets_map_.create(8, "TestDDLSched"));
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_ls_to_execution_id_.create(8, "TestDDLSched"));
    scheduler_.is_inited_ = true;
  }
  void TearDown() override
  {
    scheduler_.destroy();
    GCTX.sql_proxy_ = old_proxy_;
    tablet_merge_status_mock = nullptr;
  }
  static void prepare_schema_mgr(ObSchemaMgr &mgr, const uint64_t tenant_id)
  {
    ASSERT_EQ(OB_SUCCESS, mgr.init(tenant_id));
    mgr.set_schema_version(SCHEMA_VERSION);
    ObSimpleTenantSchema tenant;
    tenant.set_tenant_id(TEST_TENANT_ID);
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
  static void add_table_schema(const uint64_t table_id, const char *name, const bool is_index,
                               const ObIndexType index_type = INDEX_TYPE_NORMAL_LOCAL,
                               const uint64_t first_tablet_id = FIRST_TABLET_ID)
  {
    ObTableSchema table;
    table.set_tenant_id(TEST_TENANT_ID);
    table.set_database_id(500000);
    table.set_table_id(table_id);
    table.set_schema_version(SCHEMA_VERSION);
    table.set_table_type(is_index ? USER_INDEX : USER_TABLE);
    table.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, table.set_table_name(name));
    if (is_index) {
      table.set_data_table_id(DATA_TABLE_ID);
      table.set_index_type(index_type);
    }
    table.set_part_level(PARTITION_LEVEL_ONE);
    table.get_part_option().set_part_num(LS_COUNT);
    table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    for (int64_t i = 0; i < LS_COUNT; ++i) {
      ObPartition part;
      part.set_tenant_id(TEST_TENANT_ID);
      part.set_table_id(table_id);
      part.set_part_id(i);
      part.set_schema_version(SCHEMA_VERSION);
      part.set_tablet_id(first_tablet_id + i + (is_index ? 0 : 100));
      char part_name[16];
      snprintf(part_name, sizeof(part_name), "p%ld", i);
      ASSERT_EQ(OB_SUCCESS, part.set_part_name(part_name));
      ASSERT_EQ(OB_SUCCESS, table.add_partition(part));
    }
    ObSimpleTableSchemaV2 simple;
    ASSERT_EQ(OB_SUCCESS,
              ObMultiVersionSchemaService::get_instance().convert_to_simple_schema(table, simple));
    ASSERT_EQ(OB_SUCCESS, tenant_mgr_.add_table(simple));
    ASSERT_EQ(OB_SUCCESS, ObMultiVersionSchemaService::get_instance().schema_cache_.put_schema(
        TABLE_SCHEMA, TEST_TENANT_ID, table_id, SCHEMA_VERSION, table));
  }
  void add_running_ls(const int64_t index)
  {
    const ObLSID ls_id(1001 + index);
    ObArray<ObTabletID> tablets;
    ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID + index)));
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_task_ls_ids_before_.push_back(ls_id));
    ASSERT_EQ(OB_SUCCESS, scheduler_.all_ls_to_tablets_map_.set_refactored(ls_id, tablets));
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_ls_to_tablets_map_.set_refactored(ls_id, tablets));
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_ls_to_execution_id_.set_refactored(ls_id, 1));
  }
  int next_batch()
  {
    int ret = OB_SUCCESS;
    ObSchemaGetterGuard schema_guard;
    int64_t parallelism = 0;
    int64_t execution_id = 0;
    ObLSID ls_id;
    ObAddr leader;
    ObArray<ObTabletID> tablets;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        TEST_TENANT_ID, schema_guard))) {
    } else {
      ret = scheduler_.get_next_batch_tablets(
          true, parallelism, execution_id, ls_id, leader, tablets, schema_guard);
    }
    return ret;
  }
  void expect_pending(const int64_t first, const int64_t count)
  {
    ASSERT_EQ(count, scheduler_.running_task_ls_ids_before_.count());
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(ObLSID(1001 + first + i), scheduler_.running_task_ls_ids_before_.at(i));
    }
  }
  void complete_callback(const int64_t index)
  {
    ObArray<ObTabletID> tablets;
    ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID + index)));
    ASSERT_EQ(OB_SUCCESS, scheduler_.confirm_batch_tablets_status(
        1, true, ObLSID(1001 + index), tablets));
  }
  static ObSchemaMgr sys_mgr_;
  static ObSchemaMgr tenant_mgr_;
  static ObSimpleMemLimitGetter mem_limit_;
  static decltype(GCTX.startup_mode_) old_startup_mode_;
  static bool old_liboblog_mode_;
  DDLQueryConnection connection_;
  DDLQueryPool pool_;
  ObMySQLProxy proxy_;
  ObMySQLProxy *old_proxy_;
  testing::StrictMock<TabletMergeStatusMock> merge_status_mock_;
  ObDDLTabletScheduler scheduler_;
};

ObSchemaMgr TestDDLTabletScheduler::sys_mgr_;
ObSchemaMgr TestDDLTabletScheduler::tenant_mgr_;
ObSimpleMemLimitGetter TestDDLTabletScheduler::mem_limit_;
decltype(GCTX.startup_mode_) TestDDLTabletScheduler::old_startup_mode_;
bool TestDDLTabletScheduler::old_liboblog_mode_ = false;

class TestDDLTabletSchedulerInit : public TestDDLTabletScheduler
{
public:
  void SetUp() override
  {
    old_root_service_ = GCTX.root_service_;
    old_location_service_ = GCTX.location_service_;
    old_ddl_service_started_ = ObDDLServiceLauncher::is_ddl_service_started();
    ASSERT_NO_FATAL_FAILURE(TestDDLTabletScheduler::SetUp());
    scheduler_.destroy();
    GCTX.root_service_ = &root_service_;
    GCTX.location_service_ = &location_service_;
    ATOMIC_SET(&ObDDLServiceLauncher::is_ddl_service_started_, true);
  }
  void TearDown() override
  {
    TestDDLTabletScheduler::TearDown();
    GCTX.root_service_ = old_root_service_;
    GCTX.location_service_ = old_location_service_;
    ATOMIC_SET(&ObDDLServiceLauncher::is_ddl_service_started_, old_ddl_service_started_);
  }
  int init_scheduler(const uint64_t table_id = DOC_WORD_TABLE_ID)
  {
    int ret = OB_SUCCESS;
    ObSchemaGetterGuard schema_guard;
    ObArray<ObTabletID> tablets;
    ObCurTraceId::TraceId trace_id;
    trace_id.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 2882));
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        TEST_TENANT_ID, schema_guard))) {
    } else if (OB_FAIL(ObDDLUtil::get_tablets(TEST_TENANT_ID, table_id, tablets))) {
    } else {
      ret = scheduler_.init(TEST_TENANT_ID, table_id, DATA_TABLE_ID, 12229406, 2, 1000,
          trace_id, tablets, DATA_VERSION_4_6_0_0, true, schema_guard);
    }
    return ret;
  }
  ObRootService root_service_;
  ObLocationService location_service_;
  ObRootService *old_root_service_;
  ObLocationService *old_location_service_;
  bool old_ddl_service_started_;
};

TEST_F(TestDDLTabletSchedulerInit, joint_fts_init_batches_candidates_and_preserves_running_tablets)
{
  connection_.running_sql_ = ObString::make_string("INSERT INTO t PARTITION(`p2`) SELECT 1");
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID,
      testing::Truly([](const ObIArray<ObTabletID> &ids) {
        return 2 == ids.count() && ObTabletID(FIRST_INVERTED_TABLET_ID) == ids.at(0)
            && ObTabletID(FIRST_INVERTED_TABLET_ID + 1) == ids.at(1);
      }), 1000, true, testing::_, testing::NotNull()))
      .WillOnce(testing::Invoke([](const uint64_t, const ObIArray<ObTabletID> &ids,
          const int64_t, const bool, bool &all_finished,
          hash::ObHashMap<ObTabletID, bool> *statuses) {
        int ret = OB_SUCCESS;
        all_finished = false;
        if (OB_FAIL(statuses->set_refactored(ids.at(0), true))) {
        } else if (OB_FAIL(statuses->set_refactored(ids.at(1), false))) {
        }
        return ret;
      }));
  ASSERT_EQ(OB_SUCCESS, init_scheduler());
  EXPECT_TRUE(scheduler_.is_inited_);
  ASSERT_EQ(2, scheduler_.all_tablets_.count());
  EXPECT_EQ(ObTabletID(FIRST_TABLET_ID + 1), scheduler_.all_tablets_.at(0));
  EXPECT_EQ(ObTabletID(FIRST_TABLET_ID + 2), scheduler_.all_tablets_.at(1));
  EXPECT_EQ(2, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(1, scheduler_.running_ls_to_tablets_map_.size());
  ASSERT_EQ(1, scheduler_.running_task_ls_ids_before_.count());
  EXPECT_EQ(ObLSID(1003), scheduler_.running_task_ls_ids_before_.at(0));
}

TEST_F(TestDDLTabletSchedulerInit, joint_fts_init_error_does_not_enqueue_partial_results)
{
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID,
      testing::Truly([](const ObIArray<ObTabletID> &ids) { return LS_COUNT == ids.count(); }),
      1000, true, testing::_, testing::NotNull()))
      .WillOnce(testing::Invoke([](const uint64_t, const ObIArray<ObTabletID> &ids,
          const int64_t, const bool, bool &, hash::ObHashMap<ObTabletID, bool> *statuses) {
        EXPECT_EQ(OB_SUCCESS, statuses->set_refactored(ids.at(0), true));
        return OB_TIMEOUT;
      }));
  EXPECT_EQ(OB_TIMEOUT, init_scheduler());
  EXPECT_FALSE(scheduler_.is_inited_);
  EXPECT_EQ(0, scheduler_.all_tablets_.count());
  EXPECT_EQ(0, scheduler_.running_task_ls_ids_before_.count());
  EXPECT_FALSE(scheduler_.all_ls_to_tablets_map_.created());
  EXPECT_FALSE(scheduler_.running_ls_to_tablets_map_.created());
}

TEST_F(TestDDLTabletSchedulerInit, non_fts_init_does_not_check_inverted_major)
{
  // StrictMock rejects any unexpected inverted major check.
  ASSERT_EQ(OB_SUCCESS, init_scheduler(INDEX_TABLE_ID));
  EXPECT_TRUE(scheduler_.is_all_tasks_finished());
  EXPECT_EQ(0, scheduler_.all_tablets_.count());
}

// Reading candidates repeatedly must not consume any recovered LS before confirmation.
TEST_F(TestDDLTabletScheduler, candidate_query_preserves_pending_ls)
{
  for (int64_t i = 0; i < LS_COUNT; ++i) {
    ASSERT_NO_FATAL_FAILURE(add_running_ls(i));
  }
  ObArray<ObLSID> running_now;
  ASSERT_EQ(OB_SUCCESS, running_now.push_back(ObLSID(1002)));
  for (int64_t poll = 0; poll < 2; ++poll) {
    ObArray<ObLSID> candidates;
    ASSERT_EQ(OB_SUCCESS, scheduler_.get_potential_finished_lsid(running_now, candidates));
    ASSERT_EQ(2, candidates.count());
    EXPECT_EQ(ObLSID(1001), candidates.at(0));
    EXPECT_EQ(ObLSID(1003), candidates.at(1));
    ASSERT_NO_FATAL_FAILURE(expect_pending(0, LS_COUNT));
  }
}

// A checksum timeout must retain the recovered LS and allow the next poll to finish it.
TEST_F(TestDDLTabletScheduler, checksum_timeout_then_success)
{
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  connection_.timeout_tablet_id_ = FIRST_TABLET_ID;
  ASSERT_EQ(OB_TIMEOUT, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 1));
  EXPECT_EQ(1, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(1, scheduler_.running_ls_to_tablets_map_.size());
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
  EXPECT_EQ(2, connection_.checksum_reads_.count());
  EXPECT_EQ(0, scheduler_.running_ls_to_tablets_map_.size());
  EXPECT_EQ(OB_ITER_END, next_batch());
}

// Failure at each LS position must preserve that LS and the entire unprocessed suffix.
TEST_F(TestDDLTabletScheduler, partial_confirmation_retains_failed_and_unprocessed_ls)
{
  for (int64_t failed = 0; failed < LS_COUNT; ++failed) {
    SCOPED_TRACE(failed);
    scheduler_.running_task_ls_ids_before_.reset();
    connection_.checksum_reads_.reset();
    for (int64_t i = 0; i < LS_COUNT; ++i) {
      ASSERT_NO_FATAL_FAILURE(add_running_ls(i));
    }
    connection_.timeout_tablet_id_ = FIRST_TABLET_ID + failed;
    ASSERT_EQ(OB_TIMEOUT, next_batch());
    ASSERT_NO_FATAL_FAILURE(expect_pending(failed, LS_COUNT - failed));
    EXPECT_EQ(failed + 1, connection_.checksum_reads_.count());
    EXPECT_EQ(LS_COUNT - failed, scheduler_.running_ls_to_tablets_map_.size());
    ASSERT_EQ(OB_EAGAIN, next_batch());
    ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
    ASSERT_EQ(OB_ITER_END, next_batch());
    ASSERT_EQ(OB_SUCCESS, scheduler_.running_ls_to_execution_id_.reuse());
  }
}

// Candidate indices differ from pending indices when an earlier LS still has running SQL.
TEST_F(TestDDLTabletScheduler, removes_ls_by_value)
{
  for (int64_t i = 0; i < LS_COUNT; ++i) {
    ASSERT_NO_FATAL_FAILURE(add_running_ls(i));
  }
  connection_.running_sql_ = ObString::make_string("INSERT INTO t PARTITION(`p0`, `p9`) SELECT 1");
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 1));
  EXPECT_EQ(1, scheduler_.running_ls_to_tablets_map_.size());
  EXPECT_EQ(2, connection_.checksum_reads_.count());
  connection_.running_sql_.reset();
  ASSERT_EQ(OB_EAGAIN, next_batch());
  EXPECT_EQ(OB_ITER_END, next_batch());
}

// Missing reports release running state and remove recovery markers so work can be rescheduled.
TEST_F(TestDDLTabletScheduler, missing_checksum_allows_rescheduling)
{
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  connection_.missing_tablet_id_ = FIRST_TABLET_ID;
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
  EXPECT_EQ(0, scheduler_.running_ls_to_tablets_map_.size());
  EXPECT_EQ(1, scheduler_.all_ls_to_tablets_map_.size());
  bool need_send = false;
  ASSERT_EQ(OB_SUCCESS, scheduler_.determine_if_need_to_send_new_task(need_send));
  EXPECT_TRUE(need_send);
}

// SQL is still running at takeover; after it exits, a missing inverted major requires
// rescheduling even when all checksums are present.
TEST_F(TestDDLTabletScheduler, joint_fts_missing_major_allows_rescheduling)
{
  scheduler_.table_id_ = DOC_WORD_TABLE_ID;
  scheduler_.need_single_partition_build_ = true;
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  connection_.running_sql_ = ObString::make_string("INSERT INTO t PARTITION(`p0`, `p9`) SELECT 1");
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 1));
  EXPECT_EQ(0, connection_.checksum_reads_.count());

  connection_.running_sql_.reset();
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID,
      testing::Truly([](const ObIArray<ObTabletID> &tablets) {
        return 1 == tablets.count() && ObTabletID(FIRST_INVERTED_TABLET_ID) == tablets.at(0);
      }), 1000, true, testing::_, nullptr))
      .WillOnce(testing::DoAll(testing::SetArgReferee<4>(false), testing::Return(OB_SUCCESS)));
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
  EXPECT_EQ(1, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(0, scheduler_.running_ls_to_tablets_map_.size());
  bool need_send = false;
  ASSERT_EQ(OB_SUCCESS, scheduler_.determine_if_need_to_send_new_task(need_send));
  EXPECT_TRUE(need_send);
}

// A major check error must preserve recovery markers; recheck before confirming completion.
TEST_F(TestDDLTabletScheduler, joint_fts_merge_error_preserves_pending_ls)
{
  scheduler_.table_id_ = DOC_WORD_TABLE_ID;
  scheduler_.need_single_partition_build_ = true;
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID, testing::_, 1000, true, testing::_, nullptr))
      .WillOnce(testing::Return(OB_TIMEOUT))
      .WillOnce(testing::DoAll(testing::SetArgReferee<4>(true), testing::Return(OB_SUCCESS)));
  ASSERT_EQ(OB_TIMEOUT, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 1));
  EXPECT_EQ(1, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(1, scheduler_.running_ls_to_tablets_map_.size());
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
  EXPECT_EQ(OB_ITER_END, next_batch());
}

TEST_F(TestDDLTabletScheduler, joint_fts_batch_preserves_individual_status)
{
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      TEST_TENANT_ID, schema_guard));
  ObArray<ObTabletID> tablets;
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID + 2)));
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID)));
  hash::ObHashMap<uint64_t, bool> finished_map;
  ASSERT_EQ(OB_SUCCESS, finished_map.create(LS_COUNT, "TestFtsStatus"));
  for (int64_t i = 0; i < LS_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, finished_map.set_refactored(FIRST_TABLET_ID + i, true));
  }
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID,
      testing::Truly([](const ObIArray<ObTabletID> &ids) {
        return 2 == ids.count() && ObTabletID(FIRST_INVERTED_TABLET_ID + 2) == ids.at(0)
            && ObTabletID(FIRST_INVERTED_TABLET_ID) == ids.at(1);
      }), 1000, true, testing::_, testing::NotNull()))
      .WillOnce(testing::Invoke([](const uint64_t, const ObIArray<ObTabletID> &ids,
          const int64_t, const bool, bool &all_finished,
          hash::ObHashMap<ObTabletID, bool> *statuses) {
        int ret = OB_SUCCESS;
        all_finished = false;
        if (OB_FAIL(statuses->set_refactored(ids.at(0), true))) {
        } else if (OB_FAIL(statuses->set_refactored(ids.at(1), false))) {
        }
        return ret;
      }));
  bool all_finished = true;
  ASSERT_EQ(OB_SUCCESS, ObFtsIndexBuilderUtil::check_inverted_tablet_merge_status(
      TEST_TENANT_ID, schema_guard, DOC_WORD_TABLE_ID, tablets, 1000, all_finished, &finished_map));
  EXPECT_FALSE(all_finished);
  for (int64_t i = 0; i < LS_COUNT; ++i) {
    bool finished = false;
    ASSERT_EQ(OB_SUCCESS, finished_map.get_refactored(FIRST_TABLET_ID + i, finished));
    EXPECT_EQ(i != 0, finished);
  }
}

TEST_F(TestDDLTabletScheduler, joint_fts_batch_error_does_not_update_finished_status)
{
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      TEST_TENANT_ID, schema_guard));
  ObArray<ObTabletID> tablets;
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID)));
  hash::ObHashMap<uint64_t, bool> finished_map;
  ASSERT_EQ(OB_SUCCESS, finished_map.create(1, "TestFtsStatus"));
  ASSERT_EQ(OB_SUCCESS, finished_map.set_refactored(FIRST_TABLET_ID, true));
  EXPECT_CALL(merge_status_mock_, check(TEST_TENANT_ID, testing::_, 1000, true,
      testing::_, testing::NotNull()))
      .WillOnce(testing::Invoke([](const uint64_t, const ObIArray<ObTabletID> &ids,
          const int64_t, const bool, bool &, hash::ObHashMap<ObTabletID, bool> *statuses) {
        EXPECT_EQ(OB_SUCCESS, statuses->set_refactored(ids.at(0), false));
        return OB_TIMEOUT;
      }));
  bool all_finished = false;
  ASSERT_EQ(OB_TIMEOUT, ObFtsIndexBuilderUtil::check_inverted_tablet_merge_status(
      TEST_TENANT_ID, schema_guard, DOC_WORD_TABLE_ID, tablets, 1000, all_finished, &finished_map));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS, finished_map.get_refactored(FIRST_TABLET_ID, finished));
  EXPECT_TRUE(finished);
}

// A rescheduled LS must lose its recovery marker even when a later LS fails confirmation.
TEST_F(TestDDLTabletScheduler, partial_failure_does_not_reconfirm_new_batch)
{
  for (int64_t i = 0; i < LS_COUNT; ++i) {
    ASSERT_NO_FATAL_FAILURE(add_running_ls(i));
  }
  connection_.missing_tablet_id_ = FIRST_TABLET_ID;
  connection_.timeout_tablet_id_ = FIRST_TABLET_ID + 1;
  ASSERT_EQ(OB_TIMEOUT, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(1, 2));
  EXPECT_EQ(LS_COUNT, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(2, scheduler_.running_ls_to_tablets_map_.size());
  ASSERT_EQ(2, connection_.checksum_reads_.count());

  // Model a newly admitted batch whose SQL has not started yet; only the old LSs need checking.
  const ObLSID rescheduled_ls_id(1001);
  ObArray<ObTabletID> tablets;
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(FIRST_TABLET_ID)));
  ASSERT_EQ(OB_SUCCESS,
            scheduler_.running_ls_to_tablets_map_.set_refactored(rescheduled_ls_id, tablets));
  ASSERT_EQ(OB_SUCCESS,
            scheduler_.running_ls_to_execution_id_.set_refactored(rescheduled_ls_id, 2, true));
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 0));
  EXPECT_EQ(1, scheduler_.all_ls_to_tablets_map_.size());
  EXPECT_EQ(1, scheduler_.running_ls_to_tablets_map_.size());
  ASSERT_EQ(4, connection_.checksum_reads_.count());
  EXPECT_EQ(FIRST_TABLET_ID + 1, connection_.checksum_reads_.at(2));
  EXPECT_EQ(FIRST_TABLET_ID + 2, connection_.checksum_reads_.at(3));
  ASSERT_EQ(OB_SUCCESS,
            scheduler_.confirm_batch_tablets_status(2, true, rescheduled_ls_id, tablets));
  EXPECT_EQ(OB_ITER_END, next_batch());
}

// An LS completed by the normal callback still needs its recovery marker removed.
TEST_F(TestDDLTabletScheduler, callback_before_confirmation)
{
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  ASSERT_NO_FATAL_FAILURE(add_running_ls(1));
  ASSERT_NO_FATAL_FAILURE(complete_callback(0));
  connection_.running_sql_ = ObString::make_string("INSERT INTO t PARTITION(`p1`, `p9`) SELECT 1");
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(1, 1));
  EXPECT_EQ(0, connection_.checksum_reads_.count());
}

// A callback during the checksum query can make confirmation fail; the next poll must clean up.
TEST_F(TestDDLTabletScheduler, callback_during_checksum_query)
{
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  ASSERT_NO_FATAL_FAILURE(add_running_ls(1));
  connection_.running_sql_ = ObString::make_string("INSERT INTO t PARTITION(`p1`, `p9`) SELECT 1");
  connection_.callback_tablet_id_ = FIRST_TABLET_ID;
  ASSERT_EQ(OB_HASH_NOT_EXIST, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 2));
  ASSERT_EQ(OB_EAGAIN, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(1, 1));
  EXPECT_EQ(1, connection_.checksum_reads_.count());
}

// Failure before candidate calculation must leave recovery state unchanged and retryable.
TEST_F(TestDDLTabletScheduler, session_query_failure_preserves_pending_ls)
{
  ASSERT_NO_FATAL_FAILURE(add_running_ls(0));
  connection_.session_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, next_batch());
  ASSERT_NO_FATAL_FAILURE(expect_pending(0, 1));
  EXPECT_EQ(0, connection_.checksum_reads_.count());
  connection_.session_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_EAGAIN, next_batch());
  EXPECT_EQ(OB_ITER_END, next_batch());
}

class DDLMessageResult : public ObMySQLResultImpl
{
public:
  DDLMessageResult(ObMySQLStatement &statement, const ObString &message,
                   const int64_t execution_id)
    : ObMySQLResultImpl(statement), message_(message), execution_id_(execution_id), pos_(-1) {}
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_uint;
  using ObMySQLResultImpl::get_varchar;
  int next() override { return ++pos_ == 0 ? OB_SUCCESS : OB_ITER_END; }
  int get_int(const char *column, int64_t &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == strcmp(column, "execution_id")) {
      value = execution_id_;
    } else if (0 == strcmp(column, "status")) {
      value = REDEFINITION;
    } else if (0 == strcmp(column, "ret_code")) {
      value = OB_SUCCESS;
    } else if (0 == strcmp(column, "free_size")) {
      value = 1L << 40;
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    EXPECT_STREQ("snapshot_version", column);
    value = 1000;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    EXPECT_STREQ("message_unhex", column);
    value = message_;
    return OB_SUCCESS;
  }
private:
  ObString message_;
  int64_t execution_id_;
  int64_t pos_;
};

class DDLMessageResultHandler : public ObISQLResultHandler
{
public:
  DDLMessageResultHandler(const ObString &message, const int64_t execution_id)
    : result_(statement_, message, execution_id) {}
  ObMySQLResult *mysql_result() override { return &result_; }
private:
  ObMySQLStatement statement_;
  DDLMessageResult result_;
};

class DDLMessageConnection : public DDLQueryConnection
{
public:
  DDLMessageConnection()
    : execution_id_(0), staged_execution_id_(0), message_writes_(0), message_commits_(0), rollbacks_(0),
      fail_message_write_(false), fail_commit_(false), fail_rollback_(false),
      message_transaction_(false) {}
  using DDLQueryConnection::execute_read;
  using DDLQueryConnection::execute_write;
  int execute_read(const uint64_t tenant_id, const ObString &sql,
                   ObISQLClient::ReadResult &result, bool is_user_sql,
                   const ObAddr *sql_exec_addr) override
  {
    int ret = OB_SUCCESS;
    const std::string text(sql.ptr(), sql.length());
    if (text.find("__all_ddl_task_status") != std::string::npos
        || text.find("__all_virtual_disk_stat") != std::string::npos) {
      ObString message(static_cast<int32_t>(message_.size()), message_.data());
      DDLMessageResultHandler *handler = nullptr;
      ret = result.create_handler(handler, message, execution_id_);
    } else {
      ret = DDLQueryConnection::execute_read(tenant_id, sql, result, is_user_sql, sql_exec_addr);
    }
    return ret;
  }
  int execute_write(const uint64_t tenant_id, const ObString &sql, int64_t &affected_rows,
                    bool is_user_sql, const ObAddr *sql_exec_addr) override
  {
    int ret = OB_SUCCESS;
    EXPECT_EQ(TEST_TENANT_ID, tenant_id);
    const std::string text(sql.ptr(), sql.length());
    const size_t execution_pos = text.find("SET execution_id=");
    const size_t message_pos = text.find("SET message=\"");
    affected_rows = 0;
    if (execution_pos != std::string::npos) {
      if (1 != sscanf(text.c_str() + execution_pos, "SET execution_id=%ld",
                      &staged_execution_id_)) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (message_pos != std::string::npos) {
      ++message_writes_;
      message_transaction_ = true;
      if (fail_message_write_) {
        fail_message_write_ = false;
        ret = OB_TRANS_TIMEOUT;
      } else {
        const size_t begin = message_pos + strlen("SET message=\"");
        const size_t end = text.find('"', begin);
        if (end == std::string::npos || 0 != (end - begin) % 2) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          staged_message_.resize((end - begin) / 2 + 1);
          int64_t pos = 0;
          ret = hex_to_cstr(text.data() + begin, end - begin, &staged_message_[0],
                            staged_message_.size(), pos);
          staged_message_.resize((end - begin) / 2);
        }
      }
    } else {
      ADD_FAILURE() << "Unexpected write: " << text;
      ret = OB_NOT_SUPPORTED;
    }
    if (OB_SUCC(ret)) {
      affected_rows = 1;
    }
    return ret;
  }
  int start_transaction(const uint64_t &tenant_id, bool with_snapshot) override
  {
    EXPECT_EQ(TEST_TENANT_ID, tenant_id);
    staged_execution_id_ = execution_id_;
    staged_message_ = message_;
    message_transaction_ = false;
    return OB_SUCCESS;
  }
  int commit() override
  {
    message_commits_ += message_transaction_ ? 1 : 0;
    int callback_ret = OB_SUCCESS;
    if (fail_commit_ && message_transaction_) {
      fail_commit_ = false;
      callback_ret = OB_TIMEOUT;
    } else {
      execution_id_ = staged_execution_id_;
      message_ = staged_message_;
    }
    return callback_ret;
  }
  int rollback() override
  {
    ++rollbacks_;
    staged_execution_id_ = execution_id_;
    staged_message_ = message_;
    int callback_ret = OB_SUCCESS;
    if (fail_rollback_) {
      fail_rollback_ = false;
      callback_ret = OB_EAGAIN;
    }
    return callback_ret;
  }
  std::string message_;
  std::string staged_message_;
  int64_t execution_id_;
  int64_t staged_execution_id_;
  int64_t message_writes_;
  int64_t message_commits_;
  int64_t rollbacks_;
  bool fail_message_write_;
  bool fail_commit_;
  bool fail_rollback_;
  bool message_transaction_;
};

// Replace only the asynchronous queue boundary; all scheduling and submission code stays real.
class DDLSubmissionGroup : public lib::ITG
{
public:
  DDLSubmissionGroup() : calls_(0), accepted_(0), fail_next_(false), execution_id_(-1) {}
  int thread_cnt() override { return 0; }
  int set_thread_cnt(int64_t) override { return OB_NOT_SUPPORTED; }
  int start() override { return OB_SUCCESS; }
  void stop() override {}
  void wait() override {}
  int push_task(ObAsyncTask &async_task) override
  {
    int callback_ret = OB_SUCCESS;
    ++calls_;
    ObIndexSSTableBuildTask &task = static_cast<ObIndexSSTableBuildTask &>(async_task);
    EXPECT_EQ(TEST_TENANT_ID, task.tenant_id_);
    EXPECT_TRUE(task.is_partitioned_local_index_task_);
    EXPECT_EQ(ObLSID(1001), task.addition_info_.ls_id_);
    if (fail_next_) {
      fail_next_ = false;
      callback_ret = OB_EAGAIN;
    } else if (OB_SUCCESS != (callback_ret = tablets_.assign(task.addition_info_.partition_ids_))) {
    } else {
      execution_id_ = task.execution_id_;
      ++accepted_;
    }
    return callback_ret;
  }
  int64_t calls_;
  int64_t accepted_;
  bool fail_next_;
  int64_t execution_id_;
  ObArray<ObTabletID> tablets_;
};

class TestDDLIndexMessageFailure : public TestDDLTabletScheduler
{
public:
  TestDDLIndexMessageFailure()
    : message_pool_(message_connection_), old_location_service_(nullptr),
      sys_tenant_(OB_SYS_TENANT_ID, 0, 1, cgroup_ctrl_), old_tenant_(nullptr),
      old_omt_(nullptr), old_service_started_(false), submission_tg_id_(-1) {}
  void SetUp() override
  {
    old_location_service_ = GCTX.location_service_;
    old_tenant_ = ObTenantEnv::get_tenant();
    old_omt_ = GCTX.omt_;
    old_service_started_ = ObDDLServiceLauncher::is_ddl_service_started_;
    TestDDLTabletScheduler::SetUp();
    // Keep a valid tenant schema snapshot for initial submission and retry.
    ASSERT_EQ(OB_SUCCESS, ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        TEST_TENANT_ID, schema_guard_, OB_INVALID_VERSION, OB_INVALID_VERSION,
        ObMultiVersionSchemaService::RefreshSchemaMode::NORMAL));
    ASSERT_EQ(OB_SUCCESS, message_proxy_.init(&message_pool_));
    GCTX.sql_proxy_ = &message_proxy_;
    task_.tenant_id_ = TEST_TENANT_ID;
    task_.task_id_ = 1602690;
    task_.task_type_ = DDL_CREATE_PARTITIONED_LOCAL_INDEX;
    task_.task_status_ = REDEFINITION;
    task_.task_version_ = ObIndexBuildTask::OB_INDEX_BUILD_TASK_VERSION;
    task_.data_format_version_ = DATA_VERSION_4_3_3_0;
    task_.is_inited_ = true;
    task_.is_retryable_ddl_ = true;
    task_.root_service_ = &root_service_;
    task_.create_index_arg_.tenant_id_ = TEST_TENANT_ID;
    task_.create_index_arg_.index_schema_.set_tenant_id(TEST_TENANT_ID);
    task_.create_index_arg_.is_partition_local_ddl_ = true;
    ObDDLTabletScheduler &scheduler = task_.tablet_scheduler_;
    scheduler.tenant_id_ = TEST_TENANT_ID;
    scheduler.task_id_ = task_.task_id_;
    scheduler.table_id_ = INDEX_TABLE_ID;
    scheduler.ref_data_table_id_ = DATA_TABLE_ID;
    scheduler.parallelism_ = 2;
    scheduler.snapshot_version_ = 1000;
    scheduler.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, scheduler.all_ls_to_tablets_map_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.running_ls_to_tablets_map_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.running_ls_to_execution_id_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.ls_location_map_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.tablet_id_to_data_size_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.tablet_id_to_data_row_cnt_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.tablet_scheduled_times_statistic_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    ASSERT_EQ(OB_SUCCESS, scheduler.tablet_id_to_execution_id_map_.create(8, "TestDDLSched",
        ObModIds::OB_HASH_NODE, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID));
    for (int64_t i = 0; i < LS_COUNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, tablets_.push_back(ObTabletID(FIRST_TABLET_ID + i)));
      ASSERT_EQ(OB_SUCCESS,
          scheduler.tablet_id_to_data_size_.set_refactored(FIRST_TABLET_ID + i + 100, 1,
              0, 0, 0, static_cast<void *>(nullptr)));
      ASSERT_EQ(OB_SUCCESS,
          scheduler.tablet_id_to_data_row_cnt_.set_refactored(FIRST_TABLET_ID + i + 100, 1,
              0, 0, 0, static_cast<void *>(nullptr)));
    }
    ASSERT_EQ(OB_SUCCESS, scheduler.all_ls_to_tablets_map_.set_refactored(
        ObLSID(1001), tablets_, 0, 0, 0, static_cast<void *>(nullptr)));
    const ObAddr leader(ObAddr::IPV4, "127.0.0.1", 2882);
    ASSERT_EQ(OB_SUCCESS, scheduler.ls_location_map_.set_refactored(
        ObLSID(1001), leader, 0, 0, 0, static_cast<void *>(nullptr)));
    ASSERT_NO_FATAL_FAILURE(init_location_cache(leader));
    ASSERT_NO_FATAL_FAILURE(save_message());
    ASSERT_NO_FATAL_FAILURE(init_submission_group());
  }
  void TearDown() override
  {
    // Detach the stack-owned queue before the builder and tenant release their resources.
    if (submission_tg_id_ >= 0) {
      TG_MGR.tgs_[submission_tg_id_] = nullptr;
      TG_MGR.free_tg_id(submission_tg_id_);
      ddl_scheduler_.ddl_builder_.tg_id_ = -1;
    }
    root_service_.inited_ = false;
    omt_.tenants_.clear();
    sys_tenant_.set(static_cast<ObDDLScheduler *>(nullptr));
    ObTenantEnv::set_tenant(old_tenant_);
    GCTX.omt_ = old_omt_;
    ObDDLServiceLauncher::is_ddl_service_started_ = old_service_started_;
    GCTX.location_service_ = old_location_service_;
    TestDDLTabletScheduler::TearDown();
  }
  void init_submission_group()
  {
    // Use the real RootService, SYS-tenant dispatch and builder, without worker threads or SQL RPC.
    ASSERT_EQ(OB_SUCCESS, omt_.tenants_.push_back(&sys_tenant_));
    sys_tenant_.set(&ddl_scheduler_);
    ObTenantEnv::set_tenant(&sys_tenant_);
    GCTX.omt_ = &omt_;
    ObDDLServiceLauncher::is_ddl_service_started_ = true;
    root_service_.inited_ = true;
    submission_tg_id_ = TG_MGR.alloc_tg_id(lib::TGDefIDs::END);
    ASSERT_GE(submission_tg_id_, 0);
    TG_MGR.tgs_[submission_tg_id_] = &submission_group_;
    ddl_scheduler_.ddl_builder_.tg_id_ = submission_tg_id_;
    ddl_scheduler_.ddl_builder_.is_thread_started_ = true;
    ddl_scheduler_.ddl_builder_.is_stopped_ = false;
  }
  void init_location_cache(const ObAddr &leader)
  {
    ObLSLocationService &service = location_service_.ls_location_service_;
    ASSERT_EQ(OB_SUCCESS, service.inner_cache_.init());
    service.lst_ = &ls_table_;
    service.schema_service_ = &ObMultiVersionSchemaService::get_instance();
    service.rs_mgr_ = &rs_mgr_;
    service.srv_rpc_proxy_ = &rpc_proxy_;
    service.inited_ = true;
    location_service_.inited_ = true;
    const int64_t ls_ids[] = {ObLSID::SYS_LS_ID, 1001};
    for (const int64_t ls_id : ls_ids) {
      ObLSLocation location;
      ObLSReplicaLocation replica;
      replica.set_server(leader);
      replica.set_role(LEADER);
      replica.set_sql_port(2881);
      replica.set_replica_type(REPLICA_TYPE_FULL);
      replica.set_proposal_id(1);
      ASSERT_EQ(OB_SUCCESS, location.init(GCONF.cluster_id, TEST_TENANT_ID, ObLSID(ls_id),
          ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, location.add_replica_location(replica));
      ASSERT_EQ(OB_SUCCESS, service.inner_cache_.update(false, location.get_cache_key(), location));
    }
    GCTX.location_service_ = &location_service_;
  }
  void save_message()
  {
    message_connection_.message_.resize(task_.get_serialize_param_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, task_.serialize_params_to_message(&message_connection_.message_[0],
        message_connection_.message_.size(), pos));
    message_connection_.message_.resize(pos);
  }
  void expect_retry_candidate()
  {
    ObDDLTabletScheduler &scheduler = task_.tablet_scheduler_;
    EXPECT_EQ(0, scheduler.running_ls_to_tablets_map_.size());
    EXPECT_EQ(1, scheduler.all_ls_to_tablets_map_.size());
    EXPECT_EQ(0, scheduler.running_task_ls_ids_before_.count());
    bool need_send = false;
    ASSERT_EQ(OB_SUCCESS, scheduler.determine_if_need_to_send_new_task(need_send));
    ASSERT_TRUE(need_send);
    ObArray<ObTabletID> pending;
    ObLSID ls_id;
    ObAddr leader;
    ASSERT_EQ(OB_SUCCESS, scheduler.get_to_be_scheduled_tablets(ls_id, leader, pending));
    EXPECT_EQ(ObLSID(1001), ls_id);
    ASSERT_EQ(tablets_.count(), pending.count());
    for (int64_t i = 0; i < tablets_.count(); ++i) {
      EXPECT_EQ(tablets_.at(i), pending.at(i));
    }
  }
  void expect_submitted_batch()
  {
    ObDDLTabletScheduler &scheduler = task_.tablet_scheduler_;
    EXPECT_TRUE(task_.is_sstable_complete_task_submitted_);
    EXPECT_EQ(1, submission_group_.accepted_);
    ObArray<ObTabletID> running;
    ASSERT_EQ(OB_SUCCESS,
        scheduler.running_ls_to_tablets_map_.get_refactored(ObLSID(1001), running, 0));
    ASSERT_EQ(tablets_.count(), running.count());
    ASSERT_EQ(tablets_.count(), submission_group_.tablets_.count());
    for (int64_t i = 0; i < tablets_.count(); ++i) {
      EXPECT_EQ(tablets_.at(i), running.at(i));
      EXPECT_EQ(tablets_.at(i), submission_group_.tablets_.at(i));
    }
    EXPECT_EQ(1, scheduler.all_ls_to_tablets_map_.size());
    int64_t execution_id = -1;
    ASSERT_EQ(OB_SUCCESS,
        scheduler.running_ls_to_execution_id_.get_refactored(ObLSID(1001), execution_id, 0));
    EXPECT_EQ(execution_id, submission_group_.execution_id_);
  }
  void retry_submission()
  {
    bool finished = false;
    ASSERT_EQ(OB_SUCCESS,
        task_.wait_and_send_single_partition_replica_task(finished, schema_guard_));
    EXPECT_FALSE(finished);
    ASSERT_NO_FATAL_FAILURE(expect_submitted_batch());
  }
  ObSchemaGetterGuard schema_guard_;
  ObIndexBuildTask task_;
  ObArray<ObTabletID> tablets_;
  DDLMessageConnection message_connection_;
  DDLQueryPool message_pool_;
  ObMySQLProxy message_proxy_;
  ObLocationService location_service_;
  ObLocationService *old_location_service_;
  ObLSTableOperator ls_table_;
  ObRsMgr rs_mgr_;
  obrpc::ObSrvRpcProxy rpc_proxy_;
  ObCgroupCtrl cgroup_ctrl_;
  omt::ObTenant sys_tenant_;
  omt::ObMultiTenant omt_;
  ObDDLScheduler ddl_scheduler_;
  ObRootService root_service_;
  DDLSubmissionGroup submission_group_;
  ObTenantBase *old_tenant_;
  omt::ObMultiTenant *old_omt_;
  bool old_service_started_;
  int submission_tg_id_;
};

// A real message write failure must preserve the error and all pending tablets before retry.
TEST_F(TestDDLIndexMessageFailure, message_write_failure_releases_batch_and_retries)
{
  message_connection_.fail_message_write_ = true;
  const std::string old_message = message_connection_.message_;
  bool finished = false;
  ASSERT_EQ(OB_TRANS_TIMEOUT,
      task_.wait_and_send_single_partition_replica_task(finished, schema_guard_));
  EXPECT_FALSE(finished);
  EXPECT_FALSE(task_.is_sstable_complete_task_submitted_);
  EXPECT_EQ(0, submission_group_.calls_);
  EXPECT_EQ(1, message_connection_.message_writes_);
  EXPECT_EQ(1, message_connection_.rollbacks_);
  EXPECT_EQ(old_message, message_connection_.message_);
  ASSERT_NO_FATAL_FAILURE(expect_retry_candidate());
  ASSERT_NO_FATAL_FAILURE(retry_submission());
  EXPECT_EQ(2, message_connection_.message_writes_);
}

// COMMIT failure reaches the same cleanup even when message UPDATE itself succeeded.
TEST_F(TestDDLIndexMessageFailure, message_commit_failure_releases_batch_and_retries)
{
  message_connection_.fail_commit_ = true;
  bool finished = false;
  ASSERT_EQ(OB_TIMEOUT, task_.wait_and_send_single_partition_replica_task(finished, schema_guard_));
  EXPECT_FALSE(finished);
  EXPECT_FALSE(task_.is_sstable_complete_task_submitted_);
  EXPECT_EQ(1, message_connection_.message_writes_);
  EXPECT_EQ(1, message_connection_.message_commits_);
  EXPECT_EQ(0, submission_group_.calls_);
  ASSERT_NO_FATAL_FAILURE(expect_retry_candidate());
  ASSERT_NO_FATAL_FAILURE(retry_submission());
}

// A failed ROLLBACK must not replace the original message error or strand running tablets.
TEST_F(TestDDLIndexMessageFailure, rollback_failure_preserves_message_error)
{
  message_connection_.fail_message_write_ = true;
  message_connection_.fail_rollback_ = true;
  bool finished = false;
  ASSERT_EQ(OB_TRANS_TIMEOUT,
      task_.wait_and_send_single_partition_replica_task(finished, schema_guard_));
  EXPECT_FALSE(finished);
  EXPECT_FALSE(task_.is_sstable_complete_task_submitted_);
  EXPECT_EQ(1, message_connection_.rollbacks_);
  EXPECT_EQ(0, submission_group_.calls_);
  ASSERT_NO_FATAL_FAILURE(expect_retry_candidate());
  ASSERT_NO_FATAL_FAILURE(retry_submission());
}

// A retryable rejection at the real builder's queue boundary releases the unsubmitted batch.
TEST_F(TestDDLIndexMessageFailure, submission_eagain_releases_batch_and_retries)
{
  submission_group_.fail_next_ = true;
  EXPECT_TRUE(task_.is_error_need_retry(OB_EAGAIN));
  bool finished = false;
  ASSERT_EQ(OB_EAGAIN, task_.wait_and_send_single_partition_replica_task(finished, schema_guard_));
  EXPECT_FALSE(finished);
  EXPECT_FALSE(task_.is_sstable_complete_task_submitted_);
  EXPECT_EQ(1, message_connection_.message_commits_);
  EXPECT_EQ(1, submission_group_.calls_);
  EXPECT_EQ(0, submission_group_.accepted_);
  ASSERT_NO_FATAL_FAILURE(expect_retry_candidate());
  ASSERT_NO_FATAL_FAILURE(retry_submission());
  EXPECT_EQ(2, submission_group_.calls_);
}

// Successful queue acceptance retains running ownership until the completion callback.
TEST_F(TestDDLIndexMessageFailure, successful_submission_retains_running)
{
  ASSERT_NO_FATAL_FAILURE(retry_submission());
  EXPECT_EQ(1, submission_group_.calls_);
  EXPECT_EQ(1, message_connection_.message_commits_);
  ObDDLTabletScheduler &scheduler = task_.tablet_scheduler_;
  ASSERT_EQ(OB_SUCCESS, scheduler.confirm_batch_tablets_status(
      submission_group_.execution_id_, true, ObLSID(1001), tablets_));
  EXPECT_EQ(0, scheduler.running_ls_to_tablets_map_.size());
  EXPECT_EQ(0, scheduler.all_ls_to_tablets_map_.size());
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_ddl_tablet_scheduler.log", true);
  // Keep process-wide singletons alive across gtest repeat iterations.
  oceanbase::rootserver::TestDDLTabletScheduler::init_schema_context();
  int ret = 1;
  if (!testing::Test::HasFatalFailure()) {
    ret = RUN_ALL_TESTS();
  }
  oceanbase::rootserver::TestDDLTabletScheduler::destroy_schema_context();
  return ret;
}

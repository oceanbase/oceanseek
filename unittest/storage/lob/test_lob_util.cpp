/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>

#define protected public
#define private public

#include "lib/worker.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_server_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/schema/mock_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "storage/lob/access/ob_lob_retry.h"
#include "storage/lob/common/ob_lob_location.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/ob_lob_handler.h"
#include "storage/lob/store/ob_lob_persistent_adaptor.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobUtil : public ::testing::Test
{
public:
  TestLobUtil() {}
  virtual ~TestLobUtil() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

//==================== adjust_range ====================
TEST_F(TestLobUtil, adjust_range_in_range)
{
  uint64_t offset = 10;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(10, offset);
  ASSERT_EQ(50, len);
}

TEST_F(TestLobUtil, adjust_range_offset_overflow)
{
  uint64_t offset = 200;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(100, offset);
  ASSERT_EQ(0, len);
}

TEST_F(TestLobUtil, adjust_range_len_overflow)
{
  uint64_t offset = 80;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(80, offset);
  ASSERT_EQ(20, len); // clamped to length - offset
}

//==================== convert_char_range_to_byte_range ====================
TEST_F(TestLobUtil, convert_range_ascii)
{
  const char *data = "abcdefghij"; // 10 ascii chars
  uint64_t char_offset = 2;
  uint64_t char_length = 3;
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_BIN, data, 10,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(2, byte_offset);
  ASSERT_EQ(3, byte_length);
}

TEST_F(TestLobUtil, convert_range_multibyte)
{
  // five "中", each 3 bytes in utf8mb4 -> 15 bytes
  const char *data = "\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad";
  uint64_t char_offset = 1;
  uint64_t char_length = 2;
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_GENERAL_CI, data, 15,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(3, byte_offset);  // skip 1 char = 3 bytes
  ASSERT_EQ(6, byte_length);  // 2 chars = 6 bytes
}

TEST_F(TestLobUtil, convert_range_clamp)
{
  const char *data = "abcdefghij"; // 10 chars
  uint64_t char_offset = 8;
  uint64_t char_length = 100; // beyond remaining chars
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_BIN, data, 10,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(8, byte_offset);
  ASSERT_EQ(2, byte_length); // clamped to remaining 2 chars
}

//==================== ObLobCharsetUtil::get_collation_type ====================
TEST_F(TestLobUtil, get_collation_type_json_to_binary)
{
  ASSERT_EQ(CS_TYPE_BINARY,
            ObLobCharsetUtil::get_collation_type(ObJsonType, CS_TYPE_UTF8MB4_BIN));
}

TEST_F(TestLobUtil, get_collation_type_non_json_keep)
{
  ASSERT_EQ(CS_TYPE_UTF8MB4_BIN,
            ObLobCharsetUtil::get_collation_type(ObVarcharType, CS_TYPE_UTF8MB4_BIN));
}

TEST_F(TestLobUtil, tenant_not_in_server_can_retry)
{
  ASSERT_TRUE(ObLobRetryUtil::is_remote_ret_can_retry(OB_TENANT_NOT_IN_SERVER));
  ASSERT_FALSE(ObLobRetryUtil::is_remote_ret_can_retry(OB_TENANT_HAS_BEEN_DROPPED));
}

class LobRetryMockSchemaService : public share::schema::MockSchemaService
{
public:
  LobRetryMockSchemaService() : sql_impl_(), mysql_proxy_() { reset(); }
  ~LobRetryMockSchemaService() override { reset(); }
  void reset()
  {
    init_ = false;
    schema_service_ = nullptr;
    sql_proxy_ = nullptr;
    config_ = nullptr;
  }
  int init()
  {
    int ret = share::schema::MockSchemaService::init();
    if (OB_SUCC(ret)) {
      schema_service_ = &sql_impl_;
      sql_proxy_ = &mysql_proxy_;
      config_ = &GCONF;
      init_ = true;
    }
    return ret;
  }
  int get_tenant_refreshed_schema_version(
      const uint64_t tenant_id, int64_t &schema_version, const bool core_schema_version = false) const override
  {
    UNUSED(tenant_id);
    UNUSED(core_schema_version);
    schema_version = OB_CORE_SCHEMA_VERSION + 1;
    return OB_SUCCESS;
  }

private:
  share::schema::ObSchemaServiceSQLImpl sql_impl_;
  ObMySQLProxy mysql_proxy_;
};

class FailingLobQueryHandler : public ObLobQueryBaseHandler
{
public:
  explicit FailingLobQueryHandler(ObLobReadContext &read_ctx)
    : ObLobQueryBaseHandler(read_ctx), execute_count_(0)
  {
    reset();
  }
  ~FailingLobQueryHandler() { reset(); }
  void reset() { execute_count_ = 0; }
  int64_t get_execute_count() const { return execute_count_; }

protected:
  int do_execute() override
  {
    ++execute_count_;
    return OB_LS_OFFLINE;
  }

private:
  int64_t execute_count_;
};

class RetryingLobScanHandler : public ObLobQueryBaseHandler
{
public:
  RetryingLobScanHandler(ObLobReadContext &read_ctx, ObPersistentLobAdaptor &adaptor)
    : ObLobQueryBaseHandler(read_ctx), adaptor_(adaptor), execute_count_(0), arena_was_cleared_(true)
  {
    reset();
  }
  ~RetryingLobScanHandler() { reset(); }
  void reset()
  {
    execute_count_ = 0;
    arena_was_cleared_ = true;
  }
  int64_t get_execute_count() const { return execute_count_; }
  bool arena_was_cleared() const { return arena_was_cleared_; }

protected:
  int do_execute() override
  {
    arena_was_cleared_ = arena_was_cleared_ && read_ctx_.iter_allocator_.used() == 0;
    if (++execute_count_ == 4) {
      return OB_SUCCESS;
    }
    ObLobMetaTableIter *iter = nullptr;
    const int ret = adaptor_.scan_lob_meta(read_ctx_, iter);
    // Inject a retryable error after a real failed open without requiring a live tablet.
    return OB_INVALID_ARGUMENT == ret && nullptr == iter ? OB_REPLICA_NOT_READABLE : OB_ERR_UNEXPECTED;
  }

private:
  ObPersistentLobAdaptor &adaptor_;
  int64_t execute_count_;
  bool arena_was_cleared_;
};

class InvalidLobRetryWorker : public lib::Worker
{
public:
  InvalidLobRetryWorker() : check_wait_count_(0) { reset(); }
  ~InvalidLobRetryWorker() override { reset(); }
  void reset()
  {
    set_is_th_worker(false);
    check_wait_count_ = 0;
  }
  Status check_wait() override
  {
    ++check_wait_count_;
    return WS_INVALID;
  }
  int64_t get_check_wait_count() const { return check_wait_count_; }

private:
  int64_t check_wait_count_;
};

TEST_F(TestLobUtil, invalid_worker_stops_retry_with_context_specific_error)
{
  lib::Worker *saved_worker = &THIS_WORKER;
  for (const bool is_tenant_worker : {false, true}) {
    InvalidLobRetryWorker worker;
    worker.set_is_th_worker(is_tenant_worker);
    lib::Worker::set_worker_to_thread_local(&worker);
    ObLobReadContext read_ctx;
    read_ctx.timeout_ = INT64_MAX;
    bool need_retry = true;
    const int expected_error = is_tenant_worker ? OB_TENANT_NOT_IN_SERVER : OB_CANCELED;
    EXPECT_EQ(expected_error, ObLobRetryUtil::check_need_retry(read_ctx, OB_LS_OFFLINE, 0, need_retry));
    EXPECT_FALSE(need_retry);
    EXPECT_EQ(1, worker.get_check_wait_count());
    lib::Worker::set_worker_to_thread_local(saved_worker);
  }
}

TEST_F(TestLobUtil, retry_stops_on_expired_deadline)
{
  ObLobReadContext read_ctx;
  read_ctx.timeout_ = ObTimeUtility::current_time() - 1;
  bool need_retry = true;
  EXPECT_EQ(OB_SUCCESS, ObLobRetryUtil::check_need_retry(read_ctx, OB_LS_OFFLINE, 0, need_retry));
  EXPECT_FALSE(need_retry);
}

TEST_F(TestLobUtil, nonretryable_error_clears_previous_retry_decision)
{
  ObLobReadContext read_ctx;
  read_ctx.timeout_ = INT64_MAX;
  bool need_retry = true;
  EXPECT_EQ(OB_SUCCESS, ObLobRetryUtil::check_need_retry(read_ctx, OB_INVALID_ARGUMENT, 0, need_retry));
  EXPECT_FALSE(need_retry);
}

TEST_F(TestLobUtil, rpc_read_does_not_start_nested_retry)
{
  ObLobReadContext read_ctx;
  read_ctx.timeout_ = INT64_MAX;
  read_ctx.from_rpc_ = true;
  read_ctx.enable_remote_retry_ = false;
  bool need_retry = true;
  EXPECT_EQ(OB_SUCCESS, ObLobRetryUtil::check_need_retry(read_ctx, OB_LS_OFFLINE, 0, need_retry));
  EXPECT_FALSE(need_retry);
}

TEST_F(TestLobUtil, handler_stops_on_retry_timeout_without_reexecuting)
{
  ObLobReadContext read_ctx;
  read_ctx.timeout_ = ObTimeUtility::current_time() - 1;
  FailingLobQueryHandler handler(read_ctx);
  EXPECT_EQ(OB_LS_OFFLINE, handler.execute());
  EXPECT_EQ(1, handler.get_execute_count());
}

TEST_F(TestLobUtil, retry_rejects_dropping_tenant_before_location_lookup)
{
  LobRetryMockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  share::schema::ObTenantSchema tenant;
  tenant.set_tenant_id(1002);
  tenant.set_schema_version(128);
  tenant.set_status(share::schema::TENANT_STATUS_DROPPING);
  ASSERT_EQ(OB_SUCCESS, tenant.set_tenant_name("lob_retry_dropping"));
  ASSERT_EQ(OB_SUCCESS, schema_service.add_tenant_schema(tenant, 128));
  share::schema::ObMultiVersionSchemaService *saved_schema_service = GCTX.schema_service_;
  GCTX.schema_service_ = &schema_service;
  ObLobReadContext read_ctx;
  read_ctx.tenant_id_ = 1002;
  read_ctx.timeout_ = INT64_MAX;
  for (const int error_code : {OB_LS_OFFLINE, OB_REPLICA_NOT_READABLE, OB_LS_NOT_EXIST}) {
    bool need_retry = true;
    EXPECT_EQ(OB_LS_TENANT_DROPPING, ObLobRetryUtil::check_need_retry(read_ctx, error_code, 0, need_retry));
    EXPECT_FALSE(need_retry);
  }
  GCTX.schema_service_ = saved_schema_service;
}

TEST_F(TestLobUtil, retry_rejects_dropped_tenant_without_live_schema)
{
  LobRetryMockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  share::schema::ObDropTenantInfo drop_info;
  drop_info.set_tenant_id(1002);
  drop_info.set_schema_version(128);
  ASSERT_EQ(OB_SUCCESS, schema_service.mgr_.add_drop_tenant_info(drop_info));
  share::schema::ObMultiVersionSchemaService *saved_schema_service = GCTX.schema_service_;
  GCTX.schema_service_ = &schema_service;
  ObLobReadContext read_ctx;
  read_ctx.tenant_id_ = 1002;
  read_ctx.timeout_ = INT64_MAX;
  bool need_retry = true;
  EXPECT_EQ(OB_TENANT_HAS_BEEN_DROPPED, ObLobRetryUtil::check_need_retry(read_ctx, OB_LS_OFFLINE, 0, need_retry));
  EXPECT_FALSE(need_retry);
  GCTX.schema_service_ = saved_schema_service;
}

TEST_F(TestLobUtil, normal_tenant_is_not_reported_dropped_when_location_lookup_fails)
{
  LobRetryMockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  share::schema::ObTenantSchema tenant;
  tenant.set_tenant_id(1002);
  tenant.set_schema_version(128);
  tenant.set_status(share::schema::TENANT_STATUS_NORMAL);
  ASSERT_EQ(OB_SUCCESS, tenant.set_tenant_name("lob_retry_normal"));
  ASSERT_EQ(OB_SUCCESS, schema_service.add_tenant_schema(tenant, 128));
  share::schema::ObMultiVersionSchemaService *saved_schema_service = GCTX.schema_service_;
  share::ObLocationService *saved_location_service = GCTX.location_service_;
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  GCTX.schema_service_ = &schema_service;
  GCTX.location_service_ = nullptr;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000);
  ObLobReadContext read_ctx;
  read_ctx.tenant_id_ = 1002;
  read_ctx.timeout_ = INT64_MAX;
  bool need_retry = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObLobRetryUtil::check_need_retry(read_ctx, OB_LS_OFFLINE, 0, need_retry));
  EXPECT_FALSE(need_retry);
  THIS_WORKER.set_timeout_ts(saved_timeout);
  GCTX.location_service_ = saved_location_service;
  GCTX.schema_service_ = saved_schema_service;
}

TEST_F(TestLobUtil, locator_without_retry_info_waits_before_refresh)
{
  ObLobReadContext read_ctx;
  read_ctx.tenant_id_ = OB_SYS_TENANT_ID;
  read_ctx.timeout_ = INT64_MAX;
  share::ObLocationService *saved_location_service = GCTX.location_service_;
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  GCTX.location_service_ = nullptr;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000);
  const int64_t start_ts = ObTimeUtility::current_time();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObLobLocationUtil::lob_refresh_location(read_ctx, OB_LS_OFFLINE, 1));
  // Allow clock granularity while detecting the former immediate retry path.
  EXPECT_GE(ObTimeUtility::current_time() - start_ts, 9 * 1000);
  THIS_WORKER.set_timeout_ts(saved_timeout);
  GCTX.location_service_ = saved_location_service;
}

TEST_F(TestLobUtil, retry_reclaims_iter_arena_without_touching_caller_data)
{
  LobRetryMockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  share::ObLocationService location_service;
  share::ObLSTableOperator ls_table;
  share::ObRsMgr rs_mgr;
  obrpc::ObSrvRpcProxy rpc_proxy;
  share::ObLSLocationService &ls_service = location_service.ls_location_service_;
  ASSERT_EQ(OB_SUCCESS, ls_service.inner_cache_.init());
  ls_service.lst_ = &ls_table;
  ls_service.schema_service_ = &schema_service;
  ls_service.rs_mgr_ = &rs_mgr;
  ls_service.srv_rpc_proxy_ = &rpc_proxy;
  ls_service.inited_ = true;
  location_service.inited_ = true;
  const int64_t cluster_id = GCONF.cluster_id;
  const share::ObLSID ls_id(1);
  share::ObLSLocation location;
  share::ObLSReplicaLocation replica;
  ObAddr leader;
  ASSERT_TRUE(leader.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, location.init(cluster_id, OB_SYS_TENANT_ID, ls_id, ObTimeUtility::current_time()));
  ASSERT_EQ(OB_SUCCESS, replica.init(leader, LEADER, 2881, REPLICA_TYPE_FULL,
      ObReplicaProperty(), share::ObLSRestoreStatus(share::ObLSRestoreStatus::NONE), 1));
  ASSERT_EQ(OB_SUCCESS, location.add_replica_location(replica));
  ASSERT_EQ(OB_SUCCESS, ls_service.inner_cache_.update(false, location.get_cache_key(), location));
  ObArenaAllocator allocator;
  char *caller_data = static_cast<char *>(allocator.alloc(32));
  ASSERT_NE(nullptr, caller_data);
  MEMCPY(caller_data, "keep caller data", sizeof("keep caller data"));
  const int64_t initial_used = allocator.used();
  ObLobReadContext read_ctx;
  read_ctx.tenant_id_ = OB_SYS_TENANT_ID;
  read_ctx.ls_id_ = ls_id;
  read_ctx.allocator_ = &allocator;
  read_ctx.timeout_ = INT64_MAX;
  ObPersistentLobAdaptor adaptor(OB_SYS_TENANT_ID);
  RetryingLobScanHandler handler(read_ctx, adaptor);
  share::schema::ObMultiVersionSchemaService *saved_schema_service = GCTX.schema_service_;
  share::ObLocationService *saved_location_service = GCTX.location_service_;
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  GCTX.schema_service_ = &schema_service;
  GCTX.location_service_ = &location_service;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 30 * 1000 * 1000);
  EXPECT_EQ(OB_SUCCESS, handler.execute());
  EXPECT_EQ(4, handler.get_execute_count());
  EXPECT_TRUE(handler.arena_was_cleared());
  EXPECT_EQ(0, read_ctx.iter_allocator_.used());
  EXPECT_EQ(initial_used, allocator.used());
  EXPECT_STREQ("keep caller data", caller_data);
  THIS_WORKER.set_timeout_ts(saved_timeout);
  GCTX.location_service_ = saved_location_service;
  GCTX.schema_service_ = saved_schema_service;
}

TEST_F(TestLobUtil, read_context_assign_keeps_iterator_arenas_independent)
{
  ObLobReadContext source;
  ObLobReadContext target;
  char *source_data = static_cast<char *>(source.iter_allocator_.alloc(32));
  ASSERT_NE(nullptr, source_data);
  MEMCPY(source_data, "source arena", sizeof("source arena"));
  ASSERT_EQ(OB_SUCCESS, target.assign(source));
  ASSERT_NE(nullptr, target.iter_allocator_.alloc(64));
  target.iter_allocator_.reset();
  EXPECT_STREQ("source arena", source_data);
  EXPECT_GT(source.iter_allocator_.used(), 0);
  EXPECT_EQ(0, target.iter_allocator_.used());
}

//==================== ObLobCharsetUtil::transform_query_result_charset ====================
TEST_F(TestLobUtil, transform_query_result_ascii)
{
  const char *data = "abcdefghij"; // 10 ascii chars
  uint32_t byte_st = 2;  // in: char offset
  uint32_t byte_len = 3; // in: char count
  ObLobCharsetUtil::transform_query_result_charset(CS_TYPE_UTF8MB4_BIN, data, 10, byte_len, byte_st);
  ASSERT_EQ(2u, byte_st);
  ASSERT_EQ(3u, byte_len);
}

TEST_F(TestLobUtil, transform_query_result_multibyte)
{
  const char *data = "\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad"; // 5 "中" = 15 bytes
  uint32_t byte_st = 1;  // in: char offset
  uint32_t byte_len = 2; // in: char count
  ObLobCharsetUtil::transform_query_result_charset(CS_TYPE_UTF8MB4_GENERAL_CI, data, 15, byte_len, byte_st);
  ASSERT_EQ(3u, byte_st);
  ASSERT_EQ(6u, byte_len);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_util.log*");
  OB_LOGGER.set_file_name("test_lob_util.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

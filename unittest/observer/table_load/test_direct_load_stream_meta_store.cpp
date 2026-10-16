/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>
#include <string>
#include "test_direct_load_stream_utils.h"
#include "observer/table_load/stream/ob_direct_load_stream_persistent_meta_store.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{

static const int64_t TEST_STREAM_ID = 10001;

class MockDirectLoadStreamSQLClient : public common::ObISQLClient
{
public:
  MockDirectLoadStreamSQLClient()
    : write_ret_(common::OB_SUCCESS),
      read_ret_(common::OB_NOT_SUPPORTED),
      affected_rows_(1),
      write_count_(0),
      read_count_(0),
      last_read_sql_(),
      last_sql_()
  {
  }
  int escape(const char *, const int64_t, char *, const int64_t, int64_t &) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  int read(ReadResult &, const uint64_t, const char *sql) override
  {
    ++read_count_;
    last_read_sql_ = nullptr == sql ? "" : sql;
    return read_ret_;
  }
  int read(ReadResult &, const int64_t, const uint64_t, const char *sql) override
  {
    ++read_count_;
    last_read_sql_ = nullptr == sql ? "" : sql;
    return read_ret_;
  }
  int read(ReadResult &, const uint64_t, const char *sql, const int32_t) override
  {
    ++read_count_;
    last_read_sql_ = nullptr == sql ? "" : sql;
    return read_ret_;
  }
  int write(const uint64_t, const char *sql, int64_t &affected_rows) override
  {
    ++write_count_;
    last_sql_ = nullptr == sql ? "" : sql;
    affected_rows = affected_rows_;
    return write_ret_;
  }
  int write(const uint64_t tenant_id, const char *sql,
            const int32_t, int64_t &affected_rows) override
  {
    return write(tenant_id, sql, affected_rows);
  }
  common::sqlclient::ObISQLConnectionPool *get_pool() override { return nullptr; }
  common::sqlclient::ObISQLConnection *get_connection() override { return nullptr; }
  bool is_oracle_mode() const override { return false; }

  int write_ret_;
  int read_ret_;
  int64_t affected_rows_;
  int64_t write_count_;
  int64_t read_count_;
  std::string last_read_sql_;
  std::string last_sql_;
};

class ReconcileDirectLoadStreamMetaStore
  : public ObDirectLoadStreamPersistentMetaStore
{
public:
  explicit ReconcileDirectLoadStreamMetaStore(const uint64_t tenant_id)
    : ObDirectLoadStreamPersistentMetaStore(tenant_id),
      create_param_(nullptr),
      has_finish_meta_(false),
      finish_status_(DIRECT_LOAD_STREAM_STATUS_MAX),
      finish_result_code_(common::OB_SUCCESS),
      reconcile_ret_(common::OB_SUCCESS),
      reconcile_read_count_(0)
  {
  }
  void set_create_param(const ObDirectLoadStreamCreateParam &param)
  {
    create_param_ = &param;
  }
  void set_finish_meta(const ObDirectLoadStreamStatus status,
                       const int result_code)
  {
    has_finish_meta_ = true;
    finish_status_ = status;
    finish_result_code_ = result_code;
  }
  void set_reconcile_ret(const int reconcile_ret)
  {
    reconcile_ret_ = reconcile_ret;
  }
  int get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta) override
  {
    int ret = common::OB_SUCCESS;
    ++reconcile_read_count_;
    if (common::OB_SUCCESS != reconcile_ret_) {
      ret = reconcile_ret_;
    } else if (has_finish_meta_) {
      meta.stream_id_ = stream_id;
      meta.tenant_id_ = TEST_DIRECT_LOAD_STREAM_TENANT_ID;
      meta.status_ = finish_status_;
      meta.result_code_ = finish_result_code_;
    } else if (OB_ISNULL(create_param_)) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      meta.stream_id_ = stream_id;
      meta.tenant_id_ = create_param_->tenant_id_;
      meta.user_id_ = create_param_->user_id_;
      meta.table_id_ = create_param_->table_id_;
      meta.target_schema_version_ = create_param_->target_schema_version_;
      meta.status_ = DIRECT_LOAD_STREAM_STATUS_OPEN;
      meta.result_code_ = common::OB_SUCCESS;
      meta.expire_ts_ = create_param_->expire_ts_;
      if (OB_FAIL(meta.queue_entries_.assign(create_param_->queue_entries_))) {
      }
    }
    return ret;
  }

  const ObDirectLoadStreamCreateParam *create_param_;
  bool has_finish_meta_;
  ObDirectLoadStreamStatus finish_status_;
  int finish_result_code_;
  int reconcile_ret_;
  int64_t reconcile_read_count_;
};

static int build_create_param(ObDirectLoadStreamCreateParam &param)
{
  int ret = common::OB_SUCCESS;
  table::ObDirectLoadStreamQueueEntry entry;
  param = ObDirectLoadStreamCreateParam();
  param.tenant_id_ = TEST_DIRECT_LOAD_STREAM_TENANT_ID;
  param.user_id_ = 2001;
  param.table_id_ = 4001;
  param.target_schema_version_ = 5001;
  param.expire_ts_ = 6001;
  if (!entry.server_addr_.set_ip_addr("127.0.0.1", 2882)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    entry.queue_count_ = 2;
    ret = param.queue_entries_.push_back(entry);
  }
  return ret;
}

static int build_claim_param(const int64_t stream_id, const char *ip, const int32_t port,
                             const uint64_t session_id, ObDirectLoadStreamClaimParam &param)
{
  int ret = common::OB_SUCCESS;
  param = ObDirectLoadStreamClaimParam();
  param.stream_id_ = stream_id;
  param.session_id_ = session_id;
  param.session_create_ts_ = 7001;
  if (!param.owner_server_.set_ip_addr(ip, port)) {
    ret = common::OB_ERR_UNEXPECTED;
  }
  return ret;
}

static int create_mock_stream(ObMockDirectLoadStreamMetaStore &meta_store, int64_t &stream_id)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamCreateParam param;
  stream_id = TEST_STREAM_ID;
  if (OB_FAIL(build_create_param(param))) {
  } else if (OB_FAIL(meta_store.create_stream(stream_id, param))) {
  }
  return ret;
}

static void expect_missing_stream_finish_contract(
    ObIDirectLoadStreamMetaStore &meta_store, const int64_t stream_id)
{
  bool finished = true;
  EXPECT_EQ(common::OB_SUCCESS,
            meta_store.finish_expired_unclaimed_stream(stream_id, 7001, finished));
  EXPECT_FALSE(finished);
  finished = true;
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            meta_store.finish_stream_if_open(stream_id, common::OB_SCHEMA_ERROR, finished));
  EXPECT_FALSE(finished);
}

// 从未创建的 stream 在 mock 和生产 Store 中遵守同一返回码及输出参数合同。
TEST(TestDirectLoadStreamMetaStore, missing_stream_finish_contract_matches_persistent)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ObMockDirectLoadStreamMetaStore mock_store;
  MockDirectLoadStreamSQLClient sql_client;
  ReconcileDirectLoadStreamMetaStore persistent_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  sql_client.affected_rows_ = 0;
  persistent_store.set_reconcile_ret(common::OB_ENTRY_NOT_EXIST);
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, mock_store.init());
  ASSERT_EQ(common::OB_SUCCESS, persistent_store.init(sql_client));

  expect_missing_stream_finish_contract(mock_store, TEST_STREAM_ID);
  expect_missing_stream_finish_contract(persistent_store, TEST_STREAM_ID);
  EXPECT_EQ(2, sql_client.write_count_);
  EXPECT_EQ(1, persistent_store.reconcile_read_count_);
}

// GC 扫描后候选被并发删除：旧候选不应重新终结或清理，两个 Store 的结果一致。
TEST(TestDirectLoadStreamMetaStore, deleted_gc_candidate_finish_contract_matches_persistent)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ObMockDirectLoadStreamMetaStore mock_store;
  MockDirectLoadStreamSQLClient sql_client;
  ReconcileDirectLoadStreamMetaStore persistent_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  common::ObSEArray<ObDirectLoadStreamMeta, 4> candidates;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, mock_store.init());
  ASSERT_EQ(common::OB_SUCCESS, persistent_store.init(sql_client));
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(mock_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, mock_store.scan_gc_streams(7001, candidates));
  ASSERT_EQ(1, candidates.count());
  ASSERT_EQ(common::OB_SUCCESS, mock_store.finish_stream(stream_id, common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS, mock_store.delete_stream(stream_id, INT64_MAX));
  ASSERT_EQ(common::OB_ENTRY_NOT_EXIST, mock_store.get_stream(stream_id, meta));

  sql_client.affected_rows_ = 0;
  persistent_store.set_reconcile_ret(common::OB_ENTRY_NOT_EXIST);
  expect_missing_stream_finish_contract(mock_store, candidates.at(0).stream_id_);
  expect_missing_stream_finish_contract(persistent_store, candidates.at(0).stream_id_);
  EXPECT_EQ(2, sql_client.write_count_);
  EXPECT_EQ(1, persistent_store.reconcile_read_count_);
}

// 验证 mock store 可以完成初始化和显式 reset。
TEST(TestDirectLoadStreamMetaStore, mock_store_init_reset)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  meta_store.reset();
}

// 验证默认 metadata 保持非法值，只有 create 成功后才进入 OPEN。
TEST(TestDirectLoadStreamMetaStore, default_meta_is_invalid)
{
  ObDirectLoadStreamMeta meta;
  EXPECT_FALSE(is_valid_stream_id(meta.stream_id_));
  EXPECT_EQ(common::OB_INVALID_ID, meta.tenant_id_);
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_MAX, meta.status_);
}

// 验证 create 初始化 OPEN metadata，get 返回完整的独立副本。
TEST(TestDirectLoadStreamMetaStore, create_and_get_stream)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMeta meta;
  const int64_t stream_id = TEST_STREAM_ID;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(stream_id, param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(stream_id, meta.stream_id_);
  EXPECT_EQ(param.tenant_id_, meta.tenant_id_);
  EXPECT_EQ(param.user_id_, meta.user_id_);
  EXPECT_EQ(param.table_id_, meta.table_id_);
  EXPECT_EQ(param.target_schema_version_, meta.target_schema_version_);
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
  EXPECT_EQ(1, meta.queue_entries_.count());
  EXPECT_EQ(2, meta.queue_entries_.at(0).queue_count_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
  EXPECT_GT(meta.create_ts_, 0);
  EXPECT_GE(meta.modified_ts_, meta.create_ts_);
  EXPECT_EQ(param.expire_ts_, meta.expire_ts_);
}

// CREATE outcome 是 Service 判断能否补偿 queue 的唯一事实源。
TEST(TestDirectLoadStreamMetaStore, mock_create_reports_explicit_outcome)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATED, outcome);
}

// production store 成功 INSERT 必须报告 CREATED，并写入冻结 queue_list。
TEST(TestDirectLoadStreamMetaStore, persistent_create_reports_committed)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATED, outcome);
  EXPECT_EQ(1, sql_client.write_count_);
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("__all_direct_load_stream"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("queue_list"));
}

// timeout/transport 类错误后的点查仍失败时，结果必须保持 UNKNOWN，不能降为 NOT_CREATED。
TEST(TestDirectLoadStreamMetaStore, persistent_create_timeout_and_read_error_stays_unknown)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  sql_client.write_ret_ = common::OB_TIMEOUT;
  sql_client.read_ret_ = common::OB_TIMEOUT;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_TIMEOUT,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN, outcome);
  EXPECT_EQ(1, sql_client.write_count_);
  EXPECT_EQ(1, sql_client.read_count_);
}

// write 返回错误也不能证明 INSERT 未提交，点查未命中时必须保持 UNKNOWN。
TEST(TestDirectLoadStreamMetaStore, persistent_create_write_error_stays_unknown)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  sql_client.write_ret_ = common::OB_INVALID_ARGUMENT;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN, outcome);
  EXPECT_EQ(1, sql_client.write_count_);
  EXPECT_EQ(1, sql_client.read_count_);
}

// OB_TRANS_COMMITED 明确表示事务已经提交，点查命中完整 OPEN 行后应返回 CREATED。
TEST(TestDirectLoadStreamMetaStore, persistent_create_committed_error_reconciles_created)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ReconcileDirectLoadStreamMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  sql_client.write_ret_ = common::OB_TRANS_COMMITED;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  meta_store.set_create_param(param);
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_SUCCESS,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATED, outcome);
  EXPECT_EQ(1, sql_client.write_count_);
  EXPECT_EQ(1, meta_store.reconcile_read_count_);
}

// affected rows 异常不能证明 INSERT 未提交，必须进入点查并保持 UNKNOWN。
TEST(TestDirectLoadStreamMetaStore, persistent_create_affected_rows_anomaly_stays_unknown)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam param;
  ObDirectLoadStreamMetaCreateOutcome outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  sql_client.affected_rows_ = 0;
  sql_client.read_ret_ = common::OB_TIMEOUT;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            meta_store.create_stream(TEST_STREAM_ID, param, outcome));
  EXPECT_EQ(DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN, outcome);
  EXPECT_EQ(1, sql_client.write_count_);
  EXPECT_EQ(1, sql_client.read_count_);
}

// production CAS/delete SQL 必须把状态、终态 first-write-wins 和 GC guard 写进谓词。
TEST(TestDirectLoadStreamMetaStore, persistent_status_cas_and_guarded_delete_sql)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamClaimParam claim_param;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.seal_stream(TEST_STREAM_ID));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("__all_direct_load_stream"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status = 1"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status = 0"));

  ASSERT_EQ(common::OB_SUCCESS,
            build_claim_param(TEST_STREAM_ID, "127.0.0.1", 2882, 8001, claim_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.claim_stream(claim_param));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("owner_server"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("owner_server IS NULL"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("session_id IS NULL"));

  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.finish_stream(TEST_STREAM_ID, common::OB_TIMEOUT));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status = 2"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("result_code IS NULL"));

  bool finished = false;
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.finish_stream_if_open(
                TEST_STREAM_ID, common::OB_SCHEMA_ERROR, finished));
  EXPECT_TRUE(finished);
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status = 0"));

  finished = false;
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.finish_expired_unclaimed_stream(
                TEST_STREAM_ID, 7001, finished));
  EXPECT_TRUE(finished);
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status IN (0, 1)"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("expire_ts < 7001"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("owner_server IS NULL"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("session_id IS NULL"));
  EXPECT_NE(std::string::npos,
            sql_client.last_sql_.find("session_create_ts IS NULL"));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.delete_stream(TEST_STREAM_ID, INT64_MAX));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("DELETE FROM"));
  EXPECT_NE(std::string::npos, sql_client.last_sql_.find("status = 2"));
  EXPECT_NE(std::string::npos,
            sql_client.last_sql_.find("gmt_modified <= usec_to_time"));
}

// UPDATE 已提交但返回 timeout/committed error 时，
// 相同 FINISHED 行必须恢复成功，令调用方继续 queue fanout。
TEST(TestDirectLoadStreamMetaStore, persistent_finish_if_open_reconciles_committed_errors)
{
  const int write_errors[] = {
    common::OB_TIMEOUT,
    common::OB_TRANS_COMMITED
  };
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  const int64_t write_error_count =
      static_cast<int64_t>(sizeof(write_errors) / sizeof(write_errors[0]));
  for (int64_t i = 0; i < write_error_count; ++i) {
    MockDirectLoadStreamSQLClient sql_client;
    ReconcileDirectLoadStreamMetaStore meta_store(
        TEST_DIRECT_LOAD_STREAM_TENANT_ID);
    bool finished = false;
    sql_client.write_ret_ = write_errors[i];
    meta_store.set_finish_meta(
        DIRECT_LOAD_STREAM_STATUS_FINISHED, common::OB_SCHEMA_ERROR);
    ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));

    EXPECT_EQ(common::OB_SUCCESS,
              meta_store.finish_stream_if_open(
                  TEST_STREAM_ID, common::OB_SCHEMA_ERROR, finished));
    EXPECT_TRUE(finished);
    EXPECT_EQ(1, sql_client.write_count_);
    EXPECT_EQ(1, meta_store.reconcile_read_count_);
  }
}

// UPDATE 错误后的点查若确认其他终态已获胜，必须按 CAS 竞争失败返回。
TEST(TestDirectLoadStreamMetaStore, persistent_finish_if_open_detects_terminal_competition)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ReconcileDirectLoadStreamMetaStore meta_store(
      TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  bool finished = false;
  sql_client.write_ret_ = common::OB_TIMEOUT;
  meta_store.set_finish_meta(
      DIRECT_LOAD_STREAM_STATUS_FINISHED, common::OB_CANCELED);

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            meta_store.finish_stream_if_open(
                TEST_STREAM_ID, common::OB_SCHEMA_ERROR, finished));
  EXPECT_FALSE(finished);
  EXPECT_EQ(1, meta_store.reconcile_read_count_);
}

// UPDATE 错误后的点查仍失败时，保留原始错误明确表示提交结果不确定。
TEST(TestDirectLoadStreamMetaStore, persistent_finish_if_open_read_error_stays_unknown)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ReconcileDirectLoadStreamMetaStore meta_store(
      TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  bool finished = false;
  sql_client.write_ret_ = common::OB_TIMEOUT;
  meta_store.set_reconcile_ret(common::OB_TIMEOUT);

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_TIMEOUT,
            meta_store.finish_stream_if_open(
                TEST_STREAM_ID, common::OB_SCHEMA_ERROR, finished));
  EXPECT_FALSE(finished);
  EXPECT_EQ(1, meta_store.reconcile_read_count_);
}

// OPEN/SEALED 候选按 expire_ts 扫描，FINISHED GC 候选按 gmt_modified 扫描。
TEST(TestDirectLoadStreamMetaStore, persistent_scan_uses_status_specific_time_guard)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamScanParam scan_param;
  common::ObSEArray<ObDirectLoadStreamMeta, 4> metas;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  scan_param.status_ = DIRECT_LOAD_STREAM_STATUS_OPEN;
  scan_param.upper_bound_ts_ = 6001;
  scan_param.start_after_stream_id_ = 100;
  scan_param.limit_ = 16;
  EXPECT_EQ(common::OB_NOT_SUPPORTED, meta_store.scan_streams(scan_param, metas));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("expire_ts <= 6001"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("stream_id > 100"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("LIMIT 16"));

  scan_param.status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
  EXPECT_EQ(common::OB_NOT_SUPPORTED, meta_store.scan_streams(scan_param, metas));
  EXPECT_NE(std::string::npos,
            sql_client.last_read_sql_.find("gmt_modified <= usec_to_time(6001)"));
}

// 验证 GC 使用一次查询筛出全部过期非终态和已有终态记录，不携带分页游标。
TEST(TestDirectLoadStreamMetaStore, persistent_gc_scan_uses_single_candidate_filter)
{
  ObDirectLoadStreamTestTenantEnv tenant_env;
  MockDirectLoadStreamSQLClient sql_client;
  ObDirectLoadStreamPersistentMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  common::ObSEArray<ObDirectLoadStreamMeta, 4> metas;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init(sql_client));
  EXPECT_EQ(common::OB_NOT_SUPPORTED, meta_store.scan_gc_streams(7001, metas));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("status IN (0, 1)"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("expire_ts < 7001"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("owner_server IS NULL"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("session_id IS NULL"));
  EXPECT_NE(std::string::npos,
            sql_client.last_read_sql_.find("session_create_ts IS NULL"));
  EXPECT_NE(std::string::npos, sql_client.last_read_sql_.find("OR status = 2"));
  EXPECT_EQ(std::string::npos, sql_client.last_read_sql_.find("LIMIT"));
}

// 验证所有按 stream id 操作的入口拒绝非法 id。
TEST(TestDirectLoadStreamMetaStore, reject_invalid_stream_id)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim_param;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));

  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta_store.create_stream(common::OB_INVALID_ID, create_param));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.create_stream(0, create_param));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.get_stream(common::OB_INVALID_ID, meta));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.seal_stream(common::OB_INVALID_ID));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta_store.finish_stream(common::OB_INVALID_ID, common::OB_SUCCESS));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.claim_stream(claim_param));
}

// 验证无效租户和 SQL session owner 边界字段不会进入 metadata。
TEST(TestDirectLoadStreamMetaStore, reject_invalid_owner_boundary_fields)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamClaimParam claim_param;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());

  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  create_param.tenant_id_ = common::OB_INVALID_TENANT_ID;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.create_stream(TEST_STREAM_ID, create_param));

  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            build_claim_param(stream_id, "127.0.0.1", 2882, common::INVALID_SESSID,
                              claim_param));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.claim_stream(claim_param));

  ASSERT_EQ(common::OB_SUCCESS,
            build_claim_param(stream_id, "127.0.0.1", 2882,
                              static_cast<uint64_t>(common::INVALID_SESSID) + 1, claim_param));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, meta_store.claim_stream(claim_param));
}

// 验证 metadata CREATE 复用 shared queue_list 的重复 owner 校验。
TEST(TestDirectLoadStreamMetaStore, reject_invalid_queue_topology)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam duplicate_param;
  ObDirectLoadStreamMeta meta;
  table::ObDirectLoadStreamQueueEntry duplicate_entry;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(duplicate_param));
  duplicate_entry = duplicate_param.queue_entries_.at(0);
  ASSERT_EQ(common::OB_SUCCESS, duplicate_param.queue_entries_.push_back(duplicate_entry));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta_store.create_stream(TEST_STREAM_ID, duplicate_param));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, meta_store.get_stream(TEST_STREAM_ID, meta));
}

// 验证重复 stream id 被拒绝，并且不会覆盖已存在的 metadata。
TEST(TestDirectLoadStreamMetaStore, reject_duplicate_stream_id)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamCreateParam first_param;
  ObDirectLoadStreamCreateParam duplicate_param;
  ObDirectLoadStreamMeta meta;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(first_param));
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(duplicate_param));
  duplicate_param.table_id_ = first_param.table_id_ + 1;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(TEST_STREAM_ID, first_param));
  EXPECT_EQ(common::OB_ENTRY_EXIST,
            meta_store.create_stream(TEST_STREAM_ID, duplicate_param));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(TEST_STREAM_ID, meta));
  EXPECT_EQ(first_param.table_id_, meta.table_id_);
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);
}

// 验证超过内联容量的 queue entries 在业务 tenant 下完成存储和返回拷贝。
TEST(TestDirectLoadStreamMetaStore, copy_queue_entries_beyond_inline_capacity)
{
  const int64_t queue_entry_count = 5;
  ObDirectLoadStreamTestTenantEnv tenant_env;
  ObMockDirectLoadStreamMetaStore meta_store(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
  ObDirectLoadStreamCreateParam create_param;
  ObDirectLoadStreamMeta meta;

  ASSERT_EQ(common::OB_SUCCESS, tenant_env.init());
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, build_create_param(create_param));
  for (int64_t i = create_param.queue_entries_.count(); i < queue_entry_count; ++i) {
    table::ObDirectLoadStreamQueueEntry entry;
    ASSERT_TRUE(entry.server_addr_.set_ip_addr(
        "127.0.0.1", 2882 + static_cast<int32_t>(i)));
    entry.queue_count_ = i + 1;
    ASSERT_EQ(common::OB_SUCCESS, create_param.queue_entries_.push_back(entry));
  }

  ASSERT_EQ(common::OB_SUCCESS, meta_store.create_stream(TEST_STREAM_ID, create_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(TEST_STREAM_ID, meta));
  ASSERT_EQ(queue_entry_count, meta.queue_entries_.count());
  EXPECT_TRUE(create_param.queue_entries_.at(queue_entry_count - 1).server_addr_
              == meta.queue_entries_.at(queue_entry_count - 1).server_addr_);
  EXPECT_EQ(create_param.queue_entries_.at(queue_entry_count - 1).queue_count_,
            meta.queue_entries_.at(queue_entry_count - 1).queue_count_);
}

// 验证查询不存在的 stream 返回明确的 not found 错误。
TEST(TestDirectLoadStreamMetaStore, get_missing_stream)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());

  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, meta_store.get_stream(12345, meta));
}

// 验证 OPEN 可以进入 SEALED，重复 seal 保持幂等。
TEST(TestDirectLoadStreamMetaStore, seal_open_stream)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.seal_stream(stream_id));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_SEALED, meta.status_);
  // CLOSE 侧语义幂等，重复 seal 不改终态结果。
  EXPECT_EQ(common::OB_SUCCESS, meta_store.seal_stream(stream_id));
}

// 验证 OPEN 不能直接成功结束，但允许携带非零错误进入 FINISHED。
TEST(TestDirectLoadStreamMetaStore, finish_open_stream_with_error)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH, meta_store.finish_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);

  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
}

// fail-if-open 只允许 OPEN 赢得一次全局终态 CAS，SEALED 不能被覆盖。
TEST(TestDirectLoadStreamMetaStore, finish_stream_if_open_is_atomic)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;
  bool finished = false;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));

  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.finish_stream_if_open(
                stream_id, common::OB_SCHEMA_ERROR, finished));
  EXPECT_TRUE(finished);
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SCHEMA_ERROR, meta.result_code_);

  finished = false;
  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            meta_store.finish_stream_if_open(
                stream_id, common::OB_TIMEOUT, finished));
  EXPECT_FALSE(finished);
}

// 验证正常成功路径必须先 SEALED，再进入 FINISHED(OB_SUCCESS)。
TEST(TestDirectLoadStreamMetaStore, finish_sealed_stream)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.seal_stream(stream_id));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_SUCCESS));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_SUCCESS, meta.result_code_);
}

// 验证 FINISHED 终态不能被后续 finish、seal 或 claim 覆盖。
TEST(TestDirectLoadStreamMetaStore, terminal_state_not_overwritten)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim_param;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, build_claim_param(stream_id, "127.0.0.1", 2882, 8001, claim_param));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_TIMEOUT));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH, meta_store.finish_stream(stream_id, common::OB_SUCCESS));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH, meta_store.seal_stream(stream_id));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH, meta_store.claim_stream(claim_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, meta.status_);
  EXPECT_EQ(common::OB_TIMEOUT, meta.result_code_);
}

// 验证首次 claim 记录完整的 server 和 session owner 信息。
TEST(TestDirectLoadStreamMetaStore, claim_stream_once)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam claim_param;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, build_claim_param(stream_id, "127.0.0.1", 2882, 8001, claim_param));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.claim_stream(claim_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_TRUE(claim_param.owner_server_ == meta.owner_server_);
  EXPECT_EQ(claim_param.session_id_, meta.session_id_);
  EXPECT_EQ(claim_param.session_create_ts_, meta.session_create_ts_);
}

// 验证已有 owner 的 stream 拒绝第二次 claim，并保留原 owner。
TEST(TestDirectLoadStreamMetaStore, claim_rejects_existing_owner)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamClaimParam first_claim;
  ObDirectLoadStreamClaimParam second_claim;
  int64_t stream_id = common::OB_INVALID_ID;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, build_claim_param(stream_id, "127.0.0.1", 2882, 8001, first_claim));
  ASSERT_EQ(common::OB_SUCCESS,
            build_claim_param(stream_id, "127.0.0.2", 2882, 8002, second_claim));

  ASSERT_EQ(common::OB_SUCCESS, meta_store.claim_stream(first_claim));
  EXPECT_EQ(common::OB_STATE_NOT_MATCH, meta_store.claim_stream(second_claim));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_TRUE(first_claim.owner_server_ == meta.owner_server_);
  EXPECT_EQ(first_claim.session_id_, meta.session_id_);
}

// scan 使用 status 对应的时间边界，delete 只允许受保护的 FINISHED 行且保持 missing 幂等。
TEST(TestDirectLoadStreamMetaStore, scan_and_guarded_delete_finished_stream)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamScanParam scan_param;
  common::ObSEArray<ObDirectLoadStreamMeta, 4> metas;
  ObDirectLoadStreamMeta meta;
  int64_t stream_id = common::OB_INVALID_ID;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));

  scan_param.status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
  scan_param.upper_bound_ts_ = INT64_MAX;
  scan_param.start_after_stream_id_ = 0;
  scan_param.limit_ = 10;
  ASSERT_EQ(common::OB_SUCCESS, meta_store.scan_streams(scan_param, metas));
  ASSERT_EQ(1, metas.count());
  EXPECT_EQ(stream_id, metas.at(0).stream_id_);

  EXPECT_EQ(common::OB_STATE_NOT_MATCH,
            meta_store.delete_stream(stream_id, meta.modified_ts_ - 1));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.delete_stream(stream_id, INT64_MAX));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(common::OB_SUCCESS, meta_store.delete_stream(stream_id, INT64_MAX));
}

// 已 claim 的过期记录不能被 GC 终结；已有 FINISHED 记录仍是 cleanup 候选。
TEST(TestDirectLoadStreamMetaStore, gc_scan_excludes_claimed_expired_and_includes_finished)
{
  ObMockDirectLoadStreamMetaStore meta_store;
  ObDirectLoadStreamClaimParam claim_param;
  ObDirectLoadStreamMeta meta;
  common::ObSEArray<ObDirectLoadStreamMeta, 4> metas;
  int64_t stream_id = common::OB_INVALID_ID;
  bool finished = false;

  ASSERT_EQ(common::OB_SUCCESS, meta_store.init());
  ASSERT_EQ(common::OB_SUCCESS, create_mock_stream(meta_store, stream_id));
  ASSERT_EQ(common::OB_SUCCESS,
            build_claim_param(stream_id, "127.0.0.1", 2882, 8001, claim_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.claim_stream(claim_param));
  ASSERT_EQ(common::OB_SUCCESS, meta_store.scan_gc_streams(6002, metas));
  EXPECT_EQ(0, metas.count());
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.finish_expired_unclaimed_stream(stream_id, 6002, finished));
  EXPECT_FALSE(finished);
  ASSERT_EQ(common::OB_SUCCESS, meta_store.get_stream(stream_id, meta));
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_OPEN, meta.status_);

  ASSERT_EQ(common::OB_SUCCESS, meta_store.finish_stream(stream_id, common::OB_TIMEOUT));
  ASSERT_EQ(common::OB_SUCCESS,
            meta_store.scan_gc_streams(common::ObTimeUtility::current_time(), metas));
  ASSERT_EQ(1, metas.count());
  EXPECT_EQ(DIRECT_LOAD_STREAM_STATUS_FINISHED, metas.at(0).status_);
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::share::ObTenantBase tenant(oceanbase::common::OB_SYS_TENANT_ID);
  oceanbase::share::ObTenantSwitchGuard tenant_guard(&tenant);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

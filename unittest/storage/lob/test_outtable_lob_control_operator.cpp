/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "storage/lob/store/ob_outtable_lob_control_operator.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/utility/ob_defer.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

static std::string serialize_macro_id(const blocksstable::MacroBlockId &macro_id)
{
  char buf[128];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, macro_id.serialize(buf, sizeof(buf), pos));
  return std::string(buf, static_cast<size_t>(pos));
}

static std::string serialize_macro_id_as_hex(const blocksstable::MacroBlockId &macro_id)
{
  const std::string serialized = serialize_macro_id(macro_id);
  static const char HEX[] = "0123456789ABCDEF";
  std::string result;
  result.reserve(serialized.size() * 2);
  for (size_t i = 0; i < serialized.size(); ++i) {
    const uint8_t byte = static_cast<uint8_t>(serialized[i]);
    result.push_back(HEX[byte >> 4]);
    result.push_back(HEX[byte & 0x0F]);
  }
  return result;
}

class RecordingSQLClient : public ObMySQLProxy
{
public:
  RecordingSQLClient()
    : write_ret_(OB_SUCCESS), write_affected_rows_(1), call_count_(0),
      tenant_id_(OB_INVALID_TENANT_ID), sql_()
  {}

  int read(ReadResult &result, const int64_t, const uint64_t tenant_id, const char *sql) override
  {
    return read_(result, tenant_id, sql);
  }
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql) override
  {
    return read_(result, tenant_id, sql);
  }
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql, const int32_t) override
  {
    return read_(result, tenant_id, sql);
  }
  int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override
  {
    record(tenant_id, sql);
    affected_rows = write_affected_rows_;
    return write_ret_;
  }
  int write(
      const uint64_t tenant_id,
      const char *sql,
      const int32_t,
      int64_t &affected_rows) override
  {
    return write(tenant_id, sql, affected_rows);
  }
  int escape(
      const char *,
      const int64_t,
      char *,
      const int64_t,
      int64_t &) override
  {
    return OB_NOT_SUPPORTED;
  }
  void record(const uint64_t tenant_id, const char *sql)
  {
    ++call_count_;
    tenant_id_ = tenant_id;
    sql_ = nullptr == sql ? "" : sql;
  }

  int read_(ReadResult &read_result, const uint64_t tenant_id, const char *sql)
  {
    record(tenant_id, sql);
    UNUSED(read_result);
    return OB_NOT_SUPPORTED;
  }

  int write_ret_;
  int64_t write_affected_rows_;
  int64_t call_count_;
  uint64_t tenant_id_;
  std::string sql_;
};

struct ControlStatusResultRow
{
  ControlStatusResultRow(const ObOuttableLobControlKey &key, const int64_t status)
    : macro_id_(serialize_macro_id(key.macro_id_)), hash_(key.hash_), status_(status)
  {}
  std::string macro_id_;
  uint64_t hash_;
  int64_t status_;
};

class ControlStatusResult : public sqlclient::ObMySQLResultImpl
{
public:
  ControlStatusResult(sqlclient::ObMySQLStatement &statement,
      const std::vector<ControlStatusResultRow> &rows, int &close_ret)
    : ObMySQLResultImpl(statement), rows_(rows), close_ret_(close_ret), index_(-1)
  {}
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_uint;
  using ObMySQLResultImpl::get_varchar;
  using ObMySQLResultImpl::get_raw;
  int close() override { return close_ret_; }
  int next() override
  {
    return (++index_ < static_cast<int64_t>(rows_.size())) ? OB_SUCCESS : OB_ITER_END;
  }
  int get_int(const char *column, int64_t &value) const override
  {
    EXPECT_STREQ("status", column);
    value = rows_.at(index_).status_;
    return OB_SUCCESS;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    EXPECT_STREQ("hash", column);
    value = rows_.at(index_).hash_;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    EXPECT_STREQ("macro_id", column);
    const std::string &macro_id = rows_.at(index_).macro_id_;
    value.assign_ptr(macro_id.data(), macro_id.size());
    return OB_SUCCESS;
  }
  int get_raw(const char *column, ObString &value) const override
  {
    return get_varchar(column, value);
  }
private:
  const std::vector<ControlStatusResultRow> &rows_;
  int &close_ret_;
  int64_t index_;
};

class ControlStatusResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  ControlStatusResultHandler(std::vector<ControlStatusResultRow> &rows,
      bool &active, int &close_ret)
    : statement_(), result_(statement_, rows, close_ret), active_(active)
  {
    active_ = true;
  }
  ~ControlStatusResultHandler() override { active_ = false; }
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
private:
  sqlclient::ObMySQLStatement statement_;
  ControlStatusResult result_;
  bool &active_;
};

class RecordingSQLConnection : public sqlclient::ObMySQLConnection
{
public:
  RecordingSQLConnection()
    : read_ret_(OB_SUCCESS), close_ret_(OB_SUCCESS), write_ret_(OB_SUCCESS),
      write_affected_rows_(0),
      read_count_(0), write_count_(0), result_active_(false),
      tenant_id_(OB_INVALID_TENANT_ID), sql_(), rows_()
  {}
  using ObMySQLConnection::execute_read;
  int execute_read(const uint64_t tenant_id, const ObString &sql,
      ObISQLClient::ReadResult &result, const bool = false,
      const ObAddr * = nullptr) override
  {
    ++read_count_;
    record_(tenant_id, sql);
    int ret = read_ret_;
    if (OB_SUCCESS == ret) {
      EXPECT_FALSE(result_active_);
      ControlStatusResultHandler *handler = nullptr;
      ret = result.create_handler(handler, rows_, result_active_, close_ret_);
    }
    return ret;
  }
  int execute_write(const uint64_t tenant_id, const ObString &sql,
      int64_t &affected_rows, const bool = false,
      const ObAddr * = nullptr) override
  {
    ++write_count_;
    record_(tenant_id, sql);
    EXPECT_FALSE(result_active_);
    affected_rows = write_affected_rows_;
    return result_active_ ? OB_ERR_UNEXPECTED : write_ret_;
  }
  void record_(const uint64_t tenant_id, const ObString &sql)
  {
    tenant_id_ = tenant_id;
    sql_.assign(sql.ptr(), sql.length());
  }

  int read_ret_;
  int close_ret_;
  int write_ret_;
  int64_t write_affected_rows_;
  int64_t read_count_;
  int64_t write_count_;
  bool result_active_;
  uint64_t tenant_id_;
  std::string sql_;
  std::vector<ControlStatusResultRow> rows_;
};

TEST(TestOuttableLobControlOperator, insert)
{
  const uint64_t tenant_id = 1002;
  const char info_buf[] = {'a', '\0', '\'', 'z'};
  ObOuttableLobControlRow row;
  row.key_.macro_id_ = blocksstable::MacroBlockId(1, 7, 0);
  row.key_.hash_ = UINT64_MAX;
  row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  row.last_modify_time_ = 123456789;
  row.info_.assign_ptr(info_buf, sizeof(info_buf));
  RecordingSQLClient client;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::insert(client, tenant_id, row, affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(1, client.call_count_);
  EXPECT_EQ(tenant_id, client.tenant_id_);
  EXPECT_NE(std::string::npos, client.sql_.find("INSERT INTO __all_outtable_lob_control"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("X'" + serialize_macro_id_as_hex(row.key_.macro_id_) + "'"));
  EXPECT_NE(std::string::npos, client.sql_.find("18446744073709551615"));
  EXPECT_NE(std::string::npos, client.sql_.find("now(6)"));
  EXPECT_NE(std::string::npos, client.sql_.find("X'6100277A'"));

  client.write_affected_rows_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      ObOuttableLobControlOperator::insert(client, tenant_id, row, affected_rows));

  const int64_t old_call_count = client.call_count_;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObOuttableLobControlOperator::insert(client, OB_INVALID_ID, row, affected_rows));
  EXPECT_EQ(old_call_count, client.call_count_);
}

TEST(TestOuttableLobControlOperator, batch_insert_uses_all_keys_and_checks_affected_rows)
{
  const uint64_t tenant_id = 1010;
  ObArray<ObOuttableLobControlKey> keys;
  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(11, 17, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.hash_ = 0;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.macro_id_ = blocksstable::MacroBlockId(12, 18, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  RecordingSQLClient client;
  client.write_affected_rows_ = keys.count();
  int64_t affected_rows = 0;

  for (const int64_t status : {OUTTABLE_LOB_CONTROL_DOING,
      OUTTABLE_LOB_CONTROL_FAIL, OUTTABLE_LOB_CONTROL_COMPLETE}) {
    const int64_t old_call_count = client.call_count_;
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
        client, tenant_id, keys, status, affected_rows));
    EXPECT_EQ(keys.count(), affected_rows);
    EXPECT_EQ(old_call_count + 1, client.call_count_);
    EXPECT_EQ(tenant_id, client.tenant_id_);
    std::string expected_sql = "INSERT INTO __all_outtable_lob_control "
        "(macro_id, hash, status, last_modify_time, info) VALUES ";
    for (int64_t i = 0; i < keys.count(); ++i) {
      expected_sql += (0 == i) ? "(" : ",(";
      expected_sql += "X'" + serialize_macro_id_as_hex(keys.at(i).macro_id_) + "', "
          + std::to_string(keys.at(i).hash_) + ", " + std::to_string(status)
          + ", now(6), NULL)";
    }
    EXPECT_EQ(expected_sql, client.sql_);
    EXPECT_EQ(std::string::npos, client.sql_.find("IGNORE"));
    EXPECT_EQ(std::string::npos, client.sql_.find("ON DUPLICATE KEY"));
  }

  for (const int64_t count : {0, 2, 4}) {
    client.write_affected_rows_ = count;
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlOperator::insert(
        client, tenant_id, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows));
    EXPECT_EQ(count, affected_rows);
  }
  client.write_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, ObOuttableLobControlOperator::insert(
      client, tenant_id, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows));
}

TEST(TestOuttableLobControlOperator, batch_insert_rejects_invalid_input_before_sql)
{
  const uint64_t tenant_id = 1010;
  ObArray<ObOuttableLobControlKey> keys;
  RecordingSQLClient client;
  int64_t affected_rows = 99;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::insert(
      client, tenant_id, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows));
  EXPECT_EQ(0, affected_rows);

  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(11, 17, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  affected_rows = 99;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::insert(
      client, OB_INVALID_ID, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows));
  EXPECT_EQ(0, affected_rows);
  for (const int64_t status : {-1, 0, 4}) {
    affected_rows = 99;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::insert(
        client, tenant_id, keys, status, affected_rows));
    EXPECT_EQ(0, affected_rows);
  }
  key.reset();
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  affected_rows = 99;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::insert(
      client, tenant_id, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows));
  EXPECT_EQ(0, affected_rows);
  EXPECT_EQ(0, client.call_count_);
}

TEST(TestOuttableLobControlManager, batch_insert_validates_before_transaction)
{
  RecordingSQLClient client;
  ObMySQLProxy *saved_sql_proxy = GCTX.sql_proxy_;
  GCTX.sql_proxy_ = &client;
  DEFER({ GCTX.sql_proxy_ = saved_sql_proxy; });
  ObOuttableLobControlManager manager;
  ObArray<ObOuttableLobControlKey> keys;
  EXPECT_EQ(OB_NOT_INIT, manager.insert(keys));
  ASSERT_EQ(OB_SUCCESS, manager.init(1010));
  EXPECT_EQ(OB_SUCCESS, manager.insert(keys));

  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(11, 17, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.reset();
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.insert(keys));
  EXPECT_EQ(0, client.call_count_);
}

TEST(TestOuttableLobControlOperator, update)
{
  const uint64_t tenant_id = 1003;
  const char info_buf[] = {'b', '\0', '\'', static_cast<char>(0xFF)};
  ObOuttableLobControlRow row;
  row.key_.macro_id_ = blocksstable::MacroBlockId(2, 8, 0);
  row.key_.hash_ = UINT64_MAX;
  row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  row.last_modify_time_ = 123456789;
  row.info_.assign_ptr(info_buf, sizeof(info_buf));
  RecordingSQLClient client;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::update(client, tenant_id, row, affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(1, client.call_count_);
  EXPECT_EQ(tenant_id, client.tenant_id_);
  EXPECT_NE(std::string::npos,
      client.sql_.find("UPDATE internal.oceanbase.__all_outtable_lob_control SET"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("macro_id = X'" + serialize_macro_id_as_hex(row.key_.macro_id_) + "'"));
  EXPECT_NE(std::string::npos, client.sql_.find("hash = 18446744073709551615"));
  EXPECT_NE(std::string::npos, client.sql_.find("status = 3"));
  EXPECT_EQ(std::string::npos, client.sql_.find("AND status = 1"));
  EXPECT_NE(std::string::npos, client.sql_.find("last_modify_time = now(6)"));
  EXPECT_NE(std::string::npos, client.sql_.find("info = X'620027FF'"));

  row.info_.reset();
  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::update(client, tenant_id, row, affected_rows));
  EXPECT_NE(std::string::npos, client.sql_.find("info = NULL"));

  const char empty_info[] = "";
  row.info_.assign_ptr(empty_info, 0);
  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::update(client, tenant_id, row, affected_rows));
  EXPECT_NE(std::string::npos, client.sql_.find("info = X''"));

  row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::update(client, tenant_id, row, affected_rows));
  EXPECT_NE(std::string::npos, client.sql_.find("status = 2"));

  client.write_affected_rows_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      ObOuttableLobControlOperator::update(client, tenant_id, row, affected_rows));

  const int64_t old_call_count = client.call_count_;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObOuttableLobControlOperator::update(client, OB_INVALID_ID, row, affected_rows));
  EXPECT_EQ(old_call_count, client.call_count_);
}

TEST(TestOuttableLobControlOperator, delete_row)
{
  const uint64_t tenant_id = 1004;
  const blocksstable::MacroBlockId macro_id(3, 9, 0);
  RecordingSQLClient client;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::delete_row(
      client, tenant_id, macro_id, UINT64_MAX, affected_rows));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(tenant_id, client.tenant_id_);
  EXPECT_NE(std::string::npos, client.sql_.find("DELETE FROM __all_outtable_lob_control WHERE"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("macro_id = X'" + serialize_macro_id_as_hex(macro_id) + "'"));
  EXPECT_NE(std::string::npos, client.sql_.find("hash = 18446744073709551615"));

  client.write_affected_rows_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlOperator::delete_row(
      client, tenant_id, macro_id, UINT64_MAX, affected_rows));

  const int64_t old_call_count = client.call_count_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::delete_row(
      client, OB_INVALID_ID, macro_id, UINT64_MAX, affected_rows));
  EXPECT_EQ(old_call_count, client.call_count_);
}

TEST(TestOuttableLobControlOperator, select_controls_for_update)
{
  using LockMode = ObOuttableLobControlOperator::LockMode;
  const uint64_t tenant_id = 1005;
  const blocksstable::MacroBlockId macro_id(4, 10, 0);
  RecordingSQLClient client;
  ObArenaAllocator allocator;
  ObOuttableLobControlRow row;
  int64_t row_scn = 0;

  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(
      client, tenant_id, macro_id, 43, LockMode::NO_LOCK, allocator, row_scn, row));
  EXPECT_EQ(std::string::npos, client.sql_.find(" FOR UPDATE"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("WHERE hash = 43 AND macro_id = X'"
          + serialize_macro_id_as_hex(macro_id) + "'"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("SELECT ORA_ROWSCN AS row_scn, status, last_modify_time, info FROM"));
  EXPECT_NE(std::string::npos,
      client.sql_.find("FROM internal.oceanbase.__all_outtable_lob_control"));

  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(
      client, tenant_id, macro_id, 43, LockMode::FOR_UPDATE, allocator, row_scn, row));
  EXPECT_NE(std::string::npos, client.sql_.find(" FOR UPDATE"));
  EXPECT_EQ(std::string::npos, client.sql_.find(" NOWAIT"));
  EXPECT_EQ(0, row_scn);

  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(client, tenant_id, macro_id, 43,
      LockMode::FOR_UPDATE_NOWAIT, allocator, row_scn, row));
  EXPECT_NE(std::string::npos, client.sql_.find(" FOR UPDATE NOWAIT"));

  const int64_t call_count = client.call_count_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::select(client, tenant_id, macro_id,
      43, static_cast<LockMode>(-1), allocator, row_scn, row));
  EXPECT_EQ(call_count, client.call_count_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::select(client, tenant_id, macro_id,
      43, static_cast<LockMode>(3), allocator, row_scn, row));
  EXPECT_EQ(call_count, client.call_count_);
}

TEST(TestOuttableLobControlOperator, single_row_connection_uses_internal_catalog)
{
  using LockMode = ObOuttableLobControlOperator::LockMode;
  const uint64_t tenant_id = 1005;
  ObOuttableLobControlRow row;
  row.key_.macro_id_ = blocksstable::MacroBlockId(4, 10, 0);
  row.key_.hash_ = 43;
  row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  RecordingSQLConnection connection;
  connection.read_ret_ = OB_NOT_SUPPORTED;
  connection.write_affected_rows_ = 1;
  ObArenaAllocator allocator;
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  int64_t affected_rows = 0;

  // Qualify the catalog for connections bound to a session using an external catalog.
  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(
      connection, tenant_id, row.key_.macro_id_, row.key_.hash_,
      LockMode::FOR_UPDATE, allocator, row_scn, selected_row));
  EXPECT_NE(std::string::npos,
      connection.sql_.find("FROM internal.oceanbase.__all_outtable_lob_control WHERE"));
  EXPECT_NE(std::string::npos, connection.sql_.find(" FOR UPDATE"));
  EXPECT_EQ(tenant_id, connection.tenant_id_);
  EXPECT_EQ(1, connection.read_count_);

  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(
      connection, tenant_id, row.key_.macro_id_, row.key_.hash_,
      LockMode::NO_LOCK, allocator, row_scn, selected_row));
  EXPECT_EQ(std::string::npos, connection.sql_.find(" FOR UPDATE"));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObOuttableLobControlOperator::select(
      connection, tenant_id, row.key_.macro_id_, row.key_.hash_,
      LockMode::FOR_UPDATE_NOWAIT, allocator, row_scn, selected_row));
  EXPECT_NE(std::string::npos, connection.sql_.find(" FOR UPDATE NOWAIT"));
  EXPECT_EQ(3, connection.read_count_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObOuttableLobControlOperator::select(
      connection, tenant_id, row.key_.macro_id_, row.key_.hash_,
      static_cast<LockMode>(-1), allocator, row_scn, selected_row));
  EXPECT_EQ(3, connection.read_count_);

  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::update(connection, tenant_id, row, affected_rows));
  EXPECT_NE(std::string::npos,
      connection.sql_.find("UPDATE internal.oceanbase.__all_outtable_lob_control SET"));
  EXPECT_EQ(1, affected_rows);
  EXPECT_EQ(tenant_id, connection.tenant_id_);
  EXPECT_EQ(1, connection.write_count_);
}

TEST(TestOuttableLobControlOperator, batch_select_status_matches_complete_primary_keys)
{
  const uint64_t tenant_id = 1007;
  ObArray<ObOuttableLobControlKey> keys;
  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(5, 11, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.hash_ = 0;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.macro_id_ = blocksstable::MacroBlockId(6, 12, 0);
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));

  RecordingSQLConnection connection;
  // SQL row order is unrelated to the requested key order.
  connection.rows_.emplace_back(keys.at(2), OUTTABLE_LOB_CONTROL_COMPLETE);
  connection.rows_.emplace_back(keys.at(0), OUTTABLE_LOB_CONTROL_DOING);
  connection.rows_.emplace_back(keys.at(1), OUTTABLE_LOB_CONTROL_FAIL);
  ObArray<int64_t> statuses;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
  ASSERT_EQ(3, statuses.count());
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_DOING, statuses.at(0));
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_FAIL, statuses.at(1));
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, statuses.at(2));
  EXPECT_EQ(tenant_id, connection.tenant_id_);
  EXPECT_NE(std::string::npos, connection.sql_.find("SELECT macro_id, hash, status"));
  EXPECT_NE(std::string::npos,
      connection.sql_.find("FROM internal.oceanbase.__all_outtable_lob_control"));
  EXPECT_NE(std::string::npos, connection.sql_.find("(macro_id, hash) IN ("));
  EXPECT_NE(std::string::npos, connection.sql_.find("18446744073709551615"));
  EXPECT_NE(std::string::npos, connection.sql_.find(" FOR UPDATE"));
  EXPECT_EQ(std::string::npos, connection.sql_.find("status ="));
  EXPECT_FALSE(connection.result_active_);

  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, false /*for_update*/, statuses));
  EXPECT_EQ(std::string::npos, connection.sql_.find(" FOR UPDATE"));

  connection.close_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
  EXPECT_TRUE(statuses.empty());
  EXPECT_FALSE(connection.result_active_);

  connection.rows_.pop_back();
  // A read/parse failure takes precedence over the close failure.
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
  EXPECT_FALSE(connection.result_active_);
  connection.close_ret_ = OB_SUCCESS;

  connection.rows_.emplace_back(keys.at(0), OUTTABLE_LOB_CONTROL_DOING);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
  EXPECT_FALSE(connection.result_active_);

  connection.rows_.pop_back();
  key.hash_ = 123;
  connection.rows_.emplace_back(key, OUTTABLE_LOB_CONTROL_DOING);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
  EXPECT_FALSE(connection.result_active_);

  connection.read_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, ObOuttableLobControlOperator::select_status(
      connection, tenant_id, keys, true /*for_update*/, statuses));
}

TEST(TestOuttableLobControlOperator, batch_update_status_preserves_info)
{
  const uint64_t tenant_id = 1008;
  ObArray<ObOuttableLobControlKey> keys;
  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(7, 13, 0);
  key.hash_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  key.macro_id_ = blocksstable::MacroBlockId(8, 14, 0);
  key.hash_ = 0;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  RecordingSQLConnection connection;
  connection.write_affected_rows_ = keys.count();
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::update_status(
      connection, tenant_id, keys, OUTTABLE_LOB_CONTROL_COMPLETE, affected_rows));
  EXPECT_EQ(keys.count(), affected_rows);
  EXPECT_EQ(tenant_id, connection.tenant_id_);
  EXPECT_NE(std::string::npos,
      connection.sql_.find("UPDATE internal.oceanbase.__all_outtable_lob_control SET"));
  EXPECT_NE(std::string::npos, connection.sql_.find("status = 3"));
  EXPECT_NE(std::string::npos, connection.sql_.find("last_modify_time = now(6)"));
  EXPECT_EQ(std::string::npos, connection.sql_.find("info"));
  EXPECT_NE(std::string::npos, connection.sql_.find("(macro_id, hash) IN ("));
  const size_t where_pos = connection.sql_.find(" WHERE ");
  ASSERT_NE(std::string::npos, where_pos);
  EXPECT_EQ(std::string::npos, connection.sql_.substr(where_pos).find("status"));
  for (int64_t i = 0; i < keys.count(); ++i) {
    EXPECT_NE(std::string::npos, connection.sql_.find(
        "X'" + serialize_macro_id_as_hex(keys.at(i).macro_id_) + "'"));
  }

  // The Operator reports SQL results without enforcing a business transition/count.
  for (const int64_t count : {0, 1, 3}) {
    connection.write_affected_rows_ = count;
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::update_status(
        connection, tenant_id, keys, OUTTABLE_LOB_CONTROL_FAIL, affected_rows));
    EXPECT_EQ(count, affected_rows);
    EXPECT_NE(std::string::npos, connection.sql_.find("status = 2"));
  }
  connection.write_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, ObOuttableLobControlOperator::update_status(
      connection, tenant_id, keys, OUTTABLE_LOB_CONTROL_COMPLETE, affected_rows));
}

TEST(TestOuttableLobControlManager, batch_complete_validates_state_and_affected_rows)
{
  RecordingSQLClient client;
  ObMySQLProxy *saved_sql_proxy = GCTX.sql_proxy_;
  GCTX.sql_proxy_ = &client;
  DEFER({ GCTX.sql_proxy_ = saved_sql_proxy; });
  RecordingSQLConnection connection;
  ObOuttableLobControlManager manager;
  ObArray<ObOuttableLobControlKey> keys;
  EXPECT_EQ(OB_NOT_INIT, manager.update_complete(connection, keys));
  ASSERT_EQ(OB_SUCCESS, manager.init(1009));
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(connection, keys));
  EXPECT_EQ(0, connection.read_count_);
  EXPECT_EQ(0, connection.write_count_);

  ObOuttableLobControlKey key;
  key.macro_id_ = blocksstable::MacroBlockId(9, 15, 0);
  key.hash_ = 2;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  connection.rows_.emplace_back(key, OUTTABLE_LOB_CONTROL_DOING);
  key.hash_ = 1;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  connection.rows_.emplace_back(key, OUTTABLE_LOB_CONTROL_DOING);
  connection.write_affected_rows_ = 2;
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(connection, keys));
  EXPECT_EQ(1, connection.read_count_);
  EXPECT_EQ(1, connection.write_count_);
  EXPECT_FALSE(connection.result_active_);

  for (const int64_t count : {0, 1, 3}) {
    connection.write_affected_rows_ = count;
    EXPECT_EQ(OB_ERR_UNEXPECTED, manager.update_complete(connection, keys));
    EXPECT_FALSE(connection.result_active_);
  }

  const int64_t old_write_count = connection.write_count_;
  connection.close_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, manager.update_complete(connection, keys));
  EXPECT_EQ(old_write_count, connection.write_count_);
  EXPECT_FALSE(connection.result_active_);
  connection.close_ret_ = OB_SUCCESS;
  for (const int64_t status : {OUTTABLE_LOB_CONTROL_FAIL, OUTTABLE_LOB_CONTROL_COMPLETE}) {
    connection.rows_.at(1).status_ = status;
    EXPECT_EQ(OB_STATE_NOT_MATCH, manager.update_complete(connection, keys));
    EXPECT_EQ(old_write_count, connection.write_count_);
    EXPECT_FALSE(connection.result_active_);
  }
  connection.rows_.pop_back();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, manager.update_complete(connection, keys));
  EXPECT_EQ(old_write_count, connection.write_count_);

  key.reset();
  ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
  const int64_t old_read_count = connection.read_count_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.update_complete(connection, keys));
  EXPECT_EQ(old_read_count, connection.read_count_);
  EXPECT_EQ(old_write_count, connection.write_count_);
}

TEST(TestOuttableLobControlPageIterator, captures_primary_key_upper_bound)
{
  const uint64_t tenant_id = 1006;
  RecordingSQLClient client;
  ObOuttableLobControlPageIterator iterator;
  ObArenaAllocator allocator;
  ObOuttableLobControlRow row;

  EXPECT_EQ(OB_NOT_INIT, iterator.get_next(allocator, row));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iterator.init(client, 0, tenant_id, 2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iterator.init(client, 10 * 1000 * 1000, tenant_id, 0));
  EXPECT_EQ(OB_NOT_SUPPORTED, iterator.init(client, 10 * 1000 * 1000, tenant_id, 2));
  EXPECT_EQ(std::string::npos, client.sql_.find("WHERE status"));
  EXPECT_NE(std::string::npos, client.sql_.find("ORDER BY macro_id DESC, hash DESC LIMIT 1"));
  EXPECT_EQ(std::string::npos, client.sql_.find("MAX("));
  EXPECT_EQ(std::string::npos, client.sql_.find("ORDER BY last_modify_time"));
  EXPECT_EQ(OB_NOT_INIT, iterator.get_next(allocator, row));
}

TEST(TestOuttableLobControlManager, requires_init)
{
  ObOuttableLobControlManager manager;
  ObOuttableLobControlRow row;
  row.key_.macro_id_ = blocksstable::MacroBlockId(4, 10, 0);
  row.key_.hash_ = 43;
  row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ObMySQLTransaction trans;

  EXPECT_EQ(OB_NOT_INIT, manager.insert(row));
  EXPECT_EQ(OB_NOT_INIT, manager.update_complete(trans, row));
  EXPECT_EQ(OB_NOT_INIT, manager.update_fail(row));
  EXPECT_EQ(OB_NOT_INIT, manager.delete_fail_row(trans, row.key_.macro_id_, row.key_.hash_));
  EXPECT_EQ(OB_NOT_INIT,
      manager.delete_complete_row(trans, row.key_.macro_id_, row.key_.hash_, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.init(OB_INVALID_ID));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

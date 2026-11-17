/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <cstdlib>
#include <string>
#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "logservice/ipalf/ipalf_iterator.h"
#include "logservice/ob_log_handler.h"
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
const char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

class TestLobOuttableMetaRedo : public ObSimpleClusterTestBase
{
public:
  TestLobOuttableMetaRedo()
    : ObSimpleClusterTestBase("test_lob_outtable_meta_redo", "10G", "16G", "12G"),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_name_(nullptr)
  {}

  ObMySQLProxy &inner_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  ObMySQLProxy &user_proxy()
  {
    return user_sql_proxy_;
  }

  void init_user_(const char *tenant_name, const char *log_disk_size = "4G");
  void create_lob_table_(const char *table_name);
  void check_lob_(
      ObISQLClient &client,
      const int64_t id,
      const char fill_char,
      const int64_t expected_bytes);
  void check_meta_redo_(const ObLSID &ls_id, const ObTabletID &meta_tablet_id,
      uint64_t piece_table_id, const palf::LSN &start, const palf::LSN &end,
      const ObString &expected_data, int64_t expected_updates, bool is_logging);

  uint64_t tenant_id_;
  const char *table_name_;
  sqlclient::ObSingleMySQLConnectionPool user_sql_pool_;
  ObMySQLProxy user_sql_proxy_;
};

void TestLobOuttableMetaRedo::init_user_(const char *tenant_name, const char *log_disk_size)
{
  int64_t tenant_count = 0;
  {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT COUNT(*) AS cnt FROM oceanbase.__all_tenant "
        "WHERE tenant_name = '%s'", tenant_name));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, inner_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", tenant_count));
  }
  ASSERT_GE(tenant_count, 0);
  ASSERT_LE(tenant_count, 1);
  if (0 == tenant_count) {
    ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(
        tenant_name, "4G", log_disk_size, false, 1, "2G"));
  }
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_, tenant_name));
  ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id_);
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, data_version));
  ASSERT_GE(data_version, DATA_VERSION_4_6_3_1);

  // Set default session variables only for the test tenant before opening a real user connection.
  // Nine blocks exceed the default 16 MiB limit; raise REPEAT's result-size limit first.
  const char *session_defaults[] = {
      "SET GLOBAL tx_isolation = 'REPEATABLE-READ'",
      "SET GLOBAL max_allowed_packet = 67108864",
      "SET GLOBAL ob_query_timeout = 60000000",
      "SET GLOBAL ob_trx_timeout = 120000000",
  };
  int64_t affected_rows = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(session_defaults); ++i) {
    ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_, session_defaults[i], affected_rows));
  }
  if (!user_proxy().is_inited()) {
    // Keep each test's connection pool bound to its own tenant, including full-suite execution.
    ObSqlString user;
    ASSERT_EQ(OB_SUCCESS, user.assign_fmt("root@%s", tenant_name));
    ASSERT_EQ(OB_SUCCESS, user_sql_pool_.set_db_param(user.ptr(), "", "test"));
    ObAddr address;
    ASSERT_TRUE(address.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(),
        get_curr_simple_server().get_mysql_port()));
    ObConnPoolConfigParam pool_config;
    pool_config.sqlclient_wait_timeout_ = 1000;
    pool_config.long_query_timeout_ = 300L * 1000L * 1000L;
    pool_config.connection_refresh_interval_ = 200L * 1000L;
    pool_config.connection_pool_warn_time_ = 10L * 1000L * 1000L;
    pool_config.sqlclient_per_observer_conn_limit_ = 10000;
    ASSERT_EQ(OB_SUCCESS, user_sql_pool_.init(address, pool_config));
    user_sql_pool_.set_mode(sqlclient::ObMySQLConnection::MYSQL_MODE);
    ASSERT_EQ(OB_SUCCESS, user_sql_proxy_.init(&user_sql_pool_));
  }
  {
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, user_proxy().read(result,
        "SELECT @@tx_isolation = 'REPEATABLE-READ' AS is_repeatable_read"));
    sqlclient::ObMySQLResult *rows = result.get_result();
    ASSERT_NE(nullptr, rows);
    ASSERT_EQ(OB_SUCCESS, rows->next());
    int64_t is_repeatable_read = 0;
    ASSERT_EQ(OB_SUCCESS, rows->get_int("is_repeatable_read", is_repeatable_read));
    ASSERT_EQ(1, is_repeatable_read);
    ASSERT_EQ(OB_ITER_END, rows->next());
  }
}

void TestLobOuttableMetaRedo::create_lob_table_(const char *table_name)
{
  table_name_ = table_name;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "CREATE TABLE test.%s (id INT PRIMARY KEY, v LONGBLOB) "
      "LOB_INROW_THRESHOLD = 0 LOB_INTABLE_THRESHOLD = 0", table_name_));
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(sql.ptr(), affected_rows));
}

void TestLobOuttableMetaRedo::check_lob_(
    ObISQLClient &client,
    const int64_t id,
    const char fill_char,
    const int64_t expected_bytes)
{
  const uint64_t sql_tenant_id = (&client == &inner_proxy()) ? tenant_id_ : OB_SYS_TENANT_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT LENGTH(v) AS byte_length, MD5(v) = MD5(REPEAT('%c', %ld)) AS content_matches "
      "FROM test.%s WHERE id = %ld", fill_char, expected_bytes, table_name_, id));
  // LENGTH may read metadata only; MD5 requires reading the entire LOB value.
  // Keep the result local to this function so it is released before reusing the single connection.
  ObMySQLProxy::MySQLResult result;
  ASSERT_EQ(OB_SUCCESS, client.read(result, sql_tenant_id, sql.ptr()));
  sqlclient::ObMySQLResult *rows = result.get_result();
  ASSERT_NE(nullptr, rows);
  ASSERT_EQ(OB_SUCCESS, rows->next());
  int64_t byte_length = 0;
  int64_t content_matches = 0;
  ASSERT_EQ(OB_SUCCESS, rows->get_int("byte_length", byte_length));
  ASSERT_EQ(OB_SUCCESS, rows->get_int("content_matches", content_matches));
  ASSERT_EQ(expected_bytes, byte_length);
  ASSERT_EQ(1, content_matches);
  ASSERT_EQ(OB_ITER_END, rows->next());
}

void TestLobOuttableMetaRedo::check_meta_redo_(
    const ObLSID &ls_id, const ObTabletID &meta_tablet_id, uint64_t piece_table_id,
    const palf::LSN &start, const palf::LSN &end, const ObString &expected_data,
    int64_t expected_updates, bool is_logging)
{
  using namespace transaction;
  using namespace memtable;
  using namespace blocksstable;
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  const int64_t chunk_count = (expected_data.length() + chunk_size - 1) / chunk_size;
  ObSEArray<bool, 16> seen;
  for (int64_t i = 0; i < chunk_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, seen.push_back(false));
  }
  int64_t data_count = 0;
  int64_t update_count = 0;
  int64_t summary_count = 0;
  ipalf::IPalfIterator<ipalf::ILogEntry> iterator;
  ASSERT_EQ(OB_SUCCESS, logservice::seek_log_iterator(ls_id, start, iterator));
  ObArenaAllocator allocator("LobRedoCheck");
  ObArenaAllocator log_allocator("LobRedoLog");
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = iterator.next())) {
    log_allocator.reuse();
    ipalf::ILogEntry entry;
    palf::LSN lsn;
    ASSERT_EQ(OB_SUCCESS, iterator.get_entry(entry, lsn));
    if (lsn >= end) {
      break;
    }
    logservice::ObLogBaseHeader base_header;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, base_header.deserialize(entry.get_data_buf(), entry.get_data_len(), pos));
    if (logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE != base_header.get_log_type()) {
      continue;
    }
    const char *log_buf = entry.get_data_buf();
    int64_t log_size = entry.get_data_len();
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
    if (base_header.is_compressed()) {
      const int64_t base_header_size = pos;
      logservice::LogCompressedPayloadHeader compressed_header;
      int64_t compressed_pos = 0;
      ASSERT_LT(base_header_size, log_size);
      ASSERT_EQ(OB_SUCCESS, compressed_header.deserialize(
          log_buf + base_header_size, log_size - base_header_size, compressed_pos));
      ASSERT_TRUE(compressed_header.is_valid());
      const int64_t payload_size = compressed_header.get_original_len();
      char *decompressed_buf =
          static_cast<char *>(log_allocator.alloc(base_header_size + payload_size));
      ASSERT_NE(nullptr, decompressed_buf);
      int64_t decompressed_size = 0;
      ASSERT_EQ(OB_SUCCESS, logservice::decompress(log_buf + base_header_size,
          log_size - base_header_size, decompressed_buf + base_header_size,
          payload_size, decompressed_size));
      ASSERT_EQ(payload_size, decompressed_size);
      MEMCPY(decompressed_buf, log_buf, base_header_size);
      log_buf = decompressed_buf;
      log_size = base_header_size + decompressed_size;
    }
#else
    ASSERT_FALSE(base_header.is_compressed());
#endif
    ObTxLogBlock block;
    ObTxLogHeader header;
    ASSERT_EQ(OB_SUCCESS, block.init_for_replay(log_buf, log_size));
    int log_ret = OB_SUCCESS;
    while (OB_SUCCESS == (log_ret = block.get_next_log(header))) {
      if (ObTxLogType::TX_LOB_OUTTABLE_DATA_LOG == header.get_tx_log_type()) {
        ObTxLobOutTableDataLog::ConstructArg arg;
        ObTxLobOutTableDataLog old_log(arg);
        ASSERT_EQ(OB_SUCCESS, block.deserialize_log_body(old_log));
        ASSERT_NE(piece_table_id, old_log.get_lob_aux_table_id());
      } else if (ObTxLogType::TX_REDO_LOG == header.get_tx_log_type()) {
        ObTxRedoLogTempRef temp_ref;
        ObTxRedoLog redo(temp_ref);
        ASSERT_EQ(OB_SUCCESS, block.deserialize_log_body(redo));
        ObMemtableMutatorIterator rows;
        ObCLogEncryptInfo encrypt_info;
        ObEncryptRowBuf decrypt_buf;
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, rows.deserialize(redo.get_replay_mutator_buf(),
            redo.get_mutator_size(), pos, encrypt_info));
        int row_ret = OB_SUCCESS;
        while (OB_SUCCESS == (row_ret = rows.iterate_next_row(decrypt_buf, encrypt_info))) {
          if (meta_tablet_id != rows.get_row_head().tablet_id_) {
            continue;
          }
          allocator.reuse();
          const ObMemtableMutatorRow &mutator = rows.get_mutator_row();
          ObRowReader reader;
          const ObRowHeader *row_header = nullptr;
          ObDatumRow row;
          ASSERT_EQ(OB_SUCCESS, reader.read_row_header(
              mutator.new_row_.data_, mutator.new_row_.size_, row_header));
          ASSERT_NE(nullptr, row_header);
          ASSERT_EQ(OB_SUCCESS, row.init(allocator, row_header->get_column_count()));
          ASSERT_EQ(OB_SUCCESS, reader.read_row(
              mutator.new_row_.data_, mutator.new_row_.size_, nullptr, row));
          ObLobMetaInfo meta;
          ObLobMetaExtraInfo extra_info;
          ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_row_to_info(&row, meta, false));
          ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(allocator, meta.lob_extra_info_));
          const ObLobOutTableRedoPayload &payload = rows.get_lob_outtable_payload();
          if (ObLobMetaExtraInfo::LOB_META_SUMMARY == extra_info.extra_info_type_) {
            ++summary_count;
            ObString summary_seq;
            ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, 0, summary_seq));
            EXPECT_EQ(summary_seq, meta.seq_id_);
            EXPECT_EQ(MutatorType::MUTATOR_ROW, rows.get_row_head().mutator_type_);
            EXPECT_FALSE(payload.is_valid());
            EXPECT_EQ(expected_data.length(), meta.byte_len_);
            EXPECT_EQ(chunk_count, extra_info.chunk_count_);
          } else {
            ASSERT_EQ(ObLobMetaExtraInfo::LOB_DATA, extra_info.extra_info_type_);
            ASSERT_EQ(MutatorType::MUTATOR_LOB_OUTTABLE_ROW, rows.get_row_head().mutator_type_);
            ASSERT_TRUE(payload.is_valid());
            EXPECT_EQ(piece_table_id, payload.lob_piece_table_id_);
            EXPECT_EQ(meta.lob_col_idx_, payload.lob_col_idx_);
            EXPECT_EQ(meta.lob_id_, payload.lob_id_);
            EXPECT_EQ(meta.seq_id_, payload.seq_id_);
            ASSERT_EQ(3, mutator.rowkey_.get_obj_cnt());
            const ObObj *key = mutator.rowkey_.get_obj_ptr();
            EXPECT_EQ(key[0].get_int(), payload.lob_col_idx_);
            EXPECT_EQ(key[1].get_string(), payload.lob_id_.get_string());
            EXPECT_EQ(key[2].get_string(), payload.seq_id_);
            MacroBlockId macro_id;
            ASSERT_EQ(OB_SUCCESS,
                ObLobOutTableIO::deserialize_macro_id(meta.lob_block_id_, macro_id));
            EXPECT_EQ(macro_id, payload.macro_id_);
            EXPECT_EQ(meta.byte_len_, payload.byte_len_);
            EXPECT_EQ(is_logging, payload.is_logging_);
            EXPECT_TRUE(extra_info.lob_chunk_type_.out_table());
            int64_t index = OB_INVALID_INDEX;
            for (int64_t i = 0; i < chunk_count; ++i) {
              ObString seq_id;
              ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, i + 1, seq_id));
              if (seq_id == payload.seq_id_) {
                index = i;
              }
            }
            ASSERT_NE(OB_INVALID_INDEX, index);
            ASSERT_FALSE(seen.at(index));
            seen.at(index) = true;
            const int64_t offset = index * chunk_size;
            const int64_t bytes = MIN(chunk_size, expected_data.length() - offset);
            EXPECT_EQ(bytes, payload.byte_len_);
            if (is_logging) {
              EXPECT_EQ(ObString(bytes, expected_data.ptr() + offset), payload.data_);
            } else {
              EXPECT_TRUE(payload.data_.empty());
            }
            ++data_count;
            if (ObDmlFlag::DF_UPDATE == mutator.dml_flag_) {
              ++update_count;
              EXPECT_EQ(0, index);
            } else {
              EXPECT_EQ(ObDmlFlag::DF_INSERT, mutator.dml_flag_);
            }
          }
        }
        ASSERT_EQ(OB_ITER_END, row_ret);
      }
    }
    ASSERT_EQ(OB_ITER_END, log_ret);
  }
  ASSERT_TRUE(OB_SUCCESS == ret || OB_ITER_END == ret);
  EXPECT_EQ(chunk_count, data_count);
  EXPECT_EQ(expected_updates, update_count);
  EXPECT_EQ(1, summary_count);
}

TEST_F(TestLobOuttableMetaRedo, sql_meta_rows_carry_outtable_redo)
{
  ASSERT_NO_FATAL_FAILURE(init_user_("lob_redo_e2e"));
  ASSERT_NO_FATAL_FAILURE(create_lob_table_("lob_meta_redo_e2e"));
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  schema::ObSchemaGetterGuard guard;
  const schema::ObTableSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
      tenant_id_, ObString("test"), ObString(table_name_), false, schema));
  ASSERT_NE(nullptr, schema);
  const uint64_t piece_table_id = schema->get_aux_lob_piece_tid();
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, schema->get_aux_lob_meta_tid(), schema));
  ASSERT_NE(nullptr, schema);
  const ObTabletID meta_tablet_id = schema->get_tablet_id();
  ObTabletIDArray tablet_ids;
  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(meta_tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
      inner_proxy(), tenant_id_, tablet_ids, ls_ids));
  ASSERT_EQ(1, ls_ids.count());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
      MTL(ObLSService *)->get_ls(ls_ids.at(0), ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS &ls = *ls_handle.get_ls();
  omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
  ASSERT_TRUE(config.is_valid());
  const bool saved_no_logging = config->_no_logging;
  DEFER({ config->_no_logging = saved_no_logging; });
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  const int64_t sizes[] = {17, chunk_size + 17, 8 * chunk_size + 17, 17, chunk_size - 17};
  int64_t affected_rows = 0;
  palf::LSN start;
  palf::LSN end;
  ObSqlString sql;
  for (int64_t i = 0; i < ARRAYSIZEOF(sizes); ++i) {
    SCOPED_TRACE(i);
    const bool is_logging = 3 != i;
    config->_no_logging = !is_logging;
    ASSERT_EQ(OB_SUCCESS, ls.get_end_lsn(start));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "INSERT INTO test.%s VALUES (%ld, REPEAT('a', %ld))", table_name_, i + 1, sizes[i]));
    ASSERT_EQ(OB_SUCCESS, user_proxy().write(sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
    ASSERT_EQ(OB_SUCCESS, ls.get_end_lsn(end));
    const std::string expected(sizes[i], 'a');
    ASSERT_NO_FATAL_FAILURE(check_meta_redo_(ls_ids.at(0), meta_tablet_id, piece_table_id,
        start, end, ObString(expected.size(), expected.data()), 0, is_logging));
    ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), i + 1, 'a', sizes[i]));
  }

  // Rewrite a partial tail and insert the next chunk in the same APPEND statement.
  config->_no_logging = false;
  ASSERT_EQ(OB_SUCCESS, ls.get_end_lsn(start));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("UPDATE test.%s "
      "SET v = LOB_WRITEAPPEND(v, 34, REPEAT('b', 34)) WHERE id = 5", table_name_));
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(sql.ptr(), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ls.get_end_lsn(end));
  const std::string expected = std::string(chunk_size - 17, 'a') + std::string(34, 'b');
  ASSERT_NO_FATAL_FAILURE(check_meta_redo_(ls_ids.at(0), meta_tablet_id, piece_table_id,
      start, end, ObString(expected.size(), expected.data()), 1, true));
  {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT MD5(v) = "
        "MD5(CONCAT(REPEAT('a', %ld), REPEAT('b', 34))) AS matches "
        "FROM test.%s WHERE id = 5", chunk_size - 17, table_name_));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, user_proxy().read(result, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    int64_t matches = 0;
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("matches", matches));
    EXPECT_EQ(1, matches);
  }
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_));
  DEFER({ if (trans.is_started()) { EXPECT_EQ(OB_SUCCESS, trans.end(false)); } });
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "INSERT INTO test.%s VALUES (100, REPEAT('s', 17))", table_name_));
  ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, trans.write("SAVEPOINT lob_payload_sp", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "INSERT INTO test.%s VALUES (101, REPEAT('x', %ld))", table_name_, 8 * chunk_size + 17));
  ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, trans.write("ROLLBACK TO SAVEPOINT lob_payload_sp", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "INSERT INTO test.%s VALUES (102, REPEAT('t', 17))", table_name_));
  ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 100, 's', 17));
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 102, 't', 17));
  {
    ObMySQLProxy::MySQLResult rolled_back;
    ASSERT_EQ(OB_SUCCESS,
        user_proxy().read(rolled_back, "SELECT id FROM test.lob_meta_redo_e2e WHERE id = 101"));
    ASSERT_NE(nullptr, rolled_back.get_result());
    EXPECT_EQ(OB_ITER_END, rolled_back.get_result()->next());
  }
  ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "INSERT INTO test.%s VALUES (99, REPEAT('r', %ld))", table_name_, chunk_size + 17));
  ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, trans.end(false));
  ObMySQLProxy::MySQLResult result;
  ASSERT_EQ(OB_SUCCESS,
      user_proxy().read(result, "SELECT id FROM test.lob_meta_redo_e2e WHERE id = 99"));
  ASSERT_NE(nullptr, result.get_result());
  EXPECT_EQ(OB_ITER_END, result.get_result()->next());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (nullptr == s3_bucket || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = nullptr != s3_endpoint_override && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *s3_ak = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *s3_sk = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (nullptr == s3_region || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char storage_info[1000] = {0};
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  int ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (oceanbase::common::OB_SUCCESS == ret) {
    oceanbase::shared_storage_info = storage_info;
    oceanbase::unittest::init_log_and_gtest(argc, argv);
    OB_LOGGER.set_log_level("INFO");
    GCONF.ob_startup_mode.set_value("shared_storage");
    ::testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}

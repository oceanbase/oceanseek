// owner: zk250686
// owner group: transaction

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEBUG_FOR_MDS
#define DEBUG_FOR_MDS
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "lib/allocator/page_arena.h"
#include "share/ob_table_access_helper.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/multi_data_source/test/common_define.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_switch_checker.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/mds_table/ob_ls_mds_tablet_reloc_batch_arg.h"
#include "storage/mds_table/ob_ls_mds_tablet_reloc_mds_helper.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_test_mds_types.h"
#include "unittest/storage/test_tablet_helper.h"
#include "env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
share::SCN mock_tablet_oldest_scn = unittest::mock_scn(1);

namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace mds;

class TestRunCtx
{
public:
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

namespace {

void create_table(const char *table_name)
{
  int64_t affected_rows = 0;
  char sql[512] = {0};
  databuff_printf(sql, sizeof(sql), "create table if not exists %s(a int)", table_name);
  ASSERT_EQ(OB_SUCCESS, GCTX.ddl_sql_proxy_->write(OB_SYS_TENANT_ID, sql, affected_rows));
}

void resolve_tablet_and_ls(const char *table_name, ObTabletID &tablet_id, ObLSID &ls_id)
{
  char where_table[512] = {0};
  databuff_printf(where_table, sizeof(where_table), "where table_name = '%s'", table_name);
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(
      OB_SYS_TENANT_ID, {"tablet_id"}, OB_ALL_TABLE_TNAME, where_table, tablet_id));

  char where_tablet[512] = {0};
  databuff_printf(where_tablet, sizeof(where_tablet), "where tablet_id = %ld", tablet_id.id());
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(
      OB_SYS_TENANT_ID, {"ls_id"}, OB_ALL_TABLET_TO_LS_TNAME, where_tablet, ls_id));
}

void prepare_single_log_mds_for_tablet(ObLS &ls, UtSingleLogMdsWriteCtx &ctx)
{
  ASSERT_EQ(OB_SUCCESS, prepare_single_log_mds_write_ctx_for_test(ls, ctx));
}

void get_ls_and_tablet(const ObTabletID &tablet_id,
                       const ObLSID &ls_id,
                       ObLSHandle &ls_handle,
                       ObTabletHandle &tablet_handle)
{
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
}

// Mirrors former ObITabletMdsInterface::check_transfer_in_redo_written (test-only).
int check_transfer_in_redo_written_for_test(ObTablet &tablet, bool &written)
{
  int ret = OB_SUCCESS;
  bool is_online = false;
  written = false;
  ObTabletCreateDeleteMdsUserData tablet_status;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat = mds::TwoPhaseCommitState::BEFORE_PREPARE;
  share::SCN trans_version;
  if (OB_ISNULL(tablet.get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    bool is_epoch_match = true;
    do {
      is_epoch_match = true;
      ObLSSwitchChecker ls_switch_checker(*tablet.get_tablet_pointer_()->get_ls());
      is_online = ls_switch_checker.check_online();
      if (!is_online) {
        ret = OB_LS_OFFLINE;
      } else if (OB_FAIL(tablet.get_latest_tablet_status(tablet_status, writer, trans_stat, trans_version))) {
      } else if (tablet_status.get_tablet_status() != ObTabletStatus::TRANSFER_IN) {
        ret = OB_STATE_NOT_MATCH;
      } else if (trans_stat != mds::TwoPhaseCommitState::ON_COMMIT) {
      } else if (!trans_version.is_valid() || trans_version.is_max()) {
      } else {
        written = true;
      }
      if (OB_SUCC(ret)) {
        is_online = ls_switch_checker.double_check_online(is_epoch_match);
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        if (!is_epoch_match) {
          LOG_WARN("ls switch epoch changed", K(ret), K(is_online));
        }
      }
    } while (!is_epoch_match && is_online);
  }
  return ret;
}

void wait_ls_mds_checkpoint(ObLS *ls, const int max_rounds = 60, const int sleep_sec = 5)
{
  ASSERT_NE(nullptr, ls);
  ASSERT_NE(nullptr, ls->get_mds_table());
  share::SCN ckpt;
  for (int i = 0; i < max_rounds; ++i) {
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_checkpoint_scn(ckpt));
    if (!ckpt.is_min()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
  }
  ASSERT_FALSE(ckpt.is_min()) << "LS aggregate MDS checkpoint did not advance";
}

void serialize_test_mds_register_arg(char *buffer,
                                     const int64_t buf_len,
                                     int64_t &pos,
                                     const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id,
                                     const int64_t key,
                                     const int64_t value,
                                     const bool check_dup = true)
{
  mds::ObTestMdsRegisterArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.key_ = key;
  arg.value_ = value;
  arg.check_dup_ = check_dup;
  ASSERT_TRUE(arg.is_valid());
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, buf_len, pos));
}

void register_test_mds_in_tx(observer::ObInnerSQLConnection *conn,
                             const uint64_t tenant_id,
                             const share::ObLSID &ls_id,
                             const ObTabletID &tablet_id,
                             const ObTxDataSourceType type,
                             const int64_t key,
                             const int64_t value,
                             const bool check_dup = true)
{
  constexpr int64_t BUF_LEN = 256;
  char buffer[BUF_LEN];
  int64_t pos = 0;
  serialize_test_mds_register_arg(buffer, BUF_LEN, pos, ls_id, tablet_id, key, value, check_dup);
  ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(tenant_id, ls_id, type, buffer, pos));
}

void register_test_tablet_binding_in_tx(observer::ObInnerSQLConnection *conn,
                                        const uint64_t tenant_id,
                                        const share::ObLSID &ls_id,
                                        const ObTabletID &tablet_id,
                                        const ObTabletBindingMdsUserData &data)
{
  ObTabletBindingMdsArg arg;
  arg.tenant_id_ = tenant_id;
  arg.ls_id_ = ls_id;
  ASSERT_EQ(OB_SUCCESS, arg.tablet_ids_.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, arg.binding_datas_.push_back(data));
  ASSERT_TRUE(arg.is_valid());
  constexpr int64_t BUF_LEN = 512;
  char buffer[BUF_LEN];
  int64_t pos = 0;
  ASSERT_LT(arg.get_serialize_size(), BUF_LEN);
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, BUF_LEN, pos));
  ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
      tenant_id, ls_id, ObTxDataSourceType::TABLET_BINDING, buffer, pos));
}

void serialize_empty_final_reloc_batch(char *buffer,
                                       const int64_t buf_len,
                                       int64_t &pos,
                                       const share::ObLSID &ls_id,
                                       const ObTabletID &tablet_id,
                                       const share::SCN &transfer_scn)
{
  ObLsMdsTabletRelocBatchArg arg;
  ObLsMdsTabletRelocTarget target;
  arg.ls_id_ = ls_id;
  arg.reloc_type_ = ObLsMdsTabletRelocType::TRANSFER;
  target.tablet_id_ = tablet_id;
  target.transfer_scn_ = transfer_scn;
  ASSERT_EQ(OB_SUCCESS, arg.targets_.push_back(target));
  arg.is_final_ = true;
  ASSERT_TRUE(arg.is_valid());

  ASSERT_LT(arg.get_serialize_size(), buf_len);
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, buf_len, pos));
}

void register_empty_final_reloc_in_tx(observer::ObInnerSQLConnection *conn,
                                      const uint64_t tenant_id,
                                      const share::ObLSID &ls_id,
                                      const ObTabletID &tablet_id,
                                      const share::SCN &transfer_scn)
{
  constexpr int64_t BUF_LEN = 1024;
  char buffer[BUF_LEN] = {};
  int64_t pos = 0;
  serialize_empty_final_reloc_batch(
      buffer, BUF_LEN, pos, ls_id, tablet_id, transfer_scn);
  ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
      tenant_id, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, buffer, pos));
}

void write_committed_transfer_in_status(ObLS &ls,
                                        ObTablet &tablet,
                                        const ObTabletID &tablet_id,
                                        const share::SCN &row_transfer_scn,
                                        const share::SCN &user_data_transfer_scn,
                                        const transaction::ObTransID &tx_id)
{
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_IN;
  user_data.transfer_scn_ = user_data_transfer_scn;
  user_data.transfer_ls_id_ = ls.get_ls_id();
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;

  UtMemtableMdsWriteCtx ctx(tx_id);
  ASSERT_EQ(OB_SUCCESS,
      prepare_memtable_mds_write_ctx_for_test(ls, tablet, tablet_id, ctx));
  ASSERT_NE(nullptr, ls.get_mds_table());
  ASSERT_NE(nullptr, ctx.mds_ctx().get_store_ctx());
  ASSERT_EQ(OB_SUCCESS, ls.get_mds_table()->write(
      tablet_id,
      row_transfer_scn,
      user_data,
      *ctx.mds_ctx().get_store_ctx(),
      ctx.mds_ctx()));
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, ls.get_max_decided_scn(commit_scn));
  ASSERT_TRUE(commit_scn.is_valid());
  ctx.commit(commit_scn);
}

void check_committed_reloc_marker(ObLS &ls,
                                  const ObTabletID &tablet_id,
                                  const share::SCN &row_transfer_scn,
                                  const share::SCN &expected_user_data_transfer_scn,
                                  const bool expected_done)
{
  common::ObArenaAllocator allocator;
  ObTabletCreateDeleteMdsUserData user_data;
  ObMdsValueMeta meta;
  ASSERT_NE(nullptr, ls.get_mds_table());
  // tablet status 行保持不变：reloc final marker 不再叠加 status 版本
  ASSERT_EQ(OB_SUCCESS, ls.get_mds_table()->get<ObTabletCreateDeleteMdsUserData>(
      &allocator,
      tablet_id,
      row_transfer_scn,
      share::SCN::max_scn(),
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      false /*read_uncommitted*/,
      user_data,
      meta));
  EXPECT_EQ(ObTabletStatus::TRANSFER_IN, user_data.tablet_status_);
  EXPECT_EQ(expected_user_data_transfer_scn, user_data.transfer_scn_);

  // marker 位于独立的 ObTabletMdsRelocStatus unit;
  // 行不存在(OB_EMPTY_RESULT)即为未完成
  ObTabletMdsRelocStatus reloc_done_data;
  ObMdsValueMeta marker_meta;
  const int marker_ret = ls.get_mds_table()->get<ObTabletMdsRelocStatus>(
      &allocator,
      tablet_id,
      row_transfer_scn,
      share::SCN::max_scn(),
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      false /*read_uncommitted*/,
      reloc_done_data,
      marker_meta);
  if (expected_done) {
    ASSERT_EQ(OB_SUCCESS, marker_ret);
    EXPECT_TRUE(reloc_done_data.reloc_done_);
  } else {
    // 行不存在时底层可能返回 ITER_END / EMPTY_RESULT / SNAPSHOT_DISCARDED
    EXPECT_TRUE(OB_EMPTY_RESULT == marker_ret
        || OB_ITER_END == marker_ret
        || OB_SNAPSHOT_DISCARDED == marker_ret
        || (OB_SUCCESS == marker_ret && !reloc_done_data.reloc_done_));
  }
}

void register_test_single_and_multi_row_in_tx(observer::ObInnerSQLConnection *conn,
                                              const uint64_t tenant_id,
                                              const share::ObLSID &ls_id,
                                              const ObTabletID &tablet_id,
                                              const int64_t single_row_value,
                                              const int64_t multi_row_key,
                                              const int64_t multi_row_value)
{
  register_test_mds_in_tx(conn, tenant_id, ls_id, tablet_id,
      ObTxDataSourceType::TEST_SINGLE_ROW, 0, single_row_value);
  register_test_mds_in_tx(conn, tenant_id, ls_id, tablet_id,
      ObTxDataSourceType::TEST_MULTI_ROW, multi_row_key, multi_row_value);
}

void read_single_row_get_latest(ObTablet &tablet,
                                const int64_t expected_value,
                                const int expect_ret,
                                const bool expect_committed = true,
                                mds::TwoPhaseCommitState *out_stat = nullptr)
{
  mds::ObTestMdsSingleRowValue read_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat = mds::TwoPhaseCommitState::STATE_INIT;
  share::SCN trans_version;
  const int ret = tablet.get_latest(read_data, writer, trans_stat, trans_version);
  ASSERT_EQ(expect_ret, ret);
  if (OB_SUCCESS == expect_ret) {
    EXPECT_EQ(expected_value, read_data.value_);
    if (nullptr != out_stat) {
      *out_stat = trans_stat;
    }
    if (expect_committed) {
      EXPECT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
    } else {
      EXPECT_NE(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
    }
  }
}

void read_single_row_get_snapshot(ObTablet &tablet,
                                  const int64_t expected_value,
                                  const int expect_ret)
{
  mds::ObTestMdsSingleRowValue read_data;
  const share::SCN read_snapshot = share::SCN::max_scn();
  const int ret = tablet.get_snapshot(read_data, read_snapshot, 1_s);
  ASSERT_EQ(expect_ret, ret);
  if (OB_SUCCESS == expect_ret) {
    EXPECT_EQ(expected_value, read_data.value_);
  }
}

void read_single_row_get_latest_committed(ObTablet &tablet,
                                          const int64_t expected_value,
                                          const int expect_ret)
{
  mds::ObTestMdsSingleRowValue read_data;
  const int ret = tablet.get_latest_committed(read_data);
  ASSERT_EQ(expect_ret, ret);
  if (OB_SUCCESS == expect_ret) {
    EXPECT_EQ(expected_value, read_data.value_);
  }
}

void read_multi_row_key_get_latest(ObTablet &tablet,
                                   const ObTestMdsMultiRowKey &key,
                                   const int64_t expected_value,
                                   const int expect_ret,
                                   const bool expect_committed)
{
  ObTestMdsMultiRowValue read_data;
  MdsWriter writer;
  TwoPhaseCommitState trans_stat = TwoPhaseCommitState::STATE_INIT;
  share::SCN trans_version;
  const int ret = tablet.mds_get_row_(
      key,
      share::SCN::max_scn(),
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      true /*read_uncommitted*/,
      read_data,
      nullptr,
      writer,
      trans_stat,
      trans_version);
  ASSERT_EQ(expect_ret, ret);
  if (OB_SUCCESS == expect_ret) {
    EXPECT_EQ(expected_value, read_data.value_);
    if (expect_committed) {
      EXPECT_EQ(TwoPhaseCommitState::ON_COMMIT, trans_stat);
    } else {
      EXPECT_NE(TwoPhaseCommitState::ON_COMMIT, trans_stat);
    }
  }
}

void acquire_inner_sql_connection(observer::ObInnerSQLConnection *&inner_conn)
{
  common::ObMySQLProxy *sql_proxy = GCTX.ddl_sql_proxy_;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy->acquire(connection));
  inner_conn = dynamic_cast<observer::ObInnerSQLConnection *>(connection);
  ASSERT_NE(nullptr, inner_conn);
}

} // namespace

class TestMdsTransaction : public ObSimpleClusterTestBase
{
public:
  TestMdsTransaction() : ObSimpleClusterTestBase("test_mds_transaction_") {}
  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    OB_LOGGER.set_log_level("INFO");
  }
  void TearDown() override
  {
    OB_LOGGER.set_log_level("WDIAG");
    ObSimpleClusterTestBase::TearDown();
  }
};

TEST_F(TestMdsTransaction, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(TestMdsTransaction, register_mds_on_sys_tenant)
{
  static const char *TABLE = "test_mds_reg_sys";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    observer::ObInnerSQLConnection *inner_conn = nullptr;
    acquire_inner_sql_connection(inner_conn);
    ASSERT_EQ(OB_SUCCESS, inner_conn->start_transaction(OB_SYS_TENANT_ID));
    register_test_mds_in_tx(inner_conn, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_SINGLE_ROW, 0, 12345);
    ASSERT_EQ(OB_SUCCESS, inner_conn->commit());
  }
}

TEST_F(TestMdsTransaction, tablet_set_multi_key_uncommitted_overwrite)
{
  static const char *TABLE = "test_mds_tx_multi_row";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    common::ObMySQLTransaction trans1;
    ASSERT_EQ(OB_SUCCESS, trans1.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn1 =
        static_cast<observer::ObInnerSQLConnection *>(trans1.get_connection());
    register_test_mds_in_tx(conn1, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 1, 1);
    register_test_mds_in_tx(conn1, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 3, 3);
    ASSERT_EQ(OB_SUCCESS, trans1.end(true));

    common::ObMySQLTransaction trans2;
    ASSERT_EQ(OB_SUCCESS, trans2.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn2 =
        static_cast<observer::ObInnerSQLConnection *>(trans2.get_connection());
    register_test_mds_in_tx(conn2, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 1, 11, false /*check_dup: overwrite*/);
    register_test_mds_in_tx(conn2, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 2, 2);

    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(1), 11, OB_SUCCESS, false);
    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(2), 2, OB_SUCCESS, false);
    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(3), 3, OB_SUCCESS, true);

    ASSERT_EQ(OB_SUCCESS, trans2.end(true));

    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(1), 11, OB_SUCCESS, true);
    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(2), 2, OB_SUCCESS, true);
    read_multi_row_key_get_latest(*tablet, ObTestMdsMultiRowKey(3), 3, OB_SUCCESS, true);
  }
}

TEST_F(TestMdsTransaction, tablet_set_multi_key_read_committed)
{
  static const char *TABLE = "test_mds_tx_multi_row_read";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);

    common::ObMySQLTransaction trans1;
    ASSERT_EQ(OB_SUCCESS, trans1.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn1 =
        static_cast<observer::ObInnerSQLConnection *>(trans1.get_connection());
    register_test_mds_in_tx(conn1, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 1, 1);
    register_test_mds_in_tx(conn1, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 3, 3);
    ASSERT_EQ(OB_SUCCESS, trans1.end(true));

    common::ObMySQLTransaction trans2;
    ASSERT_EQ(OB_SUCCESS, trans2.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn2 =
        static_cast<observer::ObInnerSQLConnection *>(trans2.get_connection());
    register_test_mds_in_tx(conn2, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 1, 11, false /*check_dup: overwrite*/);
    register_test_mds_in_tx(conn2, OB_SYS_TENANT_ID, ls_id, tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 2, 2);

    ObTestMdsMultiRowValue read_1;
    ObTestMdsMultiRowValue read_3;
    const share::SCN read_snapshot = share::SCN::max_scn();
    ASSERT_EQ(OB_SUCCESS, tablet->get_snapshot(
        ObTestMdsMultiRowKey(1), read_1, read_snapshot, 1_s));
    ASSERT_EQ(OB_SUCCESS, tablet->get_snapshot(
        ObTestMdsMultiRowKey(3), read_3, read_snapshot, 1_s));
    EXPECT_EQ(1, read_1.value_);
    EXPECT_EQ(3, read_3.value_);

    ObTestMdsMultiRowValue read_2;
    EXPECT_EQ(OB_EMPTY_RESULT, tablet->get_snapshot(
        ObTestMdsMultiRowKey(2), read_2, read_snapshot, 1_s));

    ASSERT_EQ(OB_SUCCESS, trans2.end(false));
  }
}

TEST_F(TestMdsTransaction, tablet_binding_flush_checkpoint_and_read)
{
  static const char *TABLE = "test_mds_tx_binding";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    ObTabletBindingMdsUserData data_to_write;
    data_to_write.schema_version_ = 1;
    data_to_write.snapshot_version_ = 1;
    data_to_write.data_tablet_id_ = ObTabletID(2);
    data_to_write.hidden_tablet_id_ = ObTabletID(3);
    data_to_write.lob_meta_tablet_id_ = ObTabletID(4);
    data_to_write.lob_piece_tablet_id_ = ObTabletID(5);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    register_test_tablet_binding_in_tx(
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id, data_to_write);
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->flush_mds_table());
    wait_ls_mds_checkpoint(ls_handle.get_ls());

    mock_tablet_oldest_scn = mock_scn(2074916885902668817);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    ObTabletBindingMdsUserData read_data;
    ASSERT_EQ(OB_SUCCESS, tablet->get_snapshot(read_data, share::SCN::max_scn(), 1_s));
    EXPECT_EQ(data_to_write.schema_version_, read_data.schema_version_);
    mock_tablet_oldest_scn = mock_scn(1000);
  }
}

TEST_F(TestMdsTransaction, tablet_tx_single_row_visible_after_multi_register)
{
  static const char *TABLE = "test_mds_tx_single_row";
  static const int64_t SINGLE_ROW_VALUE = 8800;
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    register_test_single_and_multi_row_in_tx(
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id,
        SINGLE_ROW_VALUE, /*multi_row_key*/1, /*multi_row_value*/8801);

    mds::TwoPhaseCommitState trans_stat = mds::TwoPhaseCommitState::STATE_INIT;
    read_single_row_get_latest(
        *tablet, SINGLE_ROW_VALUE, OB_SUCCESS, false /*expect_committed*/, &trans_stat);
    read_single_row_get_snapshot(*tablet, SINGLE_ROW_VALUE, OB_EMPTY_RESULT);
    read_single_row_get_latest_committed(*tablet, SINGLE_ROW_VALUE, OB_EMPTY_RESULT);

    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    read_single_row_get_latest(*tablet, SINGLE_ROW_VALUE, OB_SUCCESS, true /*expect_committed*/);
    read_single_row_get_snapshot(*tablet, SINGLE_ROW_VALUE, OB_SUCCESS);
    read_single_row_get_latest_committed(*tablet, SINGLE_ROW_VALUE, OB_SUCCESS);
  }
}

TEST_F(TestMdsTransaction, tablet_tx_single_row_invisible_after_rollback)
{
  static const char *TABLE = "test_mds_tx_single_row_rollback";
  static const int64_t SINGLE_ROW_VALUE = 9900;
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    register_test_single_and_multi_row_in_tx(
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id,
        SINGLE_ROW_VALUE, /*multi_row_key*/1, /*multi_row_value*/9901);
    read_single_row_get_latest(
        *tablet, SINGLE_ROW_VALUE, OB_SUCCESS, false /*expect_committed*/);

    ASSERT_EQ(OB_SUCCESS, trans.end(false));

    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    read_single_row_get_latest(*tablet, SINGLE_ROW_VALUE, OB_EMPTY_RESULT);
    read_single_row_get_snapshot(*tablet, SINGLE_ROW_VALUE, OB_EMPTY_RESULT);
    read_single_row_get_latest_committed(*tablet, SINGLE_ROW_VALUE, OB_EMPTY_RESULT);
  }
}

TEST_F(TestMdsTransaction, tablet_transfer_in_redo_written)
{
  static const char *TABLE = "test_mds_tx_transfer_in";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    bool written = false;
    ObTabletCreateDeleteMdsUserData data_to_write;
    data_to_write.tablet_status_ = ObTabletStatus::TRANSFER_IN;
    data_to_write.create_commit_scn_ = mock_scn(10);
    data_to_write.create_commit_version_ = 10;
    data_to_write.transfer_ls_id_ = ObLSID(10);
    data_to_write.transfer_scn_ = mock_scn(10);

    EXPECT_EQ(OB_STATE_NOT_MATCH, check_transfer_in_redo_written_for_test(*tablet, written));

    UtSingleLogMdsWriteCtx ctx(ObTransID(301));
    prepare_single_log_mds_for_tablet(*ls, ctx);
    ASSERT_EQ(OB_SUCCESS, tablet->set(data_to_write, ctx));
    ASSERT_EQ(OB_SUCCESS, check_transfer_in_redo_written_for_test(*tablet, written));
    EXPECT_FALSE(written);

    share::SCN max_decided_scn;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_max_decided_scn(max_decided_scn));
    ctx.commit(max_decided_scn);

    ASSERT_EQ(OB_SUCCESS, check_transfer_in_redo_written_for_test(*tablet, written));
    EXPECT_TRUE(written);
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_empty_final_sets_marker)
{
  static const char *TABLE = "test_mds_reloc_final";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);

    const share::SCN transfer_scn = mock_scn(1100);
    write_committed_transfer_in_status(
        *ls,
        *tablet,
        tablet_id,
        transfer_scn,
        transfer_scn,
        ObTransID(401));
    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    ASSERT_NE(nullptr, conn);
    register_empty_final_reloc_in_tx(
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id, transfer_scn);
    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, true);
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_final_replay_skips_checkpointed_scn)
{
  static const char *TABLE = "test_mds_reloc_ckpt";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(TABLE);
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls(TABLE, tablet_id, ls_id);

    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObTablet *tablet = tablet_handle.get_obj();
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, tablet);
    ASSERT_NE(nullptr, ls);
    ASSERT_NE(nullptr, ls->get_mds_table());

    const share::SCN transfer_scn = mock_scn(1300);
    write_committed_transfer_in_status(
        *ls,
        *tablet,
        tablet_id,
        transfer_scn,
        transfer_scn,
        ObTransID(403));
    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);

    share::SCN checkpoint_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_checkpoint_scn(checkpoint_scn));
    ASSERT_TRUE(checkpoint_scn.is_valid());
    constexpr int64_t BUF_LEN = 1024;
    char buffer[BUF_LEN] = {};
    int64_t pos = 0;
    serialize_empty_final_reloc_batch(
        buffer, BUF_LEN, pos, ls_id, tablet_id, transfer_scn);

    UtSingleLogMdsWriteCtx replay_ctx(
        MdsWriter(WriterType::MEDIUM_INFO, 404));
    ASSERT_EQ(OB_SUCCESS,
        replay_ctx.single_log_ctx().init(ls_id, INT64_MAX, true /*is_replay*/));
    ASSERT_EQ(OB_SUCCESS, ObLsMdsTabletRelocMdsHelper::on_replay(
        buffer, pos, checkpoint_scn, replay_ctx.mds_ctx()));
    replay_ctx.abort();

    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);
  }
}

TEST_F(TestMdsTransaction, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char *)"WDIAG";
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
      log_level = optarg;
      oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
      break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif

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
#include "share/ob_cluster_version.h"
#include "share/transfer/ob_transfer_info.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "logservice/ob_log_handler.h"
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif
#include "storage/tx/ob_tx_log.h"
#include "share/ob_upgrade_utils.h"
#include "storage/ddl/ob_tablet_fork_task.h"
#include "storage/ddl/ob_tablet_fork_util.h"
#include "storage/ddl/ob_tablet_split_task.h"
#include "storage/multi_data_source/test/common_define.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_switch_checker.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/mds_table/ob_tablet_mds_reloc_batch_arg.h"
#include "storage/mds_table/ob_tablet_mds_logical_reloc.h"
#include "storage/mds_table/ob_tablet_mds_reloc_helper.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
#include "storage/mds_table/ob_test_mds_types.h"
#include "storage/high_availability/ob_storage_ha_service.h"
#include "storage/high_availability/ob_ss_transfer_backfill_utils.h"
#include "storage/high_availability/ob_ss_transfer_backfill_tx.h"
#include "storage/high_availability/ob_ss_storage_ha_reader.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "unittest/storage/test_tablet_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "storage/incremental/ob_sswriter_service.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
const char *shared_storage_info = nullptr;
share::SCN mock_tablet_oldest_scn = unittest::mock_scn(1);

namespace storage
{
// Match the single-server SS metadata fixtures: the test server is the sole
// writer. Reloc, flush, upload and checkpoint reads below are not mocked.
int ObSSWriterService::check_lease(const ObSSWriterKey &key, bool &is_sswriter, int64_t &epoch)
{
  UNUSED(key);
  is_sswriter = true;
  epoch = 1;
  return common::OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key, ObSSWriterAddr &sswriter_addr, const int64_t timeout_us)
{
  UNUSED(key);
  UNUSED(timeout_us);
  sswriter_addr.addr_ = GCTX.self_addr();
  sswriter_addr.epoch_ = 1;
  return common::OB_SUCCESS;
}
} // namespace storage

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

// V1 reader layout from MR base 254d9866c151. Keep the old target and batch
// decoders independent of the V2 implementation to catch wire-layout drift.
struct LegacyRelocTarget
{
  OB_UNIS_VERSION(1);
public:
  ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  TO_STRING_KV(K_(tablet_id), K_(transfer_scn));
};
OB_SERIALIZE_MEMBER(LegacyRelocTarget, tablet_id_, transfer_scn_);

struct LegacyRelocBatch
{
  OB_UNIS_VERSION(1);
public:
  int64_t version_ = 0;
  ObTabletMdsRelocType reloc_type_ = ObTabletMdsRelocType::INVALID;
  ObLSID ls_id_;
  ObSEArray<LegacyRelocTarget, 4> targets_;
  ObSEArray<ObTabletMdsRelocRowStorage, 4> rows_;
  ObTabletMdsRelocSkipBitmap skip_bitmap_;
  bool is_final_ = false;
};
OB_DEF_DESERIALIZE(LegacyRelocBatch)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, version_, reloc_type_, ls_id_, targets_, rows_);
  skip_bitmap_.reset();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(skip_bitmap_.deserialize(buf, data_len, pos, rows_.count(), targets_.count()))) {
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_final_))) {
    }
  }
  return ret;
}

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
                                       const share::SCN &transfer_scn,
                                       const share::SCN &src_reorg_scn,
                                       const bool use_reloc_v2)
{
  ObTabletMdsRelocBatchArg arg;
  ObTabletMdsRelocTarget target;
  arg.ls_id_ = ls_id;
  arg.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
  target.tablet_id_ = tablet_id;
  target.transfer_scn_ = transfer_scn;
  if (use_reloc_v2) {
    arg.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
    arg.src_ls_id_ = ls_id;
    arg.read_snapshot_ = share::SCN::max_scn();
    target.src_tablet_id_ = tablet_id;
    target.src_reorg_scn_ = src_reorg_scn;
  }
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
                                      const share::SCN &transfer_scn,
                                      const share::SCN &src_reorg_scn,
                                      const bool use_reloc_v2)
{
  constexpr int64_t BUF_LEN = 1024;
  char buffer[BUF_LEN] = {};
  int64_t pos = 0;
  serialize_empty_final_reloc_batch(
      buffer, BUF_LEN, pos, ls_id, tablet_id, transfer_scn, src_reorg_scn, use_reloc_v2);
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
    EXPECT_TRUE(reloc_done_data.is_reloc_done());
  } else {
    // 行不存在时底层可能返回 ITER_END / EMPTY_RESULT / SNAPSHOT_DISCARDED
    EXPECT_TRUE(OB_EMPTY_RESULT == marker_ret
        || OB_ITER_END == marker_ret
        || OB_SNAPSHOT_DISCARDED == marker_ret
        || (OB_SUCCESS == marker_ret && !reloc_done_data.is_reloc_done()));
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
                                   const bool expect_committed,
                                   share::SCN *trans_version_out = nullptr)
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
    if (nullptr != trans_version_out) {
      *trans_version_out = trans_version;
    }
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

void wait_ls_mds_checkpoint_reach(ObLS *ls,
                                  const share::SCN &target_scn,
                                  const int max_rounds = 60,
                                  const int sleep_sec = 1);

void get_ss_ls_mds_clog_checkpoint_scn(ObLS *ls, share::SCN &checkpoint_scn);

void wait_ss_ls_mds_checkpoint_reach(ObLS *ls,
                                     const share::SCN &target_scn,
                                     const int max_rounds = 60,
                                     const int sleep_sec = 1);

// V2 op-semantic marker batch: the empty non-final form acquires the marker
// mutex; the empty final form pulls op rows and updates the marker redo fence.
void serialize_v2_reloc_marker_batch(char *buffer,
                                     const int64_t buf_len,
                                     int64_t &pos,
                                     const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id,
                                     const share::SCN &transfer_scn,
                                     const share::ObLSID &src_ls_id,
                                     const ObTabletID &src_tablet_id,
                                     const share::SCN &src_reorg_scn,
                                     const bool is_final,
                                     const ObTabletMdsRelocType reloc_type = ObTabletMdsRelocType::TRANSFER);

class TestMdsTransaction : public ObSimpleClusterTestBase
{
public:
  TestMdsTransaction() : ObSimpleClusterTestBase("test_mds_transaction_") {}
  static void TearDownTestCase()
  {
    if (nullptr != shared_storage_info) {
      ResidualDataCleanerHelper::clean_in_mock_env();
    }
    ObSimpleClusterTestBase::TearDownTestCase();
  }
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

  // Shared-storage flush of LS_MDS_TABLET is disabled while the inc-sstable
  // uploader reloads the LS (it re-registers the tablet's sstables right after
  // bootstrap in farm and locally). Writing the reloc marker or driving an MDS
  // checkpoint inside that window races the reload, so the SS persistence cases
  // wait for the uploader to release the flush lock first.
  static void wait_ss_inc_uploader_ready(ObLS *ls,
                                         const int max_rounds = 180,
                                         const int sleep_sec = 1)
  {
    ASSERT_NE(nullptr, ls);
    bool ready = false;
    for (int i = 0; i < max_rounds && !ready; ++i) {
      ready = !ls->flush_is_disabled();
      if (!ready) {
        std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
      }
    }
    ASSERT_TRUE(ready) << "shared-storage inc-sstable uploader kept the LS flush disabled";
  }

  // ObMdsTable::flush() returns OB_EAGAIN while the uploader holds the flush
  // lock. A transient disable window between the readiness check and the call
  // must not fail the case, so retry within a bounded window.
  static void flush_ss_mds_table(ObLS *ls,
                                 const int max_rounds = 120,
                                 const int sleep_sec = 1)
  {
    ASSERT_NE(nullptr, ls);
    int ret = OB_EAGAIN;
    for (int i = 0; OB_EAGAIN == ret && i < max_rounds; ++i) {
      if (ls->flush_is_disabled()) {
        std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
      } else {
        ret = ls->flush_mds_table();
        if (OB_EAGAIN == ret) {
          std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret) << "shared-storage MDS flush stayed disabled while the uploader reloaded";
  }
private:
  friend class TestMdsTransaction_transfer_submit_uses_persisted_task_version_Test;
  friend class TestMdsTransaction_reloc_v2_multibatch_medium_redo_fence_Test;

  // Decode the actual committed log interval surrounding a production submit.
  // Copy payloads out of the iterator so later assertions do not borrow its buffer.
  static void collect_submitted_reloc_batches(
      ObLS &ls, const palf::LSN &begin, const palf::LSN &end,
      const ObTabletID &tablet_id, ObArenaAllocator &allocator, ObArray<ObString> &batches)
  {
    batches.reset();
    if (begin == end) {
      return;
    }
    palf::PalfBufferIterator iter;
    ASSERT_EQ(OB_SUCCESS, ls.get_log_handler()->seek(begin, iter));
    int iter_ret = OB_SUCCESS;
    while (OB_SUCCESS == (iter_ret = iter.next())) {
      const char *buf = nullptr;
      int64_t len = 0;
      share::SCN scn;
      palf::LSN lsn;
      bool raw = false;
      ASSERT_EQ(OB_SUCCESS, iter.get_entry(buf, len, scn, lsn, raw));
      if (lsn >= end) {
        break;
      }
      ASSERT_FALSE(raw);
      logservice::ObLogBaseHeader base_header;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, base_header.deserialize(buf, len, pos));
      if (ObTxLogBlock::DEFAULT_LOG_BLOCK_TYPE != base_header.get_log_type()) {
        continue;
      }
      ObArenaAllocator log_allocator("RelocLogRead");
      if (base_header.is_compressed()) {
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
        // PALF exposes the stored bytes. Match replay's decompression step
        // before interpreting the transaction block or its MDS payloads.
        logservice::LogCompressedPayload payload;
        int64_t compressed_pos = 0;
        ASSERT_EQ(OB_SUCCESS, payload.deserialize(buf + pos, len - pos, compressed_pos));
        ASSERT_TRUE(payload.is_valid());
        const int64_t original_len = payload.get_header().get_original_len();
        char *decoded = static_cast<char *>(log_allocator.alloc(pos + original_len));
        ASSERT_NE(nullptr, decoded);
        int64_t decoded_len = 0;
        ASSERT_EQ(OB_SUCCESS, logservice::decompress(
            buf + pos, len - pos, decoded + pos, original_len, decoded_len));
        ASSERT_EQ(original_len, decoded_len);
        MEMCPY(decoded, buf, pos);
        buf = decoded;
        len = pos + decoded_len;
#else
        FAIL() << "compressed log requires log-storage compression support";
#endif
      }
      ObTxLogBlock block;
      ASSERT_EQ(OB_SUCCESS, block.init_for_replay(buf, len));
      ObTxLogHeader header;
      int log_ret = OB_SUCCESS;
      while (OB_SUCCESS == (log_ret = block.get_next_log(header))) {
        if (ObTxLogType::TX_MULTI_DATA_SOURCE_LOG != header.get_tx_log_type()) {
          continue;
        }
        ObTxMultiDataSourceLog mds_log;
        ASSERT_EQ(OB_SUCCESS, block.deserialize_log_body(mds_log));
        for (int64_t i = 0; i < mds_log.get_data().count(); ++i) {
          const auto &node = mds_log.get_data().at(i);
          if (ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH != node.get_data_source_type()) {
            continue;
          }
          const ObString &data = node.get_data_buf();
          ObTabletMdsRelocBatchArg arg;
          int64_t decoded = 0;
          ASSERT_EQ(OB_SUCCESS, arg.deserialize(data.ptr(), data.length(), decoded));
          ASSERT_EQ(data.length(), decoded);
          bool matches = false;
          for (const auto &target : arg.targets_) {
            matches |= target.tablet_id_ == tablet_id;
          }
          if (matches) {
            ObString copy;
            ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, data, copy));
            ASSERT_EQ(OB_SUCCESS, batches.push_back(copy));
          }
        }
      }
      ASSERT_EQ(OB_ITER_END, log_ret) << "lsn=" << lsn.val_ << ", len=" << len
          << ", last log type=" << static_cast<int64_t>(header.get_tx_log_type());
    }
    ASSERT_TRUE(OB_SUCCESS == iter_ret || OB_ITER_END == iter_ret) << iter_ret;
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
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id, transfer_scn,
        tablet->get_reorganization_scn(), false /*use_reloc_v2*/);
    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, true);
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_persisted_uses_marker_redo_scn)
{
  static const char *TABLE = "test_mds_reloc_persisted";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    ASSERT_TRUE(GCTX.is_shared_storage_mode())
        << "SS persistence test requires -s and a configured S3 fixture";
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

    wait_ss_inc_uploader_ready(ls);

    const share::SCN transfer_scn = mock_scn(1200);
    write_committed_transfer_in_status(
        *ls,
        *tablet,
        tablet_id,
        transfer_scn,
        transfer_scn,
        ObTransID(402));

    bool is_done = true;
    bool is_persisted = true;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls, tablet_id, transfer_scn, is_done));
    EXPECT_FALSE(is_done);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, transfer_scn, is_persisted));
    EXPECT_FALSE(is_persisted);

    // Simulate an upgrade from V1: the old marker contains reloc_done_ but no
    // redo_scn_.  Its completion semantics must not start depending on the V2
    // shared checkpoint after upgrade.  In particular, the overload receiving
    // a prefetched checkpoint must recognize V1 before validating that value.
    const share::SCN legacy_transfer_scn = mock_scn(1199);
    ObTabletMdsRelocStatus legacy_status;
    legacy_status.set_reloc_done();
    UtMemtableMdsWriteCtx legacy_ctx(ObTransID(408));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *tablet, tablet_id, legacy_ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(
        tablet_id,
        legacy_transfer_scn,
        legacy_status,
        *legacy_ctx.mds_ctx().get_store_ctx(),
        legacy_ctx.mds_ctx()));
    share::SCN legacy_commit_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(legacy_commit_scn));
    legacy_ctx.commit(legacy_commit_scn);

    common::ObArenaAllocator legacy_allocator;
    ObTabletMdsRelocStatus committed_legacy_status;
    ObMdsValueMeta legacy_meta;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
        &legacy_allocator,
        tablet_id,
        legacy_transfer_scn,
        share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        false /*read_uncommitted*/,
        committed_legacy_status,
        legacy_meta));
    ASSERT_TRUE(committed_legacy_status.is_reloc_done());
    ASSERT_FALSE(committed_legacy_status.get_redo_scn().is_valid_and_not_min());
    share::SCN invalid_prefetched_checkpoint;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, legacy_transfer_scn, invalid_prefetched_checkpoint, is_persisted));
    EXPECT_TRUE(is_persisted);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, legacy_transfer_scn, is_persisted));
    EXPECT_TRUE(is_persisted);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    ASSERT_NE(nullptr, conn);
    register_empty_final_reloc_in_tx(
        conn, OB_SYS_TENANT_ID, ls_id, tablet_id, transfer_scn,
        tablet->get_reorganization_scn(), true /*use_reloc_v2*/);
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls, tablet_id, transfer_scn, is_done));
    EXPECT_TRUE(is_done);
    is_done = false;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls_id, tablet_id, transfer_scn, is_done));
    EXPECT_TRUE(is_done);
    EXPECT_EQ(OB_LS_NOT_EXIST, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ObLSID(999999), tablet_id, transfer_scn, is_done));
    EXPECT_FALSE(is_done);

    common::ObArenaAllocator allocator;
    ObTabletMdsRelocStatus reloc_status;
    ObMdsValueMeta marker_meta;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
        &allocator,
        tablet_id,
        transfer_scn,
        share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        false /*read_uncommitted*/,
        reloc_status,
        marker_meta));
    ASSERT_TRUE(reloc_status.is_reloc_done());
    ASSERT_TRUE(marker_meta.is_committed());
    const share::SCN marker_redo_scn = reloc_status.get_redo_scn();
    ASSERT_TRUE(marker_redo_scn.is_valid_and_not_min());

    share::SCN ss_mds_clog_checkpoint_scn;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(
        ls->get_ls_id(), ss_mds_clog_checkpoint_scn));
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, transfer_scn, ss_mds_clog_checkpoint_scn, is_persisted));
    EXPECT_EQ(ss_mds_clog_checkpoint_scn >= marker_redo_scn, is_persisted);

    flush_ss_mds_table(ls);
    wait_ss_ls_mds_checkpoint_reach(ls, marker_redo_scn);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, transfer_scn, is_persisted));
    EXPECT_TRUE(is_persisted);

    const share::SCN broken_generation = mock_scn(1198);
    ObTabletMdsRelocStatus broken_marker;
    broken_marker.set_reloc_done();
    broken_marker.enable_redo_fence();
    UtMemtableMdsWriteCtx broken_ctx(ObTransID(1490));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(*ls, *tablet, tablet_id, broken_ctx));
    // Preserving a different writer's committed meta skips this writer's redo
    // patch and models a corrupt committed V2 marker without a captured fence.
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(tablet_id, broken_generation,
        broken_marker, marker_meta, *broken_ctx.mds_ctx().get_store_ctx(), broken_ctx.mds_ctx()));
    broken_ctx.commit(marker_meta.get_commit_scn());
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletMdsRelocHelper::get_latest_committed_reloc_status(
        *ls->get_mds_table(), tablet_id, broken_generation, 5_s, broken_marker));
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls, tablet_id, broken_generation, is_done));
    is_done = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls_id, tablet_id, broken_generation, is_done));
    EXPECT_FALSE(is_done);
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, broken_generation, is_persisted));
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_final_replay_skips_persisted_redo_without_source)
{
  static const char *TABLE = "test_mds_reloc_ckpt";
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode())
      << "SS persistence test requires -s and a configured S3 fixture";
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

    wait_ss_inc_uploader_ready(ls);

    const share::SCN transfer_scn = mock_scn(1300);
    write_committed_transfer_in_status(
        *ls,
        *tablet,
        tablet_id,
        transfer_scn,
        transfer_scn,
        ObTransID(403));

    const share::SCN src_reorg_scn = tablet->get_reorganization_scn();
    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    ASSERT_NE(nullptr, conn);
    constexpr int64_t BUF_LEN = 1024;
    char submit_buffer[BUF_LEN] = {};
    int64_t submit_pos = 0;
    serialize_v2_reloc_marker_batch(
        submit_buffer, BUF_LEN, submit_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, true /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
        OB_SYS_TENANT_ID,
        ls_id,
        ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        submit_buffer,
        submit_pos));
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    common::ObArenaAllocator allocator;
    ObTabletMdsRelocStatus reloc_status;
    ObMdsValueMeta marker_meta;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
        &allocator,
        tablet_id,
        transfer_scn,
        share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        false /*read_uncommitted*/,
        reloc_status,
        marker_meta));
    ASSERT_TRUE(reloc_status.is_reloc_done());
    ASSERT_TRUE(marker_meta.is_committed());
    const share::SCN marker_redo_scn = reloc_status.get_redo_scn();
    ASSERT_TRUE(marker_redo_scn.is_valid_and_not_min());

    flush_ss_mds_table(ls);
    if (GCTX.is_shared_storage_mode()) {
      wait_ss_ls_mds_checkpoint_reach(ls, marker_redo_scn);
    }
    wait_ls_mds_checkpoint_reach(ls, marker_redo_scn);

    share::SCN checkpoint_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_checkpoint_scn(checkpoint_scn));
    ASSERT_TRUE(checkpoint_scn >= marker_redo_scn);

    // Simulate restart replay after the source tablet/LS dependency has gone.
    // If the checkpoint guard is bypassed, V2 pull tries this non-existent
    // source and the replay fails instead of returning success.
    char replay_buffer[BUF_LEN] = {};
    int64_t pos = 0;
    serialize_v2_reloc_marker_batch(
        replay_buffer,
        BUF_LEN,
        pos,
        ls_id,
        tablet_id,
        transfer_scn,
        share::ObLSID(999999),
        ObTabletID(999999),
        src_reorg_scn,
        true /*is_final*/);

    UtSingleLogMdsWriteCtx replay_ctx(
        MdsWriter(WriterType::MEDIUM_INFO, 404));
    ASSERT_EQ(OB_SUCCESS,
        replay_ctx.single_log_ctx().init(ls_id, INT64_MAX, true /*is_replay*/));
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::on_replay(
        replay_buffer, pos, marker_redo_scn, replay_ctx.mds_ctx()));
    replay_ctx.abort();

    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, true);
  }
}

TEST_F(TestMdsTransaction, split_report_requires_local_mds_checkpoint)
{
  static const char *TABLE = "test_split_reloc_local_gate";
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode())
      << "SS persistence test requires -s and a configured S3 fixture";
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

    wait_ss_inc_uploader_ready(ls);

    const share::SCN transfer_scn = share::SCN::min_scn();
    const share::SCN src_reorg_scn = tablet->get_reorganization_scn();
    // This is a completion-gate regression, independent of split DDL setup.
    // Persistence and source-free replay are covered by the two SS tests above.
    // Capture a real shared checkpoint, then inject a marker at that waterline
    // to isolate the shared-ready/local-behind state seen by both split DAGs.
    share::SCN marker_redo_scn;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(
        ls_id, marker_redo_scn));
    ASSERT_TRUE(marker_redo_scn.is_valid_and_not_min());
    {
      ObTabletMdsRelocStatus marker;
      marker.set_reloc_done();
      marker.enable_redo_fence();
      UtMemtableMdsWriteCtx marker_ctx(ObTransID(430));
      ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
          *ls, *tablet, tablet_id, marker_ctx));
      ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(
          tablet_id, transfer_scn, marker,
          *marker_ctx.mds_ctx().get_store_ctx(), marker_ctx.mds_ctx()));
      marker_ctx.commit(marker_redo_scn);
    }
    bool shared_ready = false;
    // The committed marker row becomes visible to the shared-ready gate without
    // another flush, but the read can still race the uploader's tablet reload,
    // so give it the same bounded settle window as the other SS cases.
    for (int i = 0; i < 60 && !shared_ready; ++i) {
      ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
          ls, tablet_id, transfer_scn, shared_ready));
      if (!shared_ready) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
    ASSERT_TRUE(shared_ready) << "shared-ready gate did not observe the split reloc marker";
    const share::SCN checkpoint_scn = marker_redo_scn;

    ObTabletHandle mds_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_tablet_handle_(mds_handle));
    ObTablet *mds_tablet = mds_handle.get_obj();
    struct RestoreCheckpoint {
      ObTablet *tablet_;
      share::SCN checkpoint_;
      ~RestoreCheckpoint() { tablet_->tablet_meta_.clog_checkpoint_scn_ = checkpoint_; }
    } restore{mds_tablet, mds_tablet->get_clog_checkpoint_scn()};
    ObArray<ObTabletID> dest_ids;
    ASSERT_EQ(OB_SUCCESS, dest_ids.push_back(tablet_id));
    int report_ret = OB_SUCCESS;
    // Exercise the actual common completion gate used by ordinary and LOB
    // split reports, including reports retried after the data is complete.
    for (int retry = 0; retry < 2; ++retry) {
      mds_tablet->tablet_meta_.clog_checkpoint_scn_.set_min();
      ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
          ls, dest_ids, DATA_VERSION_4_6_3_1, true, OB_SUCCESS, report_ret);
      EXPECT_EQ(OB_EAGAIN, report_ret);
    }
    // The task's fixed old version and pre-existing data failures retain
    // their behavior even while the local checkpoint is behind.
    ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
        ls, dest_ids, DATA_VERSION_4_6_2_1, true, OB_SUCCESS, report_ret);
    EXPECT_EQ(OB_SUCCESS, report_ret);
    ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
        ls, dest_ids, DATA_VERSION_4_6_3_1, true, OB_ERR_UNEXPECTED, report_ret);
    EXPECT_EQ(OB_ERR_UNEXPECTED, report_ret);
    mds_tablet->tablet_meta_.clog_checkpoint_scn_ = checkpoint_scn;
    ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
        ls, dest_ids, DATA_VERSION_4_6_3_1, true, OB_SUCCESS, report_ret);
    EXPECT_EQ(OB_SUCCESS, report_ret);
#ifdef ERRSIM
    // Count actual flush attempts, including failures, without racing the
    // background flusher. A batch with a stale checkpoint must request only
    // one flush even when later checks fall back to an authoritative read.
    common::EventItem *flush_event = nullptr;
    DLIST_FOREACH_NORET(item, common::EventTable::global_item_list()) {
      if (nullptr != item->item_.name_
          && 0 == strcmp(item->item_.name_, "EN_MDS_RELOC_CHECKPOINT_FLUSH")) {
        flush_event = &item->item_;
        break;
      }
    }
    ASSERT_NE(nullptr, flush_event);
    struct ResetFlushEvent {
      ~ResetFlushEvent()
      {
        common::EventTable::set_event("EN_MDS_RELOC_CHECKPOINT_FLUSH", common::EventItem());
      }
    } reset_flush_event;
    common::EventItem flush_fault;
    flush_fault.error_code_ = OB_TIMEOUT;
    flush_fault.occur_ = 100;
    flush_fault.cond_ = GETTID();
    ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event("EN_MDS_RELOC_CHECKPOINT_FLUSH", flush_fault));
    // Exercise failed prefetch AND the authoritative V2 fallback, scoped to
    // this thread so the background uploader is never fault-injected.
    for (const bool local : {false, true}) {
      const char *event_name = local ? "EN_MDS_RELOC_LOCAL_CHECKPOINT_READ"
                                     : "EN_MDS_RELOC_SHARED_CHECKPOINT_READ";
      struct ResetEvent {
        const char *name_;
        ~ResetEvent() { common::EventTable::set_event(name_, common::EventItem()); }
      } reset_event{event_name};
      const auto inject = [&](const int error, const int64_t count) {
        common::EventItem event;
        event.error_code_ = error;
        event.occur_ = count;
        event.cond_ = GETTID();
        EXPECT_EQ(OB_SUCCESS, common::EventTable::set_event(event_name, event));
      };
      const auto check = [&](const share::SCN &prefetched, bool &ready,
                             bool *flush_requested = nullptr) {
        return local
            ? ObTabletMdsRelocHelper::check_local_checkpoint_reached(
                ls, tablet_id, transfer_scn, prefetched, ready, flush_requested)
            : ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
                ls, tablet_id, transfer_scn, prefetched, ready, flush_requested);
      };
      const int64_t flush_budget = flush_event->occur_;
      bool flush_requested = false;
      bool batch_ready = false;
      EXPECT_EQ(OB_SUCCESS, check(marker_redo_scn, batch_ready, &flush_requested));
      EXPECT_TRUE(batch_ready);
      EXPECT_FALSE(flush_requested);
      EXPECT_EQ(flush_budget, flush_event->occur_);
      for (int i = 0; i < 8; ++i) {
        batch_ready = true;
        EXPECT_EQ(OB_SUCCESS, check(share::SCN::min_scn(), batch_ready, &flush_requested));
        EXPECT_FALSE(batch_ready);
      }
      EXPECT_TRUE(flush_requested);
      EXPECT_EQ(flush_budget - 1, flush_event->occur_);
      share::SCN failed_prefetch;
      inject(OB_TIMEOUT, 8);
      for (int i = 0; i < 8; ++i) {
        batch_ready = true;
        EXPECT_EQ(OB_SUCCESS, check(failed_prefetch, batch_ready, &flush_requested));
        EXPECT_FALSE(batch_ready);
      }
      EXPECT_EQ(flush_budget - 1, flush_event->occur_);
      // A fresh round retries the failed flush once, including the fallback.
      flush_requested = false;
      inject(OB_TIMEOUT, 8);
      for (int i = 0; i < 8; ++i) {
        batch_ready = true;
        EXPECT_EQ(OB_SUCCESS, check(failed_prefetch, batch_ready, &flush_requested));
        EXPECT_FALSE(batch_ready);
      }
      EXPECT_TRUE(flush_requested);
      EXPECT_EQ(flush_budget - 2, flush_event->occur_);
      for (const int fault : {OB_ERR_UNEXPECTED, OB_INVALID_ARGUMENT, OB_DISK_ERROR}) {
        flush_requested = false;
        inject(fault, 1);
        batch_ready = true;
        EXPECT_EQ(fault, check(failed_prefetch, batch_ready, &flush_requested));
        EXPECT_FALSE(batch_ready);
        EXPECT_FALSE(flush_requested);
        EXPECT_EQ(flush_budget - 2, flush_event->occur_);
      }
      // Standalone checks still request a flush on every retry.
      inject(OB_TIMEOUT, 2);
      for (int i = 0; i < 2; ++i) {
        batch_ready = true;
        EXPECT_EQ(OB_SUCCESS, check(failed_prefetch, batch_ready));
        EXPECT_FALSE(batch_ready);
      }
      EXPECT_EQ(flush_budget - 4, flush_event->occur_);
      share::SCN prefetched;
      inject(OB_TIMEOUT, 1);
      ASSERT_EQ(OB_TIMEOUT, local
          ? ObTabletMdsRelocHelper::get_local_mds_checkpoint_scn(ls, prefetched)
          : ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(ls_id, prefetched));
      ASSERT_FALSE(prefetched.is_valid());
      bool ready = false;
      EXPECT_EQ(OB_SUCCESS, check(prefetched, ready));
      EXPECT_TRUE(ready); // the one-shot prefetch fault is gone
      for (const int fault : {OB_TIMEOUT, OB_ERR_UNEXPECTED, OB_INVALID_ARGUMENT, OB_DISK_ERROR}) {
        inject(fault, 1);
        ready = true; // failed reads must clear a previously successful result
        EXPECT_EQ(OB_TIMEOUT == fault ? OB_SUCCESS : fault, check(prefetched, ready));
        EXPECT_FALSE(ready);
        inject(fault, 2); // fail both the gate's prefetch and fallback
        ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
            ls, dest_ids, DATA_VERSION_4_6_3_1, true, OB_SUCCESS, report_ret);
        EXPECT_EQ(OB_TIMEOUT == fault ? OB_EAGAIN : fault, report_ret);
      }
      inject(OB_TIMEOUT, 1);
      ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
          ls, dest_ids, DATA_VERSION_4_6_3_1, true, OB_SUCCESS, report_ret);
      EXPECT_EQ(OB_SUCCESS, report_ret); // fallback recovers the prefetch failure
    }
#endif
    // Null dest LS is a protocol/invariant error, not a retryable wait.
    ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
        nullptr, dest_ids, DATA_VERSION_4_6_3_1, true, OB_SUCCESS, report_ret);
    EXPECT_EQ(OB_ERR_UNEXPECTED, report_ret);

    // With the restored local checkpoint, replay must skip without the source.
    // If the checkpoint guard is bypassed, V2 pull tries this non-existent
    // source and the replay fails instead of returning success.
    constexpr int64_t BUF_LEN = 1024;
    char replay_buffer[BUF_LEN] = {};
    int64_t pos = 0;
    serialize_v2_reloc_marker_batch(
        replay_buffer,
        BUF_LEN,
        pos,
        ls_id,
        tablet_id,
        transfer_scn,
        ls_id,
        ObTabletID(999999),
        src_reorg_scn,
        true /*is_final*/, ObTabletMdsRelocType::SPLIT);

    UtSingleLogMdsWriteCtx replay_ctx(
        MdsWriter(WriterType::MEDIUM_INFO, 431));
    ASSERT_EQ(OB_SUCCESS,
        replay_ctx.single_log_ctx().init(ls_id, INT64_MAX, true /*is_replay*/));
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::on_replay(
        replay_buffer, pos, marker_redo_scn, replay_ctx.mds_ctx()));
    replay_ctx.abort();

    bool is_done = false;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls, tablet_id, transfer_scn, is_done));
    EXPECT_TRUE(is_done);
  }
}

// V2 arg serialize/deserialize round-trip + V1 wire compatibility.
TEST_F(TestMdsTransaction, reloc_arg_v2_serialize_roundtrip)
{
  // V2: src_ls_id_ / read_snapshot_ / target src fields survive the round-trip.
  ObTabletMdsRelocBatchArg arg;
  arg.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
  arg.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
  arg.ls_id_ = share::ObLSID(1001);
  arg.src_ls_id_ = share::ObLSID(1002);
  arg.read_snapshot_ = share::SCN::max_scn();
  ObTabletMdsRelocTarget target;
  target.tablet_id_ = ObTabletID(200001);
  target.transfer_scn_ = mock_scn(1100);
  target.src_tablet_id_ = ObTabletID(200001);
  target.src_reorg_scn_ = mock_scn(900);
  ASSERT_EQ(OB_SUCCESS, arg.targets_.push_back(target));
  ASSERT_TRUE(arg.is_marker_lock_batch());
  ASSERT_TRUE(arg.is_valid());
  arg.is_final_ = true;
  ASSERT_TRUE(arg.is_v2());
  ASSERT_TRUE(arg.is_valid());

  constexpr int64_t BUF_LEN = 2048;
  char buffer[BUF_LEN] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, BUF_LEN, pos));
  const int64_t ser_len = pos;

  ObTabletMdsRelocBatchArg out;
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, out.deserialize(buffer, ser_len, dpos));
  ASSERT_EQ(ser_len, dpos);
  EXPECT_TRUE(out.is_v2());
  EXPECT_EQ(arg.src_ls_id_, out.src_ls_id_);
  EXPECT_EQ(arg.read_snapshot_, out.read_snapshot_);
  ASSERT_EQ(1, out.targets_.count());
  EXPECT_EQ(target.src_tablet_id_, out.targets_.at(0).src_tablet_id_);
  EXPECT_EQ(target.src_reorg_scn_, out.targets_.at(0).src_reorg_scn_);

  // V1: v2 fields are NOT serialized; deserialize leaves them invalid, and the
  // batch stays valid (wire format unchanged from legacy).
  ObTabletMdsRelocBatchArg v1;
  v1.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION;
  v1.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
  v1.ls_id_ = share::ObLSID(1001);
  ObTabletMdsRelocTarget t1;
  t1.tablet_id_ = ObTabletID(200002);
  t1.transfer_scn_ = mock_scn(1200);
  ASSERT_EQ(OB_SUCCESS, v1.targets_.push_back(t1));
  v1.is_final_ = true;
  ASSERT_FALSE(v1.is_v2());
  ASSERT_TRUE(v1.is_valid());
  char b1[BUF_LEN] = {};
  int64_t p1 = 0;
  ASSERT_EQ(OB_SUCCESS, v1.serialize(b1, BUF_LEN, p1));
  ObTabletMdsRelocBatchArg v1out;
  int64_t dp1 = 0;
  ASSERT_EQ(OB_SUCCESS, v1out.deserialize(b1, p1, dp1));
  ASSERT_EQ(p1, dp1);
  EXPECT_FALSE(v1out.is_v2());
  EXPECT_FALSE(v1out.src_ls_id_.is_valid());
}

TEST_F(TestMdsTransaction, fold_mds_reloc_check_err_classification)
{
  // Shared by split completion report and fork local-land reloc flush decision.
  EXPECT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_SUCCESS));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_INVALID_ARGUMENT));
  EXPECT_EQ(OB_SERVER_OUTOF_DISK_SPACE,
      ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_SERVER_OUTOF_DISK_SPACE));
  EXPECT_EQ(OB_DISK_ERROR, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_DISK_ERROR));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_NOT_SUPPORTED));
  EXPECT_EQ(OB_ERR_DATA_TOO_LONG, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_ERR_DATA_TOO_LONG));
  EXPECT_EQ(OB_ERR_TOO_LONG_KEY_LENGTH,
      ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_ERR_TOO_LONG_KEY_LENGTH));
  EXPECT_EQ(OB_EAGAIN, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_ALLOCATE_MEMORY_FAILED));
  EXPECT_EQ(OB_EAGAIN, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_TIMEOUT));
  EXPECT_EQ(OB_EAGAIN, ObTabletMdsRelocHelper::fold_mds_reloc_check_err(OB_ENTRY_NOT_EXIST));
}

TEST_F(TestMdsTransaction, reloc_v2_data_version_boundary)
{
  ASSERT_TRUE(GCTX.is_shared_storage_mode()) << "run the version boundary test with -s";
  EXPECT_FALSE(ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_2_1));
  EXPECT_FALSE(ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_3_0));
  EXPECT_FALSE(ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_3_1 - 1));
  EXPECT_TRUE(ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_3_1));
  EXPECT_TRUE(ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_3_1 + 1));
}

TEST_F(TestMdsTransaction, transfer_reloc_uses_task_version_and_generation)
{
  const ObLSID src_ls_id(1001), dest_ls_id(1002);
  const share::SCN start_scn = mock_scn(1234);
  share::ObTransferTask task;
  task.src_ls_ = src_ls_id;
  task.dest_ls_ = dest_ls_id;
  task.start_scn_ = start_scn;
  uint64_t version = 0;
  // Old in-flight tasks keep their protocol even on a new binary/tenant.
  for (const uint64_t task_version : {DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
    task.data_version_ = task_version;
    ASSERT_EQ(OB_SUCCESS, ObTabletTransferMdsReloc::get_task_data_version_(
        task, src_ls_id, dest_ls_id, start_scn, version));
    EXPECT_EQ(task_version, version);
    EXPECT_EQ(OB_EAGAIN, ObTabletTransferMdsReloc::get_task_data_version_(
        task, src_ls_id, dest_ls_id, mock_scn(1235), version));
    EXPECT_EQ(0, version);
    EXPECT_EQ(OB_EAGAIN, ObTabletTransferMdsReloc::get_task_data_version_(
        task, ObLSID(1003), dest_ls_id, start_scn, version));
    EXPECT_EQ(OB_EAGAIN, ObTabletTransferMdsReloc::get_task_data_version_(
        task, src_ls_id, ObLSID(1003), start_scn, version));
  }
  task.data_version_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObTabletTransferMdsReloc::get_task_data_version_(
      task, src_ls_id, dest_ls_id, start_scn, version));
}

TEST_F(TestMdsTransaction, transfer_submit_uses_persisted_task_version)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_transfer_task_version");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_transfer_task_version", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(ls));
    uint64_t tenant_version = 0;
    ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, tenant_version));
    ASSERT_GE(tenant_version, DATA_VERSION_4_6_3_1);
    const share::SCN generation = mock_scn(1941);
    ASSERT_NO_FATAL_FAILURE(write_committed_transfer_in_status(
        *ls, *tablet_handle.get_obj(), tablet_id, generation, generation, ObTransID(1941)));

    ObTransferTask existing_task;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_by_src_ls(
        *GCTX.sql_proxy_, OB_SYS_TENANT_ID, ls_id, existing_task, share::OBCG_STORAGE));
    ObTransferPartList parts;
    ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTransferPartInfo(tablet_id.id(), tablet_id.id())));
    ObCurTraceId::TraceId trace_id;
    trace_id.init(GCTX.self_addr());
    const ObTransferTaskID task_id(1941001);
    ObTransferTask task;
    // This controlled persisted fixture uses one LS for both endpoints. It
    // exercises the real task-table lookup and protocol selection, not RS scheduling.
    ASSERT_EQ(OB_SUCCESS, task.init(task_id, ls_id, ls_id, parts,
        ObTransferStatus(ObTransferStatus::DOING), trace_id, ObBalanceTaskID(1941001),
        DATA_VERSION_4_6_3_0));
    task.set_start_scn(generation);
    ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(*GCTX.sql_proxy_, OB_SYS_TENANT_ID, task));
    struct RemoveTask {
      ObTransferTaskID task_id_;
      ~RemoveTask() {
        EXPECT_EQ(OB_SUCCESS, ObTransferTaskOperator::remove(*GCTX.sql_proxy_, OB_SYS_TENANT_ID, task_id_));
      }
    } remove_task{task_id};
    ObTransferTask persisted_task;
    ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_by_src_ls(
        *GCTX.sql_proxy_, OB_SYS_TENANT_ID, ls_id, persisted_task, share::OBCG_STORAGE));
    ASSERT_EQ(DATA_VERSION_4_6_3_0, persisted_task.get_data_version());

    ObArenaAllocator allocator("RelocSubmitTest");
    ObArray<ObString> batches;
    palf::LSN begin, end;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(begin));
    ASSERT_EQ(OB_SUCCESS, ObTabletTransferMdsReloc::submit_transfer_mds_reloc(
        *tablet_handle.get_obj(), generation, OB_SYS_TENANT_ID, ls_id));
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(end));
    ASSERT_NO_FATAL_FAILURE(collect_submitted_reloc_batches(*ls, begin, end, tablet_id, allocator, batches));
    ASSERT_GT(batches.count(), 0);
    bool has_final = false;
    for (const auto &data : batches) {
      ObTabletMdsRelocBatchArg batch;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, batch.deserialize(data.ptr(), data.length(), pos));
      EXPECT_EQ(LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION, batch.version_);
      EXPECT_EQ(ObTabletMdsRelocType::TRANSFER, batch.reloc_type_);
      has_final |= batch.is_final_;
    }
    EXPECT_TRUE(has_final);

    // Stale destination/start SCN must fail before any new batch is registered.
    // A different persisted source key is absent from get_by_src_ls, so that
    // case preserves OB_ENTRY_NOT_EXIST rather than manufacturing OB_EAGAIN.
    for (int mismatch = 0; mismatch < 3; ++mismatch) {
      SCOPED_TRACE(mismatch);
      if (2 == mismatch) {
        ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::remove(*GCTX.sql_proxy_, OB_SYS_TENANT_ID, task_id));
        task.src_ls_ = ObLSID(999998);
        ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(*GCTX.sql_proxy_, OB_SYS_TENANT_ID, task));
      }
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(begin));
      EXPECT_EQ(2 == mismatch ? OB_ENTRY_NOT_EXIST : OB_EAGAIN,
          ObTabletTransferMdsReloc::submit_transfer_mds_reloc(*tablet_handle.get_obj(),
              1 == mismatch ? mock_scn(1942) : generation, OB_SYS_TENANT_ID,
              0 == mismatch ? ObLSID(999999) : ls_id));
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(end));
      ASSERT_NO_FATAL_FAILURE(collect_submitted_reloc_batches(*ls, begin, end, tablet_id, allocator, batches));
      EXPECT_EQ(0, batches.count());
    }
  }
}

TEST_F(TestMdsTransaction, reloc_v2_upgrade_wire_compat)
{
  uint64_t mapped_data_version = 0;
  EXPECT_TRUE(share::ObUpgradeChecker::check_data_version_exist(DATA_VERSION_4_6_3_1));
  ASSERT_EQ(OB_SUCCESS, share::ObUpgradeChecker::get_data_version_by_cluster_version(
      CLUSTER_VERSION_4_6_3_1, mapped_data_version));
  EXPECT_EQ(DATA_VERSION_4_6_3_1, mapped_data_version);

  // Even after binary upgrade, an old task/data version must write bytes the
  // actual pre-V2 field decoder can read, including target tail extensions.
  ObTabletMdsRelocBatchArg arg;
  arg.version_ = ObTabletSplitMdsReloc::is_reloc_v2_enabled(DATA_VERSION_4_6_2_1)
      ? LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2 : LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION;
  arg.reloc_type_ = ObTabletMdsRelocType::FORK;
  arg.ls_id_ = ObLSID(1001);
  ObTabletMdsRelocTarget target;
  target.tablet_id_ = ObTabletID(200001);
  target.transfer_scn_ = mock_scn(1200);
  ASSERT_EQ(OB_SUCCESS, arg.targets_.push_back(target));
  arg.is_final_ = true;
  char buffer[2048] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, sizeof(buffer), pos));
  LegacyRelocBatch old_reader;
  int64_t decoded = 0;
  ASSERT_EQ(OB_SUCCESS, old_reader.deserialize(buffer, pos, decoded));
  EXPECT_EQ(pos, decoded);
  EXPECT_EQ(LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION, old_reader.version_);
  ASSERT_EQ(1, old_reader.targets_.count());
  EXPECT_EQ(target.tablet_id_, old_reader.targets_.at(0).tablet_id_);
  EXPECT_EQ(target.transfer_scn_, old_reader.targets_.at(0).transfer_scn_);
  EXPECT_TRUE(old_reader.is_final_);
}

TEST_F(TestMdsTransaction, transfer_managers_propagate_checkpoint_prefetch_errors)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
#ifdef ERRSIM
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_transfer_checkpoint_error");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_transfer_checkpoint_error", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    ObSSTransferReplaceMgr replace_mgr;
    ObSSTransferRefreshMgr refresh_mgr;
    ASSERT_EQ(OB_SUCCESS, replace_mgr.init(ls));
    ASSERT_EQ(OB_SUCCESS, refresh_mgr.init(ls));
    // Use real manager process entries. The fault must escape before either
    // manager examines tablet readiness or schedules a replacement/refresh.
    replace_mgr.task_id_.init(GCTX.self_addr());
    refresh_mgr.task_id_.init(GCTX.self_addr());
    ObTaskId request_id;
    request_id.init(GCTX.self_addr());
    ObTabletBackfillInfo info;
    info.tablet_id_ = tablet_id;
    info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_IN;
    info.reorganization_scn_ = tablet_handle.get_obj()->get_reorganization_scn();
    info.backfill_scn_ = info.reorganization_scn_;
    info.relative_ls_id_ = ls_id;
    ObArray<ObTabletBackfillInfo> tablets;
    ASSERT_EQ(OB_SUCCESS, tablets.push_back(info));
    for (const bool local : {false, true}) {
      const char *event_name = local ? "EN_MDS_RELOC_LOCAL_CHECKPOINT_READ"
                                     : "EN_MDS_RELOC_SHARED_CHECKPOINT_READ";
      struct ResetEvent {
        const char *name_;
        ~ResetEvent() { common::EventTable::set_event(name_, common::EventItem()); }
      } reset{event_name};
      for (const int fault : {OB_TIMEOUT, OB_EAGAIN, OB_ERR_UNEXPECTED, OB_DISK_ERROR}) {
        common::EventItem event;
        event.error_code_ = fault;
        event.occur_ = 1;
        event.cond_ = GETTID();
        ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event(event_name, event));
        EXPECT_EQ(fault, local ? refresh_mgr.process(tablets, request_id)
                               : replace_mgr.process(tablets, request_id));
        bool scheduled = true;
        ASSERT_EQ(OB_SUCCESS, local ? refresh_mgr.check_dag_net_exist_(scheduled)
                                   : replace_mgr.check_dag_net_exist_(scheduled));
        EXPECT_FALSE(scheduled);
      }
    }
  }
#else
  // GTEST_SKIP is unavailable in the bundled gtest, so pass trivially instead.
  SUCCEED() << "checkpoint read fault injection requires ERRSIM";
#endif
}

TEST_F(TestMdsTransaction, fork_publish_requires_shared_mds_checkpoint)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode()) << "run the shared publish test with -s";
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_fork_reloc_publish_gate");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_fork_reloc_publish_gate", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    const share::SCN original_reorg_scn = tablet_handle.get_obj()->get_reorganization_scn();
    ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(ls));
    // Materialize an MDS row so the initial shared checkpoint has an actual
    // redo to cover. An arbitrary LS decided SCN need not belong to MDS.
    ObTabletMdsRelocStatus initial_marker;
    initial_marker.set_reloc_done();
    UtMemtableMdsWriteCtx initial_ctx(ObTransID(429));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *tablet_handle.get_obj(), tablet_id, initial_ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(tablet_id, original_reorg_scn,
        initial_marker, *initial_ctx.mds_ctx().get_store_ctx(), initial_ctx.mds_ctx()));
    share::SCN initial_checkpoint;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(initial_checkpoint));
    initial_ctx.commit(initial_checkpoint);
    ASSERT_NO_FATAL_FAILURE(flush_ss_mds_table(ls));
    ASSERT_NO_FATAL_FAILURE(wait_ss_ls_mds_checkpoint_reach(ls, initial_checkpoint));
    ObSSMetaService *meta_service = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_service);
    ASSERT_EQ(OB_SUCCESS, meta_service->create_tablet(ls_id, tablet_id, original_reorg_scn, true));
    common::ObArenaAllocator allocator("ForkPubTest");
    ObTabletHandle shared_tablet;
    ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet(ls_id, tablet_id, original_reorg_scn, allocator, shared_tablet));
    share::SCN fork_snapshot;
    ASSERT_EQ(OB_SUCCESS, ObAtomicProtocolUtil::get_gts_sync(OB_SYS_TENANT_ID, fork_snapshot));
    // Seed a separate shared generation without modifying the live local tablet.
    // Fork identity is established at creation and cannot be installed by a
    // subsequent metadata update on an ordinary shared tablet.
    const share::SCN reorg_scn = fork_snapshot;
    ObTabletHandle fork_seed;
    // Standalone handles destroy their contents before these stack allocators
    // leave scope; pooled tablets would defer destruction to the GC timer.
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::acquire_tmp_tablet(
        ObTabletMapKey(ls_id, tablet_id), allocator, fork_seed));
    ASSERT_EQ(OB_SUCCESS, fork_seed.get_obj()->init_with_ss_tablet(
        allocator, *shared_tablet.get_obj(), fork_snapshot, false));
    fork_seed.get_obj()->tablet_meta_.transfer_info_.transfer_start_scn_ = reorg_scn;
    fork_seed.get_obj()->tablet_meta_.fork_info_.set_fork_snapshot_version(fork_snapshot.get_val_for_tx());
    fork_seed.get_obj()->tablet_meta_.fork_info_.set_src_tablet_id(tablet_id);
    fork_seed.get_obj()->tablet_meta_.fork_info_.set_data_complete(false);
    ASSERT_EQ(OB_SUCCESS, meta_service->create_tablet_(ls_id, tablet_id, reorg_scn, fork_seed, true));
    shared_tablet.reset();
    ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet(ls_id, tablet_id, reorg_scn, allocator, shared_tablet));
    ASSERT_TRUE(shared_tablet.get_obj()->get_tablet_meta().fork_info_.is_valid());
    ObTableStoreIterator table_iter;
    ObTablesHandleArray tables;
    ASSERT_EQ(OB_SUCCESS, shared_tablet.get_obj()->get_all_sstables(table_iter, false));
    ASSERT_EQ(OB_SUCCESS, ObForkTableDataDag::collect_fork_local_land_tables_(fork_snapshot, table_iter, tables));
    ObBatchUpdateTableStoreParam param;
    ASSERT_EQ(OB_SUCCESS, ObForkTabletUtil::build_fork_replace_table_store_param(
        reorg_scn, ls->get_rebuild_seq(), fork_snapshot.get_val_for_tx(), fork_snapshot.get_val_for_tx(),
        tablet_id, tables, fork_snapshot, param));
    ObArray<ObSSMetaCtrlKeyInfo> ctrl_keys;
    // Exercise the same publication boundary as ObForkTablePublishTask, including
    // the shared metadata write. A missing final marker cannot publish completion.
    EXPECT_EQ(OB_EAGAIN, ObForkTableDataDag::publish_fork_tables_to_shared_(
        *ls, tablet_id, reorg_scn, ctrl_keys, param));
    share::SCN before_publish;
    ASSERT_EQ(OB_SUCCESS, ObAtomicProtocolUtil::get_gts_sync(OB_SYS_TENANT_ID, before_publish));

    ObTabletMdsRelocStatus marker;
    marker.set_reloc_done();
    marker.enable_redo_fence();
    UtMemtableMdsWriteCtx ctx(ObTransID(430));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(*ls, *tablet_handle.get_obj(), tablet_id, ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(
        tablet_id, reorg_scn, marker, *ctx.mds_ctx().get_store_ctx(), ctx.mds_ctx()));
    share::SCN marker_redo_scn;
    ASSERT_EQ(OB_SUCCESS, ObAtomicProtocolUtil::get_gts_sync(OB_SYS_TENANT_ID, marker_redo_scn));
    ctx.commit(marker_redo_scn);
    share::SCN shared_checkpoint;
    ASSERT_NO_FATAL_FAILURE(get_ss_ls_mds_clog_checkpoint_scn(ls, shared_checkpoint));
    ASSERT_LT(shared_checkpoint, marker_redo_scn);
    EXPECT_EQ(OB_EAGAIN, ObForkTableDataDag::publish_fork_tables_to_shared_(
        *ls, tablet_id, reorg_scn, ctrl_keys, param));
    auto check_unpublished = [&]() {
      ObArenaAllocator check_allocator("ForkPubCheck");
      ObTabletHandle check_tablet;
      ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet(ls_id, tablet_id, reorg_scn, check_allocator, check_tablet));
      EXPECT_FALSE(check_tablet.get_obj()->get_tablet_meta().fork_info_.is_data_complete());
    };
    ASSERT_NO_FATAL_FAILURE(check_unpublished());

#ifdef ERRSIM
    struct ResetPublishEvent {
      ~ResetPublishEvent() {
        common::EventTable::set_event("EN_MDS_RELOC_SHARED_CHECKPOINT_READ", common::EventItem());
      }
    } reset_event;
    for (const int fault : {OB_TIMEOUT, OB_ERR_UNEXPECTED}) {
      common::EventItem event;
      event.error_code_ = fault;
      event.occur_ = 1;
      event.cond_ = GETTID();
      ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event("EN_MDS_RELOC_SHARED_CHECKPOINT_READ", event));
      EXPECT_EQ(fault, ObForkTableDataDag::publish_fork_tables_to_shared_(
          *ls, tablet_id, reorg_scn, ctrl_keys, param));
      ASSERT_NO_FATAL_FAILURE(check_unpublished());
    }
    ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event("EN_MDS_RELOC_SHARED_CHECKPOINT_READ", common::EventItem()));
#endif

    ASSERT_NO_FATAL_FAILURE(flush_ss_mds_table(ls));
    ASSERT_NO_FATAL_FAILURE(wait_ss_ls_mds_checkpoint_reach(ls, marker_redo_scn));
    // Shared publish must not wait for this replica's checkpoint. That belongs
    // to local land; HA imports LS_MDS_TABLET from the same snapshot instead.
    ObTabletHandle mds_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_tablet_handle_(mds_handle));
    {
      struct RestoreCheckpoint {
        ObTablet *tablet_;
        share::SCN checkpoint_;
        ~RestoreCheckpoint() { tablet_->tablet_meta_.clog_checkpoint_scn_ = checkpoint_; }
      } restore{mds_handle.get_obj(), mds_handle.get_obj()->get_clog_checkpoint_scn()};
      mds_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_.set_min();
      ASSERT_EQ(OB_SUCCESS, ObForkTableDataDag::publish_fork_tables_to_shared_(
          *ls, tablet_id, reorg_scn, ctrl_keys, param));
    }
    share::SCN after_publish;
    ASSERT_EQ(OB_SUCCESS, ObAtomicProtocolUtil::get_gts_sync(OB_SYS_TENANT_ID, after_publish));

    // Read both generations through the production HA snapshot reader. A
    // snapshot that sees completion must also import the final redo fence.
    for (const bool after : {false, true}) {
      ObCopySSTabletsMetaServiceReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.init(OB_SYS_TENANT_ID, ls_id, after ? after_publish : before_publish));
      ObArenaAllocator read_allocator("ForkPubHA");
      ObArenaAllocator import_allocator("ForkPubImport");
      bool found_fork = false;
      bool found_mds = false;
      while (!found_fork || !found_mds) {
        ObTabletHandle snapshot_tablet;
        ObCopySSTabletMeta snapshot_meta;
        ASSERT_EQ(OB_SUCCESS, reader.get_next_tablet(read_allocator, snapshot_tablet, snapshot_meta));
        if ((snapshot_meta.tablet_id_ == tablet_id
                && snapshot_tablet.get_obj()->get_reorganization_scn() == reorg_scn)
            || snapshot_meta.tablet_id_ == LS_MDS_TABLET) {
          ObTabletHandle imported;
          ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::acquire_tmp_tablet(
              ObTabletMapKey(ls_id, snapshot_meta.tablet_id_), import_allocator, imported));
          ASSERT_EQ(OB_SUCCESS, imported.get_obj()->init_with_ss_tablet(
              import_allocator, *snapshot_tablet.get_obj(), snapshot_meta.meta_version_, false));
          if (snapshot_meta.tablet_id_ == tablet_id) {
            found_fork = true;
            EXPECT_EQ(after, imported.get_obj()->get_tablet_meta().fork_info_.is_data_complete());
          } else {
            found_mds = true;
            if (after) {
              EXPECT_GE(imported.get_obj()->get_clog_checkpoint_scn(), marker_redo_scn);
            } else {
              EXPECT_LT(imported.get_obj()->get_clog_checkpoint_scn(), marker_redo_scn);
            }
          }
        }
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMdsTransaction, fork_local_land_retry_requires_local_checkpoint)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode())
      << "SS persistence test requires -s and a configured S3 fixture";
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_fork_reloc_local_gate");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_fork_reloc_local_gate", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    ObTablet *tablet = tablet_handle.get_obj();
    const share::SCN reorg_scn = tablet->get_reorganization_scn();
    wait_ss_inc_uploader_ready(ls);
    ObTabletMdsRelocStatus marker;
    marker.set_reloc_done();
    marker.enable_redo_fence();
    UtMemtableMdsWriteCtx ctx(ObTransID(420));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(*ls, *tablet, tablet_id, ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(
        tablet_id, reorg_scn, marker, *ctx.mds_ctx().get_store_ctx(), ctx.mds_ctx()));
    share::SCN marker_redo_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(marker_redo_scn));
    ASSERT_TRUE(marker_redo_scn.is_valid_and_not_min());
    ctx.commit(marker_redo_scn);

    // The fork land completion gate requires the shared LS_MDS_TABLET clog
    // checkpoint to cover the final marker redo as well. Establish that state
    // for real instead of assuming it, so the local-lag simulation below only
    // withholds the local checkpoint.
    flush_ss_mds_table(ls);
    wait_ss_ls_mds_checkpoint_reach(ls, marker_redo_scn);
    wait_ls_mds_checkpoint_reach(ls, marker_redo_scn);

    ObTabletHandle mds_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_tablet_handle_(mds_handle));
    ObTablet *mds_tablet = mds_handle.get_obj();
    struct RestoreMeta {
      ObTablet *tablet_;
      ObTablet *mds_tablet_;
      share::ObForkTabletInfo fork_info_;
      share::SCN checkpoint_;
      ~RestoreMeta() {
        tablet_->tablet_meta_.fork_info_ = fork_info_;
        mds_tablet_->tablet_meta_.clog_checkpoint_scn_ = checkpoint_;
      }
    } restore{tablet, mds_tablet, tablet->tablet_meta_.fork_info_, mds_tablet->get_clog_checkpoint_scn()};
    // The tablet-change timer persists an unpersisted tablet and installs a
    // freshly built ObTablet through compare_and_swap_addr_and_object, which
    // silently drops a fork_info_ patch applied to the previous object. The dag
    // re-fetches the tablet via ObDDLUtil::ddl_get_tablet, so re-acquire the
    // live handle right before each attempt and patch the object it will read.
    auto patch_fork_data_complete = [&](const bool is_complete) {
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
          tablet_id, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          ObMDSGetTabletMode::READ_ALL_COMMITED));
      tablet = tablet_handle.get_obj();
      ASSERT_NE(nullptr, tablet);
      restore.tablet_ = tablet;
      tablet->tablet_meta_.fork_info_.set_data_complete(is_complete);
    };
    ObForkTableDataDag dag;
    dag.is_inited_ = true;
    dag.fork_phase_ = ObForkDataPhase::FORK_LOCAL_LAND;
    dag.param_.is_inited_ = true;
    dag.param_.orig_tenant_id_ = dag.param_.dest_tenant_id_ = OB_SYS_TENANT_ID;
    dag.param_.orig_table_id_ = dag.param_.dest_table_id_ = 200001;
    dag.param_.orig_ls_id_ = dag.param_.dest_ls_id_ = ls_id;
    dag.param_.orig_tablet_id_ = dag.param_.dest_tablet_id_ = tablet_id;
    dag.param_.snapshot_version_ = 1;
    dag.param_.task_id_ = 1;
    dag.param_.ddl_type_ = share::ObDDLType::DDL_FORK_TABLE;
    ASSERT_TRUE(dag.param_.is_valid());

    // Simulate the same-LS replica lag: shared persistence is sufficient but
    // this replica has not persisted the marker. Neither first land nor the
    // already-data-complete retry may report success in this state.
    bool shared_ready = false;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, tablet_id, reorg_scn, shared_ready));
    ASSERT_TRUE(shared_ready);
    mds_tablet->tablet_meta_.clog_checkpoint_scn_.set_min();
    patch_fork_data_complete(false);
    EXPECT_EQ(OB_EAGAIN, dag.process_fork_local_land());
    EXPECT_FALSE(tablet->tablet_meta_.fork_info_.is_data_complete());
    mds_tablet->tablet_meta_.clog_checkpoint_scn_.set_min();
    patch_fork_data_complete(true);
    EXPECT_EQ(OB_EAGAIN, dag.process_fork_local_land());

#ifdef ERRSIM
    // Both initial land and data-complete retries must keep checkpoint read
    // failures visible instead of passing the completion gate.
    for (const char *event_name : {"EN_MDS_RELOC_LOCAL_CHECKPOINT_READ",
                                  "EN_MDS_RELOC_SHARED_CHECKPOINT_READ"}) {
      struct ResetEvent {
        const char *name_;
        ~ResetEvent() { common::EventTable::set_event(name_, common::EventItem()); }
      } reset_event{event_name};
      for (const int fault : {OB_TIMEOUT, OB_ERR_UNEXPECTED}) {
        for (const bool complete : {false, true}) {
          mds_tablet->tablet_meta_.clog_checkpoint_scn_ = marker_redo_scn;
          patch_fork_data_complete(complete);
          common::EventItem event;
          event.error_code_ = fault;
          event.occur_ = 1;
          event.cond_ = GETTID();
          ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event(event_name, event));
          EXPECT_EQ(fault, dag.process_fork_local_land());
          EXPECT_EQ(complete, tablet->tablet_meta_.fork_info_.is_data_complete());
        }
      }
    }
#endif
    // Once local persistence covers the same marker, the retry can complete.
    mds_tablet->tablet_meta_.clog_checkpoint_scn_ = marker_redo_scn;
    patch_fork_data_complete(true);
    EXPECT_EQ(OB_SUCCESS, dag.process_fork_local_land());

    // Replay after source reclamation must skip this persisted final log.
    // A missing source tablet in the same LS makes any accidental pull fail.
    ObTabletMdsRelocBatchArg replay_arg;
    replay_arg.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
    replay_arg.reloc_type_ = ObTabletMdsRelocType::FORK;
    replay_arg.ls_id_ = replay_arg.src_ls_id_ = ls_id;
    replay_arg.read_snapshot_ = share::SCN::max_scn();
    replay_arg.is_final_ = true;
    ObTabletMdsRelocTarget replay_target;
    replay_target.tablet_id_ = tablet_id;
    replay_target.transfer_scn_ = reorg_scn;
    replay_target.src_tablet_id_ = ObTabletID(999999);
    replay_target.src_reorg_scn_ = reorg_scn;
    ASSERT_EQ(OB_SUCCESS, replay_arg.targets_.push_back(replay_target));
    ASSERT_TRUE(replay_arg.is_valid());
    char replay_buffer[1024] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, replay_arg.serialize(replay_buffer, sizeof(replay_buffer), pos));
    UtSingleLogMdsWriteCtx replay_ctx(MdsWriter(WriterType::MEDIUM_INFO, 421));
    ASSERT_EQ(OB_SUCCESS, replay_ctx.single_log_ctx().init(ls_id, INT64_MAX, true /*is_replay*/));
    EXPECT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::on_replay(
        replay_buffer, pos, marker_redo_scn, replay_ctx.mds_ctx()));
    replay_ctx.abort();
  }
}

// V2 pull path end-to-end (single server): src == this ls/tablet, and the
// tablet has no op-type rows, so the pull finds nothing and the done marker is
// set on commit.  Exercises pull_and_apply_op_rows_ + the src-ready barrier +
// the marker mutex write path.
TEST_F(TestMdsTransaction, tablet_reloc_v2_empty_pull_sets_marker)
{
  static const char *TABLE = "test_mds_reloc_v2_pull";
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

    const share::SCN transfer_scn = mock_scn(1500);
    const share::SCN src_reorg_scn = tablet->get_reorganization_scn();
    write_committed_transfer_in_status(
        *ls, *tablet, tablet_id, transfer_scn, transfer_scn, ObTransID(405));
    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, false);

    common::ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    ASSERT_NE(nullptr, conn);
    constexpr int64_t BUF_LEN = 1024;
    char lock_buffer[BUF_LEN] = {};
    int64_t lock_pos = 0;
    // Exercise the production order in one transaction: INSERT the marker as
    // the mutex first, then PUT the same marker as the final redo fence.
    serialize_v2_reloc_marker_batch(
        lock_buffer, BUF_LEN, lock_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, false /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
        OB_SYS_TENANT_ID, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        lock_buffer, lock_pos));
    char final_buffer[BUF_LEN] = {};
    int64_t final_pos = 0;
    // src ls/tablet == this ls/tablet: the final pull finds no op-type rows,
    // then updates the marker written above.
    serialize_v2_reloc_marker_batch(
        final_buffer, BUF_LEN, final_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, true /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(
        OB_SYS_TENANT_ID, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        final_buffer, final_pos));
    ASSERT_EQ(OB_SUCCESS, trans.end(true));

    check_committed_reloc_marker(
        *ls, tablet_id, transfer_scn, transfer_scn, true);
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_v2_committed_marker_duplicate_aborts_loser)
{
  static const char *TABLE = "test_mds_reloc_v2_dup";
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

    const share::SCN transfer_scn = mock_scn(1600);
    const share::SCN src_reorg_scn = tablet->get_reorganization_scn();
    write_committed_transfer_in_status(
        *ls, *tablet, tablet_id, transfer_scn, transfer_scn, ObTransID(406));

    constexpr int64_t BUF_LEN = 1024;
    common::ObMySQLTransaction winner;
    ASSERT_EQ(OB_SUCCESS, winner.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *winner_conn =
        static_cast<observer::ObInnerSQLConnection *>(winner.get_connection());
    ASSERT_NE(nullptr, winner_conn);
    char lock_buffer[BUF_LEN] = {};
    int64_t lock_pos = 0;
    serialize_v2_reloc_marker_batch(
        lock_buffer, BUF_LEN, lock_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, false /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, winner_conn->register_multi_data_source(
        OB_SYS_TENANT_ID, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        lock_buffer, lock_pos));
    char final_buffer[BUF_LEN] = {};
    int64_t final_pos = 0;
    serialize_v2_reloc_marker_batch(
        final_buffer, BUF_LEN, final_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, true /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, winner_conn->register_multi_data_source(
        OB_SYS_TENANT_ID, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        final_buffer, final_pos));
    ASSERT_EQ(OB_SUCCESS, winner.end(true));

    // A later reloc attempt sees the already committed INSERT marker.  The
    // registration returns the internal already-done signal so its transaction
    // is aborted and cannot continue to payload/final pull.  The production
    // submitter normalizes this to success only after the abort succeeds.
    common::ObMySQLTransaction retry;
    ASSERT_EQ(OB_SUCCESS, retry.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *retry_conn =
        static_cast<observer::ObInnerSQLConnection *>(retry.get_connection());
    ASSERT_NE(nullptr, retry_conn);
    char retry_buffer[BUF_LEN] = {};
    int64_t retry_pos = 0;
    serialize_v2_reloc_marker_batch(
        retry_buffer, BUF_LEN, retry_pos, ls_id, tablet_id, transfer_scn,
        ls_id, tablet_id, src_reorg_scn, false /*is_final*/);
    ASSERT_EQ(OB_ALREADY_DONE, retry_conn->register_multi_data_source(
        OB_SYS_TENANT_ID, ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        retry_buffer, retry_pos));
    ASSERT_EQ(OB_SUCCESS, retry.end(false));
    check_committed_reloc_marker(*ls, tablet_id, transfer_scn, transfer_scn, true);
  }
}

TEST_F(TestMdsTransaction, reloc_v2_submit_already_done_end_result)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_submit_src");
    create_table("test_reloc_submit_dst");
    ObTabletID src_id, dest_id;
    ObLSID src_ls_id, dest_ls_id;
    resolve_tablet_and_ls("test_reloc_submit_src", src_id, src_ls_id);
    resolve_tablet_and_ls("test_reloc_submit_dst", dest_id, dest_ls_id);
    ObLSHandle src_ls_handle, dest_ls_handle;
    ObTabletHandle src_handle, dest_handle;
    get_ls_and_tablet(src_id, src_ls_id, src_ls_handle, src_handle);
    get_ls_and_tablet(dest_id, dest_ls_id, dest_ls_handle, dest_handle);
    ObLS *dest_ls = dest_ls_handle.get_ls();
    ASSERT_NE(nullptr, dest_ls);
    ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(src_ls_handle.get_ls()));
    ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(dest_ls));
    const share::SCN generation = share::SCN::min_scn();
    ObTabletCreateDeleteMdsUserData status;
    status.tablet_status_ = ObTabletStatus::SPLIT_DST;
    status.data_type_ = ObTabletMdsUserDataType::START_SPLIT_DST;
    status.transfer_scn_ = generation;
    UtMemtableMdsWriteCtx status_ctx(ObTransID(1450));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *dest_ls, *dest_handle.get_obj(), dest_id, status_ctx));
    ASSERT_EQ(OB_SUCCESS, dest_ls->get_mds_table()->write(
        dest_id, generation, status, *status_ctx.mds_ctx().get_store_ctx(), status_ctx.mds_ctx()));
    share::SCN status_commit;
    ASSERT_EQ(OB_SUCCESS, dest_ls->get_max_decided_scn(status_commit));
    status_ctx.commit(status_commit);
    ObArray<ObTabletID> dest_ids;
    ASSERT_EQ(OB_SUCCESS, dest_ids.push_back(dest_id));
    const auto submit = [&]() {
      return ObTabletSplitMdsReloc::submit_split_multikey_mds_reloc(
          *src_handle.get_obj(), dest_ids, share::SCN::max_scn(),
          OB_SYS_TENANT_ID, dest_ls_id, DATA_VERSION_4_6_3_1);
    };
    // Both the winner and retries use the production submitter, including its
    // internal SQL transaction and already-done normalization after abort.
    ASSERT_EQ(OB_SUCCESS, submit());
    common::ObArenaAllocator allocator;
    ObTabletMdsRelocStatus marker;
    ObMdsValueMeta winner_meta;
    const auto read_marker = [&](ObMdsValueMeta &meta) {
      return dest_ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
          &allocator, dest_id, generation, share::SCN::max_scn(),
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, false, marker, meta);
    };
    ASSERT_EQ(OB_SUCCESS, read_marker(winner_meta));
    ASSERT_TRUE(marker.is_reloc_done());
    const share::SCN winner_redo = marker.get_redo_scn();
    ASSERT_EQ(OB_SUCCESS, submit());
#ifdef ERRSIM
    common::EventItem *end_event = nullptr;
    DLIST_FOREACH_NORET(item, common::EventTable::global_item_list()) {
      if (nullptr != item->item_.name_
          && 0 == strcmp(item->item_.name_, "EN_MDS_RELOC_END_TRANS_RESULT")) {
        end_event = &item->item_;
        break;
      }
    }
    ASSERT_NE(nullptr, end_event);
    struct ResetEndEvent {
      ~ResetEndEvent()
      {
        common::EventTable::set_event("EN_MDS_RELOC_END_TRANS_RESULT", common::EventItem());
      }
    } reset_end_event;
    common::EventItem fault;
    fault.error_code_ = OB_TIMEOUT;
    fault.occur_ = 1;
    fault.cond_ = GETTID();
    ASSERT_EQ(OB_SUCCESS, common::EventTable::set_event("EN_MDS_RELOC_END_TRANS_RESULT", fault));
    EXPECT_EQ(OB_TIMEOUT, submit());
    EXPECT_EQ(0, end_event->occur_) << "the production transaction end must be reached";
    // The failed end result must not be normalized to success; once cleared,
    // another retry can still finish without changing the winning marker.
    ASSERT_EQ(OB_SUCCESS, submit());
#endif
    ObMdsValueMeta retry_meta;
    ASSERT_EQ(OB_SUCCESS, read_marker(retry_meta));
    EXPECT_TRUE(marker.is_reloc_done());
    EXPECT_EQ(winner_redo, marker.get_redo_scn());
    EXPECT_EQ(winner_meta.writer_trans_id_, retry_meta.writer_trans_id_);
    EXPECT_EQ(winner_meta.commit_version_, retry_meta.commit_version_);
    EXPECT_EQ(winner_meta.commit_scn_, retry_meta.commit_scn_);
  }
}

TEST_F(TestMdsTransaction, reloc_v2_duplicate_op_row_checks_value_and_meta)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_dup_op_src");
    ObTabletID src_id;
    ObLSID src_ls_id;
    resolve_tablet_and_ls("test_reloc_dup_op_src", src_id, src_ls_id);
    ObLSHandle src_ls_handle;
    ObTabletHandle src_handle;
    get_ls_and_tablet(src_id, src_ls_id, src_ls_handle, src_handle);
    ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(src_ls_handle.get_ls()));
    common::ObMySQLTransaction source_tx;
    ASSERT_EQ(OB_SUCCESS, source_tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    auto *source_conn = static_cast<observer::ObInnerSQLConnection *>(source_tx.get_connection());
    ASSERT_NE(nullptr, source_conn);
    register_test_mds_in_tx(source_conn, OB_SYS_TENANT_ID, src_ls_id, src_id,
        ObTxDataSourceType::TEST_MULTI_ROW, 7, 77);
    ASSERT_EQ(OB_SUCCESS, source_tx.end(true));
    common::ObArenaAllocator allocator;
    ObTestMdsMultiRowValue source_value;
    ObMdsValueMeta source_meta;
    ASSERT_EQ(OB_SUCCESS, (src_ls_handle.get_ls()->get_mds_table()->get<
        ObTestMdsMultiRowKey, ObTestMdsMultiRowValue>(
        &allocator, src_id, src_handle.get_obj()->get_reorganization_scn(),
        ObTestMdsMultiRowKey(7), share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, false, source_value, source_meta)));
    ASSERT_TRUE(source_meta.is_committed());
    ASSERT_EQ(77, source_value.value_);
    // Each destination starts with an independently committed op row but no
    // reloc marker, forcing final to pull and take the duplicate-check branch.
    // Isolate each semantic field so no mismatch can hide another.
    for (int variant = 0; variant < 6; ++variant) {
      SCOPED_TRACE(variant);
      char table_name[64] = {};
      ASSERT_EQ(OB_SUCCESS, databuff_printf(table_name, sizeof(table_name),
          "test_reloc_dup_op_dst_%d", variant));
      create_table(table_name);
      ObTabletID dest_id;
      ObLSID dest_ls_id;
      resolve_tablet_and_ls(table_name, dest_id, dest_ls_id);
      ObLSHandle dest_ls_handle;
      ObTabletHandle dest_handle;
      get_ls_and_tablet(dest_id, dest_ls_id, dest_ls_handle, dest_handle);
      ObLS *dest_ls = dest_ls_handle.get_ls();
      ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(dest_ls));
      const share::SCN generation = mock_scn(1800 + variant);
      write_committed_transfer_in_status(*dest_ls, *dest_handle.get_obj(), dest_id,
          generation, generation, ObTransID(1460 + variant));
      ObMdsValueMeta seeded_meta;
      seeded_meta.assign(source_meta);
      ObTestMdsMultiRowValue seeded_value;
      ASSERT_EQ(OB_SUCCESS, seeded_value.assign(source_value));
      if (1 == variant) {
        ++seeded_value.value_;
      } else if (5 == variant) {
        // operator== compares value_ only; the serialized value must also
        // reject a mismatch in its persisted callback fields.
        ++seeded_value.redo_scn_;
      }
      if (2 == variant) {
        seeded_meta.writer_trans_id_ = ObTransID(source_meta.writer_trans_id_.get_id() + 1);
      } else if (3 == variant) {
        ++seeded_meta.commit_version_;
      } else if (4 == variant) {
        ++seeded_meta.commit_scn_;
      }
      UtMemtableMdsWriteCtx seed_ctx(ObTransID(1470 + variant));
      ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
          *dest_ls, *dest_handle.get_obj(), dest_id, seed_ctx));
      ASSERT_EQ(OB_SUCCESS, dest_ls->get_mds_table()->write(
          dest_id, generation, ObTestMdsMultiRowKey(7), seeded_value, seeded_meta,
          *seed_ctx.mds_ctx().get_store_ctx(), seed_ctx.mds_ctx(), true));
      share::SCN seed_commit;
      ASSERT_EQ(OB_SUCCESS, dest_ls->get_max_decided_scn(seed_commit));
      seed_ctx.commit(seed_commit);

      common::ObMySQLTransaction reloc_tx;
      ASSERT_EQ(OB_SUCCESS, reloc_tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
      auto *conn = static_cast<observer::ObInnerSQLConnection *>(reloc_tx.get_connection());
      ASSERT_NE(nullptr, conn);
      for (const bool is_final : {false, true}) {
        char buffer[1024] = {};
        int64_t pos = 0;
        serialize_v2_reloc_marker_batch(buffer, sizeof(buffer), pos, dest_ls_id,
            dest_id, generation, src_ls_id, src_id,
            src_handle.get_obj()->get_reorganization_scn(), is_final);
        EXPECT_EQ(is_final && 0 != variant ? OB_ERR_UNEXPECTED : OB_SUCCESS,
            conn->register_multi_data_source(OB_SYS_TENANT_ID, dest_ls_id,
                ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, buffer, pos));
      }
      ASSERT_EQ(OB_SUCCESS, reloc_tx.end(0 == variant));
      check_committed_reloc_marker(*dest_ls, dest_id, generation, generation, 0 == variant);
      ObTestMdsMultiRowValue actual_value;
      ObMdsValueMeta actual_meta;
      ASSERT_EQ(OB_SUCCESS, (dest_ls->get_mds_table()->get<
          ObTestMdsMultiRowKey, ObTestMdsMultiRowValue>(
          &allocator, dest_id, generation, ObTestMdsMultiRowKey(7), share::SCN::max_scn(),
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, false, actual_value, actual_meta)));
      EXPECT_EQ(seeded_value.value_, actual_value.value_);
      EXPECT_EQ(seeded_value.commit_version_, actual_value.commit_version_);
      EXPECT_EQ(seeded_value.commit_scn_, actual_value.commit_scn_);
      EXPECT_EQ(seeded_value.redo_scn_, actual_value.redo_scn_);
      EXPECT_EQ(seeded_meta.writer_trans_id_, actual_meta.writer_trans_id_);
      EXPECT_EQ(seeded_meta.commit_version_, actual_meta.commit_version_);
      EXPECT_EQ(seeded_meta.commit_scn_, actual_meta.commit_scn_);
    }
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_v2_pulls_real_op_row)
{
  static const char *SRC_TABLE = "test_mds_reloc_v2_src";
  static const char *DEST_TABLE = "test_mds_reloc_v2_dest";
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table(SRC_TABLE);
    create_table(DEST_TABLE);
    ObTabletID src_tablet_id;
    ObTabletID dest_tablet_id;
    ObLSID src_ls_id;
    ObLSID dest_ls_id;
    resolve_tablet_and_ls(SRC_TABLE, src_tablet_id, src_ls_id);
    resolve_tablet_and_ls(DEST_TABLE, dest_tablet_id, dest_ls_id);

    ObLSHandle src_ls_handle;
    ObLSHandle dest_ls_handle;
    ObTabletHandle src_tablet_handle;
    ObTabletHandle dest_tablet_handle;
    get_ls_and_tablet(src_tablet_id, src_ls_id, src_ls_handle, src_tablet_handle);
    get_ls_and_tablet(dest_tablet_id, dest_ls_id, dest_ls_handle, dest_tablet_handle);
    ObTablet *src_tablet = src_tablet_handle.get_obj();
    ObTablet *dest_tablet = dest_tablet_handle.get_obj();
    ObLS *dest_ls = dest_ls_handle.get_ls();
    ASSERT_NE(nullptr, src_tablet);
    ASSERT_NE(nullptr, dest_tablet);
    ASSERT_NE(nullptr, dest_ls);

    const int64_t op_key = 7;
    const int64_t op_value = 77;
    common::ObMySQLTransaction source_tx;
    ASSERT_EQ(OB_SUCCESS, source_tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *source_conn =
        static_cast<observer::ObInnerSQLConnection *>(source_tx.get_connection());
    ASSERT_NE(nullptr, source_conn);
    register_test_mds_in_tx(source_conn, OB_SYS_TENANT_ID, src_ls_id, src_tablet_id,
        ObTxDataSourceType::TEST_MULTI_ROW, op_key, op_value);
    ASSERT_EQ(OB_SUCCESS, source_tx.end(true));

    share::SCN source_commit_version;
    read_multi_row_key_get_latest(*src_tablet, ObTestMdsMultiRowKey(op_key),
        op_value, OB_SUCCESS, true, &source_commit_version);
    ASSERT_TRUE(source_commit_version.is_valid_and_not_min());

    const share::SCN transfer_scn = mock_scn(1700);
    write_committed_transfer_in_status(*dest_ls, *dest_tablet, dest_tablet_id,
        transfer_scn, transfer_scn, ObTransID(407));

    constexpr int64_t BUF_LEN = 1024;
    common::ObMySQLTransaction reloc_tx;
    ASSERT_EQ(OB_SUCCESS, reloc_tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    observer::ObInnerSQLConnection *reloc_conn =
        static_cast<observer::ObInnerSQLConnection *>(reloc_tx.get_connection());
    ASSERT_NE(nullptr, reloc_conn);
    char lock_buffer[BUF_LEN] = {};
    int64_t lock_pos = 0;
    serialize_v2_reloc_marker_batch(
        lock_buffer, BUF_LEN, lock_pos, dest_ls_id, dest_tablet_id, transfer_scn,
        src_ls_id, src_tablet_id, src_tablet->get_reorganization_scn(), false /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, reloc_conn->register_multi_data_source(
        OB_SYS_TENANT_ID, dest_ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        lock_buffer, lock_pos));
    char final_buffer[BUF_LEN] = {};
    int64_t final_pos = 0;
    serialize_v2_reloc_marker_batch(
        final_buffer, BUF_LEN, final_pos, dest_ls_id, dest_tablet_id, transfer_scn,
        src_ls_id, src_tablet_id, src_tablet->get_reorganization_scn(), true /*is_final*/);
    ASSERT_EQ(OB_SUCCESS, reloc_conn->register_multi_data_source(
        OB_SYS_TENANT_ID, dest_ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH,
        final_buffer, final_pos));
    ASSERT_EQ(OB_SUCCESS, reloc_tx.end(true));

    // Reloc writes the pulled row into the destination LS_MDS_TABLET.  The
    // shared-storage refresh stage installs it into the user tablet later, so
    // verify the aggregate row directly here instead of reading dest_tablet.
    common::ObArenaAllocator read_allocator;
    ObTestMdsMultiRowValue dest_value;
    ObMdsValueMeta dest_meta;
    ret = dest_ls->get_mds_table()->get<ObTestMdsMultiRowKey, ObTestMdsMultiRowValue>(
            &read_allocator,
            dest_tablet_id,
            transfer_scn,
            ObTestMdsMultiRowKey(op_key),
            share::SCN::max_scn(),
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
            false /*read_uncommitted*/,
            dest_value,
            dest_meta);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(op_value, dest_value.value_);
    EXPECT_TRUE(dest_meta.is_committed());
    EXPECT_EQ(source_commit_version, dest_meta.get_commit_version());
    check_committed_reloc_marker(
        *dest_ls, dest_tablet_id, transfer_scn, transfer_scn, true);
  }
}

// Flush each batch separately, so max_applying_log_ts identifies the redo that
// owns this batch's destination writes, rather than the source rows' old SCNs.
static void wait_reloc_batch_redo(ObLS &ls, const ObTransID &tx_id,
                                 const int64_t expected_batches, share::SCN &redo_scn)
{
  ObPartTransCtx *tx_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls.get_tx_ctx(tx_id, false, tx_ctx));
  ASSERT_NE(nullptr, tx_ctx);
  struct RevertCtx {
    ObLS &ls_;
    ObPartTransCtx *ctx_;
    ~RevertCtx() { EXPECT_EQ(OB_SUCCESS, ls_.revert_tx_ctx(ctx_)); }
  } revert{ls, tx_ctx};
  int64_t synced = 0;
  for (int i = 0; i < 500 && synced < expected_batches; ++i) {
    {
      CtxLockGuard guard(tx_ctx->lock_);
      synced = 0;
      for (int64_t j = 0; j < tx_ctx->exec_info_.multi_data_source_.count(); ++j) {
        const auto &node = tx_ctx->exec_info_.multi_data_source_.at(j);
        if (node.get_data_source_type() == ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH
            && node.is_synced()) {
          ++synced;
        }
      }
      redo_scn = tx_ctx->exec_info_.max_applying_log_ts_;
    }
    if (synced < expected_batches) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  ASSERT_EQ(expected_batches, synced);
  ASSERT_TRUE(redo_scn.is_valid_and_not_min());
}

TEST_F(TestMdsTransaction, reloc_v2_multibatch_medium_redo_fence)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_medium_src");
    create_table("test_reloc_medium_dst");
    create_table("test_reloc_medium_replay");
    create_table("test_reloc_medium_producer");
    ObTabletID ids[4];
    ObLSID ls_ids[4];
    ObLSHandle ls_handles[4];
    ObTabletHandle handles[4];
    const char *names[] = {"test_reloc_medium_src", "test_reloc_medium_dst",
                          "test_reloc_medium_replay", "test_reloc_medium_producer"};
    for (int i = 0; i < 4; ++i) {
      resolve_tablet_and_ls(names[i], ids[i], ls_ids[i]);
      get_ls_and_tablet(ids[i], ls_ids[i], ls_handles[i], handles[i]);
      ASSERT_NO_FATAL_FAILURE(wait_ss_inc_uploader_ready(ls_handles[i].get_ls()));
      ASSERT_EQ(ls_ids[0], ls_ids[i]);
    }
    ObLS *ls = ls_handles[0].get_ls();
    common::ObArenaAllocator allocator;
    ObStorageSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, handles[0].get_obj()->load_storage_schema(allocator, schema));
    ASSERT_NE(nullptr, schema);
    share::SCN source_commit;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(source_commit));
    compaction::ObMediumCompactionInfo medium[2];
    UtMemtableMdsWriteCtx source_ctx(ObTransID(1500));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *handles[0].get_obj(), ids[0], source_ctx));
    for (int i = 0; i < 2; ++i) {
      medium[i].compaction_type_ = compaction::ObMediumCompactionInfo::MEDIUM_COMPACTION;
      medium[i].medium_snapshot_ = source_commit.get_val_for_tx() + 100 + i;
      medium[i].last_medium_snapshot_ = medium[i].medium_snapshot_ - 1;
      medium[i].cluster_id_ = GCONF.cluster_id;
      medium[i].tenant_id_ = OB_SYS_TENANT_ID;
      medium[i].encoding_granularity_ = 16 + i;
      ASSERT_EQ(OB_SUCCESS, medium[i].init_data_version(DATA_VERSION_4_6_3_1));
      ASSERT_EQ(OB_SUCCESS, medium[i].storage_schema_.init(allocator, *schema));
      ASSERT_TRUE(medium[i].is_valid());
      ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(
          ids[0], handles[0].get_obj()->get_reorganization_scn(),
          compaction::ObMediumCompactionInfoKey(medium[i].medium_snapshot_), medium[i],
          *source_ctx.mds_ctx().get_store_ctx(), source_ctx.mds_ctx()));
    }
    source_ctx.commit(source_commit);
    const share::SCN generation = mock_scn(1900);
    for (int i = 1; i < 3; ++i) {
      write_committed_transfer_in_status(*ls, *handles[i].get_obj(), ids[i],
          generation, generation, ObTransID(1500 + i));
    }

    // One mutex batch, one materialized payload batch, and one final op-pull
    // batch. Force a real redo after each registration before proceeding.
    ObTabletMdsRelocBatchArg batch;
    batch.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
    batch.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
    batch.ls_id_ = batch.src_ls_id_ = ls_ids[0];
    batch.read_snapshot_ = share::SCN::max_scn();
    ObTabletMdsRelocTarget target;
    target.tablet_id_ = ids[1];
    target.transfer_scn_ = generation;
    target.src_tablet_id_ = ids[0];
    target.src_reorg_scn_ = handles[0].get_obj()->get_reorganization_scn();
    ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));
    ObTabletBindingMdsUserData binding;
    binding.set_default_value();
    char payload[1024] = {};
    int64_t payload_pos = 0;
    ASSERT_EQ(OB_SUCCESS, mds::DummyKey().mds_serialize(payload, sizeof(payload), payload_pos));
    ASSERT_EQ(OB_SUCCESS, binding.serialize(payload, sizeof(payload), payload_pos));
    ObTabletMdsRelocRowStorage row;
    row.mds_unit_id_ = mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletBindingMdsUserData>::value;
    row.meta_.writer_trans_id_ = ObTransID(1500);
    row.meta_.commit_version_ = row.meta_.commit_scn_ = source_commit.get_val_for_tx();
    row.ser_payload_.assign_ptr(payload, payload_pos);
    common::ObMySQLTransaction tx;
    ASSERT_EQ(OB_SUCCESS, tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    auto *conn = static_cast<observer::ObInnerSQLConnection *>(tx.get_connection());
    ASSERT_NE(nullptr, conn);
    share::SCN batch_redo[3];
    for (int i = 0; i < 3; ++i) {
      SCOPED_TRACE(i);
      batch.rows_.reset();
      batch.skip_bitmap_.reset();
      batch.is_final_ = (2 == i);
      if (1 == i) {
        ASSERT_EQ(OB_SUCCESS, batch.rows_.push_back(row));
        ASSERT_EQ(OB_SUCCESS, batch.skip_bitmap_.init(batch.rows_.count(), batch.targets_.count()));
      }
      char buffer[4096] = {};
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, batch.serialize(buffer, sizeof(buffer), pos));
      transaction::ObRegisterMdsFlag flag;
      flag.need_flush_redo_instantly_ = true;
      ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(OB_SYS_TENANT_ID,
          ls_ids[0], ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, buffer, pos, flag));
      ASSERT_NO_FATAL_FAILURE(wait_reloc_batch_redo(*ls, conn->get_session().get_tx_id(), i + 1, batch_redo[i]));
      if (i > 0) {
        ASSERT_GT(batch_redo[i], batch_redo[i - 1]);
      }
    }
    ASSERT_EQ(OB_SUCCESS, tx.end(true));
    ObTabletMdsRelocStatus marker;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_latest_committed_reloc_status(
        *ls->get_mds_table(), ids[1], generation, 5_s, marker));
    EXPECT_EQ(batch_redo[2], marker.get_redo_scn());
    EXPECT_GT(marker.get_redo_scn(), batch_redo[1]);

    // Also pass the same real Medium rows through the production producer.
    // Split shares submit_selected_rows_/submit_reloc_rows_by_type_ and uses
    // a min-SCN destination generation. Its actual redo must omit Medium,
    // while final pull must still reproduce the complete source value/meta.
    ObTabletCreateDeleteMdsUserData split_status;
    split_status.tablet_status_ = ObTabletStatus::SPLIT_DST;
    split_status.data_type_ = ObTabletMdsUserDataType::START_SPLIT_DST;
    split_status.transfer_scn_ = share::SCN::min_scn();
    UtMemtableMdsWriteCtx split_status_ctx(ObTransID(1520));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *handles[3].get_obj(), ids[3], split_status_ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(ids[3], share::SCN::min_scn(),
        split_status, *split_status_ctx.mds_ctx().get_store_ctx(), split_status_ctx.mds_ctx()));
    share::SCN split_status_commit;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(split_status_commit));
    split_status_ctx.commit(split_status_commit);
    ObArray<ObTabletID> producer_targets;
    ASSERT_EQ(OB_SUCCESS, producer_targets.push_back(ids[3]));
    palf::LSN submit_begin, submit_end;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(submit_begin));
    ASSERT_EQ(OB_SUCCESS, ObTabletSplitMdsReloc::submit_split_multikey_mds_reloc(
        *handles[0].get_obj(), producer_targets, share::SCN::max_scn(),
        OB_SYS_TENANT_ID, ls_ids[0], DATA_VERSION_4_6_3_1));
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_end_lsn(submit_end));
    ObArray<ObString> submitted_batches;
    ASSERT_NO_FATAL_FAILURE(collect_submitted_reloc_batches(
        *ls, submit_begin, submit_end, ids[3], allocator, submitted_batches));
    ASSERT_GE(submitted_batches.count(), 2); // marker mutex and final
    int64_t final_count = 0;
    constexpr uint8_t medium_type = mds_type_id_detail::TypeIdOf<
        compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>::value;
    for (int64_t i = 0; i < submitted_batches.count(); ++i) {
      const ObString &data = submitted_batches.at(i);
      ObTabletMdsRelocBatchArg submitted;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, submitted.deserialize(data.ptr(), data.length(), pos));
      ASSERT_TRUE(submitted.is_v2());
      EXPECT_EQ(ObTabletMdsRelocType::SPLIT, submitted.reloc_type_);
      for (const auto &submitted_row : submitted.rows_) {
        EXPECT_NE(medium_type, submitted_row.mds_unit_id_);
      }
      if (submitted.is_final_) {
        ++final_count;
        EXPECT_EQ(submitted_batches.count() - 1, i);
      }
    }
    EXPECT_EQ(1, final_count);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_latest_committed_reloc_status(
        *ls->get_mds_table(), ids[3], share::SCN::min_scn(), 5_s, marker));
    EXPECT_TRUE(marker.has_captured_redo_fence());

    // Exercise replay's concrete Medium type dispatch against a separate
    // destination, preserving the source value and original transaction meta.
    ObTabletHandle mds_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_tablet_handle_(mds_handle));
    struct RestoreCheckpoint {
      ObTablet *tablet_;
      share::SCN checkpoint_;
      ~RestoreCheckpoint() { tablet_->tablet_meta_.clog_checkpoint_scn_ = checkpoint_; }
    } restore{mds_handle.get_obj(), mds_handle.get_obj()->get_clog_checkpoint_scn()};
    batch.targets_.at(0).tablet_id_ = ids[2];
    // A relocation transaction can replay several rows in one batch; use the
    // transaction memtable context rather than the single-log one-row slot.
    UtMemtableMdsWriteCtx replay_ctx(ObTransID(1510));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *handles[2].get_obj(), ids[2], replay_ctx));
    replay_ctx.mds_ctx().set_for_replay(true);
    replay_ctx.mds_ctx().clear_store_ctx();
    auto *replay_mem_ctx = static_cast<memtable::ObMemtableCtx *>(replay_ctx.mds_ctx().get_mem_ctx());
    ASSERT_NE(nullptr, replay_mem_ctx);
    // The callback manager also needs replay mode to capture each redo fence
    // in before_append instead of waiting for leader log submission.
    replay_mem_ctx->set_for_replay(true);
    for (int i = 0; i < 3; ++i) {
      SCOPED_TRACE(i);
      batch.rows_.reset();
      batch.skip_bitmap_.reset();
      batch.is_final_ = (2 == i);
      if (1 == i) {
        ASSERT_EQ(OB_SUCCESS, batch.rows_.push_back(row));
        ASSERT_EQ(OB_SUCCESS, batch.skip_bitmap_.init(batch.rows_.count(), batch.targets_.count()));
      }
      char buffer[4096] = {};
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, batch.serialize(buffer, sizeof(buffer), pos));
      mds_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_.set_min();
      ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::on_replay(buffer, pos, batch_redo[i], replay_ctx.mds_ctx()));
    }
    replay_ctx.commit(batch_redo[2]);
    for (int dest = 1; dest < 4; ++dest) {
      const share::SCN dest_generation = 3 == dest ? share::SCN::min_scn() : generation;
      ObMdsTableScanIterator iter;
      constexpr uint8_t medium_type = mds_type_id_detail::TypeIdOf<
          compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>::value;
      ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->table_scan_by_mds_type_(
          allocator, ids[dest], dest_generation, medium_type, share::SCN::max_scn(), iter));
      for (int i = 0; i < 2; ++i) {
        compaction::ObMediumCompactionInfoKey key;
        compaction::ObMediumCompactionInfo value;
        ObMdsValueMeta meta;
        ASSERT_EQ(OB_SUCCESS, iter.get_next_mds_kv(allocator, key, value, meta));
        EXPECT_EQ(medium[i].medium_snapshot_, value.medium_snapshot_);
      }
      compaction::ObMediumCompactionInfoKey end_key;
      compaction::ObMediumCompactionInfo end_value;
      ObMdsValueMeta end_meta;
      EXPECT_EQ(OB_ITER_END, iter.get_next_mds_kv(allocator, end_key, end_value, end_meta));
      for (int i = 0; i < 2; ++i) {
        compaction::ObMediumCompactionInfo actual;
        ObMdsValueMeta meta;
        ASSERT_EQ(OB_SUCCESS, (ls->get_mds_table()->get<compaction::ObMediumCompactionInfoKey,
            compaction::ObMediumCompactionInfo>(&allocator, ids[dest], dest_generation,
            compaction::ObMediumCompactionInfoKey(medium[i].medium_snapshot_),
            share::SCN::max_scn(), 5_s, false, actual, meta)));
        const int64_t size = medium[i].get_serialize_size();
        ASSERT_EQ(size, actual.get_serialize_size());
        char *expected_buf = static_cast<char *>(allocator.alloc(size));
        char *actual_buf = static_cast<char *>(allocator.alloc(size));
        ASSERT_NE(nullptr, expected_buf);
        ASSERT_NE(nullptr, actual_buf);
        int64_t expected_pos = 0, actual_pos = 0;
        ASSERT_EQ(OB_SUCCESS, medium[i].serialize(expected_buf, size, expected_pos));
        ASSERT_EQ(OB_SUCCESS, actual.serialize(actual_buf, size, actual_pos));
        EXPECT_EQ(0, memcmp(expected_buf, actual_buf, size));
        EXPECT_EQ(ObTransID(1500), meta.writer_trans_id_);
        EXPECT_EQ(source_commit.get_val_for_tx(), meta.commit_version_);
        EXPECT_EQ(source_commit.get_val_for_tx(), meta.commit_scn_);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_latest_committed_reloc_status(
        *ls->get_mds_table(), ids[2], generation, 5_s, marker));
    EXPECT_EQ(batch_redo[2], marker.get_redo_scn());

    // Probe the gates last: a lagging checkpoint deliberately requests a flush.
    bool ready = true;
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, ids[1], generation, batch_redo[1], ready));
    EXPECT_FALSE(ready);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
        ls, ids[1], generation, batch_redo[2], ready));
    EXPECT_TRUE(ready);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_local_checkpoint_reached(
        ls, ids[1], generation, batch_redo[1], ready));
    EXPECT_FALSE(ready);
    ASSERT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_local_checkpoint_reached(
        ls, ids[1], generation, batch_redo[2], ready));
    EXPECT_TRUE(ready);

  }
}

TEST_F(TestMdsTransaction, tablet_reloc_v2_waits_for_source_log_handler_init)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    // Model the window after publishing the source LS but before initializing
    // its log handler. Exercise the real readiness query without mocking ret.
    ObLS src_ls;
    share::SCN max_decided_scn;
    ASSERT_EQ(OB_NOT_INIT, src_ls.get_max_decided_scn(max_decided_scn));

    ObTabletMdsRelocTarget target;
    target.tablet_id_ = ObTabletID(200002);
    target.src_tablet_id_ = ObTabletID(200001);
    target.transfer_scn_ = share::SCN::min_scn();
    target.src_reorg_scn_ = share::SCN::min_scn();
    for (const bool for_replay : {false, true}) {
      ObTabletHandle ready_handle;
      EXPECT_EQ(for_replay ? OB_EAGAIN : OB_NOT_INIT,
          ObTabletMdsRelocHelper::get_ready_source_tablet_(
              for_replay, src_ls, target, share::SCN::max_scn(),
              ObLSID(1001), ready_handle));
      EXPECT_EQ(nullptr, ready_handle.get_obj());
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMdsTransaction, tablet_reloc_v2_rejects_source_generation_mismatch)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_generation_src");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_reloc_generation_src", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ObTabletMdsRelocTarget target;
    target.tablet_id_ = tablet_id;
    target.src_tablet_id_ = tablet_id;
    target.transfer_scn_ = share::SCN::min_scn();
    target.src_reorg_scn_ = share::SCN::max_scn();
    ASSERT_NE(target.src_reorg_scn_, tablet_handle.get_obj()->get_reorganization_scn());
    for (const bool for_replay : {false, true}) {
      ObTabletHandle ready_handle;
      EXPECT_EQ(for_replay ? OB_EAGAIN : OB_STATE_NOT_MATCH,
          ObTabletMdsRelocHelper::get_ready_source_tablet_(
              for_replay, *ls, target, share::SCN::max_scn(), ls_id, ready_handle));
    }
    target.src_reorg_scn_ = tablet_handle.get_obj()->get_reorganization_scn();
    for (const bool for_replay : {false, true}) {
      ObTabletHandle ready_handle;
      EXPECT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::get_ready_source_tablet_(
          for_replay, *ls, target, share::SCN::max_scn(), ls_id, ready_handle));
    }
  }
}

TEST_F(TestMdsTransaction, reloc_absent_source_cancels_only_current_migration)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_absent_source");
    ObTabletID tablet_id;
    ObLSID ls_id;
    resolve_tablet_and_ls("test_reloc_absent_source", tablet_id, ls_id);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    get_ls_and_tablet(tablet_id, ls_id, ls_handle, tablet_handle);
    ObLS *ls = ls_handle.get_ls();
    auto *handler = ls->get_ls_migration_handler();
    auto *ha_service = MTL(ObStorageHAService *);
    ASSERT_NE(nullptr, ha_service);
    // Drive the real handler ourselves, without the HA timer racing this fixture.
    ha_service->stop();
    ha_service->wait();
    struct RestartHA {
      ObStorageHAService *service_;
      ~RestartHA() { EXPECT_EQ(OB_SUCCESS, service_->start()); }
    } restart_ha{ha_service};
    ASSERT_TRUE(handler->task_list_.empty());
    struct RestoreHandler {
      ObLSMigrationHandler *handler_;
      int32_t result_;
      bool cancelled_;
      bool dag_cleared_;
      ~RestoreHandler() {
        handler_->task_list_.reset();
        handler_->result_ = result_;
        handler_->is_cancel_ = cancelled_;
        handler_->is_dag_net_cleared_ = dag_cleared_;
      }
    } restore_handler{handler, handler->result_, handler->is_cancel_, handler->is_dag_net_cleared_};
    handler->result_ = OB_SUCCESS;
    handler->is_cancel_ = false;
    EXPECT_EQ(OB_SUCCESS, handler->set_result(OB_LOG_REPLAY_ERROR));
    EXPECT_EQ(OB_SUCCESS, handler->result_);
    share::ObTaskId task_id;
    task_id.init(GCTX.self_addr());
    ObMigrationOpArg migration;
    migration.ls_id_ = ls_id;
    migration.type_ = ObMigrationOpType::REBUILD_LS_OP;
    migration.cluster_id_ = 1;
    migration.src_ = migration.dst_ = common::ObReplicaMember(GCTX.self_addr(), 1);
    ASSERT_TRUE(migration.is_valid());
    ASSERT_EQ(OB_SUCCESS, handler->add_ls_migration_task(task_id, migration));
    // A DAG being torn down still needs cancellation even after it leaves the map.
    handler->is_dag_net_cleared_ = false;
    share::ObTaskId other_id;
    other_id.init(GCTX.self_addr());
    ASSERT_NE(task_id, other_id);
    EXPECT_EQ(OB_SUCCESS, handler->set_result_for_task(OB_TIMEOUT, other_id));
    EXPECT_EQ(OB_SUCCESS, handler->result_);

    ObTabletHandle mds_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_tablet_handle_(mds_handle));
    struct RestoreCheckpoint {
      ObTablet *tablet_;
      share::SCN checkpoint_;
      ~RestoreCheckpoint() { tablet_->tablet_meta_.clog_checkpoint_scn_ = checkpoint_; }
    } restore_checkpoint{mds_handle.get_obj(), mds_handle.get_obj()->get_clog_checkpoint_scn()};
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(replay_scn));
    ASSERT_TRUE(replay_scn.is_valid_and_not_min());
    const share::SCN generation = mock_scn(2);
    ObTabletMdsRelocBatchArg batch;
    batch.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
    batch.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
    batch.ls_id_ = batch.src_ls_id_ = ls_id;
    batch.read_snapshot_ = share::SCN::max_scn();
    batch.is_final_ = true;
    ObTabletMdsRelocTarget target;
    target.tablet_id_ = tablet_id;
    target.transfer_scn_ = generation;
    target.src_tablet_id_ = ObTabletID(999999);
    target.src_reorg_scn_ = share::SCN::min_scn();
    ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));
    ObTabletBindingMdsUserData payload;
    mds::DummyKey key;
    char row_buffer[1024] = {};
    int64_t row_pos = 0;
    ASSERT_EQ(OB_SUCCESS, key.mds_serialize(row_buffer, sizeof(row_buffer), row_pos));
    ASSERT_EQ(OB_SUCCESS, payload.serialize(row_buffer, sizeof(row_buffer), row_pos));
    ObTabletMdsRelocRowStorage row;
    row.mds_unit_id_ = mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletBindingMdsUserData>::value;
    row.meta_.writer_trans_id_ = ObTransID(440);
    row.meta_.commit_scn_ = row.meta_.commit_version_ = 1;
    row.ser_payload_.assign_ptr(row_buffer, row_pos);
    ASSERT_EQ(OB_SUCCESS, batch.rows_.push_back(row));
    ASSERT_EQ(OB_SUCCESS, batch.skip_bitmap_.init(1, 1));
    ASSERT_TRUE(batch.is_valid());
    char buffer[4096] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, batch.serialize(buffer, sizeof(buffer), pos));
    for (int attempt = 0; attempt < 200 && OB_SUCCESS == handler->result_; ++attempt) {
      mds_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_.set_min();
      ObTabletHandle absent_source;
      EXPECT_EQ(OB_EAGAIN, ObTabletMdsRelocHelper::get_ready_source_tablet_(
          true, *ls, target, batch.read_snapshot_, ls_id, absent_source));
      if (OB_SUCCESS == handler->result_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    ASSERT_EQ(OB_LOG_REPLAY_ERROR, handler->result_);
    char task_id_buf[128] = {};
    ASSERT_GT(task_id.to_string(task_id_buf, sizeof(task_id_buf)), 0);
    common::ObSqlString event_sql;
    ASSERT_EQ(OB_SUCCESS, event_sql.append_fmt(
        "SELECT count(*) AS cnt FROM oceanbase.__all_server_event_history "
        "WHERE module='MDS_RELOC' AND event='RELOC_SRC_ABSENT_REQUEST_FAIL_MIGRATION' "
        "AND name1='dest_ls_id' AND value1='%ld' "
        "AND name2='src_tablet_id' AND value2='%ld' "
        "AND name5='task_id' AND value5='%s' "
        "AND name6='requested_result' AND value6='%d'",
        ls_id.id(), target.src_tablet_id_.id(), task_id_buf, OB_LOG_REPLAY_ERROR));
    int64_t event_count = 0;
    for (int i = 0; i < 100 && 0 == event_count; ++i) {
      common::ObMySQLProxy::MySQLResult result;
      ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->read(result, OB_SYS_TENANT_ID, event_sql.ptr()));
      ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
      ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", event_count));
      if (0 == event_count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    EXPECT_GT(event_count, 0) << event_sql.ptr();
    mds_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_.set_min();
    UtSingleLogMdsWriteCtx ctx(MdsWriter(WriterType::MEDIUM_INFO, 441));
    ASSERT_EQ(OB_SUCCESS, ctx.single_log_ctx().init(ls_id, INT64_MAX, true));
    EXPECT_EQ(OB_EAGAIN, ObTabletMdsRelocHelper::on_replay(
        buffer, pos, replay_scn, ctx.mds_ctx()));
    // Check before abort, including uncommitted versions: rollback must not
    // hide accidental materialization before the source readiness probe.
    common::ObArenaAllocator allocator;
    ObMdsValueMeta meta;
    ObTabletMdsRelocStatus marker;
    const int marker_ret = ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
        &allocator, tablet_id, generation, share::SCN::max_scn(), 1_s, true, marker, meta);
    EXPECT_TRUE(OB_EMPTY_RESULT == marker_ret || OB_ITER_END == marker_ret) << marker_ret;
    const int payload_ret = ls->get_mds_table()->get<ObTabletBindingMdsUserData>(
        &allocator, tablet_id, generation, share::SCN::max_scn(), 1_s, true, payload, meta);
    EXPECT_TRUE(OB_EMPTY_RESULT == payload_ret || OB_ITER_END == payload_ret) << payload_ret;
    ctx.abort();
    bool need_wait = false;
    int32_t task_result = OB_SUCCESS;
    EXPECT_EQ(OB_SUCCESS, handler->handle_current_task_(need_wait, task_result));
    EXPECT_TRUE(need_wait);
    EXPECT_TRUE(handler->is_cancel_);
    EXPECT_EQ(OB_LOG_REPLAY_ERROR, task_result);
    bool done = true;
    EXPECT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
        ls, tablet_id, generation, done));
    EXPECT_FALSE(done);
    // Delayed failure from the old task must not affect its replacement.
    handler->task_list_.reset();
    handler->result_ = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, handler->add_ls_migration_task(other_id, migration));
    EXPECT_EQ(OB_SUCCESS, handler->set_result_for_task(OB_LOG_REPLAY_ERROR, task_id));
    EXPECT_EQ(OB_SUCCESS, handler->result_);
    // A rebuilt snapshot covering this final redo no longer needs its source.
    mds_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_ = replay_scn;
    UtSingleLogMdsWriteCtx covered_ctx(MdsWriter(WriterType::MEDIUM_INFO, 442));
    ASSERT_EQ(OB_SUCCESS, covered_ctx.single_log_ctx().init(ls_id, INT64_MAX, true));
    EXPECT_EQ(OB_SUCCESS, ObTabletMdsRelocHelper::on_replay(
        buffer, pos, replay_scn, covered_ctx.mds_ctx()));
    covered_ctx.abort();
    EXPECT_EQ(OB_SUCCESS, handler->result_);
  }
}

TEST_F(TestMdsTransaction, tablet_reloc_v2_split_filters_latest_version_at_snapshot)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_table("test_reloc_snapshot_src");
    create_table("test_reloc_snapshot_dst");
    ObTabletID src_id;
    ObTabletID dst_id;
    ObLSID src_ls_id;
    ObLSID dst_ls_id;
    resolve_tablet_and_ls("test_reloc_snapshot_src", src_id, src_ls_id);
    resolve_tablet_and_ls("test_reloc_snapshot_dst", dst_id, dst_ls_id);
    ASSERT_EQ(src_ls_id, dst_ls_id);
    ObLSHandle src_ls_handle;
    ObLSHandle dst_ls_handle;
    ObTabletHandle src_handle;
    ObTabletHandle dst_handle;
    get_ls_and_tablet(src_id, src_ls_id, src_ls_handle, src_handle);
    get_ls_and_tablet(dst_id, dst_ls_id, dst_ls_handle, dst_handle);
    ObLS *ls = dst_ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(share::SCN::min_scn(), dst_handle.get_obj()->get_reorganization_scn());

    const auto write_source = [&](const int64_t key, const int64_t value) {
      common::ObMySQLTransaction tx;
      ASSERT_EQ(OB_SUCCESS, tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
      auto *conn = static_cast<observer::ObInnerSQLConnection *>(tx.get_connection());
      ASSERT_NE(nullptr, conn);
      register_test_mds_in_tx(conn, OB_SYS_TENANT_ID, src_ls_id, src_id,
          ObTxDataSourceType::TEST_MULTI_ROW, key, value, false /*check_dup*/);
      ASSERT_EQ(OB_SUCCESS, tx.end(true));
    };
    write_source(7, 70);
    write_source(8, 80);
    share::SCN snapshot;
    read_multi_row_key_get_latest(*src_handle.get_obj(), ObTestMdsMultiRowKey(8),
        80, OB_SUCCESS, true, &snapshot);
    write_source(7, 71);
    share::SCN later_commit;
    read_multi_row_key_get_latest(*src_handle.get_obj(), ObTestMdsMultiRowKey(7),
        71, OB_SUCCESS, true, &later_commit);
    ASSERT_GT(later_commit, snapshot);

    // SPLIT_DST status and reloc marker use the same min transfer-SCN key.
    // split_start_scn is independent of that MDS rowkey generation.
    ObTabletCreateDeleteMdsUserData status;
    status.tablet_status_ = ObTabletStatus::SPLIT_DST;
    status.data_type_ = ObTabletMdsUserDataType::START_SPLIT_DST;
    status.transfer_scn_ = share::SCN::min_scn();
    UtMemtableMdsWriteCtx status_ctx(ObTransID(1408));
    ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
        *ls, *dst_handle.get_obj(), dst_id, status_ctx));
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->write(dst_id, share::SCN::min_scn(),
        status, *status_ctx.mds_ctx().get_store_ctx(), status_ctx.mds_ctx()));
    share::SCN status_commit;
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(status_commit));
    status_ctx.commit(status_commit);

    common::ObMySQLTransaction reloc_tx;
    ASSERT_EQ(OB_SUCCESS, reloc_tx.start(GCTX.ddl_sql_proxy_, OB_SYS_TENANT_ID));
    auto *conn = static_cast<observer::ObInnerSQLConnection *>(reloc_tx.get_connection());
    ASSERT_NE(nullptr, conn);
    ObTabletMdsRelocBatchArg batch;
    batch.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
    batch.reloc_type_ = ObTabletMdsRelocType::SPLIT;
    batch.ls_id_ = dst_ls_id;
    batch.src_ls_id_ = src_ls_id;
    batch.read_snapshot_ = snapshot;
    ObTabletMdsRelocTarget target;
    target.tablet_id_ = dst_id;
    target.transfer_scn_ = share::SCN::min_scn();
    target.src_tablet_id_ = src_id;
    target.src_reorg_scn_ = src_handle.get_obj()->get_reorganization_scn();
    ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));
    for (const bool is_final : {false, true}) {
      batch.is_final_ = is_final;
      ASSERT_TRUE(batch.is_valid());
      char buffer[1024] = {};
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, batch.serialize(buffer, sizeof(buffer), pos));
      ASSERT_EQ(OB_SUCCESS, conn->register_multi_data_source(OB_SYS_TENANT_ID,
          dst_ls_id, ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH, buffer, pos));
    }
    ASSERT_EQ(OB_SUCCESS, reloc_tx.end(true));

    common::ObArenaAllocator allocator;
    ObTestMdsMultiRowValue value;
    ObMdsValueMeta meta;
    const auto read_dest = [&](const int64_t key) {
      return ls->get_mds_table()->get<ObTestMdsMultiRowKey, ObTestMdsMultiRowValue>(
          &allocator, dst_id, share::SCN::min_scn(), ObTestMdsMultiRowKey(key),
          share::SCN::max_scn(), ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          false /*read_uncommitted*/, value, meta);
    };
    // A key whose latest version is newer than the business snapshot must be
    // absent, while a latest version exactly at the snapshot must be retained.
    ASSERT_EQ(OB_ITER_END, read_dest(7));
    ASSERT_EQ(OB_SUCCESS, read_dest(8));
    EXPECT_EQ(80, value.value_);
    EXPECT_EQ(snapshot, meta.get_commit_version());
    ObTabletMdsRelocStatus marker;
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get<ObTabletMdsRelocStatus>(
        &allocator, dst_id, share::SCN::min_scn(), share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, false, marker, meta));
    EXPECT_TRUE(marker.is_reloc_done());
    EXPECT_TRUE(marker.get_redo_scn().is_valid_and_not_min());
  }
}

TEST_F(TestMdsTransaction, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

void wait_ls_mds_checkpoint_reach(ObLS *ls,
                                  const share::SCN &target_scn,
                                  const int max_rounds,
                                  const int sleep_sec)
{
  ASSERT_NE(nullptr, ls);
  ASSERT_NE(nullptr, ls->get_mds_table());
  ASSERT_TRUE(target_scn.is_valid_and_not_min());
  share::SCN checkpoint_scn;
  for (int i = 0; i < max_rounds; ++i) {
    ASSERT_EQ(OB_SUCCESS, ls->get_mds_table()->get_mds_checkpoint_scn(checkpoint_scn));
    if (checkpoint_scn >= target_scn) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
  }
  ASSERT_TRUE(checkpoint_scn >= target_scn)
      << "LS aggregate MDS checkpoint did not reach reloc marker redo SCN";
}

void get_ss_ls_mds_clog_checkpoint_scn(ObLS *ls, share::SCN &checkpoint_scn)
{
  ASSERT_NE(nullptr, ls);
  common::ObArenaAllocator allocator;
  ObTabletHandle ss_mds_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSTransferBackfillUtils::get_ss_tablet(
      ls->get_ls_id(), LS_MDS_TABLET, share::SCN::min_scn(), allocator,
      ss_mds_tablet_handle, true /*read_mark_delete*/));
  ASSERT_NE(nullptr, ss_mds_tablet_handle.get_obj());
  checkpoint_scn = ss_mds_tablet_handle.get_obj()->get_clog_checkpoint_scn();
}

void wait_ss_ls_mds_checkpoint_reach(ObLS *ls,
                                     const share::SCN &target_scn,
                                     const int max_rounds,
                                     const int sleep_sec)
{
  ASSERT_NE(nullptr, ls);
  ASSERT_TRUE(target_scn.is_valid_and_not_min());
  share::SCN checkpoint_scn;
  for (int i = 0; i < max_rounds; ++i) {
    get_ss_ls_mds_clog_checkpoint_scn(ls, checkpoint_scn);
    if (checkpoint_scn >= target_scn) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
  }
  ASSERT_TRUE(checkpoint_scn >= target_scn)
      << "shared-storage LS_MDS_TABLET clog checkpoint did not reach reloc marker redo SCN";
}

void serialize_v2_reloc_marker_batch(char *buffer,
                                     const int64_t buf_len,
                                     int64_t &pos,
                                     const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id,
                                     const share::SCN &transfer_scn,
                                     const share::ObLSID &src_ls_id,
                                     const ObTabletID &src_tablet_id,
                                     const share::SCN &src_reorg_scn,
                                     const bool is_final,
                                     const ObTabletMdsRelocType reloc_type)
{
  ObTabletMdsRelocBatchArg arg;
  ObTabletMdsRelocTarget target;
  arg.version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2;
  arg.ls_id_ = ls_id;
  arg.src_ls_id_ = src_ls_id;
  arg.read_snapshot_ = share::SCN::max_scn();
  arg.reloc_type_ = reloc_type;
  target.tablet_id_ = tablet_id;
  target.transfer_scn_ = transfer_scn;
  target.src_tablet_id_ = src_tablet_id;
  target.src_reorg_scn_ = src_reorg_scn;
  ASSERT_EQ(OB_SUCCESS, arg.targets_.push_back(target));
  arg.is_final_ = is_final;
  ASSERT_TRUE(arg.is_v2());
  ASSERT_EQ(!is_final, arg.is_marker_lock_batch());
  ASSERT_TRUE(arg.is_valid());
  ASSERT_LT(arg.get_serialize_size(), buf_len);
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, buf_len, pos));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char *)"WDIAG";
  bool shared_storage_mode = false;
  char storage_info[1000] = {};
  // -s runs the persistence cases against the standard mittest S3 fixture.
  // Use GTEST_FILTER in the environment together with this option.
  while (EOF != (c = getopt(argc, argv, "t:l:s"))) {
    switch (c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 's':
      shared_storage_mode = true;
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
  if (shared_storage_mode) {
    // A local fixture can supply its base URI without editing the shared
    // authorization header. Always isolate cleanup under a fresh test prefix.
    const char *storage_override = getenv("OB_MDS_TEST_STORAGE_INFO");
    int storage_ret = oceanbase::common::OB_SUCCESS;
    if (nullptr != storage_override) {
      const char *query = strchr(storage_override, '?');
      if (nullptr == query || 0 != strncmp(storage_override, "s3://", 5)) {
        fprintf(stderr, "OB_MDS_TEST_STORAGE_INFO requires an S3 base URI with storage options\n");
        return 2;
      }
      storage_ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
          "%.*s/mds_reloc_%ld%s", static_cast<int>(query - storage_override), storage_override,
          oceanbase::common::ObTimeUtility::current_time_ns(), query);
    } else {
      if (0 != strncmp(oceanbase::unittest::S3_BUCKET, "s3://", 5)) {
        fprintf(stderr, "S3 fixture is not configured; set OB_MDS_TEST_STORAGE_INFO for -s\n");
        return 2;
      }
      storage_ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
          "%s/mds_reloc_%ld?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
          oceanbase::unittest::S3_BUCKET, oceanbase::common::ObTimeUtility::current_time_ns(),
          oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
          oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
    }
    if (oceanbase::common::OB_SUCCESS != storage_ret) {
      fprintf(stderr, "S3 fixture URI is too long\n");
      return 2;
    }
    oceanbase::shared_storage_info = storage_info;
    GCONF.ob_startup_mode.set_value("shared_storage");
    GCONF.datafile_size.set_value("100G");
    GCONF.memory_limit.set_value("20G");
    GCONF.system_memory.set_value("5G");
  }

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif

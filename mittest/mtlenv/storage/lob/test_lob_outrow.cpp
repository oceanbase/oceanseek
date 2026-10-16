/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_lob_common.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace blocksstable;
using namespace share::schema;
using namespace transaction;

namespace unittest
{

class TestLobOutrow : public ::testing::Test
{
public:
  TestLobOutrow()
    : tenant_id_(OB_SYS_TENANT_ID),
      ls_id_(TestLobCommon::TEST_LOB_LS_ID),
      tablet_id_(TestLobCommon::TEST_LOB_TABLE_ID),
      lob_meta_tablet_id_(TestLobCommon::TEST_LOB_META_TABLE_ID),
      lob_piece_tablet_id_(TestLobCommon::TEST_LOB_PIECE_TABLE_ID)
  {}
  virtual ~TestLobOutrow() = default;

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
      [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    // Create LS and tablets once — shared by all TEST_Fs.
    ASSERT_EQ(OB_SUCCESS, TestLobCommon::create_data_tablet(
        OB_SYS_TENANT_ID, share::ObLSID(TestLobCommon::TEST_LOB_LS_ID),
        common::ObTabletID(TestLobCommon::TEST_LOB_TABLE_ID),
        common::ObTabletID(TestLobCommon::TEST_LOB_META_TABLE_ID),
        common::ObTabletID(TestLobCommon::TEST_LOB_PIECE_TABLE_ID)));
  }
  static void TearDownTestCase()
  {
    // Explicitly remove the test LS before env destroy, otherwise tenant stop
    // may block for a long time in multi_tenant_.wait().
    ObLSService *ls_svr = MTL(ObLSService*);
    if (nullptr != ls_svr) {
      // Wait until all tx ctx on the LS are recycled before remove_ls. The
      // commit/rollback issued by the test cases finishes asynchronously: the
      // ctx is only released after its commit/rollback clog is applied. If we
      // call remove_ls() immediately, ObLS::offline() disables clog
      // replay/sync first, so any in-flight commit can never be applied and the
      // ctx stays forever -- offline() then spins in its OB_EAGAIN retry loop
      // ("transaction not empty, try again") and the process hangs.
      ObLSHandle ls_handle;
      if (OB_SUCCESS == ls_svr->get_ls(share::ObLSID(TestLobCommon::TEST_LOB_LS_ID),
                                       ls_handle, ObLSGetMod::STORAGE_MOD)
          && nullptr != ls_handle.get_ls()) {
        ObLS *ls = ls_handle.get_ls();
        const int64_t timeout_us = 30LL * 1000 * 1000; // 30s
        const int64_t start_ts = ObTimeUtility::current_time();
        int64_t tx_ctx_count = 0;
        while (OB_SUCCESS == ls->get_tx_ctx_count(tx_ctx_count)
               && tx_ctx_count > 0
               && ObTimeUtility::current_time() - start_ts < timeout_us) {
          ob_usleep(20 * 1000); // 20ms
        }
        STORAGE_LOG(INFO, "wait tx ctx recycled before remove_ls",
            K(tx_ctx_count), "cost_us", ObTimeUtility::current_time() - start_ts);
      }
      ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(share::ObLSID(TestLobCommon::TEST_LOB_LS_ID)));
    }
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  virtual void TearDown()
  {
    // No per-test cleanup needed — TearDownTestCase destroys everything.
  }

protected:
  // wrap raw data into an inrow disk locator (ObLobCommon.in_row_ = 1 + data),
  // which is what insert_before/insert consume as the source data.
  void make_inrow_src_locator(ObIAllocator &allocator, const ObString &data, ObLobLocatorV2 &src_loc);

protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
};

void TestLobOutrow::make_inrow_src_locator(ObIAllocator &allocator, const ObString &data, ObLobLocatorV2 &src_loc)
{
  int64_t loc_len = static_cast<int64_t>(sizeof(ObLobCommon)) + data.length();
  char *buf = static_cast<char *>(allocator.alloc(loc_len));
  ASSERT_NE(nullptr, buf);
  ObLobCommon *lob_common = new (buf) ObLobCommon();
  lob_common->in_row_ = 1;
  lob_common->is_mem_loc_ = 0;
  if (data.length() > 0) {
    MEMCPY(lob_common->buffer_, data.ptr(), data.length());
  }
  src_loc.assign_buffer(buf, static_cast<uint32_t>(loc_len));
}

TEST_F(TestLobOutrow, outrow_write_read_delete)
{
  ObArenaAllocator allocator;
  const int64_t DATA_LEN = 2 * 1024 * 1024;
  char *payload = static_cast<char *>(allocator.alloc(DATA_LEN));
  ASSERT_NE(nullptr, payload);
  for (int64_t i = 0; i < DATA_LEN; ++i) {
    payload[i] = static_cast<char>(i % 256);
  }
  ObString data(static_cast<int32_t>(DATA_LEN), static_cast<int32_t>(DATA_LEN), payload);

  // write
  ObLobManager *mgr = MTL(ObLobManager*);
  ASSERT_NE(nullptr, mgr);

  ObLobLocatorV2 src_loc;
  make_inrow_src_locator(allocator, data, src_loc);

  ObTransService *txs = MTL(ObTransService*);
  ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  ObTxSEQ savepoint;
  ASSERT_EQ(OB_SUCCESS, txs->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));

  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc, ObTxIsolationLevel::RC, expire_ts, read_snapshot));

  ObLobInsertContext ctx;
  ctx.allocator_ = &allocator;
  ctx.tx_desc_ = tx_desc;
  ctx.tx_id_ = tx_desc->get_tx_id();
  ASSERT_EQ(OB_SUCCESS, ctx.snapshot_.assign(read_snapshot));
  ctx.parent_seq_no_ = savepoint;
  ctx.ls_id_ = ls_id_;
  ctx.tablet_id_ = tablet_id_;
  ctx.lob_meta_tablet_id_ = lob_meta_tablet_id_;
  ctx.lob_piece_tablet_id_ = lob_piece_tablet_id_;
  ctx.coll_type_ = CS_TYPE_BINARY;
  ctx.timeout_ = expire_ts;
  ctx.col_id_ = OB_APP_MIN_COLUMN_ID + 3; // main table "d" longtext column
  ctx.src_loc_ = src_loc;

  bool outrow = false;
  ASSERT_EQ(OB_SUCCESS, mgr->insert_before(ctx, outrow));
  ASSERT_TRUE(outrow); // data length must exceed inrow threshold

  // skip redo flush to avoid paxos dependency in mock env
  ctx.try_flush_redo_ = false;
  ASSERT_EQ(OB_SUCCESS, mgr->insert(ctx));

  // 3. read back within the SAME transaction (uncommitted read works on memtable)
  {
    ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc, ObTxIsolationLevel::RC, expire_ts, read_snapshot));
    ObLobWriteContext qparam;
    qparam.allocator_ = &allocator;
    qparam.tx_desc_ = tx_desc;
    qparam.tx_id_ = tx_desc->get_tx_id();
    ASSERT_EQ(OB_SUCCESS, qparam.snapshot_.assign(read_snapshot));
    qparam.ls_id_ = ls_id_;
    qparam.tablet_id_ = tablet_id_;
    qparam.lob_meta_tablet_id_ = lob_meta_tablet_id_;
    qparam.lob_piece_tablet_id_ = lob_piece_tablet_id_;
    qparam.coll_type_ = CS_TYPE_BINARY;
    qparam.timeout_ = expire_ts;
    qparam.addr_ = GCONF.self_addr_; 
    qparam.need_read_latest_ = true;
    qparam.col_id_ = OB_APP_MIN_COLUMN_ID + 3;
    qparam.src_loc_ = ctx.dest_loc_;
    ASSERT_EQ(OB_SUCCESS, qparam.src_loc_wrapper_.init(qparam.src_loc_.ptr_, qparam.src_loc_.size_));
    qparam.offset_ = 0;
    qparam.len_ = DATA_LEN;

    char *buf = static_cast<char *>(allocator.alloc(DATA_LEN));
    ASSERT_NE(nullptr, buf);
    ObString out;
    out.assign_buffer(buf, static_cast<int32_t>(DATA_LEN));
    ASSERT_EQ(OB_SUCCESS, mgr->query(qparam, out));
    ASSERT_EQ(DATA_LEN, out.length());
    ASSERT_EQ(0, MEMCMP(payload, out.ptr(), DATA_LEN));
  }

  // 4. delete inside the same transaction
  {
    ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc, ObTxIsolationLevel::RC, expire_ts, read_snapshot));
    ObLobDeleteContext del_ctx;
    del_ctx.allocator_ = &allocator;
    del_ctx.tx_desc_ = tx_desc;
    del_ctx.tx_id_ = tx_desc->get_tx_id();
    ASSERT_EQ(OB_SUCCESS, del_ctx.snapshot_.assign(read_snapshot));
    del_ctx.parent_seq_no_ = savepoint;
    del_ctx.ls_id_ = ls_id_;
    del_ctx.tablet_id_ = tablet_id_;
    del_ctx.lob_meta_tablet_id_ = lob_meta_tablet_id_;
    del_ctx.lob_piece_tablet_id_ = lob_piece_tablet_id_;
    del_ctx.coll_type_ = CS_TYPE_BINARY;
    del_ctx.timeout_ = expire_ts;
    del_ctx.addr_ = GCONF.self_addr_;
    del_ctx.need_read_latest_ = true;
    del_ctx.col_id_ = OB_APP_MIN_COLUMN_ID + 3;
    del_ctx.src_loc_ = ctx.dest_loc_;
    ASSERT_EQ(OB_SUCCESS, del_ctx.src_loc_wrapper_.init(del_ctx.src_loc_.ptr_, del_ctx.src_loc_.size_));
    del_ctx.offset_ = 0;
    del_ctx.len_ = DATA_LEN;
    del_ctx.try_flush_redo_ = false;
    ASSERT_EQ(OB_SUCCESS, mgr->del(del_ctx));
  }


  // read after del
  {
    ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc, ObTxIsolationLevel::RC, expire_ts, read_snapshot));
    ObLobWriteContext qparam;
    qparam.allocator_ = &allocator;
    qparam.tx_desc_ = tx_desc;
    qparam.tx_id_ = tx_desc->get_tx_id();
    ASSERT_EQ(OB_SUCCESS, qparam.snapshot_.assign(read_snapshot));
    qparam.ls_id_ = ls_id_;
    qparam.tablet_id_ = tablet_id_;
    qparam.lob_meta_tablet_id_ = lob_meta_tablet_id_;
    qparam.lob_piece_tablet_id_ = lob_piece_tablet_id_;
    qparam.coll_type_ = CS_TYPE_BINARY;
    qparam.timeout_ = expire_ts;
    qparam.addr_ = GCONF.self_addr_; 
    qparam.need_read_latest_ = true;
    qparam.col_id_ = OB_APP_MIN_COLUMN_ID + 3;
    qparam.src_loc_ = ctx.dest_loc_;
    ASSERT_EQ(OB_SUCCESS, qparam.src_loc_wrapper_.init(qparam.src_loc_.ptr_, qparam.src_loc_.size_));
    qparam.offset_ = 0;
    qparam.len_ = DATA_LEN;

    char *buf = static_cast<char *>(allocator.alloc(DATA_LEN));
    ASSERT_NE(nullptr, buf);
    ObString out;
    out.assign_buffer(buf, static_cast<int32_t>(DATA_LEN));
    ASSERT_EQ(OB_SUCCESS, mgr->query(qparam, out));
    ASSERT_EQ(0, out.length());
  } 

  // 5. rollback to savepoint to undo writes and clear pending_write,
  //    then commit + release.
  ASSERT_EQ(OB_SUCCESS, txs->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));
  ASSERT_EQ(OB_SUCCESS, txs->commit_tx(*tx_desc, expire_ts));
  txs->release_tx(*tx_desc);
}

TEST_F(TestLobOutrow, inrow_not_promoted)
{
  ObArenaAllocator allocator;

  const int64_t data_len = 100;
  char *payload = static_cast<char *>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, payload);
  MEMSET(payload, 'a', data_len);
  ObString data(static_cast<int32_t>(data_len), static_cast<int32_t>(data_len), payload);

  ObLobLocatorV2 src_loc;
  make_inrow_src_locator(allocator, data, src_loc);

  ObLobManager *mgr = MTL(ObLobManager*);
  ObTransService *txs = MTL(ObTransService*);
  ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));
  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  ObTxSEQ savepoint;
  ASSERT_EQ(OB_SUCCESS, txs->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc, ObTxIsolationLevel::RC, expire_ts, read_snapshot));

  ObLobInsertContext ctx;
  ctx.allocator_ = &allocator;
  ctx.tx_desc_ = tx_desc;
  ctx.tx_id_ = tx_desc->get_tx_id();
  ASSERT_EQ(OB_SUCCESS, ctx.snapshot_.assign(read_snapshot));
  ctx.parent_seq_no_ = savepoint;
  ctx.ls_id_ = ls_id_;
  ctx.tablet_id_ = tablet_id_;
  ctx.lob_meta_tablet_id_ = lob_meta_tablet_id_;
  ctx.lob_piece_tablet_id_ = lob_piece_tablet_id_;
  ctx.coll_type_ = CS_TYPE_BINARY;
  ctx.timeout_ = expire_ts;
  ctx.col_id_ = OB_APP_MIN_COLUMN_ID + 3;
  ctx.src_loc_ = src_loc;

  bool outrow = true;
  ASSERT_EQ(OB_SUCCESS, mgr->insert_before(ctx, outrow));
  ASSERT_FALSE(outrow); // should NOT be promoted to outrow

  txs->release_tx(*tx_desc);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_outrow.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_lob_outrow.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

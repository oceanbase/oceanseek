/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define protected public
#define private public
#define UNITTEST

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/mock_ob_log_handler.h"
#include "logservice/ob_log_handler.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace storage;
using namespace blocksstable;
using namespace share;

namespace share
{
int ObTenantTxDataAllocator::init(const char *label, TxShareThrottleTool* throttle_tool)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, common::default_blk_alloc, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

void *ObTenantTxDataAllocator::alloc(const bool enable_throttle, const int64_t abs_expire_time)
{
  void *res = slice_allocator_.alloc();
  return res;
}
};

int storage::ObTenantMetaMemMgr::fetch_tenant_config()
{
  return OB_SUCCESS;
}

namespace unittest
{

class ObTxCtxTableRecoverHelperUT : public ObTxCtxTableRecoverHelper
{
public:
  virtual int recover_one_tx_ctx_(transaction::ObLSTxCtxMgr* ls_tx_ctx_mgr,
                                  ObTxCtxTableInfo& ctx_info)
  {
    TRANS_LOG(INFO, "recover_one_tx_ctx_ called", K(ctx_info));
    recover_tx_id_arr_.push_back(ctx_info.tx_id_);
    return 0;
  }

  bool tx_id_recovered(transaction::ObTransID tx_id)
  {
    bool exist = false;

    for (int64_t i = 0; i < recover_tx_id_arr_.size(); ++i) {
      if (recover_tx_id_arr_[i] == tx_id) {
        exist = true;
        break;
      }
    }
    return exist;
  }

private:
  std::vector<transaction::ObTransID> recover_tx_id_arr_;
};

class TestTxCtxTable : public ::testing::Test
{
public:
  TestTxCtxTable():
      ls_(),
      ls_tx_service_(&ls_),
      ls_data_checkpoint_(),
      log_handler_(),
      tablet_id_(LS_TX_DATA_TABLET),
      ls_id_(1),
      tenant_id_(1),
      freezer_(&ls_),
      t3m_(common::OB_SERVER_TENANT_ID),
      mt_mgr_(nullptr),
      ctx_mt_mgr_(nullptr),
      tenant_base_(tenant_id_)
  {
    ObLSTabletService *tablet_svr = ls_.get_tablet_svr();
    tablet_svr->init(&ls_);
  }

public:
  static ObLSTxCtxMgr ls_tx_ctx_mgr_;
  static ObLSTxCtxMgr ls_tx_ctx_mgr2_;
  ObLS ls_;
  ObLSWRSHandler ls_loop_worker_;
  ObLSTxService ls_tx_service_;
  ObLSTabletService ls_tablet_service_;
  checkpoint::ObDataCheckpoint ls_data_checkpoint_;
  MockObLogHandler log_handler_;
  static int64_t ref_count_;
protected:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObTxPalfParam palf_param((logservice::ObLogHandler *)(0x01),
                             (transaction::ObDupTableLSHandler *)(0x02));
    freezer_.init(&ls_);
    EXPECT_EQ(OB_SUCCESS, t3m_.init());
    EXPECT_EQ(OB_SUCCESS,
              ls_tx_ctx_mgr_.init(tenant_id_, /*tenant_id*/
                                  ls_id_,
                                  &ls_.tx_table_,
                                  ls_.get_lock_table(),
                                  (ObTsMgr *)(0x01),
                                  MTL(transaction::ObTransService*),
                                  &palf_param,
                                  nullptr));
    EXPECT_EQ(OB_SUCCESS,
              ls_tx_ctx_mgr2_.init(tenant_id_, /*tenant_id*/
                                  ls_id_,
                                  &ls_.tx_table_,
                                  ls_.get_lock_table(),
                                  (ObTsMgr *)(0x01),
                                  MTL(transaction::ObTransService*),
                                  &palf_param,
                                  nullptr));
    ref_count_ = 0;
    ctx_mt_mgr_ = new ObTxCtxMemtableMgr();
    EXPECT_EQ(OB_SUCCESS, ctx_mt_mgr_->init(tablet_id_,
                                            ls_id_,
                                            &freezer_,
                                            &t3m_));
    mt_mgr_ = ctx_mt_mgr_;

    // tenant_base_.set(t3m_);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  }
  virtual void TearDown() override
  {
    ctx_mt_mgr_->destroy();
    ls_tx_ctx_mgr_.reset();
    ls_tx_ctx_mgr2_.reset();
    delete mt_mgr_;
    mt_mgr_ = NULL;
    ctx_mt_mgr_ = NULL;

    bool all_table_cleaned = false; // no use
    ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));
    t3m_.destroy();

    ASSERT_EQ(0, ref_count_);

    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
public:
  ObTabletID tablet_id_;
  ObLSID ls_id_;
  int64_t tenant_id_;
  ObFreezer freezer_;
  ObTenantMetaMemMgr t3m_;
  ObIMemtableMgr *mt_mgr_;
  ObTxCtxMemtableMgr *ctx_mt_mgr_;
  ObTenantTxDataAllocator tx_data_allocator_;
  ObTenantTxDataOpAllocator tx_data_op_allocator_;

  ObTenantBase tenant_base_;
};

ObLSTxCtxMgr TestTxCtxTable::ls_tx_ctx_mgr_;
ObLSTxCtxMgr TestTxCtxTable::ls_tx_ctx_mgr2_;
int64_t TestTxCtxTable::ref_count_;

TEST_F(TestTxCtxTable, test_tx_ctx_memtable_mgr)
{
  EXPECT_EQ(0, TestTxCtxTable::ref_count_);
  CreateMemtableArg arg;
  EXPECT_EQ(OB_SUCCESS, mt_mgr_->create_memtable(arg));

  EXPECT_EQ(1, TestTxCtxTable::ref_count_);

  ObTableHandleV2 handle1;
  ObTxCtxMemtable *memtable1 = NULL;
  EXPECT_EQ(OB_SUCCESS, mt_mgr_->get_active_memtable(handle1));
  EXPECT_EQ(true, handle1.get_table()->is_tx_ctx_memtable());
  EXPECT_EQ(OB_SUCCESS, handle1.get_tx_ctx_memtable(memtable1));

  ObSEArray<ObTableHandleV2, 5> handles2;
  ObTableHandleV2 handle2;
  ObTxCtxMemtable *memtable2 = NULL;
  EXPECT_EQ(OB_SUCCESS, mt_mgr_->get_all_memtables(handles2));
  EXPECT_EQ(1, handles2.count());
  handle2 = handles2[0];
  EXPECT_EQ(true, handle2.get_table()->is_tx_ctx_memtable());
  EXPECT_EQ(OB_SUCCESS, handle2.get_tx_ctx_memtable(memtable2));

  EXPECT_EQ(memtable1, memtable2);
  handle2.reset();
  handles2.reset();
  EXPECT_EQ(1, TestTxCtxTable::ref_count_);

  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx memtable mgr test successfully", KPC(ctx_mt_mgr_), K(TestTxCtxTable::ref_count_));

  ObArenaAllocator allocator;
  ObTableReadInfo read_info;
  ObTableIterParam param;
  ObArray<ObColDesc> columns_;
  columns_.reset();
  ObColDesc col_desc;
  columns_.push_back(col_desc);
  param.reset();
  read_info.init(allocator, 16000, 1, lib::is_oracle_mode(), columns_, nullptr/*storage_cols_index*/);
  param.tablet_id_ = tablet_id_;
  param.read_info_ = &read_info;
  param.is_multi_version_minor_merge_ = true;

  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  context.reset();
  context.store_ctx_ = &store_ctx;
  context.allocator_ = &allocator;
  context.stmt_allocator_ = &allocator;
  context.merge_scn_.convert_from_ts(996);
  context.is_inited_ = true;

  ObDatumRange key_range;
  ObStoreRowIterator *row_iter = NULL;
  const ObDatumRow *row = NULL;
  ObDatumRow row_copy;

  EXPECT_EQ(OB_SUCCESS, memtable1->scan(param,
                                        context,
                                        key_range,
                                        row_iter));

  EXPECT_EQ(OB_ITER_END, row_iter->get_next_row(row));

  row_iter->reset();
  row_iter = NULL;
  row = NULL;

  ObTransID id1(1);
  ObLSID ls_id(1);
  static ObPartTransCtx ctx1;
  ctx1.tenant_id_ = 1;
  ctx1.trans_id_ = id1;
  ctx1.is_inited_ = true;
  ctx1.ls_id_ = ls_id;
  ctx1.exec_info_.max_applying_log_ts_.convert_from_ts(1);
  ctx1.replay_completeness_.set(true);
  ctx1.rec_log_ts_.convert_from_ts(996);
  ObTxData data1;
  // ctx1.tx_data_ = &data1;
  ctx1.ctx_tx_data_.test_init(data1, &ls_tx_ctx_mgr_);

  ObTransID id2(2);
  static ObPartTransCtx ctx2;
  ctx2.tenant_id_ = 1;
  ctx2.trans_id_ = id2;
  ctx2.is_inited_ = true;
  ctx2.ls_id_ = ls_id;
  ctx2.exec_info_.max_applying_log_ts_.convert_from_ts(2);
  ctx2.replay_completeness_.set(true);
  ctx2.rec_log_ts_.convert_from_ts(996);
  ObTxData data2;
  // ctx2.tx_data_ = &data2;
  ctx2.ctx_tx_data_.test_init(data2, &ls_tx_ctx_mgr_);
  ObTransCtx *ctx = NULL;
  EXPECT_EQ(OB_SUCCESS, ls_tx_ctx_mgr_.ls_tx_ctx_map_.insert_and_get(id1, &ctx1, &ctx));
  EXPECT_EQ(OB_SUCCESS, ls_tx_ctx_mgr_.ls_tx_ctx_map_.insert_and_get(id2, &ctx2, &ctx));

  int64_t idx = 0;
  // ObTransSSTableDurableCtxInfo ctx_info;
  ObTxCtxTableInfo ctx_info;
  ObSliceAlloc slice_allocator;
  ObTxDataTable tx_data_table;
  ObMemAttr attr;
  attr.tenant_id_ = MTL_ID();
  tx_data_allocator_.init("test", (TxShareThrottleTool*)0x1);
  tx_data_table.tx_data_allocator_ = &tx_data_allocator_;
  tx_data_op_allocator_.init();

  ObTxPalfParam palf_param((logservice::ObLogHandler *)(0x01),
                           (transaction::ObDupTableLSHandler *)(0x02));

  ObTxCtxTableRecoverHelperUT recover_helper;
  ObLSTxCtxMgr* ls_tx_ctx_mgr_recover = &unittest::TestTxCtxTable::ls_tx_ctx_mgr2_;

  EXPECT_EQ(OB_SUCCESS, memtable1->scan(param,
                                        context,
                                        key_range,
                                        row_iter));

  recover_helper.reset();
  for (int64_t ctx_idx = 0; ctx_idx < 2; ++ctx_idx) {
    ls_tx_ctx_mgr_recover->reset();
    EXPECT_EQ(OB_SUCCESS,
              ls_tx_ctx_mgr_recover->init(TestTxCtxTable::tenant_id_, /*tenant_id*/
                                          TestTxCtxTable::ls_id_,
                                          &ls_.tx_table_,
                                          &ls_.lock_table_,
                                          (ObTsMgr *)(0x01),
                                          MTL(transaction::ObTransService*),
                                          &palf_param,
                                          nullptr));
    ObTxCtxMemtableScanIterator* tx_ctx_memtable_iter = dynamic_cast<ObTxCtxMemtableScanIterator*>(row_iter);
    ASSERT_EQ(OB_SUCCESS, tx_ctx_memtable_iter->TEST_set_max_value_length(32));
    do {
      EXPECT_EQ(OB_SUCCESS, row_iter->get_next_row(row));
      TRANS_LOG(INFO, "row_info", K(*row));

      int64_t meta_col = TX_CTX_TABLE_META_COLUMN +
        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      int64_t value_col = meta_col + 1;
      row_copy.reset();
      row_copy.init(allocator, 8);
      row_copy.deep_copy(*row, allocator);
      row_copy.storage_datums_[TX_CTX_TABLE_META_COLUMN] = row_copy.storage_datums_[meta_col];
      row_copy.storage_datums_[TX_CTX_TABLE_VAL_COLUMN] = row_copy.storage_datums_[value_col];
      TRANS_LOG(INFO, "row_info projected", K(row_copy));
      ASSERT_EQ(OB_SUCCESS, recover_helper.recover(row_copy, tx_data_table, ls_tx_ctx_mgr_recover));
    } while (tx_ctx_memtable_iter->has_unmerged_buf_);
  }
  EXPECT_EQ(true, recover_helper.tx_id_recovered(id1));
  EXPECT_EQ(true, recover_helper.tx_id_recovered(id2));
  TRANS_LOG(INFO, "[TX_CTX_TABLE] successfully recover");
/*
  TRANS_LOG(INFO, "[TX_CTX_TABLE] get next row return", KPC(row));
  EXPECT_EQ(OB_SUCCESS, ObTxCtxTable::TEST_recover(*row, idx, ctx_info, slice_allocator));
  EXPECT_EQ(0, idx);
  EXPECT_EQ(id1, ctx_info.tx_id_);
  TRANS_LOG(INFO, "[TX_CTX_TABLE] successfully recover", K(ctx_info));

  EXPECT_EQ(OB_SUCCESS, row_iter->get_next_row(row));
  TRANS_LOG(INFO, "[TX_CTX_TABLE] get next row return", KPC(row));
  EXPECT_EQ(OB_SUCCESS, ObTxCtxTable::TEST_recover(*row, idx, ctx_info, slice_allocator));
  EXPECT_EQ(1, idx);
  EXPECT_EQ(id2, ctx_info.tx_id_);
  TRANS_LOG(INFO, "[TX_CTX_TABLE] successfully recover", K(ctx_info));

  EXPECT_EQ(OB_ITER_END, row_iter->get_next_row(row));
  EXPECT_EQ(1, TestTxCtxTable::ref_count_);
*/
  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx memtable test successfully", KPC(memtable1), K(TestTxCtxTable::ref_count_));
  ctx1.is_inited_ = false;
  ctx2.is_inited_ = false;
}

TEST_F(TestTxCtxTable, parallel_dli_replay_without_serial_submitted)
{
  static const int64_t LAST_APPLIED_ENTRY_NO = 1;
  static const int64_t GAP_ENTRY_NO = 3;
  static const int64_t NEXT_ENTRY_NO = LAST_APPLIED_ENTRY_NO + 2;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = LAST_APPLIED_ENTRY_NO;
  ctx.exec_info_.next_log_entry_no_ = NEXT_ENTRY_NO;
  ctx.replay_completeness_.set(true);

  ObDDLIncLogBasic batch_key;
  ObTransID tx_id(1001);
  ObTxSEQ seq_no(2001);
  ASSERT_EQ(OB_SUCCESS,
            batch_key.init(ObTabletID(100001), ObTabletID(100002), ObTabletID(), 
                           ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
                           tx_id, seq_no, 1000000, DATA_VERSION_4_5_0_0));
  SCN dli_start_scn;
  ASSERT_EQ(OB_SUCCESS, dli_start_scn.convert_from_ts(99));
  ASSERT_EQ(OB_SUCCESS,
            ctx.exec_info_.dli_batch_set_.before_submit_ddl_start(batch_key, dli_start_scn, true));

  SCN applied_scn;
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, applied_scn.convert_from_ts(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_from_ts(101));
  ctx.exec_info_.max_applied_log_ts_ = applied_scn;
  ctx.exec_info_.max_applying_log_ts_ = applied_scn;

  // Production clears SERIAL_SUBMITTED while dli_batch_set_ is active; replay only
  // enforces entry_no continuity when the log header carries the flag.
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(replay_scn, GAP_ENTRY_NO, false));
  EXPECT_EQ(LAST_APPLIED_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}

TEST_F(TestTxCtxTable, direct_load_replay_cursor_anchor)
{
  static const int64_t LAST_SERIAL_ENTRY_NO = 10;
  static const int64_t DIRECT_LOAD_BARRIER_ENTRY_NO = 20;
  static const int64_t NEXT_SERIAL_ENTRY_NO = 21;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = LAST_SERIAL_ENTRY_NO;
  ctx.exec_info_.next_log_entry_no_ = LAST_SERIAL_ENTRY_NO + 1;
  ctx.replay_completeness_.set(true);

  SCN direct_load_barrier_scn;
  ASSERT_EQ(OB_SUCCESS, direct_load_barrier_scn.convert_from_ts(100));
  EXPECT_EQ(OB_SUCCESS,
            ctx.push_replaying_log_ts(direct_load_barrier_scn,
                                      DIRECT_LOAD_BARRIER_ENTRY_NO,
                                      false));
  EXPECT_EQ(LAST_SERIAL_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);

  EXPECT_EQ(OB_SUCCESS,
            ctx.push_replayed_log_ts(direct_load_barrier_scn,
                                     palf::LSN(100),
                                     DIRECT_LOAD_BARRIER_ENTRY_NO));
  EXPECT_EQ(DIRECT_LOAD_BARRIER_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  EXPECT_EQ(NEXT_SERIAL_ENTRY_NO, ctx.exec_info_.next_log_entry_no_);

  SCN next_serial_scn;
  ASSERT_EQ(OB_SUCCESS, next_serial_scn.convert_from_ts(101));
  EXPECT_EQ(OB_SUCCESS,
            ctx.push_replaying_log_ts(next_serial_scn, NEXT_SERIAL_ENTRY_NO, true));
  ctx.ctx_tx_data_.test_tx_data_reset();
}

TEST_F(TestTxCtxTable, repeated_serial_replay_cursor)
{
  static const int64_t LAST_APPLIED_ENTRY_NO = 10;
  static const int64_t NEXT_ENTRY_NO = LAST_APPLIED_ENTRY_NO + 1;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = LAST_APPLIED_ENTRY_NO;
  ctx.exec_info_.next_log_entry_no_ = NEXT_ENTRY_NO;
  ctx.replay_completeness_.set(true);

  SCN old_scn;
  SCN applied_scn;
  SCN next_scn;
  ASSERT_EQ(OB_SUCCESS, old_scn.convert_from_ts(90));
  ASSERT_EQ(OB_SUCCESS, applied_scn.convert_from_ts(100));
  ASSERT_EQ(OB_SUCCESS, next_scn.convert_from_ts(101));
  ctx.exec_info_.max_applied_log_ts_ = applied_scn;
  ctx.exec_info_.max_applying_log_ts_ = applied_scn;

  // The recovered TX_CTX can be newer than the LS replay start point.
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(old_scn, 3, true));
  EXPECT_EQ(OB_SUCCESS,
            ctx.push_replaying_log_ts(applied_scn, LAST_APPLIED_ENTRY_NO, true));
  EXPECT_EQ(LAST_APPLIED_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  EXPECT_EQ(applied_scn, ctx.exec_info_.max_applied_log_ts_);

  // Retrying an entry that started but did not finish keeps the applied cursor.
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(next_scn, NEXT_ENTRY_NO, true));
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(next_scn, NEXT_ENTRY_NO, true));
  EXPECT_EQ(LAST_APPLIED_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  EXPECT_EQ(applied_scn, ctx.exec_info_.max_applied_log_ts_);
  EXPECT_EQ(next_scn, ctx.exec_info_.max_applying_log_ts_);

  ASSERT_EQ(OB_SUCCESS,
            ctx.push_replayed_log_ts(next_scn, palf::LSN(101), NEXT_ENTRY_NO));
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(next_scn, NEXT_ENTRY_NO, true));
  EXPECT_EQ(NEXT_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  EXPECT_EQ(next_scn, ctx.exec_info_.max_applied_log_ts_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// A serial log whose entry_no jumps over the cursor is logged as a defensive
// ERROR. Without ENABLE_DEBUG_LOG the call still returns OB_SUCCESS so production
// replay is not stalled by a false gap. With ENABLE_DEBUG_LOG the check raises
// OB_ERR_UNEXPECTED and ob_abort()s, so these tests are compiled out then.
#ifndef ENABLE_DEBUG_LOG
TEST_F(TestTxCtxTable, serial_replay_entry_no_gap)
{
  static const int64_t LAST_ENTRY_NO = 10;
  static const int64_t GAP_ENTRY_NO = 12;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = LAST_ENTRY_NO;
  ctx.exec_info_.next_log_entry_no_ = LAST_ENTRY_NO + 1;
  ctx.replay_completeness_.set(true);

  SCN applied_scn;
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, applied_scn.convert_from_ts(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_from_ts(101));
  ctx.exec_info_.max_applied_log_ts_ = applied_scn;
  ctx.exec_info_.max_applying_log_ts_ = applied_scn;

  // normal forward gap: log_ts (101) > applied/applying (100), entry (12) > cursor (10)
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(replay_scn, GAP_ENTRY_NO, true));
  EXPECT_EQ(LAST_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// log_ts is older than the applied cursor, but entry_no exceeds it. Production
// still returns OB_SUCCESS after the defensive ERROR; it must not be treated as
// an already-applied skip that silently drops the log, nor stall replay.
TEST_F(TestTxCtxTable, serial_replay_gap_with_applied_log_ts)
{
  static const int64_t LAST_ENTRY_NO = 10;
  static const int64_t GAP_ENTRY_NO = 12;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = LAST_ENTRY_NO;
  ctx.exec_info_.next_log_entry_no_ = LAST_ENTRY_NO + 1;
  ctx.replay_completeness_.set(true);

  SCN applied_scn;
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, applied_scn.convert_from_ts(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_from_ts(90));
  ctx.exec_info_.max_applied_log_ts_ = applied_scn;
  ctx.exec_info_.max_applying_log_ts_ = applied_scn;

  // old SCN + ahead entry: log_ts (90) <= applied (100) but entry (12) > cursor (10)
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(replay_scn, GAP_ENTRY_NO, true));
  EXPECT_EQ(LAST_ENTRY_NO, ctx.exec_info_.max_applied_entry_no_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}
#endif

// When completeness is unknown (first log after a fresh/recovered ctx), a gap
// only marks completeness incomplete, it must not hard fail.
TEST_F(TestTxCtxTable, replay_from_middle_keeps_soft_path)
{
  static const int64_t NEXT_ENTRY_NO = 11;
  static const int64_t GAP_ENTRY_NO = 13;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  // skip supplement_tx_op_if_exist_ which requires a real tx table
  ctx.ctx_tx_data_.recovered_from_tx_table_ = true;
  ctx.exec_info_.max_applied_entry_no_ = NEXT_ENTRY_NO - 1;
  ctx.exec_info_.next_log_entry_no_ = NEXT_ENTRY_NO;
  // replay_completeness_ stays UNKNOWN (default)

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(100));
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(scn, GAP_ENTRY_NO, true));
  EXPECT_TRUE(ctx.replay_completeness_.is_incomplete());
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// Old checkpoints have no applied entry cursor (INT64_MIN). With completeness
// already known, the hard check is skipped entirely (no cursor to compare against).
TEST_F(TestTxCtxTable, old_checkpoint_without_cursor_skips_hard_check)
{
  static const int64_t NEXT_ENTRY_NO = 11;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.exec_info_.max_applied_entry_no_ = INT64_MIN;
  ctx.exec_info_.next_log_entry_no_ = NEXT_ENTRY_NO;
  ctx.replay_completeness_.set(true);

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(100));
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(scn, NEXT_ENTRY_NO + 5, true));
  EXPECT_EQ(INT64_MIN, ctx.exec_info_.max_applied_entry_no_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// After middle-replay is diagnosed, retrying the same serial log while the
// applied cursor is still -1 (do_replay failed, push_replayed_log_ts skipped)
// must not hard-fail. Dima 2026082000118377050.
TEST_F(TestTxCtxTable, incomplete_middle_replay_empty_cursor_retry)
{
  static const int64_t MIDDLE_ENTRY_NO = 1;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  // skip supplement_tx_op_if_exist_ which requires a real tx table
  ctx.ctx_tx_data_.recovered_from_tx_table_ = true;
  ctx.exec_info_.max_applied_entry_no_ = -1;
  ctx.exec_info_.next_log_entry_no_ = 0;
  // replay_completeness_ stays UNKNOWN (default)

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(100));
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(scn, MIDDLE_ENTRY_NO, true));
  EXPECT_TRUE(ctx.replay_completeness_.is_incomplete());
  EXPECT_EQ(-1, ctx.exec_info_.max_applied_entry_no_);

  // Retry of the same unapplied SERIAL_SUBMITTED log.
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(scn, MIDDLE_ENTRY_NO, true));
  EXPECT_EQ(-1, ctx.exec_info_.max_applied_entry_no_);
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// With no cursor and completeness still UNKNOWN, the replay-from-middle decision
// falls back to next_log_entry_no_ as the expected entry.
TEST_F(TestTxCtxTable, old_checkpoint_replay_from_middle_uses_next_entry_no)
{
  static const int64_t NEXT_ENTRY_NO = 11;
  static const int64_t MIDDLE_ENTRY_NO = 13;

  ObPartTransCtx ctx;
  ObTxData tx_data;
  ctx.ctx_tx_data_.test_init(tx_data, &ls_tx_ctx_mgr_);
  ctx.ctx_tx_data_.recovered_from_tx_table_ = true;
  ctx.exec_info_.max_applied_entry_no_ = INT64_MIN;
  ctx.exec_info_.next_log_entry_no_ = NEXT_ENTRY_NO;
  // replay_completeness_ stays UNKNOWN (default)

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(100));
  // expected = next_log_entry_no_ (11) != entry (13) -> replay from middle
  EXPECT_EQ(OB_SUCCESS, ctx.push_replaying_log_ts(scn, MIDDLE_ENTRY_NO, true));
  EXPECT_TRUE(ctx.replay_completeness_.is_incomplete());
  ctx.ctx_tx_data_.test_tx_data_reset();
}

// Verify the compatible-version gate in ObTxCtxTableInfo::deserialize_: VERSION_1
// forces the absent-cursor sentinel, while VERSION_2 preserves the decoded cursor.
// Missing trailing-field behavior is covered separately by TestObTxLog.
TEST_F(TestTxCtxTable, tx_ctx_info_compatible_version_controls_cursor)
{
  ObTxDataTable tx_data_table;
  tx_data_allocator_.init("test", (TxShareThrottleTool*)0x1);
  tx_data_table.tx_data_allocator_ = &tx_data_allocator_;

  ObTxCtxTableInfo ctx_info;
  ctx_info.tx_id_ = ObTransID(1);
  ctx_info.ls_id_ = ObLSID(1);
  ctx_info.cluster_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, tx_data_table.alloc_tx_data(ctx_info.tx_data_guard_, false));
  ctx_info.exec_info_.max_applied_entry_no_ = 10;

  const int64_t size = ctx_info.get_serialize_size_();
  char *buf = static_cast<char *>(ob_malloc(size, "TxCtxCompatTest"));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctx_info.serialize_(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObTxCtxTableInfo replay_info;
  ASSERT_EQ(OB_SUCCESS, tx_data_table.alloc_tx_data(replay_info.tx_data_guard_, false));
  replay_info.set_compatible_version(ObTxCtxTableMeta::VERSION_1);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, replay_info.deserialize_(buf, size, pos, tx_data_table));
  EXPECT_EQ(INT64_MIN, replay_info.exec_info_.max_applied_entry_no_);

  ObTxCtxTableInfo replay_info2;
  ASSERT_EQ(OB_SUCCESS, tx_data_table.alloc_tx_data(replay_info2.tx_data_guard_, false));
  replay_info2.set_compatible_version(ObTxCtxTableMeta::VERSION_2);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, replay_info2.deserialize_(buf, size, pos, tx_data_table));
  EXPECT_EQ(10, replay_info2.exec_info_.max_applied_entry_no_);

  ob_free(buf);
}
} // namspace unittest

namespace storage
{
int ObTxCtxTable::acquire_ref_(const ObLSID& ls_id)
{
  int ret = OB_SUCCESS;

  ls_tx_ctx_mgr_ = &unittest::TestTxCtxTable::ls_tx_ctx_mgr_;
  unittest::TestTxCtxTable::ref_count_++;
  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx table acquire ref", K(ls_id), K(unittest::TestTxCtxTable::ref_count_), K(this));

  return ret;
}

int ObTxCtxTable::release_ref_()
{
  int ret = OB_SUCCESS;

  ls_tx_ctx_mgr_ = NULL;
  unittest::TestTxCtxTable::ref_count_--;
  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx table release ref", K(unittest::TestTxCtxTable::ref_count_), K(this));

  return ret;
}

void ObTxData::dec_ref()
{
  return;
}

} // namespace storage

namespace transaction
{
int ObLSTxCtxMgr::init(const int64_t tenant_id,
                       const ObLSID &ls_id,
                       ObTxTable *tx_table,
                       ObLockTable *lock_table,
                       ObTsMgr *ts_mgr,
                       ObTransService *txs,
                       ObITxLogParam *param,
                       ObITxLogAdapter *log_adapter)
{
  int ret = OB_SUCCESS;

  UNUSED(log_adapter);
  if (is_inited_) {
    TRANS_LOG(WARN, "ObLSTxCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else {
    //if (ObTransCtxType::PARTICIPANT == ctx_type) {
    //  bool is_dup_table = false;
    //  // FIXME. xiaoshi.xjl
    //  //} else if (OB_FAIL(txs->check_duplicated_partition(ls_id_, is_dup_table))) {
    //  //  TRANS_LOG(WARN, "check duplicated partition serving error", KR(ret), K(ls_id_));
    //  if (is_dup_table && OB_FAIL(init_dup_table_mgr())) {
    //    TRANS_LOG(WARN, "failed to init dup table", K(ret), K(ls_id));
    //  } else {
    //    // do nothing
    //  }
    //}
    //if (OB_SUCC(ret) && (ObTransCtxType::PARTICIPANT == ctx_type)) {
    //  if (OB_ISNULL(core_local_partition_audit_info_ = ObCoreLocalPartitionAuditInfoFactory::alloc())) {
    //    ret = OB_ALLOCATE_MEMORY_FAILED;
    //    TRANS_LOG(WARN, "alloc partition audit info error", KR(ret), K(ls_id));
    //  } else if (OB_FAIL(core_local_partition_audit_info_->init(OB_PARTITION_AUDIT_LOCAL_STORAGE_COUNT))) {
    //    TRANS_LOG(WARN, "ls_id audit info init error", KR(ret), K(ls_id));
    //  } else {
    //    // do nothing
    //  }
    //}
    if (OB_FAIL(ls_tx_ctx_map_.init(lib::ObMemAttr(tenant_id, "LSTxCtxMgr")))) {
      TRANS_LOG(WARN, "ls_tx_ctx_map_ init fail", KR(ret));
    } else if (OB_FAIL(tx_ls_state_mgr_.init(ls_id))) {
      TRANS_LOG(WARN, "init tx_ls_state_mgr_ failed", KR(ret));
    } else if (OB_FAIL(tx_ls_state_mgr_.switch_tx_ls_state(ObTxLSStateMgr::TxLSAction::START))) {
      TRANS_LOG(WARN, "start ls_tx_ctx_mgr failed",K(ret),K(tx_ls_state_mgr_));
    } else {
      is_inited_ = true;
      tenant_id_ = tenant_id;
      ls_id_ = ls_id;
      tx_table_ = tx_table;
      lock_table_ = lock_table,
      txs_ = txs;
      ts_mgr_ = ts_mgr;
      TRANS_LOG(INFO, "ObLSTxCtxMgr inited success", KP(this), K(ls_id));
    }
  }

  return ret;
}
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_ctx_table.log*");
  OB_LOGGER.set_file_name("test_tx_ctx_table.log");
  OB_LOGGER.set_log_level("DEBUG");
  STORAGE_LOG(INFO, "begin unittest: test tx ctx table");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

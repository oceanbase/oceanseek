/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_tx_version_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr_msg.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestObTxMisc : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObTxMisc, read_watermark_rejects_non_finite_snapshot)
{
  ObTxVersionMgr mgr;
  share::SCN snapshot;
  share::SCN bound;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, mgr.update_max_read_ts(snapshot));
  const share::SCN invalid_inputs[] = {
      share::SCN::invalid_scn(), share::SCN::max_scn(),
      share::SCN::scn_dec(share::SCN::max_scn())};
  for (const auto &invalid_snapshot : invalid_inputs) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_read_ts(invalid_snapshot));
    bound = mgr.get_max_read_ts();
    EXPECT_EQ(101, bound.get_val_for_tx());
  }
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(50));
  ASSERT_EQ(OB_SUCCESS, mgr.update_max_read_ts(snapshot));
  bound = mgr.get_max_read_ts();
  EXPECT_EQ(101, bound.get_val_for_tx());
}

TEST_F(TestObTxMisc, read_watermark_finite_boundary_and_concurrent_updates)
{
  ObTxVersionMgr mgr;
  share::SCN bound;
  bound = mgr.get_max_read_ts();
  EXPECT_EQ(2, bound.get_val_for_tx());
  const auto update = [&mgr](const int64_t start) {
    for (int64_t i = start; i < start + 1000; ++i) {
      share::SCN snapshot;
      ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(i));
      ASSERT_EQ(OB_SUCCESS, mgr.update_max_read_ts(snapshot));
      share::SCN read_bound;
      read_bound = mgr.get_max_read_ts();
      ASSERT_GT(read_bound, snapshot);
    }
  };
  std::thread first(update, 1000);
  std::thread second(update, 2000);
  first.join();
  second.join();
  bound = mgr.get_max_read_ts();
  EXPECT_EQ(3000, bound.get_val_for_tx());
  const share::SCN last_finite_snapshot =
      share::SCN::scn_dec(share::SCN::scn_dec(share::SCN::max_scn()));
  ASSERT_EQ(OB_SUCCESS, mgr.update_max_read_ts(last_finite_snapshot));
  bound = mgr.get_max_read_ts();
  EXPECT_TRUE(bound.is_valid());
  EXPECT_FALSE(bound.is_max());
  EXPECT_GT(bound, last_finite_snapshot);
}

TEST_F(TestObTxMisc, read_watermark_access_entry_fails_before_acquiring_context)
{
  // The write error path checks the current tenant role before returning.
  share::ObTenantBase tenant(1002);
  tenant.set_tenant_role(share::ObTenantRole::PRIMARY_TENANT);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
  std::unique_ptr<ObTransService> service(new ObTransService());
  std::unique_ptr<storage::ObLS> ls(new storage::ObLS());
  service->tenant_id_ = 1002;
  storage::ObStoreCtx store_ctx;
  store_ctx.ls_id_ = share::ObLSID(1001);
  store_ctx.tablet_id_ = common::ObTabletID(200011);
  store_ctx.ls_ = ls.get();
  store_ctx.timeout_ = INT64_MAX;
  const share::SCN invalid_inputs[] = {
      share::SCN::max_scn(), share::SCN::invalid_scn(),
      share::SCN::scn_dec(share::SCN::max_scn())};
  ObTxDesc tx;
  tx.access_mode_ = ObTxAccessMode::RW;
  const concurrent_control::ObWriteFlag write_flag;
  for (const auto &invalid_snapshot : invalid_inputs) {
    ObTxReadSnapshot snapshot;
    snapshot.init_weak_read(invalid_snapshot);
    EXPECT_EQ(OB_INVALID_ARGUMENT, service->get_read_store_ctx(snapshot, false, -1, store_ctx));
    snapshot.init_special_read(invalid_snapshot);
    EXPECT_EQ(OB_INVALID_ARGUMENT, service->get_write_store_ctx(tx, snapshot, write_flag, store_ctx));
    EXPECT_FALSE(store_ctx.is_valid());
    EXPECT_EQ(nullptr, store_ctx.mvcc_acc_ctx_.tx_ctx_);
    EXPECT_EQ(share::SCN::base_scn(), service->tx_version_mgr_.max_read_ts_.atomic_get());
  }
}

class SpecialSnapshotTestTsMgr : public ObTsMgr
{
public:
  int get_gts(const uint64_t, const MonotonicTs, ObTsCbTask *task,
              share::SCN &scn, MonotonicTs &receive_gts_ts) override
  {
    EXPECT_EQ(nullptr, task);
    ++call_count_;
    receive_gts_ts = MonotonicTs::current_time();
    return scn.convert_for_tx(99);
  }
  int call_count_ = 0;
};

TEST_F(TestObTxMisc, read_watermark_registers_after_special_snapshot_validation)
{
  std::unique_ptr<SpecialSnapshotTestTsMgr> ts_mgr(new SpecialSnapshotTestTsMgr());
  std::unique_ptr<ObTransService> service(new ObTransService());
  std::unique_ptr<storage::ObLS> ls(new storage::ObLS());
  service->tenant_id_ = 1002;
  service->ts_mgr_ = ts_mgr.get();
  storage::ObStoreCtx store_ctx;
  store_ctx.ls_id_ = share::ObLSID(1001);
  store_ctx.tablet_id_ = common::ObTabletID(200011);
  store_ctx.ls_ = ls.get();
  store_ctx.timeout_ = INT64_MAX;
  share::SCN future_snapshot;
  ASSERT_EQ(OB_SUCCESS, future_snapshot.convert_for_tx(100));
  const share::SCN rejected_snapshots[] = {
      future_snapshot, share::SCN::max_scn(), share::SCN::scn_dec(share::SCN::max_scn())};
  for (const auto &version : rejected_snapshots) {
    ObTxReadSnapshot snapshot;
    snapshot.init_special_read(version);
    EXPECT_EQ(OB_INVALID_QUERY_TIMESTAMP, service->get_read_store_ctx(snapshot, false, -1, store_ctx));
    EXPECT_EQ(share::SCN::base_scn(), service->tx_version_mgr_.max_read_ts_.atomic_get());
    EXPECT_EQ(nullptr, store_ctx.mvcc_acc_ctx_.tx_ctx_);
  }
  EXPECT_EQ(3, ts_mgr->call_count_);
  service->ts_mgr_ = nullptr;
}

TEST_F(TestObTxMisc, multiple_checksum_collapse_for_commit_log)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // only one checksum
  {
    uint64_t checksum0 = 12323221;
    ObArrayHelper<uint64_t> arr(1, &checksum0, 1);
    uint8_t signature0 = 0;
    ObArrayHelper<uint8_t> sig(1, &signature0, 1);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, but only one is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 12323221);
    EXPECT_EQ(0, sig.count());
    // valid is in middle
    checksum[0] = 1;
    checksum[13] = 34443;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_EQ(result, 34443);
    EXPECT_EQ(0, sig.count());
  }
  // multiple, multiple valid: 1, 13
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1;
    checksum[13] = 34443;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 12323221);
    EXPECT_NE(result, 34443);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
    EXPECT_EQ(34443 & 0xFF, sig.at(13));
  }
  // multiple, multiple valid, 18,21
  {
    uint64_t checksum[64];
    for (int i = 0; i < 64; i++) checksum[i] = 1;
    checksum[18] = 34443;
    checksum[21] = 34444;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_GT(result, 1);
    EXPECT_NE(result, 34443);
    EXPECT_NE(result, 34444);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(34443 & 0xFF, sig.at(18));
    EXPECT_EQ(34444 & 0xFF, sig.at(21));
  }
  // multiple, all is valid
  {
    uint64_t checksum[64] = {12323221};
    for (int i = 1; i < 64; i++) checksum[i] = 1 + i;
    ObArrayHelper<uint64_t> arr(64, checksum, 64);
    uint8_t signature[64];
    ObArrayHelper<uint8_t> sig(64, signature, 0);
    uint64_t result = 0;
    memtable::ObMemtableCtx::convert_checksum_for_commit_log(arr, result, sig);
    EXPECT_NE(result, 12323221);
    EXPECT_GT(result, 1);
    EXPECT_EQ(64, sig.count());
    EXPECT_EQ(12323221 & 0xFF, sig.at(0));
  }
}

TEST_F(TestObTxMisc, TxDesc_add_mofied_tables)
{
  ObTxDesc txdesc;
  {
    uint64_t tids[] = {1,2,3,4,5,6,7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 9; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {8, 9, 10, 1, 5, 7};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {11,12,13,14,15,16,17,18,19,20};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
  {
    uint64_t tids[] = {20, 19, 17, 14, 18, 20, 1, 7, 5, 4, 9};
    int cnt = sizeof(tids)/sizeof(uint64_t);
    EXPECT_EQ(OB_SUCCESS, txdesc.add_modified_tables(ObArrayHelper<uint64_t>(cnt, tids, cnt)));
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(txdesc.modified_tables_[i], i+1);
    }
  }
}

// Freeze the existing positional layout independently of ObTxExecResult's members.
struct TxExecResultWireLayout
{
  OB_UNIS_VERSION(1);
public:
  bool incomplete_ = false;
  ObTxPartList parts_;
  ObSArray<ObTransIDAndAddr> reserved_conflicts_;
  ObSArray<storage::ObRowConflictInfo> conflicts_;
  share::ObLSArray touched_ls_;
};
OB_SERIALIZE_MEMBER(TxExecResultWireLayout, incomplete_, parts_, reserved_conflicts_,
                    conflicts_, touched_ls_);

TEST_F(TestObTxMisc, execution_result_preserves_reserved_wire_slot)
{
  ObTxExecResult result;
  ObArray<storage::ObRowConflictInfo> conflicts;
  ASSERT_EQ(OB_SUCCESS, conflicts.push_back(storage::ObRowConflictInfo()));
  conflicts.at(0).conflict_hash_ = 123;
  conflicts.at(0).conflict_tx_id_ = ObTransID(456);
  result.merge_conflict_info_array(conflicts);
  ASSERT_EQ(OB_SUCCESS, result.add_touched_ls(share::ObLSID(789)));
  char buffer[16384];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, result.serialize(buffer, sizeof(buffer), pos));
  TxExecResultWireLayout wire;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, wire.deserialize(buffer, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  EXPECT_EQ(0, wire.reserved_conflicts_.count());
  ASSERT_EQ(1, wire.conflicts_.count());
  EXPECT_EQ(123, wire.conflicts_.at(0).conflict_hash_);
  EXPECT_EQ(ObTransID(456), wire.conflicts_.at(0).conflict_tx_id_);
  ASSERT_EQ(1, wire.touched_ls_.count());
  EXPECT_EQ(share::ObLSID(789), wire.touched_ls_.at(0));

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, wire.serialize(buffer, sizeof(buffer), pos));
  ObTxExecResult decoded;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  ObTxDesc tx;
  ASSERT_EQ(OB_SUCCESS, tx.add_exec_info(decoded));
  ASSERT_EQ(1, tx.get_conflict_info_array().count());
  EXPECT_EQ(123, tx.get_conflict_info_array().at(0).conflict_hash_);
}

TEST_F(TestObTxMisc, failed_sequence_does_not_update_participant)
{
  ObTxDesc tx;
  tx.seq_base_ = 0;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  EXPECT_EQ(OB_ERR_UNEXPECTED, tx.update_clean_part(share::ObLSID(1), 1, addr));
  EXPECT_EQ(OB_ERR_UNEXPECTED, tx.add_clean_part_if_absent(share::ObLSID(1), 1, addr, false));
  EXPECT_EQ(0, tx.parts_.count());
}

TEST_F(TestObTxMisc, sequence_generation_requires_current_base)
{
  ObTxDesc tx;
  ObTxSEQ seq;
  EXPECT_FALSE(tx.get_tx_seq().is_valid());
  EXPECT_FALSE(tx.get_min_tx_seq().is_valid());
  EXPECT_EQ(OB_ERR_UNEXPECTED, tx.get_and_inc_tx_seq(0, 1, seq));
  EXPECT_EQ(OB_ERR_UNEXPECTED, tx.inc_and_get_tx_seq(0, 1, seq));
  EXPECT_FALSE(tx.inc_and_get_tx_seq(0).is_valid());

  const int64_t now = ObSequence::get_max_seq_no();
  tx.seq_base_ = now - 100;
  EXPECT_EQ(ObTxSEQ::MIN_VAL(), tx.get_min_tx_seq());
  EXPECT_EQ(ObTxSEQ(100, 0), tx.get_tx_seq(now));
  EXPECT_FALSE(tx.get_tx_seq(tx.seq_base_).is_valid());
  EXPECT_FALSE(tx.get_tx_seq(tx.seq_base_ + ObTxSEQ::MAX_SEQ + 1).is_valid());
  EXPECT_EQ(OB_SUCCESS, tx.get_and_inc_tx_seq(7, 2, seq));
  EXPECT_EQ(ObTxSEQ(100, 7), seq);
  EXPECT_EQ(OB_SUCCESS, tx.inc_and_get_tx_seq(8, 3, seq));
  EXPECT_EQ(ObTxSEQ(105, 8), seq);
  EXPECT_EQ(OB_ERR_UNEXPECTED, tx.inc_and_get_tx_seq(-1, 1, seq));
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_misc.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

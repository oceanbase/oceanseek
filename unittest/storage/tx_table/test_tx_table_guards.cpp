/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <memory>

#define protected public
#define private public
#define UNITTEST
#include "storage/tx_table/ob_tx_table.h"
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/tx/ob_tx_data_op.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#include "storage/ls/ob_ls.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace storage;
using namespace blocksstable;
using namespace share;

static int turn = 0;
static ObTxData src_tx_data;
static ObTxData dst_tx_data;
static bool capture_read_args = false;
static bool observed_cache_bypass[2] = {false, false};
static int dest_read_ret = OB_SUCCESS;

namespace storage {
int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg,
                                  ObITxDataCheckFunctor &fn)
{
  TRANS_LOG(INFO, "turn is", K(turn));
  if (capture_read_args) {
    // Observe the actual arguments received by the tx table, without running
    // transaction state resolution. A missing dest exercises the src fallback.
    if (turn >= 2) {
      return OB_ERR_UNEXPECTED;
    }
    observed_cache_bypass[turn] = read_tx_data_arg.skip_cache_;
    return turn++ == 0 ? dest_read_ret : OB_SUCCESS;
  }
  if (turn == 0) {
    // use dst
    turn++;
    return fn(dst_tx_data, NULL);
  } else if (turn == 1) {
    // use src
    turn++;
    return fn(src_tx_data, NULL);
  } else {
    return OB_SUCCESS;
  }
}


}

namespace unittest
{

class TestTxTableGuards : public ::testing::Test
{
public:
  TestTxTableGuards() {}
  virtual void SetUp() override
  {
    turn = 0;
    capture_read_args = false;
    dest_read_ret = OB_SUCCESS;
    src_tx_data.reset();
    dst_tx_data.reset();
    TRANS_LOG(INFO, "setup success");
  }

  virtual void TearDown() override
  {
    turn = 0;
    capture_read_args = false;
    src_tx_data.reset();
    dst_tx_data.reset();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase()
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();

    TRANS_LOG(INFO, "SetUpTestCase");
  }
  static void TearDownTestCase()
  {
    turn = 0;
    src_tx_data.reset();
    dst_tx_data.reset();

    TRANS_LOG(INFO, "TearDownTestCase");
  }

};

TEST_F(TestTxTableGuards, follower_mds_missing_tx_state_is_temporarily_invisible)
{
  ObTxTable tx_table;
  ObStoreCtx store_ctx;
  auto &guards = store_ctx.mvcc_acc_ctx_.get_tx_table_guards();
  guards.tx_table_guard_.tx_table_ = &tx_table;
  ObTableAccessContext context;
  context.store_ctx_ = &store_ctx;
  context.tablet_id_ = common::LS_MDS_TABLET;
  context.query_flag_.set_is_select_follower();
  ObSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, sstable.key_.scn_range_.end_scn_.convert_for_tx(100));
  common::ObArenaAllocator allocator;
  ObMultiVersionMicroBlockRowScanner scanner(allocator);
  scanner.context_ = &context;
  scanner.sstable_ = &sstable;
  ObLockForReadArg arg(store_ctx.mvcc_acc_ctx_, ObTransID(1), ObTxSEQ(), false,
                      false, SCN::min_scn(), true, false, true);
  bool can_read = false;
  int64_t version = 0;
  int64_t prepare_version = 0;
  SCN end_scn;
  ObTxState tx_state;
  capture_read_args = true;
  dest_read_ret = OB_TRANS_CTX_NOT_EXIST;
  const int64_t snapshots[] = {80, 100, 150, 200, SCN::max_scn().get_val_for_tx()};
  for (const int64_t snapshot : snapshots) {
    context.trans_version_range_.snapshot_version_ = snapshot;
    ASSERT_EQ(OB_SUCCESS, store_ctx.mvcc_acc_ctx_.snapshot_.version_.convert_for_tx(snapshot));
    for (const bool read_uncommitted : {false, true}) {
      turn = 0;
      context.query_flag_.iter_uncommitted_row_ = read_uncommitted;
      arg.read_uncommitted_ = read_uncommitted;
      // Follower MDS recovery reads must not wait for a later transaction's
      // replay. Missing local state makes this version temporarily invisible
      // to committed reads, even at max_scn; the scan may reach an older row
      // or end empty. Reads including uncommitted rows expose it as undecided.
      ASSERT_EQ(OB_SUCCESS, scanner.lock_for_read(arg, can_read, version,
                                                prepare_version, end_scn, tx_state));
      EXPECT_EQ(read_uncommitted, can_read);
      EXPECT_EQ(SCN::min_scn().get_val_for_tx(), version);
      EXPECT_EQ(SCN::min_scn().get_val_for_tx(), prepare_version);
      EXPECT_TRUE(end_scn.is_max());
      EXPECT_EQ(ObTxState::INIT, tx_state);
    }
  }
  // Known transaction state still obeys the normal snapshot rules. This does
  // not require the missing-state read above to wait for replay to catch up.
  capture_read_args = false;
  context.query_flag_.iter_uncommitted_row_ = false;
  arg.read_uncommitted_ = false;
  dst_tx_data.state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, dst_tx_data.commit_version_.convert_for_tx(160));
  ASSERT_EQ(OB_SUCCESS, dst_tx_data.end_scn_.convert_for_tx(200));
  for (const int64_t snapshot : {int64_t(150), int64_t(160), int64_t(180), INT64_MAX}) {
    turn = 0;
    context.trans_version_range_.snapshot_version_ = snapshot;
    ASSERT_EQ(OB_SUCCESS, store_ctx.mvcc_acc_ctx_.snapshot_.version_.convert_for_tx(snapshot));
    ASSERT_EQ(OB_SUCCESS, scanner.lock_for_read(arg, can_read, version,
                                              prepare_version, end_scn, tx_state));
    EXPECT_EQ(snapshot >= 160, can_read);
    EXPECT_EQ(160, version);
    EXPECT_EQ(dst_tx_data.end_scn_, end_scn);
    EXPECT_EQ(ObTxState::COMMIT, tx_state);
  }
  capture_read_args = true;
  // An unreadable generation and unrelated errors must still propagate.
  for (const int error : {OB_REPLICA_NOT_READABLE, OB_ERR_UNEXPECTED}) {
    turn = 0;
    dest_read_ret = error;
    EXPECT_EQ(error, scanner.lock_for_read(arg, can_read, version,
                                          prepare_version, end_scn, tx_state));
  }
  dest_read_ret = OB_TRANS_CTX_NOT_EXIST;
  context.query_flag_.is_select_follower_ = false;
  turn = 0;
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, scanner.lock_for_read(arg, can_read, version,
                                                       prepare_version, end_scn, tx_state));
  context.query_flag_.set_is_select_follower();
  context.tablet_id_ = common::ObTabletID(200001);
  turn = 0;
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, scanner.lock_for_read(arg, can_read, version,
                                                       prepare_version, end_scn, tx_state));
}

TEST_F(TestTxTableGuards, latest_committed_read_skips_prepared_and_aborted_writes)
{
  memtable::ObMvccAccessCtx acc_ctx;
  acc_ctx.snapshot_.version_.set_max();
  ObTxData data;
  data.tx_id_ = ObTransID(1);
  data.state_ = ObTxData::RUNNING;
  ObTxCCCtx ctx(ObTxState::PREPARE, SCN::min_scn());
  ObCleanoutNothingOperation cleanout;
  ObReCheckNothingOperation recheck;
  ObLockForReadArg arg(acc_ctx, data.tx_id_, ObTxSEQ(1, 0), false,
                      false, SCN::min_scn(), true, false, true);
  for (const int64_t prepare_version : {int64_t(100), INT64_MAX}) {
    ASSERT_EQ(OB_SUCCESS, ctx.prepare_version_.convert_for_tx(prepare_version));
    ObLockForReadResult result;
    LockForReadFunctor fn(arg, result, ObLSID(1), cleanout, recheck);
    ASSERT_EQ(OB_SUCCESS, fn(data, &ctx));
    EXPECT_FALSE(result.can_read_);
    EXPECT_FALSE(result.is_state_decided());
  }
  data.state_ = ObTxData::ABORT;
  ObLockForReadResult result;
  LockForReadFunctor fn(arg, result, ObLSID(1), cleanout, recheck);
  ASSERT_EQ(OB_SUCCESS, fn(data));
  EXPECT_FALSE(result.can_read_);
  EXPECT_EQ(ObTxState::ABORT, result.tx_state_);
}

TEST_F(TestTxTableGuards, running_without_ctx_filters_known_undo)
{
  memtable::ObMvccAccessCtx acc_ctx;
  ObTxDataOp op(nullptr, nullptr);
  ObUndoStatusNode undo;
  ObTxData tx_data;
  // Keep a stack-owned reference so the guard never frees this test object.
  op.inc_ref();
  ASSERT_EQ(OB_SUCCESS, tx_data.op_guard_.init(&op));
  undo.size_ = 1;
  undo.undo_actions_[0] = ObUndoAction(ObTxSEQ(20, 0), ObTxSEQ(10, 0));
  op.get_undo_status_list().head_ = &undo;
  op.get_undo_status_list().undo_node_cnt_ = 1;
  tx_data.state_ = ObTxData::RUNNING;
  ObCleanoutNothingOperation cleanout;
  ObReCheckNothingOperation recheck;
  for (const bool read_uncommitted : {false, true}) {
    for (const int64_t sequence : {10, 15, 20, 21}) {
      ObLockForReadArg arg(acc_ctx, ObTransID(1), ObTxSEQ(sequence, 0), false,
                          read_uncommitted, SCN::min_scn(), true, false, true);
      ObLockForReadResult result;
      LockForReadFunctor fn(arg, result, ObLSID(1), cleanout, recheck);
      ASSERT_EQ(OB_SUCCESS, fn(tx_data));
      EXPECT_EQ(read_uncommitted && (sequence <= 10 || sequence > 20), result.can_read_);
      EXPECT_FALSE(result.is_state_decided());
      EXPECT_TRUE(result.trans_version_.is_min());
      EXPECT_TRUE(result.prepare_version_.is_min());
      EXPECT_TRUE(result.end_scn_.is_max());
    }
  }
}

class TestTxDataCacheGeneration : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
              lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1002));
  }
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, common::ObTimerService::get_instance().start());
    ASSERT_EQ(OB_SUCCESS, getter_.add_tenant(1002, 8L << 20, 64L << 20));
    ASSERT_EQ(OB_SUCCESS, common::ObKVGlobalCache::get_instance().init(
        &getter_, 1024, 1L << 30, lib::ACHUNK_SIZE));
    ASSERT_EQ(OB_SUCCESS, OB_TX_DATA_KV_CACHE.init("tx_data_kv_cache", 2));
  }
  void TearDown() override
  {
    OB_TX_DATA_KV_CACHE.destroy();
    common::ObKVGlobalCache::get_instance().destroy();
    common::ObTimerService::get_instance().stop();
    common::ObTimerService::get_instance().wait();
    common::ObTimerService::get_instance().destroy();
  }
  common::ObSimpleMemLimitGetter getter_;
};

TEST_F(TestTxDataCacheGeneration, key_copy_preserves_both_generations)
{
  const ObTxDataCacheKey key(1002, ObLSID(1), 100, 2, ObTransID(123));
  const ObTxDataCacheKey same(1002, ObLSID(1), 100, 2, ObTransID(123));
  const ObTxDataCacheKey recreated(1002, ObLSID(1), 101, 2, ObTransID(123));
  const ObTxDataCacheKey reonline(1002, ObLSID(1), 100, 3, ObTransID(123));
  // The persistent LS epoch counter starts at zero.
  EXPECT_TRUE(ObTxDataCacheKey(1002, ObLSID(1), 0, 1, ObTransID(123)).is_valid());
  ASSERT_TRUE(key.is_valid());
  EXPECT_TRUE(key == same);
  EXPECT_EQ(key.hash(), same.hash());
  EXPECT_FALSE(key == recreated);
  EXPECT_FALSE(key == reonline);
  alignas(ObTxDataCacheKey) char buffer[sizeof(ObTxDataCacheKey)];
  common::ObIKVCacheKey *copy = nullptr;
  ASSERT_EQ(OB_SUCCESS, key.deep_copy(buffer, sizeof(buffer), copy));
  ASSERT_NE(nullptr, copy);
  EXPECT_TRUE(key == *copy);
  EXPECT_EQ(key.hash(), copy->hash());
  EXPECT_FALSE(*copy == recreated);
  EXPECT_FALSE(*copy == reonline);
  copy->~ObIKVCacheKey();
}

TEST_F(TestTxDataCacheGeneration, late_put_cannot_pollute_new_generation)
{
  share::ObTenantBase tenant(1002);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
  std::unique_ptr<ObLS> ls(new ObLS());
  ObTxTable table;
  table.ls_ = ls.get();
  table.ls_id_ = ObLSID(1);
  table.epoch_ = 2;
  ls->ls_meta_.set_ls_epoch(100);
  ObTxData tx_data;
  tx_data.tx_id_ = ObTransID(123);
  tx_data.state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, tx_data.commit_version_.convert_for_tx(160));
  ASSERT_EQ(OB_SUCCESS, tx_data.end_scn_.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, table.put_tx_data_into_kv_cache_(tx_data, 2));
  ObTxDataMiniCache mini_cache;
  const auto read_cached = [&](const int64_t read_epoch, int64_t &state) {
    mini_cache.reset();
    ObReadTxDataArg arg(tx_data.tx_id_, read_epoch, mini_cache);
    SCN version;
    SCN end_scn;
    GetTxStateWithSCNFunctor fn(SCN::max_scn(), state, version, end_scn);
    return table.check_tx_data_in_kv_cache_(arg, fn);
  };
  int64_t state = -1;
  ASSERT_EQ(OB_SUCCESS, read_cached(2, state));
  EXPECT_EQ(ObTxData::COMMIT, state);

  table.epoch_ = 3;
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, read_cached(3, state));
  // The old request finishes after online advanced the table's current epoch.
  // It must not make the COMMIT visible through the new generation's cache.
  ASSERT_EQ(OB_SUCCESS, table.put_tx_data_into_kv_cache_(tx_data, 2));
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, read_cached(3, state));
  ASSERT_EQ(OB_SUCCESS, table.put_tx_data_into_kv_cache_(tx_data, 3));
  ASSERT_EQ(OB_SUCCESS, table.put_tx_data_into_kv_cache_(tx_data, 2));
  ASSERT_EQ(OB_SUCCESS, read_cached(3, state));
  EXPECT_EQ(ObTxData::COMMIT, state);
  EXPECT_EQ(2, OB_TX_DATA_KV_CACHE.count(1002));
  ASSERT_EQ(OB_SUCCESS, read_cached(2, state));
  EXPECT_EQ(ObTxData::COMMIT, state);

  // Hitting the old key does not make the old read valid after online changes.
  table.state_ = ObTxTable::ONLINE;
  int read_ret = OB_SUCCESS;
  table.check_state_and_epoch_(tx_data.tx_id_, 2, true, read_ret);
  EXPECT_EQ(OB_REPLICA_NOT_READABLE, read_ret);

  // A recreated LS may reuse the tx-table epoch but must miss the old cache.
  ls->ls_meta_.set_ls_epoch(101);
  table.epoch_ = 2;
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, read_cached(2, state));
}

TEST_F(TestTxTableGuards, known_commit_keeps_snapshot_visibility)
{
  ObTxTable tx_table;
  ObTxTableGuards guards;
  guards.tx_table_guard_.tx_table_ = &tx_table;
  memtable::ObMvccAccessCtx acc_ctx;
  dst_tx_data.state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, dst_tx_data.commit_version_.convert_for_tx(160));
  ASSERT_EQ(OB_SUCCESS, dst_tx_data.end_scn_.convert_for_tx(200));
  for (const int64_t snapshot : {150, 160, 180}) {
    turn = 0;
    ASSERT_EQ(OB_SUCCESS, acc_ctx.snapshot_.version_.convert_for_tx(snapshot));
    ObLockForReadArg arg(acc_ctx, ObTransID(1), ObTxSEQ(), false, false,
                        SCN::min_scn(), true, false, true);
    ObLockForReadResult result;
    ASSERT_EQ(OB_SUCCESS, guards.lock_for_read(arg, result));
    EXPECT_EQ(snapshot >= 160, result.can_read_);
    EXPECT_EQ(ObTxState::COMMIT, result.tx_state_);
    EXPECT_EQ(160, result.trans_version_.get_val_for_tx());
  }
}

TEST_F(TestTxTableGuards, lock_for_read_uses_cache_on_all_routes) {
  // Single guard, dual guards with undecided dest, and missing-dest fallback.
  // The lock path always reads through the tx data caches now that the key
  // carries the LS / tx-table generations.
  for (int route = 0; route < 3; ++route) {
    SCOPED_TRACE(::testing::Message() << "route=" << route);
    ObTxTable dst_tx_table;
    ObTxTable src_tx_table;
    ObTxTableGuards guards;
    guards.tx_table_guard_.tx_table_ = &dst_tx_table;
    if (route != 0) {
      guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
    }
    memtable::ObMvccAccessCtx acc_ctx;
    ObLockForReadArg arg(acc_ctx, ObTransID(1), ObTxSEQ(), false, true,
                        SCN::min_scn(), true, false, true);
    ObLockForReadResult result;
    turn = 0;
    capture_read_args = true;
    dest_read_ret = route == 2 ? OB_TRANS_CTX_NOT_EXIST : OB_SUCCESS;
    observed_cache_bypass[0] = observed_cache_bypass[1] = true;

    ASSERT_EQ(OB_SUCCESS, guards.lock_for_read(arg, result));
    ASSERT_EQ(route == 0 ? 1 : 2, turn);
    EXPECT_FALSE(observed_cache_bypass[0]);
    if (route != 0) {
      EXPECT_FALSE(observed_cache_bypass[1]);
    }
  }
}

TEST_F(TestTxTableGuards, check_on_single_dest_1) {
  ObTxTable dst_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;
  share::SCN end_scn;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version,
                                                     end_scn));

  EXPECT_EQ(dst_tx_data.commit_version_, trans_version);
  EXPECT_EQ(dst_tx_data.end_scn_, end_scn);
  EXPECT_EQ(dst_tx_data.state_, ObTxData::COMMIT);
}

TEST_F(TestTxTableGuards, check_on_single_dest_2) {
  ObTxTable dst_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(80);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(trans_version, SCN::max_scn());
  EXPECT_EQ(state, ObTxData::RUNNING);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_1) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(90);
  dst_tx_data.state_ = ObTxData::COMMIT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(dst_tx_data.commit_version_, trans_version);
  EXPECT_EQ(dst_tx_data.state_, ObTxData::COMMIT);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_2) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(110);
  dst_tx_data.state_ = ObTxData::ABORT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::COMMIT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(SCN::max_scn(), trans_version);
  EXPECT_EQ(state, ObTxData::RUNNING);
}

TEST_F(TestTxTableGuards, check_on_dest_and_src_3) {
  ObTxTable dst_tx_table;
  ObTxTable src_tx_table;
  ObTxTableGuards guards;
  share::SCN scn;
  int64_t state;
  share::SCN trans_version;

  scn.convert_from_ts(100);

  guards.tx_table_guard_.tx_table_ = &dst_tx_table;
  guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
  dst_tx_data.commit_version_.convert_from_ts(1);
  dst_tx_data.end_scn_.convert_from_ts(110);
  dst_tx_data.state_ = ObTxData::COMMIT;

  src_tx_data.commit_version_.convert_from_ts(2);
  src_tx_data.end_scn_.convert_from_ts(90);
  src_tx_data.state_ = ObTxData::ABORT;

  EXPECT_EQ(OB_SUCCESS, guards.get_tx_state_with_scn(ObTransID(1),
                                                     scn,
                                                     state,
                                                     trans_version));

  EXPECT_EQ(SCN::min_scn(), trans_version);
  EXPECT_EQ(src_tx_data.state_, state);
}


} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tx_table_guards.log*");
  OB_LOGGER.set_file_name("test_tx_table_guards.log");
  OB_LOGGER.set_log_level("DEBUG");
  STORAGE_LOG(INFO, "begin unittest: test tx table guards");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

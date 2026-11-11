/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#define private public
#define protected public
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/multi_data_source/mds_ctx.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace transaction;

namespace transaction
{
// Keep the production fast check, outer submit function and CtxLock. Replace
// only the log I/O boundary: these tests exercise admission to that boundary.
static std::atomic<int> freeze_submit_calls(0);
int ObPartTransCtx::submit_redo_log_for_freeze_(bool &submitted, const uint32_t)
{
  ++freeze_submit_calls;
  submitted = false;
  return OB_SUCCESS;
}
}

namespace unittest
{
// Supply one pending row to the real callback list. Serialization is irrelevant
// to these admission tests; blocking still comes from a real memtable flag.
class PendingRowCallback : public memtable::ObITransCallback
{
public:
  explicit PendingRowCallback(memtable::ObMemtable &memtable) : memtable_(memtable) {}
  int64_t get_data_size() override { return 1; }
  bool is_logging_blocked() const override { return memtable_.get_logging_blocked(); }
  bool is_need_free() const override { return false; }
private:
  memtable::ObMemtable &memtable_;
};

class TestFreezeMdsSubmit : public ::testing::Test
{
protected:
  TestFreezeMdsSubmit() : row_callback_(ordinary_memtable_) {}
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
    ctx_.reset(new ObPartTransCtx());
    freeze_submit_calls = 0;
  }
  void TearDown() override
  {
    EXPECT_EQ(OB_SUCCESS, ctx_->mt_ctx_.trans_mgr_.callback_list_.tx_abort());
    ctx_.reset();
    ObClockGenerator::destroy();
  }
  void block_ordinary_redo()
  {
    ordinary_memtable_.set_logging_blocked();
    ASSERT_EQ(OB_SUCCESS, ctx_->mt_ctx_.trans_mgr_.callback_list_.append_callback(
        &row_callback_, false /* for_replay */));
  }

  memtable::ObMemtable ordinary_memtable_;
  PendingRowCallback row_callback_;
  std::unique_ptr<ObPartTransCtx> ctx_;
};

TEST_F(TestFreezeMdsSubmit, empty_redo_list_keeps_existing_conservative_behavior)
{
  // is_logging_blocked only sets has_pending_log when it finds pending work;
  // it does not clear the caller's initial true value for an empty list.
  EXPECT_TRUE(ctx_->fast_check_need_submit_redo_for_freeze_());
  EXPECT_EQ(OB_SUCCESS, ctx_->submit_redo_log_for_freeze(4));
  EXPECT_EQ(1, freeze_submit_calls.load());
}

TEST_F(TestFreezeMdsSubmit, busy_locks_cannot_prove_no_pending_work)
{
  block_ordinary_redo();
  // Cover each try-lock failure independently, including failure of the first
  // (ordinary redo) probe. Use another thread so no recursive-lock behavior is
  // involved. A concurrent writer may not have inserted its MDS node yet.
  const int modes[] = {CtxLockGuard::ACCESS, CtxLockGuard::REDO_FLUSH_X, CtxLockGuard::CTX};
  for (const int mode : modes) {
    CtxLockGuard guard(ctx_->lock_, mode);
    bool need_submit = false;
    std::thread scanner([&]() { need_submit = ctx_->fast_check_need_submit_redo_for_freeze_(); });
    scanner.join();
    EXPECT_TRUE(need_submit) << "lock mode=" << mode;
  }
  EXPECT_FALSE(ctx_->fast_check_need_submit_redo_for_freeze_());
}

TEST_F(TestFreezeMdsSubmit, registration_lock_contention_enters_locked_submit)
{
  block_ordinary_redo();
  std::promise<void> scanning;
  std::promise<int> completed;
  auto scanning_future = scanning.get_future();
  auto completed_future = completed.get_future();
  std::thread scanner;
  {
    CtxLockGuard registration(ctx_->lock_);
    ctx_->mds_cache_.set_need_retry_submit_mds(true);
    scanner = std::thread([&]() {
      // The main thread keeps the registration lock until this probe finishes.
      const bool need_submit = ctx_->fast_check_need_submit_redo_for_freeze_();
      scanning.set_value();
      EXPECT_TRUE(need_submit);
      completed.set_value(ctx_->submit_redo_log_for_freeze(4));
    });
    scanning_future.wait();
    // Submission must wait for registration to release the context, rather
    // than report success without submitting and leave freeze to its timer.
    EXPECT_EQ(std::future_status::timeout,
              completed_future.wait_for(std::chrono::milliseconds(100)));
    EXPECT_EQ(0, freeze_submit_calls.load());
  }
  scanner.join();
  EXPECT_EQ(OB_SUCCESS, completed_future.get());
  EXPECT_EQ(1, freeze_submit_calls.load());
}

TEST_F(TestFreezeMdsSubmit, reloc_final_is_packed_alone_after_freeze_retry)
{
  memtable::ObMemtable memtable;
  storage::mds::MdsCtx contexts[5]; // marker, payload, final, ordinary reloc, unrelated standalone MDS
  contexts[2].set_needs_own_log(true);
  contexts[4].set_needs_own_log(true);
  auto &cache = ctx_->mds_cache_;
  for (int i = 0; i < ARRAYSIZEOF(contexts); ++i) {
    contexts[i].set_target_memtable(&memtable);
    ObTxBufferNode node;
    const auto type = i == 4 ? ObTxDataSourceType::UNBIND_TABLET_NEW_MDS
                             : ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH;
    ASSERT_EQ(OB_SUCCESS, node.init(type,
        ObString::make_string("reloc"), share::SCN::min_scn(), ObTxSEQ(i + 1, 0), &contexts[i]));
    ASSERT_EQ(OB_SUCCESS, cache.insert_mds_node(node));
  }
  ObTxMultiDataSourceLog log;
  ObTxMDSRange range;
  logservice::ObReplayBarrierType barrier;
  share::SCN base_scn;
  auto fill = [&]() {
    log.reset();
    return cache.fill_mds_log(ctx_.get(), log, range, barrier, base_scn);
  };
  memtable.set_logging_blocked();
  ASSERT_EQ(OB_BLOCK_FROZEN, fill());
  EXPECT_EQ(0, range.count());
  memtable.clear_logging_blocked();

  ASSERT_EQ(OB_SUCCESS, fill());
  ASSERT_EQ(2, log.count());
  EXPECT_EQ(ObTxSEQ(1, 0), log.get_data().at(0).get_seq_no());
  EXPECT_EQ(ObTxSEQ(2, 0), log.get_data().at(1).get_seq_no());
  EXPECT_EQ(logservice::ObReplayBarrierType::NO_NEED_BARRIER, barrier);
  ASSERT_EQ(OB_SUCCESS, range.range_submitted(cache));
  ASSERT_EQ(OB_SUCCESS, fill());
  ASSERT_EQ(1, log.count());
  EXPECT_EQ(ObTxSEQ(3, 0), log.get_data().at(0).get_seq_no());
  ObTxLogBlock block;
  block.get_header().init(1, DATA_VERSION_4_6_3_1, 1, ObTransID(1), ObAddr(ObAddr::IPV4, "127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, block.init_for_fill(ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, block.add_new_log(log));
  ASSERT_EQ(OB_SUCCESS, block.seal(1 /* replay_hint */, barrier));
  logservice::ObLogBaseHeader header;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.deserialize(block.get_buf(), block.get_size(), pos));
  EXPECT_TRUE(header.need_pre_replay_barrier());
  EXPECT_TRUE(header.need_post_replay_barrier());
  ASSERT_EQ(OB_SUCCESS, range.range_submitted(cache));
  ASSERT_EQ(OB_SUCCESS, fill());
  ASSERT_EQ(1, log.count());
  EXPECT_EQ(ObTxSEQ(4, 0), log.get_data().at(0).get_seq_no());
  EXPECT_EQ(logservice::ObReplayBarrierType::NO_NEED_BARRIER, barrier);
  ASSERT_EQ(OB_SUCCESS, range.range_submitted(cache));
  ASSERT_EQ(OB_SUCCESS, fill());
  ASSERT_EQ(1, log.count());
  EXPECT_EQ(ObTxSEQ(5, 0), log.get_data().at(0).get_seq_no());
  EXPECT_EQ(logservice::ObReplayBarrierType::PRE_BARRIER, barrier);
}

// Exercise the real packing path: ordinary refreshes must not inherit the
// floor pin's barrier, regardless of which side of the pin they are queued on.
TEST_F(TestFreezeMdsSubmit, floor_pin_barrier_does_not_affect_ordinary_refresh)
{
  storage::mds::MdsCtx contexts[5];
  // The registration helper marks only the floor pin as standalone.
  contexts[2].set_needs_own_log(true);
  auto &cache = ctx_->mds_cache_;
  for (int i = 0; i < 5; ++i) {
    ObTxBufferNode node;
    ASSERT_EQ(OB_SUCCESS, node.init(ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
        ObString::make_string("refresh"), share::SCN::min_scn(), ObTxSEQ(i + 1, 0), &contexts[i]));
    ASSERT_EQ(OB_SUCCESS, cache.insert_mds_node(node));
  }

  const int counts[] = {2, 1, 2};
  const int first_seq[] = {1, 3, 4};
  for (int i = 0; i < 3; ++i) {
    ObTxMultiDataSourceLog log;
    ObTxMDSRange range;
    logservice::ObReplayBarrierType barrier;
    share::SCN base_scn;
    ASSERT_EQ(OB_SUCCESS, cache.fill_mds_log(ctx_.get(), log, range, barrier, base_scn));
    ASSERT_EQ(counts[i], log.count());
    EXPECT_EQ(counts[i], range.count());
    // Retry before submission, retaining the original node and its context.
    log.reset();
    ASSERT_EQ(OB_SUCCESS, cache.fill_mds_log(ctx_.get(), log, range, barrier, base_scn));
    ASSERT_EQ(counts[i], log.count());
    EXPECT_EQ(ObTxSEQ(first_seq[i], 0), log.get_data().at(0).get_seq_no());
    EXPECT_EQ(i == 1 ? logservice::ObReplayBarrierType::PRE_BARRIER
                     : logservice::ObReplayBarrierType::NO_NEED_BARRIER, barrier);

    ObTxLogBlock block;
    block.get_header().init(1, DATA_VERSION_4_6_3_1, 1, ObTransID(1),
        ObAddr(ObAddr::IPV4, "127.0.0.1", 2882));
    ASSERT_EQ(OB_SUCCESS, block.init_for_fill(ObTxAdaptiveLogBuf::MIN_LOG_BUF_SIZE));
    ASSERT_EQ(OB_SUCCESS, block.add_new_log(log));
    ASSERT_EQ(OB_SUCCESS, block.seal(1, barrier));
    logservice::ObLogBaseHeader header;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, header.deserialize(block.get_buf(), block.get_size(), pos));
    EXPECT_EQ(i == 1, header.need_pre_replay_barrier());
    EXPECT_FALSE(header.need_post_replay_barrier());
    ASSERT_EQ(OB_SUCCESS, range.range_submitted(cache));
  }
}

TEST_F(TestFreezeMdsSubmit, blocked_active_does_not_hide_old_memtable_work)
{
  block_ordinary_redo();
  memtable::ObMemtable old_memtable;
  memtable::ObMemtable &active_memtable = ordinary_memtable_;
  storage::mds::MdsCtx old_mds;
  storage::mds::MdsCtx active_mds;
  old_mds.set_target_memtable(&old_memtable);
  active_mds.set_target_memtable(&active_memtable);
  ObTxBufferNode old_node;
  ObTxBufferNode active_node;
  const ObString data = ObString::make_string("mds");
  ASSERT_EQ(OB_SUCCESS, old_node.init(ObTxDataSourceType::CREATE_TABLET_NEW_MDS,
      data, share::SCN::min_scn(), ObTxSEQ(1, 0), &old_mds));
  ASSERT_EQ(OB_SUCCESS, active_node.init(ObTxDataSourceType::CREATE_TABLET_NEW_MDS,
      data, share::SCN::min_scn(), ObTxSEQ(2, 0), &active_mds));
  // Nodes borrow their data and BufferCtx from this test, not the MDS allocator.
  ASSERT_EQ(OB_SUCCESS, ctx_->mds_cache_.insert_mds_node(old_node));
  ASSERT_EQ(OB_SUCCESS, ctx_->mds_cache_.insert_mds_node(active_node));
  EXPECT_TRUE(ctx_->fast_check_need_submit_redo_for_freeze_());
  EXPECT_EQ(OB_SUCCESS, ctx_->submit_redo_log_for_freeze(4));
  EXPECT_EQ(1, freeze_submit_calls.load());

  // Exercise the real packing guard as well: the old node can be packed, but
  // the blocked active node must not share its log/SCN.
  ObTxMultiDataSourceLog mds_log;
  ObTxMDSRange mds_range;
  logservice::ObReplayBarrierType barrier;
  share::SCN base_scn;
  ASSERT_EQ(OB_SUCCESS, ctx_->mds_cache_.fill_mds_log(
      ctx_.get(), mds_log, mds_range, barrier, base_scn));
  ASSERT_EQ(1, mds_log.count());
  EXPECT_EQ(ObTxSEQ(1, 0), mds_log.get_data().at(0).get_seq_no());

  // After the old node is submitted, only the blocked active node remains.
  ctx_->mds_cache_.mds_list_.begin()->set_submitted();
  mds_log.reset();
  EXPECT_EQ(OB_BLOCK_FROZEN, ctx_->mds_cache_.fill_mds_log(
      ctx_.get(), mds_log, mds_range, barrier, base_scn));
  EXPECT_EQ(0, mds_log.count());
  EXPECT_FALSE(ctx_->fast_check_need_submit_redo_for_freeze_());
  EXPECT_EQ(OB_SUCCESS, ctx_->submit_redo_log_for_freeze(4));
  EXPECT_EQ(1, freeze_submit_calls.load());
  active_memtable.clear_logging_blocked();
  EXPECT_TRUE(ctx_->fast_check_need_submit_redo_for_freeze_());
  EXPECT_EQ(OB_SUCCESS, ctx_->submit_redo_log_for_freeze(4));
  EXPECT_EQ(2, freeze_submit_calls.load());
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_freeze_mds_submit.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

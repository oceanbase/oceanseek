/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define protected public
#define private public
#define UNITTEST
#include "storage/tx_table/ob_tx_table.h"
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"

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
static bool observed_skip_cache[2] = {false, false};
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
    observed_skip_cache[turn] = read_tx_data_arg.skip_cache_;
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

TEST_F(TestTxTableGuards, lock_for_read_preserves_skip_cache) {
  // Single guard, dual guards with undecided dest, and missing-dest fallback.
  for (int route = 0; route < 3; ++route) {
    for (int skip_cache = 0; skip_cache < 2; ++skip_cache) {
      SCOPED_TRACE(::testing::Message() << "route=" << route << ", skip_cache=" << skip_cache);
      ObTxTable dst_tx_table;
      ObTxTable src_tx_table;
      ObTxTableGuards guards;
      guards.tx_table_guard_.tx_table_ = &dst_tx_table;
      if (route != 0) {
        guards.src_tx_table_guard_.tx_table_ = &src_tx_table;
      }
      memtable::ObMvccAccessCtx acc_ctx;
      ObLockForReadArg arg(acc_ctx, ObTransID(1), ObTxSEQ(), false, true,
                          SCN::min_scn(), true, false, skip_cache != 0, true);
      ObLockForReadResult result;
      turn = 0;
      capture_read_args = true;
      dest_read_ret = route == 2 ? OB_TRANS_CTX_NOT_EXIST : OB_SUCCESS;
      observed_skip_cache[0] = observed_skip_cache[1] = (skip_cache == 0);

      ASSERT_EQ(OB_SUCCESS, guards.lock_for_read(arg, result));
      ASSERT_EQ(route == 0 ? 1 : 2, turn);
      EXPECT_EQ(skip_cache != 0, observed_skip_cache[0]);
      if (route != 0) {
        EXPECT_EQ(skip_cache != 0, observed_skip_cache[1]);
      }
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

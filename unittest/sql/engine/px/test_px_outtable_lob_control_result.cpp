/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/exchange/ob_px_fifo_coord_op.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace storage;

class TestLobPxCoordOp : public ObPxFifoCoordOp
{
public:
  using ObPxFifoCoordOp::ObPxFifoCoordOp;
  ObPxCoordInfo &info() { return coord_info_; }
};

class TestPxOuttableLobControlResult : public ::testing::Test
{
public:
  TestPxOuttableLobControlResult()
    : tenant_base_(OB_SYS_TENANT_ID),
      allocator_("PxLobTest"),
      session_(),
      plan_(),
      ctx_(allocator_),
      spec_(allocator_, PHY_PX_FIFO_COORD),
      coord_(nullptr)
  {}

  void SetUp() override
  {
    ObTenantEnv::set_tenant(&tenant_base_);
    ctx_.set_my_session(&session_);
    ASSERT_EQ(OB_SUCCESS, ctx_.create_physical_plan_ctx());
    ctx_.get_physical_plan_ctx()->set_phy_plan(&plan_);
    coord_ = new TestLobPxCoordOp(ctx_, spec_, nullptr);
  }

  void TearDown() override
  {
    if (nullptr != coord_) {
      coord_->destroy();
      delete coord_;
    }
    ObTenantEnv::set_tenant(nullptr);
  }

  static ObOuttableLobControlKey make_key(const int64_t sequence)
  {
    ObOuttableLobControlKey key;
    key.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, sequence, 0, 0, 0);
    key.hash_ = sequence;
    return key;
  }

  static void expect_key_eq(
      const ObOuttableLobControlKey &expected,
      const ObOuttableLobControlKey &actual)
  {
    EXPECT_EQ(expected.macro_id_, actual.macro_id_);
    EXPECT_EQ(expected.hash_, actual.hash_);
  }

  void prepare_sqc_messages(const int64_t sqc_count)
  {
    ASSERT_GT(sqc_count, 0);
    void *buf = allocator_.alloc(sizeof(ObDfo));
    ASSERT_NE(nullptr, buf);
    ObDfo *dfo = new(buf) ObDfo(allocator_);
    dfo->set_dfo_id(0);
    ASSERT_EQ(OB_SUCCESS, coord_->info().dfo_mgr_.add_dfo_edge(dfo));
    ObPxSqcMeta sqc;
    sqc.set_dfo_id(0);
    sqc.set_task_count(1);
    sqc.set_thread_inited(true);
    sqc.set_need_report(true);
    for (int64_t i = 0; i < sqc_count; ++i) {
      sqc.set_sqc_id(i);
      ASSERT_EQ(OB_SUCCESS, dfo->add_sqc(sqc));
    }
  }

  ObTenantBase tenant_base_;
  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObPhysicalPlan plan_;
  ObExecContext ctx_;
  ObPxFifoCoordSpec spec_;
  TestLobPxCoordOp *coord_;
};

TEST_F(TestPxOuttableLobControlResult, finish_message_round_trip_and_reset)
{
  ObPxFinishSqcResultMsg source;
  source.dfo_id_ = 1;
  source.sqc_id_ = 2;
  source.sqc_affected_rows_ = 3;
  ASSERT_EQ(OB_SUCCESS, source.outtable_lob_control_keys_.push_back(make_key(2026102901)));
  ASSERT_EQ(OB_SUCCESS, source.outtable_lob_control_keys_.push_back(make_key(2026102902)));
  const int64_t size = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObPxFinishSqcResultMsg target;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  EXPECT_EQ(source.dfo_id_, target.dfo_id_);
  EXPECT_EQ(source.sqc_id_, target.sqc_id_);
  EXPECT_EQ(source.sqc_affected_rows_, target.sqc_affected_rows_);
  ASSERT_EQ(2, target.outtable_lob_control_keys_.count());
  expect_key_eq(make_key(2026102901), target.outtable_lob_control_keys_.at(0));
  expect_key_eq(make_key(2026102902), target.outtable_lob_control_keys_.at(1));

  // DTL resets its reusable packet before decoding the next SQC result.
  target.reset();
  EXPECT_TRUE(target.outtable_lob_control_keys_.empty());
  source.reset();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  EXPECT_TRUE(target.outtable_lob_control_keys_.empty());
}

TEST_F(TestPxOuttableLobControlResult, task_keys_have_independent_lifetime)
{
  ObPxTask first;
  ObPxTask second;
  EXPECT_EQ(nullptr, first.outtable_lob_control_keys_);
  EXPECT_EQ(nullptr, second.outtable_lob_control_keys_);
  first.set_use_local_thread(true);
  second.set_use_local_thread(false);
  {
    ObArray<ObOuttableLobControlKey> keys;
    ASSERT_EQ(OB_SUCCESS, first.append_outtable_lob_control_keys(keys));
    ASSERT_EQ(OB_SUCCESS, second.append_outtable_lob_control_keys(keys));
    EXPECT_EQ(nullptr, first.outtable_lob_control_keys_);
    EXPECT_EQ(nullptr, second.outtable_lob_control_keys_);
    ASSERT_EQ(OB_SUCCESS, keys.push_back(make_key(2026102903)));
    ASSERT_EQ(OB_SUCCESS, first.append_outtable_lob_control_keys(keys));
    keys.at(0) = make_key(2026102904);
    ASSERT_EQ(OB_SUCCESS, second.append_outtable_lob_control_keys(keys));
  }
  ASSERT_NE(nullptr, first.outtable_lob_control_keys_);
  ASSERT_NE(nullptr, second.outtable_lob_control_keys_);
  EXPECT_NE(first.outtable_lob_control_keys_, second.outtable_lob_control_keys_);
  ASSERT_EQ(1, first.outtable_lob_control_keys_->count());
  ASSERT_EQ(1, second.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102903), first.outtable_lob_control_keys_->at(0));
  expect_key_eq(make_key(2026102904), second.outtable_lob_control_keys_->at(0));

  // Request copies must not retain runtime results from a previous worker execution.
  ObPxTask copied(first);
  EXPECT_EQ(nullptr, copied.outtable_lob_control_keys_);
  ObSEArray<ObOuttableLobControlKey, 1> copied_keys;
  ASSERT_EQ(OB_SUCCESS, copied_keys.push_back(make_key(2026102909)));
  ASSERT_EQ(OB_SUCCESS, copied.append_outtable_lob_control_keys(copied_keys));
  ASSERT_NE(nullptr, copied.outtable_lob_control_keys_);
  EXPECT_NE(first.outtable_lob_control_keys_, copied.outtable_lob_control_keys_);
  ASSERT_EQ(1, copied.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102909), copied.outtable_lob_control_keys_->at(0));
  second = first;
  EXPECT_NE(first.outtable_lob_control_keys_, second.outtable_lob_control_keys_);
  EXPECT_TRUE(nullptr == second.outtable_lob_control_keys_
      || second.outtable_lob_control_keys_->empty());
  ASSERT_EQ(1, first.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102903), first.outtable_lob_control_keys_->at(0));
}

TEST_F(TestPxOuttableLobControlResult, empty_coord_does_not_require_a_transaction)
{
  ObPxCoordInfo &info = coord_->info();
  EXPECT_EQ(nullptr, info.outtable_lob_control_keys_);
  ObSEArray<ObOuttableLobControlKey, 1> keys;
  ASSERT_EQ(OB_SUCCESS, info.append_outtable_lob_control_keys(keys));
  EXPECT_EQ(OB_SUCCESS, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_SUCCESS, info.check_outtable_lob_control_complete());
  EXPECT_EQ(nullptr, info.outtable_lob_control_keys_);
  EXPECT_EQ(nullptr, session_.get_tx_desc());
}

TEST_F(TestPxOuttableLobControlResult, coord_owns_keys_until_reset)
{
  ObPxCoordInfo &info = coord_->info();
  {
    ObPxFinishSqcResultMsg first;
    ObPxFinishSqcResultMsg second;
    ASSERT_EQ(OB_SUCCESS, first.outtable_lob_control_keys_.push_back(make_key(2026102905)));
    ASSERT_EQ(OB_SUCCESS, second.outtable_lob_control_keys_.push_back(make_key(2026102906)));
    ASSERT_EQ(OB_SUCCESS, info.append_outtable_lob_control_keys(first.outtable_lob_control_keys_));
    ASSERT_EQ(OB_SUCCESS, info.append_outtable_lob_control_keys(second.outtable_lob_control_keys_));
  }
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  ASSERT_EQ(2, info.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102905), info.outtable_lob_control_keys_->at(0));
  expect_key_eq(make_key(2026102906), info.outtable_lob_control_keys_->at(1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());
  info.reset_for_rescan();
  EXPECT_TRUE(nullptr == info.outtable_lob_control_keys_
      || info.outtable_lob_control_keys_->empty());
  EXPECT_EQ(OB_SUCCESS, info.check_outtable_lob_control_complete());
}

TEST_F(TestPxOuttableLobControlResult, unfinished_workers_cannot_complete)
{
  ObPxCoordInfo &info = coord_->info();
  ObSEArray<ObOuttableLobControlKey, 1> keys;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(make_key(2026102907)));
  ASSERT_EQ(OB_SUCCESS, info.append_outtable_lob_control_keys(keys));
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  ASSERT_FALSE(info.all_threads_finish_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(1, info.outtable_lob_control_keys_->count());
  info.all_threads_finish_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());
  EXPECT_EQ(nullptr, session_.get_tx_desc());
}

TEST_F(TestPxOuttableLobControlResult, worker_failure_cannot_become_success)
{
  ObPxCoordInfo &info = coord_->info();
  ObSEArray<ObOuttableLobControlKey, 1> keys;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(make_key(2026102908)));
  ASSERT_EQ(OB_SUCCESS, info.append_outtable_lob_control_keys(keys));
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  info.all_threads_finish_ = true;
  info.first_error_code_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(1, info.outtable_lob_control_keys_->count());
  info.first_error_code_ = OB_SUCCESS;
  EXPECT_EQ(OB_TIMEOUT, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_TIMEOUT, info.check_outtable_lob_control_complete());
  EXPECT_EQ(nullptr, session_.get_tx_desc());
}

TEST_F(TestPxOuttableLobControlResult, invalid_key_keeps_a_sticky_error)
{
  ObPxCoordInfo &info = coord_->info();
  ObSEArray<ObOuttableLobControlKey, 1> keys;
  ASSERT_EQ(OB_SUCCESS, keys.push_back(ObOuttableLobControlKey()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info.append_outtable_lob_control_keys(keys));
  EXPECT_TRUE(nullptr == info.outtable_lob_control_keys_
      || info.outtable_lob_control_keys_->empty());
  EXPECT_EQ(OB_INVALID_ARGUMENT, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info.check_outtable_lob_control_complete());
  info.reset_for_rescan();
  EXPECT_EQ(OB_SUCCESS, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_SUCCESS, info.check_outtable_lob_control_complete());
}

TEST_F(TestPxOuttableLobControlResult, nonfinal_and_duplicate_sqc_messages_only_collect_once)
{
  ASSERT_NO_FATAL_FAILURE(prepare_sqc_messages(2));
  ObPxCoordInfo &info = coord_->info();
  ObSerialDfoScheduler scheduler(info, *coord_, coord_->get_listenner());
  ObPxMsgProc proc(info, coord_->get_listenner(), *coord_);
  proc.set_scheduler(&scheduler);
  ObPxFinishSqcResultMsg msg;
  msg.dfo_id_ = 0;
  msg.sqc_id_ = 0;
  msg.sqc_affected_rows_ = 3;
  ASSERT_EQ(OB_SUCCESS, msg.outtable_lob_control_keys_.push_back(make_key(2026102910)));

  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_FALSE(info.all_threads_finish_);
  EXPECT_EQ(OB_SUCCESS, info.outtable_lob_control_ret_);
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  ASSERT_EQ(1, info.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102910), info.outtable_lob_control_keys_->at(0));
  EXPECT_EQ(3, ctx_.get_physical_plan_ctx()->get_affected_rows());

  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_FALSE(info.all_threads_finish_);
  EXPECT_EQ(OB_SUCCESS, info.outtable_lob_control_ret_);
  EXPECT_EQ(1, info.outtable_lob_control_keys_->count());
  EXPECT_EQ(3, ctx_.get_physical_plan_ctx()->get_affected_rows());
}

TEST_F(TestPxOuttableLobControlResult, last_sqc_propagates_completion_failure)
{
  ASSERT_NO_FATAL_FAILURE(prepare_sqc_messages(2));
  ObPxCoordInfo &info = coord_->info();
  ObSerialDfoScheduler scheduler(info, *coord_, coord_->get_listenner());
  ObPxMsgProc proc(info, coord_->get_listenner(), *coord_);
  proc.set_scheduler(&scheduler);
  ObPxFinishSqcResultMsg first;
  ObPxFinishSqcResultMsg last;
  first.dfo_id_ = 0;
  first.sqc_id_ = 0;
  last.dfo_id_ = 0;
  last.sqc_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, first.outtable_lob_control_keys_.push_back(make_key(2026102911)));
  ASSERT_EQ(OB_SUCCESS, last.outtable_lob_control_keys_.push_back(make_key(2026102912)));
  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, first));
  EXPECT_FALSE(info.all_threads_finish_);
  EXPECT_EQ(OB_SUCCESS, info.outtable_lob_control_ret_);

  // A missing owner transaction fails the real COMPLETE entry after the final SQC is collected.
  ASSERT_EQ(nullptr, session_.get_tx_desc());
  EXPECT_EQ(OB_ERR_UNEXPECTED, proc.on_sqc_finish_msg(ctx_, last));
  EXPECT_TRUE(info.all_threads_finish_);
  EXPECT_EQ(OB_SUCCESS, info.first_error_code_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.outtable_lob_control_ret_);
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  ASSERT_EQ(2, info.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102911), info.outtable_lob_control_keys_->at(0));
  expect_key_eq(make_key(2026102912), info.outtable_lob_control_keys_->at(1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());

  // Duplicate finish messages cannot clear the error or append the same keys again.
  EXPECT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, last));
  EXPECT_EQ(2, info.outtable_lob_control_keys_->count());
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.complete_outtable_lob_control_keys(ctx_));
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());
}

TEST_F(TestPxOuttableLobControlResult, empty_sqc_messages_preserve_success)
{
  ASSERT_NO_FATAL_FAILURE(prepare_sqc_messages(2));
  ObPxCoordInfo &info = coord_->info();
  ObSerialDfoScheduler scheduler(info, *coord_, coord_->get_listenner());
  ObPxMsgProc proc(info, coord_->get_listenner(), *coord_);
  proc.set_scheduler(&scheduler);
  ObPxFinishSqcResultMsg msg;
  msg.dfo_id_ = 0;
  msg.sqc_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_FALSE(info.all_threads_finish_);
  msg.sqc_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_TRUE(info.all_threads_finish_);
  EXPECT_EQ(nullptr, info.outtable_lob_control_keys_);
  EXPECT_EQ(OB_SUCCESS, info.check_outtable_lob_control_complete());
  EXPECT_EQ(nullptr, session_.get_tx_desc());
}

TEST_F(TestPxOuttableLobControlResult, terminating_sqc_only_collects_keys)
{
  ASSERT_NO_FATAL_FAILURE(prepare_sqc_messages(1));
  ObPxCoordInfo &info = coord_->info();
  ObPxTerminateMsgProc proc(info, coord_->get_listenner());
  ObPxFinishSqcResultMsg msg;
  msg.dfo_id_ = 0;
  msg.sqc_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, msg.outtable_lob_control_keys_.push_back(make_key(2026102913)));

  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_FALSE(info.all_threads_finish_);
  EXPECT_EQ(OB_SUCCESS, info.outtable_lob_control_ret_);
  ASSERT_NE(nullptr, info.outtable_lob_control_keys_);
  ASSERT_EQ(1, info.outtable_lob_control_keys_->count());
  expect_key_eq(make_key(2026102913), info.outtable_lob_control_keys_->at(0));
  // Termination does not run COMPLETE; the close guard must reject the remaining keys.
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());
  ASSERT_EQ(OB_SUCCESS, proc.on_sqc_finish_msg(ctx_, msg));
  EXPECT_EQ(1, info.outtable_lob_control_keys_->count());
  EXPECT_EQ(OB_ERR_UNEXPECTED, info.check_outtable_lob_control_complete());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

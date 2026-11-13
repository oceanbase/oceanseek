/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <functional>
#include <thread>
#include "lib/utility/ob_defer.h"
#define private public
#define protected public
#define UNITTEST

#include "storage/tx/ob_mock_tx_ctx.h"
#include "storage/tx/ob_mock_tx_log_adapter.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "storage/tx_table/tx_table_local_buffer.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

namespace unittest
{
// Observe publication at the existing test replacement of set_exiting_().
static std::function<void(const ObTransCtx &)> before_exiting_hook;

class TestMockObTxCtx : public ::testing::Test
{
protected:
  virtual void SetUp() override
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    mailbox_mgr_.reset();
  }
  virtual void TearDown() override
  {
    mailbox_mgr_.reset();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  int check_mail(ObMailBox<ObTxMsg> &mailbox,
                 int64_t from,
                 int64_t to,
                 int64_t type)
  {
    int ret = OB_SUCCESS;
    ObMail<ObTxMsg> mail;
    EXPECT_EQ(OB_SUCCESS, mailbox.fetch_mail(mail));

    EXPECT_EQ(from, mail.from_);
    EXPECT_EQ(to, mail.to_);
    EXPECT_EQ(type, mail.mail_->get_msg_type());

    switch (type) {
    case TX_COMMIT_RESP: {
      const ObTxCommitRespMsg *tx_commit_resp = dynamic_cast<const ObTxCommitRespMsg*>(mail.mail_);
      EXPECT_EQ(OB_SUCCESS, tx_commit_resp->ret_);
      TRANS_LOG(INFO, "check tx commit resp", K(mail), KPC(tx_commit_resp));
      break;
    }
    default: {
      TRANS_LOG(ERROR, "invalid msg type", K(mail));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
    }

    return ret;
  }

  int build_scheduler_mailbox()
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(mailbox_mgr_.register_mailbox(scheduler_addr_,
                                              scheduler_mailbox_,
                                              NULL))) {
      TRANS_LOG(ERROR, "mock ctx register mailbox failed");
    }

    return ret;
  }

public:
  ObMailBoxMgr<ObTxMsg> mailbox_mgr_;
  int64_t scheduler_addr_ = 0;
  ObMailBox<ObTxMsg> scheduler_mailbox_;
};

TEST_F(TestMockObTxCtx, clear_watermark_after_commit_or_abort)
{
  const uint64_t saved_version = GET_MIN_CLUSTER_VERSION();
  DEFER(common::ObClusterVersion::get_instance().update_cluster_version(saved_version));
  common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_1_0_0);

  struct ClearCase {
    ObTxState state;
    int64_t end_scn;
    int64_t max_commit_scn;
    int64_t request_scn;
    int expected_ret;
  };
  const ClearCase cases[] = {
    // ABORT keeps the same strict CLEAR watermark checks as COMMIT.
    {ObTxState::ABORT, 1791448444374752001, 0, 1791448444341769000, OB_ERR_UNEXPECTED},
    {ObTxState::ABORT, 1791448444374752001, 0, 1791448444374752001, OB_SUCCESS},
    {ObTxState::ABORT, 100, 200, 150, OB_ERR_UNEXPECTED},
    {ObTxState::ABORT, 100, 200, 200, OB_SUCCESS},
    {ObTxState::COMMIT, 200, 100, 150, OB_ERR_UNEXPECTED},
    {ObTxState::COMMIT, 100, 200, 150, OB_ERR_UNEXPECTED},
    {ObTxState::COMMIT, 200, 200, 200, OB_SUCCESS},
    {ObTxState::COMMIT, 200, 200, 201, OB_SUCCESS},
  };
  for (const auto &test_case : cases) {
    SCOPED_TRACE(static_cast<int>(test_case.state));
    SCOPED_TRACE(test_case.request_scn);
    ObTxData data;
    MockObTxCtx ctx;
    ctx.change_to_leader();
    ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
    ASSERT_EQ(OB_SUCCESS, ctx.set_downstream_state(test_case.state));
    SCN end_scn;
    end_scn.convert_for_tx(test_case.end_scn);
    ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_end_log_ts(end_scn));
    ctx.max_2pc_commit_scn_.convert_for_tx(test_case.max_commit_scn);
    const SCN original_max_scn = ctx.max_2pc_commit_scn_;
    Ob2pcClearReqMsg msg;
    msg.sender_ = ObLSID(1001);
    msg.max_commit_log_scn_.convert_for_tx(test_case.request_scn);
    ctx.msg_2pc_cache_ = &msg;
    EXPECT_EQ(test_case.expected_ret,
              ctx.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ));
    ctx.msg_2pc_cache_ = nullptr;
    EXPECT_EQ(end_scn, ctx.ctx_tx_data_.get_end_log_ts());
    if (OB_SUCCESS == test_case.expected_ret) {
      EXPECT_EQ(msg.sender_, ctx.exec_info_.upstream_);
      EXPECT_EQ(SCN::max(original_max_scn, msg.max_commit_log_scn_), ctx.max_2pc_commit_scn_);
    } else {
      EXPECT_EQ(original_max_scn, ctx.max_2pc_commit_scn_);
      EXPECT_FALSE(ctx.exec_info_.upstream_.is_valid());
    }
  }
}

// The boxed payload used by AbortResp before max_abort_log_scn_ was appended.
struct LegacyAbortPayload
{
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(LegacyAbortPayload);

TEST_F(TestMockObTxCtx, abort_watermark_serialization_compatibility)
{
  char buf[4096];
  Ob2pcAbortRespMsg msg;
  EXPECT_FALSE(msg.max_abort_log_scn_.is_valid());
  // The 4.6.1 baseline uses the boxed format; pre-box compatibility was removed.
  msg.cluster_version_ = CLUSTER_VERSION_4_6_1_0;
  ASSERT_EQ(OB_SUCCESS, msg.max_abort_log_scn_.convert_for_tx(200));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buf, sizeof(buf), pos));
  EXPECT_EQ(msg.get_serialize_size(), pos);
  const int64_t len = pos;
  Ob2pcAbortRespMsg decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, len, pos));
  EXPECT_EQ(len, pos);
  EXPECT_EQ(msg.max_abort_log_scn_, decoded.max_abort_log_scn_);

  // An old boxed reader skips the appended field using the payload length.
  ObTxMsg old_header;
  LegacyAbortPayload old_payload;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_header.deserialize(buf, len, pos));
  ASSERT_EQ(OB_SUCCESS, old_payload.deserialize(buf, len, pos));
  EXPECT_EQ(len, pos);

  // A new reader accepts the old empty payload with an invalid watermark.
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, msg.ObTxMsg::serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, old_payload.serialize(buf, sizeof(buf), pos));
  const int64_t old_len = pos;
  Ob2pcAbortRespMsg old_decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_decoded.deserialize(buf, old_len, pos));
  EXPECT_EQ(old_len, pos);
  EXPECT_FALSE(old_decoded.max_abort_log_scn_.is_valid());
}

class WatermarkTxCtx : public MockObTxCtx
{
public:
  SCN sent_scn_;
  int64_t sent_type_ = TX_UNKNOWN;
  int64_t clear_count_ = 0;
  int64_t abort_count_ = 0;
  int post_msg_(const ObLSID &, ObTxMsg &msg) override
  {
    sent_type_ = msg.get_msg_type();
    if (TX_2PC_ABORT_RESP == sent_type_) {
      sent_scn_ = static_cast<const Ob2pcAbortRespMsg &>(msg).max_abort_log_scn_;
    } else if (TX_2PC_CLEAR_REQ == sent_type_) {
      sent_scn_ = static_cast<const Ob2pcClearReqMsg &>(msg).max_commit_log_scn_;
      ++clear_count_;
    } else if (TX_2PC_ABORT_REQ == sent_type_) {
      ++abort_count_;
    } else {
      return OB_ERR_UNEXPECTED;
    }
    return OB_SUCCESS;
  }
};

TEST_F(TestMockObTxCtx, abort_watermark_aggregation_and_orphan_clear)
{
  const uint64_t saved_version = GET_MIN_CLUSTER_VERSION();
  DEFER(common::ObClusterVersion::get_instance().update_cluster_version(saved_version));
  common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_1_0_0);
  ObTxData leaf_data;
  ObTxData parent_data;
  WatermarkTxCtx leaf;
  WatermarkTxCtx parent;
  leaf.change_to_leader();
  parent.change_to_leader();
  ASSERT_EQ(OB_SUCCESS, leaf.init(ObLSID(1), ObTransID(1), nullptr, &leaf_data, &mailbox_mgr_));
  ASSERT_EQ(OB_SUCCESS, parent.init(ObLSID(1001), ObTransID(1), nullptr, &parent_data, &mailbox_mgr_));
  ASSERT_EQ(OB_SUCCESS, leaf.set_downstream_state(ObTxState::ABORT));
  ASSERT_EQ(OB_SUCCESS, parent.set_downstream_state(ObTxState::ABORT));
  SCN abort_scn;
  SCN local_scn;
  ASSERT_EQ(OB_SUCCESS, abort_scn.convert_for_tx(1791448444374752001));
  ASSERT_EQ(OB_SUCCESS, local_scn.convert_for_tx(1791448444341769000));
  ASSERT_EQ(OB_SUCCESS, leaf.ctx_tx_data_.set_end_log_ts(abort_scn));
  ASSERT_EQ(OB_SUCCESS, parent.ctx_tx_data_.set_end_log_ts(local_scn));
  const SCN commit_version = parent.ctx_tx_data_.get_commit_version();

  // Leaf sends its own ABORT SCN even though its aggregated watermark is invalid.
  ASSERT_FALSE(leaf.max_2pc_commit_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, leaf.ObPartTransCtx::post_msg_(
      ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP, ObLSID(1001)));
  ASSERT_EQ(TX_2PC_ABORT_RESP, leaf.sent_type_);
  EXPECT_EQ(abort_scn, leaf.sent_scn_);
  Ob2pcAbortRespMsg resp;
  resp.max_abort_log_scn_ = leaf.sent_scn_;
  parent.msg_2pc_cache_ = &resp;
  ASSERT_EQ(OB_SUCCESS, parent.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP));
  parent.msg_2pc_cache_ = nullptr;
  EXPECT_EQ(abort_scn, parent.max_2pc_commit_scn_);

  // Retries, smaller responses, and old/orphan responses cannot lower the maximum.
  for (const SCN &scn : {abort_scn, local_scn, SCN::invalid_scn()}) {
    resp.max_abort_log_scn_ = scn;
    parent.msg_2pc_cache_ = &resp;
    ASSERT_EQ(OB_SUCCESS, parent.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP));
    parent.msg_2pc_cache_ = nullptr;
    EXPECT_EQ(abort_scn, parent.max_2pc_commit_scn_);
  }
  EXPECT_EQ(commit_version, parent.ctx_tx_data_.get_commit_version());
  EXPECT_EQ(local_scn, parent.ctx_tx_data_.get_end_log_ts());
  EXPECT_EQ(ObTxState::ABORT, parent.get_downstream_state());

  // An internal node forwards the subtree maximum; a root uses it for CLEAR.
  ASSERT_EQ(OB_SUCCESS, parent.ObPartTransCtx::post_msg_(
      ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP, ObLSID(1002)));
  EXPECT_EQ(abort_scn, parent.sent_scn_);
  ASSERT_EQ(OB_SUCCESS, parent.ObPartTransCtx::post_msg_(
      ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ, ObLSID(1)));
  EXPECT_EQ(abort_scn, parent.sent_scn_);

  // With no coordinator context, the response still raises the local LS watermark.
  resp.max_abort_log_scn_ = abort_scn;
  Ob2pcClearReqMsg clear;
  clear.sender_ = ObLSID(1001);
  clear.max_commit_log_scn_ = ObPartTransCtx::get_orphan_clear_scn_(resp, local_scn);
  EXPECT_EQ(abort_scn, clear.max_commit_log_scn_);
  leaf.msg_2pc_cache_ = &clear;
  ASSERT_EQ(OB_SUCCESS, leaf.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ));
  leaf.msg_2pc_cache_ = nullptr;
  EXPECT_EQ(abort_scn, leaf.ctx_tx_data_.get_end_log_ts());

  SCN larger_scn;
  ASSERT_EQ(OB_SUCCESS, larger_scn.convert_for_tx(1791448444374752002));
  EXPECT_EQ(larger_scn, ObPartTransCtx::get_orphan_clear_scn_(resp, larger_scn));
  resp.max_abort_log_scn_.reset();
  EXPECT_EQ(local_scn, ObPartTransCtx::get_orphan_clear_scn_(resp, local_scn));
}

// Field case 2026111000120508270: the coordinator LS has already dropped the txn context and
// receives a COMMIT_RESP whose commit_log_scn is newer than its own decided watermark. The
// orphan CLEAR must cover the participant watermark, otherwise the participant rejects it
// with -4016 while the strict receiver-side validation stays in place.
TEST_F(TestMockObTxCtx, orphan_commit_resp_watermark_drives_clear)
{
  SCN commit_scn;
  SCN local_scn;
  SCN larger_scn;
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(1794310435396031002));
  ASSERT_EQ(OB_SUCCESS, local_scn.convert_for_tx(1794310435383927000));
  ASSERT_EQ(OB_SUCCESS, larger_scn.convert_for_tx(1794310435396031003));

  Ob2pcCommitRespMsg commit_resp;
  commit_resp.commit_log_scn_ = commit_scn;

  ObTxData data;
  WatermarkTxCtx leaf;
  leaf.change_to_leader();
  ASSERT_EQ(OB_SUCCESS, leaf.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
  ASSERT_EQ(OB_SUCCESS, leaf.set_downstream_state(ObTxState::COMMIT));
  ASSERT_EQ(OB_SUCCESS, leaf.ctx_tx_data_.set_end_log_ts(commit_scn));

  // A CLEAR built from the local LS watermark alone is rejected by the participant.
  Ob2pcClearReqMsg stale_clear;
  stale_clear.sender_ = ObLSID(1001);
  stale_clear.max_commit_log_scn_ = local_scn;
  leaf.msg_2pc_cache_ = &stale_clear;
  EXPECT_EQ(OB_ERR_UNEXPECTED, leaf.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ));
  leaf.msg_2pc_cache_ = nullptr;

  // Merging the response watermark produces a CLEAR the participant accepts.
  Ob2pcClearReqMsg clear;
  clear.sender_ = ObLSID(1001);
  clear.max_commit_log_scn_ = ObPartTransCtx::get_orphan_clear_scn_(commit_resp, local_scn);
  EXPECT_EQ(commit_scn, clear.max_commit_log_scn_);
  leaf.msg_2pc_cache_ = &clear;
  ASSERT_EQ(OB_SUCCESS, leaf.apply_2pc_msg_(ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ));
  leaf.msg_2pc_cache_ = nullptr;

  // A newer local watermark is never lowered.
  EXPECT_EQ(larger_scn, ObPartTransCtx::get_orphan_clear_scn_(commit_resp, larger_scn));

  // An invalid field, as sent by an older version, keeps the original local watermark.
  commit_resp.commit_log_scn_.reset();
  EXPECT_EQ(local_scn, ObPartTransCtx::get_orphan_clear_scn_(commit_resp, local_scn));

  // The ABORT branch keeps its behavior.
  Ob2pcAbortRespMsg abort_resp;
  abort_resp.max_abort_log_scn_ = commit_scn;
  EXPECT_EQ(commit_scn, ObPartTransCtx::get_orphan_clear_scn_(abort_resp, local_scn));
}

class OrphanWatermarkLogAdapter : public MockTxLogAdapter
{
public:
  int get_max_decided_scn(SCN &scn) override
  {
    ++query_count_;
    if (on_query_) {
      on_query_();
    }
    scn = continuous_scn_;
    return ret_;
  }
  SCN continuous_scn_ = SCN::min_scn();
  int ret_ = OB_SUCCESS;
  int query_count_ = 0;
  std::function<void()> on_query_;
};

TEST_F(TestMockObTxCtx, orphan_clear_covers_sibling_after_coordinator_exit)
{
  OrphanWatermarkLogAdapter adapter;
  ObLSTxCtxMgr mgr;
  // Only the watermark APIs are exercised; no map or log writer is needed.
  mgr.is_inited_ = true;
  mgr.tx_log_adapter_ = &adapter;
  DEFER(mgr.is_inited_ = false);
  SCN local_scn, sender_commit, sibling_commit, original_clear, result;
  ASSERT_EQ(OB_SUCCESS, local_scn.convert_for_tx(80));
  ASSERT_EQ(OB_SUCCESS, sender_commit.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, sibling_commit.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, original_clear.convert_for_tx(201));
  adapter.continuous_scn_ = local_scn;
  mgr.update_max_clear_scn(original_clear);

  Ob2pcCommitRespMsg resp;
  resp.commit_log_scn_ = sender_commit;
  // Merging just the sender would still miss the higher sibling COMMIT.
  EXPECT_LT(ObPartTransCtx::get_orphan_clear_scn_(resp, local_scn), sibling_commit);
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_EQ(original_clear, ObPartTransCtx::get_orphan_clear_scn_(resp, result));
  EXPECT_GE(result, sibling_commit);
  EXPECT_EQ(0, adapter.query_count_);

  // Do not change the continuous progress API or the minimum recovery position.
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_decided_scn(result));
  EXPECT_EQ(local_scn, result);
  EXPECT_EQ(1, adapter.query_count_);
  ASSERT_EQ(OB_SUCCESS, mgr.update_aggre_log_ts_wo_lock(sender_commit));
  ASSERT_EQ(OB_SUCCESS, mgr.update_aggre_log_ts_wo_lock(local_scn));
  EXPECT_EQ(local_scn, mgr.aggre_rec_scn_);
}

TEST_F(TestMockObTxCtx, orphan_clear_uses_recovery_progress_until_clear_published)
{
  OrphanWatermarkLogAdapter adapter;
  ObLSTxCtxMgr mgr;
  mgr.is_inited_ = true;
  mgr.tx_log_adapter_ = &adapter;
  DEFER(mgr.is_inited_ = false);
  SCN checkpoint, historical_clear, result;
  ASSERT_EQ(OB_SUCCESS, checkpoint.convert_for_tx(301));
  ASSERT_EQ(OB_SUCCESS, historical_clear.convert_for_tx(300));
  // After restart no historical CLEAR has updated the in-memory maximum.
  // Replay skips SCNs < checkpoint and returns checkpoint - 1 as its baseline.
  adapter.continuous_scn_ = SCN::scn_dec(checkpoint);
  ASSERT_TRUE(mgr.max_clear_scn_.is_min());
  adapter.ret_ = OB_STATE_NOT_MATCH;
  EXPECT_EQ(OB_STATE_NOT_MATCH, mgr.get_max_clear_scn(result));
  EXPECT_EQ(1, adapter.query_count_);

  adapter.ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_GE(result, historical_clear);
  EXPECT_EQ(2, adapter.query_count_);
  EXPECT_TRUE(mgr.max_clear_scn_.is_min());

  // The CLEAR exactly at the filtering point must be replayed/registered.
  mgr.update_max_clear_scn(checkpoint);
  // Later redo can advance continuous progress without changing the bound
  // already provided by the published CLEAR for its completed transaction.
  ASSERT_EQ(OB_SUCCESS, adapter.continuous_scn_.convert_for_tx(401));
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_EQ(checkpoint, result);
  EXPECT_GE(result, historical_clear);
  EXPECT_EQ(2, adapter.query_count_);
  ASSERT_EQ(OB_SUCCESS, mgr.offline());
  EXPECT_EQ(checkpoint, mgr.max_clear_scn_.atomic_load());

  // A published CLEAR also avoids an unavailable continuous-progress query.
  adapter.ret_ = OB_STATE_NOT_MATCH;
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_EQ(checkpoint, result);
  EXPECT_EQ(2, adapter.query_count_);
}

TEST_F(TestMockObTxCtx, orphan_clear_rechecks_publication_during_recovery_query)
{
  OrphanWatermarkLogAdapter adapter;
  ObLSTxCtxMgr mgr;
  mgr.is_inited_ = true;
  mgr.tx_log_adapter_ = &adapter;
  DEFER(mgr.is_inited_ = false);
  SCN clear_scn, result;
  ASSERT_EQ(OB_SUCCESS, adapter.continuous_scn_.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(601));
  // Deterministically publish after the initial empty read, during the query.
  adapter.on_query_ = [&]() { mgr.update_max_clear_scn(clear_scn); };
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_EQ(clear_scn, result);
  EXPECT_EQ(1, adapter.query_count_);

  ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
  EXPECT_EQ(clear_scn, result);
  EXPECT_EQ(1, adapter.query_count_);
}

TEST_F(TestMockObTxCtx, orphan_clear_retries_after_failed_progress_query)
{
  for (int query_error : {OB_STATE_NOT_MATCH, OB_TIMEOUT}) {
    SCOPED_TRACE(query_error);
    OrphanWatermarkLogAdapter adapter;
    ObLSTxCtxMgr mgr;
    mgr.is_inited_ = true;
    mgr.tx_log_adapter_ = &adapter;
    DEFER(mgr.is_inited_ = false);
    SCN clear_scn, result;
    ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(601));
    ASSERT_EQ(OB_SUCCESS, adapter.continuous_scn_.convert_for_tx(901));
    adapter.ret_ = query_error;
    ASSERT_TRUE(mgr.max_clear_scn_.is_min());
    adapter.on_query_ = [&]() { mgr.update_max_clear_scn(clear_scn); };
    ASSERT_EQ(query_error, mgr.get_max_clear_scn(result));
    EXPECT_EQ(1, adapter.query_count_);

    // Retry uses the concurrently published CLEAR without another query or
    // consuming the previous failed query's unusable output.
    ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
    EXPECT_EQ(clear_scn, result);
    EXPECT_EQ(1, adapter.query_count_);
  }
}

TEST_F(TestMockObTxCtx, max_clear_scn_uses_published_clear_after_manager_stop)
{
  for (int published = 0; published < 2; ++published) {
    SCOPED_TRACE(published);
    OrphanWatermarkLogAdapter adapter;
    ObLSTxCtxMgr mgr;
    mgr.tx_log_adapter_ = &adapter;
    DEFER(mgr.is_inited_ = false);
    SCN clear_scn, result;
    ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(201));
    EXPECT_EQ(OB_NOT_INIT, mgr.get_max_clear_scn(result));

    mgr.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, mgr.tx_ls_state_mgr_.init(ObLSID(1)));
    ASSERT_EQ(OB_SUCCESS, mgr.tx_ls_state_mgr_.switch_tx_ls_state(ObTxLSStateMgr::TxLSAction::START));
    if (published) {
      mgr.update_max_clear_scn(clear_scn);
    }
    ASSERT_EQ(OB_SUCCESS, mgr.tx_ls_state_mgr_.switch_tx_ls_state(ObTxLSStateMgr::TxLSAction::STOP));
    if (published) {
      // A published watermark remains usable while the stopped manager is alive.
      ASSERT_EQ(OB_SUCCESS, mgr.get_max_clear_scn(result));
      EXPECT_EQ(clear_scn, result);
    } else {
      EXPECT_EQ(OB_STATE_NOT_MATCH, mgr.get_max_clear_scn(result));
    }
    EXPECT_EQ(OB_STATE_NOT_MATCH, mgr.get_max_decided_scn(result));
    EXPECT_EQ(0, adapter.query_count_);
  }
}

TEST_F(TestMockObTxCtx, clear_watermark_is_published_before_exiting)
{
  // Check the leader callback, complete/incomplete replay and filtered replay.
  for (int mode = 0; mode < 4; ++mode) {
    SCOPED_TRACE(mode);
    ObLSTxCtxMgr mgr;
    ObTxData data;
    MockObTxCtx ctx;
    ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
    ctx.ls_tx_ctx_mgr_ = &mgr;
    ctx.exec_info_.trans_type_ = TransType::DIST_TRANS;
    SCN clear_scn, recovery_scn;
    ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(201));
    ASSERT_EQ(OB_SUCCESS, recovery_scn.convert_for_tx(100));
    ctx.rec_log_ts_ = recovery_scn;
    bool observed_exit = false;
    before_exiting_hook = [&](const ObTransCtx &exiting_ctx) {
      EXPECT_FALSE(exiting_ctx.is_exiting_);
      EXPECT_EQ(clear_scn, mgr.max_clear_scn_.atomic_load());
      observed_exit = true;
    };
    DEFER(before_exiting_hook = nullptr);
    if (mode != 3) {
      mgr.inc_active_tx_count();
    }
    if (mode == 0) {
      ctx.exec_info_.max_applied_log_ts_ = clear_scn;
      ASSERT_EQ(OB_SUCCESS, ctx.on_clear());
    } else {
      ctx.role_state_ = TxCtxRoleState::FOLLOWER;
      ctx.exec_info_.max_applying_log_ts_ = clear_scn;
      ctx.exec_info_.max_applying_part_log_no_ = mode == 3 ? 1 : 0;
      ctx.replay_completeness_.set(mode != 2);
      ASSERT_EQ(OB_SUCCESS, ctx.set_downstream_state(ObTxState::COMMIT));
      ObTxClearLogTempRef ref;
      ObTxClearLog log(ref);
      ASSERT_EQ(OB_SUCCESS, ctx.replay_clear(log, palf::LSN(100), clear_scn, 0));
    }
    EXPECT_EQ(mode != 3, observed_exit);
    EXPECT_EQ(mode != 3, ctx.is_exiting_);
    EXPECT_EQ(clear_scn, mgr.max_clear_scn_.atomic_load());
    EXPECT_EQ(recovery_scn, mgr.aggre_rec_scn_);
    mailbox_mgr_.reset();
  }
}

TEST_F(TestMockObTxCtx, non_clear_cleanup_does_not_publish_clear_watermark)
{
  for (int published = 0; published < 2; ++published) {
    SCOPED_TRACE(published);
    ObLSTxCtxMgr mgr;
    ObTxData data;
    MockObTxCtx ctx;
    ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
    ctx.ls_tx_ctx_mgr_ = &mgr;
    SCN recovery_scn, clear_scn, terminal_scn;
    ASSERT_EQ(OB_SUCCESS, recovery_scn.convert_for_tx(100));
    ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(201));
    ASSERT_EQ(OB_SUCCESS, terminal_scn.convert_for_tx(350));
    ctx.rec_log_ts_ = recovery_scn;
    if (published) {
      mgr.update_max_clear_scn(clear_scn);
    }
    const SCN expected = mgr.max_clear_scn_.atomic_load();

    // ABORT and local COMMIT use this common cleanup path. Their terminal SCNs
    // must neither initialize nor advance the maximum durable CLEAR log SCN.
    ASSERT_EQ(OB_SUCCESS, ctx.trans_clear_(terminal_scn));
    EXPECT_EQ(expected, mgr.max_clear_scn_.atomic_load());
    EXPECT_EQ(recovery_scn, mgr.aggre_rec_scn_);
    ASSERT_EQ(OB_SUCCESS, ctx.trans_clear_(SCN::invalid_scn()));
    EXPECT_EQ(expected, mgr.max_clear_scn_.atomic_load());
    EXPECT_EQ(recovery_scn, mgr.aggre_rec_scn_);
    mailbox_mgr_.reset();
  }
}

TEST_F(TestMockObTxCtx, exited_clear_is_not_checkpointed)
{
  ObLSTxCtxMgr mgr;
  ObTxData data;
  MockObTxCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
  ctx.ls_tx_ctx_mgr_ = &mgr;
  ctx.role_state_ = TxCtxRoleState::FOLLOWER;
  ctx.exec_info_.trans_type_ = TransType::DIST_TRANS;
  ctx.exec_info_.need_checksum_ = false;
  ctx.replay_completeness_.set(true);
  SCN start_scn, commit_scn, clear_scn;
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, clear_scn.convert_for_tx(201));
  ctx.ctx_tx_data_.test_set_tx_id(1);
  ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_start_log_ts(start_scn));
  ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_commit_version(commit_scn));
  ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_end_log_ts(commit_scn));
  ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_state(ObTxData::COMMIT));
  ctx.exec_info_.max_applying_log_ts_ = clear_scn;
  ctx.exec_info_.max_applying_part_log_no_ = 0;
  ctx.exec_info_.max_applied_log_ts_ = commit_scn;
  ASSERT_EQ(OB_SUCCESS, ctx.set_downstream_state(ObTxState::COMMIT));
  mgr.inc_active_tx_count();
  ObTxClearLogTempRef ref;
  ObTxClearLog log(ref);
  ASSERT_EQ(OB_SUCCESS, ctx.replay_clear(log, palf::LSN(100), clear_scn, 0));
  ASSERT_TRUE(ctx.is_exiting_);

  common::ObArenaAllocator allocator;
  storage::ObTxLocalBuffer buffer(allocator);
  int64_t serialize_size = 0;
  // CLEAR contexts exit and must not be persisted on the supported branch.
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ctx.serialize_tx_ctx_to_buffer(buffer, serialize_size));
}

TEST_F(TestMockObTxCtx, completed_clear_watermark_never_regresses)
{
  ObLSTxCtxMgr mgr;
  std::vector<std::thread> workers;
  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&, i]() {
      for (int j = 100; j > 0; --j) {
        SCN scn;
        scn.convert_for_tx(i * 100 + j);
        mgr.update_max_clear_scn(scn);
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  SCN expected;
  ASSERT_EQ(OB_SUCCESS, expected.convert_for_tx(400));
  EXPECT_EQ(expected, mgr.max_clear_scn_.atomic_load());
  mgr.update_max_clear_scn(SCN::invalid_scn());
  EXPECT_EQ(expected, mgr.max_clear_scn_.atomic_load());
}

class AbortProtocolCtx : public WatermarkTxCtx
{
public:
  // Keep the production 2PC state machine while mocking data/scheduler effects.
  int do_abort() override { return OB_SUCCESS; }
  int on_abort() override { return OB_SUCCESS; }
  int do_clear() override { return OB_SUCCESS; }
};

TEST_F(TestMockObTxCtx, abort_collects_all_responses_before_clear)
{
  ObTxData data;
  AbortProtocolCtx ctx;
  ctx.change_to_leader();
  ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), ObTransID(1), nullptr, &data, &mailbox_mgr_));
  ctx.set_trans_type_(TransType::DIST_TRANS);
  ctx.exec_info_.upstream_ = ObLSID(1);
  for (int64_t id : {1, 2, 3}) {
    ASSERT_EQ(OB_SUCCESS, ctx.exec_info_.participants_.push_back(ObLSID(id)));
  }
  ctx.upstream_state_ = ObTxState::PREPARE;
  ASSERT_EQ(OB_SUCCESS, ctx.set_downstream_state(ObTxState::PREPARE));
  ASSERT_EQ(OB_SUCCESS, ctx.collected_.add_member(2)); // B previously replied PREPARE.
  SCN local_scn;
  ASSERT_EQ(OB_SUCCESS, local_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, ctx.ctx_tx_data_.set_end_log_ts(local_scn));

  Ob2pcAbortRespMsg resp;
  ASSERT_EQ(OB_SUCCESS, resp.max_abort_log_scn_.convert_for_tx(200));
  ctx.msg_2pc_cache_ = &resp;
  ASSERT_EQ(OB_SUCCESS, ctx.handle_2pc_abort_response(1)); // A aborts first.
  ctx.msg_2pc_cache_ = nullptr;
  EXPECT_EQ(ObTxState::ABORT, ctx.get_upstream_state());
  EXPECT_EQ(0, ctx.clear_count_);
  EXPECT_EQ(2, ctx.abort_count_);
  EXPECT_TRUE(ctx.collected_.has_member(1));
  EXPECT_FALSE(ctx.collected_.has_member(2));

  // Completing the local ABORT alone does not allow CLEAR.
  ASSERT_EQ(OB_SUCCESS, ctx.apply_abort_log());
  EXPECT_EQ(0, ctx.clear_count_);
  ASSERT_EQ(OB_SUCCESS, ctx.handle_2pc_prepare_response(2));
  EXPECT_FALSE(ctx.collected_.has_member(2));
  EXPECT_EQ(0, ctx.clear_count_);

  // The last AbortResp must be aggregated before CLEAR is generated.
  ASSERT_EQ(OB_SUCCESS, resp.max_abort_log_scn_.convert_for_tx(300));
  ctx.msg_2pc_cache_ = &resp;
  ASSERT_EQ(OB_SUCCESS, ctx.handle_2pc_abort_response(2));
  ctx.msg_2pc_cache_ = nullptr;
  EXPECT_EQ(ObTxState::CLEAR, ctx.get_upstream_state());
  EXPECT_EQ(2, ctx.clear_count_);
  EXPECT_EQ(resp.max_abort_log_scn_, ctx.max_2pc_commit_scn_);
  EXPECT_EQ(resp.max_abort_log_scn_, ctx.sent_scn_);
}

TEST_F(TestMockObTxCtx, test_simple_tx_ctx1)
{
  int64_t ls_id_gen = ObLSID::MIN_USER_LS_ID;
  MockObTxCtx ctx1;
  MockObTxCtx ctx2;
  ObTransID trans_id1(1);
  ObLSID ls_id0(++ls_id_gen);
  ObLSID ls_id1(++ls_id_gen);
  ObLSID ls_id2(++ls_id_gen);

  ObTxData data1;
  ObTxData data2;
  ctx1.change_to_leader();
  ctx2.change_to_leader();
  EXPECT_EQ(OB_SUCCESS, ctx1.init(ls_id1, trans_id1, nullptr, &data1, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx2.init(ls_id2, trans_id1, nullptr, &data2, &mailbox_mgr_));

  EXPECT_EQ(OB_SUCCESS, build_scheduler_mailbox());

  ctx1.addr_memo_[ls_id2] = ctx2.addr_;
  ctx1.ls_memo_[ctx2.addr_] = ls_id2;
  ctx1.set_trans_type_(TransType::DIST_TRANS);
  ctx1.upstream_state_ = ObTxState::INIT;
  // set self to root
  ctx1.exec_info_.upstream_ = ls_id1;
  ctx1.set_downstream_state(ObTxState::REDO_COMPLETE);
  ctx1.exec_info_.participants_.push_back(ls_id1);
  ctx1.exec_info_.participants_.push_back(ls_id2);
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id1, ctx1.epoch_, 0));
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id2, ctx2.epoch_, 0));
  ctx1.scheduler_addr_ = scheduler_mailbox_.addr_;

  // ctx2.addr_memo_[ls_id1] = ctx1.addr_;
  // ctx2.ls_memo_[ctx1.addr_] = ls_id1;
  // ctx2.set_trans_type_(TransType::DIST_TRANS);
  // ctx2.upstream_state_ = ObTxState::PREPARE;
  // ctx2.exec_info_.upstream_ = ls_id1;

  // EXPECT_NE(share::SCN::max_scn(), ctx1.mt_ctx_.trans_version_);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  EXPECT_EQ(OB_SUCCESS, ctx1.two_phase_commit());

    // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  EXPECT_EQ(OB_SUCCESS, check_mail(scheduler_mailbox_,
                                   ctx1.get_mailbox_addr() /*from*/,
                                   scheduler_addr_,
                                   TX_COMMIT_RESP));

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
  EXPECT_EQ(2, ctx1.coord_prepare_info_arr_.count());
  int64_t id1 = ctx1.coord_prepare_info_arr_[0].id_.id_;
  int64_t id2 = ctx1.coord_prepare_info_arr_[1].id_.id_;
  EXPECT_EQ(true, id1 == 1 || id1 == 2);
  EXPECT_EQ(true, id2 == 1 || id2 == 2);
  TRANS_LOG(INFO, "qianchen debug", K(id1), K(id2), K(ctx1));
}

TEST_F(TestMockObTxCtx, test_transfer_tx_ctx)
{
  int64_t ls_id_gen = ObLSID::MIN_USER_LS_ID;
  MockObTxCtx ctx1;
  MockObTxCtx ctx2;
  MockObTxCtx ctx3;
  ObTransID trans_id1(1);
  ObLSID ls_id0(++ls_id_gen);
  ObLSID ls_id1(++ls_id_gen);
  ObLSID ls_id2(++ls_id_gen);
  ObLSID ls_id3(++ls_id_gen);

  ObTxData data1;
  ObTxData data2;
  ObTxData data3;
  ctx1.change_to_leader();
  ctx2.change_to_leader();
  ctx3.change_to_leader();
  EXPECT_EQ(OB_SUCCESS, ctx1.init(ls_id1, trans_id1, nullptr, &data1, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx2.init(ls_id2, trans_id1, nullptr, &data2, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx3.init(ls_id3, trans_id1, nullptr, &data3, &mailbox_mgr_));

  EXPECT_EQ(OB_SUCCESS, build_scheduler_mailbox());

  ctx1.addr_memo_[ls_id2] = ctx2.addr_;
  ctx1.ls_memo_[ctx2.addr_] = ls_id2;
  ctx2.addr_memo_[ls_id3] = ctx3.addr_;
  ctx2.ls_memo_[ctx3.addr_] = ls_id3;
  ctx1.set_trans_type_(TransType::DIST_TRANS);
  ctx1.upstream_state_ = ObTxState::INIT;
  // set self to root
  ctx1.exec_info_.upstream_ = ls_id1;
  ctx1.set_downstream_state(ObTxState::REDO_COMPLETE);
  ctx1.exec_info_.participants_.push_back(ls_id1);
  ctx1.exec_info_.participants_.push_back(ls_id2);
  ctx1.scheduler_addr_ = scheduler_mailbox_.addr_;
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id1, ctx1.epoch_, 0));
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id2, ctx2.epoch_, 0));

  // ctx2.addr_memo_[ls_id1] = ctx1.addr_;
  // ctx2.ls_memo_[ctx1.addr_] = ls_id1;
  // ctx2.set_trans_type_(TransType::DIST_TRANS);
  // ctx2.upstream_state_ = ObTxState::PREPARE;
  // ctx2.exec_info_.upstream_ = ls_id1;

  // EXPECT_NE(share::SCN::max_scn(), ctx1.mt_ctx_.trans_version_);

  EXPECT_EQ(OB_SUCCESS, ctx2.add_intermediate_participants(ls_id3, 1000));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  EXPECT_EQ(OB_SUCCESS, ctx1.two_phase_commit());

  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx2 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx3 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx2 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  EXPECT_EQ(OB_SUCCESS, check_mail(scheduler_mailbox_,
                                   ctx1.get_mailbox_addr() /*from*/,
                                   scheduler_addr_,
                                   TX_COMMIT_RESP));

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx2 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx3.check_status_valid(true/*should commit*/));
  EXPECT_EQ(2, ctx1.coord_prepare_info_arr_.count());
  int64_t id1 = ctx1.coord_prepare_info_arr_[0].id_.id_;
  int64_t id2 = ctx1.coord_prepare_info_arr_[1].id_.id_;
  EXPECT_EQ(true, id1 == 1 || id1 == 2 || id1 == 3);
  EXPECT_EQ(true, id2 == 1 || id2 == 2 || id2 == 3);
  TRANS_LOG(INFO, "qianchen debug", K(id1), K(id2), K(ctx1));
}
}

namespace storage
{

void ObTxData::dec_ref()
{
  return;
}

} // namespace storage

namespace transaction
{
  void ObTransCtx::after_unlock(CtxLockArg &)
  {
  }

  void ObTransCtx::set_exiting_()
  {
    int tmp_ret = OB_SUCCESS;

    if (!is_exiting_) {
      if (unittest::before_exiting_hook) {
        unittest::before_exiting_hook(*this);
      }
      is_exiting_ = true;
      print_trace_log_if_necessary_();

      const int64_t ctx_ref = get_ref();
      if (NULL == ls_tx_ctx_mgr_) {
        TRANS_LOG_RET(ERROR, tmp_ret, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context", *this);
      } else {
        ls_tx_ctx_mgr_->dec_active_tx_count();
        ls_tx_ctx_mgr_->del_tx_ctx(this);
        TRANS_LOG(DEBUG, "transaction exiting", "context", *this, K(lbt()));
        REC_TRANS_TRACE_EXT2(tlog_, exiting, OB_ID(ctx_ref), ctx_ref, OB_ID(arg1), session_id_);
      }
    }
  }

}

}

int main(int argc, char **argv)
{
  system("rm -rf test_simple_tx_ctx.log*");
  OB_LOGGER.set_file_name("test_simple_tx_ctx.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test simple mock ob tx ctx");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

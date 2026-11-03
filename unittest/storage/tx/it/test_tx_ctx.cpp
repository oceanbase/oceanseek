/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#include <gtest/gtest.h>
#define private public
#define protected public
#define USING_LOG_PREFIX TRANS
#include "test_tx_dsl.h"
#include "tx_node.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share {
int ObTenantTxDataAllocator::init(const char *label, TxShareThrottleTool* throttle_tool)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}
int ObMemstoreAllocator::init(const int64_t ctx_id, const ObMemstoreResourceKind kind)
{
  throttle_tool_ = &MTL_MEM_ALLOC_MGR.share_resource_throttle_tool();
  kind_ = kind;
  return arena_.init(ctx_id, get_label_by_kind(kind));
}
int ObMemstoreAllocator::AllocHandle::bind(const ObMemstoreResourceKind kind)
{
  int ret = OB_SUCCESS;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->get_memstore_allocator_by_kind(kind);
  (void)host.register_handle(*this);
  return ret;
}
};  // namespace share

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t)
{
  return OB_SUCCESS;
}
} // namespace concurrent_control

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode *n) : n_(n) {}

  int operator()(const void *buffer,
                 const int64_t nbytes,
                 const palf::LSN &lsn,
                 const int64_t ts_ns)
  {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }

private:
  ObTxNode *n_;
};

enum class ForceAbortEntry { COMMIT, PREPARE, SUB_PREPARE, PREPARE_REDO };

class ObTestTxCtx : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    MTL_MEM_ALLOC_MGR.init();
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(1001));
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  void check_force_abort_cleanup(const bool prepared,
                                const bool fail_send = false,
                                const bool fail_abort_log = false,
                                const ForceAbortEntry entry = ForceAbortEntry::COMMIT,
                                const bool cascade = false);
  MsgBus bus_;
};

class ForceAbortTestRpc : public ObFakeTransRpc
{
public:
  ForceAbortTestRpc(ObTxNode &node, MsgBus &bus, const bool fail_send)
    : ObFakeTransRpc(&bus, node.addr_, &ObTxNode::get_location_adapter_()),
      self_(node.ls_id_), fail_send_(fail_send)
  {}
  using ObFakeTransRpc::post_msg;
  int post_msg(const ObLSID &receiver, ObTxMsg &msg) override
  {
    if (TX_2PC_ABORT_REQ == msg.type_) {
      ++abort_count_;
      sent_to_self_ |= receiver == self_;
      upstream_ = static_cast<Ob2pcAbortReqMsg &>(msg).upstream_;
      exec_epoch_ = msg.epoch_;
      transfer_epoch_ = msg.transfer_epoch_;
      if (fail_send_) {
        return OB_TIMEOUT;
      }
    }
    return ObFakeTransRpc::post_msg(receiver, msg);
  }
  ObLSID self_;
  bool fail_send_;
  int64_t abort_count_ = 0;
  bool sent_to_self_ = false;
  ObLSID upstream_;
  int64_t exec_epoch_ = -1;
  int64_t transfer_epoch_ = -1;
};

void ObTestTxCtx::check_force_abort_cleanup(const bool prepared,
                                          const bool fail_send,
                                          const bool fail_abort_log,
                                          const ForceAbortEntry entry,
                                          const bool cascade)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 1001);
  const bool xa = ForceAbortEntry::SUB_PREPARE == entry || ForceAbortEntry::PREPARE_REDO == entry;
  const bool internal = ForceAbortEntry::PREPARE == entry || ForceAbortEntry::PREPARE_REDO == entry;
  ObTxNode *n3 = nullptr;
  NAMED_DEFER(delete_n3, delete n3);
  if (cascade) {
    n3 = new ObTxNode(1003, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);
    ASSERT_EQ(OB_SUCCESS, n3->start());
  }
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 60 * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 120, 132));
  if (cascade) {
    ASSERT_EQ(OB_SUCCESS, n3->write(tx, 140, 152));
  }

  ForceAbortTestRpc rpc(*n1, bus_, fail_send);
  ObPartTransCtx *coord = nullptr;
  ObPartTransCtx *part = nullptr;
  ObPartTransCtx *leaf = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, coord));
  NAMED_DEFER(revert_coord, n1->revert_tx_ctx(coord));
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, part));
  NAMED_DEFER(revert_part, n2->revert_tx_ctx(part));
  if (cascade) {
    ASSERT_EQ(OB_SUCCESS, n3->get_tx_ctx(n3->ls_id_, tx.tx_id_, leaf));
  }
  NAMED_DEFER(revert_leaf, { if (nullptr != leaf) { n3->revert_tx_ctx(leaf); } });
  NAMED_DEFER(cleanup_tx, {
    n1->del_drop_msg_type(TX_2PC_PREPARE_RESP);
    n1->del_drop_msg_type(TX_2PC_PREPARE_REDO_RESP);
    n2->del_drop_msg_type(TX_ABORT);
    n2->del_drop_msg_type(KEEPALIVE_RESP);
    if (cascade) {
      n3->del_drop_msg_type(TX_ABORT);
      n3->del_drop_msg_type(KEEPALIVE_RESP);
    }
    if (tx.is_tx_active()) {
      (void)n1->rollback_tx(tx);
    }
  });
  NAMED_DEFER(restore_rpc, {
    CtxLockGuard guard(coord->lock_);
    coord->rpc_ = &n1->fake_rpc_;
  });
  NAMED_DEFER(restore_log, n1->fake_tx_log_adapter_->clear_block_log_type());

  // Keep the scheduler active and suppress GC/ordinary abort. Only the new
  // coordinator notification may release the participant's write lock.
  n1->add_drop_msg_type(TX_2PC_PREPARE_RESP);
  n1->add_drop_msg_type(TX_2PC_PREPARE_REDO_RESP);
  n2->add_drop_msg_type(TX_ABORT);
  n2->add_drop_msg_type(KEEPALIVE_RESP);
  {
    CtxLockGuard guard(part->lock_);
    part->last_ask_scheduler_status_ts_ = n2->ts_after_ms(60000);
  }

  ObTxCommitParts parts;
  for (int64_t i = 0; i < tx.parts_.count(); ++i) {
    if (!cascade || tx.parts_[i].id_ != n3->ls_id_) {
      ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(tx.parts_[i].id_, tx.parts_[i].epoch_, -1)));
    } else {
      CtxLockGuard guard(part->lock_);
      ASSERT_EQ(OB_SUCCESS, part->exec_info_.intermediate_participants_.push_back(
          ObTxExecPart(tx.parts_[i].id_, tx.parts_[i].epoch_, -1)));
    }
  }
  if (cascade) {
    n3->add_drop_msg_type(TX_ABORT);
    n3->add_drop_msg_type(KEEPALIVE_RESP);
    CtxLockGuard guard(leaf->lock_);
    leaf->last_ask_scheduler_status_ts_ = n3->ts_after_ms(60000);
  }
  ObXATransID xid;
  ASSERT_EQ(OB_SUCCESS, xid.set(ObString("force_abort"), ObString("branch"), 1));
  auto wait_for_state = [](ObPartTransCtx *ctx, ObTxState state) {
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
    while (ObTimeUtility::current_time() < deadline) {
      {
        CtxLockGuard guard(ctx->lock_);
        if (state == ctx->get_downstream_state()) {
          return true;
        }
      }
      usleep(1000);
    }
    return false;
  };

  if (prepared) {
    // Model PREPARE sent before the coordinator became force-aborted. Drop
    // PREPARE_RESP so orphan-response recovery cannot mask a missing broadcast.
    ObTenantEnv::set_tenant(&n1->tenant_);
    {
      CtxLockGuard guard(coord->lock_);
      ASSERT_EQ(OB_SUCCESS, coord->set_2pc_participants_(parts));
      coord->exec_info_.xid_ = xid;
      coord->exec_info_.is_sub2pc_ = xa;
      ASSERT_EQ(OB_SUCCESS, coord->post_msg_(xa ? ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REDO_REQ
                                              : ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ,
                                           n2->ls_id_));
    }
    ASSERT_TRUE(wait_for_state(part, xa ? ObTxState::REDO_COMPLETE : ObTxState::PREPARE));
  }

  ObTenantEnv::set_tenant(&n1->tenant_);
  {
    CtxLockGuard guard(coord->lock_);
    coord->rpc_ = &rpc;
    coord->sub_state_.set_force_abort();
    if (xa || internal) {
      // Model a rejected first prepare with no installed coordinator list.
      // INTERNAL descendants are discovered through transfer metadata.
      coord->exec_info_.participants_.reset();
      coord->exec_info_.commit_parts_.reset();
      coord->self_id_ = -1;
      if (internal) {
        for (int64_t i = 0; i < parts.count(); ++i) {
          if (parts.at(i).ls_id_ != n1->ls_id_) {
            ASSERT_EQ(OB_SUCCESS, coord->exec_info_.intermediate_participants_.push_back(parts.at(i)));
          }
        }
      }
    }
  }
  if (fail_abort_log) {
    n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_ABORT_LOG);
  }
  // Use the participant entries directly: scheduler error cleanup must not
  // make the regression pass by sending its own abort to the participants.
  if (ForceAbortEntry::SUB_PREPARE == entry) {
    ASSERT_EQ(fail_abort_log ? OB_EAGAIN : OB_TRANS_KILLED,
              coord->sub_prepare(parts, MonotonicTs(), n1->ts_after_ms(5000), ObString(), 1, xid));
  } else if (internal) {
    int ret = OB_SUCCESS;
    if (xa) {
      Ob2pcPrepareRedoReqMsg msg;
      coord->build_tx_common_msg_(n1->ls_id_, msg);
      msg.upstream_ = ObLSID(1000);
      msg.xid_ = xid;
      ret = coord->handle_tx_2pc_prepare_redo_req(msg);
    } else {
      Ob2pcPrepareReqMsg msg;
      coord->build_tx_common_msg_(n1->ls_id_, msg);
      msg.upstream_ = ObLSID(1000);
      ret = coord->handle_tx_2pc_prepare_req(msg);
    }
    ASSERT_EQ(fail_abort_log ? OB_EAGAIN : OB_TRANS_KILLED, ret);
  } else if (fail_abort_log) {
    CtxLockGuard guard(coord->lock_);
    ASSERT_EQ(OB_SUCCESS, coord->set_2pc_participants_(parts));
    ASSERT_EQ(OB_SUCCESS, coord->set_2pc_upstream_(n1->ls_id_));
    bool no_need_submit_log = false;
    // commit() translates EAGAIN into an accepted asynchronous commit. Check
    // the failing prepare step directly without arming a scheduler callback.
    ASSERT_EQ(OB_EAGAIN, coord->do_prepare(no_need_submit_log));
  } else {
    ASSERT_EQ(OB_TRANS_KILLED,
              coord->commit(parts, MonotonicTs(), n1->ts_after_ms(5000), ObString(), 1));
  }
  {
    CtxLockGuard guard(coord->lock_);
    ASSERT_EQ(fail_abort_log ? 0 : 1, rpc.abort_count_);
    ASSERT_FALSE(rpc.sent_to_self_);
    if (!fail_abort_log) {
      ASSERT_EQ(n1->ls_id_, rpc.upstream_);
      for (int64_t i = 0; i < parts.count(); ++i) {
        if (parts.at(i).ls_id_ == n2->ls_id_) {
          ASSERT_EQ(parts.at(i).exec_epoch_, rpc.exec_epoch_);
          ASSERT_EQ(parts.at(i).transfer_epoch_, rpc.transfer_epoch_);
        }
      }
    }
  }

  if (!fail_send && !fail_abort_log) {
    // A prepared participant must also finish ABORT_RESP/CLEAR after the
    // coordinator exits. Check its outcome and exit, then let another writer in.
    auto wait_for_abort = [](ObPartTransCtx *ctx) {
      const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
      while (ObTimeUtility::current_time() < deadline) {
        {
          CtxLockGuard guard(ctx->lock_);
          if (ObTxCommitData::ABORT == ctx->ctx_tx_data_.get_state() && ctx->is_exiting()) {
            return true;
          }
        }
        usleep(1000);
      }
      return false;
    };
    ASSERT_TRUE(wait_for_abort(part));
    auto next_guard = n2->get_tx_guard();
    ObTxDesc &next_tx = next_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n2->start_tx(next_tx, tx_param));
    ASSERT_EQ(OB_SUCCESS, n2->write(next_tx, 120, 133));
    ASSERT_EQ(OB_SUCCESS, n2->rollback_tx(next_tx));
    if (cascade) {
      ASSERT_TRUE(wait_for_abort(leaf));
      auto leaf_guard = n3->get_tx_guard();
      ObTxDesc &leaf_tx = leaf_guard.get_tx_desc();
      ASSERT_EQ(OB_SUCCESS, n3->start_tx(leaf_tx, tx_param));
      ASSERT_EQ(OB_SUCCESS, n3->write(leaf_tx, 140, 153));
      ASSERT_EQ(OB_SUCCESS, n3->rollback_tx(leaf_tx));
    }
  } else {
    {
      CtxLockGuard guard(part->lock_);
      ASSERT_EQ(ObTxState::INIT, part->get_downstream_state());
      ASSERT_FALSE(part->is_exiting());
    }
    // Explicit cleanup only after asserting that failed notification/logging
    // did not end the downstream transaction.
    n1->fake_tx_log_adapter_->clear_block_log_type();
    if (fail_abort_log) {
      ObTenantEnv::set_tenant(&n1->tenant_);
      CtxLockGuard guard(coord->lock_);
      ASSERT_EQ(OB_SUCCESS, coord->compensate_abort_log_());
    }
    ObTenantEnv::set_tenant(&n2->tenant_);
    ASSERT_EQ(OB_SUCCESS, part->abort(ObTxAbortCause::IMPLICIT_ROLLBACK));
  }
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();
  if (cascade) {
    n3->wait_all_redolog_applied();
  }
}

TEST_F(ObTestTxCtx, ForceAbortCoordinatorNotifiesInitParticipant)
{
  check_force_abort_cleanup(false);
}

TEST_F(ObTestTxCtx, ForceAbortCoordinatorNotifiesPreparedParticipant)
{
  check_force_abort_cleanup(true);
}

TEST_F(ObTestTxCtx, ForceAbortNotificationFailurePreservesKilledResult)
{
  check_force_abort_cleanup(false, true);
}

TEST_F(ObTestTxCtx, ForceAbortLogFailureDoesNotNotifyParticipants)
{
  check_force_abort_cleanup(false, false, true);
}

TEST_F(ObTestTxCtx, ForceAbortInternalNotifiesInitParticipant)
{
  check_force_abort_cleanup(false, false, false, ForceAbortEntry::PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortInternalNotifiesPreparedParticipant)
{
  check_force_abort_cleanup(true, false, false, ForceAbortEntry::PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortXaCoordinatorNotifiesInitParticipant)
{
  check_force_abort_cleanup(false, false, false, ForceAbortEntry::SUB_PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortXaCoordinatorNotifiesRedoCompleteParticipant)
{
  check_force_abort_cleanup(true, false, false, ForceAbortEntry::SUB_PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortXaInternalNotifiesInitParticipant)
{
  check_force_abort_cleanup(false, false, false, ForceAbortEntry::PREPARE_REDO);
}

TEST_F(ObTestTxCtx, ForceAbortXaInternalNotifiesRedoCompleteParticipant)
{
  check_force_abort_cleanup(true, false, false, ForceAbortEntry::PREPARE_REDO);
}

TEST_F(ObTestTxCtx, ForceAbortXaNotificationFailurePreservesKilledResult)
{
  check_force_abort_cleanup(false, true, false, ForceAbortEntry::SUB_PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortXaLogFailureDoesNotNotifyParticipants)
{
  check_force_abort_cleanup(false, false, true, ForceAbortEntry::SUB_PREPARE);
}

TEST_F(ObTestTxCtx, ForceAbortXaInternalNotificationFailurePreservesKilledResult)
{
  check_force_abort_cleanup(false, true, false, ForceAbortEntry::PREPARE_REDO);
}

TEST_F(ObTestTxCtx, ForceAbortXaInternalLogFailureDoesNotNotifyParticipants)
{
  check_force_abort_cleanup(false, false, true, ForceAbortEntry::PREPARE_REDO);
}

TEST_F(ObTestTxCtx, ForceAbortPropagatesThroughInternalParticipant)
{
  check_force_abort_cleanup(false, false, false, ForceAbortEntry::COMMIT, true);
}

TEST_F(ObTestTxCtx, ForceAbortXaPropagatesThroughInternalParticipant)
{
  check_force_abort_cleanup(true, false, false, ForceAbortEntry::SUB_PREPARE, true);
}

TEST_F(ObTestTxCtx, XaPreparedWaitsForExternalRollback)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 1001);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 60 * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 120, 132));
  ForceAbortTestRpc rpc(*n1, bus_, false);
  ObPartTransCtx *coord = nullptr;
  ObPartTransCtx *part = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, coord));
  NAMED_DEFER(revert_coord, n1->revert_tx_ctx(coord));
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, part));
  NAMED_DEFER(revert_part, n2->revert_tx_ctx(part));
  NAMED_DEFER(restore_rpc, { CtxLockGuard guard(coord->lock_); coord->rpc_ = &n1->fake_rpc_; });
  ObTxCommitParts parts;
  for (int64_t i = 0; i < tx.parts_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(tx.parts_[i].id_, tx.parts_[i].epoch_, -1)));
  }
  ObXATransID xid;
  ASSERT_EQ(OB_SUCCESS, xid.set(ObString("prepared"), ObString("branch"), 1));
  ObTenantEnv::set_tenant(&n1->tenant_);
  {
    CtxLockGuard guard(coord->lock_);
    coord->rpc_ = &rpc;
  }
  ASSERT_EQ(OB_SUCCESS,
            coord->sub_prepare(parts, MonotonicTs(), n1->ts_after_ms(5000), ObString(), 1, xid));
  auto wait_for = [](ObPartTransCtx *ctx, bool prepared) {
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
    while (ObTimeUtility::current_time() < deadline) {
      {
        CtxLockGuard guard(ctx->lock_);
        if (prepared ? ctx->is_prepared_sub2pc()
                     : (ctx->is_exiting() && ObTxCommitData::ABORT == ctx->ctx_tx_data_.get_state())) {
          return true;
        }
      }
      usleep(1000);
    }
    return false;
  };
  ASSERT_TRUE(wait_for(coord, true));
  // A prepare retry and timer retry must keep the XA prepared promise. The
  // fast notification must not turn waiting for an external decision into GC.
  ASSERT_EQ(OB_SUCCESS,
            coord->sub_prepare(parts, MonotonicTs(), n1->ts_after_ms(5000), ObString(), 2, xid));
  ASSERT_EQ(OB_SUCCESS, coord->handle_timeout(0));
  {
    CtxLockGuard guard(coord->lock_);
    ASSERT_TRUE(coord->is_prepared_sub2pc());
    ASSERT_FALSE(coord->can_be_recycled_());
    ASSERT_FALSE(coord->sub_state_.is_force_abort());
    ASSERT_EQ(0, rpc.abort_count_);
  }
  ASSERT_EQ(OB_SUCCESS, coord->sub_end_tx(3, xid, n1->addr_, true));
  ASSERT_TRUE(wait_for(coord, false));
  ASSERT_TRUE(wait_for(part, false));
  auto next_guard = n2->get_tx_guard();
  ObTxDesc &next_tx = next_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n2->start_tx(next_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n2->write(next_tx, 120, 133));
  ASSERT_EQ(OB_SUCCESS, n2->rollback_tx(next_tx));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();
}

TEST_F(ObTestTxCtx, DelayAbort)
{
  ObTxPartList backup_parts;
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  PREPARE_TX(n1, tx);
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }
  ASSERT_EQ(OB_SUCCESS, backup_parts.assign(tx.parts_));
  tx.parts_.reset();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ObPartTransCtx *tx_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, false /*for_replay*/, tx_ctx));
    GCONF._private_buffer_size = 1;
    // ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_log_impl_(ObTxLogType::TX_REDO_LOG));
    ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_redo_after_write(false, ObTxSEQ()));
    TRANS_LOG(INFO, "[TEST] after submit redo", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.max_applied_log_ts_));
    n1->wait_all_redolog_applied();
    TRANS_LOG(INFO, "[TEST] after on_success redo", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.max_applied_log_ts_));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(false, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }
  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  TRANS_LOG(INFO, "add drop KEEPALIVE msg");
  n1->add_drop_msg_type(KEEPALIVE);

  ls_tx_ctx_mgr->switch_to_follower_forcedly();

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, true /*for_replay*/, tx_ctx));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(true, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  n1->wait_all_redolog_applied();

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx.tx_id_, false /*for_replay*/, tx_ctx));
    TRANS_LOG(INFO, "[TEST] after leader takeover", K(tx_ctx->trans_id_),
              K(tx_ctx->exec_info_.state_), K(tx_ctx->exec_info_.max_applied_log_ts_));
    ASSERT_EQ(true, tx_ctx->exec_info_.redo_lsns_.count() > 0);
    ASSERT_EQ(true, tx_ctx->exec_info_.max_applied_log_ts_.is_valid());
    ASSERT_EQ(true, tx_ctx->sub_state_.is_force_abort());
    ASSERT_EQ(false, tx_ctx->sub_state_.is_state_log_submitted());
    ASSERT_EQ(ObTxState::INIT, tx_ctx->exec_info_.state_);
    ASSERT_EQ(false, tx_ctx->is_follower_());
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx));
  }

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS,
              n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_TRANS_KILLED, n1->write(tx, snapshot, 1110, 1120));
  }

  ASSERT_EQ(OB_SUCCESS, tx.parts_.assign(backup_parts));
  ASSERT_EQ(OB_TRANS_KILLED, n1->commit_tx(tx, n1->ts_after_ms(500)));
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTxCtx, 2PC_ABORT_WITH_SWITCH_TO_FOLLOWER)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 1001)
  
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  // GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 120, 132));

  ASSERT_EQ(tx.parts_.count(), 2);

  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  TRANS_LOG(INFO, "add drop KEEPALIVE msg");
  n1->add_drop_msg_type(KEEPALIVE);
  n2->add_drop_msg_type(KEEPALIVE);

  FLUSH_REDO(n1);
  FLUSH_REDO(n2);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();

  SWITCH_TO_FOLLOWER_FORCEDLY(n2);
  SWITCH_TO_LEADER(n2);

  ObPartTransCtx * ctx_n2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, ctx_n2));
  ASSERT_EQ(ctx_n2->need_force_abort_(), true);

  ObPartTransCtx * ctx_n1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, ctx_n1));
  n1->fake_tx_log_adapter_->set_pause();

  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_COMMIT_INFO_LOG);
  n1->fake_tx_log_adapter_-> push_back_block_log_type(ObTxLogType::TX_PREPARE_LOG);
  COMMIT_TX(n1, tx, 120 * 1000 * 1000);

  usleep(100*1000);

  TRANS_LOG(INFO,
            "[DEBUG] print ctx_n1",
            K(ctx_n1->trans_id_),
            K(ctx_n1->ls_id_),
            K(ctx_n1->upstream_state_),
            K(ctx_n1->exec_info_.state_),
            KPC(ctx_n1->busy_cbs_.get_first()),
            KPC(ctx_n1->busy_cbs_.get_last()));
  ASSERT_EQ(ctx_n1->upstream_state_, ObTxState::ABORT);
  ASSERT_EQ(ctx_n1->busy_cbs_.get_size(), 1);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_first()->cb_arg_array_, ObTxLogType::TX_ABORT_LOG),
            true);
  n1->fake_tx_log_adapter_->clear_block_log_type();
  SWITCH_TO_FOLLOWER_GRACEFULLY(n1);
  TRANS_LOG(INFO,
            "[DEBUG] print ctx_n1 after switch_to_follower",
            K(ctx_n1->trans_id_),
            K(ctx_n1->ls_id_),
            K(ctx_n1->upstream_state_),
            K(ctx_n1->exec_info_.state_),
            KPC(ctx_n1->busy_cbs_.get_first()),
            KPC(ctx_n1->busy_cbs_.get_last()));
  ASSERT_EQ(ctx_n1->busy_cbs_.get_size(), 1);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_first()->cb_arg_array_, ObTxLogType::TX_ABORT_LOG),
            true);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_COMMIT_INFO_LOG),
            false);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_ACTIVE_INFO_LOG),
            false);
  ASSERT_EQ(is_contain(ctx_n1->busy_cbs_.get_last()->cb_arg_array_, ObTxLogType::TX_REDO_LOG),
            false);

  n1->fake_tx_log_adapter_->clear_pause();

  n2->revert_tx_ctx(ctx_n2);
  n1->revert_tx_ctx(ctx_n1);

  n1->wait_all_redolog_applied();
  n2->wait_all_redolog_applied();



  // auto n1x = new ObTxNode(1010, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);     
  // auto n2x = new ObTxNode(1010 + 1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_); 
  // ASSERT_EQ(OB_SUCCESS, n1x->start());                                                   
  // ASSERT_EQ(OB_SUCCESS, n2x->start());                                                   
  // n1x->set_as_follower_replica(*n1);
  // n2x->set_as_follower_replica(*n2);
  //
  // ReplayLogEntryFunctor functor1(n1x);
  // ReplayLogEntryFunctor functor2(n2x);
  // n1x->fake_tx_log_adapter_->replay_all(functor1);
  // n2x->fake_tx_log_adapter_->replay_all(functor2);

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  // ASSERT_EQ(OB_SUCCESS, n1x->wait_all_tx_ctx_is_destoryed());
  // ASSERT_EQ(OB_SUCCESS, n1x->wait_all_tx_ctx_is_destoryed());
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  std::string log_file_name = "test_tx_ctx.log";
  #ifdef TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG
      log_file_name = "test_tx_ctx_no_hash_index.log";
  #endif
  system(std::string("rm -rf " + log_file_name + "*").c_str());
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_file_name.c_str(), true, false,
                       log_file_name.c_str(), // rs
                       log_file_name.c_str(), // election
                       log_file_name.c_str()); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h));
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "common/ob_timeout_ctx.h"
#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;


static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share {

ObTxDataThrottleGuard::~ObTxDataThrottleGuard() {}

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
int check_sequence_set_violation(const concurrent_control::ObWriteFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t ,
                                 const ObTransID ,
                                 const blocksstable::ObDmlFlag ,
                                 const int64_t )
{
  return OB_SUCCESS;
}
}
class ObTestTx : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObAddr ip_port(ObAddr::VER::IPV4, "119.119.0.1",2023);
    ObCurTraceId::init(ip_port);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    MTL_MEM_ALLOC_MGR.init();
    auto test_name = test_info->name();
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(1001));
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};

TEST_F(ObTestTx, basic)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_TWO_TX_NODE(n1, n2);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp0);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));
  int64_t val1 = 0, val2 = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_SUCCESS, n2->read(tx, 101, val2));
  ASSERT_EQ(112, val1);
  ASSERT_EQ(113, val2);
  // rollback to savepoint
  ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp1, 1000 * 1000);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(tx, 101, val2));
  // write after rollback
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 114));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 115));
  // read the write after rollback
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
  ASSERT_EQ(OB_SUCCESS, n2->read(tx, 101, val2));
  ASSERT_EQ(114, val1);
  ASSERT_EQ(115, val2);
  COMMIT_TX(n1, tx, 500 * 1000);
}

TEST_F(ObTestTx, tx_2pc_blocking_and_get_gts_callback_concurrent_problem)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));

  ObPartTransCtx *part_ctx = NULL;
  ObLSID ls_id(1);
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(ls_id, tx.tx_id_, part_ctx));

  // mock gts waiting
  part_ctx->sub_state_.set_gts_waiting();

  // mock transfer
  part_ctx->sub_state_.set_transfer_blocking();

  ObMonotonicTs stc(99);
  ObMonotonicTs srr(100);
  ObMonotonicTs rgt(100);
  share::SCN scn;
  scn.convert_for_gts(100);
  part_ctx->stc_ = stc;
  part_ctx->part_trans_action_ = ObPartTransAction::COMMIT;
  EXPECT_EQ(OB_SUCCESS, part_ctx->get_gts_callback(srr, scn, rgt));
  EXPECT_EQ(true, part_ctx->ctx_tx_data_.get_commit_version() >= scn);
  ObLSID dst_ls_id(2);
  share::SCN start_scn;
  share::SCN end_scn;
  start_scn.convert_for_gts(888);
  end_scn.convert_for_gts(1000);
  part_ctx->ctx_tx_data_.set_start_log_ts(start_scn);
  ObSEArray<ObTabletID, 8> array;
  ObTxCtxMoveArg arg;
  bool is_collected;
  TRANS_LOG(INFO, "qc debug");
  ASSERT_EQ(OB_SUCCESS, part_ctx->collect_tx_ctx(dst_ls_id,
                                                 end_scn,
                                                 array,
                                                 arg,
                                                 is_collected));
  ASSERT_EQ(true, is_collected);

  n1->get_ts_mgr_().repair_get_gts_error();
}

TEST_F(ObTestTx, start_trans_expired)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  auto guard = n1->get_tx_guard();
  ObTxDesc &tx = guard.get_tx_desc();
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000; // 1ms
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  usleep(100000); // 100ms
  // create tx ctx failed caused by trans_timeout
  ASSERT_EQ(OB_TRANS_TIMEOUT, n1->write(tx, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
}

TEST_F(ObTestTx, rollback_savepoint_with_msg_lost)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  ObTxDescGuard guard = n1->get_tx_guard();
  ObTxDesc &tx = guard.get_tx_desc();
  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(5), snapshot));
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ObTxSEQ sp2;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));
  // inject link failure between scheduler to participant 2
  ASSERT_EQ(OB_SUCCESS, bus_.inject_link_failure(n1->addr_, n2->addr_));
  // rollback to savepoint should hang, because of rollback msg can't be delivered
  auto rollback_sp = [&] {
    return n1->rollback_to_implicit_savepoint(tx, sp2, n1->ts_after_ms(5000), nullptr);
  };
  auto async = test::make_async(rollback_sp);
  async.wait_started();
  ASSERT_FALSE(async.is_evaled());
  // interrupt
  ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, 101));
  {int i = 2000;
  while(!async.is_evaled() && i-- > 0) {
    usleep(1000);
  }
  ASSERT_TRUE(i > 0) << "interrupt savepoint rollback";
  }
  ASSERT_EQ(OB_ERR_INTERRUPTED, async.get());
  ASSERT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
  ASSERT_EQ(ObTxDesc::State::ROLLED_BACK, tx.state_);
  // wait part_ctx gc
  ObPartTransCtx *part_ctx = NULL;
  ObLSID ls_id(2);
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(ls_id, tx.tx_id_, part_ctx));
  // release tx, then part_ctx can detect txn was terminated
  ASSERT_EQ(OB_SUCCESS, guard.release());
  part_ctx->last_ask_scheduler_status_ts_ = 0; // ensure check_scheduler_status will not be skipped
  ASSERT_EQ(OB_SUCCESS, part_ctx->check_scheduler_status());
  int i = 0;
  while(!part_ctx->is_exiting_ && i++ < 100) {
    usleep(500);
  }
  ASSERT_EQ(part_ctx->is_exiting_, true);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(part_ctx));
}

TEST_F(ObTestTx, rollback_savepoint_timeout)
{
  START_TWO_TX_NODE(n1, n2);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));
  INJECT_LINK_FAILURE(n1, n2);
  ASYNC_DO(async_op, ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp2, 2000 * 1000));
  ASYNC_WAIT(async_op, 3000 * 1000, wait_ret);
  REPAIR_LINK_FAILURE(n1, n2);
  ROLLBACK_TX(n1, tx);
}

TEST_F(ObTestTx, interrupted_rollback_bounds_location_lookup_timeout)
{
  class TimeoutLocationAdapter : public ObFakeLocationAdapter
  {
  public:
    int nonblock_get_leader(const int64_t cluster_id,
                           const int64_t tenant_id,
                           const ObLSID &ls_id,
                           ObAddr &leader) override
    {
      // Observe the context used by inner SQL on the synchronous send path.
      if (!observed_) {
        timeout_ts_ = ObTimeoutCtx::get_ctx().get_abs_timeout();
        lookup_ts_ = ObTimeUtility::current_time();
        observed_ = true;
      }
      return OB_ERR_UNEXPECTED;
    }
    int64_t timeout_ts_ = -1;
    int64_t lookup_ts_ = -1;
    bool observed_ = false;
  } location;

  START_TWO_TX_NODE(n1, n2);
  for (int i = 0; i < 3; ++i) {
    PREPARE_TX(n1, tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 30 * 1000 * 1000;
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));
    ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_ERR_INTERRUPTED));

    ObTimeoutCtx outer_ctx;
    // Cover the 1s cap, a statement deadline below the old 200ms minimum,
    // and a tighter inherited inner-SQL deadline.
    const int64_t expire_ts = n1->ts_after_us(i == 1 ? 100 * 1000 : 10 * 1000 * 1000);
    if (i == 2) {
      ASSERT_EQ(OB_SUCCESS, outer_ctx.set_abs_timeout(n1->ts_after_ms(100)));
    }
    const int64_t outer_expire_ts = outer_ctx.get_abs_timeout();
    location.observed_ = false;
    ObILocationAdapter *saved_location = n1->fake_rpc_.location_adapter_;
    n1->fake_rpc_.location_adapter_ = &location;
    const int ret = n1->rollback_to_implicit_savepoint(tx, sp2, expire_ts, nullptr);
    n1->fake_rpc_.location_adapter_ = saved_location;

    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
    ASSERT_TRUE(location.observed_);
    ASSERT_GT(location.timeout_ts_, location.lookup_ts_);
    ASSERT_LE(location.timeout_ts_ - location.lookup_ts_, 1000 * 1000);
    ASSERT_LE(location.timeout_ts_, expire_ts);
    if (i == 1) {
      ASSERT_EQ(expire_ts, location.timeout_ts_);
    } else if (i == 2) {
      ASSERT_EQ(outer_expire_ts, location.timeout_ts_);
    }
    ASSERT_EQ(outer_expire_ts, ObTimeoutCtx::get_ctx().get_abs_timeout());
    ASSERT_FALSE(tx.flags_.BLOCK_);
    ASSERT_FALSE(tx.flags_.INTERRUPTED_);
    ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
    // abort_tx_() posts the participant abort while this adapter is still
    // installed, so that message is dropped. rollback_tx() will not post it
    // again once the descriptor is ABORTED. Release the scheduler and make the
    // participant exit before the next case writes the same rows.
    ObPartTransCtx *part_ctx = NULL;
    const ObTransID aborted_tx_id = tx.tx_id_;
    const int ctx_ret = n2->get_tx_ctx(ObLSID(2), aborted_tx_id, part_ctx);
    ASSERT_EQ(OB_SUCCESS, guard.release());
    if (OB_SUCCESS == ctx_ret) {
      part_ctx->last_ask_scheduler_status_ts_ = 0;
      ASSERT_EQ(OB_SUCCESS, part_ctx->check_scheduler_status());
      int wait_cnt = 0;
      while (!part_ctx->is_exiting_ && wait_cnt++ < 200) {
        usleep(500);
      }
      ASSERT_TRUE(part_ctx->is_exiting_);
      ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(part_ctx));
    } else {
      ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, ctx_ret);
    }
  }
}

// Inject at the real synchronous send boundary, while tx.lock_ is released.
class RollbackInterruptLocation : public ObFakeLocationAdapter
{
public:
  int nonblock_get_leader(const int64_t cluster_id, const int64_t tenant_id,
                         const ObLSID &ls_id, ObAddr &leader) override
  {
    on_lookup_();
    return delegate_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader);
  }
  ObILocationAdapter *delegate_ = nullptr;
  std::function<void()> on_lookup_;
};

TEST_F(ObTestTx, interrupted_rollback_fixed_budget)
{
  // Entry interrupt; new/repeated interrupts with success; repeated interrupts
  // with lost RPCs; earlier statement/inherited/transaction deadline; first
  // interrupt after the fixed window; already expired inherited deadline.
  START_TWO_TX_NODE(n1, n2);
  for (int scenario = 0; scenario < 8; ++scenario) {
    SCOPED_TRACE(scenario);
    PREPARE_TX(n1, tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 30 * 1000 * 1000;
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));

    if (scenario != 1 && scenario != 2 && scenario != 6) {
      ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_ERR_INTERRUPTED));
    }
    if (scenario != 0) {
      ASSERT_EQ(OB_SUCCESS, bus_.inject_link_failure(n1->addr_, n2->addr_));
    }
    const int64_t begin = ObTimeUtility::current_time();
    const int64_t statement_end = begin + (scenario == 3 ? 100 * 1000 : 10 * 1000 * 1000);
    ObTimeoutCtx outer_ctx;
    if (scenario == 4 || scenario == 7) {
      ASSERT_EQ(OB_SUCCESS, outer_ctx.set_abs_timeout(begin + (scenario == 4 ? 100 * 1000 : -1)));
    }
    if (scenario == 5) {
      tx.expire_ts_ = begin + 100 * 1000;
    }
    const int64_t outer_end = outer_ctx.get_abs_timeout();
    RollbackInterruptLocation location;
    location.delegate_ = n1->fake_rpc_.location_adapter_;
    int lookups = 0;
    int64_t first_timeout = 0;
    std::atomic<int> kills_started(0);
    std::atomic<int> kills_done(0);
    std::vector<std::thread> killers;
    location.on_lookup_ = [&]() {
      // Abort cleanup also uses location lookup, but must not inject more KILLs.
      if (!tx.flags_.BLOCK_) { return; }
      ++lookups;
      if (((scenario == 1 || scenario == 2) && lookups <= 2) ||
          (scenario == 6 && lookups == 1)) {
        if (scenario == 2) { usleep(200 * 1000); }
        if (scenario == 6) { usleep(1100 * 1000); }
        killers.emplace_back([&]() {
          ++kills_started;
          EXPECT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_ERR_INTERRUPTED));
          ++kills_done;
        });
        // The first KILL must have published the flag before posting resumes.
        bool interrupted = false;
        while (!interrupted) {
          { ObSpinLockGuard lock(tx.lock_); interrupted = tx.flags_.INTERRUPTED_; }
          if (!interrupted) { usleep(100); }
        }
        while (kills_started.load() < lookups) { usleep(100); }
        EXPECT_EQ(0, kills_done.load());
      }
      if (scenario == 1 && lookups == 3) {
        EXPECT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_));
      }
      if ((scenario == 1 || scenario == 2) && lookups >= 2) {
        const int64_t timeout = ObTimeoutCtx::get_ctx().get_abs_timeout();
        if (lookups == 2) { first_timeout = timeout; }
        EXPECT_EQ(first_timeout, timeout); // repeated KILL never renews it
        EXPECT_LE(timeout, begin + 1100 * 1000);
      }
    };
    n1->fake_rpc_.location_adapter_ = &location;
    const int ret = n1->rollback_to_implicit_savepoint(tx, sp2, statement_end, nullptr);
    const int64_t elapsed = ObTimeUtility::current_time() - begin;
    n1->fake_rpc_.location_adapter_ = location.delegate_;
    for (auto &killer : killers) { killer.join(); }
    EXPECT_EQ(kills_started.load(), kills_done.load());
    if (scenario == 1 || scenario == 2) {
      EXPECT_EQ(2, kills_started.load());
      EXPECT_GE(lookups, 3);
    } else if (scenario == 6) {
      EXPECT_EQ(1, kills_started.load());
      EXPECT_EQ(1, lookups);
    }
    EXPECT_EQ(outer_end, ObTimeoutCtx::get_ctx().get_abs_timeout());
    EXPECT_FALSE(tx.flags_.BLOCK_);
    EXPECT_FALSE(tx.flags_.INTERRUPTED_);
    if (scenario <= 1) {
      EXPECT_EQ(OB_SUCCESS, ret);
      EXPECT_LT(elapsed, 1000 * 1000); // no mandatory wait to the window end
      EXPECT_TRUE(tx.is_tx_active());
      int64_t value = 0;
      EXPECT_EQ(OB_SUCCESS, n2->read(tx, 100, value));
      EXPECT_EQ(111, value); // the preceding statement survives
      EXPECT_EQ(OB_SUCCESS, n2->write(tx, 102, 113));
      EXPECT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(1000)));
    } else {
      if (scenario == 2 || scenario == 6) {
        EXPECT_EQ(OB_ERR_INTERRUPTED, ret);
        EXPECT_GE(elapsed, 1000 * 1000);
        EXPECT_LT(elapsed, 1350 * 1000); // no fresh second from a later KILL
      } else {
        EXPECT_EQ(scenario == 5 ? OB_TRANS_TIMEOUT : OB_TIMEOUT, ret);
        EXPECT_LT(elapsed, 200 * 1000); // no legacy minimum-wait extension
      }
      if (scenario == 7) { EXPECT_EQ(0, lookups); }
      EXPECT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_));
      EXPECT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
      // The abort RPC was lost with the rollback RPCs. Let the participant
      // discover that its scheduler has gone before destroying the fake nodes.
      ObPartTransCtx *part_ctx = nullptr;
      const int ctx_ret = n2->get_tx_ctx(ObLSID(2), tx.tx_id_, part_ctx);
      EXPECT_EQ(OB_SUCCESS, guard.release());
      if (OB_SUCCESS == ctx_ret) {
        part_ctx->last_ask_scheduler_status_ts_ = 0;
        EXPECT_EQ(OB_SUCCESS, part_ctx->check_scheduler_status());
        int wait_cnt = 0;
        while (!part_ctx->is_exiting_ && wait_cnt++ < 200) { usleep(500); }
        EXPECT_TRUE(part_ctx->is_exiting_);
        EXPECT_EQ(OB_SUCCESS, n2->revert_tx_ctx(part_ctx));
      } else {
        EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, ctx_ret);
      }
    }
  }
}

// Hold the message-manager bucket's read lock after insertion. Responses can
// still take read locks, but cleanup's del() must wait for our lock. Its writer
// wait bit identifies the post-loop boundary without a production test hook.
TEST_F(ObTestTx, rollback_success_survives_cleanup_delay)
{
  START_TWO_TX_NODE(n1, n2);
  for (int scenario = 0; scenario < 3; ++scenario) {
    SCOPED_TRACE(scenario); // statement expiry, late KILL, concurrent abort
    PREPARE_TX(n1, tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 30 * 1000 * 1000;
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));
    const auto saved_state = tx.state_;
    const int64_t begin = ObTimeUtility::current_time();
    const int64_t statement_end = begin + (scenario == 0 ? 500 * 1000 : 10 * 1000 * 1000);
    RollbackInterruptLocation location;
    location.delegate_ = n1->fake_rpc_.location_adapter_;
    std::atomic<bool> injected(false);
    std::atomic<bool> ready(false);
    std::atomic<bool> kill_done(false);
    std::thread cleanup;
    std::thread killer;
    location.on_lookup_ = [&]() {
      if (injected.exchange(true)) { return; }
      const auto msg_id = tx.brpc_mask_set_.get_tx_msg_id();
      auto &mgr = n1->txs_.rollback_sp_msg_mgr_;
      auto &bucket_lock = mgr.locks_[msg_id.hash() % (sizeof(mgr.locks_) / sizeof(mgr.locks_[0]))].lock_;
      cleanup = std::thread([&, msg_id]() {
        const int lock_ret = bucket_lock.rdlock();
        EXPECT_EQ(OB_SUCCESS, lock_ret);
        ready = true;
        if (OB_SUCCESS != lock_ret) { return; }
        const int64_t watchdog = ObTimeUtility::current_time() + 5 * 1000 * 1000;
        while (0 == (ATOMIC_LOAD(&bucket_lock.latch_.lock_) & ObLatch::WAIT_MASK)
               && ObTimeUtility::current_time() < watchdog) {
          usleep(100);
        }
        const bool deleting = 0 != (ATOMIC_LOAD(&bucket_lock.latch_.lock_) & ObLatch::WAIT_MASK);
        EXPECT_TRUE(deleting);
        bool completed = false;
        {
          ObSpinLockGuard lock(tx.lock_);
          completed = tx.brpc_mask_set_.is_all_mask();
          EXPECT_TRUE(completed);
          EXPECT_EQ(ObTxDesc::State::ROLLBACK_SAVEPOINT, tx.state_);
          EXPECT_TRUE(tx.flags_.BLOCK_);
        }
        EXPECT_LT(ObTimeUtility::current_time(), statement_end);
        if (deleting && completed) {
          if (scenario <= 1) {
            // KILL is first published after the fixed grace window, but after
            // rollback success. It must not retroactively fail that rollback.
            const int64_t release_after = scenario == 0 ? statement_end + 10 * 1000
                                                       : begin + 1100 * 1000;
            while (ObTimeUtility::current_time() < release_after) { usleep(1000); }
            if (scenario == 1) {
              killer = std::thread([&]() {
                EXPECT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_ERR_INTERRUPTED));
                kill_done = true;
              });
              bool interrupted = false;
              while (!interrupted && ObTimeUtility::current_time() < watchdog) {
                { ObSpinLockGuard lock(tx.lock_); interrupted = tx.flags_.INTERRUPTED_; }
                if (!interrupted) { usleep(100); }
              }
              EXPECT_TRUE(interrupted);
              EXPECT_FALSE(kill_done.load());
            }
          } else {
            EXPECT_EQ(OB_SUCCESS, n1->abort_tx(tx, ObTxAbortCause::IMPLICIT_ROLLBACK));
          }
        }
        EXPECT_EQ(OB_SUCCESS, bucket_lock.unlock());
      });
      while (!ready.load()) { usleep(100); }
    };
    n1->fake_rpc_.location_adapter_ = &location;
    const int ret = n1->rollback_to_implicit_savepoint(tx, sp2, statement_end, nullptr);
    if (cleanup.joinable()) { cleanup.join(); }
    if (killer.joinable()) { killer.join(); }
    n1->fake_rpc_.location_adapter_ = location.delegate_;
    EXPECT_TRUE(injected.load());
    EXPECT_FALSE(tx.flags_.BLOCK_);
    EXPECT_FALSE(tx.flags_.INTERRUPTED_);
    EXPECT_FALSE(tx.brpc_mask_set_.get_tx_msg_id().is_valid());
    EXPECT_EQ(0, n1->txs_.rollback_sp_msg_mgr_.count());
    if (scenario <= 1) {
      EXPECT_EQ(OB_SUCCESS, ret);
      EXPECT_EQ(saved_state, tx.state_);
      if (scenario == 1) { EXPECT_TRUE(kill_done.load()); }
      if (OB_SUCCESS == ret) {
        int64_t value = 0;
        EXPECT_EQ(OB_SUCCESS, n2->read(tx, 100, value));
        EXPECT_EQ(111, value);
        EXPECT_EQ(OB_SUCCESS, n2->write(tx, 102, 113));
        EXPECT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(1000)));
      } else {
        EXPECT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
      }
    } else {
      EXPECT_EQ(OB_TRANS_HAS_DECIDED, ret);
      EXPECT_EQ(ObTxDesc::State::ABORTED, tx.state_);
      EXPECT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
    }
  }
}

TEST_F(ObTestTx, uninterrupted_rollback_preserves_deadlines)
{
  class ObserveLocationTimeout : public ObFakeLocationAdapter
  {
  public:
    int nonblock_get_leader(const int64_t cluster_id, const int64_t tenant_id,
                           const ObLSID &ls_id, ObAddr &leader) override
    {
      ++lookups_;
      const ObTimeoutCtx &ctx = ObTimeoutCtx::get_ctx();
      EXPECT_EQ(expected_timeout_, ctx.get_abs_timeout());
      EXPECT_EQ(expected_timeout_ > 0, ctx.is_timeout_set());
      EXPECT_EQ(expected_trx_timeout_, ctx.get_trx_timeout_us());
      return delegate_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader);
    }
    ObILocationAdapter *delegate_ = nullptr;
    int64_t expected_timeout_ = -1;
    int64_t expected_trx_timeout_ = -1;
    int lookups_ = 0;
  } location;
  struct TestCase
  {
    const char *name_;
    int64_t statement_timeout_us_;
    int64_t tx_timeout_us_;
    int64_t inherited_timeout_us_;
    int expected_ret_;
  };
  const TestCase cases[] = {
    {"statement, unset context", 50 * 1000, 30 * 1000 * 1000, 0, OB_TIMEOUT},
    {"statement, longer context", 50 * 1000, 30 * 1000 * 1000, 5 * 1000 * 1000, OB_TIMEOUT},
    {"statement, shorter context", 50 * 1000, 30 * 1000 * 1000, 10 * 1000, OB_TIMEOUT},
    {"transaction, longer context", 10 * 1000 * 1000, 50 * 1000, 5 * 1000 * 1000, OB_TRANS_TIMEOUT},
  };

  START_TWO_TX_NODE(n1, n2);
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.name_);
    PREPARE_TX(n1, tx);
    PREPARE_TX_PARAM(tx_param);
    tx_param.timeout_us_ = 30 * 1000 * 1000;
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp1);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 100, 111));
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp2);
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, 101, 112));
    ASSERT_FALSE(tx.flags_.INTERRUPTED_);
    ASSERT_EQ(OB_SUCCESS, bus_.inject_link_failure(n1->addr_, n2->addr_));

    ObTimeoutCtx outer_ctx;
    outer_ctx.reset_timeout_us();
    ASSERT_EQ(OB_SUCCESS, outer_ctx.set_trx_timeout_us(7 * 1000 * 1000));
    const int64_t begin = ObTimeUtility::current_time();
    tx.expire_ts_ = begin + test_case.tx_timeout_us_;
    if (test_case.inherited_timeout_us_ > 0) {
      ASSERT_EQ(OB_SUCCESS, outer_ctx.set_abs_timeout(begin + test_case.inherited_timeout_us_));
    }
    location.expected_timeout_ = outer_ctx.get_abs_timeout();
    location.expected_trx_timeout_ = outer_ctx.get_trx_timeout_us();
    location.lookups_ = 0;
    location.delegate_ = n1->fake_rpc_.location_adapter_;
    n1->fake_rpc_.location_adapter_ = &location;
    const int ret = n1->rollback_to_implicit_savepoint(
        tx, sp2, begin + test_case.statement_timeout_us_, nullptr);
    const int64_t elapsed = ObTimeUtility::current_time() - begin;
    n1->fake_rpc_.location_adapter_ = location.delegate_;

    EXPECT_EQ(test_case.expected_ret_, ret);
    // Neither the 50ms original deadline nor a shorter inherited context
    // removes the legacy 200ms RPC wait minimum when there is no interrupt.
    EXPECT_GE(elapsed, 200 * 1000);
    EXPECT_LT(elapsed, 1000 * 1000);
    EXPECT_GT(location.lookups_, 0);
    EXPECT_EQ(&outer_ctx, &ObTimeoutCtx::get_ctx());
    EXPECT_EQ(location.expected_timeout_, outer_ctx.get_abs_timeout());
    EXPECT_EQ(location.expected_trx_timeout_, outer_ctx.get_trx_timeout_us());
    EXPECT_FALSE(tx.flags_.INTERRUPTED_);
    EXPECT_FALSE(tx.flags_.BLOCK_);

    EXPECT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_));
    EXPECT_EQ(OB_SUCCESS, n1->rollback_tx(tx));
    // The injected link failure also dropped abort_tx_()'s message. Release
    // the scheduler so the participant can discover the abort and exit.
    ObPartTransCtx *part_ctx = nullptr;
    const int ctx_ret = n2->get_tx_ctx(ObLSID(2), tx.tx_id_, part_ctx);
    EXPECT_EQ(OB_SUCCESS, guard.release());
    if (OB_SUCCESS == ctx_ret) {
      part_ctx->last_ask_scheduler_status_ts_ = 0;
      EXPECT_EQ(OB_SUCCESS, part_ctx->check_scheduler_status());
      int wait_cnt = 0;
      while (!part_ctx->is_exiting_ && wait_cnt++ < 200) { usleep(500); }
      EXPECT_TRUE(part_ctx->is_exiting_);
      EXPECT_EQ(OB_SUCCESS, n2->revert_tx_ctx(part_ctx));
    } else {
      EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, ctx_ret);
    }
  }
}

TEST_F(ObTestTx, rollback_savepoint_with_uncertain_participants)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp);
  ASSERT_TRUE(sp.is_valid());
  share::ObLSArray uncertain_parts;
  ASSERT_EQ(OB_SUCCESS, uncertain_parts.push_back(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_us(100000), &uncertain_parts));
  ASSERT_EQ(ObTxDesc::State::IDLE, tx.state_);
  ASSERT_EQ(0, tx.parts_.count());
}

TEST_F(ObTestTx, rollback_savepoint_with_keep_tx)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);

  {
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp);
    ASSERT_TRUE(sp.is_valid());
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 200));
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_ms(5), nullptr, ObTxCleanPolicy::KEEP));
    ASSERT_EQ(ObTxDesc::State::IMPLICIT_ACTIVE, tx.state_);
    ASSERT_EQ(ObTxSEQ::INVL(), tx.active_scn_);
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_ms(5), nullptr));
  }

  {
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp);
    ASSERT_TRUE(sp.is_valid());
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 200));
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_ms(5), nullptr, ObTxCleanPolicy::KEEP));
    ASSERT_EQ(ObTxDesc::State::IMPLICIT_ACTIVE, tx.state_);
    ASSERT_EQ(ObTxSEQ::INVL(), tx.active_scn_);
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_ms(5), nullptr));
  }
}

TEST_F(ObTestTx, switch_to_follower_gracefully)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  ObTxSEQ sp1, sp2;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }

  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_fail)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  const int64_t TX_CNT = 128;
  ObTxDesc *tx_ptr_arr[TX_CNT] = { nullptr };
  for (int64_t i = 0; i < TX_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr_arr[i]));
    ObTxDesc &tx = *tx_ptr_arr[i];
    // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000 + i, 2000 + i));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

// make switch_to_follower_gracefully_failed
  ObPartTransCtx* last_tx_ctx = nullptr;
  {
    ObLSTxCtxIterator iter;
    ObPartTransCtx* tx_ctx = nullptr;

    ASSERT_EQ(OB_SUCCESS, iter.set_ready(ls_tx_ctx_mgr));
    while (OB_SUCC(iter.get_next_tx_ctx(tx_ctx))) {
      last_tx_ctx = tx_ctx;
      iter.revert_tx_ctx(tx_ctx);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    iter.reset();
  }

  uint64_t tenant_id_backup = last_tx_ctx->tenant_id_;
// make last tx ctx fails to exec switch_to_follower_gracefully,
  last_tx_ctx->tenant_id_ = 0xbaba;

// switch_to_follower_gracefully
  n1->fake_tx_log_adapter_->set_pause();
  ASSERT_NE(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  n1->fake_tx_log_adapter_->clear_pause();
// reset
  last_tx_ctx->tenant_id_ = tenant_id_backup;
  n1->wait_all_redolog_applied();

// check data_complete
// TODO The maintenance of the data_completed_ variable is currently incorrect, and the
// verification will be turned on after it is fixed
  //{
  //  ObLSTxCtxIterator iter;
  //  ObPartTransCtx* tx_ctx = nullptr;
  //  ASSERT_EQ(OB_SUCCESS, iter.set_ready(ls_tx_ctx_mgr));
  //  while (OB_SUCC(iter.get_next_tx_ctx(tx_ctx))) {
  //    ASSERT_EQ(false, tx_ctx->exec_info_.data_complete_);
  //    iter.revert_tx_ctx(tx_ctx);
  //  }
  //  ASSERT_EQ(OB_ITER_END, ret);
  //  iter.reset();
  //}

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    int64_t val1 = 0;

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000 + i, val1));
    ASSERT_EQ(2000 + i, val1);
    ObTxSEQ sp;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000 + i, 3000 + i));
  }

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    int64_t val2 = 0;

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000 + i, val2));
    ASSERT_EQ(3000 + i, val2);
  }

  for (int64_t i = 0; i < TX_CNT; ++i) {
    ObTxDesc &tx = *tx_ptr_arr[i];
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(10000)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_then_forcedly)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    n1->wait_all_redolog_applied();
    ls_tx_ctx_mgr->switch_to_follower_forcedly();
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }

  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_forcedly)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  // disable keepalive msg, because switch to follower forcedly will send keepalive msg to notify
  // scheduler abort tx
  n1->add_drop_msg_type(KEEPALIVE);
  {
    ls_tx_ctx_mgr->switch_to_follower_forcedly();

    int64_t val1 = 0, val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->read(snapshot, 100, val1));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->write(tx, snapshot, 101, 113));
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, resume_leader)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  ObTxSEQ sp1;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

    int64_t val1 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_NOT_MASTER, n1->read(snapshot, 100, val1));
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->resume_leader());
    usleep(100 * 1000);

    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 113));
  }
  {
    int64_t val2 = 0;
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 101, val2));
    ASSERT_EQ(113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_then_resume_leader)
{
  ObTxNode::reset_localtion_adapter();

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;

  ObStoreCtx write_store_ctx;
  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));

    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  {
    ls_tx_ctx_mgr->switch_to_follower_gracefully();
    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));

    n1->wait_all_redolog_applied();
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
    n1->wait_all_redolog_applied();

    //ASSERT_EQ(OB_SUCCESS, n1->read(tx, 100, val1));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1001, 1113));
    ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val2 = 0, val3 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1000, val2));
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1001, val3));

    //ASSERT_EQ(112, val1);
    ASSERT_EQ(1112, val2);
    ASSERT_EQ(1113, val3);
  }
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  }
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode* n) : n_(n) {}

  int operator()(const void *buffer,
               const int64_t nbytes,
               const palf::LSN &lsn,
               const int64_t ts_ns) {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }
private:
  ObTxNode* n_;
};

TEST_F(ObTestTx, replay_basic)
{
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }

    ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
    int64_t retry_count = 0;
    while(ls_tx_ctx_mgr->get_tx_ctx_count() > 0)
    {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected tx ctx counts", K(ls_tx_ctx_mgr->get_ls_id()), K(ls_tx_ctx_mgr->get_tx_ctx_count()));
      ls_tx_ctx_mgr->print_all_tx_ctx(ObLSTxCtxMgr::MAX_HASH_ITEM_PRINT, true);
      retry_count++;
      usleep(100*1000);
      if(retry_count > 10)
      {
          ob_abort();
      }
    }
    ASSERT_EQ(0, ls_tx_ctx_mgr->get_tx_ctx_count());
    ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  }

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;
    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n2->ts_after_ms(100),
                               snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
      ASSERT_EQ(112, val1);
    }

    {
      ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx, n2->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx));
    }
  }
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, replay_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 50 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_gracefully());
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(112, val1);

    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 100, 113));
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    n2->wait_all_redolog_applied();
  }

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

void do_async_read(ObTxNode* n, ObTxReadSnapshot& snapshot, int64_t key, int64_t& val)
{
  LOG_INFO("do sync commit begin");
  ASSERT_EQ(OB_SUCCESS, n->read(snapshot, key, val));
  LOG_INFO("do sync commit end");
}


void do_async_commit(ObTxNode* n, ObTxDesc& tx, int& commit_ret)
{
  LOG_INFO("do async commit begin", K(tx));
  commit_ret = n->commit_tx(tx, n->ts_after_ms(50 * 1000));
  LOG_INFO("do async commit end", K(tx), K(commit_ret));
}

TEST_F(ObTestTx, wait_commit_version_elapse_block)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_elapse_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());

  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_elapse_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_ts_mgr_().update_fake_gts(1);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxDesc *tx_ptr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
  ObTxDesc &tx2 = *tx_ptr2;

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snapshot, 100, val1));
  }

  ObTxNode::get_ts_mgr_().update_fake_gts(ls_tx_ctx_mgr2->max_replay_commit_version_.get_val_for_gts());
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(113, val1);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
  }

  ObTxNode::get_ts_mgr_().elapse_callback();
  t.join();

  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, wait_commit_version_elapse_block_and_switch_to_follower_forcedly)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 1000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_elapse_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());

  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_elapse_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_ts_mgr_().update_fake_gts(1);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObTxDesc *tx_ptr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
  ObTxDesc &tx2 = *tx_ptr2;

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, n2->read(snapshot, 100, val1));
  }

  ObTxNode::get_ts_mgr_().update_fake_gts(ls_tx_ctx_mgr2->max_replay_commit_version_.get_val_for_gts());
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx2,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 100, val1));
    ASSERT_EQ(113, val1);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
  }

  ObTxNode::get_ts_mgr_().elapse_callback();
  t.join();

  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, get_gts_block_and_switch_to_follower_gracefully)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();
  // tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  n1->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());

  n2->wait_all_redolog_applied();
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ObTxNode::get_ts_mgr_().get_gts_callback();
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, get_gts_block_and_switch_to_follower_forcedly)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();
// tx
  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;

  { // prepare snapshot for write
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ObTxSEQ sp1;
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  }

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ObTransID fail_tx_id;
  ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id);
  n1->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();
  t.join();
  ASSERT_EQ(OB_TRANS_KILLED, commit_ret);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ObTxNode::get_ts_mgr_().get_gts_callback();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_rollback_to_last_savepoint_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  ObStoreCtx write_store_ctx;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ls_tx_ctx_mgr->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp1, n1->ts_after_ms(100), nullptr));
  }

  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1001, val2));
    ASSERT_EQ(1113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }

  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  ObStoreCtx write_store_ctx;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
  }

  ObTxSEQ sp2;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ls_tx_ctx_mgr->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1001, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp2, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp3;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n2->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1000, val1));
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }

  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}
// distributed tx
TEST_F(ObTestTx, distributed_tx_participant_switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  GCONF._ob_trans_rpc_timeout = 5000 * 1000;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);
  LOG_INFO("n3 tx_ctx_mgr", K(&n3->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
  }

  ObTxSEQ sp2;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp2));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 2000, 1111));
  }

  ObStoreCtx write_store_ctx;
  ObTxSEQ sp3;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n2->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n2->write_one_row(write_store_ctx, 2000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ls_tx_ctx_mgr2->switch_to_follower_gracefully();
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n2->write_one_row(write_store_ctx, 2000, 1113));
  ASSERT_EQ(OB_SUCCESS, n2->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp3, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp4;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp4));
    ASSERT_EQ(OB_SUCCESS, n3->write(tx, snapshot, 2001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 2000, val1));
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 2001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_gracefully_in_stmt_then_commit)
{
  int ret = OB_SUCCESS;
  GCONF._ob_trans_rpc_timeout = 5000 * 1000;
  ObTxNode::reset_localtion_adapter();

  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  LOG_INFO("n1 tx_ctx_mgr", K(&n1->txs_.tx_ctx_mgr_));

  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  LOG_INFO("n2 tx_ctx_mgr", K(&n2->txs_.tx_ctx_mgr_));

  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);
  LOG_INFO("n3 tx_ctx_mgr", K(&n3->txs_.tx_ctx_mgr_));

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  ObTxParam tx_param;
  tx_param.timeout_us_ = 100000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxSEQ sp1;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
    ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 1000, 1111));
    ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 2000, 1111));
  }

  ObStoreCtx write_store_ctx;
  ObTxSEQ sp3;
  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp3));
    ASSERT_EQ(OB_SUCCESS, n1->write_begin(tx, snapshot, write_store_ctx));
    ASSERT_EQ(OB_SUCCESS, n1->write_one_row(write_store_ctx, 1000, 1112));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ls_tx_ctx_mgr1->switch_to_follower_gracefully();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_NOT_MASTER, n1->write_one_row(write_store_ctx, 1000, 1113));
  ASSERT_EQ(OB_SUCCESS, n1->write_end(write_store_ctx));
  {
    ASSERT_EQ(OB_SUCCESS, n1->rollback_to_implicit_savepoint(tx, sp3, n1->ts_after_ms(100), nullptr));
  }

  ObTxSEQ sp4;
  { // prepare snapshot for read
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp4));
    ASSERT_EQ(OB_SUCCESS, n3->write(tx, snapshot, 1001, 1113));
  }

  {
    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
    int64_t val1 = 0, val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 1000, val1));
    ASSERT_EQ(OB_SUCCESS, n3->read(snapshot, 1001, val2));
    ASSERT_EQ(1111, val1);
    ASSERT_EQ(1113, val2);
  }

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  {
    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
  }
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
}

TEST_F(ObTestTx, distributed_tx_switch_to_follower_forcedly_in_prepare_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));

  ObTxNode::get_ts_mgr_().set_get_gts_waiting_mode();

  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PREPARE) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 100);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_follower_forcedly());
  n2->wait_all_redolog_applied();

  ObTxNode::get_ts_mgr_().clear_get_gts_waiting_mode();
  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);

  ObTxNode::get_ts_mgr_().get_gts_callback();
  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ReplayLogEntryFunctor functor_n2(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor_n2));
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_prepare_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 101, 113));

  n1->add_drop_msg_type(TX_2PC_PREPARE_RESP);

  int commit_ret = OB_SUCCESS;
  // async start commit
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  // wait coordinator into prepare state
  ObPartTransCtx *n1_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, n1_ctx));
  int i = 0;
  while(!(n1_ctx->exec_info_.state_ == ObTxState::PREPARE) && i++ < 1000) {
    usleep(5000);
  }
  ASSERT_NE(i, 1001);
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(n1_ctx));

  // switch coordinator to follower forcedly
  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();

  // n3 takeover as leader
  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));
  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));
  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  // wait commit complete on scheduler
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ReplayLogEntryFunctor functor_n1(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(functor_n1));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_participant_switch_to_follower_forcedly_in_pre_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());

  ASSERT_EQ(OB_SUCCESS, n2->start());
  n3->set_as_follower_replica(*n2);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500 * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n2->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n2->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx, n2->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx));
    }
    n2->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 112));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n2->add_drop_msg_type(TX_2PC_COMMIT_REQ);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(101, i);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_follower_forcedly());
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));

  n2->del_drop_msg_type(TX_2PC_COMMIT_REQ);
  n3->add_drop_msg_type(TX_2PC_COMMIT_REQ);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 200, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_CLEAR_REQ);
  n3->del_drop_msg_type(TX_2PC_COMMIT_REQ);
  n3->del_drop_msg_type(TX_2PC_CLEAR_REQ);

  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(0, ls_tx_ctx_mgr3->get_tx_ctx_count());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_pre_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500ll * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n1->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  n3->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  int i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));


  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 100, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_COMMIT_RESP);
  n3->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);
  n3->del_drop_msg_type(TX_2PC_COMMIT_RESP);

  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));

  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, distributed_tx_coordinator_switch_to_follower_forcedly_in_participant_commit_state)
{
  int ret = OB_SUCCESS;
  ObTxNode::reset_localtion_adapter();
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_6_1_0);

  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  auto n3 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888), bus_);

  DEFER(delete(n1));
  DEFER(delete(n2));
  DEFER(delete(n3));

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());

  n3->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n3->start());

  ObTxParam tx_param;
  tx_param.timeout_us_ = 500ll * 1000 * 1000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;

  {
    ObTxDesc *tx_ptr = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
    ObTxDesc &tx = *tx_ptr;

    {
      ObTxReadSnapshot snapshot;
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                               tx_param.isolation_,
                               n1->ts_after_ms(100),
                               snapshot));
      ObTxSEQ sp1;
      ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
      ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 112));
    }
    {
      ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));
      ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
    }
    n1->wait_all_redolog_applied();
  }

  ObTxDesc *tx_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr));
  ObTxDesc &tx = *tx_ptr;
  // prepare snapshot for write
  ObTxReadSnapshot snapshot;
  {
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx,
                             tx_param.isolation_,
                             n1->ts_after_ms(100),
                             snapshot));
  }
  ObTxSEQ sp1;
  ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp1));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 113));
  ASSERT_EQ(OB_SUCCESS, n2->write(tx, snapshot, 200, 113));

  n1->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  n3->add_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  int commit_ret = OB_SUCCESS;
  std::thread t(do_async_commit, n1, std::ref(tx), std::ref(commit_ret));
  usleep(100 * 1000);

  ObPartTransCtx *n1_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, n1_ctx));
  int i = 0;
  while(!(n1_ctx->exec_info_.state_ == ObTxState::PRE_COMMIT) && i++ < 100) {
    usleep(5000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(n1_ctx));

  // Keep the coordinator's COMMIT log unsubmitted. set_log_drop() reports
  // success without ever completing its callback, violating the callback drain
  // required before a forced role switch.
  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_COMMIT_LOG);
  n1->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);

  ObPartTransCtx *n2_ctx = NULL;
  ASSERT_EQ(OB_SUCCESS, n2->get_tx_ctx(n2->ls_id_, tx.tx_id_, n2_ctx));
  i = 0;
  while(!(n2_ctx->exec_info_.state_ == ObTxState::COMMIT) && i++ < 100) {
    usleep(50000);
  }
  ASSERT_NE(i, 101);
  ASSERT_EQ(OB_SUCCESS, n2->revert_tx_ctx(n2_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  n1->fake_tx_log_adapter_->clear_block_log_type();

  ReplayLogEntryFunctor functor(n3);
  ASSERT_EQ(OB_SUCCESS, n3->fake_tx_log_adapter_->replay_all(functor));

  ObLSTxCtxMgr *ls_tx_ctx_mgr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n3->ls_id_, ls_tx_ctx_mgr3));

  ObTxNode::get_location_adapter_().update_localtion(n3->ls_id_, n3->addr_);

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr3->switch_to_leader());
  n3->wait_all_redolog_applied();

  LOG_INFO("max_commit_ts after switch_to_leader",
           K(n3->txs_.tx_version_mgr_.get_max_commit_ts(false)));

  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n3->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;
  int64_t val1 = 0;
  ObTxReadSnapshot snapshot3;
  {
    ASSERT_EQ(OB_SUCCESS, n3->get_read_snapshot(tx3,
                             tx_param.isolation_,
                             n3->ts_after_ms(100),
                             snapshot3));
  }
  std::thread t_read(do_async_read, n3, std::ref(snapshot3), 100, std::ref(val1));
  usleep(100 * 1000);
  n3->add_drop_msg_type(TX_2PC_COMMIT_RESP);
  n3->del_drop_msg_type(TX_2PC_PRE_COMMIT_RESP);
  t.join();
  ASSERT_EQ(OB_SUCCESS, commit_ret);

  n3->del_drop_msg_type(TX_2PC_COMMIT_RESP);

  t_read.join();
  ASSERT_EQ(113, val1);
  ASSERT_EQ(OB_SUCCESS, n3->release_tx(tx3));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx));
  ASSERT_EQ(OB_SUCCESS, n3->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n3->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr3));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestTx, interrupt_get_read_snapshot)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  ObTxReadSnapshot snapshot;
  n1->get_ts_mgr_().inject_get_gts_error(OB_EAGAIN);
  int ret = OB_SUCCESS;
  do {
    ASYNC_DO(acq_snapshot, n1->get_read_snapshot(tx, ObTxIsolationLevel::RC, n1->ts_after_ms(20 * 1000), snapshot));
    ASSERT_EQ(OB_SUCCESS, n1->interrupt(tx, OB_TRANS_KILLED));
    ASYNC_WAIT(acq_snapshot, 2000 * 1000, wait_ret);
    ret = wait_ret;
  } while (OB_GTS_NOT_READY == ret);
  ASSERT_EQ(OB_ERR_INTERRUPTED, ret);
  ROLLBACK_TX(n1, tx);
}

TEST_F(ObTestTx, rollback_with_branch_savepoint)
{
  START_ONE_TX_NODE(n1);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, global_sp1);
  CREATE_BRANCH_SAVEPOINT(n1, tx, 100, sp_b100_1);
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 100, 111, 100));
  CREATE_BRANCH_SAVEPOINT(n1, tx, 200, sp_b200_1);
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 200, 211, 200));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 101, 112, 100));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 500, 505)); // global write
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 201, 212, 200));
  // rollback branch 200
  ASSERT_EQ(OB_SUCCESS, ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp_b200_1, 2000*1000));
  // check branch 100 is readable
  int64_t val = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 101, val));
  ASSERT_EQ(val, 112);
  // check global write is readable
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 500, val));
  ASSERT_EQ(val, 505);
  // check branch 200 is un-readable
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 200, val));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 201, val));
  // write with branch 200
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 206, 602, 200));
  // rollback branch 100
  ASSERT_EQ(OB_SUCCESS, ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp_b100_1, 2000*1000));
  // check global write is readable
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 500, val));
  ASSERT_EQ(val, 505);
  // check branch 200 is readable
  ASSERT_EQ(OB_SUCCESS, n1->read(tx, 206, val));
  ASSERT_EQ(val, 602);
  // check branch 100 is un-readable
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 100, val));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 101, val));
  // rollback global
  ASSERT_EQ(OB_SUCCESS, ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, global_sp1, 2000 * 1000));
  // check global and branch 200 is un-readable
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 500, val));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, n1->read(tx, 206, val));
  ROLLBACK_TX(n1, tx);
}


#define TEST_MARK_ABORT_AND_COMMIT(FLG)                         \
  TEST_F(ObTestTx, commit_tx_sanity_check_flag_ ## FLG)         \
  {                                                             \
    START_ONE_TX_NODE(n1);                                      \
    PREPARE_TX(n1, tx);                                         \
    PREPARE_TX_PARAM(tx_param);                                 \
    CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, global_sp1);    \
    ASSERT_EQ(n1->write(tx, 1, 1), OB_SUCCESS);                 \
    ASSERT_EQ(tx.state_, ObTxDesc::State::IMPLICIT_ACTIVE);     \
    tx.flags_.FLG = true;                                       \
    const int commit_ret = COMMIT_TX(n1, tx, 50000);            \
    EXPECT_EQ(commit_ret, OB_TRANS_ROLLBACKED);                 \
  }
TEST_MARK_ABORT_AND_COMMIT(PART_ABORTED_)
TEST_MARK_ABORT_AND_COMMIT(PART_EPOCH_MISMATCH_)
TEST_MARK_ABORT_AND_COMMIT(PARTS_INCOMPLETE_)
#undef _MARK_ABORT_AND_COMMIT

TEST_F(ObTestTx, participant_abort_asynchronously)
{
  START_TWO_TX_NODE(n1, n2);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, global_sp1);
  ASSERT_EQ(n1->write(tx, 1, 1), OB_SUCCESS);
  ASSERT_EQ(n2->write(tx, 2, 2), OB_SUCCESS);
  // n1 switch to follower forcedly, then switch to leader
  FLUSH_REDO(n1);
  n1->wait_tx_log_synced();
  SWITCH_TO_FOLLOWER_FORCEDLY(n1);
  SWITCH_TO_LEADER(n1);
  // check received participant aborted notify from n1
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(tx.flags_.PART_ABORTED_);
  ASSERT_EQ(tx.abort_cause_, ObTxAbortCause::PARTICIPANT_SWITCH_LEADER_DATA_INCOMPLETE);
  share::ObLSArray extra_touched_ls;
  extra_touched_ls.push_back(ObLSID(111));
  ASSERT_EQ(ROLLBACK_TO_IMPLICIT_SAVEPOINT_X(n1, tx, global_sp1, 2000, &extra_touched_ls),
            OB_TRANS_NEED_ROLLBACK);
  ASSERT_EQ(tx.state_, ObTxDesc::State::ABORTED);
  bool found_touched_ls_id_in_participant_set = false;
  for (int i = 0; i< tx.parts_.count(); i++) {
    if (tx.parts_[i].id_.id() == 111) {
      found_touched_ls_id_in_participant_set = true;
    }
  }
  ASSERT_TRUE(found_touched_ls_id_in_participant_set);
  const int commit_ret = COMMIT_TX(n1, tx, 5000);
  EXPECT_EQ(commit_ret, OB_TRANS_ROLLBACKED);
}

////
/// APPEND NEW TEST HERE, USE PRE DEFINED MACRO IN FILE `test_tx.dsl`
/// SEE EXAMPLE: TEST_F(ObTestTx, rollback_savepoint_timeout)
///

} // oceanbase

int main(int argc, char **argv)
{
  uint64_t checksum = 1100101;
  uint64_t c = 0;
  uint64_t checksum1 = ob_crc64(checksum, (void*)&c, sizeof(uint64_t));
  uint64_t checksum2 = ob_crc64(c, (void*)&checksum, sizeof(uint64_t));
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  std::string log_file_name = "test_tx.log";
  #ifdef TX_NODE_MEMTABLE_USE_HASH_INDEX_FLAG
      log_file_name = "test_tx_no_hash_index.log";
  #endif
  system(std::string("rm -rf " + log_file_name + "*").c_str());
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_file_name.c_str(), true, false,
                       log_file_name.c_str(), // rs
                       log_file_name.c_str(), // election
                       log_file_name.c_str()); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h), K(checksum1), K(checksum2));
  return RUN_ALL_TESTS();
}

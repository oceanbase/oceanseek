/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <thread>
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_local_channel.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "sql/dtl/ob_dtl_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

// Only the outgoing network boundary is replaced. EOF serialization, batching,
// incoming UNBLOCK buffers, channel queues and worker consumption are real.
class EofCaptureChannel : public ObDtlRpcChannel
{
public:
  EofCaptureChannel(uint64_t id, const ObAddr &peer)
      : ObDtlRpcChannel(1, id, peer, DtlChannelType::RPC_CHANNEL) {}
  int prepare_writer() { return init_datum_msg_writer(writer_storage_); }
  int send_message(ObDtlLinkedBuffer *&buf) override
  {
    int ret = OB_SUCCESS;
    if (buf == nullptr || !buf->is_batch_send()
        || buf->get_payload_buffers().count() != buf->get_payload_channels().count()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < buf->get_payload_buffers().count(); ++i) {
        ObDtlLinkedBuffer *payload = buf->get_payload_buffers().at(i);
        EofCaptureChannel *owner =
            static_cast<EofCaptureChannel *>(buf->get_payload_channels().at(i));
        if (payload == nullptr || owner == nullptr || !payload->is_eof()
            || !payload->is_data_msg() || payload->size() <= 0) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ++owner->eof_payloads_;
        }
      }
      ++batch_calls_;
      if (OB_SUCC(ret)) {
        ret = send_ret_;
      }
    }
    return ret;
  }
  int batch_calls_{0};
  int eof_payloads_{0};
  int send_ret_{OB_SUCCESS};
private:
  alignas(ObDtlDatumMsgWriter) char writer_storage_[sizeof(ObDtlDatumMsgWriter)];
};

class BoundedEofSender : public ObTransmitEofAsynSender
{
public:
  BoundedEofSender(ObIArray<ObDtlChannel *> &channels,
      const ObIArray<ObDtlServerChannelGroup *> *groups, int64_t deadline, ObEvalCtx &eval_ctx)
      : ObTransmitEofAsynSender(channels, nullptr, true, deadline, eval_ctx,
          ObDtlMsgType::PX_DATUM_ROW, groups) {}
  int fill_batch_msg(ObDtlPeerCtlBatch &slot) override
  {
    // Allow the production fast_check_status() sampling interval to elapse.
    if (++rounds_ > 1024) {
      watchdog_fired_ = true;
      return OB_CANCELED;
    }
    return ObTransmitEofAsynSender::fill_batch_msg(slot);
  }
  int rounds_{0};
  bool watchdog_fired_{false};
};

class TestDtlBatchUnblockRace : public ::testing::Test
{
protected:
  TestDtlBatchUnblockRace()
      : exec_ctx_(allocator_), eval_ctx_(exec_ctx_), tenant_(1), tenant_dfc_(1), unblock_proc_(dfc_) {}
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, DTL.init());
    ASSERT_EQ(OB_SUCCESS, oceanbase::omt::ObTenantConfigMgr::get_instance().add_tenant_config(1));
    // Standalone unittests have no observer to provision the tenant's DTL budget.
    oceanbase::lib::set_tenant_memory_limit(1, 1L << 30);
  }
  void SetUp() override
  {
    previous_tenant_ = ObTenantEnv::get_tenant();
    tenant_.set(&tenant_dfc_);
    ObTenantEnv::set_tenant(&tenant_);
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, &allocator_));
    ASSERT_EQ(OB_SUCCESS, session_.set_session_state(QUERY_ACTIVE));
    exec_ctx_.set_my_session(&session_);
    ASSERT_EQ(OB_SUCCESS, exec_ctx_.create_physical_plan_ctx());
    exec_ctx_.get_physical_plan_ctx()->set_timeout_timestamp(ObTimeUtility::current_time() + 5000000);
    ObTenantDfc *tenant_dfc = &tenant_dfc_;
    ASSERT_EQ(OB_SUCCESS, ObTenantDfc::mtl_init(tenant_dfc));
    peer_.set_ip_addr("127.0.0.2", 2882);
    dfc_.set_dtl_channel_watcher(&loop_);
    ASSERT_EQ(OB_SUCCESS, dfc_.init(1, 4));
    dfc_.set_transmit();
    dfc_.set_exec_ctx(exec_ctx_);
    dfc_.set_timeout_ts(ObTimeUtility::current_time() + 5000000);
    loop_.register_processor(unblock_proc_);
  }
  void TearDown() override
  {
    for (int64_t i = 0; i < owned_channels_.count(); ++i) {
      ObDtlChannel *ch = owned_channels_.at(i);
      ObDtlChannel *removed = nullptr;
      EXPECT_EQ(OB_SUCCESS, DTL.remove_channel(ch->get_id(), removed));
      EXPECT_EQ(ch, removed);
      if (ch->get_dfc() != nullptr) {
        EXPECT_EQ(OB_SUCCESS, dfc_.unregister_channel(ch));
        EXPECT_EQ(OB_SUCCESS, loop_.unregister_channel(*ch));
      }
      static_cast<ObDtlBasicChannel *>(ch)->ObDtlBasicChannel::destroy();
      EXPECT_EQ(ch->get_alloc_buffer_cnt(), ch->get_free_buffer_cnt());
      delete ch;
    }
    tenant_dfc_.get_tenant_mem_manager()->destroy();
    tenant_.set(static_cast<ObTenantDfc *>(nullptr));
    ObTenantEnv::set_tenant(previous_tenant_);
  }
  ObDtlBasicChannel *add_channel(bool local, uint64_t id, bool transmit = true)
  {
    ObDtlChannel *ch = local
        ? static_cast<ObDtlChannel *>(new ObDtlLocalChannel(
            1, id, peer_, ObDtlChannel::DtlChannelType::LOCAL_CHANNEL))
        : static_cast<ObDtlChannel *>(new EofCaptureChannel(id, peer_));
    EXPECT_EQ(OB_SUCCESS, owned_channels_.push_back(ch));
    const int ret = local ? DTL.create_local_channel(1, id, peer_, ch, nullptr)
                          : DTL.create_rpc_channel(1, id, peer_, ch, nullptr);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (transmit) {
      EXPECT_EQ(OB_SUCCESS, dfc_.register_channel(ch));
    }
    if (!local) {
      EXPECT_EQ(OB_SUCCESS, static_cast<EofCaptureChannel *>(ch)->prepare_writer());
    }
    return static_cast<ObDtlBasicChannel *>(ch);
  }
  void deliver_unblock(ObDtlBasicChannel *ch, ObDtlBasicChannel *local_sender = nullptr)
  {
    int rpc_ret = OB_ERR_UNEXPECTED;
    std::thread rpc_thread([&] {
      ObTenantEnv::set_tenant(&tenant_);
      if (local_sender == nullptr) {
        ObDtlBatchMsg msg;
        msg.contained_msg_type_ = ObDtlMsgType::UNBLOCKING_DATA_FLOW;
        rpc_ret = msg.ch_ids_.push_back(ch->get_id());
        if (rpc_ret == OB_SUCCESS) {
          rpc_ret = ObDtlBatchSendMessageP::process_batch_control_msg(msg);
        }
      } else {
        ObSEArray<ObDtlChannel *, 1> channels;
        ObDfcUnblockAsynSender sender(
            channels, nullptr, false, dfc_.get_timeout_ts(), dfc_, nullptr, exec_ctx_);
        ObDtlChannel *peer = nullptr;
        rpc_ret = DTL.get_channel(ch->get_id(), peer);
        if (rpc_ret == OB_SUCCESS) {
          rpc_ret = sender.process_local(local_sender, peer);
        }
        if (peer != nullptr) {
          DTL.release_channel(peer);
        }
      }
      ObTenantEnv::set_tenant(nullptr);
    });
    rpc_thread.join();
    ASSERT_EQ(OB_SUCCESS, rpc_ret);
  }
  // before/after use the real block_channel(). The middle case deterministically
  // stages its two public state updates; no timing-dependent sleep or product hook.
  void check_ordering(bool local, int timing)
  {
    ObDtlBasicChannel *ch = add_channel(local, 42000);
    ObDtlBasicChannel *notifier = local ? add_channel(true, 42001, false) : nullptr;
    if (timing == 1) {
      dfc_.set_block(ch->get_dfc_idx());
    }
    if (timing == 2) {
      ASSERT_EQ(OB_SUCCESS, dfc_.block_channel(ch));
    }
    const bool was_dfc_blocked = dfc_.is_block(ch);
    const bool was_channel_blocked = ch->is_blocked();
    deliver_unblock(ch, notifier);
    EXPECT_EQ(was_dfc_blocked, dfc_.is_block(ch))
        << "The notification thread must not modify sender DFC";
    EXPECT_EQ(was_channel_blocked, ch->is_blocked());
    EXPECT_EQ(1, ch->get_recv_buffer_cnt()) << "UNBLOCK must reach the sender's queue";
    if (timing == 0) {
      ASSERT_EQ(OB_SUCCESS, dfc_.block_channel(ch));
    }
    if (timing == 1) {
      ch->set_blocked();
    }
    EXPECT_EQ(dfc_.is_block(ch), ch->is_blocked());
    ASSERT_EQ(OB_SUCCESS, ch->wait_unblocking_if_blocked());
    EXPECT_FALSE(dfc_.is_block(ch));
    EXPECT_FALSE(ch->is_blocked());
    EXPECT_EQ(0, dfc_.get_blocked_cnt());
  }
  void check_interrupted_batch_entry(ObDtlMsgType type)
  {
    EofCaptureChannel *ch = static_cast<EofCaptureChannel *>(add_channel(false, 42000));
    if (type == ObDtlMsgType::UNBLOCKING_DATA_FLOW) {
      ASSERT_EQ(OB_SUCCESS, dfc_.block_channel(ch));
    }
    ObSEArray<ObDtlChannel *, 1> channels;
    ObSEArray<ObDtlServerChannelGroup *, 1> groups;
    ObDtlServerChannelGroup group;
    ASSERT_EQ(OB_SUCCESS, channels.push_back(ch));
    ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(ch));
    ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
    // Prime the shared counter through its public API so the next call samples.
    // A ready-to-send batch must check before sending, not only after stalling.
    for (int64_t i = 0; i < 255; ++i) {
      ASSERT_EQ(OB_SUCCESS, exec_ctx_.fast_check_status());
    }
    ASSERT_EQ(OB_SUCCESS, session_.set_session_state(QUERY_KILLED));
    if (type == ObDtlMsgType::PX_DATUM_ROW) {
      BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
      EXPECT_EQ(OB_ERR_QUERY_INTERRUPTED, sender.async_send_batch());
      EXPECT_EQ(0, sender.rounds_);
      EXPECT_FALSE(sender.watchdog_fired_);
    } else if (type == ObDtlMsgType::UNBLOCKING_DATA_FLOW) {
      ObDfcUnblockAsynSender sender(channels, nullptr, false, dfc_.get_timeout_ts(),
          dfc_, &groups, exec_ctx_);
      EXPECT_EQ(OB_ERR_QUERY_INTERRUPTED, sender.async_send_batch());
    } else {
      ObDfcDrainAsynSender sender(channels, nullptr, false, dfc_.get_timeout_ts(),
          &groups, exec_ctx_);
      EXPECT_EQ(OB_ERR_QUERY_INTERRUPTED, sender.async_send_batch());
    }
    EXPECT_EQ(0, ch->batch_calls_);
  }
  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObTenantBase tenant_;
  ObTenantBase *previous_tenant_{nullptr};
  ObTenantDfc tenant_dfc_;
  ObAddr peer_;
  ObDtlFlowControl dfc_;
  ObDtlChannelLoop loop_;
  ObDtlUnblockingMsgP unblock_proc_;
  ObSEArray<ObDtlChannel *, 4> owned_channels_;
};

TEST_F(TestDtlBatchUnblockRace, rpc_unblock_before_block)
{
  check_ordering(false, 0);
}
TEST_F(TestDtlBatchUnblockRace, rpc_unblock_during_block)
{
  check_ordering(false, 1);
}
TEST_F(TestDtlBatchUnblockRace, rpc_unblock_after_block)
{
  check_ordering(false, 2);
}
TEST_F(TestDtlBatchUnblockRace, local_unblock_before_block)
{
  check_ordering(true, 0);
}
TEST_F(TestDtlBatchUnblockRace, local_unblock_during_block)
{
  check_ordering(true, 1);
}
TEST_F(TestDtlBatchUnblockRace, local_unblock_after_block)
{
  check_ordering(true, 2);
}

TEST_F(TestDtlBatchUnblockRace, eof_progress_after_interleaved_unblock)
{
  EofCaptureChannel *first = static_cast<EofCaptureChannel *>(add_channel(false, 42000));
  EofCaptureChannel *second = static_cast<EofCaptureChannel *>(add_channel(false, 42002));
  dfc_.set_block(first->get_dfc_idx());
  deliver_unblock(first);
  first->set_blocked();
  ObSEArray<ObDtlChannel *, 2> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(first));
  ASSERT_EQ(OB_SUCCESS, channels.push_back(second));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(first));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(second));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
  EXPECT_EQ(OB_SUCCESS, sender.asyn_send());
  EXPECT_FALSE(sender.watchdog_fired_);
  EXPECT_EQ(1, first->eof_payloads_);
  EXPECT_EQ(1, second->eof_payloads_);
  EXPECT_FALSE(first->is_blocked());
  EXPECT_FALSE(dfc_.is_block(first));
  EXPECT_EQ(nullptr, first->get_write_buffer());
  EXPECT_EQ(nullptr, second->get_write_buffer());
}

TEST_F(TestDtlBatchUnblockRace, eof_send_failure_is_propagated_and_payloads_released)
{
  EofCaptureChannel *first = static_cast<EofCaptureChannel *>(add_channel(false, 42000));
  EofCaptureChannel *second = static_cast<EofCaptureChannel *>(add_channel(false, 42002));
  first->send_ret_ = OB_RPC_CONNECT_ERROR;
  second->send_ret_ = OB_RPC_CONNECT_ERROR;
  ObSEArray<ObDtlChannel *, 2> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(first));
  ASSERT_EQ(OB_SUCCESS, channels.push_back(second));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(first));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(second));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
  EXPECT_EQ(OB_RPC_CONNECT_ERROR, sender.asyn_send());
  EXPECT_FALSE(sender.watchdog_fired_);
  EXPECT_EQ(1, first->batch_calls_ + second->batch_calls_);
  EXPECT_EQ(nullptr, first->get_write_buffer());
  EXPECT_EQ(nullptr, second->get_write_buffer());
}

TEST_F(TestDtlBatchUnblockRace, expired_eof_exits_even_with_inconsistent_block_state)
{
  ObDtlBasicChannel *ch = add_channel(false, 42000);
  // Inject the known stranded state to independently exercise the timeout guard.
  ch->set_blocked();
  exec_ctx_.get_physical_plan_ctx()->set_timeout_timestamp(ObTimeUtility::current_time() - 1000000);
  ObSEArray<ObDtlChannel *, 1> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
  EXPECT_EQ(OB_TIMEOUT, sender.async_send_batch());
  EXPECT_FALSE(sender.watchdog_fired_);
}

TEST_F(TestDtlBatchUnblockRace, canceled_eof_exits_even_with_inconsistent_block_state)
{
  ASSERT_EQ(OB_SUCCESS, session_.set_session_state(QUERY_KILLED));
  ObDtlBasicChannel *ch = add_channel(false, 42000);
  ch->set_blocked();
  ObSEArray<ObDtlChannel *, 1> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
  EXPECT_EQ(OB_ERR_QUERY_INTERRUPTED, sender.async_send_batch());
  EXPECT_FALSE(sender.watchdog_fired_);
}

TEST_F(TestDtlBatchUnblockRace, spm_timeout_exits_before_query_deadline)
{
  ObDtlBasicChannel *ch = add_channel(false, 42000);
  // Inject the known stranded state to independently exercise the timeout guard.
  ch->set_blocked();
  exec_ctx_.get_physical_plan_ctx()->set_spm_timeout_timestamp(ObTimeUtility::current_time() - 1000000);
  ObSEArray<ObDtlChannel *, 1> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(ch));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, dfc_.get_timeout_ts(), eval_ctx_);
  ASSERT_FALSE(exec_ctx_.get_physical_plan_ctx()->is_timeout());
  ASSERT_TRUE(exec_ctx_.get_physical_plan_ctx()->is_exec_timeout());
  EXPECT_EQ(OB_TIMEOUT, sender.async_send_batch());
  EXPECT_FALSE(sender.watchdog_fired_);
}

TEST_F(TestDtlBatchUnblockRace, eof_checks_status_before_sending)
{
  check_interrupted_batch_entry(ObDtlMsgType::PX_DATUM_ROW);
}

TEST_F(TestDtlBatchUnblockRace, unblock_checks_status_before_sending)
{
  check_interrupted_batch_entry(ObDtlMsgType::UNBLOCKING_DATA_FLOW);
}

TEST_F(TestDtlBatchUnblockRace, drain_checks_status_before_sending)
{
  check_interrupted_batch_entry(ObDtlMsgType::DRAIN_DATA_FLOW);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_dtl_batch_unblock_race.log", true, true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

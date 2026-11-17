/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#define private public
#include "share/ob_ddl_common.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_srv_rpc_proxy.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

namespace
{
constexpr int64_t TEST_SECOND_US = 1000000;
char test_log_path[] = "/tmp/test_ddl_autoinc_seq_retry.XXXXXX";

struct RpcReply
{
  int rpc_ret_ = OB_SUCCESS;
  int tablet_ret_ = OB_SUCCESS;
  int64_t count_ = 1;
  int64_t elapsed_us_ = 0;
};

struct LocationReply
{
  LocationReply(const ObAddr &leader, const int64_t elapsed_us = 0)
      : ret_(OB_SUCCESS), leader_(leader), elapsed_us_(elapsed_us) {}
  LocationReply(const int ret, const int64_t elapsed_us = 0)
      : ret_(ret), leader_(), elapsed_us_(elapsed_us) {}
  int ret_;
  ObAddr leader_;
  int64_t elapsed_us_;
};

struct RetryScript
{
  int64_t now_us_ = 100 * TEST_SECOND_US;
  int64_t lookup_count_ = 0;
  int64_t rpc_count_ = 0;
  int64_t sleep_count_ = 0;
  int64_t status_count_ = 0;
  std::vector<LocationReply> leaders_;
  std::vector<RpcReply> replies_;
  std::vector<int> statuses_;
  std::vector<int64_t> rpc_timeouts_;
  ObBatchSetTabletAutoincSeqArg arg_;
};

// Like the existing simple-server method substitutions, these definitions live
// only in this executable. Exercise the production retry function directly;
// replace its external dependencies, not the behavior being tested.
thread_local RetryScript *script = nullptr;
}

namespace oceanbase
{
namespace common
{
int64_t ObTimeUtility::current_time()
{
  int64_t now_us = 0;
  if (nullptr != script) {
    now_us = script->now_us_;
  } else {
    timeval tv;
    gettimeofday(&tv, nullptr);
    now_us = tv.tv_sec * TEST_SECOND_US + tv.tv_usec;
  }
  return now_us;
}
}

namespace share
{
int ObLocationService::get_leader(const int64_t cluster_id,
                                  const uint64_t tenant_id,
                                  const ObLSID &ls_id,
                                  const bool force_renew,
                                  ObAddr &leader)
{
  EXPECT_NE(nullptr, script);
  if (nullptr == script) {
    return OB_ERR_UNEXPECTED;
  }
  EXPECT_EQ(GCONF.cluster_id, cluster_id);
  EXPECT_EQ(script->arg_.tenant_id_, tenant_id);
  EXPECT_EQ(script->arg_.ls_id_, ls_id);
  EXPECT_TRUE(force_renew);
  const int64_t attempt = script->lookup_count_++;
  if (attempt >= script->leaders_.size()) {
    ADD_FAILURE() << "Unexpected leader lookup after the scripted attempts";
    return OB_ERR_UNEXPECTED;
  }
  const LocationReply &reply = script->leaders_[attempt];
  script->now_us_ += reply.elapsed_us_;
  if (OB_SUCCESS == reply.ret_) {
    leader = reply.leader_;
  }
  return reply.ret_;
}
}

namespace obrpc
{
// Replace the RPC on the actual per-call proxy to observe its target and timeout.
int ObSrvRpcProxy::set_tablet_autoinc_seq(const ObBatchSetTabletAutoincSeqArg &arg,
                                        ObBatchSetTabletAutoincSeqRes &res,
                                        const ObRpcOpts &opts)
{
  UNUSED(opts);
  EXPECT_NE(nullptr, script);
  if (nullptr == script) {
    return OB_ERR_UNEXPECTED;
  }
  EXPECT_EQ(script->arg_.tenant_id_, arg.tenant_id_);
  EXPECT_EQ(script->arg_.ls_id_, arg.ls_id_);
  EXPECT_EQ(1, arg.autoinc_params_.count());
  EXPECT_EQ(0, res.autoinc_params_.count()) << "Each attempt needs a fresh response";
  if (1 != arg.autoinc_params_.count()) {
    return OB_ERR_UNEXPECTED;
  }
  EXPECT_EQ(script->arg_.autoinc_params_.at(0).dest_tablet_id_,
            arg.autoinc_params_.at(0).dest_tablet_id_);
  EXPECT_EQ(script->arg_.autoinc_params_.at(0).autoinc_seq_, arg.autoinc_params_.at(0).autoinc_seq_);
  const int64_t attempt = script->rpc_count_++;
  if (attempt >= script->replies_.size()) {
    ADD_FAILURE() << "Unexpected RPC after the scripted attempts";
    return OB_ERR_UNEXPECTED;
  }
  // Only remote requests configure a destination on the per-call proxy.
  if (get_server().is_valid()) {
    EXPECT_GT(script->lookup_count_, 0);
    if (0 >= script->lookup_count_) {
      return OB_ERR_UNEXPECTED;
    }
    EXPECT_EQ(script->leaders_[script->lookup_count_ - 1].leader_, get_server());
    script->rpc_timeouts_.push_back(get_timeout());
  }
  const RpcReply &reply = script->replies_[attempt];
  script->now_us_ += reply.elapsed_us_;
  if (OB_SUCCESS == reply.rpc_ret_) {
    for (int64_t i = 0; i < reply.count_; ++i) {
      ObMigrateTabletAutoincSeqParam param = arg.autoinc_params_.at(0);
      param.ret_code_ = reply.tablet_ret_;
      const int push_ret = res.autoinc_params_.push_back(param);
      EXPECT_EQ(OB_SUCCESS, push_ret);
      if (OB_SUCCESS != push_ret) {
        return push_ret;
      }
    }
  }
  return reply.rpc_ret_;
}
}
}

extern "C" int usleep(useconds_t usec)
{
  if (nullptr != script) {
    EXPECT_EQ(TEST_SECOND_US, usec);
    ++script->sleep_count_;
    script->now_us_ += usec;
    return 0;
  }
  timespec ts = {usec / TEST_SECOND_US, (usec % TEST_SECOND_US) * 1000};
  return nanosleep(&ts, nullptr);
}

namespace
{
class ScriptedWorker : public lib::Worker
{
public:
  int check_status() override
  {
    const int64_t attempt = script->status_count_++;
    return attempt < script->statuses_.size() ? script->statuses_[attempt] : OB_SUCCESS;
  }
};

class TestDDLAutoincSeqRetry : public ::testing::Test
{
public:
  void SetUp() override
  {
    old_location_ = GCTX.location_service_;
    old_proxy_ = GCTX.srv_rpc_proxy_;
    old_worker_ = &THIS_WORKER;
    GCTX.location_service_ = &location_;
    GCTX.srv_rpc_proxy_ = &proxy_;
    lib::Worker::set_worker_to_thread_local(&worker_);
    script = &state_;
    state_.arg_.tenant_id_ = 1002;
    state_.arg_.ls_id_ = ObLSID(1001);
    ObMigrateTabletAutoincSeqParam param;
    param.src_tablet_id_ = ObTabletID(200001);
    param.dest_tablet_id_ = param.src_tablet_id_;
    param.autoinc_seq_ = 12345;
    ASSERT_EQ(OB_SUCCESS, state_.arg_.autoinc_params_.push_back(param));
    ASSERT_TRUE(leader_b_.set_ip_addr("127.0.0.2", 2882));
    ASSERT_TRUE(leader_c_.set_ip_addr("127.0.0.3", 2882));
    log_.open(test_log_path);
    ASSERT_TRUE(log_.is_open());
    log_.seekg(0, std::ios::end);
    ASSERT_TRUE(log_.good());
  }

  void TearDown() override
  {
    script = nullptr;
    lib::Worker::set_worker_to_thread_local(old_worker_);
    GCTX.srv_rpc_proxy_ = old_proxy_;
    GCTX.location_service_ = old_location_;
  }

  int sync_remote() { return ObDDLUtil::remote_set_tablet_autoinc_seq(state_.arg_); }

  int sync_local()
  {
    state_.arg_.tenant_id_ = MTL_ID();
    return ObDDLUtil::set_tablet_autoinc_seq(state_.arg_.ls_id_,
        state_.arg_.autoinc_params_.at(0).dest_tablet_id_,
        state_.arg_.autoinc_params_.at(0).autoinc_seq_);
  }

  // Read only this case's synchronous log output, including negative cases.
  void expect_recovery_summary(const char *leader = nullptr,
                               const int64_t attempt_count = 0,
                               const int64_t elapsed_us = 0)
  {
    std::vector<std::string> summaries;
    log_.clear();
    for (std::string line; std::getline(log_, line);) {
      if (std::string::npos != line.find("tablet sequence sync recovered on remote leader")) {
        summaries.push_back(line);
      }
    }
    ASSERT_EQ(nullptr == leader ? 0U : 1U, summaries.size());
    if (nullptr != leader) {
      const std::string &line = summaries.front();
      const std::string fields[] = {
          " INFO ", "ret=0,", "tenant_id:" + std::to_string(state_.arg_.tenant_id_) + ",",
          "ls_id:{id:1001}", "dest_tablet_id:{id:200001}", "autoinc_seq:12345,",
          "leader=\"" + std::string(leader) + "\"",
          "attempt_count=" + std::to_string(attempt_count) + ",",
          "elapsed_us=" + std::to_string(elapsed_us) + ")"};
      for (const std::string &field : fields) {
        EXPECT_NE(std::string::npos, line.find(field)) << "Missing " << field << " in " << line;
      }
    }
  }

  RetryScript state_;
  ObAddr leader_b_;
  ObAddr leader_c_;
private:
  ObLocationService location_;
  ObSrvRpcProxy proxy_;
  ScriptedWorker worker_;
  ObLocationService *old_location_ = nullptr;
  ObSrvRpcProxy *old_proxy_ = nullptr;
  lib::Worker *old_worker_ = nullptr;
  std::ifstream log_;
};

TEST_F(TestDDLAutoincSeqRetry, local_success_does_not_lookup_leader)
{
  state_.replies_ = {{OB_SUCCESS}};
  ASSERT_EQ(OB_SUCCESS, sync_local());
  EXPECT_EQ(1, state_.rpc_count_);
  EXPECT_EQ(0, state_.lookup_count_);
  EXPECT_EQ(0, state_.sleep_count_);
  expect_recovery_summary();
}

// DDLSEQ-T002-U: RPC and tablet role rejections share the same fallback contract.
TEST_F(TestDDLAutoincSeqRetry, local_not_master_falls_back)
{
  for (const RpcReply &reply : {RpcReply{OB_NOT_MASTER}, RpcReply{OB_SUCCESS, OB_NOT_MASTER}}) {
    SCOPED_TRACE(::testing::Message() << reply.rpc_ret_ << ", " << reply.tablet_ret_);
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t lookup_count = state_.lookup_count_;
    state_.leaders_.push_back(leader_b_);
    state_.replies_.push_back(reply);
    state_.replies_.push_back({OB_SUCCESS});
    ASSERT_EQ(OB_SUCCESS, sync_local());
    EXPECT_EQ(rpc_count + 2, state_.rpc_count_);
    EXPECT_EQ(lookup_count + 1, state_.lookup_count_);
    EXPECT_EQ(0, state_.sleep_count_);
    ASSERT_EQ(lookup_count + 1, state_.rpc_timeouts_.size());
    EXPECT_EQ(std::min(ObDDLUtil::get_default_ddl_rpc_timeout(), 60 * TEST_SECOND_US),
              state_.rpc_timeouts_.back());
    expect_recovery_summary("127.0.0.2:2882", 1, 0);
  }
}

// DDLSEQ-T002-E: preserve the final remote error through the public entry.
TEST_F(TestDDLAutoincSeqRetry, local_fallback_propagates_remote_error)
{
  const struct {
    RpcReply local_reply_;
    RpcReply remote_reply_;
    int expected_ret_;
  } cases[] = {
      {{OB_NOT_MASTER}, {OB_TIMEOUT}, OB_TIMEOUT},
      {{OB_SUCCESS, OB_NOT_MASTER}, {OB_SUCCESS, OB_EAGAIN}, OB_EAGAIN}};
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.expected_ret_);
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t lookup_count = state_.lookup_count_;
    state_.leaders_.push_back(leader_b_);
    state_.replies_.push_back(test_case.local_reply_);
    state_.replies_.push_back(test_case.remote_reply_);
    ASSERT_EQ(test_case.expected_ret_, sync_local());
    EXPECT_EQ(rpc_count + 2, state_.rpc_count_);
    EXPECT_EQ(lookup_count + 1, state_.lookup_count_);
    EXPECT_EQ(0, state_.sleep_count_);
    expect_recovery_summary();
  }
}

// DDLSEQ-T002-S: the 4s local rejection precedes 11s of remote discovery/RPC/waits.
// Assert the real recovery log as part of the registered CTest target.
TEST_F(TestDDLAutoincSeqRetry, local_fallback_records_recovery_summary)
{
  state_.leaders_ = {LocationReply(leader_b_, TEST_SECOND_US),
                     LocationReply(OB_LS_LOCATION_LEADER_NOT_EXIST, 2 * TEST_SECOND_US),
                     LocationReply(leader_c_, TEST_SECOND_US)};
  state_.replies_ = {{OB_NOT_MASTER, OB_SUCCESS, 0, 4 * TEST_SECOND_US},
                     {OB_NOT_MASTER, OB_SUCCESS, 0, 2 * TEST_SECOND_US},
                     {OB_SUCCESS, OB_SUCCESS, 1, 3 * TEST_SECOND_US}};
  const int64_t start_us = state_.now_us_;
  ASSERT_EQ(OB_SUCCESS, sync_local());
  EXPECT_EQ(3, state_.lookup_count_);
  EXPECT_EQ(3, state_.rpc_count_);
  EXPECT_EQ(2, state_.sleep_count_);
  EXPECT_EQ(15 * TEST_SECOND_US, state_.now_us_ - start_us);
  expect_recovery_summary("127.0.0.3:2882", 3, 11 * TEST_SECOND_US);
}

// DDLSEQ-T003-U: the RPC rejection variant is covered by retry_rpc_uses_remaining_budget.
TEST_F(TestDDLAutoincSeqRetry, refreshes_leader_after_remote_tablet_not_master)
{
  state_.leaders_ = {leader_b_, leader_c_};
  state_.replies_ = {{OB_SUCCESS, OB_NOT_MASTER}, {OB_SUCCESS}};
  ASSERT_EQ(OB_SUCCESS, sync_remote());
  EXPECT_EQ(2, state_.lookup_count_);
  EXPECT_EQ(2, state_.rpc_count_);
  EXPECT_EQ(1, state_.sleep_count_);
}

// DDLSEQ-T004-B/L: persistent RPC rejection or missing leader consumes the same budget.
TEST_F(TestDDLAutoincSeqRetry, persistent_retryable_error_exhausts_budget_and_next_call_recovers)
{
  for (const int retry_ret : {OB_NOT_MASTER, OB_LS_LOCATION_LEADER_NOT_EXIST}) {
    SCOPED_TRACE(retry_ret);
    const int64_t lookup_count = state_.lookup_count_;
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t sleep_count = state_.sleep_count_;
    const int64_t rpc_attempts = OB_NOT_MASTER == retry_ret ? 60 : 0;
    state_.leaders_.insert(state_.leaders_.end(), 60,
        OB_NOT_MASTER == retry_ret ? LocationReply(leader_b_) : LocationReply(retry_ret));
    state_.replies_.insert(state_.replies_.end(), rpc_attempts, {OB_NOT_MASTER});
    const int64_t start_us = state_.now_us_;
    ASSERT_EQ(retry_ret, sync_remote());
    EXPECT_EQ(lookup_count + 60, state_.lookup_count_);
    EXPECT_EQ(rpc_count + rpc_attempts, state_.rpc_count_);
    EXPECT_EQ(sleep_count + 59, state_.sleep_count_);
    EXPECT_EQ(59 * TEST_SECOND_US, state_.now_us_ - start_us);

    state_.leaders_.push_back(leader_c_);
    state_.replies_.push_back({OB_SUCCESS});
    ASSERT_EQ(OB_SUCCESS, sync_remote());
    EXPECT_EQ(lookup_count + 61, state_.lookup_count_);
    EXPECT_EQ(rpc_count + rpc_attempts + 1, state_.rpc_count_);
    EXPECT_EQ(sleep_count + 59, state_.sleep_count_);
  }
}

// DDLSEQ-T004-B: RPC completion at 59s, 59.5s, 60s or 61s must not add a retry sleep.
TEST_F(TestDDLAutoincSeqRetry, no_sleep_when_retry_would_exceed_budget)
{
  for (const int64_t elapsed_us : {59 * TEST_SECOND_US, 59 * TEST_SECOND_US + TEST_SECOND_US / 2,
                                  60 * TEST_SECOND_US, 61 * TEST_SECOND_US}) {
    SCOPED_TRACE(elapsed_us);
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t lookup_count = state_.lookup_count_;
    state_.leaders_.push_back(leader_b_);
    state_.replies_.push_back({OB_NOT_MASTER, OB_SUCCESS, 0, elapsed_us});
    const int64_t start_us = state_.now_us_;
    ASSERT_EQ(OB_NOT_MASTER, sync_remote());
    EXPECT_EQ(lookup_count + 1, state_.lookup_count_);
    EXPECT_EQ(rpc_count + 1, state_.rpc_count_);
    EXPECT_EQ(0, state_.sleep_count_);
    EXPECT_EQ(elapsed_us, state_.now_us_ - start_us);
  }
}

// DDLSEQ-T006-E: discovery exhausts the budget before any RPC is sent.
TEST_F(TestDDLAutoincSeqRetry, lookup_exhausting_budget_skips_rpc)
{
  for (const int64_t worker_budget_us : {2 * TEST_SECOND_US, 120 * TEST_SECOND_US}) {
    SCOPED_TRACE(worker_budget_us);
    const int64_t deadline = state_.now_us_ + worker_budget_us;
    THIS_WORKER.set_timeout_ts(deadline);
    state_.leaders_.push_back(LocationReply(
        leader_b_, std::min(60 * TEST_SECOND_US, worker_budget_us)));
    ASSERT_EQ(OB_TIMEOUT, sync_remote());
    EXPECT_EQ(0, state_.rpc_count_);
    EXPECT_EQ(0, state_.sleep_count_);
    EXPECT_EQ(deadline, THIS_WORKER.get_timeout_ts());
  }
  EXPECT_EQ(2, state_.lookup_count_);
}

// DDLSEQ-T006-L/W: both budgets must shrink across discovery, RPC and retry sleep.
TEST_F(TestDDLAutoincSeqRetry, retry_rpc_uses_remaining_budget)
{
  for (const int64_t worker_budget_us : {120 * TEST_SECOND_US, 10 * TEST_SECOND_US}) {
    SCOPED_TRACE(worker_budget_us);
    const int64_t start_us = state_.now_us_;
    const int64_t deadline = start_us + worker_budget_us;
    const int64_t lookup_elapsed_us = std::min(60 * TEST_SECOND_US, worker_budget_us) - 2 * TEST_SECOND_US;
    const int64_t rpc_index = state_.rpc_count_;
    THIS_WORKER.set_timeout_ts(deadline);
    state_.leaders_.push_back(LocationReply(leader_b_, lookup_elapsed_us));
    state_.leaders_.push_back(leader_c_);
    state_.replies_.push_back({OB_NOT_MASTER});
    state_.replies_.push_back({OB_SUCCESS});
    ASSERT_EQ(OB_SUCCESS, sync_remote());
    EXPECT_EQ(rpc_index + 2, state_.rpc_count_);
    EXPECT_EQ(lookup_elapsed_us + TEST_SECOND_US, state_.now_us_ - start_us);
    ASSERT_EQ(rpc_index + 2, state_.rpc_timeouts_.size());
    EXPECT_EQ(2 * TEST_SECOND_US, state_.rpc_timeouts_[rpc_index]);
    EXPECT_EQ(TEST_SECOND_US, state_.rpc_timeouts_.back());
    EXPECT_EQ(deadline, THIS_WORKER.get_timeout_ts());
  }
  EXPECT_EQ(4, state_.lookup_count_);
  EXPECT_EQ(2, state_.sleep_count_);
}

// DDLSEQ-T004-L: recover within this invocation instead of restarting the caller's work.
TEST_F(TestDDLAutoincSeqRetry, missing_leader_then_recovers)
{
  state_.leaders_ = {OB_LS_LOCATION_LEADER_NOT_EXIST, leader_b_};
  state_.replies_ = {{OB_SUCCESS}};
  ASSERT_EQ(OB_SUCCESS, sync_remote());
  EXPECT_EQ(2, state_.lookup_count_);
  EXPECT_EQ(1, state_.rpc_count_);
  EXPECT_EQ(1, state_.sleep_count_);
}

// DDLSEQ-T004-E: terminal RPC/tablet errors retain their original return codes.
TEST_F(TestDDLAutoincSeqRetry, remote_terminal_error_is_returned_without_retry)
{
  const struct {
    RpcReply reply_;
    int expected_ret_;
  } cases[] = {{{OB_TIMEOUT}, OB_TIMEOUT}, {{OB_SUCCESS, OB_EAGAIN}, OB_EAGAIN}};
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.expected_ret_);
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t lookup_count = state_.lookup_count_;
    state_.leaders_.push_back(leader_b_);
    state_.replies_.push_back(test_case.reply_);
    ASSERT_EQ(test_case.expected_ret_, sync_remote());
    EXPECT_EQ(lookup_count + 1, state_.lookup_count_);
    EXPECT_EQ(rpc_count + 1, state_.rpc_count_);
    EXPECT_EQ(0, state_.sleep_count_);
  }
}

TEST_F(TestDDLAutoincSeqRetry, invalid_response_count_is_not_success)
{
  for (const int64_t count : {0, 2}) {
    SCOPED_TRACE(count);
    const int64_t rpc_count = state_.rpc_count_;
    const int64_t lookup_count = state_.lookup_count_;
    state_.leaders_.push_back(leader_b_);
    state_.replies_.push_back({OB_SUCCESS, OB_SUCCESS, count});
    ASSERT_EQ(OB_ERR_UNEXPECTED, sync_remote());
    EXPECT_EQ(lookup_count + 1, state_.lookup_count_);
    EXPECT_EQ(rpc_count + 1, state_.rpc_count_);
    EXPECT_EQ(0, state_.sleep_count_);
  }
}

// DDLSEQ-T004-C: worker errors must stop before lookup, even for retryable error codes.
TEST_F(TestDDLAutoincSeqRetry, worker_error_never_starts_an_attempt)
{
  for (const int stop_ret : {OB_CANCELED, OB_NOT_MASTER, OB_LS_LOCATION_LEADER_NOT_EXIST}) {
    SCOPED_TRACE(stop_ret);
    const int64_t status_count = state_.status_count_;
    state_.statuses_.push_back(stop_ret);
    ASSERT_EQ(stop_ret, sync_remote());
    EXPECT_EQ(status_count + 1, state_.status_count_);
    EXPECT_EQ(0, state_.lookup_count_);
    EXPECT_EQ(0, state_.rpc_count_);
    EXPECT_EQ(0, state_.sleep_count_);
  }
}

TEST_F(TestDDLAutoincSeqRetry, cancellation_after_rejection_prevents_next_attempt)
{
  state_.leaders_ = {leader_b_};
  state_.replies_ = {{OB_NOT_MASTER}};
  state_.statuses_ = {OB_SUCCESS, OB_CANCELED};
  ASSERT_EQ(OB_CANCELED, sync_remote());
  EXPECT_EQ(2, state_.status_count_);
  EXPECT_EQ(1, state_.lookup_count_);
  EXPECT_EQ(1, state_.rpc_count_);
  EXPECT_EQ(1, state_.sleep_count_);
}


}

int main(int argc, char **argv)
{
  const int log_fd = mkstemp(test_log_path);
  if (log_fd < 0) {
    perror("create sequence retry test log");
    return 1;
  }
  close(log_fd);
  OB_LOGGER.set_file_name(test_log_path, true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  if (0 == ret) {
    unlink(test_log_path);
  } else {
    fprintf(stderr, "Test log: %s\n", test_log_path);
  }
  return ret;
}

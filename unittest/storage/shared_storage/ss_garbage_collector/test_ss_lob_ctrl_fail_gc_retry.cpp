/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <cerrno>
#include <ctime>
#include <unistd.h>
#include "common/ob_timeout_ctx.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_share_util.h"
#include "share/rc/ob_tenant_base.h"
#define private public
#define protected public
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;

namespace
{
int real_sleep(const int64_t duration_us)
{
  timespec duration = {static_cast<time_t>(duration_us / 1000000),
      static_cast<long>(duration_us % 1000000) * 1000};
  int ret = 0;
  while (0 != (ret = nanosleep(&duration, &duration)) && EINTR == errno) {
  }
  return ret;
}

enum ExpirePhase { EXPIRE_NONE, EXPIRE_DELETE, EXPIRE_COMMIT, EXPIRE_ROLLBACK };

struct LobCtrlRetryState
{
  int start_ret_ = OB_SUCCESS;
  int delete_ret_ = OB_SUCCESS;
  int commit_ret_ = OB_SUCCESS;
  int rollback_ret_ = OB_SUCCESS;
  int64_t start_error_count_ = INT64_MAX;
  int64_t delete_error_count_ = INT64_MAX;
  int64_t commit_error_count_ = INT64_MAX;
  int64_t primary_check_count_ = INT64_MAX;
  int64_t role_check_count_ = 0;
  ExpirePhase expire_phase_ = EXPIRE_NONE;
  int64_t start_count_ = 0;
  int64_t delete_count_ = 0;
  int64_t commit_count_ = 0;
  int64_t rollback_count_ = 0;
  int64_t sleep_count_ = 0;
  int64_t deadline_ = -1;
  int64_t first_remaining_us_ = -1;
  int64_t row_budget_us_ = 3 * 1000 * 1000;
  uint64_t expected_hash_ = 0;
  int64_t now_us_ = 0;
  ObSEArray<int64_t, 3> start_remaining_us_;
  bool row_exists_ = true;
  bool pending_delete_ = false;
  bool apply_commit_on_error_ = false;
  int64_t applied_delete_count_ = 0;
  int64_t committed_delete_count_ = 0;

  void record_deadline()
  {
    const ObTimeoutCtx &timeout_ctx = ObTimeoutCtx::get_ctx();
    if (deadline_ < 0) {
      deadline_ = timeout_ctx.get_abs_timeout();
      first_remaining_us_ = timeout_ctx.get_timeout();
      EXPECT_GT(deadline_, 0);
      EXPECT_GT(first_remaining_us_, 0);
      EXPECT_LE(first_remaining_us_, row_budget_us_);
    }
    EXPECT_EQ(deadline_, timeout_ctx.get_abs_timeout());
  }

  void expire_at(const ExpirePhase phase)
  {
    if (phase == expire_phase_) {
      // Cross the real deadline without relying on a wall-clock upper-bound assertion.
      const int64_t remaining_us = deadline_ - ObTimeUtility::current_time() + 1000;
      if (remaining_us > 0) {
        EXPECT_EQ(0, real_sleep(remaining_us));
      }
      EXPECT_TRUE(ObTimeoutCtx::get_ctx().is_timeouted());
      record_deadline();
    }
  }
};

thread_local LobCtrlRetryState *active_retry_state = nullptr;
} // namespace

namespace common
{
int64_t ObTimeUtility::current_time()
{
  int64_t now_us = 0;
  if (nullptr != active_retry_state && active_retry_state->now_us_ > 0) {
    now_us = active_retry_state->now_us_;
  } else {
    timeval now = {};
    if (0 != gettimeofday(&now, nullptr)) {
      abort();
    }
    now_us = static_cast<int64_t>(now.tv_sec) * 1000000 + now.tv_usec;
  }
  return now_us;
}

// Override only this executable's SQL boundaries; the production retry loop stays intact.
int ObMySQLTransaction::start(ObISQLClient *proxy, const uint64_t tenant_id,
    bool with_snapshot, const int32_t group_id)
{
  UNUSEDx(proxy, with_snapshot, group_id);
  if (nullptr == active_retry_state) {
    return OB_NOT_INIT;
  }
  EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
  EXPECT_FALSE(in_trans_);
  EXPECT_EQ(nullptr, get_connection());
  EXPECT_EQ(active_retry_state->start_count_ + 1, active_retry_state->role_check_count_);
  ++active_retry_state->start_count_;
  active_retry_state->record_deadline();
  EXPECT_EQ(OB_SUCCESS,
      active_retry_state->start_remaining_us_.push_back(ObTimeoutCtx::get_ctx().get_timeout()));
  const int ret = (active_retry_state->start_count_ <= active_retry_state->start_error_count_)
      ? active_retry_state->start_ret_ : OB_SUCCESS;
  in_trans_ = OB_SUCCESS == ret;
  return ret;
}

int ObMySQLTransaction::end(const bool commit)
{
  int ret = OB_NOT_INIT;
  if (nullptr != active_retry_state) {
    EXPECT_TRUE(in_trans_);
    active_retry_state->record_deadline();
    if (commit) {
      ++active_retry_state->commit_count_;
      active_retry_state->expire_at(EXPIRE_COMMIT);
      ret = (active_retry_state->commit_count_ <= active_retry_state->commit_error_count_)
          ? active_retry_state->commit_ret_ : OB_SUCCESS;
      if (OB_SUCCESS == ret || active_retry_state->apply_commit_on_error_) {
        EXPECT_TRUE(active_retry_state->pending_delete_);
        active_retry_state->row_exists_ = false;
        ++active_retry_state->committed_delete_count_;
      }
    } else {
      ++active_retry_state->rollback_count_;
      active_retry_state->expire_at(EXPIRE_ROLLBACK);
      ret = active_retry_state->rollback_ret_;
    }
    active_retry_state->pending_delete_ = false;
  }
  // The real end() clears transaction ownership even when completion returns an error.
  in_trans_ = false;
  close();
  return ret;
}
} // namespace common

namespace share
{
int ObShareUtil::mtl_check_if_tenant_role_is_primary(const uint64_t tenant_id, bool &is_primary)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
  is_primary = true;
  if (nullptr != active_retry_state) {
    ++active_retry_state->role_check_count_;
    is_primary = active_retry_state->role_check_count_ <= active_retry_state->primary_check_count_;
  }
  return OB_SUCCESS;
}
} // namespace share

namespace storage
{
int ObOuttableLobControlManager::delete_fail_row(ObMySQLTransaction &trans,
    const blocksstable::MacroBlockId &macro_id, const uint64_t hash)
{
  if (nullptr == active_retry_state) {
    return OB_NOT_INIT;
  }
  EXPECT_TRUE(trans.is_started());
  EXPECT_TRUE(macro_id.is_valid());
  EXPECT_EQ(active_retry_state->expected_hash_, hash);
  ++active_retry_state->delete_count_;
  active_retry_state->record_deadline();
  active_retry_state->expire_at(EXPIRE_DELETE);
  int ret = (active_retry_state->delete_count_ <= active_retry_state->delete_error_count_)
      ? active_retry_state->delete_ret_ : OB_SUCCESS;
  // Model the row state without replacing the production retry/error handling.
  if (!active_retry_state->row_exists_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_SUCCESS == ret) {
    active_retry_state->pending_delete_ = true;
    ++active_retry_state->applied_delete_count_;
  }
  return ret;
}

class TestSSLobCtrlFailGCRetry : public testing::Test
{
public:
  TestSSLobCtrlFailGCRetry() : tenant_(OB_SYS_TENANT_ID), previous_tenant_(nullptr) {}

protected:
  void SetUp() override
  {
    previous_tenant_ = share::ObTenantEnv::get_tenant();
    scheduler_.set_stop(false);
    tenant_.set(&scheduler_);
    share::ObTenantEnv::set_tenant(&tenant_);
    task_.set_dag(dag_);
    ctx_.tenant_id_ = OB_SYS_TENANT_ID;
    ctx_.sql_timeout_us_ = 60L * 1000 * 1000;
    row_.key_.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20261116, 0, 0, 0);
    row_.key_.hash_ = 1;
    row_.status_ = OUTTABLE_LOB_CONTROL_FAIL;
    active_retry_state = &state_;
  }

  void TearDown() override
  {
    active_retry_state = nullptr;
    share::ObTenantEnv::set_tenant(previous_tenant_);
  }

  int run_row()
  {
    state_.expected_hash_ = row_.key_.hash_;
    state_.row_budget_us_ = MIN(ctx_.sql_timeout_us_, 3L * 1000 * 1000);
    return task_.delete_expired_fail_row_(ctx_, manager_, row_, continue_scan_);
  }

  void expect_calls(const int64_t starts, const int64_t deletes, const int64_t commits,
      const int64_t rollbacks, const int64_t sleeps)
  {
    EXPECT_EQ(starts, state_.start_count_);
    EXPECT_EQ(deletes, state_.delete_count_);
    EXPECT_EQ(commits, state_.commit_count_);
    EXPECT_EQ(rollbacks, state_.rollback_count_);
    EXPECT_EQ(sleeps, state_.sleep_count_);
    EXPECT_TRUE(continue_scan_);
  }

  void expect_next_row_success()
  {
    // Keep the caller's continuation flag and context, but give the next key a fresh row budget.
    ++row_.key_.hash_;
    state_ = LobCtrlRetryState();
    ASSERT_EQ(OB_SUCCESS, run_row());
    expect_calls(1, 1, 1, 0, 0);
    EXPECT_FALSE(state_.row_exists_);
    EXPECT_EQ(1, state_.committed_delete_count_);
  }

  share::ObTenantBase tenant_;
  share::ObTenantBase *previous_tenant_;
  share::ObTenantDagScheduler scheduler_;
  ObSSFailGCDagCtx ctx_;
  ObSSFailGCDag dag_;
  ObSSLobCtrlFailGCTask task_;
  ObOuttableLobControlManager manager_;
  ObOuttableLobControlRow row_;
  LobCtrlRetryState state_;
  bool continue_scan_ = true;
};

TEST_F(TestSSLobCtrlFailGCRetry, non_primary_tenant_does_not_start_transaction)
{
  state_.primary_check_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, run_row());
  EXPECT_FALSE(continue_scan_);
  EXPECT_EQ(1, state_.role_check_count_);
  EXPECT_EQ(0, state_.start_count_);
  EXPECT_EQ(0, state_.delete_count_);
  EXPECT_EQ(0, state_.commit_count_);
  EXPECT_EQ(0, state_.rollback_count_);
  EXPECT_EQ(0, state_.sleep_count_);
  EXPECT_TRUE(state_.row_exists_);
}

TEST_F(TestSSLobCtrlFailGCRetry, role_change_stops_before_retry_transaction)
{
  state_.primary_check_count_ = 1;
  state_.delete_ret_ = OB_EAGAIN;
  ASSERT_EQ(OB_SUCCESS, run_row());
  EXPECT_FALSE(continue_scan_);
  EXPECT_EQ(2, state_.role_check_count_);
  EXPECT_EQ(1, state_.start_count_);
  EXPECT_EQ(1, state_.delete_count_);
  EXPECT_EQ(0, state_.commit_count_);
  EXPECT_EQ(1, state_.rollback_count_);
  EXPECT_EQ(1, state_.sleep_count_);
  EXPECT_TRUE(state_.row_exists_);
}

TEST_F(TestSSLobCtrlFailGCRetry, nowait_conflicts_do_not_retry_or_sleep)
{
  const int errors[] = {OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT,
      OB_ERR_EXCLUSIVE_LOCK_CONFLICT, OB_TRY_LOCK_ROW_CONFLICT};
  for (const int error : errors) {
    state_ = LobCtrlRetryState();
    state_.delete_ret_ = error;
    ASSERT_EQ(OB_SUCCESS, run_row());
    expect_calls(1, 1, 0, 1, 0);
    expect_next_row_success();
  }
}

TEST_F(TestSSLobCtrlFailGCRetry, persistent_delete_error_stops_after_two_retries)
{
  state_.delete_ret_ = OB_EAGAIN;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 3, 0, 3, 2);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, persistent_commit_error_stops_after_two_retries)
{
  state_.commit_ret_ = OB_TRANS_TIMEOUT;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 3, 3, 0, 2);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, rollback_error_preserves_retryable_delete_error)
{
  state_.delete_ret_ = OB_EAGAIN;
  state_.rollback_ret_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 3, 0, 3, 2);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, rollback_timeout_does_not_retry_nowait_conflict)
{
  state_.delete_ret_ = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
  state_.rollback_ret_ = OB_TRANS_TIMEOUT;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(1, 1, 0, 1, 0);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, short_sql_timeout_bounds_the_row_deadline)
{
  ctx_.sql_timeout_us_ = 50 * 1000;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(1, 1, 1, 0, 0);
  EXPECT_LE(state_.first_remaining_us_, ctx_.sql_timeout_us_);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, delete_failure_after_deadline_does_not_retry)
{
  ctx_.sql_timeout_us_ = 50 * 1000;
  state_.delete_ret_ = OB_EAGAIN;
  state_.expire_phase_ = EXPIRE_DELETE;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(1, 1, 0, 1, 0);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, commit_failure_after_deadline_does_not_retry)
{
  ctx_.sql_timeout_us_ = 50 * 1000;
  state_.commit_ret_ = OB_TRANS_TIMEOUT;
  state_.expire_phase_ = EXPIRE_COMMIT;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(1, 1, 1, 0, 0);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, rollback_failure_after_deadline_does_not_retry)
{
  ctx_.sql_timeout_us_ = 50 * 1000;
  state_.delete_ret_ = OB_EAGAIN;
  state_.rollback_ret_ = OB_TRANS_TIMEOUT;
  state_.expire_phase_ = EXPIRE_ROLLBACK;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(1, 1, 0, 1, 0);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, start_failures_recover_on_third_attempt)
{
  state_.start_ret_ = OB_EAGAIN;
  state_.start_error_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 1, 1, 0, 2);
  EXPECT_FALSE(state_.row_exists_);
  EXPECT_EQ(1, state_.committed_delete_count_);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, persistent_start_error_stops_after_two_retries)
{
  state_.start_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 0, 0, 0, 2);
  EXPECT_TRUE(state_.row_exists_);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, delete_failures_recover_on_third_attempt)
{
  state_.delete_ret_ = OB_EAGAIN;
  state_.delete_error_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(3, 3, 1, 2, 2);
  EXPECT_FALSE(state_.row_exists_);
  EXPECT_EQ(1, state_.applied_delete_count_);
  EXPECT_EQ(1, state_.committed_delete_count_);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, commit_failures_recover_on_third_attempt)
{
  const int errors[] = {OB_TRANS_UNKNOWN, OB_TRANS_TIMEOUT};
  for (const int error : errors) {
    state_ = LobCtrlRetryState();
    state_.commit_ret_ = error;
    state_.commit_error_count_ = 2;
    ASSERT_EQ(OB_SUCCESS, run_row());
    expect_calls(3, 3, 3, 0, 2);
    EXPECT_FALSE(state_.row_exists_);
    EXPECT_EQ(1, state_.committed_delete_count_);
    expect_next_row_success();
  }
}

TEST_F(TestSSLobCtrlFailGCRetry, unknown_commit_rechecks_missing_row)
{
  state_.commit_ret_ = OB_TRANS_UNKNOWN;
  state_.commit_error_count_ = 1;
  // Model a committed deletion whose successful response was lost.
  state_.apply_commit_on_error_ = true;
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(2, 2, 1, 1, 1);
  EXPECT_FALSE(state_.row_exists_);
  EXPECT_EQ(1, state_.applied_delete_count_);
  EXPECT_EQ(1, state_.committed_delete_count_);
  expect_next_row_success();
}

TEST_F(TestSSLobCtrlFailGCRetry, retry_deadline_expires_before_third_start)
{
  ctx_.sql_timeout_us_ = 150 * 1000;
  state_.now_us_ = ObTimeUtility::current_time();
  state_.delete_ret_ = OB_EAGAIN;
  const ObTimeoutCtx *outer_timeout_ctx = &ObTimeoutCtx::get_ctx();
  ASSERT_EQ(OB_SUCCESS, run_row());
  expect_calls(2, 2, 0, 2, 2);
  ASSERT_EQ(2, state_.start_remaining_us_.count());
  EXPECT_GT(state_.start_remaining_us_.at(0), state_.start_remaining_us_.at(1));
  EXPECT_GT(state_.start_remaining_us_.at(1), 0);
  EXPECT_GT(state_.now_us_, state_.deadline_);
  EXPECT_EQ(outer_timeout_ctx, &ObTimeoutCtx::get_ctx());
  EXPECT_TRUE(state_.row_exists_);
  expect_next_row_success();
}
} // namespace storage
} // namespace oceanbase

extern "C" int usleep(useconds_t duration_us)
{
  oceanbase::LobCtrlRetryState *state = oceanbase::active_retry_state;
  if (nullptr == state) {
    return oceanbase::real_sleep(duration_us);
  }
  ++state->sleep_count_;
  state->record_deadline();
  EXPECT_GT(duration_us, 0U);
  EXPECT_LE(duration_us, 100000U);
  EXPECT_LE(static_cast<int64_t>(duration_us), state->first_remaining_us_);
  if (state->now_us_ > 0) {
    EXPECT_LE(static_cast<int64_t>(duration_us),
        oceanbase::common::ObTimeoutCtx::get_ctx().get_timeout());
    // Advance only this test thread's clock; do not depend on scheduler timing.
    state->now_us_ += duration_us + 1;
  }
  return 0;
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

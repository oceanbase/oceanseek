/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX TRANS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log_operator.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/incremental/ob_ls_inc_sstable_uploader.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;

namespace unittest
{
struct DliAbortGateScript
{
  DliAbortGateScript()
    : upload_ret_(OB_SUCCESS), ready_(false), upload_checks_(0),
      end_submits_(0), callback_inits_(0), submit_ret_(OB_SUCCESS), end_cb_count_(0),
      expected_rollback_(true)
  {}
  int upload_ret_;
  bool ready_;
  int64_t upload_checks_;
  int64_t end_submits_;
  int64_t callback_inits_;
  int submit_ret_;
  int64_t end_cb_count_;
  bool expected_rollback_;
  ObTxLogCb end_cbs_[8];
};

static thread_local DliAbortGateScript *gate_script = nullptr;

// Keep the real compensation and submission prechecks. Replace storage
// readiness, callback preparation and the final log operator; the END gate
// itself runs from the production implementation included below.
int record_end_submission(ObPartTransCtx &ctx,
                          ObDDLIncCommitLog &log,
                          const ObDDLIncLogBasic &key,
                          logservice::AppendCb *extra_cb,
                          SCN &scn,
                          const bool is_major,
                          const bool need_free_extra_cb)
{
  EXPECT_NE(nullptr, gate_script);
  if (nullptr == gate_script) {
    return OB_ERR_UNEXPECTED;
  }
  ++gate_script->end_submits_;
  EXPECT_EQ(gate_script->expected_rollback_, log.is_rollback());
  int ret = gate_script->submit_ret_;
  if (OB_SUCCESS == ret && gate_script->end_cb_count_ >= ARRAYSIZEOF(gate_script->end_cbs_)) {
    return OB_SIZE_OVERFLOW;
  }
  if (OB_SUCCESS == ret) {
    scn.convert_for_tx(300 + gate_script->end_cb_count_);
    ret = ctx.exec_info_.dli_batch_set_.submit_ddl_end_succ(key, scn, is_major);
    if (OB_SUCCESS == ret) {
      // Real submission leaves an END callback busy until its asynchronous sync.
      EXPECT_TRUE(ctx.busy_cbs_.add_last(&gate_script->end_cbs_[gate_script->end_cb_count_++]));
      // A successful fake submission takes the same callback ownership as the
      // real log subsystem. The test advances END sync separately.
      if (need_free_extra_cb) {
        ObDDLIncCommitClogCb *cb = static_cast<ObDDLIncCommitClogCb *>(extra_cb);
        cb->~ObDDLIncCommitClogCb();
        mtl_free(cb);
      }
    }
  }
  return ret;
}
} // namespace unittest

namespace storage
{
int ObDDLIncCommitClogCb::init(const ObLSID &ls_id, const ObDDLIncCommitLog &log)
{
  EXPECT_NE(nullptr, unittest::gate_script);
  EXPECT_TRUE(ls_id.is_valid());
  EXPECT_TRUE(log.is_valid());
  EXPECT_EQ(unittest::gate_script->expected_rollback_, log.is_rollback());
  if (nullptr == unittest::gate_script) {
    return OB_ERR_UNEXPECTED;
  }
  ++unittest::gate_script->callback_inits_;
  ls_id_ = ls_id;
  log_basic_ = log.get_log_basic();
  is_rollback_ = log.is_rollback();
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObLSIncSSTableUploader::check_inc_major_dump_ready(
    const ObDDLIncLogBasic &key, bool &ready)
{
  EXPECT_NE(nullptr, unittest::gate_script);
  EXPECT_TRUE(is_incremental_major_direct_load(key.get_direct_load_type()));
  if (nullptr == unittest::gate_script) {
    return OB_ERR_UNEXPECTED;
  }
  ++unittest::gate_script->upload_checks_;
  ready = unittest::gate_script->ready_;
  return unittest::gate_script->upload_ret_;
}
} // namespace storage

namespace unittest
{
template <typename LogType>
int record_log_operator_submission(ObTxCtxLogOperator<LogType> &op, const ObTxLogOpType type)
{
  EXPECT_EQ(ObTxLogOpType::SUBMIT, type);
  EXPECT_EQ(ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END, op.construct_arg_->ddl_log_type_);
  const auto &submit_arg = op.log_op_arg_.submit_arg_;
  EXPECT_EQ(logservice::ObReplayBarrierType::STRICT_BARRIER, submit_arg.replay_barrier_type_);
  EXPECT_EQ(op.tx_ctx_->get_trans_id().get_id(), submit_arg.replay_hint_);
  const auto &buf = op.construct_arg_->dli_buf_;
  ObDDLIncCommitLog log;
  int64_t pos = 0;
  int ret = log.deserialize(buf.submit_buf_, buf.dli_buf_size_, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  if (OB_SUCCESS == ret) {
    ret = record_end_submission(*op.tx_ctx_, log, op.construct_arg_->batch_key_,
                               submit_arg.extra_cb_, op.scn_,
                               ObTxLogType::TX_DIRECT_LOAD_INC_MAJOR_LOG == LogType::LOG_TYPE,
                               submit_arg.need_free_extra_cb_);
  }
  return ret;
}
} // namespace unittest

namespace transaction
{
template <>
int ObTxCtxLogOperator<ObTxDirectLoadIncMajorLog>::operator()(const ObTxLogOpType type)
{
  return unittest::record_log_operator_submission(*this, type);
}

template <>
int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::operator()(const ObTxLogOpType type)
{
  return unittest::record_log_operator_submission(*this, type);
}
} // namespace transaction
} // namespace oceanbase

// Make the operator test doubles visible before instantiating the real submit
// template. Do not replace submit_direct_load_inc_log_: it contains the gate.
#include "storage/tx/ob_trans_part_ctx.cpp"

namespace oceanbase
{

namespace unittest
{
class TestDliAbortGate : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ObTenantBase *tenant = ObTenantEnv::get_tenant_local();
    old_tenant_id_ = tenant->id_;
    old_ls_service_ = MTL(ObLSService *);
    tenant->id_ = 500;
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(500);
    old_server_mode_ = GCTX.startup_mode_;
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, ls_service_.ls_map_.init(tenant->id_, &ls_allocator_));
    void *buf = ls_allocator_.alloc(sizeof(ObLS));
    ASSERT_NE(nullptr, buf);
    ObLS *ls = new (buf) ObLS();
    ls->ls_meta_.ls_id_ = ObLSID(1001);
    // This lookup-only fixture never starts LS services. Use the log-only
    // teardown path, which does not unregister tablet/transaction services.
    ls->ls_meta_.replica_type_ = REPLICA_TYPE_LOGONLY;
    ASSERT_EQ(OB_SUCCESS, ls_service_.ls_map_.add_ls(*ls));
    ls_service_.tenant_id_ = tenant->id_;
    ls_service_.is_inited_ = true;
    ls_service_.is_stopped_ = true;
    tenant->set<ObLSService *>(&ls_service_);
  }

  static void TearDownTestCase()
  {
    ls_service_.is_inited_ = false;
    // Release the map's LS reference while its tenant and service are still
    // installed; each helper's temporary LSHandle has already been released.
    ls_service_.ls_map_.destroy();
    ObTenantEnv::get_tenant_local()->set<ObLSService *>(old_ls_service_);
    ObTenantEnv::get_tenant_local()->id_ = old_tenant_id_;
    GCTX.startup_mode_ = old_server_mode_;
  }

  void SetUp() override
  {
    gate_script = &script_;
    ctx_.tenant_id_ = 500;
    ctx_.ls_id_ = ObLSID(1001);
    ctx_.trans_id_ = ObTransID(777);
    ctx_.role_state_ = TxCtxRoleState::LEADER;
    ctx_.upstream_state_ = ObTxState::INIT;
  }

  void TearDown() override
  {
    ATOMIC_STORE(&ctx_.pending_write_, 0);
    // The fixture owns every fake busy callback; unlink them before destruction.
    while (nullptr != ctx_.busy_cbs_.remove_first()) {}
    ctx_.exec_info_.dli_batch_set_.destroy();
    gate_script = nullptr;
  }

  void start_batch(const bool major = true, const int64_t seq_no = 1,
                   const uint64_t tablet_id = 200001,
                   const ObDirectLoadType type = DIRECT_LOAD_INCREMENTAL_MAJOR)
  {
    ASSERT_EQ(OB_SUCCESS, key_.init(ObTabletID(tablet_id), ObTabletID(), ObTabletID(), type,
                                   ctx_.trans_id_, ObTxSEQ(seq_no, 0), 100, DATA_CURRENT_VERSION));
    SCN start_scn;
    start_scn.convert_for_tx(200 + seq_no);
    ASSERT_EQ(OB_SUCCESS, ctx_.exec_info_.dli_batch_set_.before_submit_ddl_start(key_, start_scn, major));
    ASSERT_EQ(OB_SUCCESS, ctx_.exec_info_.dli_batch_set_.submit_ddl_start_succ(key_, start_scn, major));
    ASSERT_EQ(OB_SUCCESS, ctx_.exec_info_.dli_batch_set_.sync_ddl_start_succ(key_, start_scn, major));
    ctx_.exec_info_.max_applied_log_ts_ = start_scn;
  }

  const ObTxDirectLoadIncBatchInfo *batch(const bool major = true)
  {
    return batch(key_, major);
  }

  const ObTxDirectLoadIncBatchInfo *batch(const ObDDLIncLogBasic &key, const bool major = true)
  {
    return ctx_.exec_info_.dli_batch_set_.get(ObTxDirectLoadIncBatchInfo(key, major));
  }

  ObPartTransCtx ctx_;
  ObDDLIncLogBasic key_;
  DliAbortGateScript script_;
  ObTxLogCb entry_cb_;
  static ObArenaAllocator ls_allocator_;
  static ObLSService ls_service_;
  static observer::ObServerMode old_server_mode_;
  static uint64_t old_tenant_id_;
  static ObLSService *old_ls_service_;
};

ObArenaAllocator TestDliAbortGate::ls_allocator_("DliAbortTest", OB_MALLOC_NORMAL_BLOCK_SIZE, 500);
ObLSService TestDliAbortGate::ls_service_;
observer::ObServerMode TestDliAbortGate::old_server_mode_ = observer::ObServerMode::INVALID_MODE;
uint64_t TestDliAbortGate::old_tenant_id_ = OB_INVALID_TENANT_ID;
ObLSService *TestDliAbortGate::old_ls_service_ = nullptr;

// DLI-GATE-001/002/011/012: retry until ready, submit END once, then skip readiness checks.
TEST_F(TestDliAbortGate, ready_submits_end_once_after_retry)
{
  start_batch();
  // The transaction entry must not require a transaction-wide log SCN.
  ctx_.exec_info_.max_applied_log_ts_.set_min();
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
    ASSERT_NE(nullptr, batch());
    EXPECT_TRUE(batch()->need_compensate_ddl_end());
    EXPECT_FALSE(batch()->is_ddl_end_logging());
  }
  EXPECT_EQ(3, script_.upload_checks_);
  EXPECT_EQ(0, script_.end_submits_);
  EXPECT_EQ(3, script_.callback_inits_);

  script_.ready_ = true;
  ASSERT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->is_ddl_end_logging());
  EXPECT_EQ(1, script_.end_submits_);

  // Once END is submitted, readiness must not be queried, even if it would fail.
  script_.upload_ret_ = OB_TIMEOUT;
  script_.ready_ = false;
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  ASSERT_EQ(&script_.end_cbs_[0], ctx_.busy_cbs_.remove_first());
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(4, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
  EXPECT_EQ(4, script_.callback_inits_);
  ASSERT_EQ(OB_SUCCESS, ctx_.exec_info_.dli_batch_set_.sync_ddl_end_succ(key_, SCN::min_scn(), true));
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_TRUE(ctx_.exec_info_.dli_batch_set_.empty());
  EXPECT_EQ(4, script_.upload_checks_);
}

// DLI-GATE-003: failed checks never become successful completion evidence.
TEST_F(TestDliAbortGate, storage_error_preserves_batch_for_next_retry)
{
  start_batch();
  script_.upload_ret_ = OB_TIMEOUT;
  script_.ready_ = true; // A failed check must not be accepted even if its output is true.
  EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->need_compensate_ddl_end());
  EXPECT_EQ(0, script_.end_submits_);
  script_.upload_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(1, script_.end_submits_);
}

// DLI-GATE-004: INC_MINOR keeps its existing compensation path.
TEST_F(TestDliAbortGate, minor_bypasses_shared_dump_gate)
{
  start_batch(false, 1, 200001, DIRECT_LOAD_INCREMENTAL);
  script_.upload_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
  ASSERT_NE(nullptr, batch(false));
  EXPECT_TRUE(batch(false)->is_ddl_end_logging());
}

// A physical INC_MINOR batch can use Major START/END logs during conflict handling.
TEST_F(TestDliAbortGate, minor_with_major_log_bypasses_shared_dump_gate)
{
  start_batch(true, 1, 200001, DIRECT_LOAD_INCREMENTAL);
  script_.upload_ret_ = OB_INVALID_ARGUMENT;
  ASSERT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_FALSE(batch()->need_compensate_ddl_end());
  EXPECT_TRUE(batch()->is_ddl_end_logging());

  ASSERT_EQ(&script_.end_cbs_[0], ctx_.busy_cbs_.remove_first());
  ASSERT_EQ(OB_SUCCESS, ctx_.exec_info_.dli_batch_set_.sync_ddl_end_succ(key_, SCN::min_scn(), true));
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_TRUE(ctx_.exec_info_.dli_batch_set_.empty());
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
}

// DLI-GATE-006: writes in progress can still produce KV data or callbacks.
TEST_F(TestDliAbortGate, pending_writer_must_finish_before_storage_check)
{
  start_batch();
  ASSERT_TRUE(ctx_.busy_cbs_.is_empty());
  ATOMIC_STORE(&ctx_.pending_write_, 1);
  script_.ready_ = true;
  EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(0, script_.end_submits_);
  EXPECT_EQ(0, script_.callback_inits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->need_compensate_ddl_end());

  ATOMIC_STORE(&ctx_.pending_write_, 0);
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(1, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->is_ddl_end_logging());
}

// DLI-GATE-007: a failed END submission must retain the batch for retry.
TEST_F(TestDliAbortGate, end_submission_failure_preserves_batch_for_retry)
{
  start_batch();
  script_.ready_ = true;
  script_.submit_ret_ = OB_EAGAIN;
  EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(1, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->need_compensate_ddl_end());
  script_.submit_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_TRUE(batch()->is_ddl_end_logging());
  EXPECT_EQ(2, script_.end_submits_);
  EXPECT_EQ(2, script_.callback_inits_);
}

// DLI-GATE-008: commit-info waits for the pending END; it is not a state log.
TEST_F(TestDliAbortGate, pending_commit_info_waits_for_dump)
{
  start_batch();
  ASSERT_FALSE(ObTxLogTypeChecker::is_state_log(ObTxLogType::TX_COMMIT_INFO_LOG));
  EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_COMMIT_INFO_LOG));
  EXPECT_EQ(1, script_.upload_checks_);
  EXPECT_EQ(0, script_.end_submits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->need_compensate_ddl_end());
  script_.ready_ = true;
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_COMMIT_INFO_LOG));
  EXPECT_EQ(1, script_.end_submits_);
}

// DLI-GATE-005/009/010: drain existing callbacks, then submit all ready batches in one round.
TEST_F(TestDliAbortGate, existing_callback_blocks_all_batches_until_next_round)
{
  ObDDLIncLogBasic keys[3];
  for (int64_t i = 0; i < ARRAYSIZEOF(keys); ++i) {
    start_batch(true, i + 1, 200001 + i);
    keys[i] = key_;
  }
  ASSERT_EQ(3, ctx_.exec_info_.dli_batch_set_.size());
  ASSERT_TRUE(ctx_.busy_cbs_.is_empty());
  ASSERT_TRUE(ctx_.busy_cbs_.add_last(&entry_cb_));
  script_.ready_ = true;

  EXPECT_EQ(OB_TRANS_CANNOT_BE_KILLED, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(0, script_.end_submits_);
  EXPECT_EQ(1, ctx_.busy_cbs_.get_size());
  for (const ObDDLIncLogBasic &key : keys) {
    ASSERT_NE(nullptr, batch(key));
    EXPECT_TRUE(batch(key)->need_compensate_ddl_end());
  }

  ASSERT_EQ(&entry_cb_, ctx_.busy_cbs_.remove(&entry_cb_));
  EXPECT_EQ(OB_SUCCESS, ctx_.check_dli_batch_completed_(ObTxLogType::TX_ABORT_LOG));
  EXPECT_EQ(3, script_.upload_checks_);
  EXPECT_EQ(3, script_.end_submits_);
  EXPECT_EQ(3, ctx_.busy_cbs_.get_size());
  for (const ObDDLIncLogBasic &key : keys) {
    ASSERT_NE(nullptr, batch(key));
    EXPECT_TRUE(batch(key)->is_ddl_end_logging());
  }
}

// DLI-GATE-013: a normal END must not query dump readiness.
TEST_F(TestDliAbortGate, normal_major_end_bypasses_dump_check)
{
  start_batch();
  script_.expected_rollback_ = false;
  script_.upload_ret_ = OB_TIMEOUT;
  ObDDLIncCommitLog log;
  ASSERT_EQ(OB_SUCCESS, log.init(key_, false /*is_rollback*/));
  ObDDLIncCommitClogCb cb;
  ASSERT_EQ(OB_SUCCESS, cb.init(ctx_.ls_id_, log));
  SCN scn;
  const int ret = ctx_.submit_direct_load_inc_log_<ObTxDirectLoadIncMajorLog, ObDDLIncCommitLog>(
      log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END, key_, &cb,
      logservice::ObReplayBarrierType::STRICT_BARRIER, ctx_.trans_id_.get_id(), scn, false);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
}

// DLI-GATE-014: directly submitting a rollback END still executes the gate.
TEST_F(TestDliAbortGate, rollback_end_is_checked_inside_submit)
{
  start_batch();
  ObDDLIncCommitLog log;
  ASSERT_EQ(OB_SUCCESS, log.init(key_, true /*is_rollback*/));
  ObDDLIncCommitClogCb cb;
  ASSERT_EQ(OB_SUCCESS, cb.init(ctx_.ls_id_, log));
  SCN scn;
  int ret = ctx_.submit_direct_load_inc_log_<ObTxDirectLoadIncMajorLog, ObDDLIncCommitLog>(
      log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END, key_, &cb,
      logservice::ObReplayBarrierType::STRICT_BARRIER, ctx_.trans_id_.get_id(), scn, false);
  EXPECT_EQ(OB_EAGAIN, ret);
  EXPECT_EQ(1, script_.upload_checks_);
  EXPECT_EQ(0, script_.end_submits_);
  ASSERT_NE(nullptr, batch());
  EXPECT_TRUE(batch()->need_compensate_ddl_end());

  script_.ready_ = true;
  ret = ctx_.submit_direct_load_inc_log_<ObTxDirectLoadIncMajorLog, ObDDLIncCommitLog>(
      log, ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END, key_, &cb,
      logservice::ObReplayBarrierType::STRICT_BARRIER, ctx_.trans_id_.get_id(), scn, false);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, script_.upload_checks_);
  EXPECT_EQ(1, script_.end_submits_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_dli_abort_gate.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

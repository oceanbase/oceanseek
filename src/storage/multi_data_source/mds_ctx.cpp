/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "mds_ctx.h"
#include "storage/memtable/ob_memtable_interface.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
namespace mds
{

MdsCtx::MdsCtx()
  : register_mds_memtable_write_cnt_(0),
    state_(TwoPhaseCommitState::STATE_INIT),
    lock_(common::ObLatchIds::OB_MDS_CTX_LOCK),
    writer_(),
    seq_no_(transaction::ObTxSEQ::MIN_VAL())
{
}

MdsCtx::MdsCtx(const MdsWriter &writer, const transaction::ObTxSEQ start_seq)
  : register_mds_memtable_write_cnt_(0),
    state_(TwoPhaseCommitState::STATE_INIT),
    lock_(common::ObLatchIds::OB_MDS_CTX_LOCK),
    writer_(writer),
    seq_no_(start_seq)
{
}

MdsCtx::~MdsCtx() {}

void MdsCtx::advance_state_if_behind_(TwoPhaseCommitState target)
{
  MdsWLockGuard lg(lock_);
  if (state_ < target) {
    check_and_advance_two_phase_commit(state_, target);
  }
}

int MdsCtx::assign(const MdsCtx &rhs)
{
  MdsWLockGuard lg(lock_);
  writer_ = rhs.writer_;
  state_ = rhs.state_;
  return OB_SUCCESS;
}

const MdsWriter MdsCtx::get_writer() const { return writer_; }

int MdsCtx::set_writer(const MdsWriter &writer)
{
  int ret = OB_SUCCESS;
  MdsWLockGuard lg(lock_);
  if (state_ != TwoPhaseCommitState::STATE_INIT) {
    MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "not allow set writer in non-init state", K(*this), K(writer));
  } else {
    writer_.writer_type_ = writer.writer_type_;
    writer_.writer_id_ = writer.writer_id_;
  }
  return ret;
}

void MdsCtx::set_seq_no(const transaction::ObTxSEQ seq_no)
{
  seq_no_ = seq_no;
}

int MdsCtx::inc_seq_no()
{
  int ret = OB_SUCCESS;
  int64_t seq = 0;
  if (OB_FAIL(ObSequence::get_and_inc_max_seq_no(0, seq))) {
    MDS_LOG(WARN, "fail to get and inc max seq no", K(ret), KPC(this));
  } else {
    seq_no_ = transaction::ObTxSEQ::mk_v0(seq);
  }
  return ret;
}

transaction::ObTxSEQ MdsCtx::get_seq_no() const
{
  return seq_no_;
}

bool MdsCtx::can_write() const
{
  MdsRLockGuard lg(lock_);
  return state_ == TwoPhaseCommitState::STATE_INIT &&
          writer_.is_valid();
}

void MdsCtx::register_mds_memtable_write()
{
  MdsWLockGuard lg(lock_);
  ++register_mds_memtable_write_cnt_;
}

void MdsCtx::on_redo(const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;
  MDS_ASSERT(writer_.writer_type_ == WriterType::TRANSACTION);// can only called by TRANS, or must call single_log_commit()
  memtable::ObIMemtableCtx *mt_ctx = get_mem_ctx();
  int64_t sync_flipped_cnt = 0;
  if (OB_ISNULL(mt_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "memtable ctx is null for on_redo", K(ret), KPC(mt_ctx), KPC(this));
  } else if (is_for_replay()) {
    // for replay skip
  } else if (OB_FAIL(mt_ctx->mds_sync_log_succ(seq_no_, redo_scn, sync_flipped_cnt))) {
    MDS_LOG_RET(ERROR, ret, "mds on_redo dispatch log_submitted failed", K(ret), K(seq_no_), K(redo_scn),
                K(sync_flipped_cnt), KPC(mt_ctx), KPC(this));
  } else {
    if (OB_UNLIKELY(register_mds_memtable_write_cnt_ != sync_flipped_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_RET(ERROR, ret,
                  "mds on_redo sync cnt mismatch register_mds_memtable_write_cnt vs callback flipped",
                  K(register_mds_memtable_write_cnt_),
                  K(sync_flipped_cnt),
                  K(seq_no_),
                  K(redo_scn),
                  KPC(this));
    } else {
      MDS_LOG(INFO, "completed mds on_redo", K(ret), K(seq_no_), K(redo_scn), K(sync_flipped_cnt), KPC(this));
    }
  }
}

void MdsCtx::before_prepare()
{
  MDS_ASSERT(writer_.writer_type_ == WriterType::TRANSACTION);
  advance_state_if_behind_(TwoPhaseCommitState::BEFORE_PREPARE);
}

void MdsCtx::on_prepare(const share::SCN &prepare_version)
{
  UNUSED(prepare_version);
  MDS_ASSERT(writer_.writer_type_ == WriterType::TRANSACTION);
  advance_state_if_behind_(TwoPhaseCommitState::ON_PREPARE);
}

void MdsCtx::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  UNUSED(commit_version);
  UNUSED(commit_scn);
  MDS_ASSERT(writer_.writer_type_ == WriterType::TRANSACTION);
  advance_state_if_behind_(TwoPhaseCommitState::ON_COMMIT);
}

void MdsCtx::on_abort(const share::SCN &abort_scn)
{
  UNUSED(abort_scn);
  advance_state_if_behind_(TwoPhaseCommitState::ON_ABORT);
}

void MdsCtx::single_log_commit(const share::SCN commit_version, const share::SCN commit_scn)
{
  UNUSED(commit_version);
  UNUSED(commit_scn);
  advance_state_if_behind_(TwoPhaseCommitState::ON_COMMIT);
}

void MdsCtx::single_log_abort()
{
  MDS_ASSERT(writer_.writer_type_ != WriterType::TRANSACTION);// TRANSACTION use two-phase-commit
  on_abort(share::SCN::max_scn());
}

}
}
}

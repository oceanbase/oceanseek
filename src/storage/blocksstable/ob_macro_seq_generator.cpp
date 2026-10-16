/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/ob_server_struct.h"
#endif
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

static_assert(compaction::MACRO_STEP_SIZE == ObMacroDataSeq::MAX_MACRO_SEQ + 1,
    "macro sequence slot must contain all 32-bit local sequence values");

void ObMacroSeqParam::reset()
{
  seq_type_ = SEQ_TYPE_MAX;
  start_ = 0;
  end_ = -1;
  interval_ = 0;
  step_ = 0;
}

bool ObMacroSeqParam::is_valid() const
{
  bool bret = seq_type_ < SEQ_TYPE_MAX && start_ >= 0;
  if (bret && SEQ_TYPE_INC == seq_type_) {
    bret = end_ < 0 || end_ > start_;
  }
  if (bret && SEQ_TYPE_SKIP == seq_type_) {
    bret = interval_ > 0 && step_ > 0;
  }
  return bret;
}

int ObVersionLocalMacroSeq::build_window(
    const int64_t seq_slot,
    ObMacroSeqWindow &window)
{
  int ret = OB_SUCCESS;
  const int64_t step_size = compaction::MACRO_STEP_SIZE;
  const int64_t sequence_limit =
      static_cast<int64_t>(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT);
  window.reset();
  if (OB_UNLIKELY(seq_slot < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version local sequence argument",
             K(ret), K(seq_slot), K(step_size));
  } else if (OB_UNLIKELY(seq_slot > sequence_limit / step_size - 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("version local sequence window overflows",
             K(ret), K(seq_slot), K(step_size), K(sequence_limit));
  } else {
    window.start_ = seq_slot * step_size;
    window.end_ = window.start_ + step_size;
  }
  return ret;
}

int64_t ObVersionLocalMacroSeq::get_inc_major_index_slot()
{
  return static_cast<int64_t>(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT)
         / compaction::MACRO_STEP_SIZE - 3;
}

int64_t ObVersionLocalMacroSeq::get_inc_major_persister_slot()
{
  return get_inc_major_index_slot() + 1;
}

int ObVersionLocalMacroSeq::build_inc_major_data_window(
    const int64_t seq_slot,
    ObMacroSeqWindow &window)
{
  int ret = OB_SUCCESS;
  window.reset();
  if (OB_UNLIKELY(seq_slot < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inc major data sequence slot", K(ret), K(seq_slot));
  } else if (OB_UNLIKELY(seq_slot >= get_inc_major_index_slot())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("inc major data sequence enters reserved slots",
             K(ret), K(seq_slot), "slot_limit", get_inc_major_index_slot());
  } else if (OB_FAIL(build_window(seq_slot, window))) {
    LOG_WARN("fail to build inc major data sequence window",
             K(ret), K(seq_slot));
  }
  return ret;
}

int ObVersionLocalMacroSeq::build_inc_major_index_window(
    ObMacroSeqWindow &window)
{
  return build_window(get_inc_major_index_slot(), window);
}

int ObVersionLocalMacroSeq::build_inc_major_persister_window(
    ObMacroSeqWindow &window)
{
  return build_window(get_inc_major_persister_slot(), window);
}

int ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
    const int64_t seq_slot,
    const int64_t start,
    int64_t &end)
{
  int ret = OB_SUCCESS;
  ObMacroSeqWindow data_window;
  end = -1;
  if (OB_FAIL(build_inc_major_data_window(seq_slot, data_window))) {
    LOG_WARN("fail to build inc major data sequence window",
             K(ret), K(seq_slot));
  } else if (OB_UNLIKELY(start < data_window.start_ || start >= data_window.end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inc major data sequence start is outside window",
             K(ret), K(seq_slot), K(start), K(data_window));
  } else {
    end = data_window.end_;
  }
  return ret;
}

int ObVersionLocalMacroSeq::get_raw_seq(
    const int64_t packed_seq,
    int64_t &raw_seq)
{
  int ret = OB_SUCCESS;
  raw_seq = -1;
  if (OB_UNLIKELY(packed_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid packed macro sequence", K(ret), K(packed_seq));
  } else {
    raw_seq = ObMacroDataSeq(packed_seq).get_version_local_seq();
  }
  return ret;
}

void ObMacroIncSeqGenerator::reset()
{
  is_inited_ = false;
  start_ = 0;
  current_ = -1;
  seq_threshold_ = 0;
}

int ObMacroIncSeqGenerator::init(const ObMacroSeqParam &seq_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!seq_param.is_valid() || seq_param.seq_type_ != ObMacroSeqParam::SEQ_TYPE_INC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(seq_param));
  } else {
    start_ = seq_param.start_;
    current_ = -1;
    if (seq_param.end_ > 0) {
      seq_threshold_ = seq_param.end_;
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      seq_threshold_ = start_ + (GCTX.is_shared_storage_mode()
          ? compaction::MACRO_STEP_SIZE
          : blocksstable::ObMacroDataSeq::MAX_MACRO_SEQ);
#else
      seq_threshold_ = start_ + blocksstable::ObMacroDataSeq::MAX_MACRO_SEQ;
#endif
    }
    is_inited_ = true;
  }
  return ret;
}

int ObMacroIncSeqGenerator::get_next(int64_t &seq_val)
{
  int ret = OB_SUCCESS;
  seq_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(preview_next(current_, seq_val))) {
    LOG_WARN("preview next value failed", K(ret));
  } else if (OB_UNLIKELY(seq_val >= seq_threshold_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("seq is larger than threshold", K(ret), K(seq_val), K_(seq_threshold), K_(start));
  } else {
    current_ = seq_val;
  }
  return ret;
}

int ObMacroIncSeqGenerator::preview_next(const int64_t current_val, int64_t &next_val) const
{
  int ret = OB_SUCCESS;
  next_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", K(ret));
  } else {
    if (current_val < 0) {
      next_val = start_;
    } else {
      next_val = current_val + 1;
    }
  }
  return ret;
}

void ObMacroSkipSeqGenerator::reset()
{
  ddl_seq_generator_.reset();
}

int ObMacroSkipSeqGenerator::init(const ObMacroSeqParam &seq_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!seq_param.is_valid() || seq_param.seq_type_ != ObMacroSeqParam::SEQ_TYPE_SKIP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(seq_param));
  } else if (OB_FAIL(ddl_seq_generator_.init(seq_param.start_, seq_param.interval_, seq_param.step_))) {
    LOG_WARN("init ddl sequence generator failed", K(ret), K(seq_param));
  }
  return ret;
}

int ObMacroSkipSeqGenerator::get_next(int64_t &seq_val)
{
  bool is_step_over = false;
  return ddl_seq_generator_.get_next(seq_val, is_step_over);
}

int ObMacroSkipSeqGenerator::preview_next(const int64_t current_val, int64_t &next_val) const
{
  return ddl_seq_generator_.preview_next(current_val, next_val);
}

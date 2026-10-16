/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inc_ss_macro_seq_define.h"
#include "lib/oblog/ob_log.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"

namespace oceanbase
{
namespace storage
{

int ObIncSSMacroSeq::check_parallel_slot_count(const int64_t slot_count)
{
  int ret = OB_SUCCESS;
  const int64_t max_slot_count = static_cast<int64_t>(blocksstable::ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT)
      / get_step_size();
  if (OB_UNLIKELY(slot_count < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parallel slot count", K(ret), K(slot_count));
  } else if (OB_UNLIKELY(slot_count > max_slot_count)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("parallel slot count overflows version local sequence", K(ret), K(slot_count), K(max_slot_count));
  }
  return ret;
}

int ObIncSSMacroSeq::build_parallel_start_seq(
    const int64_t global_task_idx,
    int64_t &start_seq)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroSeqWindow window;
  start_seq = -1;
  if (OB_UNLIKELY(global_task_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid global task index", K(ret), K(global_task_idx));
  } else if (OB_FAIL(blocksstable::ObVersionLocalMacroSeq::build_window(
      global_task_idx, window))) {
    LOG_WARN("global task index overflows version local sequence",
        K(ret), K(global_task_idx));
  } else {
    start_seq = window.start_;
  }
  return ret;
}

int64_t ObIncSSMacroSeq::get_step_size()
{
  return compaction::MACRO_STEP_SIZE;
}

} // namespace storage
} // namespace oceanbase

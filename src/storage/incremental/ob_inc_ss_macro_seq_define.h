/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_INC_SS_MACRO_SEQ_DEFINE_H_
#define OCEANBASE_STORAGE_INC_SS_MACRO_SEQ_DEFINE_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace storage
{

struct ObIncSSMacroSeq final
{
  static int check_parallel_slot_count(const int64_t slot_count);
  static int build_parallel_start_seq(
      const int64_t global_task_idx,
      int64_t &start_seq);
  static int64_t get_step_size();
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_INC_SS_MACRO_SEQ_DEFINE_H_

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_GRANULE_SPLITTER_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_GRANULE_SPLITTER_H_

#include "sql/engine/px/ob_granule_pump.h"

namespace oceanbase
{
namespace sql
{

// Give each ordered writer stream to exactly one read worker. A worker can
// receive several streams when the admitted DOP is smaller than their count;
// that case must not be advertised as worker-local ordering by the optimizer.
class ObAsyncResultGranuleSplitter final : public ObGranuleSplitter
{
public:
  int split_granule(ObGranulePumpArgs &args,
                    GITaskArrayMap &gi_task_array_result,
                    bool check_task_exist);

  // The input has one consecutive GI index per writer stream; entries with
  // the same index are that writer's files in manifest order. Kept separate
  // from the execution-context split so this ordering contract can be tested.
  static int assign_writer_streams(const ObGITaskSet &total_task_set,
                                   int64_t parallelism,
                                   bool worker_order_claimed,
                                   ObGITaskArray &taskset_array,
                                   int64_t &stream_count);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_GRANULE_SPLITTER_H_

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/result_catalog/ob_async_result_granule_splitter.h"

namespace oceanbase
{
namespace sql
{

int ObAsyncResultGranuleSplitter::split_granule(
    ObGranulePumpArgs &args,
    GITaskArrayMap &gi_task_array_result,
    const bool check_task_exist)
{
  int ret = OB_SUCCESS;
  ObIArray<const ObTableScanSpec *> &scan_ops = args.op_info_.get_scan_ops();
  int64_t task_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(scan_ops.count() != 1 || args.tablet_arrays_.count() != 1
                  || args.parallelism_ <= 0 || OB_ISNULL(scan_ops.at(0))
                  || !scan_ops.at(0)->result_writer_ordered_
                  || ObGranuleUtil::affinitize(args.gi_attri_flag_)
                  || ObGranuleUtil::pwj_gi(args.gi_attri_flag_)
                  || ObGranuleUtil::access_all(args.gi_attri_flag_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ordered Result scan granule arguments", K(ret), K(args));
  } else if (OB_FAIL(ObGranulePump::find_task_array_item(
                 gi_task_array_result, scan_ops, check_task_exist, task_idx))) {
    LOG_WARN("failed to find ordered Result task array", K(ret));
  } else {
    const ObTableScanSpec *tsc = scan_ops.at(0);
    ObGITaskSet total_task_set;
    ObGITaskArray &taskset_array = gi_task_array_result.at(task_idx).taskset_array_;
    bool can_partition_gi = false;
    bool partition_granule = false;
    if (OB_FAIL(ObGranuleUtil::use_partition_granule(args, can_partition_gi))) {
      LOG_WARN("failed to check ordered Result partition granule", K(ret));
    } else if (FALSE_IT(partition_granule = args.need_partition_granule() || can_partition_gi)) {
    } else if (OB_FAIL(split_gi_task(args, tsc, tsc->get_scan_key_id(), tsc->get_id(),
                                     args.tablet_arrays_.at(0), partition_granule,
                                     total_task_set, ObGITaskSet::GI_RANDOM_NONE))) {
      LOG_WARN("failed to split ordered Result writer streams", K(ret));
    }

    int64_t stream_count = 0;
    if (OB_SUCC(ret)
        && OB_FAIL(assign_writer_streams(total_task_set, args.parallelism_,
                                         tsc->result_worker_order_claimed_,
                                         taskset_array, stream_count))) {
      LOG_WARN("failed to assign ordered Result writer streams", K(ret));
    } else if (OB_SUCC(ret)) {
      gi_task_array_result.at(task_idx).tsc_op_id_ = tsc->get_id();
      LOG_TRACE("assigned ordered Result writer streams to read workers",
                K(stream_count), K(args.parallelism_), K(taskset_array));
    }
  }
  return ret;
}

int ObAsyncResultGranuleSplitter::assign_writer_streams(
    const ObGITaskSet &total_task_set,
    const int64_t parallelism,
    const bool worker_order_claimed,
    ObGITaskArray &taskset_array,
    int64_t &stream_count)
{
  int ret = OB_SUCCESS;
  stream_count = 0;
  if (OB_UNLIKELY(parallelism <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ordered Result read parallelism", K(ret), K(parallelism));
  }
  int64_t previous_idx = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < total_task_set.gi_task_set_.count(); ++i) {
    const int64_t idx = total_task_set.gi_task_set_.at(i).idx_;
    if (i == 0 || idx != previous_idx) {
      if (OB_UNLIKELY(idx != stream_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ordered Result writer task IDs are not consecutive",
                 K(ret), K(i), K(idx), K(stream_count));
      } else {
        ++stream_count;
      }
    }
    previous_idx = idx;
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(stream_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ordered Result scan has no writer task", K(ret));
  } else if (OB_SUCC(ret) && worker_order_claimed
             && OB_UNLIKELY(stream_count > parallelism)) {
    ret = OB_ERR_INSUFFICIENT_PX_WORKER;
    LOG_WARN("ordered Result scan has fewer readers than writer streams",
             K(ret), K(stream_count), K(parallelism));
  } else if (OB_SUCC(ret)) {
    taskset_array.reuse();
    if (OB_FAIL(taskset_array.prepare_allocate(parallelism))) {
      LOG_WARN("failed to allocate ordered Result worker task arrays", K(ret));
    }
  }

  int64_t current_idx = -1;
  int64_t worker_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < total_task_set.gi_task_set_.count(); ++i) {
    const ObGITaskSet::ObGITaskInfo &task = total_task_set.gi_task_set_.at(i);
    if (task.idx_ != current_idx) {
      current_idx = task.idx_;
      worker_id = current_idx % parallelism;
      ++taskset_array.at(worker_id).task_count_;
    }
    if (OB_FAIL(taskset_array.at(worker_id).gi_task_set_.push_back(task))) {
      LOG_WARN("failed to assign ordered Result file to reader",
               K(ret), K(i), K(worker_id), K(current_idx));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

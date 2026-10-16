/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/engine/table/ob_ddl_block_sample_scan_op.h"
#include "sql/engine/px/ob_px_sample_util.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

int ObDDLBlockSampleScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  set_need_sample(!MY_SPEC.get_sample_info().is_no_sample());
  if (OB_FAIL(ObTableScanOp::inner_open())) {
    LOG_WARN("fail to inner open", K(ret));
  } else if (MY_SPEC.use_dist_das()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ddl block sample scan with dist das");
  }
  return ret;
}

int ObDDLBlockSampleScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret), "op", op_name());
    }
  }
  LOG_DEBUG("static engine block sample scan get row", K(ret));
  return ret;
}

int ObDDLBlockSampleScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_batch(max_row_cnt))) {
    LOG_WARN("get next batch failed", K(ret), "op", op_name());
  }
  return ret;
}

int ObDDLBlockSampleScanOp::do_init_before_get_row()
{
  int ret = OB_SUCCESS;
  if (need_init_before_get_row_ && OB_FAIL(set_fts_sample_doc_limit())) {
    LOG_WARN("fail to set fts sample doc limit", K(ret));
  } else if (OB_FAIL(ObTableScanOp::do_init_before_get_row())) {
    LOG_WARN("fail to init before get row", K(ret));
  }
  return ret;
}

int ObDDLBlockSampleScanOp::set_fts_sample_doc_limit()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_fts_index_aux_) {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      // Async complete refresh is also marked as async index refresh, but only
      // incremental refresh scans OLD_NEW$$ in real-time mview mode.
      const bool is_async_incremental_refresh =
          session->get_ddl_info().is_async_index_refresh()
          && MY_SPEC.get_query_flag().is_mr_rt_mview_base_scan();
      if (is_async_incremental_refresh
          && ctx_.get_expect_range_count() > 1
          && OB_ISNULL(MY_SPEC.limit_)
          && OB_ISNULL(MY_SPEC.offset_)
          && !MY_SPEC.batch_scan_flag_
          && OB_ISNULL(get_tsc_ctdef().lookup_ctdef_)
          && OB_ISNULL(tsc_rtdef_.attach_rtinfo_)) {
        limit_param_.offset_ = 0;
        limit_param_.limit_ = need_sample_
            ? get_one_thread_sampling_count_by_parallel(ctx_.get_expect_range_count())
            : -1;
        tsc_rtdef_.scan_rtdef_.limit_param_ = limit_param_;
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDDLBlockSampleScanSpec, ObTableScanSpec), sample_info_);

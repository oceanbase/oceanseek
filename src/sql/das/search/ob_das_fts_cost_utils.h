/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_FTS_COST_UTILS_H_
#define OCEANBASE_SQL_OB_DAS_FTS_COST_UTILS_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

class ObDASSearchCtx;
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;

class ObDASFtsCostUtils
{
public:
  static int estimate_async_token_doc_cnts(
      ObDASSearchCtx &search_ctx,
      const ObDASIRScanCtDef &ir_ctdef,
      ObDASIRScanRtDef &ir_rtdef,
      const common::ObIArray<common::ObString> &query_tokens,
      common::ObIArray<int64_t> &token_doc_cnts);
  static int aggregate_and_cost(
      const common::ObIArray<int64_t> &costs,
      int64_t &result);
  static int aggregate_or_cost(
      const common::ObIArray<int64_t> &costs,
      const int64_t table_row_count,
      int64_t &result);
  static int aggregate_msm_cost(
      const common::ObIArray<int64_t> &costs,
      const int64_t min_should_match,
      const int64_t table_row_count,
      int64_t &result);
  static int aggregate_match_cost(
      const common::ObIArray<int64_t> &costs,
      const int64_t match_operator,
      const int64_t minimum_should_match,
      const int64_t table_row_count,
      int64_t &result);
  static int estimate_async_token_set_cost(
      ObDASSearchCtx &search_ctx,
      const ObDASIRScanCtDef &ir_ctdef,
      ObDASIRScanRtDef &ir_rtdef,
      const common::ObIArray<common::ObString> &query_tokens,
      const int64_t match_operator,
      const int64_t minimum_should_match,
      const int64_t table_row_count,
      int64_t &result);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_FTS_COST_UTILS_H_

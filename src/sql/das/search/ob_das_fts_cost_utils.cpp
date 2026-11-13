/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_fts_cost_utils.h"
#include "ob_das_scalar_define.h"
#include "ob_das_search_context.h"
#include "ob_das_search_define.h"
#include "ob_das_search_utils.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "lib/container/ob_heap.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace sql
{

int ObDASFtsCostUtils::estimate_async_token_doc_cnts(
    ObDASSearchCtx &search_ctx,
    const ObDASIRScanCtDef &ir_ctdef,
    ObDASIRScanRtDef &ir_rtdef,
    const common::ObIArray<common::ObString> &query_tokens,
    common::ObIArray<int64_t> &token_doc_cnts)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = ir_ctdef.get_inv_idx_scan_scalar_ctdef();
  const bool is_search_index_fulltext = ir_ctdef.search_index_fulltext_info_.has_search_index_fulltext();
  common::ObTabletID inv_idx_tablet_id;
  common::ObArenaAllocator term_allocator(common::ObMemAttr(MTL_ID(), "FtsCostTerm"));
  storage::ObAsyncBM25ParamEstimator estimator;
  token_doc_cnts.reuse();
  if (query_tokens.empty()) {
    // no token needs estimation
  } else if (is_search_index_fulltext && OB_ISNULL(ir_ctdef.search_text_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null Search Text expression", KR(ret));
  } else if (OB_ISNULL(inv_idx_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inverted index scan ctdef", KR(ret));
  } else if (OB_FAIL(search_ctx.get_related_tablet_id(inv_idx_scan_ctdef, inv_idx_tablet_id))) {
    LOG_WARN("failed to get inverted index tablet id", KR(ret), KPC(inv_idx_scan_ctdef));
  } else if (OB_FAIL(estimator.init(
      search_ctx, ir_ctdef, ir_rtdef, inv_idx_tablet_id,
      is_search_index_fulltext, ir_ctdef.search_index_fulltext_info_.column_idx_,
      ir_ctdef.search_index_fulltext_info_.encoded_path_, query_tokens.at(0)))) {
    LOG_WARN("failed to init async bm25 param estimator",
        KR(ret), K(inv_idx_tablet_id), K(query_tokens.at(0)));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
    uint64_t token_doc_cnt = 0;
    common::ObString token = query_tokens.at(i);
    blocksstable::ObDatumRowkey term_key;
    if (is_search_index_fulltext && OB_FAIL(ObDASSearchUtils::build_fulltext_term_key(
        ir_ctdef.search_index_fulltext_info_, token, ir_ctdef.search_text_->obj_meta_, term_allocator, term_key))) {
      LOG_WARN("failed to build async cost estimation term key", KR(ret), K(i));
    } else if (is_search_index_fulltext && FALSE_IT(
        token = term_key.get_datum(term_key.get_datum_cnt() - 1).get_string())) {
    } else if (OB_FAIL(estimator.switch_query_token(token))) {
      LOG_WARN("failed to switch async bm25 estimator token",
          KR(ret), K(i), K(query_tokens.at(i)));
    } else if (OB_FAIL(estimator.get_token_doc_cnt(token_doc_cnt))) {
      LOG_WARN("failed to get async token doc count",
          KR(ret), K(inv_idx_tablet_id), K(i), K(query_tokens.at(i)));
    } else {
      const int64_t cost = token_doc_cnt > static_cast<uint64_t>(INT64_MAX)
          ? INT64_MAX : static_cast<int64_t>(token_doc_cnt);
      if (OB_FAIL(token_doc_cnts.push_back(cost))) {
        LOG_WARN("failed to push async token doc count", KR(ret), K(i), K(cost));
      }
    }
  }
  return ret;
}

int ObDASFtsCostUtils::estimate_async_token_set_cost(
    ObDASSearchCtx &search_ctx,
    const ObDASIRScanCtDef &ir_ctdef,
    ObDASIRScanRtDef &ir_rtdef,
    const common::ObIArray<common::ObString> &query_tokens,
    const int64_t match_operator,
    const int64_t minimum_should_match,
    const int64_t table_row_count,
    int64_t &result)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 16> token_doc_cnts;
  result = 0;
  if (OB_FAIL(token_doc_cnts.reserve(query_tokens.count()))) {
    LOG_WARN("failed to reserve token document counts", KR(ret), K(query_tokens.count()));
  } else if (OB_FAIL(estimate_async_token_doc_cnts(
      search_ctx, ir_ctdef, ir_rtdef, query_tokens, token_doc_cnts))) {
    LOG_WARN("failed to estimate async token document counts", KR(ret));
  } else if (OB_FAIL(aggregate_match_cost(
      token_doc_cnts, match_operator, minimum_should_match, table_row_count, result))) {
    LOG_WARN("failed to aggregate async token set cost", KR(ret), K(match_operator),
        K(minimum_should_match), K(table_row_count));
  }
  return ret;
}

int ObDASFtsCostUtils::aggregate_and_cost(
    const common::ObIArray<int64_t> &costs,
    int64_t &result)
{
  int ret = OB_SUCCESS;
  result = costs.empty() ? 0 : costs.at(0);
  for (int64_t i = 0; OB_SUCC(ret) && i < costs.count(); ++i) {
    if (OB_UNLIKELY(costs.at(i) < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid negative token cost", KR(ret), K(i), K(costs.at(i)));
    } else {
      result = std::min(result, costs.at(i));
    }
  }
  return ret;
}

int ObDASFtsCostUtils::aggregate_or_cost(
    const common::ObIArray<int64_t> &costs,
    const int64_t table_row_count,
    int64_t &result)
{
  int ret = OB_SUCCESS;
  result = 0;
  if (OB_UNLIKELY(table_row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table row count", KR(ret), K(table_row_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < costs.count() && result < table_row_count; ++i) {
    const int64_t cur_cost = costs.at(i);
    if (OB_UNLIKELY(cur_cost < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid negative token cost", KR(ret), K(i), K(cur_cost));
    } else {
      result = cur_cost >= table_row_count - result ? table_row_count : result + cur_cost;
    }
  }
  return ret;
}

int ObDASFtsCostUtils::aggregate_msm_cost(
    const common::ObIArray<int64_t> &costs,
    const int64_t min_should_match,
    const int64_t table_row_count,
    int64_t &result)
{
  int ret = OB_SUCCESS;
  result = 0;
  if (OB_UNLIKELY(table_row_count < 0
      || min_should_match <= 1
      || min_should_match > costs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minimum should match cost arguments",
        KR(ret), K(min_should_match), K(table_row_count), K(costs.count()));
  } else {
    const int64_t least_clause_cnt = costs.count() - min_should_match + 1;
    ObDASInt64LessComp cmp;
    ObBinaryHeap<int64_t, ObDASInt64LessComp> max_heap(cmp);
    for (int64_t i = 0; OB_SUCC(ret) && i < costs.count(); ++i) {
      if (OB_UNLIKELY(costs.at(i) < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid negative token cost", KR(ret), K(i), K(costs.at(i)));
      } else if (OB_FAIL(max_heap.push(costs.at(i)))) {
        LOG_WARN("failed to push token cost", KR(ret), K(i), K(costs.at(i)));
      } else if (max_heap.count() > least_clause_cnt && OB_FAIL(max_heap.pop())) {
        LOG_WARN("failed to pop token cost", KR(ret));
      }
    }
    while (OB_SUCC(ret) && !max_heap.empty() && result < table_row_count) {
      const int64_t cur_cost = max_heap.top();
      result = cur_cost >= table_row_count - result ? table_row_count : result + cur_cost;
      if (OB_FAIL(max_heap.pop())) {
        LOG_WARN("failed to pop token cost", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASFtsCostUtils::aggregate_match_cost(
    const common::ObIArray<int64_t> &costs,
    const int64_t match_operator,
    const int64_t minimum_should_match,
    const int64_t table_row_count,
    int64_t &result)
{
  int ret = OB_SUCCESS;
  result = 0;
  if (OB_UNLIKELY(ObMatchOperator::MATCH_OPERATOR_OR != match_operator
      && ObMatchOperator::MATCH_OPERATOR_AND != match_operator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid match operator", KR(ret), K(match_operator));
  } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator
      || minimum_should_match >= costs.count()) {
    if (OB_FAIL(aggregate_and_cost(costs, result))) {
      LOG_WARN("failed to aggregate and cost", KR(ret));
    }
  } else if (minimum_should_match <= 1) {
    if (OB_FAIL(aggregate_or_cost(costs, table_row_count, result))) {
      LOG_WARN("failed to aggregate or cost", KR(ret), K(table_row_count));
    }
  } else if (OB_FAIL(aggregate_msm_cost(
      costs, minimum_should_match, table_row_count, result))) {
    LOG_WARN("failed to aggregate minimum should match cost", KR(ret),
        K(minimum_should_match), K(table_row_count));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

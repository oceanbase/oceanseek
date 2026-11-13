/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_async_segment_group_merge_op.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_token_op.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/fts/ob_fts_tokenizer.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{

int ObDASSearchUtils::build_search_index_term_key(
    const common::ObRowkey &key,
    common::ObIAllocator &allocator,
    blocksstable::ObDatumRowkey &term_key)
{
  int ret = OB_SUCCESS;
  term_key.reset();
  blocksstable::ObStorageDatum datums[share::SEARCH_INDEX_DOC_ID];
  blocksstable::ObDatumRowkey source;
  if (OB_UNLIKELY(!key.is_valid() || key.get_obj_cnt() != share::SEARCH_INDEX_DOC_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Search Index term key", K(ret), K(key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key.get_obj_cnt(); ++i) {
      if (OB_FAIL(datums[i].from_obj_enhance(key.get_obj_ptr()[i]))) {
        LOG_WARN("failed to convert Search Index term component", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(source.assign(datums, share::SEARCH_INDEX_DOC_ID))) {
        LOG_WARN("failed to assign Search Index term key", K(ret));
      } else if (OB_FAIL(source.deep_copy(term_key, allocator))) {
        LOG_WARN("failed to copy Search Index term key", K(ret));
      }
    }
  }
  return ret;
}

int ObDASSearchUtils::build_fulltext_term_key(
    const ObDASSearchIndexTextInfo &search_text,
    const common::ObString &query_token,
    const common::ObObjMeta &query_meta,
    common::ObIAllocator &allocator,
    blocksstable::ObDatumRowkey &term_key)
{
  int ret = OB_SUCCESS;
  term_key.reset();
  share::ObEncParam enc_param;
  common::ObString normalized_token = query_token;
  common::ObString encoded_token;
  if (OB_UNLIKELY(query_token.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Search Text term argument", K(ret), K(search_text), "token_len", query_token.length());
  } else if (OB_FAIL(search_text.is_json_
      ? share::ObSearchIndexValueEncoder::init_json_string_enc_param(enc_param)
      : share::ObSearchIndexValueEncoder::init_string_enc_param(
          common::ObVarcharType, query_meta.get_collation_type(), 0, true, true, enc_param))) {
    LOG_WARN("failed to init Search Text token encoding", K(ret));
  } else if (OB_FAIL(share::ObSearchIndexValueEncoder::normalize_string_for_encoding(
      allocator, normalized_token, enc_param.cs_type_))) {
    LOG_WARN("failed to normalize Search Text query token", K(ret));
  } else if (OB_FAIL(share::ObSearchIndexValueEncoder::encode_string(
      allocator, normalized_token, encoded_token, enc_param, false /* is_allow_truncate */))) {
    LOG_WARN("failed to encode Search Text query token", K(ret));
  } else {
    common::ObObj objs[share::SEARCH_INDEX_DOC_ID];
    objs[share::SEARCH_INDEX_COL_IDX].set_uint64(search_text.column_idx_);
    objs[share::SEARCH_INDEX_PATH].set_varbinary(search_text.encoded_path_);
    objs[share::SEARCH_INDEX_VALUE].set_varbinary(encoded_token);
    if (OB_FAIL(build_search_index_term_key(
        common::ObRowkey(objs, share::SEARCH_INDEX_DOC_ID), allocator, term_key))) {
      LOG_WARN("failed to build Search Text term key", K(ret));
    }
  }
  return ret;
}

int ObDASSearchUtils::resolve_nested_scope_id(
    const ObDASBaseCtDef *ctdef, ObDASScopeId &scope_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObDASBaseCtDef *, 8> pending;
  uint64_t index_table_id = OB_INVALID_ID;
  bool is_async = false;
  scope_id = DAS_SCOPE_MAX;
  if (OB_ISNULL(ctdef)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(pending.push_back(ctdef))) {
    LOG_WARN("failed to collect nested scope root", K(ret));
  }
  while (OB_SUCC(ret) && !pending.empty()) {
    const ObDASBaseCtDef *node = pending.at(pending.count() - 1);
    pending.pop_back();
    if (DAS_OP_NESTED_QUERY == node->op_type_) {
      if (OB_FAIL(resolve_nested_index(node, index_table_id, is_async))) {
        LOG_WARN("failed to resolve nested scope index", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->children_cnt_; ++i) {
        if (OB_ISNULL(node->children_) || OB_ISNULL(node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null child while resolving nested scope", K(ret), K(i));
        } else if (OB_FAIL(pending.push_back(node->children_[i]))) {
          LOG_WARN("failed to collect nested scope child", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_ID == index_table_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested scope has no nested index", K(ret));
    } else {
      scope_id = is_async ? DAS_SCOPE_NESTED_ASYNC : DAS_SCOPE_NESTED_SYNC;
    }
  }
  return ret;
}

int ObDASSearchUtils::resolve_nested_index(
    const ObDASBaseCtDef *ctdef, uint64_t &index_table_id, bool &is_async)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (DAS_OP_SCALAR_QUERY == ctdef->op_type_) {
    const auto *scalar = static_cast<const ObDASScalarCtDef *>(ctdef);
    // The primary probe is an alternative plan, not an input of this subtree.
    ret = resolve_nested_index(scalar->get_index_scan_ctdef(), index_table_id, is_async);
  } else if (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASScalarScanCtDef *>(ctdef);
    if (!scan->is_search_index_ || OB_INVALID_ID == scan->ref_table_id_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_INVALID_ID == index_table_id) {
      index_table_id = scan->ref_table_id_;
      is_async = scan->is_async_index_;
    } else if (is_async != scan->is_async_index_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested scope has inconsistent rowid layouts", K(ret), K(is_async), KPC(scan));
    }
  } else if ((DAS_OP_NESTED_QUERY != ctdef->op_type_ && DAS_OP_BOOLEAN_QUERY != ctdef->op_type_)
             || ctdef->children_cnt_ <= 0 || OB_ISNULL(ctdef->children_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(resolve_nested_index(ctdef->children_[i], index_table_id, is_async)))) {
        LOG_WARN("failed to resolve nested index", K(ret), K(i));
      }
    }
  }
  return ret;
}

namespace
{
static constexpr int64_t ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT = share::SEARCH_INDEX_DOC_ID;
static constexpr int64_t ASYNC_SCALAR_RANGE_COLUMN_CNT = share::SEARCH_INDEX_KEY_CNT;

int validate_async_scalar_range_shape(const common::ObNewRange &range)
{
  int ret = OB_SUCCESS;
  const common::ObRowkey &start_key = range.start_key_;
  const common::ObRowkey &end_key = range.end_key_;
  const bool start_is_ext = start_key.is_min_row() || start_key.is_max_row();
  const bool end_is_ext = end_key.is_min_row() || end_key.is_max_row();
  int64_t rowkey_column_cnt = -1;
  if (!start_is_ext) {
    rowkey_column_cnt = start_key.get_obj_cnt();
  }
  if (!end_is_ext) {
    if (rowkey_column_cnt >= 0 && rowkey_column_cnt != end_key.get_obj_cnt()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("inconsistent async scalar range rowkey column count", K(ret),
               "start_column_count", rowkey_column_cnt,
               "end_column_count", end_key.get_obj_cnt(), K(range));
    } else {
      rowkey_column_cnt = end_key.get_obj_cnt();
    }
  }
  if (OB_SUCC(ret) && rowkey_column_cnt >= 0
      && OB_UNLIKELY(ASYNC_SCALAR_RANGE_COLUMN_CNT != rowkey_column_cnt)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async scalar range rowkey column count", K(ret),
             K(rowkey_column_cnt), K(range));
  }
  return ret;
}

bool is_closed_async_scalar_boundary(
    const common::ObRowkey &key,
    const bool is_start_key)
{
  const common::ObObj *objs = key.get_obj_ptr();
  return !key.is_min_row()
      && !key.is_max_row()
      && ASYNC_SCALAR_RANGE_COLUMN_CNT == key.get_obj_cnt()
      && nullptr != objs
      && (is_start_key
          ? objs[ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT].is_min_value()
          : objs[ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT].is_max_value());
}
} // namespace

int ObDASSearchUtils::eval_and_tokenize_query_text(
    common::ObIAllocator &allocator,
    ObEvalCtx &eval_ctx,
    const ObExpr &query_text_expr,
    const ObDASScalarScanCtDef &inv_idx_scan_ctdef,
    common::ObIArray<common::ObString> &query_tokens,
    const ObDASIRScanCtDef *ir_ctdef)
{
  int ret = OB_SUCCESS;
  ObDatum *query_text_datum = nullptr;
  const bool is_search_text = nullptr != ir_ctdef && ir_ctdef->search_index_fulltext_info_.has_search_index_fulltext();
  query_tokens.reuse();
  if (OB_FAIL(query_text_expr.eval(eval_ctx, query_text_datum))) {
    LOG_WARN("failed to evaluate query text", KR(ret));
  } else if (OB_ISNULL(query_text_datum)
      || OB_UNLIKELY(query_text_datum->is_null() && !is_search_text)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query text datum", KR(ret), KPC(query_text_datum));
  } else if (query_text_datum->is_null()) {
    // Native Search Text treats a NULL query as an empty token set.
  } else if (OB_FAIL(storage::ObFTSTokenizer::tokenize(
      allocator,
      query_text_datum->get_string(),
      is_search_text ? ir_ctdef->search_index_fulltext_info_.parser_name_
                     : inv_idx_scan_ctdef.table_param_.get_parser_name(),
      is_search_text ? ir_ctdef->search_index_fulltext_info_.parser_properties_
                     : inv_idx_scan_ctdef.table_param_.get_parser_property(),
      query_text_expr.obj_meta_,
      query_tokens))) {
    query_tokens.reuse();
    LOG_WARN("failed to tokenize query text", KR(ret));
  }
  return ret;
}

int ObDASSearchUtils::eval_and_tokenize_query_text(
    common::ObIAllocator &allocator,
    ObEvalCtx &eval_ctx,
    const ObExpr &query_text_expr,
    const ObDASScalarScanCtDef &inv_idx_scan_ctdef,
    common::ObIArray<common::ObString> &query_tokens,
    common::ObIArray<int64_t> &token_counts,
    const ObDASIRScanCtDef *ir_ctdef)
{
  int ret = OB_SUCCESS;
  ObDatum *query_text_datum = nullptr;
  const bool is_search_text = nullptr != ir_ctdef && ir_ctdef->search_index_fulltext_info_.has_search_index_fulltext();
  query_tokens.reuse();
  token_counts.reuse();
  if (OB_FAIL(query_text_expr.eval(eval_ctx, query_text_datum))) {
    LOG_WARN("failed to evaluate query text", KR(ret));
  } else if (OB_ISNULL(query_text_datum)
      || OB_UNLIKELY(!is_search_text && query_text_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query text datum", KR(ret), KPC(query_text_datum));
  } else if (is_search_text && query_text_datum->is_null()) {
    // Native Search Text treats a NULL query as an empty token set.
  } else if (OB_FAIL(storage::ObFTSTokenizer::tokenize(
      allocator,
      query_text_datum->get_string(),
      is_search_text ? ir_ctdef->search_index_fulltext_info_.parser_name_
                     : inv_idx_scan_ctdef.table_param_.get_parser_name(),
      is_search_text ? ir_ctdef->search_index_fulltext_info_.parser_properties_
                     : inv_idx_scan_ctdef.table_param_.get_parser_property(),
      query_text_expr.obj_meta_,
      query_tokens,
      token_counts))) {
    query_tokens.reuse();
    token_counts.reuse();
    LOG_WARN("failed to tokenize query text with counts", KR(ret));
  }
  return ret;
}

int ObDASSearchUtils::project_async_scalar_term_range(
    const common::ObNewRange &range,
    AsyncScalarTermRange &term_range)
{
  int ret = OB_SUCCESS;
  common::ObNewRange &projected_range = term_range.range_;
  const common::ObRowkey &start_key = range.start_key_;
  const common::ObRowkey &end_key = range.end_key_;
  const common::ObObj *source_objs = nullptr;
  const bool keep_physical_start =
      is_closed_async_scalar_boundary(start_key, true /*is_start_key*/);
  const bool keep_physical_end =
      is_closed_async_scalar_boundary(end_key, false /*is_start_key*/);
  projected_range.reset();
  if (OB_FAIL(validate_async_scalar_range_shape(range))) {
    LOG_WARN("unexpected async scalar range shape", K(ret), K(range));
  } else if (start_key.is_min_row()) {
    projected_range.start_key_.set_min_row();
  } else if (start_key.is_max_row()) {
    projected_range.start_key_.set_max_row();
  } else if (OB_ISNULL(source_objs = start_key.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null range start key", K(ret), K(range));
  } else {
    const int64_t start_column_cnt = keep_physical_start
        ? ASYNC_SCALAR_RANGE_COLUMN_CNT : ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT;
    for (int64_t i = 0; i < start_column_cnt; ++i) {
      term_range.start_objs_[i] = source_objs[i];
    }
    projected_range.start_key_.assign(term_range.start_objs_, start_column_cnt);
  }
  if (OB_FAIL(ret)) {
  } else if (end_key.is_min_row()) {
    projected_range.end_key_.set_min_row();
  } else if (end_key.is_max_row()) {
    projected_range.end_key_.set_max_row();
  } else if (OB_ISNULL(source_objs = end_key.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null range end key", K(ret), K(range));
  } else {
    const int64_t end_column_cnt = keep_physical_end
        ? ASYNC_SCALAR_RANGE_COLUMN_CNT : ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT;
    for (int64_t i = 0; i < end_column_cnt; ++i) {
      term_range.end_objs_[i] = source_objs[i];
    }
    projected_range.end_key_.assign(term_range.end_objs_, end_column_cnt);
  }

  if (OB_SUCC(ret)) {
    projected_range.table_id_ = range.table_id_;
    projected_range.group_idx_ = range.group_idx_;
    projected_range.border_flag_ = range.border_flag_;
    if (!start_key.is_min_row() && !start_key.is_max_row()) {
      const common::ObObj &doc_id =
          start_key.get_obj_ptr()[ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT];
      if (doc_id.is_min_value()) {
        projected_range.border_flag_.set_inclusive_start();
      } else if (doc_id.is_max_value()) {
        projected_range.border_flag_.unset_inclusive_start();
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("doc id bounded async scalar range is not supported",
            K(ret), K(doc_id), K(range));
      }
    }
    if (OB_SUCC(ret) && !end_key.is_min_row() && !end_key.is_max_row()) {
      const common::ObObj &doc_id =
          end_key.get_obj_ptr()[ASYNC_SCALAR_RANGE_TERM_COLUMN_CNT];
      if (doc_id.is_max_value()) {
        projected_range.border_flag_.set_inclusive_end();
      } else if (doc_id.is_min_value()) {
        projected_range.border_flag_.unset_inclusive_end();
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("doc id bounded async scalar range is not supported",
            K(ret), K(doc_id), K(range));
      }
    }
  }
  return ret;
}

int ObDASSearchUtils::resolve_async_segment_domain(
    const ObDASIRScanCtDef &ir_ctdef,
    ObDASSearchCtx &search_ctx,
    uint64_t &index_table_id,
    int64_t &max_segment_cnt)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *inv_ctdef = ir_ctdef.get_inv_idx_scan_scalar_ctdef();
  ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
  index_table_id = common::OB_INVALID_ID;
  max_segment_cnt = 0;
  if (OB_UNLIKELY(!ir_ctdef.is_async_index_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected synchronous fulltext definition", K(ret), K(ir_ctdef.is_async_index_));
  } else if (OB_ISNULL(inv_ctdef) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async segment definition", K(ret), KP(inv_ctdef), KP(query_ctx));
  } else if (OB_UNLIKELY(!query_ctx->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async segment catalog is not initialized", K(ret), KPC(query_ctx));
  } else {
    index_table_id = inv_ctdef->ref_table_id_;
    const int64_t group_slot = search_ctx.get_async_group_slot();
    max_segment_cnt = group_slot >= 0
        ? query_ctx->get_max_segment_cnt(group_slot, index_table_id)
        : query_ctx->get_max_segment_cnt(index_table_id);
    if (OB_UNLIKELY(common::OB_INVALID_ID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async segment domain", K(ret), K(index_table_id), K(max_segment_cnt));
    }
  }
  return ret;
}

int ObDASSearchUtils::resolve_async_field_segment_domain(
    const ObDASIRScanCtDef &ir_ctdef,
    ObDASSearchCtx &search_ctx,
    ObDASAsyncBindingGuard &binding_guard,
    uint64_t &index_table_id,
    int64_t &segment_op_cnt)
{
  int ret = OB_SUCCESS;
  const uint64_t claimed_index_table_id = search_ctx.get_async_index_table_id();
  if (OB_FAIL(resolve_async_segment_domain(
          ir_ctdef, search_ctx, index_table_id, segment_op_cnt))) {
    LOG_WARN("failed to resolve async field segment domain", K(ret));
  } else if (common::OB_INVALID_ID != claimed_index_table_id) {
    if (OB_UNLIKELY(claimed_index_table_id != index_table_id)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("async field crosses claimed index domain",
               K(ret), K(claimed_index_table_id), K(index_table_id));
    } else {
      segment_op_cnt = 1;
    }
  } else {
    const int64_t group_slot = search_ctx.get_async_group_slot();
    binding_guard.set(index_table_id, group_slot, group_slot >= 0 && 1 >= segment_op_cnt);
  }
  return ret;
}

int ObDASSearchUtils::generate_async_compound_query_op(
    ObIDASSearchRtDef &rtdef,
    const ObIDASSearchCtDef &ctdef,
    const bool is_async_index,
    ObIDASSearchOp *pushdown_filter_op,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    AsyncCompoundOpGenerator generator,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASAsyncBindingGuard binding_guard(search_ctx);
  op = nullptr;
  if (OB_ISNULL(generator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null async compound query generator", K(ret));
  } else if (OB_UNLIKELY(rtdef.ctdef_ != &ctdef
                         || rtdef.op_type_ != ctdef.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async compound query definition does not match runtime definition",
             K(ret), KP(rtdef.ctdef_), KP(&ctdef),
             K(rtdef.op_type_), K(ctdef.op_type_));
  } else if (!is_async_index) {
    if (OB_FAIL(generator(rtdef, ctdef, lead_cost, search_ctx, op))) {
      LOG_WARN("failed to generate synchronous compound query", K(ret));
    }
  } else {
    ObDASAsyncIndexDomainAnalysis analysis;
    const uint64_t claimed_index_table_id = search_ctx.get_async_index_table_id();
    if (OB_FAIL(ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
            &ctdef, analysis))) {
      LOG_WARN("failed to analyze compound query async index domain", K(ret));
    } else if (common::OB_INVALID_ID != claimed_index_table_id) {
      if (OB_UNLIKELY(!analysis.is_single()
                      || analysis.index_table_id_ != claimed_index_table_id)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("compound query does not belong to claimed index domain",
                 K(ret), K(analysis), K(claimed_index_table_id));
      } else if (OB_NOT_NULL(pushdown_filter_op)
                 && !search_ctx.get_async_direct_bind()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async filter pushdown requires a direct segment binding",
                 K(ret), K(analysis), K(claimed_index_table_id));
      } else if (OB_FAIL(generator(rtdef, ctdef, lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate bound compound query subtree", K(ret), K(analysis));
      }
    } else if (ASYNC_INDEX_DOMAIN_SINGLE == analysis.type_) {
      ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
      ObSEArray<ObIDASSearchOp *, 4> segment_ops;
      const int64_t group_slot = search_ctx.get_async_group_slot();
      int64_t max_segment_cnt = 0;
      if (OB_ISNULL(query_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid compound query single-index claim context",
                 K(ret), KP(query_ctx), K(group_slot));
      } else if (FALSE_IT(max_segment_cnt = group_slot >= 0
                     ? query_ctx->get_max_segment_cnt(group_slot, analysis.index_table_id_)
                     : query_ctx->get_max_segment_cnt(analysis.index_table_id_))) {
      } else if (OB_NOT_NULL(pushdown_filter_op) && 1 < max_segment_cnt) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async filter pushdown crosses SegmentMerge", K(ret),
                 K(group_slot), K(max_segment_cnt), K(analysis));
      } else {
        binding_guard.set(analysis.index_table_id_, group_slot,
                          group_slot >= 0 && 1 >= max_segment_cnt);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX(1L, max_segment_cnt); ++i) {
        ObIDASSearchOp *segment_op = nullptr;
        if (OB_FAIL(generator(rtdef, ctdef, lead_cost, search_ctx, segment_op))) {
          LOG_WARN("failed to generate compound query segment subtree",
                   K(ret), K(i), K(analysis));
        } else if (OB_ISNULL(segment_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null compound query segment subtree", K(ret), K(i), K(analysis));
        } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
          LOG_WARN("failed to append compound query segment subtree", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(create_async_segment_merge_op(
              segment_ops, analysis.index_table_id_, ctdef.is_scoring(), search_ctx, op))) {
        LOG_WARN("failed to create whole compound query segment merge", K(ret), K(analysis));
      }
    } else if (ASYNC_INDEX_DOMAIN_MULTI == analysis.type_) {
      // A multi-field compound query owns its native Group-local BMM. Its capability
      // check guarantees that every scoring field has at most one Segment in this
      // Group before the filter is attached to that BMM.
      if (OB_FAIL(generator(rtdef, ctdef, lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate cross-index compound query", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported compound query async index domain", K(ret), K(analysis));
    }
  }
  if (OB_FAIL(ret)) {
    op = nullptr;
  }
  return ret;
}

int ObDASSearchUtils::create_async_segment_merge_op(
    const common::ObIArray<ObIDASSearchOp *> &segment_ops,
    const uint64_t index_table_id,
    const bool is_scoring,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&segment_merge_op,
    const bool force_merge,
    const ObDASAsyncSegmentMergeMode mode)
{
  int ret = OB_SUCCESS;
  ObDASAsyncSegmentMergeOp *merge_op = nullptr;
  segment_merge_op = nullptr;
  if (OB_UNLIKELY(segment_ops.empty() || common::OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment merge argument", K(ret), K(segment_ops.count()),
             K(index_table_id));
  } else if (1 == segment_ops.count()
             && search_ctx.get_async_group_slot() >= 0
             && !force_merge) {
    segment_merge_op = segment_ops.at(0);
  } else {
    ObDASAsyncSegmentMergeOpParam merge_param(
        segment_ops, ObDASAsyncSegmentDomain(
            index_table_id, search_ctx.get_async_group_slot()), force_merge, mode);
    merge_param.set_is_scoring(is_scoring);
    if (OB_FAIL(search_ctx.create_op(merge_param, merge_op))) {
      LOG_WARN("failed to create async segment merge op", K(ret), K(index_table_id));
    } else {
      segment_merge_op = merge_op;
    }
  }
  return ret;
}

int ObDASSearchUtils::create_async_segment_group_merge_op(
    const common::ObIArray<ObIDASSearchOp *> &group_ops,
    const bool is_scoring,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&group_merge_op)
{
  int ret = OB_SUCCESS;
  ObDASAsyncSegmentGroupMergeOp *merge_op = nullptr;
  group_merge_op = nullptr;
  if (OB_UNLIKELY(group_ops.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment group merge argument", K(ret),
             K(group_ops.count()));
  } else if (1 == group_ops.count()) {
    group_merge_op = group_ops.at(0);
  } else {
    ObDASAsyncSegmentGroupMergeOpParam merge_param(group_ops);
    merge_param.set_is_scoring(is_scoring);
    if (OB_FAIL(search_ctx.create_op(merge_param, merge_op))) {
      LOG_WARN("failed to create async segment group merge op", K(ret));
    } else {
      group_merge_op = merge_op;
    }
  }
  return ret;
}

int ObDASSearchUtils::create_token_search_op(
    const ObDASIRScanCtDef &ir_ctdef,
    ObDASIRScanRtDef &ir_rtdef,
    storage::ObBlockMaxScoreIterParam *block_max_param,
    const common::ObString &query_token,
    const double token_boost,
    const bool is_scoring,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&token_op)
{
  int ret = OB_SUCCESS;
  token_op = nullptr;
  if (ir_ctdef.is_async_index_) {
    ObDASAsyncTokenOp *async_token_op = nullptr;
    ObDASAsyncTokenOpParam token_param;
    token_param.set_is_scoring(is_scoring);
    token_param.ir_ctdef_ = &ir_ctdef;
    token_param.ir_rtdef_ = &ir_rtdef;
    token_param.index_table_id_ = search_ctx.get_async_index_table_id();
    token_param.group_slot_ = search_ctx.get_async_group_slot();
    token_param.direct_bind_ = search_ctx.get_async_direct_bind();
    token_param.token_boost_ = token_boost;
    token_param.query_token_ = query_token;
    if (OB_UNLIKELY(common::OB_INVALID_ID == token_param.index_table_id_
                    || (token_param.direct_bind_ && token_param.group_slot_ < 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing async token construction binding", K(ret),
               K(token_param.index_table_id_), K(token_param.group_slot_),
               K(token_param.direct_bind_));
    } else if (OB_FAIL(search_ctx.create_op(token_param, async_token_op))) {
      LOG_WARN("failed to create async token op", K(ret));
    } else {
      token_op = async_token_op;
    }
  } else {
    ObDASTokenOp *sync_token_op = nullptr;
    ObDASTokenOpParam token_param;
    token_param.set_is_scoring(is_scoring);
    token_param.ir_ctdef_ = &ir_ctdef;
    token_param.ir_rtdef_ = &ir_rtdef;
    token_param.block_max_param_ = block_max_param;
    token_param.token_boost_ = token_boost;
    token_param.query_token_ = query_token;
    token_param.use_rich_format_ = true;
    if (OB_FAIL(search_ctx.create_op(token_param, sync_token_op))) {
      LOG_WARN("failed to create synchronous token op", K(ret));
    } else {
      token_op = sync_token_op;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "share/vector/ob_i_vector.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{

namespace
{
int prepare_output_expr(
    ObExpr *expr,
    ObEvalCtx &eval_ctx,
    const int64_t capacity,
    common::ObDatum *&datums,
    common::ObIVector *&vector)
{
  int ret = OB_SUCCESS;
  datums = nullptr;
  vector = nullptr;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar range output expr", K(ret));
  } else if (expr->enable_rich_format()) {
    if (OB_FAIL(expr->init_vector_for_write(
        eval_ctx, expr->get_default_res_format(), capacity))) {
      LOG_WARN("failed to init async scalar range output vector", K(ret), K(capacity));
    } else if (OB_ISNULL(vector = expr->get_vector(eval_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null async scalar range output vector", K(ret));
    }
  } else if (OB_ISNULL(datums = expr->locate_datums_for_update(eval_ctx, capacity))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to locate async scalar range output datums", K(ret), K(capacity));
  }
  return ret;
}

void append_doc_ids_to_output(
    const uint64_t *doc_ids,
    const int64_t n,
    const int64_t offset,
    common::ObDatum *doc_id_datums,
    common::ObIVector *doc_id_vector)
{
  if (OB_NOT_NULL(doc_id_vector)) {
    if (common::VEC_FIXED == doc_id_vector->get_format()) {
      common::ObFixedLengthBase *fixed_vec =
          static_cast<common::ObFixedLengthBase *>(doc_id_vector);
      uint64_t *data = reinterpret_cast<uint64_t *>(fixed_vec->get_data());
      MEMCPY(data + offset, doc_ids, n * sizeof(uint64_t));
    } else {
      for (int64_t i = 0; i < n; ++i) {
        doc_id_vector->set_uint(offset + i, doc_ids[i]);
      }
    }
  } else {
    for (int64_t i = 0; i < n; ++i) {
      doc_id_datums[offset + i].set_uint(doc_ids[i]);
    }
  }
}

} // namespace

int ObDASAsyncScalarRangeOp::project_term_range(
    const common::ObNewRange &range,
    common::ObIAllocator &allocator,
    blocksstable::ObDatumRange &term_range)
{
  int ret = OB_SUCCESS;
  ObDASSearchUtils::AsyncScalarTermRange projected_range;
  term_range.reset();
  if (OB_FAIL(ObDASSearchUtils::project_async_scalar_term_range(
      range, projected_range))) {
    LOG_WARN("failed to project async scalar term range", K(ret), K(range));
  } else if (OB_FAIL(term_range.from_range(projected_range.range_, allocator, true))) {
    LOG_WARN("failed to convert async scalar scan range",
        K(ret), K(projected_range.range_));
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncScalarRangeOpParam &param =
      static_cast<const ObDASAsyncScalarRangeOpParam &>(op_param);
  if (OB_ISNULL(param.get_scan_ctdef()) || OB_ISNULL(param.get_scan_rtdef())
      || (param.get_direct_bind()
          && (common::OB_INVALID_ID == param.get_index_table_id()
              || param.get_group_slot() < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range op argument", K(ret),
        KP(param.get_scan_ctdef()), KP(param.get_scan_rtdef()),
        K(param.get_index_table_id()), K(param.get_group_slot()),
        K(param.get_direct_bind()));
  } else if (OB_UNLIKELY(
      (DAS_SCOPE_BASE == scope_ && DAS_ROWID_TYPE_UINT64 != get_rowid_type())
      || (DAS_SCOPE_NESTED_ASYNC == scope_ && DAS_ROWID_TYPE_UINT64 != get_rowid_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async scalar range op has unexpected rowid type", K(ret),
        K_(scope), "rowid_type", get_rowid_type());
  } else {
    scalar_ctdef_ = param.get_scan_ctdef();
    scalar_rtdef_ = param.get_scan_rtdef();
    index_table_id_ = param.get_index_table_id();
    group_slot_ = param.get_group_slot();
    direct_bind_ = param.get_direct_bind();
    // This producer materializes the shared NESTED RowID exprs from posting
    // values, but in BASE mode it writes only the child scan's doc_id expr.
    const ObDASRowIDScope &rowid_scope = search_ctx_.get_scope(scope_);
    const common::ObIArray<ObExpr *> *producer_exprs = &scalar_ctdef_->rowkey_exprs_;
    if (DAS_SCOPE_NESTED_ASYNC == scope_ || scalar_ctdef_->is_logical_table()) {
      producer_exprs = rowid_scope.rowid_exprs_;
    }
    if (OB_ISNULL(producer_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async scalar RowID producer exprs are null", K(ret), K_(scope));
    } else if (OB_FAIL(init_rowid_output_exprs(*producer_exprs))) {
      LOG_WARN("failed to initialize async scalar RowID output exprs", K(ret),
          K_(scope), "producer_expr_count", producer_exprs->count(),
          "scope_expr_count", OB_ISNULL(rowid_scope.rowid_exprs_)
              ? -1 : rowid_scope.rowid_exprs_->count());
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::init_inv_idx_scan_param()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar range ctdef/rtdef", K(ret),
        KP_(scalar_ctdef), KP_(scalar_rtdef));
  } else if (OB_FAIL(search_ctx_.get_related_tablet_id(
      scalar_ctdef_, inv_idx_tablet_id_))) {
    LOG_WARN("failed to get async scalar range index tablet id", K(ret),
        KPC_(scalar_ctdef));
  } else if (!inv_idx_scan_param_inited_) {
    // version_range_ selects the immutable segment; the regular scan param
    // retains the SQL snapshot for read-allowed checks.
    if (OB_FAIL(search_ctx_.init_scan_param(
        inv_idx_tablet_id_, scalar_ctdef_, scalar_rtdef_, inv_idx_scan_param_,
        false /* use_async_read_snapshot */))) {
      LOG_WARN("failed to init async scalar range table scan param", K(ret),
          K_(inv_idx_tablet_id));
    } else {
      inv_idx_scan_param_inited_ = true;
    }
  } else {
    ObIDASSearchOp::switch_tablet_id(
        search_ctx_.get_ls_id(), inv_idx_tablet_id_, inv_idx_scan_param_);
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::init_range_scan_param()
{
  int ret = OB_SUCCESS;
  range_scan_param_.reset();
  term_range_.reset();
  range_allocator_.reuse();
  query_flag_ = common::ObQueryFlag();
  query_flag_.set_use_block_cache();
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar ctdef/rtdef", K(ret),
        KP_(scalar_ctdef), KP_(scalar_rtdef));
  } else if (scalar_rtdef_->key_ranges_.empty()) {
    if (!scalar_ctdef_->enable_new_false_range_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty async scalar range collection", K(ret));
    }
  } else if (OB_UNLIKELY(!has_current_range())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current async scalar range index", K(ret),
        K_(current_range_idx), "range_count", scalar_rtdef_->key_ranges_.count());
  } else if (OB_FAIL(project_term_range(
      scalar_rtdef_->key_ranges_.at(current_range_idx_), range_allocator_, term_range_))) {
    LOG_WARN("failed to project async scalar term range", K(ret), K_(current_range_idx));
  } else {
    range_scan_param_.scan_range_ = &term_range_;
    range_scan_param_.tablet_id_ = inv_idx_tablet_id_;
    range_scan_param_.query_flag_ = query_flag_;
    range_scan_param_.allocator_ = &range_iter_allocator_;
    range_scan_param_.need_posting_value_ = DAS_SCOPE_NESTED_ASYNC == scope_;
    range_scan_param_.enable_doc_id_drain_ = DAS_SCOPE_BASE == scope_;
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::open_range_iter_from_tablet()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  const storage::ObTabletHandle *pinned_tablet_handle = nullptr;
  if (OB_ISNULL(tsc_service_) && OB_ISNULL(tsc_service_ = MTL(storage::ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null access service", K(ret));
  } else if (OB_UNLIKELY(!inv_idx_scan_param_inited_
      || !version_range_.is_valid() || !range_scan_param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range scan state", K(ret), K_(version_range),
        K_(range_scan_param), K_(inv_idx_scan_param_inited));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async index partition", K(ret));
  } else if (partition->ls_id_ != search_ctx_.get_ls_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async scalar range partition ls mismatch", K(ret), K(partition->ls_id_),
        "search_ls_id", search_ctx_.get_ls_id());
  } else if (OB_FAIL(partition->get_pinned_index_tablet_handle(
      inv_idx_tablet_id_, pinned_tablet_handle))) {
    LOG_WARN("failed to get pinned async index tablet", K(ret), K_(inv_idx_tablet_id));
  } else if (OB_FAIL(tsc_service_->async_index_range_scan(
      search_ctx_.get_ls_id(), pinned_tablet_handle, version_range_,
      inv_idx_scan_param_, range_scan_param_, range_iter_))) {
    LOG_WARN("failed to open async scalar range scan", K(ret),
        K_(inv_idx_tablet_id), K_(version_range));
  }
  if (OB_FAIL(ret)) {
    close_range_iter();
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::open_next_range_iter()
{
  int ret = OB_SUCCESS;
  close_range_iter();
  if (OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar rtdef", K(ret));
  } else if (++current_range_idx_ >= scalar_rtdef_->key_ranges_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(init_range_scan_param())) {
    LOG_WARN("failed to initialize next async scalar range", K(ret),
        K_(current_range_idx));
  } else if (OB_FAIL(open_range_iter_from_tablet())) {
    LOG_WARN("failed to open next async scalar range", K(ret),
        K_(current_range_idx));
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::advance_range_on_iter_end()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_next_range_iter())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to switch async scalar range iter", K(ret));
    }
  }
  return ret;
}

void ObDASAsyncScalarRangeOp::reset_nested_cursor()
{
  nested_key_list_reader_.reset();
  curr_nested_key_.reset();
  curr_nested_doc_id_ = 0;
  nested_row_ready_ = false;
}

void ObDASAsyncScalarRangeOp::close_range_iter()
{
  reset_nested_cursor();
  if (OB_NOT_NULL(range_iter_)) {
    range_iter_->~ObInvSSTableRangeIter();
    range_iter_allocator_.free(range_iter_);
    range_iter_ = nullptr;
  }
  range_iter_allocator_.reset();
  range_scan_param_.allocator_ = &range_iter_allocator_;
}

int ObDASAsyncScalarRangeOp::get_bitmap_input_range(
    bool &available, uint64_t &lo, uint64_t &hi, int64_t &estimated_count)
{
  int ret = OB_SUCCESS;
  available = false;
  lo = UINT64_MAX;
  hi = 0;
  estimated_count = 0;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  if (DAS_SCOPE_BASE != scope_ || DAS_ROWID_TYPE_UINT64 != get_rowid_type()
      || search_ctx_.is_logical_table() || !version_range_.is_valid()
      || OB_ISNULL(partition)) {
    // No proven physical input domain: keep the existing chunked layout.
  } else if (OB_FAIL(partition->get_segment_rowid_range(
                 index_table_id_, version_range_, available, lo, hi))) {
    LOG_WARN("failed to get bitmap input domain", K(ret), K_(index_table_id));
  } else if (available && OB_NOT_NULL(scalar_rtdef_)) {
    ObDASSearchCost cost;
    if (OB_FAIL(scalar_rtdef_->get_cost(search_ctx_, cost))) {
      LOG_WARN("failed to estimate bitmap density", K(ret));
    } else if (cost.is_valid()) {
      estimated_count = cost.cost();
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::do_open()
{
  int ret = OB_SUCCESS;
  current_range_idx_ = 0;
  is_empty_segment_ = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range op is not initialized", K(ret));
  } else if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init async scalar range table scan param", K(ret));
  } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                 index_table_id_, inv_idx_tablet_id_, group_slot_,
                 version_range_, is_empty_segment_))) {
    LOG_WARN("failed to resolve direct async scalar range segment", K(ret),
        K_(index_table_id), K_(inv_idx_tablet_id), K_(group_slot));
  } else if (is_empty_segment_) {
    // No physical input in this partition and Group.
  } else if (OB_FAIL(init_range_scan_param())) {
    LOG_WARN("failed to init async scalar range scan param", K(ret));
  } else if (!has_current_range()) {
    // The caller may refresh key_ranges_ with a nonempty set before rescan.
  } else if (!direct_bind_) {
    LOG_INFO("[ASYNC_INDEX_QUERY] scalar range op open(lazy)", K_(inv_idx_tablet_id));
  } else if (OB_FAIL(open_range_iter_from_tablet())) {
    LOG_WARN("failed to open async scalar range iter", K(ret));
  }
  if (OB_FAIL(ret)) {
    close_range_iter();
    if (direct_bind_) {
      version_range_.reset();
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range op is not initialized", K(ret));
  } else {
    close_range_iter();
    version_range_.reset();
    batch_allocator_.reuse();
    current_range_idx_ = 0;
    is_empty_segment_ = false;
    if (OB_FAIL(init_inv_idx_scan_param())) {
      LOG_WARN("failed to init async scalar range table scan param on rescan", K(ret));
    } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                   index_table_id_, inv_idx_tablet_id_, group_slot_,
                   version_range_, is_empty_segment_))) {
      LOG_WARN("failed to resolve direct async scalar range segment on rescan", K(ret),
          K_(index_table_id), K_(inv_idx_tablet_id), K_(group_slot));
    } else if (is_empty_segment_) {
      // No physical input in this partition and Group.
    } else if (OB_FAIL(init_range_scan_param())) {
      LOG_WARN("failed to init async scalar range param on rescan", K(ret));
    } else if (!has_current_range()) {
      // Keep the leaf reusable after the caller refreshes key_ranges_.
    } else if (direct_bind_ && OB_FAIL(open_range_iter_from_tablet())) {
      LOG_WARN("failed to open direct async scalar range iter on rescan", K(ret),
          K_(version_range));
    } else if (!direct_bind_) {
      LOG_INFO("[ASYNC_INDEX_QUERY] scalar range op rescan(lazy)", K_(inv_idx_tablet_id));
    }
    if (OB_FAIL(ret) && direct_bind_) {
      close_range_iter();
      version_range_.reset();
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range op is not initialized", K(ret));
  } else if (OB_UNLIKELY(direct_bind_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct async scalar range rejects segment rescan", K(ret),
        K_(index_table_id), K_(group_slot), K(range));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range segment", K(ret), K(range));
  } else {
    close_range_iter();
    version_range_ = range;
    current_range_idx_ = 0;
    is_empty_segment_ = false;
    if (OB_FAIL(init_range_scan_param())) {
      LOG_WARN("failed to reset async scalar ranges for segment", K(ret), K(range));
    } else if (!has_current_range()) {
      // Empty runtime ranges do not bind a physical segment.
    } else if (OB_FAIL(open_range_iter_from_tablet())) {
      LOG_WARN("failed to open async scalar range segment", K(ret), K(range));
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::position_next_nested_posting()
{
  int ret = OB_SUCCESS;
  const common::ObDatum *doc_id = nullptr;
  const common::ObDatum *nested_key_list = nullptr;
  reset_nested_cursor();
  if (OB_ISNULL(range_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range iter is not opened", K(ret));
  }
  while (OB_SUCC(ret) && !nested_row_ready_) {
    const int next_ret = range_iter_->next();
    if (OB_ITER_END == next_ret) {
      if (OB_FAIL(open_next_range_iter()) && OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to switch nested async scalar range iter", K(ret));
      }
    } else if (OB_SUCCESS != next_ret) {
      ret = next_ret;
      LOG_WARN("failed to advance async scalar range iter", K(ret));
    } else if (OB_FAIL(range_iter_->get_cur_id(doc_id))) {
      LOG_WARN("failed to get nested posting doc id", K(ret));
    } else if (OB_ISNULL(doc_id) || OB_UNLIKELY(doc_id->is_null()
        || doc_id->is_nop() || sizeof(uint64_t) != doc_id->len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid nested posting doc id", K(ret), KP(doc_id));
    } else if (OB_FAIL(range_iter_->get_cur_nested_key_list(nested_key_list))) {
      LOG_WARN("failed to get nested key list", K(ret));
    } else if (OB_ISNULL(nested_key_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null nested key list pointer", K(ret));
    } else if (nested_key_list->is_null()) {
      // A top-level field has no row in nested scope.
    } else if (OB_FAIL(nested_key_list_reader_.init(*nested_key_list))) {
      LOG_WARN("failed to init nested key list reader", K(ret), KPC(nested_key_list));
    } else if (OB_FAIL(nested_key_list_reader_.first(curr_nested_key_))) {
      LOG_WARN("failed to get first nested key", K(ret));
    } else if (OB_UNLIKELY(curr_nested_key_.is_null() || curr_nested_key_.is_nop())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid first nested key", K(ret), K_(curr_nested_key));
    } else {
      curr_nested_doc_id_ = doc_id->get_uint64();
      nested_row_ready_ = true;
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::get_output_exprs(
    const common::ObIArray<ObExpr *> *&output_exprs)
{
  int ret = OB_SUCCESS;
  output_exprs = get_rowid_output_exprs();
  const int64_t expected_count = DAS_SCOPE_NESTED_ASYNC == scope_ ? 2 : 1;
  if (OB_ISNULL(output_exprs) || OB_UNLIKELY(output_exprs->count() != expected_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async scalar range output expr count", K(ret), K_(scope),
        "expr_count", OB_ISNULL(output_exprs) ? 0 : output_exprs->count());
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::get_next_doc_ids(
    const int64_t capacity,
    const uint64_t *&doc_ids,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  bool batch_ready = false;
  doc_ids = nullptr;
  count = 0;
  if (OB_UNLIKELY(DAS_SCOPE_BASE != scope_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("raw doc-id drain is only supported by base range", K(ret), K_(scope));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw doc-id drain capacity", K(ret), K(capacity));
  } else if (is_empty_segment_ || !has_current_range()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(range_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened for raw doc-id drain", K(ret));
  } else {
    while (OB_SUCC(ret) && !batch_ready) {
      const int next_ret = range_iter_->get_next_doc_ids(capacity, doc_ids, count);
      if (OB_ITER_END == next_ret) {
        ret = advance_range_on_iter_end();
      } else if (OB_SUCCESS != next_ret) {
        ret = next_ret;
        LOG_WARN("failed to get next doc ids", K(ret), K(capacity));
      } else {
        batch_ready = true;
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::do_next_rowids(
    const int64_t max_batch_size,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  common::ObDatum *doc_id_datums = nullptr;
  common::ObDatum *nested_key_datums = nullptr;
  common::ObIVector *doc_id_vector = nullptr;
  common::ObIVector *nested_key_vector = nullptr;
  const common::ObIArray<ObExpr *> *output_exprs = nullptr;
  count = 0;
  batch_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range op is not initialized", K(ret));
  } else if (OB_UNLIKELY(max_batch_size <= 0 || max_batch_size > this->max_batch_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range batch size", K(ret), K(max_batch_size),
        "op_max_batch_size", this->max_batch_size());
  } else if (is_empty_segment_ || !has_current_range()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(range_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar range iter is not opened", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx());
    guard.set_batch_size(max_batch_size);
    if (OB_FAIL(get_output_exprs(output_exprs))) {
      LOG_WARN("failed to prepare async scalar range output", K(ret));
    } else if (OB_FAIL(prepare_output_expr(
        output_exprs->at(0), eval_ctx(), max_batch_size,
        doc_id_datums, doc_id_vector))) {
      LOG_WARN("failed to prepare async scalar doc id output", K(ret));
    } else if (DAS_SCOPE_NESTED_ASYNC == scope_ && OB_FAIL(prepare_output_expr(
        output_exprs->at(1), eval_ctx(), max_batch_size,
        nested_key_datums, nested_key_vector))) {
      LOG_WARN("failed to prepare async scalar nested key output", K(ret));
    } else if (DAS_SCOPE_BASE == scope_) {
      const uint64_t *doc_ids = nullptr;
      if (OB_FAIL(get_next_doc_ids(max_batch_size, doc_ids, count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get async scalar doc ids", K(ret), K(max_batch_size));
        }
      } else {
        append_doc_ids_to_output(doc_ids, count, 0, doc_id_datums, doc_id_vector);
      }
    } else {
      while (OB_SUCC(ret) && count < max_batch_size) {
        if (!nested_row_ready_ && OB_FAIL(position_next_nested_posting())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to position nested async scalar range row", K(ret));
          }
        } else if (OB_UNLIKELY(curr_nested_key_.is_null() || curr_nested_key_.is_nop()
            || curr_nested_key_.len_ <= 0 || OB_ISNULL(curr_nested_key_.ptr_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async scalar nested key", K(ret), K_(curr_nested_key));
        } else if (OB_NOT_NULL(nested_key_vector)) {
          nested_key_vector->set_payload(
              count, curr_nested_key_.ptr_, curr_nested_key_.len_);
        } else if (OB_FAIL(nested_key_datums[count].deep_copy(
                       curr_nested_key_, batch_allocator_))) {
          LOG_WARN("failed to copy async scalar nested key", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_NOT_NULL(doc_id_vector)) {
            doc_id_vector->set_uint(count, curr_nested_doc_id_);
          } else {
            doc_id_datums[count].set_uint(curr_nested_doc_id_);
          }
          ++count;
          int next_ret = nested_key_list_reader_.next(curr_nested_key_);
          if (OB_ITER_END == next_ret) {
            reset_nested_cursor();
          } else if (OB_SUCCESS != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to advance nested key list", K(ret));
          }
        }
      }
    }
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && count > 0) {
      for (int64_t i = 0; i < output_exprs->count(); ++i) {
        output_exprs->at(i)->set_evaluated_projected(eval_ctx());
      }
    }
  }
  INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  return ret;
}

int ObDASAsyncScalarRangeOp::do_close()
{
  int ret = OB_SUCCESS;
  close_range_iter();
  range_scan_param_.reset();
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_scan_param_inited_ = false;
  tsc_service_ = nullptr;
  term_range_.reset();
  range_allocator_.reset();
  batch_allocator_.reset();
  range_iter_allocator_.reset();
  version_range_.reset();
  index_table_id_ = common::OB_INVALID_ID;
  group_slot_ = -1;
  direct_bind_ = false;
  is_empty_segment_ = false;
  inv_idx_tablet_id_.reset();
  scalar_ctdef_ = nullptr;
  scalar_rtdef_ = nullptr;
  current_range_idx_ = 0;
  is_inited_ = false;
  return ret;
}

} // namespace sql
} // namespace oceanbase

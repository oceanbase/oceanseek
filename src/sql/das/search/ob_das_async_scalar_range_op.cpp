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
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "lib/json_type/ob_json_tree.h"

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
  // from_rowkey() keeps store_rowkey_ as a pointer to the source ObObj array.
  // Copy the projection into allocator first, as the row-count estimate path does,
  // so the pointer does not refer to this temporary.
  common::ObNewRange stable_range;
  term_range.reset();
  if (OB_FAIL(ObDASSearchUtils::project_async_scalar_term_range(
      range, projected_range))) {
    LOG_WARN("failed to project async scalar term range", K(ret), K(range));
  } else if (OB_FAIL(common::deep_copy_range(
      allocator, projected_range.range_, stable_range))) {
    LOG_WARN("deep copy async scalar term range failed",
        K(ret), K(projected_range.range_));
  } else if (OB_FAIL(term_range.from_range(stable_range, allocator, true))) {
    LOG_WARN("failed to convert async scalar scan range", K(ret), K(stable_range));
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
  } else if (active_ranges().empty()) {
    if (!has_sort_ranges_ && !scalar_ctdef_->enable_new_false_range_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty async scalar range collection", K(ret));
    }
  } else if (OB_UNLIKELY(!has_current_range())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current async scalar range index", K(ret),
        K_(current_range_idx), "range_count", active_ranges().count());
  } else if (OB_FAIL(project_term_range(
      active_ranges().at(current_range_idx_), range_allocator_, term_range_))) {
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
  } else if (++current_range_idx_ >= active_ranges().count()) {
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
  reset_sort_ranges();
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
  reset_sort_ranges();
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
  reset_sort_ranges();
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
  read_started_ = true;
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
  reset_sort_ranges();
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

namespace
{
/**
 * Check whether range belongs to the sort field before bottom pruning.
 * range: e.g. the index range for 10 < price < 100.
 * column: the main-table column ordinal, e.g. price's ordinal.
 * path: nullptr checks only column; SQL NULL for price;
 *       encoded ($.price, NUMBER) for numeric properties.price.
 *
 * 10 < price < 100, ORDER BY price:
 *   column=price's ordinal, path points to SQL NULL -> true.
 *   range for 10 < stock < 100 -> false.
 * 10 < properties.price < 100, ORDER BY properties.price:
 *   column=properties' ordinal, path=encoded ($.price, NUMBER) -> true.
 *   range for properties.stock or "10" < properties.price < "100" -> false.
 */
bool matches_sort_range_prefix(const ObNewRange &range, int64_t column, const ObObj *path)
{
  bool matches = false;
  if (range.start_key_.get_obj_cnt() == share::SEARCH_INDEX_KEY_CNT
      && range.end_key_.get_obj_cnt() == share::SEARCH_INDEX_KEY_CNT) {
    const ObObj *left = range.start_key_.get_obj_ptr();
    const ObObj *right = range.end_key_.get_obj_ptr();
    // check range column is the same as the sort column
    matches = left[share::SEARCH_INDEX_COL_IDX].is_int()
        && right[share::SEARCH_INDEX_COL_IDX].is_int()
        && left[share::SEARCH_INDEX_COL_IDX].get_int() == column
        && right[share::SEARCH_INDEX_COL_IDX].get_int() == column;
    if (matches && path != nullptr) {
      // check range path is the same as the sort path
      const ObObj &left_path = left[share::SEARCH_INDEX_PATH];
      const ObObj &right_path = right[share::SEARCH_INDEX_PATH];
      matches = path->is_null() ? left_path.is_null() && right_path.is_null()
          : path->is_varbinary() && left_path.is_varbinary() && right_path.is_varbinary()
            && left_path.get_string() == path->get_string()
            && right_path.get_string() == path->get_string();
    }
  }
  return matches;
}

/** @brief Only value-domain failures from optional boundary encoding may disable pruning. */
bool is_unrepresentable_sort_bottom(int error)
{
  return error == OB_NOT_SUPPORTED || error == OB_NUMERIC_OVERFLOW
      || error == OB_INTEGER_PRECISION_OVERFLOW || error == OB_DECIMAL_PRECISION_OVERFLOW
      || error == OB_DATA_OUT_OF_RANGE || error == OB_ERR_INCORRECT_STRING_VALUE;
}

/**
 * @brief Convert the first sort value in bottom to a search-index range key.
 *
 * Example: sort properties.price ASC, bottom [30, shard_doc].
 * Resolve properties to COL_IDX, encode the numeric $.price path, then encode 30.
 * An existing range must match that column and typed path before the value is encoded.
 * A stock range, a string price range, or an object/array bottom leaves usable=false.
 * @param[in] ctx Search context supplying the sort definition and expression state.
 * @param[in] sort_bottom Borrowed non-NULL leading TopN key; read only during this call.
 * @param[in] ranges Original, read-only query ranges of the current leaf.
 * @param[in] alloc Owns temporary JSON parsing state and encoded term payloads.
 * @param[out] column Schema store-column ordinal used by the search index.
 * @param[out] path Encoded JSON object path, or SQL NULL for an ordinary scalar column.
 * @param[out] value Binary-encoded boundary value; its storage belongs to alloc.
 * @param[out] usable True only when all term components are safe for range comparison.
 * @return OB_SUCCESS for inapplicable or unrepresentable bounds; other preparation errors propagate.
 */
int make_sort_bottom_term(ObDASSearchCtx &ctx, const ObDatum &sort_bottom,
                          const ObIArray<ObNewRange> &ranges, ObIAllocator &alloc,
                          int64_t &column, ObObj &path, ObObj &value, bool &usable)
{
  int ret = OB_SUCCESS;
  usable = false;
  const char *skip_reason = "none";
  bool can_encode = true;
  uint64_t column_id = OB_INVALID_ID;
  const ObDASHybridSearchSortCtDef &sort_ctdef = *ctx.get_sort_info().sort_ctdef_;
  ObExpr *first = sort_ctdef.sort_exprs_.at(0);
  ObExpr *base = first;
  ObExpr *path_expr = nullptr;
  ObIJsonBase *json_value = nullptr;
  ObString json_path;
  ObString encoded_path;
  ObString encoded_value;
  const bool json = first->datum_meta_.type_ == ObJsonType;
  if (json) {
    ObExpr *extract = first;
    // The resolver normalizes JSON null to SQL NULL. Unwrap only this exact
    // normalization for index identification; bottom still uses the full sort expression.
    if (first->type_ == T_FUN_SYS_NULLIF) {
      if (first->arg_cnt_ != 2 || OB_ISNULL(first->args_)
          || OB_ISNULL(first->args_[0]) || OB_ISNULL(first->args_[1])
          || !first->args_[1]->is_const_expr()
          || first->args_[1]->datum_meta_.type_ != ObJsonType) {
        can_encode = false;
        skip_reason = "JSON NULLIF does not have a constant JSON second argument";
      } else {
        ObExpr *null_expr = first->args_[1];
        ObDatum *null_datum = nullptr;
        ObIJsonBase *null_value = nullptr;
        ObEvalCtx &eval_ctx = *ctx.get_eval_ctx();
        ObCollationType null_cs = null_expr->datum_meta_.cs_type_;
        if (OB_FAIL(null_expr->eval(eval_ctx, null_datum))) {
          LOG_WARN("failed to evaluate JSON sort normalization constant", K(ret));
        } else if (OB_ISNULL(null_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("JSON sort normalization constant datum is null", K(ret));
        } else if (null_datum->is_null()) {
          // get_json_val also maps SQL NULL to JSON null; exclude it before conversion.
          can_encode = false;
          skip_reason = "JSON NULLIF second argument is SQL NULL";
        } else if (OB_FAIL(ObJsonExprHelper::get_json_val(*null_datum, eval_ctx.exec_ctx_, null_expr,
                           &alloc, ObJsonType, null_cs, null_value))) {
          LOG_WARN("failed to read JSON sort normalization constant", K(ret));
        } else if (OB_ISNULL(null_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("JSON sort normalization value is null", K(ret));
        } else if (null_value->json_type() != ObJsonNodeType::J_NULL) {
          can_encode = false;
          skip_reason = "JSON NULLIF second argument is not JSON null";
        } else {
          extract = first->args_[0];
        }
      }
    }
    if (OB_FAIL(ret) || !can_encode) {
    } else if (extract->type_ != T_FUN_SYS_JSON_EXTRACT || extract->arg_cnt_ != 2
               || OB_ISNULL(extract->args_) || OB_ISNULL(extract->args_[0])
               || OB_ISNULL(extract->args_[1])) {
      can_encode = false;
      skip_reason = "JSON sort expression is not a two-argument JSON_EXTRACT";
    } else {
      base = extract->args_[0];
      if (!extract->args_[1]->is_const_expr()) {
        can_encode = false;
        skip_reason = "JSON path expression is not constant";
      } else {
        path_expr = extract->args_[1];
      }
    }
  }
  if (OB_SUCC(ret) && can_encode) {
    const ObDASScanCtDef &scan = sort_ctdef.sort_value_scan_ctdef_;
    for (int64_t i = 0; i < scan.pd_expr_spec_.access_exprs_.count(); ++i) {
      if (scan.pd_expr_spec_.access_exprs_.at(i) == base) {
        column_id = scan.access_column_ids_.at(i);
      }
    }
    if (column_id == OB_INVALID_ID) {
      can_encode = false;
      skip_reason = "sort base expression is absent from lookup access expressions";
    } else {
      // Reuse the schema store-column ordinal in read-info instead of introducing RPC-only column metadata.
      const storage::ObTableReadInfo &read_info = scan.table_param_.get_read_info();
      const common::ObIArray<share::schema::ObColDesc> &columns = read_info.get_columns_desc();
      column = -1;
      for (int64_t i = 0; i < columns.count(); ++i) {
        if (columns.at(i).col_id_ == column_id) {
          column = read_info.get_memtable_columns_index().at(i);
        }
      }
      can_encode = column >= 0;
      if (!can_encode) {
        skip_reason = "sort column has no valid memtable column index";
      }
    }
  }
  if (OB_SUCC(ret) && can_encode) {
    // Check the column first. A range on another column needs no path evaluation or bottom conversion.
    can_encode = false;
    for (int64_t i = 0; !can_encode && i < ranges.count(); ++i) {
      can_encode = matches_sort_range_prefix(ranges.at(i), column, nullptr);
    }
    if (!can_encode) {
      skip_reason = "no range has matching key counts and sort column at both endpoints";
    }
  }
  if (OB_SUCC(ret) && can_encode) {
    if (json) {
      ObEvalCtx &eval_ctx = *ctx.get_eval_ctx();
      ObDatum *path_value = nullptr;
      ObCollationType cs = first->datum_meta_.cs_type_;
      if (OB_FAIL(path_expr->eval(eval_ctx, path_value))) {
        LOG_WARN("failed to evaluate hybrid sort JSON path", K(ret));
      } else if (OB_ISNULL(path_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hybrid sort JSON path datum is null", K(ret));
      } else if (path_value->is_null()) {
        can_encode = false;
        skip_reason = "JSON path evaluates to SQL NULL";
      } else if (OB_FAIL(ObJsonExprHelper::get_json_val(sort_bottom, eval_ctx.exec_ctx_, first,
                                                       &alloc, ObJsonType, cs, json_value))) {
        LOG_WARN("failed to get hybrid sort JSON value", K(ret));
      }
      if (OB_SUCC(ret) && can_encode) {
        json_path = path_value->get_string();
        if (OB_ISNULL(json_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hybrid sort JSON bottom value is null", K(ret));
        } else {
          const ObJsonNodeType type = json_value->json_type();
          // JSON null, objects and arrays still participate in full sorting, but do not
          // become index bounds here. SQL NULL was excluded before calling this helper.
          if (type != ObJsonNodeType::J_INT && type != ObJsonNodeType::J_UINT
              && type != ObJsonNodeType::J_DOUBLE && type != ObJsonNodeType::J_DECIMAL
              && type != ObJsonNodeType::J_STRING && type != ObJsonNodeType::J_BOOLEAN) {
            can_encode = false;
            skip_reason = "JSON bottom type is not a supported scalar";
          }
        }
      }
      if (OB_SUCC(ret) && can_encode) {
        ObJsonPath parsed(json_path, &alloc);
        parsed.is_mysql_ = true;
        if (OB_FAIL(parsed.parse_path())) {
          LOG_WARN("failed to parse hybrid sort JSON path", K(ret));
        }
        ObSEArray<share::ObSearchIndexPathEncoder::JsonPathItem, 8, ObIAllocator &> items(
            OB_MALLOC_NORMAL_BLOCK_SIZE, alloc);
        // Accept paths such as $.price or $.product.price; skip array steps and wildcard steps.
        for (JsonPathIterator it = parsed.begin(); OB_SUCC(ret) && it != parsed.end(); ++it) {
          const ObJsonPathNodeType type = (*it)->get_node_type();
          if (type == JPN_MEMBER) {
            ObPathMember member = static_cast<ObJsonPathBasicNode *>(*it)->get_object();
            if (OB_SUCC(ret) && OB_FAIL(items.push_back(share::ObSearchIndexPathEncoder::make_object_path(
                ObString(member.len_, member.object_name_))))) {
              LOG_WARN("failed to append hybrid sort JSON path item", K(ret));
            }
          } else if (type != JPN_ROOT) {
            can_encode = false;
            skip_reason = "JSON path contains an unsupported node";
            break;
          }
        }
        if (OB_SUCC(ret) && can_encode) {
          if (OB_FAIL(share::ObSearchIndexPathEncoder::encode_path(alloc, items, json_value->json_type(), encoded_path))) {
            if (ret == OB_ERR_TOO_LONG_KEY_LENGTH) {
              can_encode = false;
              skip_reason = "encoded JSON path exceeds index key length";
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to encode hybrid sort bottom path", K(ret));
            }
          } else {
            path.set_varchar(encoded_path);
            path.set_collation_type(CS_TYPE_BINARY);
          }
        }
      }
    } else {
      path.set_null(); // Ordinary scalar index keys have a SQL NULL path.
    }
  }
  if (OB_SUCC(ret) && can_encode) {
    // Matching the JSON column alone is not enough: price/number and price/string have different paths.
    can_encode = false;
    for (int64_t i = 0; !can_encode && i < ranges.count(); ++i) {
      can_encode = matches_sort_range_prefix(ranges.at(i), column, &path);
    }
    if (!can_encode) {
      skip_reason = "no range matches the sort column and encoded path/type";
    }
  }

  if (OB_SUCC(ret) && can_encode) {
    // JSON scalar type is part of path. Never compare another path/type's range
    // with this value, and retain conservative length checks for string bounds.
    if (json) {
      if (json_value->json_type() == ObJsonNodeType::J_STRING
          && !share::ObSearchIndexValueEncoder::string_safety_to_compare(
               ObString(json_value->get_data_length(), json_value->get_data()))) {
        can_encode = false;
        skip_reason = "JSON string bottom exceeds safe comparison length";
      } else if (ObIJsonBase::is_json_number_type(json_value->json_type())) {
        number::ObNumber number;
        if (OB_FAIL(json_value->to_number(&alloc, number))) {
          skip_reason = "JSON bottom cannot be converted to NUMBER";
        } else if (json_value->json_type() == ObJsonNodeType::J_DOUBLE) {
          // A successful decimal conversion may still underflow. Require the
          // converted number to recover this DOUBLE before using it as a bound.
          ObJsonDecimal decimal(number);
          double restored = 0;
          if (OB_FAIL(decimal.to_double(restored))) {
            skip_reason = "JSON DOUBLE bottom cannot be restored from NUMBER";
          } else if (restored != json_value->get_double()) {
            can_encode = false;
            skip_reason = "JSON DOUBLE bottom changes after decimal conversion";
          }
        }
        if (OB_SUCC(ret) && can_encode
            && OB_FAIL(share::ObSearchIndexValueEncoder::encode_number(alloc, number, encoded_value))) {
          skip_reason = "JSON numeric bottom cannot be encoded";
        }
      } else if (OB_FAIL(share::ObSearchIndexValueEncoder::encode_value(alloc, json_value, encoded_value))) {
        skip_reason = "JSON scalar bottom cannot be encoded";
      }
    } else {
      if (ob_is_string_type(first->datum_meta_.type_)
          && !share::ObSearchIndexValueEncoder::string_safety_to_compare(sort_bottom.get_string())) {
        can_encode = false;
        skip_reason = "string bottom exceeds safe comparison length";
      } else if (OB_FAIL(share::ObSearchIndexValueEncoder::encode_value(alloc, sort_bottom, first->obj_meta_, encoded_value))) {
        skip_reason = "scalar bottom cannot be encoded";
      }
    }
    if (OB_FAIL(ret)) {
      if (is_unrepresentable_sort_bottom(ret)) {
        can_encode = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to encode hybrid sort bottom", K(ret), K(column), K(skip_reason));
      }
    }
    if (OB_SUCC(ret) && can_encode && encoded_value.length() <= share::SEARCH_INDEX_VALUE_LENGTH) {
      value.set_varchar(encoded_value);
      value.set_collation_type(CS_TYPE_BINARY);
      usable = true;
    } else if (OB_SUCC(ret) && can_encode) {
      skip_reason = "encoded bottom value exceeds index value length";
    }
  }
  if (OB_SUCC(ret) && !usable) {
    LOG_TRACE("hybrid sort bottom skipped range", K(skip_reason), K(first->type_),
              K(first->datum_meta_), K(first->arg_cnt_), K(column_id), K(column), K(json_path),
              "json_type", json_value == nullptr ? ObJsonNodeType::J_MAX_TYPE : json_value->json_type(),
              K(path), K(ranges));
  }
  return ret;
}

} // namespace

int ObDASAsyncScalarRangeOp::check_sort_range_change_scalar_range(
    int64_t column, const ObObj &path, const ObObj &value, bool ascending,
    ObObj *cut_objects, ObRowkey &cut, bool &changed)
{
  int ret = OB_SUCCESS;
  changed = false;
  const ObIArray<ObNewRange> &original = scalar_rtdef_->key_ranges_;
  cut_objects[0].set_int(column);
  cut_objects[1] = path;
  cut_objects[2] = value;
  // Include all documents equal to the first key for later sort-key comparison.
  if (ascending) {
    cut_objects[3].set_max_value();
  } else {
    cut_objects[3].set_min_value();
  }
  cut.assign(cut_objects, 4);
  // Check the original endpoints before allocating private range storage.
  for (int64_t i = 0; OB_SUCC(ret) && !changed && i < original.count(); ++i) {
    const ObNewRange &range = original.at(i);
    if (matches_sort_range_prefix(range, column, &path)) {
      int cmp = 0;
      if (OB_FAIL(cut.compare(ascending ? range.end_key_ : range.start_key_, cmp))) {
        LOG_WARN("failed to compare TopN threshold with range endpoint", K(ret),
                 "range_index", i, K(column), K(ascending));
      } else {
        changed = ascending ? cmp < 0 : cmp > 0;
      }
    }
  }
  if (OB_FAIL(ret) || !changed) {
    LOG_TRACE("hybrid sort bottom left original ranges", K(ret), K_(index_table_id), K(changed),
              "reason", OB_SUCCESS == ret ? "bottom does not tighten original ranges" : "range preparation failed",
              K(column), K(path), K(value), K(original));
  }
  return ret;
}

int ObDASAsyncScalarRangeOp::bind_new_range_for_scalar_range(
    int64_t column, const ObObj &path, bool ascending, const ObRowkey &cut, bool &applied)
{
  int ret = OB_SUCCESS;
  applied = false;
  const ObIArray<ObNewRange> &original = scalar_rtdef_->key_ranges_;
  sort_ranges_.reset();
  sort_range_allocator_.reuse();
  // Intersect matching ranges and copy other ranges without modifying the runtime query ranges.
  for (int64_t i = 0; OB_SUCC(ret) && i < original.count(); ++i) {
    ObNewRange range = original.at(i);
    bool empty = false;
    if (matches_sort_range_prefix(range, column, &path)) {
      int cmp = 0;
      if (OB_FAIL(cut.compare(ascending ? range.end_key_ : range.start_key_, cmp))) {
        LOG_WARN("failed to compare TopN threshold with range endpoint", K(ret), "range_index", i);
      } else if ((ascending && cmp < 0) || (!ascending && cmp > 0)) {
        if (ascending) {
          range.end_key_ = cut;
          range.border_flag_.set_inclusive_end();
        } else {
          range.start_key_ = cut;
          range.border_flag_.set_inclusive_start();
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(range.start_key_.compare(range.end_key_, cmp))) {
        LOG_WARN("failed to compare pruned range endpoints", K(ret), "range_index", i);
      }
      empty = cmp > 0 || (cmp == 0
          && (!range.border_flag_.inclusive_start() || !range.border_flag_.inclusive_end()));
    }
    if (OB_SUCC(ret) && !empty) {
      ObNewRange owned;
      if (OB_FAIL(common::deep_copy_range(sort_range_allocator_, range, owned))) {
        LOG_WARN("failed to copy hybrid sort scan range", K(ret), "range_index", i, K_(index_table_id));
      } else if (OB_FAIL(sort_ranges_.push_back(owned))) {
        LOG_WARN("failed to append hybrid sort scan range", K(ret), "range_index", i, K_(index_table_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
    sort_ranges_.reset();
    sort_range_allocator_.reuse();
  } else {
    // An empty installed set must end the leaf instead of falling back to the original ranges.
    // After closing the old iterator, initialization/reopen errors propagate to the caller.
    has_sort_ranges_ = true;
    close_range_iter();
    current_range_idx_ = 0;
    if (OB_FAIL(init_range_scan_param())) {
      LOG_WARN("failed to initialize pruned range scan", K(ret), K_(index_table_id), K_(current_range_idx));
    } else if (has_current_range() && OB_FAIL(open_range_iter_from_tablet())) {
      LOG_WARN("failed to reopen pruned range scan", K(ret), K_(inv_idx_tablet_id), K_(current_range_idx));
    }
    applied = OB_SUCC(ret);
  }
  return ret;
}

/**
 * Narrow this leaf's existing ranges using the last row currently retained by a full TopN.
 * This changing bottom value is separate from the request's fixed search_after values.
 *
 * Examples with bottom price=30:
 *   price ASC:  10 < price < 100 -> 10 < price <= 30
 *   price DESC NULLS LAST: 10 < price < 100 -> 30 <= price < 100
 *   price DESC NULLS FIRST: unchanged, keeping any SQL NULL candidates
 *   price ASC:  price < 20      -> unchanged (the original range is already narrower)
 * Keep price=30: another row with that price may win on the next sort key or __shard_doc.
 *
 * Composite operators select which filter branches may receive the bound.
 * This function checks the leaf state, matches the index column/path, and intersects its ranges.
 * applied=true also covers an empty intersection; applied=false with success means no change.
 */
int ObDASAsyncScalarRangeOp::try_apply_sort_bottom(const ObDatum &sort_bottom, bool &applied)
{
  int ret = OB_SUCCESS;
  applied = false;
  INC_METRIC_VAL(common::ObMetricId::HS_SORT_BOTTOM_ATTEMPTS, 1);
  const ObDASSortRuntimeInfo &info = search_ctx_.get_sort_info();
  // 1. Need a full TopN and a non-NULL first field value. Score and __shard_doc are not indexed field bounds.
  // Only handle a bound main-document segment, before this leaf reads any document or installs a bound.
  // A direct-bound leaf keeps its physical segment binding while reopening the
  // unread value range. Nested-document and logical-table scans stay excluded.
  if (!info.enabled() || !info.sort_ctdef_->has_sort_value_lookup_
      || sort_bottom.is_null()
      || info.sort_ctdef_->score_sort_flags_.at(0) || info.sort_ctdef_->unique_shard_doc_flags_.at(0)
      || is_empty_segment_ || read_started_ || has_sort_ranges_ || !version_range_.is_valid()
      || scope_ != DAS_SCOPE_BASE || scalar_ctdef_->is_logical_table()) {
    LOG_TRACE("hybrid sort bottom skipped range state", K_(index_table_id),
              "sort_enabled", info.enabled(), "bottom_is_null", sort_bottom.is_null(),
              "has_sort_value_lookup", info.enabled() && info.sort_ctdef_->has_sort_value_lookup_,
              "score_sort", info.enabled() && !info.sort_ctdef_->score_sort_flags_.empty()
                  && info.sort_ctdef_->score_sort_flags_.at(0),
              "shard_doc_sort", info.enabled() && !info.sort_ctdef_->unique_shard_doc_flags_.empty()
                  && info.sort_ctdef_->unique_shard_doc_flags_.at(0),
              "logical_table", scalar_ctdef_ != nullptr && scalar_ctdef_->is_logical_table(),
              K_(direct_bind), K_(read_started), K_(has_sort_ranges),
              K_(is_empty_segment), K_(scope), K_(version_range));
  } else if (!info.sort_ctdef_->sort_collations_.at(0).is_ascending_
             && info.sort_ctdef_->sort_collations_.at(0).null_pos_ == NULL_LAST) {
    // NULL_LAST means NULL compares as the largest value before DESC reverses the comparison;
    // this combination therefore represents SQL DESC NULLS FIRST.
    // Example: Top3 currently retains [NULL, 50, 30], so its bottom is still the non-NULL value 30.
    // If a later group's matching range contains [NULL, 40], the correct Top3 is [NULL, NULL, 50].
    // Raising the index range start to 30 would discard SQL NULL keys, which lie below non-NULL
    // index keys, even though these rows should rank first. Keep the original ranges and let TopN
    // compare all candidates; applied stays false and normal sorting/search_after remain enabled.
    LOG_TRACE("hybrid sort bottom skipped range", K_(index_table_id), "reason", "DESC NULLS FIRST");
  } else if (OB_SUCC(ret)) {
    ObArenaAllocator tmp_bound_alloc(ObMemAttr(MTL_ID(), "SortRangeBound"));
    int64_t column = -1;
    ObObj path;
    ObObj value;
    bool usable = false;
    const ObIArray<ObNewRange> &original = scalar_rtdef_->key_ranges_;
    // 2. Find the same column and typed path in the original ranges, then encode the first bottom value.
    // Unsupported expressions, value types or encodings skip this optimization; TopN still compares full keys.
    if (OB_FAIL(make_sort_bottom_term(
        search_ctx_, sort_bottom, original, tmp_bound_alloc, column, path, value, usable))) {
      LOG_WARN("failed to prepare hybrid sort bottom range term", K(ret));
    } else if (usable) {
      const bool ascending = info.sort_ctdef_->sort_collations_.at(0).is_ascending_;
      ObObj cut_objects[4];
      ObRowkey cut;
      bool changed = false;
      if (OB_FAIL(check_sort_range_change_scalar_range(column, path, value, ascending, cut_objects, cut, changed))) {
        LOG_WARN("failed to check hybrid sort range change", K(ret));
      } else if (changed && OB_FAIL(bind_new_range_for_scalar_range(column, path, ascending, cut, applied))) {
        LOG_WARN("failed to bind hybrid sort ranges", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && applied) {
    INC_METRIC_VAL(common::ObMetricId::HS_SORT_BOTTOM_APPLIED, 1);
  } else if (OB_SUCC(ret)) {
    INC_METRIC_VAL(common::ObMetricId::HS_SORT_BOTTOM_RANGE_UNCHANGED, 1);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/search/ob_das_filter_bitmap_provider.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
namespace sql
{
ERRSIM_POINT_DEF(ERRSIM_DISABLE_FILTER_BITMAP_SINGLE_BUILD,
    "Disable direct materialization of an exclusive range filter into its provider bitmap");

ObDASFilterBitmapProvider::~ObDASFilterBitmapProvider()
{
  if (owns_bitmap_ && OB_NOT_NULL(bitmap_)) {
    bitmap_->~ObFastBitmap();
    search_ctx_.get_allocator().free(bitmap_);
  }
  if (OB_NOT_NULL(rowid_buf_)) {
    search_ctx_.get_allocator().free(rowid_buf_);
  }
  bitmap_ = nullptr;
  rowid_buf_ = nullptr;
  rowid_buf_capacity_ = 0;
  owns_bitmap_ = false;
  prepared_ = false;
  rescan_pending_ = false;
}

int ObDASFilterBitmapProvider::allocate_bitmap(
    common::ObIAllocator &allocator,
    ObFastBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t estimated_row_count = search_ctx_.get_row_count().is_valid()
      ? search_ctx_.get_row_count().cost()
      : 0;
  bitmap = OB_NEWx(ObFastBitmap, &allocator, allocator);
  if (OB_ISNULL(bitmap)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate filter provider bitmap", K(ret), K_(provider_id));
  } else if (OB_FAIL(bitmap->init_from_source(*source_op_, estimated_row_count))) {
    LOG_WARN("failed to initialize filter provider bitmap", K(ret), K_(provider_id));
    bitmap->~ObFastBitmap();
    allocator.free(bitmap);
    bitmap = nullptr;
  }
  return ret;
}

int ObDASFilterBitmapProvider::prepare_source(const bool is_rescan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter provider source op is null", K(ret), K_(provider_id));
  } else if (OB_UNLIKELY(!source_op_->can_enumerate_rowids()
                         && !source_op_->supports_direct_rowid_drain())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("filter provider source cannot produce rowids", K(ret),
             K_(provider_id), "source_type", source_op_->get_op_type());
  } else if (!source_opened_ && is_rescan) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter provider source was not opened before rescan", K(ret), K_(provider_id));
  } else if (!source_opened_ && OB_FAIL(source_op_->open())) {
    LOG_WARN("failed to open filter provider source", K(ret), K_(provider_id));
  } else if (!source_opened_) {
    source_opened_ = true;
  } else if (is_rescan && OB_FAIL(source_op_->rescan())) {
    LOG_WARN("failed to rescan filter provider source", K(ret), K_(provider_id));
  }
  return ret;
}

int ObDASFilterBitmapProvider::fill_bitmap(
    ObIDASSearchOp &source_op, ObFastBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  bool used_direct_build = false;
  if (DAS_SEARCH_OP_BITMAP == source_op.get_op_type()
      && 0 == ERRSIM_DISABLE_FILTER_BITMAP_SINGLE_BUILD
      && OB_FAIL(static_cast<ObDASBitmapOp &>(source_op).try_fill_provider_bitmap(
          bitmap, used_direct_build))) {
    LOG_WARN("failed to materialize filter directly into provider bitmap", K(ret), K_(provider_id));
  } else if (!used_direct_build && OB_FAIL(fill_bitmap_from_rowids(source_op, bitmap))) {
    LOG_WARN("failed to materialize filter provider from rowids", K(ret), K_(provider_id));
  }
  return ret;
}

int ObDASFilterBitmapProvider::fill_bitmap_from_rowids(
    ObIDASSearchOp &source_op,
    ObFastBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t capacity = OB_MAX(1, search_ctx_.max_batch_size());
  ObDASRowID boundary;
  boundary.set_max();
  bool iter_end = false;
  int64_t total_count = 0;
  if (source_op.supports_direct_rowid_drain()) {
    const uint64_t range_start = search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_lo().is_min_value()
        ? search_ctx_.get_docid_range_lo().get_uint64() : 0;
    const uint64_t range_end = search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value()
        ? search_ctx_.get_docid_range_hi().get_uint64() : UINT64_MAX;
    ObDASRowID next_id_after;
    ObFastBitmapRowIDConsumer consumer(bitmap);
    if (OB_FAIL(source_op.direct_drain_rowids(
            range_start, range_end, consumer, next_id_after))) {
      LOG_WARN("failed to directly drain filter provider source", K(ret),
          K_(provider_id), K(range_start), K(range_end));
    } else if (OB_UNLIKELY(!next_id_after.is_max()
                           && (!next_id_after.is_normal()
                               || next_id_after.get_uint64() <= range_end))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct provider drain returned invalid lookahead", K(ret),
          K_(provider_id), K(range_start), K(range_end), K(next_id_after));
    } else {
      total_count = consumer.consumed_count();
      iter_end = true;
    }
  } else if (capacity > rowid_buf_capacity_) {
    if (OB_NOT_NULL(rowid_buf_)) {
      search_ctx_.get_allocator().free(rowid_buf_);
      rowid_buf_ = nullptr;
      rowid_buf_capacity_ = 0;
    }
    if (OB_ISNULL(rowid_buf_ = static_cast<uint64_t *>(
            search_ctx_.get_allocator().alloc(sizeof(uint64_t) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate filter provider rowid batch", K(ret),
               K(capacity), K_(provider_id));
    } else {
      rowid_buf_capacity_ = capacity;
    }
  }
  while (OB_SUCC(ret) && !iter_end) {
    int64_t count = 0;
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    if (OB_FAIL(source_op.drain_until(
            boundary, capacity, rowid_buf_, nullptr, count,
            next_id_after, next_score_after))) {
      LOG_WARN("failed to drain filter provider source", K(ret),
               K_(provider_id), K(total_count));
    } else if (count > 0 && OB_FAIL(bitmap.add_batch(rowid_buf_, count))) {
      LOG_WARN("failed to append filter provider bitmap batch", K(ret),
               K_(provider_id), K(count));
    } else {
      total_count += count;
      iter_end = count < capacity && next_id_after.is_max();
      if (OB_UNLIKELY(count < capacity && !next_id_after.is_max())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected prefetched row beyond max boundary", K(ret),
                 K_(provider_id), K(count), K(next_id_after));
      }
    }
  }
  if (OB_SUCC(ret)) {
    bitmap.finalize();
    LOG_TRACE("filter provider bitmap materialized", K_(provider_id),
              K(total_count), K(bitmap.cardinality()));
  }
  return ret;
}

int ObDASFilterBitmapProvider::prepare(const bool is_rescan)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != materialization_ret_) {
    ret = materialization_ret_;
  } else if (OB_NOT_NULL(bitmap_) && !owns_bitmap_) {
    prepared_ = true;
  } else if (!is_rescan && prepared_) {
    // Keep prepare idempotent at the SearchDriver lifecycle boundary.
  } else if (is_rescan && !source_opened_) {
    // Reject a caller's invalid lifecycle transition before materialization.
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter provider source was not opened before rescan", K(ret), K_(provider_id));
  } else {
    // Hide the preceding snapshot before any rescan/open operation can fail.
    prepared_ = false;
    if (OB_FAIL(prepare_source(is_rescan))) {
      LOG_WARN("failed to prepare filter provider source", K(ret),
               K_(provider_id), K(is_rescan));
    } else if (OB_ISNULL(bitmap_)
               && OB_FAIL(allocate_bitmap(search_ctx_.get_allocator(), bitmap_))) {
      LOG_WARN("failed to allocate query filter provider bitmap", K(ret), K_(provider_id));
    } else {
      owns_bitmap_ = true;
      if (is_rescan) {
        ret = bitmap_->init_from_source(*source_op_, search_ctx_.get_row_count().cost());
      }
      if (OB_SUCC(ret) && OB_FAIL(fill_bitmap(*source_op_, *bitmap_))) {
        LOG_WARN("failed to materialize query filter provider", K(ret),
                 K_(provider_id), K(is_rescan));
      } else if (OB_SUCC(ret)) {
        prepared_ = true;
        rescan_pending_ = false;
      }
    }
    if (OB_FAIL(ret)) {
      // Failed drains may have consumed rows that never reached the bitmap.
      // Preserve the first error rather than continuing from that partial state.
      materialization_ret_ = ret;
    }
  }
  return ret;
}

int ObDASFilterBitmapProvider::reset_for_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != materialization_ret_) {
    ret = materialization_ret_;
  } else if (OB_NOT_NULL(bitmap_) && !owns_bitmap_) {
    prepared_ = true;
    rescan_pending_ = false;
  } else {
    prepared_ = false;
    rescan_pending_ = source_opened_;
    if (owns_bitmap_ && OB_NOT_NULL(bitmap_)) {
      bitmap_->reuse();
    }
  }
  return ret;
}

int ObDASFilterBitmapProvider::ensure_prepared()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != materialization_ret_) {
    ret = materialization_ret_;
  } else if (prepared_) {
    // already prepared
  } else if (OB_FAIL(prepare(rescan_pending_))) {
    LOG_WARN("failed to lazily prepare filter bitmap provider", K(ret),
             K_(provider_id), K_(rescan_pending));
  }
  return ret;
}

int ObDASFilterBitmapProvider::build_shared_bitmap(
    common::ObIAllocator &bitmap_alloc,
    ObFastBitmap *&out_bitmap)
{
  int ret = OB_SUCCESS;
  ObFastBitmap *bitmap = nullptr;
  out_bitmap = nullptr;
  if (OB_SUCCESS != materialization_ret_) {
    ret = materialization_ret_;
  } else if (prepared_ || OB_NOT_NULL(bitmap_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter provider already has a bitmap", K(ret), K_(provider_id));
  } else {
    if (OB_FAIL(prepare_source(false))) {
      LOG_WARN("failed to prepare shared filter provider source", K(ret), K_(provider_id));
    } else if (OB_FAIL(allocate_bitmap(bitmap_alloc, bitmap))) {
      LOG_WARN("failed to allocate shared filter provider bitmap", K(ret), K_(provider_id));
    } else if (OB_FAIL(fill_bitmap(*source_op_, *bitmap))) {
      LOG_WARN("failed to build shared filter provider bitmap", K(ret), K_(provider_id));
    } else {
      // The PREPARE runtime may contain provider dependencies (for example a
      // Group-local mixed Filter provider reading a query-scope sync provider
      // View). Publish the completed bitmap locally as well as through the slot,
      // so later providers in registration order reuse it instead of rebuilding.
      bitmap_ = bitmap;
      owns_bitmap_ = false;
      prepared_ = true;
      rescan_pending_ = false;
      out_bitmap = bitmap;
    }
    if (OB_FAIL(ret)) {
      materialization_ret_ = ret;
      prepared_ = false;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(bitmap)) {
    bitmap->~ObFastBitmap();
    bitmap_alloc.free(bitmap);
  }
  return ret;
}

int ObDASFilterBitmapProvider::set_external_bitmap(ObFastBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != materialization_ret_) {
    ret = materialization_ret_;
  } else if (OB_ISNULL(bitmap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("external filter provider bitmap is null", K(ret), K_(provider_id));
  } else if (prepared_ && bitmap_ == bitmap) {
    // Idempotent injection is useful in tests that model PREPARE and slice
    // runtimes with one SearchCtx. Production runtimes normally use clones.
  } else if (prepared_ || OB_NOT_NULL(bitmap_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter provider bitmap was already installed", K(ret), K_(provider_id));
  } else {
    bitmap_ = bitmap;
    owns_bitmap_ = false;
    prepared_ = true;
  }
  return ret;
}

int ObDASFilterBitmapProvider::create_view(
    ObDASSearchCtx &search_ctx,
    ObDASBitmapOp *&view_op)
{
  int ret = OB_SUCCESS;
  ObDASFilterBitmapViewOp *view = nullptr;
  ObDASBitmapOpParam view_param(this);
  view_param.set_is_scoring(false);
  view_op = nullptr;
  if (DAS_FILTER_BITMAP_GROUP_SCOPE == scope_
      && OB_UNLIKELY(search_ctx.get_async_group_slot() != group_slot_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter bitmap provider cannot cross async Group", K(ret),
             K_(provider_id), K_(scope), K_(group_slot),
             "consumer_group_slot", search_ctx.get_async_group_slot());
  } else if (OB_FAIL(search_ctx.create_op(view_param, view))) {
    LOG_WARN("failed to create filter bitmap view", K(ret), K_(provider_id));
  } else {
    view_op = view;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

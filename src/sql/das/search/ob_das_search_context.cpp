/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_search_context.h"
#include "sql/das/ob_das_utils.h"
#include "lib/container/ob_array_helper.h"
#include "sql/das/search/ob_das_logical_bm25_stats_provider.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "share/logical_table/ob_logical_table_range_builder.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/expr/ob_expr_hybrid_search_sort.h"

namespace oceanbase
{
namespace sql
{

ObTabletID ObDASSearchCtx::get_main_tablet_id() const
{
  if (!is_async_index_query()) {
    return get_root_tablet_id();
  }
  return current_async_partition_ == nullptr ? ObTabletID() : current_async_partition_->main_tablet_id_;
}

int ObDASSearchCtx::project_shard_doc(uint64_t increment, int64_t index, int64_t count)
{
  int ret = OB_SUCCESS;
  const ObTabletID main_tablet_id = get_main_tablet_id();
  if (!has_shard_doc()) {
  } else if (index < 0 || index >= count || !main_tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid sort shard document projection state", K(ret), K(index), K(count),
             K(main_tablet_id));
  } else {
    ObExpr *expr = fusion_ctdef_->shard_doc_expr_;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
    guard.set_batch_size(count);
    guard.set_batch_idx(index);
    if (index == 0 && OB_FAIL(expr->init_vector_for_write(*eval_ctx_, expr->get_default_res_format(), count))) {
      LOG_WARN("failed to initialize hybrid sort __shard_doc vector", K(ret), K(count), K(main_tablet_id));
    } else {
      char *text = nullptr;
      if (OB_ISNULL(text = expr->get_str_res_mem(*eval_ctx_, 32))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hybrid sort shard document value", K(ret), K(index), K(count),
                 "value_length", 32, K(main_tablet_id));
      } else if (OB_FAIL(ObExprHybridUniqueShardDoc::encode_shard_doc(main_tablet_id.id(), increment, text))) {
        LOG_WARN("failed to encode hybrid sort __shard_doc", K(ret), K(index), K(count), K(main_tablet_id));
      } else {
        expr->get_vector(*eval_ctx_)->set_string(index, text, 32);
        expr->set_evaluated_projected(*eval_ctx_);
      }
    }
  }
  return ret;
}

bool ObDASSearchCtx::is_serial_fusion_plan() const
{
  const ObDASBaseCtDef *ctdef = nullptr;
  const ObDASBaseCtDef *root = root_das_task_.get_attach_ctdef();
  const int ret = OB_ISNULL(root) ? OB_NOT_INIT : ObDASUtils::find_child_das_ctdef(
      root, DAS_OP_FUSION_QUERY, ctdef);
  return OB_SUCCESS == ret && OB_NOT_NULL(ctdef)
      && !static_cast<const ObDASFusionCtDef *>(ctdef)->enable_parallel_;
}

void ObDASSearchCtx::reset()
{
  fusion_ctdef_ = nullptr;
  for (int64_t i = 0; i < op_store_.count(); ++i) {
    if (OB_NOT_NULL(op_store_.at(i))) {
      op_store_.at(i)->close();
      op_store_.at(i)->~ObIDASSearchOp();
      op_store_.at(i) = nullptr;
    }
  }
  op_store_.reset();
  posting_table_cache_.reset();
  // Operators must return their leases before the query loses the pool binding.
  if (nullptr != inv_iter_pool_) {
    storage::ObAsyncInvIteratorPool *pool = inv_iter_pool_;
    inv_iter_pool_ = nullptr;
    pool->unpin();
  }
  enable_inv_iter_pool_ = false;
  op_count_ = 0;
  generating_topk_limit_ = -1;
  streaming_ror_costing_topk_hint_ = -1;
  rowid_meta_.reset();
  bm25_param_cache_.reset();
  logical_table_ctx_.reset();
  row_count_logical_table_ctx_.reset();
  for (int64_t i = 0; i < logical_bm25_stats_providers_.count(); ++i) {
    if (OB_NOT_NULL(logical_bm25_stats_providers_.at(i))) {
      logical_bm25_stats_providers_.at(i)->~ObLogicalFilteredBM25StatsProvider();
      logical_bm25_stats_providers_.at(i) = nullptr;
    }
  }
  logical_bm25_stats_providers_.reset();
  for (int64_t i = 0; i < filter_bitmap_providers_.count(); ++i) {
    if (OB_NOT_NULL(filter_bitmap_providers_.at(i))) {
      filter_bitmap_providers_.at(i)->~ObDASFilterBitmapProvider();
      filter_bitmap_providers_.at(i) = nullptr;
    }
  }
  filter_bitmap_providers_.reset();
  async_query_ctx_ = nullptr;
  current_async_partition_ = nullptr;
  async_group_mode_override_ = ASYNC_GROUP_MODE_INHERIT;
  async_index_table_id_ = common::OB_INVALID_ID;
  async_group_slot_ = -1;
  async_direct_bind_ = false;
  generating_group_filter_domain_ = false;
  op_creating_scope_ = DAS_SCOPE_BASE;
  op_creating_depth_ = 0;
  nested_scope_id_ = DAS_SCOPE_MAX;
  for (int i = 0; i < DAS_SCOPE_MAX; ++i) {
    scopes_[i] = ObDASRowIDScope();
  }
}

int ObDASSearchCtx::acquire_posting_entry(blocksstable::ObInvPostingIterEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (enable_inv_iter_pool_ && nullptr == inv_iter_pool_) {
    storage::ObAsyncInvIteratorPool *pool = MTL(storage::ObAsyncInvIteratorPool *);
    // Admission is independent of the ordinary row iterator pool.
    if (nullptr != pool && pool->pin()) {
      inv_iter_pool_ = pool;
    }
  }
  if (nullptr != inv_iter_pool_) {
    if (OB_FAIL(inv_iter_pool_->get_inv_entry(
        blocksstable::ObInvPostingIterEntry::ReuseKey{}, entry))) {
      LOG_WARN("failed to acquire posting entry, use original allocation", K(ret));
      // Pool allocation is optional; the original scan path remains available.
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDASSearchCtx::prepare_posting_scan(
    const bool use_pool,
    common::ObIAllocator &scan_allocator,
    blocksstable::ObInvPostingScanParam &param,
    blocksstable::ObInvPostingIterEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("posting entry must be closed before preparing a scan", K(ret), KP(entry));
  } else if (use_pool && OB_FAIL(acquire_posting_entry(entry))) {
    LOG_WARN("failed to acquire posting entry", K(ret));
  } else {
    param.allocator_ = &scan_allocator;
    param.reusable_iter_ = nullptr != entry ? &entry->iter_ : nullptr;
    param.table_cache_ = &posting_table_cache_;
  }
  return ret;
}

void ObDASSearchCtx::close_posting_scan(
    blocksstable::ObInvPostingScanParam &param,
    common::ObIAllocator &scan_allocator,
    blocksstable::ObInvPostingIterEntry *&entry,
    blocksstable::ObInvSSTablePostingIter *&iter)
{
  if (nullptr != entry) {
    iter = nullptr;
    inv_iter_pool_->release_inv_entry(entry);
  } else if (nullptr != iter) {
    iter->~ObInvSSTablePostingIter();
    scan_allocator.free(iter);
    iter = nullptr;
  }
  param.reusable_iter_ = nullptr;
  param.allocator_ = &scan_allocator;
}

ObDASFilterBitmapProvider *ObDASSearchCtx::find_filter_bitmap_provider(
    const ObIDASSearchRtDef *owner_rtdef,
    const ObDASFilterBitmapProviderScope scope,
    const int64_t group_slot) const
{
  ObDASFilterBitmapProvider *provider = nullptr;
  for (int64_t i = 0; OB_ISNULL(provider) && i < filter_bitmap_providers_.count(); ++i) {
    ObDASFilterBitmapProvider *candidate = filter_bitmap_providers_.at(i);
    if (OB_NOT_NULL(candidate)
        && candidate->get_owner_rtdef() == owner_rtdef
        && candidate->get_scope() == scope
        && candidate->get_group_slot() == group_slot) {
      provider = candidate;
    }
  }
  return provider;
}

int ObDASSearchCtx::create_filter_bitmap_provider(
    const ObIDASSearchRtDef *owner_rtdef,
    const ObDASFilterBitmapProviderScope scope,
    const int64_t group_slot,
    ObIDASSearchOp *source_op,
    ObDASFilterBitmapProvider *&provider)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  provider = find_filter_bitmap_provider(owner_rtdef, scope, group_slot);
  if (OB_NOT_NULL(provider)) {
    // Reuse the provider registered by an earlier consumer in the same scope.
  } else if (OB_ISNULL(owner_rtdef) || OB_ISNULL(source_op)
             || OB_UNLIKELY((DAS_FILTER_BITMAP_QUERY_SCOPE != scope
                             && DAS_FILTER_BITMAP_GROUP_SCOPE != scope)
                            || (DAS_FILTER_BITMAP_QUERY_SCOPE == scope
                                && group_slot != DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT)
                            || (DAS_FILTER_BITMAP_GROUP_SCOPE == scope && group_slot < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter bitmap provider arguments", K(ret),
             KP(owner_rtdef), K(scope), K(group_slot), KP(source_op));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type()
                         || (!source_op->can_enumerate_rowids()
                             && !source_op->supports_direct_rowid_drain()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("filter bitmap provider requires materializable uint64 rowids", K(ret),
             K(get_rowid_type()), "source_type", source_op->get_op_type());
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDASFilterBitmapProvider)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate filter bitmap provider", K(ret));
  } else {
    ObDASFilterBitmapProvider *new_provider = new (buf) ObDASFilterBitmapProvider(
        *this, owner_rtdef, scope, group_slot, source_op,
        filter_bitmap_providers_.count());
    if (OB_FAIL(filter_bitmap_providers_.push_back(new_provider))) {
      LOG_WARN("failed to register filter bitmap provider", K(ret));
      new_provider->~ObDASFilterBitmapProvider();
    } else {
      provider = new_provider;
    }
  }
  return ret;
}

int ObDASSearchCtx::reset_filter_bitmap_providers_for_rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_bitmap_providers_.count(); ++i) {
    ObDASFilterBitmapProvider *provider = filter_bitmap_providers_.at(i);
    if (OB_ISNULL(provider)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter bitmap provider is null", K(ret), K(i));
    } else if (OB_FAIL(provider->reset_for_rescan())) {
      LOG_WARN("failed to reset filter bitmap provider for rescan", K(ret), K(i));
    }
  }
  return ret;
}

bool ObDASSearchCtx::use_async_group_alignment() const
{
  bool use_group = false;
  if (ASYNC_GROUP_MODE_FORCE_GROUP == async_group_mode_override_) {
    use_group = true;
  } else if (ASYNC_GROUP_MODE_INHERIT == async_group_mode_override_
             && OB_NOT_NULL(async_query_ctx_)) {
    use_group = async_query_ctx_->is_group_alignment_enabled();
  }
  return use_group;
}

int ObDASSearchCtx::resolve_direct_async_index_segment(
    const uint64_t index_table_id,
    const common::ObTabletID &index_tablet_id,
    const int64_t group_slot,
    common::ObNewVersionRange &version_range,
    bool &is_empty) const
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  version_range.reset();
  is_empty = false;
  if (OB_ISNULL(current_async_partition_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null partition for direct async index segment", K(ret),
             K(index_table_id), K(index_tablet_id), K(group_slot));
  } else if (OB_UNLIKELY(current_async_partition_->ls_id_ != ls_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct async index partition ls mismatch", K(ret),
             "partition_ls_id", current_async_partition_->ls_id_, K_(ls_id),
             K(index_table_id), K(index_tablet_id), K(group_slot));
  } else if (OB_FAIL(current_async_partition_->find_index_tablet_idx(
                 index_table_id, tablet_idx))) {
    LOG_WARN("failed to resolve direct async index tablet", K(ret),
             K(index_table_id), K(index_tablet_id), K(group_slot));
  } else if (OB_UNLIKELY(
                 current_async_partition_->index_tablet_ids_.at(tablet_idx)
                 != index_tablet_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct async index tablet binding mismatch", K(ret),
             K(index_table_id), K(index_tablet_id), K(group_slot), K(tablet_idx),
             K(current_async_partition_->index_tablet_ids_));
  } else if (OB_UNLIKELY(
                 group_slot < 0
                 || group_slot >= current_async_partition_->get_storage_group_ctx()
                                      .get_group_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct async index group slot out of range", K(ret),
             K(index_table_id), K(index_tablet_id), K(group_slot),
             "group_count",
             current_async_partition_->get_storage_group_ctx().get_group_count());
  } else {
    const storage::ObAsyncIndexSegmentGroup *group =
        current_async_partition_->get_storage_group_ctx().get_group(group_slot);
    if (OB_ISNULL(group) || tablet_idx >= group->get_tablet_cnt()
        || OB_UNLIKELY(group->get_tablet_version_ranges(tablet_idx).count() > 1)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("direct async binding allows at most one segment", K(ret),
               K(index_table_id), K(group_slot), K(tablet_idx));
    } else if (group->get_tablet_version_ranges(tablet_idx).empty()) {
      is_empty = true;
    } else {
      version_range = group->get_tablet_version_ranges(tablet_idx).at(0);
    }
  }
  return ret;
}

ObLogicalFilteredBM25StatsProvider *ObDASSearchCtx::find_logical_bm25_stats_provider(
    const ObLogicalFilteredBM25StatsKey &key)
{
  ObLogicalFilteredBM25StatsProvider *result = nullptr;
  for (int64_t i = 0; nullptr == result && i < logical_bm25_stats_providers_.count(); ++i) {
    ObLogicalFilteredBM25StatsProvider *p = logical_bm25_stats_providers_.at(i);
    if (OB_NOT_NULL(p) && p->get_key() == key) {
      result = p;
    }
  }
  return result;
}

int ObDASSearchCtx::get_or_create_logical_bm25_stats_provider(
    const ObLogicalFilteredBM25StatsKey &key,
    const ObDASScalarScanCtDef *primary_stats_ctdef,
    ObDASScalarScanRtDef *primary_stats_rtdef,
    const ObDASScalarScanCtDef *inv_scan_ctdef,
    ObDASScalarScanRtDef *inv_scan_rtdef,
    const sql::ExprFixedArray *filter_exprs,
    ObLogicalFilteredBM25StatsProvider *&provider)
{
  int ret = OB_SUCCESS;
  provider = find_logical_bm25_stats_provider(key);
  if (nullptr == provider) {
    void *buf = allocator_.alloc(sizeof(ObLogicalFilteredBM25StatsProvider));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc filtered bm25 stats provider", K(ret));
    } else {
      ObLogicalFilteredBM25StatsProvider *p = new (buf) ObLogicalFilteredBM25StatsProvider();
      // Forward scalar ctdefs to the templated Provider::init via the base-type entry.
      const ObDASBaseCtDef *primary_base = static_cast<const ObDASBaseCtDef *>(primary_stats_ctdef);
      ObDASBaseRtDef *primary_base_rt = static_cast<ObDASBaseRtDef *>(primary_stats_rtdef);
      const ObDASBaseCtDef *inv_base = static_cast<const ObDASBaseCtDef *>(inv_scan_ctdef);
      ObDASBaseRtDef *inv_base_rt = static_cast<ObDASBaseRtDef *>(inv_scan_rtdef);
      if (OB_FAIL(p->init(key, primary_base, primary_base_rt,
                          inv_base, inv_base_rt, filter_exprs,
                          true /*use_scalar_ctdef*/, this))) {
        LOG_WARN("failed to init filtered bm25 stats provider", K(ret), K(key));
        p->~ObLogicalFilteredBM25StatsProvider();
      } else if (OB_FAIL(logical_bm25_stats_providers_.push_back(p))) {
        LOG_WARN("failed to register stats provider", K(ret));
        p->~ObLogicalFilteredBM25StatsProvider();
      } else {
        provider = p;
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::init(
  const sql::ExprFixedArray &rowid_exprs,
  const sql::ExprFixedArray &output,
  bool use_dynamic_pruning)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  enable_inv_iter_pool_ = tenant_config.is_valid()
      && static_cast<int64_t>(tenant_config->async_inverted_index_iter_cache_max_size) > 0;
  if (OB_ISNULL(root_das_task_.get_rtdef()) ||
      OB_ISNULL(eval_ctx_ = root_das_task_.get_rtdef()->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(rowid_meta_.init(rowid_exprs, 0))) {
    LOG_WARN("failed to init rowid meta", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(ObBitVector::memory_size(eval_ctx_->max_batch_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mock skip", K(ret), K(eval_ctx_->max_batch_size_));
  } else {
    mock_skip_ = to_bit_vector(buf);
    mock_skip_->init(eval_ctx_->max_batch_size_);
    rowid_exprs_ = &rowid_exprs;
    output_ = &output;
    use_dynamic_pruning_ = use_dynamic_pruning;
    rowid_type_ = DAS_ROWID_TYPE_UINT64; // TODO: determine by actual rowid type
    scopes_[DAS_SCOPE_BASE].rowid_type_ = rowid_type_;
    scopes_[DAS_SCOPE_BASE].rowid_exprs_ = rowid_exprs_;
    scopes_[DAS_SCOPE_BASE].rowid_meta_ = rowid_meta_;
    if (OB_FAIL(refresh_table_row_count())) {
      LOG_WARN("failed to estimate table row count", K(ret));
    } else if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid search ctx", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    eval_ctx_ = nullptr;
    mock_skip_ = nullptr;
    rowid_exprs_ = nullptr;
    output_ = nullptr;
    table_row_count_.reset();
    row_count_tablet_id_.reset();
    rowid_meta_.reset();
  }
  return ret;
}

int ObDASSearchCtx::build_estimate_scan_param(
    const EstimateScanInput &input,
    const bool whole_ltable_range,
    ObTableScanParam &est_param,
    common::ObIArray<common::ObNewRange> &estimate_ranges)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id;
  if (input.is_logical_table_) {
    tablet_id = logical_table_ctx_.physical_tablet_id_;
  } else if (OB_NOT_NULL(input.related_tablet_ctdef_)) {
    if (OB_FAIL(get_related_tablet_id(input.related_tablet_ctdef_, tablet_id))) {
      LOG_WARN("failed to get related tablet id for row count estimate", K(ret));
    }
  } else {
    tablet_id = get_root_tablet_id();
  }
  if (OB_FAIL(ret)) {
  } else {
    est_param.index_id_ = input.is_logical_table_
        ? logical_table_ctx_.physical_data_table_id_ : input.ref_table_id_;
    est_param.scan_flag_ = input.scan_flag_;
    est_param.tablet_id_ = tablet_id;
    est_param.ls_id_ = ls_id_;
    est_param.schema_version_ = input.schema_version_;
    est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;

    if (input.is_logical_table_) {
      if (OB_UNLIKELY(common::OB_INVALID_ID == input.logical_index_seq_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid logical index sequence for row count estimate",
                 K(ret), K(input.ref_table_id_));
      } else {
        share::ObLogicalTableKeyPrefix prefix(
            logical_table_ctx_.namespace_id_,
            logical_table_ctx_.logical_table_id_,
            input.logical_index_seq_,
            input.logical_table_aux_type_);
        if (whole_ltable_range) {
          if (OB_FAIL(share::ObLogicalTableRangeBuilder::build_primary_ranges(
                  logical_table_ctx_.physical_data_table_id_, prefix,
                  logical_table_ctx_.logical_table_id_,
                  true /* lt_start_inclusive */, true /* lt_end_inclusive */,
                  false /* has_rowid_lower_bound */, 0, true,
                  false /* has_rowid_upper_bound */, UINT64_MAX, true,
                  input.range_allocator_, estimate_ranges))) {
            LOG_WARN("build whole logical table range for row count estimate failed",
                     K(ret), K(prefix));
          }
        } else if (OB_FAIL(share::ObLogicalTableRangeBuilder::convert_ranges(
                       logical_table_ctx_.physical_data_table_id_, prefix,
                       input.key_ranges_, input.range_allocator_, estimate_ranges))) {
          LOG_WARN("convert logical table ranges for row count estimate failed",
                   K(ret), K(prefix));
        }
      }
    } else if (whole_ltable_range) {
      common::ObNewRange whole_range;
      whole_range.table_id_ = input.ref_table_id_;
      whole_range.set_whole_range();
      if (OB_FAIL(estimate_ranges.push_back(whole_range))) {
        LOG_WARN("failed to build whole table range for row count estimate", K(ret));
      }
    } else if (input.is_async_scalar_range_) {
      ObDASSearchUtils::AsyncScalarTermRange async_term_range;
      for (int64_t i = 0; OB_SUCC(ret) && i < input.key_ranges_.count(); ++i) {
        common::ObNewRange estimate_range;
        if (OB_FAIL(ObDASSearchUtils::project_async_scalar_term_range(
                input.key_ranges_.at(i), async_term_range))) {
          LOG_WARN("project async scalar term range for estimate failed",
                   K(ret), K(i), K(input.key_ranges_.at(i)));
        } else if (OB_FAIL(common::deep_copy_range(
                       input.range_allocator_, async_term_range.range_, estimate_range))) {
          LOG_WARN("deep copy async scalar term range for estimate failed",
                   K(ret), K(i), K(async_term_range.range_));
        } else if (OB_FAIL(estimate_ranges.push_back(estimate_range))) {
          LOG_WARN("failed to append async scalar term range for estimate", K(ret), K(i));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < input.key_ranges_.count(); ++i) {
        if (OB_FAIL(estimate_ranges.push_back(input.key_ranges_.at(i)))) {
          LOG_WARN("failed to append row count estimate range", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::refresh_table_row_count()
{
  int ret = OB_SUCCESS;
  int64_t logical_row_cnt = -1;
  int64_t physical_row_cnt = -1;
  const ObAccessService *access_service = nullptr;
  const ObDASScanCtDef *scan_ctdef = nullptr;
  ObDASScanRtDef *scan_rtdef = nullptr;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  ObSimpleBatch batch;
  ObTableScanParam est_param;
  common::SQLScanRangeArray estimate_ranges;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObTableScanRange table_scan_range;

  if (OB_ISNULL(scan_rtdef = static_cast<ObDASScanRtDef *>(root_das_task_.get_rtdef())) ||
      OB_ISNULL(scan_ctdef = static_cast<const ObDASScanCtDef *>(root_das_task_.get_ctdef())) ||
      OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr in refresh_table_row_count", K(ret));
  } else {
    // Root hybrid_search scan ctdef is the logical primary table ref, not a logical aux
    // scan ctdef: it lacks valid logical_index_seq_/aux_type. For partition row count,
    // estimate primary KV rows via search_ctx lt identity + ref_table_id fallback.
    const bool use_logical_primary_est = is_logical_table();
    const uint64_t logical_index_seq =
        use_logical_primary_est
            ? ((common::OB_INVALID_ID != scan_ctdef->logical_index_seq_)
                ? scan_ctdef->logical_index_seq_
                : scan_ctdef->ref_table_id_)
            : scan_ctdef->logical_index_seq_;
    const share::ObLogicalTableAuxType logical_aux_type =
        use_logical_primary_est
            ? share::LT_AUX_PRIMARY
            : scan_ctdef->logical_table_aux_type_;
    EstimateScanInput input(
        nullptr /* related_tablet_ctdef */, scan_rtdef->key_ranges_,
        scan_ctdef->ref_table_id_, scan_ctdef->schema_version_, scan_rtdef->scan_flag_,
        false /* is_async_scalar_range */,
        use_logical_primary_est, logical_index_seq, logical_aux_type, allocator_);
    if (OB_FAIL(build_estimate_scan_param(
            input, true /* whole_ltable_range */, est_param, estimate_ranges))) {
      LOG_WARN("failed to build partition row count estimate", K(ret));
    } else if (OB_UNLIKELY(estimate_ranges.empty())) {
      // A logically empty table has no rows; do not invoke storage estimation.
      logical_row_cnt = 0;
    } else {
      if (1 == estimate_ranges.count()) {
        batch.type_ = ObSimpleBatch::T_SCAN;
        batch.range_ = &estimate_ranges.at(0);
      } else {
        batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
        batch.ranges_ = &estimate_ranges;
      }
      if (OB_FAIL(table_scan_range.init(est_param, batch, allocator_))) {
        LOG_WARN("failed to init table scan range for partition row count", K(ret));
      } else if (OB_FAIL(access_service->estimate_row_count(
                     est_param, table_scan_range, timeout_us, est_records,
                     logical_row_cnt, physical_row_cnt))) {
        LOG_WARN("failed to estimate partition row count", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      table_row_count_ = ObDASSearchCost(logical_row_cnt);
      row_count_tablet_id_ = root_das_task_.get_scan_param().tablet_id_;
      row_count_logical_table_ctx_ = logical_table_ctx_;
    }
  }
  LOG_TRACE("[LT hybrid est] refresh partition row count",
            K(ret), K(logical_row_cnt), K(physical_row_cnt),
            "is_logical_table", is_logical_table(),
            K(est_param.index_id_), K(est_param.tablet_id_),
            K(logical_table_ctx_), K(estimate_ranges));
  return ret;
}

int ObDASSearchCtx::refresh_table_row_count_if_needed()
{
  int ret = OB_SUCCESS;
  if (table_row_count_.is_valid()
      && row_count_tablet_id_ == get_root_tablet_id()
      && is_row_count_logical_table_ctx_current()) {
    // Row count was estimated for the current tablet and logical-table identity.
  } else if (OB_FAIL(refresh_table_row_count())) {
    LOG_WARN("failed to refresh table row count for current tablet", K(ret));
  }
  return ret;
}

int ObDASSearchCtx::get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_das_task_.get_related_tablet_id(scalar_ctdef, tablet_id))) {
    LOG_WARN("failed to get tablet id", K(scalar_ctdef));
  }
  return ret;
}

int ObDASSearchCtx::get_related_tablet_id(const ObDASBaseCtDef *ctdef, common::ObTabletID &tablet_id)
{
  // Hybrid SearchCtx only owns ObDASScalarScanCtDef-typed children.
  return get_related_tablet_id(static_cast<const ObDASScalarScanCtDef *>(ctdef), tablet_id);
}

int ObDASSearchCtx::register_nested_scope(
    const sql::ExprFixedArray *nested_rowid_exprs, ObDASScopeId scope_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nested_rowid_exprs) || nested_rowid_exprs->empty()
      || !is_nested_scope(scope_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested scope", K(ret), K(scope_id));
  } else if (has_nested_scope() && nested_scope_id_ != scope_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("conflicting nested scope registration", K(ret), K_(nested_scope_id), K(scope_id));
  } else {
    const ObDASRowIDType rowid_type = is_async_nested_scope(scope_id)
        ? DAS_ROWID_TYPE_UINT64 : DAS_ROWID_TYPE_COMPACT;
    // Keep the original expressions, including the NK column used by async Range/SORT.
    // UINT64 RowMeta describes only the doc_id column.
    const int64_t column_count = DAS_ROWID_TYPE_UINT64 == rowid_type
        ? 1 : nested_rowid_exprs->count();
    ObArrayHelper<ObExpr *> rowid_exprs(column_count, nested_rowid_exprs->get_data(), column_count);
    ObDASRowIDScope &scope = scopes_[scope_id];
    if (OB_FAIL(scope.rowid_meta_.init(rowid_exprs, 0))) {
      LOG_WARN("failed to init nested rowid meta", K(ret), K(rowid_type));
    } else {
      scope.rowid_type_ = rowid_type;
      scope.rowid_exprs_ = nested_rowid_exprs;
      nested_scope_id_ = scope_id;
    }
  }
  return ret;
}

int ObDASSearchCtx::apply_async_index_read_snapshot(
    storage::ObTableScanParam &scan_param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_async_index_query())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-async index query", K(ret));
  } else if (OB_ISNULL(current_async_partition_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null current async index partition", K(ret));
  } else {
    const share::SCN read_snapshot = current_async_partition_->get_read_snapshot();
    if (OB_UNLIKELY(!read_snapshot.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async index read snapshot", K(ret), K(read_snapshot));
    } else {
      scan_param.snapshot_.specify_snapshot_scn(read_snapshot);
    }
  }
  return ret;
}

int ObDASSearchCtx::init_scan_param(
    common::ObTabletID &tablet_id,
    const ObDASScalarScanCtDef *ctdef,
    ObDASScalarScanRtDef *rtdef,
    ObTableScanParam &scan_param,
    const bool use_async_read_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(rtdef->p_pd_expr_op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.key_ranges_.reuse();
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.scan_tasks_.set_attr(ObMemAttr(tenant_id, "ScanParamET"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = ctdef->is_get_;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.ls_id_ = ls_id_;
    scan_param.tablet_id_ = tablet_id;
    scan_param.enable_new_false_range_ = ctdef->enable_new_false_range_;

    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc_)) {
      scan_param.tx_id_ = tx_desc_->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot_)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot_))) {
        LOG_WARN("assign snapshot fail", K(ret));
      } else if (use_async_read_snapshot
                 && is_async_index_query()
                 && OB_FAIL(apply_async_index_read_snapshot(scan_param))) {
        LOG_WARN("failed to apply async index read snapshot", K(ret), K(tablet_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    } else {
      LOG_TRACE("init scan param metadata", K(scan_param.index_id_));
    }
  }
  return ret;
}

int ObDASSearchCtx::init_scan_param(
    common::ObTabletID &tablet_id,
    const ObDASBaseCtDef *ctdef,
    ObDASBaseRtDef *rtdef,
    ObTableScanParam &scan_param)
{
  // Hybrid SearchCtx only owns ObDASScalarScanCtDef-typed children.
  return init_scan_param(tablet_id,
                         static_cast<const ObDASScalarScanCtDef *>(ctdef),
                         static_cast<ObDASScalarScanRtDef *>(rtdef),
                         scan_param);
}

int ObDASSearchCtx::lower_bound_in_frame(ObDASScopeId scope,
                                         const ObDASRowID &target,
                                         const ObIArray<ObExpr *> *rowid_exprs,
                                         const int64_t count, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowid_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rowid exprs", K(ret));
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr eval ctx", K(ret));
  } else {
    idx = 0;
    int64_t search_right = count;
    const int64_t column_count = DAS_ROWID_TYPE_UINT64 == scopes_[scope].rowid_type_
        ? MIN(1, rowid_exprs->count()) : rowid_exprs->count();
    for (int64_t i = 0; OB_SUCC(ret) && idx < search_right && i < column_count; ++i) {
      ObDatum target_datum;
      ObIVector *vec = nullptr;
      ObExpr *expr = rowid_exprs->at(i);

      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr expr", K(ret));
      } else if (OB_ISNULL(vec = expr->get_vector(*eval_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr vector", K(ret));
      } else if (OB_FAIL(get_datum_from_rowid(scope, target, target_datum, i))) {
        LOG_WARN("failed to get datum from rowid", K(ret), K(i), K(scope));
      } else {
        if (OB_FAIL(ObDASSearchUtils::binary_search_lower_bound(*eval_ctx_, search_right, *expr, target_datum, idx))) {
          LOG_WARN("failed to binary search", K(ret));
        } else if (idx == search_right) {
          idx = search_right;
          if (idx == count) {
            ret = OB_ITER_END;
          }
          break;
        } else {
          int cmp = 0;
          ObDatum datum;
          if (OB_FAIL(ObDASSearchUtils::get_datum(*expr, *eval_ctx_, idx, datum))) {
            LOG_WARN("failed to get datum", K(ret));
          } else if (OB_FAIL(expr->basic_funcs_->null_first_cmp_(datum, target_datum, cmp))) {
            LOG_WARN("failed to compare datums", K(ret));
          } else if (cmp > 0) {
            break;
          } else {
            // vec[idx] == target[i]: narrow search_right to upper bound of equal group
            int64_t upper = 0;
            if (OB_FAIL(ObDASSearchUtils::binary_search_upper_bound(
                    *eval_ctx_, search_right, *expr, target_datum, idx, upper))) {
              LOG_WARN("failed to find upper bound", K(ret));
            } else {
              search_right = upper;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::estimate_row_count(const ObDASScalarScanCtDef *ctdef, const ObDASScalarScanRtDef *rtdef, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t logical_row_cnt = 0;
  int64_t physical_row_cnt = 0;
  const ObAccessService *access_service = nullptr;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  ObSimpleBatch batch;
  ObTableScanParam est_param;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObTableScanRange table_scan_range;
  const ObLogicalTableSearchCtx &lt_ctx = get_logical_table_ctx();
  const bool is_logical_table = OB_NOT_NULL(ctdef) && lt_ctx.is_enabled_ && ctdef->is_logical_table();
  const bool is_async_scalar_range = OB_NOT_NULL(ctdef)
      && ctdef->is_async_index_
      && !ctdef->is_async_ror_scan()
      && !is_logical_table;
  common::SQLScanRangeArray estimate_ranges;
  estimate_ranges.set_attr(ObMemAttr(MTL_ID(), "LTEstRange"));
  common::ObArenaAllocator async_est_range_allocator(
      ObMemAttr(MTL_ID(), "AsyncEstRange"));
  row_count = 0;
  if (OB_ISNULL(ctdef) ||
      OB_ISNULL(rtdef) ||
      OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ctdef->is_data_table_universe_scan_) {
    // CG-synthesized parent-scope universe scan: the ctdef has no valid
    // schema_version (inherited from the table-scan node which isn't fully
    // initialized for hybrid_search). Refresh the cached data-table count first:
    // logical_table_ctx_ may have been injected after the initial non-logical
    // estimate, and refresh_table_row_count_if_needed() validates that identity.
    if (OB_FAIL(refresh_table_row_count_if_needed())) {
      LOG_WARN("failed to refresh data-table row count for universe scan", K(ret));
    } else {
      row_count = std::max(1L, get_row_count().cost());
    }
  } else if (ctdef->enable_new_false_range_ && rtdef->key_ranges_.empty()) {
    // false range
    row_count = 0;
  } else if (rtdef->key_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty key ranges", K(ret));
  } else {
    EstimateScanInput input(
        ctdef, rtdef->key_ranges_, ctdef->ref_table_id_, ctdef->schema_version_,
        rtdef->scan_flag_, is_async_scalar_range,
        is_logical_table, ctdef->logical_index_seq_, ctdef->logical_table_aux_type_,
        is_async_scalar_range
            ? static_cast<common::ObIAllocator &>(async_est_range_allocator)
            : allocator_);
    if (OB_FAIL(build_estimate_scan_param(
            input, false /* whole_ltable_range */, est_param, estimate_ranges))) {
      LOG_WARN("failed to build row count estimate", K(ret));
    } else if (estimate_ranges.empty()) {
      // False logical ranges convert to no physical ranges; do not call storage.
      row_count = 0;
    } else {
      if (1 == estimate_ranges.count()) {
        batch.type_ = ObSimpleBatch::T_SCAN;
        batch.range_ = &estimate_ranges.at(0);
      } else {
        batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
        batch.ranges_ = &estimate_ranges;
      }
      if (FAILEDx(table_scan_range.init(est_param, batch, allocator_))) {
        LOG_WARN("failed to init table scan range", K(ret));
      } else if (OB_FAIL(access_service->estimate_row_count(est_param, table_scan_range, timeout_us, est_records, logical_row_cnt, physical_row_cnt))) {
        LOG_WARN("failed to estimate row count", K(ret));
      } else {
        row_count = logical_row_cnt;
      }
    }
  }
  const common::ObIArray<common::ObNewRange> *logical_key_ranges =
      (OB_NOT_NULL(rtdef) ? &rtdef->key_ranges_ : nullptr);
  LOG_TRACE("[LT hybrid est] estimate_row_count result",
           K(ret), K(is_logical_table), K(row_count),
           K(logical_row_cnt), K(physical_row_cnt),
           "logical_ref_table_id", (OB_NOT_NULL(ctdef) ? ctdef->ref_table_id_ : common::OB_INVALID_ID),
           K(lt_ctx),
           "used_index_id", est_param.index_id_,
           "used_tablet_id", est_param.tablet_id_,
           KPC(logical_key_ranges),
           K(estimate_ranges));
  return ret;
}

int ObDASSearchCtx::compare_uint64_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                         const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                         int &cmp)
{
  ObDASRowIDCmp<DAS_ROWID_TYPE_UINT64, true> cmp_func(rowid_meta, rowid_exprs);
  return cmp_func.cmp(rowid1, rowid2, cmp);
}

int ObDASSearchCtx::compare_compact_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                          const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                          int &cmp)
{
  ObDASRowIDCmp<DAS_ROWID_TYPE_COMPACT, true> cmp_func(rowid_meta, rowid_exprs);
  return cmp_func.cmp(rowid1, rowid2, cmp);
}

int ObDASSearchCtx::deep_copy_uint64_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                           common::ObIAllocator &alloc)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  dst = src;
  return ret;
}

int ObDASSearchCtx::deep_copy_compact_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                            common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  dst = src;
  if (OB_LIKELY(src.is_normal())) {
    const ObCompactRow *src_row = src.get_compact_row();
    if (OB_ISNULL(src_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src compact row is null", K(ret));
    } else {
      // TODO: optimize by reusing dst's underlying buffer
      const int64_t row_size = src_row->get_row_size();
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(row_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(row_size));
      } else {
        MEMCPY(buf, src_row, row_size);
        dst.set_compact_row(reinterpret_cast<const ObCompactRow *>(buf));
      }
    }
  }
  return ret;
}

template<typename StoreType>
int ObDASSearchCtx::create_rowid_store(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                                       common::ObIAllocator &alloc, ObDASScopeId scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store = OB_NEWx(StoreType, &alloc, ctx, scope))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create rowid store", K(ret));
  } else if (OB_FAIL(store->init(capacity))) {
    LOG_WARN("failed to init rowid store", K(ret), K(capacity));
    store = nullptr;
  }
  return ret;
}

int ObDASSearchCtx::uint64_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                         const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx)
{
  UNUSED(rowid_meta);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_exprs.empty()) || OB_ISNULL(rowid_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowid requires a doc id expr", K(ret), K(rowid_exprs.count()));
  } else {
    ObExpr *expr = rowid_exprs.at(0);
    ObIVector *vec = nullptr;
    if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector is null", K(ret));
    } else {
      vec->set_uint(batch_idx, rowid.get_uint64());
    }
  }
  return ret;
}

int ObDASSearchCtx::compact_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                          const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *compact_row = rowid.get_compact_row();
  if (OB_ISNULL(compact_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact row is null", K(ret));
  } else {
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < rowid_exprs.count(); ++col_idx) {
      ObExpr *expr = rowid_exprs.at(col_idx);
      ObIVector *vec = nullptr;
      if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector is null", K(ret), K(col_idx));
      } else if (OB_FAIL(vec->from_row(rowid_meta, compact_row, batch_idx, col_idx))) {
        LOG_WARN("failed to set vector from compact row", K(ret), K(col_idx), K(batch_idx));
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::write_datum_to_uint64_rowid(const ObDatum &datum, const RowMeta &meta,
                                                const ExprFixedArray &rowid_exprs,
                                                ObDASRowID &rowid, common::ObIAllocator &alloc)
{
  UNUSED(meta);
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_exprs.empty()) || OB_ISNULL(rowid_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowid requires a doc id expr", K(ret), K(rowid_exprs.count()));
  } else {
    rowid.set_uint64(datum.get_uint());
  }
  return ret;
}

int ObDASSearchCtx::write_datum_to_compact_rowid(const ObDatum &datum, const RowMeta &meta,
                                                 const ExprFixedArray &rowid_exprs,
                                                 ObDASRowID &rowid, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid to write to should have exactly one expr", K(ret), K(rowid_exprs.count()));
  } else {
    ObSEArray<ObDatum, 1> datums;
    if (OB_FAIL(datums.push_back(datum))) {
      LOG_WARN("failed to push datum", K(ret));
    } else if (OB_FAIL(build_compact_rowid(datums, rowid_exprs, meta, rowid, alloc))) {
      LOG_WARN("failed to build compact rowid", K(ret));
    }
  }
  return ret;
}

int ObDASSearchCtx::build_compact_rowid(const common::ObIArray<ObDatum> &datums,
                                        const ExprFixedArray &rowid_exprs,
                                        const RowMeta &meta,
                                        ObDASRowID &rowid,
                                        common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t row_size = meta.get_row_fixed_size();
  if (OB_UNLIKELY(datums.count() != rowid_exprs.count() || datums.count() != meta.col_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch with rowid meta", K(ret), K(datums.count()),
             K(rowid_exprs.count()), K(meta.col_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datums.count(); ++i) {
      const ObDatum &datum = datums.at(i);
      if (OB_UNLIKELY(datum.is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowid datum must not be null", K(ret), K(i));
      } else if (!meta.is_reordered_fixed_expr(i)) {
        row_size += datum.len_;
      }
    }
    if (OB_SUCC(ret)) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(row_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate compact rowid buffer", K(ret), K(row_size));
      } else {
        ObCompactRow *row = new (buf) ObCompactRow;
        row->init(meta);
        row->set_row_size(static_cast<uint32_t>(row_size));
        for (int64_t i = 0; i < datums.count(); ++i) {
          const ObDatum &d = datums.at(i);
          row->set_cell_payload(meta, i, d.ptr_, d.len_);
        }
        rowid.set_compact_row(row);
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::write_datums_to_rowid(ObDASScopeId scope,
                                          const common::ObIArray<ObDatum> &datums,
                                          ObDASRowID &rowid,
                                          common::ObIAllocator &alloc) const
{
  int ret = OB_SUCCESS;
  const ObDASRowIDScope &s = scopes_[scope];
  if (OB_ISNULL(s.rowid_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scope rowid_exprs_ is null", K(ret), K(scope));
  } else if (s.rowid_type_ == DAS_ROWID_TYPE_UINT64) {
    // UINT64 RowIDs contain only doc_id, including in an asynchronous NESTED scope.
    if (OB_UNLIKELY(datums.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uint64 scope expects exactly 1 datum", K(ret), K(datums.count()));
    } else if (OB_UNLIKELY(s.rowid_exprs_->empty()) || OB_ISNULL(s.rowid_exprs_->at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uint64 scope requires a doc id expression", K(ret), K(scope));
    } else {
      rowid.set_uint64(datums.at(0).get_uint());
    }
  } else {
    // Compact RowIDs use the complete expression array.
    if (OB_UNLIKELY(datums.count() != s.rowid_exprs_->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum count mismatch with scope rowid_exprs", K(ret),
               K(datums.count()), K(s.rowid_exprs_->count()), K(scope));
    } else if (OB_FAIL(build_compact_rowid(datums, *s.rowid_exprs_, s.rowid_meta_, rowid, alloc))) {
      LOG_WARN("failed to build compact rowid for scope", K(ret), K(scope));
    }
  }
  return ret;
}

int ObDASSearchCtx::get_datum_from_uint64_rowid(const ObDASRowID &rowid, const RowMeta &meta,
                                                ObDatum &datum, int64_t col_idx)
{
  UNUSED(meta);
  UNUSED(col_idx);
  int ret = OB_SUCCESS;
  datum.ptr_ = reinterpret_cast<const char *>(&rowid.uint64_rowid_);
  datum.pack_ = sizeof(uint64_t);
  return ret;
}

int ObDASSearchCtx::get_datum_from_compact_rowid(const ObDASRowID &rowid, const RowMeta &meta,
                                                 ObDatum &datum, int64_t col_idx)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *row = rowid.get_compact_row();
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact row is null", K(ret));
  } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= meta.col_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col idx out of range", K(ret), K(col_idx), K(meta.col_cnt_));
  } else {
    datum = row->get_datum(meta, col_idx);
  }
  return ret;
}

const ObDASSearchCtx::RowIDFuncs ObDASSearchCtx::all_rowid_funcs_[DAS_ROWID_TYPE_MAX] = {
  {
    compare_uint64_rowid,
    deep_copy_uint64_rowid,
    create_rowid_store<UInt64RowIDStore>,
    uint64_rowid_to_expr,
    write_datum_to_uint64_rowid,
    get_datum_from_uint64_rowid,
  },
  {
    compare_compact_rowid,
    deep_copy_compact_rowid,
    create_rowid_store<CompactRowIDStore>,
    compact_rowid_to_expr,
    write_datum_to_compact_rowid,
    get_datum_from_compact_rowid,
  },
};

int ObDASSearchCtx::init_logical_table_ctx(const ObDASBaseCtDef *mock_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mock_ctdef) || !mock_ctdef->is_logical_table()) {
    logical_table_ctx_.is_enabled_ = false;
    logical_table_ctx_.logical_table_aux_type_ = share::LT_AUX_MAX_TYPE;
  } else {
    logical_table_ctx_.physical_data_table_id_ = mock_ctdef->physical_data_table_id_;
    logical_table_ctx_.logical_table_aux_type_ = mock_ctdef->logical_table_aux_type_;
    logical_table_ctx_.is_enabled_ = true;
    // namespace_id / logical_table_id are extracted from key_ranges at runtime
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

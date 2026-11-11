/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_sstable_scan_iter.h"

#include "storage/access/ob_dml_param.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace blocksstable;

ObIvfSSTableScanIter::ObIvfSSTableScanIter(ObIAllocator &allocator)
  : allocator_(allocator),
    ctx_guard_(),
    tablet_handle_(),
    scan_param_(nullptr),
    table_access_ctx_(),
    query_param_(),
    query_buffer_(nullptr),
    searcher_(nullptr),
    is_inited_(false),
    is_query_set_(false),
    is_sstable_switched_(false),
    profile_(nullptr),
    profile_post_filter_(false)
{
}

ObIvfSSTableScanIter::~ObIvfSSTableScanIter()
{
  reset();
}

int ObIvfSSTableScanIter::check_status() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(scan_param_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable scan iterator is not initialized", K(ret));
  } else if (OB_UNLIKELY(ObClockGenerator::getClock() > scan_param_->timeout_)) {
    ret = OB_TIMEOUT;
    LOG_WARN("ivf sstable scan iterator timeout", K(ret), K(scan_param_->timeout_));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("ivf sstable scan iterator interrupted", K(ret));
  }
  return ret;
}

int ObIvfSSTableScanIter::prepare_table_context()
{
  int ret = OB_SUCCESS;
  ObVersionRange trans_version_range;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->table_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf table scan parameter", K(ret), KP_(scan_param));
  } else {
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.base_version_ = 0;
    trans_version_range.snapshot_version_ = ctx_guard_.get_store_ctx().mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    if (OB_UNLIKELY(!trans_version_range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ivf transaction version range", K(ret), K(trans_version_range));
    } else if (OB_FAIL(table_access_ctx_.init(*scan_param_, ctx_guard_.get_store_ctx(), trans_version_range, nullptr))) {
      LOG_WARN("failed to initialize ivf table access context", K(ret), KPC_(scan_param));
    }
  }
  return ret;
}

int ObIvfSSTableScanIter::init(
    ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ivf table scan iterator is already initialized", K(ret));
  } else if (OB_UNLIKELY(!ctx_guard_.get_store_ctx().is_valid()
      || !scan_param.is_valid() || !tablet_handle.is_valid()
      || scan_param.allocator_ != &allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf table scan iterator argument", K(ret), K(scan_param));
  } else if (OB_FAIL(tablet_handle_.assign(tablet_handle))) {
    LOG_WARN("failed to retain ivf tablet handle", K(ret), K(tablet_handle));
  } else {
    scan_param_ = &scan_param;
    if (OB_FAIL(prepare_table_context())) {
      LOG_WARN("failed to prepare ivf table context", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObIvfSSTableScanIter::set_query(const ObIvfKnnSearchParam &query_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf table scan iterator is not initialized", K(ret));
  } else if (OB_UNLIKELY(is_query_set_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ivf query is already set", K(ret));
  } else if (OB_UNLIKELY(!query_param.is_common_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf query", K(ret), K(query_param));
  } else if (OB_ISNULL(query_buffer_ = static_cast<float *>(allocator_.alloc(query_param.dim_ * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to retain ivf query vector", K(ret), K(query_param.dim_));
  } else {
    MEMCPY(query_buffer_, query_param.query_, query_param.dim_ * sizeof(float));
    query_param_ = query_param;
    query_param_.query_ = query_buffer_;
    is_query_set_ = true;
  }
  return ret;
}

void ObIvfSSTableScanIter::reuse()
{
  set_profile(nullptr);
  if (nullptr != searcher_) {
    searcher_->reuse();
  }
}

int ObIvfSSTableScanIter::resolve_ivf_sstable(
    const ObNewVersionRange &version_range,
    const share::SCN &sql_read_snapshot,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  const ObITable::TableType preferred_table_type =
      ASYNC_INDEX_INITIAL_VERSION == version_range.base_version_
      ? ObITable::IVF_INDEX_SSTABLE : ObITable::INC_IVF_INDEX_SSTABLE;
  if (OB_FAIL(tablet_handle_.get_obj()->get_async_index_sstable(
      preferred_table_type, version_range, table_handle))) {
    LOG_WARN("failed to get ivf sstable", K(ret), K(preferred_table_type),
        K(version_range), K(sql_read_snapshot));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable from ivf table handle", K(ret), K(preferred_table_type),
        K(version_range));
  } else if (OB_ISNULL(sstable) || OB_UNLIKELY(!sstable->is_ivf_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-ivf sstable", K(ret), KP(sstable), K(preferred_table_type),
        K(version_range));
  }
  return ret;
}

int ObIvfSSTableScanIter::ensure_searcher(const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 retained_handle;
  ObSSTable *sstable = nullptr;
  if (OB_UNLIKELY(nullptr != searcher_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ivf sstable searcher is already created", K(ret));
  } else if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf sstable handle", K(ret), K(table_handle));
  } else {
    retained_handle = table_handle;
    if (OB_FAIL(retained_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get ivf sstable from table handle", K(ret), K(table_handle));
    } else if (OB_ISNULL(sstable) || OB_UNLIKELY(!sstable->is_ivf_sstable())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid non-ivf sstable handle", K(ret), KP(sstable), K(table_handle));
    } else if (OB_FAIL(static_cast<ObIvfSSTable *>(sstable)->create_searcher(
        table_handle, *scan_param_, table_access_ctx_, searcher_))) {
      LOG_WARN("failed to create reusable ivf sstable searcher", K(ret), K(table_handle));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(searcher_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf sstable searcher is null", K(ret));
  }
  return ret;
}

int ObIvfSSTableScanIter::switch_sstable(
    const ObNewVersionRange &version_range,
    const share::SCN &sql_read_snapshot)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 sstable_handle;
  is_sstable_switched_ = false;
  reuse();
  if (OB_UNLIKELY(!is_inited_ || !is_query_set_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf table scan iterator or query is not initialized", K(ret));
  } else if (OB_UNLIKELY(!version_range.is_valid() || !sql_read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf segment lookup argument", K(ret), K(version_range),
        K(sql_read_snapshot));
  } else if (OB_FAIL(check_status())) {
    LOG_WARN("ivf segment scan is interrupted", K(ret), K(version_range));
  } else {
    if (OB_FAIL(resolve_ivf_sstable(
        version_range, sql_read_snapshot, sstable_handle))) {
      LOG_WARN("failed to resolve ivf segment", K(ret), K(version_range),
          K(sql_read_snapshot));
    } else {
      if (nullptr == searcher_) {
        if (OB_FAIL(ensure_searcher(sstable_handle))) {
          LOG_WARN("failed to prepare reusable ivf searcher", K(ret), K(version_range));
        }
      } else if (OB_FAIL(searcher_->switch_sstable(sstable_handle))) {
        LOG_WARN("failed to switch ivf searcher sstable", K(ret), K(version_range));
      }
      if (OB_SUCC(ret)) {
        is_sstable_switched_ = true;
      }
    }
  }
  return ret;
}

int ObIvfSSTableScanIter::knn_search(
    const int64_t next_nprobe,
    ObIvfKnnSearchResult &result,
    const bool quantized_only)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher profile_switcher(profile_);
  ScopedTimer timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME, profile_);
  ScopedTimer callback_timer(ObMetricId::VEC_IVF_ITER_CALL_BACK_TIME,
                            profile_post_filter_ ? profile_ : nullptr);
  ObIvfKnnSearchParam round_query_param = query_param_;
  result.reset();
  if (OB_UNLIKELY(!is_inited_ || !is_query_set_
      || !is_sstable_switched_ || OB_ISNULL(searcher_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf table scan iterator is not ready for knn search", K(ret));
  } else if (OB_UNLIKELY(next_nprobe < 0 || (0 == next_nprobe && !query_param_.is_auto_search()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next nprobe for ivf knn search", K(ret), K(next_nprobe));
  } else if (OB_FAIL(check_status())) {
    LOG_WARN("ivf sstable knn search is interrupted", K(ret));
  } else {
    round_query_param.nprobe_ = next_nprobe;
    if (OB_FAIL(searcher_->knn_search(round_query_param, result, quantized_only))) {
      LOG_WARN("failed to search ivf sstable by knn", K(ret), K(round_query_param));
    }
    if (OB_SUCC(ret) && nullptr != profile_) {
      int ret = OB_SUCCESS; // Diagnostic errors must not change the search result.
      INC_METRIC_VAL(ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, result.count());
    }
  }
  if (OB_FAIL(ret)) {
    result.reset();
  }
  return ret;
}

int ObIvfSSTableScanIter::refine_candidates(const ObIvfKnnSearchResult &candidates,
    ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher profile_switcher(profile_);
  ScopedTimer timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME, profile_);
  ScopedTimer callback_timer(ObMetricId::VEC_IVF_ITER_CALL_BACK_TIME,
                            profile_post_filter_ ? profile_ : nullptr);
  result.reset();
  if (OB_UNLIKELY(!is_inited_ || !is_query_set_
      || !is_sstable_switched_ || OB_ISNULL(searcher_))) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_status())) {
    LOG_WARN("filtered IVF refinement interrupted", K(ret));
  } else if (OB_FAIL(searcher_->refine_candidates(query_param_, candidates, result))) {
    LOG_WARN("failed to refine filtered IVF candidates", K(ret));
  }
  return ret;
}

int ObIvfSSTableScanIter::next_candidate_batch(int64_t nprobe, float distance_threshold,
    ObIvfKnnSearchResult &result, ObIvfCandidateProgress &progress)
{
  int ret = OB_SUCCESS;
  result.reset();
  progress.reset();
  if (!is_inited_ || !is_query_set_ || !is_sstable_switched_ || nullptr == searcher_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF candidate scan is not ready", K(ret), K_(is_inited), K_(is_query_set),
             K_(is_sstable_switched), KP_(searcher));
  } else if (nprobe < 0 || (0 == nprobe && !query_param_.is_auto_search())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF candidate scan nprobe", K(ret), K(nprobe));
  } else if (OB_FAIL(check_status())) {
    LOG_WARN("IVF candidate scan interrupted", K(ret));
  } else {
    ObIvfKnnSearchParam query = query_param_;
    query.nprobe_ = nprobe;
    ret = searcher_->next_candidate_batch(query, distance_threshold, result, progress);
  }
  return ret;
}

bool ObIvfSSTableScanIter::has_remaining_centroids() const
{
  return nullptr != searcher_ && searcher_->has_remaining_centroids();
}

int ObIvfSSTableScanIter::brute_force_search(ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher profile_switcher(profile_);
  ScopedTimer timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME, profile_);
  result.reset();
  if (OB_UNLIKELY(!is_inited_ || !is_query_set_
      || !is_sstable_switched_ || OB_ISNULL(searcher_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf table scan iterator is not ready for brute-force search", K(ret));
  } else if (OB_FAIL(check_status())) {
    LOG_WARN("ivf sstable brute-force search is interrupted", K(ret));
  } else {
    if (OB_FAIL(searcher_->brute_force_search(query_param_, result))) {
      LOG_WARN("failed to search ivf sstable by brute force", K(ret), K_(query_param));
    }
    if (OB_SUCC(ret) && nullptr != profile_) {
      int ret = OB_SUCCESS; // Diagnostic errors must not change the search result.
      INC_METRIC_VAL(ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, result.count());
    }
  }
  if (OB_FAIL(ret)) {
    result.reset();
  }
  return ret;
}

void ObIvfSSTableScanIter::reset()
{
  set_profile(nullptr);
  if (nullptr != searcher_) {
    ObIvfSSTableSearcher::destroy(searcher_);
    searcher_ = nullptr;
  }
  table_access_ctx_.reset();
  tablet_handle_.reset();
  ctx_guard_.reset();
  if (nullptr != query_buffer_) {
    allocator_.free(query_buffer_);
    query_buffer_ = nullptr;
  }
  scan_param_ = nullptr;
  query_param_.reset();
  is_inited_ = false;
  is_query_set_ = false;
  is_sstable_switched_ = false;
}

} // namespace storage
} // namespace oceanbase

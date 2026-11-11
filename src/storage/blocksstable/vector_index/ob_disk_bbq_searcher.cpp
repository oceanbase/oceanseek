/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_disk_bbq_searcher.h"
#include "storage/blocksstable/vector_index/ob_ivf_search_budget.h"
#include "share/diagnosis/ob_runtime_profile.h"

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

#include "lib/alloc/alloc_struct.h"
#include "lib/utility/ob_sort.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/quantization/ob_quantization_struct.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;

struct ObIvfRawRefineEntry
{
  ObIvfRawRefineEntry() : cid_(0), vid_(0), original_index_(0) {}
  ObIvfRawRefineEntry(
      const int64_t cid, const int64_t vid, const int64_t original_index)
    : cid_(cid), vid_(vid), original_index_(original_index)
  {}
  TO_STRING_KV(K_(cid), K_(vid), K_(original_index));
  int64_t cid_;
  int64_t vid_;
  int64_t original_index_;
};

class ObIvfRawRefineEntryVidLess
{
public:
  bool operator()(const ObIvfRawRefineEntry &lhs, const ObIvfRawRefineEntry &rhs) const
  {
    return lhs.vid_ < rhs.vid_;
  }
};

class ObDiskBBQKnnBoundGreater
{
public:
  bool operator()(const ObIvfKnnEntry &lhs, const ObIvfKnnEntry &rhs) const
  {
    return lhs.distance_ > rhs.distance_;
  }
};

ObDiskBBQSearcher::ObDiskBBQSearcher(ObIAllocator &allocator)
  : ObIvfSSTableSearcher(allocator), fast_scanner_(), raw_vector_multi_getter_(nullptr),
    post_centroids_remaining_(0),
    post_scan_open_(false), post_started_(false), initial_child_budget_(-1),
    remaining_pre_filter_budget_(-1),
    auto_nprobe_(0), need_init_auto_nprobe_(false),
    refined_vids_(nullptr) {}

ObDiskBBQSearcher::~ObDiskBBQSearcher()
{
  do_close_sstable();
}

int ObDiskBBQSearcher::open(
    const ObTableHandleV2 &table_handle,
    const ObTableScanParam &scan_param,
    ObTableAccessContext &access_ctx)
{
  return init_common(table_handle, scan_param, access_ctx);
}

int ObDiskBBQSearcher::do_open_sstable()
{
  int ret = OB_SUCCESS;
  ObIvfSSTable &ivf_sstable = *sstable();
  ObTableAccessContext &table_access_ctx = *access_ctx();
  if (ivf_sstable.get_ivf_meta().is_empty() || ivf_sstable.no_data_to_read()) {
    // Empty segments keep the reusable searcher/query alive but open no
    // algorithm-specific readers.
  } else if (OB_UNLIKELY(!ivf_sstable.get_fht_rotator().is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ SSTable FHT rotator is not initialized", K(ret), K(ivf_sstable.get_ivf_meta()));
  } else if (OB_FAIL(fast_scanner_.init(
      iter_param(), output_row_desc(), ivf_sstable, table_access_ctx))) {
    LOG_WARN("failed to initialize DiskBBQ low precision vector scanner", K(ret));
  } else {
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(table_access_ctx, ObSSTableRowMultiGetter, raw_vector_multi_getter_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to allocate RAW_VECTOR multi-getter", K(ret));
    }
  }
  return ret;
}

void ObDiskBBQSearcher::do_close_sstable()
{
  do_reuse();
  release_raw_vector_multi_getter();
  fast_scanner_.reset();
}

void ObDiskBBQSearcher::do_reuse()
{
  reset_refined_vids();
  if (post_scan_open_) { fast_scanner_.close_scan(); }
  if (nullptr != raw_vector_multi_getter_) { raw_vector_multi_getter_->reuse(); }
  post_centroids_remaining_ = 0;
  post_scan_open_ = false;
  post_started_ = false;
  initial_child_budget_ = -1;
  remaining_pre_filter_budget_ = -1;
  auto_nprobe_ = 0;
  need_init_auto_nprobe_ = false;
}

int64_t ObDiskBBQSearcher::compute_auto_scan_budget(
    const int64_t k, const int64_t num_candidates, const int64_t vector_count)
{
  return calc_ivf_centroid_child_budget(k, num_candidates, vector_count);
}

int64_t ObDiskBBQSearcher::compute_auto_num_candidates(const int64_t k, const int64_t vector_count)
{
  int64_t candidates = k;
  if (k > 0 && vector_count > 0) {
    // Keep the default 4x target at one million vectors, with a bounded
    // increase or decrease for larger or smaller indexes.
    const double refine_k = std::max(1.0, std::min(8.0,
        2.0 + std::log10(static_cast<double>(vector_count) / 10000.0)));
    const double target = std::ceil(static_cast<double>(k) * refine_k);
    candidates = target >= static_cast<double>(INT64_MAX)
        ? INT64_MAX : std::max(k, static_cast<int64_t>(target));
  }
  return candidates;
}

int64_t ObDiskBBQSearcher::adjust_pre_filter_scan_budget(
    const int64_t budget, const int64_t vector_count, const double selectivity)
{
  int64_t adjusted = budget;
  if (budget > 0 && vector_count > 0) {
    // PRE filtering already expands the scan for selectivity. Dampen that
    // expansion on large segments after the common square-root size scaling.
    double scale = std::pow(1000000.0 / vector_count, 0.08);
    if (std::isfinite(selectivity) && selectivity > 0.0 && selectivity <= 1.0) {
      // Increase coverage around a 10% pass rate, but avoid scanning toward
      // an exact result when the allowed set is very small.
      const double bounded_selectivity = std::max(0.01, selectivity);
      const double size_ratio = std::min(1.0, std::max(0.0,
          std::log10(static_cast<double>(vector_count) / 1000000.0)));
      const double large_segment_factor = std::pow(
          std::max(1.0, static_cast<double>(vector_count) / 1000000.0), 0.05);
      const double sparse_filter_factor = std::pow(
          std::min(1.0, bounded_selectivity / 0.1), 0.08 * size_ratio);
      scale *= 1.05 * std::pow(1.0 / std::max(0.1, bounded_selectivity), 0.35)
          * large_segment_factor * sparse_filter_factor;
    }
    const int64_t max_budget = vector_count > INT64_MAX / 2 ? INT64_MAX : 2 * vector_count;
    const double target = std::ceil(static_cast<double>(budget) * scale);
    adjusted = target >= static_cast<double>(max_budget)
        ? max_budget : std::max<int64_t>(1, static_cast<int64_t>(target));
  }
  return adjusted;
}

int64_t ObDiskBBQSearcher::adjust_auto_scan_budget(
    const int64_t budget, const int64_t vector_count, const double selectivity,
    const bool is_pre_filter)
{
  int64_t adjusted = budget;
  if (budget > 0 && vector_count > 0) {
    // Let the scan budget grow with the square root of segment size.
    const double size_scale = 1.15 * std::sqrt(1000000.0 / vector_count);
    double filter_scale = 1.0;
    if (!is_pre_filter && std::isfinite(selectivity)
        && selectivity >= 0.0 && selectivity <= 1.0) {
      const double pass_rate = std::max(0.1, selectivity);
      // Compensate for fewer matching vectors without scaling as fast as
      // 1 / pass_rate, and bound the cost of very selective POST filters.
      filter_scale = std::min(2.1, 1.1 / std::cbrt(pass_rate));
    }
    const int64_t max_budget = vector_count > INT64_MAX / 2 ? INT64_MAX : 2 * vector_count;
    const double target = std::ceil(static_cast<double>(budget) * size_scale * filter_scale);
    adjusted = target >= static_cast<double>(max_budget)
        ? max_budget : std::max<int64_t>(1, static_cast<int64_t>(target));
    if (is_pre_filter) {
      adjusted = adjust_pre_filter_scan_budget(adjusted, vector_count, selectivity);
    }
  }
  return adjusted;
}

int64_t ObDiskBBQSearcher::get_effective_nprobe(const ObIvfKnnSearchParam &query_param) const
{
  return query_param.nprobe_ > 0 ? query_param.nprobe_ : auto_nprobe_;
}

int ObDiskBBQSearcher::prepare_centroid_batch(const ObIvfKnnSearchParam &query_param)
{
  int ret = OB_SUCCESS;
  if (need_init_auto_nprobe_) {
    int64_t initial_count = 0;
    if (OB_FAIL(centroid_iterator()->prepare_initial_batch(
        initial_child_budget_, query_param.cluster_size_, initial_count))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("[DISK_BBQ] failed to select initial centroids by child count", K(ret),
            K_(initial_child_budget), K(query_param));
      }
    } else {
      // PRE may skip posting batches, so its final window is determined while
      // scanning. Other paths know the complete window from child counts.
      auto_nprobe_ = is_pre_filter_budget_scan() ? 0 : initial_count;
      initial_child_budget_ = 0;
      need_init_auto_nprobe_ = false;
      LOG_TRACE("[DISK_BBQ] selected automatic initial centroid count", K_(auto_nprobe));
    }
  } else if (!centroid_iterator()->has_buffered_centroids()) {
    const int64_t next_count = is_pre_filter_budget_scan() ? 8 : get_effective_nprobe(query_param);
    ret = centroid_iterator()->prepare_next_batch(next_count);
  }
  return ret;
}

void ObDiskBBQSearcher::consume_pre_filter_budget(const int64_t count)
{
  if (is_pre_filter_budget_scan() && count > 0) {
    remaining_pre_filter_budget_ -= std::min(remaining_pre_filter_budget_, count);
  }
}

void ObDiskBBQSearcher::finish_pre_filter_budget_scan()
{
  if (is_pre_filter_budget_scan()) {
    LOG_TRACE("[DISK_BBQ] automatic PRE window finished", K_(auto_nprobe), K_(remaining_pre_filter_budget));
    remaining_pre_filter_budget_ = -1;
  }
}

int ObDiskBBQSearcher::estimate_legacy_vector_count(int64_t &vector_count)
{
  int ret = OB_SUCCESS;
  vector_count = 0;
  ObStorageDatum start_datums[IVF_ROWKEY_COL_COUNT];
  ObStorageDatum end_datums[IVF_ROWKEY_COL_COUNT];
  ObDatumRange range;
  start_datums[IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_RAW_VECTOR);
  end_datums[IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_RAW_VECTOR);
  start_datums[IVF_ROWKEY_COL_CID].set_int(IVF_RAW_VECTOR_CID);
  end_datums[IVF_ROWKEY_COL_CID].set_int(IVF_RAW_VECTOR_CID);
  start_datums[IVF_ROWKEY_COL_VID].set_min();
  end_datums[IVF_ROWKEY_COL_VID].set_max();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  if (OB_FAIL(range.start_key_.assign(start_datums, IVF_ROWKEY_COL_COUNT))) {
    LOG_WARN("failed to assign legacy RAW_VECTOR range start", K(ret));
  } else if (OB_FAIL(range.end_key_.assign(end_datums, IVF_ROWKEY_COL_COUNT))) {
    LOG_WARN("failed to assign legacy RAW_VECTOR range end", K(ret));
  } else if (OB_FAIL(ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
                 *sstable(), read_info(), range, nullptr, vector_count))) {
    LOG_WARN("failed to estimate legacy RAW_VECTOR range", K(ret), K(range));
  }
  return ret;
}

int ObDiskBBQSearcher::prepare_query(
    const ObIvfKnnSearchParam &query_param)
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  if (query_param.is_auto_search() && OB_UNLIKELY(meta.vector_count_ < 0)) {
    ret = OB_INVALID_DATA;
  } else if (initial_child_budget_ < 0 && query_param.is_auto_search()) {
    int64_t vector_count = meta.vector_count_;
    // Legacy metadata has no vector_count. Count only the RAW_VECTOR key range
    // through index rows, never all physical rows (centroids/SOAR postings), and
    // never load vector payloads. Boundary blocks make this a conservative upper
    // bound for the scan budget. Keep it query-local: do not rewrite old metadata.
    if (0 == vector_count && OB_FAIL(estimate_legacy_vector_count(vector_count))) {
      LOG_WARN("failed to estimate legacy DiskBBQ vector count", K(ret));
    } else if (OB_UNLIKELY(vector_count <= 0)) {
      ret = OB_INVALID_DATA;
    } else {
      // Match mainline posting-budget tuning without changing the configured
      // refinement multiplier used by SQL candidate collection and parent routing.
      const int64_t num_candidates = query_param.is_set_refine_k_
          ? query_param.num_candidates_
          : compute_auto_num_candidates(query_param.original_k_, vector_count);
      initial_child_budget_ = compute_auto_scan_budget(
          query_param.original_k_, num_candidates, vector_count);
      if (!query_param.is_set_refine_k_) {
        initial_child_budget_ = adjust_auto_scan_budget(initial_child_budget_,
            vector_count, query_param.selectivity_, nullptr != query_param.pre_filter_);
      }
      need_init_auto_nprobe_ = true;
      if (nullptr != query_param.pre_filter_) { remaining_pre_filter_budget_ = initial_child_budget_; }
      LOG_TRACE("initialize DiskBBQ automatic scan budget",
          K(query_param.original_k_), K(num_candidates), K(query_param.is_set_refine_k_),
          K(query_param.selectivity_), K(meta.vector_count_), K(vector_count),
          K_(initial_child_budget), K_(remaining_pre_filter_budget));
    }
  }
  auto &context = query_vector_context();
  const auto metric = static_cast<share::ObVectorIndexDistAlgorithm>(meta.dist_type_);
  float *prepared_query = nullptr;
  if (OB_FAIL(ret)) {
    LOG_WARN("[DISK_BBQ] failed to prepare automatic centroid budget", K(ret), K(meta), K(query_param));
  } else if (context.is_inited()) {
    // Public search entry points have validated the query and SSTable metadata.
    // This searcher retains one immutable query across rounds and index segments.
  } else if ((prepared_query = static_cast<float *>(query_allocator().alloc(meta.dim_ * sizeof(float)))) == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate rotated DiskBBQ query", K(ret), K(meta.dim_), K(query_param));
  } else {
    for (int64_t d = 0; OB_SUCC(ret) && d < meta.dim_; ++d) {
      // Query payloads need not be aligned for float loads.
      MEMCPY(prepared_query + d,
             reinterpret_cast<const char *>(query_param.query_) + d * sizeof(float), sizeof(float));
      if (OB_UNLIKELY(!std::isfinite(prepared_query[d]))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "DISK_BBQ search: query vector must contain only finite values (Inf/NaN are not allowed)");
        LOG_WARN("non-finite DiskBBQ query vector", K(ret), K(d));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (share::VIDA_COS == meta.dist_type_
        && OB_FAIL(share::ObVectorNormalize::L2_normalize_vector(
            meta.dim_, prepared_query, prepared_query))) {
      LOG_WARN("failed to normalize DiskBBQ cosine query", K(ret));
    } else if (OB_FAIL(sstable()->get_fht_rotator().rotate(meta.dim_, prepared_query))) {
      LOG_WARN("failed to rotate DiskBBQ query", K(ret));
    } else if (OB_FAIL(context.init(query_allocator(), prepared_query, meta.dim_, metric))) {
      LOG_WARN("failed to initialize DiskBBQ query vector context", K(ret));
    }
    if (OB_FAIL(ret)) { query_allocator().free(prepared_query); }
  }
  return ret;
}

void ObDiskBBQSearcher::release_raw_vector_multi_getter()
{
  if (nullptr != raw_vector_multi_getter_) {
    ObTableAccessContext *table_access_ctx = access_ctx();
    raw_vector_multi_getter_->reuse();
    if (nullptr != table_access_ctx && nullptr != table_access_ctx->get_stmt_iter_pool()) {
      table_access_ctx->get_stmt_iter_pool()->return_iter(raw_vector_multi_getter_);
    } else if (nullptr != table_access_ctx) {
      raw_vector_multi_getter_->~ObStoreRowIterator();
      FREE_TABLE_STORE_ROW_IETRATOR(*table_access_ctx, raw_vector_multi_getter_);
    }
    raw_vector_multi_getter_ = nullptr;
  }
}

int ObDiskBBQSearcher::collect_raw_vector_row(
    const ObDatumRow *row,
    const ObIvfKnnEntry &candidate,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector)
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  const ObIvfSSTableRowDesc &row_desc = output_row_desc();
  const int64_t expected_column_count = read_info().get_request_count();
  const int64_t raw_vector_size = static_cast<int64_t>(meta.dim_) * sizeof(float);
  if (OB_ISNULL(row) || row->get_column_count() < expected_column_count) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid RAW_VECTOR row", K(ret), K(candidate), KP(row));
  } else {
    const ObStorageDatum &type_datum = row->storage_datums_[row_desc.row_type_col_idx_];
    const ObStorageDatum &cid_datum = row->storage_datums_[row_desc.centroid_id_col_idx_];
    const ObStorageDatum &vid_datum = row->storage_datums_[row_desc.vector_id_col_idx_];
    const ObStorageDatum &payload_datum = row->storage_datums_[row_desc.payload_col_idx_];
    float exact_distance = 0.0F;
    if (OB_UNLIKELY(type_datum.is_null()
        || OB_ISNULL(type_datum.ptr_) || type_datum.len_ != sizeof(int64_t)
        || cid_datum.is_null() || OB_ISNULL(cid_datum.ptr_) || cid_datum.len_ != sizeof(int64_t)
        || vid_datum.is_null() || OB_ISNULL(vid_datum.ptr_) || vid_datum.len_ != sizeof(int64_t)
        || payload_datum.is_null() || OB_ISNULL(payload_datum.ptr_) || payload_datum.len_ != raw_vector_size)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid RAW_VECTOR row datum", K(ret), K(candidate),
          "type_null", type_datum.is_null(), "type_len", type_datum.len_,
          "cid_null", cid_datum.is_null(), "cid_len", cid_datum.len_,
          "vid_null", vid_datum.is_null(), "vid_len", vid_datum.len_,
          "payload_null", payload_datum.is_null(), "payload_len", payload_datum.len_,
          K(raw_vector_size));
    } else if (OB_UNLIKELY(type_datum.get_int() != IVF_ROW_TYPE_RAW_VECTOR
        || cid_datum.get_int() != IVF_RAW_VECTOR_CID
        || vid_datum.get_int() != candidate.vid_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("mismatched RAW_VECTOR identity", K(ret), K(candidate),
          "row_type", type_datum.get_int(), "row_cid", cid_datum.get_int(),
          "row_vid", vid_datum.get_int());
    } else if (OB_FAIL(computer.compute({const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(payload_datum.get_string().ptr())), raw_vector_size}, exact_distance))) {
      LOG_WARN("failed to compute exact distance", K(ret), K(candidate));
    } else if (OB_FAIL(final_collector.push(candidate.cid_, candidate.vid_, exact_distance))) {
      LOG_WARN("failed to update exact IVF top-k", K(ret), K(candidate));
    }
  }
  return ret;
}

int ObDiskBBQSearcher::read_raw_vector_batch(
    const ObIvfKnnSearchParam &query_param,
    const int64_t *vids,
    const int64_t vid_count,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector)
{
  int ret = OB_SUCCESS;
  ObTableAccessContext *table_access_ctx = access_ctx();
  const ObITableReadInfo *read_info = iter_param().read_info_;
  ObIvfSSTableReadStats *read_stats = query_param.read_stats_;
  ObStorageDatum key_datums[ObDistBBQFastScanBatch::CAPACITY][IVF_ROWKEY_COL_COUNT];
  ObSEArray<ObDatumRowkey, ObDistBBQFastScanBatch::CAPACITY> rowkeys;
  const uint64_t original_scan_order = nullptr == table_access_ctx
      ? ObQueryFlag::KeepOrder : table_access_ctx->query_flag_.scan_order_;
  if (OB_UNLIKELY(nullptr == vids || vid_count <= 0
      || vid_count > ObDistBBQFastScanBatch::CAPACITY
      || OB_ISNULL(table_access_ctx) || OB_ISNULL(read_info)
      || OB_ISNULL(raw_vector_multi_getter_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid brute-force RAW_VECTOR batch argument", K(ret),
        KP(vids), K(vid_count), KP(table_access_ctx), KP(read_info),
        KP(raw_vector_multi_getter_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vid_count; ++i) {
    if (OB_UNLIKELY(vids[i] < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid brute-force RAW_VECTOR identity", K(ret), K(i), K(vids[i]));
    } else {
      key_datums[i][IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_RAW_VECTOR);
      key_datums[i][IVF_ROWKEY_COL_CID].set_int(IVF_RAW_VECTOR_CID);
      key_datums[i][IVF_ROWKEY_COL_VID].set_int(vids[i]);
      ObDatumRowkey rowkey;
      if (OB_FAIL(rowkey.assign(key_datums[i], ARRAYSIZEOF(key_datums[i])))) {
        LOG_WARN("failed to assign brute-force RAW_VECTOR rowkey", K(ret), K(i), K(vids[i]));
      } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
        LOG_WARN("failed to append brute-force RAW_VECTOR rowkey", K(ret), K(i), K(vids[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr != read_stats) {
      read_stats->raw_vector_read_count_ =
          INT64_MAX == read_stats->raw_vector_read_count_
          ? INT64_MAX : read_stats->raw_vector_read_count_ + 1;
      read_stats->raw_vector_key_count_ =
          vid_count > INT64_MAX - read_stats->raw_vector_key_count_
          ? INT64_MAX : read_stats->raw_vector_key_count_ + vid_count;
    }
    {
      ObOpProfile<ObMetric> *profile = get_current_profile();
      if (nullptr != profile) {
        int ret = OB_SUCCESS; // Keep diagnostic errors local.
        INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 1);
        INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_KEY_COUNT, vid_count);
      }
    }
    table_access_ctx->query_flag_.scan_order_ = ObQueryFlag::Forward;
    raw_vector_multi_getter_->reuse();
    if (OB_FAIL(raw_vector_multi_getter_->init(
        iter_param(), *table_access_ctx, sstable(), &rowkeys))) {
      LOG_WARN("failed to initialize reusable brute-force RAW_VECTOR multi-getter",
          K(ret), K(vid_count));
    }
  }
  const ObDatumRow *row = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < vid_count; ++i) {
    const int next_ret = raw_vector_multi_getter_->get_next_row(row);
    if (OB_ITER_END == next_ret) {
      ret = OB_INVALID_DATA;
      LOG_WARN("brute-force RAW_VECTOR multi-getter ended early",
          K(ret), K(i), K(vid_count), K(vids[i]));
    } else if (OB_SUCCESS != next_ret) {
      ret = next_ret;
      LOG_WARN("failed to multi-get brute-force RAW_VECTOR row",
          K(ret), K(i), K(vids[i]));
    } else if (OB_ISNULL(row)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("brute-force RAW_VECTOR multi-getter returned null row",
          K(ret), K(i), K(vids[i]));
    } else if (row->row_flag_.is_not_exist()) {
      LOG_TRACE("skipping missing brute-force RAW_VECTOR", K(i), K(vids[i]));
    } else {
      ObIvfKnnEntry candidate;
      candidate.cid_ = IVF_RAW_VECTOR_CID;
      candidate.vid_ = vids[i];
      candidate.distance_ = 0.0F;
      if (OB_FAIL(collect_raw_vector_row(
          row, candidate, computer, final_collector))) {
        LOG_WARN("failed to collect brute-force RAW_VECTOR row",
            K(ret), K(i), K(candidate));
      } else {
        if (nullptr != read_stats) {
          read_stats->raw_vector_refine_count_ =
              INT64_MAX == read_stats->raw_vector_refine_count_
              ? INT64_MAX : read_stats->raw_vector_refine_count_ + 1;
        }
        {
          ObOpProfile<ObMetric> *profile = get_current_profile();
          if (nullptr != profile) {
            int ret = OB_SUCCESS; // Keep diagnostic errors local.
            INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, 1);
          }
        }
      }
    }
  }
  if (nullptr != table_access_ctx) {
    table_access_ctx->query_flag_.scan_order_ = original_scan_order;
  }
  return ret;
}

int ObDiskBBQSearcher::collect_brute_force_candidates(
    const ObIvfKnnSearchParam &query_param,
    ObIvfPreFilterIter &allowed_iter,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector)
{
  int ret = OB_SUCCESS;
  bool is_iter_end = false;
  int64_t examined = 0;
  while (OB_SUCC(ret) && !is_iter_end) {
    int64_t vids[ObDistBBQFastScanBatch::CAPACITY];
    int64_t vid_count = 0;
    while (OB_SUCC(ret)
        && vid_count < ObDistBBQFastScanBatch::CAPACITY
        && !is_iter_end) {
      if (0 == (examined++ % ObDistBBQFastScanBatch::CAPACITY)) {
        if (nullptr == access_ctx()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("missing IVF brute-force access context", K(ret), K(examined), K(vid_count));
        } else if (access_ctx()->timeout_ > 0 && ObClockGenerator::getClock() > access_ctx()->timeout_) {
          ret = OB_TIMEOUT;
          LOG_WARN("IVF brute-force scan timed out", K(ret), K(access_ctx()->timeout_), K(examined));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("brute-force IVF interrupted", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        break;
      }
      const uint64_t value = allowed_iter.get_curr_value();
      if (OB_UNLIKELY(value > static_cast<uint64_t>(INT64_MAX))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("brute-force pre-filter VID exceeds INT64_MAX", K(ret), K(value));
      } else {
        // Deletion takes precedence over the allowed set, just as in ANN
        // refine. Filter before point-get so deleted rows consume no RAW I/O.
        if (nullptr == query_param.deletion_bitmap_handle_
            || !query_param.deletion_bitmap_handle_->contains(static_cast<int64_t>(value))) {
          vids[vid_count++] = static_cast<int64_t>(value);
        }
        const int next_ret = allowed_iter.get_next();
        if (OB_ITER_END == next_ret) {
          is_iter_end = true;
        } else if (OB_SUCCESS != next_ret) {
          ret = next_ret;
          LOG_WARN("failed to iterate brute-force pre-filter", K(ret), K(vid_count));
        }
      }
    }
    if (OB_SUCC(ret) && vid_count > 0 && OB_FAIL(read_raw_vector_batch(
        query_param, vids, vid_count, computer, final_collector))) {
      LOG_WARN("failed to read brute-force RAW_VECTOR batch", K(ret), K(vid_count));
    }
  }
  return ret;
}

int ObDiskBBQSearcher::do_brute_force_search(
    const ObIvfKnnSearchParam &query_param,
    ObIvfTopKCollector &collector)
{
  int ret = OB_SUCCESS;
  ObIvfPreFilterIter allowed_iter(*query_param.pre_filter_);
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  if (post_started_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot switch IVF POST cursor to brute-force search before reuse", K(ret));
  } else if (meta.is_empty() || sstable()->no_data_to_read()) {
  } else {
    const int init_ret = allowed_iter.init();
    if (OB_ITER_END == init_ret) {
      // An initialized empty pre-filter performs no query preparation or I/O.
    } else if (OB_SUCCESS != init_ret) {
      ret = init_ret;
      LOG_WARN("failed to initialize brute-force pre-filter iterator", K(ret));
    } else if (OB_FAIL(prepare_query(query_param))) {
      LOG_WARN("failed to prepare DiskBBQ brute-force query", K(ret), K(query_param));
    } else {
      if (OB_FAIL(collect_brute_force_candidates(
          query_param, allowed_iter, query_vector_context().computer(), collector))) {
        LOG_WARN("failed to collect brute-force RAW_VECTOR candidates", K(ret));
      }
    }
  }
  allowed_iter.deinit();
  return ret;
}

int ObDiskBBQSearcher::refine_vector(
    const ObIvfKnnSearchParam &query_param,
    const ObIvfKnnEntry *candidates,
    const int64_t candidate_count,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector)
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  if (1 == meta.nbits_) {
    if (OB_FAIL(refine_raw_vector(
        query_param, candidates, candidate_count, computer, final_collector))) {
      LOG_WARN("failed to refine one-bit DiskBBQ batch with RAW_VECTOR",
          K(ret), K(candidate_count));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("HIGH_PRECISION_VECTOR refinement is not supported yet",
        K(ret), K(meta.nbits_), K(candidate_count));
  }
  return ret;
}

int ObDiskBBQSearcher::mark_raw_vector_refined(const int64_t vid)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapIVF"));
  if (nullptr == refined_vids_) {
    ROARING_TRY_CATCH(refined_vids_ = roaring::api::roaring64_bitmap_create());
    if (OB_SUCC(ret) && OB_ISNULL(refined_vids_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(refined_vids_, static_cast<uint64_t>(vid)));
  return ret;
}

void ObDiskBBQSearcher::reset_refined_vids()
{
  if (nullptr != refined_vids_) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapIVF"));
    roaring::api::roaring64_bitmap_free(refined_vids_);
    refined_vids_ = nullptr;
  }
}

int ObDiskBBQSearcher::refine_raw_vector(
    const ObIvfKnnSearchParam &query_param,
    const ObIvfKnnEntry *candidates,
    const int64_t candidate_count,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector)
{
  int ret = OB_SUCCESS;
  ObTableAccessContext *table_access_ctx = access_ctx();
  const ObITableReadInfo *read_info = iter_param().read_info_;
  ObIvfSSTableReadStats *read_stats = query_param.read_stats_;
  ObIvfRawRefineEntry entries[ObDistBBQFastScanBatch::CAPACITY];
  ObStorageDatum key_datums[ObDistBBQFastScanBatch::CAPACITY][IVF_ROWKEY_COL_COUNT];
  ObSEArray<ObDatumRowkey, ObDistBBQFastScanBatch::CAPACITY> rowkeys;
  const uint64_t original_scan_order = nullptr == table_access_ctx
      ? ObQueryFlag::KeepOrder : table_access_ctx->query_flag_.scan_order_;
  if (OB_UNLIKELY(nullptr == candidates || candidate_count <= 0
      || candidate_count > ObDistBBQFastScanBatch::CAPACITY
      || OB_ISNULL(table_access_ctx) || OB_ISNULL(read_info))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RAW_VECTOR multi-get argument", K(ret), KP(candidates),
        K(candidate_count), KP(table_access_ctx), KP(read_info));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_count; ++i) {
    if (OB_UNLIKELY(candidates[i].vid_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid RAW_VECTOR identity", K(ret), K(i), K(candidates[i]));
    } else {
      entries[i] = ObIvfRawRefineEntry(candidates[i].cid_, candidates[i].vid_, i);
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(entries, entries + candidate_count,
        ObIvfRawRefineEntryVidLess());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_count; ++i) {
    key_datums[i][IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_RAW_VECTOR);
    key_datums[i][IVF_ROWKEY_COL_CID].set_int(IVF_RAW_VECTOR_CID);
    key_datums[i][IVF_ROWKEY_COL_VID].set_int(entries[i].vid_);
    ObDatumRowkey rowkey;
    if (OB_FAIL(rowkey.assign(key_datums[i], ARRAYSIZEOF(key_datums[i])))) {
      LOG_WARN("failed to assign RAW_VECTOR rowkey", K(ret), K(i), K(entries[i]));
    } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("failed to append RAW_VECTOR rowkey", K(ret), K(i), K(entries[i]));
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr != read_stats) {
      ++read_stats->raw_vector_read_count_;
      read_stats->raw_vector_key_count_ =
          candidate_count > INT64_MAX - read_stats->raw_vector_key_count_
          ? INT64_MAX : read_stats->raw_vector_key_count_ + candidate_count;
    }
    {
      ObOpProfile<ObMetric> *profile = get_current_profile();
      if (nullptr != profile) {
        int ret = OB_SUCCESS; // Keep diagnostic errors local.
        INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 1);
        INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_KEY_COUNT, candidate_count);
      }
    }
    table_access_ctx->query_flag_.scan_order_ = ObQueryFlag::Forward;
    raw_vector_multi_getter_->reuse();
    if (OB_FAIL(raw_vector_multi_getter_->init(
        iter_param(), *table_access_ctx, sstable(), &rowkeys))) {
      LOG_WARN("failed to initialize reusable RAW_VECTOR multi-getter",
          K(ret), K(candidate_count));
    }
  }
  const ObDatumRow *row = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_count; ++i) {
    const ObIvfRawRefineEntry &entry = entries[i];
    const ObIvfKnnEntry &candidate = candidates[entry.original_index_];
    float distance_threshold = 0.0F;
    const int next_ret = raw_vector_multi_getter_->get_next_row(row);
    if (OB_ITER_END == next_ret) {
      ret = OB_INVALID_DATA;
      LOG_WARN("RAW_VECTOR multi-getter ended early", K(ret), K(i), K(entry));
    } else if (OB_SUCCESS != next_ret) {
      ret = next_ret;
      LOG_WARN("failed to multi-get RAW_VECTOR row", K(ret), K(i), K(entry));
    } else if (OB_FAIL(final_collector.get_distance_threshold(distance_threshold))) {
      LOG_WARN("failed to get latest exact Top-K threshold", K(ret), K(i), K(entry));
    } else if (candidate.distance_ > distance_threshold) {
      // The batch was selected with the threshold available before its
      // multi-get. Exact distances computed for earlier rows may have tightened
      // that threshold, matching Extended-RaBitQ's per-candidate distk update.
      LOG_TRACE("skipping stale DiskBBQ refine candidate",
          K(candidate), K(distance_threshold));
    } else if (OB_FAIL(collect_raw_vector_row(
        row, candidate, computer, final_collector))) {
      LOG_WARN("failed to collect RAW_VECTOR row", K(ret), K(i), K(entry));
    } else if (!post_started_ && OB_FAIL(mark_raw_vector_refined(candidate.vid_))) {
      LOG_WARN("failed to remember refined RAW_VECTOR", K(ret), K(i), K(entry));
    } else {
      if (nullptr != read_stats) {
        read_stats->raw_vector_refine_count_ =
            INT64_MAX == read_stats->raw_vector_refine_count_
            ? INT64_MAX : read_stats->raw_vector_refine_count_ + 1;
      }
      {
        ObOpProfile<ObMetric> *profile = get_current_profile();
        if (nullptr != profile) {
          int ret = OB_SUCCESS; // Keep diagnostic errors local.
          INC_METRIC_VAL(ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, 1);
        }
      }
    }
  }
  if (nullptr != table_access_ctx) {
    table_access_ctx->query_flag_.scan_order_ = original_scan_order;
  }
  return ret;
}

int ObDiskBBQSearcher::refine_pending_candidates(
    const ObIvfKnnSearchParam &query_param,
    ObSEArray<ObIvfKnnEntry, 256> &pending,
    const quantization::ObQuantizationComputer &computer,
    ObIvfTopKCollector &final_collector,
    float &distance_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && !pending.empty()) {
    const ObDiskBBQKnnBoundGreater greater_bound;
    // A min-heap visits only the promising prefix; unlike a full sort, it
    // does not order candidates that the final exact threshold will reject.
    std::make_heap(pending.begin(), pending.end(), greater_bound);
    ObIvfKnnEntry refine_batch[ObDistBBQFastScanBatch::CAPACITY];
    int64_t refine_count = 0;
    while (OB_SUCC(ret) && !pending.empty()) {
      const ObIvfKnnEntry candidate = pending.at(0);
      if (candidate.distance_ > distance_threshold) {
        break; // Every remaining bound is at least as large.
      }
      std::pop_heap(pending.begin(), pending.end(), greater_bound);
      pending.pop_back();
      bool duplicate = was_raw_vector_refined(candidate.vid_);
      for (int64_t j = 0; !duplicate && j < refine_count; ++j) {
        duplicate = candidate.vid_ == refine_batch[j].vid_;
      }
      if (!duplicate) {
        refine_batch[refine_count++] = candidate;
        if (refine_count == ObDistBBQFastScanBatch::CAPACITY) {
          if (OB_FAIL(refine_vector(query_param, refine_batch,
              refine_count, computer, final_collector))) {
            LOG_WARN("failed to refine ordered DiskBBQ candidates", K(ret));
          } else if (OB_FAIL(final_collector.get_distance_threshold(distance_threshold))) {
            LOG_WARN("failed to refresh exact Top-K threshold", K(ret));
          }
          refine_count = 0;
        }
      }
    }
    if (OB_SUCC(ret) && refine_count > 0 && OB_FAIL(refine_vector(query_param, refine_batch,
        refine_count, computer, final_collector))) {
      LOG_WARN("failed to refine final ordered DiskBBQ candidates", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(final_collector.get_distance_threshold(distance_threshold))) {
    LOG_WARN("failed to refresh threshold after pending candidate refinement", K(ret));
  }
  // Remaining bounds exceed the exact threshold. Keep the allocation for the
  // next chunk, but never carry rejected candidates into it.
  pending.reuse();
  return ret;
}

int ObDiskBBQSearcher::collect_approximate_candidates(
    const ObIvfKnnSearchParam &query_param,
    ObIvfTopKCollector &final_collector,
    const bool quantized_only)
{
  int ret = OB_SUCCESS;
  int prepare_ret = OB_SUCCESS;
  int64_t probe_count = 0;
  bool has_centroid_batch = true;
  int64_t round_nprobe = 0;
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  const auto &refine_computer = query_vector_context().computer();
  if (nullptr == centroid_iterator()
      && OB_FAIL(create_centroid_iterator(query_param, query_vector_context().query(),
          query_param.iterative_centroids_ || is_pre_filter_budget_scan(), MAX(0L, initial_child_budget_)))) {
    LOG_WARN("failed to create centroid iterator", K(ret));
  } else if (OB_ITER_END == (prepare_ret = prepare_centroid_batch(query_param))) {
    has_centroid_batch = false;
    LOG_TRACE("DiskBBQ centroid batches exhausted", K(prepare_ret), K(query_param.nprobe_),
        K(round_nprobe), K_(need_init_auto_nprobe), K_(auto_nprobe), K_(initial_child_budget), K(meta.centroid_count_));
  } else if (OB_SUCCESS != prepare_ret) {
    ret = prepare_ret;
    LOG_WARN("failed to prepare IVF centroid batch", K(ret), K(query_param.nprobe_));
  }
  round_nprobe = is_pre_filter_budget_scan() ? INT64_MAX : get_effective_nprobe(query_param);
  // Refine bounded chunks in lower-bound order. Small searches still use one
  // global heap; larger searches refine early instead of retaining all postings.
  // An entry-sized allocation block keeps doubling capacities at powers of two,
  // so allocator block rounding cannot grow the buffer past the candidate cap.
  ObSEArray<ObIvfKnnEntry, 256> pending(sizeof(ObIvfKnnEntry));
  if (OB_SUCC(ret) && query_param.nprobe_ > 0 && nullptr != query_param.pre_filter_
      && OB_FAIL(fast_scanner_.begin_query(query_vector_context().query()))) {
    LOG_WARN("failed to prepare fixed-probe DiskBBQ PRE query", K(ret));
  }
  pending.set_attr(ObMemAttr(MTL_ID(), "DiskBBQCands"));
  float distance_threshold = 0.0F;
  if (OB_SUCC(ret) && OB_FAIL(final_collector.get_distance_threshold(distance_threshold))) {
    LOG_WARN("failed to get initial exact Top-K threshold", K(ret));
  }
  while (OB_SUCC(ret) && has_centroid_batch
      && probe_count < round_nprobe
      && (nullptr == query_param.scan_budget_ || !query_param.scan_budget_->should_stop())) {
    ObIvfCentroidInfo centroid;
    int next_ret = prepare_centroid_batch(query_param);
    if (OB_SUCCESS == next_ret) { next_ret = centroid_iterator()->get_next(centroid); }
    if (OB_ITER_END == next_ret) {
      break;
    } else if (OB_SUCCESS != next_ret) {
      ret = next_ret;
      LOG_WARN("failed to get next IVF centroid", K(ret), K(probe_count));
    } else {
      ++probe_count;
      bool is_iter_end = false;
      if (OB_FAIL(fast_scanner_.open_scan(centroid.cid_, query_param.read_stats_,
          query_vector_context().query(), centroid.data_, query_param.pre_filter_, !quantized_only))) {
        LOG_WARN("failed to open DiskBBQ posting scan", K(ret), K(centroid.cid_));
      } else if (is_pre_filter_budget_scan()) {
        ++auto_nprobe_;
      }
      while (OB_SUCC(ret) && !is_iter_end
          && (nullptr == query_param.scan_budget_ || !query_param.scan_budget_->should_stop())) {
        ObDistBBQFastScanBatch batch;
        if (OB_FAIL(fast_scanner_.get_next_batch(batch, is_iter_end))) {
          LOG_WARN("failed to read DiskBBQ candidate bounds", K(ret));
        } else {
          consume_pre_filter_budget(batch.scan_count());
          // Charge physical rows in nonempty posting batches; PRE emits only
          // matching candidates after selective decoding. Empty batches are free.
          if (nullptr != query_param.scan_budget_) { query_param.scan_budget_->consume(batch.scan_count()); }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
          const ObIvfKnnEntry &candidate = batch.at(i);
          if (candidate.distance_ <= distance_threshold
              && (nullptr == query_param.deletion_bitmap_handle_
                  || !query_param.deletion_bitmap_handle_->contains(candidate.vid_))
              && (nullptr == query_param.pre_filter_
                  || query_param.pre_filter_->contains(candidate.vid_))
              && (quantized_only || !was_raw_vector_refined(candidate.vid_))) {
            if (quantized_only) {
              if (OB_FAIL(final_collector.push(candidate.cid_, candidate.vid_, candidate.distance_))) {
                LOG_WARN("failed to retain DiskBBQ quantized candidate", K(ret), K(candidate));
              }
            } else if (OB_FAIL(pending.push_back(candidate))) {
              LOG_WARN("failed to buffer DiskBBQ candidate", K(ret), K(candidate));
            } else if (pending.count() >= MAX_PENDING_CANDIDATES
                && OB_FAIL(refine_pending_candidates(query_param,
                    pending, refine_computer, final_collector, distance_threshold))) {
              LOG_WARN("failed to refine full pending candidate buffer", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && quantized_only
            && OB_FAIL(final_collector.get_distance_threshold(distance_threshold))) {
          LOG_WARN("failed to refresh DiskBBQ quantized Top-M threshold", K(ret));
        }
      }
      fast_scanner_.close_scan();
      if (OB_SUCC(ret) && remaining_pre_filter_budget_ == 0) {
        break; // Complete the CID even when the emitted candidates meet B mid-scan.
      }
    }
  }
  if (OB_SUCC(ret) && !quantized_only && OB_FAIL(refine_pending_candidates(query_param,
      pending, refine_computer, final_collector, distance_threshold))) {
    LOG_WARN("failed to refine final pending candidates", K(ret));
  }
  fast_scanner_.end_query();
  if (OB_SUCC(ret)) { finish_pre_filter_budget_scan(); }
  if (nullptr != query_param.read_stats_) {
    query_param.read_stats_->peak_pending_candidate_capacity_ = std::max(
        query_param.read_stats_->peak_pending_candidate_capacity_, pending.get_capacity());
  }
  return ret;
}

int ObDiskBBQSearcher::do_next_candidate_batch(const ObIvfKnnSearchParam &query_param,
    float distance_threshold, int64_t batch_size, ObIvfTopKCollector &collector,
    ObIvfCandidateProgress &progress)
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable()->get_ivf_meta();
  if (!post_started_ && nullptr != centroid_iterator()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot switch IVF KNN cursor to POST search before reuse", K(ret));
  } else if (meta.is_empty() || sstable()->no_data_to_read()
      || (nullptr != query_param.pre_filter_ && query_param.pre_filter_->is_empty())) {
    post_started_ = true;
    progress.state_ = ObIvfCandidateProgress::DOMAIN_END;
  } else if (OB_FAIL(prepare_query(query_param))) {
    LOG_WARN("failed to prepare IVF POST query", K(ret));
  } else if (nullptr == centroid_iterator()
      && OB_FAIL(create_centroid_iterator(query_param, query_vector_context().query(), true,
          MAX(0L, initial_child_budget_)))) {
    LOG_WARN("failed to prepare IVF POST centers", K(ret));
  } else {
    post_started_ = true;
    if (!post_scan_open_ && 0 == post_centroids_remaining_) {
      // Preserve an unfinished POST window across result batches. Fetch new
      // centroids only after all postings in the current window are consumed.
      const int prepare_ret = prepare_centroid_batch(query_param);
      if (OB_ITER_END == prepare_ret) {
        progress.state_ = ObIvfCandidateProgress::DOMAIN_END;
      } else if (OB_SUCCESS != prepare_ret) {
        ret = prepare_ret;
        LOG_WARN("failed to prepare IVF POST centroid batch", K(ret), K(query_param.nprobe_));
      } else {
        post_centroids_remaining_ = is_pre_filter_budget_scan() ? INT64_MAX : get_effective_nprobe(query_param);
      }
    }
    const auto &computer = query_vector_context().computer();
    int64_t selected_count = 0;
    int64_t steps = 0;
    // Leave room for an entire FastScan batch before consuming it. The local
    // collector cannot overflow, so it never discards a not-yet-filtered VID.
    while (OB_SUCC(ret) && selected_count + ObDistBBQFastScanBatch::CAPACITY <= batch_size
        && steps < 128 && (post_scan_open_ || post_centroids_remaining_ > 0)) {
      if (access_ctx()->timeout_ > 0 && ObClockGenerator::getClock() > access_ctx()->timeout_) {
        ret = OB_TIMEOUT;
        LOG_WARN("IVF POST candidate scan timed out", K(ret), K(access_ctx()->timeout_),
                 K(steps), K(selected_count), K_(post_centroids_remaining));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("IVF POST candidate scan interrupted", K(ret));
      } else if (!post_scan_open_) {
        ObIvfCentroidInfo centroid;
        int next_ret = prepare_centroid_batch(query_param);
        if (OB_SUCCESS == next_ret) { next_ret = centroid_iterator()->get_next(centroid); }
        if (OB_ITER_END == next_ret) {
          post_centroids_remaining_ = 0;
          break;
        } else if (OB_SUCCESS != next_ret) {
          ret = next_ret;
          LOG_WARN("failed to advance IVF POST centroid cursor", K(ret), K_(post_centroids_remaining));
        } else if (OB_FAIL(fast_scanner_.open_scan(centroid.cid_, query_param.read_stats_,
            query_vector_context().query(), centroid.data_, query_param.pre_filter_))) {
          LOG_WARN("failed to resume IVF POST center", K(ret));
        } else {
          --post_centroids_remaining_;
          if (is_pre_filter_budget_scan()) { ++auto_nprobe_; }
          ++progress.centroids_started_;
          post_scan_open_ = true;
        }
      }
      if (OB_SUCC(ret) && post_scan_open_) {
        ObDistBBQFastScanBatch approximate;
        bool end = false;
        ++steps;
        if (OB_FAIL(fast_scanner_.get_next_batch(approximate, end))) {
          LOG_WARN("failed to read IVF POST posting batch", K(ret));
        } else {
          progress.postings_scanned_ += approximate.scan_count();
          consume_pre_filter_budget(approximate.scan_count());
          ObIvfKnnEntry selected[ObDistBBQFastScanBatch::CAPACITY];
          int64_t count = 0;
          for (int64_t i = 0; i < approximate.count(); ++i) {
            const ObIvfKnnEntry &candidate = approximate.at(i);
            if ((nullptr == query_param.deletion_bitmap_handle_
                    || !query_param.deletion_bitmap_handle_->contains(candidate.vid_))
                && (nullptr == query_param.pre_filter_ || query_param.pre_filter_->contains(candidate.vid_))
                && candidate.distance_ <= distance_threshold && !collector.contains(candidate.vid_)) {
              selected[count++] = candidate;
            }
          }
          if (count > 0 && OB_FAIL(refine_vector(query_param, selected, count, computer, collector))) {
            LOG_WARN("failed to refine IVF POST batch", K(ret), K(count));
          }
          selected_count += count;
          // The getter borrows batch-local rowkeys; it must not retain them
          // across the call back into SQL. The FastScanner owns separate arenas.
          raw_vector_multi_getter_->reuse();
          if (end) {
            fast_scanner_.close_scan();
            post_scan_open_ = false;
            if (remaining_pre_filter_budget_ == 0) {
              post_centroids_remaining_ = 0;
              finish_pre_filter_budget_scan();
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !post_scan_open_) {
      if (!has_remaining_centroids()) {
        finish_pre_filter_budget_scan();
        post_centroids_remaining_ = 0;
        progress.state_ = ObIvfCandidateProgress::DOMAIN_END;
      } else if (0 == post_centroids_remaining_) {
        progress.state_ = ObIvfCandidateProgress::WINDOW_END;
      }
    }
  }
  return ret;
}

int ObDiskBBQSearcher::do_quantized_search(const ObIvfKnnSearchParam &query_param,
    ObIvfTopKCollector &collector)
{
  int ret = OB_SUCCESS;
  reset_refined_vids();
  if (post_started_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (sstable()->get_ivf_meta().is_empty() || sstable()->no_data_to_read()) {
  } else if (OB_FAIL(prepare_query(query_param))) {
    LOG_WARN("failed to prepare quantized DiskBBQ query", K(ret));
  } else if (OB_FAIL(collect_approximate_candidates(query_param, collector, true))) {
    LOG_WARN("failed to collect quantized DiskBBQ Top-M", K(ret));
  }
  return ret;
}

int ObDiskBBQSearcher::do_refine_candidates(const ObIvfKnnSearchParam &query_param,
    const ObIvfKnnSearchResult &candidates, ObIvfTopKCollector &collector)
{
  int ret = OB_SUCCESS;
  reset_refined_vids();
  if (post_started_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(prepare_query(query_param))) {
    LOG_WARN("failed to prepare filtered DiskBBQ refinement", K(ret));
  }
  const auto &computer = query_vector_context().computer();
  ObIvfKnnEntry batch[ObDistBBQFastScanBatch::CAPACITY];
  for (int64_t start = 0; OB_SUCC(ret) && start < candidates.count();
       start += ObDistBBQFastScanBatch::CAPACITY) {
    const int64_t count = std::min<int64_t>(ObDistBBQFastScanBatch::CAPACITY,
        candidates.count() - start);
    for (int64_t i = 0; i < count; ++i) {
      batch[i] = candidates.at(start + i);
      // Quantized distances are estimates, not lower bounds. Every accepted
      // candidate must receive an exact distance before final Top-K selection.
      batch[i].distance_ = -std::numeric_limits<float>::max();
    }
    if (OB_FAIL(refine_vector(query_param, batch, count, computer, collector))) {
      LOG_WARN("failed to refine accepted DiskBBQ batch", K(ret), K(start), K(count));
    }
  }
  return ret;
}

int ObDiskBBQSearcher::do_knn_search(
    const ObIvfKnnSearchParam &query_param,
    ObIvfTopKCollector &collector)
{
  int ret = OB_SUCCESS;
  reset_refined_vids();
  if (post_started_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot switch IVF POST cursor to KNN search before reuse", K(ret));
  } else if (sstable()->get_ivf_meta().is_empty() || sstable()->no_data_to_read()
      || (nullptr != query_param.pre_filter_ && query_param.pre_filter_->is_empty())) {
  } else if (OB_FAIL(prepare_query(query_param))) {
    LOG_WARN("failed to prepare DiskBBQ query", K(ret), K(query_param));
  } else if (OB_FAIL(collect_approximate_candidates(query_param, collector))) {
    LOG_WARN("failed to collect approximate DiskBBQ candidates", K(ret));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

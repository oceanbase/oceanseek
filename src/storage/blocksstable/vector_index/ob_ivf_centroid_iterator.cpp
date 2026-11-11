/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_heap.h"
#include "lib/utility/ob_sort.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/blocksstable/vector_index/ob_ivf_search_budget.h"
#include <algorithm>

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

////////////////////////////////////////////////////////////////////////////
// Shared centroid selection and cold-vector materialization
////////////////////////////////////////////////////////////////////////////

struct ObIvfCentroidInfoCmp
{
  bool operator()(const ObIvfCentroidInfo &lhs, const ObIvfCentroidInfo &rhs) const
  {
    return lhs.distance_ < rhs.distance_
        || (lhs.distance_ == rhs.distance_ && lhs.cid_ < rhs.cid_);
  }
  int get_error_code() const { return OB_SUCCESS; }
};

typedef ObBinaryHeap<ObIvfCentroidInfo, ObIvfCentroidInfoCmp, 16> ObIvfCentroidHeap;

struct ObIvfCentroidGroupReader::ScanContext
{
  ObStorageDatum datums_[IVF_ROWKEY_COL_COUNT];
  ObStorageDatum end_datums_[IVF_ROWKEY_COL_COUNT];
  ObDatumRowkey key_;
  ObDatumRowkey end_key_;
  ObDatumRange range_;
};

struct ObIvfCentroidSelector
{
  ObIvfCentroidSelector(int64_t dim, int64_t vector_bytes, int64_t centroid_count_limit,
      const quantization::ObQuantizationComputer &distance_computer, ObIvfCentroidHeap &selected_centroid_heap,
      ObIAllocator &vector_buffer_allocator, bool need_copy_centroid_vectors, int64_t child_count_budget = 0)
    : dim_(dim),
      vector_bytes_(vector_bytes),
      centroid_count_limit_(centroid_count_limit),
      distance_computer_(distance_computer),
      selected_centroid_heap_(selected_centroid_heap),
      vector_buffer_allocator_(vector_buffer_allocator),
      need_copy_centroid_vectors_(need_copy_centroid_vectors),
      child_count_budget_(child_count_budget),
      selected_child_count_sum_(0),
      reusable_vector_buffers_()
  {
    reusable_vector_buffers_.set_block_allocator(ObWrapperAllocator(vector_buffer_allocator));
  }
  ~ObIvfCentroidSelector()
  {
    for (int64_t i = 0; i < reusable_vector_buffers_.count(); ++i) {
      vector_buffer_allocator_.free(reusable_vector_buffers_.at(i));
    }
  }
  // Fixed-count and child-budget selection both retain the nearest prefix.
  // The heap root is its farthest center; counts include physical SOAR rows.
  // The caller has computed candidate.distance_; rejection still returns success.
  int try_add_centroid(ObIvfCentroidInfo candidate, const float *centroid_vector)
  {
    int ret = OB_SUCCESS;
    const bool use_child_count_budget = child_count_budget_ > 0;
    const bool has_enough_centroids = use_child_count_budget
        ? selected_child_count_sum_ >= static_cast<uint64_t>(child_count_budget_)
        : selected_centroid_heap_.count() >= centroid_count_limit_;
    float *centroid_vector_buffer = nullptr;
    if ((use_child_count_budget && candidate.child_count_ == 0)
        || (has_enough_centroids && !selected_centroid_heap_.empty()
            && !ObIvfCentroidInfoCmp()(candidate, selected_centroid_heap_.top()))) {
      // A farther center cannot improve an already sufficient prefix.
    } else if (selected_child_count_sum_ > UINT64_MAX - candidate.child_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[DISK_BBQ] centroid selection child count overflow",
          K(ret), K_(selected_child_count_sum), K(candidate));
    } else {
      const bool can_replace_farthest = has_enough_centroids && !selected_centroid_heap_.empty()
          && (!use_child_count_budget
              || selected_child_count_sum_ - selected_centroid_heap_.top().child_count_ + candidate.child_count_
                  >= static_cast<uint64_t>(child_count_budget_));
      const uint32_t replaced_child_count = can_replace_farthest ? selected_centroid_heap_.top().child_count_ : 0;
      if (need_copy_centroid_vectors_) {
        if (can_replace_farthest) {
          centroid_vector_buffer = const_cast<float *>(selected_centroid_heap_.top().data_);
        } else if (!reusable_vector_buffers_.empty()) {
          centroid_vector_buffer = reusable_vector_buffers_.at(reusable_vector_buffers_.count() - 1);
          reusable_vector_buffers_.pop_back();
        } else if ((centroid_vector_buffer = static_cast<float *>(
            vector_buffer_allocator_.alloc(vector_bytes_))) == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[DISK_BBQ] failed to allocate selected centroid vector", K(ret), K_(vector_bytes));
        }
        if (OB_SUCC(ret)) {
          MEMCPY(centroid_vector_buffer, centroid_vector, vector_bytes_);
          candidate.data_ = centroid_vector_buffer;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(can_replace_farthest
          ? selected_centroid_heap_.replace_top(candidate) : selected_centroid_heap_.push(candidate))) {
        LOG_WARN("[DISK_BBQ] failed to retain centroid selection", K(ret), K(candidate), K(can_replace_farthest));
      }
      if (OB_FAIL(ret)) {
        if (centroid_vector_buffer != nullptr && !can_replace_farthest) {
          vector_buffer_allocator_.free(centroid_vector_buffer);
        }
      } else {
        selected_child_count_sum_ = selected_child_count_sum_ - replaced_child_count + candidate.child_count_;
        while (OB_SUCC(ret) && !selected_centroid_heap_.empty()
            && (use_child_count_budget
                ? selected_child_count_sum_ - selected_centroid_heap_.top().child_count_
                    >= static_cast<uint64_t>(child_count_budget_)
                : selected_centroid_heap_.count() > centroid_count_limit_)) {
          const ObIvfCentroidInfo removed_centroid = selected_centroid_heap_.top();
          if (OB_FAIL(selected_centroid_heap_.pop())) {
            LOG_WARN("[DISK_BBQ] failed to trim centroid selection", K(ret));
          } else {
            selected_child_count_sum_ -= removed_centroid.child_count_;
            if (need_copy_centroid_vectors_ && OB_FAIL(reusable_vector_buffers_.push_back(
                const_cast<float *>(removed_centroid.data_)))) {
              vector_buffer_allocator_.free(const_cast<float *>(removed_centroid.data_));
              LOG_WARN("[DISK_BBQ] failed to recycle selected centroid vector", K(ret));
            }
          }
        }
      }
    }
    return ret;
  }
  int64_t dim_;
  int64_t vector_bytes_;
  // Maximum retained centroid count when child_count_budget_ is zero.
  int64_t centroid_count_limit_;
  const quantization::ObQuantizationComputer &distance_computer_;
  // Max heap: the farthest currently selected centroid is at the top.
  ObIvfCentroidHeap &selected_centroid_heap_;
  // Allocates copied centroid vectors and their reuse list, not heap storage.
  ObIAllocator &vector_buffer_allocator_;
  bool need_copy_centroid_vectors_;
  // Required sum of child_count values; automatic nprobe selection counts
  // postings. Parent routing sets this to zero and uses the count limit.
  int64_t child_count_budget_;
  // Sum of child_count for centers currently retained in selected_centroid_heap_.
  uint64_t selected_child_count_sum_;
  // Owns evicted copied buffers until reused or released by this selector.
  ObArray<float *, ObWrapperAllocator> reusable_vector_buffers_;
};

struct ObIvfCentroidVectorReader
{
  ObIvfCentroidVectorReader(ObIAllocator &alloc, ObIvfSSTable &table,
      const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
      ObTableAccessContext &ctx)
    : alloc_(alloc), table_(table), iter_param_(iter_param), row_desc_(row_desc),
      ctx_(ctx), getter_(nullptr), vectors_(nullptr), capacity_(0), slots_()
  {
    slots_.set_block_allocator(ObWrapperAllocator(alloc));
  }
  ~ObIvfCentroidVectorReader()
  {
    if (getter_ != nullptr) {
      getter_->reuse();
      if (ctx_.get_stmt_iter_pool() != nullptr) {
        ctx_.get_stmt_iter_pool()->return_iter(getter_);
      } else {
        getter_->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(ctx_, getter_);
      }
    }
    if (vectors_ != nullptr) { alloc_.free(vectors_); }
  }
  void clear_results(ObIvfCentroidInfo *results, int64_t result_count)
  {
    for (int64_t i = 0; results != nullptr && i < slots_.count(); ++i) {
      if (slots_.at(i) < result_count) { results[slots_.at(i)].data_ = nullptr; }
    }
    slots_.reuse();
  }
  int read(ObIvfCentroidInfo *results, int64_t result_count, int64_t begin, int64_t count)
  {
    int ret = OB_SUCCESS;
    const int64_t dim = table_.get_ivf_meta().dim_;
    const int64_t bytes = dim * static_cast<int64_t>(sizeof(float));
    ObArenaAllocator scratch(ObMemAttr(MTL_ID(), "IVFCentRead"));
    ObArray<ObStorageDatum, ObWrapperAllocator> datums;
    ObArray<ObDatumRowkey, ObWrapperAllocator> keys;
    datums.set_block_allocator(ObWrapperAllocator(scratch));
    keys.set_block_allocator(ObWrapperAllocator(scratch));
    const auto scan_order = ctx_.query_flag_.scan_order_;
    if (results == nullptr || begin < 0 || count <= 0 || begin > result_count
        || count > result_count - begin || bytes <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] invalid centroid vector batch", K(ret), K(begin), K(count), K(result_count), K(dim));
    } else {
      clear_results(results, result_count);
      for (int64_t i = begin; OB_SUCC(ret) && i < begin + count; ++i) {
        if (!results[i].has_vector() && OB_FAIL(slots_.push_back(i))) {
          LOG_WARN("[DISK_BBQ] failed to collect cold centroid key", K(ret), K(i));
        }
      }
    }
    const int64_t missing = slots_.count();
    if (OB_SUCC(ret) && missing > 0) {
      if (missing > INT64_MAX / bytes || missing > INT64_MAX / IVF_ROWKEY_COL_COUNT) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[DISK_BBQ] centroid batch size overflow", K(ret), K(missing), K(dim));
      } else if (missing > capacity_) {
        float *buffer = static_cast<float *>(alloc_.alloc(missing * bytes));
        if (buffer == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[DISK_BBQ] failed to allocate centroid batch vectors", K(ret), K(missing), K(bytes));
        } else {
          if (vectors_ != nullptr) { alloc_.free(vectors_); }
          vectors_ = buffer;
          capacity_ = missing;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(datums.prepare_allocate(missing * IVF_ROWKEY_COL_COUNT))) {
        LOG_WARN("[DISK_BBQ] failed to allocate centroid key datums", K(ret), K(missing));
      } else if (OB_SUCC(ret) && OB_FAIL(keys.prepare_allocate(missing))) {
        LOG_WARN("[DISK_BBQ] failed to allocate centroid rowkeys", K(ret), K(missing));
      }
      if (OB_SUCC(ret)) {
        lib::ob_sort(slots_.get_data(), slots_.get_data() + missing,
            [results](int64_t a, int64_t b) {
              return results[a].parent_id_ < results[b].parent_id_
                  || (results[a].parent_id_ == results[b].parent_id_ && results[a].cid_ < results[b].cid_);
            });
        for (int64_t i = 0; OB_SUCC(ret) && i < missing; ++i) {
          const ObIvfCentroidInfo &entry = results[slots_.at(i)];
          ObStorageDatum *key = datums.get_data() + i * IVF_ROWKEY_COL_COUNT;
          key[IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_CENTROID);
          key[IVF_ROWKEY_COL_CID].set_int(entry.parent_id_);
          key[IVF_ROWKEY_COL_VID].set_int(entry.cid_);
          if (OB_FAIL(keys.at(i).assign(key, IVF_ROWKEY_COL_COUNT))) {
            LOG_WARN("[DISK_BBQ] failed to assign centroid rowkey", K(ret), K(entry));
          }
        }
      }
      if (OB_SUCC(ret) && getter_ == nullptr) {
        ALLOCATE_TABLE_STORE_ROW_IETRATOR(ctx_, ObSSTableRowMultiGetter, getter_);
        if (OB_ISNULL(getter_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[DISK_BBQ] failed to allocate centroid multi-getter", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // Keys are sorted above. Keep input order while using the general
        // multi-get prefetch path, which also supports untransformed index
        // blocks when the storage block cache is disabled.
        ctx_.query_flag_.scan_order_ = ObQueryFlag::KeepOrder;
        if (OB_FAIL(getter_->init(iter_param_, ctx_, &table_, &keys))) {
          LOG_WARN("[DISK_BBQ] failed to open centroid batch read", K(ret), K(missing));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < missing; ++i) {
        const ObDatumRow *row = nullptr;
        ObIvfCentroidInfo &entry = results[slots_.at(i)];
        const float *vector = nullptr;
        uint32_t children = 0;
        if (OB_FAIL(getter_->get_next_row(row))) {
          if (ret == OB_ITER_END) {
            ret = OB_INVALID_DATA;
            LOG_WARN("[DISK_BBQ] selected centroid batch ended before the requested row", K(ret), K(entry));
          } else {
            LOG_WARN("[DISK_BBQ] failed to read selected centroid", K(ret), K(entry));
          }
        } else if (row == nullptr || row->row_flag_.is_not_exist()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] selected centroid row is missing", K(ret), K(entry));
        } else if (OB_FAIL(ObIvfCentroidPayload::decode(
            row->storage_datums_[row_desc_.payload_col_idx_].get_string(), dim, vector, children))) {
          LOG_WARN("[DISK_BBQ] failed to decode selected centroid", K(ret), K(entry));
        } else {
          float *destination = vectors_ + i * dim;
          MEMCPY(destination, vector, bytes);
          entry.data_ = destination;
        }
      }
    }
    // Release handles before the rowkeys' scratch memory goes out of scope.
    if (getter_ != nullptr) { getter_->reuse(); }
    ctx_.query_flag_.scan_order_ = scan_order;
    return ret;
  }
  ObIAllocator &alloc_;
  ObIvfSSTable &table_;
  const ObTableIterParam &iter_param_;
  ObIvfSSTableRowDesc row_desc_;
  ObTableAccessContext &ctx_;
  ObStoreRowIterator *getter_;
  float *vectors_;
  int64_t capacity_;
  ObArray<int64_t, ObWrapperAllocator> slots_;
};

////////////////////////////////////////////////////////////////////////////
// ObIvfCentroidIterator - common lifetime and result access
////////////////////////////////////////////////////////////////////////////

ObIvfCentroidIterator::ObIvfCentroidIterator(ObIAllocator &allocator)
  : query_allocator_(allocator), query_vector_ctx_(nullptr), owns_query_vector_ctx_(false),
    results_(nullptr), result_count_(0), cursor_(0), initial_child_budget_(0),
    owns_result_vectors_(true), skip_empty_centroids_(false), is_inited_(false), meta_(), vector_reader_(nullptr)
{}
ObIvfCentroidIterator::~ObIvfCentroidIterator() { reset_common(); }

int ObIvfCentroidIterator::prepare_query_context(const float *query, int64_t dim,
    share::ObVectorIndexDistAlgorithm metric, const ObIvfQueryVectorContext *shared_context)
{
  int ret = OB_SUCCESS;
  if (shared_context != nullptr) {
    if (!shared_context->matches_prepared_query(query, dim, metric)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] centroid query does not match shared context", K(ret),
          KP(query), K(dim), K(metric), KPC(shared_context));
    } else {
      query_vector_ctx_ = shared_context;
    }
  } else {
    void *mem = query_allocator_.alloc(sizeof(ObIvfQueryVectorContext));
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate centroid query context", K(ret),
          K(dim), K(metric), "alloc_size", sizeof(ObIvfQueryVectorContext));
    } else {
      auto *context = new (mem) ObIvfQueryVectorContext();
      if (OB_FAIL(context->init(query_allocator_, query, dim, metric))) {
        LOG_WARN("[DISK_BBQ] failed to initialize centroid query context", K(ret), K(dim), K(metric));
        context->~ObIvfQueryVectorContext();
        query_allocator_.free(context);
      } else {
        query_vector_ctx_ = context;
        owns_query_vector_ctx_ = true;
      }
    }
  }
  return ret;
}

int ObIvfCentroidIterator::init_vector_reader(ObIvfSSTable &table,
    const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc, ObTableAccessContext &ctx)
{
  int ret = OB_SUCCESS;
  if (vector_reader_ == nullptr) {
    void *mem = query_allocator_.alloc(sizeof(ObIvfCentroidVectorReader));
    if (mem == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate centroid vector reader", K(ret));
    } else {
      vector_reader_ = new (mem) ObIvfCentroidVectorReader(query_allocator_, table, iter_param, row_desc, ctx);
    }
  }
  return ret;
}

int ObIvfCentroidIterator::prepare_result_vectors(int64_t begin, int64_t count)
{
  int ret = OB_SUCCESS;
  if (vector_reader_ != nullptr && count > 0) {
    ret = vector_reader_->read(results_, result_count_, begin, count);
  }
  return ret;
}

void ObIvfCentroidIterator::release_results()
{
  if (vector_reader_ != nullptr) { vector_reader_->clear_results(results_, result_count_); }
  if (nullptr != results_) {
    for (int64_t i = 0; i < result_count_; ++i) {
      if (owns_result_vectors_ && nullptr != results_[i].data_) {
        query_allocator_.free(const_cast<float *>(results_[i].data_));
      }
      results_[i].~ObIvfCentroidInfo();
    }
    query_allocator_.free(results_);
    results_ = nullptr;
  }
  result_count_ = 0;
  cursor_ = 0;
}

void ObIvfCentroidIterator::reset_common()
{
  release_results();
  if (vector_reader_ != nullptr) {
    vector_reader_->~ObIvfCentroidVectorReader();
    query_allocator_.free(vector_reader_);
    vector_reader_ = nullptr;
  }
  if (owns_query_vector_ctx_ && query_vector_ctx_ != nullptr) {
    query_vector_ctx_->~ObIvfQueryVectorContext();
    query_allocator_.free(const_cast<ObIvfQueryVectorContext *>(query_vector_ctx_));
  }
  query_vector_ctx_ = nullptr;
  owns_query_vector_ctx_ = false;
  initial_child_budget_ = 0;
  owns_result_vectors_ = true;
  skip_empty_centroids_ = false;
  is_inited_ = false;
  meta_.reset();
}

int ObIvfCentroidIterator::get_next_result(ObIvfCentroidInfo &result, const bool require_vector)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf centroid iterator is not initialized", K(ret));
  } else if (cursor_ >= result_count_) {
    ret = OB_ITER_END;
  } else {
    if (require_vector && !results_[cursor_].has_vector()) {
      // Standalone consumers may call get_next() without prepare_next_batch().
      // Keep their cold reads bounded as well, instead of loading the full list.
      if (vector_reader_ == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing IVF centroid vector owner", K(ret), K_(cursor));
      } else if (OB_FAIL(prepare_result_vectors(cursor_, MIN(64L, result_count_ - cursor_)))) {
        LOG_WARN("failed to read cold centroid vectors", K(ret), K_(cursor));
      }
    }
    if (OB_SUCC(ret)) { result = results_[cursor_++]; }
    else { reset(); }
  }
  return ret;
}

int ObIvfCentroidIterator::init_query(
    const ObIvfKnnSearchParam &query_param, const float *rotated_query,
    ObIvfSSTable &sstable, const ObTableIterParam &iter_param,
    const ObIvfSSTableRowDesc &row_desc, ObTableAccessContext &access_ctx,
    const ObIvfQueryVectorContext *query_vector_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    initial_child_budget_ = query_param.centroid_child_budget_;
    skip_empty_centroids_ = query_param.skip_empty_centroids_;
  }
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ivf centroid iterator is already initialized", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid()
      || !row_desc.is_valid()
      || query_param.dim_ <= 0
      || query_param.nprobe_ <= 0
      || !std::isfinite(query_param.refine_k_) || query_param.refine_k_ < 1.0F
      || OB_ISNULL(rotated_query))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for ivf centroid iterator", K(ret), K(iter_param),
        K(row_desc), K(query_param), KP(rotated_query));
  } else if (OB_UNLIKELY(!access_ctx.is_valid()
      || access_ctx.tablet_id_ != sstable.get_key().get_tablet_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid access context for ivf centroid iterator", K(ret),
        K(iter_param), K(access_ctx), K(sstable.get_key()));
  } else if (OB_UNLIKELY(!sstable.is_valid() || sstable.no_data_to_read()
      || !sstable.get_ivf_meta().is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable is not ready for centroid iteration", K(ret), K(sstable));
  } else if (OB_UNLIKELY(query_param.dim_ != sstable.get_ivf_meta().dim_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf centroid query or read info", K(ret), K(query_param),
        KPC(iter_param.read_info_), K(row_desc), K(sstable.get_ivf_meta()));
  } else if (OB_FAIL(prepare_query_context(rotated_query, query_param.dim_,
      static_cast<share::ObVectorIndexDistAlgorithm>(sstable.get_ivf_meta().dist_type_),
      query_vector_ctx))) {
    LOG_WARN("failed to bind IVF query vector context", K(ret));
  } else {
    meta_ = sstable.get_ivf_meta();
  }
  return ret;
}

int ObIvfCentroidIterator::validate_initial_batch(const int64_t child_budget) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] initial centroid selector is not initialized", K(ret));
  } else if (child_budget <= 0 || child_budget != initial_child_budget_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid initial centroid child budget", K(ret), K(child_budget), K_(initial_child_budget));
  } else if (cursor_ != 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] initial centroid selection already consumed", K(ret), K_(cursor));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
// ObIvfCentroidGroupReader - resource ownership and cache binding
////////////////////////////////////////////////////////////////////////////

ObIvfCentroidGroupReader::ObIvfCentroidGroupReader(ObIAllocator &allocator)
  : query_allocator_(allocator), query_vector_ctx_(nullptr), meta_(), centroid_group_id_(0),
    expected_center_count_(0), is_inited_(false), cache_mgr_guard_(), array_cache_guard_(),
    hgraph_ctx_(), active_cache_(nullptr), array_centroids_(nullptr), shared_cache_(false), cache_hit_(false)
{}
ObIvfCentroidGroupReader::~ObIvfCentroidGroupReader() { reset(); }

void ObIvfCentroidGroupReader::release_hgraph_vectors()
{
  if (hgraph_ctx_.batch_centroids_ != nullptr) {
    hgraph_ctx_.allocator_->Deallocate(hgraph_ctx_.batch_centroids_);
    hgraph_ctx_.batch_centroids_ = nullptr;
  }
}
void ObIvfCentroidGroupReader::release_array_guard()
{
  array_cache_guard_.reset();
  array_centroids_ = nullptr;
}
void ObIvfCentroidGroupReader::reset_cache_state()
{
  release_hgraph_vectors();
  release_array_guard();
  if (hgraph_ctx_.iter_ctx_ != nullptr) { obvectorutil::delete_iter_ctx(hgraph_ctx_.iter_ctx_); }
  if (hgraph_ctx_.allocator_ != nullptr) {
    hgraph_ctx_.allocator_->~ObVsagSearchAlloc();
    query_allocator_.free(hgraph_ctx_.allocator_);
  }
  hgraph_ctx_ = HGraphSearchContext();
  active_cache_ = nullptr;
  shared_cache_ = false;
  cache_hit_ = false;
}
void ObIvfCentroidGroupReader::reset()
{
  // The cache pointer is protected until cache_mgr_guard_ is released below.
  // Clearing the binding makes repeated reset/destruction idempotent.
  if (active_cache_ != nullptr && shared_cache_) {
    active_cache_->record_iterator_access(cache_hit_);
  }
  reset_cache_state();
  cache_mgr_guard_.reset();
  query_vector_ctx_ = nullptr;
  centroid_group_id_ = expected_center_count_ = 0;
  is_inited_ = false;
  meta_.reset();
}

ObIvfCentroidGroupReader::ArrayCacheReadGuard::~ArrayCacheReadGuard() { reset(); }

int ObIvfCentroidGroupReader::ArrayCacheReadGuard::lock(
    share::ObIvfSSTableCentCache &cache)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cache_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF SSTable ARRAY cache guard is already locked",
             KR(ret), KP_(cache), KP(&cache));
  } else if (OB_FAIL(cache.get_lock().rdlock())) {
    LOG_WARN("failed to lock IVF SSTable ARRAY cache", KR(ret));
  } else {
    cache_ = &cache;
  }
  return ret;
}

void ObIvfCentroidGroupReader::ArrayCacheReadGuard::reset()
{
  if (OB_NOT_NULL(cache_)) {
    const int tmp_ret = cache_->get_lock().rdunlock();
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR_RET(tmp_ret, "failed to unlock IVF SSTable ARRAY cache", K(tmp_ret));
    }
    cache_ = nullptr;
  }
}

int ObIvfCentroidGroupReader::open_centroid_scanner(
    ObIvfSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ScanContext &scan_ctx,
    ObStoreRowIterator *&row_scanner)
{
  int ret = OB_SUCCESS;
  scan_ctx.datums_[0].set_int(IVF_ROW_TYPE_CENTROID);
  scan_ctx.datums_[1].set_int(centroid_group_id_);
  scan_ctx.datums_[2].set_min();
  scan_ctx.end_datums_[0].set_int(IVF_ROW_TYPE_CENTROID);
  scan_ctx.end_datums_[1].set_int(centroid_group_id_);
  scan_ctx.end_datums_[2].set_max();
  if (OB_FAIL(scan_ctx.key_.assign(scan_ctx.datums_, ARRAYSIZEOF(scan_ctx.datums_)))) {
    LOG_WARN("[DISK_BBQ] failed to assign centroid group start key", K(ret),
        "sstable_key", sstable.get_key(), K_(centroid_group_id));
  } else if (OB_FAIL(scan_ctx.end_key_.assign(
      scan_ctx.end_datums_, ARRAYSIZEOF(scan_ctx.end_datums_)))) {
    LOG_WARN("[DISK_BBQ] failed to assign centroid group end key", K(ret),
        "sstable_key", sstable.get_key(), K_(centroid_group_id));
  } else {
    scan_ctx.range_.set_start_key(scan_ctx.key_);
    scan_ctx.range_.set_end_key(scan_ctx.end_key_);
    scan_ctx.range_.set_left_closed();
    scan_ctx.range_.set_right_closed();
    access_ctx.query_flag_.scan_order_ = ObQueryFlag::Forward;
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(
        access_ctx, ObSSTableRowScanner<>, row_scanner);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to allocate ivf centroid row scanner", K(ret));
    } else if (OB_FAIL(row_scanner->init(
        iter_param, access_ctx, &sstable, &scan_ctx.range_))) {
      LOG_WARN("failed to open ivf centroid row scanner", K(ret),
          K(iter_param), K(scan_ctx.range_));
    }
  }
  return ret;
}

int ObIvfCentroidGroupReader::prepare_array_cache_build(
    ObIvfSSTable &sstable,
    const ObTableIterParam &iter_param,
    const int64_t cluster_size,
    const share::ObLSID &ls_id,
    const share::ObIvfCacheMgrKey &cache_key,
    uint64_t &cache_reservation,
    share::ObIvfSSTableCentCache *&cent_cache)
{
  int ret = OB_SUCCESS;
  cent_cache = nullptr;
  share::ObPluginVectorIndexService *service = MTL(share::ObPluginVectorIndexService *);
  const ObIvfSSTableMeta &meta = sstable.get_ivf_meta();
  share::ObIvfCacheMgrGuard parent_guard;
  if (centroid_group_id_ > 0 && service != nullptr) {
    const int parent_ret = service->acquire_ivf_cache_mgr_guard(
        ls_id, share::ObIvfCacheMgrKey(sstable.get_key(), 0), parent_guard);
    if (parent_ret != OB_SUCCESS && parent_ret != OB_HASH_NOT_EXIST && parent_ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("[DISK_BBQ] failed to acquire child cache owner", K(parent_ret), K(cache_key));
    }
  }
  if (OB_ISNULL(service) || expected_center_count_ <= 0
      || expected_center_count_ > INT32_MAX || meta.dim_ <= 0
      || (centroid_group_id_ > 0 && !parent_guard.is_valid())
      || !ls_id.is_valid()
      || !service->get_ivf_sstable_cache_controller().is_cache_enabled()) {
  } else if (expected_center_count_ > INT64_MAX / meta.dim_
             || expected_center_count_ * meta.dim_ > INT64_MAX / static_cast<int64_t>(sizeof(float))) {
    LOG_WARN_RET(OB_SIZE_OVERFLOW,
                 "skip IVF SSTable cache for overflowing centroid metadata",
                 K(expected_center_count_), K(meta.dim_));
  } else {
    // One reservation per group, conservatively assuming no shared leaf hits.
    const uint64_t estimate = centroid_group_id_ > 0
        ? share::ObIvfSSTableCentCache::estimate_shared_group_memory(expected_center_count_, meta.dim_)
        : static_cast<uint64_t>(expected_center_count_) * (meta.dim_ * sizeof(float) + sizeof(uint32_t))
            + (meta.is_hierarchical() ? meta.centroid_count_
                * (sizeof(share::ObIvfSSTableCentCache::SharedLeaf *) + sizeof(common::ObByteLock)) : 0);
    share::ObIvfSSTableCacheController &controller =
        service->get_ivf_sstable_cache_controller();
    share::ObVectorIndexParam vec_param;
    vec_param.type_ = static_cast<share::ObVectorIndexAlgorithmType>(meta.index_type_);
    vec_param.lib_ = share::VIAL_OB;
    vec_param.dist_algorithm_ = static_cast<share::ObVectorIndexDistAlgorithm>(meta.dist_type_);
    vec_param.dim_ = meta.dim_;
    vec_param.cluster_size_ = cluster_size;
    if (OB_FAIL(service->acquire_ivf_cache_mgr_guard(
            ls_id, cache_key, vec_param, meta.dim_,
            iter_param.table_id_, cache_mgr_guard_))) {
      LOG_WARN("failed to create IVF SSTable cache manager", KR(ret), K(cache_key));
    } else {
      share::ObIvfCacheMgr *cache_mgr = cache_mgr_guard_.get_ivf_cache_mgr();
      if (OB_ISNULL(cache_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF SSTable cache manager guard returned null manager",
                 KR(ret), K(ls_id), K(cache_key));
      } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<share::ObIvfSSTableCentCache>(
                     share::IvfCacheType::IVF_SSTABLE_CENTROID_CACHE, cent_cache))) {
        const int cache_ret = ret;
        LOG_WARN("failed to create IVF SSTable cache placeholder", KR(ret), K(cache_key));
        bool erased = false;
        uint64_t registered_hold = 0;
        const int erase_ret = service->try_erase_ivf_sstable_cache(
            ls_id, cache_key, cache_mgr, nullptr,
            0 /* expected_last_access_ts */, true /* failed_build */,
            erased, registered_hold);
        if (OB_SUCCESS != erase_ret) {
          LOG_WARN("failed to detach empty IVF SSTable cache manager",
                   K(erase_ret), K(cache_key));
        } else if (!erased) {
          LOG_WARN_RET(OB_EAGAIN,
                       "empty IVF SSTable cache manager is still referenced",
                       K(cache_key));
        }
        ret = cache_ret;
      } else if (OB_ISNULL(cent_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF SSTable cache manager returned null centroid cache",
                 KR(ret), K(ls_id), K(cache_key));
      } else if (!cent_cache->set_writing_if_idle()) {
        // Another query owns this TableKey build. Do not reserve memory and do
        // not wait for it; the current query keeps the SSTable fallback.
        cent_cache = nullptr;
      } else if ((centroid_group_id_ > 0 && OB_FAIL(cent_cache->attach_parent_cache(parent_guard)))
                 || !controller.try_reserve(estimate)) {
        bool erased = false;
        uint64_t registered_hold = 0;
        const int erase_ret = service->try_erase_ivf_sstable_cache(
            ls_id, cache_key, cache_mgr, cent_cache,
            cent_cache->get_last_access_ts(), true /* failed_build */,
            erased, registered_hold);
        if (OB_SUCCESS != erase_ret) {
          LOG_WARN("failed to detach IVF SSTable cache after reservation rejection",
                   K(erase_ret), K(cache_key));
        } else if (!erased) {
          LOG_WARN_RET(OB_EAGAIN,
                       "rejected IVF SSTable cache placeholder is still mapped",
                       K(cache_key));
        }
        cent_cache = nullptr;
      } else {
        cache_reservation = estimate;
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(cent_cache)) {
      const int cache_ret = ret;
      cache_mgr_guard_.reset();
      // Cache construction is optional; preserve the SSTable query path.
      if (OB_SUCCESS != cache_ret) {
        LOG_INFO("fall back to SSTable after cache preparation failure",
                 K(cache_ret), K(cache_key));
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObIvfCentroidGroupReader::build_array_cache(
    ObIvfSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const ObIvfSSTableRowDesc &row_desc,
    share::ObIvfSSTableCentCache &cent_cache, uint64_t &cache_reservation)
{
  int ret = OB_SUCCESS;
  const int64_t dim = sstable.get_ivf_meta().dim_;
  int64_t row_count = 0;
  ScanContext scan_ctx;
  ObStoreRowIterator *row_scanner = nullptr;
  const ObDatumRow *row = nullptr;
  const auto original_scan_order = access_ctx.query_flag_.scan_order_;
  {
    common::RWLock::WLockGuard cache_guard(cent_cache.get_lock());
    if (!cent_cache.is_writing()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF SSTable ARRAY cache is not in WRITING state",
               KR(ret), K(row_count));
    } else if (OB_FAIL(cent_cache.allocate_array(
                   expected_center_count_, centroid_group_id_))) {
      LOG_WARN("[DISK_BBQ] failed to allocate SSTable centroid ARRAY cache", KR(ret),
          "sstable_key", sstable.get_key(), K_(centroid_group_id), K_(expected_center_count), K(dim));
    } else if (centroid_group_id_ == 0 && meta_.is_hierarchical()
        && OB_FAIL(cent_cache.init_shared_leaves(meta_.centroid_count_, meta_.leaf_cid_base()))) {
      LOG_WARN("[DISK_BBQ] failed to initialize shared leaf directory", K(ret), K(meta_));
    } else if (OB_FAIL(open_centroid_scanner(
                   sstable, iter_param, access_ctx, scan_ctx, row_scanner))) {
      LOG_WARN("failed to open centroid scanner for ARRAY cache", KR(ret));
    }
    // Cache construction must stop at the same boundary as a storage query.
    while (OB_SUCC(ret) && row_count < expected_center_count_) {
      row = nullptr;
      const int next_ret = row_scanner->get_next_row(row);
      if (OB_ITER_END == next_ret) {
        break;
      } else if (OB_SUCCESS != next_ret) {
        ret = next_ret;
        LOG_WARN("failed to scan centroid for ARRAY cache", KR(ret), K(row_count));
      } else if (OB_ISNULL(row)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF SSTable centroid scanner returned null row",
                 KR(ret), K(row_count));
      } else {
        const ObStorageDatum &cid_datum =
            row->storage_datums_[row_desc.centroid_id_col_idx_];
        const ObStorageDatum &vid_datum =
            row->storage_datums_[row_desc.vector_id_col_idx_];
        const ObStorageDatum &payload_datum =
            row->storage_datums_[row_desc.payload_col_idx_];
        if (cid_datum.is_null() || OB_ISNULL(cid_datum.ptr_)
            || cid_datum.len_ != sizeof(int64_t)
            || vid_datum.is_null() || OB_ISNULL(vid_datum.ptr_)
            || vid_datum.len_ != sizeof(int64_t)
            || payload_datum.is_null() || OB_ISNULL(payload_datum.ptr_)
            || payload_datum.len_ < ObIvfCentroidPayload::get_size(dim)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid centroid row for ARRAY cache", KR(ret), K(row_count), KPC(row));
        } else {
          int64_t cid = 0, group = 0;
          uint32_t children = 0;
          const float *vector = nullptr;
          if (OB_FAIL(meta_.decode_centroid_key(cid_datum.get_int(), vid_datum.get_int(), cid, group))) {
            LOG_WARN("[DISK_BBQ] failed to decode centroid key for cache", K(ret), K(row_count), K_(centroid_group_id),
                "sstable_key", sstable.get_key());
          } else if (group != centroid_group_id_ || (group == 0 && cid != row_count + 1)) {
            ret = OB_INVALID_DATA;
            LOG_WARN("[DISK_BBQ] centroid key does not match cache group", KR(ret),
                "sstable_key", sstable.get_key(), K_(centroid_group_id), K(group), K(cid), K(row_count));
          } else if (OB_FAIL(ObIvfCentroidPayload::decode(payload_datum.get_string(), dim, vector, children))) {
            LOG_WARN("[DISK_BBQ] failed to decode centroid payload for cache", K(ret), K(cid), K(group), K(row_count));
          } else if (OB_FAIL(cent_cache.write_entry(row_count, cid, vector, children, &cache_reservation))) {
            LOG_WARN("[DISK_BBQ] failed to load centroid cache entry", KR(ret),
                "sstable_key", sstable.get_key(), K(cid), K(group), "slot", row_count);
          } else {
            ++row_count;
          }
        }
      }
    }
    if (OB_SUCC(ret)
        && (row_count != cent_cache.get_capacity()
            || cent_cache.get_count() != cent_cache.get_capacity())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] centroid count does not match ARRAY cache capacity",
               KR(ret), K(row_count), "cache_count", cent_cache.get_count(),
               "capacity", cent_cache.get_capacity(), "sstable_key", sstable.get_key(), K_(centroid_group_id));
    }
  }

  access_ctx.query_flag_.scan_order_ = original_scan_order;
  if (OB_NOT_NULL(row_scanner)) {
    row_scanner->reuse();
    if (OB_NOT_NULL(access_ctx.get_stmt_iter_pool())) {
      access_ctx.get_stmt_iter_pool()->return_iter(row_scanner);
    } else {
      row_scanner->~ObStoreRowIterator();
      FREE_TABLE_STORE_ROW_IETRATOR(access_ctx, row_scanner);
    }
  }
  return ret;
}

bool ObIvfCentroidGroupReader::finish_array_cache_build(
    const int build_ret,
    const share::ObLSID &ls_id,
    const share::ObIvfCacheMgrKey &cache_key,
    uint64_t &cache_reservation,
    share::ObIvfSSTableCentCache *cent_cache)
{
  bool published = false;
  bool private_ready = false;
  share::ObPluginVectorIndexService *service = MTL(share::ObPluginVectorIndexService *);
  share::ObIvfCacheMgr *cache_mgr = cache_mgr_guard_.get_ivf_cache_mgr();
  if (OB_NOT_NULL(service) && cache_reservation > 0) {
    if (OB_NOT_NULL(cent_cache)) {
      // Transfer the remaining reservation to actual ownership in one admission
      // decision, including failed builds and private query fallbacks.
      cent_cache->register_memory_once(cent_cache->get_memory_hold(),
          &service->get_ivf_sstable_cache_controller(), &cache_reservation);
    }
    if (OB_SUCCESS == build_ret && OB_NOT_NULL(cent_cache)
        && OB_NOT_NULL(cache_mgr)
        && cent_cache->get_count() == cent_cache->get_capacity()) {
      const uint64_t hold = cent_cache->get_memory_hold();
      const int publish_ret = service->try_publish_ivf_sstable_cache(
          ls_id, cache_key, cache_mgr, cent_cache, hold, published);
      if (OB_SUCCESS != publish_ret) {
        LOG_WARN_RET(publish_ret, "failed to publish IVF SSTable ARRAY cache",
                     K(publish_ret), K(ls_id), K(cache_key));
      }
    }
    if (published && cent_cache->can_build_hgraph()) {
      const int enqueue_ret = service->enqueue_ivf_sstable_hgraph_build(
          ls_id, cache_key);
      if (OB_SUCCESS != enqueue_ret) {
        // ARRAY is already published and remains fully queryable. A pending
        // enqueue failure only disables the optional graph acceleration.
        LOG_WARN_RET(enqueue_ret, "failed to enqueue IVF SSTable HGraph build",
                     K(enqueue_ret), K(ls_id), K(cache_key));
      }
    }
    if (!published) {
      bool erased = false;
      uint64_t registered_hold = 0;
      if (OB_NOT_NULL(cache_mgr) && OB_NOT_NULL(cent_cache)
          && ls_id.is_valid()) {
        const int tmp_ret = service->try_erase_ivf_sstable_cache(
            ls_id, cache_key, cache_mgr, cent_cache,
            cent_cache->get_last_access_ts(), true /* failed_build */,
            erased, registered_hold);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN_RET(tmp_ret, "failed to erase incomplete IVF SSTable cache",
                       K(tmp_ret), K(cache_key));
        } else if (!erased) {
          LOG_WARN_RET(OB_EAGAIN, "failed IVF SSTable cache is still mapped",
                       K(cache_key));
        }
      }
      if (erased && build_ret == OB_SUCCESS && cent_cache != nullptr
          && cent_cache->get_count() == cent_cache->get_capacity()) {
        // The complete buffer is detached from the shared map. Keep its owner
        // for this query instead of rereading the same group after a lost publish.
        cent_cache->set_completed();
        private_ready = true;
      } else {
        cache_mgr_guard_.reset();
      }
    }
    if (cache_reservation > 0) {
      service->get_ivf_sstable_cache_controller().release_reservation(cache_reservation);
    }
  }
  cache_reservation = 0;
  shared_cache_ = published;
  return published || private_ready;
}

int ObIvfCentroidGroupReader::open_hgraph_cache(
    common::obvsag::VectorIndexPtr hgraph_handle,
    const int64_t count,
    const int64_t dim)
{
  int ret = OB_SUCCESS;
  const float *rotated_query = query_vector_ctx_->query();
  void *allocator_buf = nullptr;
  if (OB_ISNULL(rotated_query) || OB_ISNULL(hgraph_handle)
      || count <= 0 || dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable HGraph cache search argument",
             KR(ret), KP(rotated_query), KP(hgraph_handle), K(count), K(dim));
  } else if (OB_ISNULL(allocator_buf = query_allocator_.alloc(sizeof(share::ObVsagSearchAlloc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF SSTable HGraph search allocator",
             KR(ret), "alloc_size", sizeof(share::ObVsagSearchAlloc));
  } else {
    hgraph_ctx_.allocator_ = new (allocator_buf) share::ObVsagSearchAlloc(MTL_ID());
    hgraph_ctx_.handle_ = hgraph_handle;
    hgraph_ctx_.visited_count_ = 0;
    hgraph_ctx_.centroid_count_ = count;
    hgraph_ctx_.centroid_dim_ = dim;
    hgraph_ctx_.has_next_ = true;
  }
  return ret;
}

int ObIvfCentroidGroupReader::bind_cache(
    share::ObIvfSSTableCentCache &cache, share::ObIvfCacheMgr &cache_mgr)
{
  int ret = OB_SUCCESS;
  const float *centroids = nullptr;
  common::obvsag::VectorIndexPtr hgraph_handle = nullptr;
  int64_t count = 0;
  int64_t dim = 0;
  if (OB_FAIL(array_cache_guard_.lock(cache))) {
    LOG_WARN("failed to guard IVF SSTable centroid cache", KR(ret));
  } else {
    active_cache_ = &cache;
    hgraph_handle = cache.get_hgraph_index();
    centroids = cache.get_centroids();
    count = cache.get_count();
    dim = cache.get_cent_vec_dim();
    if (!cache.is_completed() || cache.get_group_id() != centroid_group_id_
        || (expected_center_count_ > 0 && count != expected_center_count_)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[DISK_BBQ] SSTable centroid cache is not ready or does not match the requested group",
          KR(ret), "cache_key", cache_mgr.get_cache_mgr_key(), "cache_completed", cache.is_completed(),
          "cache_group_id", cache.get_group_id(), "expected_group_id", centroid_group_id_,
          "cache_centroid_count", count, K_(expected_center_count));
    } else if (OB_UNLIKELY(count <= 0 || count != cache.get_capacity()
               || dim <= 0 || dim != cache_mgr.get_vec_param().dim_
               || ((centroids != nullptr) + (hgraph_handle != nullptr)
                   + cache.has_shared_leaf_refs() != 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF SSTable centroid cache representation", KR(ret),
               KP(centroids), KP(hgraph_handle), K(count), K(dim));
    }
  }

  if (OB_SUCC(ret) && hgraph_handle != nullptr) {
    release_array_guard();
    if (OB_FAIL(open_hgraph_cache(hgraph_handle, count, dim))) {
      LOG_WARN("[DISK_BBQ] failed to open centroid group HGraph", K(ret));
    }
  } else if (OB_SUCC(ret)) {
    array_centroids_ = centroids;
  }
  if (OB_FAIL(ret)) { reset_cache_state(); }
  return ret;
}

int ObIvfCentroidGroupReader::try_open_cache(
    const ObIvfKnnSearchParam &query_param,
    const share::ObLSID &ls_id,
    const share::ObIvfCacheMgrKey &cache_key,
    bool &cache_hit)
{
  int ret = OB_SUCCESS;
  cache_hit = false;
  share::ObPluginVectorIndexService *service = MTL(share::ObPluginVectorIndexService *);
  share::ObIvfCacheMgr *cache_mgr = nullptr;
  share::ObIvfSSTableCentCache *cache = nullptr;
  const bool lookup_enabled = OB_NOT_NULL(service) && ls_id.is_valid()
      && service->get_ivf_sstable_cache_controller().is_cache_enabled();
  if (!lookup_enabled) {
  } else if (OB_FAIL(service->acquire_ivf_cache_mgr_guard(
                 ls_id, cache_key, cache_mgr_guard_))) {
    if (OB_HASH_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      cache_mgr_guard_.reset();
    } else {
      LOG_WARN("failed to lookup IVF SSTable centroid cache", KR(ret), K(ls_id), K(cache_key));
    }
  } else if (OB_ISNULL(cache_mgr = cache_mgr_guard_.get_ivf_cache_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF SSTable cache manager guard returned null manager",
             KR(ret), K(ls_id), K(cache_key));
  } else if (OB_FAIL(cache_mgr->get_cache_node<share::ObIvfSSTableCentCache>(
                 share::IvfCacheType::IVF_SSTABLE_CENTROID_CACHE, cache))) {
    if (OB_HASH_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      cache_mgr_guard_.reset();
    }
  } else if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF SSTable cache manager returned null centroid cache",
             KR(ret), K(ls_id), K(cache_key));
  } else if (!cache->is_completed()) {
    cache_mgr_guard_.reset();
  } else if (OB_FAIL(bind_cache(*cache, *cache_mgr))) {
    LOG_WARN("failed to search completed IVF SSTable centroid cache",
             KR(ret), K(cache_key));
  } else {
    cache_hit = true;
    cache_hit_ = true;
    shared_cache_ = true;
    if (cache->is_memory_registered()
        && cache->can_build_hgraph()
        && !cache->has_hgraph_index()) {
      const int tmp_ret = service->enqueue_ivf_sstable_hgraph_build(
          ls_id, cache_key);
      if (OB_SUCCESS != tmp_ret) {
        // ARRAY remains queryable. A failed best-effort enqueue must not fail
        // the foreground query.
        LOG_WARN_RET(tmp_ret, "failed to re-enqueue IVF SSTable HGraph build",
                     K(tmp_ret), K(ls_id), K(cache_key));
      }
    }
  }
  // Count an enabled lookup once, before a miss may build and use a new cache.
  if (OB_SUCC(ret) && lookup_enabled) {
    service->get_ivf_sstable_cache_controller().record_cache_access(cache_hit);
  }
  if (OB_FAIL(ret)) {
    reset_cache_state();
    cache_mgr_guard_.reset();
    cache_hit = false;
  }
  return ret;
}

int ObIvfCentroidGroupReader::open(const ObIvfKnnSearchParam &query_param,
    const ObIvfQueryVectorContext &query, ObIvfSSTable &sstable,
    const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
    ObTableAccessContext &access_ctx, const int64_t group_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  bool cache_hit = false;
  share::ObIvfSSTableCentCache *cent_cache = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DISK_BBQ] centroid group reader is already initialized", K(ret), K(group_id));
  } else if (group_id < 0 || count < 0 || !query.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid group reader arguments", K(ret), K(group_id), K(count));
  } else {
    meta_ = sstable.get_ivf_meta();
    centroid_group_id_ = group_id;
    expected_center_count_ = count;
    query_vector_ctx_ = &query;
    const share::ObLSID cache_ls_id = access_ctx.ls_id_;
    const share::ObIvfCacheMgrKey cache_key(sstable.get_key(), centroid_group_id_);
    uint64_t cache_reservation = 0;
    if (OB_FAIL(try_open_cache(
            query_param, cache_ls_id, cache_key, cache_hit))) {
      LOG_WARN("failed to access IVF SSTable centroid cache",
               KR(ret), K(query_param));
    } else if (!cache_hit) {
      const int prepare_ret = prepare_array_cache_build(
          sstable, iter_param, query_param.cluster_size_, cache_ls_id, cache_key,
          cache_reservation, cent_cache);
      if (OB_SUCCESS != prepare_ret) {
        LOG_WARN("failed to prepare IVF SSTable ARRAY cache, use SSTable",
                 K(prepare_ret), K(query_param));
      } else if (OB_NOT_NULL(cent_cache)) {
        const int build_ret = build_array_cache(
            sstable, iter_param, access_ctx, row_desc, *cent_cache, cache_reservation);
        const bool published = finish_array_cache_build(
            build_ret, cache_ls_id, cache_key, cache_reservation, cent_cache);
        if (OB_SUCCESS != build_ret) {
          LOG_WARN("failed to build IVF SSTable ARRAY cache", K(build_ret), K(cache_key));
          if (build_ret != OB_ALLOCATE_MEMORY_FAILED) { ret = build_ret; }
        } else if (published) {
          share::ObIvfCacheMgr *cache_mgr = cache_mgr_guard_.get_ivf_cache_mgr();
          if (OB_ISNULL(cache_mgr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("IVF SSTable cache manager is null after cache publication",
                     KR(ret), K(cache_key));
          } else if (OB_FAIL(bind_cache(*cent_cache, *cache_mgr))) {
            LOG_WARN("failed to search newly published IVF SSTable cache",
                     KR(ret), K(cache_key));
          } else {
            cache_hit = true;
          }
        }
        cent_cache = nullptr;
      }
    }

    if (OB_SUCC(ret)) { is_inited_ = true; }
  }
  if (OB_FAIL(ret) && !is_inited_) { reset(); }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
// ObIvfCentroidGroupReader - ordered rows and graph batches
////////////////////////////////////////////////////////////////////////////

ObIvfCentroidInfo ObIvfCentroidGroupReader::array_centroid(const int64_t slot) const
{
  ObIvfCentroidInfo item;
  item.cid_ = active_cache_->get_centroid_id(slot);
  item.parent_id_ = centroid_group_id_;
  item.dim_ = meta_.dim_;
  item.child_count_ = active_cache_->get_child_count(slot);
  item.data_ = active_cache_->get_centroid_vector(slot);
  return item;
}

template <typename Visitor>
int ObIvfCentroidGroupReader::scan_centroids(ObIvfSSTable &table,
    const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
    ObTableAccessContext &ctx, Visitor &visitor)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] centroid group reader is not initialized", K(ret));
  } else if (has_hgraph()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[DISK_BBQ] HGraph groups require graph batch search", K(ret), K_(centroid_group_id));
  } else if (has_array()) {
    for (int64_t slot = 0; OB_SUCC(ret) && slot < array_count(); ++slot) {
      const ObIvfCentroidInfo item = array_centroid(slot);
      if (OB_FAIL(visitor(item))) {
        LOG_WARN("[DISK_BBQ] failed to consume cached centroid", K(ret), K(slot), K_(centroid_group_id));
      }
    }
  } else {
    ScanContext scan;
    ObStoreRowIterator *scanner = nullptr;
    const auto saved_order = ctx.query_flag_.scan_order_;
    int64_t rows = 0;
    const int64_t limit = expected_center_count_ > 0 ? expected_center_count_ : INT64_MAX;
    if (OB_FAIL(open_centroid_scanner(table, iter_param, ctx, scan, scanner))) {
      LOG_WARN("[DISK_BBQ] failed to open centroid group scan", K(ret), K_(centroid_group_id));
    }
    // Physical rows include SOAR copies, even when the consumer skips them.
    while (OB_SUCC(ret) && rows < limit) {
      const ObDatumRow *row = nullptr;
      if (OB_FAIL(scanner->get_next_row(row))) {
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
        else { LOG_WARN("[DISK_BBQ] failed to read centroid group row", K(ret), K(rows)); }
        break;
      } else if (row == nullptr) {
        ret = OB_INVALID_DATA;
        LOG_WARN("[DISK_BBQ] null centroid group row", K(ret), K(rows));
      } else {
        const auto &cid = row->storage_datums_[row_desc.centroid_id_col_idx_];
        const auto &vid = row->storage_datums_[row_desc.vector_id_col_idx_];
        const auto &payload = row->storage_datums_[row_desc.payload_col_idx_];
        ObIvfCentroidInfo item;
        item.dim_ = meta_.dim_;
        if (cid.is_null() || cid.ptr_ == nullptr || cid.len_ != sizeof(int64_t)
            || vid.is_null() || vid.ptr_ == nullptr || vid.len_ != sizeof(int64_t)
            || payload.is_null() || payload.ptr_ == nullptr
            || payload.len_ < ObIvfCentroidPayload::get_size(meta_.dim_)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] invalid centroid group datum", K(ret), K(rows), KPC(row));
        } else if (OB_FAIL(meta_.decode_centroid_key(cid.get_int(), vid.get_int(), item.cid_, item.parent_id_))) {
          LOG_WARN("[DISK_BBQ] failed to decode centroid group key", K(ret), K(rows));
        } else if (item.parent_id_ != centroid_group_id_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] centroid belongs to another group", K(ret), K(item), K_(centroid_group_id));
        } else if (OB_FAIL(ObIvfCentroidPayload::decode(
            payload.get_string(), meta_.dim_, item.data_, item.child_count_))) {
          LOG_WARN("[DISK_BBQ] failed to decode centroid group payload", K(ret), K(rows));
        } else if (OB_FAIL(visitor(item))) {
          LOG_WARN("[DISK_BBQ] failed to consume stored centroid", K(ret), K(item.cid_));
        } else { ++rows; }
      }
    }
    if (OB_SUCC(ret) && expected_center_count_ > 0 && rows != expected_center_count_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] centroid group ended before expected row count", K(ret), K(rows),
          K_(expected_center_count), K_(centroid_group_id));
    }
    ctx.query_flag_.scan_order_ = saved_order;
    if (scanner != nullptr) {
      scanner->reuse();
      if (ctx.get_stmt_iter_pool() != nullptr) { ctx.get_stmt_iter_pool()->return_iter(scanner); }
      else { scanner->~ObStoreRowIterator(); FREE_TABLE_STORE_ROW_IETRATOR(ctx, scanner); }
    }
    LOG_TRACE("IVF centroid storage resource sample", K(ret), K_(centroid_group_id),
        "row_count", rows, "dimension", meta_.dim_);
  }
  return ret;
}

int ObIvfCentroidGroupReader::fetch_hgraph_batch(const int64_t nprobe, const bool with_vectors,
    ObIvfCentroidInfo *&results, int64_t &result_count)
{
  int ret = OB_SUCCESS;
  results = nullptr;
  result_count = 0;
  if (nprobe <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nprobe for IVF SSTable HGraph batch",
             KR(ret), K(nprobe));
  } else if (!hgraph_ctx_.is_active() || !hgraph_ctx_.has_next_
      || OB_ISNULL(hgraph_ctx_.allocator_) || OB_ISNULL(query_vector_ctx_)) {
    ret = OB_ITER_END;
  } else {
    release_hgraph_vectors();
    const int64_t request_count = std::min(
        nprobe, hgraph_ctx_.centroid_count_ - hgraph_ctx_.visited_count_);
    // VSAG continues from discarded candidates. Keep one beyond the returned
    // batch when possible; k == ef can otherwise drain the continuation queue.
    const int ef_search = request_count < 64 ? 64
        : static_cast<int>(std::min<int64_t>(request_count, INT_MAX - 1)) + 1;
    const float *distances = nullptr;
    const int64_t *ids = nullptr;
    float *batch_centroids = nullptr;
    const char *extra_info = nullptr;
    int64_t result_size = 0;
    if (request_count <= 0) {
      hgraph_ctx_.has_next_ = false;
      ret = OB_ITER_END;
    } else if (OB_FAIL(obvectorutil::knn_search(
            hgraph_ctx_.handle_, const_cast<float *>(query_vector_ctx_->query()),
            hgraph_ctx_.centroid_dim_,
            request_count, distances, ids, extra_info, result_size,
            ef_search, nullptr, false, false, 1.0f,
            hgraph_ctx_.allocator_, false, hgraph_ctx_.iter_ctx_, false))) {
      LOG_WARN("failed to incrementally search IVF SSTable HGraph",
               KR(ret), K(request_count), K(ef_search),
               "visited_count", hgraph_ctx_.visited_count_);
    } else if (0 == result_size) {
      hgraph_ctx_.has_next_ = false;
      // vasg may not search all node in graph, so return OB_ITER_END when vasg return 0
      ret = OB_ITER_END;
      LOG_WARN("IVF SSTable HGraph returned an empty batch",
               KR(ret), K(request_count),
               "visited_count", hgraph_ctx_.visited_count_);
    } else if (result_size < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF SSTable HGraph returned an invalid result size",
               KR(ret), K(result_size), K(request_count));
    } else if (OB_ISNULL(ids) || OB_ISNULL(distances)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF SSTable HGraph search result",
               KR(ret), KP(ids), KP(distances), K(result_size), K(request_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < result_size; ++i) {
        const int64_t cid = ids[i];
        if (cid < 1 || cid > hgraph_ctx_.centroid_count_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid centroid id returned by SSTable HGraph",
                   KR(ret), K(cid),
                   "centroid_count", hgraph_ctx_.centroid_count_);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (with_vectors && OB_FAIL(obvectorutil::get_raw_vector_by_ids(
                     hgraph_ctx_.handle_, ids, result_size, batch_centroids,
                     hgraph_ctx_.allocator_))) {
        LOG_WARN("failed to read centroids from IVF SSTable HGraph",
                 KR(ret), K(result_size),
                 "centroid_dim", hgraph_ctx_.centroid_dim_);
      } else if (with_vectors && OB_ISNULL(batch_centroids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF SSTable HGraph returned null centroid vectors",
                 KR(ret), K(result_size),
                 "centroid_dim", hgraph_ctx_.centroid_dim_);
      } else if (OB_ISNULL(results = static_cast<ObIvfCentroidInfo *>(
                     query_allocator_.alloc(result_size * sizeof(ObIvfCentroidInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate IVF SSTable HGraph centroid results",
                 KR(ret), K(result_size));
      } else {
        hgraph_ctx_.batch_centroids_ = batch_centroids;
        batch_centroids = nullptr;
        result_count = result_size;
        for (int64_t i = 0; i < result_size; ++i) {
          new (results + i) ObIvfCentroidInfo();
          const int64_t cid = ids[i];
          results[i].cid_ = cid;
          results[i].parent_id_ = centroid_group_id_;
          results[i].data_ = !with_vectors ? nullptr : hgraph_ctx_.batch_centroids_
              + i * hgraph_ctx_.centroid_dim_;
          results[i].child_count_ = active_cache_ == nullptr ? 0 : active_cache_->get_child_count(cid - 1);
          results[i].dim_ = hgraph_ctx_.centroid_dim_;
          results[i].distance_ = distances[i];
        }
        hgraph_ctx_.visited_count_ += result_size;
        hgraph_ctx_.has_next_ = result_size >= request_count
            && hgraph_ctx_.visited_count_ < hgraph_ctx_.centroid_count_;
      }
    }
    if (OB_NOT_NULL(batch_centroids)) {
      hgraph_ctx_.allocator_->Deallocate(batch_centroids);
      batch_centroids = nullptr;
    }
  }
  return ret;
}

int ObIvfCentroidGroupReader::read_hgraph_vectors(
    const int64_t *ids, const int64_t count, float *&vectors)
{
  int ret = OB_SUCCESS;
  vectors = nullptr;
  release_hgraph_vectors();
  if (OB_FAIL(obvectorutil::get_raw_vector_by_ids(hgraph_ctx_.handle_, ids, count,
      hgraph_ctx_.batch_centroids_, hgraph_ctx_.allocator_))) {
    LOG_WARN("[DISK_BBQ] failed to fetch HGraph centroid vectors", K(ret), K(count));
  } else if (hgraph_ctx_.batch_centroids_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DISK_BBQ] HGraph centroid vector buffer is null", K(ret), K(count));
  } else { vectors = hgraph_ctx_.batch_centroids_; }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
// ObIvfFlatCentroidIterator - root selection and ordered continuation
////////////////////////////////////////////////////////////////////////////

ObIvfFlatCentroidIterator::ObIvfFlatCentroidIterator(ObIAllocator &allocator, const bool route_only)
  : ObIvfCentroidIterator(allocator), reader_(allocator), route_only_(route_only), defer_storage_vectors_(false)
{}
ObIvfFlatCentroidIterator::~ObIvfFlatCentroidIterator() { reset(); }
void ObIvfFlatCentroidIterator::reset()
{
  release_results();
  reader_.reset();
  reset_common();
  defer_storage_vectors_ = false;
}
int ObIvfFlatCentroidIterator::open(const ObIvfKnnSearchParam &param, const float *query,
    ObIvfSSTable &table, const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
    ObTableAccessContext &ctx, const ObIvfQueryVectorContext *shared_context)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_query(param, query, table, iter_param, row_desc, ctx, shared_context))) {
    LOG_WARN("[DISK_BBQ] failed to initialize flat centroid query", K(ret));
  } else {
    defer_storage_vectors_ = param.iterative_centroids_ && !route_only_;
    if (OB_FAIL(reader_.open(param, *query_vector_ctx_, table, iter_param, row_desc, ctx,
        0, meta_.root_centroid_count()))) {
      LOG_WARN("[DISK_BBQ] failed to open root centroid group", K(ret));
    } else if (reader_.has_hgraph()) {
      owns_result_vectors_ = false;
    } else if (OB_FAIL(search_centroids_by_scan(param, table, iter_param, row_desc, ctx))) {
      LOG_WARN("[DISK_BBQ] failed to select root centroids", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && !is_inited_) { reset(); }
  return ret;
}
int ObIvfFlatCentroidIterator::search_centroids_by_scan(const ObIvfKnnSearchParam &param,
    ObIvfSSTable &table, const ObTableIterParam &iter_param,
    const ObIvfSSTableRowDesc &row_desc, ObTableAccessContext &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t dim = meta_.dim_;
  const int64_t bytes = dim * static_cast<int64_t>(sizeof(float));
  const bool cached = reader_.has_array();
  const auto &computer = query_vector_ctx_->computer();
  // Keep the ARRAY full-selection fast path: fill a contiguous result array
  // and sort once rather than pushing every center through a heap.
  if (cached && param.nprobe_ >= reader_.array_count()) {
    const int64_t count = reader_.array_count();
    owns_result_vectors_ = false;
    if ((results_ = static_cast<ObIvfCentroidInfo *>(query_allocator_.alloc(count * sizeof(ObIvfCentroidInfo)))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate ARRAY centroid results", K(ret), K(count));
    } else {
      result_count_ = 0;
      for (int64_t i = 0; i < count; ++i) { new (results_ + i) ObIvfCentroidInfo(); }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const ObIvfCentroidInfo item = reader_.array_centroid(i);
        if (skip_empty_centroids_ && item.child_count_ == 0) { continue; }
        auto &selected = results_[result_count_++];
        selected = item;
        if (OB_FAIL(computer.compute({reinterpret_cast<uint8_t *>(
            const_cast<float *>(selected.data_)), bytes}, selected.distance_))) {
          LOG_WARN("[DISK_BBQ] failed to compute cached root centroid distance", K(ret), K(i));
        }
        if (route_only_) { selected.data_ = nullptr; }
      }
      if (OB_SUCC(ret) && result_count_ > 1) {
        lib::ob_sort(results_, results_ + result_count_, ObIvfCentroidInfoCmp());
      }
    }
  } else {
    ObArenaAllocator scratch(ObMemAttr(MTL_ID(), "IVFCentroidTmp"));
    ObIvfCentroidInfoCmp cmp;
    ObIvfCentroidHeap heap(cmp, &scratch);
    owns_result_vectors_ = !cached && !route_only_ && !defer_storage_vectors_;
    ObIvfCentroidSelector selector(dim, bytes, param.nprobe_, computer, heap, query_allocator_,
        owns_result_vectors_, defer_storage_vectors_ ? 0 : initial_child_budget_);
    auto select = [&](const ObIvfCentroidInfo &input) {
      int ret = OB_SUCCESS;
      if (skip_empty_centroids_ && input.child_count_ == 0) { return ret; }
      ObIvfCentroidInfo item(input);
      if (OB_FAIL(computer.compute({reinterpret_cast<uint8_t *>(const_cast<float *>(input.data_)), bytes}, item.distance_))) {
        LOG_WARN("[DISK_BBQ] failed to compute root centroid distance", K(ret), K(item.cid_));
      } else {
        if (route_only_ || !cached) { item.data_ = nullptr; }
        ret = selector.try_add_centroid(item, input.data_);
      }
      return ret;
    };
    if (OB_FAIL(reader_.scan_centroids(table, iter_param, row_desc, ctx, select))) {
      LOG_WARN("[DISK_BBQ] failed to scan root centroid candidates", K(ret));
    } else if (OB_FAIL(collect_centroid_results(selector))) {
      LOG_WARN("[DISK_BBQ] failed to collect root centroid candidates", K(ret));
    }
    if (owns_result_vectors_) {
      for (int64_t i = 0; i < heap.count(); ++i) {
        query_allocator_.free(const_cast<float *>(heap.at(i).data_));
      }
    }
  }
  if (OB_SUCC(ret) && !cached && defer_storage_vectors_) {
    if (OB_FAIL(init_vector_reader(table, iter_param, row_desc, ctx))) {
      LOG_WARN("[DISK_BBQ] failed to initialize flat centroid vector reader", K(ret));
    }
  }
  if (route_only_) { reader_.release_array_guard(); }
  return ret;
}

int ObIvfFlatCentroidIterator::collect_centroid_results(ObIvfCentroidSelector &centroid_selector)
{
  int ret = OB_SUCCESS;
  const int64_t selected_count = centroid_selector.selected_centroid_heap_.count();
  if (selected_count > 0
      && OB_ISNULL(results_ = static_cast<ObIvfCentroidInfo *>(
          query_allocator_.alloc(selected_count * sizeof(ObIvfCentroidInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ivf centroid results", K(ret), K(selected_count));
  }
  if (OB_SUCC(ret)) {
    result_count_ = selected_count;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < result_count_; ++i) {
    new (results_ + i) ObIvfCentroidInfo();
  }
  for (int64_t i = result_count_ - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObIvfCentroidInfo item = centroid_selector.selected_centroid_heap_.top();
    if (OB_FAIL(centroid_selector.selected_centroid_heap_.pop())) {
      LOG_WARN("failed to pop selected ivf centroid", K(ret), K(i));
    } else {
      results_[i] = item;
    }
  }
  return ret;
}

int ObIvfFlatCentroidIterator::fetch_hgraph_batch(const int64_t nprobe)
{
  release_results();
  owns_result_vectors_ = false;
  return reader_.fetch_hgraph_batch(nprobe, !route_only_, results_, result_count_);
}

int ObIvfFlatCentroidIterator::fetch_hgraph_budget_batch(
    const int64_t child_budget, const int64_t cluster_size, const int64_t nonempty_limit)
{
  int ret = OB_SUCCESS;
  ObArray<ObIvfCentroidInfo, ObWrapperAllocator> centers;
  ObArray<int64_t, ObWrapperAllocator> ids;
  centers.set_block_allocator(ObWrapperAllocator(query_allocator_));
  ids.set_block_allocator(ObWrapperAllocator(query_allocator_));
  uint64_t children = 0;
  const int64_t effective_cluster_size = cluster_size > 0
      ? cluster_size : share::ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE;
  const int64_t estimate = child_budget > 0 ? child_budget / effective_cluster_size
      + (child_budget % effective_cluster_size != 0) : nonempty_limit;
  const int64_t first_batch_count = std::min(meta_.root_centroid_count(), std::max<int64_t>(1, estimate));
  const bool saved_route_only = route_only_;
  route_only_ = true;
  // Match the mainline HGraph window: estimate the first batch from cluster
  // size, then request eight more centers whenever the budget is not met.
  bool first_fetch = true;
  while (OB_SUCC(ret) && (child_budget > 0 ? children < static_cast<uint64_t>(child_budget)
      : centers.count() < nonempty_limit) && reader_.can_continue_hgraph_search()) {
    ret = fetch_hgraph_batch(first_fetch ? first_batch_count : 8);
    first_fetch = false;
    if (ret == OB_ITER_END || (ret == OB_ENTRY_NOT_EXIST && !reader_.can_continue_hgraph_search())) {
      ret = OB_SUCCESS;
      break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < result_count_; ++i) {
      if ((skip_empty_centroids_ || child_budget > 0) && results_[i].child_count_ == 0) { continue; }
      if (OB_FAIL(centers.push_back(results_[i]))) {
        LOG_WARN("[DISK_BBQ] failed to retain budgeted HGraph identity", K(ret), K(i));
      } else { children += results_[i].child_count_; }
    }
  }
  route_only_ = saved_route_only;
  release_results();
  if (OB_SUCC(ret) && centers.empty()) {
    ret = OB_ITER_END;
  } else if (OB_SUCC(ret)) {
    // Preserve HGraph batch order, including surplus identities from the last
    // batch for a later filter window. No cross-batch sorting is needed.
    for (int64_t i = 0; OB_SUCC(ret) && i < centers.count(); ++i) {
      if (OB_FAIL(ids.push_back(centers.at(i).cid_))) {
        LOG_WARN("[DISK_BBQ] failed to retain budgeted HGraph CID", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && (results_ = static_cast<ObIvfCentroidInfo *>(
        query_allocator_.alloc(centers.count() * sizeof(ObIvfCentroidInfo)))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate budgeted HGraph results", K(ret), "count", centers.count());
    } else if (OB_SUCC(ret)) {
      result_count_ = centers.count();
      for (int64_t i = 0; i < result_count_; ++i) { new (results_ + i) ObIvfCentroidInfo(centers.at(i)); }
      float *vectors = nullptr;
      if (!route_only_ && OB_FAIL(reader_.read_hgraph_vectors(ids.get_data(), ids.count(), vectors))) {
        LOG_WARN("[DISK_BBQ] failed to read budgeted HGraph vectors", K(ret), K_(result_count));
      } else if (!route_only_) {
        for (int64_t i = 0; i < result_count_; ++i) { results_[i].data_ = vectors + i * meta_.dim_; }
      }
    }
  }
  return ret;
}

int ObIvfFlatCentroidIterator::prepare_initial_batch(
    const int64_t child_budget, const int64_t cluster_size, int64_t &selected_count)
{
  int ret = OB_SUCCESS;
  selected_count = 0;
  if (OB_FAIL(validate_initial_batch(child_budget))) {
  } else {
    if (reader_.has_hgraph() && OB_FAIL(fetch_hgraph_budget_batch(child_budget, cluster_size))) {
      if (ret != OB_ITER_END) { LOG_WARN("[DISK_BBQ] failed to collect initial HGraph centers", K(ret)); }
    }
    uint64_t children = 0;
    while (OB_SUCC(ret) && selected_count < result_count_ && children < static_cast<uint64_t>(child_budget)) {
      children += results_[selected_count++].child_count_;
    }
    if (OB_SUCC(ret) && selected_count == 0) { ret = OB_ITER_END; }
    else if (OB_SUCC(ret) && vector_reader_ != nullptr && OB_FAIL(prepare_result_vectors(0, selected_count))) {
      LOG_WARN("[DISK_BBQ] failed to materialize flat initial centroids", K(ret), K(selected_count));
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) { reset(); }
  return ret;
}

int ObIvfFlatCentroidIterator::prepare_next_batch(const int64_t nprobe)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF centroid iterator is not initialized", KR(ret), K(nprobe));
  } else if (nprobe <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next nprobe for IVF centroid iterator", KR(ret), K(nprobe));

  } else if (!reader_.has_hgraph()) {
    // ARRAY and SSTable retain the ordered identities. Only iterative cold
    // vectors are materialized here, using the existing multi-get prefetcher.
    if (vector_reader_ != nullptr && cursor_ < result_count_
        && OB_FAIL(prepare_result_vectors(cursor_, MIN(nprobe, result_count_ - cursor_)))) {
      LOG_WARN("failed to materialize flat centroid batch", K(ret), K(nprobe));
      reset();
    }
  } else if (cursor_ < result_count_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("previous IVF SSTable HGraph batch is not fully consumed",
             KR(ret), K_(cursor), K_(result_count));
  } else if (!reader_.can_continue_hgraph_search()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(skip_empty_centroids_
      ? fetch_hgraph_budget_batch(0, 0, nprobe) : fetch_hgraph_batch(nprobe))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare next IVF SSTable HGraph batch", KR(ret), K(nprobe));
    }
  }
  return ret;
}

int ObIvfFlatCentroidIterator::get_next(ObIvfCentroidInfo &result)
{ return get_next_result(result, !route_only_); }
bool ObIvfFlatCentroidIterator::has_remaining_centroids() const
{
  return is_inited_ && (cursor_ < result_count_ || (reader_.has_hgraph() && reader_.can_continue_hgraph_search()));
}

////////////////////////////////////////////////////////////////////////////
// ObIvfHierarchicalCentroidIterator - parent expansion and leaf candidates
////////////////////////////////////////////////////////////////////////////

struct ObIvfHierarchicalCentroidIterator::HierarchicalSearchState
{
  enum class VectorSaveMode { BORROW, COPY, KEY_ONLY };
  struct Candidate : ObIvfCentroidInfo
  {
    Candidate() : ObIvfCentroidInfo(), owned_vector_(nullptr) {}
    explicit Candidate(const ObIvfCentroidInfo &value) : ObIvfCentroidInfo(value), owned_vector_(nullptr) {}
    float *owned_vector_;
  };
  struct CandidateCmp
  {
    // Iterative: nearest first, retaining all unconsumed unique candidates.
    // Non-iterative: worst first, retaining only the nearest leaf_limit entries.
    explicit CandidateCmp(bool iterative) : iterative_(iterative) {}
    bool operator()(const ObIvfCentroidInfo &a, const ObIvfCentroidInfo &b) const
    { return iterative_ ? ObIvfCentroidInfoCmp()(b, a) : ObIvfCentroidInfoCmp()(a, b); }
    int get_error_code() const { return OB_SUCCESS; }
    const bool iterative_;
  };
  HierarchicalSearchState(ObIAllocator &query_allocator, const ObIvfKnnSearchParam &query_param,
      const ObIvfQueryVectorContext &query_vector_ctx, ObIvfSSTable &sstable, const ObTableIterParam &table_iter_param,
      const ObIvfSSTableRowDesc &centroid_row_desc, ObTableAccessContext &table_access_ctx)
    : query_allocator_(query_allocator), query_param_(query_param), query_vector_ctx_(query_vector_ctx),
      sstable_(sstable), table_iter_param_(table_iter_param), centroid_row_desc_(centroid_row_desc),
      table_access_ctx_(table_access_ctx), parent_centroid_iterator_(nullptr), child_group_readers_(),
      leaf_candidate_cmp_(query_param.iterative_centroids_), leaf_candidate_heap_(leaf_candidate_cmp_, &query_allocator),
      seen_leaf_cid_bitmap_(nullptr), initial_parent_expansion_done_(false), initial_batch_prepared_(false),
      expanded_parent_count_(0), expanded_child_centroid_row_count_(0), prepared_leaf_count_(0),
      leaf_candidate_posting_count_(0),
      owned_vector_buffers_(), reusable_vector_buffers_(), auto_nprobe_centroid_cmp_(),
      auto_nprobe_centroid_heap_(auto_nprobe_centroid_cmp_, &query_allocator),
      auto_nprobe_centroid_selector_(query_param.dim_, query_param.dim_ * sizeof(float), INT64_MAX,
          query_vector_ctx.computer(), auto_nprobe_centroid_heap_, query_allocator,
          false /* need_copy_centroid_vectors */, query_param.centroid_child_budget_)
  {
    child_group_readers_.set_block_allocator(ObWrapperAllocator(query_allocator));
    owned_vector_buffers_.set_block_allocator(ObWrapperAllocator(query_allocator));
    reusable_vector_buffers_.set_block_allocator(ObWrapperAllocator(query_allocator));
  }
  ~HierarchicalSearchState()
  {
    for (int64_t i = 0; i < owned_vector_buffers_.count(); ++i) { query_allocator_.free(owned_vector_buffers_.at(i)); }
    // Release and touch children before the parent, including one-shot searches.
    for (int64_t i = 0; i < child_group_readers_.count(); ++i) {
      if (child_group_readers_.at(i) != nullptr) {
        child_group_readers_.at(i)->~ObIvfCentroidGroupReader();
        query_allocator_.free(child_group_readers_.at(i));
      }
    }
    if (parent_centroid_iterator_ != nullptr) {
      parent_centroid_iterator_->~ObIvfFlatCentroidIterator();
      query_allocator_.free(parent_centroid_iterator_);
    }
    if (seen_leaf_cid_bitmap_ != nullptr) { query_allocator_.free(seen_leaf_cid_bitmap_); }
  }
  int init()
  {
    int ret = OB_SUCCESS;
    const ObIvfSSTableMeta &meta = sstable_.get_ivf_meta();
    const int64_t leaf_bytes = meta.centroid_count_ / 8 + (meta.centroid_count_ % 8 != 0);
    void *mem = nullptr;
    if (leaf_bytes <= 0 || meta.parent_centroid_count_ <= 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid grouped centroid search metadata", K(ret), K(meta), K_(expanded_parent_count));
    } else if ((seen_leaf_cid_bitmap_ = static_cast<uint8_t *>(query_allocator_.alloc(leaf_bytes))) == nullptr
        || (mem = query_allocator_.alloc(sizeof(ObIvfFlatCentroidIterator))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate grouped centroid search state", K(ret),
          "sstable_key", sstable_.get_key(), K(leaf_bytes), KP_(seen_leaf_cid_bitmap), KP(mem),
          "parent_reader_bytes", sizeof(ObIvfFlatCentroidIterator));
    } else {
      MEMSET(seen_leaf_cid_bitmap_, 0, leaf_bytes);
      parent_centroid_iterator_ = new (mem) ObIvfFlatCentroidIterator(query_allocator_, true /* route_only */);
      ObIvfKnnSearchParam query_param = query_param_;
      query_param.nprobe_ = INT64_MAX;
      query_param.centroid_child_budget_ = 0;
      query_param.skip_empty_centroids_ = false; // Parent counts describe child centers, not postings.
      if (OB_FAIL(parent_centroid_iterator_->open(query_param, query_vector_ctx_.query(), sstable_,
          table_iter_param_, centroid_row_desc_, table_access_ctx_, &query_vector_ctx_))) {
        LOG_WARN("[DISK_BBQ] failed to initialize parent centroid routing", K(ret), K(meta), K_(expanded_parent_count));
      }
    }
    return ret;
  }
  bool parent_remaining() const
  { return parent_centroid_iterator_ != nullptr && parent_centroid_iterator_->has_remaining_centroids(); }
  int check_and_mark_leaf(const int64_t cid, bool &seen)
  {
    int ret = OB_SUCCESS;
    seen = false;
    const int64_t idx = cid - sstable_.get_ivf_meta().leaf_cid_base();
    if (idx < 0 || idx >= sstable_.get_ivf_meta().centroid_count_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] leaf centroid CID is out of range", K(ret), K(cid), K(idx),
          "leaf_count", sstable_.get_ivf_meta().centroid_count_);
    } else {
      uint8_t &bits = seen_leaf_cid_bitmap_[idx / 8];
      const uint8_t mask = 1U << (idx % 8);
      seen = (bits & mask) != 0;
      if (!seen) { bits |= mask; }
    }
    return ret;
  }
  int acquire_vector(const int64_t bytes, float *&vector)
  {
    int ret = OB_SUCCESS;
    if (!reusable_vector_buffers_.empty()) {
      vector = reusable_vector_buffers_.at(reusable_vector_buffers_.count() - 1);
      reusable_vector_buffers_.pop_back();
    } else if ((vector = static_cast<float *>(query_allocator_.alloc(bytes))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate selected centroid vector", K(ret), K(bytes));
    } else if (OB_FAIL(owned_vector_buffers_.push_back(vector))) {
      LOG_WARN("[DISK_BBQ] failed to register selected centroid vector owner", K(ret), K(bytes));
      query_allocator_.free(vector);
      vector = nullptr;
    }
    return ret;
  }
  // Only unseen CIDs reach this method. Marking happens before vector work,
  // including for candidates rejected by the heap. Any later error resets the
  // entire iterator, so partial state is never retried with a marked-only CID.
  int try_add_leaf_candidate(const ObIvfCentroidInfo &input, const int64_t limit, const VectorSaveMode mode)
  {
    int ret = OB_SUCCESS;
    const int64_t bytes = input.dim_ * static_cast<int64_t>(sizeof(float));
    const bool budget = !initial_batch_prepared_ && query_param_.centroid_child_budget_ > 0;
    const bool full = budget
        ? leaf_candidate_posting_count_ >= static_cast<uint64_t>(query_param_.centroid_child_budget_)
        : leaf_candidate_heap_.count() >= limit;
    if (!input.is_valid() || (mode != VectorSaveMode::KEY_ONLY && !input.has_vector())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid child centroid candidate", K(ret), K(input), K(limit), K(mode));
    } else if ((budget || query_param_.skip_empty_centroids_) && input.child_count_ == 0) {
    } else if (!query_param_.iterative_centroids_ && full && !leaf_candidate_heap_.empty()
        && !ObIvfCentroidInfoCmp()(input, leaf_candidate_heap_.top())) {
    } else if (leaf_candidate_posting_count_ > UINT64_MAX - input.child_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[DISK_BBQ] child centroid budget count overflow", K(ret), K_(leaf_candidate_posting_count), K(input));
    } else {
      Candidate candidate(input);
      const bool replace = !query_param_.iterative_centroids_ && full && !leaf_candidate_heap_.empty()
          && (!budget || leaf_candidate_posting_count_ - leaf_candidate_heap_.top().child_count_ + input.child_count_
              >= static_cast<uint64_t>(query_param_.centroid_child_budget_));
      const uint32_t replaced_children = replace ? leaf_candidate_heap_.top().child_count_ : 0;
      float *old_vector = replace ? leaf_candidate_heap_.top().owned_vector_ : nullptr;
      if (mode == VectorSaveMode::KEY_ONLY) { candidate.data_ = nullptr; }
      if (mode == VectorSaveMode::COPY) {
        candidate.owned_vector_ = old_vector;
        if (candidate.owned_vector_ == nullptr && OB_FAIL(acquire_vector(bytes, candidate.owned_vector_))) {
          LOG_WARN("[DISK_BBQ] failed to acquire child centroid vector", K(ret), K(bytes));
        } else {
          MEMCPY(candidate.owned_vector_, input.data_, bytes);
          candidate.data_ = candidate.owned_vector_;
        }
      } else if (old_vector != nullptr && OB_FAIL(reusable_vector_buffers_.push_back(old_vector))) {
        LOG_WARN("[DISK_BBQ] failed to recycle replaced child centroid buffer", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(replace
          ? leaf_candidate_heap_.replace_top(candidate) : leaf_candidate_heap_.push(candidate))) {
        LOG_WARN("[DISK_BBQ] failed to retain child centroid", K(ret), K(candidate), K(replace));
      } else if (OB_SUCC(ret)) {
        leaf_candidate_posting_count_ = leaf_candidate_posting_count_ - replaced_children + input.child_count_;
      }
      if (OB_SUCC(ret) && budget && query_param_.iterative_centroids_) {
        ObIvfCentroidInfo identity(input);
        identity.data_ = nullptr;
        if (OB_FAIL(auto_nprobe_centroid_selector_.try_add_centroid(identity, nullptr))) {
          LOG_WARN("[DISK_BBQ] failed to track initial iterative leaf prefix", K(ret), K(input));
        }
      }
      while (OB_SUCC(ret) && !query_param_.iterative_centroids_ && !leaf_candidate_heap_.empty()
          && (budget ? leaf_candidate_posting_count_ - leaf_candidate_heap_.top().child_count_
                           >= static_cast<uint64_t>(query_param_.centroid_child_budget_)
                     : leaf_candidate_heap_.count() > limit)) {
        const Candidate removed = leaf_candidate_heap_.top();
        if (OB_FAIL(leaf_candidate_heap_.pop())) {
          LOG_WARN("[DISK_BBQ] failed to trim child centroid selection", K(ret));
        } else {
          leaf_candidate_posting_count_ -= removed.child_count_;
          if (removed.owned_vector_ != nullptr && OB_FAIL(reusable_vector_buffers_.push_back(removed.owned_vector_))) {
            LOG_WARN("[DISK_BBQ] failed to recycle child centroid buffer", K(ret));
          }
        }
      }
    }
    return ret;
  }
  int64_t initial_leaf_count() const
  {
    return query_param_.centroid_child_budget_ > 0 && query_param_.iterative_centroids_
        ? auto_nprobe_centroid_heap_.count() : leaf_candidate_heap_.count();
  }
  bool has_initial_leaf_budget() const
  {
    return leaf_candidate_posting_count_ >= static_cast<uint64_t>(query_param_.centroid_child_budget_);
  }
  int compute_candidate(ObIvfCentroidInfo &candidate, const int64_t limit, const VectorSaveMode mode)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(query_vector_ctx_.computer().compute({reinterpret_cast<uint8_t *>(const_cast<float *>(candidate.data_)),
        candidate.dim_ * static_cast<int64_t>(sizeof(float))}, candidate.distance_))) {
      LOG_WARN("failed to compute unique child centroid distance", K(ret), K(candidate.cid_));
    } else if (OB_FAIL(try_add_leaf_candidate(candidate, limit, mode))) {
      LOG_WARN("[DISK_BBQ] failed to retain unique child centroid", K(ret), K(candidate.cid_), K(limit), K(mode));
    }
    return ret;
  }
  int next_parent(ObIvfCentroidInfo &result)
  {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ret = parent_centroid_iterator_->get_next(result);
      if (ret == OB_ITER_END && parent_centroid_iterator_->has_remaining_centroids()) {
        int64_t parent_batch_count = 64;
        if (parent_centroid_iterator_->hgraph_visited_count() == 0) {
          const ObIvfSSTableMeta &meta = sstable_.get_ivf_meta();
          int64_t estimated_leaf_count = query_param_.nprobe_;
          if (query_param_.centroid_child_budget_ > 0) {
            const int64_t leaf_cluster_size = query_param_.cluster_size_ > 0
                ? query_param_.cluster_size_ : share::ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE;
            estimated_leaf_count = query_param_.centroid_child_budget_ / leaf_cluster_size
                + (query_param_.centroid_child_budget_ % leaf_cluster_size != 0);
          }
          const int64_t parent_budget = calc_ivf_parent_child_budget(
              meta.centroid_count_, MAX(1L, estimated_leaf_count), query_param_.refine_k_);
          const int64_t estimate = parent_budget / IVF_PARENT_CLUSTER_SIZE
              + (parent_budget % IVF_PARENT_CLUSTER_SIZE != 0);
          parent_batch_count = std::min(meta.parent_centroid_count_, std::max<int64_t>(1, estimate));
        }
        // Estimate only the first HGraph request; later refills keep their
        // existing batch size. Child groups are still opened one at a time,
        // with actual child counts controlling expansion in prepare_hierarchical_batch().
        ret = parent_centroid_iterator_->prepare_next_batch(parent_batch_count);
        if (ret == OB_ENTRY_NOT_EXIST && !parent_centroid_iterator_->can_continue_hgraph_search()) { ret = OB_ITER_END; }
        if (OB_SUCC(ret)) { continue; }
      }
      if (OB_SUCC(ret)) {
        if (result.cid_ <= 0 || result.cid_ > sstable_.get_ivf_meta().parent_centroid_count_
            || result.child_count_ == 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] invalid parent centroid routing entry", K(ret), K(result),
              "parent_count", sstable_.get_ivf_meta().parent_centroid_count_);
        }
        break;
      }
    }
    return ret;
  }
  int expand_next(int64_t leaf_limit)
  {
    int ret = OB_SUCCESS;
    ObIvfCentroidInfo parent;
    ObIvfCentroidGroupReader *group = nullptr;
    void *mem = nullptr;
    if (OB_FAIL(next_parent(parent))) {
      if (ret != OB_ITER_END) { LOG_WARN("[DISK_BBQ] failed to read next parent centroid", K(ret)); }
    } else if ((mem = query_allocator_.alloc(sizeof(ObIvfCentroidGroupReader))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate child centroid group reader", K(ret), K(parent));
    } else {
      ++expanded_parent_count_;
      expanded_child_centroid_row_count_ += parent.child_count_;
      group = new (mem) ObIvfCentroidGroupReader(query_allocator_);
      if (OB_FAIL(child_group_readers_.push_back(group))) {
        LOG_WARN("[DISK_BBQ] failed to retain child group reader", K(ret));
      } else {
        ObIvfCentroidGroupReader *held_group = group;
        group = nullptr;
        if (OB_FAIL(held_group->open(query_param_, query_vector_ctx_, sstable_, table_iter_param_,
            centroid_row_desc_, table_access_ctx_, parent.cid_, parent.child_count_))) {
          LOG_WARN("[DISK_BBQ] failed to open child centroid group", K(ret), K(parent.cid_));
        } else {
          const VectorSaveMode mode = held_group->has_array()
              ? (query_param_.iterative_centroids_ || held_group->is_shared_array()
                  ? VectorSaveMode::BORROW : VectorSaveMode::COPY)
              : (query_param_.iterative_centroids_ ? VectorSaveMode::KEY_ONLY : VectorSaveMode::COPY);
          // The concrete visitor is synchronous and can be inlined. The reader
          // neither stores this callback nor refers to hierarchical state.
          auto consume = [&](const ObIvfCentroidInfo &input) {
            int ret = OB_SUCCESS;
            bool seen = false;
            if (OB_FAIL(check_and_mark_leaf(input.cid_, seen))) {
              LOG_WARN("[DISK_BBQ] failed to deduplicate child centroid", K(ret), K(input.cid_));
            } else if (!seen) {
              ObIvfCentroidInfo candidate(input);
              ret = compute_candidate(candidate, leaf_limit, mode);
            }
            return ret;
          };
          if (OB_FAIL(held_group->scan_centroids(sstable_, table_iter_param_, centroid_row_desc_,
              table_access_ctx_, consume))) {
            LOG_WARN("[DISK_BBQ] failed to collect child centroid group", K(ret), K(parent.cid_));
          } else {
            if (mode != VectorSaveMode::BORROW) {
              child_group_readers_.pop_back();
              held_group->~ObIvfCentroidGroupReader();
              query_allocator_.free(held_group);
            }
          }
        }
      }
    }
    if (group != nullptr) { group->~ObIvfCentroidGroupReader(); query_allocator_.free(group); }
    LOG_TRACE("IVF grouped centroid candidate resource sample", K(ret),
        "iterative", query_param_.iterative_centroids_, "candidate_vectors", owned_vector_buffers_.count(),
        "candidate_vector_bytes", owned_vector_buffers_.count() * query_param_.dim_ * sizeof(float));
    return ret;
  }
  ObIAllocator &query_allocator_;
  ObIvfKnnSearchParam query_param_;
  const ObIvfQueryVectorContext &query_vector_ctx_;
  ObIvfSSTable &sstable_;
  const ObTableIterParam &table_iter_param_;
  ObIvfSSTableRowDesc centroid_row_desc_;
  ObTableAccessContext &table_access_ctx_;
  ObIvfFlatCentroidIterator *parent_centroid_iterator_;
  ObArray<ObIvfCentroidGroupReader *, ObWrapperAllocator> child_group_readers_;
  CandidateCmp leaf_candidate_cmp_;
  // Unique leaf candidates waiting to be returned in a centroid batch.
  ObBinaryHeap<Candidate, CandidateCmp, 16> leaf_candidate_heap_;
  uint8_t *seen_leaf_cid_bitmap_;
  // Initial parent expansion has finished; later batches expand only as needed.
  bool initial_parent_expansion_done_;
  // The first leaf result batch was prepared successfully.
  bool initial_batch_prepared_;
  int64_t expanded_parent_count_;
  // Sum of expanded parents' child counts, including SOAR membership copies.
  uint64_t expanded_child_centroid_row_count_;
  // Unique leaves prepared across batches, including the buffered tail.
  int64_t prepared_leaf_count_;
  // Sum of child_count (posting rows) over candidates currently in the heap.
  uint64_t leaf_candidate_posting_count_;
  // Own all allocated candidate buffers, including those recycled below.
  ObArray<float *, ObWrapperAllocator> owned_vector_buffers_;
  // Non-owning pool of buffers available for the next selected candidate.
  ObArray<float *, ObWrapperAllocator> reusable_vector_buffers_;
  // Only the initial automatic, iterative search fills this auxiliary heap.
  // The main leaf heap keeps surplus candidates for subsequent batches.
  ObIvfCentroidInfoCmp auto_nprobe_centroid_cmp_;
  ObIvfCentroidHeap auto_nprobe_centroid_heap_;
  ObIvfCentroidSelector auto_nprobe_centroid_selector_;
};

ObIvfHierarchicalCentroidIterator::ObIvfHierarchicalCentroidIterator(ObIAllocator &allocator)
  : ObIvfCentroidIterator(allocator), state_(nullptr)
{}
ObIvfHierarchicalCentroidIterator::~ObIvfHierarchicalCentroidIterator() { reset(); }
void ObIvfHierarchicalCentroidIterator::reset()
{
  release_results();
  if (state_ != nullptr) {
    state_->~HierarchicalSearchState();
    query_allocator_.free(state_);
    state_ = nullptr;
  }
  reset_common();
}
int ObIvfHierarchicalCentroidIterator::open(const ObIvfKnnSearchParam &param, const float *query,
    ObIvfSSTable &table, const ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
    ObTableAccessContext &ctx, const ObIvfQueryVectorContext *shared_context)
{
  int ret = OB_SUCCESS;
  void *mem = nullptr;
  if (OB_FAIL(init_query(param, query, table, iter_param, row_desc, ctx, shared_context))) {
    LOG_WARN("[DISK_BBQ] failed to initialize hierarchical centroid query", K(ret));
  } else if (!meta_.is_hierarchical()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] hierarchical iterator requires hierarchical centroid metadata", K(ret), K(meta_));
  } else if ((mem = query_allocator_.alloc(sizeof(HierarchicalSearchState))) == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[DISK_BBQ] failed to allocate hierarchical centroid state", K(ret));
  } else {
    state_ = new (mem) HierarchicalSearchState(query_allocator_, param, *query_vector_ctx_, table, iter_param, row_desc, ctx);
    if (OB_FAIL(state_->init())) {
      LOG_WARN("[DISK_BBQ] failed to initialize hierarchical centroid state", K(ret));
    } else { is_inited_ = true; }
  }
  if (OB_FAIL(ret) && !is_inited_) { reset(); }
  return ret;
}

int ObIvfHierarchicalCentroidIterator::prepare_hierarchical_batch(int64_t nprobe)
{
  int ret = OB_SUCCESS;
  HierarchicalSearchState &state = *state_;
  const int64_t limit = MIN(nprobe, meta_.centroid_count_);
  if (cursor_ < result_count_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] previous grouped centroid batch is not consumed",
        K(ret), K_(cursor), K_(result_count), K(nprobe));
  } else if (!state.query_param_.iterative_centroids_ && state.initial_batch_prepared_) {
    ret = OB_ITER_END;
  } else {
    release_results();
    owns_result_vectors_ = false;
    const bool first_budget = !state.initial_batch_prepared_ && state.query_param_.centroid_child_budget_ > 0;
    if (!state.initial_parent_expansion_done_) {
      // Automatic leaf selection learns L from actual child counts. One group
      // bootstraps that selection; explicit nprobe already supplies L.
      bool enough = false;
      while (OB_SUCC(ret) && !enough && state.parent_remaining()) {
        if (OB_FAIL(state.expand_next(limit))) {
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
        } else {
          const int64_t target = first_budget ? state.initial_leaf_count() : limit;
          const int64_t parent_budget = calc_ivf_parent_child_budget(
              meta_.centroid_count_, MAX(1L, target), state.query_param_.refine_k_);
          enough = state.expanded_child_centroid_row_count_ >= static_cast<uint64_t>(parent_budget)
              && (first_budget ? state.has_initial_leaf_budget() : state.leaf_candidate_heap_.count() >= limit);
        }
      }
      if (OB_SUCC(ret)) { state.initial_parent_expansion_done_ = true; }
    } else {
      // Automatic PRE may need more leaves than its metadata estimate. Keep
      // parent coverage in step with the cumulative leaf demand of its refills.
      const bool auto_pre_filter = state.query_param_.pre_filter_ != nullptr
          && state.query_param_.centroid_child_budget_ > 0;
      const int64_t target = state.prepared_leaf_count_
          + MIN(limit, meta_.centroid_count_ - state.prepared_leaf_count_);
      const int64_t parent_budget = auto_pre_filter
          ? calc_ivf_parent_child_budget(meta_.centroid_count_, target, state.query_param_.refine_k_) : 0;
      while (OB_SUCC(ret) && state.parent_remaining()
          && (state.leaf_candidate_heap_.count() < limit
              || state.expanded_child_centroid_row_count_ < static_cast<uint64_t>(parent_budget))) {
        ret = state.expand_next(limit);
        if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t count = first_budget ? state.initial_leaf_count() : MIN(limit, state.leaf_candidate_heap_.count());
      if (count == 0) { ret = OB_ITER_END; }
      else if ((results_ = static_cast<ObIvfCentroidInfo *>(
          query_allocator_.alloc(count * sizeof(ObIvfCentroidInfo)))) == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[DISK_BBQ] failed to allocate grouped centroid batch results", K(ret),
            K(count), "alloc_size", count * sizeof(ObIvfCentroidInfo));
      } else {
        result_count_ = count;
        for (int64_t i = 0; i < count; ++i) {
          new (results_ + i) ObIvfCentroidInfo();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          const int64_t pos = state.query_param_.iterative_centroids_ ? i : count - i - 1;
          results_[pos] = state.leaf_candidate_heap_.top();
          state.leaf_candidate_posting_count_ -= results_[pos].child_count_;
          if (OB_FAIL(state.leaf_candidate_heap_.pop())) {
            LOG_WARN("failed to pop IVF centroid candidate", K(ret), K(i), K(count));
          }
        }
        bool missing_vectors = false;
        for (int64_t i = 0; i < count && !missing_vectors; ++i) {
          missing_vectors = !results_[i].has_vector();
        }
        if (OB_SUCC(ret) && missing_vectors) {
          if (OB_FAIL(init_vector_reader(state.sstable_, state.table_iter_param_,
              state.centroid_row_desc_, state.table_access_ctx_))) {
            LOG_WARN("failed to initialize grouped centroid batch reader", K(ret));
          } else if (OB_FAIL(prepare_result_vectors(0, count))) {
            LOG_WARN("failed to materialize grouped centroid batch", K(ret), K(count));
          }
        }
        if (OB_SUCC(ret)) {
          state.prepared_leaf_count_ += count;
          state.initial_batch_prepared_ = true;
        }
      }
    }
    if (OB_FAIL(ret) && ret != OB_ITER_END) {
      // A failed child open may already have released its cache guard. Discard
      // the partial heap so a retry cannot consume an unprotected cache pointer.
      reset();
    }
  }
  return ret;
}

int ObIvfHierarchicalCentroidIterator::prepare_initial_batch(
    const int64_t child_budget, const int64_t cluster_size, int64_t &selected_count)
{
  UNUSED(cluster_size);
  int ret = OB_SUCCESS;
  selected_count = 0;
  if (OB_FAIL(validate_initial_batch(child_budget))) {
  } else if (OB_FAIL(prepare_hierarchical_batch(INT64_MAX))) {
    if (ret != OB_ITER_END) { LOG_WARN("[DISK_BBQ] failed to select initial hierarchical centers", K(ret)); }
  } else { selected_count = result_count_; }
  if (OB_FAIL(ret) && ret != OB_ITER_END) { reset(); }
  return ret;
}
int ObIvfHierarchicalCentroidIterator::prepare_next_batch(const int64_t nprobe)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] hierarchical centroid iterator is not initialized", K(ret));
  } else if (nprobe <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid hierarchical centroid batch size", K(ret), K(nprobe));
  } else { ret = prepare_hierarchical_batch(nprobe); }
  return ret;
}
int ObIvfHierarchicalCentroidIterator::get_next(ObIvfCentroidInfo &result)
{ return get_next_result(result, true /* require_vector */); }
bool ObIvfHierarchicalCentroidIterator::has_remaining_centroids() const
{
  return is_inited_ && state_ != nullptr && (cursor_ < result_count_
      || ((!state_->initial_batch_prepared_ || state_->query_param_.iterative_centroids_)
          && (!state_->leaf_candidate_heap_.empty() || state_->parent_remaining())));
}

} // namespace blocksstable
} // namespace oceanbase

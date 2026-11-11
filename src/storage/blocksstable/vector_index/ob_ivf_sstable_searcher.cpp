/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"

#include <cmath>
#include <limits>
#include "share/vector_index/ob_vector_index_util.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_sort.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"

#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"

namespace oceanbase
{
namespace blocksstable
{

void ObIvfKnnSearchParam::reset()
{
  query_ = nullptr;
  dim_ = 0;
  nprobe_ = 0;
  centroid_child_budget_ = 0;
  iterative_centroids_ = false;
  skip_empty_centroids_ = false;
  expected_dist_type_ = -1;
  k_ = 0;
  original_k_ = 0;
  num_candidates_ = 0;
  is_set_refine_k_ = false;
  selectivity_ = -1.0;
  refine_k_ = share::ObVectorIndexParam::DEFAULT_REFINE_K;
  cluster_size_ = 0;
  read_stats_ = nullptr;
  scan_budget_ = nullptr;
  deletion_bitmap_handle_ = nullptr;
  pre_filter_ = nullptr;
}

uint64_t *ObIvfPreFilterBitmap::alloc_words(const uint64_t bit_count)
{
  uint64_t *words = static_cast<uint64_t *>(compact_alloc_->alloc(bit_count / 8));
  common::ObRbMemMgr *mem_mgr = MTL(common::ObRbMemMgr *);
  if (nullptr != words && nullptr != mem_mgr) {
    mem_mgr->incr_vec_idx_used(bit_count / 8);
  }
  return words;
}

void ObIvfPreFilterBitmap::free_words(uint64_t *words, const uint64_t bit_count)
{
  if (nullptr != words) {
    compact_alloc_->free(words);
    common::ObRbMemMgr *mem_mgr = MTL(common::ObRbMemMgr *);
    if (nullptr != mem_mgr) {
      mem_mgr->decr_vec_idx_used(bit_count / 8);
    }
  }
}

void ObIvfPreFilterBitmap::reset()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapADPV"));
  if (nullptr != words_) {
    free_words(words_, bit_count_);
    words_ = nullptr;
  }
  roaring_.set_empty();
  if (nullptr != fallback_) {
    fallback_->set_empty();
    fallback_->~ObRoaringBitmap();
    allocator_.free(fallback_);
    fallback_ = nullptr;
  }
  base_vid_ = 0;
  bit_count_ = 0;
  cardinality_ = 0;
  min_vid_ = INT64_MAX;
  max_vid_ = 0;
  estimated_cardinality_ = -1;
  next_dense_check_ = 64;
  compact_attempted_ = false;
  failed_ = false;
}

int ObIvfPreFilterBitmap::convert_to_roaring()
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapADPV"));
  int ret = OB_SUCCESS;
  common::ObRoaringBitmap *next = nullptr;
  ObIvfPreFilter filter;
  if (is_roaring()) {
  } else if (OB_ISNULL(next = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate fallback IVF filter", K(ret));
  } else if (OB_FAIL(next->convert_to_bitmap())) {
    LOG_WARN("failed to initialize fallback IVF filter", K(ret));
  } else if (OB_FAIL(filter.init(*this))) {
    LOG_WARN("failed to bind source IVF filter", K(ret));
  } else {
    ObIvfPreFilterIter iter(filter);
    ret = iter.init();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(next->value_add(iter.get_curr_value()))) {
        LOG_WARN("failed to populate fallback IVF filter", K(ret));
      } else {
        ret = iter.get_next();
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && nullptr != next) {
    // Commit only after every old member has been copied. In particular,
    // ObRoaringBitmap::convert_to_bitmap() is not transactional for a SET.
    roaring_.set_empty();
    if (nullptr != words_) {
      free_words(words_, bit_count_);
      words_ = nullptr;
    }
    bit_count_ = 0;
    fallback_ = next;
    next = nullptr;
  }
  if (nullptr != next) {
    next->set_empty();
    next->~ObRoaringBitmap();
    allocator_.free(next);
  }
  return ret;
}

int ObIvfPreFilterBitmap::reserve(
    const uint64_t min_vid, const uint64_t max_vid, const bool force_compact)
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapADPV"));
  int ret = OB_SUCCESS;
  uint64_t new_base = min_vid / 64 * 64;
  uint64_t required = 0;
  if (OB_UNLIKELY(min_vid > max_vid || max_vid > INT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compact IVF filter range", K(ret), K(min_vid), K(max_vid));
  } else {
    required = (max_vid - new_base) / 64 * 64 + 64;
  }
  if (OB_FAIL(ret)) {
  } else if (is_compact() && min_vid >= base_vid_ && max_vid - base_vid_ < bit_count_) {
  } else if (required > MAX_COMPACT_BITS) {
    ret = convert_to_roaring();
  } else if (!force_compact && !is_compact() && estimated_cardinality_ >= 0
      && MAX(static_cast<uint64_t>(estimated_cardinality_), get_cardinality()) < required / 128) {
    ret = convert_to_roaring();
  } else {
    const uint64_t capacity = MAX(required, MIN(MAX_COMPACT_BITS, bit_count_ * 2));
    if (is_compact() && min_vid < base_vid_) {
      // Put growth slack on the side being extended. Descending batches must
      // not repeatedly allocate the same large capacity for every lower word.
      new_base -= MIN(new_base, capacity - required);
    }
    uint64_t *new_words = alloc_words(capacity);
    if (OB_ISNULL(new_words)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate compact IVF filter", K(ret), K(capacity));
    } else {
      MEMSET(new_words, 0, capacity / 8);
      if (is_compact()) {
        // Copy only the populated range, not unused growth capacity. This also
        // permits moving the base right when dropping unused leading capacity.
        const uint64_t first_word = (min_vid_ - base_vid_) / 64;
        const uint64_t word_count = (max_vid_ - base_vid_) / 64 - first_word + 1;
        const uint64_t offset = (base_vid_ + first_word * 64 - new_base) / 64;
        MEMCPY(new_words + offset, words_ + first_word, word_count * sizeof(uint64_t));
      } else if (is_roaring()) {
        ObIvfPreFilter filter;
        ret = filter.init(*this);
        ObIvfPreFilterIter iter(filter);
        if (OB_SUCC(ret)) {
          ret = iter.init();
        }
        while (OB_SUCC(ret)) {
          const uint64_t offset = iter.get_curr_value() - new_base;
          new_words[offset / 64] |= uint64_t(1) << (offset % 64);
          ret = iter.get_next();
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        // Do not use ObRoaringBitmapIter here: its init converts the small
        // set to Roaring, adding allocation and mutating the borrowed set.
        if (roaring_.is_single_type()) {
          const uint64_t offset = roaring_.get_single_value() - new_base;
          new_words[offset / 64] |= uint64_t(1) << (offset % 64);
        } else if (roaring_.is_set_type()) {
          common::hash::ObHashSet<uint64_t>::const_iterator iter = roaring_.get_set()->begin();
          for (; iter != roaring_.get_set()->end(); ++iter) {
            const uint64_t offset = iter->first - new_base;
            new_words[offset / 64] |= uint64_t(1) << (offset % 64);
          }
        } else if (!roaring_.is_empty_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected small IVF filter representation", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        free_words(new_words, capacity);
      } else {
        cardinality_ = get_cardinality();
        if (nullptr != words_) {
          free_words(words_, bit_count_);
        }
        roaring_.set_empty();
        if (nullptr != fallback_) {
          fallback_->set_empty();
          fallback_->~ObRoaringBitmap();
          allocator_.free(fallback_);
          fallback_ = nullptr;
        }
        words_ = new_words;
        base_vid_ = new_base;
        bit_count_ = capacity;
        compact_attempted_ = true;
      }
    }
  }
  return ret;
}

int ObIvfPreFilterBitmap::value_add_many(const uint64_t *vids, const int64_t count)
{
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "VIBitmapADPV"));
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t min_vid = min_vid_;
  uint64_t max_vid = max_vid_;
  if (failed_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (count < 0 || (count > 0 && nullptr == vids)) {
    ret = OB_INVALID_ARGUMENT;
  }
  // Validate the entire batch before any mutation; unsigned negatives and
  // UINT64_MAX must never become allocation sizes or signed candidate IDs.
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (vids[i] > INT64_MAX) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      min_vid = MIN(min_vid, vids[i]);
      max_vid = MAX(max_vid, vids[i]);
    }
  }
  while (OB_SUCC(ret) && pos < count && !is_compact() && !is_roaring()
      && (roaring_.get_cardinality() < MAX_BITMAP_SET_VALUES || roaring_.is_contains(vids[pos]))) {
    if (OB_FAIL(roaring_.value_add(vids[pos]))) {
      LOG_WARN("failed to append small IVF filter", K(ret));
    } else {
      min_vid_ = MIN(min_vid_, vids[pos]);
      max_vid_ = MAX(max_vid_, vids[pos]);
      ++pos;
    }
  }
  if (OB_SUCC(ret) && pos < count && !is_roaring() && OB_FAIL(reserve(min_vid, max_vid))) {
    LOG_WARN("failed to reserve compact IVF filter", K(ret), K(min_vid), K(max_vid));
  }
  if (OB_SUCC(ret) && pos < count) {
    if (is_compact()) {
      for (; pos < count; ++pos) {
        const uint64_t offset = vids[pos] - base_vid_;
        const uint64_t mask = uint64_t(1) << (offset % 64);
        uint64_t &word = words_[offset / 64];
        cardinality_ += 0 == (word & mask);
        word |= mask;
      }
    } else if (OB_FAIL(get_roaring()->value_add_many(vids + pos, count - pos))) {
      LOG_WARN("failed to append roaring IVF filter", K(ret), K(count));
    }
  }
  if (OB_SUCC(ret)) {
    min_vid_ = min_vid;
    max_vid_ = max_vid;
    const uint64_t cardinality = get_cardinality();
    if (is_roaring() && !compact_attempted_ && cardinality >= next_dense_check_) {
      next_dense_check_ = cardinality * 2;
      const uint64_t required = (max_vid_ - min_vid_ / 64 * 64) / 64 * 64 + 64;
      // Check at geometrically increasing cardinalities. Promotion requires
      // twice the final density threshold to avoid reacting to tiny batches.
      if (required <= MAX_COMPACT_BITS && cardinality >= required / 64
          && OB_FAIL(reserve(min_vid_, max_vid_, true))) {
        LOG_WARN("failed to promote IVF pre-filter bitmap", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // No consumer may use a partially inserted batch. The caller must abort
    // materialization; reset is required before this object can be reused.
    failed_ = true;
    LOG_WARN("failed to insert IVF pre-filter batch", K(ret), K(count));
  }
  return ret;
}

int ObIvfPreFilterBitmap::finish()
{
  int ret = OB_SUCCESS;
  if (failed_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (get_cardinality() > MAX_BITMAP_SET_VALUES) {
    const uint64_t required = (max_vid_ - min_vid_ / 64 * 64) / 64 * 64 + 64;
    if (required > MAX_COMPACT_BITS || get_cardinality() < required / 128) {
      ret = convert_to_roaring();
    } else if (!is_compact()) {
      ret = reserve(min_vid_, max_vid_, true);
    }
  }
  if (OB_FAIL(ret)) {
    failed_ = true;
    LOG_WARN("failed to finish IVF pre-filter bitmap", K(ret));
  }
  return ret;
}

int ObIvfPreFilterIter::init()
{
  // CRoaring iterator allocation/free also consult the thread-local label.
  // Legacy borrowed bitmaps retain their caller's allocation convention.
  const lib::ObMemAttr attr = nullptr != filter_.get_adaptive_vids()
      ? lib::ObMemAttr(MTL_ID(), "VIBitmapADPV") : lib::ObMallocHookAttrGuard::get_tl_mem_attr();
  lib::ObMallocHookAttrGuard malloc_guard(attr);
  int ret = OB_SUCCESS;
  if (initialized_) {
    ret = OB_INIT_TWICE;
  } else if (!filter_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    initialized_ = true;
    if (is_compact()) {
      ret = get_next();
    } else if (!filter_.get_allowed_vids()->is_bitmap_type()) {
      common::ObRoaringBitmap &bitmap = *filter_.get_allowed_vids();
      small_mode_ = true;
      if (bitmap.is_single_type()) {
        small_values_[small_count_++] = bitmap.get_single_value();
      } else if (bitmap.is_set_type()) {
        common::hash::ObHashSet<uint64_t>::const_iterator iter = bitmap.get_set()->begin();
        for (; OB_SUCC(ret) && iter != bitmap.get_set()->end(); ++iter) {
          if (small_count_ >= MAX_BITMAP_SET_VALUES) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            small_values_[small_count_++] = iter->first;
          }
        }
        lib::ob_sort(small_values_, small_values_ + small_count_);
      }
      if (OB_SUCC(ret) && 0 == small_count_) {
        ret = OB_ITER_END;
      }
    } else {
      ret = roaring_iter_.init();
    }
  }
  return ret;
}

int ObIvfPreFilterIter::get_next()
{
  int ret = OB_SUCCESS;
  if (!initialized_) {
    ret = OB_NOT_INIT;
  } else if (small_mode_) {
    if (small_pos_ + 1 >= small_count_) {
      ret = OB_ITER_END;
    } else {
      ++small_pos_;
    }
  } else if (!is_compact()) {
    ret = roaring_iter_.get_next();
  } else {
    const ObIvfPreFilterBitmap &bitmap = *filter_.get_adaptive_vids();
    while (0 == pending_bits_ && word_pos_ < bitmap.get_bit_count() / 64) {
      pending_bits_ = bitmap.get_words()[word_pos_++];
    }
    if (0 == pending_bits_) {
      ret = OB_ITER_END;
    } else {
      current_ = bitmap.get_base_vid() + (word_pos_ - 1) * 64 + __builtin_ctzll(pending_bits_);
      pending_bits_ &= pending_bits_ - 1;
    }
  }
  return ret;
}

int ObIvfPreFilter::get_vid_range(int64_t &min_vid, int64_t &max_vid) const
{
  int ret = OB_SUCCESS;
  min_vid = 0;
  max_vid = INT64_MAX;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_empty()) {
    ret = OB_ITER_END;
  } else if (nullptr != adaptive_vids_) {
    min_vid = static_cast<int64_t>(adaptive_vids_->get_min_vid());
    max_vid = static_cast<int64_t>(adaptive_vids_->get_max_vid());
  } else {
    uint64_t first_vid = UINT64_MAX;
    if (allowed_vids_->is_single_type()) {
      first_vid = allowed_vids_->get_single_value();
    } else if (allowed_vids_->is_set_type()) {
      common::hash::ObHashSet<uint64_t>::const_iterator iter = allowed_vids_->get_set()->begin();
      for (; iter != allowed_vids_->get_set()->end(); ++iter) {
        first_vid = MIN(first_vid, iter->first);
      }
    } else if (allowed_vids_->is_bitmap_type()) {
      first_vid = roaring::api::roaring64_bitmap_minimum(allowed_vids_->get_bitmap());
    }
    if (first_vid > INT64_MAX) {
      ret = OB_ITER_END;
    } else {
      min_vid = static_cast<int64_t>(first_vid);
      max_vid = static_cast<int64_t>(MIN(allowed_vids_->get_max(), static_cast<uint64_t>(INT64_MAX)));
    }
  }
  return ret;
}

void ObIvfPreFilterIter::deinit()
{
  const lib::ObMemAttr attr = nullptr != filter_.get_adaptive_vids()
      ? lib::ObMemAttr(MTL_ID(), "VIBitmapADPV") : lib::ObMallocHookAttrGuard::get_tl_mem_attr();
  lib::ObMallocHookAttrGuard malloc_guard(attr);
  roaring_iter_.deinit();
  word_pos_ = 0;
  pending_bits_ = 0;
  current_ = 0;
  initialized_ = false;
  small_mode_ = false;
  small_count_ = 0;
  small_pos_ = 0;
}

ObIvfTopKCollector::ObIvfTopKCollector()
  : cmp_(), allocator_(nullptr), heap_(nullptr), nodes_(nullptr), buckets_(nullptr),
    k_(0), is_inited_(false)
{
}

ObIvfTopKCollector::~ObIvfTopKCollector()
{
  reset();
}

int ObIvfTopKCollector::init(const int64_t k, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  const int64_t slot_size = sizeof(ObIvfKnnNode) + sizeof(ObIvfKnnNode *);
  if (OB_UNLIKELY(k <= 0 || k > INT64_MAX / slot_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf top-k collector argument", K(ret), K(k));
  } else {
    allocator_ = &allocator;
    void *buf = allocator_->alloc(sizeof(ObIvfKnnHeap));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate ivf top-k collector", K(ret));
    } else {
      heap_ = new (buf) ObIvfKnnHeap(cmp_, allocator_);
      if (OB_ISNULL(nodes_ = static_cast<ObIvfKnnNode *>(allocator_->alloc(k * slot_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate ivf top-k VID lookup", K(ret), K(k));
      } else {
        for (int64_t i = 0; i < k; ++i) {
          new (nodes_ + i) ObIvfKnnNode();
        }
        buckets_ = reinterpret_cast<ObIvfKnnNode **>(nodes_ + k);
        MEMSET(buckets_, 0, k * sizeof(ObIvfKnnNode *));
        k_ = k;
        is_inited_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

ObIvfTopKCollector::ObIvfKnnNode **ObIvfTopKCollector::find_node_link(const int64_t vid) const
{
  const uint64_t bucket = common::murmurhash(&vid, sizeof(vid), 0) % k_;
  ObIvfKnnNode **link = &buckets_[bucket];
  while (nullptr != *link && (*link)->item_.vid_ != vid) {
    link = &(*link)->next_;
  }
  return link;
}

bool ObIvfTopKCollector::contains(const int64_t vid) const
{
  return is_inited_ && vid >= 0 && nullptr != *find_node_link(vid);
}

int ObIvfTopKCollector::push(const int64_t cid, const int64_t vid, const float distance)
{
  int ret = OB_SUCCESS;
  ObIvfKnnEntry item;
  item.cid_ = cid;
  item.vid_ = vid;
  item.distance_ = distance;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf top-k collector is not initialized", K(ret));
  } else if (OB_UNLIKELY(cid < 0 || vid < 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data in ivf top-k collector", K(ret), K(cid), K(vid), K(distance));
  } else if (OB_UNLIKELY(!std::isfinite(distance))) {
    ret = OB_NUMERIC_OVERFLOW;
    LOG_WARN("distance value is overflow", K(ret), K(distance));
  } else if (heap_->count() == k_ && !cmp_(item, heap_->top()->item_)) {
    // A candidate no better than the worst retained entry cannot improve any
    // retained VID either. Reject it before looking up the VID.
  } else {
    ObIvfKnnNode **link = find_node_link(vid);
    ObIvfKnnNode *node = *link;
    if (nullptr != node) {
      if (cmp_(item, node->item_)) {
        // Removing and reinserting an existing node needs no new capacity.
        if (OB_FAIL(heap_->remove(node))) {
          LOG_WARN("failed to remove ivf top-k duplicate for update", K(ret), K(vid));
        } else {
          node->item_ = item;
          ret = heap_->push(node);
        }
      }
    } else if (heap_->count() < k_) {
      node = &nodes_[heap_->count()];
      node->item_ = item;
      if (OB_SUCC(ret = heap_->push(node))) {
        *link = node;
      }
    } else {
      node = heap_->top();
      ObIvfKnnNode **old_link = find_node_link(node->item_.vid_);
      // If the reused node is the tail of this VID's bucket, unlinking it
      // moves the insertion position to the link that previously owned it.
      if (link == &node->next_) {
        link = old_link;
      }
      *old_link = node->next_;
      node->next_ = nullptr;
      node->item_ = item;
      // replace_top() does not record the index for a one-element heap.
      node->heap_idx_ = 0;
      ret = heap_->replace_top(node);
      *link = node;
    }
  }
  return ret;
}

int ObIvfTopKCollector::get_distance_threshold(float &distance) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf top-k collector is not initialized", K(ret));
  } else if (heap_->count() < k_) {
    distance = std::numeric_limits<float>::max();
  } else {
    distance = heap_->top()->item_.distance_;
  }
  return ret;
}

int ObIvfTopKCollector::finish(ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf top-k collector is not initialized", K(ret));
  } else {
    const int64_t item_count = heap_->count();
    ObIvfKnnEntry *items = nullptr;
    if (item_count > 0 && OB_ISNULL(items = static_cast<ObIvfKnnEntry *>(allocator_->alloc(sizeof(ObIvfKnnEntry) * item_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate ivf top-k retained copy", K(ret), K(item_count));
    } else {
      for (int64_t i = 0; i < item_count; ++i) {
        items[i] = heap_->at(i)->item_;
      }
      if (item_count > 1) {
        lib::ob_sort(items, items + item_count,
            [](const ObIvfKnnEntry &lhs, const ObIvfKnnEntry &rhs) {
              return lhs.distance_ < rhs.distance_
                  || (lhs.distance_ == rhs.distance_ && lhs.vid_ < rhs.vid_);
            });
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
        if (OB_FAIL(result.add(items[i]))) {
          LOG_WARN("failed to add ivf top-k result", K(ret), K(i), K(items[i]));
        }
      }
    }
    if (nullptr != items) {
      allocator_->free(items);
    }
  }
  if (OB_FAIL(ret)) {
    result.reset();
  }
  return ret;
}

void ObIvfTopKCollector::reset()
{
  if (nullptr != heap_) {
    heap_->~ObIvfKnnHeap();
    allocator_->free(heap_);
    heap_ = nullptr;
  }
  if (nullptr != nodes_) {
    allocator_->free(nodes_);
    nodes_ = nullptr;
  }
  buckets_ = nullptr;
  allocator_ = nullptr;
  k_ = 0;
  is_inited_ = false;
}

static int init_ivf_output_row_desc(
    const storage::ObITableReadInfo &schema_read_info,
    const ObIvfSSTableRowDesc &storage_row_desc,
    ObIvfSSTableRowDesc &output_row_desc)
{
  int ret = OB_SUCCESS;
  output_row_desc.reset();
  if (OB_UNLIKELY(!schema_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf schema read info", K(ret), K(schema_read_info), K(storage_row_desc));
  } else {
    const storage::ObColumnIndexArray &column_indexes = schema_read_info.get_columns_index();
    for (int64_t i = 0; i < column_indexes.count(); ++i) {
      const int32_t storage_index = column_indexes.at(i);
      if (storage_index == storage_row_desc.row_type_col_idx_) {
        output_row_desc.row_type_col_idx_ = i;
      } else if (storage_index == storage_row_desc.centroid_id_col_idx_) {
        output_row_desc.centroid_id_col_idx_ = i;
      } else if (storage_index == storage_row_desc.vector_id_col_idx_) {
        output_row_desc.vector_id_col_idx_ = i;
      } else if (storage_index == storage_row_desc.payload_col_idx_) {
        output_row_desc.payload_col_idx_ = i;
      }
    }
    if (OB_UNLIKELY(!output_row_desc.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ivf read info does not contain all required output columns",
          K(ret), K(column_indexes), K(storage_row_desc), K(output_row_desc));
    }
  }
  return ret;
}

ObIvfSSTableSearcher::ObIvfSSTableSearcher(common::ObIAllocator &allocator)
  : query_arena_(ObMemAttr(MTL_ID(), "IVFKnnQuery")),
    query_vector_ctx_(),
    centroid_arena_(ObMemAttr(MTL_ID(), "IVFKnnCidIter")),
    search_arena_(ObMemAttr(MTL_ID(), "IVFKnnRound")),
    allocator_(allocator),
    table_handle_(),
    sstable_(nullptr),
    access_ctx_(nullptr),
    table_access_param_(),
    output_row_desc_(),
    centroid_iterator_(nullptr),
    is_inited_(false)
{
}

ObIvfSSTableSearcher::~ObIvfSSTableSearcher()
{
  release_centroid_iterator();
}

void ObIvfSSTableSearcher::release_centroid_iterator()
{
  if (nullptr != centroid_iterator_) {
    centroid_iterator_->~ObIvfCentroidIterator();
    allocator_.free(centroid_iterator_);
    centroid_iterator_ = nullptr;
  }
  centroid_arena_.reuse();
}

void ObIvfSSTableSearcher::reuse_round()
{
  search_arena_.reuse();
}

void ObIvfSSTableSearcher::reuse()
{
  do_reuse();
  release_centroid_iterator();
  reuse_round();
}

void ObIvfSSTableSearcher::close_sstable()
{
  do_close_sstable();
  reuse();
  sstable_ = nullptr;
  table_handle_.reset();
  output_row_desc_.reset();
}

void ObIvfSSTableSearcher::destroy(ObIvfSSTableSearcher *searcher)
{
  if (nullptr != searcher) {
    common::ObIAllocator *object_allocator = &searcher->allocator_;
    OB_DELETEx(ObIvfSSTableSearcher, object_allocator, searcher);
  }
}

int ObIvfSSTableSearcher::init_common(
    const storage::ObTableHandleV2 &table_handle,
    const storage::ObTableScanParam &scan_param,
    storage::ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ivf sstable searcher is already initialized", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(scan_param.allocator_)
      || OB_ISNULL(scan_param.table_param_)
      || scan_param.allocator_ != &allocator_
      || !access_ctx.is_valid()
      || !table_handle.is_valid()
      || scan_param.allocator_ != access_ctx.stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument in ivf sstable searcher", K(ret), K(scan_param), K(access_ctx));
  } else {
    const storage::ObITableReadInfo &schema_read_info = scan_param.table_param_->get_read_info();
    table_access_param_.reset();
    if (OB_FAIL(table_access_param_.init(scan_param, nullptr, &schema_read_info))) {
      LOG_WARN("failed to initialize ivf table access param", K(ret), K(scan_param));
    } else {
      table_access_param_.iter_param_.rowkey_read_info_ = nullptr;
      table_access_param_.iter_param_.is_same_schema_column_ = false;
      access_ctx_ = &access_ctx;
      if (OB_FAIL(switch_sstable(table_handle))) {
        LOG_WARN("failed to open first ivf sstable", K(ret), K(table_handle));
      } else {
        is_inited_ = true;
      }
    }
  }
  if (OB_FAIL(ret) && !is_inited_) {
    close_sstable();
    access_ctx_ = nullptr;
    table_access_param_.reset();
    is_inited_ = false;
  }
  return ret;
}

int ObIvfSSTableSearcher::switch_sstable(const storage::ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  bool opened_new_sstable = false;
  storage::ObTableHandleV2 next_table_handle;
  ObSSTable *base_sstable = nullptr;
  ObIvfSSTable *ivf_sstable = nullptr;
  ObIvfSSTableRowDesc storage_row_desc;
  ObIvfSSTableRowDesc new_output_row_desc;
  if (OB_UNLIKELY(nullptr == access_ctx_ || !table_access_param_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable searcher is not initialized", K(ret));
  } else if (OB_UNLIKELY(!access_ctx_->is_valid() || !table_handle.is_valid()
      || nullptr == table_access_param_.iter_param_.read_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf sstable switch argument", K(ret), K_(table_access_param),
        KPC_(access_ctx), K(table_handle));
  } else {
    next_table_handle = table_handle;
    if (OB_FAIL(next_table_handle.get_sstable(base_sstable))) {
      LOG_WARN("failed to get sstable from table handle", K(ret), K(table_handle));
    } else if (OB_ISNULL(base_sstable) || OB_UNLIKELY(!base_sstable->is_ivf_sstable())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid non-ivf sstable handle", K(ret), KP(base_sstable), K(table_handle));
    } else {
      ivf_sstable = static_cast<ObIvfSSTable *>(base_sstable);
      const bool is_empty = ivf_sstable->get_ivf_meta().is_empty() || ivf_sstable->no_data_to_read();
      if (OB_UNLIKELY(!ivf_sstable->is_valid())) {
        ret = OB_NOT_INIT;
        LOG_WARN("ivf sstable is not initialized", K(ret), KPC(ivf_sstable));
      } else if (OB_UNLIKELY(access_ctx_->tablet_id_ != ivf_sstable->get_key().get_tablet_id())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet id for ivf sstable", K(ret), KPC_(access_ctx), K(ivf_sstable->get_key()));
      } else if (!is_empty && OB_UNLIKELY(!ivf_sstable->get_ivf_meta().is_valid())) {
        ret = OB_NOT_INIT;
        LOG_WARN("ivf sstable metadata is not initialized", K(ret), KPC(ivf_sstable));
      } else if (!is_empty && OB_FAIL(ivf_sstable->get_row_desc(storage_row_desc))) {
        LOG_WARN("failed to initialize ivf sstable row descriptor", K(ret), K(ivf_sstable->get_ivf_meta()));
      } else if (!is_empty && OB_FAIL(init_ivf_output_row_desc(
          *table_access_param_.iter_param_.read_info_, storage_row_desc, new_output_row_desc))) {
        LOG_WARN("failed to initialize ivf output row descriptor", K(ret), K_(table_access_param));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (nullptr != sstable_) {
        close_sstable();
      }
      table_handle_ = next_table_handle;
      sstable_ = ivf_sstable;
      output_row_desc_ = new_output_row_desc;
      opened_new_sstable = true;
      if (OB_FAIL(do_open_sstable())) {
        LOG_WARN("failed to bind algorithm-specific ivf sstable state", K(ret), K(table_handle));
      }
    }
  }
  if (OB_FAIL(ret) && opened_new_sstable) {
    close_sstable();
  }
  return ret;
}

int ObIvfSSTableSearcher::create_centroid_iterator(
    const ObIvfKnnSearchParam &query_param,
    const float *rotated_query, const bool iterative, const int64_t child_budget)
{
  int ret = OB_SUCCESS;
  ObIvfCentroidIterator *new_iterator = nullptr;
  ObIvfKnnSearchParam all_centroids_param = query_param;
  // Only iterative FLAT searches need the complete ordered center sequence.
  // One-shot searches retain Top-nprobe; GROUPED applies the same mode to each
  // batch of expanded groups and keeps their owners alive while referenced.
  all_centroids_param.iterative_centroids_ = iterative;
  all_centroids_param.skip_empty_centroids_ = true;
  all_centroids_param.centroid_child_budget_ = child_budget;
  if (child_budget > 0) { all_centroids_param.nprobe_ = 1; }
  if (iterative && sstable_ != nullptr && !sstable_->get_ivf_meta().is_hierarchical()) {
    all_centroids_param.nprobe_ = INT64_MAX;
  }
  if (OB_UNLIKELY(!is_inited_ || nullptr == sstable_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable searcher is not initialized", K(ret));
  } else if (OB_UNLIKELY(nullptr == access_ctx_ || !access_ctx_->is_valid())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ivf table access context is unavailable", K(ret), KP(access_ctx_));
  } else if (OB_UNLIKELY(nullptr != centroid_iterator_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("centroid iterator is already created", K(ret));
  } else if (OB_FAIL(sstable_->create_centroid_iterator(
      all_centroids_param, rotated_query, allocator_, centroid_arena_,
      iter_param(), output_row_desc_, *access_ctx_, new_iterator,
      query_vector_ctx_.is_inited() ? &query_vector_ctx_ : nullptr))) {
    LOG_WARN("failed to create centroid iterator", K(ret));
  } else {
    centroid_iterator_ = new_iterator;
  }
  return ret;
}

bool ObIvfSSTableSearcher::has_remaining_centroids() const
{
  return nullptr != centroid_iterator_
      && centroid_iterator_->has_remaining_centroids();
}

int ObIvfSSTableSearcher::check_query_metadata(const ObIvfKnnSearchParam &query_param) const
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable_->get_ivf_meta();
  if (!meta.is_empty()
      && (query_param.dim_ != meta.dim_
          || (query_param.expected_dist_type_ >= 0 && query_param.expected_dist_type_ != meta.dist_type_))) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObIvfSSTableSearcher::knn_search(
    const ObIvfKnnSearchParam &query_param,
    ObIvfKnnSearchResult &result,
    const bool quantized_only)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  result.reset();
  reuse_round();
  if (OB_UNLIKELY(!is_inited_ || nullptr == sstable_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable searcher is not initialized", K(ret));
  } else if (OB_UNLIKELY(!query_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf knn query param", K(ret), K(query_param));
  } else if (OB_FAIL(check_query_metadata(query_param))) {
    LOG_WARN("IVF query and segment metadata disagree", K(ret), K(query_param),
             K(sstable_->get_ivf_meta()));
  } else if (OB_FAIL(collector.init(query_param.k_, search_allocator()))) {
    LOG_WARN("failed to initialize ivf knn top-k collector", K(ret), K(query_param));
  } else if (OB_FAIL(quantized_only ? do_quantized_search(query_param, collector)
                                  : do_knn_search(query_param, collector))) {
    LOG_WARN("failed to execute ivf knn search", K(ret), K(query_param));
  } else if (OB_FAIL(collector.finish(result))) {
    LOG_WARN("failed to finish ivf knn top-k collector", K(ret), K(query_param));
  }
  if (OB_FAIL(ret)) {
    result.reset();
  }
  return ret;
}

int ObIvfSSTableSearcher::refine_candidates(const ObIvfKnnSearchParam &query_param,
    const ObIvfKnnSearchResult &candidates, ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  result.reset();
  reuse_round();
  if (OB_UNLIKELY(!is_inited_ || nullptr == sstable_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!query_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(check_query_metadata(query_param))) {
    LOG_WARN("invalid IVF refinement query metadata", K(ret), K(query_param));
  } else if (candidates.count() == 0) {
  } else if (OB_FAIL(collector.init(query_param.k_, search_allocator()))) {
    LOG_WARN("failed to initialize filtered IVF refinement collector", K(ret));
  } else if (OB_FAIL(do_refine_candidates(query_param, candidates, collector))) {
    LOG_WARN("failed to refine filtered IVF candidates", K(ret));
  } else if (OB_FAIL(collector.finish(result))) {
    LOG_WARN("failed to finish filtered IVF refinement", K(ret));
  }
  if (OB_FAIL(ret)) { result.reset(); }
  return ret;
}

int ObIvfSSTableSearcher::next_candidate_batch(const ObIvfKnnSearchParam &query_param,
    float distance_threshold, ObIvfKnnSearchResult &result,
    ObIvfCandidateProgress &progress, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  result.reset();
  progress.reset();
  reuse_round();
  if (!is_inited_ || nullptr == sstable_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF candidate searcher is not initialized", K(ret), K_(is_inited), KP_(sstable));
  } else if (!query_param.is_valid() || distance_threshold != distance_threshold
      || batch_size < 32 || batch_size > ObIvfCandidateProgress::MAX_BATCH_SIZE
      || 0 != batch_size % 32) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF candidate batch request", K(ret), K(query_param),
             K(distance_threshold), K(batch_size));
  } else if (OB_FAIL(check_query_metadata(query_param))) {
    LOG_WARN("IVF candidate query and segment metadata disagree", K(ret), K(query_param),
             K(sstable_->get_ivf_meta()));
  } else if (OB_FAIL(collector.init(batch_size, search_allocator()))) {
    LOG_WARN("failed to initialize IVF candidate batch", K(ret));
  } else if (OB_FAIL(do_next_candidate_batch(query_param, distance_threshold,
      batch_size, collector, progress))) {
    LOG_WARN("failed to read IVF candidate batch", K(ret), K(progress));
  } else if (OB_FAIL(collector.finish(result))) {
    LOG_WARN("failed to copy IVF candidate batch", K(ret));
  }
  if (OB_FAIL(ret)) {
    result.reset();
    progress.reset();
    // The caller must reuse/reopen after an error; discard any borrowed scan state.
    do_reuse();
  }
  return ret;
}

int ObIvfSSTableSearcher::brute_force_search(
    const ObIvfKnnSearchParam &query_param,
    ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  result.reset();
  reuse_round();
  if (OB_UNLIKELY(!is_inited_ || nullptr == sstable_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable searcher is not initialized", K(ret));
  } else if (OB_UNLIKELY(!query_param.is_brute_force_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf brute-force query param", K(ret), K(query_param));
  } else if (query_param.expected_dist_type_ < 0 && query_param.pre_filter_->is_empty()) {
    // Legacy callers skip query preparation for a proven empty allowed set.
    // Plan-bound callers still validate the dimension and metric below.
  } else if (OB_FAIL(check_query_metadata(query_param))) {
    LOG_WARN("IVF query and segment metadata disagree", K(ret), K(query_param),
             K(sstable_->get_ivf_meta()));
  } else if (OB_FAIL(collector.init(query_param.k_, search_allocator()))) {
    LOG_WARN("failed to initialize ivf brute-force top-k collector", K(ret), K(query_param));
  } else if (OB_FAIL(do_brute_force_search(query_param, collector))) {
    LOG_WARN("failed to execute ivf brute-force search", K(ret), K(query_param));
  } else if (OB_FAIL(collector.finish(result))) {
    LOG_WARN("failed to finish ivf brute-force top-k collector", K(ret), K(query_param));
  }
  if (OB_FAIL(ret)) {
    result.reset();
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

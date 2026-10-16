/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASBitmapOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_) && OB_ISNULL(provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap child and provider are both null", K(ret));
  } else if (OB_NOT_NULL(child_) && OB_NOT_NULL(provider_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bitmap child and provider are mutually exclusive", K(ret));
  } else if (OB_NOT_NULL(child_) && OB_FAIL(children.push_back(child_))) {
    LOG_WARN("failed to push child op", KR(ret), KPC(child_));
  }
  return ret;
}

ObFastBitmap::~ObFastBitmap()
{
  reset();
}

void ObFastBitmap::reset()
{
  chunks_.reset();
  cardinality_ = 0;
  stats_dirty_ = false;
  if (chunk_buf_ != nullptr) {
    allocator_.free(chunk_buf_);
    chunk_buf_ = nullptr;
    chunk_buf_count_ = 0;
    chunk_buf_idx_ = 0;
  }
  info_allocator_.reset();
}

void ObFastBitmap::reuse()
{
  chunks_.reuse();
  cardinality_ = 0;
  stats_dirty_ = false;
  if (chunk_buf_ != nullptr) {
    chunk_buf_idx_ = 0;
  }
  info_allocator_.reset();
}

int ObFastBitmap::init(int64_t max_value_count)
{
  int ret = common::OB_SUCCESS;
  chunks_.set_attr(common::ObMemAttr(MTL_ID(), "FBitmapChunks"));
  if (max_value_count < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("max_value_count is less than 0", K(ret), K(max_value_count));
  } else {
    int64_t required_chunks = max_value_count / CHUNK_SIZE + 1;
    if (OB_FAIL(chunks_.prepare_allocate(required_chunks))) {
      LOG_WARN("failed to prepare allocate chunks", K(ret), K(required_chunks));
    } else {
      int64_t max_chunks = MAX_BITMAP_RESERVED_MEM_SIZE / sizeof(ChunkInfo);
      int64_t alloc_chunks = required_chunks > max_chunks ? max_chunks : required_chunks;
      int64_t total_size = alloc_chunks * sizeof(ChunkInfo);
      if (OB_ISNULL(chunk_buf_ = static_cast<ChunkInfo*>(allocator_.alloc(total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for bitmap", K(ret), K(total_size));
      } else {
        chunk_buf_idx_ = 0;
        chunk_buf_count_ = alloc_chunks;
      }
    }
  }
  return ret;
}

int ObFastBitmap::alloc_chunk(uint64_t chunk_idx, ChunkInfo *&chunk_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(chunk_idx >= static_cast<uint64_t>(chunks_.count()))) {
    int64_t new_count = chunk_idx + 1;
    if (OB_FAIL(chunks_.prepare_allocate(new_count))) {
      LOG_WARN("failed to prepare allocate chunks", K(ret), K(new_count));
    }
  }

  if (OB_SUCC(ret)) {
    ChunkInfo *&chunk_ptr = chunks_.at(chunk_idx);
    if (chunk_ptr == nullptr) {
      if (chunk_buf_idx_ < chunk_buf_count_) {
        chunk_ptr = new (&chunk_buf_[chunk_buf_idx_++]) ChunkInfo();
      } else {
        void *info_buf = info_allocator_.alloc(sizeof(ChunkInfo));
        if (info_buf == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for chunk info", K(ret));
        } else {
          chunk_ptr = new (info_buf) ChunkInfo();
        }
      }
    }
    if (OB_SUCC(ret)) {
      chunk_info = chunk_ptr;
    }
  }
  return ret;
}

int ObFastBitmap::add(uint64_t val)
{
  int ret = OB_SUCCESS;
  const uint64_t chunk_idx = val >> CHUNK_SHIFT;
  const uint16_t chunk_offset = val & CHUNK_MASK;
  ChunkInfo *chunk_info = nullptr;

  if (OB_LIKELY(chunk_idx < static_cast<uint64_t>(chunks_.count()))) {
    chunk_info = chunks_.at(chunk_idx);
  }

  if (OB_UNLIKELY(nullptr == chunk_info)) {
    if (OB_FAIL(alloc_chunk(chunk_idx, chunk_info))) {
      LOG_WARN("failed to alloc chunk", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t word_idx = chunk_offset >> WORD_SHIFT;
    const uint64_t bit_mask = 1ULL << (chunk_offset & (WORD_BITS - 1));
    uint64_t &target_word = chunk_info->chunk_[word_idx];
    if (OB_LIKELY(!(target_word & bit_mask))) {
      target_word |= bit_mask;
      if (chunk_offset < chunk_info->min_val_) chunk_info->min_val_ = chunk_offset;
      if (chunk_offset > chunk_info->max_val_) chunk_info->max_val_ = chunk_offset;
      cardinality_++;
    }
  }
  return ret;
}

int ObFastBitmap::add_batch(const uint64_t *vals, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count < 0 || OB_ISNULL(vals))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch values", K(ret), KP(vals), K(count));
  } else if (count > 0) {
    uint64_t max_val = vals[0];
    int64_t same_word_count = 0;
    for (int64_t i = 1; i < count; ++i) {
      max_val = std::max(max_val, vals[i]);
      same_word_count += ((vals[i - 1] >> WORD_SHIFT) == (vals[i] >> WORD_SHIFT));
    }
    const uint64_t max_chunk_idx = max_val >> CHUNK_SHIFT;
    if (OB_SUCC(ret) && max_chunk_idx >= static_cast<uint64_t>(chunks_.count())) {
      const int64_t new_chunk_count = static_cast<int64_t>(max_chunk_idx + 1);
      if (OB_FAIL(chunks_.prepare_allocate(new_chunk_count))) {
        LOG_WARN("failed to prepare chunk pointers for batch", K(ret), K(new_chunk_count));
      }
    }

    // Count adjacent IDs in the same word during the max-value scan, so the
    // insertion path reflects the whole batch without an additional pass.
    // Short batches retain the grouped path.
    const bool aggregate_words = count < 32 || same_word_count >= count / 2;
    if (OB_SUCC(ret)) {
      if (!aggregate_words) {
        int64_t i = 0;
        while (OB_SUCC(ret) && i < count) {
          const uint64_t chunk_idx = vals[i] >> CHUNK_SHIFT;
          ChunkInfo *chunk_info = chunks_.at(static_cast<int64_t>(chunk_idx));
          if (OB_ISNULL(chunk_info) && OB_FAIL(alloc_chunk(chunk_idx, chunk_info))) {
            LOG_WARN("failed to allocate chunk for sparse batch", K(ret), K(i), K(chunk_idx));
          } else {
            while (i < count && (vals[i] >> CHUNK_SHIFT) == chunk_idx) {
              chunk_info->chunk_[(vals[i] >> WORD_SHIFT) & (WORDS_PER_CHUNK - 1)]
                  |= 1ULL << (vals[i] & (WORD_BITS - 1));
              ++i;
            }
          }
        }
      } else {
        int64_t i = 0;
        while (OB_SUCC(ret) && i < count) {
          const uint64_t chunk_idx = vals[i] >> CHUNK_SHIFT;
          ChunkInfo *chunk_info = chunks_.at(static_cast<int64_t>(chunk_idx));
          if (OB_ISNULL(chunk_info) && OB_FAIL(alloc_chunk(chunk_idx, chunk_info))) {
            LOG_WARN("failed to allocate chunk for batch", K(ret), K(i), K(chunk_idx));
          } else {
            while (i < count && (vals[i] >> CHUNK_SHIFT) == chunk_idx) {
              const uint64_t global_word_idx = vals[i] >> WORD_SHIFT;
              uint64_t pending_mask = 0;
              while (i < count && (vals[i] >> WORD_SHIFT) == global_word_idx) {
                pending_mask |= 1ULL << (vals[i] & (WORD_BITS - 1));
                ++i;
              }
              chunk_info->chunk_[global_word_idx & (WORDS_PER_CHUNK - 1)] |= pending_mask;
            }
          }
        }
      }
      stats_dirty_ = true;
    }
  }
  return ret;
}

void ObFastBitmap::finalize()
{
  if (stats_dirty_) {
    int64_t new_cardinality = 0;
    for (int64_t chunk_idx = 0; chunk_idx < chunks_.count(); ++chunk_idx) {
      ChunkInfo *chunk_info = chunks_.at(chunk_idx);
      if (OB_NOT_NULL(chunk_info)) {
        bool found = false;
        uint16_t min_val = UINT16_MAX;
        uint16_t max_val = 0;
        int64_t last_word_idx = -1;
        uint64_t last_word = 0;
        for (int64_t word_idx = 0; word_idx < WORDS_PER_CHUNK; ++word_idx) {
          const uint64_t word = chunk_info->chunk_[word_idx];
          if (word != 0) {
            new_cardinality += __builtin_popcountll(word);
            if (!found) {
              min_val = static_cast<uint16_t>((word_idx << WORD_SHIFT) + __builtin_ctzll(word));
              found = true;
            }
            last_word_idx = word_idx;
            last_word = word;
          }
        }
        if (found) {
          max_val = static_cast<uint16_t>(
            (last_word_idx << WORD_SHIFT) + WORD_BITS - 1 - __builtin_clzll(last_word));
          chunk_info->min_val_ = min_val;
          chunk_info->max_val_ = max_val;
        }
      }
    }
    cardinality_ = new_cardinality;
    stats_dirty_ = false;
  }
}

uint64_t ObFastBitmap::get_word(const uint64_t global_word_idx) const
{
  const uint64_t chunk_idx = global_word_idx >> (CHUNK_SHIFT - WORD_SHIFT);
  const uint64_t word_idx = global_word_idx & (WORDS_PER_CHUNK - 1);
  const ChunkInfo *chunk_info = chunk_idx < static_cast<uint64_t>(chunks_.count())
      ? chunks_.at(static_cast<int64_t>(chunk_idx))
      : nullptr;
  return OB_NOT_NULL(chunk_info) ? chunk_info->chunk_[word_idx] : 0;
}

int ObFastBitmap::copy_range_to_words(
    const uint64_t range_begin,
    const uint64_t range_end,
    const uint64_t window_begin,
    const int64_t window_capacity,
    uint64_t *dst_words,
    int64_t &copied_count) const
{
  int ret = OB_SUCCESS;
  copied_count = 0;
  if (OB_UNLIKELY(stats_dirty_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot copy an unfinalized bitmap", K(ret));
  } else if (OB_UNLIKELY(window_capacity <= 0 || OB_ISNULL(dst_words))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap destination window", K(ret), K(window_capacity), KP(dst_words));
  } else if (OB_UNLIKELY(range_begin < window_begin
                         || range_end < range_begin
                         || range_end - window_begin >= static_cast<uint64_t>(window_capacity))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap copy range", K(ret), K(range_begin), K(range_end),
             K(window_begin), K(window_capacity));
  } else {
    const int64_t dst_word_count = (window_capacity + WORD_BITS - 1) >> WORD_SHIFT;
    MEMSET(dst_words, 0, sizeof(uint64_t) * dst_word_count);
    const uint64_t first_offset = range_begin - window_begin;
    const uint64_t last_offset = range_end - window_begin;
    const uint64_t first_dst_word = first_offset >> WORD_SHIFT;
    const uint64_t last_dst_word = last_offset >> WORD_SHIFT;
    for (uint64_t dst_word_idx = first_dst_word;
         dst_word_idx <= last_dst_word;
         ++dst_word_idx) {
      const uint64_t global_bit_begin = window_begin + (dst_word_idx << WORD_SHIFT);
      const uint64_t src_word_idx = global_bit_begin >> WORD_SHIFT;
      const uint64_t src_shift = global_bit_begin & (WORD_BITS - 1);
      uint64_t word = get_word(src_word_idx);
      if (src_shift > 0) {
        word = (word >> src_shift)
            | (get_word(src_word_idx + 1) << (WORD_BITS - src_shift));
      }
      if (dst_word_idx == first_dst_word) {
        const uint64_t first_bit = first_offset & (WORD_BITS - 1);
        word &= (~0ULL << first_bit);
      }
      if (dst_word_idx == last_dst_word) {
        const uint64_t last_bit = last_offset & (WORD_BITS - 1);
        const uint64_t tail_mask = last_bit == WORD_BITS - 1
            ? ~0ULL : ((1ULL << (last_bit + 1)) - 1);
        word &= tail_mask;
      }
      dst_words[dst_word_idx] = word;
      copied_count += __builtin_popcountll(word);
    }
  }
  return ret;
}

int ObFastBitmap::Iterator::init(const ObFastBitmap *bm)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bm) || OB_UNLIKELY(bm->stats_dirty_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap", K(ret), KP(bm));
  } else {
    bm_ = bm;
    cur_val_ = -1;
  }
  return ret;
}

void ObFastBitmap::Iterator::reuse()
{
  cur_val_ = -1;
}

void ObFastBitmap::Iterator::reset()
{
  bm_ = nullptr;
  cur_val_ = -1;
}

int ObFastBitmap::Iterator::next_id(uint64_t &val)
{
  return advance_to(cur_val_ + 1, val);
}

int ObFastBitmap::Iterator::advance_to(const uint64_t target, uint64_t &val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bitmap", K(ret), KP(bm_));
  } else {
    int64_t start_chunk = target >> ObFastBitmap::CHUNK_SHIFT;
    uint16_t target_chunk_offset = target & ObFastBitmap::CHUNK_MASK;

    if (cur_val_ != -1 && target <= cur_val_) {
      val = cur_val_;
    } else {
      // * Loop chunks starting from start_chunk.
      // * If it's the start_chunk:
      //    a. If target > max_val, skip (out of range).
      //    b. If target <= min_val, found min_val immediately.
      //    c. Otherwise, search for the bit within this chunk.
      // * If it's a subsequent chunk, found min_val immediately.
      bool found = false;
      int64_t iter_chunk_idx = start_chunk;

      for (; !found && iter_chunk_idx < bm_->chunks_.count(); ++iter_chunk_idx) {
        const ChunkInfo* chunk_info = bm_->chunks_.at(iter_chunk_idx);

        if (chunk_info == nullptr) {
          // skip
        } else {
          if (iter_chunk_idx == start_chunk) {
            // process the start chunk
            if (target_chunk_offset > chunk_info->max_val_) {
              // target is beyond this chunk's max value, skip to the next chunk
            } else if (target_chunk_offset <= chunk_info->min_val_) {
              // target is smaller than or equal to min_val, return min_val
              val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + chunk_info->min_val_;
              cur_val_ = val;
              found = true;
            } else {
              // target is within this chunk's range, search for the next valid value
              // start from the target offset
              int64_t start_word = target_chunk_offset >> ObFastBitmap::WORD_SHIFT;
              int64_t start_bit = target_chunk_offset & (ObFastBitmap::WORD_BITS - 1);

              for (int64_t iter_word_idx = start_word; OB_SUCC(ret) && !found && iter_word_idx < ObFastBitmap::WORDS_PER_CHUNK; ++iter_word_idx) {
                uint64_t word = chunk_info->chunk_[iter_word_idx];
                if (iter_word_idx == start_word && start_bit > 0) {
                  word &= ((~0ULL) << start_bit);
                }

                if (word != 0) {
                  int bit = __builtin_ctzll(word);
                  uint64_t current_found_val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + (iter_word_idx << ObFastBitmap::WORD_SHIFT) + bit;

                  if (OB_UNLIKELY((current_found_val & ObFastBitmap::CHUNK_MASK) > chunk_info->max_val_)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("current found value is greater than chunk max value", K(ret), K(current_found_val), K(chunk_info->max_val_));
                  } else {
                    val = current_found_val;
                    cur_val_ = val;
                    found = true;
                  }
                }
              }
            }
          } else {
            // iter_chunk_idx > start_chunk
            // the minimum value of the next valid chunk is the desired value
            val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + chunk_info->min_val_;
            cur_val_ = val;
            found = true;
          }
        }
      }

      if (OB_SUCC(ret) && !found) {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObFastBitmap::Iterator::drain_until(
    const uint64_t boundary,
    const int64_t capacity,
    uint64_t *out_ids,
    int64_t &count,
    uint64_t &next_id_after,
    DrainState &state)
{
  int ret = OB_SUCCESS;
  int64_t drained_count = 0;
  count = 0;
  next_id_after = 0;
  state = DRAIN_ITER_END;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bitmap", K(ret), KP(bm_));
  } else if (OB_UNLIKELY(capacity <= 0 || OB_ISNULL(out_ids))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap drain arguments", K(ret), K(capacity), KP(out_ids));
  } else {
    const uint64_t start_val = cur_val_ + 1;
    const uint64_t start_chunk_idx = start_val >> ObFastBitmap::CHUNK_SHIFT;
    const uint64_t chunk_count = static_cast<uint64_t>(bm_->chunks_.count());
    bool scan_finished = false;

    for (uint64_t chunk_idx = start_chunk_idx;
         OB_SUCC(ret) && !scan_finished && chunk_idx < chunk_count;
         ++chunk_idx) {
      const ChunkInfo *chunk_info = bm_->chunks_.at(static_cast<int64_t>(chunk_idx));
      if (nullptr == chunk_info) {
        // Sparse chunk array: skip holes without touching individual rowids.
      } else {
        const uint64_t min_offset = static_cast<uint64_t>(chunk_info->min_val_);
        const uint64_t chunk_start = chunk_idx == start_chunk_idx
            ? std::max(start_val & ObFastBitmap::CHUNK_MASK, min_offset)
            : min_offset;
        if (chunk_start <= chunk_info->max_val_) {
          const int64_t start_word_idx = chunk_start >> ObFastBitmap::WORD_SHIFT;
          const int64_t end_word_idx =
              static_cast<int64_t>(chunk_info->max_val_) >> ObFastBitmap::WORD_SHIFT;
          const int64_t start_bit_idx =
              static_cast<int64_t>(chunk_start & (ObFastBitmap::WORD_BITS - 1));
          const uint64_t chunk_base = chunk_idx << ObFastBitmap::CHUNK_SHIFT;

          for (int64_t word_idx = start_word_idx;
               !scan_finished && word_idx <= end_word_idx;
               ++word_idx) {
            uint64_t word = chunk_info->chunk_[word_idx];
            if (word_idx == start_word_idx && start_bit_idx > 0) {
              word &= (~0ULL << start_bit_idx);
            }
            const uint64_t val_base =
                chunk_base + (static_cast<uint64_t>(word_idx) << ObFastBitmap::WORD_SHIFT);
            while (!scan_finished && 0 != word) {
              const uint64_t bit_idx = static_cast<uint64_t>(__builtin_ctzll(word));
              const uint64_t val = val_base + bit_idx;
              if (val > boundary) {
                cur_val_ = val;
                next_id_after = val;
                state = DRAIN_BOUNDARY_REACHED;
                scan_finished = true;
              } else {
                out_ids[drained_count++] = val;
                cur_val_ = val;
                if (drained_count == capacity) {
                  state = DRAIN_BATCH_FULL;
                  scan_finished = true;
                } else {
                  word &= word - 1;
                }
              }
            }
          }
        }
      }
    }
  }
  count = drained_count;
  return ret;
}

int ObDASBitmapOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASBitmapOpParam &bitmap_op_param = static_cast<const ObDASBitmapOpParam &>(op_param);
  const common::ObIArray<ObExpr *> *rowid_output_exprs = nullptr;
  if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowid type", K(ret), K(get_rowid_type()));
  } else if (OB_NOT_NULL(bitmap_op_param.get_provider())) {
    provider_ = bitmap_op_param.get_provider();
    boost_weight_ = 0.0;
  } else if (OB_ISNULL(bitmap_op_param.get_child())
             || OB_ISNULL(bitmap_op_param.get_scalar_ctdef())
             || OB_ISNULL(bitmap_op_param.get_scalar_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bitmap input or metadata", K(ret),
        KP(bitmap_op_param.get_child()), KP(bitmap_op_param.get_scalar_ctdef()),
        KP(bitmap_op_param.get_scalar_rtdef()));
  } else if (FALSE_IT(input_op_ = bitmap_op_param.get_child())) {
  } else if (OB_ISNULL(rowid_output_exprs = input_op_->get_rowid_output_exprs())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("bitmap input has no RowID output expressions", K(ret),
        K(input_op_->get_op_type()));
  } else if (OB_UNLIKELY(1 != rowid_output_exprs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected bitmap RowID output expression count", K(ret),
        K(rowid_output_exprs->count()), K(input_op_->get_scope()));
  } else if (OB_ISNULL(rowid_expr_ = rowid_output_exprs->at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bitmap RowID output expression", K(ret));
  } else {
    boost_weight_ = bitmap_op_param.get_scalar_rtdef()->get_boost_weight();
  }
  return ret;
}

int ObDASBitmapOp::set_external_bitmap(ObFastBitmap *bm)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bm)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("external bitmap is null", K(ret));
  } else if (OB_UNLIKELY(bitmap_built_ || owns_bitmap_ || OB_NOT_NULL(bitmap_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set_external_bitmap called after bitmap already initialized",
             K(ret), K(bitmap_built_), K(owns_bitmap_), KP(bitmap_));
  } else {
    bitmap_ = bm;
    owns_bitmap_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_open()
{
  int ret = OB_SUCCESS;
  has_pending_first_ = false;
  if (OB_NOT_NULL(provider_)) {
    bitmap_ = provider_->get_bitmap();
    if (OB_ISNULL(bitmap_)) {
      exhausted_ = true;
      bitmap_built_ = false;
      owns_bitmap_ = false;
      bitmap_iter_.reset();
    } else if (OB_FAIL(init_provider_bitmap_view())) {
      LOG_WARN("failed to initialize filter bitmap view", K(ret),
               "provider_id", provider_->get_provider_id());
    }
  } else if (OB_NOT_NULL(bitmap_) && !owns_bitmap_) {
    // External bitmap was injected via set_external_bitmap(): skip scan.
    if (OB_FAIL(bitmap_iter_.init(bitmap_))) {
      LOG_WARN("failed to init bitmap iterator on external bitmap", K(ret));
    } else {
      exhausted_ = (bitmap_->cardinality() == 0);
      bitmap_built_ = true;
      if (OB_FAIL(seek_iter_to_lo())) {
        LOG_WARN("failed to seek iter to docid_range_lo", K(ret));
      }
    }
  } else if (OB_ISNULL(input_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(input_op_->open())) {
    LOG_WARN("failed to open bitmap input op", K(ret));
  } else if (FALSE_IT(owns_bitmap_ = true)) {
    // Mark ownership early so do_close() closes input_op_ even if bitmap
    // allocation or init fails afterwards.
  } else if (OB_ISNULL(bitmap_ = OB_NEWx(ObFastBitmap, &ctx_allocator(), ctx_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate fast bitmap", K(ret));
  } else if (OB_FAIL(bitmap_->init(search_ctx_.get_row_count().cost()))) {
    LOG_WARN("failed to init bitmap", K(ret));
  } else if (OB_FAIL(bitmap_iter_.init(bitmap_))) {
    LOG_WARN("failed to init bitmap iterator", K(ret));
  } else {
    exhausted_ = true;
    bitmap_built_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_rescan()
{
  int ret = OB_SUCCESS;
  has_pending_first_ = false;
  if (OB_NOT_NULL(provider_)) {
    bitmap_ = provider_->get_bitmap();
    if (OB_ISNULL(bitmap_)) {
      exhausted_ = true;
      bitmap_built_ = false;
      owns_bitmap_ = false;
      bitmap_iter_.reset();
    } else if (OB_FAIL(init_provider_bitmap_view())) {
      LOG_WARN("failed to initialize filter bitmap view on rescan", K(ret),
               "provider_id", provider_->get_provider_id());
    }
  } else if (OB_NOT_NULL(bitmap_) && !owns_bitmap_) {
    // External bitmap path: content owned upstream and still valid.
    bitmap_iter_.reuse();
    exhausted_ = (bitmap_->cardinality() == 0);
    bitmap_built_ = true;
    if (OB_FAIL(seek_iter_to_lo())) {
      LOG_WARN("failed to seek iter to docid_range_lo on rescan", K(ret));
    }
  } else if (OB_ISNULL(input_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(input_op_->rescan())) {
    LOG_WARN("failed to rescan bitmap input op", K(ret));
  } else {
    if (OB_NOT_NULL(bitmap_)) {
      bitmap_->reuse();
      bitmap_iter_.reuse();
    }
    exhausted_ = true;
    bitmap_built_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  has_pending_first_ = false;
  if (OB_NOT_NULL(provider_)) {
    UNUSED(range);
    if (OB_FAIL(do_rescan())) {
      LOG_WARN("failed to reset filter bitmap view for segment", K(ret),
               "provider_id", provider_->get_provider_id());
    }
  } else if (OB_ISNULL(input_op_) || !owns_bitmap_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected bitmap state on segment rescan", K(ret),
        KP_(input_op), KP_(bitmap), K_(owns_bitmap));
  } else if (OB_FAIL(input_op_->rescan_segment(range))) {
    LOG_WARN("failed to rescan bitmap child segment", K(ret), K(range));
  } else {
    if (OB_NOT_NULL(bitmap_)) {
      bitmap_->reuse();
      bitmap_iter_.reuse();
    }
    exhausted_ = true;
    bitmap_built_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(provider_)) {
    bitmap_iter_.reset();
    bitmap_ = nullptr;
  } else if (owns_bitmap_) {
    if (OB_ISNULL(input_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected bitmap input op", K(ret));
    } else if (OB_FAIL(input_op_->close())) {
      LOG_WARN("failed to close bitmap input op", K(ret));
    } else if (OB_NOT_NULL(bitmap_)) {
      bitmap_iter_.reset();
      bitmap_->reset();
    }
    bitmap_ = nullptr;
  } else {
    // External bitmap or never opened: caller retains ownership if any.
    bitmap_iter_.reset();
    bitmap_ = nullptr;
  }
  owns_bitmap_ = false;
  exhausted_ = true;
  bitmap_built_ = false;
  has_pending_first_ = false;
  rowid_expr_ = nullptr;
  return ret;
}

int ObDASBitmapOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_FAIL(ensure_bitmap_ready())) {
    LOG_WARN("failed to ensure bitmap for advance", K(ret));
  } else if (exhausted_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    // Clamp target to docid_range_lo to ensure no <lo docids leak through
    // (range-parallel tasks must stay within their assigned [lo, hi]).
    uint64_t t = target.get_uint64();
    if (search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_lo().is_min_value()) {
      const uint64_t lo = search_ctx_.get_docid_range_lo().get_uint64();
      if (t < lo) {
        t = lo;
      }
    }
    if (has_pending_first_ && t <= pending_first_val_) {
      // First call consumes the buffered lo-seek result.
      curr_id.set_uint64(pending_first_val_);
      has_pending_first_ = false;
    } else {
      // Target moved past the buffered first val (or no buffer): drop it and
      // advance the underlying iterator.
      has_pending_first_ = false;
      uint64_t val = 0;
      if (OB_FAIL(bitmap_iter_.advance_to(t, val))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance to target", K(ret), K(target), K(t));
        } else {
          exhausted_ = true;
        }
      } else if (is_beyond_docid_range_hi(val)) {
        exhausted_ = true;
        ret = OB_ITER_END;
      } else {
        curr_id.set_uint64(val);
      }
    }
    if (OB_SUCC(ret)) {
      score = is_scoring_ ? boost_weight_ : 0.0;
    }
  }
  return ret;
}

int ObDASBitmapOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_FAIL(ensure_bitmap_ready())) {
    LOG_WARN("failed to ensure bitmap for next rowid", K(ret));
  } else if (exhausted_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else if (has_pending_first_) {
    // First call consumes the buffered lo-seek result from do_open/rescan/build.
    next_id.set_uint64(pending_first_val_);
    has_pending_first_ = false;
    score = is_scoring_ ? boost_weight_ : 0.0;
  } else {
    uint64_t val = 0;
    if (OB_FAIL(bitmap_iter_.next_id(val))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next id from bitmap iter", K(ret));
      } else {
        exhausted_ = true;
      }
    } else if (is_beyond_docid_range_hi(val)) {
      exhausted_ = true;
      ret = OB_ITER_END;
    } else {
      next_id.set_uint64(val);
      score = is_scoring_ ? boost_weight_ : 0.0;
    }
  }
  return ret;
}

int ObDASBitmapOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  bool drained = false;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  const double score = is_scoring_ ? boost_weight_ : 0.0;
  if (OB_FAIL(ensure_bitmap_ready())) {
    LOG_WARN("failed to ensure bitmap for drain", K(ret));
  } else if (exhausted_) {
    next_id_after.set_max();
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    const uint64_t boundary_val = boundary.is_max() ? UINT64_MAX : boundary.get_uint64();
    const uint64_t range_hi =
      search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value()
        ? search_ctx_.get_docid_range_hi().get_uint64() : UINT64_MAX;

    if (OB_UNLIKELY(has_pending_first_)) {
      const uint64_t val = pending_first_val_;
      has_pending_first_ = false;
      if (val > range_hi) {
        exhausted_ = true;
        next_id_after.set_max();
        drained = true;
      } else if (val > boundary_val) {
        next_id_after.set_uint64(val);
        next_score_after = score;
        drained = true;
      } else {
        out_ids[count] = val;
        if (is_scoring_ && nullptr != out_scores) {
          out_scores[count] = score;
        }
        ++count;
        drained = (count == capacity);
      }
    }

    if (OB_SUCC(ret) && !drained) {
      const uint64_t effective_boundary = std::min(range_hi, boundary_val);
      const int64_t batch_offset = count;
      const int64_t remaining_capacity = capacity - batch_offset;
      int64_t batch_count = 0;
      uint64_t next_id = 0;
      ObFastBitmap::Iterator::DrainState drain_state = ObFastBitmap::Iterator::DRAIN_ITER_END;
      if (OB_FAIL(bitmap_iter_.drain_until(
          effective_boundary,
          remaining_capacity,
          out_ids + batch_offset,
          batch_count,
          next_id,
          drain_state))) {
        LOG_WARN("failed to drain bitmap words", K(ret), K(effective_boundary),
                 K(remaining_capacity));
      } else {
        if (is_scoring_ && nullptr != out_scores) {
          for (int64_t i = 0; i < batch_count; ++i) {
            out_scores[batch_offset + i] = score;
          }
        }
        count = batch_offset + batch_count;
        if (ObFastBitmap::Iterator::DRAIN_ITER_END == drain_state) {
          exhausted_ = true;
          next_id_after.set_max();
        } else if (ObFastBitmap::Iterator::DRAIN_BOUNDARY_REACHED == drain_state) {
          if (next_id > range_hi) {
            exhausted_ = true;
            next_id_after.set_max();
          } else {
            next_id_after.set_uint64(next_id);
            next_score_after = score;
          }
        }
        // ObFastBitmap::Iterator::DRAIN_BATCH_FULL deliberately leaves next_id_after unspecified.
      }
    }
  }
  return ret;
}

int ObDASBitmapOp::drain_into_filter_bitset(
    const ObDASRowID &batch_start_id,
    const ObDASRowID &batch_end_id,
    const int64_t capacity,
    uint64_t *filter_words,
    ObDASRowID &curr_id,
    bool &iter_end)
{
  int ret = OB_SUCCESS;
  int64_t observed_count = 0;
  if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_drain_into_filter_bitset(
        batch_start_id, batch_end_id, capacity, filter_words,
        curr_id, iter_end, observed_count))) {
      LOG_WARN("failed to drain bitmap directly into filter bitset", K(ret));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    if (OB_FAIL(do_drain_into_filter_bitset(
        batch_start_id, batch_end_id, capacity, filter_words,
        curr_id, iter_end, observed_count))) {
      LOG_WARN("failed to drain bitmap directly into filter bitset", K(ret));
    } else {
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, observed_count);
    }
  }
  return ret;
}

int ObDASBitmapOp::do_drain_into_filter_bitset(
    const ObDASRowID &batch_start_id,
    const ObDASRowID &batch_end_id,
    const int64_t capacity,
    uint64_t *filter_words,
    ObDASRowID &curr_id,
    bool &iter_end,
    int64_t &observed_count)
{
  int ret = OB_SUCCESS;
  int64_t copied_count = 0;
  observed_count = 0;
  if (!is_filter_bitmap_view()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct filter bitset drain requires a provider bitmap view", K(ret));
  } else if (OB_FAIL(ensure_bitmap_ready())) {
    LOG_WARN("failed to ensure provider bitmap for direct drain", K(ret));
  } else if (OB_UNLIKELY(exhausted_ || iter_end)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot drain an exhausted provider bitmap", K(ret), K_(exhausted), K(iter_end));
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited() || OB_ISNULL(bitmap_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider bitmap is not initialized", K(ret), KP_(bitmap));
  } else if (OB_UNLIKELY(!batch_start_id.is_normal()
                         || !batch_end_id.is_normal()
                         || !curr_id.is_normal()
                         || capacity <= 0
                         || OB_ISNULL(filter_words))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct filter bitset drain arguments", K(ret),
             K(batch_start_id), K(batch_end_id), K(curr_id), K(capacity), KP(filter_words));
  } else {
    const uint64_t batch_start = batch_start_id.get_uint64();
    const uint64_t batch_end = batch_end_id.get_uint64();
    const uint64_t current = curr_id.get_uint64();
    const uint64_t range_hi =
        search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value()
        ? search_ctx_.get_docid_range_hi().get_uint64() : UINT64_MAX;
    const uint64_t effective_end = std::min(batch_end, range_hi);
    if (OB_UNLIKELY(current < batch_start
                    || effective_end < current
                    || effective_end - batch_start >= static_cast<uint64_t>(capacity))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("provider bitmap cursor is outside the destination window", K(ret),
               K(batch_start), K(batch_end), K(current), K(effective_end), K(capacity));
    } else if (OB_FAIL(bitmap_->copy_range_to_words(
                   current, effective_end, batch_start, capacity,
                   filter_words, copied_count))) {
      LOG_WARN("failed to copy provider bitmap range", K(ret),
               K(batch_start), K(effective_end), K(current), K(capacity));
    } else if (OB_UNLIKELY(copied_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current provider bitmap row is missing from copied range", K(ret),
               K(current), K(effective_end), K(copied_count));
    } else {
      has_pending_first_ = false;
      // curr_id was already observed by next_rowid()/advance_to(). Every copied
      // row after it is newly observed by this direct drain.
      observed_count = copied_count - 1;
      if (effective_end == range_hi || effective_end == UINT64_MAX) {
        exhausted_ = true;
        iter_end = true;
      } else {
        uint64_t next_val = 0;
        const int advance_ret = bitmap_iter_.advance_to(effective_end + 1, next_val);
        if (OB_ITER_END == advance_ret || (OB_SUCCESS == advance_ret && next_val > range_hi)) {
          exhausted_ = true;
          iter_end = true;
        } else if (OB_SUCCESS != advance_ret) {
          ret = advance_ret;
          LOG_WARN("failed to advance provider bitmap after direct drain", K(ret), K(effective_end));
        } else {
          curr_id.set_uint64(next_val);
          iter_end = false;
          ++observed_count;
        }
      }
    }
  }
  return ret;
}

int ObDASBitmapOp::do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, is_scoring_ ? boost_weight_ : 0.0);
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASBitmapOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = is_scoring_ ? boost_weight_ : 0.0;
  return ret;
}

int ObDASBitmapOp::fill_bitmap_from_scan(ObFastBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  int64_t count = 0;
  const bool use_raw_doc_ids = DAS_SEARCH_OP_ASYNC_SCALAR_RANGE == input_op_->get_op_type()
      || (DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == input_op_->get_op_type()
          && static_cast<ObDASAsyncSegmentMergeOp *>(input_op_)->can_forward_raw_doc_ids());
  if (use_raw_doc_ids) {
    while (OB_SUCC(ret)) {
      const uint64_t *doc_ids = nullptr;
      count = 0;
      if (DAS_SEARCH_OP_ASYNC_SCALAR_RANGE == input_op_->get_op_type()) {
        ret = static_cast<ObDASAsyncScalarRangeOp *>(input_op_)->get_next_doc_ids(
            max_batch_size(), doc_ids, count);
      } else {
        ret = static_cast<ObDASAsyncSegmentMergeOp *>(input_op_)->get_next_doc_ids(
            max_batch_size(), doc_ids, count);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN_IGNORE_ITER_END(
            ret, "failed to get raw doc id batch for bitmap",
            K(ret), K(max_batch_size()));
      } else {
        total_count += count;
        if (OB_FAIL(bitmap.add_batch(doc_ids, count))) {
          LOG_WARN("failed to add id batch to bitmap", K(ret), K(count));
        }
      }
    }
  } else {
    while (OB_SUCC(ret)) {
      count = 0;
      if (OB_FAIL(input_op_->next_rowids(max_batch_size(), count))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("failed to get next rows from scan", K(ret), K(max_batch_size()));
        } else if (count > 0) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && count > 0) {
        total_count += count;
        ObIVector *vector = nullptr;
        if (OB_ISNULL(rowid_expr_) || OB_ISNULL(vector = rowid_expr_->get_vector(eval_ctx()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null rowid expr or vector", K(ret), KPC(rowid_expr_));
        } else if (common::VEC_FIXED == vector->get_format()) {
          common::ObFixedLengthBase *fixed_vec = static_cast<common::ObFixedLengthBase *>(vector);
          const uint64_t *data = reinterpret_cast<const uint64_t *>(fixed_vec->get_data());
          if (OB_FAIL(bitmap.add_batch(data, count))) {
            LOG_WARN("failed to add id batch to bitmap", K(ret), K(count));
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
            if (OB_FAIL(bitmap.add(vector->get_uint64(i)))) {
              LOG_WARN("failed to add id to bitmap", K(ret));
            }
          }
        }
      }
    }
  }
  ret = ret == OB_ITER_END ? OB_SUCCESS : ret;
  if (OB_SUCC(ret)) {
    bitmap.finalize();
  }
  LOG_TRACE("fill_bitmap_from_scan done", K(ret), K(total_count), K(bitmap.cardinality()));
  return ret;
}

int ObDASBitmapOp::try_fill_provider_bitmap(
    ObFastBitmap &bitmap, bool &used_direct_build)
{
  int ret = OB_SUCCESS;
  used_direct_build = false;
  if (OB_NOT_NULL(provider_) || !owns_bitmap_ || OB_ISNULL(bitmap_)
      || bitmap_built_ || has_pending_first_ || is_scoring_
      || DAS_SCOPE_BASE != get_scope() || DAS_ROWID_TYPE_UINT64 != get_rowid_type()
      || search_ctx_.is_logical_table() || search_ctx_.has_docid_range()
      || OB_ISNULL(input_op_) || DAS_SCOPE_BASE != input_op_->get_scope()) {
    // Preserve Views, pre-consumed Sources and cursor-level range clipping.
  } else if (DAS_SEARCH_OP_ASYNC_SCALAR_RANGE != input_op_->get_op_type()
      && !(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == input_op_->get_op_type()
          && static_cast<ObDASAsyncSegmentMergeOp *>(input_op_)->can_forward_raw_doc_ids())) {
    // Do not bypass Boolean/deletion/nested wrappers or a SORT_MERGE cursor.
  } else if (&bitmap == bitmap_ || 0 != bitmap.cardinality()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("provider bitmap destination must be distinct and empty", K(ret), KP_(bitmap));
  } else {
    used_direct_build = true;
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    // prepare_source() already opened input_op_. Reuse exactly the private
    // materializer's raw DocID kernel, but encode/finalize only the caller's
    // bitmap. Never borrow a Source allocation into a parallel shared slot.
    if (OB_FAIL(fill_bitmap_from_scan(bitmap))) {
      LOG_WARN("failed to fill provider bitmap from opened range source", K(ret));
    } else {
      // The exclusive Source has been consumed to EOF. Its original bitmap
      // stays empty; retaining its ownership keeps close/rescan unchanged.
      bitmap_built_ = true;
      exhausted_ = true;
      has_pending_first_ = false;
      bitmap_iter_.reuse();
      LOG_TRACE("range filter materialized directly into provider bitmap",
                K(bitmap.cardinality()));
    }
  }
  return ret;
}

int ObDASBitmapOp::build_bitmap()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_op_) || OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(input_op_), K(bitmap_));
  } else if (OB_FAIL(fill_bitmap_from_scan(*bitmap_))) {
    LOG_WARN("failed to fill bitmap from scan", K(ret));
  } else {
    uint64_t cardinality = bitmap_->cardinality();
    if (cardinality == 0) {
      exhausted_ = true;
      bitmap_built_ = true;
      LOG_TRACE("bitmap built with no data");
    } else {
      exhausted_ = false;
      bitmap_built_ = true;
      if (OB_FAIL(seek_iter_to_lo())) {
        LOG_WARN("failed to seek iter to docid_range_lo after build", K(ret));
      }
    }
  }
  return ret;
}

int ObDASBitmapOp::ensure_bitmap_ready()
{
  int ret = OB_SUCCESS;
  if (bitmap_built_) {
    // already ready
  } else if (OB_NOT_NULL(provider_)) {
    if (OB_FAIL(provider_->ensure_prepared())) {
      LOG_WARN("failed to prepare filter bitmap provider on first use", K(ret),
               "provider_id", provider_->get_provider_id());
    } else if (OB_ISNULL(bitmap_ = provider_->get_bitmap())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepared filter bitmap provider has no bitmap", K(ret),
               "provider_id", provider_->get_provider_id());
    } else if (OB_FAIL(init_provider_bitmap_view())) {
      LOG_WARN("failed to initialize lazily prepared filter bitmap view", K(ret),
               "provider_id", provider_->get_provider_id());
    }
  } else if (OB_FAIL(build_bitmap())) {
    LOG_WARN("failed to build private bitmap", K(ret));
  }
  return ret;
}

int ObDASBitmapOp::init_provider_bitmap_view()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(provider_) || OB_ISNULL(bitmap_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter bitmap provider view", K(ret), KP_(provider), KP_(bitmap));
  } else if (OB_FAIL(bitmap_iter_.init(bitmap_))) {
    LOG_WARN("failed to initialize provider bitmap iterator", K(ret),
             "provider_id", provider_->get_provider_id());
  } else {
    exhausted_ = 0 == bitmap_->cardinality();
    bitmap_built_ = true;
    owns_bitmap_ = false;
    if (OB_FAIL(seek_iter_to_lo())) {
      LOG_WARN("failed to seek provider bitmap view", K(ret),
               "provider_id", provider_->get_provider_id());
    }
  }
  return ret;
}

int ObDASBitmapOp::seek_iter_to_lo()
{
  int ret = OB_SUCCESS;
  has_pending_first_ = false;
  if (!exhausted_ && bitmap_built_ && search_ctx_.has_docid_range()
      && !search_ctx_.get_docid_range_lo().is_min_value()) {
    const uint64_t lo = search_ctx_.get_docid_range_lo().get_uint64();
    uint64_t val = 0;
    if (OB_FAIL(bitmap_iter_.advance_to(lo, val))) {
      if (OB_ITER_END == ret) {
        exhausted_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to advance bitmap iter to docid_range_lo", K(ret), K(lo));
      }
    } else if (is_beyond_docid_range_hi(val)) {
      exhausted_ = true;
    } else {
      pending_first_val_ = val;
      has_pending_first_ = true;
    }
  }
  return ret;
}

int ObDASBitmapOp::build_shared_bitmap(common::ObIAllocator &bitmap_alloc, ObFastBitmap *&out_bitmap)
{
  int ret = OB_SUCCESS;
  out_bitmap = nullptr;
  if (OB_ISNULL(input_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input_op_ is null", K(ret));
  } else if (OB_FAIL(input_op_->open())) {
    LOG_WARN("failed to open input op for shared bitmap", K(ret));
  } else {
    ObFastBitmap *bm = OB_NEWx(ObFastBitmap, &bitmap_alloc, bitmap_alloc);
    if (OB_ISNULL(bm)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate shared bitmap", K(ret));
    } else if (OB_FAIL(bm->init(search_ctx_.get_row_count().cost()))) {
      LOG_WARN("failed to init shared bitmap", K(ret));
    } else if (OB_FAIL(fill_bitmap_from_scan(*bm))) {
      LOG_WARN("failed to fill shared bitmap from scan", K(ret));
    }
    int close_ret = input_op_->close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("failed to close scan op after shared bitmap build", K(close_ret));
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
    if (OB_SUCC(ret)) {
      out_bitmap = bm;
      // Caller will inject this bitmap back via set_external_bitmap() in a later
      // phase; do not assume ownership here.
      LOG_TRACE("shared bitmap built", K(bm->cardinality()));
    } else if (OB_NOT_NULL(bm)) {
      bm->~ObFastBitmap();
      bitmap_alloc.free(bm);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

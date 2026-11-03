/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_doc_score_accumulator.h"

namespace oceanbase
{
namespace sql
{

DocScoreAccumulator::DocScoreAccumulator()
  : start_id_(0),
    scores_(),
    bitset_(nullptr),
    capacity_(0),
    max_score_(0.0),
    allocator_(nullptr),
    filter_mask_(nullptr),
    is_empty_(true),
    is_inited_(false)
{}

int DocScoreAccumulator::init(const int64_t capacity, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("DocScoreAccumulator has been initialized", K(ret));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else {
    allocator_ = &allocator;
    scores_.set_allocator(allocator_);
    if (OB_FAIL(scores_.init(capacity))) {
      LOG_WARN("failed to init scores array", K(ret), K(capacity));
    } else if (OB_FAIL(scores_.prepare_allocate(capacity))) {
      LOG_WARN("failed to prepare allocate scores array", K(ret), K(capacity));
    } else {
      void *buf = nullptr;
      const int64_t mem_size = ObBitVector::memory_size(capacity);
      if (OB_ISNULL(buf = allocator_->alloc(mem_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for bitset", K(ret), K(capacity), K(mem_size));
      } else {
        bitset_ = to_bit_vector(buf);
        bitset_->init(capacity);
        capacity_ = capacity;
        start_id_ = 0;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int DocScoreAccumulator::reuse_with_start_id(const uint64_t start_id, uint64_t &max_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bitset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bitset", K(ret), KP(bitset_));
  } else {
    start_id_ = start_id;
    const uint64_t window_span = static_cast<uint64_t>(capacity_ - 1);
    max_id = start_id_ > UINT64_MAX - window_span
        ? UINT64_MAX
        : start_id_ + window_span;
    bitset_->reset(capacity_);
    filter_mask_ = nullptr;
    is_empty_ = true;
    max_score_ = 0.0;
  }
  return ret;
}

void DocScoreAccumulator::reset()
{
  if (bitset_ != nullptr && allocator_ != nullptr) {
    allocator_->free(bitset_);
    bitset_ = nullptr;
  }
  scores_.reset();
  capacity_ = 0;
  start_id_ = 0;
  filter_mask_ = nullptr;
  is_inited_ = false;
  max_score_ = 0.0;
  is_empty_ = true;
}

int DocScoreAccumulator::seed_doc(const uint64_t doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else if (OB_UNLIKELY(!is_doc_in_window(doc_id))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("seed doc_id is out of range", K(ret), K(doc_id), K(start_id_), K_(capacity));
  } else {
    const int64_t offset = static_cast<int64_t>(doc_id - start_id_);
    if (!bitset_->exist(offset)) {
      scores_.at(offset) = 0.0;
      bitset_->set(offset);
      is_empty_ = false;
    }
  }
  return ret;
}

int DocScoreAccumulator::seed_docs(const uint64_t *doc_ids, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count < 0 || (count > 0 && OB_ISNULL(doc_ids)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seed docs", K(ret), KP(doc_ids), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_UNLIKELY(i > 0 && doc_ids[i] < doc_ids[i - 1])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("seed doc_ids are not sorted", K(ret), K(i), K(doc_ids[i - 1]), K(doc_ids[i]));
    } else if (OB_FAIL(seed_doc(doc_ids[i]))) {
      LOG_WARN("failed to seed doc", K(ret), K(i), K(doc_ids[i]));
    }
  }
  return ret;
}

int DocScoreAccumulator::add_dim(const uint64_t doc_id, const double score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else if (OB_UNLIKELY(!is_doc_in_window(doc_id))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("doc_id is out of range", K(ret), K(doc_id), K(start_id_), K_(capacity));
  } else {
    const int64_t offset = doc_id - start_id_;
    if (OB_UNLIKELY(nullptr != filter_mask_ && !filter_mask_->exist(offset))) {
      // Skip doc if not in filter mask.
    } else if (!bitset_->exist(offset)) {
      // New doc — set score and bit.
      scores_.at(offset) = score;
      bitset_->set(offset);
      max_score_ = MAX(max_score_, score);
      is_empty_ = false;
    } else {
      // Existing doc — accumulate score.
      scores_.at(offset) += score;
      max_score_ = MAX(max_score_, scores_.at(offset));
    }
  }
  return ret;
}

int DocScoreAccumulator::add_dim_batch(
    const uint64_t *doc_ids, const double *scores, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    // do nothing
  } else if (OB_UNLIKELY(OB_ISNULL(doc_ids) || OB_ISNULL(scores))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch arrays", K(ret), KP(doc_ids), KP(scores), K(count));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else if (OB_UNLIKELY(!is_doc_in_window(doc_ids[0])
                         || !is_doc_in_window(doc_ids[count - 1]))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("batch doc_ids out of range", K(ret),
        K(doc_ids[0]), K(doc_ids[count - 1]), K(count), K_(start_id), K_(capacity));
  } else {
    // TODO: SIMD vectorization.
    double *acc_scores = scores_.get_data();
    if (nullptr == filter_mask_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const int64_t offset = static_cast<int64_t>(doc_ids[i] - start_id_);
        if (OB_UNLIKELY(offset < 0 || offset >= capacity_)) {
          ret = OB_INDEX_OUT_OF_RANGE;
          LOG_WARN("batch doc_id out of window", K(ret), K(i), K(doc_ids[i]),
            K(offset), K_(start_id), K_(capacity));
        } else if (!bitset_->exist(offset)) {
          // New doc — set score and bit.
          acc_scores[offset] = scores[i];
          bitset_->set(offset);
          max_score_ = MAX(max_score_, scores[i]);
          is_empty_ = false;
        } else {
          // Existing doc — accumulate score.
          acc_scores[offset] += scores[i];
          max_score_ = MAX(max_score_, acc_scores[offset]);
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const int64_t offset = static_cast<int64_t>(doc_ids[i] - start_id_);
        if (OB_UNLIKELY(offset < 0 || offset >= capacity_)) {
          ret = OB_INDEX_OUT_OF_RANGE;
          LOG_WARN("batch doc_id out of window", K(ret), K(i), K(doc_ids[i]),
            K(offset), K_(start_id), K_(capacity));
        } else if (OB_UNLIKELY(!filter_mask_->exist(offset))) {
          // Skip doc if not in filter mask.
        } else if (!bitset_->exist(offset)) {
          // New doc — set score and bit.
          acc_scores[offset] = scores[i];
          bitset_->set(offset);
          max_score_ = MAX(max_score_, scores[i]);
          is_empty_ = false;
        } else {
          // Existing doc — accumulate score.
          acc_scores[offset] += scores[i];
          max_score_ = MAX(max_score_, acc_scores[offset]);
        }
      }
    }
  }
  return ret;
}

int DocScoreAccumulator::get_score(const uint64_t doc_id, double &score) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else if (OB_UNLIKELY(!is_doc_in_window(doc_id))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("doc_id is out of range", K(ret), K(doc_id), K(start_id_), K_(capacity));
  } else {
    const int64_t offset = static_cast<int64_t>(doc_id - start_id_);
    score = bitset_->exist(offset) ? scores_.at(offset) : 0.0;
  }
  return ret;
}

int DocScoreAccumulator::filter_below_score(const double min_required_score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else if (is_empty_ || min_required_score < 0.0) {
    // do nothing
  } else if (max_score_ <= min_required_score) {
    // Fast path: all docs are below threshold, clear current batch.
    clear_all();
  } else {
    // TODO: SIMD vectorization.
    double *acc_scores = scores_.get_data();
    uint64_t *bitset_words = bitset_->reinterpret_data<uint64_t>();
    const int64_t word_bits = das_bitset_helper::WORD_BITS;
    const int64_t word_cnt = das_bitset_helper::word_count(capacity_);
    max_score_ = 0.0;
    for (int64_t word_idx = 0; word_idx < word_cnt; ++word_idx) {
      const int64_t word_base_idx = word_idx * word_bits;
      uint64_t curr_word = das_bitset_helper::mask_window_tail(
          bitset_words[word_idx], word_base_idx, capacity_);
      uint64_t kept_word = curr_word;
      while (curr_word != 0) {
        const int64_t bit_idx = das_bitset_helper::lowest_set_bit(curr_word);
        curr_word = das_bitset_helper::clear_lowest_set_bit(curr_word);
        const int64_t offset = word_base_idx + bit_idx;
        if (acc_scores[offset] > min_required_score) {
          max_score_ = MAX(max_score_, acc_scores[offset]);
        } else {
          kept_word = das_bitset_helper::clear_bit(kept_word, bit_idx);
        }
      }
      bitset_words[word_idx] = kept_word;
    }
    is_empty_ = (max_score_ == 0.0);
  }
  return ret;
}

int DocScoreAccumulator::retain_by_mask(const ObBitVector &mask)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
  } else {
    double *acc_scores = scores_.get_data();
    uint64_t *bitset_words = bitset_->reinterpret_data<uint64_t>();
    const uint64_t *mask_words = mask.reinterpret_data<uint64_t>();
    const int64_t word_bits = das_bitset_helper::WORD_BITS;
    const int64_t word_cnt = das_bitset_helper::word_count(capacity_);
    max_score_ = 0.0;
    is_empty_ = true;
    for (int64_t word_idx = 0; word_idx < word_cnt; ++word_idx) {
      const int64_t word_base_idx = word_idx * word_bits;
      uint64_t kept_word = das_bitset_helper::mask_window_tail(
          bitset_words[word_idx] & mask_words[word_idx], word_base_idx, capacity_);
      bitset_words[word_idx] = kept_word;
      if (0 != kept_word) {
        is_empty_ = false;
        while (0 != kept_word) {
          const int64_t bit_idx = das_bitset_helper::lowest_set_bit(kept_word);
          kept_word = das_bitset_helper::clear_lowest_set_bit(kept_word);
          max_score_ = MAX(max_score_, acc_scores[word_base_idx + bit_idx]);
        }
      }
    }
  }
  return ret;
}

void DocScoreAccumulator::clear_all()
{
  if (OB_LIKELY(is_valid())) {
    bitset_->reset(capacity_);
    max_score_ = 0.0;
    is_empty_ = true;
  }
}

DocScoreIter::DocScoreIter()
  : accumulator_(nullptr),
    bitset_words_(nullptr),
    curr_word_idx_(0),
    curr_word_base_idx_(0),
    word_cnt_(0),
    curr_word_(0),
    curr_id_(0),
    curr_valid_(false),
    iter_end_(true)
{}

int DocScoreIter::open(const DocScoreAccumulator &accumulator)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!accumulator.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc score accumulator", K(ret), K(accumulator));
  } else {
    accumulator_ = &accumulator;
    bitset_words_ = accumulator.bitset_->reinterpret_data<uint64_t>();
    word_cnt_ = das_bitset_helper::word_count(accumulator.capacity_);
    iter_end_ = false;
  }
  return ret;
}

int DocScoreIter::consume_next_set_bit(uint64_t &out_id, double &out_score, double min_score)
{
  int ret = OB_SUCCESS;
  const int64_t capacity = accumulator_->capacity_;
  const int64_t word_bits = das_bitset_helper::WORD_BITS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    if (curr_word_ != 0) {
      const int64_t next_bit = das_bitset_helper::lowest_set_bit(curr_word_);
      const int64_t row_offset = curr_word_base_idx_ + next_bit;
      if (OB_UNLIKELY(row_offset >= capacity)) {
        ret = OB_ITER_END;
      } else {
        curr_word_ = das_bitset_helper::clear_lowest_set_bit(curr_word_);
        const uint64_t doc_id = accumulator_->start_id_ + row_offset;
        const double doc_score = accumulator_->scores_.at(row_offset);
        // Update high-water mark regardless of whether we return this doc:
        // advance() clamps target to curr_id_ so backward seeks never re-scan
        // consumed bits. The bit is already cleared from curr_word_ above.
        curr_id_ = doc_id;
        curr_valid_ = true;
        if (!should_project(doc_score, min_score)) {
          // skip
        } else {
          out_id = doc_id;
          out_score = doc_score;
          got_row = true;
        }
      }
    } else if (curr_word_idx_ >= word_cnt_) {
      ret = OB_ITER_END;
    } else {
      // Load next word, masking off tail bits beyond the accumulator window.
      curr_word_base_idx_ = curr_word_idx_ * word_bits;
      curr_word_ = das_bitset_helper::mask_window_tail(
          bitset_words_[curr_word_idx_++], curr_word_base_idx_, capacity);
    }
  }
  return ret;
}

int DocScoreIter::next(uint64_t &next_id, double &score, double min_score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid doc score iter", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (accumulator_->max_score_ <= min_score) {
    // No doc can beat min_score, skip the whole window.
    ret = OB_ITER_END;
  } else if (OB_FAIL(consume_next_set_bit(next_id, score, min_score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to consume next set bit", K(ret));
  }
  if (OB_ITER_END == ret) {
    iter_end_ = true;
  }
  return ret;
}

int DocScoreIter::advance(uint64_t target, uint64_t &out_id, double &out_score, double min_score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid doc score iter", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (accumulator_->max_score_ <= min_score) {
    // No doc can beat min_score, skip the whole window.
    ret = OB_ITER_END;
  } else {
    const int64_t capacity = accumulator_->capacity_;
    const int64_t word_bits = das_bitset_helper::WORD_BITS;
    if (curr_valid_ && target < curr_id_) {
      target = curr_id_;
    }
    // Locate the target word and mask off bits below target.
    if (target >= accumulator_->start_id_) {
      const uint64_t unsigned_target_offset = target - accumulator_->start_id_;
      if (OB_UNLIKELY(unsigned_target_offset >= static_cast<uint64_t>(capacity))) {
        curr_word_ = 0;
        curr_word_idx_ = word_cnt_;
        curr_word_base_idx_ = capacity;
        ret = OB_ITER_END;
      } else {
        const int64_t target_offset = static_cast<int64_t>(unsigned_target_offset);
        const int64_t target_word_idx = das_bitset_helper::word_index(target_offset);
        const int64_t local_target_bit = das_bitset_helper::bit_index(target_offset);
        curr_word_base_idx_ = target_word_idx * word_bits;
        curr_word_idx_ = target_word_idx + 1;
        curr_word_ = das_bitset_helper::mask_window_tail(
            bitset_words_[target_word_idx], curr_word_base_idx_, capacity);
        curr_word_ = das_bitset_helper::clear_bits_below(curr_word_, local_target_bit);
      }
    }
    // Consume the next set bit >= target from the current cursor.
    if (OB_SUCC(ret) && OB_FAIL(consume_next_set_bit(out_id, out_score, min_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to target", K(ret), K(target));
    }
  }
  if (OB_ITER_END == ret) {
    iter_end_ = true;
  }
  return ret;
}

void DocScoreIter::reset()
{
  accumulator_ = nullptr;
  bitset_words_ = nullptr;
  curr_word_idx_ = 0;
  curr_word_base_idx_ = 0;
  word_cnt_ = 0;
  curr_word_ = 0;
  curr_id_ = 0;
  curr_valid_ = false;
  iter_end_ = true;
}

} // namespace sql
} // namespace oceanbase

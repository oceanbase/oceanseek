/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_sparse_candidate_buffer.h"

namespace oceanbase
{
namespace sql
{

SparseCandidateBuffer::SparseCandidateBuffer()
  : allocator_(nullptr),
    doc_ids_(),
    scores_(),
    alive_bits_(nullptr),
    capacity_(0),
    count_(0),
    alive_count_(0),
    cursor_(-1),
    is_inited_(false)
{}

int SparseCandidateBuffer::init(const int64_t capacity, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sparse candidate buffer has been initialized", K(ret));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sparse candidate buffer capacity", K(ret), K(capacity));
  } else {
    allocator_ = &allocator;
    doc_ids_.set_allocator(allocator_);
    scores_.set_allocator(allocator_);
    if (OB_FAIL(doc_ids_.prepare_allocate(capacity))) {
      LOG_WARN("failed to prepare allocate candidate doc ids", K(ret), K(capacity));
    } else if (OB_FAIL(scores_.prepare_allocate(capacity))) {
      LOG_WARN("failed to prepare allocate candidate scores", K(ret), K(capacity));
    } else {
      const int64_t mem_size = ObBitVector::memory_size(capacity);
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_->alloc(mem_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for alive bits", K(ret), K(capacity), K(mem_size));
      } else {
        alive_bits_ = to_bit_vector(buf);
        alive_bits_->init(capacity);
        capacity_ = capacity;
        count_ = 0;
        alive_count_ = 0;
        cursor_ = -1;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void SparseCandidateBuffer::reset()
{
  if (nullptr != alive_bits_ && nullptr != allocator_) {
    allocator_->free(alive_bits_);
    alive_bits_ = nullptr;
  }
  doc_ids_.reset();
  scores_.reset();
  allocator_ = nullptr;
  capacity_ = 0;
  count_ = 0;
  alive_count_ = 0;
  cursor_ = -1;
  is_inited_ = false;
}

void SparseCandidateBuffer::reuse()
{
  if (IS_INIT) {
    alive_bits_->reset(capacity_);
    count_ = 0;
    alive_count_ = 0;
    cursor_ = -1;
  }
}

int SparseCandidateBuffer::append(const uint64_t doc_id, const double initial_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(-1 != cursor_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot append after output has started", K(ret), K_(cursor), K_(count));
  } else if (OB_UNLIKELY(is_full())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("sparse candidate buffer is full", K(ret), K_(count), K_(capacity));
  } else if (OB_UNLIKELY(count_ > 0 && doc_id <= doc_ids_.at(count_ - 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("candidate doc id is not strictly increasing",
        K(ret), K(doc_id), "last_doc_id", doc_ids_.at(count_ - 1), K_(count));
  } else {
    const int64_t idx = count_;
    doc_ids_.at(idx) = doc_id;
    scores_.at(idx) = initial_score;
    alive_bits_->set(idx);
    ++count_;
    ++alive_count_;
  }
  return ret;
}

int SparseCandidateBuffer::append_competitive_batch(
    const uint64_t *doc_ids, const double *scores, const int64_t count,
    const double min_required_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret));
  } else if (OB_UNLIKELY(count < 0 || -1 != cursor_
      || (count > 0 && (nullptr == doc_ids || nullptr == scores)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid competitive candidate batch", K(ret), K(count), K_(cursor));
  } else if (OB_UNLIKELY(count > capacity_ - count_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("candidate batch exceeds capacity", K(ret), K(count), K_(count), K_(capacity));
  } else if (OB_UNLIKELY(count > 0 && count_ > 0 && doc_ids[0] <= doc_ids_.at(count_ - 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("candidate batch is not after collected ids", K(ret), K(doc_ids[0]), K_(count));
  } else {
    uint64_t *out_ids = doc_ids_.get_data();
    double *out_scores = scores_.get_data();
    int64_t write_pos = count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (i > 0 && doc_ids[i - 1] >= doc_ids[i]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("candidate batch doc ids are not strictly increasing",
            K(ret), K(i), K(count), "prev_doc_id", doc_ids[i - 1], "doc_id", doc_ids[i]);
      } else if (scores[i] > min_required_score) {
        out_ids[write_pos] = doc_ids[i];
        out_scores[write_pos] = scores[i];
        alive_bits_->set(write_pos++);
      }
    }
    alive_count_ += write_pos - count_;
    count_ = write_pos;
  }
  return ret;
}

bool SparseCandidateBuffer::get_doc_id(const int64_t idx, uint64_t &doc_id) const
{
  bool alive = false;
  doc_id = 0;
  if (IS_INIT && is_valid_index(idx)) {
    doc_id = doc_ids_.at(idx);
    alive = alive_bits_->at(idx);
  }
  return alive;
}

int SparseCandidateBuffer::add_score(const int64_t idx, const double score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_index(idx))) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("candidate index out of range", K(ret), K(idx), K_(count));
  } else if (OB_UNLIKELY(!alive_bits_->at(idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("candidate has been pruned", K(ret), K(idx));
  } else {
    scores_.at(idx) += score;
  }
  return ret;
}

int SparseCandidateBuffer::prune_below(const double min_required_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret), K_(is_inited));
  } else {
    for (int64_t idx = 0; idx < count_; ++idx) {
      if (alive_bits_->at(idx) && scores_.at(idx) <= min_required_score) {
        alive_bits_->unset(idx);
        --alive_count_;
      }
    }
  }
  return ret;
}

int SparseCandidateBuffer::prune_and_compact(const double min_required_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret));
  } else if (OB_UNLIKELY(-1 != cursor_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot compact after output has started", K(ret), K_(cursor));
  } else {
    int64_t kept = 0;
    for (int64_t i = 0; i < count_; ++i) {
      if (alive_bits_->at(i) && scores_.at(i) > min_required_score) {
        if (kept != i) {
          doc_ids_.at(kept) = doc_ids_.at(i);
          scores_.at(kept) = scores_.at(i);
          alive_bits_->set(kept);
        }
        ++kept;
      }
    }
    for (int64_t i = kept; i < count_; ++i) {
      alive_bits_->unset(i);
    }
    count_ = alive_count_ = kept;
  }
  return ret;
}

int SparseCandidateBuffer::advance_to(
    const uint64_t target,
    const double min_score,
    uint64_t &doc_id,
    double &score)
{
  int ret = OB_SUCCESS;
  doc_id = 0;
  score = 0.0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret), K_(is_inited));
  } else {
    bool got_row = false;
    while (!got_row && cursor_ < count_) {
      if (cursor_ < 0
          || doc_ids_.at(cursor_) < target
          || !alive_bits_->at(cursor_)
          || scores_.at(cursor_) <= min_score) {
        ++cursor_;
      } else {
        doc_id = doc_ids_.at(cursor_);
        score = scores_.at(cursor_);
        got_row = true;
      }
    }
    if (!got_row) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int SparseCandidateBuffer::next(
    const double min_score,
    uint64_t &doc_id,
    double &score)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sparse candidate buffer not init", K(ret), K_(is_inited));
  } else {
    while (!got_row && cursor_ < count_) {
      ++cursor_;
      if (cursor_ < count_ && alive_bits_->at(cursor_) && scores_.at(cursor_) > min_score) {
        doc_id = doc_ids_.at(cursor_);
        score = scores_.at(cursor_);
        got_row = true;
      }
    }
    if (!got_row) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

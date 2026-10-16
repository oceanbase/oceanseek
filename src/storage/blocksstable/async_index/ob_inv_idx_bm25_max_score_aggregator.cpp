/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_bm25_max_score_aggregator.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvIdxBM25MaxScoreAggregator::ObInvIdxBM25MaxScoreAggregator()
  : allocator_(nullptr),
    max_pair_count_(0),
    frontier_pairs_(),
    is_inited_(false)
{
}

ObInvIdxBM25MaxScoreAggregator::~ObInvIdxBM25MaxScoreAggregator()
{
  reset();
}

int ObInvIdxBM25MaxScoreAggregator::init(ObIAllocator &allocator, const int64_t max_pair_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(max_pair_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max pair count", K(ret), K(max_pair_count));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (FALSE_IT(max_pair_count_ = max_pair_count)) {
  } else if (FALSE_IT(frontier_pairs_.set_allocator(allocator_))) {
  } else if (OB_FAIL(frontier_pairs_.init(max_pair_count_))) {
    LOG_WARN("failed to init frontier pairs", K(ret), K_(max_pair_count));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObInvIdxBM25MaxScoreAggregator::aggregate(
    const ObIArray<uint64_t> &token_freqs,
    const ObIArray<uint64_t> &doc_lengths,
    const ObIArray<ObInvIdxBM25ParamPair> *&frontier_pairs)
{
  int ret = OB_SUCCESS;
  uint64_t max_token_freq = 0;
  uint64_t min_doc_length = UINT64_MAX;
  bool use_fallback_pair = false;
  frontier_pairs = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(token_freqs.count() <= 0 || token_freqs.count() != doc_lengths.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bm25 param arrays", K(ret), K(token_freqs.count()), K(doc_lengths.count()));
  } else {
    reuse();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < token_freqs.count(); ++i) {
    const uint64_t token_freq = token_freqs.at(i);
    const uint64_t doc_length = doc_lengths.at(i);
    if (max_token_freq < token_freq) {
      max_token_freq = token_freq;
    }
    if (min_doc_length > doc_length) {
      min_doc_length = doc_length;
    }
    bool is_dominated = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_dominated && !use_fallback_pair
        && j < frontier_pairs_.count(); ++j) {
      const ObInvIdxBM25ParamPair &old_pair = frontier_pairs_.at(j);
      const uint64_t old_token_freq = old_pair.token_freq_;
      const uint64_t old_doc_length = old_pair.doc_length_;
      if (old_token_freq >= token_freq && old_doc_length <= doc_length) {
        is_dominated = true;
      } else if (token_freq >= old_token_freq && doc_length <= old_doc_length) {
        frontier_pairs_.at(j) = frontier_pairs_.at(frontier_pairs_.count() - 1);
        frontier_pairs_.pop_back();
        --j;
      }
    }
    if (OB_FAIL(ret) || is_dominated || use_fallback_pair) {
    } else if (OB_UNLIKELY(token_freq > UINT32_MAX || doc_length > UINT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("bm25 param overflows", K(ret), K(token_freq), K(doc_length));
    } else if (frontier_pairs_.count() >= max_pair_count_) {
      use_fallback_pair = true;
    } else {
      ObInvIdxBM25ParamPair pair;
      pair.token_freq_ = static_cast<uint32_t>(token_freq);
      pair.doc_length_ = static_cast<uint32_t>(doc_length);
      if (OB_FAIL(frontier_pairs_.push_back(pair))) {
        LOG_WARN("failed to push frontier pair", K(ret), K(pair));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (use_fallback_pair) {
    reuse();
    if (OB_UNLIKELY(max_token_freq > UINT32_MAX || min_doc_length > UINT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fallback param overflows", K(ret), K(max_token_freq), K(min_doc_length));
    } else {
      ObInvIdxBM25ParamPair pair;
      pair.token_freq_ = static_cast<uint32_t>(max_token_freq);
      pair.doc_length_ = static_cast<uint32_t>(min_doc_length);
      if (OB_FAIL(frontier_pairs_.push_back(pair))) {
        LOG_WARN("failed to push fallback pair", K(ret), K(pair));
      }
    }
  }
  if (OB_SUCC(ret)) {
    frontier_pairs = &frontier_pairs_;
  }
  return ret;
}

void ObInvIdxBM25MaxScoreAggregator::reuse()
{
  frontier_pairs_.clear();
}

void ObInvIdxBM25MaxScoreAggregator::reset()
{
  allocator_ = nullptr;
  max_pair_count_ = 0;
  frontier_pairs_.reset();
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase

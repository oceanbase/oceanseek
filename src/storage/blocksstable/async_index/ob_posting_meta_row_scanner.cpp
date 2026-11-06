/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_posting_meta_row_scanner.h"
#include <algorithm>

namespace oceanbase
{
namespace blocksstable
{

static const int64_t AGG_HEADER_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));
static const int64_t AGG_ENTRY_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));
static const int64_t AGG_PAIR_SIZE = static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));

ObPostingMetaRowScanner::ObPostingMetaRowScanner()
  : header_(nullptr),
    entries_(nullptr),
    frontier_pool_(nullptr),
    frontier_pool_size_(0),
    cur_idx_(0),
    has_max_score_(false),
    is_inited_(false),
    is_opened_(false)
{
}

int ObPostingMetaRowScanner::init(const bool has_max_score)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    has_max_score_ = has_max_score;
    is_inited_ = true;
  }
  return ret;
}

int ObPostingMetaRowScanner::parse_agg_data(
    const char *agg_buf, const int64_t agg_size,
    const ObInvIdxAggDataHeader *&header, const ObInvIdxAggDataEntry *&entries,
    const char *&frontier_pool, int64_t &frontier_pool_size)
{
  int ret = OB_SUCCESS;
  header = nullptr;
  entries = nullptr;
  frontier_pool = nullptr;
  frontier_pool_size = 0;
  if (OB_ISNULL(agg_buf) || OB_UNLIKELY(agg_size < AGG_HEADER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agg data buffer", K(ret), KP(agg_buf), K(agg_size));
  } else {
    const ObInvIdxAggDataHeader *parsed_header = reinterpret_cast<const ObInvIdxAggDataHeader *>(agg_buf);
    const int64_t block_count = parsed_header->logical_block_count_;
    const int64_t entries_size = block_count * AGG_ENTRY_SIZE;
    const int64_t pool_size = static_cast<int64_t>(parsed_header->total_size_) - AGG_HEADER_SIZE - entries_size;
    if (OB_UNLIKELY(ObInvIdxAggDataHeader::VERSION != parsed_header->version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg data header version", K(ret), K(parsed_header->version_));
    } else if (OB_UNLIKELY(block_count <= 0
        || static_cast<int64_t>(parsed_header->total_size_) > agg_size
        || pool_size < 0
        || (pool_size % AGG_PAIR_SIZE) != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid agg data layout", K(ret), K(agg_size), K(block_count),
          K(pool_size), KP(parsed_header), K(parsed_header->total_size_));
    } else {
      header = parsed_header;
      entries = reinterpret_cast<const ObInvIdxAggDataEntry *>(agg_buf + AGG_HEADER_SIZE);
      frontier_pool = agg_buf + AGG_HEADER_SIZE + entries_size;
      frontier_pool_size = pool_size;
    }
  }
  return ret;
}

int ObPostingMetaRowScanner::open(const char *agg_buf, const int64_t agg_size)
{
  int ret = OB_SUCCESS;
  const ObInvIdxAggDataHeader *header = nullptr;
  const ObInvIdxAggDataEntry *entries = nullptr;
  const char *frontier_pool = nullptr;
  int64_t frontier_pool_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(parse_agg_data(agg_buf, agg_size, header, entries, frontier_pool, frontier_pool_size))) {
    LOG_WARN("failed to parse agg data", K(ret));
  } else {
    header_ = header;
    entries_ = entries;
    frontier_pool_ = frontier_pool;
    frontier_pool_size_ = frontier_pool_size;
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObPostingMetaRowScanner::get_block_frontier(
    const ObInvIdxAggDataEntry &entry, ObPostingBlockMaxScore &frontier) const
{
  int ret = OB_SUCCESS;
  frontier.reset();
  if (has_max_score_ && entry.frontier_count_ > 0) {
    const int64_t frontier_end = static_cast<int64_t>(entry.frontier_offset_)
        + static_cast<int64_t>(entry.frontier_count_) * AGG_PAIR_SIZE;
    if (OB_UNLIKELY(frontier_end > frontier_pool_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frontier slice out of pool", K(ret), K(entry.frontier_offset_),
          K(entry.frontier_count_), K_(frontier_pool_size));
    } else {
      frontier.pairs_ = reinterpret_cast<const ObInvIdxBM25ParamPair *>(
          frontier_pool_ + entry.frontier_offset_);
      frontier.pair_count_ = entry.frontier_count_;
    }
  }
  return ret;
}

int ObPostingMetaRowScanner::get_cur_block_meta(ObPostingBlockMeta &meta) const
{
  return get_block_meta_at(cur_idx_, meta);
}

int ObPostingMetaRowScanner::get_block_meta_at(
    const int64_t block_idx,
    ObPostingBlockMeta &meta) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret));
  } else if (OB_UNLIKELY(block_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting block index", K(ret), K(block_idx));
  } else if (block_idx >= header_->logical_block_count_) {
    ret = OB_ITER_END;
  } else {
    // 按指定下标读取 entry，不修改生产 scanner 的 cur_idx_。
    const ObInvIdxAggDataEntry &entry = entries_[block_idx];
    meta.min_id_ = entry.min_doc_id_;
    meta.max_id_ = entry.max_doc_id_;
    meta.doc_count_ = entry.doc_count_;
    meta.idx_ = block_idx;
    meta.locator_.block_idx_ = entry.block_idx_;
    meta.locator_.value_micro_offset_ = header_->posting_value_offset_;
    meta.locator_.value_micro_size_ = header_->posting_value_size_;
    if (OB_FAIL(get_block_frontier(entry, meta.block_max_))) {
      LOG_WARN("failed to get posting block frontier", K(ret), K(block_idx));
    }
  }
  return ret;
}

int ObPostingMetaRowScanner::get_frontier_span(
    const int64_t start_block_idx, const uint64_t first_id,
    const uint64_t end_id, ObPostingFrontierSpan &span) const
{
  int ret = OB_SUCCESS;
  span = ObPostingFrontierSpan();
  const ObInvIdxBM25ParamPair *frontier_begin = nullptr;
  const ObInvIdxBM25ParamPair *frontier_end = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret), K(start_block_idx), K(first_id), K(end_id));
  } else if (OB_UNLIKELY(start_block_idx < 0 || first_id > end_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid frontier span range", K(ret), K(start_block_idx), K(first_id), K(end_id));
  } else if (start_block_idx >= header_->logical_block_count_) {
    ret = OB_ITER_END;
  } else {
    for (int64_t i = start_block_idx; OB_SUCC(ret) && !span.range_covered_
        && span.score_available_ && i < header_->logical_block_count_; ++i) {
      const ObInvIdxAggDataEntry &entry = entries_[i];
      if (OB_UNLIKELY(0 == entry.doc_count_ || entry.min_doc_id_ > entry.max_doc_id_
          || (span.visited_block_count_ > 0 && entry.min_doc_id_ <= span.last_max_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid frontier span block ordering", K(ret), K(i),
            K(entry.min_doc_id_), K(entry.max_doc_id_), K(span.last_max_id_));
      } else {
        if (0 == span.visited_block_count_) {
          span.first_min_id_ = entry.min_doc_id_;
        }
        ++span.visited_block_count_;
        span.last_max_id_ = entry.max_doc_id_;
        if (entry.min_doc_id_ > end_id) {
          span.range_covered_ = true;
        } else if (entry.max_doc_id_ >= first_id) {
          if (!has_max_score_ || 0 == entry.frontier_count_) {
            span.score_available_ = false;
          } else {
            ObPostingBlockMaxScore frontier;
            if (OB_FAIL(get_block_frontier(entry, frontier))) {
              LOG_WARN("failed to get frontier span slice", K(ret), K(i));
            } else if (nullptr != frontier_end && frontier.pairs_ != frontier_end) {
              const int64_t expected_offset = reinterpret_cast<const char *>(frontier_end) - frontier_pool_;
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("non-contiguous frontier pool", K(ret), K(i),
                  K(entry.frontier_offset_), K(expected_offset));
            } else {
              if (nullptr == frontier_begin) {
                frontier_begin = frontier.pairs_;
              }
              frontier_end = frontier.pairs_ + frontier.pair_count_;
              span.pair_count_ += frontier.pair_count_;
            }
          }
        }
        if (entry.max_doc_id_ >= end_id) {
          span.range_covered_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      span.pairs_ = frontier_begin;
    }
  }
  return ret;
}

int ObPostingMetaRowScanner::get_block_idx_range(int64_t &first_block_idx, int64_t &last_block_idx) const
{
  int ret = OB_SUCCESS;
  first_block_idx = -1;
  last_block_idx = -1;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret));
  } else {
    first_block_idx = entries_[0].block_idx_;
    last_block_idx = entries_[header_->logical_block_count_ - 1].block_idx_;
  }
  return ret;
}

int ObPostingMetaRowScanner::match_cached_block_meta(
    const char *agg_buf, const int64_t agg_size, const ObPostingBlockMeta &cached,
    const uint64_t first_id, const uint64_t end_id, bool &matched) const
{
  int ret = OB_SUCCESS;
  const ObInvIdxAggDataHeader *header = nullptr;
  const ObInvIdxAggDataEntry *entries = nullptr;
  const char *frontier_pool = nullptr;
  int64_t frontier_pool_size = 0;
  matched = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret), K(first_id), K(end_id));
  } else if (OB_UNLIKELY(first_id > end_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cached metadata range", K(ret), K(first_id), K(end_id));
  } else if (OB_FAIL(parse_agg_data(agg_buf, agg_size, header, entries, frontier_pool, frontier_pool_size))) {
    LOG_WARN("failed to parse pinned agg data", K(ret));
  } else if (header != header_ || entries != entries_ || frontier_pool != frontier_pool_
      || frontier_pool_size != frontier_pool_size_) {
  } else if (cur_idx_ < 0 || cur_idx_ >= header->logical_block_count_) {
    ret = OB_ITER_END;
  } else if (cached.is_valid() && cur_idx_ == cached.idx_) {
    const ObInvIdxAggDataEntry &entry = entries[cur_idx_];
    const bool same_block = entry.min_doc_id_ == cached.min_id_
        && entry.max_doc_id_ == cached.max_id_
        && entry.doc_count_ == cached.doc_count_
        && entry.block_idx_ == cached.locator_.block_idx_
        && header->posting_value_offset_ == cached.locator_.value_micro_offset_
        && header->posting_value_size_ == cached.locator_.value_micro_size_;
    ObPostingBlockMaxScore frontier;
    if (!same_block) {
    } else if (entry.min_doc_id_ > end_id || entry.max_doc_id_ < first_id) {
      matched = true;
    } else if (OB_FAIL(get_block_frontier(entry, frontier))) {
      LOG_WARN("failed to check cached frontier", K(ret), K_(cur_idx));
    } else {
      matched = frontier.pairs_ == cached.block_max_.pairs_
          && frontier.pair_count_ == cached.block_max_.pair_count_;
    }
  }
  return ret;
}

int ObPostingMetaRowScanner::next_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret));
  } else if (++cur_idx_ >= header_->logical_block_count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObPostingMetaRowScanner::advance_to(const uint64_t target, bool &beyond)
{
  int ret = OB_SUCCESS;
  beyond = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta scanner not opened", K(ret));
  } else {
    const int64_t block_count = header_->logical_block_count_;
    const int64_t start_idx = cur_idx_ < 0 ? 0 : cur_idx_;
    if (start_idx >= block_count) {
      cur_idx_ = block_count;
      beyond = true;
    } else if (entries_[start_idx].max_doc_id_ >= target) {
      cur_idx_ = start_idx;
    } else if (start_idx + 1 < block_count
        && entries_[start_idx + 1].max_doc_id_ >= target) {
      cur_idx_ = start_idx + 1;
    } else {
      // Galloping search from start_idx + 2 to locate a tight [left, right) window.
      int64_t left = start_idx + 2;
      int64_t right = block_count;
      const int64_t base = left;
      int64_t offset = 1;
      while (left < block_count) {
        int64_t probe = base + offset - 1;
        probe = MIN(probe, block_count - 1);
        if (entries_[probe].max_doc_id_ >= target) {
          right = probe + 1;
          break;
        } else {
          left = probe + 1;
          if (left >= block_count) {
            break;
          }
          if (offset > (INT64_MAX >> 1)) {
            offset = INT64_MAX;
          } else {
            offset <<= 1;
          }
        }
      }
      if (left >= block_count) {
        cur_idx_ = block_count;
        beyond = true;
      } else {
        const ObInvIdxAggDataEntry *begin = entries_ + left;
        const ObInvIdxAggDataEntry *end = entries_ + right;
        const ObInvIdxAggDataEntry *it = std::lower_bound(
            begin, end, target, ObPostingMetaMaxDocIdLess());
        cur_idx_ = it - entries_;
      }
    }
  }
  return ret;
}

void ObPostingMetaRowScanner::reuse()
{
  header_ = nullptr;
  entries_ = nullptr;
  frontier_pool_ = nullptr;
  frontier_pool_size_ = 0;
  cur_idx_ = 0;
  is_opened_ = false;
}

void ObPostingMetaRowScanner::reset()
{
  reuse();
  has_max_score_ = false;
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase

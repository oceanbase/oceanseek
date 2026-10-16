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

int ObPostingMetaRowScanner::open(const char *agg_buf, const int64_t agg_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(agg_buf) || OB_UNLIKELY(agg_size < AGG_HEADER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agg data buffer", K(ret), KP(agg_buf), K(agg_size));
  } else {
    const ObInvIdxAggDataHeader *header = reinterpret_cast<const ObInvIdxAggDataHeader *>(agg_buf);
    const int64_t block_count = header->logical_block_count_;
    const int64_t entries_size = block_count * AGG_ENTRY_SIZE;
    const int64_t frontier_pool_size = static_cast<int64_t>(header->total_size_) - AGG_HEADER_SIZE - entries_size;
    if (OB_UNLIKELY(ObInvIdxAggDataHeader::VERSION != header->version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg data header version", K(ret), K(header->version_));
    } else if (OB_UNLIKELY(block_count <= 0
        || static_cast<int64_t>(header->total_size_) > agg_size
        || frontier_pool_size < 0
        || (frontier_pool_size % AGG_PAIR_SIZE) != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid agg data layout", K(ret), K(agg_size), K(block_count),
          K(frontier_pool_size), KP(header), K(header->total_size_));
    } else {
      header_ = header;
      entries_ = reinterpret_cast<const ObInvIdxAggDataEntry *>(agg_buf + AGG_HEADER_SIZE);
      frontier_pool_ = agg_buf + AGG_HEADER_SIZE + entries_size;
      frontier_pool_size_ = frontier_pool_size;
      cur_idx_ = 0;
      is_opened_ = true;
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
    if (has_max_score_ && entry.frontier_count_ > 0) {
      // 根据当前 entry 的 offset/count 借用 frontier pair 连续切片。
      const int64_t frontier_end = static_cast<int64_t>(entry.frontier_offset_)
          + static_cast<int64_t>(entry.frontier_count_) * AGG_PAIR_SIZE;
      if (OB_UNLIKELY(frontier_end > frontier_pool_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("frontier slice out of pool", K(ret), K(block_idx),
            K(entry.frontier_offset_), K(entry.frontier_count_), K_(frontier_pool_size));
      } else {
        meta.block_max_.pairs_ = reinterpret_cast<const ObInvIdxBM25ParamPair *>(
            frontier_pool_ + entry.frontier_offset_);
        meta.block_max_.pair_count_ = entry.frontier_count_;
      }
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

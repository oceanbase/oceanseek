/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_row_logical_block_iter.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

ObInvRowLogicalBlockIterator::ObInvRowLogicalBlockIterator()
  : decoder_(),
    value_reader_(),
    row_desc_(),
    allocator_(nullptr),
    curr_row_(),
    row_count_(0),
    out_col_cnt_(0),
    cursor_(-1),
    has_value_block_(false),
    is_opened_(false)
{
}

ObInvRowLogicalBlockIterator::~ObInvRowLogicalBlockIterator()
{
  reset();
}

int ObInvRowLogicalBlockIterator::open(
    const ObPostingBlockDesc &posting_block,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    const ObPostingValueBlockDesc *value_block)
{
  int ret = OB_SUCCESS;
  reset();
  const int64_t request_cnt = read_info.get_request_count();
  if (OB_UNLIKELY(!posting_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting block desc", K(ret), K(posting_block));
  } else if (OB_UNLIKELY(request_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid read info request count", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inv idx row desc", K(ret), K(row_desc));
  } else if (OB_UNLIKELY(row_desc.doc_id_col_idx_ >= request_cnt
                         || row_desc.token_freq_col_idx_ >= request_cnt
                         || row_desc.doc_length_col_idx_ >= request_cnt
                         || row_desc.pos_list_col_idx_ >= request_cnt
                         || row_desc.nested_key_col_idx_ >= request_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row desc col idx out of request range", K(ret),
        K(request_cnt), K(row_desc));
  } else if (nullptr != value_block && OB_UNLIKELY(!value_block->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting value block desc", K(ret), KPC(value_block));
  } else if (OB_FAIL(decoder_.init(posting_block.buf_, posting_block.size_, allocator))) {
    STORAGE_LOG(WARN, "failed to init posting block decoder", K(ret), K(posting_block));
  } else if (OB_UNLIKELY((row_count_ = decoder_.get_row_count()) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty posting block", K(ret), K_(row_count));
  } else if (nullptr != value_block
             && OB_FAIL(value_reader_.init(value_block->buf_, value_block->size_))) {
    STORAGE_LOG(WARN, "failed to init posting value block reader", K(ret), KPC(value_block));
  } else if (nullptr != value_block
             && OB_UNLIKELY(value_block->row_count_ != row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting value block row count mismatch", K(ret),
        K_(row_count), "value_row_count", value_block->row_count_);
  } else if (OB_FAIL(curr_row_.init(allocator, request_cnt))) {
    STORAGE_LOG(WARN, "failed to init materialize row", K(ret), K(request_cnt));
  } else {
    row_desc_ = row_desc;
    allocator_ = &allocator;
    out_col_cnt_ = request_cnt;
    cursor_ = -1; // 定位在第一行之前；首次 next() 落到第 0 行
    has_value_block_ = (nullptr != value_block);
    is_opened_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvRowLogicalBlockIterator::materialize_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  // 一次性解码逐行标量；相关度参数仅在存在时才获取
  uint64_t doc_id = 0;
  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  const bool has_relevance = decoder_.has_relevance_params();
  if (OB_FAIL(decoder_.get_doc_id(row_idx, doc_id))) {
    STORAGE_LOG(WARN, "failed to get doc_id", K(ret), K(row_idx));
  } else if (has_relevance
             && OB_FAIL(decoder_.get_relevance_params(row_idx, token_freq, doc_length))) {
    STORAGE_LOG(WARN, "failed to get relevance params", K(ret), K(row_idx));
  }
  // 输出为完整 schema 存储序：按 row_desc 各 FTS 列位置填值，其余位置
  // （term / trans_version / sql_seq 等）保持 nop，由上层填充。
  for (int64_t i = 0; OB_SUCC(ret) && i < out_col_cnt_; ++i) {
    ObStorageDatum &datum = curr_row_.storage_datums_[i];
    datum.set_nop();
    if (i == row_desc_.doc_id_col_idx_) {
      datum.set_uint(doc_id);
    } else if (i == row_desc_.token_freq_col_idx_) {
      if (has_relevance) {
        datum.set_uint(token_freq);
      }
    } else if (i == row_desc_.doc_length_col_idx_) {
      if (has_relevance) {
        datum.set_uint(doc_length);
      }
    } else if (i == row_desc_.pos_list_col_idx_) {
      // 列存在性由 schema 确定；由 get_position_list 报告实际不匹配，
      // 而不是在此处做守卫检查。
      if (has_value_block_) {
        ObDatum value;
        if (OB_FAIL(value_reader_.get_position_list(row_idx, value))) {
          STORAGE_LOG(WARN, "failed to get position_list", K(ret), K(row_idx));
        } else {
          datum.shallow_copy_from_datum(value);
        }
      }
    } else if (i == row_desc_.nested_key_col_idx_) {
      if (has_value_block_) {
        ObDatum value;
        if (OB_FAIL(value_reader_.get_nested_key(row_idx, value))) {
          STORAGE_LOG(WARN, "failed to get nested_key", K(ret), K(row_idx));
        } else {
          datum.shallow_copy_from_datum(value);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_row_.count_ = static_cast<uint16_t>(out_col_cnt_);
  }
  return ret;
}

int ObInvRowLogicalBlockIterator::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "logical block iterator not opened", K(ret));
  } else if (cursor_ + 1 >= row_count_) {
    cursor_ = row_count_; // 停在末尾
    ret = OB_ITER_END;
  } else {
    ++cursor_;
    if (OB_FAIL(materialize_row(cursor_))) {
      STORAGE_LOG(WARN, "failed to materialize row", K(ret), K_(cursor));
    }
  }
  return ret;
}

const ObDatumRow *ObInvRowLogicalBlockIterator::get_curr_row() const
{
  const ObDatumRow *row = nullptr;
  if (is_opened_ && cursor_ >= 0 && cursor_ < row_count_) {
    row = &curr_row_;
  }
  return row;
}

int ObInvRowLogicalBlockIterator::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_FAIL(next())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to advance logical block iterator", K(ret));
    }
  } else {
    row = get_curr_row();
  }
  return ret;
}

int ObInvRowLogicalBlockIterator::get_curr_doc_id(uint64_t &doc_id) const
{
  int ret = OB_SUCCESS;
  doc_id = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "logical block iterator not opened", K(ret));
  } else if (cursor_ < 0 || cursor_ >= row_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(decoder_.get_doc_id(cursor_, doc_id))) {
    STORAGE_LOG(WARN, "failed to read current doc_id", K(ret), K_(cursor));
  }
  return ret;
}

int ObInvRowLogicalBlockIterator::advance_to(const uint64_t target_doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "logical block iterator not opened", K(ret));
  } else {
    // 从当前游标开始搜索（首次 next() 前从第 0 行开始）；绝不回退
    const int64_t start = (cursor_ < 0) ? 0 : cursor_;
    if (start >= row_count_) {
      cursor_ = row_count_;
      ret = OB_ITER_END;
    } else {
      int64_t target_idx = 0;
      ret = decoder_.lower_bound(target_doc_id, start, row_count_, target_idx);
      if (OB_BEYOND_THE_RANGE == ret) {
        // 此 block 中没有达到 target 的行：将游标停在末尾
        cursor_ = row_count_;
        ret = OB_ITER_END;
      } else if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "failed to lower_bound doc_id", K(ret),
            K(target_doc_id), K(start), K_(row_count));
      } else if (OB_FAIL(materialize_row(target_idx))) {
        STORAGE_LOG(WARN, "failed to materialize landed row", K(ret), K(target_idx));
      } else {
        cursor_ = target_idx;
      }
    }
  }
  return ret;
}

void ObInvRowLogicalBlockIterator::reuse()
{
  cursor_ = -1; // 回绕到第一行之前
}

void ObInvRowLogicalBlockIterator::reset()
{
  decoder_.reset();
  value_reader_.reuse();
  curr_row_.reset();
  row_desc_.reset();
  allocator_ = nullptr;
  row_count_ = 0;
  out_col_cnt_ = 0;
  cursor_ = -1;
  has_value_block_ = false;
  is_opened_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase

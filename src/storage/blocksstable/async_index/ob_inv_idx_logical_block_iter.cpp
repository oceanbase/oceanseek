/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_logical_block_iter.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObInvIdxLogicalBlockIterator::ObInvIdxLogicalBlockIterator()
  : payload_reader_(),
    value_reader_(),
    has_value_(false),
    logical_block_metas_(nullptr),
    block_count_(0),
    curr_pos_(-1),
    decoded_block_idx_(-1),
    decode_alloc_(ObModIds::OB_MODULE_PAGE_ALLOCATOR, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    decode_on_demand_decoder_(),
    has_meta_(false),
    is_opened_(false)
{
}

ObInvIdxLogicalBlockIterator::~ObInvIdxLogicalBlockIterator()
{
  reset();
}

int ObInvIdxLogicalBlockIterator::open(
    const ObMicroBlockData &posting_block,
    const ObIArray<ObInvIdxLogicalBlockMeta> *logical_block_metas,
    const ObMicroBlockData *value_block)
{
  int ret = OB_SUCCESS;
  const char *payload_buf = posting_block.get_buf();
  const int64_t payload_size = posting_block.get_buf_size();
  reset();
  if (OB_ISNULL(payload_buf) || OB_UNLIKELY(payload_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index micro block payload", K(ret), K(posting_block));
  } else if (OB_FAIL(payload_reader_.init_payload(payload_buf, payload_size))) {
    STORAGE_LOG(WARN, "failed to init inverted index micro block reader", K(ret), K(posting_block));
  } else if (nullptr != value_block
      && OB_UNLIKELY(OB_ISNULL(value_block->get_buf()) || value_block->get_buf_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid value micro block payload", K(ret), KPC(value_block));
  } else if (nullptr != value_block
      && OB_FAIL(value_reader_.init_payload(value_block->get_buf(), value_block->get_buf_size()))) {
    STORAGE_LOG(WARN, "failed to init inverted index value micro block reader",
        K(ret), KPC(value_block));
  } else {
    block_count_ = payload_reader_.get_block_count();
    if (nullptr != value_block) {
      // value 微块必须与 posting-data 微块对应：相同
      // 的 logical-block 数量，以便 block_idx 能切片出行对齐的 value 子 block。
      if (OB_UNLIKELY(value_reader_.get_block_count() != block_count_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "value micro block count mismatches posting data",
            K(ret), K(block_count_), "value_count", value_reader_.get_block_count());
      } else {
        has_value_ = true;
      }
    }
    // 校验注入的 meta：block_idx 需在 [0, block_count_) 范围内，且严格
    // 递增（这样游标可以按物理顺序遍历它们）。
    if (OB_SUCC(ret) && nullptr != logical_block_metas) {
      const int64_t meta_cnt = logical_block_metas->count();
      if (OB_UNLIKELY(meta_cnt <= 0 || meta_cnt > block_count_)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "logical block meta count out of range", K(ret),
            K(meta_cnt), K_(block_count));
      } else {
        // 此处只消费范围字段；frontier_pairs_（为 BM25 准备的内容）
        // 并非必需，因此不使用 ObInvIdxLogicalBlockMeta::is_valid()。
        int64_t prev_block_idx = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < meta_cnt; ++i) {
          const ObInvIdxLogicalBlockMeta &meta = logical_block_metas->at(i);
          if (OB_UNLIKELY(meta.block_idx_ < 0 || meta.block_idx_ >= block_count_)) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "logical block meta block_idx out of range", K(ret),
                K(i), K(meta.block_idx_), K_(block_count));
          } else if (OB_UNLIKELY(meta.block_idx_ <= prev_block_idx)) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "logical block meta block_idx not strictly increasing",
                K(ret), K(i), K(meta.block_idx_), K(prev_block_idx));
          } else if (OB_UNLIKELY(meta.doc_count_ <= 0 || meta.min_doc_id_ > meta.max_doc_id_)) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "invalid logical block meta range", K(ret), K(i), K(meta));
          } else {
            prev_block_idx = meta.block_idx_;
          }
        }
        if (OB_SUCC(ret)) {
          logical_block_metas_ = logical_block_metas;
          has_meta_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      curr_pos_ = -1; // 定位在第一个 block 之前；首次 next() 落到 block 0
      decoded_block_idx_ = -1;
      is_opened_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::locate_curr_block_(const char *&buf, int64_t &size) const
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  size = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block iterator not opened", K(ret));
  } else if (OB_UNLIKELY(curr_pos_ < 0 || curr_pos_ >= domain_count_())) {
    ret = OB_ITER_END;
  } else {
    const int64_t phys = curr_physical_idx_();
    if (OB_FAIL(payload_reader_.get_payload(phys, buf, size))) {
      STORAGE_LOG(WARN, "failed to get logical block payload", K(ret), K(phys));
    }
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block iterator not opened", K(ret));
  } else if (curr_pos_ + 1 >= domain_count_()) {
    curr_pos_ = domain_count_(); // 停在末尾
    ret = OB_ITER_END;
  } else {
    ++curr_pos_;
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::fill_curr_posting_block_desc_(ObPostingBlockDesc &posting_block)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  int64_t size = 0;
  uint64_t min_id = 0;
  uint64_t max_id = 0;
  int64_t doc_count = 0;
  posting_block.reset();
  if (OB_FAIL(locate_curr_block_(buf, size))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to locate current logical block", K(ret), K_(curr_pos));
    }
  } else {
    posting_block.buf_ = buf;
    posting_block.size_ = size;
    // 范围为尽力提供：解码失败不应阻止产出 payload，
    // 且范围仍可通过 get_curr_logical_block_range 查询。
    if (OB_SUCCESS == get_curr_logical_block_range(min_id, max_id, doc_count)) {
      posting_block.doc_count_ = doc_count;
      posting_block.min_id_ = min_id;
      posting_block.max_id_ = max_id;
    }
    if (has_meta_) {
      const ObInvIdxLogicalBlockMeta &meta = logical_block_metas_->at(curr_pos_);
      posting_block.total_doc_length_ = meta.total_doc_length_;
      if (meta.frontier_pairs_.count() > 0) {
        posting_block.max_score_pairs_ = &meta.frontier_pairs_;
      }
    }
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::get_curr_block(ObPostingBlockDesc &posting_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block iterator not opened", K(ret));
    posting_block.reset();
  } else if (OB_FAIL(fill_curr_posting_block_desc_(posting_block))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to fill current posting block desc", K(ret), K_(curr_pos));
    }
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::get_next_block(ObPostingBlockDesc &posting_block)
{
  int ret = OB_SUCCESS;
  posting_block.reset();
  if (OB_FAIL(next())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to advance micro block iterator", K(ret));
    }
  } else if (OB_FAIL(get_curr_block(posting_block))) {
    STORAGE_LOG(WARN, "failed to get current logical block", K(ret), K_(curr_pos));
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::decode_curr_block_range_(
    uint64_t &min_doc_id, uint64_t &max_doc_id, int64_t &doc_count)
{
  int ret = OB_SUCCESS;
  min_doc_id = 0;
  max_doc_id = 0;
  doc_count = 0;
  const char *buf = nullptr;
  int64_t size = 0;
  const int64_t phys = curr_physical_idx_();
  if (OB_FAIL(locate_curr_block_(buf, size))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to locate current logical block for range decode", K(ret));
    }
  } else if (decoded_block_idx_ != phys) {
    // 将按需解码器重新绑定到当前 block；同时复用 arena，这样
    // 遍历大量无 meta 的 block 时不会每次都积攒临时 block 空间。
    decode_on_demand_decoder_.reuse();
    decode_alloc_.reuse();
    if (OB_FAIL(decode_on_demand_decoder_.init(buf, size, decode_alloc_))) {
      STORAGE_LOG(WARN, "failed to init on-demand posting block decoder", K(ret), K(size));
    } else {
      decoded_block_idx_ = phys;
    }
  }
  if (OB_SUCC(ret)) {
    doc_count = decode_on_demand_decoder_.get_row_count();
    if (OB_UNLIKELY(doc_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected empty logical block", K(ret), K(doc_count));
    } else if (OB_FAIL(decode_on_demand_decoder_.get_doc_id(0, min_doc_id))) {
      STORAGE_LOG(WARN, "failed to peek first row doc_id", K(ret));
    } else if (OB_FAIL(decode_on_demand_decoder_.get_doc_id(doc_count - 1, max_doc_id))) {
      STORAGE_LOG(WARN, "failed to peek last row doc_id", K(ret));
    }
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::get_curr_value_block(ObPostingValueBlockDesc &value_block)
{
  int ret = OB_SUCCESS;
  value_block.reset();
  const char *value_buf = nullptr;
  int64_t value_size = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block iterator not opened", K(ret));
  } else if (OB_UNLIKELY(curr_pos_ < 0 || curr_pos_ >= domain_count_())) {
    ret = OB_ITER_END;
  } else if (!has_value_) {
    // 此 sstable 没有 phrase 列 —— 调用方跳过 position_list / nested_key
  } else if (OB_FAIL(value_reader_.get_payload(curr_physical_idx_(), value_buf, value_size))) {
    STORAGE_LOG(WARN, "failed to get value sub-block payload", K(ret), K_(curr_pos));
  } else if (OB_UNLIKELY(value_size < static_cast<int64_t>(sizeof(ObPostingValueBlockHeader)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "value sub-block smaller than its header", K(ret), K(value_size), K_(curr_pos));
  } else {
    // row_count 直接从 value 子 block header 获取，无需
    // 为统计行数而解码 posting block。
    const ObPostingValueBlockHeader *value_header =
        reinterpret_cast<const ObPostingValueBlockHeader *>(value_buf);
    value_block.buf_ = value_buf;
    value_block.size_ = value_size;
    value_block.row_count_ = value_header->row_count_;
  }
  return ret;
}

int ObInvIdxLogicalBlockIterator::get_curr_logical_block_range(
    uint64_t &min_doc_id, uint64_t &max_doc_id, int64_t &doc_count)
{
  int ret = OB_SUCCESS;
  min_doc_id = 0;
  max_doc_id = 0;
  doc_count = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "micro block iterator not opened", K(ret));
  } else if (OB_UNLIKELY(curr_pos_ < 0 || curr_pos_ >= domain_count_())) {
    ret = OB_ITER_END;
  } else if (has_meta_) {
    // 零解码路径：curr_pos_ 直接索引 term 的 meta 子集。
    const ObInvIdxLogicalBlockMeta &meta = logical_block_metas_->at(curr_pos_);
    min_doc_id = meta.min_doc_id_;
    max_doc_id = meta.max_doc_id_;
    doc_count = meta.doc_count_;
  } else {
    if (OB_FAIL(decode_curr_block_range_(min_doc_id, max_doc_id, doc_count))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to decode current logical block range", K(ret), K_(curr_pos));
      }
    }
  }
  return ret;
}

void ObInvIdxLogicalBlockIterator::reuse()
{
  curr_pos_ = -1; // 回绕到第一个 block 之前
  decoded_block_idx_ = -1;
  decode_on_demand_decoder_.reuse();
  decode_alloc_.reset();
}

void ObInvIdxLogicalBlockIterator::reset()
{
  decode_on_demand_decoder_.reset();
  decode_alloc_.reset();
  payload_reader_.reuse();
  value_reader_.reuse();
  has_value_ = false;
  block_count_ = 0;
  curr_pos_ = -1;
  logical_block_metas_ = nullptr;
  has_meta_ = false;
  decoded_block_idx_ = -1;
  is_opened_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase

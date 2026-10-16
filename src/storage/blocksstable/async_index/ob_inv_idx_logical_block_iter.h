/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_LOGICAL_BLOCK_ITER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_LOGICAL_BLOCK_ITER_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{

// 倒排索引读栈的中间层（MICRO -> LOGICAL_BLOCK）：将一份解压后的 posting-data
// 微块 payload 按其偏移表切分为 logical block，每次产出一个借出的 ObPostingBlockDesc。
//
// 注入的 meta（ObInvIdxLogicalBlockMeta）承担两个角色：范围来源
// （get_curr_logical_block_range 无需解码即可返回）和迭代域
// （单个 term 的 block 子集，block_idx 在 [0, block_count) 内严格递增）。
// 无 meta 时（compaction / 全扫），迭代域为所有物理 block，
// 范围查询按需解码当前 block。
//
// 生命周期：输入 payload / meta 是借出的，必须比迭代器存活更久；
// 产出的视图在下一次 next()/get_next_block()/reuse()/reset() 时失效。
class ObInvIdxLogicalBlockIterator final
{
public:
  ObInvIdxLogicalBlockIterator();
  ~ObInvIdxLogicalBlockIterator();

  // 在一份解压后的 posting-data 微块上打开。当 logical_block_metas
  // 非空时，它定义了迭代域（单个 term 的 block）。当
  // value_block 被提供时（支持 phrase 的 sstable），其 block_count 必须与 posting
  // block 相同；行对齐的 value 子 block 通过
  // get_curr_value_block() 暴露。Payload / meta 均为借出。
  int open(const ObMicroBlockData &posting_block,
           const common::ObIArray<ObInvIdxLogicalBlockMeta> *logical_block_metas = nullptr,
           const ObMicroBlockData *value_block = nullptr);

  // 将游标前进到下一个 logical block。最后一个 block 之后返回 OB_ITER_END。
  int next();

  // 当前 logical block（编码字节 + 范围 meta）。可重复读取，不前进。
  int get_curr_block(ObPostingBlockDesc &posting_block);

  // 融合的 next() + get_curr_block()。
  int get_next_block(ObPostingBlockDesc &posting_block);

  // 当前 block 的 (min_doc_id, max_doc_id, doc_count)：有注入 meta 时直接读取，
  // 否则按需解码（首/末行 doc_id peek）。不前进；可重复读取。
  int get_curr_logical_block_range(uint64_t &min_doc_id,
                                   uint64_t &max_doc_id,
                                   int64_t &doc_count);

  // 与当前 posting block 行对齐的 value 子 block（按 block_idx 切片）。
  // 若 open() 时未绑定 value 微块则保持 reset 状态。借出；不前进。
  int get_curr_value_block(ObPostingValueBlockDesc &value_block);
  OB_INLINE bool is_opened() const { return is_opened_; }
  OB_INLINE int64_t get_block_count() const { return block_count_; }
  OB_INLINE bool has_meta() const { return has_meta_; }
  // 游标是否已越过迭代域的最后一个 logical block。
  OB_INLINE bool is_iter_end() const { return !is_opened_ || curr_pos_ >= domain_count_(); }

  // 将游标回绕到第一个 block 之前，保留 payload / meta 绑定。
  void reuse();
  // 释放按需解码器，解绑 payload / meta。
  void reset();

  TO_STRING_KV(K_(is_opened), K_(block_count), K_(curr_pos), K_(has_meta));

private:
  // 当前迭代域内的 block：有注入 meta 时取 term 的 meta 个数，否则取所有物理 block 个数。
  OB_INLINE int64_t domain_count_() const
  {
    return has_meta_ ? logical_block_metas_->count() : block_count_;
  }
  // 迭代域位置 -> 物理 block 索引。仅当 curr_pos_ 在 [0, domain_count_()) 内有效。
  OB_INLINE int64_t curr_physical_idx_() const
  {
    return has_meta_ ? logical_block_metas_->at(curr_pos_).block_idx_ : curr_pos_;
  }
  // 定位当前 block 的原始字节视图；对 payload 做边界检查。
  int locate_curr_block_(const char *&buf, int64_t &size) const;
  int fill_curr_posting_block_desc_(ObPostingBlockDesc &posting_block);
  int decode_curr_block_range_(uint64_t &min_doc_id,
                               uint64_t &max_doc_id,
                               int64_t &doc_count);

private:
  ObInvIdxMicroBlockReader payload_reader_;                // 借出的 payload 布局读取器
  ObInvIdxMicroBlockReader value_reader_;                  // 可选的 value 微块布局读取器
  bool has_value_;
  // 当存在时，范围字段从此处读取，无需解码 block payload。
  const common::ObIArray<ObInvIdxLogicalBlockMeta> *logical_block_metas_;
  int64_t block_count_;
  int64_t curr_pos_;           // 迭代域游标；第一次 next() 之前为 -1
  int64_t decoded_block_idx_;  // 绑定到 decode_on_demand_decoder_ 的物理 block_idx，或 -1
  // 无 meta 注入时范围查询的回退状态。
  common::ObArenaAllocator decode_alloc_;
  ObPostingBlockDecoder decode_on_demand_decoder_;
  bool has_meta_;
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxLogicalBlockIterator);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_LOGICAL_BLOCK_ITER_H_

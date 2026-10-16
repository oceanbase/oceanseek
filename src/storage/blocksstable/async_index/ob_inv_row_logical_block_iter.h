/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_ROW_LOGICAL_BLOCK_ITER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_ROW_LOGICAL_BLOCK_ITER_H_

#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{
class ObITableReadInfo;
}
namespace blocksstable
{

// 倒排索引读栈的底层（LOGICAL_BLOCK -> ROW）：通过 ObPostingBlockDecoder
// 解码一个编码后的 posting block，将其中的行产出到内部的
// ObDatumRow 中。一个可选的行对齐 value block 提供
// position_list / nested_key 列。
//
// 输出行为完整 schema 存储序（multi-version 列序），宽度 =
// read_info.get_request_count()。列角色由 ObInvIdxRowDesc 决定（与 writer 侧
// ObInvIdxRowDesc::init 对称、同一坐标系）：输出位置 i 若命中
// doc_id_col_idx_ 填 doc_id、token_freq_col_idx_ 填 token_freq、
// doc_length_col_idx_ 填 doc_length（后两者仅相关度参数存在时）、
// pos_list_col_idx_ / nested_key_col_idx_ 从 value block 取值（仅 value block
// 存在时）；其余位置（term、trans_version/sql_seq 等 MV 额外列）保持 nop。
// term 列由上层 merge iter 用 curr_term_ 覆盖；MV 额外列由 merge 框架填充。
//
// 生命周期：当前 ObDatumRow 引用内部/输入缓冲区，
// 在下一次 next()/advance_to()/reuse()/reset() 后失效；调用方必须拷贝
// 以保留数据。read_info 为借出，必须比迭代器存活更久；row_desc 按值拷贝持有。
class ObInvRowLogicalBlockIterator final
{
public:
  ObInvRowLogicalBlockIterator();
  ~ObInvRowLogicalBlockIterator();

  // 在一个编码后的 posting (logical) block 上打开；输出行宽度由 read_info
  // 决定，列角色由 row_desc（schema 存储序）决定。当 value_block 被提供
  // （行对齐）时，它提供 position_list / nested_key，且其 row_count 必须等于
  // posting block 的 row_count。输入缓冲区为借出，必须比迭代器存活更久。
  int open(const ObPostingBlockDesc &posting_block,
           const storage::ObITableReadInfo &read_info,
           const ObInvIdxRowDesc &row_desc,
           common::ObIAllocator &allocator,
           const ObPostingValueBlockDesc *value_block = nullptr);

  // 将游标前进到下一行并物化之。最后一行之后返回 OB_ITER_END。
  int next();

  // 当前已物化的行。可重复读取，不前进。首次 next() 之前或
  // block 耗尽之后返回 nullptr。
  const ObDatumRow *get_curr_row() const;

  // 融合的 next() + get_curr_row()，用于顺序 WholeScanner 路径。请勿
  // 与 advance_to() 交叉调用（后者落在当前行而不是越过它），
  // 否则落脚行会被跳过。
  int get_next_row(const ObDatumRow *&row);

  // 当前行的 doc_id；轻量操作（只解码 doc_id）。不前进。
  int get_curr_doc_id(uint64_t &doc_id) const;

  // 在当前 block 内向前跳跃到 doc_id >= target 的第一行，
  // 并落在该行上。跨 block 定位由上层迭代器负责。
  // 单调（从当前游标开始搜索），因此 target <= 当前行时
  // 为空操作；没有行到达 target 时停在末尾并返回 OB_ITER_END。
  int advance_to(const uint64_t target_doc_id);

  // 游标是否已越过该 block 的最后一行。
  OB_INLINE bool is_iter_end() const { return !is_opened_ || cursor_ >= row_count_; }

  // 将游标回绕到第一行，保留缓冲区。
  void reuse();
  // 释放解码器/读取器并解绑输入缓冲区。
  void reset();

  OB_INLINE bool is_opened() const { return is_opened_; }
  OB_INLINE int64_t get_row_count() const { return row_count_; }

  TO_STRING_KV(K_(is_opened), K_(row_count), K_(cursor), K_(has_value_block),
      K_(out_col_cnt));

private:
  int materialize_row(const int64_t row_idx);

private:
  ObPostingBlockDecoder decoder_;          // posting (logical) block 解码器
  ObPostingValueBlockReader value_reader_; // 可选的配对 value block 读取器
  ObInvIdxRowDesc row_desc_;               // 各 FTS 列在输出行中的位置（按值持有）
  common::ObIAllocator *allocator_;        // 物化缓冲区的分配器（借出）
  ObDatumRow curr_row_;                     // 物化缓冲区，get_next_row() 之后有效
  int64_t row_count_;                       // posting block 的行数
  int64_t out_col_cnt_;                     // 输出列数（= read_info 请求列数）
  int64_t cursor_;                          // 当前行索引（首次 next() 前为 -1）
  bool has_value_block_;                    // 是否绑定了 value block
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObInvRowLogicalBlockIterator);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_ROW_LOGICAL_BLOCK_ITER_H_

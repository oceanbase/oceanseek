/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_MERGE_ITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_MERGE_ITER_H_

#include "share/datum/ob_datum_funcs.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"  // ObMacroBlockDesc
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_iterator.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_iterator.h"
#include "storage/blocksstable/async_index/ob_inv_idx_logical_block_iter.h"
#include "storage/blocksstable/async_index/ob_inv_row_logical_block_iter.h"
#include "storage/compaction/ob_partition_merge_iter.h"

namespace oceanbase
{
namespace blocksstable
{

class ObStorageDatumUtils;
// 单 SSTable 的分层 compaction 迭代器，采用 drill-down 模型：
//   UNOPENED -> MACRO -> TERM_ENTRY -> LOGICAL_BLOCK -> ROW
// 底层由四个倒排索引读栈迭代器自顶向下组合而成，延迟打开。宏块层可产出复用描述；
// 微块/逻辑块复用接口仍保留为后续阶段桩实现。
//
// 输出行为完整 schema 存储序（multi-version 列序）<term..., doc_id,
// [trans_version, sql_seq], posting cols...>。term 列不存储在 posting block 中
// （row iterator 产出 nop）；materialize_curr_row_ 用 curr_term_ 覆盖它们。
// term_col_cnt_ = schema_rowkey_count - 1（最后一个 rowkey 列为 doc_id）。
// row iterator 的列角色由 ObInvIdxRowDesc（各 *_col_idx_ 均为 schema 存储序）
// 决定，与 writer 侧对称；本迭代器 init 时接收该 row desc 并透传给 row_iter_。
class ObInvSSTableMergeIter : public compaction::ObMergeIter
{
public:
  enum class Level : uint8_t {
    UNOPENED,
    MACRO,          // 2MB 宏块，可参与整块复用
    TERM_ENTRY,     // 一条 (posting-data 微块, term) 条目；非物理微块游标
    LOGICAL_BLOCK,  // 256 行 posting 组，可参与组复用
    ROW,
    END
  };

  ObInvSSTableMergeIter();
  virtual ~ObInvSSTableMergeIter();

  // ObMergeIter 基于 ObMergeParameter 的 init 重载保持为 OB_NOT_SUPPORTED；
  // 下方倒排索引特有的 init 才是真正的入口。
  using compaction::ObMergeIter::init;

  int init(ObInvSSTable &sstable,
           const ObITableReadInfo &read_info,
           const ObInvIdxRowDesc &row_desc,
           const ObDatumRange &term_range,
           ObIAllocator &allocator);
  virtual void reset() override;
  void reuse();

  // next() 驱动仅 ROW 路径的 drill-down：首次调用打开到第一行，
  // 后续调用前进一行，当前层耗尽时逐层上溯（参见 next_row()）。
  virtual int next() override;
  virtual bool is_iter_end() const override { return Level::END == curr_level_; }
  virtual const ObDatumRow *get_curr_row() const override;
  virtual const storage::ObITable *get_table() const override { return sstable_; }
  virtual int get_curr_row_id(int64_t &row_id) const override;
  virtual int64_t get_last_row_id() const override
  {
    return Level::ROW == curr_level_ ? iter_row_id_ - 1 : iter_row_id_;
  }
  // 按 curr_level_ 反映真实层级，供主循环判定「是否停在可复用的块边界」。
  //   - is_macro_block_opened(): 停在 MACRO（含 UNOPENED）为 false（可试整块复用），
  //     下钻到 TERM_ENTRY 及更深为 true。
  //   - is_micro_block_opened(): 停在 TERM_ENTRY（倒排的 posting-data 微块条目）
  //     为 false（可试整微块复用，phase-1 不启用），下钻到 LOGICAL_BLOCK/ROW 为 true。
  virtual bool is_macro_block_opened() const override { return curr_level_ > Level::MACRO; }
  virtual bool is_micro_block_opened() const override { return curr_level_ > Level::TERM_ENTRY; }

  Level get_curr_level() const { return curr_level_; }
  bool is_end() const { return Level::END == curr_level_; }

  /// 当前行的 (term, doc_id) 键，供 loser tree 比较使用。term 前缀
  /// 借用 micro iterator 的 index-entry endkey。处于 END 时返回 OB_ITER_END。
  int get_curr_key(ObDatumRowkey &key);

  /// 当前位置的完整 ObDatumRange（含 border），对齐主线 get_curr_range。
  /// MACRO/TERM_ENTRY 借用当前 macro/micro 描述（推进该层后失效）；
  /// LOGICAL_BLOCK/ROW 深拷贝到本迭代器可复用缓冲区，会改写内部缓存，故非 const。
  virtual int get_curr_range(ObDatumRange &range);

  /// 兼容旧接口：拆出 low/high；内部转调 get_curr_range(ObDatumRange)。
  int get_curr_range(ObDatumRowkey &low, ObDatumRowkey &high);

  // Drill-down（逐层打开，可能触发 IO）。
  int open_term_entry();
  int open_logical_block();
  int open_row();

  // 主线同构：按 curr_level_ 下钻一层（MACRO→TERM_ENTRY→LOGICAL_BLOCK→ROW）。
  // for_rewrite 一期忽略；for_compare 为真且落到 ROW 时会 next_row() 物化首行。
  // 若下钻过程中跨到新的 logical/term/macro 或 source END，返回
  // OB_BLOCK_SWITCHED，通知 rows merger 对齐普通 compaction 整树重建。
  virtual int open_curr_range(const bool for_rewrite, const bool for_compare = false) override;

  // 判断当前 MACRO 层游标的宏块 range 是否完整落在本任务 term_range_ 内
  // （按完整 (term..., doc_id) rowkey 及开闭端点比较）。跨 range 起止端点的
  // 边界宏块不可整块复用，需逐行裁剪。
  int curr_macro_within_merge_range(const ObMacroBlockDesc &desc, bool &within) const;
  // 公共包含判定骨架：block_range 是否完整落在 term_range_ 内，同时尊重
  // block/task 两侧 border flag（等值边界时，任一侧为开区间即不算包含）。
  // 宏块下界通常是 previous endkey 开区间，逻辑块下界是真实行闭区间，
  // 两条复用路径共用本骨架，不写死开闭假设。
  int range_within_merge_range(const ObDatumRange &block_range, bool &within) const;

  // 宏块边界推进（对齐主线 next_range，兼任首次定位）：从 UNOPENED 落到首个
  // 宏块、或从当前宏块推进到下一个，均停在 Level::MACRO 不下钻、不读内容。
  // 其余 next_* 为同层推进；当前层耗尽时回溯到父层并在父层推进。
  int next_macro_block();
  bool curr_macro_is_self_contained() const;
  int next_term_entry();
  bool curr_micro_is_single_term() const;
  int next_logical_block();
  // stop_at_logical_block_boundary=false（默认）：row-only 兼容推进，当前逻辑块
  // 耗尽后贪心跨块 open_row()，直到下一行或 MACRO/END 边界。
  // stop_at_logical_block_boundary=true（merger 逐行路径专用）：当前逻辑块耗尽后
  // 推进到下一 LOGICAL_BLOCK 边界即返回成功且不 open_row()（curr_row()==nullptr），
  // 由主循环重建 loser tree 后重新判逻辑块复用；MACRO 上浮与 END 语义不变。
  int next_row(const bool stop_at_logical_block_boundary = false);
  // 整宏块复用的行进度记账：前置 curr_level_==MACRO，调用方在 writer
  // 接受复用宏块后调用，随后显式 next_macro_block() 推进。
  int advance_reused_rows(const int64_t row_count);
  // 逻辑块复用的行进度记账：前置 curr_level_==LOGICAL_BLOCK，
  // 按复用块的 doc_count 累加 iter_row_count_/iter_row_id_，不物化行；调用方在 writer
  // 接受复用块后调用，随后显式 next_logical_block() 推进。
  int advance_reused_logical_rows(const int64_t row_count);

  // 整宏块复用借用视图：仅在 MACRO 边界读取当前宏块描述、聚合统计及（需要时）
  // clustered index micro block；调用方必须在 next_macro_block() 前消费。
  int get_macro_block_desc(ObMacroBlockDesc &desc) const;
  int get_curr_macro_inv_meta(ObInvMacroMeta &meta) const;
  int get_curr_clustered_index_info(const ObMicroBlockData *&clustered_micro_block_data);

  int get_micro_block(ObMicroBlock &micro_block) const;
  // 逻辑块复用借用视图：前置 curr_level_==LOGICAL_BLOCK，产出当前逻辑块的
  // posting desc（浅拷贝 curr_posting_block_，含 buf_/size_/doc_count_/min_id_/
  // max_id_/total_doc_length_/max_score_pairs_）与行对齐的 value desc（无 posting
  // value 时为无效 desc，不算错误）。
  // 生命周期：返回的 buf 借用当前宏块读缓冲区，next_logical_block()/宏块推进后
  // 失效，禁止跨异步回调持有；调用方必须在推进迭代器前完成 writer append。
  int get_logical_block(ObPostingBlockDesc &posting_block,
                        ObPostingValueBlockDesc &value_block);
  // 当前 term 前缀（借用 micro_iter_ 的 index-entry endkey，micro_iter_ reset 前
  // 有效）。get_curr_key() 仅在 ROW 层拼 (term, doc_id)，LOGICAL_BLOCK 层无 doc_id
  // 概念，故单独暴露。前置 curr_level_==LOGICAL_BLOCK 或 ROW。
  int get_curr_term(ObDatumRowkey &term) const;

  int64_t get_iter_row_count() const { return iter_row_count_; }

  TO_STRING_KV(K_(curr_level), K_(is_inited));

private:
  int materialize_curr_row_();
  // 逐行上界检查（next_row() 中每行调用）：必须在行级别进行，使得中途结束的
  // range 仍能产出该 term 在 end_key 之前的所有行（macro iterator 仅提供宏块
  // 粒度的边界）。
  int beyond_range_end_(const ObDatumRowkey &endkey, bool &beyond) const;
  int before_range_start_(const ObDatumRowkey &curr_key, bool &before) const;
  // 将 src 深拷贝到 dest，底层由 buf 支撑；仅在新 key 超过 buf_cap 时才扩容，
  // 否则原地复用。若每次调用都 free+alloc，在 arena 分配器（free() 为空操作）
  // 下会无限增长，而 merge 热路径通常使用 arena。
  int deep_copy_rowkey_(const ObDatumRowkey &src, ObDatumRowkey &dest,
                        char *&buf, int64_t &buf_cap) const;
  // 推进宏块游标；首次调用时延迟打开 macro_iter_，因此该函数同时充当
  // "落到第一个宏块"（顶层无单独的首次/下次拆分）。
  int advance_macro_();
  int enter_term_entry_();
  int advance_term_entry_();
  int enter_logical_block_();
  int fill_curr_term_entry_(const ObMicroBlock *mb,
                            const common::ObIArray<ObInvIdxLogicalBlockMeta> *metas);
private:
  // 四层读栈（自顶向下延迟打开）。curr_term_ / curr_micro_block_ /
  // curr_term_metas_ / curr_value_block_ 借用 micro_iter_ 持有的内存，
  // 仅在 micro_iter_ 描述当前宏块期间有效。因此 advance_macro_() 在落到
  // 新宏块时会 reset micro_iter_ 并清空这些借用状态，使得
  // enter_term_entry_() 仅在新宏块上重新初始化 micro_iter_。
  ObInvIndexBlockMacroIterator  macro_iter_;
  ObInvIndexBlockMicroIterator  micro_iter_;
  ObInvIdxLogicalBlockIterator  logical_iter_;
  ObInvRowLogicalBlockIterator  row_iter_;
  ObInvIdxRowDesc               inv_idx_row_desc_; // FTS 列在输出行中的位置，传给 row_iter_

  // 成员按对齐分组：状态对象、指针、int64_t、然后 bool 标志。
  ObDatumRow curr_row_;          // 输出行缓冲区，在 ROW 层级有效（term + posting 列）
  ObDatumRowkey curr_term_;      // 当前 term 前缀（借用 micro_iter_ 的 index entry endkey）
  ObDatumRowkey curr_key_;       // get_curr_key 使用的 (curr_term_, doc_id) 暂存区
  ObDatumRange term_range_;      // 分配给本任务的 (term, doc_id) 范围
  ObMacroBlockDesc curr_macro_desc_;
  ObMicroBlockData curr_value_block_;     // 当前微块的 value payload（phrase 模式）；无时为无效
  ObPostingBlockDesc curr_posting_block_;

  ObInvSSTable *sstable_;        // 借用，不持有所有权
  const ObITableReadInfo *read_info_;
  ObIAllocator *allocator_;      // 借用，不持有所有权
  const ObStorageDatumUtils *datum_utils_;
  char *curr_key_buf_;           // 支撑 get_curr_key 返回键的深拷贝缓冲区（term_col_cnt_+1 个 datum）
  char *range_scratch_buf_;      // get_curr_range 的独立暂存区，确保不会覆盖
                                 // 调用方仍在持有的 get_curr_key 结果
  char *range_low_buf_;          // get_curr_range low key 的持久化只增深拷贝缓冲区
  char *range_high_buf_;         // get_curr_range high key 的持久化只增深拷贝缓冲区
  const ObMicroBlock *curr_micro_block_; // 当前 term entry 的 posting-data 微块视图
  const common::ObIArray<ObInvIdxLogicalBlockMeta> *curr_term_metas_; // 当前 term 的 meta 数组
  int64_t range_low_buf_cap_;
  int64_t range_high_buf_cap_;
  int64_t iter_row_count_;       // 本迭代器产出的行数
  int64_t iter_row_id_;          // 物理输出行序，与 doc_id 无关
  int64_t term_col_cnt_;         // rowkey 中 term 列的个数（不含 doc_id）
  int64_t out_col_cnt_;          // 完整 schema 宽度 = curr_row_ 的宽度
  uint64_t curr_doc_id_;         // 当前行的 doc_id，仅用于 (term, doc_id) 键/范围
  Level      curr_level_;
  bool       macro_iter_opened_; // 在 advance_macro_ 中延迟打开
  // 逐行范围过滤快捷方式（参见 next_row()）；行按 (term, doc_id) 升序产出，
  // 因此两个边界都是单调的。curr_entry_within_end_：当前 entry 的 endkey
  // (term, max_doc_id) 在 range end 之内，故没有行会越过 end_key ——
  // 每个 entry 重新计算，仅边界 entry 保持 false。range_start_passed_：
  // 一旦某行到达 range start 即锁定为 true；仅在 reset()/reuse() 时重置。
  bool       curr_entry_within_end_;
  bool       range_start_passed_;
  bool       is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvSSTableMergeIter);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_MERGE_ITER_H_

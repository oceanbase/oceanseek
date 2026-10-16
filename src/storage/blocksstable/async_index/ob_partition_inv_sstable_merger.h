/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_PARTITION_INV_SSTABLE_MERGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_PARTITION_INV_SSTABLE_MERGER_H_

#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_helper.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

// =============================================================================
// ObPartitionInvSSTableMerger — compaction merger for inverted index SSTables
//
// External interface mirrors ObPartitionMajorMerger:
//   - Constructor takes (ObLocalArena &, const ObStaticMergeParam &)
//   - merge_partition(ctx, idx) drives the merge
//
// Internally uses ObInvSSTableMergeHelper to drive multi-way merge and produce
// a new ObInvSSTable. 分层调度：MACRO 物理复用 → LOGICAL_BLOCK 编码复用 → ROW
// 逐行重写。Physical micro-block reuse remains disabled in this phase.
// Main loop logic:
//   1. MergeHelper finds the iter holding the current minimum (term, doc_id)
//   2. Apply deletion filter
//   3. Write out the row
// =============================================================================
class ObPartitionInvSSTableMerger
{
public:
  // 块复用公共统计：尝试数/成功数/公共回退原因/耗时。
  struct ObBlockReuseStats
  {
    ObBlockReuseStats() { reset(); }
    void reset()
    {
      check_count_ = 0;
      reuse_count_ = 0;
      fallback_range_count_ = 0;
      fallback_deletion_count_ = 0;
      fallback_format_count_ = 0;
      reuse_time_us_ = 0;
    }
    int64_t check_count_;           // 当前层进入块复用尝试的次数，包含后续取块或判定失败
    int64_t reuse_count_;           // 块追加、复用行数记账和迭代器推进均成功的次数
    int64_t fallback_range_count_;  // 块未完整落入任务 merge range，放弃当前层直接复用的次数
    int64_t fallback_deletion_count_; // 块 doc-id 范围命中 deletion，放弃当前层直接复用的次数
    int64_t fallback_format_count_; // 源块语义或格式与输出不兼容，放弃当前层直接复用的次数
    int64_t reuse_time_us_;         // 所有复用尝试的累计耗时，包含成功、回退和失败路径
  };

  // 逻辑块复用统计（task 6.4）：各层尝试数/成功数/回退原因/耗时，close_ 时输出。
  struct ObLogicalBlockReuseStats : public ObBlockReuseStats
  {
    TO_STRING_KV(K_(check_count), K_(reuse_count), K_(fallback_range_count),
        K_(fallback_deletion_count), K_(fallback_format_count), K_(reuse_time_us));
  };

  // 宏块复用统计：仅按分片聚合输出，避免在高频宏块路径形成日志风暴。
  struct ObMacroBlockReuseStats : public ObBlockReuseStats
  {
    ObMacroBlockReuseStats() { reset(); }
    void reset()
    {
      ObBlockReuseStats::reset();
      fallback_payload_count_ = 0;
    }
    TO_STRING_KV(K_(check_count), K_(reuse_count), K_(fallback_range_count),
        K_(fallback_deletion_count), K_(fallback_format_count),
        K_(fallback_payload_count), K_(reuse_time_us));
    int64_t fallback_payload_count_; // 宏块复用所需统计或聚集索引载荷读取失败，放弃宏块直接复用的次数
  };
  /// @param allocator     task-level arena allocator (lifetime covers entire merge_partition call)
  /// @param static_param  static merge parameters (merge type, data version, etc.)
  ObPartitionInvSSTableMerger(
      compaction::ObLocalArena &allocator,
      const compaction::ObStaticMergeParam &static_param);
  ~ObPartitionInvSSTableMerger();
  void reset();

  /// Execute the merge. Public entry point matching ObPartitionMajorMerger::merge_partition.
  /// @param ctx  merge context
  /// @param idx  parallel task index, determines this task's term_range and macro sequence
  int merge_partition(
      compaction::ObAsyncIndexMergeCtx &ctx,
      const int64_t idx);

  /// Get the output writer (used to extract macro block write context after merge).
  ObInvIdxMacroBlockWriter *get_macro_writer() { return macro_writer_; }

  TO_STRING_KV(K_(task_idx), K_(term_range), KP_(macro_writer), KP_(merge_helper));

private:
  /// Prepare phase: bind async_ctx_ to ctx, compute term_range_ from idx.
  /// ctx already carries the inverted-merge signal (its ctor set
  /// static_param_.is_inv_index_), so init_for_merge will emit INV_INDEX_SSTABLE.
  int prepare_merge_(compaction::ObAsyncIndexMergeCtx &ctx, const int64_t idx);

  /// Create and initialize ObInvSSTableMergeHelper (N source iters + loser tree).
  int open_merge_helper_();

  /// Create and open ObInvIdxMacroBlockWriter (term-block format output).
  int open_macro_writer_();

  /// Main merge loop: prefer whole-macro reuse, then descend to logical-block reuse or row rewrite.
  int do_merge_();

  /// MACRO 层入口：安全时物理复用源 MacroBlockId；range/deletion/载荷不满足时仅
  /// 下钻到 TERM_ENTRY，使逻辑块复用仍可接管后续干净块。
  int merge_macro_block_iter_(ObInvSSTableMergeIter &iter);
  int try_process_(const ObMacroBlockDesc &macro_desc, ObInvSSTableMergeIter &iter);

  /// 逻辑块级入口（对齐主线 merge_micro_block_iter_ 的分层位置）：winner 停在
  /// TERM_ENTRY 先 open_logical_block() 落到逻辑块边界；停在 LOGICAL_BLOCK 时
  /// 先尝试逻辑块复用（check_logical_block_op → append_logical_block →
  /// advance_reused_logical_rows → next_logical_block），OP_OPEN 才 open_row()
  /// 逐行。物理微块复用（PHYSICAL_MICRO 层）本期不启用。
  int merge_micro_block_iter_(ObInvSSTableMergeIter &iter);

  /// 行级归并（复用失败回退基线）：deletion 过滤后 append_row，并推进 winner。
  int merge_row_(ObInvSSTableMergeIter &iter);

  /// Close the writer, collect block info statistics.
  int close_();

private:
  compaction::ObLocalArena              &merger_arena_;    // task-level arena allocator
  const compaction::ObStaticMergeParam  &static_param_;   // static merge parameters
  int64_t                               task_idx_;        // parallel task index
  int64_t                               start_time_;      // task execution start time
  ObDatumRange                          term_range_;      // term range for this task

  compaction::ObAsyncIndexMergeCtx     *async_ctx_;      // merge context (borrowed); is-a ObBasicTabletMergeCtx
  ObInvSSTableMergeHelper               *merge_helper_;   // multi-way merge helper (allocated from merger_arena_)
  ObInvIdxMacroBlockWriter              *macro_writer_;   // output writer (allocated from merger_arena_)
  blocksstable::ObDataStoreDesc         data_store_desc_; // data store desc for the merge
  ObLogicalBlockReuseStats              logical_reuse_stats_; // 逻辑块复用统计，close_ 时输出
  ObMacroBlockReuseStats                macro_reuse_stats_; // 宏块复用统计，close_ 时输出

  DISALLOW_COPY_AND_ASSIGN(ObPartitionInvSSTableMerger);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_PARTITION_INV_SSTABLE_MERGER_H_

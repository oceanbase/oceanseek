/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/async_index/ob_partition_inv_sstable_merger.h"
#include "lib/time/ob_time_utility.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"
#include "storage/compaction/ob_partition_merger.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace compaction;

// ---------------------------------------------------------------------------
// ObPartitionInvSSTableMerger
// ---------------------------------------------------------------------------

ObPartitionInvSSTableMerger::ObPartitionInvSSTableMerger(
    compaction::ObLocalArena &allocator,
    const compaction::ObStaticMergeParam &static_param)
  : merger_arena_(allocator),
    static_param_(static_param),
    task_idx_(0),
    start_time_(common::ObTimeUtility::fast_current_time()),
    term_range_(),
    async_ctx_(nullptr),
    merge_helper_(nullptr),
    macro_writer_(nullptr)
{
}

ObPartitionInvSSTableMerger::~ObPartitionInvSSTableMerger()
{
  reset();
}

void ObPartitionInvSSTableMerger::reset()
{
  task_idx_ = 0;
  term_range_.reset();
  async_ctx_ = nullptr;
  macro_reuse_stats_.reset();
  logical_reuse_stats_.reset();
  if (OB_NOT_NULL(merge_helper_)) {
    merge_helper_->~ObInvSSTableMergeHelper();
    merge_helper_ = nullptr;
  }
  if (OB_NOT_NULL(macro_writer_)) {
    macro_writer_->~ObInvIdxMacroBlockWriter();
    macro_writer_ = nullptr;
  }
}

int ObPartitionInvSSTableMerger::prepare_merge_(
    ObAsyncIndexMergeCtx &ctx,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  async_ctx_ = &ctx;
  task_idx_ = idx;
  if (OB_UNLIKELY(!ctx.is_valid() || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to prepare inv sstable merge", K(ret), K(ctx), K(idx));
  } else if (OB_FAIL(ctx.get_merge_range(idx, term_range_))) {
    LOG_WARN("failed to get merge range", K(ret), K(idx));
  }
  return ret;
}

int ObPartitionInvSSTableMerger::open_merge_helper_()
{
  int ret = OB_SUCCESS;
  compaction::ObMergeParameter merge_param(static_param_);
  if (OB_ISNULL(async_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async ctx is null on open merge helper", K(ret), KP_(async_ctx));
  } else if (OB_ISNULL(merge_helper_ = OB_NEWx(
      ObInvSSTableMergeHelper, &merger_arena_,
      async_ctx_->get_tablet()->get_rowkey_read_info(), async_ctx_->inv_idx_row_desc_,
      merger_arena_, async_ctx_->deletion_bitmap_handle_, &data_store_desc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for merge helper", K(ret), "size", sizeof(ObInvSSTableMergeHelper));
  } else {
    merge_param.merge_range_ = term_range_;
    if (OB_FAIL(merge_helper_->init(merge_param))) {
      LOG_WARN("failed to init merge helper", K(ret), K(merge_param));
    }
  }
  return ret;
}

int ObPartitionInvSSTableMerger::open_macro_writer_()
{
  int ret = OB_SUCCESS;
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  if (OB_ISNULL(async_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async ctx is null on open macro writer", K(ret), KP_(async_ctx));
  } else if (OB_ISNULL(macro_writer_ = OB_NEWx(ObInvIdxMacroBlockWriter, &merger_arena_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for macro writer", K(ret), "size", sizeof(ObInvIdxMacroBlockWriter));
  } else if (OB_FAIL(compaction::ObDataDescHelper::build(
        static_param_, async_ctx_->get_merge_info(), data_store_desc_))) {
    LOG_WARN("failed to build data store desc", K(ret));
  } else if (OB_FAIL(async_ctx_->generate_macro_seq_info(task_idx_, macro_seq_param.start_))) {
    LOG_WARN("failed to generate macro seq info", K(ret), K_(task_idx));
  } else if (OB_FAIL(ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(
      data_store_desc_, object_cleaner))) {
    LOG_WARN("failed to get object cleaner", K(ret));
  } else if (OB_FAIL(macro_writer_->open(
      async_ctx_->inv_idx_row_desc_,
      data_store_desc_,
      task_idx_,
      macro_seq_param,
      async_ctx_->get_pre_warm_param(),
      *object_cleaner,
      nullptr /*callback*/))) {
    LOG_WARN("failed to open inv macro block writer", K(ret));
  } else {
    LOG_INFO("success to open inv macro block writer", K(ret), KP(macro_writer_));
  }
  return ret;
}

int ObPartitionInvSSTableMerger::do_merge_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge helper is null on do merge", K(ret), KP_(merge_helper));
  } else if (OB_ISNULL(macro_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro writer is null on do merge", K(ret), KP_(macro_writer));
  } else {
    // 分层主循环：唯一 winner 停在 MACRO 时优先物理复用；不能整宏复用才下钻到
    // TERM_ENTRY，继续尝试逻辑块复用；其余状态逐行重写。next_row(true) 停在下一
    // 逻辑块边界，使同一宏块内可形成 REUSE ↔ ROW 交替。
    while (OB_SUCC(ret) && !merge_helper_->is_iter_end()) {
      ObInvSSTableMergeIter *winner_iter = nullptr;
      int64_t winner_count = 0;
      if (OB_FAIL(merge_helper_->find_minimum_iters(winner_iter, winner_count))) {
        // 循环条件已确认 rows merger 非空。此处的 OB_ITER_END 只能来自
        // loser-tree compare/pop 内部，不能当成全局完成吞掉，否则会在其他
        // source 仍有候选时提前成功并静默丢行。
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected iter end from non-empty rows merger", K(ret));
        } else {
          LOG_WARN("failed to find minimum merge iter", K(ret));
        }
      } else if (OB_ISNULL(winner_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("winner iter is null", K(ret), K(winner_count));
      } else if (1 == winner_count && nullptr == winner_iter->get_curr_row()) {
        if (!winner_iter->is_macro_block_opened()) {
          if (OB_FAIL(merge_macro_block_iter_(*winner_iter))) {
            LOG_WARN("failed to merge macro block iter", K(ret), KPC(winner_iter));
          }
        } else if (OB_FAIL(merge_micro_block_iter_(*winner_iter))) {
          LOG_WARN("failed to merge micro block iter", K(ret), KPC(winner_iter));
        }
      } else if (OB_FAIL(merge_row_(*winner_iter))) {
        LOG_WARN("failed to merge row", K(ret));
      }

      if (OB_SUCC(ret) && OB_FAIL(merge_helper_->rebuild_rows_merger())) {
        LOG_WARN("failed to rebuild rows merger", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t boundary_open_count = merge_helper_->get_range_boundary_open_count();
      logical_reuse_stats_.check_count_ += boundary_open_count;
      logical_reuse_stats_.fallback_range_count_ += boundary_open_count;
    }
  }
  return ret;
}

int ObPartitionInvSSTableMerger::merge_macro_block_iter_(ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  ObMacroBlockDesc macro_desc;
  // 源间交叠已在 loser-tree 比较面 NEED_OPEN 消解；此处唯一且未打开的 MACRO
  // 直接取 desc 交 try_process_（对齐主线 merge_macro_block_iter）。
  if (OB_FAIL(iter.get_macro_block_desc(macro_desc))) {
    LOG_WARN("failed to get macro block desc", K(ret));
  } else if (OB_FAIL(try_process_(macro_desc, iter))) {
    LOG_WARN("failed to try process macro block", K(ret), K(macro_desc));
  }
  return ret;
}

int ObPartitionInvSSTableMerger::try_process_(
    const ObMacroBlockDesc &macro_desc, ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  compaction::ObBlockOp block_op;
  const int64_t begin_us = common::ObTimeUtility::fast_current_time();
  bool macro_reused = false;
  ++macro_reuse_stats_.check_count_;
  if (OB_FAIL(merge_helper_->check_macro_block_op(iter, macro_desc, block_op))) {
    LOG_WARN("failed to check macro block op", K(ret), K(macro_desc));
  } else if (block_op.is_none()) {
    // 整块零拷贝登记复用宏块：直接从 macro meta 读取 doc 统计（min/max/
    // total_doc_length），供 writer 累加进 sstable 级 inv_meta。缺失或解码失败则优雅
    // 降级为 OP_OPEN 逐行重写，不因取不到统计而中止本轮 merge。
    ObInvIdxMacroBlockInvStats stats;
    const ObMicroBlockData *micro_block_data = nullptr;
    int tmp_ret = iter.get_curr_macro_inv_stats(stats);
    if (OB_SUCCESS == tmp_ret && macro_desc.is_clustered_index_tree_) {
      // 聚集索引树模式的整块复用还需取回该宏块的聚集索引微块，供 writer 回放给
      // clustered index writer；取不到与 stats 汇总失败同样降级为逐行重写。
      tmp_ret = iter.get_curr_clustered_index_info(micro_block_data);
    }
    if (OB_SUCCESS != tmp_ret) {
      ++macro_reuse_stats_.fallback_payload_count_;
      LOG_WARN("fail to prepare reused macro block payload, downgrade to row rewrite",
          K(tmp_ret), K(macro_desc));
      if (OB_FAIL(iter.open_curr_range(false /*for_rewrite*/))) {
        LOG_WARN("failed to open macro range after reuse prepare failure", K(ret), K(macro_desc));
      }
    } else if (merge_helper_->has_deletion_in_range(stats.min_doc_id_, stats.max_doc_id_)) {
      // min/max 来自 macro meta 预聚合统计，不依赖 (term, doc_id) 的字典序端点。
      // 命中仅表示可能有删除；为保证正确性退化到行级过滤。
      ++macro_reuse_stats_.fallback_deletion_count_;
      if (OB_FAIL(iter.open_curr_range(false /*for_rewrite*/))) {
        LOG_WARN("failed to open macro range with deleted doc id", K(ret),
            K(macro_desc), K(stats));
      }
    } else if (OB_FAIL(macro_writer_->append_macro_block(
        macro_desc, stats, micro_block_data))) {
      LOG_WARN("failed to append reused macro block", K(ret), K(macro_desc),
          K(stats));
    } else if (OB_FAIL(iter.advance_reused_rows(macro_desc.row_count_))) {
      LOG_WARN("failed to account reused macro rows", K(ret), K(macro_desc));
    } else if (OB_FAIL(iter.next_macro_block())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        macro_reused = true;
      } else {
        LOG_WARN("failed to advance to next macro block after reuse", K(ret));
      }
    } else {
      macro_reused = true;
    }
  } else if (block_op.is_open()) {
    switch (merge_helper_->get_last_macro_block_fallback()) {
      case ObInvSSTableMergeHelper::MacroBlockFallback::RANGE:
        ++macro_reuse_stats_.fallback_range_count_;
        break;
      case ObInvSSTableMergeHelper::MacroBlockFallback::FORMAT:
        ++macro_reuse_stats_.fallback_format_count_;
        break;
      default:
        break;
    }
    if (OB_FAIL(iter.open_curr_range(false /*for_rewrite*/))) {
      LOG_WARN("failed to open macro range", K(ret), K(block_op));
    }
  } else if (block_op.is_rewrite()) {
    // 一期不产出 OP_REWRITE；预留对齐主线。
    if (OB_FAIL(iter.open_curr_range(true /*for_rewrite*/))) {
      LOG_WARN("failed to open macro range for rewrite", K(ret), K(block_op));
    }
  } else if (block_op.is_filter()) {
    // 一期不产出 OP_FILTER；预留对齐主线。
    if (OB_FAIL(iter.next_macro_block())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to skip filtered macro block", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected block op", K(ret), K(block_op));
  }
  if (macro_reused) {
    ++macro_reuse_stats_.reuse_count_;
  }
  macro_reuse_stats_.reuse_time_us_ +=
      common::ObTimeUtility::fast_current_time() - begin_us;
  return ret;
}

int ObPartitionInvSSTableMerger::merge_micro_block_iter_(ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  // 逻辑块复用分层调度：winner 停在 TERM_ENTRY 先落到逻辑块边界；停在
  // LOGICAL_BLOCK 且未物化行时先试复用，OP_OPEN 才 open_row()
  // 逐行。源间交叠已由 loser-tree 比较面 NEED_OPEN 消解，此处只见唯一 champion。
  if (iter.is_end()) {
    // nothing to do
  } else if (ObInvSSTableMergeIter::Level::TERM_ENTRY == iter.get_curr_level()
      && OB_FAIL(iter.open_logical_block())) {
    LOG_WARN("failed to open logical block on term entry", K(ret), KPC(&iter));
  }
  if (OB_FAIL(ret) || iter.is_end()) {
    // 下钻失败直接返回；END 由 rebuild 从 loser tree 移除。
  } else if (ObInvSSTableMergeIter::Level::LOGICAL_BLOCK != iter.get_curr_level()
      || nullptr != iter.get_curr_row()) {
    // 成功下钻后必须停在未物化行的 LOGICAL_BLOCK 边界。若静默返回成功，
    // 主循环会在无进展的状态下反复路由回本函数。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter state at logical block merge boundary", K(ret),
        "level", iter.get_curr_level(), KP(iter.get_curr_row()), KPC(&iter));
  } else {
    ObPostingBlockDesc posting_block;
    ObPostingValueBlockDesc value_block;
    compaction::ObBlockOp block_op;
    const int64_t begin_us = common::ObTimeUtility::fast_current_time();
    ++logical_reuse_stats_.check_count_;
    if (OB_FAIL(iter.get_logical_block(posting_block, value_block))) {
      LOG_WARN("failed to get logical block", K(ret), KPC(&iter));
    } else if (OB_FAIL(merge_helper_->check_logical_block_op(
        iter, posting_block, value_block, block_op))) {
      LOG_WARN("failed to check logical block op", K(ret), K(posting_block));
    } else if (block_op.is_none()) {
      ObDatumRowkey term;
      // 严格「先 append 后推进」：借用的 buf/term 在推进迭代器后失效。
      if (OB_FAIL(iter.get_curr_term(term))) {
        LOG_WARN("failed to get current term for logical block reuse", K(ret));
      } else if (OB_FAIL(macro_writer_->append_logical_block(
          term, posting_block, value_block))) {
        LOG_WARN("failed to append reused logical block", K(ret),
            K(term), K(posting_block), K(value_block));
      } else if (OB_FAIL(iter.advance_reused_logical_rows(posting_block.doc_count_))) {
        LOG_WARN("failed to account reused logical block rows", K(ret), K(posting_block));
      } else if (OB_FAIL(iter.next_logical_block())) {
        if (OB_ITER_END == ret) {
          // 归一为成功并保留 END 状态，rebuild 会从 loser tree 移除该来源。
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to advance to next logical block after reuse", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ++logical_reuse_stats_.reuse_count_;
      }
    } else if (block_op.is_open()) {
      switch (merge_helper_->get_last_logical_block_fallback()) {
        case ObInvSSTableMergeHelper::LogicalBlockFallback::RANGE:
          ++logical_reuse_stats_.fallback_range_count_;
          break;
        case ObInvSSTableMergeHelper::LogicalBlockFallback::DELETION:
          ++logical_reuse_stats_.fallback_deletion_count_;
          break;
        case ObInvSSTableMergeHelper::LogicalBlockFallback::FORMAT:
          ++logical_reuse_stats_.fallback_format_count_;
          break;
        default:
          break;
      }
      // 下钻逐行：落到并物化当前逻辑块首行，交下一轮 merge_row_；next_row(true)
      // 在当前块耗尽后停在下一 LOGICAL_BLOCK 边界，不贪心打开后续块。
      if (OB_FAIL(iter.open_row())) {
        LOG_WARN("failed to open row on logical block", K(ret), K(posting_block));
      } else if (OB_FAIL(iter.next_row(true /*stop_at_logical_block_boundary*/))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to land first row of opened logical block", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block op for logical block", K(ret), K(block_op));
    }
    logical_reuse_stats_.reuse_time_us_ += common::ObTimeUtility::fast_current_time() - begin_us;
  }
  return ret;
}

int ObPartitionInvSSTableMerger::merge_row_(ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = iter.get_curr_row();
  const storage::ObITable *table = iter.get_table();
  bool deleted = false;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("winner row is null on row merge", K(ret));
  } else if (OB_FAIL(merge_helper_->check_row_deleted(*row, deleted))) {
    LOG_WARN("failed to check row deleted", K(ret));
  } else {
    if (deleted) {
      macro_writer_->inc_filter_row_count();
    } else if (OB_FAIL(macro_writer_->append_row(*row))) {
      LOG_WARN("failed to append row to macro writer", K(ret));
    } else if (OB_NOT_NULL(table) && table->is_inc_inv_sstable()) {
      // incremental_row_count 仅统计来自增量 SSTable、且经过逐行解码重写的行。
      // 宏块和逻辑块复用由独立复用指标统计，因此无需在复用路径中累加。
      macro_writer_->inc_incremental_row_count();
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(iter.next_row(true /*stop_at_logical_block_boundary*/))) {
    // 逐行仅消费当前逻辑块：块耗尽后停在下一 LOGICAL_BLOCK 边界（curr_row 为空），
    // 主循环重建 loser tree 后重新判逻辑块复用；不得贪心打开后续逻辑块。
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to advance winner iter", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("async index row merge diagnose", K(ret),
        "iter_level", iter.get_curr_level(),
        "iter_row_count", iter.get_iter_row_count(), KP(row), KP(table));
  }
  return ret;
}

int ObPartitionInvSSTableMerger::close_()
{
  int ret = OB_SUCCESS;
  ObInvIdxStatsMeta partial;
  ObSSTableIndexBuilder *index_builder = nullptr;
  if (OB_ISNULL(macro_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro writer is null on close", K(ret), KP_(macro_writer), KP_(async_ctx));
  } else if (OB_ISNULL(async_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async merge ctx is null on close", K(ret), KP_(macro_writer), KP_(async_ctx));
  } else if (OB_FAIL(macro_writer_->close())) {
    LOG_WARN("failed to close macro writer", K(ret));
  } else if (FALSE_IT(partial = macro_writer_->get_seg_inv_meta())) {
  } else if (OB_ISNULL(index_builder = async_ctx_->get_merge_info().get_index_builder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index builder is null on async index close", K(ret), KP(index_builder), KP_(async_ctx));
  } else if (OB_FAIL(index_builder->merge_inv_meta(partial))) {
    LOG_WARN("failed to merge segment inv meta into index builder", K(ret), K(partial), KP(index_builder));
  } else {
    LOG_INFO("inv sstable merge logical block reuse stats", K_(task_idx),
        K_(logical_reuse_stats));
    const int64_t cost_time = common::ObTimeUtility::fast_current_time() - start_time_;
    const int64_t scan_units = merge_helper_->get_iters_row_count();
    const ObMergeBlockInfo &block_info = macro_writer_->get_merge_block_info();
    const int64_t accounted_row_count =
        block_info.total_row_count_ + block_info.filter_row_count_;
    if (scan_units != accounted_row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR_RET(ret, "async index merge row accounting mismatch",
          K_(task_idx), K_(term_range), K(scan_units), K(accounted_row_count),
          K(block_info), K_(macro_reuse_stats), K_(logical_reuse_stats));
    }
    async_ctx_->get_merge_info().get_merge_history().update_merge_task_info(
        block_info, false/*without_row_cnt*/, cost_time, scan_units);
    LOG_INFO("finish async index merge partition", K_(task_idx), K_(term_range),
        "dag_param", async_ctx_->get_dag_param(),
        "input_table_count", async_ctx_->get_tables_handle().get_count(),
        K(scan_units), K(accounted_row_count), K(block_info), K(partial),
        K_(macro_reuse_stats), K_(logical_reuse_stats), K(cost_time));
  }
  return ret;
}

int ObPartitionInvSSTableMerger::merge_partition(
    ObAsyncIndexMergeCtx &ctx,
    const int64_t idx)
{
  // Main entry: prepare -> open helper -> open writer -> merge -> close
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_merge_(ctx, idx))) {
    LOG_WARN("failed to prepare async index merge", K(ret), K(idx));
  } else if (OB_FAIL(open_merge_helper_())) {
    LOG_WARN("failed to open merge helper", K(ret));
  } else if (OB_FAIL(open_macro_writer_())) {
    LOG_WARN("failed to open macro writer", K(ret));
  } else if (OB_FAIL(do_merge_())) {
    LOG_WARN("failed to do async index merge", K(ret));
  } else if (OB_FAIL(close_())) {
    LOG_WARN("failed to close async index merge", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (!ctx.info_collector_.error_location_.is_valid()) {
      CTX_SET_DIAGNOSE_LOCATION(ctx);
    }
    LOG_WARN("async index merge partition diagnose", K(ret), K(idx),
        K_(term_range), "dag_param", ctx.get_dag_param(),
        K_(macro_reuse_stats), K_(logical_reuse_stats));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_MERGE_HELPER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_MERGE_HELPER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_simple_rows_merger.h"
#include "storage/access/ob_scan_merge_loser_tree.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_block_op.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObDataStoreDesc;

static const int64_t INV_MERGE_DEFAULT_ITER_COUNT =
    common::MAX_SSTABLE_CNT_IN_STORAGE + common::MAX_INC_MAJOR_SSTABLE_CNT;

struct ObInvSSTableMergeLoserTreeItem
{
  ObInvSSTableMergeLoserTreeItem()
    : iter_(nullptr),
      iter_idx_(0),
      equal_with_next_(false)
  {}
  void reset()
  {
    iter_ = nullptr;
    iter_idx_ = 0;
    equal_with_next_ = false;
  }
  bool is_valid() const { return nullptr != iter_ && iter_idx_ >= 0; }
  int get_curr_range(ObDatumRange &range) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null iter", K(ret));
    } else {
      ret = iter_->get_curr_range(range);
    }
    return ret;
  }
  OB_INLINE bool is_range() const { return nullptr != iter_ && nullptr == iter_->get_curr_row(); }

  ObInvSSTableMergeIter *iter_;
  int64_t iter_idx_;
  bool equal_with_next_;
  TO_STRING_KV(K_(iter_idx), KPC(iter_), K_(equal_with_next));
};

// 对齐主线 ObPartitionMergeLoserTreeCmp：range/row/hybrid 比较面，交叠时在
// cmp() 循环内 NEED_OPEN 下钻，再重比。倒排为 N 路对称单堆（无 base/inc）。
class ObInvSSTableMergeLoserTreeCmp final
{
public:
  explicit ObInvSSTableMergeLoserTreeCmp(const ObITableReadInfo &read_info)
    : read_info_(read_info),
      rowkey_size_(read_info.get_schema_rowkey_count())
  {}
  int cmp(const ObInvSSTableMergeLoserTreeItem &l,
          const ObInvSSTableMergeLoserTreeItem &r,
          int64_t &cmp_ret);
  int compare(const ObInvSSTableMergeLoserTreeItem &l,
              const ObInvSSTableMergeLoserTreeItem &r,
              int64_t &cmp_ret);
  bool check_cmp_finish(const int64_t cmp_ret) const;
  int open_iter_range(const int64_t cmp_ret,
                      const ObInvSSTableMergeLoserTreeItem &left,
                      const ObInvSSTableMergeLoserTreeItem &right);
  OB_INLINE bool need_open_left_range(const int64_t cmp_ret) const
  {
    return LEFT_RANGE_NEED_OPEN == cmp_ret || ALL_RANGE_NEED_OPEN == cmp_ret;
  }
  OB_INLINE bool need_open_right_range(const int64_t cmp_ret) const
  {
    return RIGHT_RANGE_NEED_OPEN == cmp_ret || ALL_RANGE_NEED_OPEN == cmp_ret;
  }
  int compare_range(const ObDatumRange &left_range,
                    const ObDatumRange &right_range,
                    int64_t &cmp_result) const;
  int compare_hybrid(const ObDatumRow &row,
                     const ObDatumRange &range,
                     int64_t &cmp_ret) const;
  int compare_rowkey(const ObDatumRow &l_row,
                     const ObDatumRow &r_row,
                     int64_t &cmp_result) const;

public:
  static const int64_t LEFT_RANGE_NEED_OPEN = INT64_MAX;
  static const int64_t RIGHT_RANGE_NEED_OPEN = -INT64_MAX;
  static const int64_t ALL_RANGE_NEED_OPEN = INT64_MAX - 1;

private:
  const ObITableReadInfo &read_info_;
  const int64_t rowkey_size_;
};

typedef storage::ObMergeLoserTree<ObInvSSTableMergeLoserTreeItem, ObInvSSTableMergeLoserTreeCmp>
    ObInvSSTableMergeLoserTree;
typedef storage::ObSimpleRowsMerger<ObInvSSTableMergeLoserTreeItem, ObInvSSTableMergeLoserTreeCmp>
    ObInvSSTableSimpleRowsMerger;
typedef common::ObRowsMerger<ObInvSSTableMergeLoserTreeItem, ObInvSSTableMergeLoserTreeCmp>
    ObInvSSTableRowsMerger;

// =============================================================================
// ObInvSSTableMergeHelper — 倒排 SSTable 多路 merge 驱动器
//
// 管理 N 个 ObInvSSTableMergeIter（每个输入 segment 一个），按 (term, doc_id)
// 字典序归并。驱动形态对齐主线 compaction：
//   - iter 初始停在 MACRO 边界；
//   - loser tree 用主线同构的 range/row/hybrid 比较，交叠在比较中 NEED_OPEN；
//   - 唯一 winner 停在未打开 MACRO 时仅下钻；已打开到 LOGICAL_BLOCK 时尝试
//     逻辑块复用；否则下钻到 ROW。micro 级复用仍为后续阶段预留。
//
// 关键不变式：
//   - item 每次进入 rows merger 前先在树外完成 merge-range 边界归一化；
//     comparator 下钻若跨到新的 logical/term/macro，则 iterator 返回
//     OB_BLOCK_SWITCHED，helper 对齐普通 compaction 从全部 iterator 重建。
//   - find_minimum_iters 返回比较完成后的唯一 champion；多个来源落到同一
//     (term, doc_id) 行键视为输入不变量破坏。
//   - 源间交叠由比较面在 cmp() 内消解；check_logical_block_op 只做块内语义
//     （merge range / deletion 等）。
// =============================================================================
class ObInvSSTableMergeHelper
{
public:
  ObInvSSTableMergeHelper(
      const ObITableReadInfo &read_info,
      const ObInvIdxRowDesc &inv_idx_row_desc,
      common::ObIAllocator &allocator,
      const storage::ObDeletionBitmapHandle &deletion_bitmap_handle,
      const ObDataStoreDesc *data_store_desc = nullptr);
  ~ObInvSSTableMergeHelper();

  int init(const compaction::ObMergeParameter &merge_param);
  /// Reset the merge helper, destroying all source iterators.
  void reset();

  /// Returns true if all source iterators have reached END.
  bool is_iter_end() const;

  /// Find the iter with the current minimum (term, doc_id).
  /// @param winner_iter   [out] pointer to the winning iterator
  int find_minimum_iters(ObInvSSTableMergeIter *&winner_iter, int64_t &winner_count);

  /// Rebuild the loser tree after the winner has been advanced (block reused
  /// or row written). Must be called after each merge step.
  int rebuild_rows_merger();

  /// 宏块级块内语义判定。跨源交叠由 loser-tree 比较面消解；本函数只检查
  /// merge range，删除位图由调用方基于 n-1 索引微块中的真实 doc-id envelope
  /// 另行判定。
  /// 前置：winner 停在 MACRO 边界且尚未下钻；不满足时返回 OB_ERR_UNEXPECTED。
  int check_macro_block_op(
      ObInvSSTableMergeIter &winner_iter,
      const ObMacroBlockDesc &macro_desc,
      compaction::ObBlockOp &block_op);

  /// 逻辑块级块内语义判定，只判块内语义，不判跨源交叠——交叠已由 loser-tree
  /// 比较面在 NEED_OPEN 路径消解，只有
  /// find_minimum_iters 返回的唯一 champion 才进入本检查），但数据获取路径有意
  /// 不同：宏块真实 doc-id envelope 需额外读 n-1 索引微块汇总，deletion 留在
  /// merger 判；逻辑块 meta 已携带真实 min/max doc_id，deletion 直接在此判。
  /// 前置：winner 停在 LOGICAL_BLOCK（get_curr_row()==nullptr 且
  /// is_micro_block_opened() 且 curr_level==LOGICAL_BLOCK），否则 OB_ERR_UNEXPECTED。
  /// 块内判定（任一不过 → OP_OPEN 下钻逐行）：
  ///   - merge range：LOGICAL_BLOCK 层 get_curr_range() 闭闭区间完整落在
  ///     term_range_ 内（公共骨架 range_within_merge_range，尊重两侧 border flag）；
  ///   - deletion：删除位图命中逻辑块的 doc-id envelope 即保守回退逐行过滤，
  ///     不再解码 posting block 做精确确认；
  ///   - 持久化格式/schema 兼容：posting/value header version、column meta/
  ///     offset/size、posting meta 与 header/value row count、relevance/
  ///     position/nested-key flags 与输出 schema 一致；列编码须为当前 reader
  ///     可解码类型。不比较外层 micro compression/encryption，也不要求源列
  ///     编码等于 writer 默认编码。格式合法可读但不可原样复用 → OP_OPEN；
  ///     header/meta 损坏或 reader 不支持 → 返回错误（回退 ROW 同样无法安全解码）。
  int check_logical_block_op(
      ObInvSSTableMergeIter &winner_iter,
      const ObPostingBlockDesc &block,
      const ObPostingValueBlockDesc &value_block,
      compaction::ObBlockOp &block_op);

  /// 逻辑块复用回退原因（供 merger 统计用，仅记录最近一次 check_logical_block_op
  /// 的结果；OP_NONE 时为 NONE）。
  enum class LogicalBlockFallback : uint8_t
  {
    NONE = 0,      // 未回退（OP_NONE，可复用）
    RANGE = 1,     // 未完整落在 merge range
    DELETION = 2,  // deletion bitmap 命中
    FORMAT = 3     // 格式/schema 不兼容（可读但不可原样复用）
  };
  LogicalBlockFallback get_last_logical_block_fallback() const
  {
    return last_logical_block_fallback_;
  }
  int64_t get_range_boundary_open_count() const { return range_boundary_open_count_; }

  /// 宏块复用回退原因（供 merger 聚合统计，避免在宏块热路径逐块打日志）。
  enum class MacroBlockFallback : uint8_t
  {
    NONE = 0,   // 未回退（OP_NONE，可继续做 deletion/payload 检查）
    RANGE = 1,  // 未完整落在 merge range
    FORMAT = 2  // 源段语义或物理格式与输出不兼容
  };
  MacroBlockFallback get_last_macro_block_fallback() const
  {
    return last_macro_block_fallback_;
  }

  /// Checks the immutable bitmap without advancing its stateful iterator.
  /// Callers must pass the actual doc_id envelope of the macro block, rather
  /// than doc_id values extracted from its (term, doc_id) key endpoints.
  bool has_deletion_in_range(const uint64_t min_doc_id, const uint64_t max_doc_id) const
  {
    return deletion_bitmap_handle_.is_valid()
        && deletion_bitmap_handle_.contains_any_in_range(min_doc_id, max_doc_id);
  }

  /// Row-level deletion check.
  /// @param row      the current row (doc_id index comes from inv_idx_row_desc_)
  /// @param deleted  [out] whether this row should be discarded
  int check_row_deleted(const ObDatumRow &row, bool &deleted);

  /// Returns the total number of rows advanced across all merge iters (for merge progress tracking).
  int64_t get_iters_row_count() const;

  TO_STRING_KV(K_(is_inited), "iter_count", iters_.count(), K_(consume_iter_idx),
      K_(range_boundary_open_count));

private:
  static const int64_t INVALID_CONSUME_ITER_IDX = -1;
  int init_merge_iters(const compaction::ObMergeParameter &merge_param);
  int prepare_rows_merger_(const bool is_full_merge);
  int build_rows_merger(const bool is_full_merge);
  // 对齐 ObPartitionMergeHelper::build_rows_merger：从所有当前 iterator 重建；
  // 比较返回 OB_BLOCK_SWITCHED 时丢弃部分构造的树并重试。
  int rebuild_rows_merger_from_iters_();
  // 在 iterator 进入 rows merger 前消解 task range 的边界块。完整落在 task
  // range 内的块保持原层级以参与复用；跨边界块在树外逐层下钻，直到得到范围内
  // 的 ROW、后续完整块或该 source 耗尽。source 耗尽按成功返回并保留 END 状态。
  int normalize_iter_for_merge_range_(ObInvSSTableMergeIter &iter);
  // 逻辑块删除范围判定：删除位图命中 [min_doc_id, max_doc_id] 即保守回退逐行过滤，
  // 不再解码 posting block 做精确确认。posting_decoder 参数保留以维持现有接口。
  int check_logical_block_deletion_(
      const ObPostingBlockDecoder &posting_decoder,
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      bool &has_deletion);
  int check_macro_block_reuse_compatibility_(
      const ObInvSSTableMergeIter &winner_iter,
      const ObMacroBlockDesc &macro_desc,
      bool &is_compatible) const;
  void log_duplicate_key_diagnose_();

private:
  common::ObSEArray<ObInvSSTableMergeIter *, 8> iters_;
  // DAG 级共享（同一 tablet 的并行 merge task 共用一份），只允许走 const 只读
  // 接口：位图构建后不再变更，contains / contains_any_in_range 均为纯读，故并发
  // 安全。禁止在此持有或推进其内部游标（open_iter / advance_to / get_next），
  // 那会写共享状态（含 ObRoaringBitmapIter::init 内的 convert_to_bitmap）。
  const storage::ObDeletionBitmapHandle &deletion_bitmap_handle_;
  const ObITableReadInfo  &read_info_;
  const ObInvIdxRowDesc &inv_idx_row_desc_;
  common::ObIAllocator    &allocator_;
  // Optional for direct helper tests; production merge always supplies the output descriptor.
  // It is borrowed from ObPartitionInvSSTableMerger and outlives this helper.
  const ObDataStoreDesc *data_store_desc_;
  ObInvSSTableRowsMerger  *rows_merger_;
  ObInvSSTableMergeLoserTreeCmp *cmp_;
  int64_t                 consume_iter_idx_;
  MacroBlockFallback      last_macro_block_fallback_;
  int64_t                 range_boundary_open_count_;
  LogicalBlockFallback    last_logical_block_fallback_;
  bool                    is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvSSTableMergeHelper);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_MERGE_HELPER_H_

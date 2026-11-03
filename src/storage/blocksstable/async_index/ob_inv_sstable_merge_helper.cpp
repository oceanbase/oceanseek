/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_helper.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{

int ObInvSSTableMergeLoserTreeCmp::compare_range(
    const ObDatumRange &left_range,
    const ObDatumRange &right_range,
    int64_t &cmp_result) const
{
  int ret = OB_SUCCESS;
  int temp_cmp_ret = 0;
  const ObDatumRowkey &left_end_key = left_range.get_end_key();
  const ObDatumRowkey &left_start_key = left_range.get_start_key();
  const ObDatumRowkey &right_end_key = right_range.get_end_key();
  const ObDatumRowkey &right_start_key = right_range.get_start_key();
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();

  if (OB_UNLIKELY(!left_range.is_valid() || !right_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_range), K(right_range));
  } else if (OB_FAIL(left_end_key.compare(right_end_key, datum_utils, temp_cmp_ret))) {
    LOG_WARN("Failed to compare rowkey", K(ret), K(left_end_key), K(right_end_key));
  } else if (0 == temp_cmp_ret) {
    cmp_result = ALL_RANGE_NEED_OPEN;
  } else if (temp_cmp_ret > 0) {
    // left_end > right_end：若 right_end < left_start 则 right 可复用（返回 1）；
    // 否则区间交叠，优先打开 left（更可能落入 right 区间内）。
    if (OB_FAIL(right_end_key.compare(left_start_key, datum_utils, temp_cmp_ret))) {
      LOG_WARN("Failed to compare rowkey", K(ret), K(left_start_key), K(right_end_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? LEFT_RANGE_NEED_OPEN : 1;
    }
  } else if (temp_cmp_ret < 0) {
    if (OB_FAIL(left_end_key.compare(right_start_key, datum_utils, temp_cmp_ret))) {
      LOG_WARN("Failed to compare rowkey", K(ret), K(left_end_key), K(right_start_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? RIGHT_RANGE_NEED_OPEN : -1;
    }
  }
  return ret;
}

int ObInvSSTableMergeLoserTreeCmp::compare_hybrid(
    const ObDatumRow &row,
    const ObDatumRange &range,
    int64_t &cmp_ret) const
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  int temp_cmp_ret = 0;
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();

  if (OB_FAIL(rowkey.assign(row.storage_datums_, rowkey_size_))) {
    LOG_WARN("Failed to assign store rowkey", K(ret), K(row), K_(rowkey_size));
  } else if (OB_FAIL(rowkey.compare(range.get_start_key(), datum_utils, temp_cmp_ret))) {
    LOG_WARN("Failed to compare start key", K(ret), K(rowkey), K(range));
  } else if (temp_cmp_ret < 0) {
    cmp_ret = -1;
  } else if (0 == temp_cmp_ret) {
    cmp_ret = RIGHT_RANGE_NEED_OPEN;
    LOG_DEBUG("rowkey equal range start_key, open range", K(rowkey), K(range));
  } else if (OB_FAIL(rowkey.compare(range.get_end_key(), datum_utils, temp_cmp_ret))) {
    LOG_WARN("Failed to compare end key", K(ret), K(rowkey), K(range));
  } else if (0 == temp_cmp_ret) {
    if (!range.get_border_flag().inclusive_end()) {
      cmp_ret = 1;
    } else {
      cmp_ret = RIGHT_RANGE_NEED_OPEN;
    }
  } else if (temp_cmp_ret > 0) {
    cmp_ret = 1;
  } else {
    cmp_ret = RIGHT_RANGE_NEED_OPEN;
  }
  return ret;
}

int ObInvSSTableMergeLoserTreeCmp::compare_rowkey(
    const ObDatumRow &l_row,
    const ObDatumRow &r_row,
    int64_t &cmp_result) const
{
  int ret = OB_SUCCESS;
  int temp_cmp_ret = 0;
  ObDatumRowkey l_key;
  ObDatumRowkey r_key;
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();

  if (OB_UNLIKELY(l_row.get_column_count() < rowkey_size_
      || r_row.get_column_count() < rowkey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row column count", K(ret), K(l_row), K(r_row), K_(rowkey_size));
  } else if (OB_FAIL(l_key.assign(l_row.storage_datums_, rowkey_size_))) {
    LOG_WARN("Failed to assign left rowkey", K(ret), K(l_row), K_(rowkey_size));
  } else if (OB_FAIL(r_key.assign(r_row.storage_datums_, rowkey_size_))) {
    LOG_WARN("Failed to assign right rowkey", K(ret), K(r_row), K_(rowkey_size));
  } else if (OB_FAIL(l_key.compare(r_key, datum_utils, temp_cmp_ret))) {
    LOG_WARN("Failed to compare rowkey", K(ret), K(l_key), K(r_key));
  } else {
    cmp_result = temp_cmp_ret;
  }
  return ret;
}

bool ObInvSSTableMergeLoserTreeCmp::check_cmp_finish(const int64_t cmp_ret) const
{
  return !need_open_left_range(cmp_ret) && !need_open_right_range(cmp_ret);
}

int ObInvSSTableMergeLoserTreeCmp::open_iter_range(
    const int64_t cmp_ret,
    const ObInvSSTableMergeLoserTreeItem &left,
    const ObInvSSTableMergeLoserTreeItem &right)
{
  int ret = OB_SUCCESS;
  // 对齐 ObPartitionMergeLoserTreeCmp::open_iter_range：iterator 在比较下钻时
  // 若切换了当前 range，会原样返回 OB_BLOCK_SWITCHED，由 helper 整树重建。
  if (need_open_left_range(cmp_ret)
      && OB_FAIL(left.iter_->open_curr_range(false, true /* for_compare */))) {
    if (OB_BLOCK_SWITCHED != ret) {
      LOG_WARN("fail to open left iter range for compare", K(ret), K(left));
    }
  } else if (need_open_right_range(cmp_ret)
      && OB_FAIL(right.iter_->open_curr_range(false, true /* for_compare */))) {
    if (OB_BLOCK_SWITCHED != ret) {
      LOG_WARN("fail to open right iter range for compare", K(ret), K(right));
    }
  }
  return ret;
}

int ObInvSSTableMergeLoserTreeCmp::compare(
    const ObInvSSTableMergeLoserTreeItem &l,
    const ObInvSSTableMergeLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (OB_ISNULL(l.iter_) || OB_ISNULL(r.iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null merge iter in loser tree", K(ret), KP(l.iter_), KP(r.iter_));
  } else {
    const ObDatumRow *left_row = l.iter_->get_curr_row();
    const ObDatumRow *right_row = r.iter_->get_curr_row();
    if (OB_NOT_NULL(left_row) && OB_NOT_NULL(right_row)) {
      if (OB_FAIL(compare_rowkey(*left_row, *right_row, cmp_ret))) {
        LOG_WARN("compare_rowkey failed", K(ret));
      }
    } else if (OB_ISNULL(left_row) && OB_ISNULL(right_row)) {
      ObDatumRange left_range;
      ObDatumRange right_range;
      if (OB_FAIL(l.get_curr_range(left_range))) {
        LOG_WARN("get_curr_range failed", K(ret), K(l));
      } else if (OB_FAIL(r.get_curr_range(right_range))) {
        LOG_WARN("get_curr_range failed", K(ret), K(r));
      } else if (OB_FAIL(compare_range(left_range, right_range, cmp_ret))) {
        LOG_WARN("compare_range failed", K(ret));
      }
    } else {
      ObDatumRange range;
      const bool is_reverse = (nullptr == left_row);
      const ObInvSSTableMergeLoserTreeItem &item = is_reverse ? l : r;
      const ObDatumRow *row = is_reverse ? right_row : left_row;
      if (OB_FAIL(item.get_curr_range(range))) {
        LOG_WARN("get_curr_range failed", K(ret), K(item));
      } else if (OB_FAIL(compare_hybrid(*row, range, cmp_ret))) {
        LOG_WARN("compare_hybrid failed", K(ret));
      } else if (is_reverse) {
        cmp_ret = -cmp_ret;
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeLoserTreeCmp::cmp(
    const ObInvSSTableMergeLoserTreeItem &l,
    const ObInvSSTableMergeLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  bool finish = false;
  while (OB_SUCC(ret) && !finish) {
    if (OB_FAIL(compare(l, r, cmp_ret))) {
      LOG_WARN("Fail to compare item", K(ret), K(l), K(r));
    } else if ((finish = check_cmp_finish(cmp_ret))) {
    } else if (OB_FAIL(open_iter_range(cmp_ret, l, r))) {
      if (OB_BLOCK_SWITCHED != ret) {
        LOG_WARN("fail open iter", K(ret), K(cmp_ret), K(l), K(r));
      }
    }
  }
  return ret;
}

ObInvSSTableMergeHelper::ObInvSSTableMergeHelper(
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &inv_idx_row_desc,
    common::ObIAllocator &allocator,
    const storage::ObDeletionBitmapHandle &deletion_bitmap_handle,
    const ObDataStoreDesc *data_store_desc)
  : iters_(),
    deletion_bitmap_handle_(deletion_bitmap_handle),
    read_info_(read_info),
    inv_idx_row_desc_(inv_idx_row_desc),
    allocator_(allocator),
    data_store_desc_(data_store_desc),
    rows_merger_(nullptr),
    cmp_(nullptr),
    consume_iter_idx_(INVALID_CONSUME_ITER_IDX),
    last_macro_block_fallback_(MacroBlockFallback::NONE),
    range_boundary_open_count_(0),
    last_logical_block_fallback_(LogicalBlockFallback::NONE),
    is_inited_(false)
{}

ObInvSSTableMergeHelper::~ObInvSSTableMergeHelper() { reset(); }

int ObInvSSTableMergeHelper::init(const compaction::ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!merge_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid merge param", K(ret), K(merge_param));
  } else if (OB_FAIL(init_merge_iters(merge_param))) {
    LOG_WARN("failed to init inv merge iters", K(ret), K(merge_param));
  } else if (OB_FAIL(prepare_rows_merger_())) {
    LOG_WARN("failed to prepare rows merger", K(ret), K(merge_param));
  } else {
    consume_iter_idx_ = INVALID_CONSUME_ITER_IDX;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    reset();
  }
  return ret;
}

int ObInvSSTableMergeHelper::init_merge_iters(const compaction::ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  const storage::ObTablesHandleArray &tables_handle = merge_param.get_tables_handle();
  const ObDatumRange &term_range = merge_param.merge_range_;
  iters_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
    storage::ObITable *table = tables_handle.get_table(i);
    ObInvSSTableMergeIter *merge_iter = nullptr;
    if (OB_ISNULL(table) || !table->is_sstable() || !table->is_inv_sstable()) {
      if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table in inv merge tables handle", K(ret), K(i), KP(table));
      }
    } else if (OB_ISNULL(merge_iter = OB_NEWx(ObInvSSTableMergeIter, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate merge iter", K(ret), K(i));
    } else if (OB_FAIL(merge_iter->init(
        *static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table)),
        read_info_,
        inv_idx_row_desc_,
        term_range,
        allocator_))) {
      LOG_WARN("failed to init merge iter", K(ret), K(i));
    } else if (OB_FAIL(iters_.push_back(merge_iter))) {
      LOG_WARN("failed to push merge iter", K(ret), K(i));
    } else {
      merge_iter = nullptr;
    }
    if (OB_NOT_NULL(merge_iter)) {
      OB_DELETEx(ObInvSSTableMergeIter, &allocator_, merge_iter);
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t j = 0; j < iters_.count(); ++j) {
      ObInvSSTableMergeIter *iter = nullptr;
      if (OB_NOT_NULL(iter = iters_.at(j))) {
        OB_DELETEx(ObInvSSTableMergeIter, &allocator_, iter);
      }
    }
    iters_.reset();
  } else if (iters_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no inv sstable in merge tables handle", K(ret), K(merge_param));
  }
  return ret;
}

void ObInvSSTableMergeHelper::reset()
{
  for (int64_t i = 0; i < iters_.count(); ++i) {
    ObInvSSTableMergeIter *iter = nullptr;
    if (OB_NOT_NULL(iter = iters_.at(i))) {
      OB_DELETEx(ObInvSSTableMergeIter, &allocator_, iter);
    }
  }
  iters_.reset();
  if (OB_NOT_NULL(rows_merger_)) {
    OB_DELETEx(ObInvSSTableRowsMerger, &allocator_, rows_merger_);
  }
  if (OB_NOT_NULL(cmp_)) {
    OB_DELETEx(ObInvSSTableMergeLoserTreeCmp, &allocator_, cmp_);
  }
  consume_iter_idx_ = INVALID_CONSUME_ITER_IDX;
  last_macro_block_fallback_ = MacroBlockFallback::NONE;
  last_logical_block_fallback_ = LogicalBlockFallback::NONE;
  range_boundary_open_count_ = 0;
  is_inited_ = false;
}

bool ObInvSSTableMergeHelper::is_iter_end() const
{
  return !is_inited_ || nullptr == rows_merger_ || rows_merger_->empty();
}

int64_t ObInvSSTableMergeHelper::get_iters_row_count() const
{
  int64_t iter_row_count = 0;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    const ObInvSSTableMergeIter *iter = iters_.at(i);
    if (OB_NOT_NULL(iter)) {
      iter_row_count += iter->get_iter_row_count();
    }
  }
  return iter_row_count;
}

int ObInvSSTableMergeHelper::prepare_rows_merger_()
{
  int ret = OB_SUCCESS;
  const int64_t max_table_cnt = INV_MERGE_DEFAULT_ITER_COUNT;
  const int64_t iter_count = iters_.count();
  if (iter_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no merge iter to build rows merger", K(ret), K(iter_count));
  } else if (OB_UNLIKELY(iter_count > max_table_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inv merge iter count", K(ret), K(iter_count), K(max_table_cnt));
  } else if (OB_ISNULL(cmp_ = OB_NEWx(ObInvSSTableMergeLoserTreeCmp, &allocator_, read_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate loser tree cmp", K(ret));
  } else if (iter_count <= ObInvSSTableSimpleRowsMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    if (OB_ISNULL(rows_merger_ = OB_NEWx(ObInvSSTableSimpleRowsMerger, &allocator_, *cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate simple rows merger", K(ret));
    }
  } else if (OB_ISNULL(rows_merger_ = OB_NEWx(ObInvSSTableMergeLoserTree, &allocator_, *cmp_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate loser tree", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rows_merger_->init(max_table_cnt, iter_count, allocator_))) {
    LOG_WARN("failed to init rows merger", K(ret), K(iter_count));
  } else if (OB_FAIL(build_rows_merger())) {
    LOG_WARN("failed to build rows merger", K(ret), K(iter_count));
  }
  return ret;
}

int ObInvSSTableMergeHelper::build_rows_merger()
{
  int ret = OB_SUCCESS;
  const int64_t iter_count = iters_.count();
  if (OB_ISNULL(rows_merger_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows merger comparator is null", K(ret));
  } else {
    // First position every source at its initial macro boundary. The builder
    // below owns all rows-merger insertion and OB_BLOCK_SWITCHED retries.
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count; ++i) {
      ObInvSSTableMergeIter *iter = nullptr;
      if (OB_ISNULL(iter = iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null merge iter", K(ret), K(i));
      } else if (iter->is_end()) {
      } else if (OB_FAIL(iter->next_macro_block())) {
        // 统一 MACRO 停留驱动（对齐主线 next_range）：只定位到第一个宏块边界
        // （Level::MACRO），不贪心下钻到 ROW。loser tree 用主线同构的
        // range/row/hybrid 比较（交叠在 cmp 内 NEED_OPEN）；是否整块复用由
        // do_merge_ 三段式 + check_macro_block_op 决定。空 sstable → OB_ITER_END，不入堆。
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to locate merge iter to first macro", K(ret), K(i));
        }
      } else if (iter->is_end()) {
        // next_macro_block 落到 END（无宏块），不入堆。
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rebuild_rows_merger_from_iters_())) {
      LOG_WARN("failed to build rows merger from current iters", K(ret), K(iter_count));
    } else {
      consume_iter_idx_ = INVALID_CONSUME_ITER_IDX;
    }
  }
  return ret;
}

int ObInvSSTableMergeHelper::rebuild_rows_merger_from_iters_()
{
  int ret = OB_SUCCESS;
  const int64_t iter_count = iters_.count();
  if (OB_ISNULL(rows_merger_) || OB_ISNULL(cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows merger or comparator is null", K(ret), KP_(rows_merger), KP_(cmp));
  }

  // Same protocol as ObPartitionMergeHelper::build_rows_merger(): a comparator
  // may receive OB_BLOCK_SWITCHED after opening a range. Discard the partial
  // tree and reinsert every live iterator so cached child matches see new keys.
  do {
    rows_merger_->reuse();
    if (OB_FAIL(rows_merger_->open(iter_count))) {
      LOG_WARN("failed to reopen rows merger", K(ret), K(iter_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_count; ++i) {
      ObInvSSTableMergeIter *iter = iters_.at(i);
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null merge iter while rebuilding rows merger", K(ret), K(i));
      } else if (iter->is_end()) {
      } else if (OB_FAIL(normalize_iter_for_merge_range_(*iter))) {
        LOG_WARN("failed to normalize merge iter while rebuilding rows merger",
            K(ret), K(i), KPC(iter));
      } else if (!iter->is_end()) {
        ObInvSSTableMergeLoserTreeItem item;
        item.iter_ = iter;
        item.iter_idx_ = i;
        if (OB_FAIL(rows_merger_->push(item))) {
          if (OB_BLOCK_SWITCHED != ret) {
            LOG_WARN("failed to reinsert merge iter", K(ret), K(i));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !rows_merger_->empty() && OB_FAIL(rows_merger_->rebuild())) {
      if (OB_BLOCK_SWITCHED != ret) {
        LOG_WARN("failed to rebuild rows merger from current iters", K(ret), K(iter_count));
      }
    }
  } while (OB_BLOCK_SWITCHED == ret);
  return ret;
}

int ObInvSSTableMergeHelper::normalize_iter_for_merge_range_(ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  // ROW 只能由 next_row() 产出，已经做过 task range 的逐行过滤。这里仅处理尚未
  // 物化的块级 range，避免给每一行的 push_top() 增加两次 rowkey 比较。
  while (OB_SUCC(ret) && !iter.is_end() && OB_ISNULL(iter.get_curr_row())) {
    ObDatumRange curr_range;
    bool within_range = false;
    if (OB_FAIL(iter.get_curr_range(curr_range))) {
      LOG_WARN("failed to get iter range before rows merger push", K(ret), K(iter));
    } else if (OB_FAIL(iter.range_within_merge_range(curr_range, within_range))) {
      LOG_WARN("failed to check iter range against task range", K(ret), K(curr_range), K(iter));
    } else if (within_range) {
      break;
    } else {
      if (ObInvSSTableMergeIter::Level::LOGICAL_BLOCK == iter.get_curr_level()) {
        // 保持原有逻辑块复用统计语义：边界块虽改为入树前打开，仍计为 RANGE fallback。
        ++range_boundary_open_count_;
      }
      if (OB_FAIL(iter.open_curr_range(false /*for_rewrite*/, true /*for_compare*/))) {
        if (OB_ITER_END == ret || OB_BLOCK_SWITCHED == ret) {
          // Outside the rows merger, a range switch only means that the current
          // boundary range was skipped. Continue normalizing the new position.
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to open boundary range outside rows merger",
              K(ret), K(curr_range), K(iter));
        }
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeHelper::rebuild_rows_merger()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge helper is not inited", K(ret));
  } else if (OB_ISNULL(rows_merger_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows merger is null", K(ret));
  } else if (OB_ISNULL(cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows merger comparator is null", K(ret));
  } else if (consume_iter_idx_ < 0 || consume_iter_idx_ >= iters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected consume iter idx", K(ret), K_(consume_iter_idx), "iter_count", iters_.count());
  } else {
    ObInvSSTableMergeIter *iter = iters_.at(consume_iter_idx_);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null merge iter", K(ret), K_(consume_iter_idx));
    } else if (OB_FAIL(normalize_iter_for_merge_range_(*iter))) {
      LOG_WARN("failed to normalize consumed iter before push top",
          K(ret), K_(consume_iter_idx), KPC(iter));
    } else if (!iter->is_end()) {
      ObInvSSTableMergeLoserTreeItem item;
      item.iter_ = iter;
      item.iter_idx_ = consume_iter_idx_;
      if (OB_FAIL(rows_merger_->push_top(item))) {
        if (OB_BLOCK_SWITCHED != ret) {
          LOG_WARN("failed to push top item", K(ret), K_(consume_iter_idx));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(consume_iter_idx_ = INVALID_CONSUME_ITER_IDX)) {
    } else if (!rows_merger_->empty()) {
      if (OB_FAIL(rows_merger_->rebuild())) {
        if (OB_BLOCK_SWITCHED != ret) {
          LOG_WARN("failed to rebuild rows merger", K(ret));
        }
      }
    }
    if (OB_BLOCK_SWITCHED == ret) {
      if (OB_FAIL(rebuild_rows_merger_from_iters_())) {
        LOG_WARN("failed to rebuild all rows merger players after range switched", K(ret));
      } else {
        consume_iter_idx_ = INVALID_CONSUME_ITER_IDX;
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeHelper::find_minimum_iters(
    ObInvSSTableMergeIter *&winner_iter,
    int64_t &winner_count)
{
  int ret = OB_SUCCESS;
  winner_iter = nullptr;
  winner_count = 0;
  consume_iter_idx_ = INVALID_CONSUME_ITER_IDX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(rows_merger_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows merger is null", K(ret));
  } else if (rows_merger_->empty()) {
    ret = OB_ITER_END;
  } else if (!rows_merger_->is_unique_champion()) {
    // 比较面已在 cmp() 内对交叠 range 做 NEED_OPEN 下钻；此处非唯一 champion
    // 表示多个来源落到同一 (term, doc_id) 行键，破坏倒排跨源唯一不变式。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("duplicate async index merge key from multiple sstables", K(ret));
    log_duplicate_key_diagnose_();
  } else {
    const ObInvSSTableMergeLoserTreeItem *top_item = nullptr;
    if (OB_FAIL(rows_merger_->top(top_item))) {
      LOG_WARN("failed to get top item from rows merger", K(ret), KPC(rows_merger_));
    } else if (OB_ISNULL(top_item) || !top_item->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid top item from rows merger", K(ret), KP(top_item));
    } else {
      ObInvSSTableMergeIter *winner = top_item->iter_;
      const int64_t winner_idx = top_item->iter_idx_;
      if (OB_FAIL(rows_merger_->pop())) {
        LOG_WARN("failed to pop rows merger", K(ret), KPC(rows_merger_));
      } else {
        winner_iter = winner;
        winner_count = 1;
        consume_iter_idx_ = winner_idx;
      }
    }
  }
  return ret;
}

// find_minimum_iters() 在 !is_unique_champion() 时已经把 ret 设成
// OB_ERR_UNEXPECTED。本函数只做现场 dump：取出堆顶 champion 的
// (term, doc_id)，再扫一遍输入 iter，把当前停在同一把 key 上的源打出来。
// 全程 tmp_ret，诊断失败不能改调用方已经定下的错误码。
void ObInvSSTableMergeHelper::log_duplicate_key_diagnose_()
{
  int tmp_ret = OB_SUCCESS;
  const ObInvSSTableMergeLoserTreeItem *top_item = nullptr;
  ObDatumRowkey duplicate_key;
  if (OB_ISNULL(rows_merger_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "cannot diagnose duplicate key with null rows merger");
  } else if (OB_SUCCESS != (tmp_ret = rows_merger_->top(top_item))) {
    LOG_WARN_RET(tmp_ret, "failed to get duplicate key champion", KPC(rows_merger_));
  } else if (OB_ISNULL(top_item) || OB_ISNULL(top_item->iter_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid duplicate key champion", KP(top_item));
  } else if (OB_SUCCESS != (tmp_ret = top_item->iter_->get_curr_key(duplicate_key))) {
    LOG_WARN_RET(tmp_ret, "failed to get duplicate champion key", KPC(top_item->iter_));
  } else {
    for (int64_t i = 0; i < iters_.count(); ++i) {
      ObInvSSTableMergeIter *iter = iters_.at(i);
      ObDatumRowkey curr_key;
      int cmp_ret = 0;
      // 还停在 range/macro 上、没有 curr row 的 iter 比不了行键，跳过。
      if (OB_ISNULL(iter) || OB_ISNULL(iter->get_curr_row())) {
      } else if (OB_SUCCESS != (tmp_ret = iter->get_curr_key(curr_key))) {
        LOG_WARN_RET(tmp_ret, "failed to get duplicate source key", K(i), KPC(iter));
      } else if (OB_SUCCESS != (tmp_ret = curr_key.compare(
          duplicate_key, read_info_.get_datum_utils(), cmp_ret))) {
        LOG_WARN_RET(tmp_ret, "failed to compare duplicate source key",
            K(i), K(curr_key), K(duplicate_key), KPC(iter));
      } else if (0 == cmp_ret) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "duplicate async index merge key source",
            K(i), K(curr_key), KPC(iter));
      }
    }
  }
}

// Whole-macro reuse registers the source MacroBlockId directly and bypasses all posting/value
// decoders and output encoders. Schema-dependent row layout and encryption must therefore match the
// target. Compression is self-describing in each macro block, so a reused macro may keep its source
// compressor even when it differs from the output descriptor. A normal incompatible layout is
// reported through is_compatible=false so the caller can open the macro and rewrite it at a finer
// granularity; malformed source metadata remains an error.
// macro_meta.val_.rowkey_count_ is the declared physical layout, while append_macro_block()
// independently validates the actual end_key_ payload at the writer boundary.
int ObInvSSTableMergeHelper::check_macro_block_reuse_compatibility_(
    const ObInvSSTableMergeIter &winner_iter,
    const ObMacroBlockDesc &macro_desc,
    bool &is_compatible) const
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  const storage::ObITable *table = winner_iter.get_table();
  const ObDataMacroBlockMeta *macro_meta = macro_desc.macro_meta_;
  if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_inv_sstable()) || OB_ISNULL(macro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid source table or macro meta for reuse compatibility",
        K(ret), KP(table), KP(macro_meta), K(macro_desc));
  } else {
    const ObInvSSTable *inv_sstable = static_cast<const ObInvSSTable *>(
        static_cast<const ObSSTable *>(table));
    const ObInvIdxStatsMeta &source_inv_meta = inv_sstable->get_inv_meta().get_stats();
    if (OB_UNLIKELY(!source_inv_meta.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid source inverted sstable meta", K(ret), K(source_inv_meta));
    } else if (OB_ISNULL(data_store_desc_)
        || OB_UNLIKELY(!data_store_desc_->is_valid())) {
      // Without a valid output descriptor physical compatibility cannot be proven. Keep the default
      // false and force the caller down to a finer-grained rewrite path.
    } else {
      const common::ObIArray<share::schema::ObColDesc> &col_descs =
          data_store_desc_->get_full_stored_col_descs();
      const int64_t doc_id_col_idx = inv_idx_row_desc_.doc_id_col_idx_;
      // Match the layout check used when the writer merges reused macro metadata.
      is_compatible = doc_id_col_idx >= 0 && doc_id_col_idx < col_descs.count()
          && source_inv_meta.doc_id_obj_meta_ == col_descs.at(doc_id_col_idx).col_type_
          && source_inv_meta.index_type_ == inv_idx_row_desc_.index_type_
          && source_inv_meta.has_relevance_params_ == (inv_idx_row_desc_.token_freq_col_idx_ >= 0)
          && source_inv_meta.has_position_list_ == (inv_idx_row_desc_.pos_list_col_idx_ >= 0)
          && source_inv_meta.has_nested_key_ == (inv_idx_row_desc_.nested_key_col_idx_ >= 0);
      if (is_compatible) {
        const ObDataBlockMetaVal &meta_val = macro_meta->val_;
        is_compatible = meta_val.column_count_ == data_store_desc_->get_row_column_count()
            && meta_val.rowkey_count_ == data_store_desc_->get_rowkey_column_count()
            && common::INV_IDX_ROW_STORE == meta_val.row_store_type_
            && meta_val.encrypt_id_ == data_store_desc_->get_encrypt_id()
            && meta_val.master_key_id_ == data_store_desc_->get_master_key_id()
            && 0 == MEMCMP(meta_val.encrypt_key_, data_store_desc_->get_encrypt_key(),
                data_store_desc_->get_encrypt_key_size());
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeHelper::check_macro_block_op(
    ObInvSSTableMergeIter &winner_iter,
    const ObMacroBlockDesc &macro_desc,
    compaction::ObBlockOp &block_op)
{
  int ret = OB_SUCCESS;
  block_op.set_none();
  last_macro_block_fallback_ = MacroBlockFallback::NONE;
  bool within_range = false;
  bool is_compatible = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge helper not init", K(ret));
  } else if (OB_UNLIKELY(nullptr != winner_iter.get_curr_row()
                         || winner_iter.is_macro_block_opened())) {
    // 宏块入口前置条件：winner 停在 MACRO 边界且未下钻。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_macro_block_op precondition violated", K(ret),
        KP(winner_iter.get_curr_row()), "macro_opened", winner_iter.is_macro_block_opened());
  } else if (OB_FAIL(winner_iter.curr_macro_within_merge_range(macro_desc, within_range))) {
    LOG_WARN("fail to check macro within merge range", K(ret), K(macro_desc));
  } else if (!within_range) {
    // 跨 merge range 起止端点，不重用。
    block_op.set_open();
    last_macro_block_fallback_ = MacroBlockFallback::RANGE;
  } else if (OB_FAIL(check_macro_block_reuse_compatibility_(
      winner_iter, macro_desc, is_compatible))) {
    LOG_WARN("failed to check inverted macro block reuse compatibility",
        K(ret), K(macro_desc));
  } else if (!is_compatible) {
    // 源段语义或宏块物理格式与输出 schema 不兼容，下钻后重新编码。
    block_op.set_open();
    last_macro_block_fallback_ = MacroBlockFallback::FORMAT;
  } else {
    // merge range 门通过。删除位图的宏块级判定依赖真实 min/max doc_id，
    // 由 merger 在读取 n-1 索引微块统计后通过 contains_any_in_range 完成。
    // 源间交叠已由 loser-tree 比较面在 NEED_OPEN 路径消解；本函数只管块内语义。
    block_op.set_none();
  }
  return ret;
}

int ObInvSSTableMergeHelper::check_logical_block_op(
    ObInvSSTableMergeIter &winner_iter,
    const ObPostingBlockDesc &block,
    const ObPostingValueBlockDesc &value_block,
    compaction::ObBlockOp &block_op)
{
  int ret = OB_SUCCESS;
  block_op.set_none();
  last_logical_block_fallback_ = LogicalBlockFallback::NONE;
  bool within_range = false;
  ObDatumRange block_range;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge helper not init", K(ret));
  } else if (OB_UNLIKELY(nullptr != winner_iter.get_curr_row()
                         || !winner_iter.is_micro_block_opened()
                         || ObInvSSTableMergeIter::Level::LOGICAL_BLOCK
                                != winner_iter.get_curr_level())) {
    // 逻辑块入口前置条件：winner 停在 LOGICAL_BLOCK 边界且未物化行。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_logical_block_op precondition violated", K(ret),
        KP(winner_iter.get_curr_row()), "micro_opened", winner_iter.is_micro_block_opened(),
        "level", winner_iter.get_curr_level());
  } else if (OB_UNLIKELY(!block.is_valid())) {
    // 注入 meta 与编码字节均来自源 sstable，desc 无效即损坏；回退 ROW 同样无法解码。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid posting block desc", K(ret), K(block));
  } else if (OB_FAIL(winner_iter.get_curr_range(block_range))) {
    LOG_WARN("fail to get current logical block range", K(ret), K(block));
  } else if (OB_FAIL(winner_iter.range_within_merge_range(block_range, within_range))) {
    LOG_WARN("fail to check logical block within merge range", K(ret), K(block_range));
  } else if (!within_range) {
    // 跨 merge range 起止端点，不重用。
    block_op.set_open();
    last_logical_block_fallback_ = LogicalBlockFallback::RANGE;
  } else {
    // 持久化格式/schema 兼容判定 + 删除范围判定：posting/value 自描述格式必须
    // 可读（header version、column meta/offset/size、row count），flags 必须与
    // 输出 schema 一致。不比较外层 micro compression/encryption（输出 writer
    // 会重新施加），也不要求源列编码等于 writer 默认编码（posting block 自描述）。
    const bool schema_has_relevance = inv_idx_row_desc_.token_freq_col_idx_ >= 0;
    const bool schema_has_value = inv_idx_row_desc_.has_posting_value();
    bool has_deletion = false;
    ObPostingBlockDecoder posting_decoder;
    if (OB_FAIL(posting_decoder.init(block.buf_, block.size_, allocator_))) {
      // header/meta 损坏或列编码不受当前 reader 支持：回退 ROW 同样无法安全解码。
      LOG_WARN("posting block is not readable by current decoder", K(ret), K(block));
    } else if (OB_UNLIKELY(posting_decoder.get_row_count() != block.doc_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("posting header row count mismatch with injected meta", K(ret),
          K(block), "header_row_count", posting_decoder.get_row_count());
    } else if (OB_FAIL(check_logical_block_deletion_(
        posting_decoder, block.min_id_, block.max_id_, has_deletion))) {
      LOG_WARN("fail to check logical block deletion", K(ret), K(block));
    } else if (has_deletion) {
      // 删除位图命中 envelope 即保守回退，不再解码块内 doc-id 做精确确认。
      block_op.set_open();
      last_logical_block_fallback_ = LogicalBlockFallback::DELETION;
    } else if (posting_decoder.has_relevance_params() != schema_has_relevance) {
      // 可读但 relevance flags 与输出 schema 不一致，不可原样复用。
      block_op.set_open();
      last_logical_block_fallback_ = LogicalBlockFallback::FORMAT;
    } else if (schema_has_value != value_block.is_valid()) {
      // 有 posting value 的 schema 缺源 value 块（或无 value 的 schema 多出
      // value 块）：逐行物化同样无法保持输出 schema，因此不能回退并丢弃数据。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("posting value block presence does not match output schema", K(ret),
          K(schema_has_value), K(value_block), K(block));
    } else if (schema_has_value) {
      ObPostingValueBlockReader value_reader;
      if (OB_FAIL(value_reader.init(value_block.buf_, value_block.size_))) {
        LOG_WARN("posting value block is not readable by current reader", K(ret),
            K(value_block));
      } else if (OB_UNLIKELY(value_block.row_count_ != block.doc_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("posting and value block row count mismatch", K(ret),
            K(block), K(value_block));
      } else {
        const ObPostingValueBlockHeader *value_header =
            reinterpret_cast<const ObPostingValueBlockHeader *>(value_block.buf_);
        const bool flags_match =
            (value_header->has_position_list_ == (inv_idx_row_desc_.pos_list_col_idx_ >= 0))
            && (value_header->has_nested_key_ == (inv_idx_row_desc_.nested_key_col_idx_ >= 0));
        if (!flags_match) {
          block_op.set_open();
          last_logical_block_fallback_ = LogicalBlockFallback::FORMAT;
        }
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeHelper::check_logical_block_deletion_(
    const ObPostingBlockDecoder &posting_decoder,
    const uint64_t min_doc_id,
    const uint64_t max_doc_id,
    bool &has_deletion)
{
  // TODO: 考虑到大量逻辑块可能本身没几行，精确检查开销本身也不小，这个检查留给后面优化。
  UNUSED(posting_decoder);
  int ret = OB_SUCCESS;
  has_deletion = has_deletion_in_range(min_doc_id, max_doc_id);
  return ret;
}

int ObInvSSTableMergeHelper::check_row_deleted(const ObDatumRow &row, bool &deleted)
{
  int ret = OB_SUCCESS;
  deleted = false;
  const int64_t doc_id_col_idx = inv_idx_row_desc_.doc_id_col_idx_;
  if (!deletion_bitmap_handle_.is_valid()) {
    // no deletion bitmap in this merge round
  } else if (OB_UNLIKELY(doc_id_col_idx < 0
      || doc_id_col_idx >= row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid doc id column index", K(ret), K(doc_id_col_idx), K(row));
  } else {
    deleted = deletion_bitmap_handle_.contains(
        static_cast<int64_t>(row.storage_datums_[doc_id_col_idx].get_uint()));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

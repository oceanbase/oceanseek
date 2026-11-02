/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/function/ob_function.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "ob_sstable_posting_scanner.h"

namespace oceanbase
{
namespace blocksstable
{

namespace
{

class ObOrderedPostingBlockMetaVisitor final
{
public:
  ObOrderedPostingBlockMetaVisitor(
      const uint64_t end_id,
      const common::ObFunction<int(const ObPostingBlockMeta &, bool &)> &visitor)
    : end_id_(end_id),
      visitor_(visitor),
      range_covered_(false),
      has_previous_block_(false),
      previous_max_id_(0)
  {}

  int operator()(const ObPostingBlockMeta &block_meta, bool &stop)
  {
    int ret = OB_SUCCESS;
    stop = false;
    if (OB_UNLIKELY(has_previous_block_ && block_meta.min_id_ <= previous_max_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prefetched posting block metadata is not strictly ordered",
          K(ret), K_(previous_max_id), K(block_meta));
    } else {
      has_previous_block_ = true;
      previous_max_id_ = block_meta.max_id_;
      if (block_meta.min_id_ > end_id_) {
        range_covered_ = true;
        stop = true;
      } else if (OB_FAIL(visitor_(block_meta, stop))) {
        LOG_WARN("failed to visit ordered posting block meta", K(ret), K(block_meta));
      }
      if (OB_SUCC(ret) && block_meta.max_id_ >= end_id_) {
        range_covered_ = true;
        stop = true;
      }
    }
    return ret;
  }

  bool is_range_covered() const { return range_covered_; }

private:
  uint64_t end_id_;
  const common::ObFunction<int(const ObPostingBlockMeta &, bool &)> &visitor_;
  bool range_covered_;
  bool has_previous_block_;
  uint64_t previous_max_id_;
};

} // namespace

ObSSTablePostingScanner::ObSSTablePostingScanner()
  : data_io_allocator_(),
    sstable_index_scanner_(),
    meta_row_scanner_(),
    data_ring_(),
    leaf_pin_slots_(),
    last_leaf_pin_idx_(-1),
    scan_range_(),
    scan_param_(),
    scan_key_datums_(nullptr),
    term_key_(),
    skip_datums_(nullptr),
    skip_key_(),
    scan_allocator_(nullptr),
    data_read_idx_(0),
    data_prefetch_idx_(0),
    data_prefetch_depth_(0),
    doc_id_col_idx_(-1),
    data_io_enabled_(false),
    idx_iter_end_(false),
    posting_index_block_opened_(false),
    scan_iter_end_(false),
    need_posting_value_(false),
    index_type_(INV_IDX_TYPE_INVALID),
    is_inited_(false)
{
}

void ObSSTablePostingScanner::DataPrefetchItem::reset()
{
  // The leaf pin slot reference is released by the scanner (release_leaf_pin_slot) before
  // this is called; here we only drop this item's own state.
  data_handle_.reset();
  value_handle_.reset();
  value_row_header_.reset();
  value_row_header_valid_ = false;
  value_micro_offset_ = 0;
  value_micro_size_ = 0;
  index_info_.reset();
  max_doc_id_ = 0;
  io_issued_ = false;
  requested_ = false;
  value_io_issued_ = false;
  value_requested_ = false;
  leaf_pin_idx_ = -1;
  leaf_pin_generation_ = 0;
}

void ObSSTablePostingScanner::reset_scan_status()
{
  for (int64_t i = 0; i < DATA_RING_CAP; ++i) {
    release_leaf_pin_slot(data_ring_[i]);
    data_ring_[i].reset();
  }
  for (int64_t i = 0; i < LEAF_PIN_SLOT_CAP; ++i) {
    leaf_pin_slots_[i].reset();
  }
  last_leaf_pin_idx_ = -1;
  data_read_idx_ = 0;
  data_prefetch_idx_ = 0;
  posting_index_block_opened_ = false;
  scan_iter_end_ = false;
}

void ObSSTablePostingScanner::reset()
{
  // Reset our pinning handles before sstable_index_scanner_/allocators they may reference.
  reset_scan_status();
  meta_row_scanner_.reset();
  sstable_index_scanner_.reset();
  data_io_allocator_.reset();
  scan_range_.reset();
  scan_param_ = ObSSTableIndexScanParam();
  if (OB_NOT_NULL(scan_key_datums_) && OB_NOT_NULL(scan_allocator_)) {
    scan_allocator_->free(scan_key_datums_);
  }
  scan_key_datums_ = nullptr;
  term_key_.reset();
  skip_datums_ = nullptr;
  skip_key_.reset();
  scan_allocator_ = nullptr;
  data_read_idx_ = 0;
  data_prefetch_depth_ = 0;
  doc_id_col_idx_ = -1;
  data_io_enabled_ = false;
  idx_iter_end_ = false;
  posting_index_block_opened_ = false;
  scan_iter_end_ = false;
  need_posting_value_ = false;
  index_type_ = INV_IDX_TYPE_INVALID;
  is_inited_ = false;
}

int ObSSTablePostingScanner::open(
    const ObITableReadInfo &read_info,
    const ObDatumRowkey &query_key,
    const ObTabletID &tablet_id,
    const ObQueryFlag &query_flag,
    ObInvSSTable &sstable,
    ObIAllocator &scan_allocator,
    const bool need_posting_value)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTablePostingScanner init twice", K(ret));
  } else if (OB_UNLIKELY(!query_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query key", K(ret), K(query_key));
  } else if (OB_FAIL(data_io_allocator_.init(
      nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE, lib::ObMemAttr(MTL_ID(), "SSTIdxDataIO")))) {
    LOG_WARN("failed to init data io allocator", K(ret));
  } else if (OB_FAIL(scan_param_.init(
      read_info, query_flag, tablet_id, ObSSTableIndexScanParam::ScanLevel::LEAF))) {
    LOG_WARN("failed to init scan param", K(ret));
  } else if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get sstable meta", K(ret));
  } else if (OB_UNLIKELY(sstable.get_inv_meta().get_stats().index_type_ <= INV_IDX_TYPE_INVALID
             || sstable.get_inv_meta().get_stats().index_type_ >= INV_IDX_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted index type", K(ret), K(sstable.get_inv_meta()));
  } else {
    scan_allocator_ = &scan_allocator;
    need_posting_value_ = need_posting_value;
    index_type_ = sstable.get_inv_meta().get_stats().index_type_;
    const bool has_max_score = sstable.get_inv_meta().get_stats().has_relevance_params_;
    const int64_t rowkey_column_cnt =
        meta_handle.get_sstable_meta().get_schema_rowkey_column_count();
    if (OB_FAIL(init_scan_keys(query_key, rowkey_column_cnt, scan_allocator))) {
      LOG_WARN("failed to init posting scan keys", K(ret));
    } else if (OB_FAIL(sstable_index_scanner_.init(scan_range_, scan_param_, sstable, scan_allocator))) {
      LOG_WARN("failed to init base index scanner", K(ret));
    } else if (OB_FAIL(meta_row_scanner_.init(has_max_score))) {
      LOG_WARN("failed to init posting meta row scanner", K(ret));
    } else {
      reset_scan_status();
      data_prefetch_depth_ = DEFAULT_DATA_PREFETCH_DEPTH;
      data_io_enabled_ = false;
      idx_iter_end_ = false;
      // currently doc_id would always be the last rowkey column for inverted index
      doc_id_col_idx_ = meta_handle.get_sstable_meta().get_schema_rowkey_column_count() - 1;
      bool got = false;
      if (OB_FAIL(try_prefetch_one_index_row(got))) {
        LOG_WARN("failed to prefetch first index row", K(ret));
      } else if (!got) {
        // empty term, or term reduced to a trailing probe row that was filtered: cursor at end.
        posting_index_block_opened_ = false;
        scan_iter_end_ = true;
      } else if (OB_FAIL(open_cur_read_item_meta())) {
        LOG_WARN("failed to open first read item meta", K(ret));
      } else if (OB_FAIL(try_adaptive_prefetch())) {
        LOG_WARN("failed to pump prefetch after first open", K(ret));
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObSSTablePostingScanner::next_block(bool &block_changed)
{
  int ret = OB_SUCCESS;
  const int64_t prev_data_read_idx = data_read_idx_;
  block_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(meta_row_scanner_.next_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance meta row scanner", K(ret));
    } else {
      // current item's index row exhausted: roll to the next ring item
      ret = OB_SUCCESS;
      advance_data_read_cursor();
      if (!has_read_item() && !idx_iter_end_) {
        if (OB_FAIL(try_adaptive_prefetch())) {
          LOG_WARN("failed to pump prefetch after rolling item", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!has_read_item()) {
        // idx_iter_end_ && ring empty: term exhausted
        scan_iter_end_ = true;
        posting_index_block_opened_ = false;
        ret = OB_ITER_END;
      } else if (OB_FAIL(open_cur_read_item_meta())) {
        LOG_WARN("failed to open next read item meta", K(ret));
      } else if (OB_FAIL(try_adaptive_prefetch())) {
        LOG_WARN("failed to pump prefetch after data block change", K(ret));
      }
    }
  }
  block_changed = prev_data_read_idx != data_read_idx_;
  return ret;
}

int ObSSTablePostingScanner::get_cur_block_meta(ObPostingBlockMeta &posting_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!posting_index_block_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected posting index block not opened", K(ret));
  } else if (OB_FAIL(meta_row_scanner_.get_cur_block_meta(posting_meta))) {
    LOG_WARN("failed to get current posting block meta", K(ret));
  }
  return ret;
}

// 遍历单条已固定的 metadata row，并在到达 row 末尾时校验 agg metadata 的覆盖范围。
template <typename Visitor>
int ObSSTablePostingScanner::visit_prefetched_meta_row(
    const int64_t row_offset,
    const int64_t start_block_idx,
    ObPostingMetaRowScanner &meta_scanner,
    Visitor &visitor,
    bool &stop) const
{
  int ret = OB_SUCCESS;
  const char *agg_buf = nullptr;
  int64_t agg_size = 0;
  uint64_t row_max_doc_id = 0;
  bool row_has_block = false;
  uint64_t row_last_max_id = 0;
  int64_t block_idx = start_block_idx;
  // 清理上一条 row 的借用视图，再打开当前预取 row 的 agg buffer。
  meta_scanner.reuse();
  if (OB_FAIL(get_prefetched_meta_row(
      row_offset, agg_buf, agg_size, row_max_doc_id))) {
    LOG_WARN("failed to get prefetched metadata row", K(ret), K(row_offset));
  } else if (OB_FAIL(meta_scanner.open(agg_buf, agg_size))) {
    LOG_WARN("failed to open prefetched metadata row", K(ret), K(row_offset));
  }
  // 按结构下标读取当前 row 的 block metadata，并将每个有效 block 交给 visitor。
  while (OB_SUCC(ret) && !stop) {
    ObPostingBlockMeta block_meta;
    if (OB_FAIL(meta_scanner.get_block_meta_at(block_idx, block_meta))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get prefetched posting block meta",
            K(ret), K(row_offset), K(block_idx));
      } else {
        ret = OB_SUCCESS;
        if (OB_UNLIKELY(!row_has_block || row_last_max_id != row_max_doc_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("prefetched posting block metadata does not cover index row",
              K(ret), K(row_offset), K(start_block_idx), K(row_has_block),
              K(row_last_max_id), K(row_max_doc_id));
        } else {
          break;
        }
      }
    } else if (OB_UNLIKELY(!block_meta.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid prefetched posting block meta",
          K(ret), K(row_offset), K(block_idx), K(block_meta));
    } else if (OB_FAIL(visitor(block_meta, stop))) {
      // visitor 不得保留 block_meta 中借用的 frontier 指针。
      LOG_WARN("failed to visit prefetched posting block meta",
          K(ret), K(row_offset), K(block_idx));
    } else {
      row_has_block = true;
      row_last_max_id = block_meta.max_id_;
      ++block_idx;
    }
  }
  return ret;
}

// 遍历完整的预取窗口，校验 block 顺序和区间覆盖，并调用业务 visitor。
int ObSSTablePostingScanner::visit_prefetched_meta_rows(
    const uint64_t end_id,
    const int64_t prefetched_row_count,
    const common::ObFunction<int(const ObPostingBlockMeta &, bool &)> &visitor,
    bool &range_covered) const
{
  int ret = OB_SUCCESS;
  const int64_t current_block_idx = meta_row_scanner_.get_cur_block_idx();
  ObPostingMetaRowScanner meta_scanner;
  ObOrderedPostingBlockMetaVisitor ordered_visitor(end_id, visitor);
  bool stop = false;
  if (OB_UNLIKELY(current_block_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current posting block cursor", K(ret), K(current_block_idx));
  } else if (OB_FAIL(meta_scanner.init(true /* 解析 block max frontier */))) {
    LOG_WARN("failed to init prefetched metadata row scanner", K(ret));
  } else {
    // 逐行遍历已经固定的 metadata，直到发生错误、visitor 要求停止或窗口耗尽。
    for (int64_t row_offset = 0;
        OB_SUCC(ret) && !stop && row_offset < prefetched_row_count;
        ++row_offset) {
      // 首行从主 scanner 当前 block 开始，后续行从第一个 block 开始。
      const int64_t start_block_idx = 0 == row_offset ? current_block_idx : 0;
      if (OB_FAIL(visit_prefetched_meta_row(
          row_offset, start_block_idx, meta_scanner, ordered_visitor, stop))) {
        LOG_WARN("failed to visit prefetched metadata row",
            K(ret), K(row_offset), K(start_block_idx));
      }
    }
    if (OB_SUCC(ret) && ordered_visitor.is_range_covered()) {
      range_covered = true;
    } else if (OB_SUCC(ret) && !stop && idx_iter_end_) {
      // 完整的 term 末尾窗口可以闭合一个越过最后一条 posting 的区间。
      range_covered = true;
    }
  }
  return ret;
}

// 遍历当前读游标之后已固定的 posting block metadata，且不推进主 scanner 游标。
int ObSSTablePostingScanner::visit_prefetched_block_meta(
    const uint64_t end_id,
    const common::ObFunction<int(const ObPostingBlockMeta &, bool &)> &visitor,
    bool &range_covered)
{
  int ret = OB_SUCCESS;
  // 默认预取窗口未完整覆盖目标边界，仅在确认覆盖 end_id 或到达 term 末尾后置为 true。
  range_covered = false;
  if (IS_NOT_INIT) {
    // scanner 尚未初始化时不能访问预取窗口。
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    // 当前逻辑游标之后已无可遍历的 posting metadata。
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!posting_index_block_opened_ || !has_read_item())) {
    // scanner 必须持有已打开的当前 metadata row 及对应预取项。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current metadata cursor", K(ret), K_(posting_index_block_opened),
        K_(data_read_idx), K_(data_prefetch_idx));
  } else if (OB_UNLIKELY(!visitor.is_valid())) {
    // visitor 必须有效，避免调用空回调。
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prefetched block meta visitor", K(ret), K(visitor));
  } else {
    // 在不移动逻辑读游标的情况下，将 metadata 预取窗口扩展至目标边界。
    if (OB_FAIL(ensure_meta_lookahead(end_id))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to prepare metadata lookahead", K(ret), K(end_id));
    } else {
      // 获取从当前读游标开始仍由预取 ring 固定的 metadata row 数量。
      const int64_t prefetched_row_count = get_prefetched_meta_row_count();
      if (prefetched_row_count > 0) {
        const DataPrefetchItem &last_item =
            data_ring_[(data_prefetch_idx_ - 1) % DATA_RING_CAP];
        // 最后一条预取 row 覆盖 end_id 或索引扫描已到 term 末尾时，窗口具备完整覆盖能力。
        const bool window_complete = last_item.max_doc_id_ >= end_id || idx_iter_end_;
        if (window_complete && OB_FAIL(visit_prefetched_meta_rows(
            end_id, prefetched_row_count, visitor, range_covered))) {
          LOG_WARN("failed to visit prefetched metadata rows", K(ret), K(end_id));
        }
      }
    }
  }
  return ret;
}

// 按相对偏移借用预取 ring 中 metadata row 的只读 aggregate buffer，并返回其长度和 doc-id 右边界。
int ObSSTablePostingScanner::get_prefetched_meta_row(
    const int64_t row_offset,
    const char *&agg_buf,
    int64_t &agg_size,
    uint64_t &max_doc_id) const
{
  int ret = OB_SUCCESS;
  // 每次借用前重置输出，失败时不暴露上一行的指针和长度。
  agg_buf = nullptr;
  agg_size = 0;
  max_doc_id = 0;
  if (IS_NOT_INIT) {
    // scanner 尚未初始化时返回未初始化错误，不访问预取 ring。
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(row_offset < 0)) {
    // 相对当前 read cursor 的 metadata row 偏移不得为负数。
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prefetched metadata row offset", K(ret), K(row_offset));
  } else if (OB_UNLIKELY(row_offset >= get_prefetched_meta_row_count())) {
    // 偏移超出已固定窗口范围。
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prefetched metadata row offset out of range", K(ret), K(row_offset),
        "prefetched_row_count", get_prefetched_meta_row_count());
  } else {
    // 在计算槽位前先校验相对偏移，避免有符号整数溢出。
    const int64_t ring_idx =
        (data_read_idx_ % DATA_RING_CAP + row_offset) % DATA_RING_CAP;
    const DataPrefetchItem &item = data_ring_[ring_idx];
    if (OB_UNLIKELY(!validate_leaf_pin_ref(item))) {
      // generation/ref-count 不匹配时拒绝返回陈旧 agg buffer。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid leaf pin ref in prefetched metadata row", K(ret), K(row_offset),
          K(item.leaf_pin_idx_), K(item.leaf_pin_generation_));
    } else if (OB_UNLIKELY(OB_ISNULL(item.index_info_.agg_row_buf_)
        || item.index_info_.agg_buf_size_ <= 0)) {
      // ring item 未保存有效 aggregate buffer 时拒绝返回借用视图。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prefetched metadata row has no agg buffer", K(ret), K(row_offset));
    } else {
      // 返回由 leaf pin 保活的只读 agg buffer 借用视图。
      agg_buf = item.index_info_.agg_row_buf_;
      agg_size = item.index_info_.agg_buf_size_;
      // 同时返回该 metadata row 的 doc-id 右边界，供覆盖性检查使用。
      max_doc_id = item.max_doc_id_;
    }
  }
  return ret;
}

int ObSSTablePostingScanner::ensure_meta_lookahead(const uint64_t end_id)
{
  int ret = OB_SUCCESS;
  bool got = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!has_read_item())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no read item for metadata lookahead", K(ret),
        K_(data_read_idx), K_(data_prefetch_idx));
  } else {
    while (OB_SUCC(ret) && got && !idx_iter_end_
        && (data_prefetch_idx_ - data_read_idx_) < data_prefetch_depth_) {
      const DataPrefetchItem &last_item =
          data_ring_[(data_prefetch_idx_ - 1) % DATA_RING_CAP];
      if (last_item.max_doc_id_ >= end_id) {
        break;
      } else if (OB_FAIL(try_prefetch_one_index_row(got))) {
        LOG_WARN("failed to extend metadata lookahead", K(ret), K(end_id));
      }
    }
  }
  return ret;
}

int ObSSTablePostingScanner::get_cur_micro_block(ObMicroBlockData &data_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!posting_index_block_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected posting index block not opened", K(ret));
  } else if (OB_UNLIKELY(!has_read_item())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current data meta item not scanned", K(ret), K_(data_read_idx), K_(data_prefetch_idx));
  } else {
    DataPrefetchItem &item = cur_data_item();
    if (!item.io_issued_) {
      // do prefetch when data block io is not issued
      if (OB_FAIL(prefetch_one_data_block(item.index_info_, item.data_handle_))) {
        LOG_WARN("failed to fetch current data block", K(ret));
      } else {
        item.io_issued_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(item.data_handle_.get_micro_block_data(nullptr, data_block, true))) {
      LOG_WARN("failed to materialize current data block", K(ret));
      item.data_handle_.reset();
      item.io_issued_ = false;
    } else {
      item.requested_ = true;
      if (!data_io_enabled_) {
        // First read demand: turn on adaptive prefetch. Newly scanned index rows are prefetched.
        data_io_enabled_ = true;
        if (OB_FAIL(try_adaptive_prefetch())) {
          LOG_WARN("failed to deepen prefetch window", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSTablePostingScanner::get_cur_value_micro_block(ObMicroBlockData &value_block)
{
  int ret = OB_SUCCESS;
  value_block.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!need_posting_value_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("posting value access is disabled for this scan", K(ret));
  } else if (OB_UNLIKELY(!posting_index_block_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected posting index block not opened", K(ret));
  } else if (OB_UNLIKELY(!has_read_item())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current data meta item not scanned", K(ret), K_(data_read_idx), K_(data_prefetch_idx));
  } else {
    DataPrefetchItem &item = cur_data_item();
    if (0 == item.value_micro_size_) {
      // Segment without posting value column: caller skips position access.
    } else if (!item.value_io_issued_) {
      if (OB_FAIL(prefetch_one_value_block(item, item.value_handle_))) {
        LOG_WARN("failed to fetch current value block", K(ret));
      } else {
        item.value_io_issued_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (item.value_micro_size_ > 0
        && OB_FAIL(item.value_handle_.get_micro_block_data(nullptr, value_block, true))) {
      LOG_WARN("failed to materialize current value block", K(ret));
      item.value_handle_.reset();
      item.value_io_issued_ = false;
    } else if (item.value_micro_size_ > 0) {
      item.value_requested_ = true;
    }
  }
  return ret;
}

int ObSSTablePostingScanner::advance_to(const uint64_t target_id, const bool inclusive, bool &block_changed)
{
  int ret = OB_SUCCESS;
  const int64_t prev_data_read_idx = data_read_idx_;
  block_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(scan_iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(UINT64_MAX == target_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target id", K(ret), K(target_id));
  } else if (OB_UNLIKELY(!has_read_item())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no read item on advance_to", K(ret), K_(data_read_idx), K_(data_prefetch_idx));
  } else {
    const uint64_t eff_target = inclusive ? target_id : target_id + 1;
    if (cur_data_item().max_doc_id_ >= eff_target) {
      // target inside the current item
      bool beyond = false;
      if (OB_FAIL(meta_row_scanner_.advance_to(eff_target, beyond))) {
        LOG_WARN("failed to advance meta row scanner within current item", K(ret), K(eff_target));
      } else if (OB_UNLIKELY(beyond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected beyond after advance within current item", K(ret), K(eff_target));
      }
    } else {
      // linear search the ring for first item contains target
      while (OB_SUCC(ret) && has_read_item() && cur_data_item().max_doc_id_ < eff_target) {
        advance_data_read_cursor();
      }
      if (OB_FAIL(ret)) {
      } else if (has_read_item()) {
        bool beyond = false;
        if (OB_FAIL(open_cur_read_item_meta())) {
          LOG_WARN("failed to open read item meta during advance", K(ret));
        } else if (OB_FAIL(meta_row_scanner_.advance_to(eff_target, beyond))) {
          LOG_WARN("failed to advance meta row scanner on prefetched item", K(ret), K(eff_target));
        } else if (OB_UNLIKELY(beyond)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected beyond after advance to prefetched item", K(ret), K(eff_target));
        }
      } else if (idx_iter_end_) {
        scan_iter_end_ = true;
        posting_index_block_opened_ = false;
        ret = OB_ITER_END;
      } else {
        // target beyond the prefetched window
        bool got = false;
        bool beyond = false;
        set_skip_key_doc_id(eff_target);
        if (OB_FAIL(sstable_index_scanner_.advance_to(skip_key_, true /*inclusive*/))) {
          LOG_WARN("failed to advance sstable index scanner", K(ret), K(eff_target));
        } else if (OB_FAIL(try_prefetch_one_index_row(got))) {
          LOG_WARN("failed to prefetch index row after advance", K(ret));
        } else if (!got) {
          scan_iter_end_ = true;
          posting_index_block_opened_ = false;
          ret = OB_ITER_END;
        } else if (OB_FAIL(open_cur_read_item_meta())) {
          LOG_WARN("failed to open read item meta after re-seek", K(ret));
        } else if (OB_FAIL(meta_row_scanner_.advance_to(eff_target, beyond))) {
          LOG_WARN("failed to advance meta row scanner", K(ret), K(eff_target));
        } else if (OB_UNLIKELY(beyond)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected beyond_range after advance to new item", K(ret), K(eff_target));
        }
      }
    }

    if (OB_SUCC(ret) && posting_index_block_opened_) {
      if (OB_FAIL(try_adaptive_prefetch())) {
        LOG_WARN("failed to pump prefetch after advance", K(ret));
      }
    }
  }
  block_changed = prev_data_read_idx != data_read_idx_;
  return ret;
}

int ObSSTablePostingScanner::try_prefetch_one_index_row(bool &row_scanned)
{
  int ret = OB_SUCCESS;
  row_scanned = false;
  DataPrefetchItem &item = cur_prefetch_item();
  if (idx_iter_end_) {
    // nothing to scan
  } else if (OB_UNLIKELY(is_data_window_full())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data prefetch window full", K(ret), K_(data_read_idx), K_(data_prefetch_idx));
  } else if (FALSE_IT(item.reset())) {
  } else if (OB_FAIL(sstable_index_scanner_.get_next(item.index_info_))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      idx_iter_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next index info", K(ret));
    }
  } else if (OB_FAIL(check_row_in_term(item.index_info_, idx_iter_end_))) {
    LOG_WARN("failed to check term boundary", K(ret));
  } else if (idx_iter_end_) {
    // trailing probe row beyond current term: drop it, do not put into ring
    item.reset();
  } else {
    int64_t leaf_pin_idx = -1;
    uint64_t leaf_pin_generation = 0;
    if (OB_FAIL(acquire_leaf_pin_slot(leaf_pin_idx, leaf_pin_generation))) {
      LOG_WARN("failed to acquire leaf pin slot", K(ret));
    } else {
      item.leaf_pin_idx_ = leaf_pin_idx;
      item.leaf_pin_generation_ = leaf_pin_generation;
      if (OB_FAIL(get_doc_id_from_endkey(item.index_info_, item.max_doc_id_))) {
        LOG_WARN("failed to get doc id from endkey", K(ret), K(item.index_info_));
        abandon_prefetch_item(item);
      } else if (OB_FAIL(fill_value_locator_from_agg(item))) {
        LOG_WARN("failed to fill value locator from agg", K(ret), K(item.index_info_));
        abandon_prefetch_item(item);
      } else {
        ++data_prefetch_idx_;
        row_scanned = true;
        if (data_io_enabled_ && OB_FAIL(prefetch_item_blocks(item))) {
          LOG_WARN("failed to prefetch posting blocks", K(ret), K(item.index_info_));
          rollback_prefetched_index_row(item, row_scanned);
        }
      }
    }
  }
  return ret;
}

void ObSSTablePostingScanner::abandon_prefetch_item(DataPrefetchItem &item)
{
  release_leaf_pin_slot(item);
  item.reset();
}

void ObSSTablePostingScanner::rollback_prefetched_index_row(
    DataPrefetchItem &item,
    bool &row_scanned)
{
  --data_prefetch_idx_;
  row_scanned = false;
  abandon_prefetch_item(item);
}

int ObSSTablePostingScanner::check_row_in_term(const ObMicroIndexInfo &index_info, bool &iter_end)
{
  int ret = OB_SUCCESS;
  if (sstable_index_scanner_.reached_last_row_in_range()) {
    // Index scanner may return one trailing probe row beyond current term. Compare the
    // term-prefix rowkey to decide whether this row still belongs to the current term.
    int cmp_ret = 0;
    if (OB_FAIL(index_info.endkey_.compare(
        term_key_,
        scan_param_.get_index_read_info()->get_datum_utils(),
        cmp_ret,
        false /*compare_datum_cnt*/))) {
      LOG_WARN("failed to compare index endkey term prefix", K(ret), K(index_info));
    } else if (0 != cmp_ret) {
      iter_end = true;
    }
  }
  return ret;
}

// Adaptive prefetch of posting data micro blocks.
//
// Strategy:
//   - Before the first posting-data read, retain only the current metadata row.
//   - After posting-data demand is observed, prefetch metadata and data/value IO to the
//     configured depth. Metadata-only rows pinned by range-max are backfilled with IO.
//   - advance_data_read_cursor() flips data_io_enabled_ back off whenever a prefetched block
//     is skipped without being read, so prefetch self-disables on non-sequential / skip-heavy
//     access patterns to avoid wasted IO.
int ObSSTablePostingScanner::try_adaptive_prefetch()
{
  int ret = OB_SUCCESS;
  const int64_t target_prefetch_depth = data_io_enabled_ ? data_prefetch_depth_ : 1;
  bool got = true;
  // Range-max can fill the ring before posting-data demand starts. Once demand is
  // observed, issue asynchronous IO for those committed lookahead rows as well.
  for (int64_t item_idx = data_read_idx_ + 1;
      OB_SUCC(ret) && data_io_enabled_ && item_idx < data_prefetch_idx_;
      ++item_idx) {
    DataPrefetchItem &item = data_ring_[item_idx % DATA_RING_CAP];
    if (OB_FAIL(prefetch_item_blocks(item))) {
      LOG_WARN("failed to backfill posting block prefetch", K(ret), K(item_idx));
    }
  }
  while (OB_SUCC(ret) && got && !idx_iter_end_
      && (data_prefetch_idx_ - data_read_idx_) < target_prefetch_depth) {
    if (OB_FAIL(try_prefetch_one_index_row(got))) {
      LOG_WARN("failed to pump base row", K(ret));
    }
  }
  return ret;
}

int ObSSTablePostingScanner::acquire_leaf_pin_slot(
    int64_t &slot_idx,
    uint64_t &slot_generation)
{
  int ret = OB_SUCCESS;
  slot_idx = -1;
  slot_generation = 0;
  // Reuse the last prefetched row's slot when this row lives in the same leaf block.
  if (last_leaf_pin_idx_ >= 0 && last_leaf_pin_idx_ < LEAF_PIN_SLOT_CAP
      && leaf_pin_slots_[last_leaf_pin_idx_].ref_cnt_ > 0
      && !sstable_index_scanner_.cur_entered_new_leaf_block()) {
    LeafPinSlot &slot = leaf_pin_slots_[last_leaf_pin_idx_];
    ++slot.ref_cnt_;
    slot_idx = last_leaf_pin_idx_;
    slot_generation = slot.generation_;
  } else {
    // find a free slot. The live item count is < DATA_RING_CAP when prefetching (guarded by
    // is_data_window_full()), and each item holds exactly one ref, so a free slot always exists.
    int64_t free_idx = -1;
    for (int64_t i = 0; i < LEAF_PIN_SLOT_CAP; ++i) {
      if (leaf_pin_slots_[i].ref_cnt_ == 0) {
        free_idx = i;
        break;
      }
    }
    if (OB_UNLIKELY(free_idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no free leaf pin slot", K(ret));
    } else {
      LeafPinSlot &slot = leaf_pin_slots_[free_idx];
      slot.reset();  // bumps generation_ and drops any stale handle
      if (OB_FAIL(sstable_index_scanner_.hold_cur_leaf_block(slot.handle_))) {
        LOG_WARN("failed to hold leaf block for pin slot", K(ret), K(free_idx));
      } else {
        slot.ref_cnt_ = 1;
        last_leaf_pin_idx_ = free_idx;
        slot_idx = free_idx;
        slot_generation = slot.generation_;
      }
    }
  }
  return ret;
}

void ObSSTablePostingScanner::release_leaf_pin_slot(DataPrefetchItem &item)
{
  if (validate_leaf_pin_ref(item)) {
    LeafPinSlot &slot = leaf_pin_slots_[item.leaf_pin_idx_];
    --slot.ref_cnt_;
    if (0 == slot.ref_cnt_) {
      slot.reset();  // release handle and bump generation_ so stale refs fail validation
    }
  }
  item.leaf_pin_idx_ = -1;
  item.leaf_pin_generation_ = 0;
}

bool ObSSTablePostingScanner::validate_leaf_pin_ref(const DataPrefetchItem &item) const
{
  return item.leaf_pin_idx_ >= 0 && item.leaf_pin_idx_ < LEAF_PIN_SLOT_CAP
      && leaf_pin_slots_[item.leaf_pin_idx_].generation_ == item.leaf_pin_generation_
      && leaf_pin_slots_[item.leaf_pin_idx_].ref_cnt_ > 0;
}

int ObSSTablePostingScanner::open_cur_read_item_meta()
{
  int ret = OB_SUCCESS;
  DataPrefetchItem &item = cur_data_item();
  meta_row_scanner_.reuse();
  if (OB_UNLIKELY(!has_read_item())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no read item to open meta on", K(ret), K_(data_read_idx), K_(data_prefetch_idx));
  } else if (OB_UNLIKELY(!validate_leaf_pin_ref(item))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid leaf pin ref in read item", K(ret), K(item.leaf_pin_idx_), K(item.leaf_pin_generation_));
  } else if (OB_UNLIKELY(OB_ISNULL(item.index_info_.agg_row_buf_) || item.index_info_.agg_buf_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read item has no agg buf", K(ret));
  } else if (OB_FAIL(meta_row_scanner_.open(item.index_info_.agg_row_buf_, item.index_info_.agg_buf_size_))) {
    LOG_WARN("failed to open meta row scanner on item agg", K(ret));
  } else {
    posting_index_block_opened_ = true;
  }
  return ret;
}

int ObSSTablePostingScanner::init_scan_keys(
    const ObDatumRowkey &query_key,
    const int64_t rowkey_column_cnt,
    ObIAllocator &scan_allocator)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  static constexpr int64_t SCAN_KEY_CNT = 3;
  if (OB_UNLIKELY(rowkey_column_cnt != query_key.get_datum_cnt() + 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey column count", K(ret), K(rowkey_column_cnt), K(query_key.get_datum_cnt()));
  } else if (OB_ISNULL(buf = scan_allocator.alloc(
      SCAN_KEY_CNT * rowkey_column_cnt * sizeof(ObStorageDatum)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate posting scan key datums", K(ret), K(rowkey_column_cnt));
  } else {
    scan_key_datums_ = new(buf) ObStorageDatum[SCAN_KEY_CNT * rowkey_column_cnt];
    ObStorageDatum *start_datums = scan_key_datums_;
    ObStorageDatum *end_datums = scan_key_datums_ + rowkey_column_cnt;
    skip_datums_ = scan_key_datums_ + 2 * rowkey_column_cnt;
    for (int64_t i = 0; i < query_key.get_datum_cnt(); ++i) {
      start_datums[i] = query_key.get_datum(i);
      end_datums[i] = query_key.get_datum(i);
      skip_datums_[i] = query_key.get_datum(i);
    }
    start_datums[rowkey_column_cnt - 1].set_min();
    end_datums[rowkey_column_cnt - 1].set_max();
    skip_datums_[rowkey_column_cnt - 1].set_min();
    if (OB_FAIL(scan_range_.get_start_key().assign(start_datums, rowkey_column_cnt))) {
      LOG_WARN("failed to assign posting scan start key", K(ret), K(rowkey_column_cnt));
    } else if (OB_FAIL(scan_range_.get_end_key().assign(end_datums, rowkey_column_cnt))) {
      LOG_WARN("failed to assign posting scan end key", K(ret), K(rowkey_column_cnt));
    } else if (OB_FAIL(term_key_.assign(start_datums, query_key.get_datum_cnt()))) {
      LOG_WARN("failed to assign posting term key", K(ret), K(query_key.get_datum_cnt()));
    } else if (OB_FAIL(skip_key_.assign(skip_datums_, rowkey_column_cnt))) {
      LOG_WARN("failed to assign skip key datums", K(ret), K(rowkey_column_cnt));
    } else {
      scan_range_.set_left_closed();
      scan_range_.set_right_closed();
    }
  }
  return ret;
}

void ObSSTablePostingScanner::set_skip_key_doc_id(const uint64_t doc_id)
{
  skip_datums_[doc_id_col_idx_].set_uint(doc_id);
}

int ObSSTablePostingScanner::get_doc_id_from_endkey(const ObMicroIndexInfo &index_info, uint64_t &doc_id)
{
  int ret = OB_SUCCESS;
  const ObCommonDatumRowkey &endkey = index_info.endkey_;
  if (endkey.is_compact_rowkey()) {
    if (OB_UNLIKELY(doc_id_col_idx_ >= endkey.get_compact_rowkey()->get_datum_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id column index out of range", K(ret), K(doc_id_col_idx_),
          K(endkey.get_compact_rowkey()->get_datum_cnt()));
    } else {
      doc_id = endkey.get_compact_rowkey()->get_datum(doc_id_col_idx_).get_uint64();
    }
  } else if (endkey.is_discrete_rowkey()) {
    if (OB_UNLIKELY(doc_id_col_idx_ >= endkey.get_discrete_rowkey()->rowkey_vector_->col_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id column index out of range", K(ret), K(doc_id_col_idx_),
          K(endkey.get_discrete_rowkey()->rowkey_vector_->col_cnt_));
    } else {
      const ObDiscreteDatumRowkey *discrete_rowkey = endkey.get_discrete_rowkey();
      const ObColumnVector &column_vector = discrete_rowkey->rowkey_vector_->columns_[doc_id_col_idx_];
      const int64_t row_idx = discrete_rowkey->row_idx_;
      switch (column_vector.type_) {
        case ObColumnVectorType::DATUM_TYPE: {
          doc_id = column_vector.datums_[row_idx].get_uint64();
          break;
        }
        case ObColumnVectorType::UNSIGNED_INTEGER_TYPE: {
          doc_id = column_vector.unsigned_ints_[row_idx];
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column vector type for doc id", K(ret), K(column_vector.type_));
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected endkey type", K(ret), K(endkey));
  }
  return ret;
}

void ObSSTablePostingScanner::advance_data_read_cursor()
{
  if (has_read_item()) {
    DataPrefetchItem &item = cur_data_item();
    if (!item.requested_ && !item.value_requested_) {
      // Neither data nor value block was read: stop prefetching ahead.
      data_io_enabled_ = false;
    }
    release_leaf_pin_slot(item);
    item.reset();
    ++data_read_idx_;
  }
}

int ObSSTablePostingScanner::prefetch_one_data_block(
    const ObMicroIndexInfo &index_info, storage::ObMicroBlockDataHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  handle.allocator_ = &data_io_allocator_;
  handle.tenant_id_ = MTL_ID();
  ObMicroBlockCacheKey key(MTL_ID(), index_info);
  ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
  if (OB_UNLIKELY(!index_info.is_data_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-data block index info", K(ret), K(index_info));
  } else {
    handle.macro_block_id_ = index_info.get_macro_id();
    handle.micro_info_.set(
        index_info.get_block_offset(), index_info.get_block_size(),
        index_info.get_logic_micro_id(), index_info.get_data_checksum());
    if (OB_FAIL(cache.get_cache_block(key, handle.cache_handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("failed to get data cache block", K(ret), K(index_info));
      } else if (OB_FAIL(cache.prefetch(
                 MTL_ID(), index_info.get_macro_id(), index_info,
                 scan_param_.get_query_flag().is_use_block_cache(), scan_param_.get_tablet_id(),
                 handle.io_handle_, &data_io_allocator_, false /*is_preread*/))) {
        LOG_WARN("failed to prefetch data micro block", K(ret), K(index_info));
      } else {
        handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      }
    } else {
      handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
    }
  }
  return ret;
}

int ObSSTablePostingScanner::prefetch_item_blocks(DataPrefetchItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.io_issued_) {
    if (OB_FAIL(prefetch_one_data_block(item.index_info_, item.data_handle_))) {
      LOG_WARN("failed to prefetch data block", K(ret), K(item.index_info_));
    } else {
      item.io_issued_ = true;
    }
  }
  if (OB_SUCC(ret) && need_posting_value_
      && item.value_row_header_valid_ && !item.value_io_issued_) {
    if (OB_FAIL(prefetch_one_value_block(item, item.value_handle_))) {
      LOG_WARN("failed to prefetch value block", K(ret), K(item.index_info_));
    } else {
      item.value_io_issued_ = true;
    }
  }
  return ret;
}

int ObSSTablePostingScanner::fill_value_locator_from_agg(DataPrefetchItem &item)
{
  int ret = OB_SUCCESS;
  item.value_micro_offset_ = 0;
  item.value_micro_size_ = 0;
  item.value_row_header_valid_ = false;
  item.value_row_header_.reset();
  if (!need_posting_value_) {
  } else if (OB_ISNULL(item.index_info_.agg_row_buf_)
             || item.index_info_.agg_buf_size_ < static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid agg buffer for value locator", K(ret), KP(item.index_info_.agg_row_buf_),
             K(item.index_info_.agg_buf_size_));
  } else {
    const ObInvIdxAggDataHeader *header =
        reinterpret_cast<const ObInvIdxAggDataHeader *>(item.index_info_.agg_row_buf_);
    const int64_t agg_buf_size = item.index_info_.agg_buf_size_;
    const int64_t header_size = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));
    const int64_t entry_size = static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));
    const int64_t block_count = header->logical_block_count_;
    const int64_t total_size = header->total_size_;
    if (OB_UNLIKELY(ObInvIdxAggDataHeader::VERSION != header->version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg data header version", K(ret), K(header->version_));
    } else if (OB_UNLIKELY(block_count <= 0
        || block_count > (agg_buf_size - header_size) / entry_size
        || total_size > agg_buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("malformed agg data header for value locator", K(ret), K(agg_buf_size),
          K(block_count), K(total_size));
    } else if (OB_UNLIKELY(header_size + block_count * entry_size > total_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("agg data entries region exceeds total size for value locator", K(ret),
          K(agg_buf_size), K(block_count), K(total_size));
    } else if (OB_UNLIKELY((0 == header->posting_value_size_ && 0 != header->posting_value_offset_)
        || (header->posting_value_size_ > 0 && 0 == header->posting_value_offset_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("malformed posting value locator", K(ret),
          K(header->posting_value_offset_), K(header->posting_value_size_));
    } else {
      item.value_micro_offset_ = header->posting_value_offset_;
      item.value_micro_size_ = header->posting_value_size_;
      if (item.value_micro_size_ > 0) {
        if (OB_ISNULL(item.index_info_.row_header_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("missing row header for value locator", K(ret), K(item.value_micro_size_),
              K(item.value_micro_offset_));
        } else {
          item.value_row_header_ = *item.index_info_.row_header_;
          const int64_t data_block_size =
              static_cast<int64_t>(item.index_info_.row_header_->get_block_size());
          const int64_t value_end = item.value_micro_offset_ + item.value_micro_size_;
          if (OB_UNLIKELY(item.value_micro_offset_ > INT32_MAX
              || item.value_micro_size_ > INT32_MAX)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("posting value locator exceeds int32 range", K(ret),
                K(item.value_micro_offset_), K(item.value_micro_size_));
            item.value_micro_offset_ = 0;
            item.value_micro_size_ = 0;
          } else if (OB_UNLIKELY(data_block_size <= 0
              || value_end < item.value_micro_offset_
              || value_end > common::OB_DEFAULT_MACRO_BLOCK_SIZE)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("posting value locator out of macro range", K(ret),
                K(item.value_micro_offset_), K(item.value_micro_size_),
                K(value_end), K(data_block_size));
            item.value_micro_offset_ = 0;
            item.value_micro_size_ = 0;
          } else {
            item.value_row_header_.block_offset_ =
                static_cast<int32_t>(item.value_micro_offset_);
            item.value_row_header_.block_size_ =
                static_cast<int32_t>(item.value_micro_size_);
            // Value micro block must use physical cache/IO key (macro_id, offset, size).
            // Reusing data logic_micro_id/checksum would collide with the paired data block.
            item.value_row_header_.unset_has_logic_micro_id();
            item.value_row_header_.unset_has_shared_data_macro_id();
            item.value_row_header_valid_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTablePostingScanner::prefetch_one_value_block(
    DataPrefetchItem &item,
    storage::ObMicroBlockDataHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (!item.value_row_header_valid_) {
  } else {
    ObMicroIndexInfo value_index_info = item.index_info_;
    value_index_info.row_header_ = &item.value_row_header_;
    handle.allocator_ = &data_io_allocator_;
    handle.tenant_id_ = MTL_ID();
    ObMicroBlockCacheKey key(MTL_ID(), value_index_info);
    ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
    if (OB_UNLIKELY(!value_index_info.is_data_block())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-data value index info", K(ret), K(value_index_info));
    } else {
      handle.macro_block_id_ = value_index_info.get_macro_id();
      handle.micro_info_.set(
          value_index_info.get_block_offset(), value_index_info.get_block_size(),
          value_index_info.get_logic_micro_id(), value_index_info.get_data_checksum());
      if (OB_FAIL(cache.get_cache_block(key, handle.cache_handle_))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("failed to get value cache block", K(ret), K(value_index_info));
        } else if (OB_FAIL(cache.prefetch(
            MTL_ID(), value_index_info.get_macro_id(), value_index_info,
            scan_param_.get_query_flag().is_use_block_cache(), scan_param_.get_tablet_id(),
            handle.io_handle_, &data_io_allocator_))) {
          LOG_WARN("failed to prefetch value micro block", K(ret), K(value_index_info));
        } else {
          handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        }
      } else {
        handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

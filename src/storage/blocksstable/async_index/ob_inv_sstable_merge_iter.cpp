/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"

#include <type_traits>
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvSSTableMergeIter::ObInvSSTableMergeIter()
    : macro_iter_(),
      micro_iter_(),
      logical_iter_(),
      row_iter_(),
      curr_row_(),
      curr_term_(),
      curr_key_(),
      term_range_(),
      curr_macro_desc_(),
      curr_value_block_(),
      curr_posting_block_(),
      sstable_(nullptr),
      read_info_(nullptr),
      allocator_(nullptr),
      datum_utils_(nullptr),
      curr_key_buf_(nullptr),
      range_scratch_buf_(nullptr),
      range_low_buf_(nullptr),
      range_high_buf_(nullptr),
      curr_micro_block_(nullptr),
      curr_term_metas_(nullptr),
      range_low_buf_cap_(0),
      range_high_buf_cap_(0),
      iter_row_count_(0),
      iter_row_id_(-1),
      term_col_cnt_(0),
      out_col_cnt_(0),
      curr_doc_id_(common::OB_INVALID_ID),
      curr_level_(Level::UNOPENED),
      macro_iter_opened_(false),
      curr_entry_within_end_(false),
      range_start_passed_(false),
      is_inited_(false)
{
}

ObInvSSTableMergeIter::~ObInvSSTableMergeIter()
{
  reset();
}

int ObInvSSTableMergeIter::init(
    ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    const ObDatumRange &term_range,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !term_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstable), K(term_range));
  } else if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv idx row desc", K(ret), K(row_desc));
  } else {
    sstable_ = &sstable;
    read_info_ = &read_info;
    inv_idx_row_desc_ = row_desc;
    term_range_ = term_range;
    allocator_ = &allocator;
    datum_utils_ = &read_info.get_datum_utils();
    // inv sstable 的 rowkey 为 (term..., doc_id, [trans_version, sql_seq])；doc_id 是
    // 最后一个 schema rowkey 列。row iterator 对 term 列产出 nop（term 不存储在
    // posting block 中）；materialize_curr_row_ 将其覆盖为正确值。
    term_col_cnt_ = read_info.get_schema_rowkey_count() - 1;
    out_col_cnt_ = read_info.get_request_count();
    const ObIArray<ObColDesc> &cols_desc = read_info.get_columns_desc();
    if (OB_UNLIKELY(term_col_cnt_ <= 0 || out_col_cnt_ <= term_col_cnt_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid term/request column count", K(ret),
          K(term_col_cnt_), K(out_col_cnt_), K(read_info));
    } else if (OB_UNLIKELY(cols_desc.count() <= term_col_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read info columns fewer than rowkey", K(ret),
          K(term_col_cnt_), "col_count", cols_desc.count(), K(read_info));
    } else if (OB_UNLIKELY(!cols_desc.at(term_col_cnt_).col_type_.is_uint64())) {
      // 读栈以 uint64 存储/读取 doc_id，因此非 uint64 的 doc_id 列
      // 会导致静默读取错误。
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("inv sstable merge iter requires uint64 doc_id column", K(ret),
          K(term_col_cnt_), "doc_id_type", cols_desc.at(term_col_cnt_).col_type_);
    } else if (OB_FAIL(curr_row_.init(allocator, out_col_cnt_))) {
      LOG_WARN("fail to init curr row", K(ret), K(out_col_cnt_));
    } else {
      // 两个独立的 (term_col_cnt_+1)-datum 暂存缓冲区：curr_key_buf_ 支撑
      // get_curr_key，range_scratch_buf_ 支撑 get_curr_range。分开设计，确保
      // get_curr_range 不会覆盖调用方仍在持有的 get_curr_key 结果。
      const int64_t key_datum_cnt = term_col_cnt_ + 1;
      const int64_t buf_size = key_datum_cnt * sizeof(ObStorageDatum);
      curr_key_buf_ = static_cast<char *>(allocator.alloc(buf_size));
      range_scratch_buf_ = static_cast<char *>(allocator.alloc(buf_size));
      if (OB_ISNULL(curr_key_buf_) || OB_ISNULL(range_scratch_buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc key scratch buf", K(ret), K(key_datum_cnt));
      } else {
        // 对每个 ObStorageDatum 做 placement-construct：其 ctor 中的 set_nop() 将
        // ptr_/uint_ 联合体指向内联 buf_[]。缺少此步骤，get_curr_key() 对 doc_id
        // 槽位的 set_uint() 将解引用未初始化的指针（原始内存分配的内容是垃圾值）。
        // 这些缓冲区直接释放而不调用析构函数，因此需要在编译期保证可平凡析构 ——
        // 如存在非平凡成员则需要添加析构循环。
        static_assert(std::is_trivially_destructible<ObStorageDatum>::value,
            "ObStorageDatum must be trivially destructible: placement-new'd into raw "
            "buffers that are freed without invoking the destructor");
        ObStorageDatum *key_datums = reinterpret_cast<ObStorageDatum *>(curr_key_buf_);
        ObStorageDatum *range_datums = reinterpret_cast<ObStorageDatum *>(range_scratch_buf_);
        for (int64_t i = 0; i < key_datum_cnt; ++i) {
          new (&key_datums[i]) ObStorageDatum();
          new (&range_datums[i]) ObStorageDatum();
        }
        curr_doc_id_ = common::OB_INVALID_ID;
        iter_row_count_ = 0;
        iter_row_id_ = -1;
        curr_level_ = Level::UNOPENED;
        macro_iter_opened_ = false;
        curr_entry_within_end_ = false;
        range_start_passed_ = false;
        is_inited_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObInvSSTableMergeIter::reset()
{
  is_inited_ = false;
  macro_iter_.reset();
  micro_iter_.reset();
  logical_iter_.reset();
  row_iter_.reset();
  inv_idx_row_desc_.reset();
  curr_row_.reset();
  curr_term_.reset();
  curr_key_.reset();
  if (nullptr != allocator_) {
    if (nullptr != curr_key_buf_) {
      allocator_->free(curr_key_buf_);
    }
    if (nullptr != range_scratch_buf_) {
      allocator_->free(range_scratch_buf_);
    }
    if (nullptr != range_low_buf_) {
      allocator_->free(range_low_buf_);
    }
    if (nullptr != range_high_buf_) {
      allocator_->free(range_high_buf_);
    }
  }
  curr_key_buf_ = nullptr;
  range_scratch_buf_ = nullptr;
  range_low_buf_ = nullptr;
  range_high_buf_ = nullptr;
  range_low_buf_cap_ = 0;
  range_high_buf_cap_ = 0;
  iter_row_count_ = 0;
  iter_row_id_ = -1;
  curr_macro_desc_.reuse();
  curr_micro_block_ = nullptr;
  curr_term_metas_ = nullptr;
  curr_value_block_.reset();
  curr_posting_block_.reset();
  sstable_ = nullptr;
  read_info_ = nullptr;
  datum_utils_ = nullptr;
  allocator_ = nullptr;
  term_range_.reset();
  term_col_cnt_ = 0;
  out_col_cnt_ = 0;
  curr_doc_id_ = common::OB_INVALID_ID;
  curr_level_ = Level::UNOPENED;
  macro_iter_opened_ = false;
  curr_entry_within_end_ = false;
  range_start_passed_ = false;
}

void ObInvSSTableMergeIter::reuse()
{
  // 保留 sstable / read_info / term_range / allocator 绑定以及
  // curr_row_/curr_key_buf_ 缓冲区；仅回退读栈。
  macro_iter_.reset();
  micro_iter_.reset();
  logical_iter_.reset();
  row_iter_.reset();
  curr_term_.reset();
  curr_key_.reset();
  curr_macro_desc_.reuse();
  curr_micro_block_ = nullptr;
  curr_term_metas_ = nullptr;
  curr_value_block_.reset();
  curr_posting_block_.reset();
  curr_doc_id_ = common::OB_INVALID_ID;
  iter_row_count_ = 0;
  iter_row_id_ = -1;
  curr_level_ = Level::UNOPENED;
  macro_iter_opened_ = false;
  curr_entry_within_end_ = false;
  range_start_passed_ = false;
}

int ObInvSSTableMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (Level::END == curr_level_) {
    ret = OB_ITER_END;
  } else {
    // Row-only compatibility wrapper: next_row() deliberately surfaces a newly
    // reached MACRO boundary to the block-reuse driver, while next() keeps its
    // historical contract of returning only materialized rows.
    do {
      if (Level::UNOPENED == curr_level_) {
        if (OB_FAIL(next_macro_block()) && OB_ITER_END != ret) {
          LOG_WARN("fail to locate first macro", K(ret));
        } else if (Level::END == curr_level_) {
          ret = OB_ITER_END;
        }
      }
      if (OB_SUCC(ret) && Level::MACRO == curr_level_) {
        if (OB_FAIL(open_term_entry()) && OB_ITER_END != ret) {
          LOG_WARN("fail to open term entry level", K(ret));
        } else if (Level::END == curr_level_) {
          ret = OB_ITER_END;
        }
      }
      if (OB_SUCC(ret) && Level::TERM_ENTRY == curr_level_) {
        if (OB_FAIL(open_logical_block()) && OB_ITER_END != ret) {
          LOG_WARN("fail to open logical block level", K(ret));
        } else if (Level::END == curr_level_) {
          ret = OB_ITER_END;
        }
      }
      if (OB_SUCC(ret) && Level::LOGICAL_BLOCK == curr_level_) {
        if (OB_FAIL(open_row()) && OB_ITER_END != ret) {
          LOG_WARN("fail to open row level", K(ret));
        } else if (Level::END == curr_level_) {
          ret = OB_ITER_END;
        }
      }
      if (OB_SUCC(ret) && Level::ROW == curr_level_) {
        // open_row 将 row iterator 停在首行之前；next_row() 使其落到首行。
        if (OB_FAIL(next_row()) && OB_ITER_END != ret) {
          LOG_WARN("fail to advance to next row", K(ret));
        } else if (Level::END == curr_level_) {
          ret = OB_ITER_END;
        }
      }
    } while (OB_SUCC(ret) && Level::MACRO == curr_level_);
  }
  return ret;
}

const ObDatumRow *ObInvSSTableMergeIter::get_curr_row() const
{
  return (Level::ROW == curr_level_) ? &curr_row_ : nullptr;
}

int ObInvSSTableMergeIter::get_curr_row_id(int64_t &row_id) const
{
  int ret = OB_SUCCESS;
  if (Level::ROW != curr_level_) {
    ret = OB_ITER_END;
  } else {
    row_id = iter_row_id_;
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_key(ObDatumRowkey &key)
{
  int ret = OB_SUCCESS;
  key.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (Level::ROW != curr_level_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(curr_key_buf_) || OB_ISNULL(datum_utils_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("curr key buf or datum utils is null", K(ret), KP(curr_key_buf_), KP(datum_utils_));
  } else {
    // term datum 为浅拷贝 —— 其字符串 payload 指向 micro iterator 稳定的
    // index-entry endkey 缓冲区；doc_id 为标量。
    ObStorageDatum *datums = reinterpret_cast<ObStorageDatum *>(curr_key_buf_);
    for (int64_t i = 0; i < term_col_cnt_; ++i) {
      datums[i] = curr_term_.datums_[i];
    }
    datums[term_col_cnt_].set_uint(curr_doc_id_);
    if (OB_FAIL(curr_key_.assign(datums, term_col_cnt_ + 1))) {
      LOG_WARN("fail to assign curr key", K(ret));
    } else {
      key = curr_key_;
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_range(ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  range.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (curr_level_) {
      case Level::MACRO:
        range = curr_macro_desc_.range_;
        break;
      case Level::TERM_ENTRY:
        if (OB_ISNULL(curr_micro_block_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current micro block is null", K(ret), K_(curr_level));
        } else {
          range = curr_micro_block_->range_;
        }
        break;
      case Level::LOGICAL_BLOCK:
      case Level::ROW: {
        // (term, min_doc_id)..(term, max_doc_id)；ROW 时为闭点区间。
        // deep_copy 会改写 range_low_buf_/range_high_buf_ 等可复用缓冲区，故本接口非 const。
        ObDatumRowkey low;
        ObDatumRowkey high;
        if (OB_ISNULL(range_scratch_buf_) || OB_ISNULL(allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("range scratch buffer or allocator is null",
              K(ret), K_(curr_level), KP_(range_scratch_buf), KP_(allocator));
        } else {
          ObStorageDatum *datums = reinterpret_cast<ObStorageDatum *>(range_scratch_buf_);
          for (int64_t i = 0; i < term_col_cnt_; ++i) {
            datums[i] = curr_term_.datums_[i];
          }
          const uint64_t lo_id = (Level::ROW == curr_level_)
              ? curr_doc_id_ : curr_posting_block_.min_id_;
          const uint64_t hi_id = (Level::ROW == curr_level_)
              ? curr_doc_id_ : curr_posting_block_.max_id_;
          ObDatumRowkey tmp;
          datums[term_col_cnt_].set_uint(lo_id);
          if (OB_FAIL(tmp.assign(datums, term_col_cnt_ + 1))) {
            LOG_WARN("fail to assign low tmp", K(ret));
          } else if (OB_FAIL(deep_copy_rowkey_(
              tmp, low, range_low_buf_, range_low_buf_cap_))) {
            LOG_WARN("fail to deep copy low", K(ret));
          } else {
            datums[term_col_cnt_].set_uint(hi_id);
            if (OB_FAIL(tmp.assign(datums, term_col_cnt_ + 1))) {
              LOG_WARN("fail to assign high tmp", K(ret));
            } else if (OB_FAIL(deep_copy_rowkey_(
                tmp, high, range_high_buf_, range_high_buf_cap_))) {
              LOG_WARN("fail to deep copy high", K(ret));
            } else {
              range.set_start_key(low);
              range.set_end_key(high);
              // ROW 为闭点区间；LOGICAL_BLOCK 的 posting block min/max doc_id 亦含端点。
              range.set_left_closed();
              range.set_right_closed();
            }
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_curr_range on unexpected level", K(ret), K_(curr_level));
        break;
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_range(ObDatumRowkey &low, ObDatumRowkey &high)
{
  int ret = OB_SUCCESS;
  ObDatumRange range;
  low.reset();
  high.reset();
  if (OB_FAIL(get_curr_range(range))) {
    LOG_WARN("fail to get curr range", K(ret));
  } else {
    low = range.get_start_key();
    high = range.get_end_key();
  }
  return ret;
}

int ObInvSSTableMergeIter::open_term_entry()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (Level::MACRO != curr_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("open_term_entry requires MACRO level", K(ret), K(curr_level_));
  } else if (OB_FAIL(enter_term_entry_()) && OB_ITER_END != ret) {
    LOG_WARN("fail to enter term entry on current macro block", K(ret), K(curr_macro_desc_));
  }
  return ret;
}

int ObInvSSTableMergeIter::open_logical_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (Level::TERM_ENTRY != curr_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("open_logical_block requires TERM_ENTRY level", K(ret), K(curr_level_));
  } else if (OB_FAIL(enter_logical_block_())) {
    LOG_WARN("fail to enter logical block on current term entry", K(ret));
  }
  return ret;
}

int ObInvSSTableMergeIter::open_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (Level::LOGICAL_BLOCK != curr_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("open_row requires LOGICAL_BLOCK level", K(ret), K(curr_level_));
  } else {
    // 在当前 logical block 及其 value sub-block（phrase）上打开 row iterator。
    ObPostingValueBlockDesc value_block;
    if (OB_FAIL(logical_iter_.get_curr_value_block(value_block))) {
      LOG_WARN("fail to get current value sub-block", K(ret));
    } else if (OB_FAIL(row_iter_.open(curr_posting_block_, *read_info_, inv_idx_row_desc_,
            *allocator_, value_block.is_valid() ? &value_block : nullptr))) {
      LOG_WARN("fail to open row iterator", K(ret), K(curr_posting_block_));
    } else {
      curr_level_ = Level::ROW;
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::next_macro_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(advance_macro_()) && OB_ITER_END != ret) {
    LOG_WARN("fail to advance to next macro block", K(ret));
  }
  return ret;
}

int ObInvSSTableMergeIter::advance_reused_rows(const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::MACRO != curr_level_ || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid reused macro row count", K(ret), K(row_count), K_(curr_level));
  } else {
    iter_row_count_ += row_count;
    iter_row_id_ += row_count;
  }
  return ret;
}

int ObInvSSTableMergeIter::advance_reused_logical_rows(const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::LOGICAL_BLOCK != curr_level_ || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid reused logical block row count", K(ret), K(row_count), K_(curr_level));
  } else {
    iter_row_count_ += row_count;
    iter_row_id_ += row_count;
  }
  return ret;
}

int ObInvSSTableMergeIter::next_term_entry()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(advance_term_entry_()) && OB_ITER_END != ret) {
    LOG_WARN("fail to advance to next term entry", K(ret));
  }
  return ret;
}

int ObInvSSTableMergeIter::next_logical_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = logical_iter_.next();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(logical_iter_.get_curr_block(curr_posting_block_))) {
        LOG_WARN("fail to get current logical block after next", K(ret));
      } else {
        curr_level_ = Level::LOGICAL_BLOCK;
      }
    } else if (OB_ITER_END == ret) {
      // 当前 term 的 logical block 已耗尽 —— 上溯到下一个 term entry。
      ret = next_term_entry();
      if (OB_ITER_END == ret) {
        curr_level_ = Level::END;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("fail to cascade to next term entry", K(ret));
      } else if (Level::MACRO == curr_level_) {
        // Stop at the next macro boundary so the merge driver can descend and
        // reconsider logical-block reuse.
      } else if (OB_FAIL(enter_logical_block_())) {
        LOG_WARN("fail to open first logical block of new term", K(ret));
      }
    } else {
      LOG_WARN("fail to advance posting block iterator", K(ret));
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::next_row(const bool stop_at_logical_block_boundary)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool found_in_range = false;
    while (OB_SUCC(ret) && !found_in_range) {
      ret = row_iter_.next();
      if (OB_SUCC(ret)) {
        curr_level_ = Level::ROW;
        if (OB_FAIL(materialize_curr_row_())) {
          LOG_WARN("fail to materialize current row", K(ret));
        } else {
          // 逐行范围过滤，配合两个单调性捷径（键按 (term, doc_id) 升序排列）：
          // 当整个 entry 在 range end 内时跳过 end 比较 (curr_entry_within_end_)，
          // 一旦某行到达 range start 后跳过 start 比较
          // (range_start_passed_ 锁存)。两者同时满足时该行直接接受，
          // 无需键物化或比较。
          const bool need_end_check = !curr_entry_within_end_;
          const bool need_start_check = !range_start_passed_;
          if (!need_end_check && !need_start_check) {
            found_in_range = true;
          } else {
            ObDatumRowkey curr_key;
            bool beyond_end = false;
            if (OB_FAIL(get_curr_key(curr_key))) {
              LOG_WARN("fail to get current key for range check", K(ret), K(curr_level_));
            } else if (need_end_check && OB_FAIL(beyond_range_end_(curr_key, beyond_end))) {
              LOG_WARN("fail to check whether current row beyond range end", K(ret), K(curr_key));
            } else if (beyond_end) {
              curr_level_ = Level::END;
              ret = OB_ITER_END;
            } else if (!need_start_check) {
              found_in_range = true;
            } else {
              bool before = false;
              if (OB_FAIL(before_range_start_(curr_key, before))) {
                LOG_WARN("fail to check whether current row before range start", K(ret));
              } else {
                found_in_range = !before;
                if (!before) {
                  range_start_passed_ = true;
                }
              }
            }
          }
        }
      } else if (OB_ITER_END == ret) {
        // 当前 logical block 已耗尽 —— 上溯到下一个并重新打开 row iterator。
        ret = next_logical_block();
        if (OB_ITER_END == ret) {
          curr_level_ = Level::END;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to cascade to next logical block", K(ret));
        } else if (Level::MACRO == curr_level_) {
          // next_logical_block() deliberately surfaced the parent macro boundary.
          break;
        } else if (stop_at_logical_block_boundary) {
          // merger 逐行路径：停在下一 LOGICAL_BLOCK 边界（curr_row()==nullptr），
          // 由主循环重建 loser tree 后重新判逻辑块复用，不贪心 open_row()。
          break;
        } else if (OB_FAIL(open_row())) {
          LOG_WARN("fail to reopen row iterator after cascade", K(ret));
        }
      } else {
        LOG_WARN("fail to advance row iterator", K(ret));
      }
    }
    if (OB_SUCC(ret) && found_in_range) {
      ++iter_row_count_;
      ++iter_row_id_;
    }
  }
  return ret;
}

// Legacy boolean macro/micro reuse hooks remain disabled.
bool ObInvSSTableMergeIter::curr_macro_is_self_contained() const { return false; }
bool ObInvSSTableMergeIter::curr_micro_is_single_term() const { return false; }

int ObInvSSTableMergeIter::get_macro_block_desc(ObMacroBlockDesc &desc) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::MACRO != curr_level_)) {
    // 仅在停在 MACRO 边界（未下钻）时可取复用描述。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_macro_block_desc requires MACRO level", K(ret), K_(curr_level));
  } else if (OB_UNLIKELY(!curr_macro_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current macro block desc is invalid", K(ret), K_(curr_macro_desc));
  } else {
    // curr_macro_desc_ 由 advance_macro_() 从 macro_iter_ 取得，含 macro_block_id /
    // macro_meta / range / row_count 等，直接透传给 writer 的复用登记。
    desc = curr_macro_desc_;
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_macro_inv_meta(ObInvMacroMeta &meta) const
{
  int ret = OB_SUCCESS;
  meta.reset();
  const ObDataMacroBlockMeta *macro_meta = curr_macro_desc_.macro_meta_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::MACRO != curr_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_curr_macro_inv_meta requires MACRO level", K(ret), K_(curr_level));
  } else if (OB_UNLIKELY(!curr_macro_desc_.is_valid() || OB_ISNULL(macro_meta))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro desc / meta", K(ret), K_(curr_macro_desc), KP(macro_meta));
  } else {
    const ObDataBlockMetaVal &val = macro_meta->val_;
    ObInvMacroMeta macro_inv_meta;
    int64_t pos = 0;
    if (OB_UNLIKELY(common::INV_IDX_ROW_STORE != val.row_store_type_
        || val.agg_row_len_ <= 0 || OB_ISNULL(val.agg_row_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid inverted index macro agg meta", K(ret), K(val), KPC(macro_meta));
    } else if (OB_FAIL(macro_inv_meta.deserialize(val.agg_row_buf_, val.agg_row_len_, pos))) {
      LOG_WARN("fail to deserialize inverted index macro agg meta", K(ret), K(val));
    } else if (OB_UNLIKELY(!macro_inv_meta.is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid decoded inverted macro metadata", K(ret), K(macro_inv_meta));
    } else if (OB_FAIL(meta.assign(macro_inv_meta))) {
      LOG_WARN("failed to copy macro inverted stats", K(ret));
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_clustered_index_info(
    const ObMicroBlockData *&clustered_micro_block_data)
{
  int ret = OB_SUCCESS;
  clustered_micro_block_data = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::MACRO != curr_level_)) {
    // 仅在停在 MACRO 边界（未下钻）时可取聚集索引微块，与 get_macro_block_desc 同约束。
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_curr_clustered_index_info requires MACRO level", K(ret), K_(curr_level));
  } else if (OB_FAIL(macro_iter_.get_current_clustered_index_info(clustered_micro_block_data))) {
    LOG_WARN("fail to get current clustered index info", K(ret), K_(curr_macro_desc));
  }
  return ret;
}

int ObInvSSTableMergeIter::open_curr_range(const bool for_rewrite, const bool for_compare)
{
  int ret = OB_SUCCESS;
  Level expected_level = Level::END;
  const Level previous_level = curr_level_;
  UNUSED(for_rewrite);  // TODO: 预留对齐主线签名（物理重写策略），后续强制重写策略会用到。
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (curr_level_) {
      case Level::MACRO:
        expected_level = Level::TERM_ENTRY;
        ret = open_term_entry();
        break;
      case Level::TERM_ENTRY:
        expected_level = Level::LOGICAL_BLOCK;
        ret = open_logical_block();
        break;
      case Level::LOGICAL_BLOCK:
        expected_level = Level::ROW;
        ret = open_row();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("open_curr_range on non-openable level", K(ret), K_(curr_level));
        break;
    }
    if (OB_SUCC(ret) && for_compare && Level::LOGICAL_BLOCK == previous_level) {
      // 比较路径只在当前 logical block 内寻找首行；若范围过滤越过块边界，
      // next_row(true) 停在新 range，由下方转成 OB_BLOCK_SWITCHED 通知整树重建。
      if (OB_FAIL(next_row(true /*stop_at_logical_block_boundary*/))
          && OB_ITER_END != ret) {
        LOG_WARN("fail to land first row after open for compare", K(ret));
      }
    }
    if (for_compare && (OB_ITER_END == ret
        || (OB_SUCC(ret) && expected_level != curr_level_))) {
      // 对齐 ObPartitionMinorMacroMergeIter：比较下钻跨到 sibling range 或 END
      // 时不能继续使用 loser-tree 缓存的子树结果。
      ret = OB_BLOCK_SWITCHED;
      LOG_DEBUG("current inv merge range switched while comparing",
          K(previous_level), K(expected_level), K_(curr_level));
    } else if (OB_FAIL(ret) && OB_ITER_END != ret && OB_BLOCK_SWITCHED != ret) {
      LOG_WARN("fail to open current range one level down", K(ret), K_(curr_level));
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::curr_macro_within_merge_range(
    const ObMacroBlockDesc &desc, bool &within) const
{
  int ret = OB_SUCCESS;
  within = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!desc.range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block desc range invalid", K(ret), K(desc));
  } else if (OB_FAIL(range_within_merge_range(desc.range_, within))) {
    LOG_WARN("fail to check macro range within merge range", K(ret), K(desc));
  }
  return ret;
}

int ObInvSSTableMergeIter::range_within_merge_range(
    const ObDatumRange &block_range, bool &within) const
{
  int ret = OB_SUCCESS;
  within = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(datum_utils_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum utils is null", K(ret));
  } else if (OB_UNLIKELY(!block_range.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block range invalid", K(ret), K(block_range));
  } else if (term_range_.is_whole_range()) {
    // 任务范围覆盖全体，任何块都完整落在内。
    within = true;
  } else {
    // 块 range 需整体被 term_range_ 包含。宏块下界来自 index block
    // previous endkey，通常是开区间；逻辑块下界是真实行闭区间。等值边界
    // 是否安全必须同时看两侧开闭性，不写死开闭假设。
    const ObDatumRowkey &block_low = block_range.get_start_key();
    const ObDatumRowkey &block_high = block_range.get_end_key();
    const ObDatumRowkey &range_start = term_range_.get_start_key();
    const ObDatumRowkey &range_end = term_range_.get_end_key();
    int cmp_low = 0;
    int cmp_high = 0;
    bool low_ok = false;
    bool high_ok = false;
    if (range_start.is_min_rowkey()) {
      low_ok = true;
    } else if (OB_FAIL(block_low.compare(range_start, *datum_utils_, cmp_low))) {
      LOG_WARN("fail to compare block low with range start", K(ret), K(block_low), K(range_start));
    } else {
      low_ok = cmp_low > 0
          || (0 == cmp_low && (term_range_.get_border_flag().inclusive_start()
                               || !block_range.get_border_flag().inclusive_start()));
    }
    if (OB_SUCC(ret)) {
      if (!low_ok) {
        within = false;
      } else if (range_end.is_max_rowkey()) {
        high_ok = true;
        within = true;
      } else if (OB_FAIL(block_high.compare(range_end, *datum_utils_, cmp_high))) {
        LOG_WARN("fail to compare block high with range end", K(ret), K(block_high), K(range_end));
      } else {
        high_ok = cmp_high < 0
            || (0 == cmp_high && (term_range_.get_border_flag().inclusive_end()
                                  || !block_range.get_border_flag().inclusive_end()));
        within = high_ok;
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::get_micro_block(ObMicroBlock &micro_block) const
{
  UNUSED(micro_block);
  LOG_TRACE("inv sstable merge iter micro-block reuse hook not supported yet", K(lbt()));
  return OB_NOT_SUPPORTED;
}

int ObInvSSTableMergeIter::get_logical_block(
    ObPostingBlockDesc &posting_block,
    ObPostingValueBlockDesc &value_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::LOGICAL_BLOCK != curr_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_logical_block requires LOGICAL_BLOCK level", K(ret), K_(curr_level));
  } else if (OB_UNLIKELY(!curr_posting_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current posting block is invalid", K(ret), K_(curr_posting_block));
  } else {
    // 浅拷贝 desc：buf 借用当前宏块读缓冲区，推进迭代器后失效。
    posting_block = curr_posting_block_;
    if (OB_FAIL(logical_iter_.get_curr_value_block(value_block))) {
      LOG_WARN("fail to get current value block", K(ret), K(posting_block));
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::get_curr_term(ObDatumRowkey &term) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(Level::LOGICAL_BLOCK != curr_level_ && Level::ROW != curr_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_curr_term requires LOGICAL_BLOCK or ROW level", K(ret), K_(curr_level));
  } else if (OB_UNLIKELY(!curr_term_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current term is invalid", K(ret), K_(curr_level));
  } else {
    // 借用 micro_iter_ 的 index-entry endkey，micro_iter_ reset 前有效。
    term = curr_term_;
  }
  return ret;
}

int ObInvSSTableMergeIter::materialize_curr_row_()
{
  int ret = OB_SUCCESS;
  const ObDatumRow *r = row_iter_.get_curr_row();
  if (OB_ISNULL(r)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is null", K(ret));
  } else {
    // row iterator 按请求顺序产出完整 schema，前导 term 列为 nop。
    // 仅拷贝 posting 列 [term_col_cnt_, out_col_cnt_)，然后用 curr_term_
    // 填充 term 列 [0, term_col_cnt_)。
    for (int64_t i = term_col_cnt_; i < out_col_cnt_; ++i) {
      curr_row_.storage_datums_[i] = r->storage_datums_[i];
    }
    for (int64_t i = 0; i < term_col_cnt_; ++i) {
      curr_row_.storage_datums_[i] = curr_term_.datums_[i];
    }
    curr_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    uint64_t doc_id = 0;
    if (OB_FAIL(row_iter_.get_curr_doc_id(doc_id))) {
      LOG_WARN("fail to get current doc id", K(ret));
    } else {
      curr_doc_id_ = doc_id;
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::beyond_range_end_(const ObDatumRowkey &endkey, bool &beyond) const
{
  int ret = OB_SUCCESS;
  beyond = false;
  if (term_range_.is_whole_range()) {
    beyond = false;
  } else {
    int cmp_ret = 0;
    const ObDatumRowkey &end_key = term_range_.get_end_key();
    if (OB_FAIL(endkey.compare(end_key, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare endkey with range end", K(ret), K(endkey), K(end_key));
    } else {
      beyond = term_range_.get_border_flag().inclusive_end()
          ? (cmp_ret > 0)
          : (cmp_ret >= 0);
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::before_range_start_(const ObDatumRowkey &curr_key, bool &before) const
{
  int ret = OB_SUCCESS;
  before = false;
  if (term_range_.is_whole_range()) {
    before = false;
  } else {
    int cmp_ret = 0;
    const ObDatumRowkey &start_key = term_range_.get_start_key();
    if (OB_FAIL(curr_key.compare(start_key, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare current key with range start", K(ret), K(curr_key), K(start_key));
    } else {
      before = term_range_.get_border_flag().inclusive_start()
          ? (cmp_ret < 0)
          : (cmp_ret <= 0);
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::deep_copy_rowkey_(
    const ObDatumRowkey &src, ObDatumRowkey &dest, char *&buf, int64_t &buf_cap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    dest.reset();
    const int64_t copy_size = src.get_deep_copy_size();
    // 只增策略：缓冲区足够大时原地复用，将内存使用限制在所见最大 key 的规模，
    // 避免在 arena（free() 为空操作）下每次调用都泄漏内存。
    if (copy_size > buf_cap) {
      if (nullptr != buf) {
        allocator_->free(buf);
        buf = nullptr;
        buf_cap = 0;
      }
      if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(copy_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc rowkey buf", K(ret), K(copy_size));
      } else {
        buf_cap = copy_size;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(src.deep_copy(dest, buf, buf_cap))) {
      LOG_WARN("fail to deep copy rowkey", K(ret), K(src), K(copy_size), K(buf_cap));
      allocator_->free(buf);
      buf = nullptr;
      buf_cap = 0;
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::advance_macro_()
{
  int ret = OB_SUCCESS;
  if (macro_iter_.is_iter_end()) {
    curr_level_ = Level::END;
    ret = OB_ITER_END;
  } else {
    // 首次调用时延迟打开；后续调用仅获取下一个宏块。
    if (!macro_iter_opened_) {
      if (OB_FAIL(macro_iter_.open(*sstable_, term_range_, *read_info_, *allocator_))) {
        LOG_WARN("fail to open macro iterator", K(ret));
      } else {
        macro_iter_opened_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(macro_iter_.get_next_macro_block(curr_macro_desc_))) {
        if (OB_ITER_END == ret) {
          curr_level_ = Level::END;
        } else {
          LOG_WARN("fail to get next macro block", K(ret));
        }
      } else {
        // 落到新宏块时 reset micro_iter_，使旧宏块的借用视图在调用方
        // 观察到 MACRO 层级之前即失效。
        micro_iter_.reset();
        logical_iter_.reset();
        row_iter_.reset();
        curr_term_.reset();
        curr_micro_block_ = nullptr;
        curr_term_metas_ = nullptr;
        curr_value_block_.reset();
        curr_posting_block_.reset();
        curr_level_ = Level::MACRO;
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::enter_term_entry_()
{
  int ret = OB_SUCCESS;
  // micro_iter_ 已被 reset（由 advance_macro_ 或 reuse 完成）；在此宏块上重新 init。
  if (OB_FAIL(micro_iter_.init(curr_macro_desc_, *read_info_, sstable_))) {
    LOG_WARN("fail to init micro iterator on macro", K(ret), K(curr_macro_desc_));
  } else if (OB_FAIL(advance_term_entry_()) && OB_ITER_END != ret) {
    LOG_WARN("fail to advance to first term entry on macro", K(ret), K(curr_macro_desc_));
  }
  return ret;
}

int ObInvSSTableMergeIter::advance_term_entry_()
{
  int ret = OB_SUCCESS;
  const ObMicroBlock *mb = nullptr;
  const ObIArray<ObInvIdxLogicalBlockMeta> *metas = nullptr;
  ret = micro_iter_.get_next_term_entry(mb, metas);
  if (OB_ITER_END == ret) {
    // 当前宏块已耗尽 —— 上溯到下一个宏块。
    ret = advance_macro_();
    if (OB_ITER_END == ret) {
      curr_level_ = Level::END;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to cascade to next macro", K(ret));
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail to get next term entry", K(ret));
  } else if (OB_FAIL(fill_curr_term_entry_(mb, metas))) {
    LOG_WARN("fail to fill current term entry", K(ret));
  } else {
    curr_level_ = Level::TERM_ENTRY;
  }
  return ret;
}

int ObInvSSTableMergeIter::fill_curr_term_entry_(
    const ObMicroBlock *mb,
    const ObIArray<ObInvIdxLogicalBlockMeta> *metas)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mb) || OB_ISNULL(metas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null term entry", K(ret), KP(mb), KP(metas));
  } else {
    curr_micro_block_ = mb;
    curr_term_metas_ = metas;
    // term 前缀 = entry endkey 的前 term_col_cnt_ 个 datum (term..., max_doc_id)。
    // 借用；在 micro_iter_ reset 之前保持稳定（index entry 驻留在其 index_alloc_ 中）。
    const ObDatumRowkey &endkey = mb->range_.get_end_key();
    if (OB_UNLIKELY(endkey.get_datum_cnt() < term_col_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("term entry endkey too short", K(ret), K(endkey), K(term_col_cnt_));
    } else if (OB_FAIL(curr_term_.assign(const_cast<ObStorageDatum *>(endkey.datums_), term_col_cnt_))) {
      LOG_WARN("fail to assign curr term", K(ret));
    } else if (OB_FAIL(micro_iter_.get_curr_value_micro_block(curr_value_block_))) {
      LOG_WARN("fail to get current value micro block", K(ret));
    } else {
      // 预计算整个 entry 是否在 range end 之内：endkey (term, max_doc_id)
      // 是该 entry 的最大键，因此若它不越过 end_key，则没有行能越过，
      // next_row() 可跳过逐行 end 比较。这是逐行捷径，而非 entry 跳过 ——
      // 横跨 end_key 的 term 保持 false，在行级别进行边界判定。
      bool beyond_end = false;
      if (OB_FAIL(beyond_range_end_(endkey, beyond_end))) {
        LOG_WARN("fail to precompute entry within-end flag", K(ret), K(endkey));
      } else {
        curr_entry_within_end_ = !beyond_end;
      }
    }
  }
  return ret;
}

int ObInvSSTableMergeIter::enter_logical_block_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(curr_micro_block_) || OB_ISNULL(curr_term_metas_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current term entry to open logical block on", K(ret));
  } else if (OB_FAIL(logical_iter_.open(
          curr_micro_block_->payload_data_, curr_term_metas_,
          curr_value_block_.is_valid() ? &curr_value_block_ : nullptr))) {
    LOG_WARN("fail to open posting block iterator", K(ret));
  } else if (OB_FAIL(logical_iter_.next())) {
    // 正确构造的 term entry 始终包含 >=1 个 logical block。空 entry 是损坏数据，
    // 不是正常的终态 —— 应当显式报错而非上溯。
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("fail to land first logical block of term entry", K(ret));
  } else if (OB_FAIL(logical_iter_.get_curr_block(curr_posting_block_))) {
    LOG_WARN("fail to get first logical block", K(ret));
  } else {
    curr_level_ = Level::LOGICAL_BLOCK;
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

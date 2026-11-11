/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

// ==================== ObSortedFileInfo ====================

int ObSortedFileInfo::assign(const ObSortedFileInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(file_handle_.assign(other.file_handle_))) {
      LOG_WARN("fail to assign file handle", KR(ret), K(other));
    } else {
      sample_item_ = other.sample_item_;
      item_count_ = other.item_count_;
    }
  }
  return ret;
}


// K-way merge + dedup write of inputs[start_idx, end_idx) to one new tmp file.
static int merge_and_dedup_sorted_files_(
    const ObIArray<ObSortedFileInfo> &inputs,
    const int64_t start_idx,
    const int64_t end_idx,
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t expire_timestamp,
    const int64_t dir_id,
    ObGCMacroBlockCompare &compare,
    ObMarkSweepTmpFileMgr &tmp_file_mgr,
    ObSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((start_idx < 0)
      || (end_idx <= start_idx)
      || (end_idx > inputs.count())
      || (OB_INVALID_ID == tenant_id)
      || (file_buf_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx), "inputs_count",
        inputs.count(), K(tenant_id), K(file_buf_size), K(expire_timestamp), K(dir_id));
  } else {
    output.reset();
    ObArenaAllocator reader_alloc("FragReader", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    ObSEArray<ObFragmentIterator<ObGCMacroBlockItem> *, 64> readers;
    readers.set_attr(ObMemAttr(tenant_id, "SortMergeFile"));

    // init readers
    for (int64_t i = start_idx; OB_SUCC(ret) && (i < end_idx); ++i) {
      void *buf = reader_alloc.alloc(sizeof(ObFragmentReaderV2<ObGCMacroBlockItem>));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem for reader", KR(ret));
      } else {
        ObFragmentReaderV2<ObGCMacroBlockItem> *reader =
            new (buf) ObFragmentReaderV2<ObGCMacroBlockItem>(OB_MALLOC_NORMAL_BLOCK_SIZE);
        if (OB_FAIL(reader->init_for_pread(inputs.at(i).get_fd(), inputs.at(i).get_dir_id(),
                                 expire_timestamp, tenant_id,
                                 inputs.at(i).sample_item_, file_buf_size))) {
          LOG_WARN("fail to init reader", KR(ret), K(i), "file_info", inputs.at(i),
              K(expire_timestamp), K(tenant_id), K(file_buf_size));
          reader->~ObFragmentReaderV2();
        } else if (OB_FAIL(readers.push_back(reader))) {
          LOG_WARN("fail to push back reader", KR(ret));
          reader->~ObFragmentReaderV2();
        }
      }
    }

    // K-way merge and dedup
    if (OB_SUCC(ret)) {
      ObFragmentMerge<ObGCMacroBlockItem, ObGCMacroBlockCompare> merge;
      ObFragmentWriterV2<ObGCMacroBlockItem> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
      ObMarkSweepTmpFileHandle file_handle;
      if (OB_FAIL(merge.init(readers, &compare))) {
        LOG_WARN("fail to init merge", KR(ret));
      } else if (OB_FAIL(merge.open())) {
        LOG_WARN("fail to open merge", KR(ret));
      } else if (OB_FAIL(writer.open(file_buf_size, expire_timestamp, tenant_id, dir_id))) {
        LOG_WARN("fail to open writer", KR(ret));
      } else if (OB_FAIL(tmp_file_mgr.acquire_file(tenant_id, writer.get_dir_id(),
                                                   writer.get_fd(), file_handle))) {
        LOG_WARN("fail to acquire tmp file", KR(ret), K(writer.get_fd()));
      } else {
        const ObGCMacroBlockItem *item = nullptr;
        ObGCMacroBlockItem prev_written_item;
        prev_written_item.reset();
        bool has_prev = false;
        bool is_complete = false;
        int64_t written_cnt = 0;
        while (OB_SUCC(ret) && !is_complete) {
          if (OB_FAIL(merge.get_next_item(item))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              is_complete = true;
            }
          } else if (!has_prev || (*item != prev_written_item)) {
            if (OB_FAIL(writer.write_item(*item))) {
              LOG_WARN("fail to write merged item", KR(ret));
            } else {
              prev_written_item = *item;
              has_prev = true;
              ++written_cnt;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(writer.sync())) {
            LOG_WARN("fail to sync output", KR(ret));
          } else if (OB_FAIL(output.file_handle_.assign(file_handle))) {
            LOG_WARN("fail to assign output tmp file handle", KR(ret));
          } else {
            output.sample_item_ = writer.get_sample_item();
            output.item_count_ = written_cnt;
          }
        }
      }
    }

    for (int64_t i = 0; i < readers.count(); ++i) {
      if (nullptr != readers.at(i)) {
        readers.at(i)->~ObFragmentIterator();
      }
    }
  }
  return ret;
}


// ==================== ObSortedRunWriter ====================

ObSortedRunWriter::ObSortedRunWriter()
  : is_inited_(false), tenant_id_(OB_INVALID_ID),
    memory_limit_(0), file_buf_size_(0), expire_timestamp_(0),
    compare_(nullptr), tmp_file_mgr_(nullptr), dir_id_(-1),
    mem_allocator_(ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, OB_MALLOC_NORMAL_BLOCK_SIZE),
    item_list_(nullptr, ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
    has_data_(false), flush_sorted_run_cost_us_(0)
{
}

ObSortedRunWriter::~ObSortedRunWriter()
{
  reset();
}

int ObSortedRunWriter::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t expire_timestamp,
    ObGCMacroBlockCompare *compare,
    ObMarkSweepTmpFileMgr *tmp_file_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSortedRunWriter init twice", KR(ret));
  } else if (OB_UNLIKELY((OB_INVALID_ID == tenant_id)
      || (memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT)
      || (file_buf_size <= 0)
      || (nullptr == compare)
      || (nullptr == tmp_file_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(memory_limit),
        K(file_buf_size), KP(compare), KP(tmp_file_mgr));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id_))) {
    LOG_WARN("fail to alloc dir", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    tmp_file_mgr_ = tmp_file_mgr;
    mem_allocator_.set_attr(ObMemAttr(tenant_id_, "SortedRunWr"));
    runs_.set_attr(ObMemAttr(tenant_id_, "SortedRunWr"));
    has_data_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObSortedRunWriter::add_item(const ObGCMacroBlockItem &item)
{
  int ret = OB_SUCCESS;
  const int64_t item_size = sizeof(ObGCMacroBlockItem) + item.get_deep_copy_size();
  char *buf = nullptr;
  ObGCMacroBlockItem *new_item = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSortedRunWriter not init", KR(ret));
  } else if (((mem_allocator_.used() + item_size) > memory_limit_) && (item_list_.size() > 0)) {
    ObCostTimer cost_timer(flush_sorted_run_cost_us_);
    if (OB_FAIL(flush_run_())) {
      LOG_WARN("fail to flush run", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(mem_allocator_.alloc(item_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for item", KR(ret), K(item_size));
  } else if (FALSE_IT(new_item = new (buf) ObGCMacroBlockItem())) {
  } else {
    int64_t pos = sizeof(ObGCMacroBlockItem);
    if (OB_FAIL(new_item->deep_copy(item, buf, item_size, pos))) {
      LOG_WARN("fail to deep copy item", KR(ret));
    } else if (OB_FAIL(item_list_.push_back(new_item))) {
      LOG_WARN("fail to push back", KR(ret));
    } else {
      new_item = nullptr;
      has_data_ = true;
    }
    if (OB_NOT_NULL(new_item)) {
      new_item->~ObGCMacroBlockItem();
    }
  }
  return ret;
}

int ObSortedRunWriter::flush_run_()
{
  int ret = OB_SUCCESS;
  if (item_list_.size() == 0) {
    // nothing to flush
  } else {
    // 1. sort
    lib::ob_sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      LOG_WARN("fail to sort", KR(ret));
    }

    // 2. dedup and write to a temporary file
    if (OB_SUCC(ret)) {
      ObFragmentWriterV2<ObGCMacroBlockItem> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
      ObMarkSweepTmpFileHandle file_handle;
      if (OB_FAIL(writer.open(file_buf_size_, expire_timestamp_, tenant_id_, dir_id_))) {
        LOG_WARN("fail to open writer", KR(ret));
      } else if (OB_FAIL(tmp_file_mgr_->acquire_file(tenant_id_, writer.get_dir_id(),
                                                     writer.get_fd(), file_handle))) {
        LOG_WARN("fail to acquire tmp file", KR(ret), K(writer.get_fd()));
      } else {
        const ObGCMacroBlockItem *prev = nullptr;
        int64_t written_cnt = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (i < item_list_.size()); ++i) {
          ObGCMacroBlockItem *cur = item_list_.at(i);
          if ((nullptr == prev) || (*cur != *prev)) {
            if (OB_FAIL(writer.write_item(*cur))) {
              LOG_WARN("fail to write item", KR(ret));
            } else {
              prev = cur;
              ++written_cnt;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(writer.sync())) {
            LOG_WARN("fail to sync writer", KR(ret));
          } else {
            ObSortedFileInfo run;
            if (OB_FAIL(run.file_handle_.assign(file_handle))) {
              LOG_WARN("fail to assign tmp file handle", KR(ret));
            } else {
              run.sample_item_ = writer.get_sample_item();
              run.item_count_ = written_cnt;
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(runs_.push_back(run))) {
              LOG_WARN("fail to push back run", KR(ret));
            }
          }
        }
      }
    }

    // 3. 仅在 run 成功写入并 push 到 runs_ 之后才清空内存缓冲。失败时必须保留 item_list_：
    // 一个 SortTask 会把多个 tablet 聚合进同一个 sorter，该缓冲可能持有前面已成功 tablet 的
    // DEL/KEPT item。此处若销毁，单 tablet 重试时这些 item 会丢失；丢失 KEPT item 会让仍被
    // 引用的宏块变成垃圾（DEL - KEPT），造成数据丢失。
    if (OB_SUCC(ret)) {
      destruct_vector_list(item_list_);
      item_list_.reset();
      mem_allocator_.reuse();
    }
  }
  return ret;
}

int ObSortedRunWriter::finish(ObIArray<ObSortedFileInfo> &results)
{
  int ret = OB_SUCCESS;
  results.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSortedRunWriter not init", KR(ret));
  } else {
    // flush remaining in-memory data as its own run
    if (item_list_.size() > 0) {
      if (OB_FAIL(flush_run_())) {
        LOG_WARN("fail to flush remaining run", KR(ret));
      }
    }
    // Hand out all runs as separate files without a final merge. Cross-run dedup
    // is deferred to the global pipeline merge.
    if (FAILEDx(append(results, runs_))) {
      LOG_WARN("fail to append runs", KR(ret), "run_count", runs_.count());
    }
    runs_.reset();
  }
  return ret;
}

void ObSortedRunWriter::reset()
{
  destruct_vector_list(item_list_);
  item_list_.reset();
  mem_allocator_.reuse();
  runs_.reset();
  has_data_ = false;
  flush_sorted_run_cost_us_ = 0;
  is_inited_ = false;
  tmp_file_mgr_ = nullptr;
}


// ==================== ObMultiPassMerger ====================

ObMultiPassMerger::ObMultiPassMerger()
  : is_inited_(false), tenant_id_(OB_INVALID_ID),
    file_buf_size_(0), expire_timestamp_(0), compare_(nullptr),
    tmp_file_mgr_(nullptr), dir_id_(-1),
    input_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK)
{
}

ObMultiPassMerger::~ObMultiPassMerger()
{
  cleanup();
}

int ObMultiPassMerger::init(
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t expire_timestamp,
    ObGCMacroBlockCompare *compare,
    ObMarkSweepTmpFileMgr *tmp_file_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMultiPassMerger init twice", KR(ret));
  } else if (OB_UNLIKELY((OB_INVALID_ID == tenant_id)
      || (nullptr == compare)
      || (nullptr == tmp_file_mgr))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id_))) {
    LOG_WARN("fail to alloc dir", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    file_buf_size_ = file_buf_size;
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    tmp_file_mgr_ = tmp_file_mgr;
    cur_inputs_.set_attr(ObMemAttr(tenant_id_, "MulPassMerge"));
    next_inputs_.set_attr(ObMemAttr(tenant_id_, "MulPassMerge"));
    is_inited_ = true;
  }
  return ret;
}

int ObMultiPassMerger::add_input(const ObIArray<ObSortedFileInfo> &file_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiPassMerger not init", KR(ret));
  } else {
    ObSpinLockGuard guard(input_lock_);
    const int64_t original_count = cur_inputs_.count();
    if (OB_FAIL(append(cur_inputs_, file_infos))) {
      LOG_WARN("fail to append inputs", KR(ret), K(file_infos));
    }
    if (OB_FAIL(ret)) {
      while (cur_inputs_.count() > original_count) {
        cur_inputs_.pop_back();
      }
    }
  }
  return ret;
}

int64_t ObMultiPassMerger::get_input_count() const
{
  ObSpinLockGuard guard(input_lock_);
  return cur_inputs_.count();
}

int ObMultiPassMerger::single_pass_merge_(
    const ObIArray<ObSortedFileInfo> &inputs,
    const int64_t start_idx,
    const int64_t end_idx,
    ObSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiPassMerger not init", KR(ret));
  } else if (OB_UNLIKELY((start_idx < 0) || (end_idx <= start_idx) || (end_idx > inputs.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx),
        "inputs_count", inputs.count());
  } else if (OB_FAIL(merge_and_dedup_sorted_files_(inputs, start_idx, end_idx,
                                                   tenant_id_, file_buf_size_, expire_timestamp_,
                                                   dir_id_, *compare_, *tmp_file_mgr_, output))) {
    LOG_WARN("fail to single pass merge", KR(ret), K(start_idx), K(end_idx));
  }
  return ret;
}

int64_t ObMultiPassMerger::get_batch_count() const
{
  ObSpinLockGuard guard(input_lock_);
  return (cur_inputs_.count() + MAX_FAN_IN - 1) / MAX_FAN_IN;
}

int ObMultiPassMerger::get_batch_range(
    const int64_t batch_idx,
    int64_t &start,
    int64_t &end) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(input_lock_);
  const int64_t input_cnt = cur_inputs_.count();
  const int64_t batch_count = (input_cnt + MAX_FAN_IN - 1) / MAX_FAN_IN;
  if (OB_UNLIKELY((batch_idx < 0) || (batch_idx >= batch_count))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch_idx", KR(ret), K(batch_idx), K(batch_count));
  } else {
    start = batch_idx * MAX_FAN_IN;
    end = MIN(start + MAX_FAN_IN, input_cnt);
  }
  return ret;
}

int ObMultiPassMerger::batch_merge(const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;
  ObSortedFileInfo merged;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiPassMerger not init", KR(ret));
  } else {
    const int64_t cur_input_cnt = get_input_count();
    if (OB_UNLIKELY((start < 0) || (end <= start) || (end > cur_input_cnt))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid batch range", KR(ret), K(start), K(end), K(cur_input_cnt));
    // user should guarantee no concurrent prepare_next_pass()/add_input() during a pass,
    // so cur_inputs_ is stable here and the heavy I/O runs lock-free.
    } else if (OB_FAIL(single_pass_merge_(cur_inputs_, start, end, merged))) {
      LOG_WARN("fail to merge batch", KR(ret), K(start), K(end));
    } else {
      ObSpinLockGuard guard(input_lock_);
      if (OB_FAIL(next_inputs_.push_back(merged))) {
        LOG_WARN("fail to push back merged", KR(ret));
      }
    }
  }
  return ret;
}

int ObMultiPassMerger::prepare_next_pass()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiPassMerger not init", KR(ret));
  } else {
    ObSpinLockGuard guard(input_lock_);
    cur_inputs_.reset();
    if (OB_FAIL(append(cur_inputs_, next_inputs_))) {
      LOG_WARN("fail to append next_inputs", KR(ret));
    } else {
      next_inputs_.reset();
    }
  }
  return ret;
}

int ObMultiPassMerger::take_final_input(ObSortedFileInfo &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiPassMerger not init", KR(ret));
  } else {
    ObSpinLockGuard guard(input_lock_);
    if (cur_inputs_.count() == 0) {
      LOG_INFO("final input is empty");
    } else if (cur_inputs_.count() == 1) {
      if (OB_FAIL(result.assign(cur_inputs_.at(0)))) {
        LOG_WARN("fail to assign final input", KR(ret));
      }
    } else { // cur_inputs_.count() >= 2
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("take_final_input called with multiple inputs", KR(ret),
          "input_cnt", cur_inputs_.count());
    }
  }
  return ret;
}

void ObMultiPassMerger::cleanup()
{
  {
    ObSpinLockGuard guard(input_lock_);
    cur_inputs_.reset();
    next_inputs_.reset();
  }
  tmp_file_mgr_ = nullptr;
  is_inited_ = false;
}


// ==================== ObSortedSetDiff ====================

ObSortedSetDiff::ObSortedSetDiff()
  : is_inited_(false), left_file_(), right_file_(),
    left_reader_(OB_MALLOC_NORMAL_BLOCK_SIZE), right_reader_(OB_MALLOC_NORMAL_BLOCK_SIZE),
    left_item_(nullptr), right_item_(nullptr),
    left_ret_(OB_ITER_END), right_ret_(OB_ITER_END)
{
}

ObSortedSetDiff::~ObSortedSetDiff()
{
  cleanup();
}

int ObSortedSetDiff::prepare_(
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t expire_timestamp,
    const ObSortedFileInfo &info,
    ObFragmentReaderV2<ObGCMacroBlockItem> &reader,
    const ObGCMacroBlockItem *&item,
    int &iter_ret)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    // get_next_item handles the boundary via iter_ret == OB_ITER_END
    iter_ret = OB_ITER_END;
  } else if (OB_FAIL(reader.init_for_pread(info.get_fd(), info.get_dir_id(), expire_timestamp,
                                           tenant_id, info.sample_item_, file_buf_size))) {
    LOG_WARN("fail to init reader", KR(ret), K(info), K(expire_timestamp),
        K(tenant_id), K(file_buf_size));
  } else if (OB_FAIL(reader.open())) {
    LOG_WARN("fail to init reader", KR(ret), K(info), K(expire_timestamp),
        K(tenant_id), K(file_buf_size));
  } else {
    iter_ret = reader.get_next_item(item);
  }
  return ret;
}

int ObSortedSetDiff::init(
    const ObSortedFileInfo &left,
    const ObSortedFileInfo &right,
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t expire_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSortedSetDiff init twice", KR(ret));
  } else if (!left.is_valid()) {
    LOG_INFO("left file is empty");
  } else if (OB_FAIL(left_file_.assign(left))) {
    LOG_WARN("fail to assign left file", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (!right.is_valid()) {
    LOG_INFO("right file is empty");
  } else if (OB_FAIL(right_file_.assign(right))) {
    LOG_WARN("fail to assign right file", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_(tenant_id, file_buf_size, expire_timestamp, left_file_,
                              left_reader_, left_item_, left_ret_))) {
    LOG_WARN("fail to prepare left side", KR(ret));
  } else if (OB_FAIL(prepare_(tenant_id, file_buf_size, expire_timestamp, right_file_,
                              right_reader_, right_item_, right_ret_))) {
    LOG_WARN("fail to prepare right side", KR(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    cleanup();
  }
  return ret;
}

int ObSortedSetDiff::get_next_item(const ObGCMacroBlockItem *&item)
{
  int ret = OB_SUCCESS;
  item = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSortedSetDiff not init", KR(ret));
  } else {
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      if (OB_SUCCESS != left_ret_) {
        // left finished or errored out
        ret = (OB_ITER_END == left_ret_) ? OB_ITER_END : left_ret_;
      } else if (OB_SUCCESS != right_ret_ && OB_ITER_END != right_ret_) {
        // right errored out
        ret = right_ret_;
      } else if (OB_ITER_END == right_ret_ || *left_item_ < *right_item_) {
        // right is exhausted, or left_item < right_item: current element of left is not in right, emit it
        output_item_ = *left_item_;  // copy by value since get_next_item may overwrite curr_item_
        item = &output_item_;
        left_ret_ = left_reader_.get_next_item(left_item_);
        found = true;
      } else if (*left_item_ == *right_item_) {
        // equal: advance both left and right
        left_ret_ = left_reader_.get_next_item(left_item_);
        right_ret_ = right_reader_.get_next_item(right_item_);
      } else {
        // left_item > right_item: advance right
        right_ret_ = right_reader_.get_next_item(right_item_);
      }
    }
  }
  return ret;
}

void ObSortedSetDiff::cleanup()
{
  is_inited_ = false;
  left_item_ = nullptr;
  right_item_ = nullptr;
  left_ret_ = OB_ITER_END;
  right_ret_ = OB_ITER_END;
  left_reader_.reset();
  right_reader_.reset();
  left_file_.reset();
  right_file_.reset();
}


// ==================== ObMarkSweepPipeline ====================

ObMarkSweepPipeline::ObMarkSweepPipeline()
  : is_inited_(false), tenant_id_(OB_INVALID_ID), memory_limit_(0), file_buf_size_(0),
    expire_timestamp_(0), compare_(nullptr), tmp_file_mgr_(),
    file_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK), active_sorter_count_(0)
{
}

ObMarkSweepPipeline::~ObMarkSweepPipeline()
{
  if (OB_UNLIKELY(ATOMIC_LOAD(&active_sorter_count_) != 0)) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
        "Pipeline destroyed with active sorters! Possible use-after-free.",
        K_(active_sorter_count));
  }
  cleanup();
}

int ObMarkSweepPipeline::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t expire_timestamp,
    ObGCMacroBlockCompare *compare)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMarkSweepPipeline init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || nullptr == compare)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tmp_file_mgr_.init(tenant_id))) {
    LOG_WARN("fail to init mark sweep tmp file manager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_merger_.init(tenant_id, file_buf_size, expire_timestamp,
                                      compare, &tmp_file_mgr_))) {
    LOG_WARN("fail to init del merger", KR(ret));
  } else if (OB_FAIL(kept_merger_.init(tenant_id, file_buf_size, expire_timestamp,
                                       compare, &tmp_file_mgr_))) {
    LOG_WARN("fail to init kept merger", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    del_files_.set_attr(ObMemAttr(tenant_id_, "MarkSweepPipe"));
    kept_files_.set_attr(ObMemAttr(tenant_id_, "MarkSweepPipe"));
    is_inited_ = true;
  }
  return ret;
}

int ObMarkSweepPipeline::add_file(
    const ObMarkSweepGroupType group,
    const ObSortedFileInfo &file_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (!file_info.is_valid()) {
    // empty file, ignore
  } else {
    ObSpinLockGuard guard(file_lock_);
    ObSEArray<ObSortedFileInfo, 64> &files =
        ((ObMarkSweepGroupType::DEL == group) ? del_files_ : kept_files_);
    if (OB_FAIL(files.push_back(file_info))) {
      LOG_WARN("fail to push back file", KR(ret), K(group), K(file_info));
    }
  }
  return ret;
}

ObMultiPassMerger &ObMarkSweepPipeline::get_merger_(const ObMarkSweepGroupType group)
{
  return ((ObMarkSweepGroupType::DEL == group) ? del_merger_ : kept_merger_);
}

const ObMultiPassMerger &ObMarkSweepPipeline::get_merger_(const ObMarkSweepGroupType group) const
{
  return ((ObMarkSweepGroupType::DEL == group) ? del_merger_ : kept_merger_);
}

int ObMarkSweepPipeline::get_input_count(
    const ObMarkSweepGroupType group, int64_t &count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    count = get_merger_(group).get_input_count();
  }
  return ret;
}

int ObMarkSweepPipeline::get_batch_count(
    const ObMarkSweepGroupType group, int64_t &count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    count = get_merger_(group).get_batch_count();
  }
  return ret;
}

int ObMarkSweepPipeline::get_batch_range(
    const ObMarkSweepGroupType group,
    const int64_t batch_idx,
    int64_t &start, int64_t &end) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_FAIL(get_merger_(group).get_batch_range(batch_idx, start, end))) {
    LOG_WARN("fail to get batch range", KR(ret), K(group), K(batch_idx));
  }
  return ret;
}

int ObMarkSweepPipeline::batch_merge(
    const ObMarkSweepGroupType group,
    const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_FAIL(get_merger_(group).batch_merge(start, end))) {
    LOG_WARN("fail to batch merge", KR(ret), K(group), K(start), K(end));
  }
  return ret;
}

int ObMarkSweepPipeline::prepare_next_pass(const ObMarkSweepGroupType group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_FAIL(get_merger_(group).prepare_next_pass())) {
    LOG_WARN("fail to prepare next pass", KR(ret), K(group));
  }
  return ret;
}

int ObMarkSweepPipeline::prepare_merge()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&active_sorter_count_) != 0)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("prepare_merge called while sorters still active",
        KR(ret), K_(active_sorter_count));
  } else {
    diff_.cleanup();
    ObSpinLockGuard guard(file_lock_);
    merged_del_.reset();
    merged_kept_.reset();
    if (OB_FAIL(del_merger_.add_input(del_files_))) {
      LOG_WARN("fail to add del inputs", KR(ret), "cnt", del_files_.count());
    } else if (OB_FAIL(kept_merger_.add_input(kept_files_))) {
      LOG_WARN("fail to add kept inputs", KR(ret), "cnt", kept_files_.count());
    }
  }
  return ret;
}

int ObMarkSweepPipeline::complete_group(const ObMarkSweepGroupType group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    ObSpinLockGuard guard(file_lock_);
    ObSortedFileInfo &merged = ((ObMarkSweepGroupType::DEL == group) ? merged_del_ : merged_kept_);
    if (OB_FAIL(get_merger_(group).take_final_input(merged))) {
      LOG_WARN("fail to take final input", KR(ret), K(group));
    }
  }
  return ret;
}

int ObMarkSweepPipeline::complete_empty_group(const ObMarkSweepGroupType group)
{
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;
  ObFragmentWriterV2<ObGCMacroBlockItem> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSortedFileInfo empty_file;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_UNLIKELY(ObMarkSweepGroupType::DEL != group
      && ObMarkSweepGroupType::KEPT != group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty group", KR(ret), K(group));
  } else if (OB_UNLIKELY(0 != get_merger_(group).get_input_count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot complete nonempty group as empty", KR(ret), K(group));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id))) {
    LOG_WARN("fail to alloc empty group dir", KR(ret), K_(tenant_id), K(group));
  } else if (OB_FAIL(writer.open(
      file_buf_size_, expire_timestamp_, tenant_id_, dir_id))) {
    LOG_WARN("fail to open empty group writer", KR(ret), K_(tenant_id), K(group));
  } else if (OB_FAIL(tmp_file_mgr_.acquire_file(
      tenant_id_, writer.get_dir_id(), writer.get_fd(), empty_file.file_handle_))) {
    LOG_WARN("fail to acquire empty group file", KR(ret), K_(tenant_id), K(group));
  } else if (OB_FAIL(writer.sync())) {
    LOG_WARN("fail to sync empty group file", KR(ret), K_(tenant_id), K(group));
  } else {
    empty_file.sample_item_ = writer.get_sample_item();
    empty_file.item_count_ = 0;
    ObSpinLockGuard guard(file_lock_);
    ObSortedFileInfo &merged =
        (ObMarkSweepGroupType::DEL == group) ? merged_del_ : merged_kept_;
    if (OB_UNLIKELY(merged.is_valid())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("merged group file already exists", KR(ret), K(group), K(merged));
    } else if (OB_FAIL(merged.assign(empty_file))) {
      LOG_WARN("fail to publish empty group file", KR(ret), K(group));
    }
  }
  return ret;
}

int ObMarkSweepPipeline::complete_merge()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else if (OB_FAIL(init_diff_iter())) {
    LOG_WARN("fail to init diff iter", KR(ret));
  } else {
    ObSpinLockGuard guard(file_lock_);
    del_files_.reset();
    kept_files_.reset();
  }
  return ret;
}

int ObMarkSweepPipeline::init_diff_iter()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    diff_.cleanup();
    ObSortedFileInfo merged_del;
    ObSortedFileInfo merged_kept;
    {
      ObSpinLockGuard guard(file_lock_);
      if (merged_del_.is_valid()) {
        if (OB_FAIL(merged_del.assign(merged_del_))) {
          LOG_WARN("fail to assign merged_del", KR(ret));
        }
      }
      if (OB_SUCC(ret) && merged_kept_.is_valid()) {
        if (OB_FAIL(merged_kept.assign(merged_kept_))) {
          LOG_WARN("fail to assign merged_kept", KR(ret));
        }
      }
    }
    if (FAILEDx(diff_.init(merged_del, merged_kept, tenant_id_,
                           file_buf_size_, expire_timestamp_))) {
      LOG_WARN("fail to init diff", KR(ret), K(merged_del), K(merged_kept));
    }
  }
  return ret;
}

int ObMarkSweepPipeline::get_next_diff_item(const ObGCMacroBlockItem *&item)
{
  return diff_.get_next_item(item);
}

int ObMarkSweepPipeline::get_merged_item_count(
    const ObMarkSweepGroupType group,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    ObSpinLockGuard guard(file_lock_);
    const ObSortedFileInfo &merged = ((ObMarkSweepGroupType::DEL == group)
                                      ? merged_del_
                                      : merged_kept_);
    count = merged.item_count_;
  }
  return ret;
}

int ObMarkSweepPipeline::get_merged_file(
    const ObMarkSweepGroupType group,
    ObSortedFileInfo &file) const
{
  int ret = OB_SUCCESS;
  file.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mark sweep pipeline is not initialized", KR(ret));
  } else if (OB_UNLIKELY(ObMarkSweepGroupType::DEL != group
      && ObMarkSweepGroupType::KEPT != group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep merged file group", KR(ret), K(group));
  } else {
    ObSpinLockGuard guard(file_lock_);
    const ObSortedFileInfo &merged =
        (ObMarkSweepGroupType::DEL == group) ? merged_del_ : merged_kept_;
    if (!merged.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_FAIL(file.assign(merged))) {
      LOG_WARN("failed to retain mark sweep merged file", KR(ret), K(group));
    }
  }
  return ret;
}

int ObMarkSweepPipeline::get_merged_file_size(
    const ObMarkSweepGroupType group,
    int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepPipeline not init", KR(ret));
  } else {
    ObSpinLockGuard guard(file_lock_);
    const ObSortedFileInfo &merged = ((ObMarkSweepGroupType::DEL == group)
                                      ? merged_del_
                                      : merged_kept_);
    if (merged.is_valid()) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
              tenant_id_, merged.get_fd(), size))) {
        LOG_WARN("fail to get tmp file size", KR(ret), K(tenant_id_), K(group),
            "fd", merged.get_fd());
      }
    } else { // !merged.is_valid(), means empty group, set size to 0
      size = 0;
    }
  }
  return ret;
}

void ObMarkSweepPipeline::cleanup()
{
  diff_.cleanup();
  del_merger_.cleanup();
  kept_merger_.cleanup();
  {
    ObSpinLockGuard guard(file_lock_);
    del_files_.reset();
    kept_files_.reset();
    merged_del_.reset();
    merged_kept_.reset();
  }
  tmp_file_mgr_.reset();
  is_inited_ = false;
}



// ==================== ObMarkSweepSorter ====================

ObMarkSweepSorter::ObMarkSweepSorter()
  : is_inited_(false), pipeline_(nullptr)
{
}

ObMarkSweepSorter::~ObMarkSweepSorter()
{
  writer_.reset();
  if (nullptr != pipeline_) {
    ATOMIC_DEC(&pipeline_->active_sorter_count_);
    pipeline_ = nullptr;
  }
}

int ObMarkSweepSorter::init(ObMarkSweepPipeline *pipeline)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMarkSweepSorter init twice", KR(ret));
  } else if (OB_ISNULL(pipeline)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(writer_.init(pipeline->get_tenant_id(),
                                  pipeline->get_memory_limit(),
                                  pipeline->get_file_buf_size(),
                                  pipeline->get_expire_timestamp(),
                                  pipeline->get_compare(),
                                  &pipeline->tmp_file_mgr_))) {
    LOG_WARN("fail to init writer", KR(ret));
  } else {
    pipeline_ = pipeline;
    ATOMIC_INC(&pipeline_->active_sorter_count_);
    is_inited_ = true;
  }
  return ret;
}

int ObMarkSweepSorter::add_item(const ObGCMacroBlockItem &item)
{
  return writer_.add_item(item);
}

int ObMarkSweepSorter::finish(ObIArray<ObSortedFileInfo> &results)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMarkSweepSorter not init", KR(ret));
  } else if (OB_FAIL(writer_.finish(results))) {
    LOG_WARN("fail to finish writer", KR(ret));
  }
  writer_.reset();
  return ret;
}

} // namespace storage
} // namespace oceanbase

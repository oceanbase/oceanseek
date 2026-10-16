/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_list_sorter.h"
#include "lib/utility/ob_sort.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sorter.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObSSDataCheckListedObjectItem::ObSSDataCheckListedObjectItem()
  : name_()
{
}

int ObSSDataCheckListedObjectItem::init(const ObString &name)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(name.empty() || name.length() >= OB_MAX_URI_LENGTH
      || OB_NOT_NULL(name.find('\0')))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check listed object name", KR(ret), K(name));
  } else {
    name_ = name;
  }
  return ret;
}

void ObSSDataCheckListedObjectItem::reset()
{
  name_.reset();
}

bool ObSSDataCheckListedObjectItem::is_valid() const
{
  return !name_.empty() && name_.length() < OB_MAX_URI_LENGTH && OB_ISNULL(name_.find('\0'));
}

int64_t ObSSDataCheckListedObjectItem::get_deep_copy_size() const
{
  return name_.length();
}

int ObSSDataCheckListedObjectItem::deep_copy(
    const ObSSDataCheckListedObjectItem &src,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  if (src.name_.empty()) {
  } else if (OB_UNLIKELY(!src.is_valid() || OB_ISNULL(buf) || pos < 0
      || buf_len - pos < src.name_.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object deep copy argument", KR(ret), K(src), KP(buf), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, src.name_.ptr(), src.name_.length());
    name_.assign_ptr(buf + pos, src.name_.length());
    pos += src.name_.length();
  }
  return ret;
}

OB_DEF_SERIALIZE(ObSSDataCheckListedObjectItem)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object item", KR(ret), KPC(this));
  } else {
    OB_UNIS_ENCODE(name_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSSDataCheckListedObjectItem)
{
  int ret = OB_SUCCESS;
  reset();
  OB_UNIS_DECODE(name_);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("deserialized invalid listed object item", KR(ret), KPC(this));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSSDataCheckListedObjectItem)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(name_);
  return len;
}

bool ObSSDataCheckListedObjectItemCompare::operator()(
    const ObSSDataCheckListedObjectItem *left,
    const ObSSDataCheckListedObjectItem *right) const
{
  bool less = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_ISNULL(left) || OB_ISNULL(right) || !left->is_valid() || !right->is_valid()) {
    result_code_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(result_code_, "invalid listed object item for compare", KP(left), KP(right));
  } else {
    less = left->get_name().compare(right->get_name()) < 0;
  }
  return less;
}

ObSSDataCheckListedObjectSorter::ObSSDataCheckListedObjectSorter()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    dir_id_(-1),
    fan_in_(0),
    in_memory_size_(0),
    collected_count_(0),
    stop_checker_(nullptr),
    ctx_(nullptr),
    mem_allocator_("DCListSort", OB_MALLOC_NORMAL_BLOCK_SIZE),
    item_list_(),
    run_levels_(),
    final_runs_(),
    merge_work_runs_()
{
}

ObSSDataCheckListedObjectSorter::~ObSSDataCheckListedObjectSorter()
{
  reset();
}

int ObSSDataCheckListedObjectSorter::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    const share::ObCheckStopProvider &stop_checker,
    ObSSDataCheckCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t max_fan_in_by_memory = file_buf_size > 0
      ? (memory_limit / file_buf_size - 1) / 2 : 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("listed object sorter initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || max_fan_in_by_memory < 2 || io_expire_timestamp < 0 || !ctx.is_valid()
      || ctx.get_tenant_id() != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object sorter argument", KR(ret), K(tenant_id),
        K(memory_limit), K(file_buf_size), K(io_expire_timestamp),
        K(max_fan_in_by_memory), "ctx_tenant_id", ctx.get_tenant_id());
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
    fan_in_ = MIN(MAX_FAN_IN, max_fan_in_by_memory);
    stop_checker_ = &stop_checker;
    ctx_ = &ctx;
    mem_allocator_.set_attr(ObMemAttr(tenant_id_, "DCListSort"));
    item_list_.set_attr(ObMemAttr(tenant_id_, "DCListSort"));
    for (int64_t i = 0; i < MAX_COMPACT_LEVEL_COUNT; ++i) {
      run_levels_[i].set_attr(ObMemAttr(tenant_id_, "DCListRun"));
    }
    final_runs_.set_attr(ObMemAttr(tenant_id_, "DCListRun"));
    merge_work_runs_.set_attr(ObMemAttr(tenant_id_, "DCListRun"));
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("listed object sorter not initialized", KR(ret));
  } else if (OB_FAIL(stop_checker_->check_stop())) {
    LOG_INFO("listed object collection stopped before next entry", KR(ret), K_(collected_count));
  } else if (OB_ISNULL(entry) || OB_UNLIKELY('\0' == entry->d_name[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object entry", KR(ret), KP(entry));
  } else {
    const int64_t name_len = STRLEN(entry->d_name);
    const ObString name(static_cast<int32_t>(name_len), entry->d_name);
    if (OB_UNLIKELY(name_len >= OB_MAX_URI_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("listed object name is too long", KR(ret), K(name_len));
    } else if (OB_FAIL(add_name_(name))) {
      LOG_WARN("failed to add listed object name", KR(ret), K(name));
    } else {
      ++collected_count_;
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::add_name_(const ObString &name)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  ObSSDataCheckListedObjectItem *item = nullptr;
  const int64_t item_memory_limit = memory_limit_ - file_buf_size_;
  const int64_t item_size = sizeof(ObSSDataCheckListedObjectItem)
      + name.length();
  const int64_t charged_size = item_size
      + 2 * sizeof(ObSSDataCheckListedObjectItem *);
  if (OB_UNLIKELY(name.empty() || name.length() >= OB_MAX_URI_LENGTH
      || charged_size > item_memory_limit)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("listed object item exceeds memory limit", KR(ret),
        K(name), K(charged_size), K(item_memory_limit));
  } else if (in_memory_size_ + charged_size > item_memory_limit && item_list_.size() > 0) {
    if (OB_FAIL(flush_run_())) {
      LOG_WARN("failed to flush listed object run", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(mem_allocator_.alloc(item_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate listed object item", KR(ret), K(item_size));
  } else if (FALSE_IT(item = new (buf) ObSSDataCheckListedObjectItem())) {
  } else {
    char *name_buf = buf + sizeof(ObSSDataCheckListedObjectItem);
    MEMCPY(name_buf, name.ptr(), name.length());
    const ObString cached_name(static_cast<int32_t>(name.length()), name_buf);
    if (OB_FAIL(item->init(cached_name))) {
      LOG_WARN("failed to initialize listed object item", KR(ret), K(name));
    } else if (OB_FAIL(item_list_.push_back(item))) {
      LOG_WARN("failed to append listed object item", KR(ret), K(name));
    } else {
      in_memory_size_ += charged_size;
      item = nullptr;
    }
    if (OB_NOT_NULL(item)) {
      item->~ObSSDataCheckListedObjectItem();
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::sort_memory_items_()
{
  int ret = OB_SUCCESS;
  if (item_list_.size() > 1) {
    int compare_ret = OB_SUCCESS;
    ObSSDataCheckListedObjectItemCompare compare(compare_ret);
    lib::ob_sort(item_list_.begin(), item_list_.end(), compare);
    if (OB_SUCCESS != compare_ret) {
      ret = compare_ret;
      LOG_WARN("failed to sort listed object items", KR(ret));
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::sort_and_replay(
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("listed object sorter not initialized", KR(ret));
  } else if (!has_file_runs_()) {
    if (OB_UNLIKELY(item_list_.size() != collected_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in-memory listed object count mismatch", KR(ret),
          "item_count", item_list_.size(), K_(collected_count));
    } else if (OB_FAIL(sort_memory_items_())) {
      LOG_WARN("failed to sort listed objects in memory", KR(ret));
    } else if (OB_FAIL(replay_memory_items_(op))) {
      LOG_WARN("failed to replay listed objects from memory", KR(ret));
    }
  } else {
    ObSSDataCheckSortedFileInfo final_file;
    if (OB_FAIL(flush_run_())) {
      LOG_WARN("failed to flush final listed object run", KR(ret));
    } else {
      // 初始 run 已经全部落盘，释放名称缓存后再为多路归并申请读写 buffer。
      mem_allocator_.reset();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(merge_all_runs_(final_file))) {
        LOG_WARN("failed to merge listed object runs", KR(ret));
      } else if (OB_FAIL(replay_file_(final_file, op))) {
        LOG_WARN("failed to replay listed objects from file", KR(ret));
      }
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::replay_memory_items_(
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
    const ObSSDataCheckListedObjectItem *item = item_list_.at(i);
    if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid in-memory listed object item", KR(ret), K(i), KP(item));
    } else if (OB_FAIL(replay_item_(*item, op))) {
      LOG_WARN("failed to replay in-memory listed object item", KR(ret), K(i));
    } else if (OB_FAIL(check_stop_(i + 1))) {
      LOG_INFO("listed object replay stopped", KR(ret), K(i));
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::replay_file_(
    const ObSSDataCheckSortedFileInfo &file,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckListedObjectItem empty_sample;
  ListedObjectReader reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  const ObSSDataCheckListedObjectItem *item = nullptr;
  int64_t replayed_count = 0;
  bool is_complete = false;
  if (OB_UNLIKELY(!file.is_valid() || file.get_tenant_id() != tenant_id_
      || file.get_item_count() != collected_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object replay file", KR(ret), K(file),
        K_(tenant_id), K_(collected_count));
  } else if (OB_FAIL(reader.init_for_pread(file.get_fd(), file.get_dir_id(), io_expire_timestamp_,
      tenant_id_, empty_sample, file_buf_size_))) {
    LOG_WARN("failed to initialize listed object replay reader", KR(ret), K(file));
  } else if (OB_FAIL(reader.open())) {
    LOG_WARN("failed to open listed object replay reader", KR(ret), K(file));
  }
  while (OB_SUCC(ret) && !is_complete) {
    if (OB_FAIL(reader.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_complete = true;
      } else {
        LOG_WARN("failed to read sorted listed object", KR(ret));
      }
    } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sorted listed object", KR(ret), KP(item));
    } else if (OB_FAIL(replay_item_(*item, op))) {
      LOG_WARN("failed to replay sorted listed object", KR(ret), KPC(item));
    } else {
      ++replayed_count;
      if (OB_FAIL(check_stop_(replayed_count))) {
        LOG_INFO("listed object replay stopped", KR(ret), K(replayed_count));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(replayed_count != collected_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("listed object replay count mismatch", KR(ret), K(replayed_count), K_(collected_count));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::replay_item_(
    const ObSSDataCheckListedObjectItem &item,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  struct dirent entry;
  MEMSET(&entry, 0, sizeof(entry));
  const ObString &name = item.get_name();
  if (OB_UNLIKELY(!item.is_valid() || name.length() >= sizeof(entry.d_name))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("listed object name cannot be replayed", KR(ret), K(item));
  } else {
    MEMCPY(entry.d_name, name.ptr(), name.length());
    entry.d_name[name.length()] = '\0';
    if (OB_FAIL(op.func(&entry))) {
      LOG_WARN("listed object consumer failed", KR(ret), K(item));
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::ensure_dir_()
{
  int ret = OB_SUCCESS;
  if (dir_id_ >= 0) {
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id_))) {
    LOG_WARN("failed to allocate listed object run directory", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::flush_run_()
{
  int ret = OB_SUCCESS;
  if (0 == item_list_.size()) {
  } else if (OB_FAIL(sort_memory_items_())) {
    LOG_WARN("failed to sort listed object run", KR(ret));
  } else if (OB_FAIL(ensure_dir_())) {
    LOG_WARN("failed to prepare listed object run directory", KR(ret));
  } else {
    ListedObjectWriter writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObMarkSweepTmpFileHandle file_handle;
    if (OB_FAIL(writer.open(file_buf_size_, io_expire_timestamp_, tenant_id_, dir_id_))) {
      LOG_WARN("failed to open listed object run writer", KR(ret));
    } else if (OB_FAIL(ctx_->acquire_tmp_file(writer.get_dir_id(), writer.get_fd(), file_handle))) {
      LOG_WARN("failed to acquire listed object run file", KR(ret), K(writer.get_fd()));
    } else {
      int64_t written_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
        const ObSSDataCheckListedObjectItem *item = item_list_.at(i);
        if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid listed object run item", KR(ret), K(i), KP(item));
        } else if (OB_FAIL(writer.write_item(*item))) {
          LOG_WARN("failed to write listed object run item", KR(ret), K(i));
        } else {
          ++written_count;
          if (OB_FAIL(check_stop_(written_count))) {
            LOG_INFO("listed object run write stopped", KR(ret), K(written_count));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(writer.sync())) {
          LOG_WARN("failed to sync listed object run", KR(ret));
        } else {
          ObSSDataCheckSortedFileInfo run;
          if (OB_FAIL(run.init(file_handle, written_count))) {
            LOG_WARN("failed to publish listed object run", KR(ret), K(written_count));
          } else if (OB_FAIL(run_levels_[0].push_back(run))) {
            LOG_WARN("failed to save listed object run", KR(ret), K(run));
          }
        }
      }
    }
  }
  clear_memory_items_();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(compact_runs_if_needed_(0))) {
    LOG_WARN("failed to compact pending listed object runs", KR(ret));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::compact_runs_if_needed_(
    const int64_t start_level_idx)
{
  int ret = OB_SUCCESS;
  int64_t level_idx = start_level_idx;
  if (OB_UNLIKELY(start_level_idx < 0 || start_level_idx >= MAX_COMPACT_LEVEL_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object compact level", KR(ret), K(start_level_idx));
  }
  while (OB_SUCC(ret) && level_idx < MAX_COMPACT_LEVEL_COUNT
      && run_levels_[level_idx].count() >= MAX_PENDING_RUN_COUNT) {
    ObSSDataCheckSortedFileInfo compacted;
    if (OB_UNLIKELY(level_idx + 1 >= MAX_COMPACT_LEVEL_COUNT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("listed object compact level overflow", KR(ret), K(level_idx),
          "run_count", run_levels_[level_idx].count());
    } else if (OB_FAIL(merge_run_set_(run_levels_[level_idx], compacted))) {
      LOG_WARN("failed to compact listed object run level", KR(ret), K(level_idx));
    } else if (OB_FAIL(run_levels_[level_idx + 1].push_back(compacted))) {
      LOG_WARN("failed to save compacted listed object run", KR(ret), K(compacted));
    } else {
      ++level_idx;
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::collect_runs_for_final_merge_()
{
  int ret = OB_SUCCESS;
  final_runs_.reset();
  for (int64_t level_idx = 0; OB_SUCC(ret) && level_idx < MAX_COMPACT_LEVEL_COUNT; ++level_idx) {
    RunSet &level = run_levels_[level_idx];
    for (int64_t run_idx = 0; OB_SUCC(ret) && run_idx < level.count(); ++run_idx) {
      ObSSDataCheckSortedFileInfo run;
      if (OB_FAIL(run.assign(level.at(run_idx)))) {
        LOG_WARN("failed to copy listed object level run", KR(ret), K(level_idx), K(run_idx));
      } else if (OB_FAIL(final_runs_.push_back(run))) {
        LOG_WARN("failed to collect listed object level run", KR(ret), K(level_idx), K(run_idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t level_idx = 0; level_idx < MAX_COMPACT_LEVEL_COUNT; ++level_idx) {
      run_levels_[level_idx].reset();
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::merge_all_runs_(
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  output.reset();
  if (OB_FAIL(collect_runs_for_final_merge_())) {
    LOG_WARN("failed to collect listed object runs for final merge", KR(ret));
  } else if (OB_FAIL(merge_run_set_(final_runs_, output))) {
    LOG_WARN("failed to merge all listed object runs", KR(ret));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::merge_run_set_(
    RunSet &inputs,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  RunSet *current = &inputs;
  RunSet *next = &merge_work_runs_;
  output.reset();
  merge_work_runs_.reset();
  while (OB_SUCC(ret) && current->count() > 1) {
    next->reset();
    for (int64_t start = 0; OB_SUCC(ret) && start < current->count(); start += fan_in_) {
      const int64_t end = MIN(start + fan_in_, current->count());
      ObSSDataCheckSortedFileInfo merged;
      if (end - start == 1) {
        if (OB_FAIL(merged.assign(current->at(start)))) {
          LOG_WARN("failed to carry listed object run", KR(ret), K(start));
        }
      } else if (OB_FAIL(merge_batch_(*current, start, end, merged))) {
        LOG_WARN("failed to merge listed object run batch", KR(ret), K(start), K(end));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(next->push_back(merged))) {
        LOG_WARN("failed to save merged listed object run", KR(ret), K(merged));
      }
    }
    if (OB_SUCC(ret)) {
      current->reset();
      RunSet *tmp = current;
      current = next;
      next = tmp;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(1 != current->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid final listed object run count", KR(ret), "run_count", current->count());
    } else if (OB_FAIL(output.assign(current->at(0)))) {
      LOG_WARN("failed to publish final listed object run", KR(ret));
    } else {
      current->reset();
      next->reset();
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::merge_batch_(
    const RunSet &inputs,
    const int64_t start,
    const int64_t end,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator reader_allocator("DCListReader", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObSEArray<ListedObjectReader *, 64> reader_holders;
  ObSEArray<ListedObjectIterator *, 64> readers;
  output.reset();
  reader_holders.set_attr(ObMemAttr(tenant_id_, "DCListReader"));
  readers.set_attr(ObMemAttr(tenant_id_, "DCListMerge"));
  if (OB_UNLIKELY(start < 0 || end <= start || end > inputs.count() || end - start > fan_in_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid listed object merge range", KR(ret), K(start), K(end),
        "input_count", inputs.count(), K_(fan_in));
  } else if (OB_FAIL(init_merge_readers_(
      inputs, start, end, reader_allocator, reader_holders, readers))) {
    LOG_WARN("failed to initialize listed object merge readers", KR(ret));
  } else if (OB_FAIL(merge_and_write_(readers, output))) {
    LOG_WARN("failed to merge and write listed object files", KR(ret));
  }
  destroy_merge_readers_(reader_holders);
  return ret;
}

int ObSSDataCheckListedObjectSorter::init_merge_readers_(
    const RunSet &inputs,
    const int64_t start,
    const int64_t end,
    ObArenaAllocator &reader_allocator,
    ObSEArray<ListedObjectReader *, 64> &reader_holders,
    ObSEArray<ListedObjectIterator *, 64> &readers)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckListedObjectItem empty_sample;
  for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
    ListedObjectReader *reader = nullptr;
    void *buf = nullptr;
    const ObSSDataCheckSortedFileInfo &input = inputs.at(i);
    if (OB_UNLIKELY(!input.is_valid() || input.get_tenant_id() != tenant_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid listed object merge input", KR(ret), K(i), K(input));
    } else if (OB_ISNULL(buf = reader_allocator.alloc(sizeof(ListedObjectReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate listed object reader", KR(ret), K(i));
    } else if (FALSE_IT(reader = new (buf) ListedObjectReader(OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    } else if (OB_FAIL(reader->init_for_pread(input.get_fd(), input.get_dir_id(), io_expire_timestamp_,
        tenant_id_, empty_sample, file_buf_size_))) {
      LOG_WARN("failed to initialize listed object reader", KR(ret), K(i), K(input));
      reader->~ListedObjectReader();
    } else if (OB_FAIL(reader_holders.push_back(reader))) {
      LOG_WARN("failed to save listed object reader", KR(ret), K(i));
      reader->~ListedObjectReader();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < reader_holders.count(); ++i) {
    if (OB_FAIL(readers.push_back(static_cast<ListedObjectIterator *>(reader_holders.at(i))))) {
      LOG_WARN("failed to save listed object iterator", KR(ret), K(i));
    }
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::merge_and_write_(
    const ObIArray<ListedObjectIterator *> &readers,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  int compare_ret = OB_SUCCESS;
  ObSSDataCheckListedObjectItemCompare compare(compare_ret);
  ListedObjectMerge merge;
  ListedObjectWriter writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObMarkSweepTmpFileHandle file_handle;
  int64_t written_count = 0;
  if (OB_FAIL(merge.init(readers, &compare))) {
    LOG_WARN("failed to initialize listed object fragment merge", KR(ret));
  } else if (OB_FAIL(merge.open())) {
    LOG_WARN("failed to open listed object fragment merge", KR(ret));
  } else if (OB_SUCCESS != compare_ret) {
    ret = compare_ret;
    LOG_WARN("failed to compare initial listed object items", KR(ret));
  } else if (OB_FAIL(writer.open(file_buf_size_, io_expire_timestamp_, tenant_id_, dir_id_))) {
    LOG_WARN("failed to open listed object merge writer", KR(ret));
  } else if (OB_FAIL(ctx_->acquire_tmp_file(writer.get_dir_id(), writer.get_fd(), file_handle))) {
    LOG_WARN("failed to acquire listed object merge file", KR(ret), K(writer.get_fd()));
  } else if (OB_FAIL(write_merged_items_(merge, writer, written_count))) {
    LOG_WARN("failed to write merged listed objects", KR(ret));
  } else if (OB_SUCCESS != compare_ret) {
    ret = compare_ret;
    LOG_WARN("failed to compare listed object items", KR(ret));
  } else if (OB_FAIL(writer.sync())) {
    LOG_WARN("failed to sync listed object merge output", KR(ret));
  } else if (OB_FAIL(output.init(file_handle, written_count))) {
    LOG_WARN("failed to publish listed object merge output", KR(ret), K(written_count));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::write_merged_items_(
    ListedObjectMerge &merge,
    ListedObjectWriter &writer,
    int64_t &written_count)
{
  int ret = OB_SUCCESS;
  bool is_complete = false;
  const ObSSDataCheckListedObjectItem *item = nullptr;
  written_count = 0;
  while (OB_SUCC(ret) && !is_complete) {
    if (OB_FAIL(merge.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_complete = true;
      } else {
        LOG_WARN("failed to get merged listed object item", KR(ret));
      }
    } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid merged listed object item", KR(ret), KP(item));
    } else if (OB_FAIL(writer.write_item(*item))) {
      LOG_WARN("failed to write merged listed object item", KR(ret), KPC(item));
    } else {
      ++written_count;
      if (OB_FAIL(check_stop_(written_count))) {
        LOG_INFO("listed object merge stopped", KR(ret), K(written_count));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == written_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("listed object merge produced empty output", KR(ret));
  }
  return ret;
}

int ObSSDataCheckListedObjectSorter::check_stop_(
    const int64_t processed_count) const
{
  int ret = OB_SUCCESS;
  if (processed_count > 0 && 0 == processed_count % CHECK_STOP_INTERVAL
      && OB_FAIL(stop_checker_->check_stop())) {
    LOG_INFO("listed object sorter stopped", KR(ret), K(processed_count));
  }
  return ret;
}

bool ObSSDataCheckListedObjectSorter::has_file_runs_() const
{
  bool has_file_runs = false;
  for (int64_t i = 0; !has_file_runs && i < MAX_COMPACT_LEVEL_COUNT; ++i) {
    has_file_runs = run_levels_[i].count() > 0;
  }
  return has_file_runs;
}

void ObSSDataCheckListedObjectSorter::clear_memory_items_()
{
  for (int64_t i = 0; i < item_list_.size(); ++i) {
    if (OB_NOT_NULL(item_list_.at(i))) {
      item_list_.at(i)->~ObSSDataCheckListedObjectItem();
    }
  }
  item_list_.reset();
  mem_allocator_.reset();
  in_memory_size_ = 0;
}

void ObSSDataCheckListedObjectSorter::destroy_merge_readers_(
    ObSEArray<ListedObjectReader *, 64> &reader_holders)
{
  for (int64_t i = 0; i < reader_holders.count(); ++i) {
    if (OB_NOT_NULL(reader_holders.at(i))) {
      reader_holders.at(i)->~ListedObjectReader();
    }
  }
  reader_holders.reset();
}

void ObSSDataCheckListedObjectSorter::reset()
{
  clear_memory_items_();
  for (int64_t i = 0; i < MAX_COMPACT_LEVEL_COUNT; ++i) {
    run_levels_[i].reset();
  }
  final_runs_.reset();
  merge_work_runs_.reset();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  memory_limit_ = 0;
  file_buf_size_ = 0;
  io_expire_timestamp_ = 0;
  dir_id_ = -1;
  fan_in_ = 0;
  collected_count_ = 0;
  stop_checker_ = nullptr;
  ctx_ = nullptr;
}

} // namespace storage
} // namespace oceanbase

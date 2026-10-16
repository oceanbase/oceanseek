/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_expected_sorter.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_RUN_WRITE_ERROR);
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_RUN_SYNC_ERROR);
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_RUN_PUBLISH_ERROR);
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_MERGE_WRITE_ERROR);
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_MERGE_SYNC_ERROR);
ERRSIM_POINT_DEF(EN_SS_DATA_CHECK_EXPECTED_MERGE_PUBLISH_ERROR);
#endif

ObSSDataCheckExpectedSortItem::ObSSDataCheckExpectedSortItem()
  : item_(), relative_path_()
{
}

int ObSSDataCheckExpectedSortItem::init(
    const ObSSDataCheckExpectedItem &item,
    const ObString &relative_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(!item.is_valid() || relative_path.empty()
      || relative_path.length() >= OB_MAX_URI_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected sort item", KR(ret), K(item), K(relative_path));
  } else if (OB_FAIL(item_.deep_copy(item, nullptr, 0, pos))) {
    LOG_WARN("failed to copy expected item", KR(ret), K(item));
  } else {
    relative_path_ = relative_path;
  }
  return ret;
}

void ObSSDataCheckExpectedSortItem::reset()
{
  item_.reset();
  relative_path_.reset();
}

bool ObSSDataCheckExpectedSortItem::is_valid() const
{
  return item_.is_valid() && !relative_path_.empty() && relative_path_.length() < OB_MAX_URI_LENGTH;
}

bool ObSSDataCheckExpectedSortItem::is_same_key(
    const ObSSDataCheckExpectedSortItem &other) const
{
  return 0 == relative_path_.compare(other.relative_path_);
}

bool ObSSDataCheckExpectedSortItem::is_same_object(
    const ObSSDataCheckExpectedSortItem &other) const
{
  return is_same_key(other) && item_.is_same_object(other.item_);
}

bool ObSSDataCheckExpectedSortItemCompare::operator()(
    const ObSSDataCheckExpectedSortItem *left,
    const ObSSDataCheckExpectedSortItem *right) const
{
  bool less = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_ISNULL(left) || OB_ISNULL(right) || !left->is_valid() || !right->is_valid()) {
    result_code_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(result_code_, "invalid expected sort item for compare", KP(left), KP(right));
  } else {
    less = left->get_relative_path().compare(right->get_relative_path()) < 0;
  }
  return less;
}

ObSSDataCheckExpectedPathIterator::ObSSDataCheckExpectedPathIterator(
    const int64_t page_size)
  : is_inited_(false),
    reader_(page_size),
    reader_iter_(static_cast<ExpectedItemIterator *>(&reader_)),
    relative_path_buf_(),
    current_item_()
{
}

ObSSDataCheckExpectedPathIterator::~ObSSDataCheckExpectedPathIterator()
{
  reset();
}

int ObSSDataCheckExpectedPathIterator::init_for_pread(
    const ObSSDataCheckSortedFileInfo &input,
    const int64_t io_expire_timestamp,
    const uint64_t tenant_id,
    const int64_t file_buf_size)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckExpectedItem empty_sample;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected path iterator initialized twice", KR(ret));
  } else if (OB_UNLIKELY(!input.is_valid() || input.get_tenant_id() != tenant_id
      || OB_INVALID_ID == tenant_id || io_expire_timestamp < 0
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected path iterator argument", KR(ret), K(input),
        K(tenant_id), K(io_expire_timestamp), K(file_buf_size));
  } else if (OB_FAIL(reader_.init_for_pread(input.get_fd(), input.get_dir_id(), io_expire_timestamp,
      tenant_id, empty_sample, file_buf_size))) {
    LOG_WARN("failed to initialize expected fragment reader", KR(ret), K(input));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedPathIterator::get_next_item(
    const ObSSDataCheckExpectedSortItem *&item)
{
  int ret = OB_SUCCESS;
  const ObSSDataCheckExpectedItem *expected_item = nullptr;
  ObString relative_path;
  item = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected path iterator not initialized", KR(ret));
  } else if (OB_FAIL(reader_iter_->get_next_item(expected_item))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read expected item", KR(ret));
    }
  } else if (OB_ISNULL(expected_item) || OB_UNLIKELY(!expected_item->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expected item from fragment reader", KR(ret), KP(expected_item));
  } else if (OB_FAIL(ObSSDataCheckExpectedSortUtil::build_relative_path(
      expected_item->get_macro_id(), relative_path_buf_, sizeof(relative_path_buf_), relative_path))) {
    LOG_WARN("failed to build expected relative path", KR(ret), KPC(expected_item));
  } else if (OB_FAIL(current_item_.init(*expected_item, relative_path))) {
    LOG_WARN("failed to initialize current expected sort item", KR(ret), KPC(expected_item));
  } else {
    item = &current_item_;
  }
  return ret;
}

int ObSSDataCheckExpectedPathIterator::prefetch()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected path iterator not initialized", KR(ret));
  } else if (OB_FAIL(reader_iter_->prefetch())) {
    LOG_WARN("failed to prefetch expected fragment", KR(ret));
  }
  return ret;
}

int ObSSDataCheckExpectedPathIterator::clean_up()
{
  // 输入文件由临时文件 Handle 管理，这里只释放 reader 的 IO 状态，不能删除 fd。
  reset();
  return OB_SUCCESS;
}

void ObSSDataCheckExpectedPathIterator::reset()
{
  reader_.reset();
  current_item_.reset();
  relative_path_buf_[0] = '\0';
  is_inited_ = false;
}

ObSSDataCheckExpectedMergeParam::ObSSDataCheckExpectedMergeParam(
    const ObIArray<ObSSDataCheckSortedFileInfo> &inputs,
    const int64_t start,
    const int64_t end,
    const uint64_t tenant_id,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    const int64_t dir_id,
    ObMarkSweepTmpFileMgr &tmp_file_mgr)
  : inputs_(inputs),
    start_(start),
    end_(end),
    tenant_id_(tenant_id),
    file_buf_size_(file_buf_size),
    io_expire_timestamp_(io_expire_timestamp),
    dir_id_(dir_id),
    tmp_file_mgr_(tmp_file_mgr)
{
}

bool ObSSDataCheckExpectedMergeParam::is_valid() const
{
  return start_ >= 0 && end_ > start_ && end_ <= inputs_.count()
      && end_ - start_ <= ObSSDataCheckExpectedMerger::MAX_FAN_IN && OB_INVALID_ID != tenant_id_
      && ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size_)
      && io_expire_timestamp_ >= 0 && dir_id_ >= 0;
}

bool ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(
    const int64_t file_buf_size)
{
  return file_buf_size >= DIO_ALIGN_SIZE && 0 == file_buf_size % DIO_ALIGN_SIZE;
}

int ObSSDataCheckExpectedSortUtil::build_relative_path(
    const blocksstable::MacroBlockId &macro_id,
    char *buf,
    const int64_t buf_len,
    ObString &relative_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  relative_path.reset();
  if (OB_UNLIKELY(!macro_id.is_valid() || !macro_id.is_in_object_storage() || OB_ISNULL(buf)
      || buf_len <= 0 || buf_len > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relative path argument", KR(ret), K(macro_id), KP(buf), K(buf_len));
  } else if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, buf, buf_len, pos))) {
    LOG_WARN("failed to format expected relative path", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(pos <= 0 || pos >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expected relative path length", KR(ret), K(macro_id), K(pos), K(buf_len));
  } else {
    relative_path.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSSDataCheckExpectedSortUtil::merge_expected_files(
    const ObSSDataCheckExpectedMergeParam &param,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  output.reset();
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected merge argument", KR(ret), K(param));
  } else {
    ObArenaAllocator reader_allocator("DCExpReader", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
    ObSEArray<ExpectedReader *, 64> reader_holders;
    ObSEArray<ExpectedIterator *, 64> readers;
    if (OB_FAIL(init_merge_readers_(param, reader_allocator, reader_holders, readers))) {
      LOG_WARN("failed to initialize expected merge readers", KR(ret));
    } else if (OB_FAIL(merge_and_write_expected_files_(param, readers, output))) {
      LOG_WARN("failed to merge and write expected files", KR(ret));
    }
    destroy_merge_readers_(reader_holders);
  }
  return ret;
}

int ObSSDataCheckExpectedSortUtil::init_merge_readers_(
    const ObSSDataCheckExpectedMergeParam &param,
    ObArenaAllocator &reader_allocator,
    ObSEArray<ExpectedReader *, 64> &reader_holders,
    ObSEArray<ExpectedIterator *, 64> &readers)
{
  int ret = OB_SUCCESS;
  reader_holders.set_attr(ObMemAttr(param.tenant_id_, "DCExpReader"));
  readers.set_attr(ObMemAttr(param.tenant_id_, "DCExpMerge"));
  for (int64_t i = param.start_; OB_SUCC(ret) && i < param.end_; ++i) {
    ExpectedReader *reader = nullptr;
    void *buf = nullptr;
    const ObSSDataCheckSortedFileInfo &input = param.inputs_.at(i);
    if (OB_UNLIKELY(!input.is_valid() || input.get_tenant_id() != param.tenant_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expected merge input", KR(ret), K(i), K(input), K(param.tenant_id_));
    } else if (OB_ISNULL(buf = reader_allocator.alloc(sizeof(ExpectedReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate expected reader", KR(ret), K(i));
    } else if (FALSE_IT(reader = new (buf) ExpectedReader(OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    } else if (OB_FAIL(reader->init_for_pread(
        input, param.io_expire_timestamp_, param.tenant_id_, param.file_buf_size_))) {
      LOG_WARN("failed to initialize expected reader", KR(ret), K(i), K(input));
      reader->~ExpectedReader();
    } else if (OB_FAIL(reader_holders.push_back(reader))) {
      LOG_WARN("failed to save expected reader", KR(ret), K(i));
      reader->~ExpectedReader();
    } else if (OB_FAIL(readers.push_back(static_cast<ExpectedIterator *>(reader)))) {
      LOG_WARN("failed to save expected iterator", KR(ret), K(i));
    }
  }
  return ret;
}

int ObSSDataCheckExpectedSortUtil::merge_and_write_expected_files_(
    const ObSSDataCheckExpectedMergeParam &param,
    const ObIArray<ExpectedIterator *> &readers,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  int compare_ret = OB_SUCCESS;
  ObSSDataCheckExpectedSortItemCompare compare(compare_ret);
  ExpectedMerge merge;
  ExpectedWriter writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObMarkSweepTmpFileHandle file_handle;
  int64_t written_count = 0;
  if (OB_FAIL(merge.init(readers, &compare))) {
    LOG_WARN("failed to initialize expected fragment merge", KR(ret));
  } else if (OB_FAIL(merge.open())) {
    LOG_WARN("failed to open expected fragment merge", KR(ret));
  } else if (OB_FAIL(writer.open(param.file_buf_size_, param.io_expire_timestamp_,
      param.tenant_id_, param.dir_id_))) {
    LOG_WARN("failed to open expected merge writer", KR(ret));
  } else if (OB_FAIL(param.tmp_file_mgr_.acquire_file(
      param.tenant_id_, writer.get_dir_id(), writer.get_fd(), file_handle))) {
    LOG_WARN("failed to acquire expected merge file", KR(ret), K(writer.get_fd()));
  } else if (OB_FAIL(write_merged_items_(merge, writer, written_count))) {
    LOG_WARN("failed to write merged expected items", KR(ret));
  } else if (OB_UNLIKELY(0 == written_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge produced an empty output", KR(ret), "reader_count", readers.count());
#ifdef ERRSIM
  } else if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_MERGE_SYNC_ERROR) OB_SUCCESS)) {
    LOG_WARN("injected expected merge sync failure", KR(ret));
#endif
  } else if (OB_FAIL(writer.sync())) {
    LOG_WARN("failed to sync expected merge output", KR(ret));
  } else if (OB_FAIL(output.init(file_handle, written_count))) {
    LOG_WARN("failed to publish expected merge output", KR(ret), K(written_count));
#ifdef ERRSIM
  } else if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_MERGE_PUBLISH_ERROR) OB_SUCCESS)) {
    output.reset();
    LOG_WARN("injected expected merge publish failure", KR(ret), K(written_count));
#endif
  }
  return ret;
}

int ObSSDataCheckExpectedSortUtil::write_merged_items_(
    ExpectedMerge &merge,
    ExpectedWriter &writer,
    int64_t &written_count)
{
  int ret = OB_SUCCESS;
  const ObSSDataCheckExpectedSortItem *sort_item = nullptr;
  char previous_path_buffer[OB_MAX_URI_LENGTH];
  ObString previous_path;
  blocksstable::MacroBlockId previous_macro_id;
  bool has_previous = false;
  bool is_complete = false;
  written_count = 0;
  while (OB_SUCC(ret) && !is_complete) {
    if (OB_FAIL(merge.get_next_item(sort_item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_complete = true;
      } else {
        LOG_WARN("failed to get merged expected item", KR(ret));
      }
    } else if (OB_ISNULL(sort_item) || OB_UNLIKELY(!sort_item->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid merged expected sort item", KR(ret), KP(sort_item));
    } else {
      const ObSSDataCheckExpectedItem &item = sort_item->get_item();
      const ObString &relative_path = sort_item->get_relative_path();
      const int path_compare = has_previous ? previous_path.compare(relative_path) : -1;
      if (OB_UNLIKELY(path_compare > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected merge input is not ordered", KR(ret), K(previous_path), KPC(sort_item));
      } else if (0 == path_compare) {
        if (OB_UNLIKELY(previous_macro_id != item.get_macro_id())) {
          ret = OB_CONFLICT_VALUE;
          LOG_WARN("same expected relative path maps to different macro blocks",
              KR(ret), K(previous_path), K(previous_macro_id), KPC(sort_item));
        }
        // 相同路径和 MacroBlockId 是正常的跨 run 重复记录。
#ifdef ERRSIM
      } else if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_MERGE_WRITE_ERROR) OB_SUCCESS)) {
        LOG_WARN("injected expected merge write failure", KR(ret), K(item));
#endif
      } else if (OB_FAIL(writer.write_item(item))) {
        LOG_WARN("failed to write merged expected item", KR(ret), K(item));
      } else {
        MEMCPY(previous_path_buffer, relative_path.ptr(), relative_path.length());
        previous_path.assign_ptr(previous_path_buffer, relative_path.length());
        previous_macro_id = item.get_macro_id();
        has_previous = true;
        ++written_count;
      }
    }
  }
  return ret;
}

void ObSSDataCheckExpectedSortUtil::destroy_merge_readers_(
    ObSEArray<ExpectedReader *, 64> &reader_holders)
{
  for (int64_t i = 0; i < reader_holders.count(); ++i) {
    if (OB_NOT_NULL(reader_holders.at(i))) {
      reader_holders.at(i)->~ExpectedReader();
    }
  }
  reader_holders.reset();
}

// ==================== ObSSDataCheckExpectedRunWriter ====================

ObSSDataCheckExpectedRunWriter::ObSSDataCheckExpectedRunWriter()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    tmp_file_mgr_(nullptr),
    dir_id_(-1),
    mem_allocator_(ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, OB_MALLOC_NORMAL_BLOCK_SIZE),
    item_list_(nullptr, ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
    runs_()
{
}

ObSSDataCheckExpectedRunWriter::~ObSSDataCheckExpectedRunWriter()
{
  reset();
}

int ObSSDataCheckExpectedRunWriter::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    ObMarkSweepTmpFileMgr *tmp_file_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected run writer initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || io_expire_timestamp < 0 || OB_ISNULL(tmp_file_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected run writer argument", KR(ret), K(tenant_id),
        K(memory_limit), K(file_buf_size), K(io_expire_timestamp), KP(tmp_file_mgr));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id_))) {
    LOG_WARN("failed to allocate expected run directory", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
    tmp_file_mgr_ = tmp_file_mgr;
    mem_allocator_.set_attr(ObMemAttr(tenant_id_, "DCExpRunWr"));
    runs_.set_attr(ObMemAttr(tenant_id_, "DCExpRunWr"));
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedRunWriter::add_item(
    const ObSSDataCheckExpectedItem &item)
{
  int ret = OB_SUCCESS;
  char relative_path_buf[OB_MAX_URI_LENGTH];
  ObString relative_path;
  char *buf = nullptr;
  ObSSDataCheckExpectedSortItem *new_item = nullptr;
  int64_t item_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected run writer not initialized", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected item", KR(ret), K(item));
  } else if (OB_UNLIKELY(item.get_serialize_size()
      + ObExternalSortConstant::BUF_HEADER_LENGTH > file_buf_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("expected item exceeds fragment buffer", KR(ret), K(item), K_(file_buf_size));
  } else if (OB_FAIL(ObSSDataCheckExpectedSortUtil::build_relative_path(
      item.get_macro_id(), relative_path_buf, sizeof(relative_path_buf), relative_path))) {
    LOG_WARN("failed to build expected relative path", KR(ret), K(item));
  } else if (FALSE_IT(item_size = sizeof(ObSSDataCheckExpectedSortItem) + relative_path.length())) {
  } else if (mem_allocator_.used() + item_size > memory_limit_) {
    if (OB_UNLIKELY(0 == item_list_.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected item exceeds empty run memory limit", KR(ret),
          K(item_size), "memory_used", mem_allocator_.used(), K_(memory_limit));
    } else if (OB_FAIL(flush_run_())) {
      LOG_WARN("failed to flush expected run", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(mem_allocator_.alloc(item_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expected item", KR(ret), K(item_size));
  } else if (FALSE_IT(new_item = new (buf) ObSSDataCheckExpectedSortItem())) {
  } else {
    char *path_buf = buf + sizeof(ObSSDataCheckExpectedSortItem);
    MEMCPY(path_buf, relative_path.ptr(), relative_path.length());
    const ObString cached_relative_path(static_cast<int32_t>(relative_path.length()), path_buf);
    if (OB_FAIL(new_item->init(item, cached_relative_path))) {
      LOG_WARN("failed to initialize expected sort item", KR(ret), K(item));
    } else if (OB_FAIL(item_list_.push_back(new_item))) {
      LOG_WARN("failed to append expected item", KR(ret));
    } else {
      new_item = nullptr;
    }
    if (OB_NOT_NULL(new_item)) {
      new_item->~ObSSDataCheckExpectedSortItem();
    }
  }
  return ret;
}

int ObSSDataCheckExpectedRunWriter::flush_run_()
{
  int ret = OB_SUCCESS;
  if (0 == item_list_.size()) {
  } else {
    int compare_ret = OB_SUCCESS;
    ObSSDataCheckExpectedSortItemCompare compare(compare_ret);
    lib::ob_sort(item_list_.begin(), item_list_.end(), compare);
    if (OB_SUCCESS != compare_ret) {
      ret = compare_ret;
      LOG_WARN("failed to sort expected items", KR(ret));
    }

    if (OB_SUCC(ret)) {
      ObFragmentWriterV2<ObSSDataCheckExpectedItem> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
      ObMarkSweepTmpFileHandle file_handle;
      if (OB_FAIL(writer.open(file_buf_size_, io_expire_timestamp_, tenant_id_, dir_id_))) {
        LOG_WARN("failed to open expected run writer", KR(ret));
      } else if (OB_FAIL(tmp_file_mgr_->acquire_file(
          tenant_id_, writer.get_dir_id(), writer.get_fd(), file_handle))) {
        LOG_WARN("failed to acquire expected run file", KR(ret), K(writer.get_fd()));
      } else {
        const ObSSDataCheckExpectedSortItem *previous = nullptr;
        int64_t written_count = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
          const ObSSDataCheckExpectedSortItem *current = item_list_.at(i);
          if (OB_ISNULL(current) || OB_UNLIKELY(!current->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid sorted expected sort item", KR(ret), K(i), KP(current));
          } else if (OB_NOT_NULL(previous) && previous->is_same_key(*current)) {
            if (OB_UNLIKELY(!previous->is_same_object(*current))) {
              ret = OB_CONFLICT_VALUE;
              LOG_WARN("same expected relative path maps to different macro blocks",
                  KR(ret), KPC(previous), KPC(current));
            }
            // 相同路径和 MacroBlockId 是正常的重复记录。
#ifdef ERRSIM
          } else if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_RUN_WRITE_ERROR) OB_SUCCESS)) {
            LOG_WARN("injected expected run write failure", KR(ret), KPC(current));
#endif
          } else if (OB_FAIL(writer.write_item(current->get_item()))) {
            LOG_WARN("failed to write expected run item", KR(ret), KPC(current));
          } else {
            previous = current;
            ++written_count;
          }
        }
        if (OB_SUCC(ret)) {
#ifdef ERRSIM
          if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_RUN_SYNC_ERROR) OB_SUCCESS)) {
            LOG_WARN("injected expected run sync failure", KR(ret));
          } else
#endif
          if (OB_FAIL(writer.sync())) {
            LOG_WARN("failed to sync expected run", KR(ret));
          } else {
            ObSSDataCheckSortedFileInfo run;
            if (OB_FAIL(run.init(file_handle, written_count))) {
              LOG_WARN("failed to publish expected run", KR(ret), K(written_count));
#ifdef ERRSIM
            } else if (OB_FAIL(ret = OB_E(EN_SS_DATA_CHECK_EXPECTED_RUN_PUBLISH_ERROR) OB_SUCCESS)) {
              LOG_WARN("injected expected run publish failure", KR(ret), K(written_count));
#endif
            } else if (OB_FAIL(runs_.push_back(run))) {
              LOG_WARN("failed to save expected run", KR(ret), K(run));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      destruct_vector_list(item_list_);
      item_list_.reset();
      mem_allocator_.reuse();
    }
  }
  return ret;
}

int ObSSDataCheckExpectedRunWriter::finish(
    ObIArray<ObSSDataCheckSortedFileInfo> &runs)
{
  int ret = OB_SUCCESS;
  runs.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected run writer not initialized", KR(ret));
  } else {
    if (item_list_.size() > 0 && OB_FAIL(flush_run_())) {
      LOG_WARN("failed to flush final expected run", KR(ret));
    } else if (OB_FAIL(append(runs, runs_))) {
      LOG_WARN("failed to hand out expected runs", KR(ret), "run_count", runs_.count());
      runs.reset();
    } else {
      runs_.reset();
    }
  }
  return ret;
}

void ObSSDataCheckExpectedRunWriter::reset()
{
  destruct_vector_list(item_list_);
  item_list_.reset();
  mem_allocator_.reuse();
  runs_.reset();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  memory_limit_ = 0;
  file_buf_size_ = 0;
  io_expire_timestamp_ = 0;
  tmp_file_mgr_ = nullptr;
  dir_id_ = -1;
}

// ==================== ObSSDataCheckExpectedMerger ====================

ObSSDataCheckExpectedMerger::ObSSDataCheckExpectedMerger()
  : is_inited_(false),
    is_transitioning_(false),
    tenant_id_(OB_INVALID_ID),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    tmp_file_mgr_(nullptr),
    dir_id_(-1),
    fan_in_(0),
    current_pass_idx_(-1),
    current_input_set_idx_(0),
    inflight_batch_count_(0),
    input_sets_(),
    completed_batches_(),
    input_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK)
{
}

ObSSDataCheckExpectedMerger::~ObSSDataCheckExpectedMerger()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_ERROR_RET(ret, "failed to clean up expected merger",
        K_(current_pass_idx), K_(inflight_batch_count));
  }
}

int ObSSDataCheckExpectedMerger::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    ObMarkSweepTmpFileMgr *tmp_file_mgr)
{
  int ret = OB_SUCCESS;
  // 单个 pread reader 可能持有两个文件缓冲区，并为输出 writer 额外预留一个文件缓冲区。
  // 配置的内存上限按归并批次生效，DAG 级批次并发度单独控制。
  const int64_t max_fan_in_by_memory =
      file_buf_size > 0 ? (memory_limit / file_buf_size - 1) / 2 : 0;
  if (OB_UNLIKELY(is_inited_ || is_transitioning_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected merger initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || max_fan_in_by_memory < 2 || io_expire_timestamp < 0 || OB_ISNULL(tmp_file_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected merger argument", KR(ret), K(tenant_id),
        K(memory_limit), K(file_buf_size), K(max_fan_in_by_memory),
        K(io_expire_timestamp), KP(tmp_file_mgr));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id, dir_id_))) {
    LOG_WARN("failed to allocate expected merge directory", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
    tmp_file_mgr_ = tmp_file_mgr;
    fan_in_ = MIN(MAX_FAN_IN, max_fan_in_by_memory);
    current_pass_idx_ = -1;
    current_input_set_idx_ = 0;
    inflight_batch_count_ = 0;
    input_sets_[0].set_attr(ObMemAttr(tenant_id_, "DCExpMerge"));
    input_sets_[1].set_attr(ObMemAttr(tenant_id_, "DCExpMerge"));
    completed_batches_.set_attr(ObMemAttr(tenant_id_, "DCExpBatch"));
    completed_batches_.reset();
    is_inited_ = true;
  }
  return ret;
}

ObSSDataCheckExpectedMerger::FileArray &ObSSDataCheckExpectedMerger::get_current_inputs_()
{
  return input_sets_[current_input_set_idx_];
}

const ObSSDataCheckExpectedMerger::FileArray &ObSSDataCheckExpectedMerger::get_current_inputs_() const
{
  return input_sets_[current_input_set_idx_];
}

ObSSDataCheckExpectedMerger::FileArray &ObSSDataCheckExpectedMerger::get_next_inputs_()
{
  return input_sets_[1 - current_input_set_idx_];
}

const ObSSDataCheckExpectedMerger::FileArray &ObSSDataCheckExpectedMerger::get_next_inputs_() const
{
  return input_sets_[1 - current_input_set_idx_];
}

int ObSSDataCheckExpectedMerger::add_inputs(
    const ObIArray<ObSSDataCheckSortedFileInfo> &inputs)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merger not initialized", KR(ret));
  } else if (OB_UNLIKELY(is_transitioning_ || -1 != current_pass_idx_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected merger no longer accepts inputs", KR(ret),
        K_(is_transitioning), K_(current_pass_idx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
      if (OB_UNLIKELY(!inputs.at(i).is_valid() || inputs.at(i).get_tenant_id() != tenant_id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expected merger input", KR(ret), K(i), K(inputs.at(i)), K_(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObSSDataCheckSortedFileInfo, 64> &current_inputs = get_current_inputs_();
      const int64_t original_count = current_inputs.count();
      if (OB_FAIL(append(current_inputs, inputs))) {
        LOG_WARN("failed to add expected merge inputs", KR(ret), K(inputs));
        while (current_inputs.count() > original_count) {
          current_inputs.pop_back();
        }
      }
    }
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::start_merge()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merger not initialized", KR(ret));
  } else if (OB_UNLIKELY(is_transitioning_ || -1 != current_pass_idx_
      || !get_next_inputs_().empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected merger cannot start", KR(ret), K_(is_transitioning),
        K_(current_pass_idx), "next_count", get_next_inputs_().count());
  } else {
    int64_t batch_count = 0;
    if (OB_FAIL(get_batch_count_locked_(batch_count))) {
      LOG_WARN("failed to calculate initial expected batch count", KR(ret));
    } else if (OB_FAIL(get_next_inputs_().reserve(batch_count))) {
      LOG_WARN("failed to reserve expected batch outputs", KR(ret), K(batch_count));
    } else if (OB_FAIL(completed_batches_.reserve(batch_count))) {
      LOG_WARN("failed to reserve expected completion bitmap", KR(ret), K(batch_count));
    } else {
      completed_batches_.clear_all();
      current_pass_idx_ = 0;
    }
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::get_current_pass_idx(int64_t &pass_idx) const
{
  int ret = OB_SUCCESS;
  pass_idx = -1;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_transitioning_ || current_pass_idx_ < 0)) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    pass_idx = current_pass_idx_;
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::get_input_count(
    const int64_t pass_idx,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_transitioning_ || pass_idx != current_pass_idx_)) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    count = get_current_inputs_().count();
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::get_batch_count_locked_(int64_t &count) const
{
  const int64_t input_count = get_current_inputs_().count();
  count = input_count > 1 ? (input_count + fan_in_ - 1) / fan_in_ : 0;
  return OB_SUCCESS;
}

int ObSSDataCheckExpectedMerger::get_batch_count(
    const int64_t pass_idx,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_transitioning_ || pass_idx != current_pass_idx_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(get_batch_count_locked_(count))) {
    LOG_WARN("failed to calculate expected batch count", KR(ret), K(pass_idx));
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::get_batch_range_locked_(
    const int64_t batch_idx,
    int64_t &start,
    int64_t &end) const
{
  int ret = OB_SUCCESS;
  int64_t batch_count = 0;
  if (OB_FAIL(get_batch_count_locked_(batch_count))) {
  } else if (OB_UNLIKELY(batch_idx < 0 || batch_idx >= batch_count)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    start = batch_idx * fan_in_;
    end = MIN(start + fan_in_, get_current_inputs_().count());
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::get_batch_range(
    const int64_t pass_idx,
    const int64_t batch_idx,
    int64_t &start,
    int64_t &end) const
{
  int ret = OB_SUCCESS;
  start = 0;
  end = 0;
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_transitioning_ || pass_idx != current_pass_idx_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(get_batch_range_locked_(batch_idx, start, end))) {
    LOG_WARN("invalid expected merge batch", KR(ret), K(pass_idx), K(batch_idx));
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::single_pass_merge_(
    const ObIArray<ObSSDataCheckSortedFileInfo> &inputs,
    const int64_t start,
    const int64_t end,
    ObSSDataCheckSortedFileInfo &output)
{
  int ret = OB_SUCCESS;
  output.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merger not initialized", KR(ret));
  } else if (OB_UNLIKELY(start < 0 || end <= start || end > inputs.count()
      || end - start > MAX_FAN_IN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected merge range", KR(ret), K(start), K(end), "input_count", inputs.count());
  } else if (1 == end - start) {
    if (OB_FAIL(output.assign(inputs.at(start)))) {
      LOG_WARN("failed to forward singleton expected input", KR(ret), K(start));
    }
  } else {
    const ObSSDataCheckExpectedMergeParam param(
        inputs, start, end, tenant_id_, file_buf_size_, io_expire_timestamp_, dir_id_, *tmp_file_mgr_);
    if (OB_FAIL(ObSSDataCheckExpectedSortUtil::merge_expected_files(param, output))) {
      LOG_WARN("failed to merge expected files", KR(ret), K(param));
    }
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::batch_merge(
    const int64_t pass_idx,
    const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  bool batch_started = false;
  ObSEArray<ObSSDataCheckSortedFileInfo, 64> input_snapshot;
  ObSSDataCheckSortedFileInfo merged;
  {
    ObSpinLockGuard guard(input_lock_);
    int64_t start = 0;
    int64_t end = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_transitioning_ || pass_idx != current_pass_idx_)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(get_batch_range_locked_(batch_idx, start, end))) {
      LOG_WARN("invalid expected merge batch", KR(ret), K(pass_idx), K(batch_idx));
    } else if (completed_batches_.has_member(batch_idx)) {
      // A retried DAG task is idempotent once this batch has been published.
    } else {
      input_snapshot.set_attr(ObMemAttr(tenant_id_, "DCExpSnap"));
      const ObSEArray<ObSSDataCheckSortedFileInfo, 64> &current_inputs = get_current_inputs_();
      for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
        if (OB_FAIL(input_snapshot.push_back(current_inputs.at(i)))) {
          LOG_WARN("failed to snapshot expected merge input", KR(ret), K(pass_idx), K(batch_idx), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        ++inflight_batch_count_;
        batch_started = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (batch_started && OB_FAIL(single_pass_merge_(
          input_snapshot, 0, input_snapshot.count(), merged))) {
    LOG_WARN("failed to merge expected batch", KR(ret), K(pass_idx), K(batch_idx));
  }

  if (batch_started) {
    ObSpinLockGuard guard(input_lock_);
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_inited_ || is_transitioning_ || pass_idx != current_pass_idx_)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("expected merge pass changed before batch publication",
            KR(ret), K(pass_idx), K(batch_idx), K_(current_pass_idx));
      } else if (completed_batches_.has_member(batch_idx)) {
        // A concurrent retry won publication. Discard this equivalent output.
      } else if (OB_FAIL(get_next_inputs_().push_back(merged))) {
        LOG_WARN("failed to publish expected batch output", KR(ret), K(pass_idx), K(batch_idx));
      } else if (OB_FAIL(completed_batches_.add_member(batch_idx))) {
        get_next_inputs_().pop_back();
        LOG_WARN("failed to mark expected batch complete", KR(ret), K(pass_idx), K(batch_idx));
      }
    }
  }
  if (batch_started) {
    // cleanup() 使用 inflight_batch_count_ 作为生命周期门禁。计数递减前必须释放
    // 所有本地 Handle，之后不得访问 merger 成员。流水线销毁前，owner 还必须等待
    // 所有 DAG task 结束。
    input_snapshot.reset();
    merged.reset();
    ObSpinLockGuard guard(input_lock_);
    --inflight_batch_count_;
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::prepare_next_pass(const int64_t pass_idx)
{
  int ret = OB_SUCCESS;
  bool need_transition = false;
  int64_t old_input_set_idx = -1;
  {
    ObSpinLockGuard guard(input_lock_);
    int64_t expected_batch_count = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_transitioning_)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (current_pass_idx_ > 0 && pass_idx == current_pass_idx_ - 1) {
      // 本趟屏障已经提交，重试的控制 task 无需处理，
      // 携带旧代次的数据 task 仍会被隔离。
    } else if (OB_UNLIKELY(pass_idx != current_pass_idx_)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(get_batch_count_locked_(expected_batch_count))) {
      LOG_WARN("failed to calculate expected pass batch count", KR(ret), K(pass_idx));
    } else if (OB_UNLIKELY(get_current_inputs_().count() <= 1 || 0 != inflight_batch_count_
        || get_next_inputs_().count() != expected_batch_count
        || completed_batches_.num_members() != expected_batch_count)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected merge pass is incomplete", KR(ret),
          K(pass_idx), "input_count", get_current_inputs_().count(),
          "output_count", get_next_inputs_().count(), K_(inflight_batch_count),
          "completed_count", completed_batches_.num_members(), K(expected_batch_count));
    } else {
      for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < expected_batch_count; ++batch_idx) {
        if (OB_UNLIKELY(!completed_batches_.has_member(batch_idx))) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("expected merge batch is missing", KR(ret), K(pass_idx), K(batch_idx));
        }
      }
      if (OB_SUCC(ret)) {
        is_transitioning_ = true;
        need_transition = true;
        old_input_set_idx = current_input_set_idx_;
        current_input_set_idx_ = 1 - current_input_set_idx_;
        ++current_pass_idx_;
        completed_batches_.clear_all();
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_transition) {
    // 状态切换期间不得启动新批次。reuse 为后续每一趟保留足够容量，
    // 并在锁外释放废弃的文件 Handle。
    input_sets_[old_input_set_idx].reuse();
    ObSpinLockGuard guard(input_lock_);
    is_transitioning_ = false;
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::take_final_input(
    const int64_t pass_idx,
    ObSSDataCheckSortedFileInfo &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  ObSpinLockGuard guard(input_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merger not initialized", KR(ret));
  } else if (OB_UNLIKELY(is_transitioning_
      || pass_idx != current_pass_idx_ || 0 != inflight_batch_count_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected merger cannot publish final input", KR(ret),
        K(pass_idx), K_(current_pass_idx), K_(inflight_batch_count));
  } else {
    const ObSEArray<ObSSDataCheckSortedFileInfo, 64> &current_inputs = get_current_inputs_();
    if (OB_UNLIKELY(!get_next_inputs_().empty() || 0 != completed_batches_.num_members())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected merge has unpublished batch outputs", KR(ret),
          "output_count", get_next_inputs_().count(), "completed_count", completed_batches_.num_members());
    } else if (current_inputs.empty()) {
      // Empty expected set is valid and has no backing file.
    } else if (1 == current_inputs.count()) {
      if (OB_FAIL(result.assign(current_inputs.at(0)))) {
        LOG_WARN("failed to take final expected input", KR(ret));
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected merge still has multiple inputs", KR(ret),
          "input_count", current_inputs.count());
    }
  }
  return ret;
}

int ObSSDataCheckExpectedMerger::cleanup()
{
  int ret = OB_SUCCESS;
  bool need_cleanup = false;
  {
    ObSpinLockGuard guard(input_lock_);
    if (!is_inited_ && !is_transitioning_) {
    } else if (OB_UNLIKELY(is_transitioning_ || 0 != inflight_batch_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("cannot clean expected merger with active transition or batch",
          KR(ret), K_(is_transitioning), K_(inflight_batch_count));
    } else {
      is_inited_ = false;
      is_transitioning_ = true;
      need_cleanup = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_cleanup) {
    input_sets_[0].reset();
    input_sets_[1].reset();
    completed_batches_.reset();
    ObSpinLockGuard guard(input_lock_);
    is_transitioning_ = false;
    tenant_id_ = OB_INVALID_ID;
    memory_limit_ = 0;
    file_buf_size_ = 0;
    io_expire_timestamp_ = 0;
    tmp_file_mgr_ = nullptr;
    dir_id_ = -1;
    fan_in_ = 0;
    current_pass_idx_ = -1;
    current_input_set_idx_ = 0;
    inflight_batch_count_ = 0;
  }
  return ret;
}

// ==================== ObSSDataCheckExpectedSortPipeline ====================

ObSSDataCheckExpectedSortPipeline::ObSSDataCheckExpectedSortPipeline()
  : state_(State::NOT_INIT),
    tenant_id_(OB_INVALID_ID),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    tmp_file_mgr_(nullptr),
    file_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK),
    files_(),
    merger_(),
    sorted_file_(),
    active_sorter_count_(0),
    active_merge_op_count_(0),
    completed_pass_idx_(-1)
{
}

ObSSDataCheckExpectedSortPipeline::~ObSSDataCheckExpectedSortPipeline()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_ERROR_RET(ret, "failed to clean up expected sort pipeline",
        K_(active_sorter_count), K_(active_merge_op_count));
  }
}

int ObSSDataCheckExpectedSortPipeline::init(
    const uint64_t tenant_id,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp,
    ObMarkSweepTmpFileMgr *tmp_file_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::NOT_INIT != state_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected sort pipeline initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || io_expire_timestamp < 0 || OB_ISNULL(tmp_file_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected sort pipeline argument", KR(ret), K(tenant_id),
        K(memory_limit), K(file_buf_size), K(io_expire_timestamp), KP(tmp_file_mgr));
  } else if (OB_FAIL(merger_.init(
      tenant_id, memory_limit, file_buf_size, io_expire_timestamp, tmp_file_mgr))) {
    LOG_WARN("failed to initialize expected merger", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
    tmp_file_mgr_ = tmp_file_mgr;
    files_.set_attr(ObMemAttr(tenant_id_, "DCExpPipe"));
    active_merge_op_count_ = 0;
    completed_pass_idx_ = -1;
    state_ = State::COLLECTING;
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::register_sorter_()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(State::COLLECTING != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected pipeline is not collecting", KR(ret), "state", static_cast<int64_t>(state_));
  } else {
    ++active_sorter_count_;
  }
  return ret;
}

void ObSSDataCheckExpectedSortPipeline::unregister_sorter_()
{
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(active_sorter_count_ <= 0)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected active expected sorter count",
        K_(active_sorter_count));
  } else {
    --active_sorter_count_;
  }
}

int ObSSDataCheckExpectedSortPipeline::register_merge_op_() const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(State::MERGING != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected pipeline is not merging", KR(ret), "state", static_cast<int64_t>(state_));
  } else {
    ++active_merge_op_count_;
  }
  return ret;
}

void ObSSDataCheckExpectedSortPipeline::unregister_merge_op_() const
{
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(active_merge_op_count_ <= 0)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected active expected merge operation count",
        K_(active_merge_op_count));
  } else {
    --active_merge_op_count_;
  }
}

int ObSSDataCheckExpectedSortPipeline::add_files(
    const ObIArray<ObSSDataCheckSortedFileInfo> &files)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(State::COLLECTING != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected pipeline no longer accepts files", KR(ret),
        "state", static_cast<int64_t>(state_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      if (OB_UNLIKELY(!files.at(i).is_valid() || files.at(i).get_tenant_id() != tenant_id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expected sorted file", KR(ret), K(i), K(files.at(i)), K_(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t original_count = files_.count();
      if (OB_FAIL(append(files_, files))) {
        LOG_WARN("failed to add expected sorted files", KR(ret), K(files));
        while (files_.count() > original_count) {
          files_.pop_back();
        }
      }
    }
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::prepare_merge()
{
  int ret = OB_SUCCESS;
  bool need_prepare = false;
  bool entered_preparing = false;
  {
    ObSpinLockGuard guard(file_lock_);
    if (State::MERGING == state_) {
      // Idempotent retry after the initial pass has already been published.
    } else if (OB_UNLIKELY(State::COLLECTING != state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected pipeline cannot prepare merge", KR(ret), "state", static_cast<int64_t>(state_));
    } else if (OB_UNLIKELY(0 != active_sorter_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected sorters are still active", KR(ret), K_(active_sorter_count));
    } else {
      state_ = State::PREPARING;
      need_prepare = true;
      entered_preparing = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_prepare) {
    if (OB_FAIL(merger_.add_inputs(files_))) {
      LOG_WARN("failed to seed expected merger", KR(ret), "file_count", files_.count());
    } else if (OB_FAIL(merger_.start_merge())) {
      LOG_WARN("failed to start expected merger", KR(ret), "file_count", files_.count());
    }
  }

  if (OB_FAIL(ret)) {
    if (entered_preparing) {
      // 允许 prepare_merge() 重试前先重建空 merger，
      // 避免已成功设置的输入被重复追加。
      int restore_ret = merger_.cleanup();
      if (OB_SUCCESS == restore_ret) {
        restore_ret = merger_.init(
            tenant_id_, memory_limit_, file_buf_size_, io_expire_timestamp_, tmp_file_mgr_);
      }
      ObSpinLockGuard guard(file_lock_);
      state_ = OB_SUCCESS == restore_ret ? State::COLLECTING : State::FAILED;
      if (OB_SUCCESS != restore_ret) {
        LOG_ERROR_RET(restore_ret, "failed to restore expected merger after prepare failure");
      }
    }
  } else if (need_prepare) {
    // merger 已持有独立的 Handle 引用。释放提交列表后，
    // 已完成的归并趟可在屏障处回收废弃的输入文件。
    files_.reset();
    sorted_file_.reset();
    ObSpinLockGuard guard(file_lock_);
    state_ = State::MERGING;
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_current_pass_idx(
    int64_t &pass_idx) const
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  pass_idx = -1;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected current pass query", KR(ret));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.get_current_pass_idx(pass_idx))) {
      LOG_WARN("failed to get current expected pass", KR(ret));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_input_count(
    const int64_t pass_idx,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  count = 0;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected input count query", KR(ret), K(pass_idx));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.get_input_count(pass_idx, count))) {
      LOG_WARN("failed to get expected input count", KR(ret), K(pass_idx));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_batch_count(
    const int64_t pass_idx,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  count = 0;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected batch count query", KR(ret), K(pass_idx));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.get_batch_count(pass_idx, count))) {
      LOG_WARN("failed to get expected batch count", KR(ret), K(pass_idx));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_batch_range(
    const int64_t pass_idx,
    const int64_t batch_idx,
    int64_t &start,
    int64_t &end) const
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected batch range query", KR(ret), K(pass_idx), K(batch_idx));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.get_batch_range(pass_idx, batch_idx, start, end))) {
      LOG_WARN("failed to get expected merge batch range", KR(ret), K(pass_idx), K(batch_idx));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::batch_merge(
    const int64_t pass_idx,
    const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected batch merge operation", KR(ret), K(pass_idx), K(batch_idx));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.batch_merge(pass_idx, batch_idx))) {
      LOG_WARN("failed to merge expected batch", KR(ret), K(pass_idx), K(batch_idx));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::prepare_next_pass(
    const int64_t pass_idx)
{
  int ret = OB_SUCCESS;
  bool merge_op_registered = false;
  if (OB_FAIL(register_merge_op_())) {
    LOG_WARN("failed to register expected pass transition operation", KR(ret), K(pass_idx));
  } else {
    merge_op_registered = true;
    if (OB_FAIL(merger_.prepare_next_pass(pass_idx))) {
      LOG_WARN("failed to prepare next expected merge pass", KR(ret), K(pass_idx));
    }
  }
  if (merge_op_registered) {
    unregister_merge_op_();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::complete_merge(
    const int64_t pass_idx)
{
  int ret = OB_SUCCESS;
  bool need_complete = false;
  {
    ObSpinLockGuard guard(file_lock_);
    if (State::COMPLETE == state_) {
      if (OB_UNLIKELY(pass_idx != completed_pass_idx_)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("completed expected merge pass does not match retry",
            KR(ret), K(pass_idx), K_(completed_pass_idx));
      }
    } else if (OB_UNLIKELY(State::MERGING != state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected pipeline cannot complete merge", KR(ret),
          "state", static_cast<int64_t>(state_));
    } else if (OB_UNLIKELY(0 != active_merge_op_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected merge operations are still active", KR(ret), K_(active_merge_op_count));
    } else {
      int64_t current_pass_idx = -1;
      if (OB_FAIL(merger_.get_current_pass_idx(current_pass_idx))) {
        LOG_WARN("failed to validate expected pass before completion", KR(ret), K(pass_idx));
      } else if (OB_UNLIKELY(pass_idx != current_pass_idx)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("expected merge pass does not match completion request",
            KR(ret), K(pass_idx), K(current_pass_idx));
      } else {
        state_ = State::COMPLETING;
        need_complete = true;
      }
    }
  }

  ObSSDataCheckSortedFileInfo result;
  if (OB_FAIL(ret)) {
  } else if (need_complete) {
    if (OB_FAIL(merger_.take_final_input(pass_idx, result))) {
      LOG_WARN("failed to take sorted expected file", KR(ret), K(pass_idx));
    } else {
      sorted_file_.reset();
      if (result.is_valid() && OB_FAIL(sorted_file_.assign(result))) {
        LOG_WARN("failed to save sorted expected file", KR(ret), K(result));
      } else if (OB_FAIL(merger_.cleanup())) {
        LOG_WARN("failed to release completed expected merger", KR(ret), K(pass_idx));
      }
    }
  }
  if (need_complete) {
    ObSpinLockGuard guard(file_lock_);
    if (State::COMPLETING == state_) {
      if (OB_SUCC(ret)) {
        completed_pass_idx_ = pass_idx;
        state_ = State::COMPLETE;
      } else {
        state_ = State::MERGING;
      }
    }
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_sorted_file(
    ObSSDataCheckSortedFileInfo &result) const
{
  int ret = OB_SUCCESS;
  result.reset();
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(State::COMPLETE != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected sort is not complete", KR(ret), "state", static_cast<int64_t>(state_));
  } else if (sorted_file_.is_valid() && OB_FAIL(result.assign(sorted_file_))) {
    LOG_WARN("failed to copy sorted expected file", KR(ret), K_(sorted_file));
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_sorted_item_count(int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSpinLockGuard guard(file_lock_);
  if (OB_UNLIKELY(State::COMPLETE != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected sort is not complete", KR(ret), "state", static_cast<int64_t>(state_));
  } else if (sorted_file_.is_valid()) {
    count = sorted_file_.get_item_count();
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::get_sorted_file_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSSDataCheckSortedFileInfo file_info;
  {
    ObSpinLockGuard guard(file_lock_);
    if (OB_UNLIKELY(State::COMPLETE != state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected sort is not complete", KR(ret), "state", static_cast<int64_t>(state_));
    } else {
      tenant_id = tenant_id_;
      if (sorted_file_.is_valid() && OB_FAIL(file_info.assign(sorted_file_))) {
        LOG_WARN("failed to retain sorted expected file", KR(ret), K_(sorted_file));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (file_info.is_valid() && OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
          tenant_id, file_info.get_fd(), size))) {
    LOG_WARN("failed to get sorted expected file size", KR(ret), K(tenant_id), K(file_info));
  }
  return ret;
}

int ObSSDataCheckExpectedSortPipeline::cleanup()
{
  int ret = OB_SUCCESS;
  State previous_state = State::NOT_INIT;
  bool entered_cleaning = false;
  {
    ObSpinLockGuard guard(file_lock_);
    if (State::NOT_INIT == state_) {
    } else if (OB_UNLIKELY(0 != active_sorter_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("cannot clean expected pipeline with active sorters", KR(ret), K_(active_sorter_count));
    } else if (OB_UNLIKELY(0 != active_merge_op_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("cannot clean expected pipeline with active merge operations",
          KR(ret), K_(active_merge_op_count));
    } else if (OB_UNLIKELY(State::PREPARING == state_ || State::COMPLETING == state_
        || State::CLEANING == state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("expected pipeline is changing state", KR(ret), "state", static_cast<int64_t>(state_));
    } else {
      previous_state = state_;
      state_ = State::CLEANING;
      entered_cleaning = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (entered_cleaning) {
    if (OB_FAIL(merger_.cleanup())) {
      LOG_WARN("failed to clean expected merger", KR(ret));
      ObSpinLockGuard guard(file_lock_);
      state_ = previous_state;
    } else {
      // CLEANING 会拒绝新的 producer/reader，
      // 因此销毁 Handle 时无需持有自旋锁。
      files_.reset();
      sorted_file_.reset();
      ObSpinLockGuard guard(file_lock_);
      state_ = State::NOT_INIT;
      tenant_id_ = OB_INVALID_ID;
      memory_limit_ = 0;
      file_buf_size_ = 0;
      io_expire_timestamp_ = 0;
      tmp_file_mgr_ = nullptr;
      active_sorter_count_ = 0;
      active_merge_op_count_ = 0;
      completed_pass_idx_ = -1;
    }
  }
  return ret;
}

// ==================== ObSSDataCheckExpectedSorter ====================

ObSSDataCheckExpectedSorter::ObSSDataCheckExpectedSorter()
  : is_inited_(false), pipeline_(nullptr), writer_()
{
}

ObSSDataCheckExpectedSorter::~ObSSDataCheckExpectedSorter()
{
  reset();
}

int ObSSDataCheckExpectedSorter::init(
    ObSSDataCheckExpectedSortPipeline *pipeline)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected sorter initialized twice", KR(ret));
  } else if (OB_ISNULL(pipeline)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected pipeline is null", KR(ret));
  } else if (OB_FAIL(pipeline->register_sorter_())) {
    LOG_WARN("failed to register expected sorter", KR(ret));
  } else if (OB_FAIL(writer_.init(pipeline->get_tenant_id(), pipeline->get_memory_limit(),
      pipeline->get_file_buf_size(), pipeline->get_io_expire_timestamp(), pipeline->tmp_file_mgr_))) {
    LOG_WARN("failed to initialize expected run writer", KR(ret));
    pipeline->unregister_sorter_();
  } else {
    pipeline_ = pipeline;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedSorter::add_item(
    const ObSSDataCheckExpectedItem &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected sorter not initialized", KR(ret));
  } else if (OB_FAIL(writer_.add_item(item))) {
    LOG_WARN("failed to add expected item", KR(ret), K(item));
  }
  return ret;
}

int ObSSDataCheckExpectedSorter::finish()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSSDataCheckSortedFileInfo, 16> runs;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected sorter not initialized", KR(ret));
  } else {
    runs.set_attr(ObMemAttr(pipeline_->get_tenant_id(), "DCExpRuns"));
    if (OB_FAIL(writer_.finish(runs))) {
      LOG_WARN("failed to finish expected sorter", KR(ret));
    } else if (OB_FAIL(pipeline_->add_files(runs))) {
      LOG_WARN("failed to submit expected runs", KR(ret), "run_count", runs.count());
    }
    reset();
  }
  return ret;
}

void ObSSDataCheckExpectedSorter::reset()
{
  writer_.reset();
  if (OB_NOT_NULL(pipeline_)) {
    pipeline_->unregister_sorter_();
    pipeline_ = nullptr;
  }
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase

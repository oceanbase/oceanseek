/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/parquet/ob_parquet_page_mgr.h"

#include "lib/container/ob_fixed_array_iterator.h"
#include "lib/container/ob_vector.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/table/ob_external_data_page_cache.h"
#include "sql/engine/table/ob_external_file_access.h"

#include <parquet/properties.h>

namespace oceanbase
{
namespace sql
{

// Preserve OB errors before Arrow converts a nonzero callback result to ParquetException.
int ObParquetPageMgr::WithOffsetIndexCallbacks::operator()(
    const int64_t page_offset,
    std::shared_ptr<arrow::Buffer> &page_buffer,
    bool &has_mem_cached,
    bool &has_decompressed) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(page_mgr_->read_page_with_offset_index_(page_offset,
                                                      page_buffer,
                                                      has_mem_cached,
                                                      has_decompressed))) {
    throw ObErrorCodeException(ret);
  }
  return ret;
}

int ObParquetPageMgr::WithoutOffsetIndexCallbacks::operator()(
    const int64_t offset,
    const int64_t length,
    const int64_t column_end_offset,
    std::shared_ptr<arrow::Buffer> &buffer) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          page_mgr_->read_without_offset_index_range_(offset, length, column_end_offset, buffer))) {
    throw ObErrorCodeException(ret);
  }
  return ret;
}

parquet::PageReadMode ObParquetPageMgr::PageReadModeCallback::operator()() const
{
  parquet::PageReadMode mode = parquet::PageReadMode::DISABLED;
  if (page_mgr_->is_with_offset_index_enabled_()) {
    mode = parquet::PageReadMode::WITH_OFFSET_INDEX;
  } else if (page_mgr_->is_without_offset_index_cache_enabled_()) {
    mode = parquet::PageReadMode::WITHOUT_OFFSET_INDEX;
  }
  return mode;
}

ObParquetPageMgr::ObParquetPageMgr()
    : file_path_(), file_content_digest_(), file_size_(-1), file_modify_time_(-1),
      data_page_cache_(ObExternalDataPageCache::get_instance()), metrics_(),
      row_group_read_mode_(RowGroupReadMode::DISABLED), without_offset_index_windows_()
{
  pages_.set_allocator(&allocator_);
  page_buffers_.set_allocator(&allocator_);
  allocator_.set_label("ParquetPageMgr");
}

ObParquetPageMgr::~ObParquetPageMgr()
{
  int ret = OB_SUCCESS;
  OZ(reset());
}

int ObParquetPageMgr::init(int64_t hole_size_limit,
                           int64_t range_size_limit,
                           bool enable_disk_cache,
                           int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  hole_size_limit_ = hole_size_limit;
  // range_size_limit_ = range_size_limit;
  range_size_limit_ = 2 * 1024 * 1024; // 目前测试下来 2MB 效果最好
  prefetch_size_ = 8 * range_size_limit_;
  without_offset_index_window_size_ = 8 * 1024 * 1024;
  without_offset_index_prefetch_size_ = 16 * 1024 * 1024;
  enable_disk_cache_ = enable_disk_cache;
  timeout_ts_ = timeout_ts;
  return ret;
}

// open new file
int ObParquetPageMgr::open(const ObExternalFileUrlInfo &file_url_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset())) {
    LOG_WARN("failed to reset", K(ret));
  } else {
    // 关闭 ObExternalFileAccess 自己的 mem cache，由我们自己在外面 cache page
    ObExternalFileCacheOptions file_cache_options(false, enable_disk_cache_);
    file_path_ = file_url_info.get_url();
    file_content_digest_ = file_url_info.get_content_digest();
    file_size_ = file_url_info.get_file_size();
    file_modify_time_ = file_url_info.get_modify_time();
    if (OB_FAIL(file_reader_.open(file_url_info, file_cache_options))) {
      LOG_WARN("failed to open parquet file reader", K(ret));
    } else if (OB_FAIL(
                   without_offset_index_windows_.create(16,
                                                        ObMemAttr(MTL_ID(), "ParquetWindows")))) {
      LOG_WARN("failed to create parquet window map", K(ret));
    } else {
      is_opened_ = true;
    }
  }

  return ret;
}

// reset before open new file
int ObParquetPageMgr::reset()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  row_group_read_mode_ = RowGroupReadMode::DISABLED;
  without_offset_index_windows_.destroy();
  file_path_.reset();
  file_content_digest_.reset();
  file_size_ = -1;
  file_modify_time_ = -1;
  if (OB_FAIL(clear_all_pages())) {
    LOG_WARN("failed to clear all pages", K(ret));
  } else if (OB_FAIL(file_reader_.reset())) {
    LOG_WARN("failed to reset file reader", K(ret));
  }
  return ret;
}

// clear_all_pages() before each row group start
int ObParquetPageMgr::clear_all_pages()
{
  int ret = OB_SUCCESS;
  // release each page
  // 循环条件里面不能加 OB_SUCC(ret)，要确保每一个 page 都被正确释放
  for (int64_t i = 0; i < pages_.count(); i++) {
    if (OB_FAIL(release_page_by_idx(i))) {
      LOG_WARN("failed to release page by idx", K(i));
    }
  }
  pages_.reset();
  page_buffers_.reset();
  allocator_.reset();
  return ret;
}

int ObParquetPageMgr::init_pages_with_offset_index(
    const ObIArray<std::pair<int64_t, int64_t>> &selected_pages)
{
  int ret = OB_SUCCESS;
  // 清除上一个 row group hold 的 pages
  if (OB_FAIL(clear_all_pages())) {
    LOG_WARN("failed to clear all pages", K(ret));
  } else if (selected_pages.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, empty pages", K(selected_pages.count()));
  }

  // 按照 Page ReadRange 的 offset 从小到大排序
  ObSortedVector<ObParquetPage> sorted_vector;
  for (int64_t i = 0; OB_SUCC(ret) && i < selected_pages.count(); i++) {
    const std::pair<int64_t, int64_t> &page = selected_pages.at(i);
    if (OB_FAIL(sorted_vector.push_back(
            {page.first, page.second, ObParquetPageStatus::UNLOADED}))) {
      LOG_WARN("failed to push back page range");
    }
  }

  if (OB_SUCC(ret)) {
    sorted_vector.sort(ObParquetPage::compare);
  }

  // 检查 PageRange 是否存在重叠，确保所有的 Page ReadRange 都是合法的。
  if (OB_SUCC(ret)) {
    // check has overlap
    int64_t previous_end = sorted_vector.at(0).end();
    for (int64_t i = 1; OB_SUCC(ret) && i < sorted_vector.count(); i++) {
      ObParquetPage &current_page = sorted_vector.at(i);
      if (current_page.offset_ < previous_end) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("overlap read range", K(current_page), K(previous_end));
      } else {
        previous_end = current_page.end();
      }
    }
  }

  // assign page_ranges_ & page_buffers_ & page_status_
  if (OB_SUCC(ret)) {
    const int64_t total_page_count = sorted_vector.count();
    if (OB_FAIL(pages_.reserve(total_page_count))) {
      LOG_WARN("failed to reserve page ranges", K(total_page_count));
    } else if (OB_FAIL(page_buffers_.reserve(total_page_count))) {
      LOG_WARN("failed to reserve page buffers", K(total_page_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < total_page_count; i++) {
        if (OB_FAIL(pages_.push_back(sorted_vector.at(i)))) {
          LOG_WARN("failed to push back page range");
        } else if (OB_FAIL(page_buffers_.push_back(NULL))) {
          LOG_WARN("failed to push back page buffer");
        }
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::cache_page_with_offset_index_(int64_t page_offset,
                                                    bool is_decompressed,
                                                    const std::shared_ptr<arrow::Buffer> &page_buffer)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to get page idx", K(page_offset), K(page_idx));
  } else if (OB_FAIL(cache_page_by_idx(page_idx, is_decompressed, page_buffer))) {
    LOG_WARN("failed to cache page by idx", K(page_idx));
  }
  return ret;
}

int ObParquetPageMgr::cache_page_by_idx(int64_t page_idx,
                                        bool is_decompressed,
                                        const std::shared_ptr<arrow::Buffer> &page_buffer)
{
  int ret = OB_SUCCESS;
  const ObParquetPage &page = pages_.at(page_idx);
  put_page_to_cache_(
      page.offset_, page_buffer->data_as<char>(), page_buffer->size(), is_decompressed);
  return ret;
}

int ObParquetPageMgr::release_page_with_offset_index_(int64_t page_offset)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset), K(page_idx));
  } else if (OB_FAIL(release_page_by_idx(page_idx))) {
    LOG_WARN("failed to release page idx", K(page_idx));
  }
  return ret;
}

int ObParquetPageMgr::release_page_by_idx(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  // The unified Parquet iterator owns one PageReader per physical column. Once that reader
  // advances, no eager/project peer can consume the previous page again.
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid page idx", K(page_idx));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::RELEASED) {
    // do nothing, already released
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
    // do nothing, page is unloaded
    if (OB_UNLIKELY(NULL != page_buffers_[page_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page is unloaded, but page buffer existed, illegal state", K(page_idx));
    }
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::LOADED) {
    ObParquetPageBufferBase *page_buffer = page_buffers_[page_idx];
    if (OB_ISNULL(page_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page is loaded but page buffer is NULL, illegal state", K(page_idx));
    } else {
      // release buffer(call derived class' de-constructor)
      ObParquetPageBufferSlice *buffer_slice = NULL;
      ObParquetCachedPageBuffer *cached_buffer = NULL;
      if ((buffer_slice = dynamic_cast<ObParquetPageBufferSlice *>(page_buffer)) != nullptr) {
        // 确保 IO 完成后，才能 release 内存
        if (OB_FAIL(buffer_slice->coalesced_buffer_->wait(metrics_))) {
          LOG_WARN("failed to wait", K(page_idx));
        }
        // 即使 wait failed 了，也需要继续释放内存。
        buffer_slice->~ObParquetPageBufferSlice();
        allocator_.free(buffer_slice);
      } else if ((cached_buffer = dynamic_cast<ObParquetCachedPageBuffer *>(page_buffer))
                 != nullptr) {
        cached_buffer->~ObParquetCachedPageBuffer();
        allocator_.free(cached_buffer);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unreachable code");
      }
    }
    pages_[page_idx].status_ = ObParquetPageStatus::RELEASED;
    page_buffers_[page_idx] = NULL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unreachable code");
  }
  return ret;
}

int ObParquetPageMgr::read_page_with_offset_index_(int64_t page_offset,
                                                   std::shared_ptr<arrow::Buffer> &page_buffer,
                                                   bool &has_mem_cached,
                                                   bool &has_decompressed)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::RELEASED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read released page buffer", K(page_offset));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
    // start to fetch
    if (OB_FAIL(try_to_prefetch_by_idx_(page_idx))) {
      LOG_WARN("try to prefetch page failed", K(page_offset), K(page_idx));
    }
  }

  if (OB_SUCC(ret)) {
    ObParquetPageBufferBase *loaded_page_buffer = page_buffers_[page_idx];
    if (OB_UNLIKELY(pages_[page_idx].status_ != ObParquetPageStatus::LOADED)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page not loaded", K(page_offset), K(page_idx));
    } else if (OB_ISNULL(loaded_page_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page not loaded", K(page_offset), K(page_idx));
    } else if (OB_FAIL(loaded_page_buffer->get_arrow_buffer(metrics_,
                                                            page_buffer,
                                                            has_mem_cached,
                                                            has_decompressed))) {
      LOG_WARN("failed to get arrow buffer", K(page_offset), K(page_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_to_prefetch_by_idx_(page_idx + 1))) {
      LOG_WARN("failed to prefetch next page", K(page_idx + 1));
    }
  }

  return ret;
}

// 检查当前 page_offset 是否已经被 selected 了，如果当前 page_offset 经过 page-index 已经被过滤了，
// 直接返回下一个被 selected 的 page start offset
int ObParquetPageMgr::check_page_selected_with_offset_index(
    int64_t page_offset,
    bool &is_selected,
    std::optional<int64_t> &next_page_offset) const
{
  int ret = OB_SUCCESS;
  const ObFixedArray<ObParquetPage, ObIAllocator>::const_iterator iter
      = std::lower_bound(pages_.begin(), pages_.end(), page_offset, ObParquetPageCmp());
  if (iter == pages_.end()) {
    is_selected = false;
    // page 已经在末尾了
    next_page_offset = std::nullopt;
  } else if (page_offset != iter->offset_) {
    // 当前访问的 page 没有被 selected，返回下一个 page 的 offset
    is_selected = false;
    next_page_offset = iter->offset_;
  } else {
    is_selected = true;
    next_page_offset = std::nullopt;
  }
  return ret;
}

int ObParquetPageMgr::register_metrics(ObLakeTableReaderProfile &reader_profile,
                                       const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader_profile.register_metrics(&metrics_, label))) {
    LOG_WARN("failed to register metrics", K(ret));
  }
  return ret;
}

int ObParquetPageMgr::register_io_metrics(ObLakeTableReaderProfile &reader_profile,
                                          const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.register_io_metrics(reader_profile, label))) {
    LOG_WARN("failed to register io metrics", K(ret));
  }
  return ret;
}

int ObParquetPageMgr::find_page_idx_(int64_t offset, int64_t &page_idx) const
{
  int ret = OB_SUCCESS;
  page_idx = -1;
  const ObFixedArray<ObParquetPage, ObIAllocator>::const_iterator iter
      = std::lower_bound(pages_.begin(), pages_.end(), offset, ObParquetPageCmp());
  if (iter == pages_.end()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("page not found", K(offset));
  } else if (iter->offset_ != offset) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("page offset not matched", K(offset), K(iter->offset_));
  } else {
    page_idx = static_cast<int64_t>(std::distance(pages_.begin(), iter));
  }

  if (OB_SUCC(ret)) {
    if (page_idx < 0 || page_idx >= pages_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page index out of range", K(page_idx));
    }
  }
  return ret;
}

int ObParquetPageMgr::prefetch_page_with_offset_index(int64_t page_offset)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset));
  } else if (OB_FAIL(try_to_prefetch_by_idx_(page_idx))) {
    LOG_WARN("failed to prefetch page", K(page_offset), K(page_idx));
  }
  return ret;
}

int ObParquetPageMgr::try_to_prefetch_by_idx_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    // do nothing
  } else {
    // 从当前 page_idx 开始，后面 range_size_limit_ * 2 数据都被 loaded，对标原有的 Prebuffer
    // 实现，预取后面两个 buffer
    int64_t start_offset = pages_[page_idx].offset_;
    int64_t end_offset = start_offset + prefetch_size_;
    for (int64_t i = page_idx; OB_SUCC(ret) && i < pages_.count() && pages_[i].end() <= end_offset;
         ++i) {
      if (pages_[i].status_ == ObParquetPageStatus::UNLOADED) {
        if (OB_FAIL(trigger_prefetch_(i))) {
          LOG_WARN("failed to prefetch page", K(pages_[i]), K(i));
        }
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::trigger_prefetch_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> page_indices_in_window;
  ObArray<int64_t> unloaded_page_indices_in_window;
  ObArray<CoalescedReadRange> coalesced_read_range;
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("page index out of range", K(page_idx), K(pages_.count()));
  } else if (OB_FAIL(get_page_indices_in_window_(page_idx, prefetch_size_, page_indices_in_window))) {
    // 获取当前 page 后面 2 * range_size_limit_ 的 page index
    LOG_WARN("failed to get page indices", K(page_idx));
  } else if (OB_FAIL(try_pin_pages_from_mem_cache_(page_indices_in_window))) {
    // 把后面 2 * range_size_limit_ 的 page pin 住
    LOG_WARN("failed to load pages from mem cache", K(page_idx));
  } else if (OB_FAIL(collect_unloaded_pages_(page_indices_in_window,
                                             unloaded_page_indices_in_window))) {
    // 收集仍然 unloaded 的 page，从远端加载
    LOG_WARN("failed to collect unloaded pages",
             K(page_indices_in_window),
             K(unloaded_page_indices_in_window));
  } else if (OB_FAIL(coalesce_pages_(unloaded_page_indices_in_window,
                                     hole_size_limit_,
                                     range_size_limit_,
                                     coalesced_read_range))) {
    LOG_WARN("failed to coalesce page ranges", K(page_idx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < coalesced_read_range.count(); i++) {
      const CoalescedReadRange &read_range = coalesced_read_range[i];
      // load coalesced_read_ranges
      if (OB_FAIL(load_pages_from_storage_(read_range))) {
        LOG_WARN("failed to load coalesced read ranges");
      }
    }
  }
  return ret;
}

// 获取一个 windows size 下面所有 page 的 index
int ObParquetPageMgr::get_page_indices_in_window_(int64_t start_page_idx,
                                                  int64_t window_size,
                                                  ObIArray<int64_t> &result) const
{
  int ret = OB_SUCCESS;
  int64_t start_offset = pages_[start_page_idx].start();
  bool is_reached_window_size = false;
  if (OB_FAIL(result.push_back(start_page_idx))) {
    LOG_WARN("failed to push back", K(start_page_idx));
  }
  for (int64_t page_idx = start_page_idx + 1;
       OB_SUCC(ret) && page_idx < pages_.count() && !is_reached_window_size;
       page_idx++) {
    const ObParquetPage &current_page = pages_[page_idx];
    if (current_page.end() - start_offset <= window_size) {
      if (OB_FAIL(result.push_back(page_idx))) {
        LOG_WARN("failed to push back", K(page_idx));
      }
    } else {
      is_reached_window_size = true;
    }
  }
  return ret;
}

int ObParquetPageMgr::collect_unloaded_pages_(const ObIArray<int64_t> &page_indices,
                                              ObIArray<int64_t> &unloaded_page_indices) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < page_indices.count(); i++) {
    int64_t page_idx = page_indices.at(i);
    if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
      OZ(unloaded_page_indices.push_back(page_idx));
    }
  }
  return ret;
}

int ObParquetPageMgr::try_pin_pages_from_mem_cache_(const ObIArray<int64_t> &page_indices)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < page_indices.count(); i++) {
    int64_t page_idx = page_indices.at(i);
    if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
      // try to load page from mem cache
      if (OB_FAIL(try_pin_page_from_mem_cache_(page_idx))) {
        LOG_WARN("failed to load page from mem cache", K(page_idx));
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::try_pin_page_from_mem_cache_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  const ObParquetPage &page = pages_.at(page_idx);

  ObExternalDataPageCacheValueHandle value_handle;
  if (OB_FAIL(get_page_from_cache_(page.offset_, value_handle))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      metrics_.cached_page_miss_count_++;
      metrics_.cached_page_miss_size_ += page.length_;
      // cache missed, ignore it
      ret = OB_SUCCESS;
    }
  } else {
    // cache hit
    metrics_.cached_page_hit_count_++;
    metrics_.cached_page_hit_size_ += page.length_;
    std::shared_ptr<arrow::Buffer> buffer;
    ObParquetCachedPageBuffer *cached_page_buffer = NULL;
    if (OB_FAIL(ObParquetCacheBackedArrowBuffer::create(value_handle, buffer))) {
      LOG_WARN("failed to create cached parquet buffer", K(ret));
    } else if (OB_ISNULL(cached_page_buffer = OB_NEWx(ObParquetCachedPageBuffer,
                                                      &allocator_,
                                                      std::move(buffer),
                                                      value_handle.value_->is_decompressed()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory");
    } else {
      page_buffers_[page_idx] = cached_page_buffer;
      pages_[page_idx].status_ = ObParquetPageStatus::LOADED;
    }
  }
  return ret;
}

int ObParquetPageMgr::load_pages_from_storage_(const CoalescedReadRange &coalesced_read_range)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObCoalescedBuffer> coalesced_buffer;
  if (OB_FAIL(submit_read_(coalesced_read_range.offset_,
                           coalesced_read_range.length_,
                           coalesced_buffer))) {
    LOG_WARN("failed to submit coalesced read", K(ret));
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < coalesced_read_range.coalesced_page_indices_.count();
         i++) {
      int64_t page_idx = coalesced_read_range.coalesced_page_indices_.at(i);
      const ObParquetPage &page_read_range = pages_[page_idx];
      ObParquetPageBufferSlice *page_slice = NULL;
      if (OB_UNLIKELY(page_read_range.offset_ < coalesced_buffer->offset_
                      || page_read_range.end() > coalesced_buffer->end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page index out of range", K(page_read_range.offset_), K(page_read_range.length_));
      } else if (OB_ISNULL(page_slice
                           = OB_NEWx(ObParquetPageBufferSlice,
                                     &allocator_,
                                     coalesced_buffer,
                                     page_read_range.offset_ - coalesced_buffer->offset_,
                                     page_read_range.length_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for coalesced page slice");
      } else {
        pages_[page_idx].status_ = ObParquetPageStatus::LOADED;
        page_buffers_[page_idx] = page_slice;
      }
    }
  }

  return ret;
}

int ObParquetPageMgr::coalesce_pages_(const ObIArray<int64_t> &unloaded_page_indices,
                                      const int64_t hold_size_limit,
                                      const int64_t range_size_limit,
                                      ObIArray<CoalescedReadRange> &result) const
{
  int ret = OB_SUCCESS;
  if (unloaded_page_indices.count() > 0) {
    CoalescedReadRange current_range;
    int64_t first_page_idx = unloaded_page_indices.at(0);

    // 初始化第一个合并范围
    current_range.offset_ = pages_[first_page_idx].offset_;
    current_range.length_ = pages_[first_page_idx].length_;
    if (OB_FAIL(current_range.coalesced_page_indices_.push_back(first_page_idx))) {
      LOG_WARN("failed to push back page index", K(first_page_idx));
    }

    // 遍历剩余的未加载页面索引
    for (int64_t i = 1; OB_SUCC(ret) && i < unloaded_page_indices.count(); i++) {
      int64_t page_idx = unloaded_page_indices.at(i);
      const ObParquetPage &page_range = pages_[page_idx];

      int64_t current_range_end = current_range.end();
      int64_t hole_size = page_range.offset_ - current_range_end;
      int64_t new_range_size = page_range.end() - current_range.offset_;

      // 判断是否应该合并当前页面到现有范围
      // 条件：1. 间隙大小不超过限制
      //      2. 合并后的总大小不超过范围大小限制
      if (hole_size <= hold_size_limit && new_range_size <= range_size_limit) {
        // 合并到当前范围
        current_range.length_ = new_range_size;
        if (OB_FAIL(current_range.coalesced_page_indices_.push_back(page_idx))) {
          LOG_WARN("failed to push back page index", K(page_idx));
        }
      } else {
        // 不能合并，保存当前范围并开始新范围
        if (OB_FAIL(result.push_back(current_range))) {
          LOG_WARN("failed to push back coalesced range",
                   K(current_range.offset_),
                   K(current_range.length_));
        } else {
          // 重置并初始化新范围
          current_range.offset_ = page_range.offset_;
          current_range.length_ = page_range.length_;
          current_range.coalesced_page_indices_.reuse();
          if (OB_FAIL(current_range.coalesced_page_indices_.push_back(page_idx))) {
            LOG_WARN("failed to push back page index", K(page_idx));
          }
        }
      }
    }

    // 保存最后一个范围
    if (OB_SUCC(ret) && current_range.coalesced_page_indices_.count() > 0) {
      if (OB_FAIL(result.push_back(current_range))) {
        LOG_WARN("failed to push back last coalesced range",
                 K(current_range.offset_),
                 K(current_range.length_));
      }
    }
  }

  return ret;
}

void ObParquetPageMgr::register_reader_callbacks(parquet::ReaderProperties &read_properties)
{
  const PageReadModeCallback page_read_mode_callback(this);
  read_properties.set_page_read_mode_func(page_read_mode_callback);

  const WithOffsetIndexCallbacks with_offset_index_callbacks(this);
  read_properties.set_with_offset_index_read_page_func(with_offset_index_callbacks);
  read_properties.set_with_offset_index_release_page_func(with_offset_index_callbacks);
  read_properties.set_with_offset_index_cache_page_func(with_offset_index_callbacks);
  read_properties.set_with_offset_index_check_page_selected_func(with_offset_index_callbacks);

  const WithoutOffsetIndexCallbacks without_offset_index_callbacks(this);
  read_properties.set_without_offset_index_read_range_func(without_offset_index_callbacks);
  read_properties.set_without_offset_index_cache_lookup_func(without_offset_index_callbacks);
  read_properties.set_without_offset_index_cache_insert_func(without_offset_index_callbacks);
  read_properties.set_without_offset_index_cache_hit_func(without_offset_index_callbacks);
}

void ObParquetPageMgr::configure_row_group_read_mode(const bool has_complete_offset_index)
{
  if (without_offset_index_windows_.created()) {
    without_offset_index_windows_.clear();
  }
  if (!is_opened_) {
    row_group_read_mode_ = RowGroupReadMode::DISABLED;
  } else if (has_complete_offset_index) {
    row_group_read_mode_ = RowGroupReadMode::WITH_OFFSET_INDEX;
  } else {
    row_group_read_mode_ = RowGroupReadMode::WITHOUT_OFFSET_INDEX;
  }
}

bool ObParquetPageMgr::is_direct_read_enabled() const
{
  return RowGroupReadMode::DISABLED != row_group_read_mode_;
}

bool ObParquetPageMgr::is_with_offset_index_enabled_() const
{
  return RowGroupReadMode::WITH_OFFSET_INDEX == row_group_read_mode_;
}

bool ObParquetPageMgr::is_without_offset_index_cache_enabled_() const
{
  return RowGroupReadMode::WITHOUT_OFFSET_INDEX == row_group_read_mode_;
}

int ObParquetPageMgr::get_page_from_cache_(const int64_t page_offset,
                                           ObExternalDataPageCacheValueHandle &value_handle)
{
  return data_page_cache_.get_page(create_page_cache_key_(page_offset), value_handle);
}

void ObParquetPageMgr::put_page_to_cache_(const int64_t page_offset,
                                          const char *buffer,
                                          const int64_t buffer_size,
                                          const bool is_decompressed)
{
  const ObExternalDataPageCacheValue cache_value(const_cast<char *>(buffer),
                                                 buffer_size,
                                                 is_decompressed);
  data_page_cache_.try_put_page_to_cache(create_page_cache_key_(page_offset), cache_value);
}

ObExternalDataPageCacheKey ObParquetPageMgr::create_page_cache_key_(const int64_t page_offset) const
{
  return ObExternalDataPageCacheKey(file_path_.ptr(),
                                    file_path_.length(),
                                    file_content_digest_.ptr(),
                                    file_content_digest_.length(),
                                    file_modify_time_,
                                    // Key marker for Parquet pages; raw blocks use at least 4 KiB.
                                    1,
                                    page_offset,
                                    MTL_ID());
}

int ObParquetPageMgr::submit_read_(const int64_t offset,
                                   const int64_t length,
                                   std::shared_ptr<ObCoalescedBuffer> &buffer)
{
  int ret = OB_SUCCESS;
  buffer.reset();
  if (OB_UNLIKELY(offset < 0 || length <= 0 || offset >= file_size_
                  || length > file_size_ - offset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parquet read range", K(ret), K(offset), K(length), K_(file_size));
  } else {
    try {
      std::shared_ptr<ObCoalescedBuffer> pending
          = std::make_shared<ObCoalescedBuffer>(file_size_, offset, length);
      if (OB_FAIL(pending->alloc_buffer())) {
        LOG_WARN("failed to allocate read buffer", K(ret));
      } else {
        const int64_t io_timeout_ms = MAX(0, (timeout_ts_ - ObTimeUtility::current_time()) / 1000);
        ObExternalReadInfo read_info(offset, pending->buf_, length, io_timeout_ms);
        if (OB_FAIL(file_reader_.async_read(read_info, pending->io_handle_))) {
          LOG_WARN("failed to submit parquet read", K(ret), K(offset), K(length));
        } else {
          ++metrics_.async_io_count_;
          metrics_.async_io_size_ += length;
          buffer = std::move(pending);
        }
      }
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate parquet read state", K(ret));
    }
  }
  return ret;
}

int ObParquetPageMgr::create_without_offset_index_windows_(const int64_t column_end_offset,
                                                           WithoutOffsetIndexWindows *&windows)
{
  int ret = OB_SUCCESS;
  windows = nullptr;
  const WithoutOffsetIndexWindows empty_windows;
  if (OB_FAIL(without_offset_index_windows_.set_refactored(column_end_offset, empty_windows))) {
    LOG_WARN("failed to insert parquet column windows", K(ret), K(column_end_offset));
  } else if (OB_ISNULL(windows = without_offset_index_windows_.get(column_end_offset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet column windows not found", K(ret), K(column_end_offset));
  } else {
    windows->set_attr(ObMemAttr(MTL_ID(), "ParquetWindows"));
  }
  return ret;
}

int ObParquetPageMgr::prefetch_column_without_offset_index(const int64_t offset,
                                                           const int64_t column_end_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || offset >= column_end_offset || column_end_offset > file_size_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    WithoutOffsetIndexWindows *windows = without_offset_index_windows_.get(column_end_offset);
    if (OB_NOT_NULL(windows) && !windows->empty()) {
      // Two logical columns can refer to the same physical column.
    } else {
      ObExternalDataPageCacheValueHandle cache_handle;
      const int cache_ret = get_page_from_cache_(offset, cache_handle);
      if (OB_ENTRY_NOT_EXIST == cache_ret) {
        if (OB_ISNULL(windows)
            && OB_FAIL(create_without_offset_index_windows_(column_end_offset, windows))) {
          LOG_WARN("failed to create initial parquet window", K(ret));
        } else {
          try {
            std::shared_ptr<ObCoalescedBuffer> window;
            if (OB_FAIL(
                    submit_read_(offset,
                                 MIN(without_offset_index_window_size_, column_end_offset - offset),
                                 window))) {
              LOG_WARN("failed to prefetch parquet column", K(ret), K(offset));
            } else if (OB_FAIL(windows->push_back(WithoutOffsetIndexWindow(window)))) {
              LOG_WARN("failed to retain initial parquet window", K(ret));
            }
          } catch (const std::bad_alloc &) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
      }
      // A cache lookup error only disables speculative IO; normal reads still handle the miss.
    }
  }
  return ret;
}

int ObParquetPageMgr::read_without_offset_index_range_(const int64_t offset,
                                                       const int64_t length,
                                                       const int64_t column_end_offset,
                                                       std::shared_ptr<arrow::Buffer> &buffer)
{
  int ret = OB_SUCCESS;
  buffer.reset();
  WithoutOffsetIndexWindows *column_windows = nullptr;
  if (OB_UNLIKELY(offset < 0 || length <= 0 || column_end_offset > file_size_
                  || offset >= column_end_offset || length > column_end_offset - offset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parquet range", K(ret), K(offset), K(length), K(column_end_offset));
  } else if (OB_ISNULL(column_windows = without_offset_index_windows_.get(column_end_offset))
             && OB_FAIL(create_without_offset_index_windows_(column_end_offset, column_windows))) {
    LOG_WARN("failed to create parquet column windows", K(ret), K(column_end_offset));
  } else {
    try {
      WithoutOffsetIndexWindows &windows = *column_windows;
      // Returned Arrow buffers keep removed windows alive.
      while (OB_SUCC(ret) && !windows.empty() && windows.at(0).buffer_->end() <= offset) {
        if (OB_FAIL(windows.remove(0))) {
          LOG_WARN("failed to remove consumed parquet window", K(ret));
        }
      }
      if (!windows.empty() && windows.at(0).buffer_->offset_ > offset) {
        // Reload if a request moves backwards.
        windows.reuse();
      }
      const int64_t request_end = offset + length;
      const int64_t prefetch_end
          = offset + MIN(MAX(length, without_offset_index_prefetch_size_),
                         column_end_offset - offset);
      int64_t next_offset
          = windows.empty() ? offset : windows.at(windows.count() - 1).buffer_->end();
      // Submit the prefetch windows before waiting.
      while (OB_SUCC(ret) && next_offset < prefetch_end) {
        std::shared_ptr<ObCoalescedBuffer> window;
        if (OB_FAIL(submit_read_(next_offset,
                                 MIN(without_offset_index_window_size_,
                                     column_end_offset - next_offset),
                                 window))) {
          LOG_WARN("failed to read without-OffsetIndex window", K(ret));
        } else if (OB_FAIL(windows.push_back(WithoutOffsetIndexWindow(window)))) {
          LOG_WARN("failed to append parquet window", K(ret));
        } else {
          next_offset = window->end();
        }
      }
      if (OB_SUCC(ret) && windows.at(0).buffer_->end() >= request_end) {
        const std::shared_ptr<ObCoalescedBuffer> &window = windows.at(0).buffer_;
        if (OB_FAIL(ObCoalescedBuffer::slice(window,
                                             offset - window->offset_,
                                             window->end() - offset,
                                             metrics_,
                                             buffer))) {
          LOG_WARN("failed to slice parquet range", K(ret));
        }
      } else if (OB_SUCC(ret)) {
        // Arrow needs a contiguous buffer when a request crosses windows.
        std::shared_ptr<ObCoalescedBuffer> joined
            = std::make_shared<ObCoalescedBuffer>(file_size_, offset, length);
        joined->has_waited_ = true; // This buffer is filled below, never submitted to IO.
        if (OB_FAIL(joined->alloc_buffer())) {
          LOG_WARN("failed to allocate spanning page", K(ret), K(length));
        } else {
          int64_t copied = 0;
          for (int64_t i = 0; i < windows.count(); ++i) {
            const std::shared_ptr<ObCoalescedBuffer> &window = windows.at(i).buffer_;
            if (OB_FAIL(ret) || copied == length) {
              break;
            } else if (OB_FAIL(window->wait(metrics_))) {
              LOG_WARN("failed to wait for spanning page", K(ret));
            } else {
              const int64_t start = offset + copied;
              const int64_t part_size = MIN(length - copied, window->end() - start);
              MEMCPY(joined->buf_ + copied, window->buf_ + start - window->offset_, part_size);
              copied += part_size;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObCoalescedBuffer::slice(joined, 0, length, metrics_, buffer))) {
              LOG_WARN("failed to slice spanning page", K(ret));
            }
          }
        }
      }
    } catch (const std::exception &e) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate parquet range state", K(ret), "Info", e.what());
    }
  }
  return ret;
}

int ObParquetPageMgr::lookup_without_offset_index_cache_(
    const int64_t page_offset,
    std::shared_ptr<arrow::Buffer> &page_buffer,
    bool &cache_hit,
    bool &is_decompressed)
{
  int ret = OB_SUCCESS;
  cache_hit = false;
  page_buffer.reset();
  is_decompressed = false;
  if (OB_UNLIKELY(page_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObExternalDataPageCacheValueHandle value_handle;
    if (OB_FAIL(get_page_from_cache_(page_offset, value_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to look up without-OffsetIndex page cache", K(ret), K(page_offset));
      }
    } else if (OB_FAIL(ObParquetCacheBackedArrowBuffer::create(value_handle, page_buffer))) {
      LOG_WARN("failed to create cached parquet buffer", K(ret), K(page_offset));
    } else {
      cache_hit = true;
      is_decompressed = value_handle.value_->is_decompressed();
    }
  }
  return ret;
}

int ObParquetPageMgr::insert_without_offset_index_cache_(
    const int64_t page_offset,
    const int64_t physical_page_size,
    const bool is_decompressed,
    const std::shared_ptr<arrow::Buffer> &page_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_offset < 0 || physical_page_size <= 0 || OB_ISNULL(page_buffer.get())
                  || page_buffer->size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid without-OffsetIndex parquet page cache insert",
             K(ret),
             K(page_offset),
             K(physical_page_size),
             KP(page_buffer.get()));
  } else {
    put_page_to_cache_(
        page_offset, page_buffer->data_as<char>(), page_buffer->size(), is_decompressed);
    ++metrics_.cached_page_miss_count_;
    metrics_.cached_page_miss_size_ += physical_page_size;
  }
  return ret;
}

void ObParquetPageMgr::notify_without_offset_index_cache_hit_(const int64_t page_offset,
                                                              const int64_t physical_page_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_offset < 0 || physical_page_size <= 0)) {
    LOG_WARN("invalid without-OffsetIndex parquet page cache hit",
             K(page_offset),
             K(physical_page_size));
  } else {
    ++metrics_.cached_page_hit_count_;
    metrics_.cached_page_hit_size_ += physical_page_size;
  }
}

} // namespace sql
} // namespace oceanbase

/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_PARQUET_PAGE_MGR_H
#define OCEANBASE_OB_PARQUET_PAGE_MGR_H

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
#include "sql/engine/table/parquet/ob_parquet_page_buffer.h"

#include <optional>

namespace parquet
{
class ReaderProperties;
enum class PageReadMode;
} // namespace parquet

namespace oceanbase
{
namespace sql
{

class ObExternalFileAccess;
class ObExternalDataPageCache;
class CacheOptions;

enum class ObParquetPageStatus
{
  UNLOADED = 0,
  LOADED,
  RELEASED,
};

struct ObParquetPage
{
public:
  static bool compare(const ObParquetPage &l, const ObParquetPage &r)
  {
    return l.offset_ < r.offset_;
  }

  int64_t start() const
  {
    return offset_;
  }

  int64_t end() const
  {
    return offset_ + length_;
  }

  TO_STRING_KV(K(offset_), K(length_), K(status_))

  int64_t offset_;
  int64_t length_;
  ObParquetPageStatus status_;
};

struct CoalescedReadRange
{
public:
  int64_t offset_;
  int64_t length_;
  int64_t end() const
  {
    return offset_ + length_;
  }
  ObArray<int64_t> coalesced_page_indices_;

  TO_STRING_KV(K(offset_), K(length_));
};

struct ObParquetPageCmp
{
  bool operator()(const ObParquetPage &read_range, const int64_t offset)
  {
    return read_range.offset_ < offset;
  }
};

class ObParquetPageMgr
{
public:
  ObParquetPageMgr();
  ~ObParquetPageMgr();
  int init(int64_t hole_size_limit,
           int64_t range_size_limit,
           bool enable_disk_cache,
           int64_t timeout_ts);
  int open(const ObExternalFileUrlInfo &file_url_info);
  int reset();
  int clear_all_pages();
  int init_pages_with_offset_index(const ObIArray<std::pair<int64_t, int64_t>> &selected_pages);
  int prefetch_page_with_offset_index(int64_t page_offset);
  int check_page_selected_with_offset_index(int64_t page_offset,
                                            bool &is_selected,
                                            std::optional<int64_t> &next_page_offset) const;
  int prefetch_column_without_offset_index(int64_t offset, int64_t column_end_offset);
  int cache_page_by_idx(int64_t page_idx,
                        bool is_decompressed,
                        const std::shared_ptr<arrow::Buffer> &page_buffer);
  int release_page_by_idx(int64_t page_idx);
  void register_reader_callbacks(parquet::ReaderProperties &read_properties);
  void configure_row_group_read_mode(bool has_complete_offset_index);
  bool is_direct_read_enabled() const;
  int register_metrics(ObLakeTableReaderProfile &reader_profile, const ObString &label);
  int register_io_metrics(ObLakeTableReaderProfile &reader_profile, const ObString &label);

private:
  enum class RowGroupReadMode
  {
    DISABLED,
    WITH_OFFSET_INDEX,
    WITHOUT_OFFSET_INDEX,
  };

  struct PageReadModeCallback
  {
    explicit PageReadModeCallback(ObParquetPageMgr *page_mgr) : page_mgr_(page_mgr)
    {
    }

    parquet::PageReadMode operator()() const;

    ObParquetPageMgr *page_mgr_;
  };

  struct WithOffsetIndexCallbacks
  {
    explicit WithOffsetIndexCallbacks(ObParquetPageMgr *page_mgr) : page_mgr_(page_mgr)
    {
    }

    int operator()(const int64_t page_offset,
                   std::shared_ptr<arrow::Buffer> &page_buffer,
                   bool &has_mem_cached,
                   bool &has_decompressed) const
    {
      return page_mgr_->read_page_with_offset_index_(page_offset,
                                                     page_buffer,
                                                     has_mem_cached,
                                                     has_decompressed);
    }

    int operator()(const int64_t page_offset) const
    {
      return page_mgr_->release_page_with_offset_index_(page_offset);
    }

    int operator()(const int64_t page_offset,
                   const bool is_decompressed,
                   const std::shared_ptr<arrow::Buffer> &page_buffer) const
    {
      return page_mgr_->cache_page_with_offset_index_(page_offset, is_decompressed, page_buffer);
    }

    int operator()(const int64_t page_offset,
                   bool &is_page_selected,
                   std::optional<int64_t> &next_page_start_offset) const
    {
      return page_mgr_->check_page_selected_with_offset_index(page_offset,
                                                              is_page_selected,
                                                              next_page_start_offset);
    }

    ObParquetPageMgr *page_mgr_;
  };

  struct WithoutOffsetIndexCallbacks
  {
    explicit WithoutOffsetIndexCallbacks(ObParquetPageMgr *page_mgr) : page_mgr_(page_mgr)
    {
    }

    int operator()(const int64_t offset,
                   const int64_t length,
                   const int64_t column_end_offset,
                   std::shared_ptr<arrow::Buffer> &buffer) const
    {
      return page_mgr_->read_without_offset_index_range_(offset, length, column_end_offset, buffer);
    }

    int operator()(const int64_t page_offset,
                   std::shared_ptr<arrow::Buffer> &page_buffer,
                   bool &cache_hit,
                   bool &is_decompressed) const
    {
      return page_mgr_->lookup_without_offset_index_cache_(page_offset,
                                                           page_buffer,
                                                           cache_hit,
                                                           is_decompressed);
    }

    int operator()(const int64_t page_offset,
                   const int64_t physical_page_size,
                   const bool is_decompressed,
                   const std::shared_ptr<arrow::Buffer> &page_buffer) const
    {
      return page_mgr_->insert_without_offset_index_cache_(page_offset,
                                                           physical_page_size,
                                                           is_decompressed,
                                                           page_buffer);
    }

    void operator()(const int64_t page_offset, const int64_t physical_page_size) const
    {
      page_mgr_->notify_without_offset_index_cache_hit_(page_offset, physical_page_size);
    }

    ObParquetPageMgr *page_mgr_;
  };

  struct WithoutOffsetIndexWindow
  {
    WithoutOffsetIndexWindow() = default;
    explicit WithoutOffsetIndexWindow(const std::shared_ptr<ObCoalescedBuffer> &buffer)
        : buffer_(buffer)
    {
    }
    TO_STRING_KV(KP(buffer_.get()));
    std::shared_ptr<ObCoalescedBuffer> buffer_;
  };
  typedef common::ObSEArray<WithoutOffsetIndexWindow, 4> WithoutOffsetIndexWindows;
  int create_without_offset_index_windows_(int64_t column_end_offset,
                                           WithoutOffsetIndexWindows *&windows);

  bool is_with_offset_index_enabled_() const;
  bool is_without_offset_index_cache_enabled_() const;

  int read_page_with_offset_index_(int64_t page_offset,
                                   std::shared_ptr<arrow::Buffer> &page_buffer,
                                   bool &has_mem_cached,
                                   bool &has_decompressed);
  int release_page_with_offset_index_(int64_t page_offset);
  int cache_page_with_offset_index_(int64_t page_offset,
                                    bool is_decompressed,
                                    const std::shared_ptr<arrow::Buffer> &page_buffer);
  int get_page_from_cache_(int64_t page_offset, ObExternalDataPageCacheValueHandle &value_handle);
  void put_page_to_cache_(int64_t page_offset,
                          const char *buffer,
                          int64_t buffer_size,
                          bool is_decompressed);
  ObExternalDataPageCacheKey create_page_cache_key_(int64_t page_offset) const;

  int submit_read_(int64_t offset, int64_t length, std::shared_ptr<ObCoalescedBuffer> &buffer);

  int read_without_offset_index_range_(int64_t offset,
                                       int64_t length,
                                       int64_t column_end_offset,
                                       std::shared_ptr<arrow::Buffer> &buffer);
  int lookup_without_offset_index_cache_(int64_t page_offset,
                                         std::shared_ptr<arrow::Buffer> &page_buffer,
                                         bool &cache_hit,
                                         bool &is_decompressed);
  int insert_without_offset_index_cache_(int64_t page_offset,
                                         int64_t physical_page_size,
                                         bool is_decompressed,
                                         const std::shared_ptr<arrow::Buffer> &page_buffer);
  void notify_without_offset_index_cache_hit_(int64_t page_offset, int64_t physical_page_size);

  int find_page_idx_(int64_t offset, int64_t &page_idx) const;
  // try to prefetch from page_idx(it may not prefetch happened)
  int try_to_prefetch_by_idx_(int64_t page_idx);
  // trigger prefetch from page_idx
  int trigger_prefetch_(int64_t page_idx);
  int get_page_indices_in_window_(int64_t start_page_idx,
                                  int64_t window_size,
                                  ObIArray<int64_t> &result) const;
  int collect_unloaded_pages_(const ObIArray<int64_t> &page_indices,
                              ObIArray<int64_t> &unloaded_page_indices) const;
  int try_pin_pages_from_mem_cache_(const ObIArray<int64_t> &page_indices);
  int try_pin_page_from_mem_cache_(int64_t page_idx);
  int load_pages_from_storage_(const CoalescedReadRange &coalesced_read_ranges);
  int coalesce_pages_(const ObIArray<int64_t> &unloaded_page_indices,
                      const int64_t hold_size_limit,
                      const int64_t range_size_limit,
                      ObIArray<CoalescedReadRange> &result) const;
  ObFixedArray<ObParquetPage, ObIAllocator> pages_;
  ObFixedArray<ObParquetPageBufferBase *, ObIAllocator> page_buffers_;
  ObArenaAllocator allocator_;

  bool is_opened_ = false;
  ObString file_path_;
  ObString file_content_digest_;
  int64_t file_size_;
  int64_t file_modify_time_;

  // 这些值其实会在 init() 被覆盖
  int64_t hole_size_limit_ = 1 * 1024 * 1024;
  int64_t range_size_limit_ = 2 * 1024 * 1024;
  int64_t prefetch_size_ = 8 * range_size_limit_;
  int64_t without_offset_index_window_size_ = 8 * 1024 * 1024;
  int64_t without_offset_index_prefetch_size_ = 16 * 1024 * 1024;
  bool enable_disk_cache_ = true;
  int64_t timeout_ts_ = INT64_MAX;

  ObExternalFileAccess file_reader_;
  ObExternalDataPageCache &data_page_cache_;
  ObLakeTableParquetPageMgrMetrics metrics_;

  RowGroupReadMode row_group_read_mode_;
  // Created in open(); column windows are added on the first cache miss and cleared per row group.
  // Keyed by the column end offset supplied by Arrow.
  common::hash::ObHashMap<int64_t, WithoutOffsetIndexWindows, common::hash::NoPthreadDefendMode>
      without_offset_index_windows_;
};

} // namespace sql

} // namespace oceanbase

#endif // OCEANBASE_OB_PARQUET_PAGE_MGR_H

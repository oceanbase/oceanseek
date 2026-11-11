/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_PARQUET_PAGE_BUFFER_H
#define OCEANBASE_OB_PARQUET_PAGE_BUFFER_H
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/table/ob_external_data_page_cache.h"
#include "sql/engine/table/ob_external_file_access.h"

#include <arrow/buffer.h>

namespace oceanbase
{
namespace sql
{

class ObCoalescedBuffer
{
public:
  ObCoalescedBuffer(int64_t file_size, int64_t offset, int64_t length);
  ~ObCoalescedBuffer();
  int alloc_buffer();
  int64_t end() const;
  int wait(ObLakeTableParquetPageMgrMetrics &metrics);
  // The returned Arrow slice keeps its allocation alive after PageMgr releases it.
  static int slice(const std::shared_ptr<ObCoalescedBuffer> &owner,
                   int64_t offset,
                   int64_t length,
                   ObLakeTableParquetPageMgrMetrics &metrics,
                   std::shared_ptr<arrow::Buffer> &buffer);
  char *buf_ = NULL;
  int64_t file_size_;
  int64_t offset_;
  int64_t length_;
  bool has_waited_ = false;  // 判断数据是否已经被 wait 过了，即 buf 是否已经被填充完毕
  ObExternalFileReadHandle io_handle_;

private:
  common::ObMalloc alloc_;
};

class ObParquetReadBuffer final : public arrow::Buffer
{
public:
  ObParquetReadBuffer(std::shared_ptr<ObCoalescedBuffer> owner, int64_t offset, int64_t length)
      : arrow::Buffer(reinterpret_cast<const uint8_t *>(owner->buf_) + offset, length),
        owner_(std::move(owner))
  {
  }

private:
  std::shared_ptr<ObCoalescedBuffer> owner_;
};

// Keep cached data pinned for as long as Arrow holds the returned buffer.
class ObParquetCacheBackedArrowBuffer final : public arrow::Buffer
{
public:
  ObParquetCacheBackedArrowBuffer(const uint8_t *data, const int64_t size)
      : arrow::Buffer(data, size), cache_handle_()
  {
  }

  static int create(const ObExternalDataPageCacheValueHandle &cache_handle,
                    std::shared_ptr<arrow::Buffer> &buffer);

private:
  ObExternalDataPageCacheValueHandle cache_handle_;
};

class ObParquetPageBufferBase
{
public:
  ObParquetPageBufferBase() = default;
  virtual ~ObParquetPageBufferBase() = default;
  virtual int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                               std::shared_ptr<arrow::Buffer> &buffer,
                               bool &is_cache_hit,
                               bool &is_decompressed) = 0;
  TO_STRING_EMPTY();
};

class ObParquetCachedPageBuffer final : public ObParquetPageBufferBase
{
public:
  ObParquetCachedPageBuffer(std::shared_ptr<arrow::Buffer> buffer, bool is_decompressed);
  virtual ~ObParquetCachedPageBuffer() = default;
  int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                       std::shared_ptr<arrow::Buffer> &buffer,
                       bool &is_cache_hit,
                       bool &is_decompressed);

private:
  std::shared_ptr<arrow::Buffer> buffer_;
  bool is_decompressed_;
};

class ObParquetPageBufferSlice final : public ObParquetPageBufferBase
{
public:
  ObParquetPageBufferSlice(std::shared_ptr<ObCoalescedBuffer> coalesced_buffer,
                           int64_t offset,
                           int64_t length);
  virtual ~ObParquetPageBufferSlice() = default;
  int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                       std::shared_ptr<arrow::Buffer> &buffer,
                       bool &is_cache_hit,
                       bool &is_decompressed);

  std::shared_ptr<ObCoalescedBuffer> coalesced_buffer_;
  int64_t offset_; // offset_ in coalesced_buffer_
  int64_t length_; // length_ in coalesced_buffer_
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_OB_PARQUET_PAGE_BUFFER_H

/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/parquet/ob_parquet_page_buffer.h"

#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{

ObCoalescedBuffer::ObCoalescedBuffer(int64_t file_size, int64_t offset, int64_t length)
    : file_size_(file_size), offset_(offset), length_(length),
      alloc_(common::ObMemAttr(MTL_ID(), "CoalescedBuffer"))
{
  // int64_t end = offset_ + length_;
  // offset_ = (offset_ / DEFAULT_MACRO_BLOCK_SIZE) * DEFAULT_MACRO_BLOCK_SIZE;
  // // 末尾向上取整，且不超过文件的 file_size
  // end = std::min((end + DEFAULT_MACRO_BLOCK_SIZE - 1) / DEFAULT_MACRO_BLOCK_SIZE * DEFAULT_MACRO_BLOCK_SIZE, file_size_);
  // length_ = end - offset_;
}

ObCoalescedBuffer::~ObCoalescedBuffer()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf_)) {
    if (!has_waited_) {
      if (OB_FAIL(io_handle_.wait())) {
        LOG_WARN("io_handle wait failed", K(ret));
      }
    }
    alloc_.free(buf_);
    buf_ = NULL;
  }
}

int ObCoalescedBuffer::alloc_buffer()
{
  int ret = OB_SUCCESS;
  if (NULL != buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer already allocated", K(ret), K(buf_), K(length_));
  } else if (OB_ISNULL(buf_ = (char *)alloc_.alloc(length_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("buffer allocate failed", K(ret));
  }
  return ret;
}

int ObCoalescedBuffer::wait(ObLakeTableParquetPageMgrMetrics &metrics)
{
  int ret = OB_SUCCESS;
  if (has_waited_) {
    // do nothing
  } else {
    int64_t start_ns = ObTimeUtility::current_time_ns();
    if (OB_FAIL(io_handle_.wait())) {
      LOG_WARN("io_handle wait failed", K(ret));
    } else {
      has_waited_ = true;
      int64_t cost_ns = ObTimeUtility::current_time_ns() - start_ns;
      metrics.total_io_wait_time_ns_ += cost_ns;
      metrics.max_io_wait_time_ns_ = MAX(metrics.max_io_wait_time_ns_, cost_ns);
    }
  }
  return ret;
}

int ObCoalescedBuffer::slice(const std::shared_ptr<ObCoalescedBuffer> &owner,
                             const int64_t offset,
                             const int64_t length,
                             ObLakeTableParquetPageMgrMetrics &metrics,
                             std::shared_ptr<arrow::Buffer> &buffer)
{
  int ret = OB_SUCCESS;
  buffer.reset();
  if (OB_UNLIKELY(!owner || nullptr == owner->buf_ || offset < 0 || length <= 0
                  || offset >= owner->length_ || length > owner->length_ - offset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parquet buffer slice", K(ret), K(offset), K(length));
  } else if (OB_FAIL(owner->wait(metrics))) {
    LOG_WARN("failed to wait for parquet buffer", K(ret));
  } else {
    try {
      buffer = std::make_shared<ObParquetReadBuffer>(owner, offset, length);
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate parquet slice", K(ret));
    }
  }
  return ret;
}

int64_t ObCoalescedBuffer::end() const
{
  return offset_ + length_;
}

int ObParquetCacheBackedArrowBuffer::create(const ObExternalDataPageCacheValueHandle &cache_handle,
                                            std::shared_ptr<arrow::Buffer> &buffer)
{
  int ret = OB_SUCCESS;
  buffer.reset();
  ObExternalDataPageCacheValue *value = cache_handle.value_;
  if (OB_ISNULL(value) || OB_UNLIKELY(!value->is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid parquet page cache value", K(ret));
  } else {
    try {
      std::shared_ptr<ObParquetCacheBackedArrowBuffer> cached_buffer
          = std::make_shared<ObParquetCacheBackedArrowBuffer>(
              reinterpret_cast<const uint8_t *>(value->get_buffer()),
              value->get_valid_data_size());
      if (OB_FAIL(cached_buffer->cache_handle_.assign(cache_handle))) {
        LOG_WARN("failed to pin parquet page cache value", K(ret));
      } else {
        buffer = std::move(cached_buffer);
      }
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate cached parquet buffer", K(ret));
    }
  }
  return ret;
}

ObParquetCachedPageBuffer::ObParquetCachedPageBuffer(std::shared_ptr<arrow::Buffer> buffer,
                                                     bool is_decompressed)
    : buffer_(std::move(buffer)), is_decompressed_(is_decompressed)
{
}

int ObParquetCachedPageBuffer::get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                                                std::shared_ptr<arrow::Buffer> &buffer,
                                                bool &is_cache_hit,
                                                bool &is_decompressed)
{
  UNUSED(metrics);
  int ret = OB_SUCCESS;
  buffer.reset();
  is_cache_hit = false;
  is_decompressed = false;
  if (OB_ISNULL(buffer_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached parquet buffer is null", K(ret));
  } else {
    buffer = buffer_;
    is_cache_hit = true;
    is_decompressed = is_decompressed_;
  }
  return ret;
}

ObParquetPageBufferSlice::ObParquetPageBufferSlice(
    std::shared_ptr<ObCoalescedBuffer> coalesced_buffer,
    int64_t offset,
    int64_t length)
    : coalesced_buffer_(std::move(coalesced_buffer)), offset_(offset), length_(length)
{
}

int ObParquetPageBufferSlice::get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
  std::shared_ptr<arrow::Buffer> &buffer, bool &is_cache_hit, bool &is_decompressed)
{
  is_cache_hit = false;
  is_decompressed = false;
  return ObCoalescedBuffer::slice(coalesced_buffer_, offset_, length_, metrics, buffer);
}

} // namespace sql
} // namespace oceanbase

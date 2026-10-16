/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <new>
#include <string>
#include <vector>
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{

static const uint64_t TEST_DIRECT_LOAD_STREAM_TENANT_ID = 1002;

class ObDirectLoadStreamMockQueueStore final : public ObIDirectLoadStreamQueueStore
{
public:
  ObDirectLoadStreamMockQueueStore()
    : bytes_(),
      next_offset_(0),
      append_ret_(common::OB_SUCCESS),
      read_ret_(common::OB_SUCCESS),
      remove_ret_(common::OB_SUCCESS),
      remove_call_count_(0),
      short_read_(false),
      block_append_(false),
      append_entered_(false),
      allow_append_(false),
      is_inited_(false)
  {
  }
  void set_append_ret(const int ret) { append_ret_ = ret; }
  void set_read_ret(const int ret) { read_ret_ = ret; }
  void set_remove_ret(const int ret) { remove_ret_ = ret; }
  void set_short_read(const bool short_read) { short_read_ = short_read; }
  int64_t get_remove_call_count() const { return remove_call_count_; }
  void block_append()
  {
    std::lock_guard<std::mutex> guard(append_mutex_);
    block_append_ = true;
    append_entered_ = false;
    allow_append_ = false;
  }
  bool wait_until_append_entered(const std::chrono::milliseconds timeout)
  {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> guard(append_mutex_);
    while (!append_entered_
           && std::cv_status::timeout != append_cond_.wait_until(guard, deadline)) {
    }
    return append_entered_;
  }
  void unblock_append()
  {
    {
      std::lock_guard<std::mutex> guard(append_mutex_);
      allow_append_ = true;
      block_append_ = false;
    }
    append_cond_.notify_all();
  }
  int init(const uint64_t tenant_id) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else if (OB_UNLIKELY(!common::is_user_tenant(tenant_id))) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      bytes_.clear();
      next_offset_ = 0;
      is_inited_ = true;
    }
    return ret;
  }
  int append(const common::ObString &chunk, int64_t &offset) override
  {
    int ret = common::OB_SUCCESS;
    offset = 0;
    const int64_t size = chunk.length();
    {
      std::unique_lock<std::mutex> guard(append_mutex_);
      if (block_append_) {
        append_entered_ = true;
        append_cond_.notify_all();
        while (!allow_append_) {
          append_cond_.wait(guard);
        }
      }
    }
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_UNLIKELY(common::OB_SUCCESS != append_ret_)) {
      ret = append_ret_;
    } else if (OB_UNLIKELY(nullptr == chunk.ptr() || size <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(size > INT64_MAX - next_offset_)) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      offset = next_offset_;
      bytes_.append(chunk.ptr(), static_cast<size_t>(size));
      next_offset_ += size;
    }
    return ret;
  }
  int read(const int64_t offset, const int64_t size,
           common::ObIAllocator &allocator, common::ObString &chunk) override
  {
    int ret = common::OB_SUCCESS;
    char *buf = nullptr;
    chunk.reset();
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_UNLIKELY(common::OB_SUCCESS != read_ret_)) {
      ret = read_ret_;
    } else if (OB_UNLIKELY(offset < 0 || size <= 0 || size > INT32_MAX
                           || offset > next_offset_ || size > next_offset_ - offset)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      const int64_t read_size = short_read_ ? size - 1 : size;
      std::memcpy(buf, bytes_.data() + offset, static_cast<size_t>(read_size));
      chunk.assign_ptr(buf, static_cast<int32_t>(read_size));
    }
    return ret;
  }
  int remove() override
  {
    int ret = remove_ret_;
    ++remove_call_count_;
    if (common::OB_SUCCESS == ret) {
      bytes_.clear();
      next_offset_ = 0;
      is_inited_ = false;
    }
    return ret;
  }
private:
  std::string bytes_;
  int64_t next_offset_;
  int append_ret_;
  int read_ret_;
  int remove_ret_;
  int64_t remove_call_count_;
  bool short_read_;
  std::mutex append_mutex_;
  std::condition_variable append_cond_;
  bool block_append_;
  bool append_entered_;
  bool allow_append_;
  bool is_inited_;
};

class ObDirectLoadStreamMockQueueStoreFactory final
    : public ObIDirectLoadStreamQueueStoreFactory
{
public:
  ObDirectLoadStreamMockQueueStoreFactory() = default;
  ~ObDirectLoadStreamMockQueueStoreFactory() override
  {
    for (ObDirectLoadStreamMockQueueStore *store : stores_) {
      delete store;
    }
    stores_.clear();
  }
  int create_store(ObIDirectLoadStreamQueueStore *&store) override
  {
    int ret = common::OB_SUCCESS;
    store = nullptr;
    ObDirectLoadStreamMockQueueStore *mock_store =
        new (std::nothrow) ObDirectLoadStreamMockQueueStore();
    if (OB_ISNULL(mock_store)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      stores_.emplace_back(mock_store);
      store = mock_store;
    }
    return ret;
  }
  ObDirectLoadStreamMockQueueStore *get_last_store() const
  {
    return stores_.empty() ? nullptr : stores_.back();
  }
private:
  std::vector<ObDirectLoadStreamMockQueueStore *> stores_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamMockQueueStoreFactory);
};

inline ObDirectLoadStreamMockQueueStoreFactory &get_direct_load_stream_mock_store_factory()
{
  static ObDirectLoadStreamMockQueueStoreFactory factory;
  return factory;
}

class ObDirectLoadStreamTestTenantEnv
{
public:
  ObDirectLoadStreamTestTenantEnv()
    : tenant_base_(TEST_DIRECT_LOAD_STREAM_TENANT_ID),
      old_tenant_(nullptr),
      created_allocator_(false),
      is_inited_(false)
  {
  }
  ~ObDirectLoadStreamTestTenantEnv()
  {
    destroy();
  }
  int init()
  {
    int ret = common::OB_SUCCESS;
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    bool allocator_existed = false;
    int alloc_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else if (OB_ISNULL(allocator)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      // Stream 组件按业务租户分配内存，单测需先准备 tenant allocator。
      // recycle 后可能残留 unrecycled allocator，只有 active ctx allocator 可用才跳过 create。
      allocator_existed = OB_NOT_NULL(allocator->get_tenant_ctx_allocator(
          TEST_DIRECT_LOAD_STREAM_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID));
      if (!allocator_existed) {
        alloc_ret = allocator->create_and_add_tenant_allocator(TEST_DIRECT_LOAD_STREAM_TENANT_ID);
        if (common::OB_SUCCESS == alloc_ret) {
          created_allocator_ = true;
        }
      }
      if (OB_UNLIKELY(common::OB_SUCCESS != alloc_ret
          && common::OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(allocator->set_tenant_limit(TEST_DIRECT_LOAD_STREAM_TENANT_ID,
                           1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret),
                 K(TEST_DIRECT_LOAD_STREAM_TENANT_ID));
      } else if (OB_ISNULL(allocator->get_tenant_ctx_allocator(
                           TEST_DIRECT_LOAD_STREAM_TENANT_ID,
                           common::ObCtxIds::DEFAULT_CTX_ID))) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        old_tenant_ = share::ObTenantEnv::get_tenant();
        share::ObTenantEnv::set_tenant(&tenant_base_);
        is_inited_ = true;
      }
      if (OB_FAIL(ret) && created_allocator_) {
        lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(
            TEST_DIRECT_LOAD_STREAM_TENANT_ID);
        created_allocator_ = false;
      }
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      share::ObTenantEnv::set_tenant(old_tenant_);
      old_tenant_ = nullptr;
      is_inited_ = false;
    }
    if (created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(
          TEST_DIRECT_LOAD_STREAM_TENANT_ID);
      created_allocator_ = false;
    }
  }

private:
  share::ObTenantBase tenant_base_;
  share::ObTenantBase *old_tenant_;
  bool created_allocator_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <gtest/gtest.h>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>
#define USING_LOG_PREFIX SERVER
#include "test_direct_load_stream_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/worker.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"

namespace oceanbase
{
namespace observer
{

namespace
{
const uint64_t TEST_TENANT_ID = 1002;
const char *TEST_CHUNK = "encoded-chunk-bytes";
const char *SECOND_CHUNK = "second";
const int64_t TEST_CHUNK_SIZE = 19;
const int64_t SECOND_CHUNK_SIZE = 6;
const std::chrono::milliseconds SHORT_WAIT(100);
const std::chrono::seconds LONG_WAIT(5);

struct ReadResult
{
  ReadResult() : ret_(OB_SUCCESS), chunk_() {}
  int ret_;
  std::string chunk_;
};

struct AppendWriterContext
{
  ObDirectLoadStreamBufferQueue *queue_;
  int64_t chunk_count_;
  const char *payload_prefix_;
  const std::string *payload_padding_;
  std::atomic<bool> *start_;
  std::atomic<int64_t> *registered_count_;
  std::atomic<int> *first_error_;
  std::atomic<int64_t> *success_count_;
  std::mutex *expected_mutex_;
  std::set<std::string> *expected_payloads_;
};

class CancelCloseWorker : public lib::Worker
{
public:
  int check_status() override
  {
    return OB_CANCELED;
  }
};

class CancelAfterWaitWorker : public lib::Worker
{
public:
  explicit CancelAfterWaitWorker(std::atomic<int64_t> &check_count)
    : check_count_(check_count)
  {
  }

  int check_status() override
  {
    const int64_t check_count = ++check_count_;
    return check_count > 1 ? OB_CANCELED : OB_SUCCESS;
  }

private:
  std::atomic<int64_t> &check_count_;
};

class FailAllocator : public common::ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    UNUSED(size);
    return nullptr;
  }

  void free(void *ptr) override
  {
    UNUSED(ptr);
  }

  void *alloc(const int64_t size, const common::ObMemAttr &attr)
  {
    UNUSED(size);
    UNUSED(attr);
    return nullptr;
  }
};

common::ObString make_chunk(const char *buf, const int64_t size)
{
  common::ObString chunk;
  if (nullptr != buf && size > 0 && size <= INT32_MAX) {
    chunk.assign_ptr(buf, static_cast<int32_t>(size));
  }
  return chunk;
}

int init_queue(ObDirectLoadStreamBufferQueue &queue, const int64_t stream_id = 1)
{
  ObIDirectLoadStreamQueueStore *store = nullptr;
  int ret = get_direct_load_stream_mock_store_factory().create_store(store);
  if (OB_SUCC(ret) && OB_ISNULL(store)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCC(ret)) {
    ret = queue.init_with_mock_store_for_test(
        TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey(stream_id, 0), *store);
  }
  return ret;
}

int init_manager(ObDirectLoadStreamBufferQueueManager &manager)
{
  int ret = manager.set_mock_queue_store_factory_for_test(
      get_direct_load_stream_mock_store_factory());
  if (OB_SUCC(ret)) {
    ret = manager.init();
  }
  return ret;
}

int append_statement_chunk(ObDirectLoadStreamBufferQueue &queue,
                           const common::ObString &chunk)
{
  ObDirectLoadStreamAppendHandle handle;
  int ret = queue.begin_append(handle);
  if (OB_SUCCESS == ret) {
    const int append_ret = queue.append_chunk(handle, chunk);
    const int end_ret = queue.end_append(handle);
    ret = OB_SUCCESS != append_ret ? append_ret : end_ret;
  }
  return ret;
}

ReadResult read_one_chunk(ObDirectLoadStreamBufferQueue *queue)
{
  ReadResult result;
  common::ObArenaAllocator allocator("DLSBQTest");
  allocator.set_tenant_id(TEST_TENANT_ID);
  common::ObString chunk;
  if (nullptr == queue) {
    result.ret_ = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS == (result.ret_ = queue->get_next_chunk(allocator, chunk))) {
    result.chunk_.assign(chunk.ptr(), chunk.length());
  }
  return result;
}

ReadResult read_one_chunk_with_delayed_cancel(ObDirectLoadStreamBufferQueue *queue,
                                              std::atomic<int64_t> *check_count)
{
  ReadResult result;
  if (nullptr == queue || nullptr == check_count) {
    result.ret_ = OB_INVALID_ARGUMENT;
  } else {
    CancelAfterWaitWorker worker(*check_count);
    lib::Worker *previous_worker = lib::Worker::self_;
    lib::Worker::set_worker_to_thread_local(&worker);
    result = read_one_chunk(queue);
    lib::Worker::set_worker_to_thread_local(previous_worker);
  }
  return result;
}

ReadResult read_one_chunk_with_handle(ObDirectLoadStreamBufferQueueHandle *handle)
{
  ReadResult result;
  if (nullptr != handle && handle->is_valid()) {
    result = read_one_chunk(handle->get_queue());
  } else {
    result.ret_ = OB_INVALID_ARGUMENT;
  }
  if (nullptr != handle) {
    handle->reset();
  }
  return result;
}

int close_queue(ObDirectLoadStreamBufferQueue *queue)
{
  return nullptr == queue ? OB_INVALID_ARGUMENT : queue->request_close();
}

int close_queue_with_cancel(ObDirectLoadStreamBufferQueue *queue)
{
  int ret = OB_INVALID_ARGUMENT;
  if (nullptr != queue) {
    CancelCloseWorker worker;
    lib::Worker *previous_worker = lib::Worker::self_;
    lib::Worker::set_worker_to_thread_local(&worker);
    ret = queue->request_close();
    lib::Worker::set_worker_to_thread_local(previous_worker);
  }
  return ret;
}

int cleanup_queue(ObDirectLoadStreamBufferQueue *queue)
{
  return nullptr == queue ? OB_INVALID_ARGUMENT : queue->cleanup();
}

int cleanup_manager_queue(ObDirectLoadStreamBufferQueueManager *manager,
                          const ObDirectLoadStreamBufferQueueKey key)
{
  return nullptr == manager ? OB_INVALID_ARGUMENT : manager->cleanup_queue(key);
}

int cleanup_manager_queue_with_delayed_cancel(
    ObDirectLoadStreamBufferQueueManager *manager,
    const ObDirectLoadStreamBufferQueueKey key, std::atomic<int64_t> *check_count)
{
  int ret = OB_INVALID_ARGUMENT;
  if (nullptr != manager && nullptr != check_count) {
    CancelAfterWaitWorker worker(*check_count);
    lib::Worker *previous_worker = lib::Worker::self_;
    lib::Worker::set_worker_to_thread_local(&worker);
    ret = manager->cleanup_queue(key);
    lib::Worker::set_worker_to_thread_local(previous_worker);
  }
  return ret;
}

bool string_equals(const common::ObString &str, const char *buf, const int64_t size)
{
  return nullptr != str.ptr() && str.length() == size && 0 == memcmp(str.ptr(), buf, size);
}

std::string to_std_string(const common::ObString &str)
{
  return std::string(str.ptr(), str.length());
}

void record_first_error(std::atomic<int> &first_error, const int error_code)
{
  int expected = OB_SUCCESS;
  (void)first_error.compare_exchange_strong(expected, error_code);
}

bool wait_until_count(std::atomic<int64_t> &value, const int64_t expected,
                      const std::chrono::milliseconds timeout)
{
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
  while (value.load() != expected && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return value.load() == expected;
}

bool wait_until_state(ObDirectLoadStreamBufferQueue &queue,
                      const ObDirectLoadStreamBufferQueueState expected,
                      const std::chrono::milliseconds timeout)
{
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
  while (queue.get_state() != expected && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return queue.get_state() == expected;
}

void append_writer(AppendWriterContext *ctx, const int64_t writer_idx)
{
  if (nullptr == ctx || nullptr == ctx->queue_ || nullptr == ctx->first_error_
      || nullptr == ctx->expected_mutex_ || nullptr == ctx->expected_payloads_) {
    return;
  }
  ObDirectLoadStreamAppendHandle handle;
  int writer_ret = ctx->queue_->begin_append(handle);
  if (OB_SUCCESS != writer_ret) {
    record_first_error(*ctx->first_error_, writer_ret);
  } else {
    if (nullptr != ctx->registered_count_) {
      ++(*ctx->registered_count_);
    }
    while (nullptr != ctx->start_ && !ctx->start_->load()) {
      std::this_thread::yield();
    }
    for (int64_t chunk_idx = 0;
         OB_SUCCESS == writer_ret && chunk_idx < ctx->chunk_count_; ++chunk_idx) {
      std::string payload = ctx->payload_prefix_ + std::to_string(writer_idx)
          + "-chunk-" + std::to_string(chunk_idx);
      if (nullptr != ctx->payload_padding_) {
        payload += *ctx->payload_padding_;
      }
      const common::ObString chunk(
          static_cast<int32_t>(payload.length()), payload.data());
      writer_ret = ctx->queue_->append_chunk(handle, chunk);
      if (OB_SUCCESS == writer_ret) {
        std::lock_guard<std::mutex> guard(*ctx->expected_mutex_);
        if (!ctx->expected_payloads_->insert(payload).second) {
          writer_ret = OB_ERR_UNEXPECTED;
        } else if (nullptr != ctx->success_count_) {
          ++(*ctx->success_count_);
        }
      }
    }
    const int end_ret = ctx->queue_->end_append(handle);
    if (OB_SUCCESS == writer_ret) {
      writer_ret = end_ret;
    }
    if (OB_SUCCESS != writer_ret) {
      record_first_error(*ctx->first_error_, writer_ret);
    }
  }
}

void join_writers(std::vector<std::thread> &writers)
{
  for (std::thread &writer : writers) {
    if (writer.joinable()) {
      writer.join();
    }
  }
}

int drain_queue(ObDirectLoadStreamBufferQueue &queue, std::set<std::string> &payloads)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("DLSBQTest");
  allocator.set_tenant_id(TEST_TENANT_ID);
  common::ObString chunk;
  while (OB_SUCCESS == (ret = queue.get_next_chunk(allocator, chunk))) {
    if (!payloads.insert(to_std_string(chunk)).second) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

}  // namespace

int init_test_tenant_allocator()
{
  lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCCESS != (ret = allocator->create_and_add_tenant_allocator(TEST_TENANT_ID))) {
    OB_LOG(WARN, "fail to create test tenant allocator", KR(ret), K(TEST_TENANT_ID));
  } else if (OB_SUCCESS != (ret = allocator->set_tenant_limit(TEST_TENANT_ID, 1LL << 30))) {
    OB_LOG(WARN, "fail to set test tenant memory limit", KR(ret), K(TEST_TENANT_ID));
  }
  return ret;
}

// 验证 queue key 的合法性、相等性和 hash 一致性。
TEST(TestDirectLoadStreamBufferQueue, key_shape)
{
  ObDirectLoadStreamBufferQueueKey invalid_key;
  ObDirectLoadStreamBufferQueueKey zero_queue_key(1, 0);
  ObDirectLoadStreamBufferQueueKey equal_zero_queue_key(1, 0);
  EXPECT_FALSE(invalid_key.is_valid());
  EXPECT_TRUE(zero_queue_key.is_valid());
  EXPECT_FALSE(ObDirectLoadStreamBufferQueueKey(0, 0).is_valid());
  EXPECT_FALSE(ObDirectLoadStreamBufferQueueKey(-1, 0).is_valid());
  EXPECT_FALSE(ObDirectLoadStreamBufferQueueKey(1, common::OB_INVALID_ID).is_valid());
  EXPECT_EQ(zero_queue_key.hash(), equal_zero_queue_key.hash());
  EXPECT_TRUE(zero_queue_key == equal_zero_queue_key);
}

// 验证一个 statement handle 可连续发布多个 chunk，
// 并由合并读取接口按序返回。
TEST(TestDirectLoadStreamBufferQueue, append_handle_multiple_chunks_roundtrip)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  allocator.set_tenant_id(TEST_TENANT_ID);
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  ASSERT_EQ(OB_SUCCESS,
            queue.append_chunk(handle, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS,
            queue.append_chunk(handle, make_chunk(SECOND_CHUNK, SECOND_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, queue.end_append(handle));
  ASSERT_EQ(OB_SUCCESS, queue.request_close());
  EXPECT_EQ(OB_STATE_NOT_MATCH, queue.check_reader_drain_completed());
  ASSERT_EQ(OB_SUCCESS, queue.get_next_chunk(allocator, chunk));
  EXPECT_TRUE(string_equals(chunk, TEST_CHUNK, TEST_CHUNK_SIZE));
  EXPECT_EQ(OB_STATE_NOT_MATCH, queue.check_reader_drain_completed());
  ASSERT_EQ(OB_SUCCESS, queue.get_next_chunk(allocator, chunk));
  EXPECT_TRUE(string_equals(chunk, SECOND_CHUNK, SECOND_CHUNK_SIZE));
  // chunk 已从 queue 取走仍不够，reader 必须完整处理后再次观察到正常 EOF。
  EXPECT_EQ(OB_STATE_NOT_MATCH, queue.check_reader_drain_completed());
  EXPECT_EQ(OB_ITER_END, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.check_reader_drain_completed());
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 store write 失败不发布 chunk，并作为 queue 首错向后续操作传播。
TEST(TestDirectLoadStreamBufferQueue, store_append_failure_marks_failed)
{
  ObDirectLoadStreamMockQueueStore mock_store;
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  int first_error = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, queue.init_with_mock_store_for_test(
      TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey(1, 0), mock_store));
  mock_store.set_append_ret(OB_TIMEOUT);
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  EXPECT_EQ(OB_TIMEOUT,
            queue.append_chunk(handle, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, queue.get_first_error(first_error));
  EXPECT_EQ(OB_TIMEOUT, first_error);
  EXPECT_EQ(OB_TIMEOUT, queue.end_append(handle));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 store 返回短 chunk 时 queue 拒绝成功并记录为致命错误。
TEST(TestDirectLoadStreamBufferQueue, store_short_read_marks_failed)
{
  ObDirectLoadStreamMockQueueStore mock_store;
  ObDirectLoadStreamBufferQueue queue;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  int first_error = OB_SUCCESS;
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, queue.init_with_mock_store_for_test(
      TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey(1, 0), mock_store));
  ASSERT_EQ(OB_SUCCESS, append_statement_chunk(
      queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, queue.request_close());
  mock_store.set_short_read(true);
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.get_first_error(first_error));
  EXPECT_EQ(OB_ERR_UNEXPECTED, first_error);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 store read 返回 OB_ITER_END 时立即正规化为致命错误，不能伪装成正常 EOF。
TEST(TestDirectLoadStreamBufferQueue, store_iter_end_read_marks_failed)
{
  ObDirectLoadStreamMockQueueStore mock_store;
  ObDirectLoadStreamBufferQueue queue;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  int first_error = OB_SUCCESS;
  allocator.set_tenant_id(TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, queue.init_with_mock_store_for_test(
      TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey(1, 0), mock_store));
  ASSERT_EQ(OB_SUCCESS, append_statement_chunk(
      queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, queue.request_close());
  mock_store.set_read_ret(OB_ITER_END);
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.get_first_error(first_error));
  EXPECT_EQ(OB_ERR_UNEXPECTED, first_error);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证消费 1024 个 chunk 后仍有积压时，compaction 保留剩余内容和顺序。
TEST(TestDirectLoadStreamBufferQueue, ready_chunk_compaction_preserves_remaining_order)
{
  const int64_t chunk_count = 1026;
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  allocator.set_tenant_id(TEST_TENANT_ID);
  common::ObString chunk;
  std::vector<std::string> payloads;
  payloads.reserve(chunk_count);
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  for (int64_t i = 0; i < chunk_count; ++i) {
    payloads.push_back("compaction-chunk-" + std::to_string(i));
    const std::string &payload = payloads.back();
    const common::ObString payload_chunk(
        static_cast<int32_t>(payload.length()), payload.data());
    ASSERT_EQ(OB_SUCCESS, queue.append_chunk(handle, payload_chunk));
  }
  ASSERT_EQ(OB_SUCCESS, queue.end_append(handle));
  ASSERT_EQ(OB_SUCCESS, queue.request_close());
  for (int64_t i = 0; i < chunk_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, queue.get_next_chunk(allocator, chunk));
    EXPECT_EQ(payloads.at(i), to_std_string(chunk));
  }
  EXPECT_EQ(OB_ITER_END, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 default、active 重入和已结束 handle 不会借用全局 active 计数。
TEST(TestDirectLoadStreamBufferQueue, append_handle_rejects_invalid_and_ended)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  const common::ObString chunk = make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE);
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.append_chunk(handle, chunk));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.end_append(handle));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  EXPECT_EQ(OB_INIT_TWICE, queue.begin_append(handle));
  ASSERT_EQ(OB_SUCCESS, queue.append_chunk(handle, chunk));
  ASSERT_EQ(OB_SUCCESS, queue.end_append(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.append_chunk(handle, chunk));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.end_append(handle));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  EXPECT_EQ(OB_SUCCESS, queue.end_append(handle));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 AppendHandle 只能用于登记它的 queue，跨 queue 操作不释放原 statement。
TEST(TestDirectLoadStreamBufferQueue, append_handle_rejects_cross_queue)
{
  ObDirectLoadStreamBufferQueue first_queue;
  ObDirectLoadStreamBufferQueue second_queue;
  ObDirectLoadStreamAppendHandle handle;
  const common::ObString chunk = make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE);
  ASSERT_EQ(OB_SUCCESS, init_queue(first_queue, 1));
  ASSERT_EQ(OB_SUCCESS, init_queue(second_queue, 2));
  ASSERT_EQ(OB_SUCCESS, first_queue.begin_append(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, second_queue.append_chunk(handle, chunk));
  EXPECT_EQ(OB_INVALID_ARGUMENT, second_queue.end_append(handle));
  EXPECT_TRUE(handle.is_valid());
  EXPECT_EQ(OB_SUCCESS, first_queue.append_chunk(handle, chunk));
  EXPECT_EQ(OB_SUCCESS, first_queue.end_append(handle));
  EXPECT_EQ(OB_SUCCESS, first_queue.cleanup());
  EXPECT_EQ(OB_SUCCESS, second_queue.cleanup());
}

// 验证异常路径遗漏显式 end 时，AppendHandle 析构仍释放 CLOSE 屏障。
TEST(TestDirectLoadStreamBufferQueue, append_handle_destructor_releases_registration)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  std::future<int> close_future;
  bool close_waiting = false;
  std::future_status wait_status = std::future_status::deferred;
  {
    ObDirectLoadStreamAppendHandle handle;
    ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
    close_future = std::async(std::launch::async, close_queue, &queue);
    close_waiting = wait_until_state(
        queue, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
        std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
    wait_status = close_future.wait_for(SHORT_WAIT);
  }
  const std::future_status close_status = close_future.wait_for(LONG_WAIT);
  const int close_ret = std::future_status::ready == close_status
      ? close_future.get() : OB_TIMEOUT;
  EXPECT_TRUE(close_waiting);
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(std::future_status::ready, close_status);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::SEALED, queue.get_state());
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证公共 API 对未初始化、非法 tenant、空 chunk 和无效 handle 的返回合同。
TEST(TestDirectLoadStreamBufferQueue, public_api_rejects_invalid_arguments)
{
  ObDirectLoadStreamBufferQueue queue;
  char negative_length_buf = 'x';
  ObDirectLoadStreamAppendHandle handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString output;
  const common::ObString chunk = make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE);
  EXPECT_EQ(OB_NOT_INIT, queue.begin_append(handle));
  EXPECT_EQ(OB_NOT_INIT, queue.append_chunk(handle, chunk));
  EXPECT_EQ(OB_NOT_INIT, queue.end_append(handle));
  EXPECT_EQ(OB_NOT_INIT, queue.get_next_chunk(allocator, output));
  EXPECT_EQ(OB_NOT_INIT, queue.request_close());
  EXPECT_EQ(OB_NOT_INIT, queue.fail(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_NOT_INIT, queue.cleanup());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            queue.init(common::OB_INVALID_TENANT_ID,
                       ObDirectLoadStreamBufferQueueKey(1, 0)));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            queue.init(common::OB_SERVER_TENANT_ID,
                       ObDirectLoadStreamBufferQueueKey(1, 0)));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            queue.init(TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey()));
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.append_chunk(handle, chunk));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.append_chunk(handle, common::ObString()));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            queue.append_chunk(handle, common::ObString(-1, &negative_length_buf)));
  EXPECT_EQ(OB_SUCCESS, queue.end_append(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.end_append(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.fail(OB_SUCCESS));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 CLOSE 拒绝新 statement，并允许 consumer drain 已发布 bytes。
TEST(TestDirectLoadStreamBufferQueue, close_rejects_late_append_and_drains)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle late_handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS,
            append_statement_chunk(queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, queue.request_close());
  EXPECT_EQ(OB_STATE_NOT_MATCH, queue.begin_append(late_handle));
  ASSERT_EQ(OB_SUCCESS, queue.get_next_chunk(allocator, chunk));
  EXPECT_TRUE(string_equals(chunk, TEST_CHUNK, TEST_CHUNK_SIZE));
  EXPECT_EQ(OB_ITER_END, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 用 active handle 确定性验证 CLOSE 等待该 statement 发布和结束。
TEST(TestDirectLoadStreamBufferQueue, close_waits_active_append_then_drains)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  std::future<int> close_future = std::async(std::launch::async, close_queue, &queue);
  const bool close_waiting = wait_until_state(
      queue, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  const std::future_status wait_status = close_future.wait_for(SHORT_WAIT);
  const int append_ret = queue.append_chunk(
      handle, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE));
  const int end_ret = queue.end_append(handle);
  const std::future_status close_status = close_future.wait_for(LONG_WAIT);
  const int close_ret = std::future_status::ready == close_status
      ? close_future.get() : OB_TIMEOUT;
  EXPECT_TRUE(close_waiting);
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(OB_SUCCESS, append_ret);
  EXPECT_EQ(OB_SUCCESS, end_ret);
  EXPECT_EQ(std::future_status::ready, close_status);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  ASSERT_EQ(OB_SUCCESS, queue.get_next_chunk(allocator, chunk));
  EXPECT_TRUE(string_equals(chunk, TEST_CHUNK, TEST_CHUNK_SIZE));
  EXPECT_EQ(OB_ITER_END, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 CLOSE 必须等待全部 statement handle，而不是任意一次 end 后提前 SEALED。
TEST(TestDirectLoadStreamBufferQueue, close_waits_all_append_handles)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle first_handle;
  ObDirectLoadStreamAppendHandle second_handle;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(first_handle));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(second_handle));
  std::future<int> close_future = std::async(std::launch::async, close_queue, &queue);
  const bool close_waiting = wait_until_state(
      queue, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  const int first_end_ret = queue.end_append(first_handle);
  const std::future_status one_active_status = close_future.wait_for(SHORT_WAIT);
  const int second_end_ret = queue.end_append(second_handle);
  const std::future_status close_status = close_future.wait_for(LONG_WAIT);
  const int close_ret = std::future_status::ready == close_status
      ? close_future.get() : OB_TIMEOUT;
  EXPECT_TRUE(close_waiting);
  EXPECT_EQ(OB_SUCCESS, first_end_ret);
  EXPECT_EQ(std::future_status::timeout, one_active_status);
  EXPECT_EQ(OB_SUCCESS, second_end_ret);
  EXPECT_EQ(std::future_status::ready, close_status);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 CLOSE 等待被 worker 取消后进入 FAILED，end 释放 handle 但返回首错。
TEST(TestDirectLoadStreamBufferQueue, close_cancel_marks_failed)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  std::future<int> close_future = std::async(
      std::launch::async, close_queue_with_cancel, &queue);
  const std::future_status close_status = close_future.wait_for(LONG_WAIT);
  std::future_status cleanup_status = std::future_status::ready;
  int close_ret = OB_TIMEOUT;
  if (std::future_status::ready == close_status) {
    close_ret = close_future.get();
  } else {
    // 仅用于保证 future 收敛，不能覆盖首次等待结果。
    (void)queue.fail(OB_CANCELED);
    cleanup_status = close_future.wait_for(LONG_WAIT);
    if (std::future_status::ready == cleanup_status) {
      (void)close_future.get();
    }
  }
  const int append_ret = queue.append_chunk(
      handle, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE));
  const int end_ret = queue.end_append(handle);
  EXPECT_EQ(std::future_status::ready, close_status);
  EXPECT_EQ(OB_CANCELED, close_ret);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue.get_state());
  EXPECT_EQ(OB_CANCELED, append_ret);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_CANCELED, end_ret);
  EXPECT_FALSE(handle.is_valid());
  EXPECT_EQ(OB_CANCELED, queue.request_close());
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 OPEN 空队列 reader 在 chunk 发布后被唤醒并直接取得 bytes。
TEST(TestDirectLoadStreamBufferQueue, reader_waits_then_append_wakes)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  std::future<ReadResult> future = std::async(std::launch::async, read_one_chunk, &queue);
  const std::future_status wait_status = future.wait_for(SHORT_WAIT);
  const int append_ret = append_statement_chunk(
      queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE));
  const std::future_status read_status = future.wait_for(LONG_WAIT);
  const ReadResult result = std::future_status::ready == read_status
      ? future.get() : ReadResult();
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(OB_SUCCESS, append_ret);
  EXPECT_EQ(std::future_status::ready, read_status);
  EXPECT_EQ(OB_SUCCESS, result.ret_);
  EXPECT_EQ(std::string(TEST_CHUNK, TEST_CHUNK_SIZE), result.chunk_);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 OPEN 空队列 reader 周期检查 worker 状态，取消后转 FAILED 并返回首错。
TEST(TestDirectLoadStreamBufferQueue, reader_wait_is_cancelable)
{
  ObDirectLoadStreamBufferQueue queue;
  std::atomic<int64_t> check_count(0);
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  std::future<ReadResult> future = std::async(
      std::launch::async, read_one_chunk_with_delayed_cancel, &queue, &check_count);
  const std::future_status read_status = future.wait_for(LONG_WAIT);
  std::future_status cleanup_status = std::future_status::ready;
  ReadResult result;
  result.ret_ = OB_TIMEOUT;
  if (std::future_status::ready == read_status) {
    result = future.get();
  } else {
    // 仅用于保证 future 收敛，不能覆盖首次等待结果。
    (void)queue.fail(OB_ERR_UNEXPECTED);
    cleanup_status = future.wait_for(LONG_WAIT);
    if (std::future_status::ready == cleanup_status) {
      (void)future.get();
    }
  }
  common::ObArenaAllocator allocator("DLSBQTest");
  allocator.set_tenant_id(TEST_TENANT_ID);
  common::ObString chunk;
  EXPECT_EQ(std::future_status::ready, read_status);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_GE(check_count.load(), 2);
  EXPECT_EQ(OB_CANCELED, result.ret_);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue.get_state());
  EXPECT_EQ(OB_CANCELED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 OPEN 空队列 reader 在 CLOSE 后返回唯一合法 EOF。
TEST(TestDirectLoadStreamBufferQueue, reader_waits_then_close_returns_iter_end)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  std::future<ReadResult> future = std::async(std::launch::async, read_one_chunk, &queue);
  const std::future_status wait_status = future.wait_for(SHORT_WAIT);
  const int close_ret = queue.request_close();
  const std::future_status read_status = future.wait_for(LONG_WAIT);
  const ReadResult result = std::future_status::ready == read_status
      ? future.get() : ReadResult();
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  EXPECT_EQ(std::future_status::ready, read_status);
  EXPECT_EQ(OB_ITER_END, result.ret_);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证内部读取分配失败会消费 meta、转 FAILED 并保留首错。
TEST(TestDirectLoadStreamBufferQueue, read_allocation_failure_marks_failed)
{
  ObDirectLoadStreamBufferQueue queue;
  FailAllocator fail_allocator;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS,
            append_statement_chunk(queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, queue.get_next_chunk(fail_allocator, chunk));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue.get_state());
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 fail 唤醒等待 reader 并向其传递 fatal error。
TEST(TestDirectLoadStreamBufferQueue, fail_wakes_reader_with_error)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  std::future<ReadResult> future = std::async(std::launch::async, read_one_chunk, &queue);
  const std::future_status wait_status = future.wait_for(SHORT_WAIT);
  const int fail_ret = queue.fail(OB_ERR_UNEXPECTED);
  const std::future_status read_status = future.wait_for(LONG_WAIT);
  const ReadResult result = std::future_status::ready == read_status
      ? future.get() : ReadResult();
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(OB_SUCCESS, fail_ret);
  EXPECT_EQ(std::future_status::ready, read_status);
  EXPECT_EQ(OB_ERR_UNEXPECTED, result.ret_);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 FAILED 优先于已发布 chunk，不再向 consumer 暴露 ready bytes。
TEST(TestDirectLoadStreamBufferQueue, fail_rejects_ready_chunk)
{
  ObDirectLoadStreamBufferQueue queue;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS,
            append_statement_chunk(queue, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, queue.fail(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证重复 fail 保留第一个 fatal error。
TEST(TestDirectLoadStreamBufferQueue, fail_twice_keeps_first_error)
{
  ObDirectLoadStreamBufferQueue queue;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  int first_error = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.fail(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_SUCCESS, queue.fail(OB_CANCELED));
  ASSERT_EQ(OB_SUCCESS, queue.get_first_error(first_error));
  EXPECT_EQ(OB_ERR_UNEXPECTED, first_error);
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 OB_ITER_END 不能成为 fatal error，FAILED 不能伪装成正常 EOF。
TEST(TestDirectLoadStreamBufferQueue, fail_iter_end_maps_to_unexpected_error)
{
  ObDirectLoadStreamBufferQueue queue;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.fail(OB_ITER_END));
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::FAILED, queue.get_state());
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证 cleanup 后 fail 返回 queue 已移除，而不是伪装成功。
TEST(TestDirectLoadStreamBufferQueue, fail_after_cleanup_returns_not_exist)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.cleanup());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, queue.get_state());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.fail(OB_ERR_UNEXPECTED));
}

// 验证 cleanup 等待 active AppendHandle，handle 结束后返回 REMOVED 并解除等待。
TEST(TestDirectLoadStreamBufferQueue, cleanup_waits_append_handle)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.begin_append(handle));
  std::future<int> cleanup_future = std::async(
      std::launch::async, cleanup_queue, &queue);
  const std::future_status wait_status = cleanup_future.wait_for(SHORT_WAIT);
  const bool removed_seen = wait_until_state(
      queue, ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  const int append_ret = queue.append_chunk(
      handle, make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE));
  const int end_ret = queue.end_append(handle);
  const std::future_status cleanup_status = cleanup_future.wait_for(LONG_WAIT);
  const int cleanup_ret = std::future_status::ready == cleanup_status
      ? cleanup_future.get() : OB_TIMEOUT;
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_TRUE(removed_seen);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, append_ret);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, end_ret);
  EXPECT_FALSE(handle.is_valid());
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
}

// 验证 remove 失败保留 queue cleanup 重试入口，重试成功后完成清理。
TEST(TestDirectLoadStreamBufferQueue, cleanup_retries_store_remove_failure)
{
  ObDirectLoadStreamMockQueueStore mock_store;
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, queue.init_with_mock_store_for_test(
      TEST_TENANT_ID, ObDirectLoadStreamBufferQueueKey(1, 0), mock_store));
  mock_store.set_remove_ret(OB_TIMEOUT);
  EXPECT_EQ(OB_TIMEOUT, queue.cleanup());
  EXPECT_EQ(1, mock_store.get_remove_call_count());
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, queue.get_state());
  mock_store.set_remove_ret(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
  EXPECT_EQ(2, mock_store.get_remove_call_count());
}

// 验证 REMOVED 状态拒绝新 statement、读取、CLOSE 和 fail，cleanup 保持幂等。
TEST(TestDirectLoadStreamBufferQueue, cleanup_rejects_public_operations)
{
  ObDirectLoadStreamBufferQueue queue;
  ObDirectLoadStreamAppendHandle handle;
  common::ObArenaAllocator allocator("DLSBQTest");
  common::ObString chunk;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, queue.cleanup());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.begin_append(handle));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_chunk(allocator, chunk));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.request_close());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.fail(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.fail_if_open(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 验证多个 APPEND statement 并发发布后，单 consumer 完整读回且无重复。
TEST(TestDirectLoadStreamBufferQueue, concurrent_append_single_reader_validates_payloads)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  const int64_t writer_count = 8;
  const int64_t chunk_count_per_writer = 32;
  std::mutex expected_mutex;
  std::set<std::string> expected_payloads;
  std::atomic<int> first_error(OB_SUCCESS);
  AppendWriterContext ctx = {
      &queue, chunk_count_per_writer, "writer-", nullptr, nullptr, nullptr,
      &first_error, nullptr, &expected_mutex, &expected_payloads};
  std::vector<std::thread> writers;
  for (int64_t writer_idx = 0; writer_idx < writer_count; ++writer_idx) {
    writers.emplace_back(append_writer, &ctx, writer_idx);
  }
  join_writers(writers);
  const int writer_ret = first_error.load();
  const int64_t expected_count = expected_payloads.size();
  const int close_ret = queue.request_close();
  std::set<std::string> actual_payloads;
  const int drain_ret = drain_queue(queue, actual_payloads);
  EXPECT_EQ(OB_SUCCESS, writer_ret);
  EXPECT_EQ(writer_count * chunk_count_per_writer, expected_count);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  EXPECT_EQ(OB_ITER_END, drain_ret);
  EXPECT_EQ(expected_payloads, actual_payloads);
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 所有 writer 先取得 statement handle，再发起 CLOSE，稳定覆盖并发等待分支。
TEST(TestDirectLoadStreamBufferQueue, concurrent_append_with_close_validates_payloads)
{
  ObDirectLoadStreamBufferQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  const int64_t writer_count = 8;
  const int64_t chunk_count_per_writer = 128;
  const std::string payload_padding(4 * 1024, 'x');
  std::mutex expected_mutex;
  std::set<std::string> expected_payloads;
  std::atomic<bool> start(false);
  std::atomic<int64_t> registered_count(0);
  std::atomic<int> first_error(OB_SUCCESS);
  std::atomic<int64_t> success_count(0);
  AppendWriterContext ctx = {
      &queue, chunk_count_per_writer, "close-writer-", &payload_padding, &start,
      &registered_count, &first_error, &success_count, &expected_mutex, &expected_payloads};
  std::vector<std::thread> writers;
  for (int64_t writer_idx = 0; writer_idx < writer_count; ++writer_idx) {
    writers.emplace_back(append_writer, &ctx, writer_idx);
  }
  const bool all_registered = wait_until_count(
      registered_count, writer_count,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  std::future<int> close_future = std::async(std::launch::async, close_queue, &queue);
  const bool close_waiting = wait_until_state(
      queue, ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  start.store(true);
  join_writers(writers);
  const std::future_status close_status = close_future.wait_for(LONG_WAIT);
  const int close_ret = std::future_status::ready == close_status
      ? close_future.get() : OB_TIMEOUT;
  const int writer_ret = first_error.load();
  const int64_t written_count = success_count.load();
  std::set<std::string> actual_payloads;
  const int drain_ret = drain_queue(queue, actual_payloads);
  ObDirectLoadStreamAppendHandle late_handle;
  EXPECT_TRUE(all_registered);
  EXPECT_TRUE(close_waiting);
  EXPECT_EQ(std::future_status::ready, close_status);
  EXPECT_EQ(OB_SUCCESS, close_ret);
  EXPECT_EQ(OB_SUCCESS, writer_ret);
  EXPECT_EQ(writer_count * chunk_count_per_writer, written_count);
  EXPECT_EQ(OB_ITER_END, drain_ret);
  EXPECT_EQ(expected_payloads, actual_payloads);
  EXPECT_EQ(OB_STATE_NOT_MATCH, queue.begin_append(late_handle));
  EXPECT_EQ(OB_SUCCESS, queue.cleanup());
}

// 通过 manager handle 显式 assign 和 cleanup 等待验证引用计数生命周期。
TEST(TestDirectLoadStreamBufferQueue, handle_ref_count)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle copied_handle;
  ObDirectLoadStreamBufferQueueHandle assigned_handle;
  ObDirectLoadStreamBufferQueueHandle invalid_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  ObDirectLoadStreamBufferQueue *queue = handle.get_queue();
  EXPECT_EQ(OB_INVALID_ARGUMENT, handle.assign(invalid_handle));
  EXPECT_EQ(queue, handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, copied_handle.assign(handle));
  ASSERT_EQ(OB_SUCCESS, assigned_handle.assign(copied_handle));
  handle.reset();
  copied_handle.reset();
  std::future<int> future = std::async(
      std::launch::async, cleanup_manager_queue, &manager, key);
  const std::future_status wait_status = future.wait_for(SHORT_WAIT);
  assigned_handle.reset();
  const std::future_status cleanup_status = future.wait_for(LONG_WAIT);
  const int cleanup_ret = std::future_status::ready == cleanup_status
      ? future.get() : OB_TIMEOUT;
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  manager.destroy();
}

// 验证 cleanup 进入 REMOVED 后，已有 handle lineage 仍可显式增加引用并延长生命周期。
TEST(TestDirectLoadStreamBufferQueue, handle_assign_during_cleanup)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle assigned_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  ObDirectLoadStreamBufferQueue *queue = handle.get_queue();
  std::future<int> cleanup_future = std::async(
      std::launch::async, cleanup_manager_queue, &manager, key);
  const std::future_status initial_status = cleanup_future.wait_for(SHORT_WAIT);
  const bool removed_seen = wait_until_state(
      *queue, ObDirectLoadStreamBufferQueueState::REMOVED,
      std::chrono::duration_cast<std::chrono::milliseconds>(LONG_WAIT));
  const int assign_ret = assigned_handle.assign(handle);
  const bool assigned_valid = assigned_handle.is_valid();
  const bool same_queue = assigned_handle.get_queue() == queue;
  handle.reset();
  const std::future_status lineage_status = cleanup_future.wait_for(SHORT_WAIT);
  assigned_handle.reset();
  const std::future_status cleanup_status = cleanup_future.wait_for(LONG_WAIT);
  const int cleanup_ret = std::future_status::ready == cleanup_status
      ? cleanup_future.get() : OB_TIMEOUT;
  EXPECT_EQ(std::future_status::timeout, initial_status);
  EXPECT_TRUE(removed_seen);
  EXPECT_EQ(OB_SUCCESS, assign_ret);
  EXPECT_TRUE(assigned_valid);
  EXPECT_TRUE(same_queue);
  EXPECT_EQ(std::future_status::timeout, lineage_status);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  manager.destroy();
}

// 验证 cleanup 周期等待忽略单次超时，worker 取消后保留 queue 并允许重试。
TEST(TestDirectLoadStreamBufferQueue, manager_cleanup_wait_is_cancelable_and_retryable)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle probe_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  std::atomic<int64_t> check_count(0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  ObDirectLoadStreamBufferQueue *queue = handle.get_queue();
  ASSERT_NE(nullptr, queue);
  std::future<int> cleanup_future = std::async(
      std::launch::async, cleanup_manager_queue_with_delayed_cancel,
      &manager, key, &check_count);
  const std::future_status cleanup_status = cleanup_future.wait_for(LONG_WAIT);
  int cleanup_ret = OB_TIMEOUT;
  ObDirectLoadStreamBufferQueueState cleanup_state =
      ObDirectLoadStreamBufferQueueState::INVALID;
  int get_ret = OB_SUCCESS;
  int duplicate_create_ret = OB_SUCCESS;
  if (std::future_status::ready == cleanup_status) {
    cleanup_ret = cleanup_future.get();
    cleanup_state = queue->get_state();
    get_ret = manager.get_queue(key, probe_handle);
    duplicate_create_ret = manager.create_queue(key, probe_handle);
  }
  probe_handle.reset();
  handle.reset();
  std::future_status final_status = cleanup_status;
  if (std::future_status::ready != final_status) {
    final_status = cleanup_future.wait_for(LONG_WAIT);
    if (std::future_status::ready == final_status) {
      (void)cleanup_future.get();
    }
  }
  const int retry_ret = manager.cleanup_queue(key);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(std::future_status::ready, final_status);
  EXPECT_GE(check_count.load(), 2);
  EXPECT_EQ(OB_CANCELED, cleanup_ret);
  EXPECT_EQ(ObDirectLoadStreamBufferQueueState::REMOVED, cleanup_state);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_ret);
  EXPECT_EQ(OB_HASH_EXIST, duplicate_create_ret);
  EXPECT_EQ(OB_SUCCESS, retry_ret);
  manager.destroy();
}

// 验证 manager 枚举生命周期下的 tenant/init/create/get/seal/cleanup 主路径。
TEST(TestDirectLoadStreamBufferQueue, manager_create_get_seal_cleanup)
{
  ObDirectLoadStreamBufferQueueManager invalid_manager(common::OB_INVALID_TENANT_ID);
  ObDirectLoadStreamBufferQueueManager server_manager(common::OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_manager.init());
  EXPECT_EQ(OB_INVALID_ARGUMENT, server_manager.init());
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle get_handle;
  ObDirectLoadStreamBufferQueueHandle duplicate_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  EXPECT_FALSE(manager.is_inited());
  EXPECT_EQ(OB_NOT_INIT, manager.create_queue(key, handle));
  EXPECT_EQ(OB_NOT_INIT, manager.get_queue(key, handle));
  EXPECT_EQ(OB_NOT_INIT, manager.seal_queue(key));
  EXPECT_EQ(OB_NOT_INIT, manager.cleanup_queue(key));
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  EXPECT_TRUE(manager.is_inited());
  EXPECT_EQ(OB_INIT_TWICE, manager.init());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            manager.create_queue(ObDirectLoadStreamBufferQueueKey(), handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            manager.get_queue(ObDirectLoadStreamBufferQueueKey(), handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.seal_queue(ObDirectLoadStreamBufferQueueKey()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, manager.cleanup_queue(ObDirectLoadStreamBufferQueueKey()));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  EXPECT_EQ(OB_HASH_EXIST, manager.create_queue(key, duplicate_handle));
  ASSERT_EQ(OB_SUCCESS, manager.get_queue(key, get_handle));
  ASSERT_EQ(OB_SUCCESS, append_statement_chunk(
      *handle.get_queue(), make_chunk(TEST_CHUNK, TEST_CHUNK_SIZE)));
  ASSERT_EQ(OB_SUCCESS, manager.seal_queue(key));
  ObDirectLoadStreamAppendHandle late_handle;
  EXPECT_EQ(OB_STATE_NOT_MATCH, handle.get_queue()->begin_append(late_handle));
  handle.reset();
  get_handle.reset();
  ASSERT_EQ(OB_SUCCESS, manager.cleanup_queue(key));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, manager.get_queue(key, get_handle));
  EXPECT_EQ(OB_SUCCESS, manager.cleanup_queue(key));
  // 验证生命周期线程重复 destroy 时保持幂等。
  manager.destroy();
  EXPECT_FALSE(manager.is_inited());
  manager.destroy();
  EXPECT_FALSE(manager.is_inited());
}

// 验证 reader 全程持有 manager handle 时，
// cleanup 无需 active_readers 也不会提前删除。
TEST(TestDirectLoadStreamBufferQueue, manager_reader_handle_covers_cleanup)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle reader_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  ASSERT_EQ(OB_SUCCESS, reader_handle.assign(handle));
  std::future<ReadResult> read_future = std::async(
      std::launch::async, read_one_chunk_with_handle, &reader_handle);
  const std::future_status read_wait_status = read_future.wait_for(SHORT_WAIT);
  handle.reset();
  std::future<int> cleanup_future = std::async(
      std::launch::async, cleanup_manager_queue, &manager, key);
  const std::future_status read_status = read_future.wait_for(LONG_WAIT);
  const ReadResult read_result = std::future_status::ready == read_status
      ? read_future.get() : ReadResult();
  const std::future_status cleanup_status = cleanup_future.wait_for(LONG_WAIT);
  const int cleanup_ret = std::future_status::ready == cleanup_status
      ? cleanup_future.get() : OB_TIMEOUT;
  EXPECT_EQ(std::future_status::timeout, read_wait_status);
  EXPECT_EQ(std::future_status::ready, read_status);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, read_result.ret_);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  manager.destroy();
}

// 验证 manager 能在 map 中仍有 queue 时完成最终销毁。
TEST(TestDirectLoadStreamBufferQueue, manager_destroy_with_remaining_queues)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle first_handle;
  ObDirectLoadStreamBufferQueueHandle second_handle;
  ObDirectLoadStreamBufferQueueKey first_key(1, 0);
  ObDirectLoadStreamBufferQueueKey second_key(2, 0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(first_key, first_handle));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(second_key, second_handle));
  first_handle.reset();
  second_handle.reset();

  manager.destroy();

  EXPECT_FALSE(manager.is_inited());
}

// 验证 cleanup marker 阻止新 handle，并让第二次 cleanup 返回 EAGAIN。
TEST(TestDirectLoadStreamBufferQueue, manager_cleanup_marker_serializes_cleanup)
{
  ObDirectLoadStreamBufferQueueManager manager(TEST_TENANT_ID);
  ObDirectLoadStreamBufferQueueHandle handle;
  ObDirectLoadStreamBufferQueueHandle probe_handle;
  ObDirectLoadStreamBufferQueueKey key(1, 0);
  ASSERT_EQ(OB_SUCCESS, init_manager(manager));
  ASSERT_EQ(OB_SUCCESS, manager.create_queue(key, handle));
  std::future<int> future = std::async(
      std::launch::async, cleanup_manager_queue, &manager, key);
  const std::future_status wait_status = future.wait_for(SHORT_WAIT);
  int get_ret = OB_SUCCESS;
  bool cleaning_seen = false;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + LONG_WAIT;
  while (!cleaning_seen && std::chrono::steady_clock::now() < deadline) {
    probe_handle.reset();
    get_ret = manager.get_queue(key, probe_handle);
    cleaning_seen = OB_ENTRY_NOT_EXIST == get_ret;
    if (!cleaning_seen) {
      std::this_thread::yield();
    }
  }
  const int second_cleanup_ret = manager.cleanup_queue(key);
  handle.reset();
  const std::future_status cleanup_status = future.wait_for(LONG_WAIT);
  const int cleanup_ret = std::future_status::ready == cleanup_status
      ? future.get() : OB_TIMEOUT;
  EXPECT_EQ(std::future_status::timeout, wait_status);
  EXPECT_TRUE(cleaning_seen);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, get_ret);
  EXPECT_EQ(OB_EAGAIN, second_cleanup_ret);
  EXPECT_EQ(std::future_status::ready, cleanup_status);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  manager.destroy();
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = oceanbase::observer::init_test_tenant_allocator();
  if (oceanbase::common::OB_SUCCESS == ret) {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}

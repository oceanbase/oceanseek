/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "logservice/palf/log_define.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/tx/ob_tx_seq.h"
#include <gtest/gtest.h>
#include <string>
#include "lib/utility/ob_print_utils.h"

// 本测试需要直接验证私有的行编码接口及其缓冲区状态。
#define protected public
#define private public
#include "storage/lob/ob_ext_info_callback.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

// 支持指定某一次分配失败，并记录仍未释放的内存块数量。
class TestExtInfoAllocator : public ObIAllocator
{
public:
  TestExtInfoAllocator()
    : alloc_attempt_count_(0), outstanding_allocation_count_(0), last_alloc_size_(0),
      fail_on_alloc_attempt_(0), allocator_()
  {}

  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    ++alloc_attempt_count_;
    last_alloc_size_ = size;
    if (alloc_attempt_count_ != fail_on_alloc_attempt_) {
      ptr = allocator_.alloc(size);
      if (nullptr != ptr) {
        ++outstanding_allocation_count_;
      }
    }
    return ptr;
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }

  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      --outstanding_allocation_count_;
      allocator_.free(ptr);
    }
  }

  void fail_on_alloc_attempt(const int64_t attempt) { fail_on_alloc_attempt_ = attempt; }

  int64_t alloc_attempt_count_;
  int64_t outstanding_allocation_count_;
  int64_t last_alloc_size_;
  int64_t fail_on_alloc_attempt_;
private:
  ObArenaAllocator allocator_;
};

class TestExtInfoMutatorRow : public ::testing::Test
{
public:
  void SetUp() override
  {
    lob_id_.assign(7, 1001, 42);

    // ext-info mutator row 固定包含三列：
    // 1. 事务内序号；2. ext-info 日志数据；3. LOB ID。
    ObObj key;
    key.set_uint64(1234);
    ASSERT_EQ(OB_SUCCESS, row_.init(row_allocator_, ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_COUNT));
    ASSERT_EQ(OB_SUCCESS, row_.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_KEY_IDX]
        .from_obj_enhance(key));
    row_.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_LOB_ID_IDX].set_string(
        lob_id_.get_string());
  }

  void prepare_data(const int64_t data_len)
  {
    data_.assign(data_len, 'x');
    // Datum 只引用 data_ 的内存，因此 data_ 重新分配后必须重新绑定。
    row_.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].set_string(
        ObString(static_cast<int32_t>(data_.length()), data_.data()));
  }

  void check_encoded_row(
      const ObExtInfoCallback &callback,
      const std::string &expected_data)
  {
    blocksstable::ObCompatRowReader reader;
    blocksstable::ObDatumRow decoded;
    ASSERT_EQ(OB_SUCCESS, reader.read_row(callback.mutator_row_buf_,
        callback.mutator_row_len_, nullptr, decoded));
    const int32_t expected_column_count = ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_COUNT;
    ASSERT_EQ(expected_column_count, decoded.count_);
    ASSERT_EQ(1234, decoded.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_KEY_IDX].get_uint64());
    ASSERT_EQ(ObString(static_cast<int32_t>(expected_data.length()), expected_data.data()),
        decoded.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].get_string());
    ASSERT_EQ(lob_id_.get_string(),
        decoded.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_LOB_ID_IDX].get_string());
  }

protected:
  TestExtInfoAllocator allocator_;
  ObArenaAllocator row_allocator_;
  ObLobId lob_id_;
  std::string data_;
  blocksstable::ObDatumRow row_;
};

TEST_F(TestExtInfoMutatorRow, grow_buffer_when_initial_buffer_is_too_small)
{
  prepare_data(4096);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;

  // 生产调用始终传真实数据长度；这里是白盒测试，故意传入较小的 data_len，
  // 仅用于覆盖 OB_BUF_NOT_ENOUGH 后释放旧缓冲区并扩容重试的分支。
  ASSERT_EQ(OB_SUCCESS, callback.set_mutator_row_(row_, 1));
  ASSERT_GT(allocator_.alloc_attempt_count_, 1);
  ASSERT_EQ(1, allocator_.outstanding_allocation_count_);
  ASSERT_NO_FATAL_FAILURE(check_encoded_row(callback, data_));

  ASSERT_EQ(OB_SUCCESS, callback.release_resource());
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
}

TEST_F(TestExtInfoMutatorRow, write_row_near_log_limit)
{
  const int64_t max_buf_len = static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE);
  const int64_t row_encoding_margin = 512;
  prepare_data(max_buf_len - row_encoding_margin);
  const std::string expected_data(data_);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;

  // data_len + 1 KiB 超过日志上限时，初始缓冲区应截到日志上限；
  // 预留的 512 字节足够当前固定三列的行格式开销，大行仍能正常编码；
  // 该数据量也超过旧 RowWriter 内部缓冲区上限。
  ASSERT_EQ(OB_SUCCESS, callback.set_mutator_row_(row_, data_.length()));
  ASSERT_EQ(max_buf_len, allocator_.last_alloc_size_);

  // 修改源数据后仍能读回原值，证明 callback 持有的是独立副本。
  MEMSET(&data_[0], 0, data_.length());
  ASSERT_NO_FATAL_FAILURE(check_encoded_row(callback, expected_data));

  ASSERT_EQ(OB_SUCCESS, callback.release_resource());
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
}

TEST_F(TestExtInfoMutatorRow, release_buffer_when_growing_buffer_fails)
{
  prepare_data(4096);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;
  allocator_.fail_on_alloc_attempt(2);

  // 第一次写入空间不足；第二次扩容分配失败时，第一次的缓冲区必须已释放。
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, callback.set_mutator_row_(row_, 1));
  ASSERT_EQ(2, allocator_.alloc_attempt_count_);
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
  ASSERT_EQ(nullptr, callback.mutator_row_buf_);
  ASSERT_EQ(0, callback.mutator_row_len_);
}

TEST_F(TestExtInfoMutatorRow, do_not_retry_non_buffer_error)
{
  prepare_data(4096);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;
  row_.storage_datums_[ObExtInfoCallback::OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].set_min();

  // RowWriter 返回 OB_NOT_SUPPORTED，不是空间不足，不能通过扩容重试。
  ASSERT_EQ(OB_NOT_SUPPORTED, callback.set_mutator_row_(row_, data_.length()));
  ASSERT_EQ(1, allocator_.alloc_attempt_count_);
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
  ASSERT_EQ(nullptr, callback.mutator_row_buf_);
  ASSERT_EQ(0, callback.mutator_row_len_);
}

TEST_F(TestExtInfoMutatorRow, reject_invalid_data_length)
{
  const int64_t max_buf_len = static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;

  // 非正数以及超过单条日志上限的长度，都应在分配前被拒绝。
  ASSERT_EQ(OB_INVALID_ARGUMENT, callback.set_mutator_row_(row_, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, callback.set_mutator_row_(row_, max_buf_len + 1));
  ASSERT_EQ(0, allocator_.alloc_attempt_count_);
}

TEST_F(TestExtInfoMutatorRow, reject_row_larger_than_log_size_limit)
{
  const int64_t max_buf_len = static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE);
  prepare_data(max_buf_len);
  ObExtInfoCallback callback;
  callback.allocator_ = &allocator_;

  // 数据本身已占满日志上限，加入行头后必然无法写入。
  ASSERT_EQ(OB_SIZE_OVERFLOW, callback.set_mutator_row_(row_, data_.length()));
  ASSERT_EQ(1, allocator_.alloc_attempt_count_);
  ASSERT_EQ(max_buf_len, allocator_.last_alloc_size_);
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
  ASSERT_EQ(nullptr, callback.mutator_row_buf_);
  ASSERT_EQ(0, callback.mutator_row_len_);
}

TEST_F(TestExtInfoMutatorRow, destructor_releases_buffer)
{
  prepare_data(64);
  {
    ObExtInfoCallback callback;
    callback.allocator_ = &allocator_;
    ASSERT_EQ(OB_SUCCESS, callback.set_mutator_row_(row_, data_.length()));
    ASSERT_EQ(1, allocator_.outstanding_allocation_count_);
  }
  // callback 已析构，之前持有的编码缓冲区应随之释放。
  ASSERT_EQ(0, allocator_.outstanding_allocation_count_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_ext_info_mutator_row.log*");
  OB_LOGGER.set_file_name("test_lob_ext_info_mutator_row.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

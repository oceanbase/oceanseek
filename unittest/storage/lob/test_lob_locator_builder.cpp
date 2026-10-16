/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>

#define protected public
#define private public

#include "lib/allocator/page_arena.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/common/ob_lob_locator.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobLocatorBuilder : public ::testing::Test
{
public:
  TestLobLocatorBuilder() {}
  virtual ~TestLobLocatorBuilder() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

static void expect_ob_string_eq(const ObString &actual, const char *expected, const int64_t expected_len)
{
  ASSERT_EQ(expected_len, actual.length());
  if (expected_len > 0) {
    ASSERT_NE(nullptr, actual.ptr());
    ASSERT_EQ(0, MEMCMP(actual.ptr(), expected, expected_len));
  }
}

static int make_inrow_disk_locator(ObIAllocator &allocator,
                                   const char *payload,
                                   ObString &disk_locator)
{
  int ret = OB_SUCCESS;
  const int64_t payload_len = nullptr == payload ? 0 : STRLEN(payload);
  const int64_t locator_len = sizeof(ObLobCommon) + payload_len;
  char *buf = static_cast<char *>(allocator.alloc(locator_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObLobCommon *lob_common = new (buf) ObLobCommon();
    lob_common->in_row_ = true;
    lob_common->is_mem_loc_ = false;
    if (payload_len > 0) {
      MEMCPY(lob_common->buffer_, payload, payload_len);
    }
    disk_locator.assign_ptr(buf, locator_len);
  }
  return ret;
}

static int make_outrow_disk_locator(ObIAllocator &allocator,
                                    ObLobLocatorV2 &locator,
                                    ObString &disk_locator)
{
  int ret = OB_SUCCESS;
  ObLobDiskLocatorBuilder builder;
  ObLobId lob_id;
  lob_id.assign(1, 1001, 2002);
  transaction::ObTxSEQ seq_no(10, 0);
  if (OB_FAIL(builder.init_outrow(allocator))) {
  } else if (OB_FAIL(builder.set_lob_id(lob_id))) {
  } else if (OB_FAIL(builder.set_seq_no(seq_no))) {
  } else if (OB_FAIL(builder.to_locator(locator))) {
  } else if (OB_FAIL(locator.get_disk_locator(disk_locator))) {
  }
  return ret;
}

static int make_append_delta_locator(ObIAllocator &allocator,
                                     const char *append_data,
                                     ObLobLocatorV2 &locator)
{
  int ret = OB_SUCCESS;
  const int64_t data_len = nullptr == append_data ? 0 : STRLEN(append_data);
  const int64_t payload_offset =
      sizeof(ObMemLobCommon) + sizeof(ObMemLobExternHeader);
  const int64_t locator_len = payload_offset + data_len;
  char *buf = static_cast<char *>(allocator.alloc(locator_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObMemLobCommon *mem_lob = new (buf) ObMemLobCommon(ObMemLobType::APPEND_DELTA_LOB);
    mem_lob->lob_common_.in_row_ = false;
    mem_lob->set_read_only(true);
    mem_lob->set_has_extern(true);
    mem_lob->set_has_payload(true);
    mem_lob->disk_locator_size_ = 0;
    mem_lob->payload_offset_ = payload_offset;
    mem_lob->payload_size_ = data_len;

    ObMemLobExternHeader *extern_header =
        new (mem_lob->buffer_) ObMemLobExternHeader();
    extern_header->cs_type_ = CS_TYPE_BINARY;
    if (data_len > 0) {
      MEMCPY(buf + mem_lob->payload_offset_, append_data, data_len);
    }
    locator.assign_buffer(buf, locator_len);
  }
  return ret;
}

static void expect_fused_persisted_default(const char *payload, const bool is_systable)
{
  ObArenaAllocator allocator;
  ObLobLocatorHelper helper;
  helper.is_inited_ = true;
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, payload, disk_locator));
  ASSERT_EQ(sizeof(ObLobCommon) + STRLEN(payload), disk_locator.length());

  ObObj default_value;
  default_value.set_lob_value(ObLongTextType, disk_locator.ptr(), disk_locator.length());
  ASSERT_EQ(OB_SUCCESS,
            helper.fuse_mem_lob_header(default_value, OB_APP_MIN_COLUMN_ID, is_systable));

  ObLobLocatorV2 locator(default_value.get_string());
  ASSERT_TRUE(locator.is_valid());
  ASSERT_FALSE(locator.is_lob_disk_locator());

  ObString fused_disk_locator;
  ASSERT_EQ(OB_SUCCESS, locator.get_disk_locator(fused_disk_locator));
  ASSERT_EQ(disk_locator.length(), fused_disk_locator.length());
  EXPECT_EQ(0, MEMCMP(disk_locator.ptr(), fused_disk_locator.ptr(), disk_locator.length()));

  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, locator.get_inrow_data(inrow_data));
  expect_ob_string_eq(inrow_data, payload, STRLEN(payload));
}

TEST_F(TestLobLocatorBuilder, wrap_persisted_default_empty_lob)
{
  ObArenaAllocator allocator;
  const char *payload = "";
  ObString disk_locator;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;

  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, payload, disk_locator));
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_valid());
  ASSERT_FALSE(locator.is_lob_disk_locator());
  ASSERT_TRUE(locator.is_persist_lob());

  ObMemLobCommon *mem_loc = locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_loc);
  EXPECT_TRUE(mem_loc->lob_common_.in_row_);
  EXPECT_TRUE(mem_loc->lob_common_.is_mem_loc_);
  EXPECT_FALSE(mem_loc->has_extern());
  EXPECT_FALSE(mem_loc->has_payload());
  EXPECT_EQ(disk_locator.length(), mem_loc->disk_locator_size_);

  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, locator.get_inrow_data(inrow_data));
  EXPECT_EQ(0, inrow_data.length());
}

TEST_F(TestLobLocatorBuilder, wrap_persisted_default_inrow_value)
{
  ObArenaAllocator allocator;
  const char *payload = "default-lob-value";
  ObString disk_locator;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;

  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, payload, disk_locator));
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_valid());

  ObMemLobCommon *mem_loc = locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_loc);
  EXPECT_EQ(disk_locator.length(), mem_loc->disk_locator_size_);

  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, locator.get_inrow_data(inrow_data));
  expect_ob_string_eq(inrow_data, payload, STRLEN(payload));
}

TEST_F(TestLobLocatorBuilder, wrap_inrow_disk_locator)
{
  ObArenaAllocator allocator;
  const char *payload = "disk-inrow-lob";
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, payload, disk_locator));

  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));

  ObMemLobCommon *mem_loc = locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_loc);
  EXPECT_TRUE(mem_loc->lob_common_.in_row_);
  EXPECT_TRUE(mem_loc->lob_common_.is_mem_loc_);
  EXPECT_EQ(disk_locator.length(), mem_loc->disk_locator_size_);

  ObString wrapped_disk_locator;
  ASSERT_EQ(OB_SUCCESS, locator.get_disk_locator(wrapped_disk_locator));
  ASSERT_EQ(disk_locator.length(), wrapped_disk_locator.length());
  ASSERT_EQ(0, MEMCMP(disk_locator.ptr(), wrapped_disk_locator.ptr(), disk_locator.length()));

  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, locator.get_inrow_data(inrow_data));
  expect_ob_string_eq(inrow_data, payload, STRLEN(payload));
}

TEST_F(TestLobLocatorBuilder, fuse_persisted_default_keeps_single_disk_header)
{
  expect_fused_persisted_default("hello", false/*is_systable*/);
  expect_fused_persisted_default("", false/*is_systable*/);
  expect_fused_persisted_default("hello", true/*is_systable*/);
  expect_fused_persisted_default("", true/*is_systable*/);
}

TEST_F(TestLobLocatorBuilder, disk_locator_builder_round_trip)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 locator;
  ObString disk_locator;
  ObLobId expected_lob_id;
  expected_lob_id.assign(1, 1001, 2002);
  const transaction::ObTxSEQ expected_seq_no(10, 0);
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, locator, disk_locator));

  ASSERT_TRUE(locator.is_lob_disk_locator());
  ASSERT_EQ(sizeof(ObLobCommon) + sizeof(ObLobId) + sizeof(int64_t),
            disk_locator.length());
  EXPECT_EQ(0,
            MEMCMP(expected_lob_id.get_string().ptr(),
                   disk_locator.ptr() + sizeof(ObLobCommon),
                   sizeof(ObLobId)));
  uint64_t actual_seq_no_raw = 0;
  MEMCPY(&actual_seq_no_raw,
         disk_locator.ptr() + sizeof(ObLobCommon) + sizeof(ObLobId),
         sizeof(actual_seq_no_raw));
  EXPECT_EQ(expected_seq_no.cast_to_int(), actual_seq_no_raw);

  ObLobDiskLocatorWrapper wrapper;
  ASSERT_EQ(OB_SUCCESS, wrapper.init(disk_locator.ptr(), disk_locator.length()));
  ASSERT_TRUE(wrapper.is_valid());
  EXPECT_TRUE(wrapper.is_outrow());
  EXPECT_EQ(expected_lob_id, wrapper.get_lob_id());
  ASSERT_NE(nullptr, wrapper.get_lob_id_ptr());
  EXPECT_EQ(expected_lob_id, *wrapper.get_lob_id_ptr());
  EXPECT_EQ(expected_seq_no, wrapper.get_seq_no());

  ObLobDiskLocatorWrapper invalid_wrapper;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            invalid_wrapper.init(disk_locator.ptr(), disk_locator.length() - 1));
  EXPECT_FALSE(invalid_wrapper.is_valid());
  EXPECT_EQ(nullptr, invalid_wrapper.get_lob_id_ptr());
}

TEST_F(TestLobLocatorBuilder, unaligned_outrow_disk_locator_bytes)
{
  static_assert(ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE == 33,
                "outrow disk locator format must remain 33 bytes");
  alignas(int64_t) char disk_locator[ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE];
  MEMSET(disk_locator, 0, sizeof(disk_locator));
  ObLobCommon *lob_common = new (disk_locator) ObLobCommon();
  lob_common->in_row_ = 0;
  ObLobId expected_lob_id;
  expected_lob_id.assign(1, 1001, 2002);
  MEMCPY(lob_common->buffer_, expected_lob_id.get_string().ptr(), sizeof(ObLobId));
  const transaction::ObTxSEQ expected_seq_no(10, 0);
  const uint64_t expected_seq_no_raw = expected_seq_no.cast_to_int();
  MEMCPY(lob_common->buffer_ + sizeof(ObLobId),
         &expected_seq_no_raw,
         sizeof(expected_seq_no_raw));

  ObLobDiskLocatorWrapper wrapper;
  ASSERT_EQ(OB_SUCCESS, wrapper.init(disk_locator, sizeof(disk_locator)));
  EXPECT_EQ(expected_lob_id, wrapper.get_lob_id());
  EXPECT_EQ(expected_seq_no, wrapper.get_seq_no());

  ObLobLocatorV2 locator(disk_locator, sizeof(disk_locator));
  char print_buf[4096] = {};
  ASSERT_GT(locator.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_NE(nullptr, std::strstr(print_buf, "seq_no"));
}

TEST_F(TestLobLocatorBuilder, wrap_outrow_disk_locator_no_extern)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 disk_locator_obj;
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, disk_locator_obj, disk_locator));

  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));

  ObMemLobCommon *mem_loc = locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_loc);
  EXPECT_FALSE(mem_loc->lob_common_.in_row_);
  EXPECT_TRUE(mem_loc->lob_common_.is_mem_loc_);
  EXPECT_FALSE(mem_loc->has_extern());
  EXPECT_FALSE(mem_loc->has_payload());
  EXPECT_EQ(disk_locator.length(), mem_loc->disk_locator_size_);

  ObMemLobExternHeader *extern_header = nullptr;
  EXPECT_EQ(OB_ERR_NULL_VALUE, locator.get_extern_header(extern_header));
}

TEST_F(TestLobLocatorBuilder, append_delta_is_operation_not_lob_value)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, make_append_delta_locator(allocator, "abc", locator));
  ASSERT_TRUE(locator.is_valid());
  ASSERT_TRUE(locator.is_append_delta_lob());

  const ObMemLobExternHeader *extern_header = nullptr;
  ObString append_data;
  ASSERT_EQ(OB_SUCCESS, locator.get_append_delta(extern_header, append_data));
  ASSERT_NE(nullptr, extern_header);
  EXPECT_EQ(CS_TYPE_BINARY, extern_header->cs_type_);
  expect_ob_string_eq(append_data, "abc", 3);

  ObString inrow_data;
  EXPECT_EQ(OB_NOT_SUPPORTED, locator.get_inrow_data(inrow_data));
  int64_t byte_len = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, locator.get_lob_inrow_data_byte_len(byte_len));
}

TEST_F(TestLobLocatorBuilder, append_delta_datum_length_boundary)
{
  const int64_t max_payload_size =
      OB_MAX_LONGTEXT_LENGTH - ObLobLocatorV2::get_append_delta_header_len();
  int64_t locator_size = -1;

  static_assert(sizeof(ObMemLobCommon) == 24, "unexpected mem lob header size");
  static_assert(sizeof(ObMemLobExternHeader) == 24,
                "unexpected mem lob extern header size");
  ASSERT_EQ(48, ObLobLocatorV2::get_append_delta_header_len());
  ASSERT_EQ(536870863, max_payload_size);

  // 零字节 payload 会生成仅含头部的无操作 delta。
  EXPECT_EQ(OB_SUCCESS,
            ObLobLocatorV2::calc_append_delta_locator_size(0, locator_size));
  EXPECT_EQ(ObLobLocatorV2::get_append_delta_header_len(), locator_size);

  // 最大有效 payload 生成的长度恰好等于 Datum 的 29 位长度上限。
  EXPECT_EQ(OB_SUCCESS,
            ObLobLocatorV2::calc_append_delta_locator_size(max_payload_size,
                                                           locator_size));
  EXPECT_EQ(OB_MAX_LONGTEXT_LENGTH, locator_size);

  // payload 再增加一个字节会使完整 locator 超出 Datum::len_ 的表示范围。
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObLobLocatorV2::calc_append_delta_locator_size(max_payload_size + 1,
                                                           locator_size));
  EXPECT_EQ(0, locator_size);
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObLobLocatorV2::calc_append_delta_locator_size(OB_MAX_LONGTEXT_LENGTH,
                                                           locator_size));
  EXPECT_EQ(0, locator_size);
}

TEST_F(TestLobLocatorBuilder, append_delta_validates_layout)
{
  ObArenaAllocator allocator;
  const ObMemLobExternHeader *extern_header = nullptr;
  ObString append_data;

  // 空 append delta 合法，表示无操作。
  ObLobLocatorV2 empty_locator;
  ASSERT_EQ(OB_SUCCESS, make_append_delta_locator(allocator, "", empty_locator));
  EXPECT_TRUE(empty_locator.is_valid());
  EXPECT_TRUE(empty_locator.is_append_delta_lob());
  EXPECT_EQ(OB_SUCCESS, empty_locator.get_append_delta(extern_header, append_data));
  ASSERT_NE(nullptr, extern_header);
  EXPECT_EQ(CS_TYPE_BINARY, extern_header->cs_type_);
  EXPECT_TRUE(append_data.empty());

  // 拒绝 payload 范围与其所属缓冲区不匹配的 locator。
  ObLobLocatorV2 invalid_locator;
  ASSERT_EQ(OB_SUCCESS, make_append_delta_locator(allocator, "abc", invalid_locator));
  ObMemLobCommon *mem_lob = invalid_locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_lob);
  const uint32_t valid_payload_offset = mem_lob->payload_offset_;
  ++mem_lob->payload_offset_;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            invalid_locator.get_append_delta(extern_header, append_data));
  EXPECT_EQ(nullptr, extern_header);
  mem_lob->payload_offset_ = valid_payload_offset;

  // APPEND_DELTA_LOB 只携带排序规则元数据；行身份属于持久化 locator，
  // 该操作头必须拒绝行身份信息。
  ObMemLobExternHeader *mutable_header =
      reinterpret_cast<ObMemLobExternHeader *>(mem_lob->buffer_);
  mutable_header->has_row_key_ = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            invalid_locator.get_append_delta(extern_header, append_data));
  EXPECT_EQ(nullptr, extern_header);
}

}
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_locator_builder.log*");
  OB_LOGGER.set_file_name("test_lob_locator_builder.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

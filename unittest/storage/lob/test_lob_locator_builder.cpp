/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>
#include <string>

#define protected public
#define private public

#include "lib/allocator/page_arena.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_lob_access_utils.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_aux_manager.h"

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
  EXPECT_TRUE(wrapper.is_outline_lob());
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

TEST_F(TestLobLocatorBuilder, mem_external_predicate_ignores_empty_and_inrow_locators)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 empty_locator;
  EXPECT_FALSE(empty_locator.is_mem_external_lob());

  // A valid empty Disk locator has a header; test it separately from the zero-length sentinel.
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, "", disk_locator));
  ObLobLocatorV2 disk_locator_obj(disk_locator);
  EXPECT_FALSE(disk_locator_obj.is_mem_external_lob());
  ObLobLocatorV2 zero_size_locator(disk_locator.ptr(), 0);
  EXPECT_FALSE(zero_size_locator.is_mem_external_lob());

  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 mem_locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(mem_locator));
  EXPECT_FALSE(mem_locator.is_mem_external_lob());
}

TEST_F(TestLobLocatorBuilder, text_read_rejects_outline_lob_disk_locator)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 disk_locator_obj;
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, disk_locator_obj, disk_locator));
  uint64_t byte_length = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLobManager::get_byte_length(disk_locator_obj, byte_length));
  ObTextStringIter iter(ObExtendBLobType, CS_TYPE_BINARY, disk_locator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(0, nullptr, &allocator));
  EXPECT_FALSE(disk_locator_obj.is_mem_external_lob());

  // The storage wrapper validates the complete Disk layout.
  ObLobLocatorV2 truncated_disk(disk_locator.ptr(), disk_locator.length() - 1);
  EXPECT_FALSE(truncated_disk.is_mem_external_lob());
  ObLobDiskLocatorWrapper wrapper;
  EXPECT_EQ(OB_INVALID_ARGUMENT, wrapper.init(truncated_disk.ptr_, truncated_disk.size_));

  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 mem_locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.build(mem_locator));
  EXPECT_FALSE(mem_locator.is_mem_external_lob());
}

TEST_F(TestLobLocatorBuilder, mem_external_predicate_handles_invalid_headers)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 null_locator(nullptr, sizeof(ObLobCommon));
  EXPECT_FALSE(null_locator.is_mem_external_lob());

  ObLobCommon invalid_common;
  invalid_common.version_ = 0;
  ObLobLocatorV2 invalid_disk(reinterpret_cast<char *>(&invalid_common), sizeof(invalid_common));
  EXPECT_FALSE(invalid_disk.is_mem_external_lob());

  ObMemLobCommon invalid_mem(ObMemLobType::INVALID_LOB);
  ObLobLocatorV2 invalid_mem_locator(reinterpret_cast<char *>(&invalid_mem), sizeof(invalid_mem));
  EXPECT_FALSE(invalid_mem_locator.is_mem_external_lob());

  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 mem_locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(mem_locator));

  // Type predicates safely return false if the common or Extern header is truncated.
  const uint32_t truncated_sizes[] = {
      1, sizeof(ObMemLobCommon) - 1,
      sizeof(ObMemLobCommon) + sizeof(ObMemLobExternHeader) - 1};
  for (const uint32_t size : truncated_sizes) {
    ObLobLocatorV2 truncated_header(mem_locator.ptr_, size);
    EXPECT_FALSE(truncated_header.is_mem_external_lob());
  }

  // The Extern body must fit before its type flag can be read.
  ObLobLocatorV2 truncated_info(mem_locator.ptr_, mem_locator.size_ - 1);
  EXPECT_FALSE(truncated_info.is_mem_external_lob());
  ObExternalBlobInfo decoded;
  EXPECT_EQ(OB_INVALID_ARGUMENT, truncated_info.get_external_lob_info(decoded));
  mem_locator.get_mem_lob_common()->lob_common_.version_ = 0;
  EXPECT_FALSE(mem_locator.is_mem_external_lob());
}

TEST_F(TestLobLocatorBuilder, external_info_deep_copy_owns_strings)
{
  ObArenaAllocator allocator;
  char url[] = "s3://bucket/object";
  char etag[] = "original-etag";
  ObExternalBlobInfo source;
  source.url_ = ObString::make_string(url);
  source.etag_ = ObString::make_string(etag);
  source.length_ = 7;
  source.kind_ = ObExternalBlobKind::REFERENCE;
  ObExternalBlobInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.deep_copy(allocator, source));
  EXPECT_NE(source.url_.ptr(), copied.url_.ptr());
  EXPECT_NE(source.etag_.ptr(), copied.etag_.ptr());
  MEMSET(url, 'x', STRLEN(url));
  MEMSET(etag, 'x', STRLEN(etag));
  expect_ob_string_eq(copied.url_, "s3://bucket/object", 18);
  expect_ob_string_eq(copied.etag_, "original-etag", 13);
  ASSERT_EQ(OB_SUCCESS, copied.deep_copy(allocator, copied));
  EXPECT_TRUE(copied.is_valid());
  copied.etag_.reset();
  ASSERT_EQ(OB_SUCCESS, source.deep_copy(allocator, copied));
  EXPECT_TRUE(source.etag_.empty());
}

TEST_F(TestLobLocatorBuilder, external_locator_copy_is_self_contained)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator copied_allocator;
  char url[] = "s3://bucket/object";
  char etag[] = "etag";
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string(url);
  info.etag_ = ObString::make_string(etag);
  info.length_ = 7;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(source_allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  // Like rowkey, the locator no longer depends on input strings after build.
  MEMSET(url, 'x', STRLEN(url));
  MEMSET(etag, 'x', STRLEN(etag));
  ObString copied;
  ASSERT_EQ(OB_SUCCESS, ob_write_string(copied_allocator,
      ObString(locator.size_, locator.ptr_), copied));
  source_allocator.reset();
  ObLobLocatorV2 moved(copied);
  ObExternalBlobInfo decoded;
  ASSERT_EQ(OB_SUCCESS, moved.get_external_lob_info(decoded));
  expect_ob_string_eq(decoded.url_, "s3://bucket/object", 18);
  expect_ob_string_eq(decoded.etag_, "etag", 4);
  EXPECT_GE(decoded.url_.ptr(), copied.ptr());
  EXPECT_LE(decoded.url_.ptr() + decoded.url_.length(), copied.ptr() + copied.length());
}

TEST_F(TestLobLocatorBuilder, external_locator_variable_strings_and_bounds)
{
  ObArenaAllocator allocator;
  const int64_t url_lengths[] = {18, OB_MAX_URI_LENGTH - 1};
  const int64_t etag_lengths[] = {0, 1, OB_ETAG_CAPACITY - 1};
  for (int64_t url_length : url_lengths) {
    for (int64_t etag_length : etag_lengths) {
      std::string url(url_length, 'u');
      std::string etag(etag_length, 'e');
      ObExternalBlobInfo info;
      info.url_.assign_ptr(url.data(), url.length());
      info.etag_.assign_ptr(etag.data(), etag.length());
      info.length_ = 10;
      info.kind_ = ObExternalBlobKind::REFERENCE;
      ObMemLobLocatorBuilder builder;
      ObLobLocatorV2 locator;
      ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
      ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
      ASSERT_EQ(OB_SUCCESS, builder.build(locator));
      EXPECT_EQ(ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN
          + ObLobLocatorV2::MEM_LOB_EXTERN_HEADER_LEN + ObExternalBlobInfoLayout::get_size(info),
          locator.size_);
      ObExternalBlobInfo decoded;
      ASSERT_EQ(OB_SUCCESS, locator.get_external_lob_info(decoded));
      EXPECT_EQ(info.url_, decoded.url_);
      EXPECT_EQ(info.etag_, decoded.etag_);
      ObMemLobExternHeader *header = nullptr;
      ASSERT_EQ(OB_SUCCESS, locator.get_extern_header(header));
      auto *layout = reinterpret_cast<ObExternalBlobInfoLayout *>(header->buffer_);
      const uint32_t original_length = layout->url_length_;
      layout->url_length_ = UINT32_MAX;
      EXPECT_TRUE(locator.is_valid());
      EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_external_lob_info(decoded));
      char log_buf[1024] = {};
      const int64_t printed_length = locator.to_string(log_buf, sizeof(log_buf));
      EXPECT_GT(printed_length, 0);
      EXPECT_LT(printed_length, sizeof(log_buf));
      layout->url_length_ = original_length;
      layout->etag_length_ = OB_ETAG_CAPACITY;
      EXPECT_TRUE(locator.is_valid());
      EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_external_lob_info(decoded));
      layout->etag_length_ = etag_length;
      EXPECT_TRUE(locator.is_valid());
    }
  }
}

TEST_F(TestLobLocatorBuilder, external_locator_to_string_includes_info_at_buffer_end)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/to-string-boundary");
  info.etag_ = ObString::make_string("boundary-etag");
  info.offset_ = 7;
  info.length_ = 10;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_valid());
  ASSERT_TRUE(locator.is_external_temp_lob());

  // No trailing Disk locator or payload: the External descriptor ends at the buffer boundary.
  const int64_t offset = ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN
                         + ObLobLocatorV2::MEM_LOB_EXTERN_HEADER_LEN;
  ASSERT_EQ(ObExternalBlobInfoLayout::get_size(info), locator.size_ - offset);
  char print_buf[4096] = {};
  const int64_t printed_length = locator.to_string(print_buf, sizeof(print_buf));
  ASSERT_GT(printed_length, 0);
  ASSERT_LT(printed_length, sizeof(print_buf));

  // A strict '<' bound would omit the descriptor, including its URL, ETag and range.
  char info_buf[1024] = {};
  const int64_t info_length = info.to_string(info_buf, sizeof(info_buf));
  ASSERT_GT(info_length, 0);
  ASSERT_LT(info_length, sizeof(info_buf));
  EXPECT_NE(nullptr, std::strstr(print_buf, info_buf)) << print_buf;
}

TEST_F(TestLobLocatorBuilder, external_locator_snapshot_after_variable_strings)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.etag_ = ObString::make_string("e");
  info.length_ = 10;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  // Append snapshot after variable-length External data; verify skipping uses the section length.
  const int64_t snapshot_size = sizeof(ObMemLobReadSnapshot) + 4;
  char *buf = static_cast<char *>(allocator.alloc(locator.size_ + snapshot_size));
  ASSERT_NE(nullptr, buf);
  MEMCPY(buf, locator.ptr_, locator.size_);
  auto *snapshot = new (buf + locator.size_) ObMemLobReadSnapshot();
  snapshot->size_ = 4;
  MEMCPY(snapshot->buffer_, "snap", 4);
  ObLobLocatorV2 with_snapshot(buf, locator.size_ + snapshot_size);
  ObMemLobExternHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, with_snapshot.get_extern_header(header));
  header->has_read_snapshot_ = true;
  header->body_size_ += snapshot_size;
  ASSERT_TRUE(with_snapshot.is_valid());
  ObString decoded;
  ASSERT_EQ(OB_SUCCESS, with_snapshot.get_read_snapshot_data(decoded));
  expect_ob_string_eq(decoded, "snap", 4);

  const uint32_t external_body_size = header->body_size_ - snapshot_size;
  header->body_size_ = external_body_size + sizeof(ObMemLobReadSnapshot) - 1;
  ObLobLocatorV2 truncated_snapshot(buf, locator.size_ + sizeof(ObMemLobReadSnapshot) - 1);
  EXPECT_TRUE(truncated_snapshot.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, truncated_snapshot.get_read_snapshot_data(decoded));
  EXPECT_TRUE(decoded.empty());

  header->body_size_ = external_body_size + snapshot_size;
  snapshot->size_ = 5;
  EXPECT_TRUE(with_snapshot.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, with_snapshot.get_read_snapshot_data(decoded));
  EXPECT_TRUE(decoded.empty());
}

TEST_F(TestLobLocatorBuilder, external_blob_info_validates_and_updates_in_place)
{
  ObExternalBlobInfo info;
  EXPECT_FALSE(info.is_valid());
  EXPECT_TRUE(info.etag_.empty());

  info.url_ = ObString::make_string("s3://bucket/object");
  info.offset_ = 1;
  info.length_ = 2;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  EXPECT_TRUE(info.is_valid());

  info.etag_ = ObString::make_string("original-etag");
  info.offset_ = 4;
  info.length_ = 5;
  EXPECT_EQ(0, info.url_.compare(ObString::make_string("s3://bucket/object")));
  EXPECT_EQ(4, info.offset_);
  EXPECT_EQ(5, info.length_);
  EXPECT_EQ(0, info.etag_.compare(
                   ObString::make_string("original-etag")));
  EXPECT_TRUE(info.is_valid());

  info.offset_ = UINT64_MAX;
  EXPECT_FALSE(info.is_valid());

  info.offset_ = 0;
  char long_url[OB_MAX_URI_LENGTH];
  MEMSET(long_url, 'x', sizeof(long_url));
  info.url_.assign_ptr(long_url, sizeof(long_url));
  EXPECT_FALSE(info.is_valid());

  info.url_ = ObString::make_string("s3://bucket/object");
  char long_etag[OB_ETAG_CAPACITY];
  MEMSET(long_etag, 'x', sizeof(long_etag));
  info.etag_.assign_ptr(long_etag, sizeof(long_etag));
  EXPECT_FALSE(info.is_valid());
  ObArenaAllocator allocator;
  ObMemLobLocatorBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  EXPECT_EQ(OB_INVALID_ARGUMENT, builder.set_external_lob_info(info));
}

TEST_F(TestLobLocatorBuilder, internal_extend_blob_read_keeps_content)
{
  ObArenaAllocator allocator;
  ObString locator;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, "hello", locator));
  ObTextStringIter iter(ObExtendBLobType, CS_TYPE_BINARY, locator);
  ASSERT_EQ(OB_SUCCESS, iter.init(0, nullptr, &allocator));
  ObString data;
  ASSERT_EQ(OB_SUCCESS, iter.get_full_data(data));
  expect_ob_string_eq(data, "hello", 5);
}

TEST_F(TestLobLocatorBuilder, read_context_locator_build_and_rebuild)
{
  ObArenaAllocator allocator;
  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, "hello", inrow_data));
  ObLobLocatorV2 inrow_locator(inrow_data);
  ObLobLocatorV2 outrow_locator;
  ObString outrow_data;
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, outrow_locator, outrow_data));
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 external_locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(external_locator));

  ObLobReadContext read_ctx;
  EXPECT_TRUE(read_ctx.src_loc_.is_null());
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(inrow_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(read_ctx.src_loc_.is_inrow());
  ObString payload;
  ASSERT_EQ(OB_SUCCESS, read_ctx.src_loc_.get_inrow_data(payload));
  expect_ob_string_eq(payload, "hello", 5);
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(outrow_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(read_ctx.src_loc_.is_outline_lob_disk_locator());
  EXPECT_TRUE(read_ctx.src_loc_wrapper_.is_valid());
  ASSERT_TRUE(external_locator.is_external_temp_lob());
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(external_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(read_ctx.src_loc_.is_external_temp_lob());
  EXPECT_FALSE(read_ctx.src_loc_wrapper_.is_valid());
  EXPECT_TRUE(read_ctx.src_loc_.is_mem_external_lob());
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(outrow_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(read_ctx.src_loc_.is_outline_lob_disk_locator());
  EXPECT_TRUE(read_ctx.src_loc_wrapper_.is_valid());
  EXPECT_FALSE(read_ctx.src_loc_.is_mem_external_lob());

  // Keep the current input and clear the previous Disk wrapper when rebuilding fails.
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(inrow_locator, allocator, 0, UINT64_MAX));
  ObLobLocatorV2 truncated(outrow_data.ptr(), outrow_data.length() - 1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, read_ctx.build(truncated, allocator, 0, UINT64_MAX));
  EXPECT_EQ(truncated.ptr_, read_ctx.src_loc_.ptr_);
  EXPECT_EQ(truncated.size_, read_ctx.src_loc_.size_);
  EXPECT_FALSE(read_ctx.src_loc_wrapper_.is_valid());
  ASSERT_EQ(OB_SUCCESS, read_ctx.build(external_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(read_ctx.src_loc_.is_external_temp_lob());
}

TEST_F(TestLobLocatorBuilder, read_context_locator_assign_and_failure)
{
  ObArenaAllocator allocator;
  ObString inrow_data;
  ASSERT_EQ(OB_SUCCESS, make_inrow_disk_locator(allocator, "hello", inrow_data));
  ObLobLocatorV2 inrow_locator(inrow_data);
  ObLobReadContext source;
  ObLobReadContext copied;
  ASSERT_EQ(OB_SUCCESS, source.build(inrow_locator, allocator, 0, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, copied.assign(source));
  EXPECT_TRUE(copied.src_loc_.is_inrow());
  ASSERT_EQ(OB_SUCCESS, copied.assign(copied));
  ObString payload;
  ASSERT_EQ(OB_SUCCESS, copied.src_loc_.get_inrow_data(payload));
  expect_ob_string_eq(payload, "hello", 5);

  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 external_locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(external_locator));
  ASSERT_EQ(OB_SUCCESS, source.build(external_locator, allocator, 0, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, copied.assign(source));
  EXPECT_TRUE(copied.src_loc_.is_external_temp_lob());
  ASSERT_EQ(OB_SUCCESS, copied.assign(copied));
  EXPECT_TRUE(copied.src_loc_.is_mem_external_lob());

  ObLobLocatorV2 outrow_locator;
  ObString outrow_data;
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, outrow_locator, outrow_data));
  ASSERT_EQ(OB_SUCCESS, source.build(outrow_locator, allocator, 0, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, copied.assign(source));
  EXPECT_TRUE(copied.src_loc_.is_outline_lob_disk_locator());
  EXPECT_TRUE(copied.src_loc_wrapper_.is_valid());

  // Truncate source so assign cannot rebuild Disk wrapper; the target must discard its old input.
  ASSERT_TRUE(source.src_loc_wrapper_.is_valid());
  --source.src_loc_.size_;
  ASSERT_EQ(OB_SUCCESS, copied.build(inrow_locator, allocator, 0, UINT64_MAX));
  EXPECT_TRUE(copied.src_loc_.is_inrow());
  EXPECT_EQ(OB_INVALID_ARGUMENT, copied.assign(source));
  EXPECT_EQ(source.src_loc_.ptr_, copied.src_loc_.ptr_);
  EXPECT_EQ(source.src_loc_.size_, copied.src_loc_.size_);
  EXPECT_FALSE(copied.src_loc_wrapper_.is_valid());
  ASSERT_EQ(OB_SUCCESS, source.build(outrow_locator, allocator, 0, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, copied.assign(source));
  EXPECT_TRUE(copied.src_loc_wrapper_.is_valid());
}

TEST_F(TestLobLocatorBuilder, external_builder_init_and_reuse)
{
  ObArenaAllocator allocator;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  EXPECT_EQ(OB_INIT_TWICE, builder.init(allocator));
  EXPECT_EQ(OB_INVALID_ARGUMENT, builder.build(locator));

  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  EXPECT_TRUE(locator.is_external_temp_lob());

  builder.reset();
  EXPECT_EQ(OB_NOT_INIT, builder.build(locator));
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  info.length_ = 2;
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  EXPECT_TRUE(locator.is_external_temp_lob());
  ObExternalBlobInfo decoded;
  ASSERT_EQ(OB_SUCCESS, locator.get_external_lob_info(decoded));
  EXPECT_EQ(info.length_, decoded.length_);
}

TEST_F(TestLobLocatorBuilder, direct_external_memory_locator_round_trip)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo source_info;
  source_info.url_ = ObString::make_string("s3://bucket/object");
  const char *etag = "direct-\\\"etag";
  source_info.etag_ = ObString::make_string(etag);
  source_info.offset_ = 3 * 1024 * 1024;
  source_info.length_ = 10 * 1024 * 1024;
  source_info.kind_ = ObExternalBlobKind::REFERENCE;
  ASSERT_TRUE(source_info.is_valid());

  ObMemLobLocatorBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(source_info));
  // After copying the descriptor, changing the source ETag must not affect the built result.
  source_info.etag_.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            builder.set_extern_info(200, 3, CS_TYPE_UTF8MB4_GENERAL_CI));
  ASSERT_EQ(OB_SUCCESS, builder.set_extern_info(200, 3, CS_TYPE_BINARY));
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, builder.set_location_info(300, 4, addr));
  ASSERT_EQ(OB_SUCCESS, builder.set_retry_info(true, false, 500));
  ASSERT_EQ(OB_SUCCESS, builder.set_rowkey(ObString::make_string("rk")));

  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_valid());
  EXPECT_TRUE(locator.is_external_temp_lob());
  EXPECT_FALSE(locator.is_persist_lob());
  ASSERT_TRUE(locator.is_mem_external_lob());
  ASSERT_FALSE(locator.is_lob_disk_locator());
  ASSERT_FALSE(locator.has_inrow_data());

  ObMemLobCommon *mem_lob = locator.get_mem_lob_common();
  ASSERT_NE(nullptr, mem_lob);
  EXPECT_EQ(0, mem_lob->disk_locator_size_);
  EXPECT_FALSE(mem_lob->has_payload());
  // Neither overload may treat the end of a temporary External descriptor as a Disk locator.
  ObLobCommon *disk_lob = reinterpret_cast<ObLobCommon *>(locator.ptr_);
  EXPECT_EQ(OB_ERR_NULL_VALUE, locator.get_disk_locator(disk_lob));
  EXPECT_EQ(nullptr, disk_lob);
  ObString disk_locator = ObString::make_string("previous value");
  EXPECT_EQ(OB_ERR_NULL_VALUE, locator.get_disk_locator(disk_locator));
  expect_ob_string_eq(disk_locator, "previous value", STRLEN("previous value"));

  ObMemLobExternHeader *extern_header = nullptr;
  ASSERT_EQ(OB_SUCCESS, locator.get_extern_header(extern_header));
  ASSERT_TRUE(extern_header->is_external_lob());

  ObExternalBlobInfo decoded_info;
  ASSERT_EQ(OB_SUCCESS, locator.get_external_lob_info(decoded_info));
  ASSERT_TRUE(decoded_info.is_valid());
  EXPECT_EQ(0, decoded_info.url_.compare(source_info.url_));
  expect_ob_string_eq(decoded_info.etag_, etag, STRLEN(etag));
  EXPECT_EQ(source_info.offset_, decoded_info.offset_);
  EXPECT_EQ(source_info.length_, decoded_info.length_);
  EXPECT_EQ(source_info.kind_, decoded_info.kind_);

  ObMemLobLocationInfo *location_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, locator.get_location_info(location_info));
  ASSERT_NE(nullptr, location_info);
  EXPECT_EQ(300, location_info->tablet_id_);
  EXPECT_EQ(4, location_info->ls_id_);
  EXPECT_EQ(addr, location_info->addr_);
  ObMemLobRetryInfo *retry_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, locator.get_retry_info(retry_info));
  ASSERT_NE(nullptr, retry_info);
  EXPECT_TRUE(retry_info->select_leader_);
  EXPECT_FALSE(retry_info->read_latest_);
  EXPECT_EQ(500, retry_info->timeout_);
  ObString rowkey;
  ASSERT_EQ(OB_SUCCESS, locator.get_rowkey(rowkey));
  expect_ob_string_eq(rowkey, "rk", 2);
}

TEST_F(TestLobLocatorBuilder,
       persisted_external_memory_locator_carries_info_and_disk_locator)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 disk_locator_obj;
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS,
            make_outrow_disk_locator(allocator, disk_locator_obj, disk_locator));
  ASSERT_EQ(ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE, disk_locator.length());
  ASSERT_FALSE(disk_locator_obj.is_mem_external_lob());

  ObExternalBlobInfo source_info;
  source_info.url_ = ObString::make_string("s3://bucket/persisted-object");
  source_info.etag_ = ObString::make_string("persisted-etag");
  source_info.offset_ = 1024;
  source_info.length_ = 4096;
  source_info.kind_ = ObExternalBlobKind::REFERENCE;

  ObMemLobLocatorBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk_locator, false));
  ASSERT_EQ(OB_SUCCESS, builder.set_extern_info(200, 3, CS_TYPE_BINARY));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(source_info));
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_valid());
  EXPECT_FALSE(locator.is_external_temp_lob());
  EXPECT_TRUE(locator.is_persist_lob());
  ASSERT_TRUE(locator.is_mem_external_lob());

  // External and Internal share the persisted type; the extern descriptor distinguishes references.
  EXPECT_TRUE(ObMemLobType::PERSISTENT_LOB == locator.get_mem_lob_common()->type_);
  EXPECT_FALSE(locator.is_full_temp_lob());
  EXPECT_FALSE(locator.is_append_delta_lob());

  ObMemLobExternHeader *extern_header = nullptr;
  ASSERT_EQ(OB_SUCCESS, locator.get_extern_header(extern_header));
  ASSERT_TRUE(extern_header->is_external_lob());
  EXPECT_EQ(ObExternalBlobInfoLayout::get_size(source_info),
            static_cast<uint32_t>(extern_header->body_size_));

  ObExternalBlobInfo decoded_info;
  ASSERT_EQ(OB_SUCCESS, locator.get_external_lob_info(decoded_info));
  ASSERT_TRUE(decoded_info.is_valid());
  EXPECT_EQ(0, decoded_info.url_.compare(source_info.url_));
  EXPECT_EQ(source_info.offset_, decoded_info.offset_);
  EXPECT_EQ(source_info.length_, decoded_info.length_);
  EXPECT_EQ(source_info.kind_, decoded_info.kind_);
  source_info.etag_.reset();
  builder.reset();
  expect_ob_string_eq(decoded_info.etag_, "persisted-etag", 14);

  ObString persisted_disk_locator;
  ASSERT_EQ(OB_SUCCESS, locator.get_disk_locator(persisted_disk_locator));
  EXPECT_EQ(ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE,
            persisted_disk_locator.length());
  EXPECT_EQ(0, MEMCMP(disk_locator.ptr(), persisted_disk_locator.ptr(),
                      disk_locator.length()));

  extern_header->has_read_snapshot_ = 1;
  EXPECT_TRUE(locator.is_valid());
  ObString snapshot_data;
  EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_read_snapshot_data(snapshot_data));
  extern_header->has_read_snapshot_ = 0;
  EXPECT_TRUE(locator.is_valid());
}

TEST_F(TestLobLocatorBuilder, external_memory_locator_requires_complete_info)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo source_info;
  source_info.url_ = ObString::make_string("s3://bucket/object");
  source_info.offset_ = 0;
  source_info.length_ = 1;
  source_info.kind_ = ObExternalBlobKind::REFERENCE;

  ObString inrow_disk_locator;
  ASSERT_EQ(OB_SUCCESS,
            make_inrow_disk_locator(allocator, "data", inrow_disk_locator));
  ObMemLobLocatorBuilder inrow_builder;
  ASSERT_EQ(OB_SUCCESS, inrow_builder.init(allocator, inrow_disk_locator, false));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            inrow_builder.set_external_lob_info(source_info));

  ObLobLocatorV2 outrow_locator;
  ObString outrow_disk_locator;
  ASSERT_EQ(OB_SUCCESS,
            make_outrow_disk_locator(allocator, outrow_locator,
                                     outrow_disk_locator));

  ObMemLobLocatorBuilder nonbinary_builder;
  ASSERT_EQ(OB_SUCCESS,
            nonbinary_builder.init(allocator, outrow_disk_locator, false));
  ASSERT_EQ(OB_SUCCESS,
            nonbinary_builder.set_extern_info(
                200, 3, CS_TYPE_UTF8MB4_GENERAL_CI));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            nonbinary_builder.set_external_lob_info(source_info));

  ObMemLobLocatorBuilder missing_info_builder;
  ASSERT_EQ(OB_SUCCESS,
            missing_info_builder.init(allocator, outrow_disk_locator, false));
  ASSERT_EQ(OB_SUCCESS,
            missing_info_builder.set_extern_info(200, 3, CS_TYPE_BINARY));
  ObLobLocatorV2 missing_info_locator;
  ASSERT_EQ(OB_SUCCESS, missing_info_builder.build(missing_info_locator));
  ObMemLobExternHeader *extern_header = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            missing_info_locator.get_extern_header(extern_header));
  ASSERT_NE(nullptr, extern_header);
  extern_header->is_external_lob_ = 1;
  EXPECT_TRUE(missing_info_locator.is_valid());
  ObExternalBlobInfo decoded_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, missing_info_locator.get_external_lob_info(decoded_info));
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
  mutable_header->has_row_key_ = false;
  mutable_header->is_external_lob_ = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            invalid_locator.get_append_delta(extern_header, append_data));
  EXPECT_EQ(nullptr, extern_header);
}

TEST_F(TestLobLocatorBuilder, resolving_external_length_keeps_original_reference)
{
  ObArenaAllocator allocator;
  ObLobLocatorV2 disk_loc;
  ObString disk;
  ASSERT_EQ(OB_SUCCESS, make_outrow_disk_locator(allocator, disk_loc, disk));
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.offset_ = 0;
  info.length_ = 0;
  info.has_length_ = false;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator, disk, false));
  ASSERT_EQ(OB_SUCCESS, builder.set_extern_info(200, 3, CS_TYPE_BINARY));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ObLobLocatorV2 source;
  ASSERT_EQ(OB_SUCCESS, builder.build(source));
  const std::string original(source.ptr_, source.size_);

  // Resolved read lengths belong to the context, not the source reference.
  ObLobManager lob_manager(OB_SYS_TENANT_ID);
  lob_manager.is_inited_ = true;
  ObLobReadContext read_ctx;
  read_ctx.src_loc_ = source;
  ASSERT_EQ(OB_SUCCESS, read_ctx.init_src_loc_wrapper_());
  EXPECT_EQ(UINT64_MAX, read_ctx.byte_size_);
  EXPECT_EQ(nullptr, read_ctx.allocator_);
  EXPECT_EQ(nullptr, read_ctx.access_ctx_);
  uint64_t byte_len = 52;
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_byte_length(read_ctx, byte_len));
  EXPECT_EQ(52, byte_len);
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_char_length(read_ctx, byte_len));
  EXPECT_EQ(52, byte_len);
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_external_lob_byte_length_(read_ctx, byte_len));
  EXPECT_EQ(52, byte_len);
  ASSERT_EQ(OB_SUCCESS, ObLobManager::get_byte_length(source, byte_len));
  EXPECT_EQ(0, byte_len);

  // Reuse both nonempty and empty HEAD results without an allocator or provider.
  const uint64_t resolved_lengths[] = {52, 0};
  for (const uint64_t resolved_length : resolved_lengths) {
    read_ctx.byte_size_ = resolved_length;
    byte_len = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, lob_manager.get_byte_length(read_ctx, byte_len));
    EXPECT_EQ(resolved_length, byte_len);
    byte_len = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, lob_manager.get_char_length(read_ctx, byte_len));
    EXPECT_EQ(resolved_length, byte_len);
    byte_len = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, lob_manager.get_external_lob_byte_length_(read_ctx, byte_len));
    EXPECT_EQ(resolved_length, byte_len);
    EXPECT_EQ(resolved_length, read_ctx.byte_size_);
    EXPECT_EQ(source.ptr_, read_ctx.src_loc_.ptr_);
  }
  EXPECT_EQ(0, MEMCMP(source.ptr_, original.data(), original.size()));
  ObExternalBlobInfo source_info;
  ASSERT_EQ(OB_SUCCESS, source.get_external_lob_info(source_info));
  EXPECT_FALSE(source_info.has_length_);
  EXPECT_EQ(0, source_info.length_);
  ASSERT_TRUE(read_ctx.src_loc_wrapper_.is_valid());
  ObString source_disk;
  ASSERT_EQ(OB_SUCCESS, source.get_disk_locator(source_disk));
  EXPECT_EQ(source_disk.ptr(),
            reinterpret_cast<const char *>(read_ctx.src_loc_wrapper_.get_lob_common()));
  ASSERT_EQ(OB_SUCCESS, ObLobManager::get_byte_length(source, byte_len));
  EXPECT_EQ(0, byte_len);

  ObMemLobLocatorBuilder temp_builder;
  ASSERT_EQ(OB_SUCCESS, temp_builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, temp_builder.set_external_lob_info(info));
  ObLobLocatorV2 temp;
  ASSERT_EQ(OB_SUCCESS, temp_builder.build(temp));
  read_ctx.src_loc_ = temp;
  read_ctx.byte_size_ = 52;
  // A cached length must not make a temporary External readable.
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_byte_length(read_ctx, byte_len));
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_char_length(read_ctx, byte_len));
  EXPECT_EQ(OB_NOT_SUPPORTED, lob_manager.get_external_lob_byte_length_(read_ctx, byte_len));
  ASSERT_EQ(OB_SUCCESS, ObLobManager::get_byte_length(temp, byte_len));
  EXPECT_EQ(0, byte_len);

  // Rebuilding for another locator must discard the previous resolved length.
  for (const uint64_t resolved_length : resolved_lengths) {
    read_ctx.byte_size_ = resolved_length;
    ASSERT_EQ(OB_SUCCESS, read_ctx.build(temp, allocator, 0, UINT64_MAX));
    EXPECT_EQ(UINT64_MAX, read_ctx.byte_size_);
    EXPECT_EQ(temp.ptr_, read_ctx.src_loc_.ptr_);
  }
}

TEST_F(TestLobLocatorBuilder, external_iterator_uses_context_length)
{
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.offset_ = 0;
  info.length_ = 0;
  info.has_length_ = false;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObLobReadContext read_ctx;
  read_ctx.coll_type_ = CS_TYPE_BINARY;
  read_ctx.byte_size_ = 52;
  read_ctx.offset_ = 3;
  read_ctx.len_ = 10;
  ObObjectStorageInfo access_info;
  ObExternalLobQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(read_ctx, info, 1001, 2002, access_info));
  EXPECT_EQ(3, iter.read_start_);
  EXPECT_EQ(13, iter.read_end_);
  EXPECT_EQ(3, iter.read_pos_);
  EXPECT_FALSE(info.has_length_);
  EXPECT_EQ(0, info.length_);
  EXPECT_FALSE(iter.external_info_.has_length_);
  EXPECT_EQ(0, iter.external_info_.length_);
  blocksstable::MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, iter.piece_iter_.get_next(macro_id));
  EXPECT_EQ(0, macro_id.fourth_id());
  EXPECT_EQ(52, macro_id.fifth_id());
  EXPECT_EQ(OB_ITER_END, iter.piece_iter_.get_next(macro_id));

  // A resolved empty object ends without issuing IO.
  iter.reset();
  read_ctx.byte_size_ = 0;
  ASSERT_EQ(OB_SUCCESS, iter.open(read_ctx, info, 1001, 2002, access_info));
  EXPECT_EQ(0, iter.read_start_);
  EXPECT_EQ(0, iter.read_end_);
  ObString block;
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(block));
  EXPECT_EQ(nullptr, iter.io_buffer_);
  EXPECT_FALSE(info.has_length_);
  EXPECT_EQ(0, info.length_);

  iter.reset();
  read_ctx.byte_size_ = UINT64_MAX;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(read_ctx, info, 1001, 2002, access_info));

  // Declared ranges keep their object offset and clamp the requested interval.
  info.offset_ = 7;
  info.length_ = 3;
  info.has_length_ = true;
  read_ctx.byte_size_ = 3;
  read_ctx.offset_ = 1;
  ASSERT_EQ(OB_SUCCESS, iter.open(read_ctx, info, 1001, 2002, access_info));
  EXPECT_EQ(8, iter.read_start_);
  EXPECT_EQ(10, iter.read_end_);
  ASSERT_EQ(OB_SUCCESS, iter.piece_iter_.get_next(macro_id));
  EXPECT_EQ(7, macro_id.fourth_id());
  EXPECT_EQ(3, macro_id.fifth_id());
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

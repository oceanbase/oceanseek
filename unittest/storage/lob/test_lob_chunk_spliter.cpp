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
#include "lib/checksum/ob_crc64.h"
#include "lib/time/ob_time_utility.h"
#include "storage/access/ob_dml_param.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/common/ob_outtable_lob_control_key.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "storage/lob/store/ob_lob_aux_manager.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobChunkSpliter : public ::testing::Test
{
public:
  TestLobChunkSpliter() : lob_manager_(OB_SYS_TENANT_ID) {}
  virtual ~TestLobChunkSpliter() {}
  virtual void SetUp() { lob_manager_.is_inited_ = true; }
  virtual void TearDown() {}

  int init_source_ctx(
      ObLobLocatorV2 &source_locator,
      ObIAllocator &allocator,
      const ObCollationType coll_type,
      const uint64_t source_byte_len,
      ObLobReadContext &read_ctx,
      ObLobSourceReadCtx &source_ctx)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(read_ctx.build(source_locator, allocator, 0, UINT64_MAX))) {
      STORAGE_LOG(WARN, "failed to build source lob query parameter", KR(ret), K(source_locator));
    } else {
      read_ctx.coll_type_ = coll_type;
      read_ctx.byte_size_ = source_byte_len;
      source_ctx.read_ctx_ = &read_ctx;
      source_ctx.lob_manager_ = &lob_manager_;
      source_ctx.source_byte_len_ = source_byte_len;
    }
    return ret;
  }

  void prepare_redo_context(ObLobInsertContext &ctx);

  void prepare_redo_meta(ObIAllocator &allocator, const ObString &data, ObLobMetaInfo &meta);

  ObArenaAllocator external_info_allocator_;
  ObLobManager lob_manager_;
};

// fill an allocator-backed buffer with repeated unit string
static ObString make_data(ObIAllocator &allocator, const char *unit, int64_t repeat)
{
  ObString result;
  const int64_t unit_len = STRLEN(unit);
  const int64_t total = unit_len * repeat;
  char *buf = static_cast<char *>(allocator.alloc(total <= 0 ? 1 : total));
  EXPECT_NE(nullptr, buf);
  for (int64_t i = 0; i < repeat; ++i) {
    MEMCPY(buf + i * unit_len, unit, unit_len);
  }
  result.assign_ptr(buf, static_cast<int32_t>(total));
  return result;
}

//==================== ObLobMetaExtraInfo ====================
TEST_F(TestLobChunkSpliter, external_lob_index_insert_is_not_supported)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.offset_ = 3;
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));

  ObLobInsertContext ctx;
  ctx.src_loc_ = locator;
  ASSERT_EQ(OB_SUCCESS, ctx.init_src_loc_wrapper_());
  ASSERT_FALSE(ctx.src_loc_wrapper_.is_valid());
  uint64_t byte_len = 0;
  ASSERT_EQ(OB_SUCCESS, lob_manager_.get_byte_length(locator, byte_len));
  ASSERT_EQ(1, byte_len);

  // External references must use outrow even with a small declared length; they are not inrow data.
  ctx.is_index_table_ = true;
  bool outrow = false;
  ASSERT_EQ(OB_NOT_SUPPORTED, lob_manager_.insert_before(ctx, outrow));
}

TEST_F(TestLobChunkSpliter, lob_meta_extra_info_old_summary_defaults_to_insert)
{
  const ObString old_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\",\"chunk_count\":3}");
  ObLobMetaExtraInfo extra_info;
  extra_info.last_write_type_ = ObLobLastWriteType::APPEND;
  ObExternalBlobInfo external_lob_info;
  external_lob_info.url_ = ObString::make_string("s3://bucket/old");
  external_lob_info.etag_ = ObString::make_string("old-etag");
  external_lob_info.offset_ = 1;
  external_lob_info.length_ = 2;
  external_lob_info.kind_ = ObExternalBlobKind::REFERENCE;
  ASSERT_EQ(OB_SUCCESS, extra_info.set_external_lob_info(external_info_allocator_,
      external_lob_info));

  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(external_info_allocator_, old_summary));
  ASSERT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
  ASSERT_TRUE(extra_info.lob_chunk_type_.in_table());
  ASSERT_EQ(ObLobLastWriteType::INSERT, extra_info.last_write_type_);
  ASSERT_EQ(3, extra_info.chunk_count_);
  ASSERT_FALSE(extra_info.is_external_lob());
  ASSERT_FALSE(extra_info.external_lob_info_.is_valid());
  ASSERT_TRUE(extra_info.external_lob_info_.etag_.empty());
}

TEST_F(TestLobChunkSpliter, lob_meta_extra_info_legacy_invalid_type_round_trip)
{
  const ObString old_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"INVALID_TYPE\",\"chunk_count\":3}");
  ObLobMetaExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(external_info_allocator_, old_summary));
  ASSERT_TRUE(extra_info.lob_chunk_type_.in_table());
  ASSERT_STREQ("IN_TABLE", extra_info.lob_chunk_type_.to_string());
  ASSERT_EQ(ObLobLastWriteType::INSERT, extra_info.last_write_type_);
  ASSERT_STREQ("INSERT", lob_last_write_type_to_string(extra_info.last_write_type_));

  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(external_info_allocator_, serialized));
  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_TRUE(decoded.lob_chunk_type_.in_table());
  ASSERT_EQ(3, decoded.chunk_count_);
}

TEST_F(TestLobChunkSpliter, lob_meta_extra_info_uses_lob_chunk_type)
{
  ObArenaAllocator allocator;
  ObString serialized;
  ObLobMetaExtraInfo source;
  source.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  source.lob_chunk_type_.set_out_table();
  source.has_crc_ = true;
  source.compressed_len_ = 4;

  ASSERT_EQ(OB_SUCCESS, source.serialize(allocator, serialized));
  const std::string json(serialized.ptr(), serialized.length());
  ASSERT_NE(std::string::npos, json.find("\"lob_chunk_type\""));
  ASSERT_EQ(std::string::npos, json.find("\"is_no_logging\""));
  ASSERT_EQ(std::string::npos, json.find("storage_type"));

  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_EQ(ObLobMetaExtraInfo::LOB_DATA, decoded.extra_info_type_);
  ASSERT_TRUE(decoded.lob_chunk_type_.out_table());
  ASSERT_TRUE(decoded.has_crc_);
  ASSERT_EQ(0U, decoded.crc_);
  ASSERT_EQ(4U, decoded.compressed_len_);

  source.crc_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, source.serialize(allocator, serialized));
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, serialized));
  ASSERT_EQ(UINT64_MAX, decoded.crc_);

  const char *invalid_chunks[] = {
    "{\"extra_info_type\":2,\"lob_chunk_type\":\"OUT_TABLE\",\"chunk_count\":0,"
    "\"compressed_len\":4}",
    "{\"extra_info_type\":2,\"lob_chunk_type\":\"OUT_TABLE\",\"chunk_count\":0,"
    "\"crc\":-1,\"compressed_len\":4}",
  };
  for (const char *invalid_chunk : invalid_chunks) {
    SCOPED_TRACE(invalid_chunk);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              decoded.deserialize(allocator, ObString::make_string(invalid_chunk)));
  }
}

TEST_F(TestLobChunkSpliter, outtable_summary_omits_crc_and_requires_positive_length)
{
  ObArenaAllocator allocator;
  const char *invalid_summaries[] = {
    "{\"extra_info_type\":1,\"lob_chunk_type\":\"OUT_TABLE\",\"chunk_count\":1}",
    "{\"extra_info_type\":1,\"lob_chunk_type\":\"OUT_TABLE\",\"chunk_count\":1,"
    "\"compressed_len\":0}",
  };
  ObLobMetaExtraInfo decoded;
  for (const char *json : invalid_summaries) {
    SCOPED_TRACE(json);
    ASSERT_EQ(OB_INVALID_ARGUMENT, decoded.deserialize(allocator, ObString::make_string(json)));
  }

  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.lob_chunk_type_.set_out_table();
  extra_info.chunk_count_ = 1;
  extra_info.compressed_len_ = 4;
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, serialized));
  const std::string json(serialized.ptr(), serialized.length());
  ASSERT_EQ(std::string::npos, json.find("\"crc\""));
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, serialized));
  ASSERT_FALSE(decoded.has_crc_);
  ASSERT_EQ(4U, decoded.compressed_len_);

  const ObString summary_with_crc = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"OUT_TABLE\",\"chunk_count\":1,"
      "\"crc\":7,\"compressed_len\":4}");
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, summary_with_crc));
  ASSERT_FALSE(decoded.has_crc_);
  ASSERT_EQ(0U, decoded.crc_);
  ASSERT_EQ(4U, decoded.compressed_len_);
}

TEST_F(TestLobChunkSpliter, lob_meta_extra_info_rejects_invalid_last_write_type)
{
  const ObString invalid_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\","
      "\"chunk_count\":3,\"last_write_type\":99}");
  ObLobMetaExtraInfo extra_info;

  ASSERT_EQ(OB_INVALID_ARGUMENT, extra_info.deserialize(external_info_allocator_, invalid_summary));
}

TEST_F(TestLobChunkSpliter, external_summary_strings_outlive_input)
{
  ObArenaAllocator input_allocator;
  ObArenaAllocator allocator;
  ObLobMetaExtraInfo source;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.etag_ = ObString::make_string("etag");
  info.length_ = 9;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  source.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  ASSERT_EQ(OB_SUCCESS, source.set_external_lob_info(input_allocator, info));
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, source.serialize(input_allocator, serialized));
  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, serialized));
  source.reset();
  input_allocator.reset();
  EXPECT_EQ(0, decoded.external_lob_info_.url_.compare("s3://bucket/object"));
  EXPECT_EQ(0, decoded.external_lob_info_.etag_.compare("etag"));
  EXPECT_EQ(9, decoded.external_lob_info_.length_);
}

TEST_F(TestLobChunkSpliter, external_lob_meta_extra_info_round_trip)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo external_lob_info;
  external_lob_info.url_ = ObString::make_string("s3://bucket/path\\\"name");
  const ObString etag = ObString::make_string("etag-\\\"value");
  external_lob_info.etag_ = etag;
  external_lob_info.offset_ = 3 * 1024 * 1024;
  external_lob_info.length_ = 10 * 1024 * 1024;
  external_lob_info.kind_ = ObExternalBlobKind::REFERENCE;

  ObLobMetaExtraInfo source;
  source.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  source.chunk_count_ = 5;
  source.last_write_type_ = ObLobLastWriteType::INSERT;
  ASSERT_EQ(OB_SUCCESS, source.set_external_lob_info(external_info_allocator_, external_lob_info));

  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, source.serialize(allocator, serialized));

  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_TRUE(decoded.is_external_lob());
  ASSERT_TRUE(decoded.lob_chunk_type_.external_reference());
  ASSERT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, decoded.extra_info_type_);
  ASSERT_EQ(5, decoded.chunk_count_);
  ASSERT_EQ(0, decoded.external_lob_info_.url_.compare(
                   external_lob_info.url_));
  ASSERT_EQ(0, decoded.external_lob_info_.etag_.compare(etag));
  ASSERT_EQ(external_lob_info.offset_, decoded.external_lob_info_.offset_);
  ASSERT_EQ(external_lob_info.length_, decoded.external_lob_info_.length_);
  ASSERT_EQ(ObExternalBlobKind::REFERENCE, decoded.external_lob_info_.kind_);

  source.reset();
  external_lob_info.etag_.reset();
  ASSERT_FALSE(source.is_external_lob());
  ASSERT_TRUE(source.external_lob_info_.etag_.empty());
  ASSERT_EQ(0, decoded.external_lob_info_.etag_.compare(etag));
}

TEST_F(TestLobChunkSpliter, external_lob_meta_summary_json_round_trip)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo external_lob_info;
  external_lob_info.url_ = ObString::make_string("s3://bucket/object");
  external_lob_info.length_ = 1;
  external_lob_info.kind_ = ObExternalBlobKind::REFERENCE;

  ObLobMetaExtraInfo source;
  source.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  source.chunk_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, source.set_external_lob_info(external_info_allocator_, external_lob_info));
  ASSERT_TRUE(source.is_valid());
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, source.serialize(allocator, serialized));
  const std::string json(serialized.ptr(), serialized.length());
  ASSERT_NE(std::string::npos, json.find("\"EXTERNAL_REFERENCE\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_url\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_etag\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_offset\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_length\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_has_length\""));
  ASSERT_NE(std::string::npos, json.find("\"external_lob_kind\""));

  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_TRUE(decoded.is_external_lob());
  ASSERT_TRUE(decoded.lob_chunk_type_.external_reference());
  ASSERT_EQ(source.extra_info_type_, decoded.extra_info_type_);
  ASSERT_EQ(source.chunk_count_, decoded.chunk_count_);
  ASSERT_EQ(0, decoded.external_lob_info_.url_.compare(external_lob_info.url_));
  ASSERT_EQ(external_lob_info.offset_, decoded.external_lob_info_.offset_);
  ASSERT_EQ(external_lob_info.length_, decoded.external_lob_info_.length_);
  ASSERT_EQ(external_lob_info.has_length_, decoded.external_lob_info_.has_length_);
  ASSERT_EQ(0, decoded.external_lob_info_.etag_.compare(external_lob_info.etag_));
  ASSERT_EQ(external_lob_info.kind_, decoded.external_lob_info_.kind_);
}

TEST_F(TestLobChunkSpliter, external_lob_meta_etag_compatibility_and_bounds)
{
  const std::string prefix =
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\","
      "\"chunk_count\":1,\"last_write_type\":0,"
      "\"external_lob_url\":\"s3://bucket/object\","
      "\"external_lob_offset\":0,\"external_lob_length\":1,"
      "\"external_lob_has_length\":true,"
      "\"external_lob_kind\":0";
  ObLobMetaExtraInfo extra_info;
  extra_info.external_lob_info_.etag_ = ObString::make_string("old");

  // ETag is optional; clear its previous value when reusing the object.
  const std::string no_etag_summary = prefix + "}";
  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(
      external_info_allocator_, ObString(no_etag_summary.length(), no_etag_summary.data())));
  ASSERT_TRUE(extra_info.is_external_lob());
  ASSERT_TRUE(extra_info.lob_chunk_type_.external_reference());
  ASSERT_TRUE(extra_info.external_lob_info_.etag_.empty());
  ObArenaAllocator allocator;
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, serialized));
  ASSERT_FALSE(serialized.empty());
  ASSERT_NE(std::string::npos,
            std::string(serialized.ptr(), serialized.length()).find(
                "\"external_lob_etag\""));
  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_TRUE(decoded.external_lob_info_.etag_.empty());

  // ETag content must be shorter than OB_ETAG_CAPACITY.
  const std::string etag(OB_ETAG_CAPACITY - 1, 'x');
  const std::string summary = prefix + ",\"external_lob_etag\":\""
      + etag + "\"}";
  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(
      external_info_allocator_, ObString(summary.length(), summary.data())));
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, serialized));
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(external_info_allocator_, serialized));
  ASSERT_EQ(0, decoded.external_lob_info_.etag_.compare(
                   ObString(etag.length(), etag.data())));

  const std::string invalid_etags[] = {
      "17", "null", "true", "[]", "{}", "\"first\\u0000last\"",
      "\"" + std::string(OB_ETAG_CAPACITY, 'x') + "\""};
  for (const std::string &invalid_etag : invalid_etags) {
    SCOPED_TRACE(invalid_etag);
    const std::string invalid_summary =
        prefix + ",\"external_lob_etag\":" + invalid_etag + "}";
    EXPECT_EQ(OB_INVALID_ARGUMENT, extra_info.deserialize(
        external_info_allocator_, ObString(invalid_summary.length(), invalid_summary.data())));
    EXPECT_EQ(0, extra_info.external_lob_info_.etag_.compare(
                     ObString(etag.length(), etag.data())));
  }
}

TEST_F(TestLobChunkSpliter,
       external_lob_meta_extra_info_rejects_invalid_input_atomically)
{
  ObExternalBlobInfo external_lob_info;
  external_lob_info.url_ = ObString::make_string("s3://bucket/original");
  external_lob_info.etag_ = ObString::make_string("original-etag");
  external_lob_info.offset_ = 11;
  external_lob_info.length_ = 22;
  external_lob_info.kind_ = ObExternalBlobKind::REFERENCE;
  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.chunk_count_ = 7;
  extra_info.last_write_type_ = ObLobLastWriteType::APPEND;
  ASSERT_EQ(OB_SUCCESS, extra_info.set_external_lob_info(external_info_allocator_,
      external_lob_info));

  const ObString invalid_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\","
      "\"chunk_count\":1,\"last_write_type\":0,"
      "\"external_lob_url\":\"s3://bucket/invalid\","
      "\"external_lob_offset\":1,\"external_lob_length\":0,"
      "\"external_lob_has_length\":false,"
      "\"external_lob_kind\":0}");
  ASSERT_EQ(OB_INVALID_ARGUMENT, extra_info.deserialize(external_info_allocator_, invalid_summary));
  const auto expect_unchanged = [&extra_info]() {
    EXPECT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY,
              extra_info.extra_info_type_);
    EXPECT_TRUE(extra_info.is_external_lob());
    EXPECT_TRUE(extra_info.lob_chunk_type_.external_reference());
    EXPECT_EQ(7, extra_info.chunk_count_);
    EXPECT_EQ(ObLobLastWriteType::APPEND, extra_info.last_write_type_);
    EXPECT_EQ(0, extra_info.external_lob_info_.url_.compare(
                     ObString::make_string("s3://bucket/original")));
    EXPECT_EQ(0, extra_info.external_lob_info_.etag_.compare(
                     ObString::make_string("original-etag")));
    EXPECT_EQ(11, extra_info.external_lob_info_.offset_);
    EXPECT_EQ(22, extra_info.external_lob_info_.length_);
    EXPECT_EQ(ObExternalBlobKind::REFERENCE,
              extra_info.external_lob_info_.kind_);
  };
  expect_unchanged();

  const std::string external_fields =
      "\"external_lob_url\":\"s3://bucket/object\","
      "\"external_lob_offset\":0,\"external_lob_length\":1,"
      "\"external_lob_has_length\":true,"
      "\"external_lob_kind\":0}";
  const std::string valid_summary =
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\","
      "\"chunk_count\":1,\"last_write_type\":0," + external_fields;
  ObLobMetaExtraInfo valid_extra_info;
  ASSERT_EQ(OB_SUCCESS, valid_extra_info.deserialize(
      external_info_allocator_, ObString(valid_summary.length(), valid_summary.data())));
  const std::string invalid_types[] = {
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\","
      "\"chunk_count\":1,\"last_write_type\":0,",
      "{\"extra_info_type\":2,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\","
      "\"chunk_count\":1,",
      "{\"extra_info_type\":2,\"lob_chunk_type\":\"IN_TABLE\","
      "\"chunk_count\":1,"};
  for (const std::string &prefix : invalid_types) {
    const std::string summary = prefix + external_fields;
    SCOPED_TRACE(summary);
    EXPECT_EQ(OB_INVALID_ARGUMENT, extra_info.deserialize(
        external_info_allocator_, ObString(summary.length(), summary.data())));
    expect_unchanged();
  }

  // External references need full descriptors; missing required fields must preserve the old value.
  const std::string required_fields[] = {
      "\"external_lob_url\":\"s3://bucket/object\"",
      "\"external_lob_offset\":0", "\"external_lob_length\":1",
      "\"external_lob_has_length\":true",
      "\"external_lob_kind\":0"};
  for (int64_t missing = 0; missing < 5; ++missing) {
    SCOPED_TRACE(required_fields[missing]);
    std::string summary =
        "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\","
        "\"chunk_count\":1,\"last_write_type\":0";
    for (int64_t field = 0; field < 5; ++field) {
      if (field != missing) {
        summary += "," + required_fields[field];
      }
    }
    summary += "}";
    EXPECT_NE(OB_SUCCESS, extra_info.deserialize(
        external_info_allocator_, ObString(summary.length(), summary.data())));
    expect_unchanged();
  }

  ASSERT_EQ(OB_SUCCESS,
            extra_info.set_external_lob_info(external_info_allocator_,
                extra_info.external_lob_info_));
  ASSERT_EQ(0, extra_info.external_lob_info_.url_.compare(
                   ObString::make_string("s3://bucket/original")));
  ASSERT_EQ(11, extra_info.external_lob_info_.offset_);
  ASSERT_EQ(22, extra_info.external_lob_info_.length_);

  external_lob_info.length_ = 0;
  external_lob_info.has_length_ = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            extra_info.set_external_lob_info(external_info_allocator_, external_lob_info));
  external_lob_info.length_ = 22;
  external_lob_info.has_length_ = true;
  external_lob_info.offset_ = UINT64_MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            extra_info.set_external_lob_info(external_info_allocator_, external_lob_info));
  ASSERT_EQ(11, extra_info.external_lob_info_.offset_);
  ASSERT_EQ(22, extra_info.external_lob_info_.length_);
}

TEST_F(TestLobChunkSpliter, external_lob_meta_rejects_zero_length)
{
  ObExternalBlobInfo external_lob_info;
  external_lob_info.url_ = ObString::make_string("s3://bucket/empty");
  external_lob_info.length_ = 0;
  external_lob_info.has_length_ = true;
  external_lob_info.kind_ = ObExternalBlobKind::REFERENCE;
  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  ASSERT_EQ(OB_INVALID_ARGUMENT, extra_info.set_external_lob_info(
      external_info_allocator_, external_lob_info));
  const ObString serialized = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\",\"chunk_count\":0,"
      "\"external_lob_url\":\"s3://bucket/empty\",\"external_lob_offset\":0,"
      "\"external_lob_length\":0,\"external_lob_has_length\":true,\"external_lob_kind\":0}");
  ObLobMetaExtraInfo decoded;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoded.deserialize(external_info_allocator_, serialized));
}

TEST_F(TestLobChunkSpliter, external_lob_macro_id_prefix_round_trip_and_invalid_values)
{
  ObArenaAllocator allocator;
  const uint64_t valid_ids[][2] = {
      {2002, 3003}, {UINT64_MAX, static_cast<uint64_t>(INT64_MAX)}};
  ObString serialized;
  for (const auto &id : valid_ids) {
    ASSERT_EQ(OB_SUCCESS, ObExternalLobMacroIdPrefix::serialize(
        allocator, id[0], id[1], serialized));
    ASSERT_EQ(17, serialized.length());
    uint64_t decoded_lob_meta_tablet_id = 0;
    uint64_t external_lob_macro_seq_id = 0;
    ASSERT_EQ(OB_SUCCESS, ObExternalLobMacroIdPrefix::deserialize(
        serialized, decoded_lob_meta_tablet_id, external_lob_macro_seq_id));
    ASSERT_EQ(id[0], decoded_lob_meta_tablet_id);
    ASSERT_EQ(id[1], external_lob_macro_seq_id);
  }
  const uint64_t invalid_ids[][2] = {
      {0, 1}, {2002, 0}, {2002, static_cast<uint64_t>(INT64_MAX) + 1}};
  for (const auto &id : invalid_ids) {
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObExternalLobMacroIdPrefix::serialize(
                  allocator, id[0], id[1], serialized));
  }
}

TEST_F(TestLobChunkSpliter, external_lob_macro_id_prefix_rejects_malformed_data_atomically)
{
  ObArenaAllocator allocator;
  uint64_t lob_meta_tablet_id = 2002;
  uint64_t external_lob_macro_seq_id = 3003;
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, ObExternalLobMacroIdPrefix::serialize(
      allocator, lob_meta_tablet_id, external_lob_macro_seq_id, serialized));
  ASSERT_EQ(17, serialized.length());
  const std::string valid(serialized.ptr(), serialized.length());
  std::string bad_version(valid);
  bad_version[0] = 2;
  std::string zero_tablet(valid);
  zero_tablet.replace(1, 8, 8, '\0');
  std::string zero_seq(valid);
  zero_seq.replace(9, 8, 8, '\0');
  std::string overflow_seq(zero_seq);
  overflow_seq[9] = static_cast<char>(0x80);
  const std::string invalid_data[] = {
      "", valid.substr(0, valid.length() - 1), valid + "x", bad_version,
      zero_tablet, zero_seq, overflow_seq};
  // Reject truncation, trailing bytes, and invalid identities; preserve the existing identity.
  for (const std::string &data : invalid_data) {
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObExternalLobMacroIdPrefix::deserialize(
                  ObString(data.length(), data.data()), lob_meta_tablet_id,
                  external_lob_macro_seq_id));
    ASSERT_EQ(2002, lob_meta_tablet_id);
    ASSERT_EQ(3003, external_lob_macro_seq_id);
  }
}

TEST_F(TestLobChunkSpliter, external_lob_summary_block_id_survives_row_copy)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.lob_chunk_type_.set_in_table();
  ASSERT_EQ(OB_SUCCESS, extra_info.set_external_lob_info(external_info_allocator_, info));

  ObLobMetaInfo summary;
  summary.lob_col_idx_ = 2;
  summary.lob_id_.assign(2, 1001, 1002);
  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  summary.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(0, summary.seq_id_));
  summary.byte_len_ = 1;
  summary.char_len_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            extra_info.serialize(allocator, summary.lob_extra_info_));
  const uint64_t lob_meta_tablet_id = 2002;
  const uint64_t external_lob_macro_seq_id = 3003;
  ASSERT_EQ(OB_SUCCESS, ObExternalLobMacroIdPrefix::serialize(
      allocator, lob_meta_tablet_id, external_lob_macro_seq_id, summary.lob_block_id_));

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(ObLobMetaInfo::COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS,
            ObLobMetaUtil::transform_from_info_to_row(summary, &row, false));
  ASSERT_EQ(0, summary.lob_block_id_.compare(
      row.storage_datums_[ObLobMetaUtil::LOB_BLOCK_ID_COL_ID].get_string()));
  ObLobMetaInfo scanned;
  ASSERT_EQ(OB_SUCCESS,
            ObLobMetaUtil::transform_from_row_to_info(&row, scanned, false));
  ObLobMetaInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.deep_copy(allocator, scanned));

  // Overwriting the source row buffer preserves the deep copy's identity and reference descriptor.
  MEMSET(summary.lob_block_id_.ptr(), 0, summary.lob_block_id_.length());
  MEMSET(summary.lob_extra_info_.ptr(), 'x', summary.lob_extra_info_.length());
  uint64_t decoded_lob_meta_tablet_id = 0;
  uint64_t decoded_external_lob_macro_seq_id = 0;
  ASSERT_EQ(OB_SUCCESS, ObExternalLobMacroIdPrefix::deserialize(copied.lob_block_id_,
            decoded_lob_meta_tablet_id, decoded_external_lob_macro_seq_id));
  ASSERT_EQ(2002, decoded_lob_meta_tablet_id);
  ASSERT_EQ(3003, decoded_external_lob_macro_seq_id);
  ObLobMetaExtraInfo decoded_info;
  ASSERT_EQ(OB_SUCCESS, decoded_info.deserialize(external_info_allocator_, copied.lob_extra_info_));
  ASSERT_TRUE(decoded_info.is_external_lob());
  ASSERT_EQ(0, decoded_info.external_lob_info_.url_.compare(
      info.url_));
}

//==================== ObLobChunkType ====================
TEST_F(TestLobChunkSpliter, chunk_type_state)
{
  ObLobChunkType type;
  ASSERT_FALSE(type.is_valid());
  ASSERT_TRUE(type.in_table()); // 见 ObLobChunkType::in_table() 的实现和注释

  type.set_in_table();
  ASSERT_TRUE(type.is_valid());
  ASSERT_TRUE(type.in_table());

  type.reset();
  ASSERT_FALSE(type.is_valid());
}

TEST_F(TestLobChunkSpliter, outtable_write_error_classification)
{
  ASSERT_TRUE(ObLobOutTableIO::is_retryable_error(OB_TIMEOUT));
  ASSERT_TRUE(ObLobOutTableIO::is_retryable_error(OB_EAGAIN));
  ASSERT_FALSE(ObLobOutTableIO::is_retryable_error(OB_INVALID_ARGUMENT));
}

TEST_F(TestLobChunkSpliter, concurrent_uploader_allocates_slots_by_chunk_count)
{
  ObLobConcurrentPuter puter;
  const int64_t chunk_size = 1024;
  const int64_t chunk_count = 3;

  ASSERT_EQ(OB_SUCCESS, puter.init(ObTimeUtility::current_time() + 10 * 1000 * 1000,
                                  chunk_size, chunk_count));
  ASSERT_EQ(chunk_count, puter.capacity_);
  ASSERT_EQ(chunk_size, puter.chunk_size_);
  ASSERT_EQ(chunk_count, puter.slots_.count());
  ASSERT_EQ(0, puter.count_);
  puter.reset();
  ASSERT_EQ(0, puter.slots_.count());
}

TEST_F(TestLobChunkSpliter, outtable_batch_preparation_preserves_chunks_and_errors)
{
  const int64_t capacity = ObLobConcurrentPuter::DEFAULT_CAPACITY;
  const int64_t chunk_count = capacity + 2;
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  for (const bool missing_macro_id : {false, true}) {
    SCOPED_TRACE(missing_macro_id);
    ObArenaAllocator allocator;
    ObArenaAllocator batch_allocator;
    ObString data = make_data(allocator, "a", (chunk_count - 1) * chunk_size + 17);
    MEMSET(data.ptr() + capacity * chunk_size, 'b', chunk_size + 17);
    ObLobAppendSourceIter query_iter;
    ASSERT_EQ(OB_SUCCESS, query_iter.open(&allocator, ObString(), data, CS_TYPE_BINARY));
    ObLobInsertContext insert_ctx;
    ASSERT_NO_FATAL_FAILURE(prepare_redo_context(insert_ctx));
    insert_ctx.lob_meta_tablet_id_ = ObTabletID(1001);
    ObLobId lob_id;
    lob_id.assign(2, 1001, 2002);
    ObArray<ObLobMetaInfo> metas;
    ObLobWriteStats stats;
    ObLobOutTableInsertIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(&insert_ctx, &query_iter, &allocator, chunk_size,
        chunk_count, 2, lob_id, &metas, &stats));
    blocksstable::MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(
        static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
    macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_third_id(1001);
    macro_id.set_fourth_id(2002);
    for (int64_t i = 0; i < chunk_count; ++i) {
      macro_id.set_second_id(100 + i);
      if (i < chunk_count - missing_macro_id) {
        ASSERT_EQ(OB_SUCCESS, iter.macro_id_array_.push_back(macro_id));
      }
    }
    ObArray<ObString> chunk_data;
    ObArray<ObOuttableLobControlKey> control_keys;
    ObLobAppendSourceIter redo_iter;
    ASSERT_EQ(OB_SUCCESS, redo_iter.open(&allocator, ObString(), data, CS_TYPE_BINARY));
    ObLobMetaManager meta_manager;
    ObArray<ObLobOutTableRedoPayload> payloads;
    for (int64_t batch = 0; batch < 2; ++batch) {
      payloads.reuse();
      batch_allocator.reuse();
      const int ret = iter.prepare_batch_(batch_allocator, chunk_data, control_keys);
      if (missing_macro_id && 1 == batch) {
        ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
        ASSERT_FALSE(iter.is_end());
        ASSERT_EQ(chunk_count - 1, iter.idx_);
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ((0 == batch) ? capacity : 2, chunk_data.count());
      ASSERT_EQ(chunk_data.count(), control_keys.count());
      ASSERT_EQ(1 == batch, iter.is_end());
      for (int64_t i = 0; i < chunk_data.count(); ++i) {
        const int64_t idx = batch * capacity + i;
        const ObString expected((idx + 1 == chunk_count) ? 17 : chunk_size,
                                data.ptr() + idx * chunk_size);
        const uint64_t expected_crc = ob_crc64(expected.ptr(), expected.length());
        ASSERT_EQ(expected, chunk_data.at(i));
        ASSERT_NE(query_iter.buffer_.ptr(), chunk_data.at(i).ptr());
        const blocksstable::MacroBlockId &expected_macro_id = iter.macro_id_array_.at(idx);
        ASSERT_EQ(expected_macro_id, control_keys.at(i).macro_id_);
        ASSERT_EQ(expected_crc, control_keys.at(i).hash_);
        ASSERT_EQ(expected_crc, static_cast<uint64_t>(expected_macro_id.fifth_id()));
        ObLobOutTableRedoPayload payload;
        ASSERT_EQ(OB_SUCCESS, meta_manager.prepare_outtable_redo_payload_(insert_ctx,
            &redo_iter, metas.at(idx), 50001, true, batch_allocator, payload));
        ASSERT_TRUE(payload.is_valid());
        EXPECT_EQ(expected, payload.data_);
        EXPECT_NE(redo_iter.buffer_.ptr(), payload.data_.ptr());
        EXPECT_EQ(metas.at(idx).lob_col_idx_, payload.lob_col_idx_);
        EXPECT_EQ(metas.at(idx).lob_id_, payload.lob_id_);
        EXPECT_EQ(metas.at(idx).seq_id_, payload.seq_id_);
        EXPECT_EQ(control_keys.at(i).macro_id_, payload.macro_id_);
        EXPECT_EQ(insert_ctx.insert_lob_seq_no_, payload.lob_operation_seq_no_);
        EXPECT_EQ(50001, payload.lob_piece_table_id_);
        ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload));
      }
      // Later source reads reuse their buffer; every payload must still own the matching bytes.
      for (int64_t i = 0; i < payloads.count(); ++i) {
        EXPECT_EQ(chunk_data.at(i), payloads.at(i).data_);
      }
      ASSERT_EQ((0 == batch) ? capacity : chunk_count, iter.idx_);
      ASSERT_EQ(iter.idx_, metas.count());
    }
    batch_allocator.reuse();
    ASSERT_EQ(chunk_count - missing_macro_id, metas.count());
    ASSERT_EQ(static_cast<uint64_t>(metas.count()), stats.chunk_count_);
    const int64_t expected_len = missing_macro_id ? (chunk_count - 1) * chunk_size : data.length();
    ASSERT_EQ(static_cast<uint64_t>(expected_len), stats.byte_len_);
    ASSERT_EQ(stats.byte_len_, stats.char_len_);
    ASSERT_EQ(stats.byte_len_, stats.compressed_len_);
    for (int64_t i = 0; i < metas.count(); ++i) {
      ObString expected_seq;
      ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, i + 1, expected_seq));
      ASSERT_EQ(expected_seq, metas.at(i).seq_id_);
      ASSERT_EQ(OB_SUCCESS,
          ObLobOutTableIO::deserialize_macro_id(metas.at(i).lob_block_id_, macro_id));
      ASSERT_EQ(iter.macro_id_array_.at(i), macro_id);
      ASSERT_EQ(static_cast<uint64_t>((i + 1 == chunk_count) ? 17 : chunk_size),
          metas.at(i).byte_len_);
      ASSERT_EQ(metas.at(i).byte_len_, metas.at(i).char_len_);
      ASSERT_EQ(lob_id, metas.at(i).lob_id_);
      ASSERT_EQ(2, metas.at(i).lob_col_idx_);
      ObLobMetaExtraInfo extra_info;
      ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(allocator, metas.at(i).lob_extra_info_));
      ASSERT_TRUE(extra_info.lob_chunk_type_.out_table());
      ASSERT_TRUE(extra_info.has_crc_);
      ASSERT_EQ(metas.at(i).byte_len_, extra_info.compressed_len_);
      ASSERT_EQ(ob_crc64(data.ptr() + i * chunk_size, metas.at(i).byte_len_), extra_info.crc_);
    }
  }
}

TEST_F(TestLobChunkSpliter, outtable_write_requires_explicit_permission)
{
  ObArenaAllocator allocator;
  const ObString data = ObString::make_string("outtable");
  ObLobAppendSourceIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(&allocator, ObString(), data, CS_TYPE_BINARY));
  ObLobInsertContext insert_ctx;
  EXPECT_FALSE(insert_ctx.allow_outtable_lob_);
  insert_ctx.lob_meta_tablet_id_ = ObTabletID(1001);
  ObLobId lob_id;
  lob_id.assign(2, 1001, 2002);
  ObArray<ObLobMetaInfo> metas;
  ObLobWriteStats stats;
  ObLobOutTableInsertIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&insert_ctx, &query_iter, &allocator,
      ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 1, 2, lob_id, &metas, &stats));
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.write_all());
  ObArray<ObOuttableLobControlKey> control_keys;
  insert_ctx.outtable_lob_control_keys_ = &control_keys;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.write_all());
  EXPECT_TRUE(control_keys.empty());
  EXPECT_TRUE(iter.macro_id_array_.empty());
  EXPECT_TRUE(metas.empty());
  EXPECT_EQ(0, stats.chunk_count_);
  EXPECT_EQ(0, iter.idx_);
  EXPECT_FALSE(iter.is_end());

  insert_ctx.allow_outtable_lob_ = true;
  insert_ctx.outtable_lob_control_keys_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.write_all());
  EXPECT_TRUE(iter.macro_id_array_.empty());
  EXPECT_TRUE(metas.empty());
  EXPECT_EQ(0, iter.idx_);
}

TEST_F(TestLobChunkSpliter, lob_write_context_copies_outtable_permission)
{
  ObDMLBaseParam dml_param;
  EXPECT_FALSE(dml_param.allow_outtable_lob_);
  ObLobInsertContext source;
  ObLobInsertContext target;
  EXPECT_FALSE(source.allow_outtable_lob_);
  EXPECT_FALSE(target.allow_outtable_lob_);
  source.allow_outtable_lob_ = true;
  ASSERT_EQ(OB_SUCCESS, target.assign(source));
  EXPECT_TRUE(target.allow_outtable_lob_);
  source.allow_outtable_lob_ = false;
  ASSERT_EQ(OB_SUCCESS, target.assign(source));
  EXPECT_FALSE(target.allow_outtable_lob_);
}

TEST_F(TestLobChunkSpliter, outtable_async_read_rejects_expired_deadline)
{
  blocksstable::MacroBlockId macro_id;
  macro_id.set_version_v1();
  macro_id.set_id_mode(
      static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(static_cast<uint64_t>(
      blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  macro_id.set_second_id(100);
  macro_id.set_third_id(200);
  macro_id.set_fourth_id(300);
  macro_id.set_fifth_id(0);
  ObArenaAllocator allocator;
  ObString data;
  blocksstable::ObStorageObjectHandle object_handle;

  ASSERT_EQ(OB_TIMEOUT, ObLobOutTableIO::async_read(
      macro_id, 1, ObTimeUtility::current_time() - 1, allocator, data, object_handle));
  ASSERT_TRUE(data.empty());
  ASSERT_TRUE(object_handle.is_empty());
}

//==================== ObLobChunk::to_meta / to_piece ====================
TEST_F(TestLobChunkSpliter, chunk_to_meta_invalid_type)
{
  ObLobChunk chunk;
  ObArenaAllocator allocator;
  ObLobMetaInfo meta;
  // type not set -> invalid
  ASSERT_EQ(OB_INVALID_ARGUMENT, chunk.to_meta(allocator, meta));
}

TEST_F(TestLobChunkSpliter, chunk_to_meta_fields)
{
  ObArenaAllocator allocator;
  const char seq[] = "\x01\x00\x00\x01\x00";
  const char *block = "block-id";
  const char *data = "chunk-data";

  ObLobChunk chunk;
  chunk.lob_col_idx_ = 3;
  chunk.lob_id_.assign(chunk.lob_col_idx_, 1001, 2002);
  chunk.seq_id_.assign_ptr(const_cast<char*>(seq), sizeof(seq) - 1);
  chunk.byte_len_ = 100;
  chunk.char_len_ = 80;
  chunk.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  chunk.lob_extra_info_.lob_chunk_type_.set_in_table();
  chunk.lob_block_id_.assign_ptr(block, STRLEN(block));
  chunk.lob_data_.assign_ptr(data, STRLEN(data));

  ObLobMetaInfo meta;
  ASSERT_EQ(OB_SUCCESS, chunk.to_meta(allocator, meta));
  ASSERT_EQ(3, meta.lob_col_idx_);
  ASSERT_EQ(chunk.lob_id_, meta.lob_id_);
  ASSERT_EQ(100u, meta.byte_len_);
  ASSERT_EQ(80u, meta.char_len_);
  ASSERT_EQ(0, MEMCMP(meta.seq_id_.ptr(), seq, sizeof(seq) - 1));
  ASSERT_EQ(0, MEMCMP(meta.lob_block_id_.ptr(), block, STRLEN(block)));

  ObLobPieceInfo piece;
  ASSERT_EQ(OB_SUCCESS, chunk.to_piece(piece));
  ASSERT_EQ(3, piece.lob_col_idx_);
  ASSERT_EQ(chunk.lob_id_, piece.lob_id_);
  ASSERT_EQ(0, MEMCMP(piece.seq_id_.ptr(), seq, sizeof(seq) - 1));
  ASSERT_EQ(0, MEMCMP(piece.lob_data_.ptr(), data, STRLEN(data)));
}

TEST_F(TestLobChunkSpliter, chunk_reset)
{
  ObLobChunk chunk;
  chunk.lob_extra_info_.lob_chunk_type_.set_out_table();
  chunk.lob_col_idx_ = 5;
  chunk.byte_len_ = 99;
  chunk.reset();
  ASSERT_FALSE(chunk.lob_extra_info_.lob_chunk_type_.is_valid());
  ASSERT_EQ(OB_INVALID_INDEX_INT64, chunk.lob_col_idx_);
  ASSERT_EQ(0u, chunk.byte_len_);
}

//==================== streaming chunk / piece iterators ====================
TEST_F(TestLobChunkSpliter, query_iter_returns_multibyte_chunks)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  ObString data = make_data(allocator, "\xe4\xb8\xad", 512);
  ObLobInRowQueryIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      data, 0, static_cast<uint32_t>(data.length()), CS_TYPE_UTF8MB4_GENERAL_CI));

  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(1023u, chunk.byte_len_);
  ASSERT_EQ(341u, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), data.ptr(), chunk.byte_len_));

  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(513u, chunk.byte_len_);
  ASSERT_EQ(171u, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), data.ptr() + 1023, chunk.byte_len_));

  ASSERT_EQ(OB_ITER_END, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_TRUE(query_iter.is_end());
  ASSERT_FALSE(chunk.lob_extra_info_.lob_chunk_type_.is_valid());
}

TEST_F(TestLobChunkSpliter, append_source_iter_returns_binary_chunks)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  const int64_t tail_length = 17;
  ObString first = make_data(allocator, "a", chunk_size - tail_length);
  ObString second = make_data(allocator, "b", 2 * tail_length);
  ObLobAppendSourceIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(&allocator, first, second, CS_TYPE_BINARY));

  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), chunk.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), first.ptr(), first.length()));
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr() + first.length(), second.ptr(), tail_length));

  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(static_cast<uint64_t>(tail_length), chunk.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(tail_length), chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), second.ptr() + tail_length, tail_length));
  ASSERT_TRUE(query_iter.is_end());
  ASSERT_EQ(OB_ITER_END, query_iter.get_next_chunk(chunk, chunk_size));
}

TEST_F(TestLobChunkSpliter, append_source_iter_returns_multibyte_chunks)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  const int64_t first_char_count = 200;
  const int64_t second_char_count = 200;
  ObString first = make_data(allocator, "\xe4\xb8\xad", first_char_count);
  ObString second = make_data(allocator, "\xf0\x9f\x98\x80", second_char_count);
  const int64_t second_offset = chunk_size - first.length();
  const uint64_t first_chunk_char_count = first_char_count + second_offset / 4;
  const uint64_t second_chunk_char_count = second_char_count - second_offset / 4;
  ObLobAppendSourceIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      &allocator, first, second, CS_TYPE_UTF8MB4_GENERAL_CI));

  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), chunk.byte_len_);
  ASSERT_EQ(first_chunk_char_count, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), first.ptr(), first.length()));
  ASSERT_EQ(0, MEMCMP(
      chunk.lob_data_.ptr() + first.length(), second.ptr(), chunk_size - first.length()));

  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(static_cast<uint64_t>(second.length() - second_offset), chunk.byte_len_);
  ASSERT_EQ(second_chunk_char_count, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(
      chunk.lob_data_.ptr(), second.ptr() + second_offset, second.length() - second_offset));
  ASSERT_TRUE(query_iter.is_end());
  ASSERT_EQ(OB_ITER_END, query_iter.get_next_chunk(chunk, chunk_size));
}

TEST_F(TestLobChunkSpliter, multibyte_chunk_count_uses_upper_bound)
{
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  const int64_t three_byte_char_count = 682;
  std::string data;
  for (int64_t i = 0; i < three_byte_char_count; ++i) {
    data.append("\xe4\xb8\xad");
  }
  data.append("\xc2\xa2");
  ObString source;
  source.assign_ptr(data.data(), data.length());
  ASSERT_EQ(2 * chunk_size, source.length());
  int64_t mbmaxlen = 0;
  ASSERT_EQ(OB_SUCCESS, ObCharset::get_mbmaxlen_by_coll(CS_TYPE_UTF8MB4_GENERAL_CI, mbmaxlen));
  const int64_t min_chunk_size = chunk_size - mbmaxlen + 1;
  const int64_t reserved_count = source.length() / min_chunk_size + (0 != source.length() % min_chunk_size);
  ASSERT_EQ(3, reserved_count);

  ObLobInRowQueryIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      source, 0, static_cast<uint32_t>(source.length()), CS_TYPE_UTF8MB4_GENERAL_CI));

  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(1023u, chunk.byte_len_);
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(1023u, chunk.byte_len_);
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(2u, chunk.byte_len_);
  ASSERT_EQ(OB_ITER_END, query_iter.get_next_chunk(chunk, chunk_size));
}

TEST_F(TestLobChunkSpliter, piece_iter_streams_and_deep_copies_meta)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  ObString data = make_data(allocator, "a", chunk_size + 17);
  ObLobInRowQueryIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      data, 0, static_cast<uint32_t>(data.length()), CS_TYPE_BINARY));

  ObLobId lob_id;
  lob_id.assign(2, 1001, 2002);
  ObArray<ObLobMetaInfo> metas;
  ObLobWriteStats stats;
  ObLobInTablePieceInsertIter piece_iter;
  ASSERT_EQ(OB_SUCCESS, piece_iter.init(
      &query_iter, &allocator, chunk_size, 2, lob_id, &metas, &stats));

  blocksstable::ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, piece_iter.get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_EQ(1, metas.count());
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), stats.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), stats.char_len_);
  ASSERT_EQ(1u, stats.chunk_count_);
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN, metas.at(0).seq_id_.length());
  char first_seq_id[sizeof(uint32_t)];
  MEMCPY(first_seq_id, metas.at(0).seq_id_.ptr(), sizeof(first_seq_id));

  ASSERT_EQ(OB_SUCCESS, piece_iter.get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_EQ(2, metas.count());
  ASSERT_EQ(static_cast<uint64_t>(data.length()), stats.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(data.length()), stats.char_len_);
  ASSERT_EQ(2u, stats.chunk_count_);
  ASSERT_EQ(0, MEMCMP(first_seq_id, metas.at(0).seq_id_.ptr(), sizeof(first_seq_id)));
  ASSERT_NE(0, metas.at(0).seq_id_.compare(metas.at(1).seq_id_));
  ASSERT_NE(metas.at(0).seq_id_.ptr(), metas.at(1).seq_id_.ptr());
  ASSERT_NE(metas.at(0).lob_extra_info_.ptr(), metas.at(1).lob_extra_info_.ptr());

  ASSERT_EQ(OB_ITER_END, piece_iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
  ASSERT_TRUE(piece_iter.is_end());
}

//==================== ObLobWriteIter ====================
TEST_F(TestLobChunkSpliter, lob_write_iter_emits_piece_then_meta)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  ObString data = make_data(source_allocator, "a", chunk_size + 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 3;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2002);
  target_ctx.seq_no_ = transaction::ObTxSEQ(10, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));

  ObArray<ObString> piece_seq_ids;
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  int64_t piece_count = 0;
  int64_t meta_count = 0;
  int64_t data_offset = 0;
  bool summary_seen = false;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
    if (result.is_piece()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(target_ctx.lob_col_idx_, result.piece_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.piece_info_.lob_id_);
      ASSERT_EQ(0, MEMCMP(data.ptr() + data_offset,
                          result.piece_info_.lob_data_.ptr(),
                          result.piece_info_.lob_data_.length()));
      data_offset += result.piece_info_.lob_data_.length();
      ObString seq_id;
      ASSERT_EQ(OB_SUCCESS, ob_write_string(
          source_allocator, result.piece_info_.seq_id_, seq_id));
      ASSERT_EQ(OB_SUCCESS, piece_seq_ids.push_back(seq_id));
      ++piece_count;
      if (1 == piece_count) {
        ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));
      }
    } else if (result.is_meta_summary()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(2, piece_count);
      ASSERT_EQ(target_ctx.lob_col_idx_, result.meta_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.meta_info_.lob_id_);
      char summary_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
      ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
      ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_summary_seq_id(summary_seq_id));
      ASSERT_EQ(0, result.meta_info_.seq_id_.compare(summary_seq_id));
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.byte_len_);
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.char_len_);
      ObLobMetaExtraInfo extra_info;
      ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(external_info_allocator_,
          result.meta_info_.lob_extra_info_));
      ASSERT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
      ASSERT_TRUE(extra_info.lob_chunk_type_.in_table());
      ASSERT_EQ(static_cast<uint64_t>(piece_count), extra_info.chunk_count_);
      ASSERT_TRUE(extra_info.lob_chunk_type_.in_table());
      summary_seen = true;
      ++meta_count;
    } else {
      ASSERT_TRUE(result.is_meta_chunk());
      ASSERT_TRUE(summary_seen);
      ASSERT_LT(meta_count - 1, piece_seq_ids.count());
      ASSERT_EQ(target_ctx.lob_col_idx_, result.meta_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.meta_info_.lob_id_);
      ASSERT_EQ(0, result.meta_info_.seq_id_.compare(piece_seq_ids.at(meta_count - 1)));
      ++meta_count;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(data.length(), data_offset);
  ASSERT_EQ(2, piece_count);
  ASSERT_EQ(3, meta_count);
  ASSERT_TRUE(write_iter.is_finished());

  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
  ASSERT_TRUE(pending_locator.is_outline_lob_disk_locator());
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, pending_locator.get_disk_locator(disk_locator));
  ObLobDiskLocatorWrapper locator_wrapper;
  ASSERT_EQ(OB_SUCCESS, locator_wrapper.init(disk_locator.ptr(), disk_locator.length()));
  ASSERT_EQ(target_ctx.lob_id_, locator_wrapper.get_lob_id());
  ASSERT_EQ(target_ctx.seq_no_, locator_wrapper.get_seq_no());

  write_iter.reset();
  result_allocator.reuse();
  ASSERT_EQ(target_ctx.lob_id_, locator_wrapper.get_lob_id());
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(0, result.piece_info_.seq_id_.compare(piece_seq_ids.at(0)));
  write_iter.reset();
}

TEST_F(TestLobChunkSpliter, lob_write_iter_accumulates_utf8mb4_char_len)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  const int64_t emoji_count = chunk_size / 4 * 2 + 17;
  ObString data = make_data(source_allocator, "\xf0\x9f\x98\x80", emoji_count);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_UTF8MB4_GENERAL_CI,
      data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  target_ctx.lob_col_idx_ = 6;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2007);
  target_ctx.seq_no_ = transaction::ObTxSEQ(15, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  int64_t piece_count = 0;
  int64_t meta_chunk_count = 0;
  bool summary_seen = false;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
    if (result.is_piece()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(0, result.piece_info_.lob_data_.length() % 4);
      ++piece_count;
    } else if (result.is_meta_summary()) {
      ObLobMetaExtraInfo extra_info;
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.byte_len_);
      ASSERT_EQ(static_cast<uint64_t>(emoji_count), result.meta_info_.char_len_);
      ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(external_info_allocator_,
          result.meta_info_.lob_extra_info_));
      ASSERT_EQ(static_cast<uint64_t>(piece_count), extra_info.chunk_count_);
      summary_seen = true;
    } else {
      ASSERT_TRUE(result.is_meta_chunk());
      ASSERT_TRUE(summary_seen);
      ++meta_chunk_count;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(3, piece_count);
  ASSERT_EQ(piece_count, meta_chunk_count);
  ASSERT_TRUE(summary_seen);
  ASSERT_EQ(static_cast<uint64_t>(data.length()), write_iter.write_byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(emoji_count), write_iter.write_char_len_);
}

TEST_F(TestLobChunkSpliter, lob_write_iter_rebuilds_target_seq_id)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObString data = make_data(source_allocator, "a", 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 3;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2005);
  target_ctx.seq_no_ = transaction::ObTxSEQ(13, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));

  char source_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString source_seq_id(sizeof(source_seq_buf), source_seq_buf);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(9, source_seq_id));
  ObLobChunk source_chunk;
  source_chunk.seq_id_ = source_seq_id;
  source_chunk.byte_len_ = data.length();
  source_chunk.char_len_ = data.length();
  source_chunk.lob_data_ = data;

  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.build_chunk_write_result_(source_chunk, result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(target_ctx.lob_col_idx_, result.piece_info_.lob_col_idx_);
  ASSERT_EQ(target_ctx.lob_id_, result.piece_info_.lob_id_);
  ASSERT_NE(0, source_seq_id.compare(result.piece_info_.seq_id_));
  ASSERT_EQ(1, write_iter.chunk_meta_infos_.count());
  ASSERT_EQ(0, result.piece_info_.seq_id_.compare(
      write_iter.chunk_meta_infos_.at(0).seq_id_));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_empty_lob_has_summary_only)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(source_allocator, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, 0, read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 1;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2003);
  target_ctx.seq_no_ = transaction::ObTxSEQ(11, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_meta_summary());
  ASSERT_EQ(0u, result.meta_info_.byte_len_);
  ASSERT_EQ(0u, result.meta_info_.char_len_);
  ObLobMetaExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(external_info_allocator_,
      result.meta_info_.lob_extra_info_));
  ASSERT_EQ(0u, extra_info.chunk_count_);
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));

  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_failure_does_not_publish_locator)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  ObString data = make_data(source_allocator, "a", chunk_size + 1);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 2;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2004);
  target_ctx.seq_no_ = transaction::ObTxSEQ(12, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());

  ++write_iter.source_byte_len_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, write_iter.get_next_row(result));
  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_reset_clears_finished_state)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObString data = make_data(source_allocator, "a", 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 4;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2006);
  target_ctx.seq_no_ = transaction::ObTxSEQ(14, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(write_iter.is_finished());
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));

  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
  ASSERT_TRUE(pending_locator.is_outline_lob_disk_locator());

  write_iter.reset();
  ASSERT_FALSE(write_iter.is_finished());
  ASSERT_EQ(OB_NOT_INIT, write_iter.get_next_row(result));
  ASSERT_EQ(OB_NOT_INIT, write_iter.get_pending_locator(pending_locator));
}

TEST_F(TestLobChunkSpliter, outtable_redo_payload_preserves_append_prefix_and_tail)
{
  ObArenaAllocator allocator;
  ObArenaAllocator payload_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  const ObString prefix = make_data(allocator, "a", chunk_size - 17);
  const ObString append_data = make_data(allocator, "b", 34);
  ObString first_expected = make_data(allocator, "a", chunk_size);
  MEMSET(first_expected.ptr() + prefix.length(), 'b', 17);
  const ObString tail_expected(17, append_data.ptr() + 17);
  ObLobAppendSourceIter source;
  ASSERT_EQ(OB_SUCCESS, source.open(&allocator, prefix, append_data, CS_TYPE_BINARY));
  ObLobInsertContext ctx;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_context(ctx));
  ObLobMetaInfo first_meta;
  ObLobMetaInfo tail_meta;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_meta(allocator, first_expected, first_meta));
  ASSERT_NO_FATAL_FAILURE(prepare_redo_meta(allocator, tail_expected, tail_meta));
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, 2, tail_meta.seq_id_));
  ObLobMetaManager manager;
  ObLobOutTableRedoPayload first_payload;
  ObLobOutTableRedoPayload tail_payload;
  ASSERT_EQ(OB_SUCCESS, manager.prepare_outtable_redo_payload_(
      ctx, &source, first_meta, 50001, true, payload_allocator, first_payload));
  ASSERT_EQ(first_expected, first_payload.data_);
  ASSERT_NE(source.buffer_.ptr(), first_payload.data_.ptr());
  ASSERT_EQ(OB_SUCCESS, manager.prepare_outtable_redo_payload_(
      ctx, &source, tail_meta, 50001, true, payload_allocator, tail_payload));
  EXPECT_EQ(tail_expected, tail_payload.data_);
  EXPECT_EQ(first_expected, first_payload.data_);
  EXPECT_EQ(first_meta.seq_id_, first_payload.seq_id_);
  EXPECT_EQ(tail_meta.seq_id_, tail_payload.seq_id_);
  EXPECT_EQ(ctx.insert_lob_seq_no_, first_payload.lob_operation_seq_no_);
  EXPECT_EQ(ctx.insert_lob_seq_no_, tail_payload.lob_operation_seq_no_);
  EXPECT_TRUE(first_payload.is_valid());
  EXPECT_TRUE(tail_payload.is_valid());
}

TEST_F(TestLobChunkSpliter, outtable_redo_payload_nologging_does_not_read_source)
{
  ObArenaAllocator allocator;
  const ObString data = make_data(allocator, "a", 17);
  ObLobInsertContext ctx;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_context(ctx));
  ObLobMetaInfo meta;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_meta(allocator, data, meta));
  ObLobMetaManager manager;
  ObLobOutTableRedoPayload payload;
  ASSERT_EQ(OB_SUCCESS, manager.prepare_outtable_redo_payload_(
      ctx, nullptr, meta, 50001, false, allocator, payload));
  EXPECT_TRUE(payload.is_valid());
  EXPECT_FALSE(payload.is_logging_);
  EXPECT_TRUE(payload.data_.empty());
  EXPECT_EQ(17, payload.byte_len_);

  ObLobAppendSourceIter source;
  ASSERT_EQ(OB_SUCCESS, source.open(&allocator, ObString(), data, CS_TYPE_BINARY));
  ASSERT_EQ(OB_SUCCESS, manager.prepare_outtable_redo_payload_(
      ctx, &source, meta, 50001, false, allocator, payload));
  EXPECT_EQ(0, source.first_buffer_pos_);
  EXPECT_EQ(0, source.second_buffer_pos_);
  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, source.get_next_chunk(chunk, ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE));
  EXPECT_EQ(data, chunk.lob_data_);
}

TEST_F(TestLobChunkSpliter, outtable_redo_payload_rejects_short_source_and_summary)
{
  ObArenaAllocator allocator;
  const ObString data = make_data(allocator, "a", 17);
  ObLobInsertContext ctx;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_context(ctx));
  ObLobMetaInfo meta;
  ASSERT_NO_FATAL_FAILURE(prepare_redo_meta(allocator, data, meta));
  ObLobMetaManager manager;
  ObLobOutTableRedoPayload payload;
  ObLobAppendSourceIter source;
  ASSERT_EQ(OB_SUCCESS, source.open(&allocator, ObString(), data, CS_TYPE_BINARY));
  ASSERT_EQ(OB_SUCCESS, manager.prepare_outtable_redo_payload_(
      ctx, &source, meta, 50001, true, allocator, payload));
  ASSERT_EQ(OB_ERR_UNEXPECTED, manager.prepare_outtable_redo_payload_(
      ctx, &source, meta, 50001, true, allocator, payload));
  EXPECT_FALSE(payload.is_valid());
  EXPECT_TRUE(payload.data_.empty());

  source.reset();
  const ObString short_data(16, data.ptr());
  ASSERT_EQ(OB_SUCCESS, source.open(&allocator, ObString(), short_data, CS_TYPE_BINARY));
  ASSERT_EQ(OB_ERR_UNEXPECTED, manager.prepare_outtable_redo_payload_(
      ctx, &source, meta, 50001, true, allocator, payload));
  EXPECT_FALSE(payload.is_valid());
  EXPECT_TRUE(payload.data_.empty());

  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.lob_chunk_type_.set_out_table();
  extra_info.compressed_len_ = data.length();
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, meta.lob_extra_info_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, manager.prepare_outtable_redo_payload_(
      ctx, nullptr, meta, 50001, false, allocator, payload));
  EXPECT_FALSE(payload.is_valid());
}

void TestLobChunkSpliter::prepare_redo_context(ObLobInsertContext &ctx)
{
  ctx.timeout_ = ObTimeUtility::current_time_us() + 60L * 1000L * 1000L;
  ctx.insert_lob_seq_no_ = transaction::ObTxSEQ(100, 0);
}

void TestLobChunkSpliter::prepare_redo_meta(ObIAllocator &allocator, const ObString &data, ObLobMetaInfo &meta)
{
  ObLobChunk chunk;
  chunk.lob_col_idx_ = 2;
  chunk.lob_id_.assign(2, 1001, 2002);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, 1, chunk.seq_id_));
  chunk.byte_len_ = data.length();
  chunk.char_len_ = data.length();
  chunk.lob_data_ = data;
  chunk.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  chunk.lob_extra_info_.lob_chunk_type_.set_out_table();
  chunk.lob_extra_info_.has_crc_ = true;
  chunk.lob_extra_info_.crc_ = ob_crc64(data.ptr(), data.length());
  chunk.lob_extra_info_.compressed_len_ = data.length();
  blocksstable::MacroBlockId macro_id;
  macro_id.set_version_v1();
  macro_id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  macro_id.set_second_id(100);
  macro_id.set_third_id(1001);
  macro_id.set_fourth_id(2002);
  macro_id.set_fifth_id(static_cast<int64_t>(chunk.lob_extra_info_.crc_));
  ASSERT_EQ(OB_SUCCESS,
      ObLobOutTableIO::serialize_macro_id(allocator, macro_id, chunk.lob_block_id_));
  ASSERT_EQ(OB_SUCCESS, chunk.to_meta(allocator, meta));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_chunk_spliter.log*");
  OB_LOGGER.set_file_name("test_lob_chunk_spliter.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

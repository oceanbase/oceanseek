/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "share/vector_index/ob_vector_index_segment.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/schema/ob_table_schema.h"
#undef private
#undef protected


namespace oceanbase {
namespace common {
class TestVectorIndexSerialize : public ::testing::Test
{
public:
  TestVectorIndexSerialize()
  {}
  ~TestVectorIndexSerialize()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexSerialize);
};

struct TestVectorIndexParamPair
{
  OB_UNIS_VERSION(1);
public:
  share::ObVectorIndexParam first_;
  share::ObVectorIndexParam second_;
};

OB_SERIALIZE_MEMBER(TestVectorIndexParamPair, first_, second_);

struct TestOStreamCbParam : public share::ObOStreamBuf::CbParam {
  TestOStreamCbParam()
    : total_size_(0)
  {}
  virtual ~TestOStreamCbParam() {}
  virtual share::ObOStreamBuf::CbParam::CbParamType get_type() const override
  {
    return share::ObOStreamBuf::CbParam::CbParamType::HNSW_SERIALIZE;
  }
  int64_t total_size_;
};

class SerializeCallback {
public:
  SerializeCallback(int ret = OB_SUCCESS)
    : ret_(ret)
  {}
  int operator()(const char* data, const int64_t data_size, share::ObOStreamBuf::CbParam &cb_param) {
    if (OB_SUCCESS == ret_) {
      ObString str(data_size, data);
      LOG_INFO("output", K(str));
      TestOStreamCbParam* param = dynamic_cast<TestOStreamCbParam*>(&cb_param);
      param->total_size_ += data_size;
    }
    return ret_;
  }
private:
  int ret_;
};

static const char* data_str = "unittest: test plugin vector index serialize";

TEST_F(TestVectorIndexSerialize, serialize)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestOStreamCbParam cb_param;
  SerializeCallback callback;
  share::ObOStreamBuf::Callback func = callback;
  share::ObOStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::ostream out(&streambuf);
  out.write(data_str, strlen(data_str));
  streambuf.check_finish();
  ASSERT_EQ(OB_SUCCESS, streambuf.get_error_code());
  ASSERT_EQ(strlen(data_str), cb_param.total_size_);
}

TEST_F(TestVectorIndexSerialize, serialize_failed)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestOStreamCbParam cb_param;
  SerializeCallback callback(OB_ERR_UNEXPECTED);
  share::ObOStreamBuf::Callback func = callback;
  share::ObOStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::ostream out(&streambuf);
  out.write(data_str, strlen(data_str));
  ASSERT_EQ(OB_ERR_UNEXPECTED, streambuf.get_error_code());
  ASSERT_EQ(0, cb_param.total_size_);
}

TEST_F(TestVectorIndexSerialize, bitmap_serialize_buffer_overflow)
{
  const int64_t SMALL_BUF_SIZE = 64;
  ObArenaAllocator allocator(ObModIds::TEST);
  share::ObVectorIndexRoaringBitMap bitmap(&allocator, OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(true, false));
  ASSERT_NE(nullptr, bitmap.insert_bitmap_);
  {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(OB_SYS_TENANT_ID, "VecSerTest"));
    for (uint64_t i = 0; i < 1024; ++i) {
      roaring::api::roaring64_bitmap_add(bitmap.insert_bitmap_, i * 2);
    }
  }

  const int64_t serialize_size = bitmap.get_serialize_size();
  ASSERT_GT(serialize_size, SMALL_BUF_SIZE);
  char *buf = static_cast<char *>(allocator.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 0x5A, serialize_size);

  int64_t pos = 0;
  EXPECT_EQ(OB_SIZE_OVERFLOW, bitmap.serialize(buf, SMALL_BUF_SIZE, pos));
  for (int64_t i = SMALL_BUF_SIZE; i < serialize_size; ++i) {
    EXPECT_EQ(static_cast<char>(0x5A), buf[i]);
  }

  pos = 0;
  EXPECT_EQ(OB_SUCCESS, bitmap.serialize(buf, serialize_size, pos));
  EXPECT_EQ(serialize_size, pos);
  {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(OB_SYS_TENANT_ID, "VecSerTest"));
    roaring::api::roaring64_bitmap_free(bitmap.insert_bitmap_);
  }
  bitmap.insert_bitmap_ = nullptr;
}

struct TestIStreamCbParam : public share::ObIStreamBuf::CbParam {
  TestIStreamCbParam()
    : cur_(0)
  {}
  virtual ~TestIStreamCbParam() {}
  virtual share::ObIStreamBuf::CbParam::CbParamType get_type() const override
  {
    return share::ObIStreamBuf::CbParam::CbParamType::HNSW_DESERIALIZE;
  }
  int64_t cur_;
};

class DeserializeCallback {
public:
  DeserializeCallback(int ret = OB_SUCCESS)
    : ret_(ret)
  {}
  int operator()(char*& data, const int64_t data_size, int64_t &read_size, share::ObIStreamBuf::CbParam &cb_param) {
    if (OB_SUCCESS == ret_) {
      TestIStreamCbParam* param = dynamic_cast<TestIStreamCbParam*>(&cb_param);
      read_size = MIN(strlen(data_str) - param->cur_, data_size);
      if (read_size) {
        data = const_cast<char*>(data_str + param->cur_);
        param->cur_ += read_size;
        ObString str(read_size, data);
        LOG_INFO("input", K(str));
      }
    }
    return ret_;
  }
private:
  int ret_;
};

TEST_F(TestVectorIndexSerialize, deserialize)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestIStreamCbParam cb_param;
  DeserializeCallback callback;
  share::ObIStreamBuf::Callback func = callback;
  share::ObIStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::istream in(&streambuf);
  char result[1024] = {0};
  in.read(result, strlen(data_str));
  ASSERT_EQ(OB_SUCCESS, streambuf.get_error_code());
  ASSERT_EQ(strlen(data_str), strlen(result));
}

TEST_F(TestVectorIndexSerialize, deserialize_failed)
{
  const int MAX_BUF_SIZE = 8LL;
  char data[MAX_BUF_SIZE] = {0};
  TestIStreamCbParam cb_param;
  DeserializeCallback callback(OB_ERR_UNEXPECTED);
  share::ObIStreamBuf::Callback func = callback;
  share::ObIStreamBuf streambuf(data, MAX_BUF_SIZE, cb_param, func);
  std::istream in(&streambuf);
  char result[1024] = {0};
  in.read(result, strlen(data_str));
  ASSERT_EQ(OB_ERR_UNEXPECTED, streambuf.get_error_code());
  ASSERT_EQ(0, strlen(result));
}

TEST_F(TestVectorIndexSerialize, rebuild_bq_inherits_refine_type_and_fht)
{
  // This regression must work without duplicate-vector support.
  struct RebuildCase {
    const char *old_type_;
    const char *new_type_;
    const char *old_options_;
    const char *new_options_;
    int expected_refine_type_;
    bool expected_fht_;
  };
  const RebuildCase cases[] = {
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE", "", 1, true},
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE",
       ", REFINE_TYPE=FP32, BQ_USE_FHT=FALSE", 0, false},
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE",
       ", REFINE_TYPE=FP32", 0, true},
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE",
       ", BQ_USE_FHT=FALSE", 1, false},
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=FP32, BQ_USE_FHT=FALSE", "", 0, false},
      {"HNSW_BQ", "HNSW_BQ", ", REFINE_TYPE=FP32, BQ_USE_FHT=FALSE",
       ", refine_type = sq8, bq_use_fht = true", 1, true},
      {"HNSW_BQ", "HNSW_BQ", "", "", 0, false},
      {"HNSW_BQ", "HNSW", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE", "", 0, false},
      {"HNSW_BQ", "HNSW_SQ", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE", "", 0, false},
      {"HNSW", "HNSW_BQ", "", "", 0, false},
      {"HNSW_SQ", "HNSW_BQ", "", ", REFINE_TYPE=SQ8, BQ_USE_FHT=TRUE", 1, true},
  };
  share::schema::ObTableSchema data_schema;
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(i);
    ObArenaAllocator allocator("BqRebuildTest");
    char old_buf[512];
    char new_buf[512];
    snprintf(old_buf, sizeof(old_buf),
        "TYPE=%s, DISTANCE=L2, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=500%s",
        cases[i].old_type_, cases[i].old_options_);
    snprintf(new_buf, sizeof(new_buf), "TYPE=%s, DISTANCE=L2, EF_SEARCH=400%s",
        cases[i].new_type_, cases[i].new_options_);
    ObString new_params = ObString::make_string(new_buf);
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::construct_rebuild_index_param(
        data_schema, ObString::make_string(old_buf), new_params,
        share::VIT_HNSW_INDEX, &allocator, 8));
    share::ObVectorIndexParam parsed;
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
        new_params, share::VIT_HNSW_INDEX, parsed, false));
    ASSERT_EQ(cases[i].expected_refine_type_, parsed.refine_type_);
    ASSERT_EQ(cases[i].expected_fht_, parsed.bq_use_fht_);
    ASSERT_EQ(16, parsed.m_);
    ASSERT_EQ(200, parsed.ef_construction_);
    ASSERT_EQ(400, parsed.ef_search_);
  }
}

TEST_F(TestVectorIndexSerialize, rebuild_bq_options_require_full_rebuild)
{
  share::schema::ObTableSchema index_schema;
  index_schema.set_index_type(share::schema::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL);
  const ObString old_params = ObString::make_string(
      "TYPE=HNSW_BQ,DISTANCE=L2,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64,REFINE_TYPE=SQ8,BQ_USE_FHT=TRUE");
  const char *options[] = {
      "REFINE_TYPE=SQ8,BQ_USE_FHT=TRUE",
      "REFINE_TYPE=FP32,BQ_USE_FHT=TRUE",
      "REFINE_TYPE=SQ8,BQ_USE_FHT=FALSE",
      "REFINE_TYPE=FP32,BQ_USE_FHT=FALSE",
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(options); ++i) {
    SCOPED_TRACE(i);
    char new_buf[512];
    snprintf(new_buf, sizeof(new_buf),
        "TYPE=HNSW_BQ,DISTANCE=L2,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=100,%s", options[i]);
    bool search_only = false;
    bool has_change = false;
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_only_change_search_params(
        old_params, ObString::make_string(new_buf), index_schema, search_only, has_change));
    EXPECT_EQ(i == 0, search_only);
  }
}

static int serialize_legacy_vector_index_param(
    const share::ObVectorIndexParam &param,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              param.type_,
              param.lib_,
              param.dist_algorithm_,
              param.dim_,
              param.m_,
              param.ef_construction_,
              param.ef_search_,
              param.extra_info_max_size_,
              param.extra_info_actual_size_,
              param.refine_type_,
              param.bq_bits_query_,
              param.refine_k_,
              param.bq_use_fht_,
              param.sync_interval_type_,
              param.sync_interval_value_,
              param.prune_,
              param.refine_,
              param.ob_sparse_drop_ratio_build_,
              param.window_size_,
              param.ob_sparse_drop_ratio_search_);
  OB_UNIS_ENCODE_ARRAY(param.endpoint_, OB_MAX_ENDPOINT_LENGTH);
  LST_DO_CODE(OB_UNIS_ENCODE, param.similarity_threshold_, param.content_type_);
  return ret;
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_legacy_false_golden_bytes)
{
  share::ObVectorIndexParam legacy_param;
  legacy_param.reset();
  share::ObVectorIndexParam duplicate_param;
  ASSERT_EQ(OB_SUCCESS, duplicate_param.assign(legacy_param));
  duplicate_param.support_duplicate_ = true;
  char baseline_buf[4096] = {0};
  char legacy_buf[4096] = {0};
  char duplicate_buf[4096] = {0};
  int64_t baseline_pos = 0;
  int64_t legacy_pos = 0;
  int64_t duplicate_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialize_legacy_vector_index_param(
      legacy_param, baseline_buf, sizeof(baseline_buf), baseline_pos));
  ASSERT_EQ(OB_SUCCESS, legacy_param.serialize(
      legacy_buf, sizeof(legacy_buf), legacy_pos));
  ASSERT_EQ(OB_SUCCESS, duplicate_param.serialize(
      duplicate_buf, sizeof(duplicate_buf), duplicate_pos));

  int64_t legacy_body_pos = 0;
  int64_t legacy_body_len = 0;
  {
    int ret = OB_SUCCESS;
    const char *buf = legacy_buf;
    const int64_t data_len = legacy_pos;
    int64_t pos = 0;
    int64_t version = 0;
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(legacy_body_len);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, version);
    legacy_body_pos = pos;
  }
  int64_t duplicate_body_pos = 0;
  int64_t duplicate_body_len = 0;
  {
    int ret = OB_SUCCESS;
    const char *buf = duplicate_buf;
    const int64_t data_len = duplicate_pos;
    int64_t pos = 0;
    int64_t version = 0;
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(duplicate_body_len);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, version);
    duplicate_body_pos = pos;
  }
  ASSERT_EQ(baseline_pos, legacy_body_len);
  ASSERT_EQ(legacy_pos, legacy_body_pos + legacy_body_len);
  ASSERT_EQ(0, MEMCMP(baseline_buf, legacy_buf + legacy_body_pos, legacy_body_len));
  ASSERT_GT(duplicate_body_len, legacy_body_len);
  ASSERT_EQ(duplicate_pos, duplicate_body_pos + duplicate_body_len);
  ASSERT_EQ(0, MEMCMP(baseline_buf, duplicate_buf + duplicate_body_pos, legacy_body_len));
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_optional_tail)
{
  share::ObVectorIndexParam legacy_param;
  legacy_param.reset();
  share::ObVectorIndexParam duplicate_param;
  ASSERT_EQ(OB_SUCCESS, duplicate_param.assign(legacy_param));
  duplicate_param.support_duplicate_ = true;

  share::ObVectorIndexParam copied_duplicate_param;
  ASSERT_EQ(OB_SUCCESS, copied_duplicate_param.assign(duplicate_param));
  ASSERT_TRUE(copied_duplicate_param.support_duplicate_);
  copied_duplicate_param.reset();
  ASSERT_FALSE(copied_duplicate_param.support_duplicate_);

  const int64_t legacy_size = legacy_param.get_serialize_size();
  const int64_t duplicate_size = duplicate_param.get_serialize_size();
  ASSERT_GT(duplicate_size, legacy_size);

  char legacy_buf[4096] = {0};
  char duplicate_buf[4096] = {0};
  int64_t legacy_pos = 0;
  int64_t duplicate_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy_param.serialize(legacy_buf, sizeof(legacy_buf), legacy_pos));
  ASSERT_EQ(OB_SUCCESS, duplicate_param.serialize(duplicate_buf, sizeof(duplicate_buf), duplicate_pos));
  ASSERT_EQ(legacy_size, legacy_pos);
  ASSERT_EQ(duplicate_size, duplicate_pos);

  share::ObVectorIndexParam decoded_legacy;
  decoded_legacy.support_duplicate_ = true;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_legacy.deserialize(legacy_buf, legacy_size, decode_pos));
  ASSERT_FALSE(decoded_legacy.support_duplicate_);
  ASSERT_EQ(legacy_size, decode_pos);

  share::ObVectorIndexParam decoded_duplicate;
  decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_duplicate.deserialize(duplicate_buf, duplicate_size, decode_pos));
  ASSERT_TRUE(decoded_duplicate.support_duplicate_);
  ASSERT_EQ(duplicate_size, decode_pos);
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_optional_tail_contiguous_objects)
{
  share::ObVectorIndexParam legacy_param;
  legacy_param.reset();
  share::ObVectorIndexParam duplicate_param;
  ASSERT_EQ(OB_SUCCESS, duplicate_param.assign(legacy_param));
  duplicate_param.support_duplicate_ = true;

  const share::ObVectorIndexParam *const sequences[][2] = {
      {&legacy_param, &duplicate_param},
      {&duplicate_param, &legacy_param},
      {&legacy_param, &legacy_param},
  };
  const bool expected[][2] = {
      {false, true},
      {true, false},
      {false, false},
  };
  for (int64_t sequence_idx = 0;
       sequence_idx < static_cast<int64_t>(ARRAYSIZEOF(sequences));
       ++sequence_idx) {
    TestVectorIndexParamPair encoded_pair;
    ASSERT_EQ(OB_SUCCESS, encoded_pair.first_.assign(*sequences[sequence_idx][0]));
    ASSERT_EQ(OB_SUCCESS, encoded_pair.second_.assign(*sequences[sequence_idx][1]));
    char storage[8192] = {0};
    int64_t serialized_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
              encoded_pair.serialize(storage, sizeof(storage), serialized_pos));

    TestVectorIndexParamPair decoded_pair;
    decoded_pair.first_.support_duplicate_ = true;
    decoded_pair.second_.support_duplicate_ = true;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
              decoded_pair.deserialize(storage, serialized_pos, decode_pos));
    ASSERT_EQ(expected[sequence_idx][0], decoded_pair.first_.support_duplicate_);
    ASSERT_EQ(expected[sequence_idx][1], decoded_pair.second_.support_duplicate_);
    ASSERT_EQ(serialized_pos, decode_pos);
  }
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_internal_parser)
{
  share::ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE"),
      share::ObVectorIndexType::VIT_HNSW_INDEX, param));
  ASSERT_FALSE(param.support_duplicate_);

  param.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE, SUPPORT_DUPLICATE=TRUE"),
      share::ObVectorIndexType::VIT_HNSW_INDEX, param));

  param.reset();
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE, DUPLICATE_GROUP=TRUE"),
      share::ObVectorIndexType::VIT_HNSW_INDEX, param));
  ASSERT_TRUE(param.support_duplicate_);

  param.reset();
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE, DUPLICATE_GROUP=FALSE"),
      share::ObVectorIndexType::VIT_HNSW_INDEX, param));
  ASSERT_FALSE(param.support_duplicate_);

  param.reset();
  ASSERT_EQ(OB_NOT_SUPPORTED, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE, DUPLICATE_GROUP=MAYBE"),
      share::ObVectorIndexType::VIT_HNSW_INDEX, param));

  // DDL compatibility checks may parse serialized HNSW parameters through a
  // generic/IVF category.  The persisted TYPE, rather than that call-site
  // category, determines whether DUPLICATE_GROUP is valid.
  param.reset();
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=HNSW, DISTANCE=COSINE, DUPLICATE_GROUP=TRUE"),
      share::ObVectorIndexType::VIT_IVF_INDEX, param, false));
  ASSERT_TRUE(param.support_duplicate_);

  param.reset();
  ASSERT_EQ(OB_NOT_SUPPORTED, share::ObVectorIndexUtil::parser_params_from_string(
      ObString::make_string("TYPE=IVF_FLAT, DISTANCE=COSINE, DUPLICATE_GROUP=TRUE"),
      share::ObVectorIndexType::VIT_IVF_INDEX, param, false));
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_log_name)
{
  share::ObVectorIndexParam param;
  param.type_ = share::VIAT_HNSW;
  param.support_duplicate_ = true;
  char log_buf[2048] = {0};
  const int64_t log_len = param.to_string(log_buf, sizeof(log_buf));
  ASSERT_GT(ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, log_buf, log_len,
      "DUPLICATE_GROUP", sizeof("DUPLICATE_GROUP") - 1, 1), 0U);
  ASSERT_EQ(0U, ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, log_buf, log_len,
      "SUPPORT_DUPLICATE", sizeof("SUPPORT_DUPLICATE") - 1, 1));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, param.print_to_string(log_buf, sizeof(log_buf), pos));
  ASSERT_GT(ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, log_buf, pos,
      "DUPLICATE_GROUP=true", sizeof("DUPLICATE_GROUP=true") - 1, 1), 0U);
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_physical_build_type)
{
  ASSERT_EQ(share::VIAT_HNSW, share::ObVectorIndexUtil::get_physical_build_type(
      share::VIAT_HNSW, false));
  ASSERT_EQ(share::VIAT_HGRAPH, share::ObVectorIndexUtil::get_physical_build_type(
      share::VIAT_HNSW, true));
  ASSERT_EQ(share::VIAT_HNSW_SQ, share::ObVectorIndexUtil::get_physical_build_type(
      share::VIAT_HNSW_SQ, true));
  ASSERT_EQ(share::VIAT_HNSW_BQ, share::ObVectorIndexUtil::get_physical_build_type(
      share::VIAT_HNSW_BQ, true));
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_visible_in_public_ddl)
{
  share::schema::ObTableSchema index_schema;
  ASSERT_EQ(OB_SUCCESS, index_schema.set_index_params(ObString::make_string(
      "TYPE=HNSW, DISTANCE=COSINE, DUPLICATE_GROUP=TRUE, EXTRA_INFO_ACTUAL_SIZE=8")));
  index_schema.set_index_type(share::schema::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL);
  char ddl_buf[1024] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::print_index_param(
      index_schema, ddl_buf, sizeof(ddl_buf), pos));
  const ObString public_params(static_cast<int32_t>(pos), ddl_buf);
  ASSERT_GT(ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, public_params.ptr(), public_params.length(),
      "DUPLICATE_GROUP=TRUE", sizeof("DUPLICATE_GROUP=TRUE") - 1, 1), 0U);
  ASSERT_EQ(0U, ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, public_params.ptr(), public_params.length(),
      "EXTRA_INFO_ACTUAL_SIZE", 22, 1));
  ASSERT_GT(ObCharset::locate(
      CS_TYPE_UTF8MB4_GENERAL_CI, public_params.ptr(), public_params.length(),
      "TYPE=HNSW", 9, 1), 0U);

}

TEST_F(TestVectorIndexSerialize, duplicate_mode_change_is_never_search_only)
{
  static const char *BASE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64";
  static const char *BASE_TRUE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=TRUE";
  static const char *SEARCH_ONLY =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=128";
  share::schema::ObTableSchema index_schema;
  index_schema.set_index_type(share::schema::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL);
  bool only_change_search_params = true;
  bool has_change = false;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_only_change_search_params(
      ObString::make_string(BASE), ObString::make_string(BASE_TRUE),
      index_schema, only_change_search_params, has_change));
  ASSERT_FALSE(only_change_search_params);
  only_change_search_params = true;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_only_change_search_params(
      ObString::make_string(BASE_TRUE), ObString::make_string(BASE),
      index_schema, only_change_search_params, has_change));
  ASSERT_FALSE(only_change_search_params);
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_only_change_search_params(
      ObString::make_string(BASE), ObString::make_string(SEARCH_ONLY),
      index_schema, only_change_search_params, has_change));
  ASSERT_TRUE(only_change_search_params);
}

// A SUPPORT_DUPLICATE toggle converts the persisted graph format, but it does
// NOT change the embedding input. check_need_embedding_when_rebuild() must stay
// a pure "does this rebuild have to re-run the embedding model" decision and
// must never report a format-only change; ObVectorIndexRefresher::do_rebuild()
// keeps the format-conversion decision itself and refuses the hybrid
// optional-task shortcut for it.
TEST_F(TestVectorIndexSerialize, duplicate_mode_change_is_not_embedding_related)
{
  static const char *BASE =
      "DISTANCE=L2, TYPE=HNSW, LIB=VSAG, MODEL=QA_M, DIM=64, SYNC_MODE=IMMEDIATE, "
      "M=16, EF_CONSTRUCTION=200, EF_SEARCH=500, SYNC_INTERVAL=10s";
  static const char *BASE_TRUE =
      "DISTANCE=L2, TYPE=HNSW, LIB=VSAG, MODEL=QA_M, DIM=64, SYNC_MODE=IMMEDIATE, "
      "M=16, EF_CONSTRUCTION=200, EF_SEARCH=500, SYNC_INTERVAL=10s, DUPLICATE_GROUP=TRUE";
  static const char *SEARCH_ONLY =
      "DISTANCE=L2, TYPE=HNSW, LIB=VSAG, MODEL=QA_M, DIM=64, SYNC_MODE=IMMEDIATE, "
      "M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, SYNC_INTERVAL=10s";
  static const char *MODEL_ONLY =
      "DISTANCE=L2, TYPE=HNSW, LIB=VSAG, MODEL=QA_M2, DIM=64, SYNC_MODE=IMMEDIATE, "
      "M=16, EF_CONSTRUCTION=200, EF_SEARCH=500, SYNC_INTERVAL=10s";
  share::schema::ObTableSchema hybrid_schema;
  hybrid_schema.set_index_type(share::schema::INDEX_TYPE_HYBRID_INDEX_LOG_LOCAL);

  bool need_embedding = true;
  // FALSE -> TRUE (legacy HNSW -> duplicate-capable HGRAPH): format only.
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE), ObString::make_string(BASE_TRUE),
      hybrid_schema, need_embedding));
  ASSERT_FALSE(need_embedding);
  // TRUE -> FALSE (duplicate-capable -> legacy): format only.
  need_embedding = true;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE_TRUE), ObString::make_string(BASE),
      hybrid_schema, need_embedding));
  ASSERT_FALSE(need_embedding);
  // Unchanged mode: format only as well.
  need_embedding = true;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE_TRUE), ObString::make_string(BASE_TRUE),
      hybrid_schema, need_embedding));
  ASSERT_FALSE(need_embedding);
  // EF_SEARCH only: search-param change, still not an embedding change.
  need_embedding = true;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE), ObString::make_string(SEARCH_ONLY),
      hybrid_schema, need_embedding));
  ASSERT_FALSE(need_embedding);
  // A genuine embedding input change must still be reported.
  need_embedding = false;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE), ObString::make_string(MODEL_ONLY),
      hybrid_schema, need_embedding));
  ASSERT_TRUE(need_embedding);

  // Non-hybrid schema: helper is a no-op and never reports a rebuild.
  share::schema::ObTableSchema plain_schema;
  plain_schema.set_index_type(share::schema::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL);
  need_embedding = true;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::check_need_embedding_when_rebuild(
      ObString::make_string(BASE), ObString::make_string(BASE_TRUE),
      plain_schema, need_embedding));
  ASSERT_FALSE(need_embedding);
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_capability_version)
{
  static const uint64_t CAPABILITY_VERSION =
      share::ObVectorIndexUtil::HGRAPH_DUPLICATE_VECTOR_VERSION;
  ASSERT_EQ(oceanbase::common::cal_version(4, 6, 3, 0), CAPABILITY_VERSION);
  ASSERT_LT(CLUSTER_VERSION_4_6_2_0, CAPABILITY_VERSION);
  ASSERT_LT(CLUSTER_VERSION_4_6_2_1, CAPABILITY_VERSION);
}

TEST_F(TestVectorIndexSerialize, duplicate_mode_rebuild_matrix)
{
  static const char *BASE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64";
  static const char *BASE_TRUE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=TRUE";
  static const char *EXPLICIT_FALSE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=FALSE";
  static const char *EXPLICIT_TRUE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=TRUE";
  struct RebuildCase {
    const char *old_params_;
    const char *new_params_;
    bool expected_support_duplicate_;
    bool expected_support_duplicate_changed_;
  };
  const RebuildCase cases[] = {
      {BASE, BASE, false, false},
      {BASE, EXPLICIT_FALSE, false, false},
      {BASE, EXPLICIT_TRUE, true, true},
      {BASE_TRUE, BASE, true, false},
      {BASE_TRUE, EXPLICIT_TRUE, true, false},
      {BASE_TRUE, EXPLICIT_FALSE, false, true},
  };
  share::schema::ObTableSchema data_schema;
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    ObArenaAllocator allocator("DupRebuild");
    ObString new_params = ObString::make_string(cases[i].new_params_);
    bool support_duplicate_changed = false;
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::construct_rebuild_index_param(
        data_schema,
        ObString::make_string(cases[i].old_params_),
        new_params,
        share::ObVectorIndexType::VIT_HNSW_INDEX,
        &allocator,
        0,
        &support_duplicate_changed));
    ASSERT_EQ(cases[i].expected_support_duplicate_changed_, support_duplicate_changed);
    share::ObVectorIndexParam parsed;
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
        new_params, share::ObVectorIndexType::VIT_HNSW_INDEX, parsed, false));
    ASSERT_EQ(cases[i].expected_support_duplicate_, parsed.support_duplicate_);
    const uint32_t marker_pos = ObCharset::locate(
        CS_TYPE_UTF8MB4_GENERAL_CI,
        new_params.ptr(),
        new_params.length(),
        "DUPLICATE_GROUP",
        sizeof("DUPLICATE_GROUP") - 1,
        1);
    ASSERT_EQ(cases[i].expected_support_duplicate_, marker_pos > 0);
    if (cases[i].expected_support_duplicate_) {
      const uint32_t second_marker_pos = ObCharset::locate(
          CS_TYPE_UTF8MB4_GENERAL_CI,
          new_params.ptr(),
          new_params.length(),
          "DUPLICATE_GROUP",
          sizeof("DUPLICATE_GROUP") - 1,
          marker_pos + 1);
      ASSERT_EQ(0U, second_marker_pos);
    }
  }

}

TEST_F(TestVectorIndexSerialize, duplicate_mode_rebuild_param_validation)
{
  static const char *BASE =
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64";
  share::schema::ObTableSchema data_schema;
  ObArenaAllocator allocator("DupRbInvalid");
  ObString invalid = ObString::make_string(
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=MAYBE");
  ASSERT_EQ(OB_NOT_SUPPORTED,
            share::ObVectorIndexUtil::construct_rebuild_index_param(
                data_schema, ObString::make_string(BASE), invalid,
                share::ObVectorIndexType::VIT_HNSW_INDEX, &allocator));

  ObString duplicate = ObString::make_string(
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, DUPLICATE_GROUP=TRUE, DUPLICATE_GROUP=FALSE");
  ASSERT_EQ(OB_ERR_PARAM_DUPLICATE,
            share::ObVectorIndexUtil::construct_rebuild_index_param(
                data_schema, ObString::make_string(BASE), duplicate,
                share::ObVectorIndexType::VIT_HNSW_INDEX, &allocator));

  ObString legacy_request = ObString::make_string(
      "TYPE=HNSW, DISTANCE=COSINE, SUPPORT_DUPLICATE=TRUE");
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            share::ObVectorIndexUtil::construct_rebuild_index_param(
                data_schema, ObString::make_string(BASE), legacy_request,
                share::ObVectorIndexType::VIT_HNSW_INDEX, &allocator));

  ObString value_only = ObString::make_string(
      "TYPE=HNSW, DISTANCE=COSINE, LIB=VSAG, M=16, EF_CONSTRUCTION=200, EF_SEARCH=64, MODEL=DUPLICATE_GROUP");
  bool support_duplicate_changed = true;
  ASSERT_EQ(OB_NOT_SUPPORTED,
            share::ObVectorIndexUtil::construct_rebuild_index_param(
                data_schema, ObString::make_string(BASE), value_only,
                share::ObVectorIndexType::VIT_HNSW_INDEX, &allocator, 0,
                &support_duplicate_changed));
  ASSERT_FALSE(support_duplicate_changed);
  share::ObVectorIndexParam value_only_param;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexUtil::parser_params_from_string(
      value_only, share::ObVectorIndexType::VIT_HNSW_INDEX,
      value_only_param, false));
  ASSERT_FALSE(value_only_param.support_duplicate_);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

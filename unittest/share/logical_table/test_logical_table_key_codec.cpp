/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#define private public
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#undef private

using namespace std;
using namespace oceanbase::common;
namespace oceanbase {
namespace share {

class TestLogicalTableKeyCodec : public ::testing::Test {
public:
  TestLogicalTableKeyCodec() : arena_(ObModIds::TEST) {}
  ~TestLogicalTableKeyCodec() {}
  virtual void SetUp() { arena_.reset(); }
  virtual void TearDown() {}

protected:
  ObArenaAllocator arena_;

  // Helper: encode a uint64 datum
  ObDatum make_uint64_datum(uint64_t val)
  {
    uint64_t *p = static_cast<uint64_t *>(arena_.alloc(sizeof(uint64_t)));
    *p = val;
    return ObDatum(reinterpret_cast<char *>(p), sizeof(uint64_t), false);
  }

  // Helper: encode an int64 datum
  ObDatum make_int64_datum(int64_t val)
  {
    int64_t *p = static_cast<int64_t *>(arena_.alloc(sizeof(int64_t)));
    *p = val;
    return ObDatum(reinterpret_cast<char *>(p), sizeof(int64_t), false);
  }

  // Helper: encode a varchar datum
  ObDatum make_varchar_datum(const char *str, int32_t len)
  {
    char *p = static_cast<char *>(arena_.alloc(len));
    MEMCPY(p, str, len);
    return ObDatum(p, len, false);
  }

  ObDatum make_varchar_datum(const ObString &str)
  {
    return make_varchar_datum(str.ptr(), str.length());
  }

  // IVF logical keys persist the fixed-width, network-order ObCenterId representation.
  ObDatum make_cid_datum(uint64_t center_id)
  {
    unsigned char *p = static_cast<unsigned char *>(
        arena_.alloc(OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH));
    MEMSET(p, 0, OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH);
    for (int64_t i = 0; i < static_cast<int64_t>(sizeof(center_id)); ++i) {
      p[OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH - 1 - i] =
          static_cast<unsigned char>((center_id >> (i * 8)) & 0xFF);
    }
    return ObDatum(reinterpret_cast<char *>(p),
                   OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH, false);
  }

  // Helper: make null datum
  ObDatum make_null_datum()
  {
    ObDatum d;
    d.set_null();
    return d;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(TestLogicalTableKeyCodec);
};


static void build_datum_array(const ObDatum *datums, const int64_t datum_cnt, ObSEArray<ObDatum, 8> &datum_array)
{
  ASSERT_GE(datum_cnt, 0);
  ASSERT_EQ(OB_SUCCESS, datum_array.reserve(datum_cnt));
  for (int64_t i = 0; i < datum_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, datum_array.push_back(datums[i]));
  }
}

#define ENCODE_KEY(prefix, datums, cnt, key)   do {     ObSEArray<ObDatum, 8> datum_array;     build_datum_array(datums, cnt, datum_array);     ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_key(prefix, datum_array, arena_, key));   } while (0)

#define EXPECT_ENCODE_KEY_RET(expect_ret, prefix, datums, cnt)   do {     ObSEArray<ObDatum, 8> datum_array;     ObString key;     if ((cnt) > 0 && nullptr != (datums)) { build_datum_array(datums, cnt, datum_array); }     EXPECT_EQ(expect_ret, ObLogicalTableEncoder::encode_key(prefix, datum_array, arena_, key));   } while (0)

#define CALC_KEY_LEN(len, aux_type, datums, cnt)   do {     ObSEArray<ObDatum, 8> datum_array;     build_datum_array(datums, cnt, datum_array);     ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::calc_encoded_key_len_(aux_type, datum_array, len));   } while (0)

#define EXPECT_CALC_KEY_LEN_RET(expect_ret, aux_type, datums, cnt, len)   do {     ObSEArray<ObDatum, 8> datum_array;     if ((cnt) > 0 && nullptr != (datums)) { build_datum_array(datums, cnt, datum_array); }     EXPECT_EQ(expect_ret, ObLogicalTableEncoder::calc_encoded_key_len_(aux_type, datum_array, len));   } while (0)


// ======== Order-preserving decoder tests ========

TEST_F(TestLogicalTableKeyCodec, decode_uint64_roundtrip_zero)
{
  unsigned char buf[8];
  int64_t enc_len = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(0UL, buf, sizeof(buf), enc_len));
  ASSERT_EQ(8, enc_len);

  uint64_t val = 0;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint64(buf, sizeof(buf), pos, val));
  EXPECT_EQ(0UL, val);
  EXPECT_EQ(8, pos);
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_roundtrip_one)
{
  unsigned char buf[8];
  int64_t enc_len = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(1UL, buf, sizeof(buf), enc_len));

  uint64_t val = 0;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint64(buf, sizeof(buf), pos, val));
  EXPECT_EQ(1UL, val);
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_roundtrip_max)
{
  unsigned char buf[8];
  int64_t enc_len = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(UINT64_MAX, buf, sizeof(buf), enc_len));

  uint64_t val = 0;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint64(buf, sizeof(buf), pos, val));
  EXPECT_EQ(UINT64_MAX, val);
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_sign_boundary)
{
  uint64_t test_vals[] = {0x7FFFFFFFFFFFFFFFUL, 0x8000000000000000UL};
  for (int i = 0; i < 2; ++i) {
    unsigned char buf[8];
    int64_t enc_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(test_vals[i], buf, sizeof(buf), enc_len));
    uint64_t val = 0;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint64(buf, sizeof(buf), pos, val));
    EXPECT_EQ(test_vals[i], val);
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_batch_roundtrip)
{
  srand(42);
  for (int i = 0; i < 100; ++i) {
    uint64_t orig = (static_cast<uint64_t>(rand()) << 32) | rand();
    unsigned char buf[8];
    int64_t enc_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(orig, buf, sizeof(buf), enc_len));
    uint64_t val = 0;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint64(buf, sizeof(buf), pos, val));
    EXPECT_EQ(orig, val);
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_order_preservation)
{
  uint64_t vals[] = {0, 1, UINT64_MAX};
  unsigned char encoded[3][8];
  for (int i = 0; i < 3; ++i) {
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(vals[i], encoded[i], sizeof(encoded[i]), len));
  }
  EXPECT_LT(memcmp(encoded[0], encoded[1], 8), 0);
  EXPECT_LT(memcmp(encoded[1], encoded[2], 8), 0);
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_batch_order)
{
  srand(123);
  const int N = 100;
  uint64_t vals[N];
  unsigned char encoded[N][8];
  for (int i = 0; i < N; ++i) {
    vals[i] = (static_cast<uint64_t>(rand()) << 32) | rand();
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint64(vals[i], encoded[i], sizeof(encoded[i]), len));
  }
  // Verify memcmp order matches value order
  for (int i = 0; i < N; ++i) {
    for (int j = i + 1; j < N; ++j) {
      int cmp = memcmp(encoded[i], encoded[j], 8);
      if (vals[i] < vals[j]) {
        EXPECT_LT(cmp, 0);
      } else if (vals[i] > vals[j]) {
        EXPECT_GT(cmp, 0);
      } else {
        EXPECT_EQ(cmp, 0);
      }
    }
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_uint64_null_ptr)
{
  uint64_t val = 0;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::decode_uint64(nullptr, 8, pos, val));
}

// ======== Order-preserving int64 encoder/decoder tests ========

TEST_F(TestLogicalTableKeyCodec, decode_int64_roundtrip_zero)
{
  unsigned char buf[8];
  int64_t enc_len = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_int64(0, buf, sizeof(buf), enc_len));
  ASSERT_EQ(8, enc_len);

  int64_t val = -1;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_int64(buf, sizeof(buf), pos, val));
  EXPECT_EQ(0, val);
  EXPECT_EQ(8, pos);
}

TEST_F(TestLogicalTableKeyCodec, decode_int64_roundtrip_negative)
{
  int64_t test_vals[] = {INT64_MIN, -1, -100, -0x7FFFFFFFFFFFFFFFLL};
  for (int i = 0; i < 4; ++i) {
    unsigned char buf[8];
    int64_t enc_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_int64(test_vals[i], buf, sizeof(buf), enc_len));
    int64_t val = 0;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_int64(buf, sizeof(buf), pos, val));
    EXPECT_EQ(test_vals[i], val);
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_int64_roundtrip_positive)
{
  int64_t test_vals[] = {1, 100, INT64_MAX};
  for (int i = 0; i < 3; ++i) {
    unsigned char buf[8];
    int64_t enc_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_int64(test_vals[i], buf, sizeof(buf), enc_len));
    int64_t val = 0;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_int64(buf, sizeof(buf), pos, val));
    EXPECT_EQ(test_vals[i], val);
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_int64_order_preservation)
{
  // INT64_MIN < -1 < 0 < 1 < INT64_MAX
  int64_t vals[] = {INT64_MIN, -1, 0, 1, INT64_MAX};
  unsigned char encoded[5][8];
  for (int i = 0; i < 5; ++i) {
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_int64(vals[i], encoded[i], sizeof(encoded[i]), len));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_LT(memcmp(encoded[i], encoded[i + 1], 8), 0)
      << "Expected encoded(" << vals[i] << ") < encoded(" << vals[i + 1] << ")";
  }
}

TEST_F(TestLogicalTableKeyCodec, decode_int64_null_ptr)
{
  int64_t val = 0;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::decode_int64(nullptr, 8, pos, val));
}

// ======== Full key encode + decode tests ========

TEST_F(TestLogicalTableKeyCodec, primary_key_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum datums[1] = {make_uint64_datum(100)};
  ObString key;
  ENCODE_KEY(prefix, datums, 1, key);

  // Decode prefix
  ObLogicalTableKeyPrefix decoded_prefix;
  int64_t rp = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, decoded_prefix));
  EXPECT_EQ(prefix, decoded_prefix);

  // Decode rowid
  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(100UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, primary_key_boundary_rowid)
{
  uint64_t test_vals[] = {0, UINT64_MAX};
  for (int i = 0; i < 2; ++i) {
    ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
    ObDatum datums[1] = {make_uint64_datum(test_vals[i])};
    ObString key;
    ENCODE_KEY(prefix, datums, 1, key);

    ObLogicalTableKeyPrefix dp;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, dp));
    uint64_t rowid = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
    EXPECT_EQ(test_vals[i], rowid);
  }
}

TEST_F(TestLogicalTableKeyCodec, search_index_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString path("user.name");
  ObString value("alice");
  ObDatum datums[4] = {
    make_int64_datum(3),
    make_varchar_datum(path),
    make_varchar_datum(value),
    make_uint64_datum(42),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 4, key);

  ObLogicalTableKeyPrefix dp;
  int64_t rp = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, dp));
  EXPECT_EQ(prefix, dp);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(42UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, search_index_with_null)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum datums[4] = {
    make_int64_datum(3),
    make_null_datum(),  // path = NULL
    make_null_datum(),  // value = NULL
    make_uint64_datum(42),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 4, key);

  ObLogicalTableKeyPrefix dp;
  int64_t rp = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, dp));
  EXPECT_EQ(prefix, dp);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(42UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, search_index_empty_path_value_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum datums[4] = {
    make_int64_datum(3),
    make_varchar_datum(static_cast<const char *>(nullptr), 0),  // path = ""
    make_varchar_datum(static_cast<const char *>(nullptr), 0),  // value = ""
    make_uint64_datum(42),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 4, key);

  ObLogicalTableKeyPrefix dp;
  int64_t rp = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, dp));
  EXPECT_EQ(prefix, dp);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(42UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, fts_inverted_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString token("hello");
  ObDatum datums[2] = {
    make_varchar_datum(token),
    make_uint64_datum(100),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);

  ObLogicalTableKeyPrefix dp;
  int64_t rp = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), rp, dp));
  EXPECT_EQ(prefix, dp);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(100UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, fts_inverted_empty_token)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString token("");
  ObDatum datums[2] = {
    make_varchar_datum(token.ptr(), 0),
    make_uint64_datum(100),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);
}

TEST_F(TestLogicalTableKeyCodec, fts_inverted_token_with_null_byte)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  char token_data[] = {'a', 'b', '\x00', 'c', 'd'};
  ObDatum datums[2] = {
    make_varchar_datum(token_data, 5),
    make_uint64_datum(100),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);
}

TEST_F(TestLogicalTableKeyCodec, fts_inverted_utf8_token)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  const char *token_str = "\xe4\xb8\xad\xe6\x96\x87\xe6\xb5\x8b\xe8\xaf\x95"; // "中文测试"
  ObString token(token_str);
  ObDatum datums[2] = {
    make_varchar_datum(token),
    make_uint64_datum(100),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);
}

TEST_F(TestLogicalTableKeyCodec, fts_forward_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 3, LT_AUX_FTS_FORWARD);
  ObString seq("seq_val");
  ObDatum datums[2] = {make_uint64_datum(100), make_varchar_datum(seq)};
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(100UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, ivf_cid_vector_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 4, LT_AUX_IVF_CID_VECTOR);
  ObDatum datums[1] = {make_cid_datum(10)};
  ObString key;
  ENCODE_KEY(prefix, datums, 1, key);

  uint64_t rowid = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, ObLogicalTableDecoder::decode_rowid(key, rowid));
}

TEST_F(TestLogicalTableKeyCodec, ivf_cid_rowid_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 5, LT_AUX_IVF_CID_ROWID);
  ObDatum datums[2] = {make_cid_datum(5), make_uint64_datum(100)};
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(100UL, rowid);
}

TEST_F(TestLogicalTableKeyCodec, ivf_rowid_cid_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 6, LT_AUX_IVF_ROWID_CID);
  ObDatum datums[2] = {make_uint64_datum(100), make_cid_datum(5)};
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);

  uint64_t rowid = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  EXPECT_EQ(100UL, rowid);
}

// ---- Error paths ----

TEST_F(TestLogicalTableKeyCodec, encode_datum_cnt_mismatch)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum datums[2] = {make_uint64_datum(1), make_uint64_datum(2)};
  char buf[128];
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, prefix, datums, 2);
}

TEST_F(TestLogicalTableKeyCodec, encode_buf_too_short)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum datums[1] = {make_uint64_datum(100)};
  char buf[10];  // way too short
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_SUCCESS, prefix, datums, 1);
}

TEST_F(TestLogicalTableKeyCodec, encode_null_datums)
{
  // nullptr datums with cnt=1 results in empty datum_array (0 datums),
  // which is valid prefix-only encoding.
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  char buf[128];
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_SUCCESS, prefix, nullptr, 1);
}

TEST_F(TestLogicalTableKeyCodec, encode_invalid_aux_type)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_MAX_TYPE);
  ObDatum datums[1] = {make_uint64_datum(1)};
  char buf[128];
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, prefix, datums, 1);
}

// ---- Ordering tests ----

TEST_F(TestLogicalTableKeyCodec, order_primary_rowid)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum d1[1] = {make_uint64_datum(1)};
  ObDatum d2[1] = {make_uint64_datum(2)};
  ObString key1, key2;
  ENCODE_KEY(prefix, d1, 1, key1);
  ENCODE_KEY(prefix, d2, 1, key2);
  EXPECT_LT(memcmp(key1.ptr(), key2.ptr(), std::min(key1.length(), key2.length())), 0);
}

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_token)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString t1("aaa"), t2("bbb");
  ObDatum d1[2] = {make_varchar_datum(t1), make_uint64_datum(1)};
  ObDatum d2[2] = {make_varchar_datum(t2), make_uint64_datum(1)};
  ObString key1, key2;
  ENCODE_KEY(prefix, d1, 2, key1);
  ENCODE_KEY(prefix, d2, 2, key2);
  EXPECT_LT(memcmp(key1.ptr(), key2.ptr(), std::min(key1.length(), key2.length())), 0);
}

TEST_F(TestLogicalTableKeyCodec, order_same_ns_lt_different_seq)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum d1[1] = {make_uint64_datum(1)};
  ObString path("a"), value("b");
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(path), make_varchar_datum(value), make_uint64_datum(1)};
  ObString key1, key2;
  ENCODE_KEY(p1, d1, 1, key1);
  ENCODE_KEY(p2, d2, 4, key2);
  EXPECT_LT(memcmp(key1.ptr(), key2.ptr(), OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_composite)
{
  // (col_idx=1, path="a") < (col_idx=1, path="b") < (col_idx=2, path="a")
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString pa("a"), pb("b"), v("x");
  ObDatum d1[4] = {make_int64_datum(1), make_varchar_datum(pa), make_varchar_datum(v), make_uint64_datum(1)};
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(pb), make_varchar_datum(v), make_uint64_datum(1)};
  ObDatum d3[4] = {make_int64_datum(2), make_varchar_datum(pa), make_varchar_datum(v), make_uint64_datum(1)};
  ObString key1, key2, key3;
  ENCODE_KEY(prefix, d1, 4, key1);
  ENCODE_KEY(prefix, d2, 4, key2);
  ENCODE_KEY(prefix, d3, 4, key3);
  EXPECT_LT(memcmp(key1.ptr(), key2.ptr(), std::min(key1.length(), key2.length())), 0);
  EXPECT_LT(memcmp(key2.ptr(), key3.ptr(), std::min(key2.length(), key3.length())), 0);
}

TEST_F(TestLogicalTableKeyCodec, search_index_preencoded_bytes_copied_directly)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  const char path_buf[] = {'\x00', '\x1c', 'a', '\x00', '\x03'};
  const char value_buf[] = {'\x80', '\x00', '\x7f', '\xff'};
  ObDatum datums[4] = {
    make_int64_datum(7),
    make_varchar_datum(path_buf, sizeof(path_buf)),
    make_varchar_datum(value_buf, sizeof(value_buf)),
    make_uint64_datum(9),
  };
  ObString key;
  ENCODE_KEY(prefix, datums, 4, key);

  const int64_t expected_len = OB_LOGICAL_TABLE_KEY_PREFIX_LEN
    + sizeof(uint64_t)
    + 1 + sizeof(path_buf)
    + 1 + sizeof(value_buf)
    + sizeof(uint64_t);
  EXPECT_EQ(expected_len, key.length());

  int64_t offset = OB_LOGICAL_TABLE_KEY_PREFIX_LEN + sizeof(uint64_t);
  EXPECT_EQ(0x01, static_cast<unsigned char>(key.ptr()[offset]));
  ++offset;
  EXPECT_EQ(0, MEMCMP(key.ptr() + offset, path_buf, sizeof(path_buf)));
  offset += sizeof(path_buf);
  EXPECT_EQ(0x01, static_cast<unsigned char>(key.ptr()[offset]));
  ++offset;
  EXPECT_EQ(0, MEMCMP(key.ptr() + offset, value_buf, sizeof(value_buf)));
}

// ---- Cross-namespace isolation ----

TEST_F(TestLogicalTableKeyCodec, cross_namespace_order)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(2, 1, 0, LT_AUX_PRIMARY);
  ObDatum d[1] = {make_uint64_datum(UINT64_MAX)};
  ObString key1, key2;
  ENCODE_KEY(p1, d, 1, key1);
  ENCODE_KEY(p2, d, 1, key2);
  EXPECT_LT(memcmp(key1.ptr(), key2.ptr(), std::min(key1.length(), key2.length())), 0);
}

// ======== Comprehensive ordering tests ========

// Helper macro: encode two keys and assert memcmp(k1, k2) < 0
#define ASSERT_KEY_ORDER_LT(p1, d1, c1, p2, d2, c2) \
  do { \
    ObString _k1, _k2; \
    ENCODE_KEY(p1, d1, c1, _k1); \
    ENCODE_KEY(p2, d2, c2, _k2); \
    EXPECT_LT(memcmp(_k1.ptr(), _k2.ptr(), std::min(_k1.length(), _k2.length())), 0) \
      << "Expected first key < second key"; \
  } while (0)

// Helper macro: encode two keys and assert memcmp(k1, k2) == 0 on full length
#define ASSERT_KEY_ORDER_EQ(p1, d1, c1, p2, d2, c2) \
  do { \
    ObString _k1, _k2; \
    ENCODE_KEY(p1, d1, c1, _k1); \
    ENCODE_KEY(p2, d2, c2, _k2); \
    ASSERT_EQ(_k1.length(), _k2.length()); \
    EXPECT_EQ(0, memcmp(_k1.ptr(), _k2.ptr(), _k1.length())) \
      << "Expected identical keys"; \
  } while (0)

// ---- PRIMARY: rowid order at uint64 boundaries ----

TEST_F(TestLogicalTableKeyCodec, order_primary_rowid_boundaries)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  // 0 < 1 < 0x7FFF..FF < 0x8000..00 < UINT64_MAX-1 < UINT64_MAX
  uint64_t vals[] = {0, 1, 0x7FFFFFFFFFFFFFFFUL, 0x8000000000000000UL,
                     UINT64_MAX - 1, UINT64_MAX};
  for (int i = 0; i < 5; ++i) {
    ObDatum d1[1] = {make_uint64_datum(vals[i])};
    ObDatum d2[1] = {make_uint64_datum(vals[i + 1])};
    ASSERT_KEY_ORDER_LT(prefix, d1, 1, prefix, d2, 1);
  }
}

TEST_F(TestLogicalTableKeyCodec, order_primary_rowid_equal)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum d1[1] = {make_uint64_datum(12345)};
  ObDatum d2[1] = {make_uint64_datum(12345)};
  ASSERT_KEY_ORDER_EQ(prefix, d1, 1, prefix, d2, 1);
}

// ---- FTS_INVERTED: token varchar ordering ----

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_prefix_strings)
{
  // "a" < "aa" < "ab" < "b"
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString tokens[] = {ObString("a"), ObString("aa"), ObString("ab"), ObString("b")};
  for (int i = 0; i < 3; ++i) {
    ObDatum d1[2] = {make_varchar_datum(tokens[i]), make_uint64_datum(1)};
    ObDatum d2[2] = {make_varchar_datum(tokens[i + 1]), make_uint64_datum(1)};
    ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
  }
}

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_same_token_different_rowid)
{
  // Same token, rowid 1 < rowid 2
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString token("hello");
  ObDatum d1[2] = {make_varchar_datum(token), make_uint64_datum(1)};
  ObDatum d2[2] = {make_varchar_datum(token), make_uint64_datum(2)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_token_with_zero_byte)
{
  // "a\x00b" vs "a\x00c" — tests that 0x00-escaping preserves order
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  char t1[] = {'a', '\x00', 'b'};
  char t2[] = {'a', '\x00', 'c'};
  ObDatum d1[2] = {make_varchar_datum(t1, 3), make_uint64_datum(1)};
  ObDatum d2[2] = {make_varchar_datum(t2, 3), make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_zero_byte_vs_no_zero_byte)
{
  // "a" < "a\x00b" — the \x00 is escaped so "a" terminates first
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  char t2[] = {'a', '\x00', 'b'};
  ObString t1("a");
  ObDatum d1[2] = {make_varchar_datum(t1), make_uint64_datum(1)};
  ObDatum d2[2] = {make_varchar_datum(t2, 3), make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_fts_inverted_empty_vs_nonempty_token)
{
  // "" < "a" — empty token should come before any non-empty token
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObDatum d1[2] = {make_varchar_datum(static_cast<const char *>(nullptr), 0), make_uint64_datum(1)};
  ObDatum d2[2] = {make_varchar_datum(ObString("a")), make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

// ---- FTS_FORWARD: (rowid, token) composite ordering ----

TEST_F(TestLogicalTableKeyCodec, order_fts_forward_same_rowid_different_token)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 3, LT_AUX_FTS_FORWARD);
  ObString t1("aaa");
  ObString t2("bbb");
  ObDatum d1[2] = {make_uint64_datum(100), make_varchar_datum(t1)};
  ObDatum d2[2] = {make_uint64_datum(100), make_varchar_datum(t2)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_fts_forward_different_rowid)
{
  // rowid=1 < rowid=2, regardless of token
  ObLogicalTableKeyPrefix prefix(1, 1, 3, LT_AUX_FTS_FORWARD);
  ObString t1("zzz");
  ObString t2("aaa");
  ObDatum d1[2] = {make_uint64_datum(1), make_varchar_datum(t1)};
  ObDatum d2[2] = {make_uint64_datum(2), make_varchar_datum(t2)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

// ---- IVF_CID_ROWID: (cid, rowid) composite ordering ----

TEST_F(TestLogicalTableKeyCodec, order_ivf_cid_rowid_same_cid_different_rowid)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 5, LT_AUX_IVF_CID_ROWID);
  ObDatum d1[2] = {make_cid_datum(5), make_uint64_datum(100)};
  ObDatum d2[2] = {make_cid_datum(5), make_uint64_datum(200)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_ivf_cid_rowid_different_cid)
{
  // cid=1 < cid=2, regardless of rowid
  ObLogicalTableKeyPrefix prefix(1, 1, 5, LT_AUX_IVF_CID_ROWID);
  ObDatum d1[2] = {make_cid_datum(1), make_uint64_datum(UINT64_MAX)};
  ObDatum d2[2] = {make_cid_datum(2), make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

// ---- IVF_ROWID_CID: (rowid, cid) composite ordering ----

TEST_F(TestLogicalTableKeyCodec, order_ivf_rowid_cid_same_rowid_different_cid)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 6, LT_AUX_IVF_ROWID_CID);
  ObDatum d1[2] = {make_uint64_datum(100), make_cid_datum(1)};
  ObDatum d2[2] = {make_uint64_datum(100), make_cid_datum(2)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, order_ivf_rowid_cid_different_rowid)
{
  // rowid=1 < rowid=2, regardless of cid
  ObLogicalTableKeyPrefix prefix(1, 1, 6, LT_AUX_IVF_ROWID_CID);
  ObDatum d1[2] = {make_uint64_datum(1), make_cid_datum(UINT64_MAX)};
  ObDatum d2[2] = {make_uint64_datum(2), make_cid_datum(0)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 2, prefix, d2, 2);
}

TEST_F(TestLogicalTableKeyCodec, ivf_cid_must_be_exactly_16_bytes)
{
  ObLogicalTableKeyPrefix cid_rowid(1, 1, 5, LT_AUX_IVF_CID_ROWID);
  ObDatum short_cid[2] = {make_varchar_datum("short", 5), make_uint64_datum(1)};
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, cid_rowid, short_cid, 2);

  char deferred_payload[32] = {0};
  ObDatum long_cid[2] = {
    make_varchar_datum(deferred_payload, sizeof(deferred_payload)),
    make_uint64_datum(1)
  };
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, cid_rowid, long_cid, 2);

  ObLogicalTableKeyPrefix rowid_cid(1, 1, 6, LT_AUX_IVF_ROWID_CID);
  ObDatum reverse_long_cid[2] = {
    make_uint64_datum(1),
    make_varchar_datum(deferred_payload, sizeof(deferred_payload))
  };
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, rowid_cid, reverse_long_cid, 2);
}

// ---- SEARCH_INDEX: multi-column composite ordering ----

TEST_F(TestLogicalTableKeyCodec, order_search_index_col_idx_dominates)
{
  // col_idx=1 < col_idx=2, regardless of path/value/rowid
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString pa("zzz"), va("zzz"), pb("aaa"), vb("aaa");
  ObDatum d1[4] = {make_int64_datum(1), make_varchar_datum(pa), make_varchar_datum(va), make_uint64_datum(UINT64_MAX)};
  ObDatum d2[4] = {make_int64_datum(2), make_varchar_datum(pb), make_varchar_datum(vb), make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_path_dominates_over_value)
{
  // Same col_idx, path="a" < path="b", regardless of value
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString pa("a"), pb("b"), va("zzz"), vb("aaa");
  ObDatum d1[4] = {make_int64_datum(1), make_varchar_datum(pa), make_varchar_datum(va), make_uint64_datum(1)};
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(pb), make_varchar_datum(vb), make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_value_dominates_over_rowid)
{
  // Same col_idx & path, value="a" < value="b", regardless of rowid
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString p("path"), va("a"), vb("b");
  ObDatum d1[4] = {make_int64_datum(1), make_varchar_datum(p), make_varchar_datum(va), make_uint64_datum(UINT64_MAX)};
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(p), make_varchar_datum(vb), make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_rowid_last_tiebreaker)
{
  // Same col_idx, path, value => rowid breaks tie
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString p("path"), v("val");
  ObDatum d1[4] = {make_int64_datum(1), make_varchar_datum(p), make_varchar_datum(v), make_uint64_datum(10)};
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(p), make_varchar_datum(v), make_uint64_datum(20)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_null_before_nonnull)
{
  // NULL path < non-NULL path (nullable flag: 0x00 < 0x01)
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString p("a"), v("x");
  ObDatum d1[4] = {make_int64_datum(1), make_null_datum(), make_null_datum(), make_uint64_datum(1)};
  ObDatum d2[4] = {make_int64_datum(1), make_varchar_datum(p), make_varchar_datum(v), make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_null_null_equal)
{
  // Two keys with same col_idx + both NULL path/value + same rowid => equal
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum d1[4] = {make_int64_datum(1), make_null_datum(), make_null_datum(), make_uint64_datum(42)};
  ObDatum d2[4] = {make_int64_datum(1), make_null_datum(), make_null_datum(), make_uint64_datum(42)};
  ASSERT_KEY_ORDER_EQ(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_empty_path_before_nonempty)
{
  // empty path "" < "a"
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString va("x");
  ObDatum d1[4] = {make_int64_datum(1),
                   make_varchar_datum(static_cast<const char *>(nullptr), 0),
                   make_varchar_datum(va),
                   make_uint64_datum(1)};
  ObString pa("a");
  ObDatum d2[4] = {make_int64_datum(1),
                   make_varchar_datum(pa),
                   make_varchar_datum(va),
                   make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_search_index_empty_value_before_nonempty)
{
  // Same col_idx and path, empty value "" < "x"
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString pa("path");
  ObDatum d1[4] = {make_int64_datum(1),
                   make_varchar_datum(pa),
                   make_varchar_datum(static_cast<const char *>(nullptr), 0),
                   make_uint64_datum(1)};
  ObString vb("x");
  ObDatum d2[4] = {make_int64_datum(1),
                   make_varchar_datum(pa),
                   make_varchar_datum(vb),
                   make_uint64_datum(1)};
  ASSERT_KEY_ORDER_LT(prefix, d1, 4, prefix, d2, 4);
}

// ======== Cross-index ordering tests ========

TEST_F(TestLogicalTableKeyCodec, order_cross_index_primary_before_search)
{
  // Same namespace+table, primary(index_seq=0) < search_index(index_seq=1)
  // Even when primary has max rowid
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum d1[1] = {make_uint64_datum(UINT64_MAX)};
  ObString path("a"), value("b");
  ObDatum d2[4] = {make_int64_datum(0), make_varchar_datum(path), make_varchar_datum(value), make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(p1, d1, 1, p2, d2, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_cross_index_all_types_by_index_seq)
{
  // Given same (ns=1, lt=1), keys with increasing index_seq should be ordered
  // index_seq: 0(primary) < 1(search) < 2(fts_inv) < 3(fts_fwd) < 4(ivf_cv) < 5(ivf_cr) < 6(ivf_rc)
  struct IndexCase {
    uint64_t index_seq;
    ObLogicalTableAuxType aux_type;
  };
  IndexCase cases[] = {
    {0, LT_AUX_PRIMARY},
    {1, LT_AUX_SEARCH_INDEX},
    {2, LT_AUX_FTS_INVERTED},
    {3, LT_AUX_FTS_FORWARD},
    {4, LT_AUX_IVF_CID_VECTOR},
    {5, LT_AUX_IVF_CID_ROWID},
    {6, LT_AUX_IVF_ROWID_CID},
  };
  // Encode one key per index type (minimal valid datums)
  ObString keys[7];
  for (int i = 0; i < 7; ++i) {
    ObLogicalTableKeyPrefix prefix(1, 1, cases[i].index_seq, cases[i].aux_type);
    const ObLogicalTableKeyColDesc *descs = nullptr;
    int64_t cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_aux_type_col_descs(cases[i].aux_type, descs, cnt));
    // Build minimal datums: uint64=1 for uint64, int64=1 for int, varchar="a" for varchar columns
    ObString va("a");
    ObDatum datums[4];
    for (int64_t c = 0; c < cnt; ++c) {
      if (descs[c].type_ == common::ObUInt64Type) {
        datums[c] = make_uint64_datum(1);
      } else if (descs[c].type_ == common::ObIntType) {
        datums[c] = make_int64_datum(1);
      } else if (descs[c].fixed_length_
          == OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH) {
        datums[c] = make_cid_datum(1);
      } else {
        datums[c] = make_varchar_datum(va);
      }
    }
    ENCODE_KEY(prefix, datums, cnt, keys[i]);
  }
  // Verify strict ordering by prefix
  for (int i = 0; i < 6; ++i) {
    EXPECT_LT(memcmp(keys[i].ptr(), keys[i + 1].ptr(), OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0)
      << "index_seq " << cases[i].index_seq << " should be < " << cases[i + 1].index_seq;
  }
}

TEST_F(TestLogicalTableKeyCodec, order_cross_table_isolation)
{
  // logical_table_id=1 < logical_table_id=2, even with max rowid on table 1
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 2, 0, LT_AUX_PRIMARY);
  ObDatum d1[1] = {make_uint64_datum(UINT64_MAX)};
  ObDatum d2[1] = {make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(p1, d1, 1, p2, d2, 1);
}

TEST_F(TestLogicalTableKeyCodec, order_cross_namespace_with_max_table_and_index)
{
  // ns=1 with max everything < ns=2 with min everything
  ObLogicalTableKeyPrefix p1(1, UINT64_MAX, UINT64_MAX, LT_AUX_IVF_ROWID_CID);
  ObLogicalTableKeyPrefix p2(2, 1, 0, LT_AUX_PRIMARY);
  ObDatum d1[2] = {make_uint64_datum(UINT64_MAX), make_cid_datum(UINT64_MAX)};
  ObDatum d2[1] = {make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(p1, d1, 2, p2, d2, 1);
}

TEST_F(TestLogicalTableKeyCodec, order_cross_index_no_payload_leak)
{
  // Ensure that a large payload in index_seq=0 does NOT bleed into index_seq=1's key range
  // primary(ns=1,lt=1,seq=0,rowid=MAX) < search_index(ns=1,lt=1,seq=1,col_idx=0,...)
  ObLogicalTableKeyPrefix p_pri(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p_si(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObDatum d_pri[1] = {make_uint64_datum(UINT64_MAX)};
  ObDatum d_si[4] = {make_int64_datum(0),
                     make_varchar_datum(static_cast<const char *>(nullptr), 0),
                     make_varchar_datum(static_cast<const char *>(nullptr), 0),
                     make_uint64_datum(0)};
  ASSERT_KEY_ORDER_LT(p_pri, d_pri, 1, p_si, d_si, 4);
}

TEST_F(TestLogicalTableKeyCodec, order_same_table_different_aux_type_same_seq)
{
  // Same index_seq but different aux_type: PRIMARY(0) < SEARCH_INDEX(1)
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 0, LT_AUX_SEARCH_INDEX);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  // With same index_seq, the aux_type byte determines ordering
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

// ---- calc_encoded_key_len tests ----

TEST_F(TestLogicalTableKeyCodec, calc_key_len_primary)
{
  ObDatum datums[1] = {make_uint64_datum(100)};
  int64_t len = 0;
  CALC_KEY_LEN(len, LT_AUX_PRIMARY, datums, 1);
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN + 8, len);
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_fts_inverted)
{
  ObString token("hello");
  ObDatum datums[2] = {make_varchar_datum(token), make_uint64_datum(100)};
  int64_t len = 0;
  CALC_KEY_LEN(len, LT_AUX_FTS_INVERTED, datums, 2);
  // Encode and verify calc_len >= actual
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObString key;
  ENCODE_KEY(prefix, datums, 2, key);
  EXPECT_GE(len, key.length());
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_search_index)
{
  ObString path("a.b"), value("test");
  ObDatum datums[4] = {
    make_int64_datum(1),
    make_varchar_datum(path),
    make_varchar_datum(value),
    make_uint64_datum(42),
  };
  int64_t len = 0;
  CALC_KEY_LEN(len, LT_AUX_SEARCH_INDEX, datums, 4);
  // Encode and verify calc_len >= actual
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString key;
  ENCODE_KEY(prefix, datums, 4, key);
  EXPECT_EQ(len, key.length());
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_all_aux_types)
{
  // Verify calc_encoded_key_len >= actual encode_key output for all types
  struct TestCase {
    ObLogicalTableAuxType type;
    uint64_t index_seq;
  };
  TestCase cases[] = {
    {LT_AUX_PRIMARY, 0},
    {LT_AUX_FTS_FORWARD, 3},
    {LT_AUX_IVF_CID_VECTOR, 4},
    {LT_AUX_IVF_CID_ROWID, 5},
    {LT_AUX_IVF_ROWID_CID, 6},
  };
  for (size_t c = 0; c < ARRAYSIZEOF(cases); ++c) {
    const ObLogicalTableKeyColDesc *descs = nullptr;
    int64_t cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_aux_type_col_descs(cases[c].type, descs, cnt));
    // Build datums matching column types
    ObString va("test");
    ObDatum datums[4];
    for (int64_t i = 0; i < cnt; ++i) {
      if (descs[i].fixed_length_ == OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH) {
        datums[i] = make_cid_datum(42);
      } else if (descs[i].type_ == common::ObVarcharType) {
        datums[i] = make_varchar_datum(va);
      } else {
        datums[i] = make_uint64_datum(42);
      }
    }
    int64_t calc_len = 0;
    CALC_KEY_LEN(calc_len, cases[c].type, datums, cnt);
    ObLogicalTableKeyPrefix prefix(1, 1, cases[c].index_seq, cases[c].type);
    ObString key;
    ENCODE_KEY(prefix, datums, cnt, key);
    EXPECT_GE(calc_len, key.length());
  }
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_invalid_type)
{
  ObDatum datums[1] = {make_uint64_datum(1)};
  int64_t len = 0;
  EXPECT_CALC_KEY_LEN_RET(OB_INVALID_ARGUMENT, LT_AUX_MAX_TYPE, datums, 1, len);
}

// ---- CodecUtils direct encode/decode uint8 tests ----

TEST_F(TestLogicalTableKeyCodec, encode_decode_uint8_roundtrip)
{
  uint8_t test_vals[] = {0, 1, 127, 128, 255};
  for (int i = 0; i < 5; ++i) {
    unsigned char buf[4];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_uint8(test_vals[i], buf, sizeof(buf), pos));
    ASSERT_EQ(1, pos);
    uint8_t val = 0;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::decode_uint8(buf, pos, rp, val));
    EXPECT_EQ(test_vals[i], val);
    EXPECT_EQ(1, rp);
  }
}

TEST_F(TestLogicalTableKeyCodec, encode_uint8_buf_too_short)
{
  unsigned char buf[1];
  int64_t pos = 1;  // no room left
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::encode_uint8(42, buf, sizeof(buf), pos));
}

TEST_F(TestLogicalTableKeyCodec, decode_uint8_buf_too_short)
{
  unsigned char buf[1] = {0};
  int64_t pos = 1;
  uint8_t val = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::decode_uint8(buf, sizeof(buf), pos, val));
}

TEST_F(TestLogicalTableKeyCodec, encode_uint64_null_buf)
{
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::encode_uint64(42, nullptr, 8, pos));
}

TEST_F(TestLogicalTableKeyCodec, encode_uint64_buf_too_short)
{
  unsigned char buf[4];
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::encode_uint64(42, buf, sizeof(buf), pos));
}

// ---- decode_rowid error paths ----

TEST_F(TestLogicalTableKeyCodec, decode_rowid_truncated_buffer)
{
  // Encode a valid primary key, then truncate the buffer before rowid
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum datums[1] = {make_uint64_datum(100)};
  ObString key;
  ENCODE_KEY(prefix, datums, 1, key);
  // Only pass prefix length (missing rowid bytes)
  uint64_t rowid = 0;
  EXPECT_NE(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(ObString(static_cast<int32_t>(OB_LOGICAL_TABLE_KEY_PREFIX_LEN), key.ptr()), rowid));
}

TEST_F(TestLogicalTableKeyCodec, decode_prefix_null_buf)
{
  ObLogicalTableKeyPrefix dp;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableDecoder::decode_key_prefix(nullptr, 64, pos, dp));
}

// ---- encode_varchar_bin edge cases ----

TEST_F(TestLogicalTableKeyCodec, encode_varchar_null_data_zero_len)
{
  // data=NULL with data_len=0 should succeed (empty varchar)
  unsigned char buf[64];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, ObLogicalTableCodecUtils::encode_varchar_bin(nullptr, 0, buf, sizeof(buf), pos));
  // Should write only the 0x00 0x00 terminator
  EXPECT_EQ(2, pos);
}

TEST_F(TestLogicalTableKeyCodec, encode_varchar_null_data_nonzero_len)
{
  // data=NULL with data_len>0 should fail
  unsigned char buf[64];
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableCodecUtils::encode_varchar_bin(nullptr, 5, buf, sizeof(buf), pos));
}

// ---- calc_encoded_key_len error paths ----

TEST_F(TestLogicalTableKeyCodec, calc_key_len_null_datums)
{
  // nullptr datums with cnt=1 results in empty datum_array (0 datums),
  // which is valid prefix-only length calculation.
  int64_t len = 0;
  EXPECT_CALC_KEY_LEN_RET(OB_SUCCESS, LT_AUX_PRIMARY, nullptr, 1, len);
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, len);
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_datum_cnt_mismatch)
{
  ObDatum datums[2] = {make_uint64_datum(1), make_uint64_datum(2)};
  int64_t len = 0;
  EXPECT_CALC_KEY_LEN_RET(OB_INVALID_ARGUMENT, LT_AUX_PRIMARY, datums, 2, len);
}

// ---- non-nullable column receiving NULL datum ----

TEST_F(TestLogicalTableKeyCodec, encode_nonnullable_column_with_null_datum)
{
  // PRIMARY rowid is non-nullable, passing NULL should fail
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObDatum datums[1] = {make_null_datum()};
  char buf[128];
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, prefix, datums, 1);
}

TEST_F(TestLogicalTableKeyCodec, encode_nonnullable_varchar_with_null_datum)
{
  // FTS_INVERTED token is non-nullable varchar, passing NULL should fail
  ObLogicalTableKeyPrefix prefix(1, 1, 2, LT_AUX_FTS_INVERTED);
  ObDatum datums[2] = {make_null_datum(), make_uint64_datum(1)};
  char buf[128];
  int64_t pos = 0;
  EXPECT_ENCODE_KEY_RET(OB_INVALID_ARGUMENT, prefix, datums, 2);
}

TEST_F(TestLogicalTableKeyCodec, calc_key_len_nonnullable_with_null_datum)
{
  ObDatum datums[1] = {make_null_datum()};
  int64_t len = 0;
  EXPECT_CALC_KEY_LEN_RET(OB_INVALID_ARGUMENT, LT_AUX_PRIMARY, datums, 1, len);
}

// ---- SIMD/scalar contains_byte coverage via encode_varchar_bin ----
// contains_byte_simd processes 32 bytes per iteration. We test strings
// with 0x00 at various positions relative to the 32-byte boundary to
// ensure both the SIMD bulk path and the scalar tail path detect 0x00.
// If contains_byte misses a 0x00, the fast (no-escape) path runs and
// the encoded key will be shorter than expected (missing escape bytes).

TEST_F(TestLogicalTableKeyCodec, varchar_zero_byte_at_various_positions)
{
  // Lengths around SIMD boundary: < 32, exactly 32, 32+tail, 2*32, 2*32+tail
  int64_t test_lens[] = {1, 16, 31, 32, 33, 48, 63, 64, 65, 100};
  for (int t = 0; t < 10; ++t) {
    const int64_t data_len = test_lens[t];
    // Place 0x00 at each possible position in the string
    for (int64_t zero_pos = 0; zero_pos < data_len; ++zero_pos) {
      // Build string: all 'A' except one 0x00
      char *data = static_cast<char *>(arena_.alloc(data_len));
      ASSERT_NE(nullptr, data);
      memset(data, 'A', data_len);
      data[zero_pos] = '\x00';

      unsigned char buf[512];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS,
        ObLogicalTableCodecUtils::encode_varchar_bin(data, data_len, buf, sizeof(buf), pos));

      // With 0x00 escaping: 0x00 -> 0x00 0x01, plus 0x00 0x00 terminator
      // So encoded length = data_len + 1 (one extra for escape) + 2 (terminator)
      EXPECT_EQ(data_len + 1 + 2, pos)
        << "data_len=" << data_len << " zero_pos=" << zero_pos;

      // Verify the 0x00 was escaped to 0x00 0x01 (not left as raw 0x00)
      // Find the escaped position: bytes before zero_pos are 'A', then 0x00 0x01
      EXPECT_EQ(0x00, buf[zero_pos]);
      EXPECT_EQ(0x01, buf[zero_pos + 1]);

      // Verify terminator 0x00 0x00 at the end
      EXPECT_EQ(0x00, buf[pos - 2]);
      EXPECT_EQ(0x00, buf[pos - 1]);
    }
  }
}

TEST_F(TestLogicalTableKeyCodec, varchar_no_zero_byte_various_lengths)
{
  // No 0x00 → fast path (memcpy + terminator). encoded_len = data_len + 2.
  int64_t test_lens[] = {0, 1, 16, 31, 32, 33, 63, 64, 65, 100};
  for (int t = 0; t < 10; ++t) {
    const int64_t data_len = test_lens[t];
    char *data = nullptr;
    if (data_len > 0) {
      data = static_cast<char *>(arena_.alloc(data_len));
      ASSERT_NE(nullptr, data);
      memset(data, 'B', data_len);
    }

    unsigned char buf[512];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS,
      ObLogicalTableCodecUtils::encode_varchar_bin(data, data_len, buf, sizeof(buf), pos));
    EXPECT_EQ(data_len + 2, pos)
      << "data_len=" << data_len;

    // Verify content copied correctly
    for (int64_t i = 0; i < data_len; ++i) {
      EXPECT_EQ('B', buf[i]);
    }
    // Verify terminator
    EXPECT_EQ(0x00, buf[pos - 2]);
    EXPECT_EQ(0x00, buf[pos - 1]);
  }
}

TEST_F(TestLogicalTableKeyCodec, varchar_multiple_zero_bytes_simd_boundary)
{
  // String with multiple 0x00 bytes spanning SIMD chunks
  const int64_t data_len = 72;  // > 2 * 32
  char *data = static_cast<char *>(arena_.alloc(data_len));
  ASSERT_NE(nullptr, data);
  memset(data, 'C', data_len);
  // Place 0x00 at: position 0 (start), 31 (end of first chunk), 32 (start of second),
  // 63 (end of second chunk), 64 (scalar tail), 71 (last byte)
  int zero_positions[] = {0, 31, 32, 63, 64, 71};
  int num_zeros = 6;
  for (int i = 0; i < num_zeros; ++i) {
    data[zero_positions[i]] = '\x00';
  }

  unsigned char buf[512];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableCodecUtils::encode_varchar_bin(data, data_len, buf, sizeof(buf), pos));

  // Each 0x00 adds 1 byte (escape), plus 2 bytes terminator
  EXPECT_EQ(data_len + num_zeros + 2, pos);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

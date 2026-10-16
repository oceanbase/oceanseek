/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/logical_table/ob_logical_table_define.h"
#undef private

using namespace std;
namespace oceanbase {
namespace share {

class TestLogicalTableKeyPrefix : public ::testing::Test {
public:
  TestLogicalTableKeyPrefix() {}
  ~TestLogicalTableKeyPrefix() {}
  virtual void SetUp() {}
  virtual void TearDown() {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestLogicalTableKeyPrefix);
};

// ---- Round-trip encode/decode ----

TEST_F(TestLogicalTableKeyPrefix, encode_decode_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 100, 0, LT_AUX_PRIMARY);
  char buf[64] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, pos);

  ObLogicalTableKeyPrefix decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, read_pos));
  ASSERT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, read_pos);
  EXPECT_EQ(prefix, decoded);
  EXPECT_EQ(1UL, decoded.namespace_id_);
  EXPECT_EQ(100UL, decoded.logical_table_id_);
  EXPECT_EQ(0UL, decoded.index_seq_);
  EXPECT_EQ(LT_AUX_PRIMARY, decoded.aux_type_);
}

TEST_F(TestLogicalTableKeyPrefix, roundtrip_all_aux_types)
{
  for (uint8_t t = LT_AUX_PRIMARY; t < LT_AUX_MAX_TYPE; ++t) {
    ObLogicalTableAuxType aux = static_cast<ObLogicalTableAuxType>(t);
    ObLogicalTableKeyPrefix prefix(42, 99, t, aux);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));

    ObLogicalTableKeyPrefix decoded;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, read_pos));
    EXPECT_EQ(prefix, decoded);
  }
}

TEST_F(TestLogicalTableKeyPrefix, boundary_namespace_id)
{
  // minimum valid
  {
    ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(1UL, decoded.namespace_id_);
  }
  // UINT64_MAX
  {
    ObLogicalTableKeyPrefix prefix(UINT64_MAX, 1, 0, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(UINT64_MAX, decoded.namespace_id_);
  }
}

TEST_F(TestLogicalTableKeyPrefix, boundary_logical_table_id)
{
  {
    ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(1UL, decoded.logical_table_id_);
  }
  {
    ObLogicalTableKeyPrefix prefix(1, UINT64_MAX, 0, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(UINT64_MAX, decoded.logical_table_id_);
  }
}

TEST_F(TestLogicalTableKeyPrefix, boundary_index_seq)
{
  {
    ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(0UL, decoded.index_seq_);
  }
  {
    ObLogicalTableKeyPrefix prefix(1, 1, UINT64_MAX, LT_AUX_PRIMARY);
    char buf[64] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
    ObLogicalTableKeyPrefix decoded;
    int64_t rp = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(buf, pos, rp));
    EXPECT_EQ(UINT64_MAX, decoded.index_seq_);
  }
}

// ---- Lexicographic order verification via memcmp ----

TEST_F(TestLogicalTableKeyPrefix, order_by_namespace_id)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(2, 1, 0, LT_AUX_PRIMARY);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

TEST_F(TestLogicalTableKeyPrefix, order_by_logical_table_id)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 2, 0, LT_AUX_PRIMARY);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

TEST_F(TestLogicalTableKeyPrefix, order_by_index_seq)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 1, LT_AUX_PRIMARY);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

TEST_F(TestLogicalTableKeyPrefix, order_by_aux_type)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 0, LT_AUX_SEARCH_INDEX);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

TEST_F(TestLogicalTableKeyPrefix, order_composite)
{
  // (ns=1, lt=UINT64_MAX) < (ns=2, lt=1)
  ObLogicalTableKeyPrefix p1(1, UINT64_MAX, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(2, 1, 0, LT_AUX_PRIMARY);
  char buf1[64], buf2[64];
  int64_t pos1 = 0, pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, p1.encode(buf1, sizeof(buf1), pos1));
  ASSERT_EQ(OB_SUCCESS, p2.encode(buf2, sizeof(buf2), pos2));
  EXPECT_LT(memcmp(buf1, buf2, OB_LOGICAL_TABLE_KEY_PREFIX_LEN), 0);
}

// ---- operator== ----

TEST_F(TestLogicalTableKeyPrefix, operator_equal)
{
  ObLogicalTableKeyPrefix p1(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p2(1, 1, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix p3(1, 2, 0, LT_AUX_PRIMARY);
  EXPECT_TRUE(p1 == p2);
  EXPECT_FALSE(p1 == p3);
}

// ---- Error paths ----

TEST_F(TestLogicalTableKeyPrefix, encode_null_buf)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, prefix.encode(nullptr, 64, pos));
}

TEST_F(TestLogicalTableKeyPrefix, encode_buf_too_short)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  char buf[24];  // 24 < 25
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, prefix.encode(buf, sizeof(buf), pos));
}

TEST_F(TestLogicalTableKeyPrefix, encode_buf_exact)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  char buf[25];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
  EXPECT_EQ(25, pos);
}

TEST_F(TestLogicalTableKeyPrefix, encode_with_offset_not_enough)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  char buf[30];
  int64_t pos = 10;  // remaining = 20 < 25
  EXPECT_EQ(OB_INVALID_ARGUMENT, prefix.encode(buf, sizeof(buf), pos));
}

TEST_F(TestLogicalTableKeyPrefix, decode_data_too_short)
{
  char buf[24] = {0};
  ObLogicalTableKeyPrefix decoded;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(buf, 24, pos));
}

TEST_F(TestLogicalTableKeyPrefix, decode_invalid_aux_type)
{
  // Encode a valid prefix first, then corrupt aux_type byte
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  char buf[64] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, prefix.encode(buf, sizeof(buf), pos));
  // aux_type is at offset 24 (last byte of prefix)
  buf[24] = static_cast<char>(LT_AUX_MAX_TYPE);
  ObLogicalTableKeyPrefix decoded;
  int64_t rp = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, decoded.decode(buf, pos, rp));
}

// ---- is_valid ----

TEST_F(TestLogicalTableKeyPrefix, is_valid_false_namespace)
{
  ObLogicalTableKeyPrefix prefix(0, 1, 0, LT_AUX_PRIMARY);
  EXPECT_FALSE(prefix.is_valid());
}

TEST_F(TestLogicalTableKeyPrefix, is_valid_false_logical_table_id)
{
  ObLogicalTableKeyPrefix prefix(1, 0, 0, LT_AUX_PRIMARY);
  EXPECT_FALSE(prefix.is_valid());
}

TEST_F(TestLogicalTableKeyPrefix, encode_invalid_prefix)
{
  ObLogicalTableKeyPrefix prefix(0, 1, 0, LT_AUX_PRIMARY);  // invalid ns_id
  char buf[64];
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, prefix.encode(buf, sizeof(buf), pos));
}

// ---- get_aux_type_col_descs ----

TEST_F(TestLogicalTableKeyPrefix, get_col_descs_all_types)
{
  struct ExpectedResult {
    ObLogicalTableAuxType type;
    int64_t expected_cnt;
  };
  ExpectedResult tests[] = {
    {LT_AUX_PRIMARY, 1},
    {LT_AUX_SEARCH_INDEX, 4},
    {LT_AUX_FTS_INVERTED, 2},
    {LT_AUX_FTS_FORWARD, 2},
    {LT_AUX_IVF_CID_VECTOR, 1},
    {LT_AUX_IVF_CID_ROWID, 2},
    {LT_AUX_IVF_ROWID_CID, 2},
  };
  for (size_t i = 0; i < ARRAYSIZEOF(tests); ++i) {
    const ObLogicalTableKeyColDesc *descs = nullptr;
    int64_t cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_aux_type_col_descs(tests[i].type, descs, cnt));
    EXPECT_EQ(tests[i].expected_cnt, cnt);
    EXPECT_NE(nullptr, descs);
  }
}

TEST_F(TestLogicalTableKeyPrefix, get_col_descs_invalid_type)
{
  const ObLogicalTableKeyColDesc *descs = nullptr;
  int64_t cnt = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, get_aux_type_col_descs(LT_AUX_MAX_TYPE, descs, cnt));
}

TEST_F(TestLogicalTableKeyPrefix, get_col_descs_search_index_layout)
{
  const ObLogicalTableKeyColDesc *descs = nullptr;
  int64_t cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_aux_type_col_descs(LT_AUX_SEARCH_INDEX, descs, cnt));
  ASSERT_EQ(4, cnt);
  EXPECT_EQ(common::ObIntType, descs[0].type_);
  EXPECT_EQ(common::ObVarcharType, descs[1].type_);
  EXPECT_TRUE(descs[1].is_nullable_);
  EXPECT_TRUE(descs[1].is_order_preserving_encoded_);
  EXPECT_EQ(common::ObVarcharType, descs[2].type_);
  EXPECT_TRUE(descs[2].is_nullable_);
  EXPECT_TRUE(descs[2].is_order_preserving_encoded_);
  EXPECT_EQ(common::ObUInt64Type, descs[3].type_);
}

TEST_F(TestLogicalTableKeyPrefix, get_col_descs_fts_inverted_layout)
{
  const ObLogicalTableKeyColDesc *descs = nullptr;
  int64_t cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_aux_type_col_descs(LT_AUX_FTS_INVERTED, descs, cnt));
  ASSERT_EQ(2, cnt);
  EXPECT_EQ(common::ObVarcharType, descs[0].type_);
  EXPECT_FALSE(descs[0].is_nullable_);
  EXPECT_FALSE(descs[0].is_order_preserving_encoded_);
  EXPECT_EQ(common::ObUInt64Type, descs[1].type_);
  EXPECT_FALSE(descs[1].is_nullable_);
}

// ---- reset ----

TEST_F(TestLogicalTableKeyPrefix, reset_clears_all_fields)
{
  ObLogicalTableKeyPrefix prefix(1, 100, 5, LT_AUX_PRIMARY);
  ASSERT_TRUE(prefix.is_valid());
  prefix.reset();
  EXPECT_EQ(OB_INVALID_NAMESPACE_ID, prefix.namespace_id_);
  EXPECT_EQ(OB_INVALID_LOGICAL_TABLE_ID, prefix.logical_table_id_);
  EXPECT_EQ(0UL, prefix.index_seq_);
  EXPECT_EQ(LT_AUX_MAX_TYPE, prefix.aux_type_);
  EXPECT_FALSE(prefix.is_valid());
}

// ---- decode null buf ----

TEST_F(TestLogicalTableKeyPrefix, decode_null_buf)
{
  ObLogicalTableKeyPrefix decoded;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(nullptr, 25, pos));
}

// ---- is_valid aux_type boundary ----

TEST_F(TestLogicalTableKeyPrefix, is_valid_false_aux_type)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_MAX_TYPE);
  EXPECT_FALSE(prefix.is_valid());
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

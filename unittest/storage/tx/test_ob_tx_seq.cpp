/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using transaction::ObTxSEQ;

TEST(TestObTxSEQ, current_wire_format)
{
  // Bytes emitted before this cleanup for sequence 100, branch 7.
  const unsigned char expected[] = {0x87, 0x80, 0xc8, 0x81, 0x80, 0x80, 0x80, 0x80, 0x40};
  const ObTxSEQ seq(100, 7);
  EXPECT_EQ((1ULL << 62) | (100ULL << 15) | 7, seq.cast_to_int());
  char buf[32] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, seq.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(expected), pos);
  EXPECT_EQ(0, MEMCMP(expected, buf, sizeof(expected)));
  EXPECT_EQ(pos, seq.get_serialize_size());
  ObTxSEQ decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  EXPECT_EQ(seq, decoded);
  EXPECT_EQ(seq, ObTxSEQ::cast_from_int(seq.cast_to_int()));
}

TEST(TestObTxSEQ, sentinels_and_ordering)
{
  const ObTxSEQ values[] = {ObTxSEQ::INVL(), ObTxSEQ::MIN_VAL(), ObTxSEQ::MAX_VAL()};
  EXPECT_EQ(0, values[0].cast_to_int());
  EXPECT_FALSE(values[0].is_valid());
  EXPECT_TRUE(values[1].is_valid());
  EXPECT_TRUE(values[2].is_valid());
  EXPECT_EQ(INT64_MAX, values[2].cast_to_int());
  EXPECT_LT(values[0], values[1]);
  EXPECT_LT(values[1], values[2]);
  for (const auto &seq : values) {
    char buf[32] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, seq.serialize(buf, sizeof(buf), pos));
    ObTxSEQ decoded(20, 3);
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
    EXPECT_EQ(seq, decoded);
    EXPECT_EQ(seq, ObTxSEQ::cast_from_int(seq.cast_to_int()));
  }
  const ObTxSEQ a(100, 7), b(100, 8);
  EXPECT_NE(a, b); // Equality includes the branch, ordering compares only the offset.
  EXPECT_LE(a, b);
  EXPECT_GE(a, b);
  EXPECT_FALSE(a < b);
  EXPECT_FALSE(a > b);
  EXPECT_LT(a, ObTxSEQ(101, 0));
}

TEST(TestObTxSEQ, arithmetic_and_plain_offsets)
{
  ObTxSEQ seq(100, 7);
  EXPECT_EQ(ObTxSEQ(101, 7), ++seq);
  EXPECT_EQ(ObTxSEQ(103, 7), seq + 2);
  EXPECT_EQ(ObTxSEQ(99, 7), seq - 2);
  ObTxSEQ cloned;
  ASSERT_EQ(OB_SUCCESS, seq.clone_with_seq(1023, 1000, cloned));
  EXPECT_EQ(ObTxSEQ(23, 7), cloned);
  // Lock-wait nodes transport get_seq(), not cast_to_int().
  const int64_t plain_offset = seq.get_seq();
  const ObTxSEQ holder_seq = ObTxSEQ::from_seq(plain_offset);
  EXPECT_EQ(plain_offset, holder_seq.get_seq());
  EXPECT_EQ(0, holder_seq.get_branch());
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::from_seq(0));
  EXPECT_EQ(3, ObTxSEQ::from_seq(3).get_seq()); // Table-lock diagnostic count.
  seq.atomic_reset();
  EXPECT_EQ(ObTxSEQ::INVL(), seq.atomic_load());
  seq.atomic_store(holder_seq);
  EXPECT_EQ(holder_seq, seq.atomic_load());
  seq.inc_update(ObTxSEQ(102, 0));
  EXPECT_EQ(ObTxSEQ(102, 0), seq);
}

TEST(TestObTxSEQ, reject_legacy_and_malformed_input)
{
  const int64_t invalid_raw[] = {1, 100, (1LL << 62) - 1, -1, INT64_MIN,
                                1LL << 62, (1LL << 62) | 7};
  for (int64_t raw : invalid_raw) {
    char buf[32] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), pos, raw));
    ObTxSEQ seq(42, 3);
    int64_t read_pos = 0;
    EXPECT_EQ(OB_VERSION_NOT_MATCH, seq.deserialize(buf, pos, read_pos));
    EXPECT_EQ(0, read_pos);
    EXPECT_EQ(ObTxSEQ(42, 3), seq);
  }
  const char truncated[] = {static_cast<char>(0x80)};
  ObTxSEQ seq(42, 3);
  int64_t pos = 0;
  EXPECT_NE(OB_SUCCESS, seq.deserialize(truncated, sizeof(truncated), pos));
  EXPECT_EQ(0, pos);
  EXPECT_EQ(ObTxSEQ(42, 3), seq);
}

TEST(TestObTxSEQ, checked_clone_preserves_output_on_error)
{
  const ObTxSEQ source(10, 7);
  ObTxSEQ result(42, 3);
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.clone_with_seq(100, 100, result));
  EXPECT_EQ(ObTxSEQ(42, 3), result);
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.clone_with_seq(100, 0, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.clone_with_seq(ObTxSEQ::MAX_SEQ + 101, 100, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObTxSEQ::INVL().clone_with_seq(101, 100, result));
  EXPECT_EQ(ObTxSEQ(42, 3), result);
  EXPECT_EQ(OB_SUCCESS, source.clone_with_seq(123, 100, result));
  EXPECT_EQ(ObTxSEQ(23, 7), result);
}

TEST(TestObTxSEQ, malformed_cast_from_int_returns_invl)
{
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::cast_from_int(100));
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::cast_from_int(-1));
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::cast_from_int(0));
  const ObTxSEQ encoded(100, 7);
  EXPECT_EQ(encoded, ObTxSEQ::cast_from_int(encoded.cast_to_int()));
}

TEST(TestObTxSEQ, sequence_bounds)
{
  EXPECT_FALSE(ObTxSEQ::is_valid_seq(0));
  EXPECT_FALSE(ObTxSEQ::is_valid_seq(-1));
  EXPECT_TRUE(ObTxSEQ::is_valid_seq(ObTxSEQ::MAX_SEQ));
  EXPECT_FALSE(ObTxSEQ::is_valid_seq(ObTxSEQ::MAX_SEQ + 1));
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::from_seq(0));
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::from_seq(-1));
  EXPECT_EQ(ObTxSEQ(ObTxSEQ::MAX_SEQ, 0), ObTxSEQ::from_seq(ObTxSEQ::MAX_SEQ));
  EXPECT_EQ(ObTxSEQ::INVL(), ObTxSEQ::from_seq(ObTxSEQ::MAX_SEQ + 1));
  EXPECT_EQ(ObTxSEQ::MAX_SEQ, ObTxSEQ(ObTxSEQ::MAX_SEQ, INT16_MAX).get_seq());
  EXPECT_EQ(ObTxSEQ::MAX_VAL(), ObTxSEQ(ObTxSEQ::MAX_SEQ, INT16_MAX));
#ifndef NDEBUG
  EXPECT_DEATH((void)ObTxSEQ(1LL << 47, 0), "");
  EXPECT_DEATH((void)ObTxSEQ(1, -1), "");
  EXPECT_DEATH((void)(ObTxSEQ::MAX_VAL() + 1), "");
#else
  EXPECT_FALSE(ObTxSEQ(1LL << 47, 0).is_valid());
  EXPECT_FALSE(ObTxSEQ(1, -1).is_valid());
  EXPECT_FALSE((ObTxSEQ::MAX_VAL() + 1).is_valid());
#endif
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#define private public
#define protected public
#include "share/logical_table/ob_logical_table_range_builder.h"
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/object/ob_object.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace share {

class TestLogicalTableRangeBuilder : public ::testing::Test
{
public:
  TestLogicalTableRangeBuilder() : arena_(ObModIds::TEST) {}
  ~TestLogicalTableRangeBuilder() {}
  void SetUp() override { arena_.reset(); }
  void TearDown() override {}

protected:
  ObArenaAllocator arena_;

  ObString make_string(const std::initializer_list<unsigned char> &bytes)
  {
    const int32_t len = static_cast<int32_t>(bytes.size());
    char *buf = static_cast<char *>(arena_.alloc(len));
    int32_t i = 0;
    for (unsigned char b : bytes) {
      buf[i++] = static_cast<char>(b);
    }
    return ObString(len, buf);
  }

  ObObj *alloc_objs(int64_t cnt)
  {
    return static_cast<ObObj *>(arena_.alloc(sizeof(ObObj) * cnt));
  }

  ObRowkey make_int_rowkey(const std::initializer_list<int64_t> &vals)
  {
    const int64_t n = static_cast<int64_t>(vals.size());
    ObObj *objs = alloc_objs(n);
    int64_t i = 0;
    for (int64_t v : vals) {
      new (&objs[i]) ObObj();
      objs[i].set_int(v);
      ++i;
    }
    ObRowkey rk;
    rk.assign(objs, n);
    return rk;
  }

  ObRowkey make_uint64_rowkey(uint64_t v)
  {
    ObObj *objs = alloc_objs(1);
    new (&objs[0]) ObObj();
    objs[0].set_uint64(v);
    ObRowkey rk;
    rk.assign(objs, 1);
    return rk;
  }

  ObRowkey make_min_row_rowkey(int64_t n = 1)
  {
    ObObj *objs = alloc_objs(n);
    for (int64_t i = 0; i < n; ++i) {
      new (&objs[i]) ObObj();
      objs[i].set_min_value();
    }
    ObRowkey rk;
    rk.assign(objs, n);
    return rk;
  }

  ObRowkey make_max_row_rowkey(int64_t n = 1)
  {
    ObObj *objs = alloc_objs(n);
    for (int64_t i = 0; i < n; ++i) {
      new (&objs[i]) ObObj();
      objs[i].set_max_value();
    }
    ObRowkey rk;
    rk.assign(objs, n);
    return rk;
  }

  // Build a rowkey from a list of obj builder lambdas.
  template <typename... Fs>
  ObRowkey make_rowkey(Fs... fs)
  {
    const int64_t n = sizeof...(Fs);
    ObObj *objs = alloc_objs(n);
    int64_t i = 0;
    int dummy[] = { (new (&objs[i]) ObObj(), fs(objs[i]), ++i, 0)... };
    (void)dummy;
    ObRowkey rk;
    rk.assign(objs, n);
    return rk;
  }

  ObNewRange make_range(const ObRowkey &s, const ObRowkey &e, bool inc_start, bool inc_end)
  {
    ObNewRange r;
    r.table_id_ = 1;
    r.start_key_ = s;
    r.end_key_ = e;
    r.border_flag_.set_data(0);
    if (inc_start) r.border_flag_.set_inclusive_start();
    if (inc_end) r.border_flag_.set_inclusive_end();
    return r;
  }

  uint64_t read_rowid_suffix(const ObString &kv_key)
  {
    uint64_t rowid = 0;
    const unsigned char *buf = reinterpret_cast<const unsigned char *>(kv_key.ptr());
    int64_t pos = kv_key.length() - static_cast<int64_t>(sizeof(uint64_t));
    if (pos >= OB_LOGICAL_TABLE_KEY_PREFIX_LEN) {
      (void)ObLogicalTableCodecUtils::decode_uint64(buf, kv_key.length(), pos, rowid);
    }
    return rowid;
  }

  bool kv_key_rowid_suffix_is(const ObString &kv_key, const uint64_t rowid)
  {
    if (kv_key.length() < OB_LOGICAL_TABLE_KEY_PREFIX_LEN + static_cast<int32_t>(sizeof(uint64_t))) {
      return false;
    }
    return read_rowid_suffix(kv_key) == rowid;
  }
};

// ============================================================================
// next_prefix
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, next_prefix_empty_input)
{
  ObString out;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableRangeBuilder::next_prefix(ObString(), arena_, out));
  EXPECT_TRUE(out.empty());
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_normal_increment_last)
{
  ObString input = make_string({0x01, 0x02, 0x03});
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
  ASSERT_EQ(3, out.length());
  EXPECT_EQ(static_cast<unsigned char>(0x01), static_cast<unsigned char>(out.ptr()[0]));
  EXPECT_EQ(static_cast<unsigned char>(0x02), static_cast<unsigned char>(out.ptr()[1]));
  EXPECT_EQ(static_cast<unsigned char>(0x04), static_cast<unsigned char>(out.ptr()[2]));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_trailing_ff_truncates)
{
  ObString input = make_string({0x01, 0x02, 0xFF});
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
  ASSERT_EQ(2, out.length());
  EXPECT_EQ(static_cast<unsigned char>(0x01), static_cast<unsigned char>(out.ptr()[0]));
  EXPECT_EQ(static_cast<unsigned char>(0x03), static_cast<unsigned char>(out.ptr()[1]));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_multiple_trailing_ff)
{
  ObString input = make_string({0x01, 0xFF, 0xFF, 0xFF});
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
  ASSERT_EQ(1, out.length());
  EXPECT_EQ(static_cast<unsigned char>(0x02), static_cast<unsigned char>(out.ptr()[0]));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_all_ff_overflow)
{
  ObString input = make_string({0xFF, 0xFF, 0xFF});
  ObString out;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_single_byte_ff_overflow)
{
  ObString input = make_string({0xFF});
  ObString out;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_single_byte_not_ff)
{
  ObString input = make_string({0x7F});
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
  ASSERT_EQ(1, out.length());
  EXPECT_EQ(static_cast<unsigned char>(0x80), static_cast<unsigned char>(out.ptr()[0]));
}

TEST_F(TestLogicalTableRangeBuilder, next_prefix_zero_byte_boundary)
{
  // {0x00, 0x00} -> {0x00, 0x01}
  ObString input = make_string({0x00, 0x00});
  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::next_prefix(input, arena_, out));
  ASSERT_EQ(2, out.length());
  EXPECT_EQ(0x00, out.ptr()[0]);
  EXPECT_EQ(0x01, out.ptr()[1]);
}

// ============================================================================
// convert_ranges: top-level error paths
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, convert_ranges_invalid_prefix)
{
  ObLogicalTableKeyPrefix prefix;  // default-constructed, uninitialized fields
  prefix.reset();  // now is_valid() returns false
  ObSEArray<ObNewRange, 1> logical_ranges;
  ObSEArray<ObNewRange, 1> physical_ranges;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
    ObLogicalTableRangeBuilder::convert_ranges(1001, prefix, logical_ranges, arena_, physical_ranges));
}

TEST_F(TestLogicalTableRangeBuilder, convert_ranges_empty_input_list)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> logical_ranges;
  ObSEArray<ObNewRange, 1> physical_ranges;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(1001, prefix, logical_ranges, arena_, physical_ranges));
  EXPECT_EQ(0, physical_ranges.count());
}

TEST_F(TestLogicalTableRangeBuilder, convert_ranges_propagates_invalid_range)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> logical_ranges;
  ObNewRange bad_range;  // default constructed - start_key/end_key empty
  ASSERT_EQ(OB_SUCCESS, logical_ranges.push_back(bad_range));
  ObSEArray<ObNewRange, 1> physical_ranges;
  EXPECT_NE(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(1001, prefix, logical_ranges, arena_, physical_ranges));
}

// ============================================================================
// Full flow: convert_ranges with single-rowid primary key ranges
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, convert_single_rowid_equal_range)
{
  // c1 = 42: start (42,MIN) : end (42,MAX), both inclusive from optimizer
  ObLogicalTableKeyPrefix prefix(7, 8, 0, LT_AUX_PRIMARY);
  ObNewRange r = make_range(make_uint64_rowkey(42), make_uint64_rowkey(42), true, true);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(9999, prefix, in, arena_, out));
  ASSERT_EQ(1, out.count());
  const ObNewRange &phy = out.at(0);
  EXPECT_EQ(9999UL, phy.table_id_);
  EXPECT_FALSE(phy.border_flag_.is_min_value());
  EXPECT_FALSE(phy.border_flag_.is_max_value());

  // start_key and end_key must be valid 2-column (ns_id uint64, kv_key varbinary) rowkeys
  ASSERT_EQ(2, phy.start_key_.get_obj_cnt());
  ASSERT_EQ(2, phy.end_key_.get_obj_cnt());
  EXPECT_EQ(ObUInt64Type, phy.start_key_.get_obj_ptr()[0].get_type());
  EXPECT_EQ(ObUInt64Type, phy.end_key_.get_obj_ptr()[0].get_type());
  EXPECT_EQ(7UL, phy.start_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_EQ(7UL, phy.end_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_EQ(ObVarcharType, phy.start_key_.get_obj_ptr()[1].get_type());
  EXPECT_EQ(ObVarcharType, phy.end_key_.get_obj_ptr()[1].get_type());

  // Start key kv_key should be exactly the encoded prefix+rowid(42).
  ObString start_bin = phy.start_key_.get_obj_ptr()[1].get_string();
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN + static_cast<int32_t>(sizeof(uint64_t)), start_bin.length());
}

TEST_F(TestLogicalTableRangeBuilder, convert_fts_forward_ge_range_uses_next_prefix)
{
  // rowid > 1: (1, MAX) .. (MAX, MAX)
  // FTS_FORWARD has 2 columns [rowid(uint64), token(varchar)]
  ObLogicalTableKeyPrefix prefix(7, 8, 0, LT_AUX_FTS_FORWARD);
  ObRowkey start = make_rowkey(
      [](ObObj &o){ o.set_uint64(1); },
      [](ObObj &o){ o.set_max_value(); });
  ObRowkey end = make_max_row_rowkey(2);
  ObNewRange r = make_range(start, end, false, false);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(1234, prefix, in, arena_, out));
  ASSERT_EQ(1, out.count());
  const ObNewRange &phy = out.at(0);
  // From normalization: MAX in start bound -> is_prefix_bound + use_next_prefix -> inclusive_start = true
  EXPECT_TRUE(phy.border_flag_.inclusive_start());
  // end is MAX-row prefix bound -> end becomes open (uninclusive_end)
  EXPECT_FALSE(phy.border_flag_.inclusive_end());
}

TEST_F(TestLogicalTableRangeBuilder, convert_fts_forward_with_min_sentinel_start)
{
  // FTS_FORWARD has 2 columns [rowid(uint64), token(varchar)]
  // Start (1, MIN) - is_prefix_bound = true, use_next_prefix = false
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_FTS_FORWARD);
  ObObj *s_objs = alloc_objs(2);
  new (&s_objs[0]) ObObj(); s_objs[0].set_uint64(1);
  new (&s_objs[1]) ObObj(); s_objs[1].set_min_value();
  ObRowkey start; start.assign(s_objs, 2);
  // End (1, MAX) - also prefix bound, use next prefix
  ObObj *e_objs = alloc_objs(2);
  new (&e_objs[0]) ObObj(); e_objs[0].set_uint64(1);
  new (&e_objs[1]) ObObj(); e_objs[1].set_max_value();
  ObRowkey end; end.assign(e_objs, 2);
  ObNewRange r = make_range(start, end, true, true);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(42, prefix, in, arena_, out));
  ASSERT_EQ(1, out.count());
  // Both bounds come from MIN/MAX truncation → inclusive_start true, inclusive_end false.
  EXPECT_TRUE(out.at(0).border_flag_.inclusive_start());
  EXPECT_FALSE(out.at(0).border_flag_.inclusive_end());
}

TEST_F(TestLogicalTableRangeBuilder, convert_primary_null_datum_in_key)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  // SEARCH_INDEX: (col_idx:int, path:varchar nullable, value:varchar nullable, rowid:uint64)
  ObString path = ObString::make_string("user.name");
  ObString value = ObString::make_string("alice");
  ObRowkey start = make_rowkey(
      [](ObObj &o){ o.set_int(3); },
      [](ObObj &o){ o.set_null(); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_uint64(100); });
  ObRowkey end = make_rowkey(
      [](ObObj &o){ o.set_int(3); },
      [](ObObj &o){ o.set_null(); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_uint64(100); });
  ObNewRange r = make_range(start, end, true, true);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(77, prefix, in, arena_, out));
  ASSERT_EQ(1, out.count());
}

TEST_F(TestLogicalTableRangeBuilder, convert_search_index_varchar_int)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString path = ObString::make_string("a.b");
  ObString value = ObString::make_string("v");
  ObRowkey start = make_rowkey(
      [](ObObj &o){ o.set_int(1); },
      [&](ObObj &o){ o.set_varchar(path); o.set_collation_type(CS_TYPE_BINARY); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_uint64(1); });
  ObRowkey end = make_rowkey(
      [](ObObj &o){ o.set_int(1); },
      [&](ObObj &o){ o.set_varchar(path); o.set_collation_type(CS_TYPE_BINARY); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_uint64(2); });
  ObNewRange r = make_range(start, end, true, false);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(55, prefix, in, arena_, out));
  ASSERT_EQ(1, out.count());
  // Non-prefix bounds preserve inclusive_start / inclusive_end from source
  EXPECT_TRUE(out.at(0).border_flag_.inclusive_start());
  EXPECT_FALSE(out.at(0).border_flag_.inclusive_end());
}

TEST_F(TestLogicalTableRangeBuilder, convert_unsupported_obj_type)
{
  // Double is not among the supported logical-range obj types.
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObRowkey start = make_rowkey(
      [](ObObj &o){ o.set_double(1.23); });
  ObRowkey end = make_rowkey(
      [](ObObj &o){ o.set_double(4.56); });
  ObNewRange r = make_range(start, end, true, true);
  ObSEArray<ObNewRange, 1> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  ObSEArray<ObNewRange, 1> out;
  EXPECT_EQ(OB_NOT_SUPPORTED,
    ObLogicalTableRangeBuilder::convert_ranges(1, prefix, in, arena_, out));
}

TEST_F(TestLogicalTableRangeBuilder, convert_multiple_ranges)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 4> in;
  for (uint64_t v : {10UL, 20UL, 30UL}) {
    ObNewRange r = make_range(make_uint64_rowkey(v), make_uint64_rowkey(v), true, true);
    ASSERT_EQ(OB_SUCCESS, in.push_back(r));
  }
  ObSEArray<ObNewRange, 4> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(100, prefix, in, arena_, out));
  ASSERT_EQ(3, out.count());
  // Output ranges should be strictly ordered by encoded start_key bytes.
  for (int i = 0; i < 2; ++i) {
    ObString s1 = out.at(i).start_key_.get_obj_ptr()[1].get_string();
    ObString s2 = out.at(i + 1).start_key_.get_obj_ptr()[1].get_string();
    EXPECT_LT(memcmp(s1.ptr(), s2.ptr(), std::min(s1.length(), s2.length())), 0);
  }
}

// ============================================================================
// Direct private-method tests
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_min_row)
{
  // logical_rowkey.is_min_row() branch: encodes [Prefix] with no fixed columns.
  // Encoder supports partial (prefix-only) encoding, so this succeeds.
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObRowkey rk = make_min_row_rowkey(1);
  ObString out_key;
  bool use_next = true, is_prefix = false;
  int ret = ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, true, arena_, out_key, use_next, is_prefix);
  EXPECT_TRUE(is_prefix);
  EXPECT_FALSE(use_next);  // MIN row path does not set use_next_prefix
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, out_key.length());
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_max_row)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObRowkey rk = make_max_row_rowkey(1);
  ObString out_key;
  bool use_next = false, is_prefix = false;
  int ret = ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, false, arena_, out_key, use_next, is_prefix);
  EXPECT_TRUE(is_prefix);
  EXPECT_TRUE(use_next);  // MAX row path sets use_next_prefix
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, out_key.length());
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_min_value_mid_key)
{
  // Rowkey: (col_idx=3, MIN, MIN, MIN) - the first MIN sentinel terminates encoding.
  // Only col_idx=3 is encoded as a partial prefix key.
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObRowkey rk = make_rowkey(
      [](ObObj &o){ o.set_int(3); },
      [](ObObj &o){ o.set_min_value(); },
      [](ObObj &o){ o.set_min_value(); },
      [](ObObj &o){ o.set_min_value(); });
  ObString out_key;
  bool use_next = true, is_prefix = false;
  int ret = ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, true, arena_, out_key, use_next, is_prefix);
  EXPECT_TRUE(is_prefix);
  EXPECT_FALSE(use_next);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_GT(out_key.length(), OB_LOGICAL_TABLE_KEY_PREFIX_LEN);
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_max_value_mid_key)
{
  // Rowkey: (col=3, path, value, MAX) - MAX terminates encoding at last pos.
  // 3 datums encoded as partial prefix key for SEARCH_INDEX (4 cols).
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString path = ObString::make_string("p");
  ObString value = ObString::make_string("v");
  ObRowkey rk = make_rowkey(
      [](ObObj &o){ o.set_int(3); },
      [&](ObObj &o){ o.set_varchar(path); o.set_collation_type(CS_TYPE_BINARY); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_max_value(); });
  ObString out_key;
  bool use_next = false, is_prefix = false;
  int ret = ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, false, arena_, out_key, use_next, is_prefix);
  EXPECT_TRUE(is_prefix);
  EXPECT_TRUE(use_next);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_GT(out_key.length(), OB_LOGICAL_TABLE_KEY_PREFIX_LEN);
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_all_concrete_values)
{
  // Rowkey with fully-specified values - no MIN/MAX sentinel, no prefix-bound flag.
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObRowkey rk = make_uint64_rowkey(42);
  ObString out_key;
  bool use_next = true, is_prefix = true;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, true, arena_, out_key, use_next, is_prefix));
  EXPECT_FALSE(is_prefix);
  EXPECT_FALSE(use_next);
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN + static_cast<int64_t>(sizeof(uint64_t)), out_key.length());
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_int64_type)
{
  // SEARCH_INDEX first column is int64; covers ObIntType switch branch.
  ObLogicalTableKeyPrefix prefix(1, 1, 1, LT_AUX_SEARCH_INDEX);
  ObString path = ObString::make_string("a");
  ObString value = ObString::make_string("b");
  ObRowkey rk = make_rowkey(
      [](ObObj &o){ o.set_int(7); },
      [&](ObObj &o){ o.set_varchar(path); o.set_collation_type(CS_TYPE_BINARY); },
      [&](ObObj &o){ o.set_varchar(value); o.set_collation_type(CS_TYPE_BINARY); },
      [](ObObj &o){ o.set_uint64(9); });
  ObString out_key;
  bool use_next = true, is_prefix = true;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, true, arena_, out_key, use_next, is_prefix));
  EXPECT_FALSE(is_prefix);
  EXPECT_FALSE(use_next);
  EXPECT_GT(out_key.length(), OB_LOGICAL_TABLE_KEY_PREFIX_LEN);
}

TEST_F(TestLogicalTableRangeBuilder, encode_rowkey_bound_unsupported_float_type)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObRowkey rk = make_rowkey([](ObObj &o){ o.set_float(3.14f); });
  ObString out_key;
  bool use_next = false, is_prefix = false;
  EXPECT_EQ(OB_NOT_SUPPORTED, ObLogicalTableRangeBuilder::encode_rowkey_bound_(
      prefix, rk, true, arena_, out_key, use_next, is_prefix));
}

// ============================================================================
// End-to-end ordering sanity: physical ranges preserve logical order
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, end_to_end_ordering_preserved)
{
  ObLogicalTableKeyPrefix prefix(1, 1, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 4> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(make_range(make_uint64_rowkey(1), make_uint64_rowkey(1), true, true)));
  ASSERT_EQ(OB_SUCCESS, in.push_back(make_range(make_uint64_rowkey(UINT64_MAX), make_uint64_rowkey(UINT64_MAX), true, true)));
  ObSEArray<ObNewRange, 4> out;
  ASSERT_EQ(OB_SUCCESS,
    ObLogicalTableRangeBuilder::convert_ranges(1, prefix, in, arena_, out));
  ASSERT_EQ(2, out.count());
  ObString a = out.at(0).start_key_.get_obj_ptr()[1].get_string();
  ObString b = out.at(1).start_key_.get_obj_ptr()[1].get_string();
  EXPECT_LT(memcmp(a.ptr(), b.ptr(), std::min(a.length(), b.length())), 0);
}

// ============================================================================
// build_namespace_range
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, build_namespace_range_two_column_rowkey)
{
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS,
      ObLogicalTableRangeBuilder::build_namespace_range(1001, 7, arena_, range));
  EXPECT_EQ(1001UL, range.table_id_);
  EXPECT_TRUE(range.border_flag_.inclusive_start());
  EXPECT_FALSE(range.border_flag_.inclusive_end());
  ASSERT_EQ(2, range.start_key_.get_obj_cnt());
  ASSERT_EQ(2, range.end_key_.get_obj_cnt());
  EXPECT_EQ(7UL, range.start_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_EQ(8UL, range.end_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_EQ(0, range.start_key_.get_obj_ptr()[1].get_string().length());
  EXPECT_EQ(0, range.end_key_.get_obj_ptr()[1].get_string().length());
}

TEST_F(TestLogicalTableRangeBuilder, build_namespace_range_max_namespace)
{
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS,
      ObLogicalTableRangeBuilder::build_namespace_range(
          1001, UINT64_MAX, arena_, range));
  ASSERT_EQ(2, range.start_key_.get_obj_cnt());
  ASSERT_EQ(2, range.end_key_.get_obj_cnt());
  EXPECT_EQ(UINT64_MAX, range.start_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_EQ(UINT64_MAX, range.end_key_.get_obj_ptr()[0].get_uint64());
  EXPECT_TRUE(range.end_key_.get_obj_ptr()[1].is_max_value());
  EXPECT_TRUE(range.border_flag_.inclusive_end());
  EXPECT_TRUE(range.border_flag_.is_max_value());
}

// ============================================================================
// build_primary_ranges rowid bound semantics
// ============================================================================

TEST_F(TestLogicalTableRangeBuilder, build_primary_ranges_rowid_lt_max)
{
  ObLogicalTableKeyPrefix prefix(1, 2, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> ranges;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::build_primary_ranges(
      9999, prefix, 2, true, true,
      false, 0, true,
      true, UINT64_MAX, false,
      arena_, ranges));
  ASSERT_EQ(1, ranges.count());
  const ObNewRange &phy = ranges.at(0);
  EXPECT_FALSE(phy.border_flag_.inclusive_end());
  ASSERT_EQ(2, phy.end_key_.get_obj_cnt());
  ObString end_kv = phy.end_key_.get_obj_ptr()[1].get_string();
  EXPECT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN + static_cast<int32_t>(sizeof(uint64_t)),
            end_kv.length());
  EXPECT_TRUE(kv_key_rowid_suffix_is(end_kv, UINT64_MAX));
}

TEST_F(TestLogicalTableRangeBuilder, build_primary_ranges_rowid_le_max)
{
  ObLogicalTableKeyPrefix prefix(1, 2, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> ranges;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::build_primary_ranges(
      9999, prefix, 2, true, true,
      false, 0, true,
      true, UINT64_MAX, true,
      arena_, ranges));
  ASSERT_EQ(1, ranges.count());
  const ObNewRange &phy = ranges.at(0);
  EXPECT_TRUE(phy.border_flag_.inclusive_end());
  ObString end_kv = phy.end_key_.get_obj_ptr()[1].get_string();
  EXPECT_TRUE(kv_key_rowid_suffix_is(end_kv, UINT64_MAX));
}

TEST_F(TestLogicalTableRangeBuilder, build_primary_ranges_rowid_gt_start)
{
  ObLogicalTableKeyPrefix prefix(1, 2, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> ranges;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::build_primary_ranges(
      9999, prefix, 2, true, true,
      true, 100, false,
      false, 0, true,
      arena_, ranges));
  ASSERT_EQ(1, ranges.count());
  ObString start_kv = ranges.at(0).start_key_.get_obj_ptr()[1].get_string();
  EXPECT_TRUE(kv_key_rowid_suffix_is(start_kv, 101));
}

TEST_F(TestLogicalTableRangeBuilder, build_primary_ranges_unbounded_rowid_upper)
{
  ObLogicalTableKeyPrefix prefix(1, 2, 0, LT_AUX_PRIMARY);
  ObSEArray<ObNewRange, 1> ranges;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableRangeBuilder::build_primary_ranges(
      9999, prefix, 2, true, true,
      false, 0, true,
      false, 0, true,
      arena_, ranges));
  ASSERT_EQ(1, ranges.count());
  ObString end_kv = ranges.at(0).end_key_.get_obj_ptr()[1].get_string();
  ASSERT_EQ(OB_LOGICAL_TABLE_KEY_PREFIX_LEN, end_kv.length());
  EXPECT_EQ(static_cast<unsigned char>(1),
            static_cast<unsigned char>(end_kv.ptr()[OB_LOGICAL_TABLE_KEY_PREFIX_LEN - 1]));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

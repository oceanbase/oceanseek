/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_info.h"
#include "storage/blocksstable/async_index/ob_inv_idx_aggregator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"

#include <gtest/gtest.h>
#include <vector>

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

struct ExpectEntry
{
  int64_t block_idx_;
  int64_t doc_count_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  uint64_t total_doc_length_;
  std::vector<std::pair<uint32_t, uint32_t>> frontier_; // (token_freq, doc_length)
};

class InvIdxAggDataEncoderTest : public ::testing::Test
{
protected:
  InvIdxAggDataEncoderTest() {}
  virtual ~InvIdxAggDataEncoderTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  static void fill_pairs(
      common::ObArray<ObInvIdxBM25ParamPair> &arr,
      const std::vector<std::pair<uint32_t, uint32_t>> &values)
  {
    for (int64_t i = 0; i < static_cast<int64_t>(values.size()); ++i) {
      ObInvIdxBM25ParamPair pair;
      pair.token_freq_ = values[i].first;
      pair.doc_length_ = values[i].second;
      ASSERT_EQ(OB_SUCCESS, arr.push_back(pair));
    }
  }

  static void set_meta(
      ObInvIdxLogicalBlockMeta &meta,
      const int64_t block_idx,
      const int64_t doc_count,
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      const uint64_t total_doc_length,
      common::ObArray<ObInvIdxBM25ParamPair> &frontier_pairs)
  {
    meta.block_idx_ = block_idx;
    meta.doc_count_ = doc_count;
    meta.min_doc_id_ = min_doc_id;
    meta.max_doc_id_ = max_doc_id;
    meta.total_doc_length_ = total_doc_length;
    // frontier_pairs_ was changed from a pointer to an in-place ObSEArray;
    // copy the entries in via assign(), matching the writer's own usage.
    ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.assign(frontier_pairs));
  }

  // Parse the produced agg data back via the on-disk structs and assert every field.
  static void verify_agg_data(
      const char *buf,
      const int64_t len,
      const uint32_t posting_value_offset,
      const uint32_t posting_value_size,
      const std::vector<ExpectEntry> &entries)
  {
    ASSERT_NE(nullptr, buf);

    // payload header
    int64_t pos = 0;
    const ObInvIdxAggDataHeader *h = reinterpret_cast<const ObInvIdxAggDataHeader *>(buf + pos);
    const uint8_t h_version = h->version_;
    const uint8_t h_flags = h->flags_;
    const uint16_t h_count = h->logical_block_count_;
    const uint32_t h_pv_offset = h->posting_value_offset_;
    const uint32_t h_pv_size = h->posting_value_size_;
    const uint32_t h_total_size = h->total_size_;
    EXPECT_EQ(static_cast<int64_t>(ObInvIdxAggDataHeader::VERSION), static_cast<int64_t>(h_version));
    EXPECT_EQ(0, static_cast<int64_t>(h_flags));
    EXPECT_EQ(static_cast<int64_t>(entries.size()), static_cast<int64_t>(h_count));
    EXPECT_EQ(static_cast<int64_t>(posting_value_offset), static_cast<int64_t>(h_pv_offset));
    EXPECT_EQ(static_cast<int64_t>(posting_value_size), static_cast<int64_t>(h_pv_size));
    EXPECT_EQ(len, static_cast<int64_t>(h_total_size));
    pos += static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));

    // entries
    const char *entry_base = buf + pos;
    int64_t expect_frontier_offset = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(entries.size()); ++i) {
      const ObInvIdxAggDataEntry *e = reinterpret_cast<const ObInvIdxAggDataEntry *>(
          entry_base + i * static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry)));
      const uint16_t block_idx = e->block_idx_;
      const uint16_t frontier_count = e->frontier_count_;
      const uint16_t frontier_offset = e->frontier_offset_;
      const uint32_t doc_count = e->doc_count_;
      const uint64_t min_doc_id = e->min_doc_id_;
      const uint64_t max_doc_id = e->max_doc_id_;
      const uint64_t total_doc_length = e->total_doc_length_;
      EXPECT_EQ(entries[i].block_idx_, static_cast<int64_t>(block_idx));
      EXPECT_EQ(static_cast<int64_t>(entries[i].frontier_.size()), static_cast<int64_t>(frontier_count));
      EXPECT_EQ(expect_frontier_offset, static_cast<int64_t>(frontier_offset));
      EXPECT_EQ(entries[i].doc_count_, static_cast<int64_t>(doc_count));
      EXPECT_EQ(entries[i].min_doc_id_, min_doc_id);
      EXPECT_EQ(entries[i].max_doc_id_, max_doc_id);
      EXPECT_EQ(entries[i].total_doc_length_, total_doc_length);
      expect_frontier_offset += static_cast<int64_t>(entries[i].frontier_.size())
          * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));
    }
    pos += static_cast<int64_t>(entries.size()) * static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));

    // frontier pool (in entry order)
    const char *pool = buf + pos;
    int64_t pair_idx = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(entries.size()); ++i) {
      for (int64_t j = 0; j < static_cast<int64_t>(entries[i].frontier_.size()); ++j) {
        const ObInvIdxBM25ParamPair *p = reinterpret_cast<const ObInvIdxBM25ParamPair *>(
            pool + pair_idx * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair)));
        const uint32_t token_freq = p->token_freq_;
        const uint32_t doc_length = p->doc_length_;
        EXPECT_EQ(static_cast<int64_t>(entries[i].frontier_[j].first), static_cast<int64_t>(token_freq));
        EXPECT_EQ(static_cast<int64_t>(entries[i].frontier_[j].second), static_cast<int64_t>(doc_length));
        ++pair_idx;
      }
    }

    const int64_t expect_len = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader))
        + static_cast<int64_t>(entries.size()) * static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry))
        + pair_idx * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));
    EXPECT_EQ(expect_len, len);
  }

  // Round-trip the produced agg data through ObInvIdxAggDataDecoder and assert every
  // decoded field. frontier pairs are materialized pair-by-pair (compaction logical-block
  // reuse and query-side block-max pruning depend on them), so each decoded meta must
  // carry the exact frontier pairs fed into the encoder.
  static void verify_decoded(
      const char *buf,
      const int64_t len,
      const uint32_t posting_value_offset,
      const uint32_t posting_value_size,
      const std::vector<ExpectEntry> &entries)
  {
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    int64_t pv_offset = -1;
    int64_t pv_size = -1;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataDecoder::decode(buf, len, metas, pv_offset, pv_size));
    EXPECT_EQ(static_cast<int64_t>(posting_value_offset), pv_offset);
    EXPECT_EQ(static_cast<int64_t>(posting_value_size), pv_size);
    ASSERT_EQ(static_cast<int64_t>(entries.size()), metas.count());
    for (int64_t i = 0; i < metas.count(); ++i) {
      const ObInvIdxLogicalBlockMeta &m = metas.at(i);
      EXPECT_EQ(entries[i].block_idx_, m.block_idx_);
      EXPECT_EQ(entries[i].doc_count_, m.doc_count_);
      EXPECT_EQ(entries[i].min_doc_id_, m.min_doc_id_);
      EXPECT_EQ(entries[i].max_doc_id_, m.max_doc_id_);
      EXPECT_EQ(entries[i].total_doc_length_, m.total_doc_length_);
      ASSERT_EQ(static_cast<int64_t>(entries[i].frontier_.size()), m.frontier_pairs_.count());
      for (int64_t j = 0; j < m.frontier_pairs_.count(); ++j) {
        const ObInvIdxBM25ParamPair &pair = m.frontier_pairs_.at(j);
        EXPECT_EQ(static_cast<int64_t>(entries[i].frontier_[j].first),
            static_cast<int64_t>(pair.token_freq_));
        EXPECT_EQ(static_cast<int64_t>(entries[i].frontier_[j].second),
            static_cast<int64_t>(pair.doc_length_));
      }
    }
  }
};

TEST_F(InvIdxAggDataEncoderTest, legacy_v1_bytes_remain_readable)
{
  // Fixed V1 bytes, independent of the new writer: one logical block, doc 7, L = 100.
  const char legacy[] = {
    1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
    7, 0, 0, 0, 0, 0, 0, 0,
    7, 0, 0, 0, 0, 0, 0, 0,
    100, 0, 0, 0, 0, 0, 0, 0
  };
  uint64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataDecoder::get_total_doc_length(legacy, sizeof(legacy), length));
  ASSERT_EQ(100, length);
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  int64_t offset = 0;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataDecoder::decode(legacy, sizeof(legacy), metas, offset, size));
  ASSERT_EQ(1, metas.count());
  ASSERT_EQ(7, metas.at(0).min_doc_id_);
  ASSERT_EQ(100, metas.at(0).total_doc_length_);

  // Sharing the RowDesc carrier must not change the leaf wire protocol.
  common::ObArenaAllocator allocator;
  char *encoded = nullptr;
  int64_t encoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, offset, size, metas,
      encoded, encoded_size));
  ASSERT_EQ(sizeof(legacy), encoded_size);
  ASSERT_EQ(0, MEMCMP(legacy, encoded, encoded_size));
}

TEST_F(InvIdxAggDataEncoderTest, subtree_stats_number_precision)
{
  ObInvIdxAggInfo stats;
  uint64_t length = UINT64_MAX;

  common::ObArenaAllocator allocator;
  ObInvIdxAggInfo aggregate;
  aggregate.init_aggregate();
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, aggregate.merge(stats));
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 1));
  ASSERT_EQ(OB_SUCCESS, aggregate.merge(stats));
  ASSERT_EQ(OB_SIZE_OVERFLOW, aggregate.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
  common::number::ObNumber expected;
  common::number::ObNumber value;
  ASSERT_EQ(OB_SUCCESS, expected.from("18446744073709551616", allocator));
  ASSERT_EQ(OB_SUCCESS, aggregate.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(0, value.compare(expected));

  ObInvIdxAggInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, expected));
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, aggregate.merge(copied));
  ASSERT_EQ(OB_SUCCESS, expected.from("36893488147419103232", allocator));
  ASSERT_EQ(OB_SUCCESS, aggregate.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(0, value.compare(expected));
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_round_trip_and_owned_values)
{
  common::ObArenaAllocator allocator;
  // Cover zero, ordinary values, and the Number boundary beyond uint64.
  for (const char *input : {"0", "7", "18446744073709551615", "18446744073709551616"}) {
    common::number::ObNumber expected;
    ObInvIdxAggInfo stats;
    ASSERT_EQ(OB_SUCCESS, expected.from(input, allocator));
    ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, expected));
    int64_t size = -1;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::get_serialize_size(stats, size));
    const int64_t offset = 1; // Number digits at offset + 13 are not uint32-aligned.
    std::vector<char> buf(offset + size + 2, 0x5a);
    int64_t pos = offset;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::serialize(stats, buf.data(), offset + size, pos));
    ASSERT_EQ(offset + size, pos);
    EXPECT_EQ(0x5a, buf[0]);
    EXPECT_EQ(0x5a, buf[offset + size]);
    ObInvIdxAggInfo decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 42));
    int64_t read_pos = offset;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(buf.data(), pos, read_pos, decoded));
    ASSERT_EQ(pos, read_pos);
    MEMSET(buf.data(), 0xa5, buf.size());
    stats.reset();
    common::number::ObNumber actual;
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, actual));
    EXPECT_EQ(0, expected.compare(actual)) << input;
  }
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_v1_fixture_and_concatenation)
{
  // A fixed V1 record for total length 7, independent of this writer:
  // i8 version; big-endian i64 mask; ObNumber descriptor and native digit bytes.
  const unsigned char v1[] = {1, 0, 0, 0, 0, 0, 0, 0, 1, 0xc0, 0, 0, 1, 7, 0, 0, 0};
  ObInvIdxAggInfo stats;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(reinterpret_cast<const char *>(v1), sizeof(v1), pos, stats));
  ASSERT_EQ(sizeof(v1), pos);
  uint64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(7, value);
  char buf[256];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::serialize(stats, buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(v1), pos);
  ASSERT_EQ(0, MEMCMP(v1, buf, sizeof(v1)));
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 100));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::serialize(stats, buf, sizeof(buf), pos));
  int64_t read_pos = 0;
  for (const uint64_t expected : {7UL, 100UL}) {
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(buf, pos, read_pos, stats));
    ASSERT_EQ(OB_SUCCESS, stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
    EXPECT_EQ(expected, value);
  }
  ASSERT_EQ(pos, read_pos);
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_truncation_preserves_outputs)
{
  ObInvIdxAggInfo stats;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, UINT64_MAX));
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::get_serialize_size(stats, size));
  char buf[256];
  const int64_t offset = 3;
  int64_t pos = offset;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::serialize(stats, buf, sizeof(buf), pos));
  for (int64_t available = 0; available < size; ++available) {
    SCOPED_TRACE(available);
    ObInvIdxAggInfo decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 42));
    int64_t read_pos = offset;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, ObInvIdxAggInfoSerializer::deserialize(buf, offset + available, read_pos, decoded));
    EXPECT_EQ(offset, read_pos);
    uint64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
    EXPECT_EQ(42, value);
    char small[256];
    MEMSET(small, 0x5a, sizeof(small));
    int64_t write_pos = offset;
    EXPECT_EQ(OB_BUF_NOT_ENOUGH, ObInvIdxAggInfoSerializer::serialize(stats, small, offset + available, write_pos));
    EXPECT_EQ(offset, write_pos);
    for (const char byte : small) {
      ASSERT_EQ(0x5a, byte);
    }
  }
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_rejects_malformed_numbers)
{
  // Every malformed record starts at a nonzero offset and must preserve the
  // caller's value and cursor, including a corrupt length that fits the input.
  const unsigned char v1[] = {1, 0, 0, 0, 0, 0, 0, 0, 1, 0xc0, 0, 0, 1, 7, 0, 0, 0};
  for (int kind = 0; kind < 7; ++kind) {
    SCOPED_TRACE(kind);
    char buf[256] = {};
    const int64_t offset = 3;
    MEMCPY(buf + offset, v1, sizeof(v1));
    int expected_ret = OB_DESERIALIZE_ERROR;
    int64_t length = offset + sizeof(v1);
    switch (kind) {
      case 0: buf[offset] = 2; expected_ret = OB_VERSION_NOT_MATCH; break;
      case 1: buf[offset + 8] = 2; break; // Unknown statistic bit.
      case 2: buf[offset + 12] = common::number::ObNumber::MAX_STORE_LEN + 1;
              length = sizeof(buf); break; // Digits would exceed the inline destination.
      case 3: buf[offset + 11] = 1; break; // Reserved Number descriptor byte.
      case 4: buf[offset + 9] = 0x40; break; // Negative Number.
      case 5: buf[offset + 9] = 0xbf; break; // Fractional Number.
      case 6: {
        const uint32_t invalid_digit = common::number::ObNumber::BASE;
        MEMCPY(buf + offset + 13, &invalid_digit, sizeof(invalid_digit));
        break;
      }
    }
    ObInvIdxAggInfo decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 42));
    int64_t pos = offset;
    EXPECT_EQ(expected_ret, ObInvIdxAggInfoSerializer::deserialize(buf, length, pos, decoded));
    EXPECT_EQ(offset, pos);
    uint64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
    EXPECT_EQ(42, value);
  }
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_invalid_arguments_and_values)
{
  ObInvIdxAggInfo stats;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 7));
  char buf[256];
  MEMSET(buf, 0x5a, sizeof(buf));
  for (const int64_t initial : {-1L, 257L, INT64_MAX}) {
    int64_t pos = initial;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::deserialize(buf, sizeof(buf), pos, stats));
    EXPECT_EQ(initial, pos);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::serialize(stats, buf, sizeof(buf), pos));
    EXPECT_EQ(initial, pos);
  }
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::deserialize(nullptr, 1, pos, stats));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::serialize(stats, nullptr, 1, pos));
  EXPECT_EQ(0, pos);
  common::ObArenaAllocator allocator;
  for (const char *input : {"-1", "0.5"}) {
    common::number::ObNumber value;
    ASSERT_EQ(OB_SUCCESS, value.from(input, allocator));
    ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
    int64_t size = 42;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::get_serialize_size(stats, size));
    EXPECT_EQ(42, size);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggInfoSerializer::serialize(stats, buf, sizeof(buf), pos));
    EXPECT_EQ(0, pos);
  }
  for (const char byte : buf) {
    EXPECT_EQ(0x5a, byte);
  }
}

TEST_F(InvIdxAggDataEncoderTest, index_row_protocol_feeds_parent_aggregation)
{
  ObInvIdxAggregator aggregator;
  const uint64_t lengths[] = {7, 100, 24};
  for (const uint64_t length : lengths) {
    ObInvIdxAggInfo info;
    ASSERT_EQ(OB_SUCCESS, info.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
    char buf[128];
    int64_t size = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::serialize(info, buf, sizeof(buf), size));
    ObIndexBlockRowDesc row;
    row.agg_type_ = ObIndexRowAggType::INVERTED_INDEX_AGG;
    row.is_data_block_ = false;
    row.row_count_ = 1;
    row.inverted_index_agg_ = {buf, size};
    ASSERT_EQ(OB_SUCCESS, aggregator.eval(row));
  }
  common::ObArenaAllocator allocator;
  ObIndexBlockRowDesc parent;
  ASSERT_EQ(OB_SUCCESS, aggregator.fill_row_desc(parent, allocator));
  ObInvIdxAggInfo summary;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(
      parent.inverted_index_agg_.buf_, parent.inverted_index_agg_.size_, pos, summary));
  uint64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, summary.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, actual));
  EXPECT_EQ(131, actual);
}

TEST_F(InvIdxAggDataEncoderTest, invalid_stat_id_preserves_values)
{
  ObInvIdxAggInfo stats;
  uint64_t length = 42;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
  ASSERT_EQ(42, length);
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 100));
  for (const auto stat : {ObInvIdxBM25Stat::MAX_STAT,
                         static_cast<ObInvIdxBM25Stat>(64),
                         static_cast<ObInvIdxBM25Stat>(255)}) {
    ASSERT_EQ(OB_INVALID_ARGUMENT, stats.set(stat, 200));
    ASSERT_EQ(OB_INVALID_ARGUMENT, stats.get(stat, length));
    ASSERT_EQ(42, length);
  }
  ASSERT_EQ(OB_SUCCESS, stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
  ASSERT_EQ(100, length);
}

TEST_F(InvIdxAggDataEncoderTest, test_single_block)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta meta;
  char *buf;
  int64_t buf_size;

  fill_pairs(frontier_pairs, {{10, 100}});
  set_meta(meta, /*idx*/ 0, /*doc_count*/ 5, /*min*/ 3, /*max*/ 42,
      /*total_doc_length*/ 100, frontier_pairs);
  ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));

  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 1024, 256, metas, buf, buf_size));
  verify_agg_data(buf, buf_size, 1024, 256, {{0, 5, 3, 42, 100, {{10, 100}}}});
}

TEST_F(InvIdxAggDataEncoderTest, test_multi_block_and_empty_frontier)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs0, frontier_pairs1, frontier_pairs2;
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0, m1, m2;
  char *buf;
  int64_t buf_size;

  fill_pairs(frontier_pairs0, {{10, 100}, {8, 200}});
  fill_pairs(frontier_pairs1, {});
  fill_pairs(frontier_pairs2, {{5, 50}, {3, 30}, {1, 10}});
  set_meta(m0, 0, 7, 1, 70, 100, frontier_pairs0);
  set_meta(m1, 1, 2, 80, 90, 200, frontier_pairs1);
  set_meta(m2, 2, 9, 100, 900, 300, frontier_pairs2);
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m1));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m2));

  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 64, 4096, metas, buf, buf_size));
  verify_agg_data(buf, buf_size, 64, 4096, {
      {0, 7, 1, 70, 100, {{10, 100}, {8, 200}}},
      {1, 2, 80, 90, 200, {}},
      {2, 9, 100, 900, 300, {{5, 50}, {3, 30}, {1, 10}}}});
}

TEST_F(InvIdxAggDataEncoderTest, test_encode_decode_round_trip)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs0, frontier_pairs1, frontier_pairs2;
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0, m1, m2;
  char *buf;
  int64_t buf_size;

  fill_pairs(frontier_pairs0, {{10, 100}, {8, 200}});
  fill_pairs(frontier_pairs1, {});
  fill_pairs(frontier_pairs2, {{5, 50}, {3, 30}, {1, 10}});
  set_meta(m0, 0, 7, 1, 70, 100, frontier_pairs0);
  set_meta(m1, 1, 2, 80, 90, 200, frontier_pairs1);
  set_meta(m2, 2, 9, 100, 900, 300, frontier_pairs2);
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m1));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m2));

  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 64, 4096, metas, buf, buf_size));
  const std::vector<ExpectEntry> expect = {
      {0, 7, 1, 70, 100, {{10, 100}, {8, 200}}},
      {1, 2, 80, 90, 200, {}},
      {2, 9, 100, 900, 300, {{5, 50}, {3, 30}, {1, 10}}}};
  verify_agg_data(buf, buf_size, 64, 4096, expect);
  verify_decoded(buf, buf_size, 64, 4096, expect);
}

TEST_F(InvIdxAggDataEncoderTest, test_invalid_meta)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  char *buf;
  int64_t buf_size;

  // empty metas
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));

  // min > max -> is_valid() fails
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> bad;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{1, 1}});
    set_meta(meta, 0, 1, /*min*/ 100, /*max*/ 50, /*total_doc_length*/ 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, bad.push_back(meta));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggDataEncoder::build(allocator, 64, 16, bad, buf, buf_size));
  }
}

TEST_F(InvIdxAggDataEncoderTest, test_field_overflow)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf;
  int64_t buf_size;

  // block_idx > UINT16_MAX
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{1, 1}});
    set_meta(meta, /*idx*/ 70000, 1, 1, 2, 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));
  }
  // doc_count > UINT16_MAX is valid now
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{1, 1}});
    set_meta(meta, 0, /*doc_count*/ 70000, 1, 2, 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));
    verify_agg_data(buf, buf_size, 64, 16, {{0, 70000, 1, 2, 1, {{1, 1}}}});
  }
  // doc_count > UINT32_MAX
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{1, 1}});
    set_meta(meta, 0, /*doc_count*/ static_cast<int64_t>(UINT32_MAX) + 1, 1, 2, 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));
  }
  // max uint32 frontier values are valid
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{UINT32_MAX, UINT32_MAX}});
    set_meta(meta, 0, 1, 1, 2, 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));
  }
  // posting value offset out of uint32 range
  {
    common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
    common::ObArray<ObInvIdxLogicalBlockMeta> metas;
    ObInvIdxLogicalBlockMeta meta;
    fill_pairs(frontier_pairs, {{1, 1}});
    set_meta(meta, 0, 1, 1, 2, 1, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SIZE_OVERFLOW,
        ObInvIdxAggDataEncoder::build(allocator, static_cast<int64_t>(UINT32_MAX) + 1, 16, metas, buf, buf_size));
  }
}

TEST_F(InvIdxAggDataEncoderTest, test_total_size_overflow)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<ObInvIdxBM25ParamPair> frontier_pairs;
  common::ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta meta;
  char *buf;
  int64_t buf_size;

  // one block with enough frontier pairs to push the total over INT16_MAX (8B per pair)
  std::vector<std::pair<uint32_t, uint32_t>> values;
  for (int64_t i = 0; i < 4096; ++i) {
    values.push_back(std::make_pair(1, 1));
  }
  fill_pairs(frontier_pairs, values);
  set_meta(meta, 0, 1, 1, 2, 1, frontier_pairs);
  ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObInvIdxAggDataEncoder::build(allocator, 64, 16, metas, buf, buf_size));
}

TEST_F(InvIdxAggDataEncoderTest, total_length_reads_fixed_entries_only)
{
  char buf[sizeof(ObInvIdxAggDataHeader) + 2 * sizeof(ObInvIdxAggDataEntry)] = {};
  auto *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);
  header->version_ = ObInvIdxAggDataHeader::VERSION;
  header->logical_block_count_ = 2;
  header->total_size_ = sizeof(buf);
  auto *entries = reinterpret_cast<ObInvIdxAggDataEntry *>(buf + sizeof(*header));
  entries[0].total_doc_length_ = 11;
  entries[1].total_doc_length_ = 19;
  uint64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataDecoder::get_total_doc_length(buf, sizeof(buf), length));
  ASSERT_EQ(30, length);
  entries[0].total_doc_length_ = UINT64_MAX;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObInvIdxAggDataDecoder::get_total_doc_length(buf, sizeof(buf), length));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObInvIdxAggDataDecoder::get_total_doc_length(buf, sizeof(buf) - 1, length));
  header->version_ = ObInvIdxAggDataHeader::VERSION + 1;
  ASSERT_EQ(OB_VERSION_NOT_MATCH, ObInvIdxAggDataDecoder::get_total_doc_length(buf, sizeof(buf), length));
}


TEST_F(InvIdxAggDataEncoderTest, macro_summary_and_table_merge_have_separate_scopes)
{
  common::ObArenaAllocator allocator;
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  const uint64_t lengths[] = {20, 80};
  for (int64_t i = 0; i < ARRAYSIZEOF(lengths); ++i) {
    ObInvIdxStatsMeta stats;
    stats.index_type_ = INV_IDX_TYPE_SEARCH_TEXT;
    stats.has_relevance_params_ = true;
    stats.doc_id_obj_meta_.set_uint64();
    stats.min_doc_id_ = i + 1;
    stats.max_doc_id_ = i + 1;
    stats.total_doc_length_ = lengths[i];
    ObInvMacroMeta macro;
    ASSERT_EQ(OB_SUCCESS, macro.init(stats));
    char buf[256];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, macro.serialize(buf, sizeof(buf), pos));
    ASSERT_EQ(macro.get_serialize_size(), pos);
    ObDataMacroBlockMeta macro_meta;
    macro_meta.val_.row_store_type_ = INV_IDX_ROW_STORE;
    macro_meta.val_.agg_row_buf_ = buf;
    macro_meta.val_.agg_row_len_ = pos;
    ObIndexBlockRowDesc row_desc;
    ObInvMacroMeta decoded_macro;
    int64_t macro_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded_macro.deserialize(
        macro_meta.val_.agg_row_buf_, macro_meta.val_.agg_row_len_, macro_pos));
    ObInvIdxAggInfo summary;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::get_summary_from_macro(decoded_macro, summary));
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(summary, row_desc, allocator));
    ASSERT_EQ(OB_SUCCESS, builder.merge_macro_inv_meta(decoded_macro));
    ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, row_desc.agg_type_);
    ObInvIdxAggInfo decoded;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(row_desc.inverted_index_agg_.buf_,
        row_desc.inverted_index_agg_.size_, read_pos, decoded));
    ASSERT_EQ(row_desc.inverted_index_agg_.size_, read_pos);
    uint64_t length = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
    EXPECT_EQ(lengths[i], length);

    // Extracting this macro's summary must not merge it into the SSTable again.
    ObIndexBlockRowDesc standalone;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::get_summary_from_macro(decoded_macro, summary));
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(summary, standalone, allocator));
    // Neither the output carrier nor its bytes may borrow the macro's wire buffer.
    MEMSET(buf, 0xa5, sizeof(buf));
    read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(standalone.inverted_index_agg_.buf_,
        standalone.inverted_index_agg_.size_, read_pos, decoded));
    ASSERT_EQ(standalone.inverted_index_agg_.size_, read_pos);
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
    EXPECT_EQ(lengths[i], length);
  }
  EXPECT_EQ(100u, builder.get_inv_meta().get_stats().total_doc_length_);
  EXPECT_EQ(1u, builder.get_inv_meta().get_stats().min_doc_id_);
  EXPECT_EQ(2u, builder.get_inv_meta().get_stats().max_doc_id_);
}

TEST_F(InvIdxAggDataEncoderTest, fts_macro_writes_number_summary)
{
  common::ObArenaAllocator allocator;
  ObInvIdxStatsMeta stats;
  stats.index_type_ = INV_IDX_TYPE_FTS;
  stats.has_relevance_params_ = true;
  stats.doc_id_obj_meta_.set_uint64();
  stats.min_doc_id_ = stats.max_doc_id_ = 1;
  stats.total_doc_length_ = 20;
  ObInvMacroMeta macro;
  ASSERT_EQ(OB_SUCCESS, macro.init(stats));
  char buf[256];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro.serialize(buf, sizeof(buf), pos));
  ObDataMacroBlockMeta macro_meta;
  macro_meta.val_.row_store_type_ = INV_IDX_ROW_STORE;
  macro_meta.val_.agg_row_buf_ = buf;
  macro_meta.val_.agg_row_len_ = pos;
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  ObIndexBlockRowDesc row_desc;
  ObInvMacroMeta decoded_macro;
  int64_t macro_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_macro.deserialize(
      macro_meta.val_.agg_row_buf_, macro_meta.val_.agg_row_len_, macro_pos));
  ObInvIdxAggInfo summary;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::get_summary_from_macro(decoded_macro, summary));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(summary, row_desc, allocator));
  ASSERT_EQ(OB_SUCCESS, builder.merge_macro_inv_meta(decoded_macro));
  EXPECT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, row_desc.agg_type_);
  ObInvIdxAggInfo decoded_summary;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(
      row_desc.inverted_index_agg_.buf_, row_desc.inverted_index_agg_.size_, pos, decoded_summary));
  uint64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_summary.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
  EXPECT_EQ(20u, length);
  EXPECT_EQ(20u, builder.get_inv_meta().get_stats().total_doc_length_);

}

TEST_F(InvIdxAggDataEncoderTest, both_index_types_preserve_number_summaries)
{
  common::ObArenaAllocator allocator;
  for (const auto index_type : {INV_IDX_TYPE_FTS, INV_IDX_TYPE_SEARCH_TEXT}) {
    SCOPED_TRACE(static_cast<int>(index_type));
    ObInvIdxAggregator aggregator;
    for (const uint64_t length : {UINT64_MAX, uint64_t(1)}) {
      ObInvIdxStatsMeta stats;
      stats.index_type_ = index_type;
      stats.has_relevance_params_ = true;
      stats.doc_id_obj_meta_.set_uint64();
      stats.min_doc_id_ = stats.max_doc_id_ = 1;
      stats.total_doc_length_ = length;
      ObInvMacroMeta macro;
      ASSERT_EQ(OB_SUCCESS, macro.init(stats));
      ObInvIdxAggInfo summary;
      ObIndexBlockRowDesc row;
      ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::get_summary_from_macro(macro, summary));
      ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(summary, row, allocator));
      row.row_count_ = 1;
      row.is_data_block_ = false;
      ASSERT_EQ(OB_SUCCESS, aggregator.eval(row));
    }
    ObIndexBlockRowDesc parent;
    ASSERT_EQ(OB_SUCCESS, aggregator.fill_row_desc(parent, allocator));
    ObInvIdxAggInfo decoded;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(
        parent.inverted_index_agg_.buf_, parent.inverted_index_agg_.size_, pos, decoded));
    common::number::ObNumber length, expected;
    ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length));
    ASSERT_EQ(OB_SUCCESS, expected.from("18446744073709551616", allocator));
    EXPECT_EQ(0, expected.compare(length));
  }
}

TEST_F(InvIdxAggDataEncoderTest, stats_empty_write_preserves_format_and_failure_state)
{
  ObInvIdxStatsMeta stats;
  stats.index_type_ = INV_IDX_TYPE_SEARCH_TEXT;
  stats.has_relevance_params_ = true;
  stats.has_position_list_ = true;
  stats.doc_id_obj_meta_.set_uint64();
  stats.min_doc_id_ = 1;
  stats.max_doc_id_ = 2;
  stats.total_doc_length_ = 20;
  ObInvIdxStatsMeta meta = stats;
  ASSERT_EQ(OB_SUCCESS, meta.init_empty_for_write(meta));
  EXPECT_TRUE(meta.is_valid());
  EXPECT_TRUE(meta.is_empty_range());
  EXPECT_TRUE(meta.has_position_list_);
  EXPECT_EQ(0u, meta.total_doc_length_);

  stats.has_nested_key_ = true;
  meta = stats;
  EXPECT_EQ(OB_ERR_UNEXPECTED, meta.init_empty_for_write(meta));
  EXPECT_EQ(1u, meta.min_doc_id_);
  EXPECT_EQ(2u, meta.max_doc_id_);
  EXPECT_EQ(20u, meta.total_doc_length_);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

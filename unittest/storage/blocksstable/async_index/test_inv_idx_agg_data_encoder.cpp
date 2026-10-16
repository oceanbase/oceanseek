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

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

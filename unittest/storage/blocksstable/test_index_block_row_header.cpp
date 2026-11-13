// owner: zs475329
// owner group: storage

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

class TestIndexBlockRowHeader : public ::testing::Test
{
protected:
  int build_inverted_row(const ObInvIdxAggInfo &info, ObIndexBlockRowDesc &row_desc)
  {
    return ObInvIdxAggregator::fill_row_desc(info, row_desc, row_allocator_);
  }

  static void decode_inverted_row(const ObIndexBlockRowDesc &row_desc, ObInvIdxAggInfo &info)
  {
    ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, row_desc.agg_type_);
    ASSERT_NE(nullptr, row_desc.inverted_index_agg_.buf_);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(row_desc.inverted_index_agg_.buf_,
        row_desc.inverted_index_agg_.size_, pos, info));
    ASSERT_EQ(row_desc.inverted_index_agg_.size_, pos);
  }

  ObArenaAllocator row_allocator_{ObMemAttr(OB_SERVER_TENANT_ID, "TestIdxRowHdr")};
};

void build_valid_header(ObIndexBlockRowHeader &header)
{
  header.reset();
  header.row_store_type_ = static_cast<uint8_t>(ObRowStoreType::FLAT_ROW_STORE);
  header.compressor_type_ = static_cast<uint8_t>(ObCompressorType::NONE_COMPRESSOR);
  header.is_data_index_ = 1;
  header.is_data_block_ = 1;
  header.is_leaf_block_ = 1;
  header.is_major_node_ = 1;
  header.all_lob_in_row_ = 1;
  header.unset_has_logic_micro_id();
  header.unset_has_shared_data_macro_id();
  header.block_offset_ = 100;
  header.block_size_ = 2000;
  header.row_count_ = 100;
  header.schema_version_ = 1;
  header.macro_block_count_ = 1;
  header.micro_block_count_ = 1;

  MacroBlockId macro_id(0, 1, 2, 3, 4);
  macro_id.set_version_v1();
  ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
}

TEST_F(TestIndexBlockRowHeader, serialize_deserialize_v1)
{
  ObIndexBlockRowHeader header;
  build_valid_header(header);
  ASSERT_TRUE(header.is_valid());
  ASSERT_EQ(static_cast<uint8_t>(ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V1), header.get_version());
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader), header.get_serialize_size());

  char buf[sizeof(ObIndexBlockRowHeader) * 2];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader), pos);

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_TRUE(des_header.is_valid());
  ASSERT_EQ(header.get_macro_id(), des_header.get_macro_id());
  ASSERT_EQ(4, des_header.get_macro_id().fifth_id());
  ASSERT_EQ(header.block_offset_, des_header.block_offset_);
  ASSERT_EQ(header.block_size_, des_header.block_size_);
}

TEST_F(TestIndexBlockRowHeader, set_get_macro_id)
{
  MacroBlockId macro_id(0, 11, 22, 33, 44);
  macro_id.set_version_v1();

  ObIndexBlockRowHeader header;
  ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
  ASSERT_EQ(macro_id, header.get_macro_id());

  header.version_ = 2;
  ASSERT_EQ(OB_ERR_UNEXPECTED, header.set_macro_id(macro_id));
}

TEST_F(TestIndexBlockRowHeader, major_data_index_has_minor_meta)
{
  ObIndexBlockRowHeader header;
  build_valid_header(header);
  ASSERT_TRUE(header.is_major_node());
  ASSERT_TRUE(header.is_data_index());
  ASSERT_TRUE(header.is_minor_meta_info_valid());
}

TEST_F(TestIndexBlockRowHeader, deserialize_invalid_args)
{
  ObIndexBlockRowHeader header;
  build_valid_header(header);
  char buf[sizeof(ObIndexBlockRowHeader)];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(nullptr, sizeof(buf), read_pos));
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, 0, read_pos));
  read_pos = -1;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, sizeof(buf), read_pos));
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, sizeof(buf) - 1, read_pos));

  header.version_ = 2;
  MEMCPY(buf, &header, sizeof(header));
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, sizeof(buf), read_pos));
}

TEST_F(TestIndexBlockRowHeader, serialize_invalid_args)
{
  ObIndexBlockRowHeader header;
  build_valid_header(header);
  char buf[sizeof(ObIndexBlockRowHeader)];
  int64_t pos = 0;
  ASSERT_NE(OB_SUCCESS, header.serialize(nullptr, sizeof(buf), pos));
  ASSERT_NE(OB_SUCCESS, header.serialize(buf, sizeof(buf) - 1, pos));
  pos = -1;
  ASSERT_NE(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));

  header.version_ = 2;
  pos = 0;
  ASSERT_NE(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
}

TEST_F(TestIndexBlockRowHeader, inverted_payload_and_legacy_row)
{
  ObIndexBlockRowHeader header;
  build_valid_header(header);
  header.is_major_node_ = 0;
  header.is_data_block_ = 0;
  header.is_pre_aggregated_ = 0;
  header.is_inverted_index_ = 1;
  ASSERT_EQ(0, header.reserved_);
  // The generic parser only transports opaque bytes; it does not own their format.
  char buf[sizeof(header) + sizeof(ObIndexBlockRowMinorMetaInfo) + 64] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
  pos += sizeof(ObIndexBlockRowMinorMetaInfo);
  const int64_t payload_start = pos;
  const char payload[] = {1, 2, 3, 4};
  MEMCPY(buf + pos, payload, sizeof(payload));
  pos += sizeof(payload);
  ObIndexBlockRowParser parser;
  ASSERT_EQ(OB_SUCCESS, parser.init(buf, pos));
  const char *agg = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, parser.get_agg_row(agg, size));
  ASSERT_EQ(sizeof(payload), size);
  ASSERT_EQ(0, MEMCMP(payload, agg, size));

  // Pre-upgrade inner rows have neither the existing inverted-payload flag nor a payload.
  header.is_inverted_index_ = 0;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
  parser.reset();
  ASSERT_EQ(OB_SUCCESS, parser.init(buf, payload_start));
  const ObIndexBlockRowMinorMetaInfo *minor = nullptr;
  agg = nullptr;
  size = 0;
  ASSERT_EQ(OB_SUCCESS, parser.parse_minor_meta_and_agg_row(minor, agg, size));
  ASSERT_EQ(nullptr, agg);
  ASSERT_EQ(0, size);
}

TEST_F(TestIndexBlockRowHeader, inverted_parent_propagates_decode_error)
{
  ObInvIdxAggInfo stats;
  ObInvIdxAggregator aggregate;
  ObIndexBlockRowDesc child;
  child.row_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 10));
  ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, child));
  ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, child.agg_type_);
  ASSERT_EQ(OB_SUCCESS, aggregate.eval(child));

  // A truncated record must propagate its error without aggregating a stale value.
  char buf[] = {1, 2, 3, 4};
  child.inverted_index_agg_ = {buf, sizeof(buf)};
  ASSERT_EQ(OB_DESERIALIZE_ERROR, aggregate.eval(child));
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 20));
  ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, child));
  ASSERT_EQ(OB_SUCCESS, aggregate.eval(child));
  ObIndexBlockRowDesc parent;
  ASSERT_EQ(OB_SUCCESS, aggregate.fill_row_desc(parent, row_allocator_));
  ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, parent.agg_type_);
  ObInvIdxAggInfo decoded;
  ASSERT_NO_FATAL_FAILURE(decode_inverted_row(parent, decoded));
  uint64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(30, value);
}

TEST_F(TestIndexBlockRowHeader, inverted_payload_does_not_enter_skip_index_aggregation)
{
  ObInvIdxAggInfo stats;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 100));
  ObIndexBlockRowDesc child;
  child.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
  child.set_compressor_type(ObCompressorType::NONE_COMPRESSOR);
  child.set_merge_type(compaction::MINI_MERGE);
  ASSERT_EQ(OB_SUCCESS, child.set_end_scn_by_snapshot_version(10000));
  child.row_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, child));
  ASSERT_TRUE(child.is_valid());

  // Search Text schemas can still contain ordinary skip-index metadata.
  ObBaseIndexBlockBuilder builder;
  ObIndexBlockAggregator &aggregator = builder.index_block_aggregator_;
  aggregator.is_inited_ = true;
  aggregator.need_data_aggregate_ = true;
  ASSERT_EQ(OB_SUCCESS, aggregator.eval(child));
  ASSERT_FALSE(aggregator.need_data_aggregate());
  ObIndexBlockRowDesc parent;
  ASSERT_EQ(OB_SUCCESS, builder.get_aggregated_row(parent));
  ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, parent.agg_type_);
  ASSERT_NO_FATAL_FAILURE(decode_inverted_row(parent, stats));
  uint64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(100, value);
}

TEST_F(TestIndexBlockRowHeader, missing_child_disables_inverted_parent_summary)
{
  ObInvIdxAggInfo stats;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 100));
  for (int64_t missing = 0; missing < 3; ++missing) {
    ObBaseIndexBlockBuilder builder;
    ObIndexBlockAggregator &aggregator = builder.index_block_aggregator_;
    aggregator.is_inited_ = true;
    aggregator.need_data_aggregate_ = true;
    ObIndexBlockRowDesc child;
    child.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
    child.set_compressor_type(ObCompressorType::NONE_COMPRESSOR);
    child.set_merge_type(compaction::MINI_MERGE);
    ASSERT_EQ(OB_SUCCESS, child.set_end_scn_by_snapshot_version(10000));
    child.row_count_ = 1;
    for (int64_t i = 0; i < 3; ++i) {
      if (i == missing) {
        child.agg_type_ = ObIndexRowAggType::UNSERIALIZED_ROW_AGG;
        child.aggregated_row_ = nullptr;
      } else {
        ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, child));
      }
      ASSERT_EQ(OB_SUCCESS, aggregator.eval(child));
    }
    ASSERT_FALSE(aggregator.need_data_aggregate());
    ObIndexBlockRowDesc parent;
    ASSERT_EQ(OB_SUCCESS, builder.get_aggregated_row(parent));
    ASSERT_EQ(3, parent.row_count_);
    ASSERT_EQ(ObIndexRowAggType::UNSERIALIZED_ROW_AGG, parent.agg_type_);
    ASSERT_EQ(nullptr, parent.aggregated_row_);
    ASSERT_EQ(nullptr, aggregator.get_inverted_aggregator());

    // Missing statistics must not suppress a subsequent payload decoding error.
    char truncated[] = {1, 2, 3, 4};
    child.agg_type_ = ObIndexRowAggType::INVERTED_INDEX_AGG;
    child.inverted_index_agg_ = {truncated, sizeof(truncated)};
    ASSERT_EQ(OB_DESERIALIZE_ERROR, aggregator.eval(child));

    aggregator.reuse();
    child.agg_type_ = ObIndexRowAggType::UNSERIALIZED_ROW_AGG;
    child.aggregated_row_ = nullptr;
    ASSERT_EQ(OB_SUCCESS, aggregator.eval(child));
    ASSERT_FALSE(aggregator.inverted_aggregator_.has_rows());
  }
}

TEST_F(TestIndexBlockRowHeader, deferred_row_keeps_allocator_owned_payload)
{
  ObIndexBlockRowDesc row_desc;
  {
    ObInvIdxAggInfo stats;
    ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, row_desc));
    ASSERT_EQ(ObIndexRowAggType::UNSERIALIZED_ROW_AGG, row_desc.agg_type_);
    ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 123));
    ASSERT_EQ(OB_SUCCESS, build_inverted_row(stats, row_desc));
    ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 999));
    stats.reset();
  }
  ObIndexBlockRowDesc deferred(row_desc);
  // RowDesc copies borrow the same serialized bytes. The caller's allocator stays
  // alive through the deferred append; neither source values nor RowDesc own them.
  ASSERT_EQ(row_desc.inverted_index_agg_.buf_, deferred.inverted_index_agg_.buf_);
  row_desc.inverted_index_agg_ = {nullptr, 0};
  ObStaticDataStoreDesc static_desc;
  static_desc.merge_type_ = compaction::MINI_MERGE;
  ObDataStoreDesc data_desc;
  data_desc.static_desc_ = &static_desc;
  ObIndexBlockRowBuilder builder;
  builder.data_desc_ = &data_desc;
  char buffer[128] = {};
  builder.data_buf_ = buffer;
  builder.write_pos_ = 7;
  ObIndexBlockRowHeader header;
  ObAggRowWriter agg_writer;
  build_valid_header(header);
  uint64_t value = 0;
  ObInvIdxAggInfo decoded;
  ASSERT_NO_FATAL_FAILURE(decode_inverted_row(deferred, decoded));
  ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(123, value);
  ASSERT_EQ(OB_SUCCESS, builder.append_aggregate_data(header, deferred, sizeof(buffer), agg_writer));
  ASSERT_EQ(24, builder.write_pos_); // 7-byte prefix + 1-byte version + 8-byte mask + 8-byte Number.
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(0, buffer[i]);
  }
  ObInvIdxAggInfo restored;
  int64_t read_pos = 7;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggInfoSerializer::deserialize(buffer, builder.write_pos_, read_pos, restored));
  ASSERT_EQ(builder.write_pos_, read_pos);
  ASSERT_EQ(OB_SUCCESS, restored.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, value));
  ASSERT_EQ(123, value);
  for (int64_t i = builder.write_pos_; i < sizeof(buffer); ++i) {
    ASSERT_EQ(0, buffer[i]);
  }
  deferred.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
  deferred.set_compressor_type(ObCompressorType::NONE_COMPRESSOR);
  deferred.set_merge_type(compaction::MINI_MERGE);
  ASSERT_EQ(OB_SUCCESS, deferred.set_end_scn_by_snapshot_version(10000));
  deferred.row_count_ = 1;
  ASSERT_TRUE(deferred.is_valid());
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, builder.calc_data_size(deferred, agg_writer, size));
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader) + sizeof(ObIndexBlockRowMinorMetaInfo) + 17, size);
  builder.data_buf_ = nullptr;
}

TEST_F(TestIndexBlockRowHeader, builder_encoded_summary_survives_input_reuse)
{
  ObArenaAllocator input_allocator(ObMemAttr(OB_SERVER_TENANT_ID, "TestIdxRowIn"));
  ObInvIdxAggInfo stats;
  ObIndexBlockRowDesc child;
  child.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
  child.set_compressor_type(ObCompressorType::NONE_COMPRESSOR);
  child.set_merge_type(compaction::MINI_MERGE);
  ASSERT_EQ(OB_SUCCESS, child.set_end_scn_by_snapshot_version(10000));
  child.row_count_ = 1;

  ObBaseIndexBlockBuilder builder;
  ObIndexBlockAggregator &aggregator = builder.index_block_aggregator_;
  aggregator.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, UINT64_MAX));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(stats, child, input_allocator));
  ASSERT_EQ(OB_SUCCESS, aggregator.eval(child));
  ASSERT_EQ(OB_SUCCESS, stats.set(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, 1));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggregator::fill_row_desc(stats, child, input_allocator));
  ASSERT_EQ(OB_SUCCESS, aggregator.eval(child));

  ObIndexBlockRowDesc restored;
  ASSERT_EQ(OB_SUCCESS, builder.get_aggregated_row(restored));
  aggregator.reuse();
  stats.reset();
  input_allocator.reset();

  ObArenaAllocator output_allocator(ObMemAttr(OB_SERVER_TENANT_ID, "TestIdxRowOut"));
  ASSERT_EQ(2, restored.row_count_);
  ASSERT_EQ(ObIndexRowAggType::INVERTED_INDEX_AGG, restored.agg_type_);
  ObInvIdxAggInfo decoded;
  ASSERT_NO_FATAL_FAILURE(decode_inverted_row(restored, decoded));
  common::number::ObNumber expected;
  common::number::ObNumber actual;
  ASSERT_EQ(OB_SUCCESS, expected.from("18446744073709551616", output_allocator));
  ASSERT_EQ(OB_SUCCESS, decoded.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, actual));
  ASSERT_EQ(0, actual.compare(expected));

  // Reuse must not export the previous parent's statistics or a fabricated zero summary.
  ObIndexBlockRowDesc empty_row;
  ASSERT_EQ(OB_SUCCESS, builder.get_aggregated_row(empty_row));
  ASSERT_EQ(ObIndexRowAggType::UNSERIALIZED_ROW_AGG, empty_row.agg_type_);
  ASSERT_EQ(nullptr, empty_row.aggregated_row_);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_block_row_header.log*");
  OB_LOGGER.set_file_name("test_index_block_row_header.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

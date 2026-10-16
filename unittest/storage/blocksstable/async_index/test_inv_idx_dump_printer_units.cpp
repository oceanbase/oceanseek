/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define private public
#define protected public
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_dumper.h"
#undef private
#undef protected

#include <gtest/gtest.h>
#include <string>
#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "share/ob_fts_pos_list_codec.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

namespace
{
struct CapturedOutput
{
  CapturedOutput() : file_(nullptr), buf_(nullptr), size_(0) {}
  ~CapturedOutput()
  {
    if (nullptr != file_) {
      fclose(file_);
      file_ = nullptr;
    }
    if (nullptr != buf_) {
      free(buf_);
      buf_ = nullptr;
    }
  }
  FILE *open()
  {
    file_ = open_memstream(&buf_, &size_);
    return file_;
  }
  std::string str()
  {
    if (nullptr != file_) {
      fflush(file_);
    }
    return nullptr == buf_ ? std::string() : std::string(buf_, size_);
  }
  FILE *file_;
  char *buf_;
  size_t size_;
};

int write_u32(char *buf, const int64_t buf_len, int64_t &pos, const uint32_t value)
{
  int ret = OB_SUCCESS;
  if (pos + static_cast<int64_t>(sizeof(value)) > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf + pos, &value, sizeof(value));
    pos += sizeof(value);
  }
  return ret;
}
} // namespace

class TestInvIdxDumpPrinterUnits : public ::testing::Test
{
public:
protected:
  void build_payload(
      ObIAllocator &allocator,
      const char * const *blocks,
      const int64_t *block_sizes,
      const int64_t block_count,
      char *&payload,
      int64_t &payload_size)
  {
    ASSERT_GT(block_count, 0);
    int64_t blocks_size = 0;
    for (int64_t i = 0; i < block_count; ++i) {
      ASSERT_NE(nullptr, blocks[i]);
      ASSERT_GT(block_sizes[i], 0);
      blocks_size += block_sizes[i];
    }

    const int64_t header_size = sizeof(ObInvIdxMicroBlockPayloadHeader);
    const int64_t offset_table_size = (block_count + 1) * sizeof(uint32_t);
    payload_size = header_size + blocks_size + offset_table_size;
    payload = static_cast<char *>(allocator.alloc(payload_size));
    ASSERT_NE(nullptr, payload);
    MEMSET(payload, 0, payload_size);

    ObInvIdxMicroBlockPayloadHeader *header =
        reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(payload);
    header->version_ = ObInvIdxMicroBlockPayloadHeader::VERSION;
    header->block_count_ = static_cast<uint16_t>(block_count);
    header->blocks_offset_ = static_cast<uint32_t>(header_size);
    header->block_offsets_offset_ = static_cast<uint32_t>(header_size + blocks_size);
    header->total_size_ = static_cast<uint32_t>(payload_size);

    int64_t block_pos = header_size;
    int64_t offset_pos = header->block_offsets_offset_;
    uint32_t relative = 0;
    ASSERT_EQ(OB_SUCCESS, write_u32(payload, payload_size, offset_pos, relative));
    for (int64_t i = 0; i < block_count; ++i) {
      MEMCPY(payload + block_pos, blocks[i], block_sizes[i]);
      block_pos += block_sizes[i];
      relative += static_cast<uint32_t>(block_sizes[i]);
      ASSERT_EQ(OB_SUCCESS, write_u32(payload, payload_size, offset_pos, relative));
    }
  }

  void build_micro_block(
      ObIAllocator &allocator,
      const char *payload,
      const int64_t payload_size,
      const int64_t rowkey_column_count,
      char *&buf,
      int64_t &size)
  {
    ASSERT_NE(nullptr, payload);
    ASSERT_GT(payload_size, 0);
    ObMicroBlockHeader header;
    const int64_t column_count = rowkey_column_count > 1 ? rowkey_column_count : 1;
    ASSERT_EQ(OB_SUCCESS, header.init(
        1 /*data_version*/, column_count, rowkey_column_count,
        INV_IDX_ROW_STORE, false /*has_column_checksum*/));
    header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
    header.data_length_ = static_cast<int32_t>(payload_size);
    header.data_zlength_ = static_cast<int32_t>(payload_size);
    header.original_length_ = static_cast<int32_t>(payload_size);
    header.data_checksum_ = ob_crc64_sse42(0, payload, payload_size);
    header.set_header_checksum();

    size = static_cast<int64_t>(header.header_size_) + payload_size;
    buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, header.serialize(buf, size, pos));
    ASSERT_EQ(header.header_size_, pos);
    MEMCPY(buf + pos, payload, payload_size);
  }

  void build_encoded_position_list(
      ObIAllocator &allocator,
      char *&buf,
      int64_t &size)
  {
    common::ObSEArray<int64_t, 2> positions;
    ASSERT_EQ(OB_SUCCESS, positions.push_back(10));
    ASSERT_EQ(OB_SUCCESS, positions.push_back(13));

    share::ObFTSPositionListStore pos_list_store;
    ASSERT_EQ(OB_SUCCESS, pos_list_store.encode_and_serialize(allocator, positions, buf, size));
    ASSERT_NE(nullptr, buf);
    ASSERT_GT(size, 0);
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestInvIdxDumpPrinterUnits, standard_store_row_printer_preserves_long_composite_rowkey)
{
  static const char LONG_TERM_TAIL[] = "long-term-tail-marker";
  const int64_t long_term_tail_len = ARRAYSIZEOF(LONG_TERM_TAIL) - 1;
  std::string long_term(1024, 'x');
  long_term.replace(long_term.size() - long_term_tail_len,
      long_term_tail_len, LONG_TERM_TAIL);

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 6));
  row.storage_datums_[0].set_string(
      ObString(static_cast<int32_t>(long_term.size()), &long_term[0]));
  row.storage_datums_[1].set_string(ObString::make_string("json-path-component"));
  row.storage_datums_[2].set_uint(12345);
  row.storage_datums_[3].set_int(-100);
  row.storage_datums_[4].set_int(0);
  row.storage_datums_[5].set_string(ObString::make_string("opaque-index-payload"));

  ObObjMeta rowkey_types[5];
  rowkey_types[0].set_varchar();
  rowkey_types[1].set_varchar();
  rowkey_types[2].set_uint64();
  rowkey_types[3].set_int();
  rowkey_types[4].set_int();

  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());
  dumper.printer_.print_row_title(&row, 0);
  dumper.printer_.print_store_row(&row, rowkey_types, ARRAYSIZEOF(rowkey_types),
      64 /*print_hex_length*/, true /*is_index_block*/, false /*is_trans_sstable*/);

  const std::string dump_output = output.str();
  ASSERT_NE(std::string::npos, dump_output.find(LONG_TERM_TAIL));
  ASSERT_NE(std::string::npos, dump_output.find("json-path-component"));
}

TEST_F(TestInvIdxDumpPrinterUnits, rowkey_max_doc_id_uses_fixed_position_before_mv_tail)
{
  // Layout: (term..., max_doc_id, trans_version, sql_sequence). The numeric
  // second term and trailing decoy make fallback-by-type select the wrong datum.
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 5));
  row.storage_datums_[0].set_string(ObString::make_string("term"));
  row.storage_datums_[1].set_uint(777);
  row.storage_datums_[2].set_uint(12345);
  row.storage_datums_[3].set_int(-100);
  row.storage_datums_[4].set_int(0);

  ObInvIdxSSTableDataBlockDumper dumper;
  uint64_t max_doc_id = 0;
  ASSERT_TRUE(dumper.extract_rowkey_max_doc_id_(row, 5, max_doc_id));
  ASSERT_EQ(12345, max_doc_id);

  row.storage_datums_[2].set_null();
  row.storage_datums_[4].set_uint(67890);
  ASSERT_FALSE(dumper.extract_rowkey_max_doc_id_(row, 5, max_doc_id));
  ASSERT_EQ(0, max_doc_id);
}

TEST_F(TestInvIdxDumpPrinterUnits, compressed_micro_block_uses_macro_compressor)
{
  char payload[4096];
  MEMSET(payload, 'x', sizeof(payload));
  ObCompressor *compressor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(LZ4_COMPRESSOR, compressor));
  ASSERT_NE(nullptr, compressor);
  int64_t max_overflow_size = 0;
  ASSERT_EQ(OB_SUCCESS, compressor->get_max_overflow_size(sizeof(payload), max_overflow_size));
  char *compressed = static_cast<char *>(allocator_.alloc(sizeof(payload) + max_overflow_size));
  ASSERT_NE(nullptr, compressed);
  int64_t compressed_size = 0;
  ASSERT_EQ(OB_SUCCESS, compressor->compress(
      payload, sizeof(payload), compressed, sizeof(payload) + max_overflow_size, compressed_size));
  ASSERT_GT(compressed_size, 0);
  ASSERT_LT(compressed_size, static_cast<int64_t>(sizeof(payload)));

  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, header.init(
      1 /*data_version*/, 1 /*column_count*/, 0 /*rowkey_column_count*/,
      INV_IDX_ROW_STORE, false /*has_column_checksum*/));
  header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
  header.data_length_ = static_cast<int32_t>(sizeof(payload));
  header.data_zlength_ = static_cast<int32_t>(compressed_size);
  header.original_length_ = static_cast<int32_t>(sizeof(payload));
  header.data_checksum_ = ob_crc64_sse42(0, compressed, compressed_size);
  header.set_header_checksum();

  const int64_t block_size = header.header_size_ + compressed_size;
  char *block_buf = static_cast<char *>(allocator_.alloc(block_size));
  ASSERT_NE(nullptr, block_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(block_buf, block_size, pos));
  MEMCPY(block_buf + pos, compressed, compressed_size);

  ObSSTableMacroBlockHeader macro_header;
  macro_header.fixed_header_.compressor_type_ = LZ4_COMPRESSOR;
  ObInvIdxSSTableDataBlockDumper dumper;
  dumper.macro_header_ = &macro_header;
  const char *materialized = nullptr;
  int64_t materialized_size = 0;
  bool checksum_ok = false;
  ASSERT_EQ(OB_SUCCESS, dumper.materialize_micro_block_(
      header, block_buf, block_size, materialized, materialized_size, checksum_ok));
  ASSERT_TRUE(checksum_ok);
  ASSERT_EQ(sizeof(payload), materialized_size);
  ASSERT_EQ(0, MEMCMP(payload, materialized, sizeof(payload)));
}

TEST_F(TestInvIdxDumpPrinterUnits, checksum_mismatch_stops_materialization)
{
  const char payload[] = "inverted-index-payload";
  char *block_buf = nullptr;
  int64_t block_size = 0;
  build_micro_block(allocator_, payload, sizeof(payload), 0, block_buf, block_size);
  ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader *>(block_buf);
  block_buf[header->header_size_] ^= 0x01;

  ObInvIdxSSTableDataBlockDumper dumper;
  const char *materialized = nullptr;
  int64_t materialized_size = 0;
  bool checksum_ok = false;
  ASSERT_EQ(OB_PHYSIC_CHECKSUM_ERROR, dumper.materialize_micro_block_(
      *header, block_buf, block_size, materialized, materialized_size, checksum_ok));
  ASSERT_FALSE(checksum_ok);
  ASSERT_EQ(nullptr, materialized);
  ASSERT_EQ(0, materialized_size);
  const ObInvIdxDumpValidationResult &result = dumper.get_validation_result();
  ASSERT_EQ(1, result.checks_[CHECK_MICRO_BLOCK_CHECKSUM].failure_count_);
  ASSERT_EQ(1, result.checks_[CHECK_MICRO_BLOCK_MATERIALIZE].failure_count_);
}

/*
TEST_F(TestInvIdxDumpPrinterUnits, posting_decoder_rejects_unsupported_encoding)
{
  char block[128];
  MEMSET(block, 0, sizeof(block));
  ObPostingBlockHeader *header = reinterpret_cast<ObPostingBlockHeader *>(block);
  header->version_ = ObPostingBlockHeader::POSTING_BLOCK_HEADER_VERSION;
  header->is_uint_doc_id_ = 1;
  header->has_relevance_params_ = 0;
  header->row_count_ = 1;

  ObPostingColumnMeta *meta =
      reinterpret_cast<ObPostingColumnMeta *>(block + sizeof(ObPostingBlockHeader));
  meta->encoding_type_ = ObPostingColEncodingType::PFOR_DELTA;
  meta->packed_width_ = 8;
  meta->offset_ = static_cast<uint16_t>(header->get_header_size());
  meta->base_value_ = 100;
  uint64_t delta = 1;
  MEMCPY(block + meta->offset_, &delta, sizeof(delta));

  const char *blocks[] = {block};
  const int64_t sizes[] = {meta->offset_ + static_cast<int64_t>(sizeof(delta))};
  char *payload = nullptr;
  int64_t payload_size = 0;
  build_payload(allocator_, blocks, sizes, 1, payload, payload_size);
  char *micro = nullptr;
  int64_t micro_size = 0;
  build_micro_block(allocator_, payload, payload_size, 0, micro, micro_size);
  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(micro);
  ObInvIdxDumpMicroBlockInfo info;
  info.offset_ = 0;
  info.size_ = micro_size;
  info.buf_ = micro;
  info.is_posting_value_ = false;
  info.payload_buf_ = micro + micro_header->header_size_;
  info.payload_size_ = micro_header->data_zlength_;
  info.checksum_ok_ = true;
  info.logical_block_count_ = 1;

  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput output;
  dumper.hex_length_ = 64;
  dumper.printer_.set_fd(output.open());
  ASSERT_EQ(OB_NOT_SUPPORTED, dumper.dump_posting_data_micro_block_(info, 0));
  const std::string dump_output = output.str();
  ASSERT_EQ(std::string::npos, dump_output.find("raw_hex="));
  const ObInvIdxDumpValidationResult &result = dumper.get_validation_result();
  ASSERT_GT(result.checks_[CHECK_POSTING_DECODE].failure_count_, 0);
}
*/

TEST_F(TestInvIdxDumpPrinterUnits, position_list_format_success_and_fallbacks)
{
  char *position_list = nullptr;
  int64_t position_list_size = 0;
  build_encoded_position_list(allocator_, position_list, position_list_size);

  ObDatum value;
  value.ptr_ = position_list;
  value.len_ = static_cast<uint32_t>(position_list_size);
  value.pack_ = value.len_;

  ObInvIdxSSTableDataBlockDumper dumper;
  char decoded[128];
  ASSERT_TRUE(dumper.format_position_list_array_(value, 64, decoded, sizeof(decoded)));
  ASSERT_STREQ("[10,13]", decoded);

  const char bad_position_list[] = "pos";
  ObDatum bad_value;
  bad_value.ptr_ = bad_position_list;
  bad_value.len_ = static_cast<uint32_t>(sizeof(bad_position_list) - 1);
  bad_value.pack_ = bad_value.len_;
  char fallback[128];
  ASSERT_FALSE(dumper.format_position_list_array_(bad_value, 64, fallback, sizeof(fallback)));
  ASSERT_NE(std::string::npos, std::string(fallback).find("decode_error"));
  ASSERT_NE(std::string::npos, std::string(fallback).find("raw_len=3"));
  ASSERT_NE(std::string::npos, std::string(fallback).find("fallback_hex=706F73"));

  ObDatum empty_value;
  empty_value.ptr_ = bad_position_list;
  empty_value.len_ = 0;
  empty_value.pack_ = 0;
  char missing[128];
  ASSERT_FALSE(dumper.format_position_list_array_(empty_value, 64, missing, sizeof(missing)));
  ASSERT_NE(std::string::npos, std::string(missing).find("missing_position_list"));
}

TEST_F(TestInvIdxDumpPrinterUnits, posting_value_row_index_validation)
{
  char block[128];
  MEMSET(block, 0, sizeof(block));
  ObPostingValueBlockHeader *header = reinterpret_cast<ObPostingValueBlockHeader *>(block);
  header->version_ = ObPostingValueBlockHeader::VALUE_BLOCK_HEADER_VERSION;
  header->has_position_list_ = 1;
  header->column_cnt_ = 1;
  header->row_idx_byte_width_ = 1;
  header->row_count_ = 2;
  header->row_idx_offset_ = sizeof(ObPostingValueBlockHeader) + 8;

  // Row area is eight bytes. The index [0, 3] is valid and leaves row1 covering [3, 8).
  block[header->row_idx_offset_] = 0;
  block[header->row_idx_offset_ + 1] = 3;
  ASSERT_TRUE(header->is_valid());

  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput ok_output;
  dumper.printer_.set_fd(ok_output.open());
  ASSERT_EQ(OB_SUCCESS, dumper.dump_posting_value_row_index_(
      header, block, header->row_idx_offset_ + header->row_count_ * header->row_idx_byte_width_));
  ASSERT_TRUE(dumper.get_validation_result().is_ok(
      CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE));
  ASSERT_EQ(1, dumper.get_validation_result()
      .checks_[CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE].checked_count_);

  // A non-monotonic row index must be reported by the dumper's row-index
  // WARN/ERROR branch in design section 4.4.
  ObInvIdxSSTableDataBlockDumper bad_dumper;
  CapturedOutput bad_output;
  bad_dumper.printer_.set_fd(bad_output.open());
  block[header->row_idx_offset_] = 5;
  block[header->row_idx_offset_ + 1] = 3;
  ASSERT_EQ(OB_INVALID_DATA, bad_dumper.dump_posting_value_row_index_(
      header, block, header->row_idx_offset_ + header->row_count_ * header->row_idx_byte_width_));
  ASSERT_GT(bad_dumper.get_validation_result()
      .checks_[CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE].failure_count_, 0);
}

TEST_F(TestInvIdxDumpPrinterUnits, agg_data_layout_validation_for_frontier_pool)
{
  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());

  char agg[sizeof(ObInvIdxAggDataHeader) + sizeof(ObInvIdxAggDataEntry) + 1];
  MEMSET(agg, 0, sizeof(agg));
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg);
  header->version_ = ObInvIdxAggDataHeader::VERSION;
  header->logical_block_count_ = 1;
  header->total_size_ = sizeof(agg);
  ObInvIdxAggDataEntry *entry =
      reinterpret_cast<ObInvIdxAggDataEntry *>(agg + sizeof(ObInvIdxAggDataHeader));
  entry->block_idx_ = 0;
  entry->frontier_count_ = 1;
  entry->frontier_offset_ = 0;
  entry->doc_count_ = 1;
  entry->min_doc_id_ = 1;
  entry->max_doc_id_ = 1;

  int64_t frontier_pool_size = 0;
  ASSERT_EQ(OB_INVALID_DATA,
      dumper.dump_agg_data_header_(header, sizeof(agg), frontier_pool_size));
  ASSERT_EQ(1, frontier_pool_size);

  header->version_ = 0xff;
  ASSERT_EQ(OB_VERSION_NOT_MATCH,
      dumper.dump_agg_data_header_(header, sizeof(agg), frontier_pool_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      dumper.dump_agg_data_header_(nullptr, sizeof(agg), frontier_pool_size));
}

TEST_F(TestInvIdxDumpPrinterUnits, agg_entry_monotonic_validation)
{
  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());

  ObInvIdxAggDataEntry entries[2];
  MEMSET(entries, 0, sizeof(entries));
  entries[0].block_idx_ = 1;
  entries[0].max_doc_id_ = 10;
  entries[1].block_idx_ = 1;
  entries[1].max_doc_id_ = 9;

  ASSERT_EQ(OB_INVALID_DATA, dumper.dump_agg_data_entries_(entries, 2));
  const ObInvIdxDumpValidationResult &result = dumper.get_validation_result();
  ASSERT_GT(result.checks_[CHECK_AGG_MAX_DOC_ID_MONOTONIC].failure_count_, 0);
  ASSERT_GT(result.checks_[CHECK_AGG_BLOCK_IDX_MONOTONIC].failure_count_, 0);
  ASSERT_EQ(OB_INVALID_DATA,
      result.checks_[CHECK_AGG_MAX_DOC_ID_MONOTONIC].first_ret_);
  ASSERT_EQ(OB_INVALID_DATA,
      result.checks_[CHECK_AGG_BLOCK_IDX_MONOTONIC].first_ret_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, dumper.dump_agg_data_entries_(nullptr, 1));
}

TEST_F(TestInvIdxDumpPrinterUnits, agg_frontier_error_code_propagation)
{
  ObInvIdxSSTableDataBlockDumper dumper;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());

  ObInvIdxAggDataEntry entry;
  MEMSET(&entry, 0, sizeof(entry));
  ObInvIdxBM25ParamPair pair;
  MEMSET(&pair, 0, sizeof(pair));

  ASSERT_EQ(OB_SUCCESS,
      dumper.dump_agg_data_frontier_pairs_(&entry, 1, nullptr, 0));

  dumper.validation_result_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      dumper.dump_agg_data_frontier_pairs_(nullptr, 1, &pair, sizeof(pair)));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      dumper.get_validation_result().checks_[CHECK_FRONTIER_BOUNDS].first_ret_);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      dumper.get_validation_result().checks_[CHECK_FRONTIER_POOL_COVERAGE].first_ret_);

  dumper.validation_result_.reset();
  ASSERT_EQ(OB_INVALID_DATA,
      dumper.dump_agg_data_frontier_pairs_(&entry, 1, &pair, sizeof(pair)));
  ASSERT_EQ(OB_INVALID_DATA,
      dumper.get_validation_result().checks_[CHECK_FRONTIER_POOL_COVERAGE].first_ret_);
}

TEST_F(TestInvIdxDumpPrinterUnits, parse_status_preserves_block_level_failure)
{
  ObSSTableMacroBlockHeader macro_header;
  ObInvIdxSSTableDataBlockDumper dumper;
  dumper.macro_header_ = &macro_header;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());
  dumper.record_index_parse_failure_(OB_ERR_UNEXPECTED);

  ASSERT_EQ(OB_INVALID_DATA,
      dumper.dump_consistency_summary_(-1 /*macro_meta_row_count*/, false /*macro_meta_locator_ok*/));

  const ObInvIdxDumpCheckStat &stat =
      dumper.get_validation_result().checks_[CHECK_PARSE_STATUS];
  ASSERT_EQ(1, stat.checked_count_);
  ASSERT_EQ(1, stat.failure_count_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, stat.first_ret_);
}

TEST_F(TestInvIdxDumpPrinterUnits, parse_status_excludes_unattempted_rows)
{
  ObSSTableMacroBlockHeader macro_header;
  ObInvIdxSSTableDataBlockDumper dumper;
  dumper.macro_header_ = &macro_header;
  CapturedOutput output;
  dumper.printer_.set_fd(output.open());
  dumper.index_block_row_count_ = 3;
  dumper.index_parse_attempted_count_ = 1;
  dumper.record_index_parse_failure_(OB_ALLOCATE_MEMORY_FAILED);

  ASSERT_EQ(OB_INVALID_DATA,
      dumper.dump_consistency_summary_(-1 /*macro_meta_row_count*/, false /*macro_meta_locator_ok*/));

  const ObInvIdxDumpCheckStat &stat =
      dumper.get_validation_result().checks_[CHECK_PARSE_STATUS];
  ASSERT_EQ(1, stat.checked_count_);
  ASSERT_EQ(1, stat.failure_count_);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, stat.first_ret_);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

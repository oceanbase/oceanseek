/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

class InvIdxDataMicroBlockReaderTest : public ::testing::Test
{
protected:
  InvIdxDataMicroBlockReaderTest() {}
  virtual ~InvIdxDataMicroBlockReaderTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  // Build a full data micro block [ObMicroBlockHeader][payload] with the
  // given logical blocks and return a writable deep copy owned by the arena so
  // tests can corrupt it in place.
  void build_micro_block(
      common::ObIAllocator &allocator,
      const char * const *blocks,
      const int64_t *block_sizes,
      const int64_t block_count,
      char *&buf,
      int64_t &size)
  {
    ObInvIdxMicroBlockWriter writer;
    int64_t block_idx = -1;
    ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
    for (int64_t i = 0; i < block_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(blocks[i], block_sizes[i], block_idx));
    }
    const char *payload = nullptr;
    int64_t payload_size = 0;
    ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(payload, payload_size));
    ASSERT_NE(nullptr, payload);
    ASSERT_GT(payload_size, 0);
    ObMicroBlockHeader header;
    int64_t header_pos = 0;
    ASSERT_EQ(OB_SUCCESS, header.init(
        1 /*data_version*/, 1 /*column_count*/, 0 /*rowkey_column_count*/,
        common::INV_IDX_ROW_STORE, false /*has_column_checksum*/));
    header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
    header.data_length_ = static_cast<int32_t>(payload_size);
    header.data_zlength_ = static_cast<int32_t>(payload_size);
    header.original_length_ = static_cast<int32_t>(payload_size);
    header.data_checksum_ = ob_crc64_sse42(0, payload, payload_size);
    header.set_header_checksum();
    size = static_cast<int64_t>(header.header_size_) + payload_size;
    // Deep copy into arena-owned memory so the test can mutate bytes freely.
    char *mutable_buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, mutable_buf);
    ASSERT_EQ(OB_SUCCESS, header.serialize(mutable_buf, size, header_pos));
    ASSERT_EQ(header.header_size_, header_pos);
    MEMCPY(mutable_buf + header_pos, payload, payload_size);
    buf = mutable_buf;
  }

  void build_flat_cache_micro_block(
      common::ObIAllocator &allocator,
      char *&buf,
      int64_t &size)
  {
    static const uint16_t BLOCK_COUNT = 3;
    static const uint32_t DOC_COUNT = 6;
    ObMicroBlockHeader header;
    ASSERT_EQ(OB_SUCCESS, header.init(
        1 /*data_version*/, 1 /*column_count*/, 0 /*rowkey_column_count*/,
        common::INV_IDX_ROW_STORE, false /*has_column_checksum*/));
    header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
    const int64_t payload_size = sizeof(ObFlatCacheHeader) + BLOCK_COUNT * sizeof(ObFlatCacheBlockDesc) + DOC_COUNT * sizeof(uint64_t);
    header.data_length_ = static_cast<int32_t>(payload_size);
    header.data_zlength_ = static_cast<int32_t>(payload_size);
    header.original_length_ = static_cast<int32_t>(payload_size);
    header.row_count_ = DOC_COUNT;
    size = static_cast<int64_t>(header.header_size_) + payload_size;
    char *mutable_buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, mutable_buf);
    char *payload = mutable_buf + header.header_size_;
    ObFlatCacheHeader *flat_cache_header = new (payload) ObFlatCacheHeader();
    flat_cache_header->block_count_ = BLOCK_COUNT;
    ObFlatCacheBlockDesc *descs = reinterpret_cast<ObFlatCacheBlockDesc *>(payload + sizeof(ObFlatCacheHeader));
    new (&descs[0]) ObFlatCacheBlockDesc();
    new (&descs[1]) ObFlatCacheBlockDesc();
    new (&descs[2]) ObFlatCacheBlockDesc();
    descs[0].doc_offset_ = 0;
    descs[0].doc_count_ = 1;
    descs[1].doc_offset_ = 1;
    descs[1].doc_count_ = 2;
    descs[2].doc_offset_ = 3;
    descs[2].doc_count_ = 3;
    uint64_t *doc_ids = reinterpret_cast<uint64_t *>(payload + sizeof(ObFlatCacheHeader) + BLOCK_COUNT * sizeof(ObFlatCacheBlockDesc));
    doc_ids[0] = 7;
    doc_ids[1] = 11;
    doc_ids[2] = 13;
    doc_ids[3] = 17;
    doc_ids[4] = 19;
    doc_ids[5] = 23;
    header.data_checksum_ = ob_crc64_sse42(0, payload, payload_size);
    header.set_header_checksum();
    int64_t header_pos = 0;
    ASSERT_EQ(OB_SUCCESS, header.serialize(mutable_buf, size, header_pos));
    ASSERT_EQ(header.header_size_, header_pos);
    buf = mutable_buf;
  }

  // Write a little-endian uint32 at @off (relative to buf).
  static void put_u32(char *buf, const int64_t off, const uint32_t v)
  {
    MEMCPY(buf + off, &v, sizeof(uint32_t));
  }
  static uint32_t get_u32(const char *buf, const int64_t off)
  {
    uint32_t v = 0;
    MEMCPY(&v, buf + off, sizeof(uint32_t));
    return v;
  }
};

// Null payload / payload too small / negative block idx -> OB_INVALID_ARGUMENT.
TEST_F(InvIdxDataMicroBlockReaderTest, test_locate_block_invalid_args)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  const char b0[] = "AAA";
  const char b1[] = "BBBBB";
  const char *blocks[] = {b0, b1};
  const int64_t sizes[] = {3, 5};
  char *buf = nullptr;
  int64_t size = 0;
  build_micro_block(allocator, blocks, sizes, 2, buf, size);

  const char *block_buf = nullptr;
  int64_t block_size = 0;
  const int64_t header_size = ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET;
  ObInvIdxMicroBlockReader reader;

  // null micro block
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.init(nullptr, size, false /*is_posting_value*/));
  // micro block smaller than the header
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.init(buf, header_size - 1, false /*is_posting_value*/));
  // invalid expected micro block kind
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      reader.init(buf, size, true /*is_posting_value*/));
  // negative block idx
  ASSERT_EQ(OB_SUCCESS, reader.init(buf, size, false /*is_posting_value*/));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.get_payload(-1, block_buf, block_size));
}

// Corrupted common header / payload layout -> OB_ERR_UNEXPECTED.
TEST_F(InvIdxDataMicroBlockReaderTest, test_locate_block_bad_header)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  const char b0[] = "AAA";
  const char *blocks[] = {b0};
  const int64_t sizes[] = {3};
  char *buf = nullptr;
  int64_t size = 0;
  build_micro_block(allocator, blocks, sizes, 1, buf, size);

  ObInvIdxMicroBlockReader reader;

  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(buf);

  // bad common header version
  const int16_t saved_version = micro_header->version_;
  micro_header->version_ = static_cast<int16_t>(0xFF);
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader.init(buf, size, false /*is_posting_value*/));
  micro_header->version_ = saved_version;

  // payload range larger than the provided micro block size
  const int32_t saved_data_zlength = micro_header->data_zlength_;
  micro_header->data_zlength_ = static_cast<int32_t>(size);
  micro_header->set_header_checksum();
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader.init(buf, size, false /*is_posting_value*/));
  micro_header->data_zlength_ = saved_data_zlength;
  micro_header->set_header_checksum();

  // bad payload header version (physical header remains valid)
  const int64_t payload_offset = micro_header->header_size_;
  ObInvIdxMicroBlockPayloadHeader *payload_header =
      reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(buf + payload_offset);
  const uint8_t saved_payload_version = payload_header->version_;
  payload_header->version_ = static_cast<uint8_t>(0xFF);
  micro_header->data_checksum_ = ob_crc64_sse42(
      0, buf + micro_header->header_size_, static_cast<int64_t>(micro_header->data_zlength_));
  micro_header->set_header_checksum();
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader.init(buf, size, false /*is_posting_value*/));
  payload_header->version_ = saved_payload_version;

  // The offset table implied by block_count_ must fit in the payload. Keep
  // this reader-specific overflow boundary here instead of in dumper tests.
  payload_header->block_count_ = UINT16_MAX;
  payload_header->block_offsets_offset_ = payload_header->total_size_ - sizeof(uint32_t);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      reader.init_payload(buf + payload_offset, micro_header->data_zlength_));
}

// Corrupted offset table: end <= start, or range running past total_size_ -> OB_INVALID_DATA.
TEST_F(InvIdxDataMicroBlockReaderTest, test_locate_block_corrupt_offset_range)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  const char b0[] = "AAA";
  const char b1[] = "BBBBB";
  const char *blocks[] = {b0, b1};
  const int64_t sizes[] = {3, 5};
  char *buf = nullptr;
  int64_t size = 0;
  build_micro_block(allocator, blocks, sizes, 2, buf, size);

  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(buf);
  ObInvIdxMicroBlockPayloadHeader *header =
      reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(buf + micro_header->header_size_);
  const int64_t off_table =
      static_cast<int64_t>(micro_header->header_size_) + header->block_offsets_offset_;
  const char *block_buf = nullptr;
  int64_t block_size = 0;
  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(buf, size, false /*is_posting_value*/));

  // offsets[0] > offsets[1]  -> end <= start
  const uint32_t saved0 = get_u32(buf, off_table);
  const uint32_t saved1 = get_u32(buf, off_table + sizeof(uint32_t));
  put_u32(buf, off_table, 5);
  put_u32(buf, off_table + sizeof(uint32_t), 3);
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader.get_payload(0, block_buf, block_size));
  put_u32(buf, off_table, saved0);
  put_u32(buf, off_table + sizeof(uint32_t), saved1);

  // offsets[1] beyond total_size_ (relative to blocks_offset_) -> blocks_offset_ + end > total_size_
  const uint32_t huge = header->total_size_; // larger than any real block end
  put_u32(buf, off_table + sizeof(uint32_t), huge);
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader.get_payload(0, block_buf, block_size));
}

// block_idx >= block_count_ -> OB_INVALID_ARGUMENT.
TEST_F(InvIdxDataMicroBlockReaderTest, test_locate_block_out_of_range)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  const char b0[] = "AAA";
  const char b1[] = "BBBBB";
  const char *blocks[] = {b0, b1};
  const int64_t sizes[] = {3, 5};
  char *buf = nullptr;
  int64_t size = 0;
  build_micro_block(allocator, blocks, sizes, 2, buf, size);

  const char *block_buf = nullptr;
  int64_t block_size = 0;
  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(buf, size, false /*is_posting_value*/));
  // exactly block_count_ is out of range
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.get_payload(2, block_buf, block_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      reader.get_payload(100, block_buf, block_size));
}

// Single / multi block happy path including the last-block boundary.
TEST_F(InvIdxDataMicroBlockReaderTest, test_locate_block_single_and_multi)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  const char b0[] = "AAA";
  const char b1[] = "BBBBB";
  const char b2[] = "CC";
  const char *blocks[] = {b0, b1, b2};
  const int64_t sizes[] = {3, 5, 2};
  char *buf = nullptr;
  int64_t size = 0;
  build_micro_block(allocator, blocks, sizes, 3, buf, size);

  const char *block_buf = nullptr;
  int64_t block_size = 0;
  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(buf, size, false /*is_posting_value*/));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        reader.get_payload(i, block_buf, block_size));
    EXPECT_EQ(sizes[i], block_size);
    EXPECT_EQ(0, MEMCMP(blocks[i], block_buf, sizes[i]));
  }
  // last block boundary already covered by i == 2; verify it is non-empty.
  EXPECT_GT(block_size, 0);
}

TEST_F(InvIdxDataMicroBlockReaderTest, test_flat_cache_reader)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_flat_cache_micro_block(allocator, buf, size);

  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      reader.init(buf, size, false /*is_posting_value*/));
  ASSERT_EQ(OB_SUCCESS,
      reader.init_flat_cache(buf, size));
  ASSERT_EQ(3, reader.get_block_count());
  ObPostingBlockData block_data;
  ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(1, block_data));
  ASSERT_EQ(2, block_data.get_row_count());
  EXPECT_EQ(11, block_data.get_doc_ids()[0]);
  EXPECT_EQ(13, block_data.get_doc_ids()[1]);

  const uint64_t *doc_ids = nullptr;
  int64_t doc_count = 0;
  ASSERT_EQ(OB_SUCCESS, reader.get_doc_id_view(0, 2, doc_ids, doc_count));
  ASSERT_EQ(6, doc_count);
  ASSERT_NE(nullptr, doc_ids);
  EXPECT_EQ(7, doc_ids[0]);
  EXPECT_EQ(23, doc_ids[5]);

  const char *block_buf = nullptr;
  int64_t block_size = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, reader.get_payload(0, block_buf, block_size));

  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(buf);
  ObFlatCacheHeader *flat_cache_header = reinterpret_cast<ObFlatCacheHeader *>(buf + micro_header->header_size_);
  flat_cache_header->format_version_ = 0xFF;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      reader.init_flat_cache(buf, size));
  flat_cache_header->format_version_ = ObFlatCacheHeader::FORMAT_VERSION;
  flat_cache_header->column_count_ = ObPostingBlockHeader::POSTING_COL_CNT_WITH_RELEVANCE_PARAMS;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      reader.init_flat_cache(buf, size));
}

TEST_F(InvIdxDataMicroBlockReaderTest, test_flat_cache_rejects_invalid_frame_and_layout)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  enum class InvalidKind
  {
    ROW_STORE,
    MICRO_TYPE,
    COMPRESSED,
    ZERO_ROW_COUNT,
    OUTER_SIZE,
    FLAT_HEADER,
    FLAT_LAYOUT,
    MISALIGNED,
  };
  const auto expect_rejected = [&](const InvalidKind kind) {
    char *buf = nullptr;
    int64_t size = 0;
    build_flat_cache_micro_block(allocator, buf, size);
    ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(buf);
    ObFlatCacheHeader *flat_header = reinterpret_cast<ObFlatCacheHeader *>(
        buf + micro_header->header_size_);
    char *input_buf = buf;
    int64_t input_size = size;
    switch (kind) {
      case InvalidKind::ROW_STORE:
        micro_header->row_store_type_ = static_cast<uint8_t>(common::FLAT_ROW_STORE);
        micro_header->set_header_checksum();
        break;
      case InvalidKind::MICRO_TYPE:
        micro_header->inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING_VALUE);
        micro_header->set_header_checksum();
        break;
      case InvalidKind::COMPRESSED:
        --micro_header->data_zlength_;
        micro_header->set_header_checksum();
        break;
      case InvalidKind::ZERO_ROW_COUNT:
        micro_header->row_count_ = 0;
        micro_header->set_header_checksum();
        break;
      case InvalidKind::OUTER_SIZE:
        --input_size;
        break;
      case InvalidKind::FLAT_HEADER:
        flat_header->reserved32_ = 1;
        break;
      case InvalidKind::FLAT_LAYOUT:
        --flat_header->block_count_;
        break;
      case InvalidKind::MISALIGNED: {
        char *misaligned = static_cast<char *>(allocator.alloc(size + 1));
        ASSERT_NE(nullptr, misaligned);
        input_buf = misaligned + 1;
        MEMCPY(input_buf, buf, size);
        break;
      }
    }
    ObInvIdxMicroBlockReader reader;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        reader.init_flat_cache(input_buf, input_size));
  };
  expect_rejected(InvalidKind::ROW_STORE);
  expect_rejected(InvalidKind::MICRO_TYPE);
  expect_rejected(InvalidKind::COMPRESSED);
  expect_rejected(InvalidKind::ZERO_ROW_COUNT);
  expect_rejected(InvalidKind::OUTER_SIZE);
  expect_rejected(InvalidKind::FLAT_HEADER);
  expect_rejected(InvalidKind::FLAT_LAYOUT);
  expect_rejected(InvalidKind::MISALIGNED);
}

TEST_F(InvIdxDataMicroBlockReaderTest, test_flat_cache_rejects_invalid_access_indexes)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_flat_cache_micro_block(allocator, buf, size);
  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_SUCCESS,
      reader.init_flat_cache(buf, size));
  ObPostingBlockData block_data;
  const auto expect_invalid_block = [&](const int64_t block_idx) {
    ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(0, block_data));
    ASSERT_TRUE(block_data.is_valid());
    EXPECT_EQ(OB_INVALID_ARGUMENT, reader.get_posting_block_data(block_idx, block_data));
    EXPECT_EQ(0, block_data.get_row_count());
    EXPECT_EQ(nullptr, block_data.get_doc_ids());
  };
  expect_invalid_block(-1);
  expect_invalid_block(reader.get_block_count());

  const auto expect_invalid_view = [&](const int64_t first, const int64_t last) {
    const uint64_t *doc_ids = reinterpret_cast<const uint64_t *>(buf);
    int64_t doc_count = 42;
    EXPECT_EQ(OB_INVALID_ARGUMENT, reader.get_doc_id_view(first, last, doc_ids, doc_count));
    EXPECT_EQ(nullptr, doc_ids);
    EXPECT_EQ(0, doc_count);
  };
  expect_invalid_view(-1, 0);
  expect_invalid_view(1, 0);
  expect_invalid_view(0, reader.get_block_count());
}

TEST_F(InvIdxDataMicroBlockReaderTest, test_flat_cache_uninitialized_access_resets_outputs)
{
  ObInvIdxMicroBlockReader reader;
  uint64_t dummy_doc_id = 7;
  ObPostingBlockData block_data;
  block_data.doc_count_ = 1;
  block_data.doc_ids_ = &dummy_doc_id;
  EXPECT_EQ(OB_NOT_INIT, reader.get_posting_block_data(0, block_data));
  EXPECT_EQ(0, block_data.get_row_count());
  EXPECT_EQ(nullptr, block_data.get_doc_ids());

  const uint64_t *doc_ids = &dummy_doc_id;
  int64_t doc_count = 42;
  EXPECT_EQ(OB_NOT_INIT, reader.get_doc_id_view(0, 0, doc_ids, doc_count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, doc_count);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

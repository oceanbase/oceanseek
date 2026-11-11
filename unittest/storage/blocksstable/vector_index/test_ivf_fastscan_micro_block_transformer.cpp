/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <array>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_cache.h"

#define private public
#include "storage/blocksstable/vector_index/ob_ivf_fastscan_micro_block_transformer.h"
#undef private

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

namespace
{

static const int64_t TEST_DIM = 8;
static const int64_t TEST_CODE_SIZE = 16;

enum class MalformedDatum
{
  NONE,
  ROW_TYPE,
  CID_NULL,
  CID_SHORT,
  VID_NULL,
  VID_SHORT,
  PAYLOAD_NULL,
  PAYLOAD_NULL_PTR,
  PAYLOAD_SHORT,
};

class MockIvfFastScanMicroReader : public ObIMicroBlockReader
{
public:
  MockIvfFastScanMicroReader()
    : cids_(), vids_(), payloads_(), malformed_datum_(MalformedDatum::NONE),
      malformed_pass_(-1), read_count_(0)
  {}

  int get_row(const int64_t index, ObDatumRow &row) override
  {
    int ret = OB_SUCCESS;
    if (index < 0 || index >= static_cast<int64_t>(cids_.size())
        || row.get_column_count() < 6) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      row.storage_datums_[0].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
      row.storage_datums_[1].set_int(cids_[index]);
      row.storage_datums_[2].set_int(vids_[index]);
      row.storage_datums_[5].set_string(ObString(
          TEST_CODE_SIZE, reinterpret_cast<const char *>(payloads_[index].data())));
      row.count_ = 6;
      const int64_t pass = read_count_ / static_cast<int64_t>(cids_.size());
      if (pass == malformed_pass_) {
        switch (malformed_datum_) {
          case MalformedDatum::ROW_TYPE:
            row.storage_datums_[0].set_int(IVF_ROW_TYPE_CENTROID);
            break;
          case MalformedDatum::CID_NULL:
            row.storage_datums_[1].set_null();
            break;
          case MalformedDatum::CID_SHORT:
            row.storage_datums_[1].len_ = sizeof(int32_t);
            break;
          case MalformedDatum::VID_NULL:
            row.storage_datums_[2].set_null();
            break;
          case MalformedDatum::VID_SHORT:
            row.storage_datums_[2].len_ = sizeof(int32_t);
            break;
          case MalformedDatum::PAYLOAD_NULL:
            row.storage_datums_[5].set_null();
            break;
          case MalformedDatum::PAYLOAD_NULL_PTR:
            row.storage_datums_[5].ptr_ = nullptr;
            break;
          case MalformedDatum::PAYLOAD_SHORT:
            row.storage_datums_[5].len_ = TEST_CODE_SIZE - 1;
            break;
          default:
            break;
        }
      }
      ++read_count_;
    }
    return ret;
  }

  bool has_lob_out_row() const override { return false; }
  const ObMicroBlockHeader *get_micro_header() const override { return nullptr; }

  void add_row(const int64_t cid, const int64_t vid, const int64_t index)
  {
    std::array<uint8_t, TEST_CODE_SIZE> payload = {};
    payload[0] = static_cast<uint8_t>(index + 1);
    const float residual_norm = static_cast<float>(index) + 0.25F;
    const float correction = 0.5F;
    const float raw_norm = static_cast<float>(index) + 0.75F;
    MEMCPY(payload.data() + 4, &residual_norm, sizeof(float));
    MEMCPY(payload.data() + 8, &correction, sizeof(float));
    MEMCPY(payload.data() + 12, &raw_norm, sizeof(float));
    cids_.push_back(cid);
    vids_.push_back(vid);
    payloads_.push_back(payload);
  }

  void set_malformed_datum(const MalformedDatum datum, const int64_t pass)
  {
    malformed_datum_ = datum;
    malformed_pass_ = pass;
  }

  std::vector<int64_t> cids_;
  std::vector<int64_t> vids_;
  std::vector<std::array<uint8_t, TEST_CODE_SIZE>> payloads_;
  MalformedDatum malformed_datum_;
  int64_t malformed_pass_;
  int64_t read_count_;
};

uint8_t unpack_lane_zero(const uint8_t *packed_batch)
{
  return static_cast<uint8_t>(
      (packed_batch[0] & 0x0FU) | ((packed_batch[16] & 0x0FU) << 4));
}

} // namespace

TEST(TestIvfFastScanMicroBlockTransformer, SplitsBatchWhenCidChanges)
{
  ObArenaAllocator allocator("IvfFastCid");
  MockIvfFastScanMicroReader reader;
  for (int64_t i = 0; i < 35; ++i) {
    reader.add_row(i < 33 ? 10 : 20, 1000 + i, i);
  }

  ObIvfVectorMicroBlockTransformer transformer;
  transformer.is_inited_ = true;
  transformer.header_.row_count_ = 35;
  transformer.fastscan_ctx_ = ObIvfFastScanTransformContext(
      TEST_DIM, IVF_SSTABLE_FORMAT_VERSION);
  ASSERT_EQ(OB_SUCCESS,
      transformer.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  transformer.micro_reader_ = &reader;
  transformer.allocator_ = &allocator;

  char *block_buf = nullptr;
  int64_t block_size = 0;
  ASSERT_EQ(OB_SUCCESS, transformer.transform(block_buf, block_size));
  ASSERT_NE(nullptr, block_buf);
  const ObIvfFastScanBlockHeader *header =
      reinterpret_cast<const ObIvfFastScanBlockHeader *>(block_buf);
  ASSERT_TRUE(header->is_valid(block_size));
  ASSERT_EQ(3, header->batch_count_);
  ASSERT_EQ(35, header->row_count_);

  const int64_t *batch_cids = reinterpret_cast<const int64_t *>(
      block_buf + header->batch_cids_offset_);
  const int32_t *batch_row_counts = reinterpret_cast<const int32_t *>(
      block_buf + header->batch_row_counts_offset_);
  EXPECT_EQ(10, batch_cids[0]);
  EXPECT_EQ(10, batch_cids[1]);
  EXPECT_EQ(20, batch_cids[2]);
  EXPECT_EQ(32, batch_row_counts[0]);
  EXPECT_EQ(1, batch_row_counts[1]);
  EXPECT_EQ(2, batch_row_counts[2]);

  const int64_t *vids = reinterpret_cast<const int64_t *>(
      block_buf + header->vids_offset_);
  for (int64_t i = 0; i < 35; ++i) {
    EXPECT_EQ(1000 + i, vids[i]);
  }

  const uint8_t *packed_codes = reinterpret_cast<const uint8_t *>(
      block_buf + header->codes_offset_);
  EXPECT_EQ(1, unpack_lane_zero(packed_codes));
  EXPECT_EQ(33, unpack_lane_zero(packed_codes + 32));
  EXPECT_EQ(34, unpack_lane_zero(packed_codes + 64));
  for (int64_t i = 1; i < 16; ++i) {
    EXPECT_EQ(0, packed_codes[32 + i]);
    EXPECT_EQ(0, packed_codes[32 + 16 + i]);
  }

  const float *residual_norms = reinterpret_cast<const float *>(
      block_buf + header->residual_norms_offset_);
  EXPECT_FLOAT_EQ(31.25F, residual_norms[31]);
  EXPECT_FLOAT_EQ(32.25F, residual_norms[32]);
  EXPECT_FLOAT_EQ(0.0F, residual_norms[33]);
  EXPECT_FLOAT_EQ(33.25F, residual_norms[64]);
  EXPECT_FLOAT_EQ(34.25F, residual_norms[65]);
  EXPECT_FLOAT_EQ(0.0F, residual_norms[66]);

  allocator.free(block_buf);
  transformer.micro_reader_ = nullptr;
}

class TestIvfFastScanMalformedDatum
    : public ::testing::TestWithParam<std::tuple<MalformedDatum, int64_t>>
{};

TEST_P(TestIvfFastScanMalformedDatum, RejectsMalformedDatumInBothPasses)
{
  ObArenaAllocator allocator("IvfFastInvalid");
  MockIvfFastScanMicroReader reader;
  reader.add_row(10, 1000, 0);
  reader.set_malformed_datum(std::get<0>(GetParam()), std::get<1>(GetParam()));

  ObIvfVectorMicroBlockTransformer transformer;
  transformer.is_inited_ = true;
  transformer.header_.row_count_ = 1;
  transformer.fastscan_ctx_ = ObIvfFastScanTransformContext(
      TEST_DIM, IVF_SSTABLE_FORMAT_VERSION);
  ASSERT_EQ(OB_SUCCESS,
      transformer.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  transformer.micro_reader_ = &reader;
  transformer.allocator_ = &allocator;

  char *block_buf = nullptr;
  int64_t block_size = 0;
  EXPECT_EQ(OB_INVALID_DATA, transformer.transform(block_buf, block_size));
  EXPECT_EQ(nullptr, block_buf);
  EXPECT_EQ(0, block_size);
  transformer.micro_reader_ = nullptr;
}

INSTANTIATE_TEST_CASE_P(
    AllMalformedDatums,
    TestIvfFastScanMalformedDatum,
    ::testing::Combine(
        ::testing::Values(
            MalformedDatum::ROW_TYPE,
            MalformedDatum::CID_NULL,
            MalformedDatum::CID_SHORT,
            MalformedDatum::VID_NULL,
            MalformedDatum::VID_SHORT,
            MalformedDatum::PAYLOAD_NULL,
            MalformedDatum::PAYLOAD_NULL_PTR,
            MalformedDatum::PAYLOAD_SHORT),
        ::testing::Values<int64_t>(0, 1)));

TEST(TestIvfFastScanMicroBlockTransformer, QuantizedRowsPreservePayloadAfterRelocation)
{
  ObArenaAllocator allocator("IvfQuantRows");
  MockIvfFastScanMicroReader reader;
  reader.add_row(10, 1001, 0);
  reader.add_row(10, 1003, 1);
  reader.add_row(20, 1001, 2);
  ObIvfVectorMicroBlockTransformer transformer;
  transformer.is_inited_ = true;
  transformer.header_.row_count_ = 3;
  transformer.fastscan_ctx_ = ObIvfFastScanTransformContext(TEST_DIM, IVF_SSTABLE_FORMAT_VERSION);
  ASSERT_EQ(OB_SUCCESS, transformer.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  transformer.micro_reader_ = &reader;
  transformer.allocator_ = &allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, transformer.transform_quantized_rows(buf, size));
  transformer.micro_reader_ = nullptr;

  ObMicroBlockCacheValue value;
  ASSERT_EQ(OB_SUCCESS, value.init(buf, size, nullptr, 0, ObMicroBlockData::IVF_QUANTIZED_ROW_BLOCK));
  ASSERT_EQ(nullptr, value.get_block_data().get_micro_header());
  const int64_t copy_size = value.size();
  char *copy_buf = static_cast<char *>(allocator.alloc(copy_size));
  ASSERT_NE(nullptr, copy_buf);
  ObIKVCacheValue *copy = nullptr;
  ASSERT_EQ(OB_SUCCESS, value.deep_copy(copy_buf, copy_size, copy));
  MEMSET(buf, 0, size);
  allocator.free(buf);
  const auto &data = static_cast<ObMicroBlockCacheValue *>(copy)->get_block_data();
  const auto *rows = reinterpret_cast<const ObIvfQuantizedRowBlockHeader *>(data.get_buf());
  ASSERT_TRUE(rows->is_valid(data.get_buf_size()));
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_EQ(reader.cids_[i], rows->cids()[i]);
    EXPECT_EQ(reader.vids_[i], rows->vids()[i]);
    EXPECT_EQ(0, MEMCMP(reader.payloads_[i].data(), rows->payload(i), TEST_CODE_SIZE));
  }
  // Reject truncated data and offsets that could otherwise escape the value.
  EXPECT_FALSE(rows->is_valid(size - 1));
  auto *mutable_rows = const_cast<ObIvfQuantizedRowBlockHeader *>(rows);
  mutable_rows->payloads_offset_ = -1;
  EXPECT_FALSE(rows->is_valid(size));
  allocator.free(copy_buf);
}

TEST(TestIvfFastScanMicroBlockTransformer, QuantizedRowsRejectMalformedAndUnsortedInput)
{
  ObArenaAllocator allocator("IvfQuantBad");
  for (int mode = 0; mode < 3; ++mode) {
    MockIvfFastScanMicroReader reader;
    reader.add_row(10, 1001, 0);
    reader.add_row(mode == 1 ? 9 : 10, mode == 2 ? 1000 : 1002, 1);
    if (mode == 0) {
      reader.set_malformed_datum(MalformedDatum::PAYLOAD_SHORT, 0);
    }
    ObIvfVectorMicroBlockTransformer transformer;
    transformer.is_inited_ = true;
    transformer.header_.row_count_ = 2;
    transformer.fastscan_ctx_ = ObIvfFastScanTransformContext(TEST_DIM, IVF_SSTABLE_FORMAT_VERSION);
    ASSERT_EQ(OB_SUCCESS, transformer.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
    transformer.micro_reader_ = &reader;
    transformer.allocator_ = &allocator;
    char *buf = nullptr;
    int64_t size = 0;
    EXPECT_EQ(OB_INVALID_DATA, transformer.transform_quantized_rows(buf, size));
    EXPECT_EQ(nullptr, buf);
    EXPECT_EQ(0, size);
    transformer.micro_reader_ = nullptr;
  }
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ivf_fastscan_micro_block_transformer.log", true, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

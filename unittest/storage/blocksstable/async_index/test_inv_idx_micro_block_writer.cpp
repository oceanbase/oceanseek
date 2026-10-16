/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

class InvIdxMicroBlockWriterTest : public ::testing::Test
{
protected:
  InvIdxMicroBlockWriterTest() {}
  virtual ~InvIdxMicroBlockWriterTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  static void expect_offset(
      const char *buf,
      const int64_t offset_table_pos,
      const int64_t idx,
      const uint32_t expected_offset)
  {
    uint32_t actual_offset = 0;
    MEMCPY(&actual_offset,
        buf + offset_table_pos + idx * static_cast<int64_t>(sizeof(uint32_t)),
        sizeof(actual_offset));
    EXPECT_EQ(expected_offset, actual_offset);
  }

  static void verify_micro_block(
      const char *buf,
      const int64_t size,
      const int64_t expected_block_count,
      const char *expected_blocks,
      const int64_t expected_blocks_size,
      const uint32_t *expected_offsets,
      const int64_t expected_offset_count)
  {
    ASSERT_NE(nullptr, buf);
    ASSERT_EQ(expected_block_count + 1, expected_offset_count);

    const ObInvIdxMicroBlockPayloadHeader *header =
        reinterpret_cast<const ObInvIdxMicroBlockPayloadHeader *>(buf);
    const int64_t header_size = static_cast<int64_t>(sizeof(ObInvIdxMicroBlockPayloadHeader));
    const int64_t expected_total_size =
        header_size + expected_blocks_size
        + expected_offset_count * static_cast<int64_t>(sizeof(uint32_t));

    ASSERT_TRUE(header->is_valid());
    EXPECT_EQ(static_cast<int64_t>(ObInvIdxMicroBlockPayloadHeader::VERSION),
        static_cast<int64_t>(header->version_));
    EXPECT_EQ(0, static_cast<int64_t>(header->reserved_));
    EXPECT_EQ(expected_block_count, static_cast<int64_t>(header->block_count_));
    EXPECT_EQ(header_size, static_cast<int64_t>(header->blocks_offset_));
    EXPECT_EQ(header_size + expected_blocks_size,
        static_cast<int64_t>(header->block_offsets_offset_));
    EXPECT_EQ(expected_total_size, static_cast<int64_t>(header->total_size_));
    EXPECT_EQ(expected_total_size, size);

    ASSERT_EQ(0, MEMCMP(buf + header->blocks_offset_, expected_blocks, expected_blocks_size));
    for (int64_t i = 0; i < expected_offset_count; ++i) {
      expect_offset(buf, header->block_offsets_offset_, i, expected_offsets[i]);
    }
  }
};

TEST_F(InvIdxMicroBlockWriterTest, test_not_init_and_reset_state)
{
  ObInvIdxMicroBlockWriter writer;
  int64_t micro_block_size = 1;
  int64_t block_idx = 1;
  const char *buf = reinterpret_cast<const char *>(0x1);
  int64_t size = 1;
  const char block[] = "abc";

  ASSERT_EQ(OB_NOT_INIT, writer.reuse());
  ASSERT_EQ(OB_NOT_INIT, writer.estimate_size_after_append(3, micro_block_size));
  EXPECT_EQ(0, micro_block_size);
  ASSERT_EQ(OB_NOT_INIT, writer.append_logical_block(block, 3, block_idx));
  EXPECT_EQ(-1, block_idx);
  ASSERT_EQ(OB_NOT_INIT, writer.build_micro_block(buf, size));
  EXPECT_EQ(nullptr, buf);
  EXPECT_EQ(0, size);

  writer.reset();
  ASSERT_EQ(OB_NOT_INIT, writer.reuse());
}

TEST_F(InvIdxMicroBlockWriterTest, test_reuse_after_build)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char first_block[] = "abc";
  const char second_block[] = "xy";
  const char *buf = nullptr;
  int64_t size = 0;
  int64_t block_idx = -1;
  const uint32_t first_offsets[] = {0, 3};
  const uint32_t second_offsets[] = {0, 2};

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(first_block, 3, block_idx));
  ASSERT_EQ(0, block_idx);
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(buf, size));
  verify_micro_block(buf, size, 1, first_block, 3, first_offsets, 2);

  ASSERT_EQ(OB_SUCCESS, writer.reuse());
  ASSERT_TRUE(writer.is_empty());
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(second_block, 2, block_idx));
  ASSERT_EQ(0, block_idx);
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(buf, size));
  verify_micro_block(buf, size, 1, second_block, 2, second_offsets, 2);
}

TEST_F(InvIdxMicroBlockWriterTest, test_single_logical_block_layout)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char block[] = "hello";
  const char *buf = nullptr;
  int64_t size = 0;
  int64_t block_idx = -1;
  const uint32_t offsets[] = {0, 5};

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block, 5, block_idx));
  EXPECT_EQ(0, block_idx);
  EXPECT_EQ(1, writer.get_block_count());
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(buf, size));
  verify_micro_block(buf, size, 1, block, 5, offsets, 2);
}

TEST_F(InvIdxMicroBlockWriterTest, test_multiple_logical_block_layout)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char block2[] = "ijklmno";
  const char expected_blocks[] = "abcdefghijklmno";
  const uint32_t offsets[] = {0, 3, 8, 15};
  const char *buf = nullptr;
  int64_t size = 0;
  int64_t block_idx = -1;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block0, 3, block_idx));
  EXPECT_EQ(0, block_idx);
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block1, 5, block_idx));
  EXPECT_EQ(1, block_idx);
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block2, 7, block_idx));
  EXPECT_EQ(2, block_idx);
  EXPECT_EQ(3, writer.get_block_count());
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(buf, size));
  verify_micro_block(buf, size, 3, expected_blocks, 15, offsets, 4);
}

TEST_F(InvIdxMicroBlockWriterTest, test_estimate_size_after_append)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const int64_t header_size = static_cast<int64_t>(sizeof(ObInvIdxMicroBlockPayloadHeader));
  const char block[] = "abc";
  int64_t micro_block_size = 0;
  int64_t block_idx = -1;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.estimate_size_after_append(3, micro_block_size));
  EXPECT_EQ(header_size + 3 + 2 * static_cast<int64_t>(sizeof(uint32_t)), micro_block_size);

  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block, 3, block_idx));
  ASSERT_EQ(OB_SUCCESS, writer.estimate_size_after_append(5, micro_block_size));
  EXPECT_EQ(header_size + 3 + 5 + 3 * static_cast<int64_t>(sizeof(uint32_t)), micro_block_size);
}

TEST_F(InvIdxMicroBlockWriterTest, test_invalid_arguments_and_empty_build)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char block[] = "abc";
  const char *buf = nullptr;
  int64_t size = 0;
  int64_t micro_block_size = 0;
  int64_t block_idx = -1;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.estimate_size_after_append(0, micro_block_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.estimate_size_after_append(-1, micro_block_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_logical_block(nullptr, 3, block_idx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_logical_block(block, 0, block_idx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_logical_block(block, -1, block_idx));
  ASSERT_EQ(OB_ERR_UNEXPECTED, writer.build_micro_block(buf, size));
  EXPECT_EQ(nullptr, buf);
  EXPECT_EQ(0, size);
}

TEST_F(InvIdxMicroBlockWriterTest, test_append_after_build)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char block[] = "abc";
  const char *buf = nullptr;
  int64_t size = 0;
  int64_t micro_block_size = 0;
  int64_t block_idx = -1;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(block, 3, block_idx));
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(buf, size));
  ASSERT_EQ(OB_ERR_UNEXPECTED, writer.estimate_size_after_append(3, micro_block_size));
  ASSERT_EQ(OB_ERR_UNEXPECTED, writer.append_logical_block(block, 3, block_idx));
}

TEST_F(InvIdxMicroBlockWriterTest, test_exceed_macro_block_size)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char block[] = "x";
  int64_t micro_block_size = 0;
  int64_t block_idx = -1;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH,
      writer.estimate_size_after_append(common::OB_DEFAULT_MACRO_BLOCK_SIZE, micro_block_size));
  EXPECT_EQ(0, micro_block_size);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH,
      writer.append_logical_block(block, common::OB_DEFAULT_MACRO_BLOCK_SIZE, block_idx));
  EXPECT_EQ(-1, block_idx);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

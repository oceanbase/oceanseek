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

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

class TestIndexBlockRowHeader : public ::testing::Test
{
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

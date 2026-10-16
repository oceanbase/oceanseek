/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/backup/ob_backup_data_struct.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace backup
{

TEST(ObBackupDeviceMacroBlockId, basic) {
  ObBackupDeviceMacroBlockId id;
  
  // Test default constructor
  id = ObBackupDeviceMacroBlockId();
  EXPECT_FALSE(id.is_valid());
  
  // Test set() and getters
  int64_t backup_set_id = 1;
  int64_t ls_id = 2;
  int64_t data_type = 3; 
  int64_t turn_id = 4;
  int64_t retry_id = 5;
  int64_t file_id = 6;
  int64_t offset = 4096;
  int64_t length = 4096;

  EXPECT_EQ(OB_SUCCESS, 
    id.set(backup_set_id, ls_id, data_type, turn_id, retry_id, file_id, offset, length, ObBackupDeviceMacroBlockId::INDEX_TREE_BLOCK));

  EXPECT_EQ(backup_set_id, id.get_backupset_id());
  EXPECT_EQ(data_type, id.get_data_type());
  EXPECT_EQ(turn_id, id.get_turn_id());
  EXPECT_EQ(retry_id, id.get_retry_id());
  EXPECT_EQ(file_id, id.get_file_id());
  EXPECT_EQ(offset, id.get_offset());
  EXPECT_EQ(length, id.get_length());
  EXPECT_EQ(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP), id.get_id_mode());

  EXPECT_TRUE(id.is_valid());

  EXPECT_TRUE(ObBackupDeviceMacroBlockId::is_backup_block_file(id.first_id()));
  ObIOFd backup_io_fd(nullptr, id.first_id(), id.second_id(), id.third_id());
  EXPECT_TRUE(backup_io_fd.is_backup_block_file());
  EXPECT_TRUE(backup_io_fd.is_valid());
  ObIOFd invalid_backup_io_fd(nullptr, id.first_id(), id.second_id(), -1);
  EXPECT_TRUE(invalid_backup_io_fd.is_backup_block_file());
  EXPECT_FALSE(invalid_backup_io_fd.is_valid());

  constexpr uint64_t id_mode_shift = 64 - MacroBlockId::SF_BIT_VERSION - MacroBlockId::SF_BIT_ID_MODE;
  constexpr uint64_t version_shift = 64 - MacroBlockId::SF_BIT_VERSION;
  const uint64_t encoded_first_id = static_cast<uint64_t>(id.first_id());
  EXPECT_EQ(4, MacroBlockId::SF_BIT_ID_MODE);
  EXPECT_EQ(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_BACKUP),
      (encoded_first_id >> id_mode_shift) & ((1UL << MacroBlockId::SF_BIT_ID_MODE) - 1));
  EXPECT_EQ(static_cast<uint64_t>(ObBackupDeviceMacroBlockId::BACKUP_MACRO_BLOCK_ID_VERSION),
      (encoded_first_id >> version_shift) & ((1UL << MacroBlockId::SF_BIT_VERSION) - 1));

  blocksstable::MacroBlockId macro_block_id = id.get_macro_id();
  EXPECT_TRUE(macro_block_id.is_backup_id());
  EXPECT_FALSE(macro_block_id.is_local_id());
  EXPECT_TRUE(macro_block_id.is_valid());

  char buf[MacroBlockId::SERIALIZED_SIZE] = {};
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, macro_block_id.serialize(buf, sizeof(buf), pos));
  EXPECT_EQ(MacroBlockId::SERIALIZED_SIZE, pos);
  MacroBlockId decoded_id;
  int64_t decode_pos = 0;
  EXPECT_EQ(OB_SUCCESS, decoded_id.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_EQ(macro_block_id, decoded_id);
  EXPECT_TRUE(decoded_id.is_backup_id());

  ObBackupDeviceMacroBlockId decoded_backup_id;
  EXPECT_EQ(OB_SUCCESS, decoded_backup_id.set(decoded_id));
  EXPECT_EQ(id, decoded_backup_id);

  ObBackupDeviceMacroBlockId invalid_backup_id;
  MacroBlockId local_macro_id(7, 1001, 0);
  EXPECT_TRUE(local_macro_id.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      invalid_backup_id.set(local_macro_id));

  MacroBlockId shared_macro_id;
  shared_macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  shared_macro_id.set_second_id(1001);
  shared_macro_id.set_third_id(1002);
  shared_macro_id.set_fourth_id(1003);
  shared_macro_id.set_fifth_id(1004);
  EXPECT_TRUE(shared_macro_id.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      invalid_backup_id.set(shared_macro_id));

  // Test reset()
  id.reset();
  EXPECT_FALSE(id.is_valid());

  EXPECT_FALSE(ObBackupDeviceMacroBlockId::is_backup_block_file(id.first_id()));

  // fd={first_id:-1, second_id:-1, third_id:-1, fourth_id:0, fifth_id:1, device_handle:null}
  // max_file_size=4294967296, io_fd={first_id:-1, second_id:-1, third_id:-1, fourth_id:1369094286720630784, fifth_id:1, device_handle:null})
  // info={tenant_id_:1002, fd_:{first_id:0, second_id:-1, third_id:0, fourth_id:-1, fifth_id:-1, device_handle:null},
  // ObIOFd io_fd(NULL, 0, -1, 0, -1, -1);
  // EXPECT_TRUE(io_fd.is_valid());

  // [2023-08-14 15:55:33.678613] INFO  [STORAGE] TestBody (test_backup_device_macro_block_id.cpp:84) [41402][][T0][Y0-0000000000000000-0-0] [lt=0] YYY DEBUG(macro_id={backup_set_id:2, dir_id:1, turn_id:1, retry_id:0, file_id:0, offset:1, length:512})
  // first_id:69206017, second_id:1001, third_id:2233785415192543234
  // {first_id:2097153, second_id:1001, third_id:2233785415192543234
  ObBackupDeviceMacroBlockId macro_id;
  macro_id.first_id_ = 67133441;
  macro_id.second_id_ = 1;
  macro_id.third_id_ = 2161727821171392513;

  ObArray<int64_t> array;
  array.prepare_allocate(1);
  array.at(0) = 1;

  ObBackupDeviceMacroBlockId macro_id_1;
  macro_id_1.first_id_ = 4557655548891137;
  macro_id_1.second_id_ = 1003;
  macro_id_1.third_id_ = 68769808390;
  const int64_t offset_1 = macro_id_1.offset_ * 4096;
  EXPECT_EQ(offset_1, -995655680);
  const int64_t offset_2 = macro_id_1.offset_ * DIO_READ_ALIGN_SIZE;
  EXPECT_EQ(offset_2, 3299311616);
}  

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_device_macro_block_id.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_device_macro_block_id.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

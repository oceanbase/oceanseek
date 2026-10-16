/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// 场景：验证宏块复制从未完成批次恢复，以及恢复时保留增量主表宏块标识。

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define private public
#include "src/storage/high_availability/ob_ss_ha_macro_copy_dag_net.h"
#include "src/storage/high_availability/ob_ss_storage_ha_writer.h"
#undef private
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_type.h"

using namespace oceanbase;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace
{

ObSSHAMacroBlockInfo make_macro_info(const int64_t block_index)
{
  ObSSHAMacroBlockInfo macro_info;
  macro_info.ls_id_ = ObLSID(1);
  macro_info.macro_block_id_.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  macro_info.macro_block_id_.set_second_id(block_index + 1);
  macro_info.macro_block_id_.set_reorganization_scn(block_index + 1);
  macro_info.macro_block_id_.set_op_id(block_index + 1);
  macro_info.macro_block_id_.set_seq_id(block_index + 1);
  macro_info.occupy_size_ = 1;
  return macro_info;
}

bool contains(const ObSSHAMacroBlockBatch &batch, const ObSSHAMacroBlockInfo &macro_info)
{
  bool found = false;
  for (int64_t i = 0; !found && i < batch.count(); ++i) {
    found = batch.at(i) == macro_info;
  }
  return found;
}

MacroBlockId make_inc_major_macro_id(
    const ObStorageObjectType object_type,
    const int64_t fourth_id)
{
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
  macro_id.set_second_id(200001);
  macro_id.set_ss_third_id(false, 1, 300001);
  macro_id.set_fourth_id(fourth_id);
  macro_id.set_seq_id(400001);
  macro_id.set_column_group_id(5);
  return macro_id;
}

void expect_same_macro_id(
    const MacroBlockId &expected,
    const MacroBlockId &actual)
{
  EXPECT_EQ(expected.id_mode(), actual.id_mode());
  EXPECT_EQ(expected.storage_object_type(), actual.storage_object_type());
  EXPECT_EQ(expected.first_id(), actual.first_id());
  EXPECT_EQ(expected.second_id(), actual.second_id());
  EXPECT_EQ(expected.third_id(), actual.third_id());
  EXPECT_EQ(expected.fourth_id(), actual.fourth_id());
  EXPECT_EQ(expected.fifth_id(), actual.fifth_id());
  EXPECT_EQ(expected.column_group_id(), actual.column_group_id());
  EXPECT_EQ(expected.reorganization_scn(), actual.reorganization_scn());
}

void verify_restore_preserves_macro_id(const MacroBlockId &source_macro_id)
{
  char manifest_buf[512] = {0};
  int64_t pos = 0;
  backup::ObBackupBlockFileMacroIdItem source_item(source_macro_id, 4096);
  backup::ObBackupBlockFileMacroIdItem restored_item;
  ASSERT_TRUE(source_item.is_valid());
  ASSERT_EQ(OB_SUCCESS,
      source_item.serialize(manifest_buf, sizeof(manifest_buf), pos));

  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      restored_item.deserialize(manifest_buf, sizeof(manifest_buf), pos));
  ASSERT_EQ(source_item.occupy_size_, restored_item.occupy_size_);
  expect_same_macro_id(source_macro_id, restored_item.macro_id_);

  ObSSHAMacroBlockInfo macro_info;
  macro_info.ls_id_ = ObLSID(1);
  macro_info.macro_block_id_ = restored_item.macro_id_;
  macro_info.occupy_size_ = restored_item.occupy_size_;
  ObSSHAMacroBlockBatch macro_batch;
  ASSERT_EQ(OB_SUCCESS, macro_batch.add_one(macro_info));

  ObCopySSMacroBlockRestoreWriter writer;
  ObStorageObjectWriteInfo write_info;
  ObStorageObjectOpt object_opt;
  MacroBlockId rebuilt_macro_id;
  writer.ls_id_ = ObLSID(1);
  ASSERT_EQ(OB_SUCCESS,
      writer.set_macro_write_info_(
          macro_batch.at(0).macro_block_id_, write_info, object_opt));
  ASSERT_EQ(OB_SUCCESS,
      ObObjectManager::ss_get_object_id(object_opt, rebuilt_macro_id));
  expect_same_macro_id(source_macro_id, rebuilt_macro_id);
}

TEST(TestSSHAMacroCopyRetry, rebuild_from_unfinished_batch)
{
  ObSSHACopyMacroRangeInfoMgr mgr;
  ObSSHAMacroBlockBatch whole_batch;
  for (int64_t i = 0; i < 18; ++i) {
    ASSERT_EQ(OB_SUCCESS, whole_batch.add_one(make_macro_info(i)));
  }
  ASSERT_EQ(OB_SUCCESS, mgr.init(whole_batch));

  ObSSHAMacroBlockBatch first_batch;
  ASSERT_EQ(OB_SUCCESS, mgr.get_next_sub_batch(first_batch));
  ASSERT_EQ(8, first_batch.count());

  ObSSHAMacroBlockBatch unfinished_batch;
  ASSERT_EQ(OB_SUCCESS, unfinished_batch.add_one(whole_batch.at(0)));
  ASSERT_EQ(OB_SUCCESS, unfinished_batch.add_one(whole_batch.at(1)));
  ASSERT_EQ(OB_SUCCESS, unfinished_batch.add_one(whole_batch.at(16)));
  ASSERT_EQ(OB_SUCCESS, unfinished_batch.add_one(whole_batch.at(17)));
  ASSERT_EQ(OB_SUCCESS, mgr.reuse(unfinished_batch));

  ObSSHAMacroBlockBatch retry_batch;
  ASSERT_EQ(OB_SUCCESS, mgr.get_next_sub_batch(retry_batch));
  ASSERT_EQ(unfinished_batch.count(), retry_batch.count());
  for (int64_t i = 0; i < unfinished_batch.count(); ++i) {
    EXPECT_TRUE(contains(retry_batch, unfinished_batch.at(i)));
  }
  EXPECT_TRUE(mgr.is_iter_end());
}

// 验证 DDL 与 compaction inc-major 宏块经过备份清单反序列化和恢复写入后，
// 仍完整保留来源物理 ID，不按目标 SSTable 重新生成。
TEST(TestSSHAMacroCopyRetry, restore_preserves_inc_major_macro_id)
{
  const MacroBlockId ddl_macro_id = make_inc_major_macro_id(
      ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
      500001 /* DDL 快照版本 */);
  const MacroBlockId compaction_macro_id = make_inc_major_macro_id(
      ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO,
      600001 /* 合并操作 ID */);

  ASSERT_TRUE(ddl_macro_id.is_valid());
  ASSERT_TRUE(compaction_macro_id.is_valid());
  verify_restore_preserves_macro_id(ddl_macro_id);
  verify_restore_preserves_macro_id(compaction_macro_id);
}

} // namespace

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

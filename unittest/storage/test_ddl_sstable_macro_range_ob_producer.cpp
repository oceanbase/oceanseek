/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/high_availability/ob_storage_ha_reader.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"
#include "storage/tablet/ob_tablet.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/ob_ddl_common.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace unittest
{
#ifdef OB_BUILD_SHARED_STORAGE
namespace
{
class ServerModeGuard final
{
public:
  explicit ServerModeGuard(const share::ObServerMode mode)
    : old_mode_(GCTX.startup_mode_)
  {
    GCTX.startup_mode_ = mode;
  }
  ~ServerModeGuard()
  {
    GCTX.startup_mode_ = old_mode_;
  }
private:
  share::ObServerMode old_mode_;
};

class TestIncMajorInsertTabletContext final
  : public storage::ObDirectLoadInsertDataTabletContext
{
protected:
  int get_pk_interval(
      uint64_t count,
      share::ObTabletCacheInterval &pk_interval) override
  {
    UNUSED(count);
    UNUSED(pk_interval);
    return OB_SUCCESS;
  }
};
}
#endif

class TestDDLCopySSTableMacroRangeObProducer : public ::testing::Test
{
public:
  TestDDLCopySSTableMacroRangeObProducer() = default;
  void SetUp() {}
  void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

TEST_F(TestDDLCopySSTableMacroRangeObProducer, shared_storage_mode)
{
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> macro_block_id_array;
  ObCopyMacroRangeInfo macro_range_info;
  const int64_t buf_size = ObDDLCopySSTableMacroRangeObProducer::MAX_BUF_SIZE;
  char buf[buf_size] = {0};

  for (int64_t i = 0; OB_SUCC(ret) && i < 128; ++i) {
    MacroBlockId macro_id;
    macro_id.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
    macro_id.set_second_id(1L << 33);
    macro_id.set_reorganization_scn(0);
    macro_id.set_op_id(0);
    macro_id.set_seq_id(i + 1);
    ret = macro_block_id_array.push_back(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = ObStorageHAUtils::make_macro_id_to_datum(macro_block_id_array, buf, buf_size, macro_range_info.start_macro_block_end_key_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<MacroBlockId> tmp_macro_block_id_array;
  ret = ObStorageHAUtils::extract_macro_id_from_datum(macro_range_info.start_macro_block_end_key_, tmp_macro_block_id_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_macro_block_id_array.count(), macro_block_id_array.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_id_array.count(); ++i) {
    const MacroBlockId &id1 = macro_block_id_array.at(i);
    const MacroBlockId &id2 = tmp_macro_block_id_array.at(i);
    ASSERT_EQ(id1, id2);
  }
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestDDLCopySSTableMacroRangeObProducer, inc_major_fixed_sequence_slots)
{
  const int64_t tablet_id = 200001;
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  const int64_t index_slot = ObVersionLocalMacroSeq::get_inc_major_index_slot();
  ObMacroDataSeq data_start;
  ObMacroSeqWindow data_window;
  ASSERT_EQ(OB_SUCCESS,
            share::ObDDLUtil::init_macro_block_seq(
                storage::DIRECT_LOAD_INCREMENTAL_MAJOR,
                ObTabletID(tablet_id), index_slot - 1, data_start));
  ASSERT_EQ(OB_SUCCESS,
            ObVersionLocalMacroSeq::build_inc_major_data_window(
                index_slot - 1, data_window));
  ASSERT_EQ(data_window.start_, data_start.macro_data_seq_);
  ObMacroDataSeq resume_start(data_window.start_ + 123);
  int64_t resume_end = -1;
  const int64_t resume_slot = share::ObDDLUtil::get_parallel_idx(resume_start);
  ASSERT_EQ(index_slot - 1, resume_slot);
  ASSERT_EQ(OB_SUCCESS,
            ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
                resume_slot, resume_start.macro_data_seq_, resume_end));
  ASSERT_EQ(data_window.end_, resume_end);

  ASSERT_EQ(OB_SIZE_OVERFLOW,
            share::ObDDLUtil::init_macro_block_seq(
                storage::DIRECT_LOAD_INCREMENTAL_MAJOR,
                ObTabletID(tablet_id), index_slot, data_start));
}

TEST_F(TestDDLCopySSTableMacroRangeObProducer, inc_major_dynamic_slice_sequence)
{
  const int64_t tablet_id = 200001;
  const int64_t first_slot = 7;
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  storage::ObDirectLoadInsertTableParam param;
  TestIncMajorInsertTabletContext tablet_ctx;
  storage::ObDirectLoadInsertTabletWriteCtx first_write_ctx;
  storage::ObDirectLoadInsertTabletWriteCtx second_write_ctx;
  ObMacroSeqWindow first_window;
  ObMacroSeqWindow second_window;

  param.is_inc_major_ = true;
  tablet_ctx.param_ = &param;
  tablet_ctx.tablet_id_ = ObTabletID(tablet_id);
  tablet_ctx.slice_idx_ = first_slot;
  tablet_ctx.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, tablet_ctx.get_write_ctx(first_write_ctx));
  ASSERT_EQ(OB_SUCCESS, tablet_ctx.get_write_ctx(second_write_ctx));
  ASSERT_EQ(OB_SUCCESS,
            ObVersionLocalMacroSeq::build_inc_major_data_window(
                first_slot, first_window));
  ASSERT_EQ(OB_SUCCESS,
            ObVersionLocalMacroSeq::build_inc_major_data_window(
                first_slot + 1, second_window));
  ASSERT_EQ(first_slot, first_write_ctx.slice_idx_);
  ASSERT_EQ(first_window.start_, first_write_ctx.start_seq_.macro_data_seq_);
  ASSERT_EQ(first_slot + 1, second_write_ctx.slice_idx_);
  ASSERT_EQ(second_window.start_, second_write_ctx.start_seq_.macro_data_seq_);
  ASSERT_EQ(second_write_ctx.start_seq_.macro_data_seq_,
            tablet_ctx.get_last_data_seq().macro_data_seq_);

  tablet_ctx.slice_idx_ = ObVersionLocalMacroSeq::get_inc_major_index_slot();
  ASSERT_EQ(OB_SIZE_OVERFLOW, tablet_ctx.get_write_ctx(second_write_ctx));
  ASSERT_EQ(ObVersionLocalMacroSeq::get_inc_major_index_slot(),
            tablet_ctx.slice_idx_);
}

TEST_F(TestDDLCopySSTableMacroRangeObProducer, inc_major_object_identity_on_restore)
{
  const int64_t ls_id = 1001;
  const int64_t tablet_id = 200001;
  const int64_t snapshot_version = 300001;
  const int64_t reorganization_scn = 400001;
  const int64_t seq_slot = 7;
  const int64_t local_offset = 123;
  const int64_t raw_seq = seq_slot * compaction::MACRO_STEP_SIZE + local_offset;
  const uint16_t column_group_id = 5;
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  ObCopyMacroBlockRestoreReader reader;
  ObTablet tablet;
  ObLogicMacroBlockId logic_block_id(raw_seq, snapshot_version, tablet_id);
  MacroBlockId restored_macro_id;
  MacroBlockId restored_dump_macro_id;

  reader.is_inited_ = true;
  reader.table_key_.table_type_ = ObITable::INC_MAJOR_SSTABLE;
  reader.table_key_.version_range_.snapshot_version_ = snapshot_version;
  tablet.tablet_meta_.ls_id_ = ObLSID(ls_id);
  ASSERT_EQ(OB_SUCCESS,
            tablet.tablet_meta_.transfer_info_.transfer_start_scn_.convert_for_tx(
                reorganization_scn));
  logic_block_id.column_group_idx_ = column_group_id;
  ASSERT_EQ(OB_SUCCESS, logic_block_id.data_seq_.set_sstable_seq(3));

  reader.table_key_.tablet_id_ = ObTabletID(tablet_id);
  reader.tablet_handle_.obj_ = &tablet;
  const int convert_ret =
      reader.convert_logical_id_to_shared_macro_id_(logic_block_id, restored_macro_id);
  reader.tablet_handle_.obj_ = nullptr;

  ASSERT_EQ(OB_SUCCESS, convert_ret);
  ASSERT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
            restored_macro_id.storage_object_type());
  ASSERT_EQ(snapshot_version, restored_macro_id.op_id());
  ASSERT_EQ(logic_block_id.data_seq_.get_data_seq(), restored_macro_id.seq_id());
  ASSERT_EQ(column_group_id, restored_macro_id.column_group_id());
  ASSERT_EQ(reorganization_scn, restored_macro_id.reorganization_scn());

  ASSERT_EQ(OB_SUCCESS,
            reader.table_key_.scn_range_.start_scn_.convert_for_tx(500001));
  ASSERT_EQ(OB_SUCCESS,
            reader.table_key_.scn_range_.end_scn_.convert_for_tx(500002));
  ASSERT_NE(snapshot_version, reader.table_key_.get_snapshot_version());
  const ObITable::TableType dump_table_types[] = {
      ObITable::INC_MAJOR_DDL_DUMP_SSTABLE,
      ObITable::INC_MAJOR_DDL_MERGE_CO_SSTABLE,
      ObITable::INC_MAJOR_DDL_MERGE_CG_SSTABLE};
  for (int64_t i = 0; i < ARRAYSIZEOF(dump_table_types); ++i) {
    reader.table_key_.table_type_ = dump_table_types[i];
    reader.tablet_handle_.obj_ = &tablet;
    const int dump_convert_ret = reader.convert_logical_id_to_shared_macro_id_(
        logic_block_id, restored_dump_macro_id);
    reader.tablet_handle_.obj_ = nullptr;

    ASSERT_EQ(OB_SUCCESS, dump_convert_ret);
    ASSERT_EQ(restored_macro_id, restored_dump_macro_id);
  }
}
#endif

}
}

int main(int argc, char **argv)
{
  system("rm -f test_ddl_sstable_macro_range_ob_producer.log*");
  OB_LOGGER.set_file_name("test_ddl_sstable_macro_range_ob_producer.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_ddl_sstable_macro_range_ob_producer");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

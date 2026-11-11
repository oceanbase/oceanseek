/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define protected public
#define private public

#include "lib/utility/ob_defer.h"
#include "storage/blocksstable/ob_macro_block.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/test_schema_prepare.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/blocksstable/ob_data_file_prepare.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace compaction;

namespace unittest
{
static ObSimpleMemLimitGetter getter;
class TestIVFMergeObjectId : public blocksstable::TestDataFilePrepare
{
public:
  TestIVFMergeObjectId()
    : blocksstable::TestDataFilePrepare(&getter, "test_ivf_merge_object_id"),
      tenant_base_(1),
      mock_ls_id_(1),
      mock_tablet_id_(1)
  {}
  ~TestIVFMergeObjectId() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  ObTenantBase tenant_base_;
  share::ObLSID mock_ls_id_;
  ObTabletID mock_tablet_id_;
};

void TestIVFMergeObjectId::SetUp()
{
  TestDataFilePrepare::SetUp();
  ObTenantEnv::set_tenant(&tenant_base_);
}

void TestIVFMergeObjectId::TearDown()
{
  TestDataFilePrepare::TearDown();
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestIVFMergeObjectId, ivf_major_objects_use_attempt_id)
{
  const bool saved_ss_mode = OB_STORAGE_OBJECT_MGR.is_shared_storage_;
  DEFER(OB_STORAGE_OBJECT_MGR.is_shared_storage_ = saved_ss_mode);
  OB_STORAGE_OBJECT_MGR.is_shared_storage_ = true;
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, 5);
  const int64_t snapshot = 10000;
  for (const ObMergeType merge_type : {MAJOR_MERGE, MEDIUM_MERGE}) {
    for (const bool is_non_idem_major : {false, true}) {
      MacroBlockId previous_ids[2];
      for (int64_t op_id = 0; op_id < 2; ++op_id) {
        ObWholeDataStoreDesc data_desc;
        ObWholeDataStoreDesc index_desc;
        ASSERT_EQ(OB_SUCCESS, data_desc.init(false, table_schema, mock_ls_id_, mock_tablet_id_,
            merge_type, snapshot, DATA_VERSION_4_2_0_0, table_schema.get_micro_index_clustered(),
            0 /*transfer_seq*/, 1 /*concurrent_cnt*/, share::SCN::min_scn()));
        auto &static_desc = data_desc.get_static_desc();
        ASSERT_FALSE(static_desc.is_non_idem_major_);
        static_desc.is_non_idem_major_ = is_non_idem_major;
        static_desc.exec_mode_ = EXEC_MODE_OUTPUT;
        static_desc.op_id_ = op_id;
        ASSERT_EQ(OB_SUCCESS, index_desc.gen_index_store_desc(data_desc.get_desc()));
        ObDataStoreDesc *descs[] = {&data_desc.get_desc(), &index_desc.get_desc()};
        for (int64_t i = 0; i < 2; ++i) {
          ASSERT_EQ(is_non_idem_major, descs[i]->get_is_non_idem_major());
          ASSERT_EQ(merge_type, descs[i]->get_merge_type());
          ASSERT_EQ(snapshot, descs[i]->get_snapshot_version());
          ObMacroSeqParam seq_param;
          seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
          seq_param.end_ = 100;
          ObMacroIncSeqGenerator seq_generator;
          int64_t seq = -1;
          ASSERT_EQ(OB_SUCCESS, seq_generator.init(seq_param));
          ASSERT_EQ(OB_SUCCESS, seq_generator.get_next(seq));
          ObSSTablePrivateObjectCleaner cleaner;
          ObMacroBlockWriter writer;
          writer.data_store_desc_ = descs[i];
          writer.macro_seq_generator_ = &seq_generator;
          writer.object_cleaner_ = &cleaner;
          ASSERT_EQ(OB_SUCCESS, writer.alloc_block());
          const MacroBlockId &id = writer.macro_handles_[0].get_macro_id();
          const ObStorageObjectType expected_type = is_non_idem_major
              ? (0 == i ? ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO
                        : ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO)
              : (0 == i ? ObStorageObjectType::SHARED_MAJOR_DATA_MACRO
                        : ObStorageObjectType::SHARED_MAJOR_META_MACRO);
          ASSERT_EQ(expected_type, id.storage_object_type());
          ASSERT_EQ(is_non_idem_major ? op_id : snapshot, id.fourth_id());
          ASSERT_EQ(seq, id.seq_id());
          ObLogicMacroBlockId logical_id;
          writer.gen_logic_macro_id(logical_id);
          ASSERT_EQ(snapshot, logical_id.logic_version_);
          ASSERT_TRUE(logical_id.is_valid());
          if (op_id > 0) {
            ASSERT_EQ(!is_non_idem_major, id == previous_ids[i]);
          }
          previous_ids[i] = id;
        }
        static_desc.reset();
        ASSERT_FALSE(static_desc.is_non_idem_major_);
      }
    }
  }
}
TEST_F(TestIVFMergeObjectId, non_idem_major_object_paths)
{
  for (const auto type : {ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO,
                         ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO}) {
    for (const int64_t op_id : {0, 37}) {
      ObStorageObjectOpt opt;
      opt.set_ss_share_object_opt(type, false, 1, 12345, op_id,
          MACRO_STEP_SIZE + 17, 3, 99);
      MacroBlockId id;
      ASSERT_EQ(OB_SUCCESS, STI(type).get_object_id(opt, id));
      ASSERT_TRUE(id.is_valid());
      ASSERT_EQ(type, id.storage_object_type());
      ASSERT_TRUE(STI(type).is_major());
      ASSERT_TRUE(id.is_shared_data_block_or_meta_block());
      ASSERT_EQ(op_id, id.op_id());
      char path[1024] = {0};
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, STI(type).to_relative_remote_path_format(path, sizeof(path), pos, id));
      MacroBlockId parsed;
      ASSERT_EQ(OB_SUCCESS, STI(type).remote_path_to_macro_id(path, parsed));
      ASSERT_EQ(id, parsed);
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, STI(type).to_local_path_format(
          path, sizeof(path), pos, id, 1, 1, 1, 1, 1));
      parsed.reset();
      ASSERT_EQ(OB_SUCCESS, STI(type).local_path_to_macro_id(path, parsed));
      ASSERT_EQ(id, parsed);
      if (ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO == type) {
        ObSharedObjectsWriteCtx ctx;
        ctx.next_opt_ = opt;
        ASSERT_EQ(OB_SUCCESS, ctx.advance_data_seq());
        ASSERT_EQ(MACRO_STEP_SIZE + 18, ctx.next_opt_.ss_share_opt_.data_seq_);
        ASSERT_EQ(op_id, ctx.next_opt_.ss_share_opt_.fourth_id_);
      }
    }
  }
}

TEST_F(TestIVFMergeObjectId, major_control_layout_round_trip)
{
  share::SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(10000));
  for (const bool is_non_idem : {false, true}) {
    ObSSMetaCtrlObjectInfo info;
    ASSERT_EQ(OB_SUCCESS, is_non_idem
        ? info.init_by_non_idem_major(snapshot, MACRO_STEP_SIZE, 82, 0)
        : info.init_by_major(snapshot, MACRO_STEP_SIZE, 82, 0));
    ASSERT_TRUE(info.is_valid());
    ASSERT_EQ(is_non_idem ? ObSSMetaCtrlObjectInfo::TYPE::NON_IDEM_MAJOR
                               : ObSSMetaCtrlObjectInfo::TYPE::MAJOR, info.type_);
    ASSERT_EQ(!is_non_idem, info.is_major());
    ASSERT_FALSE(info.is_inc_major());
    ASSERT_EQ(is_non_idem, info.is_non_idem_major());
    char buf[256];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, info.serialize(buf, sizeof(buf), pos));
    ObSSMetaCtrlObjectInfo restored;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(buf, pos, read_pos));
    ASSERT_EQ(pos, read_pos);
    ASSERT_TRUE(info == restored);
    ASSERT_EQ(!is_non_idem, restored.is_major());
    ASSERT_EQ(is_non_idem, restored.is_non_idem_major());
    ObSSMetaCtrlObjectInfo ordinary;
    ASSERT_EQ(OB_SUCCESS, ordinary.init_by_major(snapshot, MACRO_STEP_SIZE, 82, 0));
    ASSERT_EQ(!is_non_idem, info == ordinary);
    restored.cg_cnt_ = 1;
    ASSERT_FALSE(info == restored);
    restored.cg_cnt_ = -1;
    ASSERT_FALSE(restored.is_valid());
    restored.reset();
    ASSERT_FALSE(restored.is_major());
    ASSERT_FALSE(restored.is_non_idem_major());

    restored = info;
    ASSERT_EQ(OB_SUCCESS, restored.init_by_major(snapshot, MACRO_STEP_SIZE, 82, 0));
    ASSERT_TRUE(restored == ordinary);
    ASSERT_FALSE(restored.is_non_idem_major());

    restored = info;
    ASSERT_EQ(OB_SUCCESS, restored.init_by_inc_major(snapshot, MACRO_STEP_SIZE, 82, 0));
    ASSERT_TRUE(restored.is_valid());
    ASSERT_FALSE(restored.is_non_idem_major());
    restored = info;
    ASSERT_EQ(OB_SUCCESS, restored.init_by_none_major(snapshot, 82));
    ASSERT_TRUE(restored.is_valid());
    ASSERT_FALSE(restored.is_non_idem_major());
    restored = info;
    restored.set_none();
    ASSERT_TRUE(restored.is_valid());
    ASSERT_FALSE(restored.is_non_idem_major());
  }
}

TEST_F(TestIVFMergeObjectId, non_idem_control_key_round_trip)
{
  share::SCN control_scn;
  ASSERT_EQ(OB_SUCCESS, control_scn.convert_for_tx(10000));
  for (const ObTabletID &tablet_id : {ObTabletID(1), ObTabletID(220000)}) {
    const ObSSMetaCtrlKey key(ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL,
        tablet_id, share::SCN::min_scn(), control_scn, share::ObLSID(1001));
    ASSERT_TRUE(key.is_valid());
    ASSERT_TRUE(key.is_sstable_type());
    ASSERT_FALSE(key.is_major_sstable_type());
    char buf[1024];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, key.serialize_meta_key(buf, sizeof(buf), pos));
    ObSSMetaCtrlKey restored;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize_meta_key(buf, pos, read_pos));
    ASSERT_EQ(pos, read_pos);
    ASSERT_EQ(key.convert_to_macro_id(), restored.convert_to_macro_id());
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, restored.get_lock_row_key(lock_key));
    ASSERT_TRUE(lock_key.is_valid());
    ASSERT_EQ(ObStorageObjectType::SHARED_NON_IDEM_MAJOR_LOCK, lock_key.type_);
    ASSERT_TRUE(is_ss_meta_ctrl_lock_type(lock_key.type_));
    ASSERT_TRUE(ObSSMetaCtrlType::should_backup(lock_key.type_));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, lock_key.serialize_meta_key(buf, sizeof(buf), pos));
    read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize_meta_key(buf, pos, read_pos));
    ASSERT_EQ(pos, read_pos);
    ASSERT_EQ(lock_key.convert_to_macro_id(), restored.convert_to_macro_id());
  }
}

#endif

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ivf_merge_object_id.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

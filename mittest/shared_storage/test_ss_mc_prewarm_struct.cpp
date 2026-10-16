/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "storage/shared_storage/ob_file_manager.h"
#undef private
#undef protected
#include "lib/utility/ob_test_util.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/compaction/ob_major_pre_warmer.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestSSMCPrewarmStruct : public ::testing::Test
{
public:
  TestSSMCPrewarmStruct() {}
  virtual ~TestSSMCPrewarmStruct() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void create_and_get_tablet(ObArenaAllocator &arena_allocator, const int64_t tablet_id, ObTabletHandle &tablet_handle);

public:
  static const int64_t TEST_LS_ID = 1001;
};

void TestSSMCPrewarmStruct::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());

  const ObLSID ls_id(TEST_LS_ID);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SS_PREWARM_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
}

void TestSSMCPrewarmStruct::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMCPrewarmStruct::SetUp()
{
}

void TestSSMCPrewarmStruct::TearDown()
{
}

void TestSSMCPrewarmStruct::create_and_get_tablet(
    ObArenaAllocator &arena_allocator,
    const int64_t tablet_id,
    ObTabletHandle &tablet_handle)
{
  const ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, tablet_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(tablet_id), table_schema, arena_allocator));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id), tablet_handle));
  ASSERT_NE(nullptr, tablet_handle.get_obj());
  ASSERT_EQ(tablet_id, tablet_handle.get_obj()->get_tablet_meta().tablet_id_.id());
}

TEST_F(TestSSMCPrewarmStruct, serialize)
{
  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id.offset_ = 100;
  logic_micro_id.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id.logic_macro_id_.logic_version_ = 100;
  logic_micro_id.logic_macro_id_.tablet_id_ = 200001;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(200001); // tablet_id
  macro_id.set_seq_id(1); // data_seq

  // 1. ObHotMicroInfo
  ObHotMicroInfo hot_micro_info;
  hot_micro_info.offset_ = 100;
  hot_micro_info.size_ = 4096;
  hot_micro_info.logic_micro_id_ = logic_micro_id;
  hot_micro_info.micro_crc_ = 100;
  int64_t size = hot_micro_info.get_serialize_size();
  char buf1[size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_micro_info.serialize(buf1, size, pos));
  ASSERT_EQ(size, pos);

  ObHotMicroInfo tmp_hot_micro_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_micro_info.deserialize(buf1, size, pos));
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info.micro_crc_);


  // 2. ObHotMacroInfo
  ObHotMacroInfo hot_macro_info;
  hot_macro_info.macro_id_ = macro_id;
  ASSERT_EQ(OB_SUCCESS, hot_macro_info.hot_micro_infos_.push_back(hot_micro_info));
  size = hot_macro_info.get_serialize_size();
  char buf2[size];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_macro_info.serialize(buf2, size, pos));
  ASSERT_EQ(size, pos);

  ObHotMacroInfo tmp_hot_macro_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_macro_info.deserialize(buf2, size, pos));
  ASSERT_EQ(hot_macro_info.macro_id_, tmp_hot_macro_info.macro_id_);
  ASSERT_EQ(1, tmp_hot_macro_info.hot_micro_infos_.count());
  ObHotMicroInfo &tmp_hot_micro_info_2 = tmp_hot_macro_info.hot_micro_infos_.at(0);
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info_2.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info_2.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info_2.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info_2.micro_crc_);


  // 3. ObHotTabletInfo
  ObHotTabletInfo hot_tablet_info;
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info.hot_macro_infos_.push_back(hot_macro_info));
  size = hot_tablet_info.get_serialize_size();
  char buf3[size];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info.serialize(buf3, size, pos));
  ASSERT_EQ(size, pos);

  ObHotTabletInfo tmp_hot_tablet_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_tablet_info.deserialize(buf3, size, pos));
  ASSERT_EQ(1, tmp_hot_tablet_info.hot_macro_infos_.count());
  ASSERT_EQ(1, tmp_hot_tablet_info.hot_macro_infos_.at(0).count());
  ObHotMicroInfo &tmp_hot_micro_info_3 = tmp_hot_tablet_info.hot_macro_infos_.at(0).hot_micro_infos_.at(0);
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info_3.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info_3.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info_3.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info_3.micro_crc_);
}

// ----------------------------------------------------------------------------
// Major compaction file prewarm 关闭后，不会创建任何 prewarm data/meta/index 文件
//
// 场景：使用真实 merge 参数初始化 ObMajorPreWarmerParam，并尝试创建 data/index prewarmer。
// 操作：确认 type 始终为 MEM_PRE_WARM，shared-storage 下两个 builder 均不创建 prewarmer，再完成空 Writer。
// 校验：data/index prewarmer 均为空，MAJOR_PREWARM_DATA/META 及其 INDEX 文件全部不存在。
// ----------------------------------------------------------------------------
TEST_F(TestSSMCPrewarmStruct, prewarm_file_generation_disabled)
{
  LOG_INFO("========== prewarm_file_generation_disabled begin ==========");
  const ObLSID ls_id = ObLSID(TEST_LS_ID);
  const int64_t tablet_id = 200003;
  const int64_t compaction_scn = ObTimeUtility::current_time_us();
  const SCN reorganization_scn = SCN::base_scn();
  ObHotTabletInfoWriter hot_tablet_info_writer(tablet_id, compaction_scn, reorganization_scn);
  compaction::ObMajorPreWarmerParam pre_warm_param(hot_tablet_info_writer);
  ASSERT_EQ(share::MEM_PRE_WARM, pre_warm_param.type_);
  OK(pre_warm_param.init(ls_id, ObTabletID(tablet_id)));
  ASSERT_EQ(share::MEM_PRE_WARM, pre_warm_param.type_);

  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TestPrewarm"));
  share::ObIPreWarmer *data_prewarmer = nullptr;
  OK(ObMacroBlockWriter::build_pre_warmer(
      pre_warm_param,
      false /*is_for_index*/,
      true /*is_user_tablet*/,
      allocator,
      data_prewarmer));
  ASSERT_EQ(nullptr, data_prewarmer);
  share::ObIPreWarmer *index_prewarmer = nullptr;
  OK(ObMacroBlockWriter::build_pre_warmer(
      pre_warm_param,
      true /*is_for_index*/,
      true /*is_user_tablet*/,
      allocator,
      index_prewarmer));
  ASSERT_EQ(nullptr, index_prewarmer);
  LOG_INFO("verify prewarmer creation is disabled", KP(data_prewarmer), KP(index_prewarmer));

  OK(hot_tablet_info_writer.complete());

  ObBackupIoAdapter io_adapter;
  ObDeviceConfig device_config;
  ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  ObBackupStorageInfo storage_info;
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigMgr::get_instance().get_device_config(
            ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config));
  ASSERT_EQ(OB_SUCCESS, get_storage_type_from_path(device_config.path_, type));
  ASSERT_EQ(OB_SUCCESS, storage_info.set(type, device_config.endpoint_, device_config.access_info_,
                                         device_config.extension_, OB_INVALID_DEST_ID));

  const ObStorageObjectType prewarm_types[] = {
    ObStorageObjectType::MAJOR_PREWARM_DATA,
    ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX,
    ObStorageObjectType::MAJOR_PREWARM_META,
    ObStorageObjectType::MAJOR_PREWARM_META_INDEX,
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(prewarm_types); ++i) {
    const ObStorageObjectType object_type = prewarm_types[i];
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(
        object_type, tablet_id, compaction_scn, reorganization_scn.get_val_for_sql());
    OK(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle));
    ObPathContext ctx;
    OK(ctx.set_file_ctx(object_handle.get_macro_id(),
        0 /*ls_epoch_id*/, false /*is_local_cache*/, GCONF.observer_id));
    bool is_file_exist = false;
    OK(io_adapter.is_exist(ctx.get_path(), &storage_info, is_file_exist));
    LOG_INFO("verify prewarm file does not exist", K(i), K(object_type), K(is_file_exist));
    ASSERT_FALSE(is_file_exist);
  }
  LOG_INFO("========== prewarm_file_generation_disabled end ==========");
}

// Verify major compaction prewarm writes the actual object size into local macro cache when the object is
// smaller than the standard macro size.
TEST_F(TestSSMCPrewarmStruct, prewarm_local_macro_cache_actual_size)
{
  const int64_t actual_macro_size = 1 * 1024 * 1024;
  const int64_t standard_macro_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  const int64_t test_macro_second_id = 777;
  ObSSTable fake_sstable;
  ObTabletHandle tablet_handle;
  ObArenaAllocator tablet_allocator;
  create_and_get_tablet(tablet_allocator, test_macro_second_id, tablet_handle);

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(test_macro_second_id);
  const uint64_t tenant_id = MTL_ID();
  const int64_t effective_tablet_id = macro_id.second_id();
  ObArenaAllocator allocator;

  // 1. write 1MB data to object storage
  char *write_buf = static_cast<char *>(allocator.alloc(actual_macro_size));
  ASSERT_NE(nullptr, write_buf);
  MEMSET(write_buf, 'P', actual_macro_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = actual_macro_size;
  write_info.io_timeout_ms_ = 5 * 1000;
  write_info.mtl_tenant_id_ = tenant_id;
  ObStorageObjectHandle write_handle;
  ASSERT_EQ(OB_SUCCESS, write_handle.set_macro_block_id(macro_id));
  ObSSObjectStorageWriter object_storage_writer;
  ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_info, write_handle));
  ASSERT_EQ(OB_SUCCESS, write_handle.wait());

  // 2. verify object storage file is exist, and local file is not exist
  bool is_exist = false;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  
  // 3. read with standard_macro_size from object storage
  char *read_buf = static_cast<char *>(allocator.alloc(standard_macro_size));
  ASSERT_NE(nullptr, read_buf);
  MEMSET(read_buf, '\0', standard_macro_size);
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = 0;
  read_info.size_ = standard_macro_size;
  read_info.io_desc_.set_wait_event(1);
  read_info.buf_ = read_buf;
  read_info.mtl_tenant_id_ = tenant_id;
  read_info.set_ls_epoch_id(0);
  read_info.set_effective_tablet_id(ObTabletID(effective_tablet_id));
  read_info.io_timeout_ms_ = 5 * 1000;
  ObStorageObjectHandle read_handle;

  ObSSObjectStorageReader object_storage_reader;
  ASSERT_EQ(OB_SUCCESS, object_storage_reader.aio_read(read_info, read_handle));
  ASSERT_EQ(OB_SUCCESS, read_handle.wait());

  // 4. write to local macro cache through the major compaction prewarm path
  ObHotTabletInfoReader hot_tablet_info_reader(1/*compaction_scn*/, SCN::base_scn(), tablet_handle, fake_sstable);
  hot_tablet_info_reader.tablet_id_ = effective_tablet_id;
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_reader.async_write_hot_policy_macro_into_local_cache(read_handle));
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_reader.wait_all_write_handles());

  // 5. read from local macro cache and verify actual size
  char *get_buf = static_cast<char *>(allocator.alloc(standard_macro_size));
  ASSERT_NE(nullptr, get_buf);
  ObStorageObjectReadInfo local_read_info;
  local_read_info.macro_block_id_ = macro_id;
  local_read_info.offset_ = 0;
  local_read_info.size_ = standard_macro_size;
  local_read_info.io_desc_.set_wait_event(1);
  local_read_info.buf_ = get_buf;
  local_read_info.mtl_tenant_id_ = tenant_id;
  local_read_info.set_ls_epoch_id(0);
  local_read_info.set_effective_tablet_id(ObTabletID(effective_tablet_id));
  local_read_info.io_timeout_ms_ = 5 * 1000;
  ObSSLocalCacheReader local_cache_reader;
  ObStorageObjectHandle obj_handle;
  ASSERT_EQ(OB_SUCCESS, local_cache_reader.aio_read(local_read_info, obj_handle));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(actual_macro_size, obj_handle.get_data_size());
  for (int64_t i = 0; i < actual_macro_size; ++i) {
    ASSERT_EQ('P', obj_handle.get_buffer()[i]) << "data mismatch at offset " << i;
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_mc_prewarm_struct.log*");
  OB_LOGGER.set_file_name("test_ss_mc_prewarm_struct.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

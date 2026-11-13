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
#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "storage/shared_storage/ob_file_manager.h"
#undef private
#undef protected
#include "lib/utility/ob_test_util.h"
#include "storage/blocksstable/ob_sstable.h"

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

  // 3. 通过现有宏块预热入口写入本地缓存。
  bool is_stop = false;
  ObSSTableMacroPrewarmer prewarmer(
      &fake_sstable, is_stop, ObSSLocalCachePrewarmType::MAJOR_COMPACTION);
  OK(prewarmer.prewarm_single_macro(macro_id, effective_tablet_id));

  // 4. read from local macro cache and verify actual size
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

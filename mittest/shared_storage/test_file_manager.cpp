/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <atomic>
#include <gmock/gmock.h>
#include <thread>
#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "lib/literals/ob_literals.h"
#include "lib/utility/utility.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_ss_cluster_info.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

#undef private
#undef protected
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

#define PRINT_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE)          \
  {                                                               \
    file_id.set_storage_object_type(OBJ_TYPE);                    \
    ret = ctx.set_file_ctx(file_id, ls_epoch_id, IS_LOCAL_CACHE, GCONF.observer_id, ls_id); \
    LOG_INFO("file id to path", KR(ret), K(file_id), K(ls_epoch_id), K(IS_LOCAL_CACHE), K(ctx.get_path())); \
  }

#define PRINT_MACRO_ID_PARENT_DIR_PATH(OBJ_TYPE) \
  {                                              \
    file_id.set_storage_object_type(OBJ_TYPE);   \
    ret = ObFileHelper::get_file_parent_dir(      \
        dir_path, common::MAX_PATH_SIZE, file_id, ls_epoch_id, ls_id); \
    LOG_INFO("file id's parent dir path", KR(ret), K(file_id), K(ls_epoch_id), K(dir_path)); \
  }

#define CHECK_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE, EXPECTED_RET, EXPECTED_PATH)        \
  {                                                                                          \
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));   \
    ASSERT_EQ(EXPECTED_RET, ctx.set_file_ctx(                                               \
        file_id, ls_epoch_id, IS_LOCAL_CACHE, GCONF.observer_id, ls_id));                   \
    if (OB_SUCCESS == EXPECTED_RET) {                                                        \
      expected_path[0] = '\0';                                                               \
      if (IS_LOCAL_CACHE) {                                                                  \
        ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", \
                                              OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
      } else {                                                                               \
        ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", object_storage_root_dir, EXPECTED_PATH)); \
      }                                                                                      \
      ASSERT_EQ(0, STRCMP(ctx.get_path(), expected_path)) << ctx.get_path() << "\n" << expected_path; \
    }                                                                                        \
  }

#define CHECK_MACRO_ID_PARENT_DIR(OBJ_TYPE, EXPECTED_RET, EXPECTED_PATH)                            \
  {                                                                                                 \
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));          \
    dir_path[0] = '\0';                                                                             \
    ASSERT_EQ(EXPECTED_RET, ObFileHelper::get_file_parent_dir(                              \
        dir_path, common::MAX_PATH_SIZE, file_id, ls_epoch_id, ls_id));                     \
    if (OB_SUCCESS == EXPECTED_RET) {                                                               \
      expected_dir_path[0] = '\0';                                                                  \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_dir_path, common::MAX_PATH_SIZE, "%s/%s",      \
                                            OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
      ASSERT_EQ(0, STRCMP(dir_path, expected_dir_path))<< dir_path << "\n" << expected_dir_path;    \
    }                                                                                               \
  }

#define CHECK_INC_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE, INNER_TABLET, EXPECTED_PATH)           \
  {                                                                                                 \
    inc_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));      \
    expected_path[0] = '\0';                                                                        \
    ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(inc_file_id, ls_epoch_id, IS_LOCAL_CACHE,                 \
                                           GCONF.observer_id));                                      \
    if (IS_LOCAL_CACHE) {                                                                           \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s",          \
                                            OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
    } else {                                                                                        \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", object_storage_root_dir, EXPECTED_PATH)); \
    }                                                                                               \
    EXPECT_EQ(0, STRCMP(ctx.get_path(), expected_path)) << ctx.get_path() << "\n" << expected_path; \
  }

class TestFileDelOp : public share::ObScanDirOp
{
public:
  TestFileDelOp(const char *dir = nullptr) {}
  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid entry", KR(ret));
    } else {
      char full_path[common::MAX_PATH_SIZE] = { 0 };
      int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
      if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("file name too long", KR(ret), K_(dir), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del file", KR(ret), K(full_path));
        }
      }
    }
    return ret;
  }
};

class TestDirDelOp : public share::ObScanDirOp
{
public:
  TestDirDelOp(const char *dir = nullptr) {}
  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid entry", KR(ret));
    } else {
      char full_path[common::MAX_PATH_SIZE] = { 0 };
      int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
      if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("file name too long", KR(ret), K_(dir), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del dir", KR(ret), K(full_path));
        }
      }
    }
    return ret;
  }
};

class TestFileManager : public ::testing::Test
{
public:
  TestFileManager() = default;
  virtual ~TestFileManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void get_macro_block_scatter_dir_size(int64_t &scatter_dir_size);
  void get_ls_id_dir_size(const int64_t ls_id,
                          const int64_t ls_epoch_id,
                          int64_t &dir_size);
  int prepare_calibration_macro(const uint64_t tablet_id, MacroBlockId &macro_id);
  int write_fail_and_clean_tmp_seq_file(
      const uint64_t tablet_id,
      const ObSSMacroCacheType expected_cache_type,
      const char *scenario_name,
      const int64_t clean_map_safe_del_time_s,
      const int64_t write_io_size);
};

void TestFileManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  // 本地 DEBUG_SYNC action 也要求 GDS 已注入 RPC proxy。
  GDS.set_rpc_proxy(GCTX.rs_rpc_proxy_);
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  ObTenantDiskSpaceManager *tnt_disk_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_mgr);
  // disable persist disk space task to avoid interfere with belowing tests
  tnt_disk_mgr->persist_disk_space_task_.is_inited_ = false;
}

void TestFileManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestFileManager::get_macro_block_scatter_dir_size(int64_t &scatter_dir_size)
{
  // Calculate scatter directory sizes for:
  // 1. private data/meta macro (tablet_data/00-99)
  // 2. shared mini/minor/major/inc_major macro (each has 00-FF scatter dirs)
  // 3. shared_tablet_sub_meta (00-FF scatter dirs)
  // 4. external_table_file (00-FF scatter dirs)
  // Note: Each shared macro type directory is counted separately because directory sizes
  // may differ across filesystems (e.g., XFS vs ext4) and some types have additional
  // subdirectories like /ls.
  scatter_dir_size = 0;
  ObIODFileStat statbuf;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_data_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID()));
  for (int64_t i = 0; i < ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02ld", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  // Separately count scatter dirs for mini, minor, major, and inc_major to handle
  // filesystem-specific directory size differences (e.g., XFS vs ext4)
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINI));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINOR));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MAJOR));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::INC_MAJOR));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_sub_meta_cache_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID()));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_external_table_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID()));
  for (int64_t i = 0; i < ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
}

void TestFileManager::get_ls_id_dir_size(
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    int64_t &dir_size)
{
  dir_size = 0;
  ObIODFileStat statbuf;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_dir(
      dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  dir_size += statbuf.size_;
  dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_ls_id_dir(
      dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  dir_size += statbuf.size_;
  dir_path[0] = '\0';

  int64_t scatter_dir_size = 0;
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_dir(
      dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  for (int64_t i = 0; i < ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02ld", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_size += scatter_dir_size;
}

// 写 .tmp.seq 并注入失败，污染 meta map 后清理，验证按写入时 cache_type 扣减空间。
int TestFileManager::write_fail_and_clean_tmp_seq_file(
    const uint64_t tablet_id,
    const ObSSMacroCacheType expected_cache_type,
    const char *scenario_name,
    const int64_t clean_map_safe_del_time_s,
    const int64_t write_io_size)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSMacroCacheStat hot_stat;
  ObSSMacroCacheStat macro_stat;
  int64_t hot_used_before = 0;
  int64_t macro_used_before = 0;
  MacroBlockId macro_id;
  ObStorageObjectHandle write_object_handle;
  char *write_buf = nullptr;

  // 校验输入，准备目录，避免目录空间混入基线。
  LOG_INFO("start test scenario", K(scenario_name), K(tablet_id), K(expected_cache_type));
  if (OB_ISNULL(tenant_file_mgr) || OB_ISNULL(tenant_disk_space_mgr) || OB_ISNULL(macro_cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant module is null", KR(ret), KP(tenant_file_mgr), KP(tenant_disk_space_mgr), KP(macro_cache_mgr));
  } else if (OB_ISNULL(scenario_name) || OB_UNLIKELY(ObTabletID::INVALID_TABLET_ID == tablet_id
      || write_io_size <= 0 || clean_map_safe_del_time_s < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), KP(scenario_name), K(write_io_size),
        K(clean_map_safe_del_time_s));
  } else if (OB_UNLIKELY(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK != expected_cache_type
      && ObSSMacroCacheType::MACRO_BLOCK != expected_cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected macro cache type", KR(ret), K(expected_cache_type));
  } else {
    // 确保散列目录存在，前置用例可能清理了空散列目录导致 create_tablet_data_xxx 失败。
    char scatter_dir[common::MAX_PATH_SIZE] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_scatter_dir(
        scatter_dir, sizeof(scatter_dir), MTL_ID(), MTL_EPOCH_ID(), tablet_id))) {
      LOG_WARN("fail to get scatter dir", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ObDirManager::create_dir(
        scatter_dir, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
      if (OB_FILE_OR_DIRECTORY_EXIST != ret) {
        LOG_WARN("fail to create scatter dir", KR(ret), K(scatter_dir));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (FAILEDx(OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/))) {
    LOG_WARN("fail to create tablet data transfer epoch dir", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_stat))) {
    LOG_WARN("fail to get hot tablet macro cache stat", KR(ret));
  } else if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
      ObSSMacroCacheType::MACRO_BLOCK, macro_stat))) {
    LOG_WARN("fail to get macro cache stat", KR(ret));
  } else {
    hot_used_before = hot_stat.used_;
    macro_used_before = macro_stat.used_;
  }

  // 构造私有宏块，写入 .tmp.seq 并注入 rename 失败。
  if (OB_SUCC(ret)) {
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(0);
    macro_id.set_tenant_seq(100000);
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid macro id", KR(ret), K(macro_id));
    } else if (OB_FAIL(write_object_handle.set_macro_block_id(macro_id))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
    } else if (OB_ISNULL(write_buf = static_cast<char *>(ob_malloc(write_io_size, "TmpSeqTest")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc write buffer", KR(ret), K(write_io_size));
    } else {
      MEMSET(write_buf, 'a', write_io_size);
      ObStorageObjectWriteInfo write_file_info;
      write_file_info.buffer_ = write_buf;
      write_file_info.offset_ = 0;
      write_file_info.size_ = write_io_size;
      write_file_info.io_desc_.set_wait_event(1);
      write_file_info.mtl_tenant_id_ = MTL_ID();
      write_file_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
      write_file_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;

      // 注入一次 rename 失败，模拟 .tmp.seq 残留。
      TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0/*occur*/, 1/*trigger_freq*/);
      if (OB_FAIL(ObSSObjectAccessUtil::async_write_file(write_file_info, write_object_handle))) {
        LOG_WARN("fail to async write file", KR(ret), K(macro_id));
      }
    }
  }

  // 校验 alloc 使用写入时的 cache_type 记账。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_stat))) {
      LOG_WARN("fail to get hot tablet macro cache stat after alloc", KR(ret));
    } else if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
        ObSSMacroCacheType::MACRO_BLOCK, macro_stat))) {
      LOG_WARN("fail to get macro cache stat after alloc", KR(ret));
    } else if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == expected_cache_type
        && (hot_used_before + write_io_size != hot_stat.used_ || macro_used_before != macro_stat.used_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected used size after hot macro alloc", KR(ret), K(hot_used_before),
          K(macro_used_before), K(hot_stat), K(macro_stat), K(write_io_size));
    } else if (ObSSMacroCacheType::MACRO_BLOCK == expected_cache_type
        && (hot_used_before != hot_stat.used_ || macro_used_before + write_io_size != macro_stat.used_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected used size after macro alloc", KR(ret), K(hot_used_before),
          K(macro_used_before), K(hot_stat), K(macro_stat), K(write_io_size));
    }
  }

  // 校验 active map 记录了写入时的 cache_type。
  ObAtomicWriteFileInfo atomic_write_file;
  if (FAILEDx(tenant_file_mgr->get_from_atomic_write_file_map(
      false/*is_clean*/, macro_id, atomic_write_file))) {
    LOG_WARN("fail to get atomic write file from active map", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(expected_cache_type != atomic_write_file.cache_type_
      || write_io_size != static_cast<int64_t>(atomic_write_file.size_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected atomic write file info", KR(ret), K(expected_cache_type),
        K(write_io_size), K(atomic_write_file));
  }

  // 等待写失败完成，确认条目迁移到 clean map。
  if (OB_SUCC(ret)) {
    const int wait_ret = write_object_handle.wait();
    TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0/*occur*/, 0/*trigger_freq*/);
    if (OB_SUCCESS == wait_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write should fail for tmp seq file clean test", KR(ret), K(wait_ret), K(macro_id));
    } else {
      int tmp_ret = tenant_file_mgr->get_from_atomic_write_file_map(
          false/*is_clean*/, macro_id, atomic_write_file);
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        LOG_WARN("unexpected active atomic write file map state", KR(ret), K(tmp_ret), K(macro_id));
      } else if (OB_FAIL(tenant_file_mgr->get_from_atomic_write_file_map(
          true/*is_clean*/, macro_id, atomic_write_file))) {
        LOG_WARN("fail to get atomic write file from clean map", KR(ret), K(macro_id));
      } else if (OB_UNLIKELY(expected_cache_type != atomic_write_file.cache_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cache type in clean map", KR(ret), K(expected_cache_type), K(atomic_write_file));
      }
    }
    write_object_handle.reset();
  }

  // 伪造 meta map，并改成会导致旧逻辑错扣的 cache_type。
  if (OB_SUCC(ret)) {
    ObSSMacroCacheInfo cache_info(tablet_id, static_cast<uint32_t>(write_io_size),
        expected_cache_type, true/*is_write_cache*/);
    ObSSMacroCacheMetaHandle meta_handle;
    ObSSMacroCacheMetaHandle meta_map_handle;
    if (OB_FAIL(macro_cache_mgr->construct_meta_handle(macro_id, cache_info, meta_handle))) {
      LOG_WARN("fail to construct macro cache meta handle", KR(ret), K(macro_id), K(cache_info));
    } else if (OB_FAIL(macro_cache_mgr->meta_map_.set_refactored(macro_id, meta_handle))) {
      LOG_WARN("fail to set macro cache meta", KR(ret), K(macro_id), K(cache_info));
    } else if (OB_FAIL(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_map_handle))) {
      LOG_WARN("fail to get macro cache meta", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(!meta_map_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid macro cache meta handle", KR(ret), K(macro_id), K(meta_map_handle));
    } else {
      const ObSSMacroCacheType meta_cache_type =
          (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == expected_cache_type
          ? ObSSMacroCacheType::MACRO_BLOCK
          : ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
      meta_map_handle()->set_cache_type(meta_cache_type);
      LOG_INFO("change meta cache type before clean", K(macro_id), K(expected_cache_type), K(meta_cache_type));
    }
  }

  // 触发 clean task，删除 .tmp.seq 并释放空间。
  if (OB_SUCC(ret)) {
    sleep(clean_map_safe_del_time_s + 3);
    int tmp_ret = tenant_file_mgr->tmp_seq_file_clean_task_.init();
    if (OB_SUCCESS != tmp_ret && OB_INIT_TWICE != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to init tmp seq file clean task", KR(ret));
    } else {
      tenant_file_mgr->tmp_seq_file_clean_task_.runTimerTask();
      tmp_ret = tenant_file_mgr->get_from_atomic_write_file_map(
          true/*is_clean*/, macro_id, atomic_write_file);
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        LOG_WARN("unexpected clean atomic write file map state", KR(ret), K(tmp_ret), K(macro_id));
      }
    }
  }

  // 清理伪造的 meta map，避免影响后续用例。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_cache_mgr->meta_map_.erase_refactored(macro_id))) {
      LOG_WARN("fail to erase fake macro cache meta", KR(ret), K(macro_id));
    }
  }

  // 校验 free 按写入时的 cache_type 扣减回基线。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_stat))) {
      LOG_WARN("fail to get hot tablet macro cache stat after clean", KR(ret));
    } else if (OB_FAIL(tenant_disk_space_mgr->get_macro_cache_stat(
        ObSSMacroCacheType::MACRO_BLOCK, macro_stat))) {
      LOG_WARN("fail to get macro cache stat after clean", KR(ret));
    } else if (OB_UNLIKELY(hot_used_before != hot_stat.used_ || macro_used_before != macro_stat.used_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected used size after clean", KR(ret), K(hot_used_before),
          K(macro_used_before), K(hot_stat), K(macro_stat), K(expected_cache_type));
    }
  }

  // 兜底复位故障注入和临时资源。
  // 重复复位故障注入无副作用。
  TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0/*occur*/, 0/*trigger_freq*/);
  if (OB_NOT_NULL(macro_cache_mgr) && macro_id.is_valid()) {
    int tmp_ret = macro_cache_mgr->meta_map_.erase_refactored(macro_id);
    if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
      LOG_WARN("fail to erase fake macro cache meta", KR(tmp_ret), K(macro_id));
    }
  }
  if (OB_NOT_NULL(write_buf)) {
    ob_free(write_buf);
    write_buf = nullptr;
  }
  return ret;
}

/**
 * 测试模块：
 *   SHARED_TABLET_SUB_META 对象存储路径的格式化与解析。
 * 测试步骤：
 *   1. 使用有代表性的 seq 值分别构造普通 tablet 和内部 tablet 的宏块 ID。
 *   2. 校验相对路径和完整远端路径中的 seq 均按 10 位补零。
 *   3. 将远端路径反解析为宏块 ID，校验所有字段均可正确回环。
 *   4. 校验跨越 9 -> 10 时，对象 key 字典序与 seq 数值顺序一致。
 */
TEST(StorageObjectTypePathTest, shared_tablet_sub_meta_object_storage_seq_padding)
{
  struct TabletPathCase
  {
    int64_t tablet_id_;
    bool is_inner_tablet_;
    int64_t ls_id_;
    int64_t reorganization_scn_;
    const char *relative_path_prefix_;
  };
  struct SeqPathCase
  {
    int64_t seq_id_;
    const char *expected_seq_;
  };

  const TabletPathCase tablet_cases[] = {
      {200001, false, 0, 42, "tablet/200001/42/meta/op7seq"},
      {static_cast<int64_t>(ObTabletID::LS_TX_DATA_TABLET_ID),
       true, 1002, 0, "ls/1002/TX_DATA/meta/op7seq"},
  };
  const SeqPathCase seq_cases[] = {
      {0, "0000000000"},
      {9, "0000000009"},
      {10, "0000000010"},
      {4294967295LL, "4294967295"}, // UINT32_MAX boundary used by sub-meta data_seq.
  };
  const char *object_storage_root = "oss://test-bucket";

  for (const TabletPathCase &tablet_case : tablet_cases) {
    char relative_paths[ARRAYSIZEOF(seq_cases)][common::MAX_PATH_SIZE] = {{0}};
    for (int64_t i = 0; i < ARRAYSIZEOF(seq_cases); ++i) {
      const SeqPathCase &seq_case = seq_cases[i];
      MacroBlockId macro_id;
      macro_id.set_storage_object_type(
          static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
      macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
      macro_id.set_second_id(tablet_case.tablet_id_);
      macro_id.set_ss_third_id(
          tablet_case.is_inner_tablet_, tablet_case.ls_id_, tablet_case.reorganization_scn_);
      macro_id.set_op_id(7);
      macro_id.set_seq_id(seq_case.seq_id_);

      char expected_relative_path[common::MAX_PATH_SIZE] = {0};
      int64_t expected_relative_pos = 0;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(
          expected_relative_path, sizeof(expected_relative_path), expected_relative_pos,
          "%s%s.T21", tablet_case.relative_path_prefix_, seq_case.expected_seq_));

      int64_t relative_pos = 0;
      ASSERT_EQ(OB_SUCCESS, SSObjUtil::to_relative_remote_path_format(
          macro_id, relative_paths[i], sizeof(relative_paths[i]), relative_pos));
      ASSERT_STREQ(expected_relative_path, relative_paths[i]);
      ASSERT_EQ(expected_relative_pos, relative_pos);

      char expected_remote_path[common::MAX_PATH_SIZE] = {0};
      int64_t expected_remote_pos = 0;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(
          expected_remote_path, sizeof(expected_remote_path), expected_remote_pos,
          "%s/cluster_1/tenant_1/%s", object_storage_root, expected_relative_path));

      char remote_path[common::MAX_PATH_SIZE] = {0};
      int64_t remote_pos = 0;
      ASSERT_EQ(OB_SUCCESS, SSObjUtil::to_remote_path_format(
          macro_id, remote_path, sizeof(remote_path), remote_pos,
          object_storage_root, 1/*cluster_id*/, 1/*tenant_id*/, 0/*tenant_epoch_id*/,
          1/*server_id*/, tablet_case.ls_id_, 0/*ls_epoch_id*/));
      ASSERT_STREQ(expected_remote_path, remote_path);
      ASSERT_EQ(expected_remote_pos, remote_pos);

      MacroBlockId decoded_id;
      ASSERT_EQ(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(
          ObStorageObjectType::SHARED_TABLET_SUB_META, remote_path, decoded_id));
      ASSERT_EQ(macro_id.storage_object_type(), decoded_id.storage_object_type());
      ASSERT_EQ(ObMacroBlockIdLocation::OBJECT_STORAGE, decoded_id.location());
      ASSERT_EQ(macro_id.second_id(), decoded_id.second_id());
      ASSERT_EQ(macro_id.third_id(), decoded_id.third_id());
      ASSERT_EQ(macro_id.op_id(), decoded_id.op_id());
      ASSERT_EQ(macro_id.seq_id(), decoded_id.seq_id());
    }

    // Fixed-width seq keeps object keys ordered numerically across the 9 -> 10 boundary.
    ASSERT_LT(STRCMP(relative_paths[1], relative_paths[2]), 0);
  }
}

TEST_F(TestFileManager, test_path_convert)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  ObPathContext ctx;
  char expected_path[common::MAX_PATH_SIZE] = {0};
  bool is_local_cache = true;
  int64_t ls_id = 3;
  int64_t ls_epoch_id = 4;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(0);
  file_id.set_op_id(0);
  file_id.set_tenant_seq(5);
  char *object_storage_root_dir = nullptr;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir));

  MacroBlockId inc_file_id;
  inc_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  inc_file_id.set_second_id(3); // tablet_id
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);

  // each object type and print out the path log
  for (uint64_t obj_id = static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO); obj_id < static_cast<uint64_t>(ObStorageObjectType::MAX); ++obj_id) {
    PRINT_MACRO_ID_TO_PATH(obj_id, true/*is_in_local*/);
    PRINT_MACRO_ID_TO_PATH(obj_id, false/*is_in_local*/);
  }

  // 0.PRIVATE_DATA_MACRO
  // tenant_id_epoch_id/tablet_data/tablet_id/reorganization_scn/data/server_id_op_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_DATA_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/tablet_data/03/3/0/data/svr1op0seq5.T0");
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/reorganization_scn/data/server_id_op_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_DATA_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tablet_data/3/0/data/svr1op0seq5.T0");

  // 1.PRIVATE_META_MACRO
  // tenant_id_epoch_id/tablet_data/tablet_id/reorganization_scn/meta/server_id_op_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_META_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/tablet_data/03/3/0/meta/svr1op0seq5.T1");
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/reorganization_scn/meta/server_id_op_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_META_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tablet_data/3/0/meta/svr1op0seq5.T1");

  // 2. SHARED_MINI_DATA_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_CTX_op1seq1.T2");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/mini/sstable/op_1/data/seq1.T2");
  // Additional SHARED_MINI path cases.
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/4E/t200001rs0op1seq1.T2");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/mini/sstable/op_1/data/seq1.T2");

  // Additional SHARED_MINI path cases.
  inc_file_id.set_op_id(7891);
  inc_file_id.set_seq_id(211);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 1002/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_DATA_op7891seq211.T2");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/mini/sstable/op_7891/data/seq211.T2");
  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 1002/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/60/t200002rs1002op7891seq211.T2");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200002/1002/mini/sstable/op_7891/data/seq211.T2");

  // 3. SHARED_MINI_META_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_CTX_op1seq1.T3");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/mini/sstable/op_1/meta/seq1.T3");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/8A/t200001rs0op1seq1.T3");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/mini/sstable/op_1/meta/seq1.T3");

  // Additional SHARED_MINI path cases.
  inc_file_id.set_op_id(7892);
  inc_file_id.set_seq_id(212);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 1002/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_DATA_op7892seq212.T3");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/mini/sstable/op_7892/meta/seq212.T3");
  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 1002/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/61/t200002rs1002op7892seq212.T3");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200002/1002/mini/sstable/op_7892/meta/seq212.T3");

  // 4. SHARED_MINOR_DATA_MACRO
  // inner tablet 49403
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1001/TX_LOCK_op1seq1.T4");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_LOCK/minor/sstable/op_1/data/seq1.T4");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/44/t200001rs0op1seq1.T4");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/minor/sstable/op_1/data/seq1.T4");

  // Additional SHARED_MINOR path cases.
  inc_file_id.set_op_id(7893);
  inc_file_id.set_seq_id(213);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 1/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1001/TX_DATA_op7893seq213.T4");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/minor/sstable/op_7893/data/seq213.T4");
  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1005/*ls_id*/, 2001/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/EA/t200002rs2001op7893seq213.T4");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200002/2001/minor/sstable/op_7893/data/seq213.T4");

  // 5. SHARED_MINOR_META_MACRO
  // inner tablet 49402
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  inc_file_id.set_second_id(49402);// TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1001/TX_DATA_op1seq1.T5");
  // cluster_id/tenant_id/tablet_name/minor/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/minor/sstable/op_1/meta/seq1.T5");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/31/t200001rs0op1seq1.T5");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/minor/sstable/op_1/meta/seq1.T5");

  // Additional SHARED_MINOR path cases.
  inc_file_id.set_op_id(7894);
  inc_file_id.set_seq_id(214);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1007/*ls_id*/, 1/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1007/TX_DATA_op7894seq214.T5");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1007/TX_DATA/minor/sstable/op_7894/meta/seq214.T5");
  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1005/*ls_id*/, 2002/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/E0/t200002rs2002op7894seq214.T5");
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200002/2002/minor/sstable/op_7894/meta/seq214.T5");

  // 6.SHARED_MAJOR_DATA_MACRO
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(5242880);
  file_id.set_major_snapshot(0); // legacy five-slot major
  file_id.set_seq_id(2);
  // tenant_id_epoch_id/shared_major_macro_cache/tablet_id_reorganization_scn_cg_id_macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_DATA_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/shared_major_macro_cache/FE/t3rs5242880cg0op0seq2.T6");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/data/macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_DATA_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/sstable/cg_0/op_0/data/seq2.T6");

  // 7.SHARED_MAJOR_META_MACRO
  // tenant_id_epoch_id/shared_major_macro_cache/tablet_id_reorganization_scn_cg_id_macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_META_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/shared_major_macro_cache/64/t3rs5242880cg0op0seq2.T7");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/meta/macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_META_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/sstable/cg_0/op_0/meta/seq2.T7");

  // 8.TMP_FILE
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);
  // tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/segment_id
  CHECK_MACRO_ID_TO_PATH(TMP_FILE, true/*is_in_local*/, OB_SUCCESS, "1_0/tmp_data/000/3/seg2.T8");

  expected_path[0] = '\0';
  // tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/segment_id.deleted
  ASSERT_EQ(OB_SUCCESS, ctx.set_logical_delete_ctx(file_id, ls_epoch_id, GCONF.observer_id));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/1_0/%s/000/3/seg2.T8.deleted",
                                        OB_DIR_MGR.get_local_cache_root_dir(), TMP_DATA_DIR_STR));
  ASSERT_EQ(0, STRCMP(ctx.get_path(), expected_path));

  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id
  CHECK_MACRO_ID_TO_PATH(TMP_FILE, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tmp_data/3/seg2.T8");

  // 9.SERVER_META
  // super_block
  CHECK_MACRO_ID_TO_PATH(SERVER_META, true/*is_in_local*/, OB_SUCCESS, "SERVER_META.T9");
  CHECK_MACRO_ID_TO_PATH(SERVER_META, false/*is_in_local*/, OB_NOT_SUPPORTED, "");

  // 10.PRIVATE_TABLET_META
  file_id.set_second_id(3); // tablet_id
  file_id.set_reorganization_scn(2);
  file_id.set_meta_version_id(5);
  file_id.set_fifth_id(0);
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/reorganization_scn/meta_version
  CHECK_MACRO_ID_TO_PATH(PRIVATE_TABLET_META, true/*is_in_local*/, OB_SUCCESS,
      "1_0/ls/3_4/tablet_meta/03/3/2/ver5.T10");
  CHECK_MACRO_ID_TO_PATH(PRIVATE_TABLET_META, false/*is_in_local*/, OB_SUCCESS,
      "cluster_1/server_1/1_0/ls/3_4/tablet_meta/3/2/ver5.T10");
  file_id.set_reorganization_scn(5242880);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);

  // 13. MAJOR_PREWARM_DATA
  // cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T13");

  // 14. MAJOR_PREWARM_DATA_INDEX
  // cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA_INDEX, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA_INDEX, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T14");

  // 15. MAJOR_PREWARM_META
  // cluster_id/tenant_id/tablet/tablet_id/major/scn_id_compaction_scn_prewarm_data
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T15");

  // 16. MAJOR_PREWARM_META_INDEX
  // cluster_id/tenant_id/tablet/tablet_id/major/scn_id_compaction_scn_prewarm_data
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META_INDEX, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META_INDEX, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T16");

  // 17.TENANT_DISK_SPACE_META
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);
  // tenant_id_epoch_id/tenant_disk_space_meta
  CHECK_MACRO_ID_TO_PATH(TENANT_DISK_SPACE_META, true/*is_in_local*/, OB_SUCCESS, "3_2/TENANT_DISK_SPACE_META.T17");
  CHECK_MACRO_ID_TO_PATH(TENANT_DISK_SPACE_META, false/*is_in_local*/, OB_NOT_SUPPORTED, "");

  // 18. IS_SHARED_TENANT_DELETED
  file_id.set_third_id(0);
  file_id.set_fourth_id(0);
  file_id.set_fifth_id(0);
  // cluster_id/tenant_id/is_shared_tenant_deleted
  CHECK_MACRO_ID_TO_PATH(IS_SHARED_TENANT_DELETED, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(IS_SHARED_TENANT_DELETED, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_3/IS_SHARED_TENANT_DELETED.T18");

  file_id.set_fourth_id(2);
  file_id.set_fifth_id(5);
  // 19. UNSEALED_REMOTE_SEG_FILE
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id_valid_length
  CHECK_MACRO_ID_TO_PATH(UNSEALED_REMOTE_SEG_FILE, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(UNSEALED_REMOTE_SEG_FILE, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tmp_data/3/seg2len5.T19");

  // --- Atomic protocol files -- //
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);

  // 21. SHARED_TABLET_SUB_META
  // local cache test: user_tablet : tenant_id_epoch_id/shared_tablet_sub_meta_cache/scatter_id/t%ldrs%ldop%ldseq%ld
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_tablet_sub_meta_cache/C6/t200001rs0op1seq1.T21");
  // local cache test: inner_tablet : tenant_id_epoch_id/shared_tablet_sub_meta_cache/ls/ls_id/tablet_name_op%ldseq%ld
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_tablet_sub_meta_cache/ls/1001/TX_CTX_op1seq1.T21");
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_tablet_sub_meta_cache/ls/1001/TX_DATA_op1seq1.T21");
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_tablet_sub_meta_cache/ls/1001/TX_LOCK_op1seq1.T21");

  // remote path test: user_tablet : cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/op_id_data_seq
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/meta/op1seq0000000001.T21");
  // remote path test: inner_tablet : cluster_id/tenant_id/ls/ls_id/tablet_name/meta/op_id_data_seq
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/meta/op1seq0000000001.T21");
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/meta/op1seq0000000001.T21");
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_LOCK/meta/op1seq0000000001.T21");

  // 20. SHARED_TABLET_META
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(5);
  file_id.set_meta_scn(2);
  file_id.set_seq_id(0);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/meta_scn%ldtablet_meta.T20
  CHECK_MACRO_ID_TO_PATH(SHARED_TABLET_META, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(SHARED_TABLET_META, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5/meta/scn00000000000000000002.T20");

  // 30. SHARED_LS_CKPT
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_meta_scn(2);
  file_id.set_seq_id(0);
  // cluster_id/tenant_id/ls/ls_id/meta/meta_scn%ldls_ckpt.T30
  CHECK_MACRO_ID_TO_PATH(SHARED_LS_CKPT, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(SHARED_LS_CKPT, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/ls/3/meta/scn00000000000000000002.T30");

  // 31. SHARED_LS_META
  // cluster_id/tenant_id/ls/ls_id/meta/meta_scn%ldls_meta.T31
  CHECK_MACRO_ID_TO_PATH(SHARED_LS_META, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(SHARED_LS_META, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/ls/3/meta/scn00000000000000000002.T31");
}

void check_path_to_macro_id(bool is_local_cache, ObStorageObjectType type, MacroBlockId& macro_id,
    const int64_t ls_id = 0, const char *expected_path = nullptr)
{
  MacroBlockId convert_macro_id;
  ObPathContext ctx;
  int64_t ls_epoch_id = 4;
  const uint64_t server_id = GCONF.observer_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(type));
  int ret = ctx.set_file_ctx(macro_id, ls_epoch_id, is_local_cache, server_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (nullptr != expected_path) {
    ASSERT_STREQ(expected_path, ctx.get_path());
  }
  LOG_INFO("macro id to path", K(macro_id), K(ls_epoch_id), K(is_local_cache), K(ctx.get_path()), K(type));
  printf("begin to check_path_to_macro_id, is_local_cache: %d, type: %d, path: %s\n", is_local_cache, type, ctx.get_path());
  if (is_local_cache) {
    ASSERT_EQ(OB_SUCCESS, SSObjUtil::local_path_to_macro_id(type, ctx.get_path(), convert_macro_id));
  } else {
    ASSERT_EQ(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(type, ctx.get_path(), convert_macro_id));
  }
  LOG_INFO("path to macro id", K(convert_macro_id), K(macro_id), K(type), K(ctx.get_path()));
  ASSERT_EQ(static_cast<uint64_t>(convert_macro_id.storage_object_type()), static_cast<uint64_t>(type));
  ASSERT_EQ(convert_macro_id.second_id(), macro_id.second_id());
  ASSERT_EQ(convert_macro_id.third_id(), macro_id.third_id());
  ASSERT_EQ(convert_macro_id.fourth_id(), macro_id.fourth_id());
  ASSERT_EQ(convert_macro_id.fifth_id(), macro_id.fifth_id());
}

void check_relative_path_to_macro_id(ObStorageObjectType type, MacroBlockId& macro_id)
{
  MacroBlockId convert_macro_id;
  char path[common::MAX_PATH_SIZE] = {0};
  int64_t pos = 0;
  macro_id.set_storage_object_type(static_cast<uint64_t>(type));
  macro_id.set_location(ObMacroBlockIdLocation::TABLE);
  int ret = SSObjUtil::to_relative_remote_path_format(macro_id, path, common::MAX_PATH_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("macro id to relative path", K(macro_id), K(path), K(type));
  printf("begin to check_relative_path_to_macro_id, type: %d, path: %s\n", type, path);
  ASSERT_EQ(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(type, path, convert_macro_id));
  LOG_INFO("relative path to macro id", K(convert_macro_id), K(macro_id), K(type), K(path));
  ASSERT_EQ(static_cast<uint64_t>(convert_macro_id.storage_object_type()), static_cast<uint64_t>(type));
  ASSERT_EQ(convert_macro_id.second_id(), macro_id.second_id());
  ASSERT_EQ(convert_macro_id.third_id(), macro_id.third_id());
  ASSERT_EQ(convert_macro_id.fourth_id(), macro_id.fourth_id());
  ASSERT_EQ(convert_macro_id.fifth_id(), macro_id.fifth_id());
  ASSERT_TRUE(convert_macro_id.is_in_table());
}

TEST_F(TestFileManager, test_remote_path_to_macro_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  ObPathContext ctx;
  char expected_path[common::MAX_PATH_SIZE] = {0};
  bool is_local_cache = false;
  int64_t ls_epoch_id = 4;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(0);
  file_id.set_op_id(0);
  file_id.set_tenant_seq(5);
  char *object_storage_root_dir = nullptr;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir));

  MacroBlockId inc_file_id;
  inc_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  inc_file_id.set_second_id(3); // tablet_id
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);

  // 0.PRIVATE_DATA_MACRO
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/private_macro_third_id/data/macro_server_id_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_DATA_MACRO, file_id);

  // 1.PRIVATE_META_MACRO
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/private_macro_third_id/meta/macro_server_id_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_META_MACRO, file_id);

  // 2. SHARED_MINI_DATA_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_DATA_MACRO, inc_file_id);
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_DATA_MACRO, inc_file_id);

  // 3. SHARED_MINI_META_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_META_MACRO, inc_file_id);
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_META_MACRO, inc_file_id);

  // Additional SHARED_MINI parse cases.
  inc_file_id.set_op_id(7894);
  inc_file_id.set_seq_id(214);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1007/*ls_id*/, 1/*reorganization_scn*/);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_DATA_MACRO, inc_file_id);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_META_MACRO, inc_file_id);

  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1005/*ls_id*/, 2002/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_DATA_MACRO, inc_file_id);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINI_META_MACRO, inc_file_id);

  // 4. SHARED_MINOR_DATA_MACRO
  // inner tablet 49403
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_DATA_MACRO, inc_file_id);
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_DATA_MACRO, inc_file_id);

  // 5. SHARED_MINOR_META_MACRO
  // inner tablet 49402
  inc_file_id.set_second_id(49402);// TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet_name/minor/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_META_MACRO, inc_file_id);
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_META_MACRO, inc_file_id);

  // Additional SHARED_MINOR parse cases.
  inc_file_id.set_op_id(7894);
  inc_file_id.set_seq_id(214);
  inc_file_id.set_second_id(49402);
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1005/*ls_id*/, 2002/*reorganization_scn*/);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_DATA_MACRO, inc_file_id);
  // cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_META_MACRO, inc_file_id);

  inc_file_id.set_second_id(200002);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1005/*ls_id*/, 2002/*reorganization_scn*/);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_DATA_MACRO, inc_file_id);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/macro_seq_id
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MINOR_META_MACRO, inc_file_id);

  // 6.SHARED_MAJOR_DATA_MACRO
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(0);
  file_id.set_major_snapshot(1000);
  file_id.set_seq_id(5);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/data/macro_seq_id
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, file_id);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, file_id);

  // 7.SHARED_MAJOR_META_MACRO
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/meta/macro_seq_id
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_MAJOR_META_MACRO, file_id);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_MAJOR_META_MACRO, file_id);

  // 8.TMP_FILE
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::TMP_FILE, file_id);
  // 10.PRIVATE_TABLET_META
  file_id.set_second_id(3); // tablet_id
  file_id.set_reorganization_scn(2);
  file_id.set_meta_version_id(5);
  file_id.set_fifth_id(0);
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/reorganization_scn/meta_version
  check_path_to_macro_id(
      false/*is_local_cache*/, ObStorageObjectType::PRIVATE_TABLET_META, file_id, 3/*ls_id*/);

  // 11 PRIVATE_SLOG_FILE
  // cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld or cluster_id/server_id/server_slog/seq%ld
  file_id.set_second_id(MTL_ID());
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(5);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_SLOG_FILE, file_id);
  file_id.set_second_id(OB_SERVER_TENANT_ID);
  file_id.set_third_id(0);
  file_id.set_fourth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_SLOG_FILE, file_id);
  // 12 PRIVATE_CKPT_FILE
  // cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id or cluster_id/server_id/server_ckpt/object_id
  file_id.set_second_id(MTL_ID());
  file_id.set_fourth_id(2);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_CKPT_FILE, file_id);
  file_id.set_second_id(OB_SERVER_TENANT_ID);
  file_id.set_fourth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::PRIVATE_CKPT_FILE, file_id);
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(2);
  file_id.set_fourth_id(5);
  file_id.set_fifth_id(0);
  // 13. MAJOR_PREWARM_DATA
  //cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/compaction_scn
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MAJOR_PREWARM_DATA, file_id);

  // 14. MAJOR_PREWARM_DATA_INDEX
  //cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/compaction_scn
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX, file_id);

  // 15. MAJOR_PREWARM_META
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/scn_id_compaction_scn_prewarm_data
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MAJOR_PREWARM_META, file_id);

  // 16. MAJOR_PREWARM_META_INDEX
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/scn_id_compaction_scn_prewarm_data
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MAJOR_PREWARM_META_INDEX, file_id);

  // 18. IS_SHARED_TENANT_DELETED
  // cluster_id/tenant_id/is_shared_tenant_deleted
  file_id.set_third_id(0);
  file_id.set_fourth_id(0);
  file_id.set_fifth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::IS_SHARED_TENANT_DELETED, file_id);

  // 19. UNSEALED_REMOTE_SEG_FILE
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id_valid_length
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(5);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE, file_id);

  // --- Atomic protocol files -- //
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  
  // 21. SHARED_TABLET_SUB_META
  // local path to macro_id test: user_tablet : tenant_id_epoch_id/shared_tablet_sub_meta_cache/scatter_id/t%ldrs%ldop%ldseq%ld
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  // local path to macro_id test: inner_tablet : tenant_id_epoch_id/shared_tablet_sub_meta_cache/ls/ls_id/tablet_name_op%ldseq%ld
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(true/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);

  // remote path to macro_id test: user_tablet : cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/op_id_data_seq
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_op_id(1);
  inc_file_id.set_seq_id(1);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  // remote path to macro_id test: inner_tablet : cluster_id/tenant_id/ls/ls_id/tablet_name/meta/op_id_data_seq
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  
  // MACRO_CACHE_CKPT_DATA
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MACRO_CACHE_CKPT_DATA, file_id);
  // MACRO_CACHE_CKPT_META
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id
  file_id.set_fourth_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::MACRO_CACHE_CKPT_META, file_id);

  // 26. SHARED_INC_MAJOR_DATA_MACRO
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(0);
  file_id.set_op_id(1001);
  file_id.set_seq_id(5);
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/inc_major/cg_id/data/macro_seq_id
  check_path_to_macro_id(
      true/*is_local_cache*/, ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO, file_id);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO, file_id);

  // 27. SHARED_INC_MAJOR_META_MACRO
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO));
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/inc_major/cg_id/meta/macro_seq_id
  check_path_to_macro_id(
      true/*is_local_cache*/, ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO, file_id);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO, file_id);

  // 21. SHARED_TABLET_SUB_META (location is TABLE)
  // inner_tablet: ls/ls_id/tablet_name/meta/op%ldseq%010ld.T21
  // user_tablet: tablet/tablet_id/reorganization_scn/meta/op%ldseq%010ld.T21
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_third_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  inc_file_id.set_location(ObMacroBlockIdLocation::TABLE);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META,
      inc_file_id, 0/*ls_id*/, "table://tablet/200001/0/meta/op1seq0000000001.T21");
  // inner_tablet : cluster_id/tenant_id/ls/ls_id/tablet_name/tablet_meta/data/op_id_data_seq
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_third_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_SUB_META, inc_file_id);

  // 20. SHARED_TABLET_META
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/scn%020ld.T20
  MacroBlockId tablet_meta_id;
  tablet_meta_id.set_second_id(200001); // tablet_id
  tablet_meta_id.set_reorganization_scn(2001);
  tablet_meta_id.set_meta_scn(100);
  tablet_meta_id.set_seq_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_TABLET_META, tablet_meta_id);

  // 20. SHARED_TABLET_META (location is TABLE)
  // user_tablet: tablet/tablet_id/reorganization_scn/meta/scn%020ld.T20
  tablet_meta_id.set_second_id(200001); // tablet_id
  tablet_meta_id.set_reorganization_scn(2001);
  tablet_meta_id.set_meta_scn(100);
  tablet_meta_id.set_location(ObMacroBlockIdLocation::TABLE);
  check_relative_path_to_macro_id(ObStorageObjectType::SHARED_TABLET_META, tablet_meta_id);

  // 30. SHARED_LS_CKPT
  // cluster_id/tenant_id/ls/ls_id/meta/scn%020ld.T30
  MacroBlockId ls_ckpt_id;
  ls_ckpt_id.set_second_id(1001); // ls_id
  ls_ckpt_id.set_third_id(0);
  ls_ckpt_id.set_meta_scn(200);
  ls_ckpt_id.set_seq_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_LS_CKPT, ls_ckpt_id);

  // 30. SHARED_LS_CKPT (location is TABLE)
  // ls/ls_id/meta/scn%020ld.T30
  ls_ckpt_id.set_second_id(1001); // ls_id
  ls_ckpt_id.set_meta_scn(200);
  ls_ckpt_id.set_location(ObMacroBlockIdLocation::TABLE);
  check_relative_path_to_macro_id(ObStorageObjectType::SHARED_LS_CKPT, ls_ckpt_id);

  // 31. SHARED_LS_META
  // cluster_id/tenant_id/ls/ls_id/meta/scn%020ld.T31
  MacroBlockId ls_meta_id;
  ls_meta_id.set_second_id(1001); // ls_id
  ls_meta_id.set_third_id(0);
  ls_meta_id.set_meta_scn(300);
  ls_meta_id.set_seq_id(0);
  check_path_to_macro_id(false/*is_local_cache*/, ObStorageObjectType::SHARED_LS_META, ls_meta_id);

  // 31. SHARED_LS_META (location is TABLE)
  // ls/ls_id/meta/scn%020ld.T31
  ls_meta_id.set_second_id(1001); // ls_id
  ls_meta_id.set_meta_scn(300);
  ls_meta_id.set_location(ObMacroBlockIdLocation::TABLE);
  check_relative_path_to_macro_id(ObStorageObjectType::SHARED_LS_META, ls_meta_id);
}

TEST_F(TestFileManager, test_get_file_parent_dir)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char expected_dir_path[common::MAX_PATH_SIZE] = {0};
  int64_t ls_id = 3;
  int64_t ls_epoch_id = 4;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_reorganization_scn(0);
  file_id.set_tenant_seq(5);

  // each object type and print out the parent dir path log
  for (uint64_t obj_id = static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO); obj_id < static_cast<uint64_t>(ObStorageObjectType::MAX); ++obj_id) {
    PRINT_MACRO_ID_PARENT_DIR_PATH(obj_id);
  }

  // 0.PRIVATE_DATA_MACRO
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/data/
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_DATA_MACRO, OB_SUCCESS, "1_0/tablet_data/03/3/0/data");

  // 6.SHARED_MAJOR_DATA_MACRO
  CHECK_MACRO_ID_PARENT_DIR(SHARED_MAJOR_DATA_MACRO, OB_NOT_SUPPORTED, "");

  // 8.TMP_FILE
  // tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/segment_id
  CHECK_MACRO_ID_PARENT_DIR(TMP_FILE, OB_SUCCESS, "1_0/tmp_data/000/3");

  // 9.SERVER_META
  // CHECK_MACRO_ID_PARENT_DIR(SERVER_META, OB_SUCCESS, "");

  // 10.PRIVATE_TABLET_META
  file_id.set_second_id(3); // tablet_id
  file_id.set_reorganization_scn(2);
  file_id.set_meta_version_id(5);
  file_id.set_fifth_id(0);
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/reorganization_scn
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_TABLET_META, OB_SUCCESS, "1_0/ls/3_4/tablet_meta/03/3/2");
  file_id.set_reorganization_scn(0);
  file_id.set_op_id(0);
  file_id.set_tenant_seq(5);

  // 1.PRIVATE_META_MACRO
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_META_MACRO, OB_SUCCESS, "1_0/tablet_data/03/3/0/meta");

  // 7.SHARED_MAJOR_META_MACRO
  CHECK_MACRO_ID_PARENT_DIR(SHARED_MAJOR_META_MACRO, OB_NOT_SUPPORTED, "");
  // 17.TENANT_DISK_SPACE_META
  file_id.set_second_id(3);
  file_id.set_third_id(0);
  file_id.set_fourth_id(2);
  file_id.set_fifth_id(0);
  CHECK_MACRO_ID_PARENT_DIR(TENANT_DISK_SPACE_META, OB_SUCCESS, "3_2");
  // 18. IS_SHARED_TENANT_DELETED
  CHECK_MACRO_ID_PARENT_DIR(IS_SHARED_TENANT_DELETED, OB_NOT_SUPPORTED, "");
}

void check_object_type_stat(const MacroBlockId &macro_id, const uint64_t delete_cnt)
{
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSSObjectTypeStat type_stat;
  ASSERT_EQ(OB_SUCCESS, local_cache_service->get_object_type_stat(macro_id.storage_object_type(), true, type_stat));
  ObSSBaseStat stat;
  type_stat.get_stat(ObSSObjectTypeStatType::DELETE, stat);
  ASSERT_EQ(delete_cnt, stat.get_cnt());
}
TEST_F(TestFileManager, test_private_macro_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  LOG_INFO("begin to test_private_macro_file_operator");
  MacroBlockId file_id;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);  //tablet_id
  file_id.set_reorganization_scn(0); // private_macro_third_id
  file_id.set_tenant_seq(5);  //tenant_seq

  // step 1: create dir
  int64_t tablet_id = 3;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/));

  // step 2: test write private_macro_file
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // step 3: test read private_macro_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // test 4 : test fsync private_macro_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id));

  // step 5: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, 0 /*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 6: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, 0 /*ls_epoch_id*/, file_length));
  ASSERT_EQ(write_io_size, file_length);

  // step 7: test list tablet_dir
  ObArray<int64_t> tablet_dirs;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_dirs));
  ASSERT_EQ(1, tablet_dirs.count());
  ASSERT_EQ(tablet_id, tablet_dirs.at(0));

  // test 8: test calc_private_macro_disk_space
  usleep(3000 * 1000); // sleep 3 seconds to wait file system meta update
  ObCalibrateDiskSpaceResult calibrate_res;
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_private_macro_third_id_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_id_macro_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/, ObMacroType::DATA_MACRO));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_id_macro_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/, ObMacroType::META_MACRO));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t shared_mini_ls_dir_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINI));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls"));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  shared_mini_ls_dir_size = statbuf.size_;
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t shared_minor_ls_dir_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINOR));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls"));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  shared_minor_ls_dir_size = statbuf.size_;
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t shared_tablet_sub_meta_ls_dir_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_sub_meta_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID()));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls"));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  shared_tablet_sub_meta_ls_dir_size = statbuf.size_;
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t scatter_dir_size = 0;
  get_macro_block_scatter_dir_size(scatter_dir_size);
  expected_disk_size += scatter_dir_size;

  ob_usleep(3000 * 1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_macro_block_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 9: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(file_id));

  // step 10: test delete dir
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_data_tablet_id_private_macro_third_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_data_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id));
  // step 11: test calc_private_macro_disk_space after delete file
  ob_usleep(3000 * 1000);
  start_calc_size_time_s = ObTimeUtility::current_time_s();
  calibrate_res.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_macro_block_disk_space(start_calc_size_time_s, calibrate_res));
  get_macro_block_scatter_dir_size(scatter_dir_size);
  ASSERT_EQ(shared_mini_ls_dir_size + shared_minor_ls_dir_size + shared_tablet_sub_meta_ls_dir_size + scatter_dir_size, calibrate_res.total_file_size_);
}

TEST_F(TestFileManager, test_tmp_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  file_id.set_second_id(3);  //tmp_file_id
  file_id.set_fourth_id(2);  // segment_id

  // step 1: create dir
  int64_t tmp_file_id = 3;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  // step 2: test write tmp_file
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  write_object_handle.reset();

  // step 3: test read tmp_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  read_object_handle.reset();

  // test 4: test fsync tmp_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id));

  // test 5: test append tmp_file
  ObStorageObjectWriteInfo append_info;
  append_info.io_desc_.set_wait_event(1);
  append_info.io_desc_.set_unsealed();
  append_info.buffer_ = write_buf;
  append_info.offset_ = write_io_size;
  append_info.size_ = write_io_size;
  append_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  append_info.mtl_tenant_id_ = MTL_ID();
  append_info.set_tmp_file_valid_length(write_io_size + write_io_size);
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(append_info, write_object_handle));
  write_object_handle.reset();

  // step 6: test read tmp_file
  read_object_handle.reset();
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  read_buf[0] = '\0';
  read_info.buf_ = read_buf;
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  read_object_handle.reset();

  // step 7: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, 0, is_exist));
  ASSERT_TRUE(is_exist);

  // step 8: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, 0, file_length));
  ASSERT_EQ(2 * write_io_size, file_length);

  // test 9: test calc_tmp_file_disk_space
  // expected_disk_size = file_size + dir_size
  // file_size = 8192 * 2 = 16384
  // dir_size = scatter_dirs (only created on-demand) + tmp_file_dir
  // Note: scan_dir_rec does NOT count the starting directory (tmp_data) itself,
  // only its subdirectories (scatter dirs + tmp_file_dir)  
  ObCalibrateDiskSpaceResult calibrate_res;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  int64_t expected_disk_size = 2 * write_io_size;
  ObIODFileStat statbuf;
  // Add tmp_file_dir size
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  // Add scatter_dir size (only the one created for tmp_file_id)
  // tmp_file_id / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM determines which scatter dir
  int64_t scatter_id = tmp_file_id / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  bool scatter_dir_exists = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, scatter_dir_exists));
  if (scatter_dir_exists) {
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
    expected_disk_size += statbuf.size_;
  }
  // Sleep to ensure file system timestamp progresses, then record timestamp to include all created files/dirs
  ob_usleep(2000*1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_tmp_file_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 10: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));

}

TEST_F(TestFileManager, test_tmp_file_scatter_dir)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // Test 1: Verify scatter directory path generation for different tmp_file_ids
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  char expected_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  char tmp_data_dir[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  
  // Get base tmp_data directory
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_dir, sizeof(tmp_data_dir), MTL_ID(), MTL_EPOCH_ID()));
  
  // Test scatter directory path for tmp_file_id = 3 (should be in scatter dir 000)
  int64_t tmp_file_id_1 = 3;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_1));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path), "%s/%03ld", tmp_data_dir, 
                                       tmp_file_id_1 / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM));
  ASSERT_EQ(0, STRCMP(scatter_dir_path, expected_path)) << "scatter_dir_path: " << scatter_dir_path 
                                                        << ", expected_path: " << expected_path;
  
  // Test scatter directory path for tmp_file_id = 103 (should be in scatter dir 000, 103 / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM = 0)
  char scatter_dir_path_1_saved[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  STRNCPY(scatter_dir_path_1_saved, scatter_dir_path, sizeof(scatter_dir_path_1_saved));
  scatter_dir_path[0] = '\0';
  expected_path[0] = '\0';
  int64_t tmp_file_id_2 = 103;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_2));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path), "%s/%03ld", tmp_data_dir, 
                                       tmp_file_id_2 / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM));
  ASSERT_EQ(0, STRCMP(scatter_dir_path, expected_path)) << "scatter_dir_path: " << scatter_dir_path 
                                                        << ", expected_path: " << expected_path;
  // Verify both tmp_file_id_1 and tmp_file_id_2 map to the same scatter directory
  ASSERT_EQ(0, STRCMP(scatter_dir_path_1_saved, scatter_dir_path)) 
      << "tmp_file_id " << tmp_file_id_1 << " and " << tmp_file_id_2 
      << " should map to the same scatter directory";
  
  // Test scatter directory path for tmp_file_id = 2050 (should be in scatter dir 2)
  scatter_dir_path[0] = '\0';
  expected_path[0] = '\0';
  int64_t tmp_file_id_3 = 2050;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_3));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path), "%s/%03ld", tmp_data_dir, 
                                       tmp_file_id_3 / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM));
  ASSERT_EQ(0, STRCMP(scatter_dir_path, expected_path)) << "scatter_dir_path: " << scatter_dir_path 
                                                        << ", expected_path: " << expected_path;
  
  // Test 2: Verify scatter directory creation
  // Create tmp_file_dir for tmp_file_id_1, which should create the scatter directory
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_1));
  
  // Verify scatter directory exists
  scatter_dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_1));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(scatter_dir_path, is_exist));
  ASSERT_TRUE(is_exist) << "scatter directory should exist: " << scatter_dir_path;
  
  // Verify tmp_file directory exists
  char tmp_file_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(tmp_file_dir_path, sizeof(tmp_file_dir_path), 
                                                          MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_1));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(tmp_file_dir_path, is_exist));
  ASSERT_TRUE(is_exist) << "tmp_file directory should exist: " << tmp_file_dir_path;
  
  // Test 3: Verify multiple tmp_files are scattered to different directories
  // Create tmp_file_dir for tmp_file_id_3 (should be in scatter dir 002)
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_3));
  
  // Verify scatter directory for tmp_file_id_3 exists
  scatter_dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_3));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(scatter_dir_path, is_exist));
  ASSERT_TRUE(is_exist) << "scatter directory should exist: " << scatter_dir_path;
  
  // Test 4: Verify that tmp_file_id_2 (103) uses the same scatter directory as tmp_file_id_1 (3)
  // Both should map to scatter dir 000 (3 / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM = 0, 103 / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM = 0)
  char scatter_dir_path_1[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  char scatter_dir_path_2[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path_1, sizeof(scatter_dir_path_1), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_1));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path_2, sizeof(scatter_dir_path_2), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_2));
  ASSERT_EQ(0, STRCMP(scatter_dir_path_1, scatter_dir_path_2)) 
      << "tmp_file_id " << tmp_file_id_1 << " and " << tmp_file_id_2 
      << " should map to the same scatter directory";
  
  // Create tmp_file_dir for tmp_file_id_2, should reuse existing scatter directory
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_2));
  
  // Verify tmp_file directory for tmp_file_id_2 exists
  tmp_file_dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(tmp_file_dir_path, sizeof(tmp_file_dir_path), 
                                                          MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_2));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(tmp_file_dir_path, is_exist));
  ASSERT_TRUE(is_exist) << "tmp_file directory should exist: " << tmp_file_dir_path;
  
  // Test 5: Verify scatter directory format (should be %03ld format, e.g., 000, 001, ..., 999)
  // Test with tmp_file_id = 0 (should be in scatter dir 000, 0 / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM = 0)
  scatter_dir_path[0] = '\0';
  expected_path[0] = '\0';
  int64_t tmp_file_id_4 = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_4));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path), "%s/%03ld", tmp_data_dir, 
                                       tmp_file_id_4 / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM));
  ASSERT_EQ(0, STRCMP(scatter_dir_path, expected_path)) << "scatter_dir_path: " << scatter_dir_path 
                                                        << ", expected_path: " << expected_path;
  // Verify the scatter directory name ends with "000"
  const char* dir_name = STRRCHR(scatter_dir_path, '/');
  ASSERT_NE(nullptr, dir_name);
  dir_name++; // Skip the '/'
  ASSERT_EQ(0, STRCMP(dir_name, "000")) << "scatter directory name should be '000' for tmp_file_id=0";
  
  // Test with tmp_file_id = 99 (should be in scatter dir 000, 99 / TMP_FILE_SCATTER_DIR_NUM % TMP_FILE_SCATTER_DIR_NUM = 0)
  scatter_dir_path[0] = '\0';
  expected_path[0] = '\0';
  int64_t tmp_file_id_5 = 99;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path), 
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_5));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path), "%s/%03ld", tmp_data_dir, 
                                       tmp_file_id_5 / ObDirManager::TMP_FILE_SCATTER_DIR_NUM % ObDirManager::TMP_FILE_SCATTER_DIR_NUM));
  ASSERT_EQ(0, STRCMP(scatter_dir_path, expected_path)) << "scatter_dir_path: " << scatter_dir_path 
                                                        << ", expected_path: " << expected_path;
  // Verify the scatter directory name ends with "000"
  dir_name = STRRCHR(scatter_dir_path, '/');
  ASSERT_NE(nullptr, dir_name);
  dir_name++; // Skip the '/'
  ASSERT_EQ(0, STRCMP(dir_name, "000")) << "scatter directory name should be '000' for tmp_file_id=99";
  
  // Cleanup: Delete all created tmp_files to avoid affecting subsequent tests
  MacroBlockId file_id;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  
  // Delete tmp_file_id_1
  file_id.set_second_id(tmp_file_id_1);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
  
  // Delete tmp_file_id_2
  file_id.set_second_id(tmp_file_id_2);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
  
  // Delete tmp_file_id_3
  file_id.set_second_id(tmp_file_id_3);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
  
  LOG_INFO("test_tmp_file_scatter_dir completed successfully");
}

TEST_F(TestFileManager, test_delete_old_version_files_in_scatter_dir)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObSegmentFileManager &segment_file_mgr = tenant_file_mgr->get_segment_file_mgr();
  
  char tmp_data_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path, sizeof(tmp_data_path), MTL_ID(), MTL_EPOCH_ID()));
  
  // ==========================
  // Scenario 1: Normal cleanup - scatter_id = 100
  // Old version: tmp_data/100/seg_old.T8 (file_id=100, direct under scatter_dir)
  // New version: tmp_data/100/100000/seg_new.T8 (file_id=100000)
  // ==========================
  LOG_INFO("=== Scenario 1: Normal cleanup with scatter_id=100 ===");
  
  // Create old version file structure
  char scatter_dir_100[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_100, sizeof(scatter_dir_100), "%s/100", tmp_data_path));
  ASSERT_EQ(0, ::mkdir(scatter_dir_100, 0755)) << "Failed to create scatter_dir: " << scatter_dir_100;
  
  char old_file_100[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(old_file_100, sizeof(old_file_100), "%s/seg_old.T8", scatter_dir_100));
  int fd = ::open(old_file_100, O_WRONLY | O_CREAT, 0644);
  ASSERT_GE(fd, 0);
  ::write(fd, "old version file", 16);
  ::close(fd);
  
  // Create new version file structure (file_id=100000)
  int64_t new_file_id_100 = 100000;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), new_file_id_100));
  char new_file_dir_100[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(new_file_dir_100, sizeof(new_file_dir_100), 
                                                           MTL_ID(), MTL_EPOCH_ID(), new_file_id_100));
  
  char new_file_100[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(new_file_100, sizeof(new_file_100), "%s/seg_new.T8", new_file_dir_100));
  fd = ::open(new_file_100, O_WRONLY | O_CREAT, 0644);
  ASSERT_GE(fd, 0);
  ::write(fd, "new version file", 16);
  ::close(fd);
  
  // Verify files exist before cleanup
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(old_file_100, is_exist));
  ASSERT_TRUE(is_exist) << "Old file should exist before cleanup";
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(new_file_100, is_exist));
  ASSERT_TRUE(is_exist) << "New file should exist before cleanup";
  
  // Execute cleanup
  int64_t delete_count_100 = 0;
  ASSERT_EQ(OB_SUCCESS, segment_file_mgr.delete_old_version_files_in_scatter_dir(tmp_data_path, 100, delete_count_100));
  ASSERT_EQ(1, delete_count_100) << "Should delete 1 old file";
  
  // Verify old file deleted, new file preserved
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(old_file_100, is_exist));
  ASSERT_FALSE(is_exist) << "Old file should be deleted";
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(new_file_100, is_exist));
  ASSERT_TRUE(is_exist) << "New file should be preserved";
  
  LOG_INFO("Scenario 1 passed", K(delete_count_100));
  
  // ==========================
  // Scenario 2: Multiple old files - scatter_id = 500
  // Old version: tmp_data/500/seg1.T8, seg2.T8, seg3.T8
  // New version: tmp_data/500/500500/seg_new.T8 (file_id=500500)
  // ==========================
  LOG_INFO("=== Scenario 2: Multiple old files with scatter_id=500 ===");
  
  // Create scatter directory
  char scatter_dir_500[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_500, sizeof(scatter_dir_500), "%s/500", tmp_data_path));
  ASSERT_EQ(0, ::mkdir(scatter_dir_500, 0755)) << "Failed to create scatter_dir: " << scatter_dir_500;
  
  // Create multiple old version files
  const char* old_file_names[] = {"seg1.T8", "seg2.T8", "seg3.T8"};
  char old_files_500[3][ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(old_files_500[i], sizeof(old_files_500[i]), "%s/%s", scatter_dir_500, old_file_names[i]));
    fd = ::open(old_files_500[i], O_WRONLY | O_CREAT, 0644);
    ASSERT_GE(fd, 0);
    ::write(fd, "old file", 8);
    ::close(fd);
  }
  
  // Create new version file structure (file_id=500500)
  int64_t new_file_id_500 = 500500;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), new_file_id_500));
  char new_file_dir_500[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(new_file_dir_500, sizeof(new_file_dir_500), 
                                                           MTL_ID(), MTL_EPOCH_ID(), new_file_id_500));
  
  char new_file_500[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(new_file_500, sizeof(new_file_500), "%s/seg_new.T8", new_file_dir_500));
  fd = ::open(new_file_500, O_WRONLY | O_CREAT, 0644);
  ASSERT_GE(fd, 0);
  ::write(fd, "new file", 8);
  ::close(fd);
  
  // Execute cleanup
  int64_t delete_count_500 = 0;
  ASSERT_EQ(OB_SUCCESS, segment_file_mgr.delete_old_version_files_in_scatter_dir(tmp_data_path, 500, delete_count_500));
  ASSERT_EQ(3, delete_count_500) << "Should delete 3 old files";
  
  // Verify all old files deleted, new file preserved
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(old_files_500[i], is_exist));
    ASSERT_FALSE(is_exist) << "Old file " << old_file_names[i] << " should be deleted";
  }
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(new_file_500, is_exist));
  ASSERT_TRUE(is_exist) << "New file should be preserved";
  
  LOG_INFO("Scenario 2 passed", K(delete_count_500));
  
  // ==========================
  // Scenario 3: Only new version files - scatter_id = 999
  // New version: tmp_data/999/999999/seg_new.T8 (file_id=999999)
  // ==========================
  LOG_INFO("=== Scenario 3: Only new version files with scatter_id=999 ===");
  
  // Create scatter directory
  char scatter_dir_999[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_999, sizeof(scatter_dir_999), "%s/999", tmp_data_path));
  ASSERT_EQ(0, ::mkdir(scatter_dir_999, 0755)) << "Failed to create scatter_dir: " << scatter_dir_999;
  
  // Create only new version file (file_id=999999)
  int64_t new_file_id_999 = 999999;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), new_file_id_999));
  char new_file_dir_999[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(new_file_dir_999, sizeof(new_file_dir_999), 
                                                           MTL_ID(), MTL_EPOCH_ID(), new_file_id_999));
  
  char new_file_999[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(new_file_999, sizeof(new_file_999), "%s/seg_new.T8", new_file_dir_999));
  fd = ::open(new_file_999, O_WRONLY | O_CREAT, 0644);
  ASSERT_GE(fd, 0);
  ::write(fd, "new file", 8);
  ::close(fd);
  
  // Execute cleanup
  int64_t delete_count_999 = 0;
  ASSERT_EQ(OB_SUCCESS, segment_file_mgr.delete_old_version_files_in_scatter_dir(tmp_data_path, 999, delete_count_999));
  ASSERT_EQ(0, delete_count_999) << "Should delete 0 files";
  
  // Verify new file still exists
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(new_file_999, is_exist));
  ASSERT_TRUE(is_exist) << "New file should still exist";
  
  LOG_INFO("Scenario 3 passed", K(delete_count_999));
  
  // ==========================
  // Scenario 4: Directory doesn't exist - scatter_id = 200
  // ==========================
  LOG_INFO("=== Scenario 4: Directory doesn't exist with scatter_id=200 ===");
  
  int64_t delete_count_200 = 0;
  ASSERT_EQ(OB_SUCCESS, segment_file_mgr.delete_old_version_files_in_scatter_dir(tmp_data_path, 200, delete_count_200));
  ASSERT_EQ(0, delete_count_200) << "Should delete 0 files when directory doesn't exist";
  
  LOG_INFO("Scenario 4 passed", K(delete_count_200));
  
  // ==========================
  // Scenario 5: Empty directory - scatter_id = 300
  // ==========================
  LOG_INFO("=== Scenario 5: Empty directory with scatter_id=300 ===");
  
  // Create empty scatter directory
  char scatter_dir_300[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_300, sizeof(scatter_dir_300), "%s/300", tmp_data_path));
  ASSERT_EQ(0, ::mkdir(scatter_dir_300, 0755)) << "Failed to create scatter_dir: " << scatter_dir_300;
  
  // Execute cleanup on empty directory
  int64_t delete_count_300 = 0;
  ASSERT_EQ(OB_SUCCESS, segment_file_mgr.delete_old_version_files_in_scatter_dir(tmp_data_path, 300, delete_count_300));
  ASSERT_EQ(0, delete_count_300) << "Should delete 0 files in empty directory";
  
  LOG_INFO("Scenario 5 passed", K(delete_count_300));
  
  // ==========================
  // Cleanup all test resources
  // ==========================
  LOG_INFO("=== Cleaning up test resources ===");
  
  // Cleanup scenario 1 resources
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(new_file_100));
  MacroBlockId macro_id_100;
  macro_id_100.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id_100.set_second_id(new_file_id_100);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(macro_id_100));
  
  // Cleanup scenario 2 resources
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(new_file_500));
  MacroBlockId macro_id_500;
  macro_id_500.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id_500.set_second_id(new_file_id_500);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(macro_id_500));
  
  // Cleanup scenario 3 resources
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(new_file_999));
  MacroBlockId macro_id_999;
  macro_id_999.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id_999.set_second_id(new_file_id_999);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(macro_id_999));
  
  // Cleanup empty directory from scenario 5
  ASSERT_EQ(0, ::rmdir(scatter_dir_300)) << "Failed to remove empty scatter_dir: " << scatter_dir_300;
  
  LOG_INFO("test_delete_old_version_files_in_scatter_dir completed successfully");
}

TEST_F(TestFileManager, test_rm_logical_deleted_file)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: create tmp_file_dir
  // Use tmp_file_id = 999000 to map to scatter_dir 999, avoiding conflicts with other tests
  // scatter_id = 999000 / 1000 % 1000 = 999
  int64_t tmp_file_id = 999000;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  
  // step 2: write tmp_file
  MacroBlockId file_id;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  file_id.set_second_id(tmp_file_id);
  file_id.set_fourth_id(2);  // segment_id
  
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  
  ObSSMacroCacheStat tmp_file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  int64_t tmp_file_used_size = tmp_file_cache_stat.used_;
  
  // step 3: logical delete tmp_file (pause gc to keep .deleted file)
  tenant_file_mgr->set_tmp_file_cache_pause_gc();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(file_id));
  
  // step 4: verify .deleted file exists
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS,
            ctx.set_logical_delete_ctx(file_id, 0/*ls_epoch_id*/, GCONF.observer_id));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_TRUE(is_exist);
  
  // step 5: call rm_logical_deleted_file to clean up .deleted file
  tenant_file_mgr->set_tmp_file_cache_allow_gc();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file());
  
  // step 6: verify .deleted file is removed
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_FALSE(is_exist);
  
  // step 7: verify disk space is updated
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  ASSERT_EQ(tmp_file_used_size - write_io_size, tmp_file_cache_stat.used_);
  
  // step 8: verify empty tmp_file_dir is not deleted immediately (mtime check)
  char tmp_file_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(tmp_file_dir_path, sizeof(tmp_file_dir_path),
                                                          MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(tmp_file_dir_path, is_exist));
  ASSERT_TRUE(is_exist);
  
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_scatter_dir(scatter_dir_path, sizeof(scatter_dir_path),
                                                                  MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(scatter_dir_path, is_exist));
  ASSERT_TRUE(is_exist);

  // step 9: simulate 1 hour later by modifying mtime
  struct timeval times[2];
  int64_t two_hours_ago_s = ObTimeUtility::current_time_s() - 7200; // 2 hours ago
  times[0].tv_sec = two_hours_ago_s;  // access time
  times[0].tv_usec = 0;
  times[1].tv_sec = two_hours_ago_s;  // modification time
  times[1].tv_usec = 0;

  // step 10: verify empty tmp_file_dir is deleted after 1 hour
  ASSERT_EQ(OB_SUCCESS, ::utimes(tmp_file_dir_path, times));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file());
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(tmp_file_dir_path, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(scatter_dir_path, is_exist));
  ASSERT_TRUE(is_exist); // because delete tmp_file_dir_path would update scatter_dir_path's mtime to current time
  
  ASSERT_EQ(OB_SUCCESS, ::utimes(scatter_dir_path, times));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file());
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(scatter_dir_path, is_exist));
  ASSERT_FALSE(is_exist);

  // step 11: cleanup
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
}

TEST_F(TestFileManager, test_meta_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  // step 1: construct MacroBlockId
  MacroBlockId file_id;
  const int64_t ls_id = 7;
  const int64_t ls_epoch_id = 11;
  const int64_t tablet_id = 8;
  const int64_t reorganization_scn = 0;
  const int64_t meta_version_id = 9;
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  file_id.set_second_id(tablet_id);
  file_id.set_reorganization_scn(reorganization_scn);
  file_id.set_meta_version_id(meta_version_id);
  file_id.set_fifth_id(0);

  // step 2: test write private_tablet_meta
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_ls_epoch_id(ls_epoch_id);
  write_info.set_ls_id(ls_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // step 3: test read private_tablet_meta
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.set_ls_epoch_id(ls_epoch_id);
  read_info.set_ls_id(ls_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // test 4 : test fsync private_tablet_meta
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id, ls_epoch_id, ls_id));

  // step 5: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, ls_epoch_id, is_exist, ls_id));
  ASSERT_TRUE(is_exist);

  // step 6: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, ls_epoch_id, file_length, ls_id));
  ASSERT_EQ(write_io_size, file_length);

  // step 7: test list tablet meta versions
  ObArray<int64_t> tablet_meta_versions;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(
      ls_id, ls_epoch_id, tablet_id, reorganization_scn, tablet_meta_versions));
  ASSERT_EQ(1, tablet_meta_versions.count());
  ASSERT_EQ(meta_version_id, tablet_meta_versions.at(0));

  // test 8: test calc_meta_file_disk_space
  ObCalibrateDiskSpaceResult calibrate_res;
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_private_macro_third_id_dir(
      dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(),
      ls_id, ls_epoch_id, tablet_id, reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_dir(
      dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] = '\0';

  int64_t ls_id_dir_size = 0;
  get_ls_id_dir_size(ls_id, ls_epoch_id, ls_id_dir_size);
  expected_disk_size += ls_id_dir_size;

  ob_usleep(2000*1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, calibrate_res));
  // private_tablet_meta + tablet_id_dir
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 9: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(file_id, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  // step 10: test delete dir
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_private_macro_third_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ob_usleep(2000*1000);
  start_calc_size_time_s = ObTimeUtility::current_time_s();
  get_ls_id_dir_size(ls_id, ls_epoch_id, ls_id_dir_size);
  calibrate_res.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(ls_id_dir_size, calibrate_res.total_file_size_);
}

TEST_F(TestFileManager, test_list_private_tablet_meta)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  // step 0: construct MacroBlockId
  MacroBlockId meta_file_1;
  const int64_t ls_id = 6;
  const int64_t ls_epoch_id = 0;
  const int64_t tablet_id = 8;
  const int64_t reorganization_scn = 0;
  const int64_t meta_version_id_1 = 1;
  ObSEArray<int64_t, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> meta_vers_array;
  ObIArray<int64_t> &tablet_meta_vers = meta_vers_array;
  meta_file_1.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  meta_file_1.set_second_id(tablet_id);
  meta_file_1.set_reorganization_scn(reorganization_scn);
  meta_file_1.set_meta_version_id(meta_version_id_1);
  meta_file_1.set_fifth_id(0);

  MacroBlockId meta_file_2;
  const int64_t meta_version_id_2 = 2;
  meta_file_2.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  meta_file_2.set_second_id(tablet_id);
  meta_file_2.set_reorganization_scn(reorganization_scn);
  meta_file_2.set_meta_version_id(meta_version_id_2);
  meta_file_2.set_fifth_id(0);

  ObStorageObjectHandle write_object_handle_1;
  ASSERT_EQ(OB_SUCCESS, write_object_handle_1.set_macro_block_id(meta_file_1));
  ObStorageObjectHandle write_object_handle_2;
  ASSERT_EQ(OB_SUCCESS, write_object_handle_2.set_macro_block_id(meta_file_2));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_ls_epoch_id(ls_epoch_id);
  write_info.set_ls_id(ls_id);


  // step 1: test .tmp.seq private tablet meta
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1));
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(
      meta_file_1, ls_epoch_id, true/*is_local_cache*/, GCONF.observer_id, ls_id));
  char tmp_seq_path[MAX_PATH_SIZE] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_seq_path, sizeof(tmp_seq_path), "%s.tmp.100", ctx.get_path()));
  ::rename(ctx.get_path(), tmp_seq_path); // rename filename to filename.tmp.seq
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(
      ls_id, ls_epoch_id, tablet_id, reorganization_scn, tablet_meta_vers));
  ASSERT_EQ(0, tablet_meta_vers.count());
  ::rename(tmp_seq_path, ctx.get_path()); // rename filename.tmp.seq to filename
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(meta_file_1, ls_epoch_id, ls_id));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);


  // step 2: test private tablet meta exists only in local cache
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1)); 
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(
      ls_id, ls_epoch_id, tablet_id, reorganization_scn, tablet_meta_vers));
  ASSERT_EQ(1, tablet_meta_vers.count());
  ASSERT_EQ(meta_version_id_1, tablet_meta_vers.at(0));

  // step 3: test private tablet meta only exists in object storage
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(meta_file_1, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  ASSERT_TRUE(meta_file_2.is_macro_write_cache_ctrl_obj_type());
  ASSERT_FALSE(is_macro_write_cache_disabled());
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSSLocalCacheControlMode control_mode;
  control_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  local_cache_service->set_local_cache_control_mode(control_mode);
  ASSERT_TRUE(local_cache_service->is_macro_write_cache_disabled());
  ASSERT_TRUE(is_macro_write_cache_disabled());
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_2));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_2, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_2, ls_epoch_id, is_exist, ls_id));
  ASSERT_TRUE(is_exist);
  tablet_meta_vers.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(
      ls_id, ls_epoch_id, tablet_id, reorganization_scn, tablet_meta_vers));
  ASSERT_EQ(1, tablet_meta_vers.count());
  ASSERT_EQ(meta_version_id_2, tablet_meta_vers.at(0));

  // step 4: test private tablet meta exists in local cache and object storage 
  tablet_meta_vers.reset();
  ASSERT_EQ(0, tablet_meta_vers.count());
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  control_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  local_cache_service->set_local_cache_control_mode(control_mode);
  ASSERT_FALSE(local_cache_service->is_macro_write_cache_disabled());
  ASSERT_FALSE(is_macro_write_cache_disabled());
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1)); 
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_2, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_2, ls_epoch_id, is_exist, ls_id));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(
      ls_id, ls_epoch_id, tablet_id, reorganization_scn, tablet_meta_vers));
  ASSERT_EQ(2, tablet_meta_vers.count());
  ASSERT_TRUE(
    tablet_meta_vers.at(0) == meta_version_id_1 ||
    tablet_meta_vers.at(0) == meta_version_id_2
  );
  ASSERT_TRUE(
    tablet_meta_vers.at(1) == meta_version_id_1 ||
    tablet_meta_vers.at(1) == meta_version_id_2
  );

  // step 5: test delete private tablet meta
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(meta_file_1, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(meta_file_2, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_2, ls_epoch_id, is_exist, ls_id));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_tenant_disk_space_meta)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write tmp_file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 4;
  int64_t segment_id = 5;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObSSMacroCacheStat tmp_file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  // step 2: write tenant disk space meta
  ObTenantDiskSpaceMeta disk_space_meta(MTL_ID());
  ASSERT_EQ(OB_SUCCESS, disk_space_meta.body_.set_macro_cache_stats());
  ASSERT_EQ(OB_SUCCESS, disk_space_meta.header_.construct_header(disk_space_meta.body_));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->write_tenant_disk_space_meta(disk_space_meta));
  const uint8_t idx = static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE);
  ASSERT_EQ(tmp_file_cache_stat.used_, disk_space_meta.body_.macro_cache_stats_.at(idx).used_);
  // step 3: read tenant disk space meta
  disk_space_meta.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->read_tenant_disk_space_meta(disk_space_meta));
  ASSERT_EQ(tmp_file_cache_stat.used_, disk_space_meta.body_.macro_cache_stats_.at(idx).used_);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(tmp_file));
}

TEST_F(TestFileManager, test_ss_format)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_storage_dest(storage_dest));
  // step 1: is exist ss_format
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::is_exist_ss_format(storage_dest, is_exist));
  ASSERT_EQ(false, is_exist);
  // step 2: write ss_format
  ObSSFormat ss_format;
  const uint64_t cur_cluster_version = CLUSTER_CURRENT_VERSION;
  const int64_t cur_us = ObTimeUtility::fast_current_time();
  ss_format.body_.cluster_version_ = cur_cluster_version;
  ss_format.body_.create_timestamp_ = cur_us;
  ASSERT_EQ(OB_SUCCESS, ss_format.header_.construct_header(ss_format.body_));
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::write_ss_format(storage_dest, ss_format));
  // step 3: is exist ss_format
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::is_exist_ss_format(storage_dest, is_exist));
  ASSERT_EQ(true, is_exist);
  // step 4: read ss_format
  ss_format.reset();
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::read_ss_format(storage_dest, ss_format));
  ASSERT_EQ(cur_cluster_version, ss_format.body_.cluster_version_);
  ASSERT_EQ(cur_us, ss_format.body_.create_timestamp_);
}

TEST_F(TestFileManager, test_ss_cluster_info)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_storage_dest(storage_dest));
  // step 1: is exist ss_cluster_info
  ASSERT_EQ(OB_SUCCESS, ObSSClusterInfoUtil::is_exist_ss_cluster_info(storage_dest, is_exist));
  ASSERT_EQ(false, is_exist);
  // step 2: write ss_cluster_info
  ObSSClusterInfo ss_cluster_info;
  const uint64_t cluster_id = 12345;
  const common::ObRegion cluster_region("test_region");
  ASSERT_EQ(OB_SUCCESS, ss_cluster_info.init(cluster_id, cluster_region));
  ASSERT_EQ(OB_SUCCESS, ObSSClusterInfoUtil::write_ss_cluster_info(storage_dest, ss_cluster_info));
  // step 3: is exist ss_cluster_info
  ASSERT_EQ(OB_SUCCESS, ObSSClusterInfoUtil::is_exist_ss_cluster_info(storage_dest, is_exist));
  ASSERT_EQ(true, is_exist);
  // step 4: read ss_cluster_info
  ObSSClusterInfo read_ss_cluster_info;
  ASSERT_EQ(OB_SUCCESS, ObSSClusterInfoUtil::read_ss_cluster_info(storage_dest, read_ss_cluster_info));
  ASSERT_EQ(cluster_id, read_ss_cluster_info.body_.logservice_cluster_id_);
  ASSERT_EQ(1, cluster_region == read_ss_cluster_info.body_.cluster_region_);
}

TEST_F(TestFileManager, test_list_and_delete_dir_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  const int64_t ls_id = 6;
  const int64_t ls_epoch_id = 0;

  // test1: list tmp file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 3;
  int64_t segment_file_id = 2;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tmp_files;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tmp_file(tmp_files));
  ASSERT_EQ(1, tmp_files.count());
  ASSERT_EQ(tmp_file_id, tmp_files.at(0).second_id());

  // test2: list private data macro
  MacroBlockId tablet_data_macro;
  int64_t tablet_id = 4;
  int64_t server_id = 5;
  int64_t seq_id = 6;
  tablet_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  tablet_data_macro.set_second_id(tablet_id);
  tablet_data_macro.set_reorganization_scn(0); // private_macro_third_id
  tablet_data_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_data_macro));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tablet_data_macros;
  ObArray<int64_t> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(tablet_id, tablet_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_macro_file(tablet_ids.at(0), 0/*private_macro_third_id*/, tablet_data_macros));
  ASSERT_EQ(1, tablet_data_macros.count());
  ASSERT_EQ(tablet_data_macro.second_id(), tablet_data_macros.at(0).second_id());
  ASSERT_EQ(tablet_data_macro.third_id(), tablet_data_macros.at(0).third_id());
  ASSERT_EQ(tablet_data_macro.tenant_seq(), tablet_data_macros.at(0).tenant_seq());
  ASSERT_EQ(tablet_data_macro.reorganization_scn(), tablet_data_macros.at(0).reorganization_scn());

  // test3: list private meta macro
  MacroBlockId tablet_meta_macro;
  tablet_id = 4;
  server_id = 5;
  seq_id = 7;
  tablet_meta_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_META_MACRO));
  tablet_meta_macro.set_second_id(tablet_id);
  tablet_meta_macro.set_reorganization_scn(0); // private_macro_third_id
  tablet_meta_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_macro));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tablet_meta_macros;
  tablet_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(tablet_id, tablet_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_macro_file(tablet_ids.at(0), 0/*private_macro_third_id*/, tablet_meta_macros));
  ASSERT_EQ(2, tablet_meta_macros.count());
  bool found_data_macro = false;
  bool found_meta_macro = false;
  for (int64_t i = 0; i < tablet_meta_macros.count(); ++i) {
    const MacroBlockId &macro_id = tablet_meta_macros.at(i);
    if (ObStorageObjectType::PRIVATE_DATA_MACRO == macro_id.storage_object_type()) {
      ASSERT_EQ(tablet_data_macro.second_id(), macro_id.second_id());
      ASSERT_EQ(tablet_data_macro.third_id(), macro_id.third_id());
      ASSERT_EQ(tablet_data_macro.tenant_seq(), macro_id.tenant_seq());
      ASSERT_EQ(tablet_data_macro.reorganization_scn(), macro_id.reorganization_scn());
      found_data_macro = true;
    } else if (ObStorageObjectType::PRIVATE_META_MACRO == macro_id.storage_object_type()) {
      ASSERT_EQ(tablet_meta_macro.second_id(), macro_id.second_id());
      ASSERT_EQ(tablet_meta_macro.third_id(), macro_id.third_id());
      ASSERT_EQ(tablet_meta_macro.tenant_seq(), macro_id.tenant_seq());
      ASSERT_EQ(tablet_meta_macro.reorganization_scn(), macro_id.reorganization_scn());
      found_meta_macro = true;
    }
  }
  ASSERT_TRUE(found_data_macro);
  ASSERT_TRUE(found_meta_macro);

  // test4: list private tablet meta version
  int64_t meta_version_id = 10;
  MacroBlockId tablet_meta_version;
  tablet_meta_version.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  tablet_meta_version.set_second_id(tablet_id);
  tablet_meta_version.set_reorganization_scn(tablet_meta_macro.reorganization_scn());
  tablet_meta_version.set_meta_version_id(meta_version_id);
  tablet_meta_version.set_fifth_id(0);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_version));
  write_info.set_ls_epoch_id(ls_epoch_id);
  write_info.set_ls_id(ls_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // test6: list shared tenant dir
  MacroBlockId tenant_root_key;
  tenant_root_key.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TENANT_ROOT_KEY));
  tenant_root_key.set_second_id(1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tenant_root_key));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  
  ObArray<uint64_t> tenant_ids;
  uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_shared_tenant_ids(MTL_ID(), tenant_ids));
  ASSERT_EQ(1, tenant_ids.count());
  ASSERT_EQ(tenant_id, tenant_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(tenant_root_key));
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tenant_root_key, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  tenant_ids.reset();
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_shared_tenant_ids(MTL_ID(), tenant_ids));
  // NOTICE: if object_storage opens multi_version mode, here 'tenant_ids.count()=1'
  ASSERT_EQ(0, tenant_ids.count());

  // test7: list shared major data macro
  MacroBlockId shared_major_data_macro;
  tablet_id = 13;
  seq_id = 14;
  int64_t reorganization_scn = 0;
  shared_major_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  shared_major_data_macro.set_second_id(tablet_id);
  shared_major_data_macro.set_reorganization_scn(reorganization_scn);
  shared_major_data_macro.set_seq_id(seq_id);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(shared_major_data_macro));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // test8: delete shared tablet data dir
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(shared_major_data_macro, 0, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(tablet_id, reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(shared_major_data_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);

  // test11: delete tmp file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(tmp_file));

  // test12: delete private macro files
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_files(tablet_meta_macros));

  // test13: delete ls dir file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_ls_dir(ls_id, ls_epoch_id));
}

TEST_F(TestFileManager, test_delete_tmp_seq_files)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  tenant_file_mgr->tmp_seq_file_clean_task_.destroy();
  // step 1: write tmp_seq_file
  uint64_t tablet_id = 100;
  uint64_t server_id = 1;
  LOG_INFO("start to write tmp seq file", K(tablet_id), K(server_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/));
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  const int64_t write_io_size = 2 * 1024 * 1024; // 2MB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_file_info;
  write_file_info.buffer_ = write_buf;
  write_file_info.offset_ = 0;
  write_file_info.size_ = write_io_size;
  write_file_info.io_desc_.set_wait_event(1);
  write_file_info.mtl_tenant_id_ = MTL_ID();
  write_file_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_file_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0, 1);
  ObSSMacroCacheStat file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_file_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, file_cache_stat));
  ObAtomicWriteFileInfo atomic_write_file;
  // step 2: check map before wait fail
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_from_atomic_write_file_map(false/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_EQ(MTL_EPOCH_ID(), atomic_write_file.ls_epoch_id_);
  ASSERT_EQ(write_io_size, atomic_write_file.size_);
  ASSERT_NE(OB_SUCCESS, write_object_handle.wait());
  TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0, 0);
  // step 3: check map and tmp file after wait fail
  ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_file_mgr->get_from_atomic_write_file_map(false/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_from_atomic_write_file_map(true/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_EQ(MTL_EPOCH_ID(), atomic_write_file.ls_epoch_id_);
  ASSERT_EQ(write_io_size, atomic_write_file.size_);
  write_object_handle.reset();
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_atomic_write_ctx(
      macro_id, atomic_write_file.ls_epoch_id_, atomic_write_file.seq_, GCONF.observer_id));
  ObIODFileStat stat;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(ctx.get_path(), stat));
  // step 4: sleep CLEAN_MAP_SAFE_DEL_TIME_S + 1 and clean
  const int64_t CLEAN_MAP_SAFE_DEL_TIME_S = 5L; // 5s
  sleep(CLEAN_MAP_SAFE_DEL_TIME_S + 1);
  tenant_file_mgr->tmp_seq_file_clean_task_.init();
  tenant_file_mgr->tmp_seq_file_clean_task_.runTimerTask();
  ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_file_mgr->get_from_atomic_write_file_map(true/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_NE(OB_SUCCESS, ObIODeviceLocalFileOp::stat(ctx.get_path(), stat));
}

TEST_F(TestFileManager, test_delete_not_exist_tmp_seq_file)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  tenant_file_mgr->tmp_seq_file_clean_task_.destroy();
  // step 1: write tmp_seq_file
  uint64_t tablet_id = 100;
  uint64_t server_id = 1;
  LOG_INFO("start to write tmp seq file", K(tablet_id), K(server_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*private_macro_third_id*/));
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  const int64_t write_io_size = 2 * 1024 * 1024; // 2MB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_file_info;
  write_file_info.buffer_ = write_buf;
  write_file_info.offset_ = 0;
  write_file_info.size_ = write_io_size;
  write_file_info.io_desc_.set_wait_event(1);
  write_file_info.mtl_tenant_id_ = MTL_ID();
  write_file_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_file_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0, 1);
  ObSSMacroCacheStat file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_file_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, file_cache_stat));
  ObAtomicWriteFileInfo atomic_write_file;
  // step 2: check map before wait fail
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_from_atomic_write_file_map(false/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_EQ(MTL_EPOCH_ID(), atomic_write_file.ls_epoch_id_);
  ASSERT_EQ(write_io_size, atomic_write_file.size_);
  ASSERT_NE(OB_SUCCESS, write_object_handle.wait());
  TP_SET_EVENT(EventTable::EN_FILE_SYSTEM_RENAME_ERROR, OB_ERR_SYS, 0, 0);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_from_atomic_write_file_map(true/*is_clean*/, macro_id, atomic_write_file));
  // step 3: delete tmp seq file in disk
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_atomic_write_ctx(
      macro_id, atomic_write_file.ls_epoch_id_, atomic_write_file.seq_, GCONF.observer_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(ctx.get_path()));
  // step 4: sleep CLEAN_MAP_SAFE_DEL_TIME_S + 1 and clean
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, file_cache_stat));
  int64_t used_size = file_cache_stat.used_;
  const int64_t CLEAN_MAP_SAFE_DEL_TIME_S = 5L; // 5s
  sleep(CLEAN_MAP_SAFE_DEL_TIME_S + 1);
  tenant_file_mgr->tmp_seq_file_clean_task_.init();
  tenant_file_mgr->tmp_seq_file_clean_task_.runTimerTask();
  ASSERT_EQ(OB_HASH_NOT_EXIST, tenant_file_mgr->get_from_atomic_write_file_map(true/*is_clean*/, macro_id, atomic_write_file));
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, file_cache_stat));
  // if tmp seq file is not exist, the used size should be the same
  ASSERT_EQ(used_size, file_cache_stat.used_);
}

TEST_F(TestFileManager, test_list_local_files_rec)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // test1: list tmp file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 3;
  int64_t segment_file_id = 2;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  TestDirDelOp dir_del_op;
  TestFileDelOp file_del_op;
  char file_path[common::MAX_PATH_SIZE] = {0};
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_dir(file_path, sizeof(file_path), MTL_ID(), MTL_EPOCH_ID()));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_local_files_rec(file_path, file_del_op, dir_del_op));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tmp_file, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  ObArray<MacroBlockId> tmp_files;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tmp_file(tmp_files));
  ASSERT_EQ(0, tmp_files.count());

  // test2: list private tablet data macro
  MacroBlockId tablet_data_macro;
  int64_t tablet_id = 4;
  int64_t server_id = 5;
  int64_t seq_id = 6;
  tablet_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  tablet_data_macro.set_second_id(tablet_id);
  tablet_data_macro.set_reorganization_scn(0); // private_macro_third_id
  tablet_data_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_data_macro));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  MacroBlockId tablet_meta_macro;
  tablet_id = 4;
  server_id = 5;
  seq_id = 7;
  tablet_meta_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_META_MACRO));
  tablet_meta_macro.set_second_id(tablet_id);
  tablet_meta_macro.set_reorganization_scn(0); // private_macro_third_id
  tablet_meta_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_macro));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  file_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_data_dir(file_path, sizeof(file_path), MTL_ID(), MTL_EPOCH_ID()));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_local_files_rec(file_path, file_del_op, dir_del_op));
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tablet_data_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tablet_meta_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);

  ObArray<int64_t> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(0, tablet_ids.count());
}

TEST_F(TestFileManager, test_delete_tmp_seq_files_in_calibrate_task)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  int64_t seq_id = 100;
  char disk_space_meta_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(disk_space_meta_path, ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH, "%s/%lu_%ld/%s%s%ld",
                        OB_DIR_MGR.get_local_cache_root_dir(), MTL_ID(), MTL_EPOCH_ID(),
                        get_storage_objet_type_str(ObStorageObjectType::TENANT_DISK_SPACE_META), DEFAULT_TMP_STR, seq_id));
  ObIOFd fd;
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  int64_t written_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::open(disk_space_meta_path, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::write(fd, write_buf, write_io_size, written_size));
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
  volatile bool is_stop = false;
  ObDirCalcSizeOp del_tmp_seq_file_op(is_stop);
  del_tmp_seq_file_op.set_for_del_tmp_seq_files();
  const int64_t del_security_time_s = 5;  // 5s
  ASSERT_EQ(OB_SUCCESS, del_tmp_seq_file_op.set_tmp_seq_file_del_secy_time_s(del_security_time_s));
  ob_usleep(2 * 1000 * 1000L); // sleep 2s
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.delete_tmp_seq_files(del_tmp_seq_file_op));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
  ob_usleep(5 * 1000 * 1000L); // sleep 5s
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.delete_tmp_seq_files(del_tmp_seq_file_op));
  ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
}

TEST_F(TestFileManager, test_gc_local_major_data)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId shared_data_macro;
  int64_t tablet_id = 100;
  int64_t seq_id = 101;
  shared_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  shared_data_macro.set_second_id(tablet_id);
  shared_data_macro.set_seq_id(seq_id);

  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(
      shared_data_macro, 0/*ls_epoch_id*/, true/*is_local_cache*/, GCONF.observer_id));
  ObIOFd fd;
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  int64_t written_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::open(ctx.get_path(), O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::write(fd, write_buf, write_io_size, written_size));
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf));
  ob_usleep(2 * 1000 * 1000L); // sleep 2s
  int64_t del_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_local_major_data_dir(del_time_s));
  ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf));
}

// ----------------------------------------------------------------------------
// TMP 文件逻辑删除只重命名并延迟释放空间，物理清理后才回收磁盘记账
//
// 场景：创建一个本地 TMP segment，暂停 TMP GC 后删除该文件，再恢复 GC 清理
//       .deleted 文件。
//
// 操作：
// - 写入 TMP segment 并记录 TMP_FILE used。
// - 暂停 GC，触发逻辑删除并检查原路径和 .deleted 路径。
// - 检查 rename 发布的 epoch/inflight 状态。
// - 扫描 tmp_data，确认 .deleted 文件仍计入磁盘占用。
// - 恢复 GC，物理删除 .deleted 文件。
// - 对已经不存在的文件再次执行逻辑删除。
//
// 校验：
// - 逻辑删除后原路径消失，.deleted 路径存在，TMP_FILE used 不变。
// - rename 成功使 epoch 增加，完成后 inflight 归零。
// - 校准扫描包含 .deleted 文件大小。
// - 物理删除后 .deleted 路径消失，TMP_FILE used 减少 write_io_size。
// - 重复删除不存在的文件仍返回成功，epoch 不增加且 inflight 恢复调用前状态。
// ----------------------------------------------------------------------------
TEST_F(TestFileManager, test_deleted_file_operator)
{
  LOG_INFO("========== test_deleted_file_operator begin ==========");
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 10;
  int64_t segment_file_id = 11;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  // step 1: write tmp_file
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObSSMacroCacheStat tmp_file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  int64_t tmp_file_used_size = tmp_file_cache_stat.used_;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(tmp_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 2: delete tmp_file when pause_gc
  tenant_file_mgr->set_tmp_file_cache_pause_gc();
  const int64_t logical_delete_epoch_before = tenant_file_mgr->get_tmp_logical_delete_epoch();
  OK(ObSSObjectAccessUtil::delete_local_file(tmp_file));
  const int64_t logical_delete_epoch_after = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t logical_delete_inflight = tenant_file_mgr->get_tmp_logical_delete_inflight();
  LOG_INFO("logical delete published a stable directory epoch",
      K(logical_delete_epoch_before), K(logical_delete_epoch_after), K(logical_delete_inflight));
  ASSERT_EQ(logical_delete_epoch_before + 1, logical_delete_epoch_after);
  ASSERT_EQ(0, logical_delete_inflight);
  // old path has been rename, old path is not exist
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(tmp_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS,
            ctx.set_logical_delete_ctx(tmp_file, 0/*ls_epoch_id*/, GCONF.observer_id));
  // old path has been rename, new path is exist
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_TRUE(is_exist);
  // delete tmp_file when pause_gc, rename file path, so tmp_file_write_cache_alloc_size does not changed
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  ASSERT_EQ(tmp_file_used_size, tmp_file_cache_stat.used_);

  // step 3：验证稳定扫描会按 .deleted 新路径统计文件大小。
  ObCalibrateDiskSpaceResult deleted_file_scan_result;
  const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s() + 1;
  OK(tenant_file_mgr->calc_tmp_file_disk_space(start_calc_size_time_s, deleted_file_scan_result));
  LOG_INFO("stable scan counted the logically deleted tmp file",
      K(start_calc_size_time_s), K(write_io_size), K(deleted_file_scan_result));
  ASSERT_GE(deleted_file_scan_result.total_file_size_, write_io_size);

  // step 4: delete .deleted file
  tenant_file_mgr->set_tmp_file_cache_allow_gc();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file());
  // after calibrate, rm_logical_deleted_file, so renamed file path is not exist
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_FALSE(is_exist);
  // delete .deleted file, so tmp_file_write_cache_alloc_size reduce
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  ASSERT_EQ(tmp_file_used_size - write_io_size, tmp_file_cache_stat.used_);

  // step 5：重复删除不存在的文件，验证 ENOENT 兼容路径不会污染 rename 状态。
  const int64_t missing_file_epoch_before = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t missing_file_inflight_before = tenant_file_mgr->get_tmp_logical_delete_inflight();
  OK(tenant_file_mgr->logical_delete_local_tmp_file(tmp_file, true /*is_del_seg_meta*/));
  const int64_t missing_file_epoch_after = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t missing_file_inflight_after = tenant_file_mgr->get_tmp_logical_delete_inflight();
  LOG_INFO("logical delete ENOENT path kept directory mutation state unchanged",
      K(missing_file_epoch_before), K(missing_file_epoch_after),
      K(missing_file_inflight_before), K(missing_file_inflight_after));
  ASSERT_EQ(missing_file_epoch_before, missing_file_epoch_after);
  ASSERT_EQ(missing_file_inflight_before, missing_file_inflight_after);
  LOG_INFO("========== test_deleted_file_operator end ==========");
}

// ----------------------------------------------------------------------------
// TMP 校准遇到并发逻辑删除会丢弃首轮结果，并在重试中统计 .deleted 文件
//
// 场景：创建一个本地 TMP segment，在首轮校准记录 epoch/inflight 后、扫描目录前暂停，
//       并由另一线程将该 segment 逻辑删除为 .deleted。
//
// 操作：DEBUG_SYNC 连续命中两次；第一次命中时执行 logical rename，第二次仅放行，
//       用第二次命中证明首轮结果已被丢弃并进入重试。随后执行稳定扫描作为结果基准。
//
// 校验：同步 action 因两次命中而耗尽；rename 使 epoch 增加且 inflight 归零；
//       校准重试后成功，返回结果与稳定扫描逐字段一致，并包含 .deleted 文件大小。
// ----------------------------------------------------------------------------
TEST_F(TestFileManager, test_tmp_file_calibration_retry_on_logical_delete)
{
  LOG_INFO("========== test_tmp_file_calibration_retry_on_logical_delete begin ==========");
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);
  const uint64_t tenant_id = MTL_ID();
  const int64_t tmp_file_id = 20;
  const int64_t segment_file_id = 21;
  const int64_t write_io_size = 8 * 1024;

  // step 1：创建本地 TMP segment，作为校准和逻辑删除并发访问的目标文件。
  MacroBlockId tmp_file;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  OK(write_object_handle.set_macro_block_id(tmp_file));
  char write_buf[write_io_size] = { 0 };
  MEMSET(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = tenant_id;
  OK(OB_DIR_MGR.create_tmp_file_dir(tenant_id, MTL_EPOCH_ID(), tmp_file_id));
  OK(ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  bool is_local_file_exist = false;
  OK(tenant_file_mgr->is_exist_local_file(
      tmp_file, 0 /*ls_epoch_id*/, is_local_file_exist));
  ASSERT_TRUE(is_local_file_exist);

  // step 2：启用 DEBUG_SYNC，并在每轮读取 epoch/inflight 后、扫描目录前设置同步点。
  constexpr int64_t DEBUG_SYNC_TIMEOUT_US = 5 * 1000 * 1000L;
  common::ObMalloc debug_sync_allocator;
  common::ObDSSessionActions point_actions;
  common::ObDSSessionActions control_actions;
  OK(point_actions.init(1024 /*page_size*/, debug_sync_allocator));
  OK(control_actions.init(1024 /*page_size*/, debug_sync_allocator));
  OK(GDS.add_debug_sync("reset", false /*is_global*/, control_actions));
  OK(GDS.add_debug_sync(
      "BEFORE_TMP_FILE_CALIBRATE_SCAN signal tmp_scan_ready "
      "wait_for tmp_scan_continue timeout 5000000 execute 2",
      false /*is_global*/, point_actions));
  OK(GDS.set_thread_local_actions(point_actions));
  const int64_t original_debug_sync_timeout_us = GCONF.debug_sync_timeout;
  GCONF.debug_sync_timeout = DEBUG_SYNC_TIMEOUT_US;
  const bool is_debug_sync_enabled = GCONF.is_debug_sync_enabled();

  tenant_file_mgr->set_tmp_file_cache_pause_gc();
  const int64_t epoch_before = tenant_file_mgr->get_tmp_logical_delete_epoch();
  std::atomic<int> first_wait_ret(OB_EAGAIN);
  std::atomic<int> second_wait_ret(OB_EAGAIN);
  std::atomic<int> switch_ret(OB_EAGAIN);
  std::atomic<int> delete_ret(OB_EAGAIN);
  std::atomic<int> first_signal_ret(OB_EAGAIN);
  std::atomic<int> second_signal_ret(OB_EAGAIN);

  std::thread remover([&]() {
    int ret = GDS.add_debug_sync(
        "now wait_for tmp_scan_ready timeout 5000000",
        false /*is_global*/, control_actions);
    first_wait_ret.store(ret);
    int local_switch_ret = OB_EAGAIN;
    int local_delete_ret = OB_EAGAIN;
    if (OB_SUCC(ret)) {
      share::ObTenantSwitchGuard tenant_guard;
      local_switch_ret = tenant_guard.switch_to(tenant_id);
      if (OB_SUCCESS == local_switch_ret) {
        local_delete_ret = ObSSObjectAccessUtil::delete_local_file(tmp_file);
      }
    }
    switch_ret.store(local_switch_ret);
    delete_ret.store(local_delete_ret);
    first_signal_ret.store(GDS.add_debug_sync(
        "now signal tmp_scan_continue", false /*is_global*/, control_actions));

    // 第二次命中只负责放行，用于证明校准确实丢弃首轮结果并进入了下一轮扫描。
    ret = GDS.add_debug_sync(
        "now wait_for tmp_scan_ready timeout 5000000",
        false /*is_global*/, control_actions);
    second_wait_ret.store(ret);
    second_signal_ret.store(GDS.add_debug_sync(
        "now signal tmp_scan_continue", false /*is_global*/, control_actions));
  });

  // step 3：执行并发校准；首轮 epoch 变化必须触发重试，后续稳定扫描返回成功。
  ObCalibrateDiskSpaceResult calibrate_res;
  const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s() + 1;
  const int scan_ret = tenant_file_mgr->calc_tmp_file_disk_space(
      start_calc_size_time_s, calibrate_res);
  remover.join();
  const common::ObDSActionArray *local_actions = GDS.thread_local_actions();
  const bool is_scan_action_consumed = OB_NOT_NULL(local_actions)
      && !local_actions->is_active(BEFORE_TMP_FILE_CALIBRATE_SCAN);
  point_actions.reset();
  const int clear_thread_actions_ret = GDS.set_thread_local_actions(point_actions);
  const int reset_ret = GDS.add_debug_sync("reset", false /*is_global*/, control_actions);
  GCONF.debug_sync_timeout = original_debug_sync_timeout_us;
  const int64_t restored_debug_sync_timeout_us = GCONF.debug_sync_timeout;

  // step 4：在同一磁盘状态下执行无并发扫描，作为并发重试结果的完整基准。
  ObCalibrateDiskSpaceResult stable_calibrate_res;
  const int stable_scan_ret = tenant_file_mgr->calc_tmp_file_disk_space(
      start_calc_size_time_s, stable_calibrate_res);

  const int64_t epoch_after = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t inflight_after = tenant_file_mgr->get_tmp_logical_delete_inflight();
  ObPathContext deleted_path_ctx;
  const int path_ret = deleted_path_ctx.set_logical_delete_ctx(
      tmp_file, 0 /*ls_epoch_id*/, GCONF.observer_id);
  bool is_deleted_file_exist = false;
  const int exist_ret = ((OB_SUCCESS == path_ret)
      ? ObIODeviceLocalFileOp::exist(deleted_path_ctx.get_path(), is_deleted_file_exist)
      : path_ret);

  // step 5：先恢复 GC 并完成清理，再统一断言，避免失败断言留下线程或测试文件。
  tenant_file_mgr->set_tmp_file_cache_allow_gc();
  const int cleanup_deleted_ret = tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file();
  const int cleanup_original_ret = ObSSObjectAccessUtil::delete_local_file(tmp_file);
  const int first_wait_result = first_wait_ret.load();
  const int second_wait_result = second_wait_ret.load();
  const int switch_result = switch_ret.load();
  const int delete_result = delete_ret.load();
  const int first_signal_result = first_signal_ret.load();
  const int second_signal_result = second_signal_ret.load();
  LOG_INFO("tmp file calibration retry control result",
      K(scan_ret), K(stable_scan_ret), K(first_wait_result), K(second_wait_result),
      K(switch_result), K(delete_result), K(is_scan_action_consumed),
      K(first_signal_result), K(second_signal_result), K(clear_thread_actions_ret), K(reset_ret),
      K(is_debug_sync_enabled));
  LOG_INFO("tmp file calibration retry state result",
      K(original_debug_sync_timeout_us), K(restored_debug_sync_timeout_us), K(epoch_before),
      K(epoch_after), K(inflight_after), K(path_ret), K(exist_ret), K(is_deleted_file_exist),
      K(calibrate_res), K(stable_calibrate_res), K(cleanup_deleted_ret), K(cleanup_original_ret));

  ASSERT_EQ(OB_SUCCESS, first_wait_result);
  ASSERT_EQ(OB_SUCCESS, second_wait_result);
  ASSERT_TRUE(is_scan_action_consumed);
  ASSERT_EQ(OB_SUCCESS, switch_result);
  ASSERT_EQ(OB_SUCCESS, delete_result);
  ASSERT_EQ(OB_SUCCESS, first_signal_result);
  ASSERT_EQ(OB_SUCCESS, second_signal_result);
  ASSERT_EQ(OB_SUCCESS, scan_ret);
  ASSERT_EQ(OB_SUCCESS, stable_scan_ret);
  ASSERT_EQ(OB_SUCCESS, clear_thread_actions_ret);
  ASSERT_EQ(OB_SUCCESS, reset_ret);
  ASSERT_TRUE(is_debug_sync_enabled);
  ASSERT_EQ(original_debug_sync_timeout_us, restored_debug_sync_timeout_us);
  ASSERT_EQ(epoch_before + 1, epoch_after);
  ASSERT_EQ(0, inflight_after);
  ASSERT_EQ(OB_SUCCESS, path_ret);
  ASSERT_EQ(OB_SUCCESS, exist_ret);
  ASSERT_TRUE(is_deleted_file_exist);
  ASSERT_GE(calibrate_res.total_file_size_, write_io_size);
  ASSERT_EQ(stable_calibrate_res.total_file_size_, calibrate_res.total_file_size_);
  ASSERT_EQ(stable_calibrate_res.hot_tablet_total_used_size_,
      calibrate_res.hot_tablet_total_used_size_);
  ASSERT_EQ(stable_calibrate_res.del_tmp_seq_file_size_, calibrate_res.del_tmp_seq_file_size_);
  ASSERT_EQ(stable_calibrate_res.tmp_seq_file_size_, calibrate_res.tmp_seq_file_size_);
  ASSERT_EQ(stable_calibrate_res.tmp_seq_file_cnt_, calibrate_res.tmp_seq_file_cnt_);
  ASSERT_EQ(stable_calibrate_res.total_dir_size_, calibrate_res.total_dir_size_);
  ASSERT_EQ(stable_calibrate_res.total_dir_num_, calibrate_res.total_dir_num_);
  ASSERT_EQ(OB_SUCCESS, cleanup_deleted_ret);
  ASSERT_EQ(OB_SUCCESS, cleanup_original_ret);
  LOG_INFO("========== test_tmp_file_calibration_retry_on_logical_delete end ==========");
}

// ----------------------------------------------------------------------------
// TMP 校准完成目录扫描后发生逻辑删除时，会通过 after epoch 校验丢弃结果并重试
//
// 场景：创建一个本地 TMP segment，在首轮目录扫描完成、读取 after epoch/inflight 前暂停，
//       由另一线程将该 segment 逻辑删除为 .deleted。
//
// 操作：AFTER DEBUG_SYNC 连续命中两次；第一次命中时执行 logical rename，第二次仅放行，
//       用 action 耗尽证明首轮结果已被丢弃并进入重试。
//
// 校验：第一次等待在 timeout 前收到 scan-finished signal；rename 使 epoch 增加且
//       inflight 归零；校准重试后成功，结果与无并发稳定扫描逐字段一致。
// ----------------------------------------------------------------------------
TEST_F(TestFileManager, test_tmp_file_calibration_retry_on_logical_delete_after_scan)
{
  LOG_INFO("========== test_tmp_file_calibration_retry_on_logical_delete_after_scan begin ==========");
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);
  const uint64_t tenant_id = MTL_ID();
  const int64_t tmp_file_id = 30;
  const int64_t segment_file_id = 31;
  const int64_t write_io_size = 8 * 1024;

  // step 1：创建首轮扫描完成后再执行逻辑删除的目标 TMP segment。
  MacroBlockId tmp_file;
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_fourth_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  OK(write_object_handle.set_macro_block_id(tmp_file));
  char write_buf[write_io_size] = { 0 };
  MEMSET(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = tenant_id;
  OK(OB_DIR_MGR.create_tmp_file_dir(tenant_id, MTL_EPOCH_ID(), tmp_file_id));
  OK(ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  bool is_local_file_exist = false;
  OK(tenant_file_mgr->is_exist_local_file(
      tmp_file, 0 /*ls_epoch_id*/, is_local_file_exist));
  ASSERT_TRUE(is_local_file_exist);

  // step 2：在每轮扫描完成后、读取 after epoch/inflight 前设置同步点。
  constexpr int64_t DEBUG_SYNC_TIMEOUT_US = 5_s;
  constexpr int64_t SIGNAL_WAIT_LIMIT_US = DEBUG_SYNC_TIMEOUT_US * 9 / 10;
  common::ObMalloc debug_sync_allocator;
  common::ObDSSessionActions point_actions;
  common::ObDSSessionActions control_actions;
  OK(point_actions.init(1024 /*page_size*/, debug_sync_allocator));
  OK(control_actions.init(1024 /*page_size*/, debug_sync_allocator));
  OK(GDS.add_debug_sync("reset", false /*is_global*/, control_actions));
  OK(GDS.add_debug_sync(
      "AFTER_TMP_FILE_CALIBRATE_SCAN signal tmp_after_scan_ready "
      "wait_for tmp_after_scan_continue timeout 5000000 execute 2",
      false /*is_global*/, point_actions));
  OK(GDS.set_thread_local_actions(point_actions));
  const int64_t original_debug_sync_timeout_us = GCONF.debug_sync_timeout;
  GCONF.debug_sync_timeout = DEBUG_SYNC_TIMEOUT_US;
  const bool is_debug_sync_enabled = GCONF.is_debug_sync_enabled();

  tenant_file_mgr->set_tmp_file_cache_pause_gc();
  const int64_t epoch_before = tenant_file_mgr->get_tmp_logical_delete_epoch();
  std::atomic<int> first_wait_ret(OB_EAGAIN);
  std::atomic<int> second_wait_ret(OB_EAGAIN);
  std::atomic<int> switch_ret(OB_EAGAIN);
  std::atomic<int> delete_ret(OB_EAGAIN);
  std::atomic<int> first_signal_ret(OB_EAGAIN);
  std::atomic<int> second_signal_ret(OB_EAGAIN);
  std::atomic<int64_t> first_wait_cost_us(DEBUG_SYNC_TIMEOUT_US);

  std::thread remover([&]() {
    const int64_t first_wait_start_us = ObTimeUtility::current_time_us();
    int ret = GDS.add_debug_sync(
        "now wait_for tmp_after_scan_ready timeout 5000000",
        false /*is_global*/, control_actions);
    first_wait_cost_us.store(ObTimeUtility::current_time_us() - first_wait_start_us);
    first_wait_ret.store(ret);
    int local_switch_ret = OB_EAGAIN;
    int local_delete_ret = OB_EAGAIN;
    if (OB_SUCC(ret)) {
      share::ObTenantSwitchGuard tenant_guard;
      local_switch_ret = tenant_guard.switch_to(tenant_id);
      if (OB_SUCCESS == local_switch_ret) {
        local_delete_ret = ObSSObjectAccessUtil::delete_local_file(tmp_file);
      }
    }
    switch_ret.store(local_switch_ret);
    delete_ret.store(local_delete_ret);
    first_signal_ret.store(GDS.add_debug_sync(
        "now signal tmp_after_scan_continue", false /*is_global*/, control_actions));

    // 第二次命中只放行重试扫描，并用 action 耗尽证明 after 检查触发了重试。
    ret = GDS.add_debug_sync(
        "now wait_for tmp_after_scan_ready timeout 5000000",
        false /*is_global*/, control_actions);
    second_wait_ret.store(ret);
    second_signal_ret.store(GDS.add_debug_sync(
        "now signal tmp_after_scan_continue", false /*is_global*/, control_actions));
  });

  // step 3：首轮扫描后完成 rename；after epoch 变化必须使该轮结果失效。
  ObCalibrateDiskSpaceResult calibrate_res;
  const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s() + 1;
  const int scan_ret = tenant_file_mgr->calc_tmp_file_disk_space(
      start_calc_size_time_s, calibrate_res);
  remover.join();
  const common::ObDSActionArray *local_actions = GDS.thread_local_actions();
  const bool is_after_scan_action_consumed = OB_NOT_NULL(local_actions)
      && !local_actions->is_active(AFTER_TMP_FILE_CALIBRATE_SCAN);
  point_actions.reset();
  const int clear_thread_actions_ret = GDS.set_thread_local_actions(point_actions);
  const int reset_ret = GDS.add_debug_sync("reset", false /*is_global*/, control_actions);
  GCONF.debug_sync_timeout = original_debug_sync_timeout_us;
  const int64_t restored_debug_sync_timeout_us = GCONF.debug_sync_timeout;

  // step 4：在同一磁盘状态下执行稳定扫描，作为 after-check 重试结果的基准。
  ObCalibrateDiskSpaceResult stable_calibrate_res;
  const int stable_scan_ret = tenant_file_mgr->calc_tmp_file_disk_space(
      start_calc_size_time_s, stable_calibrate_res);
  const int64_t epoch_after = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t inflight_after = tenant_file_mgr->get_tmp_logical_delete_inflight();
  ObPathContext deleted_path_ctx;
  const int path_ret = deleted_path_ctx.set_logical_delete_ctx(
      tmp_file, 0 /*ls_epoch_id*/, GCONF.observer_id);
  bool is_deleted_file_exist = false;
  const int exist_ret = ((OB_SUCCESS == path_ret)
      ? ObIODeviceLocalFileOp::exist(deleted_path_ctx.get_path(), is_deleted_file_exist)
      : path_ret);

  // step 5：恢复全局状态并清理测试文件后，再统一断言。
  tenant_file_mgr->set_tmp_file_cache_allow_gc();
  const int cleanup_deleted_ret = tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file();
  const int cleanup_original_ret = ObSSObjectAccessUtil::delete_local_file(tmp_file);
  const int first_wait_result = first_wait_ret.load();
  const int second_wait_result = second_wait_ret.load();
  const int switch_result = switch_ret.load();
  const int delete_result = delete_ret.load();
  const int first_signal_result = first_signal_ret.load();
  const int second_signal_result = second_signal_ret.load();
  const int64_t first_wait_cost_us_value = first_wait_cost_us.load();
  LOG_INFO("tmp file after-scan retry control result",
      K(scan_ret), K(stable_scan_ret), K(first_wait_result), K(second_wait_result),
      K(first_wait_cost_us_value), K(switch_result), K(delete_result),
      K(is_after_scan_action_consumed), K(first_signal_result), K(second_signal_result),
      K(clear_thread_actions_ret), K(reset_ret), K(is_debug_sync_enabled));
  LOG_INFO("tmp file after-scan retry state result",
      K(original_debug_sync_timeout_us), K(restored_debug_sync_timeout_us),
      K(epoch_before), K(epoch_after), K(inflight_after), K(path_ret), K(exist_ret),
      K(is_deleted_file_exist), K(calibrate_res), K(stable_calibrate_res),
      K(cleanup_deleted_ret), K(cleanup_original_ret));

  OK(first_wait_result);
  OK(second_wait_result);
  ASSERT_LT(first_wait_cost_us_value, SIGNAL_WAIT_LIMIT_US);
  ASSERT_TRUE(is_after_scan_action_consumed);
  OK(switch_result);
  OK(delete_result);
  OK(first_signal_result);
  OK(second_signal_result);
  OK(scan_ret);
  OK(stable_scan_ret);
  OK(clear_thread_actions_ret);
  OK(reset_ret);
  ASSERT_TRUE(is_debug_sync_enabled);
  ASSERT_EQ(original_debug_sync_timeout_us, restored_debug_sync_timeout_us);
  ASSERT_EQ(epoch_before + 1, epoch_after);
  ASSERT_EQ(0, inflight_after);
  OK(path_ret);
  OK(exist_ret);
  ASSERT_TRUE(is_deleted_file_exist);
  ASSERT_GE(calibrate_res.total_file_size_, write_io_size);
  ASSERT_EQ(stable_calibrate_res.total_file_size_, calibrate_res.total_file_size_);
  ASSERT_EQ(stable_calibrate_res.hot_tablet_total_used_size_,
      calibrate_res.hot_tablet_total_used_size_);
  ASSERT_EQ(stable_calibrate_res.del_tmp_seq_file_size_, calibrate_res.del_tmp_seq_file_size_);
  ASSERT_EQ(stable_calibrate_res.tmp_seq_file_size_, calibrate_res.tmp_seq_file_size_);
  ASSERT_EQ(stable_calibrate_res.tmp_seq_file_cnt_, calibrate_res.tmp_seq_file_cnt_);
  ASSERT_EQ(stable_calibrate_res.total_dir_size_, calibrate_res.total_dir_size_);
  ASSERT_EQ(stable_calibrate_res.total_dir_num_, calibrate_res.total_dir_num_);
  OK(cleanup_deleted_ret);
  OK(cleanup_original_ret);
  LOG_INFO("========== test_tmp_file_calibration_retry_on_logical_delete_after_scan end ==========");
}

// ----------------------------------------------------------------------------
// TMP 校准在 rename 持续进行时耗尽重试，返回 OB_EAGAIN 且不泄露失败扫描结果
//
// 场景：预填充校准出参，并在整个调用期间将 tmp logical delete inflight 保持为非零。
// 操作：调用 calc_tmp_file_disk_space()，让三次扫描尝试都在扫描前返回 OB_EAGAIN，
//       调用结束后恢复注入的 inflight 计数。
// 校验：接口最终返回 OB_EAGAIN；epoch 不变、inflight 恢复原值；所有出参字段清零。
// ----------------------------------------------------------------------------
TEST_F(TestFileManager, test_tmp_file_calibration_retry_exhausted_by_inflight)
{
  LOG_INFO("========== test_tmp_file_calibration_retry_exhausted_by_inflight begin ==========");
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // 预填充出参，用于确认失败返回前会清除调用方遗留值。
  ObCalibrateDiskSpaceResult calibrate_res;
  calibrate_res.total_file_size_ = 1;
  calibrate_res.hot_tablet_total_used_size_ = 1;
  calibrate_res.del_tmp_seq_file_size_ = 1;
  calibrate_res.tmp_seq_file_size_ = 1;
  calibrate_res.tmp_seq_file_cnt_ = 1;
  calibrate_res.total_dir_size_ = 1;
  calibrate_res.total_dir_num_ = 1;

  // 在调用期间保持一个在途 rename，使三次尝试均走 OB_EAGAIN。
  const int64_t epoch_before = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t inflight_before = tenant_file_mgr->get_tmp_logical_delete_inflight();
  ATOMIC_INC(&tenant_file_mgr->tmp_logical_delete_inflight_);
  const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s() + 1;
  const int scan_ret = tenant_file_mgr->calc_tmp_file_disk_space(
      start_calc_size_time_s, calibrate_res);
  ATOMIC_DEC(&tenant_file_mgr->tmp_logical_delete_inflight_);
  const int64_t epoch_after = tenant_file_mgr->get_tmp_logical_delete_epoch();
  const int64_t inflight_after = tenant_file_mgr->get_tmp_logical_delete_inflight();

  LOG_INFO("tmp file calibration retry exhausted result",
      K(scan_ret), K(epoch_before), K(epoch_after), K(inflight_before), K(inflight_after),
      K(calibrate_res));
  ASSERT_EQ(OB_EAGAIN, scan_ret);
  ASSERT_EQ(epoch_before, epoch_after);
  ASSERT_EQ(inflight_before, inflight_after);
  ASSERT_EQ(0, calibrate_res.total_file_size_);
  ASSERT_EQ(0, calibrate_res.hot_tablet_total_used_size_);
  ASSERT_EQ(0, calibrate_res.del_tmp_seq_file_size_);
  ASSERT_EQ(0, calibrate_res.tmp_seq_file_size_);
  ASSERT_EQ(0, calibrate_res.tmp_seq_file_cnt_);
  ASSERT_EQ(0, calibrate_res.total_dir_size_);
  ASSERT_EQ(0, calibrate_res.total_dir_num_);
  LOG_INFO("========== test_tmp_file_calibration_retry_exhausted_by_inflight end ==========");
}

void check_object_type_stat(const MacroBlockId &macro_id,
                          const uint64_t read_cnt,
                          const uint64_t read_size,
                          const uint64_t write_cnt,
                          const uint64_t write_size,
                          ObStorageObjectHandle &object_handle)
{
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSSObjectTypeStat type_stat;
  bool is_remote = object_handle.get_io_handle().is_limit_net_bandwidth_req();
  ASSERT_EQ(OB_SUCCESS, local_cache_service->get_object_type_stat(macro_id.storage_object_type(), is_remote, type_stat));
  ObSSBaseStat stat;
  type_stat.get_stat(ObSSObjectTypeStatType::READ, stat);
  ASSERT_EQ(read_cnt, stat.get_cnt());
  ASSERT_EQ(read_size, stat.get_size());
  type_stat.get_stat(ObSSObjectTypeStatType::WRITE, stat);
  ASSERT_EQ(write_cnt, stat.get_cnt());
  ASSERT_EQ(write_size, stat.get_size());
}
TEST_F(TestFileManager, test_user_tenant_slog_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 101;
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(MTL_ID());
  slog_file.set_third_id(0);
  slog_file.set_fourth_id(MTL_EPOCH_ID());
  slog_file.set_fifth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  write_info.offset_ = write_io_size;
  write_object_handle.reset();
  check_object_type_stat(slog_file, 0/*read_cnt*/, 0/*read_size*/, 1/*write_cnt*/, write_io_size, write_object_handle);
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  check_object_type_stat(slog_file, 0/*read_cnt*/, 0/*read_size*/, 2/*write_cnt*/, write_io_size * 2, write_object_handle);
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  int64_t used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(MTL_ID(), MTL_EPOCH_ID(), slog_min_id, slog_max_id));
  ASSERT_EQ(file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(MTL_ID(), MTL_EPOCH_ID(), used_size));
  ASSERT_EQ(write_io_size * 2, used_size);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 2: seal slog_file
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.seal_object(slog_file, 0/*ls_epoch_id*/));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  // wait flush success
  ObSSMacroCacheFlushTask &flush_task = macro_cache_mgr->flush_task_;
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (tenant_file_mgr->flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(tenant_file_mgr->flushed_slog_seq_));
      break;
    }
  }
  // flush success
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 3: read slog_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();

  ASSERT_EQ(OB_SUCCESS, ObObjectManager::read_object(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // step 4: list slog dir and get used size
  slog_min_id = -1;
  slog_max_id = -1;
  used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(MTL_ID(), MTL_EPOCH_ID(), slog_min_id, slog_max_id));
  ASSERT_EQ(file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(MTL_ID(), MTL_EPOCH_ID(), used_size));
  ASSERT_EQ(write_io_size * 2, used_size);

  // step 5: delete slog_file
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_get_slog_id_range)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  const int64_t min_file_id = 1;

  // test 1: test slog dir is empty
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(min_file_id, slog_max_id);

  // step 2: test slog file exists only in local cache
  MacroBlockId slog_file;
  int64_t file_id = 104;
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(OB_SERVER_TENANT_ID);
  slog_file.set_third_id(0);
  slog_file.set_fourth_id(0/*epoch_id*/);
  slog_file.set_fifth_id(file_id);
  MacroBlockId slog_file2(slog_file);
  int64_t file_id2 = 105;
  slog_file2.set_fifth_id(file_id2);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 3: test slog file exists only in object storage
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.push_to_flush_map(slog_file));
  // wait flush success
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (OB_SERVER_FILE_MGR.flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(OB_SERVER_FILE_MGR.flushed_slog_seq_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 4: test slog file exists in local cache and object storage
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 5: test two slog files
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file2));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id2, slog_max_id);

  // step 6: delete slog_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file2));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
}

TEST_F(TestFileManager, test_server_tenant_slog_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  const int64_t min_file_id = 1;

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 106;
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(OB_SERVER_TENANT_ID);
  slog_file.set_third_id(0);
  slog_file.set_fourth_id(0/*epoch_id*/);
  slog_file.set_fifth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  int64_t old_used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(OB_SERVER_TENANT_ID, 0, old_used_size));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: seal slog_file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.push_to_flush_map(slog_file));
  // wait flush success
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (OB_SERVER_FILE_MGR.flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(OB_SERVER_FILE_MGR.flushed_slog_seq_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 3: read slog_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::read_object(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 4: list slog dir and get used size
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  int64_t used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(OB_SERVER_TENANT_ID, 0, used_size));
  ASSERT_EQ(old_used_size + write_io_size, used_size);
  // step 5: delete slog_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_slog_data_out_of_range)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 107;
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(MTL_ID());
  slog_file.set_third_id(0);
  slog_file.set_fourth_id(MTL_EPOCH_ID());
  slog_file.set_fifth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  write_object_handle.reset();
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 2: read local slog_file out of range
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();

  // (offset + size) > file_len
  read_info.offset_ = write_io_size / 2;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset > file_len
  read_info.offset_ = write_io_size + 4096;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset = file_len
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // step 3: seal slog_file
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.seal_object(slog_file, 0/*ls_epoch_id*/));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  // wait flush success
  ObSSMacroCacheFlushTask &flush_task = macro_cache_mgr->flush_task_;
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (tenant_file_mgr->flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(tenant_file_mgr->flushed_slog_seq_));
      break;
    }
  }
  // flush success
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 4: read remote slog_file out of range
  // (offset + size) > file_len
  read_info.offset_ = write_io_size / 2;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset > file_len
  read_info.offset_ = write_io_size + 4096;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset = file_len
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // step 5: delete slog_file
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_user_tenant_ckpt_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write ckpt_file
  MacroBlockId ckpt_file;
  int64_t file_id = 101;
  ckpt_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file.set_second_id(MTL_ID());
  ckpt_file.set_third_id(0);
  ckpt_file.set_fourth_id(MTL_EPOCH_ID());
  ckpt_file.set_fifth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: read ckpt_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = ckpt_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 3: delete ckpt_file
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(ckpt_file));
  check_object_type_stat(ckpt_file, 1/*delete_cnt*/);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_server_tenant_ckpt_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write ckpt_file
  MacroBlockId ckpt_file;
  int64_t file_id = 102;
  ckpt_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file.set_second_id(OB_SERVER_TENANT_ID);
  ckpt_file.set_third_id(0);
  ckpt_file.set_fourth_id(0/*epoch_id*/);
  ckpt_file.set_fifth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: read ckpt_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = ckpt_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 3: delete ckpt_file
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(ckpt_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

void exhaust_macro_file_disk_size(int64_t &avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  avail_size = disk_space_manager->get_allocated_shared_macro_cache_free_size_nolock_();
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->alloc_file_size(avail_size,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, disk_space_manager->alloc_file_size(8192,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}
void release_macro_file_disk_size(const int64_t avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->free_file_size(avail_size,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

// list cluster_id/tenant_id/tablet/tablet_id/reorganization_scn
TEST_F(TestFileManager, test_list_remote_macro_ids_of_user_tablet)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  int64_t tablet_id = 13;
  int64_t seq_id = 14;
  int64_t reorganization_scn = 0;
  ObArray<blocksstable::MacroBlockId> macro_ids;
  // test0: empty macro_ids
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(0, macro_ids.count());

  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  file_id.set_second_id(tablet_id);
  file_id.set_reorganization_scn(reorganization_scn);
  file_id.set_seq_id(seq_id);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  int64_t avail_size = 0;
  exhaust_macro_file_disk_size(avail_size);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1: write one macro file and list it
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  ASSERT_EQ(file_id, macro_ids.at(0));

  // test2: write two macro files and list them
  file_id.set_seq_id(seq_id + 1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(2, macro_ids.count());

  // test3: write shared tablet sub meta file and list
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  file_id.set_seq_id(seq_id + 2);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(3, macro_ids.count());
  // test4: write no macro file and expect list old three macro files
  release_macro_file_disk_size(avail_size);
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::MAJOR_PREWARM_DATA));
  file_id.set_fourth_id(seq_id + 2);
  file_id.set_fifth_id(0);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(3, macro_ids.count());

  // test5: delete three macro files and list them
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         false/*is_ls_inner_tablet*/, 0/*ls_id*/,
                                         tablet_id, reorganization_scn, macro_ids));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_file(file_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(tablet_id, reorganization_scn));
}

TEST_F(TestFileManager, test_list_remote_macro_ids_of_ls_inner_tablet)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  int64_t tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  int64_t seq_id = 100;
  int64_t meta_ls_id = 1;
  ObArray<blocksstable::MacroBlockId> macro_ids;
  // test0: empty macro_ids
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(0, macro_ids.count());

  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINI_DATA_MACRO));
  file_id.set_second_id(tablet_id);
  file_id.set_ls_id(meta_ls_id);
  file_id.set_seq_id(seq_id);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;

  int64_t avail_size = 0;
  exhaust_macro_file_disk_size(avail_size);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1: write one macro file and list it
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  ASSERT_EQ(file_id, macro_ids.at(0));

  // test2: write two macro files and list them
  file_id.set_seq_id(seq_id + 1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(2, macro_ids.count());
  // test3: write shared tablet sub meta file and list
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  file_id.set_seq_id(seq_id + 2);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(3, macro_ids.count());
  // test4: write no macro file and expect list old three macro files
  release_macro_file_disk_size(avail_size);
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::MAJOR_PREWARM_DATA));
  file_id.set_fourth_id(seq_id + 2);
  file_id.set_fifth_id(0);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(3, macro_ids.count());

  // test5: delete three macro files and list them
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files(
                                         true/*is_ls_inner_tablet*/, meta_ls_id,
                                         tablet_id, 0/*reorganization_scn*/, macro_ids));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_file(file_id));
}

// inner_tablet: cluster_id/tenant_id/ls/ls_id/TABLET_NAME/sstable_type_str/sstable/op_id
// user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/sstable_type_str/sstable/op_id
TEST_F(TestFileManager, test_list_remote_mini_macro_ids_by_opid)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  common::ObTabletID inner_tablet_id(49402);
  int64_t ls_id = 1007;
  int64_t reorganization_scn = 1;
  int64_t op_id = 7894;
  int64_t seq = 214;
  ObArray<blocksstable::MacroBlockId> macro_ids;
  ObArray<blocksstable::MacroBlockId> macro_ids_inner_tablet;
  // test0:empty macro_ids
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(0, macro_ids.count());

  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINI_DATA_MACRO));
  file_id.set_second_id(inner_tablet_id.id());
  file_id.set_ss_third_id(inner_tablet_id.is_ls_inner_tablet(), ls_id, reorganization_scn);
  file_id.set_op_id(op_id);
  file_id.set_seq_id(seq);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1:write SHARED_MINI_DATA_MACRO inner tablet and list it:
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(1, macro_ids_inner_tablet.count());
  ASSERT_EQ(file_id, macro_ids_inner_tablet.at(0));

  // test2: write SHARED_MINI_META_MACRO inner tablet and list it:
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINI_META_MACRO));
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(2, macro_ids_inner_tablet.count());

  // test3: write SHARED_MINI_DATA_MACRO not inner tablet and list it:
  common::ObTabletID tablet_id(200002);
  file_id.set_second_id(tablet_id.id());
  file_id.set_ss_third_id(tablet_id.is_ls_inner_tablet(), ls_id, reorganization_scn);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  
  // test4: write SHARED_MINI_DATA_MACRO not inner tablet and list it:
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINI_DATA_MACRO));
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(2, macro_ids.count());

  // test5: delete two macro files and list them
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids_inner_tablet));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINI_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(tablet_id.id(), reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(inner_tablet_id.id(), reorganization_scn));
}

TEST_F(TestFileManager, test_list_remote_minor_macro_ids_by_opid)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  common::ObTabletID inner_tablet_id(49402);
  int64_t ls_id = 1007;
  int64_t reorganization_scn = 1;
  int64_t op_id = 7894;
  int64_t seq = 214;
  ObArray<blocksstable::MacroBlockId> macro_ids;
  ObArray<blocksstable::MacroBlockId> macro_ids_inner_tablet;
  // test0:empty macro_ids
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(0, macro_ids.count());

  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  file_id.set_second_id(inner_tablet_id.id());
  file_id.set_ss_third_id(inner_tablet_id.is_ls_inner_tablet(), ls_id, reorganization_scn);
  file_id.set_op_id(op_id);
  file_id.set_seq_id(seq);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1:write SHARED_MINI_DATA_MACRO inner tablet and list it:
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(1, macro_ids_inner_tablet.count());
  ASSERT_EQ(file_id, macro_ids_inner_tablet.at(0));

  // test2: write SHARED_MINI_META_MACRO inner tablet and list it:
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_META_MACRO));
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(2, macro_ids_inner_tablet.count());

  // test3: write SHARED_MINI_DATA_MACRO not inner tablet and list it:
  common::ObTabletID tablet_id(200002);
  file_id.set_second_id(tablet_id.id());
  file_id.set_ss_third_id(tablet_id.is_ls_inner_tablet(), ls_id, reorganization_scn);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(1, macro_ids.count());
  
  // test4: write SHARED_MINOR_DATA_MACRO not inner tablet and list it:
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(2, macro_ids.count());

  // test5: delete two macro files and list them
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_files(macro_ids_inner_tablet));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_remote_macro_files_by_op(
    inner_tablet_id, ls_id, reorganization_scn, MINOR_DIR_STR, op_id, macro_ids_inner_tablet));
  ASSERT_EQ(0, macro_ids.count());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(tablet_id.id(), reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(inner_tablet_id.id(), reorganization_scn));
}

// list cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id
TEST_F(TestFileManager, test_list_private_ckpt_file)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId ckpt_file_id;
  int64_t object_id = 201;
  int64_t tenant_epoch_id = MTL_EPOCH_ID();
  ObArray<int64_t> object_ids;
  
  // test0: empty ckpt file list
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_ckpt_file(MTL_ID(), tenant_epoch_id, object_ids));
  ASSERT_EQ(0, object_ids.count());

  ckpt_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file_id.set_second_id(MTL_ID());
  ckpt_file_id.set_third_id(0);
  ckpt_file_id.set_fourth_id(tenant_epoch_id);
  ckpt_file_id.set_fifth_id(object_id);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(ckpt_file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1: write one ckpt file and list it
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_ckpt_file(MTL_ID(), tenant_epoch_id, object_ids));
  ASSERT_EQ(1, object_ids.count());
  ASSERT_EQ(object_id, object_ids.at(0));

  // test2: write two ckpt files and list them
  ckpt_file_id.set_fifth_id(object_id + 1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(ckpt_file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_ckpt_file(MTL_ID(), tenant_epoch_id, object_ids));
  ASSERT_EQ(2, object_ids.count());

  // test3: write third ckpt file and list
  ckpt_file_id.set_fifth_id(object_id + 2);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(ckpt_file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_ckpt_file(MTL_ID(), tenant_epoch_id, object_ids));
  ASSERT_EQ(3, object_ids.count());

  // test4: delete ckpt files and list them
  ckpt_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  for (int64_t i = 0; i < 3; ++i) {
    ckpt_file_id.set_fifth_id(object_id + i);
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_remote_file(ckpt_file_id));
  }
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_ckpt_file(MTL_ID(), tenant_epoch_id, object_ids));
  ASSERT_EQ(0, object_ids.count());
}

// list cluster_id/server_id/server_ckpt/object_id (server tenant)
TEST_F(TestFileManager, test_list_private_ckpt_file_server_tenant)
{
  int ret = OB_SUCCESS;
  MacroBlockId ckpt_file_id;
  int64_t object_id = 301;
  int64_t tenant_epoch_id = MTL_EPOCH_ID();
  ObArray<int64_t> object_ids;
  
  // test0: empty ckpt file list for server tenant
  // Now list_private_ckpt_file is in ObBaseFileManager and supports tenant_id parameter,
  // so we can use OB_SERVER_FILE_MGR.list_private_ckpt_file to list server tenant ckpt files
  
  ckpt_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file_id.set_second_id(OB_SERVER_TENANT_ID);
  ckpt_file_id.set_third_id(0);
  ckpt_file_id.set_fourth_id(0/*epoch_id*/);
  ckpt_file_id.set_fifth_id(object_id);
  ObStorageObjectHandle write_object_handle;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'b', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.is_exist_remote_file(ckpt_file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test1: verify server tenant ckpt file path is correct
  // The path should be: cluster_id/server_id/server_ckpt
  char ckpt_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_remote_private_ckpt_file_dir(
                         ckpt_path, sizeof(ckpt_path), OB_SERVER_TENANT_ID, tenant_epoch_id));
  // Verify the path contains "server_ckpt" for server tenant
  ASSERT_NE(nullptr, STRSTR(ckpt_path, "server_ckpt"));

  // test2: write two server tenant ckpt files
  ckpt_file_id.set_fifth_id(object_id + 1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.is_exist_remote_file(ckpt_file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_EQ(true, is_exist);

  // test3: list server tenant ckpt files using list_private_ckpt_file
  object_ids.reset();
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_private_ckpt_file(OB_SERVER_TENANT_ID, tenant_epoch_id, object_ids));
  ASSERT_EQ(2, object_ids.count());

  // test4: delete server tenant ckpt files and list again
  for (int64_t i = 0; i < 2; ++i) {
    ckpt_file_id.set_fifth_id(object_id + i);
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_remote_file(ckpt_file_id));
  }
  object_ids.reset();
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_private_ckpt_file(OB_SERVER_TENANT_ID, tenant_epoch_id, object_ids));
  ASSERT_EQ(0, object_ids.count());
}

// Test 1: verify get_dir_create_time returns a valid birth time,
//         and that subsequent file writes into the directory do NOT change the create time
//         but DO update the mtime.
// Test 2: write PRIVATE_TABLET_META into an existing directory, verify:
//         - directory create_time remains unchanged (< start_calc_size_time_s)
//         - directory mtime is updated (>= start_calc_size_time_s)
//         - calc_meta_file_disk_space correctly includes the directory (via create_time)
TEST_F(TestFileManager, test_dir_create_time_and_meta_write_impact)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);

  const int64_t ls_id = 55;
  const int64_t ls_epoch_id = 0;
  const int64_t tablet_id = 1001;
  const int64_t reorganization_scn = 0;
  const int64_t write_io_size = 4096;

  // --- step 1: write first PRIVATE_TABLET_META to create the directory structure ---
  MacroBlockId file_id_1;
  file_id_1.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  file_id_1.set_second_id(tablet_id);
  file_id_1.set_reorganization_scn(reorganization_scn);
  file_id_1.set_meta_version_id(100);
  file_id_1.set_fifth_id(0);

  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'x', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_ls_epoch_id(ls_epoch_id);
  write_info.set_ls_id(ls_id);

  ObStorageObjectHandle write_handle_1;
  ASSERT_EQ(OB_SUCCESS, write_handle_1.set_macro_block_id(file_id_1));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_handle_1));

  // --- step 2: record create_time and mtime of the reorganization_scn directory after creation.
  //
  //   Path structure:
  //     ls/{ls_id}_{ls_epoch}/tablet_meta/{scatter_id}/{tablet_id}/{reorganization_scn}/ver{version}.T10
  //
  //   Writing ver100.T10 creates the reorganization_scn subdir and places the file inside it.
  //   This directory's mtime gets bumped every
  //   time a new version is written into it — this is the exact directory level that
  //   caused the original bug: a write during calibration updates its mtime to
  //   >= start_calc_size_time_s, making mtime-based scan miss it. ---
  char reorganization_scn_dir[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_private_macro_third_id_dir(
      reorganization_scn_dir, sizeof(reorganization_scn_dir), MTL_ID(), MTL_EPOCH_ID(),
      ls_id, ls_epoch_id, tablet_id, reorganization_scn));

  int64_t create_time_after_mkdir = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::get_dir_create_time(
      reorganization_scn_dir, create_time_after_mkdir));
  ASSERT_NE(INT64_MAX, create_time_after_mkdir);
  ASSERT_GT(create_time_after_mkdir, 0);

  ObIODFileStat statbuf_after_mkdir;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(reorganization_scn_dir, statbuf_after_mkdir));
  const int64_t mtime_after_mkdir = statbuf_after_mkdir.mtime_s_;
  LOG_INFO("tablet meta reorganization scn dir stat after first write", K(reorganization_scn_dir),
           K(create_time_after_mkdir), K(mtime_after_mkdir));

  // --- step 3: sleep across a second boundary, then record start_calc_size_time_s ---
  // This simulates choose_start_calc_size_time_s: sleep until we are just past a second boundary.
  ob_usleep(2000 * 1000); // 2s, ensure we cross into a new second
  const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();

  // Verify the directory's create_time is before start_calc_size_time_s (should be counted)
  ASSERT_LT(create_time_after_mkdir, start_calc_size_time_s);
  // Verify mtime is also before start_calc_size_time_s (written before the sleep)
  ASSERT_LT(mtime_after_mkdir, start_calc_size_time_s);

  // --- step 4: write a NEW PRIVATE_TABLET_META into the SAME reorganization_scn directory
  //             AFTER start_calc_size_time_s.
  //             Writing ver200.T10 directly into it bumps that dir's mtime. ---
  MacroBlockId file_id_2;
  file_id_2.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  file_id_2.set_second_id(tablet_id);
  file_id_2.set_reorganization_scn(reorganization_scn);
  file_id_2.set_meta_version_id(200); // new version written after start_calc_size_time_s
  file_id_2.set_fifth_id(0);

  ObStorageObjectHandle write_handle_2;
  ASSERT_EQ(OB_SUCCESS, write_handle_2.set_macro_block_id(file_id_2));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_handle_2));

  // --- step 5: verify the reorganization_scn dir's mtime is updated (>= start_calc_size_time_s),
  //             but create_time remains unchanged ---
  int64_t create_time_after_write2 = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::get_dir_create_time(
      reorganization_scn_dir, create_time_after_write2));

  ObIODFileStat statbuf_after_write2;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(reorganization_scn_dir, statbuf_after_write2));
  const int64_t mtime_after_write2 = statbuf_after_write2.mtime_s_;

  LOG_INFO("tablet meta reorganization scn dir stat after second write", K(reorganization_scn_dir),
           K(create_time_after_write2), K(mtime_after_write2), K(start_calc_size_time_s));

  // create_time must be unchanged — birth time is immutable
  ASSERT_LE(create_time_after_mkdir, create_time_after_write2);
  // mtime must be updated by the second write (>= start_calc_size_time_s)
  ASSERT_GE(mtime_after_write2, start_calc_size_time_s);
  // create_time is still before start_calc_size_time_s — key invariant for the fix
  ASSERT_LE(create_time_after_write2, start_calc_size_time_s);

  // --- step 6: run calc_meta_file_disk_space and verify the directory IS counted.
  //             With the old mtime-based logic, this directory would be MISSED because
  //             its mtime >= start_calc_size_time_s. With create_time-based logic it
  //             should be correctly included. ---
  ObCalibrateDiskSpaceResult calibrate_res;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, calibrate_res));

  // The tablet_id directory itself must have been counted (total_dir_num_ > 0)
  ASSERT_GT(calibrate_res.total_dir_num_, 0);
  // total_file_size_ must include the file written before start_calc_size_time_s (file_id_1)
  // and the directory sizes; it must be > 0
  ASSERT_GT(calibrate_res.total_file_size_, 0);
  LOG_INFO("calibrate result after second write into existing dir", K(calibrate_res),
           K(start_calc_size_time_s), K(create_time_after_write2), K(mtime_after_write2));

  // --- step 7: cleanup ---
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(file_id_1, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file_with_ls_context(file_id_2, ls_epoch_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_private_macro_third_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
}

// ============================================================================
// 宏块分类迁移校准回归（7 个用例）
// A. 快照与提交：旧快照、扫描后迁移；
// B. 扫描并发：双向迁移、持续迁移及扫描中取消；
// C. 重试与退出：在途耗尽、跨秒重试、取消/未初始化；
// D. 转换回调：失败计数归零。
// ============================================================================

int TestFileManager::prepare_calibration_macro(const uint64_t tablet_id, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  constexpr int64_t WRITE_SIZE = 8_KB;
  char buffer[WRITE_SIZE] = {0};
  MEMSET(buffer, 'c', WRITE_SIZE);
  ObStorageCachePolicyService *policy = MTL(ObStorageCachePolicyService *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  char scatter_dir[common::MAX_PATH_SIZE] = {0};
  ObStorageObjectHandle handle;
  ObStorageObjectWriteInfo info;
  if (OB_ISNULL(policy) || OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get calibration test modules", KR(ret), KP(policy), KP(mgr));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_scatter_dir(
      scatter_dir, sizeof(scatter_dir), MTL_ID(), MTL_EPOCH_ID(), tablet_id))) {
    LOG_WARN("fail to get calibration test directory", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObDirManager::create_dir(scatter_dir, ObStorageObjectType::PRIVATE_DATA_MACRO))
      && OB_FILE_OR_DIRECTORY_EXIST != ret) {
    LOG_WARN("fail to create calibration test directory", KR(ret), K(scatter_dir));
  } else {
    ret = OB_SUCCESS;
    macro_id.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(0 /*reorganization_scn*/);
    macro_id.set_tenant_seq(71001 /*tenant_seq*/);
    info.buffer_ = buffer;
    info.size_ = WRITE_SIZE;
    info.offset_ = 0;
    info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    info.mtl_tenant_id_ = MTL_ID();
    info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
    if (OB_FAIL(policy->update_tablet_status(tablet_id, PolicyStatus::HOT, 200001 /*table_id*/))) {
      LOG_WARN("fail to set calibration test policy", KR(ret), K(tablet_id));
    } else if (OB_FAIL(handle.set_macro_block_id(macro_id))) {
      LOG_WARN("fail to set calibration test macro id", KR(ret), K(macro_id));
    } else if (OB_FAIL(ObSSObjectAccessUtil::write_file(info, handle))) {
      LOG_WARN("fail to write calibration test macro", KR(ret), K(macro_id));
    }
  }
  LOG_INFO("prepared calibration test macro", KR(ret), K(macro_id), K(WRITE_SIZE));
  return ret;
}

// 普通/强制校准均刷新 META 阶段以前的旧快照，
// 不重复叠加已完成的 HOT->AUTO 迁移。
// 场景：先取 HOT 初始用量，再转 AUTO；
// 操作：携带旧快照调用完整宏块校准。
// 校验：结果等于当前稳定目录扫描值，limiter 与分类统计一致；
// 无效转换不推进 epoch。
TEST_F(TestFileManager, test_macro_calibration_refreshes_stale_snapshot)
{
  LOG_INFO("========== macro calibration stale snapshot begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  ObTenantDiskSpaceManager *disk = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, mgr);
  ASSERT_NE(nullptr, disk);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  task.is_stop_ = false;
  const uint64_t saved_run_cnt = task.run_cnt_;
  const bool saved_mandatory = task.is_mandatory_calibrate_;
  for (int mode = 0; mode < 2; ++mode) {
    // 构造已记账的 HOT 文件，并保留迁移前的初始快照。
    MacroBlockId macro_id;
    OK(prepare_calibration_macro(204001 + mode /*tablet_id*/, macro_id));
    ObFileStatInfo meta, tmp, stale_macro, stale_hot;
    OK(task.get_macro_cache_used_size(meta, tmp, stale_macro, stale_hot));
    const int64_t epoch = mgr->get_cache_type_change_epoch();
    OK(mgr->modify_macro_cache_type(
        macro_id, ObSSMacroCacheType::MACRO_BLOCK, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
    ASSERT_EQ(epoch, mgr->get_cache_type_change_epoch());
    OK(mgr->modify_macro_cache_type(
        macro_id, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObSSMacroCacheType::MACRO_BLOCK));
    ASSERT_EQ(epoch + 1, mgr->get_cache_type_change_epoch());
    ASSERT_EQ(0, mgr->get_cache_type_change_inflight());

    // 当前稳定扫描作为基准；
    // 旧实现会在此基础上再次给普通块加上迁移量。
    files->set_pause_gc();
    ObFileStatInfo initial;
    int64_t hot_initial = 0;
    ObCalibrateDiskSpaceResult scan;
    const int scan_ret = task.calc_macro_block_disk_space_with_retry_(initial, hot_initial, scan);
    int64_t dir_correction = 0;
    const int dir_ret = scan.get_dir_calibrate_size(initial.dir_num_, dir_correction);
    const int64_t expected_macro = ((0 == mode)
        ? scan.total_file_size_ + dir_correction
        : scan.total_file_size_ - scan.total_dir_size_
            + scan.total_dir_num_ * ObDirManager::DEFAULT_DIR_SIZE);
    const int64_t expected_hot = scan.hot_tablet_total_used_size_;
    task.run_cnt_ = ((0 == mode) ? 2 : 1);
    task.is_mandatory_calibrate_ = false;
    const int cal_ret = task.calibrate_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK,
        ObTimeUtility::current_time_s() + 1 /*start_calc_size_time_s*/,
        stale_macro, stale_hot.total_size_);
    task.run_cnt_ = saved_run_cnt;
    task.is_mandatory_calibrate_ = saved_mandatory;
    ObSSMacroCacheStat macro_stat, hot_stat;
    const int macro_ret = disk->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_stat);
    const int hot_ret = disk->get_macro_cache_stat(
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_stat);
    const int64_t used_delta = disk->get_used_size_delta_nolock_();
    files->set_allow_gc();
    const int delete_ret = ObSSObjectAccessUtil::delete_local_file(macro_id);
    LOG_INFO("verify stale snapshot calibration", K(mode), K(stale_macro), K(stale_hot),
        K(expected_macro), K(expected_hot), K(macro_stat), K(hot_stat), KR(scan_ret), KR(cal_ret));
    OK(scan_ret);
    OK(dir_ret);
    OK(cal_ret);
    OK(macro_ret);
    OK(hot_ret);
    ASSERT_EQ(expected_macro, macro_stat.used_);
    ASSERT_EQ(expected_hot, hot_stat.used_);
    ASSERT_EQ(0, used_delta);
    OK(delete_ret);
  }
  files->set_allow_gc();
  task.run_cnt_ = saved_run_cnt;
  task.is_mandatory_calibrate_ = saved_mandatory;
  LOG_INFO("========== macro calibration stale snapshot end ==========");
}

// 扫描前后发生双向迁移会重扫；持续迁移耗尽或扫描中取消时清空结果。
// 操作：四种方向/时机组合首轮转换、次轮放行；
// 再分别注入三轮转换和扫描后的停止请求。
// 校验：成功场景与独立稳定扫描一致；
// 耗尽/取消场景无残留结果，epoch/inflight 正常。
TEST_F(TestFileManager, test_macro_calibration_retry_on_cache_type_change)
{
  LOG_INFO("========== macro calibration concurrent conversion begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, mgr);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  task.is_stop_ = false;
  const uint64_t tenant_id = MTL_ID();
  const char *points[] = {"BEFORE_MACRO_BLOCK_CALIBRATE_SCAN", "AFTER_MACRO_BLOCK_CALIBRATE_SCAN"};
  for (int scenario = 0; scenario < 6; ++scenario) {
    const bool exhaust_retries = (4 == scenario);
    const bool cancel_scan = (5 == scenario);
    const int stage = ((exhaust_retries || cancel_scan) ? 1 : scenario / 2);
    const bool starts_hot = (0 == scenario % 2);
    const int attempt_count = ((cancel_scan) ? 1 : ((exhaust_retries) ? 3 : 2));
    MacroBlockId macro_id;
    OK(prepare_calibration_macro(204011 + scenario /*tablet_id*/, macro_id));
    const ObSSMacroCacheType original_type = ((starts_hot)
        ? ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK : ObSSMacroCacheType::MACRO_BLOCK);
    const ObSSMacroCacheType target_type = ((starts_hot)
        ? ObSSMacroCacheType::MACRO_BLOCK : ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
    if (!starts_hot) {
      OK(mgr->modify_macro_cache_type(macro_id,
          ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObSSMacroCacheType::MACRO_BLOCK));
    }
    common::ObMalloc allocator;
    common::ObDSSessionActions point_actions, control_actions;
    OK(point_actions.init(1024 /*page_size*/, allocator));
    OK(control_actions.init(1024 /*page_size*/, allocator));
    OK(GDS.add_debug_sync("reset", false /*is_global*/, control_actions));
    char action[256] = {0};
    OK(databuff_printf(action, sizeof(action),
        "%s signal macro_scan_ready wait_for macro_scan_continue timeout 10000000 execute %d",
        points[stage], attempt_count));
    OK(GDS.add_debug_sync(action, false /*is_global*/, point_actions));
    OK(GDS.set_thread_local_actions(point_actions));
    const int64_t saved_timeout = GCONF.debug_sync_timeout;
    GCONF.debug_sync_timeout = 10_s;
    files->set_pause_gc();
    const int64_t epoch_before = mgr->get_cache_type_change_epoch();
    std::atomic<int> worker_ret(OB_SUCCESS);
    std::thread converter([&]() {
      int ret = OB_SUCCESS;
      share::ObTenantSwitchGuard tenant_guard;
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        LOG_WARN("fail to switch calibration test tenant", KR(ret), K(tenant_id));
      }
      for (int attempt = 0; OB_SUCC(ret) && attempt < attempt_count; ++attempt) {
        ObTimePiece wait_piece;
        if (OB_FAIL(GDS.add_debug_sync("now wait_for macro_scan_ready timeout 10000000",
            false /*is_global*/, control_actions))) {
          LOG_WARN("fail to wait for macro calibration scan", KR(ret), K(stage), K(attempt));
        } else if (wait_piece.fly_us() >= 9_s) {
          // DEBUG_SYNC 等待超时也返回成功；
          // 必须排除等满超时后才执行转换的情况。
          ret = OB_TIMEOUT;
          LOG_WARN("macro calibration test did not receive scan signal in time",
              KR(ret), K(scenario), K(attempt), K(wait_piece));
        } else if (cancel_scan) {
          task.stop();
        } else if (0 == attempt || exhaust_retries) {
          const bool reverse = (exhaust_retries && 1 == attempt % 2);
          const ObSSMacroCacheType source = ((reverse) ? target_type : original_type);
          const ObSSMacroCacheType target = ((reverse) ? original_type : target_type);
          if (OB_FAIL(mgr->modify_macro_cache_type(macro_id, source, target))) {
            LOG_WARN("fail to convert calibration test macro", KR(ret), K(macro_id), K(attempt));
          }
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(GDS.add_debug_sync(
            "now signal macro_scan_continue", false /*is_global*/, control_actions))) {
          LOG_WARN("fail to resume macro calibration scan", KR(tmp_ret), KR(ret));
          ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);
        }
      }
      worker_ret.store(ret);
    });
    ObFileStatInfo initial;
    initial.total_size_ = 1;
    int64_t hot_initial = 1;
    ObCalibrateDiskSpaceResult scan;
    scan.total_file_size_ = scan.hot_tablet_total_used_size_ = 1;
    const int scan_ret = task.calc_macro_block_disk_space_with_retry_(initial, hot_initial, scan);
    converter.join();
    const common::ObDSActionArray *actions = GDS.thread_local_actions();
    const bool consumed = OB_NOT_NULL(actions) && !actions->is_active(
        ((0 == stage) ? BEFORE_MACRO_BLOCK_CALIBRATE_SCAN : AFTER_MACRO_BLOCK_CALIBRATE_SCAN));
    point_actions.reset();
    const int clear_ret = GDS.set_thread_local_actions(point_actions);
    const int reset_ret = GDS.add_debug_sync("reset", false /*is_global*/, control_actions);
    GCONF.debug_sync_timeout = saved_timeout;
    task.is_stop_ = false;
    ObFileStatInfo stable_initial;
    int64_t stable_hot_initial = 0;
    ObCalibrateDiskSpaceResult stable_scan;
    const int stable_ret = task.calc_macro_block_disk_space_with_retry_(
        stable_initial, stable_hot_initial, stable_scan);
    files->set_allow_gc();
    const int delete_ret = ObSSObjectAccessUtil::delete_local_file(macro_id);
    const int worker_result = worker_ret.load();
    LOG_INFO("verify conversion scan retry", KR(scan_ret), K(scenario), K(stage), K(consumed),
        KR(worker_result), KR(clear_ret), KR(reset_ret), KR(delete_ret), KR(stable_ret),
        K(initial), K(hot_initial), K(scan), K(stable_scan));
    if (exhaust_retries || cancel_scan) {
      ASSERT_EQ(((cancel_scan) ? OB_CANCELED : OB_EAGAIN), scan_ret);
      ASSERT_EQ(0, initial.total_size_);
      ASSERT_EQ(0, hot_initial);
      ASSERT_EQ(0, scan.total_file_size_);
      ASSERT_EQ(0, scan.hot_tablet_total_used_size_);
    } else {
      OK(scan_ret);
      ASSERT_EQ(stable_scan.total_file_size_, scan.total_file_size_);
      ASSERT_EQ(stable_scan.hot_tablet_total_used_size_, scan.hot_tablet_total_used_size_);
      ASSERT_EQ(stable_scan.total_dir_size_, scan.total_dir_size_);
      ASSERT_EQ(stable_scan.total_dir_num_, scan.total_dir_num_);
    }
    OK(worker_result);
    OK(clear_ret);
    OK(reset_ret);
    OK(stable_ret);
    OK(delete_ret);
    ASSERT_TRUE(consumed);
    const int64_t epoch_delta = ((cancel_scan) ? 0 : ((exhaust_retries) ? 3 : 1));
    ASSERT_EQ(epoch_before + epoch_delta, mgr->get_cache_type_change_epoch());
    ASSERT_EQ(0, mgr->get_cache_type_change_inflight());
    ASSERT_EQ(scan.hot_tablet_total_used_size_, hot_initial);
  }
  LOG_INFO("========== macro calibration concurrent conversion end ==========");
}

// 在途迁移使三次尝试均失败时，不提交任何分类用量，
// 也不返回上一轮残留结果。
// 操作：模拟持续在途转换，调用宏块校准；解除在途状态后重新扫描。
// 校验：先返回 OB_EAGAIN、用量不变；后续稳定扫描可成功。
TEST_F(TestFileManager, test_macro_calibration_retry_exhausted_by_inflight)
{
  LOG_INFO("========== macro calibration inflight retry begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  ObTenantDiskSpaceManager *disk = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, mgr);
  ASSERT_NE(nullptr, disk);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  task.is_stop_ = false;
  ObFileStatInfo meta, tmp, macro, hot;
  OK(task.get_macro_cache_used_size(meta, tmp, macro, hot));
  const int64_t epoch = mgr->get_cache_type_change_epoch();
  ATOMIC_INC(&mgr->cache_type_change_inflight_);
  // 预置旧结果，直接验证在途重试耗尽会清空全部出参。
  ObFileStatInfo rejected_initial;
  rejected_initial.total_size_ = 1;
  rejected_initial.dir_size_ = 1;
  rejected_initial.dir_num_ = 1;
  int64_t rejected_hot_initial = 1;
  ObCalibrateDiskSpaceResult rejected_scan;
  rejected_scan.total_file_size_ = 1;
  rejected_scan.hot_tablet_total_used_size_ = 1;
  const int rejected_ret = task.calc_macro_block_disk_space_with_retry_(
      rejected_initial, rejected_hot_initial, rejected_scan);
  const int cal_ret = task.calibrate_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK,
      ObTimeUtility::current_time_s() + 1 /*start_calc_size_time_s*/, macro, hot.total_size_);
  ATOMIC_DEC(&mgr->cache_type_change_inflight_);
  ObSSMacroCacheStat macro_after, hot_after;
  OK(disk->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_after));
  OK(disk->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_after));
  LOG_INFO("verify inflight calibration did not commit", KR(cal_ret), K(macro), K(hot),
      K(macro_after), K(hot_after));
  ASSERT_EQ(OB_EAGAIN, cal_ret);
  ASSERT_EQ(OB_EAGAIN, rejected_ret);
  ASSERT_EQ(0, rejected_initial.total_size_);
  ASSERT_EQ(0, rejected_initial.dir_size_);
  ASSERT_EQ(0, rejected_initial.dir_num_);
  ASSERT_EQ(0, rejected_hot_initial);
  ASSERT_EQ(0, rejected_scan.total_file_size_);
  ASSERT_EQ(0, rejected_scan.hot_tablet_total_used_size_);
  ASSERT_EQ(macro.total_size_, macro_after.used_);
  ASSERT_EQ(hot.total_size_, hot_after.used_);
  ASSERT_EQ(epoch, mgr->get_cache_type_change_epoch());
  ObFileStatInfo initial;
  int64_t hot_initial = 0;
  ObCalibrateDiskSpaceResult scan;
  OK(task.calc_macro_block_disk_space_with_retry_(initial, hot_initial, scan));
  ASSERT_EQ(0, mgr->get_cache_type_change_inflight());
  LOG_INFO("========== macro calibration inflight retry end ==========", K(initial), K(scan));
}

// 稳定扫描结束后的双向转换只由增量补偿一次，
// 普通/强制提交均保持分类及 limiter 一致。
// 场景：扫描一个真实宏块后再转换类型；
// 操作：直接使用已验证的快照和扫描值提交校准。
// 校验：四种方向/提交组合均与迁移后的稳定扫描一致，
// 校准不引入 used_size_delta。
TEST_F(TestFileManager, test_macro_calibration_conversion_after_stable_scan)
{
  LOG_INFO("========== macro calibration post-scan conversion begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  ObTenantDiskSpaceManager *disk = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, mgr);
  ASSERT_NE(nullptr, disk);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  task.is_stop_ = false;
  for (int scenario = 0; scenario < 4; ++scenario) {
    // 构造真实文件，并在扫描完成以后才执行转换，
    // 验证已有增量补偿的适用边界。
    MacroBlockId macro_id;
    OK(prepare_calibration_macro(204021 + scenario /*tablet_id*/, macro_id));
    const bool starts_hot = (0 == scenario % 2);
    const bool force = (scenario >= 2);
    if (!starts_hot) {
      OK(mgr->modify_macro_cache_type(macro_id,
          ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObSSMacroCacheType::MACRO_BLOCK));
    }
    files->set_pause_gc();
    const int64_t delta_before = disk->get_used_size_delta_nolock_();
    ObFileStatInfo initial;
    int64_t initial_hot = 0;
    ObCalibrateDiskSpaceResult scan;
    const int scan_ret = task.calc_macro_block_disk_space_with_retry_(initial, initial_hot, scan);
    const ObSSMacroCacheType source = ((starts_hot)
        ? ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK : ObSSMacroCacheType::MACRO_BLOCK);
    const ObSSMacroCacheType target = ((starts_hot)
        ? ObSSMacroCacheType::MACRO_BLOCK : ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
    const int64_t epoch_before = mgr->get_cache_type_change_epoch();
    const int convert_ret = mgr->modify_macro_cache_type(macro_id, source, target);
    int cal_ret = scan_ret;
    if (OB_SUCCESS == scan_ret && OB_SUCCESS == convert_ret) {
      cal_ret = ((force)
          ? task.calibrate_macro_cache_used_size_forcely(
              ObSSMacroCacheType::MACRO_BLOCK, initial, initial_hot, scan)
          : task.calibrate_macro_cache_used_size_normally(
              ObSSMacroCacheType::MACRO_BLOCK, initial, initial_hot, scan));
    }

    // 提交后重新扫描作独立基准；先恢复 GC、清理文件，再统一断言。
    ObFileStatInfo stable_initial;
    int64_t stable_hot = 0;
    ObCalibrateDiskSpaceResult stable_scan;
    const int stable_ret = task.calc_macro_block_disk_space_with_retry_(
        stable_initial, stable_hot, stable_scan);
    int64_t dir_correction = 0;
    const int dir_ret = stable_scan.get_dir_calibrate_size(initial.dir_num_, dir_correction);
    const int64_t expected_macro = ((force)
        ? stable_scan.total_file_size_ - stable_scan.total_dir_size_
            + stable_scan.total_dir_num_ * ObDirManager::DEFAULT_DIR_SIZE
        : stable_scan.total_file_size_ + dir_correction);
    ObSSMacroCacheStat macro_stat, hot_stat;
    const int macro_ret = disk->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_stat);
    const int hot_ret = disk->get_macro_cache_stat(
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_stat);
    const int64_t delta_after = disk->get_used_size_delta_nolock_();
    files->set_allow_gc();
    const int delete_ret = ObSSObjectAccessUtil::delete_local_file(macro_id);
    LOG_INFO("verify post-scan conversion calibration", K(scenario), K(force),
        KR(scan_ret), KR(convert_ret), KR(cal_ret), KR(stable_ret), KR(dir_ret),
        K(expected_macro), K(macro_stat), K(hot_stat), K(stable_scan),
        K(delta_before), K(delta_after));
    OK(scan_ret);
    OK(convert_ret);
    OK(cal_ret);
    OK(stable_ret);
    OK(dir_ret);
    OK(macro_ret);
    OK(hot_ret);
    OK(delete_ret);
    ASSERT_EQ(expected_macro, macro_stat.used_);
    ASSERT_EQ(stable_scan.hot_tablet_total_used_size_, hot_stat.used_);
    ASSERT_EQ(delta_before, delta_after);
    ASSERT_EQ(epoch_before + 1, mgr->get_cache_type_change_epoch());
  }
  LOG_INFO("========== macro calibration post-scan conversion end ==========");
}

// 时间准备跨秒时可重试；耗尽后不提交分类记账。
// 场景：在初值读取后等待跨秒，消耗时间准备尝试。
// 操作：先仅延迟前三次检查，再持续延迟所有检查。
// 校验：恢复场景成功；耗尽返回 EAGAIN，记账不变。
// 时间选择可能提前失败，因此不要求同步点固定命中次数。
TEST_F(TestFileManager, test_macro_calibration_time_snapshot_retry)
{
  LOG_INFO("========== macro calibration time retry begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObTenantDiskSpaceManager *disk = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, disk);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  task.is_stop_ = false;
  const uint64_t saved_run_cnt = task.run_cnt_;
  const bool saved_mandatory = task.is_mandatory_calibrate_;
  for (int mode = 0; mode < 2; ++mode) {
    MacroBlockId macro_id;
    OK(prepare_calibration_macro(204031 + mode /*tablet_id*/, macro_id));
    ObFileStatInfo meta, tmp, macro_before, hot_before;
    OK(task.get_macro_cache_used_size(meta, tmp, macro_before, hot_before));
    // 时间选择只在下一秒前 3ms 内成功，等待 10ms 可跨秒。
    constexpr int64_t CROSS_SECOND_DELAY_US = 10_ms;
    const int64_t delay_count = ((0 == mode) ? 3 : INT64_MAX);
    common::ObMalloc allocator;
    common::ObDSSessionActions point_actions, control_actions;
    OK(point_actions.init(1024 /*page_size*/, allocator));
    OK(control_actions.init(1024 /*page_size*/, allocator));
    OK(GDS.add_debug_sync("reset", false /*is_global*/, control_actions));
    char action[256] = {0};
    OK(databuff_printf(action, sizeof(action),
        "BEFORE_MACRO_BLOCK_CALIBRATE_TIME_CHECK "
        "wait_for macro_time_cross_boundary timeout %ld execute %ld",
        CROSS_SECOND_DELAY_US, delay_count));
    OK(GDS.add_debug_sync(action, false /*is_global*/, point_actions));
    OK(GDS.set_thread_local_actions(point_actions));
    const int64_t saved_timeout = GCONF.debug_sync_timeout;
    GCONF.debug_sync_timeout = 10_s;
    const bool debug_sync_enabled = GCONF.is_debug_sync_enabled();
    files->set_pause_gc();
    const int64_t delta_before = disk->get_used_size_delta_nolock_();

    // 不发送该事件，让到达同步点的检查自身等待跨秒。
    // 未到达同步点的合法失败不需要控制线程额外处理。
    // 调用完整校准，验证准备耗尽后的外层重试及失败时不提交。
    task.run_cnt_ = 2;
    task.is_mandatory_calibrate_ = false;
    const int cal_ret = task.calibrate_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK,
        ObTimeUtility::current_time_s() + 1 /*start_calc_size_time_s*/,
        macro_before, hot_before.total_size_);
    task.run_cnt_ = saved_run_cnt;
    task.is_mandatory_calibrate_ = saved_mandatory;
    point_actions.reset();
    const int clear_ret = GDS.set_thread_local_actions(point_actions);
    const int reset_ret = GDS.add_debug_sync("reset", false /*is_global*/, control_actions);
    GCONF.debug_sync_timeout = saved_timeout;
    ObSSMacroCacheStat macro_after, hot_after;
    const int macro_ret = disk->get_macro_cache_stat(
        ObSSMacroCacheType::MACRO_BLOCK, macro_after);
    const int hot_ret = disk->get_macro_cache_stat(
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_after);
    const int64_t delta_after = disk->get_used_size_delta_nolock_();

    // 清除注入后恢复扫描，并对照恢复场景的提交结果。
    ObFileStatInfo stable_initial;
    int64_t stable_hot = 0;
    ObCalibrateDiskSpaceResult stable_scan;
    const int stable_ret = task.calc_macro_block_disk_space_with_retry_(
        stable_initial, stable_hot, stable_scan);
    int64_t dir_correction = 0;
    const int dir_ret = stable_scan.get_dir_calibrate_size(macro_before.dir_num_, dir_correction);
    files->set_allow_gc();
    const int delete_ret = ObSSObjectAccessUtil::delete_local_file(macro_id);
    LOG_INFO("verify calibration time retries", K(mode), KR(cal_ret), K(debug_sync_enabled),
        KR(stable_ret), K(macro_before), K(hot_before),
        K(macro_after), K(hot_after), K(stable_scan), K(delta_before), K(delta_after));
    OK(clear_ret);
    OK(reset_ret);
    OK(macro_ret);
    OK(hot_ret);
    OK(stable_ret);
    OK(dir_ret);
    OK(delete_ret);
    ASSERT_TRUE(debug_sync_enabled);
    ASSERT_EQ(delta_before, delta_after);
    if (0 == mode) {
      OK(cal_ret);
      ASSERT_EQ(stable_scan.total_file_size_ + dir_correction, macro_after.used_);
      ASSERT_EQ(stable_scan.hot_tablet_total_used_size_, hot_after.used_);
    } else {
      ASSERT_EQ(OB_EAGAIN, cal_ret);
      ASSERT_EQ(macro_before.total_size_, macro_after.used_);
      ASSERT_EQ(hot_before.total_size_, hot_after.used_);
    }
  }
  task.run_cnt_ = saved_run_cnt;
  task.is_mandatory_calibrate_ = saved_mandatory;
  LOG_INFO("========== macro calibration time retry end ==========");
}

// 任务停止或未初始化时，时间准备和扫描均返回明确错误，
// 并清空调用方预置的旧结果。
// 操作：分别设置停止/未初始化状态，
// 携带非零出参调用 helper 和扫描入口。
// 校验：返回 CANCELED/NOT_INIT，所有初值和扫描大小归零，
// 不进入目录扫描或提交。
TEST_F(TestFileManager, test_macro_calibration_canceled_outputs)
{
  LOG_INFO("========== macro calibration canceled outputs begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, files);
  ObCalibrateDiskSpaceTask &task = files->calibrate_disk_space_task_;
  task.stop();
  task.wait();
  const bool saved_initialized = task.is_inited_;
  for (int mode = 0; mode < 2; ++mode) {
    // 停止状态覆盖正常初始化的取消分支；未初始化分支优先于停止检查。
    task.is_inited_ = (0 == mode && saved_initialized);
    int64_t start_time_s = 1;
    ObFileStatInfo macro, hot;
    macro.total_size_ = macro.dir_size_ = macro.dir_num_ = 1;
    hot.total_size_ = hot.dir_size_ = hot.dir_num_ = 1;
    const int prepare_ret = task.prepare_macro_calibration_snapshot_(start_time_s, macro, hot);
    ObFileStatInfo initial;
    initial.total_size_ = initial.dir_size_ = initial.dir_num_ = 1;
    int64_t hot_initial = 1;
    ObCalibrateDiskSpaceResult scan;
    scan.total_file_size_ = scan.hot_tablet_total_used_size_ =
        scan.total_dir_size_ = scan.total_dir_num_ = 1;
    scan.del_tmp_seq_file_size_ = scan.tmp_seq_file_size_ = scan.tmp_seq_file_cnt_ = 1;
    const int scan_ret = task.calc_macro_block_disk_space_with_retry_(initial, hot_initial, scan);
    task.is_inited_ = saved_initialized;
    LOG_INFO("verify canceled calibration outputs", K(mode), KR(prepare_ret), KR(scan_ret),
        K(start_time_s), K(macro), K(hot), K(initial), K(hot_initial), K(scan));
    const int expected_ret = ((0 == mode) ? OB_CANCELED : OB_NOT_INIT);
    ASSERT_EQ(expected_ret, prepare_ret);
    ASSERT_EQ(expected_ret, scan_ret);
    ASSERT_EQ(0, start_time_s);
    ASSERT_EQ(0, macro.total_size_ + macro.dir_size_ + macro.dir_num_);
    ASSERT_EQ(0, hot.total_size_ + hot.dir_size_ + hot.dir_num_);
    ASSERT_EQ(0, initial.total_size_ + initial.dir_size_ + initial.dir_num_);
    ASSERT_EQ(0, hot_initial);
    ASSERT_EQ(0, scan.total_file_size_ + scan.hot_tablet_total_used_size_
        + scan.total_dir_size_ + scan.total_dir_num_ + scan.del_tmp_seq_file_size_
        + scan.tmp_seq_file_size_ + scan.tmp_seq_file_cnt_);
  }
  task.is_stop_ = false;
  LOG_INFO("========== macro calibration canceled outputs end ==========");
}

// 转换回调在进入计数后失败，也必须推进 epoch 并归还 inflight，
// 不能永久阻塞后续校准。
// 场景：真实 meta 由已初始化的 manager 持有，
// 回调使用一个未初始化的独立 manager。
// 操作：通过原 meta map 的原子接口执行回调，使 shift_lru_list 返回 NOT_INIT。
// 校验：失败 manager 的 epoch 加一、inflight 为零；
// 原 meta 类型和原 manager 计数不变。
TEST_F(TestFileManager, test_macro_calibration_conversion_failure_balances_inflight)
{
  LOG_INFO("========== macro calibration conversion failure begin ==========");
  ObTenantFileManager *files = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, files);
  ASSERT_NE(nullptr, mgr);
  MacroBlockId macro_id;
  OK(prepare_calibration_macro(204041 /*tablet_id*/, macro_id));
  ObSSMacroCacheMgr uninitialized_mgr;
  const ObSSMacroCacheModifyCacheTypeParam param(
      false /*touch_cache*/, false /*update_effective_tablet_id*/, ObTabletID::INVALID_TABLET_ID);
  ObSSMacroCacheModifyCacheTypeCallback callback(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
      ObSSMacroCacheType::MACRO_BLOCK, param, uninitialized_mgr);
  files->set_pause_gc();
  const int64_t epoch = mgr->get_cache_type_change_epoch();
  const int map_ret = mgr->meta_map_.atomic_refactored(macro_id, callback);
  const int callback_ret = callback.get_ret();
  ObSSMacroCacheType actual_type = ObSSMacroCacheType::MAX_TYPE;
  const int type_ret = mgr->get_macro_cache_type(macro_id, actual_type);
  files->set_allow_gc();
  const int delete_ret = ObSSObjectAccessUtil::delete_local_file(macro_id);
  LOG_INFO("verify failed conversion counter balance", KR(map_ret), KR(callback_ret),
      KR(type_ret), K(actual_type), K(epoch));
  OK(map_ret);
  ASSERT_EQ(OB_NOT_INIT, callback_ret);
  OK(type_ret);
  OK(delete_ret);
  ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, actual_type);
  ASSERT_EQ(1, uninitialized_mgr.get_cache_type_change_epoch());
  ASSERT_EQ(0, uninitialized_mgr.get_cache_type_change_inflight());
  ASSERT_EQ(epoch, mgr->get_cache_type_change_epoch());
  LOG_INFO("========== macro calibration conversion failure end ==========");
}

// When free_size > macro_cache_stats_[idx].used_, free_file_size() sets used_ to 0.
// Only when is_first_calibration_done is true: accumulate free_size_exceeded_sum_[idx] and FLOG_ERROR;
// when false (before first calibration): no error stat, but log WARN.
TEST_F(TestFileManager, test_free_file_size_exceeded_used_by_is_first_calibration_done)
{
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);

  const ObSSMacroCacheType cache_type = ObSSMacroCacheType::TMP_FILE;
  const uint8_t idx = static_cast<uint8_t>(cache_type);
  const int64_t alloc_sz = 4096; // 4KB

  disk_space_mgr->set_first_calibration_done(false);

  ObSSMacroCacheStat stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
  // Reset TMP_FILE used_ to 0 so this test is independent of execution order (previous tests
  // may leave TMP_FILE used_ non-zero). Free (used_ + 1) to force the "free > used" branch
  // and clamp used_ to 0.
  if (stat.used_ > 0) {
    ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(stat.used_, cache_type, ObDiskSpaceType::FILE));
    ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
    ASSERT_EQ(0, stat.used_);
  }
  const int64_t used_before = stat.used_;

  // 1. alloc so that used_ = used_before + alloc_sz
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(alloc_sz, cache_type, ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
  const int64_t used_after_alloc = stat.used_;
  ASSERT_EQ(used_before + alloc_sz, used_after_alloc);
  const int64_t free_sz = 8192; // 8KB

  // 2. is_first_calibration_done == false: free more than used_; used_ -> 0, no error stat
  int64_t exceeded_before = disk_space_mgr->free_size_exceeded_sum_[idx];
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(free_sz, cache_type, ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
  ASSERT_EQ(0, stat.used_);
  ASSERT_EQ(exceeded_before, disk_space_mgr->free_size_exceeded_sum_[idx]);

  // 3. alloc again, then is_first_calibration_done == true: free more than used_; used_ -> 0, error stat increased
  disk_space_mgr->set_first_calibration_done(true);
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(alloc_sz, cache_type, ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
  const int64_t used_before_second_free = stat.used_;
  const int64_t free_sz2 = 16384; // 16KB
  exceeded_before = disk_space_mgr->free_size_exceeded_sum_[idx];
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(free_sz2, cache_type, ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, stat));
  ASSERT_EQ(0, stat.used_);
  ASSERT_EQ(exceeded_before + (free_sz2 - used_before_second_free), disk_space_mgr->free_size_exceeded_sum_[idx]);
}

TEST_F(TestFileManager, test_delete_tmp_seq_files_cache_type_record)
{
  // 验证 delete_tmp_seq_file 使用 map 中记录的 cache_type，而非从 meta_map 反查。
  // 核心场景：meta map 中 cache_type 变化后，free 仍用 alloc 时的 cache_type。
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  tenant_file_mgr->tmp_seq_file_clean_task_.destroy();
  tenant_file_mgr->calibrate_disk_space_task_.destroy();

  const int64_t CLEAN_MAP_SAFE_DEL_TIME_S = 5L;
  const int64_t write_io_size = 2 * 1024 * 1024; // 2MB

  // ====== 场景1：alloc(HOT)，meta map 变为 MACRO → free 仍用 HOT ======
  const uint64_t hot_to_cold_tablet_id = 101;
  ASSERT_EQ(OB_SUCCESS, policy_service->update_tablet_status(
      hot_to_cold_tablet_id, PolicyStatus::HOT, 200001/*table_id*/));
  ASSERT_EQ(OB_SUCCESS, write_fail_and_clean_tmp_seq_file(
      hot_to_cold_tablet_id,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
      "hot_alloc_become_cold_free",
      CLEAN_MAP_SAFE_DEL_TIME_S,
      write_io_size));

  // ====== 场景2：alloc(MACRO)，meta map 变为 HOT → free 仍用 MACRO ======
  const uint64_t auto_to_hot_tablet_id = 102;
  ASSERT_EQ(OB_SUCCESS, policy_service->update_tablet_status(
      auto_to_hot_tablet_id, PolicyStatus::AUTO, 200002/*table_id*/));
  ASSERT_EQ(OB_SUCCESS, write_fail_and_clean_tmp_seq_file(
      auto_to_hot_tablet_id,
      ObSSMacroCacheType::MACRO_BLOCK,
      "auto_alloc_become_hot_free",
      CLEAN_MAP_SAFE_DEL_TIME_S,
      write_io_size));
}

TEST_F(TestFileManager, test_delete_tenant)
{
  // delete local tenant dir file
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_local_tenant_dir(MTL_ID(), MTL_EPOCH_ID()));
  // delete remote tenant dir file
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_remote_tenant_dir(MTL_ID(), MTL_EPOCH_ID()));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_file_manager.log*");
  OB_LOGGER.set_file_name("test_file_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

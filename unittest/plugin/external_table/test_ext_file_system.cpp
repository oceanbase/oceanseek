/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <fcntl.h>
#include <string>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>
#define private public
#include "plugin/v2/host/ob_ext_file_system.h"
#undef private
#include "plugin/v2/host/ob_ext_file_system_common.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/restore/ob_object_storage_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_device_manager.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql::ext_plugin;

namespace share
{
// 为独立单测提供租户查找，实际 IO 管理器仍使用生产实现。
static ObTenantBase *storage_test_tenants[2] = {nullptr, nullptr};
int get_tenant_base_with_lock(uint64_t tenant_id, ObLDHandle &handle,
    ObTenantBase *&tenant, ReleaseCbFunc &release_cb)
{
  UNUSED(handle);
  int ret = OB_TENANT_NOT_EXIST;
  tenant = nullptr;
  release_cb = nullptr;
  for (ObTenantBase *candidate : storage_test_tenants) {
    if (OB_NOT_NULL(candidate) && tenant_id == candidate->id()) {
      tenant = candidate;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
}

class TestExtFileSystem : public ::testing::Test
{
public:
  TestExtFileSystem()
      : owner_tenant_(OB_SYS_TENANT_ID), caller_tenant_(1002), saved_tenant_(nullptr)
  {}

  virtual void SetUp() override
  {
    saved_tenant_ = ObTenantEnv::get_tenant();
    storage_test_tenants[0] = &owner_tenant_;
    storage_test_tenants[1] = &caller_tenant_;
    ObTenantEnv::set_tenant(&owner_tenant_);
    const uint64_t tenant_ids[] = {owner_tenant_.id(), caller_tenant_.id()};
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_ids); ++i) {
      const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_ids[i]);
      ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    }
  }

  virtual void TearDown() override
  {
    ObTenantEnv::set_tenant(saved_tenant_);
    storage_test_tenants[0] = nullptr;
    storage_test_tenants[1] = nullptr;
  }

protected:
  ObTenantBase owner_tenant_;
  ObTenantBase caller_tenant_;
  ObTenantBase *saved_tenant_;
};

TEST_F(TestExtFileSystem, ordinary_storage_guard_keeps_current_tenant_selection)
{
  const uint64_t saved_storage_tenant = ObObjectStorageTenantGuard::get_tenant_id();
  {
    ObObjectStorageTenantGuard ordinary_guard(caller_tenant_.id(), 123000);
    EXPECT_EQ(owner_tenant_.id(), common::get_storage_io_tenant_id());
    EXPECT_EQ(OB_INVALID_TENANT_ID, ObStorageAccessTenantGuard::get_target_tenant_id());
    // 对应备份检查的 MTL=0、普通 guard=系统租户场景，仍使用租户 500 的 IO 管理器。
    ObTenantEnv::set_tenant(nullptr);
    DEFER(ObTenantEnv::set_tenant(&owner_tenant_));
    ObObjectStorageTenantGuard sys_guard(OB_SYS_TENANT_ID, 123000);
    EXPECT_EQ(OB_SERVER_TENANT_ID, common::get_storage_io_tenant_id());
  }
  EXPECT_EQ(saved_storage_tenant, ObObjectStorageTenantGuard::get_tenant_id());
}

TEST_F(TestExtFileSystem, explicit_storage_tenant_restores_nested_scopes)
{
  ObObjectStorageTenantGuard ordinary_guard(caller_tenant_.id(), 123000);
  {
    ExtStorageTenantGuard owner_guard(owner_tenant_.id());
    const int64_t owner_timeout = ObObjectStorageTenantGuard::get_timeout_us();
    ObTenantSwitchGuard caller_guard(&caller_tenant_);
    EXPECT_EQ(owner_tenant_.id(), common::get_storage_io_tenant_id());
    {
      ExtStorageTenantGuard inner_guard(caller_tenant_.id());
      EXPECT_EQ(caller_tenant_.id(), common::get_storage_io_tenant_id());
    }
    EXPECT_EQ(owner_tenant_.id(), common::get_storage_io_tenant_id());
    {
      // 带资源组的普通 guard 也不能清除外层显式指定。
      ObObjectStorageTenantGuard io_guard(caller_tenant_.id(), 456000,
          USER_RESOURCE_OTHER_GROUP_ID);
      EXPECT_EQ(owner_tenant_.id(), common::get_storage_io_tenant_id());
    }
    EXPECT_EQ(owner_tenant_.id(), ObObjectStorageTenantGuard::get_tenant_id());
    EXPECT_EQ(owner_timeout, ObObjectStorageTenantGuard::get_timeout_us());
    {
      ExtStorageTenantGuard server_guard(OB_SERVER_TENANT_ID);
      EXPECT_EQ(OB_SERVER_TENANT_ID, common::get_storage_io_tenant_id());
    }
    EXPECT_EQ(owner_tenant_.id(), common::get_storage_io_tenant_id());
    std::thread other_thread([]() {
      EXPECT_EQ(OB_INVALID_TENANT_ID, ObStorageAccessTenantGuard::get_target_tenant_id());
    });
    other_thread.join();
    EXPECT_EQ(&caller_tenant_, ObTenantEnv::get_tenant());
  }
  EXPECT_EQ(OB_INVALID_TENANT_ID, ObStorageAccessTenantGuard::get_target_tenant_id());
  EXPECT_EQ(caller_tenant_.id(), ObObjectStorageTenantGuard::get_tenant_id());
  EXPECT_EQ(123000, ObObjectStorageTenantGuard::get_timeout_us());
}

TEST_F(TestExtFileSystem, unsupported_scheme_fails_before_io)
{
  SCOPED_TRACE("不支持的协议在文件系统入口返回错误，不发起远端 IO。");
  ObExtDefaultFileSystem fs;
  fs.set_tenant_id(owner_tenant_.id());
  ObString resolved;
  EXPECT_EQ(OB_SUCCESS, fs.resolve_path("my-store://Bucket/Key", resolved));
  EXPECT_EQ(&owner_tenant_, ObTenantEnv::get_tenant());
  int64_t size = -1;
  int64_t mtime_ms = -1;
  int32_t is_dir = -1;
  EXPECT_EQ(OB_INVALID_BACKUP_DEST, fs.file_status("my-store://Bucket/Key", size, mtime_ms, is_dir));
  EXPECT_EQ(&owner_tenant_, ObTenantEnv::get_tenant());
  EXPECT_EQ(-1, fs.exists("my-store://Bucket/Key"));
  EXPECT_EQ(OB_INVALID_BACKUP_DEST, fs.list_dir("my-store://Bucket/Key", [](void *, const char *, int32_t) {}, nullptr));
  EXPECT_EQ(nullptr, fs.open("my-store://Bucket/Key"));
  EXPECT_EQ(&owner_tenant_, ObTenantEnv::get_tenant());
}

TEST_F(TestExtFileSystem, metadata_and_listing_preserve_entry_types)
{
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  DEFER(ObIOManager::get_instance().destroy());
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  DEFER(ObDeviceManager::get_instance().destroy());
  // 初始化文件系统查询使用的租户 IO 上下文。
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOManager *io_service = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  DEFER({
    ObTenantIOManager::mtl_destroy(io_service);
    owner_tenant_.set(io_service);
    ObTenantEnv::set_tenant(&owner_tenant_);
  });
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  owner_tenant_.set(io_service);
  ObTenantEnv::set_tenant(&owner_tenant_);
  ASSERT_EQ(OB_SUCCESS, io_service->start());
  ObTenantIOConfig::ParamConfig io_config = io_service->get_io_config().param_config_;
  io_config.object_storage_io_timeout_ms_ = 37000;
  ASSERT_EQ(OB_SUCCESS, io_service->update_basic_io_param_config(io_config));
  char directory[] = "/tmp/ob-ext-fs-list-XXXXXX";
  ASSERT_NE(nullptr, mkdtemp(directory));
  DEFER(rmdir(directory));
  const std::string file_path = std::string(directory) + "/empty-file";
  const int fd = ::open(file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
  ASSERT_GE(fd, 0);
  DEFER(unlink(file_path.c_str()));
  ASSERT_EQ(0, ::close(fd));
  const std::string child_path = std::string(directory) + "/child";
  ASSERT_EQ(0, mkdir(child_path.c_str(), 0700));
  DEFER(rmdir(child_path.c_str()));

  ObExtDefaultFileSystem fs;
  fs.set_tenant_id(owner_tenant_.id());
  const std::string uri = std::string("file://") + directory;
  ASSERT_EQ(OB_SUCCESS, fs.set_path(ObString(uri.c_str())));
  ASSERT_EQ(OB_SUCCESS, fs.set_access_info(ObString()));
  using Entries = std::vector<std::pair<std::string, int32_t>>;
  Entries entries;
  const auto collect = [](void *ctx, const char *name, int32_t is_dir) {
    EXPECT_EQ(OB_SYS_TENANT_ID, ObObjectStorageTenantGuard::get_tenant_id());
    EXPECT_EQ(37000000, ObObjectStorageTenantGuard::get_timeout_us());
    EXPECT_EQ(1002, MTL_ID());
    static_cast<Entries *>(ctx)->emplace_back(name, is_dir);
  };
  {
    // caller 没有 IO 管理器；查询成功表明请求使用了 owner 的 IO 管理器。
    ObTenantSwitchGuard caller_guard(&caller_tenant_);
    ObObjectStorageTenantGuard storage_guard(caller_tenant_.id(), 1000000);
    ASSERT_EQ(OB_SUCCESS, fs.list_dir("", collect, &entries));
    EXPECT_EQ(caller_tenant_.id(), ObObjectStorageTenantGuard::get_tenant_id());
    EXPECT_EQ(1000000, ObObjectStorageTenantGuard::get_timeout_us());
    EXPECT_EQ(&caller_tenant_, ObTenantEnv::get_tenant());
    const Entries expected = {{"empty-file", 0}, {"child", 1}};
    EXPECT_EQ(expected, entries);
    entries.clear();
    ASSERT_EQ(OB_SUCCESS, fs.list_dir("child", collect, &entries));
    EXPECT_TRUE(entries.empty());
    ASSERT_EQ(OB_SUCCESS, fs.list_dir("missing", collect, &entries));
    EXPECT_TRUE(entries.empty());
    EXPECT_EQ(1, fs.exists("empty-file"));
    EXPECT_EQ(1, fs.exists("child"));
    EXPECT_EQ(0, fs.exists("missing"));
    int64_t size = -1;
    int64_t mtime_ms = -1;
    int32_t is_dir = -1;
    ASSERT_EQ(OB_SUCCESS, fs.file_status("empty-file", size, mtime_ms, is_dir));
    EXPECT_EQ(0, size);
    EXPECT_EQ(0, is_dir);
    ASSERT_EQ(OB_SUCCESS, fs.file_status("child", size, mtime_ms, is_dir));
    EXPECT_EQ(1, is_dir);
    EXPECT_EQ(caller_tenant_.id(), ObObjectStorageTenantGuard::get_tenant_id());
    EXPECT_EQ(1000000, ObObjectStorageTenantGuard::get_timeout_us());
    EXPECT_EQ(OB_INVALID_TENANT_ID, ObStorageAccessTenantGuard::get_target_tenant_id());
    EXPECT_EQ(&caller_tenant_, ObTenantEnv::get_tenant());
  }

  EXPECT_EQ(1, fs.exists("empty-file"));
  EXPECT_EQ(1, fs.exists("child"));
  EXPECT_EQ(0, fs.exists("missing"));

  int64_t size = -1;
  int64_t mtime_ms = -1;
  int32_t is_dir = -1;
  ASSERT_EQ(OB_SUCCESS, fs.file_status("empty-file", size, mtime_ms, is_dir));
  EXPECT_EQ(0, size);
  EXPECT_EQ(0, is_dir);
  EXPECT_GE(mtime_ms, 0);
  ASSERT_EQ(OB_SUCCESS, fs.file_status("child", size, mtime_ms, is_dir));
  EXPECT_EQ(1, is_dir);
  EXPECT_NE(OB_SUCCESS, fs.file_status("missing", size, mtime_ms, is_dir));
  EXPECT_EQ(-1, size);
  EXPECT_EQ(0, is_dir);

  // 长度和修改时间复用完整 stat 的结果。
  const std::string file_uri = std::string("file://") + file_path;
  const std::string extended_uri = file_uri + ".outside-view";
  ObExternalTableStorageInfo storage_info;
  ASSERT_EQ(OB_SUCCESS, storage_info.set(OB_STORAGE_FILE, ""));
  ObIODFileStat common_statbuf;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::get_file_stat(
      ObString(file_uri.c_str()), &storage_info, common_statbuf));
  int64_t file_length = -1;
  int64_t modify_time_s = -1;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::get_file_length(
      ObString(file_uri.c_str()), &storage_info, file_length));
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::get_file_modify_time(
      ObString(file_uri.c_str()), &storage_info, modify_time_s));
  EXPECT_EQ(common_statbuf.size_, file_length);
  EXPECT_EQ(common_statbuf.mtime_s_, modify_time_s);
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ObBackupIoAdapter::get_file_length(
      ObString(extended_uri.c_str()), &storage_info, file_length));
  EXPECT_EQ(-1, file_length);
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ObBackupIoAdapter::get_file_modify_time(
      ObString(extended_uri.c_str()), &storage_info, modify_time_s));
  EXPECT_EQ(-1, modify_time_s);

  // 在当前租户上下文中检查 URI 长度与设备引用释放。
  ObObjectStorageTenantGuard metadata_storage_guard(owner_tenant_.id(), 1000000);
  // 凭据已由调用方初始化，Adapter 只复制 URI，不能读取 ObString 长度之外的路径。
  const ObString bounded_uri(static_cast<int32_t>(file_uri.length()), extended_uri.data());
  ObIODevice *device = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().get_device(
      bounded_uri, storage_info, ObStorageIdMod::get_default_id_mod(), device));
  ASSERT_NE(nullptr, device);
  DEFER(EXPECT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().release_device(device)));
  const int64_t ref_count = device->get_ref_cnt();
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, ObExternalIoAdapter::get_file_stat(
      bounded_uri, &storage_info, statbuf));
  EXPECT_EQ(common_statbuf.size_, statbuf.size_);
  EXPECT_EQ(common_statbuf.mtime_s_, statbuf.mtime_s_);
  EXPECT_EQ(ref_count, device->get_ref_cnt());
  // 查询失败也应归还设备引用；之后仍能复用该设备查询目录类型。
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ObExternalIoAdapter::get_file_stat(
      ObString(extended_uri.c_str()), &storage_info, statbuf));
  EXPECT_EQ(ref_count, device->get_ref_cnt());
  bool is_directory = true;
  ASSERT_EQ(OB_SUCCESS, ObExternalIoAdapter::is_directory_by_meta(
      bounded_uri, &storage_info, is_directory));
  EXPECT_FALSE(is_directory);
  EXPECT_EQ(ref_count, device->get_ref_cnt());
  const std::string child_uri = std::string("file://") + child_path;
  ASSERT_EQ(OB_SUCCESS, ObExternalIoAdapter::is_directory_by_meta(
      ObString(child_uri.c_str()), &storage_info, is_directory));
  EXPECT_TRUE(is_directory);
  EXPECT_EQ(ref_count, device->get_ref_cnt());
  // 旧接口保留仅支持 HDFS 的行为，插件通过新接口查询实际目录类型。
  EXPECT_EQ(OB_NOT_SUPPORTED, ObBackupIoAdapter::is_directory(
      bounded_uri, &storage_info, is_directory));
  EXPECT_FALSE(is_directory);
  EXPECT_EQ(OB_NOT_SUPPORTED, ObBackupIoAdapter::is_directory(
      ObString(child_uri.c_str()), &storage_info, is_directory));
  EXPECT_FALSE(is_directory);
  EXPECT_EQ(ref_count, device->get_ref_cnt());
  EXPECT_EQ(owner_tenant_.id(), ObObjectStorageTenantGuard::get_tenant_id());
  EXPECT_EQ(&owner_tenant_, ObTenantEnv::get_tenant());
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

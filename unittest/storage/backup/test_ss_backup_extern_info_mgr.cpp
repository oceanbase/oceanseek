/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <fstream>
#include <iterator>
#include <string>
#include <gtest/gtest.h>
#include "test_backup.h"
#include "share/ob_device_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "rootserver/backup/ob_backup_data_set_task_mgr.h"
#include "rootserver/backup/ob_backup_service.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#endif
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::storage;

namespace oceanbase {
namespace backup {

#ifdef OB_BUILD_SHARED_STORAGE

class TestSSBackupExternInfoMgr : public ::testing::Test {
public:
  TestSSBackupExternInfoMgr();
  virtual ~TestSSBackupExternInfoMgr();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

private:
  void inner_init_();
  void clean_env_();
  void make_ss_ls_meta_info_(ObBackupSSLSMetaInfo &info, ObIAllocator &allocator);

protected:
  share::ObBackupDest backup_set_dest_;
  share::ObLSID ls_id_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestSSBackupExternInfoMgr);
};

TestSSBackupExternInfoMgr::TestSSBackupExternInfoMgr()
    : backup_set_dest_(), ls_id_(), test_dir_(""), test_dir_uri_("")
{}

TestSSBackupExternInfoMgr::~TestSSBackupExternInfoMgr()
{}

void TestSSBackupExternInfoMgr::SetUpTestCase()
{
  ObTenantBase *tenant_base = new share::ObTenantBase(OB_SYS_TENANT_ID);
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
    malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
  }
  tenant_base->init();
  ObTenantEnv::set_tenant(tenant_base);
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_base->set(io_service);
  ObTenantEnv::set_tenant(tenant_base);
}

void TestSSBackupExternInfoMgr::TearDownTestCase()
{
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
}

void TestSSBackupExternInfoMgr::SetUp()
{
  inner_init_();
}

void TestSSBackupExternInfoMgr::TearDown()
{
  clean_env_();
}

void TestSSBackupExternInfoMgr::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_),
                        "%s/test_ss_backup_extern_info_mgr", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_set_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = util.mkdir(test_dir_uri_, backup_set_dest_.get_storage_info());
  EXPECT_EQ(OB_SUCCESS, ret);
  ls_id_ = ObLSID(1001);
}

void TestSSBackupExternInfoMgr::clean_env_()
{
  char cmd[OB_MAX_URI_LENGTH + 16];
  (void)databuff_printf(cmd, sizeof(cmd), "rm -rf %s*", test_dir_);
  (void)system(cmd);
}

// Populate info with deterministic test strings, allocated from allocator.
void TestSSBackupExternInfoMgr::make_ss_ls_meta_info_(
    ObBackupSSLSMetaInfo &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  info.raw_ls_meta_row_.meta_type_ = static_cast<int64_t>(storage::ObSSMetaTypeEnum::LS_META);
  ret = ob_write_string(allocator, ObString::make_string("test_meta_key"),
                        info.raw_ls_meta_row_.meta_key_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ob_write_string(allocator, ObString::make_string("test_meta_value"),
                        info.raw_ls_meta_row_.meta_value_);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSBackupExternInfoMgr, publish_selected_ls_attr_candidate_after_reopen)
{
  SCN meta_scn;
  SCN first_read_scn;
  SCN second_read_scn;
  SCN create_scn;
  ASSERT_EQ(OB_SUCCESS, meta_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, first_read_scn.convert_for_inner_table_field(200));
  ASSERT_EQ(OB_SUCCESS, second_read_scn.convert_for_inner_table_field(300));
  ASSERT_EQ(OB_SUCCESS, create_scn.convert_for_inner_table_field(10));
  ObLSAttr attr;
  ASSERT_EQ(OB_SUCCESS, attr.init(ls_id_, 1001, ObLSFlag(ObLSFlag::NORMAL_FLAG),
      OB_LS_NORMAL, OB_LS_OP_CREATE_END, create_scn));
  ObBackupDataLSAttrDesc first;
  first.backup_scn_ = first_read_scn;
  ASSERT_EQ(OB_SUCCESS, first.ls_attr_array_.push_back(attr));
  ObBackupDataLSAttrDesc second;
  second.backup_scn_ = second_read_scn;
  ASSERT_EQ(OB_SUCCESS, second.ls_attr_array_.push_back(attr));

  {
    ObBackupDataStore writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(backup_set_dest_));
    ASSERT_EQ(OB_SUCCESS, writer.write_ls_attr_candidate(1, meta_scn, first));
    ASSERT_EQ(OB_SUCCESS, writer.write_ls_attr_candidate(1, meta_scn, second));
  }

  // 模拟切主后丢失内存，只按已选中的快照定位文件。
  ObBackupDataStore publisher;
  ASSERT_EQ(OB_SUCCESS, publisher.init(backup_set_dest_));
  ObBackupDataLSAttrDesc selected;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, publisher.read_ls_attr_info(1, selected));
  ASSERT_EQ(OB_SUCCESS, publisher.read_ls_attr_candidate(1, meta_scn, first_read_scn, selected));
  ASSERT_EQ(first_read_scn, selected.backup_scn_);
  ASSERT_EQ(1, selected.ls_attr_array_.count());
  ASSERT_EQ(ls_id_, selected.ls_attr_array_.at(0).get_ls_id());
  ASSERT_EQ(OB_SUCCESS, publisher.write_ls_attr(1, selected));
  ASSERT_EQ(OB_SUCCESS, publisher.write_ls_attr(1, selected));

  // 恢复端仍只读原来的固定文件，不依赖候选文件命名。
  ObBackupDataStore restore_reader;
  ASSERT_EQ(OB_SUCCESS, restore_reader.init(backup_set_dest_));
  ObBackupDataLSAttrDesc published;
  ASSERT_EQ(OB_SUCCESS, restore_reader.read_ls_attr_info(1, published));
  ASSERT_EQ(first_read_scn, published.backup_scn_);
  ASSERT_EQ(1, published.ls_attr_array_.count());
  ASSERT_EQ(ls_id_, published.ls_attr_array_.at(0).get_ls_id());
  ObBackupDataLSAttrDesc unselected;
  ASSERT_EQ(OB_SUCCESS, restore_reader.read_ls_attr_candidate(1, meta_scn, second_read_scn, unselected));
  ASSERT_EQ(second_read_scn, unselected.backup_scn_);

  SCN other_meta_scn;
  ASSERT_EQ(OB_SUCCESS, other_meta_scn.convert_for_inner_table_field(101));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, restore_reader.read_ls_attr_candidate(
      1, other_meta_scn, first_read_scn, unselected));
}

TEST_F(TestSSBackupExternInfoMgr, candidate_bytes_stable_for_shuffled_ls_attrs)
{
  // 只在 SQL 连接边界注入失败，保留真实的候选排序、文件读写和正式发布。
  class UnavailableSQLPool final : public sqlclient::ObMySQLConnectionPool {
  public:
    int acquire(const uint64_t, sqlclient::ObISQLConnection *&conn,
        ObISQLClient *, const int32_t) override
    {
      conn = nullptr;
      ++acquire_count_;
      return OB_EAGAIN;
    }
    int64_t acquire_count_ = 0;
  } sql_pool;
  ObMySQLProxy sql_proxy;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.init(&sql_pool));
  rootserver::ObBackupDataService backup_service;
  ObBackupJobAttr job;
  job.tenant_id_ = 1002;
  job.job_id_ = 1;
  job.backup_set_id_ = 1;
  SCN meta_scn;
  SCN start_scn;
  SCN read_scn;
  ASSERT_EQ(OB_SUCCESS, meta_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_inner_table_field(150));
  ASSERT_EQ(OB_SUCCESS, read_scn.convert_for_inner_table_field(200));
  ObLSAttr expected_attrs[3];
  for (int64_t i = 0; i < 3; ++i) {
    SCN create_scn;
    ASSERT_EQ(OB_SUCCESS, create_scn.convert_for_inner_table_field(10 + i));
    ASSERT_EQ(OB_SUCCESS, expected_attrs[i].init(ObLSID(1001 + i), 2001 + i,
        ObLSFlag(ObLSFlag::NORMAL_FLAG), OB_LS_NORMAL, OB_LS_OP_CREATE_END, create_scn));
  }

  ObBackupPath candidate_path;
  ObBackupPath published_path;
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_backup_ls_attr_candidate_path(
      backup_set_dest_, 1, meta_scn, read_scn, candidate_path));
  ASSERT_EQ(OB_SUCCESS, ObBackupPathUtil::get_backup_ls_attr_info_path(
      backup_set_dest_, 1, published_path));
  auto read_file_bytes = [](const ObBackupPath &path, std::string &bytes) {
    ASSERT_EQ(0, strncmp(path.get_ptr(), "file://", 7));
    std::ifstream input(path.get_ptr() + 7, std::ios::binary);
    ASSERT_TRUE(input.is_open());
    bytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    ASSERT_FALSE(bytes.empty());
  };
  const int64_t permutations[2][3] = {{1, 0, 2}, {2, 1, 0}};
  std::string first_candidate_bytes;
  std::string first_published_bytes;
  for (int64_t round = 0; round < 2; ++round) {
    ObLSAttrArray input_attrs;
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, input_attrs.push_back(expected_attrs[permutations[round][i]]));
    }
    rootserver::ObBackupSetTaskMgr mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.store_.init(backup_set_dest_));
    mgr.job_attr_ = &job;
    mgr.sql_proxy_ = &sql_proxy;
    mgr.backup_service_ = &backup_service;
    mgr.meta_tenant_id_ = 1001;
    mgr.set_task_attr_.tenant_id_ = job.tenant_id_;
    mgr.set_task_attr_.meta_turn_id_ = 1;
    mgr.set_task_attr_.status_ = ObBackupStatus::PREPARE_SS_BACKUP_SNAPSHOT;

    // 候选写出后在选点 SQL 前停止；测试不自行排序，也不模拟选点事务成功。
    ASSERT_EQ(OB_EAGAIN, mgr.persist_and_publish_backup_snapshot_(
        start_scn, read_scn, meta_scn, input_attrs));
    ASSERT_EQ(2 * round + 1, sql_pool.acquire_count_);
    std::string candidate_bytes;
    ASSERT_NO_FATAL_FAILURE(read_file_bytes(candidate_path, candidate_bytes));
    if (0 == round) {
      first_candidate_bytes = candidate_bytes;
    } else {
      ASSERT_EQ(first_candidate_bytes, candidate_bytes);
    }
    ObBackupDataLSAttrDesc candidate;
    ASSERT_EQ(OB_SUCCESS, mgr.store_.read_ls_attr_candidate(1, meta_scn, read_scn, candidate));
    ASSERT_EQ(read_scn, candidate.backup_scn_);
    ASSERT_EQ(3, candidate.ls_attr_array_.count());
    for (int64_t i = 0; i < 3; ++i) {
      const ObLSAttr &actual = candidate.ls_attr_array_.at(i);
      const ObLSAttr &expected = expected_attrs[i];
      ASSERT_EQ(expected.get_ls_id(), actual.get_ls_id());
      ASSERT_EQ(expected.get_ls_group_id(), actual.get_ls_group_id());
      ASSERT_EQ(expected.get_ls_flag(), actual.get_ls_flag());
      ASSERT_EQ(expected.flag_compatible_, actual.flag_compatible_);
      ASSERT_EQ(expected.get_ls_status(), actual.get_ls_status());
      ASSERT_EQ(expected.get_ls_operation_type(), actual.get_ls_operation_type());
      ASSERT_EQ(expected.get_create_scn(), actual.get_create_scn());
    }

    // 注入已选定的内存状态，执行真实发布；SQL 仍失败，文件已经完整写出。
    mgr.set_task_attr_.start_scn_ = start_scn;
    mgr.set_task_attr_.extra_info_.ss_meta_snapshot_scn_ = meta_scn;
    mgr.set_task_attr_.extra_info_.backup_meta_scn_ = meta_scn;
    mgr.set_task_attr_.extra_info_.read_scn_ = read_scn;
    ASSERT_EQ(OB_EAGAIN, mgr.publish_prepared_backup_snapshot_());
    ASSERT_EQ(2 * round + 2, sql_pool.acquire_count_);
    ASSERT_EQ(ObBackupStatus::PREPARE_SS_BACKUP_SNAPSHOT, mgr.set_task_attr_.status_.status_);
    std::string published_bytes;
    ASSERT_NO_FATAL_FAILURE(read_file_bytes(published_path, published_bytes));
    ASSERT_EQ(candidate_bytes, published_bytes);
    if (0 == round) {
      first_published_bytes = published_bytes;
    } else {
      ASSERT_EQ(first_published_bytes, published_bytes);
    }
  }
}

TEST_F(TestSSBackupExternInfoMgr, raw_meta_row_part_serialize_round_trip)
{
  const int64_t buf_len = 1024;
  char buf[buf_len];
  const int64_t parts[] = {0, 7};

  for (int64_t part : parts) {
    ObSSRawMetaRow src;
    src.tenant_id_ = OB_SYS_TENANT_ID;
    src.ls_id_ = ObLSID(1001);
    src.meta_type_ = static_cast<int64_t>(storage::ObSSMetaTypeEnum::LS_META);
    src.meta_key_ = ObString::make_string("test_meta_key");
    src.meta_value_ = ObString::make_string("test_meta_value");
    src.part_ = part;

    int64_t serialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize(buf, buf_len, serialize_pos));
    ASSERT_EQ(src.get_serialize_size(), serialize_pos);

    ObSSRawMetaRow dst;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, serialize_pos, deserialize_pos));
    EXPECT_EQ(serialize_pos, deserialize_pos);
    EXPECT_EQ(part, dst.part_);
  }
}

// Basic write → read round-trip: verify all fields survive serialization.
TEST_F(TestSSBackupExternInfoMgr, read_write_ls_meta_info)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;

  ObArenaAllocator write_allocator;
  ObBackupSSLSMetaInfo write_info;
  make_ss_ls_meta_info_(write_info, write_allocator);
  write_info.raw_ls_meta_row_.part_ = 7;

  ObSSExternLSMetaMgr mgr;
  ret = mgr.init(backup_set_dest_, ls_id_, turn_id, retry_id, dest_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mgr.write_ls_meta_info(write_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator read_allocator;
  ObBackupSSLSMetaInfo read_info;
  ret = mgr.read_ls_meta_info(read_allocator, read_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(write_info.raw_ls_meta_row_.meta_type_, read_info.raw_ls_meta_row_.meta_type_);
  EXPECT_EQ(write_info.raw_ls_meta_row_.part_, read_info.raw_ls_meta_row_.part_);
  EXPECT_EQ(0, write_info.raw_ls_meta_row_.meta_key_.compare(
                   read_info.raw_ls_meta_row_.meta_key_));
  EXPECT_EQ(0, write_info.raw_ls_meta_row_.meta_value_.compare(
                   read_info.raw_ls_meta_row_.meta_value_));
}

// Shared allocator pattern: mirrors merge_ss_ls_meta_infos_ usage.
// A single allocator services multiple read_ls_meta_info calls; the results
// are pushed (shallow-copied) into an array and must remain valid while the
// allocator is alive.
TEST_F(TestSSBackupExternInfoMgr, allocator_lifetime_shared_reuse)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;

  // Write two different LS meta files (reusing same ls_id with different turn_ids).
  ObLSID ls_id_a(2001);
  ObLSID ls_id_b(2002);

  {
    ObArenaAllocator wa;
    ObBackupSSLSMetaInfo wi;
    make_ss_ls_meta_info_(wi, wa);

    ObSSExternLSMetaMgr mgr_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.init(backup_set_dest_, ls_id_a, turn_id, retry_id, dest_id));
    ASSERT_EQ(OB_SUCCESS, mgr_a.write_ls_meta_info(wi));

    ObSSExternLSMetaMgr mgr_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.init(backup_set_dest_, ls_id_b, turn_id, retry_id, dest_id));
    ASSERT_EQ(OB_SUCCESS, mgr_b.write_ls_meta_info(wi));
  }

  // Read both back using a single shared allocator, push into array.
  ObArenaAllocator shared_allocator;
  ObSArray<ObBackupSSLSMetaInfo> infos;

  {
    ObSSExternLSMetaMgr mgr_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.init(backup_set_dest_, ls_id_a, turn_id, retry_id, dest_id));
    ObBackupSSLSMetaInfo info_a;
    ASSERT_EQ(OB_SUCCESS, mgr_a.read_ls_meta_info(shared_allocator, info_a));
    ASSERT_EQ(OB_SUCCESS, infos.push_back(info_a));
  }
  {
    ObSSExternLSMetaMgr mgr_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.init(backup_set_dest_, ls_id_b, turn_id, retry_id, dest_id));
    ObBackupSSLSMetaInfo info_b;
    ASSERT_EQ(OB_SUCCESS, mgr_b.read_ls_meta_info(shared_allocator, info_b));
    ASSERT_EQ(OB_SUCCESS, infos.push_back(info_b));
  }

  // Verify both elements are accessible while shared_allocator is alive.
  ASSERT_EQ(2, infos.count());
  EXPECT_EQ(static_cast<int64_t>(storage::ObSSMetaTypeEnum::LS_META),
            infos.at(0).raw_ls_meta_row_.meta_type_);
  EXPECT_EQ(static_cast<int64_t>(storage::ObSSMetaTypeEnum::LS_META),
            infos.at(1).raw_ls_meta_row_.meta_type_);
  EXPECT_EQ(0, infos.at(0).raw_ls_meta_row_.meta_key_.compare(
                   ObString::make_string("test_meta_key")));
  EXPECT_EQ(0, infos.at(0).raw_ls_meta_row_.meta_value_.compare(
                   ObString::make_string("test_meta_value")));
  EXPECT_EQ(0, infos.at(1).raw_ls_meta_row_.meta_key_.compare(
                   ObString::make_string("test_meta_key")));
  EXPECT_EQ(0, infos.at(1).raw_ls_meta_row_.meta_value_.compare(
                   ObString::make_string("test_meta_value")));
}

// Reading a non-existent file must return OB_OBJECT_NOT_EXIST.
TEST_F(TestSSBackupExternInfoMgr, read_nonexistent_file)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t dest_id = 1;
  ObLSID nonexistent_ls(9999);

  ObSSExternLSMetaMgr mgr;
  ret = mgr.init(backup_set_dest_, nonexistent_ls, turn_id, retry_id, dest_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator;
  ObBackupSSLSMetaInfo info;
  ret = mgr.read_ls_meta_info(allocator, info);
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ret);
}

#endif  // OB_BUILD_SHARED_STORAGE

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  (void)system("rm -f test_ss_backup_extern_info_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_backup_extern_info_mgr.log", true);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

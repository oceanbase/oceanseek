/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "lib/ob_errno.h"
#include "share/object_storage/ob_device_connectivity.h"
#include "share/object_storage/ob_object_storage_struct.h"

#define private public
#undef private

namespace oceanbase {
namespace unittest {

using namespace oceanbase::common;
using namespace oceanbase::share;

class TestDeviceConnectivity: public ::testing::Test
{
public:
  TestDeviceConnectivity() {}
  virtual ~TestDeviceConnectivity() {}
  virtual void SetUp()
  {
  }
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDeviceConnectivity);
};

TEST_F(TestDeviceConnectivity, DISABLED_test_device_connectivity)
{
  const char *path = "oss://cloudstorageb1/test";
  const char *endpoint = "host=oss-cn-hangzhou.aliyuncs.com";
  const char *encrypt_authorization = "access_id=xxx&access_key=xxx";
  const char *extension = "";
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, storage_dest.set(path, endpoint, encrypt_authorization, extension));
  ObDeviceConnectivityCheckManager conn_check_mgr;
  ASSERT_EQ(OB_SUCCESS, conn_check_mgr.check_device_connectivity(storage_dest));
}

TEST_F(TestDeviceConnectivity, test_change_checksum_type)
{
  ObStorageDestAttr dest_attr;
  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_NO_CHECKSUM_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=no_checksum"));

  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_MD5_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=md5"));
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=crc32"));

  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dest_attr.extension_, OB_MAX_BACKUP_EXTENSION_LENGTH, "%s", "delete_mode=xxx&s3_region=xxx"));

  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_MD5_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=md5"));

  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=crc32"));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(dest_attr.extension_, OB_MAX_BACKUP_EXTENSION_LENGTH, "%s", "delete_mode=xxx&checksum_type=md5&s3_region=xxx"));
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=crc32"));
}

TEST_F(TestDeviceConnectivity, test_condition_put_mode)
{
  const char *storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=cn-hangzhou";
  ObObjectStorageInfo inherited_info;
  ObObjectStorageInfo auto_info;
  ObObjectStorageInfo none_info;
  ObObjectStorageInfo if_match_info;
  ObObjectStorageInfo reloaded_info;
  ObObjectStorageInfo invalid_info;
  char inherited_key[OB_MAX_DEVICE_KEY_LENGTH] = {0};
  char auto_key[OB_MAX_DEVICE_KEY_LENGTH] = {0};
  char none_key[OB_MAX_DEVICE_KEY_LENGTH] = {0};
  char if_match_key[OB_MAX_DEVICE_KEY_LENGTH] = {0};
  char serialized_info[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  ObClusterStateBaseMgr &cluster_state_mgr = ObClusterStateBaseMgr::get_instance();
  ASSERT_EQ(OB_SUCCESS, ObObjectStorageInfo::register_cluster_state_mgr(&cluster_state_mgr));
  cluster_state_mgr.set_object_storage_condition_put_mode(ObObjectStorageConditionPutMode::NONE);

  ASSERT_EQ(OB_SUCCESS, inherited_info.set(OB_STORAGE_S3, storage_info));
  ASSERT_EQ(OB_SUCCESS, auto_info.set(
      OB_STORAGE_S3,
      "host=xxx.com&access_id=111&access_key=222&s3_region=cn-hangzhou&put_mode=auto"));
  ASSERT_EQ(OB_SUCCESS, none_info.set(
      OB_STORAGE_S3,
      "host=xxx.com&access_id=111&access_key=222&s3_region=cn-hangzhou&put_mode=none"));
  ASSERT_EQ(OB_SUCCESS, if_match_info.set(
      OB_STORAGE_S3,
      "host=xxx.com&access_id=111&access_key=222&s3_region=cn-hangzhou&put_mode=if-match"));

  ASSERT_EQ(ObObjectStorageConditionPutMode::INHERIT, inherited_info.get_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::NONE, inherited_info.get_effective_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::AUTO, auto_info.get_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::NONE, none_info.get_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::IF_MATCH, if_match_info.get_put_mode());
  ASSERT_STREQ("s3_region=cn-hangzhou&put_mode=auto", auto_info.get_extension());
  ASSERT_STREQ("s3_region=cn-hangzhou&put_mode=none", none_info.get_extension());
  ASSERT_STREQ("s3_region=cn-hangzhou&put_mode=if-match", if_match_info.get_extension());

  cluster_state_mgr.set_object_storage_condition_put_mode(ObObjectStorageConditionPutMode::IF_MATCH);
  ASSERT_EQ(ObObjectStorageConditionPutMode::IF_MATCH, inherited_info.get_effective_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::AUTO, auto_info.get_effective_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::NONE, none_info.get_effective_put_mode());

  cluster_state_mgr.set_object_storage_condition_put_mode(ObObjectStorageConditionPutMode::AUTO);
  ASSERT_EQ(ObObjectStorageConditionPutMode::AUTO, inherited_info.get_effective_put_mode());
  ASSERT_EQ(ObObjectStorageConditionPutMode::IF_MATCH, if_match_info.get_effective_put_mode());

  ASSERT_EQ(OB_SUCCESS, inherited_info.get_device_map_key_str(inherited_key, sizeof(inherited_key)));
  ASSERT_EQ(OB_SUCCESS, auto_info.get_device_map_key_str(auto_key, sizeof(auto_key)));
  ASSERT_EQ(OB_SUCCESS, none_info.get_device_map_key_str(none_key, sizeof(none_key)));
  ASSERT_EQ(OB_SUCCESS, if_match_info.get_device_map_key_str(if_match_key, sizeof(if_match_key)));
  ASSERT_STRNE(inherited_key, auto_key);
  ASSERT_STRNE(auto_key, none_key);
  ASSERT_STRNE(none_key, if_match_key);

  ASSERT_EQ(OB_SUCCESS, auto_info.get_storage_info_str(serialized_info, sizeof(serialized_info)));
  ASSERT_EQ(OB_SUCCESS, reloaded_info.set(OB_STORAGE_S3, serialized_info));
  ASSERT_EQ(ObObjectStorageConditionPutMode::AUTO, reloaded_info.get_put_mode());
  ASSERT_EQ(auto_info, reloaded_info);

  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_info.set(
      OB_STORAGE_S3,
      "host=xxx.com&access_id=111&access_key=222&put_mode=invalid"));

  cluster_state_mgr.set_object_storage_condition_put_mode(ObObjectStorageConditionPutMode::NONE);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_device_connectivity.log");
  OB_LOGGER.set_file_name("test_device_connectivity.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

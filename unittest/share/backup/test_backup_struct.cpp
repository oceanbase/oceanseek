/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#define OB_BUILD_UNITTEST

#include <gtest/gtest.h>
#define private public
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

using namespace oceanbase;
using namespace common;
using namespace share;

TEST(ObBackupUtils, check_is_tmp_file)
{
  bool is_tmp_file = false;
  ObString file_name_1 = "file_name.tmp.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_1, is_tmp_file));
  ASSERT_TRUE(is_tmp_file);
  ObString file_name_2 = "file_name.TMP.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_2, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_3 = "file_name.tmp";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_3, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_4 = "file_name.tmp.";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_4, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_5 = "file_name";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_5, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
  ObString file_name_6 = "file_name.123456";
  ASSERT_EQ(OB_SUCCESS, ObBackupUtils::check_is_tmp_file(file_name_6, is_tmp_file));
  ASSERT_FALSE(is_tmp_file);
}

TEST(ObBackupDest, nfs)
{
  const char *backup_test = "file:///backup_dir/?&delete_mode=tagging";
  ObBackupDest dest;
  ObBackupDest dest1;
  ObBackupDest dest2;
  ObBackupDest dest3;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest.set(backup_test));
  ASSERT_EQ(OB_SUCCESS, dest.set_storage_path(ObString(backup_test)));
  backup_test = "file:///backup_dir/";

  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char backup_path_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, dest.set(backup_test));
  ASSERT_EQ(OB_SUCCESS, dest1.set(backup_test));
  ASSERT_EQ(OB_SUCCESS, dest3.set_storage_path(ObString(backup_test)));
  LOG_INFO("dump backup dest", K(dest), K(dest.get_root_path()), KPC(dest.get_storage_info()));
  LOG_INFO("dump backup dest", K(dest3), K(dest.get_root_path()), KPC(dest.get_storage_info()));
  ASSERT_EQ(0, strcmp(dest.root_path_, "file:///backup_dir"));
  ASSERT_EQ(OB_STORAGE_FILE, dest.storage_info_->device_type_);
  ObString backup_str("file:///backup_dir/");
  ASSERT_EQ(OB_SUCCESS, dest2.set(backup_str));
  ASSERT_EQ(0, strcmp(dest.root_path_, "file:///backup_dir"));

  ASSERT_EQ(OB_SUCCESS, dest.get_backup_dest_str(backup_dest_str, sizeof(backup_dest_str)));
  ASSERT_EQ(0, strcmp(backup_dest_str, "file:///backup_dir"));
  ASSERT_EQ(OB_SUCCESS, dest.get_backup_path_str(backup_path_str, sizeof(backup_path_str)));
  ASSERT_EQ(0, strcmp(backup_path_str, "file:///backup_dir")); 
  ASSERT_TRUE(dest.is_root_path_equal(dest1));
  bool is_equal = false;
  ASSERT_EQ(OB_SUCCESS, dest.is_backup_path_equal(dest1, is_equal));
  ASSERT_TRUE(is_equal);
  is_equal = false;
  ASSERT_EQ(OB_SUCCESS, dest.is_backup_path_equal(dest3, is_equal));
  ASSERT_TRUE(is_equal);
  ASSERT_TRUE(dest == dest1);
  ASSERT_EQ(OB_SUCCESS, dest2.deep_copy(dest));
  ASSERT_EQ(0, strcmp(dest.root_path_, "file:///backup_dir"));
}

TEST(ObBackupDestAttributeParser, parse_option)
{
  int64_t max_iops = 0;
  int64_t max_bandwidth = 0;
  const char *option = "max_iops=1000&max_bandwidth=1024000b";
  ObBackupDestAttribute dest_option;
  dest_option.reset();
  ASSERT_EQ(OB_SUCCESS, ObBackupDestAttributeParser::parse(option, dest_option));
  ASSERT_EQ(1000, dest_option.max_iops_);
  ASSERT_EQ(1024000, dest_option.max_bandwidth_);

  const char *option_1 = "max_iops=1000";
  dest_option.reset();
  ASSERT_EQ(OB_SUCCESS, ObBackupDestAttributeParser::parse(option_1, dest_option));
  ASSERT_EQ(1000, dest_option.max_iops_);
  ASSERT_EQ(0, dest_option.max_bandwidth_);

  const char *option_2 = "max_bandwidth=10000b";
  dest_option.reset();
  ASSERT_EQ(OB_SUCCESS, ObBackupDestAttributeParser::parse(option_2, dest_option));
  ASSERT_EQ(0, dest_option.max_iops_);
  ASSERT_EQ(10000, dest_option.max_bandwidth_);

  const char *option_3 = "";
  dest_option.reset();
  ASSERT_EQ(-4002, ObBackupDestAttributeParser::parse(option_3, dest_option));
}

TEST(ObBackupDestAttributeParser, parse_acces_info)
{
  const char *option = "access_id=AAA&access_key=BBB";
  ObBackupDestAttribute dest_option;
  dest_option.reset();
  ASSERT_EQ(OB_SUCCESS, ObBackupDestAttributeParser::parse(option, dest_option));
  ASSERT_EQ(0, strcmp(dest_option.access_id_, "AAA"));
  ASSERT_EQ(0, strcmp(dest_option.access_key_, "BBB"));
}

#ifdef OB_BUILD_TDE_SECURITY
TEST(ObBackupDest, oss)
{
  const char *backup_test = "oss://backup_dir/?host=xxx.com&access_id=111&access_key=222&delete_mode=tagging";
  const char *backup_test1 = "oss://backup_dir/?host=xxx.com";
  ObBackupDest dest;
  ObBackupDest dest1;
  ObBackupDest dest2;
  ObBackupDest dest3;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest.set(backup_test));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest1.set(backup_test));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest2.set_storage_path(ObString(backup_test)));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest3.set_storage_path(ObString(backup_test1)));
}

TEST(ObBackupDest, oss_encrypt)
{
  const char *backup_test = "oss://backup_dir?host=xxx.com&access_id=111&encrypt_key=9B6FDE7E1E54CD292CDE5494CEB86B6F&dest_id=1001";
  ObBackupDest dest;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest.set(backup_test));
}

TEST(ObBackupDest, cos)
{
  const char *backup_test = "cos://backup_dir/?host=xxx.com&access_id=111&access_key=222&delete_mode=tagging&appid=333";
  const char *backup_test1 = "cos://backup_dir/?host=xxx.com";
  ObBackupDest dest;
  ObBackupDest dest1;
  ObBackupDest dest2;
  ObBackupDest dest3;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char backup_path_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest.set(backup_test));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest1.set(backup_test));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest2.set_storage_path(ObString(backup_test)));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest3.set_storage_path(ObString(backup_test1)));
}

TEST(ObBackupDest, cos_encrypt)
{
  const char *backup_test = "cos://backup_dir?host=xxx.com&access_id=111&encrypt_key=9B6FDE7E1E54CD292CDE5494CEB86B6F&appid=333";
  ObBackupDest dest;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, dest.set(backup_test));
}
#endif

TEST(ObBackupExtraInfo, encode_decode_normal)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo info;
  share::ObBackupExtraInfo decoded;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(1234567890));
  ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(9876543210));
  ASSERT_EQ(OB_SUCCESS, info.backup_meta_scn_.convert_for_gts(5555555555));

  ret = info.encode_to_str(buf, sizeof(buf), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(pos, 0);

  ret = decoded.decode_from_str(buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(info.ss_meta_snapshot_scn_, decoded.ss_meta_snapshot_scn_);
  ASSERT_EQ(info.read_scn_, decoded.read_scn_);
  ASSERT_EQ(info.backup_meta_scn_, decoded.backup_meta_scn_);
}

TEST(ObBackupExtraInfo, encode_decode_min_scn)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo info;
  share::ObBackupExtraInfo decoded;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(1234567890));
  info.read_scn_ = share::SCN::min_scn();

  ret = info.encode_to_str(buf, sizeof(buf), pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = decoded.decode_from_str(buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(info.ss_meta_snapshot_scn_, decoded.ss_meta_snapshot_scn_);
  ASSERT_EQ(info.read_scn_, decoded.read_scn_);
}

TEST(ObBackupExtraInfo, encode_decode_max_uint64)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo info;
  share::ObBackupExtraInfo decoded;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
  int64_t pos = 0;

  const uint64_t max_scn_ts = (1UL << 62) - 1;
  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(max_scn_ts));
  ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(max_scn_ts));
  ASSERT_EQ(OB_SUCCESS, info.backup_meta_scn_.convert_for_gts(max_scn_ts));

  ret = info.encode_to_str(buf, sizeof(buf), pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = decoded.decode_from_str(buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(info.ss_meta_snapshot_scn_, decoded.ss_meta_snapshot_scn_);
  ASSERT_EQ(info.read_scn_, decoded.read_scn_);
  ASSERT_EQ(info.backup_meta_scn_, decoded.backup_meta_scn_);
}

TEST(ObBackupExtraInfo, decode_empty_string)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo decoded;

  ret = decoded.decode_from_str("");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(decoded.ss_meta_snapshot_scn_.is_valid());
  ASSERT_FALSE(decoded.read_scn_.is_valid());
  ASSERT_FALSE(decoded.backup_meta_scn_.is_valid());
}

TEST(ObBackupExtraInfo, decode_null_ptr)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo decoded;

  ret = decoded.decode_from_str(nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObBackupExtraInfo, decode_malformed)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo decoded;

  ret = decoded.decode_from_str("ss_meta_snapshot_scn:123");
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = decoded.decode_from_str("garbage_data");
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = decoded.decode_from_str(
      "ss_meta_snapshot_scn:123;read_scn:456;backup_meta_scn:789");
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = decoded.decode_from_str("ss_meta_snapshot_scn:123,read_scn:456");
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObBackupExtraInfo, encode_buf_too_small)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo info;
  char tiny_buf[10] = "";
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(1234567890));
  ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(9876543210));

  ret = info.encode_to_str(tiny_buf, sizeof(tiny_buf), pos);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObBackupExtraInfo, encode_null_buf)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo info;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(100));
  ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(200));

  ret = info.encode_to_str(nullptr, 4096, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObBackupExtraInfo, encode_decode_roundtrip_multiple)
{
  const uint64_t max_scn_ts = (1UL << 62) - 1;
  const uint64_t test_values[][3] = {
    {0, 0, 0},
    {1, 1, 1},
    {100, 200, 300},
    {1000000000ULL, 2000000000ULL, 3000000000ULL},
    {max_scn_ts, 0, max_scn_ts},
    {0, max_scn_ts, 0},
  };

  for (int i = 0; i < sizeof(test_values) / sizeof(test_values[0]); i++) {
    share::ObBackupExtraInfo info;
    share::ObBackupExtraInfo decoded;
    char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(test_values[i][0]));
    ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(test_values[i][1]));
    ASSERT_EQ(OB_SUCCESS, info.backup_meta_scn_.convert_for_gts(test_values[i][2]));
    ASSERT_EQ(OB_SUCCESS, info.encode_to_str(buf, sizeof(buf), pos));
    ASSERT_EQ(OB_SUCCESS, decoded.decode_from_str(buf));
    ASSERT_EQ(info.ss_meta_snapshot_scn_, decoded.ss_meta_snapshot_scn_)
        << "roundtrip failed for ss meta snapshot at index " << i;
    ASSERT_EQ(info.read_scn_, decoded.read_scn_) << "roundtrip failed for read_scn at index " << i;
    ASSERT_EQ(info.backup_meta_scn_, decoded.backup_meta_scn_)
        << "roundtrip failed for backup_meta_scn at index " << i;
  }
}

TEST(ObBackupExtraInfo, encode_decode_backup_meta_only)
{
  share::ObBackupExtraInfo info;
  share::ObBackupExtraInfo decoded;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, info.backup_meta_scn_.convert_for_gts(300));
  ASSERT_EQ(OB_SUCCESS, info.encode_to_str(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, decoded.decode_from_str(buf));
  ASSERT_TRUE(decoded.ss_meta_snapshot_scn_.is_min());
  ASSERT_TRUE(decoded.read_scn_.is_min());
  ASSERT_EQ(info.backup_meta_scn_, decoded.backup_meta_scn_);
}

TEST(ObBackupExtraInfo, has_value)
{
  share::ObBackupExtraInfo info;
  ASSERT_FALSE(info.has_value());

  ASSERT_EQ(OB_SUCCESS, info.ss_meta_snapshot_scn_.convert_for_gts(100));
  ASSERT_TRUE(info.has_value());

  info.reset();
  ASSERT_FALSE(info.has_value());

  ASSERT_EQ(OB_SUCCESS, info.read_scn_.convert_for_gts(200));
  ASSERT_TRUE(info.has_value());

  info.reset();
  ASSERT_FALSE(info.has_value());

  ASSERT_EQ(OB_SUCCESS, info.backup_meta_scn_.convert_for_gts(300));
  ASSERT_TRUE(info.has_value());
}

TEST(ObBackupExtraInfo, assign)
{
  int ret = OB_SUCCESS;
  share::ObBackupExtraInfo src;
  share::ObBackupExtraInfo dst;

  ASSERT_EQ(OB_SUCCESS, src.ss_meta_snapshot_scn_.convert_for_gts(111));
  ASSERT_EQ(OB_SUCCESS, src.read_scn_.convert_for_gts(222));
  ASSERT_EQ(OB_SUCCESS, src.backup_meta_scn_.convert_for_gts(333));

  ret = dst.assign(src);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(src.ss_meta_snapshot_scn_, dst.ss_meta_snapshot_scn_);
  ASSERT_EQ(src.read_scn_, dst.read_scn_);
  ASSERT_EQ(src.backup_meta_scn_, dst.backup_meta_scn_);
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_struct.log*");
  OB_LOGGER.set_file_name("test_backup_struct.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

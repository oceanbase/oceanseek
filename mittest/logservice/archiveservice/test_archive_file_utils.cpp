/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define private public
#include "logservice/archiveservice/ob_archive_file_utils.h"
#include "logservice/palf/log_group_entry_header.h"
#undef private
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/ipalf/ipalf_log_group_entry.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#include <gtest/gtest.h>
#include <unistd.h>
namespace oceanbase
{
namespace unittest
{
typedef ObSEArray<int64_t, 16>  IntArray;
class FakeArchiveFilUtils : public archive::ObArchiveFileUtils
{
public:
  static int locate_file_by_scn(IntArray &array, const share::SCN &ref_scn, int64_t &file_id)
  {
    int ret = OB_SUCCESS;
    auto get_value = [&](const int64_t index, share::SCN &scn, palf::LSN &lsn) -> int
    {
      int ret = 0;
      if (index >= array.count()) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        lsn = palf::LSN(index);
        scn.convert_for_tx(array[index]);
      }
      return ret;
    };
    ret = locate_(0, array.count() - 1, ref_scn, file_id, get_value);
    return ret;
  }
};

TEST(TestLocateFunc, test_archive_locate_func)
{
  IntArray array1;
  array1.push_back(1);
  array1.push_back(23);
  array1.push_back(32);
  array1.push_back(33);
  array1.push_back(35);
  array1.push_back(65);
  array1.push_back(77);
  array1.push_back(88);
  array1.push_back(89);
  array1.push_back(99);

  share::SCN scn = share::SCN::min_scn();
  int64_t file_id = -1;

  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(1);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(2);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(2);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 0);

  scn.convert_for_tx(33);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(33);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(34);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 3);

  scn.convert_for_tx(99);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 9);

  scn.convert_for_tx(101);
  EXPECT_EQ(OB_SUCCESS, FakeArchiveFilUtils::locate_file_by_scn(array1, scn, file_id));
  EXPECT_EQ(file_id, 9);
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
class TestArchiveHeader : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, common::ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, common::ObIOManager::get_instance().init(1024L * 1024 * 1024));
    ASSERT_EQ(OB_SUCCESS, common::ObIOManager::get_instance().start());
  }

  static void TearDownTestCase()
  {
    common::ObIOManager::get_instance().stop();
    common::ObIOManager::get_instance().destroy();
  }

  void SetUp() override
  {
    old_enable_logservice_ = GCONF.enable_logservice;
    GCONF.enable_logservice = true;
    STRCPY(path_, "/tmp/test_archive_header_XXXXXX");
    fd_ = mkstemp(path_);
    ASSERT_GE(fd_, 0);
  }

  void TearDown() override
  {
    if (fd_ >= 0) {
      close(fd_);
      unlink(path_);
    }
    GCONF.enable_logservice = old_enable_logservice_;
  }

  static void decode_header(const char *hex, char *buf)
  {
    for (int i = 0; i < 56; ++i) {
      unsigned int byte = 0;
      ASSERT_EQ(1, sscanf(hex + i * 2, "%2x", &byte));
      buf[i] = static_cast<char>(byte);
    }
  }

  int read_header(const char *buf, int64_t len, palf::LSN &lsn, share::SCN &scn)
  {
    if (len != pwrite(fd_, buf, len, 0) || 0 != ftruncate(fd_, len)) {
      return OB_IO_ERROR;
    }
    char uri[256];
    snprintf(uri, sizeof(uri), "file://%s", path_);
    share::ObBackupStorageInfo storage_info;
    int ret = storage_info.set(common::OB_STORAGE_FILE, "");
    if (OB_SUCCESS == ret) {
      ret = archive::ObArchiveFileUtils::extract_file_min_log_info_(ObString(uri), &storage_info,
          common::ObStorageIdMod(1, common::ObStorageUsedMod::STORAGE_USED_ARCHIVE), lsn, scn);
    }
    return ret;
  }

  bool old_enable_logservice_ = false;
  int fd_ = -1;
  char path_[128];
};

TEST_F(TestArchiveHeader, ss_archive_header)
{
  // Real libpalf headers from LS 1001/951.obarc and LS 1003/8.obarc.
  const char *headers[] = {
      "47520001001c424500000000000000050000000ed7b7000018d5e545a3e56490"
      "000000001f20309e00000000000386fb200000000000f169",
      "475200010017a62f0000000000000004000000001bff900018d5e6c8acb1b80b"
      "0000000070e2db2c00000000000090c420000000000036b6"};
  const uint64_t lsns[] = {63749529600, 469733376};
  const uint64_t scns[] = {1789588514214864016, 1789590176514816011};
  auto get_min_log_info = [&](const int64_t file_id, share::SCN &scn, palf::LSN &lsn) -> int
  {
    char buf[archive::ARCHIVE_FILE_HEADER_SIZE + 56] = {};
    archive::ObArchiveFileHeader file_header;
    int ret = file_header.generate_header(palf::LSN(lsns[file_id - 1]));
    int64_t pos = 0;
    if (OB_SUCC(ret)) {
      ret = file_header.serialize(buf, sizeof(buf), pos);
    }
    if (OB_SUCC(ret)) {
      decode_header(headers[file_id - 1], buf + archive::ARCHIVE_FILE_HEADER_SIZE);
      ret = read_header(buf, sizeof(buf), lsn, scn);
    }
    return ret;
  };
  // Exercise file selection with valid SS headers on either side of the target SCN.
  share::SCN ref_scn;
  int64_t file_id = -1;
  ASSERT_EQ(OB_SUCCESS, ref_scn.convert_for_logservice(scns[0] + 1));
  ASSERT_EQ(OB_SUCCESS, archive::ObArchiveFileUtils::locate_(1, 2, ref_scn, file_id, get_min_log_info));
  EXPECT_EQ(1, file_id);
  ASSERT_EQ(OB_SUCCESS, ref_scn.convert_for_logservice(scns[1]));
  ASSERT_EQ(OB_SUCCESS, archive::ObArchiveFileUtils::locate_(1, 2, ref_scn, file_id, get_min_log_info));
  EXPECT_EQ(2, file_id);
  for (int i = 0; i < 2; ++i) {
    char buf[archive::ARCHIVE_FILE_HEADER_SIZE + 56] = {};
    archive::ObArchiveFileHeader file_header;
    ASSERT_EQ(OB_SUCCESS, file_header.generate_header(palf::LSN(lsns[i])));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, file_header.serialize(buf, sizeof(buf), pos));
    decode_header(headers[i], buf + archive::ARCHIVE_FILE_HEADER_SIZE);
    palf::LogGroupEntryHeader legacy_header;
    pos = 0;
    ASSERT_EQ(OB_INVALID_DATA, legacy_header.deserialize(buf + archive::ARCHIVE_FILE_HEADER_SIZE, 56, pos));

    palf::LSN lsn;
    share::SCN scn;
    ASSERT_EQ(OB_SUCCESS, read_header(buf, sizeof(buf), lsn, scn));
    EXPECT_EQ(lsns[i], lsn.val_);
    EXPECT_EQ(scns[i], scn.get_val_for_logservice());

    EXPECT_NE(OB_SUCCESS, read_header(buf, sizeof(buf) - 1, lsn, scn));
    buf[sizeof(buf) - 1] ^= 1;
    EXPECT_EQ(OB_INVALID_DATA, read_header(buf, sizeof(buf), lsn, scn));
    EXPECT_EQ(lsns[i], lsn.val_);
    EXPECT_EQ(scns[i], scn.get_val_for_logservice());
  }
}

TEST_F(TestArchiveHeader, legacy_archive_header)
{
  GCONF.enable_logservice = false;
  for (int16_t version = 1; version <= 2; ++version) {
    char buf[archive::ARCHIVE_FILE_HEADER_SIZE + 56] = {};
    archive::ObArchiveFileHeader file_header;
    ASSERT_EQ(OB_SUCCESS, file_header.generate_header(palf::LSN(4096)));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, file_header.serialize(buf, sizeof(buf), pos));
    palf::LogGroupEntryHeader header;
    header.magic_ = 0x4752;
    header.version_ = version;
    header.group_size_ = 1024;
    header.proposal_id_ = 1;
    header.committed_end_lsn_ = palf::LSN(4096);
    ASSERT_EQ(OB_SUCCESS, header.max_scn_.convert_for_logservice(123456789));
    header.log_id_ = 1;
    header.update_header_checksum();
    pos = archive::ARCHIVE_FILE_HEADER_SIZE;
    ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
    palf::LSN lsn;
    share::SCN scn;
    ASSERT_EQ(OB_SUCCESS, read_header(buf, sizeof(buf), lsn, scn));
    EXPECT_EQ(4096, lsn.val_);
    EXPECT_EQ(123456789, scn.get_val_for_logservice());
  }
}
#endif

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_locate_func.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_archive_locate_func");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

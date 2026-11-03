/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include <gtest/gtest.h>
#include <dirent.h>
#include <mutex>
#include <atomic>
#include <thread>
#include <fstream>
#include <iterator>
#include <deque>
#include <fcntl.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <signal.h>

#define protected public
#define private public

#include "lib/oblog/ob_log_compressor.h"
#include "lib/compress/ob_compressor_pool.h"

using namespace oceanbase::lib;
namespace oceanbase {
namespace common {

const int TEST_CPP_LOG_TYPE_COUNT = 4;
const char *TEST_DIR = "testoblogcompressdir";
const char *TEST_ALERT_DIR = "testoblogcompressdiralert";
const char *RM_COMMAND = "rm -rf testoblogcompressdir testoblogcompressdiralert";

#define CREATE_FILES(file_count, modify_time, timestamp)                                                              \
  for (int i = 0; i < file_count; i++) {                                                                              \
    snprintf(file_name, sizeof(file_name), "%s/%s.%ld", TEST_DIR, OB_SYSLOG_FILE_PREFIX[i % TEST_CPP_LOG_TYPE_COUNT], \
             timestamp + i);                                                                                          \
    FILE *file = fopen(file_name, "w");                                                                               \
    ASSERT_EQ(true, OB_NOT_NULL(file));                                                                               \
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, file));                                                           \
    fclose(file);                                                                                                     \
    OB_LOG_COMPRESSOR.set_last_modify_time_(file_name, modify_time + i * 60);                                         \
  }

int get_file_count_by_regex(const char *dirname, const char *pattern)
{
  int count = 0;
  regex_t regex;
  struct dirent *entry;
  DIR *dir = opendir(dirname);

  if (OB_ISNULL(dir)) {
    count = -1;
  } else if (OB_NOT_NULL(pattern) && regcomp(&regex, pattern, REG_EXTENDED | REG_NOSUB) != 0) {
    closedir(dir);
    count = -1;
  } else {
    while ((entry = readdir(dir)) != NULL) {
      if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0) {
        continue;
      }
      if (OB_ISNULL(pattern) || regexec(&regex, entry->d_name, 0, NULL, 0) == 0) {
        count++;
      }
    }

    closedir(dir);
    if (OB_NOT_NULL(pattern)) {
      regfree(&regex);
    }
  }

  return count;
}

int compress_file_by_zstd(ObCompressor *compressor, const char *file_name_in, const char *file_name_out)
{
  int ret = OB_SUCCESS;

  int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, ObModIds::OB_COMPRESSOR);
  char *dest_buf = src_buf + src_size;
  size_t read_size = 0;
  int64_t w_size = 0;
  int sleep_10us = 10 * 1000;

  FILE *input_file = fopen(file_name_in, "r");
  FILE *output_file = fopen(file_name_out, "w");
  fwrite(dest_buf, 1, w_size, output_file);

  while (OB_SUCC(ret) && !feof(input_file)) {
    if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
      if (OB_FAIL(compressor->compress(src_buf, read_size, dest_buf, dest_size, w_size))) {
        printf("Failed to log_compress_block, err_code=%d.\n", ret);
      } else if (w_size != fwrite(dest_buf, 1, w_size, output_file)) {
        ret = OB_ERR_SYS;
        printf("Failed to fwrite, err_code=%d.\n", errno);
      }
    }
  }
  fclose(input_file);
  fclose(output_file);

  // clear environment
  ob_free(src_buf);

  return ret;
}

TEST(ObLogCompressor, log_priority_array_test)
{
  int ret = OB_SUCCESS;
  const int MAX_FILE_COUNT = OB_SYSLOG_DELETE_ARRAY_SIZE - 1;
  ObSyslogFile syslog;
  const ObSyslogFile *file_name_out;
  ObSyslogCompareFunctor cmp_;
  ObSyslogPriorityArray priority_array(cmp_);

  for (int i = 0; i < MAX_FILE_COUNT; i++) {
    syslog.mtime_ = 1500 + i;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1500 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.push(syslog));
  }
  ASSERT_EQ(MAX_FILE_COUNT, priority_array.count());
  for (int i = 0; i < MAX_FILE_COUNT / 2; i++) {
    syslog.mtime_ = 1200 + i;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1200 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.push(syslog));
  }
  ASSERT_EQ(MAX_FILE_COUNT, priority_array.count());
  for (int i = 0; i < MAX_FILE_COUNT / 2; i++) {
    file_name_out = NULL;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1200 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.top(file_name_out));
    ASSERT_EQ(true, OB_NOT_NULL(file_name_out));
    ASSERT_EQ(0, strncmp(syslog.file_name_, file_name_out->file_name_, strlen(syslog.file_name_)));
    ASSERT_EQ(OB_SUCCESS, priority_array.pop());
  }
  for (int i = 0; i < MAX_FILE_COUNT / 2; i++) {
    file_name_out = NULL;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1500 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.top(file_name_out));
    ASSERT_EQ(true, OB_NOT_NULL(file_name_out));
    ASSERT_EQ(0, strncmp(syslog.file_name_, file_name_out->file_name_, strlen(syslog.file_name_)));
    ASSERT_EQ(OB_SUCCESS, priority_array.pop());
  }
}

TEST(ObLogCompressor, base_compressor_test)
{
  int ret = OB_SUCCESS;
  int test_count = 1000;
  int test_size = test_count * sizeof(int);
  ObCompressor *compressor_zstd;

  // get compressor
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor_zstd));

  // prepare file
  const char *file_name = "test_ob_log_compressor_file";
  const char *file_name_zstd = "test_ob_log_compressor_file.zst";
  std::string file_name_str = std::string(file_name);
  std::string decompress_file_name_zstd = file_name_str + "_zstd";
  unlink(file_name_str.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());

  FILE *input_file = fopen(file_name, "w");
  ASSERT_EQ(true, NULL != input_file);
  int data[test_count];
  for (int i = 0; i < test_count; i++) {
    data[i] = i;
  }
  ASSERT_EQ(test_size, fwrite(data, 1, test_size, input_file));
  fclose(input_file);
  // sleep 1s
  sleep(1);
  struct stat st, st_zstd;
  ASSERT_EQ(0, stat(file_name, &st));

  // compress
  ASSERT_EQ(OB_SUCCESS, compress_file_by_zstd(compressor_zstd, file_name, file_name_zstd));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime != st_zstd.st_mtime);  // not equal

  // modify time
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_last_modify_time_(file_name_zstd, st.st_mtime));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime == st_zstd.st_mtime);  // equal

  // decompress, make sure your machine can exec zstd command
  std::string decompress_zstd_command = "zstd -d " + std::string(file_name_zstd) + " -o " + decompress_file_name_zstd;
  ASSERT_EQ(0, std::system(decompress_zstd_command.c_str()));

  // check
  std::string compare_command = "diff -q " + file_name_str + " " + decompress_file_name_zstd + " > /dev/null";
  ASSERT_EQ(0, system(compare_command.c_str()));

  unlink(file_name_str.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());
}

TEST(ObLogCompressor, syslog_compressor_base_test)
{
  int ret = OB_SUCCESS;

  // prepare
  int data_count = 1000;
  int loop_count = 2000;
  int data_size = data_count * sizeof(int);
  int data[data_count];
  const char *file_name = "testcompressfile.log.1234";
  const char *file_name_2 = "testcompressfile2.log.1234";
  const char *file_name_zstd = "testcompressfile.log.1234.zst";
  ASSERT_EQ(false, ObLogCompressor::is_compressed_file(file_name));
  ASSERT_EQ(true, ObLogCompressor::is_compressed_file(file_name_zstd));
  std::string file_name_str = std::string(file_name);
  std::string file_name_str_2 = std::string(file_name_2);
  std::string decompress_file_name_zstd = file_name_str + "_zstd";
  unlink(file_name_str.c_str());
  unlink(file_name_str_2.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());

  // function test
  ASSERT_EQ(0, system(RM_COMMAND));
  ASSERT_EQ(0, mkdir(TEST_DIR, 0777));
  snprintf(OB_LOG_COMPRESSOR.syslog_dir_, sizeof(OB_LOG_COMPRESSOR.syslog_dir_), "%s", TEST_DIR);
  int64_t free_size = OB_LOG_COMPRESSOR.get_disk_remaining_size_();
  printf("free_size:%ld\n", free_size);
  ASSERT_GT(free_size, 0);
  ASSERT_EQ(0, system(RM_COMMAND));

  // prepare file data
  FILE *input_file = fopen(file_name, "w");
  ASSERT_EQ(true, NULL != input_file);
  for (int i = 0; i < data_count; i++) {
    data[i] = i;
  }
  for (int j = 0; j < loop_count; j++) {
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, input_file));
  }
  fclose(input_file);
  std::string cp_file_command = "cp " + std::string(file_name) + " " + std::string(file_name_2);
  ASSERT_EQ(0, std::system(cp_file_command.c_str()));

  // record last modify time
  struct stat st, st_zstd;
  ASSERT_EQ(0, stat(file_name, &st));

  // compress
  ObCompressor *compressor_zstd;
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor_zstd));
  LOG_INFO("start to init syslog compressor ");
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.init());
  snprintf(OB_LOG_COMPRESSOR.syslog_dir_, sizeof(OB_LOG_COMPRESSOR.syslog_dir_), "%s", TEST_DIR);
  OB_LOG_COMPRESSOR.compressor_ = compressor_zstd;
  ASSERT_EQ(true, OB_LOG_COMPRESSOR.is_inited_);
  ASSERT_EQ(false, OB_LOG_COMPRESSOR.stopped_);
  OB_LOG_COMPRESSOR.resume_after_config_update();
  usleep(100000);  // wait register pm in farm

  int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, ObModIds::OB_LOG);
  ASSERT_EQ(true, OB_NOT_NULL(src_buf));
  char *dest_buf = src_buf + src_size;
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.compress_single_file_(file_name, src_buf, dest_buf));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime == st_zstd.st_mtime);  // equal

  // decompress
  std::string decompress_zstd_command = "zstd -d " + std::string(file_name_zstd) + " -o " + decompress_file_name_zstd;
  ASSERT_EQ(0, std::system(decompress_zstd_command.c_str()));
  std::string compare_command = "diff -q " + file_name_str_2 + " " + decompress_file_name_zstd + " > /dev/null";
  ASSERT_EQ(0, std::system(compare_command.c_str()));

  // error test
  ASSERT_EQ(OB_INVALID_ARGUMENT, OB_LOG_COMPRESSOR.set_compress_func("zlib_1.0"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, OB_LOG_COMPRESSOR.set_compress_func("lz4_1.9.1"));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("none"));

  // clear
  LOG_INFO("start to destroy syslog compressor ");
  OB_LOG_COMPRESSOR.destroy();
  ob_free(src_buf);
  unlink(file_name_str.c_str());
  unlink(file_name_str_2.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());
}

TEST(ObLogCompressor, syslog_compressor_thread_test)
{
  int ret = OB_SUCCESS;

  // init log compressor
  ASSERT_EQ(OB_SUCCESS, ObClockGenerator::get_instance().init());
  // loop faster
  OB_LOG_COMPRESSOR.loop_interval_ = 100000;
  LOG_INFO("start to init syslog compressor ");
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.init());
  ASSERT_EQ(true, OB_LOG_COMPRESSOR.is_inited_);
  snprintf(OB_LOG_COMPRESSOR.syslog_dir_, sizeof(OB_LOG_COMPRESSOR.syslog_dir_), "%s", TEST_DIR);
  snprintf(OB_LOG_COMPRESSOR.alert_log_dir_, sizeof(OB_LOG_COMPRESSOR.alert_log_dir_), "%s", TEST_ALERT_DIR);

  // prepare syslog file
  const int MAX_SYSLOG_COUNT = 8;
  const int min_uncompressed_count = 2;
  char file_name[OB_MAX_SYSLOG_FILE_NAME_SIZE];
  char *file_name_out;
  const char *data =
      "base_data.base_data.base_data.base_data.base_dat50base_data.base_data.base_data.base_data.base_da100"
      "base_data.base_data.base_data.base_data.base_da150base_data.base_data.base_data.base_data.base_da200";
  int data_size = strlen(data);
  int file_size = 200;
  const char *pattern_uncompressed[TEST_CPP_LOG_TYPE_COUNT] = {
      "^observer\\.log\\.[0-9]+$",
      "^rootservice\\.log\\.[0-9]+$",
      "^election\\.log\\.[0-9]+$",
      "^trace\\.log\\.[0-9]+$",
  };
  const char *pattern_compressed[TEST_CPP_LOG_TYPE_COUNT] = {
      "^observer\\.log\\.[0-9]+\\.[a-z]+$",
      "^rootservice\\.log\\.[0-9]+\\.[a-z]+$",
      "^election\\.log\\.[0-9]+\\.[a-z]+$",
      "^trace\\.log\\.[0-9]+\\.[a-z]+$",
  };

  // get modify time
  time_t last_modified_time = time(NULL);

  last_modified_time -= 1000;
  ASSERT_EQ(0, system(RM_COMMAND));
  ASSERT_EQ(0, mkdir(TEST_DIR, 0777));
  ASSERT_EQ(0, mkdir(TEST_ALERT_DIR, 0777));
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    snprintf(file_name, sizeof(file_name), "%s/%s", TEST_DIR, OB_SYSLOG_FILE_PREFIX[i % TEST_CPP_LOG_TYPE_COUNT]);
    FILE *file = fopen(file_name, "w");
    ASSERT_EQ(true, OB_NOT_NULL(file));
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, file));
    fclose(file);
    OB_LOG_COMPRESSOR.set_last_modify_time_(file_name, last_modified_time + i * 60);
  }
  last_modified_time -= 50000;
  CREATE_FILES(MAX_SYSLOG_COUNT * TEST_CPP_LOG_TYPE_COUNT, last_modified_time, 202311241000L)

  // set parameter
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(MAX_SYSLOG_COUNT));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_min_uncompressed_count(min_uncompressed_count));
  int total_count = get_file_count_by_regex(TEST_DIR, NULL);
  int last_total_count = 0;
  ASSERT_EQ((MAX_SYSLOG_COUNT + 1) * TEST_CPP_LOG_TYPE_COUNT, total_count);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_EQ(MAX_SYSLOG_COUNT, uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_EQ(0, compressed_count);
  }

  // enable compress
  int64_t max_disk_size =
      OB_SYSLOG_COMPRESS_RESERVE_SIZE + file_size * ((MAX_SYSLOG_COUNT + 1 - 2) * TEST_CPP_LOG_TYPE_COUNT);
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_max_disk_size(max_disk_size));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("zstd_1.3.8"));
  OB_LOG_COMPRESSOR.resume_after_config_update();
  sleep(2);  // 2s
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_EQ((MAX_SYSLOG_COUNT + 1) * TEST_CPP_LOG_TYPE_COUNT, total_count);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_LE(uncompressed_count, MAX_SYSLOG_COUNT - 2);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_GE(compressed_count, 2);
  }

  // add more syslog file
  last_modified_time += 10000;
  CREATE_FILES(2 * TEST_CPP_LOG_TYPE_COUNT, last_modified_time, 202311242000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_EQ((MAX_SYSLOG_COUNT + 3) * TEST_CPP_LOG_TYPE_COUNT, total_count);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_LE(uncompressed_count, MAX_SYSLOG_COUNT - 2);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_GE(compressed_count, 4);
  }

  // make syslog_disk_size smaller, begin to delete file
  max_disk_size = OB_SYSLOG_DELETE_RESERVE_SIZE + file_size * (4 * TEST_CPP_LOG_TYPE_COUNT);
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_max_disk_size(max_disk_size));
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LT(total_count, (MAX_SYSLOG_COUNT + 3) * TEST_CPP_LOG_TYPE_COUNT);
  ASSERT_GE(total_count, 4 * TEST_CPP_LOG_TYPE_COUNT);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
  }
  last_total_count = total_count;

  // add more syslog file
  last_modified_time += 5000;
  CREATE_FILES(2 * TEST_CPP_LOG_TYPE_COUNT, last_modified_time, 202311243000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LE(total_count, last_total_count);
  ASSERT_GE(total_count, 4 * TEST_CPP_LOG_TYPE_COUNT);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
  }

  // disable compress
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("none"));
  last_modified_time += 5000;
  CREATE_FILES(2 * TEST_CPP_LOG_TYPE_COUNT, last_modified_time, 202311244000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LT(total_count, 5 * TEST_CPP_LOG_TYPE_COUNT);
  ASSERT_GE(total_count, 4 * TEST_CPP_LOG_TYPE_COUNT);
  for (int i = 0; i < TEST_CPP_LOG_TYPE_COUNT; i++) {
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_EQ(compressed_count, 0);
  }

  // clear
  ASSERT_EQ(0, system(RM_COMMAND));
  LOG_INFO("start to destroy syslog compressor ");
  OB_LOG_COMPRESSOR.destroy();
}

TEST(ObLogCompressor, libpalf_archive_classification)
{
  EXPECT_FALSE(ObLogCompressor::is_compressed_file("libpalf.log.20261012120000.1"));
  EXPECT_FALSE(ObLogCompressor::is_compressed_file("libpalf.log.20261012120000.10"));
  EXPECT_TRUE(ObLogCompressor::is_compressed_file("libpalf.log.20261012120000.zst"));
  EXPECT_TRUE(ObLogCompressor::is_compressed_file("libpalf.log.20261012120000.2.zst"));
  EXPECT_FALSE(ObLogCompressor::is_compressed_file("libpalf.log.20261012120000.zst.tmp.1.2"));
  EXPECT_LT(OB_LOG_COMPRESSOR.get_log_type_("libpalf.log.20261012120000.1"), OB_SYSLOG_COMPRESS_TYPE_COUNT);
}

TEST(ObLogCompressor, libpalf_preserves_existing_compressed_archive)
{
  char directory[] = "/tmp/ob_libpalf_compress_XXXXXX";
  ASSERT_NE(nullptr, mkdtemp(directory));
  const std::string source = std::string(directory) + "/libpalf.log.20261012120000.1";
  const std::string output = source + ".zst";
  const char original[] = "new source content";
  const char previous[] = "previous archive must survive";
  FILE *file = fopen(source.c_str(), "w");
  ASSERT_NE(nullptr, file);
  ASSERT_EQ(sizeof(original), fwrite(original, 1, sizeof(original), file));
  ASSERT_EQ(0, fclose(file));
  file = fopen(output.c_str(), "w");
  ASSERT_NE(nullptr, file);
  ASSERT_EQ(sizeof(previous), fwrite(previous, 1, sizeof(previous), file));
  ASSERT_EQ(0, fclose(file));
  ObLogCompressor compressor;
  compressor.stopped_ = false;
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor.compressor_));
  char *buffers = static_cast<char *>(
      ob_malloc(OB_SYSLOG_COMPRESS_BLOCK_SIZE + OB_SYSLOG_COMPRESS_BUFFER_SIZE, ObModIds::OB_COMPRESSOR));
  ASSERT_NE(nullptr, buffers);
  EXPECT_NE(OB_SUCCESS,
            compressor.compress_single_file_(source.c_str(), buffers, buffers + OB_SYSLOG_COMPRESS_BLOCK_SIZE));
  EXPECT_EQ(0, access(source.c_str(), F_OK));
  file = fopen(output.c_str(), "r");
  ASSERT_NE(nullptr, file);
  char contents[sizeof(previous)] = {};
  EXPECT_EQ(sizeof(previous), fread(contents, 1, sizeof(contents), file));
  EXPECT_EQ(0, memcmp(previous, contents, sizeof(previous)));
  fclose(file);
  ob_free(buffers);
  unlink(source.c_str());
  unlink(output.c_str());
  rmdir(directory);
}

class LibPalfSyslogTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    strcpy(directory_, "/tmp/ob_libpalf_syslog_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(directory_));
    snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
    snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
    ASSERT_EQ(0, mkdir(compressor_.alert_log_dir_, 0700));
    compressor_.stopped_ = false;
    buffers_ = static_cast<char *>(
        ob_malloc(OB_SYSLOG_COMPRESS_BLOCK_SIZE + OB_SYSLOG_COMPRESS_BUFFER_SIZE, ObModIds::OB_COMPRESSOR));
    ASSERT_NE(nullptr, buffers_);
    ASSERT_EQ(OB_SUCCESS,
              ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor_.compressor_));
  }
  void TearDown() override
  {
    compressor_.destroy();
    ob_free(buffers_);
    DIR *dir = opendir(directory_);
    if (dir != nullptr) {
      struct dirent *entry;
      while (nullptr != (entry = readdir(dir))) {
        if (entry->d_name[0] != '.') {
          unlink(path(entry->d_name).c_str());
        }
      }
      closedir(dir);
    }
    rmdir(path("alert").c_str());
    rmdir(directory_);
  }
  std::string path(const char *name) const
  {
    return std::string(directory_) + "/" + name;
  }
  void create(const char *name, const std::string &data = "a complete log record\n", int64_t modified = 100)
  {
    const std::string filename = path(name);
    FILE *file = fopen(filename.c_str(), "w");
    ASSERT_NE(nullptr, file);
    ASSERT_EQ(data.size(), fwrite(data.data(), 1, data.size(), file));
    ASSERT_EQ(0, fclose(file));
    const struct timespec times[] = {{modified, 123456789}, {modified, 123456789}};
    ASSERT_EQ(0, utimensat(AT_FDCWD, filename.c_str(), times, 0));
  }
  std::string contents(const std::string &file)
  {
    std::ifstream input(file, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
  }
  bool exists(const char *name) const
  {
    return access(path(name).c_str(), F_OK) == 0;
  }
  int compress(const char *name)
  {
    return compressor_.compress_single_file_(path(name).c_str(), buffers_, buffers_ + OB_SYSLOG_COMPRESS_BLOCK_SIZE);
  }
  char directory_[64] = {};
  ObLogCompressor compressor_;
  char *buffers_ = nullptr;
};

TEST_F(LibPalfSyslogTest, validates_full_filename_and_calendar)
{
  const char *invalid[] = {"libpalf.log.20230229010101",       "libpalf.log.20261012240000",
                           "libpalf.log.20261012120000123",    "libpalf.log.20261012120000.18446744073709551616",
                           "libpalf.log.20261012120000.1.bak", "libpalf.log.wf.20261012120000",
                           "libpalf.log.20261012120000.gz",    "libpalf.log.20261012120000.zst.tmp.1.2",
                           "libpalf.log.20261012120000."};
  for (const char *name : invalid) {
    EXPECT_EQ(LIBPALF_OTHER_FILE, ObLogCompressor::classify_libpalf_file(name)) << name;
  }
  EXPECT_EQ(LIBPALF_ARCHIVE_FILE, ObLogCompressor::classify_libpalf_file("libpalf.log.20240229010101.10"));
  EXPECT_EQ(LIBPALF_COMPRESSED_FILE, ObLogCompressor::classify_libpalf_file("libpalf.log.20261012120000.2.zst"));
  EXPECT_EQ(LIBPALF_TEMP_FILE, ObLogCompressor::classify_libpalf_file("libpalf.log.20261012120000.2.zst.tmp.11.22.33"));
  EXPECT_EQ(LIBPALF_ACTIVE_FILE, ObLogCompressor::classify_libpalf_file("libpalf.log"));
  EXPECT_EQ(NOT_LIBPALF_FILE, ObLogCompressor::classify_libpalf_file("observer.log.20261012120000123"));
}

TEST_F(LibPalfSyslogTest, round_trip_both_algorithms_preserves_nanoseconds)
{
  std::string original(2 * OB_SYSLOG_COMPRESS_BLOCK_SIZE + 117, 'x');
  for (size_t i = 0; i < original.size(); i += 101) {
    original[i] = 'a' + i % 26;
  }
  const ObCompressorType algorithms[] = {ZSTD_COMPRESSOR, ZSTD_1_3_8_COMPRESSOR};
  const char *files[] = {"libpalf.log.20261012120000.1", "libpalf.log.20261012120000.10"};
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(algorithms[i], compressor_.compressor_));
    create(files[i], original);
    ASSERT_EQ(OB_SUCCESS, compress(files[i]));
    EXPECT_FALSE(exists(files[i]));
    const std::string output = path(files[i]) + ".zst";
    struct stat st;
    ASSERT_EQ(0, stat(output.c_str(), &st));
    EXPECT_EQ(100, st.st_mtim.tv_sec);
    EXPECT_EQ(123456789, st.st_mtim.tv_nsec);
    const std::string command = "zstd -q -d -f " + output + " -o " + path("decoded");
    ASSERT_EQ(0, system(command.c_str()));
    EXPECT_EQ(original, contents(path("decoded")));
  }
}

TEST_F(LibPalfSyslogTest, scanner_counts_outputs_and_protects_active_unknown_and_symlinks)
{
  create("libpalf.log", "active");
  create("libpalf.log.20261012120000.2", "raw two", 200);
  create("libpalf.log.20261012120000.10", "raw ten", 100);
  create("libpalf.log.20261012110000", "conflicting raw", 10);
  create("libpalf.log.20261012110000.zst", "existing compressed", 10);
  create("libpalf.log.20261012120000.bak", "unknown");
  create("observer.log.wf.20261012120000000", "wf");
  create("libpalf.log.20261012100000.zst.tmp.1.2.3", "abandoned");
  ASSERT_EQ(0, symlink("libpalf.log", path("libpalf.log.20261012000000").c_str()));
  ObSyslogFile candidates[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t total = 0;
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(true, true, true, candidates, counts, total));
  EXPECT_EQ(3, counts[SYSLOG_LIBPALF]);
  EXPECT_STREQ(path("libpalf.log.20261012120000.10").c_str(), candidates[SYSLOG_LIBPALF].file_name_);
  EXPECT_EQ(4, compressor_.oldest_files_.count());
  EXPECT_FALSE(exists("libpalf.log.20261012100000.zst.tmp.1.2.3"));
  EXPECT_EQ(6 + 7 + 7 + 15 + 19 + 7 + 2 + 6, total);
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(true, false, false, candidates, counts, total));
  EXPECT_EQ(3, counts[SYSLOG_LIBPALF]);
  EXPECT_EQ('\0', candidates[SYSLOG_LIBPALF].file_name_[0]);
  EXPECT_EQ(4, compressor_.oldest_files_.count());  // .zst remains a space-recycling candidate in none mode.
}

TEST_F(LibPalfSyslogTest, rejects_symlink_and_active_hardlink)
{
  create("libpalf.log", "active content");
  ASSERT_EQ(0, symlink("libpalf.log", path("libpalf.log.20261012120000").c_str()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, compress("libpalf.log.20261012120000"));
  ASSERT_EQ(0, link(path("libpalf.log").c_str(), path("libpalf.log.20261012120000.1").c_str()));
  EXPECT_EQ(OB_STATE_NOT_MATCH, compress("libpalf.log.20261012120000.1"));
  EXPECT_EQ("active content", contents(path("libpalf.log")));
}

TEST_F(LibPalfSyslogTest, cancellation_preserves_input_and_has_no_final_output)
{
  create("libpalf.log.20261012120000");
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(OB_CANCELED, compress("libpalf.log.20261012120000"));
  EXPECT_TRUE(exists("libpalf.log.20261012120000"));
  EXPECT_FALSE(exists("libpalf.log.20261012120000.zst"));
  compressor_.resume_after_config_update();
  EXPECT_EQ(OB_SUCCESS, compress("libpalf.log.20261012120000"));
}

TEST_F(LibPalfSyslogTest, mtime_orders_across_families_and_numeric_ties)
{
  ObSyslogFile file;
  ObSyslogCompareFunctor compare;
  ObSyslogPriorityArray array(compare);
  const char *names[] = {"observer.log.20261012120000500", "libpalf.log.20261012120000.10",
                         "libpalf.log.20261012120000.2"};
  const int64_t times[] = {300, 100, 100};
  for (int i = 0; i < 3; ++i) {
    snprintf(file.file_name_, sizeof(file.file_name_), "%s", names[i]);
    file.mtime_ = times[i];
    ASSERT_EQ(OB_SUCCESS, array.push(file));
  }
  const ObSyslogFile *oldest = nullptr;
  ASSERT_EQ(OB_SUCCESS, array.top(oldest));
  EXPECT_STREQ(names[2], oldest->file_name_);
  ASSERT_EQ(OB_SUCCESS, array.pop());
  ASSERT_EQ(OB_SUCCESS, array.top(oldest));
  EXPECT_STREQ(names[1], oldest->file_name_);
}

TEST_F(LibPalfSyslogTest, background_switch_keeps_zst_eligible_for_space_recycling)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  create("libpalf.log", "active");
  create("libpalf.log.20261012120000.2", std::string(65536, 'x'));
  create("observer.log.wf.20261012120000000", "wf");
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_COMPRESS_RESERVE_SIZE));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  compressor_.set_libpalf_compression_enabled(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 500 && !exists("libpalf.log.20261012120000.2.zst"); ++i) {
    usleep(20000);
  }
  ASSERT_TRUE(exists("libpalf.log.20261012120000.2.zst"));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_FALSE(compressor_.cycle_running_);
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("none"));
  compressor_.set_libpalf_compression_enabled(false);
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_DELETE_RESERVE_SIZE));
  compressor_.resume_after_config_update();
  for (int i = 0; i < 500 && exists("libpalf.log.20261012120000.2.zst"); ++i) {
    usleep(20000);
  }
  EXPECT_FALSE(exists("libpalf.log.20261012120000.2.zst"));
  EXPECT_TRUE(exists("libpalf.log"));
  EXPECT_TRUE(exists("observer.log.wf.20261012120000000"));
}

TEST_F(LibPalfSyslogTest, startup_cleans_temporary_outputs_when_all_policies_are_disabled)
{
  const int64_t previous_count = OB_LOGGER.get_max_file_index();
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(0));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  create("libpalf.log", "active");
  create("libpalf.log.20261012120000", "raw archive");
  create("libpalf.log.20261012110000.zst", "complete archive");
  create("libpalf.log.20261012100000.zst.tmp.11.22.33", "partial output");
  compressor_.resume_after_config_update();
  for (int i = 0; i < 500 && exists("libpalf.log.20261012100000.zst.tmp.11.22.33"); ++i) {
    usleep(20000);
  }
  EXPECT_FALSE(exists("libpalf.log.20261012100000.zst.tmp.11.22.33"));
  EXPECT_TRUE(exists("libpalf.log"));
  EXPECT_TRUE(exists("libpalf.log.20261012120000"));
  EXPECT_TRUE(exists("libpalf.log.20261012110000.zst"));
  compressor_.destroy();
  EXPECT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(previous_count));
}

}  // namespace common
}  // namespace oceanbase

namespace {
int run_legacy_crash_child(const char *source);
}

int main(int argc, char **argv)
{
  if (argc == 3 && 0 == strcmp(argv[1], "--legacy-crash-child")) {
    return run_legacy_crash_child(argv[2]);
  }
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_compressor.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  OB_LOG_COMPRESSOR.destroy();
  return ret;
}

namespace {
std::string close_fault_output;
bool enable_close_fault = false;
FILE *close_fault_cookie = nullptr;
int close_fault_hits = 0;
int close_fault_result = 0;
bool enable_stat_fault = false;
std::string stat_fault_source;
int stat_fault_hits = 0;
std::atomic<bool> count_scan_opens(false);
std::atomic<int> scan_open_count(0);
std::atomic<int64_t> scan_open_delay_us(0);
std::atomic<int64_t> previous_scan_start_us(0);
std::atomic<int64_t> minimum_scan_interval_us(INT64_MAX);
std::string scan_root;
std::atomic<DIR *> tracked_scan_dir(nullptr);
std::atomic<bool> fail_readdir_once(false);
std::atomic<bool> fail_temporary_unlink_once(false);
std::atomic<int> directory_fault_hits(0);
std::atomic<bool> fail_entry_stat(false);
std::string entry_stat_name;
std::atomic<int64_t> available_bytes_override(-1);
std::atomic<bool> hold_history_eof(false);
std::atomic<bool> history_eof_reached(false);
std::atomic<bool> release_history_eof(false);
std::atomic<bool> fail_output_stat(false);
std::string output_stat_name;
}  // namespace

extern "C" {
FILE *__real_fopen(const char *, const char *);
int __real_fclose(FILE *);
int __real_stat(const char *, struct stat *);
DIR *__real_opendir(const char *);
DIR *__wrap_opendir(const char *name)
{
  DIR *dir = __real_opendir(name);
  if (scan_root == name) {
    tracked_scan_dir.store(dir);
    if (count_scan_opens.load()) {
      const int64_t now = oceanbase::common::ObTimeUtility::current_monotonic_time();
      const int64_t previous = previous_scan_start_us.exchange(now);
      if (previous != 0) {
        minimum_scan_interval_us.store(std::min(minimum_scan_interval_us.load(), now - previous));
      }
      scan_open_count.fetch_add(1);
      if (scan_open_delay_us.load() > 0) {
        usleep(scan_open_delay_us.load());
      }
    }
  }
  return dir;
}
struct dirent *__real_readdir(DIR *);
struct dirent *__wrap_readdir(DIR *dir)
{
  if (dir == tracked_scan_dir.load() && fail_readdir_once.exchange(false)) {
    directory_fault_hits.fetch_add(1);
    errno = EIO;
    return nullptr;
  }
  struct dirent *entry = __real_readdir(dir);
  if (entry == nullptr && dir == tracked_scan_dir.load() && hold_history_eof.exchange(false)) {
    history_eof_reached.store(true);
    while (!release_history_eof.load()) {
      usleep(1000);
    }
  }
  return entry;
}
int __real_lstat(const char *, struct stat *);
int __wrap_lstat(const char *name, struct stat *st)
{
  if (fail_output_stat.load() && output_stat_name == name) {
    errno = EIO;
    return -1;
  }
  return __real_lstat(name, st);
}
int __real_unlinkat(int, const char *, int);
int __wrap_unlinkat(int fd, const char *name, int flags)
{
  if (strstr(name, ".zst.tmp.") != nullptr && fail_temporary_unlink_once.exchange(false)) {
    directory_fault_hits.fetch_add(1);
    errno = EACCES;
    return -1;
  }
  return __real_unlinkat(fd, name, flags);
}
int __real_fstatat(int, const char *, struct stat *, int);
int __wrap_fstatat(int fd, const char *name, struct stat *st, int flags)
{
  if (fail_entry_stat.load() && entry_stat_name == name) {
    errno = EIO;
    return -1;
  }
  return __real_fstatat(fd, name, st, flags);
}
int __real_statfs(const char *, struct statfs *);
int __wrap_statfs(const char *name, struct statfs *st)
{
  const int ret = __real_statfs(name, st);
  const int64_t bytes = available_bytes_override.load();
  if (ret == 0 && bytes >= 0 && scan_root == name) {
    st->f_bavail = bytes / st->f_bsize;
  }
  return ret;
}
FILE *__wrap_fopen(const char *name, const char *mode)
{
  if (enable_close_fault && mode[0] == 'w'
      && (close_fault_output == name
          || strncmp(name, (close_fault_output + ".tmp.").c_str(), close_fault_output.size() + 5) == 0)) {
    close_fault_cookie = __real_fopen(name, mode);
    return close_fault_cookie;
  }
  return __real_fopen(name, mode);
}
int __wrap_fclose(FILE *file)
{
  const bool selected = enable_close_fault && file == close_fault_cookie;
  int ret = __real_fclose(file);
  if (selected) {
    ++close_fault_hits;
    errno = ENOSPC;
    ret = close_fault_result = EOF;
    close_fault_cookie = nullptr;
  }
  return ret;
}
int __wrap_stat(const char *name, struct stat *st)
{
  if (enable_stat_fault && stat_fault_source == name) {
    ++stat_fault_hits;
    errno = EIO;
    return -1;
  }
  return __real_stat(name, st);
}

// glibc's optimized stat/lstat/fstatat inline wrappers call these entry points.
int __real___xstat(int, const char *, struct stat *);
int __wrap___xstat(int version, const char *name, struct stat *st)
{
  if (enable_stat_fault && stat_fault_source == name) {
    ++stat_fault_hits;
    errno = EIO;
    return -1;
  }
  return __real___xstat(version, name, st);
}
int __real___lxstat(int, const char *, struct stat *);
int __wrap___lxstat(int version, const char *name, struct stat *st)
{
  if (fail_output_stat.load() && output_stat_name == name) {
    errno = EIO;
    return -1;
  }
  return __real___lxstat(version, name, st);
}
int __real___fxstatat(int, int, const char *, struct stat *, int);
int __wrap___fxstatat(int version, int fd, const char *name, struct stat *st, int flags)
{
  if (fail_entry_stat.load() && entry_stat_name == name) {
    errno = EIO;
    return -1;
  }
  return __real___fxstatat(version, fd, name, st, flags);
}
}

namespace oceanbase {
namespace common {

class ForwardConfigReview : public LibPalfSyslogTest
{
public:
  void SetUp() override
  {
    LibPalfSyslogTest::SetUp();
    logger_.set_file_name(path("observer.log").c_str(), true, false, path("rootservice.log").c_str(),
                          path("election.log").c_str(), path("trace.log").c_str(), path("audit.log").c_str(),
                          path("alert/alert.log").c_str());
    ASSERT_EQ(OB_SUCCESS, logger_.set_log_compressor(&compressor_));
  }
  void seed_cpp(int number)
  {
    for (int i = 0; i < number; ++i) {
      char name[128] = {};
      snprintf(name, sizeof(name), "observer.log.202610121200%05d", i);
      create(name, "original CPP archive", 100 + i);
      names_.push_back(name);
    }
  }
  int remaining()
  {
    int count = 0;
    for (const std::string &name : names_) {
      if (exists(name.c_str())) {
        ++count;
      }
    }
    return count;
  }
  void apply_forward(int count, bool recycle, const char *algorithm)
  {
    ObSyslogConfig target;
    target.max_file_count_ = count;
    target.record_old_log_file_ = recycle;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor_type(algorithm, target.compress_func_));
    if (!compressor_.is_config_applied(logger_, target)) {
      ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
      ASSERT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, target));
      compressor_.resume_after_config_update();
    }
  }
  ObLogger logger_;
  std::vector<std::string> names_;
};

TEST_F(ForwardConfigReview, none_to_zstd_preserves_all_archives)
{
  seed_cpp(12);
  apply_forward(8, true, "zstd_1.3.8");
  printf("RECHECK none_to_zstd original_archives=12 remaining=%d expected=12\n", remaining());
  EXPECT_EQ(12, remaining());
}

TEST_F(ForwardConfigReview, disabling_history_scans_with_lower_count_preserves_archives)
{
  ASSERT_EQ(OB_SUCCESS, logger_.set_max_file_index(12));
  ASSERT_EQ(OB_SUCCESS, logger_.set_record_old_log_file(true));
  seed_cpp(12);
  apply_forward(8, false, "none");
  printf("RECHECK true_to_false original_archives=12 remaining=%d expected=12\n", remaining());
  EXPECT_EQ(12, remaining());
}

TEST_F(ForwardConfigReview, zstd_to_none_enforces_count_on_first_application)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  seed_cpp(12);
  ASSERT_EQ(OB_SUCCESS, logger_.set_max_file_index(8));
  ASSERT_EQ(OB_SUCCESS, logger_.set_record_old_log_file(true));
  ASSERT_EQ(12, remaining());
  apply_forward(8, true, "none");
  const int first = remaining();
  EXPECT_EQ(8, first);
  apply_forward(8, true, "none");
  EXPECT_EQ(8, remaining());
  printf("RECHECK zstd_to_none after_first=%d after_second=%d\n", first, remaining());
}

TEST_F(LibPalfSyslogTest, bad_entries_do_not_poison_scan_return)
{
  create("libpalf.log.20261012120000", "valid archive");
  ObSyslogFile candidates[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {};
  int64_t total = 0;
  ASSERT_EQ(0, symlink("loop", path("loop").c_str()));
  const int loop_ret = compressor_.scan_log_files_(true, true, true, candidates, counts, total);
  ASSERT_EQ(OB_SUCCESS, loop_ret);
  ASSERT_EQ(1, counts[SYSLOG_LIBPALF]);
  ASSERT_EQ(0, unlink(path("loop").c_str()));
  const std::string long_name(255, 'a');
  create(long_name.c_str(), "not a managed log");
  const int long_ret = compressor_.scan_log_files_(true, true, true, candidates, counts, total);
  ASSERT_EQ(OB_SUCCESS, long_ret);
  EXPECT_EQ(30, total);
  ASSERT_EQ(1, counts[SYSLOG_LIBPALF]);
  printf("RECHECK scan loop_ret=%d long_filename_ret=%d valid_raw_candidates=%ld\n", loop_ret, long_ret,
         counts[SYSLOG_LIBPALF]);
}

TEST_F(LibPalfSyslogTest, bad_entry_does_not_block_background_recycling)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  create("libpalf.log", "active");
  create("libpalf.log.20261012120000", std::string(65536, 'x'));
  ASSERT_EQ(0, symlink("loop", path("loop").c_str()));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("none"));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_DELETE_RESERVE_SIZE));
  compressor_.resume_after_config_update();
  usleep(300000);
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_FALSE(exists("libpalf.log.20261012120000"));
  ASSERT_EQ(0, unlink(path("loop").c_str()));
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && exists("libpalf.log.20261012120000"); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_FALSE(exists("libpalf.log.20261012120000"));
}

TEST_F(LibPalfSyslogTest, retains_latest_nonempty_archives_despite_old_conflicts)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  create("libpalf.log.20261012100000", "", 100);
  create("libpalf.log.20261012100100", "", 101);
  create("libpalf.log.20261012110000", "conflicting raw", 110);
  create("libpalf.log.20261012110000.zst", "preexisting output", 110);
  create("libpalf.log.20261012110100", "conflicting raw", 111);
  create("libpalf.log.20261012110100.zst", "preexisting output", 111);
  create("libpalf.log.20261012120000", "oldest useful", 200);
  create("libpalf.log.20261012120100", "recent useful one", 201);
  create("libpalf.log.20261012120200", "recent useful two", 202);
  ASSERT_EQ(OB_SUCCESS, compressor_.set_min_uncompressed_count(2));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_COMPRESS_RESERVE_SIZE));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  compressor_.set_libpalf_compression_enabled(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && !exists("libpalf.log.20261012120000.zst"); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_TRUE(exists("libpalf.log.20261012120000.zst"));
  EXPECT_TRUE(exists("libpalf.log.20261012120100"));
  EXPECT_TRUE(exists("libpalf.log.20261012120200"));
  compressor_.resume_after_config_update();
  usleep(100000);
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_TRUE(exists("libpalf.log.20261012120100"));
  EXPECT_TRUE(exists("libpalf.log.20261012120200"));
}

TEST_F(LibPalfSyslogTest, cycle_scans_once_without_any_compression)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 10000000;
  create("libpalf.log", "active");
  create("libpalf.log.20261012120000", "original archive");
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(8LL << 30));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("none"));
  scan_root = directory_;
  scan_open_count.store(0);
  count_scan_opens.store(true);
  compressor_.resume_after_config_update();
  for (int retry = 0; retry < 300 && scan_open_count.load() < 1; ++retry) {
    usleep(10000);
  }
  // Let the entire first cycle finish before pausing; pausing at opendir could hide a second scan.
  usleep(100000);
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  count_scan_opens.store(false);
  printf("RECHECK no_compression main_directory_scans=%d\n", scan_open_count.load());
  EXPECT_EQ(1, scan_open_count.load());
}

TEST_F(LibPalfSyslogTest, recheck_legacy_fclose_failure_must_not_remove_source)
{
  const char *name = "observer.log.20261012120000000";
  create(name, std::string(65536, 'x'));
  close_fault_output = path(name) + ".zst";
  close_fault_hits = 0;
  close_fault_result = 0;
  enable_close_fault = true;
  const int ret = compress(name);
  enable_close_fault = false;
  ASSERT_GT(close_fault_hits, 0);
  ASSERT_EQ(EOF, close_fault_result);
  const size_t output_size = contents(close_fault_output).size();
  printf("RECHECK legacy_fclose fclose_ret=%d compressor_ret=%d source_exists=%d output_bytes=%zu\n",
         close_fault_result, ret, exists(name) ? 1 : 0, output_size);
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_TRUE(exists(name));
  EXPECT_FALSE(exists("observer.log.20261012120000000.zst"));
}

TEST_F(LibPalfSyslogTest, legacy_output_conflict_preserves_both_files)
{
  const char *name = "observer.log.20261012120000000";
  create(name, "source");
  create("observer.log.20261012120000000.zst", "previous complete output");
  EXPECT_EQ(OB_FILE_ALREADY_EXIST, compress(name));
  EXPECT_EQ("source", contents(path(name)));
  EXPECT_EQ("previous complete output", contents(path("observer.log.20261012120000000.zst")));
}

TEST_F(LibPalfSyslogTest, none_clears_both_compressor_pointers_before_init)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  ASSERT_NE(nullptr, compressor_.compressor_);
  ASSERT_NE(nullptr, compressor_.next_compressor_);
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("none"));
  EXPECT_EQ(nullptr, compressor_.compressor_);
  EXPECT_EQ(nullptr, compressor_.next_compressor_);
}

TEST_F(LibPalfSyslogTest, legacy_stat_failure_preserves_source)
{
  const char *name = "observer.log.20261012120000000";
  create(name, "content despite failed stat");
  stat_fault_source = path(name);
  stat_fault_hits = 0;
  enable_stat_fault = true;
  const int ret = compress(name);
  enable_stat_fault = false;
  ASSERT_GT(stat_fault_hits, 0);
  printf("RECHECK legacy_stat stat_failures=%d compressor_ret=%d source_exists=%d\n", stat_fault_hits, ret,
         exists(name) ? 1 : 0);
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_TRUE(exists(name));
  EXPECT_FALSE(exists("observer.log.20261012120000000.zst"));
}

TEST(LibPalfSyslogRecheck, compressed_suffix_preserves_no_sequence_tie)
{
  ObSyslogCompareFunctor compare;
  ObSyslogFile first, second;
  first.mtime_ = second.mtime_ = 100;
  snprintf(first.file_name_, sizeof(first.file_name_), "libpalf.log.20261012120000");
  snprintf(second.file_name_, sizeof(second.file_name_), "libpalf.log.20261012120000.1");
  const bool raw_first_is_newer = compare(first, second);
  snprintf(first.file_name_, sizeof(first.file_name_), "libpalf.log.20261012120000.zst");
  snprintf(second.file_name_, sizeof(second.file_name_), "libpalf.log.20261012120000.1.zst");
  const bool compressed_first_is_newer = compare(first, second);
  printf("RECHECK tie raw_first_is_newer=%d compressed_first_is_newer=%d\n", raw_first_is_newer,
         compressed_first_is_newer);
  EXPECT_FALSE(raw_first_is_newer);
  EXPECT_FALSE(compressed_first_is_newer);
}

TEST_F(ForwardConfigReview, failed_history_scan_is_retried_for_identical_target)
{
  seed_cpp(12);
  ObSyslogConfig target;
  target.max_file_count_ = 8;
  target.record_old_log_file_ = true;
  target.compress_func_ = ZSTD_1_3_8_COMPRESSOR;
  scan_root = directory_;
  directory_fault_hits.store(0);
  fail_readdir_once.store(true);
  EXPECT_EQ(OB_IO_ERROR, compressor_.apply_config(logger_, target));
  EXPECT_EQ(1, directory_fault_hits.load());
  EXPECT_FALSE(compressor_.is_config_applied(logger_, target));
  EXPECT_EQ(12, remaining());
  EXPECT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, target));
  EXPECT_TRUE(compressor_.is_config_applied(logger_, target));
  EXPECT_EQ(12, remaining());
}

static void observe_syslog_config(ObLogCompressor *compressor, ObLogger *logger, const ObSyslogConfig *none,
                                  const ObSyslogConfig *zstd, std::atomic<bool> *stop, std::atomic<int> *observed,
                                  std::atomic<int> *inconsistent)
{
  while (!stop->load()) {
    if (0 == pthread_mutex_lock(&logger->file_index_mutex_)) {
      const ObSyslogConfig current = compressor->get_config(*logger);
      if (!(current == *none) && !(current == *zstd)) {
        inconsistent->fetch_add(1);
      }
      observed->fetch_add(1);
      pthread_mutex_unlock(&logger->file_index_mutex_);
    }
  }
}

TEST_F(ForwardConfigReview, rotation_lock_observes_consistent_count_and_algorithm)
{
  ObSyslogConfig none;
  ObSyslogConfig zstd;
  zstd.max_file_count_ = 8;
  zstd.compress_func_ = ZSTD_1_3_8_COMPRESSOR;
  ASSERT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, none));
  std::atomic<bool> stop(false);
  std::atomic<int> observed(0);
  std::atomic<int> inconsistent(0);
  std::thread reader(observe_syslog_config, &compressor_, &logger_, &none, &zstd, &stop, &observed, &inconsistent);
  while (observed.load() == 0) {
    std::this_thread::yield();
  }
  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, zstd));
    EXPECT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, none));
  }
  stop.store(true);
  reader.join();
  EXPECT_GT(observed.load(), 0);
  EXPECT_EQ(0, inconsistent.load());
}

int compare_log_filename_by_date_suffix(const void *, const void *);
TEST(LibPalfSyslogRecheck, legacy_count_sort_uses_timestamp_before_zst_suffix)
{
  const char *older = "observer.log.20261012120000000.zst";
  const char *newer = "election.log.20261012120000001";
  EXPECT_LT(compare_log_filename_by_date_suffix(older, newer), 0);
  EXPECT_GT(compare_log_filename_by_date_suffix(newer, older), 0);
  EXPECT_EQ(0, compare_log_filename_by_date_suffix(older, "observer.log.20261012120000000"));
}

}  // namespace common
}  // namespace oceanbase

namespace oceanbase {
namespace common {
TEST_F(LibPalfSyslogTest, directory_read_error_marks_incomplete_and_next_scan_recovers)
{
  create("libpalf.log.20261012120000", "data");
  scan_root = directory_;
  directory_fault_hits.store(0);
  fail_readdir_once.store(true);
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t bytes = 0;
  ObSyslogScanStatus status;
  EXPECT_EQ(OB_SUCCESS, compressor_.scan_log_files_(true, true, true, files, counts, bytes, &status));
  EXPECT_EQ(1, directory_fault_hits.load());
  EXPECT_FALSE(status.size_complete_);
  EXPECT_EQ(OB_SUCCESS, compressor_.scan_log_files_(true, true, true, files, counts, bytes, &status));
  EXPECT_TRUE(status.size_complete_);
  EXPECT_EQ(4, bytes);
}

TEST_F(LibPalfSyslogTest, startup_retries_temporary_cleanup_when_all_policies_disabled)
{
  const int64_t old_count = OB_LOGGER.get_max_file_index();
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(0));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  const char *temporary = "libpalf.log.20261012120000.zst.tmp.1.2.3";
  create(temporary, "unfinished");
  create("libpalf.log.20261012120000", "must remain");
  directory_fault_hits.store(0);
  fail_temporary_unlink_once.store(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && exists(temporary); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(1, directory_fault_hits.load());
  EXPECT_FALSE(exists(temporary));
  EXPECT_TRUE(exists("libpalf.log.20261012120000"));
  compressor_.destroy();
  EXPECT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(old_count));
}

TEST_F(LibPalfSyslogTest, incomplete_size_still_allows_physical_emergency_recycling)
{
  scan_root = directory_;
  create("unknown", "unknown bytes");
  entry_stat_name = "unknown";
  fail_entry_stat.store(true);
  available_bytes_override.store(16LL << 30);
  EXPECT_EQ(16LL << 30, compressor_.get_effective_remaining_size_(1LL << 30, 2LL << 30, false));
  const int64_t old_count = OB_LOGGER.get_max_file_index();
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(1));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  create("libpalf.log.20261012120000", "archive");
  available_bytes_override.store(1LL << 30);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && exists("libpalf.log.20261012120000"); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_FALSE(exists("libpalf.log.20261012120000"));
  EXPECT_TRUE(exists("unknown"));
  compressor_.destroy();
  fail_entry_stat.store(false);
  available_bytes_override.store(-1);
  EXPECT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(old_count));
}

TEST_F(ForwardConfigReview, unchanged_normalized_config_does_not_cancel_running_compression)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  create("libpalf.log.20261012120000", std::string(32 * 1024 * 1024, 'x'));
  ObSyslogConfig config;
  config.max_file_count_ = ObLogger::MAX_LOG_FILE_COUNT + 1;
  config.max_disk_size_ = OB_SYSLOG_COMPRESS_RESERVE_SIZE;
  config.compress_func_ = ZSTD_1_3_8_COMPRESSOR;
  config.enable_libpalf_ = true;
  ASSERT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, config));
  compressor_.resume_after_config_update();
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(compressor_.is_config_applied(logger_, config));
    usleep(120000);
  }
  for (int i = 0; i < 300 && !exists("libpalf.log.20261012120000.zst"); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_TRUE(exists("libpalf.log.20261012120000.zst"));
  EXPECT_EQ(1, compressor_.temporary_file_sequence_);
}
}  // namespace common
}  // namespace oceanbase

namespace {
enum class ReviewFault
{
  NONE,
  READ,
  WRITE,
  FSYNC_FILE,
  CLOSE_FILE,
  LINK,
  FSYNC_DIRECTORY,
  UNLINK_SOURCE,
  RETRY_IO,
  FSYNC_SOURCE,
  UNLINK_OUTPUT,
  FSYNC_DIRECTORY_AFTER_UNLINK
};
std::atomic<ReviewFault> review_fault(ReviewFault::NONE);
std::string review_source;
std::string review_directory;
std::atomic<int> review_fault_hits(0);
std::atomic<int> review_directory_sync_calls(0);
std::atomic<bool> count_cleanup_reads(false);
std::atomic<int> cleanup_read_count(0);
std::string cleanup_read_source;
int review_read_retries = 0;
int review_write_retries = 0;
std::atomic<bool> cancel_at_output_write(false);
std::atomic<bool> output_write_reached(false);
std::atomic<bool> fail_output_cleanup_once(false);
oceanbase::common::ObLogCompressor *cancel_test_compressor = nullptr;
bool review_fd_is(int fd, bool output, bool directory = false)
{
  char fd_name[64] = {};
  char target[1024] = {};
  snprintf(fd_name, sizeof(fd_name), "/proc/self/fd/%d", fd);
  const ssize_t count = readlink(fd_name, target, sizeof(target) - 1);
  if (count < 0) {
    return false;
  }
  target[count] = '\0';
  if (directory) {
    return review_directory == target;
  }
  return output ? strncmp(target, (review_source + ".zst.tmp.").c_str(), review_source.size() + 9) == 0
                : review_source == target;
}
bool fail_legacy_write = false;
bool crash_after_legacy_write = false;
int legacy_write_fault_hits = 0;
int run_legacy_crash_child(const char *source)
{
  using namespace oceanbase::common;
  ObLogCompressor compressor;
  compressor.stopped_ = false;
  if (OB_SUCCESS != ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor.compressor_)) {
    return 79;
  }
  char *buffers = static_cast<char *>(
      ob_malloc(OB_SYSLOG_COMPRESS_BLOCK_SIZE + OB_SYSLOG_COMPRESS_BUFFER_SIZE, ObModIds::OB_COMPRESSOR));
  if (buffers == nullptr) {
    return 79;
  }
  review_source = source;
  crash_after_legacy_write = true;
  compressor.compress_single_file_(source, buffers, buffers + OB_SYSLOG_COMPRESS_BLOCK_SIZE);
  ob_free(buffers);
  return 78;  // The fwrite fault should exit with 77 before compression completes.
}
int review_fail()
{
  ++review_fault_hits;
  errno = EIO;
  return -1;
}
}  // namespace

extern "C" {
size_t __real_fwrite(const void *, size_t, size_t, FILE *);
size_t __wrap_fwrite(const void *data, size_t size, size_t count, FILE *file)
{
  char link_name[64] = {};
  char target[1024] = {};
  bool selected = false;
  if (fail_legacy_write || crash_after_legacy_write) {
    snprintf(link_name, sizeof(link_name), "/proc/self/fd/%d", fileno(file));
    const ssize_t length = readlink(link_name, target, sizeof(target) - 1);
    if (length >= 0) {
      target[length] = '\0';
      selected = review_source + ".zst" == target || review_fd_is(fileno(file), true);
    }
  }
  if (selected && fail_legacy_write) {
    ++legacy_write_fault_hits;
    const size_t written = __real_fwrite(data, size, count / 2, file);
    errno = ENOSPC;
    return written;
  }
  const size_t written = __real_fwrite(data, size, count, file);
  if (selected && crash_after_legacy_write) {
    fflush(file);
    _exit(77);
  }
  return written;
}
ssize_t __real_read(int, void *, size_t);
ssize_t __real_write(int, const void *, size_t);
int __real_fsync(int);
int __real_close(int);
int __real_link(const char *, const char *);
int __real_unlink(const char *);

ssize_t __wrap_read(int fd, void *data, size_t size)
{
  if (count_cleanup_reads.load()) {
    char fd_name[64] = {};
    char target[1024] = {};
    snprintf(fd_name, sizeof(fd_name), "/proc/self/fd/%d", fd);
    const ssize_t length = readlink(fd_name, target, sizeof(target) - 1);
    if (length >= 0 && (cleanup_read_source == target || cleanup_read_source + ".zst" == target)) {
      ++cleanup_read_count;
    }
  }
  if (review_fault != ReviewFault::NONE && review_fd_is(fd, false)) {
    if (review_fault == ReviewFault::READ) {
      return review_fail();
    }
    if (review_fault == ReviewFault::RETRY_IO && review_read_retries++ == 0) {
      ++review_fault_hits;
      errno = EINTR;
      return -1;
    }
  }
  return __real_read(fd, data, size);
}
ssize_t __wrap_write(int fd, const void *data, size_t size)
{
  if (cancel_at_output_write.load() && review_fd_is(fd, true)) {
    output_write_reached.store(true);
    while (!cancel_test_compressor->is_cancel_requested_()) {
      usleep(1000);
    }
    cancel_at_output_write.store(false);
  }
  if (review_fault != ReviewFault::NONE && review_fd_is(fd, true)) {
    if (review_fault == ReviewFault::WRITE) {
      return review_fail();
    }
    if (review_fault == ReviewFault::RETRY_IO) {
      ++review_fault_hits;
      if (review_write_retries++ == 0) {
        errno = EINTR;
        return -1;
      }
      return __real_write(fd, data, std::max<size_t>(1, size / 2));
    }
  }
  return __real_write(fd, data, size);
}
int __wrap_fsync(int fd)
{
  if ((review_fault == ReviewFault::FSYNC_SOURCE && review_fd_is(fd, false))
      || (review_fault == ReviewFault::FSYNC_FILE && review_fd_is(fd, true))
      || (review_fault == ReviewFault::FSYNC_DIRECTORY && review_fd_is(fd, false, true))) {
    return review_fail();
  }
  if (review_fault == ReviewFault::FSYNC_DIRECTORY_AFTER_UNLINK && review_fd_is(fd, false, true)
      && ++review_directory_sync_calls == 2) {
    return review_fail();
  }
  return __real_fsync(fd);
}
int __wrap_close(int fd)
{
  if (review_fault == ReviewFault::CLOSE_FILE && review_fd_is(fd, true)) {
    __real_close(fd);
    return review_fail();
  }
  return __real_close(fd);
}
int __wrap_link(const char *source, const char *target)
{
  if (review_fault == ReviewFault::LINK && review_source + ".zst" == target) {
    return review_fail();
  }
  return __real_link(source, target);
}
int __wrap_unlink(const char *name)
{
  if (strstr(name, ".zst.tmp.") != nullptr && fail_output_cleanup_once.exchange(false)) {
    ++review_fault_hits;
    errno = EACCES;
    return -1;
  }
  if (review_fault == ReviewFault::UNLINK_SOURCE && review_source == name) {
    return review_fail();
  }
  if (review_fault == ReviewFault::UNLINK_OUTPUT && review_source + ".zst" == name) {
    return review_fail();
  }
  return __real_unlink(name);
}
}

namespace oceanbase {
namespace common {

TEST_F(LibPalfSyslogTest, review_io_failures_preserve_complete_copies)
{
  const ReviewFault faults[] = {ReviewFault::READ,         ReviewFault::WRITE, ReviewFault::FSYNC_FILE,
                                ReviewFault::CLOSE_FILE,   ReviewFault::LINK,  ReviewFault::FSYNC_DIRECTORY,
                                ReviewFault::UNLINK_SOURCE};
  const char *labels[] = {"read", "write", "fsync_file", "close_file", "link", "fsync_directory", "unlink_source"};
  const std::string original(65536, 'd');
  for (size_t i = 0; i < sizeof(faults) / sizeof(faults[0]); ++i) {
    const std::string name = "libpalf.log.20261012120000." + std::to_string(i + 1);
    create(name.c_str(), original);
    review_source = path(name.c_str());
    review_directory = directory_;
    review_fault_hits = 0;
    review_fault = faults[i];
    const int ret = compress(name.c_str());
    review_fault = ReviewFault::NONE;
    ASSERT_GT(review_fault_hits.load(), 0) << labels[i];
    EXPECT_NE(OB_SUCCESS, ret) << labels[i];
    EXPECT_EQ(original, contents(review_source)) << labels[i];
    const std::string output = name + ".zst";
    const bool should_publish = faults[i] == ReviewFault::FSYNC_DIRECTORY || faults[i] == ReviewFault::UNLINK_SOURCE;
    EXPECT_EQ(should_publish, exists(output.c_str())) << labels[i];
    if (should_publish) {
      const std::string command = "zstd -q -d -f " + path(output.c_str()) + " -o " + path("decoded");
      ASSERT_EQ(0, system(command.c_str()));
      EXPECT_EQ(original, contents(path("decoded"))) << labels[i];
    }
    DIR *dir = opendir(directory_);
    ASSERT_NE(nullptr, dir);
    struct dirent *entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
      EXPECT_EQ(nullptr, strstr(entry->d_name, ".zst.tmp.")) << entry->d_name;
    }
    closedir(dir);
    printf("REVIEW injected=%s return=%d source_intact=1 final_present=%d temporary_remaining=0\n", labels[i], ret,
           exists(output.c_str()) ? 1 : 0);
  }
}

TEST_F(LibPalfSyslogTest, review_eintr_and_short_writes_keep_complete_frames)
{
  const char *name = "libpalf.log.20261012120000.10";
  const std::string original(2 * OB_SYSLOG_COMPRESS_BLOCK_SIZE + 77, 'q');
  create(name, original);
  review_source = path(name);
  review_directory = directory_;
  review_fault_hits = 0;
  review_read_retries = 0;
  review_write_retries = 0;
  review_fault = ReviewFault::RETRY_IO;
  const int ret = compress(name);
  review_fault = ReviewFault::NONE;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(review_fault_hits.load(), 2);
  const std::string command = "zstd -q -d -f " + review_source + ".zst -o " + path("decoded");
  ASSERT_EQ(0, system(command.c_str()));
  EXPECT_EQ(original, contents(path("decoded")));
  printf("REVIEW EINTR_and_short_writes_hits=%d roundtrip_equal=1\n", review_fault_hits.load());
}

TEST_F(LibPalfSyslogTest, failed_publication_sync_still_rescans_and_recycles)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 10000000;
  const char *name = "libpalf.log.20261012120000";
  const std::string original(65536, 'd');
  create(name, original);
  review_source = path(name);
  review_directory = directory_;
  review_fault_hits.store(0);
  review_fault.store(ReviewFault::FSYNC_DIRECTORY);
  scan_root = directory_;
  scan_open_count.store(0);
  count_scan_opens.store(true);
  // Before compression, the budget has 2 GiB + 1 byte free. A complete .zst published
  // before the directory-fsync failure puts it below 2 GiB, despite compressed_count == 0.
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_DELETE_RESERVE_SIZE + original.size() + 1));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  compressor_.set_libpalf_compression_enabled(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && exists(name); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  review_fault.store(ReviewFault::NONE);
  count_scan_opens.store(false);
  EXPECT_EQ(1, review_fault_hits.load());
  EXPECT_EQ(2, scan_open_count.load());
  EXPECT_FALSE(exists(name));
  ASSERT_TRUE(exists("libpalf.log.20261012120000.zst"));
  const std::string command = "zstd -q -d -f " + review_source + ".zst -o " + path("decoded");
  ASSERT_EQ(0, system(command.c_str()));
  EXPECT_EQ(original, contents(path("decoded")));
}

TEST_F(ForwardConfigReview, failed_history_preparation_keeps_applied_policy_and_queues)
{
  seed_cpp(12);
  apply_forward(8, true, "zstd_1.3.8");
  const ObSyslogConfig previous = compressor_.get_config(logger_);
  const std::deque<std::string> previous_files = logger_.file_list_;
  const std::deque<std::string> previous_wf_files = logger_.wf_file_list_;
  ObSyslogConfig target = previous;
  target.max_file_count_ = 4;
  target.compress_func_ = NONE_COMPRESSOR;
  scan_root = directory_;
  fail_readdir_once.store(true);
  EXPECT_EQ(OB_IO_ERROR, compressor_.apply_config(logger_, target));
  EXPECT_TRUE(previous == compressor_.get_config(logger_));
  EXPECT_TRUE(compressor_.is_config_applied(logger_, previous));
  EXPECT_EQ(previous_files, logger_.file_list_);
  EXPECT_EQ(previous_wf_files, logger_.wf_file_list_);
  EXPECT_EQ(12, remaining());
  EXPECT_EQ(OB_SUCCESS, compressor_.apply_config(logger_, target));
  EXPECT_EQ(4, remaining());
}

static void scan_syslog_history(ObLogger *logger, int *ret)
{
  *ret = logger->record_old_log_file();
}

TEST_F(ForwardConfigReview, history_snapshot_cannot_erase_concurrent_rotation)
{
  seed_cpp(2);
  apply_forward(8, true, "none");
  scan_root = directory_;
  history_eof_reached.store(false);
  release_history_eof.store(false);
  hold_history_eof.store(true);
  int history_ret = OB_ERR_UNEXPECTED;
  std::thread scanner(scan_syslog_history, &logger_, &history_ret);
  for (int retry = 0; retry < 300 && !history_eof_reached.load(); ++retry) {
    usleep(10000);
  }
  EXPECT_TRUE(history_eof_reached.load());
  struct stat before = {}, during = {};
  EXPECT_EQ(0, stat(path("observer.log").c_str(), &before));
  logger_.max_file_size_ = 1;
  logger_.log_file_[FD_SVR_FILE].file_size_ = 1;
  logger_.rotate_log(1, false, logger_.log_file_[FD_SVR_FILE], FD_SVR_FILE);
  EXPECT_EQ(0, stat(path("observer.log").c_str(), &during));
  EXPECT_EQ(before.st_ino, during.st_ino);
  release_history_eof.store(true);
  scanner.join();
  hold_history_eof.store(false);
  EXPECT_EQ(OB_SUCCESS, history_ret);
  logger_.log_file_[FD_SVR_FILE].file_size_ = 1;
  logger_.rotate_log(1, false, logger_.log_file_[FD_SVR_FILE], FD_SVR_FILE);
  EXPECT_EQ(3, logger_.file_list_.size());
  for (const std::string &file : logger_.file_list_) {
    EXPECT_EQ(0, access(file.c_str(), F_OK));
  }
}

TEST_F(LibPalfSyslogTest, accounting_scan_reports_unremoved_temporary_output)
{
  const char *temporary = "libpalf.log.20261012120000.zst.tmp.1.2.3";
  create(temporary, "incomplete output");
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT], bytes = 0;
  ObSyslogScanStatus status;
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(false, false, false, files, counts, bytes, &status));
  EXPECT_TRUE(status.cleanup_pending_);
  EXPECT_TRUE(status.size_complete_);
  EXPECT_TRUE(exists(temporary));
  EXPECT_EQ(17, bytes);
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(false, false, true, files, counts, bytes, &status));
  EXPECT_FALSE(status.cleanup_pending_);
  EXPECT_FALSE(exists(temporary));
}

TEST_F(LibPalfSyslogTest, output_lookup_failure_does_not_invalidate_accounted_size)
{
  create("observer.log.20261012120000000", "known bytes");
  output_stat_name = path("observer.log.20261012120000000.zst");
  fail_output_stat.store(true);
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT], bytes = 0;
  ObSyslogScanStatus status;
  const int ret = compressor_.scan_log_files_(false, false, true, files, counts, bytes, &status);
  fail_output_stat.store(false);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(11, bytes);
  EXPECT_TRUE(status.size_complete_);
  EXPECT_FALSE(status.cleanup_pending_);
  EXPECT_EQ(0, status.eligible_count_[SYSLOG_OBSERVER]);
}

TEST_F(LibPalfSyslogTest, disabled_policies_do_not_rescan_unrelated_metadata_failure)
{
  const int64_t old_count = OB_LOGGER.get_max_file_index();
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(0));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  create("unknown", "unknown bytes");
  scan_root = directory_;
  entry_stat_name = "unknown";
  fail_entry_stat.store(true);
  scan_open_count.store(0);
  count_scan_opens.store(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && scan_open_count.load() == 0; ++i) {
    usleep(10000);
  }
  usleep(150000);
  EXPECT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(1, scan_open_count.load());
  count_scan_opens.store(false);
  fail_entry_stat.store(false);
  compressor_.destroy();
  EXPECT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(old_count));
}

TEST_F(LibPalfSyslogTest, cancellation_cleanup_failure_is_retried_after_policies_disabled)
{
  const int64_t old_count = OB_LOGGER.get_max_file_index();
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(0));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  const char *name = "libpalf.log.20261012120000";
  create(name, std::string(65536, 'x'));
  review_source = path(name);
  cancel_test_compressor = &compressor_;
  output_write_reached.store(false);
  cancel_at_output_write.store(true);
  fail_output_cleanup_once.store(true);
  review_fault_hits.store(0);
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_COMPRESS_RESERVE_SIZE));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  compressor_.set_libpalf_compression_enabled(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && !output_write_reached.load(); ++i) {
    usleep(10000);
  }
  EXPECT_TRUE(output_write_reached.load());
  EXPECT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(1, review_fault_hits.load());
  EXPECT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(0));
  EXPECT_EQ(OB_SUCCESS, compressor_.set_compress_func("none"));
  compressor_.set_libpalf_compression_enabled(false);
  compressor_.resume_after_config_update();
  usleep(150000);
  EXPECT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(0, get_file_count_by_regex(directory_, ".*\\.zst\\.tmp\\..*"));
  EXPECT_TRUE(exists(name));
  EXPECT_FALSE(exists("libpalf.log.20261012120000.zst"));
  compressor_.destroy();
  cancel_at_output_write.store(false);
  fail_output_cleanup_once.store(false);
  cancel_test_compressor = nullptr;
  EXPECT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(old_count));
}

TEST_F(LibPalfSyslogTest, legacy_compression_preserves_nanoseconds_and_recycle_order)
{
  const char *name = "observer.log.20261012120000000";
  create("libpalf.log.20261012120000", "older", 100);
  create(name, "newer", 100);
  const struct timespec timestamps[] = {{100, 987654321}, {100, 987654321}};
  ASSERT_EQ(0, utimensat(AT_FDCWD, path(name).c_str(), timestamps, 0));
  ASSERT_EQ(OB_SUCCESS, compress(name));
  struct stat st = {};
  ASSERT_EQ(0, stat((path(name) + ".zst").c_str(), &st));
  EXPECT_EQ(100, st.st_mtim.tv_sec);
  EXPECT_EQ(987654321, st.st_mtim.tv_nsec);
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT], bytes = 0;
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(true, false, true, files, counts, bytes));
  const ObSyslogFile *oldest = nullptr;
  ASSERT_EQ(OB_SUCCESS, compressor_.oldest_files_.top(oldest));
  ASSERT_NE(nullptr, oldest);
  EXPECT_EQ(path("libpalf.log.20261012120000"), oldest->file_name_);
}

TEST_F(LibPalfSyslogTest, missing_compressor_preserves_legacy_source)
{
  create("observer.log.20261012120000000", "source must remain");
  compressor_.compressor_ = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, compress("observer.log.20261012120000000"));
  EXPECT_TRUE(exists("observer.log.20261012120000000"));
  EXPECT_FALSE(exists("observer.log.20261012120000000.zst"));
}

TEST_F(LibPalfSyslogTest, legacy_short_write_reports_io_error_and_preserves_source)
{
  const char *name = "observer.log.20261012120000000";
  const std::string original(65536, 'w');
  create(name, original);
  review_source = path(name);
  legacy_write_fault_hits = 0;
  fail_legacy_write = true;
  const int ret = compress(name);
  fail_legacy_write = false;
  ASSERT_GT(legacy_write_fault_hits, 0);
  EXPECT_EQ(OB_IO_ERROR, ret);
  EXPECT_TRUE(original == contents(path(name)));
  EXPECT_FALSE(exists("observer.log.20261012120000000.zst"));
}

TEST_F(LibPalfSyslogTest, legacy_crash_does_not_publish_partial_output)
{
  const char *name = "observer.log.20261012120000000";
  const std::string original(2 * OB_SYSLOG_COMPRESS_BLOCK_SIZE + 117, 'c');
  create(name, original);
  review_source = path(name);
  const std::string source = path(name);
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    // Start a fresh process; the test runner already has background threads.
    execl("/proc/self/exe", "test_ob_log_compressor", "--legacy-crash-child", source.c_str(),
          static_cast<char *>(nullptr));
    _exit(79);
  }
  int status = 0;
  pid_t waited = 0;
  for (int i = 0; i < 1000 && waited == 0; ++i) {
    waited = waitpid(child, &status, WNOHANG);
    usleep(10000);
  }
  if (waited == 0) {
    kill(child, SIGKILL);
    waitpid(child, &status, 0);
  }
  ASSERT_EQ(child, waited);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(77, WEXITSTATUS(status));
  EXPECT_TRUE(original == contents(path(name)));
  EXPECT_FALSE(exists("observer.log.20261012120000000.zst"));
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT], bytes = 0;
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(false, false, true, files, counts, bytes));
  EXPECT_EQ(original.size(), bytes);  // The abandoned temporary output was removed.
  ASSERT_EQ(OB_SUCCESS, compress(name));
  ASSERT_EQ(0, system(("zstd -q -d -f " + path(name) + ".zst -o " + path("decoded")).c_str()));
  EXPECT_TRUE(original == contents(path("decoded")));
}

TEST_F(LibPalfSyslogTest, legacy_publication_failures_preserve_source)
{
  const ReviewFault faults[] = {ReviewFault::FSYNC_FILE, ReviewFault::LINK, ReviewFault::FSYNC_DIRECTORY};
  const char *name = "observer.log.20261012120000000";
  const std::string original(65536, 'p');
  for (ReviewFault fault : faults) {
    create(name, original);
    review_source = path(name);
    review_directory = directory_;
    review_fault_hits = 0;
    review_fault = fault;
    const int ret = compress(name);
    review_fault = ReviewFault::NONE;
    EXPECT_GT(review_fault_hits.load(), 0);
    EXPECT_EQ(OB_IO_ERROR, ret);
    EXPECT_TRUE(original == contents(path(name)));
    EXPECT_EQ(fault == ReviewFault::FSYNC_DIRECTORY, exists("observer.log.20261012120000000.zst"));
    unlink((path(name) + ".zst").c_str());
  }
}

TEST_F(LibPalfSyslogTest, background_recompresses_empty_truncated_complete_and_unrelated_outputs)
{
  const std::string original(2 * OB_SYSLOG_COMPRESS_BLOCK_SIZE + 117, 'r');
  std::vector<std::string> names;
  const ObCompressorType algorithms[] = {ZSTD_COMPRESSOR, ZSTD_1_3_8_COMPRESSOR};
  for (int algorithm = 0; algorithm < 2; ++algorithm) {
    ASSERT_EQ(OB_SUCCESS,
              ObCompressorPool::get_instance().get_compressor(algorithms[algorithm], compressor_.compressor_));
    int64_t compressed_size = 0;
    ASSERT_EQ(OB_SUCCESS, compressor_.compressor_->compress(original.data(), OB_SYSLOG_COMPRESS_BLOCK_SIZE, buffers_,
                                                            OB_SYSLOG_COMPRESS_BUFFER_SIZE, compressed_size));
    const std::string frame(buffers_, compressed_size);
    ASSERT_EQ(OB_SUCCESS, compressor_.compressor_->compress(original.data(), 117, buffers_,
                                                            OB_SYSLOG_COMPRESS_BUFFER_SIZE, compressed_size));
    const std::string complete = frame + frame + std::string(buffers_, compressed_size);
    for (int kind = 0; kind < 5; ++kind) {
      char name[128] = {};
      snprintf(name, sizeof(name), "observer.log.202610121200%05d", algorithm * 5 + kind);
      names.push_back(name);
      create(name, original);
      const std::string outputs[] = {"", frame.substr(0, frame.size() / 2), frame, complete, "unrelated output"};
      create((std::string(name) + ".zst").c_str(), outputs[kind], 200);
    }
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  ASSERT_EQ(OB_SUCCESS, compressor_.set_max_disk_size(OB_SYSLOG_COMPRESS_RESERVE_SIZE));
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  compressor_.resume_after_config_update();
  for (int i = 0; i < 1000 && exists(names.back().c_str()); ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  for (const std::string &name : names) {
    EXPECT_FALSE(exists(name.c_str()));
    ASSERT_EQ(0, system(("zstd -q -d -f " + path(name.c_str()) + ".zst -o " + path("decoded")).c_str()));
    EXPECT_TRUE(original == contents(path("decoded")));
  }
}

TEST_F(LibPalfSyslogTest, legacy_temporary_cleanup_is_strict_and_accounts_retained_files)
{
  const char *temporary = "observer.log.20261012120000000.zst.tmp.1.2.3";
  create(temporary, "partial");
  create("observer.log.wf.20261012120000000.zst.tmp.1.2.3", "wf");
  create("observer.log.20261012120000000.zst.tmp.1.2.bad", "unknown");
  create("observer.log", "active");
  ASSERT_EQ(0, symlink("observer.log", path("observer.log.20261012120000001.zst.tmp.1.2.3").c_str()));
  fail_temporary_unlink_once.store(true);
  ObSyslogFile files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts[OB_SYSLOG_COMPRESS_TYPE_COUNT], bytes = 0;
  ObSyslogScanStatus scan_status;
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(false, false, true, files, counts, bytes, &scan_status));
  EXPECT_TRUE(scan_status.cleanup_pending_);
  EXPECT_TRUE(exists(temporary));
  ASSERT_EQ(OB_SUCCESS, compressor_.scan_log_files_(false, false, true, files, counts, bytes, &scan_status));
  EXPECT_FALSE(exists(temporary));
  EXPECT_FALSE(scan_status.cleanup_pending_);
  EXPECT_TRUE(exists("observer.log.wf.20261012120000000.zst.tmp.1.2.3"));
  EXPECT_TRUE(exists("observer.log.20261012120000000.zst.tmp.1.2.bad"));
  EXPECT_TRUE(exists("observer.log.20261012120000001.zst.tmp.1.2.3"));
  fail_temporary_unlink_once.store(false);
}

class SyslogConflictCleanupTest : public LibPalfSyslogTest
{
public:
  void TearDown() override
  {
    compressor_.destroy();
    count_cleanup_reads.store(false);
    count_scan_opens.store(false);
    scan_open_delay_us.store(0);
    previous_scan_start_us.store(0);
    minimum_scan_interval_us.store(INT64_MAX);
    available_bytes_override.store(-1);
    fail_temporary_unlink_once.store(false);
    fail_entry_stat.store(false);
    review_fault.store(ReviewFault::NONE);
    LibPalfSyslogTest::TearDown();
  }
  int scan(bool cleanup_conflicts = true, bool enable_libpalf = true)
  {
    return compressor_.scan_log_files_(true, enable_libpalf, true, files_, counts_, bytes_, &status_,
                                       cleanup_conflicts);
  }
  int cleanup(const char *name, bool &removed)
  {
    struct stat source_st = {}, output_st = {};
    const std::string source = path(name);
    const std::string output = source + ".zst";
    EXPECT_EQ(0, lstat(source.c_str(), &source_st));
    EXPECT_EQ(0, lstat(output.c_str(), &output_st));
    return compressor_.remove_conflicting_output_(source.c_str(), source_st, output.c_str(), output_st, removed);
  }
  ObSyslogFile files_[OB_SYSLOG_COMPRESS_TYPE_COUNT];
  int64_t counts_[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {};
  int64_t bytes_ = 0;
  ObSyslogScanStatus status_;
};

TEST_F(SyslogConflictCleanupTest, raw_archive_wins_without_reading_or_encoding_either_copy)
{
  const std::string raw(65536, 'c');
  const ObCompressorType algorithms[] = {ZSTD_COMPRESSOR, ZSTD_1_3_8_COMPRESSOR};
  int sequence = 0;
  for (ObCompressorType algorithm : algorithms) {
    ObCompressor *encoder = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(algorithm, encoder));
    int64_t encoded_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              encoder->compress(raw.data(), raw.size(), buffers_, OB_SYSLOG_COMPRESS_BUFFER_SIZE, encoded_size));
    const std::string complete(buffers_, encoded_size);
    std::string altered = complete;
    altered.back() ^= 1;
    const std::string outputs[] = {"", complete.substr(0, complete.size() / 2), complete, altered, "unrelated output"};
    for (int family = 0; family < 2; ++family) {
      for (const std::string &output : outputs) {
        const std::string name = family == 0 ? "observer.log." + std::to_string(20261012120000000L + sequence++)
                                             : "libpalf.log.20261012120000." + std::to_string(sequence++);
        create(name.c_str(), raw);
        create((name + ".zst").c_str(), output);
        cleanup_read_source = path(name.c_str());
        cleanup_read_count.store(0);
        count_cleanup_reads.store(true);
        ASSERT_EQ(OB_SUCCESS, scan());
        EXPECT_EQ(0, cleanup_read_count.load());
        EXPECT_FALSE(exists((name + ".zst").c_str()));
        EXPECT_TRUE(raw == contents(path(name.c_str())));
        EXPECT_FALSE(status_.cleanup_pending_);
      }
    }
  }
}

TEST_F(SyslogConflictCleanupTest, orphan_unmanaged_empty_and_active_pairs_are_preserved)
{
  create("observer.log.20261012120000000.zst", "orphan output");
  const char *preserved[] = {"observer.log", "observer.log.wf.20261012120000001", "unknown.log.123"};
  for (const char *name : preserved) {
    create(name, "retained source");
    create((std::string(name) + ".zst").c_str(), "retained output");
  }
  create("observer.log.20261012120000002", "");
  create("observer.log.20261012120000002.zst", "last usable copy");
  ASSERT_EQ(0, link(path("observer.log").c_str(), path("observer.log.20261012120000003").c_str()));
  create("observer.log.20261012120000003.zst", "active hardlink output");
  ASSERT_EQ(0, symlink("observer.log", path("observer.log.20261012120000004").c_str()));
  create("observer.log.20261012120000004.zst", "symlink source output");
  create("observer.log.20261012120000005", "raw");
  ASSERT_EQ(0, symlink("observer.log", path("observer.log.20261012120000005.zst").c_str()));
  ASSERT_EQ(OB_SUCCESS, scan());
  for (const char *name : preserved) {
    EXPECT_EQ("retained source", contents(path(name)));
    EXPECT_EQ("retained output", contents(path(name) + ".zst"));
  }
  for (int i = 0; i < 6; ++i) {
    if (i != 1) {
      EXPECT_TRUE(exists(("observer.log." + std::to_string(20261012120000000L + i) + ".zst").c_str()));
    }
  }
}

TEST_F(SyslogConflictCleanupTest, cleanup_respects_compression_switches)
{
  const char *legacy = "observer.log.20261012120000000";
  const char *libpalf = "libpalf.log.20261012120000";
  create(legacy, "raw");
  create(libpalf, "raw");
  create((std::string(legacy) + ".zst").c_str(), "previous output");
  create((std::string(libpalf) + ".zst").c_str(), "previous output");
  ASSERT_EQ(OB_SUCCESS, scan(false));
  EXPECT_TRUE(exists((std::string(legacy) + ".zst").c_str()));
  EXPECT_TRUE(exists((std::string(libpalf) + ".zst").c_str()));
  ASSERT_EQ(OB_SUCCESS, scan(true, false));
  EXPECT_FALSE(exists((std::string(legacy) + ".zst").c_str()));
  EXPECT_TRUE(exists((std::string(libpalf) + ".zst").c_str()));
  ASSERT_EQ(OB_SUCCESS, scan());
  EXPECT_FALSE(exists((std::string(libpalf) + ".zst").c_str()));
  EXPECT_EQ(1, status_.eligible_count_[SYSLOG_OBSERVER]);
  EXPECT_EQ(1, status_.eligible_count_[SYSLOG_LIBPALF]);
}

TEST_F(SyslogConflictCleanupTest, unreadable_source_and_changed_files_preserve_both_copies)
{
  const char *name = "observer.log.20261012120000000";
  const std::string source = path(name);
  const std::string output = source + ".zst";
  create(name, "raw");
  create((std::string(name) + ".zst").c_str(), "output");
  ASSERT_EQ(0, chmod(source.c_str(), 0000));
  bool removed = true;
  const int unreadable_ret = cleanup(name, removed);
  ASSERT_EQ(0, chmod(source.c_str(), 0600));
  if (geteuid() != 0) {
    EXPECT_EQ(OB_IO_ERROR, unreadable_ret);
    EXPECT_FALSE(removed);
    EXPECT_TRUE(exists((std::string(name) + ".zst").c_str()));
  }
  create((std::string(name) + ".zst").c_str(), "output");
  for (int replace_output = 0; replace_output < 2; ++replace_output) {
    struct stat source_st = {}, output_st = {};
    ASSERT_EQ(0, lstat(source.c_str(), &source_st));
    ASSERT_EQ(0, lstat(output.c_str(), &output_st));
    create("replacement", replace_output ? "output" : "raw");
    ASSERT_EQ(0, rename(path("replacement").c_str(), replace_output ? output.c_str() : source.c_str()));
    removed = true;
    EXPECT_EQ(OB_STATE_NOT_MATCH,
              compressor_.remove_conflicting_output_(source.c_str(), source_st, output.c_str(), output_st, removed));
    EXPECT_FALSE(removed);
    EXPECT_EQ("raw", contents(source));
    EXPECT_EQ("output", contents(output));
  }
  entry_stat_name = "observer.log";
  fail_entry_stat.store(true);
  const int active_stat_ret = cleanup(name, removed);
  fail_entry_stat.store(false);
  EXPECT_EQ(OB_IO_ERROR, active_stat_ret);
  EXPECT_FALSE(removed);
  EXPECT_EQ("output", contents(output));
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_EQ(OB_CANCELED, cleanup(name, removed));
  EXPECT_FALSE(removed);
  compressor_.resume_after_config_update();
  // The output itself need not be readable: only the raw archive is authoritative.
  ASSERT_EQ(0, chmod(output.c_str(), 0000));
  EXPECT_EQ(OB_SUCCESS, cleanup(name, removed));
  EXPECT_TRUE(removed);
  EXPECT_EQ("raw", contents(source));
}

TEST_F(SyslogConflictCleanupTest, cleanup_io_errors_keep_raw_and_retry_deletion)
{
  const char *name = "observer.log.20261012120000000";
  create(name, "raw");
  review_source = path(name);
  review_directory = directory_;
  const ReviewFault faults[] = {ReviewFault::FSYNC_SOURCE, ReviewFault::FSYNC_DIRECTORY, ReviewFault::UNLINK_OUTPUT};
  for (ReviewFault fault : faults) {
    create((std::string(name) + ".zst").c_str(), "output");
    review_fault_hits.store(0);
    review_fault.store(fault);
    bool removed = true;
    const int cleanup_ret = cleanup(name, removed);
    review_fault.store(ReviewFault::NONE);
    EXPECT_EQ(OB_IO_ERROR, cleanup_ret);
    EXPECT_GT(review_fault_hits.load(), 0);
    EXPECT_FALSE(removed);
    EXPECT_EQ("raw", contents(path(name)));
    EXPECT_EQ("output", contents(path(name) + ".zst"));
    ASSERT_EQ(OB_SUCCESS, scan());
    EXPECT_FALSE(exists((std::string(name) + ".zst").c_str()));
    EXPECT_EQ(3, bytes_);
  }
}

TEST_F(SyslogConflictCleanupTest, failed_deletion_does_not_encode_or_block_other_files)
{
  const char *failed_name = "observer.log.20261012120000000";
  const char *healthy_name = "observer.log.20261012120000001";
  create(failed_name, "raw");
  create(healthy_name, "raw");
  create((std::string(failed_name) + ".zst").c_str(), "output");
  create((std::string(healthy_name) + ".zst").c_str(), "output");
  review_source = path(failed_name);
  review_fault_hits.store(0);
  review_fault.store(ReviewFault::UNLINK_OUTPUT);
  cleanup_read_source = path(failed_name);
  cleanup_read_count.store(0);
  count_cleanup_reads.store(true);
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  scan_root = directory_;
  available_bytes_override.store(8LL << 30);
  scan_open_count.store(0);
  count_scan_opens.store(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && scan_open_count.load() < 5; ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_GE(scan_open_count.load(), 5);
  EXPECT_GT(review_fault_hits.load(), 1);
  EXPECT_EQ(0, cleanup_read_count.load());
  EXPECT_TRUE(exists(failed_name));
  EXPECT_TRUE(exists(healthy_name));
  EXPECT_TRUE(exists((std::string(failed_name) + ".zst").c_str()));
  EXPECT_FALSE(exists((std::string(healthy_name) + ".zst").c_str()));
}

TEST_F(SyslogConflictCleanupTest, failed_final_sync_still_rebuilds_accounting_and_candidates)
{
  const char *name = "observer.log.20261012120000000";
  create((std::string(name) + ".zst").c_str(), "removed output");
  create(name, "raw");
  create("observer.log.20261012120000001.zst", "orphan");
  review_source = path(name);
  review_directory = directory_;
  review_directory_sync_calls.store(0);
  review_fault.store(ReviewFault::FSYNC_DIRECTORY_AFTER_UNLINK);
  ASSERT_EQ(OB_SUCCESS, scan());
  review_fault.store(ReviewFault::NONE);
  EXPECT_EQ(2, review_directory_sync_calls.load());
  EXPECT_TRUE(status_.cleanup_pending_);
  EXPECT_EQ(9, bytes_);
  EXPECT_EQ(1, status_.eligible_count_[SYSLOG_OBSERVER]);
  EXPECT_FALSE(exists((std::string(name) + ".zst").c_str()));
  ASSERT_EQ(2, compressor_.oldest_files_.count());
  while (compressor_.oldest_files_.count() > 0) {
    const ObSyslogFile *candidate = nullptr;
    ASSERT_EQ(OB_SUCCESS, compressor_.oldest_files_.top(candidate));
    EXPECT_NE(path(name) + ".zst", std::string(candidate->file_name_));
    compressor_.oldest_files_.pop();
  }
}

TEST_F(SyslogConflictCleanupTest, cleanup_handles_all_pairs_without_recovery_budget)
{
  for (int i = 0; i < 9; ++i) {
    const std::string name = "observer.log." + std::to_string(20261012120000000L + i);
    create(name.c_str(), "raw");
    create((name + ".zst").c_str(), "output");
  }
  ASSERT_EQ(OB_SUCCESS, scan());
  EXPECT_EQ(27, bytes_);
  EXPECT_EQ(9, status_.eligible_count_[SYSLOG_OBSERVER]);
  for (int i = 0; i < 9; ++i) {
    const std::string name = "observer.log." + std::to_string(20261012120000000L + i);
    EXPECT_TRUE(exists(name.c_str()));
    EXPECT_FALSE(exists((name + ".zst").c_str()));
  }
}

TEST_F(SyslogConflictCleanupTest, slow_cycles_still_wait_before_rescanning)
{
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  ASSERT_EQ(OB_SUCCESS, compressor_.init());
  snprintf(compressor_.syslog_dir_, sizeof(compressor_.syslog_dir_), "%s", directory_);
  snprintf(compressor_.alert_log_dir_, sizeof(compressor_.alert_log_dir_), "%s/alert", directory_);
  compressor_.loop_interval_ = 20000;
  ASSERT_EQ(OB_SUCCESS, compressor_.set_compress_func("zstd_1.3.8"));
  scan_root = directory_;
  scan_open_count.store(0);
  previous_scan_start_us.store(0);
  minimum_scan_interval_us.store(INT64_MAX);
  scan_open_delay_us.store(50000);  // Every scan exceeds the configured cycle interval.
  count_scan_opens.store(true);
  compressor_.resume_after_config_update();
  for (int i = 0; i < 300 && scan_open_count.load() < 3; ++i) {
    usleep(10000);
  }
  ASSERT_EQ(OB_SUCCESS, compressor_.pause_for_config_update());
  EXPECT_GE(scan_open_count.load(), 3);
  EXPECT_GE(minimum_scan_interval_us.load(), 50000 + OB_SYSLOG_COMPRESS_MIN_IDLE - 10000);
}

}  // namespace common
}  // namespace oceanbase

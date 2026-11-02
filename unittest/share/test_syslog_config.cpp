/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include "share/config/ob_config_manager.h"
#include "share/config/ob_reload_config.h"
#include "lib/oblog/ob_log_compressor.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#define private public
#include "observer/ob_server_reload_config.h"
#undef private
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_logger.h"
#endif

using namespace oceanbase;
using namespace oceanbase::common;

static void set_config(const char *count, const char *disk, const char *algorithm, bool recycle = false)
{
  ObServerConfig &config = ObServerConfig::get_instance();
  ASSERT_TRUE(config.max_syslog_file_count.set_value(count));
  ASSERT_TRUE(config.syslog_disk_size.set_value(disk));
  ASSERT_TRUE(config.syslog_compress_func.set_value(algorithm));
  ASSERT_TRUE(config.enable_syslog_recycle.set_value(recycle ? "true" : "false"));
  ASSERT_TRUE(config.syslog_file_uncompressed_count.set_value("0"));
  ASSERT_TRUE(config.enable_logservice.set_value("true"));
}

TEST(SyslogConfig, complete_allocation_matrix_uses_real_entry_and_ffi)
{
  struct Case { const char *count; const char *disk; const char *algorithm; int64_t rust_count; int64_t cpp_count; int64_t budget; };
  const int64_t gib = 1LL << 30;
  const Case cases[] = {
    {"0", "0M", "none", 0, 0, 0}, {"0", "6G", "none", 4, 0, 5 * gib},
    {"10", "0M", "none", 2, 8, 0}, {"10", "6G", "none", 2, 8, 11 * gib / 2},
    {"0", "0M", "zstd_1.0", 10000, 0, 0}, {"0", "6G", "zstd_1.0", 10000, 0, 6 * gib},
    {"10", "0M", "zstd_1.0", 10000, 8, 0}, {"10", "6G", "zstd_1.0", 10000, 8, 6 * gib},
    {"0", "0M", "zstd_1.3.8", 10000, 0, 0}, {"0", "6G", "zstd_1.3.8", 10000, 0, 6 * gib},
    {"10", "0M", "zstd_1.3.8", 10000, 8, 0}, {"10", "6G", "zstd_1.3.8", 10000, 8, 6 * gib}
  };
  for (const Case &item : cases) {
    for (int recycle = 0; recycle < 2; ++recycle) {
      set_config(item.count, item.disk, item.algorithm, recycle != 0);
      ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      int64_t actual = 0;
      ASSERT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(actual));
      EXPECT_EQ(item.rust_count, actual);
      EXPECT_EQ(item.cpp_count, OB_LOGGER.get_max_file_index());
      EXPECT_EQ(item.budget, OB_LOG_COMPRESSOR.get_max_disk_size());
#else
      EXPECT_EQ(strtoll(item.count, nullptr, 10), OB_LOGGER.get_max_file_index());
      EXPECT_EQ(strcmp(item.disk, "0M") == 0 ? 0 : 6 * gib, OB_LOG_COMPRESSOR.get_max_disk_size());
#endif
      EXPECT_EQ(recycle != 0, OB_LOGGER.get_record_old_log_file());
    }
  }
}

TEST(SyslogConfig, logger_reload_reports_failure)
{
  ObServerConfig &config = ObServerConfig::get_instance();
  set_config("0", "0M", "none");
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(config));
  ASSERT_TRUE(config.syslog_compress_func.set_value("not_a_compressor"));
  ObReloadConfig reload(&config);
  EXPECT_NE(OB_SUCCESS, reload());
  EXPECT_EQ(NONE_COMPRESSOR, OB_LOG_COMPRESSOR.get_compress_func());
  ObReloadConfig missing(nullptr);
  EXPECT_EQ(OB_NOT_INIT, missing());
  set_config("0", "0M", "none");
  EXPECT_EQ(OB_SUCCESS, reload());
}

TEST(SyslogConfig, zstd_enable_preserves_existing_cpp_archives)
{
  set_config("0", "0M", "none");
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  for (int i = 0; i < 12; ++i) {
    char name[128];
    snprintf(name, sizeof(name), "log/observer.log.200001020000%05d", i);
    std::ofstream file(name); file << "retained history\n";
  }
  set_config("10", "0M", "zstd_1.3.8", true);
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  for (int i = 0; i < 12; ++i) {
    char name[128];
    snprintf(name, sizeof(name), "log/observer.log.200001020000%05d", i);
    EXPECT_EQ(0, access(name, F_OK));
    unlink(name);
  }
}

TEST(SyslogConfig, history_failure_rolls_back_rust_and_cpp_then_retries)
{
  set_config("10", "0M", "none", false);
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  const ObSyslogConfig previous = OB_LOG_COMPRESSOR.get_config(OB_LOGGER);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int64_t original_rust_count = 0;
  ASSERT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(original_rust_count));
  ASSERT_EQ(2, original_rust_count);
#endif
  // Keep ordinary logger/FFI paths available; only historical alert enumeration fails.
  ASSERT_EQ(0, rename("log/alert", "log/alert.saved"));
  { std::ofstream blocker("log/alert"); blocker << "not a directory"; }
  set_config("10", "0M", "zstd_1.3.8", true);
  const int failed = ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance());
  unlink("log/alert");
  ASSERT_EQ(0, rename("log/alert.saved", "log/alert"));
  EXPECT_NE(OB_SUCCESS, failed);
  EXPECT_TRUE(previous == OB_LOG_COMPRESSOR.get_config(OB_LOGGER));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int64_t restored_rust_count = 0;
  ASSERT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(restored_rust_count));
  EXPECT_EQ(original_rust_count, restored_rust_count);
#endif
  EXPECT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  EXPECT_EQ(ZSTD_1_3_8_COMPRESSOR, OB_LOG_COMPRESSOR.get_compress_func());
  EXPECT_TRUE(OB_LOGGER.get_record_old_log_file());
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int64_t retried_rust_count = 0;
  ASSERT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(retried_rust_count));
  EXPECT_EQ(10000, retried_rust_count);
#endif
}

TEST(SyslogConfig, history_failure_preserves_previously_enabled_recycling)
{
  set_config("10", "0M", "zstd_1.3.8", true);
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  const ObSyslogConfig previous = OB_LOG_COMPRESSOR.get_config(OB_LOGGER);
  ASSERT_EQ(0, rename("log/alert", "log/alert.saved"));
  { std::ofstream blocker("log/alert"); blocker << "persistent directory fault"; }
  // Exercise both unchanged Rust quota and a quota that must be restored after C++ fails.
  const char *algorithms[] = {"zstd_1.3.8", "none"};
  for (const char *algorithm : algorithms) {
    set_config("20", "0M", algorithm, true);
    EXPECT_NE(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
    EXPECT_TRUE(previous == OB_LOG_COMPRESSOR.get_config(OB_LOGGER));
    EXPECT_TRUE(OB_LOG_COMPRESSOR.is_config_applied(OB_LOGGER, previous));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    int64_t rust_count = 0;
    EXPECT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(rust_count));
    EXPECT_EQ(10000, rust_count);
    EXPECT_TRUE(OB_LOG_COMPRESSOR.is_libpalf_compression_enabled());
#endif
  }
  EXPECT_EQ(0, unlink("log/alert"));
  ASSERT_EQ(0, rename("log/alert.saved", "log/alert"));
  EXPECT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  EXPECT_EQ(NONE_COMPRESSOR, OB_LOG_COMPRESSOR.get_compress_func());
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int64_t restored_rust_count = 0;
  ASSERT_EQ(OB_SUCCESS, libpalf::LibPalfLogger::get_max_syslog_file_count(restored_rust_count));
  EXPECT_EQ(4, restored_rust_count);
#endif
}

TEST(SyslogConfig, cluster_identity_refreshes_after_logger_reload_failure)
{
  const std::string old_name = GCONF.cluster.str();
  const int64_t old_id = GCONF.cluster_id.get_value();
  const int64_t old_rpc_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  const uint64_t old_hash = obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH;
  const char *new_name = "cluster_after_syslog_error";
  ASSERT_TRUE(GCONF.cluster.set_value(new_name));
  ASSERT_TRUE(GCONF.cluster_id.set_value("123"));
  obrpc::ObRpcNetHandler::CLUSTER_ID = 0;
  obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH = 0;
  EXPECT_EQ(OB_IO_ERROR, observer::ObServerReloadConfig::reload_cluster_config_(false, OB_IO_ERROR));
  EXPECT_EQ(123, obrpc::ObRpcNetHandler::CLUSTER_ID);
  EXPECT_EQ(common::murmurhash(new_name, strlen(new_name), 0), obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH);
  EXPECT_EQ(OB_SUCCESS, observer::ObServerReloadConfig::reload_cluster_config_(false, OB_SUCCESS));
  EXPECT_TRUE(GCONF.cluster.set_value(old_name.c_str()));
  GCONF.cluster_id = old_id;
  obrpc::ObRpcNetHandler::CLUSTER_ID = old_rpc_id;
  obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH = old_hash;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
TEST(SyslogConfig, repeated_identical_updates_allow_inflight_compression_to_finish)
{
  set_config("0", "4G", "zstd_1.3.8");
  ASSERT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
  const char *source = "log/libpalf.log.20000102010101";
  const char *output = "log/libpalf.log.20000102010101.zst";
  { std::ofstream file(source); file << std::string(32 * 1024 * 1024, 'x'); }
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.init());
  bool finished_while_reloading = false;
  for (int i = 0; i < 40; ++i) {
    EXPECT_EQ(OB_SUCCESS, ObConfigManager::ob_logger_config_update(ObServerConfig::get_instance()));
    usleep(80000);
    finished_while_reloading = finished_while_reloading || access(output, F_OK) == 0;
  }
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.pause_for_config_update());
  EXPECT_TRUE(finished_while_reloading);
  EXPECT_EQ(0, access(output, F_OK));
  EXPECT_NE(0, access(source, F_OK));
  OB_LOG_COMPRESSOR.destroy();
}
#endif

int main(int argc, char **argv)
{
  char work_dir[] = "/tmp/ob_syslog_config_XXXXXX";
  if (nullptr == mkdtemp(work_dir) || chdir(work_dir) != 0) { return 1; }
  mkdir("log", 0700); mkdir("log/alert", 0700); mkdir("audit", 0700);
  OB_LOGGER.set_file_name("log/observer.log", true, false, "log/rootservice.log", "log/election.log",
                         "log/trace.log", "audit/audit.log", "log/alert/alert.log");
  OB_LOGGER.set_log_compressor(&OB_LOG_COMPRESSOR);
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  OB_LOG_COMPRESSOR.destroy();
  return ret;
}

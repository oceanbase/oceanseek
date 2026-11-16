/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_admin_test_io_device_executor.h"
#include <unistd.h>
#include "lib/restore/ob_storage_async.h"
#include "lib/restore/ob_storage_condition_put_probe.h"
#include "lib/string/ob_sql_string.h"
#include "share/object_storage/ob_object_storage_connectivity_check.h"

namespace oceanbase
{
namespace tools
{
using namespace common;
using namespace share;


enum TestGroup
{
  BASIC_CONNECTIVITY = 0,
  SHARED_STORAGE,
  BACKUP_RESTORE,
  OPTIONAL_FEATURES,
  GROUP_COUNT
};

const char *const GROUP_LABELS[GROUP_COUNT] = {
  "[1/4] Basic connectivity", "[2/4] Shared storage", "[3/4] Backup and restore", "[4/4] Optional features"
};

struct ObAdminTestIODeviceContext
{
  ObAdminTestIODeviceContext();
  ~ObAdminTestIODeviceContext();
  void reset();

  ObBackupDest dest_;
  ObSqlString subtest_output_;
  ObSqlString pending_messages_;
  int64_t timeout_us_;
  bool is_test_if_match_;
  bool is_obdal_enabled_;
  bool is_async_enabled_;
};

ObAdminTestIODeviceContext::ObAdminTestIODeviceContext()
  : subtest_output_("IOTestOutput"), pending_messages_("IOTestOutput")
{
  reset();
}

ObAdminTestIODeviceContext::~ObAdminTestIODeviceContext()
{
  reset();
}

void ObAdminTestIODeviceContext::reset()
{
  pending_messages_.reset();
  subtest_output_.reset();
  dest_.reset();
  timeout_us_ = 0;
  is_test_if_match_ = false;
  is_obdal_enabled_ = false;
  is_async_enabled_ = false;
}

// The legacy guards access the current run through this thread-local context.
static thread_local ObAdminTestIODeviceContext *active_test_context = nullptr;

const char *status_color_(const char *status)
{
  const char *color = "";
  if (isatty(STDOUT_FILENO)) {
    if (0 == STRCMP(status, "PASS")) {
      color = LIGHT_GREEN;
    } else if (0 == STRCMP(status, "FAIL")) {
      color = LIGHT_RED;
    }
  }
  return color;
}

void append_result_(
    ObAdminTestIODeviceContext &ctx,
    const char *title,
    const char *status,
    const int64_t elapsed_ms = -1)
{
  const char *color = status_color_(status);
  const char *reset = '\0' == color[0] ? "" : NONE_COLOR;
  int ret = OB_SUCCESS;
  if (elapsed_ms < 0) {
    if (OB_FAIL(ctx.subtest_output_.append_fmt("      %-34s%s%s%s\n", title, color, status, reset))) {
      printf("      %s: %s%s%s\n", title, color, status, reset);
      STORAGE_LOG(WARN, "failed to format connectivity subtest", KR(ret));
    }
  } else if (OB_FAIL(ctx.subtest_output_.append_fmt("      %-34s%s%-11s%s%6.3f s\n",
      title, color, status, reset, elapsed_ms / 1000.0))) {
    printf("      %s: %s%-11s%s%6.3f s\n", title, color, status, reset, elapsed_ms / 1000.0);
    STORAGE_LOG(WARN, "failed to format connectivity subtest", KR(ret));
  }
}

void append_details_(ObAdminTestIODeviceContext &ctx, const int test_ret)
{
  int ret = OB_SUCCESS;
  if (!ctx.pending_messages_.empty()) {
    if (OB_FAIL(ctx.subtest_output_.append(ctx.pending_messages_.ptr(), ctx.pending_messages_.length()))) {
      STORAGE_LOG(WARN, "failed to append pending messages", KR(ret));
    }
    ctx.pending_messages_.reuse();
  }
  if (OB_SUCC(ret) && OB_SUCCESS != test_ret) {
    if (OB_FAIL(ctx.subtest_output_.append_fmt("        Error: %d (%s)\n", test_ret, ob_error_name(test_ret)))) {
      STORAGE_LOG(WARN, "failed to append error details", KR(ret));
    }
  }
}

bool record_message_(ObAdminTestIODeviceContext &ctx, const char *message)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.pending_messages_.append_fmt("        %s\n", message))) {
    STORAGE_LOG(WARN, "failed to record connectivity message", KR(ret));
  }
  return OB_SUCCESS == ret;
}

const char *backend_name_(const ObStorageType type)
{
  const char *name = "unknown";
  if (type == OB_STORAGE_FILE) {
    name = "FILE";
  } else if (type == OB_STORAGE_S3) {
    name = ObClusterStateBaseMgr::get_instance().is_enable_obdal()
           ? "S3 / OBDAL"
           : "S3 / AWS SDK";
  } else if (type == OB_STORAGE_AZBLOB) {
    name = "AZBLOB / OBDAL";
  }
  return name;
}

const char *put_mode_name_(const ObObjectStorageConditionPutMode mode)
{
  const char *name = nullptr;
  if (ObObjectStorageConditionPutMode::INHERIT == mode) {
    name = "inherit";
  } else if (ObObjectStorageConditionPutMode::NONE == mode) {
    name = "none";
  } else if (ObObjectStorageConditionPutMode::IF_MATCH == mode) {
    name = "if-match";
  } else if (ObObjectStorageConditionPutMode::AUTO == mode) {
    name = "auto";
  } else {
    name = "unknown";
  }
  return name;
}

const char *delete_mode_name_(const ObStorageDeleteMode mode)
{
  const char *name = nullptr;
  if (ObStorageDeleteMode::NONE == mode) {
    name = "none";
  } else if (ObStorageDeleteMode::STORAGE_DELETE_MODE == mode) {
    name = "delete";
  } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == mode) {
    name = "tagging";
  } else {
    name = "unknown";
  }
  return name;
}

void print_group_result_(
    ObAdminTestIODeviceContext &ctx,
    const int64_t group,
    const char *status,
    const int64_t elapsed_us,
    const int group_ret)
{
  const char *color = status_color_(status);
  const char *reset = '\0' == color[0] ? "" : NONE_COLOR;
  if (isatty(STDOUT_FILENO)) {
    printf("\r\033[2K");
  }
  if (!ctx.subtest_output_.empty()) {
    printf("%s\n", GROUP_LABELS[group]);
  } else if (elapsed_us < 0) {
    printf("%-40s%s%s%s\n", GROUP_LABELS[group], color, status, reset);
  } else {
    printf("%-40s%s%-11s%s%6.3f s\n", GROUP_LABELS[group], color, status, reset, elapsed_us / 1000000.0);
  }
  append_details_(ctx, group_ret);
  if (!ctx.subtest_output_.empty()) {
    printf("%.*s", static_cast<int>(ctx.subtest_output_.length()), ctx.subtest_output_.ptr());
  }
  printf("\n");
  fflush(stdout);
}

void print_summary_(const int ret, const int passed, const int failed, const int skipped, const int not_run,
    const int64_t elapsed_us)
{
  const char *status = OB_SUCCESS == ret ? "PASS" : "FAIL";
  const char *color = status_color_(status);
  const char *reset = '\0' == color[0] ? "" : NONE_COLOR;
  printf("Result: %s%s%s - %d %s passed", color, status, reset, passed, passed == 1 ? "group" : "groups");
  if (failed > 0) {
    printf(", %d failed", failed);
  }
  if (skipped > 0) {
    printf(", %d skipped", skipped);
  }
  if (not_run > 0) {
    printf(", %d not run", not_run);
  }
  printf(", total %.3f s\n", elapsed_us / 1000000.0);
  if (OB_SUCCESS != ret) {
    printf("See ob_admin.log for the operation details.\n");
  }
}

bool is_test_io_device_report_enabled()
{
  return nullptr != active_test_context;
}

bool record_test_io_device_message(const char *message)
{
  return nullptr != active_test_context && record_message_(*active_test_context, message);
}

void report_test_io_device_subtest(const char *title, const int ret, const int64_t elapsed_ms)
{
  if (nullptr != active_test_context) {
    const char *label = title;
    if (0 == STRCMP(title, "archive log")) {
      label = "Archive log";
    } else if (0 == STRCMP(title, "backup data")) {
      label = "Backup data";
    } else if (0 == STRCMP(title, "consume clog file")) {
      label = "Consume clog";
    } else if (0 == STRCMP(title, "clean backup file")) {
      label = "Clean backup files";
    }
    append_result_(*active_test_context, label, OB_SUCCESS == ret ? "PASS" : "FAIL", elapsed_ms);
    append_details_(*active_test_context, ret);
  }
}

static int run_storage_modes_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  const ObBackupDest &dest = ctx.dest_;
  const bool is_s3sdk = OB_STORAGE_S3 == dest.get_storage_type() && !ctx.is_obdal_enabled_;
  ObClusterStateBaseMgr &state_mgr = ObClusterStateBaseMgr::get_instance();
  const bool old_async_enabled = state_mgr.is_enable_object_storage_async_io();
  const char *titles[2] = {"Sync", "Async"};
  for (int64_t mode = 0; mode < 2 && OB_SUCC(ret); ++mode) {
    const bool is_async_mode = 1 == mode;
    if (is_async_mode && is_s3sdk) {
      append_result_(ctx, titles[mode], "SKIP");
      record_message_(ctx, "Async checks are disabled with --s3sdk.");
    } else {
      state_mgr.set_enable_object_storage_async_io(is_async_mode);
      ObObjectStorageConnectivityCheck check;
      const int64_t start_ts = ObTimeUtility::current_time();
      if (OB_FAIL(check.check_shared_storage(dest, ctx.timeout_us_))) {
        STORAGE_LOG(WARN, "shared storage connectivity check failed", KR(ret), K(is_async_mode),
            K(dest.get_root_path()));
        const char *failed_case = check.get_failed_case();
        if (nullptr != failed_case) {
          record_message_(ctx, failed_case);
        }
      }
      const int64_t elapsed_ms = (ObTimeUtility::current_time() - start_ts) / 1000;

      append_result_(ctx, titles[mode], OB_SUCCESS == ret ? "PASS" : "FAIL", elapsed_ms);
    }
  }
  state_mgr.set_enable_object_storage_async_io(old_async_enabled);
  return ret;
}

int ObAdminTestIODeviceExecutor::run_new_tests_()
{
  int ret = OB_SUCCESS;
  ObAdminTestIODeviceContext ctx;
  ObClusterStateBaseMgr &state_mgr = ObClusterStateBaseMgr::get_instance();
  ctx.timeout_us_ = IO_TIMEOUT_MS * 1000L;
  ctx.is_test_if_match_ = is_test_if_match_;
  ctx.is_obdal_enabled_ = state_mgr.is_enable_obdal();
  ctx.is_async_enabled_ = state_mgr.is_enable_object_storage_async_io();
  if (OB_FAIL(ctx.dest_.set(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "failed to initialize storage destination", KR(ret), K_(backup_path));
    printf("Storage configuration: invalid, ret=%d (%s)\n\n", ret, ob_error_name(ret));
    fflush(stdout);
  } else {
    if (OB_STORAGE_S3 == ctx.dest_.get_storage_type() && !ctx.is_obdal_enabled_) {
      state_mgr.set_enable_object_storage_async_io(false);
    }
    active_test_context = &ctx;
    printf("Object Storage Connectivity Check\n");
    printf("Backend: %s    Put mode: %s    Delete mode: %s\n\n", backend_name_(ctx.dest_.get_storage_type()),
        put_mode_name_(ctx.dest_.get_storage_info()->get_effective_put_mode()),
        delete_mode_name_(ctx.dest_.get_storage_info()->get_delete_mode()));
    fflush(stdout);
    if (OB_FAIL(run_test_groups_(ctx))) {
      STORAGE_LOG(WARN, "failed to pass connectivity test groups", KR(ret), K_(backup_path));
    }
    active_test_context = nullptr;
    state_mgr.set_enable_object_storage_async_io(ctx.is_async_enabled_);
    state_mgr.set_enable_obdal(ctx.is_obdal_enabled_);
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::run_test_groups_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t total_start_ts = ObTimeUtility::current_time();
  const bool is_file = OB_STORAGE_FILE == ctx.dest_.get_storage_type();
  int passed = 0;
  int failed = 0;
  int skipped = 0;
  int not_run = 0;
  for (int64_t group = 0; group < GROUP_COUNT; ++group) {
    ctx.subtest_output_.reuse();
    ctx.pending_messages_.reuse();
    if (OB_SUCCESS != ret) {
      print_group_result_(ctx, group, "NOT RUN", -1, OB_SUCCESS);
      ++not_run;
    } else if (OPTIONAL_FEATURES == group && !ctx.is_test_if_match_) {
      record_message_(ctx, "Enable with --if-match.");
      print_group_result_(ctx, group, "SKIP", -1, OB_SUCCESS);
      ++skipped;
    } else {
      if (isatty(STDOUT_FILENO)) {
        printf("%-40sRUN", GROUP_LABELS[group]);
        fflush(stdout);
      }
      const int64_t start_ts = ObTimeUtility::current_time();
      switch (group) {
        case BASIC_CONNECTIVITY: ret = test_basic_connectivity_(ctx); break;
        case SHARED_STORAGE: ret = test_shared_storage_connectivity_(ctx); break;
        case BACKUP_RESTORE: ret = test_backup_restore_connectivity_(ctx); break;
        case OPTIONAL_FEATURES: ret = test_if_match_write_(ctx); break;
        default: ret = OB_ERR_UNEXPECTED; break;
      }
      const int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
      bool is_skipped = false;
      if (OB_SUCC(ret) && is_file && (SHARED_STORAGE == group || OPTIONAL_FEATURES == group)) {
        is_skipped = true;
      }
      const char *status = nullptr;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "connectivity test group failed", KR(ret), K(group), K(ctx.dest_.get_root_path()));
        status = "FAIL";
        ++failed;
      } else if (is_skipped) {
        status = "SKIP";
        ++skipped;
      } else {
        status = "PASS";
        ++passed;
      }
      print_group_result_(ctx, group, status, is_skipped ? -1 : elapsed_us, ret);
    }
  }
  const int64_t elapsed_us = ObTimeUtility::current_time_us() - total_start_ts;
  print_summary_(ret, passed, failed, skipped, not_run, elapsed_us);
  return ret;
}

int ObAdminTestIODeviceExecutor::test_basic_connectivity_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObObjectStorageConnectivityCheck check;
  bool is_empty_directory = false;
  if (OB_FAIL(ObBackupIoAdapter::is_empty_directory(
      ctx.dest_.get_root_path(), ctx.dest_.get_storage_info(), is_empty_directory))) {
    STORAGE_LOG_FILTER(ERROR, "test path is inaccessible", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (!is_empty_directory) {
    // The retained backup cleanup removes all test files below this directory.
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG_FILTER(ERROR, "test path must be empty", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (OB_FAIL(check.check_basic(ctx.dest_, ctx.timeout_us_))) {
    STORAGE_LOG_FILTER(ERROR, "basic connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_shared_storage_connectivity_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_STORAGE_FILE == ctx.dest_.get_storage_type()) {
    record_message_(ctx, "Object storage semantics do not apply to FILE.");
  } else if (OB_FAIL(run_storage_modes_(ctx))) {
    STORAGE_LOG(WARN, "shared storage connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_backup_restore_connectivity_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(test_archive_log_())) {
    STORAGE_LOG(WARN, "archive log connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (OB_FAIL(test_backup_data_())) {
    STORAGE_LOG(WARN, "backup data connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (OB_FAIL(test_consume_clog_file_())) {
    STORAGE_LOG(WARN, "consume clog connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (OB_FAIL(test_clean_backup_file_())) {
    STORAGE_LOG(WARN, "clean backup file connectivity check failed", KR(ret), K(ctx.dest_.get_root_path()));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_if_match_write_(ObAdminTestIODeviceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObStorageConditionPutProbe probe;
  char path[OB_MAX_URI_LENGTH];
  bool is_supported = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_STORAGE_FILE == ctx.dest_.get_storage_type()) {
    append_result_(ctx, "If-match capability", "SKIP");
    record_message_(ctx, "Conditional object creation does not apply to FILE.");
  } else if (OB_FAIL(databuff_printf(path, sizeof(path), "%s/if_match_probe", ctx.dest_.get_root_path().ptr()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to build if-match probe path", KR(ret), K(ctx.dest_.get_root_path()));
  } else if (OB_FAIL(probe.detect(path, *ctx.dest_.get_storage_info(), is_supported))) {
    STORAGE_LOG(WARN, "if-match capability probe failed", KR(ret), K(path));
    const int64_t elapsed_ms = (ObTimeUtility::current_time() - start_ts) / 1000;
    append_result_(ctx, "If-match capability", "FAIL", elapsed_ms);
  } else {
    const int64_t elapsed_ms = (ObTimeUtility::current_time() - start_ts) / 1000;
    append_result_(ctx, "If-match capability", is_supported ? "SUPPORTED" : "UNSUPPORTED", elapsed_ms);
  }
  return ret;
}

} // namespace tools
} // namespace oceanbase

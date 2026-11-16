/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_CONNECTIVITY_CHECK_H_
#define OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_CONNECTIVITY_CHECK_H_

#include "lib/allocator/ob_allocator.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace share
{

class ObObjectStorageConnectivityCheck final
{
public:
  ObObjectStorageConnectivityCheck();
  ~ObObjectStorageConnectivityCheck();
  void reset();
  // Reports the first failed shared-storage case or setup/cleanup step.
  const char *get_failed_case() const { return failed_case_; }

  // The caller selects its IO mode; online probes never modify the cluster state manager.
  int check_basic(const ObBackupDest &storage_dest, const int64_t timeout_us);
  // Case outcomes are logged; cleanup only touches objects created by this invocation.
  int check_shared_storage(const ObBackupDest &storage_dest, const int64_t timeout_us);

private:
  int prepare_(const ObBackupDest &storage_dest, const int64_t timeout_us);
  int check_deadline_() const;
  int build_path_(const char *name, char *path, const int64_t path_len) const;
  int record_file_(const char *name);
  int write_file_(const char *name, const char *data, const int64_t size);
  int verify_file_(const char *name, const char *data, const int64_t size);
  int verify_list_(const char *name, const bool is_expected);
  int verify_absent_(const char *name);
  int check_prefix_();
  int check_multipart_();
  int check_abort_();
  int check_delete_();
  int check_batch_delete_();
  int check_appender_();
  int cleanup_();
  void log_case_(const char *case_name, const int case_ret);
  bool can_delete_() const;

private:
  common::ObArenaAllocator allocator_;
  common::ObObjectStorageInfo storage_info_;
  common::ObArray<common::ObString> created_files_;
  char root_[common::OB_MAX_URI_LENGTH];
  int64_t deadline_ts_us_;
  int64_t case_start_ts_us_;
  const char *failed_case_;
  DISALLOW_COPY_AND_ASSIGN(ObObjectStorageConnectivityCheck);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OBJECT_STORAGE_OB_OBJECT_STORAGE_CONNECTIVITY_CHECK_H_

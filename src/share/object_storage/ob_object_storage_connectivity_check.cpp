/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/object_storage/ob_object_storage_connectivity_check.h"
#include "lib/random/ob_random.h"
#include "lib/restore/ob_storage_async.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

ObObjectStorageConnectivityCheck::ObObjectStorageConnectivityCheck()
  : allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "StorageIoProbe"))
{
  reset();
}

ObObjectStorageConnectivityCheck::~ObObjectStorageConnectivityCheck()
{
  reset();
}

void ObObjectStorageConnectivityCheck::reset()
{
  created_files_.reset();
  allocator_.reset();
  storage_info_.reset();
  root_[0] = '\0';
  deadline_ts_us_ = 0;
  case_start_ts_us_ = 0;
  failed_case_ = nullptr;
}

int ObObjectStorageConnectivityCheck::prepare_(
    const ObBackupDest &storage_dest,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  reset();
  if (!storage_dest.is_valid() || OB_ISNULL(storage_dest.get_storage_info()) || timeout_us <= 0
      || timeout_us > INT64_MAX - ObTimeUtility::current_time()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid connectivity check argument", KR(ret), K(timeout_us), K(storage_dest.get_root_path()));
  } else if (OB_FAIL(storage_info_.assign(*storage_dest.get_storage_info()))) {
    LOG_WARN("failed to copy connectivity storage info", KR(ret), K(storage_dest.get_root_path()));
  } else if (OB_FAIL(databuff_printf(root_, sizeof(root_), "%.*s/io_semantics_probe/%ld_%ld",
      storage_dest.get_root_path().length(), storage_dest.get_root_path().ptr(),
      ObTimeUtility::current_time(), ObRandom::rand(0, INT64_MAX)))) {
    LOG_WARN("failed to build connectivity probe root", KR(ret), K(storage_dest.get_root_path()));
  } else {
    deadline_ts_us_ = ObTimeUtility::current_time() + timeout_us;
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_deadline_() const
{
  return (ObTimeUtility::current_time() >= deadline_ts_us_) ? OB_TIMEOUT : OB_SUCCESS;
}

int ObObjectStorageConnectivityCheck::build_path_(
    const char *name,
    char *path,
    const int64_t path_len) const
{
  return databuff_printf(path, path_len, "%s/%s", root_, name);
}

int ObObjectStorageConnectivityCheck::record_file_(const char *name)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH] = {0};
  ObString owned_path;
  if (OB_FAIL(build_path_(name, path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(ob_write_string(allocator_, ObString::make_string(path), owned_path))) {
    LOG_WARN("failed to copy connectivity probe object path", KR(ret), K(path));
  } else if (OB_FAIL(created_files_.push_back(owned_path))) {
    LOG_WARN("failed to record connectivity probe object", KR(ret), K(path));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::write_file_(
    const char *name,
    const char *data,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH] = {0};
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity PUT check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_FAIL(build_path_(name, path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(record_file_(name))) {
    LOG_WARN("failed to register connectivity probe object", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::mk_parent_dir(path, &storage_info_))) {
    LOG_WARN("failed to create connectivity PUT parent directory", KR(ret), K(path));
  } else if (OB_FAIL(ObBackupIoAdapter::write_single_file(path, &storage_info_, data, size,
      ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to write connectivity PUT object", KR(ret), K(path), K(size));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::verify_file_(
    const char *name,
    const char *data,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH] = {0};
  int64_t length = 0;
  int64_t read_size = 0;
  char *read_buf = nullptr;
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity readback check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_FAIL(build_path_(name, path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_get_file_length(path, &storage_info_, length))) {
    LOG_WARN("failed to get connectivity object length", KR(ret), K(path));
  } else if (length != size) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("connectivity object length mismatch", KR(ret), K(path), K(size), K(length));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator_.alloc(size + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate connectivity read buffer", KR(ret), K(path), "buffer_size", size + 1);
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_read_single_file(
      path, &storage_info_, read_buf, size + 1, read_size, ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to read connectivity probe object", KR(ret), K(path), K(size), K(read_size));
  } else if (read_size != size || 0 != MEMCMP(read_buf, data, size)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("connectivity readback content mismatch", KR(ret), K(path), K(size), K(read_size));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::verify_list_(const char *name, const bool is_expected)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> names;
  ObFileListArrayOp op(names, allocator_);
  bool is_found = false;
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity LIST check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_list_files(root_, &storage_info_, op))) {
    LOG_WARN("failed to list connectivity probe objects", KR(ret), K_(root));
  } else {
    for (int64_t i = 0; i < names.count(); ++i) {
      if (names.at(i) == ObString::make_string(name)) {
        is_found = true;
      }
    }
    if (is_found != is_expected) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity object LIST visibility mismatch", KR(ret), KCSTRING(name), K(is_expected), K(is_found),
          K(names), K_(root));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("connectivity LIST mismatch", K(ret), KCSTRING(name), K(is_expected), K(names), K_(root));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::verify_absent_(const char *name)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH] = {0};
  char buf[1];
  int64_t length = 0;
  int64_t read_size = 0;
  if (OB_FAIL(verify_list_(name, false))) {
    LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(build_path_(name, path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), KCSTRING(name), K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(path, &storage_info_, length))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check deleted connectivity object metadata", KR(ret), K(path));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deleted connectivity object remains visible in metadata", KR(ret), K(path), K(length));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupIoAdapter::read_single_file(path, &storage_info_, buf, sizeof(buf),
      read_size, ObStorageIdMod::get_default_id_mod()))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("deleted connectivity object read did not return not-exist", KR(ret), K(path), K(read_size));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deleted connectivity object remains visible in metadata", KR(ret), K(path), K(read_size));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_prefix_()
{
  int ret = OB_SUCCESS;
  char prefix[OB_MAX_URI_LENGTH];
  ObArray<ObString> names;
  ObFileListArrayOp op(names, allocator_);
  const char data[] = "prefix probe";
  if (OB_FAIL(write_file_("prefix/a", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "prefix/a", K_(root));
  } else if (OB_FAIL(write_file_("prefix/b", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "prefix/b", K_(root));
  } else if (OB_FAIL(write_file_("prefix_other/c", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "prefix_other/c", K_(root));
  } else if (OB_FAIL(build_path_("prefix", prefix, sizeof(prefix)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "prefix", K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::list_files(prefix, &storage_info_, op))) {
    LOG_WARN("failed to list connectivity prefix", KR(ret), K(prefix));
  } else if (2 != names.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connectivity prefix object count mismatch", KR(ret), K(prefix), K(names));
  } else {
    bool has_a = false;
    bool has_b = false;
    for (int64_t i = 0; i < names.count(); ++i) {
      has_a |= names.at(i) == ObString::make_string("a");
      has_b |= names.at(i) == ObString::make_string("b");
    }
    if (!has_a || !has_b) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity prefix contents mismatch", KR(ret), K(prefix), K(has_a), K(has_b), K(names));
    }
  }
  if (OB_SUCC(ret)) {
    names.reuse();
    if (OB_FAIL(ObBackupIoAdapter::list_directories(root_, &storage_info_, op))) {
      LOG_WARN("failed to list connectivity probe directories", KR(ret), K_(root));
    } else if (2 != names.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity directory count mismatch", KR(ret), K_(root), K(names));
    } else {
      bool has_prefix = false;
      bool has_other = false;
      for (int64_t i = 0; i < names.count(); ++i) {
        has_prefix |= names.at(i) == ObString::make_string("prefix");
        has_other |= names.at(i) == ObString::make_string("prefix_other");
      }
      if (!has_prefix || !has_other) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("connectivity directory contents mismatch", KR(ret), K_(root), K(has_prefix), K(has_other), K(names));
      }
    }
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_multipart_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
  const int64_t part_size = 6L * 1024 * 1024;
  const int64_t tail_size = 64;
  char *data = static_cast<char *>(allocator_.alloc(part_size + tail_size));
  char path[OB_MAX_URI_LENGTH] = {0};
  ObIODevice *device = nullptr;
  ObIOFd fd;
  int64_t written = 0;
  bool is_opened = false;
  bool is_completed = false;
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity multipart check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_ISNULL(data)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate connectivity multipart buffer", KR(ret), K(part_size), K(tail_size), KP(data));
  } else if (OB_FAIL(build_path_("multipart", path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "multipart", K_(root));
  } else if (OB_FAIL(record_file_("multipart"))) {
    LOG_WARN("failed to register connectivity probe object", KR(ret), "object", "multipart", K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(
      device, fd, &storage_info_, path, OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER,
      ObStorageIdMod::get_default_id_mod(), false/*is_batch_write*/))) {
    LOG_WARN("failed to open connectivity multipart device", KR(ret), K(path));
  } else if (FALSE_IT(is_opened = true)) {
  } else {
    MEMSET(data, 'm', part_size);
    MEMSET(data + part_size, 'n', tail_size);
    if (OB_FAIL(device->upload_part(fd, data, part_size, 1, written))) {
      LOG_WARN("failed to upload connectivity multipart part", KR(ret), K(path), "part_id", 1, K(written));
    } else if (written != part_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity multipart part length mismatch", KR(ret), K(path), "part_id", 1,
          "expected_size", part_size, K(written));
    } else if (OB_FAIL(device->upload_part(fd, data + part_size, tail_size, 2, written))) {
      LOG_WARN("failed to upload connectivity multipart part", KR(ret), K(path), "part_id", 2, K(written));
    } else if (written != tail_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity multipart part length mismatch", KR(ret), K(path), "part_id", 2,
          "expected_size", tail_size, K(written));
    } else if (OB_FAIL(verify_list_("multipart", false))) {
      LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "multipart",
          K_(root));
    }
  }
  log_case_("C04 multipart before complete", ret);
  if (OB_SUCC(ret)) {
    case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
    if (OB_FAIL(check_deadline_())) {
      LOG_WARN("connectivity multipart check timed out", KR(ret), K_(root), K(deadline_ts_us_));
    } else if (OB_FAIL(device->complete(fd))) {
      LOG_WARN("failed to complete connectivity multipart upload", KR(ret), K(path));
    } else if (FALSE_IT(is_completed = true)) {
    } else if (OB_FAIL(verify_list_("multipart", true))) {
      LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "multipart",
          K_(root));
    } else if (OB_FAIL(verify_file_("multipart", data, part_size + tail_size))) {
      LOG_WARN("failed to verify readback of connectivity probe object", KR(ret), "object", "multipart", K_(root));
    }
    log_case_("C05 multipart after complete", ret);
  } else {
    LOG_INFO("multipart completion check not executed", "case", "C05 multipart after complete", K(ret), K_(root));
  }
  if (is_opened && !is_completed && OB_TMP_FAIL(device->abort(fd))) {
    LOG_WARN("failed to abort connectivity multipart upload", KR(tmp_ret), K(path));
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(device, fd))) {
    LOG_WARN("failed to close connectivity multipart device", KR(tmp_ret), K(path));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_abort_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t size = 6L * 1024 * 1024;
  char *data = static_cast<char *>(allocator_.alloc(size));
  char path[OB_MAX_URI_LENGTH] = {0};
  ObIODevice *device = nullptr;
  ObIOFd fd;
  int64_t written = 0;
  bool is_opened = false;
  bool is_aborted = false;
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity multipart abort check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_ISNULL(data)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate connectivity abort buffer", KR(ret), K(size));
  } else if (OB_FAIL(build_path_("abort", path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "abort", K_(root));
  } else if (OB_FAIL(record_file_("abort"))) {
    LOG_WARN("failed to register connectivity probe object", KR(ret), "object", "abort", K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(
      device, fd, &storage_info_, path, OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER,
      ObStorageIdMod::get_default_id_mod(), false/*is_batch_write*/))) {
    LOG_WARN("failed to open connectivity multipart abort device", KR(ret), K(path));
  } else if (FALSE_IT(is_opened = true)) {
  } else {
    MEMSET(data, 'a', size);
    if (OB_FAIL(device->upload_part(fd, data, size, 1, written))) {
      LOG_WARN("failed to upload connectivity multipart abort part", KR(ret), K(path), "part_id", 1, K(written));
    } else if (written != size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity abort part length mismatch", KR(ret), K(path), K(size), K(written));
    } else if (OB_FAIL(device->abort(fd))) {
      LOG_WARN("failed to abort connectivity multipart upload", KR(ret), K(path));
    } else {
      is_aborted = true;
      // The abort probe is the last registered object and has no completed object to delete.
      created_files_.pop_back();
      if (OB_FAIL(verify_list_("abort", false))) {
        LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "abort", K_(root));
      }
    }
  }
  if (is_opened && !is_aborted) {
    if (OB_TMP_FAIL(device->abort(fd))) {
      LOG_WARN("failed to clean up is_aborted connectivity upload", KR(tmp_ret), K(path));
      ret = COVER_SUCC(tmp_ret);
    } else {
      created_files_.pop_back();
    }
  }
  if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(device, fd))) {
    LOG_WARN("failed to close connectivity multipart abort device", KR(tmp_ret), K(path));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

bool ObObjectStorageConnectivityCheck::can_delete_() const
{
  return !storage_info_.is_enable_worm()
      || ObStorageDeleteMode::STORAGE_TAGGING_MODE == storage_info_.get_delete_mode();
}

int ObObjectStorageConnectivityCheck::check_delete_()
{
  int ret = OB_SUCCESS;
  const char data[] = "delete probe";
  char path[OB_MAX_URI_LENGTH] = {0};
  bool is_tagged = false;
  if (OB_FAIL(write_file_("delete", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "delete", K_(root));
  } else if (OB_FAIL(verify_list_("delete", true))) {
    LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "delete", K_(root));
  } else if (OB_FAIL(build_path_("delete", path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "delete", K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::del_file(path, &storage_info_))) {
    LOG_WARN("failed to delete connectivity probe object", KR(ret), K(path));
  } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == storage_info_.get_delete_mode()) {
    if (OB_FAIL(ObBackupIoAdapter::is_tagging(path, &storage_info_, is_tagged))) {
      LOG_WARN("failed to get connectivity object deletion tag", KR(ret), "path", path);
    } else if (!is_tagged) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connectivity DELETE did not set deletion tag", KR(ret), K(path));
    }
  } else if (OB_FAIL(verify_absent_("delete"))) {
    LOG_WARN("failed to verify deletion of connectivity probe object", KR(ret), "object", "delete", K_(root));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_batch_delete_()
{
  int ret = OB_SUCCESS;
  const char data[] = "batch delete probe";
  ObArray<ObString> files;
  ObArray<int64_t> failed_indices;
  char paths[2][OB_MAX_URI_LENGTH];
  if (OB_FAIL(write_file_("batch_a", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "batch_a", K_(root));
  } else if (OB_FAIL(write_file_("batch_b", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "batch_b", K_(root));
  } else if (OB_FAIL(write_file_("batch_keep", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "batch_keep", K_(root));
  } else if (OB_FAIL(build_path_("batch_a", paths[0], sizeof(paths[0])))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "batch_a", K_(root));
  } else if (OB_FAIL(build_path_("batch_b", paths[1], sizeof(paths[1])))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "batch_b", K_(root));
  } else if (OB_FAIL(files.push_back(ObString::make_string(paths[0])))) {
    LOG_WARN("failed to add connectivity batch DELETE object", KR(ret), "path", paths[0]);
  } else if (OB_FAIL(files.push_back(ObString::make_string(paths[1])))) {
    LOG_WARN("failed to add connectivity batch DELETE object", KR(ret), "path", paths[1]);
  } else if (OB_FAIL(ObBackupIoAdapter::batch_del_files(&storage_info_, files, failed_indices))) {
    LOG_WARN("failed to batch delete connectivity probe objects", KR(ret), K(files), K(failed_indices), K_(root));
  } else if (!failed_indices.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connectivity batch DELETE returned failed objects", K(ret), K(failed_indices), K(files));
  } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == storage_info_.get_delete_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      bool is_tagged = false;
      if (OB_FAIL(ObBackupIoAdapter::is_tagging(files.at(i), &storage_info_, is_tagged))) {
        LOG_WARN("failed to get connectivity object deletion tag", KR(ret), "path", files.at(i));
      } else if (!is_tagged) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("connectivity batch DELETE did not set deletion tag", KR(ret), "path", files.at(i));
      }
    }
  } else if (OB_FAIL(verify_absent_("batch_a"))) {
    LOG_WARN("failed to verify deletion of connectivity probe object", KR(ret), "object", "batch_a", K_(root));
  } else if (OB_FAIL(verify_absent_("batch_b"))) {
    LOG_WARN("failed to verify deletion of connectivity probe object", KR(ret), "object", "batch_b", K_(root));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(verify_list_("batch_keep", true))) {
      LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "batch_keep",
          K_(root));
    } else if (OB_FAIL(verify_file_("batch_keep", data, sizeof(data)))) {
      LOG_WARN("failed to verify readback of connectivity probe object", KR(ret), "object", "batch_keep", K_(root));
    } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == storage_info_.get_delete_mode()) {
      char path[OB_MAX_URI_LENGTH] = {0};
      bool is_tagged = false;
      if (OB_FAIL(build_path_("batch_keep", path, sizeof(path)))) {
        LOG_WARN("failed to build connectivity object path", KR(ret), "object", "batch_keep", K_(root));
      } else if (OB_FAIL(ObBackupIoAdapter::is_tagging(path, &storage_info_, is_tagged))) {
        LOG_WARN("failed to get connectivity object deletion tag", KR(ret), "path", path);
      } else if (is_tagged) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("connectivity batch DELETE tagged unselected object", KR(ret), K(path));
      }
    }
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_appender_()
{
  int ret = OB_SUCCESS;
  const char part[] = "append probe";
  const int64_t size = STRLEN(part);
  char expected[2 * sizeof(part)] = {0};
  char path[OB_MAX_URI_LENGTH] = {0};
  int64_t written = 0;
  MEMCPY(expected, part, size);
  MEMCPY(expected + size, part, size);
  if (OB_FAIL(check_deadline_())) {
    LOG_WARN("connectivity APPEND check timed out", KR(ret), K_(root), K(deadline_ts_us_));
  } else if (OB_FAIL(build_path_("append", path, sizeof(path)))) {
    LOG_WARN("failed to build connectivity object path", KR(ret), "object", "append", K_(root));
  } else if (OB_FAIL(record_file_("append"))) {
    LOG_WARN("failed to register connectivity probe object", KR(ret), "object", "append", K_(root));
  } else if (OB_FAIL(ObBackupIoAdapter::mk_parent_dir(path, &storage_info_))) {
    LOG_WARN("failed to create connectivity APPEND parent directory", KR(ret), K(path));
  } else if (OB_FAIL(ObBackupIoAdapter::pwrite(
      path, &storage_info_, part, 0, size, OB_STORAGE_ACCESS_APPENDER,
      written, false/*is_can_seal*/, ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to append connectivity probe object", KR(ret), K(path), "offset", 0, K(size), K(written));
  } else if (written != size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connectivity APPEND length mismatch", KR(ret), K(path), "offset", 0, K(size), K(written));
  } else if (OB_FAIL(ObBackupIoAdapter::pwrite(
      path, &storage_info_, part, size, size, OB_STORAGE_ACCESS_APPENDER,
      written, false/*is_can_seal*/, ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to append connectivity probe object", KR(ret), K(path), "offset", size, K(size), K(written));
  } else if (written != size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connectivity APPEND length mismatch", KR(ret), K(path), "offset", size, K(size), K(written));
  } else if (OB_FAIL(ObBackupIoAdapter::seal_file(
      path, &storage_info_, ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to seal connectivity APPEND object", KR(ret), K(path));
  } else if (OB_FAIL(verify_list_("append", true))) {
    LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "append", K_(root));
  } else if (OB_FAIL(verify_file_("append", expected, 2 * size))) {
    LOG_WARN("failed to verify readback of connectivity probe object", KR(ret), "object", "append", K_(root));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::cleanup_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (can_delete_()) {
    for (int64_t i = 0; i < created_files_.count(); ++i) {
      if (OB_TMP_FAIL(ObBackupIoAdapter::adaptively_del_file(
          created_files_.at(i), &storage_info_))) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to clean connectivity probe object", KR(tmp_ret), "path", created_files_.at(i));
      }
    }
  } else {
    LOG_INFO("connectivity probe objects retained by WORM policy", K_(root));
  }
  return ret;
}

int ObObjectStorageConnectivityCheck::check_basic(
    const ObBackupDest &storage_dest,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const char data[] = "basic connectivity probe";
  if (OB_FAIL(prepare_(storage_dest, timeout_us))) {
    LOG_WARN("failed to prepare connectivity basic check", KR(ret), K(storage_dest.get_root_path()), K(timeout_us));
  } else if (OB_FAIL(write_file_("basic", data, sizeof(data)))) {
    LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "basic", K_(root));
  } else if (OB_FAIL(verify_file_("basic", data, sizeof(data)))) {
    LOG_WARN("failed to verify readback of connectivity probe object", KR(ret), "object", "basic", K_(root));
  } else if (OB_FAIL(check_appender_())) {
    LOG_WARN("connectivity basic APPEND check failed", KR(ret), K_(root));
  }
  if (OB_TMP_FAIL(cleanup_())) {
    LOG_WARN("failed to clean up connectivity basic check", KR(tmp_ret), K_(root));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

void ObObjectStorageConnectivityCheck::log_case_(const char *case_name, const int case_ret)
{
  const int64_t elapsed_us = ObTimeUtility::current_monotonic_time() - case_start_ts_us_;
  if (OB_SUCCESS == case_ret) {
    LOG_INFO("shared storage connectivity case finished", "case", case_name, K(case_ret), K(elapsed_us), K_(root));
  } else {
    if (nullptr == failed_case_) {
      failed_case_ = case_name;
    }
    const int ret = case_ret;
    LOG_WARN("shared storage connectivity case failed", "case", case_name, KR(ret), K(elapsed_us), K_(root));
  }
}

int ObObjectStorageConnectivityCheck::check_shared_storage(
    const ObBackupDest &storage_dest,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const char data[] = "shared storage probe";
  if (OB_FAIL(prepare_(storage_dest, timeout_us))) {
    failed_case_ = "Prepare shared storage check";
    LOG_WARN("failed to prepare connectivity shared storage check", KR(ret), K(storage_dest.get_root_path()),
        K(timeout_us));
  } else if (OB_STORAGE_FILE == storage_info_.get_type()) {
    LOG_INFO("shared storage connectivity skipped for FILE", K_(root));
  } else {
    case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
    if (OB_FAIL(write_file_("put", data, sizeof(data)))) {
      LOG_WARN("failed to write connectivity probe object", KR(ret), "object", "put", K_(root));
    } else if (OB_FAIL(verify_list_("put", true))) {
      LOG_WARN("failed to verify LIST visibility of connectivity probe object", KR(ret), "object", "put", K_(root));
    }
    log_case_("C01 PUT then LIST", ret);
    if (OB_SUCC(ret)) {
      case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
      if (OB_FAIL(verify_file_("put", data, sizeof(data)))) {
        LOG_WARN("failed to verify readback of connectivity probe object", KR(ret), "object", "put", K_(root));
      }
      log_case_("C02 PUT readback", ret);
    }
    if (OB_SUCC(ret)) {
      case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
      if (OB_FAIL(check_prefix_())) {
        LOG_WARN("failed to check prefix isolation of connectivity probe object", KR(ret), K_(root));
      }
      log_case_("C03 prefix isolation", ret);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_multipart_())) {
        LOG_WARN("connectivity shared storage multipart check failed", KR(ret), K_(root));
      }
    }
    if (OB_SUCC(ret)) {
      case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
      if (OB_FAIL(check_abort_())) {
        LOG_WARN("connectivity shared storage multipart abort check failed", KR(ret), K_(root));
      }
      log_case_("C06 multipart abort", ret);
    }
    if (OB_SUCC(ret)) {
      if (!can_delete_()) {
        LOG_INFO("physical DELETE checks skipped by WORM policy", "cases", "C07, C08", K_(root));
      } else {
        case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
        if (OB_FAIL(check_delete_())) {
          LOG_WARN("connectivity shared storage DELETE check failed", KR(ret), K_(root));
        }
        log_case_("C07 DELETE visibility", ret);
        if (OB_SUCC(ret)) {
          case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
          if (OB_FAIL(check_batch_delete_())) {
            LOG_WARN("connectivity shared storage batch DELETE check failed", KR(ret), K_(root));
          }
          log_case_("C08 batch DELETE visibility", ret);
        }
      }
    }
    if (OB_SUCC(ret)) {
      case_start_ts_us_ = ObTimeUtility::current_monotonic_time();
      if (OB_FAIL(check_appender_())) {
        LOG_WARN("connectivity shared storage APPEND check failed", KR(ret), K_(root));
      }
      log_case_("C10 APPEND readback", ret);
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(cleanup_())) {
    if (OB_SUCC(ret)) {
      failed_case_ = "Cleanup probe files";
    }
    ret = COVER_SUCC(tmp_ret);
    LOG_WARN("failed to clean up connectivity shared storage check", KR(tmp_ret), KR(ret), K_(root));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

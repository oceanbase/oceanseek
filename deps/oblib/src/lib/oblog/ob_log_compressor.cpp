/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB
#include <dirent.h>
#include <regex.h>
#include <utime.h>
#include <fcntl.h>
#include <limits.h>
#include <algorithm>
#include "lib/time/ob_time_utility.h"

#include "lib/oblog/ob_log_compressor.h"
#include "lib/thread/thread_mgr.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_se_array.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {

namespace {
int open_syslog_parent_directory(const char *file_name)
{
  int directory = -1;
  bool valid_parent = true;
  char parent[OB_MAX_SYSLOG_FILE_NAME_SIZE];
  const char *slash = strrchr(file_name, '/');
  if (NULL == slash) {
    snprintf(parent, sizeof(parent), ".");
  } else if (slash == file_name) {
    snprintf(parent, sizeof(parent), "/");
  } else {
    const int length = snprintf(parent, sizeof(parent), "%.*s", static_cast<int>(slash - file_name), file_name);
    if (length < 0 || length >= sizeof(parent)) {
      errno = ENAMETOOLONG;
      valid_parent = false;
    }
  }
  if (valid_parent) {
    directory = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  }
  return directory;
}
}  // namespace

ObLogCompressor::ObLogCompressor()
    : config_update_mutex_(ObLatchIds::CONFIG_LOCK),
      config_paused_(false),
      cycle_running_(false),
      libpalf_compression_enabled_(false),
      config_applied_(false),
      process_start_time_(ObTimeUtility::current_time()),
      temporary_file_sequence_(0),
      is_inited_(false),
      stopped_(true),
      loop_interval_(OB_SYSLOG_COMPRESS_LOOP_INTERVAL),
      max_disk_size_(0),
      min_uncompressed_count_(0),
      syslog_dir_(),
      alert_log_dir_(),
      compress_func_(NONE_COMPRESSOR),
      compressor_(NULL),
      next_compressor_(NULL),
      oldest_files_(cmp_, NULL)
{
}

bool ObSyslogConfig::is_valid() const
{
  return max_file_count_ >= 0 && max_disk_size_ >= 0 && min_uncompressed_count_ >= 0
         && (compress_func_ == NONE_COMPRESSOR || compress_func_ == ZSTD_COMPRESSOR
             || compress_func_ == ZSTD_1_3_8_COMPRESSOR);
}

void ObSyslogConfig::normalize()
{
  max_file_count_ = std::min<int64_t>(max_file_count_, ObLogger::MAX_LOG_FILE_COUNT);
  if (max_disk_size_ > 0) {
    max_disk_size_ = std::max(max_disk_size_, OB_MIN_SYSLOG_DISK_SIZE);
  }
}

bool ObSyslogConfig::operator==(const ObSyslogConfig &other) const
{
  return max_file_count_ == other.max_file_count_ && record_old_log_file_ == other.record_old_log_file_
         && max_disk_size_ == other.max_disk_size_ && compress_func_ == other.compress_func_
         && min_uncompressed_count_ == other.min_uncompressed_count_ && enable_libpalf_ == other.enable_libpalf_;
}

int ObSyslogCompareFunctor::compare_archive_names(const char *left, const char *right)
{
  char left_archive[OB_MAX_SYSLOG_FILE_NAME_SIZE];
  char right_archive[OB_MAX_SYSLOG_FILE_NAME_SIZE];
  snprintf(left_archive, sizeof(left_archive), "%s", left);
  snprintf(right_archive, sizeof(right_archive), "%s", right);
  const size_t left_length = strlen(left_archive);
  const size_t right_length = strlen(right_archive);
  if (left_length >= 4 && 0 == strcmp(left_archive + left_length - 4, ".zst")) {
    left_archive[left_length - 4] = '\0';
  }
  if (right_length >= 4 && 0 == strcmp(right_archive + right_length - 4, ".zst")) {
    right_archive[right_length - 4] = '\0';
  }
  const int order = strverscmp(left_archive, right_archive);
  return order == 0 ? strverscmp(left, right) : order;
}

ObSyslogConfig ObLogCompressor::get_config(const ObLogger &logger) const
{
  ObSyslogConfig config;
  config.max_file_count_ = logger.get_max_file_index();
  config.record_old_log_file_ = logger.get_record_old_log_file();
  config.max_disk_size_ = get_max_disk_size();
  config.compress_func_ = get_compress_func();
  config.min_uncompressed_count_ = get_min_uncompressed_count();
  config.enable_libpalf_ = is_libpalf_compression_enabled();
  return config;
}

bool ObLogCompressor::is_config_applied(const ObLogger &logger, const ObSyslogConfig &config) const
{
  ObSyslogConfig normalized = config;
  normalized.normalize();
  return config_applied_ && logger.log_compressor_ == this && config.is_valid() && get_config(logger) == normalized;
}

int ObLogCompressor::apply_config(ObLogger &logger, const ObSyslogConfig &config)
{
  int ret = OB_SUCCESS;
  ObSyslogConfig target = config;
  ObCompressor *prepared = NULL;
  if (!target.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_ && !ATOMIC_LOAD(&config_paused_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (target.compress_func_ != NONE_COMPRESSOR
             && OB_FAIL(ObCompressorPool::get_instance().get_compressor(target.compress_func_, prepared))) {
  } else {
    target.normalize();
    const ObSyslogConfig previous = get_config(logger);
    const bool scan_history = target.record_old_log_file_ && target.max_file_count_ > 0
                              && (!config_applied_ || target.max_file_count_ != previous.max_file_count_
                                  || target.record_old_log_file_ != previous.record_old_log_file_
                                  || target.compress_func_ != previous.compress_func_);
    ObSEArray<ObLogger::FileName, 4> files;
    ObSEArray<ObLogger::FileName, 4> wf_files;
    bool rotation_locked = false;
    if (scan_history) {
      if (0 != pthread_mutex_lock(&logger.file_size_mutex_)) {
        ret = OB_ERR_SYS;
      } else {
        rotation_locked = true;
        // Rotation takes file_size_mutex_ before file_index_mutex_. Keep that order,
        // and exclude its rename/queue updates until this snapshot has been committed.
        ret = logger.collect_log_files_(&files, &wf_files, target.compress_func_ == NONE_COMPRESSOR);
      }
    }
    if (OB_FAIL(ret)) {
      // Preparation has not changed either the live policy or the history queues.
    } else if (0 != pthread_mutex_lock(&logger.file_index_mutex_)) {
      ret = OB_ERR_SYS;
    } else {
      // The rotation writer observes mode and count under this same lock.
      // Do not call logging, FFI or historical scanning while holding it.
      // log_compressor_ is bound during initialization. Do not rewrite it here:
      // rotate_log() reads it without holding file_index_mutex_.
      ATOMIC_STORE(&logger.max_file_index_, target.max_file_count_);
      ATOMIC_STORE(&logger.rec_old_file_flag_, target.record_old_log_file_);
      ATOMIC_STORE(&max_disk_size_, target.max_disk_size_);
      ATOMIC_STORE(&min_uncompressed_count_, target.min_uncompressed_count_);
      ATOMIC_STORE(&next_compressor_, prepared);
      ATOMIC_STORE(&compress_func_, target.compress_func_);
      ATOMIC_STORE(&libpalf_compression_enabled_, target.enable_libpalf_);
      if (!is_inited_) {
        compressor_ = prepared;
      }
      if (scan_history) {
        logger.replace_log_file_lists_(&files, &wf_files);
      }
      config_applied_ = true;
      (void)pthread_mutex_unlock(&logger.file_index_mutex_);
    }
    if (rotation_locked) {
      (void)pthread_mutex_unlock(&logger.file_size_mutex_);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to prepare syslog policy", K(ret));
    }
  }
  return ret;
}

ObLogCompressor::~ObLogCompressor()
{
  if (is_inited_) {
    destroy();
  }
}

int ObLogCompressor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("the ObLogCompressor has been inited", K(ret));
  } else if (OB_FAIL(log_compress_cond_.init(ObWaitEventIds::LOG_COMPRESSOR_COND_WAIT))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("failed to init ObThreadCond", K(ret));
  } else {
    snprintf(syslog_dir_, sizeof(syslog_dir_), "%s", OB_SYSLOG_DIR);
    snprintf(alert_log_dir_, sizeof(alert_log_dir_), "%s", OB_ALERT_LOG_DIR);
    stopped_ = false;
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(TGDefIDs::SYSLOG_COMPRESS, *this))) {
      LOG_ERROR("failed to start log compression thread", K(ret));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      LOG_ERROR("syslog compressor init failed ", K(ret));
      stopped_ = true;
      log_compress_cond_.destroy();
    }
  }
  LOG_INFO("syslog compressor init finish ", K(ret));

  return ret;
}

void ObLogCompressor::stop()
{
  if (is_inited_) {
    ATOMIC_STORE(&stopped_, true);
    awake();
    TG_STOP(TGDefIDs::SYSLOG_COMPRESS);
  }
}

void ObLogCompressor::wait()
{
  if (is_inited_) {
    TG_WAIT(TGDefIDs::SYSLOG_COMPRESS);
  }
}

void ObLogCompressor::awake()
{
  if (log_compress_cond_.is_inited()) {
    ObThreadCondGuard guard(log_compress_cond_);
    log_compress_cond_.broadcast();
  }
}

void ObLogCompressor::destroy()
{
  if (is_inited_) {
    {
      ObThreadCondGuard guard(log_compress_cond_);
      ATOMIC_STORE(&stopped_, true);
      log_compress_cond_.broadcast();
    }
    stop();
    wait();
    log_compress_cond_.destroy();
    max_disk_size_ = 0;
    compress_func_ = NONE_COMPRESSOR;
    min_uncompressed_count_ = 0;
    compressor_ = NULL;
    ATOMIC_STORE(&next_compressor_, static_cast<ObCompressor *>(NULL));
    is_inited_ = false;
    config_paused_ = false;
    cycle_running_ = false;
    libpalf_compression_enabled_ = false;
    config_applied_ = false;
    LOG_INFO("syslog compressor destroyed");
  }
}

int ObLogCompressor::set_max_disk_size(int64_t max_disk_size)
{
  int ret = OB_SUCCESS;
  if (max_disk_size < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (max_disk_size > 0 && max_disk_size < OB_MIN_SYSLOG_DISK_SIZE) {
      max_disk_size = OB_MIN_SYSLOG_DISK_SIZE;
    }
    const int64_t previous = get_max_disk_size();
    if (previous != max_disk_size) {
      config_applied_ = false;
    }
    if ((previous == 0 && max_disk_size > 0) || (previous > 0 && previous > max_disk_size)) {
      ATOMIC_STORE(&max_disk_size_, max_disk_size);
      awake();
    } else {
      ATOMIC_STORE(&max_disk_size_, max_disk_size);
    }
  }
  return ret;
}

int ObLogCompressor::set_compress_func(const char *compress_func_ptr)
{
  int ret = OB_SUCCESS;
  ObCompressorType new_compress_func;
  if (OB_ISNULL(compress_func_ptr)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(compress_func_ptr, new_compress_func))) {
    // do nothing
  } else if (new_compress_func == NONE_COMPRESSOR || new_compress_func == ZSTD_COMPRESSOR
             || new_compress_func == ZSTD_1_3_8_COMPRESSOR) {
    // to do: support ZLIB_COMPRESSOR
    if (new_compress_func != get_compress_func()) {
      config_applied_ = false;
      LOG_INFO("modify log compress func", K(compress_func_), K(new_compress_func));
      if (OB_FAIL(set_next_compressor_(new_compress_func))) {
        LOG_ERROR("fail to modify log compress func", K(ret), K(compress_func_), K(new_compress_func));
      } else if (get_compress_func() == NONE_COMPRESSOR && new_compress_func != NONE_COMPRESSOR) {
        ATOMIC_STORE(&compress_func_, new_compress_func);
        // from disable to enanble, awake compress thread
        awake();
      } else {
        ATOMIC_STORE(&compress_func_, new_compress_func);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObLogCompressor::set_min_uncompressed_count(int64_t min_uncompressed_count)
{
  int ret = OB_SUCCESS;
  if (min_uncompressed_count < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (get_min_uncompressed_count() != min_uncompressed_count) {
      config_applied_ = false;
    }
    ATOMIC_STORE(&min_uncompressed_count_, min_uncompressed_count);
  }
  return ret;
}

bool ObLogCompressor::is_compressed_file(const char *file)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  const ObLibPalfSyslogFileType libpalf_type = classify_libpalf_file(file);
  regex_t regex;
  if (libpalf_type != NOT_LIBPALF_FILE) {
    is_compressed = libpalf_type == LIBPALF_COMPRESSED_FILE;
  } else if (OB_ISNULL(file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumet for file is null", K(ret));
  } else if (OB_FAIL(regcomp(&regex, OB_COMPRESSED_SYSLOG_FILE_PATTERN, REG_EXTENDED))) {
    LOG_ERROR("failed to compile regex pattern", K(ret));
  } else {
    if (regexec(&regex, file, 0, NULL, 0) == 0) {
      is_compressed = true;
    }
    regfree(&regex);
  }
  return is_compressed;
}

void ObLogCompressor::run1()
{
  lib::set_thread_name("SyslogCompress");
  LOG_INFO("syslog compress thread start");
  log_compress_loop_();
  LOG_INFO("syslog compress thread finish");
}

int ObLogCompressor::pause_for_config_update()
{
  int ret = OB_SUCCESS;
  if (!log_compress_cond_.is_inited()) {
    ATOMIC_STORE(&config_paused_, true);
  } else {
    ObThreadCondGuard guard(log_compress_cond_);
    ATOMIC_STORE(&config_paused_, true);
    log_compress_cond_.broadcast();
    const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000;
    while (cycle_running_ && OB_SUCC(ret)) {
      if (ObTimeUtility::current_time() >= deadline) {
        ret = OB_TIMEOUT;
      } else {
        log_compress_cond_.wait_us(100 * 1000);
      }
    }
    if (OB_FAIL(ret)) {
      ATOMIC_STORE(&config_paused_, false);
      log_compress_cond_.broadcast();
    }
  }
  return ret;
}

void ObLogCompressor::resume_after_config_update()
{
  if (!log_compress_cond_.is_inited()) {
    ATOMIC_STORE(&config_paused_, false);
  } else {
    ObThreadCondGuard guard(log_compress_cond_);
    ATOMIC_STORE(&config_paused_, false);
    log_compress_cond_.broadcast();
  }
}

bool ObLogCompressor::same_file_(const ObSyslogFile &file, const struct stat &st) const
{
  return S_ISREG(st.st_mode) && file.device_ == st.st_dev && file.inode_ == st.st_ino && file.size_ == st.st_size
         && file.mtime_ == st.st_mtim.tv_sec * 1000000000L + st.st_mtim.tv_nsec;
}

int64_t ObLogCompressor::get_effective_remaining_size_(int64_t total_size, int64_t max_disk_size, bool size_complete)
{
  int64_t remaining = get_disk_remaining_size_();
  remaining = remaining >= 0 ? remaining : INT64_MAX;
  if (size_complete && max_disk_size > 0) {
    remaining = std::min(remaining, max_disk_size - total_size);
  }
  return remaining;
}

int ObLogCompressor::scan_log_files_(bool enable_delete, bool enable_libpalf_compression, bool cleanup_temporary,
                                     ObSyslogFile compress_files[], int64_t log_file_count[], int64_t &total_size,
                                     ObSyslogScanStatus *status, bool cleanup_conflicts)
{
  int ret = OB_SUCCESS;
  ObSyslogScanStatus local_status;
  ObSyslogScanStatus &scan_status = NULL == status ? local_status : *status;
  scan_status = ObSyslogScanStatus();
  regex_t archive_regex;
  regex_t raw_regex;
  total_size = 0;
  oldest_files_.reset();
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; ++i) {
    compress_files[i].reset();
    log_file_count[i] = 0;
  }
  if (0 != regcomp(&archive_regex, OB_ARCHIVED_SYSLOG_FILE_PATTERN, REG_EXTENDED)) {
    ret = OB_ERR_SYS;
  } else if (0 != regcomp(&raw_regex, OB_UNCOMPRESSED_SYSLOG_FILE_PATTERN, REG_EXTENDED)) {
    regfree(&archive_regex);
    ret = OB_ERR_SYS;
  } else {
    for (int directory = 0; directory < 2 && !is_cancel_requested_(); ++directory) {
      const int scan_ret = scan_log_directory_(directory, archive_regex, raw_regex, enable_delete,
                                               enable_libpalf_compression, cleanup_temporary, compress_files,
                                               log_file_count, total_size, scan_status, cleanup_conflicts);
      if (OB_SUCC(ret)) {
        ret = scan_ret;
      }
    }
    regfree(&archive_regex);
    regfree(&raw_regex);
  }
  if (is_cancel_requested_()) {
    ret = OB_CANCELED;
  }
  if (OB_SUCC(ret) && scan_status.directory_changed_) {
    // Deleted outputs may already have been counted or selected for deletion in this scan.
    const bool cleanup_pending = scan_status.cleanup_pending_;
    ret = scan_log_files_(enable_delete, enable_libpalf_compression, false, compress_files, log_file_count, total_size,
                          &scan_status, false);
    scan_status.cleanup_pending_ = scan_status.cleanup_pending_ || cleanup_pending;
  }
  return ret;
}

int ObLogCompressor::scan_log_directory_(int directory, const regex_t &archive_regex, const regex_t &raw_regex,
                                         bool enable_delete, bool enable_libpalf_compression, bool cleanup_temporary,
                                         ObSyslogFile compress_files[], int64_t log_file_count[], int64_t &total_size,
                                         ObSyslogScanStatus &scan_status, bool cleanup_conflicts)
{
  int ret = OB_SUCCESS;
  const char *directories[] = {syslog_dir_, alert_log_dir_};
  DIR *dir = opendir(directories[directory]);
  if (NULL == dir) {
    if (directory == 0) {
      ret = OB_IO_ERROR;
      scan_status.cleanup_pending_ = true;
    }
    if (directory == 0 || errno != ENOENT) {
      scan_status.size_complete_ = false;
      if (REACH_TIME_INTERVAL(1000000)) {
        LOG_WARN("failed to scan syslog directory", K(errno), K(directories[directory]));
      }
    }
  } else {
    const int directory_fd = dirfd(dir);
    while (!is_cancel_requested_()) {
      errno = 0;
      struct dirent *entry = readdir(dir);
      if (NULL == entry) {
        if (errno != 0) {
          scan_status.size_complete_ = false;
          if (directory == 0) {
            scan_status.cleanup_pending_ = true;
          }
          if (REACH_TIME_INTERVAL(1000000)) {
            LOG_WARN("syslog directory enumeration failed", K(errno));
          }
        }
        break;
      }
      if (entry->d_name[0] == '.') {
        continue;
      }
      const ObLibPalfSyslogFileType libpalf_type = classify_libpalf_file(entry->d_name);
      const bool is_temporary = libpalf_type == LIBPALF_TEMP_FILE || is_legacy_temporary_file_(entry->d_name);
      struct stat st;
      if (0 != fstatat(directory_fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW)) {
        if (errno != ENOENT) {
          scan_status.size_complete_ = false;
          if (is_temporary) {
            scan_status.cleanup_pending_ = true;
          }
          if (REACH_TIME_INTERVAL(1000000)) {
            LOG_WARN("failed to inspect syslog entry", K(errno), K(entry->d_name));
          }
        }
        continue;
      }
      const bool is_symlink = S_ISLNK(st.st_mode);
      if (is_symlink && 0 != fstatat(directory_fd, entry->d_name, &st, 0)) {
        // Dangling/cyclic links have no regular target to account. Other errors leave size unknown.
        if (errno != ENOENT && errno != ELOOP && errno != ENAMETOOLONG) {
          scan_status.size_complete_ = false;
        }
        if (errno != ENOENT && REACH_TIME_INTERVAL(1000000)) {
          LOG_WARN("skip inaccessible syslog symlink", K(errno), K(entry->d_name));
        }
        continue;
      }
      if (!S_ISREG(st.st_mode)) {
        continue;
      }
      if (!is_symlink && is_temporary) {
        if (cleanup_temporary && (0 == unlinkat(directory_fd, entry->d_name, 0) || ENOENT == errno)) {
          continue;
        }
        scan_status.cleanup_pending_ = true;
        if (cleanup_temporary && REACH_TIME_INTERVAL(1000000)) {
          LOG_WARN("failed to remove abandoned syslog output", K(errno), K(entry->d_name));
        }
      }
      total_size += st.st_size;
      if (is_symlink) {
        continue;
      }
      const bool is_raw = libpalf_type == NOT_LIBPALF_FILE ? 0 == regexec(&raw_regex, entry->d_name, 0, NULL, 0)
                                                           : libpalf_type == LIBPALF_ARCHIVE_FILE;
      const int type = get_log_type_(entry->d_name);
      if (directory == 0 && is_raw && type < OB_SYSLOG_COMPRESS_TYPE_COUNT) {
        ++log_file_count[type];
      }
      ObSyslogFile file;
      const int length =
          snprintf(file.file_name_, sizeof(file.file_name_), "%s/%s", directories[directory], entry->d_name);
      if (length < 0 || length >= sizeof(file.file_name_)) {
        // Already counted using fstatat; an unrepresentable candidate must not stop the worker.
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_WARN("syslog candidate path too long", K(length), K(entry->d_name));
        }
        continue;
      }
      file.size_ = st.st_size;
      file.device_ = st.st_dev;
      file.inode_ = st.st_ino;
      file.mtime_ = st.st_mtim.tv_sec * 1000000000L + st.st_mtim.tv_nsec;
      const bool is_archive = libpalf_type == NOT_LIBPALF_FILE
                                  ? 0 == regexec(&archive_regex, entry->d_name, 0, NULL, 0)
                                  : libpalf_type == LIBPALF_ARCHIVE_FILE || libpalf_type == LIBPALF_COMPRESSED_FILE;
      if (enable_delete && is_archive && !is_temporary) {
        const int push_ret = oldest_files_.push(file);
        if (OB_SUCCESS != push_ret) {
          ret = push_ret;
        }
      }
      if (directory == 0 && is_raw && type < OB_SYSLOG_COMPRESS_TYPE_COUNT && st.st_size > 0) {
        char output[OB_MAX_SYSLOG_FILE_NAME_SIZE];
        struct stat output_st;
        bool eligible = false;
        if (OB_SUCCESS == get_compressed_file_name_(file.file_name_, output)) {
          if (0 != lstat(output, &output_st)) {
            eligible = errno == ENOENT;
          } else if (cleanup_conflicts && (type < SYSLOG_LIBPALF || enable_libpalf_compression)
                     && S_ISREG(output_st.st_mode)) {
            bool removed = false;
            const int cleanup_ret = remove_conflicting_output_(file.file_name_, st, output, output_st, removed);
            scan_status.directory_changed_ = scan_status.directory_changed_ || removed;
            if (OB_SUCCESS != cleanup_ret) {
              scan_status.cleanup_pending_ = true;
              if (cleanup_ret != OB_CANCELED && REACH_TIME_INTERVAL(1000000)) {
                LOG_WARN("failed to remove conflicting syslog output", K(cleanup_ret), K(file));
              }
            }
          }
        }
        if (eligible) {
          ++scan_status.eligible_count_[type];
          if ((type != SYSLOG_LIBPALF || enable_libpalf_compression)
              && (compress_files[type].file_name_[0] == '\0' || cmp_(compress_files[type], file))) {
            compress_files[type] = file;
          }
        }
      }
    }
    if (0 != closedir(dir)) {
      scan_status.size_complete_ = false;
    }
  }
  return ret;
}

void ObLogCompressor::log_compress_loop_()
{
  int ret = OB_SUCCESS;
  const int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  char *src_buf = static_cast<char *>(ob_malloc(src_size + OB_SYSLOG_COMPRESS_BUFFER_SIZE, "SyslogCompress"));
  if (NULL == src_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate syslog compression buffers", K(ret));
  } else {
    do_log_compress_loop_(src_buf, src_buf + src_size);
    ob_free(src_buf);
  }
}

void ObLogCompressor::do_log_compress_loop_(char *src_buf, char *dest_buf)
{
  int ret = OB_SUCCESS;
  bool fast_delete_log_mode = false;
  bool cleanup_required = true;  // Clean abandoned temporary outputs even when all policies are disabled.
  while (!ATOMIC_LOAD(&stopped_)) {
    {
      ObThreadCondGuard guard(log_compress_cond_);
      while (!ATOMIC_LOAD(&stopped_)
             && (ATOMIC_LOAD(&config_paused_)
                 || (!cleanup_required && !is_enable_compress() && get_max_disk_size() <= 0
                     && OB_LOGGER.get_max_file_index() <= 0))) {
        ObBKGDSessInActiveGuard inactive_guard;
        log_compress_cond_.wait_us(loop_interval_);
      }
      if (ATOMIC_LOAD(&stopped_)) {
        break;
      }
      cycle_running_ = true;
    }
    const int64_t start_time = ObTimeUtility::current_time();
    const int64_t max_disk_size = get_max_disk_size();
    const int64_t min_uncompressed_count = get_min_uncompressed_count();
    const bool enable_compress = is_enable_compress();
    const bool enable_libpalf = is_libpalf_compression_enabled();
    const bool enable_delete = max_disk_size > 0 || OB_LOGGER.get_max_file_index() > 0;
    if (enable_compress) {
      compressor_ = ATOMIC_LOAD(&next_compressor_);
    }
    ObSyslogFile compress_files[OB_SYSLOG_COMPRESS_TYPE_COUNT];
    int64_t file_count[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {};
    int64_t total_size = 0;
    int64_t compressed_count = 0;
    int64_t deleted_count = 0;
    ObSyslogScanStatus scan_status;
    ret = scan_log_files_(enable_delete, enable_libpalf, true, compress_files, file_count, total_size, &scan_status,
                          enable_compress);
    bool cleanup_pending = scan_status.cleanup_pending_ || ret != OB_SUCCESS;
    bool needs_rescan = false;
    bool compression_failed = false;
    int64_t remaining = get_effective_remaining_size_(total_size, max_disk_size, scan_status.size_complete_);
    if (OB_SUCC(ret) && enable_compress && NULL != compressor_ && remaining < OB_SYSLOG_COMPRESS_RESERVE_SIZE
        && !fast_delete_log_mode) {
      for (int type = 0; type < OB_SYSLOG_COMPRESS_TYPE_COUNT && !is_cancel_requested_(); ++type) {
        if (scan_status.eligible_count_[type] > min_uncompressed_count && compress_files[type].file_name_[0] != '\0') {
          struct stat st;
          const ObSyslogFile &candidate = compress_files[type];
          needs_rescan = true;  // Failures can still publish an output or leave temporary bytes.
          if (0 == lstat(candidate.file_name_, &st) && same_file_(candidate, st)) {
            const int file_ret = compress_single_file_(candidate.file_name_, src_buf, dest_buf);
            if (OB_SUCCESS == file_ret) {
              ++compressed_count;
            } else if (file_ret != OB_FILE_NOT_EXIST && file_ret != OB_CANCELED && file_ret != OB_FILE_ALREADY_EXIST) {
              compression_failed = true;
              LOG_WARN("failed to compress syslog archive", K(file_ret), K(candidate));
            }
          }
        }
      }
    }
    // Account for output bytes and rebuild paths after compression. A new .zst is eligible now.
    if (needs_rescan && !is_cancel_requested_()) {
      ret = scan_log_files_(enable_delete, enable_libpalf, false, compress_files, file_count, total_size, &scan_status);
      cleanup_pending = cleanup_pending || scan_status.cleanup_pending_;
      remaining = get_effective_remaining_size_(total_size, max_disk_size, scan_status.size_complete_);
    }
    if (OB_SUCC(ret) && enable_delete) {
      for (int attempt = 0; attempt < OB_SYSLOG_DELETE_ARRAY_SIZE - 1 && !is_cancel_requested_()
                            && remaining < OB_SYSLOG_DELETE_RESERVE_SIZE && oldest_files_.count() > 0;
           ++attempt) {
        bool rescan_required = false;
        const ObSyslogFile *candidate = NULL;
        if (OB_SUCCESS != oldest_files_.top(candidate) || NULL == candidate) {
          break;
        }
        struct stat st;
        if (0 != lstat(candidate->file_name_, &st)) {
          rescan_required = errno == ENOENT;
        } else if (!same_file_(*candidate, st)) {
          rescan_required = true;
        } else if (0 == unlink(candidate->file_name_)) {
          total_size -= st.st_size;
          ++deleted_count;
        } else if (errno == ENOENT) {
          rescan_required = true;
        } else {
          LOG_WARN("failed to delete syslog archive", K(errno), KPC(candidate));
        }
        oldest_files_.pop();
        if (rescan_required) {
          // Do not report ENOENT as freed bytes, or delete others using a known stale size.
          ret = scan_log_files_(enable_delete, enable_libpalf, false, compress_files, file_count, total_size,
                                &scan_status);
          if (OB_FAIL(ret)) {
            break;
          }
        }
        remaining = get_effective_remaining_size_(total_size, max_disk_size, scan_status.size_complete_);
      }
    }
    if ((compressed_count > 0 && deleted_count > 0) || deleted_count >= OB_SYSLOG_DELETE_ARRAY_SIZE - 1) {
      fast_delete_log_mode = true;
    } else if (deleted_count == 0) {
      fast_delete_log_mode = false;
    }
    if (is_cancel_requested_()) {
      ret = OB_CANCELED;
    }
    cleanup_pending = cleanup_pending || scan_status.cleanup_pending_;
    cleanup_required = cleanup_pending || ret != OB_SUCCESS;
    const int64_t cost_time = ObTimeUtility::current_time() - start_time;
    LOG_INFO("log compressor cycles once", K(ret), K(cost_time), K(compressed_count), K(deleted_count), K(total_size),
             K(remaining), K(enable_libpalf), K(fast_delete_log_mode), "size_complete", scan_status.size_complete_,
             K(cleanup_pending));
    {
      ObThreadCondGuard guard(log_compress_cond_);
      cycle_running_ = false;
      log_compress_cond_.broadcast();
      const bool retry_pending = cleanup_pending || compression_failed || ret != OB_SUCCESS;
      const int64_t normal_wait = retry_pending ? loop_interval_ : loop_interval_ - cost_time;
      const int64_t wait_us =
          fast_delete_log_mode ? OB_SYSLOG_COMPRESS_MIN_IDLE : std::max(OB_SYSLOG_COMPRESS_MIN_IDLE, normal_wait);
      if (!ATOMIC_LOAD(&stopped_) && !ATOMIC_LOAD(&config_paused_) && wait_us > 0) {
        log_compress_cond_.wait_us(wait_us);
      }
    }
  }
}

int ObLogCompressor::get_compressed_file_name_(const char *file_name, char compressed_file_name[])
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int size = strnlen(file_name, OB_MAX_SYSLOG_FILE_NAME_SIZE);
    int suffix_size = strlen(OB_SYSLOG_COMPRESS_ZSTD_SUFFIX);
    if (size <= 0 || size + suffix_size + 1 > OB_MAX_SYSLOG_FILE_NAME_SIZE) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      strncpy(compressed_file_name, file_name, size);
      strncpy(compressed_file_name + size, OB_SYSLOG_COMPRESS_ZSTD_SUFFIX, suffix_size);
      *(compressed_file_name + size + suffix_size) = '\0';
    }
  }
  return ret;
}

int ObLogCompressor::compress_single_block_(char *dest, size_t dest_size, const char *src, size_t src_size,
                                            size_t &return_size)
{
  int ret = OB_SUCCESS;
  int64_t size = -1;
  if (OB_ISNULL(compressor_) || OB_ISNULL(src) || OB_ISNULL(dest)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(compressor_->compress(src, src_size, dest, dest_size, size))) {
    LOG_ERROR("failed to compress single block", K(ret));
  } else {
    return_size = size;
  }
  return ret;
}

int ObLogCompressor::compress_single_file_(const char *file_name, char *src_buf, char *dest_buf)
{
  int ret = OB_SUCCESS;
  if (classify_libpalf_file(file_name) != NOT_LIBPALF_FILE) {
    ret = compress_libpalf_file_(file_name, src_buf, dest_buf);
  } else {
    ret = compress_legacy_file_(file_name, src_buf, dest_buf);
  }
  return ret;
}

int ObLogCompressor::compress_legacy_file_(const char *file_name, char *src_buf, char *dest_buf)
{
  int ret = OB_SUCCESS;
  FILE *input_file = NULL;
  FILE *output_file = NULL;
  int directory = -1;
  bool temporary_created = false;
  char compressed[OB_MAX_SYSLOG_FILE_NAME_SIZE] = {};
  char temporary[OB_MAX_SYSLOG_FILE_NAME_SIZE] = {};
  struct stat source_st = {}, current_st = {};
  if (NULL == file_name || NULL == src_buf || NULL == dest_buf || NULL == compressor_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != stat(file_name, &source_st)) {
    ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_IO_ERROR;
  } else if (0 != lstat(file_name, &current_st) || !unchanged_syslog_inode_(source_st, current_st)
             || source_st.st_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_compressed_file_name_(file_name, compressed))) {
  } else if (0 == lstat(compressed, &current_st)) {
    ret = OB_FILE_ALREADY_EXIST;
  } else if (errno != ENOENT) {
    ret = OB_IO_ERROR;
  } else if ((directory = open_syslog_parent_directory(file_name)) < 0) {
    ret = OB_IO_ERROR;
  } else if (NULL == (input_file = fopen(file_name, "re"))) {
    ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_IO_ERROR;
  } else if (0 != fstat(fileno(input_file), &current_st) || !unchanged_syslog_inode_(source_st, current_st)) {
    ret = OB_STATE_NOT_MATCH;
  }
  if (OB_SUCC(ret)) {
    const int type = get_log_type_(file_name);
    if (type < SYSLOG_LIBPALF && 0 == fstatat(directory, OB_SYSLOG_FILE_PREFIX[type], &current_st, 0)
        && current_st.st_dev == source_st.st_dev && current_st.st_ino == source_st.st_ino) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      const int length = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%ld.%lu", compressed,
                                  static_cast<long>(getpid()), process_start_time_, ++temporary_file_sequence_);
      if (length < 0 || length >= sizeof(temporary)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (NULL == (output_file = fopen(temporary, "wxe"))) {
        ret = errno == EEXIST ? OB_FILE_ALREADY_EXIST : OB_IO_ERROR;
      } else {
        temporary_created = true;
        if (0 != fchmod(fileno(output_file), source_st.st_mode & 0777)) {
          ret = OB_IO_ERROR;
        }
      }
    }
  }
  int64_t read_total = 0;
  while (OB_SUCC(ret) && !feof(input_file) && !is_cancel_requested_()) {
    const size_t read_size = fread(src_buf, 1, OB_SYSLOG_COMPRESS_BLOCK_SIZE, input_file);
    size_t write_size = 0;
    if (read_size > 0) {
      read_total += read_size;
      if (OB_FAIL(compress_single_block_(dest_buf, OB_SYSLOG_COMPRESS_BUFFER_SIZE, src_buf, read_size, write_size))) {
      } else if (write_size != fwrite(dest_buf, 1, write_size, output_file)) {
        ret = OB_IO_ERROR;
        LOG_WARN("failed to write syslog temporary output", K(ret), K(errno), KCSTRING(temporary));
      }
    }
    if (ferror(input_file)) {
      ret = OB_IO_ERROR;
    }
    ob_usleep(50 * 1000);
  }
  if (OB_SUCC(ret) && is_cancel_requested_()) {
    ret = OB_CANCELED;
  }
  if (OB_SUCC(ret)
      && (read_total != source_st.st_size || 0 != fstat(fileno(input_file), &current_st)
          || !unchanged_syslog_inode_(source_st, current_st))) {
    ret = OB_STATE_NOT_MATCH;
  }
  if (OB_SUCC(ret)) {
    const struct timespec timestamps[] = {source_st.st_atim, source_st.st_mtim};
    if (0 != fflush(output_file) || 0 != futimens(fileno(output_file), timestamps) || 0 != fsync(fileno(output_file))) {
      ret = OB_IO_ERROR;
    }
  }
  if (input_file != NULL && 0 != fclose(input_file) && OB_SUCC(ret)) {
    ret = OB_IO_ERROR;
  }
  if (output_file != NULL && 0 != fclose(output_file) && OB_SUCC(ret)) {
    ret = OB_IO_ERROR;
  }
  if (OB_SUCC(ret)) {
    if (is_cancel_requested_()) {
      ret = OB_CANCELED;
    } else if (0 != lstat(file_name, &current_st) || !unchanged_syslog_inode_(source_st, current_st)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (0 != link(temporary, compressed)) {
      ret = errno == EEXIST ? OB_FILE_ALREADY_EXIST : OB_IO_ERROR;
    } else if (0 != fsync(directory)) {
      ret = OB_IO_ERROR;  // Preserve the source unless the complete output is durably published.
    } else if (is_cancel_requested_()) {
      ret = OB_CANCELED;
    } else if (0 != lstat(file_name, &current_st) || !unchanged_syslog_inode_(source_st, current_st)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (0 != unlink(file_name) && errno != ENOENT) {
      ret = OB_IO_ERROR;
    }
  }
  if (temporary_created && 0 != unlink(temporary) && errno != ENOENT) {
    LOG_WARN("failed to clean legacy syslog temporary output", K(errno), KCSTRING(temporary));
  }
  if (directory >= 0) {
    if (OB_SUCC(ret) && 0 != fsync(directory)) {
      ret = OB_IO_ERROR;
    }
    close(directory);
  }
  return ret;
}

bool ObLogCompressor::is_legacy_temporary_file_(const char *file)
{
  bool is_temporary = false;
  const char *base = strrchr(file, '/');
  base = NULL == base ? file : base + 1;
  for (int type = 0; type < SYSLOG_LIBPALF; ++type) {
    const size_t prefix_length = strlen(OB_SYSLOG_FILE_PREFIX[type]);
    if (0 == strncmp(base, OB_SYSLOG_FILE_PREFIX[type], prefix_length) && base[prefix_length] == '.') {
      const char *suffix = base + prefix_length + 1;
      is_temporary = consume_syslog_number_(suffix) && valid_syslog_temporary_suffix_(suffix);
      break;
    }
  }
  return is_temporary;
}

bool ObLogCompressor::valid_syslog_temporary_suffix_(const char *suffix)
{
  static const char temporary[] = ".zst.tmp.";
  bool valid = 0 == strncmp(suffix, temporary, sizeof(temporary) - 1);
  if (valid) {
    suffix += sizeof(temporary) - 1;
    // Temporary identity: process ID, process start microseconds, per-process sequence.
    for (int field = 0; valid && field < 3; ++field) {
      valid = consume_syslog_number_(suffix);
      if (valid && field < 2) {
        valid = *suffix == '.';
        if (valid) {
          ++suffix;
        }
      }
    }
  }
  return valid && *suffix == '\0';
}

int ObLogCompressor::remove_conflicting_output_(const char *source, const struct stat &source_st, const char *output,
                                                const struct stat &output_st, bool &removed)
{
  int ret = OB_SUCCESS;
  int input = -1;
  int directory = -1;
  int active_stat_ret = 0;
  struct stat current_st = {};
  const int type = get_log_type_(source);
  removed = false;
  // The raw archive is authoritative. Never read, decode or preserve the conflicting
  // compressed copy; make the retained source durable before dropping that copy.
  if (type >= OB_SYSLOG_COMPRESS_TYPE_COUNT || !S_ISREG(source_st.st_mode) || source_st.st_size <= 0
      || !S_ISREG(output_st.st_mode)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_cancel_requested_()) {
    ret = OB_CANCELED;
  } else if ((input = open(source, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)) < 0
             || (directory = open_syslog_parent_directory(source)) < 0) {
    ret = OB_IO_ERROR;
  } else if (0 != fstat(input, &current_st) || !unchanged_syslog_inode_(source_st, current_st)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (0 != (active_stat_ret = fstatat(directory, OB_SYSLOG_FILE_PREFIX[type], &current_st, 0))
             && errno != ENOENT) {
    ret = OB_IO_ERROR;
  } else if (0 == active_stat_ret && current_st.st_dev == source_st.st_dev && current_st.st_ino == source_st.st_ino) {
    ret = OB_STATE_NOT_MATCH;
  } else if (0 != fsync(input) || 0 != fsync(directory)) {
    ret = OB_IO_ERROR;
  } else if (is_cancel_requested_()) {
    ret = OB_CANCELED;
  } else if (0 != lstat(source, &current_st) || !unchanged_syslog_inode_(source_st, current_st)
             || 0 != lstat(output, &current_st) || !unchanged_syslog_inode_(output_st, current_st)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (0 != unlink(output) && errno != ENOENT) {
    ret = OB_IO_ERROR;
  } else {
    // This describes the directory mutation even if the following fsync fails.
    // The caller must rescan accounting; the durable raw archive remains intact.
    removed = true;
    if (0 != fsync(directory)) {
      ret = OB_IO_ERROR;
    }
  }
  if (input >= 0) {
    close(input);
  }
  if (directory >= 0) {
    close(directory);
  }
  return ret;
}

int ObLogCompressor::set_last_modify_time_(const char *file_name, const time_t &newTime)
{
  int ret = OB_SUCCESS;
  struct utimbuf newTimes;
  newTimes.actime = newTime;
  newTimes.modtime = newTime;

  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (utime(file_name, &newTimes) != 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("failed to set the file's last modified time", K(ret), K(strerror(errno)), K(file_name), K(newTime));
  }

  return ret;
}

int ObLogCompressor::set_next_compressor_(ObCompressorType compress_func)
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor = NULL;
  if (compress_func == NONE_COMPRESSOR) {
    ATOMIC_STORE(&next_compressor_, static_cast<ObCompressor *>(NULL));
    if (!is_inited_) {
      compressor_ = NULL;
    }
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compress_func, compressor))) {
    LOG_ERROR("Fail to get_compressor", K(ret), K(compress_func));
  } else {
    ATOMIC_STORE(&next_compressor_, compressor);
    if (!is_inited_) {
      compressor_ = compressor;
    }
  }
  return ret;
}

int ObLogCompressor::get_log_type_(const char *file_name)
{
  int type = OB_SYSLOG_COMPRESS_TYPE_COUNT;
  if (NULL != file_name) {
    const char *base = strrchr(file_name, '/');
    base = NULL == base ? file_name : base + 1;
    for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; ++i) {
      const size_t prefix_len = strlen(OB_SYSLOG_FILE_PREFIX[i]);
      if (0 == strncmp(base, OB_SYSLOG_FILE_PREFIX[i], prefix_len)
          && (base[prefix_len] == '\0' || base[prefix_len] == '.')) {
        type = i;
        break;
      }
    }
  }
  return type;
}

int64_t ObLogCompressor::get_disk_remaining_size_()
{
  int ret = OB_SUCCESS;
  int64_t remaining_size = 0;
  struct statfs file_system;
  if (statfs(syslog_dir_, &file_system) == -1) {
    remaining_size = -1;
    ret = OB_ERR_SYS;
    LOG_ERROR("fail to get disk remaining size", K(ret), K(strerror(errno)), K(syslog_dir_));
  } else {
    remaining_size = file_system.f_bsize * file_system.f_bavail;
  }
  return remaining_size;
}

bool ObLogCompressor::consume_syslog_number_(const char *&text)
{
  const char *begin = text;
  uint64_t value = 0;
  bool valid = true;
  while (valid && *text >= '0' && *text <= '9') {
    const uint64_t digit = *text - '0';
    if (value > (UINT64_MAX - digit) / 10) {
      valid = false;
    } else {
      value = value * 10 + digit;
      ++text;
    }
  }
  return valid && text != begin;
}

int ObLogCompressor::decimal_part_(const char *text, int length)
{
  int value = 0;
  for (int i = 0; i < length; ++i) {
    value = value * 10 + text[i] - '0';
  }
  return value;
}

bool ObLogCompressor::valid_libpalf_timestamp_(const char *text)
{
  bool valid = strlen(text) >= 14;
  for (int i = 0; valid && i < 14; ++i) {
    valid = text[i] >= '0' && text[i] <= '9';
  }
  if (valid) {
    const int year = decimal_part_(text, 4);
    const int month = decimal_part_(text + 4, 2);
    const int day = decimal_part_(text + 6, 2);
    const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    valid = month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1] + (month == 2 && leap ? 1 : 0)
            && decimal_part_(text + 8, 2) < 24 && decimal_part_(text + 10, 2) < 60 && decimal_part_(text + 12, 2) < 60;
  }
  return valid;
}

bool ObLogCompressor::unchanged_syslog_inode_(const struct stat &before, const struct stat &after)
{
  return S_ISREG(after.st_mode) && before.st_dev == after.st_dev && before.st_ino == after.st_ino
         && before.st_size == after.st_size && before.st_mtim.tv_sec == after.st_mtim.tv_sec
         && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec;
}

int ObLogCompressor::write_syslog_output_(int fd, const char *data, size_t size)
{
  int ret = OB_SUCCESS;
  while (size > 0 && OB_SUCC(ret)) {
    const ssize_t written = write(fd, data, size);
    if (written < 0 && errno == EINTR) {
      continue;
    } else if (written <= 0) {
      ret = OB_IO_ERROR;
    } else {
      data += written;
      size -= written;
    }
  }
  return ret;
}

ObLibPalfSyslogFileType ObLogCompressor::classify_libpalf_file(const char *file)
{
  ObLibPalfSyslogFileType type = NOT_LIBPALF_FILE;
  if (NULL != file) {
    const char *base = strrchr(file, '/');
    base = NULL == base ? file : base + 1;
    static const char prefix[] = "libpalf.log";
    const size_t prefix_length = sizeof(prefix) - 1;
    if (0 == strncmp(base, prefix, prefix_length) && (base[prefix_length] == '\0' || base[prefix_length] == '.')) {
      type = base[prefix_length] == '\0' ? LIBPALF_ACTIVE_FILE : LIBPALF_OTHER_FILE;
      if (type == LIBPALF_OTHER_FILE) {
        const char *suffix = base + prefix_length + 1;
        bool valid = valid_libpalf_timestamp_(suffix);
        if (valid) {
          suffix += 14;
          if (*suffix == '.' && suffix[1] >= '0' && suffix[1] <= '9') {
            ++suffix;
            valid = consume_syslog_number_(suffix);
          }
        }
        if (valid) {
          if (*suffix == '\0') {
            type = LIBPALF_ARCHIVE_FILE;
          } else if (0 == strcmp(suffix, ".zst")) {
            type = LIBPALF_COMPRESSED_FILE;
          } else if (valid_syslog_temporary_suffix_(suffix)) {
            type = LIBPALF_TEMP_FILE;
          }
        }
      }
    }
  }
  return type;
}

int ObLogCompressor::compress_libpalf_blocks_(int input, int output, const struct stat &source_st, char *src_buf,
                                              char *dest_buf)
{
  int ret = OB_SUCCESS;
  struct stat current_st = {};
  int64_t read_total = 0;
  while (OB_SUCC(ret) && !is_cancel_requested_()) {
    const ssize_t size = read(input, src_buf, OB_SYSLOG_COMPRESS_BLOCK_SIZE);
    if (size < 0 && EINTR == errno) {
      continue;
    } else if (size < 0) {
      ret = OB_IO_ERROR;
    } else if (size == 0 && read_total > 0) {
      break;
    } else {
      // An empty source still needs one complete Zstd frame.
      size_t written = 0;
      if (OB_FAIL(compress_single_block_(dest_buf, OB_SYSLOG_COMPRESS_BUFFER_SIZE, src_buf, size, written))) {
      } else if (OB_FAIL(write_syslog_output_(output, dest_buf, written))) {
      } else {
        read_total += size;
        if (size == 0) {
          break;
        }
        ob_usleep(50 * 1000);
      }
    }
  }
  if (OB_SUCC(ret) && is_cancel_requested_()) {
    ret = OB_CANCELED;
  }
  if (OB_SUCC(ret)
      && (read_total != source_st.st_size || 0 != fstat(input, &current_st)
          || !unchanged_syslog_inode_(source_st, current_st))) {
    ret = OB_STATE_NOT_MATCH;
  }
  if (OB_SUCC(ret)) {
    const struct timespec timestamps[] = {source_st.st_atim, source_st.st_mtim};
    if (0 != futimens(output, timestamps) || 0 != fsync(output)) {
      ret = OB_IO_ERROR;
    }
  }
  return ret;
}

int ObLogCompressor::compress_libpalf_file_(const char *file_name, char *src_buf, char *dest_buf)
{
  int ret = OB_SUCCESS;
  int input = -1;
  int output = -1;
  int directory = -1;
  bool temporary_created = false;
  char compressed[OB_MAX_SYSLOG_FILE_NAME_SIZE] = {};
  char temporary[OB_MAX_SYSLOG_FILE_NAME_SIZE] = {};
  char parent[OB_MAX_SYSLOG_FILE_NAME_SIZE] = {};
  struct stat source_st = {};
  struct stat current_st = {};
  if (classify_libpalf_file(file_name) != LIBPALF_ARCHIVE_FILE || NULL == src_buf || NULL == dest_buf
      || NULL == compressor_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_compressed_file_name_(file_name, compressed))) {
  } else if (0 == lstat(compressed, &current_st)) {
    ret = OB_FILE_ALREADY_EXIST;
  } else if (errno != ENOENT) {
    ret = OB_IO_ERROR;
  } else if (0 != lstat(file_name, &source_st)) {
    ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_IO_ERROR;
  } else if (!S_ISREG(source_st.st_mode)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((input = open(file_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW)) < 0) {
    ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_IO_ERROR;
  } else if (0 != fstat(input, &current_st) || !unchanged_syslog_inode_(source_st, current_st)) {
    ret = OB_STATE_NOT_MATCH;
  }
  if (OB_SUCC(ret)) {
    const char *slash = strrchr(file_name, '/');
    if (NULL == slash) {
      snprintf(parent, sizeof(parent), ".");
    } else if (slash == file_name) {
      snprintf(parent, sizeof(parent), "/");
    } else {
      snprintf(parent, sizeof(parent), "%.*s", static_cast<int>(slash - file_name), file_name);
    }
    char active[OB_MAX_SYSLOG_FILE_NAME_SIZE];
    const int active_length = snprintf(active, sizeof(active), "%s/libpalf.log", parent);
    if (active_length < 0 || active_length >= sizeof(active)) {
      ret = OB_SIZE_OVERFLOW;
    } else if (0 == stat(active, &current_st) && current_st.st_dev == source_st.st_dev
               && current_st.st_ino == source_st.st_ino) {
      ret = OB_STATE_NOT_MATCH;
    } else if ((directory = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC)) < 0) {
      ret = OB_IO_ERROR;
    }
  }
  if (OB_SUCC(ret)) {
    struct statfs filesystem;
    const int64_t blocks = source_st.st_size / OB_SYSLOG_COMPRESS_BLOCK_SIZE + 1;
    if (blocks > INT64_MAX / OB_SYSLOG_COMPRESS_BUFFER_SIZE) {
      ret = OB_SIZE_OVERFLOW;
    } else if (0 != fstatfs(input, &filesystem)) {
      ret = OB_IO_ERROR;
    } else if (static_cast<uint64_t>(filesystem.f_bavail) * filesystem.f_bsize
               < static_cast<uint64_t>(blocks * OB_SYSLOG_COMPRESS_BUFFER_SIZE) + OB_SYSLOG_COMPRESS_BLOCK_SIZE
                     + OB_SYSLOG_COMPRESS_BUFFER_SIZE) {
      ret = OB_IO_ERROR;
    }
  }
  if (OB_SUCC(ret)) {
    const int length = snprintf(temporary, sizeof(temporary), "%s.tmp.%ld.%ld.%lu", compressed,
                                static_cast<long>(getpid()), process_start_time_, ++temporary_file_sequence_);
    if (length < 0 || length >= sizeof(temporary)) {
      ret = OB_SIZE_OVERFLOW;
    } else if ((output = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, source_st.st_mode & 0777)) < 0) {
      ret = OB_IO_ERROR;
    } else {
      temporary_created = true;
    }
  }
  if (OB_SUCC(ret)) {
    ret = compress_libpalf_blocks_(input, output, source_st, src_buf, dest_buf);
  }
  if (output >= 0) {
    if (0 != close(output) && OB_SUCC(ret)) {
      ret = OB_IO_ERROR;
    }
    output = -1;
  }
  if (OB_SUCC(ret)) {
    if (0 != lstat(file_name, &current_st)) {
      ret = errno == ENOENT ? OB_FILE_NOT_EXIST : OB_IO_ERROR;
    } else if (!unchanged_syslog_inode_(source_st, current_st)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (is_cancel_requested_()) {
      ret = OB_CANCELED;
    } else if (0 != link(temporary, compressed)) {
      // Same-directory link publishes the complete file atomically without replacing a target.
      ret = errno == EEXIST ? OB_FILE_ALREADY_EXIST : OB_IO_ERROR;
    } else if (0 != fsync(directory)) {
      ret = OB_IO_ERROR;  // Keep both complete copies; never remove the input here.
    } else if (0 == lstat(file_name, &current_st)) {
      if (!unchanged_syslog_inode_(source_st, current_st)) {
        ret = OB_STATE_NOT_MATCH;
      } else if (0 != unlink(file_name) && errno != ENOENT) {
        ret = OB_IO_ERROR;
      }
    } else if (errno != ENOENT) {
      ret = OB_IO_ERROR;
    }
  }
  if (temporary_created && 0 != unlink(temporary) && errno != ENOENT) {
    LOG_WARN("failed to clean libpalf compression temporary file", K(errno), KCSTRING(temporary));
  }
  if (input >= 0) {
    close(input);
  }
  if (directory >= 0) {
    if (OB_SUCC(ret) && 0 != fsync(directory)) {
      ret = OB_IO_ERROR;
    }
    close(directory);
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase

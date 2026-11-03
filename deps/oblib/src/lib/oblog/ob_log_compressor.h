/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_LOG_COMPRESSOR_H_
#define OB_LOG_COMPRESSOR_H_

#include <sys/stat.h>
#include <regex.h>
#include "lib/container/ob_heap.h"
#include "lib/lock/mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace common {

class ObCompressor;
class ObLogger;

/* Log files are divided into blocks and then compressed. */
static const int32_t OB_SYSLOG_COMPRESS_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
/* To prevent extreme cases where the files become larger after compression,
 * the size of the decompression buffer needs to be larger than the original data.
 * This size can refer to the ZSTD code implementation.
 */
static const int32_t OB_SYSLOG_COMPRESS_BUFFER_SIZE =
    OB_SYSLOG_COMPRESS_BLOCK_SIZE + OB_SYSLOG_COMPRESS_BLOCK_SIZE / 128 + 512 + 19;
static const int32_t OB_MAX_SYSLOG_FILE_NAME_SIZE = ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE;
// Archive types are independent of ObPLogFDType: libpalf owns its FD in Rust.
enum ObSyslogCompressType
{
  SYSLOG_OBSERVER,
  SYSLOG_ROOTSERVICE,
  SYSLOG_ELECTION,
  SYSLOG_TRACE,
  SYSLOG_LIBPALF,
  OB_SYSLOG_COMPRESS_TYPE_COUNT
};
enum ObLibPalfSyslogFileType
{
  NOT_LIBPALF_FILE,
  LIBPALF_OTHER_FILE,
  LIBPALF_ACTIVE_FILE,
  LIBPALF_ARCHIVE_FILE,
  LIBPALF_COMPRESSED_FILE,
  LIBPALF_TEMP_FILE
};
/* by default, syslog file size = 256MB, and most of the logs are observer.log
 * compressed file size ≈ 13MB, 13MB * 20 = 260MB, so OB_SYSLOG_DELETE_ARRAY_SIZE = 20 + 1
 * an extra position is reserved for newly inserted file name.
 */
static const int32_t OB_SYSLOG_DELETE_ARRAY_SIZE = 21;
static const int64_t OB_MIN_SYSLOG_DISK_SIZE = 2 * (1LL << 30);          // 2GB
static const int64_t OB_SYSLOG_COMPRESS_RESERVE_SIZE = 4 * (1LL << 30);  // 4GB
static const int64_t OB_SYSLOG_DELETE_RESERVE_SIZE = 2 * (1LL << 30);    // 2GB
static const int64_t OB_SYSLOG_COMPRESS_LOOP_INTERVAL = 5000000;         // 5s
static const int64_t OB_SYSLOG_COMPRESS_MIN_IDLE = 100000;               // 100ms, even after a slow cycle
static const char OB_SYSLOG_DIR[] = "log";                               // same as LOG_DIR in src/observer/main.cpp
static const char OB_ALERT_LOG_DIR[] = "log/alert";                      // same as ALERT_DIR in src/observer/main.cpp
static const char OB_SYSLOG_COMPRESS_ZSTD_SUFFIX[] = ".zst";             // name suffix of file compressed by zstd
static const char OB_UNCOMPRESSED_SYSLOG_FILE_PATTERN[] = "^[a-z]+\\.log\\.[0-9]+$";  // only uncompressed files
static const char OB_COMPRESSED_SYSLOG_FILE_PATTERN[] = "^[a-z]+\\.log\\.[0-9]+\\.[a-z0-9]+$";  // only compressed files
static const char OB_ARCHIVED_SYSLOG_FILE_PATTERN[] =
    "^[a-z]+\\.log\\.[0-9]+[.a-z0-9]*$";  // all syslog files (excluding wf logs)
static const char *OB_SYSLOG_FILE_PREFIX[OB_SYSLOG_COMPRESS_TYPE_COUNT] = {
    "observer.log",     // FD_SVR_FILE
    "rootservice.log",  // FD_RS_FILE
    "election.log",     // FD_ELEC_FILE
    "trace.log",        // SYSLOG_TRACE
    "libpalf.log",      // SYSLOG_LIBPALF; no C++ logging FD
                        // no need to compress audit log and alert log
};
STATIC_ASSERT(MAX_FD_FILE == 6, "if you add a new log type, add it's prefix here !!!");

#define OB_LOG_COMPRESSOR ::oceanbase::common::ObLogCompressor::get_log_compressor()

struct ObSyslogFile
{
  ObSyslogFile()
  {
    reset();
  }
  void reset()
  {
    mtime_ = INT64_MAX;
    size_ = 0;
    device_ = 0;
    inode_ = 0;
    memset(file_name_, 0, OB_MAX_SYSLOG_FILE_NAME_SIZE);
  }
  TO_STRING_KV(K_(mtime), K_(file_name));
  int64_t mtime_;
  int64_t size_;
  dev_t device_;
  ino_t inode_;
  char file_name_[OB_MAX_SYSLOG_FILE_NAME_SIZE];
};

struct ObSyslogCompareFunctor
{
  static int compare_archive_names(const char *left, const char *right);
  bool operator()(const ObSyslogFile &l, const ObSyslogFile &r)
  {
    return l.mtime_ > r.mtime_ || (l.mtime_ == r.mtime_ && compare_archive_names(l.file_name_, r.file_name_) > 0);
  }
  int get_error_code()
  {
    return OB_SUCCESS;
  }
};

struct ObSyslogConfig
{
  ObSyslogConfig()
      : max_file_count_(0),
        record_old_log_file_(false),
        max_disk_size_(0),
        compress_func_(NONE_COMPRESSOR),
        min_uncompressed_count_(0),
        enable_libpalf_(false)
  {
  }
  bool is_valid() const;
  void normalize();
  bool operator==(const ObSyslogConfig &other) const;
  int64_t max_file_count_;
  bool record_old_log_file_;
  int64_t max_disk_size_;
  ObCompressorType compress_func_;
  int64_t min_uncompressed_count_;
  bool enable_libpalf_;
};

struct ObSyslogScanStatus
{
  ObSyslogScanStatus() : size_complete_(true), cleanup_pending_(false), directory_changed_(false), eligible_count_()
  {
  }
  bool size_complete_;
  bool cleanup_pending_;
  bool directory_changed_;
  int64_t eligible_count_[OB_SYSLOG_COMPRESS_TYPE_COUNT];
};

/* A priority array, order by modify time of files. */
class ObSyslogPriorityArray : public ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>
{
public:
  ObSyslogPriorityArray(ObSyslogCompareFunctor &cmp, common::ObIAllocator *allocator = NULL)
      : ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>(cmp, allocator)
  {
  }
  virtual ~ObSyslogPriorityArray()
  {
  }

  int push(const ObSyslogFile &element)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(array_.push_back(element))) {
    } else if (OB_FAIL(upheap(array_.count() - 1))) {
    } else if (array_.count() > OB_SYSLOG_DELETE_ARRAY_SIZE - 1) {
      ret = pop_back();
    }
    return ret;
  }

private:
  int pop_back()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(array_.empty())) {
      ret = OB_EMPTY_RESULT;
    } else {
      int index = 0;
      const ObSyslogFile *max_element = &array_.at(0);
      for (int i = 1; i < array_.count(); i++) {
        if (cmp_(array_.at(i), *max_element)) {
          index = i;
          max_element = &array_.at(i);
        }
      }
      reset_root_cmp_cache();
      array_.at(index) = array_.at(array_.count() - 1);
      array_.pop_back();
      if (index < array_.count()) {
        if (index == get_root()) {
          ret = downheap(index);
        } else {
          int64_t parent = get_parent(index);
          if (cmp_(array_.at(parent), array_.at(index))) {
            ret = upheap(index);
          } else {
            ret = downheap(index);
          }
        }
      }
    }
    return ret;
  }

private:
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::array_;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::cmp_;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::get_root;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::get_parent;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::reset_root_cmp_cache;
  using ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::upheap;
  using ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::downheap;
};

class ObLogCompressor final : public lib::TGRunnable
{
public:
  ObLogCompressor();
  virtual ~ObLogCompressor();
  int init();
  void stop();
  void wait();
  void awake();
  void destroy();
  inline bool is_enable_compress()
  {
    return NONE_COMPRESSOR != ATOMIC_LOAD(&compress_func_);
  }
  // Serialize configuration callers; pause drains only this C++ worker, not Rust.
  obutil::Mutex &get_config_update_mutex()
  {
    return config_update_mutex_;
  }
  int pause_for_config_update();
  void resume_after_config_update();
  bool is_libpalf_compression_enabled() const
  {
    return ATOMIC_LOAD(&libpalf_compression_enabled_);
  }
  int64_t get_max_disk_size() const
  {
    return ATOMIC_LOAD(&max_disk_size_);
  }
  int64_t get_min_uncompressed_count() const
  {
    return ATOMIC_LOAD(&min_uncompressed_count_);
  }
  ObCompressorType get_compress_func() const
  {
    return ATOMIC_LOAD(&compress_func_);
  }
  ObSyslogConfig get_config(const ObLogger &logger) const;
  bool is_config_applied(const ObLogger &logger, const ObSyslogConfig &config) const;
  // Production configuration enters through ObConfigManager::ob_logger_config_update().
  // Caller serializes updates and drains this worker. Failure leaves C++ policy and history unchanged.
  int apply_config(ObLogger &logger, const ObSyslogConfig &config);
  static ObLibPalfSyslogFileType classify_libpalf_file(const char *file);
  static bool is_compressed_file(const char *file);
  static inline ObLogCompressor &get_log_compressor()
  {
    static ObLogCompressor log_compressor;
    return log_compressor;
  }
  TO_STRING_KV(K_(is_inited), K_(stopped), K_(loop_interval), K_(max_disk_size), K_(min_uncompressed_count),
               K_(compress_func));

private:
  friend class ObConfigManager;
  // Legacy/test controls: they do not synchronize Rust quotas. Keep them out of production callers.
  int set_max_disk_size(int64_t max_disk_size);
  int set_compress_func(const char *compress_func_ptr);
  int set_min_uncompressed_count(int64_t min_uncompressed_count);
  void set_libpalf_compression_enabled(bool enabled)
  {
    ATOMIC_STORE(&libpalf_compression_enabled_, enabled);
  }
  void run1() override;
  void log_compress_loop_();
  void do_log_compress_loop_(char *src_buf, char *dest_buf);
  int scan_log_files_(bool enable_delete, bool enable_libpalf_compression, bool cleanup_temporary,
                      ObSyslogFile compress_files[], int64_t log_file_count[], int64_t &total_size,
                      ObSyslogScanStatus *status = NULL, bool cleanup_conflicts = false);
  int scan_log_directory_(int directory, const regex_t &archive_regex, const regex_t &raw_regex, bool enable_delete,
                          bool enable_libpalf_compression, bool cleanup_temporary, ObSyslogFile compress_files[],
                          int64_t log_file_count[], int64_t &total_size, ObSyslogScanStatus &scan_status,
                          bool cleanup_conflicts);
  int remove_conflicting_output_(const char *source, const struct stat &source_st, const char *output,
                                 const struct stat &output_st, bool &removed);
  static bool is_legacy_temporary_file_(const char *file);
  static bool valid_syslog_temporary_suffix_(const char *suffix);
  int64_t get_effective_remaining_size_(int64_t total_size, int64_t max_disk_size, bool size_complete = true);
  int compress_libpalf_file_(const char *file_name, char *src_buf, char *dest_buf);
  int compress_libpalf_blocks_(int input, int output, const struct stat &source_st, char *src_buf, char *dest_buf);
  bool is_cancel_requested_() const
  {
    return ATOMIC_LOAD(&stopped_) || ATOMIC_LOAD(&config_paused_);
  }
  bool same_file_(const ObSyslogFile &file, const struct stat &st) const;
  int get_compressed_file_name_(const char *file_name, char compressed_file_name[]);
  int compress_single_block_(char *dest, size_t dest_size, const char *src, size_t src_size, size_t &return_size);
  int compress_single_file_(const char *file_name, char *src_buf, char *dest_buf);
  int compress_legacy_file_(const char *file_name, char *src_buf, char *dest_buf);
  int set_last_modify_time_(const char *file_name, const time_t &newTime);
  int set_next_compressor_(ObCompressorType compress_func);
  int get_log_type_(const char *file_name);
  int64_t get_disk_remaining_size_();
  static bool consume_syslog_number_(const char *&text);
  static int decimal_part_(const char *text, int length);
  static bool valid_libpalf_timestamp_(const char *text);
  static bool unchanged_syslog_inode_(const struct stat &before, const struct stat &after);
  static int write_syslog_output_(int fd, const char *data, size_t size);

private:
  obutil::Mutex config_update_mutex_;
  bool config_paused_;
  bool cycle_running_;  // protected by log_compress_cond_
  bool libpalf_compression_enabled_;
  bool config_applied_;  // configuration caller owns this state; only successful commits change it
  int64_t process_start_time_;
  uint64_t temporary_file_sequence_;
  bool is_inited_;
  bool stopped_;
  int64_t loop_interval_;  // don't modify
  int64_t max_disk_size_;
  int64_t min_uncompressed_count_;
  char syslog_dir_[64];
  char alert_log_dir_[64];
  ObCompressorType compress_func_;
  ObCompressor *compressor_;
  ObCompressor *next_compressor_;
  ObThreadCond log_compress_cond_;
  ObSyslogCompareFunctor cmp_;
  ObSyslogPriorityArray oldest_files_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_LOG_COMPRESSOR_H_ */

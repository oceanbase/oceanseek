/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_FLUSH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_FLUSH_TASK_H_

#include "deps/oblib/src/lib/task/ob_timer.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObTenantFileManager;
struct ObSegmentFileInfo;
class ObSSMacroCacheMgr;
class ObSSMacroCacheFlushInfo;
class ObSSMacroCacheFlushMap;

class ObSSMacroCacheFlushMeta final
{
public:
  ObSSMacroCacheFlushMeta()
    : file_id_(), file_len_(0), func_type_(0), is_sealed_(true), valid_length_(0)
  {}
  ObSSMacroCacheFlushMeta(const blocksstable::MacroBlockId &file_id,
                          const int64_t file_len,
                          const uint8_t func_type,
                          const bool is_sealed,
                          const int64_t valid_length)
    : file_id_(file_id), file_len_(file_len), func_type_(func_type), is_sealed_(is_sealed),
      valid_length_(valid_length)
  {}
  ObSSMacroCacheFlushMeta &operator=(const ObSSMacroCacheFlushMeta &other);
  ~ObSSMacroCacheFlushMeta() { reset(); }
  void reset();
  bool is_valid() const { return file_id_.is_valid() && (file_len_ > 0); }
  TO_STRING_KV(K_(file_id), K_(file_len), K_(func_type), K_(is_sealed), K_(valid_length));

public:
  blocksstable::MacroBlockId file_id_;
  int64_t file_len_;
  uint8_t func_type_; // for IOManager QoS
  bool is_sealed_; // especially for TMP_FILE. mark whether seg file is sealed when flush
  int64_t valid_length_; // especially for TMP_FILE. used for concat file path when flush unsealed seg file
};

class ObSSMacroCacheFlushEntry final
{
public:
  ObSSMacroCacheFlushEntry(common::ObIAllocator &allocator)
    : buf_(nullptr), flush_meta_(), read_handle_(), write_handle_(), allocator_(allocator)
  {}
  ~ObSSMacroCacheFlushEntry() { reset(); }
  int init(const ObSSMacroCacheFlushMeta &flush_meta);
  bool is_valid() const { return (buf_ != nullptr) && flush_meta_.is_valid(); }
  bool is_read_handle_valid() const { return read_handle_.is_valid(); }
  bool is_write_handle_valid() const { return write_handle_.is_valid(); }
  void reset();
  share::ObFunctionType get_function_type() const
  {
    return static_cast<share::ObFunctionType>(flush_meta_.func_type_);
  }
  TO_STRING_KV(KP_(buf), K_(flush_meta), K_(read_handle), K_(write_handle));

public:
  char *buf_;
  ObSSMacroCacheFlushMeta flush_meta_;
  blocksstable::ObStorageObjectHandle read_handle_;
  blocksstable::ObStorageObjectHandle write_handle_;
  common::ObIAllocator &allocator_;
};

class ObSSMacroCacheFlushTaskBase : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheFlushTaskBase(ObSSMacroCacheMgr &macro_cache_mgr, volatile bool &is_stopped);
  virtual ~ObSSMacroCacheFlushTaskBase() { destroy(); }
  int init(ObTenantFileManager *file_manager);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  int schedule_flush_task();

private:
  virtual ObSSMacroCacheFlushMap &get_flush_map() = 0;
  virtual int callback(const blocksstable::MacroBlockId &macro_id, const int ret_code) = 0;
  virtual share::ObFunctionType get_function_type(const ObSSMacroCacheFlushEntry &flush_entry) const = 0;
  void destroy_flush_entry_arr();
  int pre_alloc_flush_entry();
  int flush_files();
  int batch_get_files();
  int async_write_files();
  int async_read_files();
  int complete_flush_files();
  int construct_tmp_file_flush_meta(const ObSSMacroCacheFlushInfo &flush_info);
  int construct_other_file_flush_meta(const ObSSMacroCacheFlushInfo &flush_info);
  int do_async_read_file(ObSSMacroCacheFlushEntry &flush_entry);
  int do_async_write_file(ObSSMacroCacheFlushEntry &flush_entry);
  int try_get_file_length(const blocksstable::MacroBlockId &file_id,
                          bool &is_file_exist,
                          int64_t &file_len);
  int update_flush_info_of_tmp_file_meta(const ObSSMacroCacheFlushEntry &flush_entry);
  int modify_from_write_cache_to_read_cache(const blocksstable::MacroBlockId &macro_id);
  int erase_from_flush_map(const blocksstable::MacroBlockId &macro_id);
  int callback_and_erase_from_flush_map(const blocksstable::MacroBlockId &macro_id, const int ret_code);
  int recycle_flush_entry(ObSSMacroCacheFlushEntry &flush_entry);
  void clear_for_next_round();
  void set_flush_parallel_cnt();
  void adjust_flush_task_interval();

public:
  static const int INVALID_TG_ID = -1;
  static const int64_t SLOW_SCHEDULE_INTERVAL_US = 100 * 1000L; // 100ms
  static const int64_t MODERATE_SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  static const int64_t FAST_SCHEDULE_INTERVAL_US = 100L; // 100us
  static const int64_t UPDATE_PARALLEL_INTERVAL_US = 60 * 1000 * 1000L; // 1min
  static const int64_t MAX_FLUSH_PARALLELISM = 30;
  static const int64_t DEFAULT_FLUSH_PARALLELISM = 5;
  static const int64_t GB = 1024L * 1024L * 1024L;

protected:
  ObSSMacroCacheMgr &macro_cache_mgr_;

private:
  int tg_id_;
  int64_t flush_parallel_cnt_;
  int64_t interval_us_;
  ObTenantFileManager *file_manager_;
  ObSSMacroCacheFlushEntry *flush_entry_arr_;
  common::ObFixedQueue<ObSSMacroCacheFlushEntry> free_list_;
  common::ObFixedQueue<ObSSMacroCacheFlushEntry> async_read_list_;
  common::ObFixedQueue<ObSSMacroCacheFlushEntry> async_write_list_;
  common::ObArray<ObSSMacroCacheFlushMeta> files_to_flush_;
  common::ObArray<ObSSMacroCacheFlushMeta> flushed_files_; // files succeed to flush.
  common::ObFIFOAllocator allocator_;
};


// flush macro cache (designed for all kinds of macro cache, e.g., private macro, shared macro and
// tmp file), which is triggered by eviction.
class ObSSMacroCacheFlushTask : public ObSSMacroCacheFlushTaskBase
{
public:
  ObSSMacroCacheFlushTask(ObSSMacroCacheMgr &macro_cache_mgr, volatile bool &is_stopped)
    : ObSSMacroCacheFlushTaskBase(macro_cache_mgr, is_stopped)
  {}
  virtual ~ObSSMacroCacheFlushTask() {}

private:
  virtual ObSSMacroCacheFlushMap &get_flush_map() override;
  virtual int callback(const blocksstable::MacroBlockId &macro_id, const int ret_code) override
  {
    UNUSED(macro_id);
    UNUSED(ret_code);
    return OB_SUCCESS;
  }
  virtual share::ObFunctionType get_function_type(const ObSSMacroCacheFlushEntry &flush_entry) const override
  {
    UNUSED(flush_entry);
    return share::ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW;
  }
};


// flush write cache (specially designed for shared mini macro write cache), which is triggered
// by upload mini during compaction.
class ObSSWriteCacheFlushTask : public ObSSMacroCacheFlushTaskBase
{
public:
  ObSSWriteCacheFlushTask(ObSSMacroCacheMgr &macro_cache_mgr, volatile bool &is_stopped)
    : ObSSMacroCacheFlushTaskBase(macro_cache_mgr, is_stopped)
  {}
  virtual ~ObSSWriteCacheFlushTask() {}

private:
  virtual ObSSMacroCacheFlushMap &get_flush_map() override;
  virtual int callback(const blocksstable::MacroBlockId &macro_id, const int ret_code) override;
  // both minor compaction and transfer would flush write cache, we need to use io group of
  // minor compaction thread and transfer thread theirself.
  virtual share::ObFunctionType get_function_type(const ObSSMacroCacheFlushEntry &flush_entry) const
  {
    return flush_entry.get_function_type();
  }
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_FLUSH_TASK_H_ */

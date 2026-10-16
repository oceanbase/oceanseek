/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_EVICT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_EVICT_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace storage
{
class ObSSMemMacroCacheBufMgr;
class ObSSMemMacroCacheMetaMgr;
struct ObSSMemMacroCacheStat;

struct ObSSMemMacroCacheEvictWriteEntry
{
public:
  ObSSMemMacroCacheEvictWriteEntry();
  ~ObSSMemMacroCacheEvictWriteEntry() { reset(); }
  void reset();

  int init(const blocksstable::MacroBlockId &macro_id);

  TO_STRING_KV(K_(macro_id), K_(write_handle));
public:
  blocksstable::MacroBlockId macro_id_;
  blocksstable::ObStorageObjectHandle write_handle_;
};

struct ObSSMemMacroCacheEvictCtx
{
public:
  ObSSMemMacroCacheEvictCtx();
  ~ObSSMemMacroCacheEvictCtx() { reset(); }
  void reset();
  void start_eviction(const int64_t exp_evict_cnt, const int64_t cur_fast_evict_cnt);
  void finish_eviction();
  bool need_normal_evict() const { return (exp_evict_cnt_ > 0); }
  bool need_ttl_expire() const { return need_check_ttl_; }
  bool need_eviction() const { return ((exp_evict_cnt_ > 0) || need_check_ttl_); }
  void inc_evict_macro_cnt() { ++evict_macro_cnt_; }
  void inc_write_back_macro_cnt() { ++write_back_macro_cnt_; }
  void update_check_ttl_time() { prev_check_ttl_us_ = start_us_; }

  TO_STRING_KV(K_(need_check_ttl), K_(cost_us), K_(exp_evict_cnt), K_(evict_blk_cnt), K_(evict_macro_cnt),
    K_(write_back_macro_cnt), K_(prev_check_ttl_us), K_(prev_fast_evict_cnt));

public:
  bool need_check_ttl_;
  int64_t start_us_;
  int64_t cost_us_;
  int64_t exp_evict_cnt_;
  int64_t evict_blk_cnt_;
  int64_t evict_macro_cnt_;
  int64_t write_back_macro_cnt_;
  int64_t prev_check_ttl_us_;
  int64_t prev_fast_evict_cnt_;
};

class ObSSMemMacroCacheEvictTask : public common::ObTimerTask
{
public:
  ObSSMemMacroCacheEvictTask(ObSSMemMacroCacheBufMgr &buf_mgr, ObSSMemMacroCacheMetaMgr &meta_mgr, 
                             ObSSMemMacroCacheStat &cache_stat);
  virtual ~ObSSMemMacroCacheEvictTask() { destroy(); }

  int init(const uint64_t tenant_id);
  virtual void runTimerTask() override;
  void destroy();

  void close_task() { need_close_ = true; }
  void open_task() { need_close_ = false; }
  bool is_task_closed() const { return is_closed_; }

private:
  int64_t calc_evict_mem_blk_cnt();
  int execute_eviction();
  void handle_extra_func();

  int alloc_all_entries(const uint64_t tenant_id);
  void free_all_entries();
  int can_persist_entry(const ObSSMacroCacheBlkMetaHandle &macro_meta_handle, bool &need_persist);
  int do_async_write_entry(const blocksstable::MacroBlockId &macro_id,
                           ObSSMacroCacheBlkMetaHandle &macro_meta_handle);
  int do_write_into_macro_cache(const blocksstable::MacroBlockId &macro_id,
                                ObSSMacroCacheBlkMetaHandle &macro_meta_handle,
                                blocksstable::ObStorageObjectHandle &obj_handle);
  int add_failed_macro_to_prewarm_queue(const blocksstable::MacroBlockId &macro_id);
  int handle_completed_entry();
  int recycle_entry(ObSSMemMacroCacheEvictWriteEntry &entry);

private:
  static const int64_t MAX_WRITE_MACRO_PARALLEM = 64;

  bool is_inited_;
  volatile bool need_close_;
  bool is_closed_;
  int64_t prev_fail_add_cnt_;
  int64_t prev_print_stat_us_;
  uint64_t tenant_id_;
  ObSSMemMacroCacheEvictCtx evict_ctx_;
  ObSSMemMacroCacheEvictWriteEntry *total_entry_arr_;
  common::ObFixedQueue<ObSSMemMacroCacheEvictWriteEntry> free_entry_list_;
  common::ObFixedQueue<ObSSMemMacroCacheEvictWriteEntry> write_entry_list_;
  ObSSMemMacroCacheBufMgr &buf_mgr_;
  ObSSMemMacroCacheMetaMgr &meta_mgr_;
  ObSSMemMacroCacheStat &cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_EVICT_TASK_H_ */

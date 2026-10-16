/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_ss_tmp_file_write_buffer_index_cache.h"
#include "storage/tmp_file/ob_i_tmp_file.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSharedStorageTmpFile;
class ObSSTmpFileFlushManager;
class ObSSTmpFileAsyncFlushWaitTask;
class ObSSTmpFileFlushPriorityManager;
class ObSSTmpFileAsyncFlushWaitTaskHandle;

struct ObSSTmpFileFlushContext
{
public:
  explicit ObSSTmpFileFlushContext();
  ObSSTmpFileFlushContext(const blocksstable::MacroBlockId &id,
                          const int64_t expect_flush_page_num,
                          const int64_t write_info_offset,
                          const uint32_t cur_page_id,
                          const int64_t cur_virtual_page_id);
  void init(const int64_t write_info_offset, const uint32_t cur_page_id, const int64_t cur_virtual_page_id);
  void append_batch(blocksstable::ObStorageObjectHandle *object_handle, char *flush_buff);
  void append_page(const int64_t this_page_cpy_len, const uint32_t next_page_id, const int64_t next_virtual_page_id);
  int flush_io();
  void finish_batch();
  OB_INLINE bool is_finished() const
  {
    return cur_flush_page_count_ >= max_flush_page_count_;
  }
  OB_INLINE bool should_this_batch_continue() const
  {
    return cur_flush_page_count_ < max_flush_page_count_ && !can_seal_;
  }
  OB_INLINE int32_t get_batch_expected_flush_size() const
  {
    return ObTmpFileGlobal::SS_BLOCK_SIZE - write_info_offset_ % ObTmpFileGlobal::SS_BLOCK_SIZE;
  }
  OB_INLINE void set_can_seal()
  {
    can_seal_ = true;
  }
  TO_STRING_KV(K(id_.second_id()), K(io_desc_), K(io_timeout_ms_),
               KP(object_handle_), KP(flush_buff_),
               K(cur_flush_page_count_), K(max_flush_page_count_),
               K(write_info_offset_), K(this_batch_flush_data_size_),
               K(cur_page_id_), K(cur_virtual_page_id_), K(can_seal_));

public:
  const blocksstable::MacroBlockId id_;                  // temporary file id
  const common::ObIOFlag io_desc_;                       // io flage from user
  int64_t io_timeout_ms_;
  blocksstable::ObStorageObjectHandle * object_handle_;  // write handle for once batch
  char * flush_buff_;                      // write buffer for once batch
  int64_t cur_flush_page_count_;           // total flushed page nums for once flush
  const int64_t max_flush_page_count_;     // max flushed page nums for once flush
  int64_t write_info_offset_;              // write_info_offset_ % SS_BLOCK_SIZE = write offset in macro block
                                           // this var records the offset has been flushed
  int64_t this_batch_flush_data_size_;     // flushed data size for once batch
  uint32_t cur_page_id_;                   // current page id in wbp for each `append_page`
  int64_t cur_virtual_page_id_;            // current virtual page id in wbp for each `append_page`
  bool can_seal_;                          // can this io batch set `sealed_`
};

struct ObTmpFileNode : public ObDLinkBase<ObTmpFileNode>
{
  ObTmpFileNode(ObSharedStorageTmpFile &file) : file_(file) {}
  ObSharedStorageTmpFile &file_;
};

class ObSharedStorageTmpFile final : public ObITmpFile
{
public:
  ObSharedStorageTmpFile();
  virtual ~ObSharedStorageTmpFile();
  int init(const uint64_t tenant_id, const blocksstable::MacroBlockId id,
           ObSSTmpWriteBufferPool *wbp,
           ObSSTmpFileFlushManager *flush_mgr,
           ObSSTmpFileRemoveManager *remove_mgr,
           ObIAllocator *callback_allocator,
           ObIAllocator *wbp_index_cache_allocator,
           ObIAllocator *wbp_index_cache_bkt_allocator,
           const char* label);
  virtual void reset() override;
  virtual int release_resource() override;
  virtual int write(ObTmpFileIOWriteCtx &io_ctx) override;
  virtual int truncate(const int64_t truncate_offset) override;
  int seal();

  // for flush
  int update_meta_after_flush(const ObSSTmpFileAsyncFlushWaitTask &wait_task);
  int flush(const bool flush_as_block,
            int64_t &flush_size,
            ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle);
  void release_flush_lock();

public:
  bool is_sealed();
  int64_t cal_wbp_begin_offset();
  int64_t get_data_page_nums(const bool only_full_page = false);
  // for virtual table
  virtual int copy_info_for_virtual_table(ObTmpFileBaseInfo &tmp_file_info) override;

public:
  OB_INLINE void set_data_page_flush_level(int64_t data_page_flush_level)
  {
    data_page_flush_level_ = data_page_flush_level;
  }
  OB_INLINE int64_t get_data_page_flush_level() const { return data_page_flush_level_; }
  OB_INLINE ObTmpFileNode *get_data_flush_node() { return &data_flush_node_; }
  int reinsert_data_flush_node();
  int remove_data_flush_node();
  OB_INLINE void set_discard_reason_(const int discard_reason) { ATOMIC_STORE(&discard_reason_, discard_reason); }
  OB_INLINE int get_discard_reason_() const { return ATOMIC_LOAD(&discard_reason_); }

  INHERIT_TO_STRING_KV("ObITmpFile", ObITmpFile,
                       K(id_), K(discard_reason_),
                       K(cached_page_nums_), K(write_back_data_page_num_),
                       K(begin_page_id_), K(begin_page_virtual_id_),
                       K(end_page_id_),
                       K(data_page_flush_level_),
                       KP(data_flush_node_.get_next()),
                       KP(flush_mgr_), KP(remove_mgr_),
                       KP(wbp_), KP(flush_prio_mgr_), KP(&page_idx_cache_),
                       K(write_info_), K(read_info_));

private:
  virtual int inner_read_valid_part_(ObTmpFileIOReadCtx &io_ctx) override;
  int inner_read_from_disk_(const int64_t expected_read_disk_size, ObTmpFileIOReadCtx &io_ctx);
  int inner_direct_read_from_block_(const int64_t block_index,
                                    const int64_t begin_read_offset_in_block,
                                    const int64_t end_read_offset_in_block,
                                    ObTmpFileIOReadCtx &io_ctx);
  int inner_aggregate_read_from_block_(const int64_t block_index,
                                       const int64_t begin_read_offset_in_block,
                                       const int64_t end_read_offset_in_block,
                                       ObTmpFileIOReadCtx &io_ctx,
                                       int64_t &total_kv_cache_page_read_cnt,
                                       int64_t &total_uncached_page_read_cnt,
                                       int64_t &kv_cache_page_read_hits,
                                       int64_t &uncached_page_read_hits,
                                       int64_t &aggregate_read_io_cnt);
  int collect_page_io_infos_(const int64_t block_index,
                             const int64_t begin_read_offset_in_block,
                             const int64_t end_read_offset_in_block,
                             const bool need_aggregate_read,
                             ObTmpFileIOReadCtx &io_ctx,
                             common::ObIArray<ObTmpFileIOReadCtx::ObPageCacheHandle> &page_cache_handles,
                             common::ObIArray<std::pair<ObTmpPageCacheKey, int64_t>> &page_infos);
  int inner_read_from_wbp_(ObTmpFileIOReadCtx &io_ctx);

  int inner_write_(ObTmpFileIOWriteCtx &io_ctx);
  int inner_fill_tail_page_(ObTmpFileIOWriteCtx &io_ctx);
  int load_disk_tail_page_and_rewrite_(ObTmpFileIOWriteCtx &io_ctx);
  int append_write_memory_tail_page_(ObTmpFileIOWriteCtx &io_ctx);
  int inner_write_continuous_pages_(ObTmpFileIOWriteCtx &io_ctx);
  int alloc_and_write_pages_(const ObTmpFileIOWriteCtx &io_ctx,
                             ObArray<uint32_t> &alloced_page_id,
                             int64_t &actual_write_size);
  int swap_page_to_disk_(const ObTmpFileIOWriteCtx &io_ctx, const int64_t abs_timeout_ms);

  int adjust_flush_wait_task_info_(const int64_t flushed_offset, const int64_t file_size_when_flush,
                                   uint32_t &wait_task_begin_page_id,
                                   int64_t &wait_task_begin_page_virtual_id,
                                   int64_t &expected_flushed_page_num,
                                   int64_t &flushed_page_num);
  void try_to_seal_in_flush_(const int64_t flushed_offset, const int64_t expected_flushed_page_num,
                             blocksstable::ObStorageObjectHandle &obj_handle,
                             int64_t &flushed_page_num);
  int rollback_failed_flush_pages_(const int64_t failed_page_num, const uint32_t start_page_id,
                                   const int64_t start_virtual_page_id);
  int inner_async_flush_(const bool flush_all_pages,
                         int64_t &flush_size,
                         ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle);
  int inner_async_flush_block_(ObSSTmpFileFlushContext &flush_context, int64_t &batch_flush_size);

  int truncate_cached_pages_(const int64_t truncate_offset, const int64_t wbp_begin_offset);
  int truncate_persistent_pages_(const int64_t truncate_offset);
  int truncate_the_first_wbp_page_();

  int inner_seal_();
  virtual int inner_delete_file_() override;

  // for virtual table
  virtual void inner_set_read_stats_vars_(const ObTmpFileIOReadCtx &ctx, const int64_t read_size) override;
  virtual void inner_set_write_stats_vars_(const ObTmpFileIOWriteCtx &ctx) override;
private:
  int64_t cal_wbp_begin_offset_() const;
  int64_t cal_expect_flush_page_nums_(const bool flush_all, const int64_t flush_idx_in_block);
  int64_t get_data_page_nums_(const bool only_full_page = false);
  bool is_flushing_();
  int64_t get_dirty_data_page_size_() const;
  int reinsert_data_flush_node_();
  int insert_or_update_data_flush_node_();
  OB_INLINE int64_t get_last_flushed_block_id_(const int64_t last_flushed_page_virtual_id)
  {
    return last_flushed_page_virtual_id / ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS;
  }
  OB_INLINE int64_t get_block_virtual_id_(const int64_t offset_in_file, const bool is_open_interval) const
  {
    return is_open_interval ?
           common::upper_align(offset_in_file, ObTmpFileGlobal::SS_BLOCK_SIZE) / ObTmpFileGlobal::SS_BLOCK_SIZE - 1 :
           offset_in_file / ObTmpFileGlobal::SS_BLOCK_SIZE;
  }
  OB_INLINE int64_t get_end_page_virtual_id_() const
  {
    return cached_page_nums_ == 0 ?
           ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID :
           get_page_virtual_id_(file_size_, true /*is_open_interval*/);
  }

  OB_INLINE int64_t get_offset_in_block_(int64_t file_offset) const
  {
    return file_offset % ObTmpFileGlobal::SS_BLOCK_SIZE;
  }

private:
  blocksstable::MacroBlockId id_;
  int discard_reason_; // indicates the file must be discarded after flush failed
  int64_t cached_page_nums_;      // page nums in write buffer pool
  int64_t write_back_data_page_num_;
  uint32_t begin_page_id_;        // the first page index in write buffer pool
  int64_t begin_page_virtual_id_;
  uint32_t end_page_id_;          // the last page index in write buffer pool
  int64_t diag_log_print_cnt_;    // print diagnosis log every PRINT_LOG_FILE_SIZE
  int64_t data_page_flush_level_;
  ObTmpFileNode data_flush_node_;
  ObSpinLock serial_write_lock_; // handle conflicts between multiple writes
  ObSpinLock last_page_lock_; // handle conflicts between writing and evicting for last page
  common::SpinRWLock flush_rwlock_; // prevent concurrent read/flush IO to the same segment
  ObSSTmpFileFlushManager *flush_mgr_;
  ObSSTmpFileRemoveManager *remove_mgr_;
  ObSSTmpWriteBufferPool *wbp_;
  ObSSTmpFileFlushPriorityManager *flush_prio_mgr_;
  ObSSTmpFileWBPIndexCache page_idx_cache_;
  /********for virtual table begin********/
  ObTmpFileWriteInfo write_info_;
  ObTmpFileReadInfo read_info_;
  /********for virtual table end  ********/
};

class ObSSTmpFileHandle final : public ObITmpFileHandle
{
public:
  ObSSTmpFileHandle() : ObITmpFileHandle() {}
  ObSSTmpFileHandle(ObSharedStorageTmpFile *ptr) : ObITmpFileHandle(ptr) {}
  ObSSTmpFileHandle(const ObSSTmpFileHandle &handle) : ObITmpFileHandle(handle) {}
  ObSSTmpFileHandle & operator=(const ObSSTmpFileHandle &other)
  {
    ObITmpFileHandle::operator=(other);
    return *this;
  }
  OB_INLINE ObSharedStorageTmpFile * get() const { return static_cast<ObSharedStorageTmpFile*>(ptr_); }
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_
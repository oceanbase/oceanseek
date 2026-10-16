/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/task/ob_gc_segment_file_task.h"
#include "storage/shared_storage/task/ob_clean_old_version_tmp_file_task.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_small_allocator.h"
#include "share/ob_ptr_handle.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

class ObSegmentFileManager;

enum class ObSSTmpFileSegDeleteType : uint8_t;
enum class ObSSTmpFileSegMetaOpType : uint8_t;
class ObSSTmpFileAppendCallback;
class ObTenantDiskSpaceManager;

struct TmpFileSegId final
{
public:
  TmpFileSegId() : tmp_file_id_(INT64_MAX), segment_id_(INT64_MAX) {}
  TmpFileSegId(const int64_t tmp_file_id, const int64_t segment_id)
    : tmp_file_id_(tmp_file_id),
      segment_id_(segment_id) {}
  TmpFileSegId(const TmpFileSegId &other) { *this = other; }
  ~TmpFileSegId() {}
  TmpFileSegId& operator =(const TmpFileSegId &other)
  {
    tmp_file_id_ = other.tmp_file_id_;
    segment_id_ = other.segment_id_;
    return *this;
  }
  bool is_valid() const
  {
    return ((INT64_MAX != tmp_file_id_) && (INT64_MAX != segment_id_));
  }
  bool operator==(const TmpFileSegId &other) const
  {
    return (tmp_file_id_ == other.tmp_file_id_) && (segment_id_ == other.segment_id_);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tmp_file_id_, sizeof(tmp_file_id_), hash_val);
    hash_val = murmurhash(&segment_id_, sizeof(segment_id_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(tmp_file_id), K_(segment_id));
public:
  int64_t tmp_file_id_;
  int64_t segment_id_;
};

// Concurrent access operation types
enum class ObTmpFileAccessType : uint8_t
{
  READ_ACCESS = 0,
  WRITE_ACCESS = 1
};

// Concurrent read/write access control structure
struct TmpFileConflictInfo final
{
public:
  TmpFileConflictInfo();
  TmpFileConflictInfo(const bool is_writing, const int64_t active_readers_count);
  ~TmpFileConflictInfo() { reset(); }
  void inc_ref_count();
  void dec_ref_count();
  void reset();
  void set_segment_file_manager(ObSegmentFileManager *mgr) { segment_file_manager_ = mgr; }
  
  // Conflict detection methods with proper encapsulation
  int try_acquire_write_access();
  int release_write_access();
  int try_acquire_read_access();
  int release_read_access();
  
  TO_STRING_KV(K_(is_writing), K_(active_readers_count));

public:
  common::SpinRWLock concurrent_access_lock_;   // Fine-grained lock for concurrent access control
  volatile bool is_writing_;                    // Whether a write operation is in progress
  volatile int64_t active_readers_count_;       // Count of active concurrent readers
  int64_t ref_cnt_;       // segment file's reference count
  ObSegmentFileManager *segment_file_manager_;  // for conflict_info_allocator_ deallocation, nullptr means use OB_DELETE
};

struct TmpFileMeta final
{
public:
  TmpFileMeta();
  TmpFileMeta(const bool is_in_local, const int32_t valid_length, const int32_t target_valid_length = 0);
  ~TmpFileMeta() { reset(); }
  void inc_ref_count();
  void dec_ref_count();
  int set_target_valid_length(const int32_t target_valid_length, const blocksstable::MacroBlockId &macro_id);
  void set_flush_info(const int32_t flushed_valid_length, const bool is_flushed_seg_sealed);
  void set_sealed();
  int check_tmp_file_appending(ObSSTmpFileAppendCallback &callback);
  bool is_tmp_file_appending(const int64_t flushed_valid_len);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(is_in_local), K_(is_sealed), K_(is_flushed_seg_sealed), K_(flushed_valid_length),
               K_(valid_length), K_(target_valid_length), K_(ref_cnt), K_(append_timestamp_us));

private:
  bool is_tmp_file_appending_nolock(const int64_t flushed_valid_len);

public:
  common::SpinRWLock lock_;     // segment file's meta operator lock
  volatile bool is_in_local_; // segment file store in local or object storage
  // indicate whether this seg is sealed or not, which is set to true when seal this seg
  bool is_sealed_;
  // it is set to false after flush unsealed seg, and set to true after flush sealed seg.
  // true: this means there exists one file on object storage, which is without valid length suffix.
  //       this meta can be deleted.
  // false: this means there exists one file on object storage, which is with valid length suffix.
  //        this meta cannot be deleted. otherwise, resulting in read object without valid length,
  //        and return OB_OBJECT_NOT_EXIST.
  // Note: @is_flushed_seg_sealed_ and @flushed_valid_length_ must 'update and read atomically'
  bool is_flushed_seg_sealed_;
  // indicate the valid length that has been flushed to object storage, which is leveraged to judge
  // if this seg has been appended after flush.
  int32_t flushed_valid_length_;
  int32_t valid_length_;  // segment file's valid length
  int32_t target_valid_length_; // current append segment file's target valid length
  int64_t ref_cnt_;       // segment file's reference count
  int64_t append_timestamp_us_;  // segment file's last append time stamp
};

class TmpFileConflictInfoHandle final : public ObPtrHandle<TmpFileConflictInfo>
{
public:
  TmpFileConflictInfoHandle();
  virtual ~TmpFileConflictInfoHandle();
  void reset();
  OB_INLINE bool is_valid() const { return (nullptr != ptr_); }
  int assign(const TmpFileConflictInfoHandle &other);
  TmpFileConflictInfo *get_conflict_info() const { return ptr_; }
  OB_INLINE bool is_writing() const { return ((nullptr == ptr_) ? false : ptr_->is_writing_); }
  OB_INLINE int64_t get_reader_count() const { return ((nullptr == ptr_) ? 0 : ptr_->active_readers_count_); }
  int try_acquire_write_access();
  int release_write_access();
  int try_acquire_read_access();
  int release_read_access();
  
  TO_STRING_KV(KP_(ptr));
};

class TmpFileMetaHandle final : public ObPtrHandle<TmpFileMeta>
{
public:
  TmpFileMetaHandle();
  virtual ~TmpFileMetaHandle();
  void reset();
  int set_tmpfile_meta(const bool is_in_local, const int32_t valid_length, const int32_t target_valid_length);
  OB_INLINE bool is_valid() const { return (nullptr != ptr_); }
  int assign(const TmpFileMetaHandle &other);
  void update_tmpfile_meta(const bool is_in_local, const int64_t valid_length);
  int set_target_valid_length(const int32_t target_valid_length, const blocksstable::MacroBlockId &macro_id);
  void set_flush_info(const int32_t flushed_valid_length, const bool is_flushed_seg_sealed)
  {
    if (OB_NOT_NULL(ptr_)) {
      ptr_->set_flush_info(flushed_valid_length, is_flushed_seg_sealed);
    }
  }
  void set_sealed();
  int check_tmp_file_appending(ObSSTmpFileAppendCallback &callback);
  bool is_tmp_file_appending(const int64_t flushed_valid_len) const
  {
    return ((nullptr == ptr_) ? false : ptr_->is_tmp_file_appending(flushed_valid_len));
  }
  OB_INLINE bool is_in_local() const { return ((nullptr == ptr_) ? false : ptr_->is_in_local_); }
  OB_INLINE bool is_sealed() const { return ((nullptr == ptr_) ? false : ptr_->is_sealed_); }
  OB_INLINE int32_t get_valid_length() const { return ((nullptr == ptr_) ? 0 : ptr_->valid_length_); }
  OB_INLINE int32_t get_target_valid_length() const { return ((nullptr == ptr_) ? 0 : ptr_->target_valid_length_); }
  OB_INLINE int64_t get_append_timestamp_us() const { return ((nullptr == ptr_) ? OB_INVALID_TIMESTAMP : ptr_->append_timestamp_us_); }
  TmpFileMeta *get_tmpfile_meta() const { return ptr_; }
  TO_STRING_KV(KP_(ptr));
};

class ObSegMetaUpdateOp
{
protected:
  typedef common::hash::HashMapPair<TmpFileSegId, TmpFileMetaHandle> MapKV;
public:
  ObSegMetaUpdateOp(const TmpFileMetaHandle &meta_handle)
  {
    // op can only be used once, because op used again, meta_handle will assign failed
    meta_handle_.assign(meta_handle);
  }
  virtual ~ObSegMetaUpdateOp() {}
  void operator()(MapKV &entry)
  {
    entry.second.update_tmpfile_meta(meta_handle_.is_in_local(), meta_handle_.get_valid_length());
  }
private:
  TmpFileMetaHandle meta_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSegMetaUpdateOp);
};

struct ObSSTmpFileAppendParam final
{
public:
  ObSSTmpFileAppendParam();
  ObSSTmpFileAppendParam(const bool need_write_io,
                         const bool need_free_file_size,
                         const int64_t free_file_size);
  ~ObSSTmpFileAppendParam() {}
  TO_STRING_KV(K_(need_write_io), K_(need_free_file_size), K_(free_file_size));

public:
  common::ObArenaAllocator arena_allocator_;
  bool need_write_io_; // no need write io when current_io.valid_length <= seg_meta.valid_length
  bool need_free_file_size_; // object storage write: false, local cache write: true
  int64_t free_file_size_; // if need_free_file_size_ is true, free_file_size_ is valid
  TmpFileConflictInfoHandle conflict_info_handle_; // conflict info handle for concurrent access control
};

class ObSegmentFileManager
{
  friend struct TmpFileConflictInfo; // Allow TmpFileConflictInfo to call free_conflict_info
public:
  ObSegmentFileManager();
  ~ObSegmentFileManager();
  int init(ObTenantFileManager *file_manager);
  int start();
  void destroy();
  void stop();
  void wait();
  
  // Concurrent read-write support methods
  int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                   blocksstable::ObStorageObjectHandle &object_handle);
  int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info,
                                  blocksstable::ObStorageObjectHandle &object_handle);
  int insert_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle);
  int delete_meta(const TmpFileSegId &seg_id);
  int delete_wild_meta(const int64_t tmp_file_id);
  int update_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle);
  int set_sealed(const TmpFileSegId &seg_id);
  int try_get_seg_meta(const TmpFileSegId &seg_id, TmpFileMetaHandle &meta_handle, bool &is_meta_exist);
  int try_get_seg_conflict_info(const TmpFileSegId &seg_id, TmpFileConflictInfoHandle &conflict_info_handle, bool &is_conflict_info_exist);
  int push_seg_file_to_remove_queue(const TmpFileSegId &seg_id, const int64_t valid_length);
  int exec_remove_task_once();
  int clean_old_version_tmp_files();

public:
  static const int64_t UNSEALED_TMP_FILE_FLUSH_THRESHOLD = 60L * 1000L * 1000L; //60s
  class ObUnsealedSegFile: public common::ObLink
  {
  public:
    ObUnsealedSegFile(const TmpFileSegId &seg_id, const int64_t valid_length)
    : seg_id_(seg_id), valid_length_(valid_length) {}
    ~ObUnsealedSegFile() = default;
    TO_STRING_KV(K_(seg_id), K_(valid_length));
  public:
    TmpFileSegId seg_id_;
    int64_t valid_length_;
  };

private:
  class ObSetSealedCallback
  {
  public:
    ObSetSealedCallback() {}
    virtual ~ObSetSealedCallback() {}
    void operator()(common::hash::HashMapPair<TmpFileSegId, TmpFileMetaHandle> &entry)
    {
      entry.second.set_sealed();
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(ObSetSealedCallback);
  };

private:
  bool check_tmp_file_disk_size_enough();
  int async_write_dual(ObStorageObjectWriteInfo &write_info,
                       const ObStorageObjectHandle &ori_object_handle,
                       ObStorageObjectHandle &object_handle,
                       ObSSTmpFileAppendParam &append_param);
  int handle_local_write_failure_and_fallback(const ObStorageObjectHandle &ori_object_handle,
                                               ObSSTmpFileAppendParam &append_param,
                                               blocksstable::ObStorageObjectWriteInfo &write_info,
                                               blocksstable::ObStorageObjectHandle &object_handle);
  int handle_write_on_meta_exist(TmpFileMetaHandle &meta_handle,
                                 blocksstable::ObStorageObjectWriteInfo &write_info,
                                 blocksstable::ObStorageObjectHandle &object_handle,
                                 ObSSTmpFileAppendParam &append_param);
  int handle_write_on_meta_not_exist(blocksstable::ObStorageObjectWriteInfo &write_info,
                                     blocksstable::ObStorageObjectHandle &object_handle,
                                     ObSSTmpFileAppendParam &append_param);
  int handle_write_with_local_seg(TmpFileMetaHandle &meta_handle,
                                  blocksstable::ObStorageObjectWriteInfo &write_info,
                                  blocksstable::ObStorageObjectHandle &object_handle,
                                  ObSSTmpFileAppendParam &append_param);
  int handle_write_with_remote_seg(TmpFileMetaHandle &meta_handle,
                                   blocksstable::ObStorageObjectWriteInfo &write_info,
                                   blocksstable::ObStorageObjectHandle &object_handle,
                                   ObSSTmpFileAppendParam &append_param);
  int read_existing_data_and_append_buf(const MacroBlockId &file_id,
                                        const TmpFileMetaHandle &meta_handle,
                                        blocksstable::ObStorageObjectWriteInfo &write_info,
                                        ObSSTmpFileAppendParam &append_param);
  int write_through_on_meta_not_exist(const TmpFileSegId &seg_id,
                                      blocksstable::ObStorageObjectWriteInfo &write_info,
                                      ObSSTmpFileAppendParam &append_param);
  int write_through_with_remote_seg(const TmpFileSegId &seg_id,
                                    const TmpFileMetaHandle &meta_handle,
                                    blocksstable::ObStorageObjectWriteInfo &write_info,
                                    ObSSTmpFileAppendParam &append_param);
  int simulate_io_result(const blocksstable::ObStorageObjectWriteInfo &write_info,
                         blocksstable::ObStorageObjectHandle &object_handle) const;
  int construct_tmp_file_write_callback(const TmpFileSegId &seg_id,
                                        const TmpFileMetaHandle &meta_handle,
                                        TmpFileConflictInfoHandle &conflict_info_handle,
                                        const ObSSTmpFileSegMetaOpType seg_meta_op_type,
                                        const ObSSTmpFileSegDeleteType seg_del_type,
                                        const bool need_put_or_update_macro_cache,
                                        common::ObIOCallback *&io_callback,
                                        const int64_t del_seg_valid_len);
  int aio_write_with_create_parent_dir(ObSSTmpFileAppendParam &append_param,
                                       blocksstable::ObStorageObjectWriteInfo &write_info,
                                       const blocksstable::ObStorageObjectHandle &ori_object_handle,
                                       blocksstable::ObStorageObjectHandle &object_handle);
  bool should_fallback_to_object_storage(const int ret, const blocksstable::ObStorageObjectWriteInfo &write_info) const;
  int construct_tmp_file_read_callback(const TmpFileSegId &seg_id,
                                       TmpFileConflictInfoHandle &conflict_info_handle,
                                       ObStorageObjectReadInfo &read_info);
  int get_or_create_conflict_info_handle(const TmpFileSegId &seg_id,
                                         TmpFileConflictInfoHandle &conflict_info_handle,
                                         const ObTmpFileAccessType &access_type);
  int get_or_create_conflict_info_handle(const MacroBlockId &macro_id,
                                         ObSSTmpFileAppendParam &append_param,
                                         const ObTmpFileAccessType &access_type);
  int delete_old_version_files_in_old_dir(const char *tmp_data_path,
                                          const int64_t file_num,
                                          int64_t &delete_success_count);
  int delete_old_version_files_in_scatter_dir(const char *tmp_data_path,
                                              const int64_t file_num,
                                              int64_t &delete_success_count);
  int allocate_conflict_info_handle(const bool is_writing,
                                    const int64_t active_readers_count,
                                    TmpFileConflictInfoHandle &handle);
  void free_conflict_info(TmpFileConflictInfo *ptr);

private:
  typedef hash::ObHashMap<TmpFileSegId, TmpFileMetaHandle> SegMetaMap;
  typedef hash::ObHashMap<TmpFileSegId, TmpFileConflictInfoHandle> SegConflictInfoHandleMap;
  class GetWildTmpFileSegIdFunc
  {
  public:
    explicit GetWildTmpFileSegIdFunc(const int64_t tmp_file_id);
    virtual ~GetWildTmpFileSegIdFunc() {}
  public:
    int operator()(const common::hash::HashMapPair<TmpFileSegId, TmpFileMetaHandle> &entry);
    TO_STRING_KV(K_(tmp_file_id), K_(wild_seg_ids));
  public:
    int64_t tmp_file_id_;
    ObSEArray<TmpFileSegId, 16> wild_seg_ids_;
  };

  class GetWildTmpFileSegIdFuncForConflict
  {
  public:
    explicit GetWildTmpFileSegIdFuncForConflict(const int64_t tmp_file_id);
    virtual ~GetWildTmpFileSegIdFuncForConflict() {}
  public:
    int operator()(const common::hash::HashMapPair<TmpFileSegId, TmpFileConflictInfoHandle> &entry);
    TO_STRING_KV(K_(tmp_file_id), K_(wild_seg_ids));
  public:
    int64_t tmp_file_id_;
    ObSEArray<TmpFileSegId, 16> wild_seg_ids_;
  };

private:
  bool is_inited_;
  bool is_stop_;
  ObTenantFileManager *file_manager_;
  common::ObConcurrentFIFOAllocator allocator_;
  SegMetaMap seg_meta_map_; // store unsealed seg meta and local sealed seg meta
  SegConflictInfoHandleMap seg_conflict_info_map_; // store conflict info handle
  ObGCSegmentFileTask gc_segment_file_task_;
  ObCleanOldVersionTmpFileTask clean_old_version_tmp_file_task_;
  common::ObLinkQueue unsealed_seg_file_remove_queue_;
  common::ObSmallAllocator conflict_info_allocator_;  // for TmpFileConflictInfo allocation
  int64_t conflict_info_live_count_;  // atomic counter for live TmpFileConflictInfo objects
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_ */

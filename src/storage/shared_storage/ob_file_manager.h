/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_

#include "lib/queue/ob_link_queue.h"
#include "common/storage/ob_io_device.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_ss_preread_cache_manager.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/task/ob_persist_disk_space_task.h"
#include "storage/shared_storage/task/ob_calibrate_disk_space_task.h"
#include "storage/shared_storage/task/ob_gc_local_major_data_task.h"
#include "storage/shared_storage/task/ob_check_expand_disk_size_task.h"
#include "storage/shared_storage/ob_ss_fd_cache_mgr.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/task/ob_check_data_disk_avail_task.h"
#include "storage/shared_storage/ob_segment_file_manager.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_flush_map.h"
#include "storage/shared_storage/task/ob_server_slog_flush_task.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/task/ob_tmp_seq_file_clean_task.h"
#include "storage/shared_storage/task/ob_tenant_io_detect_task.h"

namespace oceanbase
{
namespace share
{
  class ObBackupDest;
  class ObBackupStorageInfo;
}
namespace storage
{

struct ObSegmentFileInfo : public common::ObLink
{
public:
  ObSegmentFileInfo() : file_id_(), func_type_(0) {}
  ObSegmentFileInfo(const blocksstable::MacroBlockId file_id, const uint8_t func_type)
    : file_id_(file_id), func_type_(func_type) {}
  ~ObSegmentFileInfo() {}
  void reset();
  bool is_valid() const { return file_id_.is_valid(); }
  TO_STRING_KV(K_(file_id), K_(func_type));

public:
  blocksstable::MacroBlockId file_id_;
  uint8_t func_type_;
};

struct ObCalibrateDiskSpaceResult
{
public:
  ObCalibrateDiskSpaceResult()
    : total_file_size_(0), hot_tablet_total_used_size_(0), del_tmp_seq_file_size_(0),
      tmp_seq_file_size_(0), tmp_seq_file_cnt_(0), total_dir_size_(0), total_dir_num_(0) {}
  virtual ~ObCalibrateDiskSpaceResult() { reset(); }
  void reset()
  {
    total_file_size_ = 0;
    hot_tablet_total_used_size_ = 0;
    del_tmp_seq_file_size_ = 0;
    tmp_seq_file_size_ = 0;
    tmp_seq_file_cnt_ = 0;
    total_dir_size_ = 0;
    total_dir_num_ = 0;
  }

  int get_dir_calibrate_size(const int64_t initial_dir_num, int64_t &calibrate_size) const;

  TO_STRING_KV(K_(total_file_size), K_(hot_tablet_total_used_size), K_(del_tmp_seq_file_size), K_(tmp_seq_file_size),
               K_(tmp_seq_file_cnt), K_(total_dir_size), K_(total_dir_num));
public:
  static const int64_t DIR_SIZE_OVERHEAD_PERCENTAGE = 10; // 10%
  int64_t total_file_size_;
  int64_t hot_tablet_total_used_size_;
  int64_t del_tmp_seq_file_size_;
  int64_t tmp_seq_file_size_;
  int64_t tmp_seq_file_cnt_;
  int64_t total_dir_size_;
  int64_t total_dir_num_;
};

class ObCalibrateDiskSpaceUtil
{
public:
  static int get_deviation_threshold(const int64_t deviation_alloc_size, int64_t &deviation_threshold);

private:
  // deviation percentage threshold for listed_size > disk_space_mgr_size
  static const int64_t ACTUAL_MORE_DEVIATION_PERCENTAGE_THRESHOLD = 1;
  // deviation percentage threshold for listed_size <= disk_space_mgr_size
  static const int64_t ACTUAL_LESS_DEVIATION_PERCENTAGE_THRESHOLD = 5;
  static const int64_t MAX_BYTES_THRESHOLD = 20 * 1024L * 1024L * 1024L; // 20GB
  static const int64_t MIN_BYTES_THRESHOLD = 50 * 1024L * 1024L; // 50MB
};

class ObBaseFileManager
{
public:
  static const int64_t OB_MAX_FILE_PATH_LENGTH = 1024;
  static const int64_t OB_DEFAULT_BUCKET_NUM = 90000;
  static const int64_t MB = 1024L * 1024L;
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 64;
  static const int64_t MAX_RETRY_ALLOC_CACHE_SPACE_CNT = 5;
  static const int64_t PRINT_LOG_INTERVAL = 30L * 1000L * 1000L; //30s
  ObBaseFileManager();
  virtual ~ObBaseFileManager();
  bool is_inited() const { return is_inited_; }
  virtual int init(const uint64_t tenant_id);
  virtual int start();
  virtual void destroy();
  OB_INLINE bool is_started() const { return is_started_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  int get_storage_dest(share::ObBackupDest &storage_dest);
  int get_io_detect_file_fd() const { return io_detect_fd_; }
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                               blocksstable::ObStorageObjectHandle &object_handle) = 0;
  int pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);
  virtual int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);
  int delete_remote_file(const blocksstable::MacroBlockId &file_id);
  virtual int delete_remote_file_with_ls_context(const blocksstable::MacroBlockId &file_id,
                                                 const int64_t ls_epoch_id,
                                                 const int64_t ls_id);
  int delete_file(const blocksstable::MacroBlockId &file_id);
  virtual int delete_file_with_ls_context(const blocksstable::MacroBlockId &file_id,
                                          const int64_t ls_epoch_id,
                                          const int64_t ls_id);
  int delete_remote_dir(const char *dir_path);
  int fsync_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0, const int64_t ls_id = 0);
  int get_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length, const int64_t ls_id = 0);
  int get_local_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length, const int64_t ls_id = 0);
  int get_remote_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length, const int64_t ls_id = 0);
  int is_exist_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id = 0);
  int is_exist_local_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id = 0);
  int is_exist_remote_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id = 0);
  int list_remote_files(const char *dir_path, ObBaseDirEntryOperator &list_op);
  int list_remote_macro_files(const bool is_ls_inner_tablet, const int64_t ls_id,
                              const int64_t tablet_id, const int64_t reorganization_scn,
                              common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  int list_remote_macro_files_by_op(const common::ObTabletID &tablet_id, const int64_t ls_id,
                                    const int64_t reorganization_scn, const char *sstable_type_str,
                                    const int64_t op_id, common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  OB_INLINE common::ObIAllocator &get_io_callback_allocator() { return io_callback_allocator_; }
  OB_INLINE common::ObSmallObjPool<ObSSFdCacheNode> &get_fd_cache_node_pool() { return fd_cache_node_pool_; }
  int64_t get_start_server_time_s() { return start_server_time_s_; }
  int get_slog_id_range(const uint64_t tenant_id, const int64_t tenant_epoch_id, int64_t &min_slog_id, int64_t &max_slog_id);
  int list_private_ckpt_file(const uint64_t tenant_id, const int64_t tenant_epoch_id, ObIArray<int64_t> &object_ids); // No guarantee of order
  int update_flushed_slog_seq(const int64_t flushed_slog_seq);

protected:
  static const int64_t TIME_GUARD_WARN_THRESHOLD = 10L * 1000L * 1000L; // 10s
  int delete_local_dir(const char *dir_path);
  int do_aio_write(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle);
  int do_aio_read(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);
  int list_local_files(const char *dir_path, ObBaseDirEntryOperator &list_op);
  int list_local_scatter_files(const char *dir_path,
                               const int64_t scatter_num,
                               const char *scatter_format,
                               ObBaseDirEntryOperator &list_op);
  int list_local_files_rec(const char *dir_path, share::ObScanDirOp &list_file_op, share::ObScanDirOp &list_dir_op);
  int restore_flushed_slog_seq();

  // io detect file
  int create_io_detect_file();
  int fill_io_detect_file();
  void close_io_detect_file();
  virtual int get_io_detect_file_path(char *path, const int64_t length) = 0;

private:
  int try_flush_prev_slog_files(const blocksstable::MacroBlockId &macro_id);

protected:
  uint64_t tenant_id_;
  common::ObConcurrentFIFOAllocator io_callback_allocator_;
  bool is_inited_;
  bool is_started_;
  int64_t start_server_time_s_;
  volatile int64_t flushed_slog_seq_;
  int io_detect_fd_;
  common::ObSmallObjPool<ObSSFdCacheNode> fd_cache_node_pool_;
};

struct ObAtomicWriteFileInfo
{
public:
  ObAtomicWriteFileInfo()
      : ls_id_(0), ls_epoch_id_(0), seq_(0), size_(0), time_s_(0),
        cache_type_(ObSSMacroCacheType::MAX_TYPE)
  {}
  ObAtomicWriteFileInfo(const int64_t ls_id, const int64_t ls_epoch_id, const uint64_t seq, const uint64_t size)
      : ls_id_(ls_id), ls_epoch_id_(ls_epoch_id), seq_(seq), size_(size), time_s_(0),
        cache_type_(ObSSMacroCacheType::MAX_TYPE)
  {}
  virtual ~ObAtomicWriteFileInfo() {}
  void set_time_s(const int64_t time_s) { time_s_ = time_s; }
  TO_STRING_KV(K_(ls_id), K_(ls_epoch_id), K_(seq), K_(size), K_(time_s), K_SS_MACRO_CACHE_TYPE(cache_type_));
public:
  int64_t ls_id_;
  int64_t ls_epoch_id_;
  uint64_t seq_;
  uint64_t size_;
  int64_t time_s_; // the time of the atomic write file entry added to the map
  ObSSMacroCacheType cache_type_; // 记录写入时的cache_type，保证free_file_size与alloc_file_size使用一致的cache_type
};
using ObAtomicWriteFileInfoMap = common::hash::ObHashMap<blocksstable::MacroBlockId, ObAtomicWriteFileInfo>;
using ObAtomicWriteFileInfoMapPairType = common::hash::HashMapPair<blocksstable::MacroBlockId, ObAtomicWriteFileInfo>;

class ObTenantFileManager : public ObBaseFileManager
{
public:
  friend class ObSSMacroCacheFlushTaskBase;
  friend class ObSSMacroCacheFlushTask;
  // TODO(remove-old-gc-code): friend class ObSSGarbageCollector;
  ObTenantFileManager();
  virtual ~ObTenantFileManager();
  static int mtl_init(ObTenantFileManager *&tenant_file_manager);
  virtual int init(const uint64_t tenant_id) override;
  virtual int start() override;
  int start_timer_task();
  virtual int get_io_detect_file_path(char *path, const int64_t length) override;
  virtual void destroy() override;
  void stop();
  void wait();
  int pwrite_cache_block(const int64_t offset,const int64_t size, const char *buf, int64_t &write_size);
  int pread_cache_block(const int64_t offset, const int64_t size, char *buf, int64_t &read_size);
  int fsync_cache_file();
  int read_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta);
  int write_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta);
  int calc_macro_cache_disk_space(const ObSSMacroCacheType cache_type,
                                  const int64_t start_calc_time_s,
                                  ObCalibrateDiskSpaceResult &calibrate_res);
  int calc_disk_space(const char *dir_path,
                      const ObSSMacroCacheType cache_type,
                      const int64_t start_calc_time_s,
                      ObCalibrateDiskSpaceResult &calibrate_res);
  int calc_meta_file_disk_space(const int64_t start_calc_time_s,
                                ObCalibrateDiskSpaceResult &calibrate_res);
  int calc_tmp_file_disk_space(const int64_t start_calc_time_s,
                               ObCalibrateDiskSpaceResult &calibrate_res);
  int calc_macro_block_disk_space(const int64_t start_calc_time_s,
                                  ObCalibrateDiskSpaceResult &calibrate_res);
  // normal tenant async_write_file need alloc_file_size and if OB_SERVER_OUTOF_DISK_SPACE write through
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                               blocksstable::ObStorageObjectHandle &object_handle) override;
  virtual int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle) override;
  int delete_file_with_ls_context(const blocksstable::MacroBlockId &file_id,
                                  const int64_t ls_epoch_id,
                                  const int64_t ls_id) override;
  // logical delete local tmp file is renamed file to .deleted file
  int logical_delete_local_tmp_file(const blocksstable::MacroBlockId &file_id, const bool is_del_seg_meta);
  int delete_remote_file_with_ls_context(const blocksstable::MacroBlockId &file_id,
                                         const int64_t ls_epoch_id,
                                         const int64_t ls_id) override;
  int delete_tmp_file(const blocksstable::MacroBlockId &file_id);
  int delete_local_tmp_file(const blocksstable::MacroBlockId &file_id, const bool is_only_delete_read_cache = false); // used for ObAdmin delete local tmp file read cache
  int delete_tmp_file(const blocksstable::MacroBlockId &file_id, const int64_t length);
  int delete_tmp_files(const ObIArray<blocksstable::MacroBlockId> &file_ids, const ObIArray<int64_t> &lengths);
  int delete_ls_dir(const int64_t ls_id, const int64_t ls_epoch_id); // delete ls dir files
  int delete_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete files
  int delete_local_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete local files
  int delete_remote_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete remote files
  int delete_tmp_seq_file(const blocksstable::MacroBlockId &file_id, const ObAtomicWriteFileInfo &atomic_write_file); // delete .tmp.seq file when atomic write fail
  int delete_shared_tablet_data_dir(const int64_t tablet_id, const int64_t reorganization_scn); // delete shared tablet data dir files
  int delete_local_major_data_dir(const int64_t delete_time_stamp_s); // when restart observer, need gc local major_data dir's major macro file
  int list_tmp_file(ObIArray<blocksstable::MacroBlockId> &tmp_file_ids); // No guarantee of order
  int list_private_tablet_meta(const int64_t ls_id, const int64_t ls_epoch_id,
                               const int64_t tablet_id, const int64_t private_macro_third_id,
                               ObIArray<int64_t> &tablet_meta_vers); // No guarantee of order
  int list_tablet_data_dir(ObIArray<int64_t> &tablet_ids); // No guarantee of order
  int list_shared_tablet_ids(ObIArray<int64_t> &tablet_ids); // No guarantee of order
  int list_private_macro_file(const int64_t tablet_id, const int64_t private_macro_third_id,
                              ObIArray<blocksstable::MacroBlockId> &block_ids); // No guarantee of order
  int schedule_calibrate_disk_space_task(); // used for ObAdmin calibrate local data disk space
  OB_INLINE ObPrereadCacheManager &get_preread_cache_mgr() { return preread_cache_mgr_; }
  int push_to_preread_queue(const blocksstable::MacroBlockId &macro_id,
                            const common::ObTabletID &effective_tablet_id);
  int remove_preread_node(const blocksstable::MacroBlockId &macro_id);
  OB_INLINE ObSegmentFileManager &get_segment_file_mgr() { return segment_file_mgr_; }
  OB_INLINE ObSSFdCacheMgr &get_fd_cache_mgr() { return fd_cache_mgr_;}
  void set_pause_gc() { ATOMIC_STORE(&is_pause_gc_, true); }
  void set_allow_gc() { ATOMIC_STORE(&is_pause_gc_, false); }
  void set_tmp_file_cache_pause_gc() { ATOMIC_STORE(&is_tmp_file_cache_pause_gc_, true); }
  void set_tmp_file_cache_allow_gc() { ATOMIC_STORE(&is_tmp_file_cache_pause_gc_, false); }
  void set_mandatory_calibrate() { calibrate_disk_space_task_.set_mandatory_calibrate(); }
  OB_INLINE bool is_pause_gc() const { return ATOMIC_LOAD(&is_pause_gc_); }
  OB_INLINE bool is_tmp_file_cache_pause_gc() const { return ATOMIC_LOAD(&is_tmp_file_cache_pause_gc_); }
  OB_INLINE int64_t get_tmp_logical_delete_epoch() const
  {
    return ATOMIC_LOAD(&tmp_logical_delete_epoch_);
  }
  OB_INLINE int64_t get_tmp_logical_delete_inflight() const
  {
    return ATOMIC_LOAD(&tmp_logical_delete_inflight_);
  }
  bool is_object_type_need_stat(const blocksstable::MacroBlockId &macro_id);
  int aio_write_with_create_parent_dir(blocksstable::ObStorageObjectWriteInfo &write_info,
                                       const blocksstable::ObStorageObjectHandle &ori_object_handle,
                                       blocksstable::ObStorageObjectHandle &object_handle);
  OB_INLINE int get_tg_id() const { return tg_id_; }
  // interfaces for atomic write file
  int push_to_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id, ObAtomicWriteFileInfo &atomic_write_file);
  int erase_from_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id);
  int get_from_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id, ObAtomicWriteFileInfo &atomic_write_file);
  ObAtomicWriteFileInfoMap &get_atomic_write_file_map(const bool is_clean);
  int do_tmp_seq_file_clean_work(const bool is_clean, const int64_t safe_del_time_s);

private:
  // Rename a local tmp file to .deleted while publishing directory mutation state.
  int rename_local_tmp_file_to_deleted_(
      const char *old_path, const char *new_path);
  int list_local_private_macro_file(const int64_t tablet_id, const int64_t private_macro_third_id,
                                    const ObMacroType macro_type,
                                    ObIArray<blocksstable::MacroBlockId> &block_ids);
  int list_remote_private_macro_file(const int64_t tablet_id, const int64_t private_macro_third_id,
                                     const ObMacroType macro_type,
                                     ObIArray<blocksstable::MacroBlockId> &block_ids);
  // when writing PRIVATE_TABLET_META, try fsync PRIVATE_DATA_MACRO/PRIVATE_META_MACRO parent dir
  int try_fsync_private_macro_parent_dir(const blocksstable::ObStorageObjectType object_type,
                                         const blocksstable::MacroBlockId &file_id);
  int fsync_private_tablet_dir(const blocksstable::ObStorageObjectType object_type,
                               const int64_t tablet_id,
                               const int64_t private_macro_third_id);
  void batch_print_deleted_file_ids(const ObIArray<blocksstable::MacroBlockId> &block_ids, const bool is_local);
  int try_delete_last_unsealed_segment(const TmpFileSegId &seg_id);
  int is_delete_read_cache_tmp_file(const blocksstable::MacroBlockId &file_id, bool &is_need_delete);
  int check_if_write_cache_exist(const blocksstable::MacroBlockId &file_id,
                                 const int64_t ls_epoch_id,
                                 bool &is_exist,
                                 const int64_t ls_id = 0);
  int update_object_type_remote_delete_stat(const blocksstable::ObStorageObjectType &object_type,
                                            const int result, const int64_t delta_cnt);
private:
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t BATCH_DELETE_FILE_COUNT = 1000;
  static const int64_t BATCH_PRINT_FILE_PATH_COUNT = 64;
private:
  common::ObLinkQueue flush_queue_; // store sealed TmpFile sub-file
  ObPrereadCacheManager preread_cache_mgr_;
  ObSegmentFileManager segment_file_mgr_;
  int tg_id_;
  ObCalibrateDiskSpaceTask calibrate_disk_space_task_;
  ObCheckExpandDiskTask check_expand_disk_task_;
  ObTmpSeqFileCleanTask tmp_seq_file_clean_task_;
  ObTenantIODetectTask io_detect_task_;
  ObSSFdCacheMgr fd_cache_mgr_;
  bool is_pause_gc_; // when calibrate disk alloc size pause gc
  bool is_tmp_file_cache_pause_gc_; // when calibrate tmp_file_cache disk space, if delete tmp_file, rename tmp_file to .deleted file
  int64_t tmp_logical_delete_epoch_; // Successful tmp-file renames that mutate the scanned directory tree.
  int64_t tmp_logical_delete_inflight_; // Tmp-file logical deletes that may currently mutate the directory tree.
  volatile bool is_stop_;
  ObAtomicWriteFileInfoMap atomic_write_file_map_;
  ObAtomicWriteFileInfoMap atomic_write_file_clean_map_;
};

class ObTmpSeqFileCleanFunc
{
public:
  ObTmpSeqFileCleanFunc(const bool is_clean, const int64_t tmp_seq_file_del_secy_time_s, ObIArray<blocksstable::MacroBlockId> &deleted_file_ids)
      : is_clean_(is_clean), tmp_seq_file_del_secy_time_s_(tmp_seq_file_del_secy_time_s), deleted_file_ids_(deleted_file_ids)
  {}
  int operator()(const ObAtomicWriteFileInfoMapPairType &entry);
private:
  bool is_clean_;
  int64_t tmp_seq_file_del_secy_time_s_;
  ObIArray<blocksstable::MacroBlockId> &deleted_file_ids_;
};

class ObServerFileManager : public ObBaseFileManager
{
public:
  static ObServerFileManager &get_instance();
  virtual int init(const uint64_t tenant_id) override;
  int start(const int64_t reserved_size);
  void stop();
  void wait();
  virtual void destroy() override;
  int resize_device_size(const int64_t new_device_size, const int64_t new_device_disk_percentage,
                         const int64_t reserved_size, storage::ObServerSuperBlock &super_block);
  // 500 tenant async_write_file do not need alloc_file_size and only write SERVER_META/TENANT_DISK_SPACE_META
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                               blocksstable::ObStorageObjectHandle &object_handle) override;
  int delete_local_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id); // delete local tenant dir files
  int delete_remote_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id); // delete remote tenant dir files
  int delete_remote_server_dir(const uint64_t server_id); // delete remote server dir
  int delete_shared_ls_dir(
      const uint64_t tenant_id,
      const int64_t ls_id);
  int delete_shared_tenant_dir(const uint64_t tenant_id); // delete shared tenant dir files
  int list_shared_tenant_ids(const uint64_t tenant_id, ObIArray<uint64_t> &tenant_ids); // tenant id is used to set ObMemAttr
  int list_server_ids(common::ObIArray<uint64_t> &server_ids);
  int get_slog_used_size(const uint64_t tenant_id, const int64_t tenant_epoch_id, int64_t &used_size);
  int check_disk_space_available();

  int create_io_calibration_file(const int64_t block_count);
  int delete_io_calibration_file();
  int get_io_calibration_fd() const { return io_calibration_fd_; }

  virtual int get_io_detect_file_path(char *path, const int64_t length) override;

  // interfaces for slog flush
  int push_to_flush_map(const blocksstable::MacroBlockId &file_id);
  int erase_from_flush_map(const blocksstable::MacroBlockId &file_id);
  int get_next_flush_entry(ObSSMacroCacheFlushInfo &flush_info, bool &is_exist);

private:
  ObServerFileManager();
  virtual ~ObServerFileManager();
  int aio_write_with_create_parent_dir(blocksstable::ObStorageObjectWriteInfo &write_info,
                                       const blocksstable::ObStorageObjectHandle &ori_object_handle,
                                       blocksstable::ObStorageObjectHandle &object_handle);
  int check_disk_space_available_(const bool need_report_user_error) const;

private:
  static constexpr const char *IO_CALIBRATION_FILE = "ss_io_calibration_file";
  static const int INVALID_TG_ID = -1;

  lib::ObMutex resize_device_size_lock_;
  volatile bool is_stopped_;
  int io_calibration_fd_;
  int tg_id_;
  ObSSMacroCacheFlushMap flush_map_; // for server slog flush
  ObServerSlogFlushTask server_slog_flush_task_;
  ObCheckDataDiskAvailTask check_data_disk_avail_task_;
};

class ObMandatoryCalibrateGuard
{
public:
  explicit ObMandatoryCalibrateGuard(volatile bool &is_mandatory_calibrate);
  virtual ~ObMandatoryCalibrateGuard();
  bool is_cur_calibrate_mandatory() const;
  TO_STRING_KV(K_(is_mandatory_calibrate), K_(need_reset_flag));

private:
  bool need_reset_flag_;
  volatile bool &is_mandatory_calibrate_;
  DISALLOW_COPY_AND_ASSIGN(ObMandatoryCalibrateGuard);
};

#define OB_SERVER_FILE_MGR (oceanbase::storage::ObServerFileManager::get_instance())

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_ */

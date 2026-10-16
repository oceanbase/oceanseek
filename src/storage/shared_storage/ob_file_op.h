/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_

#include "common/storage/ob_device_common.h"
#include "deps/oblib/src/lib/container/ob_array.h"
#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

enum ObMacroType : uint8_t;

// ObSharedTenantDirListOp is used for list shared tenant dir in object storage, example tenant_xxx
class ObSharedTenantDirListOp : public ObBaseDirEntryOperator
{
public:
  ObSharedTenantDirListOp(const uint64_t tenant_id);
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<uint64_t> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObArray<uint64_t> file_list_;
};

// ObSharedClusterDirListOp is used for list cluster dir in object storage, example cluster_xxx
class ObSharedClusterDirListOp : public ObBaseDirEntryOperator
{
public:
  ObSharedClusterDirListOp();
  virtual int func(const dirent *entry) override;
  int get_cluster_ids(common::ObIArray<uint64_t> &cluster_ids) const;
  TO_STRING_KV(K_(cluster_ids));

public:
  ObArray<uint64_t> cluster_ids_;
};

// ObServerDirListOp is used for list server dir in object storage, example server_xxx
class ObServerDirListOp : public ObBaseDirEntryOperator
{
public:
  ObServerDirListOp() {}
  virtual ~ObServerDirListOp() {}
  virtual int func(const dirent *entry) override;
  int get_server_ids(common::ObIArray<uint64_t> &server_ids) const;
  TO_STRING_KV(K_(server_ids));

public:
  common::ObArray<uint64_t> server_ids_;
};

// ObSingleNumFileListOp is used for list single number name file, for example tablet data dir,tablet meta dir,tmp file dir,segment file
class ObSingleNumFileListOp : public ObBaseDirEntryOperator
{
public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObSingleNumFileListOp();
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<int64_t> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> file_list_;
};

// ObTabletMetaListOp is used for list private tablet meta file, which is used to list tablet meta files in local cache and remote storage
class ObTabletMetaListOp : public ObBaseDirEntryOperator
{
public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObTabletMetaListOp();
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<int64_t> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> file_list_;
};

// ObCkptFileListOp is used for list private ckpt file, which is used to list ckpt files in remote storage
class ObCkptFileListOp : public ObBaseDirEntryOperator
{
public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObCkptFileListOp();
  virtual int func(const dirent *entry) override;
  int get_object_id_list(common::ObIArray<int64_t> &object_ids) const;
  TO_STRING_KV(K_(object_ids));

public:
  ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> object_ids_;
};

class ObTmpFileSegListOp : public ObBaseDirEntryOperator
{
public:
  ObTmpFileSegListOp();
  virtual int func(const dirent *entry) override;
  TO_STRING_KV(K_(seg_ids));

public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> seg_ids_;
};

class ObPrivateMacroListOp : public ObBaseDirEntryOperator
{
public:
  explicit ObPrivateMacroListOp(const ObMacroType macro_type);
  virtual int func(const dirent *entry) override;
  TO_STRING_KV(K_(file_list));

public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObMacroType macro_type_;
  ObSEArray<std::pair<int64_t, int64_t>, OB_DEFAULT_ARRAY_CAPACITY> file_list_;
};

#define MAX_DIR_RANGE 10

class ObDirCalcSizeOp : public share::ObScanDirOp
{
public:
  static const int64_t DEFAULT_TMP_SEQ_FILE_DELETE_SECURITY_TIME_S = 3600L; // 1h, second level
  
  // Get the index of dir size bucket based on size
  // Returns: 0-9 corresponding to [0,1k),[1k,2k),[2k,4k),[4k,8k),(8k,16k],(16k,32k],(32k,64k],(64k,128k],(128k,256k],(256k,+)
  static int64_t get_dir_size_bucket_index(const int64_t size);
  ObDirCalcSizeOp(volatile bool &is_stop, const char *dir = nullptr);
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_size() const { return total_file_size_; }
  OB_INLINE int64_t get_total_dir_num() const { return total_dir_num_; }
  OB_INLINE int64_t get_hot_tablet_total_file_size() const { return hot_tablet_total_file_size_; }
  OB_INLINE int64_t get_del_tmp_seq_file_size() const { return del_tmp_seq_file_size_; }
  OB_INLINE int64_t get_tmp_seq_file_size() const { return tmp_seq_file_size_; }
  OB_INLINE int64_t get_tmp_seq_file_num() const { return tmp_seq_file_num_; }
  OB_INLINE int64_t get_not_in_meta_map_file_cnt() const { return not_in_meta_map_file_cnt_; }
  int set_start_calc_size_time(const int64_t start_calc_size_time_s);
  void set_macro_cache_type(const ObSSMacroCacheType macro_cache_type) { macro_cache_type_ = macro_cache_type; }
  void set_for_del_tmp_seq_files() { is_for_del_tmp_seq_files_ = true; }
  // if .tmp.seq file's modify time exceeds the security time(second level) compared with the current time, it will be deleted
  int set_tmp_seq_file_del_secy_time_s(const int64_t del_secy_time_s);
  TO_STRING_KV(K_(is_stop), K_(total_file_size), K_(hot_tablet_total_file_size),
               K_(tmp_seq_file_size), K_(tmp_seq_file_num), K_(del_tmp_seq_file_size),
               K_(start_calc_size_time_s), K_SS_MACRO_CACHE_TYPE(macro_cache_type_),
               K_(is_for_del_tmp_seq_files), K_(tmp_seq_file_del_secy_time_s),
               K_(not_in_meta_map_file_cnt), "dir_nums", ObArrayWrap<int64_t>(dir_nums_, MAX_DIR_RANGE),
               "dir_sizes", ObArrayWrap<int64_t>(dir_sizes_, MAX_DIR_RANGE), K_(total_dir_num),
               K_(largest_dir_size), K_(largest_dir_path));

private:
  int process_tmp_seq_file(const common::ObIODFileStat &statbuf, const char *full_path);
  int process_tmp_file(const common::ObIODFileStat &statbuf, const char *path);
  int process_meta_file(const common::ObIODFileStat &statbuf, const char *path);
  int process_macro_block(const common::ObIODFileStat &statbuf, const char *path);
  int get_macro_id_and_cache_info(const char *path,
                                  blocksstable::MacroBlockId &macro_id,
                                  ObSSMacroCacheType &cache_type,
                                  bool &is_macro_cache_meta_exist);

private:
  volatile bool &is_stop_;
  int64_t total_file_size_;
  int64_t hot_tablet_total_file_size_;
  int64_t del_tmp_seq_file_size_;
  int64_t tmp_seq_file_size_;
  int64_t tmp_seq_file_num_;
  int64_t start_calc_size_time_s_;
  ObSSMacroCacheType macro_cache_type_;
  bool is_for_del_tmp_seq_files_;
  int64_t tmp_seq_file_del_secy_time_s_;  // if .tmp.seq file's modify time exceeds the security time(second level) compared with the current time, it will be deleted
  int64_t not_in_meta_map_file_cnt_;
  int64_t largest_dir_size_;
  char largest_dir_path_[common::MAX_PATH_SIZE];
  int64_t dir_nums_[MAX_DIR_RANGE];
  int64_t dir_sizes_[MAX_DIR_RANGE];
  int64_t total_dir_num_;
};

class ObMajorDataDeleteOp : public ObBaseDirEntryOperator
{
public:
  ObMajorDataDeleteOp(const int64_t time_stamp = ObTimeUtility::current_time());
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_count() const { return total_file_count_; }
  int set_time_stamp(const int64_t time_stamp);
  TO_STRING_KV(K_(time_stamp), K_(total_file_count));

private:
  int64_t time_stamp_;
  int64_t total_file_count_;
};

class ObRMLogicalDeletedFileOp : public share::ObScanDirOp
{
public:
  ObRMLogicalDeletedFileOp(volatile bool &is_stop, const char *dir = nullptr)
  : ObScanDirOp(dir), is_stop_(is_stop), total_file_cnt_(0) {}
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_count() const { return total_file_cnt_; }
  TO_STRING_KV(K_(is_stop), K_(total_file_cnt));
private:
  volatile bool &is_stop_;
  int64_t total_file_cnt_;
};

class ObDelTmpFileDirOp : public share::ObScanDirOp
{
public:
  ObDelTmpFileDirOp(volatile bool &is_stop, const char *dir = nullptr)
  : ObScanDirOp(dir), is_stop_(is_stop) {}
  virtual int func(const dirent *entry) override;
  TO_STRING_KV(K_(is_stop));

private:
  int check_dir_if_can_del(const char *dir_path, bool &can_del);

private:
  static const int64_t TMP_FILE_DIR_DELETE_SAFE_TIME_S = 3600L; // 1h, second level
  volatile bool &is_stop_;
};

class ObObjectDirCalcSizeOp : public ObBaseDirEntryOperator
{
public:
  ObObjectDirCalcSizeOp() : total_size_(0) {}
  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    total_size_ += get_size();
    return ret;
  }
  virtual bool need_get_file_meta() const override { return true; }
  OB_INLINE int64_t get_total_size() const { return total_size_; }
private:
  int64_t total_size_;
};

/*
 * ObSSGetSlogIdRangeFunctor is used for geting slog min id and slog max id.
 * This interface is specially designed for SS mode.
 */
class ObSSGetSlogIdRangeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObSSGetSlogIdRangeFunctor()
    : min_file_id_(OB_INVALID_FILE_ID),
      max_file_id_(OB_INVALID_FILE_ID)
  {
  }
  virtual ~ObSSGetSlogIdRangeFunctor() = default;

  virtual int func(const dirent *entry) override;
  uint32_t get_min_file_id() const { return min_file_id_; }
  uint32_t get_max_file_id() const { return max_file_id_; }
private:
  uint32_t min_file_id_;
  uint32_t max_file_id_;

  DISALLOW_COPY_AND_ASSIGN(ObSSGetSlogIdRangeFunctor);
};

/*
 * ObSSGetSlogSizeFunctor is used for geting slog file size
 * This interface is specially designed for SS mode.
 */
class ObSSGetSlogSizeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObSSGetSlogSizeFunctor(const char* dir)
    : dir_(dir), total_size_(0)
  {
  }
  virtual ~ObSSGetSlogSizeFunctor() = default;

  virtual int func(const dirent *entry) override;
  int64_t get_total_size() const { return total_size_; }
private:
  const char* dir_;
  int64_t total_size_;

  DISALLOW_COPY_AND_ASSIGN(ObSSGetSlogSizeFunctor);
};

// ObMacroIdListOp is used for list macro id
class ObMacroIdListOp : public common::ObBaseDirEntryOperator
{
public:
  ObMacroIdListOp(const char *dir);
  virtual int func(const dirent *entry) override;
  int get_macro_id_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list) const;
  TO_STRING_KV(K_(macro_id_list));
public:
  const char *dir_;
  ObArray<blocksstable::MacroBlockId> macro_id_list_;
};

/*
 * ObDeleteOldVersionTmpFileOp is used for deleting old version tmp files in scatter directory
 * Old version structure: tmp_data/100/seg1.T8 (seg files directly under scatter_dir)
 * New version structure: tmp_data/100/100000/seg1.T8 (tmp_file_id directories under scatter_dir)
 */
class ObDeleteOldVersionTmpFileOp : public common::ObBaseDirEntryOperator
{
public:
  ObDeleteOldVersionTmpFileOp(const char *scatter_dir, int64_t &delete_success_count) 
    : scatter_dir_(scatter_dir), delete_success_count_(delete_success_count) {}
  virtual ~ObDeleteOldVersionTmpFileOp() = default;

  virtual int func(const dirent *entry) override;
  TO_STRING_KV(KP_(scatter_dir), K_(delete_success_count));
private:
  const char *scatter_dir_;
  int64_t &delete_success_count_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_ */

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BACKUP_SS_BACKUP_UTILS_H_
#define OCEANBASE_STORAGE_BACKUP_SS_BACKUP_UTILS_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase
{
namespace backup
{

class ObBackupTabletAllMacroIdIterator final
{
public:
  ObBackupTabletAllMacroIdIterator();
  ~ObBackupTabletAllMacroIdIterator();
  int init(storage::ObTablet *tablet);
  int get_next_macro_id(blocksstable::MacroBlockId &macro_id);

private:
  bool is_inited_;
  storage::ObTablet *tablet_;
  storage::ObMacroInfoIterator iter_;
  common::ObArenaAllocator allocator_;
  storage::ObTabletMacroInfo *macro_info_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletAllMacroIdIterator);
};

class ObBackupSSTableAllMacroIdIterator final
{
public:
  ObBackupSSTableAllMacroIdIterator();
  ~ObBackupSSTableAllMacroIdIterator();
  int init(blocksstable::ObSSTable *sstable);
  int get_next_macro_id(blocksstable::MacroBlockId &macro_id);

  TO_STRING_KV(KPC_(sstable), K_(current_index));

private:
  bool is_inited_;
  blocksstable::ObSSTable *sstable_;
  blocksstable::ObSSTableMetaHandle sst_meta_hdl_;
  blocksstable::ObMacroIdIterator data_iter_;
  blocksstable::ObMacroIdIterator other_iter_;
  blocksstable::ObMacroIdIterator linked_iter_;
  common::ObArray<blocksstable::ObMacroIdIterator*> iters_;
  int64_t current_index_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSSTableAllMacroIdIterator);
};

class ObBackupMacroBlockReaderWriter
{
public:
  ObBackupMacroBlockReaderWriter();
  ~ObBackupMacroBlockReaderWriter();
  int init(const share::ObBackupDest &backup_dest,
           const share::ObBackupSetDesc &backup_set_desc,
           const common::ObStorageIdMod &mod);
  int backup_macro_block(const blocksstable::MacroBlockId &block_id);

private:
  int read_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      common::ObArenaAllocator &allocator,
      ObStorageObjectHandle &object_handle);
  int check_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      ObStorageObjectHandle &object_handle);
  int write_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      ObStorageObjectHandle &object_handle);
  int get_macro_block_backup_path_(
      const blocksstable::MacroBlockId &block_id,
      const share::ObBackupDest &backup_dest,
      const share::ObBackupSetDesc &desc,
      share::ObBackupPath &backup_path);

private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  common::ObStorageIdMod mod_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockReaderWriter);
};

class ObSSExternLSMetaMgr {
public:
  ObSSExternLSMetaMgr();
  ~ObSSExternLSMetaMgr();
  int init(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const int64_t dest_id);
  int write_ls_meta_info(const storage::ObBackupSSLSMetaInfo &ls_meta);
  // Read ls meta info from backup storage into ls_meta.
  // NOTE: ls_meta.raw_ls_meta_row_ (meta_key_/meta_value_) holds shallow ObString
  //       references into buffers allocated from `allocator`. The caller must not destroy or
  //       reset `allocator` while ls_meta (or any shallow copy of it) is still in use.
  int read_ls_meta_info(common::ObIAllocator &allocator, storage::ObBackupSSLSMetaInfo &ls_meta);

private:
  int get_ss_ls_meta_backup_path_(share::ObBackupPath &path);

private:
  bool is_inited_;
  share::ObBackupDest backup_set_dest_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t dest_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSExternLSMetaMgr);
};

// Filter class to find max turn_id and retry_id from meta_info directories
class ObSSMetaInfoDirFilter final : public ObBaseDirEntryOperator
{
public:
  ObSSMetaInfoDirFilter() : turn_id_(0), retry_id_(0) {}
  ~ObSSMetaInfoDirFilter() {}
  int func(const dirent *entry) override;
  int64_t get_turn_id() const { return turn_id_; }
  int64_t get_retry_id() const { return retry_id_; }
  TO_STRING_KV(K_(turn_id), K_(retry_id));
private:
  int64_t turn_id_;
  int64_t retry_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaInfoDirFilter);
};

class ObBackupTabletGroupCheckpointFilter : public ObBaseDirEntryOperator
{
public:
  ObBackupTabletGroupCheckpointFilter(const common::ObString &file_prefix);
  ~ObBackupTabletGroupCheckpointFilter();
  void reset();
  int get_retry_list(ObIArray<int64_t> &retry_list) const;
  int func(const dirent *entry) override;
  TO_STRING_KV(K_(retry_list));

private:
  int extract_retry_id_(
      const common::ObString &file_name,
      const common::ObString &prefix,
      int64_t &retry_id,
      bool &match);

private:
  common::ObString file_prefix_;
  common::ObSArray<int64_t> retry_list_;
};

class ObSSExternTabletMetaWriter final
{
public:
  static const int64_t BUF_SIZE = 4 * 1024 * 1024;
  static const int64_t TRAILER_BUF = 1024;
  ObSSExternTabletMetaWriter();
  ~ObSSExternTabletMetaWriter();
  int init(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id, 
           const int64_t retry_id, const int64_t dest_id);
  int write_meta_data(const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id);
  int close(const bool is_succeed);
private:
  int write_meta_data_(const blocksstable::ObBufferReader &meta_data);
  int prepare_backup_file_(const int64_t file_id);
  int64_t get_data_file_size() const;
  bool need_switch_file_(const blocksstable::ObBufferReader &buffer);
  int switch_file_();
  int flush_trailer_();
  int write_data_align_(
      const blocksstable::ObBufferReader &buffer, const share::ObBackupFileType &type, const int64_t alignment);
private:
  bool is_inited_;
  share::ObBackupDest backup_set_dest_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t dest_id_;
  common::ObIOFd io_fd_;
  common::ObIODevice *dev_handle_;
  ObBackupFileWriteCtx file_write_ctx_;
  ObTabletInfoTrailer file_trailer_;
  blocksstable::ObSelfBufferWriter tmp_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObSSExternTabletMetaWriter);
};

class ObSSExternTabletMetaReader final
{
  class ObTabletInfoFileIdFilter final : public ObBaseDirEntryOperator
  {
  public:
    using FileIdSet = common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode>;
  public:
    ObTabletInfoFileIdFilter(): is_inited_(false), file_id_set_() {}
    ~ObTabletInfoFileIdFilter() {}
    int init();
    int get_file_id_array(ObIArray<int64_t> &file_id_array);
    int func(const dirent *entry) override;
  private:
    bool is_inited_;
    FileIdSet file_id_set_;
    DISALLOW_COPY_AND_ASSIGN(ObTabletInfoFileIdFilter);
  };
public:
  ObSSExternTabletMetaReader();
  ~ObSSExternTabletMetaReader();
  int init(
      const share::ObBackupDest &backup_set_dest,
      const common::ObStorageIdMod &mod,
      const share::ObLSID &ls_id);
  int get_next(common::ObIAllocator &allocator, storage::ObSSRawMetaRow &meta_row);
private:
  bool end_();
  int read_next_batch_();
  int read_next_range_tablet_metas_();
  int get_tablet_meta_file_path_(
      const share::ObLSID &ls_id,
      const int64_t file_id,
      ObBackupPath &backup_path);
  int get_tablet_info_file_id_list_(
      const share::ObLSID &ls_id,
      common::ObIArray<int64_t> &file_id_list);
  int get_max_turn_id_and_retry_id_(
      const share::ObLSID &ls_id,
      int64_t &turn_id,
      int64_t &retry_id);
  int fill_tablet_info_trailer_(
      const share::ObBackupDest &backup_set_dest,
      const share::ObLSID &ls_id,
      const ObStorageIdMod &mod);
  int read_file_trailer_(
      const common::ObString &path,
      const share::ObBackupStorageInfo *storage_info,
      const ObStorageIdMod &mod,
      backup::ObTabletInfoTrailer &trailer);
private:
  bool is_inited_;
  lib::ObMutex lock_;
  share::ObLSID ls_id_;
  int64_t cur_tablet_idx_;
  int64_t cur_trailer_idx_;
  int64_t cur_buf_offset_;
  ObArenaAllocator allocator_;
  common::ObArray<storage::ObSSRawMetaRow> tablet_meta_array_;
  common::ObArray<backup::ObTabletInfoTrailer> tablet_info_trailer_array_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObStorageIdMod mod_;
  share::ObBackupDest backup_set_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObSSExternTabletMetaReader);
};

struct ObSSBackupUtils
{
public:
  static int get_macro_block_relative_path(
    const blocksstable::MacroBlockId &macro_id, char *path, int64_t length);
  static int get_macro_block_backup_path(
    const blocksstable::MacroBlockId &macro_id,
    const share::ObBackupPath &backup_set_dir_path,
    share::ObBackupPath &backup_path);
  static int calc_start_replay_scn(
    common::ObISQLClient &sql_client,
    const share::ObBackupSetTaskAttr &set_task_attr,
    const storage::ObBackupSSLSMetaInfosDesc &ls_meta_infos,
    const share::ObTenantArchiveRoundAttr &round_attr,
    share::SCN &start_replay_scn);
  static int calc_min_restore_scn(
    const share::ObBackupSetTaskAttr &set_task_attr,
    const storage::ObBackupSSLSMetaInfosDesc &ls_meta_infos,
    const share::ObTenantArchiveRoundAttr &round_attr,
    share::SCN &min_restore_scn);
  // 判断宏块是否需要在备份时补充 ObBackupCommonHeader
  static bool need_backup_common_header(const blocksstable::MacroBlockId &block_id);
  // 为原始宏块数据补充 ObBackupCommonHeader
  static int wrap_with_backup_common_header(
    const blocksstable::MacroBlockId &block_id,
    const blocksstable::ObBufferReader &macro_data,
    common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &wrapped_data);
  // 校验并剥离 ObBackupCommonHeader
  static int unwrap_backup_common_header(
    const blocksstable::MacroBlockId &block_id,
    blocksstable::ObBufferReader &macro_data);
};

}
}

#endif

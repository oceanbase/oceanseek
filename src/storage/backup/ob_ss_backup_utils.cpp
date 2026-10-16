/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace backup
{

// ObBackupTabletAllMacroIdIterator

ObBackupTabletAllMacroIdIterator::ObBackupTabletAllMacroIdIterator()
  : is_inited_(false),
    tablet_(nullptr),
    iter_(),
    allocator_(common::ObMemAttr(MTL_ID(), "GetTabletBlocks")),
    macro_info_(nullptr)
{
}

ObBackupTabletAllMacroIdIterator::~ObBackupTabletAllMacroIdIterator()
{
}

int ObBackupTabletAllMacroIdIterator::init(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  bool in_memory = true;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet meta macro id iterator init twice", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet should not be null", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->load_macro_info(0 /*ls_epoch*/, allocator_, macro_info_))) {
    LOG_WARN("fail to load macro info", KR(ret));
  } else if (OB_FAIL(iter_.init(ObTabletMacroType::MAX, *macro_info_))) {
    LOG_WARN("fail to init macro iterator", KR(ret), KPC_(macro_info));
  } else {
    tablet_ = tablet;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletAllMacroIdIterator::get_next_macro_id(blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  storage::ObTabletBlockInfo block_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet all macro id iterator do not init", K(ret));
  } else if (OB_FAIL(iter_.get_next(block_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next block info", KR(ret), K(block_info));
    }
  } else {
    macro_id = block_info.macro_id();
  }
  return ret;
}

// ObBackupSSTableAllMacroIdIterator

ObBackupSSTableAllMacroIdIterator::ObBackupSSTableAllMacroIdIterator()
  : is_inited_(false),
    sstable_(nullptr),
    sst_meta_hdl_(),
    data_iter_(),
    other_iter_(),
    linked_iter_(),
    iters_(),
    current_index_(0)
{
}
    
ObBackupSSTableAllMacroIdIterator::~ObBackupSSTableAllMacroIdIterator()
{
}

int ObBackupSSTableAllMacroIdIterator::init(blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup sstable all macro id iterator init twice", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be null", K(ret));
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl_))) {
    LOG_WARN("failed to get meta", K(ret), KPC(sstable));
  } else {
    const blocksstable::ObSSTableMacroInfo &macro_info = sst_meta_hdl_.get_sstable_meta().get_macro_info();
    if (OB_FAIL(macro_info.get_data_block_iter(data_iter_))) {
      LOG_WARN("fail to get data block iter", KR(ret));
    } else if (OB_FAIL(macro_info.get_other_block_iter(other_iter_))) {
      LOG_WARN("fail to get other block iter", KR(ret));
    } else if (OB_FAIL(macro_info.get_other_block_iter(linked_iter_))) {
      LOG_WARN("fail to get linked block iter", KR(ret));
    } else if (OB_FAIL(iters_.push_back(&data_iter_))) {
      LOG_WARN("failed to push data iterator", K(ret));
    } else if (OB_FAIL(iters_.push_back(&other_iter_))) {
      LOG_WARN("failed to push other iterator", K(ret));
    } else if (OB_FAIL(iters_.push_back(&linked_iter_))) {
      LOG_WARN("failed to push linked iterator", K(ret));
    } else {
      sstable_ = sstable;
      current_index_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupSSTableAllMacroIdIterator::get_next_macro_id(blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  macro_id.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup sstable all macro id iterator not inited", K(ret));
  } else {
    while (OB_SUCC(ret) && !found) {
      if (current_index_ >= iters_.count()) {
        ret = OB_ITER_END;
        LOG_INFO("all macro id iterators reached end", K_(current_index));
      } else {
        blocksstable::ObMacroIdIterator *iter = iters_.at(current_index_);
        if (OB_ISNULL(iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("macro id iterator is null", K(ret), K_(current_index));
        } else {
          int tmp_ret = iter->get_next_macro_id(macro_id);
          if (OB_SUCCESS == tmp_ret) {
            found = true;
            LOG_INFO("get next macro id", K(macro_id), K_(current_index));
          } else if (OB_ITER_END == tmp_ret) {
            ++current_index_;
          } else {
            ret = tmp_ret;
            LOG_WARN("failed to get next macro id", K(ret), K_(current_index));
          }
        }
      }
    }
  }
  return ret;
}

// ObBackupMacroBlockReaderWriter

ObBackupMacroBlockReaderWriter::ObBackupMacroBlockReaderWriter()
  : is_inited_(false),
    backup_dest_(),
    backup_set_desc_(),
    mod_()
{
}

ObBackupMacroBlockReaderWriter::~ObBackupMacroBlockReaderWriter()
{
}

int ObBackupMacroBlockReaderWriter::init(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc,
    const common::ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup macro block reader writer init twice", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || !mod.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(mod));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    backup_set_desc_ = backup_set_desc;
    mod_ = mod;
    is_inited_ = true;
  }
  return ret;
}
        
int ObBackupMacroBlockReaderWriter::backup_macro_block(const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageObjectHandle object_handle;
  if (!block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(block_id));
  } else if (OB_FAIL(read_macro_block_(block_id, allocator, object_handle))) {
    LOG_WARN("failed to read macro block", K(ret), K(block_id));
  } else if (OB_FAIL(check_macro_block_(block_id, object_handle))) {
    LOG_WARN("failed to check macro block", K(ret), K(block_id));
  } else if (OB_FAIL(write_macro_block_(block_id, object_handle))) {
    LOG_WARN("failed to write macro block", K(ret), K(block_id));
  }
  return ret;
}

int ObBackupMacroBlockReaderWriter::read_macro_block_(
    const blocksstable::MacroBlockId &block_id,
    common::ObArenaAllocator &allocator,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = block_id;
  read_info.offset_ = 0;
  read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
  read_info.io_desc_.set_sys_module_id(ObIOModule::BACKUP_READER_IO);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.mtl_tenant_id_ = MTL_ID();
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for read buf", KR(ret), K_(read_info.size));
  } else if (OB_FALSE_IT(read_info.buf_ = buf)) {
  } else if (OB_FAIL(ObObjectManager::read_object(read_info, object_handle))) {
    LOG_WARN("failed to read macro block", KR(ret), K(read_info), K(object_handle));
  } else if (OB_UNLIKELY(object_handle.get_data_size() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object data size less than 0", K(ret));
  }
  return ret;
}

int ObBackupMacroBlockReaderWriter::check_macro_block_(
    const blocksstable::MacroBlockId &block_id,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockChecker checker;
  if (OB_FAIL(checker.check(object_handle.get_buffer(), object_handle.get_data_size()))) {
    LOG_ERROR("failed to check macro block", K(ret), K(block_id), K(object_handle));
  }
  return ret;
}

int ObBackupMacroBlockReaderWriter::write_macro_block_(
    const blocksstable::MacroBlockId &block_id,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  ObBackupIoAdapter util;
  if (OB_FAIL(get_macro_block_backup_path_(block_id, backup_dest_, backup_set_desc_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to mk parent dir", K(ret), K(backup_path));
  } else if (OB_FAIL(util.write_single_file(backup_path.get_obstr(),
                                            backup_dest_.get_storage_info(),
                                            object_handle.get_buffer(),
                                            object_handle.get_data_size(),
                                            mod_))) {
    LOG_WARN("failed to write single file", K(ret));
  } else {
    LOG_INFO("write single file", K(backup_path), K(block_id), K(object_handle));
  }
  return ret;
}

int ObBackupMacroBlockReaderWriter::get_macro_block_backup_path_(
    const blocksstable::MacroBlockId &block_id,
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &desc,
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  share::ObBackupDest backup_set_dest;
  const int64_t length = 1024;
  char path[length] = {0};
  if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(backup_dest, desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), K(backup_dest), K(desc));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(ObSSBackupUtils::get_macro_block_relative_path(block_id, path, length))) {
    LOG_WARN("failed to get macro block relative path", K(ret), K(block_id));
  } else if (OB_FAIL(backup_path.join(path))) {
    LOG_WARN("failed to join path", K(ret), K(path));
  } else {
    // TODO(yanfeng): remove this log when ss backup is stable
    LOG_INFO("get macro block path", K(block_id), K(backup_path));
  }
  return ret;
}


// ObSSExternLSMetaMgr

ObSSExternLSMetaMgr::ObSSExternLSMetaMgr()
    : is_inited_(false),
      backup_set_dest_(),
      ls_id_(),
      turn_id_(),
      retry_id_(),
      dest_id_()
{}

ObSSExternLSMetaMgr::~ObSSExternLSMetaMgr()
{}

int ObSSExternLSMetaMgr::init(const ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (!backup_set_dest.is_valid() || !ls_id.is_valid() || dest_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_set_dest), K(ls_id), K(dest_id));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_set_dest));
  } else {
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    dest_id_ = dest_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSExternLSMetaMgr::write_ls_meta_info(const ObBackupSSLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_size = ls_meta.get_total_serialize_buf_size();
  ObArenaAllocator allocator;
  ObBackupPath path;
  ObBackupIoAdapter util;
  ObStorageIdMod mod;
  mod.storage_id_ = dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(ls_meta.serialize_to(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize ls meta info.", K(ret), K(buf_size));
  } else if (pos != buf_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized size not match.", K(ret), K(pos), K(buf_size));
  } else if (OB_FAIL(get_ss_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get ls meta backup path", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), backup_set_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(path));
  } else if (OB_FAIL(util.write_single_file(path.get_obstr(),
                                            backup_set_dest_.get_storage_info(),
                                            buf,
                                            buf_size,
                                            mod))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    LOG_INFO("succeed to write ls meta info", K(path));
  }
  return ret;
}

int ObSSExternLSMetaMgr::read_ls_meta_info(ObIAllocator &allocator, ObBackupSSLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObBackupPath path;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObStorageIdMod mod;
  mod.storage_id_ = dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup extern mgr not init.", K(ret));
  } else if (OB_FAIL(get_ss_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get backup ls meta path", K(ret));
  } else if (OB_FAIL(util.get_file_length(path.get_obstr(), backup_set_dest_.get_storage_info(), file_length))) {
    if (OB_OBJECT_NOT_EXIST != ret) {
      LOG_WARN("failed to get ls meta backup path file length.", K(ret), K(path));
    } else {
      LOG_WARN("ls meta info file not exist.", K(ret), K(path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta info file is empty.", K(ret), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(path), K(file_length));
  } else if (OB_FAIL(util.read_single_file(path.get_obstr(),
                                           backup_set_dest_.get_storage_info(),
                                           buf,
                                           file_length,
                                           read_size,
                                           mod))) {
    LOG_WARN("failed to read ls meta info file.", K(ret), K(path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match.", K(ret), K(path), K(file_length), K(read_size));
  } else if (OB_FAIL(ls_meta.deserialize_from(buf, file_length))) {
    LOG_WARN("failed to deserialize ls meta info.", K(ret), K(path), K(file_length));
  } else {
    LOG_INFO("succeed to read ls meta info", K(path));
  }
  return ret;
}

int ObSSExternLSMetaMgr::get_ss_ls_meta_backup_path_(ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_ls_meta_info_backup_path(backup_set_dest_, ls_id_, turn_id_,
      retry_id_, path))) {
    LOG_WARN("failed to get ls meta info backup path", K(ret), K_(backup_set_dest), K_(ls_id),
        K_(turn_id), K_(retry_id));
  }
  return ret;
}

// ObBackupTabletGroupCheckpointFilter

ObBackupTabletGroupCheckpointFilter::ObBackupTabletGroupCheckpointFilter(const common::ObString &file_prefix)
    : file_prefix_(file_prefix), retry_list_()
{}

ObBackupTabletGroupCheckpointFilter::~ObBackupTabletGroupCheckpointFilter()
{}

int ObBackupTabletGroupCheckpointFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null", K(ret));
  } else {
    bool match = false;
    int64_t tmp_retry_id = 0;
    ObString file_name(entry->d_name);
    if (OB_FAIL(extract_retry_id_(file_name, file_prefix_, tmp_retry_id, match))) {
      LOG_WARN("failed to extract retry id", K(ret), K(file_name));
    } else if (OB_UNLIKELY(!match)) {
      // not match, skip
    } else if (OB_FAIL(retry_list_.push_back(tmp_retry_id))) {
      LOG_WARN("failed to push back", K(ret), K(tmp_retry_id));
    }
  }
  return ret;
}

int ObBackupTabletGroupCheckpointFilter::get_retry_list(common::ObIArray<int64_t> &retry_list) const
{
  return retry_list.assign(retry_list_);
}

// extract retry id from string "tablet_group_checkpoint.<retry_id>"
int ObBackupTabletGroupCheckpointFilter::extract_retry_id_(
    const common::ObString &file_name,
    const common::ObString &prefix,
    int64_t &retry_id,
    bool &match)
{
  int ret = OB_SUCCESS;
  const char *str = file_name.ptr();
  int64_t tmp_retry_id = 0;
  retry_id = 0;
  match = false;
  if (OB_UNLIKELY(file_name.empty()) || OB_ISNULL(str) || OB_UNLIKELY('\0' == (*str))) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_name));
  } else if (!file_name.prefix_match(prefix)) {
    match = false;
  } else if (file_name.length() <= prefix.length() + 1) {
    match = false;
  } else {
    const int64_t suffix_length = file_name.length() - (prefix.length() + 1);
    const char *suffix_ptr = file_name.ptr() + prefix.length() + 1;
    ObString suffix_str = ObString(suffix_length, suffix_ptr);
    char *end_ptr = NULL;
    if (OB_FAIL(ob_strtoll(suffix_ptr, end_ptr, tmp_retry_id))) {
      LOG_WARN("failed to convert str to ll", K(suffix_ptr), K(tmp_retry_id));
    } else {
      retry_id = tmp_retry_id;
      match = true;
      LOG_INFO("extract retry id", K(file_name), K(prefix), K(tmp_retry_id), K(suffix_ptr), K(suffix_length));
    }
  }
  return ret;
}

// ObTabletInfoFileIdFilter

int ObSSExternTabletMetaReader::ObTabletInfoFileIdFilter::init()
{
  int ret = OB_SUCCESS;
  const int64_t default_bucket_num  = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletInfoFileIdFilter init twice", K(ret));
  } else if (OB_FAIL(file_id_set_.create(default_bucket_num))) {
    LOG_WARN("failed to create id set", K(ret), K(default_bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSExternTabletMetaReader::ObTabletInfoFileIdFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t file_id = 0;
  char expected_format[1024] = {0};
  if (OB_FAIL(databuff_printf(expected_format, sizeof(expected_format),
      "%s.%%ld.obbak", OB_STR_SS_TABLET_INFO))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else if (1 != sscanf(entry->d_name, expected_format, &file_id)) {
  } else if (file_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", K(ret), K(file_id));
  } else if (OB_FAIL(file_id_set_.set_refactored(file_id, 0/*not cover exist object*/))) {
    LOG_WARN("failed to set refactored", K(ret), K(file_id));
  }
  return ret;
}

int ObSSExternTabletMetaReader::ObTabletInfoFileIdFilter::get_file_id_array(ObIArray<int64_t> &file_id_array)
{
  int ret = OB_SUCCESS;
  if (file_id_set_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file id must not be empty", K(ret));
  } else {
    for (FileIdSet::const_iterator iter = file_id_set_.begin(); OB_SUCC(ret) && iter != file_id_set_.end(); iter++) {
      if (OB_FAIL(file_id_array.push_back(iter->first))) {
        LOG_WARN("failed to push backup", K(ret));
      }
    }
  }
  return ret;
}

// ObSSExternTabletMetaWriter

ObSSExternTabletMetaWriter::ObSSExternTabletMetaWriter()
  : is_inited_(false),
    backup_set_dest_(),
    ls_id_(),
    turn_id_(1),
    retry_id_(0),
    dest_id_(),
    io_fd_(0),
    dev_handle_(nullptr),
    file_write_ctx_(),
    file_trailer_(),
    tmp_buffer_("BackupExtInfo")
{
}

ObSSExternTabletMetaWriter::~ObSSExternTabletMetaWriter()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dev_handle_) && io_fd_.is_valid()) {
    LOG_ERROR("device handle and fd is not closed!", KPC_(dev_handle), K_(io_fd));
    ObBackupIoAdapter util;
    if (OB_FAIL(util.close_device_and_fd(dev_handle_, io_fd_))) {
      LOG_ERROR("fail to close device and fd", K(ret), KPC_(dev_handle), K_(io_fd));
    }
  }
}

int ObSSExternTabletMetaWriter::init(
    const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id, 
    const int64_t retry_id, const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_file_id = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet meta writer init twice", K(ret));
  } else if (!backup_set_dest.is_valid() || !ls_id.is_valid() || retry_id < 0 || dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest), K(ls_id), K(retry_id));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to deep copy backup set dest", K(ret));
  } else if (OB_FAIL(tmp_buffer_.ensure_space(BUF_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else {
    ls_id_ = ls_id;
    retry_id_ = retry_id;
    dest_id_ = dest_id;
    if (OB_FAIL(prepare_backup_file_(start_file_id))) {
      LOG_WARN("failed to prepare backup file", K(ret), K(start_file_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSExternTabletMetaWriter::prepare_backup_file_(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  common::ObBackupIoAdapter util;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  const int64_t data_file_size = get_data_file_size();
  ObStorageIdMod mod;
  mod.storage_id_ = dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  const int64_t turn_id = 1; // TODO(yanfeng): turn_id always be 1 in ss backup, consider refactor backup path later
  if (OB_FAIL(ObBackupPathUtil::get_ls_data_tablet_info_path(
      backup_set_dest_, ls_id_, turn_id, retry_id_, file_id, false, backup_path))) {
    LOG_WARN("failed to get ls data tablet info path", K(ret), K(backup_set_dest_), K(ls_id_), K(retry_id_));
  } else if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), backup_set_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(backup_path));
  } else if (OB_FAIL(util.open_with_access_type(dev_handle_,
                                                io_fd_,
                                                backup_set_dest_.get_storage_info(),
                                                backup_path.get_obstr(),
                                                access_type,
                                                mod,
                                                false /*is_batch_write*/))) {
    LOG_WARN("failed to open with access type", K(ret), K(backup_set_dest_), K(backup_path));
  } else if (OB_FAIL(file_write_ctx_.open(data_file_size, io_fd_, *dev_handle_, *GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to open file write ctx", K(ret), K_(io_fd), K(backup_path), K(data_file_size), K(file_id));
  } else {
    file_trailer_.reset();
    file_trailer_.file_id_ = file_id;
    LOG_INFO("open file writer", K(ret), K(backup_path));
  }
  return ret;
}

int64_t ObSSExternTabletMetaWriter::get_data_file_size() const
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (!tenant_config.is_valid()) {
    return DEFAULT_BACKUP_DATA_FILE_SIZE;
  } else {
    return tenant_config->backup_data_file_size;
  }
}

int ObSSExternTabletMetaWriter::write_meta_data(
    const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta writer not inited", K(ret));
  } else if (!meta_data.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_data), K(tablet_id));
  } else if (need_switch_file_(meta_data) && OB_FAIL(switch_file_())) {
    LOG_WARN("failed to switch file", K(ret));
  } else if (OB_FAIL(write_meta_data_(meta_data))) {
    LOG_WARN("failed to write meta data", K(ret));
  } else {
    LOG_INFO("write meta data", K(meta_data), K(tablet_id));
  }
  return ret;
}

bool ObSSExternTabletMetaWriter::need_switch_file_(const blocksstable::ObBufferReader &buffer)
{
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  const int64_t align_size = common::upper_align(header_len + buffer.length(), DIO_READ_ALIGN_SIZE);
  return file_trailer_.length_ + align_size + TRAILER_BUF > get_data_file_size();
}

int ObSSExternTabletMetaWriter::switch_file_()
{
  int ret = OB_SUCCESS;
  int64_t next_file_id = file_trailer_.file_id_ + 1;
  const bool is_succeed = true;
  if (OB_FAIL(close(is_succeed))) {
    LOG_WARN("failed to close", K(ret));
  } else if (OB_FAIL(prepare_backup_file_(next_file_id))) {
    LOG_WARN("failed to prepare backup file", K(ret));
  }
  return ret;
}

int ObSSExternTabletMetaWriter::close(const bool is_succeed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta writer not inited", K(ret));
  } else if (is_succeed && OB_FAIL(flush_trailer_())) {
    LOG_WARN("failed to flush trailer", K(ret));
  } else if (OB_FAIL(file_write_ctx_.close())) {
    LOG_WARN("failed to close file writer", K(ret));
  }

  if (is_succeed && OB_SUCC(ret)) {
    if (OB_FAIL(dev_handle_->complete(io_fd_))) {
      LOG_WARN("fail to complete multipart upload", K(ret), K_(dev_handle), K_(io_fd));
    }
  } else {
    if (OB_NOT_NULL(dev_handle_) && OB_TMP_FAIL(dev_handle_->abort(io_fd_))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
    }
  }
  if (OB_TMP_FAIL(util.close_device_and_fd(dev_handle_, io_fd_))) {
    ret = COVER_SUCC(tmp_ret);
    LOG_WARN("fail to close device or fd", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
  } else {
    dev_handle_ = NULL;
    io_fd_.reset();
  }
  return ret;
}

int ObSSExternTabletMetaWriter::flush_trailer_()
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  ObBackupSerializeHeaderWrapper serializer_wrapper(&file_trailer_);
  int64_t pos = 0;
  int64_t buf_len = serializer_wrapper.get_serialize_size();
  if (OB_FAIL(tmp_buffer_.advance_zero(buf_len))) {
    LOG_WARN("failed to advance zero", K(ret));
  } else if (OB_FAIL(serializer_wrapper.serialize(tmp_buffer_.data(), tmp_buffer_.pos(), pos))) {
    LOG_WARN("failed to serialize", K(ret), K(ret), K(tmp_buffer_));
  } else {
    blocksstable::ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      LOG_INFO("flush data file trailer", K(file_trailer_));
    }
  }
  return ret;
}

int ObSSExternTabletMetaWriter::write_meta_data_(
    const blocksstable::ObBufferReader &meta_data)
{
  int ret = OB_SUCCESS;
  const int64_t alignment = DIO_READ_ALIGN_SIZE;
  const share::ObBackupFileType type = share::ObBackupFileType::BACKUP_TABLET_METAS_INFO;
  if (OB_FAIL(write_data_align_(meta_data, type, alignment))) {
    LOG_WARN("failed to write data align", K(ret), K(meta_data));
  } else {
    int64_t length = 0;
    blocksstable::ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.pos(), tmp_buffer_.pos());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(tmp_buffer_), K(buffer_reader));
    } else if (FALSE_IT(length = tmp_buffer_.pos())) {
    } else {
      file_trailer_.tablet_cnt_++;
      file_trailer_.length_ += length;
    }
  }
  return ret;
}

int ObSSExternTabletMetaWriter::write_data_align_(
    const blocksstable::ObBufferReader &buffer, const share::ObBackupFileType &type, const int64_t alignment)
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  const int64_t align_size = common::upper_align(header_len + buffer.length(), alignment);
  const int64_t align_length = align_size - buffer.length() - header_len;
  ObBackupCommonHeader *common_header = NULL;
  const ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
  if (!buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(tmp_buffer_.write(buffer.data(), buffer.length()))) {
    LOG_WARN("failed to write data", K(ret), K(buffer));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(align_length))) {
    LOG_WARN("failed to advance zero", K(ret), K(align_length));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(tmp_buffer_.data()))) {
  } else if (OB_FAIL(build_common_header(type, buffer.length(), buffer.length(), align_length, compressor_type, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(type), K(buffer), K(align_length));
  } else if (OB_FAIL(common_header->set_checksum(tmp_buffer_.data() + common_header->header_length_, buffer.length()))) {
    LOG_WARN("failed to set common header checksum", K(ret), K(tmp_buffer_), K(buffer), K(*common_header));
  }
  return ret;
}

// ObSSExternTabletMetaReader

int ObSSMetaInfoDirFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t cur_turn_id = 0;
  int64_t cur_retry_id = 0;
  if (2 != sscanf(entry->d_name, "meta_info_turn_%ld_retry_%ld", &cur_turn_id, &cur_retry_id)) {
    // not match, skip
  } else if (cur_turn_id > turn_id_) {
    turn_id_ = cur_turn_id;
    retry_id_ = cur_retry_id;
  } else if (cur_turn_id == turn_id_ && cur_retry_id > retry_id_) {
    retry_id_ = cur_retry_id;
  }
  return ret;
}

ObSSExternTabletMetaReader::ObSSExternTabletMetaReader()
  : is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    ls_id_(),
    cur_tablet_idx_(),
    cur_trailer_idx_(),
    cur_buf_offset_(),
    allocator_(),
    tablet_meta_array_(),
    tablet_info_trailer_array_(),
    turn_id_(),
    retry_id_(),
    mod_(),
    backup_set_dest_()
{
}

ObSSExternTabletMetaReader::~ObSSExternTabletMetaReader()
{
}

int ObSSExternTabletMetaReader::init(const share::ObBackupDest &backup_set_dest,
    const ObStorageIdMod &mod, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet meta reader init twice", K(ret), K(backup_set_dest), K(ls_id));
  } else if (!backup_set_dest.is_valid() || !mod.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_set_dest), K(mod), K(ls_id));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to assign backup dest", K(ret));
  } else if (OB_FAIL(get_max_turn_id_and_retry_id_(ls_id, turn_id_, retry_id_))) {
    LOG_WARN("failed to get max turn_id and retry_id", K(ret), K(ls_id));
  } else if (OB_FAIL(fill_tablet_info_trailer_(backup_set_dest, ls_id, mod))) {
    LOG_WARN("failed to fill tablet info trailer", K(ret), K(backup_set_dest), K(ls_id), K(mod));
  } else {
    ls_id_ = ls_id;
    mod_ = mod;
    is_inited_ = true;
  }
  return ret;
}

int ObSSExternTabletMetaReader::get_tablet_meta_file_path_(
    const share::ObLSID &ls_id, const int64_t file_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_ss_ls_data_tablet_info_path(backup_set_dest_, ls_id, turn_id_, retry_id_, file_id, backup_path))) {
    LOG_WARN("failed to get ss ls data tablet info path", K(ret), K_(backup_set_dest), K(ls_id), K(file_id), K_(turn_id), K_(retry_id));
  }
  return ret;
}

int ObSSExternTabletMetaReader::get_tablet_info_file_id_list_(
    const share::ObLSID &ls_id,
    common::ObIArray<int64_t> &file_id_array)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObTabletInfoFileIdFilter filter;
  share::ObBackupPath backup_path;
  file_id_array.reset();
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest_, ls_id, backup_path))) {
    LOG_WARN("failed to get ls backup dier path", K(ret), K(backup_set_dest_), K(ls_id));
  } else if (OB_FAIL(backup_path.join_meta_info_turn_and_retry(turn_id_, retry_id_, false/*is_final_fuse*/))) {
    LOG_WARN("failed to join info retry", K(ret));
  } else if (OB_FAIL(filter.init())) {
    LOG_WARN("failed to inited", K(ret));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), backup_set_dest_.get_storage_info(), filter))) {
    LOG_WARN("failed to list directories", K(ret), K(backup_path), K(backup_set_dest_));
  } else if (OB_FAIL(filter.get_file_id_array(file_id_array))) {
    LOG_WARN("failed to get file id array", K(ret), K_(backup_set_dest), K(backup_path));
  } else {
    LOG_INFO("succeed get tablet info file ids", K(file_id_array), K_(turn_id), K_(retry_id));
  }
  return ret;
}

int ObSSExternTabletMetaReader::get_max_turn_id_and_retry_id_(
    const share::ObLSID &ls_id,
    int64_t &turn_id,
    int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObSSMetaInfoDirFilter filter;
  share::ObBackupPath backup_path;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest_, ls_id, backup_path))) {
    LOG_WARN("failed to get ls backup dir path", K(ret), K(backup_set_dest_), K(ls_id));
  } else if (OB_FAIL(util.list_directories(backup_path.get_obstr(), backup_set_dest_.get_storage_info(), filter))) {
    LOG_WARN("failed to list directories", K(ret), K(backup_path), K(backup_set_dest_));
  } else if (filter.get_turn_id() <= 0 || filter.get_retry_id() < 0) {
    ret = OB_OBJECT_NOT_EXIST;
    LOG_WARN("invalid turn id and retry id, may not have meta info dir", K(ret), K(filter), K(backup_path));
  } else {
    turn_id = filter.get_turn_id();
    retry_id = filter.get_retry_id();
    LOG_INFO("get max turn_id and retry_id", K(turn_id), K(retry_id), K(backup_path));
  }
  return ret;
}

int ObSSExternTabletMetaReader::fill_tablet_info_trailer_(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> file_ids;
  if (OB_FAIL(get_tablet_info_file_id_list_(ls_id, file_ids))) {
    LOG_WARN("failed to get tablet info file id list", K(ret), K(ls_id));
  }
  ARRAY_FOREACH_X(file_ids, i, cnt, OB_SUCC(ret)) {
    share::ObBackupPath path;
    ObTabletInfoTrailer trailer;
    if (OB_FAIL(get_tablet_meta_file_path_(ls_id, file_ids.at(i), path))) {
      LOG_WARN("failed to get ls data tablet info path", K(ret), K(backup_set_dest_), K(ls_id), K(retry_id_));
    } else if (OB_FAIL(read_file_trailer_(path.get_obstr(), backup_set_dest.get_storage_info(), mod, trailer))) {
      LOG_WARN("failed to read file trailer", K(ret), K(path));
    } else if (OB_FAIL(tablet_info_trailer_array_.push_back(trailer))) {
      LOG_WARN("failed to push back trailer", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    cur_trailer_idx_ = 0;
    cur_buf_offset_ = tablet_info_trailer_array_.at(cur_trailer_idx_).offset_;
    cur_tablet_idx_ = 0;
    LOG_INFO("fill tablet info trailer", K(tablet_info_trailer_array_));
  }
  return ret;
}

int ObSSExternTabletMetaReader::read_file_trailer_(
    const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
    const ObStorageIdMod &mod, ObTabletInfoTrailer &trailer)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  bool exist = false;
  int64_t file_length = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  ObTabletInfoTrailer tmp_trailer;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&tmp_trailer);
  const int64_t trailer_len = serializer_wrapper.get_serialize_size();
  int64_t pos = 0;
  if (OB_FAIL(io_util.is_exist(path, storage_info, exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_OBJECT_NOT_EXIST;
    LOG_WARN("index file do not exist", K(ret), K(path));
  } else if (OB_FAIL(io_util.get_file_length(path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(path, storage_info, mod, file_length - trailer_len, trailer_len, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(file_length), K(trailer_len));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, trailer_len, pos))) {
    LOG_WARN("failed to deserialize.", K(ret));
  } else if (OB_FAIL(trailer.assign(tmp_trailer))) {
    LOG_WARN("failed to assign trailer", K(ret), K(tmp_trailer));
  } else {
    LOG_INFO("read trailer succeed", K(trailer));
  }
  return ret;
}

int ObSSExternTabletMetaReader::get_next(
    common::ObIAllocator &allocator, ObSSRawMetaRow &meta_row)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta reader not init", K(ret));
  } else if (end_() && OB_FAIL(read_next_batch_())) {
    LOG_WARN("failed to update inner array", K(ret));
  } else if (end_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_meta_array_ is empty", K(ret));
  } else if (OB_FAIL(meta_row.deep_copy(allocator, tablet_meta_array_.at(cur_tablet_idx_)))) {
    LOG_WARN("failed to assign tablet meta", K(ret), K(cur_tablet_idx_), K(tablet_meta_array_));
  } else if (OB_FALSE_IT(cur_tablet_idx_++)) {
  }
  return ret;
}

bool ObSSExternTabletMetaReader::end_() 
{
  return tablet_meta_array_.count() == cur_tablet_idx_;
}

int ObSSExternTabletMetaReader::read_next_batch_()
{
  int ret = OB_SUCCESS;
  if (cur_buf_offset_ < tablet_info_trailer_array_.at(cur_trailer_idx_).length_) {
    if (OB_FAIL(read_next_range_tablet_metas_())) {
      LOG_WARN("failed to get next range tablet metas", K(ret), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_));
    }
  } else if (cur_trailer_idx_ < tablet_info_trailer_array_.count() - 1) {
    cur_buf_offset_ = tablet_info_trailer_array_.at(++cur_trailer_idx_).offset_;
    if (OB_FAIL(read_next_range_tablet_metas_())) {
      LOG_WARN("failed to get next range tablet metas", K(ret), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_));
    }
  } else {
    ret = OB_ITER_END;
    LOG_INFO("iterate to the end", K(ret), 
        K(ls_id_), K(retry_id_), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_),
        K(cur_tablet_idx_), K(tablet_meta_array_));
  }
  return ret;
}

int ObSSExternTabletMetaReader::read_next_range_tablet_metas_()
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  char *buf = nullptr;
  const int64_t DEFAULT_BUF_LEN = 2 * MAX_BACKUP_TABLET_META_SERIALIZE_SIZE;
  const int64_t buf_len = MIN(tablet_info_trailer_array_.at(cur_trailer_idx_).length_ - cur_buf_offset_, DEFAULT_BUF_LEN);
  int64_t cur_total_len = 0;
  const int64_t file_id = tablet_info_trailer_array_.at(cur_trailer_idx_).file_id_;
  allocator_.reuse();
  if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(buf_len));
  } else if (OB_FAIL(get_tablet_meta_file_path_(ls_id_, file_id, path))) {
    LOG_WARN("failed to get ls data tablet info path", K(ret), K(file_id), K(backup_set_dest_), K(ls_id_), K(retry_id_));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(path.get_obstr(), backup_set_dest_.get_storage_info(), mod_, cur_buf_offset_, buf_len, buf))) {
    LOG_WARN("failed to pread buffer", K(ret), K(path), K(buf_len));
  } else {
    ObSSRawMetaRow tablet_meta;
    ObArray<ObSSRawMetaRow> cur_tablet_meta_array;
    blocksstable::ObBufferReader buffer_reader(buf, buf_len);
    // deserialize tablet meta one by one.
    while(OB_SUCC(ret)) {
      int64_t pos = 0;
      const ObBackupCommonHeader *common_header = NULL;
      if (buffer_reader.remain() == 0) {
        cur_total_len = buffer_reader.capacity();
        LOG_INFO("read buf finish", K(cur_total_len), K(buffer_reader));
        break;
      } else if (OB_FAIL(buffer_reader.get(common_header))) {
        LOG_WARN("failed to get common_header", K(ret), K(path), K(buffer_reader));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("common header is null", K(ret), K(path), K(buffer_reader));
      } else if (OB_FAIL(common_header->check_valid())) {
        LOG_WARN("common_header is not valid", K(ret), K(path), K(buffer_reader));
      } else if (common_header->data_zlength_ > buffer_reader.remain()) {
        cur_total_len = buffer_reader.pos() - sizeof(ObBackupCommonHeader);
        LOG_INFO("buf not enough, wait later", K(cur_total_len), K(buffer_reader), KPC(common_header));
        break;
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
        LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(buffer_reader));
      } else if (OB_FAIL(tablet_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
        LOG_WARN("failed to read data_header", K(ret), K(*common_header), K(path), K(buffer_reader));
      } else if (OB_FAIL(cur_tablet_meta_array.push_back(tablet_meta))) {
        LOG_WARN("failed to push back tablet meta", K(ret));
      } else if (OB_FAIL(buffer_reader.advance(common_header->data_length_ + common_header->align_length_))) {
        LOG_WARN("failed to advance buffer", K(ret));
      } else {
        LOG_INFO("read tablet meta", K(path), K(tablet_meta));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cur_tablet_meta_array.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet meta is too large", K(ret));
    } else {
      tablet_meta_array_.reset();
      if (OB_FAIL(tablet_meta_array_.assign(cur_tablet_meta_array))) {
        LOG_WARN("failed to assign tablet meta array", K(ret));
      } else {
        cur_buf_offset_ += cur_total_len;
        cur_tablet_idx_ = 0;
        LOG_INFO("read range tablet metas", K(cur_tablet_idx_), K(tablet_meta_array_));
      }
    }
  }
  return ret;
}

int get_ls_inner_tablet_name(const int64_t tablet_id, char *buf, const int64_t buf_len, ObString &name_str)
{
  int ret = OB_SUCCESS;
  ObTabletID id(tablet_id);
  const char *name = nullptr;

  if (id.is_ls_tx_ctx_tablet()) {
    name = "TX_CTX";
  } else if (id.is_ls_tx_data_tablet()) {
    name = "TX_DATA";
  } else if (id.is_ls_lock_tablet()) {
    name = "TX_LOCK";
  } else if (id.is_ls_reorg_info_tablet()) {
    name = "REORG_INFO";
  } else {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%ld", tablet_id))) {
      COMMON_LOG(WARN, "failed to format tablet id", K(ret), K(tablet_id));
    } else {
      name = buf;
    }
  }

  if (OB_SUCC(ret)) {
    name_str.assign_ptr(name, static_cast<int32_t>(strlen(name)));
  }
  return ret;
}

int ObSSBackupUtils::get_macro_block_relative_path(const blocksstable::MacroBlockId &macro_id, char *path, int64_t length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id));
  } else if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(macro_id, path, length, pos))) {
    LOG_WARN("failed to convert to relative remote path format", K(ret), K(macro_id), "storage object type", STI(macro_id.storage_object_type()));
  }
  return ret;
}

int ObSSBackupUtils::get_macro_block_backup_path(
    const blocksstable::MacroBlockId &macro_id,
    const share::ObBackupPath &backup_set_dir_path,
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  char relative_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
  backup_path.reset();
  
  if (!macro_id.is_valid() || backup_set_dir_path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id), K(backup_set_dir_path));
  } else if (OB_FAIL(get_macro_block_relative_path(macro_id, relative_path, OB_MAX_BACKUP_PATH_LENGTH))) {
    LOG_WARN("failed to get macro block relative path", K(ret), K(macro_id));
  } else if (OB_FAIL(backup_path.init(backup_set_dir_path.get_ptr()))) {
    LOG_WARN("failed to init backup path", K(ret), K(backup_set_dir_path));
  } else if (OB_FAIL(backup_path.join(relative_path))) {
    LOG_WARN("failed to join path", K(ret), K(relative_path));
  }
  return ret;
}

int ObSSBackupUtils::calc_start_replay_scn(
    const share::ObBackupSetTaskAttr &set_task_attr,
    const storage::ObBackupSSLSMetaInfosDesc &ls_meta_infos, 
    const share::ObTenantArchiveRoundAttr &round_attr,
    share::SCN &start_replay_scn)
{
  int ret = OB_SUCCESS;
  SCN tmp_start_replay_scn = set_task_attr.start_scn_;
  ARRAY_FOREACH_X(ls_meta_infos.ls_metas_, i, cnt, OB_SUCC(ret)) {
    const ObBackupSSLSMetaInfo &backup_ls_meta = ls_meta_infos.ls_metas_.at(i);
    ObSSLSMeta ss_ls_meta;
    int64_t pos = 0;
    typedef ObDefaultSSMetaValue<ObSSLSMeta> ObSSLSMetaValue;
    SMART_VAR(ObSSLSMetaValue, tmp) {
      if (OB_FAIL(tmp.deserialize(backup_ls_meta.raw_ls_meta_row_.meta_value_.ptr(),
                                  backup_ls_meta.raw_ls_meta_row_.meta_value_.length(),
                                  pos))) {
        LOG_WARN("ss ls meta deserialize failed", K(pos), K(i));
      } else {
        ss_ls_meta = tmp.meta_value_;
        LOG_INFO("get ss ls meta", K(i));
      }
    }
    if (OB_SUCC(ret)) {
      palf::PalfBaseInfo palf_meta;
      if (OB_FAIL(ss_ls_meta.get_palf_meta(palf_meta))) {
        LOG_WARN("failed to get palf meta", K(ret));
      } else {
        tmp_start_replay_scn = SCN::min(tmp_start_replay_scn, palf_meta.prev_log_info_.scn_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    start_replay_scn = SCN::max(tmp_start_replay_scn, round_attr.start_scn_);
    LOG_INFO("calculate start replay scn finish", K(start_replay_scn), K(ls_meta_infos), K(round_attr));
  }
  return ret;
}

int ObSSBackupUtils::calc_min_restore_scn(
    const share::ObBackupSetTaskAttr &set_task_attr,
    const storage::ObBackupSSLSMetaInfosDesc &ls_meta_infos,
    const share::ObTenantArchiveRoundAttr &round_attr,
    share::SCN &min_restore_scn)
{
  int ret = OB_SUCCESS;
  SCN tmp_min_restore_scn = set_task_attr.start_scn_;
  ARRAY_FOREACH_X(ls_meta_infos.ls_metas_, i, cnt, OB_SUCC(ret)) {
    const ObBackupSSLSMetaInfo &backup_ls_meta = ls_meta_infos.ls_metas_.at(i);
    ObSSLSMeta ss_ls_meta;
    int64_t pos = 0;
    typedef ObDefaultSSMetaValue<ObSSLSMeta> ObSSLSMetaValue;
    SMART_VAR(ObSSLSMetaValue, tmp) {
      if (OB_FAIL(tmp.deserialize(backup_ls_meta.raw_ls_meta_row_.meta_value_.ptr(),
                                  backup_ls_meta.raw_ls_meta_row_.meta_value_.length(),
                                  pos))) {
        LOG_WARN("ss ls meta deserialize failed", K(pos), K(i));
      } else {
        ss_ls_meta = tmp.meta_value_;
        LOG_INFO("get ss ls meta", K(i));
      }
    }
    if (OB_SUCC(ret)) {
      palf::PalfBaseInfo palf_meta;
      if (OB_FAIL(ss_ls_meta.get_palf_meta(palf_meta))) {
        LOG_WARN("failed to get palf meta", K(ret));
      } else {
        tmp_min_restore_scn = SCN::max(tmp_min_restore_scn, palf_meta.prev_log_info_.scn_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    min_restore_scn = SCN::max(tmp_min_restore_scn, round_attr.start_scn_);
    LOG_INFO("calculate start replay scn finish", K(min_restore_scn), K(ls_meta_infos), K(round_attr));
  }
  return ret;
}

bool ObSSBackupUtils::need_backup_common_header(const blocksstable::MacroBlockId &block_id)
{
  return block_id.is_shared_tablet_sub_meta_in_table();
}

int ObSSBackupUtils::wrap_with_backup_common_header(
    const blocksstable::MacroBlockId &block_id,
    const blocksstable::ObBufferReader &macro_data,
    common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &wrapped_data)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = sizeof(share::ObBackupCommonHeader);
  const int64_t data_len = macro_data.length();
  const int64_t total_len = header_size + data_len;
  char *buf = nullptr;

  if (OB_ISNULL(macro_data.data()) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro data", K(ret), K(macro_data), K(block_id));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(total_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf for wrapped macro block", K(ret), K(total_len));
  } else {
    share::ObBackupCommonHeader *header = reinterpret_cast<share::ObBackupCommonHeader *>(buf);
    header->reset();
    header->compressor_type_ = static_cast<uint16_t>(common::ObCompressorType::NONE_COMPRESSOR);
    header->data_type_ = static_cast<uint16_t>(share::ObBackupFileType::BACKUP_MACRO_DATA);
    header->data_version_ = share::ObBackupCommonHeader::COMMON_HEADER_VERSION;
    header->data_length_ = data_len;
    header->data_zlength_ = data_len;
    header->align_length_ = 0;
    MEMCPY(buf + header_size, macro_data.data(), data_len);
    if (OB_FAIL(header->set_checksum(buf + header_size, data_len))) {
      LOG_WARN("failed to set backup common header checksum", K(ret), K(block_id));
    } else {
      wrapped_data.assign(buf, total_len, total_len);
      LOG_DEBUG("wrapped macro block with backup common header", K(block_id), K(data_len), K(total_len));
    }
  }
  return ret;
}

int ObSSBackupUtils::unwrap_backup_common_header(
    const blocksstable::MacroBlockId &block_id,
    blocksstable::ObBufferReader &macro_data)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = sizeof(share::ObBackupCommonHeader);

  if (OB_ISNULL(macro_data.data()) || macro_data.length() <= header_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro data is too small to contain backup common header",
             K(ret), K(macro_data), K(block_id), K(header_size));
  } else {
    const share::ObBackupCommonHeader *header =
        reinterpret_cast<const share::ObBackupCommonHeader *>(macro_data.data());
    const int64_t data_buf_size = macro_data.length() - header_size;

    // Validate data_zlength_ does not exceed actual buffer size
    if (header->data_zlength_ > data_buf_size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup common header data_zlength exceeds buffer size",
               K(ret), K(block_id), K(*header), K(data_buf_size), K(macro_data.length()));
    } else if (header->data_length_ > data_buf_size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup common header data_length exceeds buffer size",
               K(ret), K(block_id), K(*header), K(data_buf_size), K(macro_data.length()));
    } else if (OB_FAIL(header->check_header_checksum())) {
      LOG_WARN("backup common header checksum error", K(ret), K(block_id), K(*header));
    } else if (OB_FAIL(header->check_data_checksum(macro_data.data() + header_size,
                                                     header->data_zlength_))) {
      LOG_WARN("backup common header data checksum error", K(ret), K(block_id), K(*header));
    } else {
      const char *raw_data = macro_data.data() + header_size;
      const int64_t raw_data_len = header->data_length_;
      macro_data.assign(raw_data, raw_data_len, raw_data_len);
      LOG_DEBUG("unwrapped backup common header from macro block",
                K(block_id), K(raw_data_len), K(header_size));
    }
  }
  return ret;
}

}
}

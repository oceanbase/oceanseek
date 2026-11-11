/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_ss_storage_ha_reader.h"
#include "lib/literals/ob_literals.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_ss_backup_restore_util.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/high_availability/ob_ss_ha_macro_copy_task_generator.h"
#include "share/io/ob_io_manager.h"
#include "lib/restore/ob_object_storage_base.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

// ObCopySSTabletMeta
OB_SERIALIZE_MEMBER(ObCopySSTabletMeta, tablet_id_, meta_version_);

ObCopySSTabletMeta::ObCopySSTabletMeta()
  : tablet_id_(),
    meta_version_(),
    update_log_()
{
}

void ObCopySSTabletMeta::reset()
{
  tablet_id_.reset();
  meta_version_.reset();
  update_log_.reset();
}

bool ObCopySSTabletMeta::is_valid() const
{
  //allow update_log_ invalid for compatible
  return tablet_id_.is_valid() && meta_version_.is_valid();
}

int ObCopySSTabletMeta::assign(const ObCopySSTabletMeta &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ss tablet meta is invalid", K(ret), K(other));
  } else {
    tablet_id_ = other.tablet_id_;
    meta_version_ = other.meta_version_;
    update_log_ = other.update_log_;
  }

  return ret;
}

// ObCopySSTabletsMetaServiceReader
ObCopySSTabletsMetaServiceReader::ObCopySSTabletsMetaServiceReader()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    snapshot_(),
    is_iter_end_(false),
    total_count_(0),
    iter_guard_(),
    iter_(nullptr),
    total_cost_(0)
{
}

int ObCopySSTabletsMetaServiceReader::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService*);
  ObTimeoutCtx timeout_ctx;
  const int64_t timeout_ts = 600_s;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy sstablets meta service reader init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() || !snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(snapshot));
  } else if (OB_ISNULL(meta_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service v2 should not be null", K(ret), KP(meta_svr));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout_ts))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(timeout_ts));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout_ts))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (FALSE_IT(read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                    ObAtomicProtocolTypeEnum::TABLET_META,
                                                    ls_id,
                                                    share::SCN::min_scn() /*meta_scn*/,
                                                    true /*is_in_table*/))) {
#ifdef ERRSIM
  } else if (FALSE_IT(read_param.set_read_mark_delete(true))) {
#endif
  } else if (OB_FAIL(meta_svr->get_tablet_iter(read_param, snapshot, iter_guard_))) {
    LOG_WARN("failed to get ss tablet iterator", K(ret), K(ls_id), K(snapshot));
  } else if (OB_FAIL(iter_guard_.get_iter(iter_))) {
    LOG_WARN("get iter fail", K(ret), K(ls_id), K(snapshot));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tablet iterator is null", K(ret), K(ls_id), K(snapshot));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    snapshot_ = snapshot;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTabletsMetaServiceReader::get_next_tablet(
    common::ObArenaAllocator &allocator, 
    ObTabletHandle &tablet_handle, 
    ObCopySSTabletMeta &sstablet_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstablets meta service reader not init", K(ret));
  } else {
    ObSSUpdateTabletLog update_log;
    share::SCN meta_scn;
    ObTablet *tablet = nullptr;
    if (is_iter_end_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(iter_->get_next(allocator, tablet_handle, meta_scn, update_log))) {
      if (OB_ITER_END == ret) {
        is_iter_end_ = true;
        LOG_INFO("finish iterator ss tablets", K_(tenant_id), K_(ls_id), K_(snapshot), K_(total_count));
      } else {
        LOG_WARN("failed to get next tablet", K(ret), K_(tenant_id), K_(ls_id), K_(snapshot), K_(total_count));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstablet should not be NULL", K(ret), K_(tenant_id), 
        K_(ls_id), K_(snapshot), K_(total_count), K(tablet_handle));
    } else {
      sstablet_meta.tablet_id_ = tablet->get_tablet_id();
      sstablet_meta.meta_version_ = meta_scn;
      sstablet_meta.update_log_ = update_log;
      ++total_count_;
    }
  }

  return ret;
}

//TODO(muwei.ym) inner sql iterator do not support multi-thread, fix it in next review
int ObCopySSTabletsMetaServiceReader::get_next_tablet(
    common::ObArenaAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len,
    ObCopySSTabletMeta &sstablet_meta)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t cost_ts = 0;
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstablets meta service reader not init", K(ret));
  } else {
    ObSSUpdateTabletLog update_log;
    share::SCN meta_scn;
    if (is_iter_end_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(iter_->get_next(allocator, tablet_raw_buf, tablet_raw_buf_len, meta_scn, update_log))) {
      if (OB_ITER_END == ret) {
        is_iter_end_ = true;
        LOG_INFO("finish iterator ss tablets", K_(tenant_id), K_(ls_id), K_(snapshot), K_(total_count));
      } else {
        LOG_WARN("failed to get next tablet", K(ret), K_(tenant_id), K_(ls_id), K_(snapshot), K_(total_count));
      }
    } else {
      sstablet_meta.tablet_id_ = update_log.tablet_id_;
      sstablet_meta.meta_version_ = meta_scn;
      sstablet_meta.update_log_ = update_log;
      ++total_count_;
      cost_ts = ObTimeUtil::current_time() - start_ts;
      total_cost_ += cost_ts;
    }
  }
  FLOG_INFO("get next tablet finish", K(ret), "cost", cost_ts);
  return ret;
}

int ObCopySSTabletsMetaServiceReader::check_is_iter_end(bool &is_iter_end) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstablets meta service reader not init", K(ret));
  } else {
    is_iter_end = is_iter_end_;
  }

  return ret;
}

// ObCopySSTabletsLsCkptReader 实现
ObCopySSTabletsLsCkptReader::ObCopySSTabletsLsCkptReader()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    ckpt_reader_(),
    cur_idx_(0),
    total_count_(0),
    total_cost_(0)
{
}

int ObCopySSTabletsLsCkptReader::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::MacroBlockId &ls_ckpt_macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy sstablets ls ckpt reader init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid()
             || !ls_ckpt_macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(ls_ckpt_macro_id));
  } else if (OB_FAIL(ckpt_reader_.init())) {
    LOG_WARN("failed to init ls ckpt reader", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ckpt_reader_.load_checkpoint(ls_id, ls_ckpt_macro_id))) {
    LOG_WARN("failed to load pinned ls ckpt", K(ret), K(tenant_id), K(ls_id),
             K(ls_ckpt_macro_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    cur_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTabletsLsCkptReader::get_next_tablet(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObCopySSTabletMeta &sstablet_meta)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstablets ls ckpt reader not init", K(ret));
  } else {
    const common::ObIArray<blocksstable::MacroBlockId> &keys =
        ckpt_reader_.get_tablet_meta_macro_ids();
    if (cur_idx_ >= keys.count()) {
      ret = OB_ITER_END;
    } else {
      const blocksstable::MacroBlockId &macro_id = keys.at(cur_idx_);
      ObSSUpdateTabletLog update_log;
      share::SCN meta_version;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(meta_version.convert_for_inner_table_field(
              static_cast<uint64_t>(macro_id.meta_scn())))) {
        LOG_WARN("failed to convert meta version", K(ret), K(macro_id));
      } else if (OB_FAIL(ckpt_reader_.read_tablet(
                     macro_id, allocator, tablet_handle, update_log))) {
        LOG_WARN("failed to read tablet from ls ckpt", K(ret), K_(ls_id), K(macro_id));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstablet should not be NULL", K(ret), K_(ls_id), K(macro_id), K(tablet_handle));
      } else {
        sstablet_meta.tablet_id_ = tablet->get_tablet_id();
        sstablet_meta.meta_version_ = meta_version;
        sstablet_meta.update_log_ = update_log;
        ++cur_idx_;
        ++total_count_;
        total_cost_ += ObTimeUtil::current_time() - start_ts;
      }
    }
  }
  return ret;
}

int ObCopySSTabletsLsCkptReader::get_next_tablet(
    common::ObArenaAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len,
    ObCopySSTabletMeta &sstablet_meta)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(allocator);
  UNUSED(sstablet_meta);
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;
  LOG_WARN("ls ckpt reader does not support raw tablet buffer", K(ret));
  return ret;
}

int ObCopySSTabletsLsCkptReader::check_is_iter_end(bool &is_iter_end) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstablets ls ckpt reader not init", K(ret));
  } else {
    is_iter_end = cur_idx_ >= ckpt_reader_.get_tablet_meta_macro_ids().count();
  }
  return ret;
}

/******************ObCopySSMacroBlockRestoreReader*********************/
ObCopySSMacroBlockRestoreReader::ObCopySSMacroBlockRestoreReader()
  : is_inited_(false),
    mod_(),
    backup_set_dest_(),
    macro_batch_(),
    curr_idx_(0),
    data_size_()
{
}    

int ObCopySSMacroBlockRestoreReader::init(const uint64_t tenant_id, 
                                          const ObBackupDest &backup_set_dest,
                                          const ObSSHAMacroBlockBatch &macro_batch)
{
  int ret = OB_SUCCESS; 
  int64_t dest_id = 0;
  const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!backup_set_dest.is_valid() || !macro_batch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest), K(macro_batch));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("fail to deep copy backup set dest", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(macro_batch_.assign(macro_batch))) {
    LOG_WARN("fail to assign macro batch", K(ret), K(macro_batch));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_restore_dest_id(
                      *(GCTX.sql_proxy_), tenant_id, backup_dest_type, dest_id))) {
    LOG_WARN("fail to get restore dest id", K(ret), K(tenant_id));
  } else {
    curr_idx_ = 0;
    data_size_ = 0;
    mod_.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
    mod_.storage_id_ = static_cast<uint64_t>(dest_id);    
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSMacroBlockRestoreReader::get_next_macro_block(
                                     common::ObIAllocator &allocator,
                                     blocksstable::ObBufferReader &macro_data, 
                                     blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore reader is not inited", KR(ret));    
  } else if (is_iter_end()) {
    ret = OB_ITER_END;
  } else {
    const ObSSHAMacroBlockInfo &macro_block_info = macro_batch_.at(curr_idx_);
    macro_block_id = macro_block_info.macro_block_id_;
    if (OB_FAIL(backup::ObSSBackupRestoreUtil::read_ss_macro_block(
                                              backup_set_dest_,
                                              macro_block_id,
                                              mod_,
                                              allocator,
                                              macro_data))) {
      LOG_WARN("fail to read ss macro block", K(ret), K(macro_block_id));
    } else if (backup::ObSSBackupUtils::need_backup_common_header(macro_block_id)
               && OB_FAIL(backup::ObSSBackupUtils::unwrap_backup_common_header(
                          macro_block_id, macro_data))) {
      LOG_WARN("fail to unwrap backup common header", K(ret), K(macro_block_id));
    } else {
      data_size_ += macro_block_info.occupy_size_;
      curr_idx_++;
    }
  }
  return ret;
}

// ObCopySSMacroBlockBackupReader
ObCopySSMacroBlockBackupReader::ObCopySSMacroBlockBackupReader()
  : is_inited_(false),
    backup_dest_(),
    backup_set_desc_(),
    mod_(),
    macro_batch_(),
    curr_idx_(0),
    data_size_(0)
{
}

ObCopySSMacroBlockBackupReader::~ObCopySSMacroBlockBackupReader()
{
}

int ObCopySSMacroBlockBackupReader::init(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc,
    const common::ObStorageIdMod &mod,
    const ObSSHAMacroBlockBatch &macro_batch)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup macro block reader init twice", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || !mod.is_valid() || !macro_batch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(mod), K(macro_batch));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(macro_batch_.assign(macro_batch))) {
    LOG_WARN("fail to assign macro batch", K(ret), K(macro_batch));
  } else {
    backup_set_desc_ = backup_set_desc;
    mod_ = mod;
    curr_idx_ = 0;
    data_size_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSMacroBlockBackupReader::get_next_macro_block(
    common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &macro_data, 
    blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectHandle object_handle;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup macro block reader not inited", K(ret));
  } else if (is_iter_end()) {
    ret = OB_ITER_END;
  } else {
    const ObSSHAMacroBlockInfo &macro_block_info = macro_batch_.at(curr_idx_);
    macro_block_id = macro_block_info.macro_block_id_;
    
    if (macro_block_id.is_shared_tablet_sub_meta_in_table()) {
      const uint64_t tenant_id = MTL_ID();
      const int64_t timeout_us = OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id) * 1000LL;
      ObObjectStorageTenantGuard tenant_guard(tenant_id, timeout_us, USER_RESOURCE_OTHER_GROUP_ID);
      if (OB_FAIL(ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
              macro_block_id, ObObjectReadResultFormat::DEFAULT, allocator, macro_data))) {
        LOG_WARN("failed to read tiered metadata", K(ret), K(macro_block_id));
      } else if (OB_UNLIKELY(!macro_data.is_valid() || macro_data.length() <= 0)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid tiered metadata", K(ret), K(macro_block_id), K(macro_data));
      }
    } else if (OB_FAIL(read_macro_block_(macro_block_info, allocator, object_handle))) {
      LOG_WARN("failed to read macro block", K(ret), K(macro_block_id));
    } else if (OB_FAIL(check_macro_block_(macro_block_id, object_handle))) {
      LOG_WARN("failed to check macro block", K(ret), K(macro_block_id));
    } else {
      macro_data.assign(object_handle.get_buffer(), object_handle.get_data_size(), object_handle.get_data_size());
    }
    if (OB_SUCC(ret)) {
      data_size_ += macro_block_info.occupy_size_;
      curr_idx_++;
      LOG_DEBUG("get next macro block success", K(macro_block_id), K(data_size_));
    }
  }
  return ret;
}

//TODO(yanfeng.yyy): use async write to optimize performance
int ObCopySSMacroBlockBackupReader::read_macro_block_(
    const ObSSHAMacroBlockInfo &macro_block_info,
    common::ObIAllocator &allocator,
    blocksstable::ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const blocksstable::MacroBlockId &block_id = macro_block_info.macro_block_id_;
  const bool is_lob = ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO == block_id.storage_object_type();
  blocksstable::ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = block_id;
  read_info.offset_ = 0;
  // LOB tasks carry the exact object length collected before scheduling.
  read_info.size_ = is_lob ? macro_block_info.occupy_size_ : OB_DEFAULT_MACRO_BLOCK_SIZE;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
  read_info.io_desc_.set_sys_module_id(ObIOModule::BACKUP_READER_IO);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.mtl_tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(read_info.size_ <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid lob object size", K(ret), K(block_id), K(read_info));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for read buf", KR(ret), K_(read_info.size));
  } else if (OB_FALSE_IT(read_info.buf_ = buf)) {
  } else if (OB_FAIL(ObObjectManager::read_object(read_info, object_handle))) {
    LOG_WARN("failed to read macro block", KR(ret), K(read_info), K(object_handle));
  } else if (OB_UNLIKELY(object_handle.get_data_size() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object data size less than 0", K(ret));
  } else if (is_lob && OB_UNLIKELY(object_handle.get_data_size() != read_info.size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob object data size", K(ret), K(block_id), K(read_info), K(object_handle));
  }
  
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator.free(buf);    
  }
  return ret;
}

int ObCopySSMacroBlockBackupReader::check_macro_block_(
    const blocksstable::MacroBlockId &block_id,
    blocksstable::ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableMacroBlockChecker checker;
  if (block_id.is_shared_tablet_sub_meta_in_table()
      || ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO == block_id.storage_object_type()) {
    // Tiered metadata and LOB data have no SSTable macro block header.
  } else if (OB_FAIL(checker.check(object_handle.get_buffer(), object_handle.get_data_size()))) {
    LOG_ERROR("failed to check macro block", K(ret), K(block_id), K(object_handle));
  }
  return ret;
}

// ==================== ObRestoreSSLSMetaIterator ====================
ObRestoreSSLSMetaIterator::ObRestoreSSLSMetaIterator()
  : ObIHASSLSMetaIter(), 
    is_inited_(false),
    iterated_ls_cnt_(0),
    tenant_id_(OB_INVALID_TENANT_ID), 
    dest_id_(-1), 
    backup_set_dest_(), 
    ls_id_array_()
{
}

int ObRestoreSSLSMetaIterator::init(const uint64_t tenant_id, const ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  storage::ObBackupDataLSAttrDesc backup_ls_attr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRestoreSSLSMetaIterator already inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(tenant_id), K(backup_set_dest));
  } else if (OB_FALSE_IT(tenant_id_ = tenant_id)) {
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("fail to deep copy backup dest", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.init(backup_set_dest_))) {
    LOG_WARN("fail to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.read_ls_attr_info(backup_ls_attr))) {
    LOG_WARN("failed to read ls info", K(ret), K(store));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_ls_attr.ls_attr_array_.count(); i++) {
      if (OB_FAIL(ls_id_array_.push_back(backup_ls_attr.ls_attr_array_.at(i).get_ls_id()))) {
        LOG_WARN("Failed to push back ls id", K(ret));
      }            
    }
    
    const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
    if (OB_FAIL(ret)) {      
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_restore_dest_id(
                        *(GCTX.sql_proxy_), tenant_id_, backup_dest_type, dest_id_))) {
      LOG_WARN("fail to get restore dest id", K(ret), K_(tenant_id));
    } else if (ls_id_array_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read no ls attr from backup set", K(ret), K_(tenant_id), K_(backup_set_dest));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObRestoreSSLSMetaIterator::get_next(ObIAllocator &allocator, ObSSRawMetaRow &ls_meta_row)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator tmp_allocator;
  storage::ObBackupSSLSMetaInfo ss_ls_meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestoreSSLSMetaIterator not inited", K(ret));
  } else if (iterated_ls_cnt_ == ls_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(backup::ObSSBackupRestoreUtil::read_ss_ls_meta(backup_set_dest_,
                       ls_id_array_.at(iterated_ls_cnt_), dest_id_, tmp_allocator, ss_ls_meta))) {
    LOG_WARN("failed to read ss ls meta", K(ret));
  } else if (OB_FAIL(ls_meta_row.deep_copy(allocator, ss_ls_meta.raw_ls_meta_row_))) {
    LOG_WARN("failed to deep copy ls meta row", K(ret));
  } else {
    iterated_ls_cnt_++;
  }
  return ret;
}

// ==================== ObRestoreSSTabletMetatIterator ====================
ObRestoreSSTabletMetatIterator::ObRestoreSSTabletMetatIterator()
  : ObIHASSTabletMetaIter(),
    is_inited_(false),
    iterated_tablet_cnt_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    reader_()
{
}

int ObRestoreSSTabletMetatIterator::init(const share::ObLSID &ls_id, 
                                         const uint64_t tenant_id, 
                                         const ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
  int64_t dest_id = -1;
  ObStorageIdMod mod;    
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!ls_id.is_valid() || !is_valid_tenant_id(tenant_id) || !backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tenant_id), K(backup_set_dest));
  } else if (OB_FALSE_IT(tenant_id_ = tenant_id)) {    
  } else if (OB_FALSE_IT(ls_id_ = ls_id)) {    
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_restore_dest_id(
                        *(GCTX.sql_proxy_), tenant_id_, backup_dest_type, dest_id))) {
      LOG_WARN("fail to get restore dest id", K(ret), K_(tenant_id));
  } else {
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
    mod.storage_id_ = static_cast<uint64_t>(dest_id);
    if (OB_FAIL(reader_.init(backup_set_dest, mod, ls_id_))) {
      LOG_WARN("fail to init ObSSExternTabletMetaReader", K(ret), K(backup_set_dest), K(mod), K(ls_id));
    } else {
      is_inited_ = true;
    }
  }     
  return ret;
}

int ObRestoreSSTabletMetatIterator::get_next(ObIAllocator &allocator, ObSSRawMetaRow &ss_tablet_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(reader_.get_next(allocator, ss_tablet_row))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("iter end", K(ret), K_(tenant_id), K_(ls_id));
    } else {
      LOG_WARN("fail to get next tablet meta row", K(ret), K_(tenant_id), K_(ls_id));
    }
  } else {
    iterated_tablet_cnt_++;
  }
  return ret;  
}

// ==================== ObBackupSSLSMetaIterator ====================
ObBackupSSLSMetaIterator::ObBackupSSLSMetaIterator()
  : ObIHASSLSMetaIter(), 
    is_inited_(false),
    iterated_ls_cnt_(0),
    tenant_id_(OB_INVALID_TENANT_ID), 
    backup_set_dest_(), 
    ls_id_array_(),
    current_ls_id_()
{
}

int ObBackupSSLSMetaIterator::init(const uint64_t tenant_id, 
                                   const common::ObSArray<share::ObLSID> &ls_id_array, 
                                   const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  
  if (!is_valid_tenant_id(tenant_id) || ls_id_array.empty() || !snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(tenant_id), K(ls_id_array), K(snapshot));
  } else if (OB_FAIL(ls_id_array_.assign(ls_id_array))) {
    LOG_WARN("failed to assign ls id array", K(ret), K(ls_id_array));
  } else {
    tenant_id_ = tenant_id;
    snapshot_ = snapshot;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupSSLSMetaIterator::get_next(ObIAllocator &allocator, ObSSRawMetaRow &ls_meta_row)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = nullptr;
  ObSSMetaReadParamV2 read_param;
  const share::SCN snapshot = snapshot_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupSSLSMetaIterator not inited", K(ret));
  } else if (iterated_ls_cnt_ >= ls_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    current_ls_id_ = ls_id_array_.at(iterated_ls_cnt_);
    MTL_SWITCH(gen_user_tenant_id(MTL_ID())) {
      if (OB_ISNULL(meta_svr = MTL(ObSSMetaService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
      } else if (OB_FALSE_IT(read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                                          ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                          ObAtomicProtocolTypeEnum::LS_META,
                                                          current_ls_id_,
                                                          share::SCN::min_scn() /*meta_scn*/,
                                                          true /*is_in_table*/))) {
      } else if (OB_FAIL(meta_svr->get_raw_meta(read_param, snapshot, allocator, ls_meta_row))) {
        LOG_WARN("failed to get ss raw meta", K(ret), K(read_param), K(snapshot));
      } else {
        iterated_ls_cnt_++;
      }
    }
  }
  return ret;
}

// ==================== ObBackupSSTableMetatIterator ====================
ObBackupSSTableMetatIterator::ObBackupSSTableMetatIterator()
  : ObIHASSTabletMetaIter(),
    is_inited_(false),
    iterated_tablet_cnt_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    meta_svr_(nullptr),
    param_(),
    iter_(nullptr),
    iter_guard_(),
    snapshot_()
{
}

int ObBackupSSTableMetatIterator::init(const share::ObLSID &ls_id, const uint64_t tenant_id, const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!ls_id.is_valid() || !is_valid_tenant_id(tenant_id) || !snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(snapshot));
  } else if (OB_FALSE_IT(param_.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                                  ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                  ObAtomicProtocolTypeEnum::TABLET_META,
                                                  ls_id,
                                                  share::SCN::min_scn() /*meta_scn placeholder*/,
                                                  true /*is_in_table*/))) {
  } else {
    MTL_SWITCH(gen_user_tenant_id(MTL_ID())) {
      if (OB_ISNULL(meta_svr_ = MTL(ObSSMetaService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
      } else if (OB_FAIL(meta_svr_->get_latest_raw_meta_iter(param_, snapshot, iter_guard_))) {
        LOG_WARN("failed to get ss raw meta iter", K(ret), K_(param));
      } else if (OB_FAIL(iter_guard_.get_iter(iter_))) {
        LOG_WARN("failed to get iter", K(ret));
      } else if (OB_ISNULL(iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter should not be null", K(ret));
      } else {
        tenant_id_ = tenant_id;    
        snapshot_ = snapshot;
        ls_id_ = ls_id;        
        is_inited_ = true;
      }
    }    
  }     
  return ret;
}

int ObBackupSSTableMetatIterator::get_next(ObIAllocator &allocator, ObSSRawMetaRow &ss_tablet_row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(iter_->get_next(allocator, ss_tablet_row))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("iter end", K(ret), K_(tenant_id), K_(ls_id));
    } else {
      LOG_WARN("fail to get next tablet meta row", K(ret), K_(tenant_id), K_(ls_id));
    }
  } else {
    iterated_tablet_cnt_++;
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

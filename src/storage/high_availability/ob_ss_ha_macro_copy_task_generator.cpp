/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_ha_macro_copy_task_generator.h"
#include "share/ob_define.h"
#include "lib/ob_errno.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_ss_backup_restore_util.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/backup/ob_ss_backup_ctx.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/high_availability/ob_ss_storage_ha_reader.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_clean_util.h"
#include "rootserver/backup/ob_backup_service.h"
#include "storage/incremental/ob_shared_meta_service.h"

using namespace oceanbase::share;
using namespace oceanbase::storage;

bool ObSSHAMacroCopyTaskGeneratorInitParam::is_valid() const
{
  bool b_ret = true;
  if (!is_valid_tenant_id(tenant_id_) || parent_task_id_ <= 0 || !task_type_.is_valid()) {
    b_ret = false;
  } else {
    if (task_type_.is_restore()) {
      b_ret = backup_set_dest_.is_valid();
    } else if (task_type_.is_backup()) {
      b_ret = ls_id_array_.count() > 0 && snapshot_.is_valid();
    } else if (task_type_.is_backup_clean()) {
      b_ret = backup_set_dest_.is_valid();
    } else {
      b_ret = false;
    }
  }
  return b_ret;
}

int ObSSHAMacroCopyTaskGeneratorInitParam::assign(const ObSSHAMacroCopyTaskGeneratorInitParam &other)
{
  int ret = OB_SUCCESS;
  if (&other == this) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    parent_task_id_ = other.parent_task_id_;
    task_type_ = other.task_type_;
    if (OB_FAIL(backup_set_dest_.deep_copy(other.backup_set_dest_))) {
      LOG_WARN("fail to deep copy backup set dest", K(ret), K(other));
    } else if (OB_FAIL(ls_id_array_.assign(other.ls_id_array_))) {
      LOG_WARN("fail to assign ls id array", K(ret), K(other));
    } else {
      snapshot_ = other.snapshot_;
    }
  }
  return ret;
}

// ==================== ObSSHAMacroBlockBatchPersister ====================
ObSSHAMacroBlockBatchPersister::ObSSHAMacroBlockBatchPersister()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    parent_task_id_(0),
    current_batch_(),
    curr_ls_id_(),
    curr_task_id_(1),
    total_task_count_(0),
    total_macro_cnt_(0),
    total_bytes_(0),
    task_type_(),
    trans_end_helper_(nullptr)
{
}

ObSSHAMacroBlockBatchPersister::~ObSSHAMacroBlockBatchPersister()
{
}

int ObSSHAMacroBlockBatchPersister::init(const uint64_t tenant_id,
    const int64_t parent_task_id, const ObSSHAMacroTaskType &task_type,
    share::ObHATransEndHelper *trans_end_helper)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSHAMacroBlockBatchPersister already inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else {
    tenant_id_ = tenant_id;
    parent_task_id_ = parent_task_id;
    task_type_ = task_type;
    trans_end_helper_ = trans_end_helper;
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroBlockBatchPersister not inited", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::delete_tasks(
      *GCTX.sql_proxy_, tenant_id_, parent_task_id_, task_type_, trans_end_helper_))) {
    LOG_WARN("fail to delete tasks", KR(ret), K(tenant_id_), K(parent_task_id_), K(task_type_));
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::add_macro_block(const ObSSHAMacroBlockInfo &macro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroBlockBatchPersister not inited", K(ret));
  } else if (!macro_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_info));
  } else if (OB_FAIL(switch_ls_id_if_need_(macro_info))) {
    LOG_WARN("fail to switch ls id if need", K(ret), K(macro_info));
  } else if (OB_FAIL(current_batch_.add_one(macro_info))) {
    LOG_WARN("fail to add macro block to batch", K(ret), K(macro_info));
  } else if (MACRO_BLOCK_BATCH_SIZE == current_batch_.count()) {
    if (OB_FAIL(persist_current_batch_())) {
      LOG_WARN("fail to persist curr batch after reaching batch size", K(ret), K_(current_batch));
    }
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::switch_ls_id_if_need_(const ObSSHAMacroBlockInfo &macro_info)
{
  int ret = OB_SUCCESS;
  if (curr_ls_id_ != macro_info.ls_id_) {
    if (OB_FAIL(persist_current_batch_())) {
      LOG_WARN("fail to persist curr batch",
               K(ret), K_(curr_ls_id), K(macro_info), K_(current_batch));
    } else {
      curr_ls_id_ = macro_info.ls_id_;
      curr_task_id_ = 1;
    }
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::add_task_attr(const ObSSHAMacroBlockTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSHAMacroBlockTaskAttr> task_attrs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroBlockBatchPersister not inited", K(ret));
  } else if (!task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_attr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(task_attrs.push_back(task_attr))) {
    LOG_WARN("fail to push back task attr", K(ret), K(task_attr));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
      *(GCTX.sql_proxy_), tenant_id_, task_attrs))) {
    LOG_WARN("failed to batch insert macro block tasks", K(ret), K(tenant_id_), K(task_attrs));
  } else {
    total_task_count_++;
    LOG_INFO("successfully added task attrs via persister", K(task_attr), K(total_task_count_));
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::persist_current_batch_()
{
  int ret = OB_SUCCESS;
  ObArray<ObSSHAMacroBlockTaskAttr> task_attrs;
  if (current_batch_.is_empty()) {
    //do nothing
  } else {
    ObMySQLTransaction trans;
    rootserver::ObBackupDataService *backup_service = MTL(rootserver::ObBackupDataService *);
    HEAP_VAR(ObSSHAMacroBlockTaskAttr, task_attr) {
      if (OB_FAIL(build_task_attr_(current_batch_, task_attr))) {
        LOG_WARN("fail to build task attr", K(ret), K_(current_batch));
      } else if (OB_ISNULL(GCTX.sql_proxy_)|| OB_ISNULL(backup_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy or backup service is null", K(ret), KP(GCTX.sql_proxy_), KP(backup_service));
      } else if (OB_FAIL(task_attrs.push_back(task_attr))) {
        LOG_WARN("fail to push back task attr", K(ret), K(task_attr));
      } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
        LOG_WARN("fail to start transaction", K(ret), K_(tenant_id));
      } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
                          trans, tenant_id_, task_attrs))) {
        LOG_WARN("fail to insert macro block task", K(ret), K(tenant_id_), K(task_attr));
      }
    }
    // End transaction
    int trans_ret = backup_service->end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }

    if (OB_SUCC(ret)) {
      curr_task_id_++;
      total_task_count_++;
      total_macro_cnt_ += current_batch_.count();
      total_bytes_ += current_batch_.get_total_bytes();
      current_batch_.reuse();
    }
  }
  return ret;
}

int ObSSHAMacroBlockBatchPersister::build_task_attr_(
    const ObSSHAMacroBlockBatch &batch,
    share::ObSSHAMacroBlockTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  if (!batch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(batch));
  } else if (OB_FAIL(task_attr.macro_id_list_.set_macro_block_batch(batch))) {
    LOG_WARN("fail to assign macro block batch", K(ret));
  } else {
    task_attr.parent_task_id_ = parent_task_id_;
    task_attr.task_type_ = task_type_;
    task_attr.tenant_id_ = tenant_id_;
    task_attr.ls_id_ = curr_ls_id_.id();
    task_attr.task_id_ = curr_task_id_;
    task_attr.total_bytes_ = batch.get_total_bytes();
    task_attr.status_ = ObBackupTaskStatus::INIT;
    task_attr.macro_block_cnt_ = batch.count();
  }
  return ret;
}


int ObSSHAMacroBlockBatchPersister::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroBlockBatchPersister not inited", K(ret));
  } else if (OB_FAIL(persist_current_batch_())) {
    LOG_WARN("fail to persist curr batch", K(ret), K_(current_batch));
  }
  return ret;
}

// ==================== ObSSHAMacroCopyTaskGenerator ====================
ObSSHAMacroCopyTaskGenerator::ObSSHAMacroCopyTaskGenerator()
  : is_inited_(false),
    init_param_(),
    strategy_(nullptr),
    persister_()
{
}

ObSSHAMacroCopyTaskGenerator::~ObSSHAMacroCopyTaskGenerator()
{
  free_generation_strategy_();
}


int ObSSHAMacroCopyTaskGenerator::init(const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
                                       share::ObHATransEndHelper *trans_end_helper)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSHAMacroCopyTaskGenerator already inited", K(ret));
  } else if (!init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid init param", K(ret), K(init_param));
  } else if (OB_FAIL(init_param_.assign(init_param))) {
    LOG_WARN("Failed to assign init param", K(ret));
  } else if (OB_FAIL(persister_.init(init_param_.tenant_id_, init_param_.parent_task_id_,
                                      init_param_.task_type_, trans_end_helper))) {
    LOG_WARN("fail to init persister", K(ret), K_(init_param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::build_generation_strategy_(const share::ObSSHAMacroTaskType &task_type)
{
  int ret = OB_SUCCESS;

  switch (task_type.type_) {
    case share::ObSSHAMacroTaskType::Type::BACKUP: {
      lib::ObMemAttr backup_attr(MTL_ID(), ObModIds::BACKUP);
      if (OB_ISNULL(strategy_ = OB_NEW(ObBackupMacroCopyTaskGenStrategy, backup_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc backup strategy", K(ret));
      }
      break;
    }
    case share::ObSSHAMacroTaskType::Type::RESTORE: {
      lib::ObMemAttr restore_attr(MTL_ID(), ObModIds::RESTORE);
      if (OB_ISNULL(strategy_ = OB_NEW(ObRestoreMacroCopyTaskGenStrategy, restore_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc restore strategy", K(ret));
      }
      break;
    }
    case share::ObSSHAMacroTaskType::Type::BACKUP_CLEAN: {
      lib::ObMemAttr attr(MTL_ID(), "BackupClean");
      if (OB_ISNULL(strategy_ = OB_NEW(ObBackupCleanMacroCopyTaskGenStrategy, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc backup clean strategy", K(ret));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected macro task type when building strategy", K(ret), K(task_type));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    free_generation_strategy_();
  }
  return ret;
}

void ObSSHAMacroCopyTaskGenerator::free_generation_strategy_()
{
  if (OB_NOT_NULL(strategy_)) {
    strategy_->~ObSSHAMacroCopyTaskGenStrategy();
    ob_free(strategy_);
    strategy_ = nullptr;
  }
}

int ObSSHAMacroCopyTaskGenerator::generate(
    int64_t &total_task_cnt,
    int64_t &total_macro_cnt,
    int64_t &total_bytes)
{
  int ret = OB_SUCCESS;
  total_task_cnt = 0;
  total_macro_cnt = 0;
  total_bytes = 0;
  const uint64_t tenant_id = init_param_.tenant_id_;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroCopyTaskGenerator not inited", K(ret));
  } else if (OB_FAIL(build_generation_strategy_(init_param_.task_type_))) {
    LOG_WARN("fail to build strategy", K(ret), K(init_param_));
  } else if (OB_ISNULL(strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("strategy is null ptr", K(ret), KP(strategy_));
  } else if (OB_FAIL(persister_.start())) {
    LOG_WARN("fail to start persister", K(ret));
  } else if (OB_FAIL(scan_all_ls_())) {
    LOG_WARN("fail to scan all ls", K(ret));
  } else if (OB_FAIL(persister_.close())) {
    LOG_WARN("fail to persist last batch of macro ids", K(ret));
  } else {
    total_task_cnt = persister_.get_total_task_count();
    total_macro_cnt = persister_.get_total_macro_cnt();
    total_bytes = persister_.get_total_bytes();
  }
  free_generation_strategy_();
  return ret;
}


int ObSSHAMacroCopyTaskGenerator::scan_all_ls_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_ISNULL(strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("strategy is null", K(ret));
  } else if (init_param_.task_type_.is_backup_clean()) {
    if (OB_FAIL(scan_ls_for_backup_clean_())) {
      LOG_WARN("failed to scan ls for backup clean", K(ret));
    }
  } else {
    if (OB_FAIL(scan_ls_with_iterator_())) {
      LOG_WARN("failed to scan ls with iterator", K(ret));
    }
  }

  const int64_t cost = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("scan all ls finish", K(ret), "tenant_id", init_param_.tenant_id_,
           "total_macro_cnt", persister_.get_total_macro_cnt(), K(cost));
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::scan_ls_for_backup_clean_()
{
  int ret = OB_SUCCESS;
  // For backup clean, read ls_attr_info directly and get ls_id_array, no need for iterator
  ObBackupDataStore store;
  storage::ObBackupDataLSAttrDesc backup_ls_attr;
  ObSSRawMetaRow ls_meta_row; // UNUSED
  share::ObLSID ls_id;
  ObArray<share::ObLSID> ls_id_array;

  if (OB_FAIL(store.init(init_param_.backup_set_dest_))) {
    LOG_WARN("fail to init backup data store", K(ret), "backup_set_dest", init_param_.backup_set_dest_);
  } else if (OB_FAIL(store.read_ls_attr_info(backup_ls_attr))) {
    LOG_WARN("failed to read ls info", K(ret), K(store));
    if (OB_OBJECT_NOT_EXIST == ret) {
      // ls_attr_info file not exist (incomplete backup set), fallback to list directories by prefix
      LOG_INFO("ls_attr_info not exist, fallback to list logstream directories by prefix", K(ret));
      ret = OB_SUCCESS;
      if (OB_FAIL(share::ObBackupCleanUtil::get_ls_ids_from_backup_set_path(
          init_param_.backup_set_dest_.get_root_path(),
          init_param_.backup_set_dest_.get_storage_info(),
          ls_id_array))) {
        LOG_WARN("failed to get ls ids from backup set path", K(ret));
      }
    } else {
      LOG_WARN("failed to read ls info", K(ret), K(store));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_ls_attr.ls_attr_array_.count(); i++) {
      if (OB_FAIL(ls_id_array.push_back(backup_ls_attr.ls_attr_array_.at(i).get_ls_id()))) {
        LOG_WARN("fail to push back ls id", K(ret));
      }
    }
  }

  // generate tasks for each ls
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array.count(); i++) {
    ls_id = ls_id_array.at(i);
    if (OB_FAIL(strategy_->generate_tasks_by_ls(ls_id, init_param_, &persister_, ls_meta_row))) {
      LOG_WARN("fail to generate tasks for ls", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::scan_ls_with_iterator_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObIHASSLSMetaIter *ls_iter = nullptr;
  const int64_t timeout_us = 24_hour;
  const int64_t previous_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + timeout_us);
  if (OB_FAIL(strategy_->alloc_and_init_ls_iter(init_param_, ls_iter))) {
    LOG_WARN("fail to alloc and init ls iter", K(ret));
  } else if (OB_ISNULL(ls_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter is null", K(ret));
  } else {
    share::ObLSID ls_id;
    ObSSRawMetaRow ls_meta_row;
    while (OB_SUCC(ret)) {
      ls_id.reset();
      ls_meta_row.reset();
      if (OB_FAIL(ls_iter->get_next(allocator, ls_meta_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("Failed to get next ls meta row", K(ret), KPC(ls_iter));
        }
      } else if (OB_FAIL(get_ls_id_from_meta_row_(ls_meta_row, ls_id))) {
        LOG_WARN("fail to parse ls id from meta row", K(ret), K(ls_meta_row));
      } else if (OB_FAIL(strategy_->generate_tasks_by_ls(ls_id, init_param_, &persister_, ls_meta_row))) {
        LOG_WARN("fail to generate tasks for ls", K(ret), K(ls_id), K(ls_meta_row));
      } else if (OB_FALSE_IT(allocator.reuse())) {
      } else if (OB_FAIL(scan_tablets_in_ls_(ls_id))) {
        LOG_WARN("fail to scan tablets in ls", K(ret), K(ls_id));
      }
    }
  }

  if (OB_NOT_NULL(ls_iter)) {
    strategy_->free_ls_iter(ls_iter);
  }
  THIS_WORKER.set_timeout_ts(previous_timeout_ts);
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::scan_tablets_in_ls_(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObIHASSTabletMetaIter *tablet_iter = nullptr;
  const uint64_t tenant_id = init_param_.tenant_id_;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret), K(ls_id));
  } else if (OB_FAIL(strategy_->alloc_and_init_tablet_iter(init_param_, ls_id, tablet_iter))) {
    LOG_WARN("fail to alloc and init tablet iter", K(ret), K(ls_id));
  } else {
    common::ObTabletID tablet_id;
    while (OB_SUCC(ret)) {
      tablet_id.reset();
      ObSSRawMetaRow ss_tablet_row;
      if (OB_FAIL(tablet_iter->get_next(allocator, ss_tablet_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("Failed to get next ls meta row", K(ret), KPC(tablet_iter));
        }
      } else if (OB_FAIL(strategy_->generate_tasks_by_tablet(tenant_id, ls_id, allocator, &persister_, ss_tablet_row))) {
        LOG_WARN("fail to generate tasks for tablet", K(ret), K(ls_id), K(ss_tablet_row));
      } else {
        allocator.reuse();
      }
    }
  }
  strategy_->free_tablet_iter(tablet_iter);
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::get_ls_id_from_meta_row_(const ObSSRawMetaRow &ls_meta_row, share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  share::SCN meta_scn;

  if (static_cast<int64_t>(ObSSMetaTypeEnum::LS_META) != ls_meta_row.meta_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta row is not ss ls_meta", K(ret), K(ls_meta_row));
  } else if (OB_UNLIKELY(ls_meta_row.meta_key_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta key is empty", K(ret), K(ls_meta_row));
  } else if (OB_FAIL(storage::ObSSMetaInfoUtil::parse_ls_meta_key(ls_meta_row.meta_key_, ls_id, meta_scn))) {
    LOG_WARN("fail to parse ls meta key", K(ret), K(ls_meta_row));
  } else if (!ls_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id in meta key is invalid", K(ret), K(ls_id));
  }
  return ret;
}

int ObSSHAMacroCopyTaskGenerator::get_tablet_id_from_meta_row_(const ObSSRawMetaRow &ss_tablet_row, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  ObLSID fake_ls_id(ObLSID::SYS_LS_ID);
  ObTabletID fake_tablet_id(ObTabletID::MIN_VALID_TABLET_ID);
  uint64_t op_id = 1;
  ObMetaDiskAddr tablet_addr;
  ObArenaAllocator allocator;
  int64_t pos = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_addr(fake_ls_id,
                                                fake_tablet_id,
                                                share::SCN::min_scn(),
                                                op_id,
                                                ss_tablet_row.meta_value_.length(),
                                                tablet_addr))) {
    LOG_WARN("generate tablet addr failed", K(ret), K(ss_tablet_row));
  } else if (OB_FAIL(ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(tablet_addr,
                                                                               ss_tablet_row.meta_value_.ptr(),
                                                                               ss_tablet_row.meta_value_.length(),
                                                                               pos,
                                                                               tablet_handle,
                                                                               allocator))) {
    LOG_WARN("failed to get ss tablet handle", K(ret), K(ss_tablet_row));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret));
  } else {
    tablet_id = tablet->get_tablet_id();
  }

  return ret;
}

//==================== ObBackupMacroCopyTaskGenStrategy ====================
int ObBackupMacroCopyTaskGenStrategy::alloc_and_init_ls_iter(
      const ObSSHAMacroCopyTaskGeneratorInitParam &param, ObIHASSLSMetaIter *&ls_iter)
{
  int ret = OB_SUCCESS;
  ObBackupSSLSMetaIterator *tmp_iter = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::BACKUP);

  if (OB_NOT_NULL(ls_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls iter is not null", K(ret), KP(ls_iter));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObBackupSSLSMetaIterator, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_iter->init(param.tenant_id_, param.ls_id_array_, param.snapshot_))) {
    LOG_WARN("fail to init backup ls iter", K(ret), K(param));
  } else {
    ls_iter = tmp_iter;
    tmp_iter = nullptr;
  }

  if (OB_NOT_NULL(tmp_iter)) {
    OB_DELETE(ObBackupSSLSMetaIterator, ObModIds::BACKUP, tmp_iter);
  }
  return ret;
}

int ObBackupMacroCopyTaskGenStrategy::alloc_and_init_tablet_iter(
    const ObSSHAMacroCopyTaskGeneratorInitParam &param,
    const ObLSID &ls_id, ObIHASSTabletMetaIter *&tablet_iter)
{
  int ret = OB_SUCCESS;
  ObBackupSSTableMetatIterator *tmp_iter = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::BACKUP);

  if (!param.is_valid() || !ls_id.is_valid() || OB_NOT_NULL(tablet_iter)) {
  ret = OB_INVALID_ARGUMENT;
  LOG_WARN("in valid args", K(ret), K(param), K(ls_id), KP(tablet_iter));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObBackupSSTableMetatIterator, attr))) {
  ret = OB_ALLOCATE_MEMORY_FAILED;
  LOG_WARN("fail to alloc mem", KR(ret));
  } else if (OB_FAIL(tmp_iter->init(ls_id, param.tenant_id_, param.snapshot_))) {
  LOG_WARN("fail to init backup tablet meta iter", K(ret), K(ls_id), K(param));
  } else {
  tablet_iter = tmp_iter;
  tmp_iter = nullptr;
  }

  if (OB_NOT_NULL(tmp_iter)) {
  OB_DELETE(ObBackupSSTableMetatIterator, ObModIds::BACKUP, tmp_iter);
  }
  return ret;
}

int ObBackupMacroCopyTaskGenStrategy::generate_tasks_by_tablet(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObArenaAllocator &allocator,
    ObSSHAMacroBlockBatchPersister *persister,
    ObSSRawMetaRow &ss_tablet_row)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  share::SCN reorganize_scn;
  share::SCN meta_scn;
  ObSEArray<ObTabletBlockInfo, 64> block_infos;

  if (static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META) != ss_tablet_row.meta_type_ || OB_ISNULL(persister)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ss_tablet_row), KP(persister));
  } else if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(ss_tablet_row.meta_key_,
                                                             tablet_id, reorganize_scn, meta_scn))) {
    LOG_WARN("fail to parse tablet meta key", K(ret), K(ss_tablet_row));
  } else {
    MTL_SWITCH(tenant_id) {
      // Reuse the tablet meta value parser so minor shared-data macros are
      // collected from each sstable.
      if (OB_FAIL(ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
              allocator, ls_id, tablet_id, reorganize_scn, ss_tablet_row.meta_value_,
              true /*fill_occupy_size*/, block_infos))) {
        LOG_WARN("fail to collect blocks from tablet meta value", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  // Reset dedup set at tablet boundaries because tablet_iter rows are grouped
  // by tablet.
  if (OB_SUCC(ret) && tablet_id != last_tablet_id_) {
    if (!dedup_set_.created()) {
      if (OB_FAIL(dedup_set_.create(1024))) {
        LOG_WARN("fail to create dedup set", K(ret));
      }
    } else {
      dedup_set_.reuse();
    }
    if (OB_SUCC(ret)) {
      last_tablet_id_ = tablet_id;
    }
  }

  int64_t macro_block_count = 0;
  int64_t in_table_meta_block_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_infos.count(); ++i) {
    const ObTabletBlockInfo &block_info = block_infos.at(i);
    // TABLET_META only stores addresses for in-table sub meta; BR must copy the
    // referenced table-device objects before restore creates tablets.
    const bool is_in_table_meta_block = block_info.macro_id().is_shared_tablet_sub_meta_in_table();
    const int hash_ret = dedup_set_.exist_refactored(block_info.macro_id());
    if (OB_HASH_EXIST == hash_ret) {
      continue;
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("fail to check dedup set", K(ret), K(block_info));
    } else if (OB_FAIL(dedup_set_.set_refactored(block_info.macro_id()))) {
      LOG_WARN("fail to insert dedup set", K(ret), K(block_info));
    } else {
      ObSSHAMacroBlockInfo ha_macro_info;
      ha_macro_info.ls_id_ = ls_id;
      ha_macro_info.macro_block_id_ = block_info.macro_id();
      ha_macro_info.occupy_size_ = block_info.occupy_size();
      if (OB_FAIL(persister->add_macro_block(ha_macro_info))) {
        LOG_WARN("fail to add macro block", K(ret), K(ha_macro_info));
      } else {
        ++macro_block_count;
        if (is_in_table_meta_block) {
          ++in_table_meta_block_count;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("generate tasks by tablet", K(ls_id), K(tablet_id), K(macro_block_count), K(in_table_meta_block_count));
  }
  return ret;
}

void ObBackupMacroCopyTaskGenStrategy::free_ls_iter(ObIHASSLSMetaIter *&ls_iter)
{
  ObBackupSSLSMetaIterator *backup_ls_iter = static_cast<ObBackupSSLSMetaIterator *>(ls_iter);
  OB_DELETE(ObBackupSSLSMetaIterator, ObModIds::BACKUP, backup_ls_iter);
}

void ObBackupMacroCopyTaskGenStrategy::free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter)
{
  ObBackupSSTableMetatIterator *backup_tablet_iter = static_cast<ObBackupSSTableMetatIterator *>(tablet_iter);
  OB_DELETE(ObBackupSSTableMetatIterator, ObModIds::BACKUP, backup_tablet_iter);
}


//==================== ObRestoreMacroCopyTaskGenStrategy ====================

int ObRestoreMacroCopyTaskGenStrategy::alloc_and_init_ls_iter(
      const ObSSHAMacroCopyTaskGeneratorInitParam &param,
      ObIHASSLSMetaIter *&ls_iter)
{
  int ret = OB_SUCCESS;
  ObRestoreSSLSMetaIterator *tmp_iter = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);

  if (OB_NOT_NULL(ls_iter)) {
  ret = OB_INVALID_ARGUMENT;
  LOG_WARN("ls iter is not null", K(ret), KP(ls_iter));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObRestoreSSLSMetaIterator, attr))) {
  ret = OB_ALLOCATE_MEMORY_FAILED;
  LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_iter->init(param.tenant_id_, param.backup_set_dest_))) {
  LOG_WARN("fail to init restore iter", K(ret), K(param));
  } else {
  ls_iter = tmp_iter;
  tmp_iter = nullptr;
  }

  if (OB_NOT_NULL(tmp_iter)) {
  OB_DELETE(ObRestoreSSLSMetaIterator, ObModIds::RESTORE, tmp_iter);
  }
  return ret;
}


int ObRestoreMacroCopyTaskGenStrategy::alloc_and_init_tablet_iter(
      const ObSSHAMacroCopyTaskGeneratorInitParam &param,
      const ObLSID &ls_id,
      ObIHASSTabletMetaIter *&tablet_iter)
{
  int ret = OB_SUCCESS;
  ObRestoreSSTabletMetatIterator *tmp_iter = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);

  if (!ls_id.is_valid() || OB_NOT_NULL(tablet_iter)) {
  ret = OB_INVALID_ARGUMENT;
  LOG_WARN("in valid args", K(ret), K(ls_id), KP(tablet_iter));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObRestoreSSTabletMetatIterator, attr))) {
  ret = OB_ALLOCATE_MEMORY_FAILED;
  LOG_WARN("fail to alloc mem", KR(ret));
  } else if (OB_FAIL(tmp_iter->init(ls_id, param.tenant_id_, param.backup_set_dest_))) {
  LOG_WARN("fail to init restore tablet meta iter", K(ret), K(ls_id), K(param));
  } else {
  tablet_iter = tmp_iter;
  tmp_iter = nullptr;
  }

  if (OB_NOT_NULL(tmp_iter)) {
  OB_DELETE(ObRestoreSSTabletMetatIterator, ObModIds::RESTORE, tmp_iter);
  }
  return ret;
}

int ObRestoreMacroCopyTaskGenStrategy::generate_tasks_by_ls(
      const ObLSID &ls_id,
      const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
      ObSSHAMacroBlockBatchPersister *persister,
      ObSSRawMetaRow &ls_meta_row)
{
  int ret = OB_SUCCESS;
  const ObBackupDest &backup_set_dest = init_param.backup_set_dest_;
  ObBackupPath file_list_dir_path;
  backup::ObBackupBlockFileItemReader reader;
  ObStorageIdMod mod;
  mod.storage_id_ = 1;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  const uint64_t tenant_id = init_param.tenant_id_;
  if (!is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || !init_param.is_valid() || OB_ISNULL(persister)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(init_param), KP(persister));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest, ls_id, file_list_dir_path))) {
    LOG_WARN("fail to get ls backup dir path", K(ret), K(ls_id));
  } else if (OB_FAIL(reader.init(backup_set_dest.get_storage_info(), backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID,
      file_list_dir_path, ObBackupFileSuffix::BACKUP, mod))) {
    LOG_WARN("fail to init reader", K(ret), K(ls_id), K(file_list_dir_path));
  } else {
    backup::ObBackupBlockFileMacroIdItem item;
    while (OB_SUCC(ret)) {
      item.reset();
      if (OB_FAIL(reader.get_next_item(item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next item", K(ret), K(ls_id), K(file_list_dir_path));
        }
      } else {
        ObSSHAMacroBlockInfo ha_macro_info;
        ha_macro_info.ls_id_ = ls_id;
        ha_macro_info.macro_block_id_ = item.macro_id_;
        ha_macro_info.occupy_size_ = item.occupy_size_;
        if (FAILEDx(persister->add_macro_block(ha_macro_info))) {
          LOG_INFO("fail to add macro block", K(ret), K(ha_macro_info));
        }
      }
    }
  }
  if (FAILEDx(update_ls_meta_in_ss_meta_(tenant_id, ls_id, ls_meta_row))) {
    LOG_WARN("fail to update ls meta in ss meta", K(ret), K(ls_id));
  }
  return ret;
}

int ObRestoreMacroCopyTaskGenStrategy::update_ls_meta_in_ss_meta_(
    const uint64_t tenant_id, const ObLSID &ls_id, ObSSRawMetaRow &ls_meta_row)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 10_s;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      ObArenaAllocator allocator;
      ObSSMetaService *meta_srv = MTL(ObSSMetaService*);
      ObRestoreLSMetaFunctor functor(ObIMetaFunction::MetaFuncType::FUNC_TYPE_RESTORE, ls_id);
       if (OB_ISNULL(meta_srv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta srv is null", K(ret));
      } /*else if (OB_FAIL(meta_srv->modify_raw_meta_tenant_id(tenant_id, allocator, ls_meta_row))) {
        LOG_WARN("fail to modify raw meta row tenant_id", K(ret), K(tenant_id));
      } */else if (OB_FAIL(functor.ls_meta_row_.deep_copy(allocator, ls_meta_row))) {
        LOG_WARN("fail to deep copy ls meta row", K(ret), K(ls_id));
      } else if (OB_FAIL(meta_srv->update_ls_meta(ls_id,
                                                  ObMetaUpdateReason::RESTORE_UPDATE_LS_META,
                                                  functor,
                                                  false/*check lease*/))) {
        LOG_WARN("fail to update shared ls meta for restore v2", K(ret), K(ls_id));
      } else {
        LOG_INFO("update shared ls meta for restore succeed", K(ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObRestoreMacroCopyTaskGenStrategy::generate_tasks_by_tablet(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObArenaAllocator &allocator,
    ObSSHAMacroBlockBatchPersister *persister,
    ObSSRawMetaRow &ss_tablet_row)
{
  int ret = OB_SUCCESS;
  UNUSEDx(allocator, persister);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META) == ss_tablet_row.meta_type_) {
    if (OB_FAIL(insert_tablet_meta_into_ss_meta_(tenant_id, ls_id, ss_tablet_row))) {
      LOG_WARN("fail to insert meta row into ss meta", K(ret));
    }
  } else if (static_cast<int64_t>(ObSSMetaTypeEnum::META_CTRL) == ss_tablet_row.meta_type_) {
    if (OB_FAIL(insert_ss_meta_ctrl_row_(tenant_id, ls_id, ss_tablet_row))) {
      LOG_WARN("fail to insert ss meta ctrl row", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected restore tablet meta row type", K(ret), K(ss_tablet_row));
  }
  return ret;
}

int ObRestoreMacroCopyTaskGenStrategy::insert_tablet_meta_into_ss_meta_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObSSRawMetaRow &meta_row)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      ObSSMetaService *meta_srv = MTL(ObSSMetaService*);
      if (OB_ISNULL(meta_srv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta srv is null", K(ret));
      } 
      if (OB_SUCC(ret)) {
        // The backup row keeps source tenant_id/ls_id; rewrite both before
        // inserting into the target tenant shared meta table.
        meta_row.tenant_id_ = tenant_id;
        meta_row.ls_id_ = ls_id;
        if (OB_FAIL(meta_srv->insert_raw_meta(meta_row))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to insert raw meta v2", K(ret), K(meta_row));
          }
        } else {
          LOG_INFO("insert meta row v2 succeed", K(meta_row));
        }
      }
    }
  }
  return ret;
}

int ObRestoreMacroCopyTaskGenStrategy::insert_ss_meta_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObSSRawMetaRow &meta_row)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()
      || static_cast<int64_t>(ObSSMetaTypeEnum::META_CTRL) != meta_row.meta_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_row));
  } else {
    MTL_SWITCH(tenant_id) {
      int64_t pos = 0;
      int64_t affected_rows = 0;
      int64_t key_serialize_size = 0;
      char *canonical_key_buf = nullptr;
      ObString canonical_meta_key;
      ObSSMetaCtrlKey ctrl_key;
      ObSSMetaCtrlTableOperator table_operator;
      ObArenaAllocator allocator("RestoreCtrl");
      // The backup meta_key comes from external (untrusted) backup media and is
      // concatenated into SQL without escaping by insert_row. Re-serialize the
      // validated ctrl_key into a canonical relative object path so any tampered
      // or trailing bytes in the raw backup key cannot be injected.
      if (OB_FAIL(ctrl_key.deserialize_meta_key(meta_row.meta_key_.ptr(), meta_row.meta_key_.length(), pos))) {
        LOG_WARN("fail to deserialize ss meta ctrl key", K(ret), K(meta_row));
      } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, ctrl_key.type_, ctrl_key))) {
        LOG_WARN("restored ss meta ctrl key mismatch with target row",
            K(ret), K(ls_id), K(ctrl_key), K(meta_row));
      } else if (!should_backup_ss_ctrl_type(ctrl_key.type_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected non-backup ss meta ctrl type in backup",
            K(ret), K(ctrl_key), K(meta_row));
      } else if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(key_serialize_size))) {
        LOG_WARN("fail to get ss meta ctrl key serialize size", K(ret), K(ctrl_key));
      } else if (OB_ISNULL(canonical_key_buf = static_cast<char *>(allocator.alloc(key_serialize_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc canonical meta key buf", K(ret), K(key_serialize_size));
      } else if (OB_FALSE_IT(pos = 0)) {
      } else if (OB_FAIL(ctrl_key.serialize_meta_key(canonical_key_buf, key_serialize_size, pos))) {
        LOG_WARN("fail to serialize canonical ss meta ctrl key", K(ret), K(ctrl_key));
      } else if (OB_FALSE_IT(canonical_meta_key.assign_ptr(canonical_key_buf, static_cast<int32_t>(pos)))) {
      } else if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", K(ret));
      } else if (OB_FAIL(table_operator.init(*GCTX.sql_proxy_, 10_s))) {
        LOG_WARN("fail to init ss meta ctrl table operator", K(ret));
      } else if (OB_FAIL(table_operator.insert_row(tenant_id, ls_id, ctrl_key.type_,
                                                   canonical_meta_key, meta_row.meta_value_, affected_rows))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to insert ss meta ctrl row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
        }
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(ctrl_key));
      } else {
        LOG_INFO("insert ss meta ctrl row succeed", K(tenant_id), K(ls_id), K(ctrl_key));
      }
    }
  }
  return ret;
}

void ObRestoreMacroCopyTaskGenStrategy::free_ls_iter(ObIHASSLSMetaIter *&ls_iter)
{
  ObRestoreSSLSMetaIterator *restore_ls_iter = static_cast<ObRestoreSSLSMetaIterator *>(ls_iter);
  OB_DELETE(ObRestoreSSLSMetaIterator, ObModIds::RESTORE, restore_ls_iter);
}

void ObRestoreMacroCopyTaskGenStrategy::free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter)
{
  ObRestoreSSTabletMetatIterator *restore_tablet_iter = static_cast<ObRestoreSSTabletMetatIterator *>(tablet_iter);
  OB_DELETE(ObRestoreSSTabletMetatIterator, ObModIds::RESTORE, restore_tablet_iter);
}

// ==================== ObBackupCleanMacroCopyTaskGenStrategy ====================
int ObBackupCleanMacroCopyTaskGenStrategy::alloc_and_init_ls_iter(
    const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
    ObIHASSLSMetaIter *&ls_iter)
{
  UNUSED(init_param);
  UNUSED(ls_iter);
  ls_iter = nullptr;
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("alloc and init ls iter is not supported", K(ret), K(init_param), KP(ls_iter));
  return ret;
}

int ObBackupCleanMacroCopyTaskGenStrategy::alloc_and_init_tablet_iter(
    const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
    const ObLSID &ls_id,
    ObIHASSTabletMetaIter *&tablet_iter)
{
  UNUSED(init_param);
  UNUSED(ls_id);
  UNUSED(tablet_iter);
  tablet_iter = nullptr;
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("alloc and init tablet iter is not supported", K(ret), K(init_param), K(ls_id), KP(tablet_iter));
  return ret;
}

void ObBackupCleanMacroCopyTaskGenStrategy::free_ls_iter(ObIHASSLSMetaIter *&ls_iter)
{
  UNUSED(ls_iter);
  ls_iter = nullptr;
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("free ls iter is not supported", K(ret), KP(ls_iter));
  // Backup clean doesn't use iterator, do nothing
}

void ObBackupCleanMacroCopyTaskGenStrategy::free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter)
{
  UNUSED(tablet_iter);
  tablet_iter = nullptr;
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("free tablet iter is not supported", K(ret), KP(tablet_iter));
  // Backup clean doesn't use iterator, do nothing
}

ERRSIM_POINT_DEF(EN_SS_DELETE_MACRO_BLOCK_GEN_TASKS_FAILED);
int ObBackupCleanMacroCopyTaskGenStrategy::generate_tasks_by_ls(
    const ObLSID &ls_id,
    const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
    ObSSHAMacroBlockBatchPersister *persister,
    ObSSRawMetaRow &ls_meta_row)
{
  int ret = OB_SUCCESS;
  UNUSED(ls_meta_row);
  if (!ls_id.is_valid() || !init_param.is_valid() || OB_ISNULL(persister)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(init_param), KP(persister));
  } else {
    // Build macro block list file directory path
    share::ObBackupPath file_list_dir;
    const ObBackupDest &backup_set_dest = init_param.backup_set_dest_;
    const ObBackupStorageInfo *backup_info = backup_set_dest.get_storage_info();

    common::ObStorageIdMod storage_id_mod;
    storage_id_mod.storage_used_mod_ = common::ObStorageUsedMod::STORAGE_USED_BACKUP;
    share::ObSSHAMacroBlockTaskAttr task_attr;
    common::ObArray<backup::ObBackupBlockFileAddr> block_addr_list;

    if (OB_ISNULL(backup_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup storage info is null", K(ret), K(backup_set_dest));
    } else if (OB_FALSE_IT(storage_id_mod.storage_id_ = backup_info->get_dest_id())) {
    } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest, ls_id, file_list_dir))) {
      LOG_WARN("failed to get ls backup dir path", K(ret), K(ls_id));
    } else {
      backup::ObBackupBlockFileReaderParam param;
      param.storage_info_ = backup_info;
      param.data_type_ = backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID;
      param.file_list_dir_ = file_list_dir;
      param.suffix_ = share::ObBackupFileSuffix::BACKUP;
      param.storage_id_mod_ = storage_id_mod;
      if (OB_FAIL(backup::ObBackupBlockFileReaderUtil::get_all_block_addrs(param, block_addr_list))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("backup file list not exist, no macro blocks to delete for this ls", K(ls_id), K(file_list_dir));
        } else {
          LOG_WARN("failed to get all block addrs", K(ret), K(ls_id), K(file_list_dir));
        }
      }

      if (OB_SUCC(ret) && block_addr_list.count() > 0) {
        // Generate a task for each block address
        for (int64_t i = 0; OB_SUCC(ret) && i < block_addr_list.count(); ++i) {
          task_attr.reset();
          const backup::ObBackupBlockFileAddr &block_addr = block_addr_list.at(i);
          task_attr.tenant_id_ = init_param.tenant_id_;
          task_attr.parent_task_id_ = init_param.parent_task_id_;
          task_attr.task_type_ = init_param.task_type_;
          task_attr.ls_id_ = ls_id.id();
          task_attr.task_id_ = i + 1;  // Use index as macro task ID
          task_attr.status_ = ObBackupTaskStatus::INIT;
          task_attr.macro_id_list_.set_macro_list_addr(block_addr);
#ifdef ERRSIM
          if (OB_FAIL(EN_SS_DELETE_MACRO_BLOCK_GEN_TASKS_FAILED)) {
            LOG_WARN("error injected for delete macro block gen tasks failed", K(ret), K(ls_id), K(task_attr));
          }
#endif
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(persister->add_task_attr(task_attr))) {
            LOG_WARN("failed to add task attr via persister", K(ret), K(ls_id), K(task_attr));
          }
          DEBUG_SYNC(AFTER_ADD_ONE_MACRO_DELETE_TASK);
        }
      }
    }
  }
  return ret;
}

int ObBackupCleanMacroCopyTaskGenStrategy::generate_tasks_by_tablet(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObArenaAllocator &allocator,
    ObSSHAMacroBlockBatchPersister *persister,
    ObSSRawMetaRow &ss_tablet_row)
{
  UNUSED(tenant_id);
  UNUSED(ls_id);
  UNUSED(allocator);
  UNUSED(persister);
  UNUSED(ss_tablet_row);
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("generate tasks by tablet is not supported", K(ret), K(tenant_id), K(ls_id), K(allocator), KP(persister), K(ss_tablet_row));
  return ret;
}

OB_SERIALIZE_MEMBER_INHERIT(ObRestoreLSMetaFunctor, ObIMetaFunction, ls_id_, ls_meta_row_);

int ObRestoreLSMetaFunctor::operator() (ObSSLSMeta &ss_ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool updated = false;
  typedef ObDefaultSSMetaValue<ObSSLSMeta> ObSSLSMetaValue;
  SMART_VAR(ObSSLSMetaValue, tmp) {
    if (OB_FAIL(tmp.deserialize(ls_meta_row_.meta_value_.ptr(),
                               ls_meta_row_.meta_value_.length(),
                               pos))) {
      LOG_WARN("fail to deserialize ss ls meta", K(ret), K_(ls_meta_row));
    } else if (OB_FAIL(ss_ls_meta.update_for_ss(tmp.meta_value_, updated))) {
      LOG_WARN("fail to update ss ls meta", K(ret));
    } else {
      LOG_INFO("get new ss ls meta", "old", tmp.meta_value_, "new", ss_ls_meta);
    // } else if (!updated) {
      // TODO(mingqiao-ss): decide this is valid or not
    }
  }
  return ret;
}

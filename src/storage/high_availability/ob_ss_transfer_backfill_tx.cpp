/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/mds_table/ob_tablet_mds_reloc_helper.h"
#include "ob_ss_transfer_backfill_tx.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "ob_ss_transfer_backfill_dag_net.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase {
namespace storage
{
ERRSIM_POINT_DEF(EN_SKIP_SS_TRANSFER_BACKFILL_TX);

ObSSTransferBackfillTaskPair::ObSSTransferBackfillTaskPair()
  : backfill_scn_(),
    task_id_()
{
}

void ObSSTransferBackfillTaskPair::reset()
{
  backfill_scn_.reset();
  task_id_.reset();
}

bool ObSSTransferBackfillTaskPair::is_valid() const
{
  return backfill_scn_.is_valid()
      && task_id_.is_valid();
}

ObSSTransferWorkerMgr::ObSSTransferWorkerMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_(nullptr),
    ls_id_(),
    upload_mgr_(),
    backfill_mgr_(),
    replace_mgr_(),
    refresh_mgr_(),
    task_pair_()
{
}

ObSSTransferWorkerMgr::~ObSSTransferWorkerMgr()
{
}

int ObSSTransferWorkerMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss transfer worker mgr init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss transfer worker mgr get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(upload_mgr_.init(ls))) {
    LOG_WARN("failed to init upload mgr", K(ret), KPC(ls));
  } else if (OB_FAIL(backfill_mgr_.init(ls))) {
    LOG_WARN("failed to init backfill mgr", K(ret), KPC(ls));
  } else if (OB_FAIL(replace_mgr_.init(ls))) {
    LOG_WARN("failed to init replace mgr", K(ret), KPC(ls));
  } else if (OB_FAIL(refresh_mgr_.init(ls))) {
    LOG_WARN("failed to init refresh mgr", K(ret), KPC(ls));
  } else {
    tenant_id_ = MTL_ID();
    ls_ = ls;
    ls_id_ = ls_->get_ls_id();
    is_inited_ = true;
  }
  return ret;
}

int ObSSTransferWorkerMgr::process()
{
  int ret = OB_SUCCESS;
  common::ObArray<ObTabletBackfillInfo> tablet_info_array;
  bool is_errsim_server = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(ObStorageHAUtils::is_errsim_transfer_server(is_errsim_server))) {
    LOG_WARN("failed to check is errsim server", K(ret));
  } else if (is_errsim_server && OB_FAIL(EN_SKIP_SS_TRANSFER_BACKFILL_TX)) {
    LOG_INFO("[ERRSIM] skip ss transfer backfill tx", K(ret));
#endif
  } else if (OB_FAIL(get_transfer_info_(tablet_info_array))) {
    LOG_WARN("failed to get transfer info", K(ret), K(ls_id_));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(schedule_ss_backfill_(tablet_info_array))) {
    LOG_WARN("failed to schedule ss backfill", K(ret), K(ls_id_), K(tablet_info_array));
  }
  return ret;
}

int ObSSTransferWorkerMgr::get_transfer_info_(
    common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  tablet_info_array.reset();
  common::ObArray<ObTabletBackfillInfo> all_tablet_info_array;
  bool allow_read = false;

  DEBUG_SYNC(TRANSFER_GET_BACKFILL_TABLETS_BEFORE);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (OB_FAIL(ls_->check_allow_read(allow_read))) {
    LOG_WARN("failed to check ls allow read", K(ret), KPC(ls_));
  } 
  // if ls is not allow read, get transfer info by iterating local tablet meta
  else if (!allow_read) {
    if (OB_FAIL(get_transfer_info_from_local_tablet_(all_tablet_info_array))) {
      LOG_WARN("failed to get transfer info from local tablet", K(ret), K(ls_id_));
    }
  } 
  // otherwise, get transfer info by reading reorg table
  else if (OB_FAIL(get_transfer_info_from_reorg_table_(all_tablet_info_array))) {
    LOG_WARN("failed to get transfer info from reorg table", K(ret), K(ls_id_));
  }

  if (OB_SUCC(ret)) {
    if (all_tablet_info_array.empty()) {
      //do nothing
    } else {
      ObBackfillScnCompare comp(ret);
      lib::ob_sort(all_tablet_info_array.begin(), all_tablet_info_array.end(), comp);
      if (OB_FAIL(ret)) {
        LOG_ERROR("failed to sort info array", K(ret), K(tablet_info_array));
      } else if (OB_FAIL(filter_transfer_info_(all_tablet_info_array, tablet_info_array))) {
        LOG_WARN("failed to filter transfer info", K(ret), K(tablet_info_array));
      }
    }
  }

  return ret;
}

int ObSSTransferWorkerMgr::get_transfer_info_from_local_tablet_(
    common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  int64_t start_time = common::ObTimeUtility::current_time();
  tablet_info_array.reset();
  // for ss transfer, we only read committed tablet status
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_ALL_COMMITED); 

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret)); 
  } else if (OB_FAIL(ls_->build_tablet_iter(tablet_iter, true /*except_ls_inner_tablet*/))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), KPC(ls_));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletCreateDeleteMdsUserData user_data;
    ObTabletBackfillInfo backfill_info;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      user_data.reset();
      backfill_info.reset();
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), KPC(ls_));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (tablet->get_tablet_id().is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not iterate ls inner tablet", K(ret), KPC(tablet));
      } else if (OB_FAIL(tablet->get_latest_committed_tablet_status(user_data))) {
        if (OB_EMPTY_RESULT == ret) {
          LOG_INFO("tablet_status does not exist", K(ret), "tablet_id", tablet->get_tablet_id());
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get latest commited tablet status", K(ret), KPC(tablet));
        }
      }
      // tablet status may be transfer out deleted but still need to backfill when ss restore
      else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
              && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_
              && !tablet->get_tablet_meta().has_transfer_table()) {
        // do nothing, not transfer tablet
      } else if (OB_FAIL(get_backfill_info_for_transfer_tablet_(tablet, user_data, backfill_info))) {
        LOG_WARN("failed to get backfill info for transfer tablet", K(ret), KPC(tablet), K(user_data));
      } else if (OB_FAIL(tablet_info_array.push_back(backfill_info))) {
        LOG_WARN("failed to push back tablet info", K(ret), K(backfill_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t cost_time = common::ObTimeUtility::current_time() - start_time;
    LOG_INFO("statistics the time that get transfer tablets from local tablet", K(ls_id_), K(cost_time));
  }

  return ret;
} 

int ObSSTransferWorkerMgr::get_backfill_info_for_transfer_tablet_(
    const ObTablet *tablet, 
    const ObTabletCreateDeleteMdsUserData &user_data, 
    ObTabletBackfillInfo &backfill_info) 
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tablet) || !user_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backfill info for transfer tablet get invalid argument", K(ret), KP(tablet), K(user_data));
  } else {
    const ObTabletTransferInfo &transfer_info = tablet->get_tablet_meta().transfer_info_;

    backfill_info.tablet_id_ = tablet->get_tablet_id();
    backfill_info.is_committed_ = true;
    backfill_info.is_shared_storage_ = true;
    backfill_info.reorganization_scn_ = tablet->get_reorganization_scn();
    backfill_info.transfer_seq_ = tablet->get_transfer_seq();
    backfill_info.tablet_status_ = user_data.tablet_status_;

    if (ObTabletStatus::TRANSFER_OUT == user_data.tablet_status_
     || ObTabletStatus::TRANSFER_OUT_DELETED == user_data.tablet_status_) {
      backfill_info.backfill_scn_ = user_data.transfer_scn_;
      backfill_info.relative_ls_id_ = user_data.transfer_ls_id_;
      backfill_info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_OUT;
      // no need to set src_reorganization_scn_ for transfer out tablet
    } else if (transfer_info.has_transfer_table()) {
      // for transfer in tablet, cannot get transfer_scn_ from user_data, because new data may overwrite the user_data 
      backfill_info.backfill_scn_ = transfer_info.transfer_start_scn_; 
      backfill_info.relative_ls_id_ = transfer_info.ls_id_;
      backfill_info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_IN;
      backfill_info.src_reorganization_scn_ = transfer_info.src_reorganization_scn_;
    } else {
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_WARN("invalid tablet status", K(ret), K(backfill_info), KPC(tablet), K(user_data));
    }

    if (OB_FAIL(ret)) {
    } else if (!backfill_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backfill info is invalid", K(ret), K(backfill_info), KPC(tablet), K(user_data));
    }
  } 

  return ret;
}

int ObSSTransferWorkerMgr::get_transfer_info_from_reorg_table_(
    common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  ObTransferInfoIterator iter;
  const ObTabletReorgInfoDataType::TYPE start_type = ObTabletReorgInfoDataType::TRANSFER_IN;
  const ObTabletReorgInfoDataType::TYPE end_type = ObTabletReorgInfoDataType::TRANSFER_OUT;
  ObTabletReorgInfoDataKey key;
  ObTransferDataValue transfer_value;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  bool is_ready = false;
  ObTabletBackfillInfo tablet_info;
  tablet_info_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (OB_FAIL(iter.init(tenant_id_, ls_id_, start_type, end_type))) {
    LOG_WARN("failed to init transfer info iterator", K(ret), K(tenant_id_), K(ls_id_));
  } else {
    while (OB_SUCC(ret)) {
      key.reset();
      transfer_value.reset();
      user_data.reset();
      is_ready = false;
      tablet_info.reset();
      if (OB_FAIL(iter.get_next(key, transfer_value))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get transfer info", K(ret), K(tenant_id_), K(ls_id_));
        }
      } else if (!ObTabletReorgInfoDataType::is_transfer(key.type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("member table data type is unexpected", K(ret), K(key));
      } else if (OB_FAIL(ls_->ha_get_tablet(key.tablet_id_, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          // tablet not exist, already ready
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key), K(transfer_value));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(key), K(transfer_value), K(tablet_handle));
      } else if (OB_FAIL(tablet->get_latest_committed_tablet_status(user_data))) {
        LOG_WARN("failed to get latest committed tablet status", K(ret), K(key), K(transfer_value));
      } else if (OB_FAIL(check_transfer_tablet_ready_(key, transfer_value, tablet, user_data, is_ready))) {
        LOG_WARN("failed to check transfer tablet ready", K(ret), K(key), K(transfer_value), KPC(tablet), K(user_data));
      } else if (is_ready) {
        //do nothing
      } else {
        tablet_info.tablet_id_ = key.tablet_id_;
        tablet_info.reorganization_scn_ = key.reorganization_scn_;
        tablet_info.relative_ls_id_ = transfer_value.relative_ls_id_;
        tablet_info.backfill_scn_ = transfer_value.transfer_scn_;
        tablet_info.src_reorganization_scn_ = transfer_value.src_reorganization_scn_;
        tablet_info.transfer_seq_ = transfer_value.transfer_seq_;
        tablet_info.is_committed_ = true;
        tablet_info.is_shared_storage_ = true;
        tablet_info.tablet_status_ = user_data.tablet_status_;
        // convert ObTabletStatus to ObTabletBackfillType
        if (ObTabletStatus::TRANSFER_OUT == transfer_value.tablet_status_) {
          tablet_info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_OUT;
        } else if (ObTabletStatus::TRANSFER_IN == transfer_value.tablet_status_) {
          tablet_info.backfill_type_ = ObTabletBackfillType::BACKFILL_TRANSFER_IN;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet status", K(ret), K(tablet_info), K(key), K(transfer_value), K(user_data));
        }

        if (OB_FAIL(ret)) {
        } else if (!tablet_info.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tablet info is invalid", K(ret), K(tablet_info));
        } else if (OB_FAIL(tablet_info_array.push_back(tablet_info))) {
          LOG_WARN("failed to push back tablet info", K(ret), K(tablet_info), K(key), K(transfer_value));
        }
      }
    }
  }
  return ret;
}

int ObSSTransferWorkerMgr::check_transfer_tablet_ready_(
    const ObTabletReorgInfoDataKey &key,
    const ObTransferDataValue &transfer_value,
    const ObTablet *tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (!key.is_valid() || !transfer_value.is_valid() || OB_ISNULL(tablet) || !user_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer tablet ready get invalid argument", K(ret), K(key), K(transfer_value), KP(tablet), K(user_data));
  } else if (transfer_value.transfer_scn_ < user_data.transfer_scn_) {
    is_ready = true;
  } else if (ObTabletReorgInfoDataType::TRANSFER_IN == key.type_) {
    if (!tablet->get_tablet_meta().has_transfer_table()) {
      is_ready = true;
    } else {
      is_ready = false;
    }
  } else {
    // ObMemberTableDataType::TRANSFER_OUT == key.type_
    // tablet status may be transfer out deleted but still need to backfill when ss restore
    is_ready = false;
  }
  return ret;
}

int ObSSTransferWorkerMgr::filter_transfer_info_(
    const common::ObIArray<ObTabletBackfillInfo> &all_tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_tablet_info)
{
  int ret = OB_SUCCESS;
  need_tablet_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = all_tablet_info_array.at(i);
      if (need_tablet_info.empty()) {
        bool need_to_push = true;
        if (ObTabletStatus::TRANSFER_OUT_DELETED == tablet_info.tablet_status_) {
          if (OB_FAIL(check_transfer_out_deleted_tablet_need_backfill_(tablet_info, need_to_push))) {
            LOG_WARN("failed to check transfer out deleted tablet need backfill", K(ret), K(tablet_info));
          } 
        }

        if (OB_FAIL(ret)) {
        } else if (need_to_push && OB_FAIL(need_tablet_info.push_back(tablet_info))) {
          LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
        }
      } else {
        const ObTabletBackfillInfo &first_tablet_info = need_tablet_info.at(0);
        if (tablet_info.backfill_scn_ < first_tablet_info.backfill_scn_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet info backfill scn is smaller than first tablet info, unexpected", K(ret),
              K(tablet_info), K(first_tablet_info));
        } else if (tablet_info.backfill_scn_ > first_tablet_info.backfill_scn_) {
          break;
        } else if (tablet_info.backfill_type_ != first_tablet_info.backfill_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("backfill type is not same, unexpected", K(ret), K(tablet_info), K(first_tablet_info));
        } else if (OB_FAIL(need_tablet_info.push_back(tablet_info))) {
          LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
        }
      }
    }
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_ss_backfill_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule ss backfill get invalid argument", K(ret), K(tablet_info_array));
  } else {
    const ObTabletBackfillInfo &first_tablet_info = tablet_info_array.at(0);
    (void)try_update_task_pair_(first_tablet_info);

    switch (first_tablet_info.backfill_type_) {
      case ObTabletBackfillType::BACKFILL_TRANSFER_OUT: {
        if (OB_FAIL(schedule_transfer_out_backfill_(tablet_info_array))) {
          LOG_WARN("failed to schedule transfer out backfill", K(ret));
        }
        break;
      }
      case ObTabletBackfillType::BACKFILL_TRANSFER_IN: {
        if (OB_FAIL(schedule_transfer_in_replace_(tablet_info_array))) {
          LOG_WARN("failed to schedule transfer in replace", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid cur status for fail", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_out_backfill_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> upload_tablet_info_array;
  ObArray<ObTabletBackfillInfo> backfill_tablet_info_array;

  DEBUG_SYNC(TRANSFER_BACKFILL_TX_BEFORE);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (OB_FAIL(classify_transfer_out_info_(tablet_info_array, upload_tablet_info_array, backfill_tablet_info_array))) {
    LOG_WARN("failed to classify transfer out info", K(ret));
  } else if (OB_FAIL(schedule_transfer_out_tablet_upload_(upload_tablet_info_array))) {
    LOG_WARN("failed to schedule transfer out tablet upload", K(ret), K(ls_id_));
  } else if (OB_FAIL(schedule_transfer_out_tablet_backfill_(backfill_tablet_info_array))) {
    LOG_WARN("failed to schedule transfer out tablet backfill", K(ret), K(ls_id_));
  } 
  return ret;
}

int ObSSTransferWorkerMgr::classify_transfer_out_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &upload_tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &backfill_tablet_info_array)
{
  int ret = OB_SUCCESS;
  upload_tablet_info_array.reset();
  backfill_tablet_info_array.reset();
  const ObSSWriterType freeze_type = ObSSWriterType::FREEZE;
  const ObSSWriterType compaction_type = ObSSWriterType::COMPACTION;
  bool is_sswriter = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      if (OB_FAIL(ObSSTransferBackfillUtils::check_self_is_sswriter(freeze_type, ls_id_, tablet_info.tablet_id_, is_sswriter))) {
        LOG_WARN("failed to check self is sswriter", K(ret), K(ls_id_), K(tablet_info));
      } else if (is_sswriter && OB_FAIL(upload_tablet_info_array.push_back(tablet_info))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      } else if (OB_FAIL(ObSSTransferBackfillUtils::check_self_is_sswriter(compaction_type, ls_id_, tablet_info.tablet_id_, is_sswriter))) {
        LOG_WARN("failed to check self is sswriter", K(ret), K(ls_id_), K(tablet_info));
      } else if (is_sswriter && OB_FAIL(backfill_tablet_info_array.push_back(tablet_info))) {
        LOG_WARN("failed to push backfill tablet into array", K(ret), K(tablet_info));
      }
    }
  }

  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_out_tablet_upload_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(upload_mgr_.process(tablet_info_array, task_pair_.task_id_))) {
    LOG_WARN("failed to do upload process", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_out_tablet_backfill_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(backfill_mgr_.process(tablet_info_array, task_pair_.task_id_))) {
    LOG_WARN("failed to do backfill process", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_in_replace_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> ready_tablet_info_array;
  ObArray<ObTabletBackfillInfo> replace_tablet_info_array;

  // Pause before either path installs SSTables on the committed transfer-in tablets.
  DEBUG_SYNC(BEFORE_SS_TRANSFER_IN_REPLACE_AND_REFRESH);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (OB_FAIL(filter_mds_reloc_done_tablets_(tablet_info_array, ready_tablet_info_array))) {
    LOG_WARN("failed to filter tablets whose mds relocation is done", K(ret), K(ls_id_));
  } else if (OB_FAIL(split_transfer_in_info_(ready_tablet_info_array, replace_tablet_info_array))) {
    LOG_WARN("failed to split transfer in info", K(ret));
  } else if (OB_FAIL(schedule_transfer_in_tablet_replace_(replace_tablet_info_array))) {
    LOG_WARN("failed to schedule transfer in tablet replace", K(ret), K(ls_id_));
  } else if (OB_FAIL(schedule_transfer_in_tablet_refresh_(ready_tablet_info_array))) {
    LOG_WARN("failed to schedule transfer in tablet refresh", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::split_transfer_in_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &replace_tablet_info_array)
{
  int ret = OB_SUCCESS;
  replace_tablet_info_array.reset();
  const ObSSWriterType compaction_type = ObSSWriterType::COMPACTION;
  bool is_sswriter = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      if (OB_FAIL(ObSSTransferBackfillUtils::check_self_is_sswriter(compaction_type, ls_id_, tablet_info.tablet_id_, is_sswriter))) {
        LOG_WARN("failed to check self is sswriter", K(ret), K(ls_id_), K(tablet_info));
      } else if (is_sswriter && OB_FAIL(replace_tablet_info_array.push_back(tablet_info))) {
        LOG_WARN("failed to push replace tablet into array", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_in_tablet_replace_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(replace_mgr_.process(tablet_info_array, task_pair_.task_id_))) {
    LOG_WARN("failed to do replace mgr process", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::schedule_transfer_in_tablet_refresh_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(refresh_mgr_.process(tablet_info_array, task_pair_.task_id_))) {
    LOG_WARN("failed to do refresh process", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::cancel_dag_net()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (OB_FAIL(upload_mgr_.cancel_dag_net())) {
    LOG_WARN("failed to cancel upload dag net", K(ret), K(ls_id_));
  } else if (OB_FAIL(backfill_mgr_.cancel_dag_net())) {
    LOG_WARN("failed to cancel backfill dag net", K(ret), K(ls_id_));
  } else if (OB_FAIL(replace_mgr_.cancel_dag_net())) {
    LOG_WARN("failed to cancel replace dag net", K(ret), K(ls_id_));
  } else if (OB_FAIL(refresh_mgr_.cancel_dag_net())) {
    LOG_WARN("failed to cancel refresh dag net", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTransferWorkerMgr::try_update_task_pair_(const ObTabletBackfillInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (task_pair_.backfill_scn_ == tablet_info.backfill_scn_) {
    //do nothing
  } else {
    task_pair_.task_id_.reset();
    task_pair_.task_id_.init(GCONF.self_addr_);
    task_pair_.backfill_scn_ = tablet_info.backfill_scn_;
    SERVER_EVENT_ADD("storage_ha", "start_ss_transfer_backfill",
        "tenant_id", tenant_id_,
        "ls_id", ls_id_.id(),
        "backfill_scn", tablet_info.backfill_scn_,
        "backfill_type", tablet_info.backfill_type_,
        "task_id", task_pair_.task_id_);
  }
  return ret;
}

int ObSSTransferWorkerMgr::check_transfer_out_deleted_tablet_need_backfill_(
    const ObTabletBackfillInfo &tablet_info,
    bool &need_backfill)
{
  int ret = OB_SUCCESS;
  need_backfill = true;
  bool is_tablet_exist = false;
  bool is_data_uploaded = false;
  bool is_data_backfilled = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (!tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet info is invalid", K(ret), K(tablet_info));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_ss_src_tablet_ready(ls_id_, tablet_info.tablet_id_, 
      tablet_info.reorganization_scn_, tablet_info.backfill_scn_, is_tablet_exist, is_data_uploaded, is_data_backfilled))) {
    LOG_WARN("failed to check ss src tablet ready", K(ret), K(tablet_info));
  } else if (!is_tablet_exist) {
    // ss tablet not exist, need to check if local tablet is gc
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    share::SCN max_decided_scn;

    if (OB_FAIL(ls_->ha_get_tablet(tablet_info.tablet_id_, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        // local tablet not exist, no need to backfill
        need_backfill = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get local tablet", K(ret), K(tablet_info));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info));
    } else if (OB_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("failed to get max decided scn", K(ret), KPC(ls_));
    } else if (max_decided_scn <= tablet_info.backfill_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls max decided scn is smaller than transfer scn, unexpected", K(ret), K(max_decided_scn), 
        "transfer_scn", tablet_info.backfill_scn_, K(tablet_info), KPC(ls_));
    } else {
      share::SCN reorganization_scn = tablet_info.reorganization_scn_;
      share::SCN local_reorganization_scn = tablet->get_reorganization_scn();
      if (local_reorganization_scn < reorganization_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local tablet is older than tablet info in reorg table, unexpected", K(ret),
          K(reorganization_scn), K(local_reorganization_scn), K(tablet_info), KPC(tablet));
      } else if (local_reorganization_scn > reorganization_scn) {
        // local tablet is newer than tablet info in reorg table, old tablet has been gc in shared storage, no need to backfill 
        need_backfill = false;
      } else { // local_reorganization_scn == reorganization_scn
        if (tablet->is_empty_shell()) {
          // local tablet is empty shell, which means the ss tablet has been gc, no need to backfill
          need_backfill = false;
        } else {
          need_backfill = true;
        }
      }
    }
  } else if (is_data_uploaded && is_data_backfilled) {
    // TRANSFER_OUT_DELETED 与 dest 变为 NORMAL 由同一 finish 事务提交，
    // 此处 reloc done marker 不再表示仍有 pending 工作。
    need_backfill = false;
  } else {
    need_backfill = true;
  }

  LOG_INFO("check transfer out deleted tablet need backfill", K(ret), K(need_backfill), 
      K(is_tablet_exist), K(is_data_uploaded), K(is_data_backfilled), K(tablet_info));
  return ret;
}

ObSSTransferBasicMgr::ObSSTransferBasicMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_(nullptr),
    ls_id_(),
    task_id_()
{
}

ObSSTransferBasicMgr::~ObSSTransferBasicMgr()
{
}

int ObSSTransferBasicMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss transfer basic mgr init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss transfer upload mgr get invalid argument", K(ret), KP(ls));
  } else {
    tenant_id_ = MTL_ID();
    ls_ = ls;
    ls_id_ = ls_->get_ls_id();
    is_inited_ = true;
  }
  return ret;
}

int ObSSTransferBasicMgr::cancel_dag_net()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  bool is_exist = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer basic mgr do not init", K(ret));
  } else if (task_id_.is_invalid()) {
    // do nothing
  } else if (OB_FAIL(check_dag_net_exist_(is_exist))) {
    LOG_WARN("fail to check task exist", K(ret), K_(task_id));
  } else if (is_exist) {
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get ObTenantDagScheduler from MTL", K(ret));
    } else if (OB_FAIL(scheduler->cancel_dag_net(task_id_))) {
      LOG_WARN("failed to cancel dag net", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      int64_t start_ts = ObTimeUtil::current_time();
      do {
        if (OB_FAIL(check_dag_net_exist_(is_exist))) {
          LOG_WARN("fail to check task exist", K(ret), K_(task_id));
        } else if (is_exist) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            ret = OB_EAGAIN;
            LOG_WARN("cancel dag task cost too much time", K(ret), K_(task_id),
                "cost_time", ObTimeUtil::current_time() - start_ts);
          }
          if (OB_SUCC(ret)) {
            ob_usleep(100 * 1000); //100ms
          }
        }
      } while (is_exist && OB_SUCC(ret));
    }
  }

  return ret;
}

int ObSSTransferBasicMgr::check_dag_net_exist_(
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  share::ObTenantDagScheduler *scheduler = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer basic mgr do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->check_dag_net_exist(task_id_, is_exist, INT64_MAX/*abs_timeout_us*/))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(task_id_));
  }
  return ret;
}

int ObSSTransferBasicMgr::schedule_dag_net_(const ObSSTransferBackfillTXParam &param)
{
  int ret = OB_SUCCESS;
  share::ObTenantDagScheduler *scheduler = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer basic mgr do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->create_and_add_dag_net<ObSSTransferBackfillDagNet>(&param))) {
      LOG_WARN("failed to create and add dag net", K(ret), K(param));
    } else {
      LOG_INFO("[SS_TRANSFER_BACKFILL] success create ss transfer backfill dag net", K(ret), K(param));
    }
  }
  return ret;
}

void ObSSTransferBasicMgr::reset_task_id_()
{
  task_id_.reset();
}

void ObSSTransferBasicMgr::update_task_id_()
{
  reset_task_id_();
  task_id_.init(GCONF.self_addr_);
}

int ObSSTransferBasicMgr::build_dag_net_param_(
    const ObSSTransferBackfillType &backfill_type,
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    const share::ObTaskId &task_id,
    ObSSTransferBackfillTXParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer basic mgr do not init", K(ret));
  } else if (ObSSTransferBackfillType::BACKFILL_MAX == backfill_type
      || !task_id.is_valid() || tablet_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build dag net param get invalid argument", K(ret), K(backfill_type), K(task_id), K(tablet_info_array));
  } else {
    ObTabletBackfillInfo tablet_info;
    update_task_id_();
    if (FALSE_IT(tablet_info = tablet_info_array.at(0))) {
    } else if (OB_FAIL(param.tablet_infos_.assign(tablet_info_array))) {
      LOG_WARN("failed to assign tablet infos", K(ret), K(tablet_info_array));
    } else {
      switch (backfill_type) {
        case ObSSTransferBackfillType::BACKFILL_UPLOAD:
        case ObSSTransferBackfillType::BACKFILL_TX:
          param.src_ls_id_ = ls_id_;
          param.dest_ls_id_ = tablet_info.relative_ls_id_;
          break;
        case ObSSTransferBackfillType::BACKFILL_REPLACE_TABLE:
        case ObSSTransferBackfillType::BACKFILL_REFRESH:
          param.src_ls_id_ = tablet_info.relative_ls_id_;
          param.dest_ls_id_ = ls_id_;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid backfill type", K(ret), K(backfill_type));
          break;
      }

      if (OB_SUCC(ret)) {
        param.tenant_id_ = tenant_id_;
        param.task_id_ = task_id;
        param.backfill_type_ = backfill_type;
        param.dag_net_id_ = task_id_;
      }
    }
  }
  return ret;
}

int ObSSTransferBasicMgr::check_local_tablet_match_(
  const ObTabletBackfillInfo &tablet_info,
  bool &is_match) 
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  is_match = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer basic mgr do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_info.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_match = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
    }
  } else {
    if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info));
    } else if (tablet->get_reorganization_scn() == tablet_info.reorganization_scn_) {
      is_match = true;
    } else if (tablet->get_reorganization_scn() < tablet_info.reorganization_scn_) {
      ret = OB_ERR_UNEXPECTED;
      is_match = false;
      LOG_WARN("local tablet reorganization scn is smaller than tablet info, unexpected", K(ret), K(tablet_info),
        "tablet_meta", tablet->get_tablet_meta());
    } else {
      is_match = false;
      LOG_INFO("local tablet is not match with tablet info", K(ret), K(tablet_info),
        "tablet_meta", tablet->get_tablet_meta());
    }
  }
  return ret;
}

ObSSTransferUploadMgr::ObSSTransferUploadMgr()
  : ObSSTransferBasicMgr()
{
}

ObSSTransferUploadMgr::~ObSSTransferUploadMgr()
{
}

int ObSSTransferUploadMgr::process(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> need_upload_tablet_infos;
  bool is_exist = false;
  ObSSTransferBackfillTXParam param;
  const ObSSTransferBackfillType type(ObSSTransferBackfillType::BACKFILL_UPLOAD);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer upload mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(check_dag_net_exist_(is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(ls_id_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(get_need_upload_tablet_info_(tablet_info_array, need_upload_tablet_infos))) {
    LOG_WARN("failed to get need upload tablet info", K(ret), K(ls_id_));
  } else if (need_upload_tablet_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(build_dag_net_param_(type, need_upload_tablet_infos, task_id, param))) {
    LOG_WARN("failed to build dag net param", K(ret), K(type), K(need_upload_tablet_infos));
  } else if (OB_FAIL(schedule_dag_net_(param))) {
    LOG_WARN("failed to schedule dag net", K(ret), K(param));
  }
  LOG_INFO("finish do ss upload mgr process", K(ret), "tablet count", tablet_info_array.count(),
      "need upload count", need_upload_tablet_infos.count(), "cost", ObTimeUtil::current_time() - start_ts, K(is_exist));

  return ret;
}

int ObSSTransferUploadMgr::get_need_upload_tablet_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_upload_tablet_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer upload mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      ObArenaAllocator allocator;
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      bool need_upload = true;
      bool is_match = false;
      if (OB_FAIL(check_local_tablet_match_(tablet_info, is_match))) {
        LOG_WARN("failed to check local tablet match", K(ret), K(tablet_info));
      } else if (!is_match) {
        need_upload = false;
      } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info.tablet_id_,
          tablet_info.reorganization_scn_, allocator, tablet_handle, true /*read_mark_delete*/))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          //overwrite ret
          need_upload = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info));
      } else if (tablet->get_tablet_meta().transfer_info_.is_transfer_upload_completed()) {
        need_upload = false;
      }
      if (OB_FAIL(ret)) {
      } else if (!need_upload) {
        //do nothing
        LOG_INFO("tablet does not need upload", K(ret), K(tablet_info));
      } else if (OB_FAIL(need_upload_tablet_infos.push_back(tablet_info))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}


ObSSTransferBackfillTXMgr::ObSSTransferBackfillTXMgr()
  : ObSSTransferBasicMgr()
{
}

ObSSTransferBackfillTXMgr::~ObSSTransferBackfillTXMgr()
{
}

int ObSSTransferBackfillTXMgr::process(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> need_backfill_tablet_infos;
  bool is_exist = false;
  ObSSTransferBackfillTXParam param;
  const ObSSTransferBackfillType type(ObSSTransferBackfillType::BACKFILL_TX);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer backfill tx mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(check_dag_net_exist_(is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(ls_id_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(get_need_backfill_tablet_info_(tablet_info_array, need_backfill_tablet_infos))) {
    LOG_WARN("failed to get need backfill tablet info", K(ret), K(ls_id_));
  } else if (need_backfill_tablet_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(build_dag_net_param_(type, need_backfill_tablet_infos, task_id, param))) {
    LOG_WARN("failed to build dag net param", K(ret), K(type), K(need_backfill_tablet_infos));
  } else if (OB_FAIL(schedule_dag_net_(param))) {
    LOG_WARN("failed to schedule dag net", K(ret), K(param));
  }

  LOG_INFO("finish do ss backfill mgr process", K(ret), "tablet count", tablet_info_array.count(),
      "need backfill count", need_backfill_tablet_infos.count(), "cost", ObTimeUtil::current_time() - start_ts, K(is_exist));

  return ret;
}

int ObSSTransferBackfillTXMgr::get_need_backfill_tablet_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_backfill_tablet_infos)
{
  int ret = OB_SUCCESS;
  need_backfill_tablet_infos.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer backfill tx mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      bool need_backfill = false;
      bool is_match = false;

      if (OB_FAIL(check_local_tablet_match_(tablet_info, is_match))) {
        LOG_WARN("failed to check local tablet match", K(ret), K(tablet_info));
      } else if (!is_match) {
        //do nothing
      } else if (OB_FAIL(check_tablet_need_backfill_(tablet_info, need_backfill))) {
        LOG_WARN("failed to check tablet need backfill", K(ret), K(tablet_info));
      } else if (!need_backfill) {
        //do nothing
        LOG_INFO("tablet does not need backfill", K(ret), K(tablet_info));
      } else if (OB_FAIL(need_backfill_tablet_infos.push_back(tablet_info))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObSSTransferBackfillTXMgr::check_tablet_need_backfill_(
    const ObTabletBackfillInfo &tablet_info,
    bool &need_backfill)
{
  int ret = OB_SUCCESS;
  need_backfill = false;
  bool is_tablet_exist = false;
  bool is_data_uploaded = false;
  bool is_data_backfilled = false;
  bool is_mds_reloc_done = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer backfill tx mgr do not init", K(ret));
  } else if (!tablet_info.is_valid()
      || ObTabletBackfillType::BACKFILL_TRANSFER_OUT != tablet_info.backfill_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tablet need backfill get invalid argument", K(ret), K(tablet_info));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_ss_src_tablet_ready(ls_id_, tablet_info.tablet_id_, 
      tablet_info.reorganization_scn_, tablet_info.backfill_scn_, is_tablet_exist, is_data_uploaded, is_data_backfilled))) {
    LOG_WARN("failed to check ss src tablet ready", K(ret), K(ls_id_), K(tablet_info));
  } else if (!is_data_uploaded) {
    need_backfill = false;
  } else if (!is_data_backfilled) {
    need_backfill = true;
  } else if (OB_FAIL(ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
      tablet_info.relative_ls_id_, tablet_info.tablet_id_, tablet_info.backfill_scn_, is_mds_reloc_done))) {
    LOG_WARN("failed to check tablet mds reloc done", K(ret), K(tablet_info));
  } else {
    need_backfill = !is_mds_reloc_done;
  }
  return ret;
}

ObSSTransferReplaceMgr::ObSSTransferReplaceMgr()
  : ObSSTransferBasicMgr()
{
}

ObSSTransferReplaceMgr::~ObSSTransferReplaceMgr()
{
}

int ObSSTransferReplaceMgr::process(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> need_replace_tablet_infos;
  bool is_exist = false;
  ObSSTransferBackfillTXParam param;
  const ObSSTransferBackfillType type(ObSSTransferBackfillType::BACKFILL_REPLACE_TABLE);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer replace mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(check_dag_net_exist_(is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(ls_id_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(get_need_replace_tablet_info_(tablet_info_array, need_replace_tablet_infos))) {
    LOG_WARN("failed to get need backfill tablet info", K(ret), K(ls_id_));
  } else if (need_replace_tablet_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(build_dag_net_param_(type, need_replace_tablet_infos, task_id, param))) {
    LOG_WARN("failed to build dag net param", K(ret), K(type), K(need_replace_tablet_infos));
  } else if (OB_FAIL(schedule_dag_net_(param))) {
    LOG_WARN("failed to schedule dag net", K(ret), K(param));
  }

  LOG_INFO("finish do ss replace mgr process", K(ret), "tablet count", tablet_info_array.count(),
      "need replace count", need_replace_tablet_infos.count(), "cost", ObTimeUtil::current_time() - start_ts, K(is_exist));
  return ret;
}

int ObSSTransferReplaceMgr::get_need_replace_tablet_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_replace_tablet_infos)
{
  int ret = OB_SUCCESS;
  need_replace_tablet_infos.reset();
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  share::SCN ss_mds_clog_checkpoint_scn;
  bool mds_flush_requested = false;

  // The authoritative SS LS_MDS_TABLET clog checkpoint is LS-wide.  Read it
  // once before the loop and reuse it for every tablet marker in this round;
  // an older snapshot can only delay scheduling and cannot admit an unsafe
  // tablet.
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer replace mgr do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (tablet_info_array.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(
      ls_id_, ss_mds_clog_checkpoint_scn))) {
    LOG_WARN("failed to read shared mds checkpoint before checking replace tablets",
        K(ret), K(ls_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      bool is_match = false;
      bool is_local_tablet_ready = false;
      bool is_ss_dest_tablet_ready = false;
      bool is_ss_src_tablet_exist = false;
      bool is_ss_src_data_uploaded = false;
      bool is_ss_src_data_backfilled = false;
      bool is_mds_reloc_persisted = false;
      bool need_replace = false;

      if (OB_FAIL(check_local_tablet_match_(tablet_info, is_match))) {
        LOG_WARN("failed to check local tablet match", K(ret), K(tablet_info));
      } else if (!is_match) {
        // do nothing
      } else if (OB_FAIL(ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
          ls, tablet_info.tablet_id_, tablet_info.backfill_scn_,
          ss_mds_clog_checkpoint_scn, is_mds_reloc_persisted, &mds_flush_requested))) {
        LOG_WARN("failed to check tablet mds reloc persisted", K(ret), K(tablet_info), KPC(ls));
      } else if (!is_mds_reloc_persisted) {
        // Replace mutates shared state, so only the authoritative SS checkpoint
        // is the relevant persistence fence at this stage.
        LOG_INFO("skip replace before mds relocation is persisted", K(tablet_info));
      } else if (OB_FAIL(check_local_tablet_ready_(tablet_info, ls, is_local_tablet_ready))) {
        LOG_WARN("failed to check local tablet ready", K(ret), K(tablet_info));
      } else if (is_local_tablet_ready) {
        //do nothing
      } else if (OB_FAIL(check_ss_dest_tablet_ready_(tablet_info, is_ss_dest_tablet_ready))) {
        LOG_WARN("failed to check ss tablet ready", K(ret), K(tablet_info));
      } else if (is_ss_dest_tablet_ready) {
        //do nothing
      } else if (OB_FAIL(ObSSTransferBackfillUtils::check_ss_src_tablet_ready(tablet_info.relative_ls_id_, tablet_info.tablet_id_,
          tablet_info.src_reorganization_scn_, tablet_info.backfill_scn_, is_ss_src_tablet_exist, is_ss_src_data_uploaded, is_ss_src_data_backfilled))) {
        LOG_WARN("failed to check ss src tablet ready", K(ret), K(ls_id_), K(tablet_info));
      } else if (!is_ss_src_data_uploaded || !is_ss_src_data_backfilled) {
        //do nothing
      } else {
        need_replace = true;
      }
      
      if (OB_FAIL(ret)) {
      } else if (!need_replace) {
        //skip this tablet
        LOG_INFO("tablet does not need replace", K(ret), K(is_mds_reloc_persisted), K(is_local_tablet_ready),
            K(is_ss_dest_tablet_ready), K(is_ss_src_tablet_exist), K(is_ss_src_data_uploaded),
            K(is_ss_src_data_backfilled), K(tablet_info));
      } else if (OB_FAIL(need_replace_tablet_infos.push_back(tablet_info))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      }
    } 
  }
  return ret;
}

int ObSSTransferReplaceMgr::check_local_tablet_ready_(
    const ObTabletBackfillInfo &tablet_info,
    ObLS *ls,
    bool &is_ready)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  is_ready = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer replace mgr do not init", K(ret));
  } else if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check local tablet ready get invalid argument", K(ret), K(tablet_info));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KPC(tablet), K(tablet_info), K(ls_id_));
  } else if (!tablet->get_tablet_meta().has_transfer_table()) {
    is_ready = true;
  } else {
    is_ready = false;
  }
  return ret;
}

int ObSSTransferReplaceMgr::check_ss_dest_tablet_ready_(
    const ObTabletBackfillInfo &tablet_info,
    bool &is_ready)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ObTablet *ss_dest_tablet = nullptr;
  is_ready = false;
  if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info.tablet_id_,
      tablet_info.reorganization_scn_, allocator, tablet_handle, true /*read_mark_delete*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      if (OB_FAIL(ObSSTransferBackfillUtils::create_ss_tablet(ls_id_, tablet_info.tablet_id_))) {
        LOG_WARN("failed to create ss tablet", K(ret), K(ls_id_), K(tablet_info));
      } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info.tablet_id_,
          tablet_info.reorganization_scn_, allocator, tablet_handle, true /*read_mark_delete*/))) {
        LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info));
      }
    } else {
      LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ss_dest_tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info));
  } else if (!ss_dest_tablet->get_tablet_meta().has_transfer_table()) {
    is_ready = true;
  } else {
    is_ready = false;
  }
  return ret;
}

ObSSTransferRefreshMgr::ObSSTransferRefreshMgr()
  : ObSSTransferBasicMgr()
{
}

ObSSTransferRefreshMgr::~ObSSTransferRefreshMgr()
{
}

int ObSSTransferRefreshMgr::process(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletBackfillInfo> need_refresh_tablet_infos;
  bool is_exist = false;
  ObSSTransferBackfillTXParam param;
  const ObSSTransferBackfillType type(ObSSTransferBackfillType::BACKFILL_REFRESH);
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer refresh tx mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    //do nothing
  } else if (OB_FAIL(check_dag_net_exist_(is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(ls_id_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(get_need_refresh_tablet_info_(tablet_info_array, need_refresh_tablet_infos))) {
    LOG_WARN("failed to get need refresh tablet info", K(ret), K(ls_id_));
  } else if (need_refresh_tablet_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(build_dag_net_param_(type, need_refresh_tablet_infos, task_id, param))) {
    LOG_WARN("failed to build dag net param", K(ret), K(type), K(need_refresh_tablet_infos));
  } else if (OB_FAIL(schedule_dag_net_(param))) {
    LOG_WARN("failed to schedule dag net", K(ret), K(param));
  }
  LOG_INFO("finish do ss refresh mgr process", K(ret), "tablet count", tablet_info_array.count(),
      "need refresh count", need_refresh_tablet_infos.count(), "cost", ObTimeUtil::current_time() - start_ts, K(is_exist));
  return ret;
}

int ObSSTransferRefreshMgr::get_need_refresh_tablet_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_refresh_tablet_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer refresh tx mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get need refresh tablet info get invalid argument", K(ret), K(tablet_info_array));
  } else {
    const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(0);
    if (ObTabletBackfillType::BACKFILL_TRANSFER_IN == tablet_info.backfill_type_) {
      if (OB_FAIL(get_need_refresh_transfer_in_tablet_info_(tablet_info_array, need_refresh_tablet_infos))) {
        LOG_WARN("failed to get need refresh transfer in tablet info", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet info backfill type is unexpected", K(ret), K(tablet_info));
    }
  }
  return ret;
}

int ObSSTransferRefreshMgr::get_need_refresh_transfer_in_tablet_info_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &need_refresh_tablet_infos)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  share::SCN local_mds_checkpoint_scn;
  bool mds_flush_requested = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer refresh tx mgr do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (tablet_info_array.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTabletMdsRelocHelper::get_local_mds_checkpoint_scn(
      ls, local_mds_checkpoint_scn))) {
    LOG_WARN("failed to read local mds checkpoint before checking refresh tablets",
        K(ret), K(ls_id_), KPC(ls));
  } else {
    // Like replace, fail this scheduling round if the LS-wide checkpoint read
    // fails. Only a successful read may be reused by the per-tablet checks.
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      bool need_refresh = false;
      bool is_match = false;
      bool is_local_mds_checkpoint_reached = false;
      if (ObTabletBackfillType::BACKFILL_TRANSFER_IN != tablet_info.backfill_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet info backfill type is not TRANSFER_IN, unexpected", K(ret), K(tablet_info));
      } else if (OB_FAIL(check_local_tablet_match_(tablet_info, is_match))) {
        LOG_WARN("failed to check local tablet match", K(ret), K(tablet_info));
      } else if (!is_match) {
        //do nothing
      } else if (OB_FAIL(ObTabletMdsRelocHelper::check_local_checkpoint_reached(
          ls, tablet_info.tablet_id_, tablet_info.backfill_scn_,
          local_mds_checkpoint_scn, is_local_mds_checkpoint_reached, &mds_flush_requested))) {
        LOG_WARN("failed to check tablet mds reloc local checkpoint",
            K(ret), K(tablet_info), KPC(ls));
      } else if (!is_local_mds_checkpoint_reached) {
        LOG_INFO("skip refresh before local mds checkpoint covers relocation", K(tablet_info));
      } else if (OB_FAIL(check_transfer_in_tablet_need_refresh_(tablet_info, ls, need_refresh))) {
        LOG_WARN("failed to check transfer in tablet need refresh", K(ret), K(tablet_info));
      } else if (!need_refresh) {
        //do nothing
        LOG_INFO("tablet does not need refresh", K(ret), K(tablet_info));
      } else if (OB_FAIL(need_refresh_tablet_infos.push_back(tablet_info))) {
        LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObSSTransferRefreshMgr::check_transfer_in_tablet_need_refresh_(
    const ObTabletBackfillInfo &tablet_info,
    ObLS *ls,
    bool &need_refresh)
{
  int ret = OB_SUCCESS;
  need_refresh = false;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;
  ObArenaAllocator allocator;
  ObTabletHandle ss_tablet_handle;
  ObTablet *ss_tablet = nullptr;
  // No reloc-done marker check here: the caller already gated this tablet on
  // check_local_checkpoint_reached, which requires the
  // committed done-marker (and, for V2, the local checkpoint fence), so a
  // second marker read would be redundant.

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer refresh tx mgr do not init", K(ret));
  } else if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer out tablet need refresh get invalid argument", K(ret), K(tablet_info), KPC(ls));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info.tablet_id_, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info), KPC(ls));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info), K(local_tablet_handle));
  } else if (!local_tablet->get_tablet_meta().has_transfer_table()) {
    need_refresh = false;
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info.tablet_id_,
      tablet_info.reorganization_scn_, allocator, ss_tablet_handle, true /*read_mark_delete*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      need_refresh = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info));
    }
  } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ss_tablet_handle), K(tablet_info));
  } else if (!ss_tablet->get_tablet_meta().has_transfer_table()) {
    need_refresh = true;
  } else {
    need_refresh = false;
  }
  return ret;
}

int ObSSTransferWorkerMgr::filter_mds_reloc_done_tablets_(
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
    common::ObIArray<ObTabletBackfillInfo> &ready_tablet_info_array)
{
  int ret = OB_SUCCESS;
  ready_tablet_info_array.reset();

  // This common prefilter feeds both replace and refresh.  It only checks that
  // the final marker committed; each stage applies its own SS or local
  // checkpoint fence later.
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss transfer worker mgr do not init", K(ret));
  } else if (tablet_info_array.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_info_array.count(); ++i) {
      const ObTabletBackfillInfo &tablet_info = tablet_info_array.at(i);
      bool is_done = false;
      if (ObTabletBackfillType::BACKFILL_TRANSFER_IN != tablet_info.backfill_type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tablet should be transfer in", K(ret), K(tablet_info));
      } else if (OB_FAIL(ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
          ls_, tablet_info.tablet_id_, tablet_info.backfill_scn_, is_done))) {
        LOG_WARN("failed to check tablet mds reloc done", K(ret), K(ls_id_), K(tablet_info));
      } else if (!is_done) {
        LOG_INFO("skip transfer in tablet before mds relocation is done", K(ls_id_), K(tablet_info));
      } else if (OB_FAIL(ready_tablet_info_array.push_back(tablet_info))) {
        LOG_WARN("failed to push ready tablet info", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

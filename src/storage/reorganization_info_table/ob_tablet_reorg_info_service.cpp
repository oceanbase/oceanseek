/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_reorg_info_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ls/ob_ls_table_operator.h"
#include "lib/utility/ob_tracepoint.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "ob_tablet_reorg_info_table_operation.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ERRSIM_POINT_DEF(EN_REORG_INFO_GC_FAST_SCHEDULE);
ERRSIM_POINT_DEF(EN_REORG_GC_TEST_SCN);

ObTabletReorgInfoTableService::ObTabletReorgInfoTableService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr)
{
}

ObTabletReorgInfoTableService::~ObTabletReorgInfoTableService()
{
}

int ObTabletReorgInfoTableService::mtl_init(ObTabletReorgInfoTableService *&reorg_info_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(reorg_info_service->init(ls_service))) {
    LOG_WARN("failed to init tablet reorg info service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObTabletReorgInfoTableService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet reorg info table service is already init", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table service get invalid argument", K(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletReorgInfoTableService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObTabletReorgInfoTableService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    is_inited_ = false;
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService destroyed");
  }
}

void ObTabletReorgInfoTableService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService stopped");
  }
}

void ObTabletReorgInfoTableService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService finish to wait");
  }
}

int ObTabletReorgInfoTableService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObTabletReorgInfoTableService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObTabletReorgInfoTableService start");
    }
  }
  return ret;
}

void ObTabletReorgInfoTableService::run1()
{
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name("ReorgSrv");

  while (!has_set_stop()) {
    if (SS_SERVING != GCTX.status_) {
      tmp_ret = OB_SERVER_IS_INIT;
      LOG_WARN_RET(tmp_ret, "server is not serving", K(GCTX.status_));
    } else if (OB_SUCCESS != (tmp_ret = calc_max_recycle_scn_())) {
      LOG_WARN_RET(tmp_ret, "failed to calc max recycle scn");
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      int64_t wait_time_ms = SCHEDULER_WAIT_TIME_MS;
      if (OB_SERVER_IS_INIT == tmp_ret) {
        wait_time_ms = WAIT_SERVER_IN_SERVICE_TIME_MS;
      }
#ifdef ERRSIM
      if (EN_REORG_INFO_GC_FAST_SCHEDULE) {
        wait_time_ms = 1000;
      }
#endif
      ObBKGDSessInActiveGuard inactive_guard;
      thread_cond_.wait(wait_time_ms);
    }
  }
}

int ObTabletReorgInfoTableService::calc_max_recycle_scn_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<share::ObLSID> ls_id_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls_ids(ls_id_array))) {
    LOG_WARN("failed to get ls ids", K(ret));
  } else {
    for (int64_t i = 0; i < ls_id_array.count(); ++i) {
      const ObLSID &ls_id = ls_id_array.at(i);
      if (OB_SUCCESS != (tmp_ret = calc_ls_max_recycle_scn_(ls_id))) {
        LOG_WARN("failed to calc ls max recycle scn", K(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::calc_ls_max_recycle_scn_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletReorgInfoTableReadOperator read_op;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 600 * 1000 * 1000; //10min
  share::SCN max_recycle_scn(share::SCN::min_scn());
  share::SCN min_unrecycle_scn(share::SCN::max_scn());
  share::SCN recycle_scn;
  share::SCN max_decided_scn;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool can_recycle = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc ls max recycle scn get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id), K(ls_handle));
  } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get local replay progress for reorg gc", K(ret), K(ls_id));
#ifdef ERRSIM
  } else if (static_cast<int64_t>(MTL_ID()) == EN_REORG_GC_TEST_SCN.item_.cond_
      && OB_FAIL(max_decided_scn.convert_for_inner_table_field(EVENT_CODE(EN_REORG_GC_TEST_SCN)))) {
    LOG_WARN("failed to inject reorg gc test progress", K(ret));
#endif
  } else if (!max_decided_scn.is_valid_and_not_min() || max_decided_scn.is_max()) {
    ret = OB_EAGAIN;
    LOG_INFO("local replay progress is not ready for reorg gc", K(ret), K(ls_id), K(max_decided_scn));
  } else if (OB_FAIL(read_op.init(tenant_id, ls_id, timeout_us))) {
    LOG_WARN("failed to init read operator", K(ret), K(tenant_id), K(ls_id));
  } else {
    // Treat local progress as an exclusive recycle boundary.
    min_unrecycle_scn = max_decided_scn;
    ObTabletReorgInfoData data;
    share::SCN commit_scn;
    while (OB_SUCC(ret)) {
      data.reset();
      commit_scn.reset();
      if (OB_FAIL(read_op.get_next(data, commit_scn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next data", K(ret), K(tenant_id), K(ls_id));
        }
      } else if (!commit_scn.is_valid_and_not_min() || commit_scn.is_max()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid reorg row commit scn", K(ret), K(ls_id), K(commit_scn), K(data));
      } else if (commit_scn > max_decided_scn) {
        // Skip rows ahead of local replay/apply progress without inspecting tablets.
        // Keep scanning: rows are ordered by rowkey, not by commit SCN.
        LOG_DEBUG("skip reorg row ahead of local progress", K(ls_id), K(commit_scn), K(max_decided_scn));
      } else if (OB_FAIL(check_can_recycle_(*ls, data, can_recycle))) {
        LOG_WARN("failed to check can recycle", K(ret), K(data));
      } else if (can_recycle) {
        max_recycle_scn = share::SCN::max(max_recycle_scn, commit_scn);
      } else {
        min_unrecycle_scn = share::SCN::min(min_unrecycle_scn, commit_scn);
      }
    }

    if (OB_SUCC(ret)) {
      if (max_recycle_scn < min_unrecycle_scn) {
        recycle_scn = max_recycle_scn;
      } else {
        recycle_scn = share::SCN::scn_dec(min_unrecycle_scn);
      }

      if (OB_FAIL(ls->get_reorg_info_table()->update_can_recycle_scn(recycle_scn))) {
        LOG_WARN("failed to update can recycle scn", K(ret), K(recycle_scn));
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_can_recycle_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (ObTabletReorgInfoDataType::is_transfer(data.key_.type_)) {
    if (OB_FAIL(check_transfer_tablet_(ls, data, can_recycle))) {
      LOG_WARN("failed to check transfer tablet", K(ret), K(data));
    }
  } else if (ObTabletReorgInfoDataType::is_fork(data.key_.type_)) {
    if (OB_FAIL(check_fork_tablet_(ls, data, can_recycle))) {
      LOG_WARN("[FORKTABLE] failed to check fork tablet", K(ret), K(data));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablet reorg info table tablet status do not supported", K(ret), K(data));
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_tablet_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  bool is_finised = false;
  ObTransferDataValue data_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(data.get_transfer_data_value(data_value))) {
    LOG_WARN("failed to get transfer data value", K(ret), K(data));
  } else if (ObTabletStatus::TRANSFER_IN == data_value.tablet_status_) {
    if (OB_FAIL(check_transfer_in_tablet_finish_(ls, data, data_value, is_finised))) {
      LOG_WARN("failed to check transfer in tablet finish", K(ret), K(ls), K(data));
    } else {
      can_recycle = is_finised;
    }
  } else if (ObTabletStatus::TRANSFER_OUT == data_value.tablet_status_) {
    if (OB_FAIL(check_transfer_out_tablet_finish_(ls, data, data_value, is_finised))) {
      LOG_WARN("failed to check transfer in tablet finish", K(ret), K(ls), K(data));
    } else {
      can_recycle = is_finised;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_fork_tablet_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    bool &can_recycle)
{
  int ret = OB_SUCCESS;
  ObForkDataValue fork_data_value;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObForkTabletInfo fork_info;
  can_recycle = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(data.get_fork_data_value(fork_data_value))) {
    LOG_WARN("[FORKTABLE] failed to get fork data value", K(ret), K(data));
  } else if (OB_FAIL(ls.ha_get_tablet(data.key_.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      can_recycle = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FORKTABLE] failed to get fork tablet", K(ret), K(data), K(fork_data_value));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] tablet should not be NULL", K(ret), K(data), K(fork_data_value), K(tablet_handle));
  } else {
    // 非 FORK_DST 可回收；active FORK_DST 仅保留与当前 fork snapshot 匹配的驱动行。
    // TODO: 后续尝试使用 local tablet meta 判断 fork reorg 行是否可回收，
    // 并根据 __all_fork_table_tablet_task 判断补数据结果是否已成功汇报，
    // 对未成功汇报的 tablet 进行重报，移除本处对 tablet status MDS 的依赖。
    ObTabletCreateDeleteMdsUserData user_data;
    if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(
            share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
      if (OB_EMPTY_RESULT == ret || OB_ENTRY_NOT_EXIST == ret) {
        can_recycle = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[FORKTABLE] failed to get committed tablet status for fork recycle", K(ret), K(data), K(fork_data_value));
      }
    } else if (ObTabletStatus::FORK_DST != user_data.get_tablet_status()) {
      can_recycle = true;
    } else if (OB_FAIL(tablet->get_fork_info(fork_info))) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_EMPTY_RESULT == ret) {
        can_recycle = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[FORKTABLE] failed to get fork info", K(ret), K(data), K(fork_data_value));
      }
    } else if (fork_info.get_fork_snapshot_scn() != data.key_.reorganization_scn_) {
      can_recycle = true;
    } else {
      can_recycle = false;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_in_tablet_finish_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    const ObTransferDataValue &data_value,
    bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  is_finish = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls.ha_get_tablet(data.key_.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_finish = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(data));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(data), K(tablet_handle));
  } else {
    const ObTabletMeta &meta = tablet->get_tablet_meta();
    if (meta.transfer_info_.transfer_seq_ > data_value.transfer_seq_
        || meta.transfer_info_.transfer_start_scn_ > data.key_.reorganization_scn_) {
      is_finish = true;
    } else if (meta.transfer_info_.transfer_seq_ == data_value.transfer_seq_
        && meta.transfer_info_.transfer_start_scn_ == data.key_.reorganization_scn_
        && !meta.transfer_info_.has_transfer_table()) {
      ObTabletCreateDeleteMdsUserData user_data;
      if (OB_FAIL(tablet->get_latest_committed_tablet_status(user_data))) {
        LOG_WARN("failed to get committed tablet status", K(ret), KPC(tablet), K(user_data));
      } else if (ObTabletStatus::NORMAL == user_data.tablet_status_) {
        is_finish = true;
      } else {
        is_finish = false;
      }
    } else {
      is_finish = false;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_out_tablet_finish_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    const ObTransferDataValue &data_value,
    bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls.ha_get_tablet(data.key_.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_finish = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(data));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(data), K(tablet_handle));
  } else {
    const ObTabletMeta &meta = tablet->get_tablet_meta();
    if (meta.transfer_info_.transfer_seq_ > data_value.transfer_seq_) {
      is_finish = true;
    } else if (meta.transfer_info_.transfer_seq_ < data_value.transfer_seq_) {
      is_finish = false;
    }
  }
  return ret;
}

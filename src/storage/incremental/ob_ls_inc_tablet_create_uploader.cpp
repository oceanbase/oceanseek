/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ls_inc_sstable_uploader.h"
#include "ob_inc_sstable_upload_task.h"
#include "ob_inc_ss_dag_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

#define TABLET_CREATE_UPLOADER_LOG(LOG_LEVEL, LOG_STRING, ...) \
  LOG_##LOG_LEVEL(LOG_STRING, KR(ret), "ls_id", ls_.get_ls_id(), K(tablet_create_ss_rec_scn_), __VA_ARGS__);

namespace
{
const int64_t PENDING_CREATE_SCAN_FACTOR = 10;
const int64_t MIN_PENDING_CREATE_SCAN_LIMIT = 1000;
}

int ObLSIncSSTableUploader::create_ss_tablet_(const ObLSID &ls_id,
                                              const TabletCreateInfo &create_info,
                                              const lib::Worker::CompatMode compat_mode,
                                              bool &create_dag_scheduled,
                                              bool &create_finished)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  create_dag_scheduled = false;
  create_finished = false;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IncSSTUploader]meta service is null", KR(ret), K(ls_id), K(create_info));
  } else if (OB_FAIL(meta_service->check_tablet_exist(ls_id,
                                                      create_info.tablet_id_,
                                                      create_info.transfer_scn_,
                                                      exist))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[IncSSTUploader]check shared tablet exist failed before create", KR(ret), K(ls_id), K(create_info));
    }
  }

  if (OB_SUCC(ret) && !exist) {
    ObIncSSTableUploadParam create_param;
    if (OB_FAIL(create_param.init_for_tablet_create(ls_id,
                                                    create_info.tablet_id_,
                                                    compat_mode,
                                                    create_info.mds_start_scn_,
                                                    create_info.transfer_scn_))) {
      LOG_WARN("[IncSSTUploader]init shared tablet create dag param failed",
               KR(ret), K(ls_id), K(create_info), K(create_param));
    } else if (OB_FAIL(ObIncSSDagUtil::schedule_inc_sst_upload_dag(create_param))) {
      if (OB_EAGAIN == ret) {
        // The same ls/tablet/transfer_scn MDS dag is already in flight.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[IncSSTUploader]schedule shared tablet create dag failed",
                 KR(ret), K(ls_id), K(create_info), K(create_param));
      }
    } else {
      create_dag_scheduled = true;
    }
  } else if (exist) {
    create_finished = true;
  }
  return ret;
}

void ObTabletCreateRegHandle::do_register(const ObTabletID &tablet_id,
                                           const share::SCN &mds_start_scn,
                                           const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uploader_) || OB_ISNULL(create_info_holder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet create reg handle is invalid", KR(ret), KP(uploader_), KP(create_info_holder_));
  } else {
    TabletCreateInfo &create_info = create_info_holder_->info_;
    create_info.tablet_id_ = tablet_id;
    create_info.mds_start_scn_ = mds_start_scn;
    create_info.transfer_scn_ = transfer_scn;
    if (OB_FAIL(uploader_->register_tablet_create_info(*create_info_holder_))) {
      LOG_ERROR("failed to register tablet create info", KR(ret), K(create_info));
      // Registration failed without transferring ownership; release after logging.
      MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", create_info_holder_);
    }
    uploader_ = nullptr;
    create_info_holder_ = nullptr;
  }
}

ObTabletCreateRegHandle::~ObTabletCreateRegHandle()
{
  if (create_info_holder_) {
    MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", create_info_holder_);
    create_info_holder_ = NULL;
  }
}

int ObLSIncSSTableUploader::prepare_register_tablet_create(ObTabletCreateRegHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("prepare_register_tablet_create", 100LL * 1000LL /*100ms*/);
  int64_t retry_alloc_max_count = 10;
  do {
    TabletCreateInfoHolder *holder = MTL_NEW(TabletCreateInfoHolder, "TabletCreateRef");
    if (OB_ISNULL(holder)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TABLET_CREATE_UPLOADER_LOG(WARN, "alloc tablet create holder failed", KP(holder));
      sched_yield();
    } else {
      handle.uploader_ = this;
      handle.create_info_holder_ = holder;
      ret = OB_SUCCESS;
    }
  } while ((--retry_alloc_max_count > 0) && (OB_ALLOCATE_MEMORY_FAILED == ret));
  return ret;
}

int ObLSIncSSTableUploader::register_tablet_create_info(TabletCreateInfoHolder &holder)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(list_lock_);
  if (ATOMIC_LOAD(&is_online_)) {
    if (!pending_create_list_.add_last(&holder)) {
      ret = OB_ERR_UNEXPECTED;
      TABLET_CREATE_UPLOADER_LOG(ERROR, "add tablet create to list fail", K(holder));
    }
  } else {
    ret = OB_LS_OFFLINE;
    TABLET_CREATE_UPLOADER_LOG(INFO, "skip register tablet create because logstream offlined", K(holder));
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("[IncSSTUploader]finish register tablet create", K(holder.info_));
  }
  return ret;
}

int ObLSIncSSTableUploader::handle_pending_tablet_create_(const int64_t limit,
                                                          int64_t &processed_cnt)
{
  int ret = OB_SUCCESS;
  processed_cnt = 0;
  int64_t succeed_cnt = 0;
  int64_t skipped_cnt = 0;
  bool ls_is_normal_status = true;
  int64_t ls_switch_epoch = 0;
  TabletCreateList pending_create_list;
  const ObLSID ls_id = ls_.get_ls_id();
  const share::SCN ls_sn_checkpoint_scn = ls_.get_clog_checkpoint_scn();
  share::SCN cur_pending_min_rec_scn(ls_sn_checkpoint_scn);
  const share::SCN old_tablet_create_ss_rec_scn = tablet_create_ss_rec_scn_;
  int64_t start_ts = ObClockGenerator::getClock();
  // Existing tablets do not consume the create budget. The scan budget keeps
  // an all-existing batch from monopolizing the uploader thread.
  const int64_t scan_limit = MAX(limit * PENDING_CREATE_SCAN_FACTOR, MIN_PENDING_CREATE_SCAN_LIMIT);
  int64_t create_dag_schedule_cnt = 0;
  int64_t create_dag_schedule_failed_cnt = 0;
  int64_t create_dag_inflight_cnt = 0;
  int64_t exist_cnt = 0;
  bool create_limit_reached = false;
  bool scan_limit_reached = false;
  if (OB_FAIL(check_ls_status_(ls_is_normal_status, ls_switch_epoch))) {
    TABLET_CREATE_UPLOADER_LOG(WARN, "[IncSSTUploader]check ls status failed before pending tablet create", K(ls_sn_checkpoint_scn));
  } else {
    const bool reload_finished = ATOMIC_LOAD(&finish_reloading_);
    get_pending_create_list_(pending_create_list);
    TabletCreateInfoHolder *header = pending_create_list.get_header();
    TabletCreateInfoHolder *iter = header->get_next();
    TabletCreateInfoHolder *next = iter;
    bool batch_limit_reached = false;
    while (OB_SUCC(ret) && next != pending_create_list.get_header() && !batch_limit_reached) {
      iter = next;
      next = iter->get_next();
      const share::SCN create_scn = iter->info_.mds_start_scn_;
      int tmp_ret = OB_SUCCESS;
      bool create_dag_scheduled = false;
      bool create_finished = false;
      bool tablet_already_exist = false;
      bool is_skipped = false;
      if (!ATOMIC_LOAD(&is_online_)) {
        ret = OB_LS_OFFLINE;
        TABLET_CREATE_UPLOADER_LOG(INFO, "[IncSSTUploader]skip tablet create because uploader has been offlined", K(is_online_));
        break;
      } else {
        ObTabletHandle local_tablet;
        if (OB_TMP_FAIL(ls_.get_tablet_svr()->ha_get_tablet(iter->info_.tablet_id_, local_tablet))) {
          if (OB_TABLET_NOT_EXIST == tmp_ret) {
            FLOG_INFO("[IncSSTUploader]tablet not exist in local t3m, skip shared tablet create",
                      K(ls_id),
                      K(iter->info_));
            tmp_ret = OB_SUCCESS;
            is_skipped = true;
          } else {
            TABLET_CREATE_UPLOADER_LOG(WARN, "[IncSSTUploader]direct get local tablet failed before create",
                                       KR(tmp_ret),
                                       K(iter->info_));
          }
        } else if (local_tablet.get_obj()->is_empty_shell()) {
          // this path may not hit, because ha_get_tablet will return OB_TABLET_NOT_EXIST if the tablet is empty shell
          FLOG_INFO("[IncSSTUploader]tablet is empty shell, skip shared tablet create",
                    K(ls_id),
                    K(iter->info_));
          is_skipped = true;
        } else if (OB_TMP_FAIL(ls_.get_tablet(iter->info_.tablet_id_,
                                              local_tablet,
                                              600_s,
                                              ObMDSGetTabletMode::READ_ALL_COMMITED))) {
          if (OB_TABLET_NOT_EXIST == tmp_ret) {
            TABLET_CREATE_UPLOADER_LOG(INFO, "[IncSSTUploader]tablet create not committed yet, retry later",
                                       K(iter->info_));
          } else {
            TABLET_CREATE_UPLOADER_LOG(WARN, "[IncSSTUploader]get local tablet failed before create",
                                       KR(tmp_ret),
                                       K(iter->info_));
          }
        } else if (local_tablet.get_obj()->get_reorganization_scn() != iter->info_.transfer_scn_) {
          // 这里提前判断，实际底层创建ss_tablet时，也会再次判断
          is_skipped = true;
          FLOG_INFO("[IncSSTUploader]skip stale pending shared tablet create", K(ls_id), K(iter->info_));
        } else if (OB_TMP_FAIL(create_ss_tablet_(ls_id,
                                                iter->info_,
                                                local_tablet.get_obj()->get_tablet_meta().compat_mode_,
                                                create_dag_scheduled,
                                                create_finished))) {
          ++create_dag_schedule_failed_cnt;
          TABLET_CREATE_UPLOADER_LOG(WARN,
                                     "[IncSSTUploader]failed to schedule shared tablet create dag",
                                     KR(tmp_ret),
                                     K(iter->info_));
        } else if (create_finished) {
          tablet_already_exist = true;
          FLOG_INFO("[IncSSTUploader]shared tablet already exists", K(ls_id), K(iter->info_));
        } else {
          ++create_dag_inflight_cnt;
        }
      }

      if (create_dag_scheduled) {
        ++create_dag_schedule_cnt;
      }
      if (tablet_already_exist) {
        ++exist_cnt;
      }
      const bool item_finished = is_skipped || create_finished;
      if (OB_SUCCESS == tmp_ret && item_finished) {
        pending_create_list.remove(iter);
        MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", iter);
        ++succeed_cnt;
        if (is_skipped) {
          ++skipped_cnt;
        }
      } else if (reload_finished) {
        cur_pending_min_rec_scn = share::SCN::min(cur_pending_min_rec_scn, create_scn);
      }
      ++processed_cnt;
      // Bound both newly scheduled and already in-flight create DAG checks. Existing
      // shared tablets still do not consume the create budget.
      create_limit_reached = create_dag_inflight_cnt + create_dag_schedule_failed_cnt >= limit;
      scan_limit_reached = processed_cnt >= scan_limit;
      batch_limit_reached = create_limit_reached || scan_limit_reached;
    }
    if (batch_limit_reached && reload_finished) {
      // Batch budget reached: stop processing to avoid stalling ss_ckpt progress,
      // but include mds_start_scn of the remaining unprocessed items so the
      // checkpoint does not advance past tablets that still need to be created on SS.
      TabletCreateInfoHolder *remaining = next;
      while (remaining != pending_create_list.get_header()) {
        cur_pending_min_rec_scn = share::SCN::min(cur_pending_min_rec_scn, remaining->info_.mds_start_scn_);
        remaining = remaining->get_next();
      }
    }
    const int64_t unprocessed_cnt = pending_create_list.get_size();
    merge_pending_create_list_(pending_create_list);

    bool rec_scn_updated = false;
    if (reload_finished && OB_SUCC(ret) && ls_is_normal_status) {
      (void)update_pending_create_rec_scn_(cur_pending_min_rec_scn, ls_switch_epoch, rec_scn_updated);
    }
    int64_t end_ts = ObClockGenerator::getClock();
    int64_t cost_time = end_ts - start_ts;
    FLOG_INFO("[IncSSTUploader]finish handle pending tablet create",
              KR(ret),
              K(ls_id),
              K(cost_time),
              K(processed_cnt),
              K(succeed_cnt),
              K(skipped_cnt),
              K(create_dag_schedule_cnt),
              K(create_dag_schedule_failed_cnt),
              K(create_dag_inflight_cnt),
              K(exist_cnt),
              K(unprocessed_cnt));
    FLOG_INFO("[IncSSTUploader]pending tablet create batch state",
              K(ls_id),
              K(limit),
              K(scan_limit),
              K(batch_limit_reached),
              K(create_limit_reached),
              K(scan_limit_reached),
              K(reload_finished),
              K(rec_scn_updated),
              K(ls_sn_checkpoint_scn),
              K(cur_pending_min_rec_scn),
              K(old_tablet_create_ss_rec_scn),
              K(tablet_create_ss_rec_scn_));
  }
  return ret;
}

void ObLSIncSSTableUploader::update_pending_create_rec_scn_(const share::SCN &cur_pending_min_rec_scn,
                                                           const int64_t prev_switch_epoch,
                                                           bool &updated)
{
  int ret = OB_SUCCESS;
  bool ls_is_normal_status = true;
  int64_t current_switch_epoch = 0;

  if (OB_FAIL(check_ls_status_(ls_is_normal_status, current_switch_epoch))) {
    STORAGE_LOG(WARN, "[IncSSTUploader]check ls status failed", KR(ret));
  } else if (!ls_is_normal_status || prev_switch_epoch != current_switch_epoch) {
    FLOG_INFO("[IncSSTUploader]ls status changed, skip update pending create rec scn",
              KR(ret),
              K(ls_is_normal_status),
              K(prev_switch_epoch),
              K(current_switch_epoch),
              K(cur_pending_min_rec_scn));
  } else {
    updated = true;
    tablet_create_ss_rec_scn_ = cur_pending_min_rec_scn;
  }
}

int ObLSIncSSTableUploader::prepare_reload_tablet_create_if_needed_(ObTablet &tablet,
                                                                    const share::SCN &ls_ss_checkpoint_scn,
                                                                    TabletCreateInfoHolder *&holder)
{
  int ret = OB_SUCCESS;
  holder = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  const share::SCN &mds_start_scn = tablet.get_tablet_meta().mds_start_scn_;
  if (mds_start_scn >= ls_ss_checkpoint_scn) {
    ObTabletCreateRegHandle create_handle;
    if (OB_FAIL(prepare_register_tablet_create(create_handle))) {
      TABLET_CREATE_UPLOADER_LOG(WARN, "[IncSSTUploader]prepare register tablet create failed", K(tablet_id), K(mds_start_scn));
    } else {
      holder = create_handle.create_info_holder_;
      holder->info_.tablet_id_ = tablet_id;
      holder->info_.mds_start_scn_ = mds_start_scn;
      holder->info_.transfer_scn_ = tablet.get_reorganization_scn();
      create_handle.uploader_ = nullptr;
      create_handle.create_info_holder_ = nullptr;
      FLOG_INFO("[IncSSTUploader]prepare reload tablet create",
                K(tablet_id),
                K(mds_start_scn),
                K(ls_ss_checkpoint_scn),
                "transfer_scn", tablet.get_reorganization_scn());
    }
  }
  return ret;
}

#undef TABLET_CREATE_UPLOADER_LOG

} // storage
} // oceanbase

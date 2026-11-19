/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/worker.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "observer/ob_srv_network_frame.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_bucket_num_helper.h"
#include "storage/ob_storage_rpc.h"
#include "storage/ob_storage_schema.h"
#include "storage/meta_mem/ob_tablet_pointer_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "lib/hash/ob_hashset.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::lib;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObSSMarkSweepGCStudyReq, tablet_list_);
OB_SERIALIZE_MEMBER(ObSSMarkSweepGCStudyRes, tablet_meta_scn_list_);
}  // namespace storage

namespace obrpc
{
int ObSSMarkSweepGCStudyP::process()
{
  lib::ErrsimModuleGuard errsim_guard(common::ObErrsimModuleType::ERRSIM_MODULE_SS_GC);
  int ret = OB_SUCCESS;
  const storage::ObSSMarkSweepGCStudyReq &req = arg_;
  storage::ObSSMarkSweepGCStudyRes &res = result_;
  if (OB_FAIL(study_(req.tablet_list_, res.tablet_meta_scn_list_))) {
    LOG_WARN("fail to study", KR(ret), K(req));
  } else {
    LOG_INFO("succ to study", K(req), K(res));
  }
  return ret;
}

int ObSSMarkSweepGCStudyP::study_(
    const ObIArray<ObSSGCTabletWithDelScn> &tablet_list,
    ObIArray<ObSSGCTabletStudyResult> &tablet_meta_scn_list)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  const int64_t tablet_cnt = tablet_list.count();
  if (0 == tablet_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet list is empty", KR(ret), K(tablet_list));
  } else if (OB_FAIL(check_tablets_belong_to_same_ls_(tablet_list))) {
    LOG_WARN("tablets do not belong to the same ls", KR(ret), K(tablet_list));
  } else {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    // all tablets in tablet_list belong to the same ls (guaranteed by caller)
    const ObLSID &ls_id = tablet_list.at(0).gc_tablet_.ls_id_;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SHARED_STORAGE_GC_MOD))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_INFO("ls not exist", K(ls_id));
        ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_cnt); i++) {
          const ObSSGCTablet &cur_gc_tablet = tablet_list.at(i).gc_tablet_;
          if (OB_FAIL(handle_ls_not_exist_(cur_gc_tablet, tablet_meta_scn_list))) {
            LOG_WARN("fail to handle ls not exist", KR(ret), K(cur_gc_tablet));
          }
        }
      } else {
        LOG_WARN("fail to get ls", KR(ret), K(ls_id));
      }
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_migration_status(migration_status))) {
      LOG_WARN("fail to get migration_status of ls", KR(ret), K(ls_id));
    } else if (!ObMigrationStatusHelper::need_online(migration_status)) {
      LOG_TRACE("this ls is in migration, gc it later", K(ls_id), K(migration_status));
      ObSSGCTabletStudyInfo study_info;
      study_info.set_to_min();
      ObSSGCUtil::report_mark_sweep_set_to_min_event(tablet_list.at(0).gc_tablet_, "LS_IN_MIGRATION");
      for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_cnt); i++) {
        const ObSSGCTablet &cur_gc_tablet = tablet_list.at(i).gc_tablet_;
        if (OB_FAIL(append_tablet_meta_scn_(
            cur_gc_tablet, study_info, ObSSGCTabletType::UNKNOWN, tablet_meta_scn_list))) {
          LOG_WARN("fail to append tablet meta scn", KR(ret), K(cur_gc_tablet));
        }
      }
    } else { // ObMigrationStatusHelper::need_online(migration_status)
      ObLS &ls = *ls_handle.get_ls();
      const int64_t ls_epoch = ls.get_ls_epoch();
      ObTabletHandle tablet_handle;
      const SCN &ls_clog_checkpoint_scn = ls.get_clog_checkpoint_scn();
      for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_cnt); i++) {
        const ObSSGCTablet &cur_gc_tablet = tablet_list.at(i).gc_tablet_;
        const ObTabletID &tablet_id = cur_gc_tablet.tablet_id_;
        const SCN &deleted_scn = tablet_list.at(i).deleted_scn_;
        if (deleted_scn.is_valid() && (ls_clog_checkpoint_scn < deleted_scn)) {
          LOG_TRACE("ls clog_checkpoint_scn has not advanced, cannot gc now", K(cur_gc_tablet),
              "ls_clog_checkpoint_scn", ls_clog_checkpoint_scn, "tablet_deleted_scn", deleted_scn);
          ObSSGCTabletStudyInfo study_info;
          study_info.set_to_min();
          ObSSGCUtil::report_mark_sweep_set_to_min_event(cur_gc_tablet, "LS_CLOG_CHECKPOINT_SCN_BEHIND");
          if (OB_FAIL(append_tablet_meta_scn_(
              cur_gc_tablet, study_info, ObSSGCTabletType::UNKNOWN, tablet_meta_scn_list))) {
            LOG_WARN("fail to append tablet meta scn", KR(ret), K(cur_gc_tablet), K(deleted_scn));
          }
        } else if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle,
                           ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                           ObMDSGetTabletMode::READ_ALL_COMMITED))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            if (OB_FAIL(handle_tablet_not_exist_(
                    cur_gc_tablet, deleted_scn, tablet_meta_scn_list))) {
              LOG_WARN("fail to handle tablet not exist", KR(ret), K(cur_gc_tablet), K(deleted_scn));
            }
          } else {
            LOG_WARN("fail to get tablet", KR(ret), K(cur_gc_tablet), K(deleted_scn));
          }
        } else if (OB_FAIL(handle_tablet_exist_(
                       ls_clog_checkpoint_scn, tablet_handle, cur_gc_tablet,
                       ls_epoch, tablet_meta_scn_list))) {
          LOG_WARN("fail to handle tablet exist", KR(ret), K(cur_gc_tablet), K(deleted_scn));
        }
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to study min tablet meta scn", KR(ret), K(cost_us), "tablet_list_cnt",
      tablet_list.count(), "tablet_list",
      ObArrayWrap<ObSSGCTabletWithDelScn>(tablet_list.get_data(), MIN(10, tablet_list.count())),
      K(tablet_meta_scn_list));
  return ret;
}

int ObSSMarkSweepGCStudyP::handle_ls_not_exist_(
    const ObSSGCTablet &cur_gc_tablet,
    ObIArray<ObSSGCTabletStudyResult> &tablet_meta_scn_list)
{
  int ret = OB_SUCCESS;
  ObSSGCTabletStudyInfo study_info;
  // OB_LS_NOT_EXIST 只表示本次查询时 LS 尚未注册，
  // 不能阻止该 LS 随后完成迁入并写入新的 StudyKey。
  // 此处不做无条件删除，避免迟到请求误删新 LS 实例的位点基线。
  study_info.set_to_max();
  if (OB_FAIL(append_tablet_meta_scn_(
      cur_gc_tablet, study_info, ObSSGCTabletType::UNKNOWN, tablet_meta_scn_list))) {
    LOG_WARN("fail to append tablet meta scn", KR(ret), K(cur_gc_tablet), K(study_info));
  }
  return ret;
}

int ObSSMarkSweepGCStudyP::handle_tablet_not_exist_(
    const ObSSGCTablet &cur_gc_tablet,
    const SCN &deleted_scn,
    ObIArray<ObSSGCTabletStudyResult> &tablet_meta_scn_list)
{
  int ret = OB_SUCCESS;
  ObSSGCTabletStudyInfo study_info;
  if (deleted_scn.is_valid()) {
    // tablet 已经被删除，并且已经在外层检查过 ls_clog_checkpoint_scn >= deleted_scn，所以可以回收所有版本
    study_info.set_to_max();
  } else { // !deleted_scn.is_valid()
    // tablet 尚未被删除，只是暂时未回放到该 tablet，不能回收任何版本
    study_info.set_to_min();
    ObSSGCUtil::report_mark_sweep_set_to_min_event(cur_gc_tablet, "TABLET_NOT_REPLAYED");
  }
  if (FAILEDx(append_tablet_meta_scn_(
      cur_gc_tablet, study_info, ObSSGCTabletType::UNKNOWN, tablet_meta_scn_list))) {
    LOG_WARN("fail to append tablet meta scn", KR(ret), K(cur_gc_tablet), K(study_info));
  }
  return ret;
}

int ObSSMarkSweepGCStudyP::need_collect_lob_rely_info_(
    const ObTablet &tablet,
    bool &need_collect,
    ObSSGCTabletType &gc_table_type)
{
  int ret = OB_SUCCESS;
  schema::ObTableType table_type = schema::MAX_TABLE_TYPE;
  need_collect = false;
  gc_table_type.reset();
  if (OB_FAIL(tablet.get_table_type(table_type))) {
    LOG_WARN("failed to get tablet type for study", KR(ret), K(tablet));
  } else {
    if (schema::is_aux_lob_meta_table(table_type)) {
      gc_table_type = ObSSGCTabletType::LOB_META;
    } else if (schema::is_aux_outtable_lob_control_table(table_type)) {
      gc_table_type = ObSSGCTabletType::LOB_CTRL;
    } else {
      gc_table_type = ObSSGCTabletType::NORMAL;
    }
    need_collect = gc_table_type.is_lob_meta() || gc_table_type.is_lob_ctrl();
  }
  return ret;
}

bool ObSSMarkSweepGCStudyP::update_cold_lob_rely_info_(
    const bool collect_lob_rely_info,
    const int64_t tablet_cnt,
    const bool has_memtable,
    const SCN &ls_clog_checkpoint_scn,
    ObSSGCTabletStudyInfo &study_info)
{
  if (collect_lob_rely_info && 1 == tablet_cnt && !has_memtable
      && study_info.clog_checkpoint_scn_.is_valid_and_not_min()
      && !study_info.clog_checkpoint_scn_.is_max()) {
    study_info.clog_checkpoint_scn_ = ls_clog_checkpoint_scn;
  }
  return collect_lob_rely_info && study_info.is_cold();
}

int ObSSMarkSweepGCStudyP::handle_tablet_exist_(
    const share::SCN &ls_clog_checkpoint_scn,
    const ObTabletHandle &tablet_handle,
    const ObSSGCTablet &cur_gc_tablet,
    const int64_t ls_epoch,
    ObIArray<ObSSGCTabletStudyResult> &tablet_meta_scn_list)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = cur_gc_tablet.ls_id_;
  const ObTabletID &tablet_id = cur_gc_tablet.tablet_id_;
  const SCN &transfer_scn = cur_gc_tablet.transfer_scn_;
  bool allow_tablet_version_gc = false;
  bool is_normal_study = false;
  ObSSGCTabletStudyInfo study_info;
  ObSSGCTabletType table_type = ObSSGCTabletType::UNKNOWN;
  if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is NULL", KR(ret), K(tablet_handle));
  } else if (tablet_handle.get_obj()->get_reorganization_scn() > transfer_scn) {
    LOG_INFO("meet earlier transfer tablet, consider it as deleted", K(cur_gc_tablet), K(tablet_handle));
    study_info.set_to_max();
  } else if (tablet_handle.get_obj()->get_reorganization_scn() < transfer_scn) {
    // 需要等待回放，现在不能回收任何版本
    study_info.set_to_min();
    ObSSGCUtil::report_mark_sweep_set_to_min_event(cur_gc_tablet, "TRANSFER_NOT_REPLAYED");
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    // Refresh may lower min_minor_meta_scn when it links the source SSTables.
    // Wait for it before caching the dependency SCN.
    // During upgrade, an older version may have already persisted a higher GC watermark.
    // GC may still fail temporarily until transfer completes and subsequent flushes/compactions
    // advance the dependency SCN to the old watermark or beyond.
    study_info.set_to_min();
    ObSSGCUtil::report_mark_sweep_set_to_min_event(cur_gc_tablet, "TRANSFER_TABLE_NOT_REFRESHED");
  } else { // tablet_handle.get_obj()->get_reorganization_scn() == transfer_scn
    study_info.set_to_min();  // means can not recycle any versions
    int64_t tablet_cnt = 0;
    bool collect_lob_rely_info = false;
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
    bool tablet_exist = false;
    if (OB_ISNULL(t3m)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObTenantMetaMemMgr is NULL", KR(ret), K(cur_gc_tablet));
    } else if (OB_FAIL(t3m->has_tablet(key, tablet_exist))) {
      LOG_WARN("fail to check tablet existence", KR(ret), K(cur_gc_tablet));
    } else if (!tablet_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("tablet no longer exists before study", KR(ret), K(cur_gc_tablet));
    } else if (tablet_handle.get_obj()->is_empty_shell()) {
      // 空壳 Tablet 没有 StorageSchema，保留 MIN 边界并等待本地 Tablet GC 完成。
      LOG_INFO("skip studying empty shell tablet", K(cur_gc_tablet), K(tablet_handle));
    } else if (OB_FAIL(need_collect_lob_rely_info_(
        *tablet_handle.get_obj(), collect_lob_rely_info, table_type))) {
      LOG_WARN("failed to determine tablet type for study", KR(ret), K(cur_gc_tablet));
    } else {
      bool has_memtable = false;
      // 为兼容既有 Study RPC，min_dep_meta_scn_ 承载 Minor 与 IncMajor 的共同物理依赖下界。
      ScanAllVersionTabletsOp::GetMinTabletRelyInfo op(study_info.min_dep_meta_scn_,
                                                       study_info.min_major_snapshot_,
                                                       study_info.clog_checkpoint_scn_,
                                                       study_info.orig_clog_checkpoint_scn_,
                                                       study_info.multi_version_start_,
                                                       has_memtable,
                                                       collect_lob_rely_info);
      if (OB_FAIL(t3m->scan_all_version_tablets(key, op, tablet_cnt))) {
        LOG_WARN("fail to scan all version tablets", KR(ret), K(cur_gc_tablet));
      } else if (!study_info.min_dep_meta_scn_.is_valid()
                 || !study_info.clog_checkpoint_scn_.is_valid()
                 || !study_info.orig_clog_checkpoint_scn_.is_valid()
                 || study_info.multi_version_start_ < 0) {
        // Note: min_dep_meta_scn_.is_max() may occur when this tablet has not linked any minor.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet study info", KR(ret), K(study_info), K(cur_gc_tablet));
      } else {
        is_normal_study = true;
      }

      if (OB_FAIL(ret)) {
      } else if (update_cold_lob_rely_info_(collect_lob_rely_info, tablet_cnt,
          has_memtable, ls_clog_checkpoint_scn, study_info)) {
        // cold tablet, using ls clog checkpoint scn as rely info
        LOG_INFO("cold tablet, using ls clog checkpoint scn as rely info",
            K(cur_gc_tablet), K(ls_clog_checkpoint_scn), K(study_info));
      } else if (collect_lob_rely_info && study_info.clog_checkpoint_scn_.is_min()) {
        LOG_INFO("lob tablet uses min checkpoint scn as rely info",
            K(cur_gc_tablet), K(study_info));
      }
    }
  }

  // Note: must check check_allow_tablet_version_gc_(flying/external tablets) after scan_all_version_tablets.
  // if there are no flying/external tablets when check_allow_tablet_version_gc_ here, any flying/external
  // tablets created afterwards will have a min tablet meta scn >= the @min_dep_meta_scn studied above.
  // it is guaranteed by scan_all_version_tablets use RLock, and create flying/external tablets use WLock.
  if (FAILEDx(check_allow_tablet_version_gc_(tablet_handle, allow_tablet_version_gc))) {
    LOG_WARN("check allow_tablet_version_gc failed", KR(ret), K(ls_id), K(tablet_id));
  } else if (!allow_tablet_version_gc) {
    LOG_TRACE("meet flying or external tablet version, gc this tablet later",
      K(cur_gc_tablet), K(tablet_handle));
    study_info.set_to_min();
    ObSSGCUtil::report_mark_sweep_set_to_min_event(cur_gc_tablet, "FLYING_OR_EXTERNAL_TABLET");
  } else if (is_normal_study) {
    ObSSGCService *gc_service = MTL(ObSSGCService *);
    if (OB_ISNULL(gc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gc service is null", KR(ret), K(cur_gc_tablet));
    } else if (OB_FAIL(gc_service->check_and_update_local_tablet_study_info(
                   cur_gc_tablet, study_info, ls_epoch))) {
      LOG_WARN("fail to update local tablet study info",
          KR(ret), K(cur_gc_tablet), K(study_info));
    }
  }

  if (FAILEDx(append_tablet_meta_scn_(
      cur_gc_tablet, study_info, table_type, tablet_meta_scn_list))) {
    LOG_WARN("fail to append tablet meta scn", KR(ret), K(cur_gc_tablet), K(study_info));
  }
  return ret;
}

int ObSSMarkSweepGCStudyP::append_tablet_meta_scn_(
    const ObSSGCTablet &cur_gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    const ObSSGCTabletType table_type,
    ObIArray<ObSSGCTabletStudyResult> &tablet_meta_scn_list)
{
  int ret = OB_SUCCESS;
  ObSSGCTabletStudyResult tablet_meta_scn(cur_gc_tablet, study_info, table_type);
  if (OB_FAIL(tablet_meta_scn_list.push_back(tablet_meta_scn))) {
    LOG_WARN("fail to push back tablet meta scn", KR(ret), K(tablet_meta_scn));
  }
  return ret;
}

int ObSSMarkSweepGCStudyP::check_tablets_belong_to_same_ls_(
    const ObIArray<ObSSGCTabletWithDelScn> &tablet_list)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = tablet_list.count();
  if (OB_UNLIKELY(0 == tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet list is empty", KR(ret));
  } else {
    const ObLSID &ls_id = tablet_list.at(0).gc_tablet_.ls_id_;
    for (int64_t i = 1; OB_SUCC(ret) && (i < tablet_cnt); ++i) {
      const ObLSID &cur_ls_id = tablet_list.at(i).gc_tablet_.ls_id_;
      if (OB_UNLIKELY(cur_ls_id != ls_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablets belong to different ls", KR(ret), K(i), K(ls_id), K(cur_ls_id),
            K(tablet_list));
      }
    }
  }
  return ret;
}

// Note: When studying the min tablet meta scn, we study the old_version_chain on tablet_pointer
// as well as the current tablet. Since flying and external tablets do not store the min tablet
// meta scn, it is impossible to study the min tablet meta scn of flying and external tablets,
// so gc must wait until there are no flying or external tablets.
int ObSSMarkSweepGCStudyP::check_allow_tablet_version_gc_(
    const ObTabletHandle &tablet_handle,
    bool &allow_tablet_version_gc)
{
  int ret = OB_SUCCESS;
  allow_tablet_version_gc = false;
  ObTenantMetaMemMgr *t3m = nullptr;
  if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is NULL", KR(ret), K(tablet_handle));
  } else if (OB_ISNULL(t3m = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", KR(ret));
  } else {
    const ObTablet *tablet = tablet_handle.get_obj();
    const int64_t reorganization_scn = tablet->get_reorganization_scn().get_val_for_tx();
    if (OB_FAIL(t3m->check_allow_tablet_gc(tablet->get_tablet_id(),
                                          reorganization_scn,
                                          allow_tablet_version_gc))) {
      LOG_WARN("fail to check allow_tablet_version_gc", KR(ret), K(tablet_handle),
          K(reorganization_scn));
    }
  }
  return ret;
}

}  // namespace obrpc

namespace storage
{

// This function will continuously study min tablet meta scn, until success or a timeout occurs.
int ObSSGCStudyUtil::do_mark_sweep_gc_study(
    const ObLSID &ls_id,
    const ObSSGCTabletWithDelScnArray &gc_tablets,
    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  const int64_t start_us = ObTimeUtility::current_time_us();
  if (OB_UNLIKELY(!ls_id.is_valid() || gc_tablets.empty() || (timeout_us <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), "gc_tablets_empty", gc_tablets.empty(), K(timeout_us));
  } else {
    bool need_retry = true;
    while (need_retry) {
      ret = OB_SUCCESS; // ignore ret
      bool study_need_retry = false;
      if (OB_FAIL(mark_sweep_gc_study(ls_id, gc_tablets, tablet_meta_scn_arr, study_need_retry))) {
        LOG_WARN("failed to study mark sweep gc", KR(ret), K(ls_id));
        if (!study_need_retry) {
          need_retry = false;
        } else if ((ObTimeUtility::current_time_us() - start_us) >= timeout_us) {
          LOG_WARN("timeout to mark sweep gc study", KR(ret), K(ls_id));
          need_retry = false;
          ret = OB_TIMEOUT;
        } else {
          need_retry = true;
          ++retry_times;
          ob_usleep(100 * 1000); // sleep 100ms before next retry
        }
      } else {
        need_retry = false;
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to do mark sweep gc study", KR(ret), K(ls_id),
      "gc_tablets_cnt", gc_tablets.count(), K(retry_times), K(cost_us));
  return ret;
}

int ObSSGCStudyUtil::mark_sweep_gc_study(
    const ObLSID &ls_id,
    const ObSSGCTabletWithDelScnArray &gc_tablets,
    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
    bool &study_need_retry)
{
  int ret = OB_SUCCESS;
  study_need_retry = true;
  SCN ls_deleted_scn;
  const int64_t start_us = ObTimeUtility::current_time_us();
  tablet_meta_scn_arr.reuse();
  ObMemAttr mem_attr(MTL_ID(), "SSMarkSweepGC");
  hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
  ObHashSet<ObAddr> ls_servers;
  const int64_t LS_SERVERS_BUCKET_NUM = 16;
  const int64_t TABLET_INDEX_MAP_BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(
                                              ObGetBucketNumType::TABLET_TYPE);
  bool ls_already_deleted = false;
  ObSSGCStudyDiagInfo::try_reset_ls_study_results(ls_id);
  if (OB_UNLIKELY(!ls_id.is_valid() || gc_tablets.empty())) {
    study_need_retry = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), "gc_tablets_empty", gc_tablets.empty());
  } else if (OB_FAIL(ObSSGCUtil::get_ls_deleted_scn(MTL_ID(), ls_id, ls_deleted_scn))) {
    study_need_retry = ((OB_EAGAIN == ret) || (OB_TIMEOUT == ret));
    LOG_WARN("fail to get ls deleted scn", KR(ret), K(ls_id), K(study_need_retry));
  } else if (OB_FAIL(try_handle_ls_deleted_(ls_id, ls_deleted_scn, gc_tablets,
                                            tablet_meta_scn_arr, ls_already_deleted))) {
    LOG_WARN("fail to try handle ls deleted", KR(ret), K(ls_id));
  } else if (ls_already_deleted) {
    // all tablets already marked as max meta scn, nothing more to do
  } else if (OB_FAIL(tablet_index_map.create(TABLET_INDEX_MAP_BUCKET_NUM, mem_attr))) {
    LOG_WARN("fail to init tablet_index_map", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_servers.create(LS_SERVERS_BUCKET_NUM, mem_attr))) {
    LOG_WARN("fail to create ls_servers hash set", KR(ret), K(ls_id));
  } else if (OB_FAIL(collect_ls_servers_(ls_id, ls_servers))) {
    LOG_WARN("fail to collect ls servers", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(ls_servers.empty())) {
    ret = OB_EAGAIN; // may be deleted after get ls_deleted_scn, double get ls_deleted_scn here again?
    LOG_WARN("no server holds this ls", KR(ret), K(ls_id));
  } else {
    for (ObHashSet<ObAddr>::const_iterator iter = ls_servers.begin();
         OB_SUCC(ret) && (iter != ls_servers.end());
         ++iter) {
      const ObAddr &server = iter->first;
      if (OB_FAIL(batch_process_server_tablets_(server, gc_tablets,
                                                tablet_index_map, tablet_meta_scn_arr))) {
        LOG_WARN("fail to batch process server tablets", KR(ret), K(ls_id), K(server));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ObSSGCStudyDiagInfo::try_reset_ls_study_results(ls_id);
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("finish to collect ss mark sweep gc infos", KR(ret), K(ls_id), K(cost_us),
      K(ls_already_deleted), K(ls_servers));
  return ret;
}

int ObSSGCStudyUtil::collect_ls_servers_(
    const ObLSID &ls_id,
    ObHashSet<ObAddr> &ls_servers)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  GlobalLearnerList learner_list;
  if (OB_FAIL(fetch_ls_member_and_learner_list_(ls_id, member_list, learner_list))) {
    LOG_WARN("fail to fetch ls member and learner list", KR(ret), K(ls_id));
  } else {
    // process learner list
    for (int64_t index = 0; OB_SUCC(ret) && (index < learner_list.get_member_number()); ++index) {
      ObMember learner;
      if (OB_FAIL(learner_list.get_member_by_index(index, learner))) {
        LOG_WARN("fail to get learner by index", KR(ret), K(index));
      } else if (OB_FAIL(ls_servers.set_refactored(learner.get_server()))) {
        LOG_WARN("fail to set ls_servers", KR(ret), K(learner));
      }
    }
    // process member list
    for (int64_t i = 0; OB_SUCC(ret) && (i < member_list.get_member_number()); ++i) {
      ObMember member;
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("fail to get member by index", KR(ret), K(i));
      } else if (OB_FAIL(ls_servers.set_refactored(member.get_server()))) {
        LOG_WARN("fail to set ls_servers", KR(ret), K(member));
      }
    }
  }
  return ret;
}

int ObSSGCStudyUtil::fetch_ls_member_and_learner_list_(
    const ObLSID &ls_id,
    ObMemberList &member_list,
    GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  learner_list.reset();
  obrpc::ObFetchLSMemberAndLearnerListInfo member_and_learner_info;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = MTL_ID();
  ObLocationService *location_service = nullptr;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;
  ObStorageHASrcInfo leader;
  leader.cluster_id_ = cluster_id;

  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret), KP(location_service));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret), KP(ls_service));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc is null", KR(ret), KP(storage_rpc));
  } else if (OB_FAIL(location_service->get_leader(cluster_id, tenant_id, ls_id,
                     true/*force_renew*/, leader.src_addr_))) {
    LOG_WARN("fail to get ls leader", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(storage_rpc->fetch_ls_member_and_learner_list(
      tenant_id, ls_id, leader, member_and_learner_info))) {
    LOG_WARN("fail to fetch ls member and learner list", KR(ret), K(tenant_id), K(ls_id), K(leader));
  } else if (OB_UNLIKELY(!member_and_learner_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member and learner list is invalid", KR(ret), K(ls_id), K(member_and_learner_info));
  } else if (OB_FAIL(member_list.deep_copy(member_and_learner_info.member_list_))) {
    LOG_WARN("fail to copy member list", KR(ret), K(ls_id), K(member_and_learner_info));
  } else if (OB_FAIL(learner_list.deep_copy(member_and_learner_info.learner_list_))) {
    LOG_WARN("fail to copy learner list", KR(ret), K(ls_id), K(member_and_learner_info));
  }
  return ret;
}

int ObSSGCStudyUtil::try_handle_ls_deleted_(
    const ObLSID &ls_id,
    const SCN &ls_deleted_scn,
    const ObSSGCTabletWithDelScnArray &gc_tablets,
    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
    bool &ls_already_deleted)
{
  int ret = OB_SUCCESS;
  ls_already_deleted = false;
  if (ls_deleted_scn.is_valid()) {
    ls_already_deleted = true;
    bool has_undeleted_tablet = false;
    ObSSGCTablet first_undeleted_tablet;
    for (int64_t i = 0; OB_SUCC(ret) && (i < gc_tablets.count()); ++i) {
      const ObSSGCTablet &gc_tablet = gc_tablets.at(i).gc_tablet_;
      const SCN &deleted_scn = gc_tablets.at(i).deleted_scn_;
      // tablet already mark-deleted -> max (can gc all versions);
      // tablet not yet mark-deleted -> min (do not recycle any version yet, safe).
      const SCN min_dep_meta_scn = (deleted_scn.is_valid() ? SCN::max_scn() : SCN::min_scn());
      const int64_t min_major_snapshot = (deleted_scn.is_valid() ? INT64_MAX : 0);
      const SCN clog_checkpoint_scn = (deleted_scn.is_valid() ? SCN::max_scn() : SCN::min_scn());
      const int64_t multi_version_start = (deleted_scn.is_valid() ? INT64_MAX : 0);
      if (!deleted_scn.is_valid() && !has_undeleted_tablet) {
        has_undeleted_tablet = true;
        first_undeleted_tablet = gc_tablet;
        ObSSGCUtil::report_mark_sweep_set_to_min_event(gc_tablet, "LS_DELETED_TABLET_UNDELETED");
      }
      if (OB_FAIL(tablet_meta_scn_arr.push_back(
              ObSSGCTabletStudyResult(
                  gc_tablet, min_dep_meta_scn, min_major_snapshot,
                  clog_checkpoint_scn, multi_version_start)))) {
        LOG_WARN("fail to push back tablet_meta_scn_arr", KR(ret), K(gc_tablet));
      }
    }
    if (OB_SUCC(ret)) {
      // Maintain in-memory tracking of "ls deleted but tablet not deleted" inconsistency.
      // Best-effort: failures here only WARN and never block gc.
      int tmp_ret = OB_SUCCESS;
      ObSSGCService *gc_service = MTL(ObSSGCService *);
      if (OB_ISNULL(gc_service)) {
        LOG_WARN("gc service is null, skip stuck tracking", K(ls_id));
      } else if (has_undeleted_tablet) {
        if (OB_TMP_FAIL(gc_service->mark_del_ls_with_undel_tablet(ls_id))) {
          LOG_WARN("fail to mark del ls with undel tablet", KR(tmp_ret), K(ls_id));
        }
      } else { // !has_undeleted_tablet
        if (OB_TMP_FAIL(gc_service->erase_del_ls_with_undel_tablet(ls_id))) {
          LOG_WARN("fail to erase del ls with undel tablet", KR(tmp_ret), K(ls_id));
        }
      }
    }
    FLOG_INFO("ls has been deleted, mark tablets meta_scn", KR(ret), K(ls_id),
        K(ls_deleted_scn), "tablet_cnt", gc_tablets.count(), K(has_undeleted_tablet),
        K(first_undeleted_tablet));
  }
  return ret;
}

int ObSSGCStudyUtil::batch_process_server_tablets_(
    const ObAddr &server,
    const ObSSGCTabletWithDelScnArray &gc_tablets,
    hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  obrpc::ObSSMarkSweepGCStudyProxy proxy;
  int64_t batch_size = MAX_BATCH_SIZE;
  int64_t consecutive_succ_cnt = 0;
  const int64_t tablet_cnt = gc_tablets.count();
  const int64_t start_us = ObTimeUtility::current_time_us();
  LOG_INFO("start to batch process server tablets", K(server), K(tablet_cnt));

  ObSSGCTabletWithDelScnArray batch_tablets;
  batch_tablets.set_attr(ObMemAttr(MTL_ID(), "SSMarkSweepGC"));
  if (OB_UNLIKELY(0 == tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc tablets is empty", KR(ret), K(server), K(gc_tablets));
  } else if (OB_FAIL(GCTX.net_frame_->get_proxy(proxy))) {
    LOG_WARN("fail to get rpc proxy", KR(ret));
  } else {
    for (int64_t start_idx = 0; OB_SUCC(ret) && start_idx < tablet_cnt; ) {
      batch_tablets.reuse(); // reuse in the beginning of each batch
      const int64_t end_idx = MIN(start_idx + batch_size, tablet_cnt);
      if (OB_FAIL(batch_tablets.reserve(end_idx - start_idx))) {
        LOG_WARN("fail to reserve batch tablets", KR(ret), "capacity", end_idx - start_idx);
      } else {
        for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
          if (OB_FAIL(batch_tablets.push_back(gc_tablets[i]))) {
            LOG_WARN("fail to push back tablet to batch", KR(ret), K(i), K(gc_tablets[i]));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        int64_t rpc_cost_us = 0;
        int batch_ret = send_batch_rpc_request_(server, batch_tablets, proxy,
                                                tablet_index_map, tablet_meta_scn_arr,
                                                rpc_cost_us);
        if (OB_SUCCESS == batch_ret) {
          LOG_INFO("batch rpc request succeed", K(server), K(start_idx), K(end_idx),
              K(batch_size), K(tablet_cnt), K(rpc_cost_us));
          start_idx = end_idx;
          ++consecutive_succ_cnt;
          // Conditional multiplicative increase: need (1) not reach MAX_BATCH_SIZE,
          // (2) enough consecutive successes, (3) latency safely below timeout.
          if ((batch_size < MAX_BATCH_SIZE)
              && (consecutive_succ_cnt >= BATCH_SIZE_GROW_SUCC_THRESHOLD)
              && (rpc_cost_us * BATCH_SIZE_SAFE_LATENCY_RATIO
                  < OB_SS_MARK_SWEEP_GC_STUDY_RPC_TIMEOUT)) {
            const int64_t old_batch_size = batch_size;
            batch_size = MIN(batch_size * BATCH_SIZE_GROW_FACTOR, MAX_BATCH_SIZE);
            consecutive_succ_cnt = 0;
            LOG_INFO("grow batch size", K(server), K(old_batch_size), K(batch_size),
                K(rpc_cost_us), K(tablet_cnt));
          }
        } else if (OB_TIMEOUT == batch_ret) {
          consecutive_succ_cnt = 0;
          if (batch_size > MIN_BATCH_SIZE) {
            const int64_t old_batch_size = batch_size;
            batch_size = MAX(batch_size / BATCH_SIZE_SHRINK_FACTOR, MIN_BATCH_SIZE);
            LOG_WARN("batch rpc timeout, shrink batch size and retry",
                K(batch_ret), K(server), K(start_idx), K(end_idx),
                K(old_batch_size), K(batch_size), K(tablet_cnt));
          } else {
            ret = batch_ret;
            LOG_ERROR("batch rpc timeout even with minimum batch size", KR(ret), K(server),
                K(start_idx), K(end_idx), K(batch_size), K(tablet_cnt));
          }
        } else {
          ret = batch_ret;
          LOG_WARN("batch rpc request failed", KR(ret), K(server), K(start_idx),
              K(end_idx), K(tablet_cnt));
        }
      }
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to batch process server tablets", KR(ret), K(cost_us), K(server),
      K(tablet_cnt), K(batch_size));
  return ret;
}

int ObSSGCStudyUtil::send_batch_rpc_request_(
    const ObAddr &server,
    const ObSSGCTabletWithDelScnArray &batch_tablets,
    obrpc::ObSSMarkSweepGCStudyProxy &proxy,
    hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
    int64_t &rpc_cost_us)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepGCStudyReq request;
  ObSSMarkSweepGCStudyRes result;
  rpc_cost_us = 0;

  if (OB_UNLIKELY(batch_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch tablets is empty", KR(ret), K(server), K(batch_tablets));
  } else if (OB_FAIL(request.tablet_list_.assign(batch_tablets))) {
    LOG_WARN("fail to assign tablet list", KR(ret));
  } else {
    const int64_t rpc_start_us = ObTimeUtility::current_time_us();
    ret = proxy.to(server)
               .by(MTL_ID())
               .timeout(OB_SS_MARK_SWEEP_GC_STUDY_RPC_TIMEOUT)
               .study_min_tablet_meta_scn(request, result);
    rpc_cost_us = ObTimeUtility::current_time_us() - rpc_start_us;
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to send ss mark sweep proxy", KR(ret), K(server), K(rpc_cost_us));
    // Diagnose disabled or collect failure must not affect GC merge or RPC retry; diagnose swallows them.
    } else if (FALSE_IT(ObSSGCStudyDiagInfo::try_save_study_result(
                            server, batch_tablets, result.tablet_meta_scn_list_))) {
    } else if (OB_FAIL(merge_server_meta_scn_infos_(result.tablet_meta_scn_list_,
                                                    tablet_index_map, tablet_meta_scn_arr))) {
      LOG_WARN("fail to merge server meta scn infos", KR(ret), K(request), K(result));
    } else {
      LOG_INFO("succ to collect ss mark sweep gc infos for batch", K(server),
          "batch_tablet_cnt", batch_tablets.count(), K(rpc_cost_us), K(result));
    }
  }

  return ret;
}

int ObSSGCStudyUtil::merge_server_meta_scn_infos_(
    const ObIArray<ObSSGCTabletStudyResult> &cur_tablet_meta_scn_arr,
    hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
    ObSSGCTabletStudyResultArray &final_tablet_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_tablet_meta_scn_arr.count(); i++) {
    const ObSSGCTabletStudyResult &cur_info = cur_tablet_meta_scn_arr.at(i);
    int64_t idx = -1;
    const int hash_ret = tablet_index_map.get_refactored(cur_info.gc_tablet_, idx);
    if (OB_SUCCESS == hash_ret) {
      ObSSGCTabletStudyResult &final_info = final_tablet_meta_scn_arr.at(idx);
      if (final_info.table_type_.is_unknown()) {
        final_info.table_type_ = cur_info.table_type_;
      } else if (!cur_info.table_type_.is_unknown()
          && cur_info.table_type_ != final_info.table_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("inconsistent tablet table type across study servers", KR(ret),
            K(cur_info), K(final_info));
      }
      // take the min of min_dep_meta_scn_ across servers.
      if (cur_info.study_info_.min_dep_meta_scn_.is_min()) {
        final_info.study_info_.min_dep_meta_scn_.set_min();
      } else if (cur_info.study_info_.min_dep_meta_scn_ < final_info.study_info_.min_dep_meta_scn_) {
        final_info.study_info_.min_dep_meta_scn_ = cur_info.study_info_.min_dep_meta_scn_;
      }
      // take the min of min_major_snapshot_ across servers.
      if (cur_info.study_info_.min_major_snapshot_ < final_info.study_info_.min_major_snapshot_) {
        final_info.study_info_.min_major_snapshot_ = cur_info.study_info_.min_major_snapshot_;
      }
      // 计算节点侧的安全位点取全部成员和学习者的最小值。
      if (cur_info.study_info_.clog_checkpoint_scn_
          < final_info.study_info_.clog_checkpoint_scn_) {
        final_info.study_info_.clog_checkpoint_scn_
            = cur_info.study_info_.clog_checkpoint_scn_;
      }
      final_info.study_info_.orig_clog_checkpoint_scn_ = SCN::min(
          final_info.study_info_.orig_clog_checkpoint_scn_,
          cur_info.study_info_.orig_clog_checkpoint_scn_);
      // 多版本起始点同样取全部成员和学习者的最小值。
      if (cur_info.study_info_.multi_version_start_
          < final_info.study_info_.multi_version_start_) {
        final_info.study_info_.multi_version_start_
            = cur_info.study_info_.multi_version_start_;
      }
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      const int64_t new_idx = final_tablet_meta_scn_arr.count();
      if (OB_FAIL(final_tablet_meta_scn_arr.push_back(cur_info))) {
        LOG_WARN("fail to push into final tablet meta scn arr", KR(ret), K(cur_info));
      } else if (OB_FAIL(tablet_index_map.set_refactored(cur_info.gc_tablet_, new_idx))) {
        LOG_WARN("fail to set tablet index map", KR(ret), K(cur_info));
        // keep final_tablet_meta_scn_arr and tablet_index_map consistent
        (void) final_tablet_meta_scn_arr.pop_back();
      }
    } else {
      ret = hash_ret;
      LOG_WARN("fail to get_refactored on tablet_index_map", KR(ret), K(cur_info));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

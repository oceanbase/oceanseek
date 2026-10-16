// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_schedule_tablet_func.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "ob_ss_skip_major_merge_dag.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_ss_tablet_merge_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction_v2/ob_ss_link_major_dag.h"

namespace oceanbase
{
using namespace storage;
namespace compaction
{
ObSSScheduleTabletFunc::ObSSScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
    const bool check_skip_merge,
    const int64_t loop_cnt)
    : ObScheduleTabletFunc(merge_version, merge_reason, loop_cnt),
    check_skip_merge_(check_skip_merge),
    skip_merge_tablet_ids_(),
    skip_major_merge_scn_(0),
    writer_schedule_quota_(nullptr) // MediumLoop 调度不受窗口 Writer quota 限制，nullptr 表示不启用 quota 检查。
{
  skip_merge_tablet_ids_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "SkipMergTablets"));
}

ObSSScheduleTabletFunc::ObSSScheduleTabletFunc(
    const int64_t target_scn,
    int64_t &writer_schedule_quota,
    const int64_t loop_cnt)
    : ObScheduleTabletFunc(target_scn, ObAdaptiveMergePolicy::WINDOW_COMPACTION, loop_cnt,
        COMPACTION_WINDOW_MODE),
    check_skip_merge_(true), // 固定开启检查，LS 批量提交 Skip Major DAG
    skip_merge_tablet_ids_(),
    skip_major_merge_scn_(0),
    writer_schedule_quota_(&writer_schedule_quota)
{
  skip_merge_tablet_ids_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "SkipMergTablets"));
}

int ObSSScheduleTabletFunc::check_with_schedule_scn(
    const ObTablet &tablet,
    const int64_t schedule_scn,
    const ObTabletStatusCache &tablet_status,
    const ObMergeType merge_type,
    bool &can_merge,
    const ObCOMajorMergeStrategy &co_major_merge_strategy)
{
  int ret = OB_SUCCESS;
  can_merge = false;
  if (is_inc_major_merge(merge_type) ?
      tablet.get_last_inc_major_snapshot_version() >= schedule_scn :
      tablet.get_last_major_snapshot_version() >= schedule_scn) {
  } else if (OB_FAIL(ObBasicScheduleTabletFunc::check_with_schedule_scn(tablet, schedule_scn, tablet_status, merge_type, can_merge, co_major_merge_strategy))) {
    LOG_WARN("failed to check with schedule_scn", K(ret), K(schedule_scn), K(ret));
  } else if (can_merge) {
    can_merge = false;
    const ObLSID ls_id = ls_status_.ls_id_;
    const ObTabletID tablet_id = tablet.get_tablet_id();
    ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "SSSchedCheck"));
    ObTabletHandle ss_tablet_handle;
    share::SCN ls_ss_checkpoint_scn;
    palf::LSN ls_ss_checkpoint_lsn;
    share::SCN ss_clog_checkpoint_scn;
    int64_t ss_tablet_snapshot_version = -1;
    bool can_generate_new_tablet = true;
    bool unused_can_gc_sstable = true;
    bool is_sswriter = false;
    if (OB_FAIL(SSCompactHelper::check_is_sswriter(ls_id, tablet_id))) {
      if (OB_NOT_MASTER != ret) {
        LOG_WARN("failed to check is sswriter", KR(ret), K(ls_id), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      is_sswriter = true;
    }
    ObSSMetaUpdateMetaInfo update_meta_info;
    share::SCN meta_scn;
    if (FAILEDx(MTL(ObSSMetaService*)->get_tablet(ls_id, tablet_id, tablet.get_reorganization_scn(), allocator, ss_tablet_handle, update_meta_info, meta_scn))) {
      LOG_WARN("failed to get ss tablet", KR(ret), K(tablet_id));
    } else if (!is_sswriter) {
      int64_t ss_last_major_snapshot_version = update_meta_info.table_store_meta_info_.last_major_snapshot_version_;
      if (is_inc_major_merge(merge_type)) {
        // last_inc_major_snapshot_version 是记录在 ObTabletMeta 上的, 所以预期不需要从 table_store_meta_info_取
        int64_t last_inc_major_snapshot_version = ss_tablet_handle.get_obj()->get_last_inc_major_snapshot_version();
        if (last_inc_major_snapshot_version >= schedule_scn) {
          can_merge = true;
          LOG_TRACE("shared inc major exist, allow merge", K(ls_id), K(tablet_id), K(schedule_scn), K(last_inc_major_snapshot_version));
        } else {
          can_merge = false;
          if (REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
            ADD_SUSPECT_INFO(INC_MAJOR_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                            ls_id,
                            tablet_id,
                            ObSuspectInfoType::SUSPECT_SS_TABLET_MAJOR_LINK_WAIT_SS_MAJOR_READY,
                            schedule_scn,
                            last_inc_major_snapshot_version);
          }
        }
      } else if (ss_last_major_snapshot_version >= schedule_scn) {
        can_merge = true;
        LOG_TRACE("shared major exist, allow merge", K(ls_id), K(tablet_id), K(schedule_scn), K(ss_last_major_snapshot_version));
      } else {
        can_merge = false;
        LOG_WARN("shared major not exist, should wait sswriter to complete merge", K(ls_id), K(tablet_id), K(schedule_scn), K(ss_last_major_snapshot_version));
        if (REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
          ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                          ls_id,
                          tablet_id,
                          ObSuspectInfoType::SUSPECT_SS_TABLET_MAJOR_LINK_WAIT_SS_MAJOR_READY,
                          schedule_scn,
                          ss_last_major_snapshot_version);
        }
      }
    } else if (FALSE_IT(ss_tablet_snapshot_version = ss_tablet_handle.get_obj()->get_snapshot_version())) {
    } else if (FALSE_IT(ss_clog_checkpoint_scn = ss_tablet_handle.get_obj()->get_clog_checkpoint_scn())) {
    } else if (OB_FAIL(ObSSTabletMergeHelper::check_can_generate_new_tablet(ls_id, ss_tablet_handle,
        ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE/*update_reason*/,
        false/*need_check_split*/,
        can_generate_new_tablet,
        unused_can_gc_sstable))) {
      // SS major compaction will not check split info to avoid the sswriter-follower cannot consume remained medium-infos after split-info flag set,
      // sswriter-leader will be refused to execute major-compaction with split-info flag set when updating shared tablet's table store.
      LOG_WARN("failed to check can exec merge", K(ret), K(ls_id), K(tablet_id), K(ss_tablet_handle));
    } else if (!can_generate_new_tablet) {
      can_merge = false;
      if (REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
        ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                        ls_id,
                        tablet_id,
                        ObSuspectInfoType::SUSPECT_SS_TABLET_REORG_SKIP_MAJOR_MERGE,
                        schedule_scn,
                        ss_tablet_snapshot_version,
                        static_cast<int64_t>(ss_clog_checkpoint_scn.get_val_for_tx()));
      }
    } else {
      if (schedule_scn <=
                 ss_tablet_handle.get_obj()->get_snapshot_version()) {
        can_merge = true;
      } else if (ss_clog_checkpoint_scn  < tablet.get_clog_checkpoint_scn()) {
        ObSSLSMeta ls_meta;
        if (OB_FAIL(MTL(ObSSMetaService*)->get_ls_meta(ls_id, ls_meta))) {
          LOG_WARN("failed to get ss ls meta", KR(ret), K(ls_id));
        } else if (OB_FAIL(ls_meta.get_ss_checkpoint(ls_ss_checkpoint_scn, ls_ss_checkpoint_lsn))) {
          LOG_WARN("failed to get ss checkpoint", KR(ret), K(ls_id));
        } else if (ls_ss_checkpoint_scn >= tablet.get_clog_checkpoint_scn()) {
          can_merge = true;
          LOG_INFO("all data of this tablet has been upload for the snapshot, allow merge",
                   K(ls_ss_checkpoint_scn), "local_tablet_clog_checkpoint", tablet.get_clog_checkpoint_scn());
        } else {
          LOG_WARN("need wait tablet upload, can not merge",
                   "shared_tablet_snapshot_version", ss_tablet_snapshot_version,
                  K(schedule_scn),
                  "shared_tablet_clog_checkpoint", ss_clog_checkpoint_scn,
                  "local_tablet_clog_checkpoint", tablet.get_clog_checkpoint_scn(),
                  K(ls_ss_checkpoint_scn),
                  K(tablet_id));
          trigger_tablet_emergency_upload_(tablet_id);
        }
      } else {
        can_merge = true;
        LOG_INFO("shared tablet with larger or equals clog checkpoint of local, allow merge",
                 "shared_tablet_clog_checkpoint", ss_clog_checkpoint_scn,
                 "local_tablet_clog_checkpoint", tablet.get_clog_checkpoint_scn(),
                 "shared_tablet_snapshot_version", ss_tablet_snapshot_version,
                 K(schedule_scn),
                 K(tablet_id));
      }
      if (OB_SUCC(ret) && !can_merge && REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
        ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                        ls_id,
                        tablet_id,
                        ObSuspectInfoType::SUSPECT_SS_TABLET_CANT_MAJOR_MERGE,
                        schedule_scn,
                        ss_tablet_snapshot_version,
                        static_cast<int64_t>(ls_ss_checkpoint_scn.get_val_for_tx()),
                        static_cast<int64_t>(tablet.get_clog_checkpoint_scn().get_val_for_tx()),
                        static_cast<int64_t>(ss_clog_checkpoint_scn.get_val_for_tx()));
      }
    }
  }
  return ret;
}
int ObSSScheduleTabletFunc::schedule_merge_dag(
    const ObLSID &ls_id,
    const ObTablet &tablet, 
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const int64_t merge_base_version,
    const ObCOMajorMergeStrategy &co_major_merge_strategy,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  if (check_skip_merge_ && ObAdaptiveMergePolicy::is_skip_merge_reason(merge_reason)) {
    if (skip_major_merge_scn_ != schedule_scn) {
      if (skip_major_merge_scn_ != 0 && OB_FAIL(post_process_ls())) {
        LOG_WARN("process skip major tablets fail", K(ret), K(skip_major_merge_scn_));
      } else {
        skip_major_merge_scn_ = schedule_scn;
      }
    }
    if (FAILEDx(skip_merge_tablet_ids_.push_back(tablet_id))) {
      LOG_WARN("push back fail", K(ret));
    } else {
      LOG_INFO("merge reason is skip merge", K(ls_id), K(tablet_id), K(merge_reason));
    }
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(ls_id, tablet_id))) {
    if (OB_NOT_MASTER != ret) {
      LOG_WARN("failed to check is sswriter", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(schedule_link_major_dag_(ls_id, tablet, merge_type, schedule_scn, merge_base_version))) {
      LOG_WARN("failed to schedule link major dag", KR(ret), K(ls_id), K(tablet_id), K(merge_type), K(schedule_scn));
    }
  } else if (OB_NOT_NULL(writer_schedule_quota_) && *writer_schedule_quota_ <= 0) {
    ret = OB_SIZE_OVERFLOW;
    LOG_DEBUG("ss window writer schedule quota is exhausted", K(ret), K(ls_id), K(tablet_id), K(schedule_scn));
  } else if (OB_FAIL(ObTenantTabletScheduler::schedule_merge_dag(ls_id, tablet, merge_type, schedule_scn, ObExecMode::EXEC_MODE_OUTPUT, nullptr/*dag_net_id*/, co_major_merge_strategy, merge_base_version))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("schedule major merge dag fail", K(ret));
    }
  } else {
    if (OB_NOT_NULL(writer_schedule_quota_)) {
      --*writer_schedule_quota_;
    }
    LOG_TRACE("schedule major merge dag success", K(ls_id), K(tablet_id), K(merge_type), K(schedule_scn), K(merge_base_version));
    ++tablet_cnt_.schedule_dag_cnt_;
  }
  return ret;
}
int ObSSScheduleTabletFunc::post_process_ls()
{
  int ret = OB_SUCCESS;
  bool dag_exist = false;
  ObUpdateSkipMajorParam param(ls_status_.ls_id_, skip_major_merge_scn_);
  if (OB_FAIL(ObBasicScheduleTabletFunc::post_process_ls())) {
    LOG_WARN("basic schedule post process ls fail", K(ret));
  } else if (skip_merge_tablet_ids_.empty()) {
    LOG_TRACE("no skip merge tablet", "ls_id", ls_status_.ls_id_);
  } else if (skip_major_merge_scn_ == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("skip major merge scn is 0, unexpected", K(ret), K(skip_merge_tablet_ids_));
  } else if (OB_FAIL(param.tablet_info_array_.assign(skip_merge_tablet_ids_))) {
    LOG_WARN("assign tablet ids fail", K(ret), K(skip_merge_tablet_ids_.count()));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->create_and_add_dag<ObSSSkipMajorMergeDag>(&param, false))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to create skip major dag", K(ret), K(param));
    } else if (OB_EAGAIN == ret) {
      // dag equal condition: <dag_type, ls_id, compaction_scn>
      // if dag_map_ already exists, 'create_and_add_dag' will return OB_HASH_EXIST(change to OB_EAGAIN)
      dag_exist = true;
      LOG_TRACE("exists same dag, wait the dag to finish", K(ret), K(param));
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && !skip_merge_tablet_ids_.empty()) {
    LOG_TRACE("finish to process tablet skip major dag", K(ret), K(param), K(dag_exist));
  }
  // 重置是为了避免tablet_id数组跨ls
  skip_merge_tablet_ids_.reset();
  skip_major_merge_scn_ = 0;
  return ret;
}

void ObSSScheduleTabletFunc::trigger_tablet_emergency_upload_(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_status_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_status_.ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_inc_sstable_uploader().schedule_emergency_tablet_upload(tablet_id))) {
    LOG_WARN("schedule emergency tablet upload failed", K(ret), K(tablet_id));
  } else {
    LOG_INFO("schedule emergency tablet upload success", K(ret), K(tablet_id));
  }
}

int ObSSScheduleTabletFunc::schedule_link_major_dag_(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const int64_t merge_base_version)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  ObTabletMergeDagParam param;
  const ObExecMode exec_mode = ObExecMode::EXEC_MODE_OUTPUT;
  if (OB_FAIL(ObDagParamFunc::fill_param(ls_id, tablet, merge_type, schedule_scn, exec_mode, param, merge_base_version))) {
    LOG_WARN("failed to fill param", KR(ret));
  } else {
    if (OB_FAIL(MTL(ObTenantDagScheduler *)->create_and_add_dag<ObLinkSSMajorSSTableDag>(&param, false))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to create link major dag", K(ret), K(param));
      } else if (OB_EAGAIN == ret) {
        LOG_DEBUG("exists same dag, wait the dag to finish", K(ret), K(param));
      }
    } else {
      LOG_DEBUG("success to schedule link major dag", K(ret), K(param));
      ++tablet_cnt_.schedule_dag_cnt_;
    }
  }
  FLOG_INFO("schedule link major dag", K(ret), K(param), K(ls_id), K(tablet_id), K(merge_type), K(schedule_scn), K(merge_base_version));
  return ret;
}
} // namespace compaction
} // namespace oceanbase

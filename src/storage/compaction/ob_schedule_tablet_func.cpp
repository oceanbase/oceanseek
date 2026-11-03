// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_tenant_schema_service.h"

namespace oceanbase
{
using namespace storage;
namespace compaction
{
ERRSIM_POINT_DEF(EN_COMPACTION_SKIP_CS_REPLICA_TO_REBUILD);
ERRSIM_POINT_DEF(ERRSIM_BLOCK_ASYNC_INDEX_TABLET_COMPACTION,
    "Defer async index compaction DAG scheduling; match specifies the index tablet id.");
/********************************************ObScheduleTabletFunc impl******************************************/

#define DIAGNOSE_TABLET_MEDIUM_MERGE(ls_id, tablet_id) \
  { int tmp_ret = OB_SUCCESS; \
    if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(ls_id, tablet_id, \
        share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) { \
      LOG_WARN("failed to add diagnose tablet", KR(tmp_ret), K(ls_id), K(tablet_id)); \
    } \
  }

ObScheduleTabletFunc::ObScheduleTabletFunc(
  const int64_t merge_version,
  const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
  const int64_t loop_cnt,
  const ObCompactionScheduleMode schedule_mode)
  : ObBasicScheduleTabletFunc(merge_version, loop_cnt),
    tablet_status_(tenant_status_snapshot_),
    time_guard_(),
    clear_stat_tablets_(),
    merge_reason_(merge_reason),
    schedule_mode_(schedule_mode),
    async_delete_published_version_(-1)
{
  clear_stat_tablets_.set_attr(ObMemAttr(MTL_ID(), "BatchClearTblts"));
}

int ObScheduleTabletFunc::switch_and_schedule_tablet(
    ObTabletHandle &tablet_handle,
    bool &tablet_merge_finish)
{
  int ret = OB_SUCCESS;
  bool unused_can_merge = false;
  bool need_schedule = false;
  if (OB_FAIL(iterate_switch_tablet(tablet_handle, unused_can_merge, need_schedule))) {
    LOG_WARN("failed to iterate switch tablet", KR(ret), K(tablet_handle));
  } else if (!need_schedule) {
    tablet_merge_finish = true;
    tablet_cnt_.finish_cnt_++;
  } else if (OB_FAIL(schedule_tablet(tablet_handle, tablet_merge_finish))) {
    LOG_WARN("failed to schedule tablet", KR(ret), K(tablet_handle));
  }
  destroy_tablet_status();
  return ret;
}

int ObScheduleTabletFunc::iterate_switch_tablet(
    storage::ObTabletHandle &tablet_handle,
    bool &can_merge,
    bool &need_schedule)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  ObTablet *tablet = nullptr;
  ObTabletID tablet_id;
  bool skip_async_index_schedule = false;
  can_merge = false;
  need_schedule = false;
  destroy_tablet_status();
  if (OB_UNLIKELY(!ls_status_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls status", KR(ret), K_(ls_status));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
  } else if (OB_UNLIKELY(ls_status_.ls_id_ != tablet->get_ls_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet and ls are not match", KR(ret), K_(ls_status), KPC(tablet));
  } else if (!tablet->get_tablet_addr().is_sslog()) {
    need_schedule = true;
  } else {
    int64_t max_medium_snapshot = 0;
    if (tablet->get_last_major_snapshot_version() < merge_version_) {
      need_schedule = true;
    } else if (tablet->get_last_major_snapshot_version() >= merge_version_) {
      if (OB_FAIL(tablet->get_max_sync_medium_scn(max_medium_snapshot))) {
        LOG_WARN("failed to get max sync medium scn", K(ret), KPC(tablet));
      } else if (max_medium_snapshot > tablet->get_last_major_snapshot_version()) {
        need_schedule = true;
      // 租户合并未结束时，主表仍需检查关联的异步索引
      } else if (merge_version_ > MERGE_SCHEDULER_PTR->get_merged_version()
          && !tablet->is_async_index()) {
        bool has_async_index = false;
        if (OB_FAIL(tablet->get_has_async_index(has_async_index))) {
          LOG_WARN("failed to get authoritative async-index membership", KR(ret), K(tablet_id));
        } else {
          need_schedule = has_async_index;
        }
      } else {
        need_schedule = false;
        LOG_DEBUG("tablet no need to schedule medium", KPC(tablet));
      }
    }
    LOG_DEBUG("tablet schedule medium", KPC(tablet), K(need_schedule),
        K(max_medium_snapshot), K(merge_version_), "last major snapshot version", tablet->get_last_major_snapshot_version());
  }
  
  if (OB_FAIL(ret)) {
  } else if (!need_schedule) {
    LOG_DEBUG("no need schedule tablet merge", KPC(tablet));
  } else if (tablet->is_async_index()
      && OB_FAIL(check_async_index_tablet_status_for_schedule_(*tablet, skip_async_index_schedule))) {
    LOG_WARN("failed to check async index tablet status", KR(ret), K(tablet_id));
  } else if (skip_async_index_schedule) {
    need_schedule = false;
    LOG_DEBUG("async index tablet does not need medium merge schedule", K(tablet_id), K_(ls_status));
  } else if (OB_FAIL(prepare_async_delete_index_tablet_(tablet_handle))) {
    LOG_WARN("failed to prepare deletion tablet for schedule", KR(ret), K(tablet_id));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet_status_.init_for_major(
                 ls_status_.get_ls(), merge_version_, *tablet,
                 ls_could_schedule_new_round_))) {
    LOG_WARN("failed to init tablet status", KR(ret), K_(ls_status), K(tablet_id));
    DIAGNOSE_TABLET_MEDIUM_MERGE(ls_id, tablet_id);
  } else {
    can_merge = tablet_status_.can_merge();
  }
  return ret;
}

// when call schedule_tablet, ls status have been checked
// in window compaction, tablet_status is pre-loaded when iterate_switch_tablet, so don't need to load tablet status again
int ObScheduleTabletFunc::schedule_tablet(
  ObTabletHandle &tablet_handle,
  bool &tablet_merge_finish)
{
  tablet_merge_finish = false;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  ObTablet *tablet = nullptr;
  ObTabletID tablet_id;
  bool need_diagnose = false;
  time_guard_.click(ObCompactionScheduleTimeGuard::GET_TABLET);
  tablet_cnt_.loop_tablet_cnt_++;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
  } else if (OB_UNLIKELY(!tablet_status_.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is not inited", KR(ret), K_(tablet_status));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
  } else {
    time_guard_.click(ObCompactionScheduleTimeGuard::INIT_TABLET_STATUS);
    if (tablet_status_.tablet_merge_finish()) {
      tablet_merge_finish = true;
      tablet_cnt_.finish_cnt_++;
    }
    if (tablet_status_.could_schedule_new_round() && OB_TMP_FAIL(try_schedule_tablet_new_round(tablet_handle))) {
      need_diagnose = true;
      LOG_WARN("failed to schedule tablet new round", KR(tmp_ret), K_(ls_status), K(tablet_id));
    }
    if (!tablet_status_.can_merge()) {
      need_diagnose = tablet_status_.need_diagnose();
    } else if (ls_could_schedule_merge_ && OB_TMP_FAIL(schedule_tablet_execute(*tablet))) {
      need_diagnose = true;
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule tablet execute", KR(tmp_ret), K_(ls_status), K(tablet_id));
      }
    } else {
      LOG_DEBUG("success to schedule tablet execute", KR(tmp_ret), K(tablet_id), K_(ls_status), K(tablet_status_), K_(ls_could_schedule_merge));
    }
  }
  if (need_diagnose) {
    DIAGNOSE_TABLET_MEDIUM_MERGE(ls_id, tablet_id);
  }
  schedule_freeze_dag(false/*force*/);
  return ret;
}

int ObScheduleTabletFunc::try_schedule_tablet_new_round(
    storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  bool need_schedule_new_round = false;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
  } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_id())) {
  } else if (is_window_compaction_func()) {
    need_schedule_new_round = true;
  } else if (is_window_compaction_active_) {
    ObWindowLoop &window_loop = MTL(ObTenantTabletScheduler *)->get_window_loop();
    if (ls_status_.is_leader_ && OB_FAIL(window_loop.process_tablet_stat(*tablet_handle.get_obj()))) {
      if (OB_NOT_INIT == ret) {
        // window compaction is stopped, schedule tablet as normal
        ret = OB_SUCCESS;
        need_schedule_new_round = true;
      } else {
        LOG_WARN("failed to loop tablet in medium loop", KR(ret), K(tablet_id));
      }
    }
  } else {
    need_schedule_new_round = true;
  }

  if (OB_FAIL(ret)) {
  } else if (need_schedule_new_round && OB_FAIL(schedule_tablet_new_round(tablet_handle, false/*user_request*/))) {
    LOG_WARN("failed to schedule tablet new round", KR(ret), K_(ls_status), K(tablet_id));
  }
  return ret;
}

int ObScheduleTabletFunc::schedule_tablet_new_round(
  ObTabletHandle &tablet_handle,
  const bool user_request)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
  bool medium_clog_submitted = false;
  bool has_async_index = false;

  // 主表未完成时本来就会继续调度；此处仅在主表已完成时，才检查是否需要继续调度其异步索引。
  if (!user_request
      && merge_version_ > MERGE_SCHEDULER_PTR->get_merged_version()
      && tablet_status_.tablet_merge_finish()
      && !tablet_handle.get_obj()->is_async_index()) {
    if (OB_FAIL(tablet_handle.get_obj()->get_has_async_index(has_async_index))) {
      LOG_WARN("failed to get authoritative async-index membership", KR(ret), K(ls_id), K(tablet_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tablet_status_.medium_list())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium list in tablet status is null", KR(ret), K_(tablet_status));
  } else if (!tablet_status_.tablet_merge_finish()
           || user_request
           || (!is_window_compaction_func()
               && MERGE_SCHEDULER_PTR->get_tenant_status().enable_adaptive_compaction_with_cpu_load())
           || has_async_index) {
    ObMediumCompactionScheduleFunc func(ls_status_.get_ls(),
                                        ls_status_.weak_read_ts_,
                                        *tablet_status_.medium_list(),
                                        &tablet_cnt_,
                                        merge_reason_,
                                        0 /*least_medium_snapshot*/,
                                        get_window_decision_log_info());
    if (OB_FAIL(func.init_tablet_handle(tablet_handle, async_delete_published_version_))) {
      LOG_WARN("failed to init func with tablet_handle", K(ret), K(tablet_handle), K(func));
    // 主表已完成但异步索引未完成时，继续携带租户合并位点推进异步索引。
    } else if (OB_FAIL(func.schedule_next_medium_for_leader(
        tablet_status_.tablet_merge_finish() && !has_async_index ? 0 : merge_version_, medium_clog_submitted))) {
      if (OB_NOT_MASTER == ret) {
        ls_status_.is_leader_ = false;
        ls_could_schedule_new_round_ = false;
      } else {
        LOG_WARN("failed to schedule next medium", K(ret), K_(ls_status), K(tablet_id));
      }
    } else if (medium_clog_submitted) {
      if (OB_TMP_FAIL(clear_stat_tablets_.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet_id for batch_freeze", KR(tmp_ret), K_(ls_status), K(tablet_id));
      }
    }
    time_guard_.click(ObCompactionScheduleTimeGuard::SCHEDULE_NEXT_MEDIUM);
  }
  return ret;
}

int ObScheduleTabletFunc::prepare_async_delete_index_tablet_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  async_delete_published_version_ = -1;
  if (OB_UNLIKELY(!tablet_handle.is_valid() || !ls_status_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deletion schedule input", KR(ret), K(tablet_handle), K_(ls_status));
  } else if (ls_could_schedule_new_round_ && tablet_handle.get_obj()->is_async_delete_index()) {
    const ObTabletID tablet_id = tablet_handle.get_obj()->get_tablet_id();
    int32_t tablet_epoch = -1;
    int64_t published_version = 0;
    ObTabletHandle current_handle;
    // Read P before acquiring the owner of both the medium list and the merge inputs.
    if (OB_FAIL(tablet_handle.get_obj()->get_private_transfer_epoch(tablet_epoch))) {
      LOG_WARN("failed to get deletion transfer epoch", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_latest_async_index_published_version(published_version))) {
      LOG_WARN("failed to get deletion published version", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ls_status_.get_ls().get_tablet(tablet_id, current_handle))) {
      LOG_WARN("failed to get deletion tablet after publication", KR(ret), K(tablet_id));
    } else if (OB_UNLIKELY(!current_handle.is_valid()
        || !current_handle.get_obj()->is_async_delete_index())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("deletion tablet changed during preparation", KR(ret), K(tablet_id), K(tablet_epoch));
    } else if (OB_FAIL(ObTablet::check_private_transfer_epoch_equal(
        *current_handle.get_obj(), tablet_epoch))) {
      LOG_WARN("deletion transfer epoch changed during preparation", KR(ret), K(tablet_id), K(tablet_epoch));
    } else if (OB_FAIL(tablet_handle.assign(current_handle))) {
      LOG_WARN("failed to hold deletion schedule tablet", KR(ret), K(tablet_id));
    } else {
      async_delete_published_version_ = published_version;
    }
  }
  return ret;
}

int ObScheduleTabletFunc::set_merge_reason(const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObAdaptiveMergePolicy::is_valid_merge_reason(merge_reason))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge reason", K(ret), K(merge_reason));
  } else {
    merge_reason_ = merge_reason;
  }
  return ret;
}

// Check whether a pending async-index medium can be scheduled.
// Recyclebin indexes retain live tablets and must continue pending medium merges.
//   - Schedulable status, skip_schedule=false, return OB_SUCCESS
//   - Committed deleted-for-GC, skip_schedule=true, return OB_SUCCESS
//   - Deleted table mapping or missing latest schema, skip_schedule=true, return OB_SUCCESS
//   - MDS or schema lookup failed, return other errors
int ObScheduleTabletFunc::check_async_index_tablet_status_for_schedule_(
    const ObTablet &tablet,
    bool &skip_schedule)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  const uint64_t tenant_id = MTL_ID();
  int64_t schema_version = 0;
  uint64_t table_id = OB_INVALID_ID;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *index_schema = nullptr;
  skip_schedule = false;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet.ObITabletMdsInterface::get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version))) {
    LOG_WARN("failed to get latest async index tablet status", KR(ret), K(tablet_id));
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_stat
             && user_data.tablet_status_.is_deleted_for_gc()) {
    skip_schedule = true;
    LOG_TRACE("async index tablet is deleted and delete transaction is committed, skip medium merge schedule",
        K(tablet_id), K(trans_stat), K(trans_version), K(user_data));
  } else if (OB_ISNULL(schema_service = MTL(share::schema::ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet.get_schema_version_from_storage_schema(schema_version))) {
    LOG_WARN("failed to get async index schema version", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_table_id(
      *schema_service, tablet_id, schema_version, table_id))) {
    if (OB_TABLE_IS_DELETED == ret) {
      ret = OB_SUCCESS;
      skip_schedule = true;
      LOG_TRACE("async index table is deleted, skip medium merge schedule",
          K(tablet_id), K(schema_version));
    } else {
      LOG_WARN("failed to get async index table id", KR(ret), K(tablet_id), K(schema_version));
    }
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
    LOG_WARN("failed to get async index schema", KR(ret), K(tenant_id), K(table_id), K(tablet_id));
  } else if (OB_ISNULL(index_schema)) {
    skip_schedule = true;
    LOG_TRACE("async index schema is null, skip medium merge schedule", K(table_id), K(tablet_id));
  }
  return ret;
}

int ObScheduleTabletFunc::request_schedule_async_index_new_round(ObTabletHandle &data_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *data_tablet = nullptr;
  ObTabletID tablet_id;
  bool has_async_index = false;
  if (OB_UNLIKELY(!ls_status_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls status", KR(ret), K_(ls_status));
  } else if (!ls_status_.is_leader_) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("not ls leader, skip async index medium wakeup", K(ret), K_(ls_status));
  } else if (!ls_could_schedule_new_round_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant or ls can not schedule async index medium wakeup", K(ret), K_(ls_status), K_(ls_could_schedule_new_round));
  } else if (OB_UNLIKELY(!data_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data tablet handle", KR(ret), K(data_tablet_handle));
  } else if (OB_ISNULL(data_tablet = data_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data tablet is null", KR(ret), K(data_tablet_handle));
  } else if (FALSE_IT(tablet_id = data_tablet->get_tablet_id())) {
  } else if (OB_UNLIKELY(!data_tablet->is_user_data_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async index medium wakeup must use data tablet", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(data_tablet->get_ls_id() != ls_status_.ls_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data tablet and ls are not match", KR(ret), K(tablet_id), K_(ls_status), KPC(data_tablet));
  } else if (OB_FAIL(data_tablet->get_has_async_index(has_async_index))) {
    LOG_WARN("failed to get authoritative async index membership", KR(ret), K(tablet_id));
  } else if (!has_async_index) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("data tablet has no async index, skip medium wakeup", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::schedule_for_async_index_only(ls_status_.get_ls(),
                                                                                   ls_status_.weak_read_ts_,
                                                                                   data_tablet_handle,
                                                                                   &tablet_cnt_))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to schedule async index new round", KR(ret), K_(ls_status), K(tablet_id));
    }
  }
  return ret;
}

int ObScheduleTabletFunc::request_schedule_new_round(
  ObTabletHandle &tablet_handle,
  const bool user_request,
  const bool need_load_tablet_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  ObTablet *tablet = nullptr;
  ObTabletID tablet_id;
  bool schedule_flag = false;
  bool skip_async_index_schedule = false;
  if (OB_UNLIKELY(!ls_status_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls status", KR(ret), K_(ls_status));
  } else if (!ls_status_.is_leader_) {
    // not leader, can't schedule
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("not ls leader, can't schedule medium", K(ret), K_(ls_status), K_(ls_status), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
  } else if (tablet->is_async_delete_index()
      && OB_FAIL(check_async_index_tablet_status_for_schedule_(*tablet, skip_async_index_schedule))) {
    LOG_WARN("failed to check deletion tablet status", KR(ret), K(tablet_id));
  } else if (skip_async_index_schedule) {
    // A direct medium request must not bypass the DROP checks in the periodic scheduler.
  } else if (need_load_tablet_status
      && OB_FAIL(prepare_async_delete_index_tablet_(tablet_handle))) {
    LOG_WARN("failed to prepare deletion tablet for request", KR(ret), K(tablet_id));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (need_load_tablet_status && OB_FAIL(tablet_status_.init_for_major(
                 ls_status_.get_ls(), merge_version_, *tablet,
                 ls_could_schedule_new_round_))) {
    LOG_WARN("failed to init tablet status", KR(ret), K_(ls_status), K(tablet_id));
  } else if (tablet->is_async_inverted_storage_index()
             && ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE != merge_reason_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async index tablet can only be scheduled via schedule_async_index on data tablet",
        K(ret), K_(ls_status), K(tablet_id));
  } else if (ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE == merge_reason_) {
    // async index force merge, only schedule for async index, not schedule for data tablet
    ObMediumCompactionScheduleFunc func(ls_status_.get_ls(),
                                        ls_status_.weak_read_ts_,
                                        *tablet_status_.medium_list(),
                                        &tablet_cnt_,
                                        merge_reason_,
                                        0 /*least_medium_snapshot*/,
                                        get_window_decision_log_info());
    if (OB_FAIL(func.init_tablet_handle(tablet_handle))) {
      LOG_WARN("failed to init func with tablet_handle", K(ret), K(tablet_handle), K(func));
    } else if (OB_FAIL(func.schedule_async_index())) {
      if (OB_NO_NEED_MERGE == ret) {
        LOG_INFO("[ASYNC_INDEX] merge already scheduled or not needed", K(ret), K_(ls_status), K(tablet_id), K(tablet_handle), K(func));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to schedule for async index", K(ret), K_(ls_status), K(tablet_id), K(tablet_handle), K(func));
      }
    } else {
      LOG_INFO("success to schedule for async index", K(ret), K_(ls_status), K(tablet_id), K(tablet_handle), K(func));
    }
  } else if (user_request) { // should print error log for user request
    if (!tablet_status_.tablet_merge_finish()) {
      ret = OB_MAJOR_FREEZE_NOT_FINISHED;
      LOG_WARN("no major sstable or not finish tenant major compaction, can't schedule another medium",
        K(ret), K_(ls_status), K(tablet_id), K_(merge_version), K_(tablet_status));
    } else if (!tablet_status_.can_merge()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tablet status can't merge now",
        K(ret), K_(ls_status), K(tablet_id), K_(merge_version), K_(tablet_status));
    } else if (!tablet_status_.could_schedule_new_round()) {
      ret = OB_MAJOR_FREEZE_NOT_FINISHED;
      LOG_WARN("tablet need check finish, can't schedule another medium", K(ret), K_(ls_status), K(tablet_id),
        K_(tablet_status));
    } else {
      schedule_flag = true;
    }
  } else {
    schedule_flag = tablet_status_.tablet_merge_finish() && tablet_status_.could_schedule_new_round();
  }
  if (OB_SUCC(ret) && schedule_flag && OB_TMP_FAIL(schedule_tablet_new_round(tablet_handle, user_request))) {
    LOG_WARN("failed to schedule tablet new round", KR(tmp_ret), K_(ls_status), K(tablet_id));
  }
  if (need_load_tablet_status) {
    destroy_tablet_status();
  }
  return ret;
}

int ObScheduleTabletFunc::schedule_tablet_execute(ObTablet &tablet)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_MEDIUM_CREATE_DAG) ret;
  if (OB_FAIL(ret)) {
    FLOG_INFO("ERRSIM EN_MEDIUM_CREATE_DAG", K(ret));
    return ret;
  }
  if (OB_SUCC(ret) && ls_status_.get_ls().is_cs_replica()) {
    ret = EN_COMPACTION_SKIP_CS_REPLICA_TO_REBUILD;
    if (OB_FAIL(ret)) {
      LOG_INFO("ERRSIM EN_COMPACTION_SKIP_CS_REPLICA_TO_REBUILD", K(ret));
      return ret;
    }
  }
#endif
  const ObLSID &ls_id = ls_status_.ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  bool can_merge = false;
  // A zero schedule SCN skips the unfinished incremental major merge check.
  int64_t schedule_scn = 0;
  int64_t merge_base_version = 0;
  ObCOMajorMergeStrategy co_major_merge_strategy;
  ObCSReplicaTabletStatus cs_replica_status = ObCSReplicaTabletStatus::NORMAL;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::INVALID_REASON;
  ObMediumCompactionInfo::ObCompactionType compaction_type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
  ObMergeType merge_type = INVALID_MERGE_TYPE;

  if (tablet.is_async_inverted_storage_index()
      && OB_SUCCESS != EVENT_CALL(ERRSIM_BLOCK_ASYNC_INDEX_TABLET_COMPACTION, tablet_id.id())) {
    ret = OB_EAGAIN;
    LOG_INFO("ERRSIM defer async index tablet compaction", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ObTenantTabletScheduler::check_ready_for_major_merge(
                 ls_id, tablet, MEDIUM_MERGE, schedule_scn, cs_replica_status))) {
    LOG_WARN("failed to check ready for major merge", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(get_schedule_execute_info(tablet, schedule_scn, merge_base_version, co_major_merge_strategy, merge_reason, compaction_type))) {
    if (OB_NO_NEED_MERGE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get schedule execute info", KR(ret), K_(ls_status), K(tablet_id));
    }
  } else if (OB_FALSE_IT(merge_type = ObMediumCompactionInfo::INC_MAJOR_COMPACTION == compaction_type ? INC_MAJOR_MERGE : MEDIUM_MERGE)) {
  } else if (OB_FAIL(check_with_schedule_scn(tablet, schedule_scn, tablet_status_, merge_type, can_merge, co_major_merge_strategy))) {
    LOG_WARN("failed to check with schedule scn", KR(ret), K(schedule_scn));
  } else if (can_merge) {
    if (OB_FAIL(schedule_merge_dag(ls_id, tablet, merge_type, schedule_scn, merge_base_version, co_major_merge_strategy, merge_reason))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule medium merge dag", K(ret), K_(ls_status), K(tablet_id));
      }
    } else {
      LOG_DEBUG("success to schedule medium merge dag", K(ret), K(schedule_scn), K(merge_reason), K_(ls_status), K(tablet_id));
    }
  }
  time_guard_.click(ObCompactionScheduleTimeGuard::SCHEDULE_TABLET_EXECUTE);
  return ret;
}

int ObScheduleTabletFunc::get_schedule_execute_info(
    ObTablet &tablet,
    int64_t &schedule_scn,
    int64_t &merge_base_version,
    ObCOMajorMergeStrategy &co_major_merge_strategy,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObMediumCompactionInfo::ObCompactionType &compaction_type)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_id();

  bool schedule_flag = false;
  const bool is_standby_window_consumer = share::is_standby_tenant(MTL_GET_TENANT_ROLE_CACHE());
  const bool is_standy_tenant = !MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID();
  const int64_t frozen_version = ObBasicMergeScheduler::get_merge_scheduler()->get_frozen_version();
  const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version();
  const int64_t last_inc_major_snapshot = tablet.get_last_inc_major_snapshot_version();
  schedule_scn = 0; // medium_snapshot in medium info
  merge_base_version = 0;
  bool is_mv_major_refresh_tablet = false;
  bool is_restore_remote_sstable_tablet = tablet.get_tablet_meta().ha_status_.is_restore_status_remote();
  const ObMediumCompactionInfoList *medium_list = nullptr;

  if (OB_ISNULL(tablet_status_.medium_list())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium list in tablet status is null", KR(ret), K_(tablet_status));
  } else if (OB_FAIL(tablet.check_is_mv_major_refresh_tablet(is_mv_major_refresh_tablet))) {
    LOG_WARN("check mv major_refresh tablet failed", KR(ret));
  } else if (is_mv_major_refresh_tablet &&
             ObBasicMergeScheduler::INIT_COMPACTION_SCN == last_major_snapshot) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                                     ls_id, tablet_id, ObSuspectInfoType::SUSPECT_MV_IN_CREATION,
                                     last_major_snapshot,
                                     static_cast<int64_t>(tablet.is_row_store())))) {
      LOG_WARN("failed to add suspect info", K(tmp_ret));
    }
    LOG_INFO("mv creation has not finished, can not schedule mv tablet", K(ret),
             K(last_major_snapshot));
  } else if (OB_FAIL(tablet_status_.medium_list()->get_next_schedule_info(
      last_major_snapshot,
      last_inc_major_snapshot,
      // 备租户消费已重放记录，以本地 frozen 为准，不依赖窗口是否启动过 MediumLoop。
      is_standby_window_consumer ? frozen_version : merge_version_,
      is_mv_major_refresh_tablet || is_restore_remote_sstable_tablet,
      compaction_type,
      schedule_scn,
      merge_base_version,
      co_major_merge_strategy,
      merge_reason))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get next schedule info", KR(ret), K(tablet_id), KPC(tablet_status_.medium_list()), K(last_major_snapshot), K_(merge_version));
    }
  } else if (ObAdaptiveMergePolicy::is_window_merge_reason(merge_reason)
      && !is_window_compaction_func()
      && !is_standby_window_consumer) {
    // 主租户窗口任务交给 SSWindowLoop；备租户沿用下方检查消费重放记录，不受窗口开关限制。
  } else if (is_standy_tenant && !is_mv_major_refresh_tablet && !is_restore_remote_sstable_tablet) { // for STANDBY/RESTORE TENANT
    if (OB_FAIL(ObMediumCompactionScheduleFunc::decide_standy_tenant_schedule(
        ls_id, tablet_id, compaction_type, schedule_scn, frozen_version, *tablet_status_.medium_list(), schedule_flag))) {
      LOG_WARN("failed to decide whehter to schedule standy schedule", K(ret), K_(ls_status), K(tablet_id),
        K(compaction_type), K(schedule_scn), K(frozen_version), K(tablet_status_));
    }
  } else {
    schedule_flag = true;
  }
  if (OB_SUCC(ret) && !schedule_flag) {
    LOG_DEBUG("tablet no need to schedule", KR(ret), K_(ls_status), K(tablet_status_), K(schedule_flag));
    ret = OB_NO_NEED_MERGE;
  }
  return ret;
}

void ObScheduleTabletFunc::schedule_freeze_dag(const bool force)
{
  int tmp_ret = OB_SUCCESS;
  IGNORE_RETURN ObBasicScheduleTabletFunc::schedule_freeze_dag(force);
  if (clear_stat_tablets_.empty()) {
  } else if (force || clear_stat_tablets_.count() > SCHEDULE_DAG_THREHOLD) {
    if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->batch_clear_tablet_stat(ls_status_.ls_id_, clear_stat_tablets_))) {
      LOG_WARN_RET(tmp_ret, "failed to batch clear tablet stats", K(ls_status_.ls_id_));
    }
    clear_stat_tablets_.reset();
  }
}

int ObScheduleTabletFunc::diagnose_switch_tablet(
    ObLS &ls,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  destroy_tablet_status();
  if (OB_FAIL(tablet_status_.init_for_diagnose(ls, merge_version_, tablet))) {
    LOG_WARN("failed to init tablet status", KR(ret), K_(merge_version), K(ls_status_), K(tablet_id));
  }
  return ret;
}

int ObScheduleTabletFunc::check_with_schedule_scn(
    const ObTablet &tablet,
    const int64_t schedule_scn,
    const ObTabletStatusCache &tablet_status,
    const ObMergeType merge_type,
    bool &can_merge,
    const ObCOMajorMergeStrategy &co_major_merge_strategy)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this function can not be used in shared-storage mode", K(ret));
  } else if (OB_FAIL(ObBasicScheduleTabletFunc::check_with_schedule_scn(tablet, schedule_scn, tablet_status, merge_type, can_merge, co_major_merge_strategy))) {
    LOG_WARN("check with schedule scn fail", K(ret));
  }
  return ret;
}

int ObScheduleTabletFunc::schedule_merge_dag(
    const ObLSID &ls_id,
    const ObTablet &tablet, 
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const int64_t merge_base_version,
    const ObCOMajorMergeStrategy &co_major_merge_strategy,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  UNUSED(merge_reason);
  if (is_major_merge(merge_type)) {
    DEBUG_SYNC(BEFORE_SCHEDULE_TABLET_FUNC);
  }
  if (GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this functor can not be used in shared-storage mode", K(ret));
  } else if (OB_FAIL(ObTenantTabletScheduler::schedule_merge_dag(ls_id, tablet, merge_type, schedule_scn, ObExecMode::EXEC_MODE_LOCAL, nullptr/*dag_net_id*/, co_major_merge_strategy, merge_base_version))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("schedule major merge dag fail", K(ret));
    }
  } else {
    LOG_TRACE("schedule major merge dag success", K(ls_id), "tablet_id", tablet.get_tablet_id(), K(merge_type), K(schedule_scn));
    ++tablet_cnt_.schedule_dag_cnt_;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

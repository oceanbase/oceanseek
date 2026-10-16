// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_skip_major_merge_dag.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_schedule_status_cache.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/incremental/ob_sstable_utils.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/ob_storage_schema_util.h"

namespace oceanbase
{
namespace compaction
{
ERRSIM_POINT_DEF(EN_UPDATE_SKIP_MAJOR_DAG_FAILED);
ObSSSkipMajorMergeTask::ObSSSkipMajorMergeTask()
  : ObBatchExecTask(ObITask::TASK_TYPE_UPDATE_SKIP_MAJOR_TABLET),
    update_cnt_()
{
}

int ObSSSkipMajorMergeTask::inner_process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  const ObUpdateSkipMajorParam &param = base_dag_->get_param();
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param.ls_id_));
  } else if (OB_UNLIKELY(!ObLSStatusCache::check_weak_read_ts_ready(param.compaction_scn_, *ls_handle.get_ls()))) {
    ret = OB_EAGAIN;
    LOG_WARN("ls weak read ts not ready, cannot schedule merge", K(ret), K(param));
  } else {
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSSkipMajor1"));
    int64_t succ_cnt = 0;
    const int64_t end_idx = MIN(param.tablet_info_array_.count(), get_end_idx());
    int64_t batch_finished_data_size = 0;

    for (int64_t idx = get_start_idx(); OB_SUCC(ret) && idx < end_idx; ++idx) {
      if (OB_TMP_FAIL(update_skip_major_tablet_meta(allocator,
                                                    *ls_handle.get_ls(),
                                                    param.compaction_scn_,
                                                    param.tablet_info_array_.at(idx),
                                                    batch_finished_data_size))) {
        LOG_WARN("failed to update tablet meta", KR(tmp_ret), "tablet_id", param.tablet_info_array_.at(idx));
      } else if (FALSE_IT(++succ_cnt)) {
      } else if (OB_FAIL(share::dag_yield())) {
        LOG_WARN("failed to dag yield", KR(ret));
      }
    }

    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("finish update skip major tablet", K(cost_ts), K(ret), "tablet_cnt", end_idx - get_start_idx(),
      K_(update_cnt), K(cost_ts));

    if (OB_SUCC(ret)) {
      (void) MTL(ObTenantCompactionProgressMgr *)->update_unfinish_tablet(param.compaction_scn_, succ_cnt, batch_finished_data_size);
    }
  }
  return ret;
}

int ObSSSkipMajorMergeTask::update_skip_major_tablet_meta(
    ObArenaAllocator &allocator,
    ObLS &ls,
    const int64_t compaction_scn,
    const ObTabletID &tablet_id,
    int64_t &batch_finished_data_size)
{
  int ret = OB_SUCCESS;
  allocator.reuse();
  ObTabletHandle tablet_handle;
  const ObTabletTableStore *table_store = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  blocksstable::ObSSTable *last_major = NULL;
  int64_t major_macro_block_count = 0;
  int64_t sswriter_epoch = 0;
  const ObLSID ls_id = ls.get_ls_id();
  bool update_local_only = false;
  bool skip = false;
  if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id,
                                              tablet_handle,
                                              0/*timeout_us*/,
                                              storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // update tablet state later
      skip = true;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(compaction_scn));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet_id), K(compaction_scn));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(tablet_id), K(compaction_scn));
  } else if (OB_ISNULL(last_major = static_cast<ObSSTable *>(table_store->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable is unexpected null", K(ret), K(tablet_id), K(compaction_scn), KPC(table_store));
  } else if (OB_UNLIKELY(last_major->get_snapshot_version() >= compaction_scn)) {
    LOG_INFO("last major have larger compaction scn, no need to update tablet meta", KR(ret), KPC(last_major), K(compaction_scn));
  } else if (FALSE_IT(major_macro_block_count = last_major->get_total_macro_block_count())) {
  } else {
    ObArenaAllocator ss_allocator(ObMemAttr(MTL_ID(), "SSSkipMajor"));
    ObTabletHandle ss_tablet_handle;
    ObTabletMemberWrapper<ObTabletTableStore> ss_table_store_wrapper;
    blocksstable::ObSSTable *last_shared_major = NULL;
    if (OB_FAIL(share::SSCompactHelper::check_shared_major_exist(ls_id,
                                                                 tablet_id,
                                                                 tablet_handle.get_obj()->get_reorganization_scn(),
                                                                 compaction_scn,
                                                                 ss_allocator,
                                                                 ss_tablet_handle,
                                                                 ss_table_store_wrapper,
                                                                 last_shared_major))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        int64_t shared_major_last_snapshot_version = last_shared_major ? last_shared_major->get_snapshot_version() : 0;
        if (shared_major_last_snapshot_version >= compaction_scn) {
          LOG_INFO("there are newer major exist, assume current major exist", K(shared_major_last_snapshot_version), K(compaction_scn));
          update_local_only = true;
        } else if (OB_FAIL(share::SSCompactHelper::check_is_sswriter(ls_id, tablet_id))) {
          if (OB_NOT_MASTER != ret) {
            LOG_WARN("check sswriter lease fail", K(ret));
          }
          // not sswriter, need wait sswriter has update shared tablet, otherwise update shared tablet
        } else if (shared_major_last_snapshot_version != last_major->get_snapshot_version()) {
          ret = OB_NEED_RETRY;
          LOG_WARN("shared tablet major snapshot version not match with local", K(ret), K(shared_major_last_snapshot_version), K(last_major->get_snapshot_version()));
        } else if (OB_FAIL(update_tablet(allocator, ls, *tablet_handle.get_obj(), *last_major, compaction_scn, last_shared_major))) {
          LOG_WARN("update ss tablet and local tablet fail", K(ret));
        }
      } else {
        LOG_WARN("check shared major exist fail", K(ret));
      }
    } else { // check major exist in shared tablet, just update local tablet
      update_local_only = true;
    }
  }
  if (OB_SUCC(ret)
      && !skip
      && update_local_only
      && OB_FAIL(update_tablet(allocator, ls, *tablet_handle.get_obj(), *last_major, compaction_scn, NULL/*last_shared_major*/))) {
    LOG_WARN("update local tablet fail", K(ret));
  }

  if (OB_SUCC(ret)) {
    ++cnt_.success_cnt_;
    if (!skip) {
      batch_finished_data_size += major_macro_block_count * DEFAULT_MACRO_BLOCK_SIZE;
    }
  } else {
    ++cnt_.failure_cnt_;
  }
  return ret;
}

int ObSSSkipMajorMergeTask::update_tablet(
    common::ObArenaAllocator &allocator,
    ObLS &ls,
    const ObTablet &old_tablet,
    blocksstable::ObSSTable &last_major,
    const int64_t compaction_scn,
    blocksstable::ObSSTable *last_shared_major)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = old_tablet.get_tablet_id();
  ObTableHandleV2 new_major_handle;
  ObTableHandleV2 new_shared_major_handle;
  blocksstable::ObSSTable *new_major = NULL;
  blocksstable::ObSSTable *new_shared_major = NULL;
  if (OB_FAIL(prep_new_major_sstable(allocator, last_major, compaction_scn, new_major_handle))) {
    LOG_WARN("failed to prep new major sstable", K(ret), K(compaction_scn), K(last_major));
  } else if (OB_NOT_NULL(last_shared_major) &&
             OB_FAIL(prep_new_major_sstable(allocator, *last_shared_major, compaction_scn, new_shared_major_handle))) {
    LOG_WARN("failed to prep new major sstable", K(ret), K(compaction_scn), K(last_shared_major));
  } else if (OB_FAIL(new_major_handle.get_sstable(new_major))) {
    LOG_WARN("failed to get sstable from sstable handle", K(ret), K(new_major_handle));
  } else if (new_shared_major_handle.is_valid() && OB_FAIL(new_shared_major_handle.get_sstable(new_shared_major))) {
    LOG_WARN("failed to get sstable from sstable handle", K(ret), K(new_shared_major_handle));
  } else if (OB_FAIL(do_update_tablet(allocator, ls, old_tablet, *new_major, new_shared_major))) {
    LOG_WARN("failed to update tablet", K(ret), K(tablet_id), K(compaction_scn));
  }
  new_major_handle.reset();
  new_shared_major_handle.reset();
  if (OB_NOT_NULL(new_major)) {
    new_major->~ObSSTable();
    new_major = NULL;
  }
  if (OB_NOT_NULL(new_shared_major)) {
    new_shared_major->~ObSSTable();
    new_shared_major = NULL;
  }
  return ret;
}

int ObSSSkipMajorMergeTask::prep_new_major_sstable(
    common::ObArenaAllocator &allocator,
    blocksstable::ObSSTable &old_major,
    const int64_t compaction_scn,
    ObTableHandleV2 &new_major_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *new_major = NULL;
  if (OB_FAIL(blocksstable::load_and_copy_sstable(old_major, allocator, true, NULL/*modify_func*/, new_major))) {
    LOG_WARN("failed to load sstable and deep copy", K(ret), K(compaction_scn), K(old_major));
  } else if (new_major->is_co_sstable() && OB_FAIL(generate_new_co_sstable(compaction_scn, *static_cast<ObCOSSTableV2 *>(new_major)))) {
    LOG_WARN("failed to generate new co sstable", KR(ret), KPC(new_major));
  } else if (OB_FAIL(update_sstable(compaction_scn, *new_major))) {
    LOG_WARN("failed to update sstable", KR(ret), K(compaction_scn), KPC(new_major));
  } else {
    new_major_handle.set_sstable(new_major, &allocator);
  }
  return ret;
}

int ObSSSkipMajorMergeTask::prepare_storage_schema_(
    const ObTablet &tablet,
    const int64_t compaction_scn,
    common::ObArenaAllocator &allocator,
    ObStorageSchema *&storage_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator medium_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObMediumCompactionInfo *medium_info = nullptr;
  if (OB_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(compaction_scn, tablet, medium_allocator, medium_info))) {
    LOG_WARN("fail to get medium info with merge version", KR(ret), KPC(this));
  } else if (medium_info->storage_schema_.is_valid()) {
    if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(allocator, storage_schema))) {
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else if (OB_FAIL(storage_schema->init(allocator, medium_info->storage_schema_,
                                            false /*skip_column_info*/,
                                            nullptr /*column_group_schema*/,
                                            false/*generate_cs_replica_cg_array*/,
                                            nullptr/*update_param*/))) {
      LOG_WARN("failed to init storage schema from current medium info", K(ret), KPC(medium_info));
    } else {
      LOG_TRACE("success to prepare storage schema from medium info", K(tablet), K(medium_info), KPC(storage_schema));
    }
  } else if (OB_FAIL(tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(tablet));
  } else {
    LOG_TRACE("success to prepare storage schema from tablet", K(tablet), KPC(storage_schema));
  }
  if (OB_NOT_NULL(medium_info)) {
    ObTabletObjLoadHelper::free(medium_allocator, medium_info);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(storage_schema)) {
    ObStorageSchemaUtil::free_storage_schema(allocator, storage_schema);
    storage_schema = nullptr;
  }
  return ret;
}

int ObSSSkipMajorMergeTask::do_update_tablet(
    common::ObArenaAllocator &allocator,
    ObLS &ls,
    const ObTablet &old_tablet,
    const blocksstable::ObSSTable &new_major,
    const blocksstable::ObSSTable *new_shared_major)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = old_tablet.get_tablet_meta().tablet_id_;
  const int64_t major_snapshot = new_major.get_key().get_snapshot_version();
  ObUpdateTableStoreParam update_param(major_snapshot,
                                       ObVersionRange::MIN_VERSION,
                                       nullptr /*storage_schema*/,
                                       ls.get_rebuild_seq(),
                                       &new_major,
                                       false /*allow_dup_major*/);
  ObStorageSchema *storage_schema = nullptr;
  if (OB_FAIL(prepare_storage_schema_(old_tablet, major_snapshot, allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(old_tablet));
  } else if (FALSE_IT(update_param.storage_schema_ = storage_schema)) {
  } else if (FALSE_IT(update_param.ddl_info_.update_with_major_flag_ = true)) {
    // download only used for update major
  } else if (OB_FAIL(update_param.init_with_compaction_info(
      ObCompactionTableStoreParam(MAJOR_MERGE, SCN::min_scn(), true/*need_report*/, false/*has_merged_with_mds_info*/)))) {
    LOG_WARN("failed to init with compaction info", KR(ret));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_UPDATE_SKIP_MAJOR_DAG_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "ERRSIM EN_UPDATE_SKIP_MAJOR_DAG_FAILED", K(ret));
    }
  }
#endif
  if (OB_SUCC(ret) && OB_NOT_NULL(new_shared_major)) {
    // gc shared old major sstables
    update_param.sstable_ = new_shared_major;
    update_param.multi_version_start_ = old_tablet.get_multi_version_start();
    // not pass new major snapshot here for now @jyx441808 @muwei.ym
    ObArray<ObSSMetaCtrlKeyInfo> empty_ctrl_keys;
    if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(ls.get_ls_id(),
                                                                   tablet_id,
                                                                   old_tablet.get_reorganization_scn(),
                                                                   ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                                                                   update_param,
                                                                   empty_ctrl_keys,
                                                                   true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail", K(ret));
    }
    if (OB_SUCC(ret)) {
      ++update_cnt_.ss_cnt_;
    }
  }
  update_param.sstable_ = &new_major;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle unused_new_tablet_handle;
  if (FAILEDx(ls.update_tablet_table_store(tablet_id, update_param, unused_new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(update_param), K(unused_new_tablet_handle));
  } else if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
    LOG_WARN("failed to submit tablet update task to report", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
    LOG_WARN("failed to update tablet report status", K(ret), K(ls_id), K(tablet_id));
  } else {
    ++update_cnt_.local_cnt_;
    LOG_INFO("success to update skip major tablet", KR(ret), K(ls_id), K(tablet_id), K(major_snapshot));
  }
  return ret;
}

int ObSSSkipMajorMergeTask::generate_new_co_sstable(
  const int64_t compaction_scn,
  ObCOSSTableV2 &new_major)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSSTableWrapper, 16> table_wrappers;
  if (new_major.is_cgs_empty_co_table()) {
    // do nothing
  } else if (OB_FAIL(new_major.get_all_tables(table_wrappers))) {
    LOG_WARN("failed to get all tables", KR(ret), K(new_major));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_wrappers.count(); ++idx) {
      blocksstable::ObSSTable *sstable = table_wrappers.at(idx).get_sstable();
      if (OB_UNLIKELY(NULL == sstable || !sstable->is_column_store_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sstable", KR(ret), KPC(sstable));
      } else if (sstable->get_column_group_id() != new_major.get_column_group_id() // new major will be update outside
          && OB_FAIL(update_sstable(compaction_scn, *sstable))) {
        LOG_WARN("failed to update sstable", KR(ret), K(compaction_scn), KPC(sstable));
      }
    } // end of for
  }
  return ret;
}

int ObSSSkipMajorMergeTask::update_sstable(
    const int64_t compaction_scn,
    blocksstable::ObSSTable &new_major)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(new_major.modify_snapshot(compaction_scn))) {
    LOG_WARN("failed to modify snapshot", KR(ret), K(compaction_scn));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/direct_load/ob_direct_load_ss_update_inc_major_task.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/incremental/ob_shared_meta_service.h"

namespace oceanbase
{
using namespace compaction;
namespace storage
{
/*---  ObDirectLoadSSUpdateIncMajorITask  ---*/
ObDirectLoadSSUpdateIncMajorITask::ObDirectLoadSSUpdateIncMajorITask(
  ObLS &ls,
  const ObTablet &tablet,
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id)
 : allocator_(lib::ObMemAttr(MTL_ID(), "SS_INC_MAJOR")),
   ls_(ls),
   tablet_(tablet),
   ls_id_(ls_id),
   tablet_id_(tablet_id)
{}

// 1. inc major trans abort, need gc
// 2. inc major trans commit, need update trans version
// 3. inc major trans running, do nothing
ERRSIM_POINT_DEF(SKIP_INC_MAJOR_TASK);
int ObDirectLoadSSUpdateIncMajorITask::process()
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet_.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", KR(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_UNLIKELY(!table_store_wrapper.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table store wrapper", KR(ret), K(ls_id_), K(tablet_id_));
  } else {
#ifdef ERRSIM
    ret = SKIP_INC_MAJOR_TASK ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("set ret = SKIP_INC_MAJOR_TASK");
    }
#endif
    const ObSSTableArray &inc_major_sstables = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_sstables.count(); ++i) {
      ObSSTable *old_inc_major = inc_major_sstables.at(i);
      ObSSTable *new_inc_major = nullptr;
      ObIncMajorSSTableAction action = ObIncMajorSSTableAction::MAX_TYPE;
      if (OB_ISNULL(old_inc_major)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old inc major is nullptr", KR(ret), K(i));
      } else if (INT64_MAX == old_inc_major->get_upper_trans_version()) {
        if (OB_FAIL(prepare_new_inc_major(*old_inc_major, new_inc_major, action))) {
          LOG_WARN("fail to prepare new inc major", KR(ret), K(ls_id_), K(tablet_id_), K(old_inc_major));
        } else if (OB_NOT_NULL(new_inc_major)) {
          if (OB_FAIL(update_tablet_table_store(new_inc_major, action))) {
            LOG_WARN("fail to update tablet table store", KR(ret), K(ls_id_), K(tablet_id_));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSSUpdateIncMajorITask::prepare_new_inc_major(
    blocksstable::ObSSTable &old_inc_major,
    blocksstable::ObSSTable *&new_inc_major,
    ObIncMajorSSTableAction &action)
{
  int ret = OB_SUCCESS;
  int64_t trans_state = ObTxData::UNKOWN;
  int64_t commit_version = OB_INVALID_VERSION;
  action = ObIncMajorSSTableAction::MAX_TYPE;
  if (OB_UNLIKELY(INT64_MAX != old_inc_major.get_upper_trans_version())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inc major don't need to update", KR(ret), K(old_inc_major));
  } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(ls_,
                                                                      old_inc_major,
                                                                      SCN::max_scn(),
                                                                      trans_state,
                                                                      commit_version))) {
    LOG_WARN("fail to get inc major trans state", KR(ret), K(old_inc_major));
  } else if (ObTxData::UNKOWN == trans_state || ObTxData::ELR_COMMIT == trans_state) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tx data", KR(ret), K(trans_state));
  } else if (ObTxData::COMMIT == trans_state) {
    if (OB_FAIL(prepare_new_committed_inc_major(old_inc_major, commit_version, new_inc_major))) {
      LOG_WARN("fail to prepare new committed inc major", KR(ret));
    } else {
      action = ObIncMajorSSTableAction::COMMIT;
    }
  } else if (ObTxData::ABORT == trans_state) {
    // GC inc major by updating the inc major to tablet table store again.
    // Check the table_key and upper trans version to decide whether to perform GC while updating the tablet table store.
    new_inc_major = &old_inc_major;
    action = ObIncMajorSSTableAction::ABORT;
    FLOG_INFO("[SS INC MAJOR]inc major need gc", K(old_inc_major));
  }
  return ret;
}

int ObDirectLoadSSUpdateIncMajorITask::prepare_new_committed_inc_major(
    ObSSTable &old_inc_major,
    const int64_t commit_version,
    ObSSTable *&new_inc_major)
{
  int ret = OB_SUCCESS;
  new_inc_major = nullptr;
  if (OB_UNLIKELY(INT64_MAX != old_inc_major.get_upper_trans_version()
                  || INT64_MAX == commit_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inc major or commit version",
             KR(ret), K(old_inc_major), K(commit_version));
  } else if (OB_FAIL(old_inc_major.deep_copy(allocator_, new_inc_major))) {
    LOG_WARN("fail to deep copy", KR(ret), K(old_inc_major));
  } else if (OB_ISNULL(new_inc_major)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new table is nullptr", KR(ret));
  } else if (OB_FAIL(new_inc_major->set_upper_trans_version(allocator_, commit_version))) {
    LOG_WARN("fail to set upper trans version", KR(ret));
  }
  FLOG_INFO("[SS INC MAJOR]prepare new commit inc major", KR(ret), K(old_inc_major), K(commit_version), KPC(new_inc_major));
  return ret;
}

/*---  ObDirectLoadSSUpdateLocalIncMajorTask  ---*/
ObDirectLoadSSUpdateLocalIncMajorTask::ObDirectLoadSSUpdateLocalIncMajorTask(
  ObLS &ls,
  const ObTablet &tablet,
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id)
 : ObDirectLoadSSUpdateIncMajorITask(ls, tablet, ls_id, tablet_id)
{}

ObDirectLoadSSUpdateLocalIncMajorTask::~ObDirectLoadSSUpdateLocalIncMajorTask() {}

int ObDirectLoadSSUpdateLocalIncMajorTask::update_tablet_table_store(
    const blocksstable::ObSSTable *new_inc_major,
    const ObIncMajorSSTableAction action)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator tmp_arena("SS_INC_MAJOR", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(tablet_.load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("fail to load storage schema", KR(ret));
  } else {
    ObUpdateTableStoreParam param(tablet_.get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION/*multi_version_start*/,
                                  storage_schema,
                                  ls_.get_rebuild_seq(),
                                  new_inc_major);
    param.inc_major_sstable_action_ = action;
    ObTabletHandle new_tablet_handle;
    if (OB_FAIL(ls_.update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("fail to update local tablet table store", KR(ret), K(ls_id_), K(tablet_id_));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

/*---  ObDirectLoadSSUpdateSharedIncMajorTask  ---*/
ObDirectLoadSSUpdateSharedIncMajorTask::ObDirectLoadSSUpdateSharedIncMajorTask(ObLS &ls,
                                                                     const ObTablet &tablet,
                                                                     const share::ObLSID &ls_id,
                                                                     const ObTabletID &tablet_id)
 : ObDirectLoadSSUpdateIncMajorITask(ls, tablet, ls_id, tablet_id)
{}

ObDirectLoadSSUpdateSharedIncMajorTask::~ObDirectLoadSSUpdateSharedIncMajorTask() {}

int ObDirectLoadSSUpdateSharedIncMajorTask::update_tablet_table_store(
    const ObSSTable *new_inc_major, const ObIncMajorSSTableAction action)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator tmp_arena("SS_INC_MAJOR", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(tablet_.load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("fail to load storage schema", KR(ret));
  } else {
    ObUpdateTableStoreParam param(tablet_.get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION/*multi_version_start*/,
                                  storage_schema,
                                  ls_.get_rebuild_seq(),
                                  new_inc_major);
    param.inc_major_sstable_action_ = action;
    share::SCN transfer_scn = tablet_.get_reorganization_scn();
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(
                                              ls_id_,
                                              tablet_id_,
                                              transfer_scn,
                                              ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE,
                                              param, meta_ctrl_keys, true/*check_lease*/))) {
      LOG_WARN("fail to update tablet table store", KR(ret), K(ls_id_), K(tablet_id_),
                                                    K(transfer_scn), K(param));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

} // namespace storage
} // namespace oceanbase

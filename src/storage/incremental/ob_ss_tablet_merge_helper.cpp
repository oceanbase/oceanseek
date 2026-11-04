// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_tablet_merge_helper.h"
#include "ob_sswriter_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace compaction
{
static int get_local_tablet_latest_committed_status_(
  const ObLSID &ls_id,
  const ObTabletID &tablet_id,
  ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle, 10_s, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get local tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_id));
  } else {
    ret = tablet->get_latest_committed_tablet_status(user_data);
  }
  return ret;
}

// This interface is used by SS mini, minor, and major compactions.
// Transfer and split may prevent a new shared-storage tablet from being generated.
// Until local land completes on every replica of a shared fork, regular shared minor can only merge SSTables after
// the fork snapshot, while shared major and upload replace minor remain blocked.
// Tablet split is invoked by SS minor but also affects major. Split relies on this interface to reject new shared
// versions so data is split from the last shared version and macroblock leaks are avoided.
int ObSSTabletMergeHelper::check_can_generate_new_tablet(
    const ObLSID &ls_id,
    const ObTabletHandle &shared_tablet_handle,
    const ObMetaUpdateReason &update_reason,
    const bool need_check_split,
    bool &can_generate_new_tablet,
    bool &can_gc_sstable)
{
  int ret = OB_SUCCESS;
  can_generate_new_tablet = true;
  can_gc_sstable = true;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTablet *tablet = nullptr;
  const ObTabletID &tablet_id = shared_tablet_handle.get_obj()->get_tablet_id();

  if (OB_UNLIKELY(!shared_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(shared_tablet_handle));
  } else if (OB_ISNULL(tablet = shared_tablet_handle.get_obj()) || !tablet->is_ss_tablet()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL or tablet is not ss tablet", K(ret), K(shared_tablet_handle));
  } else if (tablet->get_tablet_meta().fork_info_.is_active()) {
    can_gc_sstable = false;
    if (ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE == update_reason
        || ObMetaUpdateReason::TABLET_UPLOAD_REPLACE_MINOR_SSTABLE == update_reason) {
      can_generate_new_tablet = false;
      FLOG_INFO("fork stable replicas have not completed local land, cannot update shared table store",
          K(update_reason), "fork_info", tablet->get_tablet_meta().fork_info_, K(can_gc_sstable),
          K(can_generate_new_tablet));
    }
  } else if (need_check_split && tablet->get_tablet_meta().split_info_.can_not_execute_ss_minor()) {
    can_generate_new_tablet = false;
    can_gc_sstable = false; // can not recycle mini/minor sstables.
    FLOG_INFO("check_can_generate_new_tablet for the split src tablet", K(update_reason), K(can_gc_sstable), K(can_generate_new_tablet));
  } else if (OB_FAIL(get_local_tablet_latest_committed_status_(ls_id, tablet_id, user_data))) {
    if (OB_EMPTY_RESULT == ret) {
      //overwrite ret
      can_generate_new_tablet = true;
      can_gc_sstable = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get latest commit user data", K(ret), K(shared_tablet_handle));
    }
  } else if (ObTabletStatus::TRANSFER_OUT == user_data.tablet_status_ || ObTabletStatus::TRANSFER_OUT_DELETED == user_data.tablet_status_) {
    can_gc_sstable = false;
    const ObTabletTransferInfo &transfer_info = tablet->get_tablet_meta().transfer_info_;
    if (!transfer_info.is_transfer_upload_completed()) {
      can_generate_new_tablet = true;
      FLOG_INFO("transfer upload is incomplete, can generate new tablet", K(update_reason), K(user_data), K(transfer_info));
    } else if (is_transfer_backfill_update_reason(update_reason)) {
      can_generate_new_tablet = true;
      FLOG_INFO("backfill in transfer out status, can generate new tablet", K(update_reason), K(user_data), K(transfer_info));
    } else {
      can_generate_new_tablet = false;
      FLOG_INFO("tablet is in transfer out status, cannot generate new tablet", K(update_reason), K(user_data), K(transfer_info));
#ifdef ERRSIM
      SERVER_EVENT_ADD("storage_ha", "abort_minor_after_backfill",
                       "tenant_id", MTL_ID(),
                       "ls_id", shared_tablet_handle.get_obj()->get_ls_id().id(),
                       "tablet_id", shared_tablet_handle.get_obj()->get_tablet_id().id());
#endif
    }
  } else {
    can_generate_new_tablet = true;
    can_gc_sstable = true;
  }
  return ret;
}

bool ObSSTabletMergeHelper::is_hot_tablet(const ObTabletID &tablet_id)
{
  bool is_hot = false;
  int ret = OB_SUCCESS;
  if (tablet_id.is_only_mini_merge_tablet()) {
    // flush only tablet is cold
    is_hot = false;
  } else if (tablet_id.is_ls_inner_tablet()) {
    // others is hot default
    is_hot = true;
  } else {
    ObStorageCachePolicyService *storage_cache_policy_service = MTL(ObStorageCachePolicyService*);
    if (OB_ISNULL(storage_cache_policy_service)) {
      ret = OB_NOT_INIT;
      LOG_WARN("storage cache policy service not inited, assume not hot-tablet", K(ret), K(tablet_id));
    } else if (OB_FAIL(storage_cache_policy_service->is_hot_tablet(tablet_id.id(), is_hot))) {
      LOG_WARN("failed to check is hot tablet", K(ret), K(tablet_id));
    }
  }
  LOG_TRACE("is hot tablet", K(tablet_id), K(is_hot));
  return is_hot;
}

bool ObSSTabletMergeHelper::is_cold_tablet(const ObTabletID &tablet_id)
{
  bool is_cold = false;
  int ret = OB_SUCCESS;
  if (tablet_id.is_only_mini_merge_tablet()) {
    // flush only tablet is cold
    is_cold = true;
  } else if (tablet_id.is_ls_inner_tablet()) {
    // others is cold default
    is_cold = false;
  } else {
    ObStorageCachePolicyService *scps = MTL(ObStorageCachePolicyService*);
    if (OB_ISNULL(scps)) {
      ret = OB_NOT_INIT;
      LOG_WARN("storage cache policy service not inited, assume not cold-tablet", K(ret), K(tablet_id));
    } else if (OB_FAIL(scps->is_cold_tablet(tablet_id.id(), is_cold))) {
      LOG_WARN("failed to check is cold tablet", K(ret));
    }
  }
  LOG_TRACE("is cold tablet", K(tablet_id), K(is_cold));
  return is_cold;
}

int ObSSTabletMergeHelper::check_is_sswriter(
  const ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const ObSSWriterType type,
  bool &is_sswriter)
{
  int ret = OB_SUCCESS;
  ObSSWriterKey key(type, ls_id, tablet_id);
  is_sswriter = false;
  int64_t epoch = -1;
  ObSSWriterService *sswriter_service = nullptr;
  if (OB_ISNULL(sswriter_service = MTL(ObSSWriterService *))) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter service not inited", KR(ret));
  } else if (OB_FAIL(sswriter_service->check_lease(key, is_sswriter, epoch))) {
    LOG_WARN("check lease failed", KR(ret), K(key), K(is_sswriter), K(epoch));
  } else if (!is_sswriter) {
    LOG_TRACE("current is not sswriter", K(ret), K(key));
  } else {
    LOG_TRACE("current is sswriter", K(ret), K(key));
  }
  return ret;
}

bool ObSSTabletMergeHelper::can_schedule_ss_minor(const ObLSID &ls_id, const ObTabletID &tablet_id)
{
  bool bret = false;
  bool is_sswriter = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_is_sswriter(ls_id, tablet_id, ObSSWriterType::COMPACTION, is_sswriter ))) {
    LOG_WARN("check is sswriter fail", K(ret), K(ls_id), K(tablet_id));
  } else if (!is_sswriter) {
    bret = false;
  } else {
    bret = true;
  }
  return bret;
}

int ObSSTabletMergeHelper::flush_shared_block_cache(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObPromise<int> promise;
  if (!macro_id.is_shared_data_or_meta()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro id is not shared data or meta", K(ret), K(macro_id));
  } else if (OB_FAIL(promise.init())) { // FIXME use tenant allocator
    LOG_WARN("failed to init promise", K(ret));
  } else if (OB_FAIL(MTL(ObSSMacroCacheMgr*)->flush_write_cache(macro_id, promise))) {
    LOG_WARN("failed to flush write cache", K(ret));
  } else if (OB_FAIL(promise.get_future().wait())) {
    LOG_WARN("failed to wait promise", K(ret));
  } else {
    LOG_INFO("flush write cache success", K(macro_id));
  }
  return ret;
}

int ObSSTabletMergeHelper::refine_parallel_degree(
    const ObMergeType merge_type,
    int64_t &parallel_degree)
{
  int ret = OB_SUCCESS;
  int64_t refined_parallel_degree = parallel_degree;
  switch (merge_type) {
    case ObMergeType::MINI_MERGE:
    case ObMergeType::MINOR_MERGE: {
      refined_parallel_degree = MIN(
          parallel_degree, blocksstable::ObMacroDataSeq::MAX_PARALLEL_IDX);
      break;
    }
    default:
      break;
  }
  if (refined_parallel_degree != parallel_degree) {
    LOG_INFO("refine parallel degree", K(merge_type),
        "origin", parallel_degree, "current", refined_parallel_degree);
    parallel_degree = refined_parallel_degree;
  }
  return ret;
}

int ObSSTabletMergeHelper::check_tablet_complete(const ObTablet &tablet, bool &is_complete)
{
  int ret = OB_SUCCESS;
  is_complete = true;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  if (tablet_meta.split_info_.is_data_incomplete()) {
    is_complete = false;
  } else if (!tablet_meta.ha_status_.is_data_status_complete()) {
    is_complete = false;
  } else if (tablet_meta.has_transfer_table()) {
    is_complete = false;
  }
  return ret;
}

} //compaction
} //oceanbase

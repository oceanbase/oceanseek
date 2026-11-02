// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_link_major_task.h"
#ifdef ERRSIM
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#endif
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace compaction
{

int ObTabletSSLinkMajorTask::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSLinkMajor"));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle ss_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> ss_table_store_wrapper;
  const ObSSTable *major_sstable = NULL;
  share::SCN transfer_scn;
  share::SCN ss_tablet_meta_scn;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get local ls fail", K(ls_id_));
    // NOTE: use ha_get_tablet here, because the tablet maybe in TRANSFER_OUT state
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("get local tablet fail", K(tablet_id_));
  } else if (is_inc_major_merge(merge_type_) ?
      compaction_scn_ <= tablet_handle.get_obj()->get_last_inc_major_snapshot_version() :
      compaction_scn_ <= tablet_handle.get_obj()->get_last_major_snapshot_version()) {
    LOG_INFO("already linked with higher major", K(compaction_scn_), K(merge_type_),
        "last_major_snapshot_version", tablet_handle.get_obj()->get_last_major_snapshot_version(),
        "last_inc_major_snapshot_version", tablet_handle.get_obj()->get_last_inc_major_snapshot_version());
  } else if (FALSE_IT(transfer_scn = tablet_handle.get_obj()->get_reorganization_scn())) {
  } else if (OB_FAIL(find_target_major_(transfer_scn, allocator, ss_tablet_handle, ss_tablet_meta_scn, ss_table_store_wrapper, major_sstable))) {
    LOG_WARN("find target major fail", K(ret));
  } else if (prewarm_ && OB_FAIL(do_prewarm_(transfer_scn, tablet_handle, *major_sstable))) {
    LOG_WARN("do prewarm major fail", K(ret));
  } else if (OB_FAIL(do_link_(ls_handle, tablet_handle, *ss_tablet_handle.get_obj(), *major_sstable, ss_tablet_meta_scn, allocator))) {
    LOG_WARN("link major fail", K(ret), K_(compaction_scn));
  } else {
    after_link_(transfer_scn);
    LOG_INFO("link major succ", KPC(major_sstable), K(ss_tablet_meta_scn), K_(compaction_scn), K(transfer_scn));
  }
  if (OB_FAIL(ret)) {
    FLOG_WARN("link major failed", K(ret), K(compaction_scn_), KPC(major_sstable), K_(ls_id), K(transfer_scn));
  }
  return ret;
}

int ObTabletSSLinkMajorTask::find_target_major_(const share::SCN &transfer_scn,
                                                ObArenaAllocator &allocator,
                                                ObTabletHandle &ss_tablet_handle,
                                                share::SCN &ss_tablet_meta_scn,
                                                ObTabletMemberWrapper<ObTabletTableStore> &ss_table_store_wrapper,
                                                const ObSSTable *&target)
{
  int ret = OB_SUCCESS;
  bool try_old_version = false;
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id_, tablet_id_, transfer_scn, allocator, ss_tablet_handle, ss_tablet_meta_scn))) {
    LOG_WARN("get shared tablet fail", K(ret));
  } else if (!ss_tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet is invalid", K(ret));
  } else if (OB_FAIL(find_target_major_(*ss_tablet_handle.get_obj(), ss_table_store_wrapper, target, try_old_version))) {
    if (!try_old_version) {
      LOG_WARN("find target major fail", K(ret), K(transfer_scn), K(compaction_scn_));
    }
  }
  if (OB_FAIL(ret) && try_old_version) {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META, ls_id_, tablet_id_,
      SCN::min_scn() /*meta_scn*/, transfer_scn, true /*is_in_table*/);
    share::SCN end_version = ss_tablet_meta_scn;
    share::SCN start_version = SCN::base_scn();
    ObSSMetaTabletIter *tablet_iter = NULL;
    ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
    share::ObScnRange scn_range;
    scn_range.start_scn_ = SCN::base_scn();
    scn_range.end_scn_ = SCN::scn_dec(ss_tablet_meta_scn);
    if (OB_FAIL(MTL(ObSSMetaService *)->get_tablet_iter(read_param,
                                                   scn_range,
                                                   iter_guard))) {
      LOG_WARN("get tablet iter fail", K(ret), K(read_param), K(start_version), K(end_version));
    } else if (OB_FAIL(iter_guard.get_iter(tablet_iter))) {
      LOG_WARN("get iter fail", K(ret));
    } else {
      ObSSMetaUpdateMetaInfo update_meta_info;
      share::SCN reorganization_scn;
      while (OB_SUCC(ret)) {
        ss_table_store_wrapper.reset();
        ss_tablet_handle.reset();
        allocator.reuse();
        if (OB_FAIL(tablet_iter->get_next(allocator, ss_tablet_handle, update_meta_info, reorganization_scn, ss_tablet_meta_scn))) {
          if (OB_ITER_END != ret && OB_INVALID_QUERY_TIMESTAMP != ret) {
            LOG_WARN("get next tablet fail", K(ret));
          }
        } else if (OB_FAIL(find_target_major_(*ss_tablet_handle.get_obj(), ss_table_store_wrapper, target, try_old_version))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("find target major fail", K(ret), KPC(ss_tablet_handle.get_obj()));
          } else if (try_old_version) {
            ret = OB_SUCCESS; // retry
          }
        } else {
          LOG_TRACE("found target shared tablet", K(ss_tablet_meta_scn), K(update_meta_info), KPC(ss_tablet_handle.get_obj()));
          break;
        }
      }
      if (OB_ITER_END == ret || OB_INVALID_QUERY_TIMESTAMP == ret || OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("suitable shared major not found", K(ret), K(start_version), K(end_version), K(compaction_scn_));
      }
    }
  }
  return ret;
}

int ObTabletSSLinkMajorTask::find_target_major_(ObTablet &ss_tablet,
                                                ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
                                                const ObSSTable *&target,
                                                bool &try_old_version)
{
  int ret = OB_SUCCESS;
  // This flag is unrelated to on-disk format compatibility. It tells the
  // caller to keep scanning earlier Shared TabletMeta rows when the target
  // SSTable is no longer referenced by the current TabletMeta row.
  try_old_version = true;
  if (OB_FAIL(ss_tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(is_inc_major_merge(merge_type_) && !ss_tablet.is_async_index())) {
    // INC_MAJOR_MERGE is only scheduled from async-index medium INC_MAJOR_COMPACTION.
    // Ordinary dump IncMajor uses ObTabletSSLinkIncMajorTask (trans_id + seq_no)
    // and must not interpret TableKey / min-max as a logical refresh window.
    ret = OB_ERR_UNEXPECTED;
    try_old_version = false;
    LOG_WARN("inc major link only supports async index tablet", K(ret),
        K_(merge_type), K_(compaction_scn), K_(merge_base_version), K(ss_tablet));
  } else if (is_inc_major_merge(merge_type_)) {
    // Match by logical refresh window, not TableKey / snapshot_version.
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      ObNewVersionRange version_range;
      if (OB_ISNULL(sstables[i])) {
        ret = OB_ERR_UNEXPECTED;
        try_old_version = false;
        LOG_WARN("unexpected null inc major sstable", K(ret), K(i));
      } else if (OB_FAIL(sstables[i]->get_async_index_version_range(version_range))) {
        try_old_version = false;
        LOG_WARN("failed to get inc major logical version range", K(ret), K(i), KPC(sstables[i]));
      } else if (version_range.snapshot_version_ < compaction_scn_) {
        LOG_TRACE("skip older", K(i), KPC(sstables[i]));
      } else if (version_range.snapshot_version_ > compaction_scn_) {
        LOG_TRACE("skip newer", K(i), KPC(sstables[i]));
      } else if (version_range.base_version_ != merge_base_version_) {
        LOG_TRACE("skip base version mismatch", K(i), KPC(sstables[i]));
      } else {
        target = sstables[i];
        LOG_TRACE("choose target inc major", KPC(target));
        break;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(target)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found target major", K(ret), K(compaction_scn_), K(merge_base_version_), K(sstables));
    }
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_major_sstables();
    if (sstables.count() == 0) {
      try_old_version = false;
    }
    for(int i = 0; i < sstables.count(); i ++) {
      if (sstables[i]->get_snapshot_version() < compaction_scn_) {
        LOG_TRACE("skip older", K(i), KPC(sstables[i]));
        try_old_version = false;
      } else if (sstables[i]->get_snapshot_version() > compaction_scn_) {
        LOG_TRACE("skip newer", K(i), KPC(sstables[i]));
      } else {
        LOG_INFO("found target major", KPC(sstables[i]));
        target = sstables[i];
        break;
      }
    }
    if (OB_ISNULL(target)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found target major", K(ret), K(compaction_scn_), K(merge_type_), K(sstables));
    }
  }
  return ret;
}

int ObTabletSSLinkMajorTask::do_prewarm_(
    const share::SCN &transfer_scn,
    const ObTabletHandle &tablet_handle,
    const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObHotTabletInfoReader reader(compaction_scn_, transfer_scn, tablet_handle, sstable);
  if (FAILEDx(reader.load_hot_macro_infos())) {
    LOG_WARN("failed to load hot macro info", K(ret), K(ls_id_), K(tablet_id_), K(compaction_scn_));
  } else {
    LOG_INFO("prewarm major succ", K(ls_id_), K(tablet_id_), K(compaction_scn_));
  }
  return ret;
}

ERRSIM_POINT_DEF(SS_EN_COMPACTION_REFRESH_MULTI_VERSION_START);
int ObTabletSSLinkMajorTask::do_link_(ObLSHandle &ls_handle,
                                      ObTabletHandle &tablet,
                                      ObTablet &ss_tablet,
                                      const ObSSTable &sstable,
                                      const share::SCN &ss_tablet_meta_scn,
                                      ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = NULL;
  ObStorageMetaHandle major_meta_handle;
  ObSSTable *loaded_major = NULL;
  ObSSTable *copied_sstable = nullptr;
  const ObSSTable *target_sstable = &sstable;
  if (OB_FAIL(ss_tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(ss_tablet));
  } else if (is_inc_major_merge(merge_type_)) {
    ObSSTable::ObUpdateIncMajorMetaScnFunc modify_func(ss_tablet_meta_scn);
    if (OB_FAIL(ObSSTable::load_and_copy_sstable(
        sstable, allocator, false/*expand_macro_info*/, &modify_func, copied_sstable))) {
      LOG_WARN("failed to copy linked inc major", K(ret), K(ss_tablet_meta_scn), K(sstable));
    } else {
      target_sstable = copied_sstable;
    }
  } else if (!sstable.is_loaded()) {
    if (OB_FAIL(ObCacheSSTableHelper::load_sstable(sstable.get_addr(), sstable.get_meta_type(), major_meta_handle))) {
      LOG_WARN("load sstable fail", K(ret));
    } else if (OB_FAIL(major_meta_handle.get_sstable(loaded_major))) {
      LOG_WARN("get sstable fail", K(ret));
    } else {
      target_sstable = loaded_major;
      LOG_INFO("load major succ", KPC(loaded_major));
    }
  }
  if (OB_SUCC(ret)) {
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(
        sstable.get_snapshot_version(),
        ObVersionRange::MIN_VERSION,
        storage_schema,
        ls_handle.get_ls()->get_rebuild_seq(),
        target_sstable,
        true/*allow dup sstable*/);
    if (tablet.get_obj()->get_last_major_snapshot_version() <= 0) {
      LOG_INFO("no major sstable, set multi version start to major snapshot version", KPC(tablet.get_obj()));
      if (tablet.get_obj()->get_multi_version_start() < sstable.get_snapshot_version()) {
        param.multi_version_start_ = sstable.get_snapshot_version();
      }
    }
#ifdef ERRSIM
    if (storage::ObSSGCUtil::is_errsim_target(MTL_ID(), tablet_id_.id())
        && SS_EN_COMPACTION_REFRESH_MULTI_VERSION_START) {
      param.multi_version_start_ = sstable.get_snapshot_version();
      FLOG_INFO("ERRSIM SS_EN_COMPACTION_REFRESH_MULTI_VERSION_START", KR(ret), K(param.multi_version_start_));
    }
#endif
    param.ddl_info_.update_with_major_flag_ = true;
    if (OB_FAIL(param.init_with_compaction_info(
                    ObCompactionTableStoreParam(merge_type_,
                                                SCN::min_scn(),
                                                need_report_/*need_report*/,
                                                ss_tablet.has_merged_with_mds_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      if (is_inc_major_merge(merge_type_)) {
        // init_with_compaction_info() 将 INC_MAJOR_MERGE 的 action 默认设置为 COMPACTION；
        // 这里改为 LINK，因为只安装 Shared 侧已发布的 Compaction 结果，本地没有
        // input_tables_handle_，不检查输入 SSTable 是否仍存在以及 generation 是否变化。
        param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
      }
      if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(
          tablet_id_, param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(param));
      } else {
        FLOG_INFO("update tablet succ", K(ls_id_), KPC(new_tablet_handle.get_obj()), K(param));
      }
    }
  }
  if (storage_schema) {
    ObStorageSchemaUtil::free_storage_schema(allocator, storage_schema);
  }
  if (OB_NOT_NULL(copied_sstable)) {
    copied_sstable->~ObSSTable();
  }
  return ret;
}

void ObTabletSSLinkMajorTask::after_link_(const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id_, tablet_id_, true/*need_diagnose*/))) {
    LOG_WARN("failed to submit tablet update task to report", KR(tmp_ret), K(ls_id_), K(tablet_id_));
  } else if (OB_TMP_FAIL(MTL(ObLSService*)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls handle fail", KR(tmp_ret), K(ls_id_));
  } else if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id_))) {
    LOG_WARN("failed to update tablet report status", KR(tmp_ret), K(ls_id_), K(tablet_id_));
  }
  if (OB_TMP_FAIL(MTL(ObTenantMediumChecker*)->add_tablet_ls(tablet_id_, ls_id_, compaction_scn_))) {
    LOG_WARN("failed to add tablet ls for check", KR(tmp_ret), K(ls_id_), K(tablet_id_), K(compaction_scn_));
  }
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  if (OB_ISNULL(local_cache_service)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(tmp_ret));
  } else if (OB_TMP_FAIL(local_cache_service->push_old_sstable_evict_task(tablet_id_,
                         ObSSHotTabletOldSSTableEvictInfo(ls_id_, transfer_scn.get_val_for_tx())))) {
    LOG_WARN("fail to push old sstable evict task", KR(tmp_ret), K(tablet_id_), K(ls_id_), K(transfer_scn));
  }
}

void ObTabletSSLinkMajorTask::destroy()
{
}
} // namespace compaction
} // namespace oceanbase

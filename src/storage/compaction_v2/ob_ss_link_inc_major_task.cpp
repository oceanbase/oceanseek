/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_link_inc_major_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"


namespace oceanbase
{
namespace compaction
{
int ObTabletSSLinkIncMajorTask::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LinkIncMajor"));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle ss_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> ss_table_store_wrapper;
  const ObSSTable *inc_major_sstable = nullptr;
  share::SCN transfer_scn;
  share::SCN ss_tablet_meta_scn;
  bool is_exist = false;
  bool need_link = false;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get local ls fail", KR(ret), K(ls_id_));
    // NOTE: use ha_get_tablet here, because the tablet maybe in TRANSFER_OUT state
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("get local tablet fail", KR(ret), K(tablet_id_));
  } else if (OB_FAIL(ObIncMajorTxHelper::check_inc_major_exist(tablet_handle, trans_id_, seq_no_, is_exist))) {
    LOG_WARN("fail to check inc major exist", KR(ret), K(trans_id_), K(seq_no_));
  } else if (is_exist) {
    LOG_INFO("[SS INC MAJOR]The inc major already exists", K(trans_id_), K(seq_no_));
  } else if (FALSE_IT(transfer_scn = tablet_handle.get_obj()->get_reorganization_scn())) {
  } else if (OB_FAIL(find_target_inc_major_(transfer_scn,
                                           allocator,
                                           ss_tablet_handle,
                                           ss_tablet_meta_scn,
                                           ss_table_store_wrapper,
                                           inc_major_sstable))) {
    LOG_WARN("find target major fail", KR(ret));
  } else if (OB_ISNULL(inc_major_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inc major is nullptr", KR(ret));
  } else if (OB_FAIL(check_need_link_(tablet_handle, *inc_major_sstable, need_link))) {
    LOG_WARN("fail to check need link", KR(ret));
  } else if (!need_link) {
    LOG_INFO("[SS INC MAJOR]no need link inc major");
  } else if (OB_FAIL(do_link_(ls_handle, tablet_handle, *ss_tablet_handle.get_obj(), *inc_major_sstable, ss_tablet_meta_scn, allocator))) {
    LOG_WARN("link major fail", KR(ret), K_(compaction_scn));
  } else {
    LOG_INFO("[SS INC MAJOR]link inc major succ",
              KPC(inc_major_sstable), K(ss_tablet_meta_scn), K_(compaction_scn));
  }
  if (OB_FAIL(ret)) {
    FLOG_WARN("[SS INC MAJOR]link major failed",
               KR(ret), K(compaction_scn_), KPC(inc_major_sstable), K_(ls_id),
               K(transfer_scn), K(common::lbt()));
  }
  return ret;
}

int ObTabletSSLinkIncMajorTask::check_need_link_(
    const ObTabletHandle &local_tablet_handle,
    const ObSSTable &inc_major,
    bool &need_link)
{
  int ret = OB_SUCCESS;
  need_link = false;
  int64_t inc_major_version = inc_major.get_upper_trans_version();
  if (OB_UNLIKELY(!local_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (local_tablet_handle.get_obj()->get_last_major_snapshot_version() >= inc_major_version) {
    need_link = false;
    LOG_INFO("[SS INC MAJOR]major version bigger than inc major version, no need link");
  } else {
    need_link = true;
  }
  return ret;
}

int ObTabletSSLinkIncMajorTask::find_target_inc_major_(
    const share::SCN &transfer_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &ss_tablet_handle,
    share::SCN &ss_tablet_meta_scn,
    ObTabletMemberWrapper<ObTabletTableStore> &ss_table_store_wrapper,
    const ObSSTable *&target)
{
  int ret = OB_SUCCESS;
  bool try_old_version = false;
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id_, tablet_id_, transfer_scn, allocator, ss_tablet_handle, ss_tablet_meta_scn))) {
    LOG_WARN("fail to get shared tablet", K(ret));
  } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet is invalid", K(ret), K(ss_tablet_handle));
  } else if (OB_FAIL(find_target_inc_major_(*ss_tablet_handle.get_obj(),
                                           ss_table_store_wrapper,
                                           target,
                                           try_old_version))) {
    if (!try_old_version) {
      LOG_WARN("find target inc major fail", K(ret), K(transfer_scn), K(compaction_scn_));
    } else {
      LOG_INFO("find target inc major fail, try old version", KR(ret), K(transfer_scn), K(compaction_scn_));
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
          if (OB_ITER_END != ret || OB_INVALID_QUERY_TIMESTAMP != ret) {
            LOG_WARN("get next tablet fail", K(ret));
          }
        } else if (OB_FAIL(find_target_inc_major_(*ss_tablet_handle.get_obj(), ss_table_store_wrapper, target, try_old_version))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("find target inc major fail", K(ret), KPC(ss_tablet_handle.get_obj()));
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
        LOG_INFO("suitable shared inc major not found", K(ret), K(start_version), K(end_version), K(compaction_scn_));
      }
    }
  }
  return ret;
}

int ObTabletSSLinkIncMajorTask::find_target_inc_major_(
    ObTablet &ss_tablet,
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    const ObSSTable *&target,
    bool &try_old_version)
{
  int ret = OB_SUCCESS;
  try_old_version = true;
  transaction::ObTransID trans_id;
  transaction::ObTxSEQ seq_no;
  if (OB_FAIL(ss_tablet.fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_inc_major_sstables();
    if (sstables.count() == 0) {
      try_old_version = false;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      ObSSTable *inc_major = sstables.at(i);
      if (OB_ISNULL(inc_major)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major is nullptr", KR(ret), K(i));
      } else if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
                         inc_major, trans_id, seq_no))) {
        LOG_WARN("fail to get trans is and seq no", KR(ret));
      } else if (trans_id == trans_id_ && seq_no == seq_no_) {
        target = inc_major;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(target)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found target inc major", K(ret), K(sstables), K(trans_id_), K(seq_no_));
    }
  }
  return ret;
}

int ObTabletSSLinkIncMajorTask::do_link_(ObLSHandle &ls_handle,
                                         ObTabletHandle &tablet,
                                         ObTablet &ss_tablet,
                                         const ObSSTable &sstable,
                                         const share::SCN &ss_tablet_meta_scn,
                                         ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObSSTable *linked_inc_major = nullptr;
  if (OB_FAIL(ss_tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(ss_tablet));
  } else {
    ObSSTable::ObUpdateIncMajorMetaScnFunc modify_func(ss_tablet_meta_scn);
    if (OB_FAIL(ObSSTable::load_and_copy_sstable(
        sstable, allocator, false/*expand_macro_info*/, &modify_func, linked_inc_major))) {
      LOG_WARN("failed to copy linked inc major", K(ret), K(ss_tablet_meta_scn), K(sstable));
    }
  }
  if (OB_SUCC(ret)) {
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(
        0/*snapshot_version*/,
        ObVersionRange::MIN_VERSION/*multi_version_start*/,
        storage_schema,
        ls_handle.get_ls()->get_rebuild_seq(),
        linked_inc_major,
        true/*allow dup sstable*/);
    param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
    if (OB_FAIL(param.init_with_compaction_info(
                    ObCompactionTableStoreParam(compaction::MEDIUM_MERGE,
                                                SCN::min_scn(),
                                                true/*need_report*/,
                                                ss_tablet.has_merged_with_mds_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(param));
    } else {
      LOG_INFO("update tablet succ", K(ls_id_), KPC(new_tablet_handle.get_obj()), K(param));
    }
  }
  if (storage_schema) {
    ObStorageSchemaUtil::free_storage_schema(allocator, storage_schema);
  }
  if (OB_NOT_NULL(linked_inc_major)) {
    linked_inc_major->~ObSSTable();
  }
  return ret;
}

void ObTabletSSLinkIncMajorTask::destroy()
{
}
} // namespace compaction
} // namespace oceanbase

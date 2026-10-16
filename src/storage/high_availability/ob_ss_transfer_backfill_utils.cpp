/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_transfer_backfill_utils.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/mds_table/ob_mds_table.h"
namespace oceanbase
{
namespace storage
{

/*********** ObSSTransferBackfillUtils ***********/
int ObSSTransferBackfillUtils::check_tablet_need_upload(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const share::SCN &check_scn,
    bool &need_upload)
{
  int ret = OB_SUCCESS;
  need_upload = false;
  ObSSMetaService *meta_service = nullptr;
  lib::ObMemAttr mem_attr(MTL_ID(), "SSTfBackfill");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (!ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service is null", K(ret));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_id, transfer_scn, allocator, tablet_handle))) {
    LOG_WARN("failed to get tablet from meta service", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(ls_id), K(tablet_id));
  } else if (check_scn > tablet->get_clog_checkpoint_scn()) {
    need_upload = true;
  } else {
    need_upload = false;
  }

  if (OB_NOT_NULL(tablet)) {
    LOG_INFO("check tablet need upload", K(ret), K(ls_id), K(tablet_id), K(transfer_scn), K(check_scn),
      K(tablet->get_clog_checkpoint_scn()),
      K(need_upload));
  }

  return ret;
}

// TODO(jyx441808): this method will access 2nd meta (sstable meta), need a flag in 1st meta to optimize ss I/O
int ObSSTransferBackfillUtils::check_tablet_need_backfill_tx(
    const ObTabletHandle &tablet_handle,
    const share::SCN &backfill_scn,
    bool &need_backfill_tx)
{
  int ret = OB_SUCCESS;
  need_backfill_tx = false;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  ObTableStoreIterator table_store_iter;
  SCN min_recycle_scn;
  bool need_scan_sstable = false;

  if (!tablet_handle.is_valid() || !backfill_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (tablet->get_tablet_meta().clog_checkpoint_scn_ < backfill_scn) {
    need_backfill_tx = false;
  } else if (FALSE_IT(min_recycle_scn = tablet->get_min_recycle_scn())) {
  } else if (min_recycle_scn.is_min()) {
    //do not has uncommitted sstable
    need_backfill_tx = false;
  } else if (min_recycle_scn.is_valid_and_not_min()) {
    if (min_recycle_scn >= backfill_scn) {
      need_backfill_tx = false;
    } else if (0 == tablet->get_inc_major_table_count()) {
      need_backfill_tx = true;
    } else {
      need_scan_sstable = true;
    }
  } else {
    need_scan_sstable = true;
  }

  if (OB_SUCC(ret) && need_scan_sstable) {
    // A low min_recycle_scn may come only from an ABORT Inc Major. Scan the
    // actual SSTables so check_inc_major_backfill can ignore it. An invalid
    // min_recycle_scn also needs this scan for compatibility.
    if (OB_FAIL(tablet->fetch_table_store_with_prewarm(table_store_wrapper))) {
      LOG_WARN("failed to fetch table store", K(ret), K(tablet_handle));
    } else if (OB_ISNULL(table_store = table_store_wrapper.get_member())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table store is null", K(ret), K(tablet_handle));
    } else if (OB_FAIL(table_store->get_all_sstable(table_store_iter))) {
      LOG_WARN("failed to get all sstable", K(ret), K(tablet_handle));
    } else {
      ObTableHandleV2 table_handle;
      while (OB_SUCC(ret)) {
        table_handle.reset();
        const ObSSTable *sstable = nullptr;
        bool is_trans_abort = false;
        if (OB_FAIL(table_store_iter.get_next(table_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get table handle", K(ret), K(tablet_handle));
          }
        } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
          LOG_WARN("failed to get sstable", K(ret), K(tablet_handle), K(table_handle));
        } else if (OB_ISNULL(sstable)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("sstable should not be NULL", K(ret), K(sstable), K(table_handle), K(tablet_handle));
        } else if (sstable->is_inc_major_type_sstable()) {
          if (OB_FAIL(ObTransferUtils::check_inc_major_backfill(tablet->get_ls_id(), backfill_scn, table_handle, need_backfill_tx, is_trans_abort))) {
            LOG_WARN("failed to check inc major backfill", K(ret), KPC(tablet), K(backfill_scn), K(table_handle));
          } else if (need_backfill_tx) {
            break;
          }
        } else if (!sstable->is_minor_sstable()) {
          //do nothing
        } else if (sstable->contain_uncommitted_row() && sstable->get_filled_tx_scn() < backfill_scn) {
          need_backfill_tx = true;
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !need_backfill_tx) {
    const int64_t minor_table_count = tablet->get_minor_table_count();
    if (minor_table_count > compaction::ObPartitionMergePolicy::OB_EMERGENCY_TABLE_CNT) {
      need_backfill_tx = true;
      FLOG_INFO("tablet minor table count is bigger than transfer backfill limit, need backfill",
        K(tablet_handle), K(backfill_scn), K(minor_table_count));
    }
  }

  return ret;
}

int ObSSTransferBackfillUtils::check_tablet_need_replace_table(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &need_replace_table)
{
  int ret = OB_SUCCESS;
  need_replace_table = false;
  ObSSMetaService *meta_service = nullptr;
  lib::ObMemAttr mem_attr(MTL_ID(), "SSTfBackfill");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (!ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service is null", K(ret));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_id, transfer_scn, allocator, tablet_handle))) {
    LOG_WARN("failed to get tablet from meta service", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_id));
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    need_replace_table = true;
  } else {
    need_replace_table = false;
  }

  return ret;
}

int ObSSTransferBackfillUtils::check_ss_tablet_ready(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = false;
  ObSSMetaService *meta_service = nullptr;
  lib::ObMemAttr mem_attr(MTL_ID(), "SSTfBackfill");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (!ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service is null", K(ret));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_id, transfer_scn, allocator, tablet_handle))) {
    LOG_WARN("failed to get tablet from meta service", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_id));
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    is_ready = false;
  } else {
    is_ready = true;
  }
  return ret;
}

int ObSSTransferBackfillUtils::check_self_is_sswriter(
    const ObSSWriterType type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &is_sswriter)
{
  int ret = OB_SUCCESS;
  is_sswriter = false;
  int64_t epoch = 0;
  ObSSWriterKey key(type, ls_id, tablet_id);

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sswriter key", K(ret), K(key));
  } else if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, is_sswriter, epoch))) {
    LOG_WARN("check lease fail", K(ret), K(key));
  }

  return ret;
}

int ObSSTransferBackfillUtils::notify_sswriter_do_backfill(
    const ObSSWriterType type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSWriterKey key(type, ls_id, tablet_id);
  ObSSWriterService *sswriter_service = nullptr;
  ObSSWriterAddr sswriter_addr;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sswriter key", K(ret), K(key));
  } else if (OB_ISNULL(sswriter_service = MTL(ObSSWriterService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service is null", K(ret));
  } else if (OB_FAIL(sswriter_service->get_sswriter_addr(key, sswriter_addr))) {
    LOG_WARN("failed to get sswriter addr", K(ret), K(key));
  } else if (!sswriter_addr.is_valid()) {
    //ss writer may no master, sswriter_addr is not valid should not send rpc
    ret = OB_NOT_MASTER;
    LOG_WARN("sswriter addr is not valid", K(ret), K(key), K(sswriter_addr));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc is null", K(ret), K(sswriter_addr));
  } else {
    ObStorageHASrcInfo src_info;
    src_info.cluster_id_ = GCONF.cluster_id;
    src_info.src_addr_ = sswriter_addr.addr_;
    uint64_t tenant_id = MTL_ID();

    if (OB_FAIL(storage_rpc->notify_sswriter_do_backfill(tenant_id, ls_id, tablet_id, type, src_info))) {
      LOG_WARN("failed to notify sswriter do backfill",
        K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(type), K(src_info));
    }
  }
  return ret;
}

int ObSSTransferBackfillUtils::get_ss_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    const bool read_mark_delete)
{
  share::SCN meta_scn;
  return get_ss_tablet(ls_id, tablet_id, reorg_scn, allocator, tablet_handle, meta_scn, read_mark_delete);
}

int ObSSTransferBackfillUtils::get_ss_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    share::SCN &meta_scn,
    const bool read_mark_delete)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  meta_scn.reset();
  ObSSMetaService *meta_service = nullptr;

  if (!ls_id.is_valid() || !tablet_id.is_valid() || !reorg_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ss tablet get invalid argument", K(ret), K(ls_id), K(tablet_id), K(reorg_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be NULL", K(ret), K(ls_id), K(tablet_id));
  } else if (read_mark_delete) {
    if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_id, reorg_scn, allocator, tablet_handle, meta_scn))) {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id), K(reorg_scn));
    }
  } else {
    ObSSMetaUpdateMetaInfo update_meta_info;
    if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_id, reorg_scn, allocator,
        tablet_handle, update_meta_info, meta_scn))) {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id), K(reorg_scn));
    }
  }
  return ret;
}

int ObSSTransferBackfillUtils::create_ss_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service2 = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create ss tablet get invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(meta_service2 = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service v2 should not be NULL", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id), K(tablet_id), K(tablet_handle));
  } else if (OB_FAIL(meta_service2->create_tablet(ls_id, tablet_id, tablet->get_reorganization_scn(), true/*check_lease*/))) {
    LOG_WARN("failed to create ss tablet v2", K(ret), K(ls_id), K(tablet_id), KPC(tablet));
  } else {
    LOG_INFO("success to create ss tablet", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObSSTransferBackfillUtils::get_ss_ls_meta(
    const ObLSID &ls_id,
    ObSSLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  ls_meta.reset();
  ObSSMetaService *meta_service = nullptr;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create ss tablet get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be NULL", K(ret), K(ls_id));
  } else if (OB_FAIL(meta_service->get_ls_meta(ls_id, ls_meta))) {
    LOG_WARN("failed to get ss ls meta", K(ret), K(ls_id));
  }
  return ret;
}

int ObSSTransferBackfillUtils::check_ss_src_tablet_ready(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    const share::SCN &backfill_scn,
    bool &is_tablet_exist,
    bool &is_data_uploaded,
    bool &is_data_backfilled)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(MTL_ID(), "SSTfBackfill");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle tablet_handle;
  ObTablet *ss_src_tablet = nullptr;
  bool need_backfill_tx = false;
  is_tablet_exist = false;
  is_data_uploaded = false;
  is_data_backfilled = false;

  if (!ls_id.is_valid() || !tablet_id.is_valid() || !reorg_scn.is_valid() || !backfill_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check ss src tablet ready get invalid argument", K(ret), K(ls_id), K(tablet_id), K(reorg_scn), K(backfill_scn));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id, tablet_id, reorg_scn, allocator,
      tablet_handle, true /*read_mark_delete*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      is_tablet_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ss tablet", K(ret), K(ls_id), K(tablet_id), K(reorg_scn));
    }
  } else if (OB_ISNULL(ss_src_tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), K(ls_id), K(tablet_id), K(reorg_scn));
  } else if (FALSE_IT(is_tablet_exist = true)) {
  } else if (ss_src_tablet->get_tablet_meta().clog_checkpoint_scn_ < backfill_scn) {
    is_data_uploaded = false;
  } else if (FALSE_IT(is_data_uploaded = true)) {
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_need_backfill_tx(tablet_handle, backfill_scn,
      need_backfill_tx))) {
    LOG_WARN("failed to check tablet need backfill tx", K(ret), K(tablet_handle), K(backfill_scn));
  } else if (need_backfill_tx) {
    is_data_backfilled = false;
  } else {
    is_data_backfilled = true;
  }

  return ret;
}

int ObSSTransferBackfillUtils::check_tablet_mds_reloc_done(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_done)
{
  int ret = OB_SUCCESS;
  ObMdsTable *mds_table = nullptr;
  ObTabletMdsRelocStatus reloc_status;
  is_done = false;

  if (OB_ISNULL(ls) || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(mds_table = ls->get_mds_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds table should not be NULL", K(ret), K(tablet_id), K(transfer_scn), KPC(ls));
  } else if (OB_FAIL(mds_table->get_latest_committed(tablet_id, transfer_scn, 5_s, reloc_status))) {
    if (OB_EMPTY_RESULT == ret || OB_ITER_END == ret || OB_SNAPSHOT_DISCARDED == ret) {
      ret = OB_SUCCESS;
      is_done = false;
    } else {
      LOG_WARN("failed to get mds reloc status from mds table",
          K(ret), K(tablet_id), K(transfer_scn), KPC(ls));
    }
  } else {
    is_done = reloc_status.reloc_done_;
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc done", K(ret), K(is_done), K(tablet_id), K(transfer_scn),
        K(reloc_status), "ls_id", ls->get_ls_id());
  }
  return ret;
}

int ObSSTransferBackfillUtils::check_tablet_mds_reloc_done(
    const share::ObLSID &dest_ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_done)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle dest_ls_handle;
  ObLS *dest_ls = nullptr;
  is_done = false;

  if (!dest_ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(tmp_ret, "ls service should not be NULL", K(dest_ls_id), K(tablet_id));
  } else if (OB_SUCCESS != (tmp_ret = ls_service->get_ls(dest_ls_id, dest_ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_INFO("local dest ls is unavailable when checking mds reloc done",
        K(tmp_ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(tmp_ret, "dest ls should not be NULL", K(dest_ls_id), K(tablet_id));
  } else if (OB_SUCCESS != (tmp_ret = check_tablet_mds_reloc_done(
      dest_ls, tablet_id, transfer_scn, is_done))) {
    LOG_INFO("failed to read local dest tablet mds reloc marker, treat as not done",
        K(tmp_ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  }

  if (OB_SUCCESS != tmp_ret) {
    is_done = false;
  }
  return ret;
}


} //storage
} //oceanbase

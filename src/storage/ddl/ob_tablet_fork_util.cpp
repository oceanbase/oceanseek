/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "ob_tablet_fork_util.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/incremental/ob_inc_ss_macro_seq_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/ob_storage_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::compaction;

void ObForkDataSSHelper::reset()
{
  if (OB_NOT_NULL(sstable_protocol_)) {
    if (sstable_protocol_->is_status_start()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(sstable_protocol_->abort())) {
        LOG_WARN_RET(tmp_ret, "[FORKTABLE] abort fork sstable protocol failed");
      }
    }
    sstable_protocol_->~ObSSTableAtomicProtocol();
    allocator_.free(sstable_protocol_);
    sstable_protocol_ = nullptr;
  }
  total_macro_seq_slot_cnt_ = 0;
  allocator_.reset();
}

int ObForkDataSSHelper::get_op_id(int64_t &op_id) const
{
  int ret = OB_SUCCESS;
  op_id = 0;
  if (OB_ISNULL(sstable_protocol_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork sstable protocol is null", K(ret));
  } else if (OB_FAIL(sstable_protocol_->get_op_id(op_id))) {
    LOG_WARN("[FORKTABLE] get op id from fork sstable protocol failed", K(ret));
  }
  return ret;
}

int ObForkDataSSHelper::get_ctrl_key_info(ObSSMetaCtrlKeyInfo &ctrl_key_info) const
{
  int ret = OB_SUCCESS;
  ctrl_key_info.reset();
  if (OB_ISNULL(sstable_protocol_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork sstable protocol is null", K(ret));
  } else if (OB_FAIL(sstable_protocol_->get_ctrl_key_info(ctrl_key_info))) {
    LOG_WARN("[FORKTABLE] get ctrl key info from fork sstable protocol failed", K(ret));
  }
  return ret;
}

// Data and index tree slots share one compact global sequence space in the atomic operation.
int ObForkDataSSHelper::start_add_op_(const share::ObLSID &ls_id, const share::SCN &snapshot_scn,
    const share::SCN &reorg_scn, const int64_t total_macro_seq_slot_cnt,
    const common::ObTabletID &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != sstable_protocol_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork sstable protocol already started", K(ret), K(dst_tablet_id));
  } else if (OB_UNLIKELY(total_macro_seq_slot_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork minor macro sequence slot count", K(ret), K(total_macro_seq_slot_cnt));
  } else if (OB_FAIL(ObIncSSMacroSeq::check_parallel_slot_count(total_macro_seq_slot_cnt))) {
    LOG_WARN("[FORKTABLE] fork minor macro sequence slot count exceeds the version-local limit",
        K(ret), K(total_macro_seq_slot_cnt));
  } else if (OB_ISNULL(sstable_protocol_ = OB_NEWx(ObSSTableAtomicProtocol, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FORKTABLE] alloc fork sstable protocol failed", K(ret));
  } else {
    ObSSMetaCtrlObjectInfo ctrl_info;
    ObAtomicProtocolType atomic_protocol_type;
    atomic_protocol_type.type_ = ObAtomicProtocolTypeEnum::MINOR_SSTABLE;
    if (OB_FAIL(ctrl_info.init_by_none_major(snapshot_scn, total_macro_seq_slot_cnt))) {
      LOG_WARN("[FORKTABLE] fail to init ctrl info for fork minor", KR(ret));
    }
    if (FAILEDx(sstable_protocol_->init(atomic_protocol_type, MTL_ID(), ls_id, dst_tablet_id, reorg_scn, ctrl_info))) {
      LOG_WARN("[FORKTABLE] fail to init fork sstable protocol", KR(ret), K(atomic_protocol_type), K(ls_id),
          K(dst_tablet_id), K(reorg_scn));
    } else if (OB_FAIL(sstable_protocol_->create(true/*need_check_sswriter*/))) {
      LOG_WARN("[FORKTABLE] fail to create fork sstable protocol", KR(ret), K(ls_id), K(dst_tablet_id));
    } else {
      total_macro_seq_slot_cnt_ = total_macro_seq_slot_cnt;
      LOG_INFO("[FORKTABLE] started fork minor atomic operation", K(total_macro_seq_slot_cnt), K(dst_tablet_id));
    }
  }
  return ret;
}

int ObForkDataSSHelper::start_add_minor_op(const share::ObLSID &ls_id, const share::SCN &fork_snapshot_scn,
    const share::SCN &dst_reorg_scn, const int64_t total_macro_seq_slot_cnt,
    const common::ObTabletID &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !fork_snapshot_scn.is_valid()
      || !dst_reorg_scn.is_valid()
      || total_macro_seq_slot_cnt < 1
      || !dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid arg", K(ret), K(ls_id), K(fork_snapshot_scn), K(dst_reorg_scn),
        K(total_macro_seq_slot_cnt), K(dst_tablet_id));
  } else if (OB_FAIL(start_add_op_(ls_id, fork_snapshot_scn, dst_reorg_scn, total_macro_seq_slot_cnt, dst_tablet_id))) {
    LOG_WARN("[FORKTABLE] start fork minor op failed", K(ret), K(total_macro_seq_slot_cnt), K(dst_tablet_id));
  }
  return ret;
}

int ObForkDataSSHelper::finish_add_op(const bool need_finish)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable_protocol_)) {
    if (need_finish) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] defensive check, fork sstable protocol is null", K(ret));
    } else {
    }
  } else if (need_finish) {
    ObSSMetaAtomicSSTableOp finish_op;
    if (OB_FAIL(sstable_protocol_->finish(finish_op))) {
      LOG_WARN("[FORKTABLE] finish fork sstable protocol failed", K(ret));
    }
  } else if (!sstable_protocol_->is_status_complete()
      && !sstable_protocol_->is_status_abandon()) {
    if (OB_FAIL(sstable_protocol_->abort())) {
      LOG_WARN("[FORKTABLE] abort fork sstable protocol failed", K(ret));
    }
  }
  return ret;
}

int ObForkDataSSHelper::generate_minor_macro_seq_info(const int64_t global_slot_idx, int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  if (OB_UNLIKELY(!has_started_op())) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork minor atomic operation is not started", K(ret), K(global_slot_idx));
  } else if (OB_UNLIKELY(global_slot_idx < 0 || global_slot_idx >= total_macro_seq_slot_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork minor macro sequence slot", K(ret), K(global_slot_idx),
        K(total_macro_seq_slot_cnt_));
  } else if (OB_FAIL(ObIncSSMacroSeq::build_parallel_start_seq(global_slot_idx, macro_start_seq))) {
    LOG_WARN("[FORKTABLE] build fork parallel start seq failed", K(ret), K(global_slot_idx));
  } else {
    LOG_INFO("[FORKTABLE] built fork minor macro sequence", K(global_slot_idx),
        K(total_macro_seq_slot_cnt_), K(macro_start_seq));
  }
  return ret;
}

int ObForkTabletUtil::build_fork_replace_table_store_param(const share::SCN &reorg_scn,
    const int64_t ls_rebuild_seq, const int64_t snapshot_version, const int64_t multi_version_start,
    const common::ObTabletID &dst_tablet_id, const ObTablesHandleArray &tables_handle,
    const share::SCN &clog_checkpoint_scn, ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  if (OB_UNLIKELY(!reorg_scn.is_valid() || !dst_tablet_id.is_valid() || snapshot_version < 0
      || multi_version_start < 0 || !clog_checkpoint_scn.is_valid() || tables_handle.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork replace param", K(ret), K(reorg_scn), K(ls_rebuild_seq),
        K(snapshot_version), K(multi_version_start), K(dst_tablet_id), K(clog_checkpoint_scn), K(tables_handle));
  } else if (OB_FAIL(param.tables_handle_.assign(tables_handle))) {
    LOG_WARN("[FORKTABLE] failed to assign fork replace tables", K(ret), K(tables_handle));
  } else {
    param.tablet_fork_param_.snapshot_version_ = snapshot_version;
    param.tablet_fork_param_.multi_version_start_ = multi_version_start;
    param.tablet_fork_param_.clog_checkpoint_scn_ = clog_checkpoint_scn;
    param.rebuild_seq_ = ls_rebuild_seq;
    param.allow_cut_sstable_range_ = false;
    param.reorg_scn_ = reorg_scn;
  }
  return ret;
}

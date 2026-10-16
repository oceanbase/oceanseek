// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_minor_compaction.h"
#include "ob_sswriter_service.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "ob_ss_tablet_merge_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/ob_ss_diagnose_mgr.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "storage/incremental/ob_sstable_utils.h"
#include "storage/incremental/ob_inc_ss_macro_seq_define.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_ss_tablet_merge_helper.h"

//TODO(mingqiao): atomic protocol should be replaced later
namespace oceanbase
{
namespace compaction
{
int ObTabletSSMinorMergeCtx::get_ls_and_tablet()
{
  int ret = OB_SUCCESS;
  ObLSHandle &ls_handle = static_param_.ls_handle_;
  if (is_upload_minor_exec_mode(get_exec_mode())) {
    // for upload minor, use local tablet handle
    ret = ObBasicTabletMergeCtx::get_ls_and_tablet();
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(static_param_.get_ls_id(), ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(static_param_.get_ls_id()));
  } else if (ls_handle.get_ls()->is_offline()) {
    ret = OB_CANCELED;
    LOG_INFO("ls offline, skip merge", K(ret), "param", get_dag_param());
  } else if (FALSE_IT(static_param_.ls_rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq())) {
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(static_param_.get_ls_id(),
                                                        static_param_.get_tablet_id(),
                                                        get_dag_param().reorganization_scn_,
                                                        mem_ctx_.get_allocator(),
                                                        tablet_handle_))) {
    LOG_WARN("get ss tablet fail", K(ret));
  } else if (!tablet_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is invalid", K(ret));
  } else {
    LOG_INFO("succ to get tablet", K(ret), K(static_param_));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::get_snapshot_version(share::SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version_val = get_snapshot();
  if (OB_UNLIKELY(snapshot_version_val <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snapshot version", KR(ret), K(snapshot_version_val));
  } else {
    snapshot_version.convert_for_tx(snapshot_version_val);
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::build_minor_ctrl_info_(ObSSMetaCtrlObjectInfo &ctrl_info)
{
  int ret = OB_SUCCESS;
  const int64_t concurrent_cnt = get_concurrent_cnt();
  int64_t parallel_cnt = 0;
  SCN snapshot_version;
  if (OB_UNLIKELY(concurrent_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minor parallel count", K(ret), K(concurrent_cnt));
  } else if (OB_UNLIKELY(concurrent_cnt > INT64_MAX - INDEX_TREE_MACRO_SEQ_SLOT_CNT)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("minor parallel count overflow", K(ret), K(concurrent_cnt));
  } else if (FALSE_IT(parallel_cnt = concurrent_cnt + INDEX_TREE_MACRO_SEQ_SLOT_CNT)) {
  } else if (OB_FAIL(get_snapshot_version(snapshot_version))) {
    LOG_WARN("fail to get snapshot version", KR(ret));
  } else if (OB_FAIL(ctrl_info.init_by_none_major(snapshot_version, parallel_cnt))) {
    LOG_WARN("fail to init meta_ctr_obj_info", KR(ret), K(parallel_cnt));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::create_op_()
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlObjectInfo ctrl_info;
  if (OB_FAIL(build_minor_ctrl_info_(ctrl_info))) {
    LOG_WARN("fail to build minor ctrl info", KR(ret));
  }

  ObAtomicProtocolType protocol_type;
  protocol_type.type_ = get_atomic_protocol_type_enum_();
  const ObLSID &ls_id = get_ls_id();
  const ObTabletID &tablet_id = get_tablet_id();
  const SCN &reorgan_scn = get_dag_param().reorganization_scn_;
  if (FAILEDx(sstable_protocol_.init(protocol_type, MTL_ID(), ls_id, tablet_id, reorgan_scn, ctrl_info))) {
    LOG_WARN("fail to init sstable protocol", KR(ret), K(protocol_type), K(ls_id), K(tablet_id),
      K(reorgan_scn), K(ctrl_info));
  } else if (OB_FAIL(sstable_protocol_.create(true/*need_check_sswriter*/))) {
    LOG_WARN("fail to create sstable protocol", KR(ret));
  } else if (OB_FAIL(sstable_protocol_.get_op_id(op_id_))) {
    LOG_WARN("fail to get op_id", KR(ret));
  } else {
    static_desc_.op_id_ = op_id_;
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_SS_MINOR_MERGE)
int ObTabletSSMinorMergeCtx::get_merge_tables(ObGetMergeTablesResult &merge_tables_result)
{
  int ret = OB_SUCCESS;
  int64_t match_value = (get_ls_id().id() << 37) | (int64_t)get_tablet_id().id();
  if (EN_SS_MINOR_MERGE.test(match_value)) {
    ret = OB_EAGAIN;
    LOG_INFO("TRACEPOINT: EN_SS_MINOR_MERGE : LSID | TabletID", K(ret), K(get_ls_id()), K(get_tablet_id()));
  } else if (OB_FAIL(check_lease_())) {
    LOG_WARN("not minor sswriter, can not merge", K(ret));
  } else if (OB_FAIL(ObTabletExeMergeCtx::get_tables_by_key(merge_tables_result))) {
    LOG_WARN("get merge tables by table key fail", K(ret));
  }
  LOG_INFO("get merge tables done", K(ret), KPC(this));
  return ret;
}

ObStorageObjectWriteStrategy ObTabletSSMinorMergeCtx::get_io_write_strategy() const
{
  return ObStorageObjectWriteStrategy::WRITE_THROUGH;
}

int ObTabletSSMinorMergeCtx::cal_merge_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletExeMergeCtx::cal_merge_param())) {
    LOG_WARN("failed to cal merge param", KR(ret), KPC(this));
  } else if (EXEC_MODE_OUTPUT == get_exec_mode()
             && OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), static_param_.data_version_))) {
    LOG_WARN("failed to get data version", KR(ret), KPC(this));
  } else {
    LOG_INFO("succ get data version for compatible", K(static_param_.data_version_));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletMergeCtx::init_tablet_merge_info())) {
    LOG_WARN("failed to init tablet merge info", KR(ret), KPC(this));
  } else if (OB_FAIL(create_op_())) {
    LOG_WARN("fail to create op", K(ret));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::prepare_compaction_filter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletExeMergeCtx::prepare_compaction_filter())) {
    LOG_WARN("prepare compact filter fail", K(ret));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  ObStorageSchema *schema = nullptr;
  if (OB_FAIL(get_storage_schema(schema))) {
    LOG_WARN("get storage schema fail", K(ret));
  } else {
    static_param_.schema_ = schema;
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::prepare_index_tree()
{
  return ObBasicTabletMergeCtx::build_index_tree(
      merge_info_,
      &get_tablet()->get_rowkey_read_info());
}

void ObTabletSSMinorMergeCtx::free_schema()
{
  ObBasicTabletMergeCtx::free_schema();
}

int ObTabletSSMinorMergeCtx::generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIncSSMacroSeq::build_parallel_start_seq(task_idx, macro_start_seq))) {
    LOG_WARN("failed to build parallel start seq", K(ret), K(task_idx));
  } else {
    LOG_INFO("succ build parallel start seq for minor merge", K_(op_id), K(task_idx), K(macro_start_seq));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::get_index_tree_macro_start_seq(int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  const int64_t concurrent_cnt = get_concurrent_cnt();
  if (OB_UNLIKELY(concurrent_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minor concurrent count", K(ret), K(concurrent_cnt));
  } else if (OB_FAIL(ObIncSSMacroSeq::build_parallel_start_seq(
      concurrent_cnt, macro_start_seq))) {
    LOG_WARN("failed to build index tree macro seq", K(ret), K(concurrent_cnt));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::build_update_table_store_param(const ObSSTable *sstable, ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletExeMergeCtx::build_update_table_store_param(sstable, param))) {
    LOG_WARN("failed to build table store param", KR(ret), K(param));
  } else if (is_upload_minor_exec_mode(get_exec_mode())) {
    param.is_ss_upload_minor_sstable_ = true;
    param.compaction_info_.clog_checkpoint_scn_ = sstable->get_end_scn();
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::update_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObSSTable *sstable = NULL;
  ObUpdateTableStoreParam param;
  if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else if (OB_FAIL(build_update_table_store_param(sstable, param))) {
    LOG_WARN("failed to build table store param", KR(ret), K(param));
  }
  ObMetaUpdateReason reason;
  if (FILL_TX_TYPE_BACKFILL == static_param_.fill_tx_type_) {
    reason = ObMetaUpdateReason::TABLET_BACKFILL_ADD_SSTABLE;
  } else if (is_upload_minor_exec_mode(get_exec_mode())) {
    reason = ObMetaUpdateReason::TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE;
  } else {
    reason = ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE;
  }
  if (OB_SUCC(ret) && is_upload_minor_exec_mode(get_exec_mode())) {
    const share::SCN modified_start_scn = get_dag_param().modified_start_scn_;
    if (!modified_start_scn.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("modified start scn is invalid", K(ret), K(modified_start_scn));
    } else if (modified_start_scn != sstable->get_start_scn()) {
      ObSSTable *copied_sstable = NULL;
      blocksstable::ObModifySSTableStartScnFunc modify_func(modified_start_scn);
      if (OB_FAIL(blocksstable::load_and_copy_sstable(*sstable, mem_ctx_.get_allocator(), true, &modify_func, copied_sstable))) {
        LOG_WARN("failed to load and copy sstable start scn", K(ret));
      } else {
        param.sstable_ = copied_sstable;
      }
    }
  }
  // the input tables handle is used to verify table store state is consistent with this minor merge
  // if some table has been deleted by GC, but its macro block is still refered by minor result sstable
  // then the minor merge must be aborted
  if (OB_SUCC(ret) && !is_upload_minor_exec_mode(get_exec_mode())) {
    param.input_tables_handle_ = &static_param_.tables_handle_;
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(abort_op_())) {
      LOG_WARN("fail to execute abort_op", KR(ret), KR(tmp_ret));
    }
  } else if (OB_FAIL(finish_op_())) {
    // finish sstable protocol before update: update_tablet_table_store requires the ctrl row in complete status
    LOG_WARN("fail to execute finish_op before update tablet table store", KR(ret));
  } else {
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
      LOG_WARN("fail to get ctrl key info", KR(ret));
    } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
      LOG_WARN("fail to push back ctrl key info", KR(ret));
    } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(get_ls_id(),
                                                                           get_tablet_id(),
                                                                           get_dag_param().reorganization_scn_,
                                                                           reason,
                                                                           param,
                                                                           meta_ctrl_keys,
                                                                           true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail v2", KR(ret));
    }
    if (OB_SUCC(ret)) {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_SHARED_TABLET);
    }
  }
  // for upload minor, need update local tablet also.
  if (OB_SUCC(ret) && is_upload_minor_exec_mode(get_exec_mode())) {
    param.sstable_ = sstable;
    param.is_ss_upload_minor_sstable_ = false;
    if (OB_FAIL(get_ls()->update_tablet_table_store(get_tablet_id(), param, new_tablet_handle))) {
      LOG_WARN("failed to update local tablet table store", K(ret), K(param), K(new_tablet_handle));
      CTX_SET_DIAGNOSE_LOCATION(*this);
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_LOCAL_TABLET);
    }
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MINOR_MERGE_SUCCESS_COUNT);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MINOR_MERGE_TOTAL_MACRO_BLOCK_COUNT, merge_info_.get_merge_history().block_info_.macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MINOR_MERGE_REUSE_MACRO_BLOCK_COUNT, merge_info_.get_merge_history().block_info_.multiplexed_macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MINOR_MERGE_EXECUTE_TIME, merge_info_.get_merge_history().running_info_.execute_time_);
  } else {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MINOR_MERGE_FAILURE_COUNT);
  }
  LOG_INFO("update tablet", K(ret), KPC(sstable), KPC(this));
  return ret;
}

int ObTabletSSMinorMergeCtx::finish_op_()
{
  int ret = OB_SUCCESS;
  ObSSMetaAtomicSSTableOp finish_op;
  if (OB_FAIL(sstable_protocol_.finish(finish_op))) {
    LOG_WARN("fail to finish sstable protocol", KR(ret));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::abort_op_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_protocol_.abort())) {
    LOG_WARN("fail to abort sstable protocol", KR(ret));
  }
  return ret;
}

int ObTabletSSMinorMergeCtx::get_minor_op_id_(uint64_t &op_id)
{
  int ret = OB_SUCCESS;
  op_id = 0;
  if (OB_UNLIKELY(-1 == op_id_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss minor merge ctx do not init", K(ret));
  } else {
    op_id = op_id_;
  }
  return ret;
}

ObAtomicProtocolTypeEnum ObTabletSSMinorMergeCtx::get_atomic_protocol_type_enum_() const
{
  return ObAtomicProtocolTypeEnum::MINOR_SSTABLE;
}

int ObTabletSSMinorMergeCtx::check_lease_()
{
  int ret = OB_SUCCESS;
  bool is_sswriter = false;
  ObSSWriterType type = ObSSWriterType::COMPACTION;
  if (is_upload_minor_exec_mode(get_exec_mode())) {
    type = ObSSWriterType::FREEZE;
  }
  if (OB_FAIL(ObSSTabletMergeHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), type, is_sswriter))) {
    LOG_WARN("check sswriter lease fail", K(ret));
  } else if (!is_sswriter) {
    ret = OB_NOT_MASTER;
    LOG_WARN("current is not sswriter", K(ret));
  }
  return ret;
}

void ObTabletSSMinorMergeCtx::destroy()
{
  int ret = OB_SUCCESS;
  if (!sstable_protocol_.is_status_complete() && !sstable_protocol_.is_status_abandon()) {
    if (OB_FAIL(sstable_protocol_.abort())) {
      LOG_WARN("fail to abort sstable protocol", KR(ret));
    }
  }
  free_schema();
}

bool ObTabletSSMinorMergeHelper::can_schedule(const ObLSID &ls_id, const ObTabletID &tablet_id)
{
  return ObSSTabletMergeHelper::can_schedule_ss_minor(ls_id, tablet_id);
}

void ObTabletSSMinorMergeHelper::diagnose(const ObLSID &ls_id,
                                          const ObTabletID &tablet_id,
                                          ObSSDiagnoseInfo *info_array,
                                          int64_t &idx,
                                          const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  int64_t minor_compact_trigger = 0;
  if (idx >= max_cnt) {
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 10_s, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet fail", K(tablet_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      minor_compact_trigger = tenant_config->minor_compact_trigger;
    }
  }
  if (OB_SUCC(ret) && minor_compact_trigger > 0 && idx < max_cnt
      && tablet_handle.get_obj()->get_minor_table_count() > minor_compact_trigger * 3) {
    const ObTablet &tablet = *tablet_handle.get_obj();
    common::ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSDiag"));
    ObSSMetaUpdateMetaInfo update_meta_info;
    share::SCN row_scn;
    ObTabletHandle ss_tablet_handle;
    bool ss_too_many_minor = false;
    if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id,
                                                  tablet_id,
                                                  tablet_handle.get_obj()->get_reorganization_scn(),
                                                  allocator,
                                                  ss_tablet_handle,
                                                  update_meta_info,
                                                  row_scn))) {
      LOG_WARN("get ss tablet fail", K(ret));
      ObSSDiagnoseInfo &info = info_array[idx++];
      SET_SS_DIAGNOSE_INFO(info,
                           ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_MINOR,
                           ls_id,
                           tablet_id,
                           ObTimeUtility::current_time_us(),
                           "warning", "get ss tablet minor count fail",
                           "ret", ret);
    } else {
      int64_t minor_cnt = update_meta_info.table_store_meta_info_.minor_tables_cnt_;
      if (minor_cnt > minor_compact_trigger * 3) {
        ss_too_many_minor = true;
        ObSSDiagnoseInfo &info = info_array[idx++];
        SET_SS_DIAGNOSE_INFO(info,
                         ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_MINOR,
                         ls_id,
                         tablet_id,
                         ObTimeUtility::current_time_us(),
                         "warning", "tablet has too many minor sstables, maybe minor merge stuck",
                         "minor_cnt", minor_cnt,
                         "minor_compact_trigger", minor_compact_trigger);
      }
    }
  }
}

} // compaction
} // oceanbase

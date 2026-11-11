// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/compaction/ob_medium_list_checker.h"
#include "ob_ss_compact_helper.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ob_ss_tablet_merge_helper.h"
#include "ob_ss_major_merge_ctx.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_sstable_writer.h"
#include "lib/statistic_event/ob_stat_event.h"


namespace oceanbase
{
namespace compaction
{
static int link_major_if_exist_(ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSCompactHelper::link_major(ctx.get_ls_id(),
                                          ctx.get_tablet_id(),
                                          ctx.get_inner_table_merge_type(),
                                          ctx.get_merge_version(),
                                          ctx.get_merge_base_version(),
                                          true,
                                          true))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("major not exist in shared tablet");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("link major fail", K(ret));
    }
  } else {
    ret = OB_NO_NEED_MERGE;
  }
  return ret;
}

void ObSSTabletMajorMergeCtx::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inc_major_merge(get_merge_type())
      && sstable_protocol_.is_inited()
      && sstable_protocol_.is_status_start()
      && OB_TMP_FAIL(sstable_protocol_.abort())) {
    LOG_WARN_RET(tmp_ret, "abort unfinished inc major compaction protocol fail",
        K(sstable_protocol_));
  }
}

int ObSSTabletMajorMergeCtx::generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  macro_start_seq = task_idx * MACRO_STEP_SIZE;
  if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro start seq is size overflow", K(ret), K(task_idx));
  }
  return ret;
}

int ObSSTabletMajorMergeCtx::get_index_tree_macro_start_seq(int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_cnt_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro sequence task count", K(ret), K(task_cnt_));
  } else {
    macro_start_seq = task_cnt_ * MACRO_STEP_SIZE;
    if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("macro start seq is size overflow", K(ret), K(task_cnt_));
    }
  }
  return ret;
}

ObStorageObjectWriteStrategy ObSSTabletMajorMergeCtx::get_io_write_strategy() const
{
  if (ObSSTabletMergeHelper::is_hot_tablet(get_tablet_id())
      && !is_inc_major_merge(get_merge_type())) {
    return ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  } else {
    return ObStorageObjectWriteStrategy::WRITE_THROUGH;
  }
}

ERRSIM_POINT_DEF(EN_SS_MAJOR_MERGE)
int ObSSTabletMajorMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  int64_t epoch = 0;
  const bool is_inc_major_compaction = is_inc_major_merge(get_merge_type());
  if (EN_SS_MAJOR_MERGE == -1) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(20_s)) {
      FLOG_INFO("TRACEPOINT: EN_SS_MAJOR_MERGE : ALL", K(ret));
    }
  } else if (EN_SS_MAJOR_MERGE == -((get_ls_id().id() << 37) | (int64_t)get_tablet_id().id())) {
    ret = OB_EAGAIN;
    FLOG_INFO("TRACEPOINT: EN_SS_MAJOR_MERGE : LSID | TabletID", K(ret), K(get_ls_id()), K(get_tablet_id()));
  } else if (OB_FAIL(link_major_if_exist_(*this))) {
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), epoch))) {
    LOG_WARN("major sswriter is not leader", K(ret), K(epoch));
  } else if (FALSE_IT(sswriter_epoch_ = epoch)) {
  } else if (OB_FAIL(ObTabletMergeCtx::init_tablet_merge_info())) {
    LOG_WARN("failed to init tablet merge info", KR(ret), KPC(this));
  } else if (FALSE_IT(task_cnt_ = get_concurrent_cnt())) {
  } else if (OB_FAIL(major_pre_warm_param_.init(get_ls()->get_ls_id(), get_tablet_id()))) {
    LOG_WARN("failed to init pre warm param", KR(ret), KPC(this));
  } else {
    // task_cnt_ 个数据任务序号段，再加 1 个索引树序号段。
    const int64_t seq_step_cnt = task_cnt_ + INDEX_TREE_MACRO_SEQ_SLOT_CNT;
    if (is_inc_major_compaction
        && OB_FAIL(SSCompactHelper::start_inc_major_compaction_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            0/*列组数*/, static_desc_.op_id_, sstable_protocol_))) {
      LOG_WARN("start inc major compaction task fail", K(ret));
    } else if (!is_inc_major_compaction
        && OB_FAIL(SSCompactHelper::start_major_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            0/*列组数*/, sstable_protocol_))) {
      LOG_WARN("start major task fail", K(ret));
    } else {
      LOG_INFO("init merge info succ");
    }
  }
  if (OB_NOT_MASTER == ret) {
    ret = OB_NO_NEED_MERGE;
  }
  return ret;
}

int ObSSTabletMajorMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  // create sstable
  const ObSSTable *sstable = NULL;
  ObUpdateTableStoreParam param;
  int64_t epoch = 0;
  time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
  if (OB_FAIL(pre_warm_writer_.complete())) {
    LOG_WARN("failed to complete pre warm writer", KR(ret), K_(pre_warm_writer));
  } else if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else if (OB_FAIL(build_update_table_store_param(sstable, param))) {
    LOG_WARN("build table store update param fail", K(ret));
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), epoch))) {
    LOG_WARN("major sswriter lease expired, give up", K(ret), K(epoch));
  } else if (epoch != sswriter_epoch_) {
    LOG_WARN("sswriter ABA switched off and then switched in", "curr_epoch", epoch,
             "old_epoch", sswriter_epoch_);
  }
  // param.ha_info_.need_check_transfer_seq_ = false;
  if (is_inc_major_merge(get_merge_type())) {
    if (FAILEDx(SSCompactHelper::finish_inc_major_compaction_task(sstable_protocol_))) {
      LOG_WARN("finish inc major compaction task before update fail", K(ret));
    }
  } else if (FAILEDx(SSCompactHelper::finish_major_task(get_ls_id(),
                                                        get_tablet_id(),
                                                        get_tablet()->get_reorganization_scn(),
                                                        get_merge_version(),
                                                        sstable_protocol_))) {
    LOG_WARN("finish major task before update fail", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
      LOG_WARN("get ctrl key info fail", K(ret));
    } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
      LOG_WARN("push back ctrl key fail", K(ret));
    } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(get_ls_id(),
                                                                          get_tablet_id(),
                                                                          get_tablet()->get_reorganization_scn(),
                                                                          ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                                                                          param,
                                                                          meta_ctrl_keys,
                                                                          true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail", K(ret));
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
    }
  }

  mem_ctx_.mem_click();
  (void) update_and_analyze_progress();
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(collect_running_info())) {
    LOG_WARN("fail to collect running info", K(tmp_ret));
  }
  LOG_INFO("update tablet after merge", K(ret));

  if (OB_SUCC(ret) && OB_TMP_FAIL(SSCompactHelper::link_major(get_ls_id(),
                                                get_tablet_id(),
                                                get_inner_table_merge_type(),
                                                get_merge_version(),
                                                get_merge_base_version(),
                                                true,
                                                true))) {
    LOG_WARN("link major to local fail", K(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_SUCCESS_COUNT);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_TOTAL_MACRO_BLOCK_COUNT, merge_info_.get_merge_history().block_info_.macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_REUSE_MACRO_BLOCK_COUNT, merge_info_.get_merge_history().block_info_.multiplexed_macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_EXECUTE_TIME, merge_info_.get_merge_history().running_info_.execute_time_);
  } else {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_FAILURE_COUNT);
  }
  return ret;
}

int ObSSCOTabletMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  const ObSSTable *sstable = nullptr;
  ObUpdateTableStoreParam param;
  int64_t curr_epoch = 0;
  time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
  if (OB_FAIL(pre_warm_writer_.complete())) {
    LOG_WARN("failed to complete pre warm writer", KR(ret), K_(pre_warm_writer));
  } else if (OB_FAIL(ObCOTabletMergeCtx::create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret));
  } else if (OB_FAIL(build_update_table_store_param(sstable, param))) {
    LOG_WARN("build table store update param fail", K(ret));
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), curr_epoch))) {
    LOG_WARN("major sswriter switch off", K(ret));
  } else if (curr_epoch != sswriter_epoch_) {
    LOG_WARN("major sswriter switch forth and back", K(curr_epoch), K(sswriter_epoch_));
  }

  if (update_tablet_failed_times_ > 0) {
    param.allow_duplicate_sstable_ = true;
    FLOG_INFO("allow duplicate co major sstable when retry", K(ret), KPC(this));
  }
  if (is_inc_major_merge(get_merge_type())) {
    if (FAILEDx(SSCompactHelper::finish_inc_major_compaction_task(sstable_protocol_))) {
      LOG_WARN("finish inc major compaction task before update fail", K(ret));
    }
  } else if (FAILEDx(SSCompactHelper::finish_major_task(get_ls_id(),
                                                        get_tablet_id(),
                                                        get_tablet()->get_reorganization_scn(),
                                                        get_merge_version(),
                                                        sstable_protocol_))) {
    LOG_WARN("finish major task before update fail", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
      LOG_WARN("get ctrl key info fail", K(ret));
    } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
      LOG_WARN("push back ctrl key fail", K(ret));
    } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(get_ls_id(),
                                                                          get_tablet_id(),
                                                                          get_tablet()->get_reorganization_scn(),
                                                                          ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                                                                          param,
                                                                          meta_ctrl_keys,
                                                                          true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail", K(ret));
      update_tablet_failed_times_++;
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
    }
  }

  mem_ctx_.mem_click();
  (void) update_and_analyze_progress();
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(collect_running_info())) {
    LOG_WARN("fail to collect running info", K(tmp_ret));
  }
  LOG_INFO("update tablet after merge", K(ret));
  if (OB_SUCC(ret) && OB_TMP_FAIL(SSCompactHelper::link_major(get_ls_id(),
                                          get_tablet_id(),
                                          get_inner_table_merge_type(),
                                          get_merge_version(),
                                          get_merge_base_version(),
                                          true,
                                          true))) {
    LOG_WARN("link major to local fail", K(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_SUCCESS_COUNT);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_TOTAL_MACRO_BLOCK_COUNT, dag_net_merge_history_.block_info_.macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_REUSE_MACRO_BLOCK_COUNT, dag_net_merge_history_.block_info_.multiplexed_macro_block_count_);
    EVENT_ADD(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_EXECUTE_TIME, dag_net_merge_history_.running_info_.execute_time_);
  } else {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_FAILURE_COUNT);
  }
  return ret;
}

int ObSSCOTabletMergeCtx::get_index_tree_macro_start_seq(int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_cnt_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro sequence task count", K(ret), K(task_cnt_));
  } else {
    macro_start_seq = task_cnt_ * MACRO_STEP_SIZE;
    if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("macro start seq is size overflow", K(ret), K(task_cnt_));
    }
  }
  return ret;
}

int ObSSCOTabletMergeCtx::generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  macro_start_seq = task_idx * MACRO_STEP_SIZE;
  if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro start seq is size overflow", K(ret), K(task_idx), K(task_cnt_));
  }
  return ret;
}

ObStorageObjectWriteStrategy ObSSCOTabletMergeCtx::get_io_write_strategy() const
{
  if (ObSSTabletMergeHelper::is_hot_tablet(get_tablet_id())
      && !is_inc_major_merge(get_merge_type())) {
    return ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  } else {
    return ObStorageObjectWriteStrategy::WRITE_THROUGH;
  }
}

int ObSSCOTabletMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  const int64_t cg_count = get_schema()->get_column_group_count();
  int64_t epoch = 0;
  const bool is_inc_major_compaction = is_inc_major_merge(get_merge_type());
  start_schedule_cg_idx_ = 0;
  if (OB_FAIL(link_major_if_exist_(*this))) {
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), epoch))) {
    LOG_WARN("major sswriter is not leader", K(ret), K(epoch));
  } else if (FALSE_IT(sswriter_epoch_ = epoch)) {
  } else if (OB_FAIL(ObCOTabletMergeCtx::init_tablet_merge_info())) {
    LOG_WARN("failed to init tablet merge info", KR(ret), KPC(this));
  } else if (FALSE_IT(task_cnt_ = get_concurrent_cnt())) {
  } else if (OB_FAIL(major_pre_warm_param_.init(get_ls()->get_ls_id(), get_tablet_id()))) {
    LOG_WARN("failed to init pre warm param", KR(ret), KPC(this));
  } else {
    // task_cnt_ 个数据任务序号段，再加 1 个索引树序号段。
    const int64_t seq_step_cnt = task_cnt_ + INDEX_TREE_MACRO_SEQ_SLOT_CNT;
    if (is_inc_major_compaction
        && OB_FAIL(SSCompactHelper::start_inc_major_compaction_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            cg_count, static_desc_.op_id_, sstable_protocol_))) {
      LOG_WARN("start inc major compaction task fail", K(ret), K(cg_count));
    } else if (!is_inc_major_compaction
        && OB_FAIL(SSCompactHelper::start_major_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            cg_count, sstable_protocol_))) {
      LOG_WARN("start major task fail", K(ret));
    } else {
      LOG_INFO("init merge info succ");
    }
  }
  if (OB_NOT_MASTER == ret) {
    ret = OB_NO_NEED_MERGE;
  }
  return ret;
}

void ObSSCOTabletMergeCtx::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inc_major_merge(get_merge_type())
      && sstable_protocol_.is_inited()
      && sstable_protocol_.is_status_start()
      && OB_TMP_FAIL(sstable_protocol_.abort())) {
    LOG_WARN_RET(tmp_ret, "abort unfinished inc major compaction protocol fail",
        K(sstable_protocol_));
  }
  ObCOTabletMergeCtx::destroy();
}

// =============================================================================
// ObSSAsyncIndexMergeCtx
// =============================================================================
void ObSSAsyncIndexMergeCtx::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if ((is_inc_major_merge(get_merge_type()) || sstable_protocol_.is_non_idem_major())
      && sstable_protocol_.is_inited()
      && sstable_protocol_.is_status_start()
      && OB_TMP_FAIL(sstable_protocol_.abort())) {
    LOG_WARN_RET(tmp_ret, "abort unfinished async index build protocol fail",
        K(sstable_protocol_));
  }
}

int ObSSAsyncIndexMergeCtx::generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  macro_start_seq = task_idx * MACRO_STEP_SIZE;
  if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro start seq is size overflow", K(ret), K(task_idx));
  }
  return ret;
}

int64_t ObSSAsyncIndexMergeCtx::get_data_macro_seq_slot_count() const
{
  // IVF reserves one GC-aligned sequence slot per output writer, independently of merge concurrency.
  return is_ivf_index_merge()
      ? blocksstable::ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT : task_cnt_;
}

int ObSSAsyncIndexMergeCtx::get_index_tree_macro_start_seq(int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_cnt_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro sequence task count", K(ret), K(task_cnt_));
  } else {
    macro_start_seq = get_data_macro_seq_slot_count() * MACRO_STEP_SIZE;
    if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("macro start seq is size overflow", K(ret), K(task_cnt_));
    }
  }
  return ret;
}

ObStorageObjectWriteStrategy ObSSAsyncIndexMergeCtx::get_io_write_strategy() const
{
  if (ObSSTabletMergeHelper::is_hot_tablet(get_tablet_id())) {
    if (is_inc_major_merge(get_merge_type())) {
      // 根据 SHARED_INC_MAJOR_COMPACTION_DATA_MACRO 的定义，
      // inc-major compaction 不支持 WRITE_THROUGH_AND_TRY_WRITE_LCACHE。
      // 所以这里对于 inc major sstable, 使用 WRITE_THROUGH 策略
      return ObStorageObjectWriteStrategy::WRITE_THROUGH;
    } else {
      return ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
    }
  } else {
    return ObStorageObjectWriteStrategy::WRITE_THROUGH;
  }
}

int ObSSAsyncIndexMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  int64_t epoch = 0;
  const bool is_inc_major_compaction = is_inc_major_merge(get_merge_type());
  const bool is_non_idem_major =
      is_ivf_index_merge() && is_major_merge_type(get_merge_type());
  if (OB_FAIL(link_major_if_exist_(*this))) {
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), epoch))) {
    LOG_WARN("async index sswriter is not leader", K(ret), K(epoch));
  } else if (FALSE_IT(sswriter_epoch_ = epoch)) {
  } else if (OB_FAIL(ObAsyncIndexMergeCtx::init_tablet_merge_info())) {
    LOG_WARN("failed to init tablet merge info", KR(ret), KPC(this));
  } else if (FALSE_IT(task_cnt_ = get_concurrent_cnt())) {
  } else if (OB_FAIL(major_pre_warm_param_.init(get_ls()->get_ls_id(), get_tablet_id()))) {
    LOG_WARN("failed to init pre warm param", KR(ret), KPC(this));
  } else {
    static_desc_.is_non_idem_major_ = is_non_idem_major;
    // 数据 writer 个数对应的序号段，再加 1 个索引树序号段。
    const int64_t seq_step_cnt = get_data_macro_seq_slot_count() + INDEX_TREE_MACRO_SEQ_SLOT_CNT;
    // Each non-idempotent Major build gets its own op_id and dedicated object types.
    if (is_inc_major_compaction
        && OB_FAIL(SSCompactHelper::start_inc_major_compaction_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            0/*列组数*/, static_desc_.op_id_, sstable_protocol_))) {
      LOG_WARN("start inc major compaction task fail", K(ret));
    } else if (is_non_idem_major
        && OB_FAIL(SSCompactHelper::start_non_idem_major_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            0/*列组数*/, static_desc_.op_id_, sstable_protocol_))) {
      LOG_WARN("start non-idempotent build task fail", K(ret));
    } else if (!is_inc_major_compaction && !is_non_idem_major
        && OB_FAIL(SSCompactHelper::start_major_task(
            get_ls_id(), get_tablet_id(), get_tablet()->get_reorganization_scn(),
            get_merge_version(), seq_step_cnt,
            0/*列组数*/, sstable_protocol_))) {
      LOG_WARN("start major task fail", K(ret));
    } else {
      LOG_INFO("async index init merge info succ");
    }
  }
  if (OB_NOT_MASTER == ret) {
    ret = OB_NO_NEED_MERGE;
  }
  return ret;
}

int ObSSAsyncIndexMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  const ObSSTable *sstable = nullptr;
  ObUpdateTableStoreParam param;
  int64_t epoch = 0;
  time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
  if (OB_FAIL(pre_warm_writer_.complete())) {
    LOG_WARN("failed to complete pre warm writer", KR(ret), K_(pre_warm_writer));
  } else if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else if (OB_ISNULL(sstable)) {
    LOG_INFO("async index merge finished without produced sstable, keep tablet unchanged", K(get_dag_param()));
  } else if (OB_FAIL(build_update_table_store_param(sstable, param))) {
    LOG_WARN("build table store update param fail", K(ret));
  } else if (OB_FAIL(SSCompactHelper::check_is_sswriter(get_ls_id(), get_tablet_id(), epoch))) {
    LOG_WARN("async index sswriter lease expired, give up", K(ret), K(epoch));
  } else if (epoch != sswriter_epoch_) {
    LOG_WARN("sswriter ABA switched off and then switched in", "curr_epoch", epoch,
             "old_epoch", sswriter_epoch_);
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(sstable)) {
    if (is_inc_major_merge(get_merge_type())) {
      if (OB_FAIL(SSCompactHelper::finish_inc_major_compaction_task(sstable_protocol_))) {
        LOG_WARN("finish inc major compaction task fail", K(ret));
      }
    } else if (sstable_protocol_.is_non_idem_major()) {
      if (OB_FAIL(SSCompactHelper::finish_non_idem_major_task(sstable_protocol_))) {
        LOG_WARN("finish non-idempotent build task fail", K(ret));
      }
    } else if (OB_FAIL(SSCompactHelper::finish_major_task(get_ls_id(),
                                                          get_tablet_id(),
                                                          get_tablet()->get_reorganization_scn(),
                                                          get_merge_version(),
                                                          sstable_protocol_))) {
      LOG_WARN("finish major task fail", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(sstable)) {
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
      LOG_WARN("get ctrl key info fail", K(ret));
    } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
      LOG_WARN("push back ctrl key fail", K(ret));
    } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(get_ls_id(),
                                                                          get_tablet_id(),
                                                                          get_tablet()->get_reorganization_scn(),
                                                                          ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                                                                          param,
                                                                          meta_ctrl_keys,
                                                                          true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail", K(ret));
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
    }
  }

  mem_ctx_.mem_click();
  (void) update_and_analyze_progress();
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(collect_running_info())) {
    LOG_WARN("fail to collect running info", K(tmp_ret));
  }
  LOG_INFO("async index update tablet after merge", K(ret));

  if (OB_SUCC(ret) && OB_NOT_NULL(sstable) && OB_TMP_FAIL(SSCompactHelper::link_major(get_ls_id(),
                                                get_tablet_id(),
                                                get_inner_table_merge_type(),
                                                get_merge_version(),
                                                get_merge_base_version(),
                                                true,
                                                true))) {
    LOG_WARN("link major to local fail", K(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_SUCCESS_COUNT);
  } else {
    EVENT_INC(ObStatEventIds::SS_SSTABLE_MAJOR_MERGE_FAILURE_COUNT);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

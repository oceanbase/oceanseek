/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_gc_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_meta_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_merge_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_diff_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSOuttableGCTask::ObSSOuttableGCTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_OUTTABLE_LOB_USER_GC),
    is_inited_(false)
{
}

int ObSSOuttableGCTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("outtable lob user gc task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSOuttableGCTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::PREPARE);
  SCN min_outtable_data_scn = SCN::max_scn();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob user gc task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else {
    ctx->min_outtable_data_scn_.set_max();
    ctx->lob_meta_scan_inputs_.reuse();
    ctx->lob_ctrl_scan_inputs_.reuse();
    if (OB_FAIL(prepare_scan_inputs_(*ctx, ctx->lob_meta_scan_inputs_,
        ctx->lob_ctrl_scan_inputs_, min_outtable_data_scn))) {
      LOG_WARN("failed to prepare lob user gc scan inputs", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
    } else {
      // 全部 Tablet 准备成功后才发布截止位点，后续扫描不能使用部分准备结果。
      ctx->min_outtable_data_scn_ = min_outtable_data_scn;
      if (ctx->lob_ctrl_scan_inputs_.empty() || min_outtable_data_scn.is_min()) {
        LOG_INFO("skip outtable lob user gc without lob ctrl scan input or transaction boundary",
            K(ctx->tenant_id_), K(ctx->round_id_), K(min_outtable_data_scn),
            "lob_meta_input_count", ctx->lob_meta_scan_inputs_.count(),
            "lob_ctrl_input_count", ctx->lob_ctrl_scan_inputs_.count());
      } else if (!ctx->lob_meta_scan_inputs_.empty()
          && OB_FAIL(ctx->lob_meta_pipeline_.init(ctx->tenant_id_, ctx->memory_limit_,
          ctx->file_buf_size_, 0 /*expire_timestamp*/, &ctx->compare_))) {
        LOG_WARN("failed to init lob meta scan pipeline", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
      } else if (OB_FAIL(ctx->lob_ctrl_scan_file_mgr_.init(ctx->tenant_id_))) {
        LOG_WARN("failed to init lob ctrl scan file manager", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
      } else {
        // 普通 DAG 不会自动调用此后置生成接口；输入冻结后显式生成首个扫描批次。
        // 框架复制归并后继，确保引用归并等待所有已生成的扫描批次。
        if (OB_FAIL(ObITask::post_generate_next_task())) {
          LOG_WARN("failed to add first lob user gc scan task", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
        } else {
          LOG_INFO("finish preparing outtable lob user gc scan inputs", K(ctx->tenant_id_),
              K(ctx->round_id_), K(ctx->snapshot_meta_scn_), K(min_outtable_data_scn),
              "lob_meta_input_count", ctx->lob_meta_scan_inputs_.count(),
              "lob_ctrl_input_count", ctx->lob_ctrl_scan_inputs_.count());
        }
      }
    }
    if (OB_FAIL(ret)) {
      ctx->lob_meta_scan_inputs_.reuse();
      ctx->lob_ctrl_scan_inputs_.reuse();
      ctx->min_outtable_data_scn_.set_max();
    }
  }
  return ret;
}

int ObSSOuttableGCTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMarkSweepDag *dag = nullptr;
  ObITask *scan_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *merge_complete_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob user gc task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (ctx->lob_ctrl_scan_inputs_.empty() || ctx->min_outtable_data_scn_.is_min()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else {
    if (ctx->lob_meta_scan_inputs_.empty()) {
      if (OB_FAIL(create_lob_ctrl_tasks_(*dag, *ctx, *this, scan_task))) {
        LOG_WARN("failed to create lob ctrl gc tasks without lob meta inputs", KR(ret));
      }
    } else if (OB_FAIL(create_lob_meta_tasks_(*dag, *ctx, scan_task, merge_complete_task))) {
      LOG_WARN("failed to create lob meta gc tasks", KR(ret));
    } else {
      ObITask *ctrl_scan_task = nullptr;
      if (OB_FAIL(create_lob_ctrl_tasks_(*dag, *ctx, *merge_complete_task, ctrl_scan_task))) {
        LOG_WARN("failed to create lob ctrl gc tasks", KR(ret));
      } else if (OB_FAIL(merge_complete_task->add_child(*ctrl_scan_task))) {
        LOG_WARN("failed to add lob meta merge to lob ctrl scan dependency", KR(ret));
      } else if (OB_FAIL(dag->add_task(*ctrl_scan_task))) {
        LOG_WARN("failed to add first lob ctrl scan task", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      next_task = scan_task;
    }
  }
  return ret;
}

int ObSSOuttableGCTask::create_lob_meta_tasks_(
    ObSSMarkSweepDag &dag,
    ObSSMarkSweepCtx &ctx,
    ObITask *&scan_task,
    ObSSMarkSweepMergeCompleteTask *&merge_complete_task)
{
  int ret = OB_SUCCESS;
  ObSSLobMetaGCScanTask *lob_meta_scan_task = nullptr;
  ObSSMarkSweepMergeStartTask *merge_start_task = nullptr;
  scan_task = nullptr;
  merge_complete_task = nullptr;
  if (OB_FAIL(dag.alloc_task(lob_meta_scan_task))) {
    LOG_WARN("failed to allocate first lob meta scan task", KR(ret));
  } else if (OB_FAIL(lob_meta_scan_task->init(0, MIN(ctx.batch_size_, ctx.lob_meta_scan_inputs_.count())))) {
    LOG_WARN("failed to init first lob meta scan task", KR(ret));
  } else if (OB_FAIL(dag.alloc_task(merge_start_task))) {
    LOG_WARN("failed to allocate lob meta merge start task", KR(ret));
  } else if (OB_FAIL(dag.alloc_task(merge_complete_task))) {
    LOG_WARN("failed to allocate lob meta merge complete task", KR(ret));
  } else if (OB_FAIL(merge_complete_task->init(&ctx.lob_meta_pipeline_))) {
    LOG_WARN("failed to init lob meta merge complete task", KR(ret));
  } else if (OB_FAIL(merge_start_task->init(merge_complete_task, &ctx.lob_meta_pipeline_))) {
    LOG_WARN("failed to init lob meta merge start task", KR(ret));
  } else if (OB_FAIL(copy_children_to(*merge_complete_task))) {
    LOG_WARN("failed to inherit system gc dependency for lob meta merge", KR(ret));
  } else if (OB_FAIL(add_child(*merge_start_task))) {
    LOG_WARN("failed to add user gc to lob meta merge start dependency", KR(ret));
  } else if (OB_FAIL(add_child(*merge_complete_task))) {
    LOG_WARN("failed to add user gc to lob meta merge complete dependency", KR(ret));
  } else if (OB_FAIL(merge_start_task->add_child(*merge_complete_task))) {
    LOG_WARN("failed to add lob meta merge start to complete dependency", KR(ret));
  } else if (OB_FAIL(dag.add_task(*merge_start_task))) {
    LOG_WARN("failed to add lob meta merge start task", KR(ret));
  } else if (OB_FAIL(dag.add_task(*merge_complete_task))) {
    LOG_WARN("failed to add lob meta merge complete task", KR(ret));
  } else {
    scan_task = lob_meta_scan_task;
  }
  return ret;
}

int ObSSOuttableGCTask::create_lob_ctrl_tasks_(
    ObSSMarkSweepDag &dag,
    const ObSSMarkSweepCtx &ctx,
    const ObITask &prev_task,
    ObITask *&scan_task)
{
  int ret = OB_SUCCESS;
  ObSSLobCtrlGCScanTask *ctrl_scan_task = nullptr;
  ObSSLobCtrlGCMergePassTask *ctrl_merge_task = nullptr;
  ObSSLobCtrlGCDiffTask *ctrl_diff_task = nullptr;
  ObSSLobCtrlGCDeleteTask *ctrl_delete_task = nullptr;
  scan_task = nullptr;
  // Ctrl 删除继承前置任务的系统后继，首批扫描的父依赖由调用方挂接。
  // 入口直接执行 Ctrl 扫描时，由后置生成框架负责挂接，不能提前添加自身依赖。
  if (OB_FAIL(dag.alloc_task(ctrl_scan_task))) {
    LOG_WARN("failed to allocate first lob ctrl scan task", KR(ret));
  } else if (OB_FAIL(ctrl_scan_task->init(0, MIN(ctx.batch_size_, ctx.lob_ctrl_scan_inputs_.count())))) {
    LOG_WARN("failed to init first lob ctrl scan task", KR(ret));
  } else if (OB_FAIL(dag.alloc_task(ctrl_merge_task))) {
    LOG_WARN("failed to allocate lob ctrl merge task", KR(ret));
  } else if (OB_FAIL(ctrl_merge_task->init(0))) {
    LOG_WARN("failed to init lob ctrl merge task", KR(ret));
  } else if (OB_FAIL(dag.alloc_task(ctrl_diff_task))) {
    LOG_WARN("failed to allocate lob ctrl diff task", KR(ret));
  } else if (OB_FAIL(ctrl_diff_task->init())) {
    LOG_WARN("failed to init lob ctrl diff task", KR(ret));
  } else if (OB_FAIL(dag.alloc_task(ctrl_delete_task))) {
    LOG_WARN("failed to allocate lob ctrl delete task", KR(ret));
  } else if (OB_FAIL(ctrl_delete_task->init())) {
    LOG_WARN("failed to init lob ctrl delete task", KR(ret));
  } else if (OB_FAIL(prev_task.copy_children_to(*ctrl_delete_task))) {
    LOG_WARN("failed to inherit system gc dependency for lob ctrl delete", KR(ret));
  } else if (OB_FAIL(ctrl_diff_task->add_child(*ctrl_delete_task))) {
    LOG_WARN("failed to add lob ctrl diff to delete dependency", KR(ret));
  } else if (OB_FAIL(dag.add_task(*ctrl_delete_task))) {
    LOG_WARN("failed to add lob ctrl delete task", KR(ret));
  } else if (OB_FAIL(ctrl_merge_task->add_child(*ctrl_diff_task))) {
    LOG_WARN("failed to add lob ctrl merge to diff dependency", KR(ret));
  } else if (OB_FAIL(dag.add_task(*ctrl_diff_task))) {
    LOG_WARN("failed to add lob ctrl diff task", KR(ret));
  } else if (OB_FAIL(ctrl_scan_task->add_child(*ctrl_merge_task))) {
    LOG_WARN("failed to add lob ctrl scan to merge dependency", KR(ret));
  } else if (OB_FAIL(dag.add_task(*ctrl_merge_task))) {
    LOG_WARN("failed to add lob ctrl merge task", KR(ret));
  } else {
    scan_task = ctrl_scan_task;
  }
  return ret;
}

int ObSSOuttableGCTask::prepare_scan_inputs_(
    const ObSSMarkSweepCtx &ctx,
    ObSSGCTabletWithMetaScnArray &lob_meta_scan_inputs,
    ObSSGCTabletWithMetaScnArray &lob_ctrl_scan_inputs,
    SCN &min_outtable_data_scn)
{
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  lob_meta_scan_inputs.reuse();
  lob_ctrl_scan_inputs.reuse();
  min_outtable_data_scn.set_max();
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.tablet_with_meta_scn_arr_.count(); ++i) {
    const ObSSGCTabletWithMetaScn &tablet_with_meta_scn = ctx.tablet_with_meta_scn_arr_.at(i);
    if (OB_FAIL(check_if_stopped_or_failed())) {
      LOG_WARN("failed to check user gc task state", KR(ret), K(tablet_with_meta_scn));
    } else if (OB_FAIL(prepare_lob_tablet_scan_input_(ctx, tablet_with_meta_scn,
        lob_meta_scan_inputs, lob_ctrl_scan_inputs, min_outtable_data_scn))) {
      LOG_WARN("failed to prepare lob tablet scan input", KR(ret), K(tablet_with_meta_scn));
    } else if (OB_FAIL(dag_yield())) {
      LOG_WARN("failed to yield user gc task", KR(ret));
    }
  }
  if (OB_SUCC(ret) && !ctx.tablet_with_meta_scn_arr_.empty()) {
    ObTabletIDArray current_ctrl_tablets;
    if (OB_FAIL(prepare_lob_ctrl_scan_inputs_(ctx.tenant_id_, current_ctrl_tablets))) {
      LOG_WARN("failed to prepare lob ctrl scan inputs", KR(ret), K(ctx.tenant_id_));
    } else {
      // Only scan current Ctrl partitions with a shared version included in this round's Study.
      int64_t kept_count = 0;
      for (int64_t i = 0; i < lob_ctrl_scan_inputs.count(); ++i) {
        const ObTabletID &tablet_id = lob_ctrl_scan_inputs.at(i).gc_tablet_.tablet_id_;
        if (has_exist_in_array(current_ctrl_tablets, tablet_id)) {
          lob_ctrl_scan_inputs.at(kept_count++) = lob_ctrl_scan_inputs.at(i);
        }
      }
      // In-place filtering keeps the array size; remove stale or duplicate tail entries.
      // E.g. keeping A and C in [A, B, C] gives [A, C, C]; pop_back trims it to [A, C].
      while (lob_ctrl_scan_inputs.count() > kept_count) {
        lob_ctrl_scan_inputs.pop_back();
      }
    }
  }
  if (OB_SUCC(ret) && !lob_ctrl_scan_inputs.empty() && min_outtable_data_scn.is_max()) {
    // 本轮没有得到 Ctrl Tablet 的冻结事务边界，不能扫描或删除当前 Ctrl 行。
    min_outtable_data_scn.set_min();
    lob_ctrl_scan_inputs.reuse();
    LOG_INFO("skip lob ctrl scan without frozen transaction boundary",
        K(ctx.tenant_id_), K(ctx.round_id_));
  }
  if (OB_FAIL(ret)) {
    lob_meta_scan_inputs.reuse();
    lob_ctrl_scan_inputs.reuse();
    min_outtable_data_scn.set_max();
  }
  const int64_t cost_us = time_piece.fly_us();
  FLOG_INFO("finish to prepare outtable gc scan inputs", KR(ret), K(cost_us),
      K(ctx.tenant_id_), K(ctx.round_id_), "tablet_count", ctx.tablet_with_meta_scn_arr_.count(),
      "lob_meta_input_count", lob_meta_scan_inputs.count(),
      "lob_ctrl_input_count", lob_ctrl_scan_inputs.count(), K(min_outtable_data_scn));
  return ret;
}

int ObSSOuttableGCTask::prepare_lob_ctrl_scan_inputs_(
    const uint64_t tenant_id,
    ObTabletIDArray &scan_inputs)
{
  int ret = OB_SUCCESS;
  bool is_primary = false;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = nullptr;
  scan_inputs.reuse();
  if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
    LOG_WARN("failed to check tenant role for lob ctrl scan", KR(ret), K(tenant_id));
  } else if (!is_primary) {
    LOG_INFO("skip lob ctrl logical scan for non-primary tenant", K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for lob ctrl scan", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ObString("oceanbase"),
      ObString(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME), false /*is_index*/, table_schema))) {
    LOG_WARN("failed to get lob ctrl table schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(table_schema)) {
    // 旧 Schema 尚未补建控制表时不准备逻辑扫描输入，不能回退到其他表。
    LOG_INFO("skip lob ctrl scan without control table schema", K(tenant_id));
  } else if (OB_FAIL(collect_lob_ctrl_scan_inputs_(*table_schema, scan_inputs))) {
    LOG_WARN("failed to collect lob ctrl partition tablets", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSOuttableGCTask::collect_lob_ctrl_scan_inputs_(
    const schema::ObTableSchema &table_schema,
    ObTabletIDArray &scan_inputs)
{
  int ret = OB_SUCCESS;
  scan_inputs.reuse();
  if (OB_UNLIKELY(!table_schema.is_aux_outtable_lob_control_table()
      || table_schema.is_index_table()
      || schema::PARTITION_LEVEL_ONE != table_schema.get_part_level()
      || table_schema.get_partition_num() <= 0
      || table_schema.get_partition_num() != table_schema.get_part_option().get_part_num())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid lob ctrl main table partition schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_tablet_ids(scan_inputs))) {
    LOG_WARN("failed to get lob ctrl partition tablet ids", KR(ret), K(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_inputs.count(); ++i) {
    if (OB_UNLIKELY(!scan_inputs.at(i).is_valid())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("invalid lob ctrl partition tablet id", KR(ret), K(i), K(scan_inputs.at(i)));
    }
  }
  if (OB_FAIL(ret)) {
    scan_inputs.reuse();
  }
  return ret;
}

int ObSSOuttableGCTask::calc_lob_gc_checkpoint_scn_(
    const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
    const ObTablet &first_tablet,
    const ObIArray<SCN> &kept_meta_scns,
    ObArenaAllocator &allocator,
    SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  const ObSSGCTablet &gc_tablet = tablet_with_meta_scn.gc_tablet_;
  const SCN &shared_scn = first_tablet.get_clog_checkpoint_scn();
  const SCN &study_scn = tablet_with_meta_scn.clog_checkpoint_scn_;
  const SCN &orig_scn = tablet_with_meta_scn.orig_clog_checkpoint_scn_;
  checkpoint_scn.set_min();
  if (OB_UNLIKELY(kept_meta_scns.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no kept meta version for lob checkpoint", KR(ret), K(gc_tablet));
  } else if (OB_UNLIKELY(!shared_scn.is_valid() || shared_scn.is_max()
      || !study_scn.is_valid() || !orig_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob checkpoint scn", KR(ret), K(tablet_with_meta_scn), K(shared_scn));
  } else if (study_scn.is_min() || !first_tablet.is_data_complete()
      || first_tablet.get_tablet_meta().split_info_.is_data_incomplete()) {
    // An incomplete scan baseline must block user GC even when the current Study is complete.
  } else {
    checkpoint_scn = MIN3(shared_scn, orig_scn, study_scn);
    // If study_scn > orig_scn, the replica supplying orig_scn must have used
    // cold promotion. Its LS checkpoint was read before its tablet, so there
    // is no new row data in (orig_scn, study_scn). With asymmetric replica
    // checkpoints, the shared scan baseline can lie inside this interval.
    if (tablet_with_meta_scn.is_cold()
        && orig_scn <= shared_scn && shared_scn < study_scn) {
      ObTabletHandle latest_tablet_handle;
      const ObTablet *latest_tablet = &first_tablet;
      if (kept_meta_scns.count() > 1) {
        // Compare only versions from this round's frozen kept set.
        const SCN &latest_meta_scn = kept_meta_scns.at(kept_meta_scns.count() - 1);
        ObSSMetaService *meta_service = MTL(ObSSMetaService *);
        if (OB_ISNULL(meta_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shared meta service is null", KR(ret), K(gc_tablet));
        } else if (OB_FAIL(meta_service->get_tablet(gc_tablet.ls_id_, gc_tablet.tablet_id_,
            gc_tablet.transfer_scn_, latest_meta_scn, allocator, latest_tablet_handle))) {
          LOG_WARN("failed to load latest kept lob tablet", KR(ret), K(gc_tablet),
              K(latest_meta_scn));
        } else if (OB_ISNULL(latest_tablet = latest_tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("latest kept lob tablet is null", KR(ret), K(gc_tablet), K(latest_meta_scn));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (latest_tablet->is_data_complete()
          && !latest_tablet->get_tablet_meta().split_info_.is_data_incomplete()) {
        const SCN &latest_scn = latest_tablet->get_clog_checkpoint_scn();
        if (OB_UNLIKELY(!latest_scn.is_valid() || latest_scn.is_max())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid latest kept lob checkpoint scn", KR(ret), K(latest_scn),
              K(tablet_with_meta_scn));
        } else if (latest_scn == shared_scn) {
          checkpoint_scn = study_scn;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    checkpoint_scn.set_min();
  }
  return ret;
}

int ObSSOuttableGCTask::prepare_lob_tablet_scan_input_(
    const ObSSMarkSweepCtx &ctx,
    const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
    ObSSGCTabletWithMetaScnArray &scan_inputs,
    ObSSGCTabletWithMetaScnArray &lob_ctrl_scan_inputs,
    SCN &min_outtable_data_scn)
{
  int ret = OB_SUCCESS;
  ObSSGCTabletType table_type = tablet_with_meta_scn.table_type_;
  const ObSSGCTablet &gc_tablet = tablet_with_meta_scn.gc_tablet_;
  ObSEArray<SCN, 8> all_meta_scns;
  ObSEArray<SCN, 8> del_meta_scns;
  ObSEArray<SCN, 8> kept_meta_scns;
  SCN tablet_kept_meta_scn;
  SCN mark_delete_meta_scn;
  const ObMemAttr mem_attr(ctx.tenant_id_, "SSGCLobScns");
  all_meta_scns.set_attr(mem_attr);
  del_meta_scns.set_attr(mem_attr);
  kept_meta_scns.set_attr(mem_attr);
  if (table_type.is_normal()) {
    // Study already identified a non-LOB tablet; no shared metadata needs to be read.
  } else if (OB_FAIL(get_all_meta_scns_(gc_tablet, ctx.snapshot_meta_scn_, all_meta_scns))) {
    LOG_WARN("failed to get tablet meta versions", KR(ret), K(gc_tablet), K(ctx.snapshot_meta_scn_));
  } else if (all_meta_scns.empty()) {
    // 无可读共享版本不能证明没有 LOB 引用，本轮跳过用户 GC，等待后续重新准备。
    min_outtable_data_scn.set_min();
    LOG_TRACE("no visible tablet meta version for user gc", K(gc_tablet), K(ctx.snapshot_meta_scn_));
  } else if (OB_FAIL(ctx.get_tablet_kept_meta_scn(gc_tablet, tablet_kept_meta_scn))) {
    LOG_WARN("failed to get frozen tablet kept meta scn", KR(ret), K(gc_tablet));
  } else if (OB_FAIL(ctx.get_mark_delete_meta_scn(gc_tablet, mark_delete_meta_scn))) {
    LOG_WARN("failed to get tablet mark delete meta scn", KR(ret), K(gc_tablet));
  } else {
    const bool ls_can_del_all = has_exist_in_array(ctx.recyclable_deleted_ls_ids_, gc_tablet.ls_id_);
    const ObSSGetDelKeptMetaScnParam param(gc_tablet, tablet_with_meta_scn.meta_scn_,
        tablet_kept_meta_scn, ctx.safe_meta_scn_, mark_delete_meta_scn, ctx.reserved_meta_scns_,
        all_meta_scns, ctx.ls_ss_checkpoint_scn_map_, ls_can_del_all);
    if (OB_FAIL(get_del_and_kept_meta_scns_simply_(param, del_meta_scns, kept_meta_scns))) {
      LOG_WARN("failed to classify tablet meta versions", KR(ret), K(gc_tablet));
    } else if (kept_meta_scns.empty() && !mark_delete_meta_scn.is_valid()) {
      min_outtable_data_scn.set_min();
      LOG_ERROR("active tablet has no kept meta version, skip user gc",
          K(gc_tablet), K(all_meta_scns), K(del_meta_scns));
    } else if (!kept_meta_scns.empty()) {
      // 版本划分结果按升序排列，每个 Tablet 只选择最早保留版本作为扫描基线。
      const SCN &meta_scn = kept_meta_scns.at(0);
      ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "SSGCLobTablet"));
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      ObSSMetaService *meta_service = MTL(ObSSMetaService *);
      if (OB_ISNULL(meta_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared meta service is null", KR(ret), K(gc_tablet));
      } else if (OB_FAIL(meta_service->get_tablet(gc_tablet.ls_id_, gc_tablet.tablet_id_,
          gc_tablet.transfer_scn_, meta_scn, allocator, tablet_handle))) {
        LOG_WARN("failed to get selected tablet meta version", KR(ret), K(gc_tablet), K(meta_scn));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("selected tablet handle is null", KR(ret), K(gc_tablet), K(meta_scn));
      } else if (tablet->is_empty_shell()) {
        // 空壳 Tablet 没有存储 Schema 和可扫描的 LOB 行。
      } else if (OB_FAIL(get_table_type_(*tablet, table_type))) {
        LOG_WARN("failed to get selected tablet table type", KR(ret), K(gc_tablet), K(meta_scn));
      } else if (table_type.is_lob_meta() || table_type.is_lob_ctrl()) {
        SCN clog_checkpoint_scn;
        if (OB_FAIL(calc_lob_gc_checkpoint_scn_(
            tablet_with_meta_scn, *tablet, kept_meta_scns, allocator, clog_checkpoint_scn))) {
          LOG_WARN("failed to calculate lob checkpoint scn", KR(ret), K(gc_tablet), K(meta_scn));
        } else if (table_type.is_lob_meta()
            && OB_FAIL(scan_inputs.push_back(
                ObSSGCTabletWithMetaScn(gc_tablet, meta_scn, table_type)))) {
          LOG_WARN("failed to append lob meta scan input", KR(ret), K(gc_tablet), K(meta_scn));
        } else if (table_type.is_lob_ctrl() && OB_FAIL(lob_ctrl_scan_inputs.push_back(
            ObSSGCTabletWithMetaScn(gc_tablet, meta_scn, table_type)))) {
          LOG_WARN("failed to append lob ctrl scan input", KR(ret), K(gc_tablet), K(meta_scn));
        } else {
          min_outtable_data_scn = SCN::min(min_outtable_data_scn, clog_checkpoint_scn);
          LOG_TRACE("prepared lob tablet checkpoint scn", K(tablet_with_meta_scn), K(meta_scn),
              K(clog_checkpoint_scn));
          if (clog_checkpoint_scn.is_min()) {
            LOG_INFO("lob tablet cannot provide a safe gc checkpoint", K(tablet_with_meta_scn),
                K(meta_scn));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSOuttableGCTask::get_table_type_(
    const ObTablet &tablet,
    ObSSGCTabletType &table_type)
{
  int ret = OB_SUCCESS;
  if (table_type.is_unknown()) {
    // Absent tablets and older Study responses still need the frozen tablet's type.
    schema::ObTableType tablet_table_type = schema::MAX_TABLE_TYPE;
    if (OB_FAIL(tablet.get_table_type(tablet_table_type))) {
      LOG_WARN("failed to get selected tablet type", KR(ret), K(tablet.get_tablet_meta()));
    } else if (schema::is_aux_lob_meta_table(tablet_table_type)) {
      table_type = ObSSGCTabletType::LOB_META;
    } else if (schema::is_aux_outtable_lob_control_table(tablet_table_type)) {
      table_type = ObSSGCTabletType::LOB_CTRL;
    } else {
      table_type = ObSSGCTabletType::NORMAL;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

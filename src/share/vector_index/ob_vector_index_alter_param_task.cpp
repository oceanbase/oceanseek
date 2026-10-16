/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER
#include "share/vector_index/ob_vector_index_alter_param_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_ddl_common.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_debug_sync.h"
#include "observer/ob_server.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

int ObVecAlterIndexParamTaskExecutor::load_triggered_task(
    const ObVecIndexTaskStatus &task_row)
{
  UNUSED(task_row);
  return OB_NOT_SUPPORTED;
}

bool ObVecAlterIndexParamTaskExecutor::check_operation_allow()
{
  uint64_t data_version = 0;
  int ret = GET_MIN_DATA_VERSION(tenant_id_, data_version);
  return OB_SUCCESS == ret && data_version >= DATA_VERSION_4_6_3_0;
}

int ObVecAlterIndexParamTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  UNUSED(task_trace_base_num);
  return OB_SUCCESS;
}

int ObVecAlterIndexParamTaskExecutor::load_task(
    uint64_t &task_trace_base_num,
    const common::ObIArray<ObVecAlterParamParentInfo> *parents)
{
  int ret = OB_SUCCESS;
  UNUSED(task_trace_base_num);
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx *> new_ctxs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("alter param executor is not initialized", KR(ret));
  } else if (OB_ISNULL(parents) || parents->empty() || !check_operation_allow()) {
  } else if (OB_ISNULL(get_ls()) || !get_ls()->get_ls_id().is_sys_ls()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter index param task must run on sys ls", KR(ret), K_(ls_handle));
  } else if (OB_FAIL(get_index_ls_mgr(mgr))) {
    LOG_WARN("fail to get sys ls vector manager", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls vector manager is null", KR(ret));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    for (int64_t i = 0; OB_SUCC(ret) && i < parents->count(); ++i) {
      const ObVecAlterParamParentInfo &parent = parents->at(i);
      ObTabletID task_tablet = parent.task_info_.convergence_inc_tablet_id_;
      bool inserted = false;
      bool in_map = false;
      bool exist = false;
      ObVecIndexAsyncTaskCtx *ctx = nullptr;
      const int64_t task_id = parent.task_id_;
      if (!task_tablet.is_valid() || parent.trace_id_.is_invalid()
          || !parent.task_info_.is_parent_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid alter param parent", KR(ret), K(parent));
      } else if (OB_FAIL(task_opt.is_task_ctx_exist(
                     task_tablet, OB_VECTOR_ASYNC_ALTER_INDEX_PARAM, exist))) {
        LOG_WARN("fail to check alter index param task ctx", KR(ret), K(task_tablet));
      } else if (exist) {
        continue;
      } else if (OB_ISNULL(ctx = OB_NEWx(ObVecIndexAsyncTaskCtx, allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate alter index param task ctx", KR(ret));
      } else {
        ctx->tenant_id_ = tenant_id_;
        ctx->ls_handle_ = ls_handle_;
        ctx->allocator_.set_tenant_id(tenant_id_);
        ctx->task_status_.tenant_id_ = tenant_id_;
        ctx->task_status_.table_id_ = parent.table_id_;
        ctx->task_status_.tablet_id_ = task_tablet;
        ctx->task_status_.task_id_ = task_id;
        ctx->task_status_.task_type_ = OB_VECTOR_ASYNC_ALTER_INDEX_PARAM;
        ctx->task_status_.trigger_type_ = OB_VEC_TRIGGER_ALTER_INDEX_PARAM;
        ctx->task_status_.status_ = OB_VECTOR_ASYNC_TASK_PREPARE;
        ctx->task_status_.exec_addr_ = GCTX.self_addr();
        ctx->task_status_.trace_id_ = parent.trace_id_;
        ctx->task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time());
        ctx->task_status_.task_info_.alter_param_info_ = parent.task_info_;
        ctx->task_status_.task_info_.alter_param_info_.parent_task_id_ = parent.task_id_;
        if (OB_FAIL(task_opt.add_task_ctx(task_tablet, ctx, inserted))) {
          LOG_WARN("fail to add alter index param task ctx", KR(ret), K(parent));
        } else if (FALSE_IT(in_map = inserted)) {
        } else if (inserted && OB_FAIL(new_ctxs.push_back(ctx))) {
          LOG_WARN("fail to collect alter index param task ctx", KR(ret));
        }
      }
      if (OB_NOT_NULL(ctx) && (OB_FAIL(ret) || !inserted)) {
        if (in_map) {
          int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(task_opt, ctx);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to clear alter index param task ctx", KR(tmp_ret));
          }
        } else {
          ctx->~ObVecIndexAsyncTaskCtx();
          allocator->free(ctx);
        }
      }
    }
    // The tablet_id=-1 Parent is the only durable Type 12 task. The real-tablet
    // context is rebuilt from it in memory after restart or SYS LS leader switch.
    if (OB_FAIL(ret) && !new_ctxs.empty()) {
      int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctxs(task_opt, new_ctxs);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to clear alter index param task ctxs", KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObVecAlterIndexParamTaskExecutor::do_check_task_result(
    ObVecIndexAsyncTaskCtx *task_ctx,
    ObVecTaskResultCheckAction &action)
{
  int ret = do_check_normal_running_task_result(task_ctx, action);
  if (OB_SUCC(ret) && action.task_result_checked_
      && task_ctx->task_status_.ret_code_ == OB_SUCCESS
      && !task_ctx->task_status_.all_finished_) {
    task_ctx->task_status_.status_ = OB_VECTOR_ASYNC_TASK_PREPARE;
    task_ctx->task_status_.last_error_code_ = task_ctx->task_status_.ret_code_;
    task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
    action.need_sync_running_to_prepare_ = true;
    LOG_INFO("alter index param task not finished, RUNNING->PREPARE",
             K(task_ctx->task_status_.task_id_), K(task_ctx->task_status_.tablet_id_));
  }
  return ret;
}

int ObVecAlterIndexParamTask::check_parent_(bool &valid)
{
  int ret = OB_SUCCESS;
  valid = false;
  ObArenaAllocator allocator("VecAlterPrt", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObVecIndexTaskStatusArray rows;
  const ObVecIndexAlterParamTaskInfo &task_info =
      ctx_->task_status_.task_info_.alter_param_info_;
  if (OB_ISNULL(GCTX.sql_proxy_) || !task_info.is_child_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter index param task info", KR(ret), K(task_info));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_alter_index_param_parent_task(
          tenant_id_, task_info.parent_task_id_, false, *GCTX.sql_proxy_, rows, allocator))) {
    LOG_WARN("fail to read alter param parent", KR(ret), K(task_info));
  } else if (1 == rows.count()) {
    const ObVecIndexTaskStatus &parent = rows.at(0);
    valid = parent.table_id_ == ctx_->task_status_.table_id_
        && parent.task_id_ == task_info.parent_task_id_
        && parent.status_ == OB_VECTOR_ASYNC_TASK_RUNNING
        && ObVecIndexAsyncTaskUtil::is_alter_index_param_parent_matched(parent, task_info);
  }
  return ret;
}

int ObVecAlterIndexParamTask::load_tablet_progresses_(
    const ObVectorIndexParam &target_param,
    common::ObIArray<AlterParamTabletProgress> &progresses,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *inc_schema = nullptr;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *vbitmap_schema = nullptr;
  uint64_t inc_table_id = OB_INVALID_ID;
  uint64_t vbitmap_table_id = OB_INVALID_ID;
  uint64_t snapshot_table_id = OB_INVALID_ID;
  uint64_t embedded_table_id = OB_INVALID_ID;
  ObArray<ObTabletID> inc_tablets;
  ObArray<ObTabletID> vbitmap_tablets;
  ObArray<ObLSID> vbitmap_ls_ids;
  ObArray<ObAddr> tablet_leader_addrs;
  hash::ObHashMap<ObLSID, ObAddr> ls_leader_map;
  hash::ObHashMap<ObTabletID, int64_t> progress_idx_map;
  progresses.reset();
  schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, ctx_->task_status_.table_id_, inc_schema))) {
    LOG_WARN("fail to get incremental index schema", KR(ret), KPC(ctx_));
  } else if (OB_ISNULL(inc_schema) || inc_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("incremental index schema does not exist", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, inc_schema->get_data_table_id(), data_schema))) {
    LOG_WARN("fail to get vector data table schema", KR(ret), KPC(inc_schema));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("vector data table schema does not exist", KR(ret), KPC(inc_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::get_index_tids_for_hnsw_by_prefix(
          &schema_guard, *data_schema, inc_schema->get_table_id(), inc_table_id,
          vbitmap_table_id, snapshot_table_id, embedded_table_id, false))) {
    LOG_WARN("fail to resolve hnsw auxiliary tables", KR(ret), KPC(inc_schema));
  } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, vbitmap_table_id, vbitmap_schema))) {
    LOG_WARN("fail to get vbitmap schema", KR(ret), K(vbitmap_table_id));
  } else if (OB_ISNULL(vbitmap_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("vbitmap schema does not exist", KR(ret), K(vbitmap_table_id));
  } else if (OB_FAIL(inc_schema->get_tablet_ids(inc_tablets))
             || OB_FAIL(vbitmap_schema->get_tablet_ids(vbitmap_tablets))) {
    LOG_WARN("fail to get alter param tablets", KR(ret));
  } else if (inc_tablets.empty() || inc_tablets.count() != vbitmap_tablets.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter param tablet mapping is invalid", KR(ret), K(inc_tablets), K(vbitmap_tablets));
  } else {
    schema_version = inc_schema->get_schema_version();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_tablets.count(); ++i) {
      AlterParamTabletProgress progress;
      progress.inc_table_id_ = inc_table_id;
      progress.inc_tablet_id_ = inc_tablets.at(i);
      progress.vbitmap_table_id_ = vbitmap_table_id;
      progress.vbitmap_tablet_id_ = vbitmap_tablets.at(i);
      if (OB_FAIL(progresses.push_back(progress))) {
        LOG_WARN("fail to collect alter param tablet state", KR(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret) && (OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.location_service_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or location service is null", KR(ret),
             KP(GCTX.sql_proxy_), KP(GCTX.location_service_));
  } else if (OB_SUCC(ret) && OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
          *GCTX.sql_proxy_, tenant_id_, vbitmap_tablets, vbitmap_ls_ids))) {
    LOG_WARN("fail to batch get ls for vbitmap tablets", KR(ret), K_(tenant_id),
             K(vbitmap_tablets));
  } else if (OB_SUCC(ret) && OB_UNLIKELY(
          vbitmap_ls_ids.count() != progresses.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vbitmap tablet and ls count mismatch", KR(ret),
             K(vbitmap_tablets.count()), K(vbitmap_ls_ids.count()));
  } else if (OB_SUCC(ret) && OB_FAIL(ls_leader_map.create(
          progresses.count(), "VecAltLsMap", "VecAltLsMap", tenant_id_))) {
    LOG_WARN("fail to create ls leader map", KR(ret), K(progresses.count()));
  } else if (OB_SUCC(ret) && OB_FAIL(progress_idx_map.create(
          progresses.count(), "VecAltIdxMap", "VecAltIdxMap", tenant_id_))) {
    LOG_WARN("fail to create tablet progress index map", KR(ret), K(progresses.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < progresses.count(); ++i) {
    const ObLSID &ls_id = vbitmap_ls_ids.at(i);
    ObAddr leader_addr;
    const int map_ret = ls_leader_map.get_refactored(ls_id, leader_addr);
    if (OB_SUCCESS == map_ret) {
    } else if (OB_HASH_NOT_EXIST != map_ret) {
      ret = map_ret;
      LOG_WARN("fail to get cached ls leader", KR(ret), K(ls_id));
    } else if (OB_FAIL(GCTX.location_service_->get_leader(
            GCONF.cluster_id, tenant_id_, ls_id, false /* force_renew */, leader_addr))) {
      LOG_WARN("fail to get ls leader for vbitmap tablet", KR(ret), K(ls_id),
               "vbitmap_tablet_id", progresses.at(i).vbitmap_tablet_id_);
    } else if (OB_FAIL(ls_leader_map.set_refactored(ls_id, leader_addr))) {
      LOG_WARN("fail to cache ls leader", KR(ret), K(ls_id), K(leader_addr));
    }
    if (OB_SUCC(ret) && OB_FAIL(tablet_leader_addrs.push_back(leader_addr))) {
      LOG_WARN("fail to collect tablet leader", KR(ret), K(i), K(ls_id), K(leader_addr));
    } else if (OB_SUCC(ret) && OB_FAIL(progress_idx_map.set_refactored(
            progresses.at(i).vbitmap_tablet_id_, i))) {
      LOG_WARN("fail to build tablet progress index map", KR(ret), K(i),
               "vbitmap_tablet_id", progresses.at(i).vbitmap_tablet_id_);
    }
  }

  ObSqlString sql;
  if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt(
      "SELECT S.SVR_IP, S.SVR_PORT, S.VBITMAP_TABLET_ID, "
      "S.SEGMENT_STATE, S.SEGMENT_PARAMS "
      "FROM oceanbase.GV$OB_HNSW_INDEX_SEGMENT_INFO S "
      "WHERE S.TENANT_ID = %lu AND S.DATA_TABLE_ID = %lu "
      "AND S.VBITMAP_TABLET_ID IN (", tenant_id_, data_schema->get_table_id()))) {
    LOG_WARN("fail to build alter param segment query", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < progresses.count(); ++i) {
    if (OB_FAIL(sql.append_fmt("%s%lu", 0 == i ? "" : ",",
            progresses.at(i).vbitmap_tablet_id_.id()))) {
      LOG_WARN("fail to append vbitmap tablet", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
    LOG_WARN("fail to finish alter param segment query", KR(ret));
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to query alter param segment states", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter param segment result is null", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ObString svr_ip;
            int32_t svr_port = 0;
            uint64_t tablet_id = OB_INVALID_ID;
            ObString segment_state;
            ObString segment_params;
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "SVR_IP", svr_ip);
            EXTRACT_INT_FIELD_MYSQL(*result, "SVR_PORT", svr_port, int32_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "VBITMAP_TABLET_ID", tablet_id, uint64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "SEGMENT_STATE", segment_state);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "SEGMENT_PARAMS", segment_params);
            int64_t idx = -1;
            ObAddr segment_addr;
            if (OB_FAIL(ret)) {
            } else if (OB_UNLIKELY(!segment_addr.set_ip_addr(svr_ip, svr_port))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid server address in segment result", KR(ret),
                       K(svr_ip), K(svr_port), K(tablet_id));
            } else if (OB_FAIL(progress_idx_map.get_refactored(ObTabletID(tablet_id), idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected tablet in segment result", KR(ret), K(tablet_id),
                       K(segment_addr));
            } else if (segment_addr != tablet_leader_addrs.at(idx)) {
              continue;
            } else {
              AlterParamTabletProgress &progress = progresses.at(idx);
              if (0 == segment_state.case_compare("ACTIVE")) {
                bool equal = false;
                if (OB_FAIL(ObVectorIndexUtil::check_build_params_equal(
                        segment_params, target_param, equal))) {
                  LOG_WARN("fail to compare active segment params", KR(ret), K(tablet_id));
                } else {
                  progress.active_param_match_ = !progress.has_active_segment_
                      ? equal : progress.active_param_match_ && equal;
                  progress.has_active_segment_ = true;
                }
              } else if (0 == segment_state.case_compare("FROZEN")) {
                progress.has_frozen_segment_ = true;
              } else if (0 == segment_state.case_compare("PERSISTED")) {
                bool equal = false;
                if (OB_FAIL(ObVectorIndexUtil::check_build_params_equal(
                        segment_params, target_param, equal))) {
                  LOG_WARN("fail to compare persistent segment params", KR(ret), K(tablet_id));
                } else if (!equal) {
                  progress.has_old_persistent_segment_ = true;
                }
              } else if (0 == segment_state.case_compare("DEFAULT")) {
                // An empty adaptor publishes a DEFAULT placeholder without active data.
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected vector segment state for alter param",
                         KR(ret), K(tablet_id), K(segment_state));
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(load_child_task_infos_(progresses))) {
    LOG_WARN("fail to load alter param child task infos", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObVecAlterIndexParamTask::load_child_task_infos_(
    common::ObIArray<AlterParamTabletProgress> &progresses)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObArenaAllocator allocator("VecAlterChd", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  const ObVecIndexAlterParamTaskInfo &task_info =
      ctx_->task_status_.task_info_.alter_param_info_;
  if (OB_ISNULL(sql_proxy) || progresses.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid child task query argument", KR(ret), KP(sql_proxy), K(progresses));
  }
  for (int64_t type_idx = 0; OB_SUCC(ret) && type_idx < 2; ++type_idx) {
    const bool freeze = 0 == type_idx;
    const ObVecIndexAsyncTaskType task_type = freeze
        ? OB_VECTOR_ASYNC_INDEX_FREEZE : OB_VECTOR_ASYNC_INDEX_MERGE;
    const uint64_t table_id = freeze
        ? progresses.at(0).inc_table_id_ : progresses.at(0).vbitmap_table_id_;
    for (int64_t source_idx = 0; OB_SUCC(ret) && source_idx < 2; ++source_idx) {
      const bool history = 1 == source_idx;
      const char *task_table = history
          ? OB_ALL_VECTOR_INDEX_TASK_HISTORY_TNAME : OB_ALL_VECTOR_INDEX_TASK_TNAME;
      ObVecIndexFieldArray filters;
      ObVecIndexTaskStatusField tenant_filter;
      ObVecIndexTaskStatusField table_filter;
      ObVecIndexTaskStatusField type_filter;
      ObVecIndexTaskStatusArray rows;
      tenant_filter.field_name_ = "tenant_id";
      tenant_filter.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
      tenant_filter.data_.uint_ = ObSchemaUtils::get_extract_tenant_id(
          tenant_id_, tenant_id_);
      table_filter.field_name_ = "table_id";
      table_filter.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
      table_filter.data_.uint_ = table_id;
      type_filter.field_name_ = "task_type";
      type_filter.type_ = ObVecIndexTaskStatusField::INT_TYPE;
      type_filter.data_.int_ = task_type;
      allocator.reuse();
      if (OB_FAIL(filters.push_back(tenant_filter))
          || OB_FAIL(filters.push_back(table_filter))
          || OB_FAIL(filters.push_back(type_filter))) {
        LOG_WARN("fail to build child task filters", KR(ret), K(table_id), K(task_type));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_ls_scope_vec_tasks_from_inner_table(
              tenant_id_, task_table, false, filters, *sql_proxy, rows, &allocator))) {
        LOG_WARN("fail to read alter param child tasks",
                 KR(ret), K(table_id), K(task_type), K(history));
      } else {
        for (int64_t i = 0; i < rows.count(); ++i) {
          ObVecIndexTaskStatus &row = rows.at(i);
          int64_t progress_idx = -1;
          for (int64_t j = 0; j < progresses.count() && progress_idx < 0; ++j) {
            const ObTabletID &tablet_id = freeze
                ? progresses.at(j).inc_tablet_id_ : progresses.at(j).vbitmap_tablet_id_;
            if (tablet_id == row.tablet_id_) {
              progress_idx = j;
            }
          }
          if (progress_idx >= 0) {
            AlterParamTabletProgress &progress = progresses.at(progress_idx);
            const ObVecIndexAlterParamTaskInfo &child_info =
                row.task_info_.alter_param_info_;
            const bool is_alter_param_child =
                row.trigger_type_ == OB_VEC_TRIGGER_ALTER_INDEX_PARAM
                && child_info.is_child_valid();
            const bool matched = is_alter_param_child
                && child_info.parent_task_id_ == task_info.parent_task_id_
                && child_info.index_params_.str() == task_info.index_params_.str();
            const bool stale = is_alter_param_child
                && child_info.parent_task_id_ != task_info.parent_task_id_;
            if (!history && row.status_ != OB_VECTOR_ASYNC_TASK_FINISH && stale) {
              const int tmp_ret = cancel_children_(&child_info);
              if (OB_SUCCESS != tmp_ret) {
                // Keep the latest parent retryable.  The next scheduling round
                // reloads the row and retries cancellation from its new state.
                LOG_WARN("fail to cancel stale alter param child, retry next round",
                         KR(tmp_ret), K(row), "current_parent_task_id",
                         task_info.parent_task_id_);
              } else {
                LOG_INFO("cancel stale alter param child for newer parent",
                         K(row), "current_parent_task_id", task_info.parent_task_id_);
                DEBUG_SYNC(AFTER_ALTER_INDEX_PARAM_CANCEL_STALE_CHILD);
              }
              // Even a pending child that was synchronously changed to FINISH
              // may still have a stale queue node.  Do not create a replacement
              // child until a fresh inner-table scan confirms it disappeared.
              progress.has_running_conflict_task_ = true;
            } else if (!history && row.status_ != OB_VECTOR_ASYNC_TASK_FINISH && !matched) {
              progress.has_running_conflict_task_ = true;
            } else if (matched) {
              AlterParamChildTaskInfo &latest =
                  freeze ? progress.freeze_task_ : progress.merge_task_;
              const bool newer = !latest.is_valid()
                  || row.gmt_create_ > latest.gmt_create_
                  || (row.gmt_create_ == latest.gmt_create_
                      && row.task_id_ > latest.task_id_);
              if (newer) {
                latest.task_id_ = row.task_id_;
                latest.task_type_ = row.task_type_;
                latest.status_ = row.status_;
                latest.ret_code_ = static_cast<int>(row.ret_code_);
                latest.gmt_create_ = row.gmt_create_;
              }
            }
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < progresses.count(); ++i) {
    AlterParamTabletProgress &progress = progresses.at(i);
    if (progress.freeze_task_.is_valid()
        && !progress.freeze_task_.is_finished()
        && progress.merge_task_.is_valid()
        && !progress.merge_task_.is_finished()) {
      progress.has_running_conflict_task_ = true;
      LOG_WARN("freeze and merge tasks are both running", K(progress));
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::restore_tablet_status_(
    AlterParamTabletProgress &progress)
{
  int ret = OB_SUCCESS;
  const AlterParamChildTaskInfo &freeze = progress.freeze_task_;
  const AlterParamChildTaskInfo &merge = progress.merge_task_;
  if (progress.has_running_conflict_task_) {
  } else if (freeze.is_valid() && !freeze.is_finished()) {
    progress.status_ = WAIT_FREEZE_FINISH;
  } else if (merge.is_valid() && !merge.is_finished()) {
    progress.status_ = WAIT_MERGE_FINISH;
  } else if (!progress.has_active_segment_ || !progress.active_param_match_
             || progress.has_frozen_segment_) {
    progress.status_ = TRIGGER_FREEZE;
  } else if (progress.has_old_persistent_segment_) {
    progress.status_ = TRIGGER_MERGE;
  } else {
    progress.status_ = FINISHED;
  }
  return ret;
}

int ObVecAlterIndexParamTask::process_freeze_result_(
    AlterParamTabletProgress &progress)
{
  int ret = OB_SUCCESS;
  const AlterParamChildTaskInfo &freeze = progress.freeze_task_;
  if (!freeze.is_valid() || freeze.task_type_ != OB_VECTOR_ASYNC_INDEX_FREEZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid freeze task info", KR(ret), K(progress));
  } else if (freeze.is_finished()) {
    if (!freeze.is_success()) {
      progress.status_ = TRIGGER_FREEZE;
    } else if (progress.has_frozen_segment_
               || (progress.has_active_segment_ && !progress.active_param_match_)
               || (!progress.has_active_segment_
                   && progress.has_old_persistent_segment_)) {
      progress.status_ = TRIGGER_FREEZE;
    } else if (progress.has_old_persistent_segment_) {
      progress.status_ = TRIGGER_MERGE;
    } else {
      // A successful Freeze is sufficient for an empty Tablet without persistent data.
      progress.status_ = FINISHED;
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::process_merge_result_(
    AlterParamTabletProgress &progress)
{
  int ret = OB_SUCCESS;
  const AlterParamChildTaskInfo &merge = progress.merge_task_;
  if (!merge.is_valid() || merge.task_type_ != OB_VECTOR_ASYNC_INDEX_MERGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge task info", KR(ret), K(progress));
  } else if (merge.is_finished()) {
    if (!merge.is_success()) {
      progress.status_ = TRIGGER_MERGE;
    } else if (!progress.has_active_segment_ || !progress.active_param_match_
               || progress.has_frozen_segment_) {
      progress.status_ = TRIGGER_FREEZE;
    } else if (progress.has_old_persistent_segment_) {
      progress.status_ = TRIGGER_MERGE;
    } else {
      progress.status_ = FINISHED;
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::create_child_(
    const AlterParamTabletProgress &progress,
    const bool freeze)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = freeze ? progress.inc_table_id_ : progress.vbitmap_table_id_;
  const ObTabletID &tablet_id = freeze
      ? progress.inc_tablet_id_ : progress.vbitmap_tablet_id_;
  const ObVecIndexAsyncTaskType task_type = freeze
      ? OB_VECTOR_ASYNC_INDEX_FREEZE : OB_VECTOR_ASYNC_INDEX_MERGE;
  int64_t task_id = OB_INVALID_ID;
  common::ObCurTraceId::TraceId trace_id;
  ObVecIndexAsyncTaskCtx child_ctx;
  ObVecIndexTaskCtxArray child_ctxs;
  if (OB_UNLIKELY(!tablet_id.is_valid()
                  || OB_INVALID_ID == table_id
                  || (task_type != OB_VECTOR_ASYNC_INDEX_FREEZE
                      && task_type != OB_VECTOR_ASYNC_INDEX_MERGE)
                  || !ctx_->task_status_.task_info_.alter_param_info_.is_child_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter param child", KR(ret), K(freeze), K(progress), KPC(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, task_id))) {
    LOG_WARN("fail to fetch alter param child task id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(
          task_id, &allocator_, trace_id))) {
    LOG_WARN("fail to fetch alter param child trace id", KR(ret), K(task_id));
  } else {
    child_ctx.tenant_id_ = tenant_id_;
    child_ctx.task_status_.tenant_id_ = tenant_id_;
    child_ctx.task_status_.table_id_ = table_id;
    child_ctx.task_status_.tablet_id_ = tablet_id;
    child_ctx.task_status_.task_id_ = task_id;
    child_ctx.task_status_.task_type_ = task_type;
    child_ctx.task_status_.trigger_type_ = OB_VEC_TRIGGER_ALTER_INDEX_PARAM;
    child_ctx.task_status_.status_ = OB_VECTOR_ASYNC_TASK_STANDBY;
    child_ctx.task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
    child_ctx.task_status_.trace_id_ = trace_id;
    child_ctx.task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time());
    child_ctx.task_status_.task_info_.alter_param_info_ =
        ctx_->task_status_.task_info_.alter_param_info_;
    if (OB_FAIL(child_ctxs.push_back(&child_ctx))) {
      LOG_WARN("fail to collect alter param child", KR(ret), K(task_id), K(tablet_id));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::insert_new_task(
            tenant_id_, child_ctxs))) {
      LOG_WARN("fail to insert alter param child", KR(ret), K(task_id), K(tablet_id));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to create alter param child", KR(ret), K(freeze), K(tablet_id), KPC(ctx_));
  } else {
    LOG_INFO("alter param child ready", K(freeze), K(task_id), K(tablet_id),
             "parent_task_id", ctx_->task_status_.task_info_.alter_param_info_.parent_task_id_);
  }
  return ret;
}

int ObVecAlterIndexParamTask::check_cancel_(bool &is_cancel)
{
  int ret = OB_SUCCESS;
  is_cancel = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter param task ctx", KR(ret), KP(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(ctx_, is_cancel))) {
    LOG_WARN("fail to check alter param parent cancel", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObVecAlterIndexParamTask::cancel_children_(
    const ObVecIndexAlterParamTaskInfo *target_info)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObArenaAllocator allocator("VecAlterCan", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  const ObVecIndexAlterParamTaskInfo &parent_info = OB_NOT_NULL(target_info)
      ? *target_info : ctx_->task_status_.task_info_.alter_param_info_;
  if (OB_ISNULL(sql_proxy) || !parent_info.is_child_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter param cancel argument", KR(ret), KP(sql_proxy), K(parent_info));
  }
  for (int64_t type_idx = 0; OB_SUCC(ret) && type_idx < 2; ++type_idx) {
    const ObVecIndexAsyncTaskType task_type = 0 == type_idx
        ? OB_VECTOR_ASYNC_INDEX_FREEZE : OB_VECTOR_ASYNC_INDEX_MERGE;
    ObVecIndexFieldArray filters;
    ObVecIndexTaskStatusField tenant_filter;
    ObVecIndexTaskStatusField trigger_filter;
    ObVecIndexTaskStatusField type_filter;
    ObVecIndexTaskStatusArray rows;
    tenant_filter.field_name_ = "tenant_id";
    tenant_filter.type_ = ObVecIndexTaskStatusField::UINT_TYPE;
    tenant_filter.data_.uint_ = ObSchemaUtils::get_extract_tenant_id(
        tenant_id_, tenant_id_);
    trigger_filter.field_name_ = "trigger_type";
    trigger_filter.type_ = ObVecIndexTaskStatusField::INT_TYPE;
    trigger_filter.data_.int_ = OB_VEC_TRIGGER_ALTER_INDEX_PARAM;
    type_filter.field_name_ = "task_type";
    type_filter.type_ = ObVecIndexTaskStatusField::INT_TYPE;
    type_filter.data_.int_ = task_type;
    allocator.reuse();
    if (OB_FAIL(filters.push_back(tenant_filter))
        || OB_FAIL(filters.push_back(trigger_filter))
        || OB_FAIL(filters.push_back(type_filter))) {
      LOG_WARN("fail to build alter param child cancel filters", KR(ret), K(task_type));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_ls_scope_vec_tasks_from_inner_table(
            tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, false, filters,
            *sql_proxy, rows, &allocator))) {
      LOG_WARN("fail to scan alter param children for cancel", KR(ret), K(task_type));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
        ObVecIndexTaskStatus &row = rows.at(i);
        const ObVecIndexAlterParamTaskInfo &child_info =
            row.task_info_.alter_param_info_;
        if (!child_info.is_child_valid()
            || child_info.parent_task_id_ != parent_info.parent_task_id_
            || child_info.index_params_.str() != parent_info.index_params_.str()) {
          continue;
        }
        switch (row.status_) {
          case OB_VECTOR_ASYNC_TASK_STANDBY:
          case OB_VECTOR_ASYNC_TASK_PREPARE:
          case OB_VECTOR_ASYNC_TASK_QUEUE: {
            ObVecIndexFieldArray update_fields;
            ObVecIndexTaskStatusField status_field;
            ObVecIndexTaskStatusField ret_code_field;
            ObSEArray<int64_t, 3> expected_pending;
            bool matched = false;
            status_field.field_name_ = "status";
            status_field.data_.int_ = OB_VECTOR_ASYNC_TASK_FINISH;
            ret_code_field.field_name_ = "ret_code";
            ret_code_field.data_.int_ = OB_CANCELED;
            row.status_ = OB_VECTOR_ASYNC_TASK_FINISH;
            row.ret_code_ = OB_CANCELED;
            row.end_time_ = ObTimeUtility::current_time();
            ObVecIndexTaskKey key(
                tenant_id_, row.table_id_, row.tablet_id_.id(), row.task_id_);
            if (OB_FAIL(update_fields.push_back(status_field))
                || OB_FAIL(update_fields.push_back(ret_code_field))
                || OB_FAIL(expected_pending.push_back(OB_VECTOR_ASYNC_TASK_STANDBY))
                || OB_FAIL(expected_pending.push_back(OB_VECTOR_ASYNC_TASK_PREPARE))
                || OB_FAIL(expected_pending.push_back(OB_VECTOR_ASYNC_TASK_QUEUE))) {
              LOG_WARN("fail to build pending child cancel update", KR(ret), K(row));
            } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(
                    tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, *sql_proxy, key,
                    update_fields, row.progress_info_, row.task_info_, row.exec_addr_,
                    row.priority_, row.start_time_, row.end_time_, row.err_msg_,
                    &expected_pending, &matched))) {
              LOG_WARN("fail to finish pending alter param child", KR(ret), K(row));
            } else if (!matched) {
              ret = OB_EAGAIN;
              LOG_INFO("alter param child advanced while canceling, retry scan",
                       KR(ret), K(row));
            } else {
              LOG_INFO("finished pending alter param child as canceled", K(row));
            }
            break;
          }
          case OB_VECTOR_ASYNC_TASK_RUNNING:
          case OB_VECTOR_ASYNC_TASK_EXCHANGE:
          case OB_VECTOR_ASYNC_TASK_CLEAN: {
            int cancel_ret = OB_SUCCESS;
            if (row.trace_id_.is_invalid() || !row.exec_addr_.is_valid()) {
              cancel_ret = OB_EAGAIN;
              LOG_INFO("active alter param child has no cancellable sys task yet",
                       KR(cancel_ret), K(row));
            } else if (row.exec_addr_ == GCTX.self_addr()) {
              cancel_ret = SYS_TASK_STATUS_MGR.cancel_task(row.trace_id_);
            } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
              cancel_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("srv rpc proxy is null while canceling alter param child",
                       KR(cancel_ret), K(row));
            } else {
              obrpc::ObCancelTaskArg arg;
              arg.task_id_ = row.trace_id_;
              cancel_ret = GCTX.srv_rpc_proxy_->to(row.exec_addr_).cancel_sys_task(arg);
            }
            if (OB_ENTRY_NOT_EXIST == cancel_ret) {
              // The child may have just finished or may not have registered its
              // sys task yet.  Retry from a fresh inner-table snapshot next round.
              ret = OB_EAGAIN;
              LOG_INFO("alter param child sys task disappeared, retry scan",
                       KR(ret), K(row));
            } else if (OB_SUCCESS != cancel_ret) {
              ret = cancel_ret;
              LOG_WARN("fail to cancel active alter param child", KR(ret), K(row));
            } else {
              LOG_INFO("sent cancel to active alter param child", K(row));
            }
            break;
          }
          case OB_VECTOR_ASYNC_TASK_CANCEL:
          case OB_VECTOR_ASYNC_TASK_FINISH:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected alter param child status", KR(ret), K(row));
            break;
        }
      }
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::finish_parent_as_canceled_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArenaAllocator allocator("VecAlterCanP", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObVecIndexTaskStatusArray rows;
  const ObVecIndexAlterParamTaskInfo &task_info =
      ctx_->task_status_.task_info_.alter_param_info_;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null while finishing canceled alter parent", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start canceled alter parent transaction", KR(ret));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_alter_index_param_parent_task(
          tenant_id_, task_info.parent_task_id_, true, trans, rows, allocator))) {
    LOG_WARN("fail to lock canceled alter param parent", KR(ret), K(task_info));
  } else if (rows.empty()) {
    // Normal completion/archive won the race.  There is no parent left to cancel.
    LOG_INFO("alter param parent already disappeared while canceling", K(task_info));
  } else if (1 != rows.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter param parent count while canceling",
             KR(ret), K(rows.count()), K(task_info));
  } else {
    ObVecIndexTaskStatus &parent = rows.at(0);
    if (parent.status_ == OB_VECTOR_ASYNC_TASK_FINISH) {
    } else if (parent.table_id_ != ctx_->task_status_.table_id_
               || parent.status_ != OB_VECTOR_ASYNC_TASK_RUNNING
               || !ObVecIndexAsyncTaskUtil::is_alter_index_param_parent_matched(
                      parent, task_info)) {
      ret = OB_EAGAIN;
      LOG_INFO("alter param parent changed while canceling", KR(ret), K(parent), K(task_info));
    } else {
      parent.status_ = OB_VECTOR_ASYNC_TASK_FINISH;
      parent.ret_code_ = OB_CANCELED;
      parent.end_time_ = ObTimeUtility::current_time();
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_alter_index_param_parent_task(
              tenant_id_, trans, parent))) {
        LOG_WARN("fail to finish alter param parent as canceled", KR(ret), K(parent));
      }
    }
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret && OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::handle_cancel_(bool &all_finished)
{
  int ret = OB_SUCCESS;
  int cancel_ret = OB_SUCCESS;
  bool is_local_cancel = false;
  all_finished = false;
  {
    common::ObSpinLockGuard guard(ctx_->lock_);
    is_local_cancel = ctx_->task_status_.status_
        == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL;
  }
  // check_cancel_() combines local ctx CANCEL and the sys-task cancel flag.
  // SYS LS leader switch marks the ctx CANCEL to stop the old leader's instance.
  // In that case, leave children and the tablet_id=-1 Parent intact for recovery;
  // persisting FINISH/OB_CANCELED here would prevent the new leader from resuming it.
  // all_finished only ends this runtime ctx. Local CANCEL takes precedence over
  // a concurrent user cancel; perform sys-task cancellation outside the ctx lock.
  if (is_local_cancel) {
    ret = OB_CANCELED;
    all_finished = true;
  } else if (OB_SUCCESS != (cancel_ret = cancel_children_())) {
    // Keep ret successful and all_finished false to retry next round.
    // The sys-task cancel flag is retained across RUNNING->PREPARE.
    LOG_WARN("fail to cancel alter param children, retry next round",
             KR(cancel_ret), KPC(ctx_));
  } else if (OB_SUCCESS != (cancel_ret = finish_parent_as_canceled_())) {
    LOG_WARN("fail to finish alter param cancellation, retry next round",
             KR(cancel_ret), KPC(ctx_));
  } else {
    ret = OB_CANCELED;
    all_finished = true;
    LOG_INFO("alter index param parent and children canceled", KPC(ctx_));
  }
  return ret;
}

int ObVecAlterIndexParamTask::update_parent_(
    const int64_t schema_version,
    const int64_t progress,
    const bool converged,
    bool &parent_valid)
{
  int ret = OB_SUCCESS;
  parent_valid = false;
  ObMySQLTransaction trans;
  ObArenaAllocator allocator("VecAlterUpd", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObVecIndexTaskStatusArray rows;
  const ObVecIndexAlterParamTaskInfo &task_info =
      ctx_->task_status_.task_info_.alter_param_info_;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start alter parent transaction", KR(ret));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_alter_index_param_parent_task(
          tenant_id_, task_info.parent_task_id_, true, trans, rows, allocator))) {
    LOG_WARN("fail to lock alter param parent", KR(ret), K(task_info));
  } else if (1 != rows.count()) {
    ret = OB_CANCELED;
    LOG_INFO("alter param parent disappeared before progress update",
             KR(ret), K(task_info), K(rows.count()));
  } else {
    ObVecIndexTaskStatus &parent = rows.at(0);
    parent_valid = parent.table_id_ == ctx_->task_status_.table_id_
        && parent.status_ == OB_VECTOR_ASYNC_TASK_RUNNING
        && ObVecIndexAsyncTaskUtil::is_alter_index_param_parent_matched(parent, task_info);
    if (!parent_valid) {
      ret = OB_CANCELED;
      LOG_INFO("alter param parent changed before progress update",
               KR(ret), K(parent), K(task_info));
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *inc_schema = nullptr;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard in parent transaction", KR(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(
              tenant_id_, parent.table_id_, inc_schema))) {
        LOG_WARN("fail to get parent index schema", KR(ret), K(parent));
      } else if (OB_ISNULL(inc_schema)
                 || inc_schema->get_schema_version() != schema_version) {
        ret = OB_SCHEMA_EAGAIN;
      } else {
        ObVecIndexAlterParamTaskInfo &parent_info =
            parent.task_info_.alter_param_info_;
        const int64_t new_progress = converged ? 100 : MIN(progress, 99);
        int64_t elapsed_time_s = 0;
        if (converged) {
          parent.status_ = OB_VECTOR_ASYNC_TASK_FINISH;
          parent.ret_code_ = OB_SUCCESS;
          parent.end_time_ = ObTimeUtility::current_time();
        }
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::calc_alter_index_param_elapsed_time_s(
                parent, ObTimeUtility::current_time(), elapsed_time_s))) {
          LOG_WARN("fail to calculate alter param parent elapsed time", KR(ret), K(parent));
        } else if (new_progress != parent_info.progress_
                   || elapsed_time_s != parent_info.elapsed_time_s_ || converged) {
          parent_info.progress_ = new_progress;
          if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_alter_index_param_parent_task(
                  tenant_id_, trans, parent))) {
            LOG_WARN("fail to update alter param parent", KR(ret), K(parent));
          }
        }
      }
    }
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret && OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObVecAlterIndexParamTask::do_work()
{
  int ret = OB_SUCCESS;
  bool parent_valid = false;
  bool all_finished = false;
  bool is_cancel = false;
  int64_t schema_version = OB_INVALID_VERSION;
  ObArray<AlterParamTabletProgress> progresses;
  ObVectorIndexParam target_param;
  common::hash::ObHashSet<uint64_t> conflict_table_id_set;
  common::hash::ObHashSet<uint64_t> conflict_index_task_set;
  bool ddl_conflict_checked = false;
  LOG_INFO("start alter index param task round", KPC(ctx_));
  if (IS_NOT_INIT || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter index param task", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(check_cancel_(is_cancel))) {
  } else if (is_cancel) {
    ret = handle_cancel_(all_finished);
  } else {
    const ObVecIndexAlterParamTaskInfo &task_info =
        ctx_->task_status_.task_info_.alter_param_info_;
    if (!task_info.is_child_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(check_parent_(parent_valid))) {
    } else if (!parent_valid) {
      ret = OB_CANCELED;
      all_finished = true;
    } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
            task_info.index_params_.str(), ObVectorIndexType::VIT_HNSW_INDEX,
            target_param))) {
    } else if (OB_FAIL(load_tablet_progresses_(
            target_param, progresses, schema_version))) {
    } else {
      bool has_merge_candidate = false;
      for (int64_t i = 0; !has_merge_candidate && i < progresses.count(); ++i) {
        const AlterParamTabletProgress &progress = progresses.at(i);
        has_merge_candidate = !progress.has_running_conflict_task_
            && (!progress.freeze_task_.is_valid() || progress.freeze_task_.is_finished())
            && (!progress.merge_task_.is_valid() || progress.merge_task_.is_finished())
            && progress.has_active_segment_
            && progress.active_param_match_
            && !progress.has_frozen_segment_
            && progress.has_old_persistent_segment_;
      }
      if (has_merge_candidate) {
        DEBUG_SYNC(BEFORE_ALTER_INDEX_PARAM_MERGE_DDL_CHECK);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
          LOG_WARN("fail to create alter param DDL conflict table set, block Merge this round",
                   KR(tmp_ret), KPC(ctx_));
        } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
          LOG_WARN("fail to create alter param DDL conflict index set, block Merge this round",
                   KR(tmp_ret), KPC(ctx_));
        } else if (OB_TMP_FAIL(ObVecITaskExecutor::check_has_hnsw_ddl(
                tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
          LOG_WARN("fail to collect alter param DDL conflicts, block Merge this round",
                   KR(tmp_ret), KPC(ctx_));
        } else {
          ddl_conflict_checked = true;
        }
      }
      int64_t finished_count = 0;
      int64_t blocked_merge_count = 0;
      for (int64_t i = 0;
           OB_SUCC(ret) && !is_cancel && i < progresses.count(); ++i) {
        AlterParamTabletProgress &progress = progresses.at(i);
        if (progress.has_running_conflict_task_) {
          LOG_TRACE("alter param tablet cannot advance this round", K(progress));
        } else if (OB_FAIL(restore_tablet_status_(progress))) {
          LOG_WARN("fail to restore alter param tablet status", KR(ret), K(progress));
        } else {
          switch (progress.status_) {
            case TRIGGER_FREEZE:
              DEBUG_SYNC(ALTER_INDEX_PARAMS_BEFORE_CREATE_FREEZE_CHILD);
              if (OB_FAIL(check_cancel_(is_cancel))) {
              } else if (is_cancel) {
              } else if (OB_FAIL(create_child_(progress, true))) {
                LOG_WARN("fail to create alter param freeze task", KR(ret), K(progress));
              } else {
                progress.status_ = WAIT_FREEZE_FINISH;
              }
              break;
            case WAIT_FREEZE_FINISH:
              if (OB_FAIL(process_freeze_result_(progress))) {
                LOG_WARN("fail to process alter param freeze result", KR(ret), K(progress));
              }
              break;
            case TRIGGER_MERGE: {
              DEBUG_SYNC(ALTER_INDEX_PARAMS_BEFORE_CREATE_MERGE_CHILD);
              const bool merge_blocked = !ddl_conflict_checked
                  || OB_HASH_EXIST == conflict_index_task_set.exist_refactored(
                      progress.inc_table_id_);
              if (merge_blocked) {
                ++blocked_merge_count;
              } else if (OB_FAIL(check_cancel_(is_cancel))) {
              } else if (is_cancel) {
              } else if (OB_FAIL(create_child_(progress, false))) {
                LOG_WARN("fail to create alter param merge task", KR(ret), K(progress));
              } else {
                progress.status_ = WAIT_MERGE_FINISH;
              }
              break;
            }
            case WAIT_MERGE_FINISH:
              if (OB_FAIL(process_merge_result_(progress))) {
                LOG_WARN("fail to process alter param merge result", KR(ret), K(progress));
              }
              break;
            case FINISHED:
              ++finished_count;
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected alter param tablet status", KR(ret), K(progress));
              break;
            }
        }
      }
      if (blocked_merge_count > 0 && REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("skip alter param Merge creation due to DDL conflict check",
                 K_(tenant_id), K(ddl_conflict_checked), K(blocked_merge_count),
                 "parent_task_id", task_info.parent_task_id_,
                 "index_table_id", ctx_->task_status_.table_id_);
      }
      DEBUG_SYNC(ALTER_INDEX_PARAMS_BEFORE_UPDATE_PARENT);
      if (OB_SUCC(ret) && !is_cancel && OB_FAIL(check_cancel_(is_cancel))) {
        LOG_WARN("fail to check cancel before updating alter param parent", KR(ret), KPC(ctx_));
      }
      if (OB_SUCC(ret) && is_cancel) {
        ret = handle_cancel_(all_finished);
      } else if (OB_SUCC(ret)) {
        const int64_t parent_progress = progresses.empty()
            ? 0 : finished_count * 100 / progresses.count();
        const bool converged = !progresses.empty()
            && finished_count == progresses.count();
        if (OB_FAIL(update_parent_(
                schema_version, parent_progress, converged, parent_valid))) {
        } else {
          all_finished = !parent_valid || converged;
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
    ctx_->task_status_.all_finished_ = all_finished;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("alter index param task round failed", KR(ret), KPC(ctx_));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

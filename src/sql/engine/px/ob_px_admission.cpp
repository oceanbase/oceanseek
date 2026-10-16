/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include "ob_px_admission.h"
#include "observer/omt/ob_tenant.h"
#include "ob_px_target_mgr.h"
#include "share/logical_table/ob_namespace_ru_manager.h"
#include "share/logical_table/logical_table_monitor.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/resolver/ob_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

static int resolve_plan_namespace_id(ObExecContext &exec_ctx,
                                     int64_t param_idx,
                                     uint64_t literal,
                                     uint64_t &ns_id)
{
  int ret = OB_SUCCESS;
  ns_id = 0;
  if (param_idx >= 0) {
    ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan ctx is null", K(ret), K(param_idx));
    } else {
      const ParamStore &ps = plan_ctx->get_param_store();
      if (param_idx < 0 || param_idx >= ps.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ns param idx out of range", K(ret), K(param_idx), K(ps.count()));
      } else {
        const ObObj *ns_obj = &ps.at(param_idx);
        if (ns_obj->is_ext_sql_array()) {
          const ObSqlArrayObj *array_obj =
              reinterpret_cast<const ObSqlArrayObj *>(ns_obj->get_ext());
          if (OB_ISNULL(array_obj) || OB_ISNULL(array_obj->data_) || array_obj->count_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ns sql array param is invalid", K(ret), K(*ns_obj), K(param_idx));
          } else {
            ns_obj = &array_obj->data_[0];
          }
        }
        if (OB_SUCC(ret)) {
          uint64_t ns_val = 0;
          int64_t ns_ival = 0;
          if (OB_SUCC(ns_obj->get_uint64(ns_val))) {
            ns_id = ns_val;
          } else if (OB_SUCC(ns_obj->get_int(ns_ival)) && ns_ival >= 0) {
            ns_id = static_cast<uint64_t>(ns_ival);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ns param is not valid uint64", K(ret), K(*ns_obj), K(param_idx));
          }
        }
      }
    }
  } else {
    ns_id = literal;
  }
  return ret;
}

static int get_plan_namespace_id(ObExecContext &exec_ctx, const ObPhysicalPlan &plan, uint64_t &ns_id)
{
  int64_t param_idx = plan.get_logical_ns_param_idx();
  uint64_t literal = plan.get_logical_ns_literal();
  if (plan.get_logical_dml_ns_param_idx() >= 0 || 0 != plan.get_logical_dml_ns_literal()) {
    param_idx = plan.get_logical_dml_ns_param_idx();
    literal = plan.get_logical_dml_ns_literal();
  }
  return resolve_plan_namespace_id(exec_ctx, param_idx, literal, ns_id);
}

static int adjust_namespace_threads_by_id(uint64_t ns_id,
                                          int64_t count,
                                          int64_t deadline_us,
                                          bool &acquired)
{
  int ret = OB_SUCCESS;
  acquired = true;
  oceanbase::share::ObNamespaceRUManager *ns_mgr = MTL(oceanbase::share::ObNamespaceRUManager *);
  if (0 == ns_id || OB_INVALID_ID == ns_id || OB_ISNULL(ns_mgr) || 0 == count) {
    // non-namespace query, manager unavailable, or zero count: skip
  } else if (count > 0) {
    acquired = false;
    if (OB_FAIL(ns_mgr->acquire_thread(ns_id, count, deadline_us, acquired))) {
      LOG_WARN("fail to acquire namespace thread", K(ret), K(ns_id), K(count));
    }
  } else {
    ns_mgr->release_thread(ns_id, -count);
  }
  return ret;
}

int ObPxAdmission::get_parallel_session_target(ObSQLSessionInfo &session,
                                               int64_t minimal_session_target,
                                               int64_t &session_target)
{
  int ret = OB_SUCCESS;
  int64_t parallel_servers_target = INT64_MAX; // default to unlimited
  session_target = INT64_MAX; // default to unlimited
  uint64_t tenant_id = session.get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_servers_target(tenant_id, parallel_servers_target))) {
    LOG_WARN("get parallel_servers_target failed", K(ret));
  } else if (OB_UNLIKELY(minimal_session_target > parallel_servers_target)) {
    ret = OB_ERR_PARALLEL_SERVERS_TARGET_NOT_ENOUGH;
    LOG_WARN("minimal_session_target is more than parallel_servers_target", K(ret),
                                      K(minimal_session_target), K(parallel_servers_target));
  } else if (OB_LIKELY(tenant_config.is_valid())) {
    session_target = parallel_servers_target;
    int64_t pmas = tenant_config->_parallel_max_active_sessions;
    int64_t parallel_session_count;
    if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_session_count(tenant_id, parallel_session_count))) {
      LOG_WARN("get parallel_px_session failed", K(ret));
    } else if (pmas > 0 && parallel_servers_target != INT64_MAX && parallel_session_count > 0) {
      // when pmas is TOO large, session target could be less than one,
      // this is not good! We ensure this query can run with minimal threads here
      session_target = std::max(parallel_servers_target / pmas, minimal_session_target);
    }
  } else {
    // tenant_config is invalid, use parallel_servers_target
    session_target = parallel_servers_target;
  }
  LOG_TRACE("PX get parallel session target", K(tenant_id), K(tenant_config.is_valid()),
                        K(parallel_servers_target), K(minimal_session_target), K(session_target));
  return ret;
}

// 如果当前剩余线程数能满足 req_cnt，则分配线程给请求
// 但考虑到系统空闲时，要允许第一个请求执行，需要处理下面的特殊情况：
//   如果 请求的线程数 req_cnt 大于 limit，并且当前没有其它 px 请求（used = 0）
//   那么 就把所有线程分配给这个请求 (admit_cnt = req_cnt, used = limit)
//
//   推论：一个需要**过量**线程的请求，只会在系统空闲下来之后才会被调度
int64_t ObPxAdmission::admit(ObSQLSessionInfo &session, ObExecContext &exec_ctx,
                             int64_t wait_time_us, int64_t minimal_px_worker_count,
                             int64_t &session_target, ObHashMap<ObAddr, int64_t> &worker_map,
                             int64_t req_cnt, int64_t &admit_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session.get_effective_tenant_id();
  uint64_t admission_version = UINT64_MAX;
  // when pmas enabled, block thread until got expected thread resource
  int64_t left_time_us = wait_time_us;
  int64_t start_time_us = ObClockGenerator::getClock();
  bool need_retry = false;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check query status", K(ret));
    } else if (OB_FAIL(OB_PX_TARGET_MGR.apply_target(tenant_id, worker_map, wait_time_us,
                      session_target, req_cnt, admit_cnt, admission_version))) {
      LOG_WARN("apply target failed", K(ret), K(tenant_id), K(req_cnt));
    } else if (0 != admit_cnt) {
      exec_ctx.set_admission_version(admission_version);
      LOG_TRACE("after enter admission", K(ret), K(req_cnt), K(admit_cnt));
    }
    left_time_us = wait_time_us - (ObClockGenerator::getClock() - start_time_us);
    if (OB_SUCC(ret) && 0 == admit_cnt && left_time_us > 0) {
      if (!need_retry) {
        // only print once
        LOG_INFO("Not enough PX thread to execute query."
                 "should wait and re-acquire thread resource from target queue",
                 K(req_cnt), K(left_time_us));
        // fake one retry record, not really a query retry
        session.get_retry_info_for_update().set_last_query_retry_err(OB_ERR_INSUFFICIENT_PX_WORKER);
      }
      // parallel server target may changed
      if (OB_FAIL(get_parallel_session_target(session, minimal_px_worker_count, session_target))) {
        LOG_WARN("fail get session target", K(ret));
      } else {
        need_retry = true;
      }
    } else {
      need_retry = false;
    }
  } while (need_retry && OB_SUCC(ret));
  return ret;
}

int ObPxAdmission::do_px_admission(
    ObSQLSessionInfo &session, ObExecContext &exec_ctx, int64_t req_worker_count,
    int64_t minimal_px_worker_count, int64_t session_target,
    ObHashMap<ObAddr, int64_t> &acl_px_worker_map, int64_t wait_time_us,
    int64_t &admit_worker_count)
{
  int ret = OB_SUCCESS;
  int64_t start_us = ObClockGenerator::getClock();
  int64_t deadline_us = start_us + wait_time_us;
  bool ns_acquired_succ = false;
  bool need_ns_thread_limit = false;
  int64_t ns_held_worker_count = 0;
  uint64_t ns_id = 0;
  const ObPhysicalPlan *phy_plan = exec_ctx.get_physical_plan_ctx()->get_phy_plan();
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null in px admission", K(ret));
  } else if (OB_FAIL(get_plan_namespace_id(exec_ctx, *phy_plan, ns_id))) {
    LOG_WARN("fail to get namespace_id from plan", K(ret));
  // 1. 如果是namespace里面的表, 先获取 req_worker_count 个 namespace 线程槽位（超时不报错）
  } else if (ns_id != 0 && OB_INVALID_ID != ns_id) {
    share::ObNamespaceRUManager *ns_mgr = MTL(share::ObNamespaceRUManager *);
    need_ns_thread_limit = OB_NOT_NULL(ns_mgr) && ns_mgr->is_thread_limited();
    if (need_ns_thread_limit) {
      if (OB_FAIL(adjust_namespace_threads_by_id(
              ns_id, req_worker_count, deadline_us, ns_acquired_succ))) {
        LOG_WARN("fail to adjust namespace threads", K(ret), K(ns_id), K(req_worker_count));
      } else if (ns_acquired_succ) {
        ns_held_worker_count = req_worker_count;
      }
    }
  }
  // 2. PX admission
  if (OB_FAIL(ret)) {
  } else if (need_ns_thread_limit && !ns_acquired_succ) {
    ret = OB_KILLED_BY_THROTTLING;
    LOG_INFO("namespace thread limit reached before px admission",
             KR(ret), K(ns_id), K(req_worker_count), K(deadline_us));
  } else if (!need_ns_thread_limit || ns_acquired_succ) {
    int64_t left_us = std::max(deadline_us - ObClockGenerator::getClock(),
                               static_cast<int64_t>(0));
    if (OB_FAIL(ObPxAdmission::admit(session, exec_ctx, left_us,
                               minimal_px_worker_count, session_target,
                               acl_px_worker_map, req_worker_count, admit_worker_count))) {
      LOG_WARN("failed to admit", K(ret));
    }
  }
  // 3. 补差额并把 NS 线程槽位记录到 exec_ctx
  if (OB_SUCC(ret) && ns_held_worker_count > 0 && admit_worker_count > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObPxAdmission::settle_namespace_threads(
        session, exec_ctx, ns_id, admit_worker_count, deadline_us, ns_held_worker_count))) {
      ret = tmp_ret;
      LOG_WARN("failed to settle_namespace_threads", K(ret), K(ns_id),
               K(admit_worker_count), K(ns_held_worker_count));
    } else {
      ns_held_worker_count = 0;
    }
  }
  // PX admit 失败、未分配 worker 或结算失败时，只释放仍由本函数持有的 NS 槽位。
  if (ns_held_worker_count > 0 && (OB_FAIL(ret) || admit_worker_count <= 0)) {
    int tmp_ret = OB_SUCCESS;
    bool unused = false;
    if (OB_TMP_FAIL(adjust_namespace_threads_by_id(
            ns_id, -ns_held_worker_count, deadline_us, unused))) {
      LOG_WARN("fail to release namespace threads on admission cleanup",
               KR(tmp_ret), K(ns_id), K(ns_held_worker_count), K(ret), K(admit_worker_count));
    }
  }
  return ret;
}

int ObPxAdmission::enter_query_admission(ObSQLSessionInfo &session,
                                         ObExecContext &exec_ctx,
                                         sql::stmt::StmtType stmt_type,
                                         ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  uint64_t throttle_ns_id = 0;
  if (OB_FAIL(get_plan_namespace_id(exec_ctx, plan, throttle_ns_id))) {
    LOG_WARN("fail to get namespace_id from plan", K(ret));
  } else if (0 != throttle_ns_id && OB_INVALID_ID != throttle_ns_id) {
    // namespace 查询：QPS/TPS 前置限流，覆盖所有 plan 查询（含非 PX 点查）
    if (OB_FAIL(check_namespace_rate_limit(session, throttle_ns_id, stmt_type))) {
      LOG_WARN("namespace request throttled", KR(ret), K(throttle_ns_id));
    }
  }
  if (OB_SUCC(ret)
      && !plan.get_logical_collection_id().empty()
      && (plan.get_logical_dml_ns_param_idx() >= 0 || 0 != plan.get_logical_dml_ns_literal())) {
    uint64_t dml_target_ns_id = 0;
    // row limit 只能使用 DML 目标 ns，不能回落 scan ns，避免 INSERT ... SELECT 误用源表 ns。
    const int tmp_ret = resolve_plan_namespace_id(exec_ctx,
                                                   plan.get_logical_dml_ns_param_idx(),
                                                   plan.get_logical_dml_ns_literal(),
                                                   dml_target_ns_id);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to resolve DML target namespace_id, skip row limit check",
               K(tmp_ret), K(plan.get_logical_dml_ns_param_idx()));
    } else if (0 != dml_target_ns_id && OB_INVALID_ID != dml_target_ns_id) {
      if (OB_FAIL(check_namespace_row_limit(session, plan.get_logical_collection_id(),
                                            dml_target_ns_id, stmt_type))) {
        LOG_WARN("logical table row limit exceeded", KR(ret), K(dml_target_ns_id));
      }
    }
  }
  // 对于只有dop=1 的场景跳过检查，因为这种场景走 RPC 线程，不消耗 PX 线程
  // 
  if (OB_SUCC(ret)
      && stmt::T_EXPLAIN != stmt_type
      && plan.is_use_px()
      && 1 != plan.get_px_dop()
      && plan.get_expected_worker_count() > 0) {
    // use for appointment
    const auto &req_px_worker_map = plan.get_expected_worker_map();
    ObHashMap<ObAddr, int64_t> &acl_px_worker_map = exec_ctx.get_admission_addr_map();
    if (acl_px_worker_map.created()) {
      acl_px_worker_map.clear();
    } else if (OB_FAIL(acl_px_worker_map.create(hash::cal_next_prime(10), ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
      LOG_WARN("create hash map failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      for (auto it = req_px_worker_map.begin(); 
          OB_SUCC(ret) && it != req_px_worker_map.end(); ++it) {
        if (OB_FAIL(acl_px_worker_map.set_refactored(it->first, it->second))){
          LOG_WARN("set refactored failed", K(ret), K(it->first), K(it->second));
        }
      }
      // use for exec
      int64_t req_worker_count = plan.get_expected_worker_count();
      int64_t minimal_px_worker_count = plan.get_minimal_worker_count();
      int64_t admit_worker_count = 0;
      // 如果一直得不到线程资源，需要超时退出。
      // 下面处理带 timeout hint 的情景
      if (plan.get_phy_plan_hint().query_timeout_ > 0) {
        THIS_WORKER.set_timeout_ts(
            session.get_query_start_time() + plan.get_phy_plan_hint().query_timeout_);
      }
      int64_t wait_time_us = THIS_WORKER.get_timeout_remain();
      int64_t session_target = INT64_MAX;
      if (OB_FAIL(get_parallel_session_target(session, minimal_px_worker_count, session_target))) {
        LOG_WARN("fail get session target", K(ret));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("fail check query status", K(ret));
      } else if (OB_FAIL(do_px_admission(
          session, exec_ctx, req_worker_count, minimal_px_worker_count,
          session_target, acl_px_worker_map, wait_time_us, admit_worker_count))) {
        LOG_WARN("failed to do px namespace admission", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (admit_worker_count <= 0) {
        plan.inc_delayed_px_querys();
        ret = OB_ERR_INSUFFICIENT_PX_WORKER;
        ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(dop_, plan.get_px_dop());
        ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(required_px_workers_number_, req_worker_count);
        ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(admitted_px_workers_number_, admit_worker_count);
        LOG_INFO("This query is out of px worker resources and needs to be delayed; "
                "disconnection is unnecessary.",
                K(admit_worker_count),
                K(plan.get_px_dop()),
                K(plan.get_plan_id()),
                K(ret));
      } else {
        ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
        ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
        if (OB_ISNULL(plan_ctx) || OB_ISNULL(task_exec_ctx)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          plan_ctx->set_worker_count(admit_worker_count);
          // 表示 optimizer 计算的数量
          task_exec_ctx->set_expected_worker_cnt(req_worker_count);
          task_exec_ctx->set_minimal_worker_cnt(minimal_px_worker_count);
          // 表示 admission 根据当前资源排队情况实际分配的数量
          task_exec_ctx->set_admited_worker_cnt(admit_worker_count);
        }
        LOG_TRACE("PX admission set the plan worker count", K(req_worker_count), K(minimal_px_worker_count), K(admit_worker_count));
      }
    }
  } else if (OB_SUCC(ret)
             && stmt::T_EXPLAIN != stmt_type
             && 0 != throttle_ns_id && OB_INVALID_ID != throttle_ns_id
             && !(plan.is_use_px() && 1 != plan.get_px_dop())) {
    // 非 PX 路径（dop=1 / 非 PX 计划）的 namespace 查询：占用 1 个线程槽位
    if (OB_FAIL(acquire_non_px_namespace_thread(session, exec_ctx, throttle_ns_id))) {
      LOG_WARN("fail to acquire non-px namespace thread", KR(ret), K(throttle_ns_id));
    }
  }
  if (OB_SUCC(ret) && stmt::T_EXPLAIN != stmt_type && plan.get_das_dop() > 0) {
    int64_t minimal_px_worker_count = plan.get_minimal_worker_count();
    int64_t parallel_servers_target = INT64_MAX;
    if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_servers_target(session.get_effective_tenant_id(),
                                                             parallel_servers_target))) {
      LOG_WARN("get parallel_servers_target failed", K(ret));
    } else {
      int64_t real_das_dop = std::min(parallel_servers_target, plan.get_das_dop());
      exec_ctx.get_das_ctx().set_real_das_dop(real_das_dop);
      LOG_TRACE("real das dop", K(real_das_dop), K(plan.get_das_dop()), K(parallel_servers_target));
    }
  }
  return ret;
}

int ObPxAdmission::adjust_namespace_threads(ObSQLSessionInfo &session,
                                              ObExecContext &exec_ctx,
                                              int64_t count,
                                              int64_t deadline_us,
                                              bool &acquired)
{
  int ret = OB_SUCCESS;
  acquired = true;
  uint64_t ns_id = 0;
  const ObPhysicalPlan *phy_plan = exec_ctx.get_physical_plan_ctx()->get_phy_plan();
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null in adjust_namespace_threads", K(ret));
  } else if (OB_FAIL(get_plan_namespace_id(exec_ctx, *phy_plan, ns_id))) {
    LOG_WARN("fail to get namespace_id from plan", K(ret));
  } else if (OB_FAIL(adjust_namespace_threads_by_id(ns_id, count, deadline_us, acquired))) {
    LOG_WARN("fail to adjust namespace threads by id", KR(ret), K(ns_id), K(count));
  }
  return ret;
}

int ObPxAdmission::settle_namespace_threads(ObSQLSessionInfo &session,
                                              ObExecContext &exec_ctx,
                                              uint64_t ns_id,
                                              int64_t admit_worker_count,
                                              int64_t deadline_us,
                                              int64_t &held_worker_count)
{
  int ret = OB_SUCCESS;
  UNUSED(session);
  // 补差额：admit_worker_count - held_worker_count，通常为负数（释放多余槽位）
  if (0 == ns_id || OB_INVALID_ID == ns_id || admit_worker_count <= 0 || held_worker_count <= 0) {
    // no namespace slot to settle
  } else if (admit_worker_count != held_worker_count) {
    bool acquired = false;
    const int64_t diff = admit_worker_count - held_worker_count;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(adjust_namespace_threads_by_id(ns_id, diff, deadline_us, acquired))) {
      ret = tmp_ret;
      LOG_WARN("fail to adjust namespace thread delta", KR(ret), K(ns_id),
               K(diff), K(admit_worker_count), K(held_worker_count));
    } else if (diff > 0 && !acquired) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_INFO("namespace thread limit reached while settling px admission",
               KR(ret), K(ns_id), K(diff), K(admit_worker_count), K(held_worker_count));
    } else {
      held_worker_count = admit_worker_count;
    }
  }
  // 记录 NS 线程槽位，exit 时按当前 query 的 exec_ctx 精确释放
  if (OB_SUCC(ret) && ns_id != 0 && ns_id != OB_INVALID_ID) {
    if (OB_UNLIKELY(exec_ctx.has_namespace_admission())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("namespace admission record already exists",
               KR(ret), K(ns_id), K(admit_worker_count),
               "record_ns_id", exec_ctx.get_namespace_admission_ns_id(),
               "record_thread_count", exec_ctx.get_namespace_admission_thread_count());
    } else {
      exec_ctx.set_namespace_admission(ns_id, admit_worker_count);
    }
  }
  return ret;
}

int ObPxAdmission::check_namespace_rate_limit(ObSQLSessionInfo &session,
                                              uint64_t ns_id,
                                              sql::stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  share::ObNamespaceRUManager *ns_mgr = MTL(share::ObNamespaceRUManager *);
  if (OB_ISNULL(ns_mgr)) {
    // 限流器未注册/未初始化：按未限流处理
  } else {
    const share::ObNamespaceOpType op_type = ObStmt::is_dml_write_stmt(stmt_type)
        ? share::ObNamespaceOpType::WRITE : share::ObNamespaceOpType::READ;
    const int tmp_ret = ns_mgr->acquire(ns_id, op_type, session.get_database_name());
    if (OB_EAGAIN == tmp_ret) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_INFO("namespace request throttled by qps/tps limit",
               KR(ret), K(ns_id), "op_type", static_cast<int8_t>(op_type));
    } else if (OB_SUCCESS != tmp_ret) {
      // 内部错误（如未初始化）不应阻断查询执行
      LOG_WARN("namespace qps/tps acquire failed, ignored", K(tmp_ret), K(ns_id));
    }
  }
  return ret;
}

int ObPxAdmission::check_namespace_row_limit(ObSQLSessionInfo &session,
                                             const ObString &collection_id,
                                             uint64_t ns_id,
                                             sql::stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  if (stmt::T_INSERT != stmt_type && stmt::T_INSERT_ALL != stmt_type
      && stmt::T_REPLACE != stmt_type && stmt::T_MERGE != stmt_type) {
    // 非增加行数的语句不检查，避免超限后阻止 DELETE/UPDATE 自救。
  } else {
    share::ObLogicalTableMonitor *monitor = MTL(share::ObLogicalTableMonitor *);
    if (OB_ISNULL(monitor)) {
      // monitor 未初始化时按不限额处理。
    } else {
      bool is_exceeded = false;
      const int tmp_ret = monitor->check_logical_table_row_limit(
          ns_id, OB_INVALID_ID, session.get_database_name(), collection_id, is_exceeded);
      if (OB_SUCCESS != tmp_ret) {
        // monitor 内部 SQL 仅访问普通表，DML ns 与 collection_id 保持默认值而在入口跳过；
        // 且其语句是 SELECT/UPDATE，不属于增加行数的语句，双重避免递归检查。
        LOG_WARN("logical table row limit check failed, ignored",
                 K(tmp_ret), K(ns_id), K(collection_id));
      } else if (is_exceeded) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "insert that exceeds logical table row/size limit");
        if (REACH_TIME_INTERVAL(1000000)) {
          LOG_INFO("logical table row limit exceeded", KR(ret), K(ns_id), K(collection_id));
        }
      }
    }
  }
  return ret;
}

int ObPxAdmission::acquire_non_px_namespace_thread(ObSQLSessionInfo &session,
                                                   ObExecContext &exec_ctx,
                                                   uint64_t ns_id)
{
  int ret = OB_SUCCESS;
  share::ObNamespaceRUManager *ns_mgr = MTL(share::ObNamespaceRUManager *);
  if (OB_ISNULL(ns_mgr) || !ns_mgr->is_thread_limited()) {
    // 无限流器或未配置线程上限：直接放行，不注册 namespace admission 记录
  } else {
    const int64_t wait_time_us = std::max(THIS_WORKER.get_timeout_remain(),
                                          static_cast<int64_t>(0));
    const int64_t deadline_us = ObClockGenerator::getClock() + wait_time_us;
    bool acquired = false;
    if (OB_FAIL(ns_mgr->acquire_thread(ns_id, 1, deadline_us, acquired))) {
      LOG_WARN("fail to acquire non-px namespace thread", KR(ret), K(ns_id));
    } else if (!acquired) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_INFO("namespace thread limit reached, query throttled", KR(ret), K(ns_id));
    } else if (OB_UNLIKELY(exec_ctx.has_namespace_admission())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("namespace admission record already exists",
               KR(ret), K(ns_id),
               "record_ns_id", exec_ctx.get_namespace_admission_ns_id(),
               "record_thread_count", exec_ctx.get_namespace_admission_thread_count());
      ns_mgr->release_thread(ns_id, 1);
    } else {
      exec_ctx.set_namespace_admission(ns_id, 1);
    }
  }
  return ret;
}

void ObPxAdmission::exit_query_admission(ObSQLSessionInfo &session,
                                         ObExecContext &exec_ctx,
                                         sql::stmt::StmtType stmt_type,
                                         ObPhysicalPlan &plan)
{
  if (stmt::T_EXPLAIN != stmt_type
      && plan.is_use_px()
      && 1 != plan.get_px_dop()
      && exec_ctx.get_admission_version() != UINT64_MAX) {
    int ret = OB_SUCCESS;
    uint64_t tenant_id = session.get_effective_tenant_id();
    hash::ObHashMap<ObAddr, int64_t> &addr_map = exec_ctx.get_admission_addr_map();
    if (OB_FAIL(OB_PX_TARGET_MGR.release_target(tenant_id,
                                                addr_map,
                                                exec_ctx.get_admission_version()))) {
      LOG_WARN("release target failed", K(ret), K(tenant_id), K(exec_ctx.get_admission_version()));
    }
    (void)addr_map.destroy();
    LOG_DEBUG("release resource, notify wait threads");
  }
  // namespace 线程释放：覆盖 PX 与非 PX 路径，按本 query 记录的 namespace 槽位精确释放
  if (stmt::T_EXPLAIN != stmt_type
      && exec_ctx.has_namespace_admission()) {
    share::ObNamespaceRUManager *ns_mgr = MTL(share::ObNamespaceRUManager *);
    if (OB_NOT_NULL(ns_mgr)) {
      ns_mgr->release_thread(exec_ctx.get_namespace_admission_ns_id(),
                             exec_ctx.get_namespace_admission_thread_count());
    }
    exec_ctx.reset_namespace_admission();
  }
}

// 供给 SQC 端使用的 Admission 模块
// 每个租户一个资源池
void ObPxSubAdmission::acquire(int64_t max, int64_t min, int64_t tenant_min_cpu,
                               uint64_t tenant_id, int64_t &acquired_cnt)
{
  UNUSED(min);
  int64_t upper_bound = 1;
  if (OB_LIKELY(tenant_min_cpu > 0)) {
    oceanbase::omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid()) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "get tenant config failed, use default cpu_quota_concurrency");
      upper_bound = tenant_min_cpu * 4;
    } else {
      upper_bound = tenant_min_cpu * tenant_config->_max_px_workers_per_cpu;
    }
  }
  acquired_cnt = std::min(max, upper_bound);
}

void ObPxSubAdmission::release(int64_t acquired_cnt)
{
  UNUSED(acquired_cnt);
}

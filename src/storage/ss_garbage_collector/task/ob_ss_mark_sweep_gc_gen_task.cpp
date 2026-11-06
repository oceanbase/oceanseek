/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_sort.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/scn.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_bucket_num_helper.h"
#include "share/ob_max_id_fetcher.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_gc_id_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_define.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

ObSSMarkSweepGCGenTask::ObSSMarkSweepGCGenTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped),
    gc_disabled_release_checked_(false),
    need_retry_mark_sweep_dag_(false),
    last_check_executor_time_us_(0),
    round_id_(0)
{
}

ObSSMarkSweepGCGenTask::~ObSSMarkSweepGCGenTask()
{
  destroy();
}

int ObSSMarkSweepGCGenTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mark sweep gc gen task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule mark sweep gc gen task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSMarkSweepGCGenTask::destroy()
{
  gc_disabled_release_checked_ = false;
  last_check_executor_time_us_ = 0;
  round_id_ = 0;
  ATOMIC_STORE(&need_retry_mark_sweep_dag_, false);
  reset_task_state_();
}

bool ObSSMarkSweepGCGenTask::need_retry_mark_sweep_dag() const
{
  return ATOMIC_LOAD(&need_retry_mark_sweep_dag_);
}

void ObSSMarkSweepGCGenTask::set_need_retry_mark_sweep_dag(const bool need_retry)
{
  ATOMIC_STORE(&need_retry_mark_sweep_dag_, need_retry);
}

int ObSSMarkSweepGCGenTask::check_dag_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = true;
  bool unused_is_emergency = false;
  ObSSMarkSweepDag fake_dag;
  fake_dag.set_tenant_id(tenant_id_);
  ObTenantDagScheduler *dag_scheduler = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret));
  } else if (OB_FAIL(dag_scheduler->check_dag_exist(&fake_dag, is_exist, unused_is_emergency))) {
    LOG_WARN("fail to check mark sweep dag exist", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::cancel_dag_()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepDag fake_dag;
  fake_dag.set_tenant_id(tenant_id_);
  ObTenantDagScheduler *dag_scheduler = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret));
  } else if (OB_FAIL(dag_scheduler->cancel_dag(&fake_dag, true/*force_cancel*/))) {
    LOG_WARN("fail to cancel mark sweep dag", KR(ret), K_(tenant_id));
  } else {
    LOG_TRACE("succ to cancel mark sweep dag", K_(tenant_id));
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::cancel_dag_if_exist_()
{
  int ret = OB_SUCCESS;
  bool is_dag_exist = false;
  if (OB_FAIL(check_dag_exist_(is_dag_exist))) {
    LOG_WARN("fail to check mark sweep dag exist", KR(ret), K_(tenant_id));
  } else if (!is_dag_exist) {
    // do nothing.
  } else if (OB_FAIL(cancel_dag_())) { // is_dag_exist
    LOG_WARN("fail to cancel mark sweep dag", KR(ret), K_(tenant_id));
  }
  return ret;
}

// Read __all_shared_gc_control (no row lock) and, if executor_server_id no longer
// matches self, actively cancel the mark-sweep dag. This handles the case where an
// operator manually flips status to IDLE and switches the gc-sswriter to another server
// to force a takeover while the old executor is still running its dag.
int ObSSMarkSweepGCGenTask::check_executor_and_cancel_dag_if_not_self_()
{
  int ret = OB_SUCCESS;
  ObSharedGCControlInfo info;
  const uint64_t cur_server_id = GCTX.get_server_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::read_row(*GCTX.sql_proxy_, tenant_id_, info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_ERROR("control info row not exist while mark sweep dag is running, cancel mark sweep dag",
          KR(ret), K_(tenant_id), K(cur_server_id));
    } else {
      LOG_WARN("fail to read control info row", KR(ret), K_(tenant_id), K(cur_server_id));
    }
  } else if (info.executor_server_id_ == static_cast<int64_t>(cur_server_id)) {
    LOG_TRACE("executor still matches self, continue mark sweep dag", K_(tenant_id),
        K(cur_server_id), K(info));
  } else { // info.executor_server_id_ != static_cast<int64_t>(cur_server_id)
    LOG_INFO("executor has been switched to another server, cancel mark sweep dag",
        K_(tenant_id), K(cur_server_id), K(info));
    if (OB_FAIL(cancel_dag_if_exist_())) {
      LOG_WARN("fail to cancel mark sweep dag", KR(ret), K_(tenant_id), K(cur_server_id), K(info));
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::check_executor_and_round_id_(const int64_t round_id, bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  const uint64_t cur_server_id = GCTX.get_server_id();
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
                  trans, tenant_id_, info))) {
    LOG_WARN("fail to select for update", KR(ret), K_(tenant_id));
  } else if ((info.executor_server_id_ != static_cast<int64_t>(cur_server_id))
             || (info.gc_round_id_ != round_id)) {
    is_match = false;
    LOG_INFO("executor or round_id changed", K_(tenant_id),
        K(cur_server_id), K(round_id), K(info));
  } else {
    is_match = true;
    LOG_TRACE("executor and round_id still match self", K_(tenant_id), K(cur_server_id),
        K(round_id), K(info));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_(trans, OB_SUCCESS == ret/*want_commit*/, ret/*result*/))) {
    LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), K(cur_server_id));
  }
  return ret;
}

// Check whether the server is still an alive and active unit server of this tenant according
// to SVR_TRACER cache. The cache is refreshed every 8 seconds by server tracer's timer task.
int ObSSMarkSweepGCGenTask::check_tenant_server_normal_(const int64_t server_id, bool &is_normal)
{
  int ret = OB_SUCCESS;
  is_normal = false;
  ObAddr addr;
  bool is_alive = false;
  bool is_active = false;
  if (OB_FAIL(SVR_TRACER.get_tenant_server_addr(tenant_id_,
                                                static_cast<uint64_t>(server_id),
                                                addr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("server not in tenant server set",
          K_(tenant_id), K(server_id));
    } else {
      LOG_WARN("fail to get tenant server addr", KR(ret), K_(tenant_id), K(server_id));
    }
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(addr, is_alive))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("server addr not found in server tracer cache", K_(tenant_id), K(server_id), K(addr));
    } else {
      LOG_WARN("fail to check server alive", KR(ret), K_(tenant_id), K(server_id), K(addr));
    }
  } else if (!is_alive) {
    LOG_INFO("server is not alive", K_(tenant_id), K(server_id), K(addr));
  } else if (OB_FAIL(SVR_TRACER.check_server_active(addr, is_active))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("server addr not found in server tracer cache", K_(tenant_id), K(server_id), K(addr));
    } else {
      LOG_WARN("fail to check server active", KR(ret), K_(tenant_id), K(server_id), K(addr));
    }
  } else {
    is_normal = is_active;
    LOG_INFO("finish to check tenant server by server tracer cache", K_(tenant_id), K(server_id),
        K(addr), K(is_normal));
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::end_trans_(
    ObMySQLTransaction &trans,
    const bool want_commit,
    int &result)
{
  int ret = OB_SUCCESS;
  if (trans.is_started()) {
    if (OB_FAIL(trans.end(want_commit))) {
      LOG_WARN("fail to end trans", KR(ret), K_(tenant_id), K(want_commit));
      result = ((OB_SUCCESS == result) ? ret : result);
    }
  } else {
    // do nothing
  }
  return ret;
}

// release executor ownership (status DOING -> IDLE), if executor_server_id == cur_server_id
// and status == DOING. Otherwise, do nothing.
int ObSSMarkSweepGCGenTask::try_release_executor_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  const uint64_t cur_server_id = GCTX.get_server_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
                         trans, tenant_id_, info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("control info row not exist, maybe tenant create has not finished",
        KR(ret), K_(tenant_id), K(cur_server_id));
    } else {
      LOG_WARN("fail to select for update", KR(ret), K_(tenant_id));
    }
  } else if (ObSharedGCControlStatus::IDLE == info.status_) {
    // Already IDLE. do nothing
  } else if (ObSharedGCControlStatus::DOING == info.status_) {
    if (info.executor_server_id_ == static_cast<int64_t>(cur_server_id)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(ObSharedGCControlOperator::update_status(
                      trans, tenant_id_, ObSharedGCControlStatus::IDLE, affected_rows))) {
        LOG_WARN("fail to update status to IDLE", KR(ret), K_(tenant_id));
      } else if (OB_UNLIKELY(1 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected affected rows when releasing executor",
            KR(ret), K_(tenant_id), K(affected_rows), K(info));
      } else {
        LOG_INFO("succ to release executor (status DOING -> IDLE)",
            K_(tenant_id), K(cur_server_id), K(affected_rows), K(info));
      }
    } else { // info.executor_server_id_ != static_cast<int64_t>(cur_server_id)
      LOG_TRACE("executor is not self, do not release", K_(tenant_id), K(cur_server_id), K(info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K_(tenant_id), K(cur_server_id), K(info));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_(trans, OB_SUCCESS == ret/*want_commit*/, ret/*result*/))) {
    LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), K(cur_server_id));
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::fetch_new_round_id_(
    ObMySQLTransaction &trans,
    int64_t &round_id)
{
  int ret = OB_SUCCESS;
  round_id = 0;
  // USER allocations reuse the transaction holding the control lock.
  if (OB_FAIL(ObSSGCIdUtil::fetch_gc_id(
      trans, tenant_id_, OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE, round_id))) {
    LOG_WARN("fail to fetch mark sweep gc round id", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::update_gc_meta_scn_(
    const int64_t round_id,
    const SCN &safe_meta_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_server_id = GCTX.get_server_id();
  int64_t stmt_timeout = 0;
  bool is_updated = false;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  ObSharedMetaReservedMgr reserved_mgr;
  const ObSharedMetaReservedType gc_reserved_type(ObSharedMetaReservedTypeEnum::GC);
  // 1. Read the control row WITHOUT a row lock first, and bail out early when the
  // executor / round_id / gc_meta_scn does not allow this round to proceed. This
  // keeps the gc_control row lock OUT of the leading position: the only locks taken
  // below (inside the trans) are reserved_scn table lock then gc_control row lock,
  // matching every other writer's order and so avoiding an ABBA deadlock.
  // The authoritative re-check under the row lock is done by update_gc_meta_scn_in_trans.
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(round_id <= 0 || !safe_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update gc meta scn get invalid argument", KR(ret), K_(tenant_id),
        K(round_id), K(safe_meta_scn));
  } else if (OB_FAIL(ObSharedGCControlOperator::read_row(*GCTX.sql_proxy_, tenant_id_, info))) {
    LOG_WARN("fail to read control info row", KR(ret), K_(tenant_id));
  } else if ((info.executor_server_id_ != static_cast<int64_t>(cur_server_id))
      || (info.gc_round_id_ != round_id)) {
    ret = OB_EAGAIN;
    LOG_INFO("executor or round_id changed, give up this round",
        KR(ret), K_(tenant_id), K(cur_server_id), K(round_id), K(info));
  } else if (safe_meta_scn <= info.gc_meta_scn_) {
    ret = OB_EAGAIN;
    LOG_INFO("gc_meta_scn not updated, give up this round",
        KR(ret), K_(tenant_id), K(round_id), K(safe_meta_scn), K(info));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(reserved_mgr.init(tenant_id_))) {
      LOG_WARN("fail to init shared meta reserved mgr", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(reserved_mgr.remove_reserved_scn(
            trans, gc_reserved_type, info.gc_meta_scn_))) {
      LOG_WARN("fail to remove gc reserved scn", KR(ret), K_(tenant_id), K(info));
    } else if (OB_FAIL(reserved_mgr.create_reserved_scn(
            trans, gc_reserved_type, safe_meta_scn))) {
      // Reserve the new safe_meta_scn before advancing gc_meta_scn: at this point
      // gc_meta_scn is still the old value (< safe_meta_scn, guaranteed by the
      // check above), so create_reserved_scn's gc_meta_scn guard passes.
      LOG_WARN("fail to create gc reserved scn", KR(ret), K_(tenant_id),
          K(round_id), K(safe_meta_scn), K(info));
    } else if (OB_FAIL(ObSharedGCControlOperator::update_gc_meta_scn_in_trans(
            trans, tenant_id_, round_id, safe_meta_scn, is_updated))) {
      LOG_WARN("fail to update gc_meta_scn", KR(ret), K_(tenant_id), K(round_id),
          K(safe_meta_scn), K(info));
    } else if (OB_UNLIKELY(!is_updated)) {
      // executor/round_id changed or gc_meta_scn no longer advances under the row
      // lock (the unlocked read above raced a concurrent advance); give up this
      // round so the reserved row written above is rolled back with the trans.
      ret = OB_EAGAIN;
      LOG_INFO("gc_meta_scn not updated under row lock, give up this round", KR(ret),
          K_(tenant_id), K(round_id), K(safe_meta_scn), K(info));
    } else {
      LOG_INFO("succ to update gc_meta_scn", K_(tenant_id), K(round_id),
          K(safe_meta_scn), K(info), K(is_updated));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(end_trans_(trans, OB_SUCCESS == ret/*want_commit*/, ret/*result*/))) {
      LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), K(round_id), K(safe_meta_scn));
    }
  }

  return ret;
}

int ObSSMarkSweepGCGenTask::try_acquire_executor_(
    bool &is_acquired,
    bool &need_check_old_executor,
    int64_t &old_executor_server_id)
{
  int ret = OB_SUCCESS;
  is_acquired = false;
  need_check_old_executor = false;
  old_executor_server_id = 0;

  const uint64_t cur_server_id = GCTX.get_server_id();
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  int64_t affected_rows = 0;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
                  trans, tenant_id_, info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("control info row not exist, maybe tenant create has not finished",
          KR(ret), K_(tenant_id), K(cur_server_id));
    } else {
      LOG_WARN("fail to select for update", KR(ret), K_(tenant_id));
    }
  } else if (ObSharedGCControlStatus::IDLE == info.status_) {
    // take ownership (set executor=self, status=DOING, gc_round_id=new round_id).
    int64_t round_id = 0;
    if (OB_FAIL(fetch_new_round_id_(trans, round_id))) {
      LOG_WARN("fail to fetch new round id", KR(ret), K_(tenant_id), K(cur_server_id));
    } else if (OB_UNLIKELY(round_id <= MAX(info.gc_round_id_, info.allow_gc_round_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new gc round does not exceed persisted watermark; "
          "disable shared gc and verify gc id upgrade watermarks",
          KR(ret), K_(tenant_id), K(round_id), K(info));
    } else if (OB_FAIL(ObSharedGCControlOperator::update_executor_state(
                    trans, tenant_id_, static_cast<int64_t>(cur_server_id),
                    ObSharedGCControlStatus::DOING, round_id, affected_rows))) {
      LOG_WARN("fail to update executor state", KR(ret), K_(tenant_id), K(cur_server_id),
          K(info), K(round_id), K(affected_rows));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update executor state return unexpected affected rows", KR(ret), K_(tenant_id),
          K(cur_server_id), K(info), K(round_id), K(affected_rows));
    } else {
      is_acquired = true;
      round_id_ = round_id;
      LOG_INFO("acquire executor (IDLE -> DOING)", K_(tenant_id), K(cur_server_id),
          K(round_id), K(affected_rows), K(info));
    }
  } else if (ObSharedGCControlStatus::DOING == info.status_) {
    if (info.executor_server_id_ == static_cast<int64_t>(cur_server_id)) {
      // self executor retry: refresh gc_round_id with a new round_id so this retry round
      // is distinguishable from the previous one.
      int64_t round_id = 0;
      if (OB_FAIL(fetch_new_round_id_(trans, round_id))) {
        LOG_WARN("fail to fetch new round id", KR(ret), K_(tenant_id), K(cur_server_id));
      } else if (OB_UNLIKELY(round_id <= MAX(info.gc_round_id_, info.allow_gc_round_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("new gc round does not exceed persisted watermark; "
            "disable shared gc and verify gc id upgrade watermarks",
            KR(ret), K_(tenant_id), K(round_id), K(info));
      } else if (OB_FAIL(ObSharedGCControlOperator::update_executor_state(
                      trans, tenant_id_, static_cast<int64_t>(cur_server_id),
                      ObSharedGCControlStatus::DOING, round_id, affected_rows))) {
        LOG_WARN("fail to update executor state", KR(ret), K_(tenant_id), K(cur_server_id),
            K(info), K(round_id), K(affected_rows));
      } else if (OB_UNLIKELY(1 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update executor state return unexpected affected rows", KR(ret), K_(tenant_id),
            K(cur_server_id), K(info), K(round_id), K(affected_rows));
      } else {
        is_acquired = true;
        round_id_ = round_id;
        LOG_INFO("self executor retry, refresh round id", K_(tenant_id), K(cur_server_id),
            K(round_id), K(affected_rows), K(info));
      }
    } else { // info.executor_server_id_ != static_cast<int64_t>(cur_server_id)
      old_executor_server_id = info.executor_server_id_;
      need_check_old_executor = true;
      LOG_INFO("another server is executor, will check tenant unit on old executor server",
          K_(tenant_id), K(cur_server_id), K(info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K_(tenant_id), K(cur_server_id), K(info));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_(trans, OB_SUCCESS == ret/*want_commit*/, ret/*result*/))) {
    LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), K(cur_server_id));
    is_acquired = false;
    need_check_old_executor = false;
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::try_takeover_executor_(
    const int64_t old_executor_server_id,
    bool &is_takeovered)
{
  int ret = OB_SUCCESS;
  is_takeovered = false;
  const uint64_t cur_server_id = GCTX.get_server_id();
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  int64_t affected_rows = 0;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
                  trans, tenant_id_, info))) {
    LOG_WARN("fail to select for update", KR(ret), K_(tenant_id));
  } else if ((info.status_ == ObSharedGCControlStatus::DOING)
             && (info.executor_server_id_ == old_executor_server_id)) {
    int64_t round_id = 0;
    if (OB_FAIL(fetch_new_round_id_(trans, round_id))) {
      LOG_WARN("fail to fetch new round id", KR(ret), K_(tenant_id), K(cur_server_id),
          K(old_executor_server_id));
    } else if (OB_UNLIKELY(round_id <= MAX(info.gc_round_id_, info.allow_gc_round_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new gc round does not exceed persisted watermark; "
          "disable shared gc and verify gc id upgrade watermarks",
          KR(ret), K_(tenant_id), K(round_id), K(info));
    } else if (OB_FAIL(ObSharedGCControlOperator::update_executor_state(
                    trans, tenant_id_,
                    static_cast<int64_t>(cur_server_id),
                    ObSharedGCControlStatus::DOING, round_id, affected_rows))) {
      LOG_WARN("fail to takeover executor", KR(ret), K_(tenant_id),
          K(cur_server_id), K(old_executor_server_id), K(round_id));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update executor state return unexpected affected rows", KR(ret), K_(tenant_id),
          K(cur_server_id), K(old_executor_server_id), K(info), K(round_id), K(affected_rows));
    } else {
      is_takeovered = true;
      round_id_ = round_id;
      LOG_INFO("takeover executor", K_(tenant_id),
          K(cur_server_id), K(old_executor_server_id), K(round_id), K(affected_rows));
    }
  } else {
    LOG_INFO("control info changed before takeover, do not takeover now",
        K_(tenant_id), K(cur_server_id), K(old_executor_server_id), K(info));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_(trans, OB_SUCCESS == ret/*want_commit*/, ret/*result*/))) {
    LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), K(cur_server_id));
    is_takeovered = false;
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::try_acquire_or_takeover_executor_(bool &is_acquired_or_takeovered)
{
  int ret = OB_SUCCESS;
  is_acquired_or_takeovered = false;
  bool need_check_old_executor = false;
  int64_t old_executor_server_id = 0;
  bool is_normal = false;
  if (OB_FAIL(try_acquire_executor_(is_acquired_or_takeovered,
                  need_check_old_executor, old_executor_server_id))) {
    LOG_WARN("fail to try acquire executor", KR(ret), K_(tenant_id));
  } else if (need_check_old_executor) {
    if (OB_UNLIKELY(is_acquired_or_takeovered)) { // defence check
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected acquire executor result", KR(ret), K_(tenant_id),
          K(need_check_old_executor), K(is_acquired_or_takeovered), K(old_executor_server_id));
    } else if (OB_FAIL(check_tenant_server_normal_(old_executor_server_id, is_normal))) {
      LOG_WARN("fail to check tenant server normal", KR(ret), K_(tenant_id),
          K(old_executor_server_id));
    } else if (is_normal) {
      LOG_INFO("old executor still has tenant unit, do not takeover now",
          K_(tenant_id), K(old_executor_server_id));
    } else { // !is_normal
      if (OB_FAIL(try_takeover_executor_(old_executor_server_id, is_acquired_or_takeovered))) {
        LOG_WARN("fail to try takeover executor", KR(ret), K_(tenant_id),
            K(old_executor_server_id));
      }
    }
  }
  return ret;
}

// =============================================================================
// Schedule ObSSMarkSweepDag with gc-sswriter switch-leader awareness
// =============================================================================
// Goal: in the common case, only the gc sswriter of a tenant runs ObSSMarkSweepDag.
// gc sswriter is a per-tenant role and may switch between observer servers; we
// must coordinate so that:
//   - if the old gc sswriter is still running its dag, the new one does NOT
//     start a duplicate one — let the old one finish.
//   - if the old gc sswriter has no dag in flight, the new one takes over.
//   - if the old gc sswriter is shutdown / network-partitioned, the new one takes over.
//
// State is recorded in SYS tenant inner table __all_shared_gc_control,
// keyed by tenant_id. Two columns drive coordination:
//   - executor_server_id : the server currently considered "the executor"
//   - status             : IDLE | DOING
//
// -----------------------------------------------------------------------------
// Decision tree (the implementation of this function)
// -----------------------------------------------------------------------------
// 1. !enable_gc_service               → cancel_dag_if_exist_
// 2. is_dag_exist                     → check_executor_and_cancel_dag_if_not_self_
// 3. !is_dag_exist && !is_gc_sswriter → try_release_executor_
// 4. !is_dag_exist && is_gc_sswriter  → try_acquire_or_takeover_executor_
//
// 3. (!is_dag_exist && !is_gc_sswriter): if this server is still recorded as the
// executor with status DOING, flip it to IDLE inside one trans. Otherwise do nothing
//
// 4. (!is_dag_exist && is_gc_sswriter): coordinate via the row to acquire ownership
// before scheduling the dag. Sub-paths, decided under a `select for update`:
//   4.1 row not exist                           → wait until tenant create finishes;
//   4.2 status == IDLE                          → update (self, DOING);
//   4.3 status == DOING && executor == self     → do nothing;
//   4.4 status == DOING && executor != self     → release lock, and
//       check tenant unit on old executor?
//           - yes                   → do not takeover now; old will release via path 3;
//           - no                    → re-lock, verify executor still matches old,
//                                     if match, then take over (self, DOING);
//
// On dag completion: ObSSMarkSweepDag::report_result writes status=IDLE with a server-condition
// (`WHERE executor_server_id == cur_server_id`). If the row has already been taken over,
// affected_rows==0 and is treated as INFO log, not an ERROR log.
//
// -----------------------------------------------------------------------------
// Notes:
// -----------------------------------------------------------------------------
// 1. (duplicate execution under partition): if the old executor is active but
// SVR_TRACER cache has not refreshed server info, the new executor will take over
// and both servers may run mark-sweep concurrently. Fortunately, Mark-sweep
// deletes are idempotent, so the worst outcome is duplicate work.
//
// 2. If the old executor encounters an exception (e.g., out of memory, CPU exhaustion, etc.),
// we can force the new executor to take over by manually setting the status in
// __all_shared_gc_control to IDLE and performing a leader switch to move the gc sswriter
// to another server. Meanwhile, the old executor periodically checks whether executor_server_id in
// __all_shared_gc_control matches self; if not, it will cancel the dag. The periodic
// check is implemented in check_executor_and_cancel_dag_if_not_self_ and is throttled to
// CHECK_EXECUTOR_INTERVAL_US (5 minutes).
// TODO(binifei.bnf): test this feature.
//
// 3. The __all_shared_gc_control row is inserted by ObTenantDDLService::create_sys_tenant
// (for SYS) and ObTenantDDLService::create_tenant_end (for meta and user) when the tenant is
// created. This task no longer lazily inserts the row; if the row is missing it logs WARN and
// waits until tenant create finishes.

int ObSSMarkSweepGCGenTask::check_if_need_do_gc_work(bool &is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  ObCurTraceId::init(GCONF.self_addr_);
  bool enable_gc_service = false;
  bool is_dag_exist = false;
  bool is_gc_sswriter = false;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::check_enable_gc_service(tenant_id_, enable_gc_service))) {
    LOG_WARN("fail to check enable gc service", KR(ret), K_(tenant_id));
  } else if (!enable_gc_service) {
    // gc service disabled: cancel the running dag if any; otherwise release leftover
    // executor ownership (DOING -> IDLE). A still-running dag will reset status itself
    // in report_result(), so do NOT reset here.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_dag_exist_(is_dag_exist))) {
      LOG_WARN("fail to check mark sweep dag exist", KR(tmp_ret), K_(tenant_id));
    } else if (is_dag_exist) {
      // Recheck ownership after the DAG has exited, even if cancellation fails.
      gc_disabled_release_checked_ = false;
      if (OB_TMP_FAIL(cancel_dag_())) {
        LOG_WARN("fail to cancel mark sweep dag when gc service disabled", KR(tmp_ret), K_(tenant_id));
      }
    } else if (!gc_disabled_release_checked_) {
      // A failed release does not consume the check: the next scheduled round retries
      // immediately, without sleeping on the timer thread.
      if (OB_TMP_FAIL(try_release_executor_())) {
        LOG_WARN("fail to release executor when gc service disabled, will retry",
            KR(tmp_ret), K_(tenant_id));
      } else {
        gc_disabled_release_checked_ = true;
      }
    }
  } else if (OB_FAIL(check_dag_exist_(is_dag_exist))) {
    LOG_WARN("fail to check mark sweep dag exist", KR(ret), K_(tenant_id));
  } else if (is_dag_exist) {
    const int64_t current_time_us = ObTimeUtility::current_time_us();
    if ((current_time_us - last_check_executor_time_us_) >= CHECK_EXECUTOR_INTERVAL_US) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_executor_and_cancel_dag_if_not_self_())) {
        LOG_WARN("fail to check executor and cancel dag if not self",
            KR(tmp_ret), K_(tenant_id));
      }
      last_check_executor_time_us_ = current_time_us;
    } else {
      LOG_TRACE("mark sweep dag already exists, skip this round", K_(tenant_id));
    }
  } else if (OB_FAIL(check_is_gc_sswriter_(is_gc_sswriter))) {
    LOG_WARN("fail to check is gc sswriter", KR(ret), K_(tenant_id));
  } else if (!is_gc_sswriter) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_release_executor_())) {
      LOG_WARN("fail to try release executor", KR(tmp_ret), K_(tenant_id));
    }
  } else { // is_gc_sswriter
    const bool need_retry = need_retry_mark_sweep_dag();
    const int64_t current_time_us = ObTimeUtility::current_time_us();
    int64_t exec_interval_us = DEFAULT_GC_EXEC_INTERVAL_US;
    get_gc_exec_interval_us_(exec_interval_us);
    if (((current_time_us - last_exec_time_us_) >= exec_interval_us) || need_retry) {
      bool is_acquired_or_takeovered = false;
      if (OB_FAIL(try_acquire_or_takeover_executor_(is_acquired_or_takeovered))) {
        LOG_WARN("fail to acquire or takeover executor", KR(ret), K_(tenant_id));
      } else if (is_acquired_or_takeovered) {
        is_need = true;
        last_exec_time_us_ = current_time_us;
        // If the dag executes successfully (including the step that sets the need_retry flag),
        // the need_retry flag will be set to false.
        // However, if the dag fails (in various abnormal scenarios), it cannot be guaranteed
        // that the need_retry flag will be set to true.
        // Therefore, set need_retry to true at the beginning. On success, it can be set to false;
        // otherwise, need_retry remains true, so as to ensure retry.
        set_need_retry_mark_sweep_dag(true/*need_retry*/);
      }
    }
  }
  if (enable_gc_service) {
    // A subsequent disabled interval must check any newly acquired ownership.
    gc_disabled_release_checked_ = false;
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::get_all_ls_id_(
    const SCN &snapshot_meta_scn,
    ObIArray<ObLSID> &ls_id_arr)
{
  int ret = OB_SUCCESS;
  ls_id_arr.reuse();
  ObSSMetaIterGuard<ObSSMetaLSIDIter> ls_id_iter_guard;
  ObSSMetaLSIDIter *ls_id_iter = nullptr;
  ObSSMetaService *meta_srv = nullptr;
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(meta_srv->get_ls_id_iter(snapshot_meta_scn, ls_id_iter_guard))) {
    LOG_WARN("fail to get ls id iter", KR(ret), K_(tenant_id), K(snapshot_meta_scn));
  } else if (OB_FAIL(ls_id_iter_guard.get_iter(ls_id_iter))) {
    LOG_WARN("fail to get ls iter", KR(ret), K_(tenant_id));
  } else {
    ObLSID ls_id;
    bool is_ls_iter_end = false;
    while (OB_SUCC(ret) && !is_ls_iter_end) {
      ls_id.reset();
      if (OB_FAIL(ls_id_iter->get_next(ls_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_ls_iter_end = true;
        } else {
          LOG_WARN("fail to iter ls id", KR(ret), K_(tenant_id), K(snapshot_meta_scn));
        }
      } else if (OB_FAIL(ls_id_arr.push_back(ls_id))) {
        LOG_WARN("fail to push back ls id", KR(ret), K_(tenant_id), K(ls_id));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::get_one_ls_all_meta_scn_(
    const ObLSID &ls_id,
    const SCN &snapshot_meta_scn,
    ObIArray<SCN> &ls_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  ls_meta_scn_arr.reuse();
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id,
      share::SCN::min_scn(),
      true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  ObSSMetaIterGuard<ObSSMetaLSSCNIterator> ls_scn_iter_guard;
  ObSSMetaLSSCNIterator *ls_scn_iter = nullptr;
  ObSSMetaService *meta_srv = nullptr;
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(meta_srv->get_ls_scn_iter(read_param, snapshot_meta_scn, ls_scn_iter_guard))) {
    LOG_WARN("fail to get ls scn iter", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
  } else if (OB_FAIL(ls_scn_iter_guard.get_iter(ls_scn_iter))) {
    LOG_WARN("fail to get ls scn iter", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
  } else {
    SCN ls_meta_scn;
    bool is_ls_meta_scn_iter_end = false;
    while (OB_SUCC(ret) && !is_ls_meta_scn_iter_end) {
      ls_meta_scn.reset();
      if (OB_FAIL(ls_scn_iter->get_next(ls_meta_scn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_ls_meta_scn_iter_end = true;
        } else {
          LOG_WARN("fail to iter ls meta scn", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
        }
      } else if (OB_FAIL(ls_meta_scn_arr.push_back(ls_meta_scn))) {
        LOG_WARN("fail to push back ls meta scn", KR(ret), K_(tenant_id), K(ls_id), K(ls_meta_scn));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::get_all_ls_ss_checkpoint_scn_(
    const SCN &snapshot_meta_scn,
    const SCN &safe_meta_scn,
    const ObIArray<SCN> &reserved_meta_scns,
    const SCN &ckpt_ss_checkpoint_scn,
    ObHashMap<ObLSID, SCN> &ls_ss_checkpoint_scn_map,
    ObIArray<ObLSID> &recyclable_deleted_ls_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSID, 16> ls_id_arr;
  ls_id_arr.set_attr(ObMemAttr(tenant_id_, "SSGCLSIDArr"));
  ObSSMetaService *meta_srv = nullptr;
  recyclable_deleted_ls_ids.reuse();
  if (OB_FAIL(ls_ss_checkpoint_scn_map.reuse())) {
    LOG_WARN("fail to reuse ls ss checkpoint scn map", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_all_ls_id_(snapshot_meta_scn, ls_id_arr))) {
    LOG_WARN("fail to get all ls id", KR(ret), K_(tenant_id), K(snapshot_meta_scn));
  } else {
    ObSEArray<SCN, 16> all_meta_scns;
    all_meta_scns.set_attr(ObMemAttr(tenant_id_, "SSGCLSMetaSCN"));
    for (int64_t ls_idx = 0; OB_SUCC(ret) && (ls_idx < ls_id_arr.count()); ++ls_idx) {
      const ObLSID &ls_id = ls_id_arr.at(ls_idx);
      // 1. get all ls meta scn
      all_meta_scns.reuse();
      if (OB_FAIL(get_one_ls_all_meta_scn_(ls_id, snapshot_meta_scn, all_meta_scns))) {
        LOG_WARN("fail to get one ls all meta scn", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else if (OB_UNLIKELY(all_meta_scns.empty())) {
        ret = OB_EAGAIN;
        LOG_WARN("ls meta scn array is empty, may be concurrently gc'd by other executor",
            KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else {
        // 2. compute min_meta_scn.
        const int64_t all_meta_scn_cnt = all_meta_scns.count();
        SCN min_reserved_meta_scn;
        SCN real_min_reserved_meta_scn;
        const SCN min_meta_scn = ObSSGCUtil::calc_min_meta_scn(
            reserved_meta_scns, all_meta_scns, safe_meta_scn, SCN::max_scn()/*studied_meta_scn*/,
            min_reserved_meta_scn, real_min_reserved_meta_scn);

        // 3. find the minimum meta_scn >= min_meta_scn. if not found, use the last meta_scn.
        bool found = false;
        for (int64_t i = 0; OB_SUCC(ret) && (i < all_meta_scn_cnt) && !found; ++i) {
          const SCN &cur_meta_scn = all_meta_scns.at(i);
          if ((cur_meta_scn >= min_meta_scn) || (i == (all_meta_scn_cnt - 1))) {
            found = true;
            ObSSLSMeta ls_meta;
            if (OB_FAIL(meta_srv->get_ls_meta(ls_id, cur_meta_scn, ls_meta))) {
              LOG_WARN("fail to get ls meta", KR(ret), K(ls_id), K(cur_meta_scn));
            } else {
              // clog_data_scn(L_clog)：本轮选中的 LS_META 行内 replay 位点，CLOG 回放会持续推进。
              // ckpt_ss_checkpoint_scn(L_ckpt)：已发布 checkpoint 对象内嵌 ls_meta 的快照位点。
              // 二者同属 SYS_LS 数据域但时间点不同（checkpoint 发布后 CLOG 仍可能前进；
              // 或选中的 LS_META 版本不是构建快照那一版），因此可能不等，取 MIN 收紧 GC 下界。
              const SCN clog_data_scn = ls_meta.get_ss_checkpoint_scn();
              const SCN effective_data_scn =
                  is_sys_tenant(tenant_id_) && SYS_LS == ls_id
                      && ckpt_ss_checkpoint_scn.is_valid()
                      ? MIN(clog_data_scn, ckpt_ss_checkpoint_scn)
                      : clog_data_scn;
              if (OB_FAIL(ls_ss_checkpoint_scn_map.set_refactored(ls_id, effective_data_scn))) {
                LOG_WARN("fail to set ls ss checkpoint scn",
                    KR(ret), K(ls_id), K(cur_meta_scn), K(clog_data_scn),
                    K(ckpt_ss_checkpoint_scn), K(effective_data_scn));
              } else if (i == all_meta_scn_cnt - 1 && cur_meta_scn <= min_meta_scn
                         && effective_data_scn == clog_data_scn) {
                // Read the delete mark from this round's selected version to preserve snapshot consistency.
                ObSSMetaReadParamV2 read_param;
                read_param.set_ls_level_param(
                    ObSSMetaReadParamType::LS_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW,
                    ObAtomicProtocolTypeEnum::LS_META, ls_id, cur_meta_scn, true /*is_in_table*/);
                read_param.set_read_mark_delete(true);
                SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
                  if (OB_FAIL(ObSSMetaTableHelper::get_ls(read_param, ls_value))) {
                    LOG_WARN("fail to read final ls meta version", KR(ret), K(ls_id), K(cur_meta_scn));
                  } else if (ls_value.get_meta_head_info().is_mark_deleted()
                             && OB_FAIL(recyclable_deleted_ls_ids.push_back(ls_id))) {
                    LOG_WARN("fail to append recyclable deleted ls", KR(ret), K(ls_id), K(cur_meta_scn));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// Here we fetch all LS at a snapshot meta scn. LS created after this snapshot have a larger
// meta_scn and do not need to participate in this round of gc. This round of gc only gc data
// before this snapshot meta scn, without affecting LS with larger meta_scn.
// In the transfer scenario, it is also guaranteed that the src can only be mark deleted after the
// dst has finished referencing. If gc's snapshot_meta_scn sees a tablet that was mark_deleted
// on the src, it must also see the tablet referenced on the dst side, ensuring no wrong GC.
int ObSSMarkSweepGCGenTask::study_tablet_meta_scn_(
    const SCN &snapshot_meta_scn,
    const ObHashMap<ObLSID, SCN> &ls_ss_checkpoint_scn_map,
    ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr,
    ObIArray<ObSSGCMarkDeletedTablet> &mark_deleted_tablets)
{
  int ret = OB_SUCCESS;
  // Fallback cleanup of stale "deleted ls with undeleted tablet" records. (only runs every 2 days)
  // best-effort so failures do not block study.
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  if (OB_NOT_NULL(gc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(gc_service->cleanup_stale_del_ls_with_undel_tablet())) {
      LOG_WARN("fail to cleanup stale del ls with undel tablet records", KR(tmp_ret), K_(tenant_id));
    }
  }
  ObSEArray<ObLSID, 16> ls_id_arr;
  ls_id_arr.set_attr(ObMemAttr(tenant_id_, "SSGCLSIDArr"));
  if (OB_FAIL(get_all_ls_id_(snapshot_meta_scn, ls_id_arr))) {
    LOG_WARN("fail to get all ls id", KR(ret), K_(tenant_id), K(snapshot_meta_scn));
  } else {
    const ObMemAttr mem_attr(tenant_id_, "SSGCStudyArr");
    ObSSGCTabletWithDelScnArray cur_ls_gc_tablets;
    cur_ls_gc_tablets.set_attr(mem_attr);
    ObSSGCTabletWithMetaScnArray cur_ls_tablet_with_meta_scn_arr;
    cur_ls_tablet_with_meta_scn_arr.set_attr(mem_attr);
    for (int64_t ls_idx = 0; OB_SUCC(ret) && (ls_idx < ls_id_arr.count()); ++ls_idx) {
      const ObLSID &ls_id = ls_id_arr.at(ls_idx);
      SCN ls_ss_checkpoint_scn;
      cur_ls_gc_tablets.reuse();
      cur_ls_tablet_with_meta_scn_arr.reuse();
      // map 是本轮冻结快照，每个 LS 只在这里提取一次，后续 filter 与 DAG 共用同一位点。
      if (OB_FAIL(ls_ss_checkpoint_scn_map.get_refactored(ls_id, ls_ss_checkpoint_scn))
          && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get effective ls ss checkpoint scn",
            KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("effective ls ss checkpoint scn is missing, retry current round",
            KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else if (OB_UNLIKELY(!ls_ss_checkpoint_scn.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid effective ls ss checkpoint scn",
            KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn), K(ls_ss_checkpoint_scn));
      } else if (OB_FAIL(study_tablet_meta_scn_for_ls_(ls_id, snapshot_meta_scn,
              ls_ss_checkpoint_scn, cur_ls_gc_tablets, cur_ls_tablet_with_meta_scn_arr,
              mark_deleted_tablets))) {
        LOG_WARN("fail to study tablet meta scn for ls", KR(ret), K_(tenant_id), K(ls_id),
            K(snapshot_meta_scn));
      } else if (OB_FAIL(append(tablet_with_meta_scn_arr, cur_ls_tablet_with_meta_scn_arr))) {
        LOG_WARN("fail to append tablet with meta scn", KR(ret), K_(tenant_id), K(ls_id),
            K(snapshot_meta_scn));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::study_tablet_meta_scn_for_ls_(
    const ObLSID &ls_id,
    const SCN &snapshot_meta_scn,
    const SCN &ls_ss_checkpoint_scn,
    ObSSGCTabletWithDelScnArray &gc_tablets,
    ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr,
    ObIArray<ObSSGCMarkDeletedTablet> &mark_deleted_tablets)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_srv = nullptr;
  ObSSMetaIterGuard<ObSSMetaTabletTinyInfoIterator> tablet_iter_guard;
  ObSSMetaTabletTinyInfoIterator *tablet_iter = nullptr;
  ObSSGCTabletStudyResultArray study_result_arr;
  study_result_arr.set_attr(ObMemAttr(tenant_id_, "SSGCStudyRes"));
  ObSSMetaReadParamV2 ls_param;
  ls_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_ONLY_KEY, ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, SCN::min_scn() /*meta_scn*/, true /*is_in_table*/);
  ls_param.set_read_mark_delete(true);
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id), K(ls_id));
  } else if (OB_FAIL(meta_srv->get_tablet_tiny_info_iter(ls_param, snapshot_meta_scn, tablet_iter_guard))) {
    LOG_WARN("fail to get tablet tiny info iter", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
  } else if (OB_FAIL(tablet_iter_guard.get_iter(tablet_iter))) {
    LOG_WARN("fail to get tablet tiny info iter ptr", KR(ret), K_(tenant_id), K(ls_id));
  } else {
    ObSSMetaTabletKey key;
    ObSSMetaTabletTinyInfo tiny_info;
    bool is_tablet_iter_end = false;
    while (OB_SUCC(ret) && !is_tablet_iter_end) {
      key.reset();
      tiny_info.reset();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id), K(ls_id));
      } else if (OB_FAIL(tablet_iter->get_next(key, tiny_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_tablet_iter_end = true;
        } else {
          LOG_WARN("fail to iter tablet tiny info", KR(ret), K_(tenant_id), K(ls_id));
        }
      } else if (OB_UNLIKELY(!tiny_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tiny info from tablet iterator", KR(ret), K_(tenant_id), K(ls_id), K(key), K(tiny_info));
      } else if (OB_FAIL(gc_tablets.push_back(ObSSGCTabletWithDelScn(key.ls_id_, key.tablet_id_,
              key.reorganization_scn_, tiny_info.deleted_scn_)))) {
        LOG_WARN("fail to push back gc tablet", KR(ret), K_(tenant_id), K(ls_id), K(key));
      } else if (tiny_info.is_mark_delete_) {
        // key.meta_scn_ 为 <= snapshot_meta_scn 的最新版本 meta_scn；mark-delete 后 tablet
        // 不会再持久化新版本，因此该值即为 mark-delete 版本对应的 meta_scn。
        if (OB_FAIL(mark_deleted_tablets.push_back(ObSSGCMarkDeletedTablet(
            ObSSGCTablet(key.ls_id_, key.tablet_id_, key.reorganization_scn_), key.meta_scn_)))) {
          LOG_WARN("fail to push back mark_deleted tablet", KR(ret), K_(tenant_id), K(ls_id), K(key));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (gc_tablets.empty()) {
      FLOG_INFO("all tablets are already gc, but ls_meta is not gc yet", K_(tenant_id),
          K(ls_id), K(snapshot_meta_scn));
    } else if (OB_FAIL(ObSSGCStudyUtil::do_mark_sweep_gc_study(
            ls_id, gc_tablets, study_result_arr, DEFAULT_STUDY_TIMEOUT_US))) {
      LOG_WARN("fail to do mark sweep gc study", KR(ret), K_(tenant_id),
          K(ls_id), K(snapshot_meta_scn), K(gc_tablets.count()));
    } else if (OB_FAIL(calc_min_meta_scn_(snapshot_meta_scn, study_result_arr, tablet_with_meta_scn_arr))) {
      LOG_WARN("fail to calc min meta scn", KR(ret), K_(tenant_id), K(ls_id),
          K(snapshot_meta_scn));
    } else if (OB_FAIL(filter_by_ls_ss_checkpoint_scn_(
                   ls_id, snapshot_meta_scn, ls_ss_checkpoint_scn,
                   gc_tablets, tablet_with_meta_scn_arr))) {
      LOG_WARN("fail to filter by ls ss checkpoint scn", KR(ret), K_(tenant_id),
          K(ls_id), K(snapshot_meta_scn), K(gc_tablets.count()));
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::calc_min_meta_scn_(
    const SCN &snapshot_meta_scn,
    const ObSSGCTabletStudyResultArray &study_result_arr,
    ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < study_result_arr.count()); ++i) {
    const ObSSGCTabletStudyResult &study_result = study_result_arr.at(i);
    // meta_scn = min(min_dep_meta_scn_, the max meta_scn among versions whose
    // max_major_snapshot_ does not exceed study_result.min_major_snapshot_).
    // That version is the last usable bound before the still-needed min major appears.
    // If no such version exists, major side cannot recycle any version.
    // Optimize: min_major_snapshot_ == INT64_MAX means major side can recycle all
    // versions (no major constraint), so no need to walk versions; just use min_dep_meta_scn_.
    SCN meta_scn = study_result.study_info_.min_dep_meta_scn_;
    if (INT64_MAX == study_result.study_info_.min_major_snapshot_) {
      // no major constraint, keep meta_scn = min_minor_meta_scn_
    } else {
      SCN max_major_meta_scn;
      if (OB_FAIL(get_max_major_meta_scn_(snapshot_meta_scn, study_result.gc_tablet_,
              study_result.study_info_.min_major_snapshot_, max_major_meta_scn))) {
        LOG_WARN("fail to get max major meta scn", KR(ret), K_(tenant_id), K(study_result));
      } else if (max_major_meta_scn.is_valid()) {
        meta_scn = max_major_meta_scn < meta_scn ? max_major_meta_scn : meta_scn;
      } else {
        meta_scn.set_min();
        LOG_TRACE("no major meta scn found, set min scn", K_(tenant_id), K(study_result));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_with_meta_scn_arr.push_back(ObSSGCTabletWithMetaScn(
            study_result.gc_tablet_, meta_scn)))) {
      LOG_WARN("fail to push back tablet with meta scn", KR(ret), K_(tenant_id), K(study_result));
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::get_max_major_meta_scn_(
    const SCN &snapshot_meta_scn,
    const ObSSGCTablet &gc_tablet,
    const int64_t min_major_snapshot,
    SCN &max_major_meta_scn)
{
  int ret = OB_SUCCESS;
  max_major_meta_scn.reset();
  ObSSMetaService *meta_srv = nullptr;
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id), K(gc_tablet));
  } else {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        gc_tablet.ls_id_,
        gc_tablet.tablet_id_,
        SCN::min_scn()/*meta_scn*/,
        gc_tablet.transfer_scn_,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);

    ObSSMetaIterGuard<ObSSMetaTabletTinyInfoIterator> iter_guard;
    ObSSMetaTabletTinyInfoIterator *iter = nullptr;
    if (OB_FAIL(meta_srv->get_tablet_tiny_info_iter(read_param, snapshot_meta_scn, iter_guard))) {
      LOG_WARN("fail to get tablet tiny info iter", KR(ret), K_(tenant_id), K(gc_tablet));
    } else if (OB_FAIL(iter_guard.get_iter(iter))) {
      LOG_WARN("fail to get iter", KR(ret), K_(tenant_id), K(gc_tablet));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", KR(ret), K_(tenant_id), K(gc_tablet));
    } else {
      ObSSMetaTabletKey key;
      ObSSMetaTabletTinyInfo tiny_info;
      bool is_iter_end = false;
      while (OB_SUCC(ret) && !is_iter_end) {
        key.reset();
        tiny_info.reset();
        if (OB_FAIL(check_init_and_stop_())) {
          LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id), K(gc_tablet));
        } else if (OB_FAIL(iter->get_next(key, tiny_info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("fail to get next version tiny info", KR(ret), K_(tenant_id), K(gc_tablet));
          }
        } else if (tiny_info.max_major_snapshot_ <= min_major_snapshot) {
          // 收集 max_major_snapshot 不超过目标 min_major_snapshot 的版本中的最大 meta_scn。
          if (!max_major_meta_scn.is_valid() || (max_major_meta_scn < key.meta_scn_)) {
            max_major_meta_scn = key.meta_scn_;
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::filter_by_ls_ss_checkpoint_scn_(
    const ObLSID &ls_id,
    const SCN &snapshot_meta_scn,
    const SCN &ls_ss_checkpoint_scn,
    const ObSSGCTabletWithDelScnArray &gc_tablets,
    ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  // 该位点来自本轮冻结 map，不能再次读取 live LS_META，
  // 避免 filter 与后续 Sort 看到不同 checkpoint。
  if (OB_UNLIKELY(!ls_ss_checkpoint_scn.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid effective ls ss checkpoint scn",
        KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn), K(ls_ss_checkpoint_scn));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < gc_tablets.count()); ++i) {
      const ObSSGCTabletWithDelScn &gc_tablet = gc_tablets.at(i);
      if (gc_tablet.deleted_scn_.is_valid()
          && ls_ss_checkpoint_scn < gc_tablet.deleted_scn_) {
        if (OB_LIKELY(tablet_with_meta_scn_arr.at(i).gc_tablet_ == gc_tablet.gc_tablet_)) {
          const SCN ori_meta_scn = tablet_with_meta_scn_arr.at(i).meta_scn_;
          tablet_with_meta_scn_arr.at(i).meta_scn_ = SCN::min_scn();
          LOG_TRACE("ss checkpoint scn has not advanced, cannot gc", K_(tenant_id), K(ls_id),
              K(gc_tablet), K(ls_ss_checkpoint_scn), K(ori_meta_scn));
          ObSSGCUtil::report_mark_sweep_set_to_min_event(gc_tablet.gc_tablet_,
                                                         "LS_SS_CHECKPOINT_SCN_BEHIND");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("gc tablet not match, unexpected", KR(ret), K_(tenant_id), K(ls_id),
              K(gc_tablet), "tablet", tablet_with_meta_scn_arr.at(i).gc_tablet_);
        }
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::handle_ckpt_tablet_not_in_study_(
    const SCN &snapshot_meta_scn,
    const ObSSGCTablet &ckpt_tablet,
    const SCN &ckpt_tablet_meta_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_meta_scn.is_valid()
                  || !ckpt_tablet.is_valid()
                  || !ckpt_tablet_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for handling checkpoint tablet missing from study",
        KR(ret), K_(tenant_id), K(snapshot_meta_scn),
        K(ckpt_tablet), K(ckpt_tablet_meta_scn));
  } else if (ckpt_tablet_meta_scn > snapshot_meta_scn) {
    // 该 tablet 在本轮 MarkSweep 快照之后产生，本轮不会扫描到其元数据版本。
    LOG_INFO("checkpoint tablet is newer than mark sweep snapshot, skip this round",
        K_(tenant_id), K(snapshot_meta_scn),
        K(ckpt_tablet), K(ckpt_tablet_meta_scn));
  } else {
    // checkpoint 位点已进入本轮快照却没有 study 结果，不能在缺少运行态依赖位点时继续回收。
    ret = OB_EAGAIN;
    LOG_WARN("checkpoint tablet is in mark sweep snapshot but missing from study result",
        KR(ret), K_(tenant_id), K(snapshot_meta_scn),
        K(ckpt_tablet), K(ckpt_tablet_meta_scn));
    SERVER_EVENT_ADD("SHARED_GC", "CKPT_TABLET_NOT_IN_STUDY",
        "tenant_id", tenant_id_,
        "tablet", ckpt_tablet,
        "snapshot_meta_scn", snapshot_meta_scn,
        "ckpt_tablet_meta_scn", ckpt_tablet_meta_scn,
        "ret", ret);
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::check_ls_ckpt_tablet_meta_keys_count_(
    const ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys)
{
  int ret = OB_SUCCESS;
  const int64_t max_tablet_meta_keys_cnt = ObBucketNumHelper::get_bucket_num_by_mtl_mem(
      ObGetBucketNumType::TABLET_TYPE);
  if (OB_UNLIKELY(tablet_meta_keys.count() > max_tablet_meta_keys_cnt)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ls checkpoint tablet meta keys count exceeds tenant tablet scale limit",
        KR(ret), K_(tenant_id), K(SYS_LS),
        "tablet_count", tablet_meta_keys.count(), K(max_tablet_meta_keys_cnt));
    SERVER_EVENT_ADD("SHARED_GC", "CKPT_TABLET_META_KEYS_OVERFLOW",
        "tenant_id", tenant_id_, "ls_id", SYS_LS.id(),
        "tablet_count", tablet_meta_keys.count(),
        "max_tablet_meta_keys_cnt", max_tablet_meta_keys_cnt, "ret", ret);
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::read_ls_ckpt_data_(
    const SCN &ckpt_meta_scn,
    ObSSLSMeta &ls_meta,
    ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys)
{
  int ret = OB_SUCCESS;
  ls_meta.reset();
  tablet_meta_keys.reset();

  if (OB_UNLIKELY(!ckpt_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls checkpoint meta scn", KR(ret), K_(tenant_id), K(ckpt_meta_scn));
  } else {
    ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
    // 接口内部必须校验反序列化得到的 ckpt_scn_ 与入参一致，
    // 调用方只拿到裁剪后的 LS_META 和 Tablet 引用，无法再次校验。
    if (OB_ISNULL(meta_srv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(meta_srv->read_ls_ckpt_data(
                   SYS_LS, ckpt_meta_scn, ls_meta, tablet_meta_keys))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("fail to read ls checkpoint data", KR(ret), K_(tenant_id),
            K(ckpt_meta_scn));
      }
    } else if (OB_FAIL(check_ls_ckpt_tablet_meta_keys_count_(tablet_meta_keys))) {
      LOG_WARN("invalid ls checkpoint tablet meta keys count after read",
          KR(ret), K_(tenant_id), K(ckpt_meta_scn),
          "tablet_count", tablet_meta_keys.count());
    }
  }
  if (OB_FAIL(ret)) {
    ls_meta.reset();
    tablet_meta_keys.reset();
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::read_ls_ckpt_with_retry_(
    SCN &ss_checkpoint_scn,
    ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys,
    SCN &ckpt_meta_scn,
    bool &has_ls_ckpt)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_CKPT_READ_RETRY_TIMES = 3;
  const int64_t CKPT_READ_RETRY_INTERVAL_US = 100 * 1000L; // 100ms
  bool has_seen_ckpt = false;
  bool need_retry = true;
  SCN last_ckpt_meta_scn;
  ckpt_meta_scn.reset();
  ss_checkpoint_scn.reset();
  tablet_meta_keys.reset();
  has_ls_ckpt = false;

  // PALF 位点更新后，Checkpoint GC 可能并发删除上一代对象。对象不存在时重新读取
  // 最新位点，保证成功返回的对象与反序列化内容属于同一代 checkpoint。
  for (int64_t retry_count = 0;
       OB_SUCC(ret) && need_retry && (retry_count <= MAX_CKPT_READ_RETRY_TIMES);
       ++retry_count) {
    need_retry = false;
    SCN candidate_ckpt_meta_scn;
    ObSSLSMeta ls_meta;
    ObSEArray<blocksstable::MacroBlockId, 64> candidate_tablet_meta_keys;
    candidate_tablet_meta_keys.set_attr(ObMemAttr(tenant_id_, "CkptMetaKeys"));
    if (OB_FAIL(check_init_and_stop_())) {
      LOG_WARN("fail to check task state before reading ls checkpoint",
          KR(ret), K_(tenant_id), K(retry_count));
    } else if (OB_FAIL(ObSSGCUtil::get_ls_ckpt_meta_scn_from_palf_kv(
                   tenant_id_, SYS_LS, candidate_ckpt_meta_scn))) {
      if (OB_ENTRY_NOT_EXIST == ret && !has_seen_ckpt) {
        // 尚未发布 checkpoint，成功返回并通过 has_ls_ckpt 告知调用方跳过回收。
        ret = OB_SUCCESS;
        LOG_INFO("ls checkpoint has not been published",
            K_(tenant_id), K(SYS_LS));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("published ls checkpoint disappeared while retrying",
            KR(ret), K_(tenant_id), K(SYS_LS), K(retry_count), K(last_ckpt_meta_scn));
      } else {
        LOG_WARN("fail to get ls checkpoint meta scn from palf kv",
            KR(ret), K_(tenant_id), K(SYS_LS), K(retry_count), K(has_seen_ckpt));
      }
    } else if (OB_UNLIKELY(has_seen_ckpt
                           && (candidate_ckpt_meta_scn < last_ckpt_meta_scn))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ls checkpoint meta scn moved backward while retrying",
          KR(ret), K_(tenant_id), K(SYS_LS), K(candidate_ckpt_meta_scn),
          K(last_ckpt_meta_scn), K(retry_count));
    } else {
      has_seen_ckpt = true;
      last_ckpt_meta_scn = candidate_ckpt_meta_scn;
      if (OB_FAIL(read_ls_ckpt_data_(
              candidate_ckpt_meta_scn, ls_meta, candidate_tablet_meta_keys))) {
        if (OB_OBJECT_NOT_EXIST == ret && retry_count < MAX_CKPT_READ_RETRY_TIMES) {
          LOG_INFO("ls checkpoint object changed concurrently, retry with latest palf scn",
              KR(ret), K_(tenant_id), K(SYS_LS), K(candidate_ckpt_meta_scn), K(retry_count));
          // 重读 PALF 是为了追随可能已切换的新一代 checkpoint；
          // 短暂退避可避免在发布可见窗口内连续空转耗尽重试次数。
          ob_usleep(CKPT_READ_RETRY_INTERVAL_US);
          ret = OB_SUCCESS;
          need_retry = true;
        } else if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_EAGAIN;
          LOG_WARN("ls checkpoint object is still missing after retry",
              KR(ret), K_(tenant_id), K(SYS_LS), K(candidate_ckpt_meta_scn), K(retry_count));
          SERVER_EVENT_ADD("SHARED_GC", "CKPT_OBJECT_READ_RETRY_EXHAUSTED",
              "tenant_id", tenant_id_, "ls_id", SYS_LS.id(),
              "ckpt_meta_scn", candidate_ckpt_meta_scn, "ret", ret);
        } else {
          LOG_WARN("fail to read ls checkpoint data",
              KR(ret), K_(tenant_id), K(SYS_LS), K(candidate_ckpt_meta_scn), K(retry_count));
        }
      } else {
        // 对象定位、反序列化及 checkpoint 内容校验由 MetaService 统一负责。
        const SCN candidate_ss_checkpoint_scn = ls_meta.get_ss_checkpoint_scn();
        if (OB_UNLIKELY(!candidate_ss_checkpoint_scn.is_valid())) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid ls data checkpoint scn in ls checkpoint object",
              KR(ret), K_(tenant_id), K(candidate_ckpt_meta_scn), K(ls_meta));
        } else if (OB_FAIL(tablet_meta_keys.assign(candidate_tablet_meta_keys))) {
          LOG_WARN("fail to copy tablet meta keys from ls checkpoint",
              KR(ret), K_(tenant_id), K(candidate_ckpt_meta_scn), K(ls_meta),
              "tablet_count", candidate_tablet_meta_keys.count());
        } else {
          ckpt_meta_scn = candidate_ckpt_meta_scn;
          ss_checkpoint_scn = candidate_ss_checkpoint_scn;
          has_ls_ckpt = true;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ckpt_meta_scn.reset();
    ss_checkpoint_scn.reset();
    tablet_meta_keys.reset();
    has_ls_ckpt = false;
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::merge_tablet_meta_scns_from_ckpt_(
    const SCN &snapshot_meta_scn,
    const ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys,
    ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr)
{
  int ret = OB_SUCCESS;
  int64_t matched_count = 0;
  int64_t updated_count = 0;

  if (tablet_meta_keys.empty()) {
    LOG_INFO("ls checkpoint contains no tablet meta scn",
        K_(tenant_id),
        "study_tablet_count", tablet_with_meta_scn_arr.count(),
        "checkpoint_tablet_count", tablet_meta_keys.count());
  } else if (OB_FAIL(check_ls_ckpt_tablet_meta_keys_count_(tablet_meta_keys))) {
    LOG_WARN("reject merge due to oversized ls checkpoint tablet meta keys",
        KR(ret), K_(tenant_id), K(snapshot_meta_scn),
        "tablet_count", tablet_meta_keys.count());
  } else {
    const int64_t bucket_num = ObBucketNumHelper::get_bucket_num_by_mtl_mem(
        ObGetBucketNumType::TABLET_TYPE);
    ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
    bool tablet_index_map_inited = false;
    if (OB_FAIL(tablet_index_map.create(
            bucket_num, ObMemAttr(tenant_id_, "CkptTblIdx")))) {
      LOG_WARN("fail to create checkpoint tablet index map",
          KR(ret), K_(tenant_id), K(bucket_num));
    } else {
      tablet_index_map_inited = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_with_meta_scn_arr.count(); ++i) {
      const ObSSGCTabletWithMetaScn &tablet = tablet_with_meta_scn_arr.at(i);
      if (OB_FAIL(tablet_index_map.set_refactored(tablet.gc_tablet_, i))) {
        LOG_WARN("fail to add study tablet into checkpoint tablet index map",
            KR(ret), K_(tenant_id), K(i), K(tablet));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_keys.count(); ++i) {
      const blocksstable::MacroBlockId &tablet_meta_key = tablet_meta_keys.at(i);
      ObSSMetaTabletKey ckpt_tablet_key;
      // LS checkpoint 只会记录对象存储中的 tablet meta 地址，拒绝接受内部表地址。
      if (OB_UNLIKELY(!tablet_meta_key.is_in_object_storage())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("tablet meta key in ls checkpoint is not in object storage",
            KR(ret), K_(tenant_id), K(i), K(tablet_meta_key));
      } else if (OB_FAIL(ckpt_tablet_key.init_from_macro_id(tablet_meta_key, SYS_LS))) {
        LOG_WARN("fail to parse tablet meta key in ls checkpoint",
            KR(ret), K_(tenant_id), K(i), K(tablet_meta_key));
      } else {
        const ObSSGCTablet ckpt_tablet(
            ckpt_tablet_key.ls_id_, ckpt_tablet_key.tablet_id_,
            ckpt_tablet_key.reorganization_scn_);
        const SCN &ckpt_tablet_meta_scn = ckpt_tablet_key.meta_scn_;
        int64_t tablet_idx = -1;
        if (OB_FAIL(tablet_index_map.get_refactored(ckpt_tablet, tablet_idx))
            && OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to find checkpoint tablet in study result",
              KR(ret), K_(tenant_id), K(ckpt_tablet), K(tablet_meta_key));
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(handle_ckpt_tablet_not_in_study_(
                  snapshot_meta_scn, ckpt_tablet, ckpt_tablet_meta_scn))) {
            LOG_WARN("fail to handle checkpoint tablet missing from study result",
                KR(ret), K_(tenant_id), K(snapshot_meta_scn),
                K(ckpt_tablet), K(ckpt_tablet_meta_scn), K(tablet_meta_key));
          }
        } else if (OB_UNLIKELY((tablet_idx < 0)
                               || (tablet_idx >= tablet_with_meta_scn_arr.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid study tablet index", KR(ret), K_(tenant_id),
              K(tablet_idx), K(ckpt_tablet), K(tablet_meta_key));
        } else {
          ++matched_count;
          ObSSGCTabletWithMetaScn &study_tablet =
              tablet_with_meta_scn_arr.at(tablet_idx);
          if (ckpt_tablet_meta_scn < study_tablet.meta_scn_) {
            study_tablet.meta_scn_ = ckpt_tablet_meta_scn;
            ++updated_count;
          }
        }
      }
    }
    LOG_INFO("finish combining tablet meta scn in ls checkpoint",
        KR(ret), K_(tenant_id), K(snapshot_meta_scn),
        "study_tablet_count", tablet_with_meta_scn_arr.count(),
        "checkpoint_tablet_count", tablet_meta_keys.count(),
        K(matched_count), K(updated_count));
    if (tablet_index_map_inited) {
      tablet_index_map.destroy();
    }
  }
  return ret;
}

void ObSSMarkSweepGCGenTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_do_work = false;
  bool is_ls_ckpt_enabled = false;
  if (OB_FAIL(check_if_need_do_gc_work(need_do_work))) {
    LOG_WARN("fail to check if need do mark sweep gc work", KR(ret), K_(tenant_id));
  } else if (!need_do_work) {
  } else if (is_sys_tenant(tenant_id_)
             && OB_FAIL(check_ss_ls_ckpt_enabled(tenant_id_, is_ls_ckpt_enabled))) {
    LOG_WARN("failed to check ls checkpoint enabled before mark sweep", KR(ret), K_(tenant_id));
  } else if (FALSE_IT(++run_cnt_)) {
  } else {
    ObSSMarkSweepDagParam param;
    SCN ckpt_meta_scn;
    SCN ckpt_ss_checkpoint_scn;
    ObSEArray<blocksstable::MacroBlockId, 64> ckpt_tablet_meta_keys;
    ckpt_tablet_meta_keys.set_attr(ObMemAttr(tenant_id_, "CkptMetaKeys"));
    bool is_match = false;
    bool has_ls_ckpt = false;
    // Study-diagnose collect/publish entry; must share the study thread; its failure does not affect this GC round.
    ObSSGCStudyDiagGuard diag_guard(tenant_id_, round_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(diag_guard.prepare())) {
      LOG_WARN("fail to prepare round diagnose task, ignore", KR(tmp_ret), K_(tenant_id), K_(round_id));
    }
    // round_id was fetched and persisted into __all_shared_gc_control.gc_round_id when this
    // server acquired/took-over the executor role in check_if_need_do_gc_work above.
    if (OB_FAIL(param.init(tenant_id_, round_id_, ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT,
            ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE, ObSSMarkSweepDagParam::DEFAULT_BATCH_SIZE))) {
      LOG_WARN("fail to init mark sweep dag param", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(tenant_id_, param.snapshot_meta_scn_))) {
      LOG_WARN("fail to get gts", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObSSGCUtil::get_safe_meta_scn(tenant_id_, param.snapshot_meta_scn_, param.safe_meta_scn_))) {
      LOG_WARN("fail to get safe meta scn", KR(ret), K_(tenant_id), K(param.snapshot_meta_scn_));
    } else if (OB_FAIL(update_gc_meta_scn_(param.round_id_, param.safe_meta_scn_))) {
      // update_gc_meta_scn_ has logged the detailed reason.
    } else if (OB_FAIL(ObSSGCUtil::get_tenant_reserved_meta_scns(tenant_id_, param.reserved_meta_scns_))) {
      LOG_WARN("fail to get tenant reserved meta scns", KR(ret), K_(tenant_id));
    } else if (is_sys_tenant(tenant_id_)
               && is_ls_ckpt_enabled
               && OB_FAIL(read_ls_ckpt_with_retry_(
                   ckpt_ss_checkpoint_scn, ckpt_tablet_meta_keys, ckpt_meta_scn, has_ls_ckpt))) {
      LOG_WARN("fail to read ls checkpoint references",
          KR(ret), K_(tenant_id), K(SYS_LS));
    } else if (OB_FAIL(get_all_ls_ss_checkpoint_scn_(param.snapshot_meta_scn_, param.safe_meta_scn_,
        param.reserved_meta_scns_, ckpt_ss_checkpoint_scn,
        param.ls_ss_checkpoint_scn_map_, param.recyclable_deleted_ls_ids_))) {
      LOG_WARN("fail to get ls ss checkpoint scn",
          KR(ret), K_(tenant_id), K(param), K(ckpt_ss_checkpoint_scn));
    } else if (OB_FAIL(study_tablet_meta_scn_(param.snapshot_meta_scn_,
        param.ls_ss_checkpoint_scn_map_, param.tablet_with_meta_scn_arr_,
        param.mark_deleted_tablets_))) {
      LOG_WARN("fail to study tablet meta scn", KR(ret), K_(tenant_id), K(param.snapshot_meta_scn_));
    } else if (OB_FAIL(merge_tablet_meta_scns_from_ckpt_(
                   param.snapshot_meta_scn_, ckpt_tablet_meta_keys,
                   param.tablet_with_meta_scn_arr_))) {
      LOG_WARN("fail to merge tablet meta scn from ls checkpoint",
          KR(ret), K_(tenant_id), K(ckpt_ss_checkpoint_scn));
    }
    // Publish and free raw results before creating the DAG; diagnose failure does not change the GC main-path ret.
    if (OB_TMP_FAIL(diag_guard.finish(param.safe_meta_scn_, ret))) {
      LOG_WARN("fail to finish study diagnose, ignore", KR(tmp_ret), K_(tenant_id),
          K(param.round_id_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_executor_and_round_id_(param.round_id_, is_match))) {
      LOG_WARN("fail to check executor and round_id after study", KR(ret), K_(tenant_id),
          K(param.round_id_));
    } else if (!is_match) {
      ret = OB_EAGAIN;
      LOG_INFO("executor or round_id changed after study, give up this round",
          KR(ret), K_(tenant_id), K(param.round_id_));
    } else {
      // Best-effort: generate LS_META_GC tasks for stale ls_meta versions in the same round,
      // before scheduling the mark sweep dag. Use tmp_ret so any failure here does not fail
      // this round (which would otherwise release the executor) and does not block the dag.
      if (OB_TMP_FAIL(generate_ls_meta_gc_task_(param.snapshot_meta_scn_, param.safe_meta_scn_,
          param.reserved_meta_scns_))) {
        LOG_WARN("fail to generate ls meta gc task, ignore", KR(tmp_ret), K_(tenant_id),
            K(param.snapshot_meta_scn_), K(param.safe_meta_scn_));
      }
      if (is_sys_tenant(tenant_id_)
          && has_ls_ckpt
          && OB_TMP_FAIL(generate_ls_ckpt_gc_task_(param.snapshot_meta_scn_, param.safe_meta_scn_,
              ckpt_meta_scn, param.reserved_meta_scns_))) {
        LOG_WARN("fail to generate ls checkpoint gc task, ignore", KR(tmp_ret), K_(tenant_id),
            K(param.snapshot_meta_scn_), K(param.safe_meta_scn_));
      }
      ObTenantDagScheduler *dag_scheduler = nullptr;
      if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant dag scheduler is null", KR(ret));
      } else if (OB_FAIL(dag_scheduler->create_and_add_dag<ObSSMarkSweepDag>(&param))) {
        LOG_WARN("fail to create and add mark sweep dag", KR(ret), K_(tenant_id), K(param));
      } else {
        LOG_INFO("succ to create and add mark sweep dag", K_(tenant_id), K(param));
        SERVER_EVENT_ADD("SHARED_GC", "START_MARK_SWEEP_GC",
          "tenant_id", param.tenant_id_,
          "round_id", param.round_id_,
          "gc_meta_scn", param.safe_meta_scn_,
          "trace_id", ObCurTraceId::get_trace_id_str());
      }
    }
  }

  // need_do_work means check_if_need_do_gc_work has acquired executor ownership (DOING).
  // Any later failure before entering the main work block, leaves no dag to report_result(),
  // so release the executor here.
  if (need_do_work && OB_SUCCESS != ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_release_executor_())) {
      LOG_WARN("fail to release executor when mark sweep dag is not scheduled",
               KR(tmp_ret), K_(tenant_id));
    }
  }
}

int ObSSMarkSweepGCGenTask::generate_ls_meta_gc_task_(
    const SCN &snapshot_meta_scn,
    const SCN &safe_meta_scn,
    const ObIArray<SCN> &reserved_meta_scns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSID, 16> ls_id_arr;
  ls_id_arr.set_attr(ObMemAttr(tenant_id_, "SSGCLSIDArr"));
  // 记录每个 ls_id 已经生成过 LS_META_GC 任务的最大 meta_scn，用于去重：本轮只为尚未落过
  // 任务的更旧版本生成任务，避免同一个 ls_meta 版本被反复插入 __all_shared_gc_task。
  ObHashMap<ObLSID, SCN> ls_meta_max_scn_map;
  if (OB_UNLIKELY(!snapshot_meta_scn.is_valid() || !safe_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(snapshot_meta_scn), K(safe_meta_scn));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_meta_max_scn_map.create(
                 DEFAULT_LS_META_MAX_SCN_MAP_BUCKET_NUM,
                 ObMemAttr(tenant_id_, "SSGCLSMaxScn")))) {
    LOG_WARN("fail to create ls meta max scn map", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(calc_gc_ls_meta_max_scn_(ls_meta_max_scn_map))) {
    LOG_WARN("fail to calc gc ls meta max scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_all_ls_id_(snapshot_meta_scn, ls_id_arr))) {
    LOG_WARN("fail to get all ls id", KR(ret), K_(tenant_id), K(snapshot_meta_scn));
  } else {
    ObSEArray<SCN, 16> all_meta_scns;
    all_meta_scns.set_attr(ObMemAttr(tenant_id_, "SSGCLSMetaSCN"));
    for (int64_t ls_idx = 0; OB_SUCC(ret) && (ls_idx < ls_id_arr.count()); ++ls_idx) {
      const ObLSID &ls_id = ls_id_arr.at(ls_idx);
      all_meta_scns.reuse();
      // 该 ls_id 已生成过任务的最大 meta_scn；不存在则为 min_scn（表示尚无任何已生成任务）。
      SCN generated_max_scn = SCN::min_scn();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(get_one_ls_all_meta_scn_(ls_id, snapshot_meta_scn, all_meta_scns))) {
        LOG_WARN("fail to get one ls all meta scn", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else if (all_meta_scns.empty()) {
        // may be concurrently gc'd by other executor, skip this ls.
        LOG_INFO("ls meta scn array is empty, skip generate ls meta gc task",
            K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
      } else if (OB_FAIL(ls_meta_max_scn_map.get_refactored(ls_id, generated_max_scn))
                 && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get generated max scn", KR(ret), K_(tenant_id), K(ls_id));
      } else {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          generated_max_scn = SCN::min_scn();
        }
        // LS checkpoint 已内嵌完整 LS_META，原始 LS_META 行只按 CLOG/reserved/safe
        // 计算回收边界，不能使用 PALF 中的 checkpoint 对象定位位点收紧边界。
        SCN min_reserved_meta_scn;
        SCN real_min_reserved_meta_scn;
        const SCN min_meta_scn = ObSSGCUtil::calc_min_meta_scn(
            reserved_meta_scns, all_meta_scns, safe_meta_scn, SCN::max_scn()/*studied_meta_scn*/,
            min_reserved_meta_scn, real_min_reserved_meta_scn);
        SCN ls_deleted_scn;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObSSGCUtil::get_ls_deleted_scn(
                               tenant_id_, ls_id, ls_deleted_scn))) {
          LOG_WARN("fail to get ls deleted scn", KR(ret), K_(tenant_id), K(ls_id));
        } else {
          const int64_t meta_scn_cnt = all_meta_scns.count();
          const bool is_mark_deleted = ls_deleted_scn.is_valid();
          const SCN &latest_meta_scn = all_meta_scns.at(meta_scn_cnt - 1);
          // A reservation may map to the final deleted version visible in this snapshot.
          // Once all GC bounds reach it, generate its task; the executor waits for tablet cleanup.
          bool can_del_all = false;
          if (is_mark_deleted && latest_meta_scn <= min_meta_scn) {
            ObSSMetaReadParamV2 read_param;
            read_param.set_ls_level_param(
                ObSSMetaReadParamType::LS_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW,
                ObAtomicProtocolTypeEnum::LS_META, ls_id, latest_meta_scn, true /*is_in_table*/);
            read_param.set_read_mark_delete(true);
            SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
              if (OB_FAIL(ObSSMetaTableHelper::get_ls(read_param, ls_value))) {
                LOG_WARN("fail to read final ls meta version", KR(ret), K(ls_id), K(latest_meta_scn));
              } else {
                // The lock row may be marked deleted after the snapshot; check this version's mark.
                can_del_all = ls_value.get_meta_head_info().is_mark_deleted();
              }
            }
          }
          // Keep the latest snapshot version unless all versions can be deleted.
          const bool retain_latest = !can_del_all && latest_meta_scn < min_meta_scn;
          for (int64_t i = 0; OB_SUCC(ret) && (i < meta_scn_cnt); ++i) {
            const SCN &ls_meta_scn = all_meta_scns.at(i);
            if (ls_meta_scn > min_meta_scn || (ls_meta_scn == min_meta_scn && !can_del_all)) {
              // all_meta_scns is ascending, remaining versions are all kept.
              break;
            } else if (retain_latest && (i == (meta_scn_cnt - 1))) {
              // Keep the latest live version in the snapshot.
              LOG_TRACE("keep latest ls meta version for alive ls", K_(tenant_id), K(ls_id),
                  K(ls_meta_scn), K(min_meta_scn));
            } else if (ls_meta_scn <= generated_max_scn) {
              // 该版本本轮之前已经生成过任务，跳过以避免重复插入。
              LOG_TRACE("ls meta version already has gc task, skip", K_(tenant_id), K(ls_id),
                  K(ls_meta_scn), K(generated_max_scn));
            } else if (OB_FAIL(check_init_and_stop_())) {
              LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
            } else if (OB_FAIL(ObSharedGCTaskUtil::generate_ls_meta_gc_task(
                           tenant_id_, ls_id, ls_meta_scn, *GCTX.sql_proxy_))) {
              LOG_WARN("fail to generate ls meta gc task", KR(ret), K_(tenant_id), K(ls_id),
                  K(ls_meta_scn), K(min_meta_scn));
            } else {
              LOG_TRACE("succ to generate ls meta gc task", K_(tenant_id), K(ls_id),
                  K(ls_meta_scn), K(min_meta_scn), K(is_mark_deleted));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::calc_gc_ls_meta_max_scn_(
    ObHashMap<ObLSID, SCN> &ls_meta_max_scn_map)
{
  int ret = OB_SUCCESS;
  ObSharedGCTaskLSMetaIterator ls_meta_iter;
  ObSharedGCLSMetaInfo ls_meta_info;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_meta_max_scn_map.reuse())) {
    LOG_WARN("fail to reuse ls meta max scn map", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_meta_iter.init(tenant_id_, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init ls meta iter", KR(ret), K_(tenant_id));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      ls_meta_info.reset();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(ls_meta_iter.get_next(ls_meta_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to iter ls meta info", KR(ret), K_(tenant_id));
        }
      } else {
        const ObLSID &ls_id = ls_meta_info.ls_id_;
        const SCN &meta_scn = ls_meta_info.meta_scn_;
        SCN old_max_scn;
        if (OB_FAIL(ls_meta_max_scn_map.get_refactored(ls_id, old_max_scn))) {
          if (OB_HASH_NOT_EXIST == ret) {
            // 该 ls_id 首次出现，直接写入。
            ret = OB_SUCCESS;
            if (OB_FAIL(ls_meta_max_scn_map.set_refactored(ls_id, meta_scn))) {
              LOG_WARN("fail to set ls meta max scn", KR(ret), K_(tenant_id), K(ls_id), K(meta_scn));
            }
          } else {
            LOG_WARN("fail to get ls meta max scn", KR(ret), K_(tenant_id), K(ls_id));
          }
        } else if (meta_scn > old_max_scn) {
          // 出现更大的 meta_scn，覆盖更新（flag = 1 允许覆盖）。
          if (OB_FAIL(ls_meta_max_scn_map.set_refactored(ls_id, meta_scn, 1/*overwrite*/))) {
            LOG_WARN("fail to overwrite ls meta max scn", KR(ret), K_(tenant_id), K(ls_id),
                K(meta_scn), K(old_max_scn));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::generate_ls_ckpt_gc_task_(
    const SCN &snapshot_meta_scn,
    const SCN &safe_meta_scn,
    const SCN &ckpt_meta_scn,
    const ObIArray<SCN> &reserved_meta_scns)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  const ObLSID &ls_id = SYS_LS;
  SCN generated_max_scn = SCN::min_scn();
  ObSSMetaService *meta_srv = nullptr;
  ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> iter_guard;
  ObSSMetaLSCkptKeyIter *iter = nullptr;
  ObSEArray<SCN, 16> all_meta_scns;
  all_meta_scns.set_attr(ObMemAttr(tenant_id_, "SSGCCkptSCN"));
  if (OB_UNLIKELY(!is_sys_tenant(tenant_id_)
                  || !snapshot_meta_scn.is_valid()
                  || !safe_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(snapshot_meta_scn), K(safe_meta_scn));
  } else if (OB_FAIL(check_ss_ls_ckpt_data_enabled(tenant_id_, is_enabled))) {
    LOG_WARN("failed to check ls checkpoint data enabled", KR(ret), K_(tenant_id));
  } else if (!is_enabled) {
    // LS checkpoint 数据上传功能未开启时不能产生 LS_CKPT_GC 任务。
  } else if (!ckpt_meta_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ckpt meta scn is invalid", KR(ret), K_(tenant_id), K(ckpt_meta_scn));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(calc_gc_ls_ckpt_max_scn_(generated_max_scn))) {
    LOG_WARN("fail to calc gc ls checkpoint max scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(meta_srv->get_ls_ckpt_iter(
                 ls_id, snapshot_meta_scn, true/*read_mark_delete*/, iter_guard))) {
    LOG_WARN("fail to get ls checkpoint iter", KR(ret), K_(tenant_id), K(ls_id), K(snapshot_meta_scn));
  } else if (OB_FAIL(iter_guard.get_iter(iter))) {
    LOG_WARN("fail to get ls checkpoint iter from guard", KR(ret), K_(tenant_id), K(ls_id));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      ObSSMetaLSCkptKey ckpt_key;
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(iter->get_next(ckpt_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to get next ls checkpoint", KR(ret), K_(tenant_id), K(ls_id));
        }
      } else if (OB_FAIL(all_meta_scns.push_back(ckpt_key.meta_scn_))) {
        LOG_WARN("fail to save ls checkpoint scn", KR(ret), K_(tenant_id), K(ls_id), K(ckpt_key));
      }
    }
    if (OB_SUCC(ret)) {
      // 按 meta_scn 升序排序。
      lib::ob_sort(all_meta_scns.begin(), all_meta_scns.end());
      SCN min_reserved_meta_scn;
      SCN real_min_reserved_meta_scn;
      // LS_CKPT_GC使用和 MARK_SWEEP_GC 相同的palf kv发布版本计算最小位点
      const SCN min_meta_scn = ObSSGCUtil::calc_min_meta_scn(
          reserved_meta_scns, all_meta_scns, safe_meta_scn, ckpt_meta_scn,
          min_reserved_meta_scn, real_min_reserved_meta_scn);
      for (int64_t i = 0; OB_SUCC(ret) && i < all_meta_scns.count(); ++i) {
        const SCN &meta_scn = all_meta_scns.at(i);
        if (meta_scn >= min_meta_scn) {
          break;
        } else if (meta_scn <= generated_max_scn) {
          LOG_TRACE("ls checkpoint already has gc task, skip", K_(tenant_id), K(ls_id),
              K(meta_scn), K(generated_max_scn));
        } else if (OB_FAIL(check_init_and_stop_())) {
          LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(ObSharedGCTaskUtil::generate_ls_ckpt_gc_task(
                       tenant_id_, ls_id, meta_scn, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to generate ls checkpoint gc task", KR(ret), K_(tenant_id),
              K(ls_id), K(meta_scn), K(min_meta_scn));
        } else {
          LOG_TRACE("succ to generate ls checkpoint gc task", K_(tenant_id),
              K(ls_id), K(meta_scn), K(min_meta_scn));
        }
      }
    }
  }
  return ret;
}

int ObSSMarkSweepGCGenTask::calc_gc_ls_ckpt_max_scn_(SCN &max_scn)
{
  int ret = OB_SUCCESS;
  ObSharedGCTaskTypeFilter type_filter;
  ObSharedGCTaskRoundIdFilter round_id_filter;
  ObSharedGCTaskTableFullIterator task_iter;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "SSGCCkptTask"));
  ObSharedGCTask task;
  int64_t gmt_create = 0;
  int64_t gmt_modified = 0;
  max_scn = SCN::min_scn();
  type_filter.is_filter_enabled_ = true;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(type_filter.types_.push_back(ObSharedGCTaskType::LS_CKPT_GC))) {
    LOG_WARN("fail to set ls checkpoint gc task filter", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(task_iter.init(tenant_id_, OB_ALL_SHARED_GC_TASK_TNAME,
                 type_filter, round_id_filter, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init shared gc task iterator", KR(ret), K_(tenant_id));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      task.reset();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(task_iter.get_next(
                     allocator, task, gmt_create, gmt_modified))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to iter ls checkpoint gc task", KR(ret), K_(tenant_id));
        }
      } else {
        ObSharedGCLSCheckpointInfo ckpt_info;
        if (OB_FAIL(ckpt_info.from_value(task.value_))) {
          LOG_WARN("fail to deserialize ls checkpoint info", KR(ret), K(task.key_));
        } else if (OB_UNLIKELY(!ckpt_info.is_valid() || (ckpt_info.ls_id_ != SYS_LS)
                   || (0 != task.key_.round_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ls checkpoint gc task", KR(ret), K_(tenant_id), K(ckpt_info), K(task.key_));
        } else if (ckpt_info.meta_scn_ > max_scn) {
          max_scn = ckpt_info.meta_scn_;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

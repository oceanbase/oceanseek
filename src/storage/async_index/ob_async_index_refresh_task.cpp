/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_refresh_task.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/async_index/ob_async_index_service.h"  // ObAsyncIndexService
#include "storage/ob_common_id_utils.h"
#include "storage/ddl/ob_ddl_lock.h"                    // ObOnlineDDLLock
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"        // ObMySQLTransaction
#include "lib/mysqlclient/ob_isql_connection.h"          // ObISQLConnection::set_ddl_info
#include "lib/mysqlclient/ob_mysql_proxy.h"              // ObSessionDDLInfo
#include "share/ob_server_struct.h"                    // GCTX
#include "share/ob_debug_sync.h"                       // DEBUG_SYNC
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server.h"                        // observer::ObServer::get_instance()
#include "share/ob_share_util.h"                       // ObShareUtil::set_default_timeout_ctx
#include "share/ob_ddl_common.h"                       // ObDDLUtil::calc_inner_sql_execute_timeout
#include "share/rc/ob_tenant_base.h"                   // MTL
#include "storage/tx_storage/ob_ls_service.h"          // ObLSService, ObLSHandle
#include "storage/tablet/ob_tablet.h"                  // ObTablet::get_async_index_max_published_version
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

namespace oceanbase
{
namespace storage
{

static int lock_table_lock_in_trans_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const int64_t timeout_us,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_tablet(
                 tenant_id, table_id, tablet_ids, lock_mode, timeout_us, iconn))) {
    LOG_WARN("failed to lock tablets", K(ret), K(table_id), K(tablet_ids), K(lock_mode), K(timeout_us));
  }
  return ret;
}

static int lock_for_async_index_refresh_in_trans_(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const common::ObTabletID &data_tablet_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 0;
  common::ObSEArray<common::ObTabletID, 1> data_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == data_table_id
      || !data_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index refresh lock args", K(ret), K(tenant_id),
             K(data_table_id), K(data_tablet_id));
  } else if (OB_FAIL(data_tablet_ids.push_back(data_tablet_id))) {
    LOG_WARN("failed to push data tablet id", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(lock_table_lock_in_trans_(
                 tenant_id, data_table_id, data_tablet_ids,
                 transaction::tablelock::ROW_SHARE, timeout_us, trans))) {
    LOG_WARN("failed to lock data table for async index refresh", K(ret),
             K(tenant_id), K(data_table_id), K(data_tablet_id));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(
                 tenant_id, data_table_id, transaction::tablelock::ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock online ddl data table for async index refresh", K(ret),
             K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(
                 tenant_id, data_tablet_ids, transaction::tablelock::ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock online ddl tablets for async index refresh", K(ret),
             K(tenant_id), K(data_table_id), K(data_tablet_id));
  }
  return ret;
}

int ObAsyncIndexRefreshTask::set_inner_conn_ddl_info_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  sqlclient::ObISQLConnection *conn = trans.get_connection();
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *data_schema = nullptr;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null inner sql connection", K(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(stat_.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get refresh schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(stat_.tenant_id_, stat_.data_table_id_, data_schema))) {
    LOG_WARN("failed to get refresh data schema", K(ret));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (data_schema->get_schema_version() != stat_.schema_version_) {
    ret = OB_DDL_SCHEMA_VERSION_NOT_MATCH;
  } else if (data_schema->is_user_hidden_table() && ASYNC_REFRESH_TYPE_OFFLINE_DDL != stat_.refresh_type_) {
    // Periodic work sampled before the DDL must not race offline preparation.
    ret = OB_NEED_RETRY;
  } else {
    // is_ddl is already true on the ddl_sql_proxy connection's session; we set it
    // again for clarity and add the async-index-refresh flag, which makes
    // ObPxSubCoord::start_ddl read snapshot/schema version from the service map
    // instead of __all_ddl_task_status. Do NOT set refreshing_mview here: a
    // complete refresh must stay a plain snapshot scan.
    common::ObSessionDDLInfo ddl_info;
    ddl_info.set_is_ddl(true);
    ddl_info.set_is_async_index_refresh(true);
    ddl_info.set_source_table_hidden(data_schema->is_user_hidden_table());
    ddl_info.set_dest_table_hidden(data_schema->is_user_hidden_table());
    ddl_info.set_search_index_ddl(stat_.is_search_index_);
    ddl_info.set_partition_local_ddl(stat_.is_partition_local_);
    ddl_info.set_is_refresh_on_error_skip(stat_.is_refresh_on_error_skip_);
    if (OB_FAIL(conn->set_ddl_info(&ddl_info))) {
      LOG_WARN("failed to set ddl info on connection", K(ret));
    }
  }
  return ret;
}

int ObAsyncIndexRefreshTask::set_task(const ObAsyncIndexRefreshTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.sql_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null refresh sql in task param", K(ret), K(param));
  } else if (OB_FAIL(sql_.assign(*param.sql_))) {
    // deep-copy the pre-generated SQL into the task's own buffer
    LOG_WARN("failed to assign refresh sql", K(ret), K(param));
  } else {
    ObRefreshTaskStat stat;
    stat.tenant_id_ = param.tenant_id_;
    stat.data_table_id_ = param.data_table_id_;
    stat.ls_id_ = param.ls_id_;
    stat.data_tablet_id_ = param.data_tablet_id_;
    stat.tablet_id_ = param.index_tablet_id_;
    stat.data_transfer_seq_ = param.data_transfer_seq_;
    stat.index_transfer_seq_ = param.index_transfer_seq_;
    stat.start_refresh_scn_ = param.start_refresh_scn_;
    stat.target_refresh_scn_ = param.target_refresh_scn_;
    stat.refresh_id_ = param.refresh_id_;
    stat.refresh_task_id_ = param.refresh_task_id_;
    stat.refresh_type_ = param.refresh_type_;
    stat.refresh_parallelism_ = param.parallelism_;
    stat.schema_version_ = param.schema_version_;
    stat.is_partition_local_ = param.is_partition_local_;
    stat.is_complete_refresh_ = param.is_complete_refresh_;
    stat.is_refresh_on_error_skip_ = param.is_refresh_on_error_skip_;
    stat.is_search_index_ = param.is_search_index_;
    stat.queue_time_ = common::ObTimeUtility::current_time();
    stat.retry_cnt_ = param.retry_cnt_;
    stat.result_ = OB_SUCCESS;
    skip_dop_quota_ = param.skip_dop_quota_;
    dop_quota_deferred_ = false;
    ls_scheduler_ = param.ls_scheduler_;
    if (OB_NOT_NULL(ls_scheduler_)) {
      stat.leader_epoch_ = param.leader_epoch_ >= 0
          ? param.leader_epoch_ : ls_scheduler_->get_leader_epoch();
    }
    publish_stat(stat);
  }
  return ret;
}

void ObAsyncIndexRefreshTask::copy_stat(ObRefreshTaskStat &stat) const
{
  common::ObSpinLockGuard guard(stat_lock_);
  stat = stat_;
}

void ObAsyncIndexRefreshTask::publish_stat(const ObRefreshTaskStat &stat)
{
  common::ObSpinLockGuard guard(stat_lock_);
  stat_ = stat;
}

int ObAsyncIndexRefreshTask::lock_index_tablet_for_refresh_in_trans_(
    const ObRefreshTaskStat &stat,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = nullptr;
  if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null inner sql connection for index tablet lock", K(ret));
  } else {
    transaction::tablelock::ObTableLockOwnerID owner_id;
    if (OB_FAIL(owner_id.convert_from_value(
            transaction::tablelock::ObLockOwnerType::DEFAULT_OWNER_TYPE,
            get_tid_cache()))) {
      LOG_WARN("failed to get owner id", K(ret));
    } else {
      // Timer path: timeout=0 (fail immediately, retry next round)
      // Manual/post-build path: wait for ongoing refresh to finish
      const int64_t lock_timeout_us =
          (ASYNC_REFRESH_TYPE_PERIOD == stat.refresh_type_)
              ? 0
              : OB_ASYNC_INDEX_DDL_LOCK_TIMEOUT_US;
      if (stat.is_partition_local_) {
        // Pause only delete-index refreshes before they request the refresh fence.
        DEBUG_SYNC(BEFORE_ASYNC_INDEX_DELETE_REFRESH_LOCK);
      }
      transaction::tablelock::ObLockObjRequest lock_arg;
      lock_arg.obj_type_ = transaction::tablelock::ObLockOBJType::OBJ_TYPE_ASYNC_INDEX_REFRESH;
      lock_arg.obj_id_ = stat.tablet_id_.id();  // index_tablet_id
      lock_arg.owner_id_ = owner_id;
      lock_arg.lock_mode_ = transaction::tablelock::EXCLUSIVE;
      lock_arg.op_type_ = transaction::tablelock::ObTableLockOpType::IN_TRANS_COMMON_LOCK;
      lock_arg.timeout_us_ = lock_timeout_us;
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_obj(
              stat.tenant_id_, lock_arg, conn))) {
        if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret
            || OB_TRY_LOCK_ROW_CONFLICT == ret
            || OB_TIMEOUT == ret
            || OB_EAGAIN == ret) {
          LOG_INFO("[ASYNC_INDEX_REFRESH] index tablet lock conflict, another refresh in progress",
                   K(ret), K(stat.tablet_id_), K(stat.refresh_type_), K(lock_timeout_us));
          ret = OB_EAGAIN;
        } else {
          LOG_WARN("failed to lock index tablet for refresh", K(ret), K(stat.tablet_id_));
        }
      }
    }
  }
  if (OB_SUCC(ret) && stat.is_partition_local_) {
    // Test hook after the delete-index refresh fence is held and before data-table locks.
    DEBUG_SYNC(AFTER_ASYNC_INDEX_DELETE_REFRESH_LOCK);
  }
  return ret;
}

int ObAsyncIndexRefreshTask::recheck_publish_scn_after_lock_(const ObRefreshTaskStat &stat)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle data_tablet_handle;
  int64_t published_version = 0;
  // The OnlineDDL tablet ROW_EXCLUSIVE lock conflicts with transfer's
  // EXCLUSIVE lock and is held through trans.end(). Validate the scheduled
  // generation now, before reading its watermark or executing refresh SQL.
  if (stat.data_transfer_seq_ < 0 || stat.index_transfer_seq_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing async index refresh tablet generation", K(ret), K(stat));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(stat.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls for refresh tablet check", K(ret), K(stat.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(stat.ls_id_));
  } else if (OB_FAIL(get_async_index_refresh_tablet(
                 *ls_handle.get_ls(), stat.data_tablet_id_, data_tablet_handle,
                 stat.data_transfer_seq_))) {
    LOG_WARN("data tablet location changed", K(ret), K(stat));
  } else if (OB_FAIL(get_async_index_refresh_tablet(
                 *ls_handle.get_ls(), stat.tablet_id_, tablet_handle,
                 stat.index_transfer_seq_))) {
    LOG_WARN("failed to get index tablet at scheduled location",
             K(ret), K(stat.tablet_id_));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is null", K(ret), K(stat.tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_async_index_refresh_start_version(
                 published_version, false /*use_cache*/))) {
    LOG_WARN("failed to get max published version", K(ret), K(stat.tablet_id_));
  } else {
    share::SCN current_publish_scn = async_index_snapshot_to_scn(published_version);
    if (current_publish_scn >= stat.target_refresh_scn_) {
      ret = OB_NO_NEED_UPDATE;
      LOG_INFO("[ASYNC_INDEX_REFRESH] skip refresh after lock: target already reached",
               K(stat.tablet_id_), K(current_publish_scn),
               K(stat.start_refresh_scn_), K(stat.target_refresh_scn_));
    } else if (current_publish_scn > stat.start_refresh_scn_) {
      if (ASYNC_REFRESH_TYPE_PERIOD == stat.refresh_type_) {
        ret = OB_NO_NEED_UPDATE;
        LOG_INFO("[ASYNC_INDEX_REFRESH] skip periodic refresh after lock: start_scn stale, defer to next round",
                 K(stat.tablet_id_), K(current_publish_scn),
                 K(stat.start_refresh_scn_), K(stat.target_refresh_scn_));
      } else {
        // 手动/后建路径：start_scn过期但目标未达到，需要用新start重新生成SQL重试
        ret = OB_NEED_RETRY;
        LOG_INFO("[ASYNC_INDEX_REFRESH] start_scn stale but target not reached, need regenerate",
                 K(stat.tablet_id_), K(current_publish_scn),
                 K(stat.start_refresh_scn_), K(stat.target_refresh_scn_),
                 K(stat.refresh_type_));
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshTask::handle()
{
  int ret = OB_SUCCESS;
  bool skipped = false;  // true when recheck_publish_scn_after_lock_ short-circuits the task
  ObRefreshTaskStat stat; // runtime stat as a local copy of the task's stat to avoid race conditions
  stat = stat_;
  stat.start_time_ = common::ObTimeUtility::current_time();
  CONSUMER_GROUP_FUNC_GUARD(share::ObFunctionType::PRIO_ASYNC_INDEX_REFRESH);
  // Schema drift is handled by the SQL itself: the cached refresh SQL carries an
  // ob_ddl_schema_version(data_table, ver) hint, so if a DDL bumped the data
  // table's schema version since the SQL was generated, execution fails at
  // resolve time with OB_DDL_SCHEMA_VERSION_NOT_MATCH. That code is
  // excluded from is_async_index_retryable_error(), so the next
  // timer round regenerates the SQL against the new schema. No separate
  // pre-execution schema-version probe is needed.
  // LS-level leader epoch fencing: if a leader switch happened on this LS since
  // this task was enqueued, the LS scheduler's epoch has moved on. Drop the task
  // without executing so a previous leader term's residual in-flight tasks on
  // this LS cannot write after the switch. Tasks on other LSs are unaffected.
  if (OB_NOT_NULL(ls_scheduler_) && stat.leader_epoch_ != ls_scheduler_->get_leader_epoch()) {
    ret = OB_NOT_MASTER;
    LOG_INFO("LS leader epoch changed, drop residual refresh task", K(ret),
             "ls_id", stat.ls_id_,
             "task_epoch", stat.leader_epoch_,
             "cur_epoch", ls_scheduler_->get_leader_epoch(),
             K(stat.tablet_id_), K(stat.target_refresh_scn_));
  }

  if (OB_SUCC(ret)) {
    if (sql_.empty()) {
      // Empty SQL is treated as a no-op success (e.g. no incremental data).
      stat.affected_rows_ = 0;
    } else if (OB_ISNULL(GCTX.ddl_sql_proxy_)) {
      // No proxy available (unit-test environment). Treat as no-op success.
      stat.affected_rows_ = 0;
    } else {
      const int64_t timeout_us = share::ObDDLUtil::calc_inner_sql_execute_timeout();
      common::ObTimeoutCtx timeout_ctx;
      // Run the refresh SQL inside an explicit transaction (instead of an
      // autocommit proxy write) so that a multi-source-data (MDS) registration
      // can later be attached to the same transaction and committed atomically
      // with the index data write.
      common::ObMySQLTransaction trans;
      // The refresh SQL carries opt_param('ddl_task_id', refresh_task_id). At PX
      // execution time ObPxSubCoord::start_ddl needs snapshot_version &
      // schema_version, which it normally reads from __all_ddl_task_status by
      // task id -- but refresh_task_id is synthetic and has no such record. So we
      // (1) register the execution context in the node-local
      // ObAsyncIndexService map keyed by refresh_task_id, and (2) flag the session with
      // is_async_index_refresh so start_ddl takes the map-lookup branch instead.
      // start_ddl runs synchronously inside trans.write below (same node, the LS
      // leader), so the registration is visible there; we release it afterwards.
      ObAsyncIndexService *svc = MTL(ObAsyncIndexService *);
      bool registered = false;
      bool refresh_dop_acquired = false;
      share::ObCommonID unique_build_id;
      int64_t ddl_build_id = 0;
      if (OB_FAIL(ObCommonIDUtils::gen_unique_id(stat.tenant_id_, unique_build_id))) {
        LOG_WARN("failed to generate async index ddl build id", K(ret), K(stat.tenant_id_), K(stat.refresh_task_id_));
      } else if (OB_UNLIKELY(unique_build_id.id() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async index ddl build id", K(ret), K(unique_build_id), K(stat.refresh_task_id_));
      } else {
        ddl_build_id = unique_build_id.id();
      }
      // Key debug point #3: the actual refresh SQL about to execute, together
      // with its identifying context (tablet / refresh ids / scn window / dop).
      LOG_INFO("[ASYNC_INDEX_REFRESH] execute refresh sql", K(stat.tenant_id_),
               K(stat.data_table_id_),
               K(stat.ls_id_), K(stat.data_tablet_id_), K(stat.tablet_id_),
               K(stat.refresh_id_), K(stat.refresh_task_id_), K(ddl_build_id),
               K(stat.start_refresh_scn_), K(stat.target_refresh_scn_),
               K(stat.is_complete_refresh_), K(stat.is_partition_local_),
               K(stat.refresh_parallelism_),
               K(stat.schema_version_), "sql", sql_.ptr());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(timeout_ctx, timeout_us))) {
        LOG_WARN("failed to set timeout ctx for refresh sql", K(ret), K(timeout_us));
      } else if (OB_ISNULL(svc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async index service is null", K(ret), K(stat_.tenant_id_));
      } else if (OB_FAIL(svc->register_refresh_versions(stat_.refresh_task_id_,
                     stat_.start_refresh_scn_,
                     stat_.target_refresh_scn_.get_val_for_sql(),
                     stat_.schema_version_,
                     ddl_build_id))) {
        LOG_WARN("failed to register refresh context", K(ret), K(stat_.refresh_task_id_),
                 K(stat_.start_refresh_scn_), K(stat_.target_refresh_scn_),
                 K(stat_.schema_version_), K(ddl_build_id));
      } else if (FALSE_IT(registered = true)) {
      } else if (OB_FAIL(trans.start(
                     GCTX.ddl_sql_proxy_, stat.tenant_id_, false /* with_snapshot */,
                     static_cast<int32_t>(GET_GROUP_ID())))) {
        LOG_WARN("failed to start trans for async index refresh", K(ret),
                 K(stat.tenant_id_), K(stat.tablet_id_));
      } else if (OB_FAIL(set_inner_conn_ddl_info_(trans))) {
        LOG_WARN("failed to set ddl info on refresh connection", K(ret),
                 K(stat.tenant_id_), K(stat.tablet_id_));
      } else if (OB_FAIL(lock_index_tablet_for_refresh_in_trans_(stat, trans))) {
        // OB_EAGAIN: another refresh task holds the lock on this index tablet.
        // Timer path: will retry next round. Manual path: caller retries with
        // fresh publish_scn.
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to lock index tablet for refresh", K(ret), K(stat.tablet_id_));
        }
      } else if (OB_FAIL(lock_for_async_index_refresh_in_trans_(
                     stat.tenant_id_, stat.data_table_id_,
                     stat.data_tablet_id_, trans))) {
        LOG_WARN("failed to lock async index refresh objects", K(ret),
                 K(stat.tenant_id_), K(stat.data_table_id_), K(stat.data_tablet_id_));
      } else if (OB_FAIL(recheck_publish_scn_after_lock_(stat))) {
        if (OB_NO_NEED_UPDATE == ret) {
          // 目标已达到或定时任务start_scn过期，跳过执行
          ret = OB_SUCCESS;
          skipped = true;
        } else if (OB_NEED_RETRY == ret) {
          // 手动/后建路径：start_scn过期但目标未达到，传播给调用方重试
          skipped = true;
        } else {
          LOG_WARN("failed to recheck publish_scn after lock", K(ret), K(stat));
        }
      } else if (OB_ISNULL(host_)) {
        // Production tasks must be associated with the shared worker to obey
        // the tenant-level DOP quota.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async index refresh worker is null", K(ret), K(stat.tablet_id_));
      } else if (!skip_dop_quota_ && OB_FAIL(host_->try_acquire_refresh_dop(stat.refresh_parallelism_))) {
        if (OB_EAGAIN == ret) {
          // Execute SQL only when the complete requested DOP fits, so concurrent
          // tasks cannot exceed the aggregate quota.
          dop_quota_deferred_ = true;
          skipped = true;
          LOG_INFO("async index refresh DOP quota unavailable, defer task", K(ret),
                   K(stat.tablet_id_), K(stat.refresh_parallelism_));
        } else if (OB_STATE_NOT_MATCH == ret) {
          ret = OB_EAGAIN;
          LOG_INFO("async index refresh DOP request exceeds current limit, regenerate next round",
                   K(ret), K(stat.tablet_id_), K(stat.refresh_parallelism_));
        } else {
          LOG_WARN("failed to acquire async index refresh DOP", K(ret),
                   K(stat.tablet_id_), K(stat.refresh_parallelism_));
        }
      } else {
        refresh_dop_acquired = !skip_dop_quota_;
        // Flush the MDS redo before DirectLoad logs. Both records still belong
        // to this transaction, so either the physical INC and watermark commit
        // together, or neither becomes visible.
        if (OB_FAIL(ObTabletAsyncIndexRefreshInfoMdsHelper::register_published_version(
                stat.tenant_id_, stat.ls_id_, stat.tablet_id_,
                async_index_scn_to_snapshot(stat.target_refresh_scn_), trans))) {
          LOG_WARN("failed to register async-index published version", K(ret),
                   K(stat.tenant_id_), K(stat.ls_id_), K(stat.tablet_id_),
                   K(stat.target_refresh_scn_));
        } else {
          // 周期调度线程的 trace_id 初始为 0。execute_write() 检测到 trace_id=0 时会临时
          // 生成一个，但执行完后立即 reset，导致 trans.write() 返回后取到的是空值。
          // 在此提前初始化 trace_id，使 execute_write() 认为已有有效值而不做 reset，
          // 从而在 trans.write() 返回后能正确捕获 inner SQL 的 trace_id。
          bool trace_id_owned = false;
          if (0 == ObCurTraceId::get()[0]) {
            common::ObCurTraceId::init(observer::ObServer::get_instance().get_self());
            trace_id_owned = true;
          }
          if (OB_FAIL(trans.write(stat.tenant_id_, sql_.ptr(), stat.affected_rows_))) {
            LOG_WARN("[ASYNC_INDEX_REFRESH] failed to execute async index refresh sql", K(ret),
                     K(stat.tenant_id_), K(stat.tablet_id_),
                     K(stat.refresh_task_id_), "sql", sql_.ptr());
          }
          // 此时 trace_id 仍有效，记录后再清理
          {
            const ObCurTraceId::TraceId *trace = ObCurTraceId::get_trace_id();
            if (OB_NOT_NULL(trace)) {
              stat.trace_id_len_ = trace->to_string(stat.trace_id_buf_, sizeof(stat.trace_id_buf_));
              // 防止 to_string 返回值超出缓冲区长度导致后续 ObString 构造越界
              if (OB_UNLIKELY(stat.trace_id_len_ > static_cast<int32_t>(sizeof(stat.trace_id_buf_)))) {
                stat.trace_id_len_ = static_cast<int32_t>(sizeof(stat.trace_id_buf_));
              }
            }
          }
          if (trace_id_owned) {
            common::ObCurTraceId::reset();
          }
        }
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to end trans for async index refresh", K(ret), K(tmp_ret),
                   K(stat.tenant_id_), K(stat.tablet_id_));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      // Return the acquired DOP reservation after transaction cleanup on all
      // SQL success, failure, and commit-failure paths.
      if (refresh_dop_acquired && OB_NOT_NULL(host_)) {
        host_->release_refresh_dop(stat.refresh_parallelism_);
        refresh_dop_acquired = false;
      }
      // 读取 PX 执行中累计的 skipped_rows
      if (registered && OB_NOT_NULL(svc)) {
        int64_t skipped = 0;
        if (OB_SUCCESS == svc->get_skipped_rows(stat.refresh_task_id_, skipped)) {
          stat.skipped_rows_ = skipped;
        }
      }
      // trace_id 已在上方 write 分支中记录
      // Release the registered versions on every path (idempotent). start_ddl has
      // already consumed them synchronously by the time trans.write returns.
      if (registered && OB_NOT_NULL(svc)) {
        int tmp_ret = svc->release_refresh_versions(stat.refresh_task_id_);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to release refresh versions", K(tmp_ret), K(stat.refresh_task_id_));
        }
      }
      if (OB_SUCC(ret)
          && ASYNC_REFRESH_TYPE_PERIOD == stat.refresh_type_
          && !stat.is_complete_refresh_) {
        // Let mysqltest delay successful periodic insert- and delete-index samples
        // to train their independent adaptive DOP controllers deterministically.
        // Transaction and DOP resources are released, while the wait remains in
        // elapsed time.
        DEBUG_SYNC(AFTER_PERIODIC_ASYNC_INDEX_REFRESH);
      }
    }
  }

  if (!skipped) {
    stat.result_ = ret;
    stat.end_time_ = common::ObTimeUtility::current_time();
    if (OB_SUCCESS != stat.result_) {
      LOG_WARN("[ASYNC_INDEX_REFRESH] async index refresh task failed", K(ret),
               K(stat.tenant_id_), K(stat.ls_id_), K(stat.data_tablet_id_),
               K(stat.tablet_id_), K(stat.refresh_id_), K(stat.refresh_task_id_),
               K(stat.start_refresh_scn_), K(stat.target_refresh_scn_), K(stat.result_));
    } else {
      LOG_INFO("[ASYNC_INDEX_REFRESH] async index refresh task completed",
               K(stat.tenant_id_), K(stat.ls_id_), K(stat.data_tablet_id_),
               K(stat.tablet_id_), K(stat.refresh_id_), K(stat.refresh_task_id_),
               K(stat.start_refresh_scn_), K(stat.target_refresh_scn_),
               K(stat.affected_rows_), "elapsed_us", stat.end_time_ - stat.start_time_);
    }
    publish_stat(stat);
  } else if (OB_SUCCESS == ret) {
    stat.result_ = OB_NO_NEED_UPDATE;
    stat.end_time_ = common::ObTimeUtility::current_time();
    stat.affected_rows_ = 0;
    publish_stat(stat);
  }
  return ret;
}

void ObAsyncIndexRefreshTask::reset()
{
  stat_.reset();
  sql_.reset();
  ls_scheduler_ = nullptr;
  skip_dop_quota_ = false;
  dop_quota_deferred_ = false;
  // host_ is not reset here; it is set once during pool initialization
}

bool ObAsyncIndexRefreshTask::match(
    const common::ObTabletID &tablet_id,
    const share::SCN &target_scn) const
{
  return stat_.tablet_id_ == tablet_id && stat_.target_refresh_scn_ == target_scn;
}

bool ObAsyncIndexRefreshTask::need_retry() const
{
  // A task fenced by an LS-level leader-epoch change must NOT be retried: this
  // LS's residual work is obsolete and the new leader regenerates fresh tasks.
  if (OB_NOT_NULL(ls_scheduler_) && stat_.leader_epoch_ != ls_scheduler_->get_leader_epoch()) {
    return false;
  }
  // DOP admission is scheduling backpressure. Keep waiting without treating it
  // as the tablet-lock OB_EAGAIN excluded from is_async_index_retryable_error().
  if (dop_quota_deferred_) {
    return true;
  }
  // Give up after too many retries: the task falls into the finished ring as an
  // observable terminal failure instead of retrying (and occupying a retry slot)
  // forever. The next timer round recomputes the SCN window from the persisted
  // publish scn, so giving up here never loses data.
  if (stat_.retry_cnt_ >= OB_ASYNC_INDEX_MAX_RETRY_CNT) {
    return false;
  }
  return is_async_index_retryable_error(stat_.result_);
}

int64_t ObAsyncIndexRefreshTask::get_retry_delay_us() const
{
  return calc_async_index_retry_delay_us(stat_.retry_cnt_);
}

} // namespace storage
} // namespace oceanbase

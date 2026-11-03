/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_async_index_refresh_util.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_ddl_common.h"
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "share/location_cache/ob_location_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
namespace share
{

int ObAsyncIndexDDLRefreshTarget::refresh(ObIAsyncIndexDDLPublishOp &publish_op)
{
  int ret = OB_SUCCESS;
  bool need_publish = true;
  if (!is_prepared()) {
    SCN candidate;
    if (OB_FAIL(publish_op.prepare(need_publish))) {
      if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_WARN("[ASYNC_INDEX] failed to prepare DDL publish phase", K(ret), K(publish_op), K_(publish_scn));
      }
    } else if (!need_publish) {
      // 没有待刷新的索引，不获取或缓存发布 GTS。
      LOG_INFO("[ASYNC_INDEX] skip DDL publish without targets", K(publish_op));
    } else if (OB_FAIL(publish_op.get_gts(candidate))) {
      LOG_WARN("[ASYNC_INDEX] failed to acquire DDL publish target", K(ret), K(candidate), K(publish_op));
    } else if (!candidate.is_valid_and_not_min()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ASYNC_INDEX] invalid DDL publish target", K(ret), K(candidate), K(publish_op));
    } else {
      // Cache before the first partition RPC, including failed publish attempts.
      publish_scn_ = candidate;
      LOG_INFO("[ASYNC_INDEX] cached DDL publish target", K_(publish_scn), K(publish_op));
    }
  }
  if (OB_SUCC(ret) && need_publish) {
    if (OB_FAIL(publish_op.publish(publish_scn_))) {
      if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_WARN("[ASYNC_INDEX] failed to publish DDL target, retain cached target",
            K(ret), K_(publish_scn), K(publish_op));
      }
    }
  }
  return ret;
}

ObAsyncIndexDDLPublishOp::ObAsyncIndexDDLPublishOp(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    common::ObIArray<uint64_t> &index_table_ids,
    const int8_t refresh_type)
  : tenant_id_(tenant_id),
    data_table_id_(data_table_id),
    snapshot_version_(0),
    index_table_ids_(index_table_ids),
    refresh_type_(refresh_type),
    refresh_build_snapshot_(false)
{
}

ObAsyncIndexDDLPublishOp::ObAsyncIndexDDLPublishOp(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const int64_t snapshot_version,
    common::ObIArray<uint64_t> &index_table_ids,
    const int8_t refresh_type)
  : tenant_id_(tenant_id),
    data_table_id_(data_table_id),
    snapshot_version_(snapshot_version),
    index_table_ids_(index_table_ids),
    refresh_type_(refresh_type),
    refresh_build_snapshot_(true)
{
}

int ObAsyncIndexDDLPublishOp::prepare(bool &need_publish)
{
  int ret = OB_SUCCESS;
  need_publish = true;
  if (refresh_build_snapshot_ && OB_FAIL(ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
      tenant_id_, data_table_id_, snapshot_version_, index_table_ids_, refresh_type_))) {
    if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_WARN("[ASYNC_INDEX] failed to refresh DDL build snapshot", K(ret), K(*this));
    }
  } else if (OB_FAIL(collect_publish_targets())) {
    LOG_WARN("[ASYNC_INDEX] failed to collect targets before DDL publish GTS", K(ret), K(*this));
  } else {
    // The publish phase can include UNAVAILABLE indexes absent in phase one.
    // 即使只有一个异步索引，也需推进 published SCN，避免查询快照早于新建同步索引的可读版本。
    need_publish = !index_table_ids_.empty();
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::collect_publish_targets()
{
  int ret = OB_SUCCESS;
  if (storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type_) {
    ret = ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
        tenant_id_, data_table_id_, index_table_ids_,
        true /*retry_on_missing_search_schema*/, true /*allow_unavailable*/);
    if (OB_FAIL(ret)) {
      LOG_WARN("[ASYNC_INDEX] fail to collect publish targets", K(ret), K(*this));
    }
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::get_gts(SCN &scn)
{
  int ret = OB_SUCCESS;
  // prepare() has collected and checked the second phase's index set.
  if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id_, nullptr, scn))) {
    LOG_WARN("[ASYNC_INDEX] fail to get publish gts", K(ret), K(*this));
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::publish(const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collect_publish_targets())) {
    LOG_WARN("[ASYNC_INDEX] failed to recollect targets for DDL publish", K(ret), K(scn), K(*this));
  } else if (index_table_ids_.empty() && storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type_) {
    // All async indexes may have been dropped before the second phase.
    LOG_INFO("[ASYNC_INDEX] skip DDL publish after all indexes were dropped", K(scn), K(*this));
  } else {
    if (OB_FAIL(ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
        tenant_id_, data_table_id_, scn.get_val_for_tx(), index_table_ids_, refresh_type_))) {
      if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_WARN("[ASYNC_INDEX] failed to refresh DDL publish snapshot", K(ret), K(scn), K(*this));
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const int64_t target_scn,
    const common::ObIArray<uint64_t> &async_index_table_ids,
    const int8_t refresh_type)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> data_tablet_ids;
  ObArray<ObLSID> ls_ids;
  share::ObLocationService *location_service = nullptr;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  const int64_t refresh_rpc_timeout = GCONF._ob_ddl_timeout;
  const int64_t retry_interval_us = 200 * 1000; // 200ms
  constexpr int64_t MAX_RPC_RETRY_COUNT = 3; // Three retries after the initial attempt.
  const bool is_manual = storage::ASYNC_REFRESH_TYPE_MANUAL == refresh_type;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id
      || target_scn <= 0 || async_index_table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(data_table_id),
        K(target_scn), "index_count", async_index_table_ids.count());
  } else if (!is_async_index_refresh_enabled(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[ASYNC_INDEX] refresh not allowed, async index refresh is disabled", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv rpc proxy is null", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_id, data_tablet_ids))) {
    LOG_WARN("fail to get data table tablets", K(ret), K(tenant_id), K(data_table_id));
  } else if (data_tablet_ids.empty()) {
    LOG_INFO("[ASYNC_INDEX] no data tablets found, skip refresh", K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
      *GCTX.sql_proxy_, tenant_id, data_tablet_ids, ls_ids))) {
    LOG_WARN("fail to batch get ls for data tablets", K(ret), K(tenant_id));
    if (is_manual && OB_ITEM_NOT_MATCH == ret) {
      // DDL can remove tablets between schema enumeration and the mapping lookup.
      ret = OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
    }
  } else if (data_tablet_ids.count() != ls_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data tablet count not equal ls count", K(ret),
        K(data_tablet_ids.count()), K(ls_ids.count()));
  }

  // For each index table, get its tablet array and is_delete flag
  ObSEArray<uint64_t, 4> valid_index_table_ids;
  ObSEArray<ObSEArray<ObTabletID, 16>, 4> index_tablet_arrays;
  ObSEArray<bool, 4> is_delete_flags;

  if (OB_FAIL(ret) || data_tablet_ids.empty()) {
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < async_index_table_ids.count(); ++j) {
      const uint64_t index_table_id = async_index_table_ids.at(j);
      ObSEArray<ObTabletID, 16> index_tablets;
      const share::schema::ObTableSchema *idx_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, idx_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_table_id));
      } else if (OB_ISNULL(idx_schema)) {
        LOG_INFO("[ASYNC_INDEX] index dropped, skip", K(index_table_id));
        continue;
      } else if (idx_schema->is_in_deleting()) {
        LOG_INFO("[ASYNC_INDEX] index is being deleted, skip refresh", K(index_table_id));
        continue;
      } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, index_table_id, index_tablets))) {
        LOG_WARN("fail to get index table tablets", K(ret), K(index_table_id));
      } else if (index_tablets.count() != data_tablet_ids.count()) {
        // The two tablet arrays may come from opposite sides of a partition DDL.
        ret = is_manual ? OB_SCHEMA_EAGAIN : OB_ERR_UNEXPECTED;
        LOG_WARN("index tablet count mismatch data tablet count", K(ret),
            K(index_table_id), K(index_tablets.count()), K(data_tablet_ids.count()));
      } else if (OB_FAIL(valid_index_table_ids.push_back(index_table_id))) {
        LOG_WARN("fail to push back valid index table id", K(ret));
      } else if (OB_FAIL(index_tablet_arrays.push_back(index_tablets))) {
        LOG_WARN("fail to push back index tablets", K(ret));
      } else if (OB_FAIL(is_delete_flags.push_back(
          share::schema::is_async_delete_index(idx_schema->get_index_type())))) {
        LOG_WARN("fail to push back is_delete flag", K(ret));
      }
    }
  }

  // Send one RPC per data tablet, containing all index entries for that tablet
  if (OB_FAIL(ret) || data_tablet_ids.empty() || valid_index_table_ids.empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_tablet_ids.count(); ++i) {
      const ObTabletID &data_tablet_id = data_tablet_ids.at(i);
      const ObLSID &ls_id = ls_ids.at(i);
      // Each tablet gets its own retry budget. This context inherits the caller's
      // deadline and restores it on scope exit; retries never reset the budget.
      common::ObTimeoutCtx timeout_ctx;
      int64_t tablet_deadline = ObTimeUtility::current_time() + refresh_rpc_timeout;
      const int64_t worker_deadline = THIS_WORKER.get_timeout_ts();
      if (worker_deadline > 0) {
        tablet_deadline = MIN(tablet_deadline, worker_deadline);
      }
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, refresh_rpc_timeout))) {
        LOG_WARN("failed to set tablet refresh timeout",
                 K(ret), K(data_tablet_id), K(refresh_rpc_timeout));
      } else if (OB_FAIL(timeout_ctx.set_abs_timeout(
                     MIN(timeout_ctx.get_abs_timeout(), tablet_deadline)))) {
        LOG_WARN("failed to limit tablet refresh timeout", K(ret), K(data_tablet_id));
      }
      obrpc::ObAsyncIndexRefreshArg arg;
      arg.tenant_id_ = tenant_id;
      arg.ls_id_ = ls_id;
      arg.tablet_id_ = data_tablet_id;
      arg.target_refresh_scn_ = target_scn;
      arg.data_table_id_ = data_table_id;
      arg.refresh_type_ = refresh_type;
      for (int64_t j = 0; OB_SUCC(ret) && j < valid_index_table_ids.count(); ++j) {
        if (OB_FAIL(arg.index_table_ids_.push_back(valid_index_table_ids.at(j)))) {
          LOG_WARN("fail to push back index_table_id", K(ret));
        } else if (OB_FAIL(arg.index_tablet_ids_.push_back(index_tablet_arrays.at(j).at(i)))) {
          LOG_WARN("fail to push back index_tablet_id", K(ret));
        } else if (OB_FAIL(arg.is_delete_flags_.push_back(is_delete_flags.at(j)))) {
          LOG_WARN("fail to push back is_delete_flag", K(ret));
        }
      }
      if (OB_SUCC(ret) && !arg.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ASYNC_INDEX] invalid refresh arg", K(ret), K(arg));
      }
      bool force_renew = false;
      bool need_retry = true;
      int64_t retry_cnt = 0;
      while (OB_SUCC(ret) && need_retry) {
        need_retry = false;
        ObAddr leader_addr;
        obrpc::ObAsyncIndexRefreshRes res;
        if (timeout_ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("tablet refresh timed out before leader lookup",
                   K(ret), K(data_tablet_id), K(target_scn), K(tablet_deadline));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("refresh request interrupted", K(ret), K(data_tablet_id));
        } else if (OB_FAIL(location_service->get_leader(
                       GCONF.cluster_id, tenant_id, ls_id, force_renew, leader_addr))) {
          LOG_WARN("fail to get refresh leader", K(ret), K(tenant_id), K(ls_id));
        } else if (is_manual && FALSE_IT(DEBUG_SYNC(BEFORE_MANUAL_ASYNC_INDEX_REFRESH_RPC))) {
        } else if (timeout_ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("tablet refresh timed out after leader lookup",
                   K(ret), K(leader_addr), K(data_tablet_id), K(target_scn), K(tablet_deadline));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("refresh request interrupted after leader lookup", K(ret), K(data_tablet_id));
        } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr)
                       .detect_session_killed(true)
                       .timeout(timeout_ctx.get_timeout())
                       .async_index_refresh(arg, res))) {
          LOG_WARN("[ASYNC_INDEX] refresh RPC failed", K(ret), K(leader_addr), K(arg));
        } else {
          ret = res.ret_code_;
          if (OB_FAIL(ret)) {
            LOG_WARN("[ASYNC_INDEX] refresh returned error",
                     K(ret), K(leader_addr), K(arg), K(res.already_refreshed_));
          }
        }
        // Retry routing within the same LS. Mapping/schema errors must reach the
        // caller, which rebuilds tablet-to-LS routing and the full request.
        const bool renew_leader = OB_NOT_MASTER == ret || OB_NEED_RETRY == ret
            || is_server_down_error(ret) || OB_TENANT_NOT_IN_SERVER == ret
            || OB_LOCATION_NOT_EXIST == ret || OB_LS_LOCATION_NOT_EXIST == ret
            || OB_LS_LOCATION_LEADER_NOT_EXIST == ret;
        if (OB_EAGAIN == ret || renew_leader) {
          // Keep this tablet's original target and deadline. The next leader rereads the
          // checkpoint queue and rebuilds SQL; never retry an old node's window.
          // EAGAIN also covers lock/capacity contention; wait without renewing the leader.
          const int64_t remain_us = timeout_ctx.get_timeout();
          if (remain_us <= 0) {
            ret = OB_TIMEOUT;
            LOG_WARN("tablet refresh retry budget exhausted",
                     K(ret), K(leader_addr), K(data_tablet_id), K(target_scn), K(tablet_deadline));
          } else if (retry_cnt >= MAX_RPC_RETRY_COUNT) {
            // Preserve the last error for the manual/DDL caller's retry policy.
            LOG_WARN("[ASYNC_INDEX] tablet refresh retry count exhausted",
                     K(ret), K(data_tablet_id), K(ls_id), K(target_scn), K(retry_cnt));
          } else {
            ++retry_cnt;
            LOG_INFO("[ASYNC_INDEX] retry refresh on current leader", K(ret), K(leader_addr),
                     K(data_tablet_id), K(arg), K(retry_cnt), K(renew_leader));
            ob_usleep(MIN(retry_interval_us, remain_us));
            force_renew = renew_leader;
            ret = OB_SUCCESS;
            // Retry only the same target, within this tablet's remaining budget.
            need_retry = true;
          }
        }
      }
#ifdef ERRSIM
      if (OB_SUCC(ret) && 0 == i
          && static_cast<int8_t>(storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL) == refresh_type) {
        DEBUG_SYNC(AFTER_OFFLINE_ASYNC_INDEX_PARTITION_REFRESH);
      }
#endif
      if (is_manual && is_master_changed_error(ret)) {
        int tmp_ret = OB_SUCCESS;
        ObAddr new_leader;
        if (OB_SUCCESS != (tmp_ret = location_service->get_leader(
            GCONF.cluster_id, tenant_id, ls_id, true /*force_renew*/, new_leader))) {
          LOG_WARN("failed to renew leader after manual refresh error",
                   K(tmp_ret), K(ret), K(tenant_id), K(ls_id));
        }
        // Preserve the original error so the caller retries the whole table.
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[ASYNC_INDEX] trigger_refresh_for_table completed",
          K(tenant_id), K(data_table_id), K(target_scn),
          "partition_count", data_tablet_ids.count(),
          "index_count", async_index_table_ids.count());
    }
  }
  return ret;
}

bool ObAsyncIndexRefreshUtil::is_async_index_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index;
  }
  // Invalid guard: fail closed.
  return enabled;
}

bool ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index_refresh;
  }
  // Invalid guard: fail closed.
  return enabled;
}

bool ObAsyncIndexRefreshUtil::is_async_index_adaptive_parallelism_enabled(
    const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index_adaptive_parallelism;
  }
  // Invalid guard: fail closed.
  return enabled;
}

int64_t ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
    const uint64_t tenant_id)
{
  int64_t dop_limit = 1;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    dop_limit = cfg->_async_index_refresh_dop_limit;
    if (0 == dop_limit) {
      dop_limit = OB_MAX(
          static_cast<int64_t>(
              MTL_CPU_COUNT() * storage::DEFAULT_ASYNC_INDEX_REFRESH_DOP_PERCENT / 100),
          1L);
    }
  }
  return dop_limit;
}

int ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    common::ObIArray<uint64_t> &out_index_table_ids,
    const bool retry_on_missing_search_schema,
    const bool allow_unavailable)
{
  int ret = OB_SUCCESS;
  out_index_table_ids.reuse();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(data_table_id));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *data_table_schema = nullptr;
    if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table schema not exist", K(ret), K(data_table_id));
    } else {
      const common::ObIArray<share::schema::ObAuxTableMetaInfo> &simple_index_infos =
          data_table_schema->get_simple_index_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const share::schema::ObAuxTableMetaInfo &info = simple_index_infos.at(i);
        const bool is_async_search = share::schema::is_async_search_def_index(info.index_type_);
        uint64_t index_table_id = info.table_id_;
        const share::schema::ObTableSchema *index_schema = nullptr;
        if (!share::schema::is_async_index(info.index_type_)) {
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
          LOG_WARN("fail to get index schema", K(ret), K(index_table_id));
        } else if (OB_ISNULL(index_schema)) {
          if (retry_on_missing_search_schema && is_async_search) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("async search def schema does not exist", K(ret), K(tenant_id), K(info));
          } else {
            LOG_INFO("index schema does not exist, skip", K(tenant_id), K(data_table_id), K(i), K(info));
          }
        } else if (share::schema::INDEX_STATUS_AVAILABLE != index_schema->get_index_status()
                   && !(allow_unavailable && share::schema::INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status())) {
          LOG_INFO("index schema is not available, skip", K(tenant_id), K(data_table_id), K(i), K(info));
        } else if (is_async_search
                   && OB_FAIL(index_schema->get_search_data_index_tid(index_table_id))) {
          LOG_WARN("fail to get async search data table id", K(ret), K(info));
        } else if (is_async_search
                   && OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
          LOG_WARN("fail to get async search data schema", K(ret), K(index_table_id));
        } else if (OB_ISNULL(index_schema)) {
          if (retry_on_missing_search_schema) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("async search data schema does not exist", K(ret), K(tenant_id), K(index_table_id));
          } else {
            LOG_INFO("async search data schema does not exist, skip", K(tenant_id), K(data_table_id), K(index_table_id));
          }
        } else if (share::schema::INDEX_STATUS_AVAILABLE != index_schema->get_index_status()
                   && !(allow_unavailable && share::schema::INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status())) {
          LOG_INFO("async search data schema is not available, skip",
              K(tenant_id), K(data_table_id), K(index_table_id));
        } else if (OB_FAIL(out_index_table_ids.push_back(index_table_id))) {
          LOG_WARN("fail to push back index table id", K(ret), K(index_table_id));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const int8_t refresh_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> async_index_table_ids;
  share::SCN cur_scn;
  const bool allow_unavailable = static_cast<int8_t>(storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL) == refresh_type;
  if (!share::ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[ASYNC_INDEX] refresh not allowed, async index refresh is disabled",
        K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(collect_async_index_table_ids(
      tenant_id, data_table_id, async_index_table_ids,
      true /*retry_on_missing_search_schema*/, allow_unavailable))) {
    LOG_WARN("[ASYNC_INDEX] fail to collect async index table ids", K(ret),
        K(tenant_id), K(data_table_id));
  } else if (async_index_table_ids.empty()) {
    LOG_INFO("[ASYNC_INDEX] no async index found on table, skip refresh",
        K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, nullptr, cur_scn))) {
    LOG_WARN("[ASYNC_INDEX] fail to get gts", K(ret), K(tenant_id));
  } else if (OB_FAIL(trigger_refresh_for_table(
      tenant_id, data_table_id, cur_scn.get_val_for_tx(), async_index_table_ids,
      refresh_type))) {
    LOG_WARN("[ASYNC_INDEX] fail to trigger refresh for async index tables",
        K(ret), K(tenant_id), K(data_table_id), K(async_index_table_ids), K(cur_scn), K(refresh_type));
  } else {
    LOG_INFO("[ASYNC_INDEX] trigger refresh for async index tables succeeded",
        K(tenant_id), K(data_table_id), K(async_index_table_ids), K(cur_scn), K(refresh_type));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

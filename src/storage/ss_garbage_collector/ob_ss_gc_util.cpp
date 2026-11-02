/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "common/ob_smart_var.h"
#include "lib/utility/ob_sort.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/worker.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_manager.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_server_table_operator.h"
#include "share/ob_unit_table_operator.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_iterator.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

#ifdef ERRSIM
bool ObSSGCUtil::is_errsim_target(const uint64_t tenant_id)
{
  const int64_t filter_tenant_id = GCONF.errsim_shared_gc_tenant_id;
  const bool bool_ret = (0 == filter_tenant_id
      || static_cast<uint64_t>(filter_tenant_id) == tenant_id);
  return bool_ret;
}

bool ObSSGCUtil::is_errsim_target(
    const uint64_t tenant_id,
    const uint64_t tablet_id)
{
  const int64_t filter_tablet_id = GCONF.errsim_shared_gc_tablet_id;
  const bool bool_ret = is_errsim_target(tenant_id)
      && (0 == filter_tablet_id || static_cast<uint64_t>(filter_tablet_id) == tablet_id);
  return bool_ret;
}

ERRSIM_POINT_DEF(EN_SS_GC_READ_LS_CKPT_META_ERROR);
#endif

int ObSSGCUtil::create_and_add_dag(const ObSSGCExecDagInitParam &param)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler is null", KR(ret), K(param));
  } else if (OB_FAIL(scheduler->create_and_add_dag<ObSSGCExecDag>(&param))) {
    LOG_WARN("fail to create and add ss gc exec dag", KR(ret), K(param));
  } else {
    LOG_INFO("success to create and add ss gc exec dag", KR(ret), K(param));
  }
  return ret;
}

void ObSSGCUtil::report_mark_sweep_set_to_min_event(
    const ObSSGCTablet &gc_tablet,
    const char *reason_str)
{
  if (TC_REACH_TIME_INTERVAL(3600LL * 1000LL * 1000LL)) { // every 1h
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_STUDY_SET_TO_MIN", "tenant_id", MTL_ID(),
        "ls_id", gc_tablet.ls_id_.id(), "tablet_id", gc_tablet.tablet_id_.id(),
        "transfer_scn", gc_tablet.transfer_scn_, "reason", reason_str);
  }
}

int64_t ObSSGCUtil::get_scan_batch_size(const uint64_t tenant_id)
{
  int64_t batch_size = DEFAULT_SS_GC_TASK_SCAN_BATCH_SIZE;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_LIKELY(tenant_config.is_valid())) {
    batch_size = tenant_config->_shared_gc_task_scan_batch_size;
  }
  return batch_size;
}

int ObSSGCUtil::get_shared_gc_sql_timeout_us(const uint64_t tenant_id, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = DEFAULT_SS_GC_SQL_TIMEOUT_US;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      timeout_us = tenant_config->_shared_gc_sql_timeout;
    }
  }
  return ret;
}

int ObSSGCUtil::get_ls_ckpt_meta_scn_from_palf_kv(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    SCN &ckpt_meta_scn)
{
  int ret = OB_SUCCESS;
  ckpt_meta_scn.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
    if (OB_ISNULL(meta_srv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSSMetaService is null", KR(ret), K(tenant_id), K(ls_id));
#ifdef ERRSIM
    } else if (GCONF.enable_logservice && is_sys_tenant(tenant_id)
        && is_sys_tenant(MTL_ID()) && ls_id.is_sys_ls()
        && is_errsim_target(tenant_id)
        && OB_FAIL(OB_E(EN_SS_GC_READ_LS_CKPT_META_ERROR, GETTID()) OB_SUCCESS)) {
      // 仅模拟 GC 读取 PALF 的错误，不改变实际 key 的存在状态。
      LOG_INFO("inject ls checkpoint meta read error", KR(ret), K(tenant_id), K(ls_id));
#endif
    } else if (OB_FAIL(meta_srv->get_ls_ckpt_meta_scn(ls_id, ckpt_meta_scn))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get ls checkpoint meta scn", KR(ret), K(tenant_id), K(ls_id));
      }
    }
  }
  return ret;
}

int ObSSGCUtil::get_ls_deleted_scn(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    SCN &ls_deleted_scn)
{
  int ret = OB_SUCCESS;
  ls_deleted_scn.reset();
  ObSSMetaCtrlKey ls_lock_row_key(blocksstable::ObStorageObjectType::SHARED_LS_LOCK,
      ls_id.id()/*first_id*/,
      0/*second_id, unused for SHARED_LS_LOCK*/,
      0/*third_id, unused for SHARED_LS_LOCK*/);
  ObString lock_row_meta_key;
  ObString lock_row_meta_value;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SSGCUtil"));
  ObSSMetaCtrlTableOperator op;
  ObSSMetaCtrlLockRowValue ls_lock_row_value;
  int64_t value_pos = 0;
  int64_t sql_timeout_us = 0;
  char *key_buf = nullptr;
  int64_t key_buf_len = 0;
  int64_t key_pos = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_lock_row_key.get_meta_key_serialize_size(key_buf_len))) {
    LOG_WARN("failed to get meta key serialize size", KR(ret), K(tenant_id), K(ls_id),
        K(ls_lock_row_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc meta key buffer", KR(ret), K(tenant_id), K(ls_id),
        K(key_buf_len));
  } else if (OB_FAIL(ls_lock_row_key.serialize_meta_key(key_buf, key_buf_len, key_pos))) {
    LOG_WARN("failed to serialize meta key", KR(ret), K(tenant_id), K(ls_id),
        K(ls_lock_row_key));
  } else if (FALSE_IT(lock_row_meta_key.assign(key_buf, key_pos))) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_shared_gc_sql_timeout_us(tenant_id, sql_timeout_us))) {
    LOG_WARN("fail to get shared gc sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, sql_timeout_us))) {
    LOG_WARN("fail to init meta ctrl table operator", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(op.read_row(tenant_id, ls_id, ls_lock_row_key.type_,
                                 lock_row_meta_key, allocator, lock_row_meta_value))) {
    if (OB_ITER_END == ret) {
      bool is_tenant_dropped = false;
      if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(
              tenant_id, is_tenant_dropped))) {
        LOG_WARN("failed to check tenant dropped when ls lock row is absent",
            KR(ret), K(tenant_id), K(ls_id));
      } else if (is_tenant_dropped) {
        // 租户删除已提交，当前 SSGC 属于退出阶段的残留执行，终止本轮而不是继续回收。
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_INFO("tenant has been dropped when ls lock row is absent",
            KR(ret), K(tenant_id), K(ls_id));
      } else {
        // 租户仍然存在时，LS lock row 缺失仍属于真实的协议元数据异常。
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls lock row not found for existing tenant, unexpected",
            KR(ret), K(tenant_id), K(ls_id), K(ls_lock_row_key));
      }
    } else {
      LOG_WARN("fail to read ls lock row",
          KR(ret), K(tenant_id), K(ls_id), K(ls_lock_row_key));
    }
  } else if (OB_FAIL(ls_lock_row_value.deserialize(lock_row_meta_value.ptr(),
                                                   lock_row_meta_value.length(), value_pos))) {
    LOG_WARN("fail to deserialize ls lock row value",
        KR(ret), K(tenant_id), K(ls_id), K(ls_lock_row_key));
  } else if (OB_UNLIKELY(!ls_lock_row_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls lock row value", KR(ret), K(tenant_id), K(ls_id),
        K(ls_lock_row_key), K(ls_lock_row_value));
  } else if (OB_UNLIKELY(ls_lock_row_value.deleted_scn_.is_valid()
      && !ls_lock_row_value.is_mark_deleted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("deleted scn is valid but not mark deleted, unexpected",
        KR(ret), K(tenant_id), K(ls_id), K(ls_lock_row_key), K(ls_lock_row_value));
  } else {
    ls_deleted_scn = ls_lock_row_value.deleted_scn_;
  }
  return ret;
}

int ObSSGCUtil::get_tenant_min_start_service_time(
    const uint64_t tenant_id,
    int64_t &min_start_service_time)
{
  int ret = OB_SUCCESS;
  min_start_service_time = 0;
  ObUnitTableOperator ut_operator;
  ObArray<ObAddr> tenant_servers;
  ObArray<ObServerInfoInTable> all_servers_info;
  // Note: ObUnitTableOperator::get_all_servers_by_tenant cannot directly get meta tenant servers,
  // so we need to convert the meta tenant_id to user tenant_id first.
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", KR(ret));
  } else if (OB_FAIL(ut_operator.get_all_servers_by_tenant(user_tenant_id, tenant_servers, false/*is_strict*/))) {
    LOG_WARN("fail to get servers by tenant", KR(ret), K(user_tenant_id));
  } else if (tenant_servers.empty()) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant servers are empty, need retry", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, all_servers_info))) {
    LOG_WARN("fail to get all servers info", KR(ret));
  } else {
    int64_t min_val = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && (i < tenant_servers.count()); ++i) {
      const ObAddr &server = tenant_servers.at(i);
      bool found = false;
      for (int64_t j = 0; !found && (j < all_servers_info.count()); ++j) {
        const ObServerInfoInTable &cur_server_info = all_servers_info.at(j);
        if (cur_server_info.get_server() == server) {
          found = true;
          if (cur_server_info.is_permanent_offline()) {
            // Skip permanently offline servers; they are excluded from the calculation.
            LOG_INFO("skip permanently offline server", K(tenant_id), K(server), K(cur_server_info));
          } else {
            const int64_t start_service_time = cur_server_info.get_start_service_time();
            if (0 == start_service_time) {
              // start_service_time == 0 means this server has not entered service yet.
              ret = OB_EAGAIN;
              LOG_WARN("server not in service yet, need retry", KR(ret), K(tenant_id),
                  K(server), K(cur_server_info));
            } else {
              min_val = MIN(min_val, start_service_time);
            }
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        // The server exists in __all_unit but cannot be found in __all_server. retry later.
        ret = OB_EAGAIN;
        LOG_WARN("server in unit table but not found in server table, need retry",
            KR(ret), K(server), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (INT64_MAX == min_val) {
        ret = OB_EAGAIN;
        LOG_WARN("no valid start service time is found, need retry", KR(ret), K(tenant_id),
            K(tenant_servers), K(all_servers_info));
      } else {
        min_start_service_time = min_val;
      }
    }
  }
  return ret;
}

int ObSSGCUtil::get_safe_meta_scn(
    const uint64_t tenant_id,
    const SCN &cur_gts,
    SCN &safe_meta_scn)
{
  int ret = OB_SUCCESS;
  safe_meta_scn.reset();
  int64_t retention_time_us = 0;
  int64_t tenant_create_us = 0;
  int64_t min_start_service_time = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !cur_gts.is_valid()
                  || cur_gts.is_min() || cur_gts.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(cur_gts));
  } else {
    retention_time_us = GCONF._shared_gc_retention_time;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tenant_create_us(tenant_id, tenant_create_us))) {
    LOG_WARN("fail to get tenant create us", KR(ret), K(tenant_id));
  } else {
    const int64_t tenant_created_time_us = ObTimeUtility::current_time_us() - tenant_create_us;
    int64_t shutdown_compensate_us = 0;
    int64_t elapsed_us = 0;
    // 仅当租户存在时长超过 retention_time 时，才计算宕机重启补偿。
    // 新创建的租户没有需要保护的长历史数据，因此直接跳过补偿（以及额外的内部表查询）。
    if (tenant_created_time_us > retention_time_us) {
      if (OB_FAIL(get_tenant_min_start_service_time(tenant_id, min_start_service_time))) {
        LOG_WARN("fail to get tenant min start service time", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(min_start_service_time <= 0)) {
        ret = OB_EAGAIN;
        LOG_WARN("min_start_service_time is invalid, need retry",
            KR(ret), K(tenant_id), K(min_start_service_time));
      } else {
        elapsed_us = ObTimeUtility::current_time_us() - min_start_service_time;
        shutdown_compensate_us = ((retention_time_us > elapsed_us)
                                  ? (retention_time_us - elapsed_us)
                                  : 0);
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t retention_time_ns = retention_time_us * 1000L;
      const int64_t shutdown_compensate_ns = shutdown_compensate_us * 1000L;
      const int64_t total_retention_time_ns = retention_time_ns + shutdown_compensate_ns;
      const int64_t cur_gts_val = static_cast<int64_t>(cur_gts.get_val_for_gts());
      if (OB_UNLIKELY(cur_gts_val <= total_retention_time_ns)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_gts is not larger than total delta", KR(ret), K(tenant_id), K(cur_gts),
            K(retention_time_ns), K(shutdown_compensate_ns));
      } else {
        safe_meta_scn = SCN::minus(cur_gts, total_retention_time_ns);
      }
      LOG_INFO("finish to get safe meta scn", KR(ret), K(tenant_id), K(cur_gts), K(safe_meta_scn),
          K(retention_time_us), K(tenant_create_us), K(tenant_created_time_us),
          K(min_start_service_time), K(elapsed_us), K(shutdown_compensate_us));
    }
  }
  return ret;
}

int ObSSGCUtil::get_tenant_create_us(
    const uint64_t tenant_id,
    int64_t &tenant_create_us)
{
  int ret = OB_SUCCESS;
  tenant_create_us = 0;
  // __all_tenant 位于 sys 租户下，且只包含 user/sys 租户，
  // 因此先将（可能是 meta 的）tenant_id 映射为 user 租户 id 再过滤。
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.append_fmt("SELECT gmt_create FROM %s WHERE tenant_id = %lu",
              OB_ALL_TENANT_TNAME, user_tenant_id))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(user_tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // 暂时找不到租户行，交给调用方稍后重试。
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist in __all_tenant, need retry",
              KR(ret), K(tenant_id), K(user_tenant_id));
        } else {
          LOG_WARN("fail to get next row", KR(ret), K(tenant_id), K(sql));
        }
      } else if (OB_FAIL(result->get_timestamp("gmt_create", nullptr, tenant_create_us))) {
        LOG_WARN("fail to get gmt_create", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(tenant_create_us <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant create us", KR(ret), K(tenant_id), K(tenant_create_us));
      }
    }
  }
  return ret;
}

// Note: this function is related to ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_
// for fork-table reserved_meta_scn x, all meta_scn >= x should be kept.
// for ordinary reservation types (backup/snapshot/gc/...) with reserved_meta_scn x,
// only the largest meta_scn <= x should be kept.
// if ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_ logic changes later, this function should
// be updated accordingly.
int ObSSGCUtil::get_tenant_reserved_meta_scns(
    const uint64_t tenant_id,
    ObIArray<SCN> &meta_scns)
{
  int ret = OB_SUCCESS;
  meta_scns.reset();
  int64_t sql_timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us(tenant_id, sql_timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else {
    // 1. get all reserved meta scns
    ObSharedMetaReservedIterator iter;
    if (OB_FAIL(iter.init(tenant_id, sql_timeout_us))) {
      LOG_WARN("fail to init shared meta reserved iterator", KR(ret), K(tenant_id), K(sql_timeout_us));
    } else {
      bool is_iter_end = false;
      ObSharedMetaReservedInfo info;
      while (OB_SUCC(ret) && !is_iter_end) {
        info.reset();
        if (OB_FAIL(iter.get_next(info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("fail to get next shared meta reserved info", KR(ret), K(tenant_id), K(iter));
          }
        } else if (OB_FAIL(meta_scns.push_back(info.meta_scn_))) {
          LOG_WARN("fail to push back reserved meta scn", KR(ret), K(tenant_id), K(info));
        }
      }

      // 2. sort and deduplicate
      if (OB_SUCC(ret) && !meta_scns.empty()) {
        const int64_t meta_scn_cnt = meta_scns.count();
        lib::ob_sort(meta_scns.get_data(), meta_scns.get_data() + meta_scn_cnt);
        for (int64_t i = (meta_scn_cnt - 1); OB_SUCC(ret) && (i > 0); --i) {
          if (meta_scns.at(i) == meta_scns.at(i - 1)) {
            if (OB_FAIL(meta_scns.remove(i))) {
              LOG_WARN("fail to remove duplicated reserved meta scn", KR(ret), K(i), K(meta_scns));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSSGCUtil::get_sql_timeout_us(
    const uint64_t tenant_id,
    int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    int64_t config_timeout_us = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant config is not valid", KR(ret), K(tenant_id));
    } else if (FALSE_IT(config_timeout_us = tenant_config->_shared_gc_sql_timeout)) {
    } else if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", KR(ret));
    } else if (THIS_WORKER.is_timeout_ts_valid()) {
      const int64_t remain_us = THIS_WORKER.get_timeout_remain();
      if (remain_us > 0) {
        timeout_us = MIN(remain_us, config_timeout_us);
      } else {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout remain is not enough", KR(ret), K(remain_us));
      }
    } else {
      timeout_us = config_timeout_us;
    }
  }
  return ret;
}

SCN ObSSGCUtil::calc_min_meta_scn(
    const ObIArray<SCN> &reserved_meta_scns,
    const ObIArray<SCN> &all_meta_scns,
    const SCN &safe_meta_scn,
    const SCN &studied_meta_scn,
    SCN &min_reserved_meta_scn,
    SCN &real_min_reserved_meta_scn)
{
  const int64_t all_meta_scn_cnt = all_meta_scns.count();
  const int64_t reserved_meta_scn_cnt = reserved_meta_scns.count();
  // 1. find the minimum reserved_meta_scn (max_scn means "no reserved constraint").
  min_reserved_meta_scn = SCN::max_scn();
  for (int64_t r = 0; r < reserved_meta_scn_cnt; ++r) {
    min_reserved_meta_scn = MIN(min_reserved_meta_scn, reserved_meta_scns.at(r));
  }
  // 2. 根据最小 reserved_meta_scn 计算 reservation 回收边界，all_meta_scns 按升序排列。
  //    2.1 如果存在小于等于 min_reserved_meta_scn 的 meta 版本，
  //        取其中最大值作为实际的最小保留版本。
  //    2.2 如果 all_meta_scns 非空，但所有版本都大于 min_reserved_meta_scn，
  //        说明该 reservation 早于当前对象的首个 meta 版本；此时使用
  //        min_reserved_meta_scn 作为保守边界，使本轮保留当前对象的全部 meta 版本。
  //    2.3 如果 all_meta_scns 为空，则无论是否存在 reservation，
  //        real_min_reserved_meta_scn 均保持 max_scn，表示当前没有
  //        meta 版本可用于计算 reservation 对应的实际保留版本。
  //        如果 reserved_meta_scns 为空，该值同样保持 max_scn。
  real_min_reserved_meta_scn = SCN::max_scn();
  if (reserved_meta_scn_cnt > 0) {
    bool found = false;
    for (int64_t i = (all_meta_scn_cnt - 1); !found && (i >= 0); --i) {
      if (all_meta_scns.at(i) <= min_reserved_meta_scn) {
        real_min_reserved_meta_scn = all_meta_scns.at(i);
        found = true;
      }
    }
    if (!found && (all_meta_scn_cnt > 0)) {
      real_min_reserved_meta_scn = min_reserved_meta_scn;
      LOG_INFO("all meta scns are greater than min reserved meta scn, "
              "use min reserved meta scn as conservative boundary",
              K(min_reserved_meta_scn), K(all_meta_scns));
    }
  }
  // 3. get min_meta_scn.
  return MIN3(studied_meta_scn, safe_meta_scn, real_min_reserved_meta_scn);
}

bool ObSSGCUtil::is_sstable_block_(const MacroBlockId &id)
{
  // SSTABLE (mini/minor/major/prewarm)
  // is_major check for prewarm type
  return SSObjUtil::is_shared(id) && (SSObjUtil::is_macro(id) || SSObjUtil::is_major(id));
}

int ObSSGCUtil::check_need_double_delete_(
    const uint64_t tenant_id,
    const MacroBlockId &id,
    bool &need_double_delete)
{
  int ret = OB_SUCCESS;
  need_double_delete = false;
  if (!SSObjUtil::is_shared_tablet_sub_meta_in_table(id)) {
    // Only tablet sub-meta can use the tiered-metadata double-delete route.
  } else if (OB_FAIL(check_double_route_ss_meta(tenant_id, need_double_delete))) {
    LOG_WARN("fail to check gc needs double delete ss_meta", KR(ret), K(tenant_id), K(id));
  }
  return ret;
}

int ObSSGCUtil::is_exist_block(const MacroBlockId &block_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (SSObjUtil::is_shared_tablet_sub_meta_in_table(block_id)) {
    const uint64_t tenant_id = MTL_ID();
    // 超时时间由_object_storage_io_timeout控制，这里是单个tiered_meta探测，所以不修改超时时间
    ObObjectStorageTenantGuard guard(
        tenant_id,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id) * 1000LL);
    // TODO(likaiying.lky): once sys-tenant tablet meta switches to double-write,
    // revisit whether existence probing should also check the object storage side
    // in addition to is_tiered_metadata_exist (or call a new unified probe API).
    if (OB_FAIL(ObTieredMetadataObjectManager::is_tiered_metadata_exist(block_id, is_exist))) {
      LOG_WARN("check tablet meta exist failed", KR(ret), K(block_id));
    }
  } else if (OB_FAIL(ObRemoteIoAdapter::is_exist(
      MTL_ID(), block_id, GCONF.observer_id, is_exist))) {
    LOG_WARN("check block exist failed", KR(ret), K(block_id));
  }
  return ret;
}

int ObSSGCUtil::batch_delete_blocks(
    const uint64_t tenant_id,
    const ObIArray<MacroBlockId> &block_ids,
    int64_t &sstable_macro_cnt,
    int64_t &tablet_sub_meta_cnt)
{
  int ret = OB_SUCCESS;
  sstable_macro_cnt = 0;
  tablet_sub_meta_cnt = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (block_ids.empty()) {
    // empty list, just return success
    LOG_INFO("empty block_ids, skip", K(tenant_id));
  } else {
    // SSTABLE (mini/minor/major/prewarm) is always single-delete. tablet_meta is
    // single-delete on user/meta tenants and double-delete on the sys tenant.
    ObSEArray<MacroBlockId, OB_DEFAULT_SE_ARRAY_COUNT> sstable_single;
    ObSEArray<MacroBlockId, OB_DEFAULT_SE_ARRAY_COUNT> tablet_meta_single;
    ObSEArray<MacroBlockId, OB_DEFAULT_SE_ARRAY_COUNT> tablet_meta_double;
    sstable_single.set_attr(ObMemAttr(tenant_id, "GCSSTSingle"));
    tablet_meta_single.set_attr(ObMemAttr(tenant_id, "GCTMSingle"));
    tablet_meta_double.set_attr(ObMemAttr(tenant_id, "GCTMDouble"));
    bool has_checked_tablet_meta_delete = false;
    bool need_double_delete = false;

    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
      const MacroBlockId &id = block_ids.at(i);
      if (is_sstable_block_(id)) {
        if (OB_FAIL(sstable_single.push_back(id))) {
          LOG_WARN("fail to push sstable id", KR(ret), K(id));
        }
      } else if (SSObjUtil::is_shared_tablet_sub_meta_in_table(id)) {
        if (!has_checked_tablet_meta_delete) {
          if (OB_FAIL(check_need_double_delete_(tenant_id, id, need_double_delete))) {
            LOG_WARN("failed to classify tablet metadata delete",
                     KR(ret), K(tenant_id), K(id));
          } else {
            has_checked_tablet_meta_delete = true;
          }
        }
        if (OB_SUCCESS != ret) {
        } else if (need_double_delete) {
          if (OB_FAIL(tablet_meta_double.push_back(id))) {
            LOG_WARN("fail to push tablet meta double id", KR(ret), K(id));
          }
        } else {
          if (OB_FAIL(tablet_meta_single.push_back(id))) {
            LOG_WARN("fail to push tablet meta single id", KR(ret), K(id));
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported storage object type for batch delete",
            KR(ret), "type", id.storage_object_type(), K(id));
      }
    }

    if (OB_SUCC(ret) && !sstable_single.empty()) {
      if (OB_FAIL(ObRemoteIoAdapter::batch_del_files(
          tenant_id, sstable_single, GCONF.observer_id))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          LOG_INFO("some sstable blocks do not exist, skip",
              K(tenant_id), "count", sstable_single.count());
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to batch del sstable blocks", KR(ret), K(tenant_id),
              "count", sstable_single.count());
        }
      }
    }

    // 超时时间取 TIERED_METADATA_IO_TIMEOUT_US 与租户配置
    // _shared_gc_sql_timeout 两者的较大值。
    int64_t sql_timeout_us = 0;
    if (OB_SUCC(ret) && OB_FAIL(get_shared_gc_sql_timeout_us(tenant_id, sql_timeout_us))) {
      LOG_WARN("fail to get shared gc sql timeout", KR(ret), K(tenant_id));
    }
    const int64_t tablet_meta_io_timeout_us =
        MAX(TIERED_METADATA_IO_TIMEOUT_US, sql_timeout_us);
    ObObjectStorageTenantGuard tablet_meta_guard(
        tenant_id,
        tablet_meta_io_timeout_us);

    if (OB_SUCC(ret) && !tablet_meta_single.empty()) {
      if (OB_FAIL(ObTieredMetadataObjectManager::batch_del_tiered_metadata(tablet_meta_single))) {
        LOG_WARN("fail to batch del tablet meta tiered metadata", KR(ret), K(tenant_id),
            "count", tablet_meta_single.count());
      }
    }

    if (OB_SUCC(ret) && !tablet_meta_double.empty()) {
      // TODO(likaiying.lky): Step 1: call batch_del_tiered_metadata() above for
      // the tiered-metadata side. sys tenant now use single-delete for tablet meta because
      // sys tenant now use single-write API.
      // TODO(likaiying.lky): Step 2: call the sys-tenant tablet meta
      // double-delete API once it is ready to remove the inner-table row.
      if (OB_FAIL(ObTieredMetadataObjectManager::batch_del_tiered_metadata(tablet_meta_double))) {
        LOG_WARN("fail to batch del tablet meta tiered metadata", KR(ret), K(tenant_id),
            "count", tablet_meta_double.count());
      }
    }
    // TODO(likaiying.lky): Step 3: The first-level meta calls the deletion interface, 
    // the ObSSMetaService deletion interface, and waits for the interface to be ready.
    if (OB_SUCC(ret)) {
      sstable_macro_cnt = sstable_single.count();
      tablet_sub_meta_cnt = tablet_meta_single.count() + tablet_meta_double.count();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

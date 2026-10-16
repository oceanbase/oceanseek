/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/logical_table_monitor.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_multi_tenant.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "lib/hash_func/murmur_hash.h"
#include "share/ob_unit_getter.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;

const int64_t ObLogicalTableMonitor::LOGICAL_TABLE_ROW_COUNT_LIMIT;
const int64_t ObLogicalTableMonitor::LOGICAL_TABLE_STORAGE_SIZE_LIMIT;
const int64_t ObLogicalTableMonitor::STATS_STALE_TTL_SECONDS;

namespace
{
// Saturating non-negative add: row_count and cross-ltable accumulation may
// overflow int64; an overflow would flip the sign and break limit checks, so
// clamp to INT64_MAX (all estimates are non-negative by construction).
int64_t sat_add_nonneg(const int64_t a, const int64_t b)
{
  int64_t ret = a;
  if (b > 0) {
    ret = (a > INT64_MAX - b) ? INT64_MAX : (a + b);
  }
  return ret;
}

// collection_id / database_name are concatenated directly into SQL (as literals
// or back-quoted identifiers). Both come from session context (@collection_id /
// current database) and may be tampered with by a malicious client, so apply a
// conservative whitelist: only [0-9A-Za-z_] is allowed, rejecting quotes,
// back-quotes, semicolons and other injection characters. A legal collection_id
// is hex and a database name is a valid identifier, both of which pass; callers
// treat a failure as "skip the check" (fail-open, never block writes).
bool is_safe_sql_ident(const common::ObString &s, const int64_t max_len)
{
  bool ok = !s.empty() && s.length() <= max_len;
  for (int64_t i = 0; ok && i < s.length(); ++i) {
    const char c = s.ptr()[i];
    ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
         || (c >= 'A' && c <= 'Z') || c == '_';
  }
  return ok;
}

// Upper bound for identifiers fed into is_safe_sql_ident on the background path
// (covers the longest of database name / collection_id).
const int64_t MAX_SQL_IDENT_LEN = 128;
} // anonymous namespace

ObLogicalTableMonitor::ObLogicalTableMonitor()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    stopped_(false),
    running_rounds_(0),
    last_ttl_scan_ts_(0)
{
}

ObLogicalTableMonitor::~ObLogicalTableMonitor()
{
  destroy();
}

int ObLogicalTableMonitor::init(ObMySQLProxy *sql_proxy, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("logical table monitor already inited", KR(ret));
  } else if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(sql_proxy), K(tenant_id));
  } else {
    sql_proxy_ = sql_proxy;
    tenant_id_ = tenant_id;
    stopped_ = false;
    running_rounds_ = 0;
    is_inited_ = true;
  }
  LOG_TRACE("finish to init logical table monitor", KR(ret), K(tenant_id));
  return ret;
}

int ObLogicalTableMonitor::mtl_init(ObLogicalTableMonitor *&monitor)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr != monitor)) {
    uint64_t tenant_id = lib::current_resource_owner_id();
    ObMySQLProxy &sql_proxy = observer::ObServer::get_instance().get_mysql_proxy();
    if (OB_FAIL(monitor->init(&sql_proxy, tenant_id))) {
      LOG_WARN("fail to init logical table monitor", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObLogicalTableMonitor::mtl_start(ObLogicalTableMonitor *&monitor)
{
  UNUSED(monitor);
  return OB_SUCCESS;
}

void ObLogicalTableMonitor::mtl_stop(ObLogicalTableMonitor *&monitor)
{
  if (OB_NOT_NULL(monitor)) {
    ATOMIC_STORE(&monitor->stopped_, true);
  }
}

void ObLogicalTableMonitor::mtl_wait(ObLogicalTableMonitor *&monitor)
{
  if (OB_NOT_NULL(monitor)) {
    monitor->wait_rounds_drain_();
  }
}

int ObLogicalTableMonitor::run_monitor_round()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("logical table monitor is not inited", KR(ret));
  } else {
    // Bump in-flight count before re-checking stopped_ so mtl_wait cannot observe
    // zero rounds while this thread is still starting a round.
    ATOMIC_FAA(&running_rounds_, 1);
    if (ATOMIC_LOAD(&stopped_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("logical table monitor is stopping, skip round", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(dispatch_monitor_round_())) {
      LOG_WARN("fail to dispatch monitor round", KR(ret), K_(tenant_id));
    } else {
      LOG_TRACE("logical table monitor round finished", KR(ret), K_(tenant_id));
    }
    ATOMIC_FAA(&running_rounds_, -1);
  }
  return ret;
}

int ObLogicalTableMonitor::run_monitor_round_on_local_leaders()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("logical table monitor is not inited", KR(ret));
  } else {
    // Bump in-flight count before re-checking stopped_ so mtl_wait cannot observe
    // zero rounds while this thread is still starting a local round.
    ATOMIC_FAA(&running_rounds_, 1);
    if (ATOMIC_LOAD(&stopped_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("logical table monitor is stopping, skip local round", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(do_monitor_round_(false/*enable_cleanup*/))) {
      LOG_WARN("fail to do monitor round on local leaders", KR(ret), K_(tenant_id));
    } else {
      LOG_TRACE("logical table monitor local round finished", KR(ret), K_(tenant_id));
    }
    ATOMIC_FAA(&running_rounds_, -1);
  }
  return ret;
}

// The stats job is a single-instance DBMS_Scheduler task, while the storage
// estimation can only run on the LS leader. Fan the round out to every observer
// of the tenant so one round covers all leaders; the RPC count scales with the
// observer count, not with the namespace/ltable count.
int ObLogicalTableMonitor::dispatch_monitor_round_()
{
  int ret = OB_SUCCESS;
  int submit_ret = OB_SUCCESS;
  int local_ret = OB_SUCCESS;
  int wait_ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> server_list;
  share::ObUnitInfoGetter unit_getter;
  server_list.set_attr(ObMemAttr(MTL_ID(), "LTMonitorSvrs"));

  if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv rpc proxy in GCTX is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(unit_getter.init(*sql_proxy_, &GCONF))) {
    LOG_WARN("fail to init unit info getter", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(unit_getter.get_tenant_servers(tenant_id_, server_list))) {
    LOG_WARN("fail to get tenant servers", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(server_list.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no server found for tenant", KR(ret), K_(tenant_id));
  } else {
    rootserver::ObGatherLocalLogicalTableStatsProxy proxy(
        *GCTX.srv_rpc_proxy_,
        &obrpc::ObSrvRpcProxy::gather_local_logical_table_stats);
    const obrpc::UInt64 arg(tenant_id_);
    const ObAddr &self = GCTX.self_addr();
    int64_t abs_timeout_us = ObTimeUtility::current_time() + DISPATCH_RPC_TIMEOUT_US;
    if (THIS_WORKER.is_timeout_ts_valid()) {
      abs_timeout_us = std::min(abs_timeout_us, THIS_WORKER.get_timeout_ts());
    }

    // Post to the peers first, then run locally, so all observers estimate in
    // parallel instead of one node waiting for the next.
    for (int64_t i = 0; OB_SUCCESS == submit_ret && i < server_list.count(); ++i) {
      const ObAddr &server = server_list.at(i);
      if (server == self) {
        // handled by the local round below
      } else {
        const int64_t timeout_us = abs_timeout_us - ObTimeUtility::current_time();
        if (timeout_us <= 0) {
          submit_ret = OB_TIMEOUT;
          LOG_WARN("no time left to dispatch stats rpc", KR(submit_ret), K_(tenant_id), K(server));
        } else if (OB_SUCCESS != (submit_ret = proxy.call(server, timeout_us,
                                                          tenant_id_, arg))) {
          LOG_WARN("fail to send gather stats rpc", KR(submit_ret), K_(tenant_id), K(server));
        }
      }
    }

    if (OB_SUCCESS != (local_ret = do_monitor_round_(true/*enable_cleanup*/))) {
      LOG_WARN("fail to do monitor round on scheduling node", KR(local_ret), K_(tenant_id));
    }
    if (OB_SUCCESS != (wait_ret = proxy.wait())) {
      LOG_WARN("fail to wait gather stats rpc", KR(wait_ret), K_(tenant_id));
    }

    // Surface cross-node failures instead of reporting the round as successful.
    if (OB_SUCCESS != submit_ret) {
      ret = submit_ret;
    } else if (OB_SUCCESS != local_ret) {
      ret = local_ret;
    } else if (OB_SUCCESS != wait_ret) {
      ret = wait_ret;
    }
  }
  LOG_TRACE("dispatch_monitor_round_ done", KR(ret), K_(tenant_id),
            K(submit_ret), K(local_ret), K(wait_ret),
            "server_count", server_list.count());
  return ret;
}

void ObLogicalTableMonitor::wait_rounds_drain_()
{
  const int64_t deadline_us = ObTimeUtility::current_time() + DESTROY_DRAIN_TIMEOUT_US;
  while (ATOMIC_LOAD(&running_rounds_) > 0
         && ObTimeUtility::current_time() < deadline_us) {
    ob_usleep(10 * 1000);
  }
  if (ATOMIC_LOAD(&running_rounds_) > 0) {
    LOG_INFO("logical table monitor round drain timeout",
             K_(tenant_id), "running_rounds", ATOMIC_LOAD(&running_rounds_));
  }
}

void ObLogicalTableMonitor::destroy()
{
  ATOMIC_STORE(&stopped_, true);
  wait_rounds_drain_();
  sql_proxy_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

// ==================== Core flow ====================

int ObLogicalTableMonitor::do_monitor_round_(const bool enable_cleanup)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LTMonitor"));
  ObSEArray<ObString, 4> database_names;
  int64_t total_collection_count = 0;
  // The TTL full-table scan is heavy and need not run every round: run it at
  // most once per STATS_TTL_SCAN_INTERVAL_US.
  const int64_t last_ttl_ts = ATOMIC_LOAD(&last_ttl_scan_ts_);
  const bool run_ttl = enable_cleanup
      && ((0 == last_ttl_ts)
          || (start_ts - last_ttl_ts >= STATS_TTL_SCAN_INTERVAL_US));

  if (OB_FAIL(fetch_databases_with_sdk_collections_(allocator, database_names))) {
    LOG_WARN("fail to fetch databases with sdk_collections", KR(ret));
  } else {
    const int64_t db_cnt = database_names.count();
    for (int64_t d = 0; OB_SUCC(ret) && d < db_cnt; ++d) {
      const ObString &database_name = database_names.at(d);
      ObSEArray<ObString, 16> collection_ids;
      if (OB_FAIL(fetch_all_collections_(allocator, database_name, collection_ids))) {
        LOG_WARN("fail to fetch all collections", KR(ret), K(database_name));
      } else {
        const int64_t collection_cnt = collection_ids.count();
        total_collection_count += collection_cnt;
        // database_name / collection_id are concatenated into SQL downstream, so
        // apply the same [0-9A-Za-z_] whitelist the user-facing check path uses;
        // skip (best-effort) any identifier that fails instead of blocking.
        if (OB_SUCC(ret) && !is_safe_sql_ident(database_name, MAX_SQL_IDENT_LEN)) {
          LOG_WARN("unsafe database_name, skip collection stats round", K(database_name));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < collection_cnt; ++i) {
            const ObString &collection_id = collection_ids.at(i);
            int tmp_ret = OB_SUCCESS;
            if (!is_safe_sql_ident(collection_id, MAX_SQL_IDENT_LEN)) {
              LOG_WARN("unsafe collection_id, skip", K(database_name), K(collection_id), K(i));
            } else if (OB_TMP_FAIL(process_single_collection_(database_name, collection_id,
                                                              enable_cleanup))) {
              LOG_WARN("fail to process collection, skip",
                       KR(tmp_ret), K(database_name), K(collection_id), K(i), K(collection_cnt));
            }
          }
        }
        // Best-effort TTL cleanup based on last_gather_time, run at hourly
        // granularity (to reduce full-table scan pressure); never blocks the
        // main flow.
        if (OB_SUCC(ret) && run_ttl) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(cleanup_expired_stats_(database_name))) {
            LOG_WARN("fail to ttl-cleanup expired stats, ignored", KR(tmp_ret), K(database_name));
          }
        }
      }
    }
    if (run_ttl) {
      ATOMIC_STORE(&last_ttl_scan_ts_, start_ts);
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
  LOG_TRACE("do_monitor_round_ done",
            KR(ret), K_(tenant_id), K(cost_us), K(enable_cleanup),
            "database_count", database_names.count(),
            "total_collection_count", total_collection_count);
  return ret;
}

int ObLogicalTableMonitor::process_single_collection_(const ObString &database_name,
                                                      const ObString &collection_id,
                                                      const bool enable_cleanup)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 64> namespace_ids;
  ObSEArray<ObNamespaceStatRow, BATCH_UPDATE_SIZE> batch_rows;
  namespace_ids.set_attr(ObMemAttr(MTL_ID(), "LTMonitorNsIds"));

  if (OB_FAIL(fetch_namespace_ids_(database_name, collection_id, namespace_ids))) {
    LOG_WARN("fail to fetch namespace ids", KR(ret), K(database_name), K(collection_id));
  } else {
    const int64_t ns_cnt = namespace_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < ns_cnt; ++i) {
      const uint64_t namespace_id = namespace_ids.at(i);
      ObSEArray<uint64_t, 32> ltable_ids;
      ltable_ids.set_attr(ObMemAttr(MTL_ID(), "LTMonitorLtIds"));

      if (OB_FAIL(fetch_ltable_ids_(database_name, collection_id, namespace_id, ltable_ids))) {
        LOG_WARN("fail to fetch ltable ids", KR(ret), K(database_name), K(collection_id), K(namespace_id));
      } else {
        const int64_t lt_cnt = ltable_ids.count();
        int64_t ns_primary_rows = 0, ns_primary_bytes = 0, ns_primary_cnt = 0;
        int64_t ns_all_rows = 0, ns_all_bytes = 0, ns_all_cnt = 0;
        for (int64_t j = 0; j < lt_cnt; ++j) {
          int64_t primary_rows = 0, primary_bytes = 0, all_rows = 0, all_bytes = 0;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(estimate_ltable_primary_and_all_(database_name, collection_id, namespace_id,
                                                           ltable_ids.at(j), primary_rows, primary_bytes,
                                                           all_rows, all_bytes))) {
            LOG_WARN("fail to estimate single ltable, skip",
                     KR(tmp_ret), K(database_name), K(collection_id), K(namespace_id),
                     K(ltable_ids.at(j)));
          } else {
            if (primary_rows > 0 || primary_bytes > 0) {
              ns_primary_rows = sat_add_nonneg(ns_primary_rows, primary_rows);
              ns_primary_bytes = sat_add_nonneg(ns_primary_bytes, primary_bytes);
              ++ns_primary_cnt;
            }
            if (all_rows > 0 || all_bytes > 0) {
              ns_all_rows = sat_add_nonneg(ns_all_rows, all_rows);
              ns_all_bytes = sat_add_nonneg(ns_all_bytes, all_bytes);
              ++ns_all_cnt;
            }
          }
        } // end for ltable_ids

        if (OB_SUCC(ret) && (ns_primary_cnt > 0 || ns_all_cnt > 0)) {
          ObNamespaceStatRow row;
          row.namespace_id_ = namespace_id;
          row.row_count_ = ns_primary_rows;
          row.total_size_ = ns_primary_bytes;
          row.total_size_with_index_ = ns_all_bytes;
          if (OB_FAIL(batch_rows.push_back(row))) {
            LOG_WARN("fail to push back namespace stat row", KR(ret), K(namespace_id));
          } else if (batch_rows.count() >= BATCH_UPDATE_SIZE) {
            if (OB_FAIL(update_namespace_stat_(database_name, collection_id, batch_rows))) {
              LOG_WARN("fail to update namespace stat", KR(ret), K(database_name), K(collection_id));
            } else {
              batch_rows.reuse();
            }
          }
        }
      }
    } // end for namespace_ids

    if (OB_SUCC(ret) && batch_rows.count() > 0) {
      if (OB_FAIL(update_namespace_stat_(database_name, collection_id, batch_rows))) {
        LOG_WARN("fail to update remaining namespace stat", KR(ret), K(database_name), K(collection_id));
      }
    }

    // Clean up orphan stat rows: drop_namespace already cleans them on the delete
    // path, this is a best-effort fallback that removes rows outside the active
    // set (current namespaces in hot_table); never blocks the main flow. Only the
    // dispatching node cleans up, so the remote rounds cannot delete rows that
    // another node is about to write.
    if (OB_SUCC(ret) && enable_cleanup) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(cleanup_stale_namespace_stats_(database_name, collection_id, namespace_ids))) {
        LOG_WARN("fail to cleanup stale namespace stats, ignored",
                 KR(tmp_ret), K(database_name), K(collection_id));
      }
    }
  }

  LOG_TRACE("process_single_collection_ done",
            KR(ret), K(database_name), K(collection_id),
            "namespace_count", namespace_ids.count());
  return ret;
}

int ObLogicalTableMonitor::cleanup_stale_namespace_stats_(const ObString &database_name,
                                                          const ObString &collection_id,
                                                          const ObIArray<uint64_t> &active_namespace_ids)
{
  int ret = OB_SUCCESS;
  const int64_t ns_cnt = active_namespace_ids.count();
  // When the active set is too large the NOT IN list becomes very long; skip the
  // fallback cleanup and rely on synchronous cleanup on the drop path instead.
  const int64_t MAX_ACTIVE_IDS_FOR_CLEANUP = 4096;
  if (ns_cnt > MAX_ACTIVE_IDS_FOR_CLEANUP) {
    LOG_INFO("[LT_MONITOR] too many active namespaces, skip stale stats cleanup",
             K(database_name), K(collection_id), K(ns_cnt));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(
            "DELETE FROM `%.*s`.`sdk_namespace_stat` WHERE collection_id = '%.*s'",
            database_name.length(), database_name.ptr(),
            collection_id.length(), collection_id.ptr()))) {
      LOG_WARN("fail to build delete sql head", KR(ret), K(database_name), K(collection_id));
    } else if (ns_cnt > 0) {
      if (OB_FAIL(sql.append_fmt(" AND namespace_id NOT IN ("))) {
        LOG_WARN("fail to append not in head", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ns_cnt; ++i) {
        if (OB_FAIL(sql.append_fmt("%s%lu", (0 == i) ? "" : ", ", active_namespace_ids.at(i)))) {
          LOG_WARN("fail to append namespace id", KR(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("fail to append not in tail", KR(ret));
      }
    }
    // ns_cnt == 0: the collection has no active namespace left, delete all its stat rows.
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // old SDK versions have not created the stats table, skip
        } else {
          LOG_WARN("fail to delete stale namespace stats", KR(ret), K(sql));
        }
      } else if (affected_rows > 0) {
        LOG_INFO("[LT_MONITOR] cleaned stale namespace stats",
                 K(database_name), K(collection_id), K(ns_cnt), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::cleanup_expired_stats_(const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%.*s`.`sdk_namespace_stat` "
          "WHERE last_gather_time < DATE_SUB(NOW(6), INTERVAL %ld SECOND)",
          database_name.length(), database_name.ptr(),
          STATS_STALE_TTL_SECONDS))) {
    LOG_WARN("fail to build ttl delete sql", KR(ret), K(database_name));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // old SDK versions have not created the stats table, skip
    } else {
      LOG_WARN("fail to ttl-delete expired stats", KR(ret), K(sql));
    }
  } else if (affected_rows > 0) {
    LOG_INFO("[LT_MONITOR] TTL-purged expired stats rows",
             K(database_name), K(affected_rows), "ttl_seconds", STATS_STALE_TTL_SECONDS);
  }
  return ret;
}

// ==================== Step 0: fetch databases that contain sdk_collections ====================

int ObLogicalTableMonitor::fetch_databases_with_sdk_collections_(ObIAllocator &allocator,
                                                                 ObIArray<ObString> &database_names)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT d.database_name FROM oceanbase.%s t "
      "JOIN oceanbase.%s d "
      "ON t.database_id = d.database_id "
      "WHERE t.table_name = 'sdk_collections'",
      OB_ALL_TABLE_TNAME, OB_ALL_DATABASE_TNAME))) {
    LOG_WARN("fail to build sql", KR(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          ObString tmp_name;
          ObString stored_name;
          EXTRACT_VARCHAR_FIELD_MYSQL(res, "database_name", tmp_name);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ob_write_string(allocator, tmp_name, stored_name))) {
            LOG_WARN("fail to deep copy database name", KR(ret), K(tmp_name));
          } else if (OB_FAIL(database_names.push_back(stored_name))) {
            LOG_WARN("fail to push back database name", KR(ret), K(stored_name));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_TRACE("fetch_databases_with_sdk_collections_ done", KR(ret), K_(tenant_id),
           "count", database_names.count());
  return ret;
}

// ==================== Step 1: fetch all collections under the given database ====================

int ObLogicalTableMonitor::fetch_all_collections_(ObIAllocator &allocator,
                                                  const ObString &database_name,
                                                  ObIArray<ObString> &collection_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT collection_id FROM `%.*s`.sdk_collections",
      database_name.length(), database_name.ptr()))) {
    LOG_WARN("fail to build sql", KR(ret), K(database_name));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          ObString tmp_id;
          ObString stored_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(res, "collection_id", tmp_id);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ob_write_string(allocator, tmp_id, stored_id))) {
            LOG_WARN("fail to deep copy collection id", KR(ret), K(tmp_id));
          } else if (OB_FAIL(collection_ids.push_back(stored_id))) {
            LOG_WARN("fail to push back collection id", KR(ret), K(stored_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_TRACE("fetch_all_collections_ done", KR(ret), K(database_name),
           "count", collection_ids.count());
  return ret;
}

// ==================== Step 2: fetch the namespace_id list ====================

int ObLogicalTableMonitor::fetch_namespace_ids_(const ObString &database_name,
                                                const ObString &collection_id,
                                                ObIArray<uint64_t> &namespace_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (GCTX.is_shared_storage_mode()) {
    // SS mode: scan active namespace_id from <collection>_hot_table
    if (OB_FAIL(sql.append_fmt(
        "SELECT DISTINCT namespace_id FROM `%.*s`.`%.*s_hot_table`",
        database_name.length(), database_name.ptr(),
        collection_id.length(), collection_id.ptr()))) {
      LOG_WARN("fail to build sql", KR(ret), K(database_name), K(collection_id));
    }
  } else {
    // SN mode: fetch all namespace_id under the collection from the sdk_namespaces table
    if (OB_FAIL(sql.append_fmt(
        "SELECT namespace_id FROM `%.*s`.sdk_namespaces WHERE collection_id = '%.*s'",
        database_name.length(), database_name.ptr(),
        collection_id.length(), collection_id.ptr()))) {
      LOG_WARN("fail to build sql", KR(ret), K(database_name), K(collection_id));
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          uint64_t namespace_id = OB_INVALID_ID;
          EXTRACT_UINT_FIELD_MYSQL(res, "namespace_id", namespace_id, uint64_t);
          if (FAILEDx(namespace_ids.push_back(namespace_id))) {
            LOG_WARN("fail to push back namespace_id", KR(ret), K(namespace_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_DEBUG("fetch_namespace_ids_ done", KR(ret), K(database_name), K(collection_id),
            "count", namespace_ids.count());
  return ret;
}

// ==================== Step 3: fetch the ltable_id list ====================

int ObLogicalTableMonitor::fetch_ltable_ids_(const ObString &database_name,
                                             const ObString &collection_id,
                                             const uint64_t namespace_id,
                                             ObIArray<uint64_t> &ltable_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT ltable_id FROM `%.*s`.`%.*s_logic_schema_table` WHERE namespace_id = %lu",
      database_name.length(), database_name.ptr(),
      collection_id.length(), collection_id.ptr(), namespace_id))) {
    LOG_WARN("fail to build sql", KR(ret), K(database_name), K(collection_id), K(namespace_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        while (OB_SUCC(ret) && OB_SUCC(res.next())) {
          uint64_t ltable_id = OB_INVALID_ID;
          EXTRACT_UINT_FIELD_MYSQL(res, "ltable_id", ltable_id, uint64_t);
          if (FAILEDx(ltable_ids.push_back(ltable_id))) {
            LOG_WARN("fail to push back ltable_id", KR(ret), K(ltable_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_DEBUG("fetch_ltable_ids_ done", KR(ret), K(database_name), K(collection_id),
            K(namespace_id), "count", ltable_ids.count());
  return ret;
}

// ==================== Helper: build the logical-table rowkey range ====================
//
// Physical KV table Key = [ns_id(8B)][lt_id(8B)][index_seq(8B)][aux_type(1B)][payload...]
// Use ObLogicalTableEncoder::encode_key to encode the prefix (empty datums = prefix only).
// end_prefix encodes the next lt_id (or the index upper bound) directly, producing the
// half-open interval [start_prefix, end_prefix).

// PK of kv_data_table is (namespace_id BIGINT UNSIGNED, kv_key VARCHAR)
static int build_kv_table_store_rowkey(ObIAllocator &allocator,
                                       const uint64_t namespace_id,
                                       const ObString &kv_key,
                                       ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;
  if (OB_ISNULL(objs = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc rowkey objs", KR(ret));
  } else {
    objs[0].reset();
    objs[0].set_uint64(namespace_id);
    objs[1].reset();
    objs[1].set_varchar(kv_key);
    objs[1].set_collation_type(CS_TYPE_BINARY);
    if (OB_FAIL(rowkey.assign(objs, 2))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    }
  }
  return ret;
}

int ObLogicalTableMonitor::get_logical_table_bounds(ObIAllocator &allocator,
                                                    const uint64_t namespace_id,
                                                    const uint64_t ltable_id,
                                                    const bool is_included_index,
                                                    const uint64_t logic_data_table_id,
                                                    ObStoreRange &range)
{
  int ret = OB_SUCCESS;
  range.reset();

  ObSEArray<ObDatum, 1> empty_datums;
  ObString start_encoded;
  ObString end_encoded;

  // start key: encode prefix (ns_id, lt_id, index_seq=0, aux_type=PRIMARY) with no payload
  ObLogicalTableKeyPrefix start_prefix(namespace_id, ltable_id, 0, LT_AUX_PRIMARY);
  ObLogicalTableKeyPrefix end_prefix;
  end_prefix.namespace_id_ = namespace_id;
  if (OB_FAIL(ObLogicalTableEncoder::encode_key(
          start_prefix, empty_datums, allocator, start_encoded))) {
    LOG_WARN("fail to encode start prefix", KR(ret), K(namespace_id), K(ltable_id));
  } else if (!is_included_index) {
    end_prefix.logical_table_id_ = ltable_id;
    end_prefix.aux_type_ = LT_AUX_SEARCH_INDEX;
    end_prefix.index_seq_ = logic_data_table_id;
  } else {
    // all data for this logical table: end prefix = (ns_id, lt_id+1, 0, PRIMARY)
    end_prefix.logical_table_id_ = ltable_id + 1;
    end_prefix.aux_type_ = LT_AUX_PRIMARY;
    end_prefix.index_seq_ = 0;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(
      end_prefix, empty_datums, allocator, end_encoded))) {
    LOG_WARN("fail to encode end prefix", KR(ret), K(namespace_id), K(ltable_id));
  }

  if (OB_SUCC(ret)) {
    ObStoreRowkey start_rowkey;
    ObStoreRowkey end_rowkey;
    if (OB_FAIL(build_kv_table_store_rowkey(allocator, namespace_id, start_encoded, start_rowkey))) {
      LOG_WARN("fail to build start rowkey", KR(ret));
    } else if (OB_FAIL(build_kv_table_store_rowkey(allocator, namespace_id, end_encoded, end_rowkey))) {
      LOG_WARN("fail to build end rowkey", KR(ret));
    } else {
      range.set_start_key(start_rowkey);
      range.set_end_key(end_rowkey);
      range.set_left_closed();
      range.set_right_closed(); // we have already set index_seq_ to 0 which is encoded as table id for normal data
                                // so it is safe to set closed
    }
  }

  return ret;
}

// ==================== Helper: resolve tablet_id and ls_id ====================

int ObLogicalTableMonitor::resolve_tablet_and_ls_(const ObString &database_name,
                                                  const ObString &collection_id,
                                                  const uint64_t namespace_id,
                                                  ObTabletID &tablet_id,
                                                  share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  ls_id.reset();

  // Step 1a: query database_id
  uint64_t database_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_FAIL(fetch_database_id_by_name_(database_name, database_id))) {
    LOG_WARN("fail to fetch database id by name", KR(ret), K(database_name));
  }
  LOG_TRACE("[LTMonitor] step1a: got database_id", KR(ret), K(database_name), K(database_id));

  // Step 1b: query the table_id of kv_data_table
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(
        "SELECT table_id FROM oceanbase.%s "
        "WHERE table_name = '%.*s_kv_data_table' AND database_id = %lu",
        OB_ALL_TABLE_TNAME,
        collection_id.length(), collection_id.ptr(), database_id))) {
      LOG_WARN("fail to build sql", KR(ret), K(database_name), K(collection_id));
    } else {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", KR(ret), K(sql));
        } else {
          sqlclient::ObMySQLResult &res = *result.get_result();
          if (OB_FAIL(res.next())) {
            if (OB_ITER_END == ret) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("kv_data_table not found", KR(ret), K(database_name), K(collection_id));
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
          }
        }
      }
    }
  }
  LOG_TRACE("[LTMonitor] step1b: got table_id", KR(ret), K(database_name), K(collection_id),
           K(database_id), K(table_id));

  // Step 2: compute the partition -> tablet_id for namespace_id via schema
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard guard;
    const ObSimpleTableSchemaV2 *table_schema = nullptr;
    schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null", KR(ret), K(table_id));
    } else if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
      tablet_id = table_schema->get_tablet_id();
      LOG_TRACE("[LTMonitor] step2: non-partitioned table",
               K(table_id), K(tablet_id));
    } else if (PARTITION_LEVEL_ONE == table_schema->get_part_level()) {
      int64_t part_num = table_schema->get_first_part_num();
      const int64_t part_level = table_schema->get_part_level();
      const ObPartitionFuncType part_func_type = table_schema->get_part_option().get_part_func_type();
      LOG_TRACE("[LTMonitor] step2: partition info",
               K(table_id), K(namespace_id), K(part_level), K(part_func_type), K(part_num));
      if (part_num <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part num", KR(ret), K(part_num), K(table_id));
      } else {
        int64_t hash_val = 0;
        if (share::schema::is_key_part(part_func_type)) {
          // KEY partition: murmurhash on the raw bytes of namespace_id, then abs
          uint64_t murmur_val = common::murmurhash(&namespace_id, sizeof(namespace_id), 0);
          hash_val = static_cast<int64_t>(murmur_val);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported yet", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else {
          if (INT64_MIN == hash_val) {
            hash_val = INT64_MAX;
          } else if (hash_val < 0) {
            hash_val = -hash_val;
          }
          int64_t part_idx = OB_INVALID_INDEX;
          if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(
                  static_cast<uint64_t>(hash_val), part_num, part_idx))) {
            LOG_WARN("fail to calc hash part idx", KR(ret), K(hash_val), K(part_num));
          } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= part_num)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid part idx", KR(ret), K(part_idx), K(part_num));
          } else {
            ObPartition **part_array = table_schema->get_part_array();
            if (OB_ISNULL(part_array) || OB_ISNULL(part_array[part_idx])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part array is null", KR(ret), K(part_idx));
            } else {
              tablet_id = part_array[part_idx]->get_tablet_id();
              LOG_TRACE("[LTMonitor] step2: calc partition result",
                       K(namespace_id), K(hash_val), K(part_num), K(part_idx),
                       K(part_func_type),
                       "part_id", part_array[part_idx]->get_part_id(),
                       K(tablet_id));
            }
          }
        }
        
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported yet", K(ret));
    }
  }

  // Step 3: query the ls_id corresponding to tablet_id
  if (OB_SUCC(ret) && tablet_id.is_valid()) {
    ObSEArray<ObTabletID, 1> tablet_ids;
    ObSEArray<ObTabletLSPair, 1> pairs;
    if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to push back tablet_id", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_tablet_ls_pairs(
                   *sql_proxy_, tenant_id_, tablet_ids, pairs))) {
      LOG_WARN("fail to get tablet ls pairs", KR(ret), K(tablet_id));
    } else if (pairs.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no ls found for tablet", KR(ret), K(tablet_id));
    } else {
      ls_id = pairs.at(0).get_ls_id();
    }
  }
  LOG_TRACE("[LTMonitor] resolve_tablet_and_ls_ done", KR(ret), K(database_name), K(collection_id),
            K(namespace_id), K(database_id), K(table_id), K(tablet_id), K(ls_id));
  return ret;
}

int ObLogicalTableMonitor::fetch_kv_data_table_id_(const uint64_t database_id,
                                                   const ObString &collection_id,
                                                   uint64_t &kv_data_table_id)
{
  int ret = OB_SUCCESS;
  kv_data_table_id = OB_INVALID_ID;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT table_id FROM oceanbase.%s "
      "WHERE table_name = '%.*s_kv_data_table' AND database_id = %lu",
      OB_ALL_TABLE_TNAME,
      collection_id.length(), collection_id.ptr(), database_id))) {
    LOG_WARN("fail to build sql", KR(ret), K(collection_id), K(database_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("kv_data_table not found", KR(ret), K(collection_id), K(database_id));
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(res, "table_id", kv_data_table_id, uint64_t);
        }
      }
    }
  }
  return ret;
}

namespace
{
int estimate_ltable_row_count(storage::ObLSTabletService *tablet_svr,
                            const ObTabletID &tablet_id,
                            const uint64_t kv_data_table_id,
                            const int64_t timeout_ts,
                            const ObStoreRange &store_range,
                            int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  storage::ObPartitionEst batch_est;
  ObSEArray<ObEstRowCountRecord, 2> est_records;
  storage::ObTabletTableIterator tablet_iter;
  ObSEArray<storage::ObITable*, 4> tables;
  const int64_t snapshot_version = storage::GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
  const int64_t timeout_us = timeout_ts - ObTimeUtility::current_time();

  if (OB_ISNULL(tablet_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet service is null", KR(ret));
  } else if (0 >= timeout_us) {
    ret = OB_TIMEOUT;
    LOG_INFO("row count estimate timed out before getting read tables", KR(ret), K(timeout_ts));
  } else if (OB_FAIL(tablet_svr->get_read_tables(tablet_id,
                                                 timeout_us,
                                                 snapshot_version,
                                                 snapshot_version,
                                                 tablet_iter,
                                                 false/*allow_no_ready_read*/,
                                                 true/*need_split_src_table*/,
                                                 true/*need_split_dst_table*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get read tables for row count estimate", KR(ret), K(tablet_id));
    }
  } else {
    int64_t major_version = -1;
    while (OB_SUCC(ret)) {
      storage::ObITable *table = nullptr;
      if (OB_FAIL(tablet_iter.table_iter()->get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next table", KR(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", KR(ret));
      } else if (table->is_sstable()) {
        const blocksstable::ObSSTable *sstable = static_cast<const blocksstable::ObSSTable*>(table);
        if (sstable->is_major_sstable()) {
          major_version = sstable->get_data_version();
        } else if (table->get_upper_trans_version() <= major_version) {
          continue;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (table->no_data_to_read()) {
        continue;
      } else if (OB_FAIL(tables.push_back(table))) {
        LOG_WARN("fail to push back table", KR(ret));
      }
    }

    if (OB_SUCC(ret) && tables.count() > 0) {
      ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LTRowEst"));
      blocksstable::ObDatumRange datum_range;
      ObSEArray<blocksstable::ObDatumRange, 1> datum_ranges;
      common::ObQueryFlag scan_flag;
      scan_flag.read_latest_ = true;
      storage::ObTableEstimateBaseInput base_input(scan_flag,
                                                 kv_data_table_id,
                                                 transaction::ObTransID(),
                                                 tables,
                                                 tablet_iter.get_tablet_handle(),
                                                 major_version);
      if (OB_FAIL(datum_range.from_range(store_range, allocator))) {
        LOG_WARN("fail to transform store range to datum range", KR(ret), K(store_range));
      } else if (OB_FAIL(datum_ranges.push_back(datum_range))) {
        LOG_WARN("fail to push back datum range", KR(ret));
      } else if (OB_FAIL(storage::ObTableEstimator::estimate_row_count_for_scan(
                     base_input, datum_ranges, batch_est, est_records))) {
        LOG_WARN("fail to estimate row count for scan", KR(ret), K(tablet_id), K(kv_data_table_id));
      } else {
        row_count = batch_est.logical_row_count_;
        if (row_count < 0) {
          row_count = 0;
        } else if (0 == batch_est.physical_row_count_ && 1 == row_count) {
          // Estimator clamps negative logical counts to 1; treat empty as 0.
          row_count = 0;
        }
      }
    }
  }
  return ret;
}
} // namespace

// ==================== Step 4: estimate row count and data size of a single logical table ====================

int ObLogicalTableMonitor::estimate_ltable_primary_and_all_(const ObString &database_name,
                                                            const ObString &collection_id,
                                                            const uint64_t namespace_id,
                                                            const uint64_t ltable_id,
                                                            int64_t &primary_rows,
                                                            int64_t &primary_bytes,
                                                            int64_t &all_rows,
                                                            int64_t &all_bytes)
{
  int ret = OB_SUCCESS;
  primary_rows = 0;
  primary_bytes = 0;
  all_rows = 0;
  all_bytes = 0;

  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LTEstimate"));
  ObTabletID tablet_id;
  share::ObLSID ls_id;
  uint64_t logic_data_table_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t kv_data_table_id = OB_INVALID_ID;

  if (OB_FAIL(resolve_tablet_and_ls_(database_name, collection_id, namespace_id,
                                     tablet_id, ls_id))) {
    LOG_WARN("fail to resolve tablet and ls", KR(ret), K(database_name), K(collection_id),
             K(namespace_id));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(fetch_database_id_by_name_(database_name, database_id))) {
      LOG_WARN("fail to fetch database id by name", KR(ret), K(database_name));
    } else if (OB_FAIL(fetch_logic_data_table_id_(database_id, collection_id, logic_data_table_id))) {
      LOG_WARN("fail to fetch logic_data_table_id", KR(ret), K(database_id), K(collection_id));
    } else if (OB_FAIL(fetch_kv_data_table_id_(database_id, collection_id, kv_data_table_id))) {
      LOG_WARN("fail to fetch kv_data_table_id", KR(ret), K(database_id), K(collection_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!tablet_id.is_valid() || !ls_id.is_valid()) {
    LOG_DEBUG("tablet or ls not resolved (stub), skip estimation",
              K(collection_id), K(namespace_id), K(tablet_id), K(ls_id));
  } else {
    storage::ObLSHandle ls_handle;
    storage::ObLSService *ls_service = MTL(storage::ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, storage::ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id));
    } else {
      ObRole role;
      if (OB_FAIL(ls_handle.get_ls()->get_ls_role(role))) {
        LOG_WARN("fail to get ls role", KR(ret), K(ls_id));
      } else if (ObRole::LEADER != role) {
        LOG_DEBUG("current node is not ls leader, skip estimation", K(ls_id), K(role));
      } else {
        storage::ObLSTabletService *tablet_svr = ls_handle.get_ls()->get_tablet_svr();
        if (OB_ISNULL(tablet_svr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet service is null", KR(ret), K(ls_id));
        } else {
          const int64_t timeout_ts = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
          ObStoreRange primary_range;
          if (OB_FAIL(get_logical_table_bounds(allocator, namespace_id, ltable_id,
                                               false/*is_included_index*/, logic_data_table_id, primary_range))) {
            LOG_WARN("fail to get primary bounds", KR(ret), K(namespace_id), K(ltable_id));
          } else {
            ObSEArray<ObStoreRange, 1> ranges;
            storage::ObMultiRangeEstimation est;
            if (OB_FAIL(estimate_ltable_row_count(tablet_svr, tablet_id, kv_data_table_id,
                                                   timeout_ts, primary_range, primary_rows))) {
              LOG_WARN("fail to estimate primary row count", KR(ret),
                       K(tablet_id), K(namespace_id), K(ltable_id));
            } else if (OB_FAIL(ranges.push_back(primary_range))) {
              LOG_WARN("fail to push back range", KR(ret));
            } else if (OB_FAIL(tablet_svr->get_multi_ranges_size_and_row_count_precise(
                           tablet_id, timeout_ts, ranges, est))) {
              LOG_WARN("fail to get multi ranges size for primary", KR(ret),
                       K(tablet_id), K(namespace_id), K(ltable_id));
            } else {
              primary_bytes = est.total_size_;
            }
          }

          if (OB_SUCC(ret)) {
            ObStoreRange all_range;
            if (OB_FAIL(get_logical_table_bounds(allocator, namespace_id, ltable_id,
                                                 true/*is_included_index*/, logic_data_table_id, all_range))) {
              LOG_WARN("fail to get all bounds", KR(ret), K(namespace_id), K(ltable_id));
            } else {
              ObSEArray<ObStoreRange, 1> ranges;
              storage::ObMultiRangeEstimation est;
              if (OB_FAIL(estimate_ltable_row_count(tablet_svr, tablet_id, kv_data_table_id,
                                                     timeout_ts, all_range, all_rows))) {
                LOG_WARN("fail to estimate all row count", KR(ret),
                         K(tablet_id), K(namespace_id), K(ltable_id));
              } else if (OB_FAIL(ranges.push_back(all_range))) {
                LOG_WARN("fail to push back range", KR(ret));
              } else if (OB_FAIL(tablet_svr->get_multi_ranges_size_and_row_count_precise(
                             tablet_id, timeout_ts, ranges, est))) {
                LOG_WARN("fail to get multi ranges size for all", KR(ret),
                         K(tablet_id), K(namespace_id), K(ltable_id));
              } else {
                all_bytes = est.total_size_;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// ==================== Step 5: update the sdk_namespace_stat table ====================

int ObLogicalTableMonitor::update_namespace_stat_(const ObString &database_name,
                                                  const ObString &collection_id,
                                                  const ObIArray<ObNamespaceStatRow> &rows)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = rows.count();
  if (cnt <= 0) {
    // nothing to update
  } else {
    ObSqlString sql;
    int64_t valid_cnt = 0;
    if (OB_FAIL(sql.append_fmt(
        "INSERT INTO `%.*s`.`sdk_namespace_stat` "
        "(collection_id, namespace_id, row_count, total_size, total_size_with_index, "
        "last_gather_time) VALUES ",
        database_name.length(), database_name.ptr()))) {
      LOG_WARN("fail to build sql head", KR(ret), K(database_name), K(collection_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      const ObNamespaceStatRow &row = rows.at(i);
      if (OB_UNLIKELY(!row.is_valid())) {
        LOG_WARN("skip invalid namespace stat row when building sql", K(i), K(row));
      } else if (OB_FAIL(sql.append_fmt(
          "%s('%.*s', %lu, %ld, %ld, %ld, NOW(6))",
          (0 == valid_cnt) ? "" : ", ",
          collection_id.length(), collection_id.ptr(),
          row.namespace_id_,
          row.row_count_, row.total_size_, row.total_size_with_index_))) {
        LOG_WARN("fail to append values", KR(ret), K(i), K(row));
      } else {
        ++valid_cnt;
      }
    }
    if (OB_SUCC(ret) && valid_cnt > 0
        && OB_FAIL(sql.append_fmt(
            " ON DUPLICATE KEY UPDATE "
            "row_count = VALUES(row_count), "
            "total_size = VALUES(total_size), "
            "total_size_with_index = VALUES(total_size_with_index), "
            "last_gather_time = VALUES(last_gather_time)"))) {
      LOG_WARN("fail to append on duplicate key update", KR(ret), K(database_name), K(collection_id));
    }
    if (OB_SUCC(ret) && valid_cnt > 0) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else {
        LOG_DEBUG("update_namespace_stat_ done", KR(ret), K(database_name), K(collection_id),
                  K(cnt), K(valid_cnt), K(affected_rows));
      }
    }
  }
  return ret;
}

// ==================== Row-limit check related ====================

int ObLogicalTableMonitor::fetch_database_id_by_name_(const ObString &database_name,
                                                       uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT database_id FROM oceanbase.%s "
      "WHERE database_name = '%.*s'",
      OB_ALL_DATABASE_TNAME,
      database_name.length(), database_name.ptr()))) {
    LOG_WARN("fail to build sql", KR(ret), K(database_name));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("database not found", KR(ret), K(database_name));
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(res, "database_id", database_id, uint64_t);
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::fetch_database_name_(ObIAllocator &allocator,
                                                const uint64_t database_id,
                                                ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT database_name FROM oceanbase.%s WHERE database_id = %lu",
      OB_ALL_DATABASE_TNAME, database_id))) {
    LOG_WARN("fail to build sql", KR(ret), K(database_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("database not found", KR(ret), K(database_id));
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          ObString tmp_name;
          EXTRACT_VARCHAR_FIELD_MYSQL(res, "database_name", tmp_name);
          if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, tmp_name, database_name))) {
            LOG_WARN("fail to deep copy database name", KR(ret), K(tmp_name));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::fetch_logic_data_table_id_(const uint64_t database_id,
                                                      const ObString &collection_id,
                                                      uint64_t &logic_data_table_id)
{
  int ret = OB_SUCCESS;
  logic_data_table_id = OB_INVALID_ID;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT table_id FROM oceanbase.%s "
      "WHERE table_name = '%.*s_logic_data_table' AND database_id = %lu",
      OB_ALL_TABLE_TNAME,
      collection_id.length(), collection_id.ptr(), database_id))) {
    LOG_WARN("fail to build sql", KR(ret), K(collection_id), K(database_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("logic_data_table not found", KR(ret), K(collection_id), K(database_id));
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(res, "table_id", logic_data_table_id, uint64_t);
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::fetch_namespace_ops_limits_(const ObString &database_name,
                                                       const uint64_t namespace_id,
                                                       int64_t &row_limit,
                                                       int64_t &size_limit)
{
  int ret = OB_SUCCESS;
  row_limit = -1;
  size_limit = -1;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT "
      "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.row_limit')), '-1') AS SIGNED) AS row_limit, "
      "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.size_limit')), '-1') AS SIGNED) AS size_limit "
      "FROM `%.*s`.`sdk_namespaces` WHERE namespace_id = %lu",
      database_name.length(), database_name.ptr(), namespace_id))) {
    LOG_WARN("fail to build ops limit sql", KR(ret), K(database_name), K(namespace_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to read ops limits", KR(ret), K(sql));
        }
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get ops limit row", KR(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(res, "row_limit", row_limit, int64_t);
          EXTRACT_INT_FIELD_MYSQL(res, "size_limit", size_limit, int64_t);
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::fetch_namespace_stat_(const ObString &database_name,
                                                 const uint64_t namespace_id,
                                                 NsStat &stat)
{
  int ret = OB_SUCCESS;
  stat = NsStat();
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "SELECT row_count, total_size "
      "FROM `%.*s`.`sdk_namespace_stat` "
      "WHERE namespace_id = %lu",
      database_name.length(), database_name.ptr(), namespace_id))) {
    LOG_WARN("fail to build stat sql", KR(ret), K(database_name), K(namespace_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to read namespace stat", KR(ret), K(sql));
        }
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get stat row", KR(ret));
          }
        } else {
          stat.exists_ = true;
          EXTRACT_INT_FIELD_MYSQL(res, "row_count", stat.row_count_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(res, "total_size", stat.total_size_, int64_t);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fetch_namespace_ops_limits_(database_name, namespace_id,
                                            stat.row_limit_, stat.size_limit_))) {
      LOG_WARN("fail to fetch ops limits", KR(ret), K(namespace_id));
    }
  }
  return ret;
}

static int binary_to_hex_string(const ObString &binary, ObIAllocator &allocator, ObString &hex_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(binary.length() > 0 && OB_ISNULL(binary.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("binary ptr is null but length > 0", KR(ret), K(binary.length()));
  } else if (OB_UNLIKELY(binary.length() == 0)) {
    hex_str.reset();
  } else {
    const int64_t hex_len = binary.length() * 2;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(hex_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc hex buffer", KR(ret), K(hex_len));
    } else {
      static const char hex_chars[] = "0123456789ABCDEF";
      for (int64_t i = 0; i < binary.length(); ++i) {
        uint8_t byte = static_cast<uint8_t>(binary.ptr()[i]);
        buf[i * 2] = hex_chars[byte >> 4];
        buf[i * 2 + 1] = hex_chars[byte & 0x0F];
      }
      hex_str.assign_ptr(buf, static_cast<int32_t>(hex_len));
    }
  }
  return ret;
}

int ObLogicalTableMonitor::query_namespace_actual_row_count_(const ObString &database_name,
                                                             const ObString &collection_id,
                                                             const uint64_t namespace_id,
                                                             int64_t &actual_row_count)
{
  int ret = OB_SUCCESS;
  actual_row_count = 0;
  ObSEArray<uint64_t, 32> ltable_ids;
  ltable_ids.set_attr(ObMemAttr(MTL_ID(), "LTRowLimitLtIds"));
  if (OB_FAIL(fetch_ltable_ids_(database_name, collection_id, namespace_id, ltable_ids))) {
    LOG_WARN("fail to fetch ltable ids for exact count", KR(ret), K(namespace_id));
  } else {
    uint64_t database_id = OB_INVALID_ID;
    uint64_t logic_data_table_id = OB_INVALID_ID;
    if (OB_FAIL(fetch_database_id_by_name_(database_name, database_id))) {
      LOG_WARN("fail to fetch database id", KR(ret), K(database_name));
    } else if (OB_FAIL(fetch_logic_data_table_id_(database_id, collection_id, logic_data_table_id))) {
      LOG_WARN("fail to fetch logic_data_table_id", KR(ret), K(database_id), K(collection_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ltable_ids.count(); ++i) {
        ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LTRowLimit"));
        ObStoreRange range;
        const uint64_t ltable_id = ltable_ids.at(i);
        int64_t ltable_count = 0;
        if (OB_FAIL(get_logical_table_bounds(allocator, namespace_id, ltable_id,
                                             false/*is_included_index*/, logic_data_table_id, range))) {
          LOG_WARN("fail to get logical table bounds", KR(ret), K(namespace_id), K(ltable_id));
        } else {
          const ObStoreRowkey &start_key = range.get_start_key();
          const ObStoreRowkey &end_key = range.get_end_key();
          if (OB_UNLIKELY(start_key.get_obj_cnt() < 2 || end_key.get_obj_cnt() < 2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected rowkey column count", KR(ret),
                     K(start_key.get_obj_cnt()), K(end_key.get_obj_cnt()));
          } else {
            ObString start_kv_key = start_key.get_obj_ptr()[1].get_varchar();
            ObString end_kv_key = end_key.get_obj_ptr()[1].get_varchar();
            ObString start_hex;
            ObString end_hex;
            if (OB_FAIL(binary_to_hex_string(start_kv_key, allocator, start_hex))) {
              LOG_WARN("fail to convert start key to hex", KR(ret));
            } else if (OB_FAIL(binary_to_hex_string(end_kv_key, allocator, end_hex))) {
              LOG_WARN("fail to convert end key to hex", KR(ret));
            } else {
              ObSqlString sql;
              if (OB_FAIL(sql.append_fmt(
                  "SELECT COUNT(1) AS cnt FROM `%.*s`.`%.*s_kv_data_table` "
                  "WHERE namespace_id = %lu AND HEX(kv_key) >= '%.*s' AND HEX(kv_key) <= '%.*s'",
                  database_name.length(), database_name.ptr(),
                  collection_id.length(), collection_id.ptr(),
                  namespace_id,
                  static_cast<int>(start_hex.length()), start_hex.ptr(),
                  static_cast<int>(end_hex.length()), end_hex.ptr()))) {
                LOG_WARN("fail to build sql", KR(ret));
              } else {
                SMART_VAR(ObISQLClient::ReadResult, result) {
                  if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
                    LOG_WARN("fail to execute count sql", KR(ret), K(sql));
                  } else if (OB_ISNULL(result.get_result())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("get mysql result failed", KR(ret), K(sql));
                  } else {
                    sqlclient::ObMySQLResult &res = *result.get_result();
                    if (OB_FAIL(res.next())) {
                      LOG_WARN("fail to get count result", KR(ret));
                    } else {
                      EXTRACT_INT_FIELD_MYSQL(res, "cnt", ltable_count, int64_t);
                      if (OB_SUCC(ret)) {
                        actual_row_count = sat_add_nonneg(actual_row_count, ltable_count);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  LOG_TRACE("[LTMonitor] query_namespace_actual_row_count_ done", KR(ret),
           K(database_name), K(collection_id), K(namespace_id), K(actual_row_count));
  return ret;
}

int ObLogicalTableMonitor::update_namespace_row_count_(const ObString &database_name,
                                                       const ObString &collection_id,
                                                       const uint64_t namespace_id,
                                                       const int64_t actual_row_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt(
      "INSERT INTO `%.*s`.`sdk_namespace_stat` "
      "(collection_id, namespace_id, row_count, total_size, total_size_with_index, "
      "last_gather_time) VALUES "
      "('%.*s', %lu, %ld, 0, 0, NOW(6)) "
      "ON DUPLICATE KEY UPDATE "
      "row_count = VALUES(row_count), "
      "last_gather_time = VALUES(last_gather_time)",
      database_name.length(), database_name.ptr(),
      collection_id.length(), collection_id.ptr(),
      namespace_id, actual_row_count))) {
    LOG_WARN("fail to build sql", KR(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to update actual row count", KR(ret), K(sql));
    }
  }
  return ret;
}

bool ObLogicalTableMonitor::calc_estimate_exceeded_(const NsStat &stat,
                                                    bool &row_exceeded,
                                                    bool &size_exceeded)
{
  row_exceeded = false;
  size_exceeded = false;
  if (!stat.exists_) {
    return false;
  }
  if (0 == stat.row_limit_) {
    row_exceeded = true;
  }
  if (0 == stat.size_limit_) {
    size_exceeded = true;
  }
  const bool row_limit_on = (stat.row_limit_ > 0);
  const bool size_limit_on = (stat.size_limit_ > 0);
  if (!row_limit_on && !size_limit_on) {
    return row_exceeded || size_exceeded;
  }
  row_exceeded = row_exceeded || (row_limit_on && (stat.row_count_ >= stat.row_limit_));
  size_exceeded = size_exceeded || (size_limit_on && (stat.total_size_ >= stat.size_limit_));
  return row_exceeded || size_exceeded;
}

int ObLogicalTableMonitor::eval_namespace_limit_(const ObString &database_name,
                                                 const ObString &collection_id,
                                                 const uint64_t namespace_id,
                                                 const NsStat &stat,
                                                 bool &is_exceeded)
{
  int ret = OB_SUCCESS;
  is_exceeded = false;
  if (0 == stat.row_limit_ || 0 == stat.size_limit_) {
    // 0 means writes are forbidden by ops config, regardless of whether the
    // namespace has a stat row yet (an empty namespace has none).
    is_exceeded = true;
  } else if (!stat.exists_) {
    // No stat row: no measured data to compare against the limits, pass directly.
  } else {
    const bool row_limit_on = (stat.row_limit_ > 0);
    const bool size_limit_on = (stat.size_limit_ > 0);
    if (!row_limit_on && !size_limit_on) {
      // -1 means unlimited for that dimension.
    } else {
      bool row_exceeded = false;
      bool size_exceeded = false;
      if (!calc_estimate_exceeded_(stat, row_exceeded, size_exceeded)) {
        // Limit not reached.
      } else {
        int64_t actual_row_count = 0;
        if (OB_FAIL(query_namespace_actual_row_count_(database_name, collection_id,
                                                      namespace_id, actual_row_count))) {
          LOG_WARN("fail to query namespace actual row count", KR(ret), K(namespace_id));
        } else if (OB_FAIL(update_namespace_row_count_(database_name, collection_id,
                                                       namespace_id, actual_row_count))) {
          LOG_WARN("fail to update namespace row count", KR(ret), K(namespace_id));
        } else {
          is_exceeded = (row_limit_on && actual_row_count >= stat.row_limit_)
                        || (size_limit_on && stat.total_size_ >= stat.size_limit_);
        }
        LOG_TRACE("[LTMonitor] namespace limit recheck done", KR(ret), K(namespace_id),
                 K(database_name), K(stat), K(actual_row_count), K(is_exceeded));
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitor::check_logical_table_row_limit(const uint64_t namespace_id,
                                                         const uint64_t ltable_id,
                                                         const ObString &database_name,
                                                         const ObString &collection_id,
                                                         bool &is_exceeded)
{
  int ret = OB_SUCCESS;
  is_exceeded = false;
  const int64_t MAX_DB_NAME_LEN = 128;
  const int64_t MAX_COLL_ID_LEN = 64;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("logical table monitor is not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(namespace_id), K(ltable_id));
  } else if (ATOMIC_LOAD(&stopped_)) {
    // Monitor is stopping; skip the limit check instead of touching sql_proxy_.
    is_exceeded = false;
  } else if (OB_UNLIKELY(!is_safe_sql_ident(database_name, MAX_DB_NAME_LEN)
                         || !is_safe_sql_ident(collection_id, MAX_COLL_ID_LEN))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsafe database_name/collection_id for row limit check, skip",
             KR(ret), K(database_name), K(collection_id));
  } else {
    NsStat stat;
    if (OB_FAIL(fetch_namespace_stat_(database_name, namespace_id, stat))) {
      LOG_WARN("fail to fetch namespace stat", KR(ret), K(namespace_id));
    } else if (OB_FAIL(eval_namespace_limit_(database_name, collection_id, namespace_id,
                                             stat, is_exceeded))) {
      LOG_WARN("fail to eval namespace limit", KR(ret), K(namespace_id));
    }
  }

  LOG_TRACE("[LTMonitor] check_logical_table_row_limit done",
           KR(ret), K(namespace_id), K(ltable_id), K(database_name),
           K(collection_id), K(is_exceeded));
  return ret;
}

} // namespace share
} // namespace oceanbase

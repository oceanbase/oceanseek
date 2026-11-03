/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_ltable_bg_task_scheduler.h"
#include <byteswap.h>
#include <time.h>
#include "share/vector_index/ob_ivf_cache_warmup_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_cluster_version.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_server_struct.h"
#include "common/ob_smart_call.h"
#include "common/ob_smart_var.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "storage/ob_storage_struct.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_inner_table_operator.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

ObLTableBGTaskScheduler::ObLTableBGTaskScheduler()
  : is_inited_(false),
    is_leader_(false),
    is_stopped_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(0),
    ls_(NULL),
    sql_proxy_(NULL),
    last_run_time_(0),
    sdk_database_id_(OB_INVALID_ID),
    sdk_ltables_table_id_(OB_INVALID_ID),
    sdk_namespaces_table_id_(OB_INVALID_ID),
    sdk_collections_table_id_(OB_INVALID_ID),
    last_scanned_collection_id_(),
    last_scanned_ltable_id_(0),
    last_scanned_namespace_id_(0),
    last_warned_sdk_db_count_(0)
{
  MEMSET(sdk_database_name_, 0, sizeof(sdk_database_name_));
  last_scanned_collection_id_[0] = '\0';
}

ObLTableBGTaskScheduler::~ObLTableBGTaskScheduler()
{
  destroy();
}

// ---------------------------------------------------------------------------
// init()
// ---------------------------------------------------------------------------

int ObLTableBGTaskScheduler::init(uint64_t tenant_id,
                                  storage::ObLS *ls,
                                  int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLTableBGTaskScheduler init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_ISNULL(ls)
             || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(ls));
  } else {
    tenant_id_ = tenant_id;
    ls_ = ls;
    tg_id_ = tg_id;
    sql_proxy_ = GCTX.sql_proxy_;
    last_run_time_ = 0;
    ATOMIC_STORE(&is_inited_, true);
    if (OB_FAIL(TG_SCHEDULE(tg_id_, *this,
                             SCHEDULER_PERIOD_US, true /*repeat*/))) {
      LOG_WARN("fail to schedule ltable bg task", KR(ret), K(tg_id));
      ATOMIC_STORE(&is_inited_, false);
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// runTimerTask / run_task
// ---------------------------------------------------------------------------

void ObLTableBGTaskScheduler::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  run_task();
}

void ObLTableBGTaskScheduler::run_task()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("[LTABLE_BG] fail to get data version", KR(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    LOG_WARN("[LTABLE_BG] logic table bg task not supported, data version less than 4.6.1.0",
             K(data_version));
  } else if (IS_NOT_INIT) {
  } else if (!ATOMIC_LOAD(&is_leader_) || ATOMIC_LOAD(&is_stopped_)) {
  } else {
    int64_t now = ObTimeUtility::fast_current_time();
    if (now - last_run_time_ < TASK_INTERVAL_US) {
    } else {
      int tmp_ret = OB_SUCCESS;
      last_run_time_ = now;

      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      bool enable_drop = true;
      if (tenant_config.is_valid()) {
        enable_drop = tenant_config->enable_agent_db_background_task;
      }

      // Resolve ALL sdk databases each tick and loop the scan chains over each,
      // so no business db is silently skipped when multiple dbs hold sdk_collections.
      share::schema::ObSchemaGetterGuard schema_guard;
      common::ObSEArray<ObSdkDatabase, 4> sdk_dbs;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              tenant_id_, schema_guard))) {
        LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret), K_(tenant_id));
      } else if (OB_FAIL(resolve_all_sdk_databases_(schema_guard, sdk_dbs))) {
        LOG_WARN("[LTABLE_BG] fail to resolve sdk databases", K(ret), K_(tenant_id));
      } else {
        for (int64_t di = 0; di < sdk_dbs.count(); di++) {
          const ObSdkDatabase &db = sdk_dbs.at(di);
          sdk_database_id_ = db.database_id_;
          MEMCPY(sdk_database_name_, db.database_name_, sizeof(sdk_database_name_));
          sdk_database_name_[sizeof(sdk_database_name_) - 1] = '\0';
          // Cursors are per-db (scan SQL is FROM `<db>`.sdk_*); reset per db so a
          // prior db's cursor does not skip rows in this db. This gives up the
          // cross-tick advance optimization (each db re-scans from 0 each tick);
          // completed rows are deleted so they drop out, and in-flight rows are
          // deduped by the DAG scheduler — so it does not starve, only re-scans.
          last_scanned_collection_id_[0] = '\0';
          last_scanned_ltable_id_ = 0;
          last_scanned_namespace_id_ = 0;
          if (enable_drop) {
            if (OB_TMP_FAIL(scan_and_schedule_ltable_delete_())) {
              LOG_WARN("[LTABLE_BG] fail to scan ltable delete", K(tmp_ret),
                       K(db.database_id_), K(db.database_name_));
            }
            if (OB_TMP_FAIL(scan_and_schedule_namespace_delete_())) {
              LOG_WARN("[LTABLE_BG] fail to scan ns delete", K(tmp_ret),
                       K(db.database_id_), K(db.database_name_));
            }
          }
          if (OB_TMP_FAIL(scan_and_schedule_hot_ns_expire_())) {
            LOG_WARN("[LTABLE_BG] fail to scan hot ns expire", K(tmp_ret),
                     K(db.database_id_), K(db.database_name_));
          }
        }
        if (sdk_dbs.empty()) {
          // No sdk db this tick; clear any stale cache so a later tick re-resolves.
          sdk_database_id_ = OB_INVALID_ID;
          sdk_database_name_[0] = '\0';
        }
      }

      if (OB_TMP_FAIL(clear_drop_history_())) {
        LOG_WARN("[LTABLE_BG] fail to clear drop history", K(tmp_ret));
      }
    }
  }
}

// ---------------------------------------------------------------------------
// role change / stop / destroy / safe_to_destroy
// ---------------------------------------------------------------------------

void ObLTableBGTaskScheduler::switch_to_leader()
{
  ATOMIC_STORE(&is_leader_, true);
  LOG_INFO("[LTABLE_BG] switch to leader", K_(tenant_id));
}

void ObLTableBGTaskScheduler::switch_to_follower()
{
  ATOMIC_STORE(&is_leader_, false);
  LOG_INFO("[LTABLE_BG] switch to follower", K_(tenant_id));
}

void ObLTableBGTaskScheduler::stop()
{
  ATOMIC_STORE(&is_stopped_, true);
  LOG_INFO("[LTABLE_BG] scheduler stopped", K_(tenant_id));
}

void ObLTableBGTaskScheduler::destroy()
{
  ATOMIC_STORE(&is_inited_, false);
  ATOMIC_STORE(&is_leader_, false);
  ATOMIC_STORE(&is_stopped_, true);
  tenant_id_ = OB_INVALID_TENANT_ID;
  tg_id_ = 0;
  ls_ = NULL;
  sql_proxy_ = NULL;
  last_run_time_ = 0;
  sdk_database_id_ = OB_INVALID_ID;
  sdk_ltables_table_id_ = OB_INVALID_ID;
  sdk_namespaces_table_id_ = OB_INVALID_ID;
  sdk_collections_table_id_ = OB_INVALID_ID;
  last_scanned_collection_id_[0] = '\0';
  last_scanned_ltable_id_ = 0;
  last_scanned_namespace_id_ = 0;
}

int ObLTableBGTaskScheduler::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  // timer is stopped by caller (TG_STOP+TG_WAIT), but pending DAGs
  // submitted by other LS schedulers may still reference sql_proxy_
  is_safe = !ATOMIC_LOAD(&is_inited_) || ATOMIC_LOAD(&is_stopped_);
  return ret;
}

// ---------------------------------------------------------------------------
// Scan-and-schedule subtasks (DAG submission replaces inline SQL execution)
// ---------------------------------------------------------------------------

int ObLTableBGTaskScheduler::scan_and_schedule_ltable_delete_()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObLTableDroppedEntry, 100> entries;
  if (OB_FAIL(scan_dropped_ltables_(entries))) {
    LOG_WARN("[LTABLE_BG] fail to scan dropped ltables", K(ret));
  } else if (entries.empty()) {
  } else {
    common::ObSEArray<common::ObTabletID, 64> local_tablet_ids;
    bool queue_full = false;
    if (OB_FAIL(ls_->get_tablet_svr()->get_all_tablet_ids(
            true, local_tablet_ids))) {
      LOG_WARN("[LTABLE_BG] fail to get local tablet ids", K(ret));
    } else {
      for (int64_t i = 0; !queue_full && i < entries.count(); i++) {
        int tmp_ret = OB_SUCCESS;
        ObLTableDroppedEntry &entry = entries.at(i);
        bool is_completed = false;
        bool is_failed = false;
        uint64_t kv_data_table_id = OB_INVALID_ID;
        bool is_local = false;
        common::ObTabletID target_tablet_id;
        char partition_name[64];
        if (OB_TMP_FAIL(check_drop_task_completed_(
                entry.collection_id_, entry.namespace_id_, entry.ltable_id_,
                entry.recycle_time_us_, "LTABLE_DELETE", is_completed))) {
          LOG_WARN("[LTABLE_BG] fail to check ltable delete completion",
                   K(tmp_ret), K(entry));
        } else if (is_completed) {
          if (OB_TMP_FAIL(cleanup_ltable_mapping_(entry))) {
            LOG_WARN("[LTABLE_BG] fail to cleanup completed ltable mapping",
                     K(tmp_ret), K(entry));
          } else {
            last_scanned_ltable_id_ = entry.ltable_id_;
          }
        } else if (OB_TMP_FAIL(check_drop_task_failed_(
                       entry.collection_id_, entry.namespace_id_, entry.ltable_id_,
                       entry.recycle_time_us_, "LTABLE_DELETE", is_failed))) {
          LOG_WARN("[LTABLE_BG] fail to check ltable delete terminal failure",
                   K(tmp_ret), K(entry));
        } else if (is_failed) {
          // A permanent failure or a transient failure still in its cooldown.
          LOG_TRACE("[LTABLE_BG] ltable delete retry is blocked",
                   K(entry));
          last_scanned_ltable_id_ = entry.ltable_id_;
        } else if (OB_TMP_FAIL(get_cached_kv_data_table_id_(
                entry.collection_id_, kv_data_table_id))) {
          if (ObLTableBGSqlUtil::is_missing_physical_table_error(tmp_ret)) {
            // Collection 删除已同步移除物理表，此时无需再提交删除 DAG。
            // 直接回收 recyclebin catalog，避免无效 DAG 堵塞后台队列。
            if (OB_TMP_FAIL(cleanup_ltable_mapping_(entry))) {
              LOG_WARN("[LTABLE_BG] fail to cleanup ltable mapping "
                       "after physical table removed",
                       K(tmp_ret), K(entry));
            } else {
              LOG_INFO("[LTABLE_BG] physical table already removed, "
                       "cleanup ltable mapping directly",
                       K(entry));
              last_scanned_ltable_id_ = entry.ltable_id_;
            }
          } else {
            LOG_WARN("[LTABLE_BG] fail to get kv data table id",
                     K(tmp_ret), K(entry));
          }
        } else if (OB_TMP_FAIL(check_kv_data_partition_local_(
                kv_data_table_id, entry.namespace_id_,
                local_tablet_ids, target_tablet_id, partition_name,
                sizeof(partition_name), is_local))) {
          LOG_WARN("[LTABLE_BG] fail to check partition local",
                   K(tmp_ret), K(entry));
        } else if (!is_local) {
          last_scanned_ltable_id_ = entry.ltable_id_;
        } else {
          ObLTableBGTaskCtx ctx;
          ctx.type_ = LTABLE_BG_LTABLE_DELETE;
          ctx.tenant_id_ = tenant_id_;
          MEMCPY(ctx.collection_id_, entry.collection_id_,
                 sizeof(ctx.collection_id_));
          ctx.namespace_id_ = entry.namespace_id_;
          ctx.ltable_id_ = entry.ltable_id_;
          ctx.tablet_id_ = target_tablet_id;
          MEMCPY(ctx.partition_name_, partition_name, sizeof(partition_name));
          int write_len = snprintf(ctx.kv_data_table_name_,
                                   sizeof(ctx.kv_data_table_name_),
                                   "%s_kv_data_table", entry.collection_id_);
          if (write_len > 0
              && write_len < static_cast<int>(sizeof(ctx.kv_data_table_name_))) {
            ctx.kv_data_table_name_[write_len] = '\0';
          } else {
            ctx.kv_data_table_name_[sizeof(ctx.kv_data_table_name_) - 1] = '\0';
          }
          if (OB_TMP_FAIL(extract_original_name_(
                  entry.ltable_name_, ctx.original_name_,
                  sizeof(ctx.original_name_)))) {
            LOG_WARN("[LTABLE_BG] fail to extract original ltable name",
                     K(tmp_ret), K(entry));
            // Invalid recyclebin names cannot be repaired by retrying. Skip
            // this row so it does not keep a full scan batch from advancing.
            last_scanned_ltable_id_ = entry.ltable_id_;
          }
          ctx.database_id_ = sdk_database_id_;
          MEMCPY(ctx.sdk_database_name_, sdk_database_name_,
                 sizeof(ctx.sdk_database_name_));
          MEMCPY(ctx.collection_name_, entry.collection_name_,
                 sizeof(ctx.collection_name_));
          MEMCPY(ctx.namespace_name_, entry.namespace_name_,
                 sizeof(ctx.namespace_name_));
          if (OB_SUCCESS == tmp_ret) {
            if (OB_TMP_FAIL(try_schedule_dag_(ctx))) {
              if (OB_SIZE_OVERFLOW == tmp_ret) {
                // queue full: stop submitting more this tick, keep cursor at
                // last successfully advanced position so next tick resumes here
                queue_full = true;
              } else if (OB_EAGAIN == tmp_ret) {
                // empty (already converted to SUCCESS inside try_schedule_dag_)
              } else {
                LOG_WARN("[LTABLE_BG] fail to schedule ltable delete dag",
                         K(tmp_ret), K(ctx));
              }
            } else {
              last_scanned_ltable_id_ = entry.ltable_id_;
            }
          }
        }
      }
    }
    // Only reset cursor when the entire batch was drained AND we got fewer
    // than SCAN_LIMIT rows (true end of scan). If queue_full broke us out
    // early, keep the cursor so we resume from the last successful entry.
    if (OB_SUCC(ret) && !queue_full && entries.count() < SCAN_LIMIT) {
      last_scanned_ltable_id_ = 0;
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::scan_and_schedule_namespace_delete_()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObNamespaceDroppedEntry, 100> entries;
  if (OB_FAIL(scan_dropped_namespaces_(entries))) {
    LOG_WARN("[LTABLE_BG] fail to scan dropped namespaces", K(ret));
  } else if (entries.empty()) {
  } else {
    common::ObSEArray<common::ObTabletID, 64> local_tablet_ids;
    bool queue_full = false;
    if (OB_FAIL(ls_->get_tablet_svr()->get_all_tablet_ids(
            true, local_tablet_ids))) {
      LOG_WARN("[LTABLE_BG] fail to get local tablet ids", K(ret));
    } else {
      for (int64_t i = 0; !queue_full && i < entries.count(); i++) {
        int tmp_ret = OB_SUCCESS;
        ObNamespaceDroppedEntry &entry = entries.at(i);
        bool is_completed = false;
        bool is_failed = false;
        int64_t ltable_count = 0;
        uint64_t kv_data_table_id = OB_INVALID_ID;
        bool is_local = false;
        common::ObTabletID target_tablet_id;
        char partition_name[64];
        if (OB_TMP_FAIL(check_drop_task_completed_(
                entry.collection_id_, entry.namespace_id_, -1,
                entry.recycle_time_us_, "NAMESPACE_DELETE", is_completed))) {
          LOG_WARN("[LTABLE_BG] fail to check namespace delete completion",
                   K(tmp_ret), K(entry));
        } else if (is_completed) {
          if (OB_TMP_FAIL(cleanup_namespace_mapping_(entry))) {
            LOG_WARN("[LTABLE_BG] fail to cleanup completed namespace mapping",
                     K(tmp_ret), K(entry));
          } else {
            last_scanned_namespace_id_ = entry.namespace_id_;
          }
        } else if (OB_TMP_FAIL(check_drop_task_failed_(
                       entry.collection_id_, entry.namespace_id_, -1,
                       entry.recycle_time_us_, "NAMESPACE_DELETE", is_failed))) {
          LOG_WARN("[LTABLE_BG] fail to check namespace delete failure", K(tmp_ret), K(entry));
        } else if (is_failed) {
          LOG_TRACE("[LTABLE_BG] namespace delete retry is blocked", K(entry));
          last_scanned_namespace_id_ = entry.namespace_id_;
        } else if (OB_TMP_FAIL(count_ltables_under_namespace_(
                entry.collection_id_, entry.namespace_id_, ltable_count))) {
          LOG_WARN("[LTABLE_BG] fail to count ltables under namespace",
                   K(tmp_ret), K(entry));
        } else if (ltable_count > 0) {
        } else if (OB_TMP_FAIL(get_cached_kv_data_table_id_(
                entry.collection_id_, kv_data_table_id))) {
          if (ObLTableBGSqlUtil::is_missing_physical_table_error(tmp_ret)) {
            // Collection 删除已同步移除物理表，直接回收 namespace catalog。
            if (OB_TMP_FAIL(cleanup_namespace_mapping_(entry))) {
              LOG_WARN("[LTABLE_BG] fail to cleanup namespace mapping "
                       "after physical table removed",
                       K(tmp_ret), K(entry));
            } else {
              LOG_INFO("[LTABLE_BG] physical table already removed, "
                       "cleanup namespace mapping directly",
                       K(entry));
              last_scanned_namespace_id_ = entry.namespace_id_;
            }
          } else {
            LOG_WARN("[LTABLE_BG] fail to get kv data table id",
                     K(tmp_ret), K(entry));
          }
        } else if (OB_TMP_FAIL(check_kv_data_partition_local_(
                kv_data_table_id, entry.namespace_id_,
                local_tablet_ids, target_tablet_id, partition_name,
                sizeof(partition_name), is_local))) {
          LOG_WARN("[LTABLE_BG] fail to check partition local",
                   K(tmp_ret), K(entry));
        } else if (!is_local) {
          last_scanned_namespace_id_ = entry.namespace_id_;
        } else {
          ObLTableBGTaskCtx ctx;
          ctx.type_ = LTABLE_BG_NAMESPACE_DELETE;
          ctx.tenant_id_ = tenant_id_;
          MEMCPY(ctx.collection_id_, entry.collection_id_,
                 sizeof(ctx.collection_id_));
          ctx.namespace_id_ = entry.namespace_id_;
          ctx.ltable_id_ = -1;
          ctx.tablet_id_ = target_tablet_id;
          MEMCPY(ctx.partition_name_, partition_name, sizeof(partition_name));
          int write_len = snprintf(ctx.kv_data_table_name_,
                                   sizeof(ctx.kv_data_table_name_),
                                   "%s_kv_data_table", entry.collection_id_);
          if (write_len > 0
              && write_len < static_cast<int>(sizeof(ctx.kv_data_table_name_))) {
            ctx.kv_data_table_name_[write_len] = '\0';
          } else {
            ctx.kv_data_table_name_[sizeof(ctx.kv_data_table_name_) - 1] = '\0';
          }
          write_len = snprintf(ctx.hot_table_name_,
                               sizeof(ctx.hot_table_name_),
                               "%s_hot_table", entry.collection_id_);
          if (write_len > 0
              && write_len < static_cast<int>(sizeof(ctx.hot_table_name_))) {
            ctx.hot_table_name_[write_len] = '\0';
          } else {
            ctx.hot_table_name_[sizeof(ctx.hot_table_name_) - 1] = '\0';
          }
          ctx.database_id_ = sdk_database_id_;
          if (OB_TMP_FAIL(extract_original_name_(
                  entry.namespace_name_, ctx.original_name_,
                  sizeof(ctx.original_name_)))) {
            LOG_WARN("[LTABLE_BG] fail to extract original namespace name",
                     K(tmp_ret), K(entry));
            // Invalid recyclebin names cannot be repaired by retrying. Skip
            // this row so it does not keep a full scan batch from advancing.
            last_scanned_namespace_id_ = entry.namespace_id_;
          }
          MEMCPY(ctx.sdk_database_name_, sdk_database_name_,
                 sizeof(ctx.sdk_database_name_));
          MEMCPY(ctx.collection_name_, entry.collection_name_,
                 sizeof(ctx.collection_name_));
          MEMCPY(ctx.namespace_name_, entry.namespace_name_,
                 sizeof(ctx.namespace_name_));
          if (OB_SUCCESS == tmp_ret) {
            if (OB_TMP_FAIL(try_schedule_dag_(ctx))) {
              if (OB_SIZE_OVERFLOW == tmp_ret) {
                // queue full: stop submitting more this tick, keep cursor at
                // last successfully advanced position so next tick resumes here
                queue_full = true;
              } else if (OB_EAGAIN == tmp_ret) {
                // empty (already converted to SUCCESS inside try_schedule_dag_)
              } else {
                LOG_WARN("[LTABLE_BG] fail to schedule ns delete dag",
                         K(tmp_ret), K(ctx));
              }
            } else {
              last_scanned_namespace_id_ = entry.namespace_id_;
            }
          }
        }
      }
    }
    // Only reset cursor when the entire batch was drained AND we got fewer
    // than SCAN_LIMIT rows (true end of scan). If queue_full broke us out
    // early, keep the cursor so we resume from the last successful entry.
    if (OB_SUCC(ret) && !queue_full && entries.count() < SCAN_LIMIT) {
      last_scanned_namespace_id_ = 0;
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::check_sdk_namespaces_exist_(
    bool &has_sdk_namespaces)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  uint64_t sdk_namespaces_table_id = OB_INVALID_ID;
  has_sdk_namespaces = false;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_id(
          tenant_id_, sdk_database_id_, common::ObString("sdk_namespaces"),
          false /*is_index*/,
          share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
          sdk_namespaces_table_id))) {
    if (ObLTableBGSqlUtil::is_missing_physical_table_error(ret)) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[LTABLE_BG] fail to get sdk_namespaces table id",
               K(ret), K_(tenant_id), K_(sdk_database_id));
    }
  } else {
    has_sdk_namespaces = OB_INVALID_ID != sdk_namespaces_table_id;
  }
  return ret;
}

int ObLTableBGTaskScheduler::build_hot_ns_expire_scan_sql_(
    const ObCollectionInfo &collection,
    bool has_sdk_namespaces,
    int64_t expire_threshold_us,
    uint64_t hot_ns_cursor,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (has_sdk_namespaces) {
    ret = sql.assign_fmt(
        "SELECT hot.namespace_id, IFNULL(ns.namespace_name, '') AS namespace_name "
        "FROM `%s`.`%s_hot_table` hot "
        "LEFT JOIN `%s`.`sdk_namespaces` ns "
        "ON ns.namespace_id = hot.namespace_id AND ns.collection_id = '%s' "
        "WHERE hot.last_access_time < usec_to_time(%ld) "
        "AND hot.namespace_id > %lu "
        "ORDER BY hot.namespace_id LIMIT %ld",
        sdk_database_name_, collection.collection_id_, sdk_database_name_,
        collection.collection_id_, expire_threshold_us, hot_ns_cursor, SCAN_LIMIT);
  } else {
    ret = sql.assign_fmt(
        "SELECT namespace_id, '' AS namespace_name "
        "FROM `%s`.`%s_hot_table` "
        "WHERE last_access_time < usec_to_time(%ld) "
        "AND namespace_id > %lu "
        "ORDER BY namespace_id LIMIT %ld",
        sdk_database_name_, collection.collection_id_,
        expire_threshold_us, hot_ns_cursor, SCAN_LIMIT);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[LTABLE_BG] fail to construct expired ns sql", K(ret));
  }
  return ret;
}

int ObLTableBGTaskScheduler::build_hot_ns_expire_task_ctx_(
    const ObCollectionInfo &collection,
    uint64_t namespace_id,
    const common::ObString &namespace_name,
    ObLTableBGTaskCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t copy_len = 0;
  int write_len = 0;
  ctx.type_ = LTABLE_BG_HOT_NAMESPACE_EXPIRE;
  ctx.tenant_id_ = tenant_id_;
  MEMCPY(ctx.collection_id_, collection.collection_id_,
         sizeof(ctx.collection_id_));
  ctx.namespace_id_ = namespace_id;
  ctx.ltable_id_ = -1;
  write_len = snprintf(ctx.hot_table_name_, sizeof(ctx.hot_table_name_),
                       "%s_hot_table", collection.collection_id_);
  if (write_len <= 0
      || write_len >= static_cast<int>(sizeof(ctx.hot_table_name_))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[LTABLE_BG] fail to construct hot table name", K(ret), K(write_len));
  } else {
    write_len = snprintf(ctx.kv_data_table_name_, sizeof(ctx.kv_data_table_name_),
                         "%s_kv_data_table", collection.collection_id_);
    if (write_len <= 0
        || write_len >= static_cast<int>(sizeof(ctx.kv_data_table_name_))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[LTABLE_BG] fail to construct kv data table name",
               K(ret), K(write_len));
    }
  }
  if (OB_SUCC(ret)) {
    ctx.database_id_ = sdk_database_id_;
    MEMCPY(ctx.original_name_, collection.collection_name_buf_,
           sizeof(collection.collection_name_buf_));
    MEMCPY(ctx.sdk_database_name_, sdk_database_name_,
           sizeof(ctx.sdk_database_name_));
    MEMCPY(ctx.collection_name_, collection.collection_name_buf_,
           sizeof(ctx.collection_name_));
    copy_len = MIN(
        static_cast<int64_t>(namespace_name.length()),
        static_cast<int64_t>(sizeof(ctx.namespace_name_) - 1));
    MEMCPY(ctx.namespace_name_, namespace_name.ptr(), copy_len);
    ctx.namespace_name_[copy_len] = '\0';
  }
  if (OB_SUCC(ret)) {
    uint64_t kv_table_id = OB_INVALID_ID;
    bool is_local = false;
    schema::ObSchemaGetterGuard schema_guard;
    const schema::ObTableSchema *kv_schema = nullptr;
    if (OB_FAIL(get_cached_kv_data_table_id_(collection.collection_id_, kv_table_id))) {
      LOG_WARN("[LTABLE_BG] fail to resolve hot namespace KV table", K(ret), K(namespace_id));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("[LTABLE_BG] fail to get hot namespace schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, kv_table_id, kv_schema))) {
      LOG_WARN("[LTABLE_BG] fail to get hot namespace KV schema", K(ret), K(kv_table_id));
    } else if (OB_ISNULL(kv_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("[LTABLE_BG] hot namespace KV schema is absent", K(ret), K(kv_table_id));
    } else if (OB_FAIL(ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
        *kv_schema, namespace_id, ctx.tablet_id_))) {
      LOG_WARN("[LTABLE_BG] fail to route hot namespace like PREWARM", K(ret), K(namespace_id));
    } else if (OB_FAIL(ls_->get_tablet_svr()->is_tablet_exist(ctx.tablet_id_, is_local))) {
      LOG_WARN("[LTABLE_BG] fail to check hot namespace owner", K(ret), K(ctx.tablet_id_));
    } else if (!is_local) {
      // Only the owning LS may consume the shared hot row.
      ctx.tablet_id_.reset();
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::scan_and_schedule_hot_ns_expire_for_collection_(
    const ObCollectionInfo &collection,
    bool has_sdk_namespaces,
    int64_t expire_threshold_us,
    bool &queue_full)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard hot_table_schema_guard;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t hot_table_id = OB_INVALID_ID;
  common::ObSqlString hot_table_name;
  if (OB_FAIL(hot_table_name.assign_fmt(
          "%s_hot_table", collection.collection_id_))) {
    LOG_WARN("[LTABLE_BG] fail to construct hot table name", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          tenant_id_, hot_table_schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
  } else if (OB_FAIL(resolve_sdk_database_id_(
          hot_table_schema_guard, database_id))) {
  } else if (OB_INVALID_ID == database_id) {
  } else if (OB_FAIL(hot_table_schema_guard.get_table_id(
          tenant_id_, database_id, hot_table_name.ptr(), false,
          share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
          hot_table_id))) {
  } else if (OB_INVALID_ID == hot_table_id) {
  } else if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          sdk_database_name_, "sdk_database_name"))) {
    LOG_WARN("[LTABLE_BG] sdk_database_name rejected", K(ret));
  } else if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          collection.collection_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject expired ns scan",
             K(ret), KCSTRING(collection.collection_id_));
  } else {
    uint64_t hot_ns_cursor = 0;
    bool collection_done = false;
    while (!collection_done && !queue_full && OB_SUCC(ret)) {
      common::ObSqlString sql;
      if (OB_FAIL(build_hot_ns_expire_scan_sql_(
              collection, has_sdk_namespaces, expire_threshold_us,
              hot_ns_cursor, sql))) {
      } else {
        int64_t row_count = 0;
        uint64_t max_id_in_batch = hot_ns_cursor;
        SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
          sqlclient::ObMySQLResult *result = NULL;
          if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
            LOG_WARN("[LTABLE_BG] fail to execute expired ns sql",
                     K(ret), K(sql));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[LTABLE_BG] result is null", K(ret));
          } else {
            while (OB_SUCC(ret) && !queue_full) {
              uint64_t namespace_id = 0;
              common::ObString namespace_name;
              if (OB_FAIL(result->next())) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                }
                LOG_WARN("[LTABLE_BG] fail to get next row", K(ret));
              } else if (OB_FAIL(result->get_uint(
                      "namespace_id", namespace_id))) {
                LOG_WARN("[LTABLE_BG] fail to get namespace_id", K(ret));
              } else if (OB_FAIL(result->get_varchar(
                      "namespace_name", namespace_name))) {
                LOG_WARN("[LTABLE_BG] fail to get namespace_name", K(ret));
              } else {
                ObLTableBGTaskCtx ctx;
                row_count++;
                max_id_in_batch = MAX(max_id_in_batch, namespace_id);
                if (OB_FAIL(build_hot_ns_expire_task_ctx_(
                        collection, namespace_id, namespace_name, ctx))) {
                } else if (!ctx.tablet_id_.is_valid() || !ATOMIC_LOAD(&is_leader_)) {
                  // Another LS owns this namespace, or this scheduler has lost leadership.
                } else {
                  int sched_ret = try_schedule_dag_(ctx);
                  if (OB_SIZE_OVERFLOW == sched_ret) {
                    queue_full = true;
                  } else if (OB_SUCCESS != sched_ret && OB_EAGAIN != sched_ret) {
                    LOG_WARN("[LTABLE_BG] fail to schedule hot ns expire dag",
                             K(sched_ret), K(ctx));
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          collection_done = row_count < SCAN_LIMIT;
          hot_ns_cursor = max_id_in_batch;
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::scan_and_schedule_hot_ns_expire_()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObCollectionInfo, 100> collections;
  bool has_sdk_namespaces = false;
  if (OB_FAIL(scan_all_collections_(collections))) {
    LOG_WARN("[LTABLE_BG] fail to scan collections", K(ret));
  } else if (OB_FAIL(check_sdk_namespaces_exist_(has_sdk_namespaces))) {
    LOG_WARN("[LTABLE_BG] fail to check sdk_namespaces", K(ret));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    int64_t hot_ns_ttl_us = 1LL * 3600 * 1000 * 1000; // default 1h
    if (tenant_config.is_valid()) {
      hot_ns_ttl_us = tenant_config->agent_db_hot_namespace_ttl;
    }
    int64_t expire_threshold_us =
        ObTimeUtility::fast_current_time() - hot_ns_ttl_us;
    // Stop submitting for this tick when the DAG queue is full.
    bool queue_full = false;
    for (int64_t i = 0; !queue_full && i < collections.count(); i++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(scan_and_schedule_hot_ns_expire_for_collection_(
              collections.at(i), has_sdk_namespaces,
              expire_threshold_us, queue_full))) {
        LOG_WARN("[LTABLE_BG] fail to scan expired namespace for collection",
                 K(tmp_ret), K(collections.at(i)));
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::clear_drop_history_()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] fail to get tenant config", K(ret), K_(tenant_id));
  } else {
    int64_t now_us = ObTimeUtility::current_time();
    int64_t cutoff_us = now_us - tenant_config->_agent_db_background_task_history_recycle_interval;
    int64_t affected_rows = 0;
    common::ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s WHERE tenant_id = %ld "
            "AND gmt_create < usec_to_time(%ld) LIMIT %ld",
            OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME,
            static_cast<int64_t>(tenant_id_),
            cutoff_us,
            HISTORY_CLEANUP_BATCH_LIMIT))) {
      LOG_WARN("[LTABLE_BG] fail to construct clear_drop_history sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(
            gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[LTABLE_BG] fail to execute clear_drop_history sql",
               K(ret), K(sql));
    } else {
      LOG_TRACE("[LTABLE_BG] clear_drop_history done",
                K_(tenant_id), K(cutoff_us), K(affected_rows));
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::get_cached_kv_data_table_id_(
    const char *collection_id,
    uint64_t &kv_data_table_id)
{
  int ret = OB_SUCCESS;
  kv_data_table_id = OB_INVALID_ID;
  share::schema::ObSchemaGetterGuard schema_guard;
  uint64_t database_id = OB_INVALID_ID;
  common::ObSqlString table_name;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
  } else if (OB_FAIL(resolve_sdk_database_id_(schema_guard, database_id))) {
    LOG_WARN("[LTABLE_BG] fail to resolve sdk database id", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] sdk database not found", K(ret));
  } else if (OB_FAIL(table_name.assign_fmt(
          "%s_kv_data_table", collection_id))) {
    LOG_WARN("[LTABLE_BG] fail to construct table name", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(
          tenant_id_, database_id,
          table_name.ptr(), false,
          share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
          kv_data_table_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("[LTABLE_BG] fail to get kv_data_table id",
               K(ret), K(table_name));
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == kv_data_table_id) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("[LTABLE_BG] kv_data_table_id is OB_INVALID_ID after get_table_id succeeded",
             K(ret), K(collection_id));
  }
  return ret;
}

int ObLTableBGTaskScheduler::check_kv_data_partition_local_(
    uint64_t kv_data_table_id,
    int64_t namespace_id,
    const common::ObIArray<common::ObTabletID> &local_tablet_ids,
    common::ObTabletID &tablet_id,
    char *partition_name_buf,
    int64_t buf_len,
    bool &is_local)
{
  int ret = OB_SUCCESS;
  is_local = false;
  tablet_id.reset();
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, kv_data_table_id, table_schema))) {
    LOG_WARN("[LTABLE_BG] fail to get kv data table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] kv data table schema is NULL", K(ret));
  } else {
    common::ObObj cells[1];
    cells[0].set_int(namespace_id);
    common::ObNewRow row(cells, 1);
    common::ObObjectID part_id = OB_INVALID_ID;
    if (OB_FAIL(share::schema::ObPartitionUtils::get_tablet_and_part_id(
            *table_schema, row, tablet_id, part_id))) {
      if (OB_NOT_SUPPORTED == ret) {
        LOG_DEBUG("[LTABLE_BG] partition util not supported, skip",
                  K(kv_data_table_id), K(namespace_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[LTABLE_BG] fail to calc partition", K(ret),
                 K(namespace_id), K(kv_data_table_id));
      }
    } else {
      for (int64_t j = 0; j < local_tablet_ids.count(); j++) {
        if (local_tablet_ids.at(j) == tablet_id) {
          is_local = true;
          const share::schema::ObPartition *partition = NULL;
          if (OB_FAIL(table_schema->get_partition_by_part_id(
                  part_id, share::schema::CHECK_PARTITION_MODE_NORMAL,
                  partition))) {
            LOG_WARN("[LTABLE_BG] fail to get partition by part_id",
                     K(ret), K(part_id));
          } else if (OB_NOT_NULL(partition)) {
            const common::ObString &pname = partition->get_part_name();
            int64_t n = MIN(pname.length(), buf_len - 1);
            MEMCPY(partition_name_buf, pname.ptr(), n);
            partition_name_buf[n] = '\0';
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::try_schedule_dag_(const ObLTableBGTaskCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ObLTableBGDeleteDag *dag = NULL;
  ObLTableBGDeleteTask *task = NULL;
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] dag scheduler is NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
    LOG_WARN("[LTABLE_BG] fail to alloc dag", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] dag is NULL", K(ret));
  } else if (OB_FAIL(dag->init(ctx))) {
    LOG_WARN("[LTABLE_BG] fail to init dag", K(ret));
  } else if (OB_FAIL(dag->alloc_task(task))) {
    LOG_WARN("[LTABLE_BG] fail to alloc task", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] task is NULL", K(ret));
  } else if (OB_FAIL(task->init(ctx, sql_proxy_))) {
    LOG_WARN("[LTABLE_BG] fail to init task", K(ret));
  } else if (OB_FAIL(dag->add_task(*task))) {
    LOG_WARN("[LTABLE_BG] fail to add task to dag", K(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
    if (OB_EAGAIN == ret) {
      LOG_TRACE("[LTABLE_BG] duplicate dag, skip", K(ctx));
    } else if (OB_SIZE_OVERFLOW == ret) {
      LOG_TRACE("[LTABLE_BG] dag queue full, retry next tick", K(ctx));
    } else {
      LOG_WARN("[LTABLE_BG] fail to add dag to scheduler", K(ret), K(ctx));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(dag)) {
    dag_scheduler->free_dag(*dag);
    dag = NULL;
  }
  if (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// SQL infrastructure helpers
// ---------------------------------------------------------------------------

int ObLTableBGTaskScheduler::resolve_sdk_database_id_(
    share::schema::ObSchemaGetterGuard &schema_guard,
    uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  if (OB_INVALID_ID != sdk_database_id_) {
    database_id = sdk_database_id_;
    if ('\0' == sdk_database_name_[0]) {
      const share::schema::ObSimpleDatabaseSchema *db_schema = NULL;
      if (OB_FAIL(schema_guard.get_database_schema(
              tenant_id_, sdk_database_id_, db_schema))) {
        LOG_WARN("[LTABLE_BG] fail to get database schema by id",
                 K(ret), K_(tenant_id), K_(sdk_database_id));
      } else if (OB_NOT_NULL(db_schema)) {
        const common::ObString &db_name = db_schema->get_database_name();
        int64_t name_len = MIN(static_cast<int64_t>(db_name.length()),
                               static_cast<int64_t>(sizeof(sdk_database_name_) - 1));
        MEMCPY(sdk_database_name_, db_name.ptr(), name_len);
        sdk_database_name_[name_len] = '\0';
      }
    }
  } else {
    common::ObSEArray<const share::schema::ObSimpleDatabaseSchema *, 16> db_schemas;
    if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(
            tenant_id_, db_schemas))) {
      LOG_WARN("[LTABLE_BG] fail to get database schemas",
               K(ret), K_(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < db_schemas.count(); i++) {
        if (OB_ISNULL(db_schemas.at(i))) {
        } else {
          uint64_t db_id = db_schemas.at(i)->get_database_id();
          uint64_t table_id = OB_INVALID_ID;
          int tmp_ret = schema_guard.get_table_id(
              tenant_id_, db_id,
              common::ObString("sdk_collections"),
              false /*is_index*/,
              share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
              table_id);
          if (OB_SUCCESS == tmp_ret && OB_INVALID_ID != table_id) {
            sdk_database_id_ = db_id;
            database_id = db_id;
            const common::ObString &db_name = db_schemas.at(i)->get_database_name();
            int64_t name_len = MIN(static_cast<int64_t>(db_name.length()),
                                   static_cast<int64_t>(sizeof(sdk_database_name_) - 1));
            MEMCPY(sdk_database_name_, db_name.ptr(), name_len);
            sdk_database_name_[name_len] = '\0';
            break;
          }
        }
      }
      if (OB_INVALID_ID == database_id) {
        LOG_TRACE("[LTABLE_BG] sdk_collections not found in any database, skip",
                  K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::resolve_all_sdk_databases_(
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<ObSdkDatabase> &dbs)
{
  int ret = OB_SUCCESS;
  dbs.reset();
  common::ObSEArray<const share::schema::ObSimpleDatabaseSchema *, 16> db_schemas;
  if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id_, db_schemas))) {
    LOG_WARN("[LTABLE_BG] fail to get database schemas", K(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < db_schemas.count(); i++) {
      if (OB_NOT_NULL(db_schemas.at(i))) {
        uint64_t db_id = db_schemas.at(i)->get_database_id();
        uint64_t table_id = OB_INVALID_ID;
        int tmp_ret = schema_guard.get_table_id(
            tenant_id_, db_id,
            common::ObString("sdk_collections"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            table_id);
        if (OB_SUCCESS != tmp_ret) {
          // Unexpected schema errors must not be treated as "no sdk_collections".
          ret = tmp_ret;
          LOG_WARN("[LTABLE_BG] fail to get sdk_collections table_id",
                   K(ret), K_(tenant_id), K(db_id));
        } else if (OB_INVALID_ID != table_id) {
          ObSdkDatabase db;
          db.database_id_ = db_id;
          const common::ObString &db_name = db_schemas.at(i)->get_database_name();
          int64_t name_len = MIN(static_cast<int64_t>(db_name.length()),
                                 static_cast<int64_t>(sizeof(db.database_name_) - 1));
          MEMCPY(db.database_name_, db_name.ptr(), name_len);
          db.database_name_[name_len] = '\0';
          if (OB_FAIL(dbs.push_back(db))) {
            LOG_WARN("[LTABLE_BG] fail to push sdk database", K(ret), K(db_id));
          }
        }
      }
    }
    if (OB_SUCC(ret) && dbs.count() > 1
        && dbs.count() != last_warned_sdk_db_count_) {
      // Multiple sdk databases in one tenant is unusual (typically a stale test
      // db alongside the business db). Warn when the set size changes, not every tick.
      LOG_WARN("[LTABLE_BG] multiple sdk databases found, scanning all",
               K_(tenant_id), K(dbs.count()), K_(last_warned_sdk_db_count));
      last_warned_sdk_db_count_ = dbs.count();
    } else if (OB_SUCC(ret) && dbs.count() <= 1) {
      last_warned_sdk_db_count_ = dbs.count();
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::get_local_partition_names_for_table_(
    uint64_t table_id,
    common::ObIArray<common::ObString> &part_names,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  common::ObSEArray<ObTabletID, 16> all_tablet_ids;
  common::ObSEArray<ObTabletID, 16> local_tablet_ids;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, table_id, table_schema))) {
    LOG_WARN("[LTABLE_BG] fail to get table schema",
             K(ret), K_(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    LOG_TRACE("[LTABLE_BG] table schema not found, skip", K(table_id));
  } else if (OB_FAIL(table_schema->get_tablet_ids(all_tablet_ids))) {
    LOG_WARN("[LTABLE_BG] fail to get tablet ids from schema",
             K(ret), K(table_id));
  } else if (OB_FAIL(ls_->get_tablet_svr()->get_all_tablet_ids(
          true /*except_ls_inner_tablet*/, local_tablet_ids))) {
    LOG_WARN("[LTABLE_BG] fail to get local tablet ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = all_tablet_ids.at(i);
      bool found = false;
      for (int64_t j = 0; !found && j < local_tablet_ids.count(); j++) {
        if (local_tablet_ids.at(j) == tablet_id) {
          found = true;
        }
      }
      if (found) {
        if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
          // non-partitioned table: tablet is local, no partition names needed
        } else {
          int64_t part_id = OB_INVALID_ID;
          int64_t subpart_id = OB_INVALID_ID;
          const share::schema::ObPartition *partition = NULL;
          if (OB_FAIL(table_schema->get_part_id_by_tablet(
                  tablet_id, part_id, subpart_id))) {
            LOG_WARN("[LTABLE_BG] fail to get part_id from tablet",
                     K(ret), K(tablet_id));
          } else if (OB_FAIL(table_schema->get_partition_by_part_id(
                  part_id, share::schema::CHECK_PARTITION_MODE_NORMAL,
                  partition))) {
            LOG_WARN("[LTABLE_BG] fail to get partition by part_id",
                     K(ret), K(part_id));
          } else if (OB_ISNULL(partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[LTABLE_BG] partition is null", K(ret), K(part_id));
          } else {
            const common::ObString &raw_name = partition->get_part_name();
            char *buf = static_cast<char *>(allocator.alloc(raw_name.length()));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("[LTABLE_BG] fail to alloc part name buf", K(ret));
            } else {
              MEMCPY(buf, raw_name.ptr(), raw_name.length());
              common::ObString part_name_copy(raw_name.length(), buf);
              if (OB_FAIL(part_names.push_back(part_name_copy))) {
                LOG_WARN("[LTABLE_BG] fail to push back part name", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::build_partition_clause_(
    const common::ObIArray<common::ObString> &part_names,
    common::ObSqlString &clause)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clause.assign("PARTITION ("))) {
    LOG_WARN("[LTABLE_BG] fail to init partition clause", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_names.count(); i++) {
    if (i > 0 && OB_FAIL(clause.append(", "))) {
      LOG_WARN("[LTABLE_BG] fail to append comma", K(ret));
    } else if (OB_FAIL(clause.append_fmt("`%.*s`",
                   static_cast<int>(part_names.at(i).length()),
                   part_names.at(i).ptr()))) {
      LOG_WARN("[LTABLE_BG] fail to append part name", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(clause.append(")"))) {
    LOG_WARN("[LTABLE_BG] fail to close partition clause", K(ret));
  }
  return ret;
}

int ObLTableBGTaskScheduler::scan_dropped_ltables_(
    common::ObIArray<ObLTableDroppedEntry> &entries)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  common::ObSEArray<common::ObString, 8> part_names;
  if (OB_INVALID_ID == sdk_ltables_table_id_) {
    share::schema::ObSchemaGetterGuard schema_guard;
    uint64_t database_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(resolve_sdk_database_id_(schema_guard, database_id))) {
      LOG_WARN("[LTABLE_BG] fail to resolve sdk database id", K(ret));
    } else if (OB_INVALID_ID == database_id) {
      // sdk_collections not found in any database, skip
    } else if (OB_FAIL(schema_guard.get_table_id(
            tenant_id_, database_id,
            common::ObString("sdk_ltables"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            table_id))) {
      LOG_WARN("[LTABLE_BG] fail to get sdk_ltables table id", K(ret));
    } else if (OB_INVALID_ID != table_id) {
      sdk_ltables_table_id_ = table_id;
    }
  }
  bool sdk_ltables_is_partitioned = false;
  if (OB_SUCC(ret) && OB_INVALID_ID != sdk_ltables_table_id_) {
    share::schema::ObSchemaGetterGuard sg;
    const share::schema::ObTableSchema *ts = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, sg))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(sg.get_table_schema(
            tenant_id_, sdk_ltables_table_id_, ts))) {
      LOG_WARN("[LTABLE_BG] fail to get table schema", K(ret));
    } else if (OB_ISNULL(ts)) {
      // table schema not found, skip partition lookup
    } else if (ts->is_partitioned_table()) {
      sdk_ltables_is_partitioned = true;
      if (OB_FAIL(get_local_partition_names_for_table_(
              sdk_ltables_table_id_, part_names, allocator))) {
        LOG_WARN("[LTABLE_BG] fail to get local partitions for sdk_ltables",
                 K(ret));
      }
    }
  }
  // If cache is set but no local partitions found, the table may have been
  // dropped and recreated with a new table_id. Re-resolve by name so we can
  // pick up the new id and retry the partition lookup within the same tick.
  if (OB_SUCC(ret) && sdk_ltables_is_partitioned
      && OB_INVALID_ID != sdk_ltables_table_id_
      && 0 == part_names.count()) {
    int tmp_ret = OB_SUCCESS;
    share::schema::ObSchemaGetterGuard sg;
    uint64_t cur_db_id = OB_INVALID_ID;
    uint64_t cur_table_id = OB_INVALID_ID;
    if (OB_TMP_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, sg))) {
      // transient error, leave cache as-is and retry next tick
    } else if (OB_TMP_FAIL(resolve_sdk_database_id_(sg, cur_db_id))) {
      // transient error
    } else if (OB_INVALID_ID == cur_db_id) {
      sdk_ltables_table_id_ = OB_INVALID_ID;
    } else if (OB_TMP_FAIL(sg.get_table_id(
            tenant_id_, cur_db_id,
            common::ObString("sdk_ltables"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            cur_table_id))) {
      // transient error
    } else if (cur_table_id != sdk_ltables_table_id_) {
      LOG_TRACE("[LTABLE_BG] sdk_ltables table_id changed, refreshing cache",
                K_(sdk_ltables_table_id), K(cur_table_id));
      sdk_ltables_table_id_ = cur_table_id;
      if (OB_INVALID_ID != sdk_ltables_table_id_) {
        part_names.reset();
        if (OB_TMP_FAIL(get_local_partition_names_for_table_(
                sdk_ltables_table_id_, part_names, allocator))) {
          LOG_WARN("[LTABLE_BG] fail to get local partitions after table_id refresh",
                   K(tmp_ret));
          part_names.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret) && sdk_ltables_is_partitioned && 0 == part_names.count()) {
    // partitioned table with no local partitions on this LS
  } else if (OB_SUCC(ret) && OB_INVALID_ID == sdk_ltables_table_id_) {
    // sdk_ltables not yet created, skip
  } else if (OB_SUCC(ret)) {
    bool use_partition = (part_names.count() > 0);
    common::ObSqlString partition_clause;
    common::ObSqlString sql;
    if (use_partition
        && OB_FAIL(build_partition_clause_(part_names, partition_clause))) {
      LOG_WARN("[LTABLE_BG] fail to build partition clause", K(ret));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT lt.ltable_id, lt.namespace_id, lt.collection_id, lt.ltable_name, "
            "       time_to_usec(lt.updated_at) AS recycle_time_us, "
            "       IFNULL(c.collection_name, '') AS collection_name, "
            "       IFNULL(ns.namespace_name, '') AS namespace_name "
            "FROM `%s`.`sdk_ltables` %.*s AS lt "
            "LEFT JOIN `%s`.`sdk_collections` c ON c.collection_id = lt.collection_id "
            "LEFT JOIN `%s`.`sdk_namespaces` ns ON ns.namespace_id = lt.namespace_id "
            "  AND ns.collection_id = lt.collection_id "
            "WHERE lt.ltable_name LIKE '__recyclebin\\_%%' "
            "AND lt.ltable_name REGEXP '^__recyclebin_.+_[0-9]+$' "
            "AND lt.ltable_id > %ld "
            "ORDER BY lt.ltable_id LIMIT %ld",
            sdk_database_name_,
            static_cast<int>(partition_clause.length()),
            partition_clause.ptr(),
            sdk_database_name_,
            sdk_database_name_,
            last_scanned_ltable_id_,
            SCAN_LIMIT))) {
      LOG_WARN("[LTABLE_BG] fail to construct scan_dropped_ltables sql",
               K(ret));
    } else {
      SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("[LTABLE_BG] fail to execute scan_dropped_ltables sql",
                   K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[LTABLE_BG] result is null", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
              LOG_WARN("[LTABLE_BG] fail to get next row", K(ret));
            } else {
              ObLTableDroppedEntry entry;
              common::ObString name_str;
              common::ObString coll_id_str;
              common::ObString coll_name_str;
              common::ObString ns_name_str;
              EXTRACT_UINT_FIELD_MYSQL(*result, "ltable_id",
                                     entry.ltable_id_, uint64_t);
              EXTRACT_UINT_FIELD_MYSQL(*result, "namespace_id",
                                     entry.namespace_id_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "recycle_time_us",
                                      entry.recycle_time_us_, int64_t);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_id", coll_id_str);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ltable_name", name_str);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_name", coll_name_str);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "namespace_name", ns_name_str);
              if (OB_SUCC(ret)) {
                int64_t copy_len = MIN(static_cast<int64_t>(coll_id_str.length()),
                    static_cast<int64_t>(sizeof(entry.collection_id_) - 1));
                MEMCPY(entry.collection_id_, coll_id_str.ptr(), copy_len);
                entry.collection_id_[copy_len] = '\0';
                copy_len = MIN(static_cast<int64_t>(name_str.length()),
                    static_cast<int64_t>(sizeof(entry.ltable_name_) - 1));
                MEMCPY(entry.ltable_name_, name_str.ptr(), copy_len);
                entry.ltable_name_[copy_len] = '\0';
                copy_len = MIN(static_cast<int64_t>(coll_name_str.length()),
                    static_cast<int64_t>(sizeof(entry.collection_name_) - 1));
                MEMCPY(entry.collection_name_, coll_name_str.ptr(), copy_len);
                entry.collection_name_[copy_len] = '\0';
                copy_len = MIN(static_cast<int64_t>(ns_name_str.length()),
                    static_cast<int64_t>(sizeof(entry.namespace_name_) - 1));
                MEMCPY(entry.namespace_name_, ns_name_str.ptr(), copy_len);
                entry.namespace_name_[copy_len] = '\0';
                if (OB_FAIL(entries.push_back(entry))) {
                  LOG_WARN("[LTABLE_BG] fail to push back ltable entry",
                           K(ret));
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

int ObLTableBGTaskScheduler::scan_dropped_namespaces_(
    common::ObIArray<ObNamespaceDroppedEntry> &entries)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  common::ObSEArray<common::ObString, 8> part_names;
  if (OB_INVALID_ID == sdk_namespaces_table_id_) {
    share::schema::ObSchemaGetterGuard schema_guard;
    uint64_t database_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(resolve_sdk_database_id_(schema_guard, database_id))) {
      LOG_WARN("[LTABLE_BG] fail to resolve sdk database id", K(ret));
    } else if (OB_INVALID_ID == database_id) {
      // sdk tables not yet created, skip
    } else if (OB_FAIL(schema_guard.get_table_id(
            tenant_id_, database_id,
            common::ObString("sdk_namespaces"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            table_id))) {
      LOG_WARN("[LTABLE_BG] fail to get sdk_namespaces table id", K(ret));
    } else if (OB_INVALID_ID != table_id) {
      sdk_namespaces_table_id_ = table_id;
    }
  }
  bool sdk_namespaces_is_partitioned = false;
  if (OB_SUCC(ret) && OB_INVALID_ID != sdk_namespaces_table_id_) {
    share::schema::ObSchemaGetterGuard sg;
    const share::schema::ObTableSchema *ts = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, sg))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(sg.get_table_schema(
            tenant_id_, sdk_namespaces_table_id_, ts))) {
      LOG_WARN("[LTABLE_BG] fail to get table schema", K(ret));
    } else if (OB_ISNULL(ts)) {
      // table schema not found, skip partition lookup
    } else if (ts->is_partitioned_table()) {
      sdk_namespaces_is_partitioned = true;
      if (OB_FAIL(get_local_partition_names_for_table_(
              sdk_namespaces_table_id_, part_names, allocator))) {
        LOG_WARN("[LTABLE_BG] fail to get local partitions for sdk_namespaces",
                 K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && sdk_namespaces_is_partitioned
      && OB_INVALID_ID != sdk_namespaces_table_id_
      && 0 == part_names.count()) {
    int tmp_ret = OB_SUCCESS;
    share::schema::ObSchemaGetterGuard sg;
    uint64_t cur_db_id = OB_INVALID_ID;
    uint64_t cur_table_id = OB_INVALID_ID;
    if (OB_TMP_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, sg))) {
      // transient error, leave cache as-is and retry next tick
    } else if (OB_TMP_FAIL(resolve_sdk_database_id_(sg, cur_db_id))) {
      // transient error
    } else if (OB_INVALID_ID == cur_db_id) {
      sdk_namespaces_table_id_ = OB_INVALID_ID;
    } else if (OB_TMP_FAIL(sg.get_table_id(
            tenant_id_, cur_db_id,
            common::ObString("sdk_namespaces"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            cur_table_id))) {
      // transient error
    } else if (cur_table_id != sdk_namespaces_table_id_) {
      LOG_TRACE("[LTABLE_BG] sdk_namespaces table_id changed, refreshing cache",
                K_(sdk_namespaces_table_id), K(cur_table_id));
      sdk_namespaces_table_id_ = cur_table_id;
      if (OB_INVALID_ID != sdk_namespaces_table_id_) {
        part_names.reset();
        if (OB_TMP_FAIL(get_local_partition_names_for_table_(
                sdk_namespaces_table_id_, part_names, allocator))) {
          LOG_WARN("[LTABLE_BG] fail to get local partitions after table_id refresh",
                   K(tmp_ret));
          part_names.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret) && sdk_namespaces_is_partitioned && 0 == part_names.count()) {
    // partitioned table with no local partitions on this LS
  } else if (OB_SUCC(ret) && OB_INVALID_ID == sdk_namespaces_table_id_) {
    // sdk_namespaces not yet created, skip
  } else if (OB_SUCC(ret)) {
    bool use_partition = (part_names.count() > 0);
    common::ObSqlString partition_clause;
    common::ObSqlString sql;
    if (use_partition
        && OB_FAIL(build_partition_clause_(part_names, partition_clause))) {
      LOG_WARN("[LTABLE_BG] fail to build partition clause", K(ret));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT ns.namespace_id, ns.collection_id, ns.namespace_name, "
            "       time_to_usec(ns.updated_at) AS recycle_time_us, "
            "       IFNULL(c.collection_name, '') AS collection_name "
            "FROM `%s`.`sdk_namespaces` %.*s AS ns "
            "LEFT JOIN `%s`.`sdk_collections` c ON c.collection_id = ns.collection_id "
            "WHERE ns.namespace_name LIKE '__recyclebin\\_%%' "
            "AND ns.namespace_name REGEXP '^__recyclebin_.+_[0-9]+$' "
            "AND ns.namespace_id > %ld "
            "ORDER BY ns.namespace_id LIMIT %ld",
            sdk_database_name_,
            static_cast<int>(partition_clause.length()),
            partition_clause.ptr(),
            sdk_database_name_,
            last_scanned_namespace_id_,
            SCAN_LIMIT))) {
      LOG_WARN("[LTABLE_BG] fail to construct scan_dropped_namespaces sql",
               K(ret));
    } else {
      SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("[LTABLE_BG] fail to execute scan_dropped_namespaces sql",
                   K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[LTABLE_BG] result is null", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
              LOG_WARN("[LTABLE_BG] fail to get next row", K(ret));
            } else {
              ObNamespaceDroppedEntry entry;
              common::ObString name_str;
              common::ObString coll_id_str;
              common::ObString coll_name_str;
              EXTRACT_UINT_FIELD_MYSQL(*result, "namespace_id",
                                     entry.namespace_id_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "recycle_time_us",
                                      entry.recycle_time_us_, int64_t);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_id", coll_id_str);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "namespace_name", name_str);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_name", coll_name_str);
              if (OB_SUCC(ret)) {
                int64_t copy_len = MIN(
                    static_cast<int64_t>(coll_id_str.length()),
                    static_cast<int64_t>(sizeof(entry.collection_id_) - 1));
                MEMCPY(entry.collection_id_, coll_id_str.ptr(), copy_len);
                entry.collection_id_[copy_len] = '\0';
                copy_len = MIN(
                    static_cast<int64_t>(name_str.length()),
                    static_cast<int64_t>(sizeof(entry.namespace_name_) - 1));
                MEMCPY(entry.namespace_name_, name_str.ptr(), copy_len);
                entry.namespace_name_[copy_len] = '\0';
                copy_len = MIN(
                    static_cast<int64_t>(coll_name_str.length()),
                    static_cast<int64_t>(sizeof(entry.collection_name_) - 1));
                MEMCPY(entry.collection_name_, coll_name_str.ptr(), copy_len);
                entry.collection_name_[copy_len] = '\0';
                if (OB_FAIL(entries.push_back(entry))) {
                  LOG_WARN("[LTABLE_BG] fail to push back namespace entry",
                           K(ret));
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

int ObLTableBGTaskScheduler::scan_all_collections_(
    common::ObIArray<ObCollectionInfo> &collections)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_STRING);
  common::ObSEArray<common::ObString, 8> part_names;
  // 1. resolve sdk_database_id_ if not yet cached
  if (OB_INVALID_ID == sdk_database_id_) {
    share::schema::ObSchemaGetterGuard schema_guard;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(resolve_sdk_database_id_(schema_guard, database_id))) {
      LOG_WARN("[LTABLE_BG] fail to resolve sdk database id", K(ret));
    }
  }
  // 2. resolve sdk_collections table_id if not yet cached
  if (OB_SUCC(ret) && OB_INVALID_ID == sdk_collections_table_id_) {
    share::schema::ObSchemaGetterGuard schema_guard;
    uint64_t database_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(resolve_sdk_database_id_(schema_guard, database_id))) {
      LOG_WARN("[LTABLE_BG] fail to resolve sdk database id", K(ret));
    } else if (OB_INVALID_ID == database_id) {
      // sdk_collections not found in any database, skip
    } else if (OB_FAIL(schema_guard.get_table_id(
            tenant_id_, database_id,
            common::ObString("sdk_collections"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            table_id))) {
      LOG_WARN("[LTABLE_BG] fail to get sdk_collections table id", K(ret));
    } else if (OB_INVALID_ID != table_id) {
      sdk_collections_table_id_ = table_id;
    }
  }
  // 3. get local partition names for sdk_collections (partitioned tables only)
  bool sdk_collections_is_partitioned = false;
  if (OB_SUCC(ret) && OB_INVALID_ID != sdk_collections_table_id_) {
    share::schema::ObSchemaGetterGuard sg;
    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            tenant_id_, sg))) {
      LOG_WARN("[LTABLE_BG] fail to get schema guard", K(ret));
    } else if (OB_FAIL(sg.get_table_schema(
            tenant_id_, sdk_collections_table_id_, table_schema))) {
      LOG_WARN("[LTABLE_BG] fail to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      // table schema not found, skip partition lookup
    } else if (table_schema->is_partitioned_table()) {
      sdk_collections_is_partitioned = true;
      if (OB_FAIL(get_local_partition_names_for_table_(
              sdk_collections_table_id_, part_names, allocator))) {
        LOG_WARN("[LTABLE_BG] fail to get local partitions for sdk_collections",
                 K(ret));
      }
    }
  }
  // 4. If partitioned table cache is set but no local partitions, table may
  //    have been dropped and recreated. Re-resolve by name to pick up new id.
  if (OB_SUCC(ret) && sdk_collections_is_partitioned
      && OB_INVALID_ID != sdk_collections_table_id_
      && 0 == part_names.count()) {
    int tmp_ret = OB_SUCCESS;
    share::schema::ObSchemaGetterGuard sg;
    uint64_t cur_db_id = OB_INVALID_ID;
    uint64_t cur_table_id = OB_INVALID_ID;
    if (OB_TMP_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, sg))) {
      // transient error, leave cache as-is and retry next tick
    } else if (OB_TMP_FAIL(resolve_sdk_database_id_(sg, cur_db_id))) {
      // transient error
    } else if (OB_INVALID_ID == cur_db_id) {
      sdk_collections_table_id_ = OB_INVALID_ID;
    } else if (OB_TMP_FAIL(sg.get_table_id(
            tenant_id_, cur_db_id,
            common::ObString("sdk_collections"),
            false /*is_index*/,
            share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            cur_table_id))) {
      // transient error
    } else if (cur_table_id != sdk_collections_table_id_) {
      LOG_TRACE("[LTABLE_BG] sdk_collections table_id changed, refreshing cache",
                K_(sdk_collections_table_id), K(cur_table_id));
      sdk_collections_table_id_ = cur_table_id;
      if (OB_INVALID_ID != sdk_collections_table_id_) {
        part_names.reset();
        if (OB_TMP_FAIL(get_local_partition_names_for_table_(
                sdk_collections_table_id_, part_names, allocator))) {
          LOG_WARN("[LTABLE_BG] fail to get local partitions after table_id refresh",
                   K(tmp_ret));
          part_names.reset();
        }
      }
    }
  }
  // 5. execute scan: use PARTITION clause for partitioned tables,
  //    otherwise query directly (for non-partitioned tables or when
  //    partition info is unavailable)
  if (OB_SUCC(ret) && OB_INVALID_ID == sdk_collections_table_id_) {
    // no table found, nothing to do
  } else if (OB_SUCC(ret)) {
    bool use_partition = (part_names.count() > 0);
    common::ObSqlString partition_clause;
    common::ObSqlString sql;
    const int64_t scan_limit = SCAN_LIMIT * 10;
    if (use_partition
        && OB_FAIL(build_partition_clause_(part_names, partition_clause))) {
      LOG_WARN("[LTABLE_BG] fail to build partition clause", K(ret));
    }
    if (OB_SUCC(ret)
        && OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
                sdk_database_name_, "sdk_database_name"))) {
      LOG_WARN("[LTABLE_BG] sdk_database_name rejected for scan", K(ret));
    } else if (OB_SUCC(ret)
        && last_scanned_collection_id_[0] != '\0'
        && OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
                last_scanned_collection_id_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[LTABLE_BG] invalid last_scanned_collection_id, reset & reject",
               K(ret), KCSTRING(last_scanned_collection_id_));
      last_scanned_collection_id_[0] = '\0';
    }
    if (OB_SUCC(ret)) {
      if (use_partition) {
        if (last_scanned_collection_id_[0] != '\0') {
          if (OB_FAIL(sql.assign_fmt(
              "SELECT collection_id, collection_name "
              "FROM `%s`.`sdk_collections` %.*s "
              "WHERE collection_id > '%s' "
              "ORDER BY collection_id LIMIT %ld",
              sdk_database_name_,
              static_cast<int>(partition_clause.length()),
              partition_clause.ptr(),
              last_scanned_collection_id_, scan_limit))) {
            LOG_WARN("[LTABLE_BG] fail to construct sql", K(ret));
          }
        } else {
          if (OB_FAIL(sql.assign_fmt(
              "SELECT collection_id, collection_name "
              "FROM `%s`.`sdk_collections` %.*s "
              "ORDER BY collection_id LIMIT %ld",
              sdk_database_name_,
              static_cast<int>(partition_clause.length()),
              partition_clause.ptr(),
              scan_limit))) {
            LOG_WARN("[LTABLE_BG] fail to construct sql", K(ret));
          }
        }
      } else {
        if (last_scanned_collection_id_[0] != '\0') {
          if (OB_FAIL(sql.assign_fmt(
              "SELECT collection_id, collection_name "
              "FROM `%s`.`sdk_collections` "
              "WHERE collection_id > '%s' "
              "ORDER BY collection_id LIMIT %ld",
              sdk_database_name_,
              last_scanned_collection_id_, scan_limit))) {
            LOG_WARN("[LTABLE_BG] fail to construct sql", K(ret));
          }
        } else {
          if (OB_FAIL(sql.assign_fmt(
              "SELECT collection_id, collection_name "
              "FROM `%s`.`sdk_collections` "
              "ORDER BY collection_id LIMIT %ld",
              sdk_database_name_,
              scan_limit))) {
            LOG_WARN("[LTABLE_BG] fail to construct sql", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("[LTABLE_BG] fail to construct scan_all_collections sql",
                 K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("[LTABLE_BG] fail to execute scan_all_collections sql",
                   K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[LTABLE_BG] result is null", K(ret));
        } else {
          char last_id_buf[64];
          MEMCPY(last_id_buf, last_scanned_collection_id_,
                 sizeof(last_id_buf));
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
              LOG_WARN("[LTABLE_BG] fail to get next row from sdk_collections",
                       K(ret));
            } else {
              ObCollectionInfo info;
              common::ObString coll_id_str;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_id", coll_id_str);
              common::ObString name_str;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_name", name_str);
              if (OB_SUCC(ret)) {
                int64_t coll_id_len = MIN(
                    static_cast<int64_t>(coll_id_str.length()),
                    static_cast<int64_t>(sizeof(info.collection_id_) - 1));
                MEMCPY(info.collection_id_, coll_id_str.ptr(), coll_id_len);
                info.collection_id_[coll_id_len] = '\0';
                int64_t len = MIN(
                    static_cast<int64_t>(name_str.length()),
                    static_cast<int64_t>(sizeof(info.collection_name_buf_) - 1));
                MEMCPY(info.collection_name_buf_, name_str.ptr(), len);
                info.collection_name_buf_[len] = '\0';
                info.collection_name_.assign_ptr(
                    info.collection_name_buf_,
                    static_cast<int32_t>(len));
                if (OB_FAIL(collections.push_back(info))) {
                  LOG_WARN("[LTABLE_BG] fail to push back collection info",
                           K(ret));
                } else {
                  // Re-point collection_name_ to the copied element's own
                  // buffer, since push_back copies the struct but the
                  // ObString pointer still references the original stack buf.
                  ObCollectionInfo &copied = collections.at(collections.count() - 1);
                  copied.collection_name_.assign_ptr(
                      copied.collection_name_buf_,
                      static_cast<int32_t>(len));
                  MEMCPY(last_id_buf, info.collection_id_, sizeof(last_id_buf));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (collections.count() < scan_limit) {
              last_scanned_collection_id_[0] = '\0';
            } else {
              MEMCPY(last_scanned_collection_id_, last_id_buf,
                     sizeof(last_scanned_collection_id_));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::count_ltables_under_namespace_(
    const char *collection_id, int64_t namespace_id, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  common::ObSqlString sql;
  // exclude recyclebin entries so ongoing async-delete ltables do not
  // block namespace deletion from proceeding.
  if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          sdk_database_name_, "sdk_database_name"))) {
    LOG_WARN("[LTABLE_BG] sdk_database_name rejected", K(ret));
  } else if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          collection_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject count_ltables",
             K(ret), KCSTRING(collection_id));
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT COUNT(*) AS cnt FROM `%s`.`sdk_ltables` "
          "WHERE collection_id = '%s' AND namespace_id = %ld "
          "AND ltable_name NOT LIKE '__recyclebin\\_%%'",
          sdk_database_name_, collection_id, namespace_id))) {
    LOG_WARN("[LTABLE_BG] fail to construct count_ltables sql", K(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("[LTABLE_BG] fail to execute count_ltables sql",
                 K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LTABLE_BG] result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("[LTABLE_BG] fail to get count result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::check_drop_task_completed_(
    const char *collection_id,
    int64_t namespace_id,
    int64_t ltable_id,
    int64_t recycle_time_us,
    const char *operation_type,
    bool &is_completed)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  common::ObSqlString sql;
  is_completed = false;
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(collection_id)
      || recycle_time_us <= 0 || OB_ISNULL(operation_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid argument for completion check",
             K(ret), KCSTRING(collection_id), KP(operation_type));
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT COUNT(*) AS cnt FROM %s "
          "WHERE tenant_id = %lu AND database_id = %lu "
          "AND collection_id = '%s' AND namespace_id = %ld AND ltable_id = %ld "
          "AND operation_type = '%s' AND operation_status = 'COMPLETED' "
          "AND gmt_modified >= usec_to_time(%ld)",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME,
          tenant_id_, sdk_database_id_, collection_id, namespace_id,
          ltable_id, operation_type, recycle_time_us))) {
    LOG_WARN("[LTABLE_BG] fail to construct completion check sql", K(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(
              res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
        LOG_WARN("[LTABLE_BG] fail to query drop task history", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LTABLE_BG] completion check result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("[LTABLE_BG] fail to get completion check result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
        if (OB_SUCC(ret)) {
          is_completed = count > 0;
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::check_drop_task_failed_(
    const char *collection_id,
    int64_t namespace_id,
    int64_t ltable_id,
    int64_t recycle_time_us,
    const char *operation_type,
    bool &is_failed)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  common::ObSqlString sql;
  is_failed = false;
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(collection_id)
      || recycle_time_us <= 0 || OB_ISNULL(operation_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid argument for failure check",
             K(ret), KCSTRING(collection_id), KP(operation_type));
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT COUNT(*) AS cnt FROM %s "
          "WHERE tenant_id = %lu AND database_id = %lu "
          "AND collection_id = '%s' AND namespace_id = %ld AND ltable_id = %ld "
          "AND operation_type = '%s' AND operation_status = 'FAILED' "
          "AND gmt_modified >= usec_to_time(%ld) "
          // Keep permanent failures terminal. Transient failures become eligible
          // again after a persisted cooldown, including after a leader switch.
          "AND (ret_code NOT IN ('OB_EAGAIN', 'OB_TIMEOUT', 'OB_TRANS_TIMEOUT', "
          "'OB_NEED_RETRY', 'OB_NOT_MASTER', 'OB_LS_NOT_EXIST', "
          "'OB_PARTITION_NOT_EXIST', 'OB_LOCATION_NOT_EXIST', "
          "'OB_SCHEMA_EAGAIN', 'OB_TENANT_SCHEMA_NOT_FULL', 'OB_ERR_REMOTE_SCHEMA_NOT_FULL', "
          "'OB_ALLOCATE_MEMORY_FAILED', 'OB_ENTRY_NOT_EXIST') "
          "OR gmt_modified > DATE_SUB(now(6), INTERVAL %ld MICROSECOND))",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME,
          tenant_id_, sdk_database_id_, collection_id, namespace_id,
          ltable_id, operation_type, recycle_time_us, DROP_RETRY_INTERVAL_US))) {
    LOG_WARN("[LTABLE_BG] fail to construct failure check sql", K(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(
              res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
        LOG_WARN("[LTABLE_BG] fail to query failed drop task history", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LTABLE_BG] failure check result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("[LTABLE_BG] fail to get failure check result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
        if (OB_SUCC(ret)) {
          is_failed = count > 0;
        }
      }
    }
  }
  return ret;
}

int ObLTableBGTaskScheduler::cleanup_ltable_mapping_(
    const ObLTableDroppedEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  common::ObCStringHelper helper;
  const char *escaped_name = helper.convert(common::ObHexEscapeSqlStr(
      common::ObString(static_cast<int64_t>(strnlen(
          entry.ltable_name_, sizeof(entry.ltable_name_) - 1)),
          entry.ltable_name_)));
  if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          sdk_database_name_, "sdk_database_name"))) {
  } else if (OB_ISNULL(escaped_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[LTABLE_BG] fail to escape ltable recyclebin name", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%s`.`sdk_ltables` "
          "WHERE ltable_id = %ld AND namespace_id = %ld "
          "AND collection_id = '%s' AND ltable_name = '%s' "
          "AND time_to_usec(updated_at) = %ld",
          sdk_database_name_, entry.ltable_id_, entry.namespace_id_,
          entry.collection_id_, escaped_name, entry.recycle_time_us_))) {
    LOG_WARN("[LTABLE_BG] fail to construct ltable mapping cleanup sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to cleanup ltable mapping", K(ret), K(entry));
  } else {
    LOG_INFO("[LTABLE_BG] cleanup completed ltable mapping",
             K(affected_rows), K(entry));
  }
  return ret;
}

int ObLTableBGTaskScheduler::cleanup_namespace_mapping_(
    const ObNamespaceDroppedEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  common::ObCStringHelper helper;
  const char *escaped_name = helper.convert(common::ObHexEscapeSqlStr(
      common::ObString(static_cast<int64_t>(strnlen(
          entry.namespace_name_, sizeof(entry.namespace_name_) - 1)),
          entry.namespace_name_)));
  if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          sdk_database_name_, "sdk_database_name"))) {
  } else if (OB_ISNULL(escaped_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[LTABLE_BG] fail to escape namespace recyclebin name", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%s`.`sdk_namespaces` "
          "WHERE namespace_id = %ld AND collection_id = '%s' "
          "AND namespace_name = '%s' AND time_to_usec(updated_at) = %ld",
          sdk_database_name_, entry.namespace_id_, entry.collection_id_,
          escaped_name, entry.recycle_time_us_))) {
    LOG_WARN("[LTABLE_BG] fail to construct namespace mapping cleanup sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to cleanup namespace mapping", K(ret), K(entry));
  } else {
    LOG_INFO("[LTABLE_BG] cleanup completed namespace mapping",
             K(affected_rows), K(entry));
  }
  return ret;
}

int ObLTableBGTaskScheduler::extract_original_name_(
    const char *recyclebin_name, char *original_name, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char *prefix = "__recyclebin_";
  const int64_t prefix_len = 13;
  const char *suffix = NULL;
  const char *pos = NULL;
  int64_t name_len = 0;
  if (OB_ISNULL(recyclebin_name) || OB_ISNULL(original_name) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid argument for original name extraction",
             K(ret), KP(recyclebin_name), KP(original_name), K(buf_len));
  } else {
    name_len = static_cast<int64_t>(strlen(recyclebin_name));
    suffix = strrchr(recyclebin_name, '_');
    if (name_len <= prefix_len || 0 != STRNCMP(recyclebin_name, prefix, prefix_len)
        || OB_ISNULL(suffix) || suffix <= recyclebin_name + prefix_len
        || '\0' == *(suffix + 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[LTABLE_BG] invalid recyclebin name", K(ret), KCSTRING(recyclebin_name));
    }
  }
  pos = OB_SUCC(ret) ? suffix + 1 : NULL;
  for (; OB_SUCC(ret) && '\0' != *pos; ++pos) {
    if (*pos < '0' || *pos > '9') {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[LTABLE_BG] invalid recyclebin timestamp suffix",
               K(ret), KCSTRING(recyclebin_name));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t original_len = suffix - recyclebin_name - prefix_len;
    if (original_len >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[LTABLE_BG] original name buffer is too small",
               K(ret), K(original_len), K(buf_len));
    } else {
      MEMCPY(original_name, recyclebin_name + prefix_len, original_len);
      original_name[original_len] = '\0';
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

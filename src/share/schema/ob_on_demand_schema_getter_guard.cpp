/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/schema/ob_original_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_simple_schema_mode.h"
#include "share/schema/ob_schema_scan_iterator.h"
#include "share/schema/ob_schema_scan_operator.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_server_struct.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_sort.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

const ObOnDemandSchemaGetterGuard &ObOnDemandSchemaGetterGuard::instance()
{
  static const ObOnDemandSchemaGetterGuard INSTANCE;
  return INSTANCE;
}

int64_t ObOnDemandSchemaGetterGuard::get_load_deadline_()
{
  const bool has_worker_timeout = THIS_WORKER.is_timeout_ts_valid();
  return ObOnDemandSchemaRetryPolicy::get_load_deadline(
      has_worker_timeout,
      has_worker_timeout ? THIS_WORKER.get_timeout_ts() : 0,
      ObTimeUtility::current_time());
}

int64_t ObOnDemandSchemaGetterGuard::get_wait_deadline_(
    const int64_t load_deadline_us)
{
  return MIN(load_deadline_us,
             ObTimeUtility::current_time() + LOAD_WAIT_TIMEOUT_US);
}

int ObOnDemandSchemaGetterGuard::do_backoff_sleep_(
    const int64_t abs_deadline_us,
    int64_t &retry_count)
{
  int64_t backoff_us = 0;
  const int ret = ObOnDemandSchemaRetryPolicy::calc_backoff_info(
      abs_deadline_us, ObTimeUtility::current_time(), retry_count, backoff_us);
  if (OB_SUCCESS == ret && backoff_us > 0) {
    ob_usleep(backoff_us);
  }
  return ret;
}

ObTableNameNamespace ObOnDemandSchemaGetterGuard::get_table_namespace_(
    const bool is_index,
    const bool with_hidden_flag,
    const bool is_built_in_index)
{
  return with_hidden_flag ? NS_HIDDEN_TABLE
      : (is_index ? (is_built_in_index ? NS_BUILT_IN_INDEX : NS_NORMAL_INDEX)
                  : NS_NORMAL_TABLE);
}

int ObOnDemandSchemaGetterGuard::get_publish_snapshot_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const int64_t snapshot_version,
    const int64_t abs_deadline_us,
    ObTenantSchemaPublishState *&state,
    ObSchemaPublishSnapshot &publish_snapshot) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgrInfo *schema_mgr_info = NULL;
  state = NULL;
  publish_snapshot.reset();
  if (OB_ISNULL(guard.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(guard.get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    LOG_WARN("failed to get schema manager info for publication state",
             KR(ret), K(tenant_id), K(snapshot_version));
  } else if (OB_ISNULL(schema_mgr_info)
             || OB_ISNULL(state = schema_mgr_info->get_publish_state())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant publication state is absent from schema guard",
             KR(ret), K(tenant_id), K(snapshot_version), KP(schema_mgr_info));
  } else if (state->get_tenant_id() != tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant publication state does not match schema guard",
              KR(ret), K(tenant_id), "state_tenant_id", state->get_tenant_id());
  } else if (OB_FAIL(state->get_published_snapshot(
                 snapshot_version, get_wait_deadline_(abs_deadline_us),
                 publish_snapshot))) {
    if (OB_SCHEMA_EAGAIN == ret) {
      LOG_TRACE("tenant schema publication is not ready", KR(ret),
                K(tenant_id), K(snapshot_version), K(publish_snapshot));
    } else {
      LOG_WARN("failed to get tenant schema publication state", KR(ret),
               K(tenant_id), K(snapshot_version), K(publish_snapshot));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_full_table_schema_(
    ObSchemaGetterGuard &guard,
    const ObSimpleTableSchemaV2 &simple_schema,
    const ObTableSchema *&table_schema) const
{
  return get_full_table_schema_(guard, simple_schema.get_tenant_id(),
                                simple_schema.get_table_id(),
                                simple_schema.get_schema_version(), table_schema);
}

int ObOnDemandSchemaGetterGuard::get_full_table_schema_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t schema_version,
    const ObTableSchema *&table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  ret = guard.get_from_local_cache(TABLE_SCHEMA, tenant_id, table_id, table_schema);
  if (OB_SUCC(ret)) {
    // already pinned in guard.schema_objs_
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("failed to read guard-local full table schema",
             KR(ret), K(tenant_id), K(table_id), K(schema_version));
  } else if (OB_FAIL(load_full_table_schema_(
                 guard, tenant_id, table_id, schema_version, table_schema))) {
    LOG_WARN("failed to load full table schema",
             KR(ret), K(tenant_id), K(table_id), K(schema_version));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::load_full_table_schema_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t schema_version,
    const ObTableSchema *&table_schema) const
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  ObKVCacheHandle handle;
  const ObSchema *base_schema = NULL;
  table_schema = NULL;
  if (OB_FAIL(guard.get_schema_status(tenant_id, schema_status))) {
    LOG_WARN("failed to get schema status for full table load",
             KR(ret), K(tenant_id), K(table_id), K(schema_version));
  } else if (OB_FAIL(guard.schema_service_->get_schema(
                 NULL, schema_status, TABLE_SCHEMA, table_id,
                 schema_version, handle, base_schema))) {
    LOG_WARN("failed to load full table schema in on-demand mode",
             KR(ret), K(tenant_id), K(table_id), K(schema_version));
  } else if (OB_ISNULL(base_schema)) {
    ret = OB_SCHEMA_EAGAIN;
  } else {
    table_schema = static_cast<const ObTableSchema *>(base_schema);
    if (OB_FAIL(guard.put_to_local_cache(
            TABLE_SCHEMA, tenant_id, table_id, table_schema, handle))) {
      // handle will unpin on scope exit; clear out-param to avoid dangling pointer.
      table_schema = NULL;
      LOG_WARN("failed to pin full table schema",
               KR(ret), K(tenant_id), K(table_id), K(schema_version));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::fetch_tablegroup_table_ids_uncached_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLEGROUP;
  spec.filter_id_ = tablegroup_id;
  spec.caller_tag_ = "fetch_tablegroup_table_ids_uncached_";
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_can_full_scan(tenant_id))) {
    LOG_WARN("full scan not allowed", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_ISNULL(guard.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else {
    int64_t snapshot_version = OB_INVALID_VERSION;
    int64_t stale_retry_count = 0;
    const int64_t abs_deadline_us = get_load_deadline_();
    bool done = false;
    if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
      LOG_WARN("fail to get tablegroup scan snapshot version", KR(ret), K(tenant_id));
    }
    while (OB_SUCC(ret) && !done) {
      ObTenantSchemaPublishState *state = NULL;
      ObSchemaPublishSnapshot token;
      if (ObTimeUtility::current_time() >= abs_deadline_us) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(get_publish_snapshot_(guard, tenant_id, snapshot_version,
                                        abs_deadline_us, state, token))) {
        LOG_WARN("fail to get tablegroup cache publication token",
                 KR(ret), K(tenant_id), K(tablegroup_id));
      } else {
        ret = guard.schema_service_->get_tablegroup_table_id_cache().get(
            tenant_id, token.cache_generations_.tablegroup_, tablegroup_id, table_ids);
        if (OB_SUCC(ret)) {
          if (ObTimeUtility::current_time() >= abs_deadline_us) {
            table_ids.reset();
            ret = OB_TIMEOUT;
          } else if (state->can_publish(token)) {
            done = true;
            LOG_TRACE("tablegroup table id kvcache hit", K(tenant_id), K(tablegroup_id),
                      K(token.cache_generations_.tablegroup_), "cnt", table_ids.count());
          } else {
            table_ids.reset();
            ret = do_backoff_sleep_(abs_deadline_us, stale_retry_count);
          }
        } else if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablegroup table id cache",
                   KR(ret), K(tenant_id), K(tablegroup_id),
                   K(token.cache_generations_.tablegroup_));
        } else {
          ret = OB_SUCCESS;
          ObMySQLProxy *sql_proxy = guard.schema_service_->get_sql_proxy();
          if (OB_ISNULL(sql_proxy)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sql_proxy is null", KR(ret));
          } else if (OB_FAIL(ObSchemaScanOperator::scan_table_ids(
                         *sql_proxy, spec, table_ids))) {
            LOG_WARN("fail to scan table ids", KR(ret), K(spec));
          } else if (ObTimeUtility::current_time() >= abs_deadline_us) {
            table_ids.reset();
            ret = OB_TIMEOUT;
          } else {
            const int publish_ret = guard.schema_service_->get_tablegroup_table_id_cache().
                put_if_current(tenant_id, token.cache_generations_.tablegroup_, tablegroup_id,
                               table_ids, *state, token);
            if (OB_SCHEMA_EAGAIN == publish_ret || !state->can_publish(token)) {
              table_ids.reset();
              ret = do_backoff_sleep_(abs_deadline_us, stale_retry_count);
            } else {
              done = true;
            }
            if (OB_SUCCESS != publish_ret && OB_SCHEMA_EAGAIN != publish_ret) {
              LOG_WARN("fail to put tablegroup table id cache, ignore",
                       KR(publish_ret), K(tenant_id), K(tablegroup_id),
                       K(token.cache_generations_.tablegroup_), "cnt", table_ids.count());
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::fetch_table_ids_uncached_(ObSchemaGetterGuard &guard,
                                                           const ObSchemaScanSpec &spec,
                                                           ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  table_ids.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (ObSchemaScanKind::BY_TABLEGROUP == spec.kind_) {
    if (OB_FAIL(fetch_tablegroup_table_ids_uncached_(
            guard, spec.tenant_id_, spec.filter_id_, table_ids))) {
      LOG_WARN("fail to fetch tablegroup table ids", KR(ret), K(spec));
    }
  } else if (OB_FAIL(guard.check_can_full_scan(spec.tenant_id_))) {
    LOG_WARN("full scan not allowed", KR(ret), K(spec));
  } else if (OB_ISNULL(sql_proxy = guard.schema_service_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObSchemaScanOperator::scan_table_ids(*sql_proxy, spec, table_ids))) {
    LOG_WARN("fail to scan table ids", KR(ret), K(spec));
  }
  return ret;
}

bool ObOnDemandSchemaGetterGuard::simple_matches_scan_spec_(
    const ObSimpleTableSchemaV2 &schema,
    const ObSchemaScanSpec &spec,
    const ObIArray<uint64_t> *encrypted_ts_ids)
{
  bool match = false;
  switch (spec.kind_) {
    case ObSchemaScanKind::BY_TENANT:
      match = true;
      break;
    case ObSchemaScanKind::BY_TABLEGROUP:
      match = (schema.get_tablegroup_id() == spec.filter_id_)
          && is_tablegroup_listed_table_type(schema.get_table_type());
      break;
    case ObSchemaScanKind::BY_TABLESPACE:
      match = (schema.get_tablespace_id() == spec.filter_id_);
      break;
    case ObSchemaScanKind::BY_DATABASE:
      match = (schema.get_database_id() == spec.filter_id_);
      break;
    case ObSchemaScanKind::BY_DUPLICATE_SCOPE:
      match = (0 != static_cast<int64_t>(schema.get_duplicate_scope()));
      break;
    case ObSchemaScanKind::BY_DYNAMIC_PARTITION:
      match = schema.with_dynamic_partition_policy();
      break;
    case ObSchemaScanKind::BY_TABLE_TYPE: {
      if (OB_NOT_NULL(spec.enum_values_)) {
        const int64_t tt = static_cast<int64_t>(schema.get_table_type());
        for (int64_t i = 0; !match && i < spec.enum_values_->count(); ++i) {
          match = (tt == spec.enum_values_->at(i));
        }
      }
      break;
    }
    case ObSchemaScanKind::BY_INDEX_TYPE: {
      if (OB_NOT_NULL(spec.enum_values_)) {
        const int64_t it = static_cast<int64_t>(schema.get_index_type());
        for (int64_t i = 0; !match && i < spec.enum_values_->count(); ++i) {
          match = (it == spec.enum_values_->at(i));
        }
      }
      break;
    }
    case ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE: {
      if (OB_NOT_NULL(encrypted_ts_ids)) {
        for (int64_t i = 0; !match && i < encrypted_ts_ids->count(); ++i) {
          if (schema.get_tablespace_id() == encrypted_ts_ids->at(i)) {
            match = true;
          }
        }
      }
      break;
    }
    default:
      match = false;
      break;
  }
  return match;
}

int ObOnDemandSchemaGetterGuard::append_core_table_ids_from_mgr_(
    ObSchemaGetterGuard &guard,
    const ObSchemaScanSpec &spec,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  ObArray<uint64_t> encrypted_ts_ids;
  const ObIArray<uint64_t> *encrypted_ptr = NULL;
  common::hash::ObHashSet<uint64_t> id_set;
  int64_t appended = 0;

  if (OB_FAIL(guard.check_tenant_schema_guard(spec.tenant_id_))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(spec));
  } else if (OB_FAIL(guard.get_schema_mgr(spec.tenant_id_, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(spec.tenant_id_)) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get schema mgr for core id append", KR(ret), K(spec));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema mgr is null", KR(ret), K(spec));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(spec.tenant_id_, schemas))) {
    LOG_WARN("fail to get table schemas from mgr", KR(ret), K(spec));
  } else if (OB_FAIL(id_set.create(MAX(16, table_ids.count() * 2)))) {
    LOG_WARN("fail to create id hash set", KR(ret), "cnt", table_ids.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      if (OB_FAIL(id_set.set_refactored(table_ids.at(i)))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to set id into hash set", KR(ret), "table_id", table_ids.at(i));
        }
      }
    }
  }

  if (OB_SUCC(ret) && ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec.kind_) {
    ObISQLClient *sql_client = NULL;
    if (OB_FAIL(guard.prepare_schema_scan_sql_client_(spec.tenant_id_, sql_client))) {
      LOG_WARN("fail to get encrypted tablespace scan client", KR(ret), K(spec));
    } else if (OB_ISNULL(sql_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypted tablespace scan client is null", KR(ret), K(spec));
    } else if (OB_FAIL(ObSchemaScanOperator::scan_encrypted_tablespace_ids(
                   *sql_client, spec.tenant_id_, encrypted_ts_ids))) {
      LOG_WARN("fail to scan encrypted tablespace ids", KR(ret), K(spec));
    } else {
      encrypted_ptr = &encrypted_ts_ids;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = schemas.at(i);
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", KR(ret), K(i));
    } else {
      const uint64_t table_id = schema->get_table_id();
      if (!is_core_table(table_id)) {
        // skip non-core
      } else if (!simple_matches_scan_spec_(*schema, spec, encrypted_ptr)) {
        // skip unmatched predicate
      } else {
        int hash_ret = id_set.exist_refactored(table_id);
        if (OB_HASH_EXIST == hash_ret) {
          // already from __all_table
        } else if (OB_HASH_NOT_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to check id exist", KR(ret), K(table_id));
        } else if (OB_FAIL(table_ids.push_back(table_id))) {
          LOG_WARN("fail to append core table id", KR(ret), K(table_id), K(spec));
        } else if (OB_FAIL(id_set.set_refactored(table_id))) {
          LOG_WARN("fail to record appended core table id", KR(ret), K(table_id));
        } else {
          ++appended;
        }
      }
    }
  }
  if (OB_SUCC(ret) && appended > 0) {
    LOG_TRACE("append core table ids from mgr",
              K(spec), K(appended), "total", table_ids.count());
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_ids_by_inner_table(ObSchemaGetterGuard &guard,
                                                             const ObSchemaScanSpec &spec,
                                                             ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (ObSchemaScanConsistencyMode::STRONG == guard.get_schema_scan_consistency_mode()) {
    ObISQLClient *sql_client = NULL;
    if (OB_FAIL(guard.check_can_full_scan(spec.tenant_id_))) {
      LOG_WARN("full scan not allowed", KR(ret), K(spec));
    } else if (OB_FAIL(guard.prepare_schema_scan_sql_client_(spec.tenant_id_, sql_client))) {
      LOG_WARN("fail to get schema scan sql client", KR(ret), K(spec));
    } else if (OB_ISNULL(sql_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema scan sql client is null", KR(ret), K(spec));
    } else if (OB_FAIL(ObSchemaScanOperator::scan_table_ids(*sql_client, spec, table_ids))) {
      LOG_WARN("fail to scan table ids in snapshot transaction", KR(ret), K(spec));
    }
  } else if (OB_FAIL(fetch_table_ids_uncached_(guard, spec, table_ids))) {
    // RELAXED tablegroup enumeration may use process kvcache.
    LOG_WARN("fail to fetch table ids uncached", KR(ret), K(spec));
  }
  // Physical __all_table omits is_core_table rows; union matching core ids from resident mgr.
  if (OB_SUCC(ret) && OB_FAIL(append_core_table_ids_from_mgr_(guard, spec, table_ids))) {
    LOG_WARN("fail to append core table ids from mgr", KR(ret), K(spec));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.caller_tag_ = "get_table_ids_in_tenant";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get table ids in tenant", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_ids_in_tenant_for_cdc(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  // Standalone CDC supplies its SQL proxy through SchemaService, not GCTX.
  ObMySQLProxy *sql_proxy = NULL;
  int64_t schema_version = OB_INVALID_VERSION;
  ObSchemaScanSpec core_spec;
  core_spec.tenant_id_ = tenant_id;
  core_spec.kind_ = ObSchemaScanKind::BY_TENANT;
  core_spec.caller_tag_ = "get_table_ids_in_tenant_for_cdc";
  table_ids.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_can_cdc_full_scan(tenant_id))) {
    LOG_WARN("cdc full scan not allowed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy = guard.schema_service_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObSchemaScanOperator::scan_alive_table_ids_from_history(
                 *sql_proxy, tenant_id, schema_version, table_ids))) {
    LOG_WARN("fail to scan alive table ids from history",
             KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(append_core_table_ids_from_mgr_(guard, core_spec, table_ids))) {
    // History omits core rows; union from resident mgr when available.
    // Without mgr (pure OnDemand), skip cores — CDC user paths mostly ignore sys/core.
    if (OB_SCHEMA_EAGAIN == ret || OB_TENANT_NOT_EXIST == ret) {
      LOG_WARN("skip appending core table ids without mgr",
               KR(ret), K(tenant_id), K(schema_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to append core table ids from mgr",
               KR(ret), K(tenant_id), K(schema_version));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_ids_in_database(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t database_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_DATABASE;
  spec.filter_id_ = database_id;
  spec.caller_tag_ = "get_table_ids_in_database";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get table ids in database",
             KR(ret), K(tenant_id), K(database_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_ids_in_tablegroup(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t tablegroup_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLEGROUP;
  spec.filter_id_ = tablegroup_id;
  spec.caller_tag_ = "get_table_ids_in_tablegroup";
  // BY_TABLEGROUP scan already restricts to user / mysql tmp / sys tables,
  // matching ObSchemaMgr::get_table_schemas_in_tablegroup.
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get table ids in tablegroup",
             KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

// -------------------- A-class full-scan wrappers (全量扫描.md §3.5) --------------------

int ObOnDemandSchemaGetterGuard::get_view_schemas_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  // Align with is_view_table(): USER_VIEW + SYSTEM_VIEW + MATERIALIZED_VIEW.
  ObSEArray<int64_t, 3> table_types;
  ObArray<uint64_t> table_ids;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLE_TYPE;
  spec.enum_values_ = &table_types;
  spec.caller_tag_ = "get_view_schemas_in_tenant";
  if (OB_FAIL(ObSchemaScanOperator::fill_view_table_types(table_types))) {
    LOG_WARN("fail to fill view table types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get view table ids", KR(ret), K(tenant_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_tmp_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> table_types;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLE_TYPE;
  spec.enum_values_ = &table_types;
  spec.caller_tag_ = "get_tmp_table_ids_in_tenant";
  if (OB_FAIL(ObSchemaScanOperator::fill_tmp_table_types(table_types))) {
    LOG_WARN("fail to fill tmp table types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get tmp table ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_external_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> table_types;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLE_TYPE;
  spec.enum_values_ = &table_types;
  spec.caller_tag_ = "get_external_table_ids_in_tenant";
  if (OB_FAIL(ObSchemaScanOperator::fill_external_table_types(table_types))) {
    LOG_WARN("fail to fill external table types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get external table ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_duplicate_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_DUPLICATE_SCOPE;
  spec.caller_tag_ = "get_duplicate_table_ids_in_tenant";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get duplicate table ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_encrypted_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE;
  spec.caller_tag_ = "get_encrypted_table_ids_in_tenant";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get encrypted table ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_dynamic_partition_table_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_DYNAMIC_PARTITION;
  spec.caller_tag_ = "get_dynamic_partition_table_ids_in_tenant";
  // Requires __all_table.has_dynamic_partition_policy (全量扫描.md §3.2).
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get dynamic partition table ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_vector_info_index_ids_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    bool &has_ivf_index,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 32> index_types;
  ObSEArray<int64_t, 32> ivf_index_types;
  ObArray<uint64_t> ivf_table_ids;
  ObSchemaScanSpec spec;
  ObSchemaScanSpec ivf_spec;
  table_ids.reset();
  has_ivf_index = false;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_INDEX_TYPE;
  spec.enum_values_ = &index_types;
  spec.caller_tag_ = "get_vector_info_index_ids_in_tenant";
  ivf_spec.tenant_id_ = tenant_id;
  ivf_spec.kind_ = ObSchemaScanKind::BY_INDEX_TYPE;
  ivf_spec.enum_values_ = &ivf_index_types;
  ivf_spec.caller_tag_ = "get_vector_info_index_ids_in_tenant_ivf";
  if (OB_FAIL(ObSchemaScanOperator::fill_vec_index_types(index_types))) {
    LOG_WARN("fail to fill vec index types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get vector index ids", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSchemaScanOperator::fill_vec_ivf_index_types(ivf_index_types))) {
    LOG_WARN("fail to fill vec ivf index types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, ivf_spec, ivf_table_ids))) {
    // STRONG: both queries use the same Guard transaction snapshot.
    LOG_WARN("fail to get vector ivf index ids", KR(ret), K(tenant_id));
  } else {
    has_ivf_index = !ivf_table_ids.empty();
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_with_async_index_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> index_types;
  ObArray<uint64_t> index_ids;
  ObSchemaScanSpec spec;
  common::hash::ObHashSet<uint64_t> data_table_id_set;
  table_schemas.reset();
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_INDEX_TYPE;
  spec.enum_values_ = &index_types;
  spec.caller_tag_ = "get_table_schemas_with_async_index_in_tenant";
  // User tables are not resident in ObSchemaMgr under OnDemand, so the mgr
  // derived set table_with_async_index_infos_ is empty. Enumerate async index
  // tables via idx_index_type, then map to unique data tables.
  if (OB_FAIL(ObSchemaScanOperator::fill_async_index_types(index_types))) {
    LOG_WARN("fail to fill async index types", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, index_ids))) {
    LOG_WARN("fail to get async index table ids", KR(ret), K(tenant_id));
  } else if (index_ids.empty()) {
    // no async index tables
  } else if (OB_FAIL(data_table_id_set.create(MAX(16, index_ids.count() * 2)))) {
    LOG_WARN("fail to create data table id set", KR(ret), K(tenant_id),
             "index_cnt", index_ids.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
      const ObSimpleTableSchemaV2 *index_schema = NULL;
      if (OB_FAIL(get_simple_table_schema(guard, tenant_id, index_ids.at(i), index_schema))) {
        LOG_WARN("fail to get async index simple schema", KR(ret), K(tenant_id),
                 "index_id", index_ids.at(i));
      } else if (OB_ISNULL(index_schema) || !index_schema->is_async_index()) {
        // dropped between scan and lookup, or unexpected non-async row
      } else {
        const uint64_t data_table_id = index_schema->get_data_table_id();
        int hash_ret = data_table_id_set.set_refactored(data_table_id);
        if (OB_HASH_EXIST == hash_ret) {
          // already collected this data table
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to add data table id", KR(ret), K(tenant_id), K(data_table_id));
        } else {
          const ObSimpleTableSchemaV2 *data_schema = NULL;
          if (OB_FAIL(get_simple_table_schema(guard, tenant_id, data_table_id, data_schema))) {
            LOG_WARN("fail to get data table simple schema", KR(ret), K(tenant_id),
                     K(data_table_id));
          } else if (OB_ISNULL(data_schema)) {
            // Purged tables are absent; recyclebin tables still need refresh.
          } else if (!data_schema->has_async_index()) {
            // flag not set on data table: skip to align with Original derived set
          } else if (OB_FAIL(table_schemas.push_back(data_schema))) {
            LOG_WARN("fail to push data table schema", KR(ret), K(tenant_id),
                     K(data_table_id));
          }
        }
      }
    }
  }
  return ret;
}

// -------------------- B-class full-scan wrappers --------------------

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t tablegroup_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (OB_FAIL(get_table_ids_in_tablegroup(guard, tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("fail to get table ids", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init simple table schema iterator",
             KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t tablegroup_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (OB_FAIL(get_table_ids_in_tablegroup(guard, tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("fail to get table ids", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_primary_table_schema_in_tablegroup(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t tablegroup_id,
    const ObSimpleTableSchemaV2 *&primary_table_schema) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  primary_table_schema = NULL;
  // Same id source as get_table_schemas_in_tablegroup; pick first matching Simple
  // (user / mysql tmp / sys), aligned with ObSchemaMgr::get_primary_table_schema_in_tablegroup.
  if (OB_FAIL(get_table_ids_in_tablegroup(guard, tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("fail to get table ids in tablegroup",
             KR(ret), K(tenant_id), K(tablegroup_id));
  } else {
    lib::ob_sort(table_ids.begin(), table_ids.end());
    for (int64_t i = 0; OB_SUCC(ret) && NULL == primary_table_schema && i < table_ids.count(); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = NULL;
      const uint64_t table_id = table_ids.at(i);
      if (OB_FAIL(guard.get_simple_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("fail to get simple table schema",
                 KR(ret), K(tenant_id), K(tablegroup_id), K(table_id));
      } else if (OB_NOT_NULL(table_schema)
                 && is_tablegroup_listed_table_type(table_schema->get_table_type())) {
        primary_table_schema = table_schema;
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tablespace(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t tablespace_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLESPACE;
  spec.filter_id_ = tablespace_id;
  spec.caller_tag_ = "get_table_schemas_in_tablespace";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get tablespace table ids",
             KR(ret), K(tenant_id), K(tablespace_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(tablespace_id));
  }
  return ret;
}

// -------------------- C-class full-scan wrappers --------------------

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.caller_tag_ = "get_table_schemas_in_tenant";
  if (OB_FAIL(iter.init(guard, spec))) {
    LOG_WARN("fail to init simple table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.caller_tag_ = "get_table_schemas_in_tenant";
  if (OB_FAIL(iter.init(guard, spec))) {
    LOG_WARN("fail to init table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_database(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t database_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_DATABASE;
  spec.filter_id_ = database_id;
  spec.caller_tag_ = "get_table_schemas_in_database";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get database table ids",
             KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init simple table schema iterator",
             KR(ret), K(tenant_id), K(database_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_database(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t database_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_DATABASE;
  spec.filter_id_ = database_id;
  spec.caller_tag_ = "get_table_schemas_in_database";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("fail to get database table ids",
             KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(iter.init(guard, table_ids))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(database_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_name_case_mode_for_resolve_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    ObNameCaseMode &case_mode) const
{
  int ret = OB_SUCCESS;
  case_mode = OB_NAME_CASE_INVALID;
  bool need_read_sys_variable = false;
  if (OB_FAIL(ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
          tenant_id, case_mode, need_read_sys_variable))) {
    LOG_WARN("failed to get tenant name case mode by compat mode", KR(ret), K(tenant_id));
  } else if (!need_read_sys_variable) {
    // The fixed rule has determined the mode for sys/meta or Oracle tenants.
  } else if (OB_FAIL(guard.get_tenant_name_case_mode(tenant_id, case_mode))) {
    if (OB_SCHEMA_EAGAIN == ret) {
      // Lazy guard has no mgr. Full sys_variable is loaded by get_schema() at snapshot.
      const ObSysVariableSchema *sys_variable = NULL;
      ret = OB_SUCCESS;
      if (OB_FAIL(guard.get_sys_variable_schema(tenant_id, sys_variable))) {
        LOG_WARN("failed to get sys variable schema for name case mode", KR(ret),
                 K(tenant_id));
      } else if (OB_ISNULL(sys_variable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys variable schema is null", KR(ret), K(tenant_id));
      } else {
        case_mode = sys_variable->get_name_case_mode();
      }
    } else {
      LOG_WARN("failed to get tenant name case mode", KR(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)
      && (case_mode <= OB_NAME_CASE_INVALID || case_mode >= OB_NAME_CASE_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid name case mode", KR(ret), K(tenant_id), K(case_mode));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index,
    ObSchemaGetterGuard::CheckTableType check_type, uint64_t &table_id,
    bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("failed to check tenant guard for table name", KR(ret), K(tenant_id));
  } else if (is_schema_resident_in_memory(tenant_id)) {
    // Prefer Original short-circuit for sys/meta tenants.
    ret = ObOriginalSchemaGetterGuard::instance().get_table_id(
        guard, tenant_id, database_id, table_name, is_index, check_type, table_id,
        is_built_in_index);
  } else if (OB_CTE_DATABASE_ID == database_id) {
    // Virtual CTE database is resolver-only; not persisted in schema history.
    // Align with Original SchemaMgr miss: leave table_id as OB_INVALID_ID.
  } else {
    const bool with_hidden =
        ObSchemaGetterGuard::USER_HIDDEN_TABLE_TYPE == check_type;
    const ObTableNameNamespace table_namespace = get_table_namespace_(
        is_index, with_hidden, is_built_in_index);
    bool is_oracle_index = false;
    bool is_sys_table = false;
    ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
    LOG_TRACE("[OnDemandNameResolve] get_table_id by name",
             K(tenant_id), K(database_id), K(table_name), K(is_index), K(check_type),
             K(is_built_in_index), K(table_namespace));
    if (OB_FAIL(get_name_case_mode_for_resolve_(guard, tenant_id, name_case_mode))) {
      LOG_WARN("failed to get name case mode for resolve", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObSysTableChecker::is_sys_table_name(
            tenant_id, database_id, table_name, name_case_mode, is_sys_table))) {
      LOG_WARN("failed to check sys table name", KR(ret), K(tenant_id), K(database_id),
               K(table_name), K(name_case_mode));
    } else if (is_sys_table) {
      // Case-aware inner-table predicate: "ALL_TABLES" short-circuits to Original,
      // while Oracle quoted "all_tables" (user table) continues to on-demand resolve.
      ret = ObOriginalSchemaGetterGuard::instance().get_table_id(
          guard, tenant_id, database_id, table_name, is_index, check_type, table_id,
          is_built_in_index);
    } else if (is_index && is_sys_database_id(database_id)
               && OB_FAIL(ObOriginalSchemaGetterGuard::instance().get_table_id(
                   guard, tenant_id, database_id, table_name, is_index, check_type, table_id,
                   is_built_in_index))) {
      // Inner indexes live in SchemaMgr. is_sys_table_name() only matches table
      // names, so SYS indexes must be short-circuited here. Miss falls through
      // to on-demand (user indexes created in SYS).
      LOG_WARN("failed to get sys-database index table id from original", KR(ret),
               K(tenant_id), K(database_id), K(table_name));
    } else if (OB_INVALID_ID != table_id) {
      // Original hit for a SYS inner index.
    } else if (is_index && !with_hidden
        && OB_FAIL(schema_resolve_util::check_is_oracle_index(
            tenant_id, database_id, table_namespace, is_oracle_index))) {
      LOG_WARN("failed to check oracle index", KR(ret),
               K(tenant_id), K(database_id), K(table_namespace));
    } else if (is_oracle_index) {
      if (OB_FAIL(get_table_id_by_oracle_index_name_(
              guard, tenant_id, database_id, table_name,
              is_built_in_index, check_type, table_id))) {
        LOG_WARN("failed to resolve oracle index table id", KR(ret), K(tenant_id),
                 K(database_id), K(table_name), K(is_built_in_index), K(check_type));
      }
    } else {
      ObSchemaResolveRange version_range;
      ObString resolve_name = table_name;
      uint64_t resolve_session_id =
          ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE == check_type
              ? 0 : guard.session_id_;
      if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_name(
              guard, tenant_id, table_namespace, database_id, resolve_name,
              resolve_session_id, name_case_mode, version_range))) {
        LOG_WARN("failed to resolve table name", KR(ret), K(tenant_id), K(database_id),
                 K(table_name), K(resolve_name), K(table_namespace));
      } else if (version_range.is_exist_) {
        // Name resolve already yields schema_id_. Only TEMP_TABLE_TYPE needs
        // simple schema to filter is_tmp_table(); other check_types must not
        // pin simple schema into guard.schema_objs_ for a pure id lookup.
        // Align with Original: skip tmp filter for inner tables.
        if (ObSchemaGetterGuard::TEMP_TABLE_TYPE != check_type
            || is_inner_table(version_range.schema_id_)) {
          table_id = version_range.schema_id_;
        } else {
          const ObSimpleTableSchemaV2 *schema = NULL;
          if (OB_FAIL(get_simple_table_schema(guard, tenant_id, version_range.schema_id_,
                                              schema))) {
            LOG_WARN("failed to load resolved table for temp filter", KR(ret),
                     K(version_range));
          } else if (OB_NOT_NULL(schema) && schema->is_tmp_table()) {
            table_id = version_range.schema_id_;
          }
        }
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_id_by_oracle_index_name_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &index_table_name,
    const bool is_built_in_index,
    ObSchemaGetterGuard::CheckTableType check_type,
    uint64_t &table_id) const
{
  int ret = OB_SUCCESS;
  ObString origin_index_name;
  bool should_resolve = false;
  ObSchemaResolveRange version_range;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  const ObTableNameNamespace table_namespace =
      get_table_namespace_(true /*is_index*/, false /*with_hidden*/, is_built_in_index);
  table_id = OB_INVALID_ID;
  // SchemaMgr index_name_map has no session_id. Oracle indexes are never
  // session-private, so resolve against session_id=0.
  if (OB_FAIL(schema_resolve_util::resolve_oracle_index_lookup_name(
          index_table_name, origin_index_name, should_resolve))) {
    LOG_WARN("failed to normalize oracle index lookup name", KR(ret), K(index_table_name));
  } else if (!should_resolve) {
    // Malformed encoded name: align with ObSchemaMgr miss semantics.
  } else if (OB_FAIL(get_name_case_mode_for_resolve_(guard, tenant_id, name_case_mode))) {
    LOG_WARN("failed to get name case mode for resolve", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_name(
                 guard, tenant_id, table_namespace, database_id, origin_index_name,
                 0 /*session_id*/, name_case_mode, version_range))) {
    LOG_WARN("failed to resolve oracle index table id", KR(ret),
             K(tenant_id), K(database_id), K(origin_index_name), K(table_namespace));
  } else if (version_range.is_exist_) {
    if (ObSchemaGetterGuard::TEMP_TABLE_TYPE != check_type
        || is_inner_table(version_range.schema_id_)) {
      table_id = version_range.schema_id_;
    } else {
      const ObSimpleTableSchemaV2 *schema = NULL;
      if (OB_FAIL(get_simple_table_schema(guard, tenant_id, version_range.schema_id_,
                                          schema))) {
        LOG_WARN("failed to load resolved table for temp filter", KR(ret),
                 K(version_range));
      } else if (OB_NOT_NULL(schema) && schema->is_tmp_table()) {
        table_id = version_range.schema_id_;
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_foreign_key_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, uint64_t &id) const
{
  int ret = OB_SUCCESS;
  ObSimpleForeignKeyInfo info;
  id = OB_INVALID_ID;
  if (OB_FAIL(get_foreign_key_info(guard, tenant_id, database_id, name, info))) {
    LOG_WARN("failed to get foreign key info", KR(ret), K(tenant_id), K(database_id), K(name));
  } else {
    id = info.foreign_key_id_;
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_foreign_key_info(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, ObSimpleForeignKeyInfo &info) const
{
  int ret = OB_SUCCESS;
  info.reset();
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(name));
  } else if (is_schema_resident_in_memory(tenant_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_foreign_key_info(
        guard, tenant_id, database_id, name, info);
  } else if (OB_FAIL(guard.schema_service_->get_simple_foreign_key_info_by_name(
                 guard, tenant_id, database_id, name, info))) {
    LOG_WARN("failed to get foreign key info by point lookup", KR(ret), K(tenant_id),
             K(database_id), K(name));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_constraint_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, uint64_t &id) const
{
  int ret = OB_SUCCESS;
  ObSimpleConstraintInfo info;
  id = OB_INVALID_ID;
  if (OB_FAIL(get_constraint_info(guard, tenant_id, database_id, name, info))) {
    LOG_WARN("failed to get constraint info", KR(ret), K(tenant_id), K(database_id), K(name));
  } else {
    id = info.constraint_id_;
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_constraint_info(
    const ObSchemaGetterGuard &const_guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, ObSimpleConstraintInfo &info) const
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &guard = const_cast<ObSchemaGetterGuard &>(const_guard);
  info.reset();
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(name));
  } else if (is_schema_resident_in_memory(tenant_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_constraint_info(
        guard, tenant_id, database_id, name, info);
  } else if (OB_FAIL(guard.schema_service_->get_simple_constraint_info_by_name(
                 guard, tenant_id, database_id, name, info))) {
    LOG_WARN("failed to get constraint info by point lookup", KR(ret), K(tenant_id),
             K(database_id), K(name));
  }
  return ret;
}
int ObOnDemandSchemaGetterGuard::get_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    const ObTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  // Align with Original: CTE / external are not in SchemaMgr -> schema stays NULL.
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (is_cte_table(table_id)
      || is_external_object_id(table_id)
      || is_schema_resident_in_memory(tenant_id, table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_table_schema(
        guard, tenant_id, table_id, schema);
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard",
             KR(ret), K(tenant_id), "guard_tenant_id", guard.get_tenant_id());
  } else {
    ObSchemaResolveRange version_range;
    ret = guard.get_from_local_cache(TABLE_SCHEMA, tenant_id, table_id, schema);
    if (OB_SUCC(ret)) {
      // Fast path: this Guard already pins the full schema at its snapshot.
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to read guard-local full table schema",
               KR(ret), K(tenant_id), K(table_id));
    } else {
      ret = OB_SUCCESS;
      if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_id(
              guard, tenant_id, table_id, version_range))) {
        LOG_WARN("failed to get schema version range",
                 KR(ret), K(tenant_id), K(table_id));
      } else if (!version_range.is_exist_) {
        // table not exist
      } else if (OB_FAIL(load_full_table_schema_(
                     guard, tenant_id, table_id,
                     version_range.precise_schema_version_, schema))) {
        LOG_WARN("failed to get full table schema",
                 KR(ret), K(tenant_id), K(table_id), K(version_range));
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index,
    const ObSimpleTableSchemaV2 *&schema, bool with_hidden_flag,
    bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("failed to check tenant guard for simple schema name", KR(ret), K(tenant_id));
  } else if (is_schema_resident_in_memory(tenant_id)) {
    // Prefer Original short-circuit for sys/meta tenants.
    ret = ObOriginalSchemaGetterGuard::instance().get_simple_table_schema(
        guard, tenant_id, database_id, table_name, is_index, schema,
        with_hidden_flag, is_built_in_index);
  } else if (OB_CTE_DATABASE_ID == database_id) {
    // Virtual CTE database is resolver-only; not persisted in schema history.
    // Align with Original SchemaMgr miss: leave schema as NULL so schema_checker
    // can overlay tmp_cte_schemas_ when cte_table_first is set.
  } else {
    const ObTableNameNamespace table_namespace =
        get_table_namespace_(is_index, with_hidden_flag, is_built_in_index);
    bool is_oracle_index = false;
    bool is_sys_table = false;
    ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
    ObSchemaResolveRange version_range;
    // Oracle index names are unique within a database, so the name-map key is
    // origin_index_name (same as ObSchemaMgr). Encoded __idx_<id>_<name> is
    // peeled before resolve. MySQL / recyclebin / hidden keep encoded names.
    LOG_TRACE("[OnDemandNameResolve] get_simple_table_schema by name",
             K(tenant_id), K(database_id), K(table_name), K(is_index),
             K(with_hidden_flag), K(is_built_in_index), K(table_namespace),
             "session_id", guard.session_id_);
    if (OB_FAIL(get_name_case_mode_for_resolve_(guard, tenant_id, name_case_mode))) {
      LOG_WARN("failed to get name case mode for resolve", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObSysTableChecker::is_sys_table_name(
            tenant_id, database_id, table_name, name_case_mode, is_sys_table))) {
      LOG_WARN("failed to check sys table name", KR(ret), K(tenant_id), K(database_id),
               K(table_name), K(name_case_mode));
    } else if (is_sys_table) {
      ret = ObOriginalSchemaGetterGuard::instance().get_simple_table_schema(
          guard, tenant_id, database_id, table_name, is_index, schema,
          with_hidden_flag, is_built_in_index);
    } else if (is_index && is_sys_database_id(database_id)
               && OB_FAIL(ObOriginalSchemaGetterGuard::instance().get_simple_table_schema(
                   guard, tenant_id, database_id, table_name, is_index, schema,
                   with_hidden_flag, is_built_in_index))) {
      LOG_WARN("failed to get sys-database index schema from original", KR(ret),
               K(tenant_id), K(database_id), K(table_name));
    } else if (OB_NOT_NULL(schema)) {
      // Original hit for a SYS inner index.
    } else if (OB_FAIL(schema_resolve_util::check_is_oracle_index(
            tenant_id, database_id, table_namespace, is_oracle_index))) {
      LOG_WARN("failed to check oracle index", KR(ret),
               K(tenant_id), K(database_id), K(table_namespace));
    } else if (is_oracle_index) {
      if (OB_FAIL(get_simple_oracle_index_schema_(
              guard, tenant_id, database_id, table_name,
              is_built_in_index, schema))) {
        LOG_WARN("failed to resolve oracle index schema", KR(ret), K(tenant_id),
                 K(database_id), K(table_name), K(is_built_in_index));
      }
    } else if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_name(
            guard, tenant_id, table_namespace, database_id, table_name,
            guard.session_id_, name_case_mode, version_range))) {
      LOG_WARN("failed to resolve simple schema name", KR(ret), K(tenant_id),
               K(database_id), K(table_name));
    } else if (version_range.is_exist_
               && OB_FAIL(get_simple_table_schema(guard, tenant_id, version_range.schema_id_,
                                                   schema))) {
      LOG_WARN("failed to load simple schema by name", KR(ret), K(version_range));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_oracle_index_schema_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &index_table_name,
    const bool is_built_in_index,
    const ObSimpleTableSchemaV2 *&index_schema) const
{
  int ret = OB_SUCCESS;
  ObString origin_index_name;
  bool should_resolve = false;
  index_schema = NULL;
  if (OB_FAIL(schema_resolve_util::resolve_oracle_index_lookup_name(
          index_table_name, origin_index_name, should_resolve))) {
    LOG_WARN("failed to normalize oracle index lookup name", KR(ret), K(index_table_name));
  } else if (!should_resolve) {
    // Malformed encoded name: align with ObSchemaMgr miss semantics.
  } else if (OB_FAIL(get_simple_index_schema_by_origin_name_(
                 guard, tenant_id, database_id, origin_index_name,
                 is_built_in_index, index_schema))) {
    LOG_WARN("failed to resolve oracle index by origin name", KR(ret),
             K(tenant_id), K(database_id), K(origin_index_name),
             K(is_built_in_index));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_index_schema_by_origin_name_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &index_name,
    const bool is_built_in_index,
    const ObSimpleTableSchemaV2 *&index_schema) const
{
  int ret = OB_SUCCESS;
  ObSchemaResolveRange version_range;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  const ObTableNameNamespace table_namespace =
      get_table_namespace_(true /*is_index*/, false /*with_hidden_flag*/, is_built_in_index);
  index_schema = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(index_name));
  } else if (OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(get_name_case_mode_for_resolve_(guard, tenant_id, name_case_mode))) {
    LOG_WARN("failed to get name case mode for origin index resolve",
             KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_name(
                 guard, tenant_id, table_namespace, database_id, index_name,
                 0 /*session_id*/, name_case_mode, version_range))) {
    LOG_WARN("failed to resolve oracle origin index name", KR(ret),
             K(tenant_id), K(database_id), K(index_name), K(table_namespace));
  } else if (version_range.is_exist_
             && OB_FAIL(get_simple_table_schema(
                 guard, tenant_id, version_range.schema_id_, index_schema))) {
    LOG_WARN("failed to load oracle index by origin name", KR(ret), K(version_range),
             K(index_name));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index, const ObTableSchema *&schema,
    bool with_hidden_flag, bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *simple_schema = NULL;
  schema = NULL;
  if (OB_FAIL(get_simple_table_schema(guard, tenant_id, database_id, table_name,
                                      is_index, simple_schema, with_hidden_flag,
                                      is_built_in_index))) {
    LOG_WARN("failed to resolve full table schema name", KR(ret), K(tenant_id),
             K(database_id), K(table_name));
  } else if (OB_NOT_NULL(simple_schema)
             && OB_FAIL(get_full_table_schema_(guard, *simple_schema, schema))) {
    LOG_WARN("failed to load full table schema by name", KR(ret), KPC(simple_schema));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_index_meta_infos_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<ObAuxTableMetaInfo> &index_meta_infos) const
{
  int ret = OB_SUCCESS;
  if (is_schema_resident_in_memory(tenant_id, data_table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_index_meta_infos_with_data_table_id(
        guard, tenant_id, data_table_id, index_meta_infos);
  } else {
    const ObTableSchema *data_full_table_schema = NULL;
    ObArray<ObAuxTableMetaInfo> simple_index_infos;
    index_meta_infos.reset();
    if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("inner stat error", KR(ret));
    } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_table_id));
    } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_table_schema(guard, tenant_id, data_table_id,
                                        data_full_table_schema))) {
      LOG_WARN("failed to get full data table schema", KR(ret), K(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(data_full_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
    } else if (!data_full_table_schema->is_table()
               && !data_full_table_schema->is_tmp_table()
               && !data_full_table_schema->is_aux_outtable_lob_control_table()) {
      // not a data table
    } else if (OB_FAIL(data_full_table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("failed to get simple index infos", KR(ret), K(tenant_id), K(data_table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObAuxTableMetaInfo &index_info = simple_index_infos.at(i);
      if (USER_INDEX != index_info.table_type_) {
        // skip non-index aux tables
      } else if (OB_FAIL(index_meta_infos.push_back(index_info))) {
        LOG_WARN("failed to push back index meta info", KR(ret), K(tenant_id), K(data_table_id),
                 K(index_info));
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_index_schemas_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  if (is_schema_resident_in_memory(tenant_id, data_table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_index_schemas_with_data_table_id(
        guard, tenant_id, data_table_id, schemas);
  } else {
    ObArray<ObAuxTableMetaInfo> index_meta_infos;
    if (OB_FAIL(get_index_meta_infos_with_data_table_id(
            guard, tenant_id, data_table_id, index_meta_infos))) {
      LOG_WARN("failed to get index meta infos", KR(ret), K(tenant_id), K(data_table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_meta_infos.count(); ++i) {
      const ObAuxTableMetaInfo &index_meta = index_meta_infos.at(i);
      const ObSimpleTableSchemaV2 *index_schema = NULL;
      if (OB_FAIL(get_simple_table_schema(guard, tenant_id, index_meta.table_id_,
                                          index_schema))) {
        LOG_WARN("failed to lookup index schema", KR(ret), K(tenant_id), K(data_table_id),
                 K(index_meta));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", KR(ret), K(tenant_id), K(data_table_id),
                 K(index_meta));
      } else if (OB_FAIL(schemas.push_back(index_schema))) {
        LOG_WARN("failed to push back index schema", KR(ret), K(tenant_id), K(data_table_id),
                 K(index_meta));
      }
    }
  }
  return ret;
}

// Append this owner's LOB schemas; the caller controls when to clear the array.
int ObOnDemandSchemaGetterGuard::get_lob_schemas_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &lob_schemas) const
{
  int ret = OB_SUCCESS;
  if (is_schema_resident_in_memory(tenant_id, data_table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_lob_schemas_with_data_table_id(
        guard, tenant_id, data_table_id, lob_schemas);
  } else if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else {
    // User-table LOBs are not resident in mgr; resolve them through the existing guard APIs.
    const ObTableSchema *owner_schema = nullptr;
    if (OB_FAIL(get_table_schema(guard, tenant_id, data_table_id, owner_schema))) {
      LOG_WARN("failed to get lob owner schema", KR(ret), K(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(owner_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("lob owner table not exist", KR(ret), K(tenant_id), K(data_table_id));
    } else {
      const uint64_t lob_ids[] = {owner_schema->get_aux_lob_meta_tid(), owner_schema->get_aux_lob_piece_tid()};
      for (int64_t i = 0; OB_SUCC(ret) && i < OB_AUX_LOB_TABLE_CNT; ++i) {
        const ObSimpleTableSchemaV2 *lob_schema = nullptr;
        if (OB_INVALID_ID == lob_ids[i]) {
          // This owner has no auxiliary table of this LOB type.
        } else if (OB_FAIL(get_simple_table_schema(guard, tenant_id, lob_ids[i], lob_schema))) {
          LOG_WARN("failed to get lob schema", KR(ret), K(tenant_id), K(data_table_id), K(lob_ids[i]));
        } else if (OB_ISNULL(lob_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob schema is null", KR(ret), K(tenant_id), K(data_table_id), K(lob_ids[i]));
        } else if (OB_FAIL(lob_schemas.push_back(lob_schema))) {
          LOG_WARN("failed to append lob schema", KR(ret), KPC(lob_schema));
        }
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, bool only_view,
    ObIArray<const ObTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_tenant(guard, tenant_id, table_ids))) {
    LOG_WARN("failed to scan tenant table ids", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObTableSchema *schema = NULL;
    if (OB_FAIL(get_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load tenant table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && (!only_view || schema->is_view_table())
               && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append tenant table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_tenant(guard, tenant_id, table_ids))) {
    LOG_WARN("failed to scan tenant simple table ids", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = NULL;
    if (OB_FAIL(get_simple_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load tenant simple table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append tenant simple table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_database(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    ObIArray<const ObTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_database(guard, tenant_id, database_id, table_ids))) {
    LOG_WARN("failed to scan database table ids", KR(ret), K(tenant_id), K(database_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObTableSchema *schema = NULL;
    if (OB_FAIL(get_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load database table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append database table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablegroup_id,
    ObIArray<const ObTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_tablegroup(guard, tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("failed to scan tablegroup table ids", KR(ret), K(tenant_id), K(tablegroup_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObTableSchema *schema = NULL;
    if (OB_FAIL(get_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load tablegroup table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append tablegroup table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schemas_in_tablespace(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablespace_id,
    ObIArray<const ObTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObSchemaScanSpec spec;
  schemas.reset();
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TABLESPACE;
  spec.filter_id_ = tablespace_id;
  spec.caller_tag_ = "get_table_schemas_in_tablespace_array";
  if (OB_FAIL(get_table_ids_by_inner_table(guard, spec, table_ids))) {
    LOG_WARN("failed to scan tablespace table ids", KR(ret), K(tenant_id), K(tablespace_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObTableSchema *schema = NULL;
    if (OB_FAIL(get_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load tablespace table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append tablespace table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_table_schemas_in_database(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_database(guard, tenant_id, database_id, table_ids))) {
    LOG_WARN("failed to scan database simple table ids", KR(ret), K(tenant_id), K(database_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = NULL;
    if (OB_FAIL(get_simple_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load database simple table schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append database simple table schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablegroup_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  schemas.reset();
  if (OB_FAIL(get_table_ids_in_tablegroup(guard, tenant_id, tablegroup_id, table_ids))) {
    LOG_WARN("failed to scan tablegroup simple ids", KR(ret), K(tenant_id), K(tablegroup_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = NULL;
    if (OB_FAIL(get_simple_table_schema(guard, tenant_id, table_ids.at(i), schema))) {
      LOG_WARN("failed to load tablegroup simple schema", KR(ret), K(i));
    } else if (OB_NOT_NULL(schema) && OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to append tablegroup simple schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}
int ObOnDemandSchemaGetterGuard::check_table_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index,
    ObSchemaGetterGuard::CheckTableType check_type, bool &is_exist,
    uint64_t *table_id) const
{
  int ret = OB_SUCCESS;
  if (is_schema_resident_in_memory(tenant_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().check_table_exist(
        guard, tenant_id, database_id, table_name,
        is_index, check_type, is_exist, table_id);
  } else {
    uint64_t tmp_table_id = OB_INVALID_ID;
    is_exist = false;
    if (OB_NOT_NULL(table_id)) {
      *table_id = OB_INVALID_ID;
    }
    if (OB_FAIL(get_table_id(guard, tenant_id, database_id, table_name, is_index,
                             check_type, tmp_table_id, false))) {
      LOG_WARN("get table id failed", KR(ret), K(tenant_id), K(database_id), K(table_name));
    } else if (OB_INVALID_ID != tmp_table_id) {
      is_exist = true;
      if (OB_NOT_NULL(table_id)) {
        *table_id = tmp_table_id;
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::check_table_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  // Align with Original: CTE -> not exist; external -> exist via PROCESSLIST version.
  if (is_cte_table(table_id)
      || is_external_object_id(table_id)
      || is_schema_resident_in_memory(tenant_id, table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().check_table_exist(
        guard, tenant_id, table_id, is_exist);
  } else {
    ObSchemaResolveRange version_range;
    is_exist = false;
    if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
      ret = OB_INNER_STAT_ERROR;
    } else if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_id(
                   guard, tenant_id, table_id, version_range))) {
      LOG_WARN("failed to get schema version range", KR(ret), K(tenant_id), K(table_id));
    } else {
      is_exist = version_range.is_exist_;
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_table_schema_version(
    ObSchemaGetterGuard &guard, ObSchemaType schema_type, uint64_t tenant_id,
    uint64_t table_id, int64_t &schema_version, uint64_t *database_id) const
{
  int ret = OB_SUCCESS;
  // CTE / external object are not persisted in SchemaMgr; reuse Original's handling
  // (CTE -> INVALID_VERSION, external -> borrow PROCESSLIST version).
  if (is_cte_table(table_id)
      || is_external_object_id(table_id)
      || is_schema_resident_in_memory(tenant_id, table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_table_schema_version(
        guard, schema_type, tenant_id, table_id, schema_version, database_id);
  } else {
    ObSchemaResolveRange version_range;
    const ObSimpleTableSchemaV2 *simple_table = NULL;
    schema_version = OB_INVALID_VERSION;
    if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
      ret = OB_INNER_STAT_ERROR;
    } else if (OB_NOT_NULL(database_id)) {
      *database_id = OB_INVALID_ID;
      if (OB_FAIL(guard.schema_service_->get_simple_table_schema_on_demand(
              guard, tenant_id, table_id, simple_table))) {
        LOG_WARN("failed to get simple table schema on demand", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_NOT_NULL(simple_table)) {
        schema_version = simple_table->get_schema_version();
        *database_id = simple_table->get_database_id();
      }
    } else if (OB_FAIL(guard.schema_service_->get_schema_version_range_by_id(
                   guard, tenant_id, table_id, version_range))) {
      LOG_WARN("failed to get schema version range", KR(ret), K(tenant_id), K(table_id));
    } else if (version_range.is_exist_) {
      schema_version = version_range.precise_schema_version_;
    }
    UNUSED(schema_type);
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_simple_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    const ObSimpleTableSchemaV2 *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!guard.check_inner_stat() || OB_ISNULL(guard.schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_cte_table(table_id) || is_external_object_id(table_id)) {
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("failed to check tenant guard for simple schema id", KR(ret), K(tenant_id));
  } else if (is_schema_resident_in_memory(tenant_id, table_id)) {
    ret = ObOriginalSchemaGetterGuard::instance().get_simple_table_schema(
        guard, tenant_id, table_id, schema);
  } else if (OB_FAIL(guard.schema_service_->get_simple_table_schema_on_demand(
                 guard, tenant_id, table_id, schema))) {
    LOG_WARN("failed to get simple schema by coordinator", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}
int ObOnDemandSchemaGetterGuard::get_schema_count(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_count = 0;
  // Diagnose / virtual-table path: report schemas resident in ObSchemaMgr cache.
  // User tables are intentionally not resident in ON_DEMAND mode, so this count
  // is typically smaller than ORIGINAL for the same tenant workload.
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_count(schema_count))) {
    LOG_WARN("fail to get schema count from resident mgr", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_schema_size(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    int64_t &schema_size) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_size = 0;
  // Same semantics as get_schema_count: memory size of schemas resident in cache.
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_size(schema_size))) {
    LOG_WARN("fail to get schema size from resident mgr", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::scan_tenant_indexes_by_status_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const ObIndexStatus index_status,
    const bool early_stop,
    bool &exist,
    ObIArray<uint64_t> *index_ids) const
{
  int ret = OB_SUCCESS;
  ObSchemaLiteScanSpec lite_spec;
  ObSchemaLiteTableIterator lite_iter;
  const ObSchemaLiteTableResult *row = NULL;
  exist = false;
  if (OB_NOT_NULL(index_ids)) {
    index_ids->reset();
  }
  lite_spec.table_spec_.tenant_id_ = tenant_id;
  lite_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  lite_spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS;
  lite_spec.caller_tag_ = "scan_index_by_status";
  if (OB_FAIL(guard.scan_lite_tables(lite_spec, lite_iter))) {
    LOG_WARN("fail to scan lite tables in tenant", KR(ret), K(tenant_id));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(lite_iter.get_next(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next lite table", KR(ret), K(tenant_id));
      }
      break;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, lite row is NULL", KR(ret), K(tenant_id));
    } else if (row->is_index_table() && index_status == row->index_status_) {
      exist = true;
      if (OB_NOT_NULL(index_ids)) {
        if (OB_FAIL(index_ids->push_back(row->table_id_))) {
          LOG_WARN("fail to push back index id", KR(ret), "table_id", row->table_id_);
        }
      }
      if (OB_SUCC(ret) && early_stop) {
        break;
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::get_tenant_unavailable_index(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_FAIL(scan_tenant_indexes_by_status_(
          guard, tenant_id, INDEX_STATUS_UNAVAILABLE,
          false /*early_stop*/, exist, &index_ids))) {
    LOG_WARN("fail to get tenant unavailable index", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::check_unavailable_index_exist(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    bool &exist) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_tenant_indexes_by_status_(
          guard, tenant_id, INDEX_STATUS_UNAVAILABLE,
          true /*early_stop*/, exist, NULL))) {
    LOG_WARN("fail to check unavailable index exist", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::check_restore_error_index_exist(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    bool &exist) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_tenant_indexes_by_status_(
          guard, tenant_id, INDEX_STATUS_RESTORE_INDEX_ERROR,
          true /*early_stop*/, exist, NULL))) {
    LOG_WARN("fail to check restore error index exist", KR(ret), K(tenant_id));
  }
  return ret;
}

// only use in oracle mode.
// Callers (DROP INDEX / object-name resolve / unique constraint) already gate on
// lib::is_oracle_mode(); sys/meta tenants are MySQL and never enter here.
// SYS-database inner indexes still live in SchemaMgr, so try Original first.
int ObOnDemandSchemaGetterGuard::get_idx_schema_by_origin_idx_name(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &index_name, const ObTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *matched = NULL;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  schema = NULL;
  if (index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id));
  } else if (lib::Worker::CompatMode::ORACLE != compat_mode
             || is_mysql_sys_database_id(database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compat_mode is not oracle mode",
             KR(ret), K(tenant_id), K(database_id), K(compat_mode));
  } else if (is_sys_database_id(database_id)
             && OB_FAIL(ObOriginalSchemaGetterGuard::instance().get_idx_schema_by_origin_idx_name(
                 guard, tenant_id, database_id, index_name, schema))) {
    LOG_WARN("failed to get sys-database origin index from original", KR(ret),
             K(tenant_id), K(database_id), K(index_name));
  } else if (OB_NOT_NULL(schema)) {
    // Inner indexes reside in SchemaMgr.
  } else if (OB_FAIL(get_simple_index_schema_by_origin_name_(
                 guard, tenant_id, database_id, index_name,
                 false /*is_built_in_index*/, matched))) {
    LOG_WARN("failed to get simple index schema by origin name", KR(ret),
             K(tenant_id), K(database_id), K(index_name));
  } else if (OB_NOT_NULL(matched)
             && OB_FAIL(get_full_table_schema_(guard, *matched, schema))) {
    LOG_WARN("failed to load matched index schema", KR(ret), KPC(matched));
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::generate_tablet_table_map(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    hash::ObHashMap<ObTabletID, uint64_t> &tablet_map) const
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;
  int64_t schema_version = OB_INVALID_VERSION;
  const int64_t BUCKET_NUM = 10007;
  ObSimpleTableSchemaIterator table_iter;
  const ObSimpleTableSchemaV2 *table = NULL;
  if (OB_UNLIKELY(guard.get_tenant_id() != tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant_id is not match with schema_guard",
             KR(ret), K(guard.get_tenant_id()), K(tenant_id));
  } else if (OB_FAIL(guard.is_lazy_mode(tenant_id, is_lazy))) {
    LOG_WARN("fail to check lazy mode", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(is_lazy)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't generate tablet-table pairs with lazy guard",
             KR(ret), K(guard.get_tenant_id()), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!ObSchemaService::is_formal_version(schema_version))) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema version is not formal, retry to get schema guard",
             KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(tablet_map.create(BUCKET_NUM, "TabletPairMap", "TabletPairMap", tenant_id))) {
    LOG_WARN("create tablet-table map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_schemas_in_tenant(guard, tenant_id, table_iter))) {
    LOG_WARN("fail to get tables", KR(ret), K(tenant_id));
  }
  while (OB_SUCC(ret)) {
    ObTabletID tablet_id;
    if (OB_FAIL(table_iter.get_next(table))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next table schema", KR(ret), K(tenant_id));
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ptr is null", KR(ret), K(tenant_id));
    } else if (table->has_tablet()) {
      const uint64_t table_id = table->get_table_id();
      ObPartitionSchemaIter iter(*table, ObCheckPartitionMode::CHECK_PARTITION_MODE_ALL);
      while (OB_SUCC(ret) && OB_SUCC(iter.next_tablet_id(tablet_id))) {
        if (OB_FAIL(tablet_map.set_refactored(tablet_id, table_id))) {
          LOG_WARN("fail to set tablet-table map",
                   KR(ret), K(tenant_id), K(tablet_id), K(table_id));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("iter tablet failed", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObOnDemandSchemaGetterGuard::deep_copy_index_name_map(
    ObSchemaGetterGuard &guard,
    ObIAllocator &allocator,
    ObIndexNameMap &index_name_cache) const
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  const uint64_t tenant_id = guard.get_tenant_id();
  bool is_oracle_mode = false;
  ObSqlString sql;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_can_full_scan(tenant_id))) {
    LOG_WARN("full scan not allowed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy = guard.schema_service_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                 tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id));
  } else if (FALSE_IT(index_name_cache.set_sub_map_mem_size(1024))) {
    // restore ObIndexNameMap default_size (1024) before init(), otherwise alloc(0) -> -4013.
  } else if (OB_FAIL(index_name_cache.init())) {
    LOG_WARN("init index name cache failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT table_id, table_name, database_id, data_table_id, index_type, "
                 "table_type, table_mode FROM %s WHERE tenant_id = 0 AND table_type = %d",
                 OB_ALL_TABLE_TNAME, USER_INDEX))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get next", KR(ret), K(tenant_id));
            }
            break;
          }
          uint64_t table_id = OB_INVALID_ID;
          uint64_t database_id = OB_INVALID_ID;
          uint64_t data_table_id = OB_INVALID_ID;
          ObIndexType index_type = INDEX_TYPE_IS_NOT;
          ObTableType table_type = MAX_TABLE_TYPE;
          ObTableMode table_mode;
          ObString table_name;
          ObIndexNameInfo *index_name_info = NULL;
          void *buf = NULL;
          EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "database_id", database_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_table_id", data_table_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "index_type", index_type, ObIndexType);
          EXTRACT_INT_FIELD_MYSQL(*result, "table_type", table_type, ObTableType);
          EXTRACT_INT_FIELD_MYSQL(*result, "table_mode", table_mode.mode_, uint32_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", table_name);
          if (OB_FAIL(ret)) {
          } else if (!is_index_table(table_type)
                     || table_mode.is_user_hidden_table()
                     || is_built_in_index(index_type)) {
            // Align normal_index_name_map_ membership.
          } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIndexNameInfo)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc index name info", KR(ret), K(tenant_id), K(table_id));
          } else if (FALSE_IT(index_name_info = new (buf) ObIndexNameInfo())) {
          } else if (OB_FAIL(index_name_info->init(
                         allocator, tenant_id, database_id, data_table_id, table_id, table_name))) {
            LOG_WARN("fail to init index name info",
                     KR(ret), K(tenant_id), K(table_id), K(table_name));
          } else {
            uint64_t key_data_table_id = OB_INVALID_ID;
            ObString key_name;
            if (is_recyclebin_database_id(database_id)) {
              key_data_table_id = OB_INVALID_ID;
              key_name = index_name_info->get_index_name();
            } else {
              key_data_table_id = (is_oracle_mode && !is_mysql_sys_database_id(database_id))
                                  ? OB_INVALID_ID
                                  : index_name_info->get_data_table_id();
              key_name = index_name_info->get_original_index_name();
            }
            const int overwrite = 0;
            ObIndexSchemaHashWrapper index_name_wrapper(
                index_name_info->get_tenant_id(), database_id, key_data_table_id, key_name);
            if (OB_FAIL(index_name_cache.set_refactored(
                    index_name_wrapper, index_name_info, overwrite))) {
              LOG_WARN("fail to set refactored", KR(ret), KPC(index_name_info));
              if (OB_HASH_EXIST == ret) {
                ObIndexNameInfo **exist_index_info = index_name_cache.get(index_name_wrapper);
                if (OB_NOT_NULL(exist_index_info) && OB_NOT_NULL(*exist_index_info)) {
                  FLOG_ERROR("duplicated index info exist", KR(ret),
                             KPC(index_name_info), KPC(*exist_index_info));
                }
              }
            }
          }
        }
      }
    }
  }
  // __all_table omits is_core_table indexes; union matching non-built-in core indexes from mgr.
  if (OB_SUCC(ret)) {
    ObArray<const ObSimpleTableSchemaV2 *> schemas;
    if (OB_FAIL(ObOriginalSchemaGetterGuard::instance().get_simple_table_schemas_in_tenant(
            guard, tenant_id, schemas))) {
      // User indexes already loaded from __all_table; soft-skip core append when mgr unavailable
      // (align with get_table_ids_in_tenant_for_cdc core soft-skip).
      if (OB_SCHEMA_EAGAIN == ret || OB_TENANT_NOT_EXIST == ret) {
        LOG_WARN("skip appending core index names without mgr", KR(ret), K(tenant_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get mgr schemas for core index append", KR(ret), K(tenant_id));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *schema = schemas.at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", KR(ret), K(i));
      } else if (!is_core_table(schema->get_table_id())
                 || !schema->is_index_table()
                 || schema->is_user_hidden_table()
                 || is_built_in_index(schema->get_index_type())) {
        // skip
      } else {
        ObIndexNameInfo *index_name_info = NULL;
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIndexNameInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc index name info", KR(ret), K(tenant_id), KPC(schema));
        } else if (FALSE_IT(index_name_info = new (buf) ObIndexNameInfo())) {
        } else if (OB_FAIL(index_name_info->init(
                       allocator,
                       tenant_id,
                       schema->get_database_id(),
                       schema->get_data_table_id(),
                       schema->get_table_id(),
                       schema->get_table_name_str()))) {
          LOG_WARN("fail to init index name info from core schema", KR(ret), KPC(schema));
        } else {
          uint64_t key_data_table_id = OB_INVALID_ID;
          ObString key_name;
          const uint64_t database_id = index_name_info->get_database_id();
          if (is_recyclebin_database_id(database_id)) {
            key_data_table_id = OB_INVALID_ID;
            key_name = index_name_info->get_index_name();
          } else {
            key_data_table_id = (is_oracle_mode && !is_mysql_sys_database_id(database_id))
                                ? OB_INVALID_ID
                                : index_name_info->get_data_table_id();
            key_name = index_name_info->get_original_index_name();
          }
          const int overwrite = 0;
          ObIndexSchemaHashWrapper index_name_wrapper(
              index_name_info->get_tenant_id(), database_id, key_data_table_id, key_name);
          if (OB_FAIL(index_name_cache.set_refactored(
                  index_name_wrapper, index_name_info, overwrite))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to set core index name", KR(ret), KPC(index_name_info));
            }
          }
        }
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

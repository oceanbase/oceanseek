/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_simple_schema_on_demand_coordinator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_id_resolver.h"
#include "share/schema/ob_schema_name_resolver.h"
#include "share/schema/ob_simple_schema_lookup_service.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_server_schema_service.h"
#include "lib/charset/ob_charset.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/utility.h"
#include "share/ob_server_struct.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ERRSIM_POINT_DEF(ERRSIM_ON_DEMAND_RESOLVE_BACKSOURCE_FAIL);

static int inject_resolve_before_publish_for_test()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_ON_DEMAND_RESOLVE_BACKSOURCE_FAIL)) {
    ret = ERRSIM_ON_DEMAND_RESOLVE_BACKSOURCE_FAIL;
    LOG_WARN("[ERRSIM] inject on-demand resolve backsource fail", KR(ret));
  } else {
    DEBUG_SYNC(AFTER_ON_DEMAND_RESOLVE_BEFORE_PUBLISH);
  }
  return ret;
}

template <typename Key>
static int get_version_range_from_cache_(
    ObSchemaResolveCache &cache,
    const Key &key,
    const int64_t snapshot_version,
    const int64_t abs_deadline_us,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &publish_snapshot,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSchemaResolveResult result;
  version_range.reset();
  if (ObTimeUtility::current_time() >= abs_deadline_us) {
    ret = OB_TIMEOUT;
  } else if (OB_SUCC(ret = cache.get_if_current(
                 key, snapshot_version, state, publish_snapshot, result))) {
    version_range = ObSchemaResolveRange(
        result.range_start_, result.range_end_, result.schema_id_,
        result.precise_schema_version_, 0, result.table_namespace_, result.is_exist_);
  }
  return ret;
}

ObSimpleSchemaOnDemandCoordinator::ObSimpleSchemaOnDemandCoordinator()
  : schema_service_(NULL),
    is_inited_(false)
{
}

int ObSimpleSchemaOnDemandCoordinator::init(ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    schema_service_ = schema_service;
    is_inited_ = true;
  }
  return ret;
}

bool ObSimpleSchemaOnDemandCoordinator::check_inner_stat() const
{
  return is_inited_ && OB_NOT_NULL(schema_service_) && schema_service_->check_inner_stat();
}

int64_t ObSimpleSchemaOnDemandCoordinator::get_load_deadline_()
{
  const bool has_worker_timeout = THIS_WORKER.is_timeout_ts_valid();
  return ObOnDemandSchemaRetryPolicy::get_load_deadline(
      has_worker_timeout,
      has_worker_timeout ? THIS_WORKER.get_timeout_ts() : 0,
      ObTimeUtility::current_time());
}

int64_t ObSimpleSchemaOnDemandCoordinator::get_wait_deadline_(
    const int64_t load_deadline_us)
{
  return MIN(load_deadline_us,
             ObTimeUtility::current_time() + LOAD_WAIT_TIMEOUT_US);
}

int ObSimpleSchemaOnDemandCoordinator::do_backoff_sleep_(
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

int ObSimpleSchemaOnDemandCoordinator::get_publish_snapshot_(
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
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret), K(tenant_id), K(snapshot_version));
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

int ObSimpleSchemaOnDemandCoordinator::get_local_simple_schema_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t precise_schema_version,
    const ObSimpleTableSchemaV2 *&schema) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  schema = NULL;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < guard.schema_objs_.count(); ++i) {
    const SchemaObj &schema_obj = guard.schema_objs_.at(i);
    if (TABLE_SIMPLE_SCHEMA == schema_obj.schema_type_
        && tenant_id == schema_obj.tenant_id_
        && table_id == schema_obj.schema_id_) {
      if (OB_ISNULL(schema_obj.schema_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pinned simple schema object is null", KR(ret), K(tenant_id),
                 K(table_id), K(i), K(schema_obj));
      } else {
        const ObSimpleTableSchemaV2 *candidate =
            static_cast<const ObSimpleTableSchemaV2 *>(schema_obj.schema_);
        if (precise_schema_version == candidate->get_schema_version()) {
          schema = candidate;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::pin_simple_schema_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 *schema,
    ObKVCacheHandle &handle) const
{
  int ret = OB_SUCCESS;
  SchemaObj schema_obj;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is null when pinning simple schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(guard.schema_objs_.push_back(schema_obj))) {
    LOG_WARN("failed to pin simple schema in guard", KR(ret), K(tenant_id), K(table_id));
  } else {
    SchemaObj &stored = guard.schema_objs_.at(guard.schema_objs_.count() - 1);
    stored.schema_type_ = TABLE_SIMPLE_SCHEMA;
    stored.tenant_id_ = tenant_id;
    stored.schema_id_ = table_id;
    stored.schema_ = const_cast<ObSchema *>(static_cast<const ObSchema *>(schema));
    stored.handle_.move_from(handle);
    if (stored.handle_.is_valid()
        && guard.pin_cache_size_ < ObSchemaGetterGuard::FULL_SCHEMA_MEM_THREHOLD) {
      guard.pin_cache_size_ += schema->get_convert_size();
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::rollback_schema_pins_(
    ObSchemaGetterGuard &guard,
    const int64_t schema_obj_count,
    const int64_t pin_cache_size) const
{
  int ret = OB_SUCCESS;
  if (schema_obj_count < 0
      || schema_obj_count > guard.schema_objs_.count()
      || pin_cache_size < 0
      || pin_cache_size > guard.pin_cache_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid guard pin rollback boundary", KR(ret), K(schema_obj_count),
              "current_schema_obj_count", guard.schema_objs_.count(),
              K(pin_cache_size), "current_pin_cache_size", guard.pin_cache_size_);
  } else {
    while (guard.schema_objs_.count() > schema_obj_count) {
      guard.schema_objs_.pop_back();
    }
    guard.pin_cache_size_ = pin_cache_size;
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_version_range_by_id_once_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t snapshot_version,
    const int64_t abs_deadline_us,
    ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &publish_snapshot,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  version_range.reset();
  const ObSchemaIDResolveKey key(
      tenant_id, publish_snapshot.cache_generations_.id_resolve_, table_id);
  if (OB_FAIL(get_version_range_from_cache_(
          schema_service_->get_schema_resolve_cache(), key, snapshot_version,
          abs_deadline_us, state, publish_snapshot, version_range))
      && OB_ENTRY_NOT_EXIST != ret) {
    if (OB_SCHEMA_EAGAIN != ret) {
      LOG_WARN("failed to read id resolve cache", KR(ret), K(key), K(snapshot_version));
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObSchemaLoadGuard load_guard;
    const ObSchemaLoadKey load_key(LOAD_ID_RESOLVE, tenant_id,
                                   publish_snapshot.cache_generations_.id_resolve_, key.hash());
    bool is_leader = false;
    uint64_t flight_id = 0;
    if (ObTimeUtility::current_time() >= abs_deadline_us) {
      ret = OB_TIMEOUT;
      LOG_WARN("get_version_range_by_id_once_ timeout",
               KR(ret), K(key), K(snapshot_version), K(abs_deadline_us));
    } else if (OB_FAIL(load_guard.acquire(
            schema_service_->get_simple_schema_load_coordinator(),
            load_key, is_leader, flight_id))) {
      LOG_WARN("failed to coordinate id resolve", KR(ret), K(load_key));
    } else if (!is_leader) {
      DEBUG_SYNC(BEFORE_ON_DEMAND_RESOLVE_FOLLOWER_WAIT);
      if (OB_FAIL(schema_service_->get_simple_schema_load_coordinator().wait(
              load_key, flight_id, get_wait_deadline_(abs_deadline_us)))) {
        LOG_WARN("failed waiting for id resolve leader", KR(ret), K(load_key));
      } else {
        // A completed flight may have published a usable positive or negative
        // result. Only retry with backoff if no result covers this snapshot.
        ret = get_version_range_from_cache_(
            schema_service_->get_schema_resolve_cache(), key, snapshot_version,
            abs_deadline_us, state, publish_snapshot, version_range);
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SCHEMA_EAGAIN;
        }
      }
    } else if (OB_FAIL(guard.get_schema_status(tenant_id, schema_status))) {
      LOG_WARN("failed to get schema status", KR(ret), K(tenant_id), K(table_id));
    } else {
      ObSchemaResolveRange range;
      if (OB_FAIL(ObSchemaIDResolver::get_schema_version_range_by_id(
          *schema_service_->get_sql_proxy(), schema_status, schema_service_, tenant_id, table_id,
          snapshot_version, range))) {
        LOG_WARN("failed to load id resolve range", KR(ret), K(tenant_id),
                 K(table_id), K(snapshot_version));
      } else if (ObTimeUtility::current_time() >= abs_deadline_us) {
        ret = OB_TIMEOUT;
        LOG_WARN("get_version_range_by_id_once_ timeout",
                 KR(ret), K(key), K(snapshot_version), K(abs_deadline_us));
      } else if (OB_FAIL(inject_resolve_before_publish_for_test())) {
        LOG_WARN("on-demand id resolve injected before publish", KR(ret),
                 K(tenant_id), K(table_id));
      } else {
        const int publish_ret = schema_service_->get_schema_resolve_cache().
            publish_if_current(key, range, state, publish_snapshot);
        if (OB_SCHEMA_EAGAIN == publish_ret) {
          ret = OB_SCHEMA_EAGAIN;
        } else if (OB_SUCCESS != publish_ret && !state.can_publish(publish_snapshot)) {
          ret = OB_SCHEMA_EAGAIN;
        } else {
          if (OB_SUCCESS != publish_ret) {
            LOG_WARN("failed to publish id resolve range, return loaded scalar",
                     KR(publish_ret), K(key), K(range));
          }
          version_range = range;
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_schema_version_range_by_id(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t retry_count = 0;
  int64_t snapshot_version = OB_INVALID_VERSION;
  const int64_t abs_deadline_us = get_load_deadline_();
  version_range.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
    LOG_WARN("failed to get snapshot version", KR(ret), K(tenant_id), K(table_id));
  }
  while (OB_SUCC(ret) && !done) {
    ObTenantSchemaPublishState *state = NULL;
    ObSchemaPublishSnapshot publish_snapshot;
    if (OB_FAIL(get_publish_snapshot_(guard, tenant_id, snapshot_version,
                                      abs_deadline_us, state, publish_snapshot))) {
      LOG_WARN("failed to get publication token", KR(ret), K(tenant_id), K(table_id));
    } else {
      ret = get_version_range_by_id_once_(guard, tenant_id, table_id, snapshot_version,
                             abs_deadline_us, *state, publish_snapshot, version_range);
      if (OB_SUCCESS == ret) {
        done = true;
      } else if (OB_SCHEMA_EAGAIN == ret) {
        version_range.reset();
        ret = do_backoff_sleep_(abs_deadline_us, retry_count);
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_version_range_by_name_with_session_id_(
    ObSchemaGetterGuard &guard,
    const ObSchemaNameResolveKey &key,
    const ObString &table_name,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    const int64_t abs_deadline_us,
    ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &publish_snapshot,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  version_range.reset();
  if (OB_FAIL(get_version_range_from_cache_(
          schema_service_->get_schema_resolve_cache(), key, snapshot_version,
          abs_deadline_us, state, publish_snapshot, version_range))
      && OB_ENTRY_NOT_EXIST != ret) {
    if (OB_SCHEMA_EAGAIN != ret) {
      LOG_WARN("failed to read name resolve cache", KR(ret), K(key), K(snapshot_version));
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObSchemaLoadGuard load_guard;
    const ObSchemaLoadKey load_key(LOAD_NAME_RESOLVE, key.tenant_id_,
                                   key.cache_generation_, key.hash());
    bool is_leader = false;
    uint64_t flight_id = 0;
    if (ObTimeUtility::current_time() >= abs_deadline_us) {
      ret = OB_TIMEOUT;
    } else if (OB_FAIL(load_guard.acquire(
            schema_service_->get_simple_schema_load_coordinator(),
            load_key, is_leader, flight_id))) {
      LOG_WARN("failed to coordinate name resolve", KR(ret), K(load_key));
    } else if (!is_leader) {
      DEBUG_SYNC(BEFORE_ON_DEMAND_RESOLVE_FOLLOWER_WAIT);
      if (OB_FAIL(schema_service_->get_simple_schema_load_coordinator().wait(
              load_key, flight_id, get_wait_deadline_(abs_deadline_us)))) {
        LOG_WARN("failed waiting for name resolve leader", KR(ret), K(load_key));
      } else {
        // Keep the caller's token so session-private/global probes cannot
        // combine results from different metadata publications.
        ret = get_version_range_from_cache_(
            schema_service_->get_schema_resolve_cache(), key, snapshot_version,
            abs_deadline_us, state, publish_snapshot, version_range);
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SCHEMA_EAGAIN;
        }
      }
    } else if (OB_FAIL(guard.get_schema_status(key.tenant_id_, schema_status))) {
      LOG_WARN("failed to get schema status", KR(ret), K(key));
    } else {
      ObSchemaResolveRange range;
      if (OB_FAIL(ObSchemaNameResolver::get_schema_version_range_by_name_for_session(
          *schema_service_->get_sql_proxy(), schema_status, key, table_name,
          snapshot_version, name_case_mode, range))) {
        LOG_WARN("failed to resolve name for session", KR(ret), K(key),
                 K(snapshot_version));
      } else {
        // Name miss leaves an empty range; synthesize a point not-exist so L1 can
        // cache "this snapshot has no binding" without claiming a wide interval.
        if (!range.is_valid()) {
          range = ObSchemaResolveRange(snapshot_version, snapshot_version, OB_INVALID_ID,
                                       OB_INVALID_VERSION, 0, key.table_namespace_, false);
        } else if (NS_MAX == range.table_namespace_) {
          range.table_namespace_ = key.table_namespace_;
        }
        // Session-private name probes miss for ordinary tables. Do not consume
        // DEBUG_SYNC/ERRSIM for such misses, but publish their negative result so
        // cache readers and flight waiters can reuse it.
        const bool skip_inject_for_session_miss =
            0 != key.exact_session_id_ && !range.is_exist_;
        if (!range.is_valid() || !range.contains(snapshot_version)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid authoritative name resolve range", KR(ret), K(key),
                    K(range), K(snapshot_version));
        } else if (ObTimeUtility::current_time() >= abs_deadline_us) {
          ret = OB_TIMEOUT;
        } else if (!skip_inject_for_session_miss
                   && OB_FAIL(inject_resolve_before_publish_for_test())) {
          LOG_WARN("on-demand name resolve injected before publish", KR(ret), K(key));
        } else {
          const int publish_ret = schema_service_->get_schema_resolve_cache().
              publish_if_current(key, range, state, publish_snapshot);
          if (OB_SCHEMA_EAGAIN == publish_ret) {
            ret = OB_SCHEMA_EAGAIN;
          } else if (OB_SUCCESS != publish_ret && !state.can_publish(publish_snapshot)) {
            ret = OB_SCHEMA_EAGAIN;
          } else {
            if (OB_SUCCESS != publish_ret) {
              LOG_WARN("failed to publish name resolve range, return loaded scalar",
                       KR(publish_ret), K(key), K(range));
            }
            version_range = range;
          }
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_schema_version_range_by_name(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const ObTableNameNamespace table_namespace,
    const uint64_t database_id,
    const ObString &table_name,
    const uint64_t session_id,
    const ObNameCaseMode name_case_mode,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = OB_INVALID_VERSION;
  int64_t retry_count = 0;
  const int64_t abs_deadline_us = get_load_deadline_();
  version_range.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(table_namespace >= NS_MAX)
             || OB_INVALID_ID == database_id
             || table_name.empty()
             || name_case_mode <= OB_NAME_CASE_INVALID
             || name_case_mode >= OB_NAME_CASE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(table_name),
             K(table_namespace), K(name_case_mode));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(database_id),
             K(table_name));
  } else if (OB_CTE_DATABASE_ID == database_id) {
    // Virtual CTE database has no real database schema / table history.
    // Keep version_range as not-exist (already reset) to match SchemaMgr miss.
    // Placed after tenant-guard check so direct callers cannot bypass tenant validation.
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
    LOG_WARN("failed to get snapshot version", KR(ret), K(tenant_id), K(database_id),
             K(table_name));
  } else {
    LOG_TRACE("[OnDemandNameResolve] coordinator resolve by name",
             K(tenant_id), K(database_id), K(table_namespace), K(table_name),
             K(session_id), K(name_case_mode), K(snapshot_version));
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      ObTenantSchemaPublishState *state = NULL;
      ObSchemaPublishSnapshot publish_snapshot;
      ObSchemaResolveRange candidate;
      bool found_private = false;
      if (OB_FAIL(get_publish_snapshot_(guard, tenant_id, snapshot_version,
                                        abs_deadline_us, state, publish_snapshot))) {
        LOG_WARN("failed to get name resolve publication token", KR(ret), K(tenant_id),
                 K(database_id), K(table_name));
      } else {
        // Prefer current-session temporary table, then fall back to session_id=0.
        // Both probes use one publication token so they cannot cross generations.
        if (0 != session_id) {
          ObSchemaResolveRange private_range;
          // The key keeps the requested name verbatim; the case mode drives the
          // collation used by the key hash/compare (same contract as the
          // ORIGINAL SchemaMgr name map).
          const ObSchemaNameResolveKey private_key(
              tenant_id, publish_snapshot.cache_generations_.name_resolve_,
              database_id, session_id, table_namespace, name_case_mode, table_name);
          ret = get_version_range_by_name_with_session_id_(
              guard, private_key, table_name, snapshot_version, name_case_mode,
              abs_deadline_us, *state, publish_snapshot, private_range);
          if (OB_SUCCESS == ret && private_range.is_exist_) {
            candidate = private_range;
            found_private = true;
          } else if (OB_SUCCESS != ret && OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to resolve session-private table name", KR(ret), K(tenant_id),
                     K(database_id), K(table_name), K(session_id));
          }
        }
        if (OB_SUCC(ret) && !found_private) {
          const ObSchemaNameResolveKey global_key(
              tenant_id, publish_snapshot.cache_generations_.name_resolve_,
              database_id, 0 /*exact_session_id*/, table_namespace, name_case_mode,
              table_name);
          ret = get_version_range_by_name_with_session_id_(
              guard, global_key, table_name, snapshot_version, name_case_mode,
              abs_deadline_us, *state, publish_snapshot, candidate);
          if (OB_SUCCESS != ret && OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to resolve global table name", KR(ret), K(tenant_id),
                     K(database_id), K(table_name));
          }
        }
        if (OB_SUCC(ret)) {
          if (!state->can_publish(publish_snapshot)) {
            ret = OB_SCHEMA_EAGAIN;
          } else {
            version_range = candidate;
            done = true;
          }
        }
        if (OB_SCHEMA_EAGAIN == ret) {
          version_range.reset();
          ret = do_backoff_sleep_(abs_deadline_us, retry_count);
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_foreign_key_info_by_name(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &foreign_key_name,
    ObSimpleForeignKeyInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(database_id),
             K(foreign_key_name));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
    LOG_WARN("failed to get snapshot version", KR(ret), K(tenant_id), K(database_id),
             K(foreign_key_name));
  } else if (OB_FAIL(guard.get_schema_status(tenant_id, schema_status))) {
    LOG_WARN("failed to get schema status", KR(ret), K(tenant_id), K(database_id),
             K(foreign_key_name));
  } else if (OB_FAIL(ObSimpleSchemaLookupService::get_foreign_key_info_by_name(
      *schema_service_->get_sql_proxy(), *schema_service_->get_schema_service(),
      schema_status, tenant_id, database_id, foreign_key_name, snapshot_version, info))) {
    LOG_WARN("failed to get foreign key info by name", KR(ret), K(tenant_id), K(database_id),
             K(foreign_key_name));
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_constraint_info_by_name(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &constraint_name,
    ObSimpleConstraintInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(database_id),
             K(constraint_name));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
    LOG_WARN("failed to get snapshot version", KR(ret), K(tenant_id), K(database_id),
             K(constraint_name));
  } else if (OB_FAIL(guard.get_schema_status(tenant_id, schema_status))) {
    LOG_WARN("failed to get schema status", KR(ret), K(tenant_id), K(database_id),
             K(constraint_name));
  } else if (OB_FAIL(ObSimpleSchemaLookupService::get_constraint_info_by_name(
      *schema_service_->get_sql_proxy(), *schema_service_->get_schema_service(),
      schema_status, tenant_id, database_id, constraint_name, snapshot_version, info))) {
    LOG_WARN("failed to get constraint info by name", KR(ret), K(tenant_id), K(database_id),
             K(constraint_name));
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::load_simple_schema_from_cache_or_sql_(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t precise_schema_version,
    const int64_t abs_deadline_us,
    const ObSimpleTableSchemaV2 *&schema)
{
  int ret = OB_SUCCESS;
  bool done = false;
  schema = NULL;
  ret = get_local_simple_schema_(guard, tenant_id, table_id,
                                 precise_schema_version, schema);
  if (OB_SUCCESS == ret) {
    done = true;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  while (OB_SUCC(ret) && !done) {
    const ObSimpleSchemaCacheKey key(tenant_id, table_id, precise_schema_version);
    ObKVCacheHandle handle;
    if (ObTimeUtility::current_time() >= abs_deadline_us) {
      ret = OB_TIMEOUT;
    } else {
      ret = schema_service_->get_simple_schema_object_cache().get(key, schema, handle);
    }
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(pin_simple_schema_(guard, tenant_id, table_id, schema, handle))) {
        // The handle unpins on scope exit; never return a dangling pointer.
        schema = NULL;
        LOG_WARN("failed to pin simple schema cache hit", KR(ret), K(key));
      } else {
        done = true;
      }
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to read simple schema object cache", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
      ObSchemaLoadGuard load_guard;
      const ObSchemaLoadKey load_key(key);
      bool is_leader = false;
      uint64_t flight_id = 0;
      if (OB_FAIL(load_guard.acquire(
              schema_service_->get_simple_schema_load_coordinator(),
              load_key, is_leader, flight_id))) {
        LOG_WARN("failed to coordinate simple schema load", KR(ret), K(load_key));
      } else if (!is_leader) {
        if (OB_FAIL(schema_service_->get_simple_schema_load_coordinator().wait(
                load_key, flight_id, get_wait_deadline_(abs_deadline_us)))) {
          LOG_WARN("failed waiting for simple schema leader", KR(ret), K(load_key));
        }
      } else {
        ObArenaAllocator allocator("OnDmSchLoad");
        ObRefreshSchemaStatus schema_status;
        const ObSimpleTableSchemaV2 *loaded_schema = NULL;
        if (OB_FAIL(guard.get_schema_status(tenant_id, schema_status))) {
          LOG_WARN("failed to get schema status", KR(ret), K(tenant_id), K(table_id));
        } else if (OB_FAIL(ObSimpleSchemaLookupService::fetch_simple_table_schema(
            *schema_service_->get_sql_proxy(), *schema_service_->get_schema_service(),
            schema_status, tenant_id, table_id, precise_schema_version, allocator,
            loaded_schema))) {
          LOG_WARN("failed to fetch simple table schema", KR(ret), K(key));
        } else if (OB_ISNULL(loaded_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded simple schema is null", KR(ret), K(tenant_id), K(table_id),
                   K(precise_schema_version), K(key));
        } else {
          ObSimpleTableSchemaV2 *mutable_schema =
              const_cast<ObSimpleTableSchemaV2 *>(loaded_schema);
          if (OB_FAIL(ObSimpleSchemaLookupService::post_process_simple_table_schema(
              guard, tenant_id, *mutable_schema))) {
            LOG_WARN("failed to post process simple table schema", KR(ret), K(tenant_id),
                     K(table_id));
          } else if (ObTimeUtility::current_time() >= abs_deadline_us) {
            ret = OB_TIMEOUT;
            LOG_WARN("timeout when post process simple table schema", KR(ret), K(tenant_id),
                     K(table_id));
          } else {
            // An exact-version object stays valid across metadata publications.
            ret = schema_service_->get_simple_schema_object_cache().put_and_fetch(
                key, *mutable_schema, schema, handle);
            if (OB_SUCCESS != ret) {
              const int cache_ret = ret;
              ObSimpleTableSchemaV2 *local_copy = NULL;
              ret = ObSchemaUtils::alloc_schema(guard.local_allocator_,
                                                *mutable_schema, local_copy);
              if (OB_FAIL(ret)) {
                LOG_WARN("failed to make guard-local simple schema fallback", KR(ret),
                         KR(cache_ret), K(key));
              } else {
                schema = local_copy;
                handle.reset();
                LOG_WARN("simple schema cache publish failed, using guard-local object",
                         KR(cache_ret), K(key));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(pin_simple_schema_(guard, tenant_id, table_id, schema, handle))) {
                schema = NULL;
                LOG_WARN("failed to pin loaded simple schema", KR(ret), K(key));
              } else {
                done = true;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaOnDemandCoordinator::get_simple_table_schema(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 *&schema)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t retry_count = 0;
  int64_t snapshot_version = OB_INVALID_VERSION;
  const int64_t abs_deadline_us = get_load_deadline_();
  schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, snapshot_version))) {
    LOG_WARN("failed to get snapshot version", KR(ret), K(tenant_id), K(table_id));
  }
  while (OB_SUCC(ret) && !done) {
    bool retryable_stale_token = false;
    ObSchemaResolveRange version_range;
    ObSchemaPublishSnapshot publish_snapshot;
    ObTenantSchemaPublishState *state = NULL;
    if (OB_FAIL(get_publish_snapshot_(guard, tenant_id, snapshot_version,
                                      abs_deadline_us, state, publish_snapshot))) {
      LOG_WARN("failed to get publication token for object load",
               KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(get_version_range_by_id_once_(
                   guard, tenant_id, table_id, snapshot_version, abs_deadline_us,
                   *state, publish_snapshot, version_range))) {
      retryable_stale_token = (OB_SCHEMA_EAGAIN == ret);
      if (OB_SCHEMA_EAGAIN != ret) {
        LOG_WARN("failed to get schema version range", KR(ret), K(tenant_id), K(table_id));
      }
    } else if (!version_range.is_exist_) {
      if (state->can_publish(publish_snapshot)) {
        schema = NULL;
        done = true;
      } else {
        ret = OB_SCHEMA_EAGAIN;
        retryable_stale_token = true;
      }
    } else {
      const int64_t schema_obj_count = guard.schema_objs_.count();
      const int64_t pin_cache_size = guard.pin_cache_size_;
      if (OB_FAIL(load_simple_schema_from_cache_or_sql_(
              guard, tenant_id, table_id, version_range.precise_schema_version_,
              abs_deadline_us, schema))) {
        const int load_ret = ret;
        const int rollback_ret = rollback_schema_pins_(
            guard, schema_obj_count, pin_cache_size);
        if (OB_SUCCESS != rollback_ret) {
          ret = rollback_ret;
        } else {
          ret = load_ret;
        }
        retryable_stale_token = (OB_SCHEMA_EAGAIN == ret);
        if (OB_SCHEMA_EAGAIN != ret) {
          LOG_WARN("failed to load simple table schema", KR(ret), K(tenant_id), K(table_id));
        }
      } else {
        // ID resolution selected the precise version for this Guard's snapshot.
        // A later metadata publication cannot invalidate the loaded object.
        done = true;
      }
    }
    if (OB_SCHEMA_EAGAIN == ret && retryable_stale_token) {
      schema = NULL;
      ret = do_backoff_sleep_(abs_deadline_us, retry_count);
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

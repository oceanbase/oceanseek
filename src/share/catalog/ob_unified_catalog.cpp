/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog.h"

#include <stdint.h>

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/catalog/ob_unified_catalog_config.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/catalog/ob_unified_catalog_client.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace share
{

ObUnifiedCatalogSyncTarget::ObUnifiedCatalogSyncTarget(
    const uint64_t tenant_id,
    const ObString &access_token)
  : tenant_id_(tenant_id),
    access_token_(access_token),
    catalog_name_(),
    endpoint_(),
    realm_()
{
}

int ObUnifiedCatalogSyncTarget::capture(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString access_token;
  ObUnifiedCatalogConfig config;
  reset_snapshot();
  if (!is_valid_tenant_id(tenant_id_) || access_token_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog sync target identity", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ob_write_string(allocator, access_token_, access_token))) {
    LOG_WARN("failed to copy Unified Catalog access token", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObUnifiedCatalog::resolve_target(tenant_id_,
                                                       allocator,
                                                       catalog_name_,
                                                       &config))) {
    reset_snapshot();
    LOG_WARN("failed to capture Unified Catalog sync target", K(ret), K(tenant_id_));
  } else {
    access_token_ = access_token;
    endpoint_ = config.endpoint_;
    realm_ = config.realm_;
  }
  return ret;
}

bool ObUnifiedCatalog::is_configured(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    const char *config = tenant_config->unified_catalog_config.str();
    enabled = OB_NOT_NULL(config) && '\0' != config[0];
  }
  return enabled;
}

bool ObUnifiedCatalog::is_child_catalog_id(const uint64_t tenant_id,
                                           const uint64_t catalog_id)
{
  return 0 < catalog_id && catalog_id <= INT64_MAX && is_configured(tenant_id);
}

int ObUnifiedCatalog::get_session_token(const sql::ObBasicSessionInfo &session,
                                         ObIAllocator &allocator,
                                         ObString &access_token)
{
  int ret = OB_SUCCESS;
  ObString session_token;
  access_token.reset();
  if (OB_FAIL(session.get_sys_variable(SYS_VAR__UNIFIED_CATALOG_TOKEN, session_token))) {
    LOG_WARN("failed to get Unified Catalog session token", K(ret));
  } else if (session_token.empty()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Unified Catalog access without _unified_catalog_token");
    LOG_WARN("Unified Catalog session token is not set", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, session_token, access_token))) {
    LOG_WARN("failed to copy Unified Catalog session token", K(ret));
  }
  return ret;
}

int ObUnifiedCatalog::catalog_exists(const uint64_t tenant_id,
                                     const ObString &access_token,
                                     const ObString &catalog_name,
                                     ObIAllocator &allocator,
                                     bool &exists)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  exists = false;
  if (catalog_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty Unified Catalog name", K(ret));
  } else if (OB_FAIL(config.load(tenant_id, allocator))) {
    LOG_WARN("failed to load Unified Catalog configuration", K(ret));
  } else if (OB_FAIL(client.init(config,
                                 access_token,
                                 catalog_name,
                                 static_cast<uint64_t>(GCONF.cluster_id),
                                 tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog lookup", K(ret));
  } else if (OB_FAIL(client.reader().catalog_exists(exists))) {
    LOG_WARN("failed to check Unified Catalog existence", K(ret), K(catalog_name));
  }
  return ret;
}

int ObUnifiedCatalog::validate_sync_target(
    const ObUnifiedCatalogSyncTarget &target)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = target.tenant_id();
  ObArenaAllocator allocator("UnifiedCatalog", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             tenant_id);
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (!target.is_captured()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unified Catalog sync target is not captured", K(ret), K(tenant_id));
  } else if (OB_FAIL(resolve_target(tenant_id,
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog sync target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog sync target changed", K(ret), K(tenant_id));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            tenant_id))
        || OB_FAIL(client.reader().validate_catalog())) {
      LOG_WARN("failed to validate Unified Catalog sync target", K(ret), K(remote_catalog_name));
    }
  }
  return ret;
}

int ObUnifiedCatalog::resolve_target(const uint64_t tenant_id,
                                      ObIAllocator &allocator,
                                      ObString &remote_catalog_name,
                                      ObUnifiedCatalogConfig *config)
{
  int ret = OB_SUCCESS;
  remote_catalog_name.reset();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant config", K(ret), K(tenant_id));
  } else if (!is_configured(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Unified Catalog sync without unified_catalog_config");
    LOG_WARN("unified_catalog_config is empty for synchronization", K(ret), K(tenant_id));
  } else {
    const char *remote_target = tenant_config->catalog_sync_target.str();
    if (OB_ISNULL(remote_target) || '\0' == remote_target[0]) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "catalog sync without catalog_sync_target");
      LOG_WARN("catalog_sync_target is empty", K(ret), K(tenant_id));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(remote_target), remote_catalog_name))) {
      LOG_WARN("failed to copy catalog_sync_target", K(ret));
    } else if (OB_NOT_NULL(config) && OB_FAIL(config->load(tenant_id, allocator))) {
      LOG_WARN("failed to load Unified Catalog connection config", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalog::snapshot_raw_target(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    ObString &catalog_sync_target,
    ObString &unified_catalog_config)
{
  int ret = OB_SUCCESS;
  catalog_sync_target.reset();
  unified_catalog_config.reset();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *raw_target = tenant_config->catalog_sync_target.str();
    const char *raw_config = tenant_config->unified_catalog_config.str();
    const ObString target_value(OB_ISNULL(raw_target) ? "" : raw_target);
    const ObString config_value(OB_ISNULL(raw_config) ? "" : raw_config);
    if (OB_FAIL(ob_write_string(allocator, target_value, catalog_sync_target))) {
      catalog_sync_target.reset();
      unified_catalog_config.reset();
    } else if (OB_FAIL(ob_write_string(
                   allocator, config_value, unified_catalog_config))) {
      catalog_sync_target.reset();
      unified_catalog_config.reset();
    }
  }
  return ret;
}

int ObUnifiedCatalog::validate_expected_config(
    const ObString &expected_remote_catalog_name,
    const ObString &configured_remote_catalog_name)
{
  int ret = OB_SUCCESS;
  if (expected_remote_catalog_name != configured_remote_catalog_name) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "SYNC CATALOG target configuration changed");
    LOG_WARN("Unified Catalog target changed between resolve and execute", K(ret),
             K(expected_remote_catalog_name), K(configured_remote_catalog_name));
  }
  return ret;
}

int ObUnifiedCatalog::validate_expected_namespace_target(
    const ObString &expected_remote_catalog_name,
    const ObString &expected_endpoint,
    const ObString &expected_realm,
    const ObString &configured_remote_catalog_name,
    const ObUnifiedCatalogConfig &configured_config)
{
  int ret = OB_SUCCESS;
  if (expected_remote_catalog_name != configured_remote_catalog_name
      || expected_endpoint != configured_config.endpoint_
      || expected_realm != configured_config.realm_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog namespace target changed", K(ret),
             K(expected_remote_catalog_name), K(configured_remote_catalog_name));
  }
  return ret;
}

int ObUnifiedCatalog::create_namespace(const ObUnifiedCatalogSyncTarget &target,
                                       const ObString &database_name,
                                       const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (OB_INVALID_ID == database_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace source identity", K(ret), K(database_id), K(database_name));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(
                 target.catalog_name(),
                 target.endpoint(),
                 target.realm(),
                 remote_catalog_name,
                 config))) {
    LOG_WARN("Unified Catalog create namespace target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().create_namespace(database_name, database_id))) {
      LOG_WARN("failed to create Unified Catalog namespace",
               K(ret), K(remote_catalog_name), K(database_name), K(database_id));
    }
  }
  return ret;
}

int ObUnifiedCatalog::check_namespace_empty(const ObUnifiedCatalogSyncTarget &target,
                                            const ObString &database_name,
                                            const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  bool has_tables = false;
  if (database_name.empty() || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace source identity", K(ret), K(database_name), K(database_id));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(
                 target.catalog_name(),
                 target.endpoint(),
                 target.realm(),
                 remote_catalog_name,
                 config))) {
    LOG_WARN("Unified Catalog namespace inspection target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.reader().namespace_has_tables(
            database_name, database_id, has_tables))) {
      LOG_WARN("failed to inspect Unified Catalog namespace", K(ret), K(database_name));
    } else if (has_tables) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                     "drop database with tables in the target Unified Catalog");
      LOG_WARN("Unified Catalog namespace is not empty", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalog::drop_namespace(const ObUnifiedCatalogSyncTarget &target,
                                     const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace name", K(ret), K(database_name));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(
                 target.catalog_name(),
                 target.endpoint(),
                 target.realm(),
                 remote_catalog_name,
                 config))) {
    LOG_WARN("Unified Catalog drop namespace target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().drop_namespace(database_name))) {
      LOG_WARN("failed to drop Unified Catalog namespace", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalog::sync_database(
    const uint64_t tenant_id,
    const ObString &access_token,
    const ObString &expected_remote_catalog_name,
    const ObString &database_name,
    const uint64_t database_id,
    const ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (!is_valid_tenant_id(tenant_id) || database_name.empty()
      || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid database source identity",
             K(ret), K(tenant_id), K(database_name), K(database_id));
  } else if (OB_FAIL(resolve_target(tenant_id,
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(validate_expected_config(expected_remote_catalog_name,
                                              remote_catalog_name))) {
    LOG_WARN("unexpected Unified Catalog configuration", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            access_token,
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            tenant_id))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().create_namespace(database_name, database_id))) {
      LOG_WARN("failed to create Unified Catalog namespace",
               K(ret), K(remote_catalog_name), K(database_name), K(database_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(database_name), K(i));
      } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(*table_schema))) {
        LOG_WARN("table schema failed Unified Catalog synchronization preflight",
                 K(ret), K(database_name), K(table_schema->get_table_name_str()));
      } else if (OB_FAIL(client.writer().sync_table(database_name, database_id, *table_schema))) {
        LOG_WARN("failed to sync OceanBase table to Unified Catalog",
                 K(ret), K(database_name), K(table_schema->get_table_name_str()));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalog::reserve_create_table(const ObUnifiedCatalogSyncTarget &target,
                                            const ObString &database_name,
                                            const uint64_t database_id,
                                            const ObTableSchema &table_schema,
                                            const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             target.tenant_id());
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (database_name.empty() || OB_INVALID_ID == database_id || operation_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog create reservation", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_create_table_schema(table_schema))) {
    LOG_WARN("table schema failed Unified Catalog create preflight", K(ret),
             K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog create target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog create reservation target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().create_namespace(database_name, database_id))
        || OB_FAIL(client.writer().reserve_table(database_name,
                                                 database_id,
                                                 table_schema.get_table_name_str(),
                                                 operation_id))) {
      LOG_WARN("failed to reserve Unified Catalog table name",
               K(ret),
               K(database_name),
               K(table_schema.get_table_name_str()));
    }
  }
  return ret;
}

int ObUnifiedCatalog::complete_create_table(const ObUnifiedCatalogSyncTarget &target,
                                             const ObString &database_name,
                                             const ObTableSchema &table_schema,
                                             const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCreate", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             target.tenant_id());
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(table_schema))) {
    LOG_WARN("completed table schema failed Unified Catalog preflight", K(ret));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog create target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog create target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().complete_reserved_table(database_name,
                                                           table_schema.get_database_id(),
                                                           table_schema, operation_id))) {
      LOG_WARN("failed to complete Unified Catalog table creation", K(ret), K(database_name),
               K(table_schema.get_table_name_str()));
    }
  }
  return ret;
}

int ObUnifiedCatalog::cancel_create_table(const ObUnifiedCatalogSyncTarget &target,
                                          const ObString &database_name,
                                          const ObString &table_name,
                                          const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCancel", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             target.tenant_id());
  ObUnifiedCatalogConfig config;
  if (!target.is_captured()
      || database_name.empty() || table_name.empty() || operation_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog reservation cancellation identity", K(ret),
             K(database_name), K(table_name));
  } else if (OB_FAIL(config.load(target.tenant_id(), allocator))) {
    LOG_WARN("failed to resolve reserved Unified Catalog connection", K(ret));
  } else if (target.endpoint() != config.endpoint_ || target.realm() != config.realm_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog connection changed before reservation cancellation", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            target.catalog_name(),
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))) {
      LOG_WARN("failed to initialize Unified Catalog reservation cancellation client", K(ret));
    } else if (OB_FAIL(client.reader().validate_catalog())) {
      LOG_WARN("failed to validate Unified Catalog reservation cancellation Catalog", K(ret));
    } else if (OB_FAIL(client.writer().cancel_reserved_table(database_name,
                                                             table_name,
                                                             operation_id))) {
      LOG_WARN("failed to cancel Unified Catalog create reservation", K(ret),
               K(database_name), K(table_name));
    }
  }
  return ret;
}

int ObUnifiedCatalog::sync_table(const uint64_t tenant_id,
                                 const ObString &access_token,
                                 const ObString &expected_remote_catalog_name,
                                 const ObString &database_name,
                                 const ObTableSchema &table_schema,
                                 const bool recover_preparing)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObUnifiedCatalogSyncTarget target(tenant_id, access_token);
  if (OB_FAIL(target.capture(allocator))) {
    LOG_WARN("failed to capture Unified Catalog table target", K(ret), K(tenant_id));
  } else if (OB_FAIL(validate_expected_config(expected_remote_catalog_name,
                                              target.catalog_name()))) {
    LOG_WARN("unexpected Unified Catalog configuration", K(ret));
  } else if (OB_FAIL(sync_table_to_target(target,
                                          database_name,
                                          table_schema,
                                          recover_preparing))) {
    LOG_WARN("failed to sync table to Unified Catalog target",
             K(ret), K(tenant_id), K(database_name),
             K(table_schema.get_table_name_str()));
  }
  return ret;
}

int ObUnifiedCatalog::sync_table_to_target(
    const ObUnifiedCatalogSyncTarget &target,
    const ObString &database_name,
    const ObTableSchema &table_schema,
    const bool recover_preparing)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(table_schema))) {
    LOG_WARN("table schema failed Unified Catalog synchronization preflight", K(ret),
             K(database_name), K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(resolve_target(target.tenant_id(),
                                    allocator,
                                    remote_catalog_name,
                                    &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog table target changed", K(ret));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            target.tenant_id()))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().create_namespace(database_name,
                                                    table_schema.get_database_id()))
        || OB_FAIL(client.writer().sync_table(database_name,
                                              table_schema.get_database_id(),
                                              table_schema, recover_preparing))) {
      LOG_WARN("failed to sync OceanBase table to Unified Catalog",
               K(ret), K(database_name), K(table_schema.get_table_name_str()));
    }
  }
  return ret;
}

int ObUnifiedCatalog::update_table_to_target(const ObUnifiedCatalogSyncTarget &target,
                                             const ObString &database_name,
                                             const ObTableSchema &table_schema,
                                             const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = target.tenant_id();
  ObArenaAllocator allocator("UnifiedCatalog", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             tenant_id);
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (OB_INVALID_ID == expected_source_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog table update source",
             K(ret),
             K(tenant_id),
             K(database_name),
             K(expected_source_table_id));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(table_schema))) {
    LOG_WARN("table schema failed Unified Catalog update preflight",
             K(ret),
             K(tenant_id),
             K(database_name),
             K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(resolve_target(tenant_id, allocator, remote_catalog_name, &config))) {
    LOG_WARN("failed to resolve Unified Catalog update target",
             K(ret),
             K(tenant_id),
             K(database_name));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog table update target changed",
             K(ret),
             K(tenant_id),
             K(database_name));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            tenant_id))) {
      LOG_WARN("failed to initialize Unified Catalog table update client",
               K(ret),
               K(tenant_id),
               K(database_name));
    } else if (OB_FAIL(client.reader().validate_catalog())) {
      LOG_WARN("failed to validate Unified Catalog table update Catalog",
               K(ret),
               K(tenant_id),
               K(database_name));
    } else if (OB_FAIL(client.writer().update_table(database_name,
                                                    table_schema,
                                                    expected_source_table_id))) {
      LOG_WARN("failed to update OceanBase table in Unified Catalog",
               K(ret),
               K(tenant_id),
               K(database_name),
               K(table_schema.get_table_name_str()),
               K(expected_source_table_id));
    }
  }
  return ret;
}

int ObUnifiedCatalog::rename_table_to_target(const ObUnifiedCatalogSyncTarget &target,
                                             const ObString &source_database_name,
                                             const ObString &source_table_name,
                                             const ObString &destination_database_name,
                                             const ObTableSchema &final_table_schema,
                                             const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = target.tenant_id();
  ObArenaAllocator allocator("UnifiedCatalog", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             tenant_id);
  ObUnifiedCatalogConfig config;
  ObString remote_catalog_name;
  if (source_database_name.empty() || source_table_name.empty() || destination_database_name.empty()
      || OB_INVALID_ID == expected_source_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog table rename argument",
             K(ret),
             K(tenant_id),
             K(source_database_name),
             K(source_table_name),
             K(destination_database_name),
             K(expected_source_table_id));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(final_table_schema))) {
    LOG_WARN("table schema failed Unified Catalog rename preflight",
             K(ret),
             K(tenant_id),
             K(source_database_name),
             K(source_table_name),
             K(destination_database_name),
             K(final_table_schema.get_table_name_str()));
  } else if (OB_FAIL(resolve_target(tenant_id, allocator, remote_catalog_name, &config))) {
    LOG_WARN("failed to resolve Unified Catalog rename target",
             K(ret),
             K(tenant_id),
             K(source_database_name),
             K(source_table_name));
  } else if (OB_FAIL(validate_expected_namespace_target(target.catalog_name(),
                                                        target.endpoint(),
                                                        target.realm(),
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog table rename target changed",
             K(ret),
             K(tenant_id),
             K(source_database_name),
             K(source_table_name));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            target.access_token(),
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            tenant_id))) {
      LOG_WARN("failed to initialize Unified Catalog table rename client",
               K(ret),
               K(tenant_id),
               K(source_database_name),
               K(source_table_name));
    } else if (OB_FAIL(client.reader().validate_catalog())) {
      LOG_WARN("failed to validate Unified Catalog table rename Catalog",
               K(ret),
               K(tenant_id),
               K(source_database_name),
               K(source_table_name));
    } else if (OB_FAIL(client.writer().create_namespace(destination_database_name,
                                                        final_table_schema.get_database_id()))) {
      LOG_WARN("failed to validate Unified Catalog rename destination namespace",
               K(ret),
               K(tenant_id),
               K(destination_database_name));
    } else if (OB_FAIL(client.writer().rename_table(source_database_name,
                                                    source_table_name,
                                                    destination_database_name,
                                                    final_table_schema,
                                                    expected_source_table_id))) {
      LOG_WARN("failed to rename OceanBase table in Unified Catalog",
               K(ret),
               K(tenant_id),
               K(source_database_name),
               K(source_table_name),
               K(destination_database_name),
               K(final_table_schema.get_table_name_str()),
               K(expected_source_table_id));
    }
  }
  return ret;
}

int ObUnifiedCatalog::drop_table(const uint64_t tenant_id,
                                 const ObString &access_token,
                                 const ObString &database_name,
                                 const ObString &table_name,
                                 const ObString &expected_raw_target,
                                 const ObString &expected_raw_config)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UnifiedCatalog");
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogConfig snapshot_config;
  ObString remote_catalog_name;
  if (!is_valid_tenant_id(tenant_id) || database_name.empty() || table_name.empty()
      || expected_raw_target.empty() || expected_raw_config.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog drop table argument",
             K(ret),
             K(tenant_id),
             K(database_name),
             K(table_name));
  } else if (OB_FAIL(resolve_target(tenant_id, allocator, remote_catalog_name, &config))) {
    LOG_WARN("failed to resolve Unified Catalog target", K(ret));
  } else if (OB_FAIL(snapshot_config.parse(expected_raw_config, allocator))) {
    LOG_WARN("failed to parse Unified Catalog drop table target snapshot",
             K(ret), K(tenant_id), K(database_name), K(table_name));
  } else if (OB_FAIL(validate_expected_namespace_target(expected_raw_target,
                                                        snapshot_config.endpoint_,
                                                        snapshot_config.realm_,
                                                        remote_catalog_name,
                                                        config))) {
    LOG_WARN("Unified Catalog drop table target changed",
             K(ret), K(tenant_id), K(database_name), K(table_name));
  } else {
    ObUnifiedCatalogClient client(allocator);
    if (OB_FAIL(client.init(config,
                            access_token,
                            remote_catalog_name,
                            static_cast<uint64_t>(GCONF.cluster_id),
                            tenant_id))
        || OB_FAIL(client.reader().validate_catalog())
        || OB_FAIL(client.writer().drop_table(database_name, table_name))) {
      LOG_WARN("failed to drop Unified Catalog OceanBase Table",
               K(ret), K(database_name), K(table_name));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

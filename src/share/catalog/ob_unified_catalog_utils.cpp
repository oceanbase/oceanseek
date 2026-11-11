/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_utils.h"

#include "lib/container/ob_se_array.h"
#include "share/catalog/ob_catalog_utils.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/catalog/ob_unified_catalog_client.h"
#include "share/catalog/ob_unified_catalog_config.h"
#include "share/catalog/ob_unified_catalog_namespace_codec.h"
#include "share/catalog/rest/responses/ob_rest_http_response.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_catalog_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace share
{

int ObUnifiedCatalogUtils::check_local_catalog_conflict(const uint64_t tenant_id,
                                                        const ObString &remote_catalog_name,
                                                        ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObCatalogSchema *local_schema = NULL;
  if (OB_FAIL(
          schema_guard.get_catalog_schema_by_name(tenant_id, remote_catalog_name, local_schema))) {
    LOG_WARN("failed to check local Catalog name", K(ret));
  } else if (OB_NOT_NULL(local_schema) && is_external_catalog_id(local_schema->get_catalog_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("Unified Catalog conflicts with local Catalog", K(ret), K(remote_catalog_name));
  }
  return ret;
}

int ObUnifiedCatalogUtils::resolve_catalog_id(const uint64_t tenant_id,
                                              const ObString &access_token,
                                              const ObString &remote_catalog_name,
                                              ObSchemaGetterGuard &schema_guard,
                                              ObIAllocator &allocator,
                                              uint64_t &child_catalog_id)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObSEArray<ObUnifiedCatalogIdentity, 8> catalogs;
  bool catalog_exists = false;
  child_catalog_id = OB_INVALID_ID;
  if (remote_catalog_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty remote Catalog name", K(ret));
  } else if (OB_FAIL(config.load(tenant_id, allocator))) {
    LOG_WARN("failed to load UC connection config", K(ret));
  } else if (OB_FAIL(client.init(config,
                                 access_token,
                                 remote_catalog_name,
                                 static_cast<uint64_t>(GCONF.cluster_id),
                                 tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().catalog_exists(catalog_exists))) {
    LOG_WARN("failed to load Unified Catalog", K(ret), K(remote_catalog_name));
  } else if (!catalog_exists) {
    ret = OB_CATALOG_NOT_EXIST;
    LOG_WARN("Unified Catalog does not exist", K(ret), K(remote_catalog_name));
  } else if (OB_FAIL(check_local_catalog_conflict(tenant_id, remote_catalog_name, schema_guard))) {
    LOG_WARN("failed to check local Catalog conflict", K(ret), K(remote_catalog_name));
  } else if (OB_FAIL(client.reader().list_accessible_catalog_identities(catalogs))) {
    LOG_WARN("failed to list accessible Unified Catalogs", K(ret));
  } else {
    for (int64_t i = 0; i < catalogs.count(); ++i) {
      if (catalogs.at(i).name_ == remote_catalog_name) {
        child_catalog_id = catalogs.at(i).id_;
        break;
      }
    }
    if (OB_INVALID_ID == child_catalog_id) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog is not accessible", K(ret), K(remote_catalog_name));
    }
  }
  if (OB_SUCC(ret)) {
    const ObCatalogSchema *local_schema = NULL;
    if (OB_FAIL(schema_guard.get_catalog_schema_by_id(tenant_id, child_catalog_id, local_schema))) {
      LOG_WARN("failed to check local Catalog ID", K(ret), K(child_catalog_id));
    } else if (OB_NOT_NULL(local_schema)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog ID conflicts with local Catalog", K(ret), K(child_catalog_id));
    }
  }
  return ret;
}

int ObUnifiedCatalogUtils::resolve_catalog_name(const uint64_t tenant_id,
                                                const ObString &access_token,
                                                const uint64_t child_catalog_id,
                                                ObSchemaGetterGuard &schema_guard,
                                                ObIAllocator &allocator,
                                                ObString &remote_catalog_name)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObSEArray<ObUnifiedCatalogIdentity, 8> catalogs;
  remote_catalog_name.reset();
  if (!ObUnifiedCatalog::is_child_catalog_id(tenant_id, child_catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog ID", K(ret), K(child_catalog_id));
  } else if (OB_FAIL(config.load(tenant_id, allocator))) {
    LOG_WARN("failed to load UC connection config", K(ret));
  } else if (OB_FAIL(client.init_connection(config,
                                            access_token,
                                            static_cast<uint64_t>(GCONF.cluster_id),
                                            tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog list client", K(ret));
  } else if (OB_FAIL(client.reader().list_accessible_catalog_identities(catalogs))) {
    LOG_WARN("failed to list accessible Unified Catalogs", K(ret));
  } else {
    for (int64_t i = 0; i < catalogs.count(); ++i) {
      if (catalogs.at(i).id_ == child_catalog_id) {
        remote_catalog_name = catalogs.at(i).name_;
        break;
      }
    }
    if (remote_catalog_name.empty()) {
      ret = OB_CATALOG_NOT_EXIST;
      LOG_WARN("Unified Catalog ID is not accessible", K(ret), K(child_catalog_id));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(check_local_catalog_conflict(tenant_id, remote_catalog_name, schema_guard))) {
    LOG_WARN("failed to check local Catalog conflict", K(ret), K(remote_catalog_name));
  }
  return ret;
}

int ObUnifiedCatalogUtils::try_resolve_catalog(const uint64_t tenant_id,
                                               const ObString &access_token,
                                               const ObString &requested_catalog_name,
                                               const uint64_t current_catalog_id,
                                               const ObNameCaseMode case_mode,
                                               ObSchemaGetterGuard &schema_guard,
                                               ObIAllocator &allocator,
                                               uint64_t &catalog_id,
                                               ObString &remote_catalog_name,
                                               bool &is_remote_catalog)
{
  int ret = OB_SUCCESS;
  const ObCatalogSchema *local_catalog_schema = NULL;
  catalog_id = current_catalog_id;
  remote_catalog_name.reset();
  is_remote_catalog = false;

  if (!requested_catalog_name.empty()) {
    if (ObCatalogUtils::is_internal_catalog_name(requested_catalog_name, case_mode)
        || access_token.empty()) {
      // Use the caller's original local Catalog path.
    } else if (OB_FAIL(resolve_catalog_id(tenant_id,
                                          access_token,
                                          requested_catalog_name,
                                          schema_guard,
                                          allocator,
                                          catalog_id))) {
      if (OB_CATALOG_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        catalog_id = current_catalog_id;
      } else {
        LOG_WARN("failed to resolve Unified Catalog", K(ret), K(requested_catalog_name));
      }
    } else if (OB_FAIL(ob_write_string(allocator, requested_catalog_name, remote_catalog_name))) {
      LOG_WARN("failed to copy Unified Catalog name", K(ret), K(requested_catalog_name));
    } else {
      is_remote_catalog = true;
    }
  } else if (is_internal_catalog_id(current_catalog_id)) {
    // Use the caller's original internal Catalog path.
  } else if (OB_FAIL(schema_guard.get_catalog_schema_by_id(tenant_id,
                                                           current_catalog_id,
                                                           local_catalog_schema))) {
    LOG_WARN("failed to get current local Catalog", K(ret), K(tenant_id), K(current_catalog_id));
  } else if (OB_NOT_NULL(local_catalog_schema)) {
    // The current Catalog is a local external Catalog.
  } else if (access_token.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("current Unified Catalog cannot be restored without session token",
             K(ret),
             K(current_catalog_id));
  } else {
    uint64_t restored_catalog_id = OB_INVALID_ID;
    if (OB_FAIL(resolve_catalog_name(tenant_id,
                                     access_token,
                                     current_catalog_id,
                                     schema_guard,
                                     allocator,
                                     remote_catalog_name))) {
      LOG_WARN("failed to resolve current Unified Catalog name", K(ret), K(current_catalog_id));
    } else if (OB_FAIL(resolve_catalog_id(tenant_id,
                                          access_token,
                                          remote_catalog_name,
                                          schema_guard,
                                          allocator,
                                          restored_catalog_id))) {
      LOG_WARN("failed to restore current Unified Catalog", K(ret), K(remote_catalog_name));
    } else if (current_catalog_id != restored_catalog_id) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("current Unified Catalog ID changed",
               K(ret),
               K(current_catalog_id),
               K(restored_catalog_id));
    } else {
      catalog_id = restored_catalog_id;
      is_remote_catalog = true;
    }
  }
  return ret;
}

int ObUnifiedCatalogUtils::validate_database(const uint64_t tenant_id,
                                             const ObString &access_token,
                                             const ObString &remote_catalog_name,
                                             const ObString &database_name,
                                             ObSchemaGetterGuard &schema_guard,
                                             ObIAllocator &allocator,
                                             bool &need_local_db_access)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObUnifiedCatalogInfo catalog_info;
  ObString response_body;
  need_local_db_access = false;

  if (access_token.empty() || remote_catalog_name.empty() || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog database request",
             K(ret),
             K(remote_catalog_name),
             K(database_name));
  } else if (OB_FAIL(config.load(tenant_id, allocator))) {
    LOG_WARN("failed to load UC connection config", K(ret));
  } else if (OB_FAIL(client.init(config,
                                 access_token,
                                 remote_catalog_name,
                                 static_cast<uint64_t>(GCONF.cluster_id),
                                 tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().get_catalog_info(catalog_info))) {
    LOG_WARN("failed to load Unified Catalog source", K(ret), K(remote_catalog_name));
  } else if (ObUnifiedCatalogKind::OCEANBASE != catalog_info.kind_
             && ObUnifiedCatalogKind::ICEBERG != catalog_info.kind_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported Unified Catalog source", K(ret), K(remote_catalog_name));
  } else if (ObUnifiedCatalogKind::OCEANBASE == catalog_info.kind_
             && OB_FAIL(client.reader().validate_catalog())) {
    LOG_WARN("Unified Catalog source identity mismatch", K(ret), K(remote_catalog_name));
  } else if (OB_FAIL(client.reader().load_namespace(database_name, allocator, response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
      LOG_WARN("Unified Catalog namespace does not exist",
               K(ret),
               K(remote_catalog_name),
               K(database_name));
    } else {
      LOG_WARN("failed to load Unified Catalog namespace",
               K(ret),
               K(remote_catalog_name),
               K(database_name));
    }
  } else if (ObUnifiedCatalogKind::OCEANBASE == catalog_info.kind_) {
    const ObDatabaseSchema *local_database_schema = NULL;
    if (OB_FAIL(
            schema_guard.get_database_schema(tenant_id, database_name, local_database_schema))) {
      LOG_WARN("failed to get local database schema", K(ret), K(tenant_id), K(database_name));
    } else if (OB_ISNULL(local_database_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
      LOG_WARN("local database does not exist", K(ret), K(tenant_id), K(database_name));
    } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::validate_namespace_response(
                   response_body,
                   database_name,
                   static_cast<uint64_t>(GCONF.cluster_id),
                   tenant_id,
                   local_database_schema->get_database_id()))) {
      LOG_WARN("Unified Catalog namespace source identity mismatch",
               K(ret),
               K(remote_catalog_name),
               K(database_name));
    } else {
      need_local_db_access = true;
    }
  } else if (OB_FAIL(
                 ObUnifiedCatalogNamespaceCodec::validate_read_namespace_response(tenant_id,
                                                                                  response_body,
                                                                                  database_name))) {
    LOG_WARN("failed to validate Unified Catalog namespace", K(ret), K(database_name));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

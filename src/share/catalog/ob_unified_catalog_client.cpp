/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_client.h"
#include "share/catalog/ob_unified_catalog_config.h"

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/rest/client/ob_catalog_client_pool.h"
#include "share/catalog/ob_unified_catalog_namespace_codec.h"
#include "share/catalog/ob_unified_catalog_partition_sync.h"
#include "share/catalog/ob_unified_catalog_resource_paths.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
namespace share
{
ObUnifiedCatalogClient::ObUnifiedCatalogClient(ObIAllocator &allocator)
    : allocator_(&allocator), transport_(nullptr), config_(nullptr), catalog_name_(),
      authorization_(), cluster_id_(OB_INVALID_ID), tenant_id_(OB_INVALID_ID),
      is_inited_(false), reader_(*this), writer_(*this, reader_)
{
}

ObUnifiedCatalogClient::~ObUnifiedCatalogClient()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(transport_)) {
    ObCatalogClientPool<ObCurlRestClient> *client_pool
        = static_cast<ObCatalogClientPool<ObCurlRestClient> *>(transport_->get_client_pool());
    if (OB_FAIL(transport_->reuse())) {
      LOG_WARN("failed to reuse Unified Catalog REST client",
               K(ret),
               K_(tenant_id),
               K_(catalog_name));
    }
    if (OB_NOT_NULL(client_pool) && OB_FAIL(client_pool->return_client(transport_))) {
      LOG_WARN("failed to return Unified Catalog REST client to pool",
               K(ret),
               K_(tenant_id),
               K_(catalog_name));
    }
    transport_ = nullptr;
  }
}

int ObUnifiedCatalogClient::init(const ObUnifiedCatalogConfig &config,
                                 const ObString &access_token,
                                 const ObString &catalog_name,
                                 const uint64_t cluster_id,
                                 const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Unified Catalog client already initialized", K(ret));
  } else if (catalog_name.empty() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog client identity", K(ret), K(cluster_id), K(tenant_id));
  } else if (OB_FAIL(ob_write_string(*allocator_, catalog_name, catalog_name_))) {
    LOG_WARN("failed to copy Unified Catalog name", K(ret));
  } else if (OB_FAIL(init_connection_impl(config, access_token, cluster_id, tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog connection", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogClient::init_connection(const ObUnifiedCatalogConfig &config,
                                            const ObString &access_token,
                                            const uint64_t cluster_id,
                                            const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Unified Catalog client already initialized", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog client identity", K(ret), K(cluster_id), K(tenant_id));
  } else if (OB_FAIL(init_connection_impl(config, access_token, cluster_id, tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog connection", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogClient::init_connection_impl(const ObUnifiedCatalogConfig &config,
                                                 const ObString &access_token,
                                                 const uint64_t cluster_id,
                                                 const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObRestCatalogProperties rest_properties;
  ObString serialized_properties;
  ObSqlString authorization;
  ObCatalogClientPoolMgr<ObCurlRestClient> *client_pool_mgr
      = MTL(ObCatalogClientPoolMgr<ObCurlRestClient> *);
  config_ = &config;
  cluster_id_ = cluster_id;
  tenant_id_ = tenant_id;
  rest_properties.uri_ = config.endpoint_;
  if (!is_valid_access_token(access_token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unified catalog access token", K(ret));
  } else if (OB_FAIL(authorization.assign_fmt("Bearer %.*s",
                                             access_token.length(), access_token.ptr()))) {
    LOG_WARN("failed to format Unified Catalog authorization", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, authorization.string(), authorization_))) {
    LOG_WARN("failed to copy Unified Catalog authorization", K(ret));
  } else if (OB_FAIL(rest_properties.to_string_with_alloc(serialized_properties, *allocator_))) {
    LOG_WARN("failed to serialize REST transport properties", K(ret));
  } else if (OB_ISNULL(client_pool_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("REST client pool manager is null", K(ret));
  } else if (OB_FAIL(client_pool_mgr->get_client(tenant_id_,
                                                 UNIFIED_CATALOG_TRANSPORT_POOL_KEY,
                                                 serialized_properties,
                                                 transport_))) {
    LOG_WARN("failed to acquire Unified Catalog REST transport", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("acquired Unified Catalog REST transport is null", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    config_ = nullptr;
    authorization_.reset();
    cluster_id_ = OB_INVALID_ID;
    tenant_id_ = OB_INVALID_ID;
  }
  return ret;
}

bool ObUnifiedCatalogClient::is_valid_access_token(const ObString &access_token)
{
  bool valid = !access_token.empty();
  for (int64_t i = 0; valid && i < access_token.length(); ++i) {
    const unsigned char ch = static_cast<unsigned char>(access_token.ptr()[i]);
    if (ch < ' ' || '\x7f' == ch) {
      valid = false;
    }
  }
  return valid;
}

int ObUnifiedCatalogClient::request(const ObRestHttpMethod method,
                                    const ObString &path,
                                    const ObString &request_body,
                                    ObIAllocator &response_allocator,
                                    ObString &response_body,
                                    const ConflictPolicy conflict_policy,
                                    const int64_t expected_status,
                                    const ObString &access_delegation,
                                    const bool ob_data_read)
{
  int ret = OB_SUCCESS;
  response_body.reset();
  if (!is_inited_ || OB_ISNULL(transport_) || OB_ISNULL(config_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Unified Catalog client is not initialized", K(ret));
  } else if (request_body.length() > MAX_REQUEST_BODY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Unified Catalog request body exceeds size limit", K(ret), K(path));
  } else {
    ObArenaAllocator request_allocator("UnifiedReq", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObRestHttpRequest http_request(request_allocator);
    ObRestHttpResponse http_response(response_allocator);
    http_request.set_method(method);
    http_request.set_connect_timeout_ms(ObUnifiedCatalogConfig::DEFAULT_CONNECT_TIMEOUT_MS);
    http_request.set_request_timeout_ms(ObUnifiedCatalogConfig::DEFAULT_REQUEST_TIMEOUT_MS);
    http_request.set_follow_redirects(false);
    if (OB_FAIL(http_request.set_base_url(config_->endpoint_))) {
      LOG_WARN("failed to set Unified Catalog request base URL", K(ret));
    } else if (OB_FAIL(http_request.set_url(path))) {
      LOG_WARN("failed to set Unified Catalog request path", K(ret), K(path));
    } else if (OB_FAIL(http_request.add_header(ObString::make_string(AUTHORIZATION_HEADER),
                                               authorization_))) {
      LOG_WARN("failed to add Unified Catalog authorization header", K(ret), K(path));
    } else if (OB_FAIL(http_request.add_header(ObString::make_string(REALM_HEADER),
                                               config_->realm_))) {
      LOG_WARN("failed to add Unified Catalog realm header", K(ret), K(path));
    } else if (!access_delegation.empty()
               && OB_FAIL(http_request.add_header(ObString::make_string(ACCESS_DELEGATION_HEADER),
                                                  access_delegation))) {
      LOG_WARN("failed to add Unified Catalog access delegation header", K(ret), K(path));
    } else if (ob_data_read
               && OB_FAIL(http_request.add_header(ObString::make_string(OB_TABLE_ACCESS_HEADER),
                                                  ObString::make_string(OB_TABLE_ACCESS_DATA)))) {
      LOG_WARN("failed to add Unified Catalog OB table access header", K(ret), K(path));
    } else if (!request_body.empty()
               && (OB_FAIL(http_request.add_header(ObString::make_string(CONTENT_TYPE_HEADER),
                                                   ObString::make_string(JSON_CONTENT_TYPE)))
                   || OB_FAIL(http_request.set_body(request_body)))) {
      LOG_WARN("failed to set Unified Catalog request body", K(ret), K(path));
    } else if (OB_FAIL(transport_->execute(
                   http_request, http_response, MAX_RESPONSE_BODY_SIZE))) {
      const int64_t status_code = http_response.get_status_code();
      if (OB_HTTP_BAD_REQUEST == ret && HTTP_STATUS_CONFLICT == status_code) {
        if (ConflictPolicy::RETURN_ALREADY_EXISTS == conflict_policy
            && HTTP_METHOD_POST == method) {
          ret = OB_ERR_ALREADY_EXISTS;
          LOG_WARN("Unified Catalog create returned HTTP conflict", K(ret), K(path), K(status_code));
        } else if (ConflictPolicy::RETURN_EAGAIN == conflict_policy) {
          ret = OB_EAGAIN;
          LOG_WARN("Unified Catalog operation returned HTTP conflict", K(ret), K(path), K(status_code));
        } else {
          LOG_WARN("Unified Catalog request failed", K(ret), K(path), K(status_code));
        }
      } else {
        LOG_WARN("Unified Catalog request failed", K(ret), K(path), K(status_code));
      }
    } else {
      const ObString successful_body = http_response.get_body();
      if (successful_body.length() > MAX_RESPONSE_BODY_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Unified Catalog response body exceeds size limit", K(ret), K(path));
      } else if (HTTP_STATUS_ANY_SUCCESS != expected_status
          && (expected_status != http_response.get_status_code()
              || (HTTP_STATUS_NO_CONTENT == expected_status && !successful_body.empty()))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("unexpected Unified Catalog response status or body",
                 K(ret),
                 K(path),
                 "status_code",
                 http_response.get_status_code());
      } else if (!successful_body.empty()
                 && OB_FAIL(
                     ob_write_string(response_allocator, successful_body, response_body))) {
        LOG_WARN("failed to copy Unified Catalog response body", K(ret), K(path));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogReader::list_accessible_catalog_names(ObIArray<ObString> &catalog_names)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObUnifiedCatalogIdentity, 8> catalogs;
  catalog_names.reset();
  if (OB_FAIL(list_accessible_catalog_identities(catalogs))) {
    LOG_WARN("failed to list Unified Catalog identities", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < catalogs.count(); ++i) {
    if (OB_FAIL(catalog_names.push_back(catalogs.at(i).name_))) {
      LOG_WARN("failed to add Unified Catalog name", K(ret), K(i));
    }
  }
  return ret;
}

int ObUnifiedCatalogReader::list_accessible_catalog_identities(
    ObIArray<ObUnifiedCatalogIdentity> &catalogs)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString response_body;
  catalogs.reset();
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_catalog_collection_path(path))) {
    LOG_WARN("failed to build Unified Catalog list path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to list Unified Catalogs", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::decode_catalog_identities(*client_.allocator_,
                                                                               client_.tenant_id_,
                                                                               response_body,
                                                                               catalogs))) {
    LOG_WARN("failed to decode Unified Catalog list", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::list_namespace_names(ObIArray<ObString> &namespace_names)
{
  int ret = OB_SUCCESS;
  ObString page_token;
  namespace_names.reset();
  if (client_.catalog_name_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("Unified Catalog is not selected", K(ret));
  } else {
    do {
      ObSqlString path;
      ObArenaAllocator response_allocator("UCNsPage",
                                          OB_MALLOC_NORMAL_BLOCK_SIZE,
                                          client_.tenant_id_);
      ObString response_body;
      const ObString previous_token = page_token;
      if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_namespace_collection_page_path(client_.catalog_name_,
                                                                                      page_token,
                                                                                      path))) {
        LOG_WARN("failed to build Unified Catalog namespace list path", K(ret));
      } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                         path.string(),
                                         ObString(),
                                         response_allocator,
                                         response_body,
                                         ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                         ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
        LOG_WARN("failed to list Unified Catalog namespaces", K(ret));
      } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::decode_namespace_names_page(
                     *client_.allocator_,
                     client_.tenant_id_,
                     response_body,
                     namespace_names,
                     page_token))) {
        LOG_WARN("failed to decode Unified Catalog namespace page", K(ret));
      } else if (!page_token.empty() && page_token == previous_token) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("Unified Catalog namespace page token did not advance", K(ret));
      }
    } while (OB_SUCC(ret) && !page_token.empty());
  }
  return ret;
}

int ObUnifiedCatalogReader::list_ob_table_names(const ObString &database_name,
                                                ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  ObString page_token;
  table_names.reset();
  if (client_.catalog_name_.empty() || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog Table namespace", K(ret));
  } else {
    do {
      ObSqlString path;
      ObArenaAllocator response_allocator("UCTblPage",
                                          OB_MALLOC_NORMAL_BLOCK_SIZE,
                                          client_.tenant_id_);
      ObString response_body;
      const ObString previous_token = page_token;
      if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_collection_page_path(
              client_.catalog_name_, database_name, page_token, path))) {
        LOG_WARN("failed to build Unified Catalog Table list path", K(ret));
      } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                         path.string(),
                                         ObString(),
                                         response_allocator,
                                         response_body,
                                         ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                         ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
        LOG_WARN("failed to list Unified Catalog Tables", K(ret));
      } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::decode_ob_table_names_page(
                     *client_.allocator_, client_.tenant_id_, response_body,
                     table_names, page_token))) {
        LOG_WARN("failed to decode OceanBase Table page", K(ret));
      } else if (!page_token.empty() && page_token == previous_token) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("Unified Catalog Table page token did not advance", K(ret));
      }
    } while (OB_SUCC(ret) && !page_token.empty());
  }
  return ret;
}

int ObUnifiedCatalogReader::load_namespace(const ObString &database_name,
                                           ObIAllocator &response_allocator,
                                           ObString &response_body)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  response_body.reset();
  if (client_.catalog_name_.empty() || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog namespace identity", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_namespace_path(client_.catalog_name_,
                                                                         database_name,
                                                                         path))) {
    LOG_WARN("failed to build Unified Catalog namespace path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to load Unified Catalog namespace", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_iceberg_table(const ObString &database_name,
                                               const ObString &table_name,
                                               ObIAllocator &response_allocator,
                                               ObString &response_body)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  response_body.reset();
  if (client_.catalog_name_.empty() || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog Table identity", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_iceberg_table_path(
                 client_.catalog_name_, database_name, table_name, path))) {
    LOG_WARN("failed to build Unified Catalog Table path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to load Unified Catalog Table", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_iceberg_query_table(const ObString &database_name,
                                                     const ObString &table_name,
                                                     ObIAllocator &response_allocator,
                                                     ObString &response_body)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  response_body.reset();
  if (client_.catalog_name_.empty() || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog Iceberg table identity", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_iceberg_table_path(client_.catalog_name_,
                                                                             database_name,
                                                                             table_name,
                                                                             path))) {
    LOG_WARN("failed to build Unified Catalog Iceberg table path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK,
                                     ObString::make_string(ObUnifiedCatalogClient::VENDED_CREDENTIALS)))) {
    LOG_WARN("failed to load Unified Catalog Iceberg query table", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_ob_table(const ObString &database_name,
                                          const ObString &table_name,
                                          ObIAllocator &response_allocator,
                                          ObString &response_body,
                                          const bool for_data_read)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  response_body.reset();
  if (client_.catalog_name_.empty() || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog Table identity", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_path(
                 client_.catalog_name_, database_name, table_name, path))) {
    LOG_WARN("failed to build Unified Catalog Table path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK,
                                     ObString(),
                                     for_data_read))) {
    LOG_WARN("failed to load Unified Catalog Table", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_part_values(const ObString &database_name,
                                             const ObString &table_name,
                                             const ObString &page_token,
                                             ObIAllocator &response_allocator,
                                             ObString &response_body)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_part_values_path(client_.catalog_name_,
                                                                    database_name,
                                                                    table_name,
                                                                    page_token,
                                                                    path))) {
    LOG_WARN("failed to build Unified Catalog partition values path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to load Unified Catalog partition values", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::catalog_exists(bool &exists)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCCatalog",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString response_body;
  exists = false;
  if (client_.catalog_name_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("Unified Catalog is not selected", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_catalog_path(client_.catalog_name_, path))) {
    LOG_WARN("failed to build Unified Catalog path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      // Only the by-name GET's 404 means absence; authorization failures remain errors.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check Unified Catalog existence", K(ret), "catalog_name_", client_.catalog_name_);
    }
  } else {
    exists = true;
  }
  return ret;
}

int ObUnifiedCatalogReader::get_catalog_info(ObUnifiedCatalogInfo &catalog_info)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCCatalog",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString response_body;
  catalog_info = ObUnifiedCatalogInfo();
  if (client_.catalog_name_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("Unified Catalog is not selected", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_catalog_path(client_.catalog_name_, path))) {
    LOG_WARN("failed to build Unified Catalog path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::UNCHANGED,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to load Unified Catalog", K(ret), "catalog_name_", client_.catalog_name_);
  } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::decode_catalog_info(response_allocator,
                                                                         response_body,
                                                                         client_.catalog_name_,
                                                                         catalog_info))) {
    LOG_WARN("failed to decode Unified Catalog source",
             K(ret),
             "catalog_name_",
             client_.catalog_name_);
  }
  return ret;
}

int ObUnifiedCatalogReader::validate_catalog()
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_catalog_path(client_.catalog_name_, path))) {
    LOG_WARN("failed to build Unified Catalog path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body))) {
    LOG_WARN("failed to load Unified Catalog", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::validate_catalog_response(response_allocator,
                                                                    response_body,
                                                                    client_.catalog_name_,
                                                                    client_.cluster_id_,
                                                                    client_.tenant_id_))) {
    LOG_WARN("failed to validate Unified Catalog", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_and_validate_namespace_if_exists(const ObString &database_name,
                                                                  const uint64_t database_id,
                                                                  bool &exists)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  exists = false;
  if (OB_INVALID_ID == database_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected Unified Catalog namespace identity",
             K(ret),
             K(database_name),
             K(database_id));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_namespace_path(client_.catalog_name_,
                                                                         database_name,
                                                                         path))) {
    LOG_WARN("failed to build Unified Catalog namespace path", K(ret), K(database_name));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to load Unified Catalog namespace", K(ret), K(database_name));
    }
  } else if (OB_FAIL(
                 ObUnifiedCatalogNamespaceCodec::validate_namespace_response(response_body,
                                                                             database_name,
                                                                             client_.cluster_id_,
                                                                             client_.tenant_id_,
                                                                             database_id))) {
    LOG_WARN("Unified Catalog namespace metadata mismatch", K(ret), K(database_name));
  } else {
    exists = true;
  }
  return ret;
}

int ObUnifiedCatalogReader::load_and_validate_namespace(const ObString &database_name,
                                                        const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_FAIL(load_and_validate_namespace_if_exists(database_name, database_id, exists))) {
    LOG_WARN("failed to load Unified Catalog namespace", K(ret), K(database_name));
  } else if (!exists) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("conflicting Unified Catalog namespace disappeared before validation",
             K(ret),
             K(database_name));
  }
  return ret;
}

int ObUnifiedCatalogReader::namespace_has_tables(const ObString &database_name,
                                                 const uint64_t database_id,
                                                 bool &has_tables)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  bool namespace_exists = false;
  has_tables = false;
  if (OB_FAIL(
          load_and_validate_namespace_if_exists(database_name, database_id, namespace_exists))) {
    LOG_WARN("failed to validate Unified Catalog namespace before listing tables",
             K(ret),
             K(database_name),
             K(database_id));
  } else if (namespace_exists) {
    if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_collection_path(client_.catalog_name_,
                                                                              database_name,
                                                                              path))) {
      LOG_WARN("failed to build Unified Catalog OceanBase Table collection path", K(ret), K(database_name));
    } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                       path.string(),
                                       ObString(),
                                       response_allocator,
                                       response_body))) {
      if (OB_HTTP_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to list Unified Catalog OceanBase Tables", K(ret), K(database_name));
      }
    } else if (OB_FAIL(ObUnifiedCatalogNamespaceCodec::decode_namespace_table_presence(
                   response_allocator,
                   response_body,
                   has_tables))) {
      LOG_WARN("failed to read Unified Catalog namespace table list", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogReader::load_and_validate_table(const ObString &database_name,
                                                    const uint64_t database_id,
                                                    const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_path(client_.catalog_name_,
                                                                 database_name,
                                                                 table_schema.get_table_name_str(),
                                                                 path))
      || OB_FAIL(client_.request(HTTP_METHOD_GET,
                                 path.string(),
                                 ObString(),
                                 response_allocator,
                                 response_body))) {
    LOG_WARN("failed to load Unified Catalog OceanBase Table",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_load_table_response(response_body,
                                                                               client_.cluster_id_,
                                                                               client_.tenant_id_,
                                                                               database_id,
                                                                               table_schema,
                                                                               response_allocator))) {
    LOG_WARN("Unified Catalog OceanBase Table metadata mismatch",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()));
  }
  return ret;
}

int ObUnifiedCatalogReader::load_and_validate_table_source_if_exists(const ObString &database_name,
                                                                     const uint64_t database_id,
                                                                     const ObString &table_name,
                                                                     const uint64_t table_id,
                                                                     bool &exists,
                                                                     int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  exists = false;
  schema_version = 0;
  if (OB_INVALID_ID == database_id || OB_INVALID_ID == table_id || database_name.empty()
      || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected Unified Catalog table source identity",
             K(ret),
             K(database_name),
             K(database_id),
             K(table_name),
             K(table_id));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_path(client_.catalog_name_,
                                                                        database_name,
                                                                        table_name,
                                                                        path))) {
    LOG_WARN("failed to build Unified Catalog OceanBase Table path", K(ret), K(table_name));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_GET,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to load Unified Catalog OceanBase Table", K(ret), K(table_name));
    }
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_source(response_body,
                                                                        client_.cluster_id_,
                                                                        client_.tenant_id_,
                                                                        database_id,
                                                                        table_name,
                                                                        table_id,
                                                                        response_allocator,
                                                                        schema_version))) {
    LOG_WARN("Unified Catalog OceanBase Table source metadata mismatch",
             K(ret),
             K(database_name),
             K(database_id),
             K(table_name),
             K(table_id));
  } else {
    exists = true;
  }
  return ret;
}

int ObUnifiedCatalogReader::load_preparing_operation_id(const ObString &database_name,
                                                        const ObString &table_name,
                                                        ObIAllocator &response_allocator,
                                                        bool &preparing,
                                                        ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObString response_body;
  preparing = false;
  operation_id.reset();
  if (OB_FAIL(load_ob_table(database_name, table_name, response_allocator, response_body))) {
    LOG_WARN("failed to load Unified Catalog create reservation", K(ret), K(table_name));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::decode_create_state(response_body,
                                                                      table_name,
                                                                      response_allocator,
                                                                      preparing,
                                                                      operation_id))) {
    LOG_WARN("failed to decode Unified Catalog create reservation", K(ret), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogWriter::create_namespace(const ObString &database_name, const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObString request_body;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  bool exists = false;
  if (OB_FAIL(reader_.load_and_validate_namespace_if_exists(database_name, database_id, exists))) {
    LOG_WARN("failed to validate Unified Catalog namespace before create", K(ret), K(database_name));
  } else if (!exists) {
    if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_namespace_collection_path(client_.catalog_name_, path))
        || OB_FAIL(ObUnifiedCatalogNamespaceCodec::encode_namespace_request(*client_.allocator_,
                                                                            database_name,
                                                                            client_.cluster_id_,
                                                                            client_.tenant_id_,
                                                                            database_id,
                                                                            request_body))) {
      LOG_WARN("failed to build Unified Catalog namespace create request", K(ret));
    } else if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                       path.string(),
                                       request_body,
                                       response_allocator,
                                       response_body,
                                       ObUnifiedCatalogClient::ConflictPolicy::RETURN_ALREADY_EXISTS))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        if (OB_FAIL(reader_.load_and_validate_namespace(database_name, database_id))) {
          LOG_WARN("failed to validate existing Unified Catalog namespace", K(ret), K(database_name));
        }
      } else {
        LOG_WARN("failed to create Unified Catalog namespace", K(ret), K(database_name));
      }
    } else if (OB_FAIL(
                   ObUnifiedCatalogNamespaceCodec::validate_namespace_response(response_body,
                                                                               database_name,
                                                                               client_.cluster_id_,
                                                                               client_.tenant_id_,
                                                                               database_id))) {
      LOG_WARN("created Unified Catalog namespace does not match source", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::drop_namespace(const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_namespace_path(client_.catalog_name_,
                                                                  database_name,
                                                                  path))) {
    LOG_WARN("failed to build Unified Catalog namespace path", K(ret), K(database_name));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_DELETE,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to drop Unified Catalog namespace", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::reserve_table(const ObString &database_name,
                                          const uint64_t database_id,
                                          const ObString &table_name,
                                          const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator request_allocator("UCPrepare",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     client_.tenant_id_);
  ObArenaAllocator response_allocator("UCPrepare",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString request_body;
  ObString response_body;
  ObString returned_operation_id;
  bool preparing = false;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_reservation_path(client_.catalog_name_,
                                                                       database_name,
                                                                       table_name,
                                                                       path))
      || OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_prepare_table_request(operation_id,
                                                                           client_.cluster_id_,
                                                                           client_.tenant_id_,
                                                                           database_id,
                                                                           request_allocator,
                                                                           request_body))) {
    LOG_WARN("failed to build Unified Catalog create reservation", K(ret), K(table_name));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                     path.string(),
                                     request_body,
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::RETURN_ALREADY_EXISTS,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to reserve Unified Catalog table name", K(ret), K(database_name), K(table_name));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::decode_create_state(response_body,
                                                                      table_name,
                                                                      response_allocator,
                                                                      preparing,
                                                                      returned_operation_id))) {
    LOG_WARN("failed to validate Unified Catalog create reservation", K(ret), K(table_name));
  } else if (!preparing || returned_operation_id != operation_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog create reservation does not match", K(ret), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogWriter::complete_reserved_table(const ObString &database_name,
                                                    const uint64_t database_id,
                                                    const schema::ObTableSchema &table_schema,
                                                    const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator request_allocator("UCComplete",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     client_.tenant_id_);
  ObArenaAllocator response_allocator("UCComplete",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString metadata;
  ObString request_body;
  ObString response_body;
  int64_t sync_id = 0;
  if (operation_id.empty() || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog create completion identity", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_table_metadata(client_.cluster_id_,
                                                                        client_.tenant_id_,
                                                                        database_id,
                                                                        table_schema,
                                                                        request_allocator,
                                                                        metadata))) {
    LOG_WARN("failed to encode Unified Catalog completed table", K(ret));
  } else if (table_schema.is_partitioned_table()) {
    ObUnifiedCatalogPartitionSync partition_sync(client_);
    if (OB_FAIL(partition_sync.complete_reserved_partitions(database_name,
                                                            database_id,
                                                            table_schema,
                                                            operation_id,
                                                            sync_id))) {
      LOG_WARN("failed to commit Unified Catalog created table partitions", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_complete_path(client_.catalog_name_,
                                                                      database_name,
                                                                      table_schema.get_table_name_str(),
                                                                      path))
        || OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_complete_table_request(operation_id,
                                                                              metadata,
                                                                              sync_id,
                                                                              request_allocator,
                                                                              request_body))) {
      LOG_WARN("failed to build Unified Catalog create completion", K(ret));
    } else if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                       path.string(),
                                       request_body,
                                       response_allocator,
                                       response_body,
                                       ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                       ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
      LOG_WARN("failed to complete Unified Catalog table creation", K(ret));
    } else if (OB_FAIL(reader_.load_and_validate_table(database_name, database_id, table_schema))) {
      LOG_WARN("completed Unified Catalog table does not match OceanBase", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::cancel_reserved_table(const ObString &database_name,
                                                  const ObString &table_name,
                                                  const ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCCancel", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString response_body;
  if (database_name.empty() || table_name.empty() || operation_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog reservation cancellation identity", K(ret),
             K(database_name), K(table_name));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_cancel_path(client_.catalog_name_,
                                                                         database_name,
                                                                         table_name,
                                                                         operation_id,
                                                                         path))) {
    LOG_WARN("failed to build Unified Catalog reservation cancellation request", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_DELETE,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                     ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to cancel Unified Catalog table reservation", K(ret),
               K(database_name), K(table_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::sync_active_table(const ObString &database_name,
                                              const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_active_table(database_name,
                                 table_schema,
                                 TableWriteMode::SYNC,
                                 table_schema.get_table_id()))) {
    LOG_WARN("failed to synchronize active Unified Catalog table",
             K(ret), K(database_name), K(table_schema.get_table_name_str()));
  }
  return ret;
}

int ObUnifiedCatalogWriter::update_active_table(const ObString &database_name,
                                                const schema::ObTableSchema &table_schema,
                                                const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == expected_source_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog active table update source",
             K(ret), K(database_name), K(expected_source_table_id));
  } else if (OB_FAIL(write_active_table(database_name,
                                        table_schema,
                                        TableWriteMode::ALTER,
                                        expected_source_table_id))) {
    LOG_WARN("failed to update active Unified Catalog table",
             K(ret), K(database_name), K(table_schema.get_table_name_str()),
             K(expected_source_table_id));
  }
  return ret;
}

int ObUnifiedCatalogWriter::write_active_table(const ObString &database_name,
                                               const schema::ObTableSchema &table_schema,
                                               const TableWriteMode mode,
                                               const uint64_t source_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t database_id = table_schema.get_database_id();
  const bool is_alter = TableWriteMode::ALTER == mode;
  bool exists = false;
  int64_t remote_schema_version = 0;
  ObSqlString path;
  ObArenaAllocator request_allocator("UCUpdate",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     client_.tenant_id_);
  ObArenaAllocator response_allocator("UCUpdate",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString metadata;
  ObString request_body;
  ObString response_body;
  bool already_updated = false;
  if (is_alter) {
    if (OB_FAIL(reader_.load_and_validate_table(database_name, database_id, table_schema))) {
      if (OB_STATE_NOT_MATCH == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check completed OceanBase table update",
                 K(ret),
                 K(database_name),
                 K(table_schema.get_table_name_str()));
      }
    } else {
      already_updated = true;
    }
  }
  if (OB_SUCC(ret) && !already_updated) {
    if (OB_FAIL(reader_.load_and_validate_table_source_if_exists(database_name,
                                                                 database_id,
                                                                 table_schema.get_table_name_str(),
                                                                 source_table_id,
                                                                 exists,
                                                                 remote_schema_version))) {
      LOG_WARN("failed to validate existing OceanBase table source", K(ret));
    } else if (!exists) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("conflicting Unified Catalog table disappeared", K(ret));
    } else if (remote_schema_version > table_schema.get_schema_version()) {
      ret = OB_EAGAIN;
      LOG_WARN("UC table schema is newer than OceanBase",
               K(ret),
               K(remote_schema_version),
               "local_schema_version",
               table_schema.get_schema_version());
    } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_table_metadata(client_.cluster_id_,
                                                                          client_.tenant_id_,
                                                                          database_id,
                                                                          table_schema,
                                                                          request_allocator,
                                                                          metadata))) {
      LOG_WARN("failed to encode OceanBase table update", K(ret));
    } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_update_table_request(
                   metadata,
                   is_alter ? source_table_id : OB_INVALID_ID,
                   request_allocator,
                   request_body))) {
      LOG_WARN("failed to build OceanBase table update", K(ret));
    } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_path(
                   client_.catalog_name_,
                   database_name,
                   table_schema.get_table_name_str(),
                   path))) {
      LOG_WARN("failed to build OceanBase table path", K(ret));
    } else if (OB_FAIL(client_.request(HTTP_METHOD_PUT,
                                       path.string(),
                                       request_body,
                                       response_allocator,
                                       response_body,
                                       ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                       ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
      LOG_WARN("failed to update OceanBase table metadata", K(ret));
    } else if (OB_FAIL(reader_.load_and_validate_table(database_name, database_id, table_schema))) {
      LOG_WARN("updated OceanBase table does not match source", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::sync_table(const ObString &database_name,
                                       const uint64_t database_id,
                                       const schema::ObTableSchema &table_schema,
                                       const bool recover_preparing)
{
  int ret = OB_SUCCESS;
  ObSqlString operation_id;
  ObArenaAllocator response_allocator("UCSync",
                                      OB_MALLOC_NORMAL_BLOCK_SIZE,
                                      client_.tenant_id_);
  ObString existing_operation_id;
  bool preparing = false;
  bool table_exists = false;
  const bool partitioned = table_schema.is_partitioned_table();
  if (OB_FAIL(reader_.load_preparing_operation_id(database_name,
                                                  table_schema.get_table_name_str(),
                                                  response_allocator,
                                                  preparing,
                                                  existing_operation_id))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to read existing OceanBase table state", K(ret));
    }
  } else {
    table_exists = true;
  }
  if (OB_SUCC(ret) && !table_exists) {
    if (OB_FAIL(operation_id.append_fmt("sync-%lu-%ld",
                                        table_schema.get_table_id(),
                                        table_schema.get_schema_version()))) {
      LOG_WARN("failed to build OceanBase table sync operation ID", K(ret));
    } else if (OB_FAIL(reserve_table(database_name,
                                     database_id,
                                     table_schema.get_table_name_str(),
                                     operation_id.string()))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        if (OB_FAIL(reader_.load_preparing_operation_id(database_name,
                                                        table_schema.get_table_name_str(),
                                                        response_allocator,
                                                        preparing,
                                                        existing_operation_id))) {
          LOG_WARN("failed to read existing OceanBase table state", K(ret));
        } else {
          table_exists = true;
        }
      } else {
        LOG_WARN("failed to reserve OceanBase table during sync",
                 K(ret), K(database_name), K(table_schema.get_table_name_str()));
      }
    } else if (OB_FAIL(complete_reserved_table(database_name,
                                               database_id,
                                               table_schema,
                                               operation_id.string()))) {
      LOG_WARN("failed to complete OceanBase table during sync", K(ret));
    }
  }
  if (OB_SUCC(ret) && table_exists) {
    if (preparing) {
      if (recover_preparing) {
        if (OB_FAIL(complete_reserved_table(database_name,
                                            database_id,
                                            table_schema,
                                            existing_operation_id))) {
          LOG_WARN("failed to recover OceanBase table reservation", K(ret));
        }
      } else {
        ret = OB_EAGAIN;
        LOG_WARN("OceanBase table reservation is still preparing", K(ret));
      }
    } else if (partitioned) {
      bool exists = false;
      int64_t remote_schema_version = 0;
      if (OB_FAIL(
              reader_.load_and_validate_table_source_if_exists(database_name,
                                                               database_id,
                                                               table_schema.get_table_name_str(),
                                                               table_schema.get_table_id(),
                                                               exists,
                                                               remote_schema_version))) {
        LOG_WARN("failed to validate existing OceanBase table source", K(ret));
      } else if (!exists) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("conflicting Unified Catalog table disappeared", K(ret));
      } else if (remote_schema_version > table_schema.get_schema_version()) {
        ret = OB_EAGAIN;
        LOG_WARN("UC partition schema is newer than OceanBase",
                 K(ret),
                 K(remote_schema_version),
                 "local_schema_version",
                 table_schema.get_schema_version());
      } else {
        ObUnifiedCatalogPartitionSync partition_sync(client_);
        if (OB_FAIL(partition_sync.sync_existing_partitions(
                database_name, database_id, table_schema))) {
          LOG_WARN("failed to synchronize existing OceanBase partitions", K(ret));
        }
      }
    } else if (OB_FAIL(sync_active_table(database_name, table_schema))) {
      LOG_WARN("failed to sync existing OceanBase table",
               K(ret), K(database_name), K(table_schema.get_table_name_str()));
    }
  }
  return ret;
}

int ObUnifiedCatalogWriter::update_table(const ObString &database_name,
                                         const schema::ObTableSchema &table_schema,
                                         const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t database_id = table_schema.get_database_id();
  if (database_name.empty() || OB_INVALID_ID == database_id
      || OB_INVALID_ID == expected_source_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog table update argument",
             K(ret),
             K(database_name),
             K(database_id),
             K(expected_source_table_id));
  } else if (table_schema.is_partitioned_table()) {
    ObUnifiedCatalogPartitionSync partition_sync(client_);
    if (OB_FAIL(partition_sync.update_partitions(database_name,
                                                 database_id,
                                                 table_schema,
                                                 expected_source_table_id))) {
      LOG_WARN("failed to update Unified Catalog partitioned table",
               K(ret),
               K(database_name),
               K(table_schema.get_table_name_str()),
               K(expected_source_table_id));
    }
  } else if (OB_FAIL(update_active_table(database_name,
                                         table_schema,
                                         expected_source_table_id))) {
    LOG_WARN("failed to update Unified Catalog table",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()),
             K(expected_source_table_id));
  }
  return ret;
}

int ObUnifiedCatalogWriter::rename_table(const ObString &source_database_name,
                                         const ObString &source_table_name,
                                         const ObString &destination_database_name,
                                         const schema::ObTableSchema &final_table_schema,
                                         const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t destination_database_id = final_table_schema.get_database_id();
  ObSqlString path;
  ObArenaAllocator request_allocator("UCRename", OB_MALLOC_NORMAL_BLOCK_SIZE, client_.tenant_id_);
  ObArenaAllocator response_allocator("UCRename", OB_MALLOC_NORMAL_BLOCK_SIZE, client_.tenant_id_);
  ObString metadata;
  ObString request_body;
  ObString response_body;
  if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_table_metadata(client_.cluster_id_,
                                                                 client_.tenant_id_,
                                                                 destination_database_id,
                                                                 final_table_schema,
                                                                 request_allocator,
                                                                 metadata))) {
    LOG_WARN("failed to encode renamed OceanBase table", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_rename_table_request(
                 destination_database_name,
                 final_table_schema.get_table_name_str(),
                 expected_source_table_id,
                 metadata,
                 request_allocator,
                 request_body))) {
    LOG_WARN("failed to build OceanBase table rename", K(ret));
  } else if (OB_FAIL(
                 ObUnifiedCatalogResourcePaths::build_ob_table_rename_path(client_.catalog_name_,
                                                                           source_database_name,
                                                                           source_table_name,
                                                                           path))) {
    LOG_WARN("failed to build OceanBase table rename path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                     path.string(),
                                     request_body,
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                     ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
    LOG_WARN("failed to rename Unified Catalog OceanBase table",
             K(ret),
             K(source_database_name),
             K(source_table_name),
             K(destination_database_name),
             K(final_table_schema.get_table_name_str()));
  } else if (OB_FAIL(reader_.load_and_validate_table(destination_database_name,
                                                     destination_database_id,
                                                     final_table_schema))) {
    LOG_WARN("renamed Unified Catalog OceanBase table does not match source",
             K(ret),
             K(destination_database_name),
             K(final_table_schema.get_table_name_str()));
  }
  return ret;
}

int ObUnifiedCatalogWriter::drop_table(const ObString &database_name, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCResp");
  ObString response_body;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_ob_table_path(client_.catalog_name_,
                                                                 database_name,
                                                                 table_name,
                                                                 path))) {
    LOG_WARN("failed to build Unified Catalog OceanBase Table path", K(ret), K(table_name));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_DELETE,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to drop Unified Catalog OceanBase Table", K(ret), K(database_name), K(table_name));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

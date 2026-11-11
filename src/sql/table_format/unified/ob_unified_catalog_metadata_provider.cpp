/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include "sql/table_format/unified/ob_unified_catalog_metadata_provider.h"

#include "lib/charset/ob_charset.h"
#include "lib/restore/ob_storage.h"
#include "share/catalog/ob_unified_catalog_config.h"
#include "share/catalog/ob_unified_catalog_namespace_codec.h"
#include "share/catalog/ob_unified_catalog_client.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_table_schema.h"
#include "share/catalog/rest/responses/ob_rest_http_response.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/unified/ob_unified_catalog_schema_converter.h"
#include "sql/table_format/unified/ob_unified_catalog_table_codec.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

int ObUnifiedCatalogMetadataProvider::init_client(const uint64_t tenant_id,
                                                  const ObString &access_token,
                                                  const ObString &remote_catalog_name,
                                                  ObIAllocator &allocator,
                                                  ObUnifiedCatalogConfig &config,
                                                  ObUnifiedCatalogClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(config.load(tenant_id, allocator))) {
    LOG_WARN("failed to load UC connection config", K(ret));
  } else if (OB_FAIL(client.init(config, access_token, remote_catalog_name,
                                 static_cast<uint64_t>(GCONF.cluster_id), tenant_id))) {
    LOG_WARN("failed to initialize Unified Catalog metadata client", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::list_namespace_names(const uint64_t tenant_id,
                                                           const ObString &access_token,
                                                           const ObString &remote_catalog_name,
                                                           ObIAllocator &allocator,
                                                           ObIArray<ObString> &namespace_names)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().list_namespace_names(namespace_names))) {
    LOG_WARN("failed to list UC namespace names", K(ret), K(remote_catalog_name));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::list_table_names(const uint64_t tenant_id,
                                                       const ObString &access_token,
                                                       const ObString &remote_catalog_name,
                                                       const ObString &database_name,
                                                       const ObNameCaseMode case_mode,
                                                       ObIAllocator &allocator,
                                                       ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().list_ob_table_names(database_name, table_names))) {
    LOG_WARN("failed to list UC Tables", K(ret), K(remote_catalog_name), K(database_name));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_names.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      if (ObCharset::case_mode_equal(case_mode, table_names.at(i), table_names.at(j))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("ambiguous UC table name",
                 K(ret),
                 K(remote_catalog_name),
                 K(database_name),
                 K(table_names.at(i)));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::fetch_database_schema(const uint64_t tenant_id,
                                                            const ObString &access_token,
                                                            const uint64_t child_catalog_id,
                                                            const ObString &remote_catalog_name,
                                                            const ObString &database_name,
                                                            const uint64_t database_id,
                                                            const ObNameCaseMode case_mode,
                                                            ObIAllocator &allocator,
                                                            ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObString response_body;
  database_schema = NULL;
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().load_namespace(database_name, allocator, response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("UC namespace does not exist", K(ret), K(remote_catalog_name), K(database_name));
    } else {
      LOG_WARN("failed to load Unified Catalog namespace", K(ret), K(database_name));
    }
  } else if (OB_FAIL(
                 ObUnifiedCatalogNamespaceCodec::validate_read_namespace_response(tenant_id,
                                                                                  response_body,
                                                                                  database_name))) {
    LOG_WARN("failed to decode Unified Catalog namespace", K(ret), K(database_name));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaConverter::build_database_schema(allocator,
                                                                            database_name,
                                                                            tenant_id,
                                                                            child_catalog_id,
                                                                            database_id,
                                                                            case_mode,
                                                                            database_schema))) {
    LOG_WARN("failed to convert UC database schema", K(ret), K(database_name));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::check_table_read_access(
    const uint64_t tenant_id,
    const ObString &access_token,
    const ObString &remote_catalog_name,
    const ObString &database_name,
    const ObString &table_name,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObString response_body;
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog query client", K(ret));
  } else if (OB_FAIL(client.reader().load_ob_table(
                 database_name, table_name, allocator, response_body, true))) {
    LOG_WARN("failed to check UC table read access", K(ret), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::classify_query_table(
    const uint64_t tenant_id,
    const ObString &access_token,
    const ObString &remote_catalog_name,
    const ObString &database_name,
    const ObString &table_name,
    ObIAllocator &allocator,
    ObUnifiedCatalogQueryTableKind &query_table_kind)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObString response_body;
  if (OB_FAIL(
          init_client(tenant_id, access_token, remote_catalog_name, allocator, config, client))) {
    LOG_WARN("failed to open UC query table source client",
             K(ret),
             K(tenant_id),
             K(remote_catalog_name));
  } else if (OB_FAIL(
                 client.reader()
                     .load_ob_table(database_name, table_name, allocator, response_body, true))) {
    LOG_WARN("failed to load UC query table source",
             K(ret),
             K(tenant_id),
             K(remote_catalog_name),
             K(database_name),
             K(table_name));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::decode_query_table_kind(response_body,
                                                                          GCONF.cluster_id,
                                                                          tenant_id,
                                                                          table_name,
                                                                          allocator,
                                                                          query_table_kind))) {
    LOG_WARN("failed to decode UC query table source",
             K(ret),
             K(tenant_id),
             K(remote_catalog_name),
             K(database_name),
             K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::fetch_table_schema(const uint64_t tenant_id,
                                                         const ObString &access_token,
                                                         const uint64_t child_catalog_id,
                                                         const ObString &remote_catalog_name,
                                                         const ObString &database_name,
                                                         const ObString &table_name,
                                                         const uint64_t database_id,
                                                         const uint64_t table_id,
                                                         const ObNameCaseMode case_mode,
                                                         ObIAllocator &allocator,
                                                         ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObString response_body;
  bool is_ob_table = true;
  table_schema = NULL;
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog metadata client", K(ret));
  } else if (OB_FAIL(client.reader().load_ob_table(
                 database_name, table_name, allocator, response_body))) {
    if (OB_HTTP_NOT_FOUND != ret) {
      LOG_WARN("failed to load UC Table", K(ret), K(table_name));
    }
  } else if (OB_FAIL(build_ob_table_schema(tenant_id,
                                           child_catalog_id,
                                           table_name,
                                           database_id,
                                           table_id,
                                           response_body,
                                           allocator,
                                           table_schema,
                                           is_ob_table))) {
    LOG_WARN("failed to build UC OceanBase Table schema", K(ret), K(table_name));
  } else if (!is_ob_table
             && OB_FAIL(fetch_iceberg_table_schema(tenant_id,
                                                   child_catalog_id,
                                                   database_name,
                                                   table_name,
                                                   database_id,
                                                   table_id,
                                                   case_mode,
                                                   allocator,
                                                   client.reader(),
                                                   table_schema))) {
    LOG_WARN("failed to fetch UC Iceberg Table schema", K(ret), K(remote_catalog_name), K(table_name));
  }
  if (OB_HTTP_NOT_FOUND == ret) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("UC table does not exist", K(ret), K(child_catalog_id), K(database_name), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::build_ob_table_schema(
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const ObString &table_name,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObString &response_body,
    ObIAllocator &allocator,
    ObTableSchema *&table_schema,
    bool &is_ob_table)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogObTableMetadata metadata;
  table_schema = NULL;
  if (OB_FAIL(ObUnifiedCatalogSchemaCodec::try_decode_ob_table_metadata(response_body,
                                                                        table_name,
                                                                        allocator,
                                                                        metadata,
                                                                        is_ob_table))) {
    LOG_WARN("failed to decode UC OB table metadata", K(ret), K(table_name));
  } else if (is_ob_table
             && OB_FAIL(ObUnifiedCatalogSchemaConverter::build_ob_table_schema(allocator,
                                                                               metadata,
                                                                               tenant_id,
                                                                               child_catalog_id,
                                                                               database_id,
                                                                               table_id,
                                                                               table_schema))) {
    LOG_WARN("failed to convert UC OB table schema", K(ret), K(table_name));
  }
  return ret;
}
int ObUnifiedCatalogMetadataProvider::fetch_iceberg_table_schema(
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObNameCaseMode case_mode,
    ObIAllocator &allocator,
    ObUnifiedCatalogReader &reader,
    ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  ObString response_body;
  if (OB_FAIL(reader.load_iceberg_table(database_name, table_name, allocator, response_body))) {
    if (OB_HTTP_NOT_FOUND != ret) {
      LOG_WARN("failed to load UC Iceberg Table", K(ret), K(table_name));
    }
  } else {
    ObRestLoadTableResponse iceberg_response(allocator);
    if (OB_FAIL(iceberg_response.body_.assign(response_body))) {
      LOG_WARN("failed to copy UC Iceberg response body", K(ret));
    } else if (OB_FAIL(iceberg_response.parse_from_response())) {
      LOG_WARN("failed to decode UC Iceberg table metadata", K(ret), K(table_name));
    } else if (OB_FAIL(ObUnifiedCatalogSchemaConverter::build_iceberg_table_schema(
                   allocator, iceberg_response, database_name, table_name, tenant_id,
                   child_catalog_id, database_id, table_id, case_mode, table_schema))) {
      LOG_WARN("failed to convert UC Iceberg table schema", K(ret), K(table_name));
    }
  }
  return ret;
}
int ObUnifiedCatalogMetadataProvider::fetch_query_table_schema(
    const uint64_t tenant_id,
    const ObString &access_token,
    const uint64_t child_catalog_id,
    const ObString &remote_catalog_name,
    const ObString &database_name,
    const ObString &table_name,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObNameCaseMode case_mode,
    ObIAllocator &allocator,
    ObTableSchema *&table_schema,
    iceberg::ObIcebergTableMetadata *&iceberg_metadata)
{
  int ret = OB_SUCCESS;
  ObUnifiedCatalogConfig config;
  ObUnifiedCatalogClient client(allocator);
  ObUnifiedCatalogInfo catalog_info;
  bool is_ob_table = true;
  table_schema = NULL;
  iceberg_metadata = NULL;
  if (OB_FAIL(init_client(tenant_id,
                          access_token,
                          remote_catalog_name,
                          allocator,
                          config,
                          client))) {
    LOG_WARN("failed to open Unified Catalog query client", K(ret));
  } else if (OB_FAIL(client.reader().get_catalog_info(catalog_info))) {
    LOG_WARN("failed to identify Unified Catalog source", K(ret), K(remote_catalog_name));
  } else {
    if (OB_FAIL(fetch_ob_query_table_schema(child_catalog_id, database_name, table_name,
                                             allocator, client.reader(),
                                             is_ob_table))) {
      LOG_WARN("failed to fetch UC OceanBase query schema", K(ret), K(table_name));
    } else if (!is_ob_table) {
      if (ObUnifiedCatalogStorageType::HDFS != catalog_info.storage_type_
          || ObUnifiedCatalogStorageAuth::SIMPLE != catalog_info.storage_auth_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("UC Iceberg query requires HDFS SIMPLE storage", K(ret), K(remote_catalog_name));
      } else if (OB_FAIL(fetch_iceberg_query_table_schema(
                     tenant_id, child_catalog_id, database_name, table_name, database_id, table_id,
                     case_mode, allocator, client.reader(), table_schema, iceberg_metadata))) {
        LOG_WARN("failed to fetch UC Iceberg query schema", K(ret), K(table_name));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::fetch_ob_query_table_schema(
    const uint64_t child_catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    ObIAllocator &allocator,
    ObUnifiedCatalogReader &reader,
    bool &is_ob_table)
{
  int ret = OB_SUCCESS;
  ObString response_body;
  ObUnifiedCatalogObTableMetadata metadata;
  if (OB_FAIL(reader.load_ob_table(database_name, table_name, allocator, response_body, true))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("UC OceanBase table does not exist",
               K(ret), K(child_catalog_id), K(database_name), K(table_name));
    } else {
      LOG_WARN("failed to load UC OceanBase Table", K(ret), K(table_name));
    }
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::try_decode_ob_table_metadata(
                 response_body, table_name, allocator, metadata, is_ob_table))) {
    LOG_WARN("failed to decode UC OB table metadata", K(ret), K(table_name));
  } else if (is_ob_table) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("UC native OB query requires catalog_sync_target", K(ret), K(child_catalog_id));
  }
  return ret;
}

int ObUnifiedCatalogMetadataProvider::fetch_iceberg_query_table_schema(
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const ObString &database_name,
    const ObString &table_name,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObNameCaseMode case_mode,
    ObIAllocator &allocator,
    ObUnifiedCatalogReader &reader,
    ObTableSchema *&table_schema,
    iceberg::ObIcebergTableMetadata *&iceberg_metadata)
{
  using iceberg::ObIcebergTableMetadata;
  int ret = OB_SUCCESS;
  ObString response_body;
  ObString metadata_location;
  ObStorageType metadata_storage_type;
  ObStorageType table_storage_type;
  ObIcebergTableMetadata *query_metadata = NULL;
  if (OB_FAIL(reader.load_iceberg_query_table(database_name, table_name, allocator, response_body))) {
    if (OB_HTTP_NOT_FOUND == ret) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("UC Iceberg table does not exist",
               K(ret), K(child_catalog_id), K(database_name), K(table_name));
    } else {
      LOG_WARN("failed to load UC Iceberg Table", K(ret), K(table_name));
    }
  } else if (OB_FAIL(ObUnifiedCatalogIcebergTableCodec::decode_metadata_location(
                 allocator, response_body, metadata_location))) {
    LOG_WARN("failed to decode UC Iceberg metadata location", K(ret), K(table_name));
  } else if (OB_FAIL(get_storage_type_from_path_for_external_table(
                 metadata_location, metadata_storage_type))) {
    LOG_WARN("failed to identify UC Iceberg metadata storage", K(ret), K(table_name));
  } else if (OB_STORAGE_HDFS != metadata_storage_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("UC Iceberg query requires an HDFS metadata location", K(ret), K(table_name));
  } else if (OB_ISNULL(query_metadata = OB_NEWx(ObIcebergTableMetadata, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate UC Iceberg metadata", K(ret), K(table_name));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaConverter::build_iceberg_query_table_schema(
                 metadata_location, database_name, table_name, tenant_id,
                 child_catalog_id, database_id, table_id, case_mode, table_schema,
                 *query_metadata))) {
    LOG_WARN("failed to convert UC Iceberg query table schema", K(ret), K(table_name));
  } else if (OB_FAIL(get_storage_type_from_path_for_external_table(
                 query_metadata->table_metadata_.location, table_storage_type))) {
    LOG_WARN("failed to identify UC Iceberg table storage", K(ret), K(table_name));
  } else if (OB_STORAGE_HDFS != table_storage_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("UC Iceberg query requires an HDFS table location", K(ret), K(table_name));
  } else {
    iceberg_metadata = query_metadata;
  }
  if (OB_SUCCESS != ret && OB_NOT_NULL(query_metadata)) {
    OB_DELETEx(ObIcebergTableMetadata, &allocator, query_metadata);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

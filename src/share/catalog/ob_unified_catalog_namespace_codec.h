/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_NAMESPACE_CODEC_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_NAMESPACE_CODEC_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonObject;
template <typename T> class ObIArray;
}
namespace share
{

enum class ObUnifiedCatalogKind
{
  OCEANBASE,
  ICEBERG,
  OTHER
};

enum class ObUnifiedCatalogStorageType
{
  HDFS,
  OTHER
};

enum class ObUnifiedCatalogStorageAuth
{
  SIMPLE,
  OTHER
};

struct ObUnifiedCatalogInfo
{
  ObUnifiedCatalogInfo()
      : kind_(ObUnifiedCatalogKind::OTHER),
        storage_type_(ObUnifiedCatalogStorageType::OTHER),
        storage_auth_(ObUnifiedCatalogStorageAuth::OTHER) {}
  ObUnifiedCatalogKind kind_;
  ObUnifiedCatalogStorageType storage_type_;
  ObUnifiedCatalogStorageAuth storage_auth_;
};

struct ObUnifiedCatalogIdentity
{
  TO_STRING_KV(K_(name), K_(id));

  common::ObString name_;
  uint64_t id_;
};

// Encodes namespace requests and validates Unified Catalog/namespace identity responses.
class ObUnifiedCatalogNamespaceCodec
{
public:
  static int decode_catalog_identities(common::ObIAllocator &allocator,
                                       const uint64_t tenant_id,
                                       const common::ObString &response_body,
                                       common::ObIArray<ObUnifiedCatalogIdentity> &catalogs);
  static int decode_namespace_names_page(common::ObIAllocator &allocator,
                                         const uint64_t tenant_id,
                                         const common::ObString &response_body,
                                         common::ObIArray<common::ObString> &namespace_names,
                                         common::ObString &next_page_token);
  static int decode_ob_table_names_page(common::ObIAllocator &allocator,
                                       const uint64_t tenant_id,
                                       const common::ObString &response_body,
                                       common::ObIArray<common::ObString> &table_names,
                                       common::ObString &next_page_token);
  static int decode_catalog_info(common::ObIAllocator &allocator,
                                 const common::ObString &response_body,
                                 const common::ObString &catalog_name,
                                 ObUnifiedCatalogInfo &catalog_info);
  static int validate_catalog_response(common::ObIAllocator &allocator,
                                       const common::ObString &response_body,
                                       const common::ObString &catalog_name,
                                       const uint64_t cluster_id,
                                       const uint64_t tenant_id);
  static int encode_namespace_request(common::ObIAllocator &allocator,
                                      const common::ObString &database_name,
                                      const uint64_t cluster_id,
                                      const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      common::ObString &body);
  static int validate_namespace_response(const common::ObString &body,
                                         const common::ObString &database_name,
                                         const uint64_t cluster_id,
                                         const uint64_t tenant_id,
                                         const uint64_t database_id);
  static int validate_read_namespace_response(const uint64_t tenant_id,
                                              const common::ObString &body,
                                              const common::ObString &database_name);
  static int decode_namespace_table_presence(common::ObIAllocator &allocator,
                                             const common::ObString &body,
                                             bool &has_tables);

private:
  static constexpr const char *JSON_CATALOG = "catalog";
  static constexpr const char *JSON_CATALOGS = "catalogs";
  static constexpr const char *SOURCE_TYPE_OCEANBASE = "OCEANBASE";
  static constexpr const char *SOURCE_TYPE_ICEBERG = "ICEBERG";
  static constexpr const char *STORAGE_TYPE_HDFS = "HDFS";
  static constexpr const char *STORAGE_AUTH_SIMPLE = "SIMPLE";
  static constexpr const char *JSON_NAME = "name";
  static constexpr const char *JSON_ID = "id";
  static constexpr const char *JSON_SOURCE = "source";
  static constexpr const char *JSON_SOURCE_TYPE = "sourceType";
  static constexpr const char *JSON_STORAGE_CONFIG = "storage";
  static constexpr const char *JSON_STORAGE_TYPE = "storageType";
  static constexpr const char *JSON_AUTH_TYPE = "authenticationType";
  static constexpr const char *JSON_NAMESPACE = "namespace";
  static constexpr const char *JSON_NAMESPACES = "namespaces";
  static constexpr const char *JSON_TABLES = "tables";
  static constexpr const char *JSON_STATE = "state";
  static constexpr const char *TABLE_ACTIVE = "ACTIVE";
  static constexpr const char *TABLE_PREPARING = "PREPARING";
  static constexpr const char *JSON_NEXT_PAGE_TOKEN = "nextPageToken";
  static constexpr const char *NAMESPACE_SOURCE_CLUSTER_ID = "clusterId";
  static constexpr const char *NAMESPACE_SOURCE_TENANT_ID = "tenantId";
  static constexpr const char *NAMESPACE_SOURCE_DATABASE_ID = "databaseId";

  static int get_json_string(const common::ObJsonObject &object,
                             const char *key,
                             common::ObString &value);
  static int expect_json_string(const common::ObJsonObject &object,
                                const char *key,
                                const common::ObString &expected);
  static int add_json_string(common::ObIAllocator &allocator,
                             common::ObJsonObject &object,
                             const char *key,
                             const common::ObString &value);
  static int add_source_property(common::ObIAllocator &allocator,
                                 common::ObJsonObject &properties,
                                 const char *key,
                                 const uint64_t value);
  static int decode_next_page_token(common::ObIAllocator &allocator,
                                    const common::ObJsonObject &root,
                                    common::ObString &next_page_token);
  static int append_unique_name(common::ObIAllocator &allocator,
                                const common::ObString &name,
                                common::ObIArray<common::ObString> &names);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_NAMESPACE_CODEC_H_

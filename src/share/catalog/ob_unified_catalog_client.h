/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CLIENT_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CLIENT_H_

#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "share/catalog/rest/requests/ob_rest_http_request.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T> class ObIArray;
}
namespace share
{
class ObCurlRestClient;
class ObUnifiedCatalogConfig;
class ObUnifiedCatalogPartitionSync;
struct ObUnifiedCatalogIdentity;
struct ObUnifiedCatalogInfo;
namespace schema
{
class ObTableSchema;
}

class ObUnifiedCatalogClient;
class ObUnifiedCatalogWriter;

/** Read-only Unified Catalog, namespace, table, and partition metadata operations. */
class ObUnifiedCatalogReader
{
public:
  int list_accessible_catalog_names(common::ObIArray<common::ObString> &catalog_names);
  int list_accessible_catalog_identities(common::ObIArray<ObUnifiedCatalogIdentity> &catalogs);
  int list_namespace_names(common::ObIArray<common::ObString> &namespace_names);
  int list_ob_table_names(const common::ObString &database_name,
                          common::ObIArray<common::ObString> &table_names);
  int load_namespace(const common::ObString &database_name,
                     common::ObIAllocator &response_allocator,
                     common::ObString &response_body);
  int load_ob_table(const common::ObString &database_name,
                    const common::ObString &table_name,
                    common::ObIAllocator &response_allocator,
                    common::ObString &response_body,
                    const bool for_data_read = false);
  int load_iceberg_table(const common::ObString &database_name,
                         const common::ObString &table_name,
                         common::ObIAllocator &response_allocator,
                         common::ObString &response_body);
  int load_iceberg_query_table(const common::ObString &database_name,
                               const common::ObString &table_name,
                               common::ObIAllocator &response_allocator,
                               common::ObString &response_body);
  int load_part_values(const common::ObString &database_name,
                       const common::ObString &table_name,
                       const common::ObString &page_token,
                       common::ObIAllocator &response_allocator,
                       common::ObString &response_body);
  int catalog_exists(bool &exists);
  int get_catalog_info(ObUnifiedCatalogInfo &catalog_info);
  int validate_catalog();
  int namespace_has_tables(const common::ObString &database_name,
                           const uint64_t database_id,
                           bool &has_tables);

private:
  friend class ObUnifiedCatalogClient;
  friend class ObUnifiedCatalogWriter;
  friend class ObUnifiedCatalogPartitionSync;
  explicit ObUnifiedCatalogReader(ObUnifiedCatalogClient &client) : client_(client) {}
  int load_and_validate_namespace(const common::ObString &database_name,
                                  const uint64_t database_id);
  int load_and_validate_namespace_if_exists(const common::ObString &database_name,
                                            const uint64_t database_id,
                                            bool &exists);
  int load_and_validate_table(const common::ObString &database_name,
                              const uint64_t database_id,
                              const schema::ObTableSchema &table_schema);
  int load_and_validate_table_source_if_exists(
      const common::ObString &database_name,
      const uint64_t database_id,
      const common::ObString &table_name,
      const uint64_t table_id,
      bool &exists,
      int64_t *schema_version = NULL);
  int load_preparing_operation_id(const common::ObString &database_name,
                                  const common::ObString &table_name,
                                  common::ObIAllocator &response_allocator,
                                  bool &preparing,
                                  common::ObString &operation_id);

private:
  ObUnifiedCatalogClient &client_;
  DISALLOW_COPY_AND_ASSIGN(ObUnifiedCatalogReader);
};

/** Unified Catalog metadata writes using the Client connection and Reader validation. */
class ObUnifiedCatalogWriter
{
public:
  int create_namespace(const common::ObString &database_name, const uint64_t database_id);
  int drop_namespace(const common::ObString &database_name);
  int reserve_table(const common::ObString &database_name,
                    const uint64_t database_id,
                    const common::ObString &table_name,
                    const common::ObString &operation_id);
  int complete_reserved_table(const common::ObString &database_name,
                              const uint64_t database_id,
                              const schema::ObTableSchema &table_schema,
                              const common::ObString &operation_id);
  int cancel_reserved_table(const common::ObString &database_name,
                            const common::ObString &table_name,
                            const common::ObString &operation_id);
  int sync_table(const common::ObString &database_name,
                 const uint64_t database_id,
                 const schema::ObTableSchema &table_schema,
                 const bool recover_preparing = false);
  int drop_table(const common::ObString &database_name, const common::ObString &table_name);

private:
  friend class ObUnifiedCatalogClient;
  ObUnifiedCatalogWriter(ObUnifiedCatalogClient &client, ObUnifiedCatalogReader &reader)
      : client_(client), reader_(reader) {}
  int update_active_table(const common::ObString &database_name,
                          const uint64_t database_id,
                          const schema::ObTableSchema &table_schema);
private:
  ObUnifiedCatalogClient &client_;
  ObUnifiedCatalogReader &reader_;
  DISALLOW_COPY_AND_ASSIGN(ObUnifiedCatalogWriter);
};

/** Unified Catalog connection and REST transport shared by Reader and Writer. */
class ObUnifiedCatalogClient
{
public:
  explicit ObUnifiedCatalogClient(common::ObIAllocator &allocator);
  ~ObUnifiedCatalogClient();

  int init(const ObUnifiedCatalogConfig &config,
           const common::ObString &access_token,
           const common::ObString &catalog_name,
           const uint64_t cluster_id,
           const uint64_t tenant_id);
  int init_connection(const ObUnifiedCatalogConfig &config,
                      const common::ObString &access_token,
                      const uint64_t cluster_id,
                      const uint64_t tenant_id);
  ObUnifiedCatalogReader &reader() { return reader_; }
  ObUnifiedCatalogWriter &writer() { return writer_; }

private:
  friend class ObUnifiedCatalogReader;
  friend class ObUnifiedCatalogWriter;
  friend class ObUnifiedCatalogPartitionSync;

  enum class ConflictPolicy
  {
    UNCHANGED,
    RETURN_EAGAIN,
    RETURN_ALREADY_EXISTS
  };
  static constexpr int64_t HTTP_STATUS_ANY_SUCCESS = 0;
  int init_connection_impl(const ObUnifiedCatalogConfig &config,
                           const common::ObString &access_token,
                           const uint64_t cluster_id,
                           const uint64_t tenant_id);
  int request(const ObRestHttpMethod method,
              const common::ObString &path,
              const common::ObString &request_body,
              common::ObIAllocator &response_allocator,
              common::ObString &response_body,
              const ConflictPolicy conflict_policy = ConflictPolicy::UNCHANGED,
              const int64_t expected_status = HTTP_STATUS_ANY_SUCCESS,
              const common::ObString &access_delegation = common::ObString(),
              const bool ob_data_read = false);

private:
  static constexpr const char *AUTHORIZATION_HEADER = "Authorization";
  static constexpr const char *REALM_HEADER = "Polaris-Realm";
  static constexpr const char *CONTENT_TYPE_HEADER = "Content-Type";
  static constexpr const char *ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  static constexpr const char *VENDED_CREDENTIALS = "vended-credentials";
  static constexpr const char *OB_TABLE_ACCESS_HEADER = "X-OceanBase-Table-Access";
  static constexpr const char *OB_TABLE_ACCESS_DATA = "data";
  static constexpr const char *JSON_CONTENT_TYPE = "application/json";
  static constexpr uint64_t UNIFIED_CATALOG_TRANSPORT_POOL_KEY = common::OB_INVALID_ID;
  static constexpr int64_t MAX_REQUEST_BODY_SIZE = 10 * 1024 * 1024; // 10MiB
  static constexpr int64_t MAX_RESPONSE_BODY_SIZE = 16 * 1024 * 1024; // 16MiB
  static bool is_valid_access_token(const common::ObString &access_token);
  static constexpr int64_t HTTP_STATUS_OK = 200;
  static constexpr int64_t HTTP_STATUS_NO_CONTENT = 204;
  static constexpr int64_t HTTP_STATUS_CONFLICT = 409;

private:
  common::ObIAllocator *allocator_;
  ObCurlRestClient *transport_;
  const ObUnifiedCatalogConfig *config_;
  common::ObString catalog_name_;
  common::ObString authorization_;
  uint64_t cluster_id_;
  uint64_t tenant_id_;
  bool is_inited_;
  ObUnifiedCatalogReader reader_;
  ObUnifiedCatalogWriter writer_;
  DISALLOW_COPY_AND_ASSIGN(ObUnifiedCatalogClient);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_CLIENT_H_

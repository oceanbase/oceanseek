/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_ON_DEMAND_COORDINATOR_H_
#define OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_ON_DEMAND_COORDINATOR_H_

#include "share/schema/ob_simple_schema_kv_cache.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
namespace schema
{

class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
struct ObSimpleForeignKeyInfo;
struct ObSimpleConstraintInfo;

// Coordinates resolve cache, object cache and point-fetch backsource for
// ON_DEMAND simple schema point lookup. Owned by ObMultiVersionSchemaService.
class ObSimpleSchemaOnDemandCoordinator
{
public:
  ObSimpleSchemaOnDemandCoordinator();
  ~ObSimpleSchemaOnDemandCoordinator() {}

  int init(ObMultiVersionSchemaService *schema_service);
  bool check_inner_stat() const;

  int get_schema_version_range_by_id(ObSchemaGetterGuard &guard,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     ObSchemaResolveRange &version_range);

  int get_schema_version_range_by_name(ObSchemaGetterGuard &guard,
                                       const uint64_t tenant_id,
                                       const ObTableNameNamespace table_namespace,
                                       const uint64_t database_id,
                                       const common::ObString &table_name,
                                       const uint64_t session_id,
                                       const common::ObNameCaseMode name_case_mode,
                                       ObSchemaResolveRange &version_range);

  int get_foreign_key_info_by_name(ObSchemaGetterGuard &guard,
                                   const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &foreign_key_name,
                                   ObSimpleForeignKeyInfo &info);

  int get_constraint_info_by_name(ObSchemaGetterGuard &guard,
                                  const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const common::ObString &constraint_name,
                                  ObSimpleConstraintInfo &info);

  int get_simple_table_schema(ObSchemaGetterGuard &guard,
                              const uint64_t tenant_id,
                              const uint64_t table_id,
                              const ObSimpleTableSchemaV2 *&schema);

private:
  static const int64_t LOAD_WAIT_TIMEOUT_US = 5L * 1000L * 1000L;
  static int64_t get_load_deadline_();
  static int64_t get_wait_deadline_(const int64_t load_deadline_us);
  static int do_backoff_sleep_(const int64_t abs_deadline_us,
                                   int64_t &retry_count);
  int get_publish_snapshot_(ObSchemaGetterGuard &guard,
                            const uint64_t tenant_id,
                            const int64_t snapshot_version,
                            const int64_t abs_deadline_us,
                            ObTenantSchemaPublishState *&state,
                            ObSchemaPublishSnapshot &publish_snapshot) const;
  int get_version_range_by_id_once_(ObSchemaGetterGuard &guard,
                       const uint64_t tenant_id,
                       const uint64_t table_id,
                       const int64_t snapshot_version,
                       const int64_t abs_deadline_us,
                       ObTenantSchemaPublishState &state,
                       const ObSchemaPublishSnapshot &publish_snapshot,
                       ObSchemaResolveRange &version_range);
  int normalize_table_name_(const common::ObString &table_name,
                            const common::ObNameCaseMode case_mode,
                            common::ObIAllocator &allocator,
                            common::ObString &normalized_name) const;
  // L1-cached lookup for one session_id. Caller owns session→0 fallback.
  int get_version_range_by_name_with_session_id_(
      ObSchemaGetterGuard &guard,
      const ObSchemaNameResolveKey &key,
      const common::ObString &table_name,
      const int64_t snapshot_version,
      const common::ObNameCaseMode name_case_mode,
      const int64_t abs_deadline_us,
      ObTenantSchemaPublishState &state,
      const ObSchemaPublishSnapshot &publish_snapshot,
      ObSchemaResolveRange &version_range);
  int pin_simple_schema_(ObSchemaGetterGuard &guard,
                         const uint64_t tenant_id,
                         const uint64_t table_id,
                         const ObSimpleTableSchemaV2 *schema,
                         common::ObKVCacheHandle &handle) const;
  int rollback_schema_pins_(ObSchemaGetterGuard &guard,
                            const int64_t schema_obj_count,
                            const int64_t pin_cache_size) const;
  int get_local_simple_schema_(ObSchemaGetterGuard &guard,
                               const uint64_t tenant_id,
                               const uint64_t table_id,
                               const int64_t precise_schema_version,
                               const ObSimpleTableSchemaV2 *&schema) const;
  int load_simple_schema_from_cache_or_sql_(ObSchemaGetterGuard &guard,
                                            const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            const int64_t precise_schema_version,
                                            const int64_t abs_deadline_us,
                                            const ObSimpleTableSchemaV2 *&schema);

  ObMultiVersionSchemaService *schema_service_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSimpleSchemaOnDemandCoordinator);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_ON_DEMAND_COORDINATOR_H_

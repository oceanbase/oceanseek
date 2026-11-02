/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_ON_DEMAND_SCHEMA_GETTER_GUARD_H_
#define OCEANBASE_SHARE_SCHEMA_OB_ON_DEMAND_SCHEMA_GETTER_GUARD_H_

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_scan_operator.h"
#include "share/schema/ob_simple_schema_kv_cache.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObMultiVersionSchemaService;

/**
 * Stateless OnDemand table strategy. Point lookups use the two-level KVCache;
 * set-oriented lookups scan current inner-table metadata and then load objects.
 *
 * STRONG full scans share the Guard's transaction snapshot until reset/destruction.
 */
class ObOnDemandSchemaGetterGuard final : public ObISchemaGetterGuardTableImpl
{
public:
  static const ObOnDemandSchemaGetterGuard &instance();

  // --- switchable subset (implemented) ---
  int get_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &ids) const override;
  int get_table_ids_in_tenant_for_cdc(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &ids) const override;
  int get_table_ids_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, common::ObIArray<uint64_t> &ids) const override;
  int get_table_ids_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, common::ObIArray<uint64_t> &ids) const override;
  int get_primary_table_schema_in_tablegroup(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t tablegroup_id,
      const ObSimpleTableSchemaV2 *&schema) const override;
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObSimpleTableSchemaIterator &iter) const override;
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObTableSchemaIterator &iter) const override;
  int get_view_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObTableSchemaIterator &iter) const override;
  int get_table_schemas_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, ObSimpleTableSchemaIterator &iter) const override;
  int get_table_schemas_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, ObTableSchemaIterator &iter) const override;
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, ObSimpleTableSchemaIterator &iter) const override;
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, ObTableSchemaIterator &iter) const override;
  int get_table_schemas_in_tablespace(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablespace_id, ObTableSchemaIterator &iter) const override;
  int get_tmp_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const override;
  int get_external_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const override;
  int get_duplicate_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const override;
  int get_encrypted_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const override;
  int get_dynamic_partition_table_ids_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, common::ObIArray<uint64_t> &table_ids) const override;
  int get_vector_info_index_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      bool &has_ivf_index, common::ObIArray<uint64_t> &table_ids) const override;
  int get_table_schemas_with_async_index_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas) const override;

  // Extra entry used by ObSchemaIdScanIterator (not part of classic Array APIs).
  int get_table_ids_by_inner_table(ObSchemaGetterGuard &guard, const ObSchemaScanSpec &spec,
                                   common::ObIArray<uint64_t> &table_ids) const;

  // Point and compatibility APIs backed by the two-level cache / scan path.
  int get_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t, const common::ObString &,
      bool, ObSchemaGetterGuard::CheckTableType, uint64_t &, bool) const override;
  int get_foreign_key_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, uint64_t &) const override;
  int get_foreign_key_info(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, ObSimpleForeignKeyInfo &) const override;
  int get_constraint_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, uint64_t &) const override;
  int get_constraint_info(const ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, ObSimpleConstraintInfo &) const override;
  int get_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const ObTableSchema *&) const override;
  int get_simple_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, const ObSimpleTableSchemaV2 *&, bool, bool) const override;
  int get_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, const ObTableSchema *&, bool, bool) const override;
  int get_index_meta_infos_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<ObAuxTableMetaInfo> &) const override;
  int get_index_schemas_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;
  int get_lob_schemas_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t, bool,
      common::ObIArray<const ObTableSchema *> &) const override;
  int get_simple_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override;
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override;
  int get_table_schemas_in_tablespace(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override;
  int get_simple_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;
  int get_simple_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;
  int check_table_exist(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, ObSchemaGetterGuard::CheckTableType,
      bool &, uint64_t *) const override;
  int check_table_exist(ObSchemaGetterGuard &, uint64_t, uint64_t, bool &) const override;
  int get_table_schema_version(ObSchemaGetterGuard &, ObSchemaType, uint64_t,
      uint64_t, int64_t &, uint64_t *) const override;
  int get_simple_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const ObSimpleTableSchemaV2 *&) const override;
  int get_schema_count(ObSchemaGetterGuard &, uint64_t, int64_t &) const override;
  int get_schema_size(ObSchemaGetterGuard &, uint64_t, int64_t &) const override;
  int get_tenant_unavailable_index(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int check_unavailable_index_exist(ObSchemaGetterGuard &, uint64_t, bool &) const override;
  int check_restore_error_index_exist(ObSchemaGetterGuard &, uint64_t, bool &) const override;
  int get_idx_schema_by_origin_idx_name(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, const ObTableSchema *&) const override;
  int generate_tablet_table_map(ObSchemaGetterGuard &, uint64_t,
      common::hash::ObHashMap<common::ObTabletID, uint64_t> &) const override;
  int deep_copy_index_name_map(ObSchemaGetterGuard &, common::ObIAllocator &,
      ObIndexNameMap &) const override;

private:
  static const int64_t LOAD_WAIT_TIMEOUT_US = 5L * 1000L * 1000L;
  ObOnDemandSchemaGetterGuard() {}
  int get_publish_snapshot_(ObSchemaGetterGuard &guard,
                            const uint64_t tenant_id,
                            const int64_t snapshot_version,
                            const int64_t abs_deadline_us,
                            ObTenantSchemaPublishState *&state,
                            ObSchemaPublishSnapshot &publish_snapshot) const;
  int get_full_table_schema_(ObSchemaGetterGuard &guard,
                             const ObSimpleTableSchemaV2 &simple_schema,
                             const ObTableSchema *&table_schema) const;
  int get_table_id_by_oracle_index_name_(
      ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &index_table_name,
      const bool is_built_in_index,
      ObSchemaGetterGuard::CheckTableType check_type,
      uint64_t &table_id) const;
  int get_simple_oracle_index_schema_(
      ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &index_table_name,
      const bool is_built_in_index,
      const ObSimpleTableSchemaV2 *&index_schema) const;
  int get_simple_index_schema_by_origin_name_(
      ObSchemaGetterGuard &guard,
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &index_name,
      const bool is_built_in_index,
      const ObSimpleTableSchemaV2 *&index_schema) const;
  int get_full_table_schema_(ObSchemaGetterGuard &guard,
                             const uint64_t tenant_id,
                             const uint64_t table_id,
                             const int64_t schema_version,
                             const ObTableSchema *&table_schema) const;
  int load_full_table_schema_(ObSchemaGetterGuard &guard,
                              const uint64_t tenant_id,
                              const uint64_t table_id,
                              const int64_t schema_version,
                              const ObTableSchema *&table_schema) const;
  static ObTableNameNamespace get_table_namespace_(const bool is_index,
                                                    const bool with_hidden_flag,
                                                    const bool is_built_in_index);
  // Prefer mgr (same as original). Lazy guard has no mgr: fall back to full sys_variable.
  int get_name_case_mode_for_resolve_(ObSchemaGetterGuard &guard,
                                      const uint64_t tenant_id,
                                      common::ObNameCaseMode &case_mode) const;
  static int64_t get_load_deadline_();
  static int64_t get_wait_deadline_(const int64_t load_deadline_us);
  static int do_backoff_sleep_(const int64_t abs_deadline_us,
                                   int64_t &retry_count);
  int fetch_table_ids_uncached_(ObSchemaGetterGuard &guard, const ObSchemaScanSpec &spec,
                                common::ObIArray<uint64_t> &table_ids) const;
  int fetch_tablegroup_table_ids_uncached_(ObSchemaGetterGuard &guard,
                                           const uint64_t tenant_id,
                                           const uint64_t tablegroup_id,
                                           common::ObIArray<uint64_t> &table_ids) const;
  // __all_table 不含 is_core_table；枚举侧从常驻 mgr 按同一 spec 谓词补齐 table_id。
  int append_core_table_ids_from_mgr_(ObSchemaGetterGuard &guard,
                                      const ObSchemaScanSpec &spec,
                                      common::ObIArray<uint64_t> &table_ids) const;
  static bool simple_matches_scan_spec_(const ObSimpleTableSchemaV2 &schema,
                                        const ObSchemaScanSpec &spec,
                                        const common::ObIArray<uint64_t> *encrypted_ts_ids);
  int scan_tenant_indexes_by_status_(ObSchemaGetterGuard &guard,
                                     const uint64_t tenant_id,
                                     const ObIndexStatus index_status,
                                     const bool early_stop,
                                     bool &exist,
                                     common::ObIArray<uint64_t> *index_ids) const;
  DISALLOW_COPY_AND_ASSIGN(ObOnDemandSchemaGetterGuard);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_SCHEMA_OB_ON_DEMAND_SCHEMA_GETTER_GUARD_H_

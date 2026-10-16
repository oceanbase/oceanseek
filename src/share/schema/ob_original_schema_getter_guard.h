/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_ORIGINAL_SCHEMA_GETTER_GUARD_H_
#define OB_OCEANBASE_SCHEMA_OB_ORIGINAL_SCHEMA_GETTER_GUARD_H_

#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObOriginalSchemaGetterGuard final : public ObISchemaGetterGuardTableImpl
{
public:
  static const ObOriginalSchemaGetterGuard &instance();

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
  int get_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_table_ids_in_tenant_for_cdc(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_table_ids_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_table_ids_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<uint64_t> &) const override;
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
  int get_primary_table_schema_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const ObSimpleTableSchemaV2 *&) const override;
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObSimpleTableSchemaIterator &) const override;
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObTableSchemaIterator &) const override;
  int get_view_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObTableSchemaIterator &) const override;
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObSimpleTableSchemaIterator &) const override;
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override;
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObSimpleTableSchemaIterator &) const override;
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override;
  int get_table_schemas_in_tablespace(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override;
  int get_tmp_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_external_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_duplicate_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_encrypted_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
  int get_dynamic_partition_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override;
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
  int get_vector_info_index_ids_in_tenant(ObSchemaGetterGuard &, uint64_t, bool &,
      common::ObIArray<uint64_t> &) const override;
  int get_table_schemas_with_async_index_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override;

private:
  ObOriginalSchemaGetterGuard() {}
  DISALLOW_COPY_AND_ASSIGN(ObOriginalSchemaGetterGuard);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_ORIGINAL_SCHEMA_GETTER_GUARD_H_

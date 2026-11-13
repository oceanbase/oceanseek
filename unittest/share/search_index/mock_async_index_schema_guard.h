/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_UNITTEST_ASYNC_INDEX_SCHEMA_GUARD_H_
#define OCEANBASE_UNITTEST_ASYNC_INDEX_SCHEMA_GUARD_H_

#include "share/schema/ob_table_schema.h"
#define private public
#include "share/schema/ob_schema_getter_guard.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share::schema;

// Use the real guard dispatch and production enumeration. Only schema lookup is
// replaced with a fixed snapshot catalog; no lifecycle algorithm is duplicated.
// Catalog and schemas must outlive the guards bound to this catalog.
class AsyncIndexSchemaCatalog : public ObISchemaGetterGuardTableImpl
{
public:
  AsyncIndexSchemaCatalog()
      : fail_table_id_(OB_INVALID_ID), failure_(OB_TIMEOUT), reads_(0) {}
  int add(const ObTableSchema &schema) { return tables_.push_back(&schema); }
  void bind(ObSchemaGetterGuard &guard) { guard.table_impl_ = this; }
  int get_table_schema(ObSchemaGetterGuard &, uint64_t tenant_id, uint64_t table_id,
                       const ObTableSchema *&schema) const override
  {
    int ret = OB_SUCCESS;
    ++reads_;
    schema = nullptr;
    if (table_id == fail_table_id_) {
      ret = failure_;
    } else {
      for (int64_t i = 0; i < tables_.count(); ++i) {
        if (tables_.at(i)->get_table_id() == table_id) {
          // Deliberately allow malformed tenant metadata through to exercise
          // the production relationship validator, not hide it in the fixture.
          schema = tables_.at(i);
          break;
        }
      }
    }
    UNUSED(tenant_id);
    return ret;
  }
  common::ObSEArray<const ObTableSchema *, 16> tables_;
  uint64_t fail_table_id_;
  int failure_;
  mutable int64_t reads_;

  // Calls outside the tested lookup contract fail rather than fabricate data.
  int get_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t, const common::ObString &,
      bool, ObSchemaGetterGuard::CheckTableType, uint64_t &, bool) const override { return OB_NOT_SUPPORTED; }
  int get_foreign_key_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, uint64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_foreign_key_info(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, ObSimpleForeignKeyInfo &) const override { return OB_NOT_SUPPORTED; }
  int get_constraint_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, uint64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_constraint_info(const ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, ObSimpleConstraintInfo &) const override { return OB_NOT_SUPPORTED; }
  int get_simple_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, const ObSimpleTableSchemaV2 *&, bool, bool) const override { return OB_NOT_SUPPORTED; }
  int get_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, const ObTableSchema *&, bool, bool) const override { return OB_NOT_SUPPORTED; }
  int get_index_meta_infos_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<ObAuxTableMetaInfo> &) const override { return OB_NOT_SUPPORTED; }
  int get_index_schemas_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
  int get_lob_schemas_with_data_table_id(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_ids_in_tenant_for_cdc(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_ids_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_ids_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t, bool,
      common::ObIArray<const ObTableSchema *> &) const override { return OB_NOT_SUPPORTED; }
  int get_simple_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tablespace(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObTableSchema *> &) const override { return OB_NOT_SUPPORTED; }
  int get_simple_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
  int get_simple_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
  int get_primary_table_schema_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const ObSimpleTableSchemaV2 *&) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObSimpleTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_view_schemas_in_tenant(ObSchemaGetterGuard &, uint64_t,
      ObTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObSimpleTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_database(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObSimpleTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_in_tablespace(ObSchemaGetterGuard &, uint64_t, uint64_t,
      ObTableSchemaIterator &) const override { return OB_NOT_SUPPORTED; }
  int get_tmp_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_external_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_duplicate_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_encrypted_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_dynamic_partition_table_ids_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int check_table_exist(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, bool, ObSchemaGetterGuard::CheckTableType,
      bool &, uint64_t *) const override { return OB_NOT_SUPPORTED; }
  int check_table_exist(ObSchemaGetterGuard &, uint64_t, uint64_t, bool &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schema_version(ObSchemaGetterGuard &, ObSchemaType, uint64_t,
      uint64_t, int64_t &, uint64_t *) const override { return OB_NOT_SUPPORTED; }
  int get_simple_table_schema(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const ObSimpleTableSchemaV2 *&) const override { return OB_NOT_SUPPORTED; }
  int get_schema_count(ObSchemaGetterGuard &, uint64_t, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_schema_size(ObSchemaGetterGuard &, uint64_t, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_tenant_unavailable_index(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int check_unavailable_index_exist(ObSchemaGetterGuard &, uint64_t, bool &) const override { return OB_NOT_SUPPORTED; }
  int check_restore_error_index_exist(ObSchemaGetterGuard &, uint64_t, bool &) const override { return OB_NOT_SUPPORTED; }
  int get_idx_schema_by_origin_idx_name(ObSchemaGetterGuard &, uint64_t, uint64_t,
      const common::ObString &, const ObTableSchema *&) const override { return OB_NOT_SUPPORTED; }
  int generate_tablet_table_map(ObSchemaGetterGuard &, uint64_t,
      common::hash::ObHashMap<common::ObTabletID, uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int deep_copy_index_name_map(ObSchemaGetterGuard &, common::ObIAllocator &,
      ObIndexNameMap &) const override { return OB_NOT_SUPPORTED; }
  int get_vector_info_index_ids_in_tenant(ObSchemaGetterGuard &, uint64_t, bool &,
      common::ObIArray<uint64_t> &) const override { return OB_NOT_SUPPORTED; }
  int get_table_schemas_with_async_index_in_tenant(ObSchemaGetterGuard &, uint64_t,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &) const override { return OB_NOT_SUPPORTED; }
};
} // namespace unittest
} // namespace oceanbase
#endif

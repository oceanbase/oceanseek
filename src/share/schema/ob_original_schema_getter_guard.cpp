/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_original_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_scan_iterator.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

const ObOriginalSchemaGetterGuard &ObOriginalSchemaGetterGuard::instance()
{
  static const ObOriginalSchemaGetterGuard INSTANCE;
  return INSTANCE;
}

int ObOriginalSchemaGetterGuard::get_table_id(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    uint64_t database_id,
    const ObString &table_name,
    bool is_index,
    ObSchemaGetterGuard::CheckTableType check_type,
    uint64_t &table_id,
    bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  uint64_t session_id = guard.session_id_;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *simple_table = NULL;
  table_id = OB_INVALID_ID;
  if (ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE == check_type) {
    session_id = 0;
  }
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, database_id, session_id, table_name,
                                           is_index, simple_table,
                                           ObSchemaGetterGuard::USER_HIDDEN_TABLE_TYPE == check_type,
                                           is_built_in_index))) {
    LOG_WARN("get simple table failed", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (NULL != simple_table
             && !(ObSchemaGetterGuard::TEMP_TABLE_TYPE == check_type
                  && !is_inner_table(simple_table->get_table_id())
                  && !simple_table->is_tmp_table())) {
    table_id = simple_table->get_table_id();
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_foreign_key_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, uint64_t &id) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  id = OB_INVALID_ID;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_id(tenant_id, database_id, name, id))) {
    LOG_WARN("get foreign key id failed", KR(ret), K(tenant_id), K(database_id), K(name));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_foreign_key_info(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, ObSimpleForeignKeyInfo &info) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  info.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_info(tenant_id, database_id, name, info))) {
    LOG_WARN("get foreign key info failed", KR(ret), K(tenant_id), K(database_id), K(name));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_constraint_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, uint64_t &id) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  id = OB_INVALID_ID;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_id(tenant_id, database_id, name, id))) {
    LOG_WARN("get constraint id failed", KR(ret), K(tenant_id), K(database_id), K(name));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_constraint_info(
    const ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &name, ObSimpleConstraintInfo &info) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  info.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_info(tenant_id, database_id, name, info))) {
    LOG_WARN("get constraint info failed", KR(ret), K(tenant_id), K(database_id), K(name));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    const ObTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_cte_table(table_id) || is_external_object_id(table_id)) {
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema(TABLE_SCHEMA, tenant_id, table_id, schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_simple_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index,
    const ObSimpleTableSchemaV2 *&schema, bool with_hidden_flag,
    bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, database_id, guard.session_id_,
                                           table_name, is_index, schema,
                                           with_hidden_flag, is_built_in_index))) {
    LOG_WARN("get simple table failed", KR(ret), K(tenant_id), K(database_id), K(table_name));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index, const ObTableSchema *&schema,
    bool with_hidden_flag, bool is_built_in_index) const
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *simple_table = NULL;
  schema = NULL;
  if (OB_FAIL(get_simple_table_schema(guard, tenant_id, database_id, table_name,
                                      is_index, simple_table, with_hidden_flag,
                                      is_built_in_index))) {
    LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id), K(database_id));
  } else if (NULL != simple_table
             && OB_FAIL(guard.get_schema(TABLE_SCHEMA, simple_table->get_tenant_id(),
                                         simple_table->get_table_id(), schema,
                                         simple_table->get_schema_version()))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), KPC(simple_table));
  } else if (NULL != simple_table && OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_index_schemas_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  schemas.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, data_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if ((table_schema->is_table() || table_schema->is_tmp_table())
             && OB_FAIL(mgr->get_aux_schemas(tenant_id, data_table_id, schemas, USER_INDEX))) {
    LOG_WARN("fail to get aux schemas", KR(ret), K(tenant_id), K(data_table_id));
  }
  return ret;
}

// Append this owner's LOB schemas; the caller controls when to clear the array.
int ObOriginalSchemaGetterGuard::get_lob_schemas_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &lob_schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = nullptr;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObTableType lob_types[] = {AUX_LOB_META, AUX_LOB_PIECE};
    ObSEArray<const ObSimpleTableSchemaV2 *, OB_AUX_LOB_TABLE_CNT> aux_schemas;
    for (int64_t i = 0; OB_SUCC(ret) && i < OB_AUX_LOB_TABLE_CNT; ++i) {
      if (OB_FAIL(mgr->get_aux_schemas(tenant_id, data_table_id, aux_schemas, lob_types[i]))) {
        LOG_WARN("fail to get lob schemas", KR(ret), K(tenant_id), K(data_table_id));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < aux_schemas.count(); ++j) {
        if (OB_FAIL(lob_schemas.push_back(aux_schemas.at(j)))) {
          LOG_WARN("fail to append lob schema", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_index_meta_infos_with_data_table_id(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t data_table_id,
    ObIArray<ObAuxTableMetaInfo> &index_meta_infos) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> index_schemas;
  index_meta_infos.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_index_schemas_with_data_table_id(
      guard, tenant_id, data_table_id, index_schemas))) {
    LOG_WARN("fail to get index schemas", KR(ret), K(tenant_id), K(data_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *index_schema = index_schemas.at(i);
      if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", KR(ret), K(tenant_id), K(data_table_id), K(i));
      } else if (OB_FAIL(index_meta_infos.push_back(ObAuxTableMetaInfo(
          index_schema->get_table_id(),
          index_schema->get_table_type(),
          index_schema->get_index_type())))) {
        LOG_WARN("fail to push back index meta info", KR(ret), K(tenant_id), K(data_table_id), K(i));
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, ObIArray<uint64_t> &ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  ids.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get table schemas in tenant failed", KR(ret), K(tenant_id));
  }
  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
    if (OB_ISNULL(*schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ids.push_back((*schema)->get_table_id()))) {
      LOG_WARN("push back table id failed", KR(ret));
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_ids_in_tenant_for_cdc(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, ObIArray<uint64_t> &ids) const
{
  // Original path: same as get_table_ids_in_tenant (mgr enumeration at pinned version).
  return get_table_ids_in_tenant(guard, tenant_id, ids);
}

#define DEFINE_GET_TABLE_IDS_IN_SCOPE(SCOPE)                                         \
int ObOriginalSchemaGetterGuard::get_table_ids_in_##SCOPE(                           \
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t scope_id,               \
    ObIArray<uint64_t> &ids) const                                                    \
{                                                                                     \
  int ret = OB_SUCCESS;                                                               \
  const ObSchemaMgr *mgr = NULL;                                                      \
  ObArray<const ObSimpleTableSchemaV2 *> schemas;                                     \
  ids.reset();                                                                        \
  if (!guard.check_inner_stat()) {                                                    \
    ret = OB_INNER_STAT_ERROR;                                                        \
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == scope_id) {              \
    ret = OB_INVALID_ARGUMENT;                                                        \
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {                  \
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {                        \
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) \
      ret = OB_SUCCESS;                                                               \
  } else if (OB_ISNULL(mgr)) {                                                        \
    ret = OB_SCHEMA_EAGAIN;                                                           \
  } else if (OB_FAIL(mgr->get_table_schemas_in_##SCOPE(tenant_id, scope_id, schemas))) { \
  }                                                                                   \
  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {                                      \
    if (OB_ISNULL(*schema)) ret = OB_ERR_UNEXPECTED;                                  \
    else if (OB_FAIL(ids.push_back((*schema)->get_table_id()))) {}                   \
  }                                                                                   \
  return ret;                                                                         \
}
DEFINE_GET_TABLE_IDS_IN_SCOPE(database)
DEFINE_GET_TABLE_IDS_IN_SCOPE(tablegroup)
#undef DEFINE_GET_TABLE_IDS_IN_SCOPE

int ObOriginalSchemaGetterGuard::get_simple_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schemas.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get table schemas in tenant failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, bool only_view,
    ObIArray<const ObTableSchema *> &table_schemas) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
  }
  FOREACH_CNT_X(simple_schema, schemas, OB_SUCC(ret)) {
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(*simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (only_view && !(*simple_schema)->is_view_table()) {
    } else if (OB_FAIL(guard.get_schema(TABLE_SCHEMA, (*simple_schema)->get_tenant_id(),
                                        (*simple_schema)->get_table_id(), table_schema,
                                        (*simple_schema)->get_schema_version()))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
      LOG_WARN("push back table schema failed", KR(ret));
    }
  }
  return ret;
}

#define DEFINE_GET_FULL_TABLE_SCHEMAS_IN_SCOPE(SCOPE)                                \
int ObOriginalSchemaGetterGuard::get_table_schemas_in_##SCOPE(                       \
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t scope_id,               \
    ObIArray<const ObTableSchema *> &table_schemas) const                             \
{                                                                                     \
  int ret = OB_SUCCESS;                                                               \
  const ObSchemaMgr *mgr = NULL;                                                      \
  ObArray<const ObSimpleTableSchemaV2 *> schemas;                                     \
  table_schemas.reset();                                                              \
  if (!guard.check_inner_stat()) ret = OB_INNER_STAT_ERROR;                          \
  else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == scope_id) ret = OB_INVALID_ARGUMENT; \
  else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {}                   \
  else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {                          \
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) ret = OB_SUCCESS; \
  } else if (OB_ISNULL(mgr)) ret = OB_SCHEMA_EAGAIN;                                 \
  else if (OB_FAIL(mgr->get_table_schemas_in_##SCOPE(tenant_id, scope_id, schemas))) {} \
  FOREACH_CNT_X(simple_schema, schemas, OB_SUCC(ret)) {                               \
    const ObTableSchema *table_schema = NULL;                                         \
    if (OB_ISNULL(*simple_schema)) ret = OB_ERR_UNEXPECTED;                          \
    else if (OB_FAIL(guard.get_schema(TABLE_SCHEMA, (*simple_schema)->get_tenant_id(), \
        (*simple_schema)->get_table_id(), table_schema, (*simple_schema)->get_schema_version()))) {} \
    else if (OB_ISNULL(table_schema)) ret = OB_ERR_UNEXPECTED;                       \
    else if (OB_FAIL(table_schemas.push_back(table_schema))) {}                      \
  }                                                                                   \
  return ret;                                                                         \
}
DEFINE_GET_FULL_TABLE_SCHEMAS_IN_SCOPE(database)
DEFINE_GET_FULL_TABLE_SCHEMAS_IN_SCOPE(tablegroup)
DEFINE_GET_FULL_TABLE_SCHEMAS_IN_SCOPE(tablespace)
#undef DEFINE_GET_FULL_TABLE_SCHEMAS_IN_SCOPE

#define DEFINE_GET_SIMPLE_TABLE_SCHEMAS_IN_SCOPE(SCOPE)                              \
int ObOriginalSchemaGetterGuard::get_simple_table_schemas_in_##SCOPE(                \
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t scope_id,               \
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const                           \
{                                                                                     \
  int ret = OB_SUCCESS;                                                               \
  const ObSchemaMgr *mgr = NULL;                                                      \
  schemas.reset();                                                                    \
  if (!guard.check_inner_stat()) ret = OB_INNER_STAT_ERROR;                          \
  else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == scope_id) ret = OB_INVALID_ARGUMENT; \
  else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {}                   \
  else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {                          \
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) ret = OB_SUCCESS; \
  } else if (OB_ISNULL(mgr)) ret = OB_SCHEMA_EAGAIN;                                 \
  else if (OB_FAIL(mgr->get_table_schemas_in_##SCOPE(tenant_id, scope_id, schemas))) {} \
  return ret;                                                                         \
}
DEFINE_GET_SIMPLE_TABLE_SCHEMAS_IN_SCOPE(database)
DEFINE_GET_SIMPLE_TABLE_SCHEMAS_IN_SCOPE(tablegroup)
#undef DEFINE_GET_SIMPLE_TABLE_SCHEMAS_IN_SCOPE

int ObOriginalSchemaGetterGuard::get_primary_table_schema_in_tablegroup(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablegroup_id,
    const ObSimpleTableSchemaV2 *&schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(mgr->get_primary_table_schema_in_tablegroup(
      tenant_id, tablegroup_id, schema))) {
    LOG_WARN("get primary table schema failed", KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::check_table_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &table_name, bool is_index,
    ObSchemaGetterGuard::CheckTableType check_type, bool &is_exist,
    uint64_t *table_id) const
{
  int ret = OB_SUCCESS;
  uint64_t tmp_table_id = OB_INVALID_ID;
  is_exist = false;
  if (NULL != table_id) {
    *table_id = OB_INVALID_ID;
  }
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_table_id(guard, tenant_id, database_id, table_name, is_index,
                                  check_type, tmp_table_id, false))) {
    LOG_WARN("get table id failed", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_INVALID_ID != tmp_table_id) {
    is_exist = true;
    if (NULL != table_id) {
      *table_id = tmp_table_id;
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::check_table_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  is_exist = false;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_cte_table(table_id)) {
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(get_table_schema_version(
      guard, TABLE_SCHEMA, tenant_id, table_id, schema_version, NULL))) {
    LOG_WARN("get schema version failed", KR(ret), K(tenant_id), K(table_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schema_version(
    ObSchemaGetterGuard &guard, ObSchemaType schema_type, uint64_t tenant_id,
    uint64_t table_id, int64_t &schema_version, uint64_t *database_id) const
{
  UNUSED(schema_type);
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *schema = NULL;
  const bool is_external = is_external_object_id(table_id);
  schema_version = OB_INVALID_VERSION;
  if (is_cte_table(table_id)) {
  } else if (is_external) {
    table_id = OB_ALL_VIRTUAL_PROCESSLIST_TID;
    if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    } else if (OB_FAIL(mgr->get_table_schema(tenant_id, table_id, schema))) {
    }
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, table_id, schema))) {
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    schema_version = schema->get_schema_version();
    if (OB_NOT_NULL(database_id) && !is_external) {
      *database_id = schema->get_database_id();
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_simple_table_schema(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t table_id,
    const ObSimpleTableSchemaV2 *&schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_cte_table(table_id) || is_external_object_id(table_id)) {
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
  } else if (OB_ISNULL(mgr)) {
    if (!ObSchemaService::g_liboblog_mode_) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(guard.get_schema(TABLE_SIMPLE_SCHEMA, tenant_id, table_id, schema))) {
    }
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, table_id, schema))) {
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_schema_count(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_count = 0;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
  } else if (OB_FAIL(mgr->get_schema_count(schema_count))) {
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_schema_size(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, int64_t &schema_size) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_size = 0;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
  } else if (OB_FAIL(mgr->get_schema_size(schema_size))) {
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init simple table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  if (OB_FAIL(get_table_schemas_in_tenant(guard, tenant_id, false /*only_view*/, table_schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_view_schemas_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  if (OB_FAIL(get_table_schemas_in_tenant(guard, tenant_id, true /*only_view*/, table_schemas))) {
    LOG_WARN("fail to get view schemas in tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init table schema iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_database(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  if (OB_FAIL(get_simple_table_schemas_in_database(guard, tenant_id, database_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in database", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init simple table schema iterator",
             KR(ret), K(tenant_id), K(database_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_database(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  if (OB_FAIL(get_table_schemas_in_database(guard, tenant_id, database_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in database", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(database_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablegroup_id,
    ObSimpleTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  if (OB_FAIL(get_simple_table_schemas_in_tablegroup(
          guard, tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup",
             KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init simple table schema iterator",
             KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tablegroup(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablegroup_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  if (OB_FAIL(get_table_schemas_in_tablegroup(guard, tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup",
             KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_in_tablespace(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t tablespace_id,
    ObTableSchemaIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  if (OB_FAIL(get_table_schemas_in_tablespace(guard, tenant_id, tablespace_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablespace",
             KR(ret), K(tenant_id), K(tablespace_id));
  } else if (OB_FAIL(iter.init(table_schemas))) {
    LOG_WARN("fail to init table schema iterator",
             KR(ret), K(tenant_id), K(tablespace_id));
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_tmp_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (tmp_schema->is_tmp_table()) {
        if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
          LOG_WARN("push back table id failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_external_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (tmp_schema->is_external_table()) {
        if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
          LOG_WARN("push back table id failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_duplicate_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    // Align with OnDemand BY_DUPLICATE_SCOPE: duplicate_scope != 0.
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != tmp_schema->get_duplicate_scope()) {
        if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
          LOG_WARN("push back table id failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_encrypted_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTablespaceSchema *> ts_schemas;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  ObArray<uint64_t> encrypted_ts_ids;
  table_ids.reset();
  // Align with OnDemand BY_ENCRYPTED_TABLESPACE: tablespace.encryption_name != ''.
  if (OB_FAIL(guard.get_tablespace_schemas_in_tenant(tenant_id, ts_schemas))) {
    LOG_WARN("fail to get tablespace schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(ts, ts_schemas, OB_SUCC(ret)) {
      const ObTablespaceSchema *ts_schema = *ts;
      if (OB_ISNULL(ts_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(ts_schema));
      } else if (!ts_schema->get_encryption_name().empty()) {
        if (OB_FAIL(encrypted_ts_ids.push_back(ts_schema->get_tablespace_id()))) {
          LOG_WARN("push back tablespace id failed", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !encrypted_ts_ids.empty()) {
    if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
      LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        const ObSimpleTableSchemaV2 *tmp_schema = *schema;
        bool found = false;
        if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
        } else {
          const uint64_t tablespace_id = tmp_schema->get_tablespace_id();
          for (int64_t i = 0; !found && i < encrypted_ts_ids.count(); ++i) {
            if (encrypted_ts_ids.at(i) == tablespace_id) {
              found = true;
            }
          }
          if (found && OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
            LOG_WARN("push back table id failed", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_dynamic_partition_table_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  if (OB_FAIL(get_simple_table_schemas_in_tenant(guard, tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (tmp_schema->with_dynamic_partition_policy()) {
        if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
          LOG_WARN("push back table id failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_tenant_unavailable_index(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  index_ids.reset();
  if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObSchemaLiteScanSpec lite_spec;
    lite_spec.table_spec_.tenant_id_ = tenant_id;
    lite_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
    lite_spec.table_spec_.select_flags_ =
        OB_SCHEMA_SCAN_SELECT_TABLE_ID
        | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
        | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS;
    lite_spec.caller_tag_ = "unavailable_index";
    ObSchemaLiteTableIterator lite_iter;
    const ObSchemaLiteTableResult *row = NULL;
    if (OB_FAIL(guard.scan_lite_tables(lite_spec, lite_iter))) {
      LOG_WARN("fail to scan lite tables in tenant", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(lite_iter.get_next(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next lite table", KR(ret), K(tenant_id));
        }
        break;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, lite row is NULL", KR(ret));
      } else if (INDEX_STATUS_UNAVAILABLE == row->index_status_
                 && row->is_index_table()) {
        if (OB_FAIL(index_ids.push_back(row->table_id_))) {
          LOG_WARN("fail to push back index id", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::check_unavailable_index_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, bool &exist) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObSchemaLiteScanSpec lite_spec;
    lite_spec.table_spec_.tenant_id_ = tenant_id;
    lite_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
    lite_spec.table_spec_.select_flags_ =
        OB_SCHEMA_SCAN_SELECT_TABLE_ID
        | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
        | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS;
    lite_spec.caller_tag_ = "check_unavailable_index";
    ObSchemaLiteTableIterator lite_iter;
    const ObSchemaLiteTableResult *row = NULL;
    if (OB_FAIL(guard.scan_lite_tables(lite_spec, lite_iter))) {
      LOG_WARN("fail to scan lite tables in tenant", KR(ret));
    }
    while (OB_SUCC(ret) && !exist) {
      if (OB_FAIL(lite_iter.get_next(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next lite table", KR(ret), K(tenant_id));
        }
        break;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, lite row is NULL", KR(ret));
      } else if (INDEX_STATUS_UNAVAILABLE == row->index_status_
                 && row->is_index_table()) {
        exist = true;
        LOG_INFO("unavaliale index exist", KR(ret), "table_id", row->table_id_);
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::check_restore_error_index_exist(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, bool &exist) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObSchemaLiteScanSpec lite_spec;
    lite_spec.table_spec_.tenant_id_ = tenant_id;
    lite_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
    lite_spec.table_spec_.select_flags_ =
        OB_SCHEMA_SCAN_SELECT_TABLE_ID
        | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
        | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS;
    lite_spec.caller_tag_ = "check_restore_error_index";
    ObSchemaLiteTableIterator lite_iter;
    const ObSchemaLiteTableResult *row = NULL;
    if (OB_FAIL(guard.scan_lite_tables(lite_spec, lite_iter))) {
      LOG_WARN("fail to scan lite tables in tenant", KR(ret));
    }
    while (OB_SUCC(ret) && !exist) {
      if (OB_FAIL(lite_iter.get_next(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next lite table", KR(ret), K(tenant_id));
        }
        break;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, lite row is NULL", KR(ret));
      } else if (INDEX_STATUS_RESTORE_INDEX_ERROR == row->index_status_
                 && row->is_index_table()) {
        exist = true;
        LOG_INFO("restore error index exist", KR(ret), "table_id", row->table_id_);
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_idx_schema_by_origin_idx_name(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, uint64_t database_id,
    const ObString &index_name, const ObTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *simple_table = NULL;
  schema = NULL;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.check_lazy_guard(tenant_id, mgr))) {
  } else if (OB_FAIL(mgr->get_idx_schema_by_origin_idx_name(
      tenant_id, database_id, index_name, simple_table))) {
  } else if (NULL != simple_table
             && OB_FAIL(guard.get_schema(TABLE_SCHEMA, simple_table->get_tenant_id(),
                                         simple_table->get_table_id(), schema,
                                         simple_table->get_schema_version()))) {
  } else if (NULL != simple_table && OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::generate_tablet_table_map(
    ObSchemaGetterGuard &guard, uint64_t tenant_id,
    hash::ObHashMap<ObTabletID, uint64_t> &tablet_map) const
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;
  int64_t schema_version = OB_INVALID_VERSION;
  const int64_t BUCKET_NUM = 10007;
  ObSimpleTableSchemaIterator table_iter;
  const ObSimpleTableSchemaV2 *table = nullptr;
  if (OB_UNLIKELY(guard.get_tenant_id() != tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant_id is not match with schema_guard", KR(ret), K(guard.get_tenant_id()), K(tenant_id));
  } else if (OB_FAIL(guard.is_lazy_mode(tenant_id, is_lazy))) {
    LOG_WARN("fail to check lazy mode", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(is_lazy)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't generate tablet-table pairs with lazy guard", KR(ret), K(guard.get_tenant_id()), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!ObSchemaService::is_formal_version(schema_version))) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema version is not formal, retry to get schema guard", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(tablet_map.create(BUCKET_NUM, "TabletPairMap", "TabletPairMap", tenant_id))) {
    LOG_WARN("create tablet-table map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_schemas_in_tenant(guard, tenant_id, table_iter))) {
    LOG_WARN("fail to get tables", KR(ret), K(tenant_id));
  }
  while (OB_SUCC(ret)) {
    ObTabletID tablet_id;
    if (OB_FAIL(table_iter.get_next(table))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next table schema", KR(ret), K(tenant_id));
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ptr is null", KR(ret), K(tenant_id));
    } else if (table->has_tablet()) {
      const uint64_t table_id = table->get_table_id();
      ObPartitionSchemaIter iter(*table, ObCheckPartitionMode::CHECK_PARTITION_MODE_ALL);
      while (OB_SUCC(ret) && (OB_SUCC(iter.next_tablet_id(tablet_id)))) {
        if (OB_FAIL(tablet_map.set_refactored(tablet_id, table_id))) {
          LOG_WARN("fail to set tablet-table map", KR(ret), K(tenant_id), K(tablet_id), K(table_id));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("iter tablet failed", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::deep_copy_index_name_map(
    ObSchemaGetterGuard &guard, ObIAllocator &allocator,
    ObIndexNameMap &index_name_cache) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (OB_FAIL(guard.check_lazy_guard(guard.tenant_id_, mgr))) {
  } else if (OB_FAIL(const_cast<ObSchemaMgr *>(mgr)->deep_copy_index_name_map(
      allocator, index_name_cache))) {
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_table_schemas_with_async_index_in_tenant(
    ObSchemaGetterGuard &guard,
    uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_schemas.reset();
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else {
    // The schema mgr is per-tenant, so every entry in the async-index set already
    // belongs to this tenant; just copy the pointers out. Recyclebin tables
    // retain live tablets and need refresh to complete async-index compaction.
    const ObSortedVector<ObSimpleTableSchemaV2 *> &infos = mgr->get_table_with_async_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = infos.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr in async index infos", KR(ret), K(tenant_id), K(i));
      } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
        LOG_WARN("push back table schema failed", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObOriginalSchemaGetterGuard::get_vector_info_index_ids_in_tenant(
    ObSchemaGetterGuard &guard, uint64_t tenant_id, bool &has_ivf_index,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  has_ivf_index = false;
  if (!guard.check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.check_tenant_schema_guard(tenant_id))) {
  } else if (OB_FAIL(guard.get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret && guard.ignore_tenant_not_exist_error(tenant_id)) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(mgr->get_vector_index_schemas_in_tenant(tenant_id, schemas))) {
  }
  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
    if (OB_ISNULL(*schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(table_ids.push_back((*schema)->get_table_id()))) {
    } else if (!has_ivf_index && (*schema)->is_vec_ivf_index()) {
      has_ivf_index = true;
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

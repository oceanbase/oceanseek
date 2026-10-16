/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_sync_catalog_resolver.h"

#include "share/catalog/ob_catalog_properties.h"
#include "sql/resolver/ddl/ob_sync_catalog_stmt.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObSyncCatalogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSyncCatalogStmt *stmt = nullptr;
  const ParseNode *scope_node = nullptr;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_) || OB_ISNULL(allocator_)
      || T_SYNC_CATALOG != parse_tree.type_
      || 1 != parse_tree.num_child_ || OB_ISNULL(parse_tree.children_)
      || OB_ISNULL(scope_node = parse_tree.children_[0])
      || T_RELATION_FACTOR != scope_node->type_ || 2 != scope_node->num_child_
      || OB_ISNULL(scope_node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("catalog sync is not supported in oracle mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync in oracle mode");
  } else if (OB_ISNULL(stmt = create_stmt<ObSyncCatalogStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create catalog sync statement", K(ret));
  } else {
    stmt_ = stmt;
    stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    if (OB_ISNULL(scope_node->children_[0]) && OB_ISNULL(scope_node->children_[1])) {
      stmt->set_sync_scope(ObSyncCatalogStmt::CATALOG_SCOPE);
    } else if (OB_NOT_NULL(scope_node->children_[0])
               && OB_ISNULL(scope_node->children_[1])) {
      stmt->set_sync_scope(ObSyncCatalogStmt::DATABASE_SCOPE);
      if (OB_FAIL(resolve_database(*scope_node->children_[0], *stmt))) {
        LOG_WARN("failed to resolve catalog sync database", K(ret));
      }
    } else if (OB_NOT_NULL(scope_node->children_[0])
               && OB_NOT_NULL(scope_node->children_[1])) {
      stmt->set_sync_scope(ObSyncCatalogStmt::TABLE_SCOPE);
      if (OB_FAIL(resolve_table(
              *scope_node->children_[0], *scope_node->children_[1], *stmt))) {
        LOG_WARN("failed to resolve catalog sync table", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid catalog sync scope node", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resolve_sync_target(*stmt))) {
      LOG_WARN("failed to resolve configured catalog sync target", K(ret));
    }
  }
  return ret;
}

int ObSyncCatalogResolver::resolve_sync_target(ObSyncCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  uint64_t catalog_id = OB_INVALID_ID;
  ObString copied_catalog_name;
  const ObCatalogSchema *catalog_schema = nullptr;
  ObCatalogProperties::CatalogType catalog_type = ObCatalogProperties::CatalogType::INVALID_TYPE;
  ObSchemaGetterGuard *schema_guard = nullptr;
  ObSessionPrivInfo session_priv;
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(*schema_guard, tenant_id, catalog_id))) {
    LOG_WARN("failed to get configured catalog sync target", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == catalog_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "catalog sync without catalog_sync_target");
  } else if (OB_FAIL(
                 schema_guard->get_catalog_schema_by_id(tenant_id, catalog_id, catalog_schema))) {
    LOG_WARN("failed to get catalog schema", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("configured catalog sync target schema is null", K(ret), K(catalog_id));
  } else if (!is_external_catalog_id(catalog_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "internal catalog as catalog sync target");
  } else if (OB_FAIL(ObCatalogProperties::parse_catalog_type(
                 catalog_schema->get_catalog_properties_str(),
                 catalog_type))) {
    LOG_WARN("failed to parse catalog type", K(ret), K(catalog_id));
  } else if (ObCatalogProperties::CatalogType::HMS_TYPE != catalog_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync target other than HMS catalog");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_info_->get_session_priv_info(session_priv))) {
    LOG_WARN("failed to get session privilege", K(ret));
  } else if (OB_FAIL(schema_guard->check_catalog_access(session_priv,
                                                        session_info_->get_enable_role_array(),
                                                        catalog_schema->get_catalog_name_str()))) {
    LOG_WARN("failed to check catalog access",
             K(ret),
             K(catalog_schema->get_catalog_name_str()));
  } else if (OB_FAIL(ob_write_string(*allocator_,
                                     catalog_schema->get_catalog_name_str(),
                                     copied_catalog_name))) {
    LOG_WARN("failed to copy catalog name", K(ret));
  } else {
    stmt.set_catalog_id(catalog_id);
    stmt.set_catalog_name(copied_catalog_name);
  }
  return ret;
}

int ObSyncCatalogResolver::resolve_table(const ParseNode &database_node,
                                              const ParseNode &table_node,
                                              ObSyncCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  ObString copied_table_name;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  const ObTableSchema *table_schema = nullptr;
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (OB_ISNULL(database_node.str_value_) || database_node.str_len_ <= 0
      || OB_ISNULL(table_node.str_value_) || table_node.str_len_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog sync table node", K(ret));
  } else if (OB_FAIL(resolve_database(database_node, stmt))) {
    LOG_WARN("failed to resolve table database", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get connection collation", K(ret));
  } else {
    table_name.assign_ptr(table_node.str_value_, static_cast<int32_t>(table_node.str_len_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(
                 cs_type, OB_LOWERCASE_AND_INSENSITIVE != mode, table_name))) {
    LOG_WARN("failed to check table name", K(ret), K(table_name));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                       OB_INTERNAL_CATALOG_ID,
                                                       stmt.get_database_id(),
                                                       table_name,
                                                       false,
                                                       false,
                                                       false,
                                                       table_schema))) {
    LOG_WARN("failed to get table schema",
             K(ret),
             K(tenant_id),
             K(stmt.get_database_id()),
             K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table does not exist", K(ret), K(stmt.get_database_name()), K(table_name));
  } else if (!table_schema->is_user_table() || table_schema->is_index_table()
             || table_schema->is_aux_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync for non-user table");
  } else if (is_sys_database_id(table_schema->get_database_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync for system database");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_write_string(*allocator_, table_name, copied_table_name))) {
    LOG_WARN("failed to copy table name", K(ret));
  } else {
    stmt.set_table_id(table_schema->get_table_id());
    stmt.set_table_name(copied_table_name);
  }
  return ret;
}

int ObSyncCatalogResolver::resolve_database(const ParseNode &database_node,
                                                 ObSyncCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString copied_database_name;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  uint64_t database_id = OB_INVALID_ID;
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (OB_ISNULL(database_node.str_value_) || database_node.str_len_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog sync database node", K(ret));
  } else {
    database_name.assign_ptr(database_node.str_value_,
                             static_cast<int32_t>(database_node.str_len_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get connection collation", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type,
                                                           OB_LOWERCASE_AND_INSENSITIVE != mode,
                                                           database_name))) {
    LOG_WARN("failed to check database name", K(ret), K(database_name));
  } else if (OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret));
  } else if (OB_FAIL(ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                                    session_info_,
                                                    database_name,
                                                    allocator_))) {
    LOG_WARN("failed to convert database name", K(ret), K(database_name));
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id,
                                                      OB_INTERNAL_CATALOG_ID,
                                                      database_name,
                                                      database_id))) {
    LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(database_name));
  } else if (is_sys_database_id(database_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync for system database");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_write_string(*allocator_, database_name, copied_database_name))) {
    LOG_WARN("failed to copy database name", K(ret));
  } else {
    stmt.set_database_id(database_id);
    stmt.set_database_name(copied_database_name);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

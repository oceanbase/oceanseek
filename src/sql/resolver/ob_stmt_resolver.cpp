/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ob_stmt_resolver.h"

#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/catalog/ob_catalog_utils.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/catalog/ob_unified_catalog_utils.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_uc_catalog_context.h"
#include "sql/table_format/unified/ob_unified_catalog_metadata_provider.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

bool ObStmtResolver::is_uc_remote_catalog(const uint64_t catalog_id) const
{
  return OB_NOT_NULL(params_.uc_catalog_ctx_)
         && params_.uc_catalog_ctx_->is_remote_catalog(catalog_id);
}

int ObStmtResolver::cvt_uc_db_name_to_org(sql::ObSqlSchemaGuard &sql_schema_guard,
                                          const ObUcCatalogContext &uc_catalog_ctx,
                                          const ObSQLSessionInfo *session,
                                          const uint64_t catalog_id,
                                          common::ObString &db_name,
                                          ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(session) || !uc_catalog_ctx.is_remote_catalog(catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid UC database name request", K(ret), K(catalog_id));
  } else if (OB_FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
  } else if (OB_FAIL(sql_schema_guard.get_uc_catalog_database_schema(uc_catalog_ctx,
                                                                     tenant_id,
                                                                     catalog_id,
                                                                     db_name,
                                                                     database_schema))) {
    LOG_WARN("fail to get UC database schema", K(ret), K(tenant_id), K(catalog_id), K(db_name));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    db_name = database_schema->get_database_name();
    if (OB_NOT_NULL(allocator)) {
      OZ(ob_write_string(*allocator, db_name, db_name));
    }
  }
  return ret;
}

int ObStmtResolver::resolve_catalog_database_id(const uint64_t tenant_id,
                                                const uint64_t catalog_id,
                                                const ObString &database_name,
                                                uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  if (is_uc_remote_catalog(catalog_id)) {
    ObSqlSchemaGuard *sql_schema_guard = schema_checker_->get_sql_schema_guard();
    const ObDatabaseSchema *database_schema = NULL;
    if (OB_ISNULL(sql_schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("SQL schema guard is null", K(ret));
    } else {
      database_id = OB_INVALID_ID;
      if (OB_FAIL(sql_schema_guard->get_uc_catalog_database_schema(
              *params_.uc_catalog_ctx_,
              tenant_id,
              catalog_id,
              database_name,
              database_schema))) {
        LOG_WARN("failed to get UC database schema", K(ret));
        LOG_WARN("failed to get UC database ID", K(ret), K(tenant_id), K(catalog_id), K(database_name));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("UC database schema must not be null", K(ret));
        LOG_WARN("failed to get UC database ID", K(ret), K(tenant_id), K(catalog_id), K(database_name));
      } else {
        database_id = database_schema->get_database_id();
      }
    }
  } else if (OB_FAIL(schema_checker_->get_database_id(
                 tenant_id, catalog_id, database_name, database_id))) {
    LOG_WARN("failed to get database ID", K(ret), K(tenant_id), K(catalog_id), K(database_name));
  }
  return ret;
}

uint64_t ObStmtResolver::generate_table_id()
{
  if (NULL != params_.query_ctx_) {
    return params_.query_ctx_->available_tb_id_--;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "query ctx pointer is null");
    return OB_INVALID_ID;
  }
}

int ObStmtResolver::resolve_table_relation_node(const ParseNode *node,
                                                ObString &table_name,
                                                ObString &db_name,
                                                ObString &catalog_name,
                                                bool is_org /*false*/,
                                                bool is_oracle_sys_view,
                                                char **dblink_name_ptr,
                                                int32_t *dblink_name_len,
                                                bool *has_dblink_node)
{
  int ret = OB_SUCCESS;
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  if (OB_FAIL(resolve_table_relation_node_v2(node,
                                             table_name,
                                             db_name,
                                             catalog_name,
                                             is_db_explicit,
                                             is_org,
                                             is_oracle_sys_view,
                                             dblink_name_ptr,
                                             dblink_name_len,
                                             has_dblink_node))) {
    LOG_WARN("failed to resolve table name", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObStmtResolver::resolve_table_relation_node(const ParseNode *node,
                                                ObString &table_name,
                                                ObString &db_name,
                                                bool is_org/*false*/,
                                                bool is_oracle_sys_view,
                                                char **dblink_name_ptr,
                                                int32_t *dblink_name_len,
                                                bool *has_dblink_node)
{
  int ret = OB_SUCCESS;
  ObString catalog_name;
  UNUSED(catalog_name);
  if (OB_FAIL(resolve_table_relation_node(node,
                                             table_name,
                                             db_name,
                                             catalog_name,
                                             is_org,
                                             is_oracle_sys_view,
                                             dblink_name_ptr,
                                             dblink_name_len,
                                             has_dblink_node))) {
    LOG_WARN("failed to resolve table name", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObStmtResolver::resolve_dblink_info(const ParseNode* node, 
                                        char **dblink_name_ptr,
                                        int32_t *dblink_name_len,
                                        bool *has_dblink_node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(has_dblink_node)) {
    *has_dblink_node = false;
    if (node->num_child_ >= 3 && NULL != node->children_[2]) {
      *has_dblink_node = true;
    }
  }
  if (OB_NOT_NULL(dblink_name_ptr) &&
      OB_NOT_NULL(dblink_name_len) &&
      node->num_child_ >= 3 && 
      OB_NOT_NULL(node->children_[2]) && 
      T_DBLINK_NAME == node->children_[2]->type_ &&
      OB_NOT_NULL(node->children_[2]->children_) &&
      2 == node->children_[2]->num_child_ &&
      OB_NOT_NULL(node->children_[2]->children_[0]) &&
      OB_NOT_NULL(node->children_[2]->children_[1])) {
    //Obtaining dblink_name here is not to obtain the dblink name itself, but to determine whether there is an opt_dblink node
    ParseNode *dblink_name_node = node->children_[2];
    if (node->children_[2]->children_[1]->value_) { // dblink name is @!
      *dblink_name_ptr = const_cast<char*>(node->children_[2]->children_[0]->str_value_);
      *dblink_name_len = 1;
    } else { // dblink name is @xxxx or @, @ will be skip before here
      *dblink_name_ptr = const_cast<char*>(node->children_[2]->children_[0]->str_value_);
      *dblink_name_len = static_cast<int32_t>(node->children_[2]->children_[0]->str_len_);
    }
  }
  return ret;
}

int ObStmtResolver::normalize_and_validate_name(common::ObString &name,
                                                ObStmtResolver::ObNameType type,
                                                const stmt::StmtType stmt_type,
                                                const bool is_index_table)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else {
    bool perserve_lettercase = lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    switch (type) {
      case ObNameType::NAME_TYPE_TABLE: {
        if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, name, stmt_type, is_index_table))) {
          if(OB_ERR_TOO_LONG_IDENT != ret && OB_WRONG_TABLE_NAME != ret){
            LOG_WARN("fail to check and convert table name", K(ret), K(name));
          }
          else {
            LOG_INFO("table name is too long, this may cause issues in some cases.", K(ret), K(name));
          }
        }
        break;
      }
      case ObNameType::NAME_TYPE_DATABASE: {
        if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, name))) {
          LOG_WARN("fail to check and convert database name", K(ret), K(name));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected name type", K(type), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_tablename_info(const ParseNode *node,
                                          common::ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret));
  } else if (OB_UNLIKELY(node->num_child_ < 2) || OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect error", K(ret), K(node));
  } else {
    ParseNode *relation_node = node->children_[1];
    int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
    table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
    ObNameType type = ObNameType::NAME_TYPE_TABLE;
    if(OB_FAIL(normalize_and_validate_name(table_name, type))){
      LOG_WARN("fail to normalize and validate table name", K(ret), K(table_name));
    }
  }
  return ret;
}

int ObStmtResolver::resolve_db_and_catalog_info(const ParseNode *node,
                                                common::ObString &db_name,
                                                common::ObString &catalog_name, 
                                                uint64_t &catalog_id, 
                                                bool is_oracle_sys_view,
                                                bool *has_dblink_node,
                                                bool &is_db_explicit,
                                                bool is_org)
{
  int ret = OB_SUCCESS;
  ParseNode *db_node = NULL;
  ParseNode *catalog_node = NULL;
  ObSqlSchemaGuard *sql_schema_guard = schema_checker_->get_sql_schema_guard();
  catalog_id = OB_INTERNAL_CATALOG_ID;
  if(node->num_child_ == 4 && OB_NOT_NULL(node->children_[3])){
    catalog_node = node->children_[3];
    if(OB_NOT_NULL(has_dblink_node) && *has_dblink_node){
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink in catalog is");
    } 
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_NOT_NULL(catalog_node) && params_.is_from_show_resolver_
             && share::ObUnifiedCatalog::is_configured(session_info_->get_effective_tenant_id())) {
    catalog_name.assign_ptr(catalog_node->str_value_,
                            static_cast<ObString::obstr_size_t>(catalog_node->str_len_));
    if (OB_FAIL(resolve_read_catalog_name(session_info_->get_effective_tenant_id(),
                                          catalog_name,
                                          catalog_id))) {
      LOG_WARN("failed to resolve UC Catalog", K(ret), K(catalog_name));
    }
  } else if (is_org || params_.is_resolve_fake_cte_table_) {
    if (OB_FAIL(resolve_table_reference_catalog_node(catalog_node,
                                                     catalog_id,
                                                     catalog_name))) {
      LOG_WARN("fail to resolve catalog node", K(ret), K(catalog_id), K(catalog_name));
    }
  } else if (OB_FAIL(resolve_table_catalog_node(catalog_node,
                                                *node,
                                                catalog_id,
                                                catalog_name))) {
    LOG_WARN("fail to resolve catalog node", K(ret), K(catalog_id), K(catalog_name));
  } else if (is_uc_remote_catalog(catalog_id)
             && !params_.is_from_show_resolver_
             && (OB_ISNULL(get_basic_stmt()) || !get_basic_stmt()->is_select_stmt())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "UC table operation is");
  } else if (OB_UNLIKELY(
                 is_external_catalog_id(catalog_id)
                 && !is_uc_remote_catalog(catalog_id))) {
    share::ObCatalogProperties::CatalogType catalog_type = share::ObCatalogProperties::CatalogType::INVALID_TYPE;
    const ObCatalogSchema *catalog_schema = nullptr;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    CK(OB_NOT_NULL(schema_checker_));
    CK(OB_NOT_NULL(schema_checker_->get_schema_guard()));
    if (OB_FAIL(schema_checker_->get_schema_guard()->get_catalog_schema_by_id(tenant_id, catalog_id, catalog_schema))) {
      LOG_WARN("failed to get catalog schema by id", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_ISNULL(catalog_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog schema is null", K(ret), K(catalog_id));
    } else if (OB_FAIL(share::ObCatalogProperties::parse_catalog_type(catalog_schema->get_catalog_properties_str(), catalog_type))) {
      LOG_WARN("failed to parse catalog type", K(ret));
    } else if (!is_catalog_supported_stmt_(catalog_type)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this operation in catalog is");
    }
  }
  if (OB_FAIL(ret)){
    // do nothing
  } else if (node->num_child_ >= 1 && NULL != node->children_[0]) {
    db_node = node->children_[0];
    is_db_explicit = true;
    int32_t db_len = static_cast<int32_t>(db_node->str_len_);
    db_name.assign_ptr(const_cast<char*>(db_node->str_value_), db_len);
    ObNameType type = ObNameType::NAME_TYPE_DATABASE; 
    if(OB_FAIL(normalize_and_validate_name(db_name, type))){
      LOG_WARN("fail to normalize and validate database name", K(ret), K(db_name));
    } else {
      if (is_uc_remote_catalog(catalog_id)) {
        CK(OB_NOT_NULL(sql_schema_guard));
        OZ(cvt_uc_db_name_to_org(*sql_schema_guard,
                                 *params_.uc_catalog_ctx_,
                                 session_info_,
                                 catalog_id,
                                 db_name,
                                 allocator_));
      } else if (is_external_catalog_id(catalog_id)) {
        CK(OB_NOT_NULL(schema_checker_->get_sql_schema_guard()));
        OZ(ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_sql_schema_guard(),
                                          session_info_,
                                          catalog_id,
                                          db_name,
                                          allocator_));
      } else {
        CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
        OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                          session_info_,
                                          db_name,
                                          allocator_));
      }
    }
  } else {
    if (is_oracle_sys_view) {
      // ObString tmp(OB_ORA_SYS_SCHEMA_NAME); // right code
      ObString tmp("SYS");
      if (OB_FAIL(ob_write_string(*allocator_, tmp, db_name))) {
        LOG_WARN("fail to write db name", K(ret));
      }
    } else if (is_org || params_.is_resolve_fake_cte_table_) {
      db_name = ObString::make_empty_string();
    } else if (session_info_->get_database_name().empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", K(ret));
    } else {
      db_name = session_info_->get_database_name();
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_E(EventTable::EN_CHECK_PROXY_DB_ACCURACY) OB_SUCCESS;
    if (OB_SUCCESS != tmp_ret) {
      const ObString &proxy_db = session_info_->get_proxy_specified_db_name();
      if (!proxy_db.empty() && proxy_db != db_name) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("proxy specified db does not match resolve db",
                  K(ret), K(proxy_db), K(db_name));
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_index_info_particularly(common::ObString &table_name,
                                                    common::ObString &db_name,
                                                    uint64_t &catalog_id)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type = (NULL == get_basic_stmt()) ? stmt::T_NONE : get_basic_stmt()->get_stmt_type();
  bool is_index_table = false;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  const bool is_hidden = session_info_->is_table_name_hidden();
  const bool is_built_in_index = true;
  ObNameType type = ObNameType::NAME_TYPE_TABLE;
  if (is_uc_remote_catalog(catalog_id)) {
    if (OB_FAIL(normalize_and_validate_name(table_name, type, stmt_type, false))) {
      LOG_WARN("fail to check and convert table name", K(ret), K(table_name), K(stmt_type), K(is_index_table));
    }
  } else if (OB_FAIL(schema_checker_->check_table_exists(
                 tenant_id, db_name, table_name, true, is_hidden, is_index_table, catalog_id))) {
    LOG_WARN("fail to check and convert table name", K(tenant_id), K(db_name), K(table_name), K(ret));
  } else if (!is_index_table && // check again
      OB_FAIL(schema_checker_->check_table_exists(tenant_id, db_name, table_name, true, is_hidden, is_index_table, is_built_in_index))) {
    LOG_WARN("fail to check table exist again", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_FAIL(normalize_and_validate_name(table_name, type, stmt_type, is_index_table))) {
    LOG_WARN("fail to check and convert table name", K(ret), K(table_name), K(stmt_type), K(is_index_table));
  }
  return ret;
}

// description: 解析关联表
//
// @param [in] node         与关联表相关的节点
// @param [out] table_name  向 arg 中填充的关联表名称
// @param [out] db_name     向 arg 中填充的关联库名称

// @return oceanbase error code defined in lib/ob_errno.def
int ObStmtResolver::resolve_table_relation_node_v2(const ParseNode *node,
                                                   ObString &table_name,
                                                   ObString &db_name,
                                                   ObString &catalog_name,
                                                   bool &is_db_explicit,
                                                   bool is_org /*false*/,
                                                   bool is_oracle_sys_view,
                                                   char **dblink_name_ptr,
                                                   int32_t *dblink_name_len,
                                                   bool *has_dblink_node,
                                                   uint64_t *resolved_catalog_id)
{
  int ret = OB_SUCCESS;
  is_db_explicit = false;
  uint64_t catalog_id = OB_INTERNAL_CATALOG_ID;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(node));
  } else if (OB_FAIL(resolve_dblink_info(node, dblink_name_ptr, dblink_name_len, has_dblink_node))){
    LOG_WARN("fail to resolve dblink information", K(ret));
  } else if (OB_FAIL(resolve_db_and_catalog_info(node, db_name, catalog_name, catalog_id, is_oracle_sys_view, has_dblink_node, is_db_explicit, is_org))){
    LOG_WARN("fail ti resolve catalog and db information", K(ret));
  } else {
    int tmp_ret = resolve_tablename_info(node, table_name);
    bool can_retry_with_index = (OB_ERR_TOO_LONG_IDENT == tmp_ret || ((session_info_->get_ddl_info().is_ddl() || session_info_->get_ddl_info().is_dummy_ddl_for_inner_visibility()) 
                                && OB_WRONG_TABLE_NAME == tmp_ret));
    if (OB_SUCCESS == tmp_ret){
      // do nothing
    } else if (can_retry_with_index){
      if(OB_FAIL((resolve_index_info_particularly(table_name, db_name, catalog_id)))){
        LOG_WARN("fail to resolve index table", K(ret), K(tmp_ret));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to resolve table name", K(ret), K(table_name));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(resolved_catalog_id)) {
    *resolved_catalog_id = catalog_id;
  }
  return ret;
}

int ObStmtResolver::resolve_dblink_name(const ParseNode *table_node, uint64_t tenant_id, ObString &dblink_name, bool &is_reverse_link, bool &has_dblink_node)
{
  int ret = OB_SUCCESS;
  dblink_name.reset();
  if (!OB_ISNULL(table_node) && table_node->num_child_ > 2 &&
      !OB_ISNULL(table_node->children_) && !OB_ISNULL(table_node->children_[2])) {
    const ParseNode *dblink_node = table_node->children_[2];
    has_dblink_node = true;
    if (!lib::is_oracle_mode()) {
      uint64_t compat_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
      } else if (compat_version < DATA_VERSION_4_2_0_0) { 
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
      }
    }
    
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(resolve_dblink_name(dblink_node, dblink_name, is_reverse_link))) {
        LOG_WARN("failed to resolve dblink name", K(ret));
    }
  }
  return ret;
}


int ObStmtResolver::resolve_dblink_name(const ParseNode *dblink_node, common::ObString &dblink_name, bool &is_reverse_link)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(dblink_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_node is NULL", K(ret));
  } else if (dblink_node->num_child_ == 0) {  // num_child_ == 0 and num_child_ == 2 是针对 mysql 模式
    dblink_name.assign_ptr(dblink_node->str_value_, static_cast<int32_t>(dblink_node->str_len_));
  } else if (dblink_node->num_child_ == 2) {
    if (!OB_ISNULL(dblink_node->children_) && !OB_ISNULL(dblink_node->children_[0])) {
      int32_t dblink_name_len = static_cast<int32_t>(dblink_node->children_[0]->str_len_);
      dblink_name.assign_ptr(dblink_node->children_[0]->str_value_, dblink_name_len);
      if (!OB_ISNULL(dblink_node->children_[1])) {
        is_reverse_link = dblink_node->children_[1]->value_;
      }
    }
  } else if (dblink_node->num_child_ == 4 && !OB_ISNULL(dblink_node->children_) &&
             !OB_ISNULL(dblink_node->children_[0])) {
      ParseNode *database_factor = dblink_node->children_[0];
      ParseNode *opt_domain_list = dblink_node->children_[1];
      ParseNode *opt_connection_qualifier = dblink_node->children_[2];
      ParseNode *opt_reverse_link_flag = dblink_node->children_[3];
      
    // 处理database_factor
    ObSqlString dblink_name_str;
    if (OB_ISNULL(database_factor->str_value_) || database_factor->str_len_ == 0) {
      if (!opt_reverse_link_flag->value_) { // @!
        ret = OB_ERR_DATABASE_LINK_EXPECTED;
        SQL_RESV_LOG(WARN, "miss database link.", K(ret));
        LOG_USER_ERROR(OB_ERR_DATABASE_LINK_EXPECTED);
      }
    } else if (OB_FAIL(dblink_name_str.append(ObString(database_factor->str_len_, database_factor->str_value_)))) {
      LOG_WARN("failed to append database_factor", K(ret));
    }
    
    // 处理opt_domain_list
    if (OB_FAIL(ret)) {
      // pass
    } else if (OB_ISNULL(opt_domain_list)) {
      // pass
    } else if (OB_FAIL(resolve_domain_names_list(opt_domain_list, dblink_name_str))) {
      LOG_WARN("failed to resolve domain names list", K(ret));
    }

    // 处理opt_connection_qualifier
    if (OB_FAIL(ret)) {
      // pass
    } else if (OB_ISNULL(opt_connection_qualifier)) {
      // pass
    } else if (OB_FAIL(resolve_connection_qualifier(opt_connection_qualifier, dblink_name_str))) {
      LOG_WARN("failed to resolve connection qualifier", K(ret));
    }

    if (dblink_name_str.length() > OB_MAX_DBLINK_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(dblink_name_str.length()), dblink_name_str.ptr());
    } else if (!check_database_name_valid(dblink_name_str.string())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "database name is missing a component or invalid");
    } else {
      const int64_t name_len = dblink_name_str.length();
      char *name_buf = static_cast<char *>(allocator_->alloc(name_len + 1));
      if (OB_ISNULL(name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc dblink name buffer", K(ret), K(name_len));
      } else {
        MEMCPY(name_buf, dblink_name_str.ptr(), name_len);
        name_buf[name_len] = '\0';
        dblink_name.assign_ptr(name_buf, static_cast<int32_t>(name_len));
      }
    }

    // 处理opt_reverse_link_flag
    if (OB_SUCC(ret)) {
      is_reverse_link = opt_reverse_link_flag->value_;
    }
  }
  return ret;
}

int ObStmtResolver::resolve_domain_names_list(const ParseNode *domain_names_list, ObSqlString &dblink_name_str)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < domain_names_list->num_child_; i++) {
    if (OB_ISNULL(domain_names_list->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("domain_names_list child is null", K(ret));
    } else if (domain_names_list->children_[i]->str_len_ > OB_MAX_DBLINK_NAME_LENGTH 
               || domain_names_list->children_[i]->str_len_ + dblink_name_str.length() > OB_MAX_DBLINK_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(dblink_name_str.length()), dblink_name_str.ptr());
    } else if (OB_FAIL(dblink_name_str.append("."))) {
        LOG_WARN("failed to append dot", K(ret));
    } else if (OB_FAIL(dblink_name_str.append(ObString(domain_names_list->children_[i]->str_len_, 
                                                        domain_names_list->children_[i]->str_value_)))) {
      LOG_WARN("failed to append domain name", K(ret));
    }
  }
  return ret;
}

int ObStmtResolver::resolve_connection_qualifier(const ParseNode *opt_connection_qualifier, ObSqlString &dblink_name_str)
{
  int ret = OB_SUCCESS;
  if (opt_connection_qualifier->num_child_ != 2 || OB_ISNULL(opt_connection_qualifier->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database link children is null", K(ret));
  } else if (OB_ISNULL(opt_connection_qualifier->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database name is missing a component", K(ret));
  } else {
    ParseNode *part0 = opt_connection_qualifier->children_[0];
    ParseNode *part1 = opt_connection_qualifier->children_[1];
    if (OB_ISNULL(part1)) {
      if (OB_ISNULL(part0->str_value_) || part0->str_len_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database name is missing a component", K(ret));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "database name is missing a component");
      } else if (part0->str_len_ > OB_MAX_DBLINK_NAME_LENGTH 
                 || part0->str_len_ + dblink_name_str.length() > OB_MAX_DBLINK_NAME_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(dblink_name_str.length()), dblink_name_str.ptr());
      } else if (OB_FAIL(dblink_name_str.append("@"))) {
        LOG_WARN("failed to append @", K(ret));
      } else if (OB_FAIL(dblink_name_str.append(ObString(part0->str_len_, part0->str_value_)))) {
        LOG_WARN("failed to append connection qualifier", K(ret));
      }
    } else if (OB_NOT_NULL(part1->str_value_) && part1->str_len_ > 0) {
      if (OB_NOT_NULL(part0->str_value_) && part0->str_len_ > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database name link error", K(ret));
      } else if (part1->str_len_ > OB_MAX_DBLINK_NAME_LENGTH 
                 || part1->str_len_ + dblink_name_str.length() > OB_MAX_DBLINK_NAME_LENGTH) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(dblink_name_str.length()), dblink_name_str.ptr());
      } else if (OB_FAIL(dblink_name_str.append("@"))) {
        LOG_WARN("failed to append @", K(ret));
      } else if (OB_FAIL(dblink_name_str.append(ObString(part1->str_len_, part1->str_value_)))) {
        LOG_WARN("failed to append connection qualifier", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database name link error", K(ret));
    }
  }
  return ret;
}

bool ObStmtResolver::check_database_name_valid(const ObString &dblink_name_str) {
  bool is_valid = true;
  bool has_at = false;
  if (dblink_name_str.length() == 0) {

  } else if (dblink_name_str[0] == '.' || dblink_name_str[0] == '@') {
    is_valid = false;
  } else {
    for (int64_t i = 0; is_valid && i < dblink_name_str.length(); i++) {
      if (dblink_name_str[i] == '.' || dblink_name_str[i] == '@') {
        if (i + 1 < dblink_name_str.length() && (dblink_name_str[i + 1] == '.' || dblink_name_str[i + 1] == '@')) {
          is_valid = false;
        } else if (dblink_name_str[i] == '.' && has_at == true) {
          is_valid = false;
        } else if (dblink_name_str[i] == '@') {
          has_at = true;
        }
      }
    }
  }
  return is_valid;
}

// If catalog_node is null, we will deduce catalog_id/catalog_name from session
// If user is in internal_catalog, we will assign catalog_id = 0, catalog_name = OB_INTERNAL_CATALOG_NAME
// If user specific ParseNode(catalog), we will parse it, and retrieve catalog_id/catalog_name from it
int ObStmtResolver::resolve_catalog_node(const ParseNode *catalog_node, uint64_t &catalog_id, common::ObString &catalog_name)
{
  int ret = OB_SUCCESS;
  // assign default catalog_id and catalog_name first
  catalog_id = OB_INTERNAL_CATALOG_ID;
  catalog_name = is_oracle_mode() ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  const ObCatalogSchema *catalog_schema = NULL;
  if (OB_UNLIKELY(NULL == session_info_ || NULL == allocator_ || NULL == schema_checker_ || NULL == schema_checker_->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FALSE_IT(catalog_id = session_info_->get_current_default_catalog())) {
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (NULL != catalog_node) {
    // if catalog ParseNode is not null, deduce catalog from ParseNode
    ObString catalog_node_str{static_cast<int32_t>(catalog_node->str_len_), const_cast<char *>(catalog_node->str_value_)};
    if (share::ObCatalogUtils::is_internal_catalog_name(catalog_node_str, case_mode)) {
      catalog_id = OB_INTERNAL_CATALOG_ID;
      catalog_name = is_oracle_mode() ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
    } else if (share::ObUnifiedCatalog::is_configured(tenant_id)) {
      catalog_name = catalog_node_str;
      if (OB_FAIL(resolve_read_catalog_name(tenant_id, catalog_name, catalog_id))) {
        LOG_WARN("failed to resolve UC Catalog", K(ret), K(catalog_name));
      }
    } else {
      if (OB_FAIL(schema_checker_->get_schema_guard()->get_catalog_schema_by_name(tenant_id, catalog_node_str, catalog_schema))) {
        LOG_WARN("failed to get catalog schema by name", K(ret), K(tenant_id), K(catalog_node_str));
      } else if (OB_ISNULL(catalog_schema)) {
        ret = OB_CATALOG_NOT_EXIST;
        LOG_USER_ERROR(OB_CATALOG_NOT_EXIST, catalog_node_str.length(), catalog_node_str.ptr());
      } else if (OB_FAIL(ob_write_string(*allocator_, catalog_schema->get_catalog_name(), catalog_name))) {
        // use catalog name from CatalogSchema and deep copy it
        LOG_WARN("deep copy catalog_name failed", K(ret));
      } else {
        catalog_id = catalog_schema->get_catalog_id();
      }
    }
  } else if (is_external_catalog_id(catalog_id)
             || share::ObUnifiedCatalog::is_child_catalog_id(tenant_id, catalog_id)) {
    // if catalog ParseNode is null, deduce catalog from session's catalog_id
    // catalog is not explict specific, use catalog_id from session
    if (OB_FAIL(schema_checker_->get_schema_guard()->get_catalog_schema_by_id(tenant_id, catalog_id, catalog_schema))) {
      LOG_WARN("failed to get catalog schema by name", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_ISNULL(catalog_schema)) {
      ObString access_token;
      if (share::ObUnifiedCatalog::is_child_catalog_id(tenant_id, catalog_id)
          && OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR__UNIFIED_CATALOG_TOKEN,
                                                     access_token))) {
        LOG_WARN("failed to get UC session token", K(ret));
      } else if (access_token.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "current session's catalog is not existed");
      } else {
        const uint64_t selected_child_catalog_id = catalog_id;
        if (OB_FAIL(share::ObUnifiedCatalogUtils::resolve_catalog_name(
                tenant_id,
                access_token,
                selected_child_catalog_id,
                *schema_checker_->get_schema_guard(),
                *allocator_,
                catalog_name))) {
          LOG_WARN("failed to resolve selected UC Catalog name",
                   K(ret),
                   K(selected_child_catalog_id));
        } else if (OB_FAIL(resolve_read_catalog_name(tenant_id, catalog_name, catalog_id))) {
          LOG_WARN("failed to resolve selected UC Catalog", K(ret), K(catalog_name));
        } else if (catalog_id != selected_child_catalog_id
                   && !share::ObCatalogUtils::is_internal_catalog_name(catalog_name, case_mode)) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("selected UC Catalog ID changed",
                   K(ret),
                   K(selected_child_catalog_id),
                   K(catalog_id));
        }
      }
    } else if (OB_FAIL(ob_write_string(*allocator_, catalog_schema->get_catalog_name(), catalog_name))) {
      // use catalog name from CatalogSchema and deep copy it
      LOG_WARN("deep copy catalog_name failed", K(ret));
    } else if (share::ObUnifiedCatalog::is_configured(tenant_id)
               && (params_.is_from_show_resolver_ || OB_ISNULL(get_basic_stmt())
                   || !get_basic_stmt()->is_select_stmt())
               && OB_FAIL(resolve_read_catalog_name(tenant_id, catalog_name, catalog_id))) {
      LOG_WARN("failed to resolve selected Catalog", K(ret), K(catalog_name));
    }
  } else {
    // do nothing
    // use INTERNAL_CATALOG_ID and INTERNAL_CATALOG_NAME
  }
  return ret;
}

bool ObStmtResolver::is_sync_target(const uint64_t tenant_id,
                                       const ObString &catalog_name)
{
  bool is_target = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid() && share::ObUnifiedCatalog::is_configured(tenant_id)) {
    const char *target = tenant_config->catalog_sync_target.str();
    is_target = OB_NOT_NULL(target) && '\0' != target[0]
                && catalog_name == ObString(target);
  }
  return is_target;
}

int ObStmtResolver::resolve_selected_catalog_identity(const uint64_t tenant_id,
                                                      uint64_t &catalog_id,
                                                      ObString &catalog_name,
                                                      ObString &access_token,
                                                      bool &is_remote_uc_catalog)
{
  int ret = OB_SUCCESS;
  const ObCatalogSchema *catalog_schema = NULL;
  is_remote_uc_catalog = false;
  access_token.reset();
  catalog_id = session_info_->get_current_default_catalog();
  catalog_name = is_oracle_mode() ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
  if (is_external_catalog_id(catalog_id)
      || share::ObUnifiedCatalog::is_child_catalog_id(tenant_id, catalog_id)) {
    if (OB_FAIL(schema_checker_->get_schema_guard()->get_catalog_schema_by_id(
            tenant_id, catalog_id, catalog_schema))) {
      LOG_WARN("failed to get selected Catalog schema", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_NOT_NULL(catalog_schema)) {
      if (OB_FAIL(ob_write_string(*allocator_, catalog_schema->get_catalog_name(), catalog_name))) {
        LOG_WARN("failed to copy selected Catalog name", K(ret), K(catalog_id));
      }
    } else {
      if (share::ObUnifiedCatalog::is_child_catalog_id(tenant_id, catalog_id)
          && OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR__UNIFIED_CATALOG_TOKEN,
                                                     access_token))) {
        LOG_WARN("failed to get UC session token", K(ret));
      } else if (access_token.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "current session's catalog is not existed");
      } else if (OB_FAIL(share::ObUnifiedCatalogUtils::resolve_catalog_name(
                     tenant_id,
                     access_token,
                     catalog_id,
                     *schema_checker_->get_schema_guard(),
                     *allocator_,
                     catalog_name))) {
        LOG_WARN("failed to resolve selected UC Catalog name",
                 K(ret),
                 K(tenant_id),
                 K(catalog_id));
      } else {
        is_remote_uc_catalog = true;
      }
    }
  }
  return ret;
}

int ObStmtResolver::bind_selected_uc_catalog(const ObString &access_token,
                                             const ObString &catalog_name,
                                             const uint64_t catalog_id)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "UC Catalog in Oracle mode is");
  } else if (OB_ISNULL(params_.uc_catalog_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("UC Catalog context is not initialized", K(ret));
  } else if (OB_FAIL(params_.uc_catalog_ctx_->set_remote_catalog(*allocator_,
                                                                  access_token,
                                                                  catalog_name,
                                                                  catalog_id))) {
    LOG_WARN("failed to save selected UC Catalog context", K(ret), K(catalog_name));
  }
  return ret;
}

// Original-name and fake CTE paths do not probe a Unified Catalog table type.
int ObStmtResolver::resolve_table_reference_catalog_node(const ParseNode *catalog_node,
                                                         uint64_t &catalog_id,
                                                         ObString &catalog_name)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObString selected_access_token;
  bool is_selected_remote_uc_catalog = false;
  const bool is_explicit_catalog = OB_NOT_NULL(catalog_node);
  if (OB_UNLIKELY(NULL == session_info_ || NULL == allocator_ || NULL == schema_checker_
                  || NULL == schema_checker_->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (params_.is_from_show_resolver_ || OB_ISNULL(get_basic_stmt())
             || !get_basic_stmt()->is_select_stmt()) {
    if (OB_FAIL(resolve_catalog_node(catalog_node, catalog_id, catalog_name))) {
      LOG_WARN("failed to resolve table reference Catalog", K(ret), K(catalog_name));
    }
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else {
    if (is_explicit_catalog) {
      catalog_name.assign_ptr(catalog_node->str_value_,
                              static_cast<ObString::obstr_size_t>(catalog_node->str_len_));
    } else if (OB_FAIL(resolve_selected_catalog_identity(tenant_id,
                                                         catalog_id,
                                                         catalog_name,
                                                         selected_access_token,
                                                         is_selected_remote_uc_catalog))) {
      LOG_WARN("failed to resolve selected Catalog identity", K(ret), K(catalog_id));
    }
    if (OB_SUCC(ret)) {
      const bool is_explicit_non_internal_catalog = is_explicit_catalog
                                                    && !share::ObCatalogUtils::is_internal_catalog_name(
                                                        catalog_name, case_mode);
      if ((is_explicit_non_internal_catalog || is_selected_remote_uc_catalog)
          && is_sync_target(tenant_id, catalog_name)) {
        catalog_id = OB_INTERNAL_CATALOG_ID;
        catalog_name = is_oracle_mode() ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
      } else if (is_explicit_catalog
                 && OB_FAIL(resolve_catalog_node(catalog_node, catalog_id, catalog_name))) {
        LOG_WARN("failed to resolve table reference Catalog", K(ret), K(catalog_name));
      } else if (is_selected_remote_uc_catalog
                 && OB_FAIL(bind_selected_uc_catalog(selected_access_token,
                                                     catalog_name,
                                                     catalog_id))) {
        LOG_WARN("failed to bind selected UC Catalog", K(ret), K(catalog_name));
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_table_catalog_node(const ParseNode *catalog_node,
                                               const ParseNode &relation_node,
                                               uint64_t &catalog_id,
                                               ObString &catalog_name)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObSyncTargetReadRoute route = ObSyncTargetReadRoute::INVALID;
  ObString selected_access_token;
  bool is_selected_remote_uc_catalog = false;
  const bool is_explicit_catalog = OB_NOT_NULL(catalog_node);
  if (OB_UNLIKELY(NULL == session_info_ || NULL == allocator_ || NULL == schema_checker_
                  || NULL == schema_checker_->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (params_.is_from_show_resolver_ || OB_ISNULL(get_basic_stmt())
             || !get_basic_stmt()->is_select_stmt()) {
    if (OB_FAIL(resolve_catalog_node(catalog_node, catalog_id, catalog_name))) {
      LOG_WARN("failed to resolve table Catalog", K(ret), K(catalog_name));
    }
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else {
    if (is_explicit_catalog) {
      catalog_name.assign_ptr(catalog_node->str_value_,
                              static_cast<ObString::obstr_size_t>(catalog_node->str_len_));
    } else if (OB_FAIL(resolve_selected_catalog_identity(tenant_id,
                                                         catalog_id,
                                                         catalog_name,
                                                         selected_access_token,
                                                         is_selected_remote_uc_catalog))) {
      LOG_WARN("failed to resolve selected Catalog identity", K(ret), K(catalog_id));
    }
    if (OB_SUCC(ret)) {
      const bool is_explicit_non_internal_catalog = is_explicit_catalog
                                                    && !share::ObCatalogUtils::is_internal_catalog_name(
                                                        catalog_name, case_mode);
      const bool is_sync_target_catalog = (is_explicit_non_internal_catalog
                                           || is_selected_remote_uc_catalog)
                                          && is_sync_target(tenant_id, catalog_name);
      if (!is_sync_target_catalog) {
        if (is_explicit_catalog
            && OB_FAIL(resolve_catalog_node(catalog_node, catalog_id, catalog_name))) {
          LOG_WARN("failed to resolve table Catalog", K(ret), K(catalog_name));
        } else if (is_selected_remote_uc_catalog
                   && OB_FAIL(bind_selected_uc_catalog(selected_access_token,
                                                       catalog_name,
                                                       catalog_id))) {
          LOG_WARN("failed to bind selected UC Catalog", K(ret), K(catalog_name));
        }
      } else if (OB_FAIL(resolve_sync_target_read_route(tenant_id,
                                                        catalog_name,
                                                        relation_node,
                                                        case_mode,
                                                        route))) {
        LOG_WARN("failed to resolve sync target query route",
                 K(ret),
                 K(tenant_id),
                 K(catalog_name));
      } else if (ObSyncTargetReadRoute::INTERNAL_CATALOG == route) {
        catalog_id = OB_INTERNAL_CATALOG_ID;
        catalog_name = is_oracle_mode() ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
      } else if (ObSyncTargetReadRoute::UNIFIED_CATALOG == route) {
        if (is_explicit_catalog) {
          if (OB_FAIL(resolve_read_catalog_name(tenant_id, catalog_name, catalog_id))) {
            LOG_WARN("failed to resolve UC Catalog", K(ret), K(catalog_name));
          } else if (!is_uc_remote_catalog(catalog_id)) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("sync target query did not resolve to a UC Catalog",
                     K(ret),
                     K(tenant_id),
                     K(catalog_name),
                     K(catalog_id));
          }
        } else if (OB_FAIL(bind_selected_uc_catalog(selected_access_token,
                                                    catalog_name,
                                                    catalog_id))) {
          LOG_WARN("failed to bind selected UC Catalog", K(ret), K(catalog_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sync target query route", K(ret), K(tenant_id), K(catalog_name));
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_sync_target_read_route(const uint64_t tenant_id,
                                                   const ObString &catalog_name,
                                                   const ParseNode &relation_node,
                                                   const ObNameCaseMode case_mode,
                                                   ObSyncTargetReadRoute &route)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode() || (session_info_->is_inner() && !session_info_->is_user_session())) {
    route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
  } else {
    ObString access_token;
    if (OB_FAIL(
            session_info_->get_sys_variable(share::SYS_VAR__UNIFIED_CATALOG_TOKEN, access_token))) {
      LOG_WARN("failed to get sync target query token", K(ret), K(tenant_id), K(catalog_name));
    } else if (access_token.empty()) {
      route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
    } else {
      ObString db_name = session_info_->get_database_name();
      ObString table_name;
      if (relation_node.num_child_ >= 1 && OB_NOT_NULL(relation_node.children_[0])) {
        const ParseNode *db_node = relation_node.children_[0];
        db_name.assign_ptr(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
        if (OB_FAIL(normalize_and_validate_name(db_name, ObNameType::NAME_TYPE_DATABASE))) {
          LOG_WARN("failed to resolve sync target query database",
                   K(ret),
                   K(catalog_name),
                   K(db_name));
        }
      }
      if (OB_SUCC(ret)) {
        if (db_name.empty()) {
          route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
        } else if (OB_FAIL(resolve_tablename_info(&relation_node, table_name))) {
          if (OB_ERR_TOO_LONG_IDENT == ret
              || ((session_info_->get_ddl_info().is_ddl()
                   || session_info_->get_ddl_info().is_dummy_ddl_for_inner_visibility())
                  && OB_WRONG_TABLE_NAME == ret)) {
            // Preserve the existing local index-name retry.
            ret = OB_SUCCESS;
            route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
          } else {
            LOG_WARN("failed to resolve sync target query table",
                     K(ret),
                     K(catalog_name),
                     K(db_name),
                     K(table_name));
          }
        } else {
          uint64_t system_database_id = OB_INVALID_ID;
          bool is_system_table = false;
          if (0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
            system_database_id = OB_SYS_DATABASE_ID;
          } else if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)) {
            system_database_id = OB_INFORMATION_SCHEMA_ID;
          } else if (ObCharset::case_mode_equal(case_mode,
                                                db_name,
                                                ObString::make_string(OB_MYSQL_SCHEMA_NAME))) {
            system_database_id = OB_MYSQL_SCHEMA_ID;
          }
          if (OB_INVALID_ID != system_database_id
              && OB_FAIL(ObSysTableChecker::is_sys_table_name(tenant_id,
                                                              system_database_id,
                                                              table_name,
                                                              is_system_table))) {
            LOG_WARN("failed to identify native system table",
                     K(ret),
                     K(tenant_id),
                     K(catalog_name),
                     K(db_name),
                     K(table_name));
          } else if (is_system_table) {
            route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
          } else {
            share::ObUnifiedCatalogQueryTableKind query_table_kind;
            if (OB_FAIL(ObUnifiedCatalogMetadataProvider::classify_query_table(tenant_id,
                                                                               access_token,
                                                                               catalog_name,
                                                                               db_name,
                                                                               table_name,
                                                                               *allocator_,
                                                                               query_table_kind))) {
              if (OB_HTTP_NOT_FOUND == ret) {
                // Local indexes, synonyms, and auxiliary tables need not have UC entries.
                ret = OB_SUCCESS;
                route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
              } else {
                LOG_WARN("failed to identify sync target query table",
                         K(ret),
                         K(tenant_id),
                         K(catalog_name),
                         K(db_name),
                         K(table_name));
              }
            } else if (share::ObUnifiedCatalogQueryTableKind::LOCAL_OB == query_table_kind) {
              route = ObSyncTargetReadRoute::INTERNAL_CATALOG;
            } else if (share::ObUnifiedCatalogQueryTableKind::ICEBERG == query_table_kind) {
              route = ObSyncTargetReadRoute::UNIFIED_CATALOG;
            } else if (share::ObUnifiedCatalogQueryTableKind::NON_LOCAL_OB == query_table_kind) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("non-local UC native OB table query is not supported",
                       K(ret),
                       K(tenant_id),
                       K(catalog_name),
                       K(db_name),
                       K(table_name));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid UC query table kind",
                       K(ret),
                       K(tenant_id),
                       K(catalog_name),
                       K(db_name),
                       K(table_name));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_read_catalog_name(const uint64_t tenant_id,
                                              ObString &catalog_name,
                                              uint64_t &catalog_id)
{
  int ret = OB_SUCCESS;
  catalog_id = OB_INVALID_ID;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObString access_token;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("read Catalog resolver is not initialized", K(ret));
  } else if (!share::ObUnifiedCatalog::is_configured(tenant_id)) {
    if (OB_FAIL(schema_checker_
                    ->get_catalog_id_name(tenant_id, catalog_name, catalog_id, allocator_))) {
      LOG_WARN("failed to resolve local Catalog", K(ret), K(catalog_name));
    }
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (share::ObCatalogUtils::is_internal_catalog_name(catalog_name, case_mode)) {
    catalog_id = OB_INTERNAL_CATALOG_ID;
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR__UNIFIED_CATALOG_TOKEN, access_token))) {
    LOG_WARN("failed to get UC session token", K(ret));
  } else if (access_token.empty()) {
    if (OB_FAIL(schema_checker_->get_catalog_id_name(tenant_id, catalog_name, catalog_id, allocator_))) {
      LOG_WARN("failed to resolve local Catalog", K(ret), K(catalog_name));
    }
  } else if (is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "UC Catalog in Oracle mode is");
  } else {
    share::schema::ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is missing for UC Catalog resolution", K(ret));
    } else if (OB_NOT_NULL(params_.uc_catalog_ctx_)
               && params_.uc_catalog_ctx_->matches_identity(access_token, catalog_name)) {
      catalog_id = params_.uc_catalog_ctx_->get_child_catalog_id();
    } else if (OB_FAIL(share::ObUnifiedCatalogUtils::resolve_catalog_id(tenant_id,
                                                                        access_token,
                                                                        catalog_name,
                                                                        *schema_guard,
                                                                        *allocator_,
                                                                        catalog_id))) {
      if (OB_CATALOG_NOT_EXIST == ret) {
        if (OB_FAIL(schema_checker_->get_catalog_id_name(tenant_id, catalog_name, catalog_id, allocator_))) {
          LOG_WARN("failed to resolve local Catalog", K(ret), K(catalog_name));
        }
      } else {
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Unified Catalog name conflicts with local Catalog");
        }
        LOG_WARN("failed to resolve remote UC Catalog", K(ret), K(catalog_name));
      }
    } else if (OB_ISNULL(params_.uc_catalog_ctx_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("UC Catalog context is not initialized", K(ret));
    } else if (OB_FAIL(params_.uc_catalog_ctx_->set_remote_catalog(*allocator_,
                                                                   access_token,
                                                                   catalog_name,
                                                                   catalog_id))) {
      LOG_WARN("failed to save remote UC Catalog context", K(ret), K(catalog_name));
    }
  }
  return ret;
}

bool ObStmtResolver::is_catalog_supported_stmt_(share::ObCatalogProperties::CatalogType catalog_type)
{
  bool is_supported = true;
  stmt::StmtType stmt_type;
  if (OB_ISNULL(stmt_)) {
    // do nothing
    // 比如 desc catalog.db.tbl，此时这里的 stmt_ 还没有被设置
  } else if (OB_FALSE_IT(stmt_type = stmt_->get_stmt_type())) {
  } else if (ObStmt::is_dml_stmt(stmt_type)) {
    is_supported = ObStmt::is_catalog_supported_dml_stmt(stmt_type, catalog_type);
  } else if (ObStmt::is_ddl_stmt(stmt_type, stmt_->has_global_variable())) {
    is_supported = ObStmt::is_catalog_supported_ddl_stmt(stmt_type, stmt_->has_global_variable());
  }
  return is_supported;
}

int ObStmtResolver::resolve_ref_factor(const ParseNode *node,
                                       ObSQLSessionInfo *session_info, 
                                       ObString &table_name, 
                                       ObString &db_name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_ISNULL(node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(node), K(session_info), K(ret));
  } else {
    ParseNode *db_node = node->children_[0];
    ParseNode *relation_node = node->children_[1];
    int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
    table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(session_info->get_name_case_mode(mode))) {
      SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
    } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else {
      bool perserve_lettercase = lib::is_oracle_mode() ?
          true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
      if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, table_name))) {
        LOG_WARN("fail to check and convert relation name", K(table_name), K(ret));
      } else {
        if (NULL == db_node) {
          if (session_info->get_database_name().empty()) {
            ret = OB_ERR_NO_DB_SELECTED;
            LOG_WARN("No database selected");
          } else {
            db_name = session_info->get_database_name();
          }
        } else {
          int32_t db_len = static_cast<int32_t>(db_node->str_len_);
          db_name.assign_ptr(const_cast<char*>(db_node->str_value_), db_len);
          if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
            LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool is_internal_catalog_query = true;
      // use catalog name from ParseNode if explict
      // if catalog name in ParseNode is not specific, we will deduce catalog name from session's catalog id
      // this function only support non-catalog query now, if user query catalog's resources, we will return OB_NOT_SUPPORTED
      if (node->num_child_ >= 4) {
        ParseNode *catalog_node = node->children_[3];
        ObString catalog_name;
        catalog_name.assign_ptr(catalog_node->str_value_, catalog_node->str_len_);
        if (!share::ObCatalogUtils::is_internal_catalog_name(catalog_name, mode)) {
          is_internal_catalog_query = false;
        }
      } else {
        if (!is_internal_catalog_id(session_info->get_current_default_catalog())) {
          is_internal_catalog_query = false;
        }
      }

      if (OB_SUCC(ret) && !is_internal_catalog_query) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support in external catalog", K(ret));
        LOG_USER_WARN(OB_NOT_SUPPORTED, "This operation is catalog is");
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_database_factor(const ParseNode *node,
                                            const uint64_t tenant_id,
                                            const uint64_t catalog_id,
                                            uint64_t &database_id,
                                            ObString &db_name)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is NULL", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type is not T_IDENT", K(ret), K(node->type_));
  } else if (FALSE_IT(db_name.assign_ptr(const_cast<char*>(node->str_value_),
                                  static_cast<int32_t>(node->str_len_)))) {
    // won't be here
  } else if (OB_FAIL(resolve_catalog_database_id(tenant_id, catalog_id, db_name, database_id))) {
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
  }
  return ret;
}

int ObStmtResolver::normalize_table_or_database_names(ObString &name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode;
  if (name.empty() || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name is empty", K(name), K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (lib::is_oracle_mode()) {
    /* ^-^ */
  } else if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
  }
  
  return ret;
}

int ObSynonymChecker::add_synonym_id(uint64_t synonym_id, uint64_t database_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < synonym_ids_.count(); ++i) {
    if (OB_UNLIKELY(synonym_id == synonym_ids_.at(i))) {
      ret = OB_ERR_LOOP_OF_SYNONYM;
      LOG_WARN("looping chain of synonyms", K(synonym_id), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(synonym_ids_.push_back(synonym_id))) {
    LOG_WARN("fail to add synonym_id", K(synonym_id), K(ret));
  } else if (OB_FAIL(database_ids_.push_back(database_id))) {
    LOG_WARN("fail to add database id", K(database_id), K(ret));
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id,
    const ObString &column_name,
    const share::schema::ObColumnSchemaV2 *&column_schema,
    const bool get_hidden /* = false */,
    bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool is_rowid_col =
      lib::is_oracle_mode()
      && ObCharset::case_insensitive_equal(column_name, OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
    const bool hidden = is_rowid_col || get_hidden || session_info_->is_inner();

    // generated column added by function-based index is hidden in OB but can be selected in oracle
    if (OB_FAIL(schema_checker_->get_column_schema(
        session_info_->get_effective_tenant_id(), table_id, column_name, column_schema, true, is_link))) {
      LOG_WARN("fail to get column schema", K(table_id), K(column_name), K(ret));
    } else if (!hidden && column_schema->is_hidden() && !column_schema->is_generated_column() && !column_schema->is_udt_hidden_column()) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_INFO("do not get hidden column", K(table_id), K(column_name), K(ret));
    }
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id,
    const uint64_t column_id,
    const share::schema::ObColumnSchemaV2 *&column_schema,
    const bool get_hidden /* = false */,
    bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool hidden = get_hidden || session_info_->is_inner();
    if (OB_FAIL(schema_checker_->get_column_schema(session_info_->get_effective_tenant_id(), table_id, column_id, column_schema, hidden, is_link))) {
      SQL_RESV_LOG(WARN, "get_column_schema failed", K(ret), K(session_info_->get_effective_tenant_id()), K(table_id), K(column_id), K(hidden), K(is_link));
    }
  }
  return ret;
}

int ObStmtResolver::check_table_name_equal(const ObString &name1, const ObString &name2, bool &equal)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  equal = false;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (OB_NAME_CASE_INVALID == case_mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected name case mode", K(ret));
  } else {
    equal = ObCharset::case_mode_equal(case_mode, name1, name2);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

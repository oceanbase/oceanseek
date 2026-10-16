/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_rls_policy_resolver_helper.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObRlsPolicyResolverHelper::normalize_policy_name(ObSQLSessionInfo &session_info,
                                                     ObIAllocator &allocator,
                                                     ObString &name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  if (!lib::is_mysql_mode()) {
    // do nothing
  } else if (OB_FAIL(session_info.get_name_case_mode(case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else if (OB_LOWERCASE_AND_INSENSITIVE == case_mode
             && OB_FAIL(ObCharset::tolower(cs_type, name, name, allocator))) {
    LOG_WARN("failed to lowercase policy name", K(ret), K(name));
  }
  return ret;
}

int ObRlsPolicyResolverHelper::resolve_policy_name(const ParseNode *name_node,
                                                   ObSQLSessionInfo &session_info,
                                                   ObIAllocator &allocator,
                                                   ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node is null", K(ret));
  } else {
    ObString policy_name(static_cast<int32_t>(name_node->str_len_),
                         name_node->str_value_);
    if (OB_FAIL(normalize_policy_name(session_info, allocator, policy_name))) {
      LOG_WARN("failed to normalize policy name", K(ret), K(policy_name));
    } else if (OB_FAIL(schema.set_policy_name(policy_name))) {
      LOG_WARN("failed to set policy name", K(ret), K(policy_name));
    }
  }
  return ret;
}

int ObRlsPolicyResolverHelper::resolve_table(const ParseNode *table_node,
                                             ObStmtResolver &resolver,
                                             ObSQLSessionInfo &session_info,
                                             ObSchemaChecker &schema_checker,
                                             ObRlsPolicyDDLStmt &rls_stmt,
                                             ObRlsPolicySchema &schema,
                                             const ObTableSchema *&out_table_schema)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  ObString db_name;
  const ObTableSchema *table_schema = NULL;
  uint64_t tenant_id = session_info.get_effective_tenant_id();
  schema.set_tenant_id(tenant_id);
  if (OB_ISNULL(resolver.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver allocator is null", K(ret));
  } else if (OB_FAIL(resolver.resolve_table_relation_node(table_node, table_name, db_name))) {
    LOG_WARN("failed to resolve table relation node", K(ret));
  } else if (db_name.empty() && FALSE_IT(db_name = session_info.get_database_name())) {
  } else if (OB_FAIL(schema_checker.get_table_schema(tenant_id, db_name, table_name,
                                                      false/*is_index*/, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(db_name), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(db_name), helper.convert(table_name));
  } else {
    rls_stmt.set_database_name(db_name);
    rls_stmt.set_table_name(table_name);
    rls_stmt.set_is_sys_table(table_schema->is_sys_table());
    schema.set_table_id(table_schema->get_table_id());
    out_table_schema = table_schema;
  }
  return ret;
}

int ObRlsPolicyResolverHelper::resolve_role_list(const ParseNode *role_list_node,
                                                 ObSchemaChecker &schema_checker,
                                                 ObSQLSessionInfo &session_info,
                                                 ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(role_list_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("role list node is null", K(ret));
  } else {
    uint64_t tenant_id = session_info.get_effective_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < role_list_node->num_child_; ++i) {
      if (OB_ISNULL(role_list_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("role node is null", K(ret), K(i));
      } else {
        ObString role_name(static_cast<int32_t>(role_list_node->children_[i]->str_len_),
                           role_list_node->children_[i]->str_value_);
        if (0 == role_name.case_compare("public")) {
          // PUBLIC means the policy applies to all users; clear any previously added
          // role IDs and stop processing the list.
          schema.get_role_ids().reset();
          break;
        }
        const share::schema::ObUserInfo *role_info = NULL;
        if (OB_FAIL(schema_checker.get_user_info(tenant_id,
                role_name,
                ObString::make_string(OB_DEFAULT_HOST_NAME),
                role_info))) {
          if (OB_USER_NOT_EXIST == ret) {
            ret = OB_ROLE_NOT_EXIST;
            LOG_USER_ERROR(OB_ROLE_NOT_EXIST, role_name.length(), role_name.ptr());
          }
          LOG_WARN("failed to get role info", K(ret), K(role_name));
        } else if (OB_ISNULL(role_info) || !role_info->is_role()) {
          ret = OB_ROLE_NOT_EXIST;
          LOG_USER_ERROR(OB_ROLE_NOT_EXIST, role_name.length(), role_name.ptr());
          LOG_WARN("role does not exist or is not a role", K(ret), K(role_name));
        } else if (OB_FAIL(schema.add_role_id(role_info->get_user_id()))) {
          LOG_WARN("failed to add role id", K(ret), K(role_info->get_user_id()));
        }
      }
    }
  }
  return ret;
}

int ObRlsPolicyResolverHelper::resolve_stmt_type(const ParseNode *stmt_type_node,
                                                 ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  int64_t cmd_value = 0;
  if (NULL != stmt_type_node) {
    cmd_value = stmt_type_node->value_;
  }
  int64_t stmt_type_bitmask = 0;
  switch (cmd_value) {
    case 0: // ALL
      stmt_type_bitmask = RLS_POLICY_SELECT_FLAG | RLS_POLICY_INSERT_FLAG
                          | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG;
      break;
    case 1: stmt_type_bitmask = RLS_POLICY_SELECT_FLAG; break;
    case 2: stmt_type_bitmask = RLS_POLICY_INSERT_FLAG; break;
    case 3: stmt_type_bitmask = RLS_POLICY_UPDATE_FLAG; break;
    case 4: stmt_type_bitmask = RLS_POLICY_DELETE_FLAG; break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cmd type value", K(ret), K(cmd_value));
      break;
  }
  if (OB_SUCC(ret)) {
    schema.set_stmt_type(stmt_type_bitmask);
  }
  return ret;
}

int ObRlsPolicyResolverHelper::resolve_rls_predicate_expr(const ParseNode *expr_node,
                                                          ObResolverParams &params,
                                                          const ObTableSchema &table_schema,
                                                          const ObString &db_name,
                                                          const ObString &table_name,
                                                          ObString &result_str)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  ObArray<ObQualifiedName> columns;
  ObArray<ObRawExpr*> real_exprs;
  ObArray<ObRawExpr*> ref_exprs;
  ObRawExprFactory *expr_factory = params.expr_factory_;
  ObSQLSessionInfo *session_info = params.session_info_;
  ObSchemaChecker *schema_checker = params.schema_checker_;
  ObQueryCtx *query_ctx = params.query_ctx_;
  ObIAllocator *allocator = params.allocator_;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  ObRawExpr *real_ref_expr = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(expr_node) || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)
      || OB_ISNULL(schema_checker) || OB_ISNULL(allocator) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(expr_node), KP(expr_factory), KP(session_info), KP(schema_checker), KP(allocator), KP(query_ctx));
  } else if (OB_FAIL(ObRawExprUtils::build_rls_predicate_expr(*expr_node,
                                                              *expr_factory,
                                                              *session_info,
                                                              columns,
                                                              expr))) {
    LOG_WARN("failed to build rls predicate expr", K(ret));
  } else if (OB_FAIL(session_info->get_name_case_mode(name_case_mode))) {
    LOG_WARN("failed to get name case mode", K(ret));
  } else {
    tenant_id = session_info->get_effective_tenant_id();
  }
  // resolve column references and system functions
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      real_ref_expr = q_name.access_idents_.at(0).sys_func_expr_;
      if (OB_ISNULL(real_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys func expr is null", K(ret));
      }
    } else { // is column
      const ObColumnSchemaV2 *col_schema = NULL;
      ObDatabaseSchemaHashWrapper q_db_wrapper(tenant_id, name_case_mode, q_name.database_name_);
      ObDatabaseSchemaHashWrapper db_wrapper(tenant_id, name_case_mode, db_name);
      if (OB_UNLIKELY(!q_name.database_name_.empty() && !(q_db_wrapper == db_wrapper))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        ObString scope_name = "rls policy expression";
        ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                       col_name.length(), col_name.ptr(),
                       scope_name.length(), scope_name.ptr());
      } else if (OB_UNLIKELY(!q_name.tbl_name_.empty()
                              && !ObCharset::case_mode_equal(name_case_mode, q_name.tbl_name_, table_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        ObString scope_name = "rls policy expression";
        ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                       col_name.length(), col_name.ptr(),
                       scope_name.length(), scope_name.ptr());
      } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        ObString scope_name = "rls policy expression";
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                        q_name.col_name_.length(), q_name.col_name_.ptr(),
                        scope_name.length(), scope_name.ptr());
        LOG_WARN("column not found in table", K(ret), K(q_name.col_name_), K(table_name));
      } else if (col_schema->is_hidden()) {
        bool enable_hidden_visible = false;
        if (OB_FAIL(query_ctx->get_global_hint().opt_params_
                           .has_enable_opt_param(ObOptParamHint::OptParamType::HIDDEN_COLUMN_VISIBLE,
                                                 enable_hidden_visible))) {
          LOG_WARN("failed to check hidden column visible hint", K(ret));
        } else if (OB_UNLIKELY(!enable_hidden_visible)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          ObString scope_name = "rls policy expression";
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                          q_name.col_name_.length(), q_name.col_name_.ptr(),
                          scope_name.length(), scope_name.ptr());
          LOG_WARN("column not found in table", K(ret), K(q_name.col_name_), K(table_name));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema,
                                                          session_info,
                                                          *q_name.ref_expr_))) {
        LOG_WARN("failed to init column expr", K(ret));
      } else {
        real_ref_expr = q_name.ref_expr_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
      LOG_WARN("failed to push back real expr", K(ret));
    } else if (OB_FAIL(ref_exprs.push_back(q_name.ref_expr_))) {
      LOG_WARN("failed to push back ref expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(real_ref_expr,
                                                          ref_exprs,
                                                          real_exprs))) {
      LOG_WARN("failed to replace ref column", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr,
                                                        ref_exprs,
                                                        real_exprs))) {
    LOG_WARN("failed to replace ref column", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  } else if (OB_UNLIKELY(!expr->is_deterministic())) {
    ret = OB_ERR_POLICY_PREDICATE;
    LOG_WARN("rls predicate contains non-deterministic expression", K(ret));
  } else {
    // print expr to string
    HEAP_VAR(char[OB_MAX_CONSTRAINT_EXPR_LENGTH], expr_str_buf) {
      MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
      int64_t pos = 0;
      ObRawExprPrinter printer(expr_str_buf, OB_MAX_CONSTRAINT_EXPR_LENGTH, &pos,
                               schema_checker->get_schema_guard(),
                               session_info->get_timezone_info());
      if (OB_FAIL(printer.do_print(expr, T_NONE_SCOPE, false))) {
        LOG_WARN("print rls predicate expr failed", K(ret));
      } else if (OB_UNLIKELY(pos >= OB_MAX_CONSTRAINT_EXPR_LENGTH)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("rls predicate expression is too long", K(ret), K(pos));
      } else {
        ObString expr_def(static_cast<int32_t>(pos), expr_str_buf);
        if (OB_FAIL(ob_write_string(*allocator, expr_def, result_str))) {
          LOG_WARN("failed to deep copy expr string", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

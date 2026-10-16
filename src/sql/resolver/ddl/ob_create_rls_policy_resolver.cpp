/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_create_rls_policy_resolver.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "sql/resolver/ddl/ob_rls_policy_resolver_helper.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

// CREATE POLICY children layout (T_CREATE_RLS_POLICY, 9 children):
//   [0] hint (create_with_opt_hint; NULL=no hint)
//   [1] opt_if_not_exists
//   [2] policy_name (relation_name)
//   [3] table (relation_factor)
//   [4] opt_rls_policy_permissive (T_INT: 1=permissive, 0=restrictive; NULL=default permissive)
//   [5] opt_rls_policy_for_cmd (T_INT: 0=ALL,1=SELECT,2=INSERT,3=UPDATE,4=DELETE; NULL=ALL)
//   [6] opt_rls_policy_to_role (role list; NULL=PUBLIC)
//   [7] opt_rls_policy_using (expr node; NULL=none)
//   [8] opt_rls_policy_with_check (expr node; NULL=none)

ObCreateRlsPolicyResolver::ObCreateRlsPolicyResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObCreateRlsPolicyResolver::~ObCreateRlsPolicyResolver()
{
}

int ObCreateRlsPolicyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateRlsPolicyStmt *stmt = NULL;
  uint64_t data_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(T_CREATE_RLS_POLICY != parse_tree.type_)
      || OB_UNLIKELY(9 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse_tree", K(parse_tree.type_), K(parse_tree.num_child_), K(ret));
  } else if (OB_ISNULL(parse_tree.children_)
             || OB_ISNULL(parse_tree.children_[2])
             || OB_ISNULL(parse_tree.children_[3])
             || OB_ISNULL(session_info_)
             || OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!lib::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("CREATE POLICY is only supported in MySQL mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "CREATE POLICY in non-MySQL mode is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_6_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row level security is not supported when data version is below DATA_VERSION_4_6_1_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "row level security before version DATA_VERSION_4_6_1_0 is");
  } else if (OB_ISNULL(stmt = create_stmt<ObCreateRlsPolicyStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create rls policy stmt failed", K(ret));
  } else {
    ObRlsPolicyDDLArg &arg = stmt->get_arg();
    ObRlsPolicySchema &schema = arg.schema_;
    const share::schema::ObTableSchema *table_schema = NULL;
    arg.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
    arg.exec_tenant_id_ = session_info_->get_effective_tenant_id();
    schema.set_rls_group_id(0);

    // if not exists
    arg.if_not_exist_ = (NULL != parse_tree.children_[1]);

    // policy name
    if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_policy_name(
            parse_tree.children_[2], *session_info_, *allocator_, schema))) {
      LOG_WARN("failed to resolve policy name", K(ret));
    }

    // table
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker is null", K(ret));
    } else if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_table(parse_tree.children_[3],
                                                                *this,
                                                                *session_info_,
                                                                *schema_checker_,
                                                                *stmt,
                                                                schema,
                                                                table_schema))) {
      LOG_WARN("failed to resolve table", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    }

    // hint
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_hints(parse_tree.children_[0], *stmt, *table_schema))) {
        LOG_WARN("failed to resolve hints", K(ret));
      }
    }

    // permissive (default true)
    if (OB_SUCC(ret)) {
      if (NULL != parse_tree.children_[4]) {
        schema.set_permissive(1 == parse_tree.children_[4]->value_);
      } else {
        schema.set_permissive(true);
      }
    }

    // cmd type (FOR ALL/SELECT/INSERT/UPDATE/DELETE)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_stmt_type(
              parse_tree.children_[5], schema))) {
        LOG_WARN("failed to resolve stmt type", K(ret));
      }
    }

    // role list (TO role)
    if (OB_SUCC(ret) && NULL != parse_tree.children_[6]) {
      if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_role_list(
              parse_tree.children_[6], *schema_checker_,
              *session_info_, schema))) {
        LOG_WARN("failed to resolve role list", K(ret));
      }
    }

    // USING expression
    if (OB_SUCC(ret) && NULL != parse_tree.children_[7]) {
      ObString using_expr_str;
      if (OB_UNLIKELY(schema.get_stmt_type() == RLS_POLICY_INSERT_FLAG)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("USING is not allowed for INSERT policies", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "USING expression for INSERT command is");
      } else if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_rls_predicate_expr(
                  parse_tree.children_[7], params_, *table_schema,
                  stmt->get_database_name(), stmt->get_table_name(), using_expr_str))) {
        LOG_WARN("failed to resolve using expr", K(ret));
      } else if (OB_FAIL(schema.set_using_expr_str(using_expr_str))) {
        LOG_WARN("failed to set using expr str", K(ret));
      }
    }

    // WITH CHECK expression
    if (OB_SUCC(ret) && NULL != parse_tree.children_[8]) {
      ObString with_check_str;
      if (OB_UNLIKELY(schema.get_stmt_type() == RLS_POLICY_SELECT_FLAG
                       || schema.get_stmt_type() == RLS_POLICY_DELETE_FLAG)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("WITH CHECK is not allowed for SELECT or DELETE policies", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "WITH CHECK expression for SELECT or DELETE command is");
      } else if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_rls_predicate_expr(
                  parse_tree.children_[8], params_, *table_schema,
                  stmt->get_database_name(), stmt->get_table_name(), with_check_str))) {
        LOG_WARN("failed to resolve with check expr", K(ret));
      } else if (OB_FAIL(schema.set_with_check_str(with_check_str))) {
        LOG_WARN("failed to set with check str", K(ret));
      }
    }

    // require at least one of USING / WITH CHECK so access-control semantics
    // are not driven solely by the permissive_ flag (which would be easy to misconfigure).
    if (OB_SUCC(ret)
        && OB_UNLIKELY(schema.get_using_expr_str().empty()
                       && schema.get_with_check_str().empty())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("CREATE POLICY requires at least one of USING or WITH CHECK expression",
               K(ret), K(schema.get_stmt_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "CREATE POLICY with neither USING nor WITH CHECK expression is");
    }
  }

  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_alter_rls_policy_resolver.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "sql/resolver/ddl/ob_rls_policy_resolver_helper.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

// ALTER POLICY children layout (T_ALTER_RLS_POLICY, 7 children):
// Variant 1 (RENAME):
//   [0] hint (alter_with_opt_hint; NULL=no hint)
//   [1] policy_name
//   [2] table (relation_factor)
//   [3] NULL (role list not used)
//   [4] NULL (using not used)
//   [5] NULL (check not used)
//   [6] new_policy_name
// Variant 2 (modify attributes):
//   [0] hint (alter_with_opt_hint; NULL=no hint)
//   [1] policy_name
//   [2] table (relation_factor)
//   [3] opt_rls_policy_to_role
//   [4] opt_rls_policy_using
//   [5] opt_rls_policy_with_check
//   [6] NULL

ObAlterRlsPolicyResolver::ObAlterRlsPolicyResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObAlterRlsPolicyResolver::~ObAlterRlsPolicyResolver()
{
}

int ObAlterRlsPolicyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterRlsPolicyStmt *stmt = NULL;
  uint64_t data_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(T_ALTER_RLS_POLICY != parse_tree.type_)
      || OB_UNLIKELY(7 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse_tree", K(parse_tree.type_), K(parse_tree.num_child_), K(ret));
  } else if (OB_ISNULL(parse_tree.children_)
             || OB_ISNULL(parse_tree.children_[1])
             || OB_ISNULL(parse_tree.children_[2])
             || OB_ISNULL(session_info_)
             || OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!lib::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ALTER POLICY is only supported in MySQL mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ALTER POLICY in non-MySQL mode is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_6_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row level security is not supported when data version is below DATA_VERSION_4_6_1_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "row level security before version DATA_VERSION_4_6_1_0 is");
  } else if (OB_ISNULL(stmt = create_stmt<ObAlterRlsPolicyStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create alter rls policy stmt failed", K(ret));
  } else {
    ObRlsPolicyDDLArg &arg = stmt->get_arg();
    ObRlsPolicySchema &schema = arg.schema_;
    const share::schema::ObTableSchema *table_schema = NULL;
    arg.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
    arg.exec_tenant_id_ = session_info_->get_effective_tenant_id();
    schema.set_rls_group_id(0);

    // policy name
    if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_policy_name(
            parse_tree.children_[1], *session_info_, *allocator_, schema))) {
      LOG_WARN("failed to resolve policy name", K(ret));
    }

    // table
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker is null", K(ret));
    } else if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_table(parse_tree.children_[2],
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

    // role list
    if (OB_SUCC(ret) && NULL != parse_tree.children_[3]) {
      if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_role_list(
              parse_tree.children_[3], *schema_checker_,
              *session_info_, schema))) {
        LOG_WARN("failed to resolve role list", K(ret));
      } else {
        arg.option_bitset_.add_member(ObRlsPolicyDDLArg::ROLE_LIST_OPT);
      }
    }

    // USING expression
    if (OB_SUCC(ret) && NULL != parse_tree.children_[4]) {
      ObString using_expr_str;
      if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_rls_predicate_expr(
              parse_tree.children_[4], params_, *table_schema,
              stmt->get_database_name(), stmt->get_table_name(), using_expr_str))) {
        LOG_WARN("failed to resolve using expr", K(ret));
      } else if (OB_FAIL(schema.set_using_expr_str(using_expr_str))) {
        LOG_WARN("failed to set using expr str", K(ret));
      } else {
        arg.option_bitset_.add_member(ObRlsPolicyDDLArg::USING_EXPR_OPT);
      }
    }

    // WITH CHECK expression
    if (OB_SUCC(ret) && NULL != parse_tree.children_[5]) {
      ObString with_check_str;
      if (OB_FAIL(ObRlsPolicyResolverHelper::resolve_rls_predicate_expr(
              parse_tree.children_[5], params_, *table_schema,
              stmt->get_database_name(), stmt->get_table_name(), with_check_str))) {
        LOG_WARN("failed to resolve with check expr", K(ret));
      } else if (OB_FAIL(schema.set_with_check_str(with_check_str))) {
        LOG_WARN("failed to set with check str", K(ret));
      } else {
        arg.option_bitset_.add_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
      }
    }

    // RENAME TO new_name
    if (OB_SUCC(ret) && NULL != parse_tree.children_[6]) {
      ObString new_name(static_cast<int32_t>(parse_tree.children_[6]->str_len_),
                        parse_tree.children_[6]->str_value_);
      if (OB_FAIL(ObRlsPolicyResolverHelper::normalize_policy_name(
              *session_info_, *allocator_, new_name))) {
        LOG_WARN("failed to normalize new policy name", K(ret), K(new_name));
      } else {
        arg.new_policy_name_ = new_name;
        arg.option_bitset_.add_member(ObRlsPolicyDDLArg::RENAME_OPT);
      }
    }

    // reject ALTER POLICY with no modification options
    if (OB_SUCC(ret) && arg.option_bitset_.is_empty()) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("ALTER POLICY requires at least one modification option", K(ret));
    }

    // check USING/WITH CHECK compatibility with existing policy's stmt_type,
    // and ensure the merged policy still has at least one of USING / WITH CHECK
    // so access-control semantics are not driven solely by the permissive_ flag.
    if (OB_SUCC(ret)
        && (arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT)
            || arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT))) {
      share::schema::ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
      const ObRlsPolicySchema *existing_policy = NULL;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null", K(ret));
      } else if (OB_FAIL(schema_guard->get_rls_policy_schema_by_name(schema.get_tenant_id(),
                                                                     schema.get_table_id(),
                                                                     0/*rls_group_id*/,
                                                                     schema.get_policy_name(),
                                                                     existing_policy))) {
        LOG_WARN("failed to get existing rls policy schema", K(ret));
      } else if (OB_ISNULL(existing_policy)) {
        ret = OB_ERR_POLICY_NOT_EXIST;
        LOG_WARN("rls policy not exist", K(ret), K(schema.get_policy_name()));
      } else if (OB_UNLIKELY(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT)
                             && RLS_POLICY_INSERT_FLAG == existing_policy->get_stmt_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("USING is not allowed for INSERT policies", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "USING expression for INSERT command is");
      } else if (OB_UNLIKELY(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT)
                             && (RLS_POLICY_SELECT_FLAG == existing_policy->get_stmt_type()
                                 || RLS_POLICY_DELETE_FLAG == existing_policy->get_stmt_type()))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("WITH CHECK is not allowed for SELECT or DELETE policies", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "WITH CHECK expression for SELECT or DELETE command is");
      } else {
        const ObString &merged_using =
            arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT)
                ? schema.get_using_expr_str()
                : existing_policy->get_using_expr_str();
        const ObString &merged_with_check =
            arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT)
                ? schema.get_with_check_str()
                : existing_policy->get_with_check_str();
        if (OB_UNLIKELY(merged_using.empty() && merged_with_check.empty())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ALTER POLICY would leave both USING and WITH CHECK empty",
                   K(ret), K(schema.get_policy_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "ALTER POLICY with neither USING nor WITH CHECK expression is");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

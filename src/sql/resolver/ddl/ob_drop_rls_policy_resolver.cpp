/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_drop_rls_policy_resolver.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "sql/resolver/ddl/ob_rls_policy_resolver_helper.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

// DROP POLICY children layout (T_DROP_RLS_POLICY, 3 children):
//   [0] opt_if_exists
//   [1] policy_name (relation_name)
//   [2] table (relation_factor)

ObDropRlsPolicyResolver::ObDropRlsPolicyResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObDropRlsPolicyResolver::~ObDropRlsPolicyResolver()
{
}

int ObDropRlsPolicyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObDropRlsPolicyStmt *stmt = NULL;
  uint64_t data_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(T_DROP_RLS_POLICY != parse_tree.type_)
      || OB_UNLIKELY(3 != parse_tree.num_child_)) {
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
    LOG_WARN("DROP POLICY is only supported in MySQL mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DROP POLICY in non-MySQL mode is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_6_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row level security is not supported when data version is below DATA_VERSION_4_6_1_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "row level security before version DATA_VERSION_4_6_1_0 is");
  } else if (OB_ISNULL(stmt = create_stmt<ObDropRlsPolicyStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create drop rls policy stmt failed", K(ret));
  } else {
    ObRlsPolicyDDLArg &arg = stmt->get_arg();
    ObRlsPolicySchema &schema = arg.schema_;
    const share::schema::ObTableSchema *table_schema = NULL;
    arg.ddl_type_ = OB_DDL_DROP_RLS_POLICY;
    arg.exec_tenant_id_ = session_info_->get_effective_tenant_id();
    schema.set_rls_group_id(0);

    // if exists
    arg.if_exist_ = (NULL != parse_tree.children_[0]);

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
    }
  }

  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

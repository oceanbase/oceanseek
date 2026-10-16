/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_RLS_POLICY_RESOLVER_HELPER_H_
#define OCEANBASE_SQL_OB_RLS_POLICY_RESOLVER_HELPER_H_

#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_struct.h"

typedef struct _ParseNode ParseNode;

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
namespace schema
{
class ObRlsPolicySchema;
class ObTableSchema;
}
}
namespace sql
{
class ObSQLSessionInfo;
class ObStmtResolver;
class ObSchemaChecker;
class ObRlsPolicyDDLStmt;
struct ObResolverParams;

class ObRlsPolicyResolverHelper
{
public:
  static int resolve_policy_name(const ParseNode *name_node,
                                 ObSQLSessionInfo &session_info,
                                 common::ObIAllocator &allocator,
                                 share::schema::ObRlsPolicySchema &schema);
  static int normalize_policy_name(ObSQLSessionInfo &session_info,
                                   common::ObIAllocator &allocator,
                                   common::ObString &name);
  static int resolve_table(const ParseNode *table_node,
                           ObStmtResolver &resolver,
                           ObSQLSessionInfo &session_info,
                           ObSchemaChecker &schema_checker,
                           ObRlsPolicyDDLStmt &rls_stmt,
                           share::schema::ObRlsPolicySchema &schema,
                           const share::schema::ObTableSchema *&out_table_schema);
  static int resolve_role_list(const ParseNode *role_list_node,
                               ObSchemaChecker &schema_checker,
                               ObSQLSessionInfo &session_info,
                               share::schema::ObRlsPolicySchema &schema);
  static int resolve_stmt_type(const ParseNode *stmt_type_node,
                               share::schema::ObRlsPolicySchema &schema);
  static int resolve_rls_predicate_expr(const ParseNode *expr_node,
                                        ObResolverParams &params,
                                        const share::schema::ObTableSchema &table_schema,
                                        const common::ObString &db_name,
                                        const common::ObString &table_name,
                                        common::ObString &result_str);
};

}  // namespace sql
}  // namespace oceanbase

#endif // OCEANBASE_SQL_OB_RLS_POLICY_RESOLVER_HELPER_H_

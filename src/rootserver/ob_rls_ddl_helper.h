/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_ROOTSERVER_OB_RLS_DDL_HELPER_H_
#define _OCEANBASE_ROOTSERVER_OB_RLS_DDL_HELPER_H_

#include "rootserver/ob_ddl_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObDDLOperator;

// Row-Level Security (RLS) related DDL logic, extracted from ObDDLService so that
// it can be maintained independently. All methods are static; the ones that need
// ObDDLService private state (transaction / schema publishing) take an
// ObDDLService& and access it through the friend relationship.
class ObRlsDDLHelper
{
public:
  //----Functions for RLS policy / group / context DDL entry----
  static int handle_rls_policy_ddl(const obrpc::ObRlsPolicyDDLArg &arg,
                                   ObDDLService &ddl_service);
  static int handle_rls_group_ddl(const obrpc::ObRlsGroupDDLArg &arg,
                                  ObDDLService &ddl_service);
  static int handle_rls_context_ddl(const obrpc::ObRlsContextDDLArg &arg,
                                    ObDDLService &ddl_service);
  //----End of functions for RLS policy / group / context DDL entry----

  //----Functions for column reference check / flag maintenance----
  static int check_column_referenced_by_rls_policy(share::schema::ObSchemaGetterGuard &schema_guard,
                                                   const share::schema::ObTableSchema &orig_table_schema,
                                                   const common::ObString &column_name);
  static int check_columns_referenced_by_rls_policy(share::schema::ObSchemaGetterGuard &schema_guard,
                                                    const share::schema::ObTableSchema &orig_table_schema,
                                                    const common::ObIArray<uint64_t> &drop_cols_id_arr);
  static int update_new_table_rls_flag(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const common::ObIArray<uint64_t> &drop_cols_id_arr,
                                       share::schema::ObTableSchema &table_schema);
  static int drop_rls_policy_caused_by_drop_column_online(ObDDLService &ddl_service,
                                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                                          const share::schema::ObTableSchema &origin_table_schema,
                                                          const common::ObIArray<uint64_t> &drop_cols_id_arr,
                                                          share::schema::ObTableSchema &new_table_schema,
                                                          ObDDLOperator &ddl_operator,
                                                          common::ObMySQLTransaction &trans);
  static int rebuild_hidden_table_rls_objects(ObDDLService &ddl_service,
                                              const share::schema::ObTableSchema &orig_table_schema,
                                              const share::schema::ObTableSchema &hidden_table_schema,
                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                              ObDDLOperator &ddl_operator,
                                              common::ObMySQLTransaction &trans);
  //----End of functions for column reference check / flag maintenance----

private:
  // Currently, RLS does not support subqueries, so we can directly compare
  // column names without comparing table names. If subqueries are supported
  // in the future, table name comparison will be needed.
  static int check_parse_node_references_column(const ParseNode *node,
                                                const common::ObString &column_name,
                                                bool is_oracle_mode,
                                                bool &is_referenced);
  static int check_rls_expr_references_column(const common::ObString &expr_str,
                                              common::ObIAllocator &allocator,
                                              const common::ObString &column_name,
                                              bool is_oracle_mode,
                                              bool &is_referenced);
};

} // namespace rootserver
} // namespace oceanbase

#endif // _OCEANBASE_ROOTSERVER_OB_RLS_DDL_HELPER_H_

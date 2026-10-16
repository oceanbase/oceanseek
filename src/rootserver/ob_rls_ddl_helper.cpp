/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "ob_rls_ddl_helper.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_ddl_common.h"
#include "share/ob_get_compat_mode.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using sql::ObRawExprUtils;

namespace rootserver
{

int ObRlsDDLHelper::drop_rls_policy_caused_by_drop_column_online(
    ObDDLService &ddl_service,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &origin_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    share::schema::ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObString empty_ddl_stmt;
  bool rls_object_changed = false;
  const uint64_t tenant_id = origin_table_schema.get_tenant_id();
  if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_table_schema.get_rls_policy_ids().count(); i++) {
      ObRlsPolicySchema new_rls_policy;
      const ObRlsPolicySchema *policy_schema = nullptr;
      const uint64_t policy_id = origin_table_schema.get_rls_policy_ids().at(i);
      if (OB_FAIL(schema_guard.get_rls_policy_schema_by_id(tenant_id, policy_id, policy_schema))) {
        LOG_WARN("get rls policy schema failed", KR(ret), K(policy_id));
      } else if (OB_ISNULL(policy_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null policy", KR(ret), K(policy_id));
      } else if (OB_FAIL(new_rls_policy.rebuild_with_table_schema(*policy_schema, new_table_schema))) {
        LOG_WARN("rebuild with table schema failed", KR(ret), K(policy_id));
      } else if (new_rls_policy.get_sec_column_count() == policy_schema->get_sec_column_count()) {
        // do nothing, the policy does not releated to the dropped column.
      } else if (OB_FALSE_IT(rls_object_changed = true)) {
      } else if (OB_FAIL(ddl_operator.drop_rls_policy(*policy_schema, trans, empty_ddl_stmt,
          false/*is_update_table_schema*/, nullptr/*table_schema*/))) {
        LOG_WARN("drop origin policy failed", K(ret));
      } else if (policy_schema->is_column_level_policy() && !new_rls_policy.is_column_level_policy()) {
        // column level policy will be dropped after drop column
      } else if (OB_FAIL(ddl_operator.create_rls_policy(new_rls_policy, trans, empty_ddl_stmt,
          false/*is_update_table_schema*/, nullptr/*table_schema*/))) {
        LOG_WARN("create new policy failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && rls_object_changed) {
      if (OB_FAIL(update_new_table_rls_flag(schema_guard, drop_cols_id_arr, new_table_schema))) {
        LOG_WARN("fail to update new table flags", KR(ret));
      } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema, trans, OB_DDL_ALTER_TABLE))) {
        LOG_WARN("fail to update table schema", KR(ret));
      }
    }
  }
  return ret;
}

// Currently, RLS does not support subqueries, so we can directly compare
// column names without comparing table names. If subqueries are supported
// in the future, table name comparison will be needed.
int ObRlsDDLHelper::check_parse_node_references_column(const ParseNode *node,
                                                       const ObString &column_name,
                                                       bool is_oracle_mode,
                                                       bool &is_referenced)
{
  int ret = OB_SUCCESS;
  is_referenced = false;
  if (NULL == node) {
    // do nothing
  } else if (T_COLUMN_REF == node->type_ && node->num_child_ >= 3 && NULL != node->children_[2]) {
    ObString col_name(node->children_[2]->str_len_, node->children_[2]->str_value_);
    // Column names in Oracle mode are case-sensitive; in MySQL mode case-insensitive.
    // Use the explicit is_oracle_mode flag instead of lib::is_oracle_mode() because
    // lib::is_oracle_mode() reads thread-local state which is unreliable in DDL context.
    if (is_oracle_mode ? ObCharset::case_sensitive_equal(column_name, col_name)
                       : ObCharset::case_insensitive_equal(column_name, col_name)) {
      is_referenced = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_referenced && i < node->num_child_; ++i) {
      if (OB_FAIL(SMART_CALL(check_parse_node_references_column(node->children_[i],
                                                                column_name,
                                                                is_oracle_mode,
                                                                is_referenced)))) {
        LOG_WARN("failed to check parse node references column", K(ret));
      }
    }
  }
  return ret;
}

int ObRlsDDLHelper::check_rls_expr_references_column(const ObString &expr_str,
                                                      ObIAllocator &allocator,
                                                      const ObString &column_name,
                                                      bool is_oracle_mode,
                                                      bool &is_referenced)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  is_referenced = false;
  if (expr_str.empty()) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(expr_str, allocator, node))) {
    LOG_WARN("failed to parse rls expression", K(ret), K(expr_str));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parsed node is null", K(ret), K(expr_str));
  } else if (OB_FAIL(check_parse_node_references_column(node, column_name, is_oracle_mode, is_referenced))) {
    LOG_WARN("failed to check parse node references column", K(ret));
  }

  return ret;
}

int ObRlsDDLHelper::check_column_referenced_by_rls_policy(ObSchemaGetterGuard &schema_guard,
                                                           const ObTableSchema &orig_table_schema,
                                                           const ObString &column_name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  ObArenaAllocator allocator("RlsColCheck");
  bool is_oracle_mode = false;
  bool is_referenced = false;
  if (OB_FAIL(share::ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("failed to get tenant compat mode", K(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_rls_policy_ids().count(); ++i) {
    const ObRlsPolicySchema *policy_schema = NULL;
    if (OB_FAIL(schema_guard.get_rls_policy_schema_by_id(tenant_id,
                                                         orig_table_schema.get_rls_policy_ids().at(i),
                                                         policy_schema))) {
      LOG_WARN("failed to get rls policy schema", K(ret));
    } else if (OB_ISNULL(policy_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls policy schema is null", K(ret), K(tenant_id), K(orig_table_schema.get_rls_policy_ids().at(i)));
    } else if (OB_FAIL(check_rls_expr_references_column(
                policy_schema->get_using_expr_str(), allocator, column_name, is_oracle_mode, is_referenced))) {
      LOG_WARN("failed to check rls expr references column", K(ret));
    } else if (!is_referenced && OB_FAIL(check_rls_expr_references_column(
                policy_schema->get_with_check_str(), allocator, column_name, is_oracle_mode, is_referenced))) {
      LOG_WARN("failed to check rls expr references column", K(ret));
    } else {
      allocator.reuse();
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(is_referenced)) {
      ret = OB_ERR_COL_REFERENCED_BY_RLS_POLICY;
      LOG_USER_ERROR(OB_ERR_COL_REFERENCED_BY_RLS_POLICY,
                     policy_schema->get_policy_name().length(),
                     policy_schema->get_policy_name().ptr(),
                     column_name.length(), column_name.ptr());
      LOG_WARN("column is referenced by rls policy", K(ret), K(column_name),
                K(policy_schema->get_policy_name()));
    }
  }
  return ret;
}

int ObRlsDDLHelper::check_columns_referenced_by_rls_policy(ObSchemaGetterGuard &schema_guard,
                                                            const ObTableSchema &orig_table_schema,
                                                            const ObIArray<uint64_t> &drop_cols_id_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < drop_cols_id_arr.count(); ++i) {
    const ObColumnSchemaV2 *col_schema = orig_table_schema.get_column_schema(drop_cols_id_arr.at(i));
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret), K(drop_cols_id_arr.at(i)));
    } else if (OB_FAIL(check_column_referenced_by_rls_policy(schema_guard,
                                                             orig_table_schema,
                                                             col_schema->get_column_name_str()))) {
      LOG_WARN("failed to check column referenced by rls policy", K(ret), K(col_schema->get_column_name_str()));
    }
  }
  return ret;
}

// if all rls policies are column level and all sec columns of each policy will be droppped, then
// the rls object flag should be removed
int ObRlsDDLHelper::update_new_table_rls_flag(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (!table_schema.has_table_flag(CASCADE_RLS_OBJECT_FLAG) ||
      !table_schema.get_rls_group_ids().empty() ||
      !table_schema.get_rls_context_ids().empty() ||
      0 == drop_cols_id_arr.count()) {
    // do nothing
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const ObRlsPolicySchema *policy = NULL;
    const int64_t policy_count = table_schema.get_rls_policy_ids().count();
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < policy_count; ++i) {
      uint64_t policy_id = table_schema.get_rls_policy_ids().at(i);
      if (OB_FAIL(schema_guard.get_rls_policy_schema_by_id(tenant_id, policy_id, policy))) {
        LOG_WARN("failed to get rls policy schema", K(ret));
      } else if (OB_ISNULL(policy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null rls policy schema", KR(ret), K(policy_id), K(table_schema));
      } else if (!policy->is_column_level_policy()) {
        found = true;
      }
      for (int64_t j = 0; OB_SUCC(ret) && !found && j < policy->get_sec_column_count(); ++j) {
        const ObRlsSecColumnSchema* sec_column = policy->get_sec_column_by_idx(j);
        bool found_col = false;
        if (OB_ISNULL(sec_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rls column is null", KR(ret));
        }
        for (int64_t k = 0; OB_SUCC(ret) && !found_col && k < drop_cols_id_arr.count(); ++k) {
          if (sec_column->get_column_id() == drop_cols_id_arr.at(k)) {
            found_col = true;
          }
        }
        if (OB_SUCC(ret) && !found_col) {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      table_schema.del_table_flag(CASCADE_RLS_OBJECT_FLAG);
    }
  }
  return ret;
}

int ObRlsDDLHelper::rebuild_hidden_table_rls_objects(
                  ObDDLService &ddl_service,
                  const ObTableSchema &orig_table_schema,
                  const ObTableSchema &hidden_table_schema,
                  ObSchemaGetterGuard &schema_guard,
                  ObDDLOperator &ddl_operator,
                  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else {
    const uint64_t tenant_id = orig_table_schema.get_tenant_id();
    ObRlsPolicySchema new_rls_policy;
    ObRlsGroupSchema new_rls_group;
    ObRlsContextSchema new_rls_context;
    ObString empty_str;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_rls_context_ids().count(); ++i) {
      const ObRlsContextSchema *context_schema = NULL;
      uint64_t context_id = orig_table_schema.get_rls_context_ids().at(i);
      OZ (schema_guard.get_rls_context_schema_by_id(tenant_id, context_id, context_schema));
      CK (OB_NOT_NULL(context_schema));
      OZ (new_rls_context.assign(*context_schema));
      OX (new_rls_context.set_tenant_id(hidden_table_schema.get_tenant_id()));
      OX (new_rls_context.set_table_id(hidden_table_schema.get_table_id()));
      OZ (ddl_operator.create_rls_context(new_rls_context, trans, empty_str, false, NULL));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_rls_group_ids().count(); ++i) {
      const ObRlsGroupSchema *group_schema = NULL;
      uint64_t group_id = orig_table_schema.get_rls_group_ids().at(i);
      OZ (schema_guard.get_rls_group_schema_by_id(tenant_id, group_id, group_schema));
      CK (OB_NOT_NULL(group_schema));
      OZ (new_rls_group.assign(*group_schema));
      OX (new_rls_group.set_tenant_id(hidden_table_schema.get_tenant_id()));
      OX (new_rls_group.set_table_id(hidden_table_schema.get_table_id()));
      OZ (ddl_operator.create_rls_group(new_rls_group, trans, empty_str, false, NULL));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_rls_policy_ids().count(); ++i) {
      const ObRlsPolicySchema *policy_schema = NULL;
      uint64_t policy_id = orig_table_schema.get_rls_policy_ids().at(i);
      OZ (schema_guard.get_rls_policy_schema_by_id(tenant_id, policy_id, policy_schema));
      CK (OB_NOT_NULL(policy_schema));
      OZ (new_rls_policy.rebuild_with_table_schema(*policy_schema, hidden_table_schema));
      if (OB_FAIL(ret)) {
      } else if (policy_schema->is_column_level_policy()
                 && !new_rls_policy.is_column_level_policy()) {
        // column level policy will be dropped after drop column
      } else {
        OZ (ddl_operator.create_rls_policy(new_rls_policy, trans, empty_str, false, NULL));
      }
    }
  }
  return ret;
}

int ObRlsDDLHelper::handle_rls_policy_ddl(const obrpc::ObRlsPolicyDDLArg &arg,
                                           ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObRlsPolicySchema schema;
  const ObString &policy_name = arg.schema_.get_policy_name();
  const ObString &ddl_stmt_str = arg.ddl_stmt_str_;
  uint64_t tenant_id = arg.schema_.get_tenant_id();
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema.assign(arg.schema_))) {
    LOG_WARN("failed to assign rls policy schema", K(ret));
  } else {
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObDDLOperator ddl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    const ObTableSchema *table_schema = NULL;
    const ObRlsGroupSchema *rls_group_schema = NULL;
    const ObRlsPolicySchema *old_schema = NULL;
    bool is_db_in_recyclebin = false;

    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, schema.get_table_id(), table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(arg));
    } else if (table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("rls table is in recyclebin", KR(ret), K(arg));
    } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
                                                                 table_schema->get_database_id(),
                                                                 is_db_in_recyclebin))) {
      LOG_WARN("failed to check database in recyclebin", KR(ret), KPC(table_schema));
    } else if (is_db_in_recyclebin) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("database of rls table is in recyclebin", KR(ret), KPC(table_schema));
    } else if (OB_UNLIKELY(is_inner_table(table_schema->get_table_id()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rls policy is not supported on inner table", KR(ret), KPC(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "policy on inner table is");
    } else if (OB_UNLIKELY(!is_internal_catalog_id(table_schema->get_catalog_id()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rls policy is not supported on external catalog table", KR(ret), KPC(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "policy on external catalog table is");
    } else if (0 == schema.get_rls_group_id()) {
      // the policy does not belong to any group, do nothing
    } else if (OB_FAIL(schema_guard.get_rls_group_schema_by_id(tenant_id,
                                                               schema.get_rls_group_id(),
                                                               rls_group_schema))) {
      LOG_WARN("failed to get rls group schema", KR(ret), K(arg));
    } else if (OB_ISNULL(rls_group_schema)) {
      ret = OB_ERR_POLICY_GROUP_NOT_EXIST;
      LOG_WARN("rls group not exist", KR(ret), K(schema));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_rls_policy_schema_by_name(tenant_id,
                                                                  schema.get_table_id(),
                                                                  schema.get_rls_group_id(),
                                                                  policy_name,
                                                                  old_schema))) {
      LOG_WARN("failed to get rls policy from schema guard", K(ret), K(policy_name), K(arg));
    } else {
      switch (arg.ddl_type_) {
        case OB_DDL_CREATE_RLS_POLICY: {
          if (OB_NOT_NULL(old_schema)) {
            if (arg.if_not_exist_) {
              LOG_USER_WARN(OB_ERR_POLICY_EXIST);
              LOG_WARN("rls policy exist, but IF NOT EXISTS specified", KPC(old_schema));
            } else {
              ret = OB_ERR_POLICY_EXIST;
              LOG_WARN("rls policy exist", KPC(old_schema), K(ret));
            }
          } else if (OB_FAIL(ddl_operator.create_rls_policy(schema, trans, ddl_stmt_str,
                                                            true, table_schema))) {
            LOG_WARN("failed to create rls policy", K(ret));
          }
          break;
        }
        case OB_DDL_DROP_RLS_POLICY: {
          if (OB_ISNULL(old_schema)) {
            if (arg.if_exist_) {
              LOG_USER_WARN(OB_ERR_POLICY_NOT_EXIST);
              LOG_WARN("rls policy not exist, but IF EXISTS specified", K(policy_name));
            } else {
              ret = OB_ERR_POLICY_NOT_EXIST;
              LOG_WARN("rls policy not exist", K(ret), K(policy_name), K(arg));
            }
          } else if (OB_FAIL(schema.assign(*old_schema))) {
            LOG_WARN("failed to assign rls policy schema", KPC(old_schema), K(ret));
          } else if (OB_FAIL(ddl_operator.drop_rls_policy(schema, trans, ddl_stmt_str,
                                                          true, table_schema))) {
            LOG_WARN("failed to drop rls policy", K(ret));
          }
          break;
        }
        case OB_DDL_ALTER_RLS_POLICY: {
          if (OB_ISNULL(old_schema)) {
            ret = OB_ERR_POLICY_NOT_EXIST;
            LOG_WARN("rls policy not exist", K(ret), K(policy_name), K(arg));
          } else if (OB_FAIL(schema.assign(*old_schema))) {
            LOG_WARN("failed to assign rls policy schema", KPC(old_schema), K(ret));
          } else if (OB_UNLIKELY(arg.option_bitset_.is_empty())) {
            // Defense-in-depth: the resolver already rejects empty option_bitset_
            // (ob_alter_rls_policy_resolver.cpp), but ObRlsPolicyDDLArg is an RPC arg
            // and a malformed/forged request must not bump schema_version with a no-op
            // DDL (which would invalidate plan cache cluster-wide).
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alter rls policy with no modification options", K(ret), K(arg));
          } else {
            if (arg.option_bitset_.has_member(ObRlsPolicyDDLArg::AlterOption::ENABLE)) {
              schema.set_enable_flag(arg.schema_.get_enable_flag());
            }
            if (OB_SUCC(ret) && arg.option_bitset_.has_member(ObRlsPolicyDDLArg::AlterOption::RENAME_OPT)) {
              const ObRlsPolicySchema *new_name_schema = NULL;
              if (OB_FAIL(schema_guard.get_rls_policy_schema_by_name(tenant_id,
                                                                     schema.get_table_id(),
                                                                     schema.get_rls_group_id(),
                                                                     arg.new_policy_name_,
                                                                     new_name_schema))) {
                LOG_WARN("failed to get policy by name", K(ret), K(arg.new_policy_name_));
              } else if (OB_UNLIKELY(NULL != new_name_schema &&
                                     new_name_schema->get_rls_policy_id() != old_schema->get_rls_policy_id())) {
                ret = OB_ERR_POLICY_EXIST;
                LOG_WARN("rls policy with new name already exists", K(ret), K(arg.new_policy_name_));
              } else if (OB_FAIL(schema.set_policy_name(arg.new_policy_name_))) {
                LOG_WARN("failed to set new policy name", K(ret), K(arg.new_policy_name_));
              }
            }
            if (OB_SUCC(ret) && arg.option_bitset_.has_member(ObRlsPolicyDDLArg::AlterOption::USING_EXPR_OPT)) {
              if (OB_FAIL(schema.set_using_expr_str(arg.schema_.get_using_expr_str()))) {
                LOG_WARN("failed to set using expr", K(ret));
              }
            }
            if (OB_SUCC(ret) && arg.option_bitset_.has_member(ObRlsPolicyDDLArg::AlterOption::WITH_CHECK_EXPR_OPT)) {
              if (OB_FAIL(schema.set_with_check_str(arg.schema_.get_with_check_str()))) {
                LOG_WARN("failed to set with check expr", K(ret));
              }
            }
            if (OB_SUCC(ret) && arg.option_bitset_.has_member(ObRlsPolicyDDLArg::AlterOption::ROLE_LIST_OPT)) {
              if (OB_FAIL(schema.get_role_ids().assign(arg.schema_.get_role_ids()))) {
                LOG_WARN("failed to set role ids", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ddl_operator.alter_rls_policy(schema, trans, ddl_stmt_str))) {
                LOG_WARN("failed to alter rls policy", K(ret));
              }
            }
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected stmt type", K(arg), K(ret));
        }
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRlsDDLHelper::handle_rls_group_ddl(const obrpc::ObRlsGroupDDLArg &arg,
                                          ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObRlsGroupSchema schema;
  const ObString &group_name = arg.schema_.get_policy_group_name();
  const ObString &ddl_stmt_str = arg.ddl_stmt_str_;
  uint64_t tenant_id = arg.schema_.get_tenant_id();
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema.assign(arg.schema_))) {
    LOG_WARN("failed to assign rls group schema", K(ret));
  } else {
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObDDLOperator ddl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    const ObTableSchema *table_schema = NULL;
    const ObRlsGroupSchema *old_schema = NULL;
    bool is_db_in_recyclebin = false;

    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, schema.get_table_id(), table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(arg));
    } else if (table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("rls table is in recyclebin", KR(ret), K(arg));
    } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
                                                                 table_schema->get_database_id(),
                                                                 is_db_in_recyclebin))) {
      LOG_WARN("failed to check database in recyclebin", KR(ret), KPC(table_schema));
    } else if (is_db_in_recyclebin) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("database of rls table is in recyclebin", KR(ret), KPC(table_schema));
    } else if (!table_schema->is_user_table() && !table_schema->is_view_table() && !table_schema->is_external_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support rls on user table or user view", KR(ret), KPC(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "policy on non-user table");
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_rls_group_schema_by_name(tenant_id, schema.get_table_id(),
        group_name, old_schema))) {
      LOG_WARN("failed to get rls group from schema guard", K(ret), K(group_name), K(arg));
    } else {
      switch (arg.ddl_type_) {
        case OB_DDL_CREATE_RLS_GROUP: {
          if (OB_NOT_NULL(old_schema)) {
            ret = OB_ERR_POLICY_GROUP_EXIST;
            LOG_WARN("rls group exist", KPC(old_schema), K(ret));
          } else if (OB_FAIL(ddl_operator.create_rls_group(schema, trans, ddl_stmt_str,
                                                           true, table_schema))) {
            LOG_WARN("failed to create rls group", K(ret));
          }
          break;
        }
        case OB_DDL_DROP_RLS_GROUP: {
          ObSEArray<const ObRlsPolicySchema *, 4> policy_schemas;
          if (OB_ISNULL(old_schema)) {
            ret = OB_ERR_POLICY_GROUP_NOT_EXIST;
            LOG_WARN("rls group not exist", K(ret), K(group_name), K(arg));
          } else if (OB_FAIL(schema_guard.get_rls_policy_schemas_in_group(tenant_id,
              schema.get_table_id(), old_schema->get_rls_group_id(), policy_schemas))) {
            LOG_WARN("failed to get rls policy schemas", K(ret));
          } else if (OB_UNLIKELY(policy_schemas.count() != 0)) {
            ret = OB_ERR_POLICY_EXIST;
            LOG_WARN("rls group is not empty", K(policy_schemas), K(ret));
          } else if (OB_FAIL(schema.assign(*old_schema))) {
            LOG_WARN("failed to assign rls group schema", KPC(old_schema), K(ret));
          } else if (OB_FAIL(ddl_operator.drop_rls_group(schema, trans, ddl_stmt_str,
                                                         true, table_schema))) {
            LOG_WARN("failed to drop rls group", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected stmt type", K(arg), K(ret));
        }
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRlsDDLHelper::handle_rls_context_ddl(const obrpc::ObRlsContextDDLArg &arg,
                                            ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObRlsContextSchema schema;
  const ObString &context_name = arg.schema_.get_context_name();
  const ObString &attribute = arg.schema_.get_attribute();
  const ObString &ddl_stmt_str = arg.ddl_stmt_str_;
  uint64_t tenant_id = arg.schema_.get_tenant_id();
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema.assign(arg.schema_))) {
    LOG_WARN("failed to assign rls context schema", K(ret));
  } else {
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObDDLOperator ddl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    const ObTableSchema *table_schema = NULL;
    const ObRlsContextSchema *old_schema = NULL;
    bool is_db_in_recyclebin = false;

    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, schema.get_table_id(), table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(arg));
    } else if (table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("rls table is in recyclebin", KR(ret), K(arg));
    } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(tenant_id,
                                                                 table_schema->get_database_id(),
                                                                 is_db_in_recyclebin))) {
      LOG_WARN("failed to check database in recyclebin", KR(ret), KPC(table_schema));
    } else if (is_db_in_recyclebin) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("database of rls table is in recyclebin", KR(ret), KPC(table_schema));
    } else if (!table_schema->is_user_table() && !table_schema->is_view_table() && !table_schema->is_external_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support rls on user table or user view", KR(ret), KPC(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "policy on non-user table");
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_rls_context_schema_by_name(tenant_id, schema.get_table_id(),
        context_name, attribute, old_schema))) {
      LOG_WARN("failed to get rls context from schema guard", K(ret), K(context_name), K(attribute));
    } else {
      switch (arg.ddl_type_) {
        case OB_DDL_CREATE_RLS_CONTEXT: {
          if (OB_NOT_NULL(old_schema)) {
            ret = OB_ERR_DRIVING_CONTEXT_EXIST;
            LOG_WARN("rls context exist", KPC(old_schema), K(ret));
          } else if (OB_FAIL(ddl_operator.create_rls_context(schema, trans, ddl_stmt_str,
                                                             true, table_schema))) {
            LOG_WARN("failed to create rls context", K(ret));
          }
          break;
        }
        case OB_DDL_DROP_RLS_CONTEXT: {
          if (OB_ISNULL(old_schema)) {
            ret = OB_ERR_DRIVING_CONTEXT_NOT_EXIST;
            LOG_WARN("rls context not exist", K(ret), K(context_name), K(attribute), K(arg));
          } else if (OB_FAIL(schema.assign(*old_schema))) {
            LOG_WARN("failed to assign rls context schema", KPC(old_schema), K(ret));
          } else if (OB_FAIL(ddl_operator.drop_rls_context(schema, trans, ddl_stmt_str,
                                                           true, table_schema))) {
            LOG_WARN("failed to drop rls context", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected stmt type", K(arg), K(ret));
        }
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase

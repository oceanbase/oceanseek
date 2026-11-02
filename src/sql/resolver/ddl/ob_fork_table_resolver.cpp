/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_fork_table_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_cluster_version.h"
#include "sql/parser/parse_node.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_FORK_BEFORE_SUPPORTED_DATA_VERSION);
#endif

static int check_fork_min_data_version(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    SQL_RESV_LOG(WARN, "failed to get tenant min data version", KR(ret), K(tenant_id));
  }
#ifdef ERRSIM
  else if (OB_UNLIKELY(ERRSIM_FORK_BEFORE_SUPPORTED_DATA_VERSION)) {
    min_data_version = DATA_VERSION_4_6_2_0;
  }
#endif
  if (OB_SUCC(ret) && min_data_version < DATA_VERSION_4_6_3_0) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "fork is not supported during upgrade", KR(ret), K(tenant_id), K(min_data_version));
  }
  return ret;
}

ObForkTableResolver::ObForkTableResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObForkTableResolver::~ObForkTableResolver()
{
}

int ObForkTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObForkTableStmt *fork_table_stmt = NULL;

  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) ||  T_FORK_TABLE != parse_tree.type_
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "fork table in sys tenant is");
    SQL_RESV_LOG(WARN, "fork table not supported in sys tenant", K(ret));
  } else if (lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "fork table in Oracle mode is");
    SQL_RESV_LOG(WARN, "fork table not supported in oracle mode", K(ret));
  } else if (MAX_NODE != parse_tree.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid fork table child count", K(ret), K(parse_tree.num_child_));
  } else if (OB_FAIL(check_fork_min_data_version(session_info_->get_effective_tenant_id()))) {
    SQL_RESV_LOG(WARN, "tenant data version does not support fork", KR(ret),
                 K(session_info_->get_effective_tenant_id()));
  } else if (OB_ISNULL(fork_table_stmt = create_stmt<ObForkTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "create fork table stmt failed", K(ret));
  } else {
    stmt_ = fork_table_stmt;
    obrpc::ObForkTableArg &fork_table_arg = fork_table_stmt->get_fork_table_arg();
    fork_table_arg.tenant_id_ = session_info_->get_effective_tenant_id();
  }

  if (OB_SUCC(ret)) {
    obrpc::ObForkTableArg &fork_table_arg = fork_table_stmt->get_fork_table_arg();
    ParseNode *dst_table_node = parse_tree.children_[DST_TABLE_NODE];
    ParseNode *src_table_node = parse_tree.children_[SRC_TABLE_NODE];
    ParseNode *read_lock_node = parse_tree.children_[READ_LOCK_NODE];
    ParseNode *copy_grants_node = parse_tree.children_[COPY_GRANTS_NODE];
    ObString dst_database_name;
    ObString dst_table_name;
    ObString src_database_name;
    ObString src_table_name;

    if (OB_ISNULL(dst_table_node) || OB_ISNULL(src_table_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(dst_table_node, dst_table_name, dst_database_name))) {
      SQL_RESV_LOG(WARN, "failed to resolve destination table", K(ret));
    } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == dst_database_name
               || ObString(OB_PUBLIC_SCHEMA_NAME) == dst_database_name) {
      ret = OB_OP_NOT_ALLOW;
      SQL_RESV_LOG(WARN, "fork table in recyclebin or public database is not allowed", K(ret),
                   K(dst_table_name), K(dst_database_name));
    } else if (OB_FAIL(resolve_table_relation_node(src_table_node, src_table_name, src_database_name))) {
      SQL_RESV_LOG(WARN, "failed to resolve source table", K(ret));
    } else {
      bool db_equal = false;
      bool table_equal = false;
      if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_, src_database_name, dst_database_name,
                                                 OB_TABLE_NAME_CLASS, db_equal))) {
        SQL_RESV_LOG(WARN, "failed to compare db names", K(src_database_name), K(dst_database_name), K(ret));
      } else if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_, src_table_name, dst_table_name,
                                                        OB_TABLE_NAME_CLASS, table_equal))) {
        SQL_RESV_LOG(WARN, "failed to compare table names", K(src_table_name), K(dst_table_name), K(ret));
      } else if (db_equal && table_equal) {
        ret = OB_ERR_NONUNIQ_TABLE;
        LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, src_table_name.length(), src_table_name.ptr());
      } else if (OB_FAIL(deep_copy_str(dst_database_name, fork_table_arg.dst_database_name_))) {
        SQL_RESV_LOG(WARN, "failed to deep copy dst database name", K(ret));
      } else if (OB_FAIL(deep_copy_str(dst_table_name, fork_table_arg.dst_table_name_))) {
        SQL_RESV_LOG(WARN, "failed to deep copy dst table name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_database_name, fork_table_arg.src_database_name_))) {
        SQL_RESV_LOG(WARN, "failed to deep copy src database name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_table_name, fork_table_arg.src_table_name_))) {
        SQL_RESV_LOG(WARN, "failed to deep copy src table name", K(ret));
      } else {
        fork_table_arg.need_lock_src_table_ = OB_NOT_NULL(read_lock_node);
        fork_table_arg.copy_grants_ = OB_NOT_NULL(copy_grants_node);
      }
    }
  }

  return ret;
}

ObForkDatabaseResolver::ObForkDatabaseResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObForkDatabaseResolver::~ObForkDatabaseResolver()
{
}

int ObForkDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObForkDatabaseStmt *fork_database_stmt = NULL;

  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) || T_FORK_DATABASE != parse_tree.type_
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "fork database in sys tenant is");
    SQL_RESV_LOG(WARN, "fork database not supported in sys tenant", K(ret));
  } else if (lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "fork database in Oracle mode is");
    SQL_RESV_LOG(WARN, "fork database not supported in oracle mode", K(ret));
  } else if (MAX_NODE != parse_tree.num_child_) {
    // Oracle still produces the two-child tree without COPY GRANTS; reject its mode first.
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid fork database child count", K(ret), K(parse_tree.num_child_));
  } else if (OB_FAIL(check_fork_min_data_version(session_info_->get_effective_tenant_id()))) {
    SQL_RESV_LOG(WARN, "tenant data version does not support fork", KR(ret),
                 K(session_info_->get_effective_tenant_id()));
  } else if (OB_ISNULL(fork_database_stmt = create_stmt<ObForkDatabaseStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "create fork database stmt failed", K(ret));
  } else {
    stmt_ = fork_database_stmt;
    obrpc::ObForkDatabaseArg &fork_database_arg = fork_database_stmt->get_fork_database_arg();
    fork_database_arg.tenant_id_ = session_info_->get_effective_tenant_id();
  }

  if (OB_SUCC(ret)) {
    obrpc::ObForkDatabaseArg &fork_database_arg = fork_database_stmt->get_fork_database_arg();
    ParseNode *dst_database_node = parse_tree.children_[DST_DATABASE_NODE];
    ParseNode *src_database_node = parse_tree.children_[SRC_DATABASE_NODE];
    ParseNode *copy_grants_node = parse_tree.children_[COPY_GRANTS_NODE];
    ObString dst_database_name;
    ObString src_database_name;

    if (OB_ISNULL(dst_database_node) || OB_ISNULL(src_database_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    } else if (T_IDENT != dst_database_node->type_ || T_IDENT != src_database_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree node type!", K(ret),
                   K(dst_database_node->type_), K(src_database_node->type_));
    } else {
      // Get destination and source database name
      dst_database_name.assign_ptr(dst_database_node->str_value_, static_cast<int32_t>(dst_database_node->str_len_));
      src_database_name.assign_ptr(src_database_node->str_value_, static_cast<int32_t>(src_database_node->str_len_));

      // Check and convert database names
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
        SQL_RESV_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
      } else {
        bool perserve_lettercase = lib::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
        ObCollationType cs_type = CS_TYPE_INVALID;
        if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
          SQL_RESV_LOG(WARN, "fail to get collation_connection", K(ret));
        } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                    cs_type, perserve_lettercase, dst_database_name))) {
          SQL_RESV_LOG(WARN, "fail to check and convert dst database name", K(dst_database_name), K(ret));
        } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                    cs_type, perserve_lettercase, src_database_name))) {
          SQL_RESV_LOG(WARN, "fail to check and convert src database name", K(src_database_name), K(ret));
        } else {
          bool db_equal = false;
          if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_, src_database_name, dst_database_name,
                                                     OB_TABLE_NAME_CLASS, db_equal))) {
            SQL_RESV_LOG(WARN, "failed to compare db names", K(src_database_name), K(dst_database_name), K(ret));
          } else if (db_equal) {
            ret = OB_DATABASE_EXIST;
            LOG_USER_ERROR(OB_DATABASE_EXIST, dst_database_name.length(), dst_database_name.ptr());
          } else if (OB_FAIL(deep_copy_str(dst_database_name, fork_database_arg.dst_database_name_))) {
            SQL_RESV_LOG(WARN, "failed to deep copy dst database name", K(ret));
          } else if (OB_FAIL(deep_copy_str(src_database_name, fork_database_arg.src_database_name_))) {
            SQL_RESV_LOG(WARN, "failed to deep copy src database name", K(ret));
          } else {
            fork_database_arg.copy_grants_ = OB_NOT_NULL(copy_grants_node);
          }
        }
      }
    }
  }

  return ret;
}

}
}

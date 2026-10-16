/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_logic_table_mysql.h"
#include "lib/worker.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_unit_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/location_cache/ob_location_service.h"
#include "share/location_cache/ob_location_struct.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/logical_table/logical_table_monitor.h"
#include "share/logical_table/ob_namespace_resource_limit.h"
#include "share/datum/ob_datum_funcs.h"
#include "lib/ob_define.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"

namespace oceanbase
{
namespace pl
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;

static const int64_t MAX_RETRY_TIMES = 3;

// Recyclebin prefix used as the soft-delete marker on sdk_namespaces rows.
// Kept at file scope so the prewarm "is in dropping state" probe and the
// drop_namespace rename / idempotency check stay in sync.
static const char *const RECYCLEBIN_PREFIX = "__recyclebin_";
static const int64_t RECYCLEBIN_PREFIX_LEN = sizeof("__recyclebin_") - 1;
static const char *const PREWARM_NAMESPACE_NOT_FOUND_MSG =
    "Namespace was not found in the current database; it may have been deleted or the "
    "namespace ID or context may be incorrect. PREWARM cannot proceed. Stop reusing this "
    "namespace object and verify its existence, ID and context before retrying.";
static const char *const PREWARM_NAMESPACE_DROPPING_MSG =
    "Namespace is being deleted; PREWARM cannot proceed. Stop reusing this namespace object "
    "and verify that the namespace exists before retrying.";

int ObDBMSLogicTableMySql::escape_sql_literal_(const ObString &value,
                                               ObSqlString &escaped)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(escaped.reserve(value.length() * 2 + 1))) {
    LOG_WARN("fail to reserve escaped sql literal buffer", KR(ret), K(value.length()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const char c = value.ptr()[i];
    if ('\'' == c) {
      if (OB_FAIL(escaped.append("''", 2))) {
        LOG_WARN("fail to append escaped quote", KR(ret));
      }
    } else if ('\\' == c) {
      if (OB_FAIL(escaped.append("\\\\", 2))) {
        LOG_WARN("fail to append escaped backslash", KR(ret));
      }
    } else if (OB_FAIL(escaped.append(&c, 1))) {
      LOG_WARN("fail to append sql literal char", KR(ret), K(i));
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::escape_sql_identifier_(const ObString &identifier,
                                                  ObSqlString &escaped)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(identifier.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql identifier is empty", KR(ret));
  } else if (OB_FAIL(escaped.reserve(identifier.length() * 2 + 1))) {
    LOG_WARN("fail to reserve escaped sql identifier buffer", KR(ret), K(identifier.length()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < identifier.length(); ++i) {
    const char c = identifier.ptr()[i];
    if ('`' == c) {
      if (OB_FAIL(escaped.append("``", 2))) {
        LOG_WARN("fail to append escaped backtick", KR(ret));
      }
    } else if (OB_FAIL(escaped.append(&c, 1))) {
      LOG_WARN("fail to append sql identifier char", KR(ret), K(i));
    }
  }
  return ret;
}

bool ObDBMSLogicTableMySql::is_benign_stale_exec_errcode_(const int errcode)
{
  // Inner SQL connections may retry transient location errors and still succeed
  // overall, leaving a stale errcode in the PL exec ctx.
  return OB_NOT_MASTER == errcode || OB_NEED_RETRY == errcode;
}

// collection_id is a CHAR(32) UUID hex string (without hyphens, per
// sdk_collections DDL: DEFAULT (replace(uuid(), '-', ''))).
// Restrict to hex digits to prevent SQL injection via string formatting.
static bool is_valid_collection_id(const ObString &s)
{
  if (OB_UNLIKELY(32 != s.length())) {
    return false;
  }
  for (int64_t i = 0; i < 32; ++i) {
    char c = s[i];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
      return false;
    }
  }
  return true;
}

// ========================= prewarm: context builder =========================

int ObDBMSLogicTableMySql::build_prewarm_context(ObPLExecCtx &ctx,
                                                  const ObString &collection_id,
                                                  uint64_t namespace_id,
                                                  PrewarmContext &prewarm_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pl exec ctx", KR(ret));
  } else if (OB_ISNULL(session = ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, session->get_database_name(),
                                     prewarm_ctx.db_name_))) {
    LOG_WARN("fail to copy db_name", KR(ret));
  } else if (OB_UNLIKELY(prewarm_ctx.db_name_.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("database name is empty, no database selected", KR(ret));
  } else if (OB_UNLIKELY(collection_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id is empty", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_collection_id(collection_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id format invalid, expected 32-char hex", KR(ret),
             K(collection_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id || 0 == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("namespace_id is invalid", KR(ret), K(namespace_id));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, collection_id,
                                     prewarm_ctx.collection_id_))) {
    LOG_WARN("fail to copy collection_id", KR(ret));
  } else {
    prewarm_ctx.tenant_id_     = session->get_effective_tenant_id();
    prewarm_ctx.namespace_id_  = namespace_id;
    prewarm_ctx.allocator_     = ctx.allocator_;
    prewarm_ctx.pl_ctx_        = &ctx;
  }
  return ret;
}

// ========================= prewarm: catalog helpers =========================

int ObDBMSLogicTableMySql::fetch_collection_meta(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT 1 FROM `%.*s`.`sdk_collections`"
              " WHERE collection_id = '%.*s'",
              db_name.length(), db_name.ptr(),
              ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
        LOG_WARN("fail to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, ctx.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to read sdk_collections", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("collection not found", KR(ret), K(ctx.collection_id_));
        } else {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      }
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

int ObDBMSLogicTableMySql::build_collection_related_table_names(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString kv_name;
  ObSqlString hot_name;
  ObSqlString logic_name;
  if (OB_FAIL(kv_name.assign_fmt("%.*s_kv_data_table",
                                  ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
    LOG_WARN("fail to format kv_data_table name", KR(ret));
  } else if (OB_FAIL(hot_name.assign_fmt("%.*s_hot_table",
                                          ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
    LOG_WARN("fail to format hot_table name", KR(ret));
  } else if (OB_FAIL(logic_name.assign_fmt("%.*s_logic_data_table",
                                            ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
    LOG_WARN("fail to format logic_data_table name", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, kv_name.string(), ctx.kv_data_table_name_))) {
    LOG_WARN("fail to copy kv_data_table_name", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, hot_name.string(), ctx.hot_table_name_))) {
    LOG_WARN("fail to copy hot_table_name", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, logic_name.string(), ctx.logic_data_table_name_))) {
    LOG_WARN("fail to copy logic_data_table_name", KR(ret));
  }
  return ret;
}

int ObDBMSLogicTableMySql::fetch_namespace_meta(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT namespace_name FROM `%.*s`.`sdk_namespaces`"
              " WHERE collection_id = '%.*s' AND namespace_id = %lu",
              db_name.length(), db_name.ptr(),
              ctx.collection_id_.length(), ctx.collection_id_.ptr(),
              ctx.namespace_id_))) {
        LOG_WARN("fail to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, ctx.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to read sdk_namespaces", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          FORWARD_USER_ERROR(ret, PREWARM_NAMESPACE_NOT_FOUND_MSG);
          LOG_WARN("namespace not found", KR(ret), K(ctx.collection_id_), K(ctx.namespace_id_));
        } else {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      } else {
        UNUSED(result);
      }
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

int ObDBMSLogicTableMySql::check_namespace_not_in_dropping_state(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT namespace_name FROM `%.*s`.`sdk_namespaces`"
              " WHERE collection_id = '%.*s' AND namespace_id = %lu",
              db_name.length(), db_name.ptr(),
              ctx.collection_id_.length(), ctx.collection_id_.ptr(),
              ctx.namespace_id_))) {
        LOG_WARN("fail to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, ctx.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to read sdk_namespaces", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          FORWARD_USER_ERROR(ret, PREWARM_NAMESPACE_NOT_FOUND_MSG);
          LOG_WARN("namespace not found", KR(ret), K(ctx.namespace_id_));
        } else {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      } else {
        ObString ns_name;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "namespace_name", ns_name);
        if (OB_SUCC(ret) && ns_name.prefix_match(RECYCLEBIN_PREFIX)) {
          ret = OB_STATE_NOT_MATCH;
          FORWARD_USER_ERROR(ret, PREWARM_NAMESPACE_DROPPING_MSG);
          LOG_WARN("namespace is in dropping state", KR(ret), K(ctx.namespace_id_), K(ns_name));
        }
      }
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

int ObDBMSLogicTableMySql::check_namespace_in_hot_table(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT 1 FROM `%.*s`.`%.*s` WHERE namespace_id = %lu LIMIT 1",
              db_name.length(), db_name.ptr(),
              ctx.hot_table_name_.length(), ctx.hot_table_name_.ptr(),
              ctx.namespace_id_))) {
        LOG_WARN("fail to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, ctx.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to read hot_table", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          ctx.hot_table_hit_ = false;
        } else {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      } else {
        ctx.hot_table_hit_ = true;
      }
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

int ObDBMSLogicTableMySql::update_hot_table_last_access_time(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    if (OB_FAIL(sql.assign_fmt(
            "UPDATE `%.*s`.`%.*s` SET last_access_time = NOW()"
            " WHERE namespace_id = %lu",
            db_name.length(), db_name.ptr(),
            ctx.hot_table_name_.length(), ctx.hot_table_name_.ptr(),
            ctx.namespace_id_))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to update hot_table", KR(ret));
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

int ObDBMSLogicTableMySql::insert_hot_table_record(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  int64_t retry_cnt = 0;

  do {
    if (OB_NOT_MASTER == ret) {
      ret = OB_SUCCESS;
      ob_usleep(1_s);
    }
    if (OB_FAIL(sql.assign_fmt(
            "INSERT IGNORE INTO `%.*s`.`%.*s`"
            " (namespace_id, last_access_time) VALUES (%lu, NOW())",
            db_name.length(), db_name.ptr(),
            ctx.hot_table_name_.length(), ctx.hot_table_name_.ptr(),
            ctx.namespace_id_))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to insert hot_table", KR(ret));
    }
  } while (OB_NOT_MASTER == ret && ++retry_cnt <= MAX_RETRY_TIMES);
  return ret;
}

// ========================= prewarm: tablet routing =========================

// Align KEY/HASH partition routing with SQL (ObTableLocation / ObTablePartCalculator).
static int calc_hash_like_part_func_obj_(
    const share::schema::ObTableSchema &table_schema,
    const uint64_t namespace_id,
    common::ObObj &part_func_obj)
{
  int ret = OB_SUCCESS;
  ObObj part_key_obj;
  part_key_obj.set_uint64(namespace_id);
  if (table_schema.is_hash_part()) {
    if (OB_FAIL(ObExprFuncPartHash::calc_value_for_mysql(
            part_key_obj, part_func_obj, part_key_obj.get_type()))) {
      LOG_WARN("calc hash part func value failed", KR(ret), K(namespace_id));
    }
  } else if (table_schema.is_key_part()) {
    ObDatum part_key_datum;
    char part_key_datum_buf[sizeof(uint64_t)];
    ObExprBasicFuncs *basic_funcs =
        ObDatumFuncs::get_basic_func(part_key_obj.get_type(), part_key_obj.get_collation_type());
    uint64_t hash_value = 0;
    if (OB_ISNULL(basic_funcs) || OB_ISNULL(basic_funcs->murmur_hash_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null basic funcs for key partition", KR(ret), K(namespace_id),
               K(part_key_obj));
    } else {
      part_key_datum.ptr_ = part_key_datum_buf;
      if (OB_FAIL(part_key_datum.from_obj(part_key_obj))) {
        LOG_WARN("fail to convert namespace id to datum", KR(ret), K(namespace_id));
      } else if (OB_FAIL(basic_funcs->murmur_hash_(part_key_datum, 0, hash_value))) {
        LOG_WARN("fail to calc key partition hash", KR(ret), K(namespace_id));
      } else {
        int64_t result_num = static_cast<int64_t>(hash_value);
        result_num = result_num < 0 ? -result_num : result_num;
        part_func_obj.set_int(result_num);
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported partition type for namespace tablet calc", KR(ret),
             "part_func_type", table_schema.get_part_option().get_part_func_type());
  }
  return ret;
}

static int calc_tablet_by_namespace_id_on_schema_(
    const share::schema::ObTableSchema &table_schema,
    const uint64_t namespace_id,
    common::ObTabletID &tablet_id,
    ObObjectID &part_id)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    if (OB_FAIL(table_schema.get_tablet_and_object_id(tablet_id, part_id))) {
      LOG_WARN("get_tablet_and_object_id failed for LEVEL_ZERO table",
               KR(ret), "table_id", table_schema.get_table_id());
    }
  } else if (table_schema.is_hash_like_part()) {
    ObObj part_func_obj;
    ObDASTabletMapper tablet_mapper;
    tablet_mapper.set_table_schema(&table_schema);
    ObSEArray<ObTabletID, 1> tablet_ids;
    ObSEArray<ObObjectID, 1> part_ids;
    if (OB_FAIL(calc_hash_like_part_func_obj_(table_schema, namespace_id, part_func_obj))) {
      LOG_WARN("fail to calc hash-like part func obj", KR(ret), K(namespace_id));
    } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(
                   table_schema.get_part_level(), OB_INVALID_ID, part_func_obj,
                   tablet_ids, part_ids))) {
      LOG_WARN("fail to calc tablet by hash-like part func obj", KR(ret),
               K(namespace_id), K(part_func_obj));
    } else if (tablet_ids.empty() || part_ids.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty tablet/part ids from hash-like routing", KR(ret),
               K(namespace_id), K(part_func_obj));
    } else {
      tablet_id = tablet_ids.at(0);
      part_id = part_ids.at(0);
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported partition scheme for namespace tablet calc", KR(ret),
             "table_id", table_schema.get_table_id(),
             "part_func_type", table_schema.get_part_option().get_part_func_type());
  }
  return ret;
}

int ObDBMSLogicTableMySql::calc_partition_and_tablet_by_namespace_id(
    PrewarmContext &ctx,
    const ObTableSchema &kv_table_schema)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObObjectID part_id = OB_INVALID_ID;
  if (OB_FAIL(calc_tablet_by_namespace_id_on_schema_(
          kv_table_schema, ctx.namespace_id_, tablet_id, part_id))) {
    LOG_WARN("failed to calc partition by namespace_id", KR(ret), K(ctx.namespace_id_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == part_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet/part id resolved for namespace",
             KR(ret), K(tablet_id), K(part_id), K(ctx.namespace_id_));
  } else {
    ctx.kv_tablet_id_   = tablet_id;
    ctx.part_object_id_ = part_id;
  }
  return ret;
}

int ObDBMSLogicTableMySql::resolve_tablet_location(PrewarmContext &ctx,
                                                   const bool force_renew)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObLSLocation ls_location;
  // A retry refreshes both tablet-to-LS and LS-to-replica mappings.
  const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;

  if (OB_FAIL(GCTX.location_service_->get(
          ctx.tenant_id_, ctx.kv_tablet_id_,
          expire_renew_time, is_cache_hit, ctx.ls_id_))) {
    LOG_WARN("fail to get ls_id for tablet", KR(ret), K(ctx.kv_tablet_id_));
  } else if (OB_FAIL(GCTX.location_service_->get(
                 GCONF.cluster_id, ctx.tenant_id_, ctx.ls_id_,
                 expire_renew_time, is_cache_hit, ls_location))) {
    LOG_WARN("fail to get ls location", KR(ret), K(ctx.ls_id_));
  } else {
    ctx.target_addrs_.reuse();
    if (ctx.leader_only_) {
      // Logical auxiliary rows map to this physical KV tablet, so the new
      // protocol warms only its LS leader.
      common::ObAddr leader_addr;
      if (OB_FAIL(ls_location.get_leader(leader_addr))) {
        LOG_WARN("fail to get prewarm ls leader", KR(ret), K(ctx.ls_id_), K(ls_location));
      } else if (OB_UNLIKELY(!leader_addr.is_valid())) {
        ret = OB_LS_LOCATION_NOT_EXIST;
        LOG_WARN("invalid prewarm ls leader", KR(ret), K(ctx.ls_id_), K(leader_addr));
      } else if (OB_FAIL(ctx.target_addrs_.push_back(leader_addr))) {
        LOG_WARN("fail to save prewarm ls leader", KR(ret), K(leader_addr));
      } else {
        LOG_INFO("[PREWARM] resolved leader only", K(ctx.ls_id_), K(leader_addr),
                 K(force_renew));
      }
    } else {
      // Until min data version reaches the protocol gate, old RPC handlers do
      // not reject followers. Preserve the legacy SS fanout so a mixed-version
      // cluster cannot silently warm only a stale follower location.
      const ObIArray<ObLSReplicaLocation> &replicas = ls_location.get_replica_locations();
      for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
        if (OB_FAIL(ctx.target_addrs_.push_back(replicas.at(i).get_server()))) {
          LOG_WARN("fail to save legacy prewarm replica", KR(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && GCTX.is_shared_storage_mode() && OB_NOT_NULL(GCTX.sql_proxy_)) {
        share::ObUnitTableOperator unit_op;
        common::ObArray<common::ObAddr> all_servers;
        if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
          LOG_WARN("fail to init unit operator for legacy prewarm", KR(ret));
        } else if (OB_FAIL(unit_op.get_alive_servers_by_tenant(ctx.tenant_id_, all_servers))) {
          LOG_WARN("fail to get tenant servers for legacy prewarm", KR(ret), K(ctx.tenant_id_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); ++i) {
            bool exists = false;
            for (int64_t j = 0; !exists && j < ctx.target_addrs_.count(); ++j) {
              exists = all_servers.at(i) == ctx.target_addrs_.at(j);
            }
            if (!exists && OB_FAIL(ctx.target_addrs_.push_back(all_servers.at(i)))) {
              LOG_WARN("fail to append tenant server for legacy prewarm", KR(ret), K(i));
            }
          }
        }
      }
      if (OB_SUCC(ret) && ctx.target_addrs_.empty()) {
        ret = OB_LS_LOCATION_NOT_EXIST;
        LOG_WARN("no legacy prewarm target found", KR(ret), K(ctx.ls_id_));
      } else if (OB_SUCC(ret)) {
        LOG_INFO("[PREWARM] resolved legacy fanout targets", K(ctx.ls_id_),
                 K(ctx.target_addrs_), K(force_renew));
      }
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::build_prewarm_rpc_arg(
    PrewarmContext &ctx, obrpc::ObPrewarmLogicTableArg &rpc_arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *kv_table_schema = nullptr;
  const ObString &db_name = ctx.db_name_;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 ctx.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 ctx.tenant_id_, db_name, ctx.kv_data_table_name_,
                 false/*is_index*/, kv_table_schema))) {
    LOG_WARN("fail to get kv_data_table schema", KR(ret), K(ctx.kv_data_table_name_));
  } else if (OB_ISNULL(kv_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("kv_data_table not found", KR(ret), K(ctx.kv_data_table_name_));
  } else if (OB_FAIL(calc_partition_and_tablet_by_namespace_id(ctx, *kv_table_schema))) {
    LOG_WARN("fail to calc partition", KR(ret), K(ctx.namespace_id_));
  }

  if (OB_SUCC(ret) && OB_FAIL(resolve_tablet_location(ctx))) {
    LOG_WARN("fail to resolve tablet location", KR(ret));
  }

  if (OB_SUCC(ret)) {
    rpc_arg.tenant_id_    = ctx.tenant_id_;
    rpc_arg.namespace_id_ = ctx.namespace_id_;
    rpc_arg.kv_table_id_  = kv_table_schema->get_table_id();
    rpc_arg.kv_tablet_id_ = ctx.kv_tablet_id_;
    rpc_arg.ls_id_        = ctx.ls_id_;
    rpc_arg.req_start_us_ = ObTimeUtility::current_time();
  }

  // Populate IVF prewarm fields if leader gathered them successfully.
  // IVF is always non-fatal: if attaching params fails, degrade to KV-only
  // fanout instead of polluting ret and aborting the KV prewarm.
  if (OB_SUCC(ret) && ctx.has_ivf_prewarm_) {
    rpc_arg.has_ivf_prewarm_      = true;
    rpc_arg.ivf_center_table_id_  = ctx.ivf_center_table_id_;
    rpc_arg.ivf_center_tablet_id_ = ctx.ivf_center_tablet_id_;
    int tmp_ret = rpc_arg.ivf_ltable_ids_.assign(ctx.ivf_ltable_ids_);
    if (OB_SUCCESS != tmp_ret) {
      rpc_arg.has_ivf_prewarm_ = false;
      rpc_arg.ivf_ltable_ids_.reset();
      LOG_WARN("[PREWARM] fail to assign ivf_ltable_ids, degrade to KV-only fanout",
               KR(tmp_ret), K(ctx.ivf_ltable_ids_));
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::do_one_prewarm_fanout(
    const obrpc::ObPrewarmLogicTableArg &rpc_arg,
    const common::ObIArray<common::ObAddr> &addrs_to_try,
    common::ObSEArray<common::ObAddr, 4> &succeeded_addrs,
    const bool leader_only,
    bool &need_retry,
    int &last_rpc_err)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  rootserver::ObPrewarmLogicTableProxy proxy(
      *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::prewarm_logic_table);
  // Prewarm is IO-heavy (pulls the whole namespace's macro blocks from object
  // storage), so budget it with the user statement's remaining timeout
  // (ob_query_timeout) instead of the 2s-default rpc_timeout.
  int64_t timeout_us = THIS_WORKER.get_timeout_remain();

  if (OB_UNLIKELY(timeout_us <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout before prewarm rpc", KR(ret), K(timeout_us), K(rpc_arg));
  }

  LOG_INFO("[PREWARM] fanout start", K(addrs_to_try.count()), K(timeout_us), K(rpc_arg));
  for (int64_t i = 0; OB_SUCC(ret) && i < addrs_to_try.count(); ++i) {
    const common::ObAddr &addr = addrs_to_try.at(i);
    LOG_INFO("[PREWARM] sending rpc to", K(addr), K(i));
    if (OB_TMP_FAIL(proxy.call(addr, timeout_us, rpc_arg))) {
      // The addr/arg/cb are usually already registered inside the proxy (e.g.
      // blacklist interception -4122 happens after registration and fills the
      // cb's result via on_timeout), so its outcome also surfaces in
      // ret_codes below. We do NOT keep a private ledger of sent addrs:
      // proxy.get_dests() is the single source of truth aligned with
      // ret_codes. Record the real send error here because on_timeout
      // rewrites the cb's rcode to OB_TIMEOUT, masking the original code.
      LOG_WARN("fail to call prewarm_logic_table, will retry", K(tmp_ret), K(addr));
      last_rpc_err = tmp_ret;
      need_retry = true;
    }
  }

  ObArray<int> ret_codes;
  // Must call wait_all even if ret is already non-SUCCESS from the call loop
  // above — otherwise in-flight RPC resources (buffers, connections) leak.
  // OB_TMP_FAIL is used intentionally (not OB_FAIL) so wait_all executes
  // regardless of the current ret value.
  if (OB_TMP_FAIL(proxy.wait_all(ret_codes))) {
    LOG_WARN("fail to wait all prewarm rpcs", KR(ret), KR(tmp_ret));
    last_rpc_err = tmp_ret;
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ret_codes.count() != proxy.get_dests().count())) {
    // wait_all produces one ret code per registered call (sent or not), in
    // cb_list order, strictly aligned with proxy.get_dests(). A mismatch here
    // is a genuine framework-level anomaly.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ret_codes/dests count mismatch", KR(ret),
             K(ret_codes.count()), K(proxy.get_dests().count()));
  }

  const common::ObIArray<common::ObAddr> &dests = proxy.get_dests();
  for (int64_t i = 0; OB_SUCC(ret) && i < ret_codes.count(); ++i) {
    int rpc_ret = ret_codes.at(i);
    const common::ObAddr &addr = dests.at(i);
    if (OB_SUCCESS == rpc_ret) {
      if (OB_FAIL(succeeded_addrs.push_back(addr))) {
        LOG_WARN("fail to record succeeded addr", KR(ret), K(addr));
      }
    } else if (OB_NOT_MASTER == rpc_ret
               || (leader_only
                   && (OB_EAGAIN == rpc_ret
                       || OB_NEED_RETRY == rpc_ret))) {
      // OB_NOT_MASTER fences a changed role epoch. OB_EAGAIN/OB_NEED_RETRY
      // mean the current leader generation is rebuilding or another writer
      // owns the same cache key. None of them may be counted as success,
      // otherwise the caller would commit a hot-table row without a cache.
      LOG_INFO("[PREWARM] leader/cache generation changed, will refresh and retry",
               K(rpc_ret), K(addr), K(leader_only));
      last_rpc_err = rpc_ret;
      need_retry = true;
    } else if (OB_LS_NOT_EXIST == rpc_ret
               || OB_TABLET_NOT_EXIST == rpc_ret) {
      if (leader_only) {
        // A leader-only target came from the LS location and must own the
        // routed tablet. Missing LS/tablet therefore indicates stale routing.
        LOG_INFO("[PREWARM] stale leader location, will refresh and retry",
                 K(rpc_ret), K(addr));
        last_rpc_err = rpc_ret;
        need_retry = true;
      } else if (OB_FAIL(succeeded_addrs.push_back(addr))) {
        LOG_WARN("fail to record legacy non-replica target", KR(ret), K(addr));
      } else {
        // Legacy SS fanout intentionally includes tenant servers without this
        // LS; old handlers treated those nodes as a successful no-op.
        LOG_INFO("[PREWARM] skip legacy non-replica target", K(rpc_ret), K(addr));
      }
    } else if (OB_TIMEOUT == rpc_ret
               || OB_RPC_POST_ERROR == rpc_ret
               || OB_RPC_SEND_ERROR == rpc_ret) {
      // Retryable transport-level failures. OB_RPC_POST_ERROR (-4122) is
      // typically blacklist interception after a stale-location round
      // (handler returned OB_LS_NOT_EXIST and the peer got blacklisted);
      // treat it as a stale-location signal: the outer retry loop
      // force-refreshes the location cache and re-resolves replica addrs,
      // so the blacklisted peer drops out of the next fanout. Note that a
      // send failure inside proxy.call() surfaces here as OB_TIMEOUT
      // (on_timeout rewrites the rcode); the original send error was
      // already recorded into last_rpc_err in the call loop above.
      LOG_WARN("[PREWARM] retryable rpc failure, will retry via location refresh",
               K(rpc_ret), K(addr));
      // Prefer a concrete error (e.g. -4122 recorded in the call loop) over
      // the generic OB_TIMEOUT rewritten by on_timeout.
      if (OB_TIMEOUT != rpc_ret || OB_SUCCESS == last_rpc_err) {
        last_rpc_err = rpc_ret;
      }
      need_retry = true;
    } else {
      ret = rpc_ret;
      LOG_WARN("prewarm_logic_table rpc failed", KR(ret), K(i), K(addr));
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::resolve_and_send_prewarm_rpc_with_retry(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  obrpc::ObPrewarmLogicTableArg rpc_arg;
  common::ObSEArray<common::ObAddr, 4> addrs_to_retry;
  common::ObSEArray<common::ObAddr, 4> succeeded_addrs;
  int last_rpc_err = OB_SUCCESS;

  if (OB_FAIL(build_prewarm_rpc_arg(ctx, rpc_arg))) {
    LOG_WARN("fail to build prewarm rpc arg", KR(ret));
  } else if (OB_FAIL(addrs_to_retry.assign(ctx.target_addrs_))) {
    LOG_WARN("fail to init addrs_to_retry", KR(ret));
  } else {
    bool done = false;
    for (int64_t retry = 0; OB_SUCC(ret) && !done && retry <= MAX_RETRY_TIMES; ++retry) {
      bool need_retry = false;
      if (OB_FAIL(do_one_prewarm_fanout(rpc_arg, addrs_to_retry, succeeded_addrs,
                                         ctx.leader_only_,
                                         need_retry, last_rpc_err))) {
        LOG_WARN("fail to do one prewarm fanout", KR(ret), K(retry));
      } else if (!need_retry) {
        done = true;
      } else if (retry == MAX_RETRY_TIMES) {
        // Out of retries: surface the last RPC error observed so operators can
        // trace which peer/error caused the failure, rather than masking it as
        // a generic OB_ERR_UNEXPECTED. Fall back to OB_EAGAIN if nothing was
        // recorded (e.g. need_retry set but last_rpc_err was never updated).
        ret = (OB_SUCCESS != last_rpc_err) ? last_rpc_err : OB_EAGAIN;
        LOG_WARN("max retry reached for prewarm_logic_table", KR(ret), K(retry),
                 K(last_rpc_err), K(addrs_to_retry), K(succeeded_addrs));
      } else {
        uint64_t retry_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), retry_data_version))) {
          LOG_WARN("fail to refresh data version before prewarm retry", KR(ret));
        } else {
          // The rolling-upgrade gate may advance between coordinator routing
          // and RPC handling. Refresh it on every retry so both sides converge.
          ctx.leader_only_ = retry_data_version >= DATA_VERSION_4_6_2_1;
        }
        // Refresh the physical KV tablet location. Logical centroid rows do
        // not require a separate route.
        if (OB_SUCC(ret) && OB_FAIL(resolve_tablet_location(ctx, true /*force_renew*/))) {
          LOG_WARN("fail to re-resolve leader location before retry", KR(ret));
        } else {
          // Keep rpc_arg.ls_id_ in sync with the (possibly migrated) ctx.ls_id_
          // before the next fanout iteration.
          rpc_arg.ls_id_ = ctx.ls_id_;
          addrs_to_retry.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < ctx.target_addrs_.count(); ++i) {
            const common::ObAddr &new_addr = ctx.target_addrs_.at(i);
            bool already_done = false;
            for (int64_t j = 0; !already_done && j < succeeded_addrs.count(); ++j) {
              if (new_addr == succeeded_addrs.at(j)) {
                already_done = true;
              }
            }
            if (!already_done && OB_FAIL(addrs_to_retry.push_back(new_addr))) {
              LOG_WARN("fail to build next addrs_to_retry", KR(ret), K(new_addr));
            }
          }
        }
        if (OB_SUCC(ret) && addrs_to_retry.empty()) {
          done = true;
        }
      }
    }
  }
  return ret;
}

// ========================= prewarm: IVF param collection =========================

int ObDBMSLogicTableMySql::append_quoted_identifier_(const ObString &identifier,
                                                     ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t segment_start = 0;
  if (identifier.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("identifier is empty", KR(ret));
  } else if (OB_FAIL(sql.append("`"))) {
    LOG_WARN("fail to append identifier quote", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < identifier.length(); ++i) {
      if ('`' == identifier.ptr()[i]) {
        if (i > segment_start
            && OB_FAIL(sql.append(identifier.ptr() + segment_start, i - segment_start))) {
          LOG_WARN("fail to append identifier segment", KR(ret));
        } else if (OB_FAIL(sql.append("``"))) {
          LOG_WARN("fail to append escaped identifier quote", KR(ret));
        } else {
          segment_start = i + 1;
        }
      }
    }
    if (OB_SUCC(ret) && segment_start < identifier.length()
        && OB_FAIL(sql.append(identifier.ptr() + segment_start,
                              identifier.length() - segment_start))) {
      LOG_WARN("fail to append remaining identifier segment", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(sql.append("`"))) {
      LOG_WARN("fail to append identifier quote", KR(ret));
    }
  }
  return ret;
}


int ObDBMSLogicTableMySql::collect_ltable_ids_for_namespace_(const PrewarmContext &ctx,
                                                             ObIArray<uint64_t> &ltable_ids)
{
  // Query sdk_ltables catalog for all ltable_ids under (collection_id, namespace_id).
  // This is the authoritative mapping table and avoids scanning kv_data_table.
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  ltable_ids.reset();
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT ltable_id FROM `%.*s`.`sdk_ltables`"
            " WHERE collection_id = '%.*s' AND namespace_id = %lu",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr(),
            ctx.namespace_id_))) {
      LOG_WARN("fail to format collect ltable_ids sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to read distinct ltable_id", KR(ret), K(ctx.tenant_id_),
               K(ctx.namespace_id_));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        uint64_t ltable_id = OB_INVALID_LOGICAL_TABLE_ID;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to iterate ltable_id result", KR(ret));
          }
        } else if (OB_FAIL(result->get_uint("ltable_id", ltable_id))) {
          LOG_WARN("fail to get ltable_id", KR(ret));
        }
        if (OB_SUCC(ret) && OB_INVALID_LOGICAL_TABLE_ID != ltable_id) {
          if (OB_FAIL(ltable_ids.push_back(ltable_id))) {
            LOG_WARN("fail to push ltable_id", KR(ret), K(ltable_id));
          }
        }
      }
    }
  }
  LOG_INFO("[PREWARM] collected ltable_ids for namespace",
           K(ctx.collection_id_), K(ctx.namespace_id_), K(ltable_ids));
  return ret;
}

// Gathers the parameters needed to include IVF cache warmup in the prewarm RPC arg.
// Locates the IVF centroid index table, calculates the centroid tablet for this
// namespace, and scans the namespace for all distinct ltable_ids.
// Sets ctx.has_ivf_prewarm_ = true on success; on any "skip" condition (no IVF
// index, no ltable_ids under namespace) leaves it false without returning an error.
// Hard errors (schema service failure, tablet calc failure) are returned so the
// caller can log them as non-fatal.
int ObDBMSLogicTableMySql::gather_ivf_prewarm_params(PrewarmContext &ctx)
{
  int ret = OB_SUCCESS;
  bool should_skip = false;

  // Step 1: scan namespace for all distinct ltable_ids. The prewarm PL interface
  // only takes (collection_id, namespace_id), so its semantics is "warm the whole
  // namespace". We must NOT honor a stale @ltable_id left on the session by an
  // unrelated prior INSERT.
  ObSEArray<uint64_t, 4> ltable_ids;
  if (OB_FAIL(collect_ltable_ids_for_namespace_(ctx, ltable_ids))) {
    LOG_WARN("[PREWARM] fail to collect ltable_ids for namespace", KR(ret),
             K(ctx.collection_id_), K(ctx.namespace_id_));
  } else if (ltable_ids.count() == 0) {
    LOG_INFO("[PREWARM] no ltable_id found under namespace, skip ivf prewarm",
             K(ctx.collection_id_), K(ctx.namespace_id_));
    should_skip = true;
  }

  // Step 2: locate IVF centroid index table from logic_data_table schema
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *kv_table_schema = nullptr;
  const ObTableSchema *centroid_schema = nullptr;
  uint64_t centroid_table_id = OB_INVALID_ID;
  if (OB_SUCC(ret) && !should_skip) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PREWARM] schema_service is null", KR(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   ctx.tenant_id_, schema_guard))) {
      LOG_WARN("[PREWARM] fail to get schema guard", KR(ret), K(ctx.tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(
                   ctx.tenant_id_, ctx.db_name_, ctx.logic_data_table_name_,
                   false/*is_index*/, kv_table_schema))) {
      LOG_WARN("[PREWARM] fail to get logic_data_table schema", KR(ret),
               K(ctx.logic_data_table_name_));
    } else if (OB_ISNULL(kv_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("[PREWARM] logic_data_table not found", KR(ret),
               K(ctx.logic_data_table_name_));
    } else {
      const ObIArray<ObAuxTableMetaInfo> &index_infos =
          kv_table_schema->get_simple_index_infos();
      for (int64_t i = 0; i < index_infos.count(); ++i) {
        if (share::schema::is_local_vec_ivf_centroid_index(
                index_infos.at(i).index_type_)) {
          centroid_table_id = index_infos.at(i).table_id_;
          break;
        }
      }
      if (OB_INVALID_ID == centroid_table_id) {
        LOG_INFO("[PREWARM] no IVF centroid index found, skip ivf prewarm",
                 K(ctx.collection_id_), K(ctx.namespace_id_));
        should_skip = true;
      }
    }
  }

  // Step 3: get centroid schema (needed for calc_tablet_by_namespace_id_on_schema_)
  if (OB_SUCC(ret) && !should_skip) {
    if (OB_FAIL(schema_guard.get_table_schema(
                   ctx.tenant_id_, centroid_table_id, centroid_schema))) {
      LOG_WARN("[PREWARM] fail to get centroid schema", KR(ret), K(centroid_table_id));
    } else if (OB_ISNULL(centroid_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("[PREWARM] centroid schema not found", KR(ret), K(centroid_table_id));
    }
  }

  // Step 4: calculate centroid tablet for this namespace
  ObTabletID centroid_tablet_id;
  ObObjectID centroid_part_id = OB_INVALID_ID;
  if (OB_SUCC(ret) && !should_skip) {
    if (OB_FAIL(calc_tablet_by_namespace_id_on_schema_(
                   *centroid_schema, ctx.namespace_id_,
                   centroid_tablet_id, centroid_part_id))) {
      LOG_WARN("[PREWARM] fail to calc centroid tablet by namespace_id", KR(ret),
               K(ctx.namespace_id_), K(centroid_table_id));
    }
  }

  // Step 5: store params into ctx
  if (OB_SUCC(ret) && !should_skip) {
    ctx.has_ivf_prewarm_      = true;
    ctx.ivf_center_table_id_  = centroid_table_id;
    ctx.ivf_center_tablet_id_ = centroid_tablet_id;
    if (OB_FAIL(ctx.ivf_ltable_ids_.assign(ltable_ids))) {
      LOG_WARN("[PREWARM] fail to assign ivf_ltable_ids", KR(ret));
      ctx.has_ivf_prewarm_ = false;
      ctx.ivf_ltable_ids_.reset();
    } else {
      LOG_INFO("[PREWARM] gathered ivf prewarm params", K(ctx.collection_id_),
               K(ctx.namespace_id_), K(centroid_table_id), K(centroid_tablet_id),
               K(ltable_ids));
    }
  }
  return ret;
}

// ========================= prewarm: entry =========================

int ObDBMSLogicTableMySql::prewarm(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString collection_id;
  uint64_t namespace_id  = OB_INVALID_ID;
  PrewarmContext prewarm_ctx;
  const bool is_ss_mode = GCTX.is_shared_storage_mode();
  uint64_t data_version = 0;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("DBMS_LOGIC_TABLE.prewarm is not supported in data version less than 4.6.1.0",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DBMS_LOGIC_TABLE.prewarm in data version less than 4.6.1.0");
  }

  CK(OB_LIKELY(2 == params.count()));
  CK(params.at(0).is_varchar_or_char());
  CK(params.at(1).is_uint64());
  if (OB_SUCC(ret)) {
    collection_id = params.at(0).get_varchar();
    namespace_id  = params.at(1).get_uint64();
  }

  OZ(build_prewarm_context(ctx, collection_id, namespace_id, prewarm_ctx));
  if (OB_SUCC(ret)) {
    // Both coordinator and handler switch behavior at the same cluster-wide
    // gate, which keeps rolling-upgrade routing symmetric.
    prewarm_ctx.leader_only_ = data_version >= DATA_VERSION_4_6_2_1;
  }
  OZ(fetch_collection_meta(prewarm_ctx));
  OZ(build_collection_related_table_names(prewarm_ctx));
  OZ(fetch_namespace_meta(prewarm_ctx));
  OZ(check_namespace_not_in_dropping_state(prewarm_ctx));

  // SS-only: hot_table bookkeeping and prewarm RPC fanout
  if (OB_SUCC(ret) && is_ss_mode) {
    OZ(check_namespace_in_hot_table(prewarm_ctx));
    if (OB_SUCC(ret)) {
      LOG_INFO("[PREWARM] entry", K(prewarm_ctx.collection_id_), K(prewarm_ctx.namespace_id_),
               K(prewarm_ctx.hot_table_hit_));
    }
    if (OB_SUCC(ret) && prewarm_ctx.hot_table_hit_) {
      OZ(update_hot_table_last_access_time(prewarm_ctx));
    }
    if (OB_SUCC(ret) && !prewarm_ctx.hot_table_hit_) {
      // Gather IVF prewarm params before building the RPC arg (non-fatal).
      // Failures only prevent IVF from being included in the fanout; KV prewarm
      // continues regardless.
      int ivf_gather_ret = gather_ivf_prewarm_params(prewarm_ctx);
      if (OB_SUCCESS != ivf_gather_ret) {
        LOG_WARN("[PREWARM] gather_ivf_prewarm_params failed, non-fatal",
                 K(ivf_gather_ret), K(prewarm_ctx.collection_id_),
                 K(prewarm_ctx.namespace_id_));
      }
      OZ(resolve_and_send_prewarm_rpc_with_retry(prewarm_ctx));
      OZ(insert_hot_table_record(prewarm_ctx));
    }
  }
  // NOTE: IVF cache warmup is carried inside the RPC fanout above (SS mode,
  // !hot_table_hit_ only). The leader gathers params via gather_ivf_prewarm_params
  // and build_prewarm_rpc_arg attaches them; each node's RPC handler executes the warmup.

  // ObInnerSQLConnection internally retries on OB_NOT_MASTER and may succeed
  // overall, but the stale errcode leaks into the PL exec ctx and propagates
  // up as the procedure's error code. Only clear it when our own logic ran to
  // completion AND we actually observed a stale errcode, and log it so the
  // root cause (which lives in the inner SQL retry path) stays visible.
  // TODO(prewarm): replace once the framework clears errcode on retry success.
  if (OB_SUCC(ret) && OB_NOT_NULL(ctx.exec_ctx_)) {
    const int stale_errcode = ctx.exec_ctx_->get_errcode();
    if (OB_SUCCESS != stale_errcode) {
      LOG_INFO("[PREWARM] clearing stale exec ctx errcode left by inner sql retry",
               K(stale_errcode));
      ctx.exec_ctx_->set_errcode(OB_SUCCESS);
    }
  }

  return ret;
}

// ========================= drop_namespace: context builder =========================
//
// DROP_NAMESPACE synchronous stage (short inner-SQL transaction).
// In one transaction we:
//   1) rename sdk_namespaces.namespace_name to '__recyclebin_<orig>_<ts_us>'
//      (idempotent guard: LEFT(name,13) <> '__recyclebin_') so SDKs lose the
//      original name and a recyclebin prefix exists for future background scans;
//   2) DELETE all sdk_ltables rows for (collection_id, namespace_id);
//   3) DELETE all rows in <collection_id>_logic_schema_table for namespace_id;
//   4) in shared-storage mode only, DELETE from <collection_id>_hot_table for
//      namespace_id (OB_TABLE_NOT_EXIST on hot_table is tolerated);
//   5) best-effort DELETE from sdk_namespaces_stats (OB_TABLE_NOT_EXIST skipped).
// Physical kv_data_table / logic_data_table data is NOT touched here — that
// remains for the background ObLTableBGTaskScheduler once merged.
//
// Concurrency: two callers racing on the same row are serialised by
// SELECT ... FOR UPDATE.  The second sees '__recyclebin_' already and commits
// an empty transaction (success, no double prefix).

int ObDBMSLogicTableMySql::build_drop_namespace_context(ObPLExecCtx &ctx,
                                                         const ObString &collection_id,
                                                         uint64_t namespace_id,
                                                         DropNamespaceContext &dns_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pl exec ctx", KR(ret));
  } else if (OB_ISNULL(session = ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_UNLIKELY(collection_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id is empty", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_collection_id(collection_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id format invalid, expected 32-char hex", KR(ret),
             K(collection_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id || 0 == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("namespace_id is invalid", KR(ret), K(namespace_id));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, session->get_database_name(),
                                     dns_ctx.db_name_))) {
    LOG_WARN("fail to copy db_name", KR(ret));
  } else if (OB_UNLIKELY(dns_ctx.db_name_.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("database name is empty, no database selected", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, collection_id,
                                     dns_ctx.collection_id_))) {
    LOG_WARN("fail to copy collection_id", KR(ret));
  } else {
    dns_ctx.tenant_id_     = session->get_effective_tenant_id();
    dns_ctx.namespace_id_  = namespace_id;
    dns_ctx.recycle_ts_us_ = ObTimeUtility::current_time();
    dns_ctx.allocator_     = ctx.allocator_;
    dns_ctx.pl_ctx_        = &ctx;
  }
  return ret;
}

// ========================= drop_namespace: collection existence =========================

int ObDBMSLogicTableMySql::check_collection_exists_in_trans(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT 1 FROM `%.*s`.`sdk_collections`"
            " WHERE collection_id = '%.*s'",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(ctx.trans_.read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to read sdk_collections", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("collection not found", KR(ret), K(ctx.collection_id_));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    }
  }
  return ret;
}

// ========================= drop_namespace: lock + load state =========================

int ObDBMSLogicTableMySql::lock_namespace_row_and_load_state(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString &db_name = ctx.db_name_;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT namespace_name FROM `%.*s`.`sdk_namespaces`"
            " WHERE collection_id = '%.*s' AND namespace_id = %lu FOR UPDATE",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr(),
            ctx.namespace_id_))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(ctx.trans_.read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to lock namespace row", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("namespace not found", KR(ret), K(ctx.collection_id_), K(ctx.namespace_id_));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    } else {
      ObString ns_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "namespace_name", ns_name);
      if (OB_SUCC(ret)) {
        if (ns_name.prefix_match(RECYCLEBIN_PREFIX)) {
          // Idempotent: another caller already converted this namespace.
          ctx.already_recycled_ = true;
          if (OB_FAIL(ob_write_string(*ctx.allocator_, ns_name, ctx.namespace_name_))) {
            LOG_WARN("fail to copy namespace_name", KR(ret));
          } else {
            LOG_INFO("[DROP_NS] namespace already in recyclebin, treat as no-op",
                     K(ctx.collection_id_), K(ctx.namespace_id_), K(ns_name));
          }
        } else if (OB_FAIL(ob_write_string(*ctx.allocator_, ns_name, ctx.namespace_name_))) {
          LOG_WARN("fail to copy namespace_name", KR(ret));
        }
      }
    }
  }
  return ret;
}

// ========================= drop_namespace: rename namespace row =========================

int ObDBMSLogicTableMySql::rename_namespace_to_recyclebin(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;

  // Format: __recyclebin_<original_name>_<ts_us>
  // We embed the original name (not just namespace_id) so background-task
  // operators / DBAs can identify the original entity from the recyclebin
  // entry without an extra join.
  if (OB_FAIL(ctx.pending_drop_namespace_name_.assign_fmt(
          "__recyclebin_%.*s_%ld",
          ctx.namespace_name_.length(), ctx.namespace_name_.ptr(),
          ctx.recycle_ts_us_))) {
    LOG_WARN("fail to format pending_drop_namespace_name", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%.*s`.`sdk_namespaces`"
                 " SET namespace_name = '%.*s'"
                 " WHERE collection_id = '%.*s' AND namespace_id = %lu"
                 " AND LEFT(namespace_name, 13) <> '__recyclebin_'",
                 db_name.length(), db_name.ptr(),
                 static_cast<int>(ctx.pending_drop_namespace_name_.length()),
                 ctx.pending_drop_namespace_name_.ptr(),
                 ctx.collection_id_.length(), ctx.collection_id_.ptr(),
                 ctx.namespace_id_))) {
    LOG_WARN("fail to format update sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update sdk_namespaces", KR(ret), K(sql));
  } else {
    LOG_INFO("[DROP_NS] namespace renamed to recyclebin", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(ctx.namespace_name_),
             "new_name", ctx.pending_drop_namespace_name_.string(),
             K(affected_rows));
  }
  return ret;
}

// ========================= drop_namespace: delete child ltables =========================

int ObDBMSLogicTableMySql::delete_ltables_for_namespace(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%.*s`.`sdk_ltables`"
          " WHERE collection_id = '%.*s' AND namespace_id = %lu",
          db_name.length(), db_name.ptr(),
          ctx.collection_id_.length(), ctx.collection_id_.ptr(),
          ctx.namespace_id_))) {
    LOG_WARN("fail to format delete ltables sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to delete child ltables", KR(ret), K(sql));
  } else {
    LOG_INFO("[DROP_NS] sdk_ltables rows deleted", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(affected_rows));
  }
  return ret;
}

// ========================= drop_namespace: cleanup stats =========================

int ObDBMSLogicTableMySql::cleanup_namespace_stats(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  // sdk_namespace_stat is a SDK-managed auxiliary table; the table itself
  // may not have been created by every SDK version.  Treat OB_TABLE_NOT_EXIST
  // as a best-effort skip; propagate any other error so the outer transaction
  // rolls back.
  if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%.*s`.`sdk_namespace_stat`"
          " WHERE collection_id = '%.*s' AND namespace_id = %lu",
          db_name.length(), db_name.ptr(),
          ctx.collection_id_.length(), ctx.collection_id_.ptr(),
          ctx.namespace_id_))) {
    LOG_WARN("fail to format delete stats sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_INFO("[DROP_NS] sdk_namespace_stat absent, skip", KR(ret),
               K(ctx.collection_id_), K(ctx.namespace_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete from sdk_namespace_stat", KR(ret), K(sql));
    }
  } else {
    LOG_INFO("[DROP_NS] sdk_namespace_stat rows deleted", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(affected_rows));
  }
  return ret;
}

// ========================= drop_namespace: delete logic_schema rows =========================

int ObDBMSLogicTableMySql::delete_logic_schema_rows(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  if (OB_UNLIKELY(ctx.logic_schema_table_name_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logic_schema_table_name_ is empty", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM `%.*s`.`%.*s` WHERE namespace_id = %lu",
                 db_name.length(), db_name.ptr(),
                 ctx.logic_schema_table_name_.length(),
                 ctx.logic_schema_table_name_.ptr(),
                 ctx.namespace_id_))) {
    LOG_WARN("fail to format delete logic_schema sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to delete from logic_schema_table", KR(ret), K(sql));
  } else {
    LOG_INFO("[DROP_NS] logic_schema_table rows deleted", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(affected_rows),
             "table", ctx.logic_schema_table_name_);
  }
  return ret;
}

// ========================= drop_namespace: delete hot_table row =========================

int ObDBMSLogicTableMySql::delete_hot_table_row(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString &db_name = ctx.db_name_;
  if (OB_UNLIKELY(ctx.hot_table_name_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hot_table_name_ is empty", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM `%.*s`.`%.*s` WHERE namespace_id = %lu",
                 db_name.length(), db_name.ptr(),
                 ctx.hot_table_name_.length(), ctx.hot_table_name_.ptr(),
                 ctx.namespace_id_))) {
    LOG_WARN("fail to format delete hot_table sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    // hot_table is SS-only and may not exist for collections created before SS
    // mode was enabled.  Tolerate OB_TABLE_NOT_EXIST silently.
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_INFO("[DROP_NS] hot_table absent, skip", KR(ret),
               K(ctx.collection_id_), K(ctx.namespace_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete from hot_table", KR(ret), K(sql));
    }
  } else {
    LOG_INFO("[DROP_NS] hot_table row deleted", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(affected_rows),
             "table", ctx.hot_table_name_);
  }
  return ret;
}

// ========================= drop_namespace: atomic transaction =========================

int ObDBMSLogicTableMySql::execute_drop_namespace_trans(DropNamespaceContext &ctx)
{
  int ret = OB_SUCCESS;
  const bool is_ss_mode = GCTX.is_shared_storage_mode();

  // Build per-collection derived physical-table names (hot_table /
  // logic_schema_table) once before opening the transaction so SQL formatters
  // can quote them.  Inlined here because DropNamespaceContext is a private
  // nested type and isn't reachable from a free function.
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret));
  } else if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else {
    ObSqlString hot;
    ObSqlString schema;
    if (OB_FAIL(hot.assign_fmt("%.*s_hot_table",
                                ctx.collection_id_.length(),
                                ctx.collection_id_.ptr()))) {
      LOG_WARN("fail to format hot_table name", KR(ret));
    } else if (OB_FAIL(schema.assign_fmt("%.*s_logic_schema_table",
                                          ctx.collection_id_.length(),
                                          ctx.collection_id_.ptr()))) {
      LOG_WARN("fail to format logic_schema_table name", KR(ret));
    } else if (OB_FAIL(ob_write_string(*ctx.allocator_, hot.string(),
                                        ctx.hot_table_name_))) {
      LOG_WARN("fail to copy hot_table_name_", KR(ret));
    } else if (OB_FAIL(ob_write_string(*ctx.allocator_, schema.string(),
                                        ctx.logic_schema_table_name_))) {
      LOG_WARN("fail to copy logic_schema_table_name_", KR(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ctx.trans_.start(GCTX.sql_proxy_, ctx.tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K(ctx.tenant_id_));
  }

  // 1. Validate collection_id exists.
  if (OB_SUCC(ret) && OB_FAIL(check_collection_exists_in_trans(ctx))) {
    LOG_WARN("collection check failed", KR(ret));
  }

  // 2. SELECT ... FOR UPDATE the namespace row; detect already-recycled state.
  if (OB_SUCC(ret) && OB_FAIL(lock_namespace_row_and_load_state(ctx))) {
    LOG_WARN("fail to lock namespace row", KR(ret));
  }

  // 3. Fast path: another caller already renamed this namespace.  Treat as
  //    success (idempotent no-op) and just commit the empty transaction so
  //    the FOR UPDATE lock is released cleanly.
  if (OB_SUCC(ret) && ctx.already_recycled_) {
    LOG_INFO("[DROP_NS] no-op: namespace already in recyclebin",
             K(ctx.collection_id_), K(ctx.namespace_id_));
  } else if (OB_SUCC(ret)) {
    // 4. Rename namespace row so SDK clients no longer resolve it; background
    //    namespace-deletion task will sweep it eventually.
    if (OB_FAIL(rename_namespace_to_recyclebin(ctx))) {
      LOG_WARN("fail to rename namespace", KR(ret));
    }
    // 5. Drop all child ltable mapping rows synchronously.
    if (OB_SUCC(ret) && OB_FAIL(delete_ltables_for_namespace(ctx))) {
      LOG_WARN("fail to delete child ltables", KR(ret));
    }
    // 6. Drop logic_schema rows for this namespace.  Hard error if the table
    //    is missing — namespace-mode collections always create it.
    if (OB_SUCC(ret) && OB_FAIL(delete_logic_schema_rows(ctx))) {
      LOG_WARN("fail to delete logic_schema rows", KR(ret));
    }
    // 7. Drop hot_table row.  SS-only.
    if (OB_SUCC(ret) && is_ss_mode && OB_FAIL(delete_hot_table_row(ctx))) {
      LOG_WARN("fail to delete hot_table row", KR(ret));
    }
    // 8. Clean SDK-side stats rows.  Best-effort: missing table is OK.
    if (OB_SUCC(ret) && OB_FAIL(cleanup_namespace_stats(ctx))) {
      LOG_WARN("fail to cleanup namespace stats", KR(ret));
    }
  }

  if (ctx.trans_.is_started()) {
    const bool commit = OB_SUCC(ret);
    const int end_ret = ctx.trans_.end(commit);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("fail to end transaction", K(end_ret), K(commit));
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
    LOG_INFO("[DROP_NS] transaction ended", K(commit), K(ret),
             K(ctx.collection_id_), K(ctx.namespace_id_),
             K(ctx.already_recycled_), K(is_ss_mode));
  }
  return ret;
}

// ========================= drop_namespace: entry =========================

int ObDBMSLogicTableMySql::drop_namespace(ObPLExecCtx &ctx, sql::ParamStore &params,
                                           common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString collection_id;
  uint64_t namespace_id  = OB_INVALID_ID;
  DropNamespaceContext dns_ctx;
  uint64_t data_version = 0;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("DBMS_LOGIC_TABLE.drop_namespace is not supported in data version less than 4.6.1.0",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DBMS_LOGIC_TABLE.drop_namespace in data version less than 4.6.1.0");
  }

  CK(OB_LIKELY(2 == params.count()));
  CK(params.at(0).is_varchar_or_char());
  CK(params.at(1).is_uint64());
  if (OB_SUCC(ret)) {
    collection_id = params.at(0).get_varchar();
    namespace_id  = params.at(1).get_uint64();
  }

  OZ(build_drop_namespace_context(ctx, collection_id, namespace_id, dns_ctx));
  OZ(execute_drop_namespace_trans(dns_ctx));

  // Same workaround as prewarm(): the inner SQL connection's OB_NOT_MASTER
  // retry path can leave a stale errcode in the PL exec ctx even after the
  // statement ultimately succeeds. Clear it defensively and surface the fact
  // via a log line so the underlying framework issue stays observable.
  // TODO(drop_namespace): remove once the framework clears errcode on retry success.
  if (OB_SUCC(ret) && OB_NOT_NULL(ctx.exec_ctx_)) {
    const int stale_errcode = ctx.exec_ctx_->get_errcode();
    if (OB_SUCCESS != stale_errcode) {
      LOG_INFO("[DROP_NS] clearing stale exec ctx errcode left by inner sql retry",
               K(stale_errcode));
      ctx.exec_ctx_->set_errcode(OB_SUCCESS);
    }
  }

  return ret;
}

// ========================= drop_ltable: context builder =========================

int ObDBMSLogicTableMySql::build_drop_ltable_context(ObPLExecCtx &ctx,
                                                      const ObString &collection_id,
                                                      uint64_t namespace_id,
                                                      uint64_t ltable_id,
                                                      DropLtableContext &dlt_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pl exec ctx", KR(ret));
  } else if (OB_ISNULL(session = ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_UNLIKELY(collection_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id is empty", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_collection_id(collection_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection_id format invalid, expected 32-char hex", KR(ret),
             K(collection_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id || 0 == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("namespace_id is invalid", KR(ret), K(namespace_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == ltable_id || 0 == ltable_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ltable_id is invalid", KR(ret), K(ltable_id));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, session->get_database_name(),
                                     dlt_ctx.db_name_))) {
    LOG_WARN("fail to copy db_name", KR(ret));
  } else if (OB_UNLIKELY(dlt_ctx.db_name_.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("database name is empty, no database selected", KR(ret));
  } else if (OB_FAIL(escape_sql_identifier_(dlt_ctx.db_name_,
                                             dlt_ctx.escaped_db_name_))) {
    LOG_WARN("fail to escape database name", KR(ret), K(dlt_ctx.db_name_));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, collection_id,
                                     dlt_ctx.collection_id_))) {
    LOG_WARN("fail to copy collection_id", KR(ret));
  } else {
    dlt_ctx.tenant_id_     = session->get_effective_tenant_id();
    dlt_ctx.namespace_id_  = namespace_id;
    dlt_ctx.ltable_id_     = ltable_id;
    dlt_ctx.recycle_ts_us_ = ObTimeUtility::current_time();
    dlt_ctx.allocator_     = ctx.allocator_;
    dlt_ctx.pl_ctx_        = &ctx;
  }
  return ret;
}

int ObDBMSLogicTableMySql::check_collection_exists_in_trans(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString db_name = ctx.escaped_db_name_.string();
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT 1 FROM `%.*s`.`sdk_collections`"
            " WHERE collection_id = '%.*s'",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr()))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(ctx.trans_.read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to read sdk_collections", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("collection not found", KR(ret), K(ctx.collection_id_));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::lock_namespace_row_for_drop_ltable(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString db_name = ctx.escaped_db_name_.string();

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT namespace_name FROM `%.*s`.`sdk_namespaces`"
            " WHERE collection_id = '%.*s' AND namespace_id = %lu FOR UPDATE",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr(),
            ctx.namespace_id_))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(ctx.trans_.read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to lock namespace row", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("namespace not found", KR(ret), K(ctx.collection_id_), K(ctx.namespace_id_));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    } else {
      ObString ns_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "namespace_name", ns_name);
      if (OB_SUCC(ret) && ns_name.prefix_match(RECYCLEBIN_PREFIX)) {
        ctx.namespace_already_recycled_ = true;
        LOG_INFO("[DROP_LT] namespace already in recyclebin",
                 K(ctx.collection_id_), K(ctx.namespace_id_), K(ns_name));
      }
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::lock_ltable_row_and_load_state(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const ObString db_name = ctx.escaped_db_name_.string();

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT ltable_name FROM `%.*s`.`sdk_ltables`"
            " WHERE collection_id = '%.*s' AND namespace_id = %lu"
            " AND ltable_id = %lu FOR UPDATE",
            db_name.length(), db_name.ptr(),
            ctx.collection_id_.length(), ctx.collection_id_.ptr(),
            ctx.namespace_id_, ctx.ltable_id_))) {
      LOG_WARN("fail to format sql", KR(ret));
    } else if (OB_FAIL(ctx.trans_.read(res, ctx.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to lock ltable row", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        ctx.ltable_already_recycled_ = true;
        LOG_INFO("[DROP_LT] ltable catalog row already removed, treat as no-op",
                 K(ctx.collection_id_), K(ctx.namespace_id_), K(ctx.ltable_id_));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    } else {
      ObString lt_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ltable_name", lt_name);
      if (OB_SUCC(ret)) {
        if (lt_name.prefix_match(RECYCLEBIN_PREFIX)) {
          ctx.ltable_already_recycled_ = true;
          LOG_INFO("[DROP_LT] ltable already in recyclebin, treat as no-op",
                   K(ctx.collection_id_), K(ctx.namespace_id_), K(ctx.ltable_id_),
                   K(lt_name));
        } else if (OB_FAIL(ob_write_string(*ctx.allocator_, lt_name, ctx.ltable_name_))) {
          LOG_WARN("fail to copy ltable_name", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::rename_ltable_to_recyclebin(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString escaped_pending_name;
  int64_t affected_rows = 0;
  const ObString db_name = ctx.escaped_db_name_.string();

  if (OB_FAIL(ctx.pending_drop_ltable_name_.assign_fmt(
          "__recyclebin_%.*s_%ld",
          ctx.ltable_name_.length(), ctx.ltable_name_.ptr(),
          ctx.recycle_ts_us_))) {
    LOG_WARN("fail to format pending_drop_ltable_name", KR(ret));
  } else if (OB_FAIL(escape_sql_literal_(ctx.pending_drop_ltable_name_.string(),
                                         escaped_pending_name))) {
    LOG_WARN("fail to escape pending_drop_ltable_name", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%.*s`.`sdk_ltables`"
                 " SET ltable_name = '%.*s'"
                 " WHERE collection_id = '%.*s' AND namespace_id = %lu"
                 " AND ltable_id = %lu"
                 " AND LEFT(ltable_name, 13) <> '__recyclebin_'",
                 db_name.length(), db_name.ptr(),
                 static_cast<int>(escaped_pending_name.length()),
                 escaped_pending_name.ptr(),
                 ctx.collection_id_.length(), ctx.collection_id_.ptr(),
                 ctx.namespace_id_, ctx.ltable_id_))) {
    LOG_WARN("fail to format update sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update sdk_ltables", KR(ret), K(sql));
  } else {
    LOG_INFO("[DROP_LT] ltable renamed to recyclebin", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(ctx.ltable_id_), K(ctx.ltable_name_),
             "new_name", ctx.pending_drop_ltable_name_.string(),
             K(affected_rows));
  }
  return ret;
}

int ObDBMSLogicTableMySql::delete_logic_schema_rows_for_ltable(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObString db_name = ctx.escaped_db_name_.string();
  if (OB_UNLIKELY(ctx.logic_schema_table_name_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logic_schema_table_name_ is empty", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM `%.*s`.`%.*s`"
                 " WHERE namespace_id = %lu AND ltable_id = %lu",
                 db_name.length(), db_name.ptr(),
                 ctx.logic_schema_table_name_.length(),
                 ctx.logic_schema_table_name_.ptr(),
                 ctx.namespace_id_, ctx.ltable_id_))) {
    LOG_WARN("fail to format delete logic_schema sql", KR(ret));
  } else if (OB_FAIL(ctx.trans_.write(ctx.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to delete from logic_schema_table", KR(ret), K(sql));
  } else {
    LOG_INFO("[DROP_LT] logic_schema_table rows deleted", K(ctx.collection_id_),
             K(ctx.namespace_id_), K(ctx.ltable_id_), K(affected_rows),
             "table", ctx.logic_schema_table_name_);
  }
  return ret;
}

int ObDBMSLogicTableMySql::execute_drop_ltable_trans(DropLtableContext &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret));
  } else if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else {
    ObSqlString schema;
    if (OB_FAIL(schema.assign_fmt("%.*s_logic_schema_table",
                                          ctx.collection_id_.length(),
                                          ctx.collection_id_.ptr()))) {
      LOG_WARN("fail to format logic_schema_table name", KR(ret));
    } else if (OB_FAIL(ob_write_string(*ctx.allocator_, schema.string(),
                                        ctx.logic_schema_table_name_))) {
      LOG_WARN("fail to copy logic_schema_table_name_", KR(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ctx.trans_.start(GCTX.sql_proxy_, ctx.tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K(ctx.tenant_id_));
  }
  if (OB_SUCC(ret) && OB_FAIL(check_collection_exists_in_trans(ctx))) {
    LOG_WARN("collection check failed", KR(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(lock_namespace_row_for_drop_ltable(ctx))) {
    LOG_WARN("fail to lock namespace row", KR(ret));
  }
  if (OB_SUCC(ret) && ctx.namespace_already_recycled_) {
    // DROP_NAMESPACE has already taken ownership of all child-resource
    // cleanup.  A concurrent or retried DROP_LTABLE is therefore complete.
    LOG_INFO("[DROP_LT] no-op: parent namespace already in recyclebin",
             K(ctx.collection_id_), K(ctx.namespace_id_), K(ctx.ltable_id_));
  } else if (OB_SUCC(ret) && OB_FAIL(lock_ltable_row_and_load_state(ctx))) {
    LOG_WARN("fail to lock ltable row", KR(ret));
  }
  if (OB_SUCC(ret) && ctx.ltable_already_recycled_) {
    LOG_INFO("[DROP_LT] no-op: ltable already in recyclebin",
             K(ctx.collection_id_), K(ctx.namespace_id_), K(ctx.ltable_id_));
  } else if (OB_SUCC(ret)) {
    if (OB_FAIL(rename_ltable_to_recyclebin(ctx))) {
      LOG_WARN("fail to rename ltable", KR(ret));
    } else if (OB_FAIL(delete_logic_schema_rows_for_ltable(ctx))) {
      LOG_WARN("fail to delete logic_schema rows", KR(ret));
    }
    // hot_table is keyed by namespace_id only (prewarm/TTL is per-namespace).
    // Do not touch it when dropping a single ltable within the namespace.
  }
  if (ctx.trans_.is_started()) {
    const bool commit = OB_SUCC(ret);
    const int end_ret = ctx.trans_.end(commit);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("fail to end drop_ltable transaction", K(end_ret), K(commit));
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
  }
  return ret;
}

// ========================= drop_ltable: entry =========================

int ObDBMSLogicTableMySql::drop_ltable(ObPLExecCtx &ctx, sql::ParamStore &params,
                                        common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString collection_id;
  uint64_t namespace_id = OB_INVALID_ID;
  uint64_t ltable_id    = OB_INVALID_ID;
  DropLtableContext dlt_ctx;
  uint64_t data_version = 0;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("DBMS_LOGIC_TABLE.drop_ltable is not supported in data version less than 4.6.1.0",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DBMS_LOGIC_TABLE.drop_ltable in data version less than 4.6.1.0");
  }

  CK(OB_LIKELY(3 == params.count()));
  CK(params.at(0).is_varchar_or_char());
  CK(params.at(1).is_uint64());
  CK(params.at(2).is_uint64());
  if (OB_SUCC(ret)) {
    collection_id = params.at(0).get_varchar();
    namespace_id  = params.at(1).get_uint64();
    ltable_id     = params.at(2).get_uint64();
  }

  OZ(build_drop_ltable_context(ctx, collection_id, namespace_id, ltable_id, dlt_ctx));
  OZ(execute_drop_ltable_trans(dlt_ctx));

  if (OB_SUCC(ret) && OB_NOT_NULL(ctx.exec_ctx_)) {
    const int stale_errcode = ctx.exec_ctx_->get_errcode();
    if (is_benign_stale_exec_errcode_(stale_errcode)) {
      LOG_INFO("[DROP_LT] clearing stale exec ctx errcode left by inner sql retry",
               K(stale_errcode));
      ctx.exec_ctx_->set_errcode(OB_SUCCESS);
    } else if (OB_SUCCESS != stale_errcode) {
      ret = stale_errcode;
      LOG_WARN("[DROP_LT] propagate unexpected exec ctx errcode after successful transaction",
               KR(ret));
    }
  }

  return ret;
}

// ========================= set_namespace_resource_limit =========================

static bool is_safe_catalog_name(const ObString &s, const int64_t max_len)
{
  bool ok = !s.empty() && s.length() <= max_len;
  for (int64_t i = 0; ok && i < s.length(); ++i) {
    const char c = s.ptr()[i];
    ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
         || (c >= 'A' && c <= 'Z') || c == '_';
  }
  return ok;
}

int ObDBMSLogicTableMySql::set_namespace_resource_limit(ObPLExecCtx &ctx, sql::ParamStore &params,
                                                     common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  ObString collection_name;
  ObString namespace_name;
  ObString config_json;
  ObString db_name;
  uint64_t data_version = 0;
  const uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("DBMS_LOGIC_TABLE.set_namespace_resource_limit is not supported in data version less than 4.6.1.0",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "DBMS_LOGIC_TABLE.set_namespace_resource_limit in data version less than 4.6.1.0");
  }

  CK(OB_LIKELY(3 == params.count()));
  if (OB_SUCC(ret)) {
    collection_name = params.at(0).get_varchar();
    namespace_name = params.at(1).get_varchar();
    config_json = params.at(2).get_varchar();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("set_namespace_resource_limit is only allowed in user tenant", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set_namespace_resource_limit in non-user tenant");
  } else if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(session = ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pl exec ctx or session", KR(ret));
  } else if (OB_FAIL(ob_write_string(*ctx.allocator_, session->get_database_name(), db_name))) {
    LOG_WARN("fail to copy db_name", KR(ret));
  } else if (OB_UNLIKELY(db_name.empty())) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("database name is empty", KR(ret));
  } else if (OB_UNLIKELY(!is_safe_catalog_name(db_name, 128)
                         || !is_safe_catalog_name(collection_name, 256)
                         || !is_safe_catalog_name(namespace_name, 256))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid db_name, collection_name or namespace_name", KR(ret),
             K(db_name), K(collection_name), K(namespace_name));
  } else if (OB_FAIL(share::ObNamespaceResourceLimitValidator::validate_patch_json(config_json))) {
    LOG_WARN("invalid namespace resource limit json", KR(ret), K(config_json));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "config_json for set_namespace_resource_limit");
  } else {
    ObSqlString escaped_json;
    ObSqlString sql;
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(escape_sql_literal_(config_json, escaped_json))) {
      LOG_WARN("fail to escape config json", KR(ret));
    } else if (OB_FAIL(trans.start(ctx.exec_ctx_->get_sql_proxy(), tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret));
    } else if (OB_FAIL(sql.assign_fmt(
        "UPDATE `%.*s`.`sdk_namespaces` n "
        "JOIN `%.*s`.`sdk_collections` c ON n.collection_id = c.collection_id "
        "SET n.info = JSON_MERGE_PATCH(COALESCE(n.info, CAST('{}' AS JSON)), CAST('%.*s' AS JSON)) "
        "WHERE c.collection_name = '%.*s' AND n.namespace_name = '%.*s'",
        db_name.length(), db_name.ptr(),
        db_name.length(), db_name.ptr(),
        escaped_json.length(), escaped_json.ptr(),
        collection_name.length(), collection_name.ptr(),
        namespace_name.length(), namespace_name.ptr()))) {
      LOG_WARN("fail to format update sql", KR(ret));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to update sdk_namespaces info", KR(ret), K(sql));
    } else if (0 == affected_rows) {
      ObSqlString check_sql;
      if (OB_FAIL(check_sql.assign_fmt(
          "SELECT 1 FROM `%.*s`.`sdk_namespaces` n "
          "JOIN `%.*s`.`sdk_collections` c ON n.collection_id = c.collection_id "
          "WHERE c.collection_name = '%.*s' AND n.namespace_name = '%.*s' LIMIT 1",
          db_name.length(), db_name.ptr(),
          db_name.length(), db_name.ptr(),
          collection_name.length(), collection_name.ptr(),
          namespace_name.length(), namespace_name.ptr()))) {
        LOG_WARN("fail to format namespace existence check sql", KR(ret));
      } else {
        HEAP_VAR(ObMySQLProxy::ReadResult, check_res) {
          ObMySQLResult *check_result = nullptr;
          if (OB_FAIL(trans.read(check_res, tenant_id, check_sql.ptr()))) {
            LOG_WARN("fail to check namespace existence", KR(ret), K(check_sql));
          } else if (OB_ISNULL(check_result = check_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("existence check result is null", KR(ret));
          } else if (OB_FAIL(check_result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_ENTRY_NOT_EXIST;
              LOG_WARN("namespace not found", KR(ret), K(collection_name), K(namespace_name));
            } else {
              LOG_WARN("fail to fetch existence check row", KR(ret));
            }
          } else {
            LOG_INFO("[SET_NS_OPS] namespace ops config unchanged (idempotent)",
                     K(collection_name), K(namespace_name), K(config_json));
          }
        }
      }
    } else {
      LOG_INFO("[SET_NS_OPS] updated namespace ops config",
               K(collection_name), K(namespace_name), K(config_json), K(affected_rows));
    }
    if (trans.is_started()) {
      const int end_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != end_ret && OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(ctx.exec_ctx_)) {
    const int stale_errcode = ctx.exec_ctx_->get_errcode();
    if (OB_SUCCESS != stale_errcode) {
      if (is_benign_stale_exec_errcode_(stale_errcode)) {
        LOG_INFO("[SET_NS_OPS] clearing stale exec ctx errcode left by inner sql retry",
                 K(stale_errcode));
        ctx.exec_ctx_->set_errcode(OB_SUCCESS);
      } else {
        LOG_WARN("[SET_NS_OPS] keep unexpected exec ctx errcode after successful update",
                 K(stale_errcode));
      }
    }
  }
  return ret;
}

int ObDBMSLogicTableMySql::gather_namespace_stats_inner(ObPLExecCtx &ctx,
                                                        sql::ParamStore &params,
                                                        common::ObObj &result)
{
  UNUSED(ctx);
  UNUSED(params);
  UNUSED(result);
  int ret = OB_SUCCESS;
  share::ObLogicalTableMonitor *monitor = MTL(share::ObLogicalTableMonitor *);
  if (OB_ISNULL(monitor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical table monitor is null", KR(ret));
  } else if (OB_FAIL(monitor->run_monitor_round())) {
    LOG_WARN("fail to run logic table namespace stats gather round", KR(ret));
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_direct_load_stream.h"
#include "lib/time/ob_time_utility.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/table/ob_direct_load_stream_queue_list.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace observer;
using namespace sql;

namespace
{
const int64_t MICROSECONDS_PER_SECOND = 1000 * 1000L;

int get_int64_param(const ParamStore &params, const int64_t idx, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param index", KR(ret), K(idx), K(params.count()));
  } else if (params.at(idx).is_int()) {
    value = params.at(idx).get_int();
  } else if (params.at(idx).is_uint64()) {
    value = static_cast<int64_t>(params.at(idx).get_uint64());
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param type", KR(ret), K(idx), K(params.at(idx)));
  }
  return ret;
}

int get_positive_int64_param(const ParamStore &params, const int64_t idx, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_int64_param(params, idx, value))) {
  } else if (OB_UNLIKELY(value <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("non-positive param value", KR(ret), K(idx), K(value));
  }
  return ret;
}

int get_string_param(const ParamStore &params, const int64_t idx, common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param index", KR(ret), K(idx), K(params.count()));
  } else if (OB_FAIL(params.at(idx).get_string(value))) {
    LOG_WARN("fail to get string param", KR(ret), K(idx), K(params.at(idx)));
  } else if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty string param", KR(ret), K(idx));
  }
  return ret;
}

int get_stream_id_param(const ParamStore &params, const int64_t idx, int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_int64_param(params, idx, stream_id))) {
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream id", KR(ret), K(idx), K(stream_id));
  }
  return ret;
}

int calc_expire_ts(const int64_t ttl_seconds, int64_t &expire_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now_ts = common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(ttl_seconds <= 0
      || ttl_seconds > (INT64_MAX - now_ts) / MICROSECONDS_PER_SECOND)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream ttl", KR(ret), K(ttl_seconds), K(now_ts));
  } else {
    expire_ts = now_ts + ttl_seconds * MICROSECONDS_PER_SECOND;
  }
  return ret;
}

int check_target_table_insert_privilege(sql::ObSQLSessionInfo &session,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const share::schema::ObDatabaseSchema *database_schema = nullptr;
  share::schema::ObSessionPrivInfo session_priv;
  bool has_insert_priv = false;
  if (OB_FAIL(schema_guard.get_database_schema(session.get_effective_tenant_id(),
                                               table_schema.get_database_id(),
                                               database_schema))) {
    LOG_WARN("fail to get target database schema", KR(ret), K(table_schema.get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target database schema is null", KR(ret), K(table_schema.get_database_id()));
  } else if (OB_FAIL(session.get_session_priv_info(session_priv))) {
    LOG_WARN("fail to get session privilege", KR(ret));
  } else {
    has_insert_priv = OB_TEST_PRIVS(session_priv.user_priv_set_, OB_PRIV_INSERT);
  }
  for (share::schema::ObTableSchema::const_column_iterator iter = table_schema.column_begin();
       OB_SUCC(ret) && !has_insert_priv && iter != table_schema.column_end();
       ++iter) {
    const share::schema::ObColumnSchemaV2 *column_schema = *iter;
    ObPrivSet column_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target column schema is null", KR(ret));
    } else if (column_schema->get_column_id() >= OB_APP_MIN_COLUMN_ID
               && !column_schema->is_hidden()
               && !column_schema->is_unused()
               && !column_schema->is_generated_column()) {
      if (OB_FAIL(schema_guard.collect_all_priv_for_column(
              session_priv,
              session.get_enable_role_array(),
              database_schema->get_database_name_str(),
              table_schema.get_table_name_str(),
              column_schema->get_column_name_str(),
              column_priv_set))) {
        LOG_WARN("fail to collect target column privilege", KR(ret), KPC(column_schema));
      } else {
        has_insert_priv = OB_TEST_PRIVS(column_priv_set, OB_PRIV_INSERT);
      }
    }
  }
  if (OB_SUCC(ret) && !has_insert_priv) {
    const share::schema::ObNeedPriv need_priv(database_schema->get_database_name_str(),
                                              table_schema.get_table_name_str(),
                                              share::schema::OB_PRIV_TABLE_LEVEL,
                                              OB_PRIV_INSERT,
                                              table_schema.is_sys_table());
    if (OB_FAIL(schema_guard.check_single_table_priv(session_priv,
                                                      session.get_enable_role_array(),
                                                      need_priv))) {
      LOG_WARN("target table INSERT privilege check failed", KR(ret), K(need_priv));
    }
  }
  return ret;
}

int fill_target_table_info(ObExecContext &ctx,
                           common::ObString &table_name,
                           ObDirectLoadStreamCreateParam &create_param)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  const share::schema::ObTableSchema *table_schema = nullptr;
  uint64_t database_id = common::OB_INVALID_ID;
  if (OB_ISNULL(session) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", KR(ret), KP(session), KP(schema_guard));
  } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(ctx.get_allocator(),
                                                                        session->get_dtc_params(),
                                                                        table_name))) {
    LOG_WARN("fail to convert target table name", KR(ret), K(table_name));
  } else if (OB_UNLIKELY(table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty target table name", KR(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == (database_id = session->get_database_id()))) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("No database selected", KR(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                    database_id,
                                                    table_name,
                                                    false,
                                                    table_schema))) {
    LOG_WARN("fail to get target table schema", KR(ret), K(database_id), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("target table does not exist", KR(ret), K(database_id), K(table_name));
  } else if (OB_FAIL(check_target_table_insert_privilege(*session,
                                                         *schema_guard,
                                                         *table_schema))) {
    LOG_WARN("fail to check target table INSERT privilege", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(
                 ObTableLoadService::check_support_incremental_direct_load_stream_table(
                     *schema_guard, table_schema))) {
    LOG_WARN("target table does not support incremental direct load", KR(ret), KPC(table_schema));
  } else {
    create_param.tenant_id_ = session->get_effective_tenant_id();
    create_param.user_id_ = session->get_priv_user_id();
    create_param.table_id_ = table_schema->get_table_id();
    create_param.target_schema_version_ = table_schema->get_schema_version();
  }
  return ret;
}

int check_stream_owner(ObExecContext &ctx, const ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_UNLIKELY(meta.tenant_id_ != session->get_effective_tenant_id()
      || meta.user_id_ != session->get_priv_user_id())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("direct load stream owner mismatch", KR(ret), K(meta),
             "tenant_id", session->get_effective_tenant_id(),
             "user_id", session->get_priv_user_id());
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "close direct load stream");
  }
  return ret;
}
} // namespace

int ObDBMSDirectLoadStream::create_table(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamCreateParam create_param;
  common::ObString table_name;
  int64_t ttl_seconds = 0;
  int64_t stream_id = common::OB_INVALID_ID;
  if (OB_UNLIKELY(!ObTableLoadService::is_direct_load_stream_enabled())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Stream Direct Load when disabled is");
    LOG_WARN("Stream Direct Load is disabled", KR(ret));
  } else if (OB_UNLIKELY(2 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count for direct load stream create table", KR(ret), K(params.count()));
  } else if (OB_FAIL(get_string_param(params, 0, table_name))) {
    LOG_WARN("fail to get target table name", KR(ret));
  } else if (OB_FAIL(get_positive_int64_param(params, 1, ttl_seconds))) {
    LOG_WARN("fail to get ttl seconds", KR(ret));
  } else if (OB_FAIL(fill_target_table_info(ctx, table_name, create_param))) {
    LOG_WARN("fail to fill direct load stream target table info", KR(ret), K(table_name));
  } else if (OB_FAIL(calc_expire_ts(ttl_seconds, create_param.expire_ts_))) {
    LOG_WARN("fail to calc direct load stream expire ts", KR(ret), K(ttl_seconds));
  } else if (OB_FAIL(ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id))) {
    LOG_WARN("fail to create direct load stream", KR(ret), K(create_param));
  } else {
    result.set_int(stream_id);
  }
  return ret;
}

int ObDBMSDirectLoadStream::close(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  int64_t stream_id = common::OB_INVALID_ID;
  ObDirectLoadStreamMeta meta;
  if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count for direct load stream close", KR(ret), K(params.count()));
  } else if (OB_FAIL(get_stream_id_param(params, 0, stream_id))) {
    LOG_WARN("fail to get stream id", KR(ret));
  } else if (OB_FAIL(ObDirectLoadStreamService::get_direct_load_stream(stream_id, meta))) {
    LOG_WARN("fail to get direct load stream", KR(ret), K(stream_id));
  } else if (OB_FAIL(check_stream_owner(ctx, meta))) {
    LOG_WARN("fail to check direct load stream owner", KR(ret), K(stream_id), K(meta));
  } else if (OB_FAIL(ObDirectLoadStreamService::seal_direct_load_stream(stream_id))) {
    LOG_WARN("fail to seal direct load stream", KR(ret), K(stream_id));
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase

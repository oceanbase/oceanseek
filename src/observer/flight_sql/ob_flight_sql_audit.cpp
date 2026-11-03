/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flight_sql_audit.h"

#include <cstring>

#include "lib/oblog/ob_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/stat/ob_diagnose_info.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "sql/monitor/ob_exec_stat.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{

static bool audit_tag_printable_no_comment_end(const char *const s, const size_t len)
{
  if (nullptr == s || 0 == len) {
    return false;
  }
  for (size_t i = 0; i < len; ++i) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    if ('*' == c || '/' == c || c < 0x20U || c > 0x7eU) {
      return false;
    }
  }
  return true;
}

void mirror_flight_sql_audit_record_to_gv_sql_audit(
    sql::ObSQLSessionInfo &session,
    const ObFlightSqlAuditRecord &rec,
    const char *request_name)
{
  try {
  if (!GCONF.enable_sql_audit) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "[FlightSQL] audit skipped: enable_sql_audit is OFF");
    return;
  }
  if (!session.get_local_ob_enable_sql_audit()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "[FlightSQL] audit skipped: session ob_enable_sql_audit is OFF",
                 "ob_sessid", session.get_server_sid());
    return;
  }
  obmysql::ObMySQLRequestManager *req_manager = session.get_request_manager();
  if (OB_ISNULL(req_manager)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "[FlightSQL] audit skipped: request_manager is NULL",
                 "ob_sessid", session.get_server_sid());
    return;
  }

  std::string tenant_holder;
  std::string user_holder;
  std::string db_holder;
  common::ObString tstr = session.get_tenant_name();
  if (!tstr.empty()) {
    tenant_holder.assign(tstr.ptr(), static_cast<size_t>(tstr.length()));
  }
  if ('\0' != rec.user_name[0]) {
    user_holder.assign(rec.user_name);
  } else {
    common::ObString ustr = session.get_user_name();
    if (!ustr.empty()) {
      user_holder.assign(ustr.ptr(), static_cast<size_t>(ustr.length()));
    }
  }
  if ('\0' != rec.schema_name[0]) {
    db_holder.assign(rec.schema_name);
  } else {
    common::ObString dstr = session.get_database_name();
    if (!dstr.empty()) {
      db_holder.assign(dstr.ptr(), static_cast<size_t>(dstr.length()));
    }
  }

  // QUERY_SQL may include the catalog and RPC name, but never the Flight
  // session UUID: the UUID is also the bearer token.
  std::string sql_full;
  {
    const size_t cat_len =
        strnlen(rec.catalog_name, sizeof(rec.catalog_name));
    const bool cat_ok = cat_len > 0 && cat_len < sizeof(rec.catalog_name)
                        && audit_tag_printable_no_comment_end(rec.catalog_name, cat_len);
    static const size_t MAX_REQUEST_NAME_LEN = 64;
    const size_t request_len = OB_ISNULL(request_name)
        ? 0 : strnlen(request_name, MAX_REQUEST_NAME_LEN);
    const bool request_ok = request_len > 0 && request_len < MAX_REQUEST_NAME_LEN
                            && audit_tag_printable_no_comment_end(request_name, request_len);

    if (cat_ok || request_ok) {
      sql_full.append("-- flightsql:");
      if (cat_ok) {
        sql_full.append(" catalog=");
        sql_full.append(rec.catalog_name, cat_len);
      }
      if (request_ok) {
        sql_full.append(" request=");
        sql_full.append(request_name, request_len);
      }
      sql_full.append("\n");
    }
    if (!rec.sql_text.empty()) {
      sql_full.append(rec.sql_text);
    }
  }

  const int64_t limit = session.get_tenant_query_record_size_limit();
  if (limit > 0 && static_cast<int64_t>(sql_full.size()) > limit) {
    sql_full.resize(static_cast<size_t>(limit > 3 ? limit - 3 : 0));
    sql_full.append("...");
  }

  sql::ObAuditRecordData ar;
  ar.status_ = rec.ret_code;
  if (rec.trace_id[0] != '\0') {
    (void)ar.trace_id_.set(rec.trace_id);
  }
  ar.tenant_id_ = static_cast<int64_t>(rec.tenant_id);
  ar.effective_tenant_id_ = static_cast<int64_t>(rec.tenant_id);
  if (rec.ob_session_id != 0) {
    ar.session_id_ = static_cast<uint64_t>(rec.ob_session_id);
  } else {
    ar.session_id_ = session.get_server_sid();
  }
  ar.proxy_session_id_ = session.get_proxy_sessid();
  ar.execution_id_ = session.get_current_execution_id();
  ar.user_id_ = session.get_user_id();
  ar.db_id_ = session.get_database_id();
  ar.server_addr_ = GCTX.self_addr();
  if (rec.peer_ip[0] != '\0' && rec.peer_port > 0) {
    common::ObAddr peer;
    if (peer.set_ip_addr(rec.peer_ip, rec.peer_port)) {
      ar.client_addr_ = peer;
      ar.user_client_addr_ = peer;
    }
  }
  ar.plan_id_         = rec.plan_id;
  ar.plan_type_       = rec.plan_type;
  ar.is_hit_plan_cache_ = rec.is_hit_plan_cache;
  ar.return_rows_ = rec.return_rows;
  ar.affected_rows_ = rec.affected_rows;
  ar.try_cnt_ = 1;
  ar.request_type_ = static_cast<int64_t>(sql::EXECUTE_LOCAL);
  ar.is_inner_sql_ = false;
  ar.is_executor_rpc_ = false;
  ar.is_multi_stmt_ = false;
  ar.is_batched_multi_stmt_ = false;
  ar.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
  ar.partition_hit_ = true;
  ar.table_scan_ = false;
  ar.user_group_ = rec.resource_group_id;
  ar.partition_cnt_ = 0;
  ar.exec_timestamp_.receive_ts_ = rec.request_time;
  ar.exec_timestamp_.executor_end_ts_ = rec.request_time + rec.elapsed_time;
  ar.exec_timestamp_.elapsed_t_ = rec.elapsed_time;
  ar.exec_timestamp_.executor_t_ = rec.execute_time;
  ar.stmt_type_ = rec.stmt_type;
  if (rec.sql_id[0] != '\0') {
    MEMCPY(ar.sql_id_, rec.sql_id, common::OB_MAX_SQL_ID_LENGTH);
    ar.sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    MEMCPY(ar.format_sql_id_, rec.sql_id, common::OB_MAX_SQL_ID_LENGTH);
    ar.format_sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  }
  if (!tenant_holder.empty()) {
    ar.tenant_name_ = tenant_holder.data();
    ar.tenant_name_len_ = static_cast<int64_t>(tenant_holder.length());
  }
  if (!user_holder.empty()) {
    ar.user_name_ = user_holder.data();
    ar.user_name_len_ = static_cast<int64_t>(user_holder.length());
  }
  if (!db_holder.empty()) {
    ar.db_name_ = db_holder.data();
    ar.db_name_len_ = static_cast<int64_t>(db_holder.length());
  }
  ar.sql_ = const_cast<char *>(sql_full.c_str());
  ar.sql_len_ = static_cast<int64_t>(sql_full.length());
  ar.sql_cs_type_ = session.get_local_collation_connection();

  const int mr_ret =
      req_manager->record_request(ar,
                                 session.enable_query_response_time_stats(),
                                 session.get_tenant_query_record_size_limit(),
                                 false /* is_sensitive */,
                                 nullptr);
  if (OB_SUCCESS != mr_ret) {
    if (OB_SIZE_OVERFLOW != mr_ret && OB_ALLOCATE_MEMORY_FAILED != mr_ret) {
      LOG_DEBUG("mirror flight sql audit into GV$OB_SQL_AUDIT failed", K(mr_ret));
    }
  }
  } catch (const std::bad_alloc &) {
    // skip audit on OOM — LOG_WARN may itself allocate
  } catch (const std::exception &e) {
    int ret = OB_SUCCESS;
    LOG_WARN("[FlightSQL] audit record: unexpected exception, audit row dropped",
             "what", e.what(),
             "ob_session_id", rec.ob_session_id,
             "tenant_id", rec.tenant_id,
             "sql_id", rec.sql_id,
             "ret_code", rec.ret_code);
  } catch (...) {
    int ret = OB_SUCCESS;
    LOG_WARN("[FlightSQL] audit record: unknown exception, audit row dropped",
             "ob_session_id", rec.ob_session_id,
             "tenant_id", rec.tenant_id,
             "sql_id", rec.sql_id,
             "ret_code", rec.ret_code);
  }
}

} // namespace observer
} // namespace oceanbase

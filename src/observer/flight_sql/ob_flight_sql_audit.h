/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_SQL_AUDIT_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_SQL_AUDIT_H_

#include <string>

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"
#include <stdint.h>

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{

struct ObFlightSqlAuditRecord
{
  bool     is_valid{false};
  int64_t  request_id{0};
  uint64_t tenant_id{0};
  char     user_name[common::OB_MAX_USERNAME_LENGTH + 1]{};
  char     schema_name[common::OB_MAX_DATABASE_NAME_LENGTH + 1]{};
  char     catalog_name[common::OB_MAX_CATALOG_NAME_LENGTH + 1]{};
  uint32_t ob_session_id{0};
  char     trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE + 1]{};
  char     sql_id[common::OB_MAX_SQL_ID_LENGTH + 1]{};
  int64_t  plan_id{0};
  std::string sql_text;
  int64_t  request_time{0};   // microseconds since epoch
  int64_t  elapsed_time{0};   // total elapsed microseconds
  int64_t  execute_time{0};   // open() phase microseconds
  int64_t  return_rows{0};
  int64_t  affected_rows{0};
  int      ret_code{0};
  char     peer_ip[common::MAX_IP_ADDR_LENGTH + 1]{};
  int32_t  peer_port{0};
  bool              is_hit_plan_cache{false};
  sql::ObPhyPlanType plan_type{sql::OB_PHY_PLAN_UNINITIALIZED};
  sql::stmt::StmtType stmt_type{sql::stmt::T_NONE};
};

void mirror_flight_sql_audit_record_to_gv_sql_audit(sql::ObSQLSessionInfo &session, const ObFlightSqlAuditRecord &rec);

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_SQL_AUDIT_H_

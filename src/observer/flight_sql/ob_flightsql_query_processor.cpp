/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_query_processor.h"

#include "lib/charset/ob_charset.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/utility/ob_defer.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/worker.h"
#include "observer/ob_req_time_service.h"
#include "observer/flight_sql/ob_flight_worker.h"
#include "observer/flight_sql/ob_flightsql_param_converter.h"
#include "observer/flight_sql/ob_flightsql_query_retry.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "sql/ob_result_set.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_subschema_ctx.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_batch_converter.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/string/ob_string.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/ob_sql_utils.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "share/config/ob_server_config.h"
#include <algorithm>
#include <cstring>
#include "sql/executor/ob_execute_result.h"
#include "sql/executor/ob_task_executor_ctx.h"

// Arrow headers after OB headers
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace oceanbase
{
namespace observer
{
namespace sql = ::oceanbase::sql;
using ArrowDataTypePtr = std::shared_ptr<arrow::DataType>;
using ArrowKVMetadataPtr = std::shared_ptr<arrow::KeyValueMetadata>;

static bool flight_sql_is_text_family(common::ObObjType tp)
{
  return tp == common::ObVarcharType  || tp == common::ObCharType
      || tp == common::ObNVarchar2Type || tp == common::ObNCharType
      || tp == common::ObTinyTextType  || tp == common::ObTextType
      || tp == common::ObMediumTextType || tp == common::ObLongTextType;
}

static int flight_sql_validate_result_types_(
    const common::ColumnsFieldIArray *fields,
    const sql::ObPhysicalPlan *plan,
    const std::string &trace_id,
    std::string &error_detail)
{
  int ret = OB_SUCCESS;
  // Fetch converts the root operator's output datums.  A field can advertise
  // a different type, so check both while the plan is still in prepare/open.
  const sql::ObOpSpec *root_spec = OB_NOT_NULL(plan) ? plan->get_root_op_spec() : nullptr;
  const int64_t field_count = OB_NOT_NULL(fields) ? fields->count() : 0;
  if (OB_NOT_NULL(root_spec) && root_spec->output_.count() != field_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] result field/output count mismatch before fetch",
             K(ret), K(field_count), "output_expr_count", root_spec->output_.count(),
             "ob_trace_id", trace_id.c_str());
  }
  if (OB_NOT_NULL(fields)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < fields->count(); ++i) {
      const common::ObField &field = fields->at(i);
      const common::ObObjType field_type = field.type_.get_type();
      const sql::ObExpr *output_expr = OB_NOT_NULL(root_spec)
          ? root_spec->output_.at(i) : nullptr;
      if (OB_NOT_NULL(root_spec) && OB_ISNULL(output_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] null result output expression before fetch",
                 K(ret), K(i), "ob_trace_id", trace_id.c_str());
      } else if (!sql::ObArrowBatchConverter::supports_output_type(field_type)
                 || (OB_NOT_NULL(output_expr)
                     && !sql::ObArrowBatchConverter::supports_output_type(
                         output_expr->datum_meta_.type_))) {
        ret = OB_NOT_SUPPORTED;
        const common::ObObjType unsupported_type =
            sql::ObArrowBatchConverter::supports_output_type(field_type)
                ? output_expr->datum_meta_.type_ : field_type;
        std::string column_name;
        if (field.cname_.length() > 0 && OB_NOT_NULL(field.cname_.ptr())) {
          column_name.assign(field.cname_.ptr(), field.cname_.length());
        }
        const char *type_name = common::ob_obj_type_str(unsupported_type);
        error_detail = "Flight SQL cannot return column " + std::to_string(i)
            + " (" + column_name + ") of type " + type_name
            + " as Arrow (OB_NOT_SUPPORTED) trace_id=" + trace_id;
        LOG_WARN("[FlightSQL] unsupported Arrow result type", K(ret), K(i),
                 "column", column_name.c_str(),
                 "field_ob_type", common::ob_obj_type_str(field_type),
                 "datum_ob_type", OB_NOT_NULL(output_expr)
                      ? common::ob_obj_type_str(output_expr->datum_meta_.type_) : "(none)",
                 "ob_trace_id", trace_id.c_str());
      }
    }
  }
  return ret;
}

namespace
{

// RAII wrapper for placement-new'd ObResultSet.
// Ensures ~ObResultSet() is always called even on early-return paths, which is
// necessary because ObResultSet::ctor increments ObReqTimeInfo::reentrant_cnt_
// and the matching decrement only happens in the dtor.
struct ObResultSetPlacementGuard {
  sql::ObResultSet *rs_;
  explicit ObResultSetPlacementGuard(sql::ObResultSet *rs) : rs_(rs) {}
  ~ObResultSetPlacementGuard() { rs_->~ObResultSet(); }
private:
  ObResultSetPlacementGuard(const ObResultSetPlacementGuard &) = delete;
  ObResultSetPlacementGuard &operator=(const ObResultSetPlacementGuard &) = delete;
};

// Establish the thread/session state that the native MySQL processors install
// before entering ObSql.  Flight RPCs run on gRPC threads, so none of this is
// provided by the tenant request scheduler.  The request-level timeout is kept
// outside this scope: one prepared DoPut may execute many rows and all rows
// must share the same absolute deadline.
class ObFlightSqlExecutionScope
{
public:
  explicit ObFlightSqlExecutionScope(ObFlightSqlSession &flight_session)
      : session_(flight_session.ob_session),
        tenant_id_(flight_session.tenant_id),
        previous_session_(THIS_WORKER.get_session()),
        previous_rpc_tenant_(THIS_WORKER.get_rpc_tenant()),
        compat_mode_guard_(OB_NOT_NULL(session_)
            && ORACLE_MODE == session_->get_compatibility_mode()
                ? lib::Worker::CompatMode::ORACLE
                : lib::Worker::CompatMode::MYSQL)
  {
    THIS_WORKER.set_session(session_);
    // Native SQL derives RPC tenants from each SQL session, including nested
    // meta-tenant statements. Do not inherit the Flight transport's override.
    THIS_WORKER.reset_rpc_tenant();
    if (OB_NOT_NULL(session_)) {
      session_->set_current_trace_id(common::ObCurTraceId::get_trace_id());
      session_->init_use_rich_format();
      session_->set_thread_id(GETTID());
    }
  }

  ~ObFlightSqlExecutionScope()
  {
    THIS_WORKER.set_session(previous_session_);
    THIS_WORKER.set_rpc_tenant(previous_rpc_tenant_);
  }

  int refresh_session_state()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(bind_resource_group())) {
      LOG_WARN("[FlightSQL] failed to bind the current thread resource group", K(ret));
    } else if (OB_FAIL(session_->check_tenant_status())) {
      LOG_WARN("[FlightSQL] tenant is not available for execution", K(ret));
    } else if (OB_FAIL(session_->update_timezone_info())) {
      LOG_WARN("[FlightSQL] failed to update session timezone", K(ret));
    } else {
      session_->refresh_tenant_config();
      if (OB_FAIL(session_->gen_configs_in_pc_str())) {
        LOG_WARN("[FlightSQL] failed to refresh plan-cache configuration", K(ret));
      } else {
        session_->partition_hit().reset();
        session_->set_pl_can_retry(true);
      }
    }
    return ret;
  }

  int bind_resource_group()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(session_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("[FlightSQL] execution scope has no OB session", K(ret));
    } else if (tenant_id_ != session_->get_effective_tenant_id() || tenant_id_ != MTL_ID()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] execution tenant differs from fixed session tenant", K(ret),
               K(tenant_id_), "session_tenant_id", session_->get_effective_tenant_id(),
               "mtl_tenant_id", MTL_ID());
    } else {
      ret = bind_default_resource_group();
    }
    return ret;
  }

  int switch_resource_group(const uint64_t group_id)
  {
    return cgroup_guard_.switch_to(tenant_id_, group_id);
  }

private:
  int bind_default_resource_group()
  {
    int ret = OB_SUCCESS;
    uint64_t group_id = 0;
    session_->set_expect_group_id(common::OB_INVALID_ID);
    // USER mappings are not recorded in every cached plan. Resolve the current
    // mapping for each request, including removal of a previous mapping.
    if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid()) {
      if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_user(
              tenant_id_, session_->get_user_id(), group_id))) {
        LOG_WARN("[FlightSQL] failed to resolve default resource group", K(ret), K(tenant_id_));
      } else if (common::OB_INVALID_ID == group_id) {
        group_id = 0;
      }
    }
    if (OB_SUCC(ret)) {
      ret = switch_resource_group(group_id);
    }
    return ret;
  }

  sql::ObSQLSessionInfo *session_;
  const uint64_t tenant_id_;
  sql::ObSQLSessionInfo *previous_session_;
  const uint64_t previous_rpc_tenant_;
  lib::CompatModeGuard compat_mode_guard_;
  share::ObSystemTenantCgroupGuard cgroup_guard_;
  ObFlightSqlExecutionScope(const ObFlightSqlExecutionScope &) = delete;
  ObFlightSqlExecutionScope &operator=(const ObFlightSqlExecutionScope &) = delete;
};

// Publish native prepared operations in SHOW PROCESSLIST and restore the
// session's idle Flight tag on every exit path.
struct ObFlightSqlPreparedStateScope {
  ObFlightSqlPreparedStateScope(ObFlightSqlSession &flight_session,
                                const common::ObString &display_sql,
                                const obmysql::ObMySQLCmd command)
      : flight_session_(&flight_session),
        session_(flight_session.ob_session),
        idle_tag_("/* flightsql */")
  {
    if (OB_NOT_NULL(session_)) {
      std::lock_guard<std::mutex> guard(flight_session_->mutex);
      if (!flight_session_->closing_) {
        session_->set_mysql_cmd(command);
        (void)session_->set_session_state(sql::QUERY_ACTIVE);
        (void)session_->store_query_string(display_sql);
      }
    }
  }
  ~ObFlightSqlPreparedStateScope()
  {
    if (OB_NOT_NULL(session_) && OB_NOT_NULL(flight_session_)) {
      std::lock_guard<std::mutex> guard(flight_session_->mutex);
      if (flight_session_->closing_) {
        return;
      }
      session_->set_session_sleep();
      (void)session_->store_query_string(common::ObString(
          static_cast<int32_t>(idle_tag_.size()), idle_tag_.c_str()));
    }
  }

private:
  ObFlightSqlSession *flight_session_;
  sql::ObSQLSessionInfo *session_;
  std::string idle_tag_;
  ObFlightSqlPreparedStateScope(const ObFlightSqlPreparedStateScope &) = delete;
  ObFlightSqlPreparedStateScope &operator=(const ObFlightSqlPreparedStateScope &) = delete;
};

} // anonymous namespace

namespace
{
// Same rule as ObResultSet::inner_get_next_row: non-PL CMD SQL must not use get_next_row().
bool flight_sql_rs_is_non_row_cmd_sql(const sql::ObResultSet &rs)
{
  const sql::ObICmd *cmd = rs.get_cmd();
  if (OB_ISNULL(cmd)) {
    return false;
  }
  return !rs.is_pl_stmt(static_cast<stmt::StmtType>(cmd->get_cmd_type()));
}

static void flight_sql_copy_str_preview_(const char *src, const int64_t src_len,
                                         char *dst, const int64_t dst_cap)
{
  if (OB_ISNULL(dst) || dst_cap <= 0) {
    return;
  }
  dst[0] = '\0';
  if (OB_ISNULL(src) || src_len <= 0) {
    return;
  }
  const int64_t max_body = dst_cap - 4;  // reserve "..." + '\0'
  const int64_t copy_len = (src_len > max_body) ? max_body : src_len;
  if (copy_len > 0) {
    MEMCPY(dst, src, copy_len);
    dst[copy_len] = '\0';
    if (src_len > copy_len) {
      MEMCPY(dst + copy_len, "...", 3);
      dst[copy_len + 3] = '\0';
    }
  }
}

static void flight_sql_get_arrow_decimal_meta_(arrow::ArrayBuilder *builder,
                                               int32_t &arrow_prec,
                                               int32_t &arrow_scale,
                                               int &arrow_type_id)
{
  arrow_prec = -1;
  arrow_scale = -1;
  arrow_type_id = -1;
  if (OB_ISNULL(builder)) {
    return;
  }
  const ArrowDataTypePtr type_sptr = builder->type();
  if (!type_sptr) {
    return;
  }
  arrow_type_id = static_cast<int>(type_sptr->id());
  if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
    const arrow::Decimal128Type *t =
        static_cast<const arrow::Decimal128Type *>(type_sptr.get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
    const arrow::Decimal256Type *t =
        static_cast<const arrow::Decimal256Type *>(type_sptr.get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  }
}

static void flight_sql_log_append_null_(const char *reason,
                                        const common::ObObj &obj,
                                        arrow::ArrayBuilder *builder,
                                        const int64_t col_idx,
                                        const char *col_name,
                                        const char *str_preview = nullptr,
                                        const int32_t parsed_prec = -1,
                                        const int32_t parsed_scale = -1,
                                        const int32_t src_scale = -1,
                                        const char *arrow_err = nullptr)
{
  int32_t arrow_prec = -1;
  int32_t arrow_scale = -1;
  int arrow_type_id = -1;
  flight_sql_get_arrow_decimal_meta_(builder, arrow_prec, arrow_scale, arrow_type_id);
  LOG_DEBUG("[FlightSQL] append_obj_to_builder_: emit null cell",
           "reason", reason,
           "col_idx", col_idx,
           "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"),
           "ob_type", static_cast<int>(obj.get_type()),
           "ob_scale", static_cast<int64_t>(obj.get_scale()),
           "ob_int_bytes",
           (common::ObDecimalIntType == obj.get_type()
                ? static_cast<int64_t>(obj.get_int_bytes())
                : -1),
           "arrow_type_id", arrow_type_id,
           "arrow_precision", arrow_prec,
           "arrow_scale", arrow_scale,
           "str_preview", (OB_NOT_NULL(str_preview) ? str_preview : ""),
           "parsed_precision", parsed_prec,
           "parsed_scale", parsed_scale,
           "src_scale", src_scale,
           "arrow_err", (OB_NOT_NULL(arrow_err) ? arrow_err : ""));
}

static void flight_sql_log_append_null_from_status_(
    const char *reason,
    const common::ObObj &obj,
    arrow::ArrayBuilder *builder,
    const int64_t col_idx,
    const char *col_name,
    const arrow::Status &st,
    const char *str_preview = nullptr,
    const int32_t parsed_prec = -1,
    const int32_t parsed_scale = -1,
    const int32_t src_scale = -1)
{
  char err_buf[256];
  const std::string err = st.ToString();
  flight_sql_copy_str_preview_(err.c_str(), static_cast<int64_t>(err.size()),
                                 err_buf, sizeof(err_buf));
  flight_sql_log_append_null_(reason, obj, builder, col_idx, col_name, str_preview,
                              parsed_prec, parsed_scale, src_scale, err_buf);
}

template <typename ArrowDecimalType, typename ArrowBuilderType>
static int flight_sql_append_scaled_decimal_int_to_arrow_(
    ArrowBuilderType *builder,
    const common::ObDecimalInt *decint,
    const int32_t int_bytes,
    const common::ObObj &obj,
    const int64_t col_idx,
    const char *col_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(builder) || OB_ISNULL(decint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] null decimal builder or payload", K(ret), K(col_idx));
    return ret;
  }
  const int32_t arrow_bytes = static_cast<int32_t>(ArrowDecimalType::kByteWidth);
  common::ObDecimalIntBuilder extended;
  extended.from(decint, int_bytes);
  if (extended.get_int_bytes() < arrow_bytes) {
    extended.extend(arrow_bytes);
  } else if (extended.get_int_bytes() > arrow_bytes) {
    flight_sql_log_append_null_("decimal_int_bytes_exceeds_arrow_width", obj, builder, col_idx,
                                col_name, nullptr, -1, -1, -1,
                                "scaled decimal int is wider than Arrow decimal type");
    return sql::arrow_status_to_ob_ret(builder->AppendNull());
  }
  const int32_t final_bytes = extended.get_int_bytes();
  const uint8_t *buffer = reinterpret_cast<const uint8_t *>(extended.get_decimal_int());
  if (OB_ISNULL(buffer) || final_bytes < 0 || final_bytes > arrow_bytes) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid decimal buffer or width", K(ret), K(final_bytes), K(arrow_bytes));
    return ret;
  }
  uint8_t buf[ArrowDecimalType::kByteWidth] = {0};
  for (int32_t i = 0; i < final_bytes; ++i) {
    buf[arrow_bytes - 1 - i] = buffer[i];
  }
  const arrow::Result<ArrowDecimalType> dec_res =
      ArrowDecimalType::FromBigEndian(buf, arrow_bytes);
  if (!dec_res.ok()) {
    flight_sql_log_append_null_from_status_("decimal_from_big_endian_failed", obj, builder,
                                            col_idx, col_name, dec_res.status());
    return sql::arrow_status_to_ob_ret(builder->AppendNull());
  }
  const arrow::Status append_st = builder->Append(dec_res.ValueOrDie());
  ret = sql::arrow_status_to_ob_ret(append_st);
  if (!append_st.ok()) {
    LOG_WARN("[FlightSQL] append decimal output failed", K(ret), K(col_idx),
             "status", append_st.ToString().c_str());
  }
  return ret;
}

static int flight_sql_append_ob_decimal_to_builder_(
    arrow::ArrayBuilder *builder,
    const common::ObObj &obj,
    const int64_t col_idx,
    const char *col_name)
{
  using namespace common;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(builder) || OB_ISNULL(builder->type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] null decimal builder or type", K(ret), K(col_idx));
    return ret;
  }
  int32_t tgt_prec = -1;
  int32_t tgt_scale = -1;
  int arrow_type_id = -1;
  flight_sql_get_arrow_decimal_meta_(builder, tgt_prec, tgt_scale, arrow_type_id);
  if (tgt_prec <= 0 || tgt_scale < 0) {
    flight_sql_log_append_null_("decimal_arrow_meta_invalid", obj, builder, col_idx, col_name);
    return sql::arrow_status_to_ob_ret(builder->AppendNull());
  }

  ObDecimalIntBuilder work;
  ObDecimalIntBuilder scaled;
  const ObDecimalInt *src_decint = nullptr;
  int32_t src_int_bytes = 0;
  ObScale src_scale = 0;

  if (ObNumberType == obj.get_type() || ObUNumberType == obj.get_type()) {
    const number::ObNumber &nmb = obj.get_number();
    // Always use nmb.get_scale() so from_number does no truncation internally.
    // from_number_to_int512 uses integer division (truncation) when in_scale > scale;
    // by matching scales we keep full precision, then let common_scale_decimalint
    // below handle any rescaling to out_scale with proper round-half-up.
    src_scale = nmb.get_scale();
    ObDecimalInt *tmp_decint = nullptr;
    int32_t tmp_int_bytes = 0;
    ret = wide::from_number(nmb, work, src_scale, tmp_decint, tmp_int_bytes);
    if (OB_FAIL(ret)) {
      flight_sql_log_append_null_("ob_number_to_decimal_int_failed", obj, builder, col_idx,
                                  col_name, nullptr, -1, -1, static_cast<int32_t>(src_scale),
                                  "wide::from_number failed");
      return sql::arrow_status_to_ob_ret(builder->AppendNull());
    }
    src_decint = tmp_decint;
    src_int_bytes = tmp_int_bytes;
  } else if (ObDecimalIntType == obj.get_type()) {
    src_decint = obj.get_decimal_int();
    src_int_bytes = obj.get_int_bytes();
    src_scale = obj.get_scale();
    if (OB_ISNULL(src_decint)) {
      flight_sql_log_append_null_("decimal_int_ptr_null", obj, builder, col_idx, col_name);
      return sql::arrow_status_to_ob_ret(builder->AppendNull());
    }
  } else {
    flight_sql_log_append_null_("not_decimal_ob_type", obj, builder, col_idx, col_name);
    return sql::arrow_status_to_ob_ret(builder->AppendNull());
  }

  const ObPrecision out_prec = static_cast<ObPrecision>(tgt_prec);
  const ObScale out_scale = static_cast<ObScale>(tgt_scale);
  const int32_t out_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
  const ObDecimalInt *out_decint = src_decint;
  int32_t out_bytes = src_int_bytes;

  if (sql::ObDatumCast::need_scale_decimalint(src_scale, src_int_bytes, out_scale, out_int_bytes)) {
    ret = sql::ObDatumCast::common_scale_decimalint(src_decint, src_int_bytes, src_scale, out_scale,
                                                    out_prec, CM_NONE, scaled);
    if (OB_FAIL(ret)) {
      flight_sql_log_append_null_("common_scale_decimalint_failed", obj, builder, col_idx,
                                  col_name, nullptr, out_prec, out_scale,
                                  static_cast<int32_t>(src_scale),
                                  "ObDatumCast::common_scale_decimalint failed");
      return sql::arrow_status_to_ob_ret(builder->AppendNull());
    }
    out_decint = scaled.get_decimal_int();
    out_bytes = scaled.get_int_bytes();
  }

  if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
    ret = flight_sql_append_scaled_decimal_int_to_arrow_<arrow::Decimal128, arrow::Decimal128Builder>(
        static_cast<arrow::Decimal128Builder *>(builder), out_decint, out_bytes, obj, col_idx,
        col_name);
  } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
    ret = flight_sql_append_scaled_decimal_int_to_arrow_<arrow::Decimal256, arrow::Decimal256Builder>(
        static_cast<arrow::Decimal256Builder *>(builder), out_decint, out_bytes, obj, col_idx,
        col_name);
  } else {
    flight_sql_log_append_null_("unexpected_arrow_decimal_builder_type", obj, builder, col_idx,
                                col_name);
    ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
  }
  return ret;
}
} // namespace

// ============================================================
// OB → Arrow cell serialisation
// ============================================================

static int append_obj_to_builder_(arrow::ArrayBuilder *builder,
                                   const common::ObObj &obj,
                                   const int64_t tenant_id,
                                   const int64_t col_idx = -1,
                                   const char *col_name = nullptr)
{
  int ret = OB_SUCCESS;
  using namespace common;
  if (OB_ISNULL(builder) || OB_ISNULL(builder->type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] null result builder or type", K(ret), K(col_idx));
    return ret;
  }
  if (obj.is_null()) {
    LOG_DEBUG("[FlightSQL] append_obj_to_builder_: SQL null cell",
             "col_idx", col_idx,
             "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"),
             "ob_type", static_cast<int>(obj.get_type()));
    return sql::arrow_status_to_ob_ret(builder->AppendNull());
  }
  switch (obj.get_type()) {
    case ObTinyIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int8Builder *>(builder)->Append(
          static_cast<int8_t>(obj.get_tinyint())));
      break;
    case ObSmallIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int16Builder *>(builder)->Append(
          static_cast<int16_t>(obj.get_smallint())));
      break;
    case ObMediumIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_mediumint())));
      break;
    case ObInt32Type:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_int32())));
      break;
    case ObIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int64Builder *>(builder)->Append(
          static_cast<int64_t>(obj.get_int())));
      break;
    case ObUTinyIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::UInt8Builder *>(builder)->Append(
          static_cast<uint8_t>(obj.get_utinyint())));
      break;
    case ObUSmallIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::UInt16Builder *>(builder)->Append(
          static_cast<uint16_t>(obj.get_usmallint())));
      break;
    case ObUMediumIntType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_umediumint())));
      break;
    case ObUInt32Type:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::UInt32Builder *>(builder)->Append(
          static_cast<uint32_t>(obj.get_uint32())));
      break;
    case ObUInt64Type:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::UInt64Builder *>(builder)->Append(
          static_cast<uint64_t>(obj.get_uint64())));
      break;
    case ObFloatType:
    case ObUFloatType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::FloatBuilder *>(builder)->Append(obj.get_float()));
      break;
    case ObDoubleType:
    case ObUDoubleType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::DoubleBuilder *>(builder)->Append(obj.get_double()));
      break;
    case ObNumberType:
    case ObUNumberType:
    case ObDecimalIntType:
      ret = flight_sql_append_ob_decimal_to_builder_(builder, obj, col_idx, col_name);
      break;
    case ObDateTimeType:
    case ObTimestampType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder *>(builder)->Append(obj.get_datetime()));
      break;
    case ObMySQLDateTimeType: {
      const common::ObMySQLDateTime mdt = obj.get_mysql_datetime();
      int64_t dt_value = 0;
      if (OB_SUCCESS != common::ObTimeConverter::mdatetime_to_datetime(mdt, dt_value, 0)) {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
      } else {
        ret = sql::arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder *>(builder)->Append(dt_value));
      }
      break;
    }
    case ObDateType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Date32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_date())));
      break;
    case ObMySQLDateType: {
      const common::ObMySQLDate md = obj.get_mysql_date();
      int32_t d_value = 0;
      if (OB_SUCCESS != common::ObTimeConverter::mdate_to_date(md, d_value, 0)) {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
      } else {
        ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Date32Builder *>(builder)->Append(d_value));
      }
      break;
    }
    case ObTimeType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::DurationBuilder *>(builder)->Append(obj.get_time()));
      break;
    case ObYearType: {
      const uint8_t raw = obj.get_year();
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::Int16Builder *>(builder)->Append(
          static_cast<int16_t>(raw == 0 ? 0 : 1900 + raw)));
      break;
    }
    case ObBitType:
      ret = sql::arrow_status_to_ob_ret(static_cast<arrow::UInt64Builder *>(builder)->Append(obj.get_bit()));
      break;
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObEnumType:
    case ObSetType: {
      common::ObArenaAllocator arena("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      ObString s;
      if (obj.is_lob_storage() && obj.get_string_len() != 0) {
        const ObLobCommon *lob = obj.get_lob_value();
        if (OB_ISNULL(lob)) {
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          // inrow disk LOB: skip header with pointer arithmetic, zero allocation
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          s.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          // outrow or mem LOB: materialize via allocator
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, s)) {
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          if (OB_UNLIKELY(s.length() < 0)) {
            LOG_WARN("[FlightSQL] text LOB length overflowed int32 after read, returning NULL",
                     "length", s.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
        }
      } else {
        s = obj.get_string();
      }
      if (builder->type()->id() == arrow::Type::LARGE_BINARY) {
        if (OB_NOT_NULL(s.ptr())) {
          ret = sql::arrow_status_to_ob_ret(static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
              reinterpret_cast<const uint8_t *>(s.ptr()),
              static_cast<int64_t>(s.length())));
        } else {
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        }
      } else {
        ret = sql::arrow_status_to_ob_ret(static_cast<arrow::StringBuilder *>(builder)->Append(
            s.ptr(), static_cast<int32_t>(s.length())));
      }
      break;
    }
    case ObGeometryType: {
      common::ObArenaAllocator arena("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      ObString raw;
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&arena, obj, raw))) {
        LOG_WARN("[FlightSQL] read geometry payload failed", K(ret), K(col_idx));
      } else if (OB_FAIL(sql::ObArrowBatchConverter::append_geometry_to_builder(
                     raw, *static_cast<arrow::BinaryBuilder *>(builder), arena))) {
        LOG_WARN("[FlightSQL] append geometry output failed", K(ret), K(col_idx));
      }
      break;
    }
    case ObJsonType: {
      // OB stores JSON in an internal binary format.  Expression results such as
      // json_extract() are packed via ObTextStringDatumResult which prepends a LOB
      // header.  Column reads of a stored JSON column may arrive without a header.
      // Strip the LOB header when present before handing bytes to ObJsonBin.
      // arena must be declared before raw so it outlives the raw pointer.
      common::ObArenaAllocator arena("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      ObString raw;
      if (obj.is_lob_storage() && obj.get_string_len() != 0) {
        const ObLobCommon *lob = obj.get_lob_value();
        if (OB_ISNULL(lob)) {
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] JSON inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          raw.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, raw)) {
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          if (OB_UNLIKELY(raw.length() < 0)) {
            LOG_WARN("[FlightSQL] JSON LOB length overflowed int32 after read, returning NULL",
                     "length", raw.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
        }
      } else {
        raw = obj.get_string();
      }
      if (OB_ISNULL(raw.ptr()) || raw.length() == 0) {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        break;
      }
      common::ObJsonBin j_bin(raw.ptr(), raw.length(), &arena);
      common::ObJsonBuffer jbuf(&arena);
      j_bin.set_seek_flag(true);
      if (OB_SUCCESS != j_bin.reset_iter()
          || OB_SUCCESS != j_bin.print(jbuf, /*is_quoted=*/true, raw.length())
          || jbuf.empty()) {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
      } else {
        const int64_t jbuf_len = static_cast<int64_t>(jbuf.length());
        if (OB_UNLIKELY(jbuf_len < 0 || jbuf_len > INT32_MAX)) {
          LOG_WARN("[FlightSQL] serialized JSON length out of int32 range, returning NULL",
                   "jbuf_len", jbuf_len, "col_idx", col_idx,
                   "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        } else {
          ret = sql::arrow_status_to_ob_ret(static_cast<arrow::StringBuilder *>(builder)->Append(
              jbuf.ptr(), static_cast<int32_t>(jbuf_len)));
        }
      }
      break;
    }
    case ObLobType: {
      common::ObArenaAllocator arena("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      ObString s;
      if (obj.is_lob_storage() && obj.get_string_len() != 0) {
        const ObLobCommon *lob = obj.get_lob_value();
        if (OB_ISNULL(lob)) {
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] LOB inrow byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          s.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, s)) {
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          if (OB_UNLIKELY(s.length() < 0)) {
            LOG_WARN("[FlightSQL] LOB length overflowed int32 after read, returning NULL",
                     "length", s.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
        }
      } else {
        s = obj.get_string();
      }
      if (OB_NOT_NULL(s.ptr())) {
        ret = sql::arrow_status_to_ob_ret(static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
            reinterpret_cast<const uint8_t *>(s.ptr()),
            static_cast<int64_t>(s.length())));
      } else {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
      }
      break;
    }
    case ObCollectionSQLType: {
      // ObCollectionSQLType is is_lob_storage(), so get_string() returns LOB-wrapped bytes.
      // Strip the LOB header using the same logic as text types above.
      // arena must outlive raw — declared here so it covers both the LOB-read path and
      // the subsequent binary parsing that reads from raw.ptr().
      common::ObArenaAllocator arena("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      ObString raw;
      if (obj.is_lob_storage() && obj.get_string_len() != 0) {
        const ObLobCommon *lob = obj.get_lob_value();
        if (OB_ISNULL(lob)) {
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] collection inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          raw.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, raw)) {
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
          if (OB_UNLIKELY(raw.length() < 0)) {
            LOG_WARN("[FlightSQL] collection LOB length overflowed int32 after read, returning NULL",
                     "length", raw.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
            break;
          }
        }
      } else {
        raw = obj.get_string();
      }
      if (OB_ISNULL(raw.ptr())) {
        ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        break;
      }
      // VECTOR (FIXED_SIZE_LIST): binary is raw packed elements, no header.
      //   Count comes from the schema dimension; element count = dim.
      // ARRAY (LIST): binary is [uint32_t N][uint8_t nulls[N]][T vals[N]].
      //   nulls[i] == 1 means element i is NULL.
#define FLIGHT_SQL_APPEND_FIXED_LIST_(CppType, ArrowBuilder, ArrowTypeId)                     \
      } else if (elem_tid == arrow::Type::ArrowTypeId) {                                       \
        if (raw.length() >= static_cast<int64_t>(dim) * static_cast<int64_t>(sizeof(CppType))) { \
          auto *eb = static_cast<arrow::ArrowBuilder *>(list_bld->value_builder());            \
          if (OB_ISNULL(eb)) {                                                                \
            ret = OB_ERR_UNEXPECTED;                                                         \
          } else {                                                                           \
            ret = sql::arrow_status_to_ob_ret(list_bld->Append());                            \
            const CppType *vals = reinterpret_cast<const CppType *>(raw.ptr());               \
            for (int32_t i = 0; OB_SUCC(ret) && i < dim; i++) {                               \
              ret = sql::arrow_status_to_ob_ret(eb->Append(vals[i]));                         \
            }                                                                                \
          }                                                                                  \
        } else { ret = sql::arrow_status_to_ob_ret(builder->AppendNull()); }
#define FLIGHT_SQL_APPEND_VAR_LIST_(CppType, ArrowBuilder, ArrowTypeId)                        \
      } else if (elem_tid == arrow::Type::ArrowTypeId) {                                       \
        if (raw.length() < static_cast<int64_t>(sizeof(uint32_t))) {                          \
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());                                                          \
        } else {                                                                                \
          const uint32_t arr_cnt = *reinterpret_cast<const uint32_t *>(raw.ptr());            \
          const char *body = raw.ptr() + sizeof(uint32_t);                                     \
          const int64_t body_len = raw.length() - static_cast<int64_t>(sizeof(uint32_t));     \
          const int64_t need = static_cast<int64_t>(arr_cnt) * (1 + static_cast<int64_t>(sizeof(CppType))); \
          if (body_len < need) {                                                                \
            ret = sql::arrow_status_to_ob_ret(builder->AppendNull());                                                        \
          } else {                                                                              \
            auto *eb = static_cast<arrow::ArrowBuilder *>(list_bld->value_builder());         \
            if (OB_ISNULL(eb)) {                                                              \
              ret = OB_ERR_UNEXPECTED;                                                       \
            } else {                                                                         \
              ret = sql::arrow_status_to_ob_ret(list_bld->Append());                          \
              const uint8_t *nulls = reinterpret_cast<const uint8_t *>(body);                 \
              const CppType *vals = reinterpret_cast<const CppType *>(body + arr_cnt);        \
              for (uint32_t i = 0; OB_SUCC(ret) && i < arr_cnt; i++) {                         \
                ret = sql::arrow_status_to_ob_ret(                                            \
                    nulls[i] ? eb->AppendNull() : eb->Append(vals[i]));                       \
              }                                                                              \
            }                                                                                   \
          }                                                                                     \
        }
      // Arrow 20: ListBuilder::type() creates a new shared_ptr on every call.
      // Hold it alive for the full if/else chain so raw pointers don't dangle.
      const ArrowDataTypePtr type_sptr = builder->type();
      if (OB_ISNULL(type_sptr)) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      const arrow::Type::type tid = type_sptr->id();
      if (tid == arrow::Type::FIXED_SIZE_LIST) {
        auto *list_bld = static_cast<arrow::FixedSizeListBuilder *>(builder);
        const auto *list_type =
            static_cast<const arrow::FixedSizeListType *>(type_sptr.get());
        const int32_t dim = list_type->list_size();
        const ArrowDataTypePtr elem_type = list_type->value_type();
        if (dim < 0 || OB_ISNULL(elem_type)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        const arrow::Type::type elem_tid = elem_type->id();
        if (false) { // anchor for the macro chain
        FLIGHT_SQL_APPEND_FIXED_LIST_(float,    FloatBuilder,   FLOAT)
        FLIGHT_SQL_APPEND_FIXED_LIST_(double,   DoubleBuilder,  DOUBLE)
        FLIGHT_SQL_APPEND_FIXED_LIST_(int8_t,   Int8Builder,    INT8)
        FLIGHT_SQL_APPEND_FIXED_LIST_(uint8_t,  UInt8Builder,   UINT8)
        FLIGHT_SQL_APPEND_FIXED_LIST_(int16_t,  Int16Builder,   INT16)
        FLIGHT_SQL_APPEND_FIXED_LIST_(uint16_t, UInt16Builder,  UINT16)
        FLIGHT_SQL_APPEND_FIXED_LIST_(int32_t,  Int32Builder,   INT32)
        FLIGHT_SQL_APPEND_FIXED_LIST_(uint32_t, UInt32Builder,  UINT32)
        FLIGHT_SQL_APPEND_FIXED_LIST_(int64_t,  Int64Builder,   INT64)
        FLIGHT_SQL_APPEND_FIXED_LIST_(uint64_t, UInt64Builder,  UINT64)
        } else {
          flight_sql_log_append_null_("unsupported_fixed_list_elem", obj, builder, col_idx, col_name);
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        }
      } else if (tid == arrow::Type::LIST) {
        auto *list_bld = static_cast<arrow::ListBuilder *>(builder);
        const auto *list_type = static_cast<const arrow::ListType *>(type_sptr.get());
        const ArrowDataTypePtr elem_type = list_type->value_type();
        if (OB_ISNULL(elem_type)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        const arrow::Type::type elem_tid = elem_type->id();
        if (false) { // anchor for the macro chain
        FLIGHT_SQL_APPEND_VAR_LIST_(float,    FloatBuilder,   FLOAT)
        FLIGHT_SQL_APPEND_VAR_LIST_(double,   DoubleBuilder,  DOUBLE)
        FLIGHT_SQL_APPEND_VAR_LIST_(int8_t,   Int8Builder,    INT8)
        FLIGHT_SQL_APPEND_VAR_LIST_(uint8_t,  UInt8Builder,   UINT8)
        FLIGHT_SQL_APPEND_VAR_LIST_(int16_t,  Int16Builder,   INT16)
        FLIGHT_SQL_APPEND_VAR_LIST_(uint16_t, UInt16Builder,  UINT16)
        FLIGHT_SQL_APPEND_VAR_LIST_(int32_t,  Int32Builder,   INT32)
        FLIGHT_SQL_APPEND_VAR_LIST_(uint32_t, UInt32Builder,  UINT32)
        FLIGHT_SQL_APPEND_VAR_LIST_(int64_t,  Int64Builder,   INT64)
        FLIGHT_SQL_APPEND_VAR_LIST_(uint64_t, UInt64Builder,  UINT64)
        } else {
          flight_sql_log_append_null_("unsupported_list_elem", obj, builder, col_idx, col_name);
          ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
        }
      } else {
        ret = sql::arrow_status_to_ob_ret(static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
            reinterpret_cast<const uint8_t *>(raw.ptr()),
            static_cast<int64_t>(raw.length())));
      }
#undef FLIGHT_SQL_APPEND_FIXED_LIST_
#undef FLIGHT_SQL_APPEND_VAR_LIST_
      break;
    }
    default:
      flight_sql_log_append_null_("unsupported_ob_type", obj, builder, col_idx, col_name);
      ret = sql::arrow_status_to_ob_ret(builder->AppendNull());
      break;
  }
  return ret;
}

// ============================================================
// Task executor context setup (mirrors obmp_query.cpp / ob_inner_sql_connection.cpp)
// ============================================================

static int flight_sql_setup_task_exec_ctx_(sql::ObResultSet &result_set,
                                             share::schema::ObSchemaGetterGuard &schema_guard,
                                             uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  auto &task_exec_ctx = result_set.get_exec_context().get_task_exec_ctx();
  task_exec_ctx.schema_service_ = GCTX.schema_service_;
  task_exec_ctx.set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  int64_t local_tenant_schema_version = 0;
  int64_t local_sys_schema_version = 0;
  if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_tenant_schema_version))) {
    LOG_WARN("failed to set schema version", K(ret));
  } else if (OB_FALSE_IT(task_exec_ctx.set_query_tenant_begin_schema_version(local_tenant_schema_version))) {
    LOG_WARN("failed to set tenant schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(common::OB_SYS_TENANT_ID, local_sys_schema_version))) {
    LOG_WARN("failed to set sys schema version", K(ret));
  } else {
    task_exec_ctx.set_query_sys_begin_schema_version(local_sys_schema_version);
  }
  return ret;
}

// Native compilation can request a resource-group switch or an SPM recompile
// before open() starts SQL execution or consumes an Arrow ingest stream.
// Group switches need a fresh SQL context; SPM retries retain the native
// outline/baseline state, as ObMPQuery does. Never retry open()/fetch here.
template <typename Compile>
static int flight_sql_compile_in_resource_group_(
    ObFlightSqlSession &session,
    ObFlightSqlExecutionScope &execution_scope,
    sql::ObSqlCtx &sql_ctx,
    sql::ObResultSet &result_set,
    const common::ObString &sql,
    const bool ps_protocol,
    Compile &&compile)
{
  int ret = OB_SUCCESS;
  const int64_t max_group_switches = 2;
  int64_t group_switches = 0;
  bool retry = true;
  share::schema::ObSchemaGetterGuard *const schema_guard = sql_ctx.schema_guard_;
  common::ObIAllocator &allocator = result_set.get_mem_pool();
  const bool sensitive_sql = sql_ctx.is_sensitive_;
  while (retry) {
    retry = false;
    bool reset_sql_ctx = false;
    session.ob_session->set_expect_group_id(common::OB_INVALID_ID);
    ret = compile();
    if (OB_NEED_SWITCH_CONSUMER_GROUP == ret) {
      const uint64_t expected_group_id = session.ob_session->get_expect_group_id();
      if (common::OB_INVALID_ID == expected_group_id
          || expected_group_id == THIS_WORKER.get_group_id()
          || group_switches >= max_group_switches) {
        LOG_WARN("[FlightSQL] cannot resolve SQL resource group after compilation",
                 K(ret), K(expected_group_id), K(group_switches),
                 "current_group_id", THIS_WORKER.get_group_id());
      } else if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(execution_scope.switch_resource_group(expected_group_id))) {
        LOG_WARN("[FlightSQL] failed to move RPC thread to SQL resource group",
                 K(ret), K(expected_group_id));
      } else {
        ++group_switches;
        reset_sql_ctx = true;
        retry = true;
      }
    } else if (OB_SQL_RETRY_SPM == ret) {
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
      } else {
        retry = true;
      }
    }
    if (retry) {
      const bool is_sensitive = sensitive_sql || sql_ctx.is_sensitive_;
      result_set.~ObResultSet();
      if (reset_sql_ctx) {
        sql_ctx.reset();
        sql_ctx.session_info_ = session.ob_session;
        sql_ctx.schema_guard_ = schema_guard;
        sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
        sql_ctx.is_flight_sql_ = true;
        sql_ctx.enable_sql_resource_manage_ = true;
      } else {
        // clear() drops per-attempt pointers but preserves first_outline_data_,
        // first_plan_hash_, and spm_ctx_ required by native local retries. Their
        // allocations belong to the request allocator, not the result object.
        sql_ctx.clear();
        ++sql_ctx.retry_times_;
      }
      sql_ctx.is_sensitive_ = is_sensitive;
      sql_ctx.cur_sql_ = sql;
      new (&result_set) sql::ObResultSet(*session.ob_session, allocator);
      if (ps_protocol) {
        result_set.set_ps_protocol();
      }
      if (OB_FAIL(result_set.init())) {
        LOG_WARN("[FlightSQL] failed to initialize compilation retry result", K(ret));
      } else if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] compilation retry has no schema guard", K(ret));
      } else if (OB_FAIL(flight_sql_setup_task_exec_ctx_(
                     result_set, *schema_guard, session.tenant_id))) {
        LOG_WARN("[FlightSQL] failed to initialize compilation retry task context", K(ret));
      } else {
        result_set.get_exec_context().set_flight_sql_stream_registry(
            session.ob_session->get_flight_sql_stream_registry());
      }
      retry = OB_SUCC(ret);
    }
  }
  session.ob_session->set_expect_group_id(common::OB_INVALID_ID);
  return ret;
}

static int flight_sql_stmt_query_in_resource_group_(
    ObFlightSqlSession &session,
    ObFlightSqlExecutionScope &execution_scope,
    const common::ObString &sql,
    sql::ObSqlCtx &sql_ctx,
    sql::ObResultSet &result_set)
{
  return flight_sql_compile_in_resource_group_(
      session, execution_scope, sql_ctx, result_set, sql, false,
      [&]() { return GCTX.sql_engine_->stmt_query(sql, sql_ctx, result_set); });
}

// ============================================================
// GV$OB_SQL_AUDIT — same pipeline as obmp_query.cpp (mutate session audit_record_)
// QUERY_SQL: optional non-secret catalog/RPC tags + newline, then the statement.
// ============================================================

struct FlightSqlAuditTimeSource {
  int64_t receive_ts_{0};
  int64_t enqueue_ts_{0};
  int64_t run_ts_{0};
  int64_t process_ts_{0};
  int64_t single_process_ts_{0};
  int64_t exec_start_ts_{0};
  int64_t exec_end_ts_{0};
  int64_t get_send_timestamp() const { return receive_ts_; }
  int64_t get_receive_timestamp() const { return receive_ts_; }
  int64_t get_enqueue_timestamp() const { return enqueue_ts_; }
  int64_t get_run_timestamp() const { return run_ts_; }
  int64_t get_process_timestamp() const { return process_ts_; }
  int64_t get_single_process_timestamp() const { return single_process_ts_; }
  int64_t get_exec_start_timestamp() const { return exec_start_ts_; }
  int64_t get_exec_end_timestamp() const { return exec_end_ts_; }
};

static bool flight_sql_audit_tag_printable_(const char *const s, const size_t len)
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

// Build full QUERY_SQL text: optional `-- flightsql: …` line comment, newline, then statement
static int flight_sql_build_tagged_audit_sql_(
    const ObFlightSqlSession &flight_sess,
    const char *request_name,
    const common::ObString &sql_plain,
    sql::ObSQLSessionInfo &ob_sess,
    common::ObIAllocator &allocator,
    common::ObString &out_audit_sql)
{
  int ret = common::OB_SUCCESS;
  std::string sql_full;
  std::string cat;
  {
    std::lock_guard<std::mutex> guard(flight_sess.mutex);
    cat = flight_sess.current_catalog;
  }
  const size_t cat_len = strnlen(cat.c_str(), cat.size());
  const bool cat_ok = cat_len > 0 && cat_len == cat.size()
                      && flight_sql_audit_tag_printable_(cat.c_str(), cat_len);
  static const size_t MAX_REQUEST_NAME_LEN = 64;
  const size_t request_len = OB_ISNULL(request_name)
      ? 0 : strnlen(request_name, MAX_REQUEST_NAME_LEN);
  const bool request_ok = request_len > 0 && request_len < MAX_REQUEST_NAME_LEN
                          && flight_sql_audit_tag_printable_(request_name, request_len);
  if (cat_ok || request_ok) {
    sql_full.append("-- flightsql:");
    if (cat_ok) {
      sql_full.append(" catalog=");
      sql_full.append(cat.c_str(), cat_len);
    }
    if (request_ok) {
      sql_full.append(" request=");
      sql_full.append(request_name, request_len);
    }
    sql_full.append("\n");
  }
  if (sql_plain.length() > 0 && OB_NOT_NULL(sql_plain.ptr())) {
    sql_full.append(sql_plain.ptr(), static_cast<size_t>(sql_plain.length()));
  }
  const int64_t limit = ob_sess.get_tenant_query_record_size_limit();
  if (limit > 0 && static_cast<int64_t>(sql_full.size()) > limit) {
    sql_full.resize(static_cast<size_t>(limit > 3 ? limit - 3 : 0));
    sql_full.append("...");
  }
  const common::ObString src(static_cast<int32_t>(sql_full.size()), sql_full.c_str());
  OZ(common::ob_write_string(allocator, src, out_audit_sql));
  return ret;
}

// After stmt_query (and optional open/fetch/close), mirror obmp_query.cpp tail: fill session audit + handle_audit_record.
static void flight_sql_finalize_statement_audit_like_mpquery_(
    ObFlightSqlSession &flight_sess,
    sql::ObSQLSessionInfo &session,
    sql::ObSqlCtx &sql_ctx,
    sql::ObResultSet &result,
    const int exec_ret,
    common::ObIAllocator &allocator,
    const common::ObString &sql_plain,
    const char *request_name,
    FlightSqlAuditTimeSource &ts,
    const bool enable_perf_event,
    common::ObWaitEventStat &total_wait_desc,
    const sql::ObExecuteMode execute_mode = sql::EXECUTE_LOCAL)
{
  ts.exec_end_ts_ = common::ObTimeUtility::current_time();
  session.set_stmt_type(result.get_stmt_type());
  sql::ObAuditRecordData &audit_record = session.get_raw_audit_record();
  const bool first_record = (1 == audit_record.try_cnt_);
  sql::ObExecStatUtils::record_exec_timestamp(ts, first_record, audit_record.exec_timestamp_, false);
  audit_record.exec_timestamp_.update_stage_time();
  if (enable_perf_event && OB_NOT_NULL(result.get_physical_plan())) {
    const int64_t time_cost = ts.exec_end_ts_ - ts.get_receive_timestamp();
    sql::ObSQLUtils::record_execute_time(result.get_physical_plan()->get_plan_type(), time_cost);
  }
  if (enable_perf_event) {
    audit_record.exec_record_.record_end();
    audit_record.stmt_type_ = result.get_stmt_type();
    audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
    audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
    audit_record.update_event_stage_state();
  }
  if (OB_LIKELY(nullptr != result.get_physical_plan())) {
    session.partition_hit().freeze();
  }

  audit_record.status_ = (common::OB_SUCCESS == exec_ret || common::OB_ITER_END == exec_ret)
      ? obmysql::REQUEST_SUCC
      : exec_ret;

  sql::ObPhysicalPlan *plan = result.get_physical_plan();
  const bool enable_sql_audit = GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();
  if (enable_sql_audit) {
    common::ObString tagged_sql;
    const int wret = flight_sql_build_tagged_audit_sql_(
        flight_sess, request_name, sql_plain, session, allocator, tagged_sql);
    audit_record.seq_ = 0;
    audit_record.execution_id_ = session.get_current_execution_id();
    audit_record.client_addr_ = session.get_peer_addr();
    audit_record.user_client_addr_ = session.get_user_client_addr();
    audit_record.user_group_ = THIS_WORKER.get_group_id();
    MEMCPY(audit_record.sql_id_, sql_ctx.sql_id_, static_cast<int32_t>(sizeof(audit_record.sql_id_)));
    MEMCPY(audit_record.format_sql_id_, sql_ctx.format_sql_id_,
           static_cast<int32_t>(sizeof(audit_record.format_sql_id_)));
    audit_record.format_sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    if (common::OB_SUCCESS == wret && tagged_sql.length() > 0 && OB_NOT_NULL(tagged_sql.ptr())) {
      audit_record.sql_ = const_cast<char *>(tagged_sql.ptr());
      audit_record.sql_len_ = tagged_sql.length();
      audit_record.sql_cs_type_ = session.get_local_collation_connection();
      // get_final_audit_record(EXECUTE_PS_PREPARE) reloads SQL from the
      // session's current query string. Keep that source tagged as well so
      // prepare and execute audit rows expose the same Flight session header.
      if (sql::EXECUTE_PS_PREPARE == execute_mode) {
        const int store_ret = session.store_query_string(tagged_sql);
        if (OB_SUCCESS != store_ret) {
          LOG_WARN_RET(store_ret,
                       "[FlightSQL] failed to retain tagged prepared SQL for audit",
                       K(store_ret));
        }
      }
    } else {
      const int64_t lim = session.get_tenant_query_record_size_limit();
      const int64_t use_len = std::min(static_cast<int64_t>(sql_plain.length()), lim);
      audit_record.sql_ = const_cast<char *>(sql_plain.ptr());
      audit_record.sql_len_ = use_len;
      audit_record.sql_cs_type_ = session.get_local_collation_connection();
    }
    audit_record.ccl_rule_id_ = sql_ctx.ccl_rule_id_;
    audit_record.ccl_match_time_ = sql_ctx.ccl_match_time_;
    if (OB_NOT_NULL(plan)) {
      audit_record.plan_type_ = plan->get_plan_type();
      audit_record.table_scan_ = plan->contain_table_scan();
      audit_record.plan_id_ = plan->get_plan_id();
      audit_record.plan_hash_ = plan->get_plan_hash_value();
      audit_record.rule_name_ = const_cast<char *>(plan->get_rule_name().ptr());
      audit_record.rule_name_len_ = plan->get_rule_name().length();
    }
    if (OB_NOT_NULL(plan) || result.is_pl_stmt(result.get_stmt_type())) {
      audit_record.partition_hit_ = session.partition_hit().get_bool();
    }
    if (OB_SUCCESS != exec_ret && 0 == audit_record.trans_id_) {
      if (session.is_in_transaction()) {
        audit_record.trans_id_ = session.get_tx_id();
      }
    }
    audit_record.affected_rows_ = result.get_affected_rows();
    audit_record.return_rows_ = result.get_return_rows();
    audit_record.partition_cnt_ = result.get_exec_context().get_das_ctx().get_related_tablet_cnt();
    audit_record.expected_worker_cnt_ = result.get_exec_context().get_task_exec_ctx().get_expected_worker_cnt();
    audit_record.used_worker_cnt_ = result.get_exec_context().get_task_exec_ctx().get_admited_worker_cnt();
    audit_record.is_executor_rpc_ = false;
    audit_record.is_inner_sql_ = false;
    audit_record.is_hit_plan_cache_ = result.get_is_from_plan_cache() && !sql_ctx.self_add_plan_;
    audit_record.is_multi_stmt_ = session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS;
    audit_record.is_batched_multi_stmt_ = sql_ctx.multi_stmt_item_.is_batched_multi_stmt();
    audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
    audit_record.plsql_exec_time_ = session.get_plsql_exec_time();
    audit_record.plsql_compile_time_ = session.get_plsql_compile_time();
    if (result.is_pl_stmt(result.get_stmt_type()) && OB_NOT_NULL(common::ObCurTraceId::get_trace_id())) {
      audit_record.pl_trace_id_ = *common::ObCurTraceId::get_trace_id();
    }
    sql::ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx)) {
      audit_record.consistency_level_ = plan_ctx->get_consistency_level();
      audit_record.total_memstore_read_row_count_ = plan_ctx->get_total_memstore_read_row_count();
      audit_record.total_ssstore_read_row_count_ = plan_ctx->get_total_ssstore_read_row_count();
    }
  }

  if (session.get_local_ob_enable_plan_cache() && !THIS_WORKER.need_retry()) {
    common::ObIArray<sql::ObTableRowCount> *table_row_count_list = nullptr;
    sql::ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx)) {
      table_row_count_list = &(plan_ctx->get_table_row_count_list());
      audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
    }
    if (OB_NOT_NULL(plan) && OB_NOT_NULL(plan_ctx)) {
      sql::AdaptivePCConf adpt_pc_conf;
      bool enable_adaptive_pc = plan_ctx->enable_adaptive_pc();
      if (enable_adaptive_pc) {
        adpt_pc_conf = session.get_adaptive_pc_conf();
      }
      if (!sql_ctx.self_add_plan_ && sql_ctx.plan_cache_hit_) {
        plan->update_plan_stat(audit_record, false, table_row_count_list,
                               enable_adaptive_pc ? &adpt_pc_conf : nullptr);
        plan->update_cache_access_stat(audit_record.table_scan_stat_);
      } else if (sql_ctx.self_add_plan_ && !sql_ctx.plan_cache_hit_) {
        plan->update_plan_stat(audit_record, true, table_row_count_list,
                               enable_adaptive_pc ? &adpt_pc_conf : nullptr);
        plan->update_cache_access_stat(audit_record.table_scan_stat_);
      } else if (sql_ctx.self_add_plan_ && sql_ctx.plan_cache_hit_) {
        plan->update_plan_stat(audit_record, true, table_row_count_list,
                               enable_adaptive_pc ? &adpt_pc_conf : nullptr);
        plan->update_cache_access_stat(audit_record.table_scan_stat_);
      }
    }
  }

  (void)sql::ObSQLUtils::handle_audit_record(false, execute_mode, session, sql_ctx.is_sensitive_);
}

// ============================================================
// OB session setup
// ============================================================

int64_t ObFlightSqlQueryProcessor::start_request_timeout(ObFlightSqlSession &session)
{
  int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
  if (OB_ISNULL(session.ob_session)) {
    return timeout_ts;
  }

  // Mirror ObInnerSQLConnection: set query_start_time first, then arm the
  // worker with (start_time + ob_query_timeout) so init_plan_exec_context
  // computes a future absolute timeout rather than (0 + timeout) = past.
  const int64_t start_time = common::ObTimeUtility::current_time();
  session.ob_session->set_query_start_time(start_time);

  int64_t timeout_us = 10LL * 60 * 1000 * 1000;  // 10 min fallback
  int64_t t = 0;
  if (session.ob_session->get_query_timeout(t) == OB_SUCCESS && t > 0) {
    timeout_us = t;
  }
  timeout_ts = start_time + timeout_us;
  return timeout_ts;
}

int ObFlightSqlQueryRetry::init(ObFlightSqlSession &session, const common::ObString &sql)
{
  int ret = OB_SUCCESS;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_ISNULL(session.ob_session) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] retry: tenant switch failed", K(ret), K(session.tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                 session.ob_session->get_effective_tenant_id(), tenant_version))) {
    LOG_WARN("[FlightSQL] retry: failed to get tenant schema version", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                 common::OB_SYS_TENANT_ID, sys_version))) {
    LOG_WARN("[FlightSQL] retry: failed to get system schema version", K(ret));
  } else {
    common::ObCurTraceId::init(GCTX.self_addr());
    trace_id_ = *common::ObCurTraceId::get_trace_id();
    if (OB_FAIL(session.ob_session->check_and_init_retry_info(trace_id_, sql))) {
      LOG_WARN("[FlightSQL] retry: failed to initialize session retry info", K(ret));
    } else {
      retry_ctrl_.set_tenant_global_schema_version(tenant_version);
      retry_ctrl_.set_sys_global_schema_version(sys_version);
      session.ob_session->set_session_in_retry(sql::SESS_NOT_IN_RETRY);
      timeout_ts_ = ObFlightSqlQueryProcessor::start_request_timeout(session);
      initialized_ = true;
    }
  }
  return ret;
}

void ObFlightSqlQueryRetry::begin_attempt(ObFlightSqlSession &session)
{
  retry_ctrl_.clear_state_before_each_retry(session.ob_session->get_retry_info_for_update());
}

int ObFlightSqlQueryRetry::set_schema_versions(
    ObFlightSqlSession &session,
    share::schema::ObSchemaGetterGuard &schema_guard,
    sql::ObSqlCtx &sql_ctx)
{
  int ret = OB_SUCCESS;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  if (OB_FAIL(schema_guard.get_schema_version(
          session.ob_session->get_effective_tenant_id(), tenant_version))) {
    LOG_WARN("[FlightSQL] retry: failed to get local tenant schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(common::OB_SYS_TENANT_ID, sys_version))) {
    LOG_WARN("[FlightSQL] retry: failed to get local system schema version", K(ret));
  } else {
    retry_ctrl_.set_tenant_local_schema_version(tenant_version);
    retry_ctrl_.set_sys_local_schema_version(sys_version);
    sql_ctx.retry_times_ = retry_ctrl_.get_retry_times();
  }
  return ret;
}

void ObFlightSqlQueryRetry::check_retry(
    sql::ObSqlCtx &sql_ctx, sql::ObResultSet &result, int &client_ret)
{
  const int error = client_ret;
  if (sql::stmt::T_SELECT == result.get_stmt_type()
      && (OB_SCHEMA_EAGAIN == error || OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == error)) {
    // Flight RPCs cannot be put back on the MySQL request queue. Use the same
    // schema policy, but keep local retries within the original deadline.
    retry_ctrl_.test_and_save_retry_state(GCTX, sql_ctx, result, error, client_ret,
                                         true /* force_local_retry */);
    if (need_retry()) {
      result.set_will_retry();
    }
  }
}

void ObFlightSqlQueryRetry::update_session(ObFlightSqlSession &session)
{
  session.ob_session->set_session_in_retry(retry_ctrl_.need_retry());
}

void ObFlightSqlQueryRetry::finish(ObFlightSqlSession &session)
{
  if (initialized_ && OB_NOT_NULL(session.ob_session)) {
    session.ob_session->check_and_reset_retry_info(trace_id_, false /* packet_retry */);
    session.ob_session->set_session_in_retry(sql::SESS_NOT_IN_RETRY);
    initialized_ = false;
  }
}

// ============================================================
// Arrow type mapping
// ============================================================

std::shared_ptr<arrow::DataType> ObFlightSqlQueryProcessor::ob_type_to_arrow(
    common::ObObjType t)
{
  using namespace common;
  switch (t) {
    case ObNullType:       return arrow::null();
    case ObTinyIntType:    return arrow::int8();
    case ObSmallIntType:   return arrow::int16();
    // Both MEDIUMINT ranges fit Spark IntegerType.  Use signed Arrow int32 so
    // Spark can consume the IntVector directly without unsigned widening.
    case ObMediumIntType:  return arrow::int32();
    case ObInt32Type:      return arrow::int32();
    case ObIntType:        return arrow::int64();
    case ObUTinyIntType:   return arrow::uint8();
    case ObUSmallIntType:  return arrow::uint16();
    case ObUMediumIntType: return arrow::int32();
    case ObUInt32Type:     return arrow::uint32();
    case ObUInt64Type:     return arrow::uint64();
    case ObFloatType:
    case ObUFloatType:     return arrow::float32();
    case ObDoubleType:
    case ObUDoubleType:    return arrow::float64();
    case ObNumberType:
    case ObUNumberType:    return arrow::decimal128(38, 10);  // fallback; build_schema_ uses ObAccuracy
    case ObDecimalIntType: return arrow::decimal128(38, 10);  // fallback; build_schema_ uses ObAccuracy
    // DATETIME: naive wall-clock (no TZ). TIMESTAMP: UTC instant.
    // Spark/Arrow map timezone-less timestamp to TimestampNTZ and tz=UTC to UTC instant.
    case ObDateTimeType:        return arrow::timestamp(arrow::TimeUnit::MICRO);
    case ObTimestampType:       return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    case ObDateType:       return arrow::date32();
    case ObMySQLDateType:       return arrow::date32();
    case ObMySQLDateTimeType:   return arrow::timestamp(arrow::TimeUnit::MICRO);
    case ObTimeType:       return arrow::duration(arrow::TimeUnit::MICRO);
    case ObYearType:       return arrow::int16();
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:   return arrow::utf8();
    case ObBitType:        return arrow::uint64();
    case ObEnumType:
    case ObSetType:        return arrow::utf8();
    case ObLobType:        return arrow::large_binary();
    case ObJsonType:       return arrow::utf8();
    case ObGeometryType:   return arrow::binary();
    case ObURowIDType:     return arrow::utf8();
    default:               return arrow::utf8();
  }
}

std::shared_ptr<arrow::DataType> ObFlightSqlQueryProcessor::collection_type_to_arrow_(
    const common::ObCollectionTypeBase *coll_type)
{
  using namespace common;
  std::shared_ptr<arrow::DataType> arrow_type;
  if (OB_ISNULL(coll_type)) {
    // The caller keeps the existing opaque-binary fallback for unsupported
    // collection metadata.  Returning nullptr here prevents a partially
    // described nested type from being advertised as a valid Arrow schema.
  } else if (coll_type->type_id_ == OB_BASIC_TYPE) {
    const auto *basic = static_cast<const ObCollectionBasicType *>(coll_type);
    // The Flight ARRAY codecs below are deliberately limited to the mature
    // fixed-width numeric matrix.  Do not advertise, for example,
    // list<list<string>> until its native LOB/string codec exists.
    switch (basic->basic_meta_.get_obj_type()) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObFloatType:
      case ObUFloatType:
      case ObDoubleType:
      case ObUDoubleType:
        arrow_type = ob_type_to_arrow(basic->basic_meta_.get_obj_type());
        break;
      default:
        break;
    }
  } else if (coll_type->type_id_ == OB_ARRAY_TYPE) {
    const auto *arr_type = static_cast<const ObCollectionArrayType *>(coll_type);
    if (OB_NOT_NULL(arr_type->element_type_)
        && arr_type->element_type_->type_id_ != OB_VECTOR_TYPE
        && (arrow_type = collection_type_to_arrow_(arr_type->element_type_))) {
      // Keep every SQL ARRAY level as an Arrow LIST level.  In particular,
      // ARRAY(ARRAY(INT)) must become list<list<int32>>, rather than treating
      // the inner ARRAY metadata as a primitive element descriptor.
      arrow_type = arrow::list(std::move(arrow_type));
    }
  } else if (coll_type->type_id_ == OB_VECTOR_TYPE) {
    const auto *vec_type = static_cast<const ObCollectionArrayType *>(coll_type);
    // Native VECTOR has a scalar element and a fixed dimension.  It is not a
    // general recursive collection, so do not advertise a nested VECTOR shape
    // that the native factory cannot materialize.
    if (vec_type->dim_cnt_ > 0 && OB_NOT_NULL(vec_type->element_type_)
        && vec_type->element_type_->type_id_ == OB_BASIC_TYPE
        && (arrow_type = collection_type_to_arrow_(vec_type->element_type_))) {
      arrow_type = arrow::fixed_size_list(
          std::move(arrow_type), static_cast<int32_t>(vec_type->dim_cnt_));
    }
  } else if (coll_type->type_id_ == OB_MAP_TYPE) {
    const auto *map_type = static_cast<const ObCollectionMapType *>(coll_type);
    // Internally a MAP owns an ARRAY for keys and an ARRAY for values.  The
    // initial Flight read path is deliberately bounded to the same fixed-width
    // numeric leaves as ARRAY, rather than advertising a partially decoded
    // string/LOB or nested-MAP shape.
    if (OB_NOT_NULL(map_type->key_type_)
        && OB_NOT_NULL(map_type->value_type_)
        && map_type->key_type_->type_id_ == OB_ARRAY_TYPE
        && map_type->value_type_->type_id_ == OB_ARRAY_TYPE) {
      const auto *key_array =
          static_cast<const ObCollectionArrayType *>(map_type->key_type_);
      const auto *value_array =
          static_cast<const ObCollectionArrayType *>(map_type->value_type_);
      std::shared_ptr<arrow::DataType> key_type;
      std::shared_ptr<arrow::DataType> value_type;
      if (OB_NOT_NULL(key_array->element_type_)
          && OB_NOT_NULL(value_array->element_type_)
          && key_array->element_type_->type_id_ == OB_BASIC_TYPE
          && value_array->element_type_->type_id_ == OB_BASIC_TYPE
          && (key_type = collection_type_to_arrow_(key_array->element_type_))
          && (value_type = collection_type_to_arrow_(value_array->element_type_))) {
        // Arrow MAP keys are non-null by definition.  The row converter
        // rejects a native NULL key rather than emitting an invalid Arrow
        // record batch; null map values and null maps remain supported.
        arrow_type = arrow::map(std::move(key_type), std::move(value_type));
      }
    }
  }
  return arrow_type;
}

std::shared_ptr<arrow::Schema> ObFlightSqlQueryProcessor::build_schema(
    const common::ColumnsFieldIArray *fields,
    const sql::ObExecContext *exec_ctx,
    share::schema::ObSchemaGetterGuard *schema_guard,
    uint64_t tenant_id)
{
  if (OB_ISNULL(fields) || fields->count() == 0) {
    return arrow::schema({});
  }
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  arrow_fields.reserve(fields->count());
  for (int64_t i = 0; i < fields->count(); i++) {
    const common::ObField &f = fields->at(i);
    const std::string col_name(f.cname_.ptr(), f.cname_.length());

    // Base field metadata — clients use this to reconstruct the original OB type.
    std::vector<std::string> meta_keys   = {"ob_type", "ob_precision", "ob_scale",
                                            "ob_length", "ob_collation"};
    std::vector<std::string> meta_values = {
        std::to_string(static_cast<int>(f.type_.get_type())),
        std::to_string(static_cast<int>(f.accuracy_.get_precision())),
        std::to_string(static_cast<int>(f.accuracy_.get_scale())),
        std::to_string(static_cast<int>(f.accuracy_.get_length())),
        std::to_string(static_cast<int>(f.charsetnr_))};

    // DECIMAL/NUMERIC: use decimal128/decimal256 directly so ADBC and Spark
    // receive a native fixed-point type without any string parsing.
    std::shared_ptr<arrow::DataType> arrow_type;
    const common::ObObjType ob_tp = f.type_.get_type();
    if (ob_tp == common::ObNumberType || ob_tp == common::ObUNumberType
        || ob_tp == common::ObDecimalIntType) {
      const int32_t p = (f.accuracy_.get_precision() > 0)
                            ? static_cast<int32_t>(f.accuracy_.get_precision()) : 38;
      const int32_t s = (f.accuracy_.get_scale() >= 0)
                            ? static_cast<int32_t>(f.accuracy_.get_scale()) : 10;
      arrow_type = (p <= 38) ? arrow::decimal128(p, s)
                             : arrow::decimal256(p, s);
    } else if (ob_tp == common::ObCollectionSQLType && OB_NOT_NULL(exec_ctx)) {
      const uint16_t subschema_id = f.type_.get_meta().get_subschema_id();
      sql::ObSubSchemaValue sub_meta;
      if (OB_SUCCESS == exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, sub_meta)
          && sub_meta.type_ == sql::OB_SUBSCHEMA_COLLECTION_TYPE
          && OB_NOT_NULL(sub_meta.value_)) {
        const common::ObSqlCollectionInfo *coll_info =
            reinterpret_cast<const common::ObSqlCollectionInfo *>(sub_meta.value_);
        arrow_type = collection_type_to_arrow_(coll_info->collection_meta_);
      }
      if (!arrow_type) {
        arrow_type = arrow::large_binary();
      }
    } else {
      arrow_type = ob_type_to_arrow(ob_tp);
      // BLOB/VARBINARY: text-family types with binary collation carry raw bytes,
      // not valid UTF-8 — promote to large_binary so Arrow stores them safely.
      // Numeric and date types also carry CS_TYPE_BINARY as their default
      // collation, so restrict the upgrade to text-family OB types only.
      if (arrow_type->id() == arrow::Type::STRING
          && flight_sql_is_text_family(ob_tp)
          && static_cast<common::ObCollationType>(f.charsetnr_)
                 == common::CS_TYPE_BINARY) {
        arrow_type = arrow::large_binary();
      }
    }

    if (ob_tp == common::ObGeometryType) {
      meta_keys.push_back("ob_encoding");
      meta_values.push_back("ewkb");
      meta_keys.push_back("ob_geometry_axis_order");
      meta_values.push_back("xy");
    }

    // ENUM/SET: look up the backing column schema once to get the member list.
    // OB reports both ENUM and SET as ObCharType in ObField, so we can't detect
    // them from ob_tp alone — a schema lookup on the original table column is the
    // only reliable source.  This runs once per column, not per row.
    if (OB_NOT_NULL(schema_guard) && tenant_id != common::OB_INVALID_TENANT_ID
        && f.org_tname_.length() > 0 && f.org_cname_.length() > 0) {
      uint64_t db_id = common::OB_INVALID_ID;
      const share::schema::ObTableSchema   *table_schema = nullptr;
      const share::schema::ObColumnSchemaV2 *col_schema  = nullptr;
      if (f.dname_.length() > 0) {
        (void)schema_guard->get_database_id(tenant_id, f.dname_, db_id);
      }
      if (db_id != common::OB_INVALID_ID) {
        (void)schema_guard->get_table_schema(tenant_id, db_id,
                                             f.org_tname_, false, table_schema);
      }
      if (OB_NOT_NULL(table_schema)) {
        col_schema = table_schema->get_column_schema(f.org_cname_);
      }
      if (OB_NOT_NULL(col_schema)) {
        const common::ObObjType real_type = col_schema->get_meta_type().get_type();
        if (real_type == common::ObEnumType || real_type == common::ObSetType) {
          const common::ObIArray<common::ObString> &type_info =
              col_schema->get_extended_type_info();
          std::string values_str;
          for (int64_t vi = 0; vi < type_info.count(); vi++) {
            if (vi > 0) values_str += ',';
            values_str.append(type_info.at(vi).ptr(),
                              static_cast<size_t>(type_info.at(vi).length()));
          }
          meta_keys.push_back(real_type == common::ObEnumType
                              ? "ob_enum_values" : "ob_set_values");
          meta_values.push_back(std::move(values_str));
        }
      }
    }

    ArrowKVMetadataPtr field_meta = arrow::KeyValueMetadata::Make(meta_keys, meta_values);
    arrow_fields.push_back(
        arrow::field(col_name, std::move(arrow_type), /*nullable=*/true,
                     std::move(field_meta)));
  }
  return arrow::schema(std::move(arrow_fields));
}

std::shared_ptr<arrow::Schema> ObFlightSqlQueryProcessor::build_parameter_schema(
    const common::ParamsFieldIArray *fields)
{
  const std::shared_ptr<arrow::Schema> native_schema = build_schema(fields);
  std::vector<std::shared_ptr<arrow::Field>> positional_fields;
  positional_fields.reserve(native_schema->num_fields());
  for (int i = 0; i < native_schema->num_fields(); ++i) {
    const std::shared_ptr<arrow::Field> &field = native_schema->field(i);
    positional_fields.push_back(arrow::field(
        std::to_string(i), field->type(), field->nullable(), field->metadata()));
  }
  return arrow::schema(std::move(positional_fields), native_schema->metadata());
}

// ============================================================
// prepare()
// ============================================================

static bool is_flight_sql_doget_stmt(const stmt::StmtType stmt_type)
{
  return stmt::T_SELECT == stmt_type
      || stmt::T_EXPLAIN == stmt_type
      || stmt::T_HELP == stmt_type
      || sql::ObStmt::is_show_stmt(stmt_type);
}

int ObFlightSqlQueryProcessor::prepare(ObFlightSqlSession &session,
                                       const common::ObString &sql,
                                       const PrepareMode mode,
                                       const bool query_already_locked)
{
  if (PrepareMode::STATEMENT_QUERY != mode || OB_ISNULL(session.ob_session)) {
    return prepare_once(session, sql, mode, query_already_locked, nullptr);
  }

  ObFlightWorkerGuard worker_guard;
  ObSessionQueryGuard query_guard(session, query_already_locked);
  if (!query_guard.acquired()) {
    return OB_EAGAIN;
  }
  ObFlightSqlQueryRetry retry;
  DEFER(retry.finish(session));
  int ret = retry.init(session, sql);
  if (OB_SUCC(ret)) {
    do {
      retry.begin_attempt(session);
      ret = prepare_once(session, sql, mode, true /* query_already_locked */, &retry);
      retry.update_session(session);
    } while (OB_SUCCESS != ret && retry.need_retry());
  }
  return ret;
}

int ObFlightSqlQueryProcessor::prepare_once(ObFlightSqlSession &session,
                                            const common::ObString &sql,
                                            const PrepareMode mode,
                                            const bool query_already_locked,
                                            ObFlightSqlQueryRetry *retry)
{
  int ret = OB_SUCCESS;
  arrow_schema_.reset();
  trace_id_str_.clear();
  error_detail_.clear();
  plan_id_ = 0;
  sql_id_str_.clear();
  is_hit_plan_cache_ = false;
  plan_type_ = sql::OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_ = sql::stmt::T_NONE;

  if (OB_ISNULL(session.ob_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] prepare: ob_session is null", K(ret));
    return ret;
  }
  if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
      || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] prepare: server not yet initialized", K(ret));
    return ret;
  }

  ObSessionQueryGuard query_guard(session, query_already_locked);
  if (!query_guard.acquired()) {
    ret = OB_EAGAIN;
    LOG_WARN("[FlightSQL] prepare: concurrent query on same session rejected — "
             "use separate sessions for parallel queries", K(ret));
    return ret;
  }

  if (OB_NOT_NULL(retry)) {
    common::ObCurTraceId::set(retry->trace_id());
  } else {
    common::ObCurTraceId::init(GCTX.self_addr());
  }
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] QueryProcessor::prepare: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  const int64_t timeout_ts = OB_NOT_NULL(retry)
      ? retry->timeout_ts() : start_request_timeout(session);
  lib::WorkerTimeoutGuard timeout_guard(timeout_ts);
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  } else if (OB_NOT_NULL(retry) && OB_FAIL(THIS_WORKER.check_status())) {
    return ret;
  }

  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session.tenant_id, schema_guard))) {
    LOG_WARN("[FlightSQL] QueryProcessor::prepare: get_tenant_schema_guard failed", K(ret));
    return ret;
  }

  sql::ObSqlCtx sql_ctx;
  sql_ctx.session_info_            = session.ob_session;
  sql_ctx.schema_guard_            = &schema_guard;
  sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx.is_flight_sql_           = true;
  sql_ctx.enable_sql_resource_manage_ = true;
  if (OB_NOT_NULL(retry) && OB_FAIL(retry->set_schema_versions(session, schema_guard, sql_ctx))) {
    return ret;
  }

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
    ObReqTimeGuard req_time_guard;
    common::ObArenaAllocator arena_alloc("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         static_cast<int64_t>(session.tenant_id));
    char rs_buf[sizeof(sql::ObResultSet)] __attribute__((aligned(16)));
    auto *result_set = new (rs_buf) sql::ObResultSet(*session.ob_session, arena_alloc);
    ObResultSetPlacementGuard rs_guard(result_set);
    if (OB_FAIL(result_set->init())) {
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: result_set->init failed", K(ret));
      return ret;
    }
    if (OB_FAIL(flight_sql_setup_task_exec_ctx_(*result_set, schema_guard, session.tenant_id))) {
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: setup_task_exec_ctx failed",
        K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
        "sql", sql.ptr());
      return ret;
    }
    if (OB_FAIL(flight_sql_stmt_query_in_resource_group_(
            session, execution_scope, sql, sql_ctx, *result_set))) {
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: stmt_query failed",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "sql", sql.ptr());
      if (OB_NOT_NULL(retry)) {
        retry->check_retry(sql_ctx, *result_set, ret);
      }
      return ret;
    }
    const stmt::StmtType stmt_type = result_set->get_stmt_type();
    const bool is_with_rows = result_set->is_with_rows();
    stmt_type_ = stmt_type;
    is_hit_plan_cache_ = sql_ctx.plan_cache_hit_;
    const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
    if (OB_NOT_NULL(plan)) {
      plan_id_ = plan->get_plan_id();
      plan_type_ = plan->get_plan_type();
    }
    if (sql_ctx.sql_id_[0] != '\0') {
      sql_id_str_ = sql_ctx.sql_id_;
    }
    if (PrepareMode::STATEMENT_UPDATE == mode && is_with_rows) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: StatementUpdate only accepts "
               "non-row DDL/DML/command statements",
               K(ret), K(stmt_type));
      return ret;
    } else if (PrepareMode::STATEMENT_QUERY == mode && !is_with_rows) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: StatementQuery requires a "
               "result-producing statement; use StatementUpdate for non-query SQL",
               K(ret), K(stmt_type));
      return ret;
    } else if (PrepareMode::STATEMENT_QUERY == mode
               && OB_FAIL(flight_sql_validate_result_types_(
                   result_set->get_field_columns(), plan, trace_id_str_, error_detail_))) {
      return ret;
    }
    arrow_schema_ = build_schema(result_set->get_field_columns(),
                                 &result_set->get_exec_context(),
                                 &schema_guard, session.tenant_id);
    LOG_TRACE("[FlightSQL] QueryProcessor::prepare: OK",
             "ob_trace_id", ObCurTraceId::get_trace_id_str(),
             "plan_id", plan_id_, "sql_id", sql_id_str_.c_str(),
             "field_count", OB_NOT_NULL(result_set->get_field_columns())
                 ? result_set->get_field_columns()->count() : 0);
  }

  if (OB_NOT_NULL(session.ob_session)) {
    session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  }
  return ret;
}

namespace
{

bool flight_sql_is_supported_prepared_dml(const sql::stmt::StmtType stmt_type)
{
  return sql::stmt::T_INSERT == stmt_type
      || sql::stmt::T_UPDATE == stmt_type
      || sql::stmt::T_DELETE == stmt_type;
}

FlightSqlAuditTimeSource flight_sql_make_audit_time_source_(
    const sql::ObSQLSessionInfo &session)
{
  FlightSqlAuditTimeSource ts;
  const int64_t query_start = session.get_query_start_time();
  const int64_t receive_time = query_start > 0
      ? query_start : common::ObTimeUtility::current_time();
  ts.receive_ts_ = receive_time;
  ts.enqueue_ts_ = receive_time;
  ts.run_ts_ = receive_time;
  ts.process_ts_ = receive_time;
  ts.single_process_ts_ = receive_time;
  ts.exec_start_ts_ = receive_time;
  return ts;
}

} // namespace

int ObFlightSqlQueryProcessor::prepare_prepared_update(
    ObFlightSqlSession &session,
    const common::ObString &sql,
    common::ObPsStmtId &client_stmt_id,
    sql::stmt::StmtType &stmt_type,
    int64_t &parameter_count,
    bool &sensitive_sql,
    const char *audit_request_name)
{
  int ret = OB_SUCCESS;
  client_stmt_id = common::OB_INVALID_ID;
  stmt_type = sql::stmt::T_NONE;
  parameter_count = 0;
  sensitive_sql = false;
  arrow_schema_.reset();
  trace_id_str_.clear();
  plan_id_ = 0;
  sql_id_str_.clear();
  is_hit_plan_cache_ = false;
  plan_type_ = sql::OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_ = sql::stmt::T_NONE;
  affected_rows_ = 0;
  execute_time_us_ = 0;

  if (OB_ISNULL(session.ob_session) || sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepare_prepared_update: invalid argument", K(ret));
    return ret;
  } else if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
             || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] prepare_prepared_update: server not initialized", K(ret));
    return ret;
  }

  common::ObCurTraceId::init(GCTX.self_addr());
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] prepare_prepared_update: switch_to failed",
             K(ret), "tenant_id", static_cast<int64_t>(session.tenant_id));
    return ret;
  }
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  }

  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          session.tenant_id, schema_guard))) {
    LOG_WARN("[FlightSQL] prepare_prepared_update: get schema guard failed", K(ret));
    return ret;
  }

  sql::ObSqlCtx sql_ctx;
  sql_ctx.session_info_ = session.ob_session;
  sql_ctx.schema_guard_ = &schema_guard;
  sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx.is_flight_sql_ = true;
  sql_ctx.is_prepare_protocol_ = true;
  sql_ctx.is_prepare_stage_ = true;
  sql_ctx.cur_sql_ = sql;

  ObFlightSqlPreparedStateScope state_scope(
      session, sql, obmysql::ObMySQLCmd::COM_STMT_PREPARE);
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQLPS")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
    ObReqTimeGuard req_time_guard;
    common::ObArenaAllocator arena_alloc("FlightSQLPS", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         static_cast<int64_t>(session.tenant_id));
    char rs_buf[sizeof(sql::ObResultSet)] __attribute__((aligned(16)));
    auto *result_set = new (rs_buf) sql::ObResultSet(*session.ob_session, arena_alloc);
    ObResultSetPlacementGuard rs_guard(result_set);
    if (OB_FAIL(result_set->init())) {
      LOG_WARN("[FlightSQL] prepare_prepared_update: result_set init failed", K(ret));
      return ret;
    } else if (OB_FAIL(flight_sql_setup_task_exec_ctx_(
                   *result_set, schema_guard, session.tenant_id))) {
      LOG_WARN("[FlightSQL] prepare_prepared_update: task context setup failed", K(ret));
      return ret;
    }

    sql::ObSQLSessionInfo &ob_session = *session.ob_session;
    sql::ObAuditRecordData &audit_record = ob_session.get_raw_audit_record();
    audit_record.try_cnt_++;
    ob_session.set_current_execution_id(GCTX.sql_engine_->get_execution_id());
    ob_session.reset_plsql_exec_time();
    ob_session.reset_plsql_compile_time();
    ob_session.set_stmt_type(sql::stmt::T_NONE);
    FlightSqlAuditTimeSource audit_time = flight_sql_make_audit_time_source_(ob_session);
    const bool enable_perf_event = lib::is_diagnose_info_enabled();
    common::ObWaitEventStat total_wait_desc;
    common::ObMaxWaitGuard max_wait_guard(
        enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : nullptr);
    common::ObTotalWaitGuard total_wait_guard(
        enable_perf_event ? &total_wait_desc : nullptr);
    if (enable_perf_event) {
      audit_record.exec_record_.record_start();
    }

    common::ObPsStmtId inner_stmt_id = common::OB_INVALID_ID;
    if (OB_FAIL(GCTX.sql_engine_->stmt_prepare(
            sql, sql_ctx, *result_set, false /* is_inner_sql */))) {
      LOG_WARN("[FlightSQL] prepare_prepared_update: stmt_prepare failed",
               K(ret), "ob_trace_id", common::ObCurTraceId::get_trace_id_str());
    }
    audit_time.exec_start_ts_ = common::ObTimeUtility::current_time();
    sensitive_sql = sql_ctx.is_sensitive_;
    client_stmt_id = result_set->get_statement_id();
    stmt_type = result_set->get_stmt_type();
    stmt_type_ = stmt_type;

    if (OB_SUCC(ret) && common::OB_INVALID_ID == client_stmt_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] prepare_prepared_update: native statement ID is invalid", K(ret));
    } else if (OB_SUCC(ret)
               && OB_FAIL(ob_session.get_inner_ps_stmt_id(client_stmt_id, inner_stmt_id))) {
      LOG_WARN("[FlightSQL] prepare_prepared_update: get inner statement ID failed",
               K(ret), K(client_stmt_id));
    } else if (OB_SUCC(ret)
               && (result_set->is_with_rows()
                   || !flight_sql_is_supported_prepared_dml(stmt_type))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] prepared statement is not INSERT, UPDATE, or DELETE without rows",
               K(ret), K(stmt_type), "is_with_rows", result_set->is_with_rows());
    } else if (OB_SUCC(ret)) {
      const common::ParamsFieldIArray *param_fields = result_set->get_param_fields();
      parameter_count = OB_ISNULL(param_fields) ? 0 : param_fields->count();
      arrow_schema_ = build_parameter_schema(param_fields);
      const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
      if (OB_NOT_NULL(plan)) {
        plan_id_ = plan->get_plan_id();
        plan_type_ = plan->get_plan_type();
      }
      is_hit_plan_cache_ = sql_ctx.plan_cache_hit_;
      if ('\0' != sql_ctx.sql_id_[0]) {
        sql_id_str_ = sql_ctx.sql_id_;
      }
    }

    audit_record.ps_stmt_id_ = client_stmt_id;
    audit_record.ps_inner_stmt_id_ = inner_stmt_id;
    const common::ObString audit_sql = sensitive_sql
        ? common::ObString(OB_MASKED_STR) : sql;
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, ob_session, sql_ctx, *result_set, ret, arena_alloc, audit_sql,
        audit_request_name, audit_time, enable_perf_event, total_wait_desc,
        sql::EXECUTE_PS_PREPARE);
  }

  session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  return ret;
}

int ObFlightSqlQueryProcessor::execute_prepared_update(
    ObFlightSqlSession &session,
    const common::ObPsStmtId client_stmt_id,
    const sql::stmt::StmtType stmt_type,
    const arrow::RecordBatch &parameter_batch,
    const int64_t parameter_row_idx,
    common::ObIAllocator &parameter_allocator,
    const common::ObString &sql,
    const bool sensitive_sql,
    const char *audit_request_name,
    int64_t &failed_column,
    const ObFlightSqlRequestCancellation *cancellation)
{
  int ret = OB_SUCCESS;
  UNUSED(audit_request_name);
  failed_column = -1;
  arrow_schema_.reset();
  batches_.clear();
  trace_id_str_.clear();
  error_detail_.clear();
  plan_id_ = 0;
  sql_id_str_.clear();
  is_hit_plan_cache_ = false;
  plan_type_ = sql::OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_ = sql::stmt::T_NONE;
  affected_rows_ = 0;
  execute_time_us_ = 0;

  if (OB_ISNULL(session.ob_session) || common::OB_INVALID_ID == client_stmt_id
      || !flight_sql_is_supported_prepared_dml(stmt_type)
      || parameter_row_idx < 0 || parameter_row_idx >= parameter_batch.num_rows()
      || !parameter_batch.schema()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] execute_prepared_update: invalid argument",
             K(ret), K(client_stmt_id), K(stmt_type), K(parameter_row_idx),
             "parameter_rows", parameter_batch.num_rows(),
             KP(parameter_batch.schema().get()));
    return ret;
  } else if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
             || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] execute_prepared_update: server not initialized", K(ret));
    return ret;
  }

  // One DoPut can carry many parameter rows, but every native stmt_execute()
  // is an independent SQL execution. Give each one its own trace, matching
  // MySQL/JDBC executemany, and restore the outer DoPut trace on return.
  common::ObCurTraceId::TraceId stmt_trace_id;
  stmt_trace_id.init(GCTX.self_addr());
  common::ObTraceIdGuard stmt_trace_guard(stmt_trace_id);
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] execute_prepared_update: switch_to failed",
             K(ret), "tenant_id", static_cast<int64_t>(session.tenant_id));
    return ret;
  }
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  }

  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          session.tenant_id, schema_guard))) {
    LOG_WARN("[FlightSQL] execute_prepared_update: get schema guard failed", K(ret));
    return ret;
  }

  sql::ObSqlCtx sql_ctx;
  sql_ctx.session_info_ = session.ob_session;
  sql_ctx.schema_guard_ = &schema_guard;
  sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx.is_flight_sql_ = true;
  sql_ctx.is_sensitive_ = sensitive_sql;
  sql_ctx.cur_sql_ = sql;
  sql_ctx.enable_sql_resource_manage_ = true;

  const common::ObString display_sql = sensitive_sql
      ? common::ObString(OB_MASKED_STR) : sql;
  ObFlightSqlPreparedStateScope state_scope(
      session, display_sql, obmysql::ObMySQLCmd::COM_STMT_EXECUTE);
  // QUERY_ACTIVE above can replace a query-killed state from cancellation
  // between parameter rows. Keep the request cancellation sticky across rows
  // and check after publishing the new native execution state.
  if (OB_NOT_NULL(cancellation) && cancellation->is_cancelled()) {
    return OB_CANCELED;
  }
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQLPS")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
    ObReqTimeGuard req_time_guard;
    sql::ObSQLMockSchemaGuard mock_schema_guard;
    common::ObArenaAllocator arena_alloc("FlightSQLPS", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         static_cast<int64_t>(session.tenant_id));
    char rs_buf[sizeof(sql::ObResultSet)] __attribute__((aligned(16)));
    auto *result_set = new (rs_buf) sql::ObResultSet(*session.ob_session, arena_alloc);
    ObResultSetPlacementGuard rs_guard(result_set);
    result_set->set_ps_protocol();
    if (OB_FAIL(result_set->init())) {
      LOG_WARN("[FlightSQL] execute_prepared_update: result_set init failed", K(ret));
      return ret;
    } else if (OB_FAIL(flight_sql_setup_task_exec_ctx_(
                   *result_set, schema_guard, session.tenant_id))) {
      LOG_WARN("[FlightSQL] execute_prepared_update: task context setup failed", K(ret));
      return ret;
    }

    sql::ObSQLSessionInfo &ob_session = *session.ob_session;
    sql::ObAuditRecordData &audit_record = ob_session.get_raw_audit_record();
    audit_record.try_cnt_++;
    ob_session.set_current_execution_id(GCTX.sql_engine_->get_execution_id());
    ob_session.reset_plsql_exec_time();
    ob_session.reset_plsql_compile_time();
    ob_session.set_stmt_type(sql::stmt::T_NONE);
    FlightSqlAuditTimeSource audit_time = flight_sql_make_audit_time_source_(ob_session);
    const bool enable_perf_event = lib::is_diagnose_info_enabled();
    common::ObWaitEventStat total_wait_desc;
    common::ObMaxWaitGuard max_wait_guard(
        enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : nullptr);
    common::ObTotalWaitGuard total_wait_guard(
        enable_perf_event ? &total_wait_desc : nullptr);
    if (enable_perf_event) {
      audit_record.exec_record_.record_start();
    }

    sql::ParamStore params((common::ObWrapperAllocator(parameter_allocator)));
    common::ObPsStmtId inner_stmt_id = common::OB_INVALID_ID;
    if (OB_FAIL(flight_sql_compile_in_resource_group_(
            session, execution_scope, sql_ctx, *result_set, sql, true,
            [&]() {
              int ret = OB_SUCCESS;
              params.reuse();
              failed_column = -1;
              // Rebind this borrowed Arrow row after a compile-only retry so
              // its collection subschemas belong to the fresh execution context.
              if (OB_FAIL(ObFlightSqlParamConverter::convert_row(
                      parameter_batch, parameter_row_idx, session,
                      result_set->get_exec_context(), parameter_allocator,
                      params, failed_column))) {
                LOG_WARN("[FlightSQL] execute_prepared_update: Arrow parameter binding failed",
                         K(ret), K(parameter_row_idx), K(failed_column));
              } else if (OB_FAIL(ob_session.get_inner_ps_stmt_id(
                             client_stmt_id, inner_stmt_id))) {
                LOG_WARN("[FlightSQL] execute_prepared_update: get inner statement ID failed",
                         K(ret), K(client_stmt_id));
              } else {
                ret = GCTX.sql_engine_->stmt_execute(
                    client_stmt_id, stmt_type, params, sql_ctx, *result_set,
                    false /* is_inner_sql */);
              }
              return ret;
            }))) {
      LOG_WARN("[FlightSQL] execute_prepared_update: stmt_execute failed",
               K(ret), K(client_stmt_id),
               "ob_trace_id", common::ObCurTraceId::get_trace_id_str());
    }
    audit_time.exec_start_ts_ = common::ObTimeUtility::current_time();
    stmt_type_ = result_set->get_stmt_type();

    bool open_attempted = false;
    if (OB_SUCC(ret) && result_set->is_with_rows()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] execute_prepared_update produced rows unexpectedly",
               K(ret), K(client_stmt_id), K(stmt_type_));
    } else if (OB_SUCC(ret)) {
      open_attempted = true;
      const int64_t open_start = common::ObTimeUtility::current_time();
      if (OB_FAIL(result_set->open())) {
        execute_time_us_ = common::ObTimeUtility::current_time() - open_start;
        LOG_WARN("[FlightSQL] execute_prepared_update: result open failed",
                 K(ret), K(client_stmt_id));
      } else {
        execute_time_us_ = common::ObTimeUtility::current_time() - open_start;
        affected_rows_ = result_set->get_affected_rows();
        const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
        if (OB_NOT_NULL(plan)) {
          plan_id_ = plan->get_plan_id();
          plan_type_ = plan->get_plan_type();
        }
      }
    }
    if (open_attempted) {
      const int close_ret = result_set->close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("[FlightSQL] execute_prepared_update: result close failed",
                 K(close_ret), K(ret), K(client_stmt_id));
        if (OB_SUCC(ret)) {
          ret = close_ret;
        }
      }
    }

    is_hit_plan_cache_ = sql_ctx.plan_cache_hit_;
    if ('\0' != sql_ctx.sql_id_[0]) {
      sql_id_str_ = sql_ctx.sql_id_;
    }
    audit_record.ps_stmt_id_ = client_stmt_id;
    audit_record.ps_inner_stmt_id_ = inner_stmt_id;
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, ob_session, sql_ctx, *result_set, ret, arena_alloc, display_sql,
        audit_request_name, audit_time, enable_perf_event, total_wait_desc,
        sql::EXECUTE_PS_EXECUTE);
  }

  session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  return ret;
}

int ObFlightSqlQueryProcessor::close_prepared_statement(
    ObFlightSqlSession &session,
    const common::ObPsStmtId client_stmt_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session.ob_session) || common::OB_INVALID_ID == client_stmt_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] close_prepared_statement: invalid argument",
             K(ret), K(client_stmt_id));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
      LOG_WARN("[FlightSQL] close_prepared_statement: switch_to failed",
               K(ret), "tenant_id", static_cast<int64_t>(session.tenant_id));
    } else if (session.tenant_id != session.ob_session->get_effective_tenant_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] close_prepared_statement: fixed session tenant changed",
               K(ret), K(session.tenant_id),
               "effective_tenant_id", session.ob_session->get_effective_tenant_id());
    } else {
      ObFlightSqlExecutionScope execution_scope(session);
      const int group_ret = execution_scope.bind_resource_group();
      if (OB_SUCCESS != group_ret) {
        // The fixed tenant has already been entered. Release native prepared
        // state even if cancellation or teardown prevents cgroup attachment.
        LOG_WARN("[FlightSQL] close_prepared_statement: resource group bind failed",
                 K(group_ret), K(client_stmt_id));
      }
      if (OB_FAIL(session.ob_session->close_ps_stmt(client_stmt_id))) {
        LOG_WARN("[FlightSQL] close_prepared_statement: native close failed",
                 K(ret), K(client_stmt_id));
      } else {
        ret = group_ret;
      }
    }
  }
  return ret;
}

// ============================================================
// execute_sql()
// ============================================================

int ObFlightSqlQueryProcessor::execute_sql(ObFlightSqlSession &session,
                                            const common::ObString &sql)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session.ob_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] execute_sql: ob_session is null", K(ret));
    return ret;
  }
  if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
      || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] execute_sql: server not yet initialized", K(ret));
    return ret;
  }

  ObSessionQueryGuard query_guard(session);
  if (!query_guard.acquired()) {
    ret = OB_EAGAIN;
    LOG_WARN("[FlightSQL] execute_sql: concurrent query on same session rejected — "
             "use separate sessions for parallel queries", K(ret));
    return ret;
  }

  common::ObCurTraceId::init(GCTX.self_addr());

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] execute_sql: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  const int64_t timeout_ts = start_request_timeout(session);
  lib::WorkerTimeoutGuard timeout_guard(timeout_ts);
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  }

  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session.tenant_id, schema_guard))) {
    LOG_WARN("[FlightSQL] execute_sql: get_tenant_schema_guard failed", K(ret));
    return ret;
  }

  sql::ObSqlCtx sql_ctx;
  sql_ctx.session_info_            = session.ob_session;
  sql_ctx.schema_guard_            = &schema_guard;
  sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx.is_flight_sql_           = true;
  sql_ctx.enable_sql_resource_manage_ = true;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
    ObReqTimeGuard req_time_guard;
    sql::ObSQLMockSchemaGuard mock_schema_guard;
    common::ObArenaAllocator arena_alloc("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         static_cast<int64_t>(session.tenant_id));
    char rs_buf[sizeof(sql::ObResultSet)] __attribute__((aligned(16)));
    auto *result_set = new (rs_buf) sql::ObResultSet(*session.ob_session, arena_alloc);
    ObResultSetPlacementGuard rs_guard(result_set);
    if (OB_FAIL(result_set->init())) {
      LOG_WARN("[FlightSQL] execute_sql: result_set->init failed", K(ret));
      return ret;
    }
  if (OB_FAIL(flight_sql_setup_task_exec_ctx_(*result_set, schema_guard, session.tenant_id))) {
    LOG_WARN("[FlightSQL] execute_sql: setup_task_exec_ctx failed", K(ret));
    return ret;
  }

  sql::ObSQLSessionInfo *const ob_sess = session.ob_session;
  sql::ObAuditRecordData &audit_rec = ob_sess->get_raw_audit_record();
  audit_rec.try_cnt_++;
  ob_sess->set_current_execution_id(GCTX.sql_engine_->get_execution_id());
  ob_sess->reset_plsql_exec_time();
  ob_sess->reset_plsql_compile_time();
  ob_sess->set_stmt_type(sql::stmt::T_NONE);

  FlightSqlAuditTimeSource ts_src;
  const int64_t qst0 = ob_sess->get_query_start_time();
  const int64_t t_recv = (qst0 > 0 ? qst0 : common::ObTimeUtility::current_time());
  ts_src.receive_ts_ = t_recv;
  ts_src.enqueue_ts_ = t_recv;
  ts_src.run_ts_ = t_recv;
  ts_src.process_ts_ = t_recv;
  ts_src.single_process_ts_ = t_recv;
  ts_src.exec_start_ts_ = t_recv;
  ts_src.exec_end_ts_ = 0;

  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  common::ObWaitEventStat total_wait_desc;
  common::ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_rec.exec_record_.max_wait_event_ : nullptr);
  common::ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : nullptr);
  if (enable_perf_event) {
    audit_rec.exec_record_.record_start();
  }

  if (OB_FAIL(flight_sql_stmt_query_in_resource_group_(
            session, execution_scope, sql, sql_ctx, *result_set))) {
    ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
    LOG_WARN("[FlightSQL] execute_sql: stmt_query failed", K(ret), "sql", sql.ptr());
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, nullptr, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }
  ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
  LOG_TRACE("[FlightSQL] execute_sql: stmt_query done (plan), opening result_set",
           "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
  if (OB_FAIL(result_set->open())) {
    const int open_err = ret;
    LOG_WARN("[FlightSQL] execute_sql: open failed (query not executed)",
             K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
    int close_ret = result_set->close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("[FlightSQL] execute_sql: result_set->close failed after open error",
               K(close_ret), K(open_err), "ob_trace_id", ObCurTraceId::get_trace_id_str());
    }
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, nullptr, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }
  LOG_TRACE("[FlightSQL] execute_sql: result_set open OK (execution started)",
           "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
  const int close_ret = result_set->close();
  if (OB_SUCCESS != close_ret) {
    LOG_WARN("[FlightSQL] execute_sql: result_set->close failed",
             K(close_ret), K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str());
    if (OB_SUCC(ret)) {
      ret = close_ret;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[FlightSQL] execute_sql: OK (executed and closed)", "sql", sql.ptr());
  }
  flight_sql_finalize_statement_audit_like_mpquery_(
      session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, nullptr, ts_src,
      enable_perf_event, total_wait_desc);
  }
  return ret;
}

// ============================================================
// open_query() — compile + open, transfer result_set ownership
// ============================================================

int ObFlightSqlQueryProcessor::open_query(ObFlightSqlSession &session,
                                          const common::ObString &sql,
                                          lib::MemoryContext &out_mem_ctx,
                                          ObFlightSqlQueryPtr<common::ObArenaAllocator> &out_arena,
                                          ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> &out_schema_guard,
                                          ObFlightSqlQueryPtr<sql::ObSqlCtx> &out_sql_ctx,
                                          ObFlightSqlQueryPtr<sql::ObResultSet> &out_result_set,
                                          ObFlightSqlQueryRetry &retry,
                                          const ObFlightSqlRequestCancellation *cancellation)
{
  int ret = OB_SUCCESS;
  do {
    retry.begin_attempt(session);
    if (OB_NOT_NULL(cancellation) && cancellation->is_cancelled()) {
      ret = OB_CANCELED;
    } else {
      ret = open_query_once(session, sql, out_mem_ctx, out_arena, out_schema_guard,
                            out_sql_ctx, out_result_set, retry);
    }
    retry.update_session(session);
  } while (OB_SUCCESS != ret && retry.need_retry());
  return ret;
}

int ObFlightSqlQueryProcessor::open_query_once(
    ObFlightSqlSession &session,
    const common::ObString &sql,
    lib::MemoryContext &out_mem_ctx,
    ObFlightSqlQueryPtr<common::ObArenaAllocator> &out_arena,
    ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> &out_schema_guard,
    ObFlightSqlQueryPtr<sql::ObSqlCtx> &out_sql_ctx,
    ObFlightSqlQueryPtr<sql::ObResultSet> &out_result_set,
    ObFlightSqlQueryRetry &retry)
{
  int ret = OB_SUCCESS;
  resource_group_id_ = 0;
  timeout_ts_ = INT64_MAX;

  if (OB_ISNULL(session.ob_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] open_query: ob_session is null", K(ret));
    return ret;
  }
  if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
      || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] open_query: server not yet initialized", K(ret));
    return ret;
  }

  arrow_schema_.reset();
  batches_.clear();
  trace_id_str_.clear();
  error_detail_.clear();
  plan_id_ = 0;
  sql_id_str_.clear();
  is_hit_plan_cache_ = false;
  plan_type_ = sql::OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_ = sql::stmt::T_NONE;
  common::ObCurTraceId::set(retry.trace_id());
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] open_query: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  lib::WorkerTimeoutGuard timeout_guard(retry.timeout_ts());
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    return ret;
  }

  const common::ObMemAttr query_mem_attr(session.tenant_id, "FlightSQLQuery");
  ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> schema_guard(
      OB_NEW(share::schema::ObSchemaGetterGuard, query_mem_attr));
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] open_query: OOM for ObSchemaGetterGuard", K(ret));
    return ret;
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 session.tenant_id, *schema_guard))) {
    LOG_WARN("[FlightSQL] open_query: get_tenant_schema_guard failed", K(ret));
    return ret;
  }

  ObFlightSqlQueryPtr<sql::ObSqlCtx> sql_ctx(
      OB_NEW(sql::ObSqlCtx, query_mem_attr));
  if (OB_ISNULL(sql_ctx)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] open_query: OOM for ObSqlCtx", K(ret));
    return ret;
  }
  sql_ctx->session_info_            = session.ob_session;
  sql_ctx->schema_guard_            = schema_guard.get();
  sql_ctx->disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx->is_flight_sql_           = true;
  sql_ctx->enable_sql_resource_manage_ = true;
  if (OB_FAIL(retry.set_schema_versions(session, *schema_guard, *sql_ctx))) {
    return ret;
  }

  ObFlightSqlQueryPtr<common::ObArenaAllocator> arena_alloc(
      OB_NEW(common::ObArenaAllocator, query_mem_attr,
             "FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE,
             static_cast<int64_t>(session.tenant_id)));
  if (OB_ISNULL(arena_alloc)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] open_query: OOM for ObArenaAllocator", K(ret));
    return ret;
  }
  ObFlightSqlQueryPtr<sql::ObResultSet> result_set(
      OB_NEW(sql::ObResultSet, query_mem_attr, *session.ob_session, *arena_alloc));
  if (OB_ISNULL(result_set)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] open_query: OOM for ObResultSet", K(ret));
    return ret;
  }
  if (OB_FAIL(result_set->init())) {
    LOG_WARN("[FlightSQL] open_query: result_set->init failed", K(ret));
    return ret;
  }
  if (OB_FAIL(flight_sql_setup_task_exec_ctx_(*result_set, *schema_guard, session.tenant_id))) {
    LOG_WARN("[FlightSQL] open_query: setup_task_exec_ctx failed", K(ret));
    return ret;
  }

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  lib::MemoryContext mem_ctx;
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_ctx, ctx_param))) {
    LOG_WARN("[FlightSQL] open_query: CREATE_CONTEXT failed", K(ret));
    return ret;
  }

  bool open_attempted = false;
  WITH_CONTEXT(mem_ctx) {
    session.ob_session->set_current_execution_id(GCTX.sql_engine_->get_execution_id());
    session.ob_session->reset_plsql_exec_time();
    session.ob_session->reset_plsql_compile_time();
    session.ob_session->set_stmt_type(sql::stmt::T_NONE);
    if (OB_FAIL(flight_sql_stmt_query_in_resource_group_(
            session, execution_scope, sql, *sql_ctx, *result_set))) {
      LOG_WARN("[FlightSQL] open_query: stmt_query failed",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
    }
    if (OB_SUCC(ret)) {
      stmt_type_ = result_set->get_stmt_type();
      is_hit_plan_cache_ = sql_ctx->plan_cache_hit_;
      const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
      if (OB_NOT_NULL(plan)) {
        plan_id_ = plan->get_plan_id();
        plan_type_ = plan->get_plan_type();
      }
      if (sql_ctx->sql_id_[0] != '\0') {
        sql_id_str_ = sql_ctx->sql_id_;
      }
    }
    if (OB_SUCC(ret)
        && !result_set->is_with_rows()
        && !is_flight_sql_doget_stmt(result_set->get_stmt_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] open_query: DoGet only supports query statements",
               K(ret), "stmt_type", result_set->get_stmt_type(),
               "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
    }
    if (OB_SUCC(ret)) {
      ret = flight_sql_validate_result_types_(
          result_set->get_field_columns(), result_set->get_physical_plan(),
          trace_id_str_, error_detail_);
    }
    if (OB_SUCC(ret)) {
      arrow_schema_ = build_schema(result_set->get_field_columns(),
                                   &result_set->get_exec_context(),
                                   schema_guard.get(), session.tenant_id);
      open_attempted = true;
      if (OB_FAIL(result_set->open())) {
        const int open_err = ret;
        LOG_WARN("[FlightSQL] open_query: open failed", K(open_err));
      }
      const sql::ObPhysicalPlanCtx *plan_ctx = result_set->get_exec_context().get_physical_plan_ctx();
      if (OB_NOT_NULL(plan_ctx)) {
        retry.set_plan_timeout(plan_ctx->get_timeout_timestamp());
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("[FlightSQL] open_query: OK",
               "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str());
    }
  }

  resource_group_id_ = THIS_WORKER.get_group_id();
  timeout_ts_ = THIS_WORKER.get_timeout_ts();
  if (OB_FAIL(ret)) {
    const int attempt_error = ret;
    int client_ret = ret;
    // WITH_CONTEXT only enters when ret is OB_SUCCESS.
    ret = OB_SUCCESS;
    WITH_CONTEXT(mem_ctx) {
      retry.check_retry(*sql_ctx, *result_set, client_ret);
      if (open_attempted) {
        // Decide retry before closing, as MySQL does. close(ret) can cancel a
        // retry if transaction cleanup reports an unrecoverable error.
        result_set->set_errcode(attempt_error);
        const int close_ret = result_set->close(client_ret);
        if (OB_SUCCESS != close_ret && close_ret != attempt_error) {
          LOG_WARN("[FlightSQL] open_query: cleanup after failed open failed",
                   K(close_ret), K(attempt_error), "ob_trace_id", trace_id_str_.c_str());
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = client_ret;
    }
    result_set.reset();
    sql_ctx.reset();
    schema_guard.reset();
    arena_alloc.reset();
    DESTROY_CONTEXT(mem_ctx);
    return ret;
  }

  out_mem_ctx      = mem_ctx;
  out_arena        = std::move(arena_alloc);
  out_schema_guard = std::move(schema_guard);
  out_sql_ctx      = std::move(sql_ctx);
  out_result_set   = std::move(result_set);

  if (OB_NOT_NULL(session.ob_session)) {
    session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  }
  return ret;
}

// ============================================================
// execute()
// ============================================================

int ObFlightSqlQueryProcessor::execute(ObFlightSqlSession &session,
                                       const common::ObString &sql,
                                       const char *audit_request_name,
                                       const bool query_already_locked,
                                       const ObFlightSqlRequestCancellation *cancellation,
                                       const ResultMode result_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(audit_request_name);
  arrow_schema_.reset();
  batches_.clear();
  trace_id_str_.clear();
  error_detail_.clear();
  plan_id_          = 0;
  sql_id_str_.clear();
  is_hit_plan_cache_ = false;
  plan_type_         = sql::OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_         = sql::stmt::T_NONE;
  affected_rows_     = 0;
  execute_time_us_   = 0;
  arrow_mem_pool_.init(session.tenant_id);

  // Guard against partial server initialization: all engine components must be
  // ready before we can safely run SQL.  During startup the FlightSQL port may
  // be accepting connections before these pointers are wired up.
  if (OB_ISNULL(session.ob_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] execute: ob_session is null", K(ret));
    return ret;
  }
  if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.session_mgr_)
      || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FlightSQL] execute: server not yet initialized",
             K(ret), "sql_engine", GCTX.sql_engine_,
             "session_mgr", GCTX.session_mgr_,
             "schema_service", GCTX.schema_service_);
    return ret;
  }

  ObSessionQueryGuard query_guard(session, query_already_locked);
  if (!query_guard.acquired()) {
    ret = OB_EAGAIN;
    LOG_WARN("[FlightSQL] execute: concurrent query on same session rejected — "
             "use separate sessions for parallel queries", K(ret));
    return ret;
  }

  common::ObCurTraceId::init(GCTX.self_addr());
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] QueryProcessor::execute: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  const int64_t timeout_ts = start_request_timeout(session);
  lib::WorkerTimeoutGuard timeout_guard(timeout_ts);
  ObFlightSqlExecutionScope execution_scope(session);
  if (OB_FAIL(execution_scope.refresh_session_state())) {
    return ret;
  }

  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session.tenant_id, schema_guard))) {
    LOG_WARN("[FlightSQL] QueryProcessor::execute: get_tenant_schema_guard failed", K(ret));
    return ret;
  }

  sql::ObSqlCtx sql_ctx;
  sql_ctx.session_info_            = session.ob_session;
  sql_ctx.schema_guard_            = &schema_guard;
  sql_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  sql_ctx.is_flight_sql_           = true;
  sql_ctx.enable_sql_resource_manage_ = true;

  // Update SHOW PROCESSLIST while this query runs: ACTIVE state + current SQL.
  // A RAII guard restores SLEEP + session tag on every exit path, including
  // early returns inside CREATE_WITH_TEMP_CONTEXT below.
  const std::string flight_session_tag_ = "/* flightsql */";
  {
    std::lock_guard<std::mutex> guard(session.mutex);
    if (session.closing_) {
      return OB_CANCELED;
    }
    session.ob_session->set_mysql_cmd(obmysql::COM_QUERY);
    (void)session.ob_session->set_session_state(sql::QUERY_ACTIVE);
    (void)session.ob_session->store_query_string(sql);
  }
  struct FlightExecStateGuard_ {
    ObFlightSqlSession *flight_;
    sql::ObSQLSessionInfo *s_;
    const std::string     &tag_;
    FlightExecStateGuard_(ObFlightSqlSession *flight,
                          sql::ObSQLSessionInfo *s,
                          const std::string &t)
        : flight_(flight), s_(s), tag_(t) {}
    ~FlightExecStateGuard_() {
      if (OB_NOT_NULL(flight_) && OB_NOT_NULL(s_)) {
        std::lock_guard<std::mutex> guard(flight_->mutex);
        if (flight_->closing_) {
          return;
        }
        s_->set_session_sleep();
        (void)s_->store_query_string(common::ObString(
            static_cast<int32_t>(tag_.size()), tag_.c_str()));
      }
    }
  } flight_exec_state_guard_(&session, session.ob_session, flight_session_tag_);

  if (OB_NOT_NULL(cancellation) && cancellation->is_cancelled()) {
    return OB_CANCELED;
  }

  // gRPC threads have no per-request memory context set up by the tenant scheduler,
  // so THIS_WORKER.get_sql_arena_allocator() falls back to the process-global ROOT_CONTEXT
  // arena, which is not thread-safe for concurrent queries.  Create an isolated context here
  // so that handle_text_query gets a per-request arena, matching normal tenant worker threads.
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
  ObReqTimeGuard req_time_guard;
  sql::ObSQLMockSchemaGuard mock_schema_guard;
  common::ObArenaAllocator arena_alloc("FlightSQL", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                       static_cast<int64_t>(session.tenant_id));
  char rs_buf[sizeof(sql::ObResultSet)] __attribute__((aligned(16)));
  auto *result_set = new (rs_buf) sql::ObResultSet(*session.ob_session, arena_alloc);
  ObResultSetPlacementGuard rs_guard(result_set);
  if (OB_FAIL(result_set->init())) {
    LOG_WARN("[FlightSQL] QueryProcessor::execute: result_set->init failed", K(ret));
    return ret;
  }
  if (OB_FAIL(flight_sql_setup_task_exec_ctx_(*result_set, schema_guard, session.tenant_id))) {
    LOG_WARN("[FlightSQL] QueryProcessor::execute: setup_task_exec_ctx failed", K(ret));
    return ret;
  }
  // Propagate Flight SQL stream registry to the exec context so PX workers can find it.
  if (OB_NOT_NULL(session.ob_session)
      && OB_NOT_NULL(session.ob_session->get_flight_sql_stream_registry())) {
    result_set->get_exec_context().set_flight_sql_stream_registry(
        session.ob_session->get_flight_sql_stream_registry());
  }

  sql::ObSQLSessionInfo *const ob_sess = session.ob_session;
  sql::ObAuditRecordData &audit_rec = ob_sess->get_raw_audit_record();
  audit_rec.try_cnt_++;
  ob_sess->set_current_execution_id(GCTX.sql_engine_->get_execution_id());
  ob_sess->reset_plsql_exec_time();
  ob_sess->reset_plsql_compile_time();
  ob_sess->set_stmt_type(sql::stmt::T_NONE);

  FlightSqlAuditTimeSource ts_src;
  const int64_t qst0 = ob_sess->get_query_start_time();
  const int64_t t_recv = (qst0 > 0 ? qst0 : common::ObTimeUtility::current_time());
  ts_src.receive_ts_ = t_recv;
  ts_src.enqueue_ts_ = t_recv;
  ts_src.run_ts_ = t_recv;
  ts_src.process_ts_ = t_recv;
  ts_src.single_process_ts_ = t_recv;
  ts_src.exec_start_ts_ = t_recv;
  ts_src.exec_end_ts_ = 0;

  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  common::ObWaitEventStat total_wait_desc;
  common::ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_rec.exec_record_.max_wait_event_ : nullptr);
  common::ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : nullptr);
  if (enable_perf_event) {
    audit_rec.exec_record_.record_start();
  }

  if (OB_FAIL(flight_sql_stmt_query_in_resource_group_(
            session, execution_scope, sql, sql_ctx, *result_set))) {
    ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
    LOG_WARN("[FlightSQL] QueryProcessor::execute: stmt_query failed",
             K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
             "sql", sql.ptr());
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, audit_request_name, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }
  ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
  LOG_TRACE("[FlightSQL] QueryProcessor::execute: stmt_query OK (plan only, not executed yet)",
           "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());

  if (ResultMode::MATERIALIZE == result_mode
      && OB_FAIL(flight_sql_validate_result_types_(
          result_set->get_field_columns(), result_set->get_physical_plan(),
          trace_id_str_, error_detail_))) {
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, audit_request_name, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }

  if (ResultMode::MATERIALIZE == result_mode) {
    arrow_schema_ = build_schema(result_set->get_field_columns(),
                                 &result_set->get_exec_context(),
                                 &schema_guard, session.tenant_id);
  }

  // Capture fields available immediately after plan resolution.
  if (sql_ctx.sql_id_[0] != '\0') {
    sql_id_str_ = sql_ctx.sql_id_;
  }
  is_hit_plan_cache_ = sql_ctx.plan_cache_hit_;
  stmt_type_         = result_set->get_stmt_type();

  const int64_t open_start_us = common::ObTimeUtility::current_time();
  if (OB_FAIL(result_set->open())) {
    execute_time_us_ = common::ObTimeUtility::current_time() - open_start_us;
    const int open_err = ret;
    LOG_WARN("[FlightSQL] QueryProcessor::execute: result_set->open failed",
             K(open_err), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
             "sql", sql.ptr(), "sql_id", sql_id_str_.c_str());
    int close_ret = result_set->close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("[FlightSQL] QueryProcessor::execute: result_set->close failed after open error",
               K(close_ret), K(open_err), "ob_trace_id", ObCurTraceId::get_trace_id_str());
    }
  } else {
    execute_time_us_ = common::ObTimeUtility::current_time() - open_start_us;
    const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
    if (OB_NOT_NULL(plan)) {
      plan_id_   = plan->get_plan_id();
      plan_type_ = plan->get_plan_type();
    }
    LOG_TRACE("[FlightSQL] QueryProcessor::execute: result_set->open OK (execution started)",
             "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr(),
             "sql_id", sql_id_str_.c_str(), "plan_id", plan_id_);

    if (!result_set->is_with_rows()) {
      // Commands and non-row plans complete in open().  Match the native SQL
      // drivers: only result sets that advertise rows may be fetched.
      LOG_TRACE("[FlightSQL] QueryProcessor::execute: non-row result, skip row fetch",
               "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
    } else if (ResultMode::DISCARD == result_mode) {
      const common::ObNewRow *row = nullptr;
      int iter_ret = OB_SUCCESS;
      int64_t total_rows_drained = 0;
      while (OB_SUCCESS == (iter_ret = result_set->get_next_row(row))) {
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] QueryProcessor::execute: get_next_row returned null row",
                   K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
                   "sql", sql.ptr(), K(total_rows_drained));
          break;
        }
        ++total_rows_drained;
      }
      if (OB_SUCC(ret) && OB_ITER_END != iter_ret) {
        ret = iter_ret;
        LOG_WARN("[FlightSQL] QueryProcessor::execute: drain result rows failed",
                 K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
                 "sql", sql.ptr(), K(total_rows_drained));
      } else if (OB_SUCC(ret)) {
        LOG_TRACE("[FlightSQL] QueryProcessor::execute: result rows drained",
                  "ob_trace_id", ObCurTraceId::get_trace_id_str(),
                  "sql", sql.ptr(), K(total_rows_drained));
      }
    } else if (OB_ISNULL(arrow_schema_) || session.arrow_batch_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid result schema or Arrow batch size", K(ret),
               "batch_size", session.arrow_batch_size);
    } else {
      // Build one Arrow ArrayBuilder per column.
      const int64_t ncols      = arrow_schema_->num_fields();
      const int64_t batch_rows = session.arrow_batch_size;
      std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(ncols);
      bool builders_ok = true;
      for (int64_t c = 0; c < ncols && builders_ok; c++) {
        const std::shared_ptr<arrow::Field> &field = arrow_schema_->field(static_cast<int>(c));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        if (OB_ISNULL(field) || OB_ISNULL(field->type())) {
          ret = OB_ERR_UNEXPECTED;
          builders_ok = false;
          LOG_WARN("[FlightSQL] null result field or type", K(ret), "col", c);
          break;
        }
        const arrow::Status bld_status = arrow::MakeBuilder(
            &arrow_mem_pool_, field->type(), &builder);
        if (!bld_status.ok()) {
          ret = sql::arrow_status_to_ob_ret(bld_status);
          LOG_WARN("[FlightSQL] QueryProcessor::execute: MakeBuilder failed",
                   K(ret), "col", c, "st", bld_status.ToString().c_str());
          builders_ok = false;
        } else if (OB_ISNULL(builder)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] MakeBuilder returned null builder", K(ret), "col", c);
          builders_ok = false;
        } else {
          builders[c] = std::move(builder);
        }
      }

      int64_t total_rows_fetched = 0;
      if (builders_ok) {
        for (int64_t c = 0; c < ncols; c++) {
          const std::shared_ptr<arrow::Field> &field = arrow_schema_->field(static_cast<int>(c));
          const std::string arrow_type_str = field->type()->ToString();
          LOG_DEBUG("[FlightSQL] result column schema",
                   "col_idx", c,
                   "col_name", field->name().c_str(),
                   "arrow_type", arrow_type_str.c_str());
        }
        const common::ObNewRow *row = nullptr;
        int64_t row_count = 0;
        // If get_next_row returns e.g. OB_TIMEOUT, we must surface ret != OB_SUCCESS;
        // otherwise DoGetStatement streams partial batches as a successful query.
        int fetch_err_ret = OB_SUCCESS;
        const auto finish_batch = [&]() -> int {
          int finish_ret = OB_SUCCESS;
          std::vector<std::shared_ptr<arrow::Array>> arrays(ncols);
          for (int64_t c = 0; OB_SUCC(finish_ret) && c < ncols; c++) {
            if (OB_ISNULL(builders[c])) {
              finish_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("[FlightSQL] null result builder while finishing batch", K(finish_ret), K(c));
            } else {
              const arrow::Status finish_status = builders[c]->Finish(&arrays[c]);
              finish_ret = sql::arrow_status_to_ob_ret(finish_status);
              if (OB_SUCCESS != finish_ret) {
                LOG_WARN("[FlightSQL] finish result array failed", K(finish_ret), K(c),
                         "status", finish_status.ToString().c_str());
              } else if (OB_ISNULL(arrays[c]) || arrays[c]->length() != row_count) {
                finish_ret = OB_ERR_UNEXPECTED;
                LOG_WARN("[FlightSQL] invalid result array length", K(finish_ret), K(c), K(row_count));
              }
            }
          }
          if (OB_SUCC(finish_ret)) {
            batches_.push_back(arrow::RecordBatch::Make(arrow_schema_, row_count, std::move(arrays)));
            total_rows_fetched += row_count;
            row_count = 0;
          }
          return finish_ret;
        };

        while (true) {
          const int iter_ret = result_set->get_next_row(row);
          if (iter_ret == OB_ITER_END) { break; }
          if (iter_ret != OB_SUCCESS) {
            LOG_WARN("[FlightSQL] QueryProcessor::execute: get_next_row failed",
                     "iter_ret", iter_ret, "ob_trace_id", ObCurTraceId::get_trace_id_str(),
                     "sql", sql.ptr(), "rows_fetched_so_far", total_rows_fetched + row_count);
            fetch_err_ret = iter_ret;
            break;
          }
          if (OB_ISNULL(row)) {
            LOG_WARN("[FlightSQL] QueryProcessor::execute: get_next_row returned null row",
                     "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr(),
                     "rows_fetched_so_far", total_rows_fetched + row_count);
            fetch_err_ret = OB_ERR_UNEXPECTED;
            break;
          }

          const int64_t cell_count = row->get_count();
          for (int64_t c = 0; OB_SUCCESS == fetch_err_ret && c < ncols; c++) {
            const char *col_name = arrow_schema_->field(static_cast<int>(c))->name().c_str();
            if (OB_ISNULL(builders[c])) {
              fetch_err_ret = OB_ERR_UNEXPECTED;
            } else if (c < cell_count) {
              fetch_err_ret = append_obj_to_builder_(
                  builders[c].get(), row->get_cell(c),
                  static_cast<int64_t>(session.tenant_id), c, col_name);
            } else {
              LOG_DEBUG("[FlightSQL] append_obj_to_builder_: missing row cell, emit null",
                       "col_idx", c, "col_name", col_name,
                       "cell_count", cell_count, "ncols", ncols);
              fetch_err_ret = sql::arrow_status_to_ob_ret(builders[c]->AppendNull());
            }
          }
          if (OB_SUCCESS != fetch_err_ret) {
            LOG_WARN("[FlightSQL] convert result row failed", K(fetch_err_ret),
                     "rows_fetched_so_far", total_rows_fetched + row_count);
            break;
          }
          row_count++;

          if (row_count >= batch_rows) {
            fetch_err_ret = finish_batch();
            if (OB_SUCCESS != fetch_err_ret) {
              break;
            }
          }
        }

        // Flush remaining rows.
        if (OB_SUCCESS == fetch_err_ret && row_count > 0) {
          fetch_err_ret = finish_batch();
        }
        if (OB_SUCCESS != fetch_err_ret) {
          ret = fetch_err_ret;
        }
      } else {
        LOG_WARN("[FlightSQL] QueryProcessor::execute: Arrow builders init failed; "
                 "fetch skipped despite open OK",
                 "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
      }
    }

    affected_rows_ = result_set->get_affected_rows();
    const int close_ret = result_set->close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("[FlightSQL] QueryProcessor::execute: result_set->close failed",
               K(close_ret), K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str());
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
    if (OB_SUCCESS == ret) {
      LOG_TRACE("[FlightSQL] QueryProcessor::execute: OK (executed, result_set closed)",
               "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str(),
               "sql", sql.ptr(), "batches", (int64_t)batches_.size());
    } else {
      LOG_WARN("[FlightSQL] QueryProcessor::execute: finished with error during fetch or close",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str(),
               "sql", sql.ptr(), "batches", (int64_t)batches_.size());
    }
  }

  flight_sql_finalize_statement_audit_like_mpquery_(
      session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, audit_request_name, ts_src,
      enable_perf_event, total_wait_desc);
  } // CREATE_WITH_TEMP_CONTEXT

  if (OB_NOT_NULL(session.ob_session)) {
    session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

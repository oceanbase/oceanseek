/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_query_processor.h"

#include "lib/charset/ob_charset.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/worker.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_result_set.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_subschema_ctx.h"
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

// Binds a Flight SQL session to the current worker thread for the duration of
// a query, mirroring what obmp_query.cpp does: THIS_WORKER.set_session(sess)
// at the start, THIS_WORKER.set_session(NULL) at the end.
// Unlike ObInnerSQLSessionGuard (which saves/restores for nested inner SQL),
// this always clears to nullptr because gRPC threads have no prior session.
struct ObFlightSqlWorkerSessionScope {
  explicit ObFlightSqlWorkerSessionScope(sql::ObSQLSessionInfo *s) { THIS_WORKER.set_session(s); }
  ~ObFlightSqlWorkerSessionScope() { THIS_WORKER.set_session(nullptr); }
private:
  ObFlightSqlWorkerSessionScope(const ObFlightSqlWorkerSessionScope &) = delete;
  ObFlightSqlWorkerSessionScope &operator=(const ObFlightSqlWorkerSessionScope &) = delete;
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
static void flight_sql_append_scaled_decimal_int_to_arrow_(
    ArrowBuilderType *builder,
    const common::ObDecimalInt *decint,
    const int32_t int_bytes,
    const common::ObObj &obj,
    const int64_t col_idx,
    const char *col_name)
{
  const int32_t arrow_bytes = static_cast<int32_t>(ArrowDecimalType::kByteWidth);
  common::ObDecimalIntBuilder extended;
  extended.from(decint, int_bytes);
  if (extended.get_int_bytes() < arrow_bytes) {
    extended.extend(arrow_bytes);
  } else if (extended.get_int_bytes() > arrow_bytes) {
    flight_sql_log_append_null_("decimal_int_bytes_exceeds_arrow_width", obj, builder, col_idx,
                                col_name, nullptr, -1, -1, -1,
                                "scaled decimal int is wider than Arrow decimal type");
    (void)builder->AppendNull();
    return;
  }
  const int32_t final_bytes = extended.get_int_bytes();
  const uint8_t *buffer = reinterpret_cast<const uint8_t *>(extended.get_decimal_int());
  uint8_t buf[ArrowDecimalType::kByteWidth] = {0};
  for (int32_t i = 0; i < final_bytes; ++i) {
    buf[arrow_bytes - 1 - i] = buffer[i];
  }
  const arrow::Result<ArrowDecimalType> dec_res =
      ArrowDecimalType::FromBigEndian(buf, arrow_bytes);
  if (!dec_res.ok()) {
    flight_sql_log_append_null_from_status_("decimal_from_big_endian_failed", obj, builder,
                                            col_idx, col_name, dec_res.status());
    (void)builder->AppendNull();
    return;
  }
  const arrow::Status append_st = builder->Append(dec_res.ValueOrDie());
  if (!append_st.ok()) {
    flight_sql_log_append_null_from_status_("decimal_append_failed", obj, builder, col_idx,
                                            col_name, append_st);
    (void)builder->AppendNull();
  }
}

static void flight_sql_append_ob_decimal_to_builder_(
    arrow::ArrayBuilder *builder,
    const common::ObObj &obj,
    const int64_t col_idx,
    const char *col_name)
{
  using namespace common;
  int32_t tgt_prec = -1;
  int32_t tgt_scale = -1;
  int arrow_type_id = -1;
  flight_sql_get_arrow_decimal_meta_(builder, tgt_prec, tgt_scale, arrow_type_id);
  if (tgt_prec <= 0 || tgt_scale < 0) {
    flight_sql_log_append_null_("decimal_arrow_meta_invalid", obj, builder, col_idx, col_name);
    (void)builder->AppendNull();
    return;
  }

  ObDecimalIntBuilder work;
  ObDecimalIntBuilder scaled;
  const ObDecimalInt *src_decint = nullptr;
  int32_t src_int_bytes = 0;
  ObScale src_scale = 0;
  int ret = OB_SUCCESS;

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
      (void)builder->AppendNull();
      return;
    }
    src_decint = tmp_decint;
    src_int_bytes = tmp_int_bytes;
  } else if (ObDecimalIntType == obj.get_type()) {
    src_decint = obj.get_decimal_int();
    src_int_bytes = obj.get_int_bytes();
    src_scale = obj.get_scale();
    if (OB_ISNULL(src_decint)) {
      flight_sql_log_append_null_("decimal_int_ptr_null", obj, builder, col_idx, col_name);
      (void)builder->AppendNull();
      return;
    }
  } else {
    flight_sql_log_append_null_("not_decimal_ob_type", obj, builder, col_idx, col_name);
    (void)builder->AppendNull();
    return;
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
      (void)builder->AppendNull();
      return;
    }
    out_decint = scaled.get_decimal_int();
    out_bytes = scaled.get_int_bytes();
  }

  if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
    flight_sql_append_scaled_decimal_int_to_arrow_<arrow::Decimal128, arrow::Decimal128Builder>(
        static_cast<arrow::Decimal128Builder *>(builder), out_decint, out_bytes, obj, col_idx,
        col_name);
  } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
    flight_sql_append_scaled_decimal_int_to_arrow_<arrow::Decimal256, arrow::Decimal256Builder>(
        static_cast<arrow::Decimal256Builder *>(builder), out_decint, out_bytes, obj, col_idx,
        col_name);
  } else {
    flight_sql_log_append_null_("unexpected_arrow_decimal_builder_type", obj, builder, col_idx,
                                col_name);
    (void)builder->AppendNull();
  }
}
} // namespace

// ============================================================
// OB → Arrow cell serialisation
// ============================================================

static void append_obj_to_builder_(arrow::ArrayBuilder *builder,
                                   const common::ObObj &obj,
                                   const int64_t tenant_id,
                                   const int64_t col_idx = -1,
                                   const char *col_name = nullptr)
{
  int ret = OB_SUCCESS;
  using namespace common;
  if (obj.is_null()) {
    LOG_DEBUG("[FlightSQL] append_obj_to_builder_: SQL null cell",
             "col_idx", col_idx,
             "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"),
             "ob_type", static_cast<int>(obj.get_type()));
    (void)builder->AppendNull();
    return;
  }
  switch (obj.get_type()) {
    case ObTinyIntType:
      (void)static_cast<arrow::Int8Builder *>(builder)->Append(
          static_cast<int8_t>(obj.get_tinyint()));
      break;
    case ObSmallIntType:
      (void)static_cast<arrow::Int16Builder *>(builder)->Append(
          static_cast<int16_t>(obj.get_smallint()));
      break;
    case ObInt32Type:
      (void)static_cast<arrow::Int32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_int32()));
      break;
    case ObIntType:
      (void)static_cast<arrow::Int64Builder *>(builder)->Append(
          static_cast<int64_t>(obj.get_int()));
      break;
    case ObUTinyIntType:
      (void)static_cast<arrow::UInt8Builder *>(builder)->Append(
          static_cast<uint8_t>(obj.get_utinyint()));
      break;
    case ObUSmallIntType:
      (void)static_cast<arrow::UInt16Builder *>(builder)->Append(
          static_cast<uint16_t>(obj.get_usmallint()));
      break;
    case ObUInt32Type:
      (void)static_cast<arrow::UInt32Builder *>(builder)->Append(
          static_cast<uint32_t>(obj.get_uint32()));
      break;
    case ObUInt64Type:
      (void)static_cast<arrow::UInt64Builder *>(builder)->Append(
          static_cast<uint64_t>(obj.get_uint64()));
      break;
    case ObFloatType:
    case ObUFloatType:
      (void)static_cast<arrow::FloatBuilder *>(builder)->Append(obj.get_float());
      break;
    case ObDoubleType:
    case ObUDoubleType:
      (void)static_cast<arrow::DoubleBuilder *>(builder)->Append(obj.get_double());
      break;
    case ObNumberType:
    case ObUNumberType:
    case ObDecimalIntType:
      flight_sql_append_ob_decimal_to_builder_(builder, obj, col_idx, col_name);
      break;
    case ObDateTimeType:
    case ObTimestampType:
      (void)static_cast<arrow::TimestampBuilder *>(builder)->Append(obj.get_datetime());
      break;
    case ObMySQLDateTimeType: {
      const common::ObMySQLDateTime mdt = obj.get_mysql_datetime();
      int64_t dt_value = 0;
      if (OB_SUCCESS != common::ObTimeConverter::mdatetime_to_datetime(mdt, dt_value, 0)) {
        (void)builder->AppendNull();
      } else {
        (void)static_cast<arrow::TimestampBuilder *>(builder)->Append(dt_value);
      }
      break;
    }
    case ObDateType:
      (void)static_cast<arrow::Date32Builder *>(builder)->Append(
          static_cast<int32_t>(obj.get_date()));
      break;
    case ObMySQLDateType: {
      const common::ObMySQLDate md = obj.get_mysql_date();
      int32_t d_value = 0;
      if (OB_SUCCESS != common::ObTimeConverter::mdate_to_date(md, d_value, 0)) {
        (void)builder->AppendNull();
      } else {
        (void)static_cast<arrow::Date32Builder *>(builder)->Append(d_value);
      }
      break;
    }
    case ObTimeType:
      (void)static_cast<arrow::DurationBuilder *>(builder)->Append(obj.get_time());
      break;
    case ObYearType: {
      const uint8_t raw = obj.get_year();
      (void)static_cast<arrow::Int16Builder *>(builder)->Append(
          static_cast<int16_t>(raw == 0 ? 0 : 1900 + raw));
      break;
    }
    case ObBitType:
      (void)static_cast<arrow::UInt64Builder *>(builder)->Append(obj.get_bit());
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
        if (!lob->is_mem_loc_ && lob->in_row_) {
          // inrow disk LOB: skip header with pointer arithmetic, zero allocation
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
          s.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          // outrow or mem LOB: materialize via allocator
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, s)) {
            (void)builder->AppendNull();
            break;
          }
          if (OB_UNLIKELY(s.length() < 0)) {
            LOG_WARN("[FlightSQL] text LOB length overflowed int32 after read, returning NULL",
                     "length", s.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
        }
      } else {
        s = obj.get_string();
      }
      if (builder->type()->id() == arrow::Type::LARGE_BINARY) {
        if (OB_NOT_NULL(s.ptr())) {
          (void)static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
              reinterpret_cast<const uint8_t *>(s.ptr()),
              static_cast<int64_t>(s.length()));
        } else {
          (void)builder->AppendNull();
        }
      } else {
        (void)static_cast<arrow::StringBuilder *>(builder)->Append(
            s.ptr(), static_cast<int32_t>(s.length()));
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
          (void)builder->AppendNull();
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] JSON inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
          raw.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, raw)) {
            (void)builder->AppendNull();
            break;
          }
          if (OB_UNLIKELY(raw.length() < 0)) {
            LOG_WARN("[FlightSQL] JSON LOB length overflowed int32 after read, returning NULL",
                     "length", raw.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
        }
      } else {
        raw = obj.get_string();
      }
      if (OB_ISNULL(raw.ptr()) || raw.length() == 0) {
        (void)builder->AppendNull();
        break;
      }
      common::ObJsonBin j_bin(raw.ptr(), raw.length(), &arena);
      common::ObJsonBuffer jbuf(&arena);
      j_bin.set_seek_flag(true);
      if (OB_SUCCESS != j_bin.reset_iter()
          || OB_SUCCESS != j_bin.print(jbuf, /*is_quoted=*/true, raw.length())
          || jbuf.empty()) {
        (void)builder->AppendNull();
      } else {
        const int64_t jbuf_len = static_cast<int64_t>(jbuf.length());
        if (OB_UNLIKELY(jbuf_len < 0 || jbuf_len > INT32_MAX)) {
          LOG_WARN("[FlightSQL] serialized JSON length out of int32 range, returning NULL",
                   "jbuf_len", jbuf_len, "col_idx", col_idx,
                   "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
          (void)builder->AppendNull();
        } else {
          (void)static_cast<arrow::StringBuilder *>(builder)->Append(
              jbuf.ptr(), static_cast<int32_t>(jbuf_len));
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
          (void)builder->AppendNull();
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] LOB inrow byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
          s.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, s)) {
            (void)builder->AppendNull();
            break;
          }
          if (OB_UNLIKELY(s.length() < 0)) {
            LOG_WARN("[FlightSQL] LOB length overflowed int32 after read, returning NULL",
                     "length", s.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
        }
      } else {
        s = obj.get_string();
      }
      if (OB_NOT_NULL(s.ptr())) {
        (void)static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
            reinterpret_cast<const uint8_t *>(s.ptr()),
            static_cast<int64_t>(s.length()));
      } else {
        (void)builder->AppendNull();
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
          (void)builder->AppendNull();
          break;
        }
        if (!lob->is_mem_loc_ && lob->in_row_) {
          const int64_t byte_sz = lob->get_byte_size(obj.get_string_len());
          if (OB_UNLIKELY(byte_sz < 0 || byte_sz > INT32_MAX)) {
            LOG_WARN("[FlightSQL] collection inrow LOB byte_size out of int32 range, returning NULL",
                     "byte_sz", byte_sz, "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
          raw.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(byte_sz));
        } else {
          if (OB_SUCCESS != sql::ObTextStringHelper::read_real_string_data(&arena, obj, raw)) {
            (void)builder->AppendNull();
            break;
          }
          if (OB_UNLIKELY(raw.length() < 0)) {
            LOG_WARN("[FlightSQL] collection LOB length overflowed int32 after read, returning NULL",
                     "length", raw.length(), "col_idx", col_idx,
                     "col_name", (OB_NOT_NULL(col_name) ? col_name : "(unknown)"));
            (void)builder->AppendNull();
            break;
          }
        }
      } else {
        raw = obj.get_string();
      }
      if (OB_ISNULL(raw.ptr())) {
        (void)builder->AppendNull();
        break;
      }
      // VECTOR (FIXED_SIZE_LIST): binary is raw packed elements, no header.
      //   Count comes from the schema dimension; element count = dim.
      // ARRAY (LIST): binary is [uint32_t N][uint8_t nulls[N]][T vals[N]].
      //   nulls[i] == 1 means element i is NULL.
#define FLIGHT_SQL_APPEND_FIXED_LIST_(CppType, ArrowBuilder, ArrowTypeId)                     \
      } else if (elem_tid == arrow::Type::ArrowTypeId) {                                       \
        if (raw.length() >= static_cast<int64_t>(dim * static_cast<int32_t>(sizeof(CppType)))) { \
          (void)list_bld->Append();                                                             \
          auto *eb = static_cast<arrow::ArrowBuilder *>(list_bld->value_builder());            \
          const CppType *vals = reinterpret_cast<const CppType *>(raw.ptr());                  \
          for (int32_t i = 0; i < dim; i++) { (void)eb->Append(vals[i]); }                    \
        } else { (void)builder->AppendNull(); }
#define FLIGHT_SQL_APPEND_VAR_LIST_(CppType, ArrowBuilder, ArrowTypeId)                        \
      } else if (elem_tid == arrow::Type::ArrowTypeId) {                                       \
        if (raw.length() < static_cast<int64_t>(sizeof(uint32_t))) {                          \
          (void)builder->AppendNull();                                                          \
        } else {                                                                                \
          const uint32_t arr_cnt = *reinterpret_cast<const uint32_t *>(raw.ptr());            \
          const char *body = raw.ptr() + sizeof(uint32_t);                                     \
          const int64_t body_len = raw.length() - static_cast<int64_t>(sizeof(uint32_t));     \
          const int64_t need = static_cast<int64_t>(arr_cnt) * (1 + static_cast<int64_t>(sizeof(CppType))); \
          if (body_len < need) {                                                                \
            (void)builder->AppendNull();                                                        \
          } else {                                                                              \
            (void)list_bld->Append();                                                           \
            auto *eb = static_cast<arrow::ArrowBuilder *>(list_bld->value_builder());         \
            const uint8_t *nulls = reinterpret_cast<const uint8_t *>(body);                   \
            const CppType *vals = reinterpret_cast<const CppType *>(body + arr_cnt);          \
            for (uint32_t i = 0; i < arr_cnt; i++) {                                          \
              if (nulls[i]) { (void)eb->AppendNull(); }                                        \
              else { (void)eb->Append(vals[i]); }                                              \
            }                                                                                   \
          }                                                                                     \
        }
      // Arrow 20: ListBuilder::type() creates a new shared_ptr on every call.
      // Hold it alive for the full if/else chain so raw pointers don't dangle.
      const ArrowDataTypePtr type_sptr = builder->type();
      const arrow::Type::type tid = type_sptr->id();
      if (tid == arrow::Type::FIXED_SIZE_LIST) {
        auto *list_bld = static_cast<arrow::FixedSizeListBuilder *>(builder);
        const auto *list_type =
            static_cast<const arrow::FixedSizeListType *>(type_sptr.get());
        const int32_t dim = list_type->list_size();
        const arrow::Type::type elem_tid = list_type->value_type()->id();
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
          (void)builder->AppendNull();
        }
      } else if (tid == arrow::Type::LIST) {
        auto *list_bld = static_cast<arrow::ListBuilder *>(builder);
        const auto *list_type = static_cast<const arrow::ListType *>(type_sptr.get());
        const arrow::Type::type elem_tid = list_type->value_type()->id();
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
          (void)builder->AppendNull();
        }
      } else {
        (void)static_cast<arrow::LargeBinaryBuilder *>(builder)->Append(
            reinterpret_cast<const uint8_t *>(raw.ptr()),
            static_cast<int64_t>(raw.length()));
      }
#undef FLIGHT_SQL_APPEND_FIXED_LIST_
#undef FLIGHT_SQL_APPEND_VAR_LIST_
      break;
    }
    default:
      flight_sql_log_append_null_("unsupported_ob_type", obj, builder, col_idx, col_name);
      (void)builder->AppendNull();
      break;
  }
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

// ============================================================
// GV$OB_SQL_AUDIT — same pipeline as obmp_query.cpp (mutate session audit_record_)
// QUERY_SQL: optional `-- flightsql: bearer=… catalog=…` line + newline (same rules as ob_flight_sql_audit.cpp)
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

static bool flight_sql_audit_token_printable_(const char *const s, const size_t len)
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
    const common::ObString &sql_plain,
    sql::ObSQLSessionInfo &ob_sess,
    common::ObIAllocator &allocator,
    common::ObString &out_audit_sql)
{
  int ret = common::OB_SUCCESS;
  std::string sql_full;
  std::string sid;
  std::string cat;
  {
    std::lock_guard<std::mutex> guard(flight_sess.mutex);
    sid = flight_sess.session_id;
    cat = flight_sess.current_catalog;
  }
  const size_t uuid_len = strnlen(sid.c_str(), sid.size());
  const bool uuid_ok = uuid_len > 0 && uuid_len == sid.size()
                       && flight_sql_audit_token_printable_(sid.c_str(), uuid_len);
  const size_t cat_len = strnlen(cat.c_str(), cat.size());
  const bool cat_ok = cat_len > 0 && cat_len == cat.size()
                      && flight_sql_audit_token_printable_(cat.c_str(), cat_len);
  if (uuid_ok || cat_ok) {
    sql_full.append("-- flightsql:");
    if (uuid_ok && cat_ok) {
      sql_full.append(" bearer=");
      sql_full.append(sid.c_str(), uuid_len);
      sql_full.append(" catalog=");
      sql_full.append(cat.c_str(), cat_len);
    } else if (uuid_ok) {
      sql_full.append(" bearer=");
      sql_full.append(sid.c_str(), uuid_len);
    } else {
      sql_full.append(" catalog=");
      sql_full.append(cat.c_str(), cat_len);
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
    FlightSqlAuditTimeSource &ts,
    const bool enable_perf_event,
    common::ObWaitEventStat &total_wait_desc)
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
        flight_sess, sql_plain, session, allocator, tagged_sql);
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

  (void)sql::ObSQLUtils::handle_audit_record(false, sql::EXECUTE_LOCAL, session, sql_ctx.is_sensitive_);
}

// ============================================================
// OB session setup
// ============================================================

void ObFlightSqlQueryProcessor::setup_worker_timeout(ObFlightSqlSession &session)
{
  if (OB_ISNULL(session.ob_session)) { return; }

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
  THIS_WORKER.set_timeout_ts(start_time + timeout_us);
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
    case ObInt32Type:      return arrow::int32();
    case ObIntType:        return arrow::int64();
    case ObUTinyIntType:   return arrow::uint8();
    case ObUSmallIntType:  return arrow::uint16();
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
    default:               return arrow::utf8();
  }
}

std::shared_ptr<arrow::DataType> ObFlightSqlQueryProcessor::collection_type_to_arrow_(
    const common::ObCollectionTypeBase *coll_type)
{
  using namespace common;
  if (OB_ISNULL(coll_type)) {
    return arrow::large_binary();
  }
  if (coll_type->type_id_ == OB_VECTOR_TYPE || coll_type->type_id_ == OB_ARRAY_TYPE) {
    const auto *arr_type = static_cast<const ObCollectionArrayType *>(coll_type);
    std::shared_ptr<arrow::DataType> elem_type = arrow::float32();
    if (OB_NOT_NULL(arr_type->element_type_)) {
      const auto *basic = static_cast<const ObCollectionBasicType *>(arr_type->element_type_);
      elem_type = ob_type_to_arrow(basic->basic_meta_.get_obj_type());
    }
    if (coll_type->type_id_ == OB_VECTOR_TYPE && arr_type->dim_cnt_ > 0) {
      return arrow::fixed_size_list(elem_type, static_cast<int32_t>(arr_type->dim_cnt_));
    } else {
      return arrow::list(elem_type);
    }
  }
  return arrow::large_binary();
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
                                       const PrepareMode mode)
{
  int ret = OB_SUCCESS;
  arrow_schema_.reset();
  trace_id_str_.clear();
  plan_id_ = 0;
  sql_id_str_.clear();

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

  ObSessionQueryGuard query_guard(session);
  if (!query_guard.acquired()) {
    ret = OB_EAGAIN;
    LOG_WARN("[FlightSQL] prepare: concurrent query on same session rejected — "
             "use separate sessions for parallel queries", K(ret));
    return ret;
  }

  common::ObCurTraceId::init(GCTX.self_addr());
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] QueryProcessor::prepare: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  setup_worker_timeout(session);
  ObFlightSqlWorkerSessionScope worker_session_scope(session.ob_session);

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

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
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
    if (OB_FAIL(GCTX.sql_engine_->stmt_query(sql, sql_ctx, *result_set))) {
      LOG_WARN("[FlightSQL] QueryProcessor::prepare: stmt_query failed",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "sql", sql.ptr());
      return ret;
    }
    const stmt::StmtType stmt_type = result_set->get_stmt_type();
    const bool is_with_rows = result_set->is_with_rows();
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
    }
    arrow_schema_ = build_schema(result_set->get_field_columns(),
                                 &result_set->get_exec_context(),
                                 &schema_guard, session.tenant_id);

    const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
    if (OB_NOT_NULL(plan)) {
      plan_id_ = plan->get_plan_id();
    }
    if (sql_ctx.sql_id_[0] != '\0') {
      sql_id_str_ = sql_ctx.sql_id_;
    }
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

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] execute_sql: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  setup_worker_timeout(session);
  ObFlightSqlWorkerSessionScope worker_session_scope(session.ob_session);

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

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
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

  if (OB_FAIL(GCTX.sql_engine_->stmt_query(sql, sql_ctx, *result_set))) {
    ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
    LOG_WARN("[FlightSQL] execute_sql: stmt_query failed", K(ret), "sql", sql.ptr());
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, ts_src,
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
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }
  LOG_TRACE("[FlightSQL] execute_sql: result_set open OK (execution started)",
           "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
  result_set->close();
  LOG_TRACE("[FlightSQL] execute_sql: OK (executed and closed)", "sql", sql.ptr());
  flight_sql_finalize_statement_audit_like_mpquery_(
      session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, ts_src,
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
                                          ObFlightSqlQueryPtr<sql::ObResultSet> &out_result_set)
{
  int ret = OB_SUCCESS;

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
  plan_id_ = 0;
  sql_id_str_.clear();
  common::ObCurTraceId::init(GCTX.self_addr());
  trace_id_str_ = common::ObCurTraceId::get_trace_id_str();

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(session.tenant_id))) {
    LOG_WARN("[FlightSQL] open_query: switch_to failed",
             K(ret), "tenant_id", (int64_t)session.tenant_id);
    return ret;
  }
  setup_worker_timeout(session);
  ObInnerSQLSessionGuard worker_session_guard(session.ob_session);

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

  WITH_CONTEXT(mem_ctx) {
    session.ob_session->set_current_execution_id(GCTX.sql_engine_->get_execution_id());
    session.ob_session->reset_plsql_exec_time();
    session.ob_session->reset_plsql_compile_time();
    session.ob_session->set_stmt_type(sql::stmt::T_NONE);
    if (OB_FAIL(GCTX.sql_engine_->stmt_query(sql, *sql_ctx, *result_set))) {
      LOG_WARN("[FlightSQL] open_query: stmt_query failed",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());
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
      arrow_schema_ = build_schema(result_set->get_field_columns(),
                                   &result_set->get_exec_context(),
                                   schema_guard.get(), session.tenant_id);
      if (sql_ctx->sql_id_[0] != '\0') { sql_id_str_ = sql_ctx->sql_id_; }
      if (OB_FAIL(result_set->open())) {
        const int open_err = ret;
        LOG_WARN("[FlightSQL] open_query: open failed", K(open_err));
      }
    }
    if (OB_SUCC(ret)) {
      const sql::ObPhysicalPlan *plan = result_set->get_physical_plan();
      if (OB_NOT_NULL(plan)) { plan_id_ = plan->get_plan_id(); }
      LOG_TRACE("[FlightSQL] open_query: OK",
               "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str());
    }
  }

  if (OB_FAIL(ret)) {
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
                                       const common::ObString &sql)
{
  int ret = OB_SUCCESS;
  arrow_schema_.reset();
  batches_.clear();
  trace_id_str_.clear();
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

  ObSessionQueryGuard query_guard(session);
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
  setup_worker_timeout(session);
  ObFlightSqlWorkerSessionScope worker_session_scope(session.ob_session);

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

  // Update SHOW PROCESSLIST while this query runs: ACTIVE state + current SQL.
  // A RAII guard restores SLEEP + session tag on every exit path, including
  // early returns inside CREATE_WITH_TEMP_CONTEXT below.
  const std::string flight_session_tag_ = "/* flight:" + session.session_id + " */";
  session.ob_session->set_mysql_cmd(obmysql::COM_QUERY);
  (void)session.ob_session->set_session_state(sql::QUERY_ACTIVE);
  (void)session.ob_session->store_query_string(sql);
  struct FlightExecStateGuard_ {
    sql::ObSQLSessionInfo *s_;
    const std::string     &tag_;
    FlightExecStateGuard_(sql::ObSQLSessionInfo *s, const std::string &t) : s_(s), tag_(t) {}
    ~FlightExecStateGuard_() {
      if (OB_NOT_NULL(s_)) {
        s_->set_session_sleep();
        (void)s_->store_query_string(common::ObString(
            static_cast<int32_t>(tag_.size()), tag_.c_str()));
      }
    }
  } flight_exec_state_guard_(session.ob_session, flight_session_tag_);

  // gRPC threads have no per-request memory context set up by the tenant scheduler,
  // so THIS_WORKER.get_sql_arena_allocator() falls back to the process-global ROOT_CONTEXT
  // arena, which is not thread-safe for concurrent queries.  Create an isolated context here
  // so that handle_text_query gets a per-request arena, matching normal tenant worker threads.
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(session.tenant_id, "FlightSQL")
           .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  CREATE_WITH_TEMP_CONTEXT(ctx_param) {
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

  if (OB_FAIL(GCTX.sql_engine_->stmt_query(sql, sql_ctx, *result_set))) {
    ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
    LOG_WARN("[FlightSQL] QueryProcessor::execute: stmt_query failed",
             K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
             "sql", sql.ptr());
    flight_sql_finalize_statement_audit_like_mpquery_(
        session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, ts_src,
        enable_perf_event, total_wait_desc);
    return ret;
  }
  ts_src.exec_start_ts_ = common::ObTimeUtility::current_time();
  LOG_TRACE("[FlightSQL] QueryProcessor::execute: stmt_query OK (plan only, not executed yet)",
           "ob_trace_id", ObCurTraceId::get_trace_id_str(), "sql", sql.ptr());

  arrow_schema_ = build_schema(result_set->get_field_columns(),
                               &result_set->get_exec_context(),
                               &schema_guard, session.tenant_id);

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
    } else {
      // Build one Arrow ArrayBuilder per column.
      const int64_t ncols      = arrow_schema_->num_fields();
      const int64_t batch_rows = session.arrow_batch_size;
      std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(ncols);
      bool builders_ok = true;
      for (int64_t c = 0; c < ncols && builders_ok; c++) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        const arrow::Status bld_status = arrow::MakeBuilder(
            &arrow_mem_pool_, arrow_schema_->field(c)->type(), &builder);
        if (!bld_status.ok()) {
          LOG_WARN("[FlightSQL] QueryProcessor::execute: MakeBuilder failed",
                   "col", (int64_t)c, "st", bld_status.ToString().c_str());
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
          for (int64_t c = 0; c < ncols; c++) {
            const char *col_name = arrow_schema_->field(static_cast<int>(c))->name().c_str();
            if (c < cell_count) {
              append_obj_to_builder_(builders[c].get(), row->get_cell(c),
                                     static_cast<int64_t>(session.tenant_id), c, col_name);
            } else {
              LOG_DEBUG("[FlightSQL] append_obj_to_builder_: missing row cell, emit null",
                       "col_idx", c, "col_name", col_name,
                       "cell_count", cell_count, "ncols", ncols);
              (void)builders[c]->AppendNull();
            }
          }
          row_count++;

          if (row_count >= batch_rows) {
            std::vector<std::shared_ptr<arrow::Array>> arrays(ncols);
            for (int64_t c = 0; c < ncols; c++) {
              (void)builders[c]->Finish().Value(&arrays[c]);
            }
            batches_.push_back(arrow::RecordBatch::Make(arrow_schema_, row_count, std::move(arrays)));
            total_rows_fetched += row_count;
            row_count = 0;
          }
        }

        // Flush remaining rows.
        if (row_count > 0) {
          std::vector<std::shared_ptr<arrow::Array>> arrays(ncols);
          for (int64_t c = 0; c < ncols; c++) {
            (void)builders[c]->Finish().Value(&arrays[c]);
          }
          batches_.push_back(arrow::RecordBatch::Make(arrow_schema_, row_count, std::move(arrays)));
          total_rows_fetched += row_count;
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
    result_set->close();
    if (OB_SUCCESS == ret) {
      LOG_TRACE("[FlightSQL] QueryProcessor::execute: OK (executed, result_set closed)",
               "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str(),
               "sql", sql.ptr(), "batches", (int64_t)batches_.size());
    } else {
      LOG_WARN("[FlightSQL] QueryProcessor::execute: finished with error after fetch",
               K(ret), "ob_trace_id", ObCurTraceId::get_trace_id_str(),
               "plan_id", plan_id_, "sql_id", sql_id_str_.c_str(),
               "sql", sql.ptr(), "batches", (int64_t)batches_.size());
    }
  }

  flight_sql_finalize_statement_audit_like_mpquery_(
      session, *ob_sess, sql_ctx, *result_set, ret, arena_alloc, sql, ts_src,
      enable_perf_event, total_wait_desc);
  } // CREATE_WITH_TEMP_CONTEXT

  if (OB_NOT_NULL(session.ob_session)) {
    session.ob_session->set_last_trace_id(common::ObCurTraceId::get_trace_id());
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

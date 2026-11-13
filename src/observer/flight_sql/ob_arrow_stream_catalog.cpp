/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "observer/flight_sql/ob_arrow_stream_catalog.h"
#include "observer/flight_sql/ob_flightsql_session.h"
#include "observer/flight_sql/ob_flightsql_stream_registry.h"
#include "observer/flight_sql/ob_flightsql_ingest_registry.h"
#include "observer/flight_sql/ob_flightsql_arrow_converter.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_server_struct.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_subschema_ctx.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/charset/ob_charset.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_sql_string.h"
#include "lib/udt/ob_collection_type.h"
#include <climits>
#include <arrow/type.h>

namespace oceanbase
{
using namespace common;
namespace observer
{

int ObArrowStreamCatalog::get_estimated_row_count(const sql::ObRawExpr *func_expr,
                                                   double &row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(func_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow function expression is null", K(ret));
  } else if (T_FUN_SYS_ARROW_STREAM == func_expr->get_expr_type()) {
    // V1 streams normally supply bulk ingest input.
    row_count = 1000000.0;
  } else if (T_FUN_SYS_ARROW_STREAMS == func_expr->get_expr_type()) {
    // Keep the existing estimate for V2/V3 streams.
    row_count = 199.0;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] unexpected Arrow function expression", K(ret), KPC(func_expr));
  }
  return ret;
}

static bool is_arrow_collection_field(const arrow::Field &arrow_field)
{
  const arrow::Type::type type_id = arrow_field.type()->id();
  return type_id == arrow::Type::LIST
      || type_id == arrow::Type::LARGE_LIST
      || type_id == arrow::Type::FIXED_SIZE_LIST
      || type_id == arrow::Type::MAP;
}

// Register the source collection carrier, not the destination column's
// collection type.  The resolver installs this subschema on arrow_stream and
// then performs its normal source-to-target collection compatibility check.
static int resolve_middle_collection_meta(const arrow::Field &arrow_field,
                                          const sql::ObSQLSessionInfo *session,
                                          uint16_t &subschema_id,
                                          const common::ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  subschema_id = UINT_MAX16;
  collection_meta = nullptr;
  sql::ObExecContext *exec_ctx = OB_NOT_NULL(session)
      ? const_cast<sql::ObSQLSessionInfo *>(session)->get_cur_exec_ctx() : nullptr;
  sql::ObSubSchemaValue sub_meta;
  ObSqlString type_string;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] no execution context for Arrow middle collection", K(ret));
  } else if (OB_FAIL(ob_flight_sql_get_middle_collection_type_string(
                 arrow_field, session->is_oracle_compatible(), type_string))) {
    LOG_WARN("[FlightSQL] failed to derive Arrow collection middle type", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (OB_UNLIKELY(type_string.empty()
                         || type_string.length() > INT32_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid Arrow collection middle type string", K(ret),
             "field_name", arrow_field.name().c_str(),
             "type_string_length", type_string.length());
  } else {
    const ObString type_str = type_string.string();
    if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(type_str, subschema_id))) {
      LOG_WARN("[FlightSQL] failed to register Arrow collection middle subschema", K(ret),
               K(type_str));
    } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, sub_meta))) {
      LOG_WARN("[FlightSQL] failed to resolve Arrow collection middle subschema", K(ret),
               K(subschema_id));
    } else if (sub_meta.type_ != sql::OB_SUBSCHEMA_COLLECTION_TYPE
               || OB_ISNULL(sub_meta.value_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid Arrow collection middle subschema", K(ret),
               K(subschema_id), K(sub_meta.type_), KP(sub_meta.value_));
    } else {
      const common::ObSqlCollectionInfo *collection_info =
          reinterpret_cast<const common::ObSqlCollectionInfo *>(sub_meta.value_);
      if (OB_ISNULL(collection_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] Arrow collection middle metadata is null", K(ret),
                 K(subschema_id));
      } else {
        collection_meta = collection_info->collection_meta_;
      }
    }
  }
  return ret;
}

// MERGE can refer to source-only fields (for example old_id or op). Keep the
// existing conversion contract for named target columns. Supply only the
// metadata that the existing scalar converter needs when no target is named;
// it still owns the Arrow-to-SQL type decision and rejects unsupported types.
static int build_merge_source_column(const arrow::Field &field,
                                     const sql::ObSQLSessionInfo &session,
                                     share::schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  ObObjType type = ObVarcharType;
  ObAccuracy accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObVarcharType];
  ObCollationType collation = CS_TYPE_UTF8MB4_BIN;
  switch (field.type()->id()) {
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY: collation = CS_TYPE_BINARY; break;
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
      const arrow::DecimalType &decimal = static_cast<const arrow::DecimalType &>(*field.type());
      type = session.is_oracle_compatible() ? ObNumberType : ObDecimalIntType;
      accuracy.set_precision(decimal.precision());
      accuracy.set_scale(decimal.scale());
      break;
    }
    case arrow::Type::DATE32:
      type = session.is_oracle_compatible() ? ObDateType : ObMySQLDateType;
      break;
    case arrow::Type::TIMESTAMP:
      type = session.is_oracle_compatible() ? ObDateTimeType : ObMySQLDateTimeType;
      break;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64: type = ObTimeType; break;
    default: break;
  }
  if (OB_SUCC(ret)) {
    if (field.type()->id() != arrow::Type::DECIMAL128
        && field.type()->id() != arrow::Type::DECIMAL256) {
      accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[type];
    }
    column.set_data_type(type);
    column.set_accuracy(accuracy);
    column.set_collation_type(ob_is_string_type(type) ? collation : CS_TYPE_INVALID);
  }
  return ret;
}

int ObArrowStreamCatalog::get_columns(uint64_t stream_id,
                                      const sql::ObSQLSessionInfo *session,
                                      ObIAllocator &alloc,
                                      ObIArray<sql::ObArrowStreamColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  void *session_registry = OB_NOT_NULL(session)
      ? session->get_flight_sql_stream_registry() : nullptr;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] SQL session is null", K(ret), K(stream_id));
    return ret;
  } else if (OB_ISNULL(session_registry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] no active stream registry", K(ret), K(stream_id));
    return ret;
  }

  ObArrowStreamRegistry *reg = static_cast<ObArrowStreamRegistry *>(session_registry);
  ObArrowStreamEntryPtr entry = reg->get(stream_id);
  if (OB_ISNULL(entry)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("[FlightSQL] stream_id not found in registry", K(ret), K(stream_id));
    return ret;
  }

  // Generated V1 SQL validates Arrow fields against its target table. A
  // schema-only registration belongs to user-provided SQL and has no single
  // target table, so its function-table schema comes directly from Arrow.
  const uint64_t tenant_id = session->get_effective_tenant_id();
  share::schema::ObSchemaGetterGuard guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (!entry->schema_only) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] schema service unavailable", K(ret));
      return ret;
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("[FlightSQL] get_tenant_schema_guard failed", K(ret), K(tenant_id));
      return ret;
    }

    uint64_t db_id = common::OB_INVALID_ID;
    const ObString ob_db(static_cast<int32_t>(entry->db_name.size()),
                         entry->db_name.c_str());
    if (OB_FAIL(guard.get_database_id(tenant_id, ob_db, db_id))
        || OB_INVALID_ID == db_id) {
      ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("[FlightSQL] database not found in schema guard", K(ret),
               "db_name", entry->db_name.c_str());
      return ret;
    }

    const ObString ob_tbl(static_cast<int32_t>(entry->table_name.size()),
                          entry->table_name.c_str());
    if (OB_FAIL(guard.get_table_schema(tenant_id, db_id, ob_tbl, false, table_schema))
        || OB_ISNULL(table_schema)) {
      ret = OB_SUCC(ret) ? OB_TABLE_NOT_EXIST : ret;
      LOG_WARN("[FlightSQL] table not found in schema guard", K(ret),
               "table_name", entry->table_name.c_str());
      return ret;
    }
  }

  // ── Map each Arrow field → OB column, populate desc ────────────────────────
  ObFlightSqlColumnMapping &mapping = entry->column_mapping;
  for (int64_t i = 0; OB_SUCC(ret) && i < mapping.count(); ++i) {
    ObFlightSqlColumnDesc &col_desc = mapping.at(i);
    const int field_idx = col_desc.arrow_field_idx;
    if (OB_UNLIKELY(!entry->arrow_schema
                    || field_idx < 0
                    || field_idx >= entry->arrow_schema->num_fields())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] arrow_field_idx out of range", K(ret), K(i), K(field_idx));
      break;
    }

    std::shared_ptr<arrow::Field> arrow_field_ptr =
        entry->arrow_schema->field(field_idx);
    if (OB_ISNULL(arrow_field_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] Arrow schema field is null", K(ret), K(i), K(field_idx));
      break;
    }
    const arrow::Field &arrow_field = *arrow_field_ptr;
    const ObString ob_col_name(static_cast<int32_t>(arrow_field.name().size()),
                               arrow_field.name().c_str());
    const share::schema::ObColumnSchemaV2 *ob_col = entry->schema_only
        ? nullptr : table_schema->get_column_schema(ob_col_name);
    share::schema::ObColumnSchemaV2 source_column;
    if (entry->schema_only) {
      // No target-column validation is meaningful for arbitrary user SQL.
    } else if (OB_ISNULL(ob_col) && entry->is_merge_source) {
      if (OB_FAIL(build_merge_source_column(arrow_field, *session, source_column))) {
        LOG_WARN("[FlightSQL] failed to map source-only MERGE field", K(ret), K(i));
        break;
      }
      ob_col = &source_column;
    } else if (OB_ISNULL(ob_col)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("[FlightSQL] Arrow field not found in OB table schema", K(ret),
               "field_name", arrow_field.name().c_str(),
               "table_name", entry->table_name.c_str());
      break;
    }

    uint16_t collection_subschema_id = UINT_MAX16;
    const common::ObCollectionTypeBase *collection_meta = nullptr;
    if (is_arrow_collection_field(arrow_field)
        && OB_FAIL(resolve_middle_collection_meta(
               arrow_field, session, collection_subschema_id, collection_meta))) {
      LOG_WARN("[FlightSQL] failed to resolve Arrow middle collection metadata", K(ret), K(i));
      break;
    }

    sql::ObArrowStreamColDesc out;
    // This is the one type-mapping decision for the whole ingest path.  The
    // resolver exposes this descriptor and the converter below materializes
    // the matching datum layout.  Do not replace it with the target table
    // type: column_conv in the generated INSERT owns the subsequent SQL cast.
    if (OB_FAIL(ob_flight_sql_get_output_col_desc(
            arrow_field, session->is_oracle_compatible(), collection_subschema_id, out))) {
      LOG_WARN("[FlightSQL] failed to map Arrow field to converter output", K(ret), K(i));
      break;
    }

    // Populate conversion metadata into the registry entry for executor use.
    if (OB_FAIL(ob_flight_sql_build_convert_fn(arrow_field, out, collection_meta,
                                               col_desc))) {
      LOG_WARN("[FlightSQL] ob_flight_sql_build_convert_fn failed", K(ret), K(i));
      break;
    }

    const ObString raw_name = entry->schema_only || entry->is_merge_source
        ? ob_col_name : ob_col->get_column_name_str();
    char *buf = static_cast<char *>(alloc.alloc(raw_name.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FlightSQL] failed to allocate column name buffer", K(ret));
      break;
    }
    MEMCPY(buf, raw_name.ptr(), raw_name.length());
    out.col_name.assign(buf, raw_name.length());
    if (OB_FAIL(col_descs.push_back(out))) {
      LOG_WARN("[FlightSQL] failed to push col desc", K(ret), K(i));
    }
  }
  return ret;
}

int ObArrowStreamCatalog::get_columns_for_ingest(
    const std::string &ingest_id,
    const sql::ObSQLSessionInfo *session,
    ObIAllocator &alloc,
    ObIArray<sql::ObArrowStreamColDesc> &col_descs)
{
  int ret = OB_SUCCESS;

  std::shared_ptr<ObFlightSqlIngestEntry> ingest_entry =
      ObFlightSqlIngestRegistry::instance().get(ingest_id);
  if (OB_ISNULL(ingest_entry)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("[FlightSQL] ingest_id not found in IngestRegistry", K(ret),
             "ingest_id", ingest_id.c_str());
    return ret;
  }

  // The resolver must see the same Arrow schema as the VecOp converter.  V3
  // receives it explicitly from ExecuteIngest because the coordinator cannot
  // inspect remote DoPut readers.  V2 uses the locally registered schema as a
  // backwards-compatible fallback.  Arrow field names are the one source of
  // truth for function-table column names.  In both cases the descriptor below
  // is produced by ob_flight_sql_get_output_col_desc(), never by copying the
  // target table schema.
  std::shared_ptr<arrow::Schema> mapping_schema;
  bool has_remote_sources = false;
  {
    std::lock_guard<std::mutex> lock(ingest_entry->mu);
    mapping_schema = ingest_entry->advertised_arrow_schema;
    has_remote_sources = !ingest_entry->server_list.empty();
  }

  std::shared_ptr<ObFlightSqlIngestEntry> stream_entry = ingest_entry;
  ObArrowStreamEntryPtr first_stream;
  if (OB_ISNULL(mapping_schema)) {
    // The V2 DoPut group is deliberately separate from the coordinator entry
    // (the ':stream' suffix).  Wait for it here so resolve-time metadata does
    // not race with an ExecuteIngest issued just before the DoPuts.
    const std::string local_stream_key = ingest_id + ":stream";
    if (has_remote_sources) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] missing advertised Arrow schema for ingest", K(ret),
               "ingest_id", ingest_id.c_str(), K(has_remote_sources));
      return ret;
    }
    static constexpr int64_t INGEST_SCHEMA_WAIT_US = 600LL * 1000 * 1000;
    if (OB_FAIL(ObFlightSqlIngestRegistry::instance().wait_for_stream(
            local_stream_key, INGEST_SCHEMA_WAIT_US))) {
      LOG_WARN("[FlightSQL] waiting for local Arrow schema failed", K(ret),
               "ingest_id", ingest_id.c_str());
      return ret;
    }
    stream_entry = ObFlightSqlIngestRegistry::instance().get(local_stream_key);
    if (OB_ISNULL(stream_entry)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FlightSQL] local Arrow schema entry disappeared", K(ret),
               "ingest_id", ingest_id.c_str());
      return ret;
    }
    {
      std::lock_guard<std::mutex> lock(stream_entry->mu);
      mapping_schema = stream_entry->local_arrow_schema;
    }
  }
  if (OB_ISNULL(mapping_schema) || mapping_schema->num_fields() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] ingest has no Arrow schema for type mapping", K(ret),
             "ingest_id", ingest_id.c_str());
    return ret;
  }

  // A V2 local entry needs the converter functions now; V3 entries live on
  // remote observers and initialize from the serialized function-table output
  // expressions after verifying the same advertised schema.
  if (stream_entry != ingest_entry) {
    std::vector<ObFlightSqlSlotInfo> slots;
    {
      std::lock_guard<std::mutex> lock(stream_entry->mu);
      slots = stream_entry->slots;
    }
    for (const ObFlightSqlSlotInfo &slot : slots) {
      if (!slot.ready) { continue; }
      ObArrowStreamEntryPtr stream;
      int lookup_ret = OB_SUCCESS;
      if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
        lookup_ret = OB_ERR_UNEXPECTED;
      } else {
        lookup_ret = GCTX.flight_sql_session_mgr_->get_active_stream_entry(
            slot.sess_id, slot.stream_id, stream);
      }
      if (OB_SUCCESS == lookup_ret) {
        first_stream = stream;
        break;
      } else if (OB_ENTRY_NOT_EXIST != lookup_ret) {
        ret = lookup_ret;
        LOG_WARN("[FlightSQL] failed to look up local Arrow stream",
                 K(ret), K(slot.sess_id), K(slot.stream_id));
        break;
      }
    }
    if (OB_SUCCESS != ret) {
      return ret;
    } else if (OB_ISNULL(first_stream)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FlightSQL] no local Arrow stream available for converter setup", K(ret),
               "ingest_id", ingest_id.c_str());
      return ret;
    }
  }

  // OB schema lookup uses IngestEntry db/table names.
  const uint64_t tenant_id = OB_NOT_NULL(session)
      ? session->get_effective_tenant_id() : OB_INVALID_TENANT_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] schema service unavailable", K(ret));
    return ret;
  }
  share::schema::ObSchemaGetterGuard guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("[FlightSQL] get_tenant_schema_guard failed", K(ret), K(tenant_id));
    return ret;
  }

  uint64_t db_id = common::OB_INVALID_ID;
  const ObString ob_db(static_cast<int32_t>(ingest_entry->db_name.size()),
                       ingest_entry->db_name.c_str());
  if (OB_FAIL(guard.get_database_id(tenant_id, ob_db, db_id))
      || OB_INVALID_ID == db_id) {
    ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
    LOG_WARN("[FlightSQL] database not found for ingest", K(ret),
             "db_name", ingest_entry->db_name.c_str());
    return ret;
  }

  const share::schema::ObTableSchema *table_schema = nullptr;
  const ObString ob_tbl(static_cast<int32_t>(ingest_entry->table_name.size()),
                        ingest_entry->table_name.c_str());
  if (OB_FAIL(guard.get_table_schema(tenant_id, db_id, ob_tbl, false, table_schema))
      || OB_ISNULL(table_schema)) {
    ret = OB_SUCC(ret) ? OB_TABLE_NOT_EXIST : ret;
    LOG_WARN("[FlightSQL] table not found for ingest", K(ret),
             "table_name", ingest_entry->table_name.c_str());
    return ret;
  }

  // Build function-table descriptors and (for V2) the local converter mapping
  // from the shared Arrow-field + compatibility-mode middle-type decision.
  const int64_t num_cols = mapping_schema->num_fields();

  for (int64_t i = 0; OB_SUCC(ret) && i < num_cols; ++i) {
    const std::shared_ptr<arrow::Field> arrow_field_ptr = mapping_schema->field(i);
    if (OB_ISNULL(arrow_field_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] Arrow schema field is null (ingest)", K(ret), K(i));
      break;
    }
    const arrow::Field &arrow_field = *arrow_field_ptr;
    const std::string &col_name_str = arrow_field.name();
    const ObString ob_col_name(static_cast<int32_t>(col_name_str.size()),
                               col_name_str.c_str());
    const share::schema::ObColumnSchemaV2 *ob_col =
        table_schema->get_column_schema(ob_col_name);
    if (OB_ISNULL(ob_col)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("[FlightSQL] column not found in OB table (ingest)", K(ret),
               "col_name", col_name_str.c_str());
      break;
    }

    uint16_t collection_subschema_id = UINT_MAX16;
    const common::ObCollectionTypeBase *collection_meta = nullptr;
    if (is_arrow_collection_field(arrow_field)
        && OB_FAIL(resolve_middle_collection_meta(
               arrow_field, session, collection_subschema_id, collection_meta))) {
      LOG_WARN("[FlightSQL] failed to resolve Arrow middle collection metadata for ingest",
               K(ret), K(i));
      break;
    }

    sql::ObArrowStreamColDesc out;
    if (OB_FAIL(ob_flight_sql_get_output_col_desc(
            arrow_field, session->is_oracle_compatible(), collection_subschema_id, out))) {
      LOG_WARN("[FlightSQL] failed to map ingest Arrow field to converter output", K(ret), K(i));
      break;
    }

    if (OB_NOT_NULL(first_stream)) {
      if (OB_UNLIKELY(i >= first_stream->column_mapping.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] V2 Arrow mapping column count mismatch", K(ret), K(i),
                 "mapping_count", first_stream->column_mapping.count());
        break;
      }
      ObFlightSqlColumnDesc &col_desc = first_stream->column_mapping.at(i);
      if (OB_FAIL(ob_flight_sql_build_convert_fn(arrow_field, out, collection_meta,
                                                 col_desc))) {
        LOG_WARN("[FlightSQL] failed to build V2 ingest converter", K(ret), K(i));
        break;
      }
    }
    const ObString raw_name(ob_col->get_column_name_str());
    char *buf = static_cast<char *>(alloc.alloc(raw_name.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
    MEMCPY(buf, raw_name.ptr(), raw_name.length());
    out.col_name.assign(buf, raw_name.length());
    if (OB_FAIL(col_descs.push_back(out))) { break; }
  }

  // V2: propagate the shared converter mapping to every local DoPut slot.
  if (OB_SUCC(ret) && OB_NOT_NULL(first_stream)) {
    std::vector<ObFlightSqlSlotInfo> slots;
    {
      std::lock_guard<std::mutex> lock(stream_entry->mu);
      slots = stream_entry->slots;
    }
    for (const ObFlightSqlSlotInfo &slot : slots) {
      if (!slot.ready) { continue; }
      ObArrowStreamEntryPtr stream;
      if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] Flight SQL session manager is null", K(ret));
        break;
      } else if (OB_FAIL(GCTX.flight_sql_session_mgr_->get_active_stream_entry(
                     slot.sess_id, slot.stream_id, stream))) {
        LOG_WARN("[FlightSQL] failed to look up stream for converter propagation",
                 K(ret), K(slot.sess_id), K(slot.stream_id));
        break;
      } else if (stream != first_stream) {
        stream->column_mapping = first_stream->column_mapping;
      }
    }
  }
  return ret;
}

static std::string extract_ingest_id_from_expr(const sql::ObRawExpr *func_expr)
{
  if (OB_ISNULL(func_expr) || func_expr->get_param_count() < 1) { return ""; }
  const sql::ObRawExpr *arg0 = func_expr->get_param_expr(0);
  if (OB_ISNULL(arg0) || arg0->get_expr_class() != sql::ObRawExpr::EXPR_CONST) { return ""; }
  const sql::ObConstRawExpr *const_expr = static_cast<const sql::ObConstRawExpr *>(arg0);
  const common::ObString &s = const_expr->get_value().get_string();
  return std::string(s.ptr(), static_cast<size_t>(s.length()));
}

// server_list stores observer RPC addresses, not Flight SQL listener addresses.
// Keep parsing in one place so sharding and PX location selection agree on what
// constitutes a local source.
static bool parse_observer_rpc_addr(const std::string &addr_str, common::ObAddr &addr)
{
  bool valid = false;
  const size_t colon = addr_str.rfind(':');
  if (colon != std::string::npos && colon != 0 && colon + 1 < addr_str.size()) {
    const std::string host = addr_str.substr(0, colon);
    try {
      const int port = std::stoi(addr_str.substr(colon + 1));
      if (port > 0 && port <= 65535) {
        common::ObAddr parsed(common::ObAddr::IPV4, host.c_str(), port);
        if (parsed.is_valid()) {
          addr = parsed;
          valid = true;
        }
      }
    } catch (...) {
      // The location caller reports the malformed address as an optimizer error.
    }
  }
  return valid;
}

sql::ObShardingInfo *ObArrowStreamCatalog::get_sharding(sql::ObOptimizerContext &opt_ctx,
                                                        const sql::ObRawExpr *func_expr)
{
  if (OB_ISNULL(func_expr) ||
      func_expr->get_expr_type() != T_FUN_SYS_ARROW_STREAMS) {
    LOG_TRACE("[FlightSQL] get_sharding: not ARROW_STREAMS, returning local");
    return opt_ctx.get_local_sharding();
  }
  const std::string ingest_id = extract_ingest_id_from_expr(func_expr);
  if (ingest_id.empty()) {
    LOG_TRACE("[FlightSQL] get_sharding: empty ingest_id, returning local");
    return opt_ctx.get_local_sharding();
  }
  std::shared_ptr<ObFlightSqlIngestEntry> entry =
      ObFlightSqlIngestRegistry::instance().get(ingest_id);
  if (OB_ISNULL(entry)) {
    LOG_TRACE("[FlightSQL] get_sharding: entry not found, returning local",
              "ingest_id", ingest_id.c_str());
    return opt_ctx.get_local_sharding();
  }
  if (entry->server_list.empty()) {
    LOG_TRACE("[FlightSQL] get_sharding: no server location, returning local",
              "ingest_id", ingest_id.c_str());
    return opt_ctx.get_local_sharding();
  }
  if (entry->server_list.size() == 1) {
    common::ObAddr source_addr;
    if (parse_observer_rpc_addr(entry->server_list.front(), source_addr)
        && source_addr == GCTX.self_addr()) {
      LOG_TRACE("[FlightSQL] get_sharding: single local server, returning local",
                "ingest_id", ingest_id.c_str(), K(source_addr));
      return opt_ctx.get_local_sharding();
    }
  }
  LOG_TRACE("[FlightSQL] get_sharding: remote or multi-server, returning distributed",
            "ingest_id", ingest_id.c_str(),
            "server_list_size", entry->server_list.size());
  return opt_ctx.get_distributed_sharding();
}

int ObArrowStreamCatalog::get_locations_for_arrow_streams(
    const sql::ObRawExpr *func_expr,
    common::ObIArray<common::ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  const std::string ingest_id = extract_ingest_id_from_expr(func_expr);
  if (ingest_id.empty()) { return OB_SUCCESS; }

  std::shared_ptr<ObFlightSqlIngestEntry> entry =
      ObFlightSqlIngestRegistry::instance().get(ingest_id);
  if (OB_ISNULL(entry)) {
    LOG_TRACE("[FlightSQL] get_locations: entry not found",
              "ingest_id", ingest_id.c_str());
    return OB_SUCCESS;
  }
  if (entry->server_list.empty()) {
    LOG_TRACE("[FlightSQL] get_locations: no server locations",
              "ingest_id", ingest_id.c_str());
    return OB_SUCCESS;
  }

  for (const std::string &addr_str : entry->server_list) {
    common::ObAddr addr;
    if (!parse_observer_rpc_addr(addr_str, addr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] invalid observer RPC address", K(ret),
               "addr", addr_str.c_str());
      break;
    }
    bool duplicate = false;
    for (int64_t i = 0; i < servers.count(); ++i) {
      if (servers.at(i) == addr) {
        duplicate = true;
        break;
      }
    }
    if (duplicate) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] duplicate observer RPC address",
               K(ret), "addr", addr_str.c_str(), K(addr));
      break;
    } else if (OB_FAIL(servers.push_back(addr))) {
      LOG_WARN("[FlightSQL] failed to push server addr", K(ret));
      break;
    }
  }
  LOG_TRACE("[FlightSQL] get_locations_for_arrow_streams",
            "ingest_id", ingest_id.c_str(),
            "server_count", servers.count());
  return ret;
}

} // namespace observer
} // namespace oceanbase
